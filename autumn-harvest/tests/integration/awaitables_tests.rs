//! Tests for issue #615 — the open-awaitables diagnostic projection.
//!
//! These are **pure** tests (no DB, no testcontainers): they exercise the core
//! `awaitables::project_awaitables` projection over hand-built timestamped
//! histories plus the read-only replay driver `executor::drive_query_replay`
//! (issue #612) — the exact composition the plugin's
//! `GET /workflows/{id}/awaitables` endpoint uses.
//!
//! The falsifiable delta from the issue: a workflow parked on
//! `ctx.receive_signal("approval")` with **no signal yet sent** must report an
//! awaitable naming `"approval"`. The pre-existing side-table panel reports
//! nothing here, because the awaited-but-unsent signal exists only as a parked
//! `WaitForSignal` command inside the replayed coroutine — never as a row.
//!
//! The HTTP status mapping, admin gating, and shard routing are covered by
//! `autumn-harvest-plugin/tests/awaitables_integration.rs`.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::awaitables::{
    AWAITABLE_CATEGORY_CAP, Awaitable, AwaitableKind, WaitSetInput, WaitSetSource,
    project_awaitables,
};
use autumn_harvest::context::{
    WorkflowCommand, WorkflowContext, WorkflowHistoryPolicy, empty_shared_state,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{QueryReplayOutcome, drive_query_replay};
use autumn_harvest::types::{ActivityExecId, ExecutionId, UpdateId};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{Value, json};

// ── Test workflow handlers ────────────────────────────────────────────────

/// Parks forever on an awaited-but-unsent signal named `approval`.
fn signal_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let payload = ctx
            .wait_for_signal("approval")
            .await
            .map_err(|e| e.to_string())?;
        Ok(payload)
    })
}

/// Parks on the signal-or-deadline race (`wait_for_signal_timeout`, issue #476).
fn signal_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let outcome = ctx
            .wait_for_signal_timeout("approval", Duration::from_secs(3600))
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "approved": outcome.is_some() }))
    })
}

/// Parks on a single already-scheduled activity.
fn activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("charge_card", json!({"amount": 42}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks on a durable timer.
fn timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("cooldown", 300)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks awaiting a child workflow's terminal result.
fn child_parent_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw("fulfillment_flow", json!({"order": 7}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks command-less on `await_condition(|| false)` — the cold park only a
/// replayed view can observe.
fn condition_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.await_condition(|| false)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks on `continue_as_new` — the run is transitioning, NOT stuck on a
/// missing input, so no condition awaitable may be fabricated for it.
fn continue_as_new_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.continue_as_new(json!({"carry": 1}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Parks acquiring a durable mutex (issue #691) — bonus coverage beyond the
/// issue's six categories.
fn mutex_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _guard = ctx
            .mutex("ledger:acct-1")
            .acquire()
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ── History builders ──────────────────────────────────────────────────────

const fn ts(offset_secs: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_750_000_000 + offset_secs, 0).expect("valid ts")
}

const fn started_row(at: DateTime<Utc>) -> (DateTime<Utc>, WorkflowEvent) {
    (
        at,
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: at,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
    )
}

fn events_of(rows: &[(DateTime<Utc>, WorkflowEvent)]) -> Vec<WorkflowEvent> {
    rows.iter().map(|(_, e)| e.clone()).collect()
}

fn build_ctx(exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> WorkflowContext {
    WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        events,
        empty_shared_state(),
        WorkflowHistoryPolicy::default(),
    )
}

const DRIVE_BUDGET: Duration = Duration::from_secs(5);

/// Drives `handler` over `rows`, asserts the drive suspends, and projects the
/// drained command buffer — the exact pipeline the endpoint composes.
fn drive_and_project(
    rows: &[(DateTime<Utc>, WorkflowEvent)],
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> autumn_harvest::awaitables::AwaitablesProjection {
    let ctx = build_ctx(ExecutionId::new(), events_of(rows));
    let outcome = drive_query_replay(&ctx, handler, Value::Null, DRIVE_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "a parked workflow must suspend during the read-only drive"
    );
    let commands = ctx.drain_commands();
    project_awaitables(
        rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    )
}

fn kinds_of(awaitables: &[Awaitable]) -> Vec<AwaitableKind> {
    awaitables.iter().map(|a| a.kind).collect()
}

// ── The falsifiable delta: awaited-but-unsent signal ─────────────────────

#[test]
fn unsent_signal_awaitable_is_named() {
    let rows = vec![started_row(ts(0))];
    let projection = drive_and_project(&rows, signal_wait_workflow);

    assert_eq!(projection.source, WaitSetSource::Replayed);
    assert_eq!(
        projection.awaitables.len(),
        1,
        "exactly one awaitable: the unsent signal; got {:?}",
        projection.awaitables
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.kind, AwaitableKind::Signal);
    assert_eq!(awaitable.name.as_deref(), Some("approval"));
    assert_eq!(
        awaitable.since,
        Some(ts(0)),
        "an awaited-unsent signal is waiting since the last recorded event"
    );
    assert!(!projection.truncated);
}

#[test]
fn signal_timeout_race_folds_deadline_into_signal_awaitable() {
    // The #476 race arms a reserved `__signal_timeout:{seq}:{name}` timer; the
    // diagnostic must fold it into the signal's deadline rather than report an
    // internal timer row.
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                // The per-context signal_timeout_seq counter pre-increments, so
                // the first race in workflow code carries seq 1.
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 3600,
            },
        ),
    ];
    let projection = drive_and_project(&rows, signal_timeout_workflow);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Signal],
        "the reserved race timer must not surface as a separate timer awaitable: {:?}",
        projection.awaitables
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.name.as_deref(), Some("approval"));
    assert_eq!(
        awaitable.deadline,
        Some(ts(10) + ChronoDuration::seconds(3600)),
        "the race deadline is the reserved timer's fire time"
    );
    assert_eq!(
        awaitable.since,
        Some(ts(10)),
        "since = when the race was armed"
    );
}

// ── Pending activity ──────────────────────────────────────────────────────

#[test]
fn pending_activity_reports_name_id_and_since() {
    let activity_id = ActivityExecId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(30),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: json!({"amount": 42}),
                queue: "payments".into(),
            },
        ),
    ];
    let projection = drive_and_project(&rows, activity_workflow);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.name.as_deref(), Some("charge_card"));
    assert_eq!(
        awaitable.id.as_deref(),
        Some(activity_id.to_string().as_str())
    );
    assert_eq!(awaitable.since, Some(ts(30)));
    assert!(!awaitable.local);
    assert!(!awaitable.external);
}

// ── Unfired timer ─────────────────────────────────────────────────────────

#[test]
fn unfired_timer_reports_deadline() {
    let rows = vec![
        started_row(ts(0)),
        (
            ts(5),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("cooldown"),
                duration_secs: 300,
            },
        ),
    ];
    let projection = drive_and_project(&rows, timer_workflow);

    assert_eq!(kinds_of(&projection.awaitables), vec![AwaitableKind::Timer]);
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.id.as_deref(), Some("cooldown"));
    assert_eq!(awaitable.since, Some(ts(5)));
    assert_eq!(
        awaitable.deadline,
        Some(ts(5) + ChronoDuration::seconds(300)),
        "timer deadline = armed-at + duration"
    );
}

// ── Pending child workflow ────────────────────────────────────────────────

#[test]
fn pending_child_reports_name_and_exec_id() {
    let child_id = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(60),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: json!({"order": 7}),
            },
        ),
    ];
    let projection = drive_and_project(&rows, child_parent_workflow);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::ChildWorkflow]
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.name.as_deref(), Some("fulfillment_flow"));
    assert_eq!(awaitable.id.as_deref(), Some(child_id.to_string().as_str()));
    assert_eq!(awaitable.since, Some(ts(60)));
}

// ── await_condition park ──────────────────────────────────────────────────

#[test]
fn await_condition_park_reports_condition_awaitable() {
    let rows = vec![started_row(ts(0))];
    let projection = drive_and_project(&rows, condition_workflow);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Condition],
        "a command-less cold park is an await_condition park"
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(
        awaitable.name, None,
        "await_condition carries no site label"
    );
    assert_eq!(
        awaitable.since,
        Some(ts(0)),
        "waiting since the last recorded event"
    );
}

#[test]
fn continue_as_new_park_is_not_a_condition_awaitable() {
    let rows = vec![started_row(ts(0))];
    let ctx = build_ctx(ExecutionId::new(), events_of(&rows));
    let outcome = drive_query_replay(&ctx, continue_as_new_workflow, Value::Null, DRIVE_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::Suspended);
    let commands = ctx.drain_commands();
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );
    assert!(
        projection.awaitables.is_empty(),
        "a continue-as-new park is a transition, not a stuck wait: {:?}",
        projection.awaitables
    );
}

// ── Pending updates ───────────────────────────────────────────────────────

#[test]
fn pending_update_reported_and_resolved_update_not() {
    let pending_id = UpdateId::new();
    let resolved_id = UpdateId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(20),
            WorkflowEvent::UpdateAdmitted {
                update_id: resolved_id,
                name: "set_priority".into(),
                input: json!({"level": 1}),
                timestamp: ts(20),
            },
        ),
        (
            ts(21),
            WorkflowEvent::UpdateCompleted {
                update_id: resolved_id,
                output: Value::Null,
            },
        ),
        (
            ts(40),
            WorkflowEvent::UpdateAdmitted {
                update_id: pending_id,
                name: "cancel_order".into(),
                input: json!({}),
                timestamp: ts(40),
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    let updates: Vec<&Awaitable> = projection
        .awaitables
        .iter()
        .filter(|a| a.kind == AwaitableKind::Update)
        .collect();
    assert_eq!(updates.len(), 1, "only the unresolved update is pending");
    assert_eq!(updates[0].name.as_deref(), Some("cancel_order"));
    assert_eq!(
        updates[0].id.as_deref(),
        Some(pending_id.to_string().as_str())
    );
    assert_eq!(updates[0].since, Some(ts(40)));
}

#[test]
fn in_flight_update_result_command_suppresses_pending_update() {
    // An update whose handler completed THIS cycle (RecordUpdateResult drained
    // but UpdateCompleted not yet persisted) is no longer pending.
    let update_id = UpdateId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(20),
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "set_priority".into(),
                input: json!({}),
                timestamp: ts(20),
            },
        ),
    ];
    let commands = vec![WorkflowCommand::RecordUpdateResult {
        update_id,
        result: Ok(Value::Null),
    }];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );
    assert!(
        projection
            .awaitables
            .iter()
            .all(|a| a.kind != AwaitableKind::Update),
        "an update resolved in the drained cycle is not pending: {:?}",
        projection.awaitables
    );
}

// ── Mutex (bonus category) ────────────────────────────────────────────────

#[test]
fn mutex_acquire_reported() {
    let rows = vec![started_row(ts(0))];
    let projection = drive_and_project(&rows, mutex_workflow);

    assert_eq!(kinds_of(&projection.awaitables), vec![AwaitableKind::Mutex]);
    assert_eq!(
        projection.awaitables[0].name.as_deref(),
        Some("ledger:acct-1")
    );
}

// ── History-only degraded mode ────────────────────────────────────────────

#[test]
fn history_only_mode_covers_scannable_categories() {
    let activity_id = ActivityExecId::new();
    let child_id = ExecutionId::new();
    let update_id = UpdateId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(1),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "payments".into(),
            },
        ),
        (
            ts(2),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("cooldown"),
                duration_secs: 60,
            },
        ),
        (
            ts(3),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(4),
            WorkflowEvent::UpdateAdmitted {
                update_id,
                name: "cancel_order".into(),
                input: Value::Null,
                timestamp: ts(4),
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    assert_eq!(projection.source, WaitSetSource::HistoryOnly);
    let mut kinds = kinds_of(&projection.awaitables);
    kinds.sort();
    let mut expected = vec![
        AwaitableKind::Activity,
        AwaitableKind::Timer,
        AwaitableKind::ChildWorkflow,
        AwaitableKind::Update,
    ];
    expected.sort();
    assert_eq!(kinds, expected);
}

#[test]
fn history_only_mode_skips_resolved_waits() {
    let activity_id = ActivityExecId::new();
    let child_id = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(1),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "payments".into(),
            },
        ),
        (
            ts(2),
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
        ),
        (
            ts(3),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("cooldown"),
                duration_secs: 60,
            },
        ),
        (
            ts(4),
            WorkflowEvent::TimerFired {
                timer_id: autumn_harvest::types::TimerId::new("cooldown"),
            },
        ),
        (
            ts(5),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(6),
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: Value::Null,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert!(
        projection.awaitables.is_empty(),
        "resolved waits are not awaitables: {:?}",
        projection.awaitables
    );
}

#[test]
fn history_only_local_activity_mid_retry_reported() {
    let activity_id = ActivityExecId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(1),
            WorkflowEvent::LocalActivityScheduled {
                activity_id,
                name: "compute_checksum".into(),
                input: Value::Null,
                resolved: true,
                retry_policy: None,
                start_to_close_nanos: None,
            },
        ),
        (
            ts(2),
            WorkflowEvent::LocalActivityFailed {
                activity_id,
                error: "transient".into(),
                attempt: 1,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    let awaitable = &projection.awaitables[0];
    assert!(awaitable.local, "a local activity carries the local flag");
    assert_eq!(awaitable.name.as_deref(), Some("compute_checksum"));
}

#[test]
fn history_only_external_activity_reports_deadline() {
    let activity_id = ActivityExecId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(9),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token: autumn_harvest::types::ExternalActivityToken::new(),
                name: "human_review".into(),
                input: Value::Null,
                queue: "reviews".into(),
                schedule_to_close_secs: 86_400,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    let awaitable = &projection.awaitables[0];
    assert!(
        awaitable.external,
        "an external handoff carries the external flag"
    );
    assert_eq!(
        awaitable.deadline,
        Some(ts(9) + ChronoDuration::seconds(86_400)),
        "external activity deadline = awaiting-since + schedule_to_close"
    );
}

// ── Terminal history ──────────────────────────────────────────────────────

#[test]
fn terminal_history_projection_is_empty() {
    let activity_id = ActivityExecId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(1),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "payments".into(),
            },
        ),
        (
            ts(2),
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: Value::Null,
            },
        ),
        (
            ts(3),
            WorkflowEvent::WorkflowCompleted {
                output: Value::Null,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert!(projection.awaitables.is_empty());
    assert!(!projection.truncated);
}

// ── Bounding / truncation ─────────────────────────────────────────────────

#[test]
fn per_category_cap_truncates_with_flag() {
    let mut rows = vec![started_row(ts(0))];
    for i in 0..(AWAITABLE_CATEGORY_CAP + 10) {
        rows.push((
            ts(i64::try_from(i).expect("small index") + 1),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new(format!("t-{i}")),
                duration_secs: 600,
            },
        ));
    }
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    let timers = projection
        .awaitables
        .iter()
        .filter(|a| a.kind == AwaitableKind::Timer)
        .count();
    assert_eq!(timers, AWAITABLE_CATEGORY_CAP, "first N only");
    assert!(projection.truncated);
    assert!(projection.truncated_kinds.contains(&AwaitableKind::Timer));
    // First-N: the earliest timers are the ones kept.
    assert_eq!(
        projection.awaitables[0].id.as_deref(),
        Some("t-0"),
        "the first-armed timer is kept"
    );
}

// ── Scale: 10k-event history projects correctly ───────────────────────────

#[test]
fn ten_thousand_event_history_projects() {
    // 5k scheduled+completed activity pairs, then one open activity: the
    // projection over a 10k-event history must stay correct (the p95 < 1s
    // budget is release-mode evidence; this test pins correctness at scale).
    let mut rows = vec![started_row(ts(0))];
    for i in 0..4_999_i64 {
        let id = ActivityExecId::new();
        rows.push((
            ts(i * 2 + 1),
            WorkflowEvent::ActivityScheduled {
                activity_id: id,
                name: "process_item".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ));
        rows.push((
            ts(i * 2 + 2),
            WorkflowEvent::ActivityCompleted {
                activity_id: id,
                output: Value::Null,
            },
        ));
    }
    let open = ActivityExecId::new();
    rows.push((
        ts(20_000),
        WorkflowEvent::ActivityScheduled {
            activity_id: open,
            name: "final_step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ));

    let start = std::time::Instant::now();
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    let elapsed = start.elapsed();
    println!("10k-event projection took {elapsed:?}");

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    assert_eq!(projection.awaitables[0].name.as_deref(), Some("final_step"));
    assert_eq!(
        projection.awaitables[0].id.as_deref(),
        Some(open.to_string().as_str())
    );
}

// ── Serialization contract ────────────────────────────────────────────────

#[test]
fn awaitable_kinds_serialize_snake_case() {
    assert_eq!(
        serde_json::to_value(AwaitableKind::ChildWorkflow).expect("serialize"),
        json!("child_workflow")
    );
    assert_eq!(
        serde_json::to_value(AwaitableKind::Condition).expect("serialize"),
        json!("condition")
    );
    let awaitable = Awaitable {
        kind: AwaitableKind::Signal,
        name: Some("approval".into()),
        id: None,
        since: None,
        deadline: None,
        local: false,
        external: false,
    };
    let value = serde_json::to_value(&awaitable).expect("serialize");
    assert_eq!(value["kind"], json!("signal"));
    assert_eq!(value["name"], json!("approval"));
    assert!(
        value.get("id").is_none() || value["id"].is_null(),
        "absent fields are omitted or null, never fabricated"
    );
}
