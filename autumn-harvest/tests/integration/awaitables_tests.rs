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

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::awaitables::{
    AWAITABLE_CATEGORY_CAP, Awaitable, AwaitableKind, AwaitablesProjection, WaitSetInput,
    WaitSetSource, project_awaitables, retain_fire_eligible_timers,
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

/// A benign trailing event: appended AFTER an awaitable's own scheduling event
/// so `last_event_at` differs from the scheduling timestamp — proving `since`
/// is derived from the awaitable's own event, not the last-event fallback
/// (test review T1).
fn marker_row(at: DateTime<Utc>) -> (DateTime<Utc>, WorkflowEvent) {
    (
        at,
        WorkflowEvent::MarkerRecorded {
            name: "trailing-benign-marker".into(),
            details: Value::Null,
        },
    )
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
        // Trailing benign event: last_event_at (ts 99) != scheduled-at (ts 30),
        // so the since assert below proves event-derived provenance (T1).
        marker_row(ts(99)),
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
        // Trailing benign event decouples last_event_at from the arm ts (T1).
        marker_row(ts(99)),
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
        // Trailing benign event decouples last_event_at from started-at (T1).
        marker_row(ts(99)),
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
    // The cycle's only drained command is a RecordUpdateResult — an in-flight
    // update-result commit, NOT a command-less `await_condition` cold park. The
    // result must be empty: the resolving update is suppressed AND no phantom
    // Condition is inferred (issue #615 Codex P2 — a `RecordUpdateResult` leaves
    // `awaitables` empty and `transitioning` false, which the old
    // `awaitables.is_empty() && !transitioning` gate misread as a condition park).
    assert!(
        projection.awaitables.is_empty(),
        "a RecordUpdateResult-only cycle waits on nothing — no phantom \
         Condition, no pending update: {:?}",
        projection.awaitables
    );
}

#[test]
fn in_flight_update_result_does_not_mask_a_still_pending_update() {
    // One update's handler completed this cycle (RecordUpdateResult), a second
    // update is still admitted-but-unresolved. The resolving one is suppressed,
    // the pending one is reported — and the RecordUpdateResult must NOT cause a
    // phantom `await_condition` to be inferred alongside it.
    let resolving_id = UpdateId::new();
    let pending_id = UpdateId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(20),
            WorkflowEvent::UpdateAdmitted {
                update_id: resolving_id,
                name: "set_priority".into(),
                input: json!({}),
                timestamp: ts(20),
            },
        ),
        (
            ts(30),
            WorkflowEvent::UpdateAdmitted {
                update_id: pending_id,
                name: "cancel_order".into(),
                input: json!({}),
                timestamp: ts(30),
            },
        ),
    ];
    let commands = vec![WorkflowCommand::RecordUpdateResult {
        update_id: resolving_id,
        result: Ok(Value::Null),
    }];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Update],
        "only the still-pending update is reported — no phantom Condition: {:?}",
        projection.awaitables
    );
    assert_eq!(
        projection.awaitables[0].name.as_deref(),
        Some("cancel_order")
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
    assert_eq!(
        awaitable.since,
        Some(ts(1)),
        "since = the scheduling event, not the mid-retry failure (T1)"
    );
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
        // Trailing benign event decouples last_event_at from awaiting-since (T1).
        marker_row(ts(99)),
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
        awaitable.since,
        Some(ts(9)),
        "since = when the external handoff was recorded, not the last event"
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

// ── Multi-awaitable replayed projection (test review T3) ──────────────────

/// Parks on an activity AND a timer concurrently (`futures::join!`).
fn join_activity_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (a, t) = futures::join!(
            ctx.execute_activity_raw("charge_card", json!({"amount": 42}), "payments"),
            ctx.timer("cooldown", 300),
        );
        a.map_err(|e| e.to_string())?;
        t.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

#[test]
fn joined_activity_and_timer_both_reported() {
    let rows = vec![started_row(ts(0))];
    let projection = drive_and_project(&rows, join_activity_timer_workflow);

    let mut kinds = kinds_of(&projection.awaitables);
    kinds.sort();
    assert_eq!(
        kinds,
        vec![AwaitableKind::Activity, AwaitableKind::Timer],
        "a join! park reports every joined wait: {:?}",
        projection.awaitables
    );
    let activity = projection
        .awaitables
        .iter()
        .find(|a| a.kind == AwaitableKind::Activity)
        .expect("activity awaitable");
    assert_eq!(activity.name.as_deref(), Some("charge_card"));
    let timer = projection
        .awaitables
        .iter()
        .find(|a| a.kind == AwaitableKind::Timer)
        .expect("timer awaitable");
    assert_eq!(timer.id.as_deref(), Some("cooldown"));
}

// ── Replayed-mode pending update (test review T2) ─────────────────────────

#[test]
fn replayed_mode_reports_pending_update_alongside_wait() {
    // A pending (admitted, unresolved) update rides along with whatever wait
    // the workflow is parked on — the replayed projection reports BOTH.
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
        marker_row(ts(99)),
    ];
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let commands = vec![WorkflowCommand::WaitForSignal {
        signal_name: "approval".into(),
        result_tx: tx,
    }];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    let mut kinds = kinds_of(&projection.awaitables);
    kinds.sort();
    assert_eq!(kinds, vec![AwaitableKind::Signal, AwaitableKind::Update]);
    let update = projection
        .awaitables
        .iter()
        .find(|a| a.kind == AwaitableKind::Update)
        .expect("pending update awaitable");
    assert_eq!(update.name.as_deref(), Some("set_priority"));
    assert_eq!(update.since, Some(ts(20)), "since = admission time (T1)");
}

// ── Synthetic-buffer arm coverage (test review T5/T6) ─────────────────────

#[test]
fn external_activity_command_reports_external_with_deadline() {
    let activity_id = ActivityExecId::new();
    let token = autumn_harvest::types::ExternalActivityToken::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(9),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "human_review".into(),
                input: Value::Null,
                queue: "reviews".into(),
                schedule_to_close_secs: 86_400,
            },
        ),
        marker_row(ts(99)),
    ];
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let commands = vec![WorkflowCommand::ScheduleExternalActivity {
        activity_id,
        token,
        name: "human_review".into(),
        input: Value::Null,
        queue: "reviews".into(),
        schedule_to_close_secs: 86_400,
        result_tx: tx,
    }];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    let awaitable = &projection.awaitables[0];
    assert!(awaitable.external);
    assert_eq!(awaitable.name.as_deref(), Some("human_review"));
    assert_eq!(awaitable.since, Some(ts(9)));
    assert_eq!(
        awaitable.deadline,
        Some(ts(9) + ChronoDuration::seconds(86_400))
    );
}

#[test]
fn external_workflow_commands_report_targets() {
    let await_target = ExecutionId::new();
    let signal_target = ExecutionId::new();
    let cancel_target = ExecutionId::new();
    let await_id = autumn_harvest::types::ExternalAwaitId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(7),
            WorkflowEvent::ExternalAwaitRequested {
                await_id,
                target: await_target,
            },
        ),
        marker_row(ts(99)),
    ];
    let (await_tx, _r1) = tokio::sync::oneshot::channel();
    let (signal_tx, _r2) = tokio::sync::oneshot::channel();
    let (cancel_tx, _r3) = tokio::sync::oneshot::channel();
    let commands = vec![
        WorkflowCommand::AwaitExternalWorkflow {
            await_id,
            target: await_target,
            result_tx: await_tx,
            already_requested: true,
        },
        WorkflowCommand::SignalExternalWorkflow {
            signal_id: autumn_harvest::types::ExternalSignalId::new(),
            target: signal_target,
            signal_name: "release".into(),
            payload: Value::Null,
            result_tx: signal_tx,
            already_requested: false,
            idempotency_key: None,
        },
        WorkflowCommand::RequestCancelExternalWorkflow {
            cancel_id: autumn_harvest::types::ExternalCancelId::new(),
            target: cancel_target,
            result_tx: cancel_tx,
            already_requested: false,
        },
    ];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![
            AwaitableKind::ExternalWorkflow,
            AwaitableKind::ExternalWorkflow,
            AwaitableKind::ExternalWorkflow,
        ]
    );
    let awaited = &projection.awaitables[0];
    assert_eq!(
        awaited.id.as_deref(),
        Some(await_target.to_string().as_str())
    );
    assert_eq!(
        awaited.since,
        Some(ts(7)),
        "an open external await's since = its recorded request event"
    );
    let signalled = &projection.awaitables[1];
    assert_eq!(signalled.name.as_deref(), Some("release"));
    assert_eq!(
        signalled.id.as_deref(),
        Some(signal_target.to_string().as_str())
    );
    let cancelled = &projection.awaitables[2];
    assert_eq!(
        cancelled.id.as_deref(),
        Some(cancel_target.to_string().as_str())
    );
}

#[test]
fn orphan_race_timer_falls_back_to_timer_awaitable() {
    // Defensive: a reserved race timer with NO matching signal wait in the
    // drained buffer must not be silently dropped — it degrades to a plain
    // timer awaitable.
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 3600,
            },
        ),
    ];
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let commands = vec![WorkflowCommand::StartTimer {
        timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
        duration_secs: 3600,
        result_tx: tx,
    }];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    assert_eq!(kinds_of(&projection.awaitables), vec![AwaitableKind::Timer]);
    assert_eq!(
        projection.awaitables[0].deadline,
        Some(ts(10) + ChronoDuration::seconds(3600)),
        "the orphan race arm keeps its deadline"
    );
}

#[test]
fn duplicate_signal_race_waiters_each_keep_their_own_deadline() {
    // Two concurrent signal-or-deadline waits on the SAME signal name with
    // distinct deadlines — join!(receive_signal_timeout("approval", 60s),
    // receive_signal_timeout("approval", 120s)) — must each retain their own
    // deadline. The reserved race deadlines are paired to the Signal awaitables
    // in command order (per-name FIFO), not collapsed by name (issue #615 Codex
    // P2 — the old per-name HashMap let the second race overwrite the first, and
    // the per-name position map only ever updated the first awaitable, so the
    // second open wait dropped its deadline).
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 60,
            },
        ),
        (
            ts(20),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:2:approval"),
                duration_secs: 120,
            },
        ),
    ];
    let (s1, _r1) = tokio::sync::oneshot::channel();
    let (t1, _r2) = tokio::sync::oneshot::channel();
    let (s2, _r3) = tokio::sync::oneshot::channel();
    let (t2, _r4) = tokio::sync::oneshot::channel();
    let commands = vec![
        WorkflowCommand::WaitForSignal {
            signal_name: "approval".into(),
            result_tx: s1,
        },
        WorkflowCommand::StartTimer {
            timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
            duration_secs: 60,
            result_tx: t1,
        },
        WorkflowCommand::WaitForSignal {
            signal_name: "approval".into(),
            result_tx: s2,
        },
        WorkflowCommand::StartTimer {
            timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:2:approval"),
            duration_secs: 120,
            result_tx: t2,
        },
    ];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    // Both waits are open Signal awaitables (no reserved timer surfaces as its
    // own row), each with its own deadline and arm time.
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Signal, AwaitableKind::Signal],
        "both same-name waits are open signals, no orphan timer rows: {:?}",
        projection.awaitables
    );
    assert_eq!(
        projection.awaitables[0].deadline,
        Some(ts(10) + ChronoDuration::seconds(60)),
        "first waiter keeps the 60s deadline (armed at ts(10)): {:?}",
        projection.awaitables
    );
    assert_eq!(projection.awaitables[0].since, Some(ts(10)));
    assert_eq!(
        projection.awaitables[1].deadline,
        Some(ts(20) + ChronoDuration::seconds(120)),
        "second waiter keeps the 120s deadline (armed at ts(20)): {:?}",
        projection.awaitables
    );
    assert_eq!(projection.awaitables[1].since, Some(ts(20)));
}

// ── Child-or-deadline race fold (test review T8, issue #779) ──────────────

#[test]
fn child_timeout_race_folds_deadline_into_child_awaitable() {
    let child_id = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(60),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(61),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
                duration_secs: 600,
            },
        ),
        marker_row(ts(99)),
    ];
    let (child_tx, _r1) = tokio::sync::oneshot::channel();
    let (timer_tx, _r2) = tokio::sync::oneshot::channel();
    let commands = vec![
        WorkflowCommand::StartChildWorkflow {
            child_id,
            workflow_name: "fulfillment_flow".into(),
            input: Value::Null,
            result_tx: child_tx,
        },
        WorkflowCommand::StartTimer {
            timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
            duration_secs: 600,
            result_tx: timer_tx,
        },
    ];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::ChildWorkflow],
        "the reserved child race timer folds into the child, never a timer row: {:?}",
        projection.awaitables
    );
    let awaitable = &projection.awaitables[0];
    assert_eq!(awaitable.name.as_deref(), Some("fulfillment_flow"));
    assert_eq!(awaitable.since, Some(ts(60)));
    assert_eq!(
        awaitable.deadline,
        Some(ts(61) + ChronoDuration::seconds(600)),
        "the child race deadline is the reserved timer's fire time"
    );
}

#[test]
fn duplicate_child_race_waiters_each_keep_their_own_deadline() {
    // Mirror of duplicate_signal_race_waiters for the child-or-deadline race
    // (issue #779): two concurrent child-timeout waits on the SAME child
    // workflow type with distinct deadlines each retain their own deadline,
    // paired in command order rather than collapsed by name (issue #615 Codex
    // P2).
    let child_a = ExecutionId::new();
    let child_b = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_a,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(11),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
                duration_secs: 300,
            },
        ),
        (
            ts(20),
            WorkflowEvent::ChildWorkflowStarted {
                child_id: child_b,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(21),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__child_timeout:2:fulfillment_flow"),
                duration_secs: 600,
            },
        ),
    ];
    let (c1, _r1) = tokio::sync::oneshot::channel();
    let (t1, _r2) = tokio::sync::oneshot::channel();
    let (c2, _r3) = tokio::sync::oneshot::channel();
    let (t2, _r4) = tokio::sync::oneshot::channel();
    let commands = vec![
        WorkflowCommand::StartChildWorkflow {
            child_id: child_a,
            workflow_name: "fulfillment_flow".into(),
            input: Value::Null,
            result_tx: c1,
        },
        WorkflowCommand::StartTimer {
            timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
            duration_secs: 300,
            result_tx: t1,
        },
        WorkflowCommand::StartChildWorkflow {
            child_id: child_b,
            workflow_name: "fulfillment_flow".into(),
            input: Value::Null,
            result_tx: c2,
        },
        WorkflowCommand::StartTimer {
            timer_id: autumn_harvest::types::TimerId::new("__child_timeout:2:fulfillment_flow"),
            duration_secs: 600,
            result_tx: t2,
        },
    ];
    let projection = project_awaitables(
        &rows,
        WaitSetInput::Replayed {
            commands: &commands,
        },
        AWAITABLE_CATEGORY_CAP,
    );

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::ChildWorkflow, AwaitableKind::ChildWorkflow],
        "both same-type child waits are open, no orphan timer rows: {:?}",
        projection.awaitables
    );
    assert_eq!(
        projection.awaitables[0].deadline,
        Some(ts(11) + ChronoDuration::seconds(300)),
        "first child waiter keeps the 300s deadline: {:?}",
        projection.awaitables
    );
    assert_eq!(
        projection.awaitables[1].deadline,
        Some(ts(21) + ChronoDuration::seconds(600)),
        "second child waiter keeps the 600s deadline: {:?}",
        projection.awaitables
    );
}

#[test]
fn history_only_child_timeout_race_folds_deadline() {
    let child_id = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(60),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(61),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
                duration_secs: 600,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::ChildWorkflow]
    );
    assert_eq!(
        projection.awaitables[0].deadline,
        Some(ts(61) + ChronoDuration::seconds(600))
    );
}

// ── Resolved races leave no stale arms (replay review R3) ─────────────────

#[test]
fn history_only_signal_win_race_is_not_reported_open() {
    // A signal-win tears its reserved deadline timer down via an EVENT-LESS
    // CancelRaceLosers delete — no TimerFired/TimerCancelled is ever recorded.
    // The SignalReceived closes the arm, so a RESOLVED race must not surface
    // as a still-open signal awaitable.
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 3600,
            },
        ),
        (
            ts(20),
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: json!({"ok": true}),
            },
        ),
        marker_row(ts(99)),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert!(
        projection.awaitables.is_empty(),
        "a resolved signal-win race is not an open awaitable: {:?}",
        projection.awaitables
    );
}

#[test]
fn history_only_timer_win_late_signal_keeps_arm_closed_once() {
    // Timer-win: TimerFired closed the arm. A LATE SignalReceived afterwards
    // must be a no-op (nothing left to close, nothing reported open).
    let rows = vec![
        started_row(ts(0)),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 60,
            },
        ),
        (
            ts(70),
            WorkflowEvent::TimerFired {
                timer_id: autumn_harvest::types::TimerId::new("__signal_timeout:1:approval"),
            },
        ),
        (
            ts(80),
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: Value::Null,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert!(
        projection.awaitables.is_empty(),
        "a timer-won race with a late signal reports nothing open: {:?}",
        projection.awaitables
    );
}

#[test]
fn history_only_child_win_race_is_not_reported_open() {
    // Child-win of a child-or-deadline race (issue #779): the reserved timer
    // is torn down event-lessly; the child terminal closes the arm.
    let child_id = ExecutionId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(60),
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name: "fulfillment_flow".into(),
                input: Value::Null,
            },
        ),
        (
            ts(61),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new("__child_timeout:1:fulfillment_flow"),
                duration_secs: 600,
            },
        ),
        (
            ts(90),
            WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: Value::Null,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert!(
        projection.awaitables.is_empty(),
        "a resolved child-win race is not an open awaitable: {:?}",
        projection.awaitables
    );
}

#[test]
fn history_only_reports_in_flight_external_signal_and_cancel() {
    // Codex P2: an ExternalSignalRequested / ExternalCancelRequested with no
    // Delivered/Failed terminal is still parked on that external op — the
    // crash-recovery / cross-shard-outbox window. The history-only fallback
    // (unregistered handler, decode failure) must report it, not drop it into
    // the catch-all. A resolved signal in the same history is NOT reported.
    let open_signal_target = ExecutionId::new();
    let resolved_signal_target = ExecutionId::new();
    let open_cancel_target = ExecutionId::new();
    let open_signal_id = autumn_harvest::types::ExternalSignalId::new();
    let resolved_signal_id = autumn_harvest::types::ExternalSignalId::new();
    let open_cancel_id = autumn_harvest::types::ExternalCancelId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(5),
            WorkflowEvent::ExternalSignalRequested {
                signal_id: resolved_signal_id,
                target: resolved_signal_target,
                signal_name: "ping".into(),
                payload: Value::Null,
                idempotency_key: None,
            },
        ),
        (
            ts(6),
            WorkflowEvent::ExternalSignalDelivered {
                signal_id: resolved_signal_id,
            },
        ),
        (
            ts(10),
            WorkflowEvent::ExternalSignalRequested {
                signal_id: open_signal_id,
                target: open_signal_target,
                signal_name: "release".into(),
                payload: Value::Null,
                idempotency_key: None,
            },
        ),
        (
            ts(20),
            WorkflowEvent::ExternalCancelRequested {
                cancel_id: open_cancel_id,
                target: open_cancel_target,
            },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![
            AwaitableKind::ExternalWorkflow,
            AwaitableKind::ExternalWorkflow,
        ],
        "only the two UNRESOLVED external ops are open: {:?}",
        projection.awaitables
    );
    let signal = &projection.awaitables[0];
    assert_eq!(signal.name.as_deref(), Some("release"));
    assert_eq!(
        signal.id.as_deref(),
        Some(open_signal_target.to_string().as_str())
    );
    assert_eq!(signal.since, Some(ts(10)));
    let cancel = &projection.awaitables[1];
    assert_eq!(cancel.name, None, "a cancel request carries no signal name");
    assert_eq!(
        cancel.id.as_deref(),
        Some(open_cancel_target.to_string().as_str())
    );
    assert_eq!(cancel.since, Some(ts(20)));
}

#[test]
fn history_only_extended_external_deadline_is_cleared() {
    // ActivityExternalDeadlineExtended carries no new deadline value, so the
    // stale original schedule-to-close must be CLEARED, not reported as due.
    let activity_id = ActivityExecId::new();
    let token = autumn_harvest::types::ExternalActivityToken::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(9),
            WorkflowEvent::ActivityAwaitingExternal {
                activity_id,
                token,
                name: "human_review".into(),
                input: Value::Null,
                queue: "reviews".into(),
                schedule_to_close_secs: 600,
            },
        ),
        (
            ts(500),
            WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token },
        ),
    ];
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity]
    );
    let awaitable = &projection.awaitables[0];
    assert!(awaitable.external);
    assert_eq!(
        awaitable.deadline, None,
        "an extended external deadline is unknown from history — never stale"
    );
}

// ── Truncation boundary (test review T10) ─────────────────────────────────

#[test]
fn exactly_cap_entries_are_not_truncated() {
    let mut rows = vec![started_row(ts(0))];
    for i in 0..AWAITABLE_CATEGORY_CAP {
        rows.push((
            ts(i64::try_from(i).expect("small index") + 1),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new(format!("t-{i}")),
                duration_secs: 600,
            },
        ));
    }
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);
    assert_eq!(projection.awaitables.len(), AWAITABLE_CATEGORY_CAP);
    assert!(!projection.truncated, "exactly-cap is NOT truncation");
    assert!(projection.truncated_kinds.is_empty());
}

#[test]
fn truncation_is_per_category_not_global() {
    // Cap+1 timers truncate the Timer category only; a single activity in the
    // same projection is untouched.
    let activity_id = ActivityExecId::new();
    let mut rows = vec![
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
    ];
    for i in 0..=AWAITABLE_CATEGORY_CAP {
        rows.push((
            ts(i64::try_from(i).expect("small index") + 2),
            WorkflowEvent::TimerStarted {
                timer_id: autumn_harvest::types::TimerId::new(format!("t-{i}")),
                duration_secs: 600,
            },
        ));
    }
    let projection = project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    assert!(projection.truncated);
    assert_eq!(projection.truncated_kinds, vec![AwaitableKind::Timer]);
    assert_eq!(
        projection
            .awaitables
            .iter()
            .filter(|a| a.kind == AwaitableKind::Activity)
            .count(),
        1,
        "the activity category is untouched by timer truncation"
    );
    assert_eq!(
        projection
            .awaitables
            .iter()
            .filter(|a| a.kind == AwaitableKind::Timer)
            .count(),
        AWAITABLE_CATEGORY_CAP
    );
}

// ── Dormant cancellable timer arms in history-only mode (Codex P2 #1134) ──

#[test]
fn history_only_dormant_timer_arm_is_filtered_when_not_fire_eligible() {
    // A cancellable `ctx.start_timer` arm (issue #768,
    // `WorkflowCommand::ArmTimer { for_await: false }`) records `TimerStarted`
    // but inserts NO `harvest_timers` row, so it cannot fire until
    // `await_fire()`. In the history-only fallback we cannot tell an
    // armed-but-dormant timer from a fire-eligible one — the authoritative
    // signal is the live `harvest_timers` table. `retain_fire_eligible_timers`
    // drops any history-only `Timer` awaitable whose id is not among the live
    // unfired timer ids, keeping genuine timers and all non-timer waits.
    let activity_id = ActivityExecId::new();
    let rows = vec![
        started_row(ts(0)),
        (
            ts(5),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "payments".into(),
            },
        ),
        (
            ts(10),
            WorkflowEvent::TimerStarted {
                // Dormant cancellable arm: no `harvest_timers` row exists.
                timer_id: autumn_harvest::types::TimerId::new("idle_debounce"),
                duration_secs: 3600,
            },
        ),
        (
            ts(20),
            WorkflowEvent::TimerStarted {
                // Genuine, fire-eligible durable timer.
                timer_id: autumn_harvest::types::TimerId::new("cooldown"),
                duration_secs: 300,
            },
        ),
    ];
    let mut projection =
        project_awaitables(&rows, WaitSetInput::HistoryOnly, AWAITABLE_CATEGORY_CAP);

    // Pre-filter: history-only reports every unclosed `TimerStarted`.
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![
            AwaitableKind::Activity,
            AwaitableKind::Timer,
            AwaitableKind::Timer
        ],
        "history-only reports every unclosed TimerStarted before the fire-eligibility filter"
    );

    let live_timer_ids: HashSet<String> = HashSet::from(["cooldown".to_string()]);
    retain_fire_eligible_timers(&mut projection, &live_timer_ids);

    // The dormant `idle_debounce` arm is dropped; the fire-eligible `cooldown`
    // timer and the unrelated activity survive.
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Activity, AwaitableKind::Timer]
    );
    let timer = projection
        .awaitables
        .iter()
        .find(|a| a.kind == AwaitableKind::Timer)
        .expect("the fire-eligible timer survives");
    assert_eq!(timer.id.as_deref(), Some("cooldown"));
}

#[test]
fn retain_fire_eligible_timers_is_a_noop_for_replayed_projections() {
    // The replayed mode already excludes dormant arms by construction (it only
    // matches `ArmTimer { for_await: true }`), so the endpoint-side timer-table
    // filter must never touch a `Replayed` projection — even when a timer id is
    // absent from the live set (a genuine `for_await` arm committed in the same
    // cycle is exactly that: recorded but not yet in `harvest_timers`).
    let mut projection = AwaitablesProjection {
        source: WaitSetSource::Replayed,
        awaitables: vec![Awaitable {
            kind: AwaitableKind::Timer,
            name: None,
            id: Some("cooldown".to_string()),
            since: None,
            deadline: None,
            local: false,
            external: false,
        }],
        truncated: false,
        truncated_kinds: Vec::new(),
        category_cap: AWAITABLE_CATEGORY_CAP,
    };
    let empty: HashSet<String> = HashSet::new();
    retain_fire_eligible_timers(&mut projection, &empty);
    assert_eq!(
        kinds_of(&projection.awaitables),
        vec![AwaitableKind::Timer],
        "a Replayed projection is authoritative — never filtered by the live timer table"
    );
}
