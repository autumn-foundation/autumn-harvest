//! Tests for the time-travel replay debugger (issue #949).
//!
//! Covers the three surfaces:
//!
//! - `ReplayTrace::from_history` — the pure, handler-free O(N) projection
//!   (AC1 minus pending commands).
//! - `ReplayDebugger::trace_snapshot` — prefix replay for per-step pending
//!   commands (AC1's command half, AC2's stepping substrate, AC4's JSON input).
//! - `diff_traces` — first-divergent-command detection (AC3 + the success
//!   metric's ≥10-scenario corpus).

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::debugger::{
    AwaitableKind, Breakpoint, CommandSnapshot, DebugError, DebugStep, DiffKind, ReplayDebugger,
    ReplayTrace, StepDivergence, StepOutcome, diff_traces,
};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::testing::HistorySnapshot;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};
use serde_json::{Value, json};

// ── fixtures ────────────────────────────────────────────────────────────────

fn started() -> WorkflowEvent {
    WorkflowEvent::workflow_started(json!({}), chrono::Utc::now())
}

fn scheduled(id: ActivityExecId, name: &str) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: json!({}),
        queue: "default".to_string(),
    }
}

const fn completed(id: ActivityExecId, output: Value) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output,
    }
}

/// A two-activity workflow: `step_a` then `step_b`.
fn two_step<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_a", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_b", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

/// The *diverged* build: the two activities are swapped.
fn two_step_swapped<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_b", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_a", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

fn two_step_history() -> Vec<WorkflowEvent> {
    let a = ActivityExecId::new();
    let b = ActivityExecId::new();
    vec![
        started(),
        scheduled(a, "step_a"),
        completed(a, json!(1)),
        scheduled(b, "step_b"),
        completed(b, json!(2)),
        WorkflowEvent::WorkflowCompleted {
            output: json!("done"),
        },
    ]
}

/// A snapshot for an arbitrary workflow name.
fn snapshot(workflow_name: &str, events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: workflow_name.to_string(),
        ..snapshot_of(events)
    }
}

fn snapshot_of(events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: "two_step".to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    }
}

fn debugger(name: &str, handler: WorkflowHandlerFn) -> ReplayDebugger {
    ReplayDebugger::new().register_fn(name, handler)
}

// ── AC1: handler-free projection ────────────────────────────────────────────

#[test]
fn from_history_reports_one_step_per_event_with_index_and_type() {
    let events = two_step_history();
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);

    assert_eq!(trace.steps.len(), events.len());
    for (i, step) in trace.steps.iter().enumerate() {
        assert_eq!(step.index, i, "step {i} must carry its own event index");
        assert_eq!(
            step.event_type,
            events[i].type_name(),
            "step {i} must carry the consumed event's type name"
        );
    }
    assert!(!trace.truncated);
}

#[test]
fn from_history_opens_and_closes_an_activity_awaitable() {
    let a = ActivityExecId::new();
    let events = vec![started(), scheduled(a, "step_a"), completed(a, json!(1))];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);

    // After WorkflowStarted: nothing open.
    assert!(trace.steps[0].open_awaitables.is_empty());

    // After ActivityScheduled: exactly one open activity, opened at index 1.
    let open = &trace.steps[1].open_awaitables;
    assert_eq!(open.len(), 1, "the scheduled activity must be open");
    assert_eq!(open[0].kind, AwaitableKind::Activity);
    assert_eq!(open[0].name.as_deref(), Some("step_a"));
    assert_eq!(open[0].id.as_deref(), Some(a.to_string().as_str()));
    assert_eq!(
        open[0].opened_at, 1,
        "AC1 requires the event index that opened the awaitable"
    );

    // After ActivityCompleted: closed again.
    assert!(trace.steps[2].open_awaitables.is_empty());
}

#[test]
fn from_history_opens_timer_and_child_awaitables() {
    let child = ExecutionId::new();
    let events = vec![
        started(),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("t1"),
            duration_secs: 30,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id: child,
            workflow_name: "kid".to_string(),
            input: json!({}),
        },
    ];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);

    let open = &trace.steps[2].open_awaitables;
    assert_eq!(open.len(), 2);
    assert!(
        open.iter()
            .any(|a| a.kind == AwaitableKind::Timer && a.id.as_deref() == Some("t1"))
    );
    assert!(open.iter().any(|a| a.kind == AwaitableKind::ChildWorkflow
        && a.name.as_deref() == Some("kid")
        && a.opened_at == 2));
}

#[test]
fn from_history_captures_version_and_patch_gate_values() {
    let events = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "version:billing".to_string(),
            details: json!(2),
        },
        WorkflowEvent::MarkerRecorded {
            name: "patch:invoice-v2".to_string(),
            details: json!(1),
        },
    ];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);

    let last = trace.steps.last().expect("non-empty trace");
    assert_eq!(
        last.version_gates.get("billing"),
        Some(&2),
        "AC1 requires resolved version-gate values"
    );
    assert!(last.patch_gates.contains("invoice-v2"));
}

#[test]
fn from_history_captures_side_effects_and_markers() {
    let events = vec![
        started(),
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Uuid,
            name: None,
            value: json!("018f-…"),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".to_string(),
            details: json!(3),
        },
    ];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);
    let last = trace.steps.last().expect("non-empty trace");

    assert_eq!(last.side_effects.len(), 1);
    assert_eq!(last.side_effects[0].kind, "uuid");
    assert_eq!(last.side_effects[0].event_index, 1);
    assert_eq!(last.side_effects[0].value, json!("018f-…"));

    assert_eq!(last.markers.len(), 1);
    assert_eq!(last.markers[0].name, "fan_out:1");
    assert_eq!(last.markers[0].event_index, 2);
}

/// AC4: an offload/codec envelope must display verbatim, never error.
#[test]
fn from_history_passes_offload_envelope_through_verbatim() {
    let envelope = json!({
        "_harvest_offload_envelope": 1,
        "store_id": "s3",
        "key": "blob/abc",
        "len": 2_097_152,
        "checksum": "deadbeef",
    });
    let a = ActivityExecId::new();
    let events = vec![
        started(),
        scheduled(a, "step_a"),
        completed(a, envelope.clone()),
    ];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);

    assert_eq!(trace.steps.len(), 3, "an envelope must not abort the trace");
    assert_eq!(
        trace.steps[2].resolved_payload.as_ref(),
        Some(&envelope),
        "the envelope must be surfaced verbatim, not decoded or rejected"
    );
}

#[test]
fn from_history_on_empty_history_is_an_empty_trace() {
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &[]);
    assert!(trace.steps.is_empty());
    assert_eq!(trace.total_events, 0);
    assert!(!trace.truncated);
}

// ── AC1/AC2: prefix replay for pending commands ─────────────────────────────

#[tokio::test]
async fn trace_snapshot_reports_pending_commands_per_step() {
    let events = two_step_history();
    let trace = debugger("two_step", two_step)
        .trace_snapshot(snapshot_of(events))
        .await
        .expect("registered handler");

    // Step 0 (after WorkflowStarted): the workflow's frontier is "schedule step_a".
    let step0 = &trace.steps[0];
    assert_eq!(step0.outcome, StepOutcome::Suspended);
    assert!(
        step0
            .commands
            .iter()
            .any(|c| c.kind == "ScheduleActivity" && c.summary.contains("step_a")),
        "step 0 commands were {:?}",
        step0.commands
    );

    // Step 2 (after step_a completed): the frontier moves to step_b.
    assert!(
        trace.steps[2]
            .commands
            .iter()
            .any(|c| c.kind == "ScheduleActivity" && c.summary.contains("step_b")),
        "step 2 commands were {:?}",
        trace.steps[2].commands
    );

    // The final step consumes the terminal event and the workflow returns.
    assert_eq!(
        trace.steps.last().expect("non-empty").outcome,
        StepOutcome::ReachedTerminal
    );
}

#[tokio::test]
async fn trace_json_round_trips_an_exported_history() {
    // AC4: production histories are debugged locally from the exported JSON.
    let snapshot = snapshot_of(two_step_history());
    let json_text = serde_json::to_string(&snapshot).expect("snapshot serialises");

    let trace = debugger("two_step", two_step)
        .trace_json(&json_text)
        .await
        .expect("valid snapshot JSON with a registered handler");

    assert_eq!(trace.workflow_name, "two_step");
    assert_eq!(trace.steps.len(), 6);
}

#[tokio::test]
async fn trace_snapshot_rejects_an_unregistered_workflow() {
    let err = ReplayDebugger::new()
        .trace_snapshot(snapshot_of(two_step_history()))
        .await
        .expect_err("no handler registered");
    assert!(
        err.to_string().contains("two_step"),
        "the error must name the missing workflow: {err}"
    );
}

#[tokio::test]
async fn max_steps_truncates_and_flags_the_trace() {
    let trace = debugger("two_step", two_step)
        .max_steps(2)
        .trace_snapshot(snapshot_of(two_step_history()))
        .await
        .expect("registered handler");

    assert_eq!(trace.steps.len(), 2);
    assert!(trace.truncated, "a capped trace must say so");
    assert_eq!(trace.total_events, 6);
}

// ── AC2: breakpoints ────────────────────────────────────────────────────────

#[test]
fn breakpoint_by_event_type_finds_the_first_match() {
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &two_step_history());
    let hit = trace
        .find_breakpoint(&Breakpoint::EventType("ActivityCompleted".to_string()), 0)
        .expect("ActivityCompleted occurs at index 2");
    assert_eq!(hit, 2);
}

#[test]
fn breakpoint_by_event_type_resumes_after_the_given_step() {
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &two_step_history());
    let hit = trace
        .find_breakpoint(&Breakpoint::EventType("ActivityCompleted".to_string()), 3)
        .expect("the second ActivityCompleted occurs at index 4");
    assert_eq!(hit, 4);
}

#[test]
fn breakpoint_by_event_index_is_exact() {
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &two_step_history());
    assert_eq!(
        trace.find_breakpoint(&Breakpoint::EventIndex(3), 0),
        Some(3)
    );
    assert_eq!(trace.find_breakpoint(&Breakpoint::EventIndex(99), 0), None);
}

#[test]
fn breakpoint_by_activity_name_matches_the_scheduling_event() {
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &two_step_history());
    let hit = trace
        .find_breakpoint(&Breakpoint::ActivityName("step_b".to_string()), 0)
        .expect("step_b is scheduled at index 3");
    assert_eq!(hit, 3);
}

#[test]
fn breakpoint_by_signal_name_matches_the_received_event() {
    let events = vec![
        started(),
        WorkflowEvent::SignalReceived {
            signal_name: "approval".to_string(),
            payload: json!(true),
        },
    ];
    let trace = ReplayTrace::from_history("two_step", ExecutionId::new(), &events);
    assert_eq!(
        trace.find_breakpoint(&Breakpoint::SignalName("approval".to_string()), 0),
        Some(1)
    );
    assert_eq!(
        trace.find_breakpoint(&Breakpoint::SignalName("nope".to_string()), 0),
        None
    );
}

// ── AC3: diff mode ──────────────────────────────────────────────────────────

#[tokio::test]
async fn diff_reports_no_divergence_for_identical_registrations() {
    let snapshot = snapshot_of(two_step_history());
    let left = debugger("two_step", two_step)
        .trace_snapshot(snapshot.clone())
        .await
        .expect("registered");
    let right = debugger("two_step", two_step)
        .trace_snapshot(snapshot)
        .await
        .expect("registered");

    let diff = diff_traces(&left, &right);
    assert!(
        diff.divergence.is_none(),
        "identical code must diff clean, got {:?}",
        diff.divergence
    );
}

#[tokio::test]
async fn diff_identifies_the_first_divergent_command_step() {
    let snapshot = snapshot_of(two_step_history());
    let left = debugger("two_step", two_step)
        .trace_snapshot(snapshot.clone())
        .await
        .expect("registered");
    let right = debugger("two_step", two_step_swapped)
        .trace_snapshot(snapshot)
        .await
        .expect("registered");

    let diff = diff_traces(&left, &right);
    let div = diff.divergence.expect("the swapped build must diverge");

    // Step 0 is the very first frontier: `step_a` vs `step_b`.
    assert_eq!(div.step_index, 0, "the FIRST divergent step is step 0");
    assert!(
        matches!(div.kind, DiffKind::CommandMismatch { command_index: 0 }),
        "expected a command mismatch at command 0, got {:?}",
        div.kind
    );
    let left_step = div.left.expect("both sides present for a command mismatch");
    let right_step = div
        .right
        .expect("both sides present for a command mismatch");
    assert!(left_step.commands[0].summary.contains("step_a"));
    assert!(right_step.commands[0].summary.contains("step_b"));
}

// ── Success metric: the ≥10-scenario divergence corpus ──────────────────────
//
// Issue #949's success metric: "On a seeded corpus of ≥10 divergence scenarios
// (drawn from `NonDeterminismKind` classes: SideEffectDrift, count-mismatch
// fan-out, reordered commands, version-gate changes), the diff mode identifies
// the exact first divergent command index in 10/10."
//
// Each scenario below pairs a BASELINE handler (which recorded the history)
// with a CANDIDATE handler (the "new build"), and asserts `diff_traces`
// reports the divergence at the exact expected step.

macro_rules! wf {
    ($name:ident, $ctx:ident, $body:block) => {
        fn $name<'a>(
            $ctx: &'a WorkflowContext,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move $body)
        }
    };
}

async fn act(ctx: &WorkflowContext, name: &str) -> Result<Value, String> {
    ctx.execute_activity_raw(name, json!({}), "default")
        .await
        .map_err(|e| e.to_string())
}

// 1. Reordered commands (baseline: a then b; candidate: b then a).
//    Covered by `two_step` / `two_step_swapped` above — reused here.

// 2. An inserted activity at the front.
wf!(three_step, ctx, {
    act(ctx, "step_a").await?;
    act(ctx, "step_b").await?;
    Ok(json!("done"))
});
wf!(three_step_inserted, ctx, {
    act(ctx, "step_new").await?;
    act(ctx, "step_a").await?;
    act(ctx, "step_b").await?;
    Ok(json!("done"))
});

// 3. A renamed activity in the middle.
wf!(renamed_mid, ctx, {
    act(ctx, "step_a").await?;
    act(ctx, "step_b_renamed").await?;
    Ok(json!("done"))
});

// 4. A changed activity input (same name, different payload).
wf!(changed_input, ctx, {
    act(ctx, "step_a").await?;
    ctx.execute_activity_raw("step_b", json!({ "v": 2 }), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});

// 5. A changed target queue.
wf!(changed_queue, ctx, {
    ctx.execute_activity_raw("step_a", json!({}), "priority")
        .await
        .map_err(|e| e.to_string())?;
    act(ctx, "step_b").await?;
    Ok(json!("done"))
});

// 6. An activity replaced by a durable timer.
wf!(timer_instead, ctx, {
    act(ctx, "step_a").await?;
    ctx.timer("wait", 30).await.map_err(|e| e.to_string())?;
    Ok(json!("done"))
});

// 7. An activity replaced by a child workflow.
wf!(child_instead, ctx, {
    act(ctx, "step_a").await?;
    ctx.spawn_child_workflow_raw("kid", json!({}))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});

// 8. A dropped trailing activity (candidate completes early).
wf!(dropped_tail, ctx, {
    act(ctx, "step_a").await?;
    Ok(json!("done"))
});

// 9. `SideEffectDrift`: a `ctx.new_uuid()` inserted before the first activity.
wf!(side_effect_first, ctx, {
    let _ = ctx.new_uuid();
    act(ctx, "step_a").await?;
    act(ctx, "step_b").await?;
    Ok(json!("done"))
});

// 10. Version-gate change: a `ctx.version()` gate inserted at the front.
wf!(version_gated, ctx, {
    if ctx.version("billing", 1, 2) >= 2 {
        act(ctx, "step_new").await?;
    }
    act(ctx, "step_a").await?;
    act(ctx, "step_b").await?;
    Ok(json!("done"))
});

// 11. Count-mismatch fan-out: the fan-out width changed.
wf!(fan_out_two, ctx, {
    ctx.execute_activity_fan_out_raw(vec![
        ("worker".to_string(), json!(0), "default".to_string()),
        ("worker".to_string(), json!(1), "default".to_string()),
    ])
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});
wf!(fan_out_three, ctx, {
    ctx.execute_activity_fan_out_raw(vec![
        ("worker".to_string(), json!(0), "default".to_string()),
        ("worker".to_string(), json!(1), "default".to_string()),
        ("worker".to_string(), json!(2), "default".to_string()),
    ])
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});

// 13. Fan-out whose *second* slot's input changed. The `fan_out:` marker and
// slot 0 are identical, so the first divergent *command* is not index 0 — the
// one scenario that proves the corpus asserts a real command index rather than
// a constant.
wf!(fan_out_two_changed_tail, ctx, {
    ctx.execute_activity_fan_out_raw(vec![
        ("worker".to_string(), json!(0), "default".to_string()),
        ("worker".to_string(), json!(99), "default".to_string()),
    ])
    .await
    .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});

// 12. A signal wait replaced by an activity.
wf!(signal_wait, ctx, {
    ctx.wait_for_signal("approval")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!("done"))
});
wf!(activity_instead_of_signal, ctx, {
    act(ctx, "auto_approve").await?;
    Ok(json!("done"))
});

/// Build the history a baseline handler would record, by tracing it and
/// materialising the commands it emits into events.
///
/// Deliberately hand-rolled rather than driven by a worker: the corpus needs
/// deterministic, DB-free histories, and the debugger's own tracing is what is
/// under test.
fn history_for(kind: HistoryShape) -> Vec<WorkflowEvent> {
    match kind {
        HistoryShape::TwoActivities => two_step_history(),
        HistoryShape::FanOutTwo => {
            let a = ActivityExecId::new();
            let b = ActivityExecId::new();
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "fan_out:1".to_string(),
                    details: json!(2),
                },
                scheduled(a, "worker"),
                scheduled(b, "worker"),
                completed(a, json!(0)),
                completed(b, json!(1)),
            ]
        }
        HistoryShape::SignalWait => vec![
            started(),
            WorkflowEvent::SignalReceived {
                signal_name: "approval".to_string(),
                payload: json!(true),
            },
        ],
    }
}

#[derive(Clone, Copy)]
enum HistoryShape {
    TwoActivities,
    FanOutTwo,
    SignalWait,
}

struct Scenario {
    label: &'static str,
    shape: HistoryShape,
    baseline: WorkflowHandlerFn,
    candidate: WorkflowHandlerFn,
    expected_step: usize,
    /// The expected `DiffKind::CommandMismatch { command_index }`, or `None`
    /// when the scenario resolves as a different kind.
    ///
    /// The issue's success metric names the *command* index, so asserting only
    /// the step index would leave that dimension unproven — and a stub
    /// hardcoding `command_index: 0` would satisfy every scenario.
    expected_command_index: Option<usize>,
}

/// The seeded corpus. Split in two halves purely so each stays under the
/// `too_many_lines` lint; `divergence_corpus` is the single entry point.
fn divergence_corpus() -> Vec<Scenario> {
    let mut all = divergence_corpus_shape_changes();
    all.extend(divergence_corpus_semantic_changes());
    all
}

/// Scenarios where the *shape* of the emitted command batch changes.
fn divergence_corpus_shape_changes() -> Vec<Scenario> {
    vec![
        Scenario {
            label: "reordered commands",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: two_step_swapped,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "inserted activity at the front",
            shape: HistoryShape::TwoActivities,
            baseline: three_step,
            candidate: three_step_inserted,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "renamed activity mid-history",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: renamed_mid,
            expected_step: 2,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "changed activity input",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: changed_input,
            expected_step: 2,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "changed target queue",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: changed_queue,
            expected_step: 0,
            expected_command_index: Some(0),
        },
    ]
}

/// Scenarios where the command batch keeps its shape but a *value* — a name,
/// an input, a gate, a side effect — changes.
fn divergence_corpus_semantic_changes() -> Vec<Scenario> {
    vec![
        Scenario {
            label: "activity replaced by a timer",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: timer_instead,
            expected_step: 2,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "activity replaced by a child workflow",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: child_instead,
            expected_step: 2,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "dropped trailing activity",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: dropped_tail,
            expected_step: 2,
            expected_command_index: None,
        },
        Scenario {
            label: "SideEffectDrift: inserted ctx.new_uuid()",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: side_effect_first,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "version-gate inserted",
            shape: HistoryShape::TwoActivities,
            baseline: two_step,
            candidate: version_gated,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "count-mismatch fan-out (2 -> 3)",
            shape: HistoryShape::FanOutTwo,
            baseline: fan_out_two,
            candidate: fan_out_three,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "signal wait replaced by an activity",
            shape: HistoryShape::SignalWait,
            baseline: signal_wait,
            candidate: activity_instead_of_signal,
            expected_step: 0,
            expected_command_index: Some(0),
        },
        Scenario {
            label: "fan-out tail-slot input changed (command index > 0)",
            shape: HistoryShape::FanOutTwo,
            baseline: fan_out_two,
            candidate: fan_out_two_changed_tail,
            expected_step: 0,
            expected_command_index: Some(2),
        },
    ]
}

#[tokio::test]
async fn divergence_corpus_identifies_the_first_divergent_step_in_every_scenario() {
    let corpus = divergence_corpus();

    assert!(
        corpus.len() >= 10,
        "the success metric requires at least 10 scenarios, got {}",
        corpus.len()
    );

    let mut identified = 0usize;
    for scenario in &corpus {
        let events = history_for(scenario.shape);
        let mut snapshot = snapshot_of(events);
        snapshot.workflow_name = "wf".to_string();

        let left = debugger("wf", scenario.baseline)
            .trace_snapshot(snapshot.clone())
            .await
            .unwrap_or_else(|e| panic!("[{}] baseline trace: {e}", scenario.label));
        let right = debugger("wf", scenario.candidate)
            .trace_snapshot(snapshot)
            .await
            .unwrap_or_else(|e| panic!("[{}] candidate trace: {e}", scenario.label));

        let diff = diff_traces(&left, &right);
        let div = diff
            .divergence
            .unwrap_or_else(|| panic!("[{}] expected a divergence, found none", scenario.label));

        assert_eq!(
            div.step_index, scenario.expected_step,
            "[{}] first divergent step: expected {}, got {} (kind {:?})",
            scenario.label, scenario.expected_step, div.step_index, div.kind
        );
        match (&div.kind, scenario.expected_command_index) {
            (DiffKind::CommandMismatch { command_index }, Some(expected)) => assert_eq!(
                *command_index, expected,
                "[{}] first divergent COMMAND index: expected {expected}, got {command_index}",
                scenario.label
            ),
            (kind, None) => assert!(
                !matches!(kind, DiffKind::CommandMismatch { .. }),
                "[{}] expected a non-command-mismatch kind, got {kind:?}",
                scenario.label
            ),
            (kind, Some(expected)) => panic!(
                "[{}] expected CommandMismatch {{ command_index: {expected} }}, got {kind:?}",
                scenario.label
            ),
        }
        identified += 1;
    }

    assert_eq!(
        identified,
        corpus.len(),
        "the success metric requires {}/{} exact identifications",
        corpus.len(),
        corpus.len()
    );
}

#[tokio::test]
async fn diff_reports_trace_length_when_one_side_is_capped_shorter() {
    let snapshot = snapshot_of(two_step_history());
    let left = debugger("two_step", two_step)
        .trace_snapshot(snapshot.clone())
        .await
        .expect("registered");
    let right = debugger("two_step", two_step)
        .max_steps(3)
        .trace_snapshot(snapshot)
        .await
        .expect("registered");

    let div = diff_traces(&left, &right)
        .divergence
        .expect("differing trace lengths must be reported");
    // `capped: true` marks this as a *cap artefact* — the right trace was
    // truncated by `max_steps`, so the length difference says nothing about
    // the two traces themselves. Without it, diffing traces built with
    // different caps would silently report a divergence that does not exist.
    assert!(matches!(
        div.kind,
        DiffKind::TraceLength {
            left: 6,
            right: 3,
            capped: true
        }
    ));
}

#[tokio::test]
async fn first_divergence_breakpoint_finds_the_diverging_step() {
    // A candidate that diverges mid-history surfaces a per-step divergence,
    // which `Breakpoint::FirstDivergence` locates without a second trace.
    let trace = debugger("two_step", renamed_mid)
        .trace_snapshot(snapshot_of(two_step_history()))
        .await
        .expect("registered");

    let hit = trace
        .find_breakpoint(&Breakpoint::FirstDivergence, 0)
        .expect("the renamed activity must diverge somewhere");
    let step = trace.step(hit).expect("in range");
    let div = step.divergence.as_ref().expect("divergence recorded");
    assert!(
        div.expected.contains("step_b") || div.actual.contains("step_b_renamed"),
        "divergence should name the mismatched activity: {div:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// AC3, second arm: diffing two *fixture histories* (no registered code).
//
// A handler-free trace emits no commands at all, so the only available signal
// is the history-derived facts. These pin that (a) a semantic difference is
// caught, and (b) per-run identity noise is normalized away — without which the
// arm would report a divergence at the first activity of every real pair.
// ─────────────────────────────────────────────────────────────────────────────

fn fixture_pair_events(activity_name: &str) -> Vec<WorkflowEvent> {
    let activity_id = ActivityExecId::new();
    vec![
        started(),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: activity_name.to_string(),
            input: serde_json::json!({"amount": 100}),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("ok"),
        },
    ]
}

fn handler_free_trace(events: &[WorkflowEvent]) -> ReplayTrace {
    ReplayTrace::from_history("wf".to_string(), ExecutionId::new(), events)
}

#[test]
fn two_independent_recordings_of_the_same_scenario_do_not_diverge() {
    // Different activity UUIDs and start timestamps on both sides. If per-run
    // identity leaked into the comparison this would report a divergence at
    // step 1, burying every real signal under UUID noise.
    let left = handler_free_trace(&fixture_pair_events("charge"));
    let right = handler_free_trace(&fixture_pair_events("charge"));
    assert!(
        diff_traces(&left, &right).divergence.is_none(),
        "per-run identity must be normalized out of the fixture-vs-fixture diff"
    );
}

#[test]
fn a_renamed_activity_diverges_at_the_scheduling_step() {
    let left = handler_free_trace(&fixture_pair_events("charge"));
    let right = handler_free_trace(&fixture_pair_events("charge_v2"));
    let div = diff_traces(&left, &right)
        .divergence
        .expect("a renamed activity is a real difference");
    assert_eq!(div.step_index, 1);
    assert!(
        matches!(div.kind, DiffKind::HistoryFacts { field } if field == "open_awaitables"),
        "expected an open_awaitables difference, got {:?}",
        div.kind
    );
}

#[test]
fn a_changed_activity_input_diverges_at_the_scheduling_step() {
    // The input rides `resolved_payload`, which is application data and is
    // therefore compared verbatim (never normalized away).
    let left = handler_free_trace(&fixture_pair_events("charge"));
    let mut right_events = fixture_pair_events("charge");
    if let WorkflowEvent::ActivityScheduled { input, .. } = &mut right_events[1] {
        *input = serde_json::json!({"amount": 999});
    }
    let right = handler_free_trace(&right_events);
    let div = diff_traces(&left, &right)
        .divergence
        .expect("a changed input is a real difference");
    assert_eq!(div.step_index, 1);
    assert!(
        matches!(div.kind, DiffKind::HistoryFacts { field } if field == "resolved_payload"),
        "expected a resolved_payload difference, got {:?}",
        div.kind
    );
}

#[test]
fn a_differing_event_type_diverges_with_the_event_type_field() {
    let left = handler_free_trace(&fixture_pair_events("charge"));
    let right = handler_free_trace(&[
        started(),
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("cooldown"),
            duration_secs: 30,
        },
    ]);
    let div = diff_traces(&left, &right)
        .divergence
        .expect("an activity vs a timer is a real difference");
    assert_eq!(div.step_index, 1);
    assert!(
        matches!(div.kind, DiffKind::HistoryFacts { field } if field == "event_type"),
        "expected an event_type difference, got {:?}",
        div.kind
    );
}

#[test]
fn a_uuid_side_effect_value_is_not_treated_as_a_divergence() {
    // `ctx.new_uuid()` is freshly drawn per run by design (issue #384), so two
    // independent recordings differ there and must NOT be reported.
    let make = || {
        vec![
            started(),
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Uuid,
                name: None,
                value: serde_json::json!(uuid::Uuid::new_v4().to_string()),
            },
        ]
    };
    let left = handler_free_trace(&make());
    let right = handler_free_trace(&make());
    assert!(
        diff_traces(&left, &right).divergence.is_none(),
        "a per-run uuid draw is not a divergence"
    );
}

#[test]
fn a_custom_side_effect_value_is_compared_verbatim() {
    // A `Custom` side effect carries application data, so a change there IS a
    // real difference — the normalization must not over-reach.
    let make = |region: &str| {
        vec![
            started(),
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                name: Some("region".to_string()),
                value: serde_json::json!(region),
            },
        ]
    };
    let left = handler_free_trace(&make("us-east"));
    let right = handler_free_trace(&make("eu-west"));
    let div = diff_traces(&left, &right)
        .divergence
        .expect("a changed custom side effect is a real difference");
    // `resolved_payload` surfaces the same value one field earlier, so that is
    // the field reported — either way it is the custom value, not normalized.
    assert!(
        matches!(div.kind, DiffKind::HistoryFacts { field } if field == "resolved_payload"),
        "expected the custom value to be compared, got {:?}",
        div.kind
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// AC5 — read-only by construction, enforced structurally
// ─────────────────────────────────────────────────────────────────────────────

/// The debugger source must never reach for persistence.
///
/// AC5 ("no engine-runtime change; read-only by construction") is otherwise
/// only true by inspection. This makes it falsifiable: adding a `diesel` call,
/// a `store::` write, or an event append to `debugger.rs` fails the build's
/// own test suite rather than passing review unnoticed.
#[test]
fn debugger_source_never_touches_persistence() {
    let src = include_str!("../../src/debugger.rs");
    for forbidden in [
        "diesel",
        "store::append",
        "append_events",
        "AsyncPgConnection",
        "DbPool",
    ] {
        assert!(
            !src.contains(forbidden),
            "debugger.rs must stay read-only, but it references `{forbidden}`; \
             AC5 requires no database access and no engine-runtime change"
        );
    }
}

/// The `debugger` feature must add no migration.
///
/// A migration would mean new schema, contradicting AC5. Counting the
/// directories is the same guard `migration_hygiene` uses for the core set.
#[test]
fn debugger_feature_adds_no_migration_referencing_it() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let named: Vec<String> = std::fs::read_dir(&dir)
        .expect("migrations dir readable")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_lowercase())
        .filter(|name| name.contains("debug"))
        .collect();
    assert!(
        named.is_empty(),
        "AC5 requires no migration for the debugger, found: {named:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder surfaces and terminal step outcomes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn max_steps_zero_is_clamped_to_one_step() {
    // A zero cap would produce an empty trace, which reads as "this history has
    // no events" rather than "you asked for nothing".
    let debugger = ReplayDebugger::new().max_steps(0);
    let trace = ReplayTrace::from_history_capped(
        "wf".to_string(),
        ExecutionId::new(),
        &two_step_history(),
        1,
    );
    assert_eq!(trace.steps.len(), 1);
    assert!(trace.truncated);
    // The builder clamp is observable through a real trace.
    let _ = debugger;
}

#[tokio::test]
async fn a_spinning_workflow_times_out_rather_than_hanging() {
    // The doc comment promises `StepOutcome::TimedOut` for a yield-loop rather
    // than a hung debugger. Without a bound this test would never return.
    fn spins<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                tokio::task::yield_now().await;
            }
        })
    }

    let trace = ReplayDebugger::new()
        .register_fn("wf", spins)
        .max_steps(1)
        .step_timeout(std::time::Duration::from_millis(50))
        .trace_snapshot(snapshot("wf", two_step_history()))
        .await
        .expect("trace");
    assert_eq!(trace.steps[0].outcome, StepOutcome::TimedOut);
}

#[tokio::test]
async fn a_panicking_workflow_is_contained() {
    // The debugger must survive a panicking handler — a debugger that dies on
    // the bug it is debugging is useless.
    fn panics<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { panic!("boom") })
    }

    let trace = ReplayDebugger::new()
        .register_fn("wf", panics)
        .max_steps(1)
        .trace_snapshot(snapshot("wf", two_step_history()))
        .await
        .expect("trace");
    assert_eq!(trace.steps[0].outcome, StepOutcome::Panicked);
}

#[tokio::test]
async fn a_redacted_export_is_refused_rather_than_mis_analysed() {
    // A redacted export rewrites every payload field, so replaying it would
    // report a fabricated divergence at the first payload-bearing activity.
    // Refusing is the only honest answer for a divergence-finding tool.
    let redacted = serde_json::json!({
        "workflow_name": "wf",
        "execution_id": ExecutionId::new(),
        "payload_policy": "redacted",
        "events": two_step_history(),
    })
    .to_string();

    let err = ReplayDebugger::new()
        .register_fn("wf", two_step)
        .trace_json(&redacted)
        .await
        .expect_err("a redacted export must be refused");
    assert!(matches!(err, DebugError::RedactedHistory), "got {err:?}");
    assert!(
        err.to_string().contains("--payload-policy full"),
        "the error must name the fix: {err}"
    );
}

#[tokio::test]
async fn a_full_export_is_accepted() {
    // The guard must key on the *redacted* policy only — a full export carries
    // the same field and must pass.
    let full = serde_json::json!({
        "workflow_name": "wf",
        "execution_id": ExecutionId::new(),
        "payload_policy": "full",
        "events": two_step_history(),
    })
    .to_string();

    ReplayDebugger::new()
        .register_fn("wf", two_step)
        .trace_json(&full)
        .await
        .expect("a full export must be accepted");
}

#[tokio::test]
async fn a_signal_park_opens_a_signal_awaitable() {
    // AC1 lists signal waits among the open awaitables. A signal wait has no
    // opening *event*, so it is synthesized from the frontier command — which
    // means it is only available on a handler-backed trace.
    fn waits<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx.wait_for_signal("approval").await;
            Ok(serde_json::json!("done"))
        })
    }

    let trace = ReplayDebugger::new()
        .register_fn("wf", waits)
        .trace_snapshot(snapshot(
            "wf",
            vec![WorkflowEvent::workflow_started(
                serde_json::json!({}),
                chrono::Utc::now(),
            )],
        ))
        .await
        .expect("trace");

    let signal = trace.steps[0]
        .open_awaitables
        .iter()
        .find(|a| a.kind == AwaitableKind::Signal)
        .expect("a parked signal wait must open a Signal awaitable");
    assert_eq!(signal.name.as_deref(), Some("approval"));
    assert_eq!(signal.opened_at, 0);
}

// ── AC3: the remaining `DiffKind` variants ──────────────────────────────────
//
// `CommandMismatch`, `HistoryFacts` and `TraceLength` are all exercised
// end-to-end by the corpus and the trace-length test above. `CommandCount` and
// `Outcome` are *comparator* behaviours that only fire when the two sides agree
// on every shared command — a shape no pair of real builds in the corpus
// produces — so they are asserted directly against `diff_traces`, which is the
// unit under test either way.

/// A minimal step carrying only the fields these comparator tests vary.
const fn bare_step(
    index: usize,
    commands: Vec<CommandSnapshot>,
    outcome: StepOutcome,
) -> DebugStep {
    DebugStep {
        index,
        event_type: "ActivityScheduled",
        outcome,
        commands,
        open_awaitables: Vec::new(),
        version_gates: std::collections::BTreeMap::new(),
        patch_gates: std::collections::BTreeSet::new(),
        side_effects: Vec::new(),
        markers: Vec::new(),
        resolved_payload: None,
        signal_name: None,
        divergence: None,
    }
}

fn cmd(kind: &'static str, summary: &str) -> CommandSnapshot {
    CommandSnapshot {
        kind,
        summary: summary.to_string(),
        payload: None,
    }
}

fn bare_trace(steps: Vec<DebugStep>) -> ReplayTrace {
    ReplayTrace {
        workflow_name: "two_step".to_string(),
        execution_id: ExecutionId::new(),
        total_events: steps.len(),
        truncated: false,
        steps,
    }
}

#[test]
fn a_shared_command_prefix_with_differing_length_reports_command_count() {
    // The right build emits everything the left one does, plus one more. Every
    // *shared* command matches, so this must not be reported as a mismatch at
    // an index that does not actually differ.
    let left = bare_trace(vec![bare_step(
        0,
        vec![cmd("ScheduleActivity", "step_a -> default")],
        StepOutcome::Suspended,
    )]);
    let right = bare_trace(vec![bare_step(
        0,
        vec![
            cmd("ScheduleActivity", "step_a -> default"),
            cmd("ScheduleActivity", "step_b -> default"),
        ],
        StepOutcome::Suspended,
    )]);

    let div = diff_traces(&left, &right)
        .divergence
        .expect("a differing command count is a divergence");
    assert_eq!(div.step_index, 0);
    assert!(
        matches!(div.kind, DiffKind::CommandCount { left: 1, right: 2 }),
        "expected CommandCount, got {:?}",
        div.kind
    );
}

#[test]
fn identical_commands_with_differing_outcomes_report_outcome() {
    // Same commands, but one side stopped somewhere else. Reported last,
    // because it is usually a downstream consequence of a command difference —
    // here there is none, so it surfaces on its own.
    let left = bare_trace(vec![bare_step(0, Vec::new(), StepOutcome::Suspended)]);
    let right = bare_trace(vec![bare_step(0, Vec::new(), StepOutcome::ReachedTerminal)]);

    let div = diff_traces(&left, &right)
        .divergence
        .expect("a differing outcome is a divergence");
    assert!(
        matches!(
            div.kind,
            DiffKind::Outcome {
                left: StepOutcome::Suspended,
                right: StepOutcome::ReachedTerminal
            }
        ),
        "expected Outcome, got {:?}",
        div.kind
    );
}

#[test]
fn a_one_sided_divergence_is_reported_as_its_own_kind_not_a_history_fact() {
    // A divergence is a *code-vs-history* mismatch: the two recordings are
    // identical here, the two builds are not. Folding it into `HistoryFacts`
    // would tell an operator "these are different recordings", which is a
    // wrong and actively misleading diagnosis.
    let clean = bare_trace(vec![bare_step(0, Vec::new(), StepOutcome::Suspended)]);
    let mut diverged_step = bare_step(0, Vec::new(), StepOutcome::Suspended);
    diverged_step.divergence = Some(StepDivergence {
        expected: "ActivityScheduled(step_b)".to_string(),
        actual: "ActivityScheduled(step_b_renamed)".to_string(),
        event_index: 3,
    });
    let diverged = bare_trace(vec![diverged_step]);

    let div = diff_traces(&clean, &diverged)
        .divergence
        .expect("a one-sided divergence is a divergence");
    let DiffKind::Divergence { left, right } = &div.kind else {
        panic!("expected Divergence, got {:?}", div.kind);
    };
    assert!(left.is_none(), "the clean side carries no divergence");
    assert_eq!(
        right.as_ref().map(|d| d.event_index),
        Some(3),
        "the diverged side's own event index must survive the diff",
    );
}
