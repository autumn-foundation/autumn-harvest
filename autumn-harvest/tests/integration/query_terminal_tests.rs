//! Tests for issue #612 — serving query handlers on terminal (closed) workflows
//! for post-mortem state inspection.
//!
//! These are **pure** tests (no DB, no testcontainers): they exercise the
//! reusable, read-only replay driver `executor::drive_query_replay`, the pure
//! terminal-query classifier `executor::classify_terminal_query` +
//! `executor::history_reached_terminal_seal`, and the O(1) erasure row-check
//! `erase::execution_input_is_erased` — the pieces the plugin's
//! `hydrate_ctx_for_query` composes to decide, for a terminal execution, whether
//! to serve a query (200), report an unregistered name (404), report an
//! unqueryable history (410), or report a spinning replay (408).
//!
//! Crucially, a run the engine seals while its function is parked mid-command
//! (`TIMED_OUT`, `CONTINUED_AS_NEW`, mid-await `CANCELLED`/`FAILED`) replays to
//! a `Suspended` outcome, **not** `Poll::Ready` — yet its history carries a
//! terminal lifecycle event, so it must still serve 200, not 410. These tests
//! pin exactly that.
//!
//! The HTTP status mapping (200/404/410/408) and the zero-writes guarantee are
//! covered by `autumn-harvest-plugin/tests/query_integration.rs` (testcontainers,
//! compile-checked in this sandbox).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::context::{WorkflowContext, WorkflowHistoryPolicy, empty_shared_state};
use autumn_harvest::erase;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{
    QueryReplayOutcome, TerminalQueryDecision, classify_terminal_query, drive_query_replay,
    history_reached_terminal_seal,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::{Value, json};

// ── Test workflow handler ─────────────────────────────────────────────────

/// A workflow that processes each item from its input via an activity,
/// incrementing an internal counter, and registers a `progress` query that
/// reports that counter. If `should_fail` is set in the input it returns `Err`
/// **after** processing every item (so its final internal state is preserved
/// regardless of the error).
fn progress_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let processed = Arc::new(Mutex::new(0u64));
        let state = processed.clone();
        ctx.register_query_handler::<Value, u64, _>("progress", move |_req: &Value| {
            Ok(*state.lock().expect("counter lock poisoned"))
        });

        let items = input
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &items {
            ctx.execute_activity_raw("process_item", item.clone(), "default")
                .await
                .map_err(|e| e.to_string())?;
            *processed.lock().expect("counter lock poisoned") += 1;
        }

        if input
            .get("should_fail")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err("deliberate failure after processing all items".to_string());
        }
        Ok(json!({ "processed": *processed.lock().expect("counter lock poisoned") }))
    })
}

/// Like [`progress_workflow`], but instead of returning it calls
/// `ctx.continue_as_new(...)` after processing every item. `continue_as_new`
/// parks the function forever (it never returns), so driving this workflow
/// always classifies as `Suspended` — even though the run is terminal
/// (`CONTINUED_AS_NEW`) and its history carries the terminal seal.
fn continue_as_new_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let processed = Arc::new(Mutex::new(0u64));
        let state = processed.clone();
        ctx.register_query_handler::<Value, u64, _>("progress", move |_req: &Value| {
            Ok(*state.lock().expect("counter lock poisoned"))
        });

        let items = input
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &items {
            ctx.execute_activity_raw("process_item", item.clone(), "default")
                .await
                .map_err(|e| e.to_string())?;
            *processed.lock().expect("counter lock poisoned") += 1;
        }

        ctx.continue_as_new(json!({ "carry": *processed.lock().expect("counter lock poisoned") }))
            .await
            .map_err(|e| e.to_string())?;
        // Unreachable: continue_as_new parks the function forever.
        Ok(Value::Null)
    })
}

// ── History builders ──────────────────────────────────────────────────────

fn workflow_input(items: &[&str], should_fail: bool) -> Value {
    json!({ "items": items, "should_fail": should_fail })
}

fn started_event(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// A full history: `WorkflowStarted` followed by one scheduled+completed pair
/// per item — enough to drive `progress_workflow` to `Poll::Ready`. Returns the
/// input the history was produced with so the drive replays deterministically.
fn complete_history(items: &[&str], should_fail: bool) -> (ExecutionId, Value, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let input = workflow_input(items, should_fail);
    let mut events = vec![started_event(input.clone())];
    for item in items {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "process_item".into(),
            input: json!(item),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    (exec_id, input, events)
}

/// A deliberately-short history: the workflow expects `expected` items but only
/// `available` activity pairs are recorded, so the drive suspends before
/// `Poll::Ready`.
fn insufficient_history(
    expected: &[&str],
    available: usize,
) -> (ExecutionId, Value, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let input = workflow_input(expected, false);
    let mut events = vec![started_event(input.clone())];
    for item in expected.iter().take(available) {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "process_item".into(),
            input: json!(item),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    (exec_id, input, events)
}

/// A `continue_as_new_workflow` history: full activity pairs for every item,
/// then the `WorkflowContinuedAsNew` terminal seal. Driving parks at
/// `continue_as_new` (→ `Suspended`), but the history reached its terminal seal.
fn continued_as_new_history(items: &[&str]) -> (ExecutionId, Value, Vec<WorkflowEvent>) {
    let (exec_id, input, mut events) = complete_history(items, false);
    events.push(WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id: ExecutionId::new(),
        input: json!({ "carry": items.len() }),
    });
    (exec_id, input, events)
}

/// A history the engine sealed while a workflow was parked mid-activity: full
/// pairs for `completed`, then a bare `ActivityScheduled` for `pending` (no
/// completion), then the `seal` terminal event. Driving `progress_workflow` with
/// `items = completed ++ [pending]` processes `completed` (counter =
/// `completed.len()`), then parks on the incomplete activity (→ `Suspended`),
/// so the reconstructed partial count is `completed.len()`.
fn sealed_mid_activity_history(
    completed: &[&str],
    pending: &str,
    seal: WorkflowEvent,
) -> (ExecutionId, Value, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let mut items: Vec<&str> = completed.to_vec();
    items.push(pending);
    let input = workflow_input(&items, false);
    let mut events = vec![started_event(input.clone())];
    for item in completed {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "process_item".into(),
            input: json!(item),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    // A trailing scheduled-but-never-completed activity: the workflow parks here.
    events.push(WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "process_item".into(),
        input: json!(pending),
        queue: "default".into(),
    });
    events.push(seal);
    (exec_id, input, events)
}

fn build_ctx(exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> WorkflowContext {
    WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        events,
        empty_shared_state(),
        WorkflowHistoryPolicy::default(),
    )
}

/// The generous per-query replay budget used by the "should reach terminal"
/// tests. The 10k-event replay budget is < 200ms; 5s is the production default.
const QUERY_BUDGET: Duration = Duration::from_secs(5);

// ── Tests ─────────────────────────────────────────────────────────────────

#[test]
fn completed_history_drives_to_terminal_and_query_reports_final_count() {
    let (exec_id, input, events) = complete_history(&["a", "b", "c"], false);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::ReachedTerminal,
        "a full history must drive to Poll::Ready"
    );

    let result = ctx
        .execute_query("progress")
        .expect("progress must be registered");
    assert_eq!(
        result,
        json!(3),
        "query reports the final reconstructed count"
    );
}

#[test]
fn failed_terminal_history_serves_internal_state_not_the_error() {
    // Workflow returns Err after processing all 2 items — the query must still
    // report the internal count (2), never the error string.
    let (exec_id, input, events) = complete_history(&["x", "y"], true);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::ReachedTerminal,
        "an Err return still reaches Poll::Ready (final state reconstructed)"
    );

    let result = ctx
        .execute_query("progress")
        .expect("progress must be registered");
    assert_eq!(
        result,
        json!(2),
        "post-mortem query reads computed internal state, not the failure string"
    );
}

#[test]
fn truncated_history_with_no_seal_classifies_as_history_unavailable() {
    // Expected 3 items, only 1 activity pair recorded AND no terminal lifecycle
    // event → the history was genuinely truncated (pruned by retention / released
    // on reset). Drive suspends, the seal check is false, so the terminal-query
    // classifier returns HistoryUnavailable (→ 410).
    let (exec_id, input, events) = insufficient_history(&["a", "b", "c"], 1);
    let sealed = history_reached_terminal_seal(&events);
    assert!(
        !sealed,
        "a truncated history with no terminal event must NOT be treated as sealed"
    );
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::Suspended);
    assert_eq!(
        classify_terminal_query(outcome, sealed),
        TerminalQueryDecision::HistoryUnavailable,
        "Suspended + no terminal seal → 410 (pruned/released)"
    );
}

#[test]
fn continued_as_new_sealed_history_serves_partial_state_not_gone() {
    // continue_as_new parks the function forever → Suspended, but the recorded
    // history ends with WorkflowContinuedAsNew (a terminal seal), so the query
    // must serve the reconstructed partial state (200), NEVER 410.
    let (exec_id, input, events) = continued_as_new_history(&["a", "b"]);
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowContinuedAsNew history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, continue_as_new_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "continue_as_new parks forever → Suspended, not ReachedTerminal"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (CONTINUED_AS_NEW) → serve (200)"
    );

    let result = ctx
        .execute_query("progress")
        .expect("progress must be registered before continue_as_new parks");
    assert_eq!(
        result,
        json!(2),
        "query reads the count reached at the continue_as_new point"
    );
}

#[test]
fn cancelled_sealed_mid_activity_serves_partial_state() {
    // A run externally/hard-cancelled while parked on an in-flight activity:
    // history has one completed pair then a bare ActivityScheduled, sealed by
    // WorkflowCancelled. Drive parks on the incomplete activity → Suspended, but
    // the seal is present → serve the partial count (1).
    let seal = WorkflowEvent::WorkflowCancelled {
        reason: "operator cancelled mid-flight".into(),
    };
    let (exec_id, input, events) = sealed_mid_activity_history(&["a"], "b", seal);
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowCancelled history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::Suspended);
    assert_eq!(
        classify_terminal_query(outcome, sealed),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (CANCELLED) → serve (200)"
    );
    assert_eq!(
        ctx.execute_query("progress").expect("progress registered"),
        json!(1),
        "partial count at the recorded terminal point (one activity completed)"
    );
}

#[test]
fn timed_out_sealed_mid_activity_serves_partial_state() {
    // A run the execution-timeout scanner sealed while parked on an in-flight
    // activity: history sealed by WorkflowExecutionTimedOut. Same shape/behaviour
    // as the cancelled case.
    let seal = WorkflowEvent::WorkflowExecutionTimedOut {
        deadline: Utc::now(),
        timed_out_at: Utc::now(),
    };
    let (exec_id, input, events) = sealed_mid_activity_history(&["a"], "b", seal);
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowExecutionTimedOut history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::Suspended);
    assert_eq!(
        classify_terminal_query(outcome, sealed),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (TIMED_OUT) → serve (200)"
    );
    assert_eq!(
        ctx.execute_query("progress").expect("progress registered"),
        json!(1),
        "partial count at the recorded terminal point"
    );
}

#[test]
fn past_deadline_classifies_as_timed_out_deterministically() {
    let (exec_id, input, events) = complete_history(&["a", "b", "c"], false);
    let ctx = build_ctx(exec_id, events);

    // A zero-length budget forces the deadline check before the first poll.
    let outcome = drive_query_replay(&ctx, progress_workflow, input, Duration::ZERO);
    assert_eq!(
        outcome,
        QueryReplayOutcome::TimedOut,
        "an already-elapsed deadline must classify as TimedOut, not hang \
         (→ 408 for a terminal execution)"
    );
}

#[test]
fn unregistered_query_on_terminal_is_not_found() {
    let (exec_id, input, events) = complete_history(&["a"], false);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::ReachedTerminal);

    let err = ctx
        .execute_query("nonexistent")
        .expect_err("an unregistered name must not silently succeed");
    assert!(
        matches!(err, HarvestError::QueryHandlerNotFound(_)),
        "unregistered query on a terminal run → QueryHandlerNotFound (→ 404), got {err:?}"
    );
}

#[test]
fn running_partial_history_still_serves_query_without_error() {
    // Regression guard: the running-workflow path stops at first suspension and
    // serves the current partial state. A Suspended outcome must NOT prevent the
    // ctx from answering the query (that is what the running path relies on, and
    // it must never be turned into a 410).
    let (exec_id, input, events) = insufficient_history(&["a", "b", "c"], 2);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::Suspended);

    // The handler was registered and the counter reached 2 before suspending.
    let result = ctx
        .execute_query("progress")
        .expect("partial replay still exposes the registered query");
    assert_eq!(
        result,
        json!(2),
        "running path serves partial internal state"
    );
}

#[test]
fn erased_execution_row_is_detected() {
    // A clean execution-row input is not flagged.
    let clean_input = workflow_input(&["a", "b"], false);
    assert!(
        !erase::execution_input_is_erased(&clean_input),
        "a non-erased row input must not be flagged as erased"
    );

    // `erase_workflow_payloads` always tombstones the row's own `input` column
    // to exactly this value — the O(1) authoritative signal a terminal query
    // uses to return 410 rather than replay a tombstoned history.
    let erased_input = erase::erasure_tombstone();
    assert!(
        erase::execution_input_is_erased(&erased_input),
        "a PII-erased row must be detected so a terminal query returns 410, \
         never a tombstoned answer"
    );
}
