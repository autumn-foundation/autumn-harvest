//! Tests for issue #612 — serving query handlers on terminal (closed) workflows
//! for post-mortem state inspection.
//!
//! These are **pure** tests (no DB, no testcontainers): they exercise the
//! reusable, read-only replay driver `executor::drive_query_replay` and the
//! erasure-detection helper `erase::history_events_contain_tombstone` — the two
//! pieces the plugin's `hydrate_ctx_for_query` composes to decide, for a
//! terminal execution, whether to serve a query (200), report an unregistered
//! name (404), report an unqueryable history (410), or report a spinning replay
//! (408).
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
use autumn_harvest::executor::{QueryReplayOutcome, drive_query_replay};
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
fn insufficient_history_suspends_before_terminal() {
    // Expected 3 items, only 1 activity pair recorded → drive suspends.
    let (exec_id, input, events) = insufficient_history(&["a", "b", "c"], 1);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "a history too short to reach Poll::Ready must classify as Suspended \
         (→ 410 for a terminal execution)"
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
fn erased_history_is_detected() {
    let (_exec_id, _input, mut events) = complete_history(&["a", "b"], false);

    // A clean history is not flagged.
    assert!(
        !erase::history_events_contain_tombstone(&events),
        "a non-erased history must not be flagged as erased"
    );

    // Tombstone the payload fields the way `erase_workflow_payloads` does.
    for event in &mut events {
        let mut value = serde_json::to_value(&*event).expect("event serialises");
        let _ = erase::tombstone_payload_fields(&mut value);
        *event = serde_json::from_value(value).expect("event round-trips");
    }

    assert!(
        erase::history_events_contain_tombstone(&events),
        "a PII-erased history must be detected so a terminal query returns 410, \
         never a tombstoned answer"
    );
}
