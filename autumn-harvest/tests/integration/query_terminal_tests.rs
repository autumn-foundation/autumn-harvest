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

use autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME;
use autumn_harvest::context::{WorkflowContext, WorkflowHistoryPolicy, empty_shared_state};
use autumn_harvest::erase;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::executor::{
    QueryReplayOutcome, TerminalQueryDecision, classify_terminal_query, drive_query_replay,
    drive_query_replay_async, history_reached_terminal_seal,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::{Duration as ChronoDuration, Utc};
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

/// A workflow whose handler panics **during future construction** — the panic
/// unwinds synchronously before the `Box::pin(...)` future is produced (issue
/// #782 / PR #1012 review). Driving it must contain the panic as
/// `QueryReplayOutcome::Panicked` rather than unwind the read-only query caller.
fn construction_panicking_query_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    panic!("boom constructing query workflow future");
}

/// Like [`progress_workflow`], but after processing every item it parks on
/// `ctx.await_condition(|| false)` — a **command-less cold park**: its
/// `Future::poll` returns `Pending` without pushing any workflow command and
/// **without registering the waker** (it ignores `_cx`), so it never self-wakes.
/// This is the exact case the three-way suspension discriminator (issue #612)
/// must classify as an immediate `Suspended` (case 3), *not* busy-drive to the
/// deadline: a RUNNING query over such a park must serve fast, and a terminal run
/// sealed while parked on it must serve its partial state (200), not 408/410.
fn await_condition_workflow<'a>(
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

        // Park forever on a predicate that never holds. `await_condition` pushes
        // no command and never wakes — during a query drive nothing re-polls it.
        ctx.await_condition(|| false).await.map_err(|e| e.to_string())?;
        // Unreachable: the predicate is always false.
        Ok(Value::Null)
    })
}

/// A workflow that spins forever via `tokio::task::yield_now()` — the in-runtime
/// case the old waker-only heuristic misclassified. Inside a tokio runtime
/// `yield_now` DEFERS its wake to the scheduler queue, so the async driver
/// ([`drive_query_replay_async`]) must open its quiet window to observe the wake
/// and keep driving to the deadline (case 2 → `TimedOut` → 408), rather than
/// misread the first `Poll::Pending` as a suspension.
fn spinning_query_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.register_query_handler::<Value, u64, _>("progress", |_req: &Value| Ok(0u64));
        loop {
            tokio::task::yield_now().await;
        }
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
fn construction_phase_panic_during_query_replay_is_contained() {
    // Issue #782 / PR #1012 review: a hand-written handler that panics while
    // *constructing* its future (before returning the boxed future) must be
    // contained by the read-only query driver as `Panicked`, not unwind the
    // caller (the plugin's query handler / axum request task). The poll-time
    // `catch_unwind` cannot reach a construction panic — the construction call is
    // wrapped too.
    let (exec_id, input, events) = complete_history(&["a"], false);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(
        &ctx,
        construction_panicking_query_workflow,
        input,
        QUERY_BUDGET,
    );
    assert_eq!(
        outcome,
        QueryReplayOutcome::Panicked,
        "a construction-phase panic must be contained as Panicked, not escape the caller"
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
        classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()),
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
        classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (CONTINUED_AS_NEW), only the lifecycle seal \
         unconsumed → serve (200)"
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
        classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (CANCELLED), only the lifecycle seal \
         unconsumed → serve (200)"
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
        classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (TIMED_OUT), only the lifecycle seal \
         unconsumed → serve (200)"
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
fn running_await_condition_park_suspends_fast_and_serves_partial_state() {
    // Issue #612 core bug proof (RED against the old `any_pending_command` guard).
    // A RUNNING workflow that processed all its recorded activities and then
    // parked on `ctx.await_condition(|| false)` — a COMMAND-LESS COLD PARK: it
    // pushes no workflow command and never registers its waker, so nothing
    // re-polls it during a query drive. The three-way discriminator must classify
    // it as an immediate `Suspended` (case 3) so the query serves the partial
    // state FAST, rather than busy-driving to `query_timeout` (which the old
    // `!flag && any_pending_command(...)` guard did: no command was ever pushed,
    // so the peek was false, and the drive fell to the deadline).
    let (exec_id, input, events) = complete_history(&["a", "b", "c"], false);
    assert!(
        !history_reached_terminal_seal(&events),
        "no terminal event → this is the RUNNING path"
    );
    let ctx = build_ctx(exec_id, events);

    let started = std::time::Instant::now();
    let outcome = drive_query_replay(&ctx, await_condition_workflow, input, QUERY_BUDGET);
    let elapsed = started.elapsed();

    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "a command-less `await_condition` cold park must classify Suspended \
         immediately (case 3), not drive to the deadline"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "the drive must return fast (well under the {QUERY_BUDGET:?} budget); \
         the old any_pending_command guard would busy-drive the full budget — \
         elapsed was {elapsed:?}"
    );

    // Running path: a Suspended outcome still serves the reconstructed partial
    // state (the query is answerable; it is NEVER turned into a 410 here).
    let result = ctx
        .execute_query("progress")
        .expect("progress must be registered before await_condition parks");
    assert_eq!(
        result,
        json!(3),
        "query reads the count reached at the await_condition park point"
    );
}

#[test]
fn terminal_sealed_while_await_condition_parked_serves_partial_state() {
    // Issue #612: a run the engine sealed (TIMED_OUT) while its function was
    // parked command-less on `await_condition`. The drive parks at
    // await_condition → Suspended, but the history carries a terminal lifecycle
    // seal and all non-lifecycle history is consumed, so the terminal-query
    // classifier must Serve (200) the reconstructed partial state — NOT 408
    // (which the old any_pending_command guard produced by driving to the
    // deadline) and NOT 410.
    let (exec_id, input, mut events) = complete_history(&["a", "b"], false);
    events.push(WorkflowEvent::WorkflowExecutionTimedOut {
        deadline: Utc::now() - ChronoDuration::seconds(1),
        timed_out_at: Utc::now(),
    });
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowExecutionTimedOut history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, await_condition_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "await_condition parks forever → Suspended, not ReachedTerminal"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed, ctx.history_has_unconsumed_events()),
        TerminalQueryDecision::Serve,
        "Suspended + terminal seal (TIMED_OUT), only the lifecycle seal \
         unconsumed → serve (200), never 408/410"
    );
    assert_eq!(
        ctx.execute_query("progress").expect("progress registered"),
        json!(2),
        "partial count at the recorded terminal point (both activities completed)"
    );
}

#[test]
fn genuine_command_suspension_serves_partial_state() {
    // Case 1 of the three-way discriminator: the workflow parks on an UNRECORDED
    // activity, which pushes a replay-significant command on THAT poll (a positive
    // per-poll command delta). This must classify Suspended and remain answerable
    // — distinct from the zero-delta cold-park (case 3) and spin (case 2) paths.
    let (exec_id, input, events) = insufficient_history(&["a", "b", "c"], 1);
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "an unrecorded activity emits a command this poll → CommandSuspension"
    );
    assert_eq!(
        ctx.execute_query("progress").expect("progress registered"),
        json!(1),
        "one recorded activity processed before the command-suspension"
    );
}

#[tokio::test]
async fn async_driver_await_condition_cold_park_classifies_suspended_in_runtime() {
    // Async-driver counterpart to the sync cold-park test, run INSIDE a tokio
    // runtime. `drive_query_replay_async` opens a `yield_now().await` quiet window
    // on a zero-delta poll to observe tokio's deferred `yield_now` wake; a
    // command-less `await_condition` park never wakes, so after the quiet window
    // the flag is still cold → case 3 → Suspended immediately (fast serve), NOT
    // misread as a self-wake spin.
    let (exec_id, input, events) = complete_history(&["a", "b"], false);
    let ctx = build_ctx(exec_id, events);

    let outcome =
        drive_query_replay_async(&ctx, await_condition_workflow, input, QUERY_BUDGET).await;
    assert_eq!(
        outcome,
        QueryReplayOutcome::Suspended,
        "in a runtime, a command-less await_condition park classifies Suspended \
         (case 3), not busy-driven to the deadline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_driver_in_runtime_yield_now_spin_drives_to_deadline() {
    // The exact in-runtime scenario the old waker-only PR-D heuristic got wrong,
    // now covered WITHOUT testcontainers. `spinning_query_workflow` loops on
    // `tokio::task::yield_now()`, whose wake tokio DEFERS to the scheduler queue
    // inside a runtime. The async driver's quiet-window `yield_now().await`
    // flushes that deferred queue → the flag flips → case 2 → keep driving to the
    // deadline → TimedOut (→ 408). A `multi_thread` runtime also makes the drive
    // future's `Send`-ness a compile-time regression guard.
    let (exec_id, input, events) = complete_history(&["a"], false);
    let ctx = build_ctx(exec_id, events);

    // A small budget bounds the spin; the driver re-checks the deadline at the
    // top of each cycle and yields the runtime between polls, so it never hangs
    // or starves other tasks.
    let outcome =
        drive_query_replay_async(&ctx, spinning_query_workflow, input, Duration::from_millis(50))
            .await;
    assert_eq!(
        outcome,
        QueryReplayOutcome::TimedOut,
        "an in-runtime yield_now spin must drive to the deadline (case 2 → 408), \
         not classify Suspended (which the old waker-only heuristic did → 410)"
    );
}

#[test]
fn code_drift_reaching_terminal_early_with_unconsumed_history_is_gone() {
    // Codex P2 (PR #986 follow-up): the recorded history was produced by code
    // that ran THREE activities, but the handler driving replay now completes
    // after only ONE (a code change / drift since the run executed). The drive
    // reaches Poll::Ready (ReachedTerminal) while two recorded activity pairs
    // remain unconsumed — the reconstructed state does NOT correspond to what
    // actually happened, so the terminal-query classifier must return
    // HistoryUnavailable (→ 410), never a misleading partial answer.
    let (exec_id, _recorded_input, events) = complete_history(&["a", "b", "c"], false);
    let sealed = history_reached_terminal_seal(&events);
    assert!(
        !sealed,
        "a plain completed-pair history carries no terminal seal"
    );
    let ctx = build_ctx(exec_id, events);

    // Drive with a SHORTER input: the drifted handler processes only one item,
    // matching the first recorded activity pair and then completing early.
    let drift_input = workflow_input(&["a"], false);
    let outcome = drive_query_replay(&ctx, progress_workflow, drift_input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::ReachedTerminal,
        "the drifted handler completes early (Poll::Ready) after one activity"
    );
    let has_unconsumed = ctx.history_has_unconsumed_events();
    assert!(
        has_unconsumed,
        "two recorded activity pairs were never consumed by the drifted handler"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed, has_unconsumed),
        TerminalQueryDecision::HistoryUnavailable,
        "ReachedTerminal + genuine unconsumed non-lifecycle history → 410 (code drift)"
    );
}

#[test]
fn completed_history_fully_consumed_still_serves() {
    // AC2 non-regression: a faithfully-replayed completed run drives to
    // Poll::Ready with NO unconsumed non-lifecycle history — the trailing
    // WorkflowCompleted seal is excluded by has_non_lifecycle_unconsumed — so the
    // drift gate does not fire and the query still Serves (200). This is the
    // "trailing-lifecycle exclusion is why a truthfully-replayed sealed run still
    // Serves" property the drift gate must not break.
    let (exec_id, input, mut events) = complete_history(&["a", "b", "c"], false);
    events.push(WorkflowEvent::WorkflowCompleted {
        output: json!({ "processed": 3 }),
    });
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowCompleted history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, progress_workflow, input, QUERY_BUDGET);
    assert_eq!(outcome, QueryReplayOutcome::ReachedTerminal);
    let has_unconsumed = ctx.history_has_unconsumed_events();
    assert!(
        !has_unconsumed,
        "a faithful full replay leaves only the trailing terminal seal, which is \
         excluded — so no genuine unconsumed history remains"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed, has_unconsumed),
        TerminalQueryDecision::Serve,
        "ReachedTerminal + only trailing lifecycle events → still Serve (AC2 non-regression)"
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

/// A workflow that registers a **push-based signal handler** (issue #546) plus a
/// query reporting the handler's mutated state, and then completes with **no**
/// further cursor-advancing call (no activity / timer / signal wait). Because
/// nothing advances the matcher cursor after registration, no `match_history`
/// post-hook pump fires during the drive — the recorded `SignalReceived` is only
/// consumed by the end-of-cycle signal-handler flush.
fn signal_handler_only_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let cancelled = Arc::new(Mutex::new(false));
        let write = cancelled.clone();
        ctx.register_signal_handler_raw("cancel", move |_payload: Value| {
            *write.lock().expect("flag lock poisoned") = true;
        });
        let read = cancelled;
        ctx.register_query_handler::<Value, bool, _>("cancelled", move |_req: &Value| {
            Ok(*read.lock().expect("flag lock poisoned"))
        });
        // Complete immediately — no activity/timer/signal-wait after registration.
        Ok(Value::Null)
    })
}

#[test]
fn signal_handler_completed_run_serves_not_gone() {
    // Codex P2 (PR #993): a terminal/COMPLETED run that registered a push signal
    // handler, received a `SignalReceived`, and completed WITHOUT any further
    // cursor-advancing call. `drive_query_replay` must mirror the executor's
    // completion-time signal-handler flush so the recorded signal is claimed
    // before the caller's `history_has_unconsumed_events()` drift check — a
    // truthfully replayed run must classify as Serve (200), never
    // HistoryUnavailable (410).
    //
    // Before the fix: the drive returned ReachedTerminal with the `SignalReceived`
    // still unconsumed, so `history_has_unconsumed_events()` was true and the
    // classifier returned HistoryUnavailable (410).
    let exec_id = ExecutionId::new();
    let input = Value::Null;
    let events = vec![
        started_event(input.clone()),
        WorkflowEvent::SignalReceived {
            signal_name: "cancel".into(),
            payload: json!({ "reason": "operator" }),
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowCompleted history is sealed");
    let ctx = build_ctx(exec_id, events);

    let outcome = drive_query_replay(&ctx, signal_handler_only_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::ReachedTerminal,
        "the handler registers and returns → Poll::Ready"
    );

    // Exactly the plugin's ordering: classify with the post-drive drift flag.
    let has_unconsumed = ctx.history_has_unconsumed_events();
    assert!(
        !has_unconsumed,
        "the completion-time flush must claim the push-handler's SignalReceived, \
         so no genuine unconsumed non-lifecycle history remains"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed, has_unconsumed),
        TerminalQueryDecision::Serve,
        "a truthfully replayed signal-handler run must Serve (200), not 410"
    );

    // The flush also reconstructs the handler's effect on internal state, so the
    // post-mortem query reflects the processed signal.
    assert_eq!(
        ctx.execute_query("cancelled")
            .expect("cancelled query must be registered"),
        json!(true),
        "the push handler fired during the flush, so the query reads the mutated state"
    );
}

// ── Issue #772 (round 6): deadline-aware continue-as-new × query replay ──────
//
// A workflow with an `execution_timeout` that calls `should_continue_as_new()`
// records a reserved `SideEffectRecorded { kind: Now, name:
// "__harvest_deadline_probe" }` sentinel on the live frontier (the deadline
// branch's tolerant clock read, #772). A query replay context that does NOT
// thread the execution's `execution_timeout`/`deadline_at` returns from the
// deadline guard *before* the tolerant clock read, so the recorded probe is left
// UNCONSUMED at the cursor — the next replayed command (or the terminal-query
// drift check) then treats an otherwise valid history as divergent. These pure
// tests pin the mechanism the plugin's `hydrate_ctx_for_query` and the
// in-process `execute_query_in_process` path both depend on: threading the
// budget makes the probe replay cleanly.

/// Registers a `progress` query, performs the deadline-aware checkpoint check
/// (recording/consuming the `__harvest_deadline_probe` when the run has an
/// `execution_timeout`), then processes each item via an activity. Never
/// actually continues-as-new in these fixtures (the deadline is far in the
/// future and history is tiny, so `should_continue_as_new()` returns `false`).
fn deadline_probe_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let processed = Arc::new(Mutex::new(0u64));
        let state = processed.clone();
        ctx.register_query_handler::<Value, u64, _>("progress", move |_req: &Value| {
            Ok(*state.lock().expect("counter lock poisoned"))
        });

        // Deadline-aware checkpoint: reads (and, when threaded, consumes) the
        // reserved deadline-probe side-effect. Must not trip in these fixtures.
        if ctx.should_continue_as_new() {
            return Err("fixture must not trip continue-as-new".to_string());
        }

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
        Ok(json!({ "processed": *processed.lock().expect("counter lock poisoned") }))
    })
}

/// A start time far enough in the past that a 24h budget is nowhere near
/// consumed, so `should_continue_as_new()`'s deadline branch never trips.
const fn probe_t0() -> chrono::DateTime<Utc> {
    let Some(t0) = chrono::DateTime::from_timestamp_millis(1_700_000_000_000) else {
        panic!("valid probe start instant")
    };
    t0
}

/// `WorkflowStarted` + the reserved deadline probe + one scheduled/completed
/// pair per item + `WorkflowCompleted` — a terminal history whose replay MUST
/// consume the probe (via a budget-threaded context) to reach `Poll::Ready`
/// cleanly. Returns the timeout/deadline the run was recorded with so the caller
/// can thread them exactly as `hydrate_ctx_for_query` does from the row.
fn deadline_probe_history(
    items: &[&str],
) -> (
    ExecutionId,
    Value,
    Vec<WorkflowEvent>,
    ChronoDuration,
    chrono::DateTime<Utc>,
) {
    let exec_id = ExecutionId::new();
    let t0 = probe_t0();
    let budget = ChronoDuration::hours(24);
    let deadline = t0 + budget;
    let recorded_now = t0 + ChronoDuration::seconds(1);
    let input = json!({ "items": items });
    let mut events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: t0,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Now,
            name: Some(DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: json!(recorded_now.timestamp_millis()),
        },
    ];
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
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    (exec_id, input, events, budget, deadline)
}

/// Build a replay context that threads the execution row's
/// `execution_timeout`/`deadline_at` exactly as the fixed query hydration paths
/// (`hydrate_ctx_for_query`, `execute_query_in_process`) do (#772 round 6).
fn build_ctx_with_deadline(
    exec_id: ExecutionId,
    events: Vec<WorkflowEvent>,
    execution_timeout: Option<ChronoDuration>,
    deadline_at: Option<chrono::DateTime<Utc>>,
) -> WorkflowContext {
    WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        events,
        empty_shared_state(),
        WorkflowHistoryPolicy::default(),
    )
    .with_execution_timeout(execution_timeout)
    .with_deadline(deadline_at)
}

#[test]
fn deadline_probe_history_replays_and_serves_when_budget_is_threaded() {
    // GREEN: a context that threads the execution_timeout/deadline_at (the fix)
    // consumes the recorded __harvest_deadline_probe, drives to Poll::Ready with
    // no leftover unconsumed history, and serves the reconstructed count.
    let (exec_id, input, events, budget, deadline) = deadline_probe_history(&["a", "b", "c"]);
    let sealed = history_reached_terminal_seal(&events);
    assert!(sealed, "a WorkflowCompleted history is sealed");

    let ctx = build_ctx_with_deadline(exec_id, events, Some(budget), Some(deadline));
    let outcome = drive_query_replay(&ctx, deadline_probe_workflow, input, QUERY_BUDGET);
    assert_eq!(
        outcome,
        QueryReplayOutcome::ReachedTerminal,
        "with the budget threaded, the deadline probe is consumed and the drive completes"
    );

    let has_unconsumed = ctx.history_has_unconsumed_events();
    assert!(
        !has_unconsumed,
        "the probe was consumed, so no genuine non-lifecycle history remains unconsumed"
    );
    assert_eq!(
        classify_terminal_query(outcome, sealed, has_unconsumed),
        TerminalQueryDecision::Serve,
        "a budget-threaded terminal query with a deadline probe must Serve (200), not 410"
    );
    assert_eq!(
        ctx.execute_query("progress")
            .expect("progress must be registered"),
        json!(3),
        "the query reports the fully reconstructed count across the probe"
    );
}

#[test]
fn deadline_probe_history_diverges_when_budget_is_not_threaded() {
    // RED (pre-fix behavior): a context that does NOT thread the budget — exactly
    // what the query hydration paths did before #772 round 6 — leaves the recorded
    // deadline probe UNCONSUMED, so the terminal-query drift check sees leftover
    // non-lifecycle history and classifies the otherwise-valid history as 410.
    // This pins WHY threading is required at the hydration sites.
    let (exec_id, input, events, _budget, _deadline) = deadline_probe_history(&["a", "b", "c"]);
    let sealed = history_reached_terminal_seal(&events);

    let ctx = build_ctx(exec_id, events); // no execution_timeout / deadline_at
    let outcome = drive_query_replay(&ctx, deadline_probe_workflow, input, QUERY_BUDGET);
    let has_unconsumed = ctx.history_has_unconsumed_events();
    assert_ne!(
        classify_terminal_query(outcome, sealed, has_unconsumed),
        TerminalQueryDecision::Serve,
        "without the budget threaded, the unconsumed deadline probe must NOT Serve — \
         this is the divergence the fix eliminates"
    );
}
