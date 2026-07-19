//! Workflow executor -- runs a single workflow function through replay + live execution.
//!
//! The executor builds a [`WorkflowContext`] from the event history, runs the
//! handler with a short timeout, and classifies the outcome:
//!
//! - **Completed**: handler returned `Ok(output)`.
//! - **Failed**: handler returned `Err(error)`.
//! - **Suspended**: handler blocked on a oneshot (waiting for activity/timer resolution).
//!
//! This module is pure async logic and does NOT require the `db` feature.

use std::time::Duration;

use serde_json::Value;
use tracing::Instrument;

use crate::context::{
    SharedState, WorkflowCommand, WorkflowContext, WorkflowHistoryPolicy, empty_shared_state,
};
use crate::event::WorkflowEvent;
use crate::info::{QueryHandlerInfo, UpdateHandlerInfo, WorkflowHandlerFn};
use crate::telemetry::{
    ATTR_EXECUTION_ID, ATTR_QUEUE, ATTR_REPLAY, ATTR_SHARD_ID, ATTR_WORKFLOW_ID, MetricsRecorder,
    NoOpMetrics,
};
use crate::types::ExecutionId;

/// The outcome of running a workflow function through the executor.
#[derive(Debug)]
pub enum WorkflowOutcome {
    /// The workflow ran to completion and returned a value.
    Completed {
        /// The final result serialized as JSON.
        output: Value,
        /// Signals delivered into history but never consumed by the workflow
        /// at this terminal outcome, keyed by signal name → occurrence count
        /// (issue #684). Computed by the executor's terminal arms from the
        /// driven matcher (the only place the authoritative consumed-set
        /// exists) and carried out to the worker, which emits
        /// `harvest.signal.unhandled` **at the same site and under the same
        /// suppressions as `record_workflow_terminal`** (#519) — downstream of
        /// the #603 ND-block gate and the fast-path pause discard. Empty for
        /// every non-terminal-arm construction.
        unhandled_signals: std::collections::BTreeMap<String, u64>,
    },
    /// The workflow function returned an error.
    Failed {
        /// The string description of the error encountered.
        error: String,
        /// Structured details if the error is a non-determinism divergence.
        non_deterministic_details: Option<crate::error::NonDeterministicDetails>,
        /// `true` only when this failure is a **contained handler panic** the
        /// engine caught at the dispatch boundary (issue #782), as opposed to an
        /// author-returned `Err`. This drives the worker's non-terminal
        /// panic-retry gate; it is executor-internal state and is never
        /// persisted. `error` already carries the typed `HandlerPanic` envelope
        /// in that case, so the string channel alone is deliberately **not**
        /// used to gate retry (an author who fabricates a `HandlerPanic`
        /// error-type string reaches this variant with `handler_panic == false`
        /// and never triggers the panic-retry loop).
        handler_panic: bool,
        /// Unconsumed delivered signals at this terminal outcome (issue #684).
        /// See [`WorkflowOutcome::Completed::unhandled_signals`]. Populated on
        /// the executor's `Ok(Err)` / deferred-nd-reroute terminal arms; a
        /// `Failed { non_deterministic_details: Some(_) }` ND-block outcome
        /// carries it too but is diverted by the worker's #603 gate before the
        /// emission site, so it is never counted.
        unhandled_signals: std::collections::BTreeMap<String, u64>,
    },
    /// The workflow suspended awaiting activity results or timer firings.
    /// The accumulated commands describe what the worker needs to schedule.
    Suspended {
        /// A list of commands representing the side effects (e.g. activities) requested.
        commands: Vec<WorkflowCommand>,
    },
    /// The workflow signalled `continue_as_new`. The current execution is
    /// terminal and the worker should atomically start a fresh execution
    /// with the same logical `WorkflowId` but a new `ExecutionId`, passing
    /// `input` as the initial payload.
    ContinuedAsNew {
        /// JSON payload to pass to the next iteration of the workflow.
        input: Value,
    },
}

/// Default timeout for detecting suspension -- if the workflow hasn't completed
/// within this window, it's blocked on a oneshot channel (suspended).
const SUSPENSION_TIMEOUT: Duration = Duration::from_millis(100);

/// Outcome of running a workflow handler future for one executor cycle with
/// panic containment (issue #782).
///
/// Every executor entry point runs the handler through
/// [`run_workflow_handler_cycle`], which wraps it in `catch_unwind` **inside**
/// the [`SUSPENSION_TIMEOUT`] so a handler that unwinds (panics) is contained
/// rather than crashing the spawned worker task and leaving its
/// `harvest_task_queue` row stuck `RUNNING`.
enum HandlerCycleResult {
    /// The handler returned within the suspension timeout (`Ok`/`Err`).
    Returned(Result<Value, String>),
    /// The suspension timeout elapsed — the handler is parked on a oneshot
    /// (this is the normal suspension signal, not an error).
    Suspended,
    /// The handler panicked; the payload was caught and extracted to a message.
    Panicked(String),
}

/// Run a workflow handler future for one executor cycle, containing any panic.
///
/// Mirrors the pre-#782 `tokio::time::timeout(SUSPENSION_TIMEOUT, handler(...))`
/// call exactly for the non-panic paths (`Returned`/`Suspended`), but a panic
/// during any poll — including a poll during the post-await tail — is caught and
/// returned as [`HandlerCycleResult::Panicked`] instead of unwinding the caller.
///
/// `catch_unwind` requires `AssertUnwindSafe` because `&WorkflowContext` is not
/// `UnwindSafe`; this is sound here because the context is discarded after the
/// cycle (the same assertion the synchronous query/update dispatch sites already
/// make).
async fn run_workflow_handler_cycle(
    ctx: &WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
) -> HandlerCycleResult {
    use futures::FutureExt as _;
    // Issue #782 (PR #1012 review): contain a panic during future *construction*.
    // The `catch_unwind` below wraps only the future's poll; a hand-written
    // handler that does synchronous work before returning its boxed future would
    // panic here, before the future exists, and escape the poll-time guard.
    let handler_fut = match crate::error::catch_construct(|| handler(ctx, input)) {
        Ok(fut) => fut,
        Err(message) => return HandlerCycleResult::Panicked(message),
    };
    match tokio::time::timeout(
        SUSPENSION_TIMEOUT,
        std::panic::AssertUnwindSafe(handler_fut).catch_unwind(),
    )
    .await
    {
        Ok(Ok(result)) => HandlerCycleResult::Returned(result),
        Ok(Err(panic_payload)) => {
            HandlerCycleResult::Panicked(crate::error::panic_message(panic_payload))
        }
        Err(_elapsed) => HandlerCycleResult::Suspended,
    }
}

/// Encode a contained workflow-handler panic message as the typed
/// `harvest_workflow_failure_v1` envelope carrying the engine-reserved
/// [`ERROR_TYPE_HANDLER_PANIC`](crate::failure::ERROR_TYPE_HANDLER_PANIC)
/// class (issue #782).
///
/// The resulting string is what the terminal `WorkflowFailed` event and the
/// worker's `HarvestError::WorkflowFailed` surface carry, so operators and
/// result-awaiting callers can classify a panic without parsing the message,
/// and the worker's #523 exclusion guard can key on the error type.
fn encode_workflow_panic(message: String) -> String {
    use crate::failure::{ERROR_TYPE_HANDLER_PANIC, IntoWorkflowErrorString as _, WorkflowFailure};
    WorkflowFailure::new(ERROR_TYPE_HANDLER_PANIC, message)
        .non_retryable()
        .into_workflow_error_payload()
}

/// Caller-supplied metadata recorded onto the `harvest.workflow.execute` span.
pub struct WorkflowExecuteSpanMeta {
    /// Logical workflow name (recorded as `harvest.workflow.id`).
    pub workflow_name: String,
    /// Business-level workflow identifier (e.g. `"subscription-123"`).
    /// Forwarded to [`WorkflowContext`] so [`WorkflowLogger`] can tag events.
    pub workflow_id: String,
    /// Shard identifier (recorded as `harvest.shard.id`).
    pub shard_id: i64,
    /// Task queue name (recorded as `harvest.queue`).
    pub queue_name: String,
    /// Whether this cycle is a deterministic replay (recorded as `harvest.replay`).
    pub is_replay: bool,
    /// W3C traceparent linking back to the original trace, present only on
    /// replay runs and only when a prior carrier stored a link.
    pub link_traceparent: Option<String>,
    /// The worker build ID of the worker executing this workflow.
    pub build_id: Option<String>,
    /// The effective `execution_timeout` budget for this run (issue #243/#772),
    /// threaded into the [`WorkflowContext`] as the deadline fallback and as the
    /// total budget for the deadline-fraction continue-as-new check. `None` when
    /// the workflow has no `execution_timeout`.
    pub execution_timeout: Option<chrono::Duration>,
    /// The authoritative absolute deadline for this run (issue #772), read live
    /// from the execution row's `deadline_at` column. Threaded into the
    /// [`WorkflowContext`] so `ctx.deadline()` / `ctx.should_continue_as_new()`
    /// reason about the **effective** deadline the timeout scanner enforces —
    /// which pause/resume (#383) and redrive push forward past
    /// `started_at + execution_timeout`. `None` = fall back to start + timeout.
    pub deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The execution id of the parent workflow that spawned this run, or `None`
    /// for a top-level run (issue #698). The worker populates it from the
    /// execution row's `parent_id` column so a child workflow can identify its
    /// spawner via `ctx.info()` / `ctx.parent_execution_id()`.
    pub parent_execution_id: Option<ExecutionId>,
}

/// Classification of a bounded, read-only query-replay drive (issue #612).
///
/// A query drives the workflow handler purely in replay against recorded
/// history — emitting no commands and appending no events — to reconstruct the
/// context state its registered query handlers read. This enum reports where
/// that drive stopped so the caller can decide how to respond:
///
/// - [`ReachedTerminal`](Self::ReachedTerminal): the handler ran to `Poll::Ready`
///   (`Ok` **or** `Err`), i.e. the full history was drained and the context now
///   reflects the workflow's final reconstructed state.
/// - [`Suspended`](Self::Suspended): the handler blocked on a workflow command
///   (activity/timer/signal) before reaching `Poll::Ready`, i.e. the recorded
///   history is insufficient to reconstruct the final state (pruned/released).
/// - [`TimedOut`](Self::TimedOut): the drive exceeded the supplied deadline
///   before reaching `Poll::Ready` (a workflow spinning on replay).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryReplayOutcome {
    /// The handler reached `Poll::Ready`; the context holds final state.
    ReachedTerminal,
    /// The handler suspended before completing; history is insufficient.
    Suspended,
    /// The drive exceeded the supplied deadline.
    TimedOut,
    /// The workflow handler **panicked** (unwound) during the replay drive
    /// (issue #782). The panic is contained here — the process/request survives
    /// — but the reconstructed context state is untrustworthy, so a query
    /// cannot be safely served against it.
    Panicked,
}

/// Returns `true` when `cmd` is a **replay-significant** workflow command — i.e.
/// one that reflects genuine workflow progress or a suspension, as opposed to a
/// pure-metadata command that does not affect replay determinism.
///
/// The pure-metadata exclusions are:
/// - [`UpsertSearchAttributes`](WorkflowCommand::UpsertSearchAttributes) and
///   [`SetCurrentDetails`](WorkflowCommand::SetCurrentDetails): operator-facing
///   metadata, never part of the deterministic command stream.
/// - [`PublishProgress`](WorkflowCommand::PublishProgress): an ephemeral,
///   best-effort live-output side channel (issue #791) that appends nothing to
///   `harvest_events` and is suppressed during replay — replay-neutral by
///   construction.
/// - [`CancelRaceLosers`](WorkflowCommand::CancelRaceLosers): for the
///   timer+signal race shape (issue #600) this is a pure, deterministic function
///   of already-resolved history (the winner is fixed by recorded order), so it
///   is re-emitted identically on every replay and carries no new information.
///
/// This is the **single source of truth** for two callers that must agree, so
/// the command classification is never re-enumerated by hand:
/// - the completion-time "new commands emitted beyond recorded history"
///   non-determinism check in [`run_workflow_with_state`]/[`run_strict_with_ctx`],
///   and
/// - the query-replay drivers' per-poll **delta** suspension discriminator
///   ([`drive_query_replay`] / [`drive_query_replay_async`]): a `Poll::Pending`
///   cycle whose replay-significant command count *increased* is a genuine
///   [`Suspended`](QueryReplayOutcome::Suspended) (case 1); a zero-delta cycle is
///   either a `tokio::task::yield_now()` spin the driver keeps driving to the
///   deadline (case 2 → [`TimedOut`](QueryReplayOutcome::TimedOut)) or a
///   command-less cold park such as `await_condition` that suspends immediately
///   (case 3 → [`Suspended`](QueryReplayOutcome::Suspended)).
pub(crate) const fn is_replay_significant_command(cmd: &WorkflowCommand) -> bool {
    !matches!(
        cmd,
        WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            | WorkflowCommand::CancelRaceLosers { .. }
    )
}

/// Waker used by the query-replay drivers ([`drive_query_replay`] /
/// [`drive_query_replay_async`]) to detect a self-wake — a workflow calling
/// `tokio::task::yield_now()`.
///
/// **Out of a tokio runtime** (the 16 pure `#[test]` callers of the sync driver)
/// `yield_now` fires the wake *synchronously* during the poll, so the flag is
/// readable immediately. **Inside a runtime** (the real HTTP query path)
/// `yield_now` DEFERS its wake to the scheduler queue, so the flag only flips
/// after the driver itself cooperatively yields (a single
/// `tokio::task::yield_now().await` "quiet window" in
/// [`drive_query_replay_async`]). Either way, the authoritative "genuine
/// suspension" signal is the per-poll command **delta**
/// ([`WorkflowContext::count_commands`] over [`is_replay_significant_command`]);
/// this flag only discriminates the two zero-delta cases (self-wake spin vs.
/// command-less cold park).
struct QueryReplayWaker(std::sync::atomic::AtomicBool);

impl futures::task::ArcWake for QueryReplayWaker {
    fn wake_by_ref(arc_self: &std::sync::Arc<Self>) {
        arc_self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// The classification of a single [`poll_query_step`] cycle — the shared,
/// `!Send`-safe polling primitive both query-replay drivers loop over.
enum DriveStep {
    /// The handler future resolved (`Poll::Ready`): the reconstructed context now
    /// reflects the workflow's final state.
    Ready,
    /// The handler panicked (construction is contained separately by
    /// `catch_construct`); carries the extracted panic message.
    Panicked(String),
    /// `Poll::Pending` with a **positive** per-poll command delta: the handler
    /// emitted ≥1 replay-significant command on this poll, i.e. a genuine
    /// workflow suspension (case 1).
    CommandSuspension,
    /// `Poll::Pending` with a **zero** per-poll command delta: either a
    /// self-waking `tokio::task::yield_now()` spin (`woken == true`, case 2 —
    /// keep driving to the deadline) or a command-less cold park such as
    /// `await_condition` (`woken == false`, case 3 — suspend immediately). In a
    /// runtime the caller must open a scheduler "quiet window" before trusting
    /// `woken`, since tokio defers the `yield_now` wake.
    MaybeSpin { woken: bool },
}

/// Poll the query-replay handler future exactly once and classify the result
/// (issue #612).
///
/// This helper is **pure and synchronous**: it builds the `!Send`
/// [`futures::task::waker_ref`] and [`std::task::Context`] internally and drops
/// them before returning a plain [`DriveStep`]. That is load-bearing for
/// [`drive_query_replay_async`]: the only values that ever cross its
/// `yield_now().await` are `Send` (the `Arc<QueryReplayWaker>` flag, the
/// `Pin<Box<dyn Future + Send>>` handler future, and `&WorkflowContext`), because
/// the waker/`Context` never escape this function's stack frame.
///
/// `before_significant` is the running count of replay-significant commands the
/// context held *before* this poll; the returned `usize` is the count *after*,
/// so the caller threads it forward and reads the per-poll delta
/// (`after > before` ⇒ a command was emitted on this poll ⇒ a genuine
/// suspension).
fn poll_query_step(
    fut: std::pin::Pin<&mut (dyn std::future::Future<Output = Result<Value, String>> + Send)>,
    flag: &std::sync::Arc<QueryReplayWaker>,
    before_significant: usize,
    ctx: &WorkflowContext,
) -> (DriveStep, usize) {
    use std::sync::atomic::Ordering;
    use std::task::Poll;

    // Reset before polling so `flag` reflects only THIS poll's wake activity.
    flag.0.store(false, Ordering::Release);
    let waker = futures::task::waker_ref(flag);
    let mut poll_cx = std::task::Context::from_waker(&waker);
    // Issue #782: contain a workflow-handler panic during the read-only replay
    // drive. Without this, a panicking handler unwinds through the caller (the
    // plugin's query handler / axum request task) — the process survives but the
    // request 500s ungracefully. Query replays never emit workflow commands or
    // append events, so there is nothing to roll back.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.poll(&mut poll_cx))) {
        Err(panic_payload) => (
            DriveStep::Panicked(crate::error::panic_message(panic_payload)),
            before_significant,
        ),
        Ok(Poll::Ready(_)) => (DriveStep::Ready, before_significant),
        Ok(Poll::Pending) => {
            let after = ctx.count_commands(is_replay_significant_command);
            if after > before_significant {
                (DriveStep::CommandSuspension, after)
            } else {
                (
                    DriveStep::MaybeSpin {
                        woken: flag.0.load(Ordering::Acquire),
                    },
                    after,
                )
            }
        }
    }
}

/// Compute the finite drive deadline for a `timeout` budget.
///
/// `checked_add` guards against a pathologically large `timeout` overflowing
/// `Instant` (which would panic on `+`). `None` means "no representable
/// deadline" → never time out (the drivers skip the deadline check).
fn query_drive_deadline(timeout: Duration) -> Option<std::time::Instant> {
    std::time::Instant::now().checked_add(timeout)
}

/// `true` when the finite `deadline` (if any) has already elapsed.
///
/// Checked BEFORE each poll so an already-elapsed (or zero) deadline classifies
/// deterministically as `TimedOut` without a hang. This ordering is intentional
/// and load-bearing: the pure test
/// `past_deadline_classifies_as_timed_out_deterministically` passes
/// `Duration::ZERO` and expects `TimedOut`, so switching to poll-first would
/// break the deterministic-timeout semantics. (The running-path parity nit — a
/// zero-timeout running query would classify as `TimedOut` rather than serve
/// partial state — is intentionally not addressed: the default `query_timeout`
/// is 5s, so this is unreachable in practice.)
fn query_deadline_elapsed(deadline: Option<std::time::Instant>) -> bool {
    deadline.is_some_and(|d| std::time::Instant::now() >= d)
}

/// The boxed, `Send` handler future a query-replay drive polls. Aliased to keep
/// the [`construct_query_handler_fut`] signature under clippy's `type_complexity`
/// bar; identical in shape to [`WorkflowHandlerFn`]'s return type.
type QueryHandlerFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Construct the handler future for a query-replay drive, containing a
/// construction-phase panic (issue #782 / PR #1012 review). A hand-written
/// handler doing synchronous work before returning its boxed future can panic
/// during construction; the poll-time `catch_unwind` in [`poll_query_step`]
/// cannot reach that, so the construction call is wrapped too. Query replays
/// emit no commands and append no events, so there is nothing to roll back.
fn construct_query_handler_fut(
    ctx: &WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
) -> Result<QueryHandlerFut<'_>, QueryReplayOutcome> {
    match crate::error::catch_construct(|| handler(ctx, input)) {
        Ok(fut) => Ok(fut),
        Err(message) => {
            tracing::warn!(
                panic = %message,
                "harvest: workflow handler panicked constructing the query replay drive; \
                 containing as a query error (issue #782)"
            );
            Err(QueryReplayOutcome::Panicked)
        }
    }
}

/// Terminal actions shared by both drivers when [`poll_query_step`] resolves the
/// non-spin cases; kept in one place so the sync and async loops agree.
///
/// The `Ready` / `CommandSuspension` / cold-park (`MaybeSpin { woken: false }`)
/// arms all flush push-based signal handlers before returning (issue #612, Codex
/// P2 PR #993): a push-based signal handler (issue #546) whose target
/// `SignalReceived` became claimable but was never picked up by a real
/// cursor-advancing call this cycle leaves that event stashed. The caller's
/// `history_has_unconsumed_events()` drift check (`hydrate_ctx_for_query`) runs
/// *after* the drive returns, so without the flush a truthfully replayed run
/// would be misclassified as 410. The flush is cursor-bound and
/// push-handler-only, so it never consumes a signal a genuine pull-based
/// `wait_for_signal` or an open signal-or-deadline race is still waiting on. A
/// no-op when no handlers are registered (the common case).
fn finish_query_drive(ctx: &WorkflowContext, outcome: QueryReplayOutcome) -> QueryReplayOutcome {
    ctx.flush_pending_signal_handlers();
    outcome
}

/// Drive a workflow handler future purely in replay **synchronously**, bounded by
/// `timeout`, and classify where it stopped (issue #612).
///
/// This is **read-only**: it never persists anything. The provided `ctx` must
/// already be a replay context (built via
/// [`WorkflowContext::for_replay_with_state_and_history_policy`]) with its query
/// handlers registered; recorded events resolve synchronously via pre-sent
/// oneshot channels, so the whole history replays without I/O. The returned
/// [`QueryReplayOutcome`] tells the caller whether the context now reflects the
/// workflow's final state ([`ReachedTerminal`](QueryReplayOutcome::ReachedTerminal)),
/// stopped at a genuine suspension with insufficient history
/// ([`Suspended`](QueryReplayOutcome::Suspended)), or blew the deadline
/// ([`TimedOut`](QueryReplayOutcome::TimedOut)).
///
/// The `Ok`/`Err` return value of the workflow function is intentionally
/// ignored — a `FAILED` run still reconstructs its final internal state, which
/// is exactly what a post-mortem query wants to read.
///
/// # Sync vs. async
///
/// This synchronous entry point exists for the ~16 pure `#[test]` callers in
/// `query_terminal_tests.rs`, which run **out of a tokio runtime**. There
/// `tokio::task::yield_now()` fires its wake *synchronously*, so a self-wake spin
/// (case 2) is observable directly on the [`QueryReplayWaker`] flag with no
/// scheduler yield. The real production caller uses
/// [`drive_query_replay_async`], which opens a scheduler "quiet window" to
/// observe tokio's *deferred* `yield_now` wake inside a runtime.
///
/// Suspension is discriminated by the per-poll **command delta**
/// ([`poll_query_step`]), not waker timing:
/// - `CommandSuspension` (≥1 replay-significant command this poll) → `Suspended`.
/// - `MaybeSpin { woken: true }` (self-wake spin) → keep driving to the deadline
///   (→ `TimedOut` → 408). Re-polling still makes progress: `yield_now`'s
///   `Pending` is a one-shot that returns `Ready` on the next poll regardless of
///   its wake, and the deadline is re-checked at the loop top so we never poll
///   past `query_timeout`.
/// - `MaybeSpin { woken: false }` (command-less cold park, e.g.
///   `await_condition`) → `Suspended` immediately, so a RUNNING query over such a
///   park serves fast rather than burning the whole budget.
#[must_use]
pub fn drive_query_replay(
    ctx: &WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
    timeout: Duration,
) -> QueryReplayOutcome {
    let flag = std::sync::Arc::new(QueryReplayWaker(std::sync::atomic::AtomicBool::new(false)));
    let mut handler_fut = match construct_query_handler_fut(ctx, handler, input) {
        Ok(fut) => fut,
        Err(outcome) => return outcome,
    };

    let deadline = query_drive_deadline(timeout);
    let mut before = 0usize;
    loop {
        if query_deadline_elapsed(deadline) {
            return QueryReplayOutcome::TimedOut;
        }
        let (step, after) = poll_query_step(handler_fut.as_mut(), &flag, before, ctx);
        before = after;
        match step {
            DriveStep::Ready => {
                return finish_query_drive(ctx, QueryReplayOutcome::ReachedTerminal);
            }
            DriveStep::Panicked(message) => {
                tracing::warn!(
                    panic = %message,
                    "harvest: workflow handler panicked during query replay drive; \
                     containing as a query error (issue #782)"
                );
                return QueryReplayOutcome::Panicked;
            }
            // Case 1: a genuine workflow suspension on a durable command.
            DriveStep::CommandSuspension => {
                return finish_query_drive(ctx, QueryReplayOutcome::Suspended);
            }
            DriveStep::MaybeSpin { woken } => {
                if woken {
                    // Case 2: out-of-runtime `yield_now` fired synchronously →
                    // keep driving to the deadline. Yield the OS thread slice so a
                    // spinning query does not needlessly hard-pin a core for the
                    // whole budget.
                    std::thread::yield_now();
                } else {
                    // Case 3: command-less cold park (e.g. `await_condition`) →
                    // Suspended immediately.
                    return finish_query_drive(ctx, QueryReplayOutcome::Suspended);
                }
            }
        }
    }
}

/// Async twin of [`drive_query_replay`] for the single production caller
/// (`hydrate_ctx_for_query`), which awaits inside an axum request task (issue
/// #612).
///
/// Identical classification to the sync driver except for the zero-command-delta
/// `MaybeSpin` case: **inside a tokio runtime** `tokio::task::yield_now()` defers
/// its wake to the scheduler queue rather than firing it synchronously, so this
/// driver opens a **quiet window** — exactly one `tokio::task::yield_now().await`
/// — before trusting the [`QueryReplayWaker`] flag. One scheduler tick flushes
/// the deferred wake queue:
/// - if the flag then reads set, the workflow is a self-waking spin (case 2) →
///   keep driving to the deadline (→ `TimedOut` → 408);
/// - if it stays cold, the workflow parked command-less on an external re-poll
///   that never comes during a query drive (case 3, `await_condition`) →
///   `Suspended` immediately.
///
/// A raw `tokio::time::sleep(d)` in workflow code registers a *timer* waker (not
/// the deferred queue), which one `yield_now().await` does not fire, so it too
/// classifies case 3 → `Suspended` immediately rather than blocking the query for
/// `d`. That is correct and desirable (it is a determinism bug regardless).
///
/// # Send-safety
///
/// The `!Send` waker/`Context` are built and dropped inside the synchronous
/// [`poll_query_step`], so the only values live across the `yield_now().await`
/// are `Send`: the `Arc<QueryReplayWaker>` flag, the `Pin<Box<dyn Future + Send>>`
/// handler future, `&WorkflowContext`, and a couple of `Copy` scalars. This keeps
/// the returned future `Send`, as the axum handler requires.
#[must_use]
pub async fn drive_query_replay_async(
    ctx: &WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
    timeout: Duration,
) -> QueryReplayOutcome {
    let flag = std::sync::Arc::new(QueryReplayWaker(std::sync::atomic::AtomicBool::new(false)));
    let mut handler_fut = match construct_query_handler_fut(ctx, handler, input) {
        Ok(fut) => fut,
        Err(outcome) => return outcome,
    };

    let deadline = query_drive_deadline(timeout);
    let mut before = 0usize;
    loop {
        if query_deadline_elapsed(deadline) {
            return QueryReplayOutcome::TimedOut;
        }
        let (step, after) = poll_query_step(handler_fut.as_mut(), &flag, before, ctx);
        before = after;
        match step {
            DriveStep::Ready => {
                return finish_query_drive(ctx, QueryReplayOutcome::ReachedTerminal);
            }
            DriveStep::Panicked(message) => {
                tracing::warn!(
                    panic = %message,
                    "harvest: workflow handler panicked during query replay drive; \
                     containing as a query error (issue #782)"
                );
                return QueryReplayOutcome::Panicked;
            }
            // Case 1: a genuine workflow suspension on a durable command.
            DriveStep::CommandSuspension => {
                return finish_query_drive(ctx, QueryReplayOutcome::Suspended);
            }
            DriveStep::MaybeSpin { woken } => {
                if !woken {
                    // Quiet window: one cooperative scheduler tick flushes tokio's
                    // deferred `yield_now` wake queue so an in-runtime spin's wake
                    // reaches our flag. Nothing `!Send` is live here — the
                    // waker/`Context` were built and dropped inside
                    // `poll_query_step`, and `handler_fut`'s borrow ended when it
                    // returned.
                    tokio::task::yield_now().await;
                }
                if flag.0.load(std::sync::atomic::Ordering::Acquire) {
                    // Case 2: self-waking spin (`tokio::task::yield_now()` in
                    // workflow code) → keep driving to the deadline (→ 408). The
                    // `yield_now().await` above already yielded the runtime this
                    // cycle, so a spinning query never starves other tasks.
                    continue;
                }
                // Case 3: command-less cold park (e.g. `await_condition`) that
                // never self-wakes during a query drive → Suspended immediately,
                // so a RUNNING query serves fast rather than burning the budget.
                return finish_query_drive(ctx, QueryReplayOutcome::Suspended);
            }
        }
    }
}

/// How a query on a *terminal* execution should be answered after driving its
/// history through [`drive_query_replay`] (issue #612).
///
/// This is the pure classification the plugin's `hydrate_ctx_for_query` maps to
/// HTTP status codes: [`Serve`](Self::Serve) → 200, [`TimedOut`](Self::TimedOut)
/// → 408, [`HistoryUnavailable`](Self::HistoryUnavailable) → 410.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalQueryDecision {
    /// Serve the query against the reconstructed context (HTTP 200).
    Serve,
    /// The replay exceeded the deadline (HTTP 408).
    TimedOut,
    /// The recorded history cannot be replayed to its terminal seal — it was
    /// pruned by retention or released on reset (HTTP 410).
    HistoryUnavailable,
}

/// Returns `true` when the recorded history reached its **terminal seal** (issue
/// #612).
///
/// A terminal seal is any terminal lifecycle event (`WorkflowCompleted`/
/// `WorkflowFailed`/`WorkflowCancelled`/`WorkflowContinuedAsNew`/
/// `WorkflowResetTerminated`/`WorkflowExecutionTimedOut`, plus the trailing
/// bookkeeping events [`is_terminal_lifecycle`](WorkflowEvent::is_terminal_lifecycle)
/// also classifies) present in the log.
///
/// The append-only event log always seals with the terminal event, so it sits
/// at or near the tail; the scan runs **from the end** so a sealed history
/// short-circuits in O(1) while tolerating any trailing bookkeeping events
/// (`ChildWorkflowCascadeApplied`, `WorkflowRetryScheduled`) that follow the
/// seal — those are themselves classified as terminal-lifecycle, so
/// last-event-only would also work, but `rev().any()` is robust to any future
/// non-terminal trailing bookkeeping without an O(N) full scan on the common
/// sealed case.
///
/// Only meaningful for executions whose DB row is already in a terminal state;
/// callers gate this behind that check.
#[must_use]
pub fn history_reached_terminal_seal(events: &[WorkflowEvent]) -> bool {
    events
        .iter()
        .rev()
        .any(WorkflowEvent::is_terminal_lifecycle)
}

/// Classify how to answer a query on a **terminal** execution (issue #612).
///
/// Takes the replay `outcome`, whether the recorded history reached its terminal
/// seal (see [`history_reached_terminal_seal`]), and whether — after the drive —
/// the context still holds **unconsumed non-lifecycle history** (see
/// [`WorkflowContext::history_has_unconsumed_events`]).
///
/// `has_unconsumed_history` is the drift guard (Codex P2, PR #986 follow-up).
/// If the driven handler settled to a servable state (`ReachedTerminal`, or a
/// `Suspended` run the engine sealed while parked) while genuine recorded
/// non-lifecycle history remains unmatched, the handler diverged from the
/// recorded history — the workflow code changed since the run executed — and the
/// reconstructed state does **not** correspond to what actually happened.
/// Serving it would be misleading, so gate every `Serve` on this check and
/// return [`HistoryUnavailable`](TerminalQueryDecision::HistoryUnavailable)
/// (410) instead. The check comes from
/// [`HistoryMatcher::has_non_lifecycle_unconsumed`](crate::replay::HistoryMatcher::has_non_lifecycle_unconsumed),
/// which **excludes trailing terminal-lifecycle events** — so a truthfully
/// replayed completed / sealed-while-parked run (whose only unconsumed tail is
/// the terminal seal) reports `false` and still Serves. That exclusion is
/// exactly why the sealed-mid-flight shapes below are not regressed.
///
/// - [`ReachedTerminal`](QueryReplayOutcome::ReachedTerminal): the handler drove
///   all the way to `Poll::Ready`, so the context holds the workflow's fully
///   reconstructed final state → [`Serve`](TerminalQueryDecision::Serve) (200),
///   **unless** `has_unconsumed_history` (code drift) →
///   [`HistoryUnavailable`](TerminalQueryDecision::HistoryUnavailable) (410).
/// - [`TimedOut`](QueryReplayOutcome::TimedOut) →
///   [`TimedOut`](TerminalQueryDecision::TimedOut) (408): a spinning replay.
/// - [`Suspended`](QueryReplayOutcome::Suspended):
///     - if the history reached a terminal seal **and** no non-lifecycle history
///       remains unconsumed, the engine sealed the run while its function was
///       parked mid-command — the canonical shapes are `CONTINUED_AS_NEW`
///       (`continue_as_new` parks forever), `TIMED_OUT` (incomplete trailing
///       command + `WorkflowExecutionTimedOut`), and a mid-await external/hard
///       `CANCELLED`/`FAILED`. This is exactly the running-path behaviour applied
///       to a run the engine sealed while parked, so serve the reconstructed
///       partial state at the recorded terminal point
///       ([`Serve`](TerminalQueryDecision::Serve), 200).
///     - otherwise the history has no terminal lifecycle event at all (genuinely
///       truncated: pruned by retention / released on reset / empty), or the
///       drifted handler stopped short on a sealed run leaving recorded
///       non-lifecycle history unconsumed → return
///       [`HistoryUnavailable`](TerminalQueryDecision::HistoryUnavailable) (410)
///       rather than a misleading partial/empty answer.
#[must_use]
pub const fn classify_terminal_query(
    outcome: QueryReplayOutcome,
    history_reached_terminal_seal: bool,
    has_unconsumed_history: bool,
) -> TerminalQueryDecision {
    match outcome {
        QueryReplayOutcome::ReachedTerminal => {
            if has_unconsumed_history {
                TerminalQueryDecision::HistoryUnavailable
            } else {
                TerminalQueryDecision::Serve
            }
        }
        QueryReplayOutcome::TimedOut => TerminalQueryDecision::TimedOut,
        QueryReplayOutcome::Suspended => {
            if history_reached_terminal_seal && !has_unconsumed_history {
                TerminalQueryDecision::Serve
            } else {
                TerminalQueryDecision::HistoryUnavailable
            }
        }
        // Issue #782: a workflow handler that panics during the replay drive
        // cannot reconstruct trustworthy state, so the reconstructed context
        // must never be served. Classified as `HistoryUnavailable` (410 Gone,
        // permanent) rather than `TimedOut` (408, retryable): a deterministic
        // handler panic recurs on every retry, so retrying is pointless.
        QueryReplayOutcome::Panicked => TerminalQueryDecision::HistoryUnavailable,
    }
}

/// Run a workflow function through replay and live execution.
///
/// Builds a [`WorkflowContext`] from the provided event history, invokes the
/// handler, and returns the outcome. If the handler completes within the
/// timeout, the result is `Completed` or `Failed`. If it blocks (suspended on
/// a oneshot waiting for activity/timer resolution), the accumulated commands
/// are returned as `Suspended`.
///
/// # Arguments
///
/// * `exec_id` - The execution ID for this workflow run.
/// * `history` - The event history to replay (must start with `WorkflowStarted`).
/// * `handler` - The type-erased workflow handler function.
/// * `input` - The serialized input to pass to the workflow.
pub async fn run_workflow(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> WorkflowOutcome {
    let (outcome, _pending, _span) =
        run_workflow_with_state(exec_id, history, handler, input, empty_shared_state(), None).await;
    outcome
}

/// Like [`run_workflow`] but runs in strict replay mode.
///
/// Uses [`WorkflowContext::for_replay_strict`] so that activity and local-activity
/// dispatch additionally compare input payloads against the recorded history,
/// returning a non-determinism error on any mismatch.  This is used by
/// [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to catch
/// input-changing code changes before deployment.
#[allow(clippy::implicit_hasher, clippy::too_many_arguments)]
pub async fn run_workflow_strict(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    context_headers: std::collections::HashMap<String, String>,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
    execution_timeout: Option<chrono::Duration>,
    // Issue #772: the per-execution live effective `deadline_at` (pause/resume/
    // redrive-shifted), threaded so the internal continue-as-new budget check
    // reasons about the same deadline the timeout scanner enforces. `None` falls
    // back to `start + execution_timeout` (the file/JSON replay path).
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    // Issue #698: the spawning parent's execution id. `parent_execution_id` lives
    // in no `WorkflowEvent`, so a pure-history replay cannot recover it — the
    // replayer (`WorkflowReplayer::with_parent_execution_id`) threads it here so a
    // child that branches command-affecting control flow on
    // `ctx.info().parent_execution_id` replays deterministically. `None` models a
    // top-level run.
    parent_execution_id: Option<ExecutionId>,
    // Issue #698: the logical workflow type name (mechanism 1 — the handler-lookup
    // key already carried by the snapshot / DB row) and the business-level
    // `workflow_id` (mechanism 2 — added field). Neither lives in a `WorkflowEvent`
    // read by `ctx.info()` (the executor sets them on the live context via
    // span_meta), so a pure-history replay must apply them here or a workflow that
    // branches command-affecting control flow on `ctx.info().workflow_type` /
    // `ctx.info().workflow_id` (or embeds either in an activity input) false-reports
    // non-determinism. `workflow_id` is `None` for a raw-events fixture with no id.
    workflow_name: String,
    workflow_id: Option<String>,
    // Issue #614: the runtime registry's history policy
    // (`registry.history_policy()`, threaded by the worker), so a strict/diagnosis
    // replay of a workflow that branches on `ctx.should_continue_as_new()` stays
    // byte-faithful to the live worker rather than silently using the default
    // policy. `WorkflowHistoryPolicy::default()` preserves prior behavior.
    history_policy: WorkflowHistoryPolicy,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_strict_with_state(exec_id, history, state)
        .with_context_headers(context_headers)
        .with_execution_timeout(execution_timeout)
        .with_deadline(deadline_at)
        .with_parent_execution_id(parent_execution_id)
        .with_workflow_name(workflow_name)
        .with_workflow_id(workflow_id.unwrap_or_default())
        .with_history_policy(history_policy)
        .with_metrics(metrics);
    run_strict_with_ctx(exec_id, ctx, handler, input).await
}

/// Like [`run_workflow_strict`] but enables the advancing virtual clock (issue #526).
///
/// Used by [`WorkflowReplayer::with_advancing_timer_clock`] so that
/// `replay_check` on a [`TestRunOutcome`](crate::testing::TestRunOutcome) can
/// verify time-branching workflows without false non-determinism failures.
#[cfg(any(test, feature = "testing"))]
#[allow(clippy::implicit_hasher, clippy::too_many_arguments)]
pub(crate) async fn run_workflow_strict_advancing_clock(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    context_headers: std::collections::HashMap<String, String>,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
    execution_timeout: Option<chrono::Duration>,
    // Issue #772: per-execution live effective `deadline_at` (see
    // [`run_workflow_strict`]).
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    // Issue #698: the spawning parent's execution id (see [`run_workflow_strict`]).
    parent_execution_id: Option<ExecutionId>,
    // Issue #698: workflow type name / business `workflow_id` (see
    // [`run_workflow_strict`]).
    workflow_name: String,
    workflow_id: Option<String>,
    // Issue #614: the runtime registry's history policy (see [`run_workflow_strict`]).
    // `WorkflowHistoryPolicy::default()` preserves prior behavior.
    history_policy: WorkflowHistoryPolicy,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_strict_with_state(exec_id, history, state)
        .with_context_headers(context_headers)
        .with_advancing_timer_clock()
        .with_execution_timeout(execution_timeout)
        .with_deadline(deadline_at)
        .with_parent_execution_id(parent_execution_id)
        .with_workflow_name(workflow_name)
        .with_workflow_id(workflow_id.unwrap_or_default())
        .with_history_policy(history_policy)
        .with_metrics(metrics);
    run_strict_with_ctx(exec_id, ctx, handler, input).await
}

/// Inner body shared by [`run_workflow_strict`] and
/// [`run_workflow_strict_advancing_clock`].  The caller builds the context
/// (including any advancing-clock opt-in) and passes it here.
#[allow(clippy::too_many_lines)]
async fn run_strict_with_ctx(
    exec_id: ExecutionId,
    ctx: WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
) -> WorkflowOutcome {
    // ADR-0001 §2.1: strict mode is always a replay cycle.
    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = true,
    );

    async {
        // Issue #782: run the handler with panic containment. A contained panic
        // short-circuits to a typed HandlerPanic `Failed` outcome, discarding
        // the panicked cycle's commands (there are none to drain here). The
        // Returned/Suspended arms are byte-equivalent to the pre-#782
        // `timeout(SUSPENSION_TIMEOUT, handler(...))` call.
        let timeout_result = match run_workflow_handler_cycle(&ctx, handler, input).await {
            HandlerCycleResult::Returned(result) => Ok(result),
            HandlerCycleResult::Suspended => Err(()),
            HandlerCycleResult::Panicked(message) => {
                return WorkflowOutcome::Failed {
                    error: encode_workflow_panic(message),
                    non_deterministic_details: None,
                    handler_panic: true,
                    unhandled_signals: std::collections::BTreeMap::new(),
                };
            }
        };
        match timeout_result {
            // An infallible built-in primitive (system_now/new_uuid/random_*) may
            // have absorbed a divergence and returned a fallback value (issue #384);
            // surface it before the other completion checks.
            Ok(Ok(output)) => ctx.take_deferred_nd_error().map_or_else(
                || {
                    // Issue #546 post-ship hardening: flush any push-based signal
                    // handler whose target became claimable but was never picked
                    // up by a real cursor-advancing call this cycle, BEFORE the
                    // unconsumed-history check below (so a signal a registered
                    // handler would have claimed is never mistaken for genuinely
                    // unconsumed history).
                    ctx.flush_pending_signal_handlers();
                    if ctx.history_has_unconsumed_events() {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<end of history>".to_string()),
                                actual: Some("<workflow returned early>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: early completion mismatch: \
                                    expected <end of history>, got <workflow returned early>"
                                .to_string(),
                            non_deterministic_details: nd,
                            handler_panic: false,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    } else if ctx
                        .drain_commands()
                        .iter()
                        .any(is_replay_significant_command)
                    {
                        // New commands emitted after history was fully consumed (e.g. a
                        // newly-added version() or side_effect() call on an old history).
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<no new commands>".to_string()),
                                actual: Some("<new commands emitted>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: new commands emitted beyond \
                                    recorded history"
                                .to_string(),
                            non_deterministic_details: nd,
                            handler_panic: false,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    } else {
                        WorkflowOutcome::Completed {
                            output,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    }
                },
                |nd| {
                    let details = ctx.take_nd_details();
                    WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    }
                },
            ),
            // A primitive may have drifted before the workflow returned Err from
            // its own logic; prefer the non-determinism error (issue #384).
            Ok(Err(error)) => {
                // See the `Ok(Ok(output))` arm above (issue #546).
                ctx.flush_pending_signal_handlers();
                let details = ctx.take_nd_details();
                ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    },
                )
            }
            Err(_elapsed) => {
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have recorded a divergence before the workflow parked on an
                // await point. Fail the execution now rather than suspending from
                // a non-deterministic state (issue #384).
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    };
                }
                let mut commands = ctx.drain_commands();
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
}

/// Run a workflow function through replay canary mode.
///
/// Simulates workflow execution under strict replay, but utilizing a canary
/// context. If execution reaches the end of the recorded history and suspends,
/// it returns `WorkflowOutcome::Suspended` rather than a non-determinism error.
/// If it suspends *before* all events in history are processed, it fails.
#[allow(
    clippy::implicit_hasher,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]
pub(crate) async fn run_workflow_canary(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    context_headers: std::collections::HashMap<String, String>,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
    execution_timeout: Option<chrono::Duration>,
    // Issue #772: per-execution live effective `deadline_at` (see
    // [`run_workflow_strict`]).
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    // Issue #698: the spawning parent's execution id (see [`run_workflow_strict`]).
    // `run_canary` sources it from the sampled `harvest_workflow_executions.parent_id`
    // column so a parent-aware child does not false-report non-determinism in the
    // deploy replay canary.
    parent_execution_id: Option<ExecutionId>,
    // Issue #698: workflow type name / business `workflow_id` (see
    // [`run_workflow_strict`]). `run_canary` sources both from the sampled
    // `harvest_workflow_executions` row so a workflow that branches on
    // `ctx.info().workflow_type` / `workflow_id` does not false-report
    // non-determinism in the deploy replay canary.
    workflow_name: String,
    workflow_id: Option<String>,
    // Issue #614: the runtime registry's history policy
    // (`registry.history_policy()`, threaded by the worker), so a canary/diagnosis
    // replay of a workflow that branches on `ctx.should_continue_as_new()` stays
    // byte-faithful to the live worker rather than silently using the default
    // policy. `WorkflowHistoryPolicy::default()` preserves prior behavior.
    history_policy: WorkflowHistoryPolicy,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_canary_with_state(exec_id, history, state)
        .with_context_headers(context_headers)
        .with_execution_timeout(execution_timeout)
        .with_deadline(deadline_at)
        .with_parent_execution_id(parent_execution_id)
        .with_workflow_name(workflow_name)
        .with_workflow_id(workflow_id.unwrap_or_default())
        .with_history_policy(history_policy)
        .with_metrics(metrics);

    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = true,
    );

    async {
        // Issue #782: run the handler with panic containment (see
        // `run_strict_with_ctx` for rationale). A contained panic short-circuits
        // to a typed HandlerPanic `Failed` outcome.
        let timeout_result = match run_workflow_handler_cycle(&ctx, handler, input).await {
            HandlerCycleResult::Returned(result) => Ok(result),
            HandlerCycleResult::Suspended => Err(()),
            HandlerCycleResult::Panicked(message) => {
                return WorkflowOutcome::Failed {
                    error: encode_workflow_panic(message),
                    non_deterministic_details: None,
                    handler_panic: true,
                    unhandled_signals: std::collections::BTreeMap::new(),
                };
            }
        };
        match timeout_result {
            Ok(Ok(output)) => ctx.take_deferred_nd_error().map_or_else(
                || {
                    // Issue #546 post-ship hardening: flush any push-based signal
                    // handler whose target became claimable but was never picked
                    // up by a real cursor-advancing call this cycle, BEFORE the
                    // unconsumed-history check below (so a signal a registered
                    // handler would have claimed is never mistaken for genuinely
                    // unconsumed history).
                    ctx.flush_pending_signal_handlers();
                    if ctx.history_has_unconsumed_events() {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<end of history>".to_string()),
                                actual: Some("<workflow returned early>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: early completion mismatch: \
                                    expected <end of history>, got <workflow returned early>"
                                .to_string(),
                            non_deterministic_details: nd,
                            handler_panic: false,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    } else if ctx
                        .drain_commands()
                        .iter()
                        .any(is_replay_significant_command)
                    {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<no new commands>".to_string()),
                                actual: Some("<new commands emitted>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: new commands emitted beyond \
                                    recorded history"
                                .to_string(),
                            non_deterministic_details: nd,
                            handler_panic: false,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    } else {
                        WorkflowOutcome::Completed {
                            output,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        }
                    }
                },
                |nd| {
                    let details = ctx.take_nd_details();
                    WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    }
                },
            ),
            Ok(Err(error)) => {
                // See the `Ok(Ok(output))` arm above (issue #546).
                ctx.flush_pending_signal_handlers();
                let details = ctx.take_nd_details();
                ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    },
                )
            }
            Err(_elapsed) => {
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    };
                }

                // If history still has unconsumed events when we suspend, that's non-deterministic
                if ctx.history_has_unconsumed_events() {
                    let nd = ctx.take_nd_details().or_else(|| {
                        Some(crate::error::NonDeterministicDetails {
                            event_index: i32::try_from(ctx.replay_position()).ok(),
                            expected: Some("<consume all history>".to_string()),
                            actual: Some("<workflow suspended early>".to_string()),
                            workflow_type: Some(ctx.workflow_type().to_string()),
                            build_id: ctx.build_id().map(String::from),
                        })
                    });
                    return WorkflowOutcome::Failed {
                        error: "non-deterministic replay: workflow suspended before all history events were replayed".to_string(),
                        non_deterministic_details: nd,
                        handler_panic: false,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    };
                }

                let mut commands = ctx.drain_commands();
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
}

/// Run a workflow function through replay and live execution with shared state.
///
/// Returns a triple of `(outcome, pending_commands, span_handle)`:
/// - `outcome`: the workflow's terminal or suspended state.
/// - `pending_commands`: commands emitted during a `Completed` or `Failed` run
///   that the worker must persist before recording the terminal event. This is
///   non-empty only when the workflow invoked `execute_admitted_update` in live
///   mode — the `RecordUpdateResult` commands must be appended to history before
///   `WorkflowCompleted`/`WorkflowFailed`. For `Suspended` outcomes the commands
///   are already carried inside the variant; this Vec will be empty.
/// - `span_handle`: the open `harvest.workflow.execute` span. The caller should
///   hold it alive while persisting producer-side side-effects (activity
///   schedules, child workflow starts) so those producer spans are nested inside
///   the executor cycle. Dropping the handle closes the span.
pub async fn run_workflow_with_state(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    run_workflow_with_state_and_history_policy(
        exec_id,
        history,
        handler,
        input,
        state,
        WorkflowHistoryPolicy::default(),
        span_meta,
        &[],
        &[],
        std::sync::Arc::new(NoOpMetrics),
    )
    .await
}

/// Like [`run_workflow_with_state`] but enables the advancing virtual clock.
///
/// Issue #526, test harness only. The context is built with
/// [`WorkflowContext::with_advancing_timer_clock`] so that each durable timer
/// resolved from history increments `ctx.now()` by its duration.
#[cfg(any(test, feature = "testing"))]
pub async fn run_workflow_with_state_advancing_clock(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    use crate::context::WorkflowContext;
    let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        history,
        state,
        WorkflowHistoryPolicy::default(),
    )
    .with_advancing_timer_clock()
    // Thread the workflow/queue names from span_meta into the context
    // (mirrors run_workflow_with_state_history_policy_and_caps) so harness
    // users — WorkflowTestEnv::with_workflow_name/with_queue_name — can
    // assert metric label content (issue #801 post-review P3).
    .with_workflow_name(span_meta.map_or("", |m| m.workflow_name.as_str()))
    // Issue #698: thread the business `workflow_id` (mirrors the caps sibling
    // `run_workflow_with_state_history_policy_and_caps`) so `ctx.info().workflow_id`
    // reports it under a WorkflowTestEnv run that sets it on `span_meta`.
    .with_workflow_id(span_meta.map_or("", |m| m.workflow_id.as_str()))
    .with_queue_name(span_meta.map_or("", |m| m.queue_name.as_str()))
    // Issue #772: thread the execution-timeout budget so a WorkflowTestEnv run
    // can exercise deadline-aware continue-as-new.
    .with_execution_timeout(span_meta.and_then(|m| m.execution_timeout))
    // Issue #772: thread the authoritative absolute deadline (the effective,
    // pause/resume/redrive-shifted `deadline_at`) so `ctx.deadline()` matches
    // the timeout scanner rather than a stale start+timeout recompute.
    .with_deadline(span_meta.and_then(|m| m.deadline_at))
    // Issue #698: thread the spawning parent's execution id so a child workflow
    // can read it via `ctx.info()` / `ctx.parent_execution_id()`.
    .with_parent_execution_id(span_meta.and_then(|m| m.parent_execution_id))
    .with_metrics(metrics);
    drive_workflow(ctx, handler, input, span_meta).await
}

/// Like [`run_workflow_with_state`] but installs explicit history guardrails,
/// workflow name, and payload size caps into the [`WorkflowContext`].
#[allow(clippy::too_many_arguments)]
pub async fn run_workflow_with_state_and_history_policy(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    history_policy: WorkflowHistoryPolicy,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
    declarative_query_handlers: &[&QueryHandlerInfo],
    declarative_update_handlers: &[&UpdateHandlerInfo],
    metrics: std::sync::Arc<dyn MetricsRecorder>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    run_workflow_with_state_history_policy_and_caps(
        exec_id,
        history,
        handler,
        input,
        state,
        history_policy,
        span_meta,
        declarative_query_handlers,
        declarative_update_handlers,
        "",
        crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
        crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
        crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
        crate::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES,
        std::collections::HashMap::new(),
        None,
        metrics,
        None,
        None,
    )
    .await
}

/// Full executor entry point used by the worker, which injects the workflow name
/// and payload size caps configured on the `BuiltHarvest` instance.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn run_workflow_with_state_history_policy_and_caps(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    history_policy: WorkflowHistoryPolicy,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
    declarative_query_handlers: &[&QueryHandlerInfo],
    declarative_update_handlers: &[&UpdateHandlerInfo],
    workflow_name: &str,
    max_activity_input_bytes: u64,
    max_signal_payload_bytes: u64,
    max_workflow_input_bytes: u64,
    max_current_details_bytes: usize,
    context_headers: std::collections::HashMap<String, String>,
    payload_offload_threshold: Option<u64>,
    metrics: std::sync::Arc<dyn MetricsRecorder>,
    default_activity_retry_policy: Option<crate::policy::RetryPolicy>,
    default_activity_start_to_close: Option<std::time::Duration>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        history,
        state,
        history_policy,
    )
    .with_workflow_name(workflow_name)
    .with_workflow_id(span_meta.map_or("", |m| m.workflow_id.as_str()))
    .with_queue_name(span_meta.map_or("", |m| m.queue_name.as_str()))
    .with_build_id(span_meta.and_then(|m| m.build_id.clone()))
    .with_execution_timeout(span_meta.and_then(|m| m.execution_timeout))
    // Issue #772: thread the authoritative absolute deadline (the effective,
    // pause/resume/redrive-shifted `deadline_at`) so `ctx.deadline()` matches
    // the timeout scanner rather than a stale start+timeout recompute.
    .with_deadline(span_meta.and_then(|m| m.deadline_at))
    // Issue #698: thread the spawning parent's execution id so a child workflow
    // can read it via `ctx.info()` / `ctx.parent_execution_id()`.
    .with_parent_execution_id(span_meta.and_then(|m| m.parent_execution_id))
    .with_payload_caps(
        max_activity_input_bytes,
        0,
        max_signal_payload_bytes,
        max_workflow_input_bytes,
    )
    .with_current_details_cap(max_current_details_bytes)
    .with_payload_offload_threshold(payload_offload_threshold)
    .with_context_headers(context_headers)
    .with_metrics(metrics)
    // Issue #620: thread the builder-level default activity retry/timeout floor
    // so LOCAL activities (resolved in `execute_local_activity_with_opts`) fall
    // back to it when no call-site or activity-level default is set.
    .with_activity_defaults(
        default_activity_retry_policy,
        default_activity_start_to_close,
    );

    // Auto-register declarative handlers before any workflow code runs.
    // This satisfies the AC: "authors do not call ctx.register_*_handler in
    // their workflow body; the runtime guarantees registration happens first."
    for h in declarative_query_handlers {
        ctx.register_declarative_query_handler(h);
    }
    for h in declarative_update_handlers {
        ctx.register_declarative_update_handler(h);
    }

    drive_workflow(ctx, handler, input, span_meta).await
}

/// Core executor body: emit the `OTel` span, run the handler with a suspension
/// timeout, and return the outcome.  Shared by all public entry points so the
/// advancing-clock variant (`run_workflow_with_state_advancing_clock`) does not
/// duplicate the span/timeout/drain logic.
#[allow(clippy::too_many_lines)] // one linear span/timeout/drain orchestrator
async fn drive_workflow(
    ctx: WorkflowContext,
    handler: WorkflowHandlerFn,
    input: Value,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    let exec_id = ctx.execution_id();

    // ADR-0001 §2.1: emit harvest.workflow.execute for every executor cycle.
    // harvest.replay defaults to false at span creation so subscribers that only
    // observe on_new_span (e.g. tests) see the correct value for callers that
    // don't supply span_meta. The worker passes span_meta to override it and to
    // populate the Empty fields (workflow.id, shard.id, queue) that only the
    // worker context knows.
    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = false,
        { ATTR_WORKFLOW_ID } = tracing::field::Empty,
        { ATTR_SHARD_ID } = tracing::field::Empty,
        { ATTR_QUEUE } = tracing::field::Empty,
        "link.traceparent" = tracing::field::Empty,
    );
    if let Some(meta) = span_meta {
        span.record(ATTR_REPLAY, meta.is_replay);
        span.record(ATTR_WORKFLOW_ID, meta.workflow_name.as_str());
        span.record(ATTR_SHARD_ID, meta.shard_id);
        span.record(ATTR_QUEUE, meta.queue_name.as_str());
        if let Some(link) = meta.link_traceparent.as_deref() {
            span.record("link.traceparent", link);
        }
    }

    // Clone the span handle BEFORE passing ownership to .instrument().
    // The clone keeps the ref-count above zero after .instrument() exits so the
    // OTel span is not ended until the caller explicitly drops the returned handle.
    // This allows caller-side producer spans (activity.schedule,
    // child_workflow.start) to be created as children of this span even though
    // the instrumented future has already completed.
    let span_handle = span.clone();

    let (outcome, pending) = async {
        // Run the handler with a timeout. If it completes, we get the result.
        // If it blocks on a oneshot (suspended), the timeout fires and we drain
        // the accumulated commands.
        //
        // Issue #782: run with panic containment. A contained panic short-circuits
        // to a typed HandlerPanic `Failed` outcome with NO pending commands — the
        // panicked cycle's commands are untrustworthy and are discarded (R5), so
        // `ctx.drain_commands()` is deliberately not called on this path.
        let timeout_result = match run_workflow_handler_cycle(&ctx, handler, input).await {
            HandlerCycleResult::Returned(result) => Ok(result),
            HandlerCycleResult::Suspended => Err(()),
            HandlerCycleResult::Panicked(message) => {
                return (
                    WorkflowOutcome::Failed {
                        error: encode_workflow_panic(message),
                        non_deterministic_details: None,
                        handler_panic: true,
                        unhandled_signals: std::collections::BTreeMap::new(),
                    },
                    Vec::new(),
                );
            }
        };

        match timeout_result {
            // Handler completed within the timeout window.  Drain any commands
            // emitted during live execution (e.g. RecordUpdateResult from
            // execute_admitted_update) so the worker can persist them before the
            // terminal WorkflowCompleted/WorkflowFailed event.
            Ok(Ok(output)) => {
                // Issue #546 post-ship hardening: flush any push-based signal
                // handler whose target became claimable but was never picked
                // up by a real cursor-advancing call this cycle (a workflow
                // that registers a handler and then completes without ever
                // awaiting an activity/timer/signal).
                ctx.flush_pending_signal_handlers();
                // Issue #684: snapshot the unconsumed signals (after the flush,
                // so #546 push handlers claim first) and carry them out on the
                // outcome; the WORKER emits from the map (see `unhandled_signals`
                // docs — emission moved off the executor's pre-#603-gate path).
                let unhandled_signals = ctx.unhandled_signals();
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have absorbed a replay divergence and recorded it as a
                // deferred non-determinism error (issue #384). Surface it as a
                // failure rather than letting the workflow complete silently.
                let details = ctx.take_nd_details();
                let outcome = ctx.take_deferred_nd_error().map_or_else(
                    || WorkflowOutcome::Completed {
                        output,
                        unhandled_signals: unhandled_signals.clone(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: unhandled_signals.clone(),
                    },
                );
                (outcome, ctx.drain_commands())
            }
            // A primitive may have drifted before the workflow returned Err from
            // its own logic; prefer the non-determinism error (issue #384).
            Ok(Err(error)) => {
                // See the `Ok(Ok(output))` arm above (issue #546).
                ctx.flush_pending_signal_handlers();
                // Issue #684: same terminal-arm snapshot as the completed path.
                let unhandled_signals = ctx.unhandled_signals();
                let details = ctx.take_nd_details();
                let outcome = ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                        handler_panic: false,
                        unhandled_signals: unhandled_signals.clone(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                        handler_panic: false,
                        unhandled_signals: unhandled_signals.clone(),
                    },
                );
                (outcome, ctx.drain_commands())
            }

            // Timeout elapsed -- the handler is suspended on a oneshot channel.
            // Drain the commands it emitted before suspending. RecordUpdateResult
            // commands emitted in this cycle are included in the commands list and
            // will be handled by the worker alongside the suspension side-effects.
            Err(_elapsed) => {
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have recorded a divergence before the workflow parked on an
                // await point. Fail the execution now rather than suspending from
                // a non-deterministic state (issue #384).
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return (
                        WorkflowOutcome::Failed {
                            error: format!("non-deterministic replay: {nd}"),
                            non_deterministic_details: details,
                            handler_panic: false,
                            unhandled_signals: std::collections::BTreeMap::new(),
                        },
                        ctx.drain_commands(),
                    );
                }
                let mut commands = ctx.drain_commands();
                // ContinueAsNew is terminal: when the workflow body parks on
                // the dedicated suspension future, the latest command in the
                // drain is the ContinueAsNew the user requested. Bookkeeping
                // commands earlier in the drain (e.g. RecordMarker, side_effect)
                // are returned as pending_cmds so the worker can still apply
                // any UpsertSearchAttributes patches before sealing the execution.
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return (WorkflowOutcome::ContinuedAsNew { input }, commands);
                }
                (WorkflowOutcome::Suspended { commands }, vec![])
            }
        }
    }
    .instrument(span)
    .await;

    (outcome, pending, span_handle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;
    use std::pin::Pin;

    /// A trivial workflow that just returns its input.
    fn echo_workflow<'a>(
        _ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(input) })
    }

    /// A workflow that always fails.
    fn failing_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Err("something went wrong".to_string()) })
    }

    /// A workflow whose handler **panics** (unwinds) instead of returning an
    /// `Err` (issue #782 fixture).
    fn panicking_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            panic!("boom from workflow handler");
        })
    }

    /// A workflow whose handler panics **during future construction** — the panic
    /// unwinds synchronously *before* the `Box::pin(...)` future is ever produced
    /// (issue #782 / PR #1012 review). A hand-written `WorkflowInfo::handler` may
    /// do synchronous work before returning its boxed future; this exercises the
    /// construction-phase `catch_construct` guard, which the poll-time
    /// `catch_unwind` cannot reach.
    fn construction_panicking_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        panic!("boom during workflow future construction");
    }

    /// A workflow that returns an author `Err` whose error type *happens* to be
    /// the engine-reserved `HandlerPanic` string. This must NOT be treated as a
    /// contained panic (issue #782 false-positive guard).
    fn fake_handler_panic_error_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            use crate::failure::{
                ERROR_TYPE_HANDLER_PANIC, IntoWorkflowErrorString as _, WorkflowFailure,
            };
            Err(WorkflowFailure::new(ERROR_TYPE_HANDLER_PANIC, "fabricated")
                .into_workflow_error_payload())
        })
    }

    /// A workflow that captures a side-effect (drifts against history) and then
    /// returns Err from its own logic.
    fn drift_then_error_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx.system_now(); // diverges from the recorded activity event
            Err("business rule violated".to_string())
        })
    }

    /// A workflow that calls an activity (will suspend if not in history).
    fn activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let result = ctx
                .execute_activity_raw("send_email", input, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(result)
        })
    }

    #[tokio::test]
    async fn executor_replays_completed_workflow() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"greeting": "hello"});

        // Full history: workflow started and the echo handler completes immediately.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, echo_workflow, input.clone()).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(output, input);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_returns_failed_for_erroring_workflow() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, failing_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { error, .. } => {
                assert!(error.contains("something went wrong"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_prefers_deferred_drift_over_workflow_error() {
        // Regression (issue #384): a primitive that drifts before the workflow
        // returns Err from its own logic must surface as non-determinism rather
        // than masquerading as an ordinary workflow failure.
        let exec_id = ExecutionId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            // The workflow calls system_now() here, but history recorded an
            // activity — a genuine divergence.
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let outcome = run_workflow(exec_id, history, drift_then_error_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { error, .. } => {
                assert!(
                    error.contains("non-deterministic replay"),
                    "drift must win over the workflow's own error: {error}"
                );
                assert!(
                    !error.contains("business rule violated"),
                    "the workflow's Err must not mask the drift: {error}"
                );
            }
            other => panic!("expected Failed(non-determinism), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn author_error_produces_failed_without_nd_details() {
        // AC3 pin (issue #603): a workflow body's own Err(...) must carry NO
        // NonDeterministicDetails — it is the worker's signal that the failure
        // is a legitimate author decision and must stay terminal.
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, failing_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed {
                non_deterministic_details,
                ..
            } => {
                assert!(
                    non_deterministic_details.is_none(),
                    "an author Err must not be classified as engine non-determinism"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Contained handler-panic conversion (issue #782)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn panicking_workflow_produces_failed_with_handler_panic_flag() {
        // AC2 core (issue #782): a workflow handler panic is caught and returned
        // as WorkflowOutcome::Failed { handler_panic: true, .. } carrying the
        // typed HandlerPanic envelope with the extracted panic message.
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, panicking_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
                handler_panic,
                ..
            } => {
                assert!(
                    handler_panic,
                    "a caught panic must set handler_panic = true"
                );
                assert!(
                    non_deterministic_details.is_none(),
                    "a handler panic is not an engine non-determinism divergence"
                );
                let decoded = crate::failure::decode_workflow_failure(&error);
                assert_eq!(
                    decoded.error_type.as_deref(),
                    Some(crate::failure::ERROR_TYPE_HANDLER_PANIC),
                    "the contained panic must carry the reserved HandlerPanic error type"
                );
                assert_eq!(decoded.message, "boom from workflow handler");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn construction_phase_panicking_workflow_is_contained_as_handler_panic() {
        // Issue #782 / PR #1012 review: a hand-written handler that panics while
        // *constructing* its future (before returning the boxed future) must be
        // contained identically to a poll-phase panic — the poll-time
        // `catch_unwind` cannot cover it, so the construction call itself is
        // wrapped. Proves the construction-phase containment path.
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(
            exec_id,
            history,
            construction_panicking_workflow,
            Value::Null,
        )
        .await;

        match outcome {
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
                handler_panic,
                ..
            } => {
                assert!(
                    handler_panic,
                    "a construction-phase panic must set handler_panic = true"
                );
                assert!(
                    non_deterministic_details.is_none(),
                    "a construction-phase panic is not an engine non-determinism divergence"
                );
                let decoded = crate::failure::decode_workflow_failure(&error);
                assert_eq!(
                    decoded.error_type.as_deref(),
                    Some(crate::failure::ERROR_TYPE_HANDLER_PANIC),
                    "the contained construction panic must carry the reserved HandlerPanic error type"
                );
                assert_eq!(decoded.message, "boom during workflow future construction");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn author_error_produces_failed_without_handler_panic_flag() {
        // AC3 pin (issue #782): a workflow body's own Err(...) must NOT set the
        // handler_panic flag, so the worker never routes it into the
        // panic-retry loop.
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, failing_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { handler_panic, .. } => {
                assert!(
                    !handler_panic,
                    "an author Err must not be classified as a contained handler panic"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fabricated_handler_panic_error_type_does_not_set_handler_panic_flag() {
        // AC3 false-positive guard (issue #782 / Q11): an author who returns
        // Err(WorkflowFailure::new("HandlerPanic", ...)) reaches the normal
        // Ok(Err(_)) arm with handler_panic = false, so it can never manufacture
        // a panic-retry via the error-type string.
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(
            exec_id,
            history,
            fake_handler_panic_error_workflow,
            Value::Null,
        )
        .await;

        match outcome {
            WorkflowOutcome::Failed { handler_panic, .. } => {
                assert!(
                    !handler_panic,
                    "a fabricated HandlerPanic error string must not set the caught-panic flag"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn engine_divergence_produces_failed_with_nd_details() {
        // AC3 pin (issue #603): an engine-detected replay divergence must carry
        // structured NonDeterministicDetails so the worker can block the
        // execution non-terminally instead of failing it.
        let exec_id = ExecutionId::new();
        // History recorded a timer, but the workflow schedules an activity —
        // a genuine divergence surfaced by the fallible match path.
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::TimerStarted {
                timer_id: crate::types::TimerId::new("t1"),
                duration_secs: 60,
            },
        ];

        let outcome = run_workflow(exec_id, history, activity_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
                ..
            } => {
                assert!(
                    error.contains("non-deterministic replay"),
                    "unexpected error: {error}"
                );
                let details = non_deterministic_details
                    .expect("engine divergence must carry NonDeterministicDetails");
                // `expected` carries what the code requested this cycle;
                // `actual` carries what the recorded history holds.
                assert_eq!(
                    details.expected.as_deref(),
                    Some("ActivityScheduled(send_email)")
                );
                assert_eq!(details.actual.as_deref(), Some("TimerStarted"));
                assert!(details.event_index.is_some());
            }
            other => panic!("expected Failed(non-determinism), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_suspends_on_new_activity() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"to": "alice@example.com"});

        // History has only WorkflowStarted -- no activity events.
        // The workflow will call execute_activity_raw which will emit a
        // ScheduleActivity command and block on the oneshot.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                assert_eq!(commands.len(), 1, "expected exactly one command");
                assert!(
                    matches!(&commands[0], WorkflowCommand::ScheduleActivity { name, .. } if name == "send_email"),
                    "expected ScheduleActivity command for send_email"
                );
            }
            other => panic!("expected Suspended, got {other:?}"),
        }
    }

    /// A workflow that triggers `continue_as_new` mid-flight.
    fn continue_as_new_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            // The future returned by continue_as_new never resolves on its
            // own; the executor's suspension timeout drains the command and
            // surfaces it as ContinuedAsNew.
            let _ = ctx
                .continue_as_new(serde_json::json!({"prev": input}))
                .await;
            unreachable!("continue_as_new must not resolve");
        })
    }

    #[tokio::test]
    async fn executor_returns_continued_as_new_when_command_drained() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(
            exec_id,
            history,
            continue_as_new_workflow,
            serde_json::json!("v1"),
        )
        .await;

        match outcome {
            WorkflowOutcome::ContinuedAsNew { input } => {
                assert_eq!(input, serde_json::json!({"prev": "v1"}));
            }
            other => panic!("expected ContinuedAsNew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_replays_activity_from_history() {
        let exec_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let input = serde_json::json!({"to": "alice@example.com"});
        let activity_output = serde_json::json!({"email_id": "msg-001"});

        // Full history with completed activity -- replay should complete.
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: input.clone(),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: activity_output.clone(),
            },
        ];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(output, activity_output);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    /// A workflow that reacts to a `cancel` signal via a push-based handler
    /// (issue #546) instead of a hand-coded `wait_for_signal` interleave.
    ///
    /// Dispatch is deferred to the next history-consulting call (see
    /// `register_and_dispatch_signal_handler`'s doc comment), so this reads
    /// the handler-mutated flag *after* a trivial deterministic primitive
    /// call rather than on the literal next line -- `ctx.system_now()` is
    /// cheap, replay-safe, and (like any other `match_history`-routed call)
    /// triggers the pump. A real workflow would typically have an actual
    /// activity/timer/loop in between instead.
    fn signal_handler_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let cancelled = std::sync::Arc::new(std::sync::Mutex::new(false));
            let c = cancelled.clone();
            ctx.register_signal_handler_raw("cancel", move |_payload| {
                *c.lock().unwrap() = true;
            });
            let _ = ctx.system_now();
            Ok(serde_json::json!({"cancelled": *cancelled.lock().unwrap()}))
        })
    }

    #[tokio::test]
    async fn executor_dispatches_signal_handler_end_to_end() {
        // The full production path: run_workflow drives the workflow function
        // exactly as the worker would, with a signal already recorded in
        // history before the handler is registered.
        let exec_id = ExecutionId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "user_requested"}),
            },
        ];

        let outcome = run_workflow(exec_id, history, signal_handler_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(output, serde_json::json!({"cancelled": true}));
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_signal_handler_workflow_completes_with_no_signal_recorded() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, signal_handler_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(output, serde_json::json!({"cancelled": false}));
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    // ── signal.unhandled plumb-through via drive_workflow (issue #684) ─────
    //
    // The executor NO LONGER emits harvest.signal.unhandled — it computes the
    // unconsumed-signal map at the terminal frontier and carries it out on the
    // Completed/Failed outcome; the worker emits from it (symmetric with #519's
    // record_workflow_terminal). These tests prove (a) the terminal outcomes
    // carry the map, (b) Suspended does not, and (c) the executor itself emits
    // NOTHING (a recorder passed in sees zero samples — the emission moved to
    // the worker).

    /// Recording double: fails the test if the executor ever emits a sample.
    #[derive(Default)]
    struct UnhandledSignalRecorder {
        samples: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl MetricsRecorder for UnhandledSignalRecorder {
        fn record_signal_unhandled(&self, workflow_name: &str, queue: &str) {
            self.samples
                .lock()
                .unwrap()
                .push((workflow_name.to_owned(), queue.to_owned()));
        }
    }

    fn span_meta(workflow_name: &str, queue_name: &str) -> WorkflowExecuteSpanMeta {
        WorkflowExecuteSpanMeta {
            workflow_name: workflow_name.to_owned(),
            workflow_id: String::new(),
            shard_id: 0,
            queue_name: queue_name.to_owned(),
            is_replay: false,
            link_traceparent: None,
            build_id: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
        }
    }

    fn started_then_late_signal() -> Vec<WorkflowEvent> {
        vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "late_signal".into(),
                payload: Value::Null,
            },
        ]
    }

    #[tokio::test]
    async fn completed_outcome_carries_unhandled_signals_and_executor_does_not_emit() {
        // A workflow that completes while a delivered SignalReceived was never
        // consumed CARRIES the unconsumed map on its Completed outcome — and the
        // executor emits NOTHING itself (the worker emits from the carried map).
        let recorder = std::sync::Arc::new(UnhandledSignalRecorder::default());
        let meta = span_meta("notif", "q");
        let (outcome, _cmds, _span) = run_workflow_with_state_advancing_clock(
            ExecutionId::new(),
            started_then_late_signal(),
            echo_workflow,
            Value::Null,
            empty_shared_state(),
            Some(&meta),
            recorder.clone(),
        )
        .await;
        match outcome {
            WorkflowOutcome::Completed {
                unhandled_signals, ..
            } => {
                assert_eq!(unhandled_signals.get("late_signal"), Some(&1));
                assert_eq!(unhandled_signals.len(), 1);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
        assert!(
            recorder.samples.lock().unwrap().is_empty(),
            "the executor must NOT emit harvest.signal.unhandled — the worker does"
        );
    }

    #[tokio::test]
    async fn failed_outcome_carries_unhandled_signals() {
        // A workflow that FAILS while leaving a signal unconsumed still carries
        // the unconsumed map (a failed run with leftover signals is legitimately
        // "unhandled").
        let recorder = std::sync::Arc::new(UnhandledSignalRecorder::default());
        let meta = span_meta("notif", "q");
        let (outcome, _cmds, _span) = run_workflow_with_state_advancing_clock(
            ExecutionId::new(),
            started_then_late_signal(),
            failing_workflow,
            Value::Null,
            empty_shared_state(),
            Some(&meta),
            recorder.clone(),
        )
        .await;
        match outcome {
            WorkflowOutcome::Failed {
                unhandled_signals, ..
            } => assert_eq!(unhandled_signals.get("late_signal"), Some(&1)),
            other => panic!("expected Failed, got {other:?}"),
        }
        assert!(recorder.samples.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn suspended_outcome_carries_no_unhandled_signals() {
        // Suspended is not a terminal arm — it has no unhandled_signals field,
        // and the executor emits nothing.
        let recorder = std::sync::Arc::new(UnhandledSignalRecorder::default());
        let meta = span_meta("notif", "q");
        let (outcome, _cmds, _span) = run_workflow_with_state_advancing_clock(
            ExecutionId::new(),
            started_then_late_signal(),
            activity_workflow, // suspends on send_email (not in history)
            Value::Null,
            empty_shared_state(),
            Some(&meta),
            recorder.clone(),
        )
        .await;
        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert!(
            recorder.samples.lock().unwrap().is_empty(),
            "a suspended workflow must not emit or carry harvest.signal.unhandled"
        );
    }
}
