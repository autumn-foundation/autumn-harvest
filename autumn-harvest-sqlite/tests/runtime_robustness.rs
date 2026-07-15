//! Regression tests for the Codex #1069 (P1/P2) and round-8 healthy-run-killer
//! findings on the single-writer backend:
//!
//! - **FIX 1 (P1)** — engine-detected replay non-determinism is NON-TERMINAL: the
//!   divergent decision cycle is discarded (nothing appended, execution stays
//!   `RUNNING`/resumable) and surfaced as [`SqliteError::NonDeterministic`],
//!   rather than sealing the run `FAILED` and poisoning history (mirrors the
//!   Postgres engine's issue #603 gate).
//! - **FIX 2 (P2)** — a workflow that schedules an UNREGISTERED activity releases
//!   the just-claimed task back to `PENDING` (instead of stranding it `RUNNING`),
//!   so a later drain re-claims it once the body is registered — no DB reopen.
//! - **FIX 3 (round 8)** — a PANICKING activity body is contained and routed
//!   through the normal retryable-failure path (retry per policy, else terminal
//!   `ActivityFailed`) instead of unwinding past the finalize and wedging the run.
//! - **FIX 4 (round 8)** — a single decision cycle emitting BOTH a timer and an
//!   activity (`tokio::join!`) records its schedule events in command order and
//!   replays cleanly on the core `WorkflowReplayer` (the matcher tolerates the
//!   interleaved sibling terminals).

// The `#[workflow]` macro references the workflow's input parameter, so a
// deliberately-unused param reads as a "used underscore binding" through the
// expansion.
#![allow(clippy::used_underscore_binding)]
// Some regression workflows deliberately hold no `.await` on one branch.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn echo_activity() -> ActivitySpec {
    ActivitySpec::new(1, |input: serde_json::Value| {
        Ok(json!(input.as_i64().unwrap_or_default()))
    })
}

/// Read a single task row's `state` from a raw connection to the same file (the
/// established inspection pattern in `signal_timeout.rs`/`reopen_consistency.rs`).
fn any_task_state(path: &std::path::Path, exec: ExecutionId) -> Option<String> {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        "SELECT state FROM harvest_tasks WHERE exec_id = ?1 ORDER BY seq LIMIT 1",
        [exec.to_string()],
        |r| r.get(0),
    )
    .ok()
}

// ── FIX 1: engine non-determinism does NOT seal FAILED / poison history ────────

/// A drift phase flag the workflow reads to change its command stream between
/// drives — the injected-flag construction the reviewer suggested for a
/// deterministic engine nd-divergence. `1` arms a long durable timer (records a
/// suspension and blocks); `2` schedules an activity at the SAME command
/// position, which diverges against the phase-1 history (expected `TimerStarted`,
/// got `ActivityScheduled`). Only this test references the flag.
static DRIFT_PHASE: AtomicU32 = AtomicU32::new(1);

#[workflow]
async fn drifty(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    if DRIFT_PHASE.load(Ordering::SeqCst) == 1 {
        // Phase 1: arm a long durable timer — records `TimerStarted`, then blocks.
        ctx.timer("t", 3600).await.map_err(|e| e.to_string())?;
    } else {
        // Phase 2 (drifted code): schedule an activity at the same command slot.
        let _ = ctx
            .execute_activity_raw("work", json!(1), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(1)
}

#[tokio::test]
async fn engine_nondeterminism_is_nonterminal_and_resumable() {
    let (_dir, path) = temp_db();
    let t0: DateTime<Utc> = DateTime::from_timestamp(5_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&drifty_info());
    rt.register_activity("work", echo_activity());

    // Phase 1: drive to a durable-timer block. Records [WorkflowStarted, TimerStarted].
    DRIFT_PHASE.store(1, Ordering::SeqCst);
    let exec = rt.start_workflow("drifty", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "phase 1 must block on the durable timer, got {state:?}"
    );

    let before = rt.load_history(exec).unwrap();
    assert!(
        before
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "phase 1 must have recorded a suspension:\n{before:?}"
    );

    // Phase 2: the workflow code drifts. Re-driving the UNCHANGED history now
    // diverges (expected TimerStarted, got ActivityScheduled).
    DRIFT_PHASE.store(2, Ordering::SeqCst);
    let err = rt.run_until_blocked_as_of(exec, t0).await.unwrap_err();

    // (a) the drive surfaces a NON-terminal non-determinism error.
    assert!(
        matches!(err, SqliteError::NonDeterministic { execution_id, .. } if execution_id == exec),
        "a divergence must surface as NonDeterministic, got {err}"
    );

    // (b) NO terminal event was appended — history is byte-identical to before.
    let after = rt.load_history(exec).unwrap();
    assert_eq!(
        after.len(),
        before.len(),
        "the divergent cycle must append NOTHING (no WorkflowFailed):\nbefore={before:?}\nafter={after:?}"
    );
    assert!(
        !after
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a non-determinism divergence must NOT seal WorkflowFailed:\n{after:?}"
    );

    // (c) the execution is still RUNNING (not sealed FAILED), i.e. resumable.
    assert!(
        matches!(rt.outcome(exec).unwrap(), ExecutionOutcome::Running),
        "the execution must stay RUNNING after a divergence"
    );

    // (d) a corrected (compatible) build resumes the SAME history and completes —
    // proving the run was discarded/resumable, not poisoned. Restore phase 1 and
    // fire the durable timer past its deadline.
    DRIFT_PHASE.store(1, Ordering::SeqCst);
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(3601))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(1)),
        "the corrected build must resume and complete, got {state:?}"
    );
}

/// A genuine author `Err(...)` still seals `FAILED` (the `None` nd arm is
/// unchanged) — the divergence gate must not swallow real failures.
#[workflow]
async fn plain_error(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ = ctx;
    Err("author boom".to_string())
}

#[tokio::test]
async fn author_error_still_seals_failed() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&plain_error_info());
    let exec = rt.start_workflow("plain_error", json!(0)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref e) if e == "author boom"),
        "an author Err must seal FAILED, got {state:?}"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(ref e) if e == "author boom"
    ));
    assert!(
        rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "a real author failure records WorkflowFailed"
    );
}

// ── FIX 2: an unregistered activity releases its claim (re-claimable) ──────────

#[workflow]
async fn needs_unreg(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("unreg", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default() * 2)
}

#[tokio::test]
async fn unregistered_activity_releases_claim_and_is_reclaimable() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&needs_unreg_info());
    // NOTE: "unreg" is deliberately NOT registered yet.

    let exec = rt.start_workflow("needs_unreg", json!(21)).unwrap();

    // The drive schedules the activity, claims it to RUNNING, then misses the
    // handler → releases the claim → surfaces UnregisteredActivity.
    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnregisteredActivity(ref n) if n == "unreg"),
        "expected UnregisteredActivity(unreg), got {err}"
    );

    // The task must be back in PENDING (NOT stranded RUNNING) — no DB reopen.
    assert_eq!(
        any_task_state(&path, exec).as_deref(),
        Some("PENDING"),
        "the claimed task must be released to PENDING, not stranded RUNNING"
    );

    // Register the body on the SAME runtime and drive again — no reopen. The
    // task is re-claimed and the workflow completes.
    rt.register_activity("unreg", echo_activity());
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "the workflow must complete once the body is registered (no reopen), got {state:?}"
    );
}

// ── FIX 3: a panicking activity body is contained (retried, then terminal) ─────

#[workflow]
async fn calls_panicky(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("panicky", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default())
}

#[tokio::test]
async fn panicking_activity_is_contained_and_exhausts_to_terminal() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&calls_panicky_info());
    // A body that ALWAYS panics, max 2 attempts.
    rt.register_activity(
        "panicky",
        ActivitySpec::new(
            2,
            |_input: serde_json::Value| -> Result<serde_json::Value, String> {
                panic!("activity boom")
            },
        ),
    );
    let exec = rt.start_workflow("calls_panicky", json!(7)).unwrap();

    // Must NOT wedge/Stuck/hang — the caught panic follows the retry policy and
    // exhausts to a clean terminal failure.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(_)),
        "a panicking body must exhaust to a terminal RunState::Failed, got {state:?}"
    );

    // Both attempts were consumed and recorded (the panic → normal retry path).
    let attempts = rt.activity_attempts(exec, "panicky").unwrap();
    assert_eq!(
        attempts.len(),
        2,
        "both attempts consumed via the retry policy"
    );
    assert!(attempts.iter().all(|a| a.result.is_err()));

    // Exactly one terminal ActivityFailed + a WorkflowFailed — and the task is
    // DONE, never stranded RUNNING.
    let history = rt.load_history(exec).unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
            .count(),
        1,
        "exactly one terminal ActivityFailed"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "the workflow fails terminally after retry exhaustion"
    );
    assert_ne!(
        any_task_state(&path, exec).as_deref(),
        Some("RUNNING"),
        "a panicking body must NEVER leave the task stranded RUNNING"
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Failed(_)
    ));
}

#[tokio::test]
async fn panicking_activity_retry_then_succeeds() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&calls_panicky_info());
    // Panic on the FIRST attempt, succeed on the second — proves the caught
    // panic requeues per policy rather than failing terminally on attempt 1.
    let calls = Arc::new(AtomicU32::new(0));
    let calls_body = calls.clone();
    rt.register_activity(
        "panicky",
        ActivitySpec::new(
            3,
            move |input: serde_json::Value| -> Result<serde_json::Value, String> {
                // Deliberate fault injection: panic on the first attempt only.
                assert!(
                    calls_body.fetch_add(1, Ordering::SeqCst) != 0,
                    "first attempt boom"
                );
                Ok(json!(input.as_i64().unwrap_or_default() * 10))
            },
        ),
    );
    let exec = rt.start_workflow("calls_panicky", json!(4)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(40)),
        "a panic on attempt 1 must retry and the retry succeed, got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "exactly two body invocations"
    );

    // A clean, terminal-only history — no ActivityFailed reached the log (the
    // panic was a retryable attempt, not a terminal failure).
    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .all(|e| !matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "a retried-then-succeeded activity records no terminal ActivityFailed:\n{history:?}"
    );
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the successful retry records ActivityCompleted"
    );
    // The retryable panic IS in the per-attempt audit log.
    let attempts = rt.activity_attempts(exec, "panicky").unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(
        attempts[0].result.is_err(),
        "attempt 1 (panic) recorded as a failure"
    );
    assert!(
        attempts[1].result.is_ok(),
        "attempt 2 recorded as a success"
    );
}

// ── FIX 4: a mixed timer+activity batch records in command order and replays ───

/// Emits BOTH a `ScheduleActivity` and a `StartTimer` in ONE decision cycle via
/// `tokio::join!`. The activity is the FIRST joined future — the order the core
/// determinism engine supports for this interleave. See the module note below on
/// why the reverse (`join!(timer, activity)`) is a pre-existing CORE limitation
/// shared with the Postgres engine, not a backend bug.
#[workflow]
async fn timer_and_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let (act_res, timer_res) = tokio::join!(
        ctx.execute_activity_raw("work", json!(n), "default"),
        ctx.timer("t", 1),
    );
    timer_res.map_err(|e| e.to_string())?;
    let out = act_res.map_err(|e| e.to_string())?;
    Ok(out.as_i64().unwrap_or_default() * 2)
}

/// A single decision cycle emitting BOTH a timer and an activity drives cleanly
/// to completion and the resulting history replays byte-identically on the core
/// `WorkflowReplayer` — proving the backend dispatches a mixed batch in command
/// order and the core matcher tolerates the interleaved sibling terminals
/// (`ActivityCompleted` then `TimerFired`).
///
/// **Command/history-order note (Codex round 4).** `apply_commands` appends a
/// cycle's schedule events in COMMAND order (the `drain_commands()` order), so
/// the interleave the Codex spike saw go Stuck is a poll-order concern, not a
/// schedule-order one: here the activity is the first joined future, so on
/// replay it consumes its `ActivityScheduled`/`ActivityCompleted` before the
/// timer's forward scan runs, and `match_timer_strict` then skips those
/// (consumed) events to reach `TimerFired`. This backend needed NO drain-order
/// change — the append is already command-ordered.
///
/// **Known CORE limitation (documented, not a backend bug).** The REVERSE join
/// order — `tokio::join!(ctx.timer(..), ctx.execute_activity(..))`, i.e. the
/// timer polled first — is unsupported by the *core* determinism engine itself:
/// `HistoryMatcher::match_timer_strict` (replay.rs) BREAKS its forward
/// `TimerFired` scan at an UNCONSUMED `ActivityScheduled` (it skips consumed
/// siblings, child workflows, signals, external, update, and cancellable-timer
/// events, but stops on an unconsumed activity). Verified directly: the core
/// `WorkflowReplayer` rejects even an ideal timer-first history with
/// `NonDeterminismDetected(EarlyCompletion)`. This is shared with the Postgres
/// engine and is out of scope for this backend to change; put the activity
/// branch first in a mixed `join!`.
#[tokio::test]
async fn mixed_timer_activity_batch_completes_and_replays() {
    let (_dir, _path) = temp_db();
    let t0: DateTime<Utc> = DateTime::from_timestamp(6_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&timer_and_activity_info());
    rt.register_activity("work", echo_activity());
    let exec = rt.start_workflow("timer_and_activity", json!(9)).unwrap();

    // Drive 1 at t0: the batch dispatches BOTH commands (ActivityScheduled + task
    // row, TimerStarted + timer row), the activity runs to completion, and the run
    // blocks CLEANLY on the still-armed 1s timer (deadline t0+1, not yet due at
    // t0) — never Stuck.
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingTimer),
        "the mixed batch must dispatch cleanly and block on the timer (not Stuck), got {state:?}"
    );

    // Drive 2 at t0+5: the timer is now due, fires, and the joined workflow
    // resolves to completion.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(5))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(18)),
        "the mixed timer+activity batch must drive to completion, got {state:?}"
    );

    // Both schedule events landed in COMMAND order: the activity was the first
    // joined future, so ActivityScheduled precedes TimerStarted in history.
    let history = rt.load_history(exec).unwrap();
    let as_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
        .expect("ActivityScheduled present");
    let ts_idx = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        .expect("TimerStarted present");
    assert!(
        as_idx < ts_idx,
        "schedule events must be in command order (ActivityScheduled before TimerStarted):\n{history:?}"
    );

    // The core matcher tolerates the interleaved sibling terminals — the history
    // replays byte-identically on the core WorkflowReplayer.
    let report = WorkflowReplayer::new()
        .register_fn("timer_and_activity", timer_and_activity_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a mixed timer+activity history must replay on the core engine:\n{report}"
    );
}
