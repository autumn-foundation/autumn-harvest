//! Durability scenarios (AC4) + crash recovery (AC7).
//!
//! No Docker: `SQLite` is embedded via `rusqlite`'s `bundled` feature, and durable
//! timers are driven by an injected "as-of" time (`*_as_of`) so restart tests are
//! deterministic without sleeping.

// The `#[workflow]` macro's generated body references the workflow's input
// parameter, so a deliberately-unused `_n` param reads as a "used underscore
// binding" through the expansion — a macro-interaction false positive.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

// ── Test workflows (real `#[workflow]` fns — the reused determinism core) ─────

/// One activity ("work"), then double its integer output.
#[workflow]
async fn single_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out = ctx
        .execute_activity_raw("work", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    Ok(out.as_i64().ok_or("bad activity output")? * 2)
}

/// Two activities in sequence, the second consuming the first's output.
///
/// AC-D2 words the first durability scenario as "one workflow with **two
/// activities** and a retry". `single_activity` above covers the retry with one
/// activity; this covers the literal shape, which is also the more interesting
/// one: the second activity's input is the first's *recorded* output, so a
/// retry of either must not perturb the value the other sees.
#[workflow]
async fn two_activities(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let doubled = ctx
        .execute_activity_raw("double", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    let incremented = ctx
        .execute_activity_raw("increment", doubled, "default")
        .await
        .map_err(|e| e.to_string())?;
    incremented
        .as_i64()
        .ok_or_else(|| "bad activity output".to_string())
}

/// Arm a durable timer, then return a constant.
#[workflow]
async fn timer_then_done(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    ctx.timer("sleep", 5).await.map_err(|e| e.to_string())?;
    Ok("woke_up".to_string())
}

/// Wait for a signal, then echo its payload.
#[workflow]
async fn wait_then_echo(ctx: &WorkflowContext, _n: i64) -> Result<serde_json::Value, String> {
    let payload = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    Ok(payload)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn temp_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

const fn fixed_now() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap()
}

// ── AC4(a): activity retry, then success ─────────────────────────────────────

#[tokio::test]
async fn activity_retry_then_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = calls.clone();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&single_activity_info());
    rt.register_activity_raw(
        "work",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            // Fail the first attempt, succeed the second.
            if body_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap()))
            }
        }),
    );

    let exec = rt.start_workflow("single_activity", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(42)),
        "expected Completed(42), got {state:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "body ran twice (fail, ok)");

    // The retryable failure lives in the audit log, NOT the replayable event log.
    let attempts = rt.activity_attempts(exec, "work").unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts[0].result.is_err());
    assert!(attempts[1].result.is_ok());
    let scheduled = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(scheduled, 0, "a recovered retry appends no ActivityFailed");
}

// ── AC4(a), literal shape: TWO activities and a retry ────────────────────────

/// The AC-D2 scenario as worded: two activities in one workflow, one of which
/// retries.
///
/// The retry is placed on the *first* activity deliberately. Its recovered
/// output is what the second activity consumes, so a retry that leaked the
/// failed attempt's state — or re-ran the first activity when replaying to
/// reach the second — would corrupt the second's input rather than merely
/// costing an extra call. The assertions therefore pin the *value* the second
/// activity received, not just the final result: `(21 * 2) + 1 == 43`, with
/// `double` called twice (fail, ok) and `increment` called exactly once.
#[tokio::test]
async fn two_activities_with_retry() {
    let double_calls = Arc::new(AtomicUsize::new(0));
    let increment_calls = Arc::new(AtomicUsize::new(0));
    // What the second activity actually saw, so a corrupted hand-off is visible
    // as a wrong input rather than only as a wrong final answer.
    let increment_inputs = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));

    let double_body = double_calls.clone();
    let increment_body = increment_calls.clone();
    let seen = increment_inputs.clone();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&two_activities_info());
    rt.register_activity_raw(
        "double",
        ActivitySpec::new(3, move |input: serde_json::Value| {
            // Fail the first attempt, succeed the second.
            if double_body.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient".to_string())
            } else {
                Ok(json!(input.as_i64().unwrap() * 2))
            }
        }),
    );
    rt.register_activity_raw(
        "increment",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            let n = input.as_i64().unwrap();
            increment_body.fetch_add(1, Ordering::SeqCst);
            seen.lock().unwrap().push(n);
            Ok(json!(n + 1))
        }),
    );

    let exec = rt.start_workflow("two_activities", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(43)),
        "expected Completed(43) = (21 * 2) + 1, got {state:?}"
    );
    assert_eq!(
        double_calls.load(Ordering::SeqCst),
        2,
        "`double` ran twice (fail, then ok)"
    );
    assert_eq!(
        increment_calls.load(Ordering::SeqCst),
        1,
        "`increment` ran exactly once — the first activity's retry must not \
         re-drive the second"
    );
    assert_eq!(
        *increment_inputs.lock().unwrap(),
        vec![42],
        "the second activity must consume the *recovered* output of the first, \
         not the failed attempt's"
    );

    // Both activities are in the replayable log; the retryable failure is not.
    let history = rt.load_history(exec).unwrap();
    let completed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .count();
    assert_eq!(
        completed, 2,
        "both activities must record an ActivityCompleted: {history:#?}"
    );
    let failed = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(failed, 0, "a recovered retry appends no ActivityFailed");
}

// ── AC4(b): a durable timer fires across a restart ───────────────────────────

#[tokio::test]
async fn timer_fires_across_restart() {
    let (_dir, path) = temp_db_path();
    let t0 = fixed_now();

    // First process: arm the timer, then "crash" (drop) before it is due.
    let exec;
    {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&timer_then_done_info());
        exec = rt.start_workflow("timer_then_done", json!(0)).unwrap();
        let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
        assert!(
            matches!(state, RunState::WaitingTimer),
            "timer not yet due, got {state:?}"
        );
    } // rt dropped == process exit; the timer row persists in the file.

    // Second process: reopen the SAME file, advance the as-of time past the
    // absolute deadline (t0 + 5s), and the timer fires — no virtual clock reset,
    // no sleep.
    let mut rt2 = SqliteRuntime::open(&path).unwrap();
    rt2.register_workflow(&timer_then_done_info());
    let state = rt2
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(10))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "woke_up"),
        "timer must fire after restart, got {state:?}"
    );
}

// ── AC4(c): signal delivery unblocks a waiting workflow ──────────────────────

#[tokio::test]
async fn signal_delivery_unblocks_workflow() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&wait_then_echo_info());
    let exec = rt.start_workflow("wait_then_echo", json!(0)).unwrap();

    // Blocks on the signal.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "expected WaitingSignal(go), got {state:?}"
    );

    // Deliver it out of band, then resume.
    rt.send_signal(exec, "go", json!({"ok": true})).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({"ok": true})),
        "expected the echoed payload, got {state:?}"
    );
}

// ── AC4(d) + AC7 crash-AFTER-commit: replay does not re-execute the activity ──

#[tokio::test]
async fn deterministic_replay_after_crash_does_not_reexecute_activity() {
    let (_dir, path) = temp_db_path();
    let calls = Arc::new(AtomicUsize::new(0));

    let exec;
    {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&single_activity_info());
        let body_calls = calls.clone();
        rt.register_activity_raw(
            "work",
            ActivitySpec::new(1, move |input: serde_json::Value| {
                body_calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!(input.as_i64().unwrap()))
            }),
        );
        exec = rt.start_workflow("single_activity", json!(5)).unwrap();

        // Exactly one decision cycle: the activity runs + `ActivityCompleted` is
        // persisted (finalize committed), but the workflow is not yet resumed
        // past it — this is the crash-AFTER-commit state.
        let progressed = rt.poll_once().await.unwrap();
        assert!(progressed);
        assert!(matches!(
            rt.outcome(exec).unwrap(),
            ExecutionOutcome::Running
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "body ran once");
        assert!(
            rt.load_history(exec)
                .unwrap()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        );
    } // crash.

    // Reopen and resume: the workflow replays the recorded `ActivityCompleted`
    // and completes WITHOUT re-running the body.
    let mut rt2 = SqliteRuntime::open(&path).unwrap();
    rt2.register_workflow(&single_activity_info());
    let body_calls = calls.clone();
    rt2.register_activity_raw(
        "work",
        ActivitySpec::new(1, move |_input| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!(-999)) // would corrupt the result if wrongly re-run
        }),
    );
    let state = rt2.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(10)),
        "resume must complete from replay, got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the completed activity must NOT be re-executed on resume"
    );
}

// ── AC7 crash-BEFORE-commit: an orphaned RUNNING task is reclaimed + re-run ───

#[tokio::test]
async fn orphaned_running_task_is_reclaimed_and_body_reruns() {
    let (_dir, path) = temp_db_path();
    let exec;
    {
        // First process: register the workflow but NOT the activity. The cycle
        // persists `ActivityScheduled` + the task row (committed) and the drain
        // CLAIMS the task, then misses the (unregistered) body — which now RELEASES
        // the claim back to PENDING (Codex #1069 P2), leaving the task re-claimable
        // rather than stranded. That release is a *different* recovery path from the
        // one under test here, so we fabricate the genuine crash-BEFORE-finalize
        // state (a task left RUNNING by a process that claimed it and died mid-body)
        // directly below.
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&single_activity_info());
        exec = rt.start_workflow("single_activity", json!(7)).unwrap();
        let err = rt.run_until_blocked(exec).await.unwrap_err();
        assert!(
            err.to_string().contains("unregistered activity"),
            "expected the claim-then-no-body state, got {err}"
        );
    } // crash.

    // Fabricate the crash-BEFORE-finalize orphan: a process claimed the (now
    // PENDING) task, began running its body, and died before the finalize commit —
    // i.e. the row is left RUNNING with no terminal event. Flip it via a raw
    // connection to simulate exactly that stranded state.
    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        let pending: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM harvest_tasks WHERE state = 'PENDING'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            pending, 1,
            "the unregistered-activity claim must have been released to PENDING (FIX 2)"
        );
        raw.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();
        let running: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM harvest_tasks WHERE state = 'RUNNING'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(running, 1, "the task must be stranded RUNNING pre-reclaim");
    }

    // Second process: reopen (which reclaims the orphan → PENDING) and register a
    // counting body. The body re-runs and the workflow completes.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt2 = SqliteRuntime::open(&path).unwrap();
    // open() reclaimed the RUNNING orphan back to PENDING.
    {
        let raw = rusqlite::Connection::open(&path).unwrap();
        let running: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM harvest_tasks WHERE state = 'RUNNING'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(running, 0, "open() must reclaim the orphaned RUNNING task");
    }
    rt2.register_workflow(&single_activity_info());
    let body_calls = calls.clone();
    rt2.register_activity_raw(
        "work",
        ActivitySpec::new(1, move |input: serde_json::Value| {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!(input.as_i64().unwrap()))
        }),
    );
    let state = rt2.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(14)),
        "the reclaimed task's body must re-run and complete, got {state:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the reclaimed body runs (at-least-once recovery)"
    );
}
