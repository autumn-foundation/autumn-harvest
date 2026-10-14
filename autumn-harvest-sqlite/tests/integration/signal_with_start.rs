//! Issue #2028: signal idempotency keys, signal by workflow id, and
//! signal-with-start on the `SQLite` backend.
//!
//! The acceptance criteria:
//!
//! 1. A duplicate keyed signal is staged once.
//! 2. Signal-with-start starts a run when none exists and signals it when one
//!    does, in one transaction.
//! 3. Signal by workflow id resolves the live run and rejects a terminal one.

// The `#[workflow]` macro reads the input parameter, so an unused one is a
// used underscore binding.
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::doc_markdown)]

use std::path::{Path, PathBuf};

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::durable_promise::PromiseSettlement;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{
    ExecutionOutcome, RunState, SignalDelivery, SignalWithStart, SignalWithStartOutcome,
    SqliteError, SqliteRuntime,
};
use serde_json::{Value, json};

// ── Workflows ────────────────────────────────────────────────────────────────

/// Waits for `go` and returns its payload.
#[workflow]
async fn inbox(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Waits for `first`, continues as new, then waits for `go`.
#[workflow]
async fn relay(ctx: &WorkflowContext, phase: i64) -> Result<Value, String> {
    if phase == 0 {
        ctx.wait_for_signal("first")
            .await
            .map_err(|e| e.to_string())?;
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

fn runtime(path: &Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&inbox_info());
    rt.register_workflow(&relay_info());
    rt
}

fn raw(path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(path).unwrap()
}

/// The `harvest_signals` rows of `exec` with this name, delivered or not.
fn staged(path: &Path, exec: ExecutionId, name: &str) -> i64 {
    raw(path)
        .query_row(
            "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND name = ?2",
            rusqlite::params![exec.to_string(), name],
            |r| r.get(0),
        )
        .unwrap()
}

/// Every `harvest_signals` row.
fn all_signals(path: &Path) -> i64 {
    raw(path)
        .query_row("SELECT COUNT(*) FROM harvest_signals", [], |r| r.get(0))
        .unwrap()
}

/// Every `harvest_executions` row of the key, in any state.
fn runs_for_key(path: &Path, name: &str, id: &str) -> i64 {
    raw(path)
        .query_row(
            "SELECT COUNT(*) FROM harvest_executions \
             WHERE workflow_name = ?1 AND workflow_id = ?2",
            rusqlite::params![name, id],
            |r| r.get(0),
        )
        .unwrap()
}

fn all_events(path: &Path) -> i64 {
    raw(path)
        .query_row("SELECT COUNT(*) FROM harvest_events", [], |r| r.get(0))
        .unwrap()
}

fn signal_events(rt: &SqliteRuntime, exec: ExecutionId) -> usize {
    rt.load_history(exec)
        .unwrap()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::SignalReceived { .. }))
        .count()
}

fn completed(state: &RunState) -> Value {
    match state {
        RunState::Completed(v) => v.clone(),
        other => panic!("expected Completed, got {other:?}"),
    }
}

async fn assert_replays(history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register_fn("inbox", inbox_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history must replay on the core engine:\n{report}"
    );
}

fn sws<'a>(
    id: &'a str,
    policy: WorkflowIdReusePolicy,
    payload: Value,
    key: Option<&'a str>,
) -> SignalWithStart<'a> {
    SignalWithStart {
        workflow_name: "inbox",
        workflow_id: id,
        input: json!(0),
        reuse_policy: policy,
        signal_name: "go",
        signal_payload: payload,
        idempotency_key: key,
    }
}

/// Start `inbox` under `id` and park it on its signal wait.
async fn parked_inbox(rt: &mut SqliteRuntime, id: &str) -> ExecutionId {
    let exec = rt.start_workflow_with_id("inbox", id, json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");
    exec
}

// ── AC1: a duplicate keyed signal is staged once ─────────────────────────────

#[tokio::test]
async fn a_duplicate_keyed_signal_is_staged_once() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-1").await;

    let first = rt
        .send_signal_idempotent(exec, "go", json!("a"), Some("hook-1"))
        .unwrap();
    let second = rt
        .send_signal_idempotent(exec, "go", json!("b"), Some("hook-1"))
        .unwrap();
    assert!(first, "the first send stages the signal");
    assert!(!second, "the retry is deduped");
    assert_eq!(staged(&path, exec, "go"), 1);

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(completed(&state), json!("a"), "the first payload wins");
    assert_eq!(signal_events(&rt, exec), 1, "the run sees the signal once");
    assert_replays(rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn a_keyed_retry_after_delivery_stages_nothing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = rt.start_workflow("relay", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert!(
        rt.send_signal_idempotent(exec, "first", json!(1), Some("k"))
            .unwrap()
    );
    // The run ingests `first` at cycle start and stays parked on `go`.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");

    let again = rt
        .send_signal_idempotent(exec, "first", json!(1), Some("k"))
        .unwrap();
    assert!(!again, "a delivered key still dedupes");
    assert_eq!(staged(&path, exec, "first"), 1);
}

#[tokio::test]
async fn a_keyed_retry_after_completion_is_a_no_op() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-2").await;
    rt.send_signal_idempotent(exec, "go", json!(1), Some("k"))
        .unwrap();
    completed(&rt.run_until_blocked(exec).await.unwrap());

    // Postgres inserts before it validates, so a keyed retry of a landed
    // signal succeeds even on a terminal run.
    let again = rt
        .send_signal_idempotent(exec, "go", json!(1), Some("k"))
        .unwrap();
    assert!(!again);

    let err = rt
        .send_signal_idempotent(exec, "go", json!(1), Some("other"))
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::WorkflowNotRunning { .. }),
        "a new key on a terminal run is refused: {err:?}"
    );
}

#[tokio::test]
async fn a_keyed_row_survives_a_terminate_and_still_dedupes() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-3").await;
    rt.send_signal_idempotent(exec, "go", json!(1), Some("k"))
        .unwrap();
    rt.send_signal(exec, "go", json!(2)).unwrap();
    rt.terminate(exec, "stop").unwrap();

    assert_eq!(
        staged(&path, exec, "go"),
        1,
        "the seal keeps the keyed row and deletes the unkeyed one"
    );
    let again = rt
        .send_signal_idempotent(exec, "go", json!(1), Some("k"))
        .unwrap();
    assert!(!again, "the retry dedupes after the seal");
}

#[tokio::test]
async fn distinct_keys_and_unkeyed_signals_each_stage() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-4").await;

    for key in [Some("a"), Some("b"), None, None, Some(""), Some("")] {
        assert!(
            rt.send_signal_idempotent(exec, "go", json!(1), key)
                .unwrap(),
            "{key:?} stages"
        );
    }
    assert_eq!(staged(&path, exec, "go"), 6, "an empty key is no key");
}

#[tokio::test]
async fn a_key_is_scoped_to_its_execution() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let a = parked_inbox(&mut rt, "order-5a").await;
    let b = parked_inbox(&mut rt, "order-5b").await;

    assert!(
        rt.send_signal_idempotent(a, "go", json!(1), Some("k"))
            .unwrap()
    );
    assert!(
        rt.send_signal_idempotent(b, "go", json!(1), Some("k"))
            .unwrap()
    );
}

#[tokio::test]
async fn a_promise_key_on_a_normal_signal_is_refused() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-6").await;

    let err = rt
        .send_signal_idempotent(exec, "go", json!(1), Some("harvest.promise:x"))
        .unwrap_err();
    assert!(matches!(err, SqliteError::InvalidSignal(_)), "{err:?}");
    assert_eq!(staged(&path, exec, "go"), 0);
}

#[tokio::test]
async fn an_unknown_execution_is_not_found() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let err = rt
        .send_signal_idempotent(ExecutionId::new(), "go", json!(1), Some("k"))
        .unwrap_err();
    assert!(matches!(err, SqliteError::ExecutionNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn the_database_rejects_a_duplicate_key() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "order-7").await;
    rt.send_signal_idempotent(exec, "go", json!(1), Some("k"))
        .unwrap();

    let err = raw(&path)
        .execute(
            "INSERT INTO harvest_signals (exec_id, name, payload_json, idempotency_key) \
             VALUES (?1, 'go', '1', 'k')",
            [exec.to_string()],
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE"),
        "the unique index enforces the key: {err}"
    );
}

// ── AC2: signal-with-start ───────────────────────────────────────────────────

#[tokio::test]
async fn signal_with_start_starts_a_run_and_the_run_receives_the_signal() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);

    let out = rt
        .signal_with_start(sws(
            "cart-1",
            WorkflowIdReusePolicy::AllowDuplicate,
            json!("hello"),
            None,
        ))
        .unwrap();
    assert!(out.started_fresh);
    assert!(out.signal_delivered);
    assert_eq!(runs_for_key(&path, "inbox", "cart-1"), 1);

    let state = rt.run_until_blocked(out.exec_id).await.unwrap();
    assert_eq!(completed(&state), json!("hello"));
    assert_replays(rt.load_history(out.exec_id).unwrap()).await;
}

#[tokio::test]
async fn signal_with_start_signals_the_live_run() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "cart-2").await;

    let out = rt
        .signal_with_start(sws(
            "cart-2",
            WorkflowIdReusePolicy::AllowDuplicate,
            json!("x"),
            None,
        ))
        .unwrap();
    assert_eq!(
        out,
        SignalWithStartOutcome {
            exec_id: exec,
            started_fresh: false,
            signal_delivered: true,
        }
    );
    assert_eq!(runs_for_key(&path, "inbox", "cart-2"), 1);
    assert_eq!(
        completed(&rt.run_until_blocked(exec).await.unwrap()),
        json!("x")
    );
}

#[tokio::test]
async fn a_failed_stage_rolls_back_the_start() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    raw(&path)
        .execute_batch(
            "CREATE TRIGGER fail_stage BEFORE INSERT ON harvest_signals \
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();

    let err = rt
        .signal_with_start(sws(
            "cart-3",
            WorkflowIdReusePolicy::AllowDuplicate,
            json!(1),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, SqliteError::Sqlite(_)), "{err:?}");
    assert_eq!(runs_for_key(&path, "inbox", "cart-3"), 0, "no run is left");
    assert_eq!(all_events(&path), 0, "no WorkflowStarted is left");

    raw(&path)
        .execute_batch("DROP TRIGGER fail_stage;")
        .unwrap();
    let out = rt
        .signal_with_start(sws(
            "cart-3",
            WorkflowIdReusePolicy::AllowDuplicate,
            json!(1),
            None,
        ))
        .unwrap();
    assert!(out.started_fresh && out.signal_delivered);
}

#[tokio::test]
async fn a_failed_stage_rolls_back_the_prior_cancel() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let prior = parked_inbox(&mut rt, "cart-4").await;
    let events_before = rt.load_history(prior).unwrap().len();
    raw(&path)
        .execute_batch(
            "CREATE TRIGGER fail_stage BEFORE INSERT ON harvest_signals \
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();

    rt.signal_with_start(sws(
        "cart-4",
        WorkflowIdReusePolicy::TerminateIfRunning,
        json!(1),
        None,
    ))
    .unwrap_err();
    assert!(matches!(
        rt.outcome(prior).unwrap(),
        ExecutionOutcome::Running
    ));
    assert_eq!(rt.load_history(prior).unwrap().len(), events_before);
    assert_eq!(runs_for_key(&path, "inbox", "cart-4"), 1);
}

#[tokio::test]
async fn a_keyed_signal_with_start_retry_stages_once() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let policy = WorkflowIdReusePolicy::AllowDuplicate;

    let first = rt
        .signal_with_start(sws("cart-5", policy, json!("a"), Some("hook")))
        .unwrap();
    let second = rt
        .signal_with_start(sws("cart-5", policy, json!("b"), Some("hook")))
        .unwrap();
    assert!(first.started_fresh && first.signal_delivered);
    assert_eq!(
        second,
        SignalWithStartOutcome {
            exec_id: first.exec_id,
            started_fresh: false,
            signal_delivered: false,
        }
    );
    assert_eq!(staged(&path, first.exec_id, "go"), 1);

    // The run completes. The terminal-prior rule would start a fresh run, but
    // the key is in the chain, so the retry starts nothing.
    completed(&rt.run_until_blocked(first.exec_id).await.unwrap());
    let third = rt
        .signal_with_start(sws("cart-5", policy, json!("c"), Some("hook")))
        .unwrap();
    assert!(!third.started_fresh && !third.signal_delivered);
    assert_eq!(third.exec_id, first.exec_id);
    assert_eq!(runs_for_key(&path, "inbox", "cart-5"), 1);
    assert_eq!(all_signals(&path), 1);
}

#[tokio::test]
async fn a_terminal_prior_gets_a_fresh_run_and_the_signal() {
    for policy in [
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        WorkflowIdReusePolicy::TerminateIfRunning,
    ] {
        let (_dir, path) = temp_db();
        let mut rt = runtime(&path);
        let prior = parked_inbox(&mut rt, "cart-6").await;
        rt.send_signal(prior, "go", json!(0)).unwrap();
        completed(&rt.run_until_blocked(prior).await.unwrap());

        let out = rt
            .signal_with_start(sws("cart-6", policy, json!(1), None))
            .unwrap();
        assert!(out.started_fresh && out.signal_delivered, "{policy:?}");
        assert_ne!(out.exec_id, prior);
        assert!(
            matches!(rt.outcome(prior).unwrap(), ExecutionOutcome::Terminated(ref s) if s == "CONTINUED_AS_NEW"),
            "{policy:?}: the prior is sealed"
        );
        assert_eq!(
            completed(&rt.run_until_blocked(out.exec_id).await.unwrap()),
            json!(1)
        );
    }
}

#[tokio::test]
async fn reject_duplicate_refuses_a_live_run_and_stages_nothing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let prior = parked_inbox(&mut rt, "cart-7").await;

    let err = rt
        .signal_with_start(sws(
            "cart-7",
            WorkflowIdReusePolicy::RejectDuplicate,
            json!(1),
            Some("k"),
        ))
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::AlreadyExists { existing_exec_id, .. } if existing_exec_id == prior),
        "{err:?}"
    );
    assert_eq!(all_signals(&path), 0);
}

#[tokio::test]
async fn terminate_if_running_cancels_the_live_run_and_starts_fresh() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let prior = parked_inbox(&mut rt, "cart-8").await;

    let out = rt
        .signal_with_start(sws(
            "cart-8",
            WorkflowIdReusePolicy::TerminateIfRunning,
            json!(2),
            None,
        ))
        .unwrap();
    assert!(out.started_fresh && out.signal_delivered);
    assert!(matches!(
        rt.load_history(prior).unwrap().last(),
        Some(WorkflowEvent::WorkflowCancelled { .. })
    ));
    assert_eq!(
        completed(&rt.run_until_blocked(out.exec_id).await.unwrap()),
        json!(2)
    );
}

#[tokio::test]
async fn allow_duplicate_failed_only_signals_a_live_run() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let prior = parked_inbox(&mut rt, "cart-9").await;

    let out = rt
        .signal_with_start(sws(
            "cart-9",
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            json!(3),
            None,
        ))
        .unwrap();
    assert_eq!(out.exec_id, prior);
    assert!(!out.started_fresh && out.signal_delivered);
}

#[tokio::test]
async fn a_bad_request_starts_nothing() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let policy = WorkflowIdReusePolicy::AllowDuplicate;

    let err = rt
        .signal_with_start(sws("  ", policy, json!(1), None))
        .unwrap_err();
    assert!(matches!(err, SqliteError::BlankWorkflowId), "{err:?}");

    let mut unknown = sws("cart-10", policy, json!(1), None);
    unknown.workflow_name = "nope";
    let err = rt.signal_with_start(unknown).unwrap_err();
    assert!(matches!(err, SqliteError::UnknownWorkflow(_)), "{err:?}");

    let big = json!("x".repeat(300 * 1024));
    let err = rt
        .signal_with_start(sws("cart-10", policy, big, None))
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::PayloadTooLarge { .. }),
        "{err:?}"
    );

    let err = rt
        .signal_with_start(sws("cart-10", policy, json!(1), Some("harvest.promise:x")))
        .unwrap_err();
    assert!(matches!(err, SqliteError::InvalidSignal(_)), "{err:?}");

    assert_eq!(runs_for_key(&path, "inbox", "cart-10"), 0);
    assert_eq!(all_events(&path), 0);
}

#[tokio::test]
async fn a_repeated_promise_settlement_through_signal_with_start_stages_once() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let prior = parked_inbox(&mut rt, "cart-11").await;
    let name = "harvest.promise:approval";
    let settle = |v: i64| SignalWithStart {
        signal_name: name,
        signal_payload: PromiseSettlement::resolved(json!(v)).to_value(),
        ..sws(
            "cart-11",
            WorkflowIdReusePolicy::AllowDuplicate,
            json!(0),
            None,
        )
    };

    let first = rt.signal_with_start(settle(1)).unwrap();
    let second = rt.signal_with_start(settle(2)).unwrap();
    assert_eq!(first.exec_id, prior);
    assert!(first.signal_delivered);
    assert!(!second.signal_delivered);
    assert_eq!(staged(&path, prior, name), 1);
}

// ── AC3: signal by workflow id ───────────────────────────────────────────────

#[tokio::test]
async fn signal_by_workflow_id_reaches_the_live_run() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let exec = parked_inbox(&mut rt, "acct-1").await;

    let delivery = rt
        .send_signal_by_workflow_id("inbox", "acct-1", "go", json!("hi"), None)
        .unwrap();
    assert_eq!(
        delivery,
        SignalDelivery {
            exec_id: exec,
            delivered: true,
        }
    );
    assert_eq!(
        completed(&rt.run_until_blocked(exec).await.unwrap()),
        json!("hi")
    );
}

#[tokio::test]
async fn signal_by_workflow_id_rejects_a_terminal_run() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);

    let done = parked_inbox(&mut rt, "acct-2").await;
    rt.send_signal(done, "go", json!(0)).unwrap();
    completed(&rt.run_until_blocked(done).await.unwrap());
    let cancelled = parked_inbox(&mut rt, "acct-3").await;
    rt.cancel(cancelled, "stop").unwrap();
    rt.run_until_blocked(cancelled).await.unwrap();
    let terminated = parked_inbox(&mut rt, "acct-4").await;
    rt.terminate(terminated, "stop").unwrap();

    for (id, exec, state) in [
        ("acct-2", done, "COMPLETED"),
        ("acct-3", cancelled, "CANCELLED"),
        ("acct-4", terminated, "TERMINATED"),
    ] {
        let before = all_signals(&path);
        let err = rt
            .send_signal_by_workflow_id("inbox", id, "go", json!(1), None)
            .unwrap_err();
        assert!(
            matches!(&err, SqliteError::WorkflowNotRunning { execution_id, state: s }
                if *execution_id == exec && s == state),
            "{id}: {err:?}"
        );
        assert_eq!(all_signals(&path), before, "{id}: nothing staged");
    }
}

#[tokio::test]
async fn signal_by_workflow_id_with_no_run_is_not_found() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    parked_inbox(&mut rt, "acct-5").await;

    for (name, id) in [("inbox", "missing"), ("relay", "acct-5")] {
        let err = rt
            .send_signal_by_workflow_id(name, id, "go", json!(1), None)
            .unwrap_err();
        assert!(
            matches!(&err, SqliteError::WorkflowIdNotFound { workflow_name, workflow_id }
                if workflow_name == name && workflow_id == id),
            "{err:?}"
        );
    }
    let err = rt
        .send_signal_by_workflow_id("inbox", "", "go", json!(1), None)
        .unwrap_err();
    assert!(matches!(err, SqliteError::BlankWorkflowId), "{err:?}");
}

#[tokio::test]
async fn signal_by_workflow_id_follows_a_continue_as_new() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let first = rt
        .start_workflow_with_id("relay", "acct-6", json!(0))
        .unwrap();
    rt.run_until_blocked(first).await.unwrap();

    let hop = rt
        .send_signal_by_workflow_id("relay", "acct-6", "first", json!(0), Some("k1"))
        .unwrap();
    assert_eq!(hop.exec_id, first);
    let RunState::ContinuedAsNew(successor) = rt.run_until_blocked(first).await.unwrap() else {
        panic!("the run continues as new");
    };
    rt.run_until_blocked(successor).await.unwrap();

    // A retry of the delivered key stages nothing on the successor.
    let retry = rt
        .send_signal_by_workflow_id("relay", "acct-6", "first", json!(0), Some("k1"))
        .unwrap();
    assert_eq!(
        retry,
        SignalDelivery {
            exec_id: first,
            delivered: false,
        }
    );
    assert_eq!(staged(&path, successor, "first"), 0);

    let go = rt
        .send_signal_by_workflow_id("relay", "acct-6", "go", json!("done"), Some("k2"))
        .unwrap();
    assert_eq!(
        go,
        SignalDelivery {
            exec_id: successor,
            delivered: true,
        }
    );
    assert_eq!(
        completed(&rt.run_until_blocked(successor).await.unwrap()),
        json!("done")
    );
}

#[tokio::test]
async fn signal_by_workflow_id_prefers_the_live_run() {
    let (_dir, path) = temp_db();
    let mut rt = runtime(&path);
    let old = parked_inbox(&mut rt, "acct-7").await;
    rt.terminate(old, "stop").unwrap();
    // A terminated run leaves the key free, so this is a new run.
    let live = parked_inbox(&mut rt, "acct-7").await;
    assert_ne!(old, live);

    let delivery = rt
        .send_signal_by_workflow_id("inbox", "acct-7", "go", json!(1), None)
        .unwrap();
    assert_eq!(delivery.exec_id, live);
}
