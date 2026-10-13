//! Issue #2022: `SqliteRuntime::query` serves `ctx.register_query` handlers.
//!
//! A query loads the history and replays the run to its frontier. It then
//! calls the handler. It writes nothing.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{ActivitySpec, RunState, SqliteError, SqliteRuntime};
use chrono::{DateTime, TimeDelta, Utc};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};

// ── Test workflows ───────────────────────────────────────────────────────────

/// The typed args of the `typed` query.
#[derive(serde::Deserialize)]
struct Probe {
    k: i64,
}

fn set(stage: &Mutex<Value>, value: Value) {
    *stage.lock().unwrap() = value;
}

/// Sets `stage` before and after each suspension. Registers one query only
/// after the last suspension.
#[workflow]
async fn staged(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let stage = Arc::new(Mutex::new(json!("started")));
    let read = Arc::clone(&stage);
    ctx.register_query("stage", move || read.lock().unwrap().clone());
    ctx.register_query_handler("echo", |req: &Value| Ok::<Value, String>(req.clone()));
    ctx.register_query_handler("fail", |_: &Value| {
        Err::<Value, String>("bad query".to_string())
    });
    ctx.register_query("boom", || panic!("query boom"));
    ctx.register_query_handler("typed", |req: &Probe| Ok::<i64, String>(req.k));

    set(&stage, json!("before-activity"));
    let out = ctx
        .execute_activity_raw("bump", json!(1), "default")
        .await
        .map_err(|e| e.to_string())?;
    set(&stage, json!({ "after_activity": out }));

    let payload = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    set(&stage, json!("after-signal"));
    ctx.register_query("late", || json!("late"));
    Ok(payload)
}

/// Completes at once. It replaces `staged` to model a code change.
#[workflow]
async fn drifted(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("drifted"));
    Ok(json!("drifted"))
}

/// Never suspends on a Harvest future, so a query replay spins.
#[workflow]
async fn spinning(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("spinning"));
    loop {
        tokio::task::yield_now().await;
    }
}

/// Panics after it registers a query.
#[workflow]
async fn panicking(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("panicking"));
    panic!("workflow boom");
}

/// Reports its identity. The values come from the execution row.
#[workflow]
async fn identity(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let info = ctx.info();
    let ids = json!([info.workflow_id, info.workflow_type]);
    ctx.register_query("info", move || ids.clone());
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Waits on a durable timer.
#[workflow]
async fn timed(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let stage = Arc::new(Mutex::new(json!("started")));
    let read = Arc::clone(&stage);
    ctx.register_query("stage", move || read.lock().unwrap().clone());
    set(&stage, json!("before-timer"));
    ctx.timer("t", 60).await.map_err(|e| e.to_string())?;
    set(&stage, json!("after-timer"));
    Ok(json!("done"))
}

/// Reaches an unsupported feature, so the drive seals the run `FAILED`.
#[workflow]
async fn spawns_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let stage = Arc::new(Mutex::new(json!("started")));
    let read = Arc::clone(&stage);
    ctx.register_query("stage", move || read.lock().unwrap().clone());
    set(&stage, json!("before-child"));
    ctx.spawn_child_workflow_raw("child", json!({}))
        .await
        .map_err(|e| e.to_string())
}

/// Like `spawns_child`, with a side effect in the rolled-back cycle.
#[workflow]
async fn spawns_child_after_uuid(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let id = ctx.new_uuid().to_string();
    ctx.register_query("stage", move || json!(id));
    ctx.spawn_child_workflow_raw("child", json!({}))
        .await
        .map_err(|e| e.to_string())
}

/// Mints a value, then continues as new once. The seal records the value.
#[workflow]
async fn mints_then_continues(ctx: &WorkflowContext, phase: i64) -> Result<Value, String> {
    let id = ctx.new_uuid().to_string();
    ctx.register_query("stage", move || json!(id));
    if phase == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Mints a value before each step, until a history cap seals the run.
#[workflow]
async fn mints_until_capped(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let last = Arc::new(Mutex::new(Value::Null));
    let read = Arc::clone(&last);
    ctx.register_query("stage", move || read.lock().unwrap().clone());
    loop {
        set(&last, json!(ctx.new_uuid().to_string()));
        ctx.execute_activity_raw("step", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
}

/// Like `staged`, with a renamed activity. It models a code change.
#[workflow]
async fn renamed_activity(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("renamed"));
    ctx.execute_activity_raw("other", json!(1), "default")
        .await
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Like `staged`, with the activity removed. It models a code change.
#[workflow]
async fn skipped_activity(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("skipped"));
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Exposes a query and waits for `go`. A declarative update serves it.
#[workflow]
async fn updatable(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.register_query("stage", || json!("waiting"));
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[update(workflow = "updatable")]
async fn relabel(_ctx: &WorkflowContext, name: String) -> Result<String, String> {
    Ok(format!("relabeled {name}"))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn temp_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// Opens `path` with `staged` and a counting `bump` activity.
fn open_staged(path: &Path, calls: &Arc<AtomicUsize>) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(path).unwrap();
    rt.register_workflow(&staged_info());
    let calls = Arc::clone(calls);
    rt.register_activity_raw(
        "bump",
        ActivitySpec::new(1, move |v| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!(v.as_i64().unwrap_or_default() + 41))
        }),
    );
    rt
}

/// Starts `staged` and drives it to its signal wait.
async fn parked_run(rt: &mut SqliteRuntime) -> ExecutionId {
    let exec = rt.start_workflow("staged", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "got {state:?}"
    );
    exec
}

/// Every row of every table, as text, in a stable order.
fn snapshot(conn: &Connection) -> Vec<(String, Vec<String>)> {
    let mut tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    let names: Vec<String> = tables
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    names
        .into_iter()
        .map(|table| {
            let mut stmt = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            let width = stmt.column_count();
            let mut rows: Vec<String> = stmt
                .query_map([], |r| {
                    (0..width)
                        .map(|i| r.get_ref(i).map(|v| format!("{v:?}")))
                        .collect::<Result<Vec<_>, _>>()
                        .map(|cells| cells.join("|"))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            rows.sort();
            (table, rows)
        })
        .collect()
}

fn data_version(conn: &Connection) -> i64 {
    conn.query_row("PRAGMA data_version", [], |r| r.get(0))
        .unwrap()
}

// ── AC1: a query returns state set before the last suspension ────────────────

#[tokio::test]
async fn query_returns_state_set_before_the_last_suspension() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = rt.start_workflow("staged", json!(0)).unwrap();

    // No drive yet. The replay reaches the first activity.
    let before = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(before, json!("before-activity"));

    rt.run_until_blocked(exec).await.unwrap();
    let parked = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(
        parked,
        json!({ "after_activity": 42 }),
        "the query must show the state at the signal wait"
    );

    // `late` is registered past the frontier, so it is not yet known.
    let late = rt.query(exec, "late", Value::Null).await.unwrap_err();
    assert!(
        matches!(late, SqliteError::QueryNotFound { ref name, .. } if name == "late"),
        "got {late:?}"
    );
}

#[tokio::test]
async fn query_answer_survives_a_restart() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = open_staged(&path, &calls);
        parked_run(&mut rt).await
    };

    // A fresh runtime has no in-memory state. Only the workflow is registered.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&staged_info());
    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!({ "after_activity": 42 }));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "replay runs no activity body"
    );
}

#[tokio::test]
async fn completed_run_answers_with_its_final_state() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;
    rt.send_signal(exec, "go", json!("done")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");

    let reader = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows_before = snapshot(&reader);
    let version_before = data_version(&reader);
    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!("after-signal"));
    let late = rt.query(exec, "late", Value::Null).await.unwrap();
    assert_eq!(late, json!("late"));
    assert_eq!(
        snapshot(&reader),
        rows_before,
        "a terminal query changes no row"
    );
    assert_eq!(data_version(&reader), version_before);
}

// ── AC2: a query appends no event and changes no row ─────────────────────────

#[tokio::test]
async fn query_changes_no_row_in_any_table() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;
    // A staged signal waits in `harvest_signals`. A query must not take it.
    rt.send_signal(exec, "go", json!("staged")).unwrap();

    let reader = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows_before = snapshot(&reader);
    let version_before = data_version(&reader);
    let history_before = serde_json::to_value(rt.load_history(exec).unwrap()).unwrap();

    rt.query(exec, "stage", Value::Null).await.unwrap();
    rt.query(exec, "echo", json!({ "k": 1 })).await.unwrap();
    rt.query(exec, "nope", Value::Null).await.unwrap_err();
    rt.query(exec, "fail", Value::Null).await.unwrap_err();
    rt.query(exec, "boom", Value::Null).await.unwrap_err();

    assert_eq!(snapshot(&reader), rows_before, "a query must change no row");
    assert_eq!(
        data_version(&reader),
        version_before,
        "a query must commit no transaction"
    );
    assert_eq!(
        serde_json::to_value(rt.load_history(exec).unwrap()).unwrap(),
        history_before
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a query runs no activity body"
    );

    // The staged signal is still there for the next drive.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("staged")),
        "got {state:?}"
    );
    // The probe sees a real write, so the equal reads above are not vacuous.
    assert_ne!(snapshot(&reader), rows_before);
    assert_ne!(data_version(&reader), version_before);
}

// ── AC3: typed errors for an unknown name and an unknown execution ───────────

#[tokio::test]
async fn unknown_query_name_is_query_not_found() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (_dir, path) = temp_db();
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let err = rt.query(exec, "nope", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryNotFound { execution_id, ref name }
                if execution_id == exec && name == "nope"
        ),
        "got {err:?}"
    );
    assert!(err.to_string().contains("nope"), "got: {err}");
}

#[tokio::test]
async fn unknown_execution_is_execution_not_found() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&staged_info());
    let bogus = ExecutionId::new();

    let err = rt.query(bogus, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::ExecutionNotFound(id) if id == bogus),
        "got {err:?}"
    );
    assert!(err.to_string().contains(&bogus.to_string()), "got: {err}");
}

// ── Handler outcomes ─────────────────────────────────────────────────────────

#[tokio::test]
async fn query_args_reach_the_handler() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let echoed = rt
        .query(exec, "echo", json!({ "k": [1, 2] }))
        .await
        .unwrap();
    assert_eq!(echoed, json!({ "k": [1, 2] }));
}

#[tokio::test]
async fn handler_error_is_query_failed() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let err = rt.query(exec, "fail", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryFailed { ref name, ref details, .. }
                if name == "fail" && details == "bad query"
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn typed_handler_with_bad_args_is_query_failed() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let good = rt.query(exec, "typed", json!({ "k": 7 })).await.unwrap();
    assert_eq!(good, json!(7));
    let err = rt.query(exec, "typed", json!("bad")).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryFailed { ref details, .. }
                if details.contains("failed to deserialize query args")
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn handler_panic_is_query_panicked() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let err = rt.query(exec, "boom", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryPanicked { ref name, ref details, .. }
                if name == "boom" && details.contains("query boom")
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn workflow_panic_in_replay_is_query_panicked() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&panicking_info());
    let exec = rt.start_workflow("panicking", json!(0)).unwrap();

    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::QueryPanicked { execution_id, .. } if execution_id == exec),
        "got {err:?}"
    );
}

#[tokio::test]
async fn spinning_replay_is_query_timed_out() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&spinning_info());
    rt.set_query_timeout(Duration::from_millis(50));
    let exec = rt.start_workflow("spinning", json!(0)).unwrap();

    let started = Instant::now();
    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the configured timeout must bound the replay, took {:?}",
        started.elapsed()
    );
    assert!(
        matches!(
            err,
            SqliteError::QueryTimedOut { execution_id, timeout_ms: 50, .. }
                if execution_id == exec
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn unregistered_workflow_is_unknown_workflow() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = open_staged(&path, &calls);
        parked_run(&mut rt).await
    };

    let rt = SqliteRuntime::open(&path).unwrap();
    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnknownWorkflow(ref name) if name == "staged"),
        "got {err:?}"
    );
}

#[tokio::test]
async fn drifted_terminal_run_is_query_unavailable() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = open_staged(&path, &calls);
        let exec = parked_run(&mut rt).await;
        rt.send_signal(exec, "go", json!("done")).unwrap();
        rt.run_until_blocked(exec).await.unwrap();
        exec
    };

    // New code under the old name. It skips the recorded activity.
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&WorkflowInfo {
        name: "staged",
        ..drifted_info()
    });
    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::QueryUnavailable { execution_id, .. } if execution_id == exec),
        "got {err:?}"
    );
}

// ── The future borrows nothing ───────────────────────────────────────────────

#[tokio::test]
async fn query_future_is_send_and_static() {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut rt = open_staged(&path, &calls);
    let exec = parked_run(&mut rt).await;

    let pending = rt.query(exec, "stage", Value::Null);
    // The future reads its rows at call time, so the runtime can go first.
    drop(rt);
    let answer = tokio::spawn(pending).await.unwrap().unwrap();
    assert_eq!(answer, json!({ "after_activity": 42 }));
}

// ── More run shapes ──────────────────────────────────────────────────────────

#[tokio::test]
async fn query_sees_the_run_identity() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&identity_info());
    let exec = rt
        .start_workflow_with_id("identity", "biz-1", json!(0))
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let ids = rt.query(exec, "info", Value::Null).await.unwrap();
    assert_eq!(ids, json!(["biz-1", "identity"]));
}

#[tokio::test]
async fn query_does_not_fire_a_due_timer() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&timed_info());
    let t0 = DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap();
    let exec = rt.start_workflow("timed", json!(0)).unwrap();
    let state = rt.run_until_blocked_as_of(exec, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    // The timer is now due. Only a drive may fire it.
    rt.set_clock(move || t0 + TimeDelta::hours(2));

    let reader = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows_before = snapshot(&reader);
    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!("before-timer"));
    assert_eq!(snapshot(&reader), rows_before, "a query fires no timer");

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
}

#[tokio::test]
async fn unsupported_sealed_run_serves_state_before_the_feature() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&spawns_child_info());
    let exec = rt.start_workflow("spawns_child", json!(0)).unwrap();
    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "got {err:?}");

    let reader = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let rows_before = snapshot(&reader);
    let version_before = data_version(&reader);
    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!("before-child"));
    assert_eq!(
        snapshot(&reader),
        rows_before,
        "a terminal query changes no row"
    );
    assert_eq!(data_version(&reader), version_before);
}

/// The sealed cycle rolled back its side effect. A replay mints a new value
/// each time, so the query refuses rather than give a changing answer.
#[tokio::test]
async fn unrecorded_side_effect_on_a_terminal_run_is_query_unavailable() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&spawns_child_after_uuid_info());
    let exec = rt
        .start_workflow("spawns_child_after_uuid", json!(0))
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap_err();

    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryUnavailable { execution_id, ref details, .. }
                if execution_id == exec && details.contains("side-effect")
        ),
        "got {err:?}"
    );
}

/// The panic budget seals the run `FAILED`. A replay panic on a terminal run
/// recurs on every retry, so it is `QueryUnavailable`, as on Postgres.
#[tokio::test]
async fn workflow_panic_on_a_terminal_run_is_query_unavailable() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&panicking_info());
    let exec = rt.start_workflow("panicking", json!(0)).unwrap();
    let mut sealed = false;
    for _ in 0..5 {
        if let Ok(state) = rt.run_until_blocked(exec).await {
            assert!(matches!(state, RunState::Failed(_)), "got {state:?}");
            sealed = true;
            break;
        }
    }
    assert!(sealed, "the panic budget must seal the run");

    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryUnavailable { execution_id, ref details, .. }
                if execution_id == exec && details.contains("panicked")
        ),
        "got {err:?}"
    );
}

/// The continue-as-new seal records the side effects of its last cycle
/// (issue #2023). A query then serves a stable answer.
#[tokio::test]
async fn continued_as_new_run_serves_a_stable_answer() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&mints_then_continues_info());
    let exec = rt.start_workflow("mints_then_continues", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::ContinuedAsNew(_)),
        "got {state:?}"
    );

    let first = rt.query(exec, "stage", Value::Null).await.unwrap();
    let second = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert!(first.is_string(), "got {first:?}");
    assert_eq!(
        first, second,
        "a recorded side effect replays the same value"
    );
}

/// A history-cap seal rolls back its last cycle, as an unsupported feature
/// does (issue #2023). The replay then mints an unrecorded value.
#[tokio::test]
async fn unrecorded_side_effect_on_a_capped_run_is_query_unavailable() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&mints_until_capped_info());
    rt.register_activity_raw("step", ActivitySpec::new(1, |_| Ok(Value::Null)));
    rt.set_history_event_hard_cap(Some(6));
    let exec = rt.start_workflow("mints_until_capped", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Failed(_)), "got {state:?}");

    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    assert!(
        matches!(
            err,
            SqliteError::QueryUnavailable { execution_id, ref details, .. }
                if execution_id == exec && details.contains("side-effect")
        ),
        "got {err:?}"
    );
}

/// Reopens a parked `staged` run under new code with the old name.
async fn drifted_running_query(new_code: WorkflowInfo) -> (ExecutionId, SqliteError) {
    let (_dir, path) = temp_db();
    let calls = Arc::new(AtomicUsize::new(0));
    let exec = {
        let mut rt = open_staged(&path, &calls);
        parked_run(&mut rt).await
    };
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&WorkflowInfo {
        name: "staged",
        ..new_code
    });
    let err = rt.query(exec, "stage", Value::Null).await.unwrap_err();
    (exec, err)
}

/// A drive of drifted code returns `NonDeterministic`. A query on the same
/// running run must not serve the state that the new code rebuilds.
#[tokio::test]
async fn drifted_running_run_is_non_deterministic() {
    let (exec, err) = drifted_running_query(renamed_activity_info()).await;
    assert!(
        matches!(err, SqliteError::NonDeterministic { execution_id, .. } if execution_id == exec),
        "got {err:?}"
    );
}

#[tokio::test]
async fn running_run_that_skips_a_recorded_command_is_non_deterministic() {
    let (exec, err) = drifted_running_query(skipped_activity_info()).await;
    assert!(
        matches!(err, SqliteError::NonDeterministic { execution_id, .. } if execution_id == exec),
        "got {err:?}"
    );
}

/// A recorded declarative update (issue #2025) does not make the query
/// replay diverge, before or after the run ends.
#[tokio::test]
async fn query_after_a_declarative_update_serves() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&updatable_info());
    rt.register_update(&relabel_info());
    let exec = rt.start_workflow("updatable", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.update(exec, "relabel", json!("a")).await.unwrap();

    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!("waiting"));

    rt.send_signal(exec, "go", json!("done")).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "got {state:?}");
    let answer = rt.query(exec, "stage", Value::Null).await.unwrap();
    assert_eq!(answer, json!("waiting"));
}
