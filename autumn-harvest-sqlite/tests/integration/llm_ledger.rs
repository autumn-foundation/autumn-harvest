//! The agent cost ledger on the `SQLite` backend (issue #2275).
//!
//! The runtime writes one `harvest_llm_ledger` row per LLM call of a
//! completed attempt. The write is in the transaction that appends the
//! completion event, as on Postgres (issue #1996). These tests cover the
//! three ways an activity gives its calls and the outcomes that write no
//! row. They also cover a restart, an older file, the reader and retention.

// The `#[activity]` macro reads its input param, so an unused `_n` reads as a
// used underscore binding.
#![allow(clippy::used_underscore_binding)]
// `#[activity]` needs an `async fn`, also for a body with no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use autumn_harvest::HarvestError;
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{LlmLedgerRow, RunState, SqliteError, SqliteReader, SqliteRuntime};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

/// The stub of a ledger closure. Its body never runs on this backend.
#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
async fn think(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// Two calls of `think`, with a signal wait between them.
#[workflow]
async fn two_thoughts(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let first: i64 = ctx
        .execute_activity(&think_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    ctx.execute_activity(&think_info(), first + 1)
        .await
        .map_err(|e| e.to_string())
}

/// One call of `think`.
#[workflow]
async fn one_thought(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&think_info(), n)
        .await
        .map_err(|e| e.to_string())
}

/// One call of `think` with a 1 ms start-to-close budget. A timeout ends the
/// run with `"timed_out"`.
#[workflow]
async fn hasty_thought(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity_with_opts::<_, i64>(
            &think_info(),
            n,
            None,
            None,
            Some(Duration::from_millis(1)),
        )
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        Err(HarvestError::Timeout { .. }) => Ok("timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

/// An `#[activity]` body that records two calls on its context.
#[activity]
async fn ctx_think(ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    ctx.record_llm_call(LlmCall::new("ctx-model", 5, 6).with_cost_usd_micros(9))?;
    ctx.record_llm_call(LlmCall::new("ctx-model", 7, 8))?;
    Ok(n)
}

/// An `#[activity]` body that records a call, then fails for good.
#[activity]
async fn ctx_fails(ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    ctx.record_llm_call(LlmCall::new("ctx-model", 5, 6))?;
    Err("the model answer is unusable".to_string())
}

/// A local `#[activity]` body that records one call.
#[activity(local = true)]
async fn local_think(ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    ctx.record_llm_call(LlmCall::new("local-model", 3, 4).with_cost_usd_micros(1))?;
    Ok(n)
}

#[workflow]
async fn context_thoughts(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let n: i64 = ctx
        .execute_activity(&ctx_think_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    let n: i64 = ctx
        .execute_local_activity(&local_think_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    // The failure is the outcome under test, so the run goes on.
    let failed = ctx.execute_activity::<_, i64>(&ctx_fails_info(), n).await;
    Ok(if failed.is_err() { n } else { -1 })
}

/// The calls that the ledger closure returns for input `n`.
fn calls_for(n: i64) -> Vec<LlmCall> {
    let n = n.unsigned_abs();
    vec![
        LlmCall::new("m-1", n * 10, n)
            .with_cost_usd_micros(n * 100)
            .with_latency(Duration::from_millis(250)),
        LlmCall::new("m-2", n, 2 * n),
    ]
}

/// Register the workflows and a `think` closure that counts its runs. The
/// first `fail_first` attempts fail.
fn register(rt: &mut SqliteRuntime, runs: &Arc<AtomicU32>, fail_first: u32) {
    rt.register_workflow(&two_thoughts_info());
    rt.register_workflow(&one_thought_info());
    rt.register_workflow(&hasty_thought_info());
    rt.register_workflow(&context_thoughts_info());
    rt.register_activity(&ctx_think_info());
    rt.register_activity(&ctx_fails_info());
    rt.register_activity(&local_think_info());
    let runs = Arc::clone(runs);
    rt.register_activity_with_ledger(&think_info(), move |input: Value, _at| {
        let n = input.as_i64().ok_or("the input is not a number")?;
        if runs.fetch_add(1, Ordering::SeqCst) < fail_first {
            return Err("the provider is down".to_string());
        }
        if n < 0 {
            std::thread::sleep(Duration::from_millis(40));
        }
        Ok((json!(n), calls_for(n)))
    });
}

fn fresh_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ledger.db");
    (dir, path)
}

/// The figures of a row that do not depend on time or position.
const fn figures(row: &LlmLedgerRow) -> (&str, &str, u64, u64, Option<u64>) {
    (
        row.activity_name.as_str(),
        row.model.as_str(),
        row.input_tokens,
        row.output_tokens,
        row.cost_usd_micros,
    )
}

/// `exec` has exactly one row per call that `calls_for` gives for `inputs`.
fn assert_rows_for(rows: &[LlmLedgerRow], inputs: &[i64]) {
    let expected: Vec<_> = inputs
        .iter()
        .flat_map(|n| calls_for(*n))
        .map(|call| {
            (
                "think".to_string(),
                call.model().to_string(),
                call.input_tokens(),
                call.output_tokens(),
                call.cost_usd_micros(),
            )
        })
        .collect();
    let actual: Vec<_> = rows
        .iter()
        .map(|row| {
            let (activity, model, input, output, cost) = figures(row);
            (activity.to_string(), model.to_string(), input, output, cost)
        })
        .collect();
    assert_eq!(actual, expected);
}

// A ledger closure writes one row per call. Each row names the completion
// event of its attempt, and the call index counts from 0 in each attempt.
#[tokio::test]
async fn a_ledger_closure_writes_one_row_per_call_with_its_completion() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let exec = rt.start_workflow("two_thoughts", json!(3)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingSignal(_)
    ));
    rt.send_signal(exec, "go", json!(null)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));

    let rows = rt.llm_ledger(exec).unwrap();
    assert_rows_for(&rows, &[3, 4]);
    let history = rt.load_history(exec).unwrap();
    for row in &rows {
        assert_eq!(row.exec_id, exec);
        let event = &history[usize::try_from(row.event_seq).unwrap()];
        assert!(
            matches!(event, WorkflowEvent::ActivityCompleted { .. }),
            "{row:?} names {event:?}"
        );
    }
    let positions: Vec<_> = rows.iter().map(|row| row.call_index).collect();
    assert_eq!(positions, [0, 1, 0, 1]);
    assert_ne!(rows[0].event_seq, rows[2].event_seq);
    assert_eq!(rows[0].latency_ms, 250, "a set latency is kept");
}

// The completion stamps `recorded_at` from the drive clock. A window read
// returns the rows inside it only.
#[tokio::test]
async fn a_window_read_returns_the_rows_inside_it() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let at = Utc.with_ymd_and_hms(2026, 10, 1, 12, 0, 0).unwrap();
    let exec = rt.start_workflow("one_thought", json!(2)).unwrap();
    assert!(matches!(
        rt.run_until_blocked_as_of(exec, at).await.unwrap(),
        RunState::Completed(_)
    ));

    let rows = rt.llm_ledger(exec).unwrap();
    assert!(rows.iter().all(|row| row.recorded_at == at), "{rows:?}");
    let second = chrono::Duration::seconds(1);
    assert_eq!(rt.llm_ledger_between(at, at + second).unwrap(), rows);
    assert_eq!(rt.llm_ledger_between(at - second, at).unwrap(), Vec::new());
    assert_eq!(
        rt.llm_ledger_between(at + second, at + second * 2).unwrap(),
        Vec::new()
    );
}

// A ledger closure gets the drive clock of its claim (issue #2272). Its rows
// carry the same clock, so a budget window over the ledger can use it.
#[tokio::test]
async fn a_ledger_closure_gets_the_drive_clock_of_its_claim() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&one_thought_info());
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    rt.register_activity_with_ledger(&think_info(), move |input: Value, at| {
        sink.lock().unwrap().push(at);
        let n = input.as_i64().ok_or("the input is not a number")?;
        Ok((json!(n), calls_for(n)))
    });
    let at = Utc.with_ymd_and_hms(2031, 2, 3, 4, 5, 6).unwrap();
    let exec = rt.start_workflow("one_thought", json!(2)).unwrap();
    assert!(matches!(
        rt.run_until_blocked_as_of(exec, at).await.unwrap(),
        RunState::Completed(_)
    ));

    assert_eq!(*seen.lock().unwrap(), [at]);
    let rows = rt.llm_ledger(exec).unwrap();
    assert_rows_for(&rows, &[2]);
    assert!(rows.iter().all(|row| row.recorded_at == at), "{rows:?}");
}

// A failed attempt writes no row. The retry that completes writes its own
// rows once.
#[tokio::test]
async fn a_failed_attempt_writes_no_row_and_its_retry_writes_once() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let runs = Arc::new(AtomicU32::new(0));
    register(&mut rt, &runs, 2);
    let exec = rt.start_workflow("one_thought", json!(5)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));

    assert_eq!(
        runs.load(Ordering::SeqCst),
        3,
        "two failures, then a success"
    );
    assert_rows_for(&rt.llm_ledger(exec).unwrap(), &[5]);
}

// A timed-out attempt writes no row, as on Postgres.
#[tokio::test]
async fn a_timed_out_attempt_writes_no_row() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let exec = rt.start_workflow("hasty_thought", json!(-1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::Completed(out) if *out == json!("timed_out")),
        "{state:?}"
    );
    assert_eq!(rt.llm_ledger(exec).unwrap(), Vec::new());
}

// `ActivityContext::record_llm_call` works on this backend. An `#[activity]`
// body and a local activity write their calls with the completion. The
// context fills each latency. A failed body writes none.
#[tokio::test]
async fn an_activity_context_records_calls_on_both_activity_paths() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let exec = rt.start_workflow("context_thoughts", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(&state, RunState::Completed(out) if *out == json!(1)),
        "{state:?}"
    );

    let rows = rt.llm_ledger(exec).unwrap();
    let got: Vec<_> = rows.iter().map(figures).collect();
    assert_eq!(
        got,
        [
            ("ctx_think", "ctx-model", 5, 6, Some(9)),
            ("ctx_think", "ctx-model", 7, 8, None),
            ("local_think", "local-model", 3, 4, Some(1)),
        ]
    );
    let history = rt.load_history(exec).unwrap();
    let event = |row: &LlmLedgerRow| &history[usize::try_from(row.event_seq).unwrap()];
    assert!(matches!(
        event(&rows[0]),
        WorkflowEvent::ActivityCompleted { .. }
    ));
    assert!(matches!(
        event(&rows[2]),
        WorkflowEvent::LocalActivityCompleted { .. }
    ));
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "ctx_fails ran and failed, so its call is not in the ledger"
    );
}

// A restart between two attempts keeps the rows of the first, and does not
// run or count it again.
#[tokio::test]
async fn a_restart_keeps_each_row_once() {
    let (_dir, db) = fresh_db();
    let runs = Arc::new(AtomicU32::new(0));
    let exec = {
        let mut rt = SqliteRuntime::open(&db).unwrap();
        register(&mut rt, &runs, 0);
        let exec = rt.start_workflow("two_thoughts", json!(1)).unwrap();
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingSignal(_)
        ));
        assert_rows_for(&rt.llm_ledger(exec).unwrap(), &[1]);
        exec
    };

    let mut rt = SqliteRuntime::open(&db).unwrap();
    register(&mut rt, &runs, 0);
    rt.send_signal(exec, "go", json!(null)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));

    assert_eq!(runs.load(Ordering::SeqCst), 2, "each attempt ran once");
    assert_rows_for(&rt.llm_ledger(exec).unwrap(), &[1, 2]);
}

// A file from before issue #2275 has no ledger table. A reader sees no rows
// in it. The next open adds the table, and a completion writes its rows.
#[tokio::test]
async fn an_older_file_gains_the_ledger_on_open() {
    let (_dir, db) = fresh_db();
    let runs = Arc::new(AtomicU32::new(0));
    let old_run = {
        let mut rt = SqliteRuntime::open(&db).unwrap();
        register(&mut rt, &runs, 0);
        rt.start_workflow("one_thought", json!(1)).unwrap()
    };
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("DROP TABLE harvest_llm_ledger;")
            .unwrap();
    }
    let reader = SqliteReader::open(&db).unwrap();
    assert_eq!(reader.llm_ledger(old_run).unwrap(), Vec::new());
    assert_eq!(
        reader
            .llm_ledger_between(DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC)
            .unwrap(),
        Vec::new()
    );
    drop(reader);

    let mut rt = SqliteRuntime::open(&db).unwrap();
    register(&mut rt, &runs, 0);
    assert!(matches!(
        rt.run_until_blocked(old_run).await.unwrap(),
        RunState::Completed(_)
    ));
    assert_rows_for(&rt.llm_ledger(old_run).unwrap(), &[1]);
    let index: i64 = {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'index' AND name = 'idx_harvest_llm_ledger_recorded_at'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(index, 1, "the open builds the window index too");
}

// The read-only reader returns the same rows as the runtime, beside it.
#[tokio::test]
async fn the_reader_reads_the_rows_beside_a_live_runtime() {
    let (_dir, db) = fresh_db();
    let mut rt = SqliteRuntime::open(&db).unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let exec = rt.start_workflow("one_thought", json!(4)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    assert_eq!(
        reader.llm_ledger(exec).unwrap(),
        rt.llm_ledger(exec).unwrap()
    );
    assert!(matches!(
        reader.llm_ledger(ExecutionId::new()),
        Err(SqliteError::ExecutionNotFound(_))
    ));
}

#[tokio::test]
async fn an_unknown_execution_is_not_found() {
    let rt = SqliteRuntime::open_in_memory().unwrap();
    let unknown = ExecutionId::new();
    assert!(matches!(
        rt.llm_ledger(unknown),
        Err(SqliteError::ExecutionNotFound(id)) if id == unknown
    ));
}

// Retention removes the rows with their run, as the Postgres cascade does.
#[tokio::test]
async fn retention_removes_the_rows_with_their_run() {
    let (_dir, db) = fresh_db();
    let mut rt = SqliteRuntime::open(&db).unwrap();
    register(&mut rt, &Arc::new(AtomicU32::new(0)), 0);
    let exec = rt.start_workflow("one_thought", json!(2)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    assert_eq!(rt.llm_ledger(exec).unwrap().len(), 2);

    let removed = rt
        .purge_terminal_as_of(
            Utc::now() + chrono::Duration::hours(2),
            Duration::from_secs(3600),
        )
        .unwrap();
    assert_eq!(removed, 1);
    let left: i64 = rusqlite::Connection::open(&db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM harvest_llm_ledger", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(left, 0);
}
