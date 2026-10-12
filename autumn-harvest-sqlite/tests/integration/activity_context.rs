//! `register_activity(&info)` runs the `#[activity]` body itself (issue #2024).
//!
//! The body gets a real `ActivityContext`. These tests prove the body runs, that
//! `info()` reports the dispatch values, and that retry and heartbeat state
//! reach the next attempt.

// The `#[activity]` macro reads its input param, so an unused `_n` reads as a
// used underscore binding.
#![allow(clippy::used_underscore_binding)]
// `#[activity]` needs an `async fn`, also for a body with no `.await`.
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC;
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// Runs `exec` to a terminal state and returns its output.
async fn completed(rt: &mut SqliteRuntime, exec: ExecutionId) -> Value {
    match rt.run_until_blocked(exec).await.unwrap() {
        RunState::Completed(v) => v,
        other => panic!("the run must complete, got {other:?}"),
    }
}

/// The `ActivityExecId` of the first `ActivityScheduled` event.
fn scheduled_activity_id(history: &[WorkflowEvent]) -> ActivityExecId {
    history
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityScheduled { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .expect("the run schedules an activity")
}

// ── The body runs ─────────────────────────────────────────────────────────────

#[activity]
async fn triple(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(n * 3)
}

#[workflow]
async fn triple_flow(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&triple_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn the_activity_body_itself_runs() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&triple_flow_info());
    rt.register_activity(&triple_info());
    let exec = rt.start_workflow("triple_flow", json!(14)).unwrap();

    assert_eq!(completed(&mut rt, exec).await, json!(42));
}

// ── info() reports the dispatch values ────────────────────────────────────────

#[activity(start_to_close = "1h")]
async fn report_info(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    let info = ctx.info();
    ctx.check_cancellation().await.map_err(|e| e.to_string())?;
    Ok(json!({
        "execution_id": info.execution_id.to_string(),
        "workflow_id": info.workflow_id,
        "workflow_type": info.workflow_type,
        "activity_type": info.activity_type,
        "activity_id": info.activity_id.to_string(),
        "attempt": info.attempt,
        "max_attempts": info.max_attempts,
        "is_local": info.is_local,
        "queue_name": info.queue_name,
        "task_id": info.task_id.map(|id| id.to_string()),
        "deadline": ctx.deadline().map(|d| d.to_rfc3339()),
        "idempotency_key": ctx.idempotency_key().map_err(|e| e.to_string())?.as_str(),
        "cancelled": ctx.is_cancelled(),
    }))
}

#[workflow]
async fn info_flow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&report_info_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn info_reports_the_dispatch_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("info.db");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&info_flow_info());
    rt.register_activity(&report_info_info());
    let exec = rt.start_workflow_with_id("info_flow", "order-7", json!(1)).unwrap();

    let before = Utc::now();
    let report = completed(&mut rt, exec).await;
    let after = Utc::now();
    let activity_id = scheduled_activity_id(&rt.load_history(exec).unwrap());
    drop(rt);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let task_id: String = conn
        .query_row("SELECT task_id FROM harvest_tasks", [], |r| r.get(0))
        .unwrap();

    assert_eq!(report["execution_id"], json!(exec.to_string()));
    assert_eq!(report["workflow_id"], json!("order-7"));
    assert_eq!(report["workflow_type"], json!("info_flow"));
    assert_eq!(report["activity_type"], json!("report_info"));
    assert_eq!(report["activity_id"], json!(activity_id.to_string()));
    assert_eq!(report["attempt"], json!(1));
    assert_eq!(report["max_attempts"], json!(1));
    assert_eq!(report["is_local"], json!(false));
    assert_eq!(report["queue_name"], json!("default"));
    assert_eq!(report["task_id"], json!(task_id));
    assert_eq!(
        report["idempotency_key"],
        json!(IdempotencyKey::from_activity_exec_id(activity_id).as_str())
    );
    assert_eq!(report["cancelled"], json!(false));

    // The deadline is the claim instant plus the one-hour budget.
    let deadline: DateTime<Utc> = report["deadline"].as_str().unwrap().parse().unwrap();
    let budget = chrono::Duration::hours(1);
    assert!(deadline >= before + budget - chrono::Duration::seconds(1));
    assert!(deadline <= after + budget);
}

// ── Retry state reaches the next attempt ──────────────────────────────────────

#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
async fn flaky(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    let attempt = ctx.attempt();
    if attempt < 3 {
        return Err(format!("boom {attempt}"));
    }
    Ok(json!({
        "attempt": attempt,
        "max_attempts": ctx.max_attempts(),
        "previous_failure": ctx.previous_failure(),
        "is_last_attempt": ctx.is_last_attempt(),
    }))
}

#[workflow]
async fn flaky_flow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&flaky_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn attempt_and_previous_failure_advance_across_retries() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&flaky_flow_info());
    rt.register_activity(&flaky_info());
    let exec = rt.start_workflow("flaky_flow", json!(0)).unwrap();

    assert_eq!(
        completed(&mut rt, exec).await,
        json!({
            "attempt": 3,
            "max_attempts": 3,
            "previous_failure": "boom 2",
            "is_last_attempt": true,
        })
    );
}

// ── Heartbeat checkpoints reach the next attempt ──────────────────────────────

#[activity(retry = RetryPolicy::fixed(2, Duration::ZERO))]
async fn checkpointing(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    let resumed: Option<Value> = ctx.heartbeat_details().map_err(|e| e.to_string())?;
    if ctx.attempt() == 1 {
        ctx.heartbeat(json!({"done": 3}))
            .await
            .map_err(|e| e.to_string())?;
        ctx.heartbeat(json!({"done": 4}))
            .await
            .map_err(|e| e.to_string())?;
        return Err(format!("first attempt resumed from {resumed:?}"));
    }
    Ok(json!({ "resumed": resumed }))
}

#[workflow]
async fn checkpoint_flow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&checkpointing_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn the_last_heartbeat_reaches_the_retry() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&checkpoint_flow_info());
    rt.register_activity(&checkpointing_info());
    let exec = rt.start_workflow("checkpoint_flow", json!(0)).unwrap();

    assert_eq!(
        completed(&mut rt, exec).await,
        json!({ "resumed": { "done": 4 } })
    );
}

/// Sends far more heartbeats than the channel holds, then fails once.
#[activity(retry = RetryPolicy::fixed(2, Duration::ZERO))]
async fn chatty(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    if ctx.attempt() == 1 {
        for done in 0..500 {
            ctx.heartbeat(json!({ "done": done }))
                .await
                .map_err(|e| e.to_string())?;
        }
        return Err("first attempt".to_string());
    }
    let resumed: Option<Value> = ctx.heartbeat_details().map_err(|e| e.to_string())?;
    Ok(json!({ "resumed": resumed }))
}

#[workflow]
async fn chatty_flow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&chatty_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn a_heartbeat_burst_neither_blocks_nor_loses_the_last_payload() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&chatty_flow_info());
    rt.register_activity(&chatty_info());
    let exec = rt.start_workflow("chatty_flow", json!(0)).unwrap();

    let out = tokio::time::timeout(Duration::from_secs(30), completed(&mut rt, exec))
        .await
        .expect("a full heartbeat channel must not block the drive");
    assert_eq!(out, json!({ "resumed": { "done": 499 } }));
}

/// Attempt 1 checkpoints and then hangs. Attempt 2 returns the checkpoint.
#[activity]
async fn hangs_after_checkpoint(ctx: &ActivityContext, _n: i64) -> Result<Value, String> {
    if let Some(resumed) = ctx
        .heartbeat_details::<Value>()
        .map_err(|e| e.to_string())?
    {
        return Ok(json!({ "resumed": resumed }));
    }
    ctx.heartbeat(json!({"done": 9}))
        .await
        .map_err(|e| e.to_string())?;
    std::future::pending::<()>().await;
    unreachable!("a pending future never resolves")
}

#[workflow]
async fn crash_flow(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.execute_activity(&hangs_after_checkpoint_info(), n)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn a_heartbeat_checkpoint_survives_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("crash.db");

    let exec = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        rt.register_workflow(&crash_flow_info());
        rt.register_activity(&hangs_after_checkpoint_info());
        let exec = rt.start_workflow("crash_flow", json!(0)).unwrap();
        // The body hangs, so the drive never returns. Dropping it is the crash.
        let drive = tokio::time::timeout(Duration::from_millis(200), rt.run_until_blocked(exec));
        assert!(drive.await.is_err(), "the hung body must block the drive");
        exec
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&crash_flow_info());
    rt.register_activity(&hangs_after_checkpoint_info());

    assert_eq!(
        completed(&mut rt, exec).await,
        json!({ "resumed": { "done": 9 } })
    );
}

// ── A panic is contained ──────────────────────────────────────────────────────

#[activity]
async fn panics_after_await(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    tokio::task::yield_now().await;
    panic!("kaboom");
}

#[workflow]
async fn panic_flow(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx.execute_activity::<_, i64>(&panics_after_await_info(), n).await {
        Ok(_) => Ok("completed".to_string()),
        Err(e) => Ok(format!("failed: {e}")),
    }
}

#[tokio::test]
async fn a_panicking_body_records_a_handler_panic() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&panic_flow_info());
    rt.register_activity(&panics_after_await_info());
    let exec = rt.start_workflow("panic_flow", json!(0)).unwrap();

    let out = completed(&mut rt, exec).await;
    assert!(out.as_str().unwrap().starts_with("failed:"), "{out}");

    let failure = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .find_map(|e| match e {
            WorkflowEvent::ActivityFailed {
                error, error_type, ..
            } => Some((error, error_type)),
            _ => None,
        })
        .expect("the panic records ActivityFailed");
    assert_eq!(failure.1, ERROR_TYPE_HANDLER_PANIC);
    assert!(failure.0.contains("kaboom"), "{}", failure.0);
}

// ── The closure path keeps working ────────────────────────────────────────────

#[tokio::test]
async fn register_activity_with_body_runs_the_closure_not_the_handler() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&triple_flow_info());
    rt.register_activity_with_body(&triple_info(), |input| {
        Ok(json!(input.as_i64().unwrap() + 1))
    });
    let exec = rt.start_workflow("triple_flow", json!(14)).unwrap();

    assert_eq!(completed(&mut rt, exec).await, json!(15));
}
