//! Activity traces from a real worker (issue #2003).
//!
//! The infrastructure-fault tests also run activities, but they need
//! Docker. This test runs on a shared `HARVEST_TEST_DATABASE_URL` too, so
//! the `ActivityClaim` trace check has engine traces on any machine.
//!
//! The workload covers the claim, the start fence, heartbeats, a retry, an
//! orphan reclaim and the finalize path. The test asserts the shape of each
//! trace, so a recorder that logs nothing fails here. `chaos.yml` checks the
//! traces with TLC.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{
    HandlerRegistry, Worker, append_activity_started_for_test, chaos_drive_one_workflow_task,
};
use autumn_harvest::{ExecutionId, ShardId};
use serde_json::{Value, json};

use super::tla_trace::{self, TaskTrace};
use super::{base_params, chaos_db, chaos_noop_info, connect, exec_state};

/// Heartbeats once, then fails attempt 1 when `input.fail_first` is set.
#[activity(
    start_to_close = "30s",
    retry = autumn_harvest::policy::RetryPolicy::fixed(3, Duration::from_millis(100))
)]
async fn trace_step(ctx: &ActivityContext, input: Value) -> Result<Value, String> {
    ctx.heartbeat(json!({ "step": 1 }))
        .await
        .map_err(|e| e.to_string())?;
    // Give the heartbeat flusher time to write the beat.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    if input["fail_first"].as_bool() == Some(true) && ctx.info().attempt == 1 {
        return Err("attempt 1 fails".into());
    }
    Ok(input)
}

/// Runs one [`trace_step`] activity, then completes.
#[workflow]
async fn trace_activity_wf(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    ctx.execute_activity(&trace_step_info(), input)
        .await
        .map_err(|e| e.to_string())
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![trace_activity_wf_info(), chaos_noop_info()],
        vec![trace_step_info()],
    ))
}

/// Start one `trace_activity_wf` run.
async fn start(url: &str, workflow_id: &'static str, input: Value) -> ExecutionId {
    let mut conn = connect(url).await;
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let params = base_params("trace_activity_wf", workflow_id, exec_id, input);
    autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
        .await
        .expect("start trace_activity_wf");
    exec_id
}

/// The number of lines of `trace` with `op`.
fn ops(trace: &TaskTrace, op: &str) -> usize {
    trace.lines.iter().filter(|l| l["op"] == op).count()
}

/// Real-worker activity traces: a retry, and an orphan of a dead worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::significant_drop_tightening)]
#[allow(clippy::too_many_lines)] // One workload, then one shape check per trace.
async fn chaos_trace_real_worker_activities() {
    let (_body, url, _c) = chaos_db().await;
    let mut conn = connect(&url).await;
    tla_trace::install_now(&mut conn).await;
    let codecs = PayloadCodecs::default();

    // The orphan run. Drive its first cycle by hand, so the activity task
    // exists before any worker polls.
    let orphan = start(&url, "trace-act-orphan", json!({})).await;
    let task = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "trace-wf",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the workflow task is due");
    let drive_url = tla_trace::actor_url(&url, task.id, "trace-wf", task.attempt);
    let _ = chaos_drive_one_workflow_task(&drive_url, registry(), task, "trace-wf".into()).await;

    // A dead worker claims and starts the activity. It never registered.
    let dead = autumn_harvest::queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "trace-dead",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim")
    .expect("the activity task is due");
    assert_eq!(dead.task_type, "activity");
    let started = append_activity_started_for_test(
        &mut conn,
        &dead,
        orphan,
        "trace_step",
        "trace-dead",
        &codecs,
    )
    .await
    .expect("start fence");
    assert!(started.is_some(), "the dead worker's claim must start");
    let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
        &mut conn,
        3,
        0,
        None,
        &NoOpMetrics,
        &codecs,
    )
    .await
    .expect("reclaim");
    assert!(
        summary.requeued >= 1,
        "the reclaimer must requeue the orphan"
    );

    // The retry run, then a real worker for both runs.
    let retry = start(&url, "trace-act-retry", json!({ "fail_first": true })).await;
    let mut config =
        crate::integration_e2e::runtime_config("trace-worker", 4, 4, Duration::from_secs(10));
    config.worker_heartbeat_interval = Duration::from_millis(500);
    let worker = Arc::new(Worker::new(config, registry()).expect("worker builds"));
    let handle = crate::integration_e2e::spawn_test_worker(
        Arc::clone(&worker),
        crate::integration_e2e::build_test_pool(&url),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let done = exec_state(&mut conn, orphan).await == "COMPLETED"
            && exec_state(&mut conn, retry).await == "COMPLETED";
        if done {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the runs must complete"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    worker.shutdown();
    let _ = handle.await;

    let traces = tla_trace::take(&mut conn).await;
    let activities: Vec<&TaskTrace> = traces
        .iter()
        .filter(|t| t.spec == "ActivityClaim")
        .collect();
    assert_eq!(
        activities.len(),
        2,
        "one trace per activity task: {traces:#?}"
    );
    for trace in &activities {
        let last = trace.lines.last().expect("lines");
        assert_eq!(last["state"], "COMPLETED", "{trace:#?}");
        assert_eq!(last["terminal"], 1, "{trace:#?}");
        assert_eq!(ops(trace, "start"), 2, "two attempts start: {trace:#?}");
    }
    assert!(
        activities.iter().any(|t| ops(t, "heartbeat") >= 1),
        "a heartbeat must reach the trace: {activities:#?}"
    );
    tla_trace::write("real-worker-activities", &traces, |_| tla_trace::accept());
    tla_trace::uninstall_unless_recording(&mut conn).await;
}
