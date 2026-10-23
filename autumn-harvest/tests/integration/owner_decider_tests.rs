#![cfg(feature = "db")]
//! The owner decider of issue #2255.
//!
//! In owner mode, the owner of a partition takes the workflow tasks of that
//! partition by id. The shared claim loop takes activities only. Each test
//! runs on its own database, because owner mode is a per-database switch.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::decision_owner::{self, DecisionPartition, EnableOwnerMode};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{ClaimKind, MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::Value;

use crate::decision_owner_claim_tests::OwnDb;
use crate::integration_e2e::{
    build_test_pool, spawn_test_worker, wait_for_execution_state_with_timeout,
};

/// The longest wait of a test for one condition.
const WAIT: Duration = Duration::from_secs(60);

const STEPS: [&str; 3] = ["decider_step_a", "decider_step_b", "decider_step_c"];

#[activity(start_to_close = "30s")]
async fn decider_step_a(_ctx: &ActivityContext, n: u64) -> Result<u64, String> {
    Ok(n)
}

#[activity(start_to_close = "30s")]
async fn decider_step_b(_ctx: &ActivityContext, n: u64) -> Result<u64, String> {
    Ok(n)
}

#[activity(start_to_close = "30s")]
async fn decider_step_c(_ctx: &ActivityContext, n: u64) -> Result<u64, String> {
    Ok(n)
}

/// Three activities in sequence, the assay 0014 shape. Four decisions.
#[workflow]
async fn decider_three_steps(ctx: &WorkflowContext, queue: String) -> Result<String, String> {
    for name in STEPS {
        ctx.execute_activity_raw(name, serde_json::json!(1), &queue)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok("done".to_string())
}

/// Wait for one signal and return its payload.
#[workflow]
async fn decider_signal_flow(ctx: &WorkflowContext, input: String) -> Result<Value, String> {
    let _ = input;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Counts the claims and the owner takes of one worker by kind.
#[derive(Default, Clone)]
struct Claims(Arc<Mutex<BTreeMap<(&'static str, &'static str), u64>>>);

impl Claims {
    fn get(&self, op: &'static str, kind: ClaimKind) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(op, kind.as_str()))
            .copied()
            .unwrap_or(0)
    }

    fn count(&self, kind: ClaimKind) -> u64 {
        self.get("claim", kind)
    }

    fn takes(&self, kind: ClaimKind) -> u64 {
        self.get("take", kind)
    }

    fn bump(&self, op: &'static str, kind: ClaimKind) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry((op, kind.as_str()))
            .or_default() += 1;
    }
}

impl MetricsRecorder for Claims {
    fn record_claim_duration(&self, kind: ClaimKind, _shard: u16, _seconds: f64) {
        self.bump("claim", kind);
    }

    fn record_owner_take_duration(&self, kind: ClaimKind, _shard: u16, _seconds: f64) {
        self.bump("take", kind);
    }
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// Turn owner mode on. No owner loop is live yet, so the step forces.
async fn enable(conn: &mut AsyncPgConnection) {
    decision_owner::enable_owner_mode(
        conn,
        &EnableOwnerMode {
            force: true,
            ..EnableOwnerMode::default()
        },
    )
    .await
    .expect("enable owner mode");
}

/// A worker that listens for owner wakes on `url`.
struct Node {
    worker: Arc<Worker>,
    claims: Claims,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Node {
    fn new(url: &str, queue: &str, claim_loops: usize) -> Self {
        let claims = Claims::default();
        let built = HarvestBuilder::new()
            .workflows(workflows![decider_three_steps, decider_signal_flow])
            .activities(activities![decider_step_a, decider_step_b, decider_step_c])
            .worker(WorkerConfig::default().with_queues([queue]))
            .telemetry(
                TelemetryConfig::builder()
                    .metrics(Arc::new(claims.clone()))
                    .build(),
            )
            .build();
        let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
        let mut runtime: WorkerRuntimeConfig = worker_config.into();
        runtime.worker_id = unique("decider-worker");
        runtime.poll_interval = Duration::from_millis(100);
        runtime.max_concurrent_workflows = 8;
        runtime.max_concurrent_activities = 16;
        runtime.max_concurrent_claims = claim_loops;
        runtime.worker_heartbeat_interval = Duration::from_secs(1);
        runtime.notification_database_url = Some(url.to_string());
        let worker = Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"));
        Self {
            worker,
            claims,
            task: None,
        }
    }

    fn run(&mut self, pool: &DbPool) {
        self.task = Some(spawn_test_worker(Arc::clone(&self.worker), pool.clone()));
    }

    async fn stop(&mut self) {
        self.worker.shutdown();
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(15), task).await;
        }
    }
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

/// The number of partitions with a live lease.
async fn live_partitions(conn: &mut AsyncPgConnection) -> i64 {
    let row: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_decision_owner WHERE lease_until > clock_timestamp()",
    )
    .get_result(conn)
    .await
    .expect("count live leases");
    row.n
}

/// Wait until `check` holds, up to [`WAIT`].
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = std::time::Instant::now() + WAIT;
    while !check().await {
        assert!(std::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(QueryableByName)]
struct Epoch {
    #[diesel(sql_type = BigInt)]
    epoch: i64,
}

async fn epoch_of(conn: &mut AsyncPgConnection, partition: DecisionPartition) -> i64 {
    let row: Epoch =
        diesel::sql_query("SELECT epoch FROM harvest_decision_owner WHERE partition = $1")
            .bind::<Integer, _>(i32::from(partition.get()))
            .get_result(conn)
            .await
            .expect("read the epoch");
    row.epoch
}

/// Whether the workflow task of `exec` is parked: `RUNNING` with no worker.
async fn parked(conn: &mut AsyncPgConnection, exec: ExecutionId) -> bool {
    let row: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_task_queue WHERE workflow_exec_id = $1 \
         AND task_type = 'workflow' AND state = 'RUNNING' AND worker_id IS NULL",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result(conn)
    .await
    .expect("read the task row");
    row.n == 1
}

#[derive(QueryableByName)]
struct Output {
    #[diesel(sql_type = Text)]
    output: String,
}

/// The JSON output of `exec`, as text.
async fn output_of(conn: &mut AsyncPgConnection, exec: ExecutionId) -> String {
    let row: Output = diesel::sql_query(
        "SELECT COALESCE(output::text, '') AS output FROM harvest_workflow_executions \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result(conn)
    .await
    .expect("read the output");
    row.output
}

async fn signal_events(conn: &mut AsyncPgConnection, exec: ExecutionId) -> i64 {
    let row: Count = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_events WHERE workflow_exec_id = $1 \
         AND event_type = 'SignalReceived'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result(conn)
    .await
    .expect("count signal events");
    row.n
}

/// Done-when item 3 (restate#4566): a signal acknowledged just before an
/// epoch bump is never lost.
///
/// Worker A owns every partition and parks the run on its signal wait. A
/// stops and releases, so no owner listens. The signal commits and
/// `send_signal` returns. That return is the acknowledgement. Its owner wake
/// reaches nobody. Worker B then acquires every partition at a new epoch.
/// B never saw the wake, so only a scan of its new partitions can find the
/// run. B must deliver the signal once, and it must decide off the claim
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_acknowledged_just_before_an_epoch_bump_is_never_lost() {
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let pool: DbPool = build_test_pool(&db.url);
    let queue = unique("decider-signal");

    let mut a = Node::new(&db.url, &queue, 2);
    a.run(&pool);
    eventually("A owns every partition", async || {
        live_partitions(&mut conn).await == 512
    })
    .await;
    let workflow_id = unique("decider-signal-wf");
    let partition = DecisionPartition::for_workflow_id(&workflow_id);
    let client = WorkflowHandleClient::single(pool.clone(), db.url.clone());
    let exec = DeciderSignalFlowStub::start_with_options(
        &mut conn,
        &client,
        &workflow_id,
        String::new(),
        TypedStartOptions {
            queue_name: Some(queue.clone()),
            ..Default::default()
        },
    )
    .await
    .expect("start")
    .exec_id();
    eventually("A parks the run on its signal wait", async || {
        parked(&mut conn, exec).await
    })
    .await;

    a.stop().await;
    assert_eq!(live_partitions(&mut conn).await, 0, "A released everything");
    let released_at = epoch_of(&mut conn, partition).await;

    // The acknowledgement: the row and the wake commit, then the call returns.
    autumn_harvest::signal::send_signal(&mut conn, exec, "go", serde_json::json!("acked"))
        .await
        .expect("the signal is acknowledged");

    let mut b = Node::new(&db.url, &queue, 2);
    b.run(&pool);
    wait_for_execution_state_with_timeout(&db.url, exec, "COMPLETED", WAIT).await;
    let bumped_to = epoch_of(&mut conn, partition).await;
    b.stop().await;

    assert!(
        bumped_to > released_at,
        "B acquired at a new epoch: {released_at} then {bumped_to}"
    );
    assert_eq!(output_of(&mut conn, exec).await, "\"acked\"");
    assert_eq!(signal_events(&mut conn, exec).await, 1, "delivered once");
    assert_eq!(
        b.claims.count(ClaimKind::Workflow),
        0,
        "B decided off the claim path"
    );
    assert!(b.claims.takes(ClaimKind::Workflow) >= 1, "B took the task");
    db.drop_db().await;
}

/// Done-when item 1: claims per 3-activity workflow drop from 7 to 3.
///
/// One owner runs the assay 0014 shape. The claim loops claim the three
/// activities of each run. The owner takes the four workflow tasks by id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_takes_workflow_tasks_off_the_claim_path() {
    let runs: u64 = 40;
    let db = OwnDb::new().await;
    let mut conn = db.conn().await;
    enable(&mut conn).await;
    let pool: DbPool = build_test_pool(&db.url);
    let queue = unique("decider-steps");
    let mut node = Node::new(&db.url, &queue, 2);
    node.run(&pool);
    eventually("the owner owns every partition", async || {
        live_partitions(&mut conn).await == 512
    })
    .await;

    let client = WorkflowHandleClient::single(pool.clone(), db.url.clone());
    let mut started = Vec::new();
    for _ in 0..runs {
        let exec = DeciderThreeStepsStub::start_with_options(
            &mut conn,
            &client,
            &unique("decider-steps-wf"),
            queue.clone(),
            TypedStartOptions {
                queue_name: Some(queue.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("start")
        .exec_id();
        started.push(exec);
    }
    for exec in started {
        wait_for_execution_state_with_timeout(&db.url, exec, "COMPLETED", WAIT).await;
    }
    node.stop().await;

    assert_eq!(
        node.claims.count(ClaimKind::Workflow),
        0,
        "no workflow claim"
    );
    assert_eq!(
        node.claims.count(ClaimKind::Activity),
        3 * runs,
        "three activity claims per run"
    );
    assert_eq!(
        node.claims.takes(ClaimKind::Workflow),
        4 * runs,
        "four owner takes per run"
    );
    db.drop_db().await;
}
