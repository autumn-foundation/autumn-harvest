#![cfg(feature = "db")]
#![allow(clippy::unused_async)]
//! A worker whose tenure is stale is fenced in its decision commit (issue
//! #2251).
//!
//! The worker holds a tenure at an epoch that another holder has replaced.
//! Its first decision must not write. The worker drops the tenure, counts
//! the fence and re-pends the task. It does not retry at the stale epoch.
//! Owner mode is still off (issue #2252), so the next claim has no tenure
//! and completes the run on the legacy path.
//!
//! With owner mode on and no tenure, a decision fails closed and writes
//! nothing.
//!
//! A body-timeout quarantine with no tenure re-pends the task. It does not
//! fail the run, and it does not strand the claim.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnerHolder, OwnerLeaseConfig,
};
use autumn_harvest::prelude::*;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use diesel::QueryableByName;
use diesel::sql_types::Integer;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state_with_timeout,
};

/// Partitions that other owner suites use. This suite picks another one.
const RESERVED: std::ops::RangeInclusive<u16> = 400..=499;

#[workflow]
async fn owner_fenced_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    let _ = ctx;
    Ok(format!("done:{input}"))
}

/// Counts each fenced decision, by shard label.
#[derive(Default, Clone)]
struct FenceCounter {
    fenced: Arc<Mutex<Vec<String>>>,
}

impl MetricsRecorder for FenceCounter {
    fn record_decision_owner_fenced(&self, shard: &str) {
        self.fenced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(shard.to_string());
    }
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

/// A workflow id whose partition no other owner suite uses.
fn free_workflow_id() -> (String, DecisionPartition) {
    loop {
        let id = unique("owner-fenced");
        let p = DecisionPartition::for_workflow_id(&id);
        if !RESERVED.contains(&p.get()) {
            return (id, p);
        }
    }
}

/// The event types of a run, in history order.
async fn event_types(conn: &mut AsyncPgConnection, exec: ExecutionId) -> Vec<String> {
    #[derive(QueryableByName)]
    struct Type {
        #[diesel(sql_type = diesel::sql_types::Text)]
        t: String,
    }
    diesel::sql_query(
        "SELECT event_data->>'type' AS t FROM harvest_events \
         WHERE workflow_exec_id = $1 ORDER BY event_id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .load::<Type>(conn)
    .await
    .expect("read the history")
    .into_iter()
    .map(|r| r.t)
    .collect()
}

/// A worker, its metrics and its owner tenures.
struct Fixture {
    url: String,
    pool: DbPool,
    conn: AsyncPgConnection,
    queue: String,
    worker_id: String,
    metrics: FenceCounter,
}

impl Fixture {
    async fn new() -> (Self, Option<impl Sized>) {
        let (url, container) = setup_test_database_url_or_env().await;
        let pool: DbPool = build_test_pool(&url);
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (
            Self {
                url,
                pool,
                conn,
                queue: unique("owner-q"),
                worker_id: unique("owner-worker"),
                metrics: FenceCounter::default(),
            },
            container,
        )
    }

    /// Start `workflow_id` on the queue. No worker runs yet.
    async fn start(&mut self, workflow_id: &str) -> ExecutionId {
        let client = WorkflowHandleClient::single(self.pool.clone(), self.url.clone());
        let opts = TypedStartOptions {
            queue_name: Some(self.queue.clone()),
            ..Default::default()
        };
        OwnerFencedFlowStub::start_with_options(
            &mut self.conn,
            &client,
            workflow_id,
            "x".to_string(),
            opts,
        )
        .await
        .expect("start")
        .exec_id()
    }

    /// Build the worker. `owned` sets up its tenures before it runs.
    fn worker(
        &self,
        owned: impl FnOnce(&decision_owner::OwnedPartitions),
    ) -> (Arc<Worker>, Arc<decision_owner::OwnedPartitions>) {
        let built = HarvestBuilder::new()
            .workflows(workflows![owner_fenced_flow])
            .worker(WorkerConfig::default().with_queues([self.queue.as_str()]))
            .telemetry(
                TelemetryConfig::builder()
                    .metrics(Arc::new(self.metrics.clone()))
                    .build(),
            )
            .build();
        let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
        let tenures = registry.decision_owner();
        owned(&tenures);
        let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
        runtime_config.worker_id.clone_from(&self.worker_id);
        runtime_config.poll_interval = Duration::from_millis(50);
        let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker"));
        (worker, tenures)
    }

    fn fences(&self) -> Vec<String> {
        self.metrics
            .fenced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

async fn free_row(conn: &mut AsyncPgConnection, partition: DecisionPartition) {
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET holder = NULL, lease_until = '-infinity' \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(partition.get()))
    .execute(conn)
    .await
    .expect("free the row");
}

#[tokio::test]
async fn a_stale_tenure_is_fenced_unloaded_and_never_retried() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, partition) = free_workflow_id();
    // A control run on the legacy path gives the expected history. It must
    // not share the fenced partition.
    let control_id = loop {
        let (id, p) = free_workflow_id();
        if p != partition {
            break id;
        }
    };
    let exec = fx.start(&workflow_id).await;
    let control = fx.start(&control_id).await;
    // A single-pool worker keys its tenures by the pool shard.
    let shard = decision_owner::OwnerKey::pool_shard(None);

    // The worker takes the row, its lease lapses, and a peer takes it over.
    free_row(&mut fx.conn, partition).await;
    let config = OwnerLeaseConfig::default();
    let mine = OwnerHolder::for_worker(&fx.worker_id);
    let Acquisition::Acquired(stale) =
        decision_owner::acquire(&mut fx.conn, shard, &mine, partition, &config)
            .await
            .expect("acquire")
    else {
        panic!("the free row is acquired");
    };
    diesel::sql_query(
        "UPDATE harvest_decision_owner SET lease_until = clock_timestamp() \
         WHERE partition = $1",
    )
    .bind::<Integer, _>(i32::from(partition.get()))
    .execute(&mut fx.conn)
    .await
    .expect("lapse the lease");
    let peer = OwnerHolder::for_worker("owner-peer");
    let Acquisition::Acquired(_live) =
        decision_owner::acquire(&mut fx.conn, shard, &peer, partition, &config)
            .await
            .expect("take over")
    else {
        panic!("the lapsed row is taken over");
    };

    let (worker, owned) = fx.worker(|owned| {
        owned.adopt(stale.clone());
    });
    let task = spawn_test_worker(Arc::clone(&worker), fx.pool.clone());
    for run in [exec, control] {
        let done = wait_for_execution_state_with_timeout(
            &fx.url,
            run,
            "COMPLETED",
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(
            done.output.as_ref().map(ToString::to_string),
            Some(r#""done:x""#.to_string())
        );
    }

    // The fenced decision dropped the tenure and counted the fence once.
    assert!(
        owned.tenure_for(stale.key()).is_none(),
        "the tenure is unloaded"
    );
    assert_eq!(fx.fences(), [shard.as_i32().to_string()], "one fence");
    // The fenced decision wrote nothing, so the history equals a clean run.
    assert_eq!(
        event_types(&mut fx.conn, exec).await,
        event_types(&mut fx.conn, control).await
    );
    // The run completed, so the fenced claim went back to PENDING. A claim
    // left RUNNING would wait for the orphan sweep of a dead worker.

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
}

/// With owner mode on and no tenure, a decision fails closed. It writes
/// nothing until the worker may decide again.
#[tokio::test]
async fn owner_mode_without_a_tenure_fails_closed() {
    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, _partition) = free_workflow_id();
    let exec = fx.start(&workflow_id).await;
    // A single-pool worker keys its tenures by the pool shard.
    let shard = decision_owner::OwnerKey::pool_shard(None);

    let (worker, owned) = fx.worker(|owned| owned.set_owner_mode(shard, true));
    let task = spawn_test_worker(Arc::clone(&worker), fx.pool.clone());
    for _ in 0..400 {
        if !fx.fences().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!fx.fences().is_empty(), "the decision fails closed");
    assert_eq!(
        event_types(&mut fx.conn, exec).await,
        ["WorkflowStarted"],
        "a fenced decision writes nothing"
    );

    // Owner mode off: the legacy path completes the run.
    owned.set_owner_mode(shard, false);
    wait_for_execution_state_with_timeout(&fx.url, exec, "COMPLETED", Duration::from_secs(30))
        .await;
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
}

/// A body-timeout quarantine with no tenure re-pends the task.
///
/// The quarantine is a decision write, so it needs the tenure of the run.
/// With owner mode on and no tenure, it must not fail the run. It must also
/// not leave the claim `RUNNING`, as only the stuck-task sweep would then
/// free it.
#[tokio::test]
async fn a_quarantine_without_a_tenure_repends_the_task() {
    #[derive(QueryableByName)]
    struct Task {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        error: Option<String>,
    }
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    let (mut fx, _container) = Fixture::new().await;
    let (workflow_id, _partition) = free_workflow_id();
    let exec = fx.start(&workflow_id).await;
    let shard = decision_owner::OwnerKey::pool_shard(None);

    // The worker holds the claim at attempt 1 when its body times out.
    let task = diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, \
         attempt = 1, started_at = NOW() WHERE workflow_exec_id = $1 \
         RETURNING id, state, error",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .bind::<diesel::sql_types::Text, _>(&fx.worker_id)
    .get_result::<Task>(&mut fx.conn)
    .await
    .expect("claim the task");
    let owned = decision_owner::OwnedPartitions::default();
    owned.set_owner_mode(shard, true);

    let recovery = autumn_harvest::worker::quarantine_workflow_task_timeout_owned(
        &fx.pool,
        task.id,
        Some(exec.as_uuid()),
        &fx.worker_id,
        1,
        "",
        1,
        10,
        "owner_fenced_flow",
        &fx.queue,
        &fx.metrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
        &owned,
        shard,
    )
    .await;
    assert_eq!(
        recovery,
        autumn_harvest::worker::ClaimRecovery::ClaimLost,
        "the next owner decides"
    );

    let after = diesel::sql_query("SELECT id, state, error FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .get_result::<Task>(&mut fx.conn)
        .await
        .expect("read the task");
    assert_eq!(after.state, "PENDING", "the claim is not stranded");
    assert!(
        after
            .error
            .as_deref()
            .is_some_and(|e| e.contains("owner fence")),
        "the re-pend names the fence: {:?}",
        after.error
    );
    let dead = diesel::sql_query(
        "SELECT count(*) AS n FROM harvest_dead_letters WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
    .get_result::<Count>(&mut fx.conn)
    .await
    .expect("count dead letters");
    assert_eq!(dead.n, 0, "the run is not quarantined");
    assert_eq!(
        event_types(&mut fx.conn, exec).await,
        ["WorkflowStarted"],
        "the run did not fail"
    );
}
