#![cfg(feature = "db")]
//! Tenant cell isolation under a flood (issue #1837).
//!
//! Acceptance criterion: tenant A's flood does not raise tenant B's
//! schedule-to-start beyond a bound.
//!
//! A cell is one reserved shard and the worker pool assigned to it. Tenant A
//! lives in a cell. Tenant B uses the shared shard. Each tenant has its own
//! task queue, so the `harvest.queue.schedule_to_start{queue}` histogram
//! measures each tenant on its own.
//!
//! Two tests run the same flood:
//!
//! - The cell layout keeps B's worst schedule-to-start under [`BOUND`].
//! - The control puts both tenants on one shared shard and one pool. There,
//!   B waits behind A's backlog and breaks the bound. The control shows that
//!   the flood is real, so the cell result is not vacuous.
//!
//! Execution: honours `HARVEST_TEST_DATABASE_URL` for the admin connection
//! and creates one fresh database per shard. Otherwise it boots a
//! testcontainer and does the same.

use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::shard::{ShardPlacement, ShardRouter, ShardedDbPool};
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, WorkflowContext,
    start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::task::JoinHandle;

/// The worst schedule-to-start tenant B may see while A floods.
///
/// An idle poll loop claims within one 50 ms poll interval. The bound is
/// many intervals wide, so a slow CI runner does not fail the cell test.
/// The control backlog is [`BACKLOG_BEFORE_PROBE`] slow tasks on
/// [`ACTIVITY_SLOTS`] slots, or at least 7.5 s. That is far above the bound,
/// so the control fails it with a wide margin.
const BOUND: Duration = Duration::from_secs(3);

/// Workflows tenant A starts in one burst.
const FLOOD: usize = 100;

/// Tenant A activity tasks that must wait in the queue before B starts.
const BACKLOG_BEFORE_PROBE: i64 = 60;

/// How long one tenant A activity holds an activity slot.
const FLOOD_STEP: Duration = Duration::from_millis(250);

/// Tenant B workflows that probe the schedule-to-start.
const PROBES: usize = 5;

/// Activity slots per worker pool. Small, so the flood saturates them.
const ACTIVITY_SLOTS: usize = 2;

const QUEUE_A: &str = "tenant-a";
const QUEUE_B: &str = "tenant-b";
const SHARED: ShardId = ShardId::new(0);
const CELL_A: ShardId = ShardId::new(1);

// ── Harness ───────────────────────────────────────────────────────────────

/// Create one fresh, migrated database per shard.
async fn setup_shard_databases(
    shards: &[ShardId],
) -> (BTreeMap<ShardId, String>, Option<ContainerAsync<Postgres>>) {
    let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("connect to the admin database");
    let mut urls = BTreeMap::new();
    for shard in shards {
        let db_name = format!(
            "h1837_s{}_{}",
            shard.as_i32(),
            uuid::Uuid::new_v4().simple()
        );
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin)
            .await
            .unwrap_or_else(|e| panic!("create the shard {shard} database: {e}"));
        let url = replace_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect to the shard database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate the shard database");
        urls.insert(*shard, url);
    }
    (urls, container)
}

/// Swap the database name in a Postgres URL.
fn replace_database(url: &str, db_name: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let cut = base.rfind('/').expect("a Postgres URL has a database path");
    let mut out = format!("{}/{db_name}", &base[..cut]);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

fn build_sharded_pool(urls: &BTreeMap<ShardId, String>) -> ShardedDbPool {
    let pools: BTreeMap<ShardId, DbPool> = urls
        .iter()
        .map(|(shard, url)| {
            let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
            let pool = deadpool::managed::Pool::builder(manager)
                .max_size(6)
                .build()
                .expect("build a shard pool");
            (*shard, pool)
        })
        .collect();
    ShardedDbPool::from_map(pools, SHARED)
}

/// Records each `harvest.queue.schedule_to_start` sample by queue.
#[derive(Default)]
struct WaitRecorder {
    samples: Mutex<HashMap<String, Vec<f64>>>,
}

impl WaitRecorder {
    fn samples_for(&self, queue: &str) -> Vec<f64> {
        self.samples
            .lock()
            .unwrap()
            .get(queue)
            .cloned()
            .unwrap_or_default()
    }
}

impl MetricsRecorder for WaitRecorder {
    fn is_enabled(&self) -> bool {
        true
    }

    fn record_schedule_to_start(&self, queue_name: &str, wait_secs: f64) {
        self.samples
            .lock()
            .unwrap()
            .entry(queue_name.to_owned())
            .or_default()
            .push(wait_secs);
    }
}

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Tenant A: one slow activity per workflow.
fn flood_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("flood_step", input, QUEUE_A)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Tenant B: one instant activity per workflow.
fn probe_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("probe_step", input, QUEUE_B)
            .await
            .map_err(|e| e.to_string())
    })
}

fn flood_step(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        tokio::time::sleep(FLOOD_STEP).await;
        Ok(serde_json::json!({ "tenant": "a" }))
    })
}

fn probe_step(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!({ "tenant": "b" })) })
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tenant_cell_isolation_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn activity_info(
    name: &'static str,
    queue: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "tenant_cell_isolation_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(60)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(queue),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

/// A worker pool that serves both tenant queues on `shards` only.
fn build_worker(
    sharded: &ShardedDbPool,
    shards: Vec<ShardId>,
    metrics: Arc<WaitRecorder>,
    worker_id: &str,
) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![
            workflow_info("flood_wf", flood_workflow),
            workflow_info("probe_wf", probe_workflow),
        ])
        .activities(vec![
            activity_info("flood_step", QUEUE_A, flood_step),
            activity_info("probe_step", QUEUE_B, probe_step),
        ])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("tenant_cell_isolation_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics,
        })
        .worker(
            WorkerConfig::default()
                .with_queues([QUEUE_A, QUEUE_B])
                .with_shard_assignments(shards),
        )
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = worker_id.to_string();
    runtime.poll_interval = Duration::from_millis(50);
    runtime.shutdown_timeout = Duration::from_secs(2);
    runtime.max_concurrent_workflows = 8;
    runtime.max_concurrent_activities = ACTIVITY_SLOTS;
    runtime.sharded_pool = Some(sharded.clone());
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("build the worker"))
}

fn spawn(worker: &Arc<Worker>, sharded: &ShardedDbPool) -> JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = sharded
        .exact_pool_for(SHARED)
        .expect("a pool for the shared shard")
        .clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

async fn stop(worker: &Arc<Worker>, handle: JoinHandle<()>) {
    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

fn start_params<'a>(
    exec_id: ExecutionId,
    workflow_name: &'a str,
    workflow_id: &'a str,
    queue_name: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: serde_json::json!({}).into(),
        parent_id: None,
        queue_name,
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::types::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
        tenant: None,
    }
}

/// Start one workflow on the shard the router resolves for `placement`.
async fn start(
    router: &ShardRouter,
    sharded: &ShardedDbPool,
    placement: &ShardPlacement,
    workflow_name: &str,
    workflow_id: &str,
    queue: &str,
) -> ExecutionId {
    let shard = router
        .resolve_placement(placement, workflow_name, workflow_id)
        .expect("the router resolves the placement");
    let exec_id = ExecutionId::new_for_shard(shard);
    let pool = sharded.exact_pool_for(shard).expect("a pool for the shard");
    let mut conn = pool.get().await.expect("a shard connection");
    start_or_load_workflow_execution(
        &mut conn,
        start_params(exec_id, workflow_name, workflow_id, queue),
        None,
    )
    .await
    .expect("start the workflow");
    exec_id
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Wait until `min` tenant A activity tasks are pending on `url`.
async fn wait_for_backlog(url: &str, min: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect for the backlog poll");
    loop {
        let row: Count = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_task_queue \
             WHERE queue_name = $1 AND task_type = 'activity' AND state = 'PENDING'",
        )
        .bind::<diesel::sql_types::Text, _>(QUEUE_A)
        .get_result(&mut conn)
        .await
        .expect("count the backlog");
        if row.n >= min {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the flood never built a backlog of {min} tasks (saw {})",
            row.n
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_completed(url: &str, exec_id: ExecutionId, budget: Duration) -> bool {
    use autumn_harvest::schema::harvest_workflow_executions as wfe;
    let deadline = tokio::time::Instant::now() + budget;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect for the state poll");
    loop {
        let state: Option<String> = wfe::table
            .find(exec_id.as_uuid())
            .select(wfe::state)
            .first(&mut conn)
            .await
            .optional()
            .expect("read the state");
        if state.as_deref() == Some("COMPLETED") {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start [`FLOOD`] tenant A workflows.
///
/// Call this before the workers start. The pools then cannot drain the
/// flood while it is still being written, so the backlog always forms.
async fn flood_tenant_a(router: &ShardRouter, sharded: &ShardedDbPool, placement: &ShardPlacement) {
    for i in 0..FLOOD {
        start(
            router,
            sharded,
            placement,
            "flood_wf",
            &format!("a-{i}"),
            QUEUE_A,
        )
        .await;
    }
}

/// Start [`PROBES`] tenant B workflows with no pin and wait for each.
/// Return tenant B's worst schedule-to-start.
async fn probe_tenant_b(
    router: &ShardRouter,
    sharded: &ShardedDbPool,
    urls: &BTreeMap<ShardId, String>,
    recorder: &WaitRecorder,
) -> Duration {
    let mut probes = Vec::new();
    for i in 0..PROBES {
        let id = format!("b-{i}");
        let exec_id = start(
            router,
            sharded,
            &ShardPlacement::Auto,
            "probe_wf",
            &id,
            QUEUE_B,
        )
        .await;
        assert_eq!(
            exec_id.shard(),
            SHARED,
            "unpinned tenant B start `{id}` must not land in tenant A's cell"
        );
        probes.push(exec_id);
    }
    for exec_id in probes {
        assert!(
            wait_completed(&urls[&SHARED], exec_id, Duration::from_secs(60)).await,
            "tenant B probe {exec_id} never completed"
        );
    }
    let samples = recorder.samples_for(QUEUE_B);
    // Each probe records one workflow task and one activity task at least.
    assert!(
        samples.len() >= 2 * PROBES,
        "expected at least {} tenant B samples, got {}",
        2 * PROBES,
        samples.len()
    );
    let worst = Duration::from_secs_f64(samples.into_iter().fold(0.0, f64::max));
    eprintln!("tenant B worst schedule-to-start: {worst:?} (bound {BOUND:?})");
    worst
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// The issue #1837 acceptance test.
///
/// Tenant A is pinned to a reserved shard with its own pool. Tenant B starts
/// unpinned, so it lands on the shared shard. A floods. B stays under the
/// bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_in_one_cell_does_not_raise_the_other_tenants_schedule_to_start() {
    let (urls, _container) = setup_shard_databases(&[SHARED, CELL_A]).await;
    let sharded = build_sharded_pool(&urls);
    let router = ShardRouter::new(vec![SHARED, CELL_A], vec![SHARED, CELL_A], SHARED)
        .with_residency_map([("cell-a".to_string(), CELL_A)])
        .with_reserved_shards([CELL_A]);

    let shared_metrics = Arc::new(WaitRecorder::default());
    let cell_metrics = Arc::new(WaitRecorder::default());
    let shared_pool = build_worker(
        &sharded,
        vec![SHARED],
        Arc::clone(&shared_metrics),
        "shared",
    );
    let cell_pool = build_worker(&sharded, vec![CELL_A], Arc::clone(&cell_metrics), "cell-a");
    let cell = ShardPlacement::residency_key("cell-a");
    flood_tenant_a(&router, &sharded, &cell).await;
    let shared_run = spawn(&shared_pool, &sharded);
    let cell_run = spawn(&cell_pool, &sharded);
    wait_for_backlog(&urls[&CELL_A], BACKLOG_BEFORE_PROBE).await;
    let worst_b = probe_tenant_b(&router, &sharded, &urls, &shared_metrics).await;

    stop(&shared_pool, shared_run).await;
    stop(&cell_pool, cell_run).await;

    assert!(
        cell_metrics.samples_for(QUEUE_B).is_empty(),
        "the cell pool served tenant B work"
    );
    assert!(
        shared_metrics.samples_for(QUEUE_A).is_empty(),
        "the shared pool served tenant A work"
    );
    assert!(
        worst_b <= BOUND,
        "tenant A's flood raised tenant B's schedule-to-start to {worst_b:?}, \
         above the {BOUND:?} bound"
    );
}

/// Control for the acceptance test.
///
/// Both tenants share one shard and one pool. B queues behind A's backlog,
/// so the same flood breaks the bound. If this test ever passes the bound,
/// the flood is too weak and the acceptance test proves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_flood_on_a_shared_shard_breaks_the_bound() {
    let (urls, _container) = setup_shard_databases(&[SHARED]).await;
    let sharded = build_sharded_pool(&urls);
    let router = ShardRouter::single();

    let metrics = Arc::new(WaitRecorder::default());
    let pool = build_worker(&sharded, vec![SHARED], Arc::clone(&metrics), "shared");
    flood_tenant_a(&router, &sharded, &ShardPlacement::Auto).await;
    let run = spawn(&pool, &sharded);
    wait_for_backlog(&urls[&SHARED], BACKLOG_BEFORE_PROBE).await;
    let worst_b = probe_tenant_b(&router, &sharded, &urls, &metrics).await;

    stop(&pool, run).await;

    assert!(
        worst_b > BOUND,
        "on a shared shard the flood should push tenant B above {BOUND:?}, \
         but the worst wait was {worst_b:?}"
    );
}
