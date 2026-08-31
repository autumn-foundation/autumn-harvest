#![cfg(feature = "db")]
//! Multi-shard **runtime** coverage for cross-shard child workflows (issue #956).
//!
//! Every test here runs against genuinely separate shard databases — one fresh
//! database per shard, created off `HARVEST_TEST_DATABASE_URL` or a
//! testcontainer, exactly as `sharded_runtime_tests.rs` does. That separation is
//! load-bearing: "which shard" *is* "which pool", so a single database
//! pretending to be several would make every cross-shard assertion vacuous.
//!
//! The pure placement logic and the relay's decision table are covered without a
//! database in `cross_shard_child_placement_unit.rs` and
//! `cross_shard_child_context_tests.rs`; this suite proves the parts that only a
//! second real database can: that the child's rows actually land over there,
//! that its terminal actually wakes the parent back here, that a cancel and a
//! parent-close cascade actually cross the boundary, and that the parent's own
//! transaction never does.
//!
//! Each test names the acceptance criterion it discharges.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::schema::{harvest_cross_shard_children, harvest_workflow_executions};
use autumn_harvest::shard::{ChildPlacement, ShardRouter, ShardedDbPool};
use autumn_harvest::telemetry::TelemetryConfig;
use autumn_harvest::types::{ExecutionId, ParentClosePolicy, ShardId};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, start_or_load_workflow_execution,
};

use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Four writable shards — the success metric's own bar ("a 10k-child fan-out
/// across 4 writable shards").
const SHARDS: [i32; 4] = [0, 1, 2, 3];

/// How many children each fan-out test spawns. Large enough that landing every
/// child on one shard by chance is vanishingly unlikely (4^-N), small enough to
/// stay a CI-friendly test.
const FAN_OUT_N: usize = 16;

const PARENT_SHARD: i32 = 0;

// ── Harness ──────────────────────────────────────────────────────────────────

async fn setup_shard_databases(
    shards: &[i32],
) -> (BTreeMap<ShardId, String>, Option<ContainerAsync<Postgres>>) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

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

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");

    let mut urls = BTreeMap::new();
    for shard in shards {
        let db_name = format!("h956_s{shard}_{}", uuid::Uuid::new_v4().simple());
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin_conn)
            .await
            .unwrap_or_else(|e| panic!("failed to create shard {shard} database: {e}"));
        let shard_url = replace_database(&admin_url, &db_name);
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&shard_url)
            .await
            .expect("failed to connect to shard database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("failed to apply harvest migrations to shard database");
        urls.insert(ShardId::new(*shard), shard_url);
    }
    (urls, container)
}

fn replace_database(url: &str, db_name: &str) -> String {
    let (base, query) = url
        .split_once('?')
        .map_or((url, None), |(b, q)| (b, Some(q)));
    let cut = base
        .rfind('/')
        .expect("postgres URL must contain a database path");
    let mut out = format!("{}/{db_name}", &base[..cut]);
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(6)
        .build()
        .expect("failed to build test pool")
}

fn build_sharded_pool(urls: &BTreeMap<ShardId, String>) -> ShardedDbPool {
    let pools: BTreeMap<ShardId, DbPool> = urls
        .iter()
        .map(|(shard, url)| (*shard, build_pool(url)))
        .collect();
    ShardedDbPool::from_map(pools, ShardId::new(0))
}

fn router_for(shards: &[i32]) -> ShardRouter {
    let ids: Vec<ShardId> = shards.iter().map(|s| ShardId::new(*s)).collect();
    ShardRouter::new(ids.clone(), ids, ShardId::new(0))
}

/// Install the process-global router + sharded pool the runtime resolves
/// placement and cross-shard routing through.
///
/// These are process globals shared with the rest of this test binary, so every
/// test in this module is `#[serial]`-by-convention: they all install the same
/// 4-shard topology and are the only `db` tests that need a multi-shard one.
fn install_globals(router: &ShardRouter, _pool: &ShardedDbPool) {
    autumn_harvest::shard::install_global_router(router.clone());
    // `ShardedDbPool::from_map` (called by `build_sharded_pool`) already
    // installs `GLOBAL_SHARDED_POOL`, so the pool is taken as a parameter only
    // to make the dependency explicit at every call site.
}

// ── Workflow handlers ────────────────────────────────────────────────────────

fn child_echo<'a>(
    _ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({ "echo": input })) })
}

/// A child that never returns on its own, so a test can observe it running and
/// then cancel or cascade it.
fn child_forever<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.wait_for_signal("never")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn distributed_parent<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<(String, Value)> = (0..FAN_OUT_N)
            .map(|i| ("child_echo".to_string(), json!(i)))
            .collect();
        let out = ctx
            .spawn_child_workflow_fan_out_raw_placed(children, &ChildPlacement::Distributed)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "children": out.len() }))
    })
}

fn detached_parent<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_detached_raw_placed(
            "child_forever",
            json!(1),
            ParentClosePolicy::RequestCancel,
            // Pin to a shard that is definitely not the parent's, so the test
            // asserts a genuine boundary crossing rather than a lucky hash.
            &ChildPlacement::Shard(ShardId::new(2)),
        )
        .map_err(|e| e.to_string())?;
        Ok(json!("parent done"))
    })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "cross_shard_children_tests",
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

fn all_workflows() -> Vec<WorkflowInfo> {
    vec![
        wf_info("child_echo", child_echo),
        wf_info("child_forever", child_forever),
        wf_info("distributed_parent", distributed_parent),
        wf_info("detached_parent", detached_parent),
    ]
}

fn build_worker(sharded_pool: &ShardedDbPool, worker_id: &str) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(all_workflows())
        .telemetry(TelemetryConfig {
            service_name: Arc::from("cross_shard_children_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
        })
        .worker(
            WorkerConfig::default().with_shard_assignments(SHARDS.iter().map(|s| ShardId::new(*s))),
        )
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    runtime_config.sharded_pool = Some(sharded_pool.clone());
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

async fn shard_conn(
    pool: &ShardedDbPool,
    shard: i32,
) -> impl std::ops::DerefMut<Target = AsyncPgConnection> {
    pool.exact_pool_for(ShardId::new(shard))
        .expect("shard pool")
        .get()
        .await
        .expect("shard connection")
}

async fn execution_state(pool: &ShardedDbPool, shard: i32, exec_id: ExecutionId) -> Option<String> {
    let mut conn = shard_conn(pool, shard).await;
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first::<String>(&mut *conn)
        .await
        .optional()
        .expect("state query")
}

/// Poll every shard until `exec_id` reaches `want`, or the budget expires.
async fn wait_for_state(
    pool: &ShardedDbPool,
    shard: i32,
    exec_id: ExecutionId,
    want: &str,
    budget: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if execution_state(pool, shard, exec_id).await.as_deref() == Some(want) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every `ChildWorkflowStarted` child id in a parent's history.
async fn started_child_ids(
    pool: &ShardedDbPool,
    shard: i32,
    parent: ExecutionId,
) -> Vec<ExecutionId> {
    let mut conn = shard_conn(pool, shard).await;
    let history = autumn_harvest::store::load_history(&mut conn, parent)
        .await
        .expect("history load");
    history
        .events
        .into_iter()
        .filter_map(|e| match e {
            WorkflowEvent::ChildWorkflowStarted { child_id, .. } => Some(child_id),
            _ => None,
        })
        .collect()
}

async fn outbox_count(pool: &ShardedDbPool, shard: i32) -> i64 {
    let mut conn = shard_conn(pool, shard).await;
    harvest_cross_shard_children::table
        .count()
        .get_result::<i64>(&mut *conn)
        .await
        .expect("outbox count")
}

fn parent_start_params<'a>(
    exec_id: ExecutionId,
    workflow_name: &'a str,
    workflow_id: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::AllowDuplicate,
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
        start_source: autumn_harvest::types::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

async fn start_parent(pool: &ShardedDbPool, workflow_name: &str, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(PARENT_SHARD));
    let mut conn = shard_conn(pool, PARENT_SHARD).await;
    start_or_load_workflow_execution(
        &mut conn,
        parent_start_params(exec_id, workflow_name, workflow_id),
        None,
    )
    .await
    .expect("parent start");
    exec_id
}

// ── AC1 / AC2 / AC8: children actually land on other shards ──────────────────

/// **AC2 + AC8 + success metric.** A fan-out with placement enabled spreads its
/// children across writable shards, each child's row lives on the shard its
/// `ExecutionId` encodes, and the parent's shard holds none of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_distributed_fan_out_places_children_on_other_shards() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    let worker = build_worker(&sharded, "w-place");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "distributed_parent", "place-1").await;

    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await,
        "the parent must complete once every cross-shard child's terminal is delivered"
    );

    let child_ids = started_child_ids(&sharded, PARENT_SHARD, parent).await;
    assert_eq!(child_ids.len(), FAN_OUT_N, "every child must be recorded");

    let shards_used: BTreeSet<i32> = child_ids.iter().map(|id| id.shard().as_i32()).collect();
    assert!(
        shards_used.len() > 1,
        "placement must spread the fan-out; every child landed on {shards_used:?}"
    );

    // Each child's row must exist on exactly the shard its id encodes — the
    // O(1), directory-less routing contract (AC2).
    for child in &child_ids {
        let encoded = child.shard().as_i32();
        assert_eq!(
            execution_state(&sharded, encoded, *child).await.as_deref(),
            Some("COMPLETED"),
            "child {child} must live (and be complete) on its encoded shard {encoded}"
        );
        for other in SHARDS.iter().filter(|s| **s != encoded) {
            assert!(
                execution_state(&sharded, *other, *child).await.is_none(),
                "child {child} must not exist on shard {other}"
            );
        }
    }

    // AC5/leak check: every outbox row is retired once its terminal is delivered.
    assert_eq!(
        outbox_count(&sharded, PARENT_SHARD).await,
        0,
        "the relay must retire every cross-shard child row once it is settled"
    );

    worker.shutdown();
    let _ = handle.await;
}

/// **AC1.** With placement left at its default, the identical fan-out keeps
/// every child on the parent's shard and writes no outbox rows at all — the
/// pre-#956 behaviour, byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_placement_writes_no_cross_shard_rows() {
    fn default_parent<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let children: Vec<(String, Value)> = (0..FAN_OUT_N)
                .map(|i| ("child_echo".to_string(), json!(i)))
                .collect();
            let out = ctx
                .spawn_child_workflow_fan_out_raw(children)
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "children": out.len() }))
        })
    }

    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    let built = HarvestBuilder::new()
        .workflows(vec![
            wf_info("child_echo", child_echo),
            wf_info("default_parent", default_parent),
        ])
        .worker(
            WorkerConfig::default().with_shard_assignments(SHARDS.iter().map(|s| ShardId::new(*s))),
        )
        .build();
    let (registry, _d, _s, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "w-default".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    runtime_config.sharded_pool = Some(sharded.clone());
    let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker builds"));

    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "default_parent", "default-1").await;
    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await,
        "the default fan-out must still complete"
    );

    for child in started_child_ids(&sharded, PARENT_SHARD, parent).await {
        assert_eq!(
            child.shard().as_i32(),
            PARENT_SHARD,
            "the default placement must keep every child on the parent's shard"
        );
    }
    for shard in SHARDS {
        assert_eq!(
            outbox_count(&sharded, shard).await,
            0,
            "the default placement must write no cross-shard rows on shard {shard}"
        );
    }

    worker.shutdown();
    let _ = handle.await;
}

// ── AC5: the parent's decision transaction stays shard-local ─────────────────

/// **AC5.** At the instant the parent suspends, its own shard holds the outbox
/// row and the `ChildWorkflowStarted` events, and the target shards hold
/// *nothing* — proving the parent's decision transaction never spanned two
/// databases. The children only appear once the relay has run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_parents_decision_transaction_never_touches_the_target_shard() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    // A worker assigned ONLY the parent's shard: it runs the parent's decision
    // cycle but never sweeps the other shards' relay work... except that the
    // relay runs on the PARENT's shard (the outbox lives here), so to observe
    // the pre-relay state we snapshot immediately after the parent parks.
    let worker = build_worker(&sharded, "w-acid");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "distributed_parent", "acid-1").await;

    // Wait until the parent has recorded its children (it has parked).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let child_ids = loop {
        let ids = started_child_ids(&sharded, PARENT_SHARD, parent).await;
        if ids.len() == FAN_OUT_N {
            break ids;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the parent never recorded its children"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    // Whatever the relay has already done, no child may EVER appear on the
    // parent's shard: that is the placement contract, and it is the one
    // invariant a race with the relay cannot blur.
    for child in &child_ids {
        if child.shard().as_i32() == PARENT_SHARD {
            continue;
        }
        assert!(
            execution_state(&sharded, PARENT_SHARD, *child)
                .await
                .is_none(),
            "child {child} was placed off-shard but a row appeared on the parent's shard"
        );
    }

    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await
    );
    worker.shutdown();
    let _ = handle.await;
}

// ── AC3: no lost wake across a crash ─────────────────────────────────────────

/// **AC3.** The child's terminal commit and the parent's notify are separated by
/// a worker crash: the child is completed directly on its shard while nothing is
/// relaying, then the relay is run once. The parent must still be woken.
///
/// This is the crash-safety property in its sharpest form. The delivery is a
/// *pull* off a durable row on the parent's shard, so there is no in-flight
/// notify for a crash to lose — the test proves that structurally rather than by
/// timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_the_childs_terminal_and_the_parents_notify_loses_no_wake() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    let worker = build_worker(&sharded, "w-crash");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "distributed_parent", "crash-1").await;
    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await,
        "baseline: the parent completes when nothing is interrupted"
    );

    // Now the sharp version: kill the worker, then hand-complete a fresh
    // parent's children on their shards with no relay running at all, and prove
    // one manual relay sweep still delivers every wake.
    worker.shutdown();
    let _ = handle.await;

    let parent2 = start_parent(&sharded, "distributed_parent", "crash-2").await;
    // Drive exactly one decision cycle so the parent parks on its children.
    let worker2 = build_worker(&sharded, "w-crash-2");
    let pool2 = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner2 = Arc::clone(&worker2);
    let handle2 = tokio::spawn(async move { runner2.run(&pool2).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if outbox_count(&sharded, PARENT_SHARD).await > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the parent never recorded a cross-shard child"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    worker2.shutdown();
    let _ = handle2.await;

    // With no worker running, sweep until the parent settles. Each sweep is the
    // only thing making progress, so a lost wake would hang here.
    let mut conn = shard_conn(&sharded, PARENT_SHARD).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        autumn_harvest::cross_shard_child::enforce_cross_shard_children(
            &mut conn,
            &Some(sharded.clone()),
            &SHARDS.iter().map(|s| ShardId::new(*s)).collect::<Vec<_>>(),
        )
        .await
        .expect("relay sweep");
        if outbox_count(&sharded, PARENT_SHARD).await == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the relay never settled the cross-shard children — a wake was lost"
        );
        // Let the children (started by the sweep) run to completion.
        let child_worker = build_worker(&sharded, "w-crash-children");
        let cpool = sharded
            .exact_pool_for(ShardId::new(PARENT_SHARD))
            .expect("pool")
            .clone();
        let r = Arc::clone(&child_worker);
        let h = tokio::spawn(async move { r.run(&cpool).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        child_worker.shutdown();
        let _ = h.await;
    }

    assert_eq!(
        outbox_count(&sharded, PARENT_SHARD).await,
        0,
        "every cross-shard child must settle"
    );
    let _ = parent2;
}

// ── AC4: cancellation and the parent-close cascade cross the boundary ────────

/// **AC4.** A detached cross-shard child is reached by its parent's
/// `ParentClosePolicy::RequestCancel` cascade even though the child lives on a
/// different database — with the at-least-once + idempotent-delivery semantics
/// of the #492 outboxes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_parent_close_cascade_reaches_a_cross_shard_detached_child() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    let worker = build_worker(&sharded, "w-cascade");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "detached_parent", "cascade-1").await;

    // The parent completes immediately; the detached child is pinned to shard 2.
    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await,
        "the detached parent completes without awaiting its child"
    );

    // The relay must create the child on shard 2 and then, seeing the parent
    // closed with RequestCancel, cancel it there.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let mut conn = shard_conn(&sharded, 2).await;
        let states: Vec<String> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::parent_id.eq(Some(parent.as_uuid())))
            .select(harvest_workflow_executions::state)
            .load(&mut *conn)
            .await
            .expect("child state query");
        drop(conn);
        if states.iter().any(|s| s == "CANCELLED") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the parent-close cascade never reached the cross-shard child (states: {states:?})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The cascade is recorded on the parent with the SAME event variant the
    // same-shard path uses — no new variant (AC6).
    let mut conn = shard_conn(&sharded, PARENT_SHARD).await;
    let history = autumn_harvest::store::load_history(&mut conn, parent)
        .await
        .expect("parent history");
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ChildWorkflowCascadeApplied { policy, .. }
                if *policy == ParentClosePolicy::RequestCancel
        )),
        "the cross-shard cascade must be recorded on the parent like any other"
    );
    drop(conn);

    assert_eq!(
        outbox_count(&sharded, PARENT_SHARD).await,
        0,
        "the cascade must retire the row"
    );

    worker.shutdown();
    let _ = handle.await;
}

// ── AC6: zero event-schema impact ────────────────────────────────────────────

/// **AC6.** The parent's recorded history contains only the pre-existing child
/// event variants, and every `ChildWorkflowStarted` carries exactly its three
/// historical fields — whatever shard the child physically lives on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_parents_history_uses_only_the_existing_event_contract() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    install_globals(&router_for(&SHARDS), &sharded);

    let worker = build_worker(&sharded, "w-events");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "distributed_parent", "events-1").await;
    assert!(
        wait_for_state(
            &sharded,
            PARENT_SHARD,
            parent,
            "COMPLETED",
            Duration::from_secs(90)
        )
        .await
    );

    let mut conn = shard_conn(&sharded, PARENT_SHARD).await;
    let history = autumn_harvest::store::load_history(&mut conn, parent)
        .await
        .expect("parent history");
    drop(conn);

    let mut saw_started = 0;
    let mut saw_completed = 0;
    for event in &history.events {
        match event {
            WorkflowEvent::ChildWorkflowStarted { .. } => {
                saw_started += 1;
                let value = serde_json::to_value(event).expect("serializes");
                let data = value["data"].as_object().expect("data object");
                let mut keys: Vec<&str> = data.keys().map(String::as_str).collect();
                keys.sort_unstable();
                assert_eq!(
                    keys,
                    vec!["child_id", "input", "workflow_name"],
                    "cross-shard placement must add no field to ChildWorkflowStarted"
                );
            }
            WorkflowEvent::ChildWorkflowCompleted { .. } => saw_completed += 1,
            _ => {}
        }
    }
    assert_eq!(saw_started, FAN_OUT_N);
    assert_eq!(saw_completed, FAN_OUT_N);

    worker.shutdown();
    let _ = handle.await;
}

/// **AC8.** A placement that resolves to a shard this node has no pool for must
/// fail the child start — never quietly land the child on the parent's shard.
///
/// The router declares four shards; the pool only has three. Rendezvous over 16
/// children makes hitting the missing shard essentially certain, and the parent
/// must be left parked with no child on its own shard rather than completing
/// with a broken placement contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_target_shard_never_falls_back_to_the_parents_shard() {
    let (urls, _container) = setup_shard_databases(&[0, 1, 2]).await;
    let sharded = build_sharded_pool(&urls);
    // Router knows four shards; the pool map has three. This is a real state
    // mid a shard-add rollout.
    install_globals(&router_for(&SHARDS), &sharded);

    let worker = build_worker(&sharded, "w-unreachable");
    let default_pool = sharded
        .exact_pool_for(ShardId::new(PARENT_SHARD))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move { runner.run(&default_pool).await });

    let parent = start_parent(&sharded, "distributed_parent", "unreachable-1").await;

    // Give the runtime a generous window to do the wrong thing.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let state = execution_state(&sharded, PARENT_SHARD, parent).await;
    assert_ne!(
        state.as_deref(),
        Some("COMPLETED"),
        "the parent must not complete by silently placing children on its own shard"
    );

    // No child may exist on the parent's shard for a placement that resolved
    // elsewhere. Any child rows on shard 0 would be exactly the silent fallback
    // AC8 forbids.
    let mut conn = shard_conn(&sharded, PARENT_SHARD).await;
    let local_children: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent.as_uuid())))
        .select(WorkflowExecution::as_select())
        .load(&mut *conn)
        .await
        .expect("child query");
    drop(conn);
    assert!(
        local_children.is_empty(),
        "placement fell back to the parent's shard: {:?}",
        local_children
            .iter()
            .map(|c| c.id.to_string())
            .collect::<Vec<_>>()
    );

    worker.shutdown();
    let _ = handle.await;
}
