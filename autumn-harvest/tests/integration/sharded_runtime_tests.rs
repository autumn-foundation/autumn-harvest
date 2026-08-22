#![cfg(feature = "db")]
//! Multi-shard runtime coverage (issue #961).
//!
//! Issue #961 asks the multi-shard runtime to *provably* drain every writable
//! shard. Most of the architecture shipped piecemeal under #522 (per-shard poll
//! loops, per-shard control loops, the stranded-work sampler), #796/#1157
//! (per-shard scheduler ticks and DAG->shard pinning), and #797 (per-shard
//! scanner liveness) — but there was **zero** multi-shard *runtime* test
//! coverage: `sharding_unit.rs` exercises only the pure router, and
//! `scheduler_ha_tests.rs` contains the string "shard" zero times.
//!
//! This suite is the falsifiable bar. Each test names the acceptance criterion
//! it discharges.
//!
//! Execution: honours `HARVEST_TEST_DATABASE_URL` (a migrated Postgres) for the
//! **admin** connection and creates one fresh database per shard off it;
//! otherwise boots a testcontainer and does the same. Every test therefore gets
//! genuinely separate shard databases rather than one database pretending to be
//! several — the distinction matters, because "which shard" *is* "which pool"
//! (there is no `shard_id` column on `harvest_task_queue`).

use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::scheduler::{SchedulerMonitor, tick_once_sharded};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// One `(workflow_id, count)` row of the per-slot fire tally (AC3).
#[derive(QueryableByName)]
struct SlotRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A single `COUNT(*)` scalar.
#[derive(QueryableByName)]
struct TerminalCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// The shard set every test in this suite uses. Three shards is the success
/// metric's own bar ("in a 3-shard integration deployment ...").
const SHARDS: [i32; 3] = [0, 1, 2];

// ── Harness ───────────────────────────────────────────────────────────────

/// Boots (or reuses) a Postgres and creates one **fresh database per shard**.
///
/// Returns the per-shard URLs plus the container guard (kept alive for the
/// test's duration). Separate databases are load-bearing: a single database
/// shared by three "shards" would make every cross-shard assertion vacuous,
/// since the runtime distinguishes shards purely by which pool it claims from.
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
        let db_name = format!("h961_s{shard}_{}", uuid::Uuid::new_v4().simple());
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

/// Swap the database component of a Postgres URL.
fn replace_database(url: &str, db_name: &str) -> String {
    // `postgres://user:pass@host:port/dbname[?params]`
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

/// A metrics recorder that captures exactly the two per-shard signals this
/// suite asserts on.
#[derive(Default)]
struct ShardMetrics {
    dispatched: Mutex<HashMap<u16, u64>>,
    stranded: Mutex<HashMap<u16, u64>>,
    /// `harvest.schedule.fire_attempts{outcome}` counts (issue #350). The
    /// metric carries no `shard` label, so this is a fleet-wide total — see
    /// [`per_shard_scheduler_ha_fires_each_slot_exactly_once_under_two_replicas`]
    /// for why a global `claimed` count is still a per-slot exclusivity proof.
    fire_attempts: Mutex<HashMap<String, u64>>,
}

impl ShardMetrics {
    fn dispatched_for(&self, shard: i32) -> u64 {
        let shard = u16::try_from(shard).unwrap_or(0);
        *self.dispatched.lock().unwrap().get(&shard).unwrap_or(&0)
    }

    fn stranded_for(&self, shard: i32) -> u64 {
        let shard = u16::try_from(shard).unwrap_or(0);
        *self.stranded.lock().unwrap().get(&shard).unwrap_or(&0)
    }

    fn fire_attempts_for(&self, outcome: &str) -> u64 {
        *self
            .fire_attempts
            .lock()
            .unwrap()
            .get(outcome)
            .unwrap_or(&0)
    }
}

impl MetricsRecorder for ShardMetrics {
    fn is_enabled(&self) -> bool {
        true
    }

    fn record_shard_dispatched(&self, shard: u16) {
        *self.dispatched.lock().unwrap().entry(shard).or_insert(0) += 1;
    }

    fn record_shard_stranded_pending(&self, shard: u16, count: u64) {
        self.stranded.lock().unwrap().insert(shard, count);
    }

    fn record_schedule_fire_attempt(&self, _schedule: &str, outcome: &str) {
        *self
            .fire_attempts
            .lock()
            .unwrap()
            .entry(outcome.to_owned())
            .or_insert(0) += 1;
    }
}

fn start_params<'a>(
    exec_id: ExecutionId,
    workflow_name: &'a str,
    workflow_id: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
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
    }
}

// ── Workflow handlers ─────────────────────────────────────────────────────

/// Completes immediately — the workflow-side of "did this shard get drained?".
fn instant_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(serde_json::json!({ "ok": true })) })
}

fn instant_info() -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "shard_probe",
        module: "sharded_runtime_tests",
        handler: instant_workflow,
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

/// Build a worker whose runtime config is fully explicit (no builder default
/// poll interval), pointed at `sharded_pool`, with the given assignments.
fn build_worker(
    sharded_pool: &ShardedDbPool,
    assignments: Vec<ShardId>,
    metrics: Arc<ShardMetrics>,
    worker_id: &str,
) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![instant_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sharded_runtime_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics,
        })
        .worker(WorkerConfig::default().with_shard_assignments(assignments))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    runtime_config.sharded_pool = Some(sharded_pool.clone());
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

async fn wait_for_state(url: &str, exec_id: ExecutionId, want: &str, budget: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("connect for state poll");
        let row: Option<WorkflowExecution> = harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .optional()
            .expect("state poll query");
        if row.is_some_and(|r| r.state == want) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── AC1 ───────────────────────────────────────────────────────────────────

/// **AC1.** One worker with **no explicit shard assignments** must drain
/// workflows on *every* writable shard.
///
/// Scope note: the #961 success metric words this as "dispatch within ≤ 1 poll
/// interval on all three". The round-robin loop meets that by construction —
/// every iteration visits every assigned shard — but wall-clock latency is not
/// assertable on shared CI, so this test certifies the *coverage* half ("zero
/// permanently-undispatched workflows") against a generous deadline rather
/// than pretending to time a 50 ms budget.
///
/// This is the headline silent failure #961 exists to close: before the auto
/// resolution, `WorkerConfig::default().shard_assignments` was a literal
/// `[ShardId::new(0)]`, so this exact deployment shape (three shards in the
/// pool, no `with_shard_assignments` call) drained shard 0 and left shards 1
/// and 2 permanently undispatched with no error anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_shard_assignments_drain_every_writable_shard() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());

    // Deliberately NO with_shard_assignments call -> auto.
    let worker = build_worker(&sharded, Vec::new(), Arc::clone(&metrics), "w-auto");
    assert_eq!(
        worker.config.shard_assignments.len(),
        SHARDS.len(),
        "an unconfigured worker must auto-resolve to every pool shard, got {:?}",
        worker.config.shard_assignments,
    );

    let default_pool = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    // Seed exactly one workflow on each shard, in that shard's own database.
    let mut execs = Vec::new();
    for shard in SHARDS {
        let shard_id = ShardId::new(shard);
        let exec_id = ExecutionId::new_for_shard(shard_id);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(exec_id, "shard_probe", &format!("probe-shard-{shard}")),
            None,
        )
        .await
        .expect("start workflow on shard");
        execs.push((shard, exec_id));
    }

    for (shard, exec_id) in &execs {
        let url = &urls[&ShardId::new(*shard)];
        assert!(
            wait_for_state(url, *exec_id, "COMPLETED", Duration::from_secs(30)).await,
            "workflow on shard {shard} was never dispatched — an auto-assigned \
             worker must drain every shard in its pool (issue #961 AC1)",
        );
    }

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    // AC5: the per-shard dispatch counter must have fired on every shard.
    for shard in SHARDS {
        assert!(
            metrics.dispatched_for(shard) > 0,
            "harvest.shard.dispatched must be emitted for shard {shard}",
        );
    }
}

/// **AC1 (narrowing).** An **explicit** assignment is an operator decision and
/// must never be widened by auto-resolution — the one-worker-process-per-shard
/// deployment shape depends on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_shard_assignment_is_never_widened() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());

    let worker = build_worker(
        &sharded,
        vec![ShardId::new(1)],
        Arc::clone(&metrics),
        "w-narrow",
    );
    assert_eq!(worker.config.shard_assignments, vec![ShardId::new(1)]);

    let default_pool = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    // One workflow on the covered shard, one on an uncovered shard.
    let covered = ExecutionId::new_for_shard(ShardId::new(1));
    let uncovered = ExecutionId::new_for_shard(ShardId::new(2));
    for (shard, exec_id, wf_id) in [
        (1, covered, "narrow-covered"),
        (2, uncovered, "narrow-other"),
    ] {
        let shard_id = ShardId::new(shard);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(exec_id, "shard_probe", wf_id),
            None,
        )
        .await
        .expect("start workflow");
    }

    assert!(
        wait_for_state(
            &urls[&ShardId::new(1)],
            covered,
            "COMPLETED",
            Duration::from_secs(30)
        )
        .await,
        "the explicitly assigned shard must still be drained",
    );
    // The uncovered shard must NOT have been drained: this is what proves the
    // narrowing was honoured rather than silently widened back to all shards.
    assert!(
        !wait_for_state(
            &urls[&ShardId::new(2)],
            uncovered,
            "COMPLETED",
            Duration::from_secs(2)
        )
        .await,
        "an explicitly narrowed worker must not claim from an unassigned shard",
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

// ── AC5 ───────────────────────────────────────────────────────────────────

/// **AC5.** A deep backlog on one assigned shard must not starve dispatch on
/// its siblings.
///
/// The multi-shard poll loop rotates its start index so a shard that just
/// claimed is tried *last* on the next hot iteration — a round-robin, which is
/// a strictly stronger equal-share guarantee than #515's weighted queue
/// draining.
///
/// **Why the backlog is self-replenishing.** A *finite* backlog does not
/// falsify anything: even a strictly shard-0-first loop drains 40 queued tasks
/// in well under the assertion window and then reaches the siblings, so the
/// test passes with rotation disabled. Here a feeder task keeps shard 0
/// permanently non-empty for the whole run, so a non-rotating loop would claim
/// from shard 0 on *every* iteration and never poll shards 1 and 2 at all. The
/// test therefore asserts both halves of the guarantee:
///
/// * the two singleton workflows on the starvable shards complete, **and**
/// * shard 0 still had claimable pending work at that moment — proving the
///   siblings were served *while* shard 0 was hot, not after it drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn deep_backlog_on_one_shard_does_not_starve_the_others() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());
    let worker = build_worker(&sharded, Vec::new(), Arc::clone(&metrics), "w-fair");

    // Deep backlog on shard 0, seeded BEFORE the worker starts so the loop
    // faces a fully-loaded shard from its very first iteration.
    let shard0 = ShardId::new(0);
    let pool0 = sharded.exact_pool_for(shard0).expect("shard 0 pool");
    for i in 0..40 {
        let mut conn = pool0.get().await.expect("shard 0 conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(
                ExecutionId::new_for_shard(shard0),
                "shard_probe",
                &format!("backlog-seed-{i}"),
            ),
            None,
        )
        .await
        .expect("seed backlog");
    }

    // One workflow each on the two "starvable" shards.
    let mut singletons = Vec::new();
    for shard in [1, 2] {
        let shard_id = ShardId::new(shard);
        let exec_id = ExecutionId::new_for_shard(shard_id);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(exec_id, "shard_probe", &format!("starvable-{shard}")),
            None,
        )
        .await
        .expect("seed singleton");
        singletons.push((shard, exec_id));
    }

    // Keep shard 0 hot for the whole run: a tight-loop feeder outpaces the
    // worker, which claims at most one task per loop iteration.
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = {
        let pool0 = pool0.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut fed = 0usize;
            while !AtomicBool::load(&stop, Ordering::Relaxed) && fed < 5_000 {
                let Ok(mut conn) = pool0.get().await else {
                    break;
                };
                let ok = start_or_load_workflow_execution(
                    &mut conn,
                    start_params(
                        ExecutionId::new_for_shard(shard0),
                        "shard_probe",
                        &format!("backlog-feed-{fed}"),
                    ),
                    None,
                )
                .await
                .is_ok();
                if !ok {
                    break;
                }
                fed += 1;
            }
            fed
        })
    };

    let default_pool = pool0.clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    for (shard, exec_id) in &singletons {
        assert!(
            wait_for_state(
                &urls[&ShardId::new(*shard)],
                *exec_id,
                "COMPLETED",
                Duration::from_secs(30)
            )
            .await,
            "shard {shard} was starved by shard 0's continuously-replenished \
             backlog — the round-robin poll order must guarantee forward \
             progress on every assigned shard (issue #961 AC5)",
        );
    }

    // Shard 0 must still be hot: the siblings were served *while* it had work,
    // not because the loop got to them after draining it.
    let mut conn0 = pool0.get().await.expect("shard 0 conn");
    let pending: Vec<TerminalCount> =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_task_queue WHERE state = 'PENDING'")
            .load(&mut conn0)
            .await
            .expect("shard 0 pending count");
    drop(conn0);
    let pending = match pending.as_slice() {
        [row, ..] => row.n,
        [] => 0,
    };

    stop.store(true, Ordering::Relaxed);
    let fed = tokio::time::timeout(Duration::from_secs(30), feeder)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(0);

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert!(
        pending > 0,
        "shard 0 had drained ({fed} fed) by the time the sibling shards were \
         served — this run did not actually exercise starvation, so it cannot \
         falsify the round-robin guarantee (issue #961 AC5)",
    );

    // Every shard must show dispatch activity, not just the backlogged one.
    for shard in SHARDS {
        assert!(
            metrics.dispatched_for(shard) > 0,
            "shard {shard} recorded zero dispatches — it was starved",
        );
    }
}

// ── AC6 ───────────────────────────────────────────────────────────────────

/// **AC6.** A writable shard with pending work and no covering live worker must
/// surface a **named** undrained-shard signal within a bounded window.
///
/// The signal's *name* is the point: `harvest.shard.stranded_pending{shard}`
/// identifies which shard, so an operator is not left guessing. The literal
/// "≤ 2 poll intervals" of the success metric is a cadence target set by the
/// sampler interval and the fleet-liveness staleness window; this test asserts
/// the signal appears within a CI-safe bound, not that bound itself. See
/// [`killing_the_only_covering_worker_surfaces_stranded_pending`] for the
/// success metric's literal *scenario*.
///
/// The worker covers only shard 0, so shards 1 and 2 hold claimable pending
/// work nobody polls. The stranded-work sampler must report a non-zero
/// `harvest.shard.stranded_pending{shard}` for those shards and `0` for the
/// covered one — the metric backing the `harvest_shard_undrained` alert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncovered_shard_surfaces_stranded_pending_within_a_bounded_window() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());
    let worker = build_worker(
        &sharded,
        vec![ShardId::new(0)],
        Arc::clone(&metrics),
        "w-cover0",
    );

    // Pending work on every shard, including the two nobody covers.
    for shard in SHARDS {
        let shard_id = ShardId::new(shard);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(
                ExecutionId::new_for_shard(shard_id),
                "shard_probe",
                &format!("stranded-{shard}"),
            ),
            None,
        )
        .await
        .expect("seed pending work");
    }

    let default_pool = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    // The sampler runs on the worker's poll interval (50 ms here); allow a
    // generous multiple so a slow CI box is not the thing under test.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut saw_stranded = false;
    while tokio::time::Instant::now() < deadline {
        if metrics.stranded_for(1) > 0 && metrics.stranded_for(2) > 0 {
            saw_stranded = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Steady state for the covered shard: the gauge is last-write-wins, and a
    // sample taken in the startup window (before this worker's own fleet
    // registration commits) can legitimately see shard 0 as uncovered. Wait for
    // it to settle rather than asserting on whichever sample happened to land.
    let settle = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < settle && metrics.stranded_for(0) > 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let covered_stranded = metrics.stranded_for(0);

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert!(
        saw_stranded,
        "shards 1 and 2 have claimable pending work and no covering worker, so \
         harvest.shard.stranded_pending must go non-zero for both (issue #961 AC6); \
         observed shard1={} shard2={}",
        metrics.stranded_for(1),
        metrics.stranded_for(2),
    );
    assert_eq!(
        covered_stranded, 0,
        "the covered shard must settle at zero stranded pending work",
    );
}

/// **AC6 — the success metric's literal scenario.** *Killing the only worker
/// covering one shard* must surface the named undrained-shard signal.
///
/// The sibling test above covers a shard that was *never* polled. This one
/// covers the harder, more realistic case: shard 1 **is** covered, work is
/// draining, and then that worker dies. A surviving worker (covering shard 0)
/// keeps sampling, and shard 1 must flip from `0` to non-zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_the_only_covering_worker_surfaces_stranded_pending() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);

    // The observer: covers shard 0, samples every shard, and outlives the peer.
    let observer_metrics = Arc::new(ShardMetrics::default());
    let observer = build_worker_with(
        &sharded,
        vec![ShardId::new(0)],
        Arc::clone(&observer_metrics),
        "w-observer",
        |cfg| {
            // Long grace so the dying peer's fleet row is not reaped before the
            // sampler can notice shard 1 has lost its only poller.
            cfg.worker_heartbeat_interval = Duration::from_millis(100);
        },
    );

    // The doomed peer: the only worker covering shard 1.
    let peer = build_worker(
        &sharded,
        vec![ShardId::new(1)],
        Arc::new(ShardMetrics::default()),
        "w-doomed",
    );

    let pool0 = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let pool1 = sharded
        .exact_pool_for(ShardId::new(1))
        .expect("shard 1 pool")
        .clone();

    let observer_handle = {
        let runner = Arc::clone(&observer);
        let pool = pool0.clone();
        tokio::spawn(async move { runner.run(&pool).await })
    };
    let peer_handle = {
        let runner = Arc::clone(&peer);
        let pool = pool1.clone();
        tokio::spawn(async move { runner.run(&pool).await })
    };

    // Let shard 1 be genuinely covered first: its work must drain.
    let drained = ExecutionId::new_for_shard(ShardId::new(1));
    {
        let mut conn = pool1.get().await.expect("shard 1 conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(drained, "shard_probe", "covered-then-orphaned"),
            None,
        )
        .await
        .expect("seed covered work");
    }
    assert!(
        wait_for_state(
            &urls[&ShardId::new(1)],
            drained,
            "COMPLETED",
            Duration::from_secs(30)
        )
        .await,
        "shard 1 must be genuinely covered before its worker is killed",
    );

    // Kill the only worker covering shard 1, then strand work on it.
    peer.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), peer_handle).await;
    {
        let mut conn = pool1.get().await.expect("shard 1 conn");
        start_or_load_workflow_execution(
            &mut conn,
            start_params(
                ExecutionId::new_for_shard(ShardId::new(1)),
                "shard_probe",
                "orphaned-after-kill",
            ),
            None,
        )
        .await
        .expect("seed orphaned work");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut saw = false;
    while tokio::time::Instant::now() < deadline {
        if observer_metrics.stranded_for(1) > 0 {
            saw = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    observer.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), observer_handle).await;

    assert!(
        saw,
        "shard 1 lost its only covering worker and still holds claimable \
         pending work, so harvest.shard.stranded_pending{{shard=\"1\"}} must go \
         non-zero — this is the named undrained-shard signal the #961 success \
         metric requires (issue #961 AC6)",
    );
}

// ── AC7 ───────────────────────────────────────────────────────────────────

/// **AC7.** A single-shard deployment is byte-for-byte unchanged.
///
/// With no sharded pool at all — the legacy shape — auto resolution must yield
/// exactly `[ShardId::new(0)]`, the pre-#961 default, and the worker must take
/// the single-shard fast path rather than the multi-shard loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_shard_deployment_is_unchanged() {
    let (urls, _container) = setup_shard_databases(&[0]).await;
    let url = urls[&ShardId::new(0)].clone();
    let pool = build_pool(&url);
    let metrics = Arc::new(ShardMetrics::default());

    let built = HarvestBuilder::new()
        .workflows(vec![instant_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sharded_runtime_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics: Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        })
        .worker(WorkerConfig::default())
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = "w-single".to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    // Deliberately NO sharded_pool — the legacy single-shard shape.
    let worker = Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker builds"));

    assert_eq!(
        worker.config.shard_assignments,
        vec![ShardId::new(0)],
        "a pool-less worker must resolve to exactly the pre-#961 [0] default",
    );

    let run_pool = pool.clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&run_pool).await;
    });

    let exec_id = ExecutionId::new();
    let mut conn = pool.get().await.expect("conn");
    start_or_load_workflow_execution(
        &mut conn,
        start_params(exec_id, "shard_probe", "single-shard"),
        None,
    )
    .await
    .expect("start workflow");
    drop(conn);

    assert!(
        wait_for_state(&url, exec_id, "COMPLETED", Duration::from_secs(30)).await,
        "the single-shard fast path must still dispatch",
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let shard0 = metrics.dispatched_for(0);
    assert!(
        shard0 > 0,
        "the single-shard path must still emit harvest.shard.dispatched{{shard=\"0\"}} \
         — a zero here would make the equality below vacuously true",
    );
    assert_eq!(
        shard0,
        metrics.dispatched.lock().unwrap().values().sum::<u64>(),
        "a single-shard deployment must only ever report the shard-0 series",
    );
}

// ── AC3 + success metric ──────────────────────────────────────────────────

/// **AC3 + success metric.** Per-shard scheduler ticks preserve the issue #350
/// HA claim guarantee *per shard*: exactly one replica claims each due slot on
/// its home shard, with zero double-fires, across 100 contested slots per shard.
///
/// No migration is needed for this (AC3 asks it be flagged only *if* it is):
/// the #350 claim is a row-level `UPDATE ... WHERE fire_claim_token IS NULL`,
/// and each `harvest_schedules` row lives in exactly one shard database — so
/// the claim is per-shard by construction. This test proves that rather than
/// assuming it.
///
/// **Why this is falsifiable and the naive "no duplicate `workflow_id`" check
/// is not.** A scheduled fire is started under `RejectDuplicate` against the
/// partial unique index `harvest_we_workflow_name_workflow_id_active_key`, so
/// the *database* already refuses a second execution row per slot — a test
/// that only counts execution rows passes even with the entire `fire_claim_token`
/// mechanism deleted. So this test asserts on the **claim** instead, via the
/// `harvest.schedule.fire_attempts{outcome}` counter:
///
/// * `claimed == <distinct slots fired>` — the claim admitted exactly one
///   replica per slot. Delete the `fire_claim_token IS NULL OR ...` guard and
///   both replicas' claim `UPDATE`s match whenever the loser's claim lands
///   before the winner advances `next_run_at`, so `claimed` overshoots.
/// * `lost_race > 0` — proves the two replicas genuinely *contended* (i.e. the
///   test exercised the race rather than serialising by luck).
/// * Every shard fired at least 100 slots — proves the per-shard tick ran on
///   **each** shard, not that one shard carried a global total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn per_shard_scheduler_ha_fires_each_slot_exactly_once_under_two_replicas() {
    const ROUNDS: usize = 100;

    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let router = router_for(&SHARDS);

    // A schedule registered on every writable shard. `max_active_runs` is
    // raised because no worker runs in this test, so every fired execution
    // stays non-terminal and the default overlap `Skip` would otherwise stand
    // every round after the first down.
    let mut schedule =
        WorkflowSchedule::new("shard_probe", Schedule::Interval(Duration::from_secs(3600)));
    schedule.all_writable_shards = true;
    schedule.max_active_runs = u32::try_from(ROUNDS).expect("rounds fits u32") * 10;
    let schedules = Arc::new(vec![schedule]);

    let metrics = Arc::new(ShardMetrics::default());
    let built = HarvestBuilder::new()
        .workflows(vec![instant_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sharded_runtime_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics: Arc::clone(&metrics) as Arc<dyn MetricsRecorder>,
        })
        .worker(WorkerConfig::default())
        .build();
    let (registry, dags, _ws, _wc) = built.into_worker_parts();
    let registry = Arc::new(registry);
    let dags = Arc::new(autumn_harvest::scheduler::compile_dag_catalog(dags).expect("dag catalog"));

    let tick = |pool: ShardedDbPool, router: ShardRouter| {
        let registry = Arc::clone(&registry);
        let dags = Arc::clone(&dags);
        let schedules = Arc::clone(&schedules);
        async move {
            let _ = tick_once_sharded(
                pool,
                router,
                registry,
                dags,
                schedules,
                SchedulerMonitor::offline(),
            )
            .await;
        }
    };

    // Warm-up tick: registers the schedule row on every writable shard.
    tick(sharded.clone(), router.clone()).await;

    // One connection per shard, held across every round.
    let mut conns = Vec::new();
    for shard in SHARDS {
        let url = &urls[&ShardId::new(shard)];
        conns.push((
            shard,
            <AsyncPgConnection as AsyncConnection>::establish(url)
                .await
                .expect("connect for slot backdating"),
        ));
    }

    // Each round hands every shard a *fresh, distinct* past slot and clears any
    // held claim, then races two replicas at it. Distinct slots mean distinct
    // `sched:{id}:{name}:{slot}` workflow ids, so a duplicated fire is visible.
    let anchor =
        chrono::Utc::now() - chrono::Duration::hours(i64::try_from(ROUNDS).expect("fits") + 10);
    for round in 0..ROUNDS {
        let slot = anchor + chrono::Duration::hours(i64::try_from(round).expect("fits"));
        for (_, conn) in &mut conns {
            diesel::sql_query(
                "UPDATE harvest_schedules \
                 SET next_run_at = $1, fire_claim_token = NULL, fire_claimed_until = NULL \
                 WHERE workflow_name = 'shard_probe'",
            )
            .bind::<diesel::sql_types::Timestamptz, _>(slot)
            .execute(conn)
            .await
            .expect("backdate slot");
        }

        let a = tokio::spawn(tick(sharded.clone(), router.clone()));
        let b = tokio::spawn(tick(sharded.clone(), router.clone()));
        let _ = tokio::time::timeout(Duration::from_secs(30), a).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), b).await;
    }

    // ── Per shard: no slot fired twice, and every shard fired ─────────────
    let mut distinct_slots = 0usize;
    for (shard, conn) in &mut conns {
        let rows: Vec<SlotRow> = diesel::sql_query(
            "SELECT workflow_id, COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE workflow_name = 'shard_probe' GROUP BY workflow_id",
        )
        .load(conn)
        .await
        .expect("slot query");

        assert!(
            rows.len() >= ROUNDS,
            "shard {shard} fired only {} slots of {ROUNDS} — the per-shard \
             scheduler tick did not run on this shard (issue #961 AC3)",
            rows.len(),
        );
        for row in &rows {
            assert_eq!(
                row.n, 1,
                "shard {shard} slot {} fired {} times — the #350 per-shard HA \
                 claim must admit exactly one replica per slot (issue #961 AC3)",
                row.workflow_id, row.n,
            );
        }
        distinct_slots += rows.len();
    }

    // ── The claim itself, not just the unique index, enforced exclusivity ──
    let claimed = metrics.fire_attempts_for("claimed");
    let lost = metrics.fire_attempts_for("lost_race");
    assert!(
        lost > 0,
        "the two replicas never contended for a slot ({claimed} claimed, \
         0 lost) — this run did not exercise the #350 claim race at all",
    );
    assert_eq!(
        claimed,
        u64::try_from(distinct_slots).expect("slot count fits u64"),
        "the #350 claim admitted {claimed} replicas across {distinct_slots} \
         distinct slots — exactly one claim per slot is the per-shard HA \
         guarantee (issue #961 AC3); an overshoot means a slot was claimed \
         twice and only the unique index stopped the second fire",
    );
}

// ── AC2 — background control loops run on every assigned shard ────────────

/// Build a worker like [`build_worker`], but let the caller tweak the runtime
/// config first (poll interval, grace window, queues, …).
fn build_worker_with(
    sharded_pool: &ShardedDbPool,
    assignments: Vec<ShardId>,
    metrics: Arc<ShardMetrics>,
    worker_id: &str,
    tweak: impl FnOnce(&mut WorkerRuntimeConfig),
) -> Arc<Worker> {
    let built = HarvestBuilder::new()
        .workflows(vec![instant_info()])
        .telemetry(TelemetryConfig {
            service_name: Arc::from("sharded_runtime_tests"),
            propagator: Arc::new(autumn_harvest::telemetry::NoOpPropagator),
            metrics,
        })
        .worker(WorkerConfig::default().with_shard_assignments(assignments))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime_config: WorkerRuntimeConfig = worker_config.into();
    runtime_config.worker_id = worker_id.to_string();
    runtime_config.poll_interval = Duration::from_millis(50);
    runtime_config.shutdown_timeout = Duration::from_secs(2);
    runtime_config.sharded_pool = Some(sharded_pool.clone());
    tweak(&mut runtime_config);
    Arc::new(Worker::new(runtime_config, Arc::new(registry)).expect("worker should build"))
}

/// Insert a bare RUNNING execution row directly, pinned to `shard`.
///
/// Deliberately raw SQL rather than `start_or_load_workflow_execution`: these
/// AC2 fixtures want a row the *scanners* act on, with no task the poll loop
/// could race them for.
async fn insert_running_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    shard: ShardId,
    workflow_id: &str,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, state, input, started_at, shard_id, queue_name) \
         VALUES ($1, 'shard_probe', $2, 'RUNNING', '{}'::jsonb, NOW(), $3, 'default')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .bind::<diesel::sql_types::Integer, _>(shard.as_i32())
    .execute(conn)
    .await
    .expect("insert execution row");
}

async fn task_state(conn: &mut AsyncPgConnection, id: uuid::Uuid) -> String {
    #[derive(QueryableByName)]
    struct S {
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }
    let row: S = diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(id)
        .get_result(conn)
        .await
        .expect("task state query");
    row.state
}

/// **AC2 (timeout scanner).** The timeout/SLA enforcement scanner must run
/// against **every** assigned shard, not just the default one.
///
/// Each shard gets one activity task that is already past its
/// `schedule_to_start` deadline, enqueued on a queue the worker does **not**
/// poll — so the *only* thing that can move it is the per-shard timeout
/// scanner. If the scanners were spawned for shard 0 alone (the pre-#961
/// default-assignment bug), shards 1 and 2 stay `PENDING` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_scanner_enforces_on_every_assigned_shard() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());

    // Seed one already-expired task per shard on an unpolled queue.
    let mut tasks = Vec::new();
    for shard in SHARDS {
        let shard_id = ShardId::new(shard);
        let exec_id = ExecutionId::new_for_shard(shard_id);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        insert_running_execution(&mut conn, exec_id, shard_id, &format!("sts-{shard}")).await;

        let mut params = autumn_harvest::queue::EnqueueParams::new(
            "scanner-only",
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        );
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("never_polled".to_string());
        params.activity_id = Some(uuid::Uuid::new_v4());
        params.schedule_to_start = Some(chrono::Duration::seconds(1));
        params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(600);
        let task_id = autumn_harvest::queue::enqueue(&mut conn, &params)
            .await
            .expect("enqueue expired task");
        tasks.push((shard, task_id));
    }

    let worker = build_worker_with(
        &sharded,
        Vec::new(),
        Arc::clone(&metrics),
        "w-timeout",
        |_| {},
    );
    assert_eq!(
        worker.config.shard_assignments.len(),
        SHARDS.len(),
        "auto assignments must cover every shard before asserting per-shard scanners",
    );

    let default_pool = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    for (shard, task_id) in &tasks {
        let shard_id = ShardId::new(*shard);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut state;
        loop {
            let mut conn = pool.get().await.expect("shard conn");
            state = task_state(&mut conn, *task_id).await;
            if state != "PENDING" || tokio::time::Instant::now() >= deadline {
                break;
            }
            drop(conn);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            state, "FAILED",
            "the schedule-to-start timeout scanner never ran on shard {shard} — \
             every assigned shard must get its own control loop, and enforcement \
             must fail the expired task (not merely move it out of PENDING) \
             (issue #961 AC2)",
        );
    }

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

/// **AC2 (≥1 outbox).** The external-signal outbox must sweep **every**
/// assigned shard.
///
/// Each shard gets a RUNNING execution carrying a backdated
/// `ExternalSignalRequested` aimed at a target that does not exist on that
/// shard. Past the (deliberately tiny) unknown-target grace window the outbox
/// must resolve it to a terminal `ExternalSignalFailed { target_unknown }` —
/// on every shard, not just shard 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_signal_outbox_sweeps_every_assigned_shard() {
    let (urls, _container) = setup_shard_databases(&SHARDS).await;
    let sharded = build_sharded_pool(&urls);
    let metrics = Arc::new(ShardMetrics::default());

    for shard in SHARDS {
        let shard_id = ShardId::new(shard);
        let caller = ExecutionId::new_for_shard(shard_id);
        // A target on the SAME shard that was never started -> NotFound ->
        // (past the grace window) `target_unknown`.
        let target = ExecutionId::new_for_shard(shard_id);

        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let mut conn = pool.get().await.expect("shard conn");
        insert_running_execution(&mut conn, caller, shard_id, &format!("outbox-{shard}")).await;

        let event = autumn_harvest::event::WorkflowEvent::ExternalSignalRequested {
            signal_id: autumn_harvest::types::ExternalSignalId::new(),
            target: autumn_harvest::types::ExternalTarget::ExecutionId(target),
            signal_name: "ping".to_string(),
            payload: serde_json::json!({}),
            idempotency_key: None,
        };
        let event_data = serde_json::to_value(&event).expect("encode event");
        // Backdated well past any plausible grace window.
        diesel::sql_query(
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             VALUES ($1, 1, 'ExternalSignalRequested', $2, NOW() - INTERVAL '1 hour')",
        )
        .bind::<diesel::sql_types::Uuid, _>(caller.as_uuid())
        .bind::<diesel::sql_types::Jsonb, _>(event_data)
        .execute(&mut conn)
        .await
        .expect("insert ExternalSignalRequested");
    }

    let worker = build_worker_with(
        &sharded,
        Vec::new(),
        Arc::clone(&metrics),
        "w-outbox",
        |cfg| cfg.unknown_target_grace_window = Duration::from_millis(1),
    );

    let default_pool = sharded
        .exact_pool_for(ShardId::new(0))
        .expect("shard 0 pool")
        .clone();
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&default_pool).await;
    });

    for shard in SHARDS {
        let shard_id = ShardId::new(shard);
        let pool = sharded.exact_pool_for(shard_id).expect("shard pool");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut resolved;
        loop {
            let mut conn = pool.get().await.expect("shard conn");
            let row: TerminalCount = diesel::sql_query(
                "SELECT COUNT(*) AS n FROM harvest_events \
                 WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')",
            )
            .get_result(&mut conn)
            .await
            .expect("terminal count query");
            resolved = row.n;
            if resolved > 0 || tokio::time::Instant::now() >= deadline {
                break;
            }
            drop(conn);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            resolved > 0,
            "the external-signal outbox never swept shard {shard} — every assigned \
             shard must get its own outbox sweep (issue #961 AC2)",
        );
    }

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}

// ── AC4 — DAG -> shard pinning is stable ─────────────────────────────────

/// The shard set used by the AC4 widening test: three shards, then a fourth.
const WIDENED_SHARDS: [i32; 4] = [0, 1, 2, 3];

/// Find a DAG name whose rendezvous shard is `home` under `router`.
fn dag_name_pinned_to(router: &ShardRouter, home: ShardId) -> String {
    (0..10_000)
        .map(|i| format!("pinned-dag-{i}"))
        .find(|name| router.pick_for_dag(name) == home)
        .expect("a DAG name pinning to the requested shard")
}

/// Count `harvest_schedules` rows for `workflow_name` on one shard database.
async fn schedule_rows(url: &str, workflow_name: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect for schedule-row count");
    let rows: Vec<TerminalCount> =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_schedules WHERE workflow_name = $1")
            .bind::<diesel::sql_types::Text, _>(workflow_name)
            .load(&mut conn)
            .await
            .expect("schedule-row count");
    // NOTE: `.first()` on a `Vec` resolves to diesel's `FirstDsl` because the
    // query-DSL prelude is in scope; match on the slice instead.
    match rows.as_slice() {
        [row, ..] => row.n,
        [] => 0,
    }
}

/// Run one sharded scheduler tick against a freshly built runtime.
///
/// A fresh runtime each phase — `restart` is a genuinely new registry,
/// catalog, pool handle and router, not a second call on the same objects.
async fn tick(pool: ShardedDbPool, router: ShardRouter, schedules: Arc<Vec<WorkflowSchedule>>) {
    let built = HarvestBuilder::new()
        .workflows(vec![instant_info()])
        .worker(WorkerConfig::default())
        .build();
    let (registry, dags, _ws, _wc) = built.into_worker_parts();
    let dags = Arc::new(autumn_harvest::scheduler::compile_dag_catalog(dags).expect("dag catalog"));
    let _ = tick_once_sharded(
        pool,
        router,
        Arc::new(registry),
        dags,
        schedules,
        SchedulerMonitor::offline(),
    )
    .await;
}

/// **AC4.** A DAG's home shard survives a process restart and a shard-set
/// widening — asserted against the **production** registration path
/// (`tick_once_sharded` -> `register_workflow_schedules_for_shard` ->
/// `schedule_targets_shard`), not against the pure router.
///
/// Three phases, each against real per-shard databases:
///
/// 1. **Pinning.** A DAG schedule registers on its rendezvous shard and on
///    **no other** — the `schedule_targets_shard` gate.
/// 2. **Restart.** A freshly-constructed router, pool, registry and catalog
///    (the stand-in for a new process) re-ticks and the row stays on the same
///    shard, still exactly one row, with no copy resurrected elsewhere.
/// 3. **Widening.** A fourth shard joins. A name the new shard wins moves onto
///    it, and #1157's `collect_stale_dag_workflow_schedule` removes the row
///    from its former home — so the DAG is never registered on two shards at
///    once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dag_home_shard_is_stable_across_restart_and_widening() {
    let (urls, _container) = setup_shard_databases(&WIDENED_SHARDS).await;

    let three = router_for(&SHARDS);
    let four = router_for(&WIDENED_SHARDS);

    // A DAG that the fourth shard wins on widening — so phase 3 exercises a
    // genuine move rather than a no-op.
    let moving = (0..10_000)
        .map(|i| format!("pinned-dag-{i}"))
        .find(|name| {
            three.pick_for_dag(name) != four.pick_for_dag(name)
                && four.pick_for_dag(name) == ShardId::new(3)
        })
        .expect("a DAG name the widened shard wins");
    let home = three.pick_for_dag(&moving);

    // Pools: three shards first, then the widened four.
    let urls3: BTreeMap<ShardId, String> = urls
        .iter()
        .filter(|(shard, _)| shard.as_i32() != 3)
        .map(|(shard, url)| (*shard, url.clone()))
        .collect();
    let pool3 = build_sharded_pool(&urls3);
    let pool4 = build_sharded_pool(&urls);

    let mut schedule = WorkflowSchedule::new(
        moving.clone(),
        Schedule::Interval(Duration::from_secs(3600)),
    );
    schedule.dag_name = Some(moving.clone());
    let schedules = Arc::new(vec![schedule]);

    // ── Phase 1: pinned to exactly one shard ──────────────────────────────
    tick(pool3.clone(), three.clone(), Arc::clone(&schedules)).await;
    for shard in SHARDS {
        let expected = i64::from(ShardId::new(shard) == home);
        assert_eq!(
            schedule_rows(&urls[&ShardId::new(shard)], &moving).await,
            expected,
            "DAG '{moving}' pins to shard {home:?}; shard {shard} must hold \
             {expected} schedule row(s) (issue #961 AC4)",
        );
    }

    // ── Phase 2: restart keeps the same home, idempotently ────────────────
    tick(
        build_sharded_pool(&urls3),
        router_for(&SHARDS),
        Arc::clone(&schedules),
    )
    .await;
    for shard in SHARDS {
        let expected = i64::from(ShardId::new(shard) == home);
        assert_eq!(
            schedule_rows(&urls[&ShardId::new(shard)], &moving).await,
            expected,
            "after a restart DAG '{moving}' must still be registered on shard \
             {home:?} only — shard {shard} drifted (issue #961 AC4)",
        );
    }

    // ── Phase 3: widening moves it, and the stale row is collected ────────
    tick(pool4, four.clone(), Arc::clone(&schedules)).await;
    assert_eq!(
        schedule_rows(&urls[&ShardId::new(3)], &moving).await,
        1,
        "widening must move DAG '{moving}' onto the new shard 3 (issue #961 AC4)",
    );
    assert_eq!(
        schedule_rows(&urls[&home], &moving).await,
        0,
        "the stale schedule row on the DAG's former home {home:?} must be \
         collected on widening, or the DAG fires from two shards (#1157)",
    );

    // A non-moving DAG must be undisturbed by the widening.
    let stable = dag_name_pinned_to(&three, ShardId::new(2));
    if four.pick_for_dag(&stable) == ShardId::new(2) {
        assert_eq!(
            three.pick_for_dag(&stable),
            four.pick_for_dag(&stable),
            "a DAG the new shard does not win must not move (issue #961 AC4)",
        );
    }
}

/// **AC4 (property).** The rendezvous widening invariant the DB test above
/// relies on: adding a shard may only move a DAG **onto the new shard**, never
/// between two pre-existing shards. That is what makes #1157's stale-row
/// collection sufficient — only names the new shard wins ever need collecting.
#[test]
fn dag_widening_only_moves_names_onto_the_new_shard() {
    let names: Vec<String> = (0..200).map(|i| format!("dag-{i}")).collect();
    let before = router_for(&SHARDS);
    let widened = router_for(&WIDENED_SHARDS);
    let new_shard = ShardId::new(3);
    let mut moved = 0usize;
    for name in &names {
        let old = before.pick_for_dag(name);
        let new = widened.pick_for_dag(name);
        if old != new {
            moved += 1;
            assert_eq!(
                new, new_shard,
                "widening moved DAG '{name}' from shard {old:?} to {new:?} — a \
                 rendezvous widening may only move names ONTO the new shard \
                 (issue #961 AC4)",
            );
        }
    }
    assert!(
        moved > 0,
        "widening moved no DAG at all — the fixture is not exercising the \
         rebalance path, so the stability assertion above is vacuous",
    );
}
