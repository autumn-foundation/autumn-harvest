//! End-to-end latency of `GET /workflows/{id}/diagnose` against a real
//! Postgres (issue #1194).
//!
//! Issue #809 published a structural argument for `p95 < 500 ms` but never
//! measured it, and the argument has a hole: the handler's cost is **not**
//! constant in the shape of the execution being diagnosed. Three drivers are
//! unbounded (see `build_diagnosis_report` in `src/api.rs` for the code this
//! bench exercises):
//!
//! 1. **Fan-out width x fleet size.** `eligible_worker_ids` is called once
//!    per pending-activity row, each call doing an in-memory linear scan of
//!    the whole fetched worker fleet -- O(N x M) CPU work for N pending
//!    activities and M live workers, even though the two DB queries that
//!    feed it are each a single flat round trip.
//! 2. **The replay path.** When the DB-observable categories alone would
//!    report `sleeping_timer` or `no_pending_work`, the handler drives a full
//!    history replay (`build_awaitables_report`) bounded by
//!    `WorkerConfig::query_timeout` (default 5 s -- 10x the 500 ms budget).
//! 3. **Pending-row count**, which is deliberately unbounded (no `LIMIT`):
//!    a worst-of fold must see every row to find the one wedged slot.
//!
//! This bench sweeps N (fan-out), M (fleet), and history length on the
//! replay path, plus a single-pending-activity baseline as the control, and
//! prints markdown-ready tables meant to be pasted into
//! `docs/performance-diagnose-latency.md`.
//!
//! # Why this crate, not `autumn-harvest/benches/`
//!
//! `build_diagnosis_report`, `eligible_worker_ids` and
//! `build_awaitables_report` are all private to `autumn-harvest-plugin`, and
//! `autumn-harvest` has no dev-dependency on this crate. A bench under
//! `autumn-harvest/benches/` could therefore only ever measure a
//! reimplementation of the query shapes, not the literal handler -- exactly
//! the kind of drift risk the `harvest_events` append-only invariant
//! elsewhere in this repo is so careful to avoid for a different reason.
//! Issue #1194 explicitly allows "a plugin-side equivalent", so this bench
//! drives the real, exported `harvest_api_router` over
//! `tower::ServiceExt::oneshot` -- the same technique
//! `tests/query_integration.rs` and its siblings already use -- rather than
//! calling any internal function directly.
//!
//! # Why not criterion
//!
//! `claim_bench.rs` (issue #786) avoids criterion because `claim_task` is
//! destructive: criterion's repeat-the-closure statistical loop would drain
//! the seeded backlog and end up timing an empty queue. `GET /diagnose` is
//! read-only by construction (see `build_diagnosis_report`'s own doc
//! comment), so that constraint does not apply here -- criterion would be
//! numerically valid. This bench still uses a hand-rolled `harness = false`
//! loop, purely so its output matches the p50/p99/max table format
//! `docs/performance.md` already established for `claim_bench`, rather than
//! introducing a second report shape.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest-plugin --bench diagnose_bench
//! ```
//!
//! Against `HARVEST_TEST_DATABASE_URL` when set (an admin connection string;
//! a fresh throwaway database is created, migrated and dropped for this run),
//! otherwise a per-run `postgres:16` testcontainer. With neither Docker nor
//! the env var reachable, prints a skip notice and exits 0 -- never a CI
//! failure. `cargo bench -p autumn-harvest-plugin --bench diagnose_bench
//! --no-run` (no database needed) is the compile-only bit-rot guard CI runs
//! on every PR.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel::sql_types;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

// ── The one registered workflow ─────────────────────────────────────────────
//
// Every seeded execution uses this workflow. For the fan-out/fleet scenarios
// the handler is never actually replayed (the DB-observable categories decide
// the verdict before the replay drive would run), so one shared, simple
// function covers every scenario:
//
//   for i in 0..history_activities { execute_activity_raw("noop", i) }
//   wait_for_signal("never_arrives")  // never recorded -> parks
//
// Seeding a matching history that stops right after the last completed
// activity pair (no `SignalReceived`, no seal) reproduces exactly the
// "awaited-but-unsent signal" case `build_awaitables_report` exists to find,
// with `history_activities` as the knob on replay length.
fn diagnose_bench_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = input
            .get("history_activities")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        for i in 0..n {
            ctx.execute_activity_raw("noop", json!(i), "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        let _: Value = ctx
            .wait_for_signal("never_arrives")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn diagnose_bench_workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "diagnose_bench_wf",
        module: "diagnose_bench",
        handler: diagnose_bench_workflow,
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

// ── Database provisioning (soft-skip, mirrors claim_bench's contract) ──────

struct SkipReason(String);

struct BenchDb {
    url: String,
    // Held for the container's whole lifetime; dropping it tears the
    // container down. `None` on the `HARVEST_TEST_DATABASE_URL` path.
    _container: Option<ContainerAsync<Postgres>>,
}

fn run_token() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    std::process::id().hash(&mut hasher);
    Instant::now().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn with_db_name(admin_url: &str, db: &str) -> Result<String, String> {
    let mut url = url::Url::parse(admin_url).map_err(|e| format!("parse admin url: {e}"))?;
    url.set_path(&format!("/{db}"));
    Ok(url.to_string())
}

async fn setup_bench_db() -> Result<BenchDb, SkipReason> {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db = format!(
            "harvest_diagnose_bench_{}_{}",
            std::process::id(),
            run_token()
        );
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .map_err(|e| SkipReason(format!("connect admin db: {e}")))?;
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .map_err(|e| SkipReason(format!("create database {db}: {e}")))?;
        let url = with_db_name(&admin_url, &db).map_err(SkipReason)?;
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .map_err(|e| SkipReason(format!("connect {db}: {e}")))?;
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .map_err(|e| SkipReason(format!("migrate {db}: {e}")))?;
        return Ok(BenchDb {
            url,
            _container: None,
        });
    }

    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
        .with_tag("16")
        .start()
        .await
        .map_err(|e| SkipReason(format!("start postgres container (is Docker running?): {e}")))?;
    let host = container
        .get_host()
        .await
        .map_err(|e| SkipReason(format!("container host: {e}")))?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .map_err(|e| SkipReason(format!("container port: {e}")))?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    Ok(BenchDb {
        url,
        _container: Some(container),
    })
}

/// Best-effort cleanup for the `HARVEST_TEST_DATABASE_URL` path. Never fails
/// the run: an orphaned throwaway database is an operator inconvenience, not
/// a correctness problem, and this bench (unlike `claim_bench`'s CI-gated
/// budget test) is not expected to run unattended many times per day.
async fn drop_bench_db(admin_url: &str, db_url: &str) {
    let Ok(parsed) = url::Url::parse(db_url) else {
        return;
    };
    let db = parsed.path().trim_start_matches('/');
    if db.is_empty() {
        return;
    }
    if let Ok(mut admin) = AsyncPgConnection::establish(admin_url).await {
        let _ = diesel::sql_query(format!(
            "DROP DATABASE IF EXISTS {db} WITH (FORCE)"
        ))
        .execute(&mut admin)
        .await;
    }
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

// ── App under test ───────────────────────────────────────────────────────

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![diagnose_bench_workflow_info()],
            vec![],
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("diagnose-bench".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn diagnose(app: &HarvestApiApp, exec_id: ExecutionId) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/workflows/{exec_id}/diagnose"))
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("GET /diagnose");
    response.status()
}

// ── Seeding ──────────────────────────────────────────────────────────────

async fn reset(conn: &mut AsyncPgConnection) {
    diesel::sql_query(
        "TRUNCATE harvest_task_queue, harvest_workflow_executions, harvest_workers, \
         harvest_events RESTART IDENTITY CASCADE",
    )
    .execute(conn)
    .await
    .expect("truncate bench tables");
}

async fn seed_execution(conn: &mut AsyncPgConnection, history_activities: u64) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let input = json!({ "history_activities": history_activities });
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'diagnose_bench_wf', $2, 0, $3, 'default', 'RUNNING')",
    )
    .bind::<sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<sql_types::Text, _>(exec_id.to_string())
    .bind::<sql_types::Jsonb, _>(&input)
    .execute(conn)
    .await
    .expect("seed execution");

    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for i in 0..history_activities {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "noop".into(),
            input: json!(i),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    store::append_events(conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// Bulk-insert `n` claimable `PENDING` activity rows for `exec_id`, set-based
/// like `claim_bench_support.rs::seed_backlog` -- one round trip regardless
/// of `n`.
async fn seed_pending_activities(conn: &mut AsyncPgConnection, exec_id: ExecutionId, n: u32) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
         (queue_name, task_type, workflow_exec_id, activity_name, input, state) \
         SELECT 'default', 'activity', $1, 'noop', '{}'::jsonb, 'PENDING' \
         FROM generate_series(1, $2)",
    )
    .bind::<sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<sql_types::Int4, _>(i32::try_from(n).unwrap_or(i32::MAX))
    .execute(conn)
    .await
    .expect("seed pending activities");
}

/// Bulk-insert `m` live workers covering the `default` queue, set-based like
/// `claim_bench_support.rs::seed_workers`. `queues` deliberately includes
/// `"default"` (unlike claim_bench's workers, whose `claim_task` predicate
/// doesn't need it) because `eligible_worker_ids`' queue-coverage check does.
async fn seed_workers(conn: &mut AsyncPgConnection, m: u32) {
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, max_concurrency, host, build_id, queues, labels) \
         SELECT 'diagnose-bench-worker-' || i, 16, 'bench-host', 'bench-build', \
                '[\"default\"]'::jsonb, '{}'::jsonb \
         FROM generate_series(1, $1) AS s(i)",
    )
    .bind::<sql_types::Int4, _>(i32::try_from(m).unwrap_or(i32::MAX))
    .execute(conn)
    .await
    .expect("seed workers");
}

// ── Latency stats (nearest-rank percentiles, matching claim_bench) ─────────

#[derive(Clone, Copy)]
struct LatencyStats {
    n: usize,
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn percentile_ms(sorted_ms: &[f64], pct: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let rank = ((pct * sorted_ms.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted_ms.len() - 1);
    sorted_ms[rank]
}

fn stats_from(mut samples_ms: Vec<f64>) -> LatencyStats {
    samples_ms.sort_by(|a, b| a.partial_cmp(b).expect("latency is never NaN"));
    LatencyStats {
        n: samples_ms.len(),
        p50_ms: percentile_ms(&samples_ms, 0.50),
        p99_ms: percentile_ms(&samples_ms, 0.99),
        max_ms: samples_ms.last().copied().unwrap_or(0.0),
    }
}

const WARMUP: usize = 10;
const MEASURED: usize = 60;
const REPLAY_WARMUP: usize = 5;
const REPLAY_MEASURED: usize = 20;

async fn run_scenario(
    app: &HarvestApiApp,
    exec_id: ExecutionId,
    warmup: usize,
    measured: usize,
) -> LatencyStats {
    for _ in 0..warmup {
        let status = diagnose(app, exec_id).await;
        assert_eq!(status, StatusCode::OK, "warmup request must succeed");
    }
    let mut samples_ms = Vec::with_capacity(measured);
    for _ in 0..measured {
        let start = Instant::now();
        let status = diagnose(app, exec_id).await;
        let elapsed = start.elapsed();
        assert_eq!(status, StatusCode::OK, "measured request must succeed");
        samples_ms.push(elapsed.as_secs_f64() * 1000.0);
    }
    stats_from(samples_ms)
}

fn print_row(label: &str, stats: LatencyStats) {
    println!(
        "| {label} | {} | {:.2} | {:.2} | {:.2} |",
        stats.n, stats.p50_ms, stats.p99_ms, stats.max_ms
    );
}

// ── Scenario matrix ─────────────────────────────────────────────────────

const FANOUT_SWEEP: [u32; 4] = [1, 10, 100, 1_000];
const FLEET_SWEEP: [u32; 4] = [1, 10, 100, 1_000];
const REPLAY_SWEEP: [u64; 4] = [10, 100, 1_000, 5_000];
const HEADLINE_FLEET: u32 = 8;
const HEADLINE_FANOUT: u32 = 10;

async fn fanout_section(app: &HarvestApiApp, pool: &DbPool) {
    println!();
    println!(
        "## Diagnose latency vs pending-activity fan-out (fleet fixed at {HEADLINE_FLEET} workers)"
    );
    println!();
    println!("| pending activities (N) | n | p50 ms | p99 ms | max ms |");
    println!("|--:|--:|--:|--:|--:|");
    for &n in &FANOUT_SWEEP {
        let mut conn = pool.get().await.expect("pooled conn");
        reset(&mut conn).await;
        let exec_id = seed_execution(&mut conn, 0).await;
        seed_pending_activities(&mut conn, exec_id, n).await;
        seed_workers(&mut conn, HEADLINE_FLEET).await;
        diesel::sql_query("ANALYZE")
            .execute(&mut conn)
            .await
            .expect("analyze");
        drop(conn);
        let label = if n == 1 {
            "1 (baseline / control)".to_string()
        } else {
            n.to_string()
        };
        let stats = run_scenario(app, exec_id, WARMUP, MEASURED).await;
        print_row(&label, stats);
    }
}

async fn fleet_section(app: &HarvestApiApp, pool: &DbPool) {
    println!();
    println!(
        "## Diagnose latency vs live-worker fleet size (fan-out fixed at {HEADLINE_FANOUT} pending activities)"
    );
    println!();
    println!("| live workers (M) | n | p50 ms | p99 ms | max ms |");
    println!("|--:|--:|--:|--:|--:|");
    for &m in &FLEET_SWEEP {
        let mut conn = pool.get().await.expect("pooled conn");
        reset(&mut conn).await;
        let exec_id = seed_execution(&mut conn, 0).await;
        seed_pending_activities(&mut conn, exec_id, HEADLINE_FANOUT).await;
        seed_workers(&mut conn, m).await;
        diesel::sql_query("ANALYZE")
            .execute(&mut conn)
            .await
            .expect("analyze");
        drop(conn);
        let stats = run_scenario(app, exec_id, WARMUP, MEASURED).await;
        print_row(&m.to_string(), stats);
    }
}

async fn replay_section(app: &HarvestApiApp, pool: &DbPool) {
    println!();
    println!("## Diagnose latency on the replay path (no pending activities -- forces `build_awaitables_report`)");
    println!();
    println!("| history events replayed | n | p50 ms | p99 ms | max ms |");
    println!("|--:|--:|--:|--:|--:|");
    for &history_activities in &REPLAY_SWEEP {
        let mut conn = pool.get().await.expect("pooled conn");
        reset(&mut conn).await;
        let exec_id = seed_execution(&mut conn, history_activities).await;
        seed_workers(&mut conn, HEADLINE_FLEET).await;
        diesel::sql_query("ANALYZE")
            .execute(&mut conn)
            .await
            .expect("analyze");
        drop(conn);
        // `WorkflowStarted` + one `ActivityScheduled`/`ActivityCompleted`
        // pair per history_activities.
        let event_count = 1 + history_activities * 2;
        let stats = run_scenario(app, exec_id, REPLAY_WARMUP, REPLAY_MEASURED).await;
        print_row(&event_count.to_string(), stats);
    }
}

#[tokio::main]
async fn main() {
    let db = match setup_bench_db().await {
        Ok(db) => db,
        Err(SkipReason(reason)) => {
            println!(
                "diagnose_bench: skipping (no reachable database) -- {reason}"
            );
            return;
        }
    };
    let pool = build_pool(&db.url);
    let app = build_app(&pool);

    println!("# Diagnose latency (issue #1194)");
    println!();
    println!("Machine: {} logical CPUs", num_cpus());
    println!("Endpoint: `GET /workflows/{{id}}/diagnose`");

    fanout_section(&app, &pool).await;
    fleet_section(&app, &pool).await;
    replay_section(&app, &pool).await;

    drop(pool);
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        drop_bench_db(&admin_url, &db.url).await;
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}
