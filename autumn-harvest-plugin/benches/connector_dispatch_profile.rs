//! Deterministic (non-criterion) instruction-count profiling harness for the
//! broker connector's dispatch pass (issue #944): `ConnectorRuntime::run_once`
//! driven `receive -> map -> idempotency key -> dispatch -> decide ->
//! settle` for a realistic batch of inbound messages, against a real
//! Postgres.
//!
//! # Why this workload
//!
//! `autumn-harvest-plugin/src/connector/` (13k+ lines: `dispatch.rs`,
//! `disposition.rs`, `idempotency.rs`, `binding.rs`, `runtime.rs`, ...) had no
//! bench, profile, or `docs/performance-*.md` writeup as of this survey,
//! unlike almost every other subsystem in this repo. This harness exercises
//! it through the real public entry point — `ConnectorRuntime::run_once`,
//! the same one `run` calls in a loop — using `MockSource` (a supported,
//! exported adapter, not test-only scaffolding) so the broker side is
//! deterministic and network-free, exactly as
//! `tests/connector_integration.rs`'s own
//! `soak_ten_thousand_messages_with_redeliveries_and_poison` does. Dispatch
//! itself is **not** mocked: every mapped message is driven all the way
//! through `dispatch()` into the real `start_workflow` /
//! `signal_with_start_workflow` handlers and a real Postgres, because that is
//! the only way to exercise the actual ack-after-commit and idempotency-dedupe
//! contract this subsystem exists to provide — mocking dispatch out would
//! profile a different, unrealistic program.
//!
//! # Workload
//!
//! `CONNECTOR_DISPATCH_PROFILE_TOTAL` (default 1,000) Kafka-shaped messages on
//! one partition, with the same *distribution* (not just total) as the soak
//! test's headline metric: 1 message in 1,000 is malformed JSON (poison, so
//! ~0.1% here rounds up to at least one), and 1 message in 20 (5%) is
//! delivered twice at identical coordinates (a forced redelivery, standing in
//! for a consumer-group rebalance or an un-acked crash). Every non-poison
//! payload is a realistic order-shaped JSON body
//! (`{"order_id": "...", "customer_id": "...", "items": [...], "total_cents":
//! ...}`), matching `connector_integration.rs`'s `order_mapper` fixture
//! shape. The binding starts a `fulfil_order` workflow, matching that same
//! test file's target.
//!
//! `run_once` is called in a loop (mirroring `ConnectorRuntime::run`'s own
//! driving loop) until a pass receives nothing, which is exactly how the soak
//! test drains its batch.
//!
//! # Running
//!
//! ```text
//! # Needs a reachable Postgres. Point at a local server (a fresh,
//! # uniquely-named database is created, migrated, and dropped for this run;
//! # never a testcontainer here -- this harness has no Docker dependency so
//! # it stays runnable under callgrind on a Docker-less sandbox):
//! export HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
//!
//! BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
//!   --features connectors --bench connector_dispatch_profile --no-run \
//!   --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="connector_dispatch_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! With no reachable database this prints a skip notice and exits 0, matching
//! `diagnose_bench.rs`'s convention -- never a CI failure.
//!
//! # Wall-clock timing is not admissible evidence
//!
//! This machine is a shared vCPU, so every number this harness's callgrind/
//! dhat runs produce is a deterministic instruction or allocation count, never
//! a wall-clock duration. See `docs/performance-connector-dispatch.md`.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowId;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, RetentionConfig};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, ConnectorRuntimeConfig, MappedMessage, MappingError, MessageCtx, MockSource,
    PostgresDeadLetterSink, SourceBinding, resolve_idempotency_mode,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};

// The connector dead-letter table is plugin-owned, not part of the core
// engine's `test_init_sql()`, exactly as `connector_integration.rs` documents.
const CONNECTOR_DLQ_SQL: &str =
    include_str!("../migrations/harvest/20260719900000_harvest_connector_dead_letters/up.sql");

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Total messages the soak-shaped batch pushes before redeliveries, scaled
/// down from the soak test's 10,000 so a callgrind run finishes in bounded
/// time while keeping its distribution (poison, redelivery) proportional.
const DEFAULT_TOTAL: usize = 1_000;

struct SkipReason(String);

struct BenchDb {
    url: String,
}

fn run_token() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    std::process::id().hash(&mut hasher);
    std::time::Instant::now().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn with_db_name(admin_url: &str, db: &str) -> Result<String, String> {
    let mut url =
        reqwest::Url::parse(admin_url).map_err(|e| format!("parse admin url: {e}"))?;
    url.set_path(&format!("/{db}"));
    Ok(url.to_string())
}

/// Point `HARVEST_TEST_DATABASE_URL` at a local Postgres (an *admin*
/// connection string); a fresh, uniquely-named database is created and
/// migrated for this run. Deliberately no testcontainers fallback: unlike
/// `diagnose_bench.rs`, this harness's whole purpose is to run under
/// `valgrind --tool=callgrind`, and a Docker-less sandbox is exactly the
/// environment that needs a callgrind-friendly connector profile in the
/// first place (see this repo's Bolt survey of the `connector/` subsystem).
async fn setup_bench_db() -> Result<BenchDb, SkipReason> {
    let admin_url = std::env::var("HARVEST_TEST_DATABASE_URL").map_err(|_| {
        SkipReason("HARVEST_TEST_DATABASE_URL is not set".to_string())
    })?;
    let db = format!(
        "harvest_connector_dispatch_bench_{}_{}",
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
    conn.batch_execute(CONNECTOR_DLQ_SQL)
        .await
        .map_err(|e| SkipReason(format!("migrate connector dlq {db}: {e}")))?;
    Ok(BenchDb { url })
}

/// Best-effort cleanup, matching `diagnose_bench.rs`: an orphaned throwaway
/// database is an operator inconvenience, not a correctness problem, and
/// this profiling harness is not expected to run unattended many times a day.
async fn drop_bench_db(admin_url: &str, db_url: &str) {
    let Ok(parsed) = reqwest::Url::parse(db_url) else {
        return;
    };
    let db = parsed.path().trim_start_matches('/');
    if db.is_empty() {
        return;
    }
    if let Ok(mut admin) = AsyncPgConnection::establish(admin_url).await {
        let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)"))
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

fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({"status": "ok"})) })
}

fn workflow_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "connector_dispatch_profile",
        handler: noop_workflow,
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

fn api_state(pool: &DbPool) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.set_admin_auth_boundary(true);
    state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(
            vec![workflow_info("fulfil_order")],
            vec![],
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("connector-dispatch-bench".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::default(),
    ));
    state
}

/// `{"order_id": "...", "customer_id": "...", "items": [...], "total_cents":
/// ...}` -> that order id as the workflow id. The realistic shape
/// `connector_integration.rs`'s `order_mapper` stands in for, widened with a
/// few more fields so the JSON decode this profile measures is not a
/// one-field toy.
fn order_mapper(ctx: &MessageCtx) -> Result<MappedMessage, MappingError> {
    let payload: Value = serde_json::from_slice(&ctx.raw_body)
        .map_err(|e| MappingError::Deserialize(e.to_string()))?;
    let order_id = payload
        .get("order_id")
        .and_then(Value::as_str)
        .ok_or_else(|| MappingError::Rejected("missing order_id".to_string()))?;
    Ok(MappedMessage {
        workflow_id: WorkflowId::new(order_id),
        payload,
    })
}

fn order_body(i: usize) -> Vec<u8> {
    let payload = json!({
        "order_id": format!("o-{i}"),
        "customer_id": format!("c-{}", i % 733),
        "items": [
            { "sku": "SKU-1234", "qty": 2, "unit_cents": 1999 },
            { "sku": "SKU-5678", "qty": 1, "unit_cents": 4599 },
        ],
        "total_cents": 8597,
        "currency": "USD",
        "placed_at": "2026-09-25T12:00:00Z",
    });
    serde_json::to_vec(&payload).expect("payload serializes")
}

/// Push a soak-shaped batch onto `source`: `total` messages, 1-in-1000
/// malformed (poison), 1-in-20 forced-redelivered, matching
/// `soak_ten_thousand_messages_with_redeliveries_and_poison`'s distribution
/// scaled to `total`.
fn seed_batch(source: &MockSource, total: usize) -> usize {
    let poison_every = (total / 1000).max(1);
    let mut delivered = 0usize;
    for i in 0..total {
        let poison = i % poison_every == 0;
        let body = if poison {
            b"{ not json".to_vec()
        } else {
            order_body(i)
        };
        #[allow(clippy::cast_possible_wrap)]
        let offset = i as i64;
        source.push_kafka(0, offset, &body);
        delivered += 1;
        if i % 20 == 0 {
            source.push_kafka(0, offset, &body);
            delivered += 1;
        }
    }
    delivered
}

async fn reset(conn: &mut AsyncPgConnection) {
    conn.batch_execute(
        "TRUNCATE harvest_events, harvest_signals, harvest_task_queue, \
         harvest_workflow_executions, harvest_connector_dead_letters, \
         harvest_start_idempotency, harvest_start_throttle, harvest_rate_limit_buckets \
         RESTART IDENTITY CASCADE",
    )
    .await
    .expect("truncate");
}

#[tokio::main]
async fn main() {
    let db = match setup_bench_db().await {
        Ok(db) => db,
        Err(SkipReason(reason)) => {
            println!("connector_dispatch_profile: skipping (no reachable database) -- {reason}");
            return;
        }
    };
    let pool = build_pool(&db.url);
    let state = api_state(&pool);

    let total = env_usize("CONNECTOR_DISPATCH_PROFILE_TOTAL", DEFAULT_TOTAL);
    let reps = env_usize("CONNECTOR_DISPATCH_PROFILE_REPS", 1);

    let mut grand_total = autumn_harvest_plugin::connector::PassSummary::default();
    for _ in 0..reps {
        let mut conn = AsyncPgConnection::establish(&db.url)
            .await
            .expect("connect for reset");
        reset(&mut conn).await;
        drop(conn);

        let source = Arc::new(MockSource::new("orders"));
        let delivered = seed_batch(&source, total);

        let binding = SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_raw(order_mapper)
            .max_in_flight(32);
        // Use the production resolver, exactly as `connector_integration.rs`
        // does, so this profile exercises the same mode-selection logic the
        // plugin's own wiring runs.
        let mode = resolve_idempotency_mode(binding.target, binding.idempotency_mode, None);
        let rt = ConnectorRuntime::new(
            Arc::new(binding),
            Arc::clone(&source) as Arc<dyn autumn_harvest_plugin::connector::EventSource>,
            state.clone(),
            Arc::new(NoOpMetrics),
            mode,
        )
        .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())))
        .with_config(ConnectorRuntimeConfig {
            max_batch: 512,
            ..Default::default()
        });

        let mut total_summary = autumn_harvest_plugin::connector::PassSummary::default();
        loop {
            let pass = rt.run_once().await.expect("pass");
            if pass.received == 0 {
                break;
            }
            total_summary.received += pass.received;
            total_summary.acked += pass.acked;
            total_summary.retried += pass.retried;
            total_summary.dead_lettered += pass.dead_lettered;
        }
        assert_eq!(total_summary.received, delivered, "every pushed message was received");
        grand_total.received += total_summary.received;
        grand_total.acked += total_summary.acked;
        grand_total.retried += total_summary.retried;
        grand_total.dead_lettered += total_summary.dead_lettered;
    }

    println!(
        "connector_dispatch_profile: total={total} reps={reps} received={} acked={} retried={} dead_lettered={}",
        grand_total.received, grand_total.acked, grand_total.retried, grand_total.dead_lettered
    );

    drop(pool);
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        drop_bench_db(&admin_url, &db.url).await;
    }
}
