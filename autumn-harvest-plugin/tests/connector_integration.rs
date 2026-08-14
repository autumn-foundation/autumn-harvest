#![cfg(feature = "connectors")]
#![allow(clippy::too_many_lines)]
//! End-to-end connector tests against a real Postgres (issue #944).
//!
//! These are the *falsifiable* halves of the acceptance criteria — the ones
//! that only mean something once a start actually commits to the database:
//!
//! | AC | Test |
//! |----|------|
//! | AC3 idempotent redelivery | `redelivered_message_creates_exactly_one_execution` |
//! | AC3 signal dedupe | `redelivered_message_delivers_exactly_one_signal` |
//! | AC4 ack after commit | `crash_between_commit_and_ack_dedupes_on_redelivery` |
//! | AC4 harvest failure never acks | `a_rejected_dispatch_is_never_acked_as_success` |
//! | AC5 throttle deferral | `a_throttle_deferred_start_is_acked_not_retried` |
//! | AC6 poison isolation | `one_poison_message_does_not_block_a_hundred_valid_ones` |
//! | AC6 idempotent dead-lettering | `a_second_dead_letter_for_one_key_is_reported_as_already_recorded` |
//! | AC8 provenance | `a_broker_started_execution_records_broker_provenance` |
//! | metric | `soak_ten_thousand_messages_with_redeliveries_and_poison` |
//!
//! Deliberately driven through [`ConnectorRuntime`] rather than a hand-rolled
//! loop, so the ack ordering, poison accounting and backpressure under test
//! are the production code paths.
//!
//! Execution: honours `HARVEST_TEST_DATABASE_URL` (a migrated Postgres) so the
//! suite runs where Docker is unavailable; otherwise boots a testcontainers
//! Postgres from `autumn_harvest::full_migrations_sql()` plus this plugin's
//! own connector dead-letter migration.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowId;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::throttle::ThrottlePolicy;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::connector::{
    ConnectorDeadLetter, ConnectorRuntime, DeadLetterOutcome, IdempotencyMode, MappedMessage,
    MappingError, MockSource, PostgresDeadLetterSink, SourceBinding, insert_dead_letter,
};
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const CONNECTOR_DLQ_SQL: &str =
    include_str!("../migrations/harvest/20260719900000_harvest_connector_dead_letters/up.sql");

fn init_sql() -> Vec<u8> {
    let mut sql = autumn_harvest::full_migrations_sql().to_string();
    sql.push('\n');
    sql.push_str(CONNECTOR_DLQ_SQL);
    sql.into_bytes()
}

/// Honour `HARVEST_TEST_DATABASE_URL` for a local run; else boot testcontainers.
async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str, size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("pool should build")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("direct connection")
}

/// Ensure the connector dead-letter table exists and start each test from a
/// clean slate, so a shared `HARVEST_TEST_DATABASE_URL` database stays usable.
async fn reset(conn: &mut AsyncPgConnection) {
    let _ = conn.batch_execute(CONNECTOR_DLQ_SQL).await;
    conn.batch_execute(
        "TRUNCATE harvest_events, harvest_signals, harvest_task_queue, \
         harvest_workflow_executions, harvest_connector_dead_letters, \
         harvest_start_idempotency, harvest_start_throttle, harvest_rate_limit_buckets \
         RESTART IDENTITY CASCADE",
    )
    .await
    .expect("truncate");
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    v: String,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .load::<Count>(conn)
        .await
        .expect("count query")[0]
        .n
}

// ─────────────────────────── fixtures ───────────────────────────

fn noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(json!({"status": "ok"})) })
}

fn workflow_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
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

/// A workflow whose admission is deferred by a start throttle (issue #607),
/// used to prove AC5's "a 202 counts as a successful dispatch".
fn throttled_info(name: &'static str) -> WorkflowInfo {
    let mut info = workflow_info(name);
    info.throttle = Some(
        ThrottlePolicy::from_rate_str("1/h", Some(1.0), Some("input.tenant_id"), None)
            .expect("valid rate"),
    );
    info
}

fn api_state(pool: &DbPool, infos: Vec<WorkflowInfo>) -> HarvestApiState {
    let state = HarvestApiState::new();
    state.set_admin_auth_boundary(true);
    state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(infos, vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("connector-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    state
}

/// `{"order_id": "..."}` → that order id as the workflow id.
fn order_mapper(
    ctx: &autumn_harvest_plugin::connector::MessageCtx,
) -> Result<MappedMessage, MappingError> {
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

fn runtime_for(
    binding: SourceBinding,
    source: &Arc<MockSource>,
    state: &HarvestApiState,
    pool: &DbPool,
) -> ConnectorRuntime {
    // Use the production resolver, so these tests exercise the same
    // mode-selection logic the plugin's own wiring does.
    let mode = autumn_harvest_plugin::connector::resolve_idempotency_mode(
        binding.target,
        binding.idempotency_mode,
        None,
    );
    ConnectorRuntime::new(
        Arc::new(binding),
        Arc::clone(source) as Arc<dyn autumn_harvest_plugin::connector::EventSource>,
        state.clone(),
        Arc::new(NoOpMetrics),
        mode,
    )
    .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())))
}

// ─────────────────────────── AC3 ───────────────────────────

#[tokio::test]
async fn redelivered_message_creates_exactly_one_execution() {
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order").map_raw(order_mapper),
        &source,
        &state,
        &pool,
    );

    // Deliver the SAME broker coordinates three times, exactly as a
    // rebalance or an un-acked crash would.
    for _ in 0..3 {
        source.push_kafka(0, 91, br#"{"order_id":"o-1"}"#);
        rt.run_once().await.expect("pass");
    }

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE workflow_name = 'fulfil_order'"
        )
        .await,
        1,
        "three redeliveries of one message must produce exactly one execution"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_events WHERE event_type = 'WorkflowStarted'"
        )
        .await,
        1,
        "and exactly one WorkflowStarted event"
    );
    assert_eq!(source.acked().len(), 3, "every delivery is acknowledged");
}

/// A mapper whose `workflow_id` is different on every call.
///
/// Stands in for the realistic hazard: a mapping function that folds a
/// timestamp, a UUID, or any evolving field into the id. Under
/// [`IdempotencyMode::WorkflowId`] such a mapper cannot dedupe a redelivery,
/// because the id IS the dedupe key.
fn drifting_id_mapper(
    ctx: &autumn_harvest_plugin::connector::MessageCtx,
) -> Result<MappedMessage, MappingError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let payload: Value = serde_json::from_slice(&ctx.raw_body)
        .map_err(|e| MappingError::Deserialize(e.to_string()))?;
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    Ok(MappedMessage {
        workflow_id: WorkflowId::new(format!("drift-{n}")),
        payload,
    })
}

#[tokio::test]
async fn broker_coordinate_dedupe_survives_a_mapper_whose_workflow_id_drifts() {
    // AC3's load-bearing claim: the dedupe key comes from STABLE BROKER
    // COORDINATES, not from whatever the mapping function computed. That is
    // precisely why `BrokerCoordinates` is the default for a `Starts` binding
    // -- it holds even when the mapper's id does not.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_raw(drifting_id_mapper)
            .idempotency_mode(IdempotencyMode::BrokerCoordinates),
        &source,
        &state,
        &pool,
    );

    for _ in 0..3 {
        source.push_kafka(0, 404, br#"{"order_id":"o-drift"}"#);
        rt.run_once().await.expect("pass");
    }

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE workflow_name = 'fulfil_order'"
        )
        .await,
        1,
        "the broker coordinate deduped all three, despite three distinct workflow_ids",
    );
    assert_eq!(source.acked().len(), 3);
}

#[tokio::test]
async fn workflow_id_dedupe_cannot_survive_a_mapper_whose_workflow_id_drifts() {
    // The falsifying control for the test above. Without it, "BrokerCoordinates
    // dedupes" would be unfalsifiable -- every dedupe assertion would pass
    // under either mode, because a stable mapper makes the two indistinguishable.
    //
    // This is also the honest warning for `SignalsWithStart`, which is forced
    // into `WorkflowId` mode (a keyed start is mutually exclusive with the
    // signal-with-start path): its mapping function MUST derive a stable id.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_raw(drifting_id_mapper)
            .idempotency_mode(IdempotencyMode::WorkflowId),
        &source,
        &state,
        &pool,
    );

    for _ in 0..3 {
        source.push_kafka(0, 505, br#"{"order_id":"o-drift"}"#);
        rt.run_once().await.expect("pass");
    }

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE workflow_name = 'fulfil_order'"
        )
        .await,
        3,
        "workflow_id dedupe cannot collapse ids that differ -- the two modes \
         are genuinely distinct, which is what makes the sibling test meaningful",
    );
}

#[tokio::test]
async fn redelivered_message_delivers_exactly_one_signal() {
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("cart")]);

    let source = Arc::new(MockSource::new("cart-events"));
    let rt = runtime_for(
        SourceBinding::signals_with_start("cart", "cart-events", "cart", "item_added")
            .map_raw(order_mapper),
        &source,
        &state,
        &pool,
    );

    for _ in 0..3 {
        source.push_opaque("dedup-1", br#"{"order_id":"cart-7"}"#);
        rt.run_once().await.expect("pass");
    }

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = 'cart'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&mut conn, "SELECT COUNT(*) AS n FROM harvest_signals").await,
        1,
        "the same broker message must never deliver two signals"
    );
}

// ─────────────────────────── AC4 ───────────────────────────

#[tokio::test]
async fn crash_between_commit_and_ack_dedupes_on_redelivery() {
    // AC4's falsifiable bar. `settle` is what acks, so dropping the runtime
    // right after `map_and_dispatch` commits — modelled here by dispatching
    // through a runtime whose source is then replaced — leaves the message
    // unacknowledged. The broker redelivers, and the redelivery must resolve
    // as an idempotent replay rather than a second run.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    // Pass 1: dispatch commits.
    let first = Arc::new(MockSource::new("orders"));
    first.push_kafka(0, 5, br#"{"order_id":"o-crash"}"#);
    let rt1 = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order").map_raw(order_mapper),
        &first,
        &state,
        &pool,
    );
    rt1.run_once().await.expect("pass 1");
    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        1
    );

    // "Crash": a brand-new runtime (fresh in-process offset/poison state, as
    // after a restart) receives the same message again because the broker
    // never saw a commit.
    let second = Arc::new(MockSource::new("orders"));
    second.push_kafka(0, 5, br#"{"order_id":"o-crash"}"#);
    let rt2 = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order").map_raw(order_mapper),
        &second,
        &state,
        &pool,
    );
    let summary = rt2.run_once().await.expect("pass 2");

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        1,
        "the post-crash redelivery must not create a second execution"
    );
    assert_eq!(summary.acked, 1, "and it is acknowledged this time");
    assert_eq!(second.acked().len(), 1);
}

#[tokio::test]
async fn a_rejected_dispatch_is_never_acked_as_success() {
    // A target the registry does not know is a deterministic refusal (404), so
    // it dead-letters rather than looping — but crucially it is never counted
    // as a successful dispatch.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    // Register a DIFFERENT workflow so the binding's target is unknown.
    let state = api_state(&pool, vec![workflow_info("something_else")]);

    let source = Arc::new(MockSource::new("orders"));
    source.push_kafka(0, 1, br#"{"order_id":"o-1"}"#);
    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order").map_raw(order_mapper),
        &source,
        &state,
        &pool,
    );
    let summary = rt.run_once().await.expect("pass");

    assert_eq!(summary.dead_lettered, 1);
    assert_eq!(summary.acked, 0);
    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        0,
        "a refused dispatch starts nothing"
    );
    let rows = diesel::sql_query(
        "SELECT reason AS v FROM harvest_connector_dead_letters ORDER BY failed_at",
    )
    .load::<TextRow>(&mut conn)
    .await
    .expect("dlq rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v, "target_rejected");
}

#[tokio::test]
async fn a_second_dead_letter_for_one_key_is_reported_as_already_recorded() {
    // `ON CONFLICT (idempotency_key) DO NOTHING` makes the write idempotent,
    // and the affected-row count is that conflict made observable. It is the
    // only DURABLE answer to "is this a new poison message": the in-process
    // tracker is emptied by the very restart that causes the redelivery (the
    // process died between writing the record and acking the broker), so
    // without this the `poisoned` counter gains a sample per restart.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;

    let entry = |detail: &str| ConnectorDeadLetter {
        binding: "orders".to_string(),
        stream: "orders".to_string(),
        coordinates: "orders:0:1".to_string(),
        idempotency_key: "conn:L6:orders:L0::L11:orders:0:1".to_string(),
        workflow_name: "fulfil_order".to_string(),
        reason: autumn_harvest::telemetry::PoisonReason::Malformed,
        detail: detail.to_string(),
        attempts: 1,
        payload: b"{".to_vec(),
        failed_at: chrono::Utc::now(),
    };

    assert_eq!(
        insert_dead_letter(&mut conn, &entry("bad json"))
            .await
            .expect("first insert"),
        DeadLetterOutcome::Recorded,
        "the first write creates the record"
    );
    assert_eq!(
        insert_dead_letter(&mut conn, &entry("bad json (again)"))
            .await
            .expect("second insert"),
        DeadLetterOutcome::AlreadyRecorded,
        "the same message came back; the row was already there, and the caller \
         needs to know so it does not count the poison message twice"
    );

    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_connector_dead_letters"
        )
        .await,
        1,
        "one record per poison message, however many times it is redelivered"
    );
    let rows = diesel::sql_query("SELECT detail AS v FROM harvest_connector_dead_letters")
        .load::<TextRow>(&mut conn)
        .await
        .expect("dlq rows");
    assert_eq!(
        rows[0].v, "bad json",
        "DO NOTHING leaves the original row untouched"
    );
}

// ─────────────────────────── AC5 ───────────────────────────

#[tokio::test]
async fn a_throttle_deferred_start_is_acked_not_retried() {
    // AC5: a `202 Accepted` deferral is a SUCCESS. Busy-retrying it would
    // defeat the very throttle that deferred it.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![throttled_info("sync_tenant")]);

    // A throttled target cannot take a keyed start (they are mutually
    // exclusive, `400`), so the binding dedupes on the mapper's workflow_id.
    let binding = SourceBinding::starts("tenants", "tenants", "sync_tenant")
        .map_raw(|ctx| {
            let payload: Value = serde_json::from_slice(&ctx.raw_body)
                .map_err(|e| MappingError::Deserialize(e.to_string()))?;
            let tenant = payload
                .get("tenant_id")
                .and_then(Value::as_str)
                .ok_or_else(|| MappingError::Rejected("missing tenant_id".to_string()))?;
            Ok(MappedMessage {
                workflow_id: WorkflowId::new(format!("{tenant}-{}", ctx.coordinates.render())),
                payload,
            })
        })
        .idempotency_mode(IdempotencyMode::WorkflowId);

    let source = Arc::new(MockSource::new("tenants"));
    // Burst of 4 against a burst-1 bucket: the first admits, the rest defer.
    for offset in 0..4 {
        source.push_kafka(0, offset, br#"{"tenant_id":"acme"}"#);
    }
    let rt = runtime_for(binding, &source, &state, &pool);
    let summary = rt.run_once().await.expect("pass");

    assert_eq!(summary.received, 4);
    assert_eq!(
        summary.acked, 4,
        "every message settles: one started, three durably deferred"
    );
    assert_eq!(summary.retried, 0, "a deferral must never be retried");
    assert_eq!(summary.dead_lettered, 0);
    assert!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_start_throttle"
        )
        .await
            >= 1,
        "the deferred starts are durably parked for the throttle scanner"
    );
}

// ─────────────────────────── AC6 ───────────────────────────

#[tokio::test]
async fn one_poison_message_does_not_block_a_hundred_valid_ones() {
    // AC6's falsifiable bar, verbatim: 1 poison + 100 valid → all 100 dispatch.
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 16);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    source.push_kafka(0, 0, b"{ this is not json");
    for i in 1..=100 {
        source.push_kafka(0, i, format!(r#"{{"order_id":"o-{i}"}}"#).as_bytes());
    }

    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_raw(order_mapper)
            .max_in_flight(16),
        &source,
        &state,
        &pool,
    );
    let config = autumn_harvest_plugin::connector::ConnectorRuntimeConfig {
        max_batch: 200,
        ..Default::default()
    };
    let rt = rt.with_config(config);

    // Drained across passes, NOT in one: a pass receives at most
    // `effective_max_batch(max_batch, max_in_flight)` — 16 here, not the
    // configured 200 — because capping the prefetch at in-flight capacity IS
    // the backpressure guarantee. Asserting a 101-message single pass would be
    // asserting that the cap is absent. The poison sits at offset 0, so it is
    // in the FIRST batch and every later batch has to get past it; that is
    // exactly the head-of-line blocking AC6 forbids, and draining is what
    // exposes it. Bounded so a genuine block fails on the counts below rather
    // than hanging CI.
    let mut total = autumn_harvest_plugin::connector::PassSummary::default();
    for _ in 0..64 {
        let pass = rt.run_once().await.expect("pass");
        if pass.received == 0 {
            break;
        }
        total.received += pass.received;
        total.acked += pass.acked;
        total.retried += pass.retried;
        total.dead_lettered += pass.dead_lettered;
    }

    assert_eq!(total.received, 101);
    assert_eq!(total.dead_lettered, 1, "exactly the poison message");
    assert_eq!(total.acked, 100);
    assert_eq!(
        total.retried, 0,
        "the poison is quarantined, never recycled as a retry"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        100,
        "one poison message must not cost a single valid dispatch"
    );
    let rows = diesel::sql_query("SELECT reason AS v FROM harvest_connector_dead_letters")
        .load::<TextRow>(&mut conn)
        .await
        .expect("dlq rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v, "malformed");
}

// ─────────────────────────── provenance ───────────────────────────

#[tokio::test]
async fn a_broker_started_execution_records_broker_provenance() {
    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 8);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    source.push_kafka(3, 91, br#"{"order_id":"o-1"}"#);
    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order").map_raw(order_mapper),
        &source,
        &state,
        &pool,
    );
    rt.run_once().await.expect("pass");

    let rows =
        diesel::sql_query("SELECT start_source AS v FROM harvest_workflow_executions LIMIT 1")
            .load::<TextRow>(&mut conn)
            .await
            .expect("provenance");
    assert_eq!(rows[0].v, "broker");

    let refs =
        diesel::sql_query("SELECT start_source_ref AS v FROM harvest_workflow_executions LIMIT 1")
            .load::<TextRow>(&mut conn)
            .await
            .expect("provenance ref");
    assert_eq!(
        refs[0].v, "orders:3:91",
        "the rendered coordinates trace the run back to the exact message"
    );
}

// ─────────────────────────── success metric ───────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_ten_thousand_messages_with_redeliveries_and_poison() {
    // The issue's headline success metric, verbatim:
    //   10,000 messages, 5% forced redeliveries, 10 poison messages
    //   → exactly 10,000 − 10 dispatched outcomes, 0 duplicate executions,
    //     10 dead-lettered messages.
    const TOTAL: i64 = 10_000;
    const POISON: i64 = 10;

    let (url, _c) = setup_database().await;
    let mut conn = connect(&url).await;
    reset(&mut conn).await;
    let pool = build_pool(&url, 24);
    let state = api_state(&pool, vec![workflow_info("fulfil_order")]);

    let source = Arc::new(MockSource::new("orders"));
    // Deterministic layout: every 1000th message is poison (10 of them), and
    // every 20th message is redelivered once (5% forced redeliveries).
    let mut delivered = 0_i64;
    for i in 0..TOTAL {
        let poison = i % (TOTAL / POISON) == 0;
        let body = if poison {
            b"{ not json".to_vec()
        } else {
            format!(r#"{{"order_id":"o-{i}"}}"#).into_bytes()
        };
        source.push_kafka(0, i, &body);
        delivered += 1;
        if i % 20 == 0 {
            // Same coordinates again — a forced redelivery.
            source.push_kafka(0, i, &body);
            delivered += 1;
        }
    }
    assert_eq!(delivered, 10_500, "5% forced redeliveries on top of 10,000");

    let rt = runtime_for(
        SourceBinding::starts("orders", "orders", "fulfil_order")
            .map_raw(order_mapper)
            .max_in_flight(32),
        &source,
        &state,
        &pool,
    );
    let config = autumn_harvest_plugin::connector::ConnectorRuntimeConfig {
        max_batch: 512,
        ..Default::default()
    };
    let rt = rt.with_config(config);

    let started = std::time::Instant::now();
    let mut total = autumn_harvest_plugin::connector::PassSummary::default();
    loop {
        let pass = rt.run_once().await.expect("pass");
        if pass.received == 0 {
            break;
        }
        total.received += pass.received;
        total.acked += pass.acked;
        total.retried += pass.retried;
        total.dead_lettered += pass.dead_lettered;
    }
    let elapsed = started.elapsed();

    assert_eq!(total.received, 10_500);
    assert_eq!(total.retried, 0, "nothing should need a redelivery");

    // Exactly 10 dead-lettered *messages* (the 10 poison ids; their
    // redeliveries dedupe on the dead-letter table's unique key).
    let dlq = count(
        &mut conn,
        "SELECT COUNT(*) AS n FROM harvest_connector_dead_letters",
    )
    .await;
    assert_eq!(dlq, POISON, "exactly {POISON} dead-lettered messages");

    // Exactly 10,000 − 10 dispatched outcomes, and zero duplicates.
    let executions = count(
        &mut conn,
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions",
    )
    .await;
    assert_eq!(
        executions,
        TOTAL - POISON,
        "exactly {} dispatched outcomes",
        TOTAL - POISON
    );

    let distinct = count(
        &mut conn,
        "SELECT COUNT(DISTINCT workflow_id) AS n FROM harvest_workflow_executions",
    )
    .await;
    assert_eq!(distinct, executions, "0 duplicate executions");

    println!("soak: {delivered} deliveries settled in {elapsed:?}");
}
