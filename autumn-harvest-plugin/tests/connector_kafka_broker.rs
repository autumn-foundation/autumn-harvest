#![cfg(feature = "kafka")]
//! AC11: the Kafka adapter against a **real broker container** (issue #944).
//!
//! `connector_integration.rs` proves the connector *semantics* (idempotency,
//! ack ordering, poison isolation, backpressure) against a real Postgres with
//! a `MockSource`. This suite proves the other half — that
//! [`KafkaSource`][k] talks to a real broker correctly:
//!
//! * consuming a burst yields exactly N executions,
//! * an offset commit really is a commit (a fresh consumer in the same group
//!   does not re-read committed messages),
//! * a message left unacked really is redelivered, and dedupes onto the
//!   original execution when it is.
//!
//! Requires Docker for both the Kafka broker and Postgres, so it is a Linux-CI
//! suite. Locally: `docker` running, then
//! `cargo test -p autumn-harvest-plugin --features kafka --test connector_kafka_broker`.
//!
//! [k]: autumn_harvest_plugin::connector::KafkaSource

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::WorkflowId;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{WorkflowInfo, context::WorkflowContext};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::connector::{
    ConnectorRuntime, ConnectorRuntimeConfig, IdempotencyMode, KafkaSource, KafkaSourceConfig,
    MappedMessage, MappingError, PostgresDeadLetterSink, SourceBinding,
};
use diesel::sql_types::BigInt;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const CONNECTOR_DLQ_SQL: &str =
    include_str!("../migrations/20260716000000_harvest_connector_dead_letters/up.sql");

const TOPIC: &str = "orders.placed";

fn init_sql() -> Vec<u8> {
    let mut sql = autumn_harvest::full_migrations_sql().to_string();
    sql.push('\n');
    sql.push_str(CONNECTOR_DLQ_SQL);
    sql.into_bytes()
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
        Some("kafka-connector-test".to_string()),
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

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .load::<Count>(conn)
        .await
        .expect("count query")[0]
        .n
}

/// One `KafkaSource` + `ConnectorRuntime` over `group_id`, so a test can spin
/// up a *fresh consumer in the same group* to prove a commit really committed.
fn runtime_for(
    brokers: &str,
    group_id: &str,
    state: &HarvestApiState,
    pool: &DbPool,
) -> ConnectorRuntime {
    let source = Arc::new(
        KafkaSource::connect(&KafkaSourceConfig::new(brokers, group_id, TOPIC))
            .expect("kafka consumer connects"),
    );
    let binding = SourceBinding::starts("orders", TOPIC, "fulfil_order").map_raw(order_mapper);

    ConnectorRuntime::new(
        Arc::new(binding),
        source as Arc<dyn autumn_harvest_plugin::connector::EventSource>,
        state.clone(),
        Arc::new(NoOpMetrics),
        IdempotencyMode::BrokerCoordinates,
    )
    .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())))
    .with_config(ConnectorRuntimeConfig {
        // Generous: a cold consumer group must join and get its assignment
        // before the first poll can return anything.
        poll_timeout: Duration::from_secs(15),
        max_batch: 64,
        ..Default::default()
    })
}

async fn produce(brokers: &str, bodies: &[Value]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for body in bodies {
        let payload = serde_json::to_vec(body).expect("serializable");
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .payload(&payload)
                    .key(body["order_id"].as_str().unwrap_or("k")),
                Duration::from_secs(10),
            )
            .await
            .expect("produce");
    }
}

/// Drain until `want` messages have been received, or the budget expires.
async fn drain_until(runtime: &ConnectorRuntime, want: usize) -> usize {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut received = 0;
    while received < want && std::time::Instant::now() < deadline {
        let pass = runtime.run_once().await.expect("pass");
        received += pass.received;
    }
    received
}

// ─────────────────────────── the tests ───────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_on_a_real_topic_yields_exactly_one_execution_per_message() {
    let kafka = Kafka::default().start().await.expect("kafka container");
    let brokers = format!(
        "{}:{}",
        kafka.get_host().await.unwrap(),
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let pg = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container");
    let url = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        pg.get_host().await.unwrap(),
        pg.get_host_port_ipv4(5432).await.unwrap()
    );

    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
    let pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("conn");
    let state = api_state(&pool);

    let bodies: Vec<Value> = (0..25)
        .map(|i| json!({"order_id": format!("A-{i}")}))
        .collect();
    produce(&brokers, &bodies).await;

    let runtime = runtime_for(&brokers, "harvest-burst", &state, &pool);
    let received = drain_until(&runtime, bodies.len()).await;
    assert_eq!(received, bodies.len(), "every produced message is consumed");

    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        25,
        "exactly one execution per message"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_connector_dead_letters"
        )
        .await,
        0,
        "nothing was poisoned"
    );
    // Provenance points back at real Kafka coordinates.
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions \
             WHERE start_source = 'broker' AND start_source_ref LIKE 'orders.placed:%'"
        )
        .await,
        25,
        "every run records its topic:partition:offset"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_offset_is_not_re_read_and_a_redelivery_dedupes() {
    let kafka = Kafka::default().start().await.expect("kafka container");
    let brokers = format!(
        "{}:{}",
        kafka.get_host().await.unwrap(),
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let pg = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container");
    let url = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        pg.get_host().await.unwrap(),
        pg.get_host_port_ipv4(5432).await.unwrap()
    );

    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.as_str());
    let pool: DbPool = deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("conn");
    let state = api_state(&pool);

    let bodies: Vec<Value> = (0..5)
        .map(|i| json!({"order_id": format!("B-{i}")}))
        .collect();
    produce(&brokers, &bodies).await;

    // Pass 1: consume and commit.
    {
        let runtime = runtime_for(&brokers, "harvest-commit", &state, &pool);
        assert_eq!(drain_until(&runtime, bodies.len()).await, bodies.len());
    }
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        5,
    );

    // Pass 2: a brand-new consumer in the SAME group. If the offsets really
    // committed, it sees nothing. This is the falsifiable half of AC4 — a
    // no-op `ack` would leave the messages uncommitted and this would re-read
    // all five.
    {
        let runtime = runtime_for(&brokers, "harvest-commit", &state, &pool);
        let pass = runtime.run_once().await.expect("pass");
        assert_eq!(
            pass.received, 0,
            "committed offsets must not be re-read by a fresh consumer in the same group"
        );
    }

    // Pass 3: a DIFFERENT group re-reads from the beginning — a deliberate
    // redelivery of the very same messages. Dedupe must collapse them onto the
    // original executions rather than creating five more.
    {
        let runtime = runtime_for(&brokers, "harvest-replay", &state, &pool);
        assert_eq!(drain_until(&runtime, bodies.len()).await, bodies.len());
    }
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        5,
        "a full redelivery creates no duplicate executions"
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) AS n FROM harvest_events").await,
        5,
        "and appends no second WorkflowStarted"
    );
}
