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
    include_str!("../migrations/harvest/20260719900000_harvest_connector_dead_letters/up.sql");

const TOPIC: &str = "orders.placed";

fn init_sql() -> Vec<u8> {
    let mut sql = autumn_harvest::test_init_sql().to_string();
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
        quota: None,
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

/// Lag must be measured from the DURABLE committed group offset, not the local
/// next-fetch position (Codex review of a872555).
///
/// `position()` advances the instant a record is fetched, regardless of
/// whether it has been dispatched, is being retried, or is stuck behind a
/// blocked commit prefix. A consumer wedged with a large uncommitted backlog —
/// exactly the condition the `harvest.connector.lag` gauge exists to expose —
/// would therefore report its lag collapsing to zero while a restart replayed
/// everything from the last commit.
///
/// This drives the source directly rather than through `ConnectorRuntime`, so
/// the records are *fetched but never acked*: the honest reproduction of a
/// blocked prefix.
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn lag_reflects_the_committed_offset_not_the_fetch_position() {
    use autumn_harvest_plugin::connector::EventSource;

    let kafka = Kafka::default()
        .with_tag("3.9.1")
        .start()
        .await
        .expect("kafka starts");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.expect("port")
    );

    let bodies: Vec<Value> = (0..5)
        .map(|i| json!({"order_id": format!("lag-{i}")}))
        .collect();
    produce(&brokers, &bodies).await;

    let source = KafkaSource::connect(&KafkaSourceConfig::new(&brokers, "harvest-lag", TOPIC))
        .expect("kafka consumer connects");

    // Fetch every record without acking any of them.
    let mut fetched = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while fetched < bodies.len() && std::time::Instant::now() < deadline {
        fetched += source
            .receive(64, Duration::from_secs(15))
            .await
            .expect("receive")
            .len();
    }
    assert_eq!(fetched, bodies.len(), "all records were fetched");

    // Nothing was committed, so the whole backlog is still outstanding. Under
    // the old `position()`-based computation this read as 0.
    let lag = source.lag().await.expect("lag is available");
    let produced = i64::try_from(bodies.len()).expect("fixture size fits an i64");
    assert!(
        lag >= produced,
        "fetched-but-uncommitted records must still count as lag, got {lag}"
    );
}

/// Both replicas of a consumer group report the SAME lag, and it is the whole
/// group's backlog rather than the share of partitions each one happens to own.
///
/// This is the property the dashboard depends on. It aggregates the gauge with
/// `max by (source)` — correct for SQS, where every replica reads the same
/// whole-queue depth — so an assignment-scoped Kafka sample would surface the
/// largest replica's subtotal (roughly `1/replicas` of the truth) and hide a
/// partition stalled on any of the others.
///
/// It needs a real broker because the bug is in *which* partitions the sample
/// asks about: the unit tests take the partition list as a parameter, so they
/// cannot distinguish `fetch_metadata` from `assignment()`. Only a genuine
/// group with a genuine split assignment can.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (testcontainers)"]
async fn every_replica_in_a_group_reports_the_whole_group_lag() {
    use autumn_harvest_plugin::connector::EventSource;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    const PARTITIONS: i32 = 4;
    const GROUP: &str = "harvest-group-lag";
    const MULTI_TOPIC: &str = "orders.multipartition";

    let kafka = Kafka::default()
        .with_tag("3.9.1")
        .start()
        .await
        .expect("kafka starts");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.expect("port")
    );

    // Explicit multi-partition topic: an auto-created one would have a single
    // partition, which cannot split across replicas and so cannot exhibit the
    // bug at all.
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(
                MULTI_TOPIC,
                PARTITIONS,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");

    // One record per partition, addressed explicitly so the backlog is spread
    // rather than hashed onto one partition.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    let per_partition = 3;
    for partition in 0..PARTITIONS {
        for i in 0..per_partition {
            let payload = serde_json::to_vec(&json!({"order_id": format!("p{partition}-{i}")}))
                .expect("serializable");
            producer
                .send(
                    FutureRecord::to(MULTI_TOPIC)
                        .payload(&payload)
                        .partition(partition)
                        .key("k"),
                    Duration::from_secs(10),
                )
                .await
                .expect("produce");
        }
    }
    let total_backlog = i64::from(PARTITIONS) * i64::from(per_partition);

    // Two replicas in ONE group: Kafka splits the four partitions between them.
    let replica_a = KafkaSource::connect(&KafkaSourceConfig::new(&brokers, GROUP, MULTI_TOPIC))
        .expect("replica A connects");
    let replica_b = KafkaSource::connect(&KafkaSourceConfig::new(&brokers, GROUP, MULTI_TOPIC))
        .expect("replica B connects");

    // Drive both until each holds a non-empty assignment, so the split is real
    // and neither is reporting from an empty one.
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        let _ = replica_a.receive(16, Duration::from_secs(5)).await;
        let _ = replica_b.receive(16, Duration::from_secs(5)).await;
        if replica_a.lag().await.is_some() && replica_b.lag().await.is_some() {
            break;
        }
    }

    let lag_a = replica_a.lag().await.expect("replica A reports lag");
    let lag_b = replica_b.lag().await.expect("replica B reports lag");

    assert_eq!(
        lag_a, lag_b,
        "both replicas must report the same group-wide lag, got A={lag_a} B={lag_b}"
    );
    assert_eq!(
        lag_a, total_backlog,
        "the sample must be the whole group backlog ({total_backlog}), not one replica's share"
    );
}
