#![cfg(feature = "sqs")]
//! AC11: the SQS adapter against a **real queue container** (issue #944).
//!
//! `connector_integration.rs` proves the connector *semantics* against a real
//! Postgres with a `MockSource`. This suite proves the other half — that
//! [`SqsSource`][s] talks to a real SQS-compatible queue correctly:
//!
//! * consuming a burst yields exactly N executions,
//! * an ack really is a `DeleteMessage` (a second receive finds nothing), and
//! * a message the connector *abandons* is redelivered and dedupes onto the
//!   original execution.
//!
//! The container is [ElasticMQ], which speaks the SQS query API and starts in
//! about a second — far cheaper than `LocalStack` for a queue-only test.
//! Requires Docker for both it and Postgres, so it is a Linux-CI suite.
//! Locally: `docker` running, then
//! `cargo test -p autumn-harvest-plugin --features sqs --test connector_sqs_broker`.
//!
//! [s]: autumn_harvest_plugin::connector::SqsSource
//! [ElasticMQ]: https://github.com/softwaremill/elasticmq

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
    ConnectorRuntime, ConnectorRuntimeConfig, IdempotencyMode, MappedMessage, MappingError,
    PostgresDeadLetterSink, SourceBinding, SqsSource, SqsSourceConfig,
};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region};
use diesel::sql_types::BigInt;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::elasticmq::ElasticMq;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const CONNECTOR_DLQ_SQL: &str =
    include_str!("../migrations/harvest/20260719900000_harvest_connector_dead_letters/up.sql");

const QUEUE: &str = "orders";

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
        Some("sqs-connector-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    state
}

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

/// An SQS client pointed at the `ElasticMQ` container.
fn sqs_client(endpoint: &str) -> Client {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("elasticmq"))
        .credentials_provider(Credentials::new("x", "x", None, None, "test"))
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(config)
}

fn runtime_for(
    client: Client,
    queue_url: &str,
    state: &HarvestApiState,
    pool: &DbPool,
) -> ConnectorRuntime {
    let source = Arc::new(SqsSource::new(
        client,
        SqsSourceConfig::new(queue_url)
            .stream(QUEUE)
            // Short, so the abandon-then-redeliver test does not wait long.
            .visibility_timeout_secs(5),
    ));
    let binding = SourceBinding::starts("orders", QUEUE, "fulfil_order").map_raw(order_mapper);

    ConnectorRuntime::new(
        Arc::new(binding),
        source as Arc<dyn autumn_harvest_plugin::connector::EventSource>,
        state.clone(),
        Arc::new(NoOpMetrics),
        IdempotencyMode::BrokerCoordinates,
    )
    .with_dead_letter_sink(Arc::new(PostgresDeadLetterSink::new(pool.clone())))
    .with_config(ConnectorRuntimeConfig {
        poll_timeout: Duration::from_secs(2),
        max_batch: 10,
        ..Default::default()
    })
}

async fn drain_until(runtime: &ConnectorRuntime, want: usize) -> usize {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut received = 0;
    while received < want && std::time::Instant::now() < deadline {
        let pass = runtime.run_once().await.expect("pass");
        received += pass.received;
    }
    received
}

/// Boot `ElasticMQ` + Postgres and wire everything up.
async fn harness() -> (
    Client,
    String,
    DbPool,
    AsyncPgConnection,
    HarvestApiState,
    // Keep the containers alive for the test's lifetime.
    impl Sized,
) {
    let mq = ElasticMq::default().start().await.expect("elasticmq");
    let endpoint = format!(
        "http://{}:{}",
        mq.get_host().await.unwrap(),
        mq.get_host_port_ipv4(9324).await.unwrap()
    );
    let client = sqs_client(&endpoint);
    let queue_url = client
        .create_queue()
        .queue_name(QUEUE)
        .send()
        .await
        .expect("create queue")
        .queue_url
        .expect("queue url");

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
    let conn = AsyncPgConnection::establish(&url).await.expect("conn");
    let state = api_state(&pool);
    (client, queue_url, pool, conn, state, (mq, pg))
}

/// `(visible, in_flight)` message counts straight from the broker.
///
/// This is what makes the ack assertion falsifiable. A message that was
/// *received but never deleted* is **invisible** for its visibility timeout,
/// so a follow-up `receive` returns nothing whether or not the ack actually
/// deleted it — `received == 0` alone cannot tell the two apart. The queue's
/// own attributes can: an acked message leaves BOTH counters at zero, while a
/// no-op ack parks the messages in `NotVisible`.
async fn queue_depth(client: &Client, queue_url: &str) -> (i64, i64) {
    use aws_sdk_sqs::types::QueueAttributeName;

    let out = client
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
        .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
        .send()
        .await
        .expect("get queue attributes");
    let read = |name: &QueueAttributeName| -> i64 {
        out.attributes()
            .and_then(|a| a.get(name))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    (
        read(&QueueAttributeName::ApproximateNumberOfMessages),
        read(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible),
    )
}

async fn send(client: &Client, queue_url: &str, bodies: &[Value]) {
    for body in bodies {
        client
            .send_message()
            .queue_url(queue_url)
            .message_body(body.to_string())
            .send()
            .await
            .expect("send");
    }
}

// ─────────────────────────── the tests ───────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_on_a_real_queue_yields_exactly_one_execution_per_message() {
    let (client, queue_url, pool, mut conn, state, _guard) = harness().await;

    let bodies: Vec<Value> = (0..12)
        .map(|i| json!({"order_id": format!("S-{i}")}))
        .collect();
    send(&client, &queue_url, &bodies).await;

    let runtime = runtime_for(client.clone(), &queue_url, &state, &pool);
    assert_eq!(drain_until(&runtime, bodies.len()).await, bodies.len());

    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        12,
        "exactly one execution per message"
    );

    // The falsifiable half of AC4: an ack really is a DeleteMessage. Asking
    // the broker directly is what makes this a real assertion -- a no-op ack
    // would leave all twelve sitting in `NotVisible` until their visibility
    // timeout expired, which a mere `received == 0` could never distinguish.
    let (visible, in_flight) = queue_depth(&client, &queue_url).await;
    assert_eq!(
        (visible, in_flight),
        (0, 0),
        "an acked message is deleted from the queue, not merely made invisible",
    );

    let pass = runtime.run_once().await.expect("pass");
    assert_eq!(pass.received, 0, "acked messages are deleted, not re-read");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_message_is_redelivered_and_dedupes() {
    let (client, queue_url, pool, mut conn, state, _guard) = harness().await;

    // A body the mapping function rejects: no `order_id`. With the default
    // poison threshold of 3, the first two deliveries are abandoned (visibility
    // reset to 0, so SQS redelivers immediately) and the third dead-letters.
    send(&client, &queue_url, &[json!({"nope": true})]).await;
    // ...alongside a valid one, to prove the poison message never blocks it.
    send(&client, &queue_url, &[json!({"order_id": "S-ok"})]).await;

    let runtime = runtime_for(client.clone(), &queue_url, &state, &pool);

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut dead_lettered = 0;
    while dead_lettered == 0 && std::time::Instant::now() < deadline {
        let pass = runtime.run_once().await.expect("pass");
        dead_lettered += pass.dead_lettered;
    }

    assert_eq!(
        dead_lettered, 1,
        "the poison message is eventually quarantined"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_connector_dead_letters \
             WHERE reason = 'mapping_rejected'"
        )
        .await,
        1,
        "and lands in the dead-letter table with its raw payload"
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) AS n FROM harvest_workflow_executions"
        )
        .await,
        1,
        "the valid message still ran, exactly once, despite the poison sibling"
    );

    // Everything settled: the queue really is empty, in-flight included, so
    // neither the poison message nor the valid one is left circulating.
    let pass = runtime.run_once().await.expect("pass");
    assert_eq!(pass.received, 0, "nothing is left circulating");
    let (visible, in_flight) = queue_depth(&client, &queue_url).await;
    assert_eq!(
        (visible, in_flight),
        (0, 0),
        "a dead-lettered message is acked off the queue, not left in flight",
    );
}
