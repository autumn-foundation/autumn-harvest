//! Testcontainers integration tests for the inbound webhook receiver
//! (issue #344).
//!
//! Requires Docker. In sandboxes without a Docker daemon these tests are
//! compile-checked with `cargo test --no-run` (repo precedent: #543/#544).
//!
//! Mirrors `webhook_durable_integration.rs`'s harness shape
//! (`TestApp::plugin` + `TestDb::shared()` +
//! `autumn_web::migrate::run_pending`) rather than the hand-rolled
//! per-migration `INIT_SQL` pattern some other integration tests use --
//! that pattern needs the full harvest migration set enumerated by hand
//! and buys nothing extra here.

#![cfg(feature = "webhooks")]
#![allow(
    clippy::unused_async,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps
)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::security::hmac_sha256_hex;
use autumn_web::test::{TestApp, TestDb};
use autumn_web::webhook::{WebhookConfig, WebhookEndpointConfig};
use diesel_async::RunQueryDsl;

const SECRET: &str = "test-webhook-secret-at-least-16-bytes";

#[workflow]
async fn order_flow(_ctx: &WorkflowContext, order_id: String) -> Result<String, String> {
    Ok(order_id)
}

#[workflow]
async fn subscription_flow(ctx: &WorkflowContext) -> Result<String, String> {
    let payment: serde_json::Value = ctx
        .receive_signal("payment_succeeded")
        .await
        .map_err(|e| e.to_string())?;
    Ok(payment.to_string())
}

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("order-{}", evt.order_id)))
}

// A throttled Starts target (issue #607). Used to prove that an upstream
// provider `Idempotency-Key` header does NOT trip issue #808's
// `idempotency_key` + throttle mutual-exclusion `400` on a webhook start --
// the webhook receiver strips the raw header before delegating.
#[workflow(throttle(rate = "100/m", burst = 100))]
async fn throttled_order_flow(_ctx: &WorkflowContext, order_id: String) -> Result<String, String> {
    Ok(order_id)
}

#[webhook(path = "/hooks/throttled-orders", starts = "throttled_order_flow")]
fn map_throttled_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("torder-{}", evt.order_id)))
}

#[webhook(
    path = "/hooks/subscriptions",
    signals = "subscription_flow",
    signal_name = "payment_succeeded"
)]
fn map_subscription(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("subscription-shared"))
}

fn sign(body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(SECRET.as_bytes(), body))
}

fn webhook_config() -> autumn_web::config::AutumnConfig {
    let mut config = autumn_web::config::AutumnConfig::default();
    config.security.webhooks = WebhookConfig {
        endpoints: vec![
            WebhookEndpointConfig::generic("orders", "/hooks/orders", SECRET)
                .without_replay_protection(),
            WebhookEndpointConfig::generic("throttled-orders", "/hooks/throttled-orders", SECRET)
                .without_replay_protection(),
            WebhookEndpointConfig::generic("subscriptions", "/hooks/subscriptions", SECRET)
                .without_replay_protection(),
        ],
        ..Default::default()
    };
    config
}

async fn count_executions(
    pool: &diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    workflow_name: &str,
) -> i64 {
    let mut conn = pool.get().await.expect("pool conn");
    diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("count query")
    .count
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// Env-aware test-DB handle for the #740 provenance test.
///
/// Honours `HARVEST_TEST_DATABASE_URL` (a **blank** Postgres that the test
/// migrates itself via `autumn_web::migrate::run_pending`, exactly as it does
/// against a fresh testcontainer) so it can run against a local instance when
/// Docker/testcontainers is unavailable; otherwise falls back to the shared
/// testcontainers `TestDb`. Mirrors the `setup_*_or_env` fallback pattern used
/// by the core `integration_e2e` suite and `start_source_integration.rs`.
///
/// Exposes the same `url()` / `pool()` / `execute_sql()` surface the test
/// relied on from `TestDb`, so only the DB-acquisition line of the test
/// changes.
enum WebhookTestDb {
    Container(&'static TestDb),
    Env {
        url: String,
        pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    },
}

impl WebhookTestDb {
    async fn get() -> Self {
        if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
                diesel_async::AsyncPgConnection,
            >::new(&url);
            let pool = diesel_async::pooled_connection::deadpool::Pool::builder(manager)
                .max_size(5)
                .build()
                .expect("failed to build local-PG pool");
            Self::Env { url, pool }
        } else {
            Self::Container(TestDb::shared().await)
        }
    }

    fn url(&self) -> &str {
        match self {
            Self::Container(db) => db.url(),
            Self::Env { url, .. } => url,
        }
    }

    fn pool(
        &self,
    ) -> diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection> {
        match self {
            Self::Container(db) => db.pool(),
            Self::Env { pool, .. } => pool.clone(),
        }
    }

    async fn execute_sql(&self, sql: &str) {
        match self {
            Self::Container(db) => db.execute_sql(sql).await,
            Self::Env { pool, .. } => {
                let mut conn = pool.get().await.expect("pool conn");
                diesel::sql_query(sql)
                    .execute(&mut *conn)
                    .await
                    .unwrap_or_else(|e| panic!("SQL execution failed: {e}\nSQL: {sql}"));
            }
        }
    }
}

/// AC (issue #344): "a synthetic vendor sends two identical webhook
/// deliveries; exactly one workflow execution is created; both responses
/// return the same `workflow_exec_id`."
// Multi-thread runtime: `TestApp::plugin(HarvestPlugin)` runs the plugin
// startup hook on a spawned thread via `Handle::block_on` while the test thread
// blocks on its join, which deadlocks a current-thread runtime (see the fuller
// note on `webhook_start_records_webhook_source`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_webhook_delivery_creates_exactly_one_execution_with_same_exec_id() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;
    db.execute_sql("TRUNCATE TABLE harvest_audit_log CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-order-1",
        "order_id": "o-42"
    }))
    .unwrap();
    let sig = sign(&body);

    let first = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-order-1")
        .body(body.clone())
        .send()
        .await;
    first.assert_status(202);
    let first_json: serde_json::Value = first.json();
    assert_eq!(first_json["status"], "accepted");
    let exec_id = first_json["workflow_exec_id"]
        .as_str()
        .expect("workflow_exec_id present")
        .to_string();

    let second = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-order-1")
        .body(body)
        .send()
        .await;
    second.assert_status(200);
    let second_json: serde_json::Value = second.json();
    assert_eq!(second_json["status"], "idempotent_replay");
    assert_eq!(
        second_json["workflow_exec_id"].as_str().unwrap(),
        exec_id,
        "redelivery must resolve to the same execution id"
    );

    let count = count_executions(&db.pool(), "order_flow").await;
    assert_eq!(
        count, 1,
        "exactly one execution must exist after redelivery"
    );

    // Audit: a dispatch attempt is recorded (issue #344, OP_WEBHOOK_TRIGGER).
    let mut conn = db.pool().get().await.expect("pool conn");
    let audit_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_audit_log WHERE operation = 'webhook.trigger'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("audit count query")
    .count;
    assert!(
        audit_count >= 1,
        "expected at least one webhook.trigger audit row"
    );
}

/// AC3 (issue #740): a start delegated from the inbound webhook receiver records
/// `start_source = "webhook"`, distinguishing it from an ordinary `api` start.
///
/// Uses a multi-thread runtime: `TestApp::plugin(HarvestPlugin)` runs the
/// plugin startup hook on a spawned thread via `Handle::block_on` while the
/// test thread blocks on its join, which deadlocks a current-thread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_start_records_webhook_source() {
    #[derive(diesel::QueryableByName)]
    struct SourceRow {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        start_source: Option<String>,
    }

    let _ = tracing_subscriber::fmt::try_init();

    let db = WebhookTestDb::get().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-src-1",
        "order_id": "o-src"
    }))
    .unwrap();
    let sig = sign(&body);

    let resp = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-src-1")
        .body(body)
        .send()
        .await;
    resp.assert_status(202);

    let mut conn = db.pool().get().await.expect("pool conn");
    let row: SourceRow = diesel::sql_query(
        "SELECT start_source FROM harvest_workflow_executions WHERE workflow_name = 'order_flow' LIMIT 1",
    )
    .get_result(&mut conn)
    .await
    .expect("created execution row");
    assert_eq!(
        row.start_source.as_deref(),
        Some("webhook"),
        "a webhook-delegated start must record start_source='webhook'"
    );
}

/// The `signals` target variant dedupes on the verified delivery ID: two
/// identical deliveries admit exactly one `SignalReceived` event, not two.
// Multi-thread runtime: `TestApp::plugin` deadlocks a current-thread runtime
// (see `webhook_start_records_webhook_source`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_webhook_delivery_to_signals_target_delivers_signal_exactly_once() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;
    db.execute_sql("TRUNCATE TABLE harvest_signals CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-payment-1",
        "amount_cents": 500
    }))
    .unwrap();
    let sig = sign(&body);

    let first = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-payment-1")
        .body(body.clone())
        .send()
        .await;
    first.assert_status(202);

    let second = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-payment-1")
        .body(body)
        .send()
        .await;
    second.assert_status(200);

    let count = count_executions(&db.pool(), "subscription_flow").await;
    assert_eq!(
        count, 1,
        "exactly one execution must exist after redelivery"
    );

    // The idempotency key is namespaced by binding (path + signal_name), not
    // the raw provider delivery id -- see webhook_receiver.rs::handle_webhook.
    let mut conn = db.pool().get().await.expect("pool conn");
    let signal_count: i64 = diesel::sql_query(
        "SELECT COUNT(*) as count FROM harvest_signals WHERE idempotency_key = \
         '/hooks/subscriptions:payment_succeeded:dlv-payment-1'",
    )
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("signal count query")
    .count;
    assert_eq!(signal_count, 1, "the signal must be admitted exactly once");
}

/// A blank top-level `"id"` field (`autumn_web::webhook`'s JSON-body
/// delivery-id fallback has no non-empty guard) must be treated as "no
/// delivery id resolved" -- not a genuine, if empty, id that would let two
/// unrelated `SignalsWithStart` deliveries collide on the same namespaced
/// idempotency key (Codex review, PR #918).
// Multi-thread runtime: `TestApp::plugin` deadlocks a current-thread runtime
// (see `webhook_start_records_webhook_source`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blank_delivery_id_is_rejected_as_missing_for_signals_target() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![order_flow, subscription_flow])
                .webhooks(webhooks![map_order, map_subscription])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "",
        "amount_cents": 500
    }))
    .unwrap();
    let sig = sign(&body);

    let resp = client
        .post("/hooks/subscriptions")
        .header("X-Webhook-Signature", &sig)
        .body(body)
        .send()
        .await;
    resp.assert_status(400);
    resp.assert_body_contains("missing_idempotency");
}

/// Regression (Codex P2, PR #992): a webhook `Starts` target whose workflow
/// carries a throttle policy must NOT be broken by an upstream provider
/// `Idempotency-Key` header. Issue #808 made `start_workflow` read that header
/// as a start-dedup key and reject it with a `400` when the workflow has a
/// throttle/debounce/batch policy. Many providers (Stripe et al.) send their
/// own `Idempotency-Key` on deliveries -- that is the provider's key, not a
/// Harvest start key. The webhook receiver strips it before delegating, so the
/// mutual-exclusion `400` is never reached and the throttled start proceeds
/// via the normal webhook path.
// Multi-thread runtime: `TestApp::plugin` deadlocks a current-thread runtime
// (see `webhook_start_records_webhook_source`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_idempotency_key_header_does_not_trip_throttle_mutual_exclusion_on_starts() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![
                    order_flow,
                    subscription_flow,
                    throttled_order_flow
                ])
                .webhooks(webhooks![map_order, map_subscription, map_throttled_order])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    let body = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-throttle-1",
        "order_id": "t-1"
    }))
    .unwrap();
    let sig = sign(&body);

    let resp = client
        .post("/hooks/throttled-orders")
        .header("X-Webhook-Signature", &sig)
        .header("X-Webhook-Delivery", "dlv-throttle-1")
        // The upstream provider's own idempotency key -- must NOT drive #808.
        .header("Idempotency-Key", "provider-key-abc")
        .body(body)
        .send()
        .await;

    // The regression proof: with the raw header stripped, the start is NOT
    // rejected with the #808 mutual-exclusion 400; it dispatches normally.
    resp.assert_status(202);
    let json: serde_json::Value = resp.json();
    assert_eq!(json["status"], "accepted");
    assert!(
        json["workflow_exec_id"].as_str().is_some(),
        "a throttled webhook start must produce an execution, got {json}"
    );

    let count = count_executions(&db.pool(), "throttled_order_flow").await;
    assert_eq!(
        count, 1,
        "the throttled webhook start must create exactly one execution"
    );
}

/// Regression (Codex P2, PR #992): two DISTINCT webhook deliveries that map to
/// DIFFERENT `workflow_id`s but carry the SAME upstream provider
/// `Idempotency-Key` header must produce TWO distinct executions. Without the
/// header strip, issue #808's key dedup (claim keyed by
/// `(workflow_name, idempotency_key)`) would collapse the second delivery into
/// the first even though it is a genuinely different order. Webhook starts
/// dedupe via the mapper's `workflow_id` (reuse policy on
/// `(workflow_name, workflow_id)`), never the provider's header.
// Multi-thread runtime: `TestApp::plugin` deadlocks a current-thread runtime
// (see `webhook_start_records_webhook_source`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_deliveries_sharing_an_upstream_idempotency_key_are_not_collapsed() {
    let _ = tracing_subscriber::fmt::try_init();

    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db.execute_sql("TRUNCATE TABLE harvest_workflow_executions CASCADE")
        .await;

    let client = TestApp::new()
        .config(webhook_config())
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![
                    order_flow,
                    subscription_flow,
                    throttled_order_flow
                ])
                .webhooks(webhooks![map_order, map_subscription, map_throttled_order])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .with_db(db.pool())
        .build();

    // Two different orders (different mapper workflow_ids) that happen to carry
    // the SAME upstream provider idempotency key.
    let shared_provider_key = "provider-shared-xyz";

    let body_a = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-order-a",
        "order_id": "collapse-a"
    }))
    .unwrap();
    let first = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sign(&body_a))
        .header("X-Webhook-Delivery", "dlv-order-a")
        .header("Idempotency-Key", shared_provider_key)
        .body(body_a)
        .send()
        .await;
    first.assert_status(202);
    let first_json: serde_json::Value = first.json();
    assert_eq!(first_json["status"], "accepted");
    let exec_a = first_json["workflow_exec_id"]
        .as_str()
        .expect("workflow_exec_id present")
        .to_string();

    let body_b = serde_json::to_vec(&serde_json::json!({
        "id": "dlv-order-b",
        "order_id": "collapse-b"
    }))
    .unwrap();
    let second = client
        .post("/hooks/orders")
        .header("X-Webhook-Signature", &sign(&body_b))
        .header("X-Webhook-Delivery", "dlv-order-b")
        .header("Idempotency-Key", shared_provider_key)
        .body(body_b)
        .send()
        .await;
    // A genuinely new order: 202 accepted, NOT a 200 idempotent_replay of the
    // first (which is what #808 key dedup would have wrongly produced).
    second.assert_status(202);
    let second_json: serde_json::Value = second.json();
    assert_eq!(second_json["status"], "accepted");
    let exec_b = second_json["workflow_exec_id"]
        .as_str()
        .expect("workflow_exec_id present")
        .to_string();

    assert_ne!(
        exec_a, exec_b,
        "two distinct orders sharing a provider idempotency key must be distinct executions"
    );
    let count = count_executions(&db.pool(), "order_flow").await;
    assert_eq!(
        count, 2,
        "the shared upstream Idempotency-Key header must not collapse two distinct orders"
    );
}
