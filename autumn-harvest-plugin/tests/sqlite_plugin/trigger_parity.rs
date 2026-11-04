//! MCP tools and tasks, webhooks and connectors on SQLite (issue #2471).
//!
//! Each surface starts its runs through the `HarvestSqlite` handle. Each
//! test boots a real app on a temp directory, with no Postgres.

// The `#[webhook]` macro fixes the mapper signature: an owned event and a
// `Result`, as in `webhook_receiver_integration.rs`.
#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::connector::{
    IdempotencyMode, MappedMessage, MappingError, MessageCtx, MockSource, SourceBinding,
};
use autumn_harvest_plugin::mcp_tasks::{CLIENT_CAPABILITIES_META, PAYLOAD_FIELD, TASKS_EXTENSION};
use autumn_web::security::hmac_sha256_hex;
use autumn_web::test::{TestApp, TestClient};
use autumn_web::webhook::{WebhookConfig, WebhookEndpointConfig};
use serde_json::{Value, json};

const SECRET: &str = "test-webhook-secret-at-least-16-bytes";

/// The text of `value`: a string, or the `field` of an event object. A
/// webhook passes the whole event as the input, as on Postgres.
fn text_of(value: &Value, field: &str) -> String {
    value
        .as_str()
        .or_else(|| value[field].as_str())
        .unwrap_or_default()
        .to_owned()
}

#[workflow(mcp, description = "Echoes an order id")]
async fn order_flow(_ctx: &WorkflowContext, order: Value) -> Result<String, String> {
    Ok(text_of(&order, "order_id"))
}

#[workflow(mcp, description = "Waits for one approval")]
async fn approval_flow(ctx: &WorkflowContext, request: Value) -> Result<String, String> {
    let approval: Value = ctx
        .wait_for_signal("approval")
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!(
        "{}:{}",
        text_of(&request, "id"),
        text_of(&approval, "decision")
    ))
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Bump {
    by: i64,
}

#[update(workflow = "approval_flow", mcp)]
async fn bump(_ctx: &WorkflowContext, req: Bump) -> Result<i64, String> {
    Ok(req.by + 1)
}

#[derive(serde::Deserialize)]
struct OrderEvent {
    order_id: String,
}

#[webhook(path = "/hooks/orders", starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("order-{}", evt.order_id)))
}

#[webhook(
    path = "/hooks/approvals",
    signals = "approval_flow",
    signal_name = "approval"
)]
fn map_approval(_ctx: &WebhookCtx, _evt: Value) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new("approval-shared"))
}

#[webhook(path = "/hooks/queued", starts = "order_flow", queue = "fast")]
fn map_queued(_ctx: &WebhookCtx, evt: OrderEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(evt.order_id))
}

fn sign(body: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(SECRET.as_bytes(), body))
}

fn hook_config(dir: &Path) -> autumn_web::config::AutumnConfig {
    let mut config = super::config(dir);
    config.security.webhooks = WebhookConfig {
        endpoints: vec![
            WebhookEndpointConfig::generic("orders", "/hooks/orders", SECRET)
                .without_replay_protection(),
            WebhookEndpointConfig::generic("approvals", "/hooks/approvals", SECRET)
                .without_replay_protection(),
        ],
        ..Default::default()
    };
    config
}

fn base() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![order_flow, approval_flow])
        .updates(updates![bump])
        .api("/harvest")
}

/// Poll `path` until `done` accepts the body, or fail after 15 s.
async fn eventually(client: &TestClient, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.get(path).send().await;
        let body: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
        if response.status.is_success() && done(&body) {
            return body;
        }
        assert!(Instant::now() < deadline, "timed out on {path}: {body}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn runs_of(client: &TestClient, workflow: &str) -> Vec<Value> {
    let body: Value = client
        .get(&format!("/harvest/workflows?workflow_name={workflow}"))
        .send()
        .await
        .json();
    body.as_array().cloned().unwrap_or_default()
}

async fn post_hook(client: &TestClient, path: &str, delivery: &str, body: &Value) -> TestResponse {
    let bytes = serde_json::to_vec(body).unwrap();
    client
        .post(path)
        .header("X-Webhook-Signature", &sign(&bytes))
        .header("X-Webhook-Delivery", delivery)
        .body(bytes)
        .send()
        .await
}

type TestResponse = autumn_web::test::TestResponse;

fn build_panic(plugin: HarvestPlugin, dir: &Path) -> String {
    let config = super::config(dir);
    let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        TestApp::new().config(config).plugin(plugin)
    }));
    let payload = built.err().expect("the build refuses the option");
    payload
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default()
}

// ── Webhooks ─────────────────────────────────────────────────────────────────

/// A webhook start goes through the handle. A redelivery attaches to the
/// same run, and each dispatch writes one audit row to the Harvest file.
#[tokio::test(flavor = "multi_thread")]
async fn a_webhook_starts_a_run_once_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let client = TestApp::new()
        .config(hook_config(dir.path()))
        .profile("dev")
        .plugin(base().webhooks(webhooks![map_order, map_approval]))
        .build();

    let event = json!({ "id": "dlv-1", "order_id": "o-42" });
    let first = post_hook(&client, "/hooks/orders", "dlv-1", &event).await;
    first.assert_status(202);
    let first: Value = first.json();
    assert_eq!(first["status"], "accepted");
    assert_eq!(first["workflow_id"], "order-o-42");
    let exec = first["workflow_exec_id"].as_str().unwrap().to_owned();

    let done = eventually(&client, &format!("/harvest/workflows/{exec}"), |body| {
        body["execution"]["state"] == "COMPLETED"
    })
    .await;
    assert_eq!(done["execution"]["output"], "o-42");

    // `allow_duplicate` returns the run of the key in any state, as on
    // Postgres. So a redelivery after the end starts no second run.
    let again = post_hook(&client, "/hooks/orders", "dlv-1", &event).await;
    again.assert_status(200);
    let again: Value = again.json();
    assert_eq!(again["status"], "idempotent_replay");
    assert_eq!(again["workflow_exec_id"], exec);
    assert_eq!(runs_of(&client, "order_flow").await.len(), 1);

    let audit: Value = client.get("/harvest/admin/audit").send().await.json();
    assert!(
        audit.to_string().contains("webhook.trigger"),
        "a dispatch writes an audit row: {audit}"
    );
}

/// A signal-with-start webhook dedupes a redelivery on its delivery id.
#[tokio::test(flavor = "multi_thread")]
async fn a_webhook_signal_with_start_dedupes_a_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let client = TestApp::new()
        .config(hook_config(dir.path()))
        .profile("dev")
        .plugin(base().webhooks(webhooks![map_order, map_approval]))
        .build();

    let event = json!({ "id": "dlv-a", "decision": "yes" });
    let first = post_hook(&client, "/hooks/approvals", "dlv-a", &event).await;
    first.assert_status(202);
    let first: Value = first.json();
    let exec = first["workflow_exec_id"].as_str().unwrap().to_owned();
    let again = post_hook(&client, "/hooks/approvals", "dlv-a", &event).await;
    again.assert_status(200);
    let again: Value = again.json();
    assert_eq!(again["status"], "idempotent_replay");
    assert_eq!(again["workflow_exec_id"], exec);

    let done = eventually(&client, &format!("/harvest/workflows/{exec}"), |body| {
        body["execution"]["state"] == "COMPLETED"
    })
    .await;
    assert!(
        done["execution"]["output"]
            .as_str()
            .is_some_and(|out| out == "dlv-a:yes"),
        "{done}"
    );
    assert_eq!(runs_of(&client, "approval_flow").await.len(), 1);
}

/// A webhook `queue` has no SQLite meaning, so the build refuses it.
#[test]
fn a_webhook_queue_fails_the_build() {
    let dir = tempfile::tempdir().unwrap();
    let message = build_panic(base().webhooks(webhooks![map_queued]), dir.path());
    assert!(message.contains("queue"), "{message}");
}

// ── Connectors ───────────────────────────────────────────────────────────────

fn order_mapper(ctx: &MessageCtx) -> Result<MappedMessage, MappingError> {
    let payload: Value = serde_json::from_slice(&ctx.raw_body)
        .map_err(|e| MappingError::Deserialize(e.to_string()))?;
    let order_id = payload
        .get("order_id")
        .and_then(Value::as_str)
        .ok_or_else(|| MappingError::Rejected("missing order_id".to_string()))?
        .to_owned();
    Ok(MappedMessage {
        workflow_id: WorkflowId::new(order_id.clone()),
        payload: Value::String(order_id),
    })
}

/// A connector start goes through the handle. A redelivery starts no
/// second run. A poison message lands in the Harvest file and is acked.
#[tokio::test(flavor = "multi_thread")]
async fn a_connector_starts_runs_and_dead_letters_a_poison_message() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(MockSource::new("orders"));
    let binding = SourceBinding::starts("orders", "orders", "order_flow")
        .map_raw(order_mapper)
        .idempotency_mode(IdempotencyMode::WorkflowId)
        .poison_threshold(1);
    let client = TestApp::new()
        .config(super::config(dir.path()))
        .profile("dev")
        .plugin(base().connector(binding, Arc::clone(&source) as _))
        .build();

    source.push_kafka(0, 1, br#"{"order_id":"o-1"}"#);
    let deadline = Instant::now() + Duration::from_secs(15);
    while source.acked().is_empty() {
        assert!(Instant::now() < deadline, "the message is never acked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    eventually(
        &client,
        "/harvest/workflows?workflow_name=order_flow",
        |body| {
            body.as_array()
                .is_some_and(|runs| runs.len() == 1 && runs[0]["state"] == "COMPLETED")
        },
    )
    .await;
    // A redelivery of a finished run starts no second run.
    source.push_kafka(0, 1, br#"{"order_id":"o-1"}"#);
    let deadline = Instant::now() + Duration::from_secs(15);
    while source.acked().len() < 2 {
        assert!(Instant::now() < deadline, "the redelivery is never acked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(runs_of(&client, "order_flow").await.len(), 1);

    source.push_kafka(0, 2, b"not json");
    let deadline = Instant::now() + Duration::from_secs(15);
    while source.acked().len() < 3 {
        assert!(
            Instant::now() < deadline,
            "the poison message is never acked"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let store =
        autumn_harvest_sqlite::SqliteAuthStore::open(dir.path().join("app.harvest.db")).unwrap();
    let keys = store.dead_letter_keys().unwrap();
    assert_eq!(keys.len(), 1, "{keys:?}");
    assert_eq!(runs_of(&client, "order_flow").await.len(), 1);
}

/// A signal-with-start binding signals one run, and dedupes a redelivery.
#[tokio::test(flavor = "multi_thread")]
async fn a_connector_signal_with_start_dedupes_a_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(MockSource::new("approvals"));
    let binding =
        SourceBinding::signals_with_start("approvals", "approvals", "approval_flow", "approval")
            .map_raw(|ctx: &MessageCtx| {
                let payload: Value = serde_json::from_slice(&ctx.raw_body)
                    .map_err(|e| MappingError::Deserialize(e.to_string()))?;
                Ok(MappedMessage {
                    workflow_id: WorkflowId::new("approval-1"),
                    payload,
                })
            });
    let client = TestApp::new()
        .config(super::config(dir.path()))
        .profile("dev")
        .plugin(base().connector(binding, Arc::clone(&source) as _))
        .build();

    for _ in 0..2 {
        source.push_kafka(0, 7, br#"{"decision":"go"}"#);
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while source.acked().len() < 2 {
        assert!(Instant::now() < deadline, "the messages are never acked");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let runs = eventually(
        &client,
        "/harvest/workflows?workflow_name=approval_flow",
        |body| {
            body.as_array()
                .is_some_and(|runs| runs.len() == 1 && runs[0]["state"] == "COMPLETED")
        },
    )
    .await;
    let exec = runs[0]["id"].as_str().unwrap().to_owned();
    let history: Value = client
        .get(&format!("/harvest/workflows/{exec}/history"))
        .send()
        .await
        .json();
    let signals = history.to_string().matches("SignalReceived").count();
    assert_eq!(signals, 1, "one signal for two deliveries: {history}");
}

/// A `Starts` binding in `BrokerCoordinates` mode, the default, needs a
/// start-idempotency table that SQLite does not have. The build says so.
#[test]
fn a_coordinate_dedupe_start_binding_fails_the_build() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(MockSource::new("orders"));
    let binding = SourceBinding::starts("orders", "orders", "order_flow").map_raw(order_mapper);
    let message = build_panic(base().connector(binding, source as _), dir.path());
    assert!(message.contains("BrokerCoordinates"), "{message}");
    assert!(message.contains("WorkflowId"), "{message}");
}

// ── MCP tools ────────────────────────────────────────────────────────────────

fn mcp_app(dir: &Path) -> TestClient {
    TestApp::new()
        .config(super::config(dir))
        .profile("dev")
        .plugin(base().mcp_tools().mcp_tasks())
        .build()
}

const TOOLS: &str = "/harvest/mcp/workflows";

/// `start_`, `status`, `signal_`, `update_` and `watch` all reach the
/// SQLite file, with the Postgres bodies.
#[tokio::test(flavor = "multi_thread")]
async fn the_mcp_tools_drive_a_run_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let client = mcp_app(dir.path());

    let started = client
        .post(&format!("{TOOLS}/approval_flow/start"))
        .json(&json!("req-1"))
        .send()
        .await;
    started.assert_status(201);
    let exec = started.json::<Value>()["execution_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let status = eventually(
        &client,
        &format!("{TOOLS}/approval_flow/{exec}/status"),
        |body| body["state"] == "RUNNING",
    )
    .await;
    assert_eq!(status["is_terminal"], false);
    assert_eq!(status["workflow_name"], "approval_flow");

    // Another workflow's tool cannot read this run.
    client
        .get(&format!("{TOOLS}/order_flow/{exec}/status"))
        .send()
        .await
        .assert_status(404);

    let updated = client
        .post(&format!("{TOOLS}/approval_flow/{exec}/update/bump"))
        .json(&json!({ "by": 2 }))
        .send()
        .await;
    updated.assert_ok();
    assert_eq!(updated.json::<Value>()["output"], 3);

    let signalled = client
        .post(&format!("{TOOLS}/approval_flow/{exec}/signal/approval"))
        .json(&json!({ "decision": "ok" }))
        .send()
        .await;
    signalled.assert_status(202);
    assert_eq!(signalled.json::<Value>()["signal_delivered"], true);

    let status = eventually(
        &client,
        &format!("{TOOLS}/approval_flow/{exec}/status"),
        |body| body["state"] == "COMPLETED",
    )
    .await;
    assert_eq!(status["output"], "req-1:ok");

    let watch = client
        .get(&format!("{TOOLS}/approval_flow/{exec}/watch"))
        .send()
        .await;
    watch.assert_ok();
    let text = String::from_utf8_lossy(&watch.body).into_owned();
    assert!(text.contains("event: result"), "{text}");
    assert!(text.contains("COMPLETED"), "{text}");
}

// ── MCP tasks ────────────────────────────────────────────────────────────────

const TASKS: &str = "/harvest/mcp/tasks";

fn declared() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {
            "extensions": {TASKS_EXTENSION: {}},
            "elicitation": {},
        },
    })
}

async fn rpc(client: &TestClient, method: &str, params: Value) -> Value {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let name_key = if method == "tools/call" {
        "name"
    } else {
        "taskId"
    };
    let mut request = client
        .post(TASKS)
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    if let Some(name) = body["params"][name_key].as_str() {
        request = request.header("mcp-name", name);
    }
    let out: Value = request.json(&body).send().await.json();
    assert!(out.get("error").is_none(), "{method}: {out}");
    out["result"].clone()
}

async fn get_task(client: &TestClient, task_id: &str) -> Value {
    rpc(
        client,
        "tasks/get",
        json!({ "taskId": task_id, "_meta": declared() }),
    )
    .await
}

async fn task_until(client: &TestClient, task_id: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let task = get_task(client, task_id).await;
        if done(&task) {
            return task;
        }
        assert!(Instant::now() < deadline, "task never reached it: {task}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A task starts through the handle, asks for its signal, takes the answer
/// and completes. A second task is cancelled.
#[tokio::test(flavor = "multi_thread")]
async fn an_mcp_task_asks_for_input_and_completes_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let client = mcp_app(dir.path());

    let created = rpc(
        &client,
        "tools/call",
        json!({
            "name": "start_approval_flow",
            "arguments": { "body": "req-7" },
            "_meta": declared(),
        }),
    )
    .await;
    assert_eq!(created["resultType"], "task", "{created}");
    let task_id = created["taskId"].as_str().unwrap().to_owned();

    let waiting = task_until(&client, &task_id, |task| task["status"] == "input_required").await;
    let requests = waiting["inputRequests"].as_object().expect("inputRequests");
    assert_eq!(requests.len(), 1, "{waiting}");
    let key = requests.keys().next().unwrap().clone();

    let answer = json!({ PAYLOAD_FIELD: json!({ "decision": "yes" }).to_string() });
    rpc(
        &client,
        "tasks/update",
        json!({
            "taskId": task_id,
            "inputResponses": { key: { "action": "accept", "content": answer } },
            "_meta": declared(),
        }),
    )
    .await;
    let done = task_until(&client, &task_id, |task| task["status"] == "completed").await;
    assert!(done.to_string().contains("req-7"), "{done}");

    let second = rpc(
        &client,
        "tools/call",
        json!({
            "name": "start_approval_flow",
            "arguments": { "body": "req-8" },
            "_meta": declared(),
        }),
    )
    .await;
    let second_id = second["taskId"].as_str().unwrap().to_owned();
    rpc(
        &client,
        "tasks/cancel",
        json!({ "taskId": second_id, "_meta": declared() }),
    )
    .await;
    task_until(&client, &second_id, |task| task["status"] == "cancelled").await;
}
