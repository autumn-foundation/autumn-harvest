//! End-to-end MCP tool integration tests (issue #597) — testcontainers.
//!
//! Drives the full agent flow against a real Postgres and the full
//! plugin-wired app: `tools/call start_*` returns a durable handle
//! immediately; `*_status` reflects the run; an mcp update executes
//! synchronously; `*_watch` streams `notifications/progress` frames over
//! streaming MCP; `signal_*` unblocks a `wait_for_signal`; and a workflow
//! started via an MCP tool survives a simulated daemon restart (the first
//! app is torn down and a second app on the same database finishes the run).
//!
//! Requires Docker. In sandboxes without a Docker daemon these tests are
//! compile-checked with `cargo test --no-run` (repo precedent: #543/#544).

#![cfg(feature = "mcp")]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient, TestDb, TestResponse};
use serde_json::{Value, json};

// ── Fixtures ──────────────────────────────────────────────────────────────────

fn approval_input_schema() -> Value {
    json!({ "type": "string", "description": "request id" })
}

/// Multi-step agent-driven workflow: publishes progress breadcrumbs, parks on
/// an approval signal, and finishes with a typed output.
#[workflow(mcp, description = "Agent-driven approval flow")]
async fn agent_approval_flow(ctx: &WorkflowContext, request_id: String) -> Result<String, String> {
    ctx.set_current_details("step 1/2: awaiting approval signal");
    let approval = ctx
        .wait_for_signal("approval")
        .await
        .map_err(|e| e.to_string())?;
    ctx.set_current_details("step 2/2: finalizing");
    let decision = approval
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Ok(format!("{request_id}:{decision}"))
}

/// Second mcp workflow used to prove handles cannot be driven through another
/// workflow's tools.
#[workflow(mcp)]
async fn agent_other_flow(ctx: &WorkflowContext, _input: String) -> Result<(), String> {
    let _ = ctx.wait_for_signal("never").await;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PriorityRequest {
    level: String,
}

#[update(workflow = "agent_approval_flow", mcp)]
async fn set_priority(_ctx: &WorkflowContext, req: PriorityRequest) -> Result<String, String> {
    Ok(format!("priority set to {}", req.level))
}

// ── Harness ───────────────────────────────────────────────────────────────────

fn harvest_plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_agent_approval_flow()
                .with_input_schema_fn(approval_input_schema),
            __autumn_workflow_info_agent_other_flow(),
        ])
        .updates(updates![set_priority])
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .mcp_tools()
}

async fn build_app(db: &TestDb) -> TestClient {
    TestApp::new()
        .plugin(harvest_plugin())
        .with_db(db.pool())
        .mount_mcp("/mcp")
        .build()
}

async fn setup_db() -> &'static TestDb {
    let db = TestDb::shared().await;
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", db.url());
    }
    autumn_web::migrate::run_pending(db.url(), autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    autumn_web::migrate::run_pending(db.url(), autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    db
}

async fn rpc(client: &TestClient, body: Value) -> Value {
    let resp = client.post("/mcp").json(&body).send().await;
    resp.assert_ok();
    resp.json::<Value>()
}

/// `tools/call` returning the inner handler body parsed as JSON.
async fn call_tool(client: &TestClient, name: &str, arguments: Value) -> (bool, Value) {
    let out = rpc(
        client,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }),
    )
    .await;
    let is_error = out["result"]["isError"].as_bool().unwrap_or(false);
    let text = out["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (is_error, body)
}

/// Poll the status tool until `pred` holds or ~15 s elapse.
async fn wait_for_status(
    client: &TestClient,
    tool: &str,
    handle: &str,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..150 {
        let (is_error, body) = call_tool(client, tool, json!({"handle": handle})).await;
        if !is_error && pred(&body) {
            return body;
        }
        last = body;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("status condition not reached within 15s; last status: {last}");
}

fn sse_messages(body: &str) -> Vec<Value> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .collect()
}

async fn post_sse(client: &TestClient, body: Value) -> TestResponse {
    client
        .post("/mcp")
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// The issue #597 end-to-end example: an agent starts a multi-step workflow,
/// observes >= 2 progress updates over streaming MCP, executes a synchronous
/// update, sends a signal that unblocks a `wait_for_signal`, and reads a
/// terminal status — all through the generated MCP tools.
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn agent_drives_a_durable_workflow_via_mcp_tools() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    // start_ returns the durable handle immediately, without blocking to
    // completion (the workflow is parked on a signal).
    let (is_error, started) = call_tool(
        &client,
        "start_agent_approval_flow",
        json!({"body": "req-42"}),
    )
    .await;
    assert!(!is_error, "start must succeed, got: {started}");
    let handle = started["execution_id"]
        .as_str()
        .expect("start returns the execution_id handle")
        .to_string();
    assert_eq!(started["workflow_name"], "agent_approval_flow");

    // status_ observes the parked run and the author-published breadcrumb.
    let status = wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["current_details"]
            .as_str()
            .is_some_and(|d| d.contains("awaiting approval"))
    })
    .await;
    assert_eq!(status["state"], "RUNNING");
    assert_eq!(status["is_terminal"], json!(false));

    // The mcp update executes synchronously (request/response, distinct from
    // the async signal below).
    let (is_error, update_result) = call_tool(
        &client,
        "agent_approval_flow_update_set_priority",
        json!({"handle": handle, "body": {"level": "high"}}),
    )
    .await;
    assert!(!is_error, "update must succeed, got: {update_result}");
    assert_eq!(update_result["output"], json!("priority set to high"));

    // Subscribe to streaming progress *before* the signal so the stream sees
    // the remaining transitions live (initial frame + signal-driven events),
    // and deliver the signal concurrently (the watch response body only
    // completes once the run reaches a terminal state).
    let watch_fut = post_sse(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 77, "method": "tools/call",
            "params": {
                "name": "agent_approval_flow_watch",
                "arguments": {"handle": handle},
                "_meta": {"progressToken": "watch-1"}
            }
        }),
    );
    let signal_fut = async {
        // Give the SSE subscription a moment to establish its LISTEN connection.
        tokio::time::sleep(Duration::from_millis(500)).await;
        call_tool(
            &client,
            "signal_agent_approval_flow",
            json!({"handle": handle, "signal_name": "approval", "body": {"decision": "approved"}}),
        )
        .await
    };
    let (watch_resp, (signal_error, ack)) = tokio::join!(watch_fut, signal_fut);
    assert!(!signal_error, "signal must be delivered, got: {ack}");

    // The watch stream ends with the terminal result; >= 2 progress
    // notifications were observed on the way (initial snapshot + at least one
    // signal/completion-driven event).
    watch_resp.assert_ok();
    let messages = sse_messages(&watch_resp.text());
    let progress: Vec<&Value> = messages
        .iter()
        .filter(|m| m["method"] == "notifications/progress")
        .collect();
    assert!(
        progress.len() >= 2,
        "agent must observe >= 2 progress updates, got {}: {messages:?}",
        progress.len()
    );
    for p in &progress {
        assert_eq!(p["params"]["progressToken"], "watch-1");
    }
    let final_msg = messages
        .iter()
        .find(|m| m["id"] == 77)
        .expect("id-correlated terminal result ends the stream");
    let final_text = final_msg["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        final_text.contains("COMPLETED"),
        "terminal watch frame carries the final state, got: {final_text}"
    );

    // Terminal status carries the workflow output.
    let status = wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["is_terminal"] == json!(true)
    })
    .await;
    assert_eq!(status["state"], "COMPLETED");
    assert_eq!(status["output"], json!("req-42:approved"));
}

/// Durability across a daemon restart: the workflow is started through an MCP
/// tool on app #1, app #1 is torn down entirely (simulated daemon stop), and
/// app #2 — a fresh process-equivalent on the same database — resumes it. The
/// agent's connection lifetime is irrelevant to the work completing.
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn workflow_started_via_mcp_survives_daemon_restart() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;

    let handle = {
        let client = build_app(&db).await;
        let (is_error, started) = call_tool(
            &client,
            "start_agent_approval_flow",
            json!({"body": "restart-1"}),
        )
        .await;
        assert!(!is_error, "start must succeed, got: {started}");
        let handle = started["execution_id"].as_str().unwrap().to_string();
        // Ensure the run is durably parked before the "daemon" goes down.
        wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
            s["current_details"]
                .as_str()
                .is_some_and(|d| d.contains("awaiting approval"))
        })
        .await;
        handle
        // client dropped here — the first app (worker included) is gone.
    };

    // "Daemon restart": a brand new app against the same database.
    let client = build_app(&db).await;

    // The run is still there, still parked — recovered purely from Postgres.
    let status = wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["state"] == json!("RUNNING")
    })
    .await;
    assert_eq!(status["is_terminal"], json!(false));

    // Steering still works after the restart; the run completes.
    let (is_error, ack) = call_tool(
        &client,
        "signal_agent_approval_flow",
        json!({"handle": handle, "signal_name": "approval", "body": {"decision": "approved"}}),
    )
    .await;
    assert!(!is_error, "signal after restart must deliver, got: {ack}");

    let status = wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["is_terminal"] == json!(true)
    })
    .await;
    assert_eq!(status["state"], "COMPLETED");
    assert_eq!(status["output"], json!("restart-1:approved"));
}

/// A handle minted by one workflow's start tool is rejected by another
/// workflow's tools with the same 404 an unknown handle gets (no oracle).
#[tokio::test]
#[ignore = "requires Docker (testcontainers)"]
async fn handles_cannot_cross_workflow_tool_boundaries() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let (is_error, started) = call_tool(
        &client,
        "start_agent_approval_flow",
        json!({"body": "cross-1"}),
    )
    .await;
    assert!(!is_error);
    let handle = started["execution_id"].as_str().unwrap();

    let (is_error, body) = call_tool(
        &client,
        "agent_other_flow_status",
        json!({"handle": handle}),
    )
    .await;
    assert!(is_error, "cross-workflow status must fail, got: {body}");

    let (is_error, body) = call_tool(
        &client,
        "signal_agent_other_flow",
        json!({"handle": handle, "signal_name": "never", "body": {}}),
    )
    .await;
    assert!(is_error, "cross-workflow signal must fail, got: {body}");
}
