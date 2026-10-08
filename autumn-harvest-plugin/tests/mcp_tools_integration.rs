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
//! Requires Docker. CI runs this suite from a `linux` manifest row (issue #1959).
//! The tests that wait for an update handler stay ignored until a fix for
//! issue #2035 lands. The engine admits a declarative update but never runs it.
//! Each test uses a multi-thread runtime. `TestApp::plugin` blocks on plugin
//! startup, and that deadlocks a current-thread runtime.

#![cfg(feature = "mcp")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_web::test::{TestApp, TestClient, TestResponse};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
    // A durable step after the signal keeps the run open for 1 s. The watch
    // then reads it as RUNNING and emits a second progress frame.
    ctx.timer("finalize", 1).await.map_err(|e| e.to_string())?;
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

fn validate_priority(input: &Value) -> Result<(), String> {
    let level = input.get("level").and_then(Value::as_str).unwrap_or("");
    if level.is_empty() {
        return Err("level must not be empty".to_string());
    }
    Ok(())
}

/// Validator-bearing mcp update — used to prove `{wf}_update_{name}` rejects
/// an invalid payload at admission time instead of durably admitting it and
/// letting it run/fail inside the workflow (code-review fix, PR #908).
#[update(
    workflow = "agent_approval_flow",
    validator = validate_priority,
    mcp
)]
async fn set_priority_validated(
    _ctx: &WorkflowContext,
    req: PriorityRequest,
) -> Result<String, String> {
    Ok(format!("validated priority set to {}", req.level))
}

/// Continues itself exactly once, then completes — used to prove
/// `{wf}_status`/`{wf}_watch` resolve the `ContinuedAsNew` chain to the
/// successor's real outcome instead of surfacing the sealed predecessor's
/// dead-end sentinel (code-review fix for issue #597).
#[workflow(mcp, description = "Continues itself once, then completes")]
async fn agent_relay_flow(ctx: &WorkflowContext, hop: u32) -> Result<String, String> {
    if hop == 0 {
        ctx.continue_as_new(serde_json::json!(1))
            .await
            .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new does not resolve while the execution is active");
    }
    Ok(format!("relay complete at hop {hop}"))
}

/// Continues itself once, then parks on a signal — used to prove
/// `signal_{wf}`/`{wf}_update_{name}` resolve the `ContinuedAsNew` chain to
/// the live successor instead of delegating with the sealed predecessor's
/// id (code-review fix, PR #908).
#[workflow(mcp, description = "Continues itself once, then waits for a signal")]
async fn agent_relay_signal_flow(ctx: &WorkflowContext, hop: u32) -> Result<String, String> {
    if hop == 0 {
        ctx.continue_as_new(serde_json::json!(1))
            .await
            .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new does not resolve while the execution is active");
    }
    let payload = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    let value = payload
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or("none")
        .to_string();
    Ok(format!("relay signalled with {value}"))
}

#[update(workflow = "agent_relay_signal_flow", mcp)]
async fn ping_relay(_ctx: &WorkflowContext, _req: Value) -> Result<String, String> {
    Ok("pong".to_string())
}

/// Sleeps 2 s. A concurrent second DAG trigger then sees the first run still
/// `RUNNING`. This exercises `max_active_runs` without a race against an
/// instant completion.
#[activity]
async fn dag_mcp_slow_task(_ctx: &ActivityContext) -> Result<(), String> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    Ok(())
}

/// A unified DAG opted into MCP exposure (issue #601 follow-up). No
/// `schedule` attribute, so nothing fires it automatically -- only the
/// generated `start_agent_mcp_dag` MCP tool (routed through the real DAG
/// trigger contract) fires it. `max_active_runs` defaults to 1, which is
/// exactly what the test below exercises.
#[cfg(feature = "unified-dag-execution")]
#[dag(mcp)]
fn agent_mcp_dag(dag: &mut DagBuilder) {
    let _ = dag.activity(dag_mcp_slow_task);
}

// ── Harness ───────────────────────────────────────────────────────────────────

fn harvest_plugin() -> HarvestPlugin {
    let plugin = HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_agent_approval_flow()
                .with_input_schema_fn(approval_input_schema),
            __autumn_workflow_info_agent_other_flow(),
            __autumn_workflow_info_agent_relay_flow(),
            __autumn_workflow_info_agent_relay_signal_flow(),
        ])
        .updates(updates![set_priority, set_priority_validated, ping_relay])
        .worker(WorkerConfig::default())
        .api("/api/harvest")
        .mcp_tools()
        // Issue #1802: set the opt-out. This test exercises the tools, not auth.
        .allow_unauthenticated_mutations();
    #[cfg(feature = "unified-dag-execution")]
    let plugin = plugin
        .activities(activities![dag_mcp_slow_task])
        .dags(dags![agent_mcp_dag]);
    plugin
}

async fn build_app(db: &TestPg) -> TestClient {
    TestApp::new()
        .plugin(harvest_plugin())
        .with_db(db.pool.clone())
        .mount_mcp("/mcp")
        .build()
}

/// A migrated Postgres 16 container for one test.
///
/// `TestDb` starts Postgres 11. There, the worker claim query fails on
/// `MATERIALIZED`, so no task runs (issue #1959). Harvest needs Postgres 12+.
///
/// The Harvest schema comes from `test_init_sql()`, not from `run_pending`.
/// Six Harvest migrations share a version with a framework migration.
/// A second `run_pending` call skips them, and the schema has no
/// `harvest_schedules.paused_at`. `plugin_migrations` resolves such
/// collisions in production.
struct TestPg {
    _container: ContainerAsync<Postgres>,
    pool: Pool<AsyncPgConnection>,
}

async fn setup_db() -> TestPg {
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    unsafe {
        std::env::set_var("AUTUMN_DATABASE__URL", &url);
    }
    autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool = Pool::builder(manager)
        .max_size(5)
        .build()
        .expect("failed to build pool");
    TestPg {
        _container: container,
        pool,
    }
}

async fn rpc(client: &TestClient, body: Value) -> Value {
    let resp = client.post("/mcp").json(&body).send().await;
    resp.assert_ok();
    resp.json::<Value>()
}

/// `tools/call` returning the inner handler body parsed as JSON.
///
/// A JSON-RPC `error` (an unknown tool or a missing argument) fails the test.
/// It never counts as a tool error, so a negative test cannot pass on it.
async fn call_tool(client: &TestClient, name: &str, arguments: Value) -> (bool, Value) {
    let out = rpc(
        client,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }),
    )
    .await;
    assert!(
        out.get("error").is_none(),
        "{name}: JSON-RPC error, not a tool result: {}",
        out["error"]
    );
    let is_error = out["result"]["isError"].as_bool().unwrap_or(false);
    let text = out["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (is_error, body)
}

/// Poll the status tool until `pred` holds, for at most 150 polls.
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

/// The issue #597 end-to-end example, through the generated MCP tools only.
/// An agent starts a multi-step workflow and sees >= 2 progress updates over
/// streaming MCP. It sends a signal that unblocks a `wait_for_signal`, then
/// reads a terminal status. `mcp_update_tools_run_the_handler_synchronously`
/// covers the update step (issue #2035).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    // Subscribe to streaming progress *before* the signal so the stream sees
    // the remaining transitions live (initial frame + signal-driven events),
    // and deliver the signal concurrently (the watch response body only
    // completes once the run reaches a terminal state).
    let listeners_before = count_listeners(&db).await;
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
        // Wait until the watch holds its LISTEN connection.
        for _ in 0..100 {
            if count_listeners(&db).await > listeners_before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

/// Durability across a daemon restart. App #1 starts the workflow through an
/// MCP tool. App #2, a new app on the same database, then finds, steers and
/// completes the run. `TestApp` does not run shutdown hooks, so app #1's
/// worker can still run in this process. The test proves that app #2 needs
/// only Postgres. It does not prove that app #1's worker stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        // The client drops here. App #1's HTTP surface is gone.
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

/// Code-review regression test (issue #597): `{wf}_status` and `{wf}_watch`
/// must resolve a `ContinuedAsNew` chain to the successor's real outcome —
/// before the fix, both surfaced the sealed predecessor's dead-end sentinel
/// (`state: CONTINUED_AS_NEW`, `output: null`, `error: null`) for any run
/// that continued itself, exactly like `agent_relay_flow` does once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_and_watch_follow_the_continue_as_new_chain_to_the_real_result() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let (is_error, started) =
        call_tool(&client, "start_agent_relay_flow", json!({"body": 0})).await;
    assert!(!is_error, "start must succeed, got: {started}");
    let handle = started["execution_id"]
        .as_str()
        .expect("start returns the execution_id handle")
        .to_string();

    // status_ queried on the ORIGINAL (predecessor) handle must resolve
    // through the successor to the real terminal outcome, not the sealed
    // predecessor's null output/error.
    let status = wait_for_status(&client, "agent_relay_flow_status", &handle, |s| {
        s["is_terminal"] == json!(true)
    })
    .await;
    assert_eq!(
        status["state"], "COMPLETED",
        "status must report the successor's real state, not CONTINUED_AS_NEW; got: {status}"
    );
    assert_eq!(status["output"], json!("relay complete at hop 1"));
    assert!(status["error"].is_null());

    // watch_ on the same (now-terminal-by-chain) handle must also resolve
    // through the chain rather than emitting a dead-end CONTINUED_AS_NEW
    // result frame.
    let resp = post_sse(
        &client,
        json!({
            "jsonrpc": "2.0", "id": 88, "method": "tools/call",
            "params": {
                "name": "agent_relay_flow_watch",
                "arguments": {"handle": handle}
            }
        }),
    )
    .await;
    resp.assert_ok();
    let messages = sse_messages(&resp.text());
    // The terminal frame becomes the id-correlated `tools/call` result. Its
    // first content item carries the `{state, output, error}` JSON as text.
    let result: Value = messages
        .iter()
        .find(|m| m["id"] == 88)
        .and_then(|m| m["result"]["content"][0]["text"].as_str())
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or_else(|| panic!("a terminal result frame must be emitted; got: {messages:?}"));
    assert_eq!(
        result["state"], "COMPLETED",
        "watch's terminal frame must report the successor's real state, not \
         CONTINUED_AS_NEW; got: {result}"
    );
    assert_eq!(result["output"], json!("relay complete at hop 1"));
    assert!(result["error"].is_null());
}

/// Starts `agent_relay_signal_flow` and waits until its successor is live.
///
/// Status follows the chain, so a new `execution_id` proves the successor
/// runs. A bare `RUNNING` check is not enough: the predecessor is `RUNNING`
/// before it continues, and the successor never sees a call sent then.
async fn start_relay_and_wait_for_successor(client: &TestClient) -> String {
    let (is_error, started) =
        call_tool(client, "start_agent_relay_signal_flow", json!({"body": 0})).await;
    assert!(!is_error, "start must succeed, got: {started}");
    let handle = started["execution_id"]
        .as_str()
        .expect("start returns the execution_id handle")
        .to_string();
    wait_for_status(client, "agent_relay_signal_flow_status", &handle, |s| {
        s["state"] == "RUNNING" && s["execution_id"] != json!(handle)
    })
    .await;
    handle
}

/// Code-review regression test (issue #597, PR #908): `signal_{wf}` must
/// resolve a `ContinuedAsNew` chain to the live successor and delegate with
/// *its* execution id. Before the fix, it delegated with the sealed
/// predecessor id, which signal admission rejects as terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_follows_the_continue_as_new_chain_to_the_live_successor() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;
    let handle = start_relay_and_wait_for_successor(&client).await;

    // The signal tool, called with the ORIGINAL handle, must unblock the
    // live successor's wait_for_signal.
    let (is_error, signal_result) = call_tool(
        &client,
        "signal_agent_relay_signal_flow",
        json!({"handle": handle, "signal_name": "go", "body": {"value": "hello"}}),
    )
    .await;
    assert!(
        !is_error,
        "signal against the original handle must reach the live successor, got: {signal_result}"
    );

    let status = wait_for_status(&client, "agent_relay_signal_flow_status", &handle, |s| {
        s["is_terminal"] == json!(true)
    })
    .await;
    assert_eq!(status["state"], "COMPLETED");
    assert_eq!(status["output"], json!("relay signalled with hello"));
}

/// Code-review regression test (issue #597, PR #908): `{wf}_update_{name}`
/// must resolve a `ContinuedAsNew` chain to the live successor, as
/// `signal_{wf}` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "issue #2035: the engine never runs an admitted declarative update"]
async fn update_follows_the_continue_as_new_chain_to_the_live_successor() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;
    let handle = start_relay_and_wait_for_successor(&client).await;

    let (is_error, update_result) = call_tool(
        &client,
        "agent_relay_signal_flow_update_ping_relay",
        json!({"handle": handle, "body": {}}),
    )
    .await;
    assert!(
        !is_error,
        "update against the original handle must reach the live successor, got: {update_result}"
    );
    assert_eq!(update_result["output"], json!("pong"));
}

/// The mcp update tools run the handler and return its output in the same
/// call. This is request/response, unlike the async signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "issue #2035: the engine never runs an admitted declarative update"]
async fn mcp_update_tools_run_the_handler_synchronously() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let (is_error, started) = call_tool(
        &client,
        "start_agent_approval_flow",
        json!({"body": "upd-1"}),
    )
    .await;
    assert!(!is_error, "start must succeed, got: {started}");
    let handle = started["execution_id"].as_str().unwrap().to_string();
    wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["current_details"]
            .as_str()
            .is_some_and(|d| d.contains("awaiting approval"))
    })
    .await;

    for (tool, output) in [
        ("set_priority", "priority set to high"),
        ("set_priority_validated", "validated priority set to high"),
    ] {
        let (is_error, result) = call_tool(
            &client,
            &format!("agent_approval_flow_update_{tool}"),
            json!({"handle": handle, "body": {"level": "high"}}),
        )
        .await;
        assert!(!is_error, "{tool} must succeed, got: {result}");
        assert_eq!(result["output"], json!(output));
    }
}

/// Code-review regression test (issue #597, PR #908): an mcp update declared
/// with `validator = ...` must reject an invalid payload *before* admission
/// -- `{wf}_update_{name}` previously delegated straight to `admit_update`,
/// which never consulted the registered validator, so an invalid payload
/// became durable history (`UpdateAdmitted`) instead of being rejected at
/// the edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_update_tool_rejects_invalid_payload_via_registered_validator() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let (is_error, started) = call_tool(
        &client,
        "start_agent_approval_flow",
        json!({"body": "validator-test"}),
    )
    .await;
    assert!(!is_error, "start must succeed, got: {started}");
    let handle = started["execution_id"].as_str().unwrap().to_string();

    wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["state"] == "RUNNING"
    })
    .await;

    // Invalid payload (empty level): the validator must reject it before any
    // durable admission, not let it run/fail inside the workflow.
    let (is_error, rejected) = call_tool(
        &client,
        "agent_approval_flow_update_set_priority_validated",
        json!({"handle": handle, "body": {"level": ""}}),
    )
    .await;
    assert!(
        is_error,
        "an empty level must be rejected by the registered validator, got: {rejected}"
    );

    // The rejected payload must not reach durable history.
    assert_eq!(
        count_update_admitted(&db, &handle).await,
        0,
        "a rejected update must not write `UpdateAdmitted`"
    );
    let status = wait_for_status(&client, "agent_approval_flow_status", &handle, |s| {
        s["state"] == "RUNNING"
    })
    .await;
    assert_eq!(status["is_terminal"], json!(false));
}

/// Counts the sessions whose last statement is a `LISTEN`.
async fn count_listeners(db: &TestPg) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = db.pool.get().await.expect("pool connection");
    diesel::sql_query("SELECT COUNT(*) AS n FROM pg_stat_activity WHERE query LIKE 'LISTEN %'")
        .get_result::<Count>(&mut conn)
        .await
        .expect("count LISTEN sessions")
        .n
}

/// Counts the `UpdateAdmitted` events of one execution.
async fn count_update_admitted(db: &TestPg, execution_id: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let id = uuid::Uuid::parse_str(execution_id).expect("execution id is a uuid");
    let mut conn = db.pool.get().await.expect("pool connection");
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_events \
         WHERE workflow_exec_id = $1 AND event_data->>'type' = 'UpdateAdmitted'",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count UpdateAdmitted events")
    .n
}

/// Code-review regression test (issue #597, PR #908): `start_{wf}` must still
/// reject input that violates the workflow's published issue #373 schema,
/// with a real runtime installed. `start_tool` no longer duplicates this
/// check itself (removed as a redundant, closure-captured-schema copy of the
/// check `start_workflow` already performs against the live registry); the
/// guarantee now depends entirely on `start_workflow`'s own validation, which
/// only runs once `api_state.runtime()` resolves -- unreachable in the no-DB
/// `tests/mcp_tools_http_tests.rs` harness. This test is that guarantee's
/// only remaining coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_start_tool_rejects_input_that_violates_the_published_schema() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    // agent_approval_flow's published schema requires a string body; send an
    // object instead.
    let (is_error, rejected) = call_tool(
        &client,
        "start_agent_approval_flow",
        json!({"body": {"not": "a string"}}),
    )
    .await;
    assert!(
        is_error,
        "schema-violating start input must be rejected, got: {rejected}"
    );
    let detail = rejected["detail"]
        .as_str()
        .or_else(|| rejected["error"].as_str())
        .unwrap_or_default();
    assert!(
        detail.contains("validation") || rejected.to_string().contains("violations"),
        "rejection must reference the issue #373 schema-validation contract, got: {rejected}"
    );
}

/// A unified DAG's `start_{dag}` MCP tool must route through the real DAG
/// trigger contract (`trigger_dag_run` -> `trigger_unified_dag`), not the
/// generic `start_workflow` path -- proven end to end by: (1) the first start
/// actually creates and runs a DAG execution that reaches `COMPLETED`, and
/// (2) a second start fired while the first is still `RUNNING` is rejected by
/// the DAG's `max_active_runs` admission gate (default 1), which
/// `start_workflow` has no notion of and would have silently admitted.
#[cfg(feature = "unified-dag-execution")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dag_start_tool_triggers_a_real_dag_run_and_enforces_max_active_runs() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let (is_error, first) = call_tool(&client, "start_agent_mcp_dag", json!({"body": null})).await;
    assert!(!is_error, "first DAG start must succeed, got: {first}");
    let handle = first["execution_id"]
        .as_str()
        .expect("execution_id")
        .to_string();

    wait_for_status(&client, "agent_mcp_dag_status", &handle, |s| {
        s["state"] == "RUNNING"
    })
    .await;

    // The first run's activity sleeps 2 s, so this second trigger sees it
    // still RUNNING. `max_active_runs` (default 1) must reject the trigger.
    let (is_error, second) = call_tool(&client, "start_agent_mcp_dag", json!({"body": null})).await;
    assert!(
        is_error,
        "a concurrent DAG start must be rejected by max_active_runs, got: {second}"
    );
    let detail = second["detail"]
        .as_str()
        .or_else(|| second["error"].as_str())
        .unwrap_or_default();
    assert!(
        detail.contains("max_active_runs") || second.to_string().contains("max_active_runs"),
        "rejection must cite max_active_runs, got: {second}"
    );

    let status = wait_for_status(&client, "agent_mcp_dag_status", &handle, |s| {
        s["state"] == "COMPLETED"
    })
    .await;
    assert_eq!(status["state"], "COMPLETED");
}

/// A unified DAG never consumes signals (issue #601 follow-up): its tool set
/// must not include `signal_{dag}` at all, so calling it is an unknown-tool
/// error from autumn-web's own MCP dispatch, not a delegated handler call.
#[cfg(feature = "unified-dag-execution")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dag_tool_set_has_no_signal_tool() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let out = rpc(
        &client,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    )
    .await;
    let names: Vec<&str> = out["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"start_agent_mcp_dag"));
    assert!(names.contains(&"agent_mcp_dag_status"));
    assert!(names.contains(&"agent_mcp_dag_watch"));
    assert!(
        !names.contains(&"signal_agent_mcp_dag"),
        "a DAG must never surface a signal tool, got: {names:?}"
    );
}
