//! MCP Tasks end to end (issue #2005), with testcontainers.
//!
//! Each test drives the task route of a full plugin-wired app against a real
//! Postgres. The tests prove the three acceptance criteria of the issue:
//!
//! 1. A task moves through the spec states, and each move is legal. A
//!    terminal status does not change on later polls.
//! 2. A retried task-create request starts one execution.
//! 3. An `input_required` task resumes when the client supplies input.
//!
//! Requires Docker. Each test uses a multi-thread runtime, because
//! `TestApp::plugin` blocks on plugin startup.

#![cfg(feature = "mcp")]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::mcp_tasks::{
    CLIENT_CAPABILITIES_META, PAYLOAD_FIELD, START_KEY_META, TASKS_EXTENSION, TaskStatus,
};
use autumn_web::test::{TestApp, TestClient};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const TASKS: &str = "/api/harvest/mcp/tasks";
const RETENTION: Duration = Duration::from_secs(30 * 86_400);

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// Waits for one approval, then holds the run open for 1 s.
#[workflow(mcp, description = "Waits for one approval")]
async fn task_approval_flow(ctx: &WorkflowContext, request_id: String) -> Result<String, String> {
    ctx.set_current_details("awaiting approval");
    let approval = ctx
        .wait_for_signal("approval")
        .await
        .map_err(|e| e.to_string())?;
    ctx.set_current_details("finalizing");
    // The timer keeps the run in `working` after the signal, so the test
    // sees the move from `input_required` back to `working`.
    ctx.timer("finalize", 1).await.map_err(|e| e.to_string())?;
    let decision = approval
        .get("decision")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Ok(format!("{request_id}:{decision}"))
}

/// Waits twice on the same signal name and returns both payloads. The name
/// is not ASCII, so the input key cannot ride in an HTTP header.
#[workflow(mcp)]
async fn task_two_step_flow(ctx: &WorkflowContext, _input: String) -> Result<Value, String> {
    let first = ctx
        .wait_for_signal("étape")
        .await
        .map_err(|e| e.to_string())?;
    let second = ctx
        .wait_for_signal("étape")
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!([first, second]))
}

/// Fails at once with a business error.
#[workflow(mcp)]
async fn task_failing_flow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Err("card declined".to_string())
}

/// Parks on a signal that never comes.
#[workflow(mcp)]
async fn task_parked_flow(ctx: &WorkflowContext, _input: String) -> Result<(), String> {
    let _ = ctx.wait_for_signal("never").await;
    Ok(())
}

/// The inputs that `flaky_gate` has failed once.
static FLAKY_SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Fails the first time it sees an input. An activity holds this state,
/// because a workflow body must not touch process globals.
#[activity]
async fn flaky_gate(_ctx: &ActivityContext, input: String) -> Result<(), String> {
    let first = {
        let mut seen = FLAKY_SEEN.lock().unwrap_or_else(PoisonError::into_inner);
        let first = !seen.contains(&input);
        if first {
            seen.push(input);
        }
        first
    };
    if first {
        return Err("transient".to_string());
    }
    Ok(())
}

/// Fails on the first attempt for each input. The gate has no activity
/// retry, so the workflow retry policy runs a second attempt. That attempt
/// holds the run open for 1 s and then succeeds.
#[workflow(mcp)]
async fn task_flaky_flow(ctx: &WorkflowContext, input: String) -> Result<String, String> {
    ctx.execute_activity_raw_with_opts(
        "flaky_gate",
        json!(input),
        "default",
        Some(autumn_harvest::policy::RetryPolicy::exponential(
            1,
            Duration::from_millis(100),
        )),
        None,
    )
    .await
    .map_err(|e| e.to_string())?;
    ctx.timer("settle", 1).await.map_err(|e| e.to_string())?;
    Ok("recovered".to_string())
}

/// Not an MCP workflow. Its runs are not tasks.
#[workflow]
async fn task_hidden_flow(ctx: &WorkflowContext, _input: String) -> Result<(), String> {
    let _ = ctx.wait_for_signal("never").await;
    Ok(())
}

// ── Harness ───────────────────────────────────────────────────────────────────

fn harvest_plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(vec![
            __autumn_workflow_info_task_approval_flow(),
            __autumn_workflow_info_task_two_step_flow(),
            __autumn_workflow_info_task_failing_flow(),
            __autumn_workflow_info_task_parked_flow(),
            __autumn_workflow_info_task_flaky_flow().with_retry_policy(
                autumn_harvest::policy::RetryPolicy::exponential(3, Duration::from_millis(200)),
            ),
            __autumn_workflow_info_task_hidden_flow(),
        ])
        .activities(activities![flaky_gate])
        .worker(WorkerConfig::default())
        // A long retention gives an ended task a TTL. No run is old enough to
        // be deleted during a test.
        .retention(autumn_harvest::retention::RetentionConfig::with_max_age(
            RETENTION,
        ))
        .api("/api/harvest")
        .mcp_tasks()
        // Issue #1802: set the opt-out. These tests exercise tasks, not auth.
        .allow_unauthenticated_mutations()
}

async fn build_app(db: &TestPg) -> TestClient {
    let config = autumn_web::config::AutumnConfig {
        profile: Some("test".into()),
        security: autumn_web::security::SecurityConfig {
            csrf: autumn_web::security::CsrfConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        database: autumn_web::config::DatabaseConfig {
            url: Some(db.url.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    TestApp::new()
        .config(config)
        .plugin(harvest_plugin())
        .with_db(db.pool.clone())
        .build()
}

/// A migrated Postgres 16 container for one test. See
/// `mcp_tools_integration.rs` for why the schema comes from `test_init_sql`.
struct TestPg {
    _container: ContainerAsync<Postgres>,
    url: String,
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
    autumn_web::migrate::run_pending(&url, autumn_web::migrate::FRAMEWORK_MIGRATIONS)
        .expect("failed to run framework migrations");
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
    let pool = Pool::builder(manager)
        .max_size(5)
        .build()
        .expect("failed to build pool");
    TestPg {
        _container: container,
        url,
        pool,
    }
}

/// The client capabilities of a client that takes tasks and elicitations.
fn declared() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {
            "extensions": {TASKS_EXTENSION: {}},
            "elicitation": {},
        },
    })
}

async fn rpc_with(client: &TestClient, body: Value, idempotency_key: Option<&str>) -> Value {
    let mut request = client.post(TASKS);
    if let Some(key) = idempotency_key {
        request = request.header("idempotency-key", key);
    }
    // A 2026-07-28 body needs its Streamable HTTP headers.
    let params = &body["params"];
    if let Some(version) = params
        .pointer("/_meta/io.modelcontextprotocol~1protocolVersion")
        .and_then(Value::as_str)
    {
        let method = body["method"].as_str().unwrap_or_default();
        request = request
            .header("mcp-protocol-version", version)
            .header("mcp-method", method);
        let name = if method == "tools/call" {
            "name"
        } else {
            "taskId"
        };
        if let Some(name) = params.get(name).and_then(Value::as_str) {
            request = request.header("mcp-name", name);
        }
    }
    let resp = request.json(&body).send().await;
    let out = resp.json::<Value>();
    // A 2026-07-28 error can carry an HTTP error status.
    if out.get("error").is_none() {
        resp.assert_ok();
    }
    out
}

async fn rpc(client: &TestClient, method: &str, params: Value) -> Value {
    let out = rpc_with(
        client,
        json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        None,
    )
    .await;
    assert!(out.get("error").is_none(), "{method}: {out}");
    out["result"].clone()
}

fn tool_call(name: &str, body: &Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": name, "arguments": {"body": body}, "_meta": declared()}
    })
}

/// Create a task and return its `CreateTaskResult`.
async fn create_task(client: &TestClient, name: &str, body: Value) -> Value {
    let out = rpc_with(client, tool_call(name, &body), None).await;
    assert!(out.get("error").is_none(), "{out}");
    out["result"].clone()
}

async fn get_task(client: &TestClient, task_id: &str) -> Value {
    rpc(
        client,
        "tasks/get",
        json!({"taskId": task_id, "_meta": declared()}),
    )
    .await
}

fn status_of(task: &Value) -> TaskStatus {
    match task["status"].as_str() {
        Some("working") => TaskStatus::Working,
        Some("input_required") => TaskStatus::InputRequired,
        Some("completed") => TaskStatus::Completed,
        Some("failed") => TaskStatus::Failed,
        Some("cancelled") => TaskStatus::Cancelled,
        other => panic!("not a task status: {other:?} in {task}"),
    }
}

/// Polls `tasks/get` until `pred` holds. Each new status goes to `seen`.
async fn poll_until(
    client: &TestClient,
    task_id: &str,
    seen: &mut Vec<TaskStatus>,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..300 {
        let task = get_task(client, task_id).await;
        let status = status_of(&task);
        if seen.last() != Some(&status) {
            seen.push(status);
        }
        if pred(&task) {
            return task;
        }
        last = task;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("task condition not reached in 15 s; last task: {last}");
}

fn only_key(task: &Value) -> String {
    let requests = task["inputRequests"].as_object().expect("inputRequests");
    assert_eq!(requests.len(), 1, "{task}");
    requests.keys().next().unwrap().clone()
}

fn update_call(task_id: &str, key: &str, action: &str, content: &Value) -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "tasks/update",
        "params": {
            "taskId": task_id,
            "inputResponses": {key: {"action": action, "content": content}},
            "_meta": declared(),
        }
    })
}

/// The form content of an answer: the payload as JSON text.
fn form(payload: &Value) -> Value {
    json!({PAYLOAD_FIELD: payload.to_string()})
}

async fn answer(client: &TestClient, task_id: &str, key: &str, content: Value) -> Value {
    let out = rpc_with(client, update_call(task_id, key, "accept", &content), None).await;
    assert!(out.get("error").is_none(), "tasks/update: {out}");
    out["result"].clone()
}

/// After a terminal status, a few more polls see the same status and result.
async fn assert_settled(client: &TestClient, task_id: &str, done: &Value) {
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let again = get_task(client, task_id).await;
        assert_eq!(again["status"], done["status"], "a terminal status moved");
        assert_eq!(again["result"], done["result"], "a terminal result moved");
    }
}

async fn count_runs(db: &TestPg, workflow: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = db.pool.get().await.expect("pool connection");
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow)
    .get_result::<Count>(&mut conn)
    .await
    .expect("count executions")
    .n
}

fn assert_legal(seen: &[TaskStatus]) {
    for pair in seen.windows(2) {
        assert!(
            pair[0].can_transition_to(pair[1]),
            "illegal move {:?} -> {:?} in {seen:?}",
            pair[0],
            pair[1]
        );
    }
}

// ── AC 1: spec state transitions ──────────────────────────────────────────────

/// A task moves `working` -> `input_required` -> `working` -> `completed`.
/// Every observed move is legal under the spec state diagram, and the
/// terminal status does not move on later polls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_moves_through_the_spec_states() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_approval_flow", json!("r1")).await;
    assert_eq!(created["resultType"], "task", "{created}");
    let task_id = created["taskId"].as_str().expect("taskId").to_string();
    let mut seen = vec![status_of(&created)];
    assert!(!seen[0].is_terminal(), "{created}");
    for absent in ["result", "error", "inputRequests"] {
        assert!(created.get(absent).is_none(), "{absent}: {created}");
    }

    let waiting = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    assert_eq!(waiting["resultType"], "complete");
    assert_eq!(waiting["createdAt"], created["createdAt"]);
    let key = only_key(&waiting);
    assert!(key.contains(":signal:approval:"), "{key}");
    let request = &waiting["inputRequests"][&key];
    assert_eq!(request["method"], "elicitation/create");

    // The key is stable while the run waits, so a client asks once.
    let again = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    assert_eq!(only_key(&again), key);

    // A form client sends the payload as JSON text in the one field.
    let ack = answer(
        &client,
        &task_id,
        &key,
        json!({PAYLOAD_FIELD: "{\"decision\": \"approve\"}"}),
    )
    .await;
    assert_eq!(ack, json!({"resultType": "complete"}));

    // After the ack, the run never asks again. It works on, then completes.
    let mut after = Vec::new();
    let done = poll_until(&client, &task_id, &mut after, |t| {
        status_of(t).is_terminal()
    })
    .await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"]["isError"], false);
    assert_eq!(done["result"]["content"][0]["text"], "\"r1:approve\"");
    assert_eq!(done["createdAt"], created["createdAt"]);
    assert_settled(&client, &task_id, &done).await;
    assert_eq!(
        after,
        [TaskStatus::Working, TaskStatus::Completed],
        "after the answer: {after:?}"
    );

    // Before the answer, a live status may flip, for example while a replay
    // degrades. The task must still have asked for input.
    assert!(seen.contains(&TaskStatus::InputRequired), "{seen:?}");
    seen.extend(after);
    seen.dedup();
    assert_legal(&seen);
}

/// An ended task carries a TTL from the retention policy, counted from
/// `createdAt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ended_task_has_a_ttl_from_retention() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_failing_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = Vec::new();
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    let ttl = done["ttlMs"].as_u64().expect("an ended task has a TTL");
    let retention = u64::try_from(RETENTION.as_millis()).unwrap();
    assert!(ttl >= retention, "{ttl} < {retention}");
    assert!(ttl < retention + 60_000, "{ttl}");
}

/// A client that takes no elicitation sees no input request. The task reads
/// as `working`, and its message names the signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_without_elicitation_sees_no_input_request() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_parked_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = Vec::new();
    poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    let tasks_only = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        CLIENT_CAPABILITIES_META: {"extensions": {TASKS_EXTENSION: {}}},
    });
    let task = rpc(
        &client,
        "tasks/get",
        json!({"taskId": task_id, "_meta": tasks_only}),
    )
    .await;
    assert_eq!(task["status"], "working", "{task}");
    assert_eq!(task["statusMessage"], "waiting for signal: never");
    assert!(task.get("inputRequests").is_none(), "{task}");
}

/// A run with a workflow retry policy fails once and then succeeds. The task
/// shows no terminal status until the retry ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_run_shows_no_early_terminal_status() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let input = uuid::Uuid::new_v4().to_string();
    let created = create_task(&client, "start_task_flaky_flow", json!(input)).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"]["isError"], false, "{done}");
    assert_eq!(done["result"]["content"][0]["text"], "\"recovered\"");
    assert_eq!(seen, [TaskStatus::Working, TaskStatus::Completed]);
    assert_settled(&client, &task_id, &done).await;
    assert_eq!(count_runs(&db, "task_flaky_flow").await, 2);
}

/// A workflow error is a tool error. The task is `completed` with
/// `isError: true`, never `failed`, and Harvest starts no second run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_workflow_error_completes_the_task_as_a_tool_error() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_failing_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"]["isError"], true);
    assert!(
        done["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("card declined"),
        "{done}"
    );
    assert!(!seen.contains(&TaskStatus::Failed), "{seen:?}");
    assert_legal(&seen);

    // Later polls read the same result, and no retry run exists.
    assert_settled(&client, &task_id, &done).await;
    assert_eq!(count_runs(&db, "task_failing_flow").await, 1);
}

/// An operator rerun seals the ended run as `CONTINUED_AS_NEW`, with no
/// successor event. The task keeps its terminal status and its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rerun_leaves_an_ended_task_settled() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_failing_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["status"], "completed", "{done}");

    let mut conn = db.pool.get().await.expect("pool connection");
    let source = autumn_harvest::ExecutionId::from_uuid(task_id.parse().unwrap());
    let outcome = autumn_harvest::execution::rerun_workflow_execution(
        &mut conn,
        source,
        autumn_harvest::execution::RerunRequest {
            input_override: None,
            workflow_id_override: None,
            started_by: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            max_workflow_input_bytes: 0,
            max_execution_timeout_ceiling: None,
            max_workflow_chain_timeout_ceiling: None,
            max_workflow_attempts_ceiling: None,
            trace_context: None,
        },
        None,
    )
    .await
    .expect("rerun");
    drop(conn);
    assert_eq!(outcome.source_prior_state, "FAILED");

    let after = get_task(&client, &task_id).await;
    assert_eq!(after["status"], "completed", "{after}");
    assert_eq!(after["result"], done["result"], "{after}");
    assert_settled(&client, &task_id, &done).await;
}

/// An operator reset of a failed run seals it as `TERMINATED` and clears its
/// error. The task keeps its `completed` status and its error text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_leaves_a_failed_task_settled() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_failing_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["result"]["isError"], true, "{done}");

    let mut conn = db.pool.get().await.expect("pool connection");
    let source = autumn_harvest::ExecutionId::from_uuid(task_id.parse().unwrap());
    autumn_harvest::reset::reset_workflow_execution(
        &mut conn,
        source,
        autumn_harvest::reset::WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "operator retry".to_string(),
            operator_id: "op-1".to_string(),
            signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::default(),
            allow_terminal_source: true,
        },
        None,
    )
    .await
    .expect("reset");
    drop(conn);

    let after = get_task(&client, &task_id).await;
    assert_eq!(after["status"], "completed", "{after}");
    assert_eq!(after["result"], done["result"], "{after}");
    assert_settled(&client, &task_id, &done).await;
}

/// `tasks/cancel` moves a live task to `cancelled`. A second cancel and a
/// late answer are acknowledged and change nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tasks_cancel_moves_the_task_to_cancelled() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_parked_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];
    let waiting = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    let key = only_key(&waiting);

    let params = json!({"taskId": task_id, "_meta": declared()});
    assert_eq!(
        rpc(&client, "tasks/cancel", params.clone()).await,
        json!({"resultType": "complete"})
    );
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["status"], "cancelled", "{done}");
    assert!(done.get("result").is_none(), "{done}");
    assert_legal(&seen);
    assert_settled(&client, &task_id, &done).await;

    assert_eq!(
        rpc(&client, "tasks/cancel", params).await,
        json!({"resultType": "complete"})
    );
    answer(&client, &task_id, &key, form(&json!({}))).await;
    assert_eq!(get_task(&client, &task_id).await["status"], "cancelled");
}

// ── AC 2: crash-safe task creation ────────────────────────────────────────────

/// A retried `tools/call` with the same start key returns the same task and
/// starts one execution. The key can ride the `Idempotency-Key` header or the
/// request `_meta`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_task_create_starts_one_execution() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;
    let call = tool_call("start_task_parked_flow", &json!("retry"));

    let first = rpc_with(&client, call.clone(), Some("create-1")).await;
    let retry = rpc_with(&client, call.clone(), Some("create-1")).await;
    assert_eq!(first["result"]["resultType"], "task", "{first}");
    assert_eq!(first["result"]["taskId"], retry["result"]["taskId"]);
    assert_eq!(count_runs(&db, "task_parked_flow").await, 1);

    let mut meta_call = call.clone();
    meta_call["params"]["_meta"][START_KEY_META] = json!("create-2");
    let first = rpc_with(&client, meta_call.clone(), None).await;
    let retry = rpc_with(&client, meta_call, None).await;
    assert_eq!(first["result"]["taskId"], retry["result"]["taskId"]);
    assert_eq!(count_runs(&db, "task_parked_flow").await, 2);

    // The header wins over `_meta`. A call that carries header `create-3`
    // and `_meta` key `create-2` is a new task, not the `create-2` task.
    let mut both = call.clone();
    both["params"]["_meta"][START_KEY_META] = json!("create-2");
    let header_wins = rpc_with(&client, both, Some("create-3")).await;
    assert_ne!(header_wins["result"]["taskId"], first["result"]["taskId"]);
    assert_eq!(count_runs(&db, "task_parked_flow").await, 3);

    // With no key, each call is a new task.
    let a = rpc_with(&client, call.clone(), None).await;
    let b = rpc_with(&client, call, None).await;
    assert_ne!(a["result"]["taskId"], b["result"]["taskId"]);
    assert_eq!(count_runs(&db, "task_parked_flow").await, 5);
}

// ── AC 3: input_required resumes on input ─────────────────────────────────────

/// Two waits on one signal name get two keys. Two answers to one key that
/// race each other deliver one signal, so the run sees each answer once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_wait_gets_its_own_key_and_a_raced_answer_lands_once() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_two_step_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = vec![status_of(&created)];

    let first = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    let key1 = only_key(&first);
    assert!(key1.contains(":signal:étape:"), "{key1}");
    // Both calls read the wait as open, so the signal idempotency key is
    // what lets only one of them land.
    let (a, b) = tokio::join!(
        answer(&client, &task_id, &key1, form(&json!({"n": 1}))),
        answer(&client, &task_id, &key1, form(&json!({"n": 1}))),
    );
    assert_eq!(a, json!({"resultType": "complete"}));
    assert_eq!(b, json!({"resultType": "complete"}));

    let second = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required" && only_key(t) != key1
    })
    .await;
    let key2 = only_key(&second);
    assert!(key2.contains(":signal:étape:"), "{key2}");
    // A key that is not open now is ignored.
    answer(&client, &task_id, &key1, form(&json!({"n": 98}))).await;
    answer(&client, &task_id, &key2, form(&json!({"n": 2}))).await;

    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert!(
        done["result"].get("structuredContent").is_none(),
        "an array output has no structured content: {done}"
    );
    let output: Value =
        serde_json::from_str(done["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(output, json!([{"n": 1}, {"n": 2}]));
    assert_legal(&seen);
}

/// A `decline` answer is an error, and the wait stays open with its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declined_answer_is_an_error_and_the_wait_stays_open() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let created = create_task(&client, "start_task_parked_flow", json!("x")).await;
    let task_id = created["taskId"].as_str().unwrap().to_string();
    let mut seen = Vec::new();
    let waiting = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    let key = only_key(&waiting);
    let out = rpc_with(
        &client,
        update_call(&task_id, &key, "decline", &json!({})),
        None,
    )
    .await;
    assert_eq!(out["error"]["code"], -32602, "{out}");
    // An accept whose content does not match the requested schema is refused
    // the same way, and no signal goes in.
    for content in [
        json!({}),
        json!({"decision": "ship"}),
        json!({PAYLOAD_FIELD: 1}),
    ] {
        let out = rpc_with(
            &client,
            update_call(&task_id, &key, "accept", &content),
            None,
        )
        .await;
        assert_eq!(out["error"]["code"], -32602, "{content}: {out}");
    }
    let again = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    assert_eq!(only_key(&again), key);
}

/// A task outlives the app that created it, because the task is the run.
///
/// This builds a second app on the same database. As in
/// `mcp_tools_integration.rs`, the first app runs no shutdown hook, so this
/// does not prove that its worker stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_survives_a_daemon_restart() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;

    let task_id = {
        let client = build_app(&db).await;
        let created = create_task(&client, "start_task_approval_flow", json!("r2")).await;
        let task_id = created["taskId"].as_str().unwrap().to_string();
        let mut seen = Vec::new();
        poll_until(&client, &task_id, &mut seen, |t| {
            t["status"] == "input_required"
        })
        .await;
        task_id
    };

    let client = build_app(&db).await;
    let mut seen = Vec::new();
    let waiting = poll_until(&client, &task_id, &mut seen, |t| {
        t["status"] == "input_required"
    })
    .await;
    let key = only_key(&waiting);
    answer(&client, &task_id, &key, form(&json!({"decision": "ship"}))).await;
    let done = poll_until(&client, &task_id, &mut seen, |t| status_of(t).is_terminal()).await;
    assert_eq!(done["result"]["content"][0]["text"], "\"r2:ship\"");
}

// ── Protocol edges ────────────────────────────────────────────────────────────

/// A run of a workflow outside the task catalog is not a task. It gets the
/// same error as an unknown id, so the route is no existence oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_outside_the_catalog_is_not_a_task() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let started = client
        .post("/api/harvest/workflows/task_hidden_flow/start")
        .json(&json!({"input": "x"}))
        .send()
        .await;
    let started = started.json::<Value>();
    let hidden = started["execution_id"].as_str().expect("execution_id");
    let unknown = uuid::Uuid::new_v4().to_string();
    for task_id in [hidden, unknown.as_str()] {
        let out = rpc_with(
            &client,
            json!({
                "jsonrpc": "2.0", "id": 3, "method": "tasks/get",
                "params": {"taskId": task_id, "_meta": declared()}
            }),
            None,
        )
        .await;
        assert_eq!(out["error"]["code"], -32602, "{task_id}: {out}");
        assert_eq!(
            out["error"]["message"], "Failed to retrieve task: Task not found",
            "{task_id}"
        );
    }
}

/// A client that does not declare the extension gets a plain tool result
/// with the run handle, as `start_{wf}` on `/mcp` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_without_the_extension_gets_a_plain_tool_result() {
    let _ = tracing_subscriber::fmt::try_init();
    let db = setup_db().await;
    let client = build_app(&db).await;

    let out = rpc(
        &client,
        "tools/call",
        json!({"name": "start_task_parked_flow", "arguments": {"body": "x"}}),
    )
    .await;
    assert_ne!(out["resultType"], "task", "{out}");
    assert_eq!(out["isError"], false, "{out}");
    let handle: Value = serde_json::from_str(out["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(handle["execution_id"].is_string(), "{handle}");
}
