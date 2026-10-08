//! Integration tests for the DAG run graph view (issue #690).
//!
//! These exercise `GET /dags/{dag_name}/runs/{run_exec_id}` end to end against a
//! real Postgres (testcontainers). The endpoint is read-only: it reconstructs a
//! unified DAG run's node topology annotated with per-node status/timing purely
//! from the registered `DagDefinition` and recorded history. It adds no new
//! event variant and no migration.

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_const_for_fn)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::dag::DagBuilder;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::prelude::*;
use autumn_harvest::scheduler::{RegisteredDag, SchedulerMonitor, compile_dag_catalog};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, Priority, ShardId, WorkerId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    OverlapPolicy, StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DAG + activity definitions under test ───────────────────────────────────
//
// Node name == activity name. Distinct activities give distinct node names.

#[activity(queue = "default")]
async fn step_a(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_b(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_c(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}
#[activity(queue = "default")]
async fn step_d(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

/// Linear: step_a -> step_b -> step_c
#[dag(default_queue = "default")]
fn graph_linear_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let _c = dag.activity(step_c).upstream(&b);
}

/// Fan-out: step_a -> {step_b, step_c} -> step_d
#[dag(default_queue = "default")]
fn graph_fanout_dag(dag: &mut DagBuilder) {
    let a = dag.activity(step_a);
    let b = dag.activity(step_b).upstream(&a);
    let c = dag.activity(step_c).upstream(&a);
    let _d = dag.activity(step_d).upstream(&b).upstream(&c);
}

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        workflows![graph_linear_dag, graph_fanout_dag],
        activities![step_a, step_b, step_c, step_d],
    ))
}

/// Build the app with a fabricated catalog. When `classic` is provided, that
/// `(name, RegisteredDag)` pair is inserted alongside the unified DAGs so the
/// classic (non-unified) 400 path can be exercised.
fn build_app_with(
    pool: &DbPool,
    admin: bool,
    classic: Option<(String, RegisteredDag)>,
) -> HarvestApiApp {
    let mut catalog = compile_dag_catalog(dags![graph_linear_dag, graph_fanout_dag])
        .expect("dag catalog compiles");
    if let Some((name, dag)) = classic {
        catalog.insert(name, dag);
    }
    let api_state = HarvestApiState::new();
    if admin {
        // Bypass the admin guard for the happy-path / DB assertions.
        api_state.set_admin_auth_boundary(true);
    }
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(catalog),
        Arc::new(Vec::new()),
        Some("dag-graph-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    build_app_with(pool, true, None)
}

/// Byte-reversal test codec, so `encode_event` produces the real
/// `_harvest_codec_envelope` shape a codec-encrypting deployment stores.
#[derive(Debug)]
struct ReverseGraphCodec;

impl autumn_harvest::payload_codec::PayloadCodec for ReverseGraphCodec {
    fn codec_id(&self) -> &'static str {
        "reverse"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        let mut v = raw.to_vec();
        v.reverse();
        Ok(v)
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
        let mut v = encoded.to_vec();
        v.reverse();
        Ok(v)
    }
}

fn reversing_codecs() -> autumn_harvest::payload_codec::PayloadCodecs {
    let mut codecs = autumn_harvest::payload_codec::PayloadCodecs::default();
    codecs.set_default(Arc::new(ReverseGraphCodec));
    codecs
}

/// Same app as [`build_app`] but with a non-identity codec registered, so the
/// handler must decode before deriving the run graph (issue #780 round 5).
fn build_app_with_codec(pool: &DbPool) -> HarvestApiApp {
    let catalog = compile_dag_catalog(dags![graph_linear_dag, graph_fanout_dag])
        .expect("dag catalog compiles");
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.set_payload_codecs(reversing_codecs());
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        registry(),
        Arc::new(catalog),
        Arc::new(Vec::new()),
        Some("dag-graph-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// A compensation dispatch (the shape `unwind_dag_compensations` records)
/// whose `input` has been run through the reversing codec, i.e. exactly what a
/// codec-encrypting deployment stores in `harvest_events.event_data`.
///
/// `store::append_events` encodes with the identity codec, whose
/// `encode_payload` is a pass-through clone — so the already-enveloped `input`
/// survives verbatim into storage.
fn codec_encoded_compensation_dispatch(
    compensator_name: &str,
    compensates_node: &str,
    id: ActivityExecId,
) -> WorkflowEvent {
    let plain = WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: compensator_name.to_string(),
        input: json!({
            "dag_compensate": compensates_node,
            "input": Value::Null,
            "output": Value::Null,
        }),
        queue: "default".to_string(),
    };
    let encoded = reversing_codecs()
        .encode_event(&plain)
        .expect("encode compensation dispatch");
    assert_eq!(
        encoded["data"]["input"]["_harvest_codec_envelope"], 1,
        "fixture sanity: the stored dispatch must carry a codec envelope on `input`"
    );
    serde_json::from_value(encoded).expect("re-deserialize stored shape")
}

/// A hand-built classic (non-unified) `RegisteredDag`. `compile_dag_catalog`
/// rejects classic DAGs, so the only way to register one is to construct it.
fn classic_registered_dag(name: &str) -> RegisteredDag {
    let mut builder = DagBuilder::new();
    let _a = builder.activity(step_a);
    let definition = builder.build().expect("classic dag builds");
    RegisteredDag {
        name: name.to_string(),
        module: "test".to_string(),
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: Some("default".to_string()),
        is_unified: false,
        definition,
        jitter: Duration::ZERO,
        overlap_policy: OverlapPolicy::default(),
        buffer_all_max: 0,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

fn sched(name: &str, id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: name.to_string(),
        input: json!({ "dag_task": name }),
        queue: "default".to_string(),
    }
}

fn completed(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: id,
        output: Value::Null,
    }
}

/// An `ActivityStarted` — the engine appends one per worker claim (including
/// each activity-level retry), reusing the original `activity_id`.
fn activity_started(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityStarted {
        activity_id: id,
        worker_id: WorkerId::new("worker-1"),
    }
}

fn failed(id: ActivityExecId) -> WorkflowEvent {
    WorkflowEvent::ActivityFailed {
        activity_id: id,
        error: "transient S3 500\nstack trace line".to_string(),
        attempt: 1,
        error_type: "S3Error".to_string(),
        non_retryable: false,
        details: None,
    }
}

/// A `dag_skip:{task_index}` marker (#482 data-dependent condition branch).
fn skip_marker(task_index: usize, activity_name: &str) -> WorkflowEvent {
    WorkflowEvent::MarkerRecorded {
        name: format!("dag_skip:{task_index}"),
        details: json!({ "task": activity_name, "reason": "condition_false" }),
    }
}

/// A `dag_skip:{task_index}` marker whose `details` is an opaque codec envelope,
/// exactly as it is persisted on a deployment with a non-identity `PayloadCodec`
/// (`details` is a payload field, wrapped by `transform_event_data` on write).
/// The default identity codec stores this verbatim, so seeding it reproduces a
/// codec-deployment's on-disk state without configuring a real codec. The read
/// path does not decode, so the recorded task/upstream fingerprint is opaque
/// here — classification must fall back to the marker name/index (issue #690
/// review, Codex CLAIM D).
fn skip_marker_codec_envelope(task_index: usize) -> WorkflowEvent {
    WorkflowEvent::MarkerRecorded {
        name: format!("dag_skip:{task_index}"),
        details: json!({
            "_harvest_codec_envelope": 1,
            "codec_id": "reverse",
            "data": "b3BhcXVl",
        }),
    }
}

/// Seed a workflow execution for `dag_name`, append `events` after the initial
/// `WorkflowStarted`, then transition it to `state`.
async fn seed_run(
    conn: &mut AsyncPgConnection,
    dag_name: &'static str,
    workflow_id: &str,
    events: Vec<WorkflowEvent>,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: dag_name,
            workflow_id,
            exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("seed workflow");

    let history = store::load_history(conn, exec_id).await.unwrap();
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append seed events");

    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq(state))
        .execute(conn)
        .await
        .expect("set state");

    exec_id
}

async fn establish(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

fn node<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| n["node_name"] == json!(name))
        .unwrap_or_else(|| panic!("node {name} present in body: {body}"))
}

// ── (a) annotated topology, end to end ──────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_returns_annotated_topology() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a succeeded; b failed; c never reached. Run is FAILED.
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_b", ib),
        failed(ib),
        WorkflowEvent::workflow_failed("one or more DAG tasks failed"),
    ];
    let exec_id = seed_run(&mut conn, "graph_linear_dag", "graph-1", events, "FAILED").await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Run-level summary.
    assert_eq!(body["dag_name"], json!("graph_linear_dag"));
    assert_eq!(body["run_exec_id"], json!(exec_id.to_string()));
    assert_eq!(body["state"], json!("FAILED"));
    assert!(!body["started_at"].is_null(), "started_at present");

    // Node statuses.
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    assert_eq!(node(&body, "step_b")["status"], json!("failed"));
    assert_eq!(node(&body, "step_c")["status"], json!("pending"));

    // depends_on topology (AC3).
    assert_eq!(node(&body, "step_a")["depends_on"], json!([]));
    assert_eq!(node(&body, "step_b")["depends_on"], json!(["step_a"]));
    assert_eq!(node(&body, "step_c")["depends_on"], json!(["step_b"]));

    // Timing + attempts for the attempted nodes.
    assert!(!node(&body, "step_a")["started_at"].is_null());
    assert!(!node(&body, "step_a")["finished_at"].is_null());
    assert_eq!(node(&body, "step_a")["attempts"], json!(1));

    // Failed node carries error_type + first-line-truncated error.
    let b_node = node(&body, "step_b");
    assert_eq!(b_node["error_type"], json!("S3Error"));
    assert_eq!(b_node["error"], json!("transient S3 500"));
    assert!(!b_node["finished_at"].is_null());

    // Never-reached node has no timing / error fields (skip_serializing_if).
    let c_node = node(&body, "step_c");
    assert!(c_node.get("started_at").is_none() || c_node["started_at"].is_null());
    assert!(c_node.get("error").is_none() || c_node["error"].is_null());
}

// ── (b) 404 for unknown DAG ─────────────────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_unknown_dag_is_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-404",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/no_such_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── (c) 404 when the exec is not a run of that DAG ──────────────────────────

#[tokio::test]
async fn dag_run_graph_exec_not_a_run_of_dag_is_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // The run belongs to graph_linear_dag; ask for it under graph_fanout_dag.
    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-mismatch",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/graph_fanout_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── (d) 400 (JSON body) for a classic (non-unified) DAG ─────────────────────

#[tokio::test]
async fn dag_run_graph_classic_dag_is_bad_request_with_json_body() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let classic = classic_registered_dag("classic_dag");
    let app = build_app_with(&pool, true, Some(("classic_dag".to_string(), classic)));
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-classic",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/classic_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    // AC6: a JSON error body, never a 500 / empty body.
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("classic")),
        "expected a JSON error body mentioning classic DAGs, got: {body}"
    );
}

// ── (e) admin-auth rejection ────────────────────────────────────────────────

#[tokio::test]
async fn dag_run_graph_requires_admin_auth() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    // No admin boundary, non-dev profile, no session on the request → the
    // admin route layer rejects with 401 before any handler logic runs.
    let app = build_app_with(&pool, false, None);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-auth",
        events,
        "RUNNING",
    )
    .await;

    let (status, _body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ── (f) running vs cancelled node status depends on run state ────────────────

#[tokio::test]
async fn dag_run_graph_running_node_on_live_run() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a completed; b scheduled but no terminal event; run still RUNNING.
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![sched("step_a", ia), completed(ia), sched("step_b", ib)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-live",
        events,
        "RUNNING",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    // Scheduled-no-terminal on a live run → running (not cancelled).
    assert_eq!(node(&body, "step_b")["status"], json!("running"));
    assert!(body["finished_at"].is_null(), "live run has no finished_at");
}

// ── (g) PAUSED run: in-flight node reports running, not cancelled (FIX 1) ─────

#[tokio::test]
async fn dag_run_graph_paused_run_node_is_running_not_cancelled() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a completed; b scheduled but no terminal event; run PAUSED (issue #383).
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![sched("step_a", ia), completed(ia), sched("step_b", ib)];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-paused",
        events,
        "PAUSED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], json!("PAUSED"));
    // PAUSED is non-terminal → a scheduled-no-terminal node is running, not cancelled.
    assert_eq!(node(&body, "step_b")["status"], json!("running"));
}

// ── (h) terminal run: in-flight node reports cancelled (FIX 7a) ──────────────

#[tokio::test]
async fn dag_run_graph_terminal_run_node_is_cancelled() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // a completed; b scheduled but no terminal event; run CANCELLED.
    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_b", ib),
        WorkflowEvent::WorkflowCancelled {
            reason: "operator cancel".to_string(),
        },
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-cancel",
        events,
        "CANCELLED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["state"], json!("CANCELLED"));
    // Scheduled-no-terminal on a terminal run → cancelled.
    assert_eq!(node(&body, "step_b")["status"], json!("cancelled"));
    // step_c was never reached.
    assert_eq!(node(&body, "step_c")["status"], json!("pending"));
}

// ── (i) skip marker → skipped; downstream never-reached → pending (FIX 7b) ───

#[tokio::test]
async fn dag_run_graph_skipped_node_and_pending_downstream() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // graph_linear_dag: step_a (idx 0) -> step_b (idx 1) -> step_c (idx 2).
    // a succeeds; b is skipped by a data-dependent condition (dag_skip:1 marker,
    // #482); c downstream of b is never reached.
    let ia = ActivityExecId::new();
    let events = vec![sched("step_a", ia), completed(ia), skip_marker(1, "step_b")];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-skip",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    // Skipped by condition marker.
    assert_eq!(node(&body, "step_b")["status"], json!("skipped"));
    // Downstream of the skipped node, never reached → pending (not skipped).
    assert_eq!(node(&body, "step_c")["status"], json!("pending"));
    // A skipped node has no timing.
    let b_node = node(&body, "step_b");
    assert!(b_node.get("started_at").is_none() || b_node["started_at"].is_null());
}

#[tokio::test]
async fn dag_run_graph_skip_marker_with_opaque_codec_details_is_still_skipped() {
    // Issue #690 review (Codex CLAIM D): on a non-identity-codec deployment the
    // dag_skip marker's `details` is stored as an opaque envelope, so its
    // recorded task/upstream fingerprint is unreadable at this (non-decoding)
    // read path. The condition-skipped node must still be reported `skipped`
    // (falling back to the always-clear marker name/index), not `pending`.
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // graph_linear_dag: step_a (0) -> step_b (1) -> step_c (2).
    let ia = ActivityExecId::new();
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        skip_marker_codec_envelope(1),
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-skip-codec",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    // Skipped despite the opaque codec-envelope details.
    assert_eq!(node(&body, "step_b")["status"], json!("skipped"));
    assert_eq!(node(&body, "step_c")["status"], json!("pending"));
}

#[tokio::test]
async fn dag_run_graph_excludes_a_codec_encoded_compensation_dispatch() {
    // Issue #780 round 5 (Codex): the run-graph readers load RAW history
    // (`load_history_with_timestamps` leaves payload fields undecoded), so on a
    // codec-encrypting or payload-offloading deployment the round-4 compensation
    // exclusion silently stopped working — `is_compensation_dispatch` saw an
    // opaque envelope, returned false, and a never-executed forward node was
    // reported with the old compensation's status/timings/attempts.
    //
    // Cross-version drift: history was produced by an older definition whose
    // unwind dispatched a compensator named `step_b`; the CURRENT
    // `graph_linear_dag` declares `step_b` as an ordinary forward node.
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with_codec(&pool);
    let mut conn = establish(&url).await;

    let ia = ActivityExecId::new();
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        // The old unwind's compensator for step_a, named like today's step_b.
        codec_encoded_compensation_dispatch("step_b", "step_a", ActivityExecId::new()),
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-compensation-codec",
        events,
        "FAILED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));

    // The compensation dispatch must NOT be read as step_b's own dispatch.
    // Pre-fix this reported the compensation's outcome instead of `pending`.
    let b_node = node(&body, "step_b");
    assert_eq!(
        b_node["status"],
        json!("pending"),
        "a compensator dispatch named like a current forward node must be excluded \
         even when its input is a codec envelope: {b_node}"
    );
    // And it must carry none of the compensation's timing/attempts, so status
    // and timing can never describe different attempts.
    assert!(
        b_node.get("started_at").is_none() || b_node["started_at"].is_null(),
        "an excluded compensation must leave no timing on the forward node: {b_node}"
    );
    assert_eq!(b_node["attempts"], json!(0), "node: {b_node}");
}

#[tokio::test]
async fn dag_run_graph_still_reads_a_codec_encoded_forward_dispatch() {
    // The guard for the fix above: decoding must not make an ORDINARY forward
    // dispatch disappear. Its decoded input is plain user data, so
    // `is_compensation_dispatch` stays false and the node reads normally.
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with_codec(&pool);
    let mut conn = establish(&url).await;

    let (ia, ib) = (ActivityExecId::new(), ActivityExecId::new());
    let events = vec![
        sched("step_a", ia),
        completed(ia),
        sched("step_b", ib),
        completed(ib),
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-forward-codec",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(node(&body, "step_a")["status"], json!("succeeded"));
    assert_eq!(
        node(&body, "step_b")["status"],
        json!("succeeded"),
        "an ordinary forward dispatch must still be read on a codec deployment"
    );
}

// ── (j) retried node: attempts counted from ActivityStarted (Codex CLAIM A) ──

#[tokio::test]
async fn dag_run_graph_retried_node_reports_latest_attempt() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // Realistic engine output for an activity-level retry: step_c is scheduled
    // ONCE and the engine appends a fresh ActivityStarted on each claim. The
    // first attempt fails *retryably* (which appends NO ActivityFailed —
    // queue::requeue_for_retry re-pends the same task row and appends no event),
    // then the second attempt (claim) succeeds. So the node has one
    // ActivityScheduled and two ActivityStarted events, and `attempts` must be
    // counted from ActivityStarted (the pre-fix code counted ActivityScheduled
    // and would report attempts: 1 here).
    let (ia, ib, ic) = (
        ActivityExecId::new(),
        ActivityExecId::new(),
        ActivityExecId::new(),
    );
    let events = vec![
        sched("step_a", ia),
        activity_started(ia),
        completed(ia),
        sched("step_b", ib),
        activity_started(ib),
        completed(ib),
        sched("step_c", ic),  // scheduled ONCE
        activity_started(ic), // claim 1
        // attempt 1 fails retryably → requeue (no ActivityFailed appended)
        activity_started(ic), // claim 2 (the retry)
        completed(ic),
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-retry",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let c_node = node(&body, "step_c");
    // Both execution attempts counted (from ActivityStarted, not the single
    // ActivityScheduled).
    assert_eq!(c_node["attempts"], json!(2));
    // A single-run node reports 1 attempt.
    assert_eq!(node(&body, "step_a")["attempts"], json!(1));
    // Status/timing describe the latest (successful) attempt.
    assert_eq!(c_node["status"], json!("succeeded"));
    assert!(!c_node["started_at"].is_null(), "latest attempt started_at");
    assert!(
        !c_node["finished_at"].is_null(),
        "latest attempt finished_at"
    );
    // The latest attempt succeeded, so no error fields are carried.
    assert!(c_node.get("error").is_none() || c_node["error"].is_null());
    assert!(c_node.get("error_type").is_none() || c_node["error_type"].is_null());
}

// ── (l) skip marker with a stale task fingerprint is ignored (Codex CLAIM B) ──

#[tokio::test]
async fn dag_run_graph_skip_marker_with_mismatched_task_is_ignored() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = establish(&url).await;

    // graph_linear_dag: step_a (idx 0) -> step_b (idx 1) -> step_c (idx 2).
    // A dag_skip:1 marker was recorded for a node named "was_renamed" (the DAG
    // was reordered/renamed after this run), so it no longer identifies the
    // "step_b" now at index 1 — the graph must NOT mislabel step_b as skipped.
    let ia = ActivityExecId::new();
    let events = vec![
        sched("step_a", ia),
        activity_started(ia),
        completed(ia),
        skip_marker(1, "was_renamed"),
    ];
    let exec_id = seed_run(
        &mut conn,
        "graph_linear_dag",
        "graph-skip-stale",
        events,
        "COMPLETED",
    )
    .await;

    let (status, body) = get_json(&app, &format!("/dags/graph_linear_dag/runs/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    // The recorded task fingerprint no longer matches → the marker is ignored,
    // and the never-scheduled node falls through to pending, not skipped.
    assert_eq!(node(&body, "step_b")["status"], json!("pending"));
}

// ── (k) malformed run_exec_id → 400 JSON (FIX 5) ─────────────────────────────

#[tokio::test]
async fn dag_run_graph_malformed_run_exec_id_is_bad_request_with_json_body() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // A valid unified DAG name reaches the id parse (the unknown-DAG and
    // classic-DAG checks run first); a garbage run_exec_id is rejected 400.
    let (status, body) = get_json(&app, "/dags/graph_linear_dag/runs/not-a-valid-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    // A JSON error body, never a 500 / empty body.
    assert!(
        !body.is_null(),
        "expected a JSON error body for a malformed run_exec_id, got null"
    );
}
