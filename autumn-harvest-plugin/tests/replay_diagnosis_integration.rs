//! Integration tests for `POST /api/harvest/workflows/{id}/replay-diagnosis`
//! (issue #614).
//!
//! Tests run against a real Postgres instance (see the Execution note below) and
//! exercise the single-execution replay-diagnosis endpoint end-to-end: it loads one
//! execution's recorded history from its owning shard and replays it against the
//! currently-registered `#[workflow]` handler via `WorkflowReplayer`, returning a
//! structured verdict.
//!
//! Scenarios covered:
//!   (1)  A `COMPLETED` run whose history matches the registered handler → 200
//!        `clean` (message "no divergence under current code").
//!   (2)  A history that diverges from the registered handler (a recorded
//!        activity name the handler no longer schedules) → 200 `diverged` with
//!        `{kind, event_index, expected, actual}`.
//!   (3)  A `FAILED` run diagnosed retroactively (AC4) → 200 `clean` carrying the
//!        reproduced failure (issue #952: reproducing a recorded terminal failure
//!        is not a divergence).
//!   (4)  Unknown execution id → 404.
//!   (5)  Malformed execution id → 400.
//!   (6)  A workflow type not registered on this node → 200 `not_registered`.
//!   (7)  A classic (non-unified) DAG run → 200 `not_replayable_dag`.
//!   (8)  A PII-erased history (issue #495) → 410.
//!   (9)  Zero-writes (AC3): the event count and execution row state are
//!        unchanged before and after a diagnosis.
//!   (10) The headline: an nd-blocked `RUNNING` run diagnoses its divergence →
//!        200 `diverged`.
//!   (11) A healthy in-flight `RUNNING` run parked mid-activity (frontier
//!        suspension) → 200 `clean` (canary replay mode, FIX 2).
//!   (12) A `TIMED_OUT` run sealed mid-await (single-activity in-progress) → 200
//!        `clean` (frontier suspension, FIX 3).
//!   (13) A `TIMED_OUT` run sealed awaiting a second activity → 200 `clean`
//!        (sealed-mid-await reclassification, FIX 3).
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to an already-migrated Postgres to
//! run against it directly (no Docker); otherwise a testcontainer is started with
//! the schema applied via `autumn_harvest::full_migrations_sql()`. This suite is
//! executed Docker-backed in CI (per the #543/#544/#601 precedent).

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::erase;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

// ── Test workflow ────────────────────────────────────────────────────────────

/// Processes each item from its input via a `process_item` activity. Returns
/// `Err` after processing every item when `should_fail` is set.
fn progress_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let items = input
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut processed = 0u64;
        for item in &items {
            ctx.execute_activity_raw("process_item", item.clone(), "default")
                .await
                .map_err(|e| e.to_string())?;
            processed += 1;
        }
        if input
            .get("should_fail")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err("deliberate failure after processing all items".to_string());
        }
        Ok(json!({ "processed": processed }))
    })
}

fn progress_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "progress_wf",
        module: "tests",
        handler: progress_workflow,
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

/// Typed shared state injected on the registry (issue #614/#1107 regression):
/// modelled after a real deployment that registers config via
/// `HarvestBuilder::state(...)` and reads it during replay with `ctx.state::<T>()`.
struct DiagConfig {
    greeting: String,
}

/// Reads typed shared state via `ctx.state::<DiagConfig>()` and returns it as the
/// output; when the state is absent it returns `Err`. This is the regression
/// probe for PR #1107: the live worker replays with `registry.shared_state()`, so
/// a faithful diagnosis replay MUST see the same state — otherwise `ctx.state`
/// returns `None` under diagnosis and this workflow spuriously fails, yielding a
/// `workflow_failed` verdict for a run that replays cleanly on the worker.
fn state_reading_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.state::<DiagConfig>().map_or_else(
            || Err("shared state DiagConfig not available during replay".to_string()),
            |cfg| Ok(json!({ "greeting": cfg.greeting.clone() })),
        )
    })
}

fn state_reading_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "state_reading_wf",
        module: "tests",
        handler: state_reading_workflow,
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

/// Branches its command set on `ctx.should_continue_as_new()` (issue #614/#1107
/// regression): schedules `checkpoint` when the history-size trigger has fired,
/// else `keep_going`. Because `should_continue_as_new()` reads the history
/// policy's `continue_as_new_threshold`, this workflow's replay is faithful only
/// when the SAME policy the live worker uses (`registry.history_policy()`) is
/// threaded into the diagnosis replay. Under the default policy (large
/// threshold) the trigger never fires for a small history, so the diagnosis
/// would schedule `keep_going` and diverge from a history recorded under a
/// low-threshold deployment that scheduled `checkpoint`.
fn policy_branch_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = if ctx.should_continue_as_new() {
            "checkpoint"
        } else {
            "keep_going"
        };
        ctx.execute_activity_raw(activity, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn policy_branch_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "policy_branch_wf",
        module: "tests",
        handler: policy_branch_workflow,
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

// ── Harness ──────────────────────────────────────────────────────────────────

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    // Run against an already-migrated Postgres when HARVEST_TEST_DATABASE_URL is
    // set (no Docker needed), else spin up a testcontainer with the schema
    // applied via init SQL. Mirrors the dual-mode precedent in
    // retention_summary_tests.
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

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    // Register `progress_wf` as a workflow, and `classic_dag` as a classic DAG
    // name (present in the DAG-name set but NOT in `registry.workflows`), so the
    // not_replayable_dag verdict is reachable.
    let runtime = HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![progress_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("replay-diagnosis-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    )
    .with_registered_dag_names(["classic_dag".to_string()]);
    api_state.install(runtime);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Like [`build_app`] but registers `policy_branch_wf` on a registry carrying a
/// NON-default history policy (`continue_as_new_threshold = 2`) — mirroring a
/// deployment that tunes `HarvestBuilder::history_policy(...)`. Used by the
/// issue #614 regression test to prove the diagnosis replay threads
/// `registry.history_policy()` into the replayer exactly as the live worker does,
/// so a `should_continue_as_new`-branching workflow replays under the same policy.
fn build_app_with_history_policy(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let policy =
        autumn_harvest::context::WorkflowHistoryPolicy::default().with_continue_as_new_threshold(2);
    let runtime = HarvestApiRuntime::new(
        Arc::new(
            HandlerRegistry::new(vec![policy_branch_info()], vec![]).with_history_policy(policy),
        ),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("replay-diagnosis-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    );
    api_state.install(runtime);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Like [`build_app`] but registers `state_reading_wf` on a registry carrying a
/// typed `DiagConfig` shared-state value — mirroring a deployment that injects
/// config via `HarvestBuilder::state(...)`. Used by the PR #1107 regression test
/// to prove the diagnosis replay threads `registry.shared_state()` into the
/// replayer exactly as the live worker does.
fn build_app_with_state(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));

    let mut state_map: autumn_harvest::context::SharedStateMap = std::collections::HashMap::new();
    state_map.insert(
        std::any::TypeId::of::<DiagConfig>(),
        Box::new(DiagConfig {
            greeting: "hello".to_string(),
        }),
    );
    let shared_state: autumn_harvest::context::SharedState = Arc::new(state_map);

    let runtime = HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::with_state(
            vec![state_reading_info()],
            vec![],
            shared_state,
        )),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("replay-diagnosis-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    );
    api_state.install(runtime);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn post_diagnosis(app: &HarvestApiApp, id: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/workflows/{id}/replay-diagnosis"))
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Seed an execution row with an explicit `state` plus the given history events.
async fn seed_execution(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    input: Value,
    mut events: Vec<WorkflowEvent>,
    erased: bool,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();

    let row_input = if erased {
        erase::erasure_tombstone()
    } else {
        input.clone()
    };

    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(row_input)
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(&mut conn)
    .await
    .expect("seed execution");

    if erased {
        for event in &mut events {
            let mut value = serde_json::to_value(&*event).expect("event serialises");
            let _ = erase::tombstone_payload_fields(&mut value);
            *event = serde_json::from_value(value).expect("event round-trips");
        }
    }

    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// Full `progress_wf` history for `items`: `WorkflowStarted` + one
/// scheduled/completed activity pair per item.
fn progress_history(items: &[&str], should_fail: bool) -> (Value, Vec<WorkflowEvent>) {
    let input = json!({ "items": items, "should_fail": should_fail });
    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for item in items {
        let id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "process_item".into(),
            input: json!(item),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        });
    }
    (input, events)
}

/// A history whose FIRST activity is recorded under a name (`renamed_activity`)
/// that the registered `progress_wf` handler no longer schedules — so replaying
/// the handler (which schedules `process_item`) diverges at event index 1.
fn diverging_history() -> (Value, Vec<WorkflowEvent>) {
    let input = json!({ "items": ["a"], "should_fail": false });
    let id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "renamed_activity".into(),
            input: json!("a"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        },
    ];
    (input, events)
}

/// A history with a CLEAN PREFIX (one matching `process_item` activity) followed
/// by a divergence: the SECOND recorded activity is `renamed_activity`, which the
/// registered `progress_wf` handler (scheduling `process_item` for both items)
/// does not schedule — so replay matches item 1, then diverges at event index 3.
fn diverging_history_with_clean_prefix() -> (Value, Vec<WorkflowEvent>) {
    let input = json!({ "items": ["a", "b"], "should_fail": false });
    let a = ActivityExecId::new();
    let b = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a,
            name: "process_item".into(),
            input: json!("a"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: b,
            name: "renamed_activity".into(),
            input: json!("b"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: b,
            output: Value::Null,
        },
    ];
    (input, events)
}

async fn count_events(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    use diesel::sql_types::BigInt;
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let rows: Vec<Count> =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_events WHERE workflow_exec_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("count events");
    rows.into_iter().next().map_or(0, |c| c.n)
}

async fn count_tasks(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    use diesel::sql_types::BigInt;
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let rows: Vec<Count> = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_task_queue WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("count tasks");
    rows.into_iter().next().map_or(0, |c| c.n)
}

async fn row_state(pool: &DbPool, exec_id: ExecutionId) -> String {
    use diesel::sql_types::Text;
    #[derive(diesel::QueryableByName)]
    struct S {
        #[diesel(sql_type = Text)]
        state: String,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let rows: Vec<S> =
        diesel::sql_query("SELECT state FROM harvest_workflow_executions WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .load(&mut conn)
            .await
            .expect("load state");
    rows.into_iter()
        .next()
        .map_or_else(String::new, |s| s.state)
}

// ── Scenarios ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn completed_run_matching_handler_is_clean() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, mut events) = progress_history(&["a", "b"], false);
    // A real COMPLETED run always seals its history with `WorkflowCompleted`;
    // the seal-gate (issue #614) 410s a terminal execution whose history is
    // truncated before its seal, so seed the realistic terminal history.
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    let exec = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"));
    assert_eq!(body["message"], json!("no divergence under current code"));
    assert_eq!(body["state"], json!("COMPLETED"));
    assert!(body.get("divergence").is_none(), "clean has no divergence");
}

#[tokio::test]
async fn history_diverging_from_handler_is_diverged() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, mut events) = diverging_history();
    // A real COMPLETED run (recorded under old code that scheduled
    // `renamed_activity`) seals with `WorkflowCompleted`; the divergence is
    // detected at event index 1, well before this trailing seal, so the
    // `diverged` verdict is unchanged while the seal-gate (issue #614) is
    // satisfied.
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    let exec = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("diverged"));
    let div = &body["divergence"];
    assert_eq!(div["kind"], json!("ActivityScheduleMismatch"));
    // The divergence is at the first (and only) activity, event index 1 (AC2:
    // exact fields, not just "some string").
    assert_eq!(div["event_index"], json!(1), "div={div}");
    assert_eq!(
        div["expected"],
        json!("ActivityScheduled(process_item)"),
        "handler schedules process_item"
    );
    assert_eq!(
        div["actual"],
        json!("ActivityScheduled(renamed_activity)"),
        "history recorded renamed_activity"
    );
}

#[tokio::test]
async fn failed_run_is_diagnosable_retroactively() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // should_fail=true → the handler returns Err after processing all items, so a
    // faithful replay reports `workflow_failed` (AC4: retroactive forensics on a
    // terminal FAILED run, still a 200).
    let (input, mut events) = progress_history(&["a"], true);
    // A real FAILED run always seals its history with `WorkflowFailed`; the
    // seal-gate (issue #614) 410s a terminal execution truncated before its
    // seal, so seed the realistic terminal history.
    events.push(WorkflowEvent::WorkflowFailed {
        error: "activity failed after processing all items".into(),
        error_type: None,
        details: None,
        non_retryable: None,
    });
    let exec = seed_execution(&pool, "progress_wf", "FAILED", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    // Issue #952: a history sealed by a terminal `WorkflowFailed` that replays to
    // the same failure has NOT diverged — the verdict is `clean`, and the
    // reproduced error is surfaced in `failure` so this endpoint stays the
    // post-mortem surface it was built to be (#614 AC4).
    assert_eq!(body["diagnosis"], json!("clean"));
    assert!(
        body["failure"]["error"].as_str().is_some(),
        "a clean verdict that reproduced the recorded failure must still carry it: {body}"
    );
}

#[tokio::test]
async fn unknown_execution_returns_404() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let missing = ExecutionId::new();
    let (status, _body) = post_diagnosis(&app, &missing.to_string()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn malformed_id_returns_400() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, _body) = post_diagnosis(&app, "not-a-uuid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unregistered_workflow_type_returns_not_registered() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events) = progress_history(&["a"], false);
    let exec = seed_execution(&pool, "ghost_wf", "RUNNING", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("not_registered"));
    assert_eq!(body["events_replayed"], json!(0));
}

#[tokio::test]
async fn classic_dag_returns_not_replayable_dag() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // `classic_dag` is registered as a DAG name (via with_registered_dag_names)
    // but has no `#[workflow]` handler, so the pre-check routes it to the
    // not_replayable_dag verdict.
    let (input, events) = progress_history(&["a"], false);
    let exec = seed_execution(&pool, "classic_dag", "RUNNING", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("not_replayable_dag"));
}

#[tokio::test]
async fn erased_history_returns_410() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, events) = progress_history(&["a"], false);
    let exec = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, true).await;

    let (status, _body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::GONE);
}

/// Issue #614 seal-gate (Codex review): a TERMINAL execution whose recorded
/// event rows are truncated BEFORE its terminal lifecycle event (pruned by
/// retention / released on reset — incomplete, not PII-erased) replays only to
/// a frontier suspension, which canary would report as `clean` — falsely
/// implying "no divergence, safe to rule out" for a run whose real seal we can
/// no longer see. Refuse it with 410, mirroring the #612 terminal-query path
/// (`classify_terminal_query` maps a terminal execution with no seal to
/// `HistoryUnavailable`). Here the state is `TIMED_OUT` (terminal) but the
/// seeded history has NO `WorkflowExecutionTimedOut` seal.
#[tokio::test]
async fn terminal_run_with_truncated_history_returns_410() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let input = json!({ "items": ["a"], "should_fail": false });
    let activity_id = ActivityExecId::new();
    // Seal-less prefix: WorkflowStarted + ActivityScheduled, NO terminal
    // lifecycle event.
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "process_item".into(),
            input: json!("a"),
            queue: "default".into(),
        },
    ];
    let exec = seed_execution(&pool, "progress_wf", "TIMED_OUT", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::GONE, "body={body}");
}

#[tokio::test]
async fn diagnosis_performs_no_writes() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (input, mut events) = progress_history(&["a", "b", "c"], false);
    // A real COMPLETED run always seals its history with `WorkflowCompleted`
    // (issue #614 seal-gate); seed the realistic terminal history.
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    let exec = seed_execution(&pool, "progress_wf", "COMPLETED", input, events, false).await;

    let before_events = count_events(&pool, exec).await;
    let before_state = row_state(&pool, exec).await;
    let before_tasks = count_tasks(&pool, exec).await;

    let (status, _body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK);

    let after_events = count_events(&pool, exec).await;
    let after_state = row_state(&pool, exec).await;
    let after_tasks = count_tasks(&pool, exec).await;
    assert_eq!(before_events, after_events, "diagnosis appended events");
    assert_eq!(before_state, after_state, "diagnosis mutated the row state");
    // AC3: the replay executes no activities/signals — a diagnosis must never
    // enqueue a task-queue row (a spurious enqueue is exactly what a bug causes).
    assert_eq!(
        before_tasks, after_tasks,
        "diagnosis enqueued a task-queue row"
    );
}

#[tokio::test]
async fn nd_blocked_running_run_diagnoses_its_divergence() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // A non-determinism-blocked run stays RUNNING (issue #603). Its history has a
    // CLEAN PREFIX (item 1 replays cleanly) and then diverges under the current
    // handler (item 2 was recorded as `renamed_activity`). The endpoint replays it
    // and reports the divergence AT the point it actually occurs — the headline
    // diagnosability use case. The nd_block_* columns are cosmetic here (the
    // endpoint does not read them); they model the real incident shape.
    let (input, events) = diverging_history_with_clean_prefix();
    let exec = seed_execution(&pool, "progress_wf", "RUNNING", input, events, false).await;
    // Mark it nd-blocked to model the real incident shape.
    {
        let mut conn = pool.get().await.expect("pooled conn");
        diesel::sql_query(
            "UPDATE harvest_workflow_executions \
             SET nd_blocked_at = NOW(), nd_block_reason = 'test', nd_block_count = 1 \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid())
        .execute(&mut conn)
        .await
        .expect("mark nd-blocked");
    }

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("diverged"));
    assert_eq!(body["state"], json!("RUNNING"));
    let div = &body["divergence"];
    assert_eq!(div["kind"], json!("ActivityScheduleMismatch"));
    // The clean prefix (item 1: WorkflowStarted + scheduled + completed) replays,
    // then the divergence lands at the SECOND scheduled activity — event index 3.
    assert_eq!(div["event_index"], json!(3), "div={div}");
    assert_eq!(div["expected"], json!("ActivityScheduled(process_item)"));
    assert_eq!(div["actual"], json!("ActivityScheduled(renamed_activity)"));
}

/// FIX 2 (canary replay mode): a healthy in-flight RUNNING run parked mid-flight
/// — its recorded history stops after the first activity, with no trailing
/// terminal event and no divergence. Under STRICT replay the workflow's next
/// command (schedule item 2) at the frontier is classified as a non-determinism
/// suspension → a FALSE `diverged`. Under CANARY replay (the fix) that frontier
/// suspension is `clean`. This is the endpoint's headline use case and the
/// falsifiable proof of FIX 2 (it fails under strict, passes under canary).
#[tokio::test]
async fn running_healthy_run_parked_mid_activity_is_clean() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Input drives TWO items, but only item 1 has been scheduled + completed:
    // WorkflowStarted + ActivityScheduled(process_item, a) + ActivityCompleted(a).
    // The workflow is parked about to schedule item 2 (the live frontier).
    let input = json!({ "items": ["a", "b"], "should_fail": false });
    let a = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a,
            name: "process_item".into(),
            input: json!("a"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a,
            output: Value::Null,
        },
    ];
    let exec = seed_execution(&pool, "progress_wf", "RUNNING", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"), "body={body}");
    assert_eq!(body["message"], json!("no divergence under current code"));
    assert_eq!(body["state"], json!("RUNNING"));
    assert!(body.get("divergence").is_none(), "clean has no divergence");
}

/// FIX 3 (sealed-mid-await terminal, single-activity shape): a `TIMED_OUT` run
/// whose recorded history is `WorkflowStarted + ActivityScheduled(a) +
/// WorkflowExecutionTimedOut` — the activity was scheduled but never completed
/// before the execution-timeout scanner sealed the run. The workflow replays its
/// one recorded command faithfully and then *suspends* awaiting the in-progress
/// activity's (unrecorded) result, which canary replay reports as `clean`. The
/// run replayed faithfully up to where it was externally sealed — there is no
/// code divergence.
#[tokio::test]
async fn timed_out_run_sealed_mid_activity() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let input = json!({ "items": ["a"], "should_fail": false });
    let a = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a,
            name: "process_item".into(),
            input: json!("a"),
            queue: "default".into(),
        },
        WorkflowEvent::WorkflowExecutionTimedOut {
            deadline: Utc::now(),
            timed_out_at: Utc::now(),
        },
    ];
    let exec = seed_execution(&pool, "progress_wf", "TIMED_OUT", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"), "body={body}");
    assert_eq!(body["state"], json!("TIMED_OUT"));
    assert!(body.get("divergence").is_none());
}

/// FIX 3 (sealed-mid-await terminal, multi-activity shape): a `TIMED_OUT` run whose
/// recorded history is `WorkflowStarted + ActivityScheduled(a) +
/// ActivityCompleted(a) + WorkflowExecutionTimedOut` — item 1 completed, then the
/// run was sealed while awaiting item 2. On replay the workflow consumes item 1
/// and issues the NEXT command (schedule item 2), which lands exactly on the
/// external `WorkflowExecutionTimedOut` seal → a raw `diverged{actual:
/// "WorkflowExecutionTimedOut"}`. The handler's `reclassify_sealed_mid_await`
/// rewrites this false positive to `clean` (terminal state + seal reached +
/// external-seal actual). Proves the reclassification wiring fires end-to-end.
#[tokio::test]
async fn timed_out_run_sealed_awaiting_second_activity_is_clean() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let input = json!({ "items": ["a", "b"], "should_fail": false });
    let a = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a,
            name: "process_item".into(),
            input: json!("a"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a,
            output: Value::Null,
        },
        WorkflowEvent::WorkflowExecutionTimedOut {
            deadline: Utc::now(),
            timed_out_at: Utc::now(),
        },
    ];
    let exec = seed_execution(&pool, "progress_wf", "TIMED_OUT", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"), "body={body}");
    assert_eq!(body["message"], json!("no divergence under current code"));
    assert_eq!(body["state"], json!("TIMED_OUT"));
    assert!(body.get("divergence").is_none(), "reclassified to clean");
}

/// PR #1107 (Codex review): the diagnosis replay must thread the runtime
/// registry's shared state into `WorkflowReplayer`, exactly as the live worker
/// replays with `registry.shared_state()`. `state_reading_wf` reads
/// `ctx.state::<DiagConfig>()` and completes cleanly only when that state is
/// present; its recorded history is a clean `WorkflowStarted + WorkflowCompleted`.
///
/// WITH the fix the diagnosis replayer receives the registry's `DiagConfig`, so
/// `ctx.state` returns `Some` → the workflow completes matching history → `clean`.
/// WITHOUT the fix the replayer defaults to `empty_shared_state()`, so
/// `ctx.state` returns `None` → the workflow returns `Err` → a spurious
/// `workflow_failed` verdict, and this assertion fails. It therefore fails
/// against the pre-fix code and passes against the fix.
#[tokio::test]
async fn state_registering_workflow_diagnoses_clean_with_registry_shared_state() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with_state(&pool);

    let input = json!({});
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCompleted {
            output: json!({ "greeting": "hello" }),
        },
    ];
    let exec = seed_execution(&pool, "state_reading_wf", "COMPLETED", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"), "body={body}");
    assert!(body.get("divergence").is_none(), "clean has no divergence");
    assert!(body.get("failure").is_none(), "clean has no failure");
}

/// Issue #614 regression: the diagnosis replay must thread the runtime registry's
/// history policy (`registry.history_policy()`) into the replayer, exactly as the
/// live worker does. `policy_branch_wf` schedules `checkpoint` when
/// `ctx.should_continue_as_new()` fires and `keep_going` otherwise; the trigger
/// reads the policy's `continue_as_new_threshold`. This deployment tunes the
/// threshold to 2, and the seeded 4-event history (> 2) was recorded by the
/// `checkpoint` branch. A faithful diagnosis replay under the SAME policy trips
/// the trigger and schedules `checkpoint`, matching history → `clean`.
///
/// WITHOUT the fix the replayer defaults to `WorkflowHistoryPolicy::default()`
/// (a large threshold the 4-event history never exceeds), so
/// `should_continue_as_new()` returns `false` → the workflow schedules
/// `keep_going` → the replay diverges from the recorded `checkpoint` at event
/// index 1 → a spurious `diverged` verdict, and this assertion fails. It
/// therefore fails against the pre-fix code and passes against the fix.
#[tokio::test]
async fn policy_branching_workflow_diagnoses_clean_with_registry_history_policy() {
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app_with_history_policy(&pool);

    // 4 recorded events (> the configured threshold of 2), recorded by the
    // `checkpoint` branch: WorkflowStarted, ActivityScheduled(checkpoint),
    // ActivityCompleted, WorkflowCompleted.
    let input = json!({});
    let activity_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "checkpoint".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    let exec = seed_execution(&pool, "policy_branch_wf", "COMPLETED", input, events, false).await;

    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;
    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(body["diagnosis"], json!("clean"), "body={body}");
    assert!(body.get("divergence").is_none(), "clean has no divergence");
    assert!(body.get("failure").is_none(), "clean has no failure");
}

// ─── Issue #608 read-path decode gate (PR #1107 Codex review, finding #5) ─────
//
// The merged endpoint decoded history with the runtime's codecs UNCONDITIONALLY,
// so on a non-identity-codec deployment it (a) decoded encrypted payloads even
// with the operator's read-path decode gate OFF (surfacing plaintext via
// failure/divergence fields) and (b) wrote no `payload.decode_read` audit row.
// The fix gates the codec decode behind `read_path_decoder`
// (`decode_payloads_on_read` + admin) and audits when it decodes. These tests
// exercise a genuine non-identity codec + both decode-flag states. Identity
// deployments (all the tests above) hit the decode-OFF identity-load branch and
// are byte-identical to the merged behavior — confirmed by their unchanged pass.

/// A trivial invertible non-identity codec (byte reversal). Encoding a payload
/// produces a `_harvest_codec_envelope` with `codec_id = "test-reverse"`, so a
/// history seeded with it is only decodable by a `PayloadCodecs` that has this
/// codec registered — the identity default hard-errors `UnknownPayloadCodec`.
#[derive(Clone, Copy)]
struct ReverseCodec;

impl PayloadCodec for ReverseCodec {
    fn codec_id(&self) -> &'static str {
        "test-reverse"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().rev().copied().collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().rev().copied().collect())
    }
}

fn reverse_codecs() -> PayloadCodecs {
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(ReverseCodec));
    codecs
}

/// Like [`build_app`] but installs a non-identity codec registry and toggles the
/// `decode_payloads_on_read` gate — the two levers finding #5 exercises.
fn build_app_with_codecs(pool: &DbPool, decode_on: bool, codecs: PayloadCodecs) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.set_payload_codecs(codecs);
    api_state.set_decode_payloads_on_read(decode_on);
    let runtime = HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![progress_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("replay-diagnosis-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    );
    api_state.install(runtime);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Seed an execution whose event payloads are codec-ENCODED (envelopes), using
/// [`PayloadCodecs::encode_event`] — the same encoding the live write path
/// produces for a non-identity deployment. The execution row `input` is left
/// plaintext (the diagnosis reads `history.events`, not the row input, and the
/// erased-guard only looks for the tombstone marker).
async fn seed_encoded_execution(
    pool: &DbPool,
    workflow_name: &str,
    state: &str,
    input: Value,
    events: Vec<WorkflowEvent>,
    codecs: &PayloadCodecs,
) -> ExecutionId {
    let mut conn = pool.get().await.expect("pooled conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, $2, $3, 0, $4, 'default', $5)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(input)
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(&mut conn)
    .await
    .expect("seed execution");

    for (i, event) in events.iter().enumerate() {
        let encoded = codecs
            .encode_event(event)
            .expect("encode event into envelope");
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let event_id = i as i32;
        diesel::sql_query(
            "INSERT INTO harvest_events \
             (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             VALUES ($1, $2, $3, $4, now())",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Integer, _>(event_id)
        .bind::<diesel::sql_types::Text, _>(event.type_name())
        .bind::<diesel::sql_types::Jsonb, _>(encoded)
        .execute(&mut conn)
        .await
        .expect("seed encoded event");
    }
    exec_id
}

/// Count `payload.decode_read` audit rows recorded for an execution (issue #608
/// AC8).
async fn count_decode_audit_rows(pool: &DbPool, exec_id: ExecutionId) -> i64 {
    use diesel::sql_types::BigInt;
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.expect("pooled conn");
    let rows: Vec<Count> = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_audit_log \
         WHERE operation = 'payload.decode_read' AND target_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .load(&mut conn)
    .await
    .expect("count audit rows");
    rows.into_iter().next().map_or(0, |c| c.n)
}

#[tokio::test]
async fn encrypted_history_decode_on_diagnoses_clean_and_writes_audit_row() {
    // Encrypted deployment + decode-on-read ENABLED (admin): the endpoint decodes
    // the codec-encoded history exactly as the live worker does, so it replays
    // clean, AND writes a `payload.decode_read` audit row (issue #608 AC8).
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);

    let (input, mut events) = progress_history(&["a"], false);
    // A real COMPLETED run seals its history; the seal-gate (issue #614) 410s a
    // terminal execution truncated before its seal, so seed the realistic
    // terminal history (encoded, like every other event).
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    let exec = seed_encoded_execution(
        &pool,
        "progress_wf",
        "COMPLETED",
        input,
        events,
        &reverse_codecs(),
    )
    .await;

    let app = build_app_with_codecs(&pool, true, reverse_codecs());
    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;

    assert_eq!(status, StatusCode::OK, "body={body}");
    assert_eq!(
        body["diagnosis"],
        json!("clean"),
        "an encrypted history must decode and replay clean under decode-on; body={body}"
    );
    assert!(
        count_decode_audit_rows(&pool, exec).await >= 1,
        "decode-on must write a payload.decode_read audit row (issue #608 AC8)"
    );
}

#[tokio::test]
async fn encrypted_history_decode_off_returns_honest_410_and_no_audit_row() {
    // Encrypted deployment + decode-on-read DISABLED (the default): the endpoint
    // must NOT decode. It loads with the identity codec (no gate), which
    // hard-errors on the reverse envelopes, and returns an HONEST 410 pointing at
    // decode-on-read — never leaking ciphertext into a verdict, never 503-ing —
    // and writes NO `payload.decode_read` audit row.
    let (url, _c) = setup_database().await;
    let pool = build_pool(&url);

    let (input, mut events) = progress_history(&["a"], false);
    // A real COMPLETED run seals its history; the seal-gate (issue #614) 410s a
    // terminal execution truncated before its seal, so seed the realistic
    // terminal history (encoded, like every other event).
    events.push(WorkflowEvent::WorkflowCompleted {
        output: Value::Null,
    });
    let exec = seed_encoded_execution(
        &pool,
        "progress_wf",
        "COMPLETED",
        input,
        events,
        &reverse_codecs(),
    )
    .await;

    // Codecs installed but the ONLY difference from the test above is the flag.
    let app = build_app_with_codecs(&pool, false, reverse_codecs());
    let (status, body) = post_diagnosis(&app, &exec.to_string()).await;

    assert_eq!(
        status,
        StatusCode::GONE,
        "encrypted + decode-off must be an honest 410, not a 503 or a leaked verdict; body={body}"
    );
    let text = body.to_string();
    assert!(
        text.contains("decode-on-read") || text.contains("decoding is required"),
        "the 410 body must point the operator at decode-on-read; body={body}"
    );
    assert_eq!(
        count_decode_audit_rows(&pool, exec).await,
        0,
        "decode-off must NOT write a payload.decode_read audit row"
    );
}
