//! Axum management routes for Harvest workflows and DAGs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::{get, patch, post};
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dlq;
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::models::{DagRun, DeadLetter, HarvestSchedule, WorkflowExecution};
use autumn_harvest::scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerSnapshot, trigger_dag,
};
use autumn_harvest::schema::{harvest_dag_runs, harvest_schedules, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::signal;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, cancel_workflow_execution, start_or_load_workflow_execution,
};

use crate::state::HarvestDbPool;

#[derive(Clone)]
pub struct HarvestApiRuntime {
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    worker_id: Option<String>,
    queues: Vec<String>,
    scheduler: SchedulerMonitor,
    router: ShardRouter,
}

impl HarvestApiRuntime {
    /// Build an API runtime snapshot from the available Harvest registrations
    /// and any locally owned worker/scheduler state.
    #[must_use]
    pub const fn new(
        registry: Arc<HandlerRegistry>,
        dags: Arc<DagCatalog>,
        worker_id: Option<String>,
        queues: Vec<String>,
        scheduler: SchedulerMonitor,
        router: ShardRouter,
    ) -> Self {
        Self {
            registry,
            dags,
            worker_id,
            queues,
            scheduler,
            router,
        }
    }

    /// Shard router used to pick a destination for new workflows.
    #[must_use]
    pub const fn router(&self) -> &ShardRouter {
        &self.router
    }
}

#[derive(Clone, Default)]
pub struct HarvestApiState {
    runtime: Arc<Mutex<Option<HarvestApiRuntime>>>,
    storage_pool: Arc<Mutex<Option<HarvestDbPool>>>,
}

impl HarvestApiState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the currently running Harvest runtime snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn install(&self, runtime: HarvestApiRuntime) {
        *self
            .runtime
            .lock()
            .expect("harvest api state lock poisoned") = Some(runtime);
    }

    /// Install the Harvest storage pool used by management routes.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn install_storage_pool(&self, pool: HarvestDbPool) {
        *self
            .storage_pool
            .lock()
            .expect("harvest api state lock poisoned") = Some(pool);
    }

    /// Clear the currently running Harvest runtime snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn clear(&self) {
        *self
            .runtime
            .lock()
            .expect("harvest api state lock poisoned") = None;
        *self
            .storage_pool
            .lock()
            .expect("harvest api state lock poisoned") = None;
    }

    fn runtime(&self) -> HarvestResult<HarvestApiRuntime> {
        self.runtime
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
            .ok_or_else(|| HarvestError::Config("harvest runtime is not started".to_string()))
    }

    fn storage_pool(&self) -> HarvestResult<HarvestDbPool> {
        self.storage_pool
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
            .ok_or_else(|| {
                HarvestError::Config("harvest storage pool is not configured".to_string())
            })
    }
}

#[derive(Debug, Serialize)]
struct WorkflowDetailsResponse {
    execution: WorkflowExecution,
    history: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct StartWorkflowResponse {
    execution_id: String,
    workflow_name: String,
    workflow_id: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct BasicAck {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct DagSummary {
    name: String,
    schedule_expr: Option<String>,
    is_paused: bool,
    next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    max_active_runs: i32,
    catchup: bool,
    task_count: usize,
}

#[derive(Debug, Serialize)]
struct HarvestHealth {
    runtime_ready: bool,
    worker_id: Option<String>,
    queues: Vec<String>,
    dag_count: usize,
    scheduler: SchedulerSnapshot,
}

#[derive(Debug, Serialize)]
struct ReplayDeadLetterResponse {
    ok: bool,
    dead_letter_id: String,
    task_id: String,
}

#[derive(Debug, Serialize)]
struct CancelWorkflowResponse {
    ok: bool,
    execution_id: String,
    state: String,
    reason: String,
    newly_cancelled: bool,
    failed_task_count: usize,
}

#[derive(Debug, Deserialize)]
struct StartWorkflowRequest {
    workflow_id: Option<String>,
    input: Option<Value>,
    queue: Option<String>,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    execution_timeout_secs: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DagTriggerRequest {
    conf: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CancelWorkflowRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DagPauseRequest {
    paused: bool,
}

#[derive(Debug, Deserialize)]
struct WorkflowListQuery {
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DeadLetterListQuery {
    limit: Option<i64>,
}

pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    Router::new()
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{workflow_name}/start", post(start_workflow))
        .route("/workflows/{id}/cancel", post(cancel_workflow))
        .route(
            "/workflows/{id}/signal/{signal_name}",
            post(signal_workflow),
        )
        .route("/workflows/{id}/query/{query_name}", get(query_workflow))
        .route("/dags", get(list_dags))
        .route("/dags/{dag_name}/runs", get(list_dag_runs))
        .route("/dags/{dag_name}/trigger", post(trigger_dag_run))
        .route("/dags/{dag_name}", patch(patch_dag))
        .route("/dead-letters", get(list_dead_letters))
        .route("/dead-letters/{id}/replay", post(replay_dead_letter))
        .route("/health", get(health))
        .layer(Extension(api_state))
}

async fn list_workflows(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<Vec<WorkflowExecution>>, AutumnError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let workflows = load_workflows_from_shards(&api_state, None, limit).await?;
    Ok(Json(workflows))
}

async fn get_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowDetailsResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let events = history
        .events
        .into_iter()
        .map(|event| serde_json::to_value(event).map_err(HarvestError::from))
        .collect::<HarvestResult<Vec<_>>>()
        .map_err(map_error)?;

    Ok(Json(WorkflowDetailsResponse {
        execution,
        history: events,
    }))
}

async fn start_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    Json(request): Json<StartWorkflowRequest>,
) -> Result<(axum::http::StatusCode, Json<StartWorkflowResponse>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    if !runtime.registry.workflows.contains_key(&workflow_name) {
        return Err(AutumnError::not_found_msg(format!(
            "workflow '{workflow_name}'"
        )));
    }

    let workflow_id = request
        .workflow_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let queue_name = request
        .queue
        .or_else(|| runtime.queues.as_slice().first().cloned())
        .unwrap_or_else(|| "default".to_string());
    let input = request.input.unwrap_or(Value::Null);

    let shard = runtime
        .router
        .pick_for_new_workflow(&workflow_name, &workflow_id);
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = db_conn_for_shard(&api_state, shard).await?;

    let start = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: request
                .execution_timeout_secs
                .map(chrono::Duration::seconds),
            memo: request.memo.clone(),
            search_attrs: request.search_attrs.clone(),
        },
    )
    .await
    .map_err(map_error)?;

    Ok((
        if start.created {
            axum::http::StatusCode::CREATED
        } else {
            axum::http::StatusCode::OK
        },
        Json(StartWorkflowResponse {
            execution_id: start.exec_id.to_string(),
            workflow_name: start.workflow_name,
            workflow_id: start.workflow_id,
            state: start.state,
        }),
    ))
}

async fn cancel_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Json(request): Json<CancelWorkflowRequest>,
) -> Result<(axum::http::StatusCode, Json<CancelWorkflowResponse>), AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let reason = request
        .reason
        .as_deref()
        .unwrap_or("workflow cancellation requested");
    let cancelled = cancel_workflow_execution(&mut conn, exec_id, reason)
        .await
        .map_err(map_error)?;

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(CancelWorkflowResponse {
            ok: true,
            execution_id: cancelled.exec_id.to_string(),
            state: cancelled.state,
            reason: cancelled.reason,
            newly_cancelled: cancelled.newly_cancelled,
            failed_task_count: cancelled.failed_task_count,
        }),
    ))
}

async fn signal_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, signal_name)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<(axum::http::StatusCode, Json<BasicAck>), AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    signal::send_signal(&mut conn, exec_id, &signal_name, payload)
        .await
        .map_err(map_error)?;

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(BasicAck { ok: true }),
    ))
}

async fn query_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, query_name)): Path<(String, String)>,
) -> Result<Json<Value>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let workflow = runtime
        .registry
        .workflows
        .get(&execution.workflow_name)
        .ok_or_else(|| {
            AutumnError::not_found_msg(format!(
                "workflow handler '{}' is not registered",
                execution.workflow_name
            ))
        })?;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;

    let ctx = WorkflowContext::for_replay_with_state(
        exec_id,
        history.events,
        runtime.registry.shared_state(),
    );
    let _ = tokio::time::timeout(
        Duration::from_millis(100),
        (workflow.handler)(&ctx, execution.input.clone()),
    )
    .await;

    ctx.execute_query(&query_name).map(Json).map_err(map_error)
}

async fn list_dags(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<DagSummary>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let schedules = load_schedules_from_shards(&api_state).await?;

    let dags = schedules
        .into_iter()
        .map(|schedule| DagSummary {
            name: schedule.dag_name.clone(),
            schedule_expr: schedule.schedule_expr.clone(),
            is_paused: schedule.is_paused,
            next_run_at: schedule.next_run_at,
            max_active_runs: schedule.max_active_runs,
            catchup: schedule.catchup,
            task_count: runtime
                .dags
                .get(&schedule.dag_name)
                .map_or(0, RegisteredDag::task_count),
        })
        .collect();

    Ok(Json(dags))
}

async fn list_dag_runs(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
) -> Result<Json<Vec<DagRun>>, AutumnError> {
    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;
    let runs = harvest_dag_runs::table
        .filter(harvest_dag_runs::dag_name.eq(&dag_name))
        .order(harvest_dag_runs::created_at.desc())
        .select(DagRun::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(Json(runs))
}

async fn trigger_dag_run(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    Json(request): Json<DagTriggerRequest>,
) -> Result<(axum::http::StatusCode, Json<DagRun>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let shard = runtime.router.pick_for_dag(&dag_name);
    let run = trigger_dag(
        pool.pool_for(shard).clone(),
        Arc::clone(&runtime.registry),
        Arc::clone(&runtime.dags),
        &dag_name,
        request.conf,
        runtime.scheduler,
    )
    .await
    .map_err(map_error)?;
    Ok((axum::http::StatusCode::CREATED, Json(run)))
}

async fn patch_dag(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    Json(request): Json<DagPauseRequest>,
) -> Result<Json<HarvestSchedule>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;
    let updated = diesel::update(dsl::harvest_schedules.filter(dsl::dag_name.eq(&dag_name)))
        .set((
            dsl::is_paused.eq(request.paused),
            dsl::updated_at.eq(chrono::Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    if updated == 0 {
        return Err(AutumnError::not_found_msg(format!("dag '{dag_name}'")));
    }

    let schedule = dsl::harvest_schedules
        .filter(dsl::dag_name.eq(&dag_name))
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(Json(schedule))
}

async fn list_dead_letters(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<DeadLetterListQuery>,
) -> Result<Json<Vec<DeadLetter>>, AutumnError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let dead_letters = load_dead_letters_from_shards(&api_state, limit).await?;
    Ok(Json(dead_letters))
}

async fn replay_dead_letter(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<(axum::http::StatusCode, Json<ReplayDeadLetterResponse>), AutumnError> {
    let dead_letter_id = parse_uuid(&id, "dead-letter id")?;
    let task_id = replay_dead_letter_from_shards(&api_state, dead_letter_id).await?;

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(ReplayDeadLetterResponse {
            ok: true,
            dead_letter_id: dead_letter_id.to_string(),
            task_id: task_id.to_string(),
        }),
    ))
}

async fn health(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<HarvestHealth>, AutumnError> {
    let runtime = api_state.runtime().ok();
    let scheduler = runtime
        .as_ref()
        .map_or_else(SchedulerMonitor::offline, |runtime| {
            runtime.scheduler.clone()
        })
        .snapshot();

    Ok(Json(HarvestHealth {
        runtime_ready: runtime.is_some(),
        worker_id: runtime
            .as_ref()
            .and_then(|runtime| runtime.worker_id.clone()),
        queues: runtime
            .as_ref()
            .map_or_else(Vec::new, |runtime| runtime.queues.clone()),
        dag_count: runtime.as_ref().map_or(0, |runtime| runtime.dags.len()),
        scheduler,
    }))
}

pub(crate) async fn load_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

pub(crate) type PoolConn = deadpool::managed::Object<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

fn map_pool_error(error: impl ToString) -> AutumnError {
    AutumnError::service_unavailable_msg(error.to_string())
}

async fn acquire_conn(pool: &DbPool) -> Result<PoolConn, AutumnError> {
    pool.get().await.map_err(map_pool_error)
}

pub(crate) async fn db_conn_for_execution(
    api_state: &HarvestApiState,
    exec_id: ExecutionId,
) -> Result<PoolConn, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    acquire_conn(pool.pool_for_execution(exec_id)).await
}

async fn db_conn_for_shard(
    api_state: &HarvestApiState,
    shard: ShardId,
) -> Result<PoolConn, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    acquire_conn(pool.pool_for(shard)).await
}

async fn db_conn_for_dag(
    api_state: &HarvestApiState,
    dag_name: &str,
) -> Result<PoolConn, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    db_conn_for_shard(api_state, runtime.router.pick_for_dag(dag_name)).await
}

pub(crate) async fn load_workflows(
    conn: &mut AsyncPgConnection,
    state_filter: Option<&str>,
    limit: i64,
) -> HarvestResult<Vec<WorkflowExecution>> {
    let mut query = harvest_workflow_executions::table
        .into_boxed()
        .order(harvest_workflow_executions::created_at.desc())
        .limit(limit);
    if let Some(state) = state_filter {
        query = query.filter(harvest_workflow_executions::state.eq(state.to_string()));
    }
    query
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

pub(crate) async fn load_workflows_from_shards(
    api_state: &HarvestApiState,
    state_filter: Option<&str>,
    limit: i64,
) -> Result<Vec<WorkflowExecution>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut workflows = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = load_workflows(&mut conn, state_filter, limit)
            .await
            .map_err(map_error)?;
        workflows.append(&mut rows);
    }

    workflows.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    workflows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    Ok(workflows)
}

async fn load_schedules_from_shards(
    api_state: &HarvestApiState,
) -> Result<Vec<HarvestSchedule>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut schedules = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = harvest_schedules::table
            .order(harvest_schedules::dag_name.asc())
            .select(HarvestSchedule::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;
        schedules.append(&mut rows);
    }

    schedules.sort_by(|left, right| {
        left.dag_name
            .cmp(&right.dag_name)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(schedules)
}

async fn load_dead_letters_from_shards(
    api_state: &HarvestApiState,
    limit: i64,
) -> Result<Vec<DeadLetter>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut dead_letters = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = dlq::list_dead_letters(&mut conn, limit)
            .await
            .map_err(map_error)?;
        dead_letters.append(&mut rows);
    }

    dead_letters.sort_by(|left, right| {
        right
            .failed_at
            .cmp(&left.failed_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    dead_letters.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    Ok(dead_letters)
}

async fn replay_dead_letter_from_shards(
    api_state: &HarvestApiState,
    dead_letter_id: uuid::Uuid,
) -> Result<uuid::Uuid, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        match dlq::replay_dead_letter(&mut conn, dead_letter_id).await {
            Ok(task_id) => return Ok(task_id),
            Err(HarvestError::NotFound(_)) => continue,
            Err(error) => return Err(map_error(error)),
        }
    }

    Err(AutumnError::not_found_msg(format!(
        "dead-letter {dead_letter_id}"
    )))
}

pub(crate) fn parse_execution_id(raw: &str) -> Result<ExecutionId, AutumnError> {
    raw.parse::<ExecutionId>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid execution id '{raw}'")))
}

fn parse_uuid(raw: &str, label: &str) -> Result<uuid::Uuid, AutumnError> {
    raw.parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid {label} '{raw}'")))
}

pub(crate) fn map_error(error: HarvestError) -> AutumnError {
    match error {
        HarvestError::NotFound(message) => AutumnError::not_found_msg(message),
        HarvestError::Config(message)
        | HarvestError::NonDeterministic(message)
        | HarvestError::Cancelled(message)
        | HarvestError::WorkflowFailed {
            name: _,
            reason: message,
        } => AutumnError::bad_request_msg(message),
        HarvestError::Database(message) => AutumnError::service_unavailable_msg(message),
        other => AutumnError::service_unavailable_msg(other.to_string()),
    }
}
