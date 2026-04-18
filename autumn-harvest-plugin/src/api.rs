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
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::models::{DagRun, HarvestSchedule, WorkflowExecution};
use autumn_harvest::scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerSnapshot, trigger_dag,
};
use autumn_harvest::schema::{harvest_dag_runs, harvest_schedules, harvest_workflow_executions};
use autumn_harvest::signal;
use autumn_harvest::store;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};

use crate::state::HarvestDbPool;

#[derive(Clone)]
pub struct HarvestApiRuntime {
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    worker_id: Option<String>,
    queues: Vec<String>,
    scheduler: SchedulerMonitor,
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
    ) -> Self {
        Self {
            registry,
            dags,
            worker_id,
            queues,
            scheduler,
        }
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
struct DagPauseRequest {
    paused: bool,
}

#[derive(Debug, Deserialize)]
struct WorkflowListQuery {
    limit: Option<i64>,
}

pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    Router::new()
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{workflow_name}/start", post(start_workflow))
        .route(
            "/workflows/{id}/signal/{signal_name}",
            post(signal_workflow),
        )
        .route("/workflows/{id}/query/{query_name}", get(query_workflow))
        .route("/dags", get(list_dags))
        .route("/dags/{dag_name}/runs", get(list_dag_runs))
        .route("/dags/{dag_name}/trigger", post(trigger_dag_run))
        .route("/dags/{dag_name}", patch(patch_dag))
        .route("/health", get(health))
        .layer(Extension(api_state))
}

async fn list_workflows(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<WorkflowListQuery>,
) -> Result<Json<Vec<WorkflowExecution>>, AutumnError> {
    let mut conn = db_conn(&api_state).await?;
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let workflows = harvest_workflow_executions::table
        .order(harvest_workflow_executions::created_at.desc())
        .limit(limit)
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(Json(workflows))
}

async fn get_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowDetailsResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn(&api_state).await?;
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

    let mut conn = db_conn(&api_state).await?;
    let workflow_id = request
        .workflow_id
        .unwrap_or_else(|| ExecutionId::new().to_string());
    let queue_name = request
        .queue
        .or_else(|| runtime.queues.as_slice().first().cloned())
        .unwrap_or_else(|| "default".to_string());
    let input = request.input.unwrap_or(Value::Null);

    let start = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &workflow_name,
            workflow_id: &workflow_id,
            shard_id: 0,
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

async fn signal_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, signal_name)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<(axum::http::StatusCode, Json<BasicAck>), AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn(&api_state).await?;
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
    let mut conn = db_conn(&api_state).await?;
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
    let mut conn = db_conn(&api_state).await?;
    let schedules = harvest_schedules::table
        .order(harvest_schedules::dag_name.asc())
        .select(HarvestSchedule::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

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
    let mut conn = db_conn(&api_state).await?;
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
    let run = trigger_dag(
        pool.clone_inner(),
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

    let mut conn = db_conn(&api_state).await?;
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

async fn load_execution(
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

async fn db_conn(
    api_state: &HarvestApiState,
) -> Result<
    deadpool::managed::Object<
        diesel_async::pooled_connection::AsyncDieselConnectionManager<
            diesel_async::AsyncPgConnection,
        >,
    >,
    AutumnError,
> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let conn = pool
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    Ok(conn)
}

fn parse_execution_id(raw: &str) -> Result<ExecutionId, AutumnError> {
    raw.parse::<ExecutionId>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid execution id '{raw}'")))
}

fn map_error(error: HarvestError) -> AutumnError {
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
