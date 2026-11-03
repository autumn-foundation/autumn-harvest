//! The management API on the SQLite backend (issue #2467).
//!
//! It serves the core routes with the Postgres paths, status codes and field
//! names (issue #2489). A body carries the fields this backend stores, so it
//! is a subset of the Postgres body. A route that has no SQLite port answers
//! `501` and names the epic. It never answers an empty `200`, so a client
//! cannot mistake a missing feature for an empty result.
//!
//! `tests/support/wire_contract.rs` holds the checks of the contract. The
//! SQLite suite and the Postgres integration tests both run them.
//!
//! A request option that this backend cannot honour, such as `queue` or
//! `start_at`, is a `400` for the same reason.
//!
//! The #1802 mutation gate wraps the router, as on Postgres. Outside `dev`, a
//! mutating call needs a declared auth boundary, an admin session or the
//! explicit opt-out.
//!
//! With `sqlite_per_tenant`, `/tenants/{key}/..` serves the same routes on
//! one fleet tenant (issue #2472). `{key}` is the app's routing key. The gate
//! covers these routes too.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use autumn_harvest::ExecutionId;
use autumn_harvest_sqlite::{
    ExecutionDescription, ExecutionFilter, ExecutionSummary, HistoryFilter, ListCursor,
    SqliteError, StartOptions, Stored,
};
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    EPIC_ISSUE, HarvestFleet, HarvestSqlite, HarvestSqliteError, SignalWithStartParams, StopAck,
};
use crate::api::HarvestApiState;

/// The issue that adds per-tenant Harvest.
const TENANT_ISSUE: u32 = 2472;

tokio::task_local! {
    /// The tenant Harvest that serves the current tenant request.
    static TENANT: HarvestSqlite;
}

/// How often a `/result` long poll reads the file. SQLite has no `LISTEN`.
const RESULT_POLL: Duration = Duration::from_millis(100);

/// The time that `/health/ready` gives its read of the file, as on Postgres.
const READY_BUDGET: Duration = Duration::from_secs(1);

/// What every handler reads.
#[derive(Clone)]
pub struct ApiState {
    /// Filled by the startup hook. A request before it gets `503`.
    pub handle: Arc<OnceLock<HarvestSqlite>>,
    /// Filled by the startup hook when `sqlite_per_tenant` is on.
    pub fleet: Arc<OnceLock<HarvestFleet>>,
    /// The flags that the #1802 mutation gate reads.
    pub flags: HarvestApiState,
}

impl ApiState {
    /// The tenant Harvest of a tenant request, else the control Harvest.
    fn handle(&self) -> Result<HarvestSqlite, AutumnError> {
        if let Ok(handle) = TENANT.try_with(Clone::clone) {
            return Ok(handle);
        }
        self.handle.get().cloned().ok_or_else(|| {
            AutumnError::service_unavailable_msg("the Harvest SQLite backend is not running yet")
        })
    }
}

/// The SQLite management router, gated like the Postgres one.
pub fn router(state: ApiState) -> Router<()> {
    let gate = middleware::from_fn_with_state(
        state.flags.clone(),
        crate::api::require_mutation_auth_by_method,
    );
    let scope = middleware::from_fn_with_state(state.clone(), tenant_scope);
    routes()
        .nest("/tenants/{tenant}", routes().layer(scope))
        .layer(gate)
        .with_state(state)
}

/// The `{tenant}` segment of a tenant route. The other segments are ignored.
#[derive(Deserialize)]
struct TenantParams {
    tenant: String,
}

/// Serve one request on the tenant that `{tenant}` routes to.
///
/// The tenant handle holds a lease, so the tenant stays open until the
/// response is ready. Each handler reads it through [`ApiState::handle`].
/// A tenant that does not exist is a `404`. The API never creates one.
async fn tenant_scope(
    State(state): State<ApiState>,
    Path(TenantParams { tenant }): Path<TenantParams>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let Some(fleet) = state.fleet.get() else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "per-tenant Harvest is off: call HarvestPlugin::sqlite_per_tenant",
                "backend": "sqlite",
                "issue": format!("https://github.com/autumn-foundation/autumn-harvest/issues/{TENANT_ISSUE}"),
            })),
        )
            .into_response();
    };
    match fleet.open_existing_for(&tenant).await {
        Ok(handle) => TENANT.scope(handle, next.run(request)).await,
        Err(error) => map_error(error).into_response(),
    }
}

/// The core routes, on the control Harvest or on one tenant.
fn routes() -> Router<ApiState> {
    // A route that reads no query parameter refuses one. A Postgres client
    // may send `?after=` or `?event_type=` here. Answering it with the full,
    // unfiltered body would look like success (#2489).
    let no_query = middleware::from_fn(refuse_query_parameters);
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/workflows", get(list_workflows))
        .route(
            "/workflows/registered",
            get(list_registered).route_layer(no_query.clone()),
        )
        .route(
            "/workflows/{id}",
            get(get_workflow).route_layer(no_query.clone()),
        )
        .route("/workflows/{id}/history", get(get_history))
        .route("/workflows/{id}/result", get(get_result))
        .route(
            "/workflows/{id}/cancel",
            post(cancel).route_layer(no_query.clone()),
        )
        .route(
            "/workflows/{id}/terminate",
            post(terminate).route_layer(no_query.clone()),
        )
        .route("/workflows/{id}/signal/{signal_name}", post(signal))
        .route(
            "/workflows/{id}/query/{query_name}",
            get(query_get)
                .post(query_post)
                .route_layer(no_query.clone()),
        )
        .route("/workflows/{id}/update/{update_name}", post(update))
        .route(
            "/workflows/{workflow_name}/start",
            post(start).route_layer(no_query.clone()),
        )
        .route(
            "/workflows/{workflow_name}/signal-with-start",
            post(signal_with_start).route_layer(no_query.clone()),
        )
        .route(
            "/workflows/by-id/{workflow_name}/{workflow_id}",
            get(get_by_id).route_layer(no_query.clone()),
        )
        .route(
            "/workflows/by-id/{workflow_name}/{workflow_id}/result",
            get(get_result_by_id),
        )
        // `matchit` binds no empty last segment. These two shapes reach a
        // handler only through their own routes, so they answer the Postgres
        // `400`, not the catch-all `501` (issue #1353).
        .route(
            "/workflows/by-id/{workflow_name}",
            get(by_id_without_id).post(by_id_without_id),
        )
        .route(
            "/workflows/by-id/{workflow_name}/",
            get(by_id_with_empty_id).route_layer(no_query),
        )
        .route(
            "/workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}",
            post(signal_by_id),
        )
        // `AppBuilder::nest` drops a nested fallback, so a catch-all route
        // stands in for it. A static or a parameter route still wins.
        .route("/{*rest}", any(not_ported))
}

// ── Path segments ────────────────────────────────────────────────────────────
//
// Each handler names its segments. A tenant route also carries `{tenant}`,
// which a tuple extractor would count as one more segment.

#[derive(Deserialize)]
struct IdParams {
    id: String,
}

#[derive(Deserialize)]
struct NameParams {
    workflow_name: String,
}

#[derive(Deserialize)]
struct ByIdParams {
    workflow_name: String,
    workflow_id: String,
}

#[derive(Deserialize)]
struct SignalParams {
    id: String,
    signal_name: String,
}

#[derive(Deserialize)]
struct ByIdSignalParams {
    workflow_name: String,
    workflow_id: String,
    signal_name: String,
}

#[derive(Deserialize)]
struct QueryParams {
    id: String,
    query_name: String,
}

#[derive(Deserialize)]
struct UpdateParams {
    id: String,
    update_name: String,
}

// ── Errors ───────────────────────────────────────────────────────────────────

/// The HTTP form of a backend error.
fn map_error(error: HarvestSqliteError) -> AutumnError {
    let error = match error {
        HarvestSqliteError::Sqlite(error) => error,
        HarvestSqliteError::InvalidInput(_) | HarvestSqliteError::InvalidSignal(_) => {
            return AutumnError::bad_request_msg(error.to_string());
        }
        // The fleet gives the status of a tenant key and an open. A bad key
        // is `400`. A missing tenant is `404`. A tenant in a delete is `410`.
        HarvestSqliteError::Fleet(error) => {
            let status = error.http_status();
            return AutumnError::service_unavailable_msg(error.to_string()).with_status(status);
        }
        other => return AutumnError::service_unavailable_msg(other.to_string()),
    };
    let message = error.to_string();
    match error {
        SqliteError::ExecutionNotFound(_)
        | SqliteError::WorkflowIdNotFound { .. }
        | SqliteError::UnknownWorkflow(_)
        | SqliteError::QueryNotFound { .. }
        | SqliteError::UpdateHandlerNotFound { .. }
        | SqliteError::ScheduleNotFound(_) => AutumnError::not_found_msg(message),
        SqliteError::AlreadyExists { .. } | SqliteError::WorkflowNotRunning { .. } => {
            AutumnError::conflict_msg(message)
        }
        // A handler error and a replay that diverges are `400` on Postgres.
        SqliteError::BlankWorkflowId
        | SqliteError::InvalidSignal(_)
        | SqliteError::InvalidSearchAttribute(_)
        | SqliteError::InvalidFilter(_)
        | SqliteError::UpdateInputInvalid { .. }
        | SqliteError::SignalPayloadInvalid { .. }
        | SqliteError::WorkflowInputInvalid { .. }
        | SqliteError::PayloadTooDeep { .. }
        | SqliteError::QueryFailed { .. }
        | SqliteError::NonDeterministic { .. }
        | SqliteError::RetryChainTooDeep { .. } => AutumnError::bad_request_msg(message),
        SqliteError::PayloadTooLarge { .. } => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::PAYLOAD_TOO_LARGE)
        }
        SqliteError::UpdateRejected { .. }
        | SqliteError::UpdateFailed { .. }
        | SqliteError::UpdateHistoryCapReached { .. } => AutumnError::unprocessable_msg(message),
        SqliteError::QueryTimedOut { .. } => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::REQUEST_TIMEOUT)
        }
        // A terminal run with no stable answer is `HistoryUnavailable` on
        // Postgres (issue #612).
        SqliteError::QueryUnavailable { .. } => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::GONE)
        }
        SqliteError::UpdateTimedOut { .. } => {
            AutumnError::service_unavailable_msg(message).with_status(StatusCode::GATEWAY_TIMEOUT)
        }
        // A panicked query handler is an engine fault, and a storage error
        // is safe to retry. Postgres answers both with `503`.
        SqliteError::QueryPanicked { .. }
        | SqliteError::DatabaseLocked { .. }
        | SqliteError::Sqlite(_)
        | SqliteError::TaskFailed { .. } => AutumnError::service_unavailable_msg(message),
        _ => AutumnError::internal_server_error_msg(message),
    }
}

fn parse_exec(raw: &str) -> Result<ExecutionId, AutumnError> {
    crate::api::parse_execution_id(raw)
}

/// A JSON body that may be empty. An empty body is `T::default()`.
fn optional_body<T: Default + for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, AutumnError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|error| AutumnError::bad_request_msg(format!("invalid JSON body: {error}")))
}

/// Refuse each request field that this backend cannot honour.
fn refuse_unsupported(fields: &[(&str, bool)]) -> Result<(), AutumnError> {
    let set: Vec<&str> = fields
        .iter()
        .filter_map(|(name, present)| present.then_some(*name))
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    Err(AutumnError::bad_request_msg(format!(
        "not supported on the SQLite backend: {} (autumn-harvest#{EPIC_ISSUE})",
        set.join(", ")
    )))
}

/// Refuse a request that carries any query parameter.
async fn refuse_query_parameters(
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    let Some(query) = request.uri().query().filter(|query| !query.is_empty()) else {
        return next.run(request).await;
    };
    let keys: Vec<&str> = query
        .split('&')
        .filter_map(|pair| pair.split('=').next())
        .filter(|key| !key.is_empty())
        .collect();
    AutumnError::bad_request_msg(format!(
        "not supported on the SQLite backend: query parameter {} (autumn-harvest#2489)",
        keys.join(", ")
    ))
    .into_response()
}

async fn not_ported() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "not available on the SQLite backend yet",
            "backend": "sqlite",
            "issue": format!("https://github.com/autumn-foundation/autumn-harvest/issues/{EPIC_ISSUE}"),
        })),
    )
        .into_response()
}

// ── Health ───────────────────────────────────────────────────────────────────

async fn health(State(state): State<ApiState>) -> Response {
    match state.handle() {
        Ok(handle) => Json(json!({
            "status": "ok",
            "backend": "sqlite",
            "path": handle.path().display().to_string(),
        }))
        .into_response(),
        Err(error) => error.into_response(),
    }
}

/// `GET /health/live`. It does no I/O, as on Postgres. This backend never
/// drains.
async fn health_live() -> Json<Value> {
    Json(json!({ "alive": true, "draining": false }))
}

/// `GET /health/ready`. The replica is ready once the driver runs and the
/// Harvest file answers a read. The body keeps the Postgres fields.
async fn health_ready(State(state): State<ApiState>) -> Response {
    // A tenant route reads the tenant's own file. The control file does not
    // decide whether a tenant is ready.
    let handle = state.handle().ok();
    // The Postgres probe gives its database check one second.
    let reachable = match &handle {
        Some(handle) => Some(matches!(
            tokio::time::timeout(READY_BUDGET, handle.ping()).await,
            Ok(Ok(()))
        )),
        None => None,
    };
    let mut reasons = Vec::new();
    if handle.is_none() {
        reasons.push("runtime_not_started");
    }
    if reachable == Some(false) {
        reasons.push("database_unreachable");
    }
    let ready = reasons.is_empty();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ready": ready,
            "runtime_ready": handle.is_some(),
            "draining": false,
            "database_reachable": reachable,
            "shard_readiness_enforced": false,
            "shard_readiness": null,
            "reasons": reasons,
        })),
    )
        .into_response()
}

// ── Reads ────────────────────────────────────────────────────────────────────

fn stored_json<T: serde::Serialize>(value: &Stored<T>) -> Value {
    value
        .value()
        .and_then(|value| serde_json::to_value(value).ok())
        .unwrap_or(Value::Null)
}

/// One run as a listing shows it. The names follow the Postgres row.
fn summary_json(summary: &ExecutionSummary) -> Value {
    json!({
        "id": stored_json(&summary.exec_id),
        "workflow_name": stored_json(&summary.workflow_name),
        "workflow_id": stored_json(&summary.workflow_id),
        "state": stored_json(&summary.state),
        "started_at": stored_json(&summary.started_at),
        "completed_at": stored_json(&summary.closed_at),
        "search_attrs": stored_json(&summary.search_attributes),
        "memo": stored_json(&summary.memo),
    })
}

fn parse_stored_json(text: &Stored<String>) -> Value {
    text.value()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or(Value::Null)
}

fn execution_json(description: &ExecutionDescription) -> Value {
    let mut execution = summary_json(&description.summary);
    if let Value::Object(fields) = &mut execution {
        fields.insert("input".into(), parse_stored_json(&description.input_json));
        fields.insert("output".into(), parse_stored_json(&description.output_json));
        fields.insert("error".into(), stored_json(&description.error));
    }
    execution
}

/// Query parameters of `GET /workflows`. Unknown keys are refused.
#[derive(Debug, Default)]
struct ListQuery {
    workflow_name: Option<String>,
    workflow_id: Option<String>,
    workflow_id_prefix: Option<String>,
    /// One state, or several separated by commas.
    states: Vec<String>,
    limit: Option<i64>,
    page_size: Option<i64>,
    cursor: Option<ListCursor>,
    /// `cursor` or `page_size` asks for the `{ workflows, next_cursor }`
    /// envelope, as on Postgres. Otherwise the body is a bare array.
    paginated: bool,
}

/// A blank value counts as absent, as on Postgres.
fn non_blank(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn parse_count(key: &str, value: &str) -> Result<i64, AutumnError> {
    value
        .trim()
        .parse()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid {key} '{value}'")))
}

impl ListQuery {
    fn parse(raw: Option<&str>) -> Result<Self, AutumnError> {
        let mut query = Self::default();
        for (key, value) in crate::strict_query::decode_or_autumn_error(raw)? {
            match key.as_str() {
                "workflow_name" => query.workflow_name = non_blank(&value),
                "workflow_id" => query.workflow_id = non_blank(&value),
                "workflow_id_prefix" => query.workflow_id_prefix = non_blank(&value),
                "state" => {
                    // The check is case sensitive, as on Postgres. A state that
                    // no run can have would otherwise list nothing with a `200`.
                    for state in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                        if !crate::api::KNOWN_WORKFLOW_STATES.contains(&state) {
                            return Err(AutumnError::bad_request_msg(format!(
                                "unknown workflow state '{state}'; expected one of {:?}",
                                crate::api::KNOWN_WORKFLOW_STATES
                            )));
                        }
                        if !query.states.iter().any(|known| known == state) {
                            query.states.push(state.to_owned());
                        }
                    }
                }
                "limit" => query.limit = Some(parse_count(&key, &value)?),
                // `page_size` wins over `limit` in any order, as on Postgres.
                "page_size" => {
                    query.page_size = Some(parse_count(&key, &value)?);
                    query.paginated = true;
                }
                "cursor" => {
                    query.paginated = true;
                    let raw: i64 = value.parse().map_err(|_| {
                        AutumnError::bad_request_msg(format!("invalid cursor '{value}'"))
                    })?;
                    query.cursor = Some(ListCursor::from_raw(raw));
                }
                other => {
                    return Err(AutumnError::bad_request_msg(format!(
                        "unknown or unsupported query parameter '{other}' on the SQLite backend"
                    )));
                }
            }
        }
        Ok(query)
    }

    /// The page size: 50 by default, cut to the range 1 to 200, as on
    /// Postgres.
    fn page_size(&self) -> u32 {
        let size = self
            .page_size
            .or(self.limit)
            .unwrap_or(crate::api::DEFAULT_WORKFLOW_LIMIT)
            .clamp(1, crate::api::MAX_WORKFLOW_LIMIT);
        u32::try_from(size).unwrap_or(1)
    }

    fn into_filter(self) -> ExecutionFilter {
        let mut filter = ExecutionFilter::new();
        filter.limit = Some(self.page_size());
        filter.workflow_name = self.workflow_name;
        filter.workflow_id = self.workflow_id;
        filter.workflow_id_prefix = self.workflow_id_prefix;
        filter.states = self.states;
        filter.before = self.cursor;
        filter
    }
}

async fn list_workflows(
    State(state): State<ApiState>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    let query = ListQuery::parse(raw.as_deref())?;
    let paginated = query.paginated;
    let page = state
        .handle()?
        .list(query.into_filter())
        .await
        .map_err(map_error)?;
    let rows: Vec<Value> = page.executions.iter().map(summary_json).collect();
    if paginated {
        let next_cursor = page.next.map(|cursor| cursor.raw().to_string());
        // One file has no shard to miss, so the page is always complete.
        return Ok(Json(json!({
            "workflows": rows,
            "next_cursor": next_cursor,
            "status": "complete",
            "unavailable_shards": [],
        }))
        .into_response());
    }
    Ok(Json(rows).into_response())
}

/// `GET /workflows/registered`: the Postgres record of each workflow, sorted
/// by name.
async fn list_registered(State(state): State<ApiState>) -> Result<Json<Value>, AutumnError> {
    let handle = state.handle()?;
    let records: Vec<autumn_harvest::info::RegisteredWorkflowRecord> = handle
        .workflow_names()
        .iter()
        .filter_map(|name| handle.workflow_info(name))
        .map(autumn_harvest::info::RegisteredWorkflowRecord::from_info)
        .collect();
    serde_json::to_value(records)
        .map(Json)
        .map_err(|error| AutumnError::internal_server_error_msg(error.to_string()))
}

/// `GET /workflows/{id}`: the run and its first 100 events, as on Postgres.
async fn describe_json(handle: &HarvestSqlite, exec: ExecutionId) -> Result<Value, AutumnError> {
    let description = handle.describe(exec).await.map_err(map_error)?;
    let page_size = u32::try_from(crate::api::DEFAULT_HISTORY_PAGE).unwrap_or(u32::MAX);
    let page = handle
        .history_page(exec, HistoryFilter::new().limit(page_size))
        .await
        .map_err(map_error)?;
    let parent = handle.parent(exec).await.map_err(map_error)?;
    let events: Vec<Value> = page
        .events
        .iter()
        .filter_map(|paged| serde_json::to_value(&paged.event).ok())
        .collect();
    let mut body = json!({
        "parent_id": parent.map(|parent| parent.to_string()),
        "execution": execution_json(&description),
        "history": events,
        "external_handoffs": [],
        "history_truncated": page.next.is_some(),
        "history_endpoint": format!("/workflows/{exec}/history"),
        // This backend has no legal hold.
        "legal_hold": false,
    });
    // The first event carries the schedule carry-over, as on Postgres.
    if let Some(autumn_harvest::WorkflowEvent::WorkflowStarted {
        last_completion_result,
        last_error,
        scheduled_time,
        ..
    }) = page.events.first().map(|paged| &paged.event)
        && let Value::Object(fields) = &mut body
    {
        let optional = [
            ("last_completion_result", last_completion_result.clone()),
            ("last_error", last_error.clone().map(Value::from)),
            (
                "scheduled_time",
                scheduled_time.and_then(|at| serde_json::to_value(at).ok()),
            ),
        ];
        for (name, value) in optional {
            if let Some(value) = value {
                fields.insert(name.into(), value);
            }
        }
    }
    Ok(body)
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    Ok(Json(describe_json(&state.handle()?, exec).await?))
}

/// Set `X-Harvest-Execution-Id` to the run that a by-id route resolved, as
/// on Postgres. It is set on an error too.
fn with_execution_id(mut response: Response, exec: ExecutionId) -> Response {
    if let Ok(value) = axum::http::HeaderValue::from_str(&exec.to_string()) {
        response.headers_mut().insert(
            axum::http::HeaderName::from_static(crate::api::HEADER_EXECUTION_ID),
            value,
        );
    }
    response
}

/// The response of a by-id route: the delegate's answer with the header.
fn by_id_response<T: IntoResponse>(out: Result<T, AutumnError>, exec: ExecutionId) -> Response {
    with_execution_id(
        out.map_or_else(IntoResponse::into_response, IntoResponse::into_response),
        exec,
    )
}

/// Resolve a business key for a by-id route. An empty id is the Postgres
/// `400`, before any lookup (issue #1353).
async fn resolve_key(
    handle: &HarvestSqlite,
    workflow_name: &str,
    workflow_id: &str,
) -> Result<ExecutionId, AutumnError> {
    if workflow_id.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "workflow_id must not be empty",
        ));
    }
    handle
        .resolve_by_id(workflow_name, workflow_id)
        .await
        .map_err(map_error)
}

async fn get_by_id(
    State(state): State<ApiState>,
    Path(ByIdParams {
        workflow_name,
        workflow_id,
    }): Path<ByIdParams>,
) -> Result<Response, AutumnError> {
    let handle = state.handle()?;
    let exec = resolve_key(&handle, &workflow_name, &workflow_id).await?;
    Ok(by_id_response(
        describe_json(&handle, exec).await.map(Json),
        exec,
    ))
}

/// `/workflows/by-id/{workflow_name}` with no id segment. The body is the
/// Postgres guard body.
async fn by_id_without_id() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": "workflow_id is required",
            "detail": "address the business-id form as /workflows/by-id/{workflow_name}/{workflow_id}",
        })),
    )
        .into_response()
}

/// `/workflows/by-id/{workflow_name}/`: an empty id, as on Postgres.
async fn by_id_with_empty_id() -> AutumnError {
    AutumnError::bad_request_msg("workflow_id must not be empty")
}

/// `GET /workflows/{id}/history`. It follows the Postgres cursor contract:
/// `after`, `limit` and `event_type`. Like Postgres, it ignores other keys.
async fn get_history(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Json<Value>, AutumnError> {
    let pairs = crate::strict_query::decode_or_autumn_error(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    let (limit, after, event_types) = crate::api::parse_workflow_history_query(&pairs)?;
    let mut filter = HistoryFilter::new()
        .after(after)
        .limit(u32::try_from(limit).unwrap_or(u32::MAX));
    filter.event_types = event_types;
    let page = state
        .handle()?
        .history_page(exec, filter)
        .await
        .map_err(map_error)?;
    let events: Vec<Value> = page
        .events
        .iter()
        .map(|paged| {
            let (event_type, data) = match serde_json::to_value(&paged.event) {
                Ok(Value::Object(mut fields)) => (
                    fields.remove("type").unwrap_or(Value::Null),
                    fields.remove("data").unwrap_or_else(|| json!({})),
                ),
                _ => (Value::Null, json!({})),
            };
            json!({
                "id": paged.id,
                "event_id": paged.event_id,
                "timestamp": paged.recorded_at,
                "type": event_type,
                "data": data,
            })
        })
        .collect();
    Ok(Json(json!({
        "events": events,
        "next_cursor": page.next.map(|cursor| cursor.to_string()),
        "total_events": page.total_events,
        "last_event_id": page.last_event_id,
    })))
}

/// `GET /workflows/{id}/result`. It keeps the Postgres polling contract.
///
/// A run that is not terminal answers `204` with `Retry-After: 1`. A
/// terminal run answers the compact form. The read follows the
/// continue-as-new chain and the workflow retry chain. `wait` polls the file,
/// because SQLite has no `LISTEN`.
async fn get_result(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    let pairs = crate::strict_query::decode_or_autumn_error(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    let wait = crate::api::parse_workflow_result_wait_query(
        &pairs,
        state.flags.workflow_result_max_wait(),
    )?;
    result_response(&state.handle()?, exec, wait).await
}

async fn result_response(
    handle: &HarvestSqlite,
    exec: ExecutionId,
    wait: Duration,
) -> Result<Response, AutumnError> {
    let deadline = Instant::now() + wait;
    let mut from = exec;
    loop {
        let walk = handle.chain_result(from).await.map_err(map_error)?;
        // A chain past its hop bound answers the run it reached at once, as
        // the Postgres zero-wait read does. A long poll answers `204`.
        if walk.exhausted && !wait.is_zero() {
            return Ok(crate::api::workflow_result_pending_response());
        }
        if walk.exhausted || walk.result.is_terminal() {
            return Ok((StatusCode::OK, Json(walk.result)).into_response());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(crate::api::workflow_result_pending_response());
        }
        // The next poll resumes at the run that this one reached.
        from = walk.last;
        tokio::time::sleep(remaining.min(RESULT_POLL)).await;
    }
}

async fn get_result_by_id(
    State(state): State<ApiState>,
    Path(ByIdParams {
        workflow_name,
        workflow_id,
    }): Path<ByIdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    // Only a bad encoding is a `400` before the lookup, as on Postgres. The
    // `wait` check runs after it, so its `400` carries the header.
    let pairs = crate::strict_query::decode_or_autumn_error(raw.as_deref())?;
    let handle = state.handle()?;
    let exec = resolve_key(&handle, &workflow_name, &workflow_id).await?;
    let answer = match crate::api::parse_workflow_result_wait_query(
        &pairs,
        state.flags.workflow_result_max_wait(),
    ) {
        Ok(wait) => result_response(&handle, exec, wait).await,
        Err(error) => Err(error),
    };
    Ok(by_id_response(answer, exec))
}

// ── Writes ───────────────────────────────────────────────────────────────────

/// `POST /workflows/{workflow_name}/start`. The names follow the Postgres
/// request. Each option that this backend cannot honour is refused.
#[derive(Debug, Default, Deserialize)]
struct SqliteStartBody {
    workflow_id: Option<String>,
    input: Option<Value>,
    memo: Option<Value>,
    search_attrs: Option<serde_json::Map<String, Value>>,
    reuse_policy: Option<String>,
    queue: Option<String>,
    execution_timeout_secs: Option<i64>,
    sla_secs: Option<i64>,
    conflict_policy: Option<String>,
    start_at: Option<Value>,
    delay: Option<String>,
    batch_key: Option<String>,
    batch_max_size: Option<usize>,
    batch_max_wait: Option<String>,
    completion_callbacks: Option<Value>,
    context_headers: Option<Value>,
    priority: Option<Value>,
    fairness_key: Option<String>,
    idempotency_key: Option<String>,
    shard_id: Option<i32>,
    residency_key: Option<String>,
}

impl SqliteStartBody {
    fn refuse_unsupported(&self) -> Result<(), AutumnError> {
        refuse_unsupported(&[
            ("queue", self.queue.is_some()),
            (
                "execution_timeout_secs",
                self.execution_timeout_secs.is_some(),
            ),
            ("sla_secs", self.sla_secs.is_some()),
            ("conflict_policy", self.conflict_policy.is_some()),
            ("start_at", self.start_at.is_some()),
            ("delay", self.delay.is_some()),
            ("batch_key", self.batch_key.is_some()),
            ("batch_max_size", self.batch_max_size.is_some()),
            ("batch_max_wait", self.batch_max_wait.is_some()),
            ("completion_callbacks", self.completion_callbacks.is_some()),
            ("context_headers", self.context_headers.is_some()),
            ("priority", self.priority.is_some()),
            ("fairness_key", self.fairness_key.is_some()),
            ("idempotency_key", self.idempotency_key.is_some()),
            ("shard_id", self.shard_id.is_some()),
            ("residency_key", self.residency_key.is_some()),
        ])
    }
}

/// A refused write as its Postgres response.
///
/// A schema violation is the Postgres `400` with `violations`: the
/// workflow's `input_schema` (issue #373) or a signal's `arg_schema` (issue
/// #610). Every other error goes through [`map_error`].
fn write_error(error: HarvestSqliteError) -> Response {
    let (message, violations) = match error {
        HarvestSqliteError::InvalidInput(violations) => ("input validation failed", violations),
        HarvestSqliteError::InvalidSignal(violations) => {
            ("signal payload validation failed", violations)
        }
        HarvestSqliteError::Sqlite(SqliteError::UpdateInputInvalid { violations, .. }) => {
            ("update payload validation failed", violations)
        }
        // A `reject_duplicate` start names the run it collided with.
        HarvestSqliteError::Sqlite(SqliteError::AlreadyExists {
            existing_exec_id,
            existing_state,
        }) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "existing_execution_id": existing_exec_id.to_string(),
                    "existing_state": existing_state,
                })),
            )
                .into_response();
        }
        other => return map_error(other).into_response(),
    };
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": message, "violations": violations })),
    )
        .into_response()
}

/// `terminate_if_running` ends another caller's run, so it needs an admin.
/// A caller without admin access gets the Postgres `401`.
async fn require_admin_for_policy(
    state: &ApiState,
    policy: Option<&str>,
    session: Option<Session>,
) -> Result<(), AutumnError> {
    if policy == Some("terminate_if_running")
        && !crate::api::has_harvest_admin_access(&state.flags, session).await
    {
        return Err(AutumnError::unauthorized_msg("authentication required"));
    }
    Ok(())
}

/// A signal error. A run that is not running is the Postgres `400`, not the
/// `409` of the other writes.
fn signal_error(error: HarvestSqliteError) -> Response {
    match error {
        HarvestSqliteError::Sqlite(error @ SqliteError::WorkflowNotRunning { .. }) => {
            AutumnError::bad_request_msg(error.to_string()).into_response()
        }
        other => write_error(other),
    }
}

async fn start(
    State(state): State<ApiState>,
    Path(NameParams { workflow_name }): Path<NameParams>,
    session: Option<axum::Extension<Session>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let request: SqliteStartBody = optional_body(&body)?;
    if let Err(response) = crate::api::reject_empty_workflow_id(request.workflow_id.as_deref()) {
        return Ok(response);
    }
    request.refuse_unsupported()?;
    // Postgres dedupes a start on this header (issue #808). This backend
    // does not, so a retried start would run twice. Refuse it instead.
    refuse_unsupported(&[(
        "the Idempotency-Key header",
        headers.contains_key("idempotency-key"),
    )])?;
    let session = session.map(|axum::Extension(session)| session);
    require_admin_for_policy(&state, request.reuse_policy.as_deref(), session).await?;
    let mut options = StartOptions::new().reuse_policy(crate::api::parse_reuse_policy(
        request.reuse_policy.as_deref(),
    )?);
    options.workflow_id = request.workflow_id;
    options.memo = request.memo;
    options.search_attributes = request.search_attrs.unwrap_or_default();
    let input = request.input.unwrap_or(Value::Null);
    let started = match state
        .handle()?
        .start_with_state(&workflow_name, input, options)
        .await
    {
        Ok(started) => started,
        Err(error) => return Ok(write_error(error)),
    };
    let status = if started.outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    // Postgres sends `started_fresh` only for an idempotency key or a
    // `conflict_policy`. This backend refuses both, so the field is absent.
    Ok((
        status,
        Json(json!({
            "execution_id": started.outcome.exec_id.to_string(),
            "workflow_name": workflow_name,
            "workflow_id": started.workflow_id,
            "state": started.state,
        })),
    )
        .into_response())
}

/// `POST /workflows/{workflow_name}/signal-with-start`.
#[derive(Debug, Deserialize)]
struct SignalWithStartBody {
    workflow_id: String,
    #[serde(default)]
    start_input: Option<Value>,
    signal_name: String,
    #[serde(default)]
    signal_payload: Option<Value>,
    #[serde(default)]
    id_reuse_policy: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    memo: Option<Value>,
    #[serde(default)]
    search_attrs: Option<Value>,
    #[serde(default)]
    execution_timeout_secs: Option<i64>,
}

async fn signal_with_start(
    State(state): State<ApiState>,
    Path(NameParams { workflow_name }): Path<NameParams>,
    session: Option<axum::Extension<Session>>,
    Json(body): Json<SignalWithStartBody>,
) -> Result<Response, AutumnError> {
    if let Err(response) = crate::api::reject_empty_workflow_id(Some(&body.workflow_id)) {
        return Ok(response);
    }
    if let Some(response) = body
        .idempotency_key
        .as_deref()
        .and_then(crate::api::reject_reserved_idempotency_key)
    {
        return Ok(response);
    }
    refuse_unsupported(&[
        ("queue", body.queue.is_some()),
        ("memo", body.memo.is_some()),
        ("search_attrs", body.search_attrs.is_some()),
        (
            "execution_timeout_secs",
            body.execution_timeout_secs.is_some(),
        ),
    ])?;
    let session = session.map(|axum::Extension(session)| session);
    require_admin_for_policy(&state, body.id_reuse_policy.as_deref(), session).await?;
    let reuse_policy = crate::api::parse_reuse_policy(body.id_reuse_policy.as_deref())?;
    let handle = state.handle()?;
    // The runtime checks the start input when the call starts a run, and the
    // signal payload, as Postgres does.
    let started = match handle
        .signal_with_start_state(SignalWithStartParams {
            workflow_name: workflow_name.clone(),
            workflow_id: body.workflow_id.clone(),
            input: body.start_input.unwrap_or(Value::Null),
            reuse_policy,
            signal_name: body.signal_name,
            signal_payload: body.signal_payload.unwrap_or(Value::Null),
            idempotency_key: body.idempotency_key,
        })
        .await
    {
        Ok(started) => started,
        Err(error) => return Ok(write_error(error)),
    };
    let outcome = started.outcome;
    let status = if outcome.started_fresh {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(json!({
            "execution_id": outcome.exec_id.to_string(),
            "workflow_name": workflow_name,
            "workflow_id": body.workflow_id,
            "state": started.state,
            "started_fresh": outcome.started_fresh,
            "signal_delivered": outcome.signal_delivered,
        })),
    )
        .into_response())
}

/// The exactly-once key: the `Idempotency-Key` header, then the
/// `idempotency_key` query parameter. A blank header is a `400`, as on
/// Postgres, so a client that asked for exactly-once never silently gets
/// at-least-once.
fn idempotency_key(
    headers: &axum::http::HeaderMap,
    raw_query: Option<&str>,
) -> Result<Option<String>, Box<Response>> {
    let key =
        raw_idempotency_key(headers, raw_query).map_err(|error| Box::new(error.into_response()))?;
    // The `conn:` namespace belongs to the engine, as on Postgres.
    if let Some(response) = key
        .as_deref()
        .and_then(crate::api::reject_reserved_idempotency_key)
    {
        return Err(Box::new(response));
    }
    Ok(key)
}

/// The key as the request carries it, before the namespace check.
fn raw_idempotency_key(
    headers: &axum::http::HeaderMap,
    raw_query: Option<&str>,
) -> Result<Option<String>, AutumnError> {
    if let Some(value) = headers.get("idempotency-key") {
        let key = value
            .to_str()
            .ok()
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                AutumnError::bad_request_msg("the Idempotency-Key header must not be blank")
            })?;
        return Ok(Some(key.to_owned()));
    }
    let pairs = crate::strict_query::decode_or_autumn_error(raw_query)?;
    for (key, value) in pairs {
        if key != "idempotency_key" {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown query parameter '{key}'"
            )));
        }
        if !value.trim().is_empty() {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

async fn signal(
    State(state): State<ApiState>,
    Path(SignalParams { id, signal_name }): Path<SignalParams>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let exec = parse_exec(&id)?;
    let key = match idempotency_key(&headers, raw.as_deref()) {
        Ok(key) => key,
        Err(response) => return Ok(*response),
    };
    let payload: Value = optional_body::<Option<Value>>(&body)?.unwrap_or(Value::Null);
    let routed = match state
        .handle()?
        .signal_routed(exec, &signal_name, payload, key)
        .await
    {
        Ok(routed) => routed,
        Err(error) => return Ok(signal_error(error)),
    };
    // The fields of the Postgres `SignalAck`. `routed_execution_id` names the
    // live retry of a failed run (issue #843), and is absent otherwise.
    let mut ack = json!({
        "ok": true,
        "signal_delivered": routed.delivered,
    });
    if routed.exec != exec
        && let Value::Object(fields) = &mut ack
    {
        fields.insert("routed_execution_id".into(), routed.exec.to_string().into());
    }
    Ok((StatusCode::ACCEPTED, Json(ack)).into_response())
}

async fn signal_by_id(
    State(state): State<ApiState>,
    Path(ByIdSignalParams {
        workflow_name,
        workflow_id,
        signal_name,
    }): Path<ByIdSignalParams>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let key = match idempotency_key(&headers, raw.as_deref()) {
        Ok(key) => key,
        Err(response) => return Ok(*response),
    };
    let payload: Value = optional_body::<Option<Value>>(&body)?.unwrap_or(Value::Null);
    if workflow_id.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "workflow_id must not be empty",
        ));
    }
    let handle = state.handle()?;
    // `execution_id` and the header name the run that the key resolves to,
    // as on Postgres (issue #805). The driver resolves it in the command that
    // delivers, so the answer names the run that governed the delivery. The
    // run that took the signal can differ: a failed run hands it to its live
    // retry (issue #843).
    let keyed = handle
        .signal_by_key(&workflow_name, &workflow_id, &signal_name, payload, key)
        .await
        .map_err(map_error)?;
    let delivery = match (HarvestSqlite::lift_reply(keyed.delivery), keyed.resolved) {
        (Ok(delivery), _) => delivery,
        (Err(error), Some(resolved)) => {
            return Ok(with_execution_id(signal_error(error), resolved));
        }
        (Err(error), None) => return Ok(signal_error(error)),
    };
    let resolved = keyed.resolved.unwrap_or(delivery.exec_id);
    let mut ack = json!({
        "execution_id": resolved.to_string(),
        "ok": true,
        "signal_delivered": delivery.delivered,
    });
    if delivery.delivered
        && delivery.exec_id != resolved
        && let Value::Object(fields) = &mut ack
    {
        fields.insert(
            "routed_execution_id".into(),
            delivery.exec_id.to_string().into(),
        );
    }
    Ok(with_execution_id(
        (StatusCode::ACCEPTED, Json(ack)).into_response(),
        resolved,
    ))
}

/// The body of cancel and terminate. Both take an optional reason.
#[derive(Debug, Default, Deserialize)]
struct ReasonBody {
    reason: Option<String>,
}

/// `POST /workflows/{id}/cancel`.
///
/// The cancel is cooperative. The next drive replays the body with the
/// reason set, so a new request leaves the run `RUNNING` and answers
/// `deferred: true`. A repeat request and a cancelled run answer as no-ops,
/// as on Postgres.
async fn cancel(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AutumnError> {
    let exec = parse_exec(&id)?;
    let ReasonBody { reason } = optional_body(&body)?;
    let reason = capped_reason(reason.as_deref());
    let ack = state
        .handle()?
        .cancel(exec, &reason)
        .await
        .map_err(map_error)?;
    let deferred = ack.state == "RUNNING";
    Ok((
        StatusCode::ACCEPTED,
        Json(stop_json(&ack, "newly_cancelled", Some(deferred))),
    ))
}

/// `POST /workflows/{id}/terminate`. A terminal run answers as a no-op, as
/// on Postgres.
async fn terminate(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AutumnError> {
    let exec = parse_exec(&id)?;
    let ReasonBody { reason } = optional_body(&body)?;
    let reason = capped_reason(reason.as_deref());
    let ack = state
        .handle()?
        .terminate(exec, &reason)
        .await
        .map_err(map_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(stop_json(&ack, "newly_terminated", None)),
    ))
}

/// The reason of a cancel or a terminate, trimmed, then cut to the
/// operator-reason cap.
///
/// The run keeps the reason in its cancel state and in history. The cap
/// bounds that storage. Postgres stores the whole reason. The trim runs
/// first, so a reason with long leading white space still keeps its text.
/// The runtime gives an empty reason its default.
fn capped_reason(reason: Option<&str>) -> String {
    crate::api::truncate_operator_reason(reason.unwrap_or_default().trim())
}

/// The Postgres body of a cancel or a terminate.
fn stop_json(ack: &StopAck, newly_field: &str, deferred: Option<bool>) -> Value {
    let mut body = json!({
        "ok": true,
        "execution_id": ack.exec.to_string(),
        "state": ack.state,
        "reason": ack.reason,
    });
    if let Value::Object(fields) = &mut body {
        fields.insert(newly_field.into(), ack.newly.into());
        if let Some(deferred) = deferred {
            fields.insert("deferred".into(), deferred.into());
        }
    }
    body
}

async fn query_get(
    State(state): State<ApiState>,
    Path(QueryParams { id, query_name }): Path<QueryParams>,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let answer = state
        .handle()?
        .query(exec, &query_name, Value::Null)
        .await
        .map_err(map_error)?;
    Ok(Json(answer))
}

#[derive(Debug, Default, Deserialize)]
struct QueryBody {
    #[serde(default)]
    args: Value,
}

async fn query_post(
    State(state): State<ApiState>,
    Path(QueryParams { id, query_name }): Path<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let QueryBody { args } = optional_body(&body)?;
    let result = state
        .handle()?
        .query(exec, &query_name, args)
        .await
        .map_err(map_error)?;
    Ok(Json(json!({ "result": result })))
}

#[derive(Debug, Default, Deserialize)]
struct UpdateBody {
    #[serde(default)]
    input: Value,
}

/// `POST /workflows/{id}/update/{update_name}`.
///
/// SQLite runs the handler in the request. That is the Postgres default,
/// `wait=completed`: `200` with `update_id` and `output`, or `409` with
/// `update_id` and `error`. `wait=admitted` and `timeout_secs` are refused,
/// because there is nothing to wait for.
async fn update(
    State(state): State<ApiState>,
    Path(UpdateParams { id, update_name }): Path<UpdateParams>,
    RawQuery(raw): RawQuery,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let exec = parse_exec(&id)?;
    for (key, value) in crate::strict_query::decode_or_autumn_error(raw.as_deref())? {
        if key != "wait" || value != "completed" {
            return Err(AutumnError::bad_request_msg(format!(
                "not supported on the SQLite backend: query parameter '{key}={value}' \
                 (autumn-harvest#{EPIC_ISSUE}). An update completes in the request"
            )));
        }
    }
    let UpdateBody { input } = optional_body(&body)?;
    let routed = match state
        .handle()?
        .update_routed(exec, &update_name, input)
        .await
    {
        Ok(routed) => routed,
        Err(HarvestSqliteError::Sqlite(SqliteError::UpdateRejected { reason, .. })) => {
            return Ok((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": "update rejected by validator", "reason": reason })),
            )
                .into_response());
        }
        // Postgres admits an update with no handler, and its replay fails it.
        // This backend writes no event, but the answer is the same.
        Err(HarvestSqliteError::Sqlite(SqliteError::UpdateHandlerNotFound { name, .. })) => {
            return Ok((
                StatusCode::CONFLICT,
                Json(json!({
                    "update_id": autumn_harvest::UpdateId::new().to_string(),
                    "error": format!("update handler '{name}' not found"),
                })),
            )
                .into_response());
        }
        Err(HarvestSqliteError::Sqlite(error @ SqliteError::UpdateTimedOut { .. })) => {
            return Ok((
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "update_id": autumn_harvest::UpdateId::new().to_string(),
                    "message": error.to_string(),
                })),
            )
                .into_response());
        }
        Err(error) => return Ok(write_error(error)),
    };
    let update_id = routed.update.update_id.to_string();
    Ok(match routed.update.result {
        Ok(output) => (
            StatusCode::OK,
            Json(json!({ "update_id": update_id, "output": output })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::CONFLICT,
            Json(json!({ "update_id": update_id, "error": error })),
        )
            .into_response(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_execution_maps_to_404() {
        let error = map_error(HarvestSqliteError::Sqlite(SqliteError::ExecutionNotFound(
            ExecutionId::new(),
        )));
        assert_eq!(error.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn a_stopped_driver_maps_to_503() {
        let error = map_error(HarvestSqliteError::Stopped);
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn an_unsupported_start_option_is_refused_by_name() {
        let request = SqliteStartBody {
            queue: Some("q".into()),
            priority: Some(json!("high")),
            ..SqliteStartBody::default()
        };
        let error = request.refuse_unsupported().unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        let message = error.to_string();
        assert!(
            message.contains("queue") && message.contains("priority"),
            "{message}"
        );
        assert!(SqliteStartBody::default().refuse_unsupported().is_ok());
    }

    #[test]
    fn the_list_query_parses_the_postgres_keys() {
        let query = ListQuery::parse(Some(
            "page_size=5&workflow_name=w&state=RUNNING,FAILED,RUNNING&limit=9&cursor=9",
        ))
        .unwrap();
        assert!(query.paginated);
        let filter = query.into_filter();
        assert_eq!(filter.workflow_name.as_deref(), Some("w"));
        assert_eq!(
            filter.states,
            vec!["RUNNING".to_owned(), "FAILED".to_owned()]
        );
        // `page_size` wins over `limit` in any order, as on Postgres.
        assert_eq!(filter.limit, Some(5));
        assert_eq!(filter.before, Some(ListCursor::from_raw(9)));
        let plain = ListQuery::parse(Some("workflow_id=123&limit=2&workflow_name=+")).unwrap();
        assert!(!plain.paginated);
        assert_eq!(plain.workflow_id.as_deref(), Some("123"));
        assert_eq!(plain.workflow_name, None, "a blank name is no filter");
        assert!(ListQuery::parse(Some("owner=x")).is_err());
        assert!(ListQuery::parse(Some("limit=x")).is_err());
        // The state check is case sensitive, as on Postgres.
        assert!(ListQuery::parse(Some("state=running")).is_err());
        assert!(ListQuery::parse(Some("state=BOGUS")).is_err());
    }

    #[test]
    fn the_page_size_follows_the_postgres_bounds() {
        let size = |raw: &str| ListQuery::parse(Some(raw)).unwrap().page_size();
        assert_eq!(ListQuery::default().page_size(), 50);
        assert_eq!(size("limit=500"), 200);
        assert_eq!(size("limit=-3"), 1);
        assert_eq!(size("page_size=0"), 1);
    }

    #[test]
    fn query_errors_map_to_the_postgres_codes() {
        let exec = ExecutionId::new();
        let name = "q".to_owned();
        let status = |error: SqliteError| map_error(HarvestSqliteError::Sqlite(error)).status();
        assert_eq!(
            status(SqliteError::QueryFailed {
                execution_id: exec,
                name: name.clone(),
                details: String::new(),
            }),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(SqliteError::QueryTimedOut {
                execution_id: exec,
                name: name.clone(),
                timeout_ms: 1,
            }),
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(
            status(SqliteError::QueryUnavailable {
                execution_id: exec,
                name: name.clone(),
                details: String::new(),
            }),
            StatusCode::GONE
        );
        assert_eq!(
            status(SqliteError::QueryPanicked {
                execution_id: exec,
                name,
                details: String::new(),
            }),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn write_errors_keep_the_postgres_bodies() {
        let exec = ExecutionId::new();
        let body = |response: Response| async move {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        };
        let conflict = write_error(HarvestSqliteError::Sqlite(SqliteError::AlreadyExists {
            existing_exec_id: exec,
            existing_state: "COMPLETED".to_owned(),
        }));
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert_eq!(
            body(conflict).await,
            json!({ "existing_execution_id": exec.to_string(), "existing_state": "COMPLETED" })
        );
        let violation = autumn_harvest::info::SchemaViolation {
            field_path: None,
            message: "bad".to_owned(),
        };
        let invalid = write_error(HarvestSqliteError::Sqlite(
            SqliteError::UpdateInputInvalid {
                execution_id: exec,
                name: "u".to_owned(),
                violations: vec![violation],
            },
        ));
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let invalid = body(invalid).await;
        assert_eq!(invalid["error"], "update payload validation failed");
        assert_eq!(invalid["violations"].as_array().map(Vec::len), Some(1));
        let not_running = signal_error(HarvestSqliteError::Sqlite(
            SqliteError::WorkflowNotRunning {
                execution_id: exec,
                state: "COMPLETED".to_owned(),
            },
        ));
        assert_eq!(not_running.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_blank_idempotency_header_is_refused() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("idempotency-key", " ".parse().unwrap());
        assert!(idempotency_key(&headers, None).is_err());
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("idempotency-key", "k1".parse().unwrap());
        assert_eq!(
            idempotency_key(&headers, Some("idempotency_key=k2"))
                .unwrap()
                .as_deref(),
            Some("k1")
        );
        let none = axum::http::HeaderMap::new();
        assert_eq!(
            idempotency_key(&none, Some("idempotency_key=k2"))
                .unwrap()
                .as_deref(),
            Some("k2")
        );
        assert!(idempotency_key(&none, Some("other=1")).is_err());
        let padded = format!("{}why", " ".repeat(600));
        assert_eq!(capped_reason(Some(&padded)), "why");
        let long = "x".repeat(600);
        assert_eq!(capped_reason(Some(&long)).chars().count(), 500);
        assert_eq!(capped_reason(None), "");
        // The engine owns the `conn:` namespace, as on Postgres.
        let reserved = idempotency_key(&none, Some("idempotency_key=conn:x")).unwrap_err();
        assert_eq!(reserved.status(), StatusCode::BAD_REQUEST);
    }
}
