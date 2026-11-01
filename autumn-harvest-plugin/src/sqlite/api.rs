//! The management API on the SQLite backend (issue #2467).
//!
//! It serves the core routes with the Postgres paths and field names. A body
//! carries the fields this backend stores, so it is a subset of the Postgres
//! body. A route that has no SQLite port answers `501` and names the epic. It
//! never answers an empty `200`, so a client cannot mistake a missing feature
//! for an empty result.
//!
//! A request option that this backend cannot honour, such as `queue` or
//! `start_at`, is a `400` for the same reason.
//!
//! The #1802 mutation gate wraps the router, as on Postgres. Outside `dev`, a
//! mutating call needs a declared auth boundary, an admin session or the
//! explicit opt-out.

use std::sync::{Arc, OnceLock};

use autumn_harvest::ExecutionId;
use autumn_harvest_sqlite::{
    ExecutionDescription, ExecutionFilter, ExecutionSummary, ListCursor, SqliteError, StartOptions,
    Stored,
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

use super::{EPIC_ISSUE, HarvestSqlite, HarvestSqliteError, SignalWithStartRequest};
use crate::api::HarvestApiState;

/// What every handler reads.
#[derive(Clone)]
pub struct ApiState {
    /// Filled by the startup hook. A request before it gets `503`.
    pub handle: Arc<OnceLock<HarvestSqlite>>,
    /// The flags that the #1802 mutation gate reads.
    pub api_state: HarvestApiState,
}

impl ApiState {
    fn handle(&self) -> Result<&HarvestSqlite, AutumnError> {
        self.handle.get().ok_or_else(|| {
            AutumnError::service_unavailable_msg("the Harvest SQLite backend is not running yet")
        })
    }
}

/// The SQLite management router, gated like the Postgres one.
pub fn router(state: ApiState) -> Router<()> {
    let gate = middleware::from_fn_with_state(
        state.api_state.clone(),
        crate::api::require_mutation_auth_by_method,
    );
    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health))
        .route("/workflows", get(list_workflows))
        .route("/workflows/registered", get(list_registered))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/result", get(get_result))
        .route("/workflows/{id}/history", get(get_history))
        .route("/workflows/{id}/cancel", post(cancel))
        .route("/workflows/{id}/terminate", post(terminate))
        .route("/workflows/{id}/signal/{signal_name}", post(signal))
        .route(
            "/workflows/{id}/query/{query_name}",
            get(query_get).post(query_post),
        )
        .route("/workflows/{id}/update/{update_name}", post(update))
        .route("/workflows/{workflow_name}/start", post(start))
        .route(
            "/workflows/{workflow_name}/signal-with-start",
            post(signal_with_start),
        )
        .route(
            "/workflows/by-id/{workflow_name}/{workflow_id}",
            get(get_by_id),
        )
        .route(
            "/workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}",
            post(signal_by_id),
        )
        // `AppBuilder::nest` drops a nested fallback, so a catch-all route
        // stands in for it. A static or a parameter route still wins.
        .route("/{*rest}", any(not_ported))
        .layer(gate)
        .with_state(state)
}

// ── Errors ───────────────────────────────────────────────────────────────────

/// The HTTP form of a backend error.
fn map_error(error: HarvestSqliteError) -> AutumnError {
    let HarvestSqliteError::Sqlite(error) = error else {
        return AutumnError::service_unavailable_msg(error.to_string());
    };
    let message = error.to_string();
    match error {
        SqliteError::ExecutionNotFound(_)
        | SqliteError::WorkflowIdNotFound { .. }
        | SqliteError::UnknownWorkflow(_)
        | SqliteError::QueryNotFound { .. }
        | SqliteError::UpdateHandlerNotFound { .. }
        | SqliteError::ScheduleNotFound(_) => AutumnError::not_found_msg(message),
        SqliteError::AlreadyExists { .. }
        | SqliteError::WorkflowNotRunning { .. }
        | SqliteError::QueryUnavailable { .. } => AutumnError::conflict_msg(message),
        SqliteError::BlankWorkflowId
        | SqliteError::InvalidSignal(_)
        | SqliteError::InvalidSearchAttribute(_)
        | SqliteError::InvalidFilter(_)
        | SqliteError::UpdateInputInvalid { .. }
        | SqliteError::PayloadTooDeep { .. } => AutumnError::bad_request_msg(message),
        SqliteError::PayloadTooLarge { .. } => {
            AutumnError::bad_request_msg(message).with_status(StatusCode::PAYLOAD_TOO_LARGE)
        }
        SqliteError::UpdateRejected { .. }
        | SqliteError::UpdateFailed { .. }
        | SqliteError::UpdateHistoryCapReached { .. }
        | SqliteError::QueryFailed { .. } => AutumnError::unprocessable_msg(message),
        SqliteError::QueryTimedOut { .. } | SqliteError::UpdateTimedOut { .. } => {
            AutumnError::service_unavailable_msg(message).with_status(StatusCode::GATEWAY_TIMEOUT)
        }
        SqliteError::DatabaseLocked { .. } => AutumnError::service_unavailable_msg(message),
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

async fn health_live() -> Json<Value> {
    Json(json!({ "status": "ok" }))
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
    limit: Option<u32>,
    cursor: Option<ListCursor>,
    /// `cursor` or `page_size` asks for the `{ workflows, next_cursor }`
    /// envelope, as on Postgres. Otherwise the body is a bare array.
    paginated: bool,
}

impl ListQuery {
    fn parse(raw: Option<&str>) -> Result<Self, AutumnError> {
        let mut query = Self::default();
        for (key, value) in crate::strict_query::decode_or_autumn_error(raw)? {
            match key.as_str() {
                "workflow_name" => query.workflow_name = Some(value),
                "workflow_id" => query.workflow_id = Some(value),
                "workflow_id_prefix" => query.workflow_id_prefix = Some(value),
                "state" => query.states.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|state| !state.is_empty())
                        .map(str::to_ascii_uppercase),
                ),
                "limit" | "page_size" => {
                    query.paginated |= key == "page_size";
                    query.limit = Some(value.parse().map_err(|_| {
                        AutumnError::bad_request_msg(format!("invalid {key} '{value}'"))
                    })?);
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

    fn into_filter(self) -> ExecutionFilter {
        let mut filter = ExecutionFilter::new();
        filter.workflow_name = self.workflow_name;
        filter.workflow_id = self.workflow_id;
        filter.workflow_id_prefix = self.workflow_id_prefix;
        filter.states = self.states;
        filter.limit = self.limit;
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
        return Ok(Json(json!({ "workflows": rows, "next_cursor": next_cursor })).into_response());
    }
    Ok(Json(rows).into_response())
}

async fn list_registered(State(state): State<ApiState>) -> Result<Json<Value>, AutumnError> {
    let names = state.handle()?.workflow_names();
    let records: Vec<Value> = names.iter().map(|name| json!({ "name": name })).collect();
    Ok(Json(Value::Array(records)))
}

async fn describe_json(handle: &HarvestSqlite, exec: ExecutionId) -> Result<Value, AutumnError> {
    let description = handle.describe(exec).await.map_err(map_error)?;
    let history = handle.history(exec).await.map_err(map_error)?;
    let events: Vec<Value> = history
        .iter()
        .filter_map(|event| serde_json::to_value(event).ok())
        .collect();
    Ok(json!({
        "backend": "sqlite",
        "execution": execution_json(&description),
        "history": events,
        "history_truncated": false,
        "history_endpoint": format!("/workflows/{exec}/history"),
    }))
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    Ok(Json(describe_json(state.handle()?, exec).await?))
}

async fn get_by_id(
    State(state): State<ApiState>,
    Path((workflow_name, workflow_id)): Path<(String, String)>,
) -> Result<Json<Value>, AutumnError> {
    let handle = state.handle()?;
    let filter = ExecutionFilter::new()
        .workflow_name(workflow_name.clone())
        .workflow_id(workflow_id.clone())
        .limit(1);
    let page = handle.list(filter).await.map_err(map_error)?;
    let exec = page
        .executions
        .first()
        .and_then(ExecutionSummary::execution_id)
        .ok_or_else(|| {
            AutumnError::not_found_msg(format!(
                "no run of workflow '{workflow_name}' with workflow_id '{workflow_id}'"
            ))
        })?;
    Ok(Json(describe_json(handle, exec).await?))
}

async fn get_result(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let description = state.handle()?.describe(exec).await.map_err(map_error)?;
    let state_name = stored_json(&description.summary.state);
    let terminal = state_name
        .as_str()
        .is_some_and(crate::api::is_terminal_state);
    Ok(Json(json!({
        "execution_id": exec.to_string(),
        "state": state_name,
        "is_terminal": terminal,
        "output": parse_stored_json(&description.output_json),
        "error": stored_json(&description.error),
    })))
}

async fn get_history(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let history = state.handle()?.history(exec).await.map_err(map_error)?;
    let events: Vec<Value> = history
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            let mut value = serde_json::to_value(event).ok()?;
            if let Value::Object(fields) = &mut value {
                fields.insert("event_id".into(), Value::from(index));
            }
            Some(value)
        })
        .collect();
    let total = events.len();
    Ok(Json(json!({
        "events": events,
        "next_cursor": null,
        "total_events": total,
        "last_event_id": total.checked_sub(1),
    })))
}

// ── Writes ───────────────────────────────────────────────────────────────────

/// `POST /workflows/{workflow_name}/start`. The names follow the Postgres
/// request. Each option that this backend cannot honour is refused.
#[derive(Debug, Default, Deserialize)]
struct StartRequest {
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

impl StartRequest {
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

/// `terminate_if_running` ends another caller's run, so it needs an admin,
/// as on Postgres.
async fn require_admin_for_policy(
    state: &ApiState,
    policy: Option<&str>,
    session: Option<Session>,
) -> Result<(), AutumnError> {
    if policy == Some("terminate_if_running")
        && !crate::api::has_harvest_admin_access(&state.api_state, session).await
    {
        return Err(AutumnError::forbidden_msg(
            "reuse_policy terminate_if_running requires harvest admin access",
        ));
    }
    Ok(())
}

async fn start(
    State(state): State<ApiState>,
    Path(workflow_name): Path<String>,
    session: Option<axum::Extension<Session>>,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let request: StartRequest = optional_body(&body)?;
    request.refuse_unsupported()?;
    let session = session.map(|axum::Extension(session)| session);
    require_admin_for_policy(&state, request.reuse_policy.as_deref(), session).await?;
    let mut options = StartOptions::new().reuse_policy(crate::api::parse_reuse_policy(
        request.reuse_policy.as_deref(),
    )?);
    options.workflow_id = request.workflow_id;
    options.memo = request.memo;
    options.search_attributes = request.search_attrs.unwrap_or_default();
    let started = state
        .handle()?
        .start(
            &workflow_name,
            request.input.unwrap_or(Value::Null),
            options,
        )
        .await
        .map_err(map_error)?;
    let description = state
        .handle()?
        .describe(started.exec_id)
        .await
        .map_err(map_error)?;
    let status = if started.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(json!({
            "execution_id": started.exec_id.to_string(),
            "workflow_name": workflow_name,
            "workflow_id": stored_json(&description.summary.workflow_id),
            "state": stored_json(&description.summary.state),
            "started_fresh": started.created,
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
    Path(workflow_name): Path<String>,
    session: Option<axum::Extension<Session>>,
    Json(body): Json<SignalWithStartBody>,
) -> Result<Json<Value>, AutumnError> {
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
    let outcome = handle
        .signal_with_start(SignalWithStartRequest {
            workflow_name: workflow_name.clone(),
            workflow_id: body.workflow_id.clone(),
            input: body.start_input.unwrap_or(Value::Null),
            reuse_policy,
            signal_name: body.signal_name,
            signal_payload: body.signal_payload.unwrap_or(Value::Null),
            idempotency_key: body.idempotency_key,
        })
        .await
        .map_err(map_error)?;
    let description = handle.describe(outcome.exec_id).await.map_err(map_error)?;
    Ok(Json(json!({
        "execution_id": outcome.exec_id.to_string(),
        "workflow_name": workflow_name,
        "workflow_id": body.workflow_id,
        "state": stored_json(&description.summary.state),
        "started_fresh": outcome.started_fresh,
        "signal_delivered": outcome.signal_delivered,
    })))
}

/// The exactly-once key: the `Idempotency-Key` header, then the
/// `idempotency_key` query parameter. A blank header is a `400`, as on
/// Postgres, so a client that asked for exactly-once never silently gets
/// at-least-once.
fn idempotency_key(
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
    Path((id, signal_name)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let key = idempotency_key(&headers, raw.as_deref())?;
    let payload: Value = optional_body::<Option<Value>>(&body)?.unwrap_or(Value::Null);
    let delivered = state
        .handle()?
        .signal(exec, &signal_name, payload, key)
        .await
        .map_err(map_error)?;
    Ok(Json(json!({
        "ok": true,
        "execution_id": exec.to_string(),
        "delivered": delivered,
    })))
}

async fn signal_by_id(
    State(state): State<ApiState>,
    Path((workflow_name, workflow_id, signal_name)): Path<(String, String, String)>,
    RawQuery(raw): RawQuery,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let key = idempotency_key(&headers, raw.as_deref())?;
    let payload: Value = optional_body::<Option<Value>>(&body)?.unwrap_or(Value::Null);
    let delivery = state
        .handle()?
        .signal_by_workflow_id(&workflow_name, &workflow_id, &signal_name, payload, key)
        .await
        .map_err(map_error)?;
    Ok(Json(json!({
        "ok": true,
        "execution_id": delivery.exec_id.to_string(),
        "delivered": delivery.delivered,
    })))
}

/// The body of cancel and terminate. Both take an optional reason.
#[derive(Debug, Default, Deserialize)]
struct ReasonBody {
    reason: Option<String>,
}

async fn cancel(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AutumnError> {
    let exec = parse_exec(&id)?;
    let ReasonBody { reason } = optional_body(&body)?;
    let reason = crate::api::truncate_operator_reason(reason.as_deref().unwrap_or_default());
    let handle = state.handle()?;
    handle.cancel(exec, &reason).await.map_err(map_error)?;
    // The cancel is cooperative. The next drive replays the body with the
    // reason set, so the run is not terminal yet.
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "ok": true,
            "execution_id": exec.to_string(),
            "state": "RUNNING",
            "reason": reason,
            "newly_cancelled": true,
            "deferred": true,
        })),
    ))
}

async fn terminate(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let ReasonBody { reason } = optional_body(&body)?;
    let reason = crate::api::truncate_operator_reason(reason.as_deref().unwrap_or_default());
    state
        .handle()?
        .terminate(exec, &reason)
        .await
        .map_err(map_error)?;
    Ok(Json(json!({
        "ok": true,
        "execution_id": exec.to_string(),
        "state": "TERMINATED",
        "reason": reason,
        "newly_terminated": true,
    })))
}

async fn query_get(
    State(state): State<ApiState>,
    Path((id, query_name)): Path<(String, String)>,
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
    Path((id, query_name)): Path<(String, String)>,
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
/// Postgres admits an update and answers `202`, and the result comes later.
/// SQLite runs the handler in the request, so this answers `200` with the
/// output in one step. `wait` and `timeout_secs` are refused: there is
/// nothing to wait for.
async fn update(
    State(state): State<ApiState>,
    Path((id, update_name)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let exec = parse_exec(&id)?;
    let pairs = crate::strict_query::decode_or_autumn_error(raw.as_deref())?;
    if let Some((key, _)) = pairs.first() {
        return Err(AutumnError::bad_request_msg(format!(
            "not supported on the SQLite backend: query parameter '{key}' \
             (autumn-harvest#{EPIC_ISSUE}). An update completes in the request"
        )));
    }
    let UpdateBody { input } = optional_body(&body)?;
    let output = state
        .handle()?
        .update(exec, &update_name, input)
        .await
        .map_err(map_error)?;
    Ok(Json(json!({ "state": "COMPLETED", "output": output })))
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
        let request = StartRequest {
            queue: Some("q".into()),
            priority: Some(json!("high")),
            ..StartRequest::default()
        };
        let error = request.refuse_unsupported().unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        let message = error.to_string();
        assert!(
            message.contains("queue") && message.contains("priority"),
            "{message}"
        );
        assert!(StartRequest::default().refuse_unsupported().is_ok());
    }

    #[test]
    fn the_list_query_parses_the_postgres_keys() {
        let query = ListQuery::parse(Some(
            "workflow_name=w&state=running,failed&page_size=5&cursor=9",
        ))
        .unwrap();
        assert!(query.paginated);
        let filter = query.into_filter();
        assert_eq!(filter.workflow_name.as_deref(), Some("w"));
        assert_eq!(
            filter.states,
            vec!["RUNNING".to_owned(), "FAILED".to_owned()]
        );
        assert_eq!(filter.limit, Some(5));
        assert_eq!(filter.before, Some(ListCursor::from_raw(9)));
        let plain = ListQuery::parse(Some("workflow_id=123&limit=2")).unwrap();
        assert!(!plain.paginated);
        assert_eq!(plain.workflow_id.as_deref(), Some("123"));
        assert!(ListQuery::parse(Some("owner=x")).is_err());
        assert!(ListQuery::parse(Some("limit=x")).is_err());
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
    }
}
