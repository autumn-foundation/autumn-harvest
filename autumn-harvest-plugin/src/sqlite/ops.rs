//! The schedule and retention admin routes on SQLite (issue #2471).
//!
//! Each route keeps its Postgres path, status codes and field names. A body
//! carries the fields this backend stores, so it is a subset of the Postgres
//! body.
//!
//! - A schedule is keyed by its workflow name. Its `{id}` is a UUID v5 of
//!   that name, so a client can keep the id that a list gave it.
//! - A read uses the read-only connection. A write runs on the driver task.
//! - A field that this backend cannot honour is a `400` that names it.
//!   Backfill, run listing, decisions and preview answer `501`.
//! - A `PATCH` of a code schedule sets an operator override (issue #2390).
//!   A `PATCH` of an API schedule edits it in place, as on Postgres.
//! - Each write adds one audit row, as on Postgres.

// An audit route label such as `PATCH /admin/schedules/{id}` names an axum
// path parameter, not a format argument.
#![allow(clippy::literal_string_with_formatting_args)]

use autumn_harvest::audit::{
    OP_RETENTION_RUN_NOW, OP_SCHEDULE_CREATE, OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE,
    OP_SCHEDULE_RESUME, OP_SCHEDULE_TRIGGER, OP_SCHEDULE_UPDATE, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_RETENTION, TARGET_SCHEDULE,
};
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::policy::{OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::schedule_patch::WorkflowSchedulePatch;
use autumn_harvest_sqlite::{ScheduleSummary, ScheduleTrigger, SqliteError};
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::api::{ApiState, map_error, not_ported};
use super::{HarvestSqlite, HarvestSqliteError};
use crate::api::HarvestApiState;

/// The namespace of the schedule ids. A fixed value keeps each id stable
/// across restarts and copies of the file.
const SCHEDULE_ID_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x7c1d_4a52_9b0e_4f3a_8d61_2471_5c4e_a9b3);

/// The id of the schedule of `workflow_name`.
pub fn schedule_id(workflow_name: &str) -> uuid::Uuid {
    uuid::Uuid::new_v5(&SCHEDULE_ID_NAMESPACE, workflow_name.as_bytes())
}

/// The schedule and retention routes. The #1802 gate of the caller covers
/// the writes. Trigger and retention need harvest admin, as on Postgres.
pub fn routes(flags: &HarvestApiState) -> Router<ApiState> {
    let admin = middleware::from_fn_with_state(flags.clone(), crate::api::require_harvest_admin);
    Router::new()
        .route("/admin/schedules", get(list))
        .route("/admin/schedules/workflow", post(create))
        // Static paths win over `{id}`. These have no SQLite port.
        .route("/admin/schedules/decisions", any(not_ported))
        .route("/admin/schedules/preview", any(not_ported))
        .route(
            "/admin/schedules/{id}",
            get(get_one).patch(patch).delete(delete),
        )
        .route("/admin/schedules/{id}/pause", post(pause))
        .route("/admin/schedules/{id}/resume", post(resume))
        .route(
            "/admin/schedules/{id}/trigger",
            post(trigger).route_layer(admin.clone()),
        )
        .route(
            "/admin/retention",
            get(retention_status).route_layer(admin.clone()),
        )
        .route("/admin/retention/run-now", post(run_now).route_layer(admin))
}

// ── Shapes ───────────────────────────────────────────────────────────────────

/// One schedule in the Postgres `ScheduleEntry` shape.
///
/// The fields that this backend does not store are left out. A calendar,
/// a run budget and catch-up do not exist here, so the values that say
/// "none" are fixed.
#[derive(Debug, Serialize)]
struct Entry {
    id: uuid::Uuid,
    kind: &'static str,
    name: String,
    schedule_expr: String,
    timezone: String,
    is_paused: bool,
    pause_reason: Option<String>,
    next_run_at: Option<DateTime<Utc>>,
    last_run_at: Option<DateTime<Utc>>,
    max_active_runs: u32,
    catchup: bool,
    workflow_input: Value,
    jitter_secs: u64,
    overlap_policy: &'static str,
    catchup_policy_effective: &'static str,
}

impl Entry {
    fn of(row: &ScheduleSummary) -> Self {
        let (schedule_expr, timezone) = expr_of(&row.schedule);
        Self {
            id: schedule_id(&row.workflow_name),
            kind: "workflow",
            name: row.workflow_name.clone(),
            schedule_expr,
            timezone,
            is_paused: row.paused,
            pause_reason: row.pause_reason.clone(),
            next_run_at: row.next_run_at,
            last_run_at: row.last_run_at,
            max_active_runs: row.max_active_runs,
            catchup: false,
            workflow_input: row.input.clone(),
            jitter_secs: 0,
            overlap_policy: row.overlap_policy.as_str(),
            catchup_policy_effective: "skip_all",
        }
    }
}

/// The stored expression and the time zone of `schedule`, as on Postgres.
fn expr_of(schedule: &Schedule) -> (String, String) {
    let utc = || "UTC".to_owned();
    match schedule {
        Schedule::Cron(expr) => (format!("cron:{expr}"), utc()),
        Schedule::CronInTimezone { expr, tz } => (format!("cron_tz:{tz}:{expr}"), tz.clone()),
        Schedule::Interval(every) => (format!("interval:{}", every.as_secs()), utc()),
        Schedule::Manual => ("manual".to_owned(), utc()),
    }
}

/// `POST /admin/schedules/workflow`, with every Postgres field.
///
/// A field that this backend cannot honour is kept, so the refusal can
/// name it.
#[derive(Debug, Deserialize)]
struct CreateBody {
    workflow_name: String,
    schedule_expr: String,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    catchup: bool,
    #[serde(default)]
    max_active_runs: Option<u32>,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    queue_name: Option<String>,
    #[serde(default)]
    jitter_secs: Option<u64>,
    #[serde(default)]
    overlap_policy: Option<String>,
    #[serde(default)]
    buffer_all_max: Option<u32>,
    #[serde(default)]
    calendar: Option<String>,
    #[serde(default)]
    skip_policy: Option<String>,
    #[serde(default)]
    consecutive_failure_limit: Option<u32>,
    #[serde(default)]
    end_at: Option<DateTime<Utc>>,
    #[serde(default)]
    max_runs: Option<u32>,
    #[serde(default)]
    catchup_policy: Option<String>,
    #[serde(default)]
    catchup_window_secs: Option<i64>,
    #[serde(default)]
    retry_policy: Option<Value>,
}

/// `PATCH /admin/schedules/{id}`. An absent field keeps its value.
///
/// `deny_unknown_fields`: a misspelt field would otherwise be a silent
/// no-op, as on Postgres.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchBody {
    workflow_name: Option<String>,
    schedule_expr: Option<String>,
    timezone: Option<String>,
    input: Option<Value>,
    max_active_runs: Option<u32>,
    overlap_policy: Option<String>,
    #[serde(default)]
    clear_override: bool,
    reason: Option<String>,
    // The Postgres fields that this backend cannot honour. A present one is
    // refused by name.
    catchup: Option<Value>,
    queue_name: Option<Value>,
    jitter_secs: Option<Value>,
    buffer_all_max: Option<Value>,
    calendar: Option<Value>,
    skip_policy: Option<Value>,
    consecutive_failure_limit: Option<Value>,
    end_at: Option<Value>,
    max_runs: Option<Value>,
    catchup_policy: Option<Value>,
    catchup_window_secs: Option<Value>,
    retry_policy: Option<Value>,
}

impl PatchBody {
    /// Refuse each field that this backend cannot honour. A value that `GET`
    /// shows, such as `catchup: false`, changes nothing, so it passes. A
    /// read-modify-write client can then send the body back.
    fn refuse_unsupported(&self) -> Result<(), AutumnError> {
        let set_to =
            |field: &Option<Value>, keep: &Value| field.as_ref().is_some_and(|v| v != keep);
        unsupported(&[
            ("catchup", set_to(&self.catchup, &json!(false))),
            ("queue_name", set_to(&self.queue_name, &json!("default"))),
            ("jitter_secs", set_to(&self.jitter_secs, &json!(0))),
            ("buffer_all_max", self.buffer_all_max.is_some()),
            ("calendar", self.calendar.is_some()),
            ("skip_policy", set_to(&self.skip_policy, &json!("skip"))),
            (
                "consecutive_failure_limit",
                self.consecutive_failure_limit.is_some(),
            ),
            ("end_at", self.end_at.is_some()),
            ("max_runs", self.max_runs.is_some()),
            ("catchup_policy", self.catchup_policy.is_some()),
            ("catchup_window_secs", self.catchup_window_secs.is_some()),
            ("retry_policy", self.retry_policy.is_some()),
        ])
    }

    /// Whether the body edits a field, beyond `clear_override` and `reason`.
    /// The other Postgres fields are refused before this check.
    const fn has_edit_fields(&self) -> bool {
        self.schedule_expr.is_some()
            || self.timezone.is_some()
            || self.input.is_some()
            || self.max_active_runs.is_some()
            || self.overlap_policy.is_some()
    }

    /// The Postgres `check_override_fields`, before any write.
    ///
    /// A code schedule takes its input from code. A clear stands alone,
    /// because the other fields resolve against the stored override. An API
    /// schedule has no override, so a clear or a reason is a mistake.
    fn check_override_fields(&self, code: bool, id: uuid::Uuid) -> Result<(), AutumnError> {
        let refusal = if code && self.input.is_some() {
            Some(format!(
                "schedule {id} is declared in code; its input is set in code and an override \
                 cannot change it"
            ))
        } else if code && self.clear_override && self.has_edit_fields() {
            Some(format!(
                "schedule {id}: clear_override cannot be combined with other fields; clear \
                 first, then send the new values"
            ))
        } else if !code && (self.clear_override || self.reason.is_some()) {
            Some(format!(
                "schedule {id} is not declared in code, so it has no override; clear_override \
                 and reason apply to a code-declared schedule only"
            ))
        } else {
            None
        };
        refusal.map_or(Ok(()), |message| Err(AutumnError::bad_request_msg(message)))
    }

    /// The new cadence, or `None` when the body keeps it.
    ///
    /// An expression with no `timezone` keeps the stored time zone, as on
    /// Postgres. A `cron_tz:` expression names its own.
    fn schedule(&self, stored: &Schedule) -> Result<Option<Schedule>, AutumnError> {
        let stored_tz = match stored {
            Schedule::CronInTimezone { tz, .. } => tz.as_str(),
            _ => "UTC",
        };
        match (&self.schedule_expr, &self.timezone) {
            (Some(expr), tz) => parse_expr(expr, tz.as_deref().unwrap_or(stored_tz)).map(Some),
            // A time zone alone re-anchors a stored cron, as on Postgres.
            (None, Some(tz)) => Ok(match stored {
                Schedule::Cron(expr) | Schedule::CronInTimezone { expr, .. } => {
                    Some(parse_expr(&format!("cron:{expr}"), tz)?)
                }
                Schedule::Interval(_) | Schedule::Manual => None,
            }),
            (None, None) => Ok(None),
        }
    }
}

/// `POST /admin/schedules/{id}/pause` and `/resume`.
#[derive(Debug, Default, Deserialize)]
struct ReasonBody {
    #[serde(default)]
    reason: Option<String>,
}

/// `POST /admin/schedules/{id}/trigger`. Its `reason` is read by no one,
/// as on Postgres, where only the audit trail keeps the route.
#[derive(Debug, Default, Deserialize)]
struct TriggerBody {
    #[serde(default)]
    overlap_policy: Option<String>,
}

#[derive(Deserialize)]
struct IdParams {
    id: String,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Refuse each request field that this backend cannot honour, by name.
fn unsupported(fields: &[(&str, bool)]) -> Result<(), AutumnError> {
    let set: Vec<&str> = fields
        .iter()
        .filter_map(|(name, present)| present.then_some(*name))
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    Err(AutumnError::bad_request_msg(format!(
        "not supported on the SQLite backend: {} (autumn-harvest#{})",
        set.join(", "),
        super::EPIC_ISSUE
    )))
}

fn parse_expr(expr: &str, timezone: &str) -> Result<Schedule, AutumnError> {
    crate::api::parse_schedule_expr_with_tz(expr, timezone)
        .map_err(|error| AutumnError::bad_request_msg(format!("invalid schedule_expr: {error}")))
}

fn parse_overlap(raw: &str) -> Result<OverlapPolicy, AutumnError> {
    OverlapPolicy::from_user_input(raw).map_err(|value| {
        AutumnError::bad_request_msg(format!(
            "invalid overlap_policy '{value}'; valid values: {}",
            OverlapPolicy::VALID_VALUES
        ))
    })
}

/// A schedule error. A refused field or an invalid value is a `400`.
fn schedule_error(error: HarvestSqliteError) -> AutumnError {
    match error {
        HarvestSqliteError::Sqlite(
            error @ (SqliteError::Unsupported(_) | SqliteError::InvalidSchedule(_)),
        ) => AutumnError::bad_request_msg(error.to_string()),
        other => map_error(other),
    }
}

/// A JSON body that may be empty. An empty body is `T::default()`.
fn optional_body<T: Default + for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, AutumnError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|error| AutumnError::bad_request_msg(format!("invalid request body: {error}")))
}

/// Every schedule, read off the driver.
async fn load_all(handle: &HarvestSqlite) -> Result<Vec<ScheduleSummary>, AutumnError> {
    handle
        .read(autumn_harvest_sqlite::SqliteReader::list_schedules)
        .await
        .map_err(map_error)
}

/// The schedule of `{id}`. A malformed id is a `400`, an unknown one `404`.
async fn load_one(handle: &HarvestSqlite, raw: &str) -> Result<ScheduleSummary, AutumnError> {
    let id = raw
        .parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid schedule id '{raw}'")))?;
    load_all(handle)
        .await?
        .into_iter()
        .find(|row| schedule_id(&row.workflow_name) == id)
        .ok_or_else(|| AutumnError::not_found_msg(format!("schedule {id}")))
}

/// The entry of `workflow_name` after a write.
async fn entry_of(handle: &HarvestSqlite, workflow_name: &str) -> Result<Json<Value>, AutumnError> {
    let row = load_all(handle)
        .await?
        .into_iter()
        .find(|row| row.workflow_name == workflow_name)
        .ok_or_else(|| AutumnError::not_found_msg(format!("schedule {workflow_name}")))?;
    Ok(Json(json!(Entry::of(&row))))
}

/// The parts of one audit row.
struct Audit<'a> {
    operation: &'a str,
    target_type: &'a str,
    target_id: Option<&'a str>,
    route: &'a str,
}

/// `POST /admin/x` on tenant `acme` gives `POST /tenants/acme/admin/x`.
fn tenant_route(route: &str, tenant: Option<&str>) -> String {
    match (tenant, route.split_once(' ')) {
        (Some(tenant), Some((method, path))) => format!("{method} /tenants/{tenant}{path}"),
        _ => route.to_owned(),
    }
}

/// Whether `workflow_name` is a code schedule of this Harvest.
///
/// Only the control Harvest upserts the code schedules. A tenant schedule of
/// the same name is an API schedule.
fn is_code_schedule(handle: &HarvestSqlite, workflow_name: &str) -> bool {
    handle.tenant().is_none() && handle.catalog().code_schedules.contains(workflow_name)
}

/// The error text of a failed route, for its audit row.
fn failure<T>(outcome: &Result<T, AutumnError>) -> Option<String> {
    outcome.as_ref().err().map(ToString::to_string)
}

/// Write one audit row, best effort, as the Postgres schedule routes do.
///
/// All rows go to the control store. A tenant route names its tenant in
/// the route, because a schedule id is the same in every tenant.
async fn audit(state: &ApiState, headers: &HeaderMap, row: &Audit<'_>, error: Option<String>) {
    let flags = &state.flags;
    let Some(store) = flags.auth_store() else {
        return;
    };
    let (actor, source, request_id) = crate::api::audit_context(headers, flags);
    let tenant = state
        .handle()
        .ok()
        .and_then(|handle| handle.tenant().map(ToString::to_string));
    let route = tenant_route(row.route, tenant.as_deref());
    let record = NewAuditRecord {
        actor: &actor,
        operation: row.operation,
        target_type: row.target_type,
        target_id: row.target_id,
        route_or_command: &route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status: if error.is_some() {
            STATUS_FAILED
        } else {
            STATUS_SUCCEEDED
        },
        error_summary: error.as_deref(),
        shard_id: None,
        source: &source,
    };
    let written = match store.session().await {
        Ok(mut session) => session.insert_audit(&record).await,
        Err(error) => Err(error),
    };
    if let Err(error) = written {
        tracing::warn!(%error, operation = row.operation, "audit row not written");
    }
}

// ── Schedule routes ──────────────────────────────────────────────────────────

/// `GET /admin/schedules`. One file is one shard, so the list is complete
/// and is a plain array, as on Postgres.
async fn list(State(state): State<ApiState>) -> Result<Json<Value>, AutumnError> {
    let rows = load_all(&state.handle()?).await?;
    Ok(Json(json!(rows.iter().map(Entry::of).collect::<Vec<_>>())))
}

async fn get_one(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<Value>, AutumnError> {
    let row = load_one(&state.handle()?, &id).await?;
    Ok(Json(json!(Entry::of(&row))))
}

/// `POST /admin/schedules/workflow`. It upserts, as on Postgres, and answers
/// `201`.
async fn create(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AutumnError> {
    let outcome = create_inner(&state, &body).await;
    let target = outcome.as_ref().ok().map(|(name, _)| name.clone());
    let row = Audit {
        operation: OP_SCHEDULE_CREATE,
        target_type: TARGET_SCHEDULE,
        target_id: target.as_deref(),
        route: "POST /admin/schedules/workflow",
    };
    audit(&state, &headers, &row, failure(&outcome)).await;
    let (_, entry) = outcome?;
    Ok((StatusCode::CREATED, entry).into_response())
}

async fn create_inner(
    state: &ApiState,
    body: &Bytes,
) -> Result<(String, Json<Value>), AutumnError> {
    let request: CreateBody = serde_json::from_slice(body)
        .map_err(|error| AutumnError::bad_request_msg(format!("invalid request body: {error}")))?;
    let handle = state.handle()?;
    if handle.workflow_info(&request.workflow_name).is_none() {
        return Err(AutumnError::not_found_msg(format!(
            "workflow '{}' is not registered",
            request.workflow_name
        )));
    }
    unsupported(&[
        ("catchup", request.catchup),
        (
            "queue_name",
            request
                .queue_name
                .as_deref()
                .is_some_and(|q| q != "default"),
        ),
        ("jitter_secs", request.jitter_secs.is_some_and(|j| j > 0)),
        ("buffer_all_max", request.buffer_all_max.is_some()),
        ("calendar", request.calendar.is_some()),
        (
            "skip_policy",
            request.skip_policy.as_deref().is_some_and(|p| p != "skip"),
        ),
        (
            "consecutive_failure_limit",
            request.consecutive_failure_limit.is_some(),
        ),
        ("end_at", request.end_at.is_some()),
        ("max_runs", request.max_runs.is_some()),
        (
            "catchup_policy",
            request
                .catchup_policy
                .as_deref()
                .is_some_and(|p| p != "skip_all"),
        ),
        ("catchup_window_secs", request.catchup_window_secs.is_some()),
        ("retry_policy", request.retry_policy.is_some()),
    ])?;
    let schedule = parse_expr(
        &request.schedule_expr,
        request.timezone.as_deref().unwrap_or("UTC"),
    )?;
    let mut code = WorkflowSchedule::new(request.workflow_name.clone(), schedule)
        .with_input(request.input)
        .with_paused(request.paused);
    code.max_active_runs = request.max_active_runs.unwrap_or(1);
    if let Some(raw) = &request.overlap_policy {
        code.overlap_policy = parse_overlap(raw)?;
    }
    handle
        .run(move |runtime| runtime.upsert_schedule(&code))
        .await
        .map_err(schedule_error)?
        .map_err(|error| schedule_error(error.into()))?;
    let entry = entry_of(&handle, &request.workflow_name).await?;
    Ok((request.workflow_name, entry))
}

/// `PATCH /admin/schedules/{id}`.
async fn patch(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let outcome = patch_inner(&state, &id, &headers, &body).await;
    let row = Audit {
        operation: OP_SCHEDULE_UPDATE,
        target_type: TARGET_SCHEDULE,
        target_id: Some(&id),
        route: "PATCH /admin/schedules/{id}",
    };
    audit(&state, &headers, &row, failure(&outcome)).await;
    outcome
}

async fn patch_inner(
    state: &ApiState,
    id: &str,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Json<Value>, AutumnError> {
    let request: PatchBody = optional_body(body)?;
    if request.workflow_name.is_some() {
        return Err(AutumnError::bad_request_msg(
            "workflow_name is not editable; create a new schedule to target a different workflow",
        ));
    }
    request.refuse_unsupported()?;
    let handle = state.handle()?;
    let stored = load_one(&handle, id).await?;
    let name = stored.workflow_name.clone();
    let is_code = is_code_schedule(&handle, &name);
    request.check_override_fields(is_code, schedule_id(&name))?;
    let overlap = request
        .overlap_policy
        .as_deref()
        .map(parse_overlap)
        .transpose()?;
    if is_code {
        let schedule = request.schedule(&stored.schedule)?;
        let actor = state.flags.extract_actor(headers);
        override_code(&handle, &name, &request, schedule, overlap, actor).await?;
    } else {
        let job_name = name.clone();
        handle
            .run(move |runtime| edit_in_place(runtime, &job_name, &request, overlap))
            .await
            .map_err(schedule_error)??;
    }
    entry_of(&handle, &name).await
}

/// Edit an API schedule in place, on the driver task.
///
/// The read, the merge and the upsert run in one job. Two overlapping
/// `PATCH` requests then apply one after the other, so the second keeps
/// the fields that the first changed.
fn edit_in_place(
    runtime: &mut autumn_harvest_sqlite::SqliteRuntime,
    name: &str,
    request: &PatchBody,
    overlap: Option<OverlapPolicy>,
) -> Result<(), AutumnError> {
    let stored = runtime
        .list_schedules()
        .map_err(|error| schedule_error(error.into()))?
        .into_iter()
        .find(|row| row.workflow_name == name)
        .ok_or_else(|| AutumnError::not_found_msg(format!("schedule {}", schedule_id(name))))?;
    let schedule = request.schedule(&stored.schedule)?;
    let mut code = WorkflowSchedule::new(name.to_owned(), schedule.unwrap_or(stored.schedule))
        .with_input(request.input.clone().unwrap_or(stored.input))
        .with_paused(stored.paused);
    code.max_active_runs = request.max_active_runs.unwrap_or(stored.max_active_runs);
    code.overlap_policy = overlap.unwrap_or(stored.overlap_policy);
    runtime
        .upsert_schedule(&code)
        .map_err(|error| schedule_error(error.into()))
}

/// Set or clear the operator override of a code schedule (issue #2390).
///
/// The caller has run `check_override_fields`, so a clear comes alone. The
/// engine refuses a field it cannot override by name, before any write.
async fn override_code(
    handle: &HarvestSqlite,
    name: &str,
    request: &PatchBody,
    schedule: Option<Schedule>,
    overlap: Option<OverlapPolicy>,
    actor: String,
) -> Result<(), AutumnError> {
    let patch = WorkflowSchedulePatch {
        schedule,
        input: request.input.clone(),
        max_active_runs: request.max_active_runs,
        overlap_policy: overlap,
        ..WorkflowSchedulePatch::default()
    };
    let clear = request.clear_override;
    let reason = request.reason.clone();
    let name = name.to_owned();
    handle
        .run(move |runtime| {
            if clear {
                runtime.clear_schedule_override(&name)?;
            }
            if autumn_harvest::schedule_patch::workflow_fields_are_empty(&patch) {
                return Ok(());
            }
            runtime
                .set_schedule_override(&name, &patch, &actor, reason.as_deref())
                .map(|_| ())
        })
        .await
        .map_err(schedule_error)?
        .map_err(|error: SqliteError| schedule_error(error.into()))
}

async fn pause(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    set_paused(&state, &id, &headers, &body, true).await
}

async fn resume(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    set_paused(&state, &id, &headers, &body, false).await
}

/// Pause or resume. A pause stores its optional `reason`, as on Postgres.
/// A resume clears it.
async fn set_paused(
    state: &ApiState,
    id: &str,
    headers: &HeaderMap,
    body: &Bytes,
    paused: bool,
) -> Result<Json<Value>, AutumnError> {
    let outcome = async {
        let request: ReasonBody = optional_body(body)?;
        let handle = state.handle()?;
        let name = load_one(&handle, id).await?.workflow_name;
        let reason = request.reason;
        handle
            .run(move |runtime| {
                if paused {
                    runtime.pause_schedule_with_reason(&name, reason.as_deref())
                } else {
                    runtime.resume_schedule(&name)
                }
            })
            .await
            .map_err(schedule_error)?
            .map_err(|error| schedule_error(error.into()))
    }
    .await;
    let (operation, route) = if paused {
        (OP_SCHEDULE_PAUSE, "POST /admin/schedules/{id}/pause")
    } else {
        (OP_SCHEDULE_RESUME, "POST /admin/schedules/{id}/resume")
    };
    let row = Audit {
        operation,
        target_type: TARGET_SCHEDULE,
        target_id: Some(id),
        route,
    };
    audit(state, headers, &row, failure(&outcome)).await;
    outcome.map(|()| Json(json!({ "ok": true })))
}

async fn delete(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    headers: HeaderMap,
) -> Result<Json<Value>, AutumnError> {
    let outcome = async {
        let handle = state.handle()?;
        let name = load_one(&handle, &id).await?.workflow_name;
        // The next boot would upsert it again, as on Postgres, which refuses.
        if is_code_schedule(&handle, &name) {
            return Err(AutumnError::bad_request_msg(format!(
                "schedule {id} is managed by the in-process workflow schedule catalog \
                 (HarvestPlugin::workflow_schedule); remove it from code, or pause it"
            )));
        }
        handle
            .run(move |runtime| runtime.delete_schedule(&name))
            .await
            .map_err(schedule_error)?
            .map_err(|error| schedule_error(error.into()))
    }
    .await;
    let row = Audit {
        operation: OP_SCHEDULE_DELETE,
        target_type: TARGET_SCHEDULE,
        target_id: Some(&id),
        route: "DELETE /admin/schedules/{id}",
    };
    audit(&state, &headers, &row, failure(&outcome)).await;
    outcome.map(|()| Json(json!({ "ok": true })))
}

/// `POST /admin/schedules/{id}/trigger`. `?force=true` triggers a paused
/// schedule. A paused schedule without it is a `409`, as on Postgres.
async fn trigger(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AutumnError> {
    let outcome = trigger_inner(&state, &id, query.as_deref(), &body).await;
    let row = Audit {
        operation: OP_SCHEDULE_TRIGGER,
        target_type: TARGET_SCHEDULE,
        target_id: Some(&id),
        route: "POST /admin/schedules/{id}/trigger",
    };
    audit(&state, &headers, &row, failure(&outcome)).await;
    outcome
}

async fn trigger_inner(
    state: &ApiState,
    id: &str,
    query: Option<&str>,
    body: &Bytes,
) -> Result<Json<Value>, AutumnError> {
    let force = force_of(query)?;
    let request: TriggerBody = optional_body(body)?;
    let overlap = request
        .overlap_policy
        .as_deref()
        .map(parse_overlap)
        .transpose()?;
    let handle = state.handle()?;
    let stored = load_one(&handle, id).await?;
    let name = stored.workflow_name.clone();
    let triggered_at = Utc::now();
    let outcome = handle
        .run(move |runtime| runtime.trigger_schedule(&name, force, overlap))
        .await
        .map_err(schedule_error)?
        .map_err(|error| schedule_error(error.into()))?;
    let id = schedule_id(&stored.workflow_name);
    match outcome {
        ScheduleTrigger::Fired {
            exec_id,
            workflow_id,
        } => Ok(Json(json!({
            "execution_id": exec_id.to_string(),
            "workflow_id": workflow_id,
            "triggered_at": triggered_at,
            "outcome": "fired",
        }))),
        ScheduleTrigger::SkippedOverlap { workflow_id } => Ok(Json(json!({
            "workflow_id": workflow_id,
            "triggered_at": triggered_at,
            "outcome": "skipped_overlap",
        }))),
        ScheduleTrigger::Paused => Err(AutumnError::bad_request_msg(format!(
            "schedule {id} is paused; pass ?force=true to trigger a paused schedule"
        ))
        .with_status(StatusCode::CONFLICT)),
        _ => Err(AutumnError::internal_server_error_msg(
            "unknown schedule trigger outcome",
        )),
    }
}

/// The `force` query flag. Any other key is a `400`, so no flag is ignored.
fn force_of(query: Option<&str>) -> Result<bool, AutumnError> {
    let mut force = false;
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
    {
        match pair.split_once('=').unwrap_or((pair, "")) {
            ("force", "true") => force = true,
            ("force", "false") => force = false,
            ("force", other) => {
                return Err(AutumnError::bad_request_msg(format!(
                    "invalid force '{other}'; use true or false"
                )));
            }
            (key, _) => {
                return Err(AutumnError::bad_request_msg(format!(
                    "not supported on the SQLite backend: query parameter {key} \
                     (autumn-harvest#2489)"
                )));
            }
        }
    }
    Ok(force)
}

// ── Retention routes ─────────────────────────────────────────────────────────

/// `GET /admin/retention`: the config and the last pass, in the Postgres
/// `RetentionStatus` shape. One file is one shard.
async fn retention_status(State(state): State<ApiState>) -> Result<Response, AutumnError> {
    let handle = state.handle()?;
    Ok(Json(handle.retention().status()).into_response())
}

/// `POST /admin/retention/run-now`. It queues a pass, as on Postgres. The
/// driver runs it on its next turn. `GET /admin/retention` shows the result.
async fn run_now(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AutumnError> {
    let outcome = state.handle().map(|handle| handle.retention().request());
    let row = Audit {
        operation: OP_RETENTION_RUN_NOW,
        target_type: TARGET_RETENTION,
        target_id: None,
        route: "POST /admin/retention/run-now",
    };
    audit(&state, &headers, &row, failure(&outcome)).await;
    outcome.map(|()| Json(json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_schedule_id_is_stable_and_differs_by_name() {
        assert_eq!(schedule_id("tick"), schedule_id("tick"));
        assert_ne!(schedule_id("tick"), schedule_id("tock"));
        assert_eq!(schedule_id("tick").get_version_num(), 5);
    }

    #[test]
    fn the_expression_round_trips_the_postgres_form() {
        for (schedule, expr, tz) in [
            (Schedule::Cron("0 3 * * *".into()), "cron:0 3 * * *", "UTC"),
            (
                Schedule::CronInTimezone {
                    expr: "0 9 * * 1-5".into(),
                    tz: "Europe/London".into(),
                },
                "cron_tz:Europe/London:0 9 * * 1-5",
                "Europe/London",
            ),
            (
                Schedule::Interval(Duration::from_secs(90)),
                "interval:90",
                "UTC",
            ),
            (Schedule::Manual, "manual", "UTC"),
        ] {
            let (got, zone) = expr_of(&schedule);
            assert_eq!((got.as_str(), zone.as_str()), (expr, tz));
            let parsed = parse_expr(expr, "UTC").unwrap();
            assert_eq!(expr_of(&parsed).0, expr, "{expr} parses back");
        }
    }

    #[test]
    fn force_takes_true_or_false_and_refuses_other_keys() {
        assert!(!force_of(None).unwrap());
        assert!(force_of(Some("force=true")).unwrap());
        assert!(!force_of(Some("force=false")).unwrap());
        assert!(force_of(Some("force=yes")).is_err());
        assert!(force_of(Some("dry_run=true")).is_err());
    }
}
