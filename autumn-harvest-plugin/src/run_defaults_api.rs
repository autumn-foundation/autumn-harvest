//! `GET` and `PATCH /admin/workflow-types/{workflow_name}/run-defaults`
//! (issue #2387).
//!
//! The routes read and change the operator override of the `sla` and
//! `execution_timeout` of one workflow type. A DAG uses its own name. The
//! override lives on the default shard. See
//! [`autumn_harvest::run_defaults`] for the precedence and the cache.
//!
//! A change reaches new starts in this process at once, through a write
//! through to the cache. Other processes see it within their refresh period.
//! `recompute_in_flight` also moves the SLA deadline of the runs that are in
//! flight, on every shard.

use std::time::Duration;

use autumn_harvest::audit::{
    self, OP_WORKFLOW_TYPE_RUN_DEFAULTS, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_WORKFLOW_TYPE,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::run_defaults::{
    RunDefaultChange, RunDefaultFields, RunDefaultOverride, RunDefaultPatch,
    get_run_default_override, recompute_in_flight_sla, update_run_default_override,
};
use autumn_web::AutumnError;
use autumn_web::reexports::axum;
use axum::extract::{Extension, Json, Path};
use diesel_async::AsyncPgConnection;
use serde::{Deserialize, Serialize};

use crate::api::{
    HarvestApiRuntime, HarvestApiState, acquire_conn, audit_context, deserialize_tristate,
    map_error,
};
use crate::shard_fanout::UnavailableShard;

const ROUTE: &str = "PATCH /admin/workflow-types/{workflow_name}/run-defaults";

/// The two run-default fields, in whole seconds. `None` sets no value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RunDefaultSecs {
    /// The soft SLA.
    pub sla_secs: Option<u64>,
    /// The hard execution timeout.
    pub execution_timeout_secs: Option<u64>,
}

impl From<RunDefaultFields> for RunDefaultSecs {
    fn from(fields: RunDefaultFields) -> Self {
        Self {
            sla_secs: fields.sla.map(|d| d.as_secs()),
            execution_timeout_secs: fields.execution_timeout.map(|d| d.as_secs()),
        }
    }
}

/// The stored override, as the routes show it.
#[derive(Debug, Clone, Serialize)]
pub struct RunDefaultOverrideView {
    /// The soft SLA that the override sets, if any.
    pub sla_secs: Option<u64>,
    /// The hard execution timeout that the override sets, if any.
    pub execution_timeout_secs: Option<u64>,
    /// The operator's stated reason.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override last changed.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<RunDefaultOverride> for RunDefaultOverrideView {
    fn from(stored: RunDefaultOverride) -> Self {
        let secs = RunDefaultSecs::from(stored.fields);
        Self {
            sla_secs: secs.sla_secs,
            execution_timeout_secs: secs.execution_timeout_secs,
            reason: stored.reason,
            updated_by: stored.updated_by,
            updated_at: stored.updated_at,
        }
    }
}

/// The result of an in-flight recompute.
#[derive(Debug, Clone, Serialize)]
pub struct RecomputeReport {
    /// The number of runs whose SLA deadline moved.
    pub moved_runs: u64,
    /// The shards that the recompute could not reach. Their runs keep the old
    /// SLA. Send the request again to retry them.
    pub unavailable_shards: Vec<UnavailableShard>,
    /// Whether a newer change to the SLA of the type stopped this recompute.
    /// That change then decides the SLA of the in-flight runs.
    pub superseded: bool,
}

/// The run defaults of one workflow type.
#[derive(Debug, Clone, Serialize)]
pub struct RunDefaultsView {
    /// The workflow type name.
    pub workflow_name: String,
    /// The values declared in code.
    pub code: RunDefaultSecs,
    /// The stored override, or `null` when there is none.
    #[serde(rename = "override")]
    pub override_: Option<RunDefaultOverrideView>,
    /// The values that a new run takes, before the start clamps.
    pub effective: RunDefaultSecs,
    /// The in-flight recompute. Present only on a `PATCH` that asked for one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recompute: Option<RecomputeReport>,
}

/// The body of `PATCH /admin/workflow-types/{workflow_name}/run-defaults`.
///
/// Each duration is tri-state. An absent key keeps the stored override. A
/// `null` returns the field to the code value. A number sets the override.
#[expect(
    clippy::option_option,
    reason = "serde tri-state: absent keeps, null returns to code, a value sets"
)]
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunDefaultsPatchRequest {
    #[serde(default, deserialize_with = "deserialize_tristate")]
    sla_secs: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    execution_timeout_secs: Option<Option<u64>>,
    #[serde(default)]
    clear_override: bool,
    reason: Option<String>,
    #[serde(default)]
    recompute_in_flight: bool,
}

impl RunDefaultsPatchRequest {
    fn patch(&self) -> RunDefaultPatch {
        let to_duration = |value: Option<Option<u64>>| value.map(|v| v.map(Duration::from_secs));
        RunDefaultPatch {
            execution_timeout: to_duration(self.execution_timeout_secs),
            sla: to_duration(self.sla_secs),
        }
    }

    const fn is_empty(&self) -> bool {
        self.sla_secs.is_none()
            && self.execution_timeout_secs.is_none()
            && !self.clear_override
            && !self.recompute_in_flight
    }
}

/// The code values of a type, or `None` when this process does not register it.
///
/// A unified DAG counts, because its runs take the values of its shadow type.
fn code_fields(runtime: &HarvestApiRuntime, workflow_name: &str) -> Option<RunDefaultFields> {
    match runtime.registry().workflows.get(workflow_name) {
        Some(info) => Some(RunDefaultFields {
            execution_timeout: info.execution_timeout,
            sla: info.sla,
        }),
        None if runtime.is_registered_dag(workflow_name) => Some(RunDefaultFields::default()),
        None => None,
    }
}

/// The values that a new run takes: the override, then code.
fn effective_fields(
    code: RunDefaultFields,
    stored: Option<&RunDefaultOverride>,
) -> RunDefaultFields {
    let (execution_timeout, sla) = stored.map_or((code.execution_timeout, code.sla), |stored| {
        stored.fields.apply(code.execution_timeout, code.sla)
    });
    RunDefaultFields {
        execution_timeout,
        sla,
    }
}

fn view(
    workflow_name: &str,
    code: RunDefaultFields,
    stored: Option<RunDefaultOverride>,
    recompute: Option<RecomputeReport>,
) -> RunDefaultsView {
    let effective = effective_fields(code, stored.as_ref());
    RunDefaultsView {
        workflow_name: workflow_name.to_string(),
        code: code.into(),
        override_: stored.map(Into::into),
        effective: effective.into(),
        recompute,
    }
}

fn not_registered(workflow_name: &str) -> AutumnError {
    AutumnError::not_found_msg(format!("workflow type '{workflow_name}' is not registered"))
}

/// `GET /admin/workflow-types/{workflow_name}/run-defaults`.
pub async fn get_run_defaults(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
) -> Result<Json<RunDefaultsView>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let code =
        code_fields(&runtime, &workflow_name).ok_or_else(|| not_registered(&workflow_name))?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let stored = get_run_default_override(&mut conn, &workflow_name)
        .await
        .map_err(map_error)?;
    Ok(Json(view(&workflow_name, code, stored, None)))
}

/// Insert one `workflow_type.run_defaults` audit row.
///
/// A failed request logs its audit error and keeps its own error. A
/// succeeded request returns the audit error, because an unaudited change
/// must not report success.
async fn audit_patch(
    conn: &mut AsyncPgConnection,
    ctx: (&str, &str, Option<&str>),
    workflow_name: &str,
    error_summary: Option<&str>,
) -> Result<(), AutumnError> {
    let (actor, source, request_id) = ctx;
    let record = NewAuditRecord {
        actor,
        operation: OP_WORKFLOW_TYPE_RUN_DEFAULTS,
        target_type: TARGET_WORKFLOW_TYPE,
        target_id: Some(workflow_name),
        route_or_command: ROUTE,
        request_id,
        idempotency_key: None,
        status: if error_summary.is_some() {
            STATUS_FAILED
        } else {
            STATUS_SUCCEEDED
        },
        error_summary,
        shard_id: None,
        source,
    };
    match audit::insert_audit(conn, &record).await {
        Ok(_) => Ok(()),
        Err(error) if error_summary.is_some() => {
            tracing::warn!(error = %error, workflow_name, "audit insert failed for run defaults");
            Ok(())
        }
        Err(error) => Err(map_error(error)),
    }
}

/// Move the SLA of the in-flight runs of a type on every shard.
///
/// One unreachable shard does not stop the others. The report names it. A
/// shard other than the default one also takes a default-shard connection,
/// to read the stored override before each batch. It takes it only while it
/// holds no other default-shard connection, so a pool of one is enough.
async fn recompute_on_every_shard(
    api_state: &HarvestApiState,
    workflow_name: &str,
    code_sla: Option<Duration>,
    sla: Option<Duration>,
) -> Result<RecomputeReport, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let default_shard = pool.default_shard();
    let mut report = RecomputeReport {
        moved_runs: 0,
        unavailable_shards: Vec::new(),
        superseded: false,
    };
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        let on_default =
            shard == default_shard || pool.sharded_pool().same_physical_pool(shard, default_shard);
        let outcome = async {
            let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
            let mut store = if on_default {
                None
            } else {
                Some(
                    acquire_conn(pool.default_pool())
                        .await
                        .map_err(|e| e.to_string())?,
                )
            };
            recompute_in_flight_sla(
                &mut conn,
                store.as_deref_mut(),
                workflow_name,
                code_sla,
                sla,
            )
            .await
            .map_err(|error| error.to_string())
        }
        .await;
        match outcome {
            Ok(outcome) => {
                report.moved_runs += outcome.moved;
                if outcome.superseded {
                    // A newer change owns the in-flight runs now.
                    report.superseded = true;
                    break;
                }
            }
            Err(reason) => report
                .unavailable_shards
                .push(UnavailableShard { shard_id, reason }),
        }
    }
    Ok(report)
}

/// `PATCH /admin/workflow-types/{workflow_name}/run-defaults`.
pub async fn patch_run_defaults(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    headers: axum::http::HeaderMap,
    request: Result<Json<RunDefaultsPatchRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<RunDefaultsView>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let ctx = (actor.as_str(), source.as_str(), request_id.as_deref());

    // Unwrapped in the handler, so a bad body is this route's 400 with an
    // audit row, not the plain-text 422 of axum.
    let request = match request {
        Ok(Json(request)) if request.is_empty() => Err(String::from(
            "empty patch: set sla_secs, execution_timeout_secs, clear_override or \
             recompute_in_flight",
        )),
        Ok(Json(request)) => Ok(request),
        Err(rejection) => Err(format!("invalid request body: {}", rejection.body_text())),
    };
    let request = match request {
        Ok(request) => request,
        Err(summary) => {
            audit_patch(&mut conn, ctx, &workflow_name, Some(&summary)).await?;
            return Err(AutumnError::bad_request_msg(summary));
        }
    };
    let Some(code) = code_fields(&runtime, &workflow_name) else {
        audit_patch(
            &mut conn,
            ctx,
            &workflow_name,
            Some("workflow type not registered"),
        )
        .await?;
        return Err(not_registered(&workflow_name));
    };

    let change = RunDefaultChange {
        workflow_name: &workflow_name,
        patch: request.patch(),
        clear: request.clear_override,
        actor: &actor,
        reason: request.reason.as_deref(),
    };
    let stored = match update_run_default_override(&mut conn, &change).await {
        Ok(stored) => stored,
        Err(error) => {
            let summary = error.to_string();
            audit_patch(&mut conn, ctx, &workflow_name, Some(&summary)).await?;
            return Err(match error {
                HarvestError::Config(message) => {
                    AutumnError::bad_request_msg(format!("invalid run-default override: {message}"))
                }
                other => map_error(other),
            });
        }
    };
    // Write through, so this process applies the change to its next start.
    let cache = runtime.registry().run_defaults();
    cache.write_through(
        &workflow_name,
        stored.as_ref().map(|s| s.fields).unwrap_or_default(),
    );
    audit_patch(&mut conn, ctx, &workflow_name, None).await?;
    // The recompute takes its own connections, the default shard among them.
    // Holding this one would starve a small pool.
    drop(conn);

    let recompute = if request.recompute_in_flight {
        let effective = effective_fields(code, stored.as_ref());
        Some(recompute_on_every_shard(&api_state, &workflow_name, code.sla, effective.sla).await?)
    } else {
        None
    };
    Ok(Json(view(&workflow_name, code, stored, recompute)))
}
