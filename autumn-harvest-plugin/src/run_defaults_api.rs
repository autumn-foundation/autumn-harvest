//! `GET` and `PATCH /admin/workflow-types/{workflow_name}/run-defaults`
//! (issues #2387, #2406 and #2407).
//!
//! The routes read and change the operator override of the `sla`, the
//! `execution_timeout`, the `chain_execution_timeout` and the concurrency
//! `limit` of one workflow type. A DAG uses its own name. A DAG declares no
//! chain cap, so the route refuses a chain cap for a DAG. The override lives on
//! the default shard. See [`autumn_harvest::run_defaults`] for the precedence
//! and the cache.
//!
//! A change reaches new starts in this process at once, through a write
//! through to the cache. Other processes see it within their refresh period.
//! `recompute_in_flight` also moves the SLA deadline of the runs that are in
//! flight, on every shard. It never moves a chain deadline. A chain in flight
//! keeps the deadline that its first run stored.
//!
//! A change to the concurrency limit applies to the next start. The route
//! also raises the stored cap of the live task rows on every shard. So a
//! higher limit admits the waiting starts at the next claim. It never lowers
//! a stored cap. It refuses a limit for a type with no concurrency policy. It
//! also refuses a limit for a latest-wins type, because a later drop of that
//! limit cancels runs in flight.

use std::time::Duration;

use autumn_harvest::audit::{
    self, OP_WORKFLOW_TYPE_RUN_DEFAULTS, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_WORKFLOW_TYPE,
};
use autumn_harvest::concurrency::ConcurrencyPolicy;
use autumn_harvest::error::HarvestError;
use autumn_harvest::models::NewAuditRecord;
use autumn_harvest::run_defaults::{
    RUN_DEFAULTS_TTL, RunDefaultChange, RunDefaultFields, RunDefaultOverride, RunDefaultPatch,
    get_run_default_override, raise_waiting_concurrency_caps, recompute_in_flight_sla,
    update_run_default_override,
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
use crate::state::HarvestDbPool;

const ROUTE: &str = "PATCH /admin/workflow-types/{workflow_name}/run-defaults";

/// The run-default fields, in whole seconds. `None` sets no value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RunDefaultSecs {
    /// The soft SLA.
    pub sla_secs: Option<u64>,
    /// The hard execution timeout.
    pub execution_timeout_secs: Option<u64>,
    /// The cap of a continue-as-new chain (issue #2406).
    pub chain_execution_timeout_secs: Option<u64>,
    /// The concurrency limit of a new start, a count (issue #2407).
    pub concurrency_limit: Option<u32>,
}

impl From<RunDefaultFields> for RunDefaultSecs {
    fn from(fields: RunDefaultFields) -> Self {
        Self {
            sla_secs: fields.sla.map(|d| d.as_secs()),
            execution_timeout_secs: fields.execution_timeout.map(|d| d.as_secs()),
            chain_execution_timeout_secs: fields.chain_execution_timeout.map(|d| d.as_secs()),
            concurrency_limit: fields.concurrency_limit,
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
    /// The chain cap that the override sets, if any (issue #2406).
    pub chain_execution_timeout_secs: Option<u64>,
    /// The concurrency limit that the override sets, if any (issue #2407).
    pub concurrency_limit: Option<u32>,
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
            chain_execution_timeout_secs: secs.chain_execution_timeout_secs,
            concurrency_limit: secs.concurrency_limit,
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

/// The result of the raise of the waiting starts (issue #2407).
#[derive(Debug, Clone, Serialize)]
pub struct ConcurrencyRaiseReport {
    /// The number of waiting task rows whose stored cap went up.
    pub raised_tasks: u64,
    /// The shards that the raise could not reach. Their waiting starts keep
    /// the old cap. Send the request again to retry them.
    pub unavailable_shards: Vec<UnavailableShard>,
    /// Whether a newer change to the limit stopped this raise.
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
    /// The raise of the waiting starts. Present only on a `PATCH` that names
    /// `concurrency_limit` or `clear_override`, for a type with a policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency_raise: Option<ConcurrencyRaiseReport>,
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
    #[serde(default, deserialize_with = "deserialize_tristate")]
    chain_execution_timeout_secs: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_tristate")]
    concurrency_limit: Option<Option<u32>>,
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
            chain_execution_timeout: to_duration(self.chain_execution_timeout_secs),
            concurrency_limit: self.concurrency_limit,
        }
    }

    const fn is_empty(&self) -> bool {
        self.sla_secs.is_none()
            && self.execution_timeout_secs.is_none()
            && self.chain_execution_timeout_secs.is_none()
            && self.concurrency_limit.is_none()
            && !self.clear_override
            && !self.recompute_in_flight
    }

    /// Whether the request sets a chain cap. A `null` sets none.
    const fn sets_chain_cap(&self) -> bool {
        matches!(self.chain_execution_timeout_secs, Some(Some(_)))
    }

    /// The concurrency limit that the request sets. A `null` sets none.
    const fn sets_concurrency_limit(&self) -> Option<u32> {
        match self.concurrency_limit {
            Some(Some(limit)) => Some(limit),
            _ => None,
        }
    }

    /// Whether the request can change the effective concurrency limit.
    const fn touches_concurrency_limit(&self) -> bool {
        self.concurrency_limit.is_some() || self.clear_override
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
            chain_execution_timeout: info.chain_execution_timeout,
            concurrency_limit: info.concurrency.map(|policy| policy.limit),
        }),
        None if runtime.is_registered_dag(workflow_name) => Some(RunDefaultFields::default()),
        None => None,
    }
}

/// The code concurrency policy of a type. A DAG declares none.
fn code_policy(runtime: &HarvestApiRuntime, workflow_name: &str) -> Option<ConcurrencyPolicy> {
    runtime
        .registry()
        .workflows
        .get(workflow_name)
        .and_then(|info| info.concurrency)
}

/// The values that a new run takes: the override, then code.
///
/// `policy` is the code concurrency policy. A latest-wins policy takes the
/// larger limit, as at start.
fn effective_fields(
    code: RunDefaultFields,
    policy: Option<ConcurrencyPolicy>,
    stored: Option<&RunDefaultOverride>,
) -> RunDefaultFields {
    let Some(stored) = stored else {
        return code;
    };
    let (execution_timeout, sla) = stored.fields.apply(code.execution_timeout, code.sla);
    RunDefaultFields {
        execution_timeout,
        sla,
        chain_execution_timeout: stored.fields.apply_chain(code.chain_execution_timeout),
        concurrency_limit: stored
            .fields
            .apply_concurrency(policy)
            .map(|policy| policy.limit),
    }
}

/// The reports that a `PATCH` adds to the view.
#[derive(Default)]
struct Reports {
    recompute: Option<RecomputeReport>,
    concurrency_raise: Option<ConcurrencyRaiseReport>,
}

fn view(
    workflow_name: &str,
    code: RunDefaultFields,
    policy: Option<ConcurrencyPolicy>,
    stored: Option<RunDefaultOverride>,
    reports: Reports,
) -> RunDefaultsView {
    let effective = effective_fields(code, policy, stored.as_ref());
    RunDefaultsView {
        workflow_name: workflow_name.to_string(),
        code: code.into(),
        override_: stored.map(Into::into),
        effective: effective.into(),
        recompute: reports.recompute,
        concurrency_raise: reports.concurrency_raise,
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
    let policy = code_policy(&runtime, &workflow_name);
    Ok(Json(view(
        &workflow_name,
        code,
        policy,
        stored,
        Reports::default(),
    )))
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

/// A change that a `PATCH` applies to the rows of a type on every shard.
#[derive(Debug, Clone, Copy)]
enum ShardJob {
    /// Move the SLA of the in-flight runs (issue #2387).
    Recompute {
        code_sla: Option<Duration>,
        sla: Option<Duration>,
    },
    /// Raise the stored cap of the live workflow task rows (issue #2407).
    Raise { code: ConcurrencyPolicy, limit: u32 },
}

impl ShardJob {
    /// Run the job on one shard. Returns the changed rows and whether a newer
    /// change stopped the job.
    async fn run(
        self,
        conn: &mut AsyncPgConnection,
        store: Option<&mut AsyncPgConnection>,
        workflow_name: &str,
    ) -> Result<(u64, bool), HarvestError> {
        match self {
            Self::Recompute { code_sla, sla } => {
                recompute_in_flight_sla(conn, store, workflow_name, code_sla, sla)
                    .await
                    .map(|done| (done.moved, done.superseded))
            }
            Self::Raise { code, limit } => {
                raise_waiting_concurrency_caps(conn, store, workflow_name, code, limit)
                    .await
                    .map(|done| (done.raised, done.superseded))
            }
        }
    }
}

/// The result of one [`ShardJob`] on every shard.
#[derive(Debug, Default)]
struct ShardPass {
    changed: u64,
    unavailable_shards: Vec<UnavailableShard>,
    superseded: bool,
}

/// Run `job` on every shard.
///
/// One unreachable shard does not stop the others. The pass names it. A
/// shard other than the default one also takes a default-shard connection,
/// so the job can read the stored override before each batch. It takes it
/// only while it holds no other default-shard connection, so a pool of one
/// is enough. A superseded job stops the pass, because a newer change owns
/// the rows now.
async fn on_every_shard(pool: &HarvestDbPool, workflow_name: &str, job: ShardJob) -> ShardPass {
    let default_shard = pool.default_shard();
    let mut pass = ShardPass::default();
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
            job.run(&mut conn, store.as_deref_mut(), workflow_name)
                .await
                .map_err(|error| error.to_string())
        }
        .await;
        match outcome {
            Ok((changed, superseded)) => {
                pass.changed += changed;
                if superseded {
                    pass.superseded = true;
                    break;
                }
            }
            Err(reason) => pass
                .unavailable_shards
                .push(UnavailableShard { shard_id, reason }),
        }
    }
    if !pass.unavailable_shards.is_empty() {
        let shard_ids: Vec<i32> = pass
            .unavailable_shards
            .iter()
            .map(|shard| shard.shard_id)
            .collect();
        tracing::warn!(
            workflow_name,
            ?job,
            ?shard_ids,
            "run-defaults job could not reach every shard"
        );
    }
    pass
}

/// The base wait before the settle raises of the live rows (issue #2407).
///
/// Another process applies the new limit only after its cache reloads,
/// within [`RUN_DEFAULTS_TTL`] plus one refresh period. A start that it
/// stores before then takes the old cap. A settle raise lifts that start. It
/// also lifts a row that an earlier raise skipped, because another
/// transaction held it.
pub const CONCURRENCY_RAISE_SETTLE_DELAY: Duration =
    Duration::from_secs(2 * RUN_DEFAULTS_TTL.as_secs());

/// The settle raises run at these multiples of the base wait: 10 seconds,
/// 1 minute and 5 minutes with [`CONCURRENCY_RAISE_SETTLE_DELAY`].
///
/// A later raise lifts a row that an earlier one missed. For example, a
/// caller transaction can hold an uncommitted start through the first one.
const SETTLE_MULTIPLES: [u32; 3] = [1, 6, 30];

/// Raise the stored cap of the live rows of a type on every shard, now and
/// again at each settle wait (issue #2407).
///
/// Each raise runs in its own task, so a client that disconnects stops none
/// of them. The response reports the first raise. A settle raise logs a
/// shard that it could not reach. A settle raise stops when a newer change
/// replaced the limit.
async fn raise_on_every_shard(
    api_state: &HarvestApiState,
    pool: &HarvestDbPool,
    workflow_name: &str,
    code: ConcurrencyPolicy,
    limit: u32,
) -> Result<ConcurrencyRaiseReport, AutumnError> {
    let job = ShardJob::Raise { code, limit };
    let first = {
        let pool = pool.clone();
        let workflow_name = workflow_name.to_string();
        tokio::spawn(async move { on_every_shard(&pool, &workflow_name, job).await })
    };
    let base = api_state.concurrency_raise_settle_delay();
    let pool = pool.clone();
    let name = workflow_name.to_string();
    tokio::spawn(async move {
        let mut waited = Duration::ZERO;
        for multiple in SETTLE_MULTIPLES {
            let at = base * multiple;
            tokio::time::sleep(at.saturating_sub(waited)).await;
            waited = at;
            if on_every_shard(&pool, &name, job).await.superseded {
                break;
            }
        }
    });
    let pass = first.await.map_err(|error| {
        AutumnError::internal_server_error_msg(format!("concurrency raise failed: {error}"))
    })?;
    Ok(ConcurrencyRaiseReport {
        raised_tasks: pass.changed,
        unavailable_shards: pass.unavailable_shards,
        superseded: pass.superseded,
    })
}

/// The reason to refuse the concurrency limit of a request, if any.
///
/// A latest-wins start cancels runs down to its limit. Any later drop of an
/// override, a clear included, would then cancel runs in flight. So a
/// latest-wins type refuses every override.
fn refuse_concurrency_limit(
    request: &RunDefaultsPatchRequest,
    policy: Option<ConcurrencyPolicy>,
) -> Option<String> {
    request.sets_concurrency_limit()?;
    let Some(policy) = policy else {
        return Some(
            "the type declares no concurrency policy, so concurrency_limit is refused".to_string(),
        );
    };
    policy.on_conflict.is_cancel_running().then(|| {
        "the type is latest-wins (cancel_running), so concurrency_limit is refused: \
         a later drop of the limit would cancel runs in flight"
            .to_string()
    })
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
            "empty patch: set sla_secs, execution_timeout_secs, \
             chain_execution_timeout_secs, concurrency_limit, clear_override or \
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
    if request.sets_chain_cap() && runtime.is_registered_dag(&workflow_name) {
        let summary = "a DAG declares no chain cap, so chain_execution_timeout_secs is refused";
        audit_patch(&mut conn, ctx, &workflow_name, Some(summary)).await?;
        return Err(AutumnError::bad_request_msg(summary));
    }
    let policy = code_policy(&runtime, &workflow_name);
    if let Some(summary) = refuse_concurrency_limit(&request, policy) {
        audit_patch(&mut conn, ctx, &workflow_name, Some(&summary)).await?;
        return Err(AutumnError::bad_request_msg(summary));
    }

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

    let effective = effective_fields(code, policy, stored.as_ref());
    let recompute = if request.recompute_in_flight {
        let job = ShardJob::Recompute {
            code_sla: code.sla,
            sla: effective.sla,
        };
        let pass = on_every_shard(&pool, &workflow_name, job).await;
        Some(RecomputeReport {
            moved_runs: pass.changed,
            unavailable_shards: pass.unavailable_shards,
            superseded: pass.superseded,
        })
    } else {
        None
    };
    let concurrency_raise = match (policy, effective.concurrency_limit) {
        (Some(policy), Some(limit)) if request.touches_concurrency_limit() => {
            Some(raise_on_every_shard(&api_state, &pool, &workflow_name, policy, limit).await?)
        }
        _ => None,
    };
    let reports = Reports {
        recompute,
        concurrency_raise,
    };
    Ok(Json(view(&workflow_name, code, policy, stored, reports)))
}
