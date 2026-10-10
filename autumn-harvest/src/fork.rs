//! Non-destructive fork of a workflow run (issue #2000).
//!
//! A fork copies a history prefix of a source run to a new execution with a
//! new workflow id. It never writes a source row, so it accepts a run in any
//! state. See `DESIGN-2000.md`.
//!
//! # Effects
//!
//! [`ForkEffects::Recorded`] is the default. In this mode, the fork never runs
//! an effect for real:
//!
//! - A remote activity gets a caller override, or the result that the source
//!   recorded for the same name, occurrence and input. With neither, it fails
//!   with [`ERROR_TYPE_FORK_EFFECT_UNAVAILABLE`]. The worker resolves it in the
//!   transaction that schedules it, so no worker runs it.
//! - A local activity, a child workflow, an external activity, an external
//!   signal and an external cancel fail the fork run before they run.
//! - Completion callbacks and completion triggers do not fire.
//!
//! [`ForkEffects::Live`] runs effects for real. The caller must ask for it.

use std::collections::{HashMap, HashSet};

use diesel::prelude::*;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::context::WorkflowCommand;
use crate::error::{HarvestError, HarvestResult, database_error};
pub use crate::event::ForkEffects;
use crate::event::WorkflowEvent;
use crate::models::{HarvestEvent, WorkflowExecution};
use crate::payload_codec::PayloadCodecs;
use crate::reset::{ResetInvalidPoint, ResetPlan, ResetPoint, ResetSkipReason};
use crate::schema::{harvest_events, harvest_task_queue, harvest_workflow_executions};
use crate::types::{ActivityExecId, ExecutionId, ShardId, StartSource};
use crate::worker::HandlerRegistry;

/// Error type of an activity that a recorded fork cannot serve.
pub const ERROR_TYPE_FORK_EFFECT_UNAVAILABLE: &str = "ForkEffectUnavailable";

/// Request body of `POST /workflows/{id}/fork`.
///
/// An unknown field is an error, so a typo such as `"effect"` cannot pass
/// silently.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowForkRequest {
    /// Where the fork starts. `None` means event `0` (`WorkflowStarted`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_point: Option<ResetPoint>,
    /// Operator-supplied reason.
    #[serde(default)]
    pub reason: String,
    /// Operator identity for audit.
    #[serde(default)]
    pub operator_id: String,
    /// How the fork resolves an effect. The default is `recorded`.
    #[serde(default)]
    pub effects: ForkEffects,
    /// Workflow id of the fork. `None` derives a new id from the source id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Input of the fork. Needs fork point `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// Results that replace the results of activities after the fork point.
    /// An override applies in both effects modes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activity_overrides: Vec<ForkActivityOverride>,
}

impl WorkflowForkRequest {
    fn normalized(mut self) -> Self {
        self.reason = non_empty_or(&self.reason, "workflow fork requested");
        self.operator_id = non_empty_or(&self.operator_id, "unknown");
        self.workflow_id = self
            .workflow_id
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty());
        self
    }
}

fn non_empty_or(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

/// One activity result that the caller sets for a fork.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkActivityOverride {
    /// Name of the activity.
    pub activity_name: String,
    /// 1-based count of `ActivityScheduled` events with this name.
    pub occurrence: u32,
    /// The result that the activity returns in the fork.
    pub output: Value,
}

/// Result of a committed fork.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkResult {
    /// The new execution.
    pub new_exec_id: ExecutionId,
    /// Workflow id of the new execution.
    pub workflow_id: String,
    /// The source execution. The fork did not change it.
    pub forked_from_exec_id: ExecutionId,
    /// Last source event copied into the fork.
    pub fork_event_id: i64,
    /// Number of source events copied into the fork.
    pub events_carried_over: usize,
    /// How the fork resolves an effect.
    pub effects: ForkEffects,
}

/// Errors of a fork.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowForkError {
    /// The fork point is not a valid decision boundary.
    #[error(transparent)]
    InvalidPoint(#[from] ResetInvalidPoint),
    /// The source history holds a continue-as-new.
    #[error("continue-as-new histories cannot be forked")]
    ContinueAsNew,
    /// The source payloads were erased (issue #495).
    #[error("workflow execution {exec_id} had its payloads erased, so it cannot be forked")]
    ErasedSource {
        /// The source execution.
        exec_id: ExecutionId,
    },
    /// Recorded mode cannot serve an effect after the fork point.
    #[error(
        "recorded mode cannot serve {kind} at event {event_id}; fork with effects = live to run it"
    )]
    UnservableEffect {
        /// The event type of the effect.
        kind: String,
        /// The source event id of the effect.
        event_id: i64,
    },
    /// The carried history holds a mutex grant.
    #[error(
        "event {event_id} grants durable mutex '{key}'; a fork cannot inherit a mutex, so fork \
         before the grant"
    )]
    CarriedMutex {
        /// The mutex key.
        key: String,
        /// The source event id of the grant.
        event_id: i64,
    },
    /// An input or an activity override is not valid.
    #[error("invalid fork override: {message}")]
    InvalidOverride {
        /// What is wrong.
        message: String,
    },
    /// Another live run holds the workflow id.
    #[error("workflow id '{workflow_id}' is in use by another run")]
    WorkflowIdInUse {
        /// The workflow id.
        workflow_id: String,
    },
    /// The fork lineage is deeper than the erased-lineage scan.
    #[error(
        "the fork lineage is deeper than {depth} links, so no erased ancestor can be ruled out"
    )]
    LineageTooDeep {
        /// The scan bound.
        depth: usize,
    },
    /// The source shard cannot take the fork, or the business key cannot be
    /// checked on every shard.
    #[error("{message}")]
    ShardRefused {
        /// What blocks the fork.
        message: String,
    },
    /// A storage or database error.
    #[error(transparent)]
    Harvest(#[from] HarvestError),
}

impl From<diesel::result::Error> for WorkflowForkError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Harvest(database_error(error))
    }
}

/// Fork `source_id` to a new execution. The source is never changed.
///
/// The fork runs in one transaction. It holds a `FOR SHARE` lock on the source
/// row, so an erasure cannot commit between the erasure check and the copy.
/// It reads source rows and inserts only new rows.
///
/// # Errors
///
/// Returns [`WorkflowForkError`] when the source or the request is not valid,
/// or when a write fails.
pub async fn fork_workflow_execution(
    conn: &mut AsyncPgConnection,
    source_id: ExecutionId,
    request: WorkflowForkRequest,
    registry: Option<&HandlerRegistry>,
) -> Result<ForkResult, WorkflowForkError> {
    let request = request.normalized();
    let codecs = registry.map_or(&*crate::store::DEFAULT_PAYLOAD_CODECS, |reg| {
        reg.payload_codecs()
    });
    // The first workflow task raises a dispatch hint (issue #1312). The
    // buffering scope holds the hint until the transaction commits.
    crate::dispatch::buffered_settled(Box::pin(
        conn.transaction::<ForkResult, WorkflowForkError, _>(async |conn| {
            fork_in_transaction(conn, source_id, &request, registry, codecs).await
        }),
    ))
    .await
}

async fn fork_in_transaction(
    conn: &mut AsyncPgConnection,
    source_id: ExecutionId,
    request: &WorkflowForkRequest,
    registry: Option<&HandlerRegistry>,
    codecs: &PayloadCodecs,
) -> Result<ForkResult, WorkflowForkError> {
    let source = harvest_workflow_executions::table
        .find(source_id.as_uuid())
        .for_share()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {source_id}")))?;
    // Issue #495: an erased source is refused in every effects mode. The
    // check reads the locked row, before any event is read.
    if crate::erase::execution_input_is_erased(&source.input) {
        return Err(WorkflowForkError::ErasedSource { exec_id: source_id });
    }
    // Erasure does not reach a fork, which is a new root. So a fork of a fork
    // of an erased run is refused too.
    if let Some(erased) = erased_fork_ancestor(conn, &source).await? {
        return Err(WorkflowForkError::ErasedSource { exec_id: erased });
    }
    if let (Some(input), Some(registry)) = (&request.input, registry) {
        check_input_override(registry, &source.workflow_name, input)?;
    }

    let rows = crate::reset::load_event_rows(conn, source_id).await?;
    let events = decode_rows(&rows, codecs)?;
    let fork_event_id = match &request.fork_point {
        None => 0,
        Some(point) => {
            crate::reset::resolve_reset_point(&events, point).map_err(skip_reason_to_error)?
        }
    };
    let plan = validate_fork(&events, fork_event_id, request)?;

    let workflow_id = fork_workflow_id(conn, &source, request).await?;

    let new_exec_id = ExecutionId::new_for_shard(ShardId::new(source.shard_id));
    let input = match &request.input {
        Some(input) => codecs.encode_column(input)?,
        None => source.input.clone(),
    };
    let row = crate::reset::ForkRow {
        workflow_id: &workflow_id,
        input,
        start_source: StartSource::Fork,
        // A completion callback notifies an outside system. Only a live fork
        // may do that.
        completion_callbacks: match request.effects {
            ForkEffects::Live => source.completion_callbacks.clone(),
            ForkEffects::Recorded => None,
        },
    };
    let fork = match crate::reset::insert_fork_row(conn, &source, new_exec_id, row).await {
        Ok(fork) => fork,
        Err(diesel::result::Error::DatabaseError(
            diesel::result::DatabaseErrorKind::UniqueViolation,
            _,
        )) => return Err(WorkflowForkError::WorkflowIdInUse { workflow_id }),
        Err(error) => return Err(error.into()),
    };

    let copied = copy_prefix(
        conn,
        new_exec_id,
        &rows,
        &events,
        fork_event_id,
        request,
        codecs,
    )
    .await?;
    // The fork shares the blobs of the source. Its own references keep them
    // alive after retention deletes the source. Only the rows that the fork
    // stores count, so a replaced input releases its blob.
    let mut keys = envelope_keys(&copied);
    if request.input.is_none() {
        keys.extend(
            crate::payload_store::extract_offload_ref(&source.input).map(|blob| blob.blob_key),
        );
    }
    let refs = crate::store::load_payload_refs(conn, source_id).await?;
    let reachable = refs_named_in(refs, &keys);
    crate::store::insert_payload_refs(conn, new_exec_id, &reachable).await?;
    let tail = fork_tail(source_id, fork_event_id, request);
    let tail_start = i32::try_from(plan.events_carried_over)
        .map_err(|_| HarvestError::Database("fork carried too many events".to_string()))?;
    crate::store::append_events_offloaded_with_codecs(
        conn,
        new_exec_id,
        &tail,
        tail_start,
        registry.and_then(HandlerRegistry::payload_offloader),
        codecs,
    )
    .await?;
    crate::reset::enqueue_fork_workflow_task(conn, &fork, new_exec_id, registry).await?;

    Ok(ForkResult {
        new_exec_id,
        workflow_id,
        forked_from_exec_id: source_id,
        fork_event_id,
        events_carried_over: plan.events_carried_over,
        effects: request.effects,
    })
}

/// The workflow id of the fork: the caller's choice, or a new derived id.
///
/// The source key routes by-id signals and updates to the real entity, so a
/// what-if fork must not take it over. A key that another run holds is
/// refused too.
async fn fork_workflow_id(
    conn: &mut AsyncPgConnection,
    source: &WorkflowExecution,
    request: &WorkflowForkRequest,
) -> Result<String, WorkflowForkError> {
    let workflow_id = request
        .workflow_id
        .clone()
        .unwrap_or_else(|| format!("{}-fork-{}", source.workflow_id, Uuid::new_v4().simple()));
    if workflow_id == source.workflow_id {
        return Err(WorkflowForkError::InvalidOverride {
            message: "a fork needs a workflow id other than the source id".to_string(),
        });
    }
    if workflow_id_in_use(conn, &source.workflow_name, &workflow_id).await? {
        return Err(WorkflowForkError::WorkflowIdInUse { workflow_id });
    }
    check_shards(conn, source, &workflow_id, request.workflow_id.is_some()).await?;
    Ok(workflow_id)
}

/// Apply the shard checks of a rerun (issue #777) to the fork.
///
/// The fork lands on the source shard, so that shard must accept new work.
/// The business-key index is shard-local. So a caller-chosen key is checked
/// on every readable shard, whatever shard it hashes to. Explicit placement
/// or a change of the writable set can leave a holder on another shard. A
/// shard that cannot be read fails closed. A derived key holds a new UUID, so
/// it needs no fan-out. With no process router, both checks pass, as for a
/// rerun.
async fn check_shards(
    conn: &mut AsyncPgConnection,
    source: &WorkflowExecution,
    workflow_id: &str,
    caller_chose_key: bool,
) -> Result<(), WorkflowForkError> {
    use crate::external_target_location::CrossShardOccupancy;

    let source_shard = ShardId::new(source.shard_id);
    let router = crate::shard::GLOBAL_SHARD_ROUTER
        .read()
        .ok()
        .and_then(|guard| guard.as_ref().cloned());
    if let Some(router) = &router
        && !router.is_writable(source_shard)
    {
        return Err(WorkflowForkError::ShardRefused {
            message: format!(
                "the source lives on shard {source_shard}, which is draining and takes no new run"
            ),
        });
    }
    let Some(expected) =
        router.map(|router| router.pick_for_new_workflow(&source.workflow_name, workflow_id))
    else {
        return Ok(());
    };
    if !caller_chose_key {
        return Ok(());
    }
    let occupancy = crate::execution::rerun_cross_shard_occupancy(
        conn,
        &source.workflow_name,
        workflow_id,
        source_shard,
    )
    .await;
    match occupancy {
        CrossShardOccupancy::Free => Ok(()),
        CrossShardOccupancy::Occupied { .. } => Err(WorkflowForkError::WorkflowIdInUse {
            workflow_id: workflow_id.to_string(),
        }),
        // No sharded pool is configured, so the source shard is the only
        // reachable one. Its own index check above covers it.
        CrossShardOccupancy::Indeterminate { uninspected }
            if uninspected.is_empty() && expected == source_shard =>
        {
            Ok(())
        }
        CrossShardOccupancy::Indeterminate { uninspected } => {
            let shards = uninspected
                .iter()
                .map(|u| format!("shard {} ({})", u.shard, u.reason))
                .collect::<Vec<_>>()
                .join(", ");
            Err(WorkflowForkError::ShardRefused {
                message: format!(
                    "workflow id '{workflow_id}' routes to shard {expected}, but these shards \
                     could not be checked for a run of the key: {shards}"
                ),
            })
        }
    }
}

/// The blob keys that the offload envelopes in the stored `rows` name.
pub(crate) fn envelope_keys<'a>(
    rows: impl IntoIterator<Item = &'a Value>,
) -> std::collections::HashSet<String> {
    rows.into_iter()
        .flat_map(crate::payload_store::refs_in_event_value)
        .map(|blob| blob.blob_key)
        .collect()
}

/// The references in `refs` whose blob key is in `keys`.
///
/// The keys come from parsed envelopes, so the match is exact. A text search
/// misses a key that JSON escapes. It also keeps a key that a longer key
/// contains.
pub(crate) fn refs_named_in(
    refs: Vec<crate::payload_store::OffloadedRef>,
    keys: &std::collections::HashSet<String>,
) -> Vec<crate::payload_store::OffloadedRef> {
    refs.into_iter()
        .filter(|blob| keys.contains(&blob.blob_key))
        .collect()
}

/// The marker and the override events that follow the carried prefix.
fn fork_tail(
    source_id: ExecutionId,
    fork_event_id: i64,
    request: &WorkflowForkRequest,
) -> Vec<WorkflowEvent> {
    let marker = WorkflowEvent::WorkflowForked {
        forked_from_exec_id: source_id,
        fork_event_id,
        effects: request.effects,
        reason: request.reason.clone(),
        operator_id: request.operator_id.clone(),
    };
    std::iter::once(marker)
        .chain(request.activity_overrides.iter().map(|o| {
            WorkflowEvent::ForkActivityResultOverridden {
                activity_name: o.activity_name.clone(),
                occurrence: o.occurrence,
                output: o.output.clone(),
            }
        }))
        .collect()
}

/// Most fork links that a lineage check follows.
const MAX_FORK_LINEAGE: usize = 64;

/// The first erased run in the fork lineage above `source`, if any.
///
/// It follows `start_source_ref` while the row is a fork. A missing ancestor
/// ends the walk, because retention may delete it. Each ancestor row is read
/// `FOR SHARE`, as the source is. So an erasure of an ancestor cannot commit
/// between this check and the fork.
///
/// # Errors
///
/// Returns [`WorkflowForkError::LineageTooDeep`] past
/// [`MAX_FORK_LINEAGE`] links. The walk fails closed there.
async fn erased_fork_ancestor(
    conn: &mut AsyncPgConnection,
    source: &WorkflowExecution,
) -> Result<Option<ExecutionId>, WorkflowForkError> {
    let mut current = (is_fork(source), source.start_source_ref.clone());
    for _ in 0..MAX_FORK_LINEAGE {
        let (true, Some(parent)) = current else {
            return Ok(None);
        };
        let Ok(parent) = parent.parse::<Uuid>() else {
            return Ok(None);
        };
        let row: Option<(Value, Option<String>, Option<String>)> =
            harvest_workflow_executions::table
                .find(parent)
                .for_share()
                .select((
                    harvest_workflow_executions::input,
                    harvest_workflow_executions::start_source,
                    harvest_workflow_executions::start_source_ref,
                ))
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?;
        let Some((input, start_source, start_source_ref)) = row else {
            return Ok(None);
        };
        if crate::erase::execution_input_is_erased(&input) {
            return Ok(Some(ExecutionId::from_uuid(parent)));
        }
        current = (
            start_source.as_deref() == Some(StartSource::Fork.as_str()),
            start_source_ref,
        );
    }
    match current {
        (true, Some(_)) => Err(WorkflowForkError::LineageTooDeep {
            depth: MAX_FORK_LINEAGE,
        }),
        _ => Ok(None),
    }
}

/// Apply the start checks of the workflow type to an input override.
///
/// It checks the published input schema (issue #373) and the input byte cap
/// (issue #252), as a start and a rerun do.
fn check_input_override(
    registry: &HandlerRegistry,
    workflow_name: &str,
    input: &Value,
) -> Result<(), WorkflowForkError> {
    let info = registry.workflows.get(workflow_name);
    if let Some(info) = info
        && let Err(violations) = info.validate_input(input)
    {
        let detail = violations
            .iter()
            .map(|violation| {
                violation.field_path.as_ref().map_or_else(
                    || violation.message.clone(),
                    |path| format!("{path}: {}", violation.message),
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(WorkflowForkError::InvalidOverride {
            message: format!("the input fails the workflow input schema: {detail}"),
        });
    }
    let cap = info
        .and_then(|info| info.max_input_bytes)
        .map_or(registry.max_workflow_input_bytes, |own| {
            own.max(registry.max_workflow_input_bytes)
        });
    let observed = serde_json::to_vec(input).map_or(0, |bytes| bytes.len() as u64);
    if cap > 0 && observed > cap {
        return Err(WorkflowForkError::InvalidOverride {
            message: format!("the input has {observed} bytes, over the cap of {cap}"),
        });
    }
    Ok(())
}

fn decode_rows(rows: &[HarvestEvent], codecs: &PayloadCodecs) -> HarvestResult<Vec<WorkflowEvent>> {
    rows.iter()
        .map(|row| codecs.decode_event(row.event_data.clone()))
        .collect()
}

/// Map a logical fork point that cannot resolve to a fork error.
fn skip_reason_to_error(reason: ResetSkipReason) -> WorkflowForkError {
    let message = match reason {
        ResetSkipReason::ContinueAsNew => return WorkflowForkError::ContinueAsNew,
        ResetSkipReason::NoMatchingActivity { activity_name } => {
            format!("no ActivityScheduled event found for activity '{activity_name}'")
        }
        ResetSkipReason::EmptyHistory => "execution has no recorded history".to_string(),
        other => format!("the fork point cannot resolve: {other:?}"),
    };
    WorkflowForkError::InvalidPoint(ResetInvalidPoint {
        message,
        reset_to_event_id: -1,
        last_event_id: -1,
        unresolved_side_effects: Vec::new(),
        nearest_valid_before: None,
        nearest_valid_after: None,
    })
}

/// Whether a run of `workflow_name` holds the key `workflow_id`.
///
/// It mirrors the partial unique index
/// `harvest_we_workflow_name_workflow_id_active_key`. A run holds the key
/// unless it continued as new, was terminated or is a migration seal. The
/// insert also maps a unique violation, which covers a concurrent start.
async fn workflow_id_in_use(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<bool> {
    diesel::select(diesel::dsl::exists(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
            .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
            .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
            .filter(harvest_workflow_executions::migrated_run_terminal_at.is_null()),
    ))
    .get_result(conn)
    .await
    .map_err(database_error)
}

/// Insert the carried prefix as new rows of the fork.
///
/// The rows keep their stored bytes. An input override replaces only the
/// stored `data.input` of the `WorkflowStarted` row. That row is the whole
/// prefix at fork point `0`. The function returns the stored rows, so the
/// caller can find the blob keys that they name.
///
/// The insert runs in chunks. One statement for a long prefix exceeds the
/// bind-parameter limit of Postgres.
async fn copy_prefix(
    conn: &mut AsyncPgConnection,
    new_exec_id: ExecutionId,
    rows: &[HarvestEvent],
    events: &[WorkflowEvent],
    fork_event_id: i64,
    request: &WorkflowForkRequest,
    codecs: &PayloadCodecs,
) -> HarvestResult<Vec<Value>> {
    let mut copied = Vec::new();
    for (row, event) in rows.iter().zip(events) {
        if i64::from(row.event_id) > fork_event_id {
            break;
        }
        let event_data = match (event, &request.input) {
            (WorkflowEvent::WorkflowStarted { .. }, Some(input)) => {
                let mut started = event.clone();
                if let WorkflowEvent::WorkflowStarted { input: field, .. } = &mut started {
                    field.clone_from(input);
                }
                with_start_input(&row.event_data, &codecs.encode_event(&started)?)
            }
            _ => row.event_data.clone(),
        };
        copied.push(NewForkEvent {
            workflow_exec_id: new_exec_id.as_uuid(),
            event_id: row.event_id,
            event_type: row.event_type.clone(),
            event_data,
        });
    }
    for chunk in copied.chunks(COPY_CHUNK_ROWS) {
        diesel::insert_into(harvest_events::table)
            .values(chunk)
            .execute(conn)
            .await
            .map_err(database_error)?;
    }
    Ok(copied.into_iter().map(|row| row.event_data).collect())
}

/// Rows per insert of the carried prefix. Each row binds four parameters, so
/// one chunk stays far below the limit of 65,535.
const COPY_CHUNK_ROWS: usize = 1_000;

/// The stored start row with the `data.input` of `encoded` in place.
///
/// The other fields keep their stored bytes. So an offloaded
/// `last_completion_result` stays an envelope and does not become a payload.
fn with_start_input(stored: &Value, encoded: &Value) -> Value {
    let mut row = stored.clone();
    if let (Some(data), Some(input)) = (
        row.get_mut("data").and_then(Value::as_object_mut),
        encoded.pointer("/data/input"),
    ) {
        data.insert("input".to_string(), input.clone());
    }
    row
}

#[derive(Insertable)]
#[diesel(table_name = harvest_events)]
struct NewForkEvent {
    workflow_exec_id: Uuid,
    event_id: i32,
    event_type: String,
    event_data: Value,
}

/// Effects that recorded mode cannot serve, as event type names.
const UNSERVABLE_EVENTS: [&str; 6] = [
    "LocalActivityScheduled",
    "ChildWorkflowStarted",
    "ChildWorkflowSpawnedDetached",
    "ActivityAwaitingExternal",
    "ExternalSignalRequested",
    "ExternalCancelRequested",
];

/// Check a fork point and the request against the source history.
///
/// # Errors
///
/// Returns [`WorkflowForkError`] when the fork cannot start at `fork_event_id`.
pub fn validate_fork(
    events: &[WorkflowEvent],
    fork_event_id: i64,
    request: &WorkflowForkRequest,
) -> Result<ResetPlan, WorkflowForkError> {
    if events
        .iter()
        .any(|event| matches!(event, WorkflowEvent::WorkflowContinuedAsNew { .. }))
    {
        return Err(WorkflowForkError::ContinueAsNew);
    }
    let plan = crate::reset::validate_reset_point(events, fork_event_id)?;
    let prefix = &events[..plan.events_carried_over];

    // A carried terminal event ends the fork at once on replay.
    if let Some(terminal) = prefix.iter().position(WorkflowEvent::is_terminal_lifecycle) {
        let terminal_id = event_id_of(terminal);
        let nearest_valid_before = terminal.checked_sub(1).and_then(|before| {
            match crate::reset::validate_reset_point(events, event_id_of(before)) {
                Ok(_) => Some(event_id_of(before)),
                Err(invalid) => invalid.nearest_valid_before,
            }
        });
        return Err(WorkflowForkError::InvalidPoint(ResetInvalidPoint {
            message: format!(
                "event {terminal_id} ends the run; fork at or before event {}",
                terminal_id - 1
            ),
            reset_to_event_id: fork_event_id,
            last_event_id: event_id_of(events.len().saturating_sub(1)),
            unresolved_side_effects: Vec::new(),
            nearest_valid_before,
            nearest_valid_after: None,
        }));
    }

    // No `MutexReleased` event exists, so a carried grant may still be held.
    if let Some((index, key)) = prefix
        .iter()
        .enumerate()
        .find_map(|(index, event)| match event {
            WorkflowEvent::MutexGranted { key, .. } => Some((index, key)),
            _ => None,
        })
    {
        return Err(WorkflowForkError::CarriedMutex {
            key: key.clone(),
            event_id: event_id_of(index),
        });
    }

    // The carried prefix replays against the fork input, so only an empty
    // decision history can take a new input.
    if request.input.is_some() && fork_event_id != 0 {
        return Err(WorkflowForkError::InvalidOverride {
            message: format!("an input override needs fork point 0, not {fork_event_id}"),
        });
    }

    validate_overrides(prefix, &request.activity_overrides)?;

    if request.effects == ForkEffects::Recorded
        && let Some((index, event)) = events
            .iter()
            .enumerate()
            .skip(prefix.len())
            .find(|(_, event)| UNSERVABLE_EVENTS.contains(&event.type_name()))
    {
        return Err(WorkflowForkError::UnservableEffect {
            kind: event.type_name().to_string(),
            event_id: event_id_of(index),
        });
    }
    Ok(plan)
}

/// The most activity overrides that one fork request can set.
///
/// The fork appends one event per override in one insert. The cap keeps that
/// insert below the bind-parameter limit of Postgres.
pub const MAX_ACTIVITY_OVERRIDES: usize = 1_000;

fn validate_overrides(
    prefix: &[WorkflowEvent],
    overrides: &[ForkActivityOverride],
) -> Result<(), WorkflowForkError> {
    if overrides.len() > MAX_ACTIVITY_OVERRIDES {
        return Err(WorkflowForkError::InvalidOverride {
            message: format!(
                "{} activity overrides; the limit is {MAX_ACTIVITY_OVERRIDES}",
                overrides.len()
            ),
        });
    }
    let mut carried = HashMap::<&str, u32>::new();
    for event in prefix {
        if let WorkflowEvent::ActivityScheduled { name, .. } = event {
            *carried.entry(name.as_str()).or_default() += 1;
        }
    }
    let mut seen = HashSet::new();
    for item in overrides {
        let name = item.activity_name.as_str();
        let message = if name.trim().is_empty() {
            Some("activity_name is empty".to_string())
        } else if item.occurrence == 0 {
            Some(format!("occurrence of '{name}' is 1-based, not 0"))
        } else if !seen.insert((name, item.occurrence)) {
            Some(format!(
                "'{name}' occurrence {} is set twice",
                item.occurrence
            ))
        } else if item.occurrence <= carried.get(name).copied().unwrap_or(0) {
            Some(format!(
                "'{name}' occurrence {} is in the carried history; fork before it",
                item.occurrence
            ))
        } else {
            None
        };
        if let Some(message) = message {
            return Err(WorkflowForkError::InvalidOverride { message });
        }
    }
    Ok(())
}

fn event_id_of(index: usize) -> i64 {
    i64::try_from(index).unwrap_or(i64::MAX)
}

/// The first command in `commands` that recorded mode cannot serve.
///
/// Returns the command name. A recorded fork fails before such a command
/// runs. The match has no catch-all, so a new command kind needs a decision.
#[must_use]
pub fn live_effect_refusal(commands: &[WorkflowCommand]) -> Option<&'static str> {
    commands.iter().find_map(|command| match command {
        WorkflowCommand::RunLocalActivity { .. } => Some("RunLocalActivity"),
        WorkflowCommand::StartChildWorkflow { .. } => Some("StartChildWorkflow"),
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => Some("SpawnDetachedChildWorkflow"),
        WorkflowCommand::ScheduleExternalActivity { .. } => Some("ScheduleExternalActivity"),
        WorkflowCommand::SignalExternalWorkflow { .. } => Some("SignalExternalWorkflow"),
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => {
            Some("RequestCancelExternalWorkflow")
        }
        // A successor run holds no fork marker, so it would run live.
        WorkflowCommand::ContinueAsNew { .. } => Some("ContinueAsNew"),
        // A production mutex key is shared with live runs. A what-if fork
        // must not block them.
        WorkflowCommand::AcquireMutex { .. } => Some("AcquireMutex"),
        // A remote activity is served from the record at schedule time.
        WorkflowCommand::ScheduleActivity { .. }
        | WorkflowCommand::WaitForActivity { .. }
        | WorkflowCommand::StartTimer { .. }
        | WorkflowCommand::RecordMarker { .. }
        | WorkflowCommand::RecordSideEffect { .. }
        | WorkflowCommand::WaitForSignal { .. }
        | WorkflowCommand::Complete { .. }
        | WorkflowCommand::Fail { .. }
        | WorkflowCommand::RecordUpdateResult { .. }
        | WorkflowCommand::UpsertSearchAttributes { .. }
        | WorkflowCommand::SetCurrentDetails { .. }
        | WorkflowCommand::PublishProgress { .. }
        | WorkflowCommand::RecordLog { .. }
        | WorkflowCommand::AwaitExternalWorkflow { .. }
        | WorkflowCommand::CancelRaceLosers { .. }
        | WorkflowCommand::ArmTimer { .. }
        | WorkflowCommand::CancelTimer { .. }
        | WorkflowCommand::ReleaseMutex { .. } => None,
    })
}

/// The effect in one decision `outcome` that recorded mode cannot serve.
///
/// A suspended decision carries its commands in `outcome`. Any other outcome
/// carries them in `pending`. A continue-as-new outcome is itself refused.
#[must_use]
pub fn recorded_outcome_refusal(
    outcome: &crate::executor::WorkflowOutcome,
    pending: &[WorkflowCommand],
) -> Option<&'static str> {
    match outcome {
        crate::executor::WorkflowOutcome::ContinuedAsNew { .. } => Some("ContinueAsNew"),
        crate::executor::WorkflowOutcome::Suspended { commands } => live_effect_refusal(commands),
        _ => live_effect_refusal(pending),
    }
}

/// Whether `execution` is a fork of issue #2000. A cheap in-memory check.
///
/// A reset of a fork keeps `start_source = fork`, so it stays a fork.
#[must_use]
pub fn is_fork(execution: &WorkflowExecution) -> bool {
    execution.start_source.as_deref() == Some(StartSource::Fork.as_str())
}

/// The source and the effects mode of the fork whose history is `events`.
///
/// The *last* marker counts. A fork of a fork carries the marker of its
/// source in its prefix, and its own marker comes after that one.
#[must_use]
pub fn fork_marker(events: &[WorkflowEvent]) -> Option<(ExecutionId, ForkEffects)> {
    events.iter().rev().find_map(|event| match event {
        WorkflowEvent::WorkflowForked {
            forked_from_exec_id,
            effects,
            ..
        } => Some((*forked_from_exec_id, *effects)),
        _ => None,
    })
}

/// Whether a run with fork provenance runs in recorded mode.
///
/// A fork row with no marker fails safe, as recorded. That happens for a
/// reset of a fork at a point before the marker.
#[must_use]
pub fn history_is_recorded_fork(execution: &WorkflowExecution, events: &[WorkflowEvent]) -> bool {
    is_fork(execution)
        && fork_marker(events).is_none_or(|(_, effects)| effects == ForkEffects::Recorded)
}

/// Whether `execution` is a recorded fork. A recorded fork sends no
/// completion callback and fires no completion trigger.
///
/// It reads the marker only for a fork, so another run costs no query.
///
/// # Errors
///
/// Returns [`HarvestError`] when the read fails.
pub async fn is_recorded_fork(
    conn: &mut AsyncPgConnection,
    execution: &WorkflowExecution,
) -> HarvestResult<bool> {
    if !is_fork(execution) {
        return Ok(false);
    }
    let marker: Option<Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(execution.id))
        .filter(harvest_events::event_type.eq("WorkflowForked"))
        .order(harvest_events::event_id.desc())
        .select(harvest_events::event_data)
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?;
    // A fork row with no readable marker fails safe, as recorded.
    Ok(marker
        .and_then(|value| serde_json::from_value::<WorkflowEvent>(value).ok())
        .and_then(|event| fork_marker(std::slice::from_ref(&event)))
        .is_none_or(|(_, effects)| effects == ForkEffects::Recorded))
}

/// What a fork does with one activity that it just scheduled.
#[derive(Debug, Clone)]
pub enum ForkResolution {
    /// The activity runs as usual.
    Run,
    /// The fork appends this terminal event. No worker runs the activity.
    Serve(WorkflowEvent),
    /// The activity stays pending and never runs. The source cancelled it as
    /// a race loser, so a sibling branch wins again.
    Hold,
}

/// Resolve the fork activity `activity_id`. The caller knows the run is a fork.
///
/// The order is: a caller override, then (in recorded mode) the source
/// record with the same name, occurrence and input, then a non-retryable
/// [`ERROR_TYPE_FORK_EFFECT_UNAVAILABLE`] failure. A live fork runs an
/// activity with no override. An activity with an outcome runs as usual.
#[must_use]
pub fn resolve_activity(
    fork_events: &[WorkflowEvent],
    source_events: &[WorkflowEvent],
    activity_id: ActivityExecId,
) -> ForkResolution {
    if fork_events
        .iter()
        .any(|event| terminal_activity_id(event) == Some(activity_id))
    {
        return ForkResolution::Run;
    }
    let Some((name, occurrence, input)) = scheduled_occurrence(fork_events, |id| id == activity_id)
    else {
        return ForkResolution::Run;
    };
    if let Some(output) = override_for(fork_events, name, occurrence) {
        return ForkResolution::Serve(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: output.clone(),
        });
    }
    let effects = fork_marker(fork_events).map_or(ForkEffects::Recorded, |(_, effects)| effects);
    if effects == ForkEffects::Live {
        return ForkResolution::Run;
    }
    match recorded_terminal(source_events, name, occurrence, input) {
        Some(event) if is_race_loser(event) => ForkResolution::Hold,
        Some(event) if !is_synthetic(event) => ForkResolution::Serve(rebind(event, activity_id)),
        _ => ForkResolution::Serve(unavailable(activity_id, name, occurrence)),
    }
}

/// The error text of the terminal that `ctx.race()` writes for a loser.
const RACE_LOSER_ERROR: &str = "lost race to a sibling branch";

fn is_race_loser(event: &WorkflowEvent) -> bool {
    matches!(event, WorkflowEvent::ActivityFailed { error, .. } if error == RACE_LOSER_ERROR)
}

/// A failure that the engine wrote, not one that the activity returned.
fn is_synthetic(event: &WorkflowEvent) -> bool {
    matches!(
        event,
        WorkflowEvent::ActivityFailed { error, .. }
            if error == crate::event::ABANDONED_DISPATCH_REASON
    )
}

/// Name, 1-based occurrence and input of the first `ActivityScheduled` whose
/// id passes `matches`.
fn scheduled_occurrence(
    events: &[WorkflowEvent],
    matches: impl Fn(ActivityExecId) -> bool,
) -> Option<(&str, u32, &Value)> {
    let mut counts = HashMap::<&str, u32>::new();
    events.iter().find_map(|event| {
        let WorkflowEvent::ActivityScheduled {
            activity_id,
            name,
            input,
            ..
        } = event
        else {
            return None;
        };
        let count = counts.entry(name.as_str()).or_default();
        *count += 1;
        matches(*activity_id).then_some((name.as_str(), *count, input))
    })
}

/// The indices of the overrides that belong to the last marker in `events`.
///
/// Only an override after the last marker counts. An override that a fork of
/// a fork carries in its prefix belongs to the earlier fork. A reset of a
/// fork copies these overrides after its own marker.
#[must_use]
pub fn own_override_indices(events: &[WorkflowEvent]) -> Vec<usize> {
    let Some(marker) = events
        .iter()
        .rposition(|event| matches!(event, WorkflowEvent::WorkflowForked { .. }))
    else {
        return Vec::new();
    };
    (marker..events.len())
        .filter(|&index| {
            matches!(
                events[index],
                WorkflowEvent::ForkActivityResultOverridden { .. }
            )
        })
        .collect()
}

/// The override for occurrence `occurrence` of `name`.
fn override_for<'a>(events: &'a [WorkflowEvent], name: &str, occurrence: u32) -> Option<&'a Value> {
    own_override_indices(events)
        .into_iter()
        .find_map(|index| match &events[index] {
            WorkflowEvent::ForkActivityResultOverridden {
                activity_name,
                occurrence: at,
                output,
            } if activity_name == name && *at == occurrence => Some(output),
            _ => None,
        })
}

const fn terminal_activity_id(event: &WorkflowEvent) -> Option<ActivityExecId> {
    match event {
        WorkflowEvent::ActivityCompleted { activity_id, .. }
        | WorkflowEvent::ActivityFailed { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
        _ => None,
    }
}

/// The terminal event that the source recorded for occurrence `occurrence`
/// of `name`, when the source scheduled it with the same input.
///
/// An erased payload is no record. A tombstone never reaches the fork.
fn recorded_terminal<'a>(
    source_events: &'a [WorkflowEvent],
    name: &str,
    occurrence: u32,
    input: &Value,
) -> Option<&'a WorkflowEvent> {
    let mut seen = 0_u32;
    let (index, source_id) = source_events
        .iter()
        .enumerate()
        .find_map(|(index, event)| {
            let WorkflowEvent::ActivityScheduled {
                activity_id,
                name: scheduled,
                ..
            } = event
            else {
                return None;
            };
            if scheduled != name {
                return None;
            }
            seen += 1;
            (seen == occurrence).then_some((index, *activity_id))
        })?;
    let WorkflowEvent::ActivityScheduled {
        input: recorded_input,
        ..
    } = &source_events[index]
    else {
        return None;
    };
    if recorded_input != input || crate::erase::is_erasure_tombstone(recorded_input) {
        return None;
    }
    let terminal = source_events[index + 1..]
        .iter()
        .find(|event| terminal_activity_id(event) == Some(source_id))?;
    let erased = match terminal {
        WorkflowEvent::ActivityCompleted { output, .. } => {
            crate::erase::is_erasure_tombstone(output)
        }
        WorkflowEvent::ActivityFailed { details, .. } => details
            .as_ref()
            .is_some_and(crate::erase::is_erasure_tombstone),
        _ => false,
    };
    (!erased).then_some(terminal)
}

fn rebind(event: &WorkflowEvent, activity_id: ActivityExecId) -> WorkflowEvent {
    let mut event = event.clone();
    match &mut event {
        WorkflowEvent::ActivityCompleted {
            activity_id: id, ..
        }
        | WorkflowEvent::ActivityFailed {
            activity_id: id, ..
        }
        | WorkflowEvent::ActivityTimedOut {
            activity_id: id, ..
        } => *id = activity_id,
        _ => {}
    }
    event
}

fn unavailable(activity_id: ActivityExecId, name: &str, occurrence: u32) -> WorkflowEvent {
    WorkflowEvent::ActivityFailed {
        activity_id,
        error: format!(
            "fork has no recorded result for activity '{name}' (occurrence {occurrence}); \
             set an activity override, or fork with effects = live to run it"
        ),
        attempt: 1,
        error_type: ERROR_TYPE_FORK_EFFECT_UNAVAILABLE.to_string(),
        non_retryable: true,
        details: None,
    }
}

/// Whether the record source exists and is not erased.
///
/// It reads the row `FOR SHARE`, as a fork does. So an erasure of the source
/// cannot commit between this check and the copy of a recorded result.
async fn record_source_is_readable(
    conn: &mut AsyncPgConnection,
    source_id: ExecutionId,
) -> HarvestResult<bool> {
    let input: Option<Value> = harvest_workflow_executions::table
        .find(source_id.as_uuid())
        .for_share()
        .select(harvest_workflow_executions::input)
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?;
    Ok(input.is_some_and(|input| !crate::erase::execution_input_is_erased(&input)))
}

/// Resolve the activities that a fork just scheduled, in the same transaction.
///
/// For each activity in `scheduled` that [`resolve_activity`] serves or
/// holds, this cancels its task row, so no worker runs it. A served activity
/// also gets its outcome appended. The caller must wake the workflow when this
/// returns `true`, as for a broken session. A run that is not a fork returns at
/// once with no query.
///
/// Both histories load inflated, so an offloaded input matches its source.
/// A source that retention deleted or erased has no record. Recorded mode
/// then fails closed. A failed load of the source returns an error, so the
/// decision rolls back and retries. A transient store fault never becomes a
/// permanent `ForkEffectUnavailable`.
///
/// The caller runs this before the broken-session check and skips each
/// settled activity there. A recorded fork often carries a session that the
/// source closed. The record serves its members, not a `SessionBroken`.
///
/// # Errors
///
/// Returns [`HarvestError`] when a read or a write fails.
pub(crate) async fn serve_recorded_activities(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    is_fork: bool,
    scheduled: &[ActivityExecId],
    next_event_id: &mut i32,
    registry: &HandlerRegistry,
) -> HarvestResult<ForkSettlement> {
    let mut settlement = ForkSettlement::default();
    if !is_fork || scheduled.is_empty() {
        return Ok(settlement);
    }
    let codecs = registry.payload_codecs();
    let offloader = registry.payload_offloader();
    let fork_events = crate::store::load_history_inflated(conn, exec_id, codecs, offloader)
        .await?
        .events;
    // A live fork takes no record, so it never reads the source. A fault in
    // the source history then cannot stop its dispatch.
    let source_events = match fork_marker(&fork_events) {
        Some((source_id, ForkEffects::Recorded))
            if record_source_is_readable(conn, source_id).await? =>
        {
            crate::store::load_history_inflated(conn, source_id, codecs, offloader)
                .await?
                .events
        }
        _ => Vec::new(),
    };

    for activity_id in scheduled {
        let outcome = match resolve_activity(&fork_events, &source_events, *activity_id) {
            ForkResolution::Run => continue,
            ForkResolution::Hold => None,
            ForkResolution::Serve(event) => Some(event),
        };
        let cancelled = diesel::update(
            harvest_task_queue::table
                .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .filter(harvest_task_queue::activity_id.eq(Some(activity_id.as_uuid())))
                .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"])),
        )
        .set((
            harvest_task_queue::state.eq("CANCELLED"),
            harvest_task_queue::worker_id.eq(None::<String>),
            harvest_task_queue::error.eq(Some("resolved by the fork record (issue #2000)")),
            harvest_task_queue::completed_at.eq(Some(chrono::Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
        if cancelled > 0 {
            settlement.settled.insert(*activity_id);
        }
        // No open row means that a real outcome exists or is on its way.
        let Some(outcome) = outcome.filter(|_| cancelled > 0) else {
            continue;
        };
        crate::store::append_events_offloaded_with_codecs(
            conn,
            exec_id,
            &[outcome],
            *next_event_id,
            offloader,
            codecs,
        )
        .await?;
        *next_event_id = next_event_id.saturating_add(1);
        settlement.served = true;
    }
    Ok(settlement)
}

/// The activities that the fork record settles in one decision.
#[derive(Debug, Default)]
pub(crate) struct ForkSettlement {
    /// The activities whose task row the record cancelled.
    pub(crate) settled: std::collections::HashSet<ActivityExecId>,
    /// True when an outcome was appended. The workflow then needs a wake.
    pub(crate) served: bool,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::{ExternalSignalId, ExternalTarget};

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(id: ActivityExecId, name: &str, input: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: name.to_string(),
            input,
            queue: "default".to_string(),
        }
    }

    fn completed(id: ActivityExecId, output: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output,
        }
    }

    fn marker(effects: ForkEffects) -> WorkflowEvent {
        WorkflowEvent::WorkflowForked {
            forked_from_exec_id: ExecutionId::new(),
            fork_event_id: 0,
            effects,
            reason: "test".to_string(),
            operator_id: "test".to_string(),
        }
    }

    /// A source that charged twice and completed.
    fn source() -> (Vec<WorkflowEvent>, ActivityExecId, ActivityExecId) {
        let first = ActivityExecId::new();
        let second = ActivityExecId::new();
        let events = vec![
            started(json!({})),
            scheduled(first, "charge", json!({ "n": 1 })),
            completed(first, json!("ch-1")),
            scheduled(second, "charge", json!({ "n": 2 })),
            completed(second, json!("ch-2")),
            WorkflowEvent::WorkflowCompleted {
                output: json!("done"),
            },
        ];
        (events, first, second)
    }

    fn output_of(resolution: ForkResolution) -> Value {
        match resolution {
            ForkResolution::Serve(WorkflowEvent::ActivityCompleted { output, .. }) => output,
            other => panic!("expected ActivityCompleted, got {other:?}"),
        }
    }

    fn is_unavailable(resolution: &ForkResolution) -> bool {
        matches!(
            resolution,
            ForkResolution::Serve(WorkflowEvent::ActivityFailed { error_type, non_retryable: true, .. })
                if error_type == ERROR_TYPE_FORK_EFFECT_UNAVAILABLE
        )
    }

    #[test]
    fn effects_default_to_recorded() {
        assert_eq!(ForkEffects::default(), ForkEffects::Recorded);
        let request: WorkflowForkRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(request.effects, ForkEffects::Recorded);
        let live: WorkflowForkRequest =
            serde_json::from_value(json!({ "effects": "live" })).unwrap();
        assert_eq!(live.effects, ForkEffects::Live);
        assert!(
            serde_json::from_value::<WorkflowForkRequest>(json!({ "effects": "LIVE" })).is_err()
        );
    }

    #[test]
    fn a_fork_takes_the_recorded_result_by_occurrence() {
        let (source, _, _) = source();
        let fork_first = ActivityExecId::new();
        let fork_second = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(fork_first, "charge", json!({ "n": 1 })),
            completed(fork_first, json!("ch-1")),
            scheduled(fork_second, "charge", json!({ "n": 2 })),
        ];
        let served = resolve_activity(&fork, &source, fork_second);
        assert_eq!(output_of(served.clone()), json!("ch-2"));
        match served {
            ForkResolution::Serve(WorkflowEvent::ActivityCompleted { activity_id, .. }) => {
                assert_eq!(activity_id, fork_second, "the fork id, not the source id");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_different_input_has_no_record() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 99 })),
        ];
        assert!(is_unavailable(&resolve_activity(&fork, &source, id)));
    }

    #[test]
    fn an_erased_record_is_not_served() {
        let (mut source, first, _) = source();
        source[2] = completed(first, crate::erase::erasure_tombstone());
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert!(is_unavailable(&resolve_activity(&fork, &source, id)));
    }

    #[test]
    fn an_override_wins_over_the_record() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            WorkflowEvent::ForkActivityResultOverridden {
                activity_name: "charge".to_string(),
                occurrence: 1,
                output: json!("stub"),
            },
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert_eq!(
            output_of(resolve_activity(&fork, &source, id)),
            json!("stub")
        );
    }

    #[test]
    fn a_recorded_failure_is_served_as_a_failure() {
        let first = ActivityExecId::new();
        let source = vec![
            started(json!({})),
            scheduled(first, "charge", json!({})),
            WorkflowEvent::ActivityFailed {
                activity_id: first,
                error: "declined".to_string(),
                attempt: 3,
                error_type: "CardDeclined".to_string(),
                non_retryable: true,
                details: None,
            },
        ];
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({})),
        ];
        match resolve_activity(&fork, &source, id) {
            ForkResolution::Serve(WorkflowEvent::ActivityFailed {
                activity_id,
                error_type,
                ..
            }) => {
                assert_eq!(activity_id, id);
                assert_eq!(error_type, "CardDeclined");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_live_fork_serves_nothing() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Live),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert!(matches!(
            resolve_activity(&fork, &source, id),
            ForkResolution::Run
        ));
    }

    #[test]
    fn an_activity_with_an_outcome_is_not_served_again() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 1 })),
            completed(id, json!("race")),
        ];
        assert!(matches!(
            resolve_activity(&fork, &source, id),
            ForkResolution::Run
        ));
    }

    #[test]
    fn a_race_loser_is_held_and_an_abandoned_dispatch_is_unavailable() {
        let failed = |id, error: &str| WorkflowEvent::ActivityFailed {
            activity_id: id,
            error: error.to_string(),
            attempt: 1,
            error_type: "Error".to_string(),
            non_retryable: true,
            details: None,
        };
        for (error, held) in [
            (RACE_LOSER_ERROR, true),
            (crate::event::ABANDONED_DISPATCH_REASON, false),
        ] {
            let first = ActivityExecId::new();
            let source = vec![
                started(json!({})),
                scheduled(first, "slow", json!({})),
                failed(first, error),
            ];
            let id = ActivityExecId::new();
            let fork = vec![
                started(json!({})),
                marker(ForkEffects::Recorded),
                scheduled(id, "slow", json!({})),
            ];
            let resolution = resolve_activity(&fork, &source, id);
            if held {
                assert!(matches!(resolution, ForkResolution::Hold));
            } else {
                assert!(is_unavailable(&resolution), "{resolution:?}");
            }
        }
    }

    #[test]
    fn the_last_marker_and_its_own_overrides_count() {
        let (source, _, _) = source();
        let id = ActivityExecId::new();
        // A recorded fork of a live fork carries the live marker and an old
        // override in its prefix.
        let fork = vec![
            started(json!({})),
            marker(ForkEffects::Live),
            WorkflowEvent::ForkActivityResultOverridden {
                activity_name: "charge".to_string(),
                occurrence: 1,
                output: json!("old"),
            },
            marker(ForkEffects::Recorded),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert_eq!(
            output_of(resolve_activity(&fork, &source, id)),
            json!("ch-1")
        );
        let mut live_last = fork;
        live_last.swap(1, 3);
        assert!(matches!(
            resolve_activity(&live_last, &source, id),
            ForkResolution::Run
        ));
    }

    #[test]
    fn a_fork_with_no_marker_fails_closed() {
        // With no marker there is no source, so the serving path passes none.
        let id = ActivityExecId::new();
        let fork = vec![
            started(json!({})),
            scheduled(id, "charge", json!({ "n": 1 })),
        ];
        assert!(is_unavailable(&resolve_activity(&fork, &[], id)));
        let mut row = crate::reset::tests::execution_in_state("RUNNING");
        row.start_source = Some("fork".to_string());
        assert!(history_is_recorded_fork(&row, &fork));
        row.start_source = Some("reset".to_string());
        assert!(!history_is_recorded_fork(&row, &fork));
    }

    fn blob(key: &str) -> crate::payload_store::OffloadedRef {
        crate::payload_store::OffloadedRef {
            blob_key: key.to_string(),
            store_id: "s".to_string(),
            byte_len: 1,
        }
    }

    #[test]
    fn only_named_payload_refs_are_copied() {
        let envelope = |key: &str| {
            json!({
                "_harvest_offload_envelope": 1,
                "store_id": "s",
                "key": key,
                "len": 1,
                "checksum": "c",
            })
        };
        let rows = [
            json!({ "data": { "output": envelope("blob-10") } }),
            json!({ "data": { "input": envelope("a\"b\\c") } }),
        ];
        let refs = vec![
            blob("blob-1"),
            blob("blob-10"),
            blob("a\"b\\c"),
            blob("blob-gone"),
        ];
        let kept = refs_named_in(refs, &envelope_keys(&rows));
        // A key that JSON escapes still matches. A key that a named key
        // contains does not.
        assert_eq!(
            kept.into_iter().map(|r| r.blob_key).collect::<Vec<_>>(),
            vec!["blob-10", "a\"b\\c"]
        );
    }

    #[test]
    fn an_input_override_keeps_the_other_stored_bytes() {
        let carryover = json!({ "_harvest_offloaded": { "key": "blob-last" } });
        let stored = json!({
            "type": "WorkflowStarted",
            "data": { "input": { "old": 1 }, "last_completion_result": carryover },
        });
        let encoded = json!({
            "type": "WorkflowStarted",
            "data": { "input": { "new": 2 }, "last_completion_result": { "inflated": true } },
        });
        let row = with_start_input(&stored, &encoded);
        assert_eq!(row["data"]["input"], json!({ "new": 2 }));
        assert_eq!(row["data"]["last_completion_result"], carryover);
        assert_eq!(row["type"], json!("WorkflowStarted"));
    }

    #[test]
    fn own_overrides_follow_the_last_marker() {
        let over = |output: &str| WorkflowEvent::ForkActivityResultOverridden {
            activity_name: "charge".to_string(),
            occurrence: 1,
            output: json!(output),
        };
        let events = vec![
            started(json!({})),
            marker(ForkEffects::Live),
            over("old"),
            marker(ForkEffects::Recorded),
            over("new"),
        ];
        assert_eq!(own_override_indices(&events), vec![4]);
        assert_eq!(
            own_override_indices(&[started(json!({}))]),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn an_unknown_request_field_is_refused() {
        assert!(
            serde_json::from_value::<WorkflowForkRequest>(json!({ "effect": "live" })).is_err()
        );
        assert!(
            serde_json::from_value::<ForkActivityOverride>(json!({
                "activity_name": "a", "occurrence": 1, "output": 1, "extra": 1
            }))
            .is_err()
        );
    }

    #[test]
    fn a_fork_point_after_the_terminal_event_is_refused() {
        let (source, _, _) = source();
        let error = validate_fork(&source, 5, &WorkflowForkRequest::default()).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::InvalidPoint(_)),
            "{error}"
        );
        assert!(validate_fork(&source, 4, &WorkflowForkRequest::default()).is_ok());
        assert!(validate_fork(&source, 0, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn a_carried_mutex_grant_is_refused() {
        let events = vec![
            started(json!({})),
            WorkflowEvent::MutexGranted {
                key: "acct-1".to_string(),
                lock_seq: 1,
                acquired_at: chrono::Utc::now(),
            },
        ];
        let error = validate_fork(&events, 1, &WorkflowForkRequest::default()).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::CarriedMutex { ref key, event_id: 1 } if key == "acct-1"),
            "{error}"
        );
        assert!(validate_fork(&events, 0, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn recorded_mode_refuses_an_effect_it_cannot_serve() {
        let id = ActivityExecId::new();
        let events = vec![
            started(json!({})),
            WorkflowEvent::LocalActivityScheduled {
                activity_id: id,
                name: "lookup".to_string(),
                input: json!({}),
                resolved: true,
                retry_policy: None,
                start_to_close_nanos: None,
            },
            WorkflowEvent::LocalActivityCompleted {
                activity_id: id,
                output: json!({}),
            },
        ];
        let error = validate_fork(&events, 0, &WorkflowForkRequest::default()).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::UnservableEffect { ref kind, event_id: 1 } if kind == "LocalActivityScheduled"),
            "{error}"
        );
        let live = WorkflowForkRequest {
            effects: ForkEffects::Live,
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&events, 0, &live).is_ok());
        assert!(validate_fork(&events, 2, &WorkflowForkRequest::default()).is_ok());
    }

    #[test]
    fn an_input_override_needs_fork_point_zero() {
        let (source, _, _) = source();
        let request = WorkflowForkRequest {
            input: Some(json!({ "n": 5 })),
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&source, 0, &request).is_ok());
        let error = validate_fork(&source, 2, &request).unwrap_err();
        assert!(
            matches!(error, WorkflowForkError::InvalidOverride { .. }),
            "{error}"
        );
    }

    #[test]
    fn the_override_count_is_bounded() {
        let (source, _, _) = source();
        let many = |count: u32| WorkflowForkRequest {
            activity_overrides: (2..2 + count)
                .map(|occurrence| ForkActivityOverride {
                    activity_name: "charge".to_string(),
                    occurrence,
                    output: json!("stub"),
                })
                .collect(),
            ..WorkflowForkRequest::default()
        };
        let cap = u32::try_from(MAX_ACTIVITY_OVERRIDES).expect("cap");
        assert!(validate_fork(&source, 2, &many(cap)).is_ok());
        assert!(matches!(
            validate_fork(&source, 2, &many(cap + 1)),
            Err(WorkflowForkError::InvalidOverride { .. })
        ));
    }

    #[test]
    fn an_override_must_target_an_activity_after_the_fork_point() {
        let (source, _, _) = source();
        let at = |occurrence| WorkflowForkRequest {
            activity_overrides: vec![ForkActivityOverride {
                activity_name: "charge".to_string(),
                occurrence,
                output: json!("stub"),
            }],
            ..WorkflowForkRequest::default()
        };
        assert!(validate_fork(&source, 2, &at(2)).is_ok());
        for bad in [0, 1] {
            let error = validate_fork(&source, 2, &at(bad)).unwrap_err();
            assert!(
                matches!(error, WorkflowForkError::InvalidOverride { .. }),
                "{error}"
            );
        }
        let twice = WorkflowForkRequest {
            activity_overrides: [at(2).activity_overrides, at(2).activity_overrides].concat(),
            ..WorkflowForkRequest::default()
        };
        assert!(matches!(
            validate_fork(&source, 2, &twice),
            Err(WorkflowForkError::InvalidOverride { .. })
        ));
    }

    #[test]
    fn a_recorded_fork_refuses_to_continue_as_new() {
        let next = || WorkflowCommand::ContinueAsNew {
            input: json!({}),
            new_workflow_type: None,
        };
        assert_eq!(live_effect_refusal(&[next()]), Some("ContinueAsNew"));
        let outcome = crate::executor::WorkflowOutcome::ContinuedAsNew {
            input: json!({}),
            new_workflow_type: None,
        };
        assert_eq!(
            recorded_outcome_refusal(&outcome, &[]),
            Some("ContinueAsNew")
        );
        let suspended = crate::executor::WorkflowOutcome::Suspended {
            commands: vec![next()],
        };
        assert_eq!(
            recorded_outcome_refusal(&suspended, &[]),
            Some("ContinueAsNew")
        );
    }

    #[test]
    fn a_recorded_fork_refuses_a_live_effect_command() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let signal = WorkflowCommand::SignalExternalWorkflow {
            signal_id: ExternalSignalId::new(),
            target: ExternalTarget::ExecutionId(ExecutionId::new()),
            signal_name: "go".to_string(),
            payload: json!({}),
            result_tx: tx,
            already_requested: false,
            idempotency_key: None,
        };
        let marker = WorkflowCommand::RecordMarker {
            name: "m".to_string(),
            details: json!({}),
        };
        assert_eq!(live_effect_refusal(&[marker]), None);
        let (lock_tx, _lock_rx) = tokio::sync::oneshot::channel();
        let acquire = WorkflowCommand::AcquireMutex {
            key: "acct-1".to_string(),
            result_tx: lock_tx,
        };
        assert_eq!(live_effect_refusal(&[acquire]), Some("AcquireMutex"));
        let marker = WorkflowCommand::RecordMarker {
            name: "m".to_string(),
            details: json!({}),
        };
        assert_eq!(
            live_effect_refusal(&[marker, signal]),
            Some("SignalExternalWorkflow")
        );
    }
}
