//! Event store -- append-only persistence for workflow event histories.
//!
//! All writes go through [`append_events()`] which inserts atomically.
//! The `UNIQUE(workflow_exec_id, event_id)` constraint guarantees
//! that two workers can't append conflicting events to the same workflow.

use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::HarvestResult;
use crate::event::WorkflowEvent;
use crate::models::NewHarvestEvent;
use crate::schema::{harvest_events, harvest_execution_summaries, harvest_workflow_executions};
use crate::types::ExecutionId;

/// Loaded event history for a single workflow execution.
///
/// Contains the deserialized events and the next `event_id` to use when
/// appending new events (i.e. one past the last existing event).
#[derive(Debug)]
pub struct EventHistory {
    pub exec_id: ExecutionId,
    pub events: Vec<WorkflowEvent>,
    pub next_event_id: i32,
}

/// Filters for loading child workflow execution rows under one parent.
#[derive(Debug, Clone, Default)]
pub struct WorkflowChildFilters {
    pub statuses: Vec<String>,
    pub workflow_name: Option<String>,
    pub cursor: Option<WorkflowChildCursor>,
    pub limit: Option<i64>,
}

/// Cursor anchor for paged child workflow queries.
#[derive(Debug, Clone)]
pub struct WorkflowChildCursor {
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub exec_id: uuid::Uuid,
}

/// Whether a child workflow was spawned in await or detached mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AwaitMode {
    /// Parent suspended until the child's terminal result (classic spawn).
    Awaited,
    /// Parent did not suspend; child runs independently under a parent-close
    /// policy.
    Detached,
}

/// Operator-facing child workflow row used by management API read models.
#[derive(Debug, Clone)]
pub struct WorkflowChildRow {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub status: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub error_summary: Option<String>,
    pub shard_id: i32,
    pub depth: u8,
    /// How this child was spawned (awaited or detached).
    pub await_mode: AwaitMode,
    /// For detached children, the policy applied when the parent closes. `None`
    /// for awaited children.
    pub parent_close_policy: Option<crate::types::ParentClosePolicy>,
}

type WorkflowChildProjection = (
    uuid::Uuid,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
    i32,
    Option<String>,
);

/// Convert in-memory events to insertable rows with sequential event IDs
/// starting from 0.
///
/// This is a convenience wrapper around [`events_to_insert_rows_from`] for
/// fresh workflow executions where the history starts empty.
pub fn events_to_insert_rows(
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
) -> Result<Vec<NewHarvestEvent<'_>>, crate::error::HarvestError> {
    events_to_insert_rows_from_with_codecs(
        exec_id,
        events,
        0,
        &crate::payload_codec::PayloadCodecs::default(),
    )
}

/// Convert in-memory events to insertable rows with sequential event IDs
/// starting from `start_id`.
///
/// Use `start_id = 0` for new workflows. For appending to in-progress workflows,
/// pass the current event count so IDs continue sequentially.
///
/// # Panics
///
/// Panics if a `WorkflowEvent` variant fails to serialize to JSON. This should
/// never happen in practice since all variants derive `Serialize`.
pub fn events_to_insert_rows_from(
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
    start_id: i32,
) -> Result<Vec<NewHarvestEvent<'_>>, crate::error::HarvestError> {
    events_to_insert_rows_from_with_codecs(
        exec_id,
        events,
        start_id,
        &crate::payload_codec::PayloadCodecs::default(),
    )
}

pub fn events_to_insert_rows_from_with_codecs<'a>(
    exec_id: ExecutionId,
    events: &'a [WorkflowEvent],
    start_id: i32,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> Result<Vec<NewHarvestEvent<'a>>, crate::error::HarvestError> {
    events
        .iter()
        .enumerate()
        .map(|(i, event)| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let i_i32 = i as i32;
            let event_id = start_id.checked_add(i_i32).ok_or_else(|| {
                crate::error::HarvestError::Database("Event ID overflow".to_string())
            })?;
            Ok(NewHarvestEvent {
                workflow_exec_id: exec_id.as_uuid(),
                event_id,
                event_type: event.type_name(),
                event_data: codecs.encode_event(event)?,
            })
        })
        .collect()
}

/// Append events to a workflow's history in a single INSERT.
///
/// Returns the number of events inserted. Fails with a unique constraint
/// violation (wrapped as [`crate::error::HarvestError::Database`]) if `start_id` conflicts --
/// this indicates a concurrency conflict where two workers tried to advance
/// the same workflow simultaneously.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the INSERT fails (e.g. unique
/// constraint violation on `(workflow_exec_id, event_id)` or connection error).
pub async fn append_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
    start_id: i32,
) -> HarvestResult<usize> {
    if events.is_empty() {
        return Ok(0);
    }

    // Cross-region DR write-authority fence (issue #954). Below the empty-append
    // early return: an empty append writes nothing, so there is nothing to
    // fence, and checking above it turned a pure `Ok(0)` into a round trip.
    crate::replication::assert_fence(conn, exec_id.shard()).await?;

    let rows = events_to_insert_rows_from(exec_id, events, start_id)?;

    let inserted = diesel::insert_into(harvest_events::table)
        .values(&rows)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    if let Some(last_event) = events.last() {
        crate::notify::notify_workflow_events_appended(
            conn,
            exec_id.as_uuid(),
            inserted,
            last_event.type_name(),
        )
        .await?;
    }

    Ok(inserted)
}

/// Append events, offloading any over-threshold payload fields (issue #524).
///
/// Offloads to the configured
/// [`PayloadOffloader`](crate::payload_store::PayloadOffloader).
/// When `offloader` is `None` this delegates verbatim to [`append_events`], so a
/// deployment with no `PayloadStore` registered sees byte-for-byte identical
/// behaviour. Otherwise each event's payload-bearing fields are offloaded (after
/// codec encode) and a per-execution reference row is recorded in
/// `harvest_payload_refs` for each blob created, so the retention sweep can GC it
/// when the execution is collected.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError`] on a store `put` failure, serialization
/// error, or INSERT failure.
#[cfg(feature = "db")]
pub async fn append_events_offloaded(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
    start_id: i32,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<usize> {
    let Some(offloader) = offloader else {
        return append_events(conn, exec_id, events, start_id).await;
    };
    if events.is_empty() {
        return Ok(0);
    }

    let mut rows = events_to_insert_rows_from(exec_id, events, start_id)?;
    let mut all_refs: Vec<crate::payload_store::OffloadedRef> = Vec::new();
    for row in &mut rows {
        let refs = offloader.offload_event_value(&mut row.event_data).await?;
        all_refs.extend(refs);
    }

    // Wrap the event INSERT and the ref INSERT in one transaction so that a
    // failed ref INSERT cannot leave events with offload envelopes but no
    // corresponding harvest_payload_refs rows (which would make those blobs
    // permanently invisible to the GC sweep).
    let inserted = Box::pin(conn.transaction::<usize, crate::error::HarvestError, _>(
        async |conn| {
            // Cross-region DR write-authority fence (issue #954), inside the
            // transaction and *after* the offload upload above. Checking before
            // the upload was doubly wrong: the check's `ACCESS SHARE` was
            // released before the INSERT ever began (so it was not a barrier at
            // all on an autocommit caller), and on a transactional caller it
            // held the lock across an unbounded network upload — so an operator
            // fencing during an incident queued behind the slowest payload PUT.
            crate::replication::assert_fence(conn, exec_id.shard()).await?;

            let inserted = diesel::insert_into(harvest_events::table)
                .values(&rows)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            if !all_refs.is_empty() {
                insert_payload_refs(conn, exec_id, &all_refs).await?;
            }
            Ok(inserted)
        },
    ))
    .await?;

    if let Some(last_event) = events.last() {
        crate::notify::notify_workflow_events_appended(
            conn,
            exec_id.as_uuid(),
            inserted,
            last_event.type_name(),
        )
        .await?;
    }

    Ok(inserted)
}

/// Record per-execution references to offloaded payload blobs (issue #524).
///
/// Idempotent: a duplicate `(blob_key, workflow_exec_id)` row is ignored, so a
/// retried append or a carry-forward of an already-referenced key is safe.
#[cfg(feature = "db")]
pub async fn insert_payload_refs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    refs: &[crate::payload_store::OffloadedRef],
) -> HarvestResult<()> {
    use crate::models::NewHarvestPayloadRef;
    use crate::schema::harvest_payload_refs;

    if refs.is_empty() {
        return Ok(());
    }
    let rows: Vec<NewHarvestPayloadRef> = refs
        .iter()
        .map(|r| NewHarvestPayloadRef {
            blob_key: r.blob_key.clone(),
            workflow_exec_id: exec_id.as_uuid(),
            store_id: r.store_id.clone(),
            byte_len: i64::try_from(r.byte_len).unwrap_or(i64::MAX),
        })
        .collect();
    diesel::insert_into(harvest_payload_refs::table)
        .values(&rows)
        .on_conflict_do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Load all blob references for an execution (issue #524). Used by the retention
/// sweep to discover an execution's blobs before deleting it.
#[cfg(feature = "db")]
pub async fn load_payload_refs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<crate::payload_store::OffloadedRef>> {
    use crate::schema::harvest_payload_refs::dsl;

    let rows: Vec<(String, String, i64)> = dsl::harvest_payload_refs
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .select((dsl::blob_key, dsl::store_id, dsl::byte_len))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(rows
        .into_iter()
        .map(
            |(blob_key, store_id, byte_len)| crate::payload_store::OffloadedRef {
                blob_key,
                store_id,
                byte_len: byte_len.max(0).cast_unsigned(),
            },
        )
        .collect())
}

/// Whether any execution still references `blob_key` (issue #524).
///
/// The retention sweep calls this after an execution row (and its
/// cascade-deleted refs) is gone to decide whether the blob may be deleted.
#[cfg(feature = "db")]
pub async fn blob_key_still_referenced(
    conn: &mut AsyncPgConnection,
    blob_key: &str,
) -> HarvestResult<bool> {
    use crate::schema::harvest_payload_refs::dsl;

    let found: Option<String> = dsl::harvest_payload_refs
        .filter(dsl::blob_key.eq(blob_key))
        .select(dsl::blob_key)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(found.is_some())
}

/// Return the subset of `blob_keys` that are still referenced by at least one
/// execution (issue #524).
///
/// Used by the retention sweep to batch-check all of a candidate's blobs in a
/// single query rather than one query per blob, eliminating the N+1 pattern.
#[cfg(feature = "db")]
pub async fn batch_blob_keys_still_referenced(
    conn: &mut AsyncPgConnection,
    blob_keys: &[String],
) -> HarvestResult<std::collections::HashSet<String>> {
    use crate::schema::harvest_payload_refs::dsl;

    if blob_keys.is_empty() {
        return Ok(std::collections::HashSet::new());
    }

    let still_referenced: Vec<String> = dsl::harvest_payload_refs
        .filter(dsl::blob_key.eq_any(blob_keys))
        .select(dsl::blob_key)
        .distinct()
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(still_referenced.into_iter().collect())
}

/// Fetch the raw (un-inflated) `data.last_completion_result` of an execution's
/// first event (issue #524).
///
/// Used by continue-as-new carry-forward to copy an offloaded carryover
/// envelope into the successor WITHOUT re-uploading.
///
/// Returns `None` if the execution has no events or the field is absent.
#[cfg(feature = "db")]
pub async fn load_raw_started_carryover(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Option<serde_json::Value>> {
    use crate::models::HarvestEvent;

    let row: Option<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(row.and_then(|r| {
        r.event_data
            .get("data")
            .and_then(|d| d.get("last_completion_result"))
            .cloned()
    }))
}

/// Append a single event to a workflow's history without loading the full log.
///
/// Acquires a row-level lock on the workflow execution before reading
/// `MAX(event_id)`, serializing concurrent appenders (management API paths,
/// timeout enforcement) so they never race to allocate the same event ID.
/// Must be called inside a transaction; the lock is held until the transaction
/// commits or rolls back.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query or insert fails,
/// or [`crate::error::HarvestError::NotFound`] if the execution does not exist.
pub async fn append_single_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event: WorkflowEvent,
) -> HarvestResult<()> {
    use crate::models::WorkflowExecution;
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::max;

    // Cross-region DR fence (see `replication::assert_fence`), before the
    // execution row lock so a fenced worker releases immediately rather than
    // holding a lock the region that now owns this data needs.
    crate::replication::assert_fence(conn, exec_id.shard()).await?;

    // Lock the parent execution row so that concurrent callers serialise their
    // MAX(event_id) + INSERT pairs — preventing a duplicate-event-id collision
    // on the UNIQUE(workflow_exec_id, event_id) constraint.
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| {
            crate::error::HarvestError::NotFound(format!("workflow execution {exec_id}"))
        })?;

    let max_id: Option<i32> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(max(harvest_events::event_id))
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_id = max_id.map_or(0, |id| id.saturating_add(1));
    append_events(conn, exec_id, &[event], next_id).await?;
    Ok(())
}

/// Read the next event id (`MAX(event_id) + 1`) for an execution's history,
/// taking the same `FOR UPDATE` row lock [`append_single_event`] uses so the
/// returned value stays valid for a subsequent append inside the same
/// transaction. Returns `0` when the execution has no events yet.
///
/// Callers that maintain an in-memory event-id cursor must use this (rather
/// than a fixed increment) to re-synchronise after another code path appended a
/// **variable** number of events onto the same history within the transaction —
/// e.g. `notify_awaited_parent_of_child_terminal`, which appends the child
/// terminal plus zero or more preceding materialized `__child_timeout`
/// `TimerFired` deadlines (issue #779, Codex P2). Re-reading the true next id
/// prevents a later append from reusing a consumed id and colliding on the
/// `UNIQUE(workflow_exec_id, event_id)` constraint.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails, or
/// [`crate::error::HarvestError::NotFound`] if the execution does not exist.
pub(crate) async fn next_event_id_for(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<i32> {
    use crate::models::WorkflowExecution;
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::max;

    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| {
            crate::error::HarvestError::NotFound(format!("workflow execution {exec_id}"))
        })?;

    let max_id: Option<i32> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(max(harvest_events::event_id))
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(max_id.map_or(0, |id| id.saturating_add(1)))
}

/// Count an execution's durably-persisted `harvest_events` rows.
///
/// Lock-free (no `FOR UPDATE`, no execution-row existence check) and
/// autocommit-safe -- unlike [`next_event_id_for`], which is meant for
/// in-transaction use and raises [`crate::error::HarvestError::NotFound`] on
/// a missing execution. This is a best-effort read intended for post-commit
/// telemetry decisions (issue #704's history-bloat soft-threshold warning):
/// the caller must already know the execution exists (it just persisted a
/// decision cycle for it), so a `NotFound` distinction is unnecessary here
/// -- an execution with zero rows (impossible in practice) simply counts 0.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub(crate) async fn count_history_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<u64> {
    use diesel::dsl::count_star;

    let count: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(count_star())
        .first(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(u64::try_from(count).unwrap_or(0))
}

/// Durably admit an update into a workflow's event history.
///
/// Opens a transaction, acquires a row-level `FOR UPDATE` lock on the
/// execution row, verifies the execution is still `RUNNING`, reads
/// `MAX(event_id)`, and then appends the `UpdateAdmitted` event.  Doing
/// the state check and the insert inside the same lock ensures that a
/// concurrent state transition (e.g. `RUNNING → COMPLETED` from the worker)
/// is fully visible before admission proceeds.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::NotFound`] if `exec_id` does not exist.
/// Returns [`crate::error::HarvestError::UpdateRejected`] if the execution is
/// not in the `RUNNING` state.
/// Returns [`crate::error::HarvestError::Database`] on query or insert failure.
///
/// When `metrics` is `Some`, `harvest.update.admitted` is emitted **once,
/// post-commit** (issue #684), labeled by the resolved workflow name and queue.
/// Callers that run `admit_update_event` inside a larger outer transaction
/// (e.g. `update_with_start`) pass `None` and emit at their own outer-commit
/// boundary instead, so the metric never fires on a rollback.
///
/// The update `name` is deliberately NOT a label (issue #684, Codex P2): unlike
/// the terminal `harvest.update.completed`/`failed` counters — which bound an
/// unregistered name to the `__unregistered__` sentinel using the workflow's
/// handler-not-found result — the admission site has no way to know whether a
/// name resolves to a handler (imperative `ctx.register_update_handler`
/// handlers are not known until the workflow executes), so it cannot bound the
/// name without mislabeling legitimate imperatively-registered updates.
/// Dropping the label bounds this counter's cardinality by construction;
/// per-name visibility lives on the post-resolution completed/failed/rejected
/// counters.
pub async fn admit_update_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    update_id: crate::types::UpdateId,
    name: String,
    input: serde_json::Value,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
) -> HarvestResult<()> {
    use crate::models::WorkflowExecution;
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::max;

    let (workflow_name, queue_name) = Box::pin(
        conn.transaction::<(String, String), crate::error::HarvestError, _>(async |conn| {
            // Acquire a row-level lock so concurrent appenders serialize their
            // MAX(event_id) + INSERT pairs and the state check is consistent.
            let execution: WorkflowExecution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .for_update()
                .select(WorkflowExecution::as_select())
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .ok_or_else(|| {
                    crate::error::HarvestError::NotFound(format!("workflow execution {exec_id}"))
                })?;

            // Reject updates submitted while the execution is paused with a
            // dedicated error (issue #383). Updates may admit-and-mutate workflow
            // state, so they are rejected rather than silently queued behind the
            // pause — surfacing operator intent as a 409 at the API layer.
            if execution.state == "PAUSED" {
                return Err(crate::error::HarvestError::WorkflowPaused(exec_id));
            }

            // Reject the update if the execution is no longer running.
            if execution.state != "RUNNING" {
                return Err(crate::error::HarvestError::UpdateRejected {
                    reason: format!(
                        "workflow {exec_id} is not RUNNING (state: {})",
                        execution.state
                    ),
                });
            }

            let max_id: Option<i32> = harvest_events::table
                .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
                .select(max(harvest_events::event_id))
                .first(conn)
                .await
                .map_err(crate::error::database_error)?;

            let next_id = max_id.map_or(0, |id| id.saturating_add(1));
            let event = WorkflowEvent::UpdateAdmitted {
                update_id,
                name,
                input,
                timestamp: chrono::Utc::now(),
            };
            append_events(conn, exec_id, &[event], next_id).await?;
            Ok((execution.workflow_name, execution.queue_name))
        }),
    )
    .await?;

    // Post-commit: emit harvest.update.admitted exactly once (issue #684).
    if let Some(metrics) = metrics {
        metrics.record_update_admitted(&workflow_name, &queue_name);
    }
    Ok(())
}

/// Load the full event history for a workflow execution, ordered by `event_id`.
///
/// Lock the workflow execution row `FOR UPDATE` and then load its full event
/// history.
///
/// Acquiring the row lock first ensures that concurrent event appends
/// (e.g. from a second worker racing on the same task) serialise correctly:
/// the transaction that holds the lock owns the right to append the next
/// event batch.
///
/// This is an internal helper called by the transactional activity commit
/// path in [`crate::context::ActivityContext::run_transactional`] and by
/// several private functions in `worker.rs`.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::NotFound`] when the execution row
/// does not exist, and [`crate::error::HarvestError::Database`] on any other
/// query failure.
pub(crate) async fn lock_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<EventHistory> {
    use crate::error::HarvestError;
    use crate::schema::harvest_workflow_executions::dsl;

    // Acquire a row-level lock so concurrent writers serialize around this
    // transaction.  We only need the id to confirm the row exists.
    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .for_update()
        .select(dsl::id)
        .first::<uuid::Uuid>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    load_history(conn, exec_id).await
}

/// Deserializes each row's `event_data` JSON back into [`WorkflowEvent`].
///
/// The returned [`EventHistory::next_event_id`] is set to one past the last
/// loaded event (or 0 if the history is empty), ready for use with
/// [`append_events()`].
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on connection or query errors, or
/// [`crate::error::HarvestError::Serialization`] if a stored JSON value can't be deserialized
/// into `WorkflowEvent`.
pub async fn load_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<EventHistory> {
    load_history_with_codecs(
        conn,
        exec_id,
        &crate::payload_codec::PayloadCodecs::default(),
    )
    .await
}

pub async fn load_history_with_codecs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_event_id = rows.last().map_or(0, |r| r.event_id.saturating_add(1));

    let events = rows
        .into_iter()
        .map(|row| codecs.decode_event(row.event_data))
        .collect::<Result<Vec<WorkflowEvent>, _>>()?;

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
}

/// Load history **without** applying any payload-codec transform (issue #608).
///
/// Each row's `event_data` is deserialized directly into [`WorkflowEvent`],
/// so codec envelopes (`_harvest_codec_envelope`) written by a non-identity
/// codec ride along verbatim as opaque [`serde_json::Value`]s inside the
/// event's payload fields. This is the loader for operator **read surfaces**
/// that apply their own payload policy downstream — history-export redaction
/// replaces payload fields wholesale (envelope included), and the issue-#608
/// read-path decoder tolerantly decodes (or marks) each envelope per-field —
/// so the strict identity-only [`load_history`] path (which hard-errors
/// `UnknownPayloadCodec` on the first foreign envelope) must not pre-empt
/// them.
///
/// On an identity-codec deployment no envelopes are ever stored (the identity
/// default encodes payloads as plain JSON), so this returns byte-identical
/// events to [`load_history`].
///
/// **Never use this for replay or any engine execution path** — replay must
/// see decoded plaintext and uses the codec-aware loaders
/// ([`load_history_inflated`] / [`load_history_with_codecs`]).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on connection or query
/// errors, or [`crate::error::HarvestError::Serialization`] if a stored JSON
/// value can't be deserialized into [`WorkflowEvent`].
pub async fn load_history_undecoded(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_event_id = rows.last().map_or(0, |r| r.event_id.saturating_add(1));

    let events = rows
        .into_iter()
        .map(|row| {
            serde_json::from_value::<WorkflowEvent>(row.event_data)
                .map_err(crate::error::HarvestError::from)
        })
        .collect::<Result<Vec<WorkflowEvent>, _>>()?;

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
}

/// Load every event of an execution paired with its `harvest_events` row
/// timestamp, ordered by `event_id ASC` (issue #739).
///
/// This is the read-only input loader for the per-execution timeline read model
/// ([`crate::timeline::derive_timeline`]). Each row's `event_data` is decoded
/// directly with `serde_json::from_value` — **no** payload codec is applied
/// (mirroring [`load_history_undecoded`]): the timeline reads only structural
/// fields (ids, names, `attempt`, durations), which payload codecs never
/// encrypt, so a foreign codec envelope riding along inside a payload field is
/// harmless and must not make the strict identity-only [`load_history`] path
/// hard-error `UnknownPayloadCodec`.
///
/// **Known limitation — unbounded load.** No pagination or `LIMIT`: the *entire*
/// event history is loaded (consistent with replay's full load), even though
/// this is an HTTP-triggerable read surface. A hard cap is deliberately **not**
/// imposed, because a silently-truncated history would make the timeline rollup
/// (busy/wait attribution, slowest step) wrong rather than merely incomplete.
/// Bounding the history size is instead the workflow author's responsibility via
/// continue-as-new discipline (the ≤500-event target); a run that ignores that
/// discipline and accumulates a very large history will make this loader (and the
/// timeline derivation) proportionally expensive. **Never use this for replay or
/// any engine execution path**; it is a read-surface loader only.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on connection or query
/// errors, or [`crate::error::HarvestError::Serialization`] if a stored JSON
/// value can't be deserialized into [`WorkflowEvent`].
#[cfg(feature = "db")]
pub async fn load_timestamped_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<crate::timeline::TimelineEventRow>> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    rows.into_iter()
        .map(|row| {
            serde_json::from_value::<WorkflowEvent>(row.event_data)
                .map(|event| crate::timeline::TimelineEventRow {
                    timestamp: row.timestamp,
                    event,
                })
                .map_err(crate::error::HarvestError::from)
        })
        .collect()
}

/// Load a workflow's full event history paired with each event's
/// `harvest_events.timestamp` (issue #690).
///
/// [`WorkflowEvent`] variants carry no timestamp and [`load_history`] discards
/// the per-row `timestamp` column, but read views such as the DAG run graph
/// need per-node `started_at`/`finished_at` timing. This helper loads the
/// [`HarvestEvent`](crate::models::HarvestEvent) rows in `event_id` order and
/// zips each row's `timestamp` with its deserialized event.
///
/// Payloads are deserialized directly (no codec transform), matching
/// [`load_history_undecoded`]: on an identity-codec deployment this is
/// byte-identical to [`load_history`]. The DAG graph classification reads
/// non-payload fields (activity name, error type) plus the error string, and
/// the `dag_skip:{idx}` marker's `details` fingerprint — a payload field that
/// is an opaque codec/offload envelope on a non-identity-codec deployment. The
/// classifier (`dag_graph::has_skip_marker`, issue #690 review) tolerates that
/// opacity by falling back to the always-clear marker name/index, so this raw
/// (non-decoding) load is correct for the graph view in every deployment.
///
/// **Never use this for replay or any engine execution path** — replay must see
/// codec-decoded plaintext and uses the codec-aware loaders.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on connection or query
/// errors, or [`crate::error::HarvestError::Serialization`] if a stored JSON
/// value can't be deserialized into [`WorkflowEvent`].
pub async fn load_history_with_timestamps(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<(chrono::DateTime<chrono::Utc>, WorkflowEvent)>> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    rows.into_iter()
        .map(|row| {
            let event = serde_json::from_value::<WorkflowEvent>(row.event_data)
                .map_err(crate::error::HarvestError::from)?;
            Ok((row.timestamp, event))
        })
        .collect()
}

/// Load history, inflating any offloaded payload fields from the configured
/// [`PayloadOffloader`](crate::payload_store::PayloadOffloader) (issue #524).
///
/// When `offloader` is `None` this delegates verbatim to [`load_history`].
/// Otherwise each loaded event is inflated (store fetch + checksum verify)
/// **before** codec decode, the inverse of the encode-then-offload write order,
/// so replay sees byte-identical payloads.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError`] on a store `get` failure, checksum
/// mismatch, or deserialization error.
#[cfg(feature = "db")]
pub async fn load_history_inflated(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    codecs: &crate::payload_codec::PayloadCodecs,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let Some(offloader) = offloader else {
        return load_history_with_codecs(conn, exec_id, codecs).await;
    };

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_event_id = rows.last().map_or(0, |r| r.event_id.saturating_add(1));

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let mut data = row.event_data;
        offloader.inflate_event_value(&mut data).await?;
        events.push(codecs.decode_event(data)?);
    }

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
}

/// Load only events appended since a known event-id cursor.
///
/// Returns events where `event_id >= from_event_id`, ordered by `event_id ASC`.
/// When the result is empty (no new events), `next_event_id` is set to
/// `from_event_id` so callers can use it as the baseline for the next ingestion.
///
/// This is the delta-load companion to [`load_history`]: the worker calls this
/// on cache hits to fetch only the timer-fire / signal events appended since
/// the last suspension, and prepends the cached event snapshot to reconstruct
/// the full history without reading old events from Postgres.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn load_history_since(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    from_event_id: i32,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_events::event_id.ge(from_event_id))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_event_id = rows
        .last()
        .map_or(from_event_id, |r| r.event_id.saturating_add(1));

    let events = rows
        .into_iter()
        .map(|row| crate::payload_codec::PayloadCodecs::default().decode_event(row.event_data))
        .collect::<Result<Vec<WorkflowEvent>, _>>()?;

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
}

/// Delta-load companion to [`load_history_inflated`] (issue #524): load events
/// with `event_id >= from_event_id`, inflating offloaded payloads before decode.
///
/// When `offloader` is `None` this delegates verbatim to [`load_history_since`].
///
/// # Errors
///
/// Returns [`crate::error::HarvestError`] on a store `get` failure, checksum
/// mismatch, or deserialization error.
#[cfg(feature = "db")]
pub async fn load_history_since_inflated(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    from_event_id: i32,
    codecs: &crate::payload_codec::PayloadCodecs,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_events::event_id.ge(from_event_id))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let next_event_id = rows
        .last()
        .map_or(from_event_id, |r| r.event_id.saturating_add(1));

    let mut events = Vec::with_capacity(rows.len());
    if let Some(offloader) = offloader {
        for row in rows {
            let mut data = row.event_data;
            offloader.inflate_event_value(&mut data).await?;
            events.push(codecs.decode_event(data)?);
        }
    } else {
        for row in rows {
            events.push(codecs.decode_event(row.event_data)?);
        }
    }

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
}

/// Load raw `harvest_events` rows for `exec_id` with `id > after_row_id`.
///
/// Returns rows ordered by `id ASC`. The `id` column is the `BIGSERIAL` primary
/// key and serves as the SSE resume cursor (`Last-Event-ID`). Pass `-1` for
/// `after_row_id` to load all events.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn load_events_after_row_id(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    after_row_id: i64,
    limit: Option<i64>,
) -> HarvestResult<Vec<crate::models::HarvestEvent>> {
    let mut query = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_events::id.gt(after_row_id))
        .order(harvest_events::id.asc())
        .into_boxed();
    if let Some(n) = limit {
        query = query.limit(n);
    }
    query
        .select(crate::models::HarvestEvent::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// A single deserialized row from a paged history query.
#[derive(Debug)]
pub struct PagedHistoryEvent {
    /// `harvest_events.id` — the BIGSERIAL row cursor anchor.
    pub id: i64,
    /// Sequential event index within this execution (`event_id` column).
    pub event_id: i32,
    /// Wall-clock timestamp recorded when the event was appended.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Deserialized workflow event (for structured inspection and pattern matching).
    pub event: crate::event::WorkflowEvent,
    /// Raw adjacently-tagged JSON exactly as stored in `harvest_events.event_data`.
    /// Avoids a round-trip serialize when the caller only needs the wire representation.
    pub raw_event: serde_json::Value,
}

/// Result of a paged history query.
#[derive(Debug)]
pub struct HistoryPage {
    /// The events on this page (at most `limit` entries).
    pub events: Vec<PagedHistoryEvent>,
    /// Opaque cursor to pass as `after` on the next request.  `None` when this
    /// is the last page.
    pub next_cursor: Option<i64>,
    /// Total event count for this execution, ignoring any type filter.
    pub total_events: i64,
    /// Highest `harvest_events.id` for this execution (unfiltered).
    pub last_event_id: i64,
}

/// Returns `true` when a workflow execution with `exec_id` exists in the database.
///
/// Cheaper than [`load_execution`] when only presence needs to be confirmed
/// because it projects only the primary key column.
#[cfg(feature = "db")]
pub async fn check_execution_exists(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> crate::error::HarvestResult<bool> {
    use diesel::dsl::exists;
    diesel::select(exists(
        harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid())),
    ))
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Load a page of events for `exec_id`.
///
/// - `after_id`: exclusive lower-bound cursor (`harvest_events.id`).  `None`
///   means start from the first event.
/// - `limit`: maximum number of events to return (1–1000).
/// - `event_types`: if non-empty, only return rows whose `event_type` is in
///   this list.  Unknown type names yield an empty page (not an error).
/// - `include_totals`: when `true`, runs an extra `COUNT`/`MAX` aggregate
///   query to populate `HistoryPage::total_events` and `HistoryPage::last_event_id`.
///   Pass `false` when the caller doesn't need these fields to save a DB round-trip.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure or if
/// any stored event cannot be decoded.
#[cfg(feature = "db")]
pub async fn load_history_page(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    after_id: Option<i64>,
    limit: i64,
    event_types: &[String],
    include_totals: bool,
) -> crate::error::HarvestResult<HistoryPage> {
    use diesel::dsl::{count_star, max};

    // ── 1. Aggregate query: total events + last id (unfiltered) ──────────────
    // Skipped when the caller doesn't need totals (e.g. get_workflow bounded view).
    let (total_events, last_event_id) = if include_totals {
        let (total, last): (i64, Option<i64>) = harvest_events::table
            .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
            .select((count_star(), max(harvest_events::id)))
            .first(conn)
            .await
            .map_err(crate::error::database_error)?;
        (total, last.unwrap_or(0))
    } else {
        (0, 0)
    };

    // ── 2. Page query: fetch limit+1 rows to detect next page ────────────────
    let fetch = limit + 1;
    let mut query = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::id.asc())
        .limit(fetch)
        .into_boxed();

    if let Some(after) = after_id {
        query = query.filter(harvest_events::id.gt(after));
    }
    if !event_types.is_empty() {
        query = query.filter(harvest_events::event_type.eq_any(event_types));
    }

    let mut rows: Vec<crate::models::HarvestEvent> = query
        .select(crate::models::HarvestEvent::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    // ── 3. Detect next page using the extra row ───────────────────────────────
    // `limit` is validated to [1, MAX_HISTORY_PAGE=1000] by the caller before this function is
    // invoked, so the usize conversions below are always safe on any supported target.
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let next_cursor = if rows.len() > limit_usize {
        let cursor = rows[limit_usize - 1].id;
        rows.truncate(limit_usize);
        Some(cursor)
    } else {
        None
    };

    // ── 4. Decode events ──────────────────────────────────────────────────────
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_event = row.event_data.clone();
        let event: crate::event::WorkflowEvent =
            serde_json::from_value(row.event_data).map_err(crate::error::HarvestError::from)?;
        events.push(PagedHistoryEvent {
            id: row.id,
            event_id: row.event_id,
            timestamp: row.timestamp,
            event,
            raw_event,
        });
    }

    Ok(HistoryPage {
        events,
        next_cursor,
        total_events,
        last_event_id,
    })
}

/// Load the direct children of `parent_id` from one shard.
///
/// Callers that need cross-shard discovery should call this once per shard and
/// merge the rows after applying any global ordering/pagination.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn load_workflow_children(
    conn: &mut AsyncPgConnection,
    parent_id: ExecutionId,
    filters: &WorkflowChildFilters,
    depth: u8,
) -> HarvestResult<Vec<WorkflowChildRow>> {
    let mut query = harvest_workflow_executions::table
        .into_boxed()
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_id.as_uuid())))
        .order((
            harvest_workflow_executions::started_at.desc(),
            harvest_workflow_executions::id.desc(),
        ));

    if !filters.statuses.is_empty() {
        query = query.filter(harvest_workflow_executions::state.eq_any(filters.statuses.clone()));
    }
    if let Some(name) = &filters.workflow_name {
        query = query.filter(harvest_workflow_executions::workflow_name.eq(name.clone()));
    }
    if let Some(cursor) = &filters.cursor {
        query = query.filter(
            harvest_workflow_executions::started_at
                .lt(cursor.started_at)
                .or(harvest_workflow_executions::started_at
                    .eq(cursor.started_at)
                    .and(harvest_workflow_executions::id.lt(cursor.exec_id))),
        );
    }
    if let Some(limit) = filters.limit {
        query = query.limit(limit);
    }

    query
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
            harvest_workflow_executions::completed_at,
            harvest_workflow_executions::error,
            harvest_workflow_executions::shard_id,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<WorkflowChildProjection>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| {
            rows.into_iter()
                .map(
                    |(
                        id,
                        workflow_name,
                        state,
                        started_at,
                        completed_at,
                        error,
                        shard_id,
                        parent_close_policy,
                    )| {
                        workflow_child_row_from_parts(
                            id,
                            workflow_name,
                            state,
                            started_at,
                            completed_at,
                            error,
                            shard_id,
                            depth,
                            parent_close_policy.as_deref(),
                        )
                    },
                )
                .collect()
        })
}

#[allow(clippy::too_many_arguments)]
fn workflow_child_row_from_parts(
    id: uuid::Uuid,
    workflow_name: String,
    state: String,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    error: Option<String>,
    shard_id: i32,
    depth: u8,
    parent_close_policy_str: Option<&str>,
) -> WorkflowChildRow {
    let parent_close_policy =
        parent_close_policy_str.and_then(|s| s.parse::<crate::types::ParentClosePolicy>().ok());
    let await_mode = if parent_close_policy.is_some() {
        AwaitMode::Detached
    } else {
        AwaitMode::Awaited
    };
    WorkflowChildRow {
        exec_id: ExecutionId::from_uuid(id),
        workflow_name,
        status: state,
        started_at,
        completed_at,
        error_summary: summarize_error(error),
        shard_id,
        depth,
        await_mode,
        parent_close_policy,
    }
}

/// Load the direct children of every id in `parent_ids` from one shard, in
/// one `parent_id = ANY($1)` query.
///
/// This is the batched sibling of `load_workflow_children`: a caller walking
/// a traversal *frontier* (the accumulated set of parents discovered at one
/// depth level) should call this once per shard per depth level rather than
/// calling `load_workflow_children` once per *parent* per shard per depth
/// level — the latter costs `O(nodes × shards)` round trips for a depth-`D`
/// tree, this costs `O(D × shards)`. See the "Cross-shard lineage tree
/// loaders" doc comment below for the same argument as applied to
/// `load_workflow_children_batch`, which this mirrors exactly except for
/// returning `WorkflowChildRow` (this endpoint's flat, non-parent-tagged
/// projection) instead of `LineageChildRow` (the nested `/tree` endpoint's
/// parent-tagged one) — the caller here doesn't need to know *which* parent
/// in the frontier produced a given child, only the whole next frontier and
/// the whole set of matching rows.
///
/// Returns an empty vec without querying when `parent_ids` is empty.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn load_workflow_children_multi(
    conn: &mut AsyncPgConnection,
    parent_ids: &[uuid::Uuid],
    filters: &WorkflowChildFilters,
    depth: u8,
) -> HarvestResult<Vec<WorkflowChildRow>> {
    if parent_ids.is_empty() {
        return Ok(Vec::new());
    }

    let parents: Vec<uuid::Uuid> = parent_ids.to_vec();
    let mut query = harvest_workflow_executions::table
        .into_boxed()
        .filter(harvest_workflow_executions::parent_id.eq_any(parents))
        .order((
            harvest_workflow_executions::started_at.desc(),
            harvest_workflow_executions::id.desc(),
        ));

    if !filters.statuses.is_empty() {
        query = query.filter(harvest_workflow_executions::state.eq_any(filters.statuses.clone()));
    }
    if let Some(name) = &filters.workflow_name {
        query = query.filter(harvest_workflow_executions::workflow_name.eq(name.clone()));
    }
    if let Some(cursor) = &filters.cursor {
        query = query.filter(
            harvest_workflow_executions::started_at
                .lt(cursor.started_at)
                .or(harvest_workflow_executions::started_at
                    .eq(cursor.started_at)
                    .and(harvest_workflow_executions::id.lt(cursor.exec_id))),
        );
    }
    if let Some(limit) = filters.limit {
        query = query.limit(limit);
    }

    query
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
            harvest_workflow_executions::completed_at,
            harvest_workflow_executions::error,
            harvest_workflow_executions::shard_id,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<WorkflowChildProjection>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| {
            rows.into_iter()
                .map(
                    |(
                        id,
                        workflow_name,
                        state,
                        started_at,
                        completed_at,
                        error,
                        shard_id,
                        parent_close_policy,
                    )| {
                        workflow_child_row_from_parts(
                            id,
                            workflow_name,
                            state,
                            started_at,
                            completed_at,
                            error,
                            shard_id,
                            depth,
                            parent_close_policy.as_deref(),
                        )
                    },
                )
                .collect()
        })
}

// ── Cross-shard lineage tree loaders (issue #621) ────────────────────────────
//
// The recursive lineage walk expands a whole *frontier* of parents per level,
// so these helpers take a batch of parent ids (`parent_id = ANY($1)`) rather
// than a single parent. That turns a depth-`D` tree over `S` shards from
// `O(nodes × S)` round trips (one query per parent per shard — what a naive
// recursion over `load_workflow_children` costs) into `O(D × S)`, which is what
// keeps a ~200-node/8-shard tree inside the issue's p95 < 500 ms budget.
//
// Both helpers are shard-local reads; the cross-shard fan-out and the bounded
// walk itself live in the plugin (`autumn-harvest-plugin::lineage`).

/// One execution row in a cross-shard lineage tree (issue #621).
///
/// Distinct from [`WorkflowChildRow`] (the `GET /workflows/{id}/children` read
/// model): a lineage node additionally carries its own `parent_id` — needed to
/// nest a flat, cross-shard row set back into a tree — and `workflow_id`, the
/// business key an operator recognises. It deliberately does **not** carry an
/// error summary: the tree is a topology/state map, and a node's failure detail
/// is one `GET /workflows/{id}` away.
#[derive(Debug, Clone)]
pub struct LineageChildRow {
    /// This execution's id.
    pub exec_id: ExecutionId,
    /// The parent this row was discovered through. `None` only for a row whose
    /// `parent_id` column is NULL (a root), which the batch loader never
    /// returns — it is populated for every discovered descendant.
    pub parent_id: Option<ExecutionId>,
    /// Registered workflow type name.
    pub workflow_name: String,
    /// Caller-supplied business workflow id.
    pub workflow_id: String,
    /// Raw persisted state (`RUNNING`, `FAILED`, …), matching the `state=`
    /// filter vocabulary of `GET /workflows`.
    pub state: String,
    /// When this execution started.
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When this execution reached a terminal state, if it has.
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Shard this row was read from.
    pub shard_id: i32,
    /// How this child was spawned (awaited vs detached).
    pub await_mode: AwaitMode,
    /// For detached children, the policy applied when the parent closes.
    pub parent_close_policy: Option<crate::types::ParentClosePolicy>,
}

type LineageChildProjection = (
    uuid::Uuid,
    Option<uuid::Uuid>,
    String,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    i32,
    Option<String>,
);

/// Load the direct children of **every** parent in `parent_ids` from one shard,
/// in a single query (issue #621).
///
/// Rows are ordered `started_at ASC, id ASC` and capped at `limit`. The
/// ordering is load-bearing, not cosmetic: when the walk's `max_nodes` budget
/// truncates a level it keeps the first `N` rows of this order, so the tree an
/// operator sees is stable across retries instead of varying with Postgres'
/// physical row order.
///
/// Returns an empty vector without touching the database when `parent_ids` is
/// empty or `limit <= 0` — the walk calls this once per shard per level and
/// both cases occur naturally (an exhausted node budget, an empty frontier).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn load_workflow_children_batch(
    conn: &mut AsyncPgConnection,
    parent_ids: &[uuid::Uuid],
    limit: i64,
) -> HarvestResult<Vec<LineageChildRow>> {
    if parent_ids.is_empty() || limit <= 0 {
        return Ok(Vec::new());
    }

    let parents: Vec<uuid::Uuid> = parent_ids.to_vec();
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq_any(parents))
        .order((
            harvest_workflow_executions::started_at.asc(),
            harvest_workflow_executions::id.asc(),
        ))
        .limit(limit)
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::parent_id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::workflow_id,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
            harvest_workflow_executions::completed_at,
            harvest_workflow_executions::shard_id,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<LineageChildProjection>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| rows.into_iter().map(lineage_child_row_from_parts).collect())
}

fn lineage_child_row_from_parts(parts: LineageChildProjection) -> LineageChildRow {
    let (
        id,
        parent_id,
        workflow_name,
        workflow_id,
        state,
        started_at,
        completed_at,
        shard_id,
        parent_close_policy_str,
    ) = parts;
    let parent_close_policy = parent_close_policy_str
        .as_deref()
        .and_then(|s| s.parse::<crate::types::ParentClosePolicy>().ok());
    // A detached child is exactly one that carries a parent-close policy; an
    // awaited child's column is NULL. Same derivation as
    // `workflow_child_row_from_parts`, kept in lockstep with it.
    let await_mode = if parent_close_policy.is_some() {
        AwaitMode::Detached
    } else {
        AwaitMode::Awaited
    };
    LineageChildRow {
        exec_id: ExecutionId::from_uuid(id),
        parent_id: parent_id.map(ExecutionId::from_uuid),
        workflow_name,
        workflow_id,
        state,
        started_at,
        completed_at,
        shard_id,
        await_mode,
        parent_close_policy,
    }
}

/// Bounded existence probe: of the given `parent_ids`, which ones have at least
/// one child on this shard? (issue #621)
///
/// This is what lets a truncated lineage walk name **precisely** the nodes whose
/// subtrees were dropped, instead of conservatively naming every unexpanded
/// leaf. A false positive there would send an operator chasing a subtree that
/// does not exist, so the walk pays one extra `SELECT DISTINCT` per shard to
/// stay honest.
///
/// The result is `DISTINCT`, so a parent with 500 children contributes one id —
/// the response's dropped-subtree list is bounded by frontier size, not by
/// child count.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn load_parents_with_children(
    conn: &mut AsyncPgConnection,
    parent_ids: &[uuid::Uuid],
) -> HarvestResult<Vec<uuid::Uuid>> {
    if parent_ids.is_empty() {
        return Ok(Vec::new());
    }

    let parents: Vec<uuid::Uuid> = parent_ids.to_vec();
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq_any(parents))
        .select(harvest_workflow_executions::parent_id)
        .distinct()
        .load::<Option<uuid::Uuid>>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| rows.into_iter().flatten().collect())
}

/// Bounded existence probe against **retained summaries**: which of the given
/// `parent_ids` have a demoted child on this shard? (issues #621, #752)
///
/// "Demoted" means the child now lives only in `harvest_execution_summaries`.
/// Tiered summary retention is opt-in, but when it is on, a *terminal* child is
/// independently retention-eligible: it can be demoted into a summary and have
/// its `harvest_workflow_executions` row deleted while its long-running parent
/// is still live. [`load_workflow_children_batch`] reads only the executions
/// table, so such a child — and everything beneath it — is invisible to a
/// lineage walk. Without this probe a `FAILED` demoted child would produce a
/// tree that reports itself complete, which is precisely the silent omission
/// the walk's bounds machinery exists to prevent.
///
/// Retention deliberately preserves `harvest_execution_summaries.parent_id` for
/// exactly this case (it skips the terminal-child `parent_id` null-out when
/// summary retention is enabled), and [`crate::erase`]'s cascade already unions
/// both tables on it; this is the read-side counterpart.
///
/// Returns only the *nearest live ancestors* of omitted lineage. A summary
/// child's own children are not enumerated — naming the live node an operator
/// can actually act from is the useful signal, and it keeps the probe a single
/// `DISTINCT` per shard.
///
/// When summary retention is disabled (the default) the table is empty and this
/// returns nothing, so the walk is unaffected.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn load_parents_with_summary_children(
    conn: &mut AsyncPgConnection,
    parent_ids: &[uuid::Uuid],
) -> HarvestResult<Vec<uuid::Uuid>> {
    if parent_ids.is_empty() {
        return Ok(Vec::new());
    }

    let parents: Vec<uuid::Uuid> = parent_ids.to_vec();
    harvest_execution_summaries::table
        .filter(harvest_execution_summaries::parent_id.eq_any(parents))
        .select(harvest_execution_summaries::parent_id)
        .distinct()
        .load::<Option<uuid::Uuid>>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| rows.into_iter().flatten().collect())
}

/// Merge-patch the `search_attrs` JSONB column for a workflow execution.
///
/// `Some(value)` entries in `patch` overwrite the stored key; `None` entries
/// remove the key. Keys absent from `patch` are preserved. The update is done
/// as an atomic read-modify-write within the caller's transaction context so
/// concurrent same-execution updates (which the task queue serialises) do not
/// race.
pub async fn update_search_attrs<S: std::hash::BuildHasher + Sync>(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    patch: &std::collections::HashMap<String, Option<serde_json::Value>, S>,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    if patch.is_empty() {
        return Ok(());
    }

    // Read the current value so we can apply the merge in Rust.  Workflow
    // tasks are serialised per-execution by SKIP LOCKED, so no TOCTOU risk.
    let current: Option<serde_json::Value> = dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::search_attrs)
        .first::<Option<serde_json::Value>>(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut merged: serde_json::Map<String, serde_json::Value> = match current {
        Some(serde_json::Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    };

    for (key, value) in patch {
        match value {
            Some(v) => {
                merged.insert(key.clone(), v.clone());
            }
            None => {
                merged.remove(key.as_str());
            }
        }
    }

    let new_attrs = serde_json::Value::Object(merged);

    diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set(dsl::search_attrs.eq(Some(new_attrs)))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

/// Overwrite `current_details` on the execution row (issue #473).
///
/// Called by the worker after each execution cycle when the workflow author
/// called `ctx.set_current_details(...)` during live (non-replay) execution.
/// Uses a simple overwrite; the application layer enforces last-write-wins
/// (and empty-string-clears, issue #593) via
/// `worker::latest_current_details_update`, which resolves the command list
/// to the single effective write before this function is ever called.
///
/// `details = None` clears the column to SQL `NULL` (the workflow called
/// `set_current_details("")`); `details = Some(s)` sets it to `s`.
pub async fn update_current_details(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    details: Option<&str>,
) -> crate::error::HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set(dsl::current_details.eq(details))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Durable per-execution workflow logs (issue #790)
// ---------------------------------------------------------------------------

/// `seq` reserved for the per-execution truncation marker (issue #790).
///
/// `i64::MAX` sorts strictly after every real line — a real `seq` is a
/// **call ordinal** (the Nth `ctx.log_*` call of this run), so a workflow would
/// have to make ~2^63 log calls in one execution to reach it, and the
/// per-execution line cap plus the in-memory enqueue bound stop storing long
/// before that. Using the ordering key itself to pin the marker last means no
/// extra column and no special-casing in the read path: the marker is just the
/// final line.
pub const WORKFLOW_LOG_TRUNCATION_SEQ: i64 = i64::MAX;

/// Message stored on the truncation marker row.
pub const WORKFLOW_LOG_TRUNCATION_MESSAGE: &str =
    "[harvest] per-execution log cap reached; subsequent lines were dropped";

/// One log line to persist, as resolved from the drained command list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowLogLine {
    /// Deterministic logical-position identity; the dedup + ordering key.
    pub seq: i64,
    /// `"info"` / `"warn"` / `"error"`.
    pub level: &'static str,
    /// The author's message (already byte-capped by the context).
    pub message: String,
}

/// Append durable workflow log lines, enforcing the per-execution cap.
///
/// **Exactly-once (issue #790 AC2).** Every row is inserted with
/// `ON CONFLICT DO NOTHING` against the unique `(workflow_exec_id, seq)` index.
/// Replay-suppression alone is not sufficient — a decision cycle that logs and
/// then parks can be re-driven at an *unchanged* history position (a spurious
/// wake, or a cycle whose persist rolled back), where `is_replaying()` is still
/// false and the line is emitted again. Because `seq` is a deterministic
/// function of that position, the re-emitted line collides and is collapsed.
///
/// **Bounded volume (AC4) — drop-newest.** Once the execution holds `max_lines`
/// real lines, further lines are dropped and a single terminal truncation
/// marker (`WORKFLOW_LOG_TRUNCATION_SEQ`) is recorded instead, so the loss is
/// visible rather than silent. Drop-newest keeps the start of the run (where a
/// workflow's setup and branch decisions are) and costs one bounded `COUNT`
/// per decision cycle rather than a `DELETE` per line.
///
/// **The marker is terminal.** Once it exists the gate stays shut: a later
/// batch is rejected even if `max_lines` has since been RAISED. This is not
/// hypothetical — `max_lines` is per-worker-process config, so on a rolling
/// deployment a run can truncate under an old worker's cap and have its next
/// decision cycle handled by a new worker with a larger one. Re-deciding
/// admission against the current policy would store a line *after* the one that
/// was dropped, leaving a hole in the stored prefix and a marker whose
/// "subsequent lines were dropped" claim is false. Latching keeps the stored
/// rows a contiguous prefix of the run and keeps the marker honest; the
/// already-dropped lines are unrecoverable either way, so re-opening the gate
/// buys nothing.
///
/// Rejecting a post-marker batch wholesale loses nothing: every line in such a
/// batch is either already stored (so `ON CONFLICT DO NOTHING` would have
/// collapsed it) or was deliberately dropped.
///
/// Returns the number of real (non-marker) rows **actually inserted** — the
/// affected-row count from the INSERT, not the number offered. A re-driven
/// cycle whose rows are all collapsed by the conflict clause therefore returns
/// `0`, which is the honest answer: nothing was written.
pub async fn append_workflow_logs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    lines: &[WorkflowLogLine],
    max_lines: u32,
) -> HarvestResult<usize> {
    use crate::schema::harvest_workflow_logs::dsl;

    if lines.is_empty() {
        return Ok(0);
    }

    // The marker latches the cap decision (see the doc comment): once it is
    // present, no previously unseen line is admitted, whatever `max_lines`
    // currently says. Checked BEFORE the count so a raised cap cannot re-open
    // the gate on a rolling deployment.
    let truncated: bool = diesel::select(diesel::dsl::exists(
        dsl::harvest_workflow_logs
            .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(dsl::seq.eq(WORKFLOW_LOG_TRUNCATION_SEQ)),
    ))
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    if truncated {
        return Ok(0);
    }

    // Count only real lines that this batch is NOT about to re-insert.
    //
    // Two exclusions, each load-bearing:
    //   * the marker, so its own presence can never push the count over the cap
    //     and re-trigger truncation accounting; and
    //   * this batch's own seqs (issue #790 review), because on the re-drive
    //     path the design depends on -- the same position re-minting the same
    //     seq -- those rows are ALREADY stored and `ON CONFLICT DO NOTHING`
    //     will collapse them. Counting them would charge the cycle's lines to
    //     the budget twice and fire the truncation marker on a run that
    //     dropped nothing, which is exactly the false alarm the marker exists
    //     to rule out.
    let batch_seqs: Vec<i64> = lines.iter().map(|line| line.seq).collect();
    let existing: i64 = dsl::harvest_workflow_logs
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::seq.ne(WORKFLOW_LOG_TRUNCATION_SEQ))
        .filter(dsl::seq.ne_all(batch_seqs))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    let remaining = i64::from(max_lines).saturating_sub(existing).max(0);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let admit = (remaining as usize).min(lines.len());

    // Keep the affected-row count (issue #790 review round 2): on the re-drive
    // path the conflict clause collapses rows that are already stored, so
    // `admit` counts what we OFFERED, not what landed.
    let inserted = if admit > 0 {
        let rows: Vec<crate::models::NewHarvestWorkflowLog> = lines[..admit]
            .iter()
            .map(|line| crate::models::NewHarvestWorkflowLog {
                workflow_exec_id: exec_id.as_uuid(),
                seq: line.seq,
                level: line.level.to_string(),
                message: line.message.clone(),
            })
            .collect();
        diesel::insert_into(dsl::harvest_workflow_logs)
            .values(&rows)
            .on_conflict((dsl::workflow_exec_id, dsl::seq))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?
    } else {
        0
    };

    // Anything we could not admit is dropped -- record the marker exactly once.
    //
    // This condition MUST stay on `admit`, never on `inserted`. A re-drive
    // offers rows that are already stored, so `inserted` is 0 while nothing was
    // dropped at all -- gating on it would stamp a truncation marker on every
    // re-driven cycle and tell an operator a healthy run had lost lines.
    if admit < lines.len() {
        diesel::insert_into(dsl::harvest_workflow_logs)
            .values(crate::models::NewHarvestWorkflowLog {
                workflow_exec_id: exec_id.as_uuid(),
                seq: WORKFLOW_LOG_TRUNCATION_SEQ,
                level: "warn".to_string(),
                message: WORKFLOW_LOG_TRUNCATION_MESSAGE.to_string(),
            })
            .on_conflict((dsl::workflow_exec_id, dsl::seq))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(inserted)
}

/// Read filters for one page of an execution's durable log lines (issue #790).
///
/// Mirrors the `ScheduleRunQuery` params-struct precedent so the HTTP layer has
/// one thing to build and the signature stays extensible.
#[derive(Debug, Clone, Default)]
pub struct WorkflowLogQuery<'a> {
    /// Exclusive keyset cursor: the previous page's last `seq`.
    pub after_seq: Option<i64>,
    /// Wall-clock lower bound on `occurred_at` (exclusive). Independent of
    /// `after_seq`; both may be set and are `AND`ed.
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Level allow-list. Empty means "all levels".
    pub levels: &'a [&'a str],
    /// Page size. The caller is responsible for clamping it.
    pub limit: i64,
}

/// Load one page of an execution's durable log lines, in emission order.
///
/// Ordering is by `seq`, the deterministic emission order -- **not** by
/// `occurred_at`, which is wall-clock and can be non-monotonic across workers.
/// `since` is offered as a convenience bound only; pagination must use
/// `after_seq`.
pub async fn load_workflow_logs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    query: &WorkflowLogQuery<'_>,
) -> HarvestResult<Vec<crate::models::HarvestWorkflowLog>> {
    use crate::schema::harvest_workflow_logs::dsl;

    let mut q = dsl::harvest_workflow_logs
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .into_boxed();
    if let Some(after) = query.after_seq {
        q = q.filter(dsl::seq.gt(after));
    }
    if let Some(since) = query.since {
        q = q.filter(dsl::occurred_at.gt(since));
    }
    if !query.levels.is_empty() {
        // The truncation marker is a `warn` row, so a `?level=warn` filter
        // naturally still surfaces it; other filters correctly hide it.
        q = q.filter(dsl::level.eq_any(query.levels.to_vec()));
    }
    q.order(dsl::seq.asc())
        .limit(query.limit)
        .select(crate::models::HarvestWorkflowLog::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Count an execution's **real** durable log lines, across all levels.
///
/// The synthetic truncation marker is excluded, matching the accounting in
/// [`append_workflow_logs`]: it is engine bookkeeping, not an author line, so
/// counting it would make a capped execution report `max_lines + 1` and break
/// the `total_lines <= max_lines` invariant exactly on the runs an operator is
/// most likely inspecting. Whether a run was truncated is reported separately,
/// by probing for the marker directly.
pub async fn count_workflow_logs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<i64> {
    use crate::schema::harvest_workflow_logs::dsl;

    dsl::harvest_workflow_logs
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::seq.ne(WORKFLOW_LOG_TRUNCATION_SEQ))
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Delete every durable log line for an execution (issue #790 × issue #495).
///
/// Called by the targeted PII-erasure path: an author's log messages are
/// free-form text that can carry personal data, so erasing an execution's
/// payloads must erase its logs too. Returns the number of rows removed.
pub async fn delete_workflow_logs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<usize> {
    use crate::schema::harvest_workflow_logs::dsl;

    diesel::delete(dsl::harvest_workflow_logs.filter(dsl::workflow_exec_id.eq(exec_id.as_uuid())))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Guarded exactly-once stamp for the operator early-warning soft threshold
/// on workflow history bloat (issue #704).
///
/// Called by the worker, post-commit (in autocommit), when a still-RUNNING
/// execution's recorded history event count has just crossed
/// `history_bloat_warn_fraction * event_hard_cap` for the first time.
///
/// The `WHERE history_bloat_warned_at IS NULL` guard makes this exactly-once
/// and race-safe: concurrent workers racing to persist the same execution's
/// next cycle (or repeated re-dispatch of an already-warned execution) can
/// never double-stamp the row. Returns `Ok(true)` iff THIS call performed the
/// transition (the row was NULL and is now stamped) so the caller emits the
/// counter exactly once; `Ok(false)` means it was already stamped (by an
/// earlier cycle or a racing worker) and the caller must not emit again.
pub async fn mark_history_bloat_warned(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
) -> crate::error::HarvestResult<bool> {
    use crate::schema::harvest_workflow_executions::dsl;

    let rows = diesel::update(
        dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .filter(dsl::history_bloat_warned_at.is_null()),
    )
    .set(dsl::history_bloat_warned_at.eq(diesel::dsl::now))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows > 0)
}

fn summarize_error(error: Option<String>) -> Option<String> {
    const MAX_ERROR_SUMMARY_CHARS: usize = 240;

    let first_line = error?.lines().next()?.trim().to_string();
    if first_line.is_empty() {
        return None;
    }

    Some(first_line.chars().take(MAX_ERROR_SUMMARY_CHARS).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;

    #[test]
    fn stored_event_has_sequential_event_id() {
        let exec_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "send_email".into(),
                input: serde_json::Value::Null,
                queue: "default".into(),
            },
        ];

        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event_id, 0);
        assert_eq!(rows[1].event_id, 1);
        assert_eq!(rows[0].event_type, "WorkflowStarted");
        assert_eq!(rows[1].event_type, "ActivityScheduled");
    }

    #[test]
    fn events_to_rows_serializes_json() {
        let exec_id = ExecutionId::new();
        let events = vec![WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"result": 42}),
        }];

        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        let data = &rows[0].event_data;
        // serde tagged enum with (tag = "type", content = "data") wraps in "data"
        assert!(
            data.get("data").is_some(),
            "serde adjacently-tagged enum should wrap payload in 'data' key, got: {data}"
        );
    }

    #[test]
    fn events_to_rows_preserves_event_type_name() {
        let exec_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::workflow_failed("boom"),
            WorkflowEvent::TimerFired {
                timer_id: crate::types::TimerId::new("t1"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approve".into(),
                payload: serde_json::json!(true),
            },
        ];

        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        for (row, event) in rows.iter().zip(events.iter()) {
            assert_eq!(
                row.event_type,
                event.type_name(),
                "event_type column must match WorkflowEvent::type_name()"
            );
        }
    }

    #[test]
    fn events_to_rows_from_applies_start_offset() {
        let exec_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::ActivityCompleted {
                activity_id: ActivityExecId::new(),
                output: serde_json::json!("ok"),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!(null),
            },
        ];

        let rows = events_to_insert_rows_from(exec_id, &events, 5).unwrap();
        assert_eq!(rows[0].event_id, 5);
        assert_eq!(rows[1].event_id, 6);
    }

    #[test]
    fn events_to_rows_sets_exec_id_on_every_row() {
        let exec_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::Value::Null,
            },
        ];

        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        for row in &rows {
            assert_eq!(row.workflow_exec_id, exec_id.as_uuid());
        }
    }

    #[test]
    fn empty_events_produce_empty_rows() {
        let exec_id = ExecutionId::new();
        let rows = events_to_insert_rows(exec_id, &[]);
        assert!(rows.unwrap().is_empty());
    }

    #[test]
    fn history_from_rows_deserializes_events() -> Result<(), serde_json::Error> {
        let exec_id = ExecutionId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({"user": "alice"}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "send_email".into(),
                input: serde_json::json!({"to": "bob@example.com"}),
                queue: "default".into(),
            },
            WorkflowEvent::WorkflowCompleted {
                output: serde_json::json!({"status": "ok"}),
            },
        ];

        // Serialize via the writer path
        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        assert_eq!(rows.len(), 3);

        // Deserialize each row's event_data back into WorkflowEvent
        let deserialized: Result<Vec<WorkflowEvent>, _> = rows
            .iter()
            .map(|row| serde_json::from_value(row.event_data.clone()))
            .collect();
        let deserialized = deserialized?;

        assert_eq!(deserialized.len(), 3);
        assert!(matches!(
            deserialized[0],
            WorkflowEvent::WorkflowStarted { .. }
        ));
        assert!(matches!(
            deserialized[1],
            WorkflowEvent::ActivityScheduled { .. }
        ));
        assert!(matches!(
            deserialized[2],
            WorkflowEvent::WorkflowCompleted { .. }
        ));

        // Verify data fidelity on WorkflowStarted
        if let WorkflowEvent::WorkflowStarted { ref input, .. } = deserialized[0] {
            assert_eq!(input, &serde_json::json!({"user": "alice"}));
        } else {
            panic!("expected WorkflowStarted");
        }

        // Verify data fidelity on ActivityScheduled
        if let WorkflowEvent::ActivityScheduled {
            ref name,
            ref queue,
            ..
        } = deserialized[1]
        {
            assert_eq!(name, "send_email");
            assert_eq!(queue, "default");
        } else {
            panic!("expected ActivityScheduled");
        }

        // Verify data fidelity on WorkflowCompleted
        if let WorkflowEvent::WorkflowCompleted { ref output } = deserialized[2] {
            assert_eq!(output, &serde_json::json!({"status": "ok"}));
        } else {
            panic!("expected WorkflowCompleted");
        }
        Ok(())
    }

    #[test]
    fn json_contains_type_tag() {
        let exec_id = ExecutionId::new();
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "checkpoint".into(),
            details: serde_json::json!({"step": 3}),
        }];

        let rows = events_to_insert_rows(exec_id, &events).unwrap();
        let data = &rows[0].event_data;
        // The "type" key comes from serde(tag = "type", content = "data")
        assert_eq!(
            data.get("type").and_then(serde_json::Value::as_str),
            Some("MarkerRecorded"),
            "serialized JSON must include the serde 'type' tag"
        );
    }
}
