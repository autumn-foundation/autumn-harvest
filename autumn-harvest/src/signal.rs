//! Workflow signal delivery and management.
//!
//! Signals provide a way to send asynchronous events or payloads into a running workflow.
//! This module handles the durable enqueuing of signals into the database, loading pending
//! signals for a workflow, and marking them as consumed once processed by the workflow context.
#[cfg(feature = "db")]
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
#[cfg(feature = "db")]
use scoped_futures::ScopedFutureExt;

use crate::error::{HarvestError, HarvestResult};
#[cfg(feature = "db")]
use crate::models::{HarvestSignal, NewHarvestSignal};
#[cfg(feature = "db")]
use crate::telemetry::{ATTR_EXECUTION_ID, ATTR_WORKFLOW_ID};
use crate::types::ExecutionId;

/// Queue a workflow signal for durable delivery and wake the parked workflow.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist,
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal, and
/// [`HarvestError::Database`](crate::error::HarvestError::Database) if the
/// insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
) -> HarvestResult<()> {
    // Thin wrapper preserving the pre-#521 at-least-once contract: with no
    // idempotency key the partial unique index excludes the NULL row, so every
    // insert succeeds. The delivered/deduped bool is discarded.
    send_signal_idempotent(conn, exec_id, signal_name, payload, None)
        .await
        .map(|_delivered| ())
}

/// Queue a workflow signal with optional exactly-once delivery (issue #521).
///
/// When `idempotency_key` is `Some`, the insert is arbitrated against the
/// partial unique index `uq_harvest_signals_idem` on
/// `harvest_signals (workflow_exec_id, idempotency_key) WHERE idempotency_key
/// IS NOT NULL`. A second call carrying the same key for the same execution is
/// a no-op: no row is queued and the parked workflow is **not** re-woken.
///
/// Returns `Ok(true)` when a signal row was freshly queued (the workflow was
/// woken), and `Ok(false)` when the key collided with an already-staged signal
/// (deduplicated — the equivalent signal already landed once). With
/// `idempotency_key = None` every insert succeeds, so the return is always
/// `Ok(true)` — byte-for-byte the legacy [`send_signal`] behavior.
///
/// Dedupe scope is shard-local and keyed on `(workflow_exec_id,
/// idempotency_key)`, matching `signal_with_start_workflow_execution` (#244).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist,
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal, and
/// [`HarvestError::Database`](crate::error::HarvestError::Database) if the
/// insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal_idempotent(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<bool> {
    use crate::schema::harvest_signals;
    use crate::schema::harvest_workflow_executions;

    conn.transaction::<bool, HarvestError, _>(|conn| {
        async move {
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .for_update()
                .select(crate::models::WorkflowExecution::as_select())
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            match execution.state.as_str() {
                // PAUSED is a non-terminal active state (issue #383): a paused
                // workflow waiting on a signal must still accept (buffer) it so
                // it is delivered on resume. The wake below re-pends the task,
                // which the claim gate defers until the execution is RUNNING.
                "RUNNING" | "PAUSED" => {}
                "CANCELLED" => {
                    return Err(HarvestError::Cancelled(execution.error.unwrap_or_else(
                        || format!("workflow execution {exec_id} is cancelled"),
                    )));
                }
                state => {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is terminal ({state})"
                    )));
                }
            }

            // ADR-0001 §2.5: harvest.signal.send — PRODUCER, emitted only after
            // confirming RUNNING state so the span is not created for rejected signals.
            // in_scope is synchronous so EnteredSpan (!Send) is dropped before any await.
            tracing::info_span!(
                "harvest.signal.send",
                "otel.kind" = "producer",
                { ATTR_WORKFLOW_ID } = execution.workflow_name.as_str(),
                { ATTR_EXECUTION_ID } = %exec_id,
                signal.name = %signal_name,
            )
            .in_scope(|| {});

            let row = NewHarvestSignal {
                workflow_exec_id: exec_id.as_uuid(),
                signal_name,
                payload,
                idempotency_key,
            };

            // `on_conflict_do_nothing()` (no explicit target) lets Postgres
            // arbitrate against the partial unique index `uq_harvest_signals_idem`.
            // For a NULL key the index excludes the row, so the insert always
            // succeeds (rows-affected = 1) — identical to the legacy path.
            let inserted = diesel::insert_into(harvest_signals::table)
                .values(&row)
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            if inserted == 0 {
                // Idempotency-key collision: an equivalent signal is already
                // queued. Do not re-wake — the original insert already did.
                return Ok(false);
            }

            crate::queue::wake_workflow_task(conn, exec_id).await?;
            Ok(true)
        }
        .scope_boxed()
    })
    .await
}

/// Load all unconsumed queued signals for an execution, ordered by receive time.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the query fails.
#[cfg(feature = "db")]
pub async fn load_pending_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<HarvestSignal>> {
    use crate::schema::harvest_signals::dsl;

    dsl::harvest_signals
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::consumed.eq(false))
        .order((dsl::received_at.asc(), dsl::id.asc()))
        .select(HarvestSignal::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Mark the provided signal IDs consumed.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the update fails.
#[cfg(feature = "db")]
pub async fn mark_signals_consumed(
    conn: &mut AsyncPgConnection,
    signal_ids: &[uuid::Uuid],
) -> HarvestResult<()> {
    use crate::schema::harvest_signals::dsl;

    if signal_ids.is_empty() {
        return Ok(());
    }

    diesel::update(dsl::harvest_signals.filter(dsl::id.eq_any(signal_ids)))
        .set(dsl::consumed.eq(true))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}
