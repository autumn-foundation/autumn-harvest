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
    use crate::schema::harvest_signals;
    use crate::schema::harvest_workflow_executions;

    // ADR-0001 §2.5: harvest.signal.send — PRODUCER, parent = active span at call site.
    // Emitted synchronously before the DB await so EnteredSpan (!Send) is dropped
    // before the async transaction boundary.
    tracing::info_span!(
        "harvest.signal.send",
        "otel.kind" = "producer",
        { ATTR_WORKFLOW_ID } = %exec_id,
        { ATTR_EXECUTION_ID } = %exec_id,
        signal.name = %signal_name,
    )
    .in_scope(|| {});

    conn.transaction::<(), HarvestError, _>(|conn| {
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
                "RUNNING" => {}
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

            let row = NewHarvestSignal {
                workflow_exec_id: exec_id.as_uuid(),
                signal_name,
                payload,
            };

            diesel::insert_into(harvest_signals::table)
                .values(&row)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            crate::queue::wake_workflow_task(conn, exec_id).await
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
