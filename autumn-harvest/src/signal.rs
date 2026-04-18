#[cfg(feature = "db")]
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::HarvestResult;
#[cfg(feature = "db")]
use crate::models::{HarvestSignal, NewHarvestSignal};
use crate::types::ExecutionId;

/// Queue a workflow signal for durable delivery and wake the parked workflow.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_signals;

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
