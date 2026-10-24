//! The Postgres side: the load path of a cold decision.
//!
//! A cold decision reads the history with `store::load_history_inflated`,
//! then reads it again with the stored byte sum (`worker.rs`,
//! `load_workflow_replay_state`). [`load`] times one such read.

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::store::{self, EventHistory};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::{StartWorkflowParams, WorkflowEvent, start_or_load_workflow_execution};
use diesel::sql_types::{Array, Uuid as SqlUuid};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::BoxError;

/// The workflow name of each seeded run. No worker runs it.
pub const WORKFLOW_NAME: &str = "history_load_gate";

/// Events per append. One insert binds four values per row, so this stays
/// far below the Postgres limit of 65,535 binds.
const APPEND_CHUNK: usize = 2_000;

/// Starts one run with `events[0]` and appends the rest. Returns its id.
///
/// The start goes through `start_or_load_workflow_execution` and the
/// appends through `store::append_events`, so the rows have the stored
/// form of a real run. The run waits on `queue`, where no worker polls.
///
/// # Errors
///
/// Returns an error when `events` does not start with `WorkflowStarted`,
/// or when the start or an append fails.
pub async fn seed(
    conn: &mut AsyncPgConnection,
    events: &[WorkflowEvent],
    queue: &str,
) -> Result<ExecutionId, BoxError> {
    let Some(WorkflowEvent::WorkflowStarted { input, .. }) = events.first() else {
        return Err("a seed history must start with WorkflowStarted".into());
    };
    let exec_id = ExecutionId::new();
    let workflow_id = format!("hlg-{exec_id}");
    let params =
        StartWorkflowParams::new(WORKFLOW_NAME, &workflow_id, exec_id, input.clone(), queue);
    start_or_load_workflow_execution(conn, params, None).await?;
    for (index, chunk) in events[1..].chunks(APPEND_CHUNK).enumerate() {
        let start_id = i32::try_from(1 + index * APPEND_CHUNK)?;
        store::append_events(conn, exec_id, chunk, start_id).await?;
    }
    Ok(exec_id)
}

/// Copies the run `template` and its events into `count` new runs.
///
/// The copy runs in SQL, so a seed of 100k runs takes seconds. Each copy
/// gets a new id, run id and workflow id. The events go in run by run, as
/// on the `SQLite` side, so both stores keep the rows of one run together.
/// A busy fleet scatters them. Each copied event takes the default
/// `cohort`. That fits the flat layout only. Returns the new ids.
///
/// # Errors
///
/// Returns an error when an insert fails.
pub async fn clone_runs(
    conn: &mut AsyncPgConnection,
    template: ExecutionId,
    count: usize,
) -> Result<Vec<ExecutionId>, BoxError> {
    let ids: Vec<ExecutionId> = (0..count).map(|_| ExecutionId::new()).collect();
    let uuids: Vec<uuid::Uuid> = ids.iter().map(ExecutionId::as_uuid).collect();
    // `jsonb_populate_record` copies every column, so a new column needs no
    // change here.
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         SELECT (jsonb_populate_record(NULL::harvest_workflow_executions, \
                   to_jsonb(e) || jsonb_build_object( \
                     'id', c.id, 'run_id', c.id, 'workflow_id', 'hlg-' || c.id::text))).* \
         FROM harvest_workflow_executions e CROSS JOIN unnest($2::uuid[]) AS c(id) \
         WHERE e.id = $1",
    )
    .bind::<SqlUuid, _>(template.as_uuid())
    .bind::<Array<SqlUuid>, _>(&uuids)
    .execute(conn)
    .await?;
    diesel::sql_query(
        "INSERT INTO harvest_events \
           (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         SELECT c.id, ev.event_id, ev.event_type, ev.event_data, ev.timestamp \
         FROM harvest_events ev CROSS JOIN unnest($2::uuid[]) AS c(id) \
         WHERE ev.workflow_exec_id = $1 \
         ORDER BY c.id, ev.event_id",
    )
    .bind::<SqlUuid, _>(template.as_uuid())
    .bind::<Array<SqlUuid>, _>(&uuids)
    .execute(conn)
    .await?;
    Ok(ids)
}

/// Loads the history of `exec_id` as one cold read of the worker does.
///
/// # Errors
///
/// Returns an error when the query fails or a row does not decode.
pub async fn load(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<EventHistory, BoxError> {
    let codecs = PayloadCodecs::default();
    Ok(store::load_history_inflated(conn, exec_id, &codecs, None).await?)
}

/// Deletes the runs `ids`. Their events go with them.
///
/// # Errors
///
/// Returns an error when the delete fails.
pub async fn delete_runs(
    conn: &mut AsyncPgConnection,
    ids: &[ExecutionId],
) -> Result<(), BoxError> {
    let uuids: Vec<uuid::Uuid> = ids.iter().map(ExecutionId::as_uuid).collect();
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = ANY($1)")
        .bind::<Array<SqlUuid>, _>(&uuids)
        .execute(conn)
        .await?;
    Ok(())
}
