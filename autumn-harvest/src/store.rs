//! Event store -- append-only persistence for workflow event histories.
//!
//! All writes go through [`append_events()`] which inserts atomically.
//! The `UNIQUE(workflow_exec_id, event_id)` constraint guarantees
//! that two workers can't append conflicting events to the same workflow.

use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use scoped_futures::ScopedFutureExt as _;

use crate::error::HarvestResult;
use crate::event::WorkflowEvent;
use crate::models::NewHarvestEvent;
use crate::schema::{harvest_events, harvest_workflow_executions};
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
}

type WorkflowChildProjection = (
    uuid::Uuid,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    Option<String>,
    i32,
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

    let rows = events_to_insert_rows_from(exec_id, events, start_id)?;

    diesel::insert_into(harvest_events::table)
        .values(&rows)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
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
pub async fn admit_update_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    update_id: crate::types::UpdateId,
    name: String,
    input: serde_json::Value,
) -> HarvestResult<()> {
    use crate::models::WorkflowExecution;
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::max;

    conn.transaction::<(), crate::error::HarvestError, _>(|conn| {
        async move {
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
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

/// Load the full event history for a workflow execution, ordered by `event_id`.
///
/// Deserializes each row's `event_data` JSON back into [`WorkflowEvent`].
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

    query
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::state,
            harvest_workflow_executions::started_at,
            harvest_workflow_executions::completed_at,
            harvest_workflow_executions::error,
            harvest_workflow_executions::shard_id,
        ))
        .load::<WorkflowChildProjection>(conn)
        .await
        .map_err(crate::error::database_error)
        .map(|rows| {
            rows.into_iter()
                .map(
                    |(id, workflow_name, state, started_at, completed_at, error, shard_id)| {
                        WorkflowChildRow {
                            exec_id: ExecutionId::from_uuid(id),
                            workflow_name,
                            status: state,
                            started_at,
                            completed_at,
                            error_summary: summarize_error(error),
                            shard_id,
                            depth,
                        }
                    },
                )
                .collect()
        })
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
            WorkflowEvent::WorkflowFailed {
                error: "boom".into(),
            },
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
