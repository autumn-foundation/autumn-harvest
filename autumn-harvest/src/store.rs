//! Event store -- append-only persistence for workflow event histories.
//!
//! All writes go through [`append_events()`] which inserts atomically.
//! The `UNIQUE(workflow_exec_id, event_id)` constraint guarantees
//! that two workers can't append conflicting events to the same workflow.

use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::models::NewHarvestEvent;
use crate::schema::harvest_events;
use crate::types::ExecutionId;

/// Convert in-memory events to insertable rows with sequential event IDs
/// starting from 0.
///
/// This is a convenience wrapper around [`events_to_insert_rows_from`] for
/// fresh workflow executions where the history starts empty.
#[must_use]
pub fn events_to_insert_rows(
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
) -> Vec<NewHarvestEvent<'_>> {
    events_to_insert_rows_from(exec_id, events, 0)
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
#[must_use]
pub fn events_to_insert_rows_from(
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
    start_id: i32,
) -> Vec<NewHarvestEvent<'_>> {
    events
        .iter()
        .enumerate()
        .map(|(i, event)| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let event_id = start_id + i as i32;
            NewHarvestEvent {
                workflow_exec_id: exec_id.as_uuid(),
                event_id,
                event_type: event.type_name(),
                event_data: serde_json::to_value(event).expect("WorkflowEvent must serialize"),
            }
        })
        .collect()
}

/// Append events to a workflow's history in a single INSERT.
///
/// Returns the number of events inserted. Fails with a unique constraint
/// violation (wrapped as [`HarvestError::Database`]) if `start_id` conflicts --
/// this indicates a concurrency conflict where two workers tried to advance
/// the same workflow simultaneously.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the INSERT fails (e.g. unique
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

    let rows = events_to_insert_rows_from(exec_id, events, start_id);

    diesel::insert_into(harvest_events::table)
        .values(&rows)
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))
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

        let rows = events_to_insert_rows(exec_id, &events);
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

        let rows = events_to_insert_rows(exec_id, &events);
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

        let rows = events_to_insert_rows(exec_id, &events);
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

        let rows = events_to_insert_rows_from(exec_id, &events, 5);
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

        let rows = events_to_insert_rows(exec_id, &events);
        for row in &rows {
            assert_eq!(row.workflow_exec_id, exec_id.as_uuid());
        }
    }

    #[test]
    fn empty_events_produce_empty_rows() {
        let exec_id = ExecutionId::new();
        let rows = events_to_insert_rows(exec_id, &[]);
        assert!(rows.is_empty());
    }

    #[test]
    fn json_contains_type_tag() {
        let exec_id = ExecutionId::new();
        let events = vec![WorkflowEvent::MarkerRecorded {
            name: "checkpoint".into(),
            details: serde_json::json!({"step": 3}),
        }];

        let rows = events_to_insert_rows(exec_id, &events);
        let data = &rows[0].event_data;
        // The "type" key comes from serde(tag = "type", content = "data")
        assert_eq!(
            data.get("type").and_then(serde_json::Value::as_str),
            Some("MarkerRecorded"),
            "serialized JSON must include the serde 'type' tag"
        );
    }
}
