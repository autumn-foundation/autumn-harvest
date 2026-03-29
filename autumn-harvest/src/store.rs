//! Event store -- append-only persistence for workflow event histories.
//!
//! All writes go through [`append_events()`] which inserts atomically.
//! The `UNIQUE(workflow_exec_id, event_id)` constraint guarantees
//! that two workers can't append conflicting events to the same workflow.

use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::models::NewHarvestEvent;
use crate::schema::harvest_events;
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

/// Load the full event history for a workflow execution, ordered by `event_id`.
///
/// Deserializes each row's `event_data` JSON back into [`WorkflowEvent`].
/// The returned [`EventHistory::next_event_id`] is set to one past the last
/// loaded event (or 0 if the history is empty), ready for use with
/// [`append_events()`].
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on connection or query errors, or
/// [`HarvestError::Serialization`] if a stored JSON value can't be deserialized
/// into `WorkflowEvent`.
pub async fn load_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<EventHistory> {
    use crate::models::HarvestEvent;

    let rows: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    let mut events = Vec::with_capacity(rows.len());
    for row in &rows {
        let event: WorkflowEvent = serde_json::from_value(row.event_data.clone())?;
        events.push(event);
    }

    let next_event_id = rows.last().map_or(0, |r| r.event_id.saturating_add(1));

    Ok(EventHistory {
        exec_id,
        events,
        next_event_id,
    })
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
    fn history_from_rows_deserializes_events() {
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
        let rows = events_to_insert_rows(exec_id, &events);
        assert_eq!(rows.len(), 3);

        // Deserialize each row's event_data back into WorkflowEvent
        let deserialized: Vec<WorkflowEvent> = rows
            .iter()
            .map(|row| serde_json::from_value(row.event_data.clone()).unwrap())
            .collect();

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
