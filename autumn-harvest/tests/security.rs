#![cfg(feature = "db")]

use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::store::events_to_insert_rows_from;
use autumn_harvest::types::ExecutionId;

#[test]
fn test_integer_overflow_in_event_id_returns_error() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        },
    ];

    let exec_id = ExecutionId::new();

    let error = events_to_insert_rows_from(exec_id, &events, i32::MAX)
        .expect_err("event-id overflow should return an error");
    assert!(matches!(error, HarvestError::Database(message) if message == "Event ID overflow"));
}
