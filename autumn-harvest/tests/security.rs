use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::store::events_to_insert_rows_from;
use autumn_harvest::types::ExecutionId;

#[test]
#[should_panic(expected = "Event ID overflow")]
fn test_integer_overflow_in_event_id() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: chrono::Utc::now(),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::Value::Null,
        },
    ];

    let exec_id = ExecutionId::new();

    let _rows = events_to_insert_rows_from(exec_id, &events, i32::MAX);
}
