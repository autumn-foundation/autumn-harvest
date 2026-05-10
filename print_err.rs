use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::event::WorkflowEvent;
use std::convert::TryFrom;
fn main() {
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

    let result = store::events_to_insert_rows_from(exec_id, &events, i32::MAX);
    println!("Err: {:?}", result.unwrap_err().to_string());
}
