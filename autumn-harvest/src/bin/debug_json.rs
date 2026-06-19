//! A diagnostic tool for printing JSON representations of workflow events.
//!
//! When developing clients in other languages (or testing the raw REST API),
//! you often need exact JSON payloads for the `WorkflowEvent` variants. This
//! binary generates properly structured and `serde`-serialized JSON output
//! for core event types so you don't have to guess the field names.
//!
//! ## Examples
//!
//! Run the tool to see the serialized output for external signals:
//!
//! ```bash
//! cargo run -p autumn-harvest --bin debug_json
//! ```

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ExternalSignalId;

fn main() {
    let signal_id = ExternalSignalId::new();
    let event = WorkflowEvent::ExternalSignalDelivered { signal_id };
    let json_str = serde_json::to_string_pretty(&event).unwrap();
    println!("ExternalSignalDelivered JSON:");
    println!("{json_str}");

    let event_req = WorkflowEvent::ExternalSignalRequested {
        signal_id,
        target: autumn_harvest::types::ExecutionId::new(),
        signal_name: "test".to_string(),
        payload: serde_json::json!({"a": 1}),
    };
    let json_req = serde_json::to_string_pretty(&event_req).unwrap();
    println!("ExternalSignalRequested JSON:");
    println!("{json_req}");
}
