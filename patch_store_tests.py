import sys

with open("./autumn-harvest/src/store.rs", "r") as f:
    content = f.read()

search_str = """        let rows = events_to_insert_rows_from(exec_id, &events, 5).unwrap();
        assert_eq!(rows[0].event_id, 5);
        assert_eq!(rows[1].event_id, 6);
    }

    #[test]
    fn events_to_rows_sets_exec_id_on_every_row() {"""

replace_str = """        let rows = events_to_insert_rows_from(exec_id, &events, 5).unwrap();
        assert_eq!(rows[0].event_id, 5);
        assert_eq!(rows[1].event_id, 6);
    }

    #[test]
    fn events_to_insert_rows_from_handles_max_event_id() {
        use crate::types::{ActivityExecId, ExecutionId};
        use crate::event::WorkflowEvent;
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

        let result = events_to_insert_rows_from(exec_id, &events, i32::MAX);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("event ID overflow"));
    }

    #[test]
    fn events_to_rows_sets_exec_id_on_every_row() {"""

content = content.replace(search_str, replace_str)

search_str2 = """            let event_id = start_id.checked_add(i_i32).ok_or_else(|| {
                crate::error::database_error(format!(
                    "event ID overflow while appending {i_i32} events to {exec_id} (start: {start_id})"
                ))
            })?;"""

with open("./autumn-harvest/src/store.rs", "w") as f:
    f.write(content)
