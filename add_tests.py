import re

with open("autumn-harvest/src/analyzer.rs", "r") as f:
    content = f.read()

tests_code = """
    #[test]
    fn sequential_activities_rule_flags_over_threshold() {
        let rule = SequentialActivitiesRule::new(3);

        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let id3 = ActivityExecId::new();

        let history = vec![
            WorkflowEvent::ActivityScheduled {
                activity_id: id1,
                name: "Task1".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id1,
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id2,
                name: "Task2".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id2,
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id3,
                name: "Task3".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id3,
                output: serde_json::Value::Null,
            },
        ];

        let warnings = rule.analyze(&history);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].rule_name, "SequentialActivities");
        assert!(warnings[0].message.contains("3 activities executed strictly sequentially"));
    }

    #[test]
    fn sequential_activities_rule_ignores_concurrent_activities() {
        let rule = SequentialActivitiesRule::new(3);

        let id1 = ActivityExecId::new();
        let id2 = ActivityExecId::new();
        let id3 = ActivityExecId::new();

        let history = vec![
            // A & B concurrent
            WorkflowEvent::ActivityScheduled {
                activity_id: id1,
                name: "Task1".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: id2,
                name: "Task2".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id1,
                output: serde_json::Value::Null,
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id2,
                output: serde_json::Value::Null,
            },
            // C sequential after A & B
            WorkflowEvent::ActivityScheduled {
                activity_id: id3,
                name: "Task3".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id3,
                output: serde_json::Value::Null,
            },
        ];

        let warnings = rule.analyze(&history);
        assert!(warnings.is_empty(), "Concurrency should reset sequential count");
    }
"""

content = content.replace("mod tests {\n    use super::*;\n    use crate::types::{ActivityExecId, TimerId};\n    use chrono::Utc;", "mod tests {\n    use super::*;\n    use crate::types::{ActivityExecId, TimerId};\n    use chrono::Utc;\n\n" + tests_code)

with open("autumn-harvest/src/analyzer.rs", "w") as f:
    f.write(content)
