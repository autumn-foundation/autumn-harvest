//! Tests for within-queue task priority (issue #249).
//!
//! RED-phase tests — all expected to fail until the feature is implemented.

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::types::Priority;

// ---------------------------------------------------------------------------
// Priority enum: basic shape
// ---------------------------------------------------------------------------

#[test]
fn priority_has_four_variants() {
    let _ = Priority::Low;
    let _ = Priority::Normal;
    let _ = Priority::High;
    let _ = Priority::Critical;
}

#[test]
fn priority_default_is_normal() {
    assert_eq!(Priority::default(), Priority::Normal);
}

#[test]
fn priority_as_i32_ordering() {
    // Critical > High > Normal > Low (DESC ordering means higher int = higher priority)
    assert!(Priority::Critical.as_i32() > Priority::High.as_i32());
    assert!(Priority::High.as_i32() > Priority::Normal.as_i32());
    assert!(Priority::Normal.as_i32() > Priority::Low.as_i32());
}

#[test]
fn priority_normal_is_zero() {
    // Normal must map to 0 so pre-upgrade rows (priority = 0) are treated as Normal.
    assert_eq!(Priority::Normal.as_i32(), 0);
}

#[test]
fn priority_from_i32_round_trips() {
    for p in [
        Priority::Low,
        Priority::Normal,
        Priority::High,
        Priority::Critical,
    ] {
        let back = Priority::from_i32(p.as_i32()).expect("round trip must succeed");
        assert_eq!(back, p);
    }
}

#[test]
fn priority_from_i32_returns_none_for_unknown() {
    assert!(Priority::from_i32(999).is_none());
    assert!(Priority::from_i32(-999).is_none());
}

// ---------------------------------------------------------------------------
// Priority: serde
// ---------------------------------------------------------------------------

#[test]
fn priority_serializes_as_lowercase_string() {
    assert_eq!(
        serde_json::to_string(&Priority::Normal).unwrap(),
        "\"normal\""
    );
    assert_eq!(serde_json::to_string(&Priority::High).unwrap(), "\"high\"");
    assert_eq!(serde_json::to_string(&Priority::Low).unwrap(), "\"low\"");
    assert_eq!(
        serde_json::to_string(&Priority::Critical).unwrap(),
        "\"critical\""
    );
}

#[test]
fn priority_deserializes_from_lowercase_string() {
    assert_eq!(
        serde_json::from_str::<Priority>("\"normal\"").unwrap(),
        Priority::Normal
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"high\"").unwrap(),
        Priority::High
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"low\"").unwrap(),
        Priority::Low
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"critical\"").unwrap(),
        Priority::Critical
    );
}

#[test]
fn priority_deserializes_case_insensitively() {
    assert_eq!(
        serde_json::from_str::<Priority>("\"NORMAL\"").unwrap(),
        Priority::Normal
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"HIGH\"").unwrap(),
        Priority::High
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"Critical\"").unwrap(),
        Priority::Critical
    );
    assert_eq!(
        serde_json::from_str::<Priority>("\"LOW\"").unwrap(),
        Priority::Low
    );
}

#[test]
fn priority_display_is_lowercase() {
    assert_eq!(Priority::Normal.to_string(), "normal");
    assert_eq!(Priority::High.to_string(), "high");
    assert_eq!(Priority::Low.to_string(), "low");
    assert_eq!(Priority::Critical.to_string(), "critical");
}

// ---------------------------------------------------------------------------
// EnqueueParams: priority API (db feature only)
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
#[test]
fn enqueue_params_default_priority_is_normal() {
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    let params = EnqueueParams::new("q", TaskType::Activity, serde_json::json!(null));
    assert_eq!(params.priority, Priority::Normal.as_i32());
}

#[cfg(feature = "db")]
#[test]
fn enqueue_params_with_priority_builder() {
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    let params = EnqueueParams::new("q", TaskType::Activity, serde_json::json!(null))
        .with_priority(Priority::High);
    assert_eq!(params.priority, Priority::High.as_i32());
}

#[cfg(feature = "db")]
#[test]
fn enqueue_params_with_priority_critical() {
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    let params = EnqueueParams::new("q", TaskType::Workflow, serde_json::json!(null))
        .with_priority(Priority::Critical);
    assert_eq!(params.priority, Priority::Critical.as_i32());
}

#[cfg(feature = "db")]
#[test]
fn enqueue_params_with_priority_low() {
    use autumn_harvest::queue::{EnqueueParams, TaskType};
    let params = EnqueueParams::new("q", TaskType::Activity, serde_json::json!(null))
        .with_priority(Priority::Low);
    assert_eq!(params.priority, Priority::Low.as_i32());
}

// ---------------------------------------------------------------------------
// WorkerConfig: priority aging
// ---------------------------------------------------------------------------

#[test]
fn worker_config_default_has_no_priority_aging() {
    let cfg = WorkerConfig::default();
    assert!(cfg.priority_aging_secs.is_none());
}

#[test]
fn worker_config_with_priority_aging() {
    let cfg = WorkerConfig::default().with_priority_aging_secs(300);
    assert_eq!(cfg.priority_aging_secs, Some(300));
}

#[test]
fn worker_config_priority_aging_zero_disables() {
    // Setting 0 seconds is equivalent to no aging (would divide by zero in SQL).
    // The builder should normalize this to None.
    let cfg = WorkerConfig::default().with_priority_aging_secs(0);
    assert!(cfg.priority_aging_secs.is_none());
}

// ---------------------------------------------------------------------------
// Priority: parse from str (for management API body parsing)
// ---------------------------------------------------------------------------

#[test]
fn priority_parses_from_str() {
    use std::str::FromStr;
    assert_eq!(Priority::from_str("normal").unwrap(), Priority::Normal);
    assert_eq!(Priority::from_str("high").unwrap(), Priority::High);
    assert_eq!(Priority::from_str("low").unwrap(), Priority::Low);
    assert_eq!(Priority::from_str("critical").unwrap(), Priority::Critical);
}

#[test]
fn priority_from_str_is_case_insensitive() {
    use std::str::FromStr;
    assert_eq!(Priority::from_str("NORMAL").unwrap(), Priority::Normal);
    assert_eq!(Priority::from_str("HIGH").unwrap(), Priority::High);
    assert_eq!(Priority::from_str("Low").unwrap(), Priority::Low);
    assert_eq!(Priority::from_str("CRITICAL").unwrap(), Priority::Critical);
}

#[test]
fn priority_from_str_unknown_is_error() {
    use std::str::FromStr;
    assert!(Priority::from_str("urgent").is_err());
    assert!(Priority::from_str("").is_err());
}

// ---------------------------------------------------------------------------
// StartWorkflowParams: priority field
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
#[test]
fn start_workflow_params_has_priority_field() {
    use autumn_harvest::execution::StartWorkflowParams;
    use autumn_harvest::types::{ExecutionId, WorkflowIdReusePolicy};

    let params = StartWorkflowParams {
        workflow_name: "test",
        workflow_id: "wf-1",
        exec_id: ExecutionId::new(),
        input: serde_json::json!(null),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: Priority::High,
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,
    };

    assert_eq!(params.priority, Priority::High);
}

#[cfg(feature = "db")]
#[test]
fn start_workflow_params_default_priority_is_normal() {
    use autumn_harvest::execution::StartWorkflowParams;
    use autumn_harvest::types::{ExecutionId, WorkflowIdReusePolicy};

    let params = StartWorkflowParams {
        workflow_name: "test",
        workflow_id: "wf-1",
        exec_id: ExecutionId::new(),
        input: serde_json::json!(null),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,
    };

    assert_eq!(params.priority, Priority::Normal);
}
