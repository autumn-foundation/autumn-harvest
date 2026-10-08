//! Adapters for the API of an older release.
//!
//! The smoke source must build against the current tree and the previous
//! release. Where their APIs differ, a cargo feature selects the old form.
//! Delete an adapter when no supported previous release needs it.

use autumn_harvest::{ExecutionId, StartWorkflowParams};
use serde_json::Value;

/// Build start params. Releases from 0.7 have `StartWorkflowParams::new`.
#[cfg(not(feature = "v0_6"))]
pub fn start_params<'a>(
    name: &'a str,
    id: &'a str,
    input: Value,
    queue: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams::new(name, id, ExecutionId::new(), input, queue)
}

/// Build start params. Release 0.6 has no constructor, so list each field.
#[cfg(feature = "v0_6")]
pub fn start_params<'a>(
    name: &'a str,
    id: &'a str,
    input: Value,
    queue: &'a str,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: name,
        workflow_id: id,
        exec_id: ExecutionId::new(),
        input,
        parent_id: None,
        queue_name: queue,
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: Default::default(),
        conflict_policy: Default::default(),
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: Default::default(),
        priority: Default::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: Default::default(),
        start_source_ref: None,
        started_by: None,
    }
}
