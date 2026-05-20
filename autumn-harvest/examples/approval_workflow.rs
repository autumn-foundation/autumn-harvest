//! Declarative approval workflow: `#[update]`, `#[query]`, `updates![]`, `queries![]`.

use autumn_harvest::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct Decision {
    pub approved: bool,
    pub reason: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
pub struct StatusResponse {
    pub pending: bool,
    pub approved: Option<bool>,
}

fn validate_decision(input: &serde_json::Value) -> Result<(), String> {
    if input
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .is_empty()
    {
        return Err("reason must not be empty".into());
    }
    Ok(())
}

// Declarative update handler — auto-registered before the workflow runs.
/// Approves or rejects the workflow.
///
/// # Errors
/// Returns an error if the reason is missing or invalid.
#[update(workflow = "approval_workflow", validator = validate_decision)]
#[allow(clippy::used_underscore_binding, clippy::unused_async)]
#[allow(unused_variables)]
pub async fn decide(ctx: &WorkflowContext, input: Decision) -> Result<(), String> {
    Ok(())
}

// Declarative query handler — auto-registered before the workflow runs.
/// Returns the current approval status.
///
/// # Errors
/// Never returns an error, but uses `Result` to satisfy the handler trait.
#[query(workflow = "approval_workflow")]
#[allow(clippy::used_underscore_binding, clippy::missing_const_for_fn)]
#[allow(unused_variables)]
pub fn approval_status(ctx: &WorkflowContext) -> Result<StatusResponse, String> {
    Ok(StatusResponse {
        pending: true,
        approved: None,
    })
}

/// The main workflow logic.
///
/// # Errors
/// Never returns an error in this example.
#[workflow]
#[allow(clippy::used_underscore_binding, clippy::unused_async)]
#[allow(unused_variables)]
pub async fn approval_workflow(
    ctx: &WorkflowContext,
    id: String,
) -> Result<StatusResponse, String> {
    // The worker injects declarative handlers before this fn runs on every replay.
    // Business logic (e.g. wait for the "decide" signal) would go here.
    Ok(StatusResponse {
        pending: false,
        approved: Some(true),
    })
}

fn main() {
    let _harvest = HarvestBuilder::new()
        .workflows(workflows![approval_workflow])
        .updates(updates![decide])
        .queries(queries![approval_status]);
}
