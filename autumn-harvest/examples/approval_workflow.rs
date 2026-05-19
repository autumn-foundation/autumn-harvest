//! Declarative approval workflow: `#[update]`, `#[query]`, `updates![]`, `queries![]`.

#![allow(clippy::used_underscore_binding)]

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
#[update(workflow = "approval_workflow", validator = validate_decision)]
#[allow(
    clippy::unused_async,
    clippy::used_underscore_binding,
    clippy::missing_errors_doc
)]
pub async fn decide(_ctx: &WorkflowContext, _input: Decision) -> Result<(), String> {
    Ok(())
}

// Declarative query handler — auto-registered before the workflow runs.
#[query(workflow = "approval_workflow")]
#[allow(clippy::missing_const_for_fn, clippy::missing_errors_doc)]
pub fn approval_status(_ctx: &WorkflowContext) -> Result<StatusResponse, String> {
    Ok(StatusResponse {
        pending: true,
        approved: None,
    })
}

#[workflow]
#[allow(
    clippy::unused_async,
    clippy::used_underscore_binding,
    clippy::missing_errors_doc
)]
pub async fn approval_workflow(
    _ctx: &WorkflowContext,
    _id: String,
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
