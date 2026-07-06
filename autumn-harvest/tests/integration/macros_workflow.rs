#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;

#[workflow]
async fn test_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow(execution_timeout = "24h")]
async fn billing_reconciliation(_ctx: &WorkflowContext, _run_date: String) -> Result<(), String> {
    Ok(())
}

#[workflow(execution_timeout = "30m")]
async fn quick_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(concurrency(key = "input.tenant_id", limit = 10))]
async fn concurrency_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[test]
fn workflow_companion_exists_and_returns_info() {
    let info = __autumn_workflow_info_test_workflow();
    assert_eq!(info.name, "test_workflow");
    assert!(
        info.execution_timeout.is_none(),
        "no execution_timeout attribute → None"
    );
    assert!(
        info.concurrency.is_none(),
        "plain workflow should have no concurrency policy"
    );
}

#[test]
fn workflow_execution_timeout_attribute_24h() {
    let info = __autumn_workflow_info_billing_reconciliation();
    assert_eq!(info.name, "billing_reconciliation");
    let timeout = info
        .execution_timeout
        .expect("execution_timeout = '24h' must produce Some(...)");
    assert_eq!(
        timeout,
        std::time::Duration::from_secs(86_400),
        "24h = 86400 seconds"
    );
}

#[test]
fn workflow_execution_timeout_attribute_30m() {
    let info = __autumn_workflow_info_quick_workflow();
    assert_eq!(info.name, "quick_workflow");
    let timeout = info
        .execution_timeout
        .expect("execution_timeout = '30m' must produce Some(...)");
    assert_eq!(
        timeout,
        std::time::Duration::from_secs(1_800),
        "30m = 1800 seconds"
    );
}

#[test]
fn workflow_concurrency_macro_sets_policy() {
    let info = __autumn_workflow_info_concurrency_workflow();
    assert_eq!(info.name, "concurrency_workflow");
    let policy = info
        .concurrency
        .expect("concurrency workflow must have a policy");
    assert_eq!(policy.key_expr, "input.tenant_id");
    assert_eq!(policy.limit, 10);
}

#[workflow(
    owner = "billing",
    runbook = "https://wiki.acme.com/runbook",
    severity = "sev2"
)]
async fn metadata_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_metadata_attributes() {
    let info = __autumn_workflow_info_metadata_workflow();
    assert_eq!(info.owner, Some("billing"));
    assert_eq!(info.runbook_url, Some("https://wiki.acme.com/runbook"));
    assert_eq!(info.severity, Some("sev2"));
}

// ── MCP tool exposure opt-in (issue #597) ─────────────────────────────────────

#[workflow(mcp)]
async fn mcp_bare_workflow(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok("done".into())
}

#[workflow(mcp = true)]
async fn mcp_eq_true_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp = false)]
async fn mcp_eq_false_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[workflow(mcp, description = "an MCP-exposed workflow")]
async fn mcp_with_description_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

#[test]
fn workflow_mcp_bare_flag_sets_true() {
    let info = __autumn_workflow_info_mcp_bare_workflow();
    assert!(
        info.mcp,
        "#[workflow(mcp)] must set WorkflowInfo.mcp = true"
    );
}

#[test]
fn workflow_mcp_eq_true_sets_true() {
    let info = __autumn_workflow_info_mcp_eq_true_workflow();
    assert!(info.mcp, "#[workflow(mcp = true)] must set mcp = true");
}

#[test]
fn workflow_mcp_eq_false_sets_false() {
    let info = __autumn_workflow_info_mcp_eq_false_workflow();
    assert!(!info.mcp, "#[workflow(mcp = false)] must set mcp = false");
}

#[test]
fn workflow_mcp_defaults_to_false() {
    let info = __autumn_workflow_info_test_workflow();
    assert!(
        !info.mcp,
        "workflows without the mcp attribute must default to mcp = false"
    );
}

#[test]
fn workflow_mcp_composes_with_other_attributes() {
    let info = __autumn_workflow_info_mcp_with_description_workflow();
    assert!(info.mcp);
    assert_eq!(info.description, Some("an MCP-exposed workflow"));
}
