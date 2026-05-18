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

#[test]
fn workflow_companion_exists_and_returns_info() {
    let info = __autumn_workflow_info_test_workflow();
    assert_eq!(info.name, "test_workflow");
    assert!(
        info.execution_timeout.is_none(),
        "no execution_timeout attribute → None"
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
