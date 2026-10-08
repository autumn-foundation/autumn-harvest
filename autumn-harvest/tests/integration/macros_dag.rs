#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;

#[activity]
async fn extract_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[activity]
async fn load_users(_ctx: &ActivityContext) -> Result<(), String> {
    Ok(())
}

#[dag(
    schedule = "0 2 * * *",
    catchup = false,
    max_active_runs = 1,
    default_queue = "etl-workers"
)]
fn daily_etl(dag: &mut DagBuilder) {
    let extract = dag
        .activity(extract_users)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));
    let _load = dag
        .activity(load_users)
        .upstream(&extract)
        .trigger_rule(TriggerRule::AllDone);
}

#[test]
fn dag_companion_returns_metadata() {
    let info = __autumn_dag_info_daily_etl();
    assert_eq!(info.name, "daily_etl");
    assert_eq!(info.default_queue, Some("etl-workers"));
    assert_eq!(info.max_active_runs, 1);
    assert!(!info.catchup);
    assert!(matches!(info.schedule, Some(Schedule::Cron(ref expr)) if expr == "0 2 * * *"));
}

/// `unified-dag-execution` is a default feature (Step 4 of issue #256).
/// The `#[dag]` macro must therefore populate `workflow_handler` so every
/// registered DAG can execute on the unified path without any opt-in flag.
#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_populates_workflow_handler_in_default_build() {
    let info = __autumn_dag_info_daily_etl();
    assert!(
        info.workflow_handler.is_some(),
        "workflow_handler must be Some when unified-dag-execution is in the default feature set"
    );
}

#[dag(
    schedule = "0 * * * *",
    catchup = false,
    max_active_runs = 1,
    jitter = "5m"
)]
fn hourly_report(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_jitter_attribute_populates_field() {
    let info = __autumn_dag_info_hourly_report();
    assert_eq!(
        info.jitter,
        Duration::from_secs(300),
        "jitter should be 5 minutes"
    );
    assert_eq!(info.name, "hourly_report");
}

#[test]
fn dag_macro_default_jitter_is_zero() {
    let info = __autumn_dag_info_daily_etl();
    assert_eq!(info.jitter, Duration::ZERO, "jitter must default to zero");
}

#[test]
fn dags_macro_collects_and_builds_definitions() {
    let dags: Vec<DagInfo> = dags![daily_etl];
    assert_eq!(dags.len(), 1);

    let definition = dags[0].build_definition().expect("dag should compile");
    assert_eq!(definition.tasks().len(), 2);
    assert_eq!(definition.execution_levels().len(), 2);
    assert_eq!(definition.tasks()[0].queue.as_deref(), Some("etl-workers"));
    assert_eq!(definition.tasks()[1].trigger_rule, TriggerRule::AllDone);
}

#[dag(
    schedule = "0 2 * * *",
    catchup = false,
    owner = "etl-team",
    runbook = "https://wiki.acme.com/etl-runbook",
    severity = "sev3"
)]
fn metadata_dag(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_metadata_attributes() {
    let info = __autumn_dag_info_metadata_dag();
    assert_eq!(info.owner, Some("etl-team"));
    assert_eq!(info.runbook_url, Some("https://wiki.acme.com/etl-runbook"));
    assert_eq!(info.severity, Some("sev3"));
}

// ── Issue #482 — condition macro + builder parity (AC2) ──────────────────────

#[activity]
async fn score_payment(_ctx: &ActivityContext) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({"fraud_score": 0.0}))
}

#[activity]
async fn manual_review(_ctx: &ActivityContext) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!("reviewed"))
}

/// DAG using `.condition(...)` via the `#[dag]` macro — same `DagTaskRef` builder
/// method as a hand-written `DagBuilder` call, so macro and builder produce
/// identical `DagDefinitions` (AC2).
#[cfg(feature = "unified-dag-execution")]
#[dag(default_queue = "risk-workers")]
fn conditional_dag_macro(dag: &mut DagBuilder) {
    let score = dag.activity(score_payment);
    let _review = dag
        .activity(manual_review)
        .upstream(&score)
        .condition(|ups| ups[0]["fraud_score"].as_f64().is_some_and(|s| s > 0.8));
}

/// Verify that a `#[dag]` using `.condition(...)` compiles and produces a
/// `DagDefinition` where the conditioned task has `condition.is_some()`.
#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_condition_compiles_and_is_stored_on_task() {
    let info = __autumn_dag_info_conditional_dag_macro();
    let definition = info.build_definition().expect("definition should build");
    let tasks = definition.tasks();
    assert_eq!(tasks.len(), 2, "two tasks expected");
    assert!(
        tasks[0].condition.is_none(),
        "root task should have no condition"
    );
    assert!(
        tasks[1].condition.is_some(),
        "conditioned task should have condition set (AC2: macro parity with builder)"
    );
}

// ── DAGs as MCP tools (issue #601 follow-up) ─────────────────────────────────

#[dag(schedule = "0 3 * * *", mcp)]
fn mcp_daily_etl(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_mcp_bare_flag_sets_mcp_true() {
    let info = __autumn_dag_info_mcp_daily_etl();
    assert!(info.mcp, "bare `#[dag(mcp)]` must set DagInfo.mcp = true");
}

#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_mcp_propagates_to_shadow_workflow_info() {
    let workflow_info = __autumn_workflow_info_mcp_daily_etl();
    assert!(
        workflow_info.mcp,
        "the shadow WorkflowInfo companion must carry the same mcp flag as the DagInfo"
    );
}

#[dag(schedule = "0 4 * * *")]
fn non_mcp_daily_etl(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_default_mcp_is_false() {
    let info = __autumn_dag_info_non_mcp_daily_etl();
    assert!(
        !info.mcp,
        "a `#[dag]` without the `mcp` attribute must default to mcp = false"
    );
}

#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_default_mcp_is_false_on_shadow_workflow_info() {
    let workflow_info = __autumn_workflow_info_non_mcp_daily_etl();
    assert!(!workflow_info.mcp);
}

#[dag(mcp = false)]
fn explicit_non_mcp_dag(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_mcp_explicit_false() {
    let info = __autumn_dag_info_explicit_non_mcp_dag();
    assert!(!info.mcp);
}

// ── DAG-level execution_timeout / sla (issue #743) ───────────────────────────

#[dag(schedule = "0 6 * * *", execution_timeout = "4h", sla = "3h")]
fn nightly_deadline_etl(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_execution_timeout_attribute_populates_field() {
    let info = __autumn_dag_info_nightly_deadline_etl();
    assert_eq!(
        info.execution_timeout,
        Some(Duration::from_secs(4 * 3600)),
        "execution_timeout should be 4 hours"
    );
}

#[test]
fn dag_macro_sla_attribute_populates_field() {
    let info = __autumn_dag_info_nightly_deadline_etl();
    assert_eq!(
        info.sla,
        Some(Duration::from_secs(3 * 3600)),
        "sla should be 3 hours"
    );
}

#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_execution_timeout_propagates_to_shadow_workflow_info() {
    let workflow_info = __autumn_workflow_info_nightly_deadline_etl();
    assert_eq!(
        workflow_info.execution_timeout,
        Some(Duration::from_secs(4 * 3600)),
        "the shadow WorkflowInfo companion must carry the same execution_timeout as the DagInfo"
    );
}

#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_sla_propagates_to_shadow_workflow_info() {
    let workflow_info = __autumn_workflow_info_nightly_deadline_etl();
    assert_eq!(
        workflow_info.sla,
        Some(Duration::from_secs(3 * 3600)),
        "the shadow WorkflowInfo companion must carry the same sla as the DagInfo"
    );
}

#[test]
fn dag_macro_default_execution_timeout_and_sla_are_none() {
    let info = __autumn_dag_info_daily_etl();
    assert_eq!(
        info.execution_timeout, None,
        "execution_timeout must default to None (AC7: zero regression)"
    );
    assert_eq!(
        info.sla, None,
        "sla must default to None (AC7: zero regression)"
    );
}

#[cfg(feature = "unified-dag-execution")]
#[test]
fn dag_macro_default_execution_timeout_and_sla_are_none_on_shadow_workflow_info() {
    let workflow_info = __autumn_workflow_info_daily_etl();
    assert_eq!(workflow_info.execution_timeout, None);
    assert_eq!(workflow_info.sla, None);
}

#[dag(schedule = "0 7 * * *", execution_timeout = "1h")]
fn execution_timeout_only_dag(dag: &mut DagBuilder) {
    let _ = dag.activity(extract_users);
}

#[test]
fn dag_macro_execution_timeout_only_leaves_sla_none() {
    let info = __autumn_dag_info_execution_timeout_only_dag();
    assert_eq!(info.execution_timeout, Some(Duration::from_secs(3600)));
    assert_eq!(info.sla, None);
}
