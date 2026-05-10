/// CLI contract coverage tests.
///
/// Every CLI subcommand that calls the management API must map to a route that
/// is documented in `docs/api-contract.json`.  If a CLI command maps to a path
/// that is not in the contract, either the contract is incomplete or the CLI
/// has drifted from the documented API surface.
use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;
use std::collections::HashSet;

const CONTRACT_JSON: &str = include_str!("../../docs/api-contract.json");

/// Returns all `(METHOD, path-template)` pairs from the contract.
fn contract_route_set() -> HashSet<(String, String)> {
    let contract: serde_json::Value =
        serde_json::from_str(CONTRACT_JSON).expect("docs/api-contract.json must be valid JSON");
    contract["routes"]
        .as_array()
        .expect("contract.routes must be an array")
        .iter()
        .map(|r| {
            (
                r["method"].as_str().unwrap().to_string(),
                r["path"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// Returns the HTTP method string for an `ApiMethod`.
fn method_str(m: &ApiMethod) -> &'static str {
    match m {
        ApiMethod::Get => "GET",
        ApiMethod::Post => "POST",
        ApiMethod::Patch => "PATCH",
        ApiMethod::Delete => "DELETE",
    }
}

/// Strips the query string from a path.
fn bare_path(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

/// Returns true if a concrete path (e.g. `/workflows/abc-123/cancel`) matches
/// a contract path template (e.g. `/workflows/{id}/cancel`).
fn path_matches_template(actual: &str, template: &str) -> bool {
    let a: Vec<&str> = actual.trim_start_matches('/').split('/').collect();
    let t: Vec<&str> = template.trim_start_matches('/').split('/').collect();
    if a.len() != t.len() {
        return false;
    }
    a.iter()
        .zip(t.iter())
        .all(|(seg, tmpl)| tmpl.starts_with('{') && tmpl.ends_with('}') || seg == tmpl)
}

/// Asserts that a CLI invocation produces an API request whose method and path
/// are covered by the contract.
#[track_caller]
fn assert_covered(args: &[&str]) {
    let all: Vec<&str> = std::iter::once("harvest").chain(args.iter().copied()).collect();
    let cli = Cli::try_parse_from(&all)
        .unwrap_or_else(|e| panic!("CLI args {args:?} should parse: {e}"));
    let req = cli
        .api_request()
        .unwrap_or_else(|e| panic!("api_request() should succeed for {args:?}: {e}"));

    let method = method_str(&req.method);
    let path = bare_path(&req.path);
    let routes = contract_route_set();

    let found = routes
        .iter()
        .any(|(m, t)| m == method && path_matches_template(path, t));

    assert!(
        found,
        "CLI command {args:?} maps to {method} {path} \
         which is not covered by docs/api-contract.json"
    );
}

// ── health / admin ────────────────────────────────────────────────────────────

#[test]
fn health_is_covered() {
    assert_covered(&["health"]);
}

#[test]
fn preflight_is_covered() {
    assert_covered(&["preflight"]);
}

#[test]
fn shard_health_is_covered() {
    assert_covered(&["shard", "health"]);
}

#[test]
fn version_usage_is_covered() {
    assert_covered(&["version-usage"]);
}

#[test]
fn version_gate_retirement_is_covered() {
    assert_covered(&[
        "version-gate-retirement",
        "--change-id",
        "my_gate",
        "--min-safe-version",
        "2",
    ]);
}

// ── workflows ─────────────────────────────────────────────────────────────────

#[test]
fn workflow_list_is_covered() {
    assert_covered(&["workflow", "list"]);
}

#[test]
fn workflow_get_is_covered() {
    assert_covered(&["workflow", "get", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn workflow_stack_is_covered() {
    assert_covered(&["workflow", "stack", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn workflow_children_is_covered() {
    assert_covered(&[
        "workflow",
        "children",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn workflow_start_is_covered() {
    assert_covered(&["workflow", "start", "my_workflow"]);
}

#[test]
fn workflow_cancel_is_covered() {
    assert_covered(&[
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn workflow_reset_is_covered() {
    assert_covered(&[
        "workflow",
        "reset",
        "00000000-0000-0000-0000-000000000001",
        "--to-event",
        "50",
        "--reason",
        "bad deploy",
    ]);
}

#[test]
fn workflow_signal_is_covered() {
    assert_covered(&[
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "my_signal",
    ]);
}

#[test]
fn workflow_query_is_covered() {
    assert_covered(&[
        "workflow",
        "query",
        "00000000-0000-0000-0000-000000000001",
        "my_query",
    ]);
}

#[test]
fn workflow_update_is_covered() {
    assert_covered(&[
        "workflow",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "my_update",
    ]);
}

#[test]
fn workflow_update_result_is_covered() {
    assert_covered(&[
        "workflow",
        "update-result",
        "00000000-0000-0000-0000-000000000001",
        "aaaaaaaa-bbbb-cccc-dddd-000000000002",
    ]);
}

// ── history ───────────────────────────────────────────────────────────────────

#[test]
fn history_export_single_is_covered() {
    assert_covered(&[
        "history",
        "export",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn history_export_batch_is_covered() {
    assert_covered(&["history", "export-batch"]);
}

// ── external handoffs ─────────────────────────────────────────────────────────

#[test]
fn handoff_list_is_covered() {
    assert_covered(&["handoff", "list"]);
}

#[test]
fn handoff_inspect_is_covered() {
    assert_covered(&[
        "handoff",
        "inspect",
        "11111111-1111-4111-8111-111111111111",
    ]);
}

#[test]
fn handoff_complete_is_covered() {
    assert_covered(&[
        "handoff",
        "complete",
        "11111111-1111-4111-8111-111111111111",
    ]);
}

#[test]
fn handoff_fail_is_covered() {
    assert_covered(&[
        "handoff",
        "fail",
        "11111111-1111-4111-8111-111111111111",
        "--error",
        "rejected",
    ]);
}

#[test]
fn handoff_heartbeat_is_covered() {
    assert_covered(&[
        "handoff",
        "heartbeat",
        "11111111-1111-4111-8111-111111111111",
    ]);
}

// ── DAGs ──────────────────────────────────────────────────────────────────────

#[test]
fn dag_list_is_covered() {
    assert_covered(&["dag", "list"]);
}

#[test]
fn dag_runs_is_covered() {
    assert_covered(&["dag", "runs", "my_dag"]);
}

#[test]
fn dag_trigger_is_covered() {
    assert_covered(&["dag", "trigger", "my_dag"]);
}

#[test]
fn dag_pause_is_covered() {
    assert_covered(&["dag", "pause", "my_dag"]);
}

#[test]
fn dag_unpause_is_covered() {
    assert_covered(&["dag", "unpause", "my_dag"]);
}

// ── schedules ─────────────────────────────────────────────────────────────────

#[test]
fn schedule_list_is_covered() {
    assert_covered(&["schedule", "list"]);
}

#[test]
fn schedule_create_workflow_is_covered() {
    assert_covered(&[
        "schedule",
        "create-workflow",
        "--name",
        "nightly_job",
        "--cron",
        "0 0 * * *",
    ]);
}

#[test]
fn schedule_pause_is_covered() {
    assert_covered(&[
        "schedule",
        "pause",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn schedule_resume_is_covered() {
    assert_covered(&[
        "schedule",
        "resume",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn schedule_delete_is_covered() {
    assert_covered(&[
        "schedule",
        "delete",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

// ── retention ─────────────────────────────────────────────────────────────────

#[test]
fn retention_status_is_covered() {
    assert_covered(&["retention", "status"]);
}

#[test]
fn retention_run_now_is_covered() {
    assert_covered(&["retention", "run-now"]);
}

// ── concurrency ───────────────────────────────────────────────────────────────

#[test]
fn concurrency_status_is_covered() {
    assert_covered(&["concurrency"]);
}

// ── audit ─────────────────────────────────────────────────────────────────────

#[test]
fn audit_list_is_covered() {
    assert_covered(&["audit", "list"]);
}

// ── batch operations ──────────────────────────────────────────────────────────

#[test]
fn batch_list_is_covered() {
    assert_covered(&["batch", "list"]);
}

#[test]
fn batch_get_is_covered() {
    assert_covered(&["batch", "get", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn batch_submit_is_covered() {
    assert_covered(&[
        "batch",
        "submit",
        "Cancel",
        "--filter-json",
        r#"{"states":["RUNNING"]}"#,
    ]);
}

// ── dead-letter queue ─────────────────────────────────────────────────────────

#[test]
fn dlq_list_is_covered() {
    assert_covered(&["dlq", "list"]);
}

#[test]
fn dlq_replay_single_is_covered() {
    assert_covered(&["dlq", "replay", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn dlq_bulk_replay_is_covered() {
    assert_covered(&["dlq", "bulk-replay"]);
}

#[test]
fn dlq_bulk_discard_is_covered() {
    assert_covered(&["dlq", "bulk-discard"]);
}

// ── workers ───────────────────────────────────────────────────────────────────

#[test]
fn worker_list_is_covered() {
    assert_covered(&["worker", "list"]);
}

#[test]
fn worker_get_is_covered() {
    assert_covered(&["worker", "get", "worker-abc"]);
}

#[test]
fn worker_health_is_covered() {
    assert_covered(&["worker", "health"]);
}

#[test]
fn worker_drain_preview_is_covered() {
    assert_covered(&["worker", "drain-preview"]);
}

#[test]
fn worker_drain_is_covered() {
    assert_covered(&["worker", "drain", "worker-abc"]);
}
