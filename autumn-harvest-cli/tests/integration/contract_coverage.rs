/// CLI contract coverage tests.
///
/// Every CLI subcommand that calls the management API must map to a route that
/// is documented in `docs/api-contract.json`.  If a CLI command maps to a path
/// that is not in the contract, either the contract is incomplete or the CLI
/// has drifted from the documented API surface.
///
/// The body-field tests additionally verify that every JSON key the CLI sends
/// in a request body is listed in the contract's `request_body.fields` array,
/// enforcing AC 4: "uses the documented request shape."
use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;
use std::collections::{HashMap, HashSet};

const CONTRACT_JSON: &str = include_str!("../../../docs/api-contract.json");

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
const fn method_str(m: ApiMethod) -> &'static str {
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
    let all: Vec<&str> = std::iter::once("harvest")
        .chain(args.iter().copied())
        .collect();
    let cli =
        Cli::try_parse_from(&all).unwrap_or_else(|e| panic!("CLI args {args:?} should parse: {e}"));
    let req = cli
        .api_request()
        .unwrap_or_else(|e| panic!("api_request() should succeed for {args:?}: {e}"));

    let method = method_str(req.method);
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

// ── Body-field helpers (AC 4) ─────────────────────────────────────────────────

/// Returns a map of (METHOD, path-template) → documented field names extracted
/// from the contract's structured `request_body.fields` array.
/// `None` means the body is free-form (no fixed schema).
/// Panics if a mutating route is missing the required `fields` array.
fn contract_route_request_fields() -> HashMap<(String, String), Option<Vec<String>>> {
    let contract: serde_json::Value =
        serde_json::from_str(CONTRACT_JSON).expect("api-contract.json must be valid JSON");
    let mutating = ["POST", "PUT", "PATCH", "DELETE"];
    let mut map: HashMap<(String, String), Option<Vec<String>>> = HashMap::new();

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap().to_string();
        let path = route["path"].as_str().unwrap().to_string();
        if !mutating.contains(&method.as_str()) {
            continue;
        }
        let rb = &route["request_body"];
        assert!(
            rb.is_object(),
            "mutating route {method} {path} must have a 'request_body' object in the contract"
        );
        if rb["free_form"].as_bool().unwrap_or(false) {
            map.insert((method, path), None);
        } else {
            let fields: Vec<String> = rb["fields"]
                .as_array()
                .unwrap_or_else(|| {
                    panic!(
                        "{method} {path}: contract request_body must have a 'fields' array \
                         (set free_form:true for routes whose body is an opaque payload)"
                    )
                })
                .iter()
                .filter_map(|f| f["name"].as_str().map(ToString::to_string))
                .collect();
            map.insert((method, path), Some(fields));
        }
    }
    map
}

/// Asserts that every JSON key the CLI sends in a request body is listed in
/// the contract's `request_body.fields` for the matching route.
/// Skips GET requests and `None` bodies.  Free-form routes are also skipped.
#[track_caller]
fn assert_body_fields_documented(args: &[&str]) {
    let all: Vec<&str> = std::iter::once("harvest")
        .chain(args.iter().copied())
        .collect();
    let cli =
        Cli::try_parse_from(&all).unwrap_or_else(|e| panic!("CLI args {args:?} should parse: {e}"));
    let req = cli
        .api_request()
        .unwrap_or_else(|e| panic!("api_request() should succeed for {args:?}: {e}"));

    // GET requests never have a body — nothing to validate.
    let method = method_str(req.method);
    if method == "GET" {
        return;
    }

    let Some(body) = req.body else { return };
    let Some(body_obj) = body.as_object() else {
        return; // free-form non-object body — skip
    };
    if body_obj.is_empty() {
        return; // empty body — nothing to validate
    }

    let path = bare_path(&req.path);
    let fields_map = contract_route_request_fields();

    // Find the matching contract entry by template matching.
    let entry = fields_map
        .iter()
        .find(|((m, t), _)| m == method && path_matches_template(path, t));

    let Some((_, field_spec)) = entry else {
        // Route not in fields map — covered by the route-membership test.
        return;
    };

    let Some(contract_fields) = field_spec else {
        // Free-form body — no per-field validation possible.
        return;
    };

    let documented: HashSet<&str> = contract_fields.iter().map(String::as_str).collect();
    for key in body_obj.keys() {
        assert!(
            documented.contains(key.as_str()),
            "CLI command {args:?} sends body field '{key}' for {method} {path} \
             but '{key}' is not documented in docs/api-contract.json. \
             Add it to request_body.fields and management_api_request_fields()."
        );
    }
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
fn workflow_types_reachability_is_covered() {
    assert_covered(&["workflow-types", "reachability"]);
}

#[test]
fn canary_is_covered() {
    assert_covered(&["canary"]);
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
fn workflow_summaries_is_covered() {
    assert_covered(&["workflow", "summaries"]);
    assert_covered(&[
        "workflow",
        "summaries",
        "--workflow-name",
        "onboarding",
        "--state",
        "COMPLETED",
    ]);
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
fn workflow_timeline_is_covered() {
    assert_covered(&[
        "workflow",
        "timeline",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn workflow_run_chain_is_covered() {
    assert_covered(&[
        "workflow",
        "run-chain",
        "00000000-0000-0000-0000-000000000001",
    ]);
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
fn legal_hold_set_is_covered() {
    assert_covered(&[
        "legal-hold",
        "set",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "case 42",
    ]);
}

#[test]
fn legal_hold_release_is_covered() {
    assert_covered(&[
        "legal-hold",
        "release",
        "00000000-0000-0000-0000-000000000001",
    ]);
}

#[test]
fn workflow_start_is_covered() {
    assert_covered(&["workflow", "start", "my_workflow"]);
}

#[test]
fn workflow_cancel_is_covered() {
    assert_covered(&["workflow", "cancel", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn workflow_pause_is_covered() {
    assert_covered(&["workflow", "pause", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn workflow_resume_is_covered() {
    assert_covered(&["workflow", "resume", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn workflow_fail_activity_is_covered() {
    assert_covered(&[
        "workflow",
        "fail-activity",
        "00000000-0000-0000-0000-000000000001",
        "11111111-1111-4111-8111-111111111111",
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
fn workflow_signal_with_idempotency_key_is_covered() {
    // Issue #753: --idempotency-key maps onto the documented
    // ?idempotency_key= query param of the same contract route.
    assert_covered(&[
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "my_signal",
        "--idempotency-key",
        "evt_abc123",
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
    assert_covered(&["history", "export", "00000000-0000-0000-0000-000000000001"]);
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
    assert_covered(&["handoff", "inspect", "11111111-1111-4111-8111-111111111111"]);
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

#[test]
fn dag_retry_is_covered() {
    assert_covered(&[
        "dag",
        "retry",
        "my_dag",
        "11111111-2222-3333-4444-555555555555",
        "--from-node",
        "step_6",
        "--reason",
        "incident",
    ]);
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
    assert_covered(&["schedule", "pause", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn schedule_resume_is_covered() {
    assert_covered(&["schedule", "resume", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn schedule_delete_is_covered() {
    assert_covered(&["schedule", "delete", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn schedule_update_is_covered() {
    assert_covered(&[
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "--cron",
        "0 3 * * *",
    ]);
}

#[test]
fn schedule_backfill_is_covered() {
    assert_covered(&[
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
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
    assert_covered(&["concurrency", "status"]);
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

// ── Body-field validation tests (AC 4) ───────────────────────────────────────
//
// For every mutating CLI command that sends a structured request body, verify
// that each body field is listed in the contract's request_body.fields.

#[test]
fn workflow_start_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "start",
        "my_workflow",
        "--workflow-id",
        "wf-1",
        "--queue",
        "critical",
        "--input-json",
        r#"{"x":1}"#,
        "--memo-json",
        r#"{"note":"test"}"#,
        "--search-attrs-json",
        r#"{"tenant":"acme"}"#,
        "--execution-timeout-secs",
        "30",
        "--reuse-policy",
        "reject_duplicate",
        "--conflict-policy",
        "use_existing",
    ]);
}

#[test]
fn workflow_cancel_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "test",
    ]);
}

#[test]
fn workflow_pause_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "pause",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "test",
    ]);
}

#[test]
fn workflow_reset_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "reset",
        "00000000-0000-0000-0000-000000000001",
        "--to-event",
        "50",
        "--reason",
        "bad deploy",
        "--operator-id",
        "ops",
        "--signal-reapply",
        "buffer",
    ]);
}

#[test]
fn workflow_update_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "my_update",
        "--input-json",
        r#"{"approved":true}"#,
    ]);
}

#[test]
fn dag_trigger_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dag",
        "trigger",
        "my_dag",
        "--conf-json",
        r#"{"date":"2026-01-01"}"#,
    ]);
}

#[test]
fn dag_pause_body_fields_are_documented() {
    assert_body_fields_documented(&["dag", "pause", "my_dag"]);
}

#[test]
fn dag_unpause_body_fields_are_documented() {
    assert_body_fields_documented(&["dag", "unpause", "my_dag"]);
}

#[test]
fn workflow_fail_activity_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "workflow",
        "fail-activity",
        "00000000-0000-0000-0000-000000000001",
        "11111111-1111-4111-8111-111111111111",
        "--reason",
        "hung in flight",
    ]);
}

#[test]
fn dag_retry_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dag",
        "retry",
        "my_dag",
        "11111111-2222-3333-4444-555555555555",
        "--from-node",
        "step_6",
        "--reason",
        "incident",
    ]);
}

#[test]
fn dlq_bulk_replay_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-replay",
        "--activity-name",
        "send_email",
        "--workflow-name",
        "billing",
        "--failed-after",
        "2026-01-01T00:00:00Z",
        "--failed-before",
        "2026-02-01T00:00:00Z",
        "--limit",
        "100",
        "--dry-run",
    ]);
}

#[test]
fn dlq_bulk_discard_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-discard",
        "--activity-name",
        "send_email",
        "--dry-run",
    ]);
}

#[test]
fn dlq_bulk_replay_cause_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-replay",
        "--error-class",
        "CircuitOpen",
        "--dlq-reason",
        "poison_pill",
        "--failure-signature",
        "connection refused",
    ]);
}

#[test]
fn dlq_bulk_discard_cause_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-discard",
        "--error-class",
        "CircuitOpen",
        "--dlq-reason",
        "poison_pill",
        "--failure-signature",
        "connection refused",
    ]);
}

#[test]
fn dlq_bulk_replay_queue_and_min_attempts_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-replay",
        "--queue-name",
        "low-pri",
        "--min-attempts",
        "3",
    ]);
}

#[test]
fn dlq_bulk_discard_queue_and_min_attempts_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "dlq",
        "bulk-discard",
        "--queue-name",
        "low-pri",
        "--min-attempts",
        "3",
    ]);
}

#[test]
fn handoff_complete_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "handoff",
        "complete",
        "11111111-1111-4111-8111-111111111111",
        "--output-json",
        r#"{"approved":true}"#,
    ]);
}

#[test]
fn handoff_fail_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "handoff",
        "fail",
        "11111111-1111-4111-8111-111111111111",
        "--error",
        "rejected",
        "--retryable",
    ]);
}

#[test]
fn handoff_heartbeat_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "handoff",
        "heartbeat",
        "11111111-1111-4111-8111-111111111111",
        "--extend-by-secs",
        "3600",
    ]);
}

#[test]
fn worker_drain_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "worker",
        "drain",
        "worker-abc",
        "--deadline",
        "2026-06-01T12:00:00Z",
    ]);
}

#[test]
fn batch_submit_cancel_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "batch",
        "submit",
        "Cancel",
        "--filter-json",
        r#"{"states":["RUNNING"]}"#,
    ]);
}

#[test]
fn batch_submit_signal_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "batch",
        "submit",
        "Signal",
        "--filter-json",
        r#"{"states":["RUNNING"]}"#,
        "--signal-name",
        "my_signal",
        "--signal-payload-json",
        r#"{"key":"val"}"#,
    ]);
}

#[test]
fn schedule_create_workflow_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "schedule",
        "create-workflow",
        "--name",
        "nightly",
        "--cron",
        "0 0 * * *",
        "--input-json",
        r#"{"env":"prod"}"#,
        "--max-active-runs",
        "3",
        "--catchup",
        "--paused",
    ]);
}

#[test]
fn schedule_update_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "--cron",
        "0 3 * * *",
        "--tz",
        "America/New_York",
        "--input-json",
        r#"{"env":"prod"}"#,
        "--queue",
        "etl-workers",
        "--overlap-policy",
        "buffer_all",
        "--buffer-all-max",
        "12",
        "--catchup-policy",
        "window",
        "--catchup-window-secs",
        "7200",
        "--jitter-secs",
        "30",
        "--max-active-runs",
        "4",
        "--calendar",
        "us-holidays",
        "--end-at",
        "2030-01-01T00:00:00Z",
        "--max-runs",
        "24",
    ]);
}

#[test]
fn schedule_backfill_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
        "--dry-run",
        "--max-count",
        "50",
        "--include-paused",
    ]);
}

// ── start-batch (issue #357) ──────────────────────────────────────────────────

#[test]
fn start_batch_is_covered() {
    assert_covered(&[
        "start-batch",
        "--items-json",
        r#"[{"workflow_name":"onboarding","workflow_id":"w1"}]"#,
    ]);
}

#[test]
fn start_batch_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "start-batch",
        "--items-json",
        r#"[{"workflow_name":"onboarding","workflow_id":"w1"}]"#,
    ]);
}

#[test]
fn start_batch_atomic_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "start-batch",
        "--items-json",
        r#"[{"workflow_name":"onboarding"}]"#,
        "--atomic",
    ]);
}

#[test]
fn canary_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "canary",
        "--sample-size",
        "20",
        "--workflow-name",
        "billing",
        "--queue",
        "critical",
    ]);
}

// ── Build routing ramp (issue #604) ────────────────────────────────────────

#[test]
fn build_ramp_set_is_covered() {
    assert_covered(&[
        "build",
        "ramp",
        "set",
        "--queue",
        "default",
        "--target-build-id",
        "canary-v2",
        "--percent",
        "25",
    ]);
}

#[test]
fn build_ramp_show_is_covered() {
    assert_covered(&["build", "ramp", "show"]);
}

#[test]
fn build_ramp_clear_is_covered() {
    assert_covered(&["build", "ramp", "clear", "--queue", "default"]);
}

#[test]
fn build_ramp_set_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "build",
        "ramp",
        "set",
        "--queue",
        "default",
        "--target-build-id",
        "canary-v2",
        "--percent",
        "25",
    ]);
}

// ── Scoped API tokens (issue #942) ────────────────────────────────────────────

#[test]
fn token_create_is_covered() {
    assert_covered(&["token", "create", "ci-bot", "--scope", "read"]);
}

#[test]
fn token_list_is_covered() {
    assert_covered(&["token", "list"]);
}

#[test]
fn token_revoke_is_covered() {
    assert_covered(&["token", "revoke", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn token_rotate_is_covered() {
    assert_covered(&["token", "rotate", "00000000-0000-0000-0000-000000000001"]);
}

#[test]
fn token_create_body_fields_are_documented() {
    assert_body_fields_documented(&[
        "token", "create", "ci-bot", "--scope", "read", "--expires-at",
        "2027-01-01T00:00:00Z",
    ]);
}
