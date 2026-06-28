use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;
use serde_json::json;

#[test]
fn preflight_maps_to_management_api_request() {
    let cli = Cli::try_parse_from(["harvest", "preflight"]).expect("preflight args should parse");

    let request = cli.api_request().expect("preflight request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/preflight");
    assert_eq!(request.body, None);
}

#[test]
fn shard_health_maps_to_management_api_request() {
    let cli = Cli::try_parse_from(["harvest", "shard", "health"])
        .expect("shard health args should parse");

    let request = cli
        .api_request()
        .expect("shard health request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/shards/health");
    assert_eq!(request.body, None);
}

#[test]
fn shard_health_candidate_maps_to_query_string() {
    let cli = Cli::try_parse_from([
        "harvest",
        "shard",
        "health",
        "--candidate-shard",
        "2",
        "--fail-on-unready",
    ])
    .expect("shard health candidate args should parse");

    let request = cli
        .api_request()
        .expect("shard health request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/shards/health?candidate_shard=2");
    assert_eq!(request.body, None);
}

#[test]
fn version_usage_report_maps_filters_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "version-usage",
        "--workflow-name",
        "billing_checkout",
        "--change-id",
        "billing_checkout_v2_tax",
        "--version",
        "1",
        "--state-group",
        "active",
        "--shard-id",
        "2",
    ])
    .expect("version usage args should parse");

    let request = cli
        .api_request()
        .expect("version usage request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/version-gates/usage?workflow_name=billing_checkout\
         &change_id=billing_checkout_v2_tax&recorded_version=1&state_group=active&shard_id=2"
    );
    assert_eq!(request.body, None);
}

#[test]
fn version_usage_guard_maps_to_active_state_group() {
    let cli = Cli::try_parse_from([
        "harvest",
        "version-usage",
        "--change-id",
        "billing_checkout_v2_tax",
        "--version",
        "1",
        "--guard",
    ])
    .expect("version usage guard args should parse");

    let request = cli
        .api_request()
        .expect("version usage guard request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/version-gates/usage?change_id=billing_checkout_v2_tax&recorded_version=1&state_group=active"
    );
    assert_eq!(request.body, None);
}

#[test]
fn workflow_start_maps_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "start",
        "approval_workflow",
        "--workflow-id",
        "approval-42",
        "--queue",
        "critical",
        "--input-json",
        r#"{"request_id":"42"}"#,
        "--memo-json",
        r#"{"source":"cli"}"#,
        "--search-attrs-json",
        r#"{"tenant":"acme"}"#,
        "--execution-timeout-secs",
        "30",
    ])
    .expect("workflow start args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/approval_workflow/start");
    assert_eq!(
        request.body,
        Some(json!({
            "workflow_id": "approval-42",
            "input": { "request_id": "42" },
            "queue": "critical",
            "memo": { "source": "cli" },
            "search_attrs": { "tenant": "acme" },
            "execution_timeout_secs": 30,
        }))
    );
}

#[test]
fn workflow_list_and_query_use_get_requests() {
    let list = Cli::try_parse_from(["harvest", "workflow", "list", "--limit", "25"])
        .expect("workflow list args should parse");
    let list_request = list.api_request().expect("list request should build");

    assert_eq!(list_request.method, ApiMethod::Get);
    assert_eq!(list_request.path, "/workflows?limit=25");
    assert_eq!(list_request.body, None);

    let query = Cli::try_parse_from([
        "harvest",
        "workflow",
        "query",
        "00000000-0000-0000-0000-000000000001",
        "status",
    ])
    .expect("workflow query args should parse");
    let query_request = query.api_request().expect("query request should build");

    assert_eq!(query_request.method, ApiMethod::Get);
    assert_eq!(
        query_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/query/status"
    );
    assert_eq!(query_request.body, None);

    let stack = Cli::try_parse_from([
        "harvest",
        "workflow",
        "stack",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow stack args should parse");
    let stack_request = stack.api_request().expect("stack request should build");
    assert_eq!(stack_request.method, ApiMethod::Get);
    assert_eq!(
        stack_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/stack"
    );
    assert_eq!(stack_request.body, None);
}

#[test]
fn workflow_list_filters_map_to_query_string() {
    let list = Cli::try_parse_from([
        "harvest",
        "workflow",
        "list",
        "--state",
        "RUNNING",
        "--workflow-name",
        "onboarding",
        "--search-attr",
        "tenant=acme",
        "--search-attr",
        "customer_id=42",
    ])
    .expect("filtered list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows?state=RUNNING&workflow_name=onboarding\
         &search_attr=tenant:acme&search_attr=customer_id:42"
    );
    assert_eq!(request.body, None);
}

#[test]
fn workflow_list_search_attr_filter_maps_to_query_string() {
    // Issue #506: typed comparison/set predicates forwarded verbatim to the
    // `search_attr_filter` API param. The CLI does not transform the value.
    let list = Cli::try_parse_from([
        "harvest",
        "workflow",
        "list",
        "--search-attr-filter",
        "amount:gt:10000",
        "--search-attr-filter",
        "phase:in:blocked,awaiting_approval",
    ])
    .expect("predicate list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows?search_attr_filter=amount:gt:10000\
         &search_attr_filter=phase:in:blocked,awaiting_approval"
    );
    assert_eq!(request.body, None);
}

#[test]
fn workflow_list_supports_repeated_and_comma_states() {
    let list = Cli::try_parse_from([
        "harvest",
        "workflow",
        "list",
        "--state",
        "RUNNING,FAILED",
        "--state",
        "TIMED_OUT",
    ])
    .expect("multi-state list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.path, "/workflows?state=RUNNING,FAILED,TIMED_OUT");
}

#[test]
fn workflow_children_maps_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "children",
        "00000000-0000-0000-0000-000000000001",
        "--status",
        "Failed",
        "--status",
        "Running",
        "--workflow-name",
        "billing_child",
        "--limit",
        "25",
        "--cursor",
        "2026-05-04T12:00:00Z|00000000-0000-0000-0000-000000000099",
        "--depth",
        "2",
        "--json",
    ])
    .expect("workflow children args should parse");
    let request = cli.api_request().expect("children request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/children\
         ?status=Failed&status=Running&workflow_name=billing_child&limit=25\
         &cursor=2026-05-04T12:00:00Z%7C00000000-0000-0000-0000-000000000099&depth=2"
    );
    assert_eq!(request.body, None);
}

#[test]
fn history_export_maps_single_execution_to_read_only_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "history",
        "export",
        "00000000-0000-0000-0000-000000000001",
        "--payload-policy",
        "full",
        "--max-bytes",
        "1048576",
        "--output-file",
        "fixtures/billing.json",
    ])
    .expect("history export args should parse");
    let request = cli
        .api_request()
        .expect("history export request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/history/export?payload_policy=full&max_bytes=1048576"
    );
    assert_eq!(request.body, None);
}

#[test]
fn history_export_batch_maps_filters_to_admin_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "history",
        "export-batch",
        "--workflow-name",
        "billing_checkout",
        "--state-group",
        "terminal",
        "--updated-after",
        "2026-05-01T00:00:00Z",
        "--updated-before",
        "2026-05-08T00:00:00Z",
        "--shard-id",
        "2",
        "--limit",
        "1000",
        "--payload-policy",
        "redacted",
    ])
    .expect("batch history export args should parse");
    let request = cli
        .api_request()
        .expect("batch history export request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/history/exports?workflow_name=billing_checkout&state_group=terminal\
         &updated_after=2026-05-01T00:00:00Z&updated_before=2026-05-08T00:00:00Z\
         &shard_id=2&limit=1000&payload_policy=redacted"
    );
    assert_eq!(request.body, None);
}

#[test]
fn handoff_list_maps_filters_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "handoff",
        "list",
        "--state",
        "PENDING,FAILED",
        "--workflow-name",
        "billing_checkout",
        "--execution-id",
        "00000000-0000-0000-0000-000000000001",
        "--activity-name",
        "manager_approval",
        "--token",
        "11111111-1111-4111-8111-111111111111",
        "--shard-id",
        "2",
        "--due-before",
        "2026-05-08T12:00:00Z",
        "--updated-before",
        "2026-05-08T13:00:00Z",
        "--limit",
        "25",
    ])
    .expect("handoff list args should parse");
    let request = cli
        .api_request()
        .expect("handoff list request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/external-handoffs?state=PENDING,FAILED&workflow_name=billing_checkout\
         &execution_id=00000000-0000-0000-0000-000000000001&activity_name=manager_approval\
         &token=11111111-1111-4111-8111-111111111111&shard_id=2\
         &due_before=2026-05-08T12:00:00Z&updated_before=2026-05-08T13:00:00Z&limit=25"
    );
    assert_eq!(request.body, None);
}

#[test]
fn handoff_inspect_maps_to_detail_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "handoff",
        "inspect",
        "11111111-1111-4111-8111-111111111111",
    ])
    .expect("handoff inspect args should parse");
    let request = cli
        .api_request()
        .expect("handoff inspect request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/external-handoffs/11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(request.body, None);
}

#[test]
fn handoff_mutations_map_to_token_completion_routes() {
    let complete = Cli::try_parse_from([
        "harvest",
        "handoff",
        "complete",
        "11111111-1111-4111-8111-111111111111",
        "--output-json",
        r#"{"approved":true}"#,
    ])
    .expect("handoff complete args should parse");
    let complete_request = complete
        .api_request()
        .expect("complete request should build");
    assert_eq!(complete_request.method, ApiMethod::Post);
    assert_eq!(
        complete_request.path,
        "/activities/external/11111111-1111-4111-8111-111111111111/complete"
    );
    assert_eq!(
        complete_request.body,
        Some(json!({ "output": { "approved": true } }))
    );

    let fail = Cli::try_parse_from([
        "harvest",
        "handoff",
        "fail",
        "11111111-1111-4111-8111-111111111111",
        "--error",
        "manager rejected",
        "--retryable",
    ])
    .expect("handoff fail args should parse");
    let fail_request = fail.api_request().expect("fail request should build");
    assert_eq!(fail_request.method, ApiMethod::Post);
    assert_eq!(
        fail_request.path,
        "/activities/external/11111111-1111-4111-8111-111111111111/fail"
    );
    assert_eq!(
        fail_request.body,
        Some(json!({ "error": "manager rejected", "retryable": true }))
    );

    let heartbeat = Cli::try_parse_from([
        "harvest",
        "handoff",
        "heartbeat",
        "11111111-1111-4111-8111-111111111111",
        "--extend-by-secs",
        "3600",
    ])
    .expect("handoff heartbeat args should parse");
    let heartbeat_request = heartbeat
        .api_request()
        .expect("heartbeat request should build");
    assert_eq!(heartbeat_request.method, ApiMethod::Post);
    assert_eq!(
        heartbeat_request.path,
        "/activities/external/11111111-1111-4111-8111-111111111111/heartbeat"
    );
    assert_eq!(
        heartbeat_request.body,
        Some(json!({ "extend_by_secs": 3600 }))
    );
}

#[test]
fn workflow_list_rejects_search_attr_without_equals() {
    let list = Cli::try_parse_from(["harvest", "workflow", "list", "--search-attr", "tenant"])
        .expect("malformed search-attr args should still parse at clap level");

    let err = list
        .api_request()
        .expect_err("malformed search-attr should fail to map");
    let message = err.to_string();
    assert!(
        message.contains("invalid --search-attr"),
        "unexpected error: {message}"
    );
}

#[test]
fn workflow_signal_and_cancel_use_post_bodies() {
    let signal = Cli::try_parse_from([
        "harvest",
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "approved",
        "--payload-json",
        r#"{"approved":true}"#,
    ])
    .expect("workflow signal args should parse");
    let signal_request = signal.api_request().expect("signal request should build");

    assert_eq!(signal_request.method, ApiMethod::Post);
    assert_eq!(
        signal_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/signal/approved"
    );
    assert_eq!(signal_request.body, Some(json!({ "approved": true })));

    let cancel = Cli::try_parse_from([
        "harvest",
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "operator changed their mind",
    ])
    .expect("workflow cancel args should parse");
    let cancel_request = cancel.api_request().expect("cancel request should build");

    assert_eq!(cancel_request.method, ApiMethod::Post);
    assert_eq!(
        cancel_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/cancel"
    );
    assert_eq!(
        cancel_request.body,
        Some(json!({ "reason": "operator changed their mind" }))
    );
}

#[test]
fn workflow_reset_maps_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "reset",
        "00000000-0000-0000-0000-000000000001",
        "--to-event",
        "100",
        "--reason",
        "bad deploy",
        "--operator-id",
        "oncall",
        "--signal-reapply",
        "buffer",
    ])
    .expect("workflow reset args should parse");
    let request = cli.api_request().expect("reset request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/reset"
    );
    assert_eq!(
        request.body,
        Some(json!({
            "reset_to_event_id": 100,
            "reason": "bad deploy",
            "operator_id": "oncall",
            "signal_reapply": "buffer"
        }))
    );
}

#[test]
fn workflow_reset_dry_run_sets_query_flag() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "reset",
        "00000000-0000-0000-0000-000000000001",
        "--to-event",
        "10",
        "--reason",
        "verify before the pointy end",
        "--dry-run",
    ])
    .expect("workflow reset dry-run args should parse");
    let request = cli
        .api_request()
        .expect("reset dry-run request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/reset?dry_run=true"
    );
    assert_eq!(
        request.body,
        Some(json!({
            "reset_to_event_id": 10,
            "reason": "verify before the pointy end",
            "operator_id": "cli",
            "signal_reapply": "drop"
        }))
    );
}

#[test]
fn dag_commands_match_dag_management_routes() {
    let trigger = Cli::try_parse_from([
        "harvest",
        "dag",
        "trigger",
        "daily_pipeline",
        "--conf-json",
        r#"{"date":"2026-04-21"}"#,
    ])
    .expect("dag trigger args should parse");
    let trigger_request = trigger.api_request().expect("trigger request should build");

    assert_eq!(trigger_request.method, ApiMethod::Post);
    assert_eq!(trigger_request.path, "/dags/daily_pipeline/trigger");
    assert_eq!(
        trigger_request.body,
        Some(json!({ "conf": { "date": "2026-04-21" } }))
    );

    let pause = Cli::try_parse_from(["harvest", "dag", "pause", "daily_pipeline"])
        .expect("dag pause args should parse");
    let pause_request = pause.api_request().expect("pause request should build");

    assert_eq!(pause_request.method, ApiMethod::Patch);
    assert_eq!(pause_request.path, "/dags/daily_pipeline");
    assert_eq!(pause_request.body, Some(json!({ "paused": true })));

    let unpause = Cli::try_parse_from(["harvest", "dag", "unpause", "daily_pipeline"])
        .expect("dag unpause args should parse");
    let unpause_request = unpause.api_request().expect("unpause request should build");

    assert_eq!(unpause_request.method, ApiMethod::Patch);
    assert_eq!(unpause_request.path, "/dags/daily_pipeline");
    assert_eq!(unpause_request.body, Some(json!({ "paused": false })));
}

#[test]
fn dag_retry_maps_to_retry_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "--actor",
        "oncall@example.com",
        "dag",
        "retry",
        "nightly_etl",
        "11111111-2222-3333-4444-555555555555",
        "--from-node",
        "step_6",
        "--from-node",
        "step_7",
        "--reason",
        "S3 incident 2026-05-17",
        "--dry-run",
    ])
    .expect("dag retry args should parse");
    let request = cli.api_request().expect("retry request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/dags/nightly_etl/runs/11111111-2222-3333-4444-555555555555/retry"
    );
    assert_eq!(
        request.body,
        Some(json!({
            "from_nodes": ["step_6", "step_7"],
            "reason": "S3 incident 2026-05-17",
            "operator_id": "oncall@example.com",
            "dry_run": true
        }))
    );
}

#[test]
fn dead_letter_replay_matches_management_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "replay",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("dead-letter replay args should parse");

    let request = cli.api_request().expect("replay request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/dead-letters/00000000-0000-0000-0000-000000000001/replay"
    );
    assert_eq!(request.body, None);
}

#[test]
fn path_segments_are_percent_encoded() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "start",
        "tenant/workflow with spaces",
    ])
    .expect("workflow start args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.path,
        "/workflows/tenant%2Fworkflow%20with%20spaces/start"
    );
}

#[test]
fn dlq_bulk_replay_maps_to_management_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-replay",
        "--activity-name",
        "send_email",
        "--dry-run",
    ])
    .expect("bulk-replay args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/replay");
    assert_eq!(
        request.body,
        Some(json!({
            "activity_name": "send_email",
            "dry_run": true
        }))
    );
}

#[test]
fn dlq_bulk_discard_maps_to_management_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-discard",
        "--activity-name",
        "send_email",
        "--failed-after",
        "2026-04-27T12:30:00Z",
    ])
    .expect("bulk-discard args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/discard");
    assert_eq!(
        request.body,
        Some(json!({
            "activity_name": "send_email",
            "failed_after": "2026-04-27T12:30:00Z"
        }))
    );
}

#[test]
fn dlq_redrive_maps_to_management_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "redrive",
        "--error-contains",
        "connection refused",
        "--dry-run",
    ])
    .expect("redrive args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dlq/redrive");
    assert_eq!(
        request.body,
        Some(json!({
            "error_contains": "connection refused",
            "dry_run": true
        }))
    );
}

#[test]
fn dlq_redrive_with_all_filters_maps_correctly() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "redrive",
        "--queue",
        "email-workers",
        "--workflow-name",
        "onboarding",
        "--dead-lettered-after",
        "2026-04-27T12:30:00Z",
        "--dead-lettered-before",
        "2026-04-27T14:30:00Z",
        "--error-contains",
        "timeout",
        "--dead-letter-id",
        "11111111-1111-1111-1111-111111111111,22222222-2222-2222-2222-222222222222",
        "--max",
        "250",
        "--reason",
        "downstream fixed",
    ])
    .expect("redrive with all filters should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dlq/redrive");
    let body = request.body.expect("should have body");
    assert_eq!(body["queue"], "email-workers");
    assert_eq!(body["workflow_name"], "onboarding");
    assert_eq!(body["dead_lettered_after"], "2026-04-27T12:30:00Z");
    assert_eq!(body["dead_lettered_before"], "2026-04-27T14:30:00Z");
    assert_eq!(body["error_contains"], "timeout");
    assert_eq!(
        body["dead_letter_ids"],
        json!([
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222"
        ])
    );
    assert_eq!(body["max"], 250);
    assert_eq!(body["reason"], "downstream fixed");
    // dry_run omitted → not present
    assert!(body.get("dry_run").is_none());
}

#[test]
fn dlq_bulk_replay_with_all_filters_maps_correctly() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-replay",
        "--activity-name",
        "charge_card",
        "--workflow-name",
        "billing",
        "--failed-after",
        "2026-04-27T12:30:00Z",
        "--failed-before",
        "2026-04-27T14:30:00Z",
        "--limit",
        "500",
        "--dry-run",
    ])
    .expect("bulk-replay with all filters should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/replay");
    let body = request.body.expect("should have body");
    assert_eq!(body["activity_name"], "charge_card");
    assert_eq!(body["workflow_name"], "billing");
    assert_eq!(body["failed_after"], "2026-04-27T12:30:00Z");
    assert_eq!(body["failed_before"], "2026-04-27T14:30:00Z");
    assert_eq!(body["limit"], 500);
    assert_eq!(body["dry_run"], true);
}

#[test]
fn workflow_update_maps_to_post_with_wait_query() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "approve",
        "--input-json",
        r#"{"approved":true}"#,
    ])
    .expect("workflow update args should parse");
    let request = cli.api_request().expect("update request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/update/approve?wait=completed"
    );
    assert_eq!(request.body, Some(json!({ "input": { "approved": true } })));
}

#[test]
fn workflow_update_admitted_mode_sets_wait_query_param() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "update",
        "00000000-0000-0000-0000-000000000001",
        "approve",
        "--wait",
        "admitted",
        "--timeout-secs",
        "10",
    ])
    .expect("workflow update admitted args should parse");
    let request = cli
        .api_request()
        .expect("update admitted request should build");

    assert_eq!(request.method, ApiMethod::Post);
    // wait=admitted is in path; timeout_secs is included too
    assert!(
        request.path.contains("wait=admitted"),
        "path must include wait=admitted: {}",
        request.path
    );
    assert!(
        request.path.contains("timeout_secs=10"),
        "path must include timeout_secs=10: {}",
        request.path
    );
}

#[test]
fn workflow_update_result_maps_to_get() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "update-result",
        "00000000-0000-0000-0000-000000000001",
        "aaaaaaaa-bbbb-cccc-dddd-000000000002",
    ])
    .expect("workflow update-result args should parse");
    let request = cli
        .api_request()
        .expect("update-result request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/update/aaaaaaaa-bbbb-cccc-dddd-000000000002/result"
    );
    assert_eq!(request.body, None);
}

#[test]
fn retention_commands_match_management_routes() {
    let status = Cli::try_parse_from(["harvest", "retention", "status"])
        .expect("retention status args should parse");
    let status_request = status.api_request().expect("status request should build");
    assert_eq!(status_request.method, ApiMethod::Get);
    assert_eq!(status_request.path, "/admin/retention");
    assert_eq!(status_request.body, None);

    let run_now = Cli::try_parse_from(["harvest", "retention", "run-now"])
        .expect("retention run-now args should parse");
    let run_now_request = run_now.api_request().expect("run-now request should build");
    assert_eq!(run_now_request.method, ApiMethod::Post);
    assert_eq!(run_now_request.path, "/admin/retention/run-now");
    assert_eq!(run_now_request.body, None);
}

#[test]
fn audit_list_no_filters_maps_to_admin_audit() {
    let cli =
        Cli::try_parse_from(["harvest", "audit", "list"]).expect("audit list args should parse");
    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/audit");
    assert_eq!(request.body, None);
}

#[test]
fn audit_list_all_filters_builds_correct_query_string() {
    let cli = Cli::try_parse_from([
        "harvest",
        "audit",
        "list",
        "--actor",
        "alice@example.com",
        "--operation",
        "workflow.cancel",
        "--target-type",
        "workflow",
        "--target-id",
        "00000000-0000-0000-0000-000000000001",
        "--status",
        "succeeded",
        "--since",
        "2026-01-01T00:00:00Z",
        "--before",
        "2026-02-01T00:00:00Z",
        "--limit",
        "25",
    ])
    .expect("audit list args should parse");
    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert!(
        request.path.starts_with("/admin/audit?"),
        "path should include query string"
    );
    // Each filter must appear in the path. Colons in ISO timestamps are left
    // unencoded by query_encode (matches the server's stable key:value shape).
    for fragment in &[
        "actor=alice%40example.com",
        "operation=workflow.cancel",
        "target_type=workflow",
        "target_id=00000000-0000-0000-0000-000000000001",
        "status=succeeded",
        "since=2026-01-01T00:00:00Z",
        "before=2026-02-01T00:00:00Z",
        "limit=25",
    ] {
        assert!(
            request.path.contains(fragment),
            "expected fragment '{fragment}' not found in '{}'",
            request.path
        );
    }
    assert_eq!(request.body, None);
}

#[test]
fn audit_list_partial_filters() {
    let cli = Cli::try_parse_from([
        "harvest", "audit", "list", "--status", "failed", "--limit", "10",
    ])
    .expect("audit list partial args should parse");
    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Get);
    assert!(request.path.contains("status=failed"));
    assert!(request.path.contains("limit=10"));
    assert!(!request.path.contains("actor="), "actor should be absent");
}

// ── schedule backfill (issue #177) ──────────────────────────────────────────

#[test]
fn schedule_backfill_maps_to_post_backfill_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000042",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
    ])
    .expect("schedule backfill args should parse");

    let request = cli
        .api_request()
        .expect("schedule backfill request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/admin/schedules/00000000-0000-0000-0000-000000000042/backfill"
    );
    let body = request.body.expect("backfill request must have a body");
    assert_eq!(body["from"], "2026-04-01T00:00:00Z");
    assert_eq!(body["to"], "2026-04-08T00:00:00Z");
    assert_eq!(body["dry_run"], false);
    assert_eq!(body["include_paused"], false);
}

#[test]
fn schedule_backfill_dry_run_flag_sets_body_field() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000099",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
        "--dry-run",
    ])
    .expect("backfill --dry-run args should parse");

    let request = cli
        .api_request()
        .expect("backfill dry-run request should build");
    let body = request.body.expect("dry-run request must have a body");

    assert_eq!(body["dry_run"], true);
    assert_eq!(body["from"], "2026-04-01T00:00:00Z");
    assert_eq!(body["to"], "2026-04-08T00:00:00Z");
}

#[test]
fn schedule_backfill_max_count_appears_in_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
        "--max-count",
        "50",
    ])
    .expect("backfill --max-count args should parse");

    let request = cli.api_request().expect("request should build");
    let body = request.body.expect("request must have a body");

    assert_eq!(body["max_count"], json!(50u64));
}

#[test]
fn schedule_backfill_include_paused_appears_in_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--from",
        "2026-04-01T00:00:00Z",
        "--to",
        "2026-04-08T00:00:00Z",
        "--include-paused",
    ])
    .expect("backfill --include-paused args should parse");

    let request = cli.api_request().expect("request should build");
    let body = request.body.expect("request must have a body");

    assert_eq!(body["include_paused"], true);
}

#[test]
fn schedule_backfill_missing_from_is_rejected_by_clap() {
    let result = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--to",
        "2026-04-08T00:00:00Z",
    ]);
    assert!(
        result.is_err(),
        "--from is required and its absence should be rejected"
    );
}

#[test]
fn schedule_backfill_missing_to_is_rejected_by_clap() {
    let result = Cli::try_parse_from([
        "harvest",
        "schedule",
        "backfill",
        "00000000-0000-0000-0000-000000000001",
        "--from",
        "2026-04-01T00:00:00Z",
    ]);
    assert!(
        result.is_err(),
        "--to is required and its absence should be rejected"
    );
}

#[test]
fn schedule_runs_maps_to_get_runs_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "runs",
        "00000000-0000-0000-0000-000000000042",
    ])
    .expect("schedule runs args should parse");

    let request = cli
        .api_request()
        .expect("schedule runs request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/schedules/00000000-0000-0000-0000-000000000042/runs"
    );
    assert!(request.body.is_none());
}

#[test]
fn schedule_runs_threads_filters_into_query() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "runs",
        "00000000-0000-0000-0000-000000000042",
        "--state",
        "FAILED",
        "--state",
        "TIMED_OUT",
        "--origin",
        "scheduled",
        "--since",
        "24h",
        "--limit",
        "50",
    ])
    .expect("schedule runs filter args should parse");

    let request = cli
        .api_request()
        .expect("schedule runs request should build");
    assert_eq!(request.method, ApiMethod::Get);
    let path = request.path;
    assert!(path.starts_with("/admin/schedules/00000000-0000-0000-0000-000000000042/runs?"));
    assert!(path.contains("state=FAILED"), "path was {path}");
    assert!(path.contains("state=TIMED_OUT"), "path was {path}");
    assert!(path.contains("origin=scheduled"), "path was {path}");
    assert!(path.contains("since=24h"), "path was {path}");
    assert!(path.contains("limit=50"), "path was {path}");
}

#[test]
fn canary_maps_to_management_api_request() {
    let cli = Cli::try_parse_from([
        "harvest",
        "canary",
        "--sample-size",
        "200",
        "--workflow-name",
        "checkout_workflow",
        "--queue",
        "orders_queue",
    ])
    .expect("canary args should parse");

    let request = cli.api_request().expect("canary request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/workflows/replay-canary");
    assert_eq!(
        request.body,
        Some(json!({
            "sample_size": 200,
            "workflow_name": "checkout_workflow",
            "queue_name": "orders_queue",
        }))
    );
}

#[test]
fn workflow_retry_activity_maps_to_post_retry_now_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "retry-activity",
        "exec-123",
        "act-456",
    ])
    .expect("retry-activity args should parse");

    let request = cli
        .api_request()
        .expect("retry-activity request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/exec-123/activities/act-456/retry-now"
    );
    assert_eq!(request.body, None, "retry-activity sends no body");
}
