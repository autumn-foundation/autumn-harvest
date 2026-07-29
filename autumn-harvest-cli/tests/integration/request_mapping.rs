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

    let timeline = Cli::try_parse_from([
        "harvest",
        "workflow",
        "timeline",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow timeline args should parse");
    let timeline_request = timeline
        .api_request()
        .expect("timeline request should build");
    assert_eq!(timeline_request.method, ApiMethod::Get);
    assert_eq!(
        timeline_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/timeline"
    );
    assert_eq!(timeline_request.body, None);

    let awaitables = Cli::try_parse_from([
        "harvest",
        "workflow",
        "awaitables",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow awaitables args should parse");
    let awaitables_request = awaitables
        .api_request()
        .expect("awaitables request should build");
    assert_eq!(awaitables_request.method, ApiMethod::Get);
    assert_eq!(
        awaitables_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/awaitables"
    );
    assert_eq!(awaitables_request.body, None);

    let run_chain = Cli::try_parse_from([
        "harvest",
        "workflow",
        "run-chain",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow run-chain args should parse");
    let run_chain_request = run_chain
        .api_request()
        .expect("run-chain request should build");
    assert_eq!(run_chain_request.method, ApiMethod::Get);
    assert_eq!(
        run_chain_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/run-chain"
    );
    assert_eq!(run_chain_request.body, None);

    let replay_diagnosis = Cli::try_parse_from([
        "harvest",
        "workflow",
        "replay-diagnosis",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow replay-diagnosis args should parse");
    let replay_diagnosis_request = replay_diagnosis
        .api_request()
        .expect("replay-diagnosis request should build");
    assert_eq!(replay_diagnosis_request.method, ApiMethod::Post);
    assert_eq!(
        replay_diagnosis_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/replay-diagnosis"
    );
    assert_eq!(replay_diagnosis_request.body, None);
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
fn workflow_list_start_source_filter_maps_to_query_string() {
    // Issue #740: `--start-source` forwards a single bounded provenance value
    // verbatim to the `start_source` query param. The CLI does not validate it
    // (the server 400s an unknown value), matching how `--state` passes through.
    let list = Cli::try_parse_from(["harvest", "workflow", "list", "--start-source", "schedule"])
        .expect("start-source list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/workflows?start_source=schedule");
    assert_eq!(request.body, None);
}

#[test]
fn workflow_summaries_maps_to_get_request() {
    let cli = Cli::try_parse_from(["harvest", "workflow", "summaries"])
        .expect("summaries args should parse");
    let request = cli.api_request().expect("summaries request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/workflows/summaries");
    assert_eq!(request.body, None);
}

#[test]
fn workflow_summaries_filters_map_to_query_string() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "summaries",
        "--workflow-name",
        "onboarding",
        "--workflow-id",
        "acme-1",
        "--state",
        "COMPLETED,FAILED",
        "--completed-after",
        "2026-01-01T00:00:00Z",
        "--completed-before",
        "2026-12-31T23:59:59Z",
        "--search-attr",
        "tenant=acme",
        "--limit",
        "100",
        "--cursor",
        "2026-05-04T12:00:00Z|00000000-0000-0000-0000-000000000099",
    ])
    .expect("filtered summaries args should parse");
    let request = cli.api_request().expect("summaries request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/workflows/summaries?workflow_name=onboarding&workflow_id=acme-1\
         &state=COMPLETED,FAILED&completed_after=2026-01-01T00:00:00Z\
         &completed_before=2026-12-31T23:59:59Z&search_attr=tenant:acme&limit=100\
         &cursor=2026-05-04T12:00:00Z%7C00000000-0000-0000-0000-000000000099"
    );
    assert_eq!(request.body, None);
}

#[test]
fn workflow_summaries_order_maps_to_query_string() {
    let cli = Cli::try_parse_from(["harvest", "workflow", "summaries", "--order", "asc"])
        .expect("summaries --order args should parse");
    let request = cli.api_request().expect("summaries request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/workflows/summaries?order=asc");
    assert_eq!(request.body, None);
}

#[test]
fn workflow_summaries_reject_invalid_order() {
    // Restricted at clap parse time to asc|desc.
    let parsed = Cli::try_parse_from(["harvest", "workflow", "summaries", "--order", "sideways"]);
    assert!(parsed.is_err(), "invalid --order value must be rejected");
}

#[test]
fn workflow_summaries_reject_search_attr_without_equals() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "summaries",
        "--search-attr",
        "tenant",
    ])
    .expect("summaries args should parse");
    assert!(
        cli.api_request().is_err(),
        "malformed --search-attr should error before building the request"
    );
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
fn workflow_signal_idempotency_key_maps_to_query_param() {
    // Issue #753: the CLI signal subcommand reaches parity with the HTTP
    // surface (issue #521) by mapping --idempotency-key onto the
    // ?idempotency_key= query param of the plain signal route.
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "approved",
        "--payload-json",
        r#"{"approved":true}"#,
        "--idempotency-key",
        "evt_abc123",
    ])
    .expect("workflow signal args with --idempotency-key should parse");
    let request = cli.api_request().expect("signal request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/signal/approved?idempotency_key=evt_abc123"
    );
    assert_eq!(request.body, Some(json!({ "approved": true })));
}

#[test]
fn workflow_signal_idempotency_key_is_query_encoded() {
    // Keys derived from upstream event ids may carry reserved characters —
    // they must be RFC 3986 query-encoded, mirroring the other query params.
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "approved",
        "--idempotency-key",
        "evt 1/2&3",
    ])
    .expect("workflow signal args should parse");
    let request = cli.api_request().expect("signal request should build");

    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/signal/approved?idempotency_key=evt%201%2F2%263"
    );
}

#[test]
fn workflow_signal_empty_idempotency_key_is_rejected() {
    // Mirror the server's header semantics (issue #521): a present but empty
    // Idempotency-Key is rejected rather than silently degraded to
    // at-least-once — the server treats an empty ?idempotency_key= as
    // omitted, so a client that intended exactly-once must never send one.
    for empty in ["", "   "] {
        let result = Cli::try_parse_from([
            "harvest",
            "workflow",
            "signal",
            "00000000-0000-0000-0000-000000000001",
            "approved",
            "--idempotency-key",
            empty,
        ]);
        let message = match result {
            Err(e) => e.to_string(),
            Ok(cli) => match cli.api_request() {
                Err(e) => e.to_string(),
                Ok(req) => panic!(
                    "empty --idempotency-key {empty:?} must be rejected, but mapped to {}",
                    req.path
                ),
            },
        };
        assert!(
            message.contains("idempotency"),
            "rejection must name the flag, got: {message}"
        );
    }
}

#[test]
fn workflow_signal_without_idempotency_key_omits_query_param() {
    // AC (issue #753): omitting the key preserves today's at-least-once
    // behavior exactly — no query param is sent at all.
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "signal",
        "00000000-0000-0000-0000-000000000001",
        "approved",
    ])
    .expect("workflow signal args should parse");
    let request = cli.api_request().expect("signal request should build");

    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/signal/approved"
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
fn dlq_bulk_replay_with_cause_filters_maps_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-replay",
        "--dlq-reason",
        "poison_pill",
        "--error-class",
        "CircuitOpen",
        "--failure-signature",
        "connection refused",
    ])
    .expect("bulk-replay cause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/replay");
    let body = request.body.expect("should have body");
    assert_eq!(body["dlq_reason"], "poison_pill");
    assert_eq!(body["error_class"], "CircuitOpen");
    assert_eq!(body["failure_signature"], "connection refused");
}

#[test]
fn dlq_bulk_replay_with_queue_and_min_attempts_maps_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-replay",
        "--queue-name",
        "low-pri",
        "--min-attempts",
        "3",
    ])
    .expect("bulk-replay queue/min-attempts args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/replay");
    let body = request.body.expect("should have body");
    assert_eq!(body["queue_name"], "low-pri");
    assert_eq!(body["min_attempts"], 3);
}

#[test]
fn dlq_bulk_discard_with_queue_and_min_attempts_maps_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-discard",
        "--queue-name",
        "low-pri",
        "--min-attempts",
        "5",
    ])
    .expect("bulk-discard queue/min-attempts args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/discard");
    let body = request.body.expect("should have body");
    assert_eq!(body["queue_name"], "low-pri");
    assert_eq!(body["min_attempts"], 5);
}

#[test]
fn dlq_bulk_discard_with_cause_filters_maps_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "dlq",
        "bulk-discard",
        "--dlq-reason",
        "workflow_task_timeout",
        "--error-class",
        "WorkflowTaskTimeout",
        "--failure-signature",
        "task timed out",
    ])
    .expect("bulk-discard cause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/dead-letters/discard");
    let body = request.body.expect("should have body");
    assert_eq!(body["dlq_reason"], "workflow_task_timeout");
    assert_eq!(body["error_class"], "WorkflowTaskTimeout");
    assert_eq!(body["failure_signature"], "task timed out");
}

#[test]
fn dlq_summary_alias_maps_like_aggregate() {
    let summary = Cli::try_parse_from(["harvest", "dlq", "summary", "--group-by", "dlq_reason"])
        .expect("dlq summary should parse")
        .api_request()
        .expect("summary request should build");

    let aggregate =
        Cli::try_parse_from(["harvest", "dlq", "aggregate", "--group-by", "dlq_reason"])
            .expect("dlq aggregate should parse")
            .api_request()
            .expect("aggregate request should build");

    assert_eq!(summary.method, ApiMethod::Get);
    assert_eq!(summary.method, aggregate.method);
    assert_eq!(summary.path, aggregate.path);
    assert!(
        summary.path.starts_with("/dead-letters/aggregate?"),
        "unexpected path: {}",
        summary.path
    );
    assert!(
        summary.path.contains("group_by=dlq_reason"),
        "unexpected path: {}",
        summary.path
    );
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

// ── schedule update (issue #771) ─────────────────────────────────────────────

#[test]
fn schedule_update_maps_to_patch_route_with_partial_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000042",
        "--cron",
        "0 3 * * *",
        "--tz",
        "America/New_York",
        "--input-json",
        r#"{"env":"prod"}"#,
    ])
    .expect("schedule update args should parse");

    let request = cli
        .api_request()
        .expect("schedule update request should build");

    assert_eq!(request.method, ApiMethod::Patch);
    assert_eq!(
        request.path,
        "/admin/schedules/00000000-0000-0000-0000-000000000042"
    );
    let body = request.body.expect("update request must have a body");
    assert_eq!(body["schedule_expr"], "0 3 * * *");
    assert_eq!(body["timezone"], "America/New_York");
    assert_eq!(body["input"], json!({"env": "prod"}));
    // Only provided flags appear in the body — partial semantics.
    let obj = body.as_object().expect("body must be an object");
    assert!(
        !obj.contains_key("queue_name"),
        "absent flags must be omitted"
    );
    assert!(!obj.contains_key("max_active_runs"));
    assert!(
        !obj.contains_key("workflow_name"),
        "workflow_name is not editable"
    );
}

#[test]
fn schedule_update_interval_and_clear_flags_map_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000042",
        "--interval-secs",
        "120",
        "--clear-calendar",
        "--clear-end-at",
        "--clear-max-runs",
        "--queue",
        "etl-workers",
        "--jitter-secs",
        "30",
        "--max-active-runs",
        "4",
        "--overlap-policy",
        "buffer_all",
        "--buffer-all-max",
        "12",
        "--catchup-policy",
        "window",
        "--catchup-window-secs",
        "7200",
    ])
    .expect("schedule update interval args should parse");

    let request = cli
        .api_request()
        .expect("schedule update request should build");

    assert_eq!(request.method, ApiMethod::Patch);
    let body = request.body.expect("update request must have a body");
    assert_eq!(body["schedule_expr"], "interval:120");
    assert_eq!(body["queue_name"], "etl-workers");
    assert_eq!(body["jitter_secs"], 30);
    assert_eq!(body["max_active_runs"], 4);
    assert_eq!(body["overlap_policy"], "buffer_all");
    assert_eq!(body["buffer_all_max"], 12);
    assert_eq!(body["catchup_policy"], "window");
    assert_eq!(body["catchup_window_secs"], 7200);
    // --clear-* flags send explicit JSON null (tri-state clear).
    let obj = body.as_object().expect("body must be an object");
    assert!(obj.contains_key("calendar") && body["calendar"].is_null());
    assert!(obj.contains_key("end_at") && body["end_at"].is_null());
    assert!(obj.contains_key("max_runs") && body["max_runs"].is_null());
}

#[test]
fn schedule_update_manual_and_bounds_map_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000042",
        "--manual",
        "--calendar",
        "us-holidays",
        "--end-at",
        "2030-01-01T00:00:00Z",
        "--max-runs",
        "24",
    ])
    .expect("schedule update manual args should parse");

    let request = cli
        .api_request()
        .expect("schedule update request should build");

    let body = request.body.expect("update request must have a body");
    assert_eq!(body["schedule_expr"], "manual");
    assert_eq!(body["calendar"], "us-holidays");
    assert_eq!(body["end_at"], "2030-01-01T00:00:00Z");
    assert_eq!(body["max_runs"], 24);
}

#[test]
fn schedule_update_no_flags_sends_empty_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "schedule",
        "update",
        "00000000-0000-0000-0000-000000000042",
    ])
    .expect("schedule update args should parse");

    let request = cli
        .api_request()
        .expect("schedule update request should build");
    assert_eq!(request.method, ApiMethod::Patch);
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn schedule_update_conflicting_expr_flags_are_rejected_by_clap() {
    assert!(
        Cli::try_parse_from([
            "harvest",
            "schedule",
            "update",
            "00000000-0000-0000-0000-000000000042",
            "--cron",
            "0 3 * * *",
            "--interval-secs",
            "60",
        ])
        .is_err(),
        "--cron and --interval-secs must conflict"
    );
    assert!(
        Cli::try_parse_from([
            "harvest",
            "schedule",
            "update",
            "00000000-0000-0000-0000-000000000042",
            "--manual",
            "--cron",
            "0 3 * * *",
        ])
        .is_err(),
        "--manual and --cron must conflict"
    );
}

#[test]
fn schedule_update_set_and_clear_flags_conflict() {
    assert!(
        Cli::try_parse_from([
            "harvest",
            "schedule",
            "update",
            "00000000-0000-0000-0000-000000000042",
            "--calendar",
            "us-holidays",
            "--clear-calendar",
        ])
        .is_err(),
        "--calendar and --clear-calendar must conflict"
    );
    assert!(
        Cli::try_parse_from([
            "harvest",
            "schedule",
            "update",
            "00000000-0000-0000-0000-000000000042",
            "--end-at",
            "2030-01-01T00:00:00Z",
            "--clear-end-at",
        ])
        .is_err(),
        "--end-at and --clear-end-at must conflict"
    );
    assert!(
        Cli::try_parse_from([
            "harvest",
            "schedule",
            "update",
            "00000000-0000-0000-0000-000000000042",
            "--max-runs",
            "5",
            "--clear-max-runs",
        ])
        .is_err(),
        "--max-runs and --clear-max-runs must conflict"
    );
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

#[test]
fn workflow_fail_activity_maps_to_post_fail_now_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "fail-activity",
        "exec-123",
        "act-456",
        "--reason",
        "hung in flight",
    ])
    .expect("fail-activity args should parse");

    let request = cli
        .api_request()
        .expect("fail-activity request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/exec-123/activities/act-456/fail-now"
    );
    let body = request.body.expect("fail-activity sends a body");
    assert_eq!(body["reason"], "hung in flight");
}

// ── pause / resume subcommands (issue #609) ───────────────────────────────────

#[test]
fn workflow_pause_maps_to_post_pause_route_with_reason_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "pause",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "investigating incident INC-42",
    ])
    .expect("workflow pause args should parse");
    let request = cli.api_request().expect("pause request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/pause"
    );
    assert_eq!(
        request.body,
        Some(json!({ "reason": "investigating incident INC-42" }))
    );
}

#[test]
fn workflow_pause_without_reason_sends_empty_object_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "pause",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow pause args should parse");
    let request = cli.api_request().expect("pause request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/pause"
    );
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn workflow_resume_maps_to_post_resume_route_with_no_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "resume",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow resume args should parse");
    let request = cli.api_request().expect("resume request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/resume"
    );
    assert_eq!(request.body, None, "resume sends no body");
}

// ── batch-reset subcommand (issue #538) ───────────────────────────────────────

#[test]
fn workflow_batch_reset_first_activity_maps_to_batch_reset_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"],"workflow_name":"subscription_flow"}"#,
        "--first-activity",
        "activity_x",
        "--reason",
        "post-deploy fix #1234",
        "--operator-id",
        "oncall-jane",
    ])
    .expect("batch-reset args should parse");
    let request = cli.api_request().expect("batch-reset request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/batch_reset");

    let body = request.body.unwrap();
    assert_eq!(body["filter"]["states"][0], "FAILED");
    assert_eq!(body["filter"]["workflow_name"], "subscription_flow");
    assert_eq!(body["reset_point"]["type"], "first_activity_run");
    assert_eq!(body["reset_point"]["activity_name"], "activity_x");
    assert_eq!(body["reason"], "post-deploy fix #1234");
    assert_eq!(body["operator_id"], "oncall-jane");
    assert_eq!(body["signal_reapply"], "drop");
    assert_eq!(body["preview"], false);
}

#[test]
fn workflow_batch_reset_event_id_maps_correctly() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"]}"#,
        "--event-id",
        "42",
        "--reason",
        "manual fix",
    ])
    .expect("batch-reset --event-id args should parse");
    let request = cli
        .api_request()
        .expect("batch-reset --event-id request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/batch_reset");

    let body = request.body.unwrap();
    assert_eq!(body["reset_point"]["type"], "event_id");
    assert_eq!(body["reset_point"]["event_id"], 42);
    assert_eq!(body["operator_id"], "cli");
}

#[test]
fn workflow_batch_reset_last_workflow_task_maps_correctly() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"]}"#,
        "--last-workflow-task",
        "--reason",
        "recover from stuck state",
    ])
    .expect("batch-reset --last-workflow-task args should parse");
    let request = cli
        .api_request()
        .expect("batch-reset --last-workflow-task request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/batch_reset");

    let body = request.body.unwrap();
    assert_eq!(body["reset_point"]["type"], "last_workflow_task");
}

#[test]
fn workflow_batch_reset_preview_flag_sets_preview_true() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"]}"#,
        "--first-activity",
        "my_activity",
        "--reason",
        "dry run check",
        "--preview",
    ])
    .expect("batch-reset --preview args should parse");
    let request = cli
        .api_request()
        .expect("batch-reset --preview request should build");

    let body = request.body.unwrap();
    assert_eq!(body["preview"], true);
}

#[test]
fn workflow_batch_reset_signal_reapply_buffer_maps_correctly() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"]}"#,
        "--first-activity",
        "my_activity",
        "--reason",
        "fix",
        "--signal-reapply",
        "buffer",
    ])
    .expect("batch-reset --signal-reapply buffer args should parse");
    let request = cli
        .api_request()
        .expect("batch-reset --signal-reapply buffer request should build");

    let body = request.body.unwrap();
    assert_eq!(body["signal_reapply"], "buffer");
}

#[test]
fn workflow_batch_reset_no_point_flag_returns_error() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "batch-reset",
        "--filter-json",
        r#"{"states":["FAILED"]}"#,
        "--reason",
        "fix",
    ])
    .expect("batch-reset parse should succeed (missing point is a runtime error)");
    let result = cli.api_request();
    assert!(
        result.is_err(),
        "batch-reset without a reset-point flag should return an error"
    );
}

// ── Build routing ramp (issue #604) ────────────────────────────────────────

#[test]
fn build_ramp_set_maps_to_post_request_with_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "build",
        "ramp",
        "set",
        "--queue",
        "default",
        "--target-build-id",
        "canary-v2",
        "--percent",
        "25",
    ])
    .expect("build ramp set args should parse");

    let request = cli
        .api_request()
        .expect("build ramp set request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/build-routing/ramp");
    let body = request.body.expect("ramp set must send a body");
    assert_eq!(body["queue_name"], "default");
    assert_eq!(body["target_build_id"], "canary-v2");
    assert_eq!(body["ramp_percent"], 25);
}

#[test]
fn build_ramp_show_maps_to_get_request() {
    let cli = Cli::try_parse_from(["harvest", "build", "ramp", "show"])
        .expect("build ramp show args should parse");

    let request = cli
        .api_request()
        .expect("build ramp show request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/build-routing");
    assert_eq!(request.body, None);
}

#[test]
fn build_ramp_clear_maps_to_delete_request() {
    let cli = Cli::try_parse_from(["harvest", "build", "ramp", "clear", "--queue", "default"])
        .expect("build ramp clear args should parse");

    let request = cli
        .api_request()
        .expect("build ramp clear request should build");

    assert_eq!(request.method, ApiMethod::Delete);
    assert_eq!(request.path, "/admin/build-routing/ramp/default");
    assert_eq!(request.body, None);
}

#[test]
fn build_ramp_set_rejects_percent_out_of_range_at_parse_or_request_time() {
    let cli = Cli::try_parse_from([
        "harvest",
        "build",
        "ramp",
        "set",
        "--queue",
        "default",
        "--target-build-id",
        "canary-v2",
        "--percent",
        "101",
    ]);
    match cli {
        Err(_) => {} // rejected at clap parse time — acceptable
        Ok(cli) => {
            let result = cli.api_request();
            assert!(
                result.is_err(),
                "percent=101 must be rejected before an HTTP request is built"
            );
        }
    }
}

#[test]
fn legal_hold_set_maps_to_post_with_reason_and_until() {
    let cli = Cli::try_parse_from([
        "harvest",
        "legal-hold",
        "set",
        "exec-1",
        "--reason",
        "litigation hold",
        "--until",
        "2027-01-01T00:00:00Z",
    ])
    .expect("legal-hold set args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/exec-1/legal-hold");
    assert_eq!(
        request.body,
        Some(json!({
            "reason": "litigation hold",
            "hold_until": "2027-01-01T00:00:00Z",
        }))
    );
}

#[test]
fn legal_hold_set_without_until_omits_hold_until() {
    let cli = Cli::try_parse_from(["harvest", "legal-hold", "set", "exec-1", "--reason", "hold"])
        .expect("legal-hold set args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/exec-1/legal-hold");
    assert_eq!(request.body, Some(json!({ "reason": "hold" })));
}

#[test]
fn legal_hold_release_maps_to_post_with_no_body() {
    let cli = Cli::try_parse_from(["harvest", "legal-hold", "release", "exec-1"])
        .expect("legal-hold release args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/workflows/exec-1/legal-hold/release");
    assert_eq!(request.body, None);
}

// ── Task-queue pause/resume (issue #619) ──────────────────────────────────────

#[test]
fn queue_pause_maps_to_post_with_reason() {
    let cli = Cli::try_parse_from([
        "harvest",
        "queue",
        "pause",
        "email-workers",
        "--reason",
        "SMTP provider outage",
    ])
    .expect("queue pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/queues/email-workers/pause");
    assert_eq!(
        request.body,
        Some(json!({ "reason": "SMTP provider outage" })),
        "omitting --shard-id must send NO shard_id field: the default is a \
         fleet-wide hold, not shard 0"
    );
}

#[test]
fn queue_pause_with_shard_id_scopes_the_hold() {
    let cli = Cli::try_parse_from([
        "harvest",
        "queue",
        "pause",
        "email-workers",
        "--reason",
        "one shard only",
        "--shard-id",
        "2",
    ])
    .expect("queue pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.body,
        Some(json!({ "reason": "one shard only", "shard_id": 2 }))
    );
}

#[test]
fn queue_resume_maps_to_post_with_empty_body() {
    let cli = Cli::try_parse_from(["harvest", "queue", "resume", "email-workers"])
        .expect("queue resume args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/queues/email-workers/resume");
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn queue_list_paused_maps_to_the_read_route() {
    let cli = Cli::try_parse_from(["harvest", "queue", "list-paused"])
        .expect("queue list-paused args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/queues/paused");
    assert_eq!(request.body, None);
}

#[test]
fn queue_pause_percent_encodes_the_queue_name() {
    let cli = Cli::try_parse_from([
        "harvest",
        "queue",
        "pause",
        "email workers/eu",
        "--reason",
        "x",
    ])
    .expect("queue pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.path, "/admin/queues/email%20workers%2Feu/pause",
        "a queue name with a space or slash must not break out of the path segment"
    );
}

#[test]
fn queue_pause_requires_a_reason() {
    // A hold with no stated cause is unauditable -- clap must reject it.
    assert!(
        Cli::try_parse_from(["harvest", "queue", "pause", "email-workers"]).is_err(),
        "queue pause must require --reason"
    );
}

// ── Scoped API tokens (issue #942) ────────────────────────────────────────────

#[test]
fn token_create_maps_to_post() {
    let cli = Cli::try_parse_from(["harvest", "token", "create", "ci-bot", "--scope", "read"])
        .expect("token create args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/tokens");
    assert_eq!(
        request.body,
        Some(json!({ "name": "ci-bot", "scope": "read" }))
    );
}

#[test]
fn token_create_with_expiry_includes_expires_at() {
    let cli = Cli::try_parse_from([
        "harvest",
        "token",
        "create",
        "dash",
        "--scope",
        "mutate",
        "--expires-at",
        "2027-01-01T00:00:00Z",
    ])
    .expect("token create args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/tokens");
    assert_eq!(
        request.body,
        Some(json!({
            "name": "dash",
            "scope": "mutate",
            "expires_at": "2027-01-01T00:00:00Z",
        }))
    );
}

#[test]
fn token_create_defaults_scope_to_read() {
    let cli = Cli::try_parse_from(["harvest", "token", "create", "reader"])
        .expect("token create args should parse");

    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/tokens");
    assert_eq!(
        request.body,
        Some(json!({ "name": "reader", "scope": "read" }))
    );
}

#[test]
fn token_list_maps_to_get() {
    let cli = Cli::try_parse_from(["harvest", "token", "list"]).expect("token list should parse");
    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/tokens");
    assert_eq!(request.body, None);
}

#[test]
fn token_revoke_maps_to_delete() {
    let cli = Cli::try_parse_from(["harvest", "token", "revoke", "abc-123"])
        .expect("token revoke should parse");
    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Delete);
    assert_eq!(request.path, "/admin/tokens/abc-123");
    assert_eq!(request.body, None);
}

#[test]
fn token_rotate_maps_to_post_create_replacement() {
    // AC8 convenience: rotate mints a replacement via the create route (no
    // dedicated server route). The old token is revoked as a documented second
    // step; the CLI maps rotate → POST /admin/tokens.
    let cli = Cli::try_parse_from(["harvest", "token", "rotate", "old-id", "--scope", "mutate"])
        .expect("token rotate should parse");
    let request = cli.api_request().expect("request should build");
    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/admin/tokens");
}
