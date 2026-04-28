use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;
use serde_json::json;

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
