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

/// issue #697: `--residency-key` maps to the `residency_key` body field, and an
/// unpinned start's body stays byte-identical to a pre-#697 CLI.
#[test]
fn workflow_start_residency_key_maps_to_the_body_field() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "start",
        "approval_workflow",
        "--workflow-id",
        "approval-42",
        "--residency-key",
        "eu",
    ])
    .expect("workflow start args should parse");

    let request = cli.api_request().expect("request should build");
    assert_eq!(request.path, "/workflows/approval_workflow/start");
    assert_eq!(
        request.body,
        Some(json!({
            "workflow_id": "approval-42",
            "residency_key": "eu",
        }))
    );
}

/// issue #697: `--shard-id` maps to a numeric `shard_id` body field.
#[test]
fn workflow_start_shard_id_maps_to_a_numeric_body_field() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "start",
        "approval_workflow",
        "--shard-id",
        "2",
    ])
    .expect("workflow start args should parse");

    let request = cli.api_request().expect("request should build");
    assert_eq!(request.body, Some(json!({ "shard_id": 2 })));
}

/// Lineage tree (issue #621) maps onto `GET /workflows/{id}/tree`, with every
/// bound carried as a query param so an omitted flag inherits the *server's*
/// documented default rather than a second copy of it in the CLI.
#[test]
fn workflow_tree_maps_bounds_onto_query_params() {
    // Bare form sends no query string at all, so the server's documented
    // defaults (max_depth 20 / max_nodes 1000) apply.
    let tree = Cli::try_parse_from([
        "harvest",
        "workflow",
        "tree",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow tree args should parse");
    let tree_request = tree.api_request().expect("tree request should build");
    assert_eq!(tree_request.method, ApiMethod::Get);
    assert_eq!(
        tree_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/tree"
    );
    assert_eq!(tree_request.body, None);

    let tree_summary = Cli::try_parse_from([
        "harvest",
        "workflow",
        "tree",
        "00000000-0000-0000-0000-000000000001",
        "--summary",
        "--max-depth",
        "3",
        "--max-nodes",
        "50",
    ])
    .expect("workflow tree flags should parse");
    let tree_summary_request = tree_summary
        .api_request()
        .expect("tree summary request should build");
    assert_eq!(tree_summary_request.method, ApiMethod::Get);
    assert_eq!(
        tree_summary_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/tree\
         ?summary=true&max_depth=3&max_nodes=50"
    );
    assert_eq!(tree_summary_request.body, None);
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

/// Durable per-execution author logs (issue #790) — `harvest workflow logs`.
///
/// Its own test rather than more assertions on
/// `workflow_list_and_query_use_get_requests`: that function is already at the
/// `clippy::too_many_lines` ceiling, and the logs route has three distinct
/// query-shaping behaviours worth naming (bare, fully-filtered, repeated
/// `--level`).
#[test]
fn workflow_logs_maps_to_the_logs_route_with_query_filters() {
    let logs = Cli::try_parse_from([
        "harvest",
        "workflow",
        "logs",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow logs args should parse");
    let logs_request = logs.api_request().expect("logs request should build");
    assert_eq!(logs_request.method, ApiMethod::Get);
    assert_eq!(
        logs_request.path, "/workflows/00000000-0000-0000-0000-000000000001/logs",
        "no flags must send no query string at all"
    );
    assert_eq!(logs_request.body, None);

    // Every flag, including a comma-separated --level that must expand into
    // one `level=` param per value.
    let logs_filtered = Cli::try_parse_from([
        "harvest",
        "workflow",
        "logs",
        "00000000-0000-0000-0000-000000000001",
        "--level",
        "warn,error",
        "--limit",
        "50",
        "--cursor",
        "17",
        "--since",
        "2026-05-06T00:00:00Z",
    ])
    .expect("workflow logs filter args should parse");
    let logs_filtered_request = logs_filtered
        .api_request()
        .expect("filtered logs request should build");
    assert_eq!(
        logs_filtered_request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/logs\
         ?level=warn&level=error&limit=50&cursor=17&since=2026-05-06T00:00:00Z"
    );
    assert_eq!(logs_filtered_request.body, None);

    // A repeated --level flag is equivalent to the comma-separated form.
    let logs_repeated = Cli::try_parse_from([
        "harvest",
        "workflow",
        "logs",
        "00000000-0000-0000-0000-000000000001",
        "--level",
        "warn",
        "--level",
        "error",
    ])
    .expect("repeated --level should parse");
    assert_eq!(
        logs_repeated
            .api_request()
            .expect("repeated-level request should build")
            .path,
        "/workflows/00000000-0000-0000-0000-000000000001/logs?level=warn&level=error"
    );
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
fn workflow_list_history_bloat_min_events_maps_to_query_string() {
    // Issue #704: operator early-warning discovery for workflow history bloat.
    // Distinct query param from the server's pre-existing, general-purpose
    // `min_history_events` filter (issue #493) -- reusing that name broke
    // callers combining it with state=/pagination (PR #1139 review). The CLI
    // forwards the raw value verbatim -- the server owns validation
    // (non-numeric/negative -> 400) and the non-terminal + sorted-by-size
    // discovery behavior.
    let list = Cli::try_parse_from([
        "harvest",
        "workflow",
        "list",
        "--history-bloat-min-events",
        "5000",
    ])
    .expect("history-bloat-min-events list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/workflows?history_bloat_min_events=5000");
    assert_eq!(request.body, None);
}

#[test]
fn workflow_list_omits_history_bloat_min_events_by_default() {
    let list = Cli::try_parse_from(["harvest", "workflow", "list"])
        .expect("default list args should parse");
    let request = list.api_request().expect("list request should build");

    assert_eq!(request.path, "/workflows");
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

/// Issue #798: the sample export maps onto the stratified admin route with the
/// per-type cap, the caller-specified non-terminal states, and the payload
/// policy — the three inputs a CI drift gate actually varies.
#[test]
fn history_export_sample_maps_to_the_stratified_admin_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "history",
        "export-sample",
        "--per-workflow",
        "25",
        "--states",
        "RUNNING",
        "--workflow-name",
        "billing_checkout",
        "--order",
        "newest",
        "--shard-id",
        "2",
        "--payload-policy",
        "full",
        "--max-bytes",
        "1048576",
        "--output-dir",
        "./fixtures",
    ])
    .expect("sample export args should parse");
    let request = cli
        .api_request()
        .expect("sample export request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/history/export-sample?workflow_name=billing_checkout&states=RUNNING\
         &per_workflow=25&order=newest&shard_id=2&payload_policy=full&max_bytes=1048576"
    );
    assert_eq!(request.body, None);
}

/// Omitting every optional flag must still send the two defaults the AC names
/// (`per_workflow=50`, redacted payloads) — never an unbounded request.
#[test]
fn history_export_sample_defaults_are_explicit_on_the_wire() {
    let cli = Cli::try_parse_from([
        "harvest",
        "history",
        "export-sample",
        "--output-dir",
        "./fixtures",
    ])
    .expect("sample export args should parse");
    let request = cli
        .api_request()
        .expect("sample export request should build");

    assert_eq!(
        request.path,
        "/admin/history/export-sample?per_workflow=50&order=oldest&payload_policy=redacted"
    );
}

/// Repeating `--states` accumulates rather than overwriting, so
/// `--states RUNNING --states PAUSED` is not silently narrowed to one state.
#[test]
fn history_export_sample_accumulates_repeated_states() {
    let cli = Cli::try_parse_from([
        "harvest",
        "history",
        "export-sample",
        "--states",
        "RUNNING",
        "--states",
        "PAUSED",
        "--output-dir",
        "./fixtures",
    ])
    .expect("sample export args should parse");
    let request = cli
        .api_request()
        .expect("sample export request should build");

    // `,` is deliberately left unencoded by `query_encode`, matching every other
    // repeatable/comma-separated management-API param.
    assert!(
        request.path.contains("states=RUNNING,PAUSED"),
        "repeated --states must accumulate: {}",
        request.path
    );
}

/// The comma-separated idiom must produce the identical wire shape as the
/// repeated-flag idiom, so a CI recipe copied from either samples the same
/// population.
#[test]
fn history_export_sample_comma_separated_states_match_repeated_flags() {
    let repeated = Cli::try_parse_from([
        "harvest",
        "history",
        "export-sample",
        "--states",
        "RUNNING",
        "--states",
        "PAUSED",
        "--output-dir",
        "./fixtures",
    ])
    .expect("parse")
    .api_request()
    .expect("build");
    let comma = Cli::try_parse_from([
        "harvest",
        "history",
        "export-sample",
        "--states",
        "RUNNING,PAUSED",
        "--output-dir",
        "./fixtures",
    ])
    .expect("parse")
    .api_request()
    .expect("build");

    assert_eq!(repeated.path, comma.path);
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

// ── rerun (operator re-run of a terminal execution, issue #777) ───────────────

#[test]
fn workflow_rerun_maps_to_post_rerun_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "rerun",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow rerun args should parse");
    let request = cli.api_request().expect("rerun request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/rerun"
    );
}

#[test]
fn workflow_rerun_without_flags_sends_empty_object_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "rerun",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow rerun args should parse");
    let request = cli.api_request().expect("rerun request should build");

    // No `input` key at all: the server treats an absent `input` as "clone the
    // source's stored input verbatim" and an explicit JSON null as an override,
    // so the CLI must never inject a null to mean "the operator said nothing".
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn workflow_rerun_with_input_json_and_workflow_id() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "rerun",
        "00000000-0000-0000-0000-000000000001",
        "--input-json",
        r#"{"user_id": 7}"#,
        "--workflow-id",
        "order-42-retry",
    ])
    .expect("workflow rerun args should parse");
    let request = cli.api_request().expect("rerun request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.body,
        Some(json!({ "input": { "user_id": 7 }, "workflow_id": "order-42-retry" }))
    );
}

#[test]
fn workflow_rerun_with_input_file_and_workflow_id() {
    let mut tmp = tempfile::NamedTempFile::new().expect("tmp file");
    std::io::Write::write_all(&mut tmp, br#"{"user_id": 7, "retry": true}"#).expect("write input");
    let path = tmp.into_temp_path();
    let path_str = path.to_str().expect("temp path is utf-8");

    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "rerun",
        "00000000-0000-0000-0000-000000000001",
        "--input-file",
        path_str,
        "--workflow-id",
        "order-42-retry",
    ])
    .expect("workflow rerun args should parse");
    let request = cli.api_request().expect("rerun request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/rerun"
    );
    assert_eq!(
        request.body,
        Some(json!({
            "input": { "user_id": 7, "retry": true },
            "workflow_id": "order-42-retry",
        }))
    );
}

#[test]
fn workflow_rerun_input_json_and_input_file_conflict() {
    // Positive controls: each flag alone must parse, so the conflict assertion
    // below cannot pass merely because the subcommand or flags are unknown.
    for flag in ["--input-json", "--input-file"] {
        assert!(
            Cli::try_parse_from([
                "harvest",
                "workflow",
                "rerun",
                "00000000-0000-0000-0000-000000000001",
                flag,
                "{}",
            ])
            .is_ok(),
            "{flag} alone should parse"
        );
    }

    assert!(
        Cli::try_parse_from([
            "harvest",
            "workflow",
            "rerun",
            "00000000-0000-0000-0000-000000000001",
            "--input-json",
            "{}",
            "--input-file",
            "input.json",
        ])
        .is_err(),
        "--input-json and --input-file are mutually exclusive"
    );
}

// ── annotate (operator-mutable triage tags, issue #759) ───────────────────────

#[test]
fn workflow_annotate_sets_owner_and_severity_maps_to_patch_triage_route() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "annotate",
        "00000000-0000-0000-0000-000000000001",
        "--owner",
        "team-payments",
        "--severity",
        "P1",
    ])
    .expect("workflow annotate args should parse");
    let request = cli.api_request().expect("annotate request should build");

    assert_eq!(request.method, ApiMethod::Patch);
    assert_eq!(
        request.path,
        "/workflows/00000000-0000-0000-0000-000000000001/triage"
    );
    assert_eq!(
        request.body,
        Some(json!({ "owner": "team-payments", "severity": "P1" }))
    );
}

#[test]
fn workflow_annotate_note_only_maps_to_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "annotate",
        "00000000-0000-0000-0000-000000000001",
        "--note",
        "claimed, investigating stuck timer",
    ])
    .expect("workflow annotate note args should parse");
    let request = cli.api_request().expect("annotate request should build");

    assert_eq!(request.method, ApiMethod::Patch);
    assert_eq!(
        request.body,
        Some(json!({ "note": "claimed, investigating stuck timer" }))
    );
}

#[test]
fn workflow_annotate_clear_flags_send_explicit_null() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "annotate",
        "00000000-0000-0000-0000-000000000001",
        "--clear-owner",
        "--clear-severity",
        "--clear-note",
    ])
    .expect("workflow annotate clear args should parse");
    let request = cli.api_request().expect("annotate request should build");

    let body = request.body.expect("annotate request must have a body");
    let obj = body.as_object().expect("body must be an object");
    assert!(obj.contains_key("owner") && body["owner"].is_null());
    assert!(obj.contains_key("severity") && body["severity"].is_null());
    assert!(obj.contains_key("note") && body["note"].is_null());
}

#[test]
fn workflow_annotate_no_flags_sends_empty_body() {
    let cli = Cli::try_parse_from([
        "harvest",
        "workflow",
        "annotate",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("workflow annotate args should parse");
    let request = cli.api_request().expect("annotate request should build");

    assert_eq!(request.method, ApiMethod::Patch);
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn workflow_annotate_set_and_clear_flags_conflict() {
    for (set_flag, set_value, clear_flag) in [
        ("--owner", "team-payments", "--clear-owner"),
        ("--severity", "P1", "--clear-severity"),
        ("--note", "investigating", "--clear-note"),
    ] {
        assert!(
            Cli::try_parse_from([
                "harvest",
                "workflow",
                "annotate",
                "00000000-0000-0000-0000-000000000001",
                set_flag,
                set_value,
                clear_flag,
            ])
            .is_err(),
            "{set_flag} and {clear_flag} must conflict"
        );
    }
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
fn queue_pause_rejects_url_dot_segment_queue_names() {
    // A literal `.` or `..` queue name is NOT percent-encodable out of the
    // problem: the WHATWG URL parser that reqwest uses collapses dot-segments
    // *after* `ApiRequest.path` is built, so a `.` name silently rewrites the
    // request to `/admin/queues/pause` -- a DIFFERENT route. Rejection at
    // request-construction time is the only correct handling.
    //
    // The `%2e` spellings are additionally rejected as defense in depth: they
    // are already neutralized today by `PATH_SEGMENT_ENCODE_SET` encoding `%`
    // (they reach the URL as `%252e`), and the guard should not silently depend
    // on that staying true.
    for bad in [".", "..", "%2e", "%2E", "%2e%2e", "%2E%2E"] {
        let cli = Cli::try_parse_from(["harvest", "queue", "pause", bad, "--reason", "x"])
            .expect("queue pause args should parse");

        assert!(
            cli.api_request().is_err(),
            "queue name {bad:?} normalizes away in the URL path and must be rejected"
        );
    }
}

#[test]
fn queue_resume_rejects_url_dot_segment_queue_names() {
    // Same hazard on the destructive direction: a `..` resume must never be
    // allowed to silently target `/admin/pause`.
    for bad in [".", "..", "%2e", "%2E"] {
        let cli = Cli::try_parse_from(["harvest", "queue", "resume", bad])
            .expect("queue resume args should parse");

        assert!(
            cli.api_request().is_err(),
            "queue name {bad:?} normalizes away in the URL path and must be rejected"
        );
    }
}

#[test]
fn queue_pause_percent_encodes_a_backslash_in_the_queue_name() {
    // The URL parser treats `\` as a path separator for http/https, so an
    // unencoded backslash splits the segment and the request silently lands on
    // a different route -- and it re-enables `..` traversal inside what should
    // be one opaque segment, past the whole-segment dot-segment guard.
    // Encoding IS the right fix here (unlike the literal `.`/`..` forms):
    // `%5C` is not collapsed, so a queue whose name genuinely carries a
    // backslash stays pausable.
    let cli = Cli::try_parse_from([
        "harvest",
        "queue",
        "pause",
        r"payments\..\admin",
        "--reason",
        "x",
    ])
    .expect("queue pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.path, "/admin/queues/payments%5C..%5Cadmin/pause",
        "a backslash must not split the path segment or enable traversal"
    );
}

#[test]
fn queue_resume_percent_encodes_a_backslash_in_the_queue_name() {
    let cli = Cli::try_parse_from(["harvest", "queue", "resume", r"payments\eu"])
        .expect("queue resume args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.path, "/admin/queues/payments%5Ceu/resume");
}

#[test]
fn queue_pause_still_accepts_names_that_merely_contain_dots() {
    // Only a WHOLE-segment `.`/`..` is a dot-segment -- `a.b` is a perfectly
    // ordinary queue name and must not be swept up by the rejection.
    let cli = Cli::try_parse_from(["harvest", "queue", "pause", "orders.eu", "--reason", "x"])
        .expect("queue pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.path, "/admin/queues/orders.eu/pause");
}

#[test]
fn queue_pause_requires_a_reason() {
    // A hold with no stated cause is unauditable -- clap must reject it.
    assert!(
        Cli::try_parse_from(["harvest", "queue", "pause", "email-workers"]).is_err(),
        "queue pause must require --reason"
    );
}

// ── Per-activity-type pause/resume (issue #807) ───────────────────────────────

#[test]
fn activity_pause_maps_to_post_with_reason_and_actor() {
    let cli = Cli::try_parse_from([
        "harvest",
        "activity",
        "pause",
        "charge_card",
        "--reason",
        "payments provider outage",
        "--actor",
        "alice",
    ])
    .expect("activity pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/activities/charge_card/pause");
    assert_eq!(
        request.body,
        Some(json!({ "reason": "payments provider outage", "actor": "alice" }))
    );
}

#[test]
fn activity_pause_without_flags_sends_an_empty_body() {
    // Unlike `queue pause`, `--reason` is OPTIONAL here: containment must not
    // wait on paperwork. Omitted flags must be absent from the body entirely so
    // the server applies its own documented defaults, rather than being
    // overwritten with an empty string.
    let cli = Cli::try_parse_from(["harvest", "activity", "pause", "charge_card"])
        .expect("activity pause args should parse without --reason");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/activities/charge_card/pause");
    assert_eq!(
        request.body,
        Some(json!({})),
        "omitted --reason/--actor must send NO field, not an empty string"
    );
}

#[test]
fn activity_pause_sends_only_the_flag_that_was_given() {
    let cli = Cli::try_parse_from([
        "harvest",
        "activity",
        "pause",
        "charge_card",
        "--actor",
        "pagerduty-bot",
    ])
    .expect("activity pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.body, Some(json!({ "actor": "pagerduty-bot" })));
}

#[test]
fn activity_resume_maps_to_post_with_empty_body() {
    let cli = Cli::try_parse_from(["harvest", "activity", "resume", "charge_card"])
        .expect("activity resume args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Post);
    assert_eq!(request.path, "/activities/charge_card/resume");
    assert_eq!(request.body, Some(json!({})));
}

#[test]
fn activity_list_maps_to_the_read_route() {
    let cli = Cli::try_parse_from(["harvest", "activity", "list"])
        .expect("activity list args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/activities");
    assert_eq!(request.body, None);
}

#[test]
fn activity_list_json_flag_is_render_only_and_never_reaches_the_request() {
    // `--json` selects the OUTPUT rendering, so it must not leak into the wire
    // request as a query param or body field. Pinned because the read route is
    // shared with the default table path: a leak would send the server an
    // unknown param that a strict handler could reject.
    let plain = Cli::try_parse_from(["harvest", "activity", "list"])
        .expect("activity list args should parse")
        .api_request()
        .expect("request should build");
    let json = Cli::try_parse_from(["harvest", "activity", "list", "--json"])
        .expect("activity list --json args should parse")
        .api_request()
        .expect("request should build");

    assert_eq!(json.method, ApiMethod::Get);
    assert_eq!(json.path, "/activities");
    assert_eq!(json.body, None);
    assert_eq!(
        plain, json,
        "--json must produce a byte-identical request to the default table path"
    );
}

#[test]
fn activity_get_maps_to_the_single_read_route() {
    let cli = Cli::try_parse_from(["harvest", "activity", "get", "charge_card"])
        .expect("activity get args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/activities/charge_card");
    assert_eq!(request.body, None);
}

#[test]
fn activity_list_accepts_the_documented_aliases() {
    // `list-paused` mirrors the queue command an operator already knows, and
    // `status` mirrors the other read subcommands. Both must reach the SAME
    // route -- an alias that silently 404s is worse than no alias.
    for alias in ["list", "list-paused", "status"] {
        let cli = Cli::try_parse_from(["harvest", "activity", alias])
            .unwrap_or_else(|e| panic!("activity {alias} should parse: {e}"));
        let request = cli.api_request().expect("request should build");
        assert_eq!(
            request.path, "/activities",
            "alias {alias} must map to the read route"
        );
    }

    // The plural top-level alias, for parity with `harvest queues`.
    let cli = Cli::try_parse_from(["harvest", "activities", "list"])
        .expect("the `activities` top-level alias should parse");
    assert_eq!(
        cli.api_request().expect("request should build").path,
        "/activities"
    );
}

#[test]
fn activity_pause_percent_encodes_the_activity_name() {
    let cli = Cli::try_parse_from(["harvest", "activity", "pause", "charge card/eu"])
        .expect("activity pause args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.path, "/activities/charge%20card%2Feu/pause",
        "an activity name with a space or slash must not break out of the path segment"
    );
}

#[test]
fn activity_routes_reject_url_dot_segment_names() {
    // Identical hazard to the queue path: the WHATWG URL parser collapses
    // dot-segments AFTER `ApiRequest.path` is assembled, so an activity named
    // `.` would silently rewrite `/activities/./pause` to `/activities/pause`
    // -- a different route. Every subcommand that interpolates the name must
    // reject it, including the reads (a `..` on `get` would target `/`).
    for bad in [".", "..", "%2e", "%2E", "%2e%2e", "%2E%2E"] {
        for verb in ["pause", "resume", "get"] {
            let cli = Cli::try_parse_from(["harvest", "activity", verb, bad])
                .unwrap_or_else(|e| panic!("activity {verb} {bad:?} should parse: {e}"));

            assert!(
                cli.api_request().is_err(),
                "activity {verb} name {bad:?} normalizes away in the URL path \
                 and must be rejected"
            );
        }
    }
}

// ── Queue coverage (issue #774) ────────────────────────────────────────────

#[test]
fn queue_coverage_maps_to_the_read_route_unfiltered() {
    let cli = Cli::try_parse_from(["harvest", "queue", "coverage"]).expect("args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(request.path, "/admin/queue-coverage");
    assert_eq!(request.body, None);
}

#[test]
fn queue_coverage_threads_the_queue_filter_into_the_query_string() {
    let cli = Cli::try_parse_from(["harvest", "queue", "coverage", "--queue", "email-workers"])
        .expect("args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(request.method, ApiMethod::Get);
    assert_eq!(
        request.path,
        "/admin/queue-coverage?queue_name=email-workers"
    );
    assert_eq!(request.body, None);
}

#[test]
fn queue_coverage_query_encodes_a_queue_name_with_special_characters() {
    let cli = Cli::try_parse_from([
        "harvest",
        "queue",
        "coverage",
        "--queue",
        "email workers/eu",
    ])
    .expect("args should parse");

    let request = cli.api_request().expect("request should build");

    assert_eq!(
        request.path,
        "/admin/queue-coverage?queue_name=email%20workers%2Feu"
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
