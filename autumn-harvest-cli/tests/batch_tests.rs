use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;
use std::io::Write as _;

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
        .expect("CLI should parse successfully")
}

/// Write a temp file with NDJSON content and return its path.
/// The returned `TempPath` deletes the file when dropped.
fn write_ndjson_file(lines: &[&str]) -> tempfile::TempPath {
    let mut tmp = tempfile::NamedTempFile::new().expect("tmp file");
    for line in lines {
        writeln!(tmp, "{line}").expect("write line");
    }
    tmp.into_temp_path()
}

#[test]
fn batch_list_maps_to_management_route() {
    let req = parse(&["batch", "list"]).api_request().unwrap();

    assert_eq!(req.method, ApiMethod::Get);
    assert_eq!(req.path, "/batch-operations");
    assert!(req.body.is_none());
}

#[test]
fn batch_list_with_limit_maps_to_management_route() {
    let req = parse(&["batch", "list", "--limit", "50"])
        .api_request()
        .unwrap();

    assert_eq!(req.method, ApiMethod::Get);
    assert_eq!(req.path, "/batch-operations?limit=50");
    assert!(req.body.is_none());
}

#[test]
fn batch_get_maps_to_management_route() {
    let req = parse(&["batch", "get", "123e4567-e89b-12d3-a456-426614174000"])
        .api_request()
        .unwrap();

    assert_eq!(req.method, ApiMethod::Get);
    assert_eq!(
        req.path,
        "/batch-operations/123e4567-e89b-12d3-a456-426614174000"
    );
    assert!(req.body.is_none());
}

#[test]
fn batch_submit_cancel_maps_to_management_route() {
    let req = parse(&[
        "batch",
        "submit",
        "Cancel",
        "--filter-json",
        r#"{"states": ["RUNNING"]}"#,
    ])
    .api_request()
    .unwrap();

    assert_eq!(req.method, ApiMethod::Post);
    assert_eq!(req.path, "/batch-operations");

    let body = req.body.unwrap();
    assert_eq!(body["action"], "Cancel");
    assert_eq!(body["filter"]["states"][0], "RUNNING");
}

#[test]
fn batch_submit_signal_maps_to_management_route() {
    let req = parse(&[
        "batch",
        "submit",
        "Signal",
        "--filter-json",
        r#"{"states": ["RUNNING"]}"#,
        "--signal-name",
        "my_signal",
        "--signal-payload-json",
        r#"{"key": "value"}"#,
    ])
    .api_request()
    .unwrap();

    assert_eq!(req.method, ApiMethod::Post);
    assert_eq!(req.path, "/batch-operations");

    let body = req.body.unwrap();
    assert_eq!(body["action"], "Signal");
    assert_eq!(body["filter"]["states"][0], "RUNNING");
    assert_eq!(body["signal_name"], "my_signal");
    assert_eq!(body["signal_payload"]["key"], "value");
}

// ── start-batch subcommand (issue #357) ───────────────────────────────────────

#[test]
fn start_batch_file_maps_to_batch_start_route() {
    let path = write_ndjson_file(&[
        r#"{"workflow_name":"onboarding","workflow_id":"w1","input":{"user_id":1}}"#,
        r#"{"workflow_name":"billing","workflow_id":"w2"}"#,
    ]);
    let req = parse(&["start-batch", "--file", path.to_str().unwrap()])
        .api_request()
        .unwrap();

    assert_eq!(req.method, ApiMethod::Post);
    assert_eq!(req.path, "/workflows/batch_start");

    let body = req.body.unwrap();
    assert_eq!(body["atomic"], false);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["workflow_name"], "onboarding");
    assert_eq!(items[0]["workflow_id"], "w1");
    assert_eq!(items[1]["workflow_name"], "billing");
}

#[test]
fn start_batch_atomic_flag_sets_atomic_true() {
    let path = write_ndjson_file(&[r#"{"workflow_name":"onboarding","workflow_id":"w1"}"#]);
    let req = parse(&["start-batch", "--file", path.to_str().unwrap(), "--atomic"])
        .api_request()
        .unwrap();

    let body = req.body.unwrap();
    assert_eq!(body["atomic"], true);
}

#[test]
fn start_batch_items_json_inline() {
    let req = parse(&[
        "start-batch",
        "--items-json",
        r#"[{"workflow_name":"onboarding","workflow_id":"w1"},{"workflow_name":"billing"}]"#,
    ])
    .api_request()
    .unwrap();

    assert_eq!(req.method, ApiMethod::Post);
    assert_eq!(req.path, "/workflows/batch_start");

    let body = req.body.unwrap();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["workflow_name"], "onboarding");
}

#[test]
fn start_batch_items_json_atomic() {
    let req = parse(&[
        "start-batch",
        "--items-json",
        r#"[{"workflow_name":"onboarding"}]"#,
        "--atomic",
    ])
    .api_request()
    .unwrap();

    let body = req.body.unwrap();
    assert_eq!(body["atomic"], true);
}
