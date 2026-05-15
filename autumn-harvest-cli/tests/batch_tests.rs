use autumn_harvest_cli::{ApiMethod, Cli};
use clap::Parser;

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
        .expect("CLI should parse successfully")
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
