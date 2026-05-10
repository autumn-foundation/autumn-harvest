/// Contract regression tests for the Harvest management API.
///
/// These tests enforce that `docs/api-contract.json` stays in sync with
/// the routes registered in `harvest_api_router`.  If you add, remove, rename,
/// or change the HTTP method of any management route you MUST update
/// `docs/api-contract.json` AND `CHANGELOG.md` before this test will pass.
///
/// Compatibility rules (stated in the contract):
///   - Adding response fields is non-breaking.
///   - Removing, renaming, or changing the type of a response field is breaking.
///   - New mutating routes must be classified (read_only = false) in the contract.
use autumn_harvest_plugin::api::management_api_routes;
use std::collections::HashSet;

const CONTRACT_JSON: &str = include_str!("../../docs/api-contract.json");

fn load_contract() -> serde_json::Value {
    serde_json::from_str(CONTRACT_JSON).expect("docs/api-contract.json must be valid JSON")
}

fn contract_route_set(contract: &serde_json::Value) -> HashSet<(String, String)> {
    contract["routes"]
        .as_array()
        .expect("contract.routes must be a JSON array")
        .iter()
        .map(|r| {
            let method = r["method"]
                .as_str()
                .expect("each route must have a string 'method'")
                .to_string();
            let path = r["path"]
                .as_str()
                .expect("each route must have a string 'path'")
                .to_string();
            (method, path)
        })
        .collect()
}

/// Every route registered in `harvest_api_router` must appear in the contract,
/// and every route in the contract must exist in `harvest_api_router`.
/// Drift in either direction causes this test to fail.
#[test]
fn management_routes_match_contract() {
    let contract = load_contract();
    let contract_routes = contract_route_set(&contract);

    let code_routes: HashSet<(String, String)> = management_api_routes()
        .iter()
        .map(|(m, p)| (m.to_string(), p.to_string()))
        .collect();

    let in_code_not_contract: Vec<_> = code_routes.difference(&contract_routes).collect();
    let in_contract_not_code: Vec<_> = contract_routes.difference(&code_routes).collect();

    assert!(
        in_code_not_contract.is_empty(),
        "Routes in harvest_api_router but missing from docs/api-contract.json \
         (update the contract and CHANGELOG):\n{in_code_not_contract:#?}"
    );
    assert!(
        in_contract_not_code.is_empty(),
        "Routes in docs/api-contract.json but not registered in harvest_api_router \
         (remove stale entries from the contract):\n{in_contract_not_code:#?}"
    );
}

/// Every route in the contract must carry all required metadata fields.
#[test]
fn contract_routes_have_required_fields() {
    let contract = load_contract();

    for (i, route) in contract["routes"]
        .as_array()
        .expect("contract.routes must be an array")
        .iter()
        .enumerate()
    {
        let path = route["path"].as_str().unwrap_or("(missing path)");
        let method = route["method"].as_str().unwrap_or("(missing method)");
        let loc = format!("route[{i}] {method} {path}");

        assert!(
            route["method"].is_string(),
            "{loc}: missing required field 'method'"
        );
        assert!(
            route["path"].is_string(),
            "{loc}: missing required field 'path'"
        );
        assert!(
            route["description"].is_string(),
            "{loc}: missing required field 'description'"
        );
        assert!(
            route["read_only"].is_boolean(),
            "{loc}: missing required boolean field 'read_only'"
        );
        assert!(
            route["category"].is_string(),
            "{loc}: missing required field 'category'"
        );
    }
}

/// The contract document must carry version and compatibility metadata.
#[test]
fn contract_has_version_and_compatibility_rules() {
    let contract = load_contract();

    assert!(
        contract["version"].is_string(),
        "contract must have a 'version' string (matches crate version)"
    );
    assert!(
        contract["contract_version"].is_string(),
        "contract must have a 'contract_version' string"
    );
    assert!(
        contract["compatibility"].is_object(),
        "contract must have a 'compatibility' object documenting breaking-change rules"
    );

    let compat = &contract["compatibility"];
    assert!(
        compat["additive_response_fields_allowed"].is_boolean(),
        "compatibility must declare 'additive_response_fields_allowed'"
    );
    assert!(
        compat["breaking_changes"].is_array(),
        "compatibility must list 'breaking_changes'"
    );
}

/// No contract route may be both read_only:true and use a mutating HTTP method.
#[test]
fn contract_read_only_classification_is_consistent() {
    let contract = load_contract();
    let mutating_methods = ["POST", "PUT", "PATCH", "DELETE"];

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap_or("");
        let path = route["path"].as_str().unwrap_or("");
        let read_only = route["read_only"].as_bool().unwrap_or(false);

        if mutating_methods.contains(&method) {
            assert!(
                !read_only,
                "route {method} {path} uses a mutating HTTP method but is marked read_only:true"
            );
        }
    }
}
