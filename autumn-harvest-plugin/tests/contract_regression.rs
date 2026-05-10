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
use autumn_harvest_plugin::api::{management_api_request_fields, management_api_routes};
use std::collections::{HashMap, HashSet};

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

/// Every mutating route in the contract must have a `request_body.fields` array
/// so that the body-field coverage tests can validate CLI output against it.
#[test]
fn contract_mutating_routes_have_structured_request_body() {
    let contract = load_contract();
    let mutating_methods = ["POST", "PUT", "PATCH", "DELETE"];

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap_or("");
        let path = route["path"].as_str().unwrap_or("");
        if !mutating_methods.contains(&method) {
            continue;
        }
        let rb = &route["request_body"];
        assert!(
            rb.is_object(),
            "{method} {path}: mutating route must have a 'request_body' object"
        );
        let free_form = rb["free_form"].as_bool().unwrap_or(false);
        if !free_form {
            assert!(
                rb["fields"].is_array(),
                "{method} {path}: request_body must have a 'fields' array \
                 (use free_form:true for routes whose body is an opaque payload)"
            );
        }
    }
}

/// The request field registry in code must match the structured field list in the contract.
/// Update management_api_request_fields() AND docs/api-contract.json together.
#[test]
fn contract_request_fields_match_code_registry() {
    let contract = load_contract();

    // Build (method, path) → field names from the contract's structured schemas.
    let mut contract_fields: HashMap<(String, String), Option<Vec<String>>> = HashMap::new();
    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap().to_string();
        let path = route["path"].as_str().unwrap().to_string();
        let rb = &route["request_body"];
        if !rb.is_object() {
            continue;
        }
        if rb["free_form"].as_bool().unwrap_or(false) {
            contract_fields.insert((method, path), None);
        } else if let Some(arr) = rb["fields"].as_array() {
            let names: Vec<String> = arr
                .iter()
                .filter_map(|f| f["name"].as_str().map(|s| s.to_string()))
                .collect();
            contract_fields.insert((method, path), Some(names));
        }
    }

    // Compare against management_api_request_fields().
    for (method, path, code_fields) in management_api_request_fields() {
        let key = (method.to_string(), path.to_string());
        match (code_fields, contract_fields.get(&key)) {
            (None, Some(None)) => {} // both free-form ✓
            (Some(cf), Some(Some(contract_f))) => {
                let mut code_set: Vec<&str> = cf.iter().copied().collect();
                let mut contract_set: Vec<&str> =
                    contract_f.iter().map(|s| s.as_str()).collect();
                code_set.sort_unstable();
                contract_set.sort_unstable();
                assert_eq!(
                    code_set, contract_set,
                    "Request field mismatch for {method} {path}: \
                     code registry has {code_set:?} but contract has {contract_set:?}. \
                     Update both management_api_request_fields() and docs/api-contract.json."
                );
            }
            (None, Some(Some(_))) => panic!(
                "{method} {path}: code registry says free-form but contract has structured fields"
            ),
            (Some(_), Some(None)) => panic!(
                "{method} {path}: code registry has structured fields but contract says free-form"
            ),
            (_, None) => {
                // Route is read-only or has no body — not in contract_fields map, skip.
            }
        }
    }
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
