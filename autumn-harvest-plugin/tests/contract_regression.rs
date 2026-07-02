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
///   - New mutating routes must be classified (`read_only = false`) in the contract.
use autumn_harvest_plugin::api::{
    management_api_request_fields, management_api_response_fields, management_api_routes,
};
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

/// `GET /workflows/count` (issue #544) must have an entry in
/// `autumn_harvest::audit::CLASSIFIED_ROUTES`.
///
/// `audit.rs`'s own exhaustiveness tests only check `CLASSIFIED_ROUTES` and
/// `ALL_MUTATION_ROUTES` against each other, which stays green even if a route
/// is added to `harvest_api_router` and never classified at all — as happened
/// here. This test walks the live router for this one route so the specific
/// regression this diff fixed can't silently reappear.
///
/// A broader "every route in `harvest_api_router` must be classified" sweep
/// was tried here and found 27 pre-existing, unrelated routes with the same
/// gap; fixing those is out of scope for this change and risks misclassifying
/// a route's mutation semantics without a dedicated per-route review, so this
/// test stays scoped to the route this diff actually introduces.
#[test]
fn workflow_count_route_is_classified() {
    use autumn_harvest::audit::CLASSIFIED_ROUTES;

    let route = "GET /workflows/count";
    assert!(
        management_api_routes()
            .iter()
            .any(|(m, p)| format!("{m} {p}") == route),
        "{route} must be registered in management_api_routes()"
    );
    assert!(
        CLASSIFIED_ROUTES.iter().any(|(r, _)| *r == route),
        "{route} must have an entry in autumn_harvest::audit::CLASSIFIED_ROUTES"
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
/// Update `management_api_request_fields()` AND `docs/api-contract.json` together.
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
                .filter_map(|f| f["name"].as_str().map(ToString::to_string))
                .collect();
            contract_fields.insert((method, path), Some(names));
        }
    }

    let registered_request_routes: HashSet<(String, String)> = management_api_request_fields()
        .iter()
        .map(|(method, path, _)| ((*method).to_string(), (*path).to_string()))
        .collect();
    for ((method, path), fields) in &contract_fields {
        if fields.is_some() {
            assert!(
                registered_request_routes.contains(&(method.clone(), path.clone())),
                "{method} {path}: contract has a structured request body but \
                 management_api_request_fields() has no entry, so field drift is unchecked"
            );
        }
    }

    // Compare against management_api_request_fields().
    for (method, path, code_fields) in management_api_request_fields() {
        let key = (method.to_string(), path.to_string());
        match (code_fields, contract_fields.get(&key)) {
            (Some(cf), Some(Some(contract_f))) => {
                let mut code_set: Vec<&str> = cf.to_vec();
                let mut contract_set: Vec<&str> = contract_f.iter().map(String::as_str).collect();
                code_set.sort_unstable();
                contract_set.sort_unstable();
                assert_eq!(
                    code_set, contract_set,
                    "Request field mismatch for {method} {path}: \
                     code registry has {code_set:?} but contract has {contract_set:?}. \
                     Update both `management_api_request_fields()` and docs/api-contract.json."
                );
            }
            (None, Some(Some(_))) => panic!(
                "{method} {path}: code registry says free-form but contract has structured fields"
            ),
            (Some(_), Some(None)) => panic!(
                "{method} {path}: code registry has structured fields but contract says free-form"
            ),
            // both free-form, or route has no body in contract — skip
            (None, Some(None)) | (_, None) => {}
        }
    }
}

/// No contract route may be both `read_only:true` and use a mutating HTTP method,
/// unless the route is annotated with `post_for_body_only:true` to signal that
/// POST was chosen solely to allow a structured request body and the route never
/// writes workflow events (e.g. `POST /workflows/{id}/query/{query_name}`).
#[test]
fn contract_read_only_classification_is_consistent() {
    let contract = load_contract();
    let mutating_methods = ["POST", "PUT", "PATCH", "DELETE"];

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap_or("");
        let path = route["path"].as_str().unwrap_or("");
        let read_only = route["read_only"].as_bool().unwrap_or(false);
        let post_for_body_only = route["post_for_body_only"].as_bool().unwrap_or(false);

        if mutating_methods.contains(&method) && !post_for_body_only {
            assert!(
                !read_only,
                "route {method} {path} uses a mutating HTTP method but is marked read_only:true \
                 (set post_for_body_only:true if POST is used only for body-passing)"
            );
        }
    }
}

/// Every contract route that has a structured `success_response` (i.e. a `fields`
/// array rather than `free_form: true`) must have its top-level response field
/// names listed in `management_api_response_fields()`, and vice-versa.
///
/// Update `management_api_response_fields()` AND the `success_response.fields`
/// array in docs/api-contract.json together whenever you add, remove, or rename
/// a top-level response field on any management route.
#[test]
fn contract_response_fields_match_code_registry() {
    let contract = load_contract();

    // Build (method, path) → field names from the contract's success_response.
    let mut contract_resp: HashMap<(String, String), Option<Vec<String>>> = HashMap::new();
    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap().to_string();
        let path = route["path"].as_str().unwrap().to_string();
        let sr = &route["success_response"];
        if !sr.is_object() {
            continue;
        }
        if sr["free_form"].as_bool().unwrap_or(false) {
            contract_resp.insert((method, path), None);
        } else if let Some(arr) = sr["fields"].as_array() {
            let names: Vec<String> = arr
                .iter()
                .filter_map(|f| {
                    // fields may be plain strings or {name: ...} objects
                    f.as_str()
                        .map(ToString::to_string)
                        .or_else(|| f["name"].as_str().map(ToString::to_string))
                })
                .collect();
            contract_resp.insert((method, path), Some(names));
        }
    }

    let registered_response_routes: HashSet<(String, String)> = management_api_response_fields()
        .iter()
        .map(|(method, path, _)| ((*method).to_string(), (*path).to_string()))
        .collect();
    for ((method, path), fields) in &contract_resp {
        if fields.is_some() {
            assert!(
                registered_response_routes.contains(&(method.clone(), path.clone())),
                "{method} {path}: contract has a structured success_response but \
                 management_api_response_fields() has no entry, so field drift is unchecked"
            );
        }
    }

    for (method, path, code_fields) in management_api_response_fields() {
        let key = (method.to_string(), path.to_string());
        match (code_fields, contract_resp.get(&key)) {
            (None, Some(None)) => {}
            (Some(cf), Some(Some(contract_f))) => {
                let mut code_set: Vec<&str> = cf.to_vec();
                let mut contract_set: Vec<&str> = contract_f.iter().map(String::as_str).collect();
                code_set.sort_unstable();
                contract_set.sort_unstable();
                assert_eq!(
                    code_set, contract_set,
                    "Response field mismatch for {method} {path}: \
                     code registry has {code_set:?} but contract has {contract_set:?}. \
                     Update both management_api_response_fields() and docs/api-contract.json."
                );
            }
            (None, Some(Some(_))) => panic!(
                "{method} {path}: code registry says free-form but contract has structured fields"
            ),
            (Some(_), Some(None)) => panic!(
                "{method} {path}: code registry has structured fields but contract says free-form"
            ),
            (_, None) => panic!(
                "{method} {path}: in code response registry but missing from contract \
                 success_response (add 'fields' or 'free_form: true' to the route)"
            ),
        }
    }
}

#[test]
fn schedule_backfill_response_documents_paused_schedule_warning() {
    let contract = load_contract();
    let route = contract["routes"]
        .as_array()
        .expect("contract.routes must be a JSON array")
        .iter()
        .find(|route| {
            route["method"].as_str() == Some("POST")
                && route["path"].as_str() == Some("/admin/schedules/{id}/backfill")
        })
        .expect("POST /admin/schedules/{id}/backfill must be documented");

    let contract_fields: HashSet<&str> = route["success_response"]["fields"]
        .as_array()
        .expect("schedule backfill success_response must list structured fields")
        .iter()
        .filter_map(|field| field["name"].as_str())
        .collect();
    assert!(
        contract_fields.contains("paused_schedule_warning"),
        "schedule backfill contract must document the optional warning emitted \
         when include_paused=true backfills a paused DAG schedule"
    );

    let registry_fields = management_api_response_fields()
        .iter()
        .find(|(method, path, _)| *method == "POST" && *path == "/admin/schedules/{id}/backfill")
        .and_then(|(_, _, fields)| *fields)
        .expect("schedule backfill must have structured response registry fields");
    assert!(
        registry_fields.contains(&"paused_schedule_warning"),
        "schedule backfill response registry must include paused_schedule_warning"
    );
}

#[test]
fn schedule_list_preserves_array_response_classification() {
    let contract = load_contract();
    let route = contract["routes"]
        .as_array()
        .expect("contract.routes must be a JSON array")
        .iter()
        .find(|route| {
            route["method"].as_str() == Some("GET")
                && route["path"].as_str() == Some("/admin/schedules")
        })
        .expect("GET /admin/schedules must be documented");

    assert!(
        route["success_response"]["free_form"]
            .as_bool()
            .unwrap_or(false),
        "GET /admin/schedules returns a JSON array, so its top-level \
         success_response must stay free_form/array instead of object fields"
    );

    let registry_fields = management_api_response_fields()
        .iter()
        .find(|(method, path, _)| *method == "GET" && *path == "/admin/schedules")
        .map(|(_, _, fields)| *fields)
        .expect("GET /admin/schedules must stay in the response registry");

    assert!(
        registry_fields.is_none(),
        "GET /admin/schedules returns Json<Vec<ScheduleEntry>>, so the response \
         registry must preserve array/free-form classification"
    );
}

/// Every contract route must document an `idempotency` field so embedders
/// know which operations are safe to retry.
#[test]
fn contract_mutating_routes_have_idempotency_field() {
    let contract = load_contract();
    let mutating_methods = ["POST", "PUT", "PATCH", "DELETE"];

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap_or("");
        let path = route["path"].as_str().unwrap_or("");
        if !mutating_methods.contains(&method) {
            continue;
        }
        assert!(
            route["idempotency"].is_string(),
            "mutating route {method} {path}: missing required 'idempotency' string field \
             (state whether the operation is idempotent and under what conditions)"
        );
    }
}

/// Every route in the contract must have a `params` array (may be empty).
#[test]
fn contract_routes_have_params_array() {
    let contract = load_contract();
    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap_or("?");
        let path = route["path"].as_str().unwrap_or("?");
        assert!(
            route["params"].is_array(),
            "route {method} {path}: missing 'params' array (use [] when the route has no parameters)"
        );
    }
}
