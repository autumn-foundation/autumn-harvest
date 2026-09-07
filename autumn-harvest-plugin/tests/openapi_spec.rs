// OpenAPI and JSON-schema recur throughout this file and are legitimate
// acronyms. Silence clippy::doc_markdown here rather than wrapping every
// mention in backticks, matching `autumn_web::openapi`.
#![allow(clippy::doc_markdown)]

//! OpenAPI 3.1 contract tests for the management API (issue #694).
//!
//! The served document, the checked-in `docs/openapi.json` artifact and the
//! live router all derive from one source: `docs/api-contract.json`. These
//! tests pin that chain end to end, so a route can never appear in the router
//! without appearing in the published spec.
//!
//! No database is required. The document is a pure transform of the contract,
//! and the served endpoint reads no state. The router therefore runs against
//! `AppState::for_test()` through `tower`, as `effective_config_http_tests` does.

use std::collections::{BTreeSet, HashMap, HashSet};

use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_harvest_plugin::management_api_routes;
use autumn_harvest_plugin::openapi::{openapi_document, openapi_json};
use autumn_web::AppState;
use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::axum::routing::get;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

/// The pretty-printed copy, resolved from this crate rather than the caller
/// working directory. The compact copy is compiled into the crate.
const PRETTY_ARTIFACT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/openapi.json");

/// The contract the document is derived from, read for cross-checks.
const API_CONTRACT_JSON: &str = include_str!("../../docs/api-contract.json");

/// HTTP methods the document may carry, lower-cased as OpenAPI spells them.
const OPERATION_KEYS: &[&str] = &["get", "put", "post", "delete", "patch"];

fn document() -> &'static Value {
    openapi_document()
}

/// Every `(method, path)` pair present in the document, method upper-cased.
fn document_operations(doc: &Value) -> BTreeSet<(String, String)> {
    let mut found = BTreeSet::new();
    let paths = doc["paths"].as_object().expect("paths must be an object");
    for (path, item) in paths {
        let item = item.as_object().expect("path item must be an object");
        for (key, _) in item {
            assert!(
                OPERATION_KEYS.contains(&key.as_str()),
                "path {path}: unexpected path-item key {key}"
            );
            found.insert((key.to_uppercase(), path.clone()));
        }
    }
    found
}

fn router_routes() -> BTreeSet<(String, String)> {
    management_api_routes()
        .iter()
        .map(|(method, path)| ((*method).to_owned(), (*path).to_owned()))
        .collect()
}

/// AC3: every mounted route is an operation, and the document invents none.
#[test]
fn document_covers_every_management_route_exactly() {
    let doc = document();
    let documented = document_operations(doc);
    let mounted = router_routes();

    let missing: Vec<_> = mounted.difference(&documented).collect();
    let extra: Vec<_> = documented.difference(&mounted).collect();

    assert!(
        missing.is_empty(),
        "routes in management_api_routes() with no operation in the OpenAPI document:\n{missing:#?}"
    );
    assert!(
        extra.is_empty(),
        "operations in the OpenAPI document that no mounted route serves:\n{extra:#?}"
    );
}

/// AC4: every operation declares its parameters with an explicit `required`
/// flag, and every path template placeholder is declared as a path parameter.
#[test]
fn every_operation_declares_its_parameters() {
    let doc = document();
    let paths = doc["paths"].as_object().unwrap();
    for (path, item) in paths {
        let template_params: HashSet<String> = path
            .split('/')
            .filter_map(|segment| {
                segment
                    .strip_prefix('{')
                    .and_then(|s| s.strip_suffix('}'))
                    .map(ToOwned::to_owned)
            })
            .collect();

        for (method, operation) in item.as_object().unwrap() {
            let declared: Vec<&Value> = operation["parameters"]
                .as_array()
                .map(|a| a.iter().collect())
                .unwrap_or_default();
            let mut declared_path = HashSet::new();
            for param in &declared {
                let name = param["name"].as_str().expect("parameter needs a name");
                let location = param["in"].as_str().expect("parameter needs `in`");
                assert!(
                    matches!(location, "path" | "query" | "header"),
                    "{method} {path}: parameter {name} has unsupported location {location}"
                );
                assert!(
                    param["required"].is_boolean(),
                    "{method} {path}: parameter {name} has no boolean `required` flag"
                );
                assert!(
                    param["schema"].is_object(),
                    "{method} {path}: parameter {name} has no schema"
                );
                if location == "path" {
                    assert_eq!(
                        param["required"],
                        Value::Bool(true),
                        "{method} {path}: path parameter {name} must be required"
                    );
                    declared_path.insert(name.to_owned());
                }
            }
            assert_eq!(
                declared_path, template_params,
                "{method} {path}: declared path parameters must match the path template"
            );
        }
    }
}

/// AC4: every operation declares at least one response, and that response
/// carries a JSON schema unless it is a bodiless status.
#[test]
fn every_operation_declares_a_response_with_a_schema() {
    let doc = document();
    let contract: Value = serde_json::from_str(API_CONTRACT_JSON).expect("contract must parse");
    // The success status of each route. The check then lands on the response
    // that carries the body, not on whichever response happens to have one.
    let success: HashMap<(String, String), u64> = contract["routes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|route| {
            (
                (
                    route["method"].as_str().unwrap().to_lowercase(),
                    route["path"].as_str().unwrap().to_owned(),
                ),
                route["success_response"]["status"].as_u64().unwrap(),
            )
        })
        .collect();

    for (path, item) in doc["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            let responses = operation["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{method} {path}: no responses object"));
            assert!(
                !responses.is_empty(),
                "{method} {path}: responses object is empty"
            );
            let mut schemas = 0;
            for (status, response) in responses {
                assert!(
                    status.parse::<u16>().is_ok(),
                    "{method} {path}: response key {status} is not an HTTP status"
                );
                assert!(
                    response["description"].is_string(),
                    "{method} {path}: response {status} has no description"
                );
                if let Some(content) = response["content"].as_object() {
                    for media in content.values() {
                        assert!(
                            media["schema"].is_object(),
                            "{method} {path}: response {status} content has no schema"
                        );
                    }
                    schemas += 1;
                }
            }
            let success_status = success[&(method.clone(), path.clone())];
            let success_response = &responses[&success_status.to_string()];
            if success_status == 204 {
                assert!(
                    success_response["content"].is_null(),
                    "{method} {path}: a 204 carries no body"
                );
            } else {
                assert!(
                    success_response["content"].is_object(),
                    "{method} {path}: the {success_status} response declares no body schema"
                );
                assert!(
                    schemas > 0,
                    "{method} {path}: no response declares a schema"
                );
            }
        }
    }
}

/// AC4: a mutating route publishes a request-body schema whenever the contract
/// documents a body.
///
/// A route with `free_form: false` and an empty `fields` list documents a route
/// that takes no body, such as a `DELETE`. Publishing an empty body schema for
/// one of those would tell a generated client to send `{}`, which is wrong.
#[test]
fn mutating_operations_publish_a_request_body_schema() {
    let doc = document();
    let contract: Value = serde_json::from_str(API_CONTRACT_JSON).expect("contract must parse");
    let mut offenders = Vec::new();

    for route in contract["routes"].as_array().unwrap() {
        let method = route["method"].as_str().unwrap();
        let path = route["path"].as_str().unwrap();
        let body = &route["request_body"];
        let free_form = body["free_form"].as_bool().unwrap_or(false);
        let has_fields = body["fields"]
            .as_array()
            .is_some_and(|fields| !fields.is_empty());
        let documents_a_body = free_form || has_fields;

        let operation = &doc["paths"][path][method.to_lowercase()];
        let published = operation.get("requestBody");
        match (documents_a_body, published) {
            (true, Some(published)) => {
                assert!(
                    published["content"]["application/json"]["schema"].is_object(),
                    "{method} {path}: request body has no JSON schema"
                );
                assert!(
                    published["required"].is_boolean(),
                    "{method} {path}: request body has no boolean `required` flag"
                );
            }
            (true, None) => {
                offenders.push(format!("{method} {path}: body documented, none published"));
            }
            (false, Some(_)) => {
                offenders.push(format!(
                    "{method} {path}: no body documented, one published"
                ));
            }
            (false, None) => {}
        }
    }

    assert!(
        offenders.is_empty(),
        "request bodies must follow the contract:\n{offenders:#?}"
    );
}

/// AC5: the read-only classification survives the transform, on every
/// operation, and agrees with the engine's own route classification.
#[test]
fn read_only_classification_is_published() {
    use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};

    let class_is_read: HashMap<&str, bool> = CLASSIFIED_ROUTES
        .iter()
        .map(|(template, class)| {
            (
                *template,
                matches!(class, RouteClass::ReadOnly | RouteClass::PublicSafe),
            )
        })
        .collect();
    let class_name: HashMap<&str, &str> = CLASSIFIED_ROUTES
        .iter()
        .map(|(template, class)| {
            (
                *template,
                match class {
                    RouteClass::PublicSafe => "public_safe",
                    RouteClass::ReadOnly => "read_only",
                    RouteClass::Mutating => "mutating",
                },
            )
        })
        .collect();

    let doc = document();
    let mut mismatches = Vec::new();
    for (path, item) in doc["paths"].as_object().unwrap() {
        for (method, operation) in item.as_object().unwrap() {
            let read_only = operation["x-harvest-read-only"]
                .as_bool()
                .unwrap_or_else(|| panic!("{method} {path}: no x-harvest-read-only extension"));
            assert!(
                operation["tags"].as_array().is_some_and(|t| !t.is_empty()),
                "{method} {path}: operation carries no tag"
            );
            let key = format!("{} {path}", method.to_uppercase());
            if let Some(&is_read) = class_is_read.get(key.as_str())
                && is_read != read_only
            {
                mismatches.push(format!(
                    "{key}: document read-only={read_only}, CLASSIFIED_ROUTES read={is_read}"
                ));
            }
            let published_class = operation["x-harvest-route-class"].as_str().unwrap_or("");
            let expected = class_name.get(key.as_str()).copied().unwrap_or("unknown");
            if published_class != expected {
                mismatches.push(format!(
                    "{key}: document class={published_class}, CLASSIFIED_ROUTES class={expected}"
                ));
            }
            // A PublicSafe route needs no credential, and only an
            // operation-level empty list can say that (issue #174).
            let waives_auth = operation["security"].as_array().is_some_and(Vec::is_empty);
            if (expected == "public_safe") != waives_auth {
                mismatches.push(format!(
                    "{key}: class={expected} but security waiver={waives_auth}"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "x-harvest-read-only must match autumn_harvest::audit::CLASSIFIED_ROUTES:\n{mismatches:#?}"
    );
}

/// Pin the classification of four routes by hand.
///
/// The sweep above compares the document against `CLASSIFIED_ROUTES`, which is
/// also what the transform reads. These four are written out, so a wrong entry
/// in that table shows up as a failure here rather than as agreement.
#[test]
fn known_routes_carry_their_expected_class() {
    let doc = document();
    for (method, path, class, waives_auth) in [
        ("get", "/openapi.json", "public_safe", true),
        ("get", "/health", "public_safe", true),
        ("get", "/workflows", "read_only", false),
        ("post", "/workflows/{id}/cancel", "mutating", false),
    ] {
        let operation = &doc["paths"][path][method];
        assert_eq!(
            operation["x-harvest-route-class"], class,
            "{method} {path} must be classified {class}"
        );
        assert_eq!(
            operation["security"].as_array().is_some_and(Vec::is_empty),
            waives_auth,
            "{method} {path}: unexpected security waiver"
        );
        assert_eq!(
            operation["x-harvest-read-only"],
            Value::Bool(class != "mutating"),
            "{method} {path}: read-only flag disagrees with the class"
        );
    }
}

/// A documented response header reaches the document, so a generated client
/// can read it. The by-id family resolves a business id, and returns that
/// resolution in `X-Harvest-Execution-Id`.
#[test]
fn documented_response_headers_are_published() {
    let doc = document();
    let by_id = &doc["paths"]["/workflows/by-id/{workflow_name}/{workflow_id}"]["get"];
    assert_eq!(
        by_id["responses"]["200"]["headers"]["X-Harvest-Execution-Id"]["schema"]["type"], "string",
        "the resolved execution id must be readable from the response"
    );

    let result = &doc["paths"]["/workflows/{id}/result"]["get"];
    assert!(
        result["responses"]["204"].is_object(),
        "the 204 a still-running execution returns must be declared"
    );
    assert_eq!(
        result["responses"]["204"]["headers"]["Retry-After"]["schema"]["type"],
        "integer"
    );
    assert!(
        result["responses"]["204"]["content"].is_null(),
        "a 204 carries no body"
    );
}

/// A route that answers in more than one representation declares both.
///
/// `GET /admin/queues/scaling` returns Prometheus text for `format=prometheus`
/// and JSON otherwise, and `GET /admin/metrics` is Prometheus text only. A
/// generated client that assumed JSON would fail to decode either.
#[test]
fn multi_format_responses_declare_every_media_type() {
    const PROMETHEUS: &str = "text/plain; version=0.0.4; charset=utf-8";

    let doc = document();

    let metrics = &doc["paths"]["/admin/metrics"]["get"]["responses"]["200"]["content"];
    assert!(metrics[PROMETHEUS].is_object(), "{metrics}");
    assert!(
        metrics["application/json"].is_null(),
        "the metrics scrape is never JSON"
    );

    let scaling = &doc["paths"]["/admin/queues/scaling"]["get"]["responses"]["200"]["content"];
    assert!(scaling["application/json"].is_object(), "{scaling}");
    assert!(scaling[PROMETHEUS].is_object(), "{scaling}");

    // Only an event stream is unbounded, so only it carries the marker.
    assert!(doc["paths"]["/admin/metrics"]["get"]["x-harvest-stream"].is_null());
}

/// A filter the query parser accepts reaches the document as a parameter.
///
/// These routes take a `RawQuery` and parse the pairs by hand, so no struct
/// field names them. `docs/audits/openapi-response-coverage.py` gates the whole
/// set; this test pins the ones a client needs most.
#[test]
fn hand_parsed_query_filters_are_published() {
    let doc = document();

    let listing = parameter_names(&doc["paths"]["/workflows"]["get"]);
    for name in [
        "owner",
        "severity",
        "failure_cause",
        "no_progress_minutes",
        "include_sleeping",
        "min_history_events",
    ] {
        assert!(listing.contains(name), "GET /workflows omits {name}");
    }

    let exports = parameter_names(&doc["paths"]["/admin/history/exports"]["get"]);
    for name in ["state", "max_bytes"] {
        assert!(
            exports.contains(name),
            "GET /admin/history/exports omits {name}"
        );
    }
}

/// The query parameter names one operation publishes.
fn parameter_names(operation: &Value) -> BTreeSet<String> {
    operation["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|parameter| parameter["in"] == "query")
        .filter_map(|parameter| parameter["name"].as_str())
        .map(str::to_owned)
        .collect()
}

/// A rejection the handler forwards unchanged keeps its own media type.
#[test]
fn forwarded_extractor_rejections_declare_their_representation() {
    let doc = document();
    let start = &doc["paths"]["/workflows/{workflow_name}/start"]["post"];
    let content = &start["responses"]["422"]["content"];
    assert!(content["application/json"].is_object(), "{content}");
    assert_eq!(
        content["text/plain"]["schema"]["type"], "string",
        "{content}"
    );
}

/// A streaming route says so, and does not pretend to return JSON.
#[test]
fn streaming_operations_are_marked() {
    let doc = document();
    let stream = &doc["paths"]["/workflows/{id}/stream"]["get"];
    assert_eq!(stream["x-harvest-stream"], Value::Bool(true));
    assert_eq!(
        stream["responses"]["200"]["content"]["text/event-stream"]["schema"]["type"],
        "string"
    );
}

/// AC6: the issue #174 auth posture is declared as a security scheme stub.
#[test]
fn security_scheme_stub_is_declared() {
    let doc = document();
    let schemes = doc["components"]["securitySchemes"]
        .as_object()
        .expect("components.securitySchemes must be an object");
    let bearer = &schemes["HarvestBearerToken"];
    assert_eq!(bearer["type"], "http");
    assert_eq!(bearer["scheme"], "bearer");
    let session = &schemes["HarvestSessionCookie"];
    assert_eq!(session["type"], "apiKey");
    assert_eq!(session["in"], "cookie");

    let security = doc["security"].as_array().expect("top-level security list");
    assert!(
        security.iter().any(|entry| entry
            .as_object()
            .is_some_and(|requirement| requirement.contains_key("HarvestBearerToken"))),
        "the bearer scheme must appear in the top-level security list"
    );
    assert!(
        security
            .iter()
            .any(|entry| entry.as_object().is_some_and(serde_json::Map::is_empty)),
        "an empty requirement must record that enforcement is the embedder's choice (issue #174)"
    );
}

/// AC1 and AC2: the document is a structurally valid OpenAPI 3.1 object with
/// unique operation ids and resolvable references.
#[test]
fn document_is_structurally_valid_openapi_3_1() {
    let doc = document();
    assert_eq!(doc["openapi"], "3.1.0");
    assert!(doc["info"]["title"].is_string());
    assert!(doc["info"]["version"].is_string());
    assert_eq!(
        doc["servers"][0]["url"], "/api/harvest",
        "the server entry names the conventional mount point, which every doc \
         and the quickstart use"
    );
    assert_eq!(
        doc["info"]["version"],
        env!("CARGO_PKG_VERSION"),
        "the document version tracks the crate version. Update `version` in \
         docs/api-contract.json, then run scripts/regenerate-openapi.sh"
    );

    let mut ids = HashSet::new();
    for (path, item) in doc["paths"].as_object().unwrap() {
        assert!(path.starts_with('/'), "path {path} must be absolute");
        for (method, operation) in item.as_object().unwrap() {
            let id = operation["operationId"]
                .as_str()
                .unwrap_or_else(|| panic!("{method} {path}: no operationId"));
            assert!(ids.insert(id.to_owned()), "duplicate operationId {id}");
            assert!(
                operation["summary"].is_string(),
                "{method} {path}: no summary"
            );
        }
    }

    let schemas = doc["components"]["schemas"].as_object();
    let mut unresolved = Vec::new();
    collect_unresolved_refs(doc, schemas, &mut unresolved);
    assert!(
        unresolved.is_empty(),
        "every $ref must resolve inside components.schemas:\n{unresolved:#?}"
    );
}

fn collect_unresolved_refs(
    value: &Value,
    schemas: Option<&serde_json::Map<String, Value>>,
    out: &mut Vec<String>,
) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get("$ref") {
                let resolved = reference
                    .strip_prefix("#/components/schemas/")
                    .is_some_and(|name| schemas.is_some_and(|s| s.contains_key(name)));
                if !resolved {
                    out.push(reference.clone());
                }
            }
            for nested in map.values() {
                collect_unresolved_refs(nested, schemas, out);
            }
        }
        Value::Array(items) => {
            for nested in items {
                collect_unresolved_refs(nested, schemas, out);
            }
        }
        _ => {}
    }
}

/// AC7: both checked-in copies are the transform's output, and nothing else.
///
/// `autumn-harvest-plugin/openapi.json` is compiled in and served verbatim;
/// `docs/openapi.json` is the same document, pretty-printed for review. The
/// transform runs here against the contract, so a stale copy fails the build.
#[test]
fn checked_in_artifacts_match_the_generated_document() {
    let contract: Value = serde_json::from_str(API_CONTRACT_JSON).expect("contract must parse");
    let generated = autumn_harvest_plugin::openapi::document_from_contract(&contract)
        .expect("the contract must transform into OpenAPI 3.1");

    let served = openapi_json();
    let compiled_in: Value = serde_json::from_str(served).expect("the crate copy must be JSON");
    assert_eq!(
        compiled_in, generated,
        "autumn-harvest-plugin/openapi.json is stale. Regenerate both copies with:\n  \
         scripts/regenerate-openapi.sh"
    );
    assert_eq!(
        served,
        format!(
            "{}\n",
            serde_json::to_string(&generated).expect("must serialize")
        ),
        "the crate copy must be the compact document with a trailing newline"
    );

    let pretty = std::fs::read_to_string(PRETTY_ARTIFACT_PATH)
        .unwrap_or_else(|error| panic!("docs/openapi.json must be readable: {error}"));
    assert_eq!(
        pretty,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&generated).expect("must serialize")
        ),
        "docs/openapi.json is stale. Regenerate both copies with:\n  \
         scripts/regenerate-openapi.sh"
    );
}

/// The route is nest-relative, so an app that already serves `/openapi.json`
/// keeps it.
///
/// `HarvestPlugin` mounts `harvest_api_router` with `nest(api_path, ..)`, so
/// the document lands at `{api_path}/openapi.json`. An embedding app that
/// enables `autumn-web`'s own OpenAPI generation serves that document at the
/// root, and the two never meet.
#[tokio::test]
async fn the_route_does_not_take_the_application_root_path() {
    async fn app_document() -> &'static str {
        "{\"openapi\":\"3.1.0\",\"info\":{\"title\":\"the embedding app\"}}"
    }

    let app = Router::new()
        .route("/openapi.json", get(app_document))
        .nest("/api/harvest", harvest_api_router(HarvestApiState::new()))
        .with_state(AppState::for_test());

    let harvest = body_of(app.clone(), "/api/harvest/openapi.json").await;
    assert_eq!(
        harvest["info"]["title"], "Harvest management API",
        "the plugin serves its document under its own mount point"
    );

    let embedder = body_of(app, "/openapi.json").await;
    assert_eq!(
        embedder["info"]["title"], "the embedding app",
        "the embedding application keeps the root path"
    );
}

/// Fetch a path and parse the body as JSON.
async fn body_of(app: Router<()>, uri: &str) -> Value {
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
    let body = autumn_web::reexports::axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).expect("body must be JSON")
}

/// AC1 and AC7: the served endpoint answers 200 with exactly the artifact.
#[tokio::test]
async fn served_endpoint_returns_the_document() {
    let app = harvest_api_router(HarvestApiState::new()).with_state(AppState::for_test());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/json"),
        "the spec is served as JSON so a generator can read it directly"
    );

    let body = autumn_web::reexports::axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let served: Value = serde_json::from_slice(&body).expect("served body must be JSON");
    assert_eq!(&served, document(), "the served document is the artifact");
    assert_eq!(
        String::from_utf8(body.to_vec()).unwrap(),
        openapi_json(),
        "the endpoint serves the compiled-in bytes, unchanged"
    );
}
