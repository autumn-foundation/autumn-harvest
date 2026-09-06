// OpenAPI, JSON and JSON-schema recur throughout this module and are
// legitimate acronyms. Silence clippy::doc_markdown here rather than wrapping
// every mention in backticks, matching `autumn_web::openapi`.
#![allow(clippy::doc_markdown)]

//! OpenAPI 3.1 document for the Harvest management API (issue #694).
//!
//! # One source
//!
//! `docs/api-contract.json` is the single source of truth for the management
//! route surface. This module transforms it into an OpenAPI 3.1 document. The
//! served `GET /openapi.json` endpoint and the checked-in `docs/openapi.json`
//! artifact are both that transform's output, so they cannot disagree.
//!
//! The contract itself is pinned to the live router by
//! `tests/contract_regression.rs`, which fails when a route exists in
//! `harvest_api_router` but not in the contract, or the reverse. A route
//! therefore cannot reach the router without reaching the published spec.
//!
//! # Why a transform, not a macro
//!
//! `autumn-web` derives an OpenAPI document from routes declared with its own
//! route macros, which carry `ApiDoc` metadata. `harvest_api_router` is a plain
//! `axum::Router`, so no such metadata exists for these routes. The contract
//! already records what the macros would infer, plus per-parameter `required`
//! flags and per-route read-only classification, so the contract is the richer
//! input. See `docs/openapi.md`.
//!
//! # Failure posture
//!
//! The contract is compiled in, and `document_from_contract` rejects a contract
//! that omits a field the spec needs. A defect is therefore a build-time bug,
//! caught by `tests/openapi_spec.rs` and by the contract regression suite, and
//! never a runtime surprise for a served request.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use autumn_web::reexports::axum::response::IntoResponse;
use autumn_web::reexports::http::header;
use serde_json::{Map, Value, json};

/// The machine-readable management API contract this document is derived from.
const API_CONTRACT_JSON: &str = include_str!("../../docs/api-contract.json");

/// Conventional mount point for `harvest_api_router`.
///
/// `HarvestPlugin::api(path)` chooses the real prefix, so this is a documented
/// default, not a guarantee. Every path in the document is relative to it.
pub const CONVENTIONAL_MOUNT_PATH: &str = "/api/harvest";

/// Media type served for a route whose contract declares none.
const DEFAULT_MEDIA_TYPE: &str = "application/json";

/// Bearer scheme name, for the scoped API tokens of issue #942.
const BEARER_SCHEME: &str = "HarvestBearerToken";

/// Session-cookie scheme name, for an embedder that authenticates with the
/// `autumn-web` session.
const SESSION_SCHEME: &str = "HarvestSessionCookie";

/// A contract that cannot be transformed into a valid OpenAPI document.
#[derive(Debug, thiserror::Error)]
#[error("docs/api-contract.json is not transformable into OpenAPI 3.1: {0}")]
pub struct OpenApiError(String);

static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    let contract: Value =
        serde_json::from_str(API_CONTRACT_JSON).expect("docs/api-contract.json must be valid JSON");
    document_from_contract(&contract).expect("docs/api-contract.json must transform cleanly")
});

static DOCUMENT_JSON: LazyLock<String> =
    LazyLock::new(|| serde_json::to_string(&*DOCUMENT).expect("the document must serialize"));

/// The OpenAPI 3.1 document for the management API.
#[must_use]
pub fn openapi_document() -> &'static Value {
    &DOCUMENT
}

/// The document as compact JSON, serialized once.
#[must_use]
pub fn openapi_json() -> &'static str {
    &DOCUMENT_JSON
}

/// `GET /openapi.json` — serve the document. Read-only, and reads no state.
pub(crate) async fn get_openapi_document() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, DEFAULT_MEDIA_TYPE)], openapi_json())
}

/// Transform a parsed management API contract into an OpenAPI 3.1 document.
///
/// # Errors
///
/// Returns [`OpenApiError`] when the contract omits a field the document needs,
/// or when two contract entries claim the same method and path.
pub fn document_from_contract(contract: &Value) -> Result<Value, OpenApiError> {
    let routes = contract["routes"]
        .as_array()
        .ok_or_else(|| OpenApiError("`routes` must be an array".to_owned()))?;

    let mut paths: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    let mut tags: BTreeMap<String, Value> = BTreeMap::new();

    for route in routes {
        let method = string_field(route, "method")?;
        let path = string_field(route, "path")?;
        let category = string_field(route, "category")?;
        tags.entry(category.to_owned()).or_insert_with(|| {
            json!({
                "name": category,
                "description": format!("Management routes in the {category} category."),
            })
        });

        let item = paths.entry(path.to_owned()).or_default();
        let key = method.to_lowercase();
        if item.contains_key(&key) {
            return Err(OpenApiError(format!(
                "duplicate contract entry {method} {path}"
            )));
        }
        item.insert(key, operation(route)?);
    }

    Ok(json!({
        "openapi": "3.1.0",
        "info": info(contract),
        "servers": [{
            "url": CONVENTIONAL_MOUNT_PATH,
            "description": "Conventional HarvestPlugin mount point. Change it to the prefix \
                            passed to HarvestPlugin::api when the router is mounted elsewhere.",
        }],
        "security": security_requirements(),
        "tags": tags.into_values().collect::<Vec<_>>(),
        "paths": paths,
        "components": { "securitySchemes": security_schemes() },
    }))
}

/// Document-level metadata, including the contract version it was built from.
fn info(contract: &Value) -> Value {
    let version = contract["version"].as_str().unwrap_or("0.0.0");
    let contract_version = contract["contract_version"].as_str().unwrap_or("0");
    let auth_note = contract["auth_note"].as_str().unwrap_or_default();
    json!({
        "title": "Harvest management API",
        "version": version,
        "summary": "Control-plane HTTP API for autumn-harvest durable workflows.",
        "description": format!(
            "Generated from `docs/api-contract.json` (contract version {contract_version}). \
             Do not edit by hand. Regenerate with `cargo run -p autumn-harvest-plugin \
             --example emit_openapi > docs/openapi.json`.\n\n\
             Every path is relative to the prefix the embedder passes to \
             `HarvestPlugin::api`, `{CONVENTIONAL_MOUNT_PATH}` by convention.\n\n\
             Auth: {auth_note}"
        ),
        "x-harvest-contract-version": contract_version,
    })
}

/// Security schemes are declared, never enforced here (issue #174).
fn security_schemes() -> Value {
    json!({
        BEARER_SCHEME: {
            "type": "http",
            "scheme": "bearer",
            "description": "Scoped Harvest API token (issue #942). Tokens carry an `hvst_` \
                            prefix and a read or admin scope.",
        },
        SESSION_SCHEME: {
            "type": "apiKey",
            "in": "cookie",
            "name": "session",
            "description": "The embedding autumn-web application session. Admin-gated routes \
                            admit a session principal that holds the Harvest admin role.",
        },
    })
}

/// Both schemes, plus the empty requirement.
///
/// The empty requirement records the issue #174 posture exactly: enforcement is
/// the embedder's, so a generated client must not assume a credential is
/// mandatory on every route.
fn security_requirements() -> Value {
    json!([
        { BEARER_SCHEME: [] },
        { SESSION_SCHEME: [] },
        {},
    ])
}

/// Build one OpenAPI operation from one contract route.
fn operation(route: &Value) -> Result<Value, OpenApiError> {
    let method = string_field(route, "method")?;
    let path = string_field(route, "path")?;
    let description = string_field(route, "description")?;
    let category = string_field(route, "category")?;
    let read_only = route["read_only"]
        .as_bool()
        .ok_or_else(|| OpenApiError(format!("{method} {path}: `read_only` must be a boolean")))?;

    let mut operation = Map::new();
    operation.insert("operationId".to_owned(), json!(operation_id(method, path)));
    operation.insert("summary".to_owned(), json!(first_sentence(description)));
    operation.insert("description".to_owned(), json!(description));
    operation.insert("tags".to_owned(), json!([category]));
    operation.insert("x-harvest-read-only".to_owned(), json!(read_only));
    if let Some(idempotency) = route["idempotency"].as_str() {
        operation.insert("x-harvest-idempotency".to_owned(), json!(idempotency));
    }

    let parameters = parameters(route)?;
    if !parameters.is_empty() {
        operation.insert("parameters".to_owned(), Value::Array(parameters));
    }
    if let Some(body) = request_body(route) {
        operation.insert("requestBody".to_owned(), body);
    }
    operation.insert("responses".to_owned(), responses(route)?);

    Ok(Value::Object(operation))
}

/// A stable, unique operation id, so a generated client keeps method names
/// across regenerations. `{param}` becomes `by_param`.
fn operation_id(method: &str, path: &str) -> String {
    let mut id = method.to_lowercase();
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        id.push('_');
        if let Some(name) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
            id.push_str("by_");
            id.push_str(&sanitize(name));
        } else {
            id.push_str(&sanitize(segment));
        }
    }
    id
}

/// Lower-case an identifier and reduce every other character to `_`.
fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_underscore = false;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_underscore = false;
        } else if !last_underscore {
            out.push('_');
            last_underscore = true;
        }
    }
    out.trim_matches('_').to_owned()
}

/// The first sentence of a description, for the operation summary.
///
/// A summary is one line in a client's generated documentation, so it is
/// truncated. The full text stays in the operation description.
fn first_sentence(description: &str) -> String {
    let mut end = description.len();
    let bytes = description.as_bytes();
    for (index, window) in bytes.windows(2).enumerate() {
        if window[0] == b'.' && window[1].is_ascii_whitespace() {
            end = index + 1;
            break;
        }
    }
    let sentence = description[..end].trim();
    if sentence.chars().count() <= 160 {
        return sentence.to_owned();
    }
    let truncated: String = sentence.chars().take(157).collect();
    format!("{truncated}...")
}

/// Path, query and header parameters, each with an explicit `required` flag.
fn parameters(route: &Value) -> Result<Vec<Value>, OpenApiError> {
    let method = string_field(route, "method")?;
    let path = string_field(route, "path")?;
    let mut out = Vec::new();

    if let Some(params) = route["params"].as_array() {
        for param in params {
            let name = param["name"].as_str().ok_or_else(|| {
                OpenApiError(format!("{method} {path}: a parameter has no `name`"))
            })?;
            let location = param["in"].as_str().ok_or_else(|| {
                OpenApiError(format!("{method} {path}: parameter {name} has no `in`"))
            })?;
            let required = param["required"].as_bool().ok_or_else(|| {
                OpenApiError(format!(
                    "{method} {path}: parameter {name} has no boolean `required`"
                ))
            })?;
            let repeated = param["repeated"].as_bool().unwrap_or(false);
            out.push(parameter(
                name,
                location,
                required,
                param["description"].as_str(),
                param["type"].as_str(),
                repeated,
            ));
        }
    }

    if let Some(headers) = route["headers"].as_array() {
        for header in headers {
            let name = header["name"]
                .as_str()
                .ok_or_else(|| OpenApiError(format!("{method} {path}: a header has no `name`")))?;
            let required = header["required"].as_bool().unwrap_or(false);
            out.push(parameter(
                name,
                "header",
                required,
                header["description"].as_str(),
                header["type"].as_str(),
                false,
            ));
        }
    }

    Ok(out)
}

/// One parameter object. A repeated parameter is an exploded array, which is
/// how the handlers read a query key that may appear more than once.
fn parameter(
    name: &str,
    location: &str,
    required: bool,
    description: Option<&str>,
    declared_type: Option<&str>,
    repeated: bool,
) -> Value {
    let scalar = json!({ "type": declared_type.unwrap_or("string") });
    let schema = if repeated {
        json!({ "type": "array", "items": scalar })
    } else {
        scalar
    };
    let mut param = Map::new();
    param.insert("name".to_owned(), json!(name));
    param.insert("in".to_owned(), json!(location));
    param.insert("required".to_owned(), json!(required));
    if let Some(description) = description {
        param.insert("description".to_owned(), json!(description));
    }
    param.insert("schema".to_owned(), schema);
    if repeated {
        param.insert("style".to_owned(), json!("form"));
        param.insert("explode".to_owned(), json!(true));
    }
    Value::Object(param)
}

/// The request body, or `None` for a route that accepts no body.
///
/// A contract entry with `free_form: false` and an empty `fields` array
/// documents a route that takes no body at all, such as a `DELETE`.
fn request_body(route: &Value) -> Option<Value> {
    let body = route["request_body"].as_object()?;
    let free_form = body
        .get("free_form")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let fields: &[Value] = body
        .get("fields")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);
    let empty = fields.is_empty();
    if !free_form && empty {
        return None;
    }

    let description = body.get("description").and_then(Value::as_str);
    let schema = if free_form {
        json!({
            "description": description
                .unwrap_or("Opaque JSON body. See the operation description for its shape."),
        })
    } else {
        object_schema(fields, None, description)
    };

    Some(json!({
        "required": body.get("required").and_then(Value::as_bool).unwrap_or(false),
        "content": { DEFAULT_MEDIA_TYPE: { "schema": schema } },
    }))
}

/// Every documented response, keyed by status code.
///
/// Statuses can repeat across the contract's success, additional and error
/// lists, so descriptions for one status are merged rather than overwritten.
fn responses(route: &Value) -> Result<Value, OpenApiError> {
    let method = string_field(route, "method")?;
    let path = string_field(route, "path")?;
    let success = &route["success_response"];
    let status = success["status"].as_u64().ok_or_else(|| {
        OpenApiError(format!(
            "{method} {path}: `success_response.status` must be a number"
        ))
    })?;

    let mut descriptions: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    descriptions
        .entry(status)
        .or_default()
        .push(success_description(success));

    for key in ["additional_responses", "error_responses"] {
        for response in route[key].as_array().into_iter().flatten() {
            let Some(status) = response["status"].as_u64() else {
                return Err(OpenApiError(format!(
                    "{method} {path}: a {key} entry has no numeric `status`"
                )));
            };
            let description = response["description"]
                .as_str()
                .unwrap_or("No description recorded.")
                .to_owned();
            descriptions.entry(status).or_default().push(description);
        }
    }

    let media_type = success["content_type"]
        .as_str()
        .unwrap_or(DEFAULT_MEDIA_TYPE);
    let mut out = Map::new();
    for (code, texts) in descriptions {
        let mut response = Map::new();
        response.insert("description".to_owned(), json!(texts.join(" ")));
        // Only the success status has a documented body shape. A 204 carries
        // no body at all.
        if code == status && code != 204 {
            response.insert(
                "content".to_owned(),
                json!({ media_type: { "schema": success_schema(success, media_type) } }),
            );
        }
        out.insert(code.to_string(), Value::Object(response));
    }
    Ok(Value::Object(out))
}

/// Merge the contract's several description keys into one response description.
fn success_description(success: &Value) -> String {
    let mut parts = Vec::new();
    for key in ["description", "note", "notes"] {
        if let Some(text) = success[key].as_str() {
            parts.push(text.to_owned());
        }
    }
    if parts.is_empty() {
        parts.push("Success.".to_owned());
    }
    parts.join(" ")
}

/// The success body schema.
///
/// A stream response is a sequence of `text/event-stream` frames, not a JSON
/// document, so it is typed as a string.
fn success_schema(success: &Value, media_type: &str) -> Value {
    if media_type != DEFAULT_MEDIA_TYPE {
        return json!({
            "type": "string",
            "description": "Server-sent event frames. See the response description.",
        });
    }
    let free_form = success["free_form"].as_bool().unwrap_or(false);
    let fields = success["fields"].as_array();
    match (free_form, fields) {
        (false, Some(fields)) if !fields.is_empty() => {
            object_schema(fields, success["field_notes"].as_object(), None)
        }
        _ => json!({
            "description": "Shape is documented in the response description, not as a schema.",
        }),
    }
}

/// An object schema built from a contract field list.
///
/// The contract records field names, and sometimes a type; it does not record a
/// full JSON Schema. Properties are therefore left open unless a type is
/// declared. `additionalProperties` stays unset because the contract allows
/// additive response fields without a breaking change.
///
/// A field is either a bare name or an object carrying `name`, `description`,
/// `type` and `required`. Response lists use both forms; request lists use the
/// object form only.
fn object_schema(
    fields: &[Value],
    field_notes: Option<&Map<String, Value>>,
    description: Option<&str>,
) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for field in fields {
        let Some(name) = field_name(field) else {
            continue;
        };
        let mut property = Map::new();
        if let Some(declared_type) = field["type"].as_str() {
            property.insert("type".to_owned(), json!(declared_type));
        }
        let note = field_notes
            .and_then(|notes| notes.get(name))
            .and_then(Value::as_str);
        let text = match (field["description"].as_str(), note) {
            (Some(description), Some(note)) => Some(format!("{description} {note}")),
            (Some(text), None) | (None, Some(text)) => Some(text.to_owned()),
            (None, None) => None,
        };
        if let Some(text) = text {
            property.insert("description".to_owned(), json!(text));
        }
        if field["required"].as_bool().unwrap_or(false) {
            required.push(json!(name));
        }
        properties.insert(name.to_owned(), Value::Object(property));
    }

    let mut schema = Map::new();
    schema.insert("type".to_owned(), json!("object"));
    if let Some(description) = description {
        schema.insert("description".to_owned(), json!(description));
    }
    schema.insert("properties".to_owned(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_owned(), Value::Array(required));
    }
    Value::Object(schema)
}

/// The name of a contract field, in either of its two forms.
fn field_name(field: &Value) -> Option<&str> {
    field.as_str().or_else(|| field["name"].as_str())
}

/// Read a required string field, naming the offender on failure.
fn string_field<'a>(route: &'a Value, key: &str) -> Result<&'a str, OpenApiError> {
    route[key]
        .as_str()
        .ok_or_else(|| OpenApiError(format!("a route entry has no string `{key}`")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract_transforms_cleanly() {
        let contract: Value = serde_json::from_str(API_CONTRACT_JSON).expect("valid JSON");
        let document = document_from_contract(&contract).expect("contract must transform");
        assert_eq!(document["openapi"], "3.1.0");
    }

    #[test]
    fn operation_ids_are_derived_from_method_and_path() {
        assert_eq!(operation_id("GET", "/workflows"), "get_workflows");
        assert_eq!(
            operation_id("POST", "/workflows/{workflow_name}/start"),
            "post_workflows_by_workflow_name_start"
        );
        assert_eq!(operation_id("GET", "/openapi.json"), "get_openapi_json");
    }

    #[test]
    fn a_parameter_without_a_required_flag_is_rejected() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "category": "admin",
            "read_only": true,
            "description": "X.",
            "params": [{ "name": "q", "in": "query", "description": "Q." }],
            "success_response": { "status": 200, "free_form": true },
            "error_responses": [],
        });
        let error = operation(&route).expect_err("a missing required flag must fail");
        assert!(error.to_string().contains("boolean `required`"), "{error}");
    }

    #[test]
    fn a_route_with_no_body_fields_declares_no_request_body() {
        let route = json!({
            "request_body": { "required": false, "free_form": false, "fields": [] },
        });
        assert!(request_body(&route).is_none());
    }

    #[test]
    fn a_free_form_body_still_declares_a_schema() {
        let route = json!({
            "request_body": { "required": true, "free_form": true },
        });
        let body = request_body(&route).expect("a free-form body is still a body");
        assert_eq!(body["required"], json!(true));
        assert!(body["content"]["application/json"]["schema"].is_object());
    }

    #[test]
    fn repeated_query_parameters_are_exploded_arrays() {
        let param = parameter("search_attr", "query", false, Some("Filter."), None, true);
        assert_eq!(param["schema"]["type"], "array");
        assert_eq!(param["schema"]["items"]["type"], "string");
        assert_eq!(param["explode"], json!(true));
    }

    #[test]
    fn duplicate_statuses_merge_their_descriptions() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "success_response": { "status": 400, "free_form": true, "note": "Always 400." },
            "error_responses": [{ "status": 400, "description": "Bad input." }],
        });
        let responses = responses(&route).expect("responses must build");
        let description = responses["400"]["description"].as_str().unwrap();
        assert!(description.contains("Always 400."), "{description}");
        assert!(description.contains("Bad input."), "{description}");
    }

    #[test]
    fn a_stream_response_is_typed_as_event_frames() {
        let success = json!({
            "status": 200,
            "free_form": true,
            "content_type": "text/event-stream",
        });
        let schema = success_schema(&success, "text/event-stream");
        assert_eq!(schema["type"], "string");
    }

    #[test]
    fn a_field_list_accepts_bare_names_and_objects() {
        let fields = vec![
            json!("worker_id"),
            json!({ "name": "queues", "type": "array" }),
        ];
        let schema = object_schema(&fields, None, None);
        assert!(schema["properties"]["worker_id"].is_object());
        assert_eq!(schema["properties"]["queues"]["type"], "array");
    }

    #[test]
    fn a_summary_is_the_first_sentence() {
        assert_eq!(first_sentence("Start a run. More text."), "Start a run.");
        assert_eq!(first_sentence("No trailing period"), "No trailing period");
    }
}
