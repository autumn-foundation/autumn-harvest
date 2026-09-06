// OpenAPI, JSON and JSON-schema recur throughout this module and are
// legitimate acronyms. Silence clippy::doc_markdown here rather than wrapping
// every mention in backticks, matching `autumn_web::openapi`.
#![allow(clippy::doc_markdown)]

//! OpenAPI 3.1 document for the Harvest management API (issue #694).
//!
//! # One source
//!
//! `docs/api-contract.json` is the single source of truth for the management
//! route surface. [`document_from_contract`] transforms it into an OpenAPI 3.1
//! document. Two checked-in files hold that document, and one command writes
//! both:
//!
//! * `autumn-harvest-plugin/openapi.json` — compact, compiled in below, and
//!   served verbatim by `GET /openapi.json`. It lives inside the crate because
//!   a published crate can carry no file from outside its own directory.
//! * `docs/openapi.json` — the same document, pretty-printed for reading and
//!   for review diffs.
//!
//! `tests/openapi_spec.rs` fails when either file drifts from the transform, or
//! from the other. Regenerate both with `scripts/regenerate-openapi.sh`.
//!
//! `tests/contract_regression.rs` pins the contract to the live router. It
//! fails when a route exists in `harvest_api_router` but not in the contract.
//! It fails the other way too. A route therefore cannot reach the router
//! without reaching the published spec.
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
//! The served endpoint returns compiled-in bytes. It runs no transform, parses
//! nothing, and allocates nothing, so a contract defect can never surface as a
//! failed or panicking request. The transform runs in the generator and in
//! tests, where a defect names the offending route and fails the build.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use autumn_web::reexports::axum::response::IntoResponse;
use autumn_web::reexports::http::header;
use serde_json::{Map, Value, json};

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

/// `x-harvest-route-class` value for a route that needs no credential.
const PUBLIC_SAFE: &str = "public_safe";

/// A contract that cannot be transformed into a valid OpenAPI document.
#[derive(Debug, thiserror::Error)]
#[error("docs/api-contract.json is not transformable into OpenAPI 3.1: {0}")]
pub struct OpenApiError(String);

/// The generated document, compiled in and served verbatim.
///
/// Written by `scripts/regenerate-openapi.sh`. Never edit it by hand.
const OPENAPI_JSON: &str = include_str!("../openapi.json");

static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(OPENAPI_JSON).expect("the generated openapi.json must be valid JSON")
});

/// The OpenAPI 3.1 document for the management API, parsed once.
///
/// # Panics
///
/// Panics when the compiled-in `openapi.json` is not valid JSON. The generator
/// writes it and `tests/openapi_spec.rs` parses it, so a build that ships a
/// malformed one cannot pass CI. The served endpoint never calls this.
#[must_use]
pub fn openapi_document() -> &'static Value {
    &DOCUMENT
}

/// The document exactly as served: compact JSON, compiled in.
#[must_use]
pub const fn openapi_json() -> &'static str {
    OPENAPI_JSON
}

/// `GET /openapi.json` — serve the document.
///
/// Read-only, reads no state, and copies no bytes: the body is a `&'static str`
/// from the binary. Nothing here can fail.
pub(crate) async fn get_openapi_document() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, DEFAULT_MEDIA_TYPE)], OPENAPI_JSON)
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
    let mut operation_ids: BTreeSet<String> = BTreeSet::new();

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
        let operation = operation(route)?;
        // A repeated operationId is invalid OpenAPI, and a generator would
        // silently drop or overwrite one of the two methods it names. Two
        // different paths can sanitize to the same id, so check here rather
        // than trusting the path check above.
        let id = operation["operationId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if !operation_ids.insert(id.clone()) {
            return Err(OpenApiError(format!(
                "{method} {path}: operationId `{id}` is already used by another route"
            )));
        }
        item.insert(key, operation);
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
             Do not edit by hand. Regenerate with \
             `scripts/regenerate-openapi.sh`.\n\n\
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
/// The empty requirement records the issue #174 posture exactly. Enforcement
/// belongs to the embedder. A generated client must therefore not assume a
/// credential is mandatory on every route.
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
    operation.insert(
        "x-harvest-route-class".to_owned(),
        json!(route_class(method, path)),
    );
    if let Some(idempotency) = route["idempotency"].as_str() {
        operation.insert("x-harvest-idempotency".to_owned(), json!(idempotency));
    }
    // A stream stays open until the execution reaches a terminal state. A
    // blocking generated client that buffers the whole body would hang, so the
    // document says so where a generator can read it.
    if route["success_response"]["content_type"]
        .as_str()
        .is_some_and(|media_type| media_type != DEFAULT_MEDIA_TYPE)
    {
        operation.insert("x-harvest-stream".to_owned(), json!(true));
    }
    // A route the engine classifies PublicSafe needs no credential, ever. The
    // document-level requirement leaves credentials optional everywhere, which
    // cannot say that. An empty operation-level list can (issue #174).
    if route_class(method, path) == PUBLIC_SAFE {
        operation.insert("security".to_owned(), json!([]));
    }

    let parameters = parameters(route)?;
    if !parameters.is_empty() {
        operation.insert("parameters".to_owned(), Value::Array(parameters));
    }
    if let Some(body) = request_body(route)? {
        operation.insert("requestBody".to_owned(), body);
    }
    operation.insert("responses".to_owned(), responses(route)?);

    Ok(Value::Object(operation))
}

/// The engine's own security class for a route, as a document extension.
///
/// Read from [`autumn_harvest::audit::CLASSIFIED_ROUTES`], the table the
/// read-only operator role enforces against, so the published class cannot
/// drift from the enforced one. An unclassified route reports `unknown`;
/// `contract_regression::every_management_route_is_classified` makes that
/// impossible for a mounted route.
fn route_class(method: &str, path: &str) -> &'static str {
    use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};

    let key = format!("{method} {path}");
    CLASSIFIED_ROUTES
        .iter()
        .find(|(template, _)| *template == key)
        .map_or("unknown", |(_, class)| match class {
            RouteClass::PublicSafe => PUBLIC_SAFE,
            RouteClass::ReadOnly => "read_only",
            RouteClass::Mutating => "mutating",
        })
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
/// documents a route that takes no body at all, such as a `DELETE`. A read
/// method never publishes one: `GET` and `HEAD` bodies have no defined
/// meaning, and several HTTP clients refuse to send them.
///
/// # Errors
///
/// Returns [`OpenApiError`] when a body-bearing entry omits its boolean
/// `required` flag. That flag decides whether a generated client may omit the
/// body, so guessing it produces a client the handler rejects.
fn request_body(route: &Value) -> Result<Option<Value>, OpenApiError> {
    let method = string_field(route, "method")?;
    let path = string_field(route, "path")?;
    let Some(body) = route["request_body"].as_object() else {
        return Ok(None);
    };
    let free_form = body
        .get("free_form")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let fields: &[Value] = body
        .get("fields")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);
    if !free_form && fields.is_empty() {
        return Ok(None);
    }
    if matches!(method, "GET" | "HEAD") {
        return Err(OpenApiError(format!(
            "{method} {path}: a read method must not document a request body"
        )));
    }

    let required = body
        .get("required")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            OpenApiError(format!(
                "{method} {path}: `request_body` has no boolean `required`"
            ))
        })?;
    let description = body.get("description").and_then(Value::as_str);
    // `free_form` with fields is a documented shape plus room to grow, so the
    // fields still reach the client. Dropping them would publish less than the
    // contract records.
    let schema = if fields.is_empty() {
        json!({
            "description": description
                .unwrap_or("Opaque JSON body. See the operation description for its shape."),
        })
    } else {
        object_schema(fields, None, description)
    };

    Ok(Some(json!({
        "required": required,
        "content": { DEFAULT_MEDIA_TYPE: { "schema": schema } },
    })))
}

/// Every documented response, keyed by status code.
///
/// Statuses can repeat across the contract's success, additional and error
/// lists, so descriptions for one status are merged rather than overwritten.
/// An `additional_responses` entry that documents its own body publishes a
/// schema of its own. An entry that documents none is description-only. Every
/// `error_responses` entry is description-only today.
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
    let mut bodies: BTreeMap<u64, Value> = BTreeMap::new();
    let mut headers: BTreeMap<u64, Value> = BTreeMap::new();

    descriptions
        .entry(status)
        .or_default()
        .push(description_of(success, method, path)?);
    let media_type = success["content_type"]
        .as_str()
        .unwrap_or(DEFAULT_MEDIA_TYPE);
    // A 204 carries no body at all.
    if status != 204 {
        bodies.insert(
            status,
            json!({ media_type: { "schema": success_schema(success, media_type) } }),
        );
    }
    if let Some(declared) = response_headers(success) {
        headers.insert(status, declared);
    }

    for key in ["additional_responses", "error_responses"] {
        for response in route[key].as_array().into_iter().flatten() {
            let Some(code) = response["status"].as_u64() else {
                return Err(OpenApiError(format!(
                    "{method} {path}: a {key} entry has no numeric `status`"
                )));
            };
            descriptions
                .entry(code)
                .or_default()
                .push(description_of(response, method, path)?);
            let documents_a_body = response["free_form"].as_bool().unwrap_or(false)
                || response["fields"]
                    .as_array()
                    .is_some_and(|fields| !fields.is_empty());
            if documents_a_body && code != 204 && !bodies.contains_key(&code) {
                let media_type = response["content_type"]
                    .as_str()
                    .unwrap_or(DEFAULT_MEDIA_TYPE);
                bodies.insert(
                    code,
                    json!({ media_type: { "schema": success_schema(response, media_type) } }),
                );
            }
            if let Some(declared) = response_headers(response) {
                headers.entry(code).or_insert(declared);
            }
        }
    }

    let mut out = Map::new();
    for (code, texts) in descriptions {
        let mut response = Map::new();
        response.insert("description".to_owned(), json!(join_unique(texts)));
        if let Some(content) = bodies.remove(&code) {
            response.insert("content".to_owned(), content);
        }
        if let Some(declared) = headers.remove(&code) {
            response.insert("headers".to_owned(), declared);
        }
        out.insert(code.to_string(), Value::Object(response));
    }
    Ok(Value::Object(out))
}

/// Response headers a caller can read, from `headers` on a response entry.
fn response_headers(response: &Value) -> Option<Value> {
    let declared = response["headers"].as_array()?;
    let mut out = Map::new();
    for header in declared {
        let Some(name) = header["name"].as_str() else {
            continue;
        };
        let mut entry = Map::new();
        if let Some(description) = header["description"].as_str() {
            entry.insert("description".to_owned(), json!(description));
        }
        entry.insert(
            "schema".to_owned(),
            json!({ "type": header["type"].as_str().unwrap_or("string") }),
        );
        out.insert(name.to_owned(), Value::Object(entry));
    }
    if out.is_empty() {
        None
    } else {
        Some(Value::Object(out))
    }
}

/// Merge the contract's several description keys into one response description.
///
/// # Errors
///
/// Returns [`OpenApiError`] when a description key holds neither a string nor a
/// list of strings. Ignoring it would drop documented behaviour silently.
fn description_of(response: &Value, method: &str, path: &str) -> Result<String, OpenApiError> {
    let mut parts = Vec::new();
    for key in ["description", "note", "notes"] {
        match &response[key] {
            Value::Null => {}
            Value::String(text) => parts.push(text.clone()),
            Value::Array(items) => {
                for item in items {
                    let text = item.as_str().ok_or_else(|| {
                        OpenApiError(format!("{method} {path}: `{key}` holds a non-string entry"))
                    })?;
                    parts.push(text.to_owned());
                }
            }
            _ => {
                return Err(OpenApiError(format!(
                    "{method} {path}: `{key}` must be a string or a list of strings"
                )));
            }
        }
    }
    if parts.is_empty() {
        parts.push("Success.".to_owned());
    }
    Ok(join_unique(parts))
}

/// Join text fragments with a space, dropping an exact repeat.
///
/// A status that appears in both the success entry and an error list otherwise
/// reads its own sentence twice.
fn join_unique(parts: Vec<String>) -> String {
    let mut seen = BTreeSet::new();
    parts
        .into_iter()
        .filter(|part| !part.is_empty() && seen.insert(part.clone()))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The body schema for one response entry.
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
    // `free_form` with fields is a documented shape plus room to grow. The
    // fields therefore still reach the client. The request side agrees.
    let fields = success["fields"].as_array();
    match fields {
        Some(fields) if !fields.is_empty() => {
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
    fn the_compiled_in_document_is_openapi_3_1() {
        assert_eq!(openapi_document()["openapi"], "3.1.0");
        assert!(
            openapi_document()["paths"]
                .as_object()
                .is_some_and(|p| !p.is_empty())
        );
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
            "method": "DELETE",
            "path": "/x",
            "request_body": { "required": false, "free_form": false, "fields": [] },
        });
        assert!(
            request_body(&route)
                .expect("no body is not an error")
                .is_none()
        );
    }

    #[test]
    fn a_free_form_body_still_declares_a_schema() {
        let route = json!({
            "method": "POST",
            "path": "/x",
            "request_body": { "required": true, "free_form": true },
        });
        let body = request_body(&route)
            .expect("a free-form body transforms")
            .expect("a free-form body is still a body");
        assert_eq!(body["required"], json!(true));
        assert!(body["content"]["application/json"]["schema"].is_object());
    }

    #[test]
    fn a_body_without_a_required_flag_is_rejected() {
        let route = json!({
            "method": "POST",
            "path": "/x",
            "request_body": { "free_form": true },
        });
        let error = request_body(&route).expect_err("a missing required flag must fail");
        assert!(error.to_string().contains("boolean `required`"), "{error}");
    }

    #[test]
    fn a_read_method_must_not_document_a_body() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "request_body": { "required": false, "free_form": true },
        });
        let error = request_body(&route).expect_err("a GET body must fail");
        assert!(error.to_string().contains("read method"), "{error}");
    }

    #[test]
    fn a_free_form_body_with_fields_keeps_the_fields() {
        let route = json!({
            "method": "POST",
            "path": "/x",
            "request_body": {
                "required": true,
                "free_form": true,
                "fields": [{ "name": "reason", "description": "Why." }],
            },
        });
        let body = request_body(&route)
            .expect("transforms")
            .expect("has a body");
        let schema = &body["content"]["application/json"]["schema"];
        assert!(schema["properties"]["reason"].is_object(), "{schema}");
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
    fn an_additional_response_with_fields_publishes_a_schema() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "success_response": { "status": 200, "free_form": true },
            "additional_responses": [{
                "status": 202,
                "description": "Admitted.",
                "fields": [{ "name": "update_id" }],
            }],
            "error_responses": [],
        });
        let responses = responses(&route).expect("responses must build");
        assert!(
            responses["202"]["content"]["application/json"]["schema"]["properties"]["update_id"]
                .is_object(),
            "{responses}"
        );
    }

    #[test]
    fn a_response_header_reaches_the_document() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "success_response": {
                "status": 200,
                "free_form": true,
                "headers": [{ "name": "X-Harvest-Execution-Id", "description": "Resolved id." }],
            },
            "error_responses": [],
        });
        let responses = responses(&route).expect("responses must build");
        assert_eq!(
            responses["200"]["headers"]["X-Harvest-Execution-Id"]["schema"]["type"],
            "string"
        );
    }

    #[test]
    fn a_non_string_note_is_rejected() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "success_response": { "status": 200, "free_form": true, "note": 7 },
            "error_responses": [],
        });
        let error = responses(&route).expect_err("a numeric note must fail");
        assert!(error.to_string().contains("`note`"), "{error}");
    }

    #[test]
    fn a_repeated_description_is_said_once() {
        let route = json!({
            "method": "GET",
            "path": "/x",
            "success_response": { "status": 400, "free_form": true, "note": "Always 400." },
            "error_responses": [{ "status": 400, "description": "Always 400." }],
        });
        let responses = responses(&route).expect("responses must build");
        assert_eq!(responses["400"]["description"], "Always 400.");
    }

    #[test]
    fn a_duplicate_operation_id_is_rejected() {
        let contract = json!({
            "routes": [
                {
                    "method": "GET", "path": "/a-b", "category": "admin", "read_only": true,
                    "description": "A.", "params": [],
                    "success_response": { "status": 200, "free_form": true },
                    "error_responses": [],
                },
                {
                    "method": "GET", "path": "/a_b", "category": "admin", "read_only": true,
                    "description": "B.", "params": [],
                    "success_response": { "status": 200, "free_form": true },
                    "error_responses": [],
                },
            ],
        });
        let error = document_from_contract(&contract).expect_err("a collision must fail");
        assert!(error.to_string().contains("already used"), "{error}");
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
