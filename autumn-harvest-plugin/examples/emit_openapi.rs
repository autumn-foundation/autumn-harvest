// OpenAPI and JSON-schema recur throughout this file and are legitimate
// acronyms. Silence clippy::doc_markdown here rather than wrapping every
// mention in backticks, matching `autumn_web::openapi`.
#![allow(clippy::doc_markdown)]

//! Print the management API OpenAPI 3.1 document to stdout (issue #694).
//!
//! This is how `docs/openapi.json` is produced:
//!
//! ```sh
//! cargo run -p autumn-harvest-plugin --example emit_openapi > docs/openapi.json
//! ```
//!
//! The document is derived from `docs/api-contract.json`, and the served
//! `GET /openapi.json` endpoint returns the same document, so the artifact and
//! the endpoint cannot disagree. `tests/openapi_spec.rs` fails when the checked-in
//! artifact is stale.

fn main() {
    let document = autumn_harvest_plugin::openapi::openapi_document();
    let json = serde_json::to_string_pretty(document).expect("the document must serialize");
    println!("{json}");
}
