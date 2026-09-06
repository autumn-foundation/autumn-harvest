// OpenAPI and JSON-schema recur throughout this file and are legitimate
// acronyms. Silence clippy::doc_markdown here rather than wrapping every
// mention in backticks, matching `autumn_web::openapi`.
#![allow(clippy::doc_markdown)]

//! Transform the management API contract into an OpenAPI 3.1 document (issue #694).
//!
//! Prints the document to stdout. `scripts/regenerate-openapi.sh` runs it twice
//! to write the two checked-in copies:
//!
//! ```sh
//! emit_openapi docs/api-contract.json           > docs/openapi.json
//! emit_openapi docs/api-contract.json --compact > autumn-harvest-plugin/openapi.json
//! ```
//!
//! The contract path is an argument, not an `include_str!`, on purpose. A
//! published crate carries no file from outside its own directory. Nothing
//! here may therefore reach into `docs/` at compile time.

use std::process::ExitCode;

use autumn_harvest_plugin::openapi::document_from_contract;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: emit_openapi <path-to-api-contract.json> [--compact]");
        return ExitCode::FAILURE;
    };
    let compact = args.any(|arg| arg == "--compact");

    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) => {
            eprintln!("cannot read {path}: {error}");
            return ExitCode::FAILURE;
        }
    };
    let contract: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(contract) => contract,
        Err(error) => {
            eprintln!("{path} is not valid JSON: {error}");
            return ExitCode::FAILURE;
        }
    };
    let document = match document_from_contract(&contract) {
        Ok(document) => document,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };

    let json = if compact {
        serde_json::to_string(&document)
    } else {
        serde_json::to_string_pretty(&document)
    };
    match json {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("the document does not serialize: {error}");
            ExitCode::FAILURE
        }
    }
}
