//! Deterministic (non-criterion) instruction/allocation profiling harness for
//! `schema_contract::diff_schema_contracts`, the differ behind
//! `harvest schema check` and the CI schema gate.
//!
//! Wall-clock time is not admissible on this machine. Use
//! `valgrind --tool=callgrind` for instructions and `valgrind --tool=dhat`
//! for allocations.
//!
//! # Workload
//!
//! A mature deployment publishes hundreds of workflow and activity types,
//! each with schemars-shaped draft-07 schemas: `definitions`, `$ref`,
//! `oneOf`, enums and doc-comment annotations. The gate diffs the checked-in
//! baseline against the contract built from the current build. Most entries
//! are unchanged. One in `CHANGE_EVERY` gets an annotation edit, a new
//! optional field, or a widened enum.
//!
//! Knobs (env): `SCHEMA_DIFF_WORKFLOWS` (default 200), `SCHEMA_DIFF_ACTIVITIES`
//! (default 400), `SCHEMA_DIFF_REPS` (default 1). `SCHEMA_DIFF_BUILD_ONLY=1`
//! builds both contracts and skips the diff. Subtract that run's dhat totals
//! to isolate the allocations of the diff itself.

use autumn_harvest::schema_contract::{
    ActivitySchemaEntry, WorkflowSchemaContract, WorkflowSchemaEntry, canonicalize_schema,
    diff_schema_contracts,
};
use serde_json::{Value, json};

const CHANGE_EVERY: usize = 5;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A schemars-shaped payload schema. `variant` selects an edit.
/// 0 = base, 1 = annotation edit only, 2 = new optional field, 3 = widened enum.
fn payload_schema(i: usize, variant: usize) -> Value {
    let mut statuses = vec!["pending", "running", "done"];
    if variant == 3 {
        statuses.push("cancelled");
    }
    let mut props = json!({
        "id": {"type": "string", "format": "uuid", "description": "Unique id."},
        "customer": {"$ref": "#/definitions/Customer"},
        "items": {
            "type": "array",
            "items": {"$ref": "#/definitions/LineItem"},
            "description": "Order lines."
        },
        "status": {"$ref": "#/definitions/Status"},
        "channel": {
            "oneOf": [
                {"type": "object", "required": ["web"], "properties": {"web": {"type": "string"}}},
                {"type": "object", "required": ["api"], "properties": {"api": {"type": "integer", "format": "int64"}}}
            ]
        },
        "note": {"type": ["string", "null"], "title": "Free text"},
        "retries": {"type": "integer", "format": "uint32", "minimum": 0.0}
    });
    if variant == 2 {
        props["extra"] = json!({"type": ["string", "null"]});
    }
    let desc = if variant == 1 {
        format!("Edited doc comment {i}")
    } else {
        format!("Payload {i}")
    };
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": format!("Payload{i}"),
        "description": desc,
        "type": "object",
        "required": ["id", "customer", "items", "status"],
        "properties": props,
        "definitions": {
            "Customer": {
                "description": "A customer.",
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string", "description": "Display name."},
                    "email": {"type": ["string", "null"]},
                    "tags": {"type": "array", "items": {"type": "string"}}
                }
            },
            "LineItem": {
                "type": "object",
                "required": ["sku", "qty"],
                "properties": {
                    "sku": {"type": "string"},
                    "qty": {"type": "integer", "format": "uint32", "minimum": 0.0},
                    "price": {"type": "number", "format": "double"}
                }
            },
            "Status": {"type": "string", "enum": statuses}
        }
    })
}

fn variant_for(i: usize, current: bool) -> usize {
    if current && i % CHANGE_EVERY == 0 {
        1 + (i / CHANGE_EVERY) % 3
    } else {
        0
    }
}

fn build(workflows: usize, activities: usize, current: bool) -> WorkflowSchemaContract {
    let entries = (0..workflows).map(|i| {
        let v = variant_for(i, current);
        WorkflowSchemaEntry {
            name: format!("workflow_{i:05}"),
            description: Some(format!("Workflow {i}")),
            input_schema: Some(payload_schema(i, v)),
            output_schema: Some(payload_schema(i + 7, 0)),
            error_schema: Some(
                json!({"type": "object", "properties": {"message": {"type": "string"}}}),
            ),
        }
    });
    let mut contract = WorkflowSchemaContract::from_entries("0.0.0", entries);
    contract.activities = (0..activities)
        .map(|i| ActivitySchemaEntry {
            name: format!("activity_{i:05}"),
            // Stored contracts hold canonical schemas: `with_activities` and
            // `parse` canonicalise on the way in, so do the same here.
            input_schema: Some(canonicalize_schema(&payload_schema(
                i + 3,
                variant_for(i, current),
            ))),
            output_schema: Some(canonicalize_schema(&payload_schema(i + 11, 0))),
        })
        .collect();
    contract
}

fn main() {
    let workflows = env_usize("SCHEMA_DIFF_WORKFLOWS", 200);
    let activities = env_usize("SCHEMA_DIFF_ACTIVITIES", 400);
    let reps = env_usize("SCHEMA_DIFF_REPS", 1);
    assert!(reps > 0, "SCHEMA_DIFF_REPS must be at least 1");

    let baseline = build(workflows, activities, false);
    let current = build(workflows, activities, true);

    if std::env::var_os("SCHEMA_DIFF_BUILD_ONLY").is_some() {
        println!("build only: workflows={workflows} activities={activities}");
        return;
    }

    let mut total = 0usize;
    let mut breaking = 0usize;
    for _ in 0..reps {
        let diff = diff_schema_contracts(&baseline, &current);
        total += diff.deltas.len();
        breaking += diff.breaking_count;
        std::hint::black_box(&diff);
    }
    println!(
        "workflows={workflows} activities={activities} reps={reps} deltas={total} breaking={breaking}"
    );
}
