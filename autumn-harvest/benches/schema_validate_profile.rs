//! Deterministic instruction/allocation-count profiling harness for
//! `autumn_harvest::info::validate_against_schema`.
//!
//! It is the hand-rolled JSON Schema walker. It runs on each workflow start,
//! signal-with-start and update-with-start call for a workflow with a published
//! input schema (issue #373). It also runs on each completion-trigger fan-in
//! delivery (`completion_trigger.rs`). Unlike the `replay_profile.rs` workload,
//! this function has no feature gate. So each deployment that publishes a
//! schema pays this cost on the HTTP request path.
//!
//! `harness = false` + its own `main()`, same shape as `claim_bench.rs` and
//! `replay_profile.rs`. The artifact is a plain executable for
//! `valgrind --tool=callgrind` / `valgrind --tool=dhat`, not for the
//! `cargo bench` wall-clock loop. `validate_against_schema` is a synchronous,
//! in-memory, sub-microsecond call. On this shared-vCPU machine only
//! deterministic counters give real signal, never wall time.
//!
//! # Workload
//!
//! A realistic order-checkout input schema is validated N times against a
//! **matching valid payload**. The schema has nested objects, an array of
//! line-item objects, `required`, `enum`, `minLength`/`maxLength` and
//! `minimum`/`maximum`. It uses `additionalProperties` in its `false` form
//! (reject unknown keys) and its schema-object form (a free-form `metadata` map).
//! The valid-payload case is the one measured, because most production requests
//! are well-formed. It is also the case where eagerly computed diagnostic work
//! is pure waste, because no violation is ever pushed.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --bench schema_validate_profile
//! ```
//!
//! `SCHEMA_PROFILE_N` (default `5_000`) controls the iteration count.

use autumn_harvest::info::validate_against_schema;
use serde_json::{Value, json};

fn order_schema() -> Value {
    json!({
        "type": "object",
        "required": ["order_id", "customer", "items", "shipping_address"],
        "additionalProperties": false,
        "properties": {
            "order_id": {"type": "string", "minLength": 3, "maxLength": 64},
            "customer": {
                "type": "object",
                "required": ["id", "email", "tier"],
                "additionalProperties": false,
                "properties": {
                    "id": {"type": "string"},
                    "email": {"type": "string", "minLength": 5, "maxLength": 254},
                    "tier": {"type": "string", "enum": ["standard", "gold", "platinum"]}
                }
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["sku", "qty", "unit_cents"],
                    "additionalProperties": false,
                    "properties": {
                        "sku": {"type": "string", "minLength": 1, "maxLength": 40},
                        "qty": {"type": "integer", "minimum": 1, "maximum": 999},
                        "unit_cents": {"type": "integer", "minimum": 0}
                    }
                }
            },
            "shipping_address": {
                "type": "object",
                "required": ["street", "city", "zip", "country"],
                "additionalProperties": false,
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                    "zip": {"type": "string"},
                    "country": {"type": "string", "minLength": 2, "maxLength": 2}
                }
            },
            "metadata": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            }
        }
    })
}

/// A valid order payload with six line items and a populated metadata map.
/// It exercises each branch of the schema on the happy path with zero
/// violations.
fn order_payload() -> Value {
    let items: Vec<Value> = (0..6)
        .map(|i| {
            json!({
                "sku": format!("SKU-{i:04}"),
                "qty": 1 + (i % 5),
                "unit_cents": 999 + i * 100,
            })
        })
        .collect();
    json!({
        "order_id": "order-00001234",
        "customer": {
            "id": "cust-9981",
            "email": "alice@example.com",
            "tier": "gold"
        },
        "items": items,
        "shipping_address": {
            "street": "1 Market St",
            "city": "San Francisco",
            "zip": "94105",
            "country": "US"
        },
        "metadata": {
            "campaign": "spring-sale",
            "referrer": "email"
        }
    })
}

fn main() {
    let n: usize = std::env::var("SCHEMA_PROFILE_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);

    let schema = order_schema();
    let payload = order_payload();

    let mut ok_count = 0usize;
    let mut violation_count = 0usize;
    for _ in 0..n {
        match validate_against_schema(&schema, &payload) {
            Ok(()) => ok_count += 1,
            Err(v) => violation_count += v.len(),
        }
    }

    // Printed so LTO cannot dead-code-eliminate the loop. A run that
    // accidentally produces violations (a fixture bug) is then loudly
    // visible rather than silently under-measuring the validator.
    println!("schema_validate_profile: n={n} ok={ok_count} violations={violation_count}");
    assert_eq!(
        ok_count, n,
        "fixture bug: valid payload produced violations"
    );
}
