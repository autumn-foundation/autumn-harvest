//! Deterministic instruction/allocation-count profiling harness for
//! `autumn_harvest::info::validate_against_schema` — the hand-rolled JSON
//! Schema walker that runs on every workflow start, signal-with-start, and
//! update-with-start call for a workflow that has published an input schema
//! (issue #373), and on every completion-trigger fan-in delivery
//! (`completion_trigger.rs`). Unlike `replay_profile.rs`'s workflow-replay
//! workload, this function is unconditional — it has no feature gate — so
//! every deployment that publishes a schema pays this cost on the HTTP
//! request path, not just ones with `db`/`testing` enabled.
//!
//! `harness = false` + its own `main()`, same shape as `claim_bench.rs` and
//! `replay_profile.rs`: the compiled artifact is a plain executable meant to
//! be pointed at `valgrind --tool=callgrind` / `valgrind --tool=dhat`
//! directly, not measured with `cargo bench`'s wall-clock timing loop
//! (`validate_against_schema` is a synchronous, in-memory, sub-microsecond
//! call — real signal on this shared-vCPU machine comes only from
//! deterministic counters, never wall time).
//!
//! # Workload
//!
//! A realistic order-checkout input schema — nested objects, an array of
//! line-item objects, `required`, `enum`, `minLength`/`maxLength`,
//! `minimum`/`maximum`, and `additionalProperties` in both its `false` form
//! (reject unknown keys) and its schema-object form (validate a free-form
//! `metadata` map) — validated against a **matching valid payload** N times.
//! The valid-payload case is deliberately the one measured: it is the common
//! case in production (most requests are well-formed) and it is the one
//! where any eagerly-computed-but-unused diagnostic work is pure waste, since
//! no violation is ever pushed.
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

/// A valid order payload — six line items, a populated metadata map — so
/// every branch of the schema above is exercised on the happy path and zero
/// violations are ever pushed.
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

    // Printed so LTO can't dead-code-eliminate the loop and so a run
    // accidentally producing violations (a fixture bug) is loudly visible
    // rather than silently under-measuring the validator.
    println!("schema_validate_profile: n={n} ok={ok_count} violations={violation_count}");
    assert_eq!(
        ok_count, n,
        "fixture bug: valid payload produced violations"
    );
}
