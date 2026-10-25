//! Deterministic profiling harness for `payload_depth::refuse_deep_result`.
//!
//! The worker runs this function on every successful activity result. The
//! harness calls it on a fixed mix of results that all pass the cap. Each call
//! takes an owned clone of the value, as the worker does.
//!
//! Wall-clock time is not valid evidence on a shared machine. Use instruction
//! counts from callgrind and allocation counts from dhat. The workload has no
//! randomness, so the counts repeat.
//!
//! The harness clones each value. The clone and drop cost most of the
//! instructions. Subtract that share before you judge the walk. See
//! `docs/performance-payload-depth.md`.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench payload_depth_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_depth_profile") | .executable')
//! valgrind --tool=callgrind --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --inclusive=yes cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `PAYLOAD_DEPTH_PROFILE_PASSES` (default `2000`) sets the number of passes
//! over the mix. Each pass makes 7 calls.

use autumn_harvest::payload_depth::{MAX_PAYLOAD_DEPTH, nests_past_cap, refuse_deep_result};
use serde_json::{Value, json};
use std::hint::black_box;

/// One order document: a customer record and three line items.
fn order() -> Value {
    json!({
        "id": "ord-1042",
        "customer": {
            "name": "Ada Example",
            "address": { "city": "Oslo", "country": "NO", "geo": [59.91, 10.75] },
        },
        "lines": [
            { "sku": "A-1", "qty": 2, "tags": ["new", "gift"] },
            { "sku": "B-7", "qty": 1, "tags": [] },
            { "sku": "C-3", "qty": 5, "tags": ["bulk"] },
        ],
        "paid": true,
    })
}

/// A result of 200 small rows.
fn rows() -> Value {
    Value::Array(
        (0..200)
            .map(|i| json!({ "id": i, "name": format!("row-{i}"), "ok": i % 2 == 0 }))
            .collect(),
    )
}

/// A flat object of 100 keys.
fn wide_object() -> Value {
    Value::Object(
        (0..100)
            .map(|i| (format!("key-{i:03}"), json!(i)))
            .collect(),
    )
}

/// The fixed mix of results. Every one passes the cap.
fn mix() -> Vec<Value> {
    vec![
        Value::Null,
        json!(42),
        json!("done"),
        order(),
        order(),
        rows(),
        wide_object(),
    ]
}

fn main() {
    let passes: usize = std::env::var("PAYLOAD_DEPTH_PROFILE_PASSES")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2000);
    let mix = mix();
    // A mix that fails the cap would profile the refusal path, not the pass path.
    assert!(
        mix.iter().all(|value| !nests_past_cap(value)),
        "every result in the mix must pass the cap of {MAX_PAYLOAD_DEPTH}"
    );
    let mut accepted = 0_usize;
    for _ in 0..passes {
        for value in &mix {
            if refuse_deep_result("charge", Ok(black_box(value.clone()))).is_ok() {
                accepted += 1;
            }
        }
    }
    assert_eq!(accepted, passes * mix.len(), "every call must pass");
    println!("calls: {accepted}");
}
