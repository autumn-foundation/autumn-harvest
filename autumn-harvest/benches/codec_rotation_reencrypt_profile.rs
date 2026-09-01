//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `codec_rotation::{has_non_active_key, reencrypt_event_payload_fields_under}`
//! — the per-row body of `sweep_codec_reencryption_once` (issue #948), the
//! background sweep that lazily migrates `harvest_events` payloads off a
//! retired codec key. Folded into `enforce_timeouts_once` and run on the
//! scanner's tick interval (500ms by default) for every shard on which a
//! keyed codec is registered and rotation has not yet converged — so this is
//! a real, recurring batch job on any deployment that rotates keys, not a
//! one-off migration script.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is driven directly under `valgrind --tool=callgrind` (instruction
//! counts) and `valgrind --tool=dhat` (allocation counts/bytes), which are
//! deterministic across runs.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench codec_rotation_reencrypt_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! # Workload
//!
//! A batch of `CODEC_ROTATION_BATCH` (default 200, matching
//! `codec_rotation::CODEC_ROTATION_DEFAULT_BATCH`) serialized workflow
//! events — alternating `ActivityScheduled` (an `input` field) and
//! `ActivityCompleted` (an `output` field), each carrying a realistic
//! nested-object JSON payload (an order-checkout-shaped record, a few hundred
//! bytes), every one still encoded under a retired key. This is the batch
//! shape the very first sweep tick after a key rotation sees: every row the
//! scanner fetches still needs conversion, which is exactly where this
//! function's CPU/allocation cost is paid — the already-converted steady
//! state that dominates a shard's *lifetime* is a cheap map-lookup precheck
//! (`has_non_active_key` returning `false`) that this harness also exercises
//! once per row before the conversion, mirroring the real per-row order in
//! `sweep_codec_reencryption_once`.
//!
//! `CODEC_ROTATION_PROFILE_REPS` (default 25) repeats the whole batch, for a
//! default of 5,000 row conversions.

use std::sync::Arc;

use autumn_harvest::codec_rotation::{
    CODEC_ROTATION_DEFAULT_BATCH, has_non_active_key, reencrypt_event_payload_fields_under,
};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::types::ActivityExecId;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// A cheap, deterministic stand-in for an embedder-supplied encryption codec
/// (the same shape `tests/integration/codec_rotation_db_tests.rs` uses). The
/// profiled cost here is the crate's own envelope/JSON/base64 machinery
/// around the codec, not the codec algorithm itself — a real cipher adds
/// fixed per-call work on top without changing that machinery's share.
#[derive(Debug)]
struct XorCodec(u8);

impl PayloadCodec for XorCodec {
    fn codec_id(&self) -> &'static str {
        "xor"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().map(|b| b ^ self.0).collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().map(|b| b ^ self.0).collect())
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A realistic activity payload: an order-checkout-shaped record with nested
/// objects and an array, a few hundred bytes serialized.
fn order_payload(i: usize) -> Value {
    let tier = ["standard", "gold", "platinum"][i % 3];
    json!({
        "order_id": format!("order-{i:08}"),
        "customer": {
            "id": format!("cust-{:04}", i % 5000),
            "email": format!("user{i}@example.com"),
            "tier": tier,
        },
        "items": [
            {"sku": format!("SKU-{:04}", i % 200), "qty": 1 + (i % 5), "unit_cents": 999 + (i % 50) * 100},
            {"sku": format!("SKU-{:04}", (i + 7) % 200), "qty": 1 + (i % 3), "unit_cents": 1499 + (i % 20) * 50},
        ],
        "shipping_address": {
            "street": "1 Market St",
            "city": "San Francisco",
            "zip": "94105",
            "country": "US",
        },
        "metadata": {
            "campaign": "spring-sale",
            "referrer": "email",
        }
    })
}

fn fixed_timestamp() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-08-29T12:00:00Z")
        .expect("fixed timestamp")
        .with_timezone(&Utc)
}

/// One serialized event, encoded under `key_id`: `ActivityScheduled` (an
/// `input` field) for even `i`, `ActivityCompleted` (an `output` field) for
/// odd `i` — the two most common payload-bearing event kinds in a workflow's
/// history.
fn event_under(codecs: &PayloadCodecs, key_id: &str, i: usize) -> Value {
    let restore = codecs.active_key_id();
    codecs.set_active_key(key_id).expect("activate for fixture");
    let event = if i.is_multiple_of(2) {
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "charge_card".to_string(),
            input: order_payload(i),
            queue: "default".to_string(),
        }
    } else {
        WorkflowEvent::ActivityCompleted {
            activity_id: ActivityExecId::new(),
            output: order_payload(i),
        }
    };
    let _ = fixed_timestamp();
    let encoded = codecs.encode_event(&event).expect("encode fixture event");
    codecs.set_active_key(&restore).expect("restore active key");
    encoded
}

fn main() {
    let batch_size = env_usize(
        "CODEC_ROTATION_PROFILE_BATCH",
        usize::try_from(CODEC_ROTATION_DEFAULT_BATCH).unwrap_or(200),
    );
    let reps = env_usize("CODEC_ROTATION_PROFILE_REPS", 25);

    let codecs = PayloadCodecs::default();
    codecs
        .register_key("k1-retired", Arc::new(XorCodec(0x11)))
        .expect("register retired key");
    codecs
        .register_key("k2-active", Arc::new(XorCodec(0x22)))
        .expect("register active key");
    codecs.set_active_key("k2-active").expect("activate k2");
    let active_key_id = codecs.active_key_id();

    let batch: Vec<Value> = (0..batch_size)
        .map(|i| event_under(&codecs, "k1-retired", i))
        .collect();

    let mut precheck_hits = 0usize;
    let mut rows_reencrypted = 0usize;
    let mut fields_reencrypted = 0usize;

    for _ in 0..reps {
        for original in &batch {
            // Mirrors `sweep_codec_reencryption_once`'s per-row order: a cheap
            // precheck before the deep clone, then clone-and-convert only for
            // rows that actually need it.
            if !has_non_active_key(original, &active_key_id) {
                continue;
            }
            precheck_hits += 1;
            let mut candidate = original.clone();
            let outcome =
                reencrypt_event_payload_fields_under(&codecs, &active_key_id, &mut candidate)
                    .expect("reencrypt fixture event");
            if outcome.changed() {
                rows_reencrypted += 1;
                fields_reencrypted += outcome.fields_reencrypted;
            }
            std::hint::black_box(&candidate);
        }
    }

    println!(
        "codec_rotation_reencrypt_profile: batch_size={batch_size} reps={reps} \
         precheck_hits={precheck_hits} rows_reencrypted={rows_reencrypted} \
         fields_reencrypted={fields_reencrypted}"
    );
    assert_eq!(
        rows_reencrypted,
        batch_size * reps,
        "fixture bug: not every row in the batch needed conversion"
    );
}
