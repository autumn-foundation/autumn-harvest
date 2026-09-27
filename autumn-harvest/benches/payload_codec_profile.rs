//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for `payload_codec::PayloadCodecs::{encode_event, decode_event}`.
//! `store.rs` runs this codec transform on literally every workflow-event
//! append (`encode_event`, `store.rs` lines 150/558) and every history read
//! or replay load (`decode_event`, `store.rs` lines
//! 1131/1349/1395/1441/1445). It runs once for every payload-bearing field
//! (`input`, `output`, `payload`, `details`, `value`,
//! `last_completion_result`, see `payload_store::PAYLOAD_FIELD_KEYS`) of
//! every event of every execution ever recorded or replayed.
//!
//! Wall-clock timing is not admissible evidence on this (shared-vCPU)
//! machine. Every number this harness produces evidence for is a
//! deterministic instruction count (`valgrind --tool=callgrind`) or an
//! allocation count/byte total (`valgrind --tool=dhat`).
//!
//! This harness is not bit-for-bit reproducible run to run.
//! `ActivityExecId::new()` mints fresh random UUIDs on every run, feeding
//! `serde_json::Value`'s content. `PayloadCodecs`'s own key registry uses
//! `std::collections::BTreeMap`, so that part stays stable, but the JSON
//! payload bytes vary in their random UUID substrings. `timeline_profile.rs`
//! documents this same class of spread. It measured roughly 0.1-0.2% run to
//! run there, two orders of magnitude below the impact floor. dhat's
//! allocation counts and byte totals are unaffected: an allocation count
//! does not depend on UUID content.
//!
//! # Workload
//!
//! `PayloadCodecs::default()` is the identity codec, with no key ever
//! registered or rotated. `any_keys`'s own doc comment on the
//! `PayloadCodecs` struct calls this "the overwhelmingly common case".
//! That is what this harness measures: a deployment that has never
//! configured a non-identity codec.
//!
//! The harness builds a fixed, realistic mixed event history for one
//! long-running execution. It opens with a `WorkflowStarted`, carrying a
//! scheduled-carryover `last_completion_result` on some runs (issue #488).
//! It adds regular activities, most closed and a handful retried once,
//! matching `timeline_profile.rs`'s 9:1 closed:open / 1-in-5-retried mix.
//! Local activities, heartbeats, and a terminal `WorkflowCompleted` round
//! out the history. The harness then round-trips every event through
//! `encode_event` (the write path) immediately followed by `decode_event`
//! on the result (the read/replay path), `PAYLOAD_CODEC_PROFILE_REPS`
//! times. Each payload field carries a representative nested
//! order-processing document (customer record, three line items). It is
//! not a bare scalar, so a per-field clone carries a real cost.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features \
//!   --bench payload_codec_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_codec_profile") | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `PAYLOAD_CODEC_PROFILE_N` (default `500`) sets how many regular
//! activities the fixed history contains (other categories scale off it,
//! same ratios as `timeline_profile.rs`). `PAYLOAD_CODEC_PROFILE_REPS`
//! (default `200`, minimum `20`) sets how many times the whole fixed
//! history is encode+decode round-tripped. This mirrors how many times a
//! busy shard writes then replays a comparable history.

use autumn_harvest::PayloadCodecs;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, WorkerId};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, json};

fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000, 0).unwrap()
}

/// A representative per-event payload -- a nested order-processing
/// document, not a bare scalar, so a full-tree clone or walk carries a
/// real cost. Large and varied enough to be typical of real activity
/// input/output.
fn payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer": {
            "id": format!("cust-{:06}", i % 5000),
            "email": format!("customer{i}@example.com"),
            "tier": if i.is_multiple_of(7) { "gold" } else { "standard" },
        },
        "line_items": (0..3)
            .map(|j| {
                json!({
                    "sku": format!("sku-{:05}", (i + j) % 900),
                    "qty": (j + 1) as u64,
                    "unit_cents": ((i * 37 + j * 911) % 50_000) as u64,
                })
            })
            .collect::<Vec<_>>(),
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
    })
}

const ACTIVITY_NAMES: [&str; 4] = [
    "charge_card",
    "send_receipt",
    "notify_partner",
    "sync_inventory",
];
const QUEUES: [&str; 4] = ["payments", "email", "webhooks", "inventory"];

/// Builds the fixed, realistic event history this harness round-trips
/// repeatedly. It opens with a `WorkflowStarted`, carrying a
/// scheduled-carryover `last_completion_result` on 1 in 5 runs (issue
/// #488). It then adds `n` regular activities: 9 of every 10 close, and 1
/// of every 5 closed ones retries once before succeeding, mirroring
/// `timeline_profile.rs`. Local activities and heartbeats scale off `n`.
/// A terminal `WorkflowCompleted` closes the history.
fn build_history(n: usize) -> Vec<WorkflowEvent> {
    let mut events: Vec<WorkflowEvent> = Vec::new();
    let worker = WorkerId::new("worker-a");

    events.push(WorkflowEvent::WorkflowStarted {
        input: payload(0),
        timestamp: now(),
        last_completion_result: Some(payload(1)),
        last_error: None,
        scheduled_time: None,
    });

    let mut closed_count: usize = 0;
    for i in 0..n {
        let activity_id = ActivityExecId::new();
        let name = ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()].to_string();
        let queue = QUEUES[i % QUEUES.len()].to_string();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name,
            input: payload(i),
            queue,
        });
        if i % 10 != 9 {
            let retries = closed_count % 5 == 4;
            closed_count += 1;
            if retries {
                events.push(WorkflowEvent::ActivityStarted {
                    activity_id,
                    worker_id: worker.clone(),
                });
                events.push(WorkflowEvent::ActivityFailed {
                    activity_id,
                    error: "transient".to_string(),
                    attempt: 1,
                    error_type: "Error".to_string(),
                    non_retryable: false,
                    details: Some(payload(i)),
                });
            }
            events.push(WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: worker.clone(),
            });
            events.push(WorkflowEvent::ActivityHeartbeat {
                activity_id,
                details: payload(i),
            });
            events.push(WorkflowEvent::ActivityCompleted {
                activity_id,
                output: payload(i),
            });
        }
    }

    let local_n = (n / 4).max(1);
    for i in 0..local_n {
        let activity_id = ActivityExecId::new();
        let name = format!("local_{}", ACTIVITY_NAMES[i % ACTIVITY_NAMES.len()]);
        events.push(WorkflowEvent::LocalActivityScheduled {
            activity_id,
            name,
            input: payload(i),
            resolved: true,
            retry_policy: None,
            start_to_close_nanos: None,
        });
        if i % 10 != 9 {
            events.push(WorkflowEvent::LocalActivityCompleted {
                activity_id,
                output: payload(i),
            });
        }
    }

    events.push(WorkflowEvent::WorkflowCompleted { output: payload(n) });

    events
}

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

fn main() {
    let n = env_usize("PAYLOAD_CODEC_PROFILE_N", 500);
    let reps = env_usize("PAYLOAD_CODEC_PROFILE_REPS", 200);
    assert!(
        n >= 20,
        "PAYLOAD_CODEC_PROFILE_N must be at least 20, got {n}"
    );
    assert!(
        reps >= 20,
        "PAYLOAD_CODEC_PROFILE_REPS must be at least 20 (keeps one-time setup cost \
         under ~1% of the total collected instruction count), got {reps}"
    );

    let history = build_history(n);
    let codecs = PayloadCodecs::default();

    // Sanity-check the fixture once, unmeasured. A round trip must
    // reproduce the original event. Otherwise this harness would silently
    // profile a codec transform that corrupts data.
    for event in &history {
        let encoded = codecs.encode_event(event).expect("encode");
        let decoded = codecs.decode_event(encoded).expect("decode");
        assert_eq!(
            serde_json::to_string(&decoded).unwrap(),
            serde_json::to_string(event).unwrap(),
            "fixture bug: encode/decode round trip did not reproduce the original event"
        );
    }

    let mut total_events: u64 = 0;

    for _ in 0..reps {
        for event in std::hint::black_box(&history) {
            let encoded = codecs.encode_event(event).expect("encode");
            let decoded = codecs
                .decode_event(std::hint::black_box(encoded))
                .expect("decode");
            std::hint::black_box(&decoded);
            total_events += 1;
        }
    }

    println!(
        "payload_codec_profile: n={n} reps={reps} history_len={} total_events={total_events}",
        history.len(),
    );
}
