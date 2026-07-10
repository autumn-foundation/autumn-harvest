//! Fuzz target: `WorkflowEvent` JSON deserialization is total.
//!
//! `WorkflowEvent` is the append-only, adjacently-tagged event enum stored in
//! `harvest_events`. Every read path (`store::load_history`, the replayer, the
//! read-path decode surface) deserializes attacker/history-influenced JSON into
//! it, so a deserialization panic is a real availability bug. Raw-bytes fuzzing
//! beats proptest here because it drives serde_json's own parser over
//! byte-level malformed/truncated/adversarial input the property strategies
//! never construct.
//!
//! Invariant: `serde_json::from_slice::<WorkflowEvent>(data)` returns `Ok` or
//! `Err` — it never panics.
#![no_main]

use autumn_harvest::WorkflowEvent;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<WorkflowEvent>(data);
});
