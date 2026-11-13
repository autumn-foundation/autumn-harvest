//! Fuzz target: `WorkflowEvent` JSON deserialization is total.
//!
//! `WorkflowEvent` is the append-only, adjacently tagged event enum stored in
//! `harvest_events`. Every read path deserializes history JSON into it:
//! `store::load_history`, the replayer and the read-path decode surface.
//! That JSON can carry attacker input, so a panic is a real availability bug.
//! Raw-bytes fuzzing beats proptest here. It drives the `serde_json` parser
//! over malformed, truncated and adversarial bytes that the proptest
//! strategies never build.
//!
//! Invariant: `serde_json::from_slice::<WorkflowEvent>(data)` returns `Ok` or
//! `Err` — it never panics.
#![no_main]

use autumn_harvest::WorkflowEvent;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<WorkflowEvent>(data);
});
