//! Fuzz target: the replayer over arbitrary histories (issue #1835).
//!
//! Each input is a `ReplayCase`. Raw bytes feed the `arbitrary` generator,
//! which builds a `Vec<WorkflowEvent>`. Input that starts with `{` is a JSON
//! case, such as the seeds in `fuzz/seeds/fuzz_replay/`.
//!
//! `autumn_harvest::fuzzing::check_case` holds the invariants:
//! - nothing panics;
//! - a history that the write path stores reads back unchanged;
//! - two runs of the case give the same report.
#![no_main]

use autumn_harvest::fuzzing::{ReplayCase, check_case};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some(case) = ReplayCase::from_fuzz_bytes(data) {
        let _ = check_case(&case);
    }
});
