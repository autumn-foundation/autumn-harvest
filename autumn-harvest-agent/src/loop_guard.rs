//! Loop detection: stop an agent that repeats the same tool call.
//!
//! A model sometimes polls a tool that never changes, or bounces between two
//! calls. Each repeat costs a model call. The guard keeps a fingerprint of the
//! recent calls: tool name, arguments, and result. At `warn_after` identical
//! fingerprints it adds a note to the result. At `stop_after` the run ends as
//! `loop_detected`.
//!
//! The guard runs inside the workflow, on recorded data only. The fingerprint
//! is FNV-1a, which is stable across builds and Rust versions, so replay always
//! reaches the same verdict.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// Loop-detection thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopGuard {
    /// Identical calls in the window before the result gets a warning note.
    pub warn_after: u32,
    /// Identical calls in the window before the run stops.
    pub stop_after: u32,
    /// How many recent calls the guard remembers. Zero turns it off.
    pub window: usize,
}

impl Default for LoopGuard {
    fn default() -> Self {
        Self {
            warn_after: 3,
            stop_after: 5,
            window: 30,
        }
    }
}

impl LoopGuard {
    /// A guard that never fires.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            warn_after: u32::MAX,
            stop_after: u32::MAX,
            window: 0,
        }
    }
}

/// What the guard says about one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopVerdict {
    /// Nothing unusual.
    Ok,
    /// The call repeated this many times. Warn the model.
    Warn(u32),
    /// The call repeated this many times. Stop the run.
    Stop(u32),
}

/// The rolling fingerprint history of one run.
#[derive(Debug, Clone, Default)]
pub(crate) struct LoopTracker {
    recent: VecDeque<u64>,
}

impl LoopTracker {
    /// Record one call and judge it.
    pub(crate) fn record(
        &mut self,
        guard: &LoopGuard,
        name: &str,
        arguments: &serde_json::Value,
        result: &str,
    ) -> LoopVerdict {
        if guard.window == 0 {
            return LoopVerdict::Ok;
        }
        let print = fingerprint(name, arguments, result);
        self.recent.push_back(print);
        while self.recent.len() > guard.window {
            self.recent.pop_front();
        }
        let repeats = u32::try_from(self.recent.iter().filter(|seen| **seen == print).count())
            .unwrap_or(u32::MAX);
        if repeats >= guard.stop_after {
            LoopVerdict::Stop(repeats)
        } else if repeats >= guard.warn_after {
            LoopVerdict::Warn(repeats)
        } else {
            LoopVerdict::Ok
        }
    }
}

/// The note added to the result of a repeated call.
#[must_use]
pub fn warning_note(name: &str, repeats: u32) -> String {
    format!(
        "\n[loop guard] You repeated the same `{name}` call {repeats} times with the same \
         result. Change your approach or give your answer."
    )
}

/// FNV-1a over the name, the arguments and the result, with a separator
/// between them.
fn fingerprint(name: &str, arguments: &serde_json::Value, result: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let arguments = arguments.to_string();
    let mut hash = OFFSET;
    for part in [
        name.as_bytes(),
        &[0],
        arguments.as_bytes(),
        &[0],
        result.as_bytes(),
    ] {
        for byte in part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_fingerprint_is_stable_across_builds() {
        // A fixed FNV-1a value, checked with an independent script. A change here
        // changes the verdicts that replay recomputes.
        assert_eq!(fingerprint("", &json!(null), ""), 0xc3de_faea_e2ce_b336);
        assert_ne!(
            fingerprint("a", &json!({}), "b"),
            fingerprint("ab", &json!({}), "")
        );
    }

    #[test]
    fn repeats_warn_then_stop() {
        let guard = LoopGuard {
            warn_after: 2,
            stop_after: 3,
            window: 10,
        };
        let mut tracker = LoopTracker::default();
        let args = json!({"q": 1});
        assert_eq!(tracker.record(&guard, "t", &args, "r"), LoopVerdict::Ok);
        assert_eq!(
            tracker.record(&guard, "t", &args, "r"),
            LoopVerdict::Warn(2)
        );
        assert_eq!(
            tracker.record(&guard, "t", &args, "r"),
            LoopVerdict::Stop(3)
        );
    }

    #[test]
    fn a_changing_result_is_not_a_loop() {
        let guard = LoopGuard::default();
        let mut tracker = LoopTracker::default();
        for n in 0..10 {
            let verdict = tracker.record(&guard, "poll", &json!({}), &n.to_string());
            assert_eq!(verdict, LoopVerdict::Ok);
        }
    }

    #[test]
    fn a_disabled_guard_never_fires() {
        let guard = LoopGuard::disabled();
        let mut tracker = LoopTracker::default();
        for _ in 0..10 {
            assert_eq!(
                tracker.record(&guard, "t", &json!({}), "r"),
                LoopVerdict::Ok
            );
        }
    }
}
