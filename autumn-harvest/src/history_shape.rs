//! History shape fingerprints — compare runs by event structure (Nova).
//!
//! # Overview
//!
//! Two runs of one workflow type often take the same path. A fingerprint of the
//! ordered event type names shows this at a glance, with no payload read.
//! Embedders can group runs by shape, find a rare path, or see where a new run
//! left the path of a known good run.
//!
//! ```rust
//! use autumn_harvest::event::WorkflowEvent;
//! use autumn_harvest::history_shape::{first_divergence, shape_fingerprint};
//!
//! let done = WorkflowEvent::WorkflowCompleted { output: serde_json::json!(1) };
//! let a = [done.clone()];
//! let b = [done];
//! assert_eq!(shape_fingerprint(&a), shape_fingerprint(&b));
//! assert_eq!(first_divergence(&a, &b), None);
//! ```
//!
//! # Scope
//!
//! The module is pure. It reads events and writes nothing. It adds no
//! `WorkflowEvent` variant, no SQL and no migration, so replay is unaffected.
//! The fingerprint covers event type names only. Payload changes do not move it.

use std::collections::BTreeMap;

use crate::event::WorkflowEvent;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Count the events of each type in `events`.
#[must_use]
pub fn shape_histogram(events: &[WorkflowEvent]) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.type_name()).or_insert(0) += 1;
    }
    counts
}

/// Return a stable 64-bit fingerprint of the ordered event type names.
///
/// The hash is FNV-1a. Its value does not change between builds or platforms.
/// A zero byte follows each name, so `["ab", "c"]` and `["a", "bc"]` differ.
#[must_use]
pub fn shape_fingerprint(events: &[WorkflowEvent]) -> u64 {
    let mut hash = FNV_OFFSET;
    for event in events {
        for byte in event.type_name().bytes().chain(std::iter::once(0)) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

/// Return the index of the first event whose type differs between two histories.
///
/// When one history is a strict prefix of the other, the index is the length of
/// the shorter one. Equal shapes return `None`.
#[must_use]
pub fn first_divergence(a: &[WorkflowEvent], b: &[WorkflowEvent]) -> Option<usize> {
    let common = a.len().min(b.len());
    (0..common)
        .find(|&i| a[i].type_name() != b[i].type_name())
        .or_else(|| (a.len() != b.len()).then_some(common))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn done() -> WorkflowEvent {
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!(null),
        }
    }

    fn failed() -> WorkflowEvent {
        WorkflowEvent::WorkflowFailed {
            error: "boom".into(),
            error_type: None,
            details: None,
            non_retryable: Some(false),
        }
    }

    #[test]
    fn histogram_counts_each_type() {
        let counts = shape_histogram(&[done(), failed(), done()]);
        assert_eq!(counts.get("WorkflowCompleted"), Some(&2));
        assert_eq!(counts.get("WorkflowFailed"), Some(&1));
        assert!(shape_histogram(&[]).is_empty());
    }

    #[test]
    fn fingerprint_ignores_payload_and_tracks_order() {
        let other = WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"x": 1}),
        };
        assert_eq!(shape_fingerprint(&[done()]), shape_fingerprint(&[other]));
        assert_ne!(
            shape_fingerprint(&[done(), failed()]),
            shape_fingerprint(&[failed(), done()])
        );
        assert_eq!(shape_fingerprint(&[]), FNV_OFFSET);
    }

    #[test]
    fn divergence_finds_type_mismatch_and_prefix() {
        assert_eq!(first_divergence(&[done()], &[done()]), None);
        assert_eq!(
            first_divergence(&[done(), done()], &[done(), failed()]),
            Some(1)
        );
        assert_eq!(first_divergence(&[done()], &[done(), failed()]), Some(1));
        assert_eq!(first_divergence(&[], &[]), None);
    }
}
