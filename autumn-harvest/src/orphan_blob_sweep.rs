//! Sweep of payload blobs that a rolled-back write uploaded (issue #2123).
//!
//! RED PHASE STUB.

use std::collections::HashSet;

use chrono::{DateTime, Utc};

use crate::payload_store::StoredBlob;

/// The keys of `blobs` that are old enough and that no database references.
pub(crate) fn select_orphans(
    blobs: &[StoredBlob],
    cutoff: DateTime<Utc>,
    referenced: &HashSet<String>,
) -> Vec<String> {
    let _ = (blobs, cutoff, referenced);
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(key: &str, age_hours: i64) -> StoredBlob {
        StoredBlob {
            key: key.to_string(),
            created_at: Utc::now() - chrono::Duration::hours(age_hours),
        }
    }

    #[test]
    fn an_old_unreferenced_blob_is_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let orphans = select_orphans(&[blob("a", 48)], cutoff, &HashSet::new());
        assert_eq!(orphans, vec!["a".to_string()]);
    }

    #[test]
    fn a_referenced_blob_is_never_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let referenced = HashSet::from(["a".to_string()]);
        let orphans = select_orphans(&[blob("a", 48), blob("b", 48)], cutoff, &referenced);
        assert_eq!(orphans, vec!["b".to_string()]);
    }

    #[test]
    fn a_blob_younger_than_the_cutoff_is_never_an_orphan() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let orphans = select_orphans(&[blob("young", 1)], cutoff, &HashSet::new());
        assert!(orphans.is_empty(), "a store may ignore the filter");
    }

    #[test]
    fn a_blob_created_exactly_at_the_cutoff_is_kept() {
        let cutoff = Utc::now() - chrono::Duration::hours(24);
        let at = StoredBlob {
            key: "edge".to_string(),
            created_at: cutoff,
        };
        assert!(select_orphans(&[at], cutoff, &HashSet::new()).is_empty());
    }
}
