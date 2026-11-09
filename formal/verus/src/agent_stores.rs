//! The version counter of the durable agent stores.
//!
//! `next_doc_version` gives the version of the next write of a document.
//! A writer names the version that it read, and the store writes only
//! when the stored version is still that one. The copy below proves that
//! the first version is 1, that each write moves the version up by one,
//! and that a full counter refuses the write. So a version never comes
//! back, and a write that read an old version never matches.

use vstd::prelude::*;

verus! {

/// `next_doc_version` as a spec.
pub open spec fn spec_next_doc_version(current: Option<u64>) -> Option<u64> {
    match current {
        None => Some(1u64),
        Some(version) => if version < u64::MAX {
            Some((version + 1) as u64)
        } else {
            None
        },
    }
}

// mirror: autumn-harvest/src/agent_store.rs next_doc_version
pub fn next_doc_version(current: Option<u64>) -> (r: Option<u64>)
    ensures
        r == spec_next_doc_version(current),
        current.is_none() ==> r == Some(1u64),
        current.is_some() && r.is_some() ==> r.unwrap() == current.unwrap() + 1,
        r.is_none() ==> current == Some(u64::MAX),
{
    match current {
        None => Some(1),
        Some(version) => {
            if version < u64::MAX {
                Some(version + 1)
            } else {
                None
            }
        }
    }
}

/// Each write moves the version up, so no version comes back.
pub proof fn a_version_only_grows(current: u64)
    requires
        current < u64::MAX,
    ensures
        spec_next_doc_version(Some(current)).unwrap() > current,
        spec_next_doc_version(Some(current)).unwrap() >= 1,
{
}

} // verus!
