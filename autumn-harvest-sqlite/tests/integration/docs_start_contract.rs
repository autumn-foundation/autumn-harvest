//! Issue #1636: the task-oriented guide and the crate README claimed
//! `start_workflow_with_id` never dedupes ("every call creates a new,
//! independent execution"). The rustdoc on the same method, and
//! `reuse_policy.rs` in this suite, prove the opposite: a non-blank
//! `workflow_id` attaches to a non-sealed prior run by default (issue #1068).
//! A caller who followed the stale guide's "dedupe upstream" advice got
//! silent input loss instead.
//!
//! This is a docs-only regression guard: it pins the guide and the README
//! away from the false claim and onto the real, already-implemented
//! contract, so the two cannot drift apart again unnoticed.

use std::path::Path;

fn read(path: &str) -> String {
    let full = Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
    std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()))
}

const GUIDE_STALE_CLAIMS: &[&str] = &[
    "does **not** enforce",
    "does **not** apply the core's",
    "**Every call creates a new, independent",
];

#[test]
fn guide_does_not_claim_non_idempotent_start() {
    let guide = read("../docs/sqlite-backend.md");
    for claim in GUIDE_STALE_CLAIMS {
        assert!(
            !guide.contains(claim),
            "docs/sqlite-backend.md still makes the stale non-idempotent-start \
             claim ({claim:?}); start_workflow_with_id has attached to a \
             non-sealed prior since issue #1068 (see runtime.rs rustdoc)"
        );
    }
    assert!(
        guide.contains("AllowDuplicate") && guide.contains("attaches"),
        "docs/sqlite-backend.md §6 should describe the real AllowDuplicate \
         attach-by-default contract"
    );
}

#[test]
fn readme_does_not_claim_non_idempotent_start() {
    let readme = read("README.md");
    assert!(
        !readme.contains("call creates a new, independent execution"),
        "README.md non-goals should not claim every start call is a fresh, \
         independent execution; idempotent starts shipped under issue #1068"
    );
    assert!(
        !readme.contains("dedupe upstream"),
        "README.md should not tell callers to dedupe upstream — \
         start_workflow_with_id now attaches by default"
    );
}
