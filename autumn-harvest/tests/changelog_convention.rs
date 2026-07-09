//! Guard for the changelog-fragment convention (`docs/changelog.d/`).
//! See `docs/changelog.d/README.md`. This is a lightweight anchor, not tooling:
//! it just asserts the convention doc still exists so the fragment directory
//! can't silently disappear.

use std::path::PathBuf;

#[test]
fn changelog_fragments_readme_exists() {
    // Walk up from this crate's manifest dir to the workspace root so the test
    // is robust to crate nesting depth.
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("docs/changelog.d/README.md").exists() {
            return;
        }
        if !dir.pop() {
            panic!(
                "docs/changelog.d/README.md not found walking up from CARGO_MANIFEST_DIR; \
                 see docs/changelog.d/README.md for the changelog-fragment convention"
            );
        }
    }
}
