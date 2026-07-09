//! Guard for the changelog-fragment convention (`docs/changelog.d/`).
//! See `docs/changelog.d/README.md`. This is a lightweight anchor, not tooling:
//! it just asserts the convention doc still exists so the fragment directory
//! can't silently disappear.

use std::path::PathBuf;

#[test]
fn changelog_fragments_readme_exists() {
    // Walk up from this crate's manifest dir to the workspace root so the test
    // is robust to crate nesting depth. Only assert when we're clearly inside a
    // git checkout: a packaged crates.io distribution ships the crate without the
    // workspace-level `docs/` dir and must not fail the build there.
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut in_git_repo = false;
    loop {
        if dir.join("docs/changelog.d/README.md").exists() {
            return;
        }
        if dir.join(".git").exists() {
            in_git_repo = true;
        }
        if !dir.pop() {
            assert!(
                !in_git_repo,
                "docs/changelog.d/README.md not found walking up from CARGO_MANIFEST_DIR; \
                 see docs/changelog.d/README.md for the changelog-fragment convention"
            );
            // No workspace docs and no .git — a packaged distribution; skip.
            return;
        }
    }
}
