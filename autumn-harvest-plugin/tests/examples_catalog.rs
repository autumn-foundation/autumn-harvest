use std::fs;
use std::path::Path;

#[test]
fn advanced_examples_are_workspace_members_with_reference_docs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("plugin crate should live below workspace root");
    let workspace = fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");

    for member in ["examples/billing-autumn-web", "examples/standalone-runner"] {
        assert!(
            workspace.contains(member),
            "{member} should be listed as a workspace member"
        );

        let readme = fs::read_to_string(root.join(member).join("README.md"))
            .unwrap_or_else(|error| panic!("{member}/README.md should exist: {error}"));
        assert!(
            readme.contains("saga")
                && readme.contains("child workflow")
                && readme.contains("version")
                && readme.contains("runner"),
            "{member} README should explain the advanced surfaces it demonstrates"
        );
    }
}
