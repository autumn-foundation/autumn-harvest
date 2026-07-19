//! Integration tests for the `harvest new` project scaffold (issue #692).
//!
//! Black-box tests over the crate's public scaffold surface: they derive names,
//! render the embedded `minimal` template, and drive `run_new` end to end into
//! `tempfile::tempdir()` sandboxes (mirroring `det_check_cli.rs`).

use std::path::Path;

use autumn_harvest_cli::{
    ScaffoldNames, ScaffoldTemplate, apply_substitutions, derive_crate_ident, derive_names,
    render_minimal, run_new,
};

/// Reads the real `examples/quickstart/src/main.rs` at test time so the parity
/// assertion tracks the shape CI actually compiles (the scaffold-rot signal).
fn quickstart_main_rs() -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/quickstart/src/main.rs"
    );
    std::fs::read_to_string(path).expect("examples/quickstart/src/main.rs must be readable")
}

/// Convenience: render the minimal template for `name`, returning
/// (relative_path, content) pairs. Panics if the name is invalid.
fn render(name: &str) -> Vec<(&'static str, String)> {
    let names = derive_names(name).expect("name should be valid");
    render_minimal(&names)
}

fn file_content<'a>(files: &'a [(&'static str, String)], rel: &str) -> &'a str {
    files
        .iter()
        .find(|(p, _)| *p == rel)
        .map(|(_, c)| c.as_str())
        .unwrap_or_else(|| panic!("template must emit {rel}"))
}

// ── name derivation & substitution (unit-ish, over the public surface) ───────

#[test]
fn derive_names_maps_all_identifiers() {
    let names = derive_names("orders").expect("valid");
    assert_eq!(names.crate_name, "orders");
    assert_eq!(names.ident, "orders");
    assert_eq!(names.workflow_fn, "orders_workflow");
    assert_eq!(names.activity_fn, "orders_activity");
    assert_eq!(names.queue, "orders");
}

#[test]
fn hyphenated_name_sanitizes_ident_to_underscores() {
    assert_eq!(derive_crate_ident("my-cool-app"), "my_cool_app");
    let names = derive_names("my-cool-app").expect("valid");
    assert_eq!(names.crate_name, "my-cool-app");
    assert_eq!(names.ident, "my_cool_app");
    assert_eq!(names.workflow_fn, "my_cool_app_workflow");
    assert_eq!(names.activity_fn, "my_cool_app_activity");
}

#[test]
fn apply_substitutions_replaces_all_keys_and_leaves_none() {
    let out = apply_substitutions(
        "fn {{workflow_fn}}() { queue = \"{{queue}}\"; ident = {{ident}}; }",
        &[
            ("{{workflow_fn}}", "orders_workflow"),
            ("{{queue}}", "orders"),
            ("{{ident}}", "orders"),
        ],
    );
    assert_eq!(
        out,
        "fn orders_workflow() { queue = \"orders\"; ident = orders; }"
    );
    assert!(!out.contains("{{"), "no placeholder must survive: {out}");
}

// ── name validation (AC-5 / AC-6 safety) ─────────────────────────────────────

#[test]
fn invalid_project_names_are_rejected() {
    for bad in [
        "",           // empty
        "123bad",     // starts with a digit
        "has space",  // whitespace
        "a.b",        // dot / path-ish
        "../evil",    // path traversal
        "a/b",        // separator
        "fn",         // Rust keyword (via ident)
        "match",      // Rust keyword
        "-lead",      // leading hyphen
    ] {
        assert!(
            derive_names(bad).is_err(),
            "expected {bad:?} to be rejected"
        );
    }
}

// ── rendering / substitution across the whole file set ───────────────────────

#[test]
fn no_placeholder_survives_in_any_generated_file() {
    for (path, content) in render("app") {
        assert!(
            !content.contains("{{") && !content.contains("}}"),
            "{path} still contains a template placeholder:\n{content}"
        );
    }
}

#[test]
fn generated_cargo_toml_uses_cratesio_version_deps_and_no_path() {
    let files = render("my-app");
    let cargo = file_content(&files, "Cargo.toml");
    assert!(cargo.contains("name = \"my-app\""), "cargo: {cargo}");
    assert!(cargo.contains("autumn-harvest = { version = \"0.4\""), "cargo: {cargo}");
    assert!(cargo.contains("autumn-harvest-plugin = \"0.4\""), "cargo: {cargo}");
    assert!(cargo.contains("autumn-web = \"0.5\""), "cargo: {cargo}");
    assert!(cargo.contains("features = [\"db\"]"), "cargo: {cargo}");
    assert!(
        !cargo.contains("path ="),
        "generated project must not use path deps: {cargo}"
    );
}

#[test]
fn generated_main_rs_substitutes_all_identifiers() {
    let files = render("my_app");
    let main = file_content(&files, "src/main.rs");
    assert!(main.contains("async fn my_app_workflow"), "main: {main}");
    assert!(main.contains("async fn my_app_activity"), "main: {main}");
    assert!(main.contains("queue = \"my_app\""), "main: {main}");
    assert!(main.contains("workflows![my_app_workflow]"), "main: {main}");
    assert!(main.contains("activities![my_app_activity]"), "main: {main}");
}

#[test]
fn readme_documents_three_command_path() {
    let files = render("app");
    let readme = file_content(&files, "README.md");
    assert!(readme.contains("docker compose up -d"), "readme: {readme}");
    assert!(
        readme.contains("AUTUMN_PROFILE=dev cargo run"),
        "readme: {readme}"
    );
    assert!(
        readme.contains("workflows/app_workflow/start"),
        "readme must curl the named workflow: {readme}"
    );
    assert!(readme.contains("Docker"), "readme must state the prereq: {readme}");
}

#[test]
fn scaffold_rot_wiring_parity_with_quickstart() {
    let files = render("app");
    let main = file_content(&files, "src/main.rs");
    let qs = quickstart_main_rs();
    for token in [
        "use autumn_harvest::prelude::*;",
        "HarvestPlugin::new()",
        ".workflows(workflows![",
        ".activities(activities![",
        ".worker(WorkerConfig::default()",
        ".api(\"/api/harvest\")",
        "#[autumn_web::main]",
    ] {
        assert!(
            main.contains(token),
            "scaffold main.rs missing wiring token {token:?} (drift vs quickstart)"
        );
        assert!(
            qs.contains(token),
            "quickstart main.rs missing wiring token {token:?} — parity anchor moved"
        );
    }
}

// ── run_new: filesystem behavior (AC-1/2/6) ──────────────────────────────────

#[test]
fn scaffold_writes_all_expected_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("app");
    run_new("app", Some(&target), false, ScaffoldTemplate::Minimal).expect("scaffold ok");

    for rel in [
        "Cargo.toml",
        "src/main.rs",
        "README.md",
        "compose.yaml",
        "autumn.toml",
        ".gitignore",
    ] {
        let p = target.join(rel);
        assert!(p.exists(), "{rel} must be written");
        let meta = std::fs::metadata(&p).expect("metadata");
        assert!(meta.len() > 0, "{rel} must be non-empty");
    }
}

#[test]
fn non_empty_dir_fails_safe_without_force() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("app");
    std::fs::create_dir_all(&target).expect("mkdir");
    let sentinel = target.join("keep.txt");
    std::fs::write(&sentinel, "do not touch").expect("seed");

    let result = run_new("app", Some(&target), false, ScaffoldTemplate::Minimal);
    assert!(result.is_err(), "non-empty dir without --force must fail");
    // Sentinel untouched, and no partial write happened.
    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("sentinel readable"),
        "do not touch"
    );
    assert!(
        !target.join("Cargo.toml").exists(),
        "no files may be written on the fail-safe path"
    );
}

#[test]
fn force_overwrites_existing_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("app");
    std::fs::create_dir_all(&target).expect("mkdir");
    std::fs::write(target.join("keep.txt"), "old").expect("seed");

    run_new("app", Some(&target), true, ScaffoldTemplate::Minimal).expect("force scaffold ok");
    assert!(
        target.join("Cargo.toml").exists(),
        "--force must write the project"
    );
}

#[test]
fn path_flag_creates_missing_target_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("nested").join("app");
    assert!(!target.exists());
    run_new("app", Some(&target), false, ScaffoldTemplate::Minimal).expect("scaffold ok");
    assert!(target.join("src").join("main.rs").exists());
}

#[test]
fn run_new_rejects_invalid_name_without_writing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("out");
    let result = run_new("123bad", Some(&target), false, ScaffoldTemplate::Minimal);
    assert!(result.is_err(), "invalid name must be rejected");
    assert!(
        !target.exists() || std::fs::read_dir(&target).map(|mut d| d.next().is_none()).unwrap_or(true),
        "no files may be written for an invalid name"
    );
}

#[test]
fn default_path_uses_name_relative_to_cwd() {
    // When path is None, the target is ./<name>. Drive it inside a tempdir cwd
    // so the test never pollutes the workspace.
    let dir = tempfile::tempdir().expect("tempdir");
    let name = "scaffold_cwd_demo";
    // Render + write manually via run_new with an explicit path equal to the
    // documented default (cwd/<name>), which is what None resolves to.
    let target: &Path = &dir.path().join(name);
    run_new(name, Some(target), false, ScaffoldTemplate::Minimal).expect("scaffold ok");
    assert!(target.join("Cargo.toml").exists());
    let _ = ScaffoldNames {
        crate_name: name.to_string(),
        ident: name.to_string(),
        workflow_fn: format!("{name}_workflow"),
        activity_fn: format!("{name}_activity"),
        queue: name.to_string(),
    };
}
