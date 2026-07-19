//! Integration tests for the `harvest new` project scaffold (issue #692).
//!
//! Black-box tests over the crate's public scaffold surface: they derive names,
//! render the embedded `minimal` template, and drive `run_new` end to end into
//! `tempfile::tempdir()` sandboxes (mirroring `det_check_cli.rs`).

use std::sync::Mutex;

use autumn_harvest_cli::{
    ScaffoldTemplate, apply_substitutions, derive_crate_ident, derive_names, render_minimal,
    run_new,
};

/// Serializes the one test that mutates the process-global current directory so
/// it never races the other (cwd-independent) tests in this binary.
static CWD_GUARD: Mutex<()> = Mutex::new(());

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
/// `(relative_path, content)` pairs. Panics if the name is invalid.
fn render(name: &str) -> Vec<(&'static str, String)> {
    let names = derive_names(name).expect("name should be valid");
    render_minimal(&names)
}

fn file_content<'a>(files: &'a [(&'static str, String)], rel: &str) -> &'a str {
    match files.iter().find(|(p, _)| *p == rel) {
        Some((_, c)) => c.as_str(),
        None => panic!("template must emit {rel}"),
    }
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
fn derived_ident_is_clean_snake_case_for_any_valid_name() {
    // Uppercase → lowercased (no non_snake_case warnings in the generated code).
    assert_eq!(derive_crate_ident("MyApp"), "myapp");
    // Trailing hyphen → trimmed (no trailing `_` in the ident).
    assert_eq!(derive_crate_ident("trail-"), "trail");
    // Repeated separators → collapsed (no double underscore).
    assert_eq!(derive_crate_ident("my--app"), "my_app");
    assert_eq!(derive_crate_ident("a_-b"), "a_b");
    // crate_name stays verbatim (Cargo package name = user's `<name>`).
    let names = derive_names("MyApp").expect("valid");
    assert_eq!(names.crate_name, "MyApp");
    assert_eq!(names.ident, "myapp");
    assert_eq!(names.workflow_fn, "myapp_workflow");
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
        "",          // empty
        "123bad",    // starts with a digit
        "has space", // whitespace
        "a.b",       // dot / path-ish
        "../evil",   // path traversal
        "a/b",       // separator
        "fn",        // Rust keyword (via ident)
        "match",     // Rust keyword
        "-lead",     // leading hyphen
    ] {
        assert!(
            derive_names(bad).is_err(),
            "expected {bad:?} to be rejected"
        );
    }
}

#[test]
fn over_length_name_is_rejected() {
    // MAX_PROJECT_NAME_LEN is 64; a 65-char name must be rejected before any write.
    let too_long = "a".repeat(65);
    assert!(
        derive_names(&too_long).is_err(),
        "a 65-character name must be rejected"
    );
    // The boundary (exactly 64) is accepted.
    let at_limit = "a".repeat(64);
    assert!(
        derive_names(&at_limit).is_ok(),
        "a 64-character name must be accepted"
    );
}

#[test]
fn reserved_project_names_are_rejected() {
    for reserved in ["core", "test", "std", "main", "lib", "build", "deps"] {
        assert!(
            derive_names(reserved).is_err(),
            "reserved name {reserved:?} must be rejected"
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
    assert!(
        cargo.contains("autumn-harvest = { version = \"0.4\""),
        "cargo: {cargo}"
    );
    assert!(
        cargo.contains("autumn-harvest-plugin = \"0.4\""),
        "cargo: {cargo}"
    );
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
    assert!(
        main.contains("activities![my_app_activity]"),
        "main: {main}"
    );
}

#[test]
fn hyphenated_name_yields_valid_rust_ident_in_main_rs() {
    // `my-app` is a valid Cargo name but an invalid Rust ident: main.rs must
    // use the underscore form, never the literal hyphen form.
    let files = render("my-app");
    let main = file_content(&files, "src/main.rs");
    assert!(
        main.contains("async fn my_app_workflow"),
        "main must use the underscore ident: {main}"
    );
    assert!(
        !main.contains("my-app_workflow"),
        "main must not contain a hyphenated ident: {main}"
    );
    // Cargo.toml keeps the verbatim package name.
    let cargo = file_content(&files, "Cargo.toml");
    assert!(cargo.contains("name = \"my-app\""), "cargo: {cargo}");
}

#[test]
fn uppercase_and_repeat_separator_names_render_clean_snake_case_idents() {
    // Uppercase package name is accepted (cargo-new parity) but the derived
    // idents are clean lowercase snake_case, and no generated ident has a
    // double underscore.
    for name in ["MyApp", "my--app", "trail-"] {
        let files = render(name);
        let main = file_content(&files, "src/main.rs");
        assert!(
            !main.contains("__"),
            "generated main.rs for {name:?} must have no double-underscore idents:\n{main}"
        );
    }
    let files = render("MyApp");
    let main = file_content(&files, "src/main.rs");
    assert!(
        main.contains("async fn myapp_workflow"),
        "MyApp must derive the lowercase ident myapp_workflow: {main}"
    );
    assert!(
        !main.contains("MyApp_workflow"),
        "MyApp must not leak an uppercase ident into main.rs: {main}"
    );
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
    assert!(
        readme.contains("Docker"),
        "readme must state the prereq: {readme}"
    );
}

#[test]
fn scaffold_rot_wiring_parity_with_quickstart() {
    let files = render("app");
    let main = file_content(&files, "src/main.rs");
    let qs = quickstart_main_rs();
    for token in [
        "use autumn_harvest::prelude::*;",
        "HarvestPlugin::new()",
        ".routes(routes![",
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
    // --force never removes files it did not write (no rm -rf): the pre-seeded
    // sentinel survives untouched.
    assert_eq!(
        std::fs::read_to_string(target.join("keep.txt")).expect("keep.txt readable"),
        "old",
        "--force must not remove or overwrite files it did not write"
    );
}

#[test]
fn existing_file_at_target_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let target = dir.path().join("afile");
    std::fs::write(&target, "i am a file").expect("seed file");

    // A plain file at the target is not a directory we can scaffold into; the
    // command must reject it with a clear "is not a directory" message (not an
    // opaque create_dir_all OS error), even with --force.
    let err = run_new("app", Some(&target), false, ScaffoldTemplate::Minimal)
        .expect_err("a plain file at target must be rejected");
    assert!(
        matches!(err, autumn_harvest_cli::CliError::InvalidInput(_)),
        "must be a clean InvalidInput rejection, got: {err:?}"
    );
    assert!(
        err.to_string().contains("not a directory"),
        "error must explain the target is a file, got: {err}"
    );
    let forced = run_new("app", Some(&target), true, ScaffoldTemplate::Minimal);
    assert!(
        forced.is_err(),
        "a plain file at target must be rejected even with --force"
    );
    // The file is left untouched.
    assert_eq!(
        std::fs::read_to_string(&target).expect("file readable"),
        "i am a file"
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
        !target.exists() || std::fs::read_dir(&target).is_ok_and(|mut d| d.next().is_none()),
        "no files may be written for an invalid name"
    );
}

#[test]
fn default_path_none_resolves_to_name_under_cwd() {
    // When --path is omitted, run_new writes to ./<name>. Drive it inside a
    // tempdir cwd (serialized, restored after) so the workspace is untouched.
    let _guard = CWD_GUARD
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("tempdir");
    let original = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(dir.path()).expect("chdir to tempdir");

    let name = "scaffold_cwd_demo";
    let result = run_new(name, None, false, ScaffoldTemplate::Minimal);

    // Restore cwd before asserting so a panic can't leave the process adrift.
    std::env::set_current_dir(&original).expect("restore cwd");

    result.expect("scaffold ok");
    assert!(
        dir.path().join(name).join("Cargo.toml").exists(),
        "None path must resolve to ./<name>"
    );
}
