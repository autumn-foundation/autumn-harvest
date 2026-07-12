use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let migrations_dir = Path::new("migrations");

    // Watch the directory so Cargo re-runs this script when migrations are
    // added or removed.
    println!("cargo:rerun-if-changed=migrations/");

    // Collect and sort migration names, then emit them as a rustc-env so
    // that the build-script *output* changes whenever a new migration
    // appears.  A changed output invalidates the lib fingerprint and forces
    // Cargo to recompile lib.rs (re-running embed_migrations!()).
    let mut names: Vec<String> = migrations_dir
        .read_dir()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();

    // Each individual SQL file is also watched so edits to existing
    // migrations trigger a recompile as well.
    for name in &names {
        let dir = migrations_dir.join(name);
        println!("cargo:rerun-if-changed={}", dir.display());
    }

    println!(
        "cargo:rustc-env=HARVEST_MIGRATIONS_LIST={}",
        names.join(",")
    );

    // Emit a single concatenated bundle of every migration's `up.sql`, in
    // sorted (== timestamp) directory order, joined with newlines.  This is
    // the "paved path" for testcontainers-backed integration tests: they can
    // `include_str!` it via `autumn_harvest::full_migrations_sql()` instead of
    // hand-rolling a `concat!(include_str!(...))` list that silently drifts
    // out of date whenever a new migration lands (issue: nd_block_tests was
    // missing 20260704000001_harvest_build_policy_ramp, so every start there
    // failed `column "target_build_id" does not exist` — invisible because CI
    // only compiled that suite, never ran it against a DB).
    //
    // Concatenating all `up.sql` files in timestamp order applies cleanly to a
    // fresh Postgres (the migrations are ordered and idempotent-safe: no live
    // `CREATE INDEX CONCURRENTLY`, distinct `ADD COLUMN`s, `DO $$` blocks that
    // are self-contained).
    let mut bundle = String::new();
    for name in &names {
        let up = migrations_dir.join(name).join("up.sql");
        if let Ok(sql) = fs::read_to_string(&up) {
            bundle.push_str(&sql);
            bundle.push('\n');
        }
    }
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR must be set by cargo");
    let bundle_path = Path::new(&out_dir).join("all_migrations_bundle.sql");
    fs::write(&bundle_path, bundle).expect("failed to write all_migrations_bundle.sql");
}
