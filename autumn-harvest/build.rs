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
}
