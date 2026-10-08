#![cfg(feature = "dev-runtime-managed")]
//! The `dev-runtime-managed` acquisition path (issue #525, AC2).
//!
//! This is the half of AC2 that says "on a clean machine with only the Rust
//! toolchain": when no `PostgreSQL` server is installed, the runtime downloads
//! and extracts one. Everything else in the suite runs against a Postgres that
//! is already there, so without this the download path would be compiled and
//! never executed.
//!
//! **Opt-in**, because it fetches roughly 30 MB over the network:
//!
//! ```text
//! HARVEST_DEV_TEST_DOWNLOAD=1 cargo test -p autumn-harvest-plugin \
//!     --features dev-runtime-managed --test dev_runtime_managed
//! ```
//!
//! CI compiles this target on every leg (a `compileonly` manifest row) so it
//! cannot rot, and the opt-in run is how the path is actually exercised.

use autumn_harvest_plugin::dev::{DevRuntimeConfig, EphemeralPostgres, running_as_root};

#[tokio::test]
async fn a_downloaded_postgres_can_actually_run_a_cluster() {
    if std::env::var("HARVEST_DEV_TEST_DOWNLOAD").as_deref() != Ok("1") {
        eprintln!("SKIP: set HARVEST_DEV_TEST_DOWNLOAD=1 to exercise the ~30 MB download");
        return;
    }

    // A cache of its own, so the test proves a *fresh* acquisition rather than
    // finding whatever a previous run left in the user's cache.
    let cache = std::env::temp_dir().join(format!("harvest-dev-dl-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cache);
    std::fs::create_dir_all(&cache).expect("cache dir");
    // SAFETY: single-threaded at this point in the test, and the variable is
    // read once inside the call below.
    unsafe { std::env::set_var("HARVEST_DEV_CACHE_DIR", &cache) };

    let binaries = autumn_harvest_plugin::dev::acquire::acquire_postgres_binaries()
        .await
        .expect("the managed tier should acquire a PostgreSQL build");

    // Not just "files landed": the acquired binaries must be able to provision
    // and run a real cluster, which is the only claim that matters.
    for tool in autumn_harvest_plugin::dev::REQUIRED_TOOLS {
        assert!(
            binaries.tool(tool).is_file(),
            "{tool} missing from {}",
            binaries.bin_dir().display()
        );
    }

    if running_as_root() {
        eprintln!("SKIP the run half: PostgreSQL refuses to run as root by design");
        let _ = std::fs::remove_dir_all(&cache);
        return;
    }

    let postgres = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
        .await
        .expect("a downloaded PostgreSQL should start");
    let version = postgres
        .query_scalar("SELECT current_setting('server_version') AS value")
        .await
        .expect("server_version");
    assert!(
        version.starts_with("16."),
        "expected a 16.x build, got {version}"
    );
    postgres.shutdown().await.expect("shutdown");

    // A second call must reuse the cache rather than downloading again.
    let again = autumn_harvest_plugin::dev::acquire::acquire_postgres_binaries()
        .await
        .expect("a populated cache should be reused");
    assert_eq!(again.bin_dir(), binaries.bin_dir());

    let _ = std::fs::remove_dir_all(&cache);
}
