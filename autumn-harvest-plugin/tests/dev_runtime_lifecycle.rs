#![cfg(feature = "dev-runtime")]
//! End-to-end lifecycle coverage for the zero-setup dev runtime (issue #525).
//!
//! These are the falsifiable halves of the acceptance criteria — the ones that
//! only mean anything once a real postmaster has actually been started:
//!
//! | AC | Test |
//! |----|------|
//! | AC1 storage lifecycle managed, migrations applied, no manual `DATABASE_URL` | `a_durable_workflow_executes_and_is_observable` |
//! | AC2 a durable workflow executes and is observable | `a_durable_workflow_executes_and_is_observable` |
//! | AC5 teardown reclaims *all* ephemeral state | `shutdown_leaves_no_process_and_no_data_directory` |
//! | AC5 `SIGKILL` survivors are reclaimed on the next start | `the_reaper_reclaims_an_abandoned_session_directory` |
//!
//! **Skips cleanly** (prints `SKIP:` and returns) where no Postgres server
//! binaries are discoverable, so CI legs without them stay green — the same
//! convention `status_summary_localpg.rs` uses for `DATABASE_URL`.
//!
//! A silent skip is also how a suite quietly stops testing anything, and this
//! one is invisible to `ci_run_coverage.rs` (it uses no live-DB token, so that
//! guard never classifies it). Set **`HARVEST_DEV_REQUIRE_POSTGRES=1`** — as the
//! Linux CI leg does — to turn every skip into a failure, so a runner image that
//! drops `PostgreSQL` is a red build rather than a green no-op.

use std::path::Path;

use autumn_harvest_plugin::dev::{
    DevRuntimeConfig, EphemeralPostgres, PostgresBinaries, running_as_root,
};

/// The binaries to provision with, or `None` with a printed reason.
///
/// Two environmental reasons to skip rather than fail: no Postgres server
/// binaries anywhere, and running as `root` (`PostgreSQL` refuses to start as
/// root by design, so a root container simply cannot exercise this path).
fn binaries() -> Option<PostgresBinaries> {
    if running_as_root() {
        return skip("running as root; PostgreSQL refuses to run as root by design");
    }
    match PostgresBinaries::discover() {
        Ok(binaries) => Some(binaries),
        Err(error) => skip(&format!(
            "no Postgres server binaries discoverable: {error}"
        )),
    }
}

/// Report an environmental skip — or fail, when the environment promised to
/// provide one.
///
/// A silent skip is how a suite quietly stops testing anything, and this one is
/// invisible to `ci_run_coverage.rs`. `HARVEST_DEV_REQUIRE_POSTGRES=1` makes a
/// runner image that lost its `PostgreSQL` a red build rather than a green
/// no-op.
fn skip(reason: &str) -> Option<PostgresBinaries> {
    assert!(
        std::env::var("HARVEST_DEV_REQUIRE_POSTGRES").as_deref() != Ok("1"),
        "HARVEST_DEV_REQUIRE_POSTGRES=1 but this suite would have skipped: {reason}"
    );
    eprintln!("SKIP: {reason}");
    None
}

#[tokio::test]
async fn provisioned_storage_is_a_real_loopback_postgres() {
    let Some(binaries) = binaries() else { return };

    let postgres = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
        .await
        .expect("ephemeral postgres should start");

    // Nobody set `DATABASE_URL`: the runtime produced its own, and it is local.
    let dsn = postgres.database_url().to_owned();
    assert!(dsn.contains("127.0.0.1"), "{dsn}");
    assert!(
        matches!(
            autumn_harvest_plugin::dev::classify_database_url(&dsn),
            autumn_harvest_plugin::dev::DatabaseSafety::Allowed
        ),
        "a DSN we generated must pass our own safety gate: {dsn}"
    );

    // It really is `PostgreSQL`, not a stand-in — AC6's "storage stays Postgres".
    let version = postgres
        .query_scalar("SELECT current_setting('server_version') AS value")
        .await
        .expect("server_version");
    assert!(!version.is_empty(), "{version}");

    // And it is reachable only on loopback.
    let listening = postgres
        .query_scalar("SELECT current_setting('listen_addresses') AS value")
        .await
        .expect("listen_addresses");
    assert_eq!(listening, "127.0.0.1");

    postgres.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn shutdown_leaves_no_process_and_no_data_directory() {
    let Some(binaries) = binaries() else { return };

    let postgres = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
        .await
        .expect("start");
    let session_dir = postgres.session_dir().to_path_buf();
    let postmaster_pid = postgres.postmaster_pid();

    assert!(
        session_dir.exists(),
        "the session dir must exist while running"
    );
    assert!(
        autumn_harvest_plugin::dev::process_is_alive(postmaster_pid),
        "the postmaster must be running"
    );

    postgres.shutdown().await.expect("shutdown");

    assert!(
        !session_dir.exists(),
        "AC5: the data directory must be reclaimed, {} still exists",
        session_dir.display()
    );
    assert!(
        !autumn_harvest_plugin::dev::process_is_alive(postmaster_pid),
        "AC5: postmaster {postmaster_pid} is still running after shutdown"
    );
}

#[tokio::test]
async fn dropping_the_handle_without_calling_shutdown_still_reclaims_state() {
    let Some(binaries) = binaries() else { return };

    let (session_dir, postmaster_pid) = {
        let postgres = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
            .await
            .expect("start");
        (
            postgres.session_dir().to_path_buf(),
            postgres.postmaster_pid(),
        )
        // dropped here without `shutdown()` — the panic path
    };

    // No polling loop: `Drop` is entirely synchronous, so by the time the block
    // above ends the teardown has already succeeded or already failed. A wait
    // here could only mask a regression that made it asynchronous.
    assert!(
        !session_dir.exists(),
        "AC5: the data directory must be reclaimed by the Drop guard, {} still exists",
        session_dir.display()
    );
    assert!(
        !autumn_harvest_plugin::dev::process_is_alive(postmaster_pid),
        "AC5: postmaster {postmaster_pid} survived the Drop guard"
    );
}

#[tokio::test]
async fn the_reaper_reclaims_an_abandoned_session_directory() {
    let Some(binaries) = binaries() else { return };

    // Its own session root, so the count below means what it says rather than
    // being satisfied by an unrelated leftover from a sibling test.
    let base = std::env::temp_dir().join(format!("harvest-dev-reaper-{}", std::process::id()));
    std::fs::create_dir_all(&base).expect("base");
    let config = DevRuntimeConfig {
        session_root: Some(base.clone()),
        ..DevRuntimeConfig::default()
    };

    // Simulate a `kill -9`d predecessor: a real running postmaster whose owner
    // process no longer exists. `forget` deliberately skips the Drop guard.
    let postgres = EphemeralPostgres::start(&binaries, &config)
        .await
        .expect("start");
    let session_dir = postgres.session_dir().to_path_buf();
    let postmaster_pid = postgres.postmaster_pid();
    postgres.leak_for_reaper_test();

    // Rewrite the record so its owner pid is one that cannot be alive.
    autumn_harvest_plugin::dev::rewrite_owner_pid_for_test(&session_dir, dead_pid());

    let reclaimed = autumn_harvest_plugin::dev::reap_stale_sessions(Path::new(
        session_dir.parent().expect("parent"),
    ))
    .expect("reap");
    assert_eq!(
        reclaimed, 1,
        "exactly this session should have been reclaimed"
    );

    assert!(!session_dir.exists(), "{}", session_dir.display());
    assert!(
        !autumn_harvest_plugin::dev::process_is_alive(postmaster_pid),
        "the reaper must stop the orphaned postmaster, not just delete its dir"
    );
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn a_busy_http_port_is_refused_before_any_cluster_is_created() {
    let Some(_binaries) = binaries() else { return };

    // autumn-web `process::exit(1)`s on a bind failure, skipping every
    // destructor — so a cluster created before the port is settled would be
    // stranded outright. Hold the port and prove nothing is provisioned.
    let held = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = held.local_addr().expect("addr").port();

    let base = std::env::temp_dir().join(format!("harvest-dev-portclash-{}", std::process::id()));
    std::fs::create_dir_all(&base).expect("base");

    let error = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
        http_port: port,
        session_root: Some(base.clone()),
        ..DevRuntimeConfig::default()
    })
    .await
    .expect_err("a busy port must be refused");
    assert!(
        matches!(
            error,
            autumn_harvest_plugin::dev::DevError::HttpPortUnavailable { .. }
        ),
        "{error}"
    );

    // Nothing was provisioned: no session directory exists under our root.
    let sessions: Vec<_> = std::fs::read_dir(&base)
        .into_iter()
        .flatten()
        .flatten()
        .flat_map(|root| {
            std::fs::read_dir(root.path())
                .into_iter()
                .flatten()
                .flatten()
        })
        .collect();
    assert!(
        sessions.is_empty(),
        "no cluster should have been created: {sessions:?}"
    );

    drop(held);
    let _ = std::fs::remove_dir_all(&base);
}

/// A pid that is certain not to be running: `pid_max` is at least 2^15 on every
/// platform we support and this exceeds the 32-bit ceiling.
const fn dead_pid() -> u32 {
    u32::MAX - 1
}

#[tokio::test]
async fn two_concurrent_dev_runtimes_do_not_collide() {
    let Some(binaries) = binaries() else { return };

    let first = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
        .await
        .expect("first");
    let second = EphemeralPostgres::start(&binaries, &DevRuntimeConfig::default())
        .await
        .expect("second");

    assert_ne!(first.session_dir(), second.session_dir());
    assert_ne!(first.port(), second.port());
    assert_ne!(first.database_url(), second.database_url());

    first.shutdown().await.expect("first shutdown");
    // The first teardown must not have taken the second down with it.
    assert!(autumn_harvest_plugin::dev::process_is_alive(
        second.postmaster_pid()
    ));
    second.shutdown().await.expect("second shutdown");
}

#[tokio::test]
async fn a_durable_workflow_executes_and_is_observable() {
    // The runtime provisions its own cluster; this only establishes that it
    // *can*, so the test skips rather than fails where nothing is installed.
    let Some(_binaries) = binaries() else { return };

    let runtime = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
        // Port 0: let the kernel pick, so the test never fights a real dev run.
        http_port: 0,
        ..DevRuntimeConfig::default()
    })
    .await
    .expect("dev runtime should start");

    let base = runtime.api_url().to_owned();
    let client = reqwest::Client::new();

    // AC1: migrations were applied automatically — no `diesel migration run`,
    // no `DATABASE_URL`, no compose file. If they had not been, the engine's
    // tables would not exist and the start below would fail; assert it directly
    // so the failure names the actual cause.
    let health = client
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("health request");
    assert!(health.status().is_success(), "{:?}", health.status());

    let started = client
        .post(format!("{base}/workflows/dev_greeting/start"))
        .json(&serde_json::json!({ "workflow_id": "dev-1", "input": "World" }))
        .send()
        .await
        .expect("start request");
    assert!(started.status().is_success(), "{:?}", started.status());
    let started: serde_json::Value = started.json().await.expect("start response");
    // The read routes are keyed by execution id, not by the caller's business
    // `workflow_id`.
    let execution_id = started["execution_id"]
        .as_str()
        .expect("start response should carry an execution_id")
        .to_owned();

    // The worker is polling: the execution should reach a terminal state.
    let mut status = String::new();
    for _ in 0..300 {
        let response = client
            .get(format!("{base}/workflows/{execution_id}"))
            .send()
            .await
            .expect("status request");
        if response.status().is_success() {
            let body: serde_json::Value = response.json().await.expect("json");
            // The detail response nests the execution row; its lifecycle column
            // is `state`.
            status = body["execution"]["state"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if status == "COMPLETED" {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert_eq!(
        status, "COMPLETED",
        "the sample workflow should complete (execution {execution_id})"
    );

    // AC2: observable in the Vantage UI, not merely in the API.
    let ui = client
        .get(runtime.ui_url())
        .send()
        .await
        .expect("ui request");
    assert!(ui.status().is_success());
    let html = ui.text().await.expect("ui body");
    assert!(
        html.contains("dev-1"),
        "the execution must appear in the UI"
    );

    runtime.shutdown().await.expect("shutdown");
}
