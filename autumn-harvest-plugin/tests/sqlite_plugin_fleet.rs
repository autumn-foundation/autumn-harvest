//! Per-tenant Harvest in a SQLite fleet app (issue #2472).
//!
//! Each test boots a real app on a temp directory: a control database, a
//! tenant fleet and the plugin with `sqlite_per_tenant()`. No Postgres and
//! no Docker run.
//!
//! ```bash
//! cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_fleet
//! ```

#![cfg(feature = "sqlite")]
// The `#[workflow]` and `#[activity]` macros need an `async fn`. The
// activity macro reads an unused, underscore-named input.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{HarvestFleet, HarvestTenant, StartOptions};
use autumn_web::config::{AutumnConfig, DatabaseConfig, DatabaseFleetConfig};
use autumn_web::db::fleet::DatabaseFleet;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::test::{TestApp, TestClient};
use serde_json::{Value, json};

/// Names the tenant whose driver runs it.
#[activity]
async fn whoami(_ctx: &ActivityContext, _input: Value) -> Result<String, String> {
    Ok(HarvestTenant::current().map_or_else(|| "control".to_owned(), |key| key.name()))
}

#[workflow]
async fn tenant_flow(ctx: &WorkflowContext, wait: bool) -> Result<Value, String> {
    let who: String = ctx
        .execute_activity(&whoami_info(), json!(null))
        .await
        .map_err(|error| error.to_string())?;
    if wait {
        ctx.wait_for_signal("go")
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(json!({ "who": who }))
}

struct Fixture {
    dir: tempfile::TempDir,
    fleet: DatabaseFleet,
    client: TestClient,
}

fn fleet(root: &Path, mode: &str) -> DatabaseFleet {
    let config: DatabaseFleetConfig = serde_json::from_value(json!({
        "mode": mode,
        "root": root.display().to_string(),
        "max_open": 8,
        "pool_size": 2,
        "create_on_demand": true,
        "idle_close_secs": 0,
    }))
    .expect("fleet config");
    DatabaseFleet::builder(config).build().expect("fleet")
}

fn plugin() -> HarvestPlugin {
    HarvestPlugin::new()
        .workflows(workflows![tenant_flow])
        .activities(activities![whoami])
        .sqlite_per_tenant()
        .api("/harvest")
}

fn boot(profile: &str) -> Fixture {
    boot_with(profile, "tenant", &[])
}

/// Boot an app on a fresh fleet. Each tenant in `open` is open before the
/// plugin starts.
fn boot_with(profile: &str, mode: &str, open: &[&str]) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut config = AutumnConfig::default();
    let url = format!("sqlite://{}", dir.path().join("control.db").display());
    config.database.url = Some(url.clone());
    let control = autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .expect("control pool")
    .expect("a url gives a pool");
    let fleet = fleet(&dir.path().join("fleet"), mode);
    for tenant in open {
        let key = fleet.key_for(tenant).expect("key");
        let opened = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("runtime")
                        .block_on(fleet.provision(&key))
                })
                .join()
                .expect("provision thread")
        });
        // The fleet keeps the database open after the handle drops.
        drop(opened.expect("provision"));
    }
    let client = TestApp::new()
        .config(config)
        .profile(profile)
        .with_db(control)
        .with_fleet(fleet.clone())
        .plugin(plugin())
        .build();
    Fixture { dir, fleet, client }
}

/// Create each tenant database, as an app does before it serves a tenant.
async fn provision(fleet: &DatabaseFleet, tenants: &[&str]) {
    for tenant in tenants {
        drop(
            fleet
                .provision(&key(fleet, tenant))
                .await
                .expect("provision"),
        );
    }
}

fn key(fleet: &DatabaseFleet, tenant: &str) -> FleetDbKey {
    fleet.key_for(tenant).expect("tenant key")
}

/// `acme.db` gives `acme.harvest.db`.
fn harvest_file(fleet: &DatabaseFleet, tenant: &str) -> PathBuf {
    let db = fleet.path_of(&key(fleet, tenant));
    let stem = db.file_stem().unwrap().to_string_lossy().into_owned();
    db.with_file_name(format!("{stem}.harvest.db"))
}

async fn wait_for(client: &TestClient, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.get(path).send().await;
        if response.status.is_success() {
            let body: Value = response.json();
            if done(&body) {
                return body;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting on {path}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn start(client: &TestClient, tenant: &str, wait: bool) -> String {
    let response = client
        .post(&format!(
            "/harvest/tenants/{tenant}/workflows/tenant_flow/start"
        ))
        .json(&json!({ "input": wait }))
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

async fn completed(client: &TestClient, tenant: &str, exec: &str) -> Value {
    let body = wait_for(
        client,
        &format!("/harvest/tenants/{tenant}/workflows/{exec}"),
        |body| body["execution"]["state"] == "COMPLETED",
    )
    .await;
    body["execution"]["output"].clone()
}

async fn list_len(client: &TestClient, path: &str) -> usize {
    let response = client.get(path).send().await;
    response.assert_ok();
    let body: Value = response.json();
    body.as_array().map_or(0, Vec::len)
}

/// Each tenant runs its runs in its own file, on its own driver.
#[tokio::test(flavor = "multi_thread")]
async fn each_open_tenant_runs_its_own_harvest_file() {
    let fx = boot("dev");
    provision(&fx.fleet, &["acme", "bee"]).await;

    let acme = start(&fx.client, "acme", false).await;
    let bee = start(&fx.client, "bee", false).await;
    assert_eq!(
        completed(&fx.client, "acme", &acme).await["who"],
        "tenant:acme"
    );
    assert_eq!(
        completed(&fx.client, "bee", &bee).await["who"],
        "tenant:bee"
    );

    assert!(harvest_file(&fx.fleet, "acme").exists());
    assert!(harvest_file(&fx.fleet, "bee").exists());
    // No tenant sees another tenant's runs. The control Harvest has none.
    assert_eq!(
        list_len(&fx.client, "/harvest/tenants/acme/workflows").await,
        1
    );
    assert_eq!(
        list_len(&fx.client, "/harvest/tenants/bee/workflows").await,
        1
    );
    assert_eq!(list_len(&fx.client, "/harvest/workflows").await, 0);
    // Each tenant route reads its own tenant. A second id segment works.
    let by_tenant = fx
        .client
        .get(&format!("/harvest/tenants/acme/workflows/{acme}/history"))
        .send()
        .await;
    by_tenant.assert_ok();
    let missing = fx
        .client
        .get(&format!("/harvest/tenants/bee/workflows/{acme}"))
        .send()
        .await;
    missing.assert_status(404);

    // A control run has no tenant.
    let response = fx
        .client
        .post("/harvest/workflows/tenant_flow/start")
        .json(&json!({ "input": false }))
        .send()
        .await;
    response.assert_status(201);
    let control: Value = response.json();
    let control = control["execution_id"].as_str().unwrap().to_owned();
    let output = wait_for(
        &fx.client,
        &format!("/harvest/workflows/{control}"),
        |body| body["execution"]["state"] == "COMPLETED",
    )
    .await;
    assert_eq!(output["execution"]["output"]["who"], "control");

    // A Harvest file never reads as a fleet database.
    let listed = fx.fleet.list().await.expect("list");
    assert_eq!(listed.len(), 2, "{listed:?}");
    // The control Harvest stays next to the control database.
    assert!(fx.dir.path().join("control.harvest.db").exists());
    fx.fleet.close_all().await;
}

/// App code gets a tenant handle that keeps the tenant open.
#[tokio::test(flavor = "multi_thread")]
async fn app_code_starts_a_run_in_one_tenant() {
    let fx = boot("dev");
    let harvest = fx
        .client
        .state()
        .extension::<HarvestFleet>()
        .expect("the plugin installs HarvestFleet");

    let tenant = harvest.open_for("acme").await.expect("open acme");
    assert_eq!(tenant.path(), harvest_file(&fx.fleet, "acme"));
    let started = tenant
        .start("tenant_flow", json!(false), StartOptions::new())
        .await
        .expect("start");
    let output = completed(&fx.client, "acme", &started.exec_id.to_string()).await;
    assert_eq!(output["who"], "tenant:acme");

    assert_eq!(tenant.tenant(), Some(&key(&fx.fleet, "acme")));
    // The fleet owns the tenant driver, so a handle cannot stop it.
    assert!(tenant.shutdown().await.is_err());

    // The fleet counts a tenant idle when only its own lease is left. A
    // caller's handle adds one. The plugin's registry adds none.
    let leases = || {
        let lease = fx.fleet.get_open(&key(&fx.fleet, "acme")).unwrap().lease();
        std::sync::Arc::strong_count(&lease) - 1
    };
    assert_eq!(leases(), 2, "the fleet and the caller's handle");
    assert_eq!(harvest.open_tenants(), vec![key(&fx.fleet, "acme")]);
    drop(tenant);
    assert_eq!(leases(), 1, "the registry holds no lease");
    fx.fleet.close_all().await;
}

/// A close stops the tenant's driver and frees its writer lock. The next
/// open resumes the run from the file.
#[tokio::test(flavor = "multi_thread")]
async fn a_close_stops_the_driver_and_a_reopen_resumes() {
    let fx = boot("dev");
    provision(&fx.fleet, &["acme"]).await;
    let exec = start(&fx.client, "acme", true).await;
    wait_for(
        &fx.client,
        &format!("/harvest/tenants/acme/workflows/{exec}/history"),
        |body| {
            body["events"]
                .as_array()
                .is_some_and(|events| events.iter().any(|e| e["type"] == "ActivityCompleted"))
        },
    )
    .await;

    fx.fleet.close(&key(&fx.fleet, "acme")).await;
    let harvest = fx.client.state().extension::<HarvestFleet>().unwrap();
    assert_eq!(harvest.open_tenants(), Vec::<FleetDbKey>::new());
    // The writer lock is free, so the driver has stopped.
    let path = harvest_file(&fx.fleet, "acme");
    drop(autumn_harvest_sqlite::SqliteRuntime::open(&path).expect("the lock is free"));

    let signal = fx
        .client
        .post(&format!("/harvest/tenants/acme/workflows/{exec}/signal/go"))
        .json(&json!({}))
        .send()
        .await;
    signal.assert_ok();
    assert_eq!(
        completed(&fx.client, "acme", &exec).await["who"],
        "tenant:acme"
    );
    fx.fleet.close_all().await;
}

/// A tenant delete erases its workflow history too.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_removes_the_tenant_harvest_file() {
    let fx = boot("dev");
    provision(&fx.fleet, &["acme"]).await;
    let exec = start(&fx.client, "acme", false).await;
    completed(&fx.client, "acme", &exec).await;
    let path = harvest_file(&fx.fleet, "acme");
    assert!(path.exists());
    let sibling = |suffix: &str| PathBuf::from(format!("{}{suffix}", path.display()));
    // A clean close removes the WAL files. Stubs prove the delete removes
    // each sidecar itself.
    fx.fleet.close(&key(&fx.fleet, "acme")).await;
    for suffix in ["-wal", "-shm", "-journal"] {
        std::fs::write(sibling(suffix), b"stub").unwrap();
    }

    assert!(
        fx.fleet
            .delete(&key(&fx.fleet, "acme"))
            .await
            .expect("delete")
    );
    for suffix in ["", "-wal", "-shm", "-journal"] {
        assert!(!sibling(suffix).exists(), "{suffix} survived the delete");
    }
    // The writer lock file stays, as the lock module requires.
    assert!(sibling(".lock").exists());
    let harvest = fx.client.state().extension::<HarvestFleet>().unwrap();
    assert_eq!(harvest.open_tenants(), Vec::<FleetDbKey>::new());
    fx.fleet.close_all().await;
}

/// The tenant routes sit behind the #1802 mutation gate.
#[tokio::test(flavor = "multi_thread")]
async fn an_anonymous_tenant_mutation_outside_dev_is_refused() {
    // `test` is not `dev`, so the gate is closed. `prod` would also check
    // the `Host` header, which this test does not exercise.
    let fx = boot("test");
    provision(&fx.fleet, &["acme"]).await;
    let response = fx
        .client
        .post("/harvest/tenants/acme/workflows/tenant_flow/start")
        .json(&json!({ "input": false }))
        .send()
        .await;
    response.assert_status(401);
    // A read still answers.
    fx.client
        .get("/harvest/tenants/acme/workflows")
        .send()
        .await
        .assert_ok();
    fx.fleet.close_all().await;
}

/// The management API never creates a tenant, even when the fleet creates
/// on demand. An unknown tenant is a `404` and leaves no file behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_route_never_creates_a_tenant() {
    let fx = boot("dev");
    for (method, path) in [
        ("GET", "/harvest/tenants/ghost/workflows"),
        ("GET", "/harvest/tenants/ghost/health/live"),
        ("POST", "/harvest/tenants/ghost/workflows/tenant_flow/start"),
    ] {
        let response = if method == "GET" {
            fx.client.get(path).send().await
        } else {
            fx.client
                .post(path)
                .json(&json!({ "input": false }))
                .send()
                .await
        };
        response.assert_status(404);
    }
    assert!(!fx.fleet.exists(&key(&fx.fleet, "ghost")));
    assert!(!harvest_file(&fx.fleet, "ghost").exists());
    fx.fleet.close_all().await;
}

/// A routing key that names no tenant is a `400`, as the fleet rules it.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_tenant_key_is_a_bad_request() {
    let fx = boot("dev");
    fx.client
        .get("/harvest/tenants/Not%20Valid/workflows")
        .send()
        .await
        .assert_status(400);
    fx.fleet.close_all().await;
}

/// Per-tenant Harvest needs a fleet. The boot refuses an app without one.
#[test]
#[should_panic(expected = "sqlite_per_tenant")]
fn per_tenant_without_a_fleet_fails_the_boot() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.path().join("app.db").display()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let _client = TestApp::new()
            .config(config)
            .profile("dev")
            .plugin(plugin())
            .build();
    });
}

/// A slot fleet never lists a Harvest file as a slot database.
#[tokio::test(flavor = "multi_thread")]
async fn a_slot_fleet_never_lists_a_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let config: DatabaseFleetConfig = serde_json::from_value(json!({
        "mode": "slot",
        "root": dir.path().display().to_string(),
        "idle_close_secs": 0,
    }))
    .expect("fleet config");
    let fleet = DatabaseFleet::builder(config).build().expect("fleet");
    let key = FleetDbKey::slot(42).unwrap();
    drop(fleet.provision(&key).await.expect("provision"));
    let harvest = autumn_harvest_plugin::sqlite::tenant_harvest_file(&fleet.path_of(&key));
    assert!(
        harvest.ends_with("slot-00042.harvest.db"),
        "{}",
        harvest.display()
    );
    std::fs::write(&harvest, b"").unwrap();
    assert_eq!(fleet.list().await.expect("list"), vec![key]);
    fleet.close_all().await;
}

/// A tenant that is open before the plugin starts gets a driver at startup,
/// before any request reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_open_before_startup_gets_a_driver() {
    let fx = boot_with("dev", "tenant", &["acme"]);
    let harvest = fx.client.state().extension::<HarvestFleet>().unwrap();
    assert_eq!(harvest.open_tenants(), vec![key(&fx.fleet, "acme")]);
    fx.fleet.close_all().await;
}

/// A slot fleet works the same way. A routing key reaches its slot, and the
/// slot is the isolation unit.
#[tokio::test(flavor = "multi_thread")]
async fn a_slot_fleet_runs_one_harvest_per_slot() {
    let fx = boot_with("dev", "slot", &["acme"]);
    let exec = start(&fx.client, "acme", false).await;
    let slot = key(&fx.fleet, "acme");
    assert!(matches!(slot, FleetDbKey::Slot(_)), "{slot:?}");
    assert_eq!(
        completed(&fx.client, "acme", &exec).await["who"],
        slot.name()
    );
    assert!(harvest_file(&fx.fleet, "acme").exists());
    fx.fleet.close_all().await;
}
