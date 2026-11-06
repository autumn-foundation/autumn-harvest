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
    boot_plugin(profile, mode, open, plugin())
}

fn boot_plugin(profile: &str, mode: &str, open: &[&str], plugin: HarvestPlugin) -> Fixture {
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
        .plugin(plugin)
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
    // A signal answers `202`, as on Postgres (issue #2489).
    signal.assert_status(202);
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

/// The read routes and Vantage serve one tenant's file, and keep the admin
/// gate on a tenant route (issue #2468).
#[tokio::test(flavor = "multi_thread")]
async fn tenant_read_routes_and_vantage_read_only_the_tenant_file() {
    let fx = boot("dev");
    provision(&fx.fleet, &["acme", "bee"]).await;
    let acme = start(&fx.client, "acme", false).await;
    completed(&fx.client, "acme", &acme).await;

    let count: Value = fx
        .client
        .get("/harvest/tenants/acme/workflows/count")
        .send()
        .await
        .json();
    assert_eq!(count["total"], 1, "{count}");
    let other: Value = fx
        .client
        .get("/harvest/tenants/bee/workflows/count")
        .send()
        .await
        .json();
    assert_eq!(other["total"], 0, "{other}");
    fx.client
        .get(&format!("/harvest/tenants/acme/workflows/{acme}/timeline"))
        .send()
        .await
        .assert_ok();
    fx.client
        .get(&format!("/harvest/tenants/bee/workflows/{acme}/stack"))
        .send()
        .await
        .assert_status(404);
    let page = fx
        .client
        .get("/harvest/tenants/acme/ui/workflows")
        .send()
        .await;
    page.assert_ok();
    let page = String::from_utf8(page.body.clone()).unwrap();
    assert!(page.contains(&format!("workflows/{acme}")), "{page}");
    fx.fleet.close_all().await;

    let fx = boot("test");
    provision(&fx.fleet, &["acme"]).await;
    let logs = fx
        .client
        .get(&format!("/harvest/tenants/acme/workflows/{acme}/logs"))
        .send()
        .await;
    logs.assert_status(401);
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

/// A tenant readiness probe reads the tenant's Harvest file, not the
/// control file (issue #2489).
#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_ready_probe_reads_the_tenant_file() {
    let fx = boot("dev");
    provision(&fx.fleet, &["acme"]).await;
    start(&fx.client, "acme", true).await;

    // Take the control file away. Only the control probe sees it.
    let control = fx.dir.path().join("control.harvest.db");
    let moved = fx.dir.path().join("control.harvest.moved");
    std::fs::rename(&control, &moved).unwrap();
    let ready = fx.client.get("/harvest/health/ready").send().await;
    ready.assert_status(503);
    let ready: Value = ready.json();
    assert_eq!(ready["reasons"], json!(["database_unreachable"]));
    let tenant = fx
        .client
        .get("/harvest/tenants/acme/health/ready")
        .send()
        .await;
    tenant.assert_ok();
    let tenant: Value = tenant.json();
    assert_eq!(tenant["database_reachable"], true);

    std::fs::rename(&moved, &control).unwrap();
    fx.fleet.close_all().await;
}

// ── Auth on the tenant routes (issue #2469) ─────────────────────────────────

/// A tenant route keeps the class of its core route. A read token reads a
/// tenant, and gets `403` on a tenant mutation.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_token_reads_a_tenant_and_cannot_mutate_it() {
    let fx = boot_plugin("dev", "tenant", &[], plugin().enable_api_tokens());
    provision(&fx.fleet, &["acme"]).await;
    let minted = fx
        .client
        .post("/harvest/admin/tokens")
        .json(&json!({ "name": "reader", "scope": "read" }))
        .send()
        .await;
    minted.assert_status(201);
    let secret = minted.json::<Value>()["secret"]
        .as_str()
        .unwrap()
        .to_owned();
    let bearer = format!("Bearer {secret}");

    let read = fx
        .client
        .get("/harvest/tenants/acme/workflows")
        .header("authorization", &bearer)
        .send()
        .await;
    read.assert_ok();
    let start = fx
        .client
        .post("/harvest/tenants/acme/workflows/tenant_flow/start")
        .header("authorization", &bearer)
        .json(&json!({ "input": false }))
        .send()
        .await;
    start.assert_status(403);
    fx.fleet.close_all().await;
}

/// Denies the tenant `blocked` and the business key `secret`.
struct Policy;

impl autumn_harvest_plugin::authz::HarvestAuthorizer for Policy {
    fn authorize<'a>(
        &'a self,
        request: &'a autumn_harvest_plugin::authz::AuthzRequest<'a>,
    ) -> autumn_harvest_plugin::authz::AuthzFuture<'a> {
        use autumn_harvest_plugin::authz::AuthzDecision;
        let decision = if request.tenant_key == Some("blocked") {
            AuthzDecision::deny("tenant is blocked")
        } else if request.workflow_id == Some("secret") {
            AuthzDecision::deny("key is secret")
        } else {
            AuthzDecision::Allow
        };
        Box::pin(std::future::ready(decision))
    }
}

/// The authorizer sees the path tenant and the fields of a tenant start. A
/// tenant header that names another tenant is a `400`.
#[tokio::test(flavor = "multi_thread")]
async fn the_authorizer_sees_the_path_tenant_and_the_start_body() {
    let fx = boot_plugin("dev", "tenant", &[], plugin().with_authorizer(Policy));
    provision(&fx.fleet, &["acme", "blocked"]).await;

    let open = fx
        .client
        .get("/harvest/tenants/acme/workflows")
        .send()
        .await;
    open.assert_ok();
    let blocked = fx
        .client
        .get("/harvest/tenants/blocked/workflows")
        .send()
        .await;
    blocked.assert_status(403);
    let forged = fx
        .client
        .get("/harvest/tenants/blocked/workflows")
        .header("x-harvest-tenant", "acme")
        .send()
        .await;
    forged.assert_status(400);

    let secret = fx
        .client
        .post("/harvest/tenants/acme/workflows/tenant_flow/start")
        .json(&json!({ "workflow_id": "secret", "input": false }))
        .send()
        .await;
    secret.assert_status(403);
    let plain = fx
        .client
        .post("/harvest/tenants/acme/workflows/tenant_flow/start")
        .json(&json!({ "workflow_id": "plain", "input": false }))
        .send()
        .await;
    plain.assert_status(201);
    fx.fleet.close_all().await;
}

/// With OIDC login, a tenant health route stays public. Each other tenant
/// route needs a login (issue #2469).
#[cfg(feature = "oidc")]
#[tokio::test(flavor = "multi_thread")]
async fn an_oidc_login_keeps_tenant_health_public() {
    use autumn_harvest_plugin::oidc::{OAuth2ProviderConfig, OidcLogin};
    use autumn_harvest_plugin::roles::{ClaimRoleMap, ClaimRule, HarvestRoles, ROLE_VIEWER};

    // No request here logs in, so the provider is never called.
    let issuer = "http://127.0.0.1:9";
    let provider = OAuth2ProviderConfig {
        client_id: "harvest".to_owned(),
        client_secret: "secret".to_owned(),
        authorize_url: format!("{issuer}/authorize"),
        token_url: format!("{issuer}/token"),
        userinfo_url: None,
        redirect_uri: "http://127.0.0.1/harvest/auth/oidc/callback".to_owned(),
        scope: "openid".to_owned(),
        issuer: Some(issuer.to_owned()),
        jwks_url: Some(format!("{issuer}/jwks")),
        discovery_url: None,
    };
    let roles = HarvestRoles::builder().builtin_roles().build().unwrap();
    let claims = ClaimRoleMap::new().rule(ClaimRule::new("groups", "ops", ROLE_VIEWER));
    let login = OidcLogin::new(provider, roles, claims).unwrap();
    let plugin = HarvestPlugin::new()
        .workflows(workflows![tenant_flow])
        .activities(activities![whoami])
        .sqlite_per_tenant()
        .api_with_oidc("/harvest", login);
    let fx = boot_plugin("dev", "tenant", &[], plugin);
    provision(&fx.fleet, &["acme"]).await;

    let control = fx.client.get("/harvest/health/ready").send().await;
    control.assert_ok();
    let tenant = fx
        .client
        .get("/harvest/tenants/acme/health/ready")
        .send()
        .await;
    tenant.assert_ok();
    let runs = fx
        .client
        .get("/harvest/tenants/acme/workflows")
        .send()
        .await;
    runs.assert_status(401);
    let login_route = fx.client.get("/harvest/auth/oidc/login").send().await;
    assert_ne!(login_route.status.as_u16(), 404, "the login route exists");
    fx.fleet.close_all().await;
}

#[workflow]
async fn every_second(_ctx: &WorkflowContext, _n: Value) -> Result<(), String> {
    Ok(())
}

/// `harvest.scheduler_enabled = false` on a SQLite app (issue #2471): the
/// boot upserts no code schedule and no slot fires.
///
/// The flag is a process environment variable. This binary holds the test
/// because no other test in it uses a schedule, so the flag changes nothing
/// else here. `tests/sqlite_plugin.rs` proves that the same schedule fires
/// with the flag on.
#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_scheduler_fires_no_code_schedule() {
    // SAFETY: no other test in this binary reads the variable concurrently
    // for a schedule. Each one tolerates either value.
    unsafe { std::env::set_var("AUTUMN_HARVEST__SCHEDULER_ENABLED", "false") };
    let dir = tempfile::tempdir().unwrap();
    let mut config = AutumnConfig::default();
    config.database.url = Some(format!("sqlite://{}", dir.path().join("app.db").display()));
    let client = TestApp::new()
        .config(config)
        .profile("dev")
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![every_second])
                .workflow_schedule(autumn_harvest::policy::WorkflowSchedule::new(
                    "every_second",
                    autumn_harvest::policy::Schedule::Interval(Duration::from_secs(1)),
                ))
                .api("/harvest"),
        )
        .build();
    // SAFETY: as above. The boot read the variable already.
    unsafe { std::env::remove_var("AUTUMN_HARVEST__SCHEDULER_ENABLED") };

    // Three slots pass. With the scheduler on, each one starts a run.
    tokio::time::sleep(Duration::from_millis(3200)).await;
    let schedules: Value = client.get("/harvest/admin/schedules").send().await.json();
    assert_eq!(schedules, json!([]), "no code schedule is upserted");
    let runs: Value = client
        .get("/harvest/workflows?workflow_name=every_second")
        .send()
        .await
        .json();
    assert_eq!(runs, json!([]), "no slot fires");
}

/// The outbox of a fleet app lives in the control database, and the control
/// Harvest runs each start. A tenant database has no outbox table, so a
/// write there fails rather than wait for a relay (issue #2470).
#[tokio::test(flavor = "multi_thread")]
async fn the_outbox_runs_on_the_control_harvest_and_refuses_a_tenant_database() {
    use autumn_harvest_plugin::WorkflowStartRequest;
    use autumn_harvest_plugin::sqlite::{HarvestSqlite, enqueue_workflow_start_outbox};

    let fixture = boot_with("dev", "tenant", &["acme"]);
    let state = fixture.client.state();
    let start = WorkflowStartRequest {
        workflow_name: "tenant_flow".to_owned(),
        workflow_id: "o-fleet".to_owned(),
        queue_name: "default".to_owned(),
        input: json!(false),
        memo: None,
        search_attrs: None,
    };

    let tenant = fixture.fleet.open_for("acme").await.expect("open acme");
    let mut conn = tenant.pool().get().await.expect("a tenant connection");
    let refused = enqueue_workflow_start_outbox(&mut conn, &start)
        .await
        .expect_err("a tenant database has no outbox");
    assert!(refused.to_string().contains("#2470"), "{refused}");
    drop(conn);

    let mut conn = state.pool().unwrap().get().await.unwrap();
    enqueue_workflow_start_outbox(&mut conn, &start)
        .await
        .unwrap();
    drop(conn);
    autumn_harvest_plugin::flush_workflow_start_outbox(state)
        .await
        .unwrap();
    let control = state.extension::<HarvestSqlite>().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let filter = autumn_harvest_plugin::sqlite::ExecutionFilter::new()
            .workflow_name("tenant_flow")
            .workflow_id("o-fleet".to_owned())
            .state("COMPLETED");
        let page = control.list(filter).await.unwrap();
        if let Some(run) = page.executions.first() {
            let exec = run.execution_id().unwrap();
            let result = control.result(exec).await.unwrap();
            assert_eq!(result.output, Some(json!({ "who": "control" })));
            break;
        }
        assert!(Instant::now() < deadline, "the start did not run");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
