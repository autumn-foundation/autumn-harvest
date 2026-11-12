//! A closed tenant wakes when its next timer or schedule slot is due.
//!
//! Each test boots a real app on a temp directory: a control database, a
//! tenant fleet and the plugin with `sqlite_per_tenant()`. A test reads the
//! tenant Harvest file read only, so no read opens the tenant.
//!
//! ```bash
//! cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_fleet_wake
//! ```

#![cfg(feature = "sqlite")]
// The `#[workflow]` macro needs an `async fn`, and reads the unused input.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{ExecutionFilter, ExecutionSummary, HarvestFleet};
use autumn_harvest_sqlite::SqliteReader;
use autumn_web::config::{AutumnConfig, DatabaseConfig, DatabaseFleetConfig};
use autumn_web::db::fleet::DatabaseFleet;
use autumn_web::db::fleet_ownership::{
    FleetOwnership, OwnerId, SlotOwnershipStore, SlotRange, SqlSlotOwnership,
};
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::test::{TestApp, TestClient};
use serde_json::{Value, json};

/// The driver tick and the sweep interval of each test app.
const TICK: Duration = Duration::from_millis(250);

/// The time that a background open and a first drive pass may take on a
/// loaded CI host.
const SLACK: Duration = Duration::from_millis(1500);

/// Sleeps on a durable timer of `secs` seconds.
#[workflow]
async fn napper(ctx: &WorkflowContext, secs: u64) -> Result<(), String> {
    ctx.timer("nap", secs).await.map_err(|e| e.to_string())?;
    Ok(())
}

/// The target of a schedule. It ends at once.
#[workflow]
async fn beat(_ctx: &WorkflowContext, _n: Value) -> Result<(), String> {
    Ok(())
}

struct Host {
    fleet: DatabaseFleet,
    client: TestClient,
}

fn fleet_at(root: &Path) -> DatabaseFleet {
    let config: DatabaseFleetConfig = serde_json::from_value(json!({
        "mode": "tenant",
        "root": root.display().to_string(),
        "max_open": 8,
        "pool_size": 2,
        "create_on_demand": true,
        "idle_close_secs": 0,
    }))
    .expect("fleet config");
    DatabaseFleet::builder(config).build().expect("fleet")
}

fn control_url(dir: &Path) -> String {
    format!("sqlite://{}", dir.join("control.db").display())
}

fn control_pool(
    dir: &Path,
) -> diesel_async::pooled_connection::deadpool::Pool<autumn_web::db::RuntimeConnection> {
    autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(control_url(dir)),
        ..DatabaseConfig::default()
    })
    .expect("control pool")
    .expect("a url gives a pool")
}

/// Boot one host on `root`, with the control database in `dir`. The control
/// Harvest of each host gets its own file.
fn boot(dir: &Path, host: &str, fleet: DatabaseFleet) -> Host {
    let mut config = AutumnConfig::default();
    config.database.url = Some(control_url(dir));
    let plugin = HarvestPlugin::new()
        .workflows(workflows![napper, beat])
        .sqlite_per_tenant()
        .sqlite_path(dir.join(format!("{host}.harvest.db")))
        .api("/harvest");
    let client = TestApp::new()
        .config(config)
        .profile("dev")
        .with_db(control_pool(dir))
        .with_fleet(fleet.clone())
        .plugin(plugin)
        .build();
    Host { fleet, client }
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

/// The runs of `workflow` in the Harvest file, read only.
fn runs(file: &Path, workflow: &str) -> Vec<ExecutionSummary> {
    let reader = SqliteReader::open(file).expect("reader");
    reader
        .list_executions(&ExecutionFilter::new().workflow_name(workflow.to_owned()))
        .expect("list")
        .executions
}

fn state(run: &ExecutionSummary) -> String {
    run.state.clone().into_value().unwrap_or_default()
}

fn workflow_id(run: &ExecutionSummary) -> String {
    run.workflow_id.clone().into_value().unwrap_or_default()
}

fn harvest_fleet(host: &Host) -> HarvestFleet {
    host.client
        .state()
        .extension::<HarvestFleet>()
        .map(|fleet| (*fleet).clone())
        .expect("the plugin installs HarvestFleet")
}

/// Wait until `done`, or fail after `within`.
async fn eventually(within: Duration, what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Close `tenant`, then wait until the fleet and the plugin both see it
/// closed.
async fn close(host: &Host, tenant: &str) {
    let key = key(&host.fleet, tenant);
    host.fleet.close(&key).await;
    let harvest = harvest_fleet(host);
    eventually(Duration::from_secs(10), "the tenant closes", || {
        host.fleet.get_open(&key).is_none() && harvest.open_tenants().is_empty()
    })
    .await;
}

async fn start(client: &TestClient, tenant: &str, workflow: &str, input: Value) -> String {
    let response = client
        .post(&format!(
            "/harvest/tenants/{tenant}/workflows/{workflow}/start"
        ))
        .json(&json!({ "input": input }))
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

/// Wait until the timer of `exec` is armed. The tenant is open, so the read
/// goes through the API.
async fn armed(client: &TestClient, tenant: &str, exec: &str) {
    let path = format!("/harvest/tenants/{tenant}/workflows/{exec}/history");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client.get(&path).send().await;
        if response.status.is_success() && response.text().contains("TimerStarted") {
            return;
        }
        assert!(Instant::now() < deadline, "the timer never armed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A tenant with a timer 5 s out is closed, and no request opens it. The
/// timer fires within one sweep of its deadline. The sweep then closes the
/// tenant again.
#[tokio::test(flavor = "multi_thread")]
async fn a_closed_tenant_wakes_for_its_timer() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("fleet");
    let host = boot(dir.path(), "a", fleet_at(&root));
    drop(
        host.fleet
            .provision(&key(&host.fleet, "acme"))
            .await
            .expect("provision"),
    );
    let exec = start(&host.client, "acme", "napper", json!(5)).await;
    armed(&host.client, "acme", &exec).await;
    close(&host, "acme").await;

    let file = harvest_file(&host.fleet, "acme");
    let run = || runs(&file, "napper").into_iter().next().expect("the run");
    assert_eq!(state(&run()), "RUNNING", "nothing ran while closed");
    let started = run().started_at.into_value().expect("started_at");
    let due = started + chrono::Duration::seconds(5);

    eventually(
        Duration::from_secs(5) + TICK + SLACK * 4,
        "the timer fires",
        || state(&run()) == "COMPLETED",
    )
    .await;
    let closed = run().closed_at.into_value().expect("closed_at");
    assert!(closed >= due, "the timer fired early: {closed} < {due}");
    let late = (closed - due).to_std().unwrap_or_default();
    assert!(late <= TICK + SLACK, "the timer fired {late:?} late");

    // The sweep lets the fleet close the tenant again.
    let harvest = harvest_fleet(&host);
    let acme = key(&host.fleet, "acme");
    eventually(Duration::from_secs(10), "the tenant closes again", || {
        host.fleet.get_open(&acme).is_none() && harvest.open_tenants().is_empty()
    })
    .await;
    host.fleet.close_all().await;
}

/// Create a schedule of `beat` every `secs` seconds on `tenant`.
async fn schedule(client: &TestClient, tenant: &str, secs: u64) {
    let response = client
        .post(&format!(
            "/harvest/tenants/{tenant}/admin/schedules/workflow"
        ))
        .json(&json!({
            "workflow_name": "beat",
            "schedule_expr": format!("interval:{secs}"),
            "input": null,
        }))
        .send()
        .await;
    response.assert_status(201);
}

/// Each slot of the schedule ran once, and at least `slots` slots ran.
fn assert_each_slot_ran_once(file: &Path, slots: usize) {
    let runs = runs(file, "beat");
    let ids: Vec<String> = runs.iter().map(workflow_id).collect();
    let unique: HashSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "a slot ran twice: {ids:?}");
    assert!(runs.len() >= slots, "{} of {slots} slots ran", runs.len());
}

/// A schedule slot of a closed tenant runs once.
#[tokio::test(flavor = "multi_thread")]
async fn a_closed_tenant_runs_a_schedule_slot_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("fleet");
    let host = boot(dir.path(), "a", fleet_at(&root));
    drop(
        host.fleet
            .provision(&key(&host.fleet, "acme"))
            .await
            .expect("provision"),
    );
    schedule(&host.client, "acme", 2).await;
    close(&host, "acme").await;

    let file = harvest_file(&host.fleet, "acme");
    assert!(runs(&file, "beat").is_empty(), "no slot is due yet");
    eventually(Duration::from_secs(15), "two slots run", || {
        runs(&file, "beat")
            .iter()
            .filter(|run| state(run) == "COMPLETED")
            .count()
            >= 2
    })
    .await;
    assert_each_slot_ran_once(&file, 2);
    host.fleet.close_all().await;
}

/// A fleet on `root` that owns the slot range `start..=end`, through the
/// ownership table in the shared control database.
async fn owned_fleet(dir: &Path, root: &Path, owner: &str, start: u16, end: u16) -> DatabaseFleet {
    let fleet = fleet_at(root);
    let store = SqlSlotOwnership::new(control_pool(dir), Arc::new(autumn_web::time::AmbientClock));
    store.ensure_schema().await.expect("ownership schema");
    let ownership = Arc::new(
        FleetOwnership::new(
            Arc::new(store) as Arc<dyn SlotOwnershipStore>,
            OwnerId::new(owner).expect("owner"),
            vec![SlotRange::new(start, end).expect("range")],
            Duration::from_secs(600),
        )
        .expect("ownership"),
    );
    let report = fleet
        .start_ownership(ownership)
        .await
        .expect("start ownership");
    assert_eq!(report.claimed.len(), 1, "{report:?}");
    fleet
}

/// Two hosts share one control database. Each sweep reads every wake row,
/// and each host wakes only the tenants of its own range. So a schedule slot
/// of a closed tenant runs once, not once per host. The other host never
/// opens the tenant, and never removes its row.
#[tokio::test(flavor = "multi_thread")]
async fn a_host_wakes_only_the_tenants_it_owns() {
    let dir = tempfile::tempdir().unwrap();
    let root_a = dir.path().join("fleet-a");
    let root_b = dir.path().join("fleet-b");
    let probe = fleet_at(&dir.path().join("probe"));
    let slot = key(&probe, "acme").routing_slot();
    let other = (slot + 1) % 16_384;
    let host_a = boot(
        dir.path(),
        "a",
        owned_fleet(dir.path(), &root_a, "host-a", slot, slot).await,
    );
    let host_b = boot(
        dir.path(),
        "b",
        owned_fleet(dir.path(), &root_b, "host-b", other, other).await,
    );
    assert!(host_a.fleet.owns(&key(&host_a.fleet, "acme")));
    assert!(!host_b.fleet.owns(&key(&host_b.fleet, "acme")));

    drop(
        host_a
            .fleet
            .provision(&key(&host_a.fleet, "acme"))
            .await
            .expect("provision"),
    );
    schedule(&host_a.client, "acme", 2).await;
    close(&host_a, "acme").await;

    let file = harvest_file(&host_a.fleet, "acme");
    eventually(Duration::from_secs(15), "two slots run", || {
        runs(&file, "beat")
            .iter()
            .filter(|run| state(run) == "COMPLETED")
            .count()
            >= 2
    })
    .await;
    assert_each_slot_ran_once(&file, 2);
    // Host B never opened the tenant. It has no file of it.
    assert!(!harvest_file(&host_b.fleet, "acme").exists());
    assert!(!host_b.fleet.path_of(&key(&host_b.fleet, "acme")).exists());
    host_a.fleet.close_all().await;
    host_b.fleet.close_all().await;
}
