//! The tenant Harvest file ships and restores with its tenant database.
//!
//! Each test runs one or two hosts in one process. A host is a real app on
//! its own volume. All hosts share one replica directory and one slot
//! ownership store. No Postgres and no Docker run.
//!
//! ```bash
//! cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_fleet_replica
//! ```

#![cfg(feature = "sqlite")]
// The `#[workflow]` and `#[activity]` macros need an `async fn`. The
// activity macro reads an unused, underscore-named input.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::sqlite::{
    HarvestFleet, HarvestTenant, TenantReplica, tenant_harvest_replica_root,
};
use autumn_web::config::{AutumnConfig, DatabaseConfig, DatabaseFleetConfig};
use autumn_web::db::fleet::DatabaseFleet;
use autumn_web::db::fleet_ownership::{FleetOwnership, OwnerId, SlotRange, SqlSlotOwnership};
use autumn_web::db::fleet_replication::FleetReplication;
use autumn_web::fleet_layout::FleetDbKey;
use autumn_web::replication::{FileDestination, ReplicaDestination, ReplicationSettings};
use autumn_web::test::{TestApp, TestClient};
use autumn_web::time::{SystemClock, TickingClock};
use serde_json::{Value, json};

/// The lease of a slot range in the ownership store.
const TTL: Duration = Duration::from_secs(30);

/// The replication root that every host shares.
const BASE: &str = "app/test";

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

/// One host: an app on its own volume.
struct Host {
    fleet: DatabaseFleet,
    replication: Arc<FleetReplication>,
    ownership: Option<Arc<FleetOwnership>>,
    client: TestClient,
}

impl Host {
    fn harvest(&self) -> HarvestFleet {
        (*self
            .client
            .state()
            .extension::<HarvestFleet>()
            .expect("the plugin installs HarvestFleet"))
        .clone()
    }

    fn key(&self) -> FleetDbKey {
        self.fleet.key_for("acme").expect("key")
    }

    fn harvest_file(&self) -> PathBuf {
        let db = self.fleet.path_of(&self.key());
        let stem = db.file_stem().unwrap().to_string_lossy().into_owned();
        db.with_file_name(format!("{stem}.harvest.db"))
    }

    /// Ship the tenant database and its Harvest file. The first tick of a
    /// file takes its base snapshot, and the next one ships the WAL.
    async fn ship(&self) {
        let replication = Arc::clone(&self.replication);
        let harvest = self.harvest();
        tokio::task::spawn_blocking(move || {
            for _ in 0..2 {
                replication.tick_all();
                harvest.tick_replicas();
            }
        })
        .await
        .expect("ship task");
    }
}

fn settings() -> ReplicationSettings {
    ReplicationSettings {
        database_path: PathBuf::new(),
        root: BASE.to_owned(),
        sync_interval: Duration::from_secs(3600),
        snapshot_interval: Duration::from_secs(3600),
        max_wal_bytes: 4 * 1024 * 1024,
        retention: Duration::from_hours(24),
        verify_interval: None,
    }
}

fn destination(dir: &Path) -> Arc<dyn ReplicaDestination> {
    Arc::new(FileDestination::new(dir.join("replicas")).expect("destination"))
}

fn sqlite_pool(
    path: &Path,
) -> diesel_async::pooled_connection::deadpool::Pool<autumn_web::db::RuntimeConnection> {
    autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(format!("sqlite://{}", path.display())),
        ..DatabaseConfig::default()
    })
    .expect("pool")
    .expect("a url gives a pool")
}

/// The options of one host.
struct Options<'a> {
    volume: &'a str,
    owner: Option<&'a str>,
    retention_after_delete: Duration,
}

impl<'a> Options<'a> {
    const fn owned(volume: &'a str) -> Self {
        Self {
            volume,
            owner: Some(volume),
            retention_after_delete: Duration::ZERO,
        }
    }
}

async fn host(dir: &Path, clock: &TickingClock, options: &Options<'_>) -> Host {
    let config: DatabaseFleetConfig = serde_json::from_value(json!({
        "mode": "tenant",
        "root": dir.join(options.volume).join("fleet").display().to_string(),
        "max_open": 8,
        "pool_size": 2,
        "create_on_demand": true,
        "idle_close_secs": 0,
        "restore_missing": true,
    }))
    .expect("fleet config");
    let fleet = DatabaseFleet::builder(config).build().expect("fleet");
    let replication = Arc::new(
        FleetReplication::new(
            destination(dir),
            settings(),
            Duration::from_secs(30),
            true,
            Arc::new(SystemClock),
        )
        .with_retention_after_delete(options.retention_after_delete),
    );
    assert!(fleet.install_replication(Arc::clone(&replication)).is_ok());
    let ownership = match options.owner {
        Some(owner) => {
            let store = SqlSlotOwnership::new(
                sqlite_pool(&dir.join("ownership.db")),
                Arc::new(clock.clone()),
            );
            store.ensure_schema().await.expect("ownership schema");
            let ownership = Arc::new(
                FleetOwnership::new(
                    Arc::new(store),
                    OwnerId::new(owner).expect("owner"),
                    vec![SlotRange::ALL],
                    TTL,
                )
                .expect("ownership"),
            );
            fleet
                .start_ownership(Arc::clone(&ownership))
                .await
                .expect("start ownership");
            Some(ownership)
        }
        None => None,
    };
    let control = dir.join(options.volume).join("control.db");
    std::fs::create_dir_all(control.parent().unwrap()).unwrap();
    let mut app = AutumnConfig::default();
    app.database.url = Some(format!("sqlite://{}", control.display()));
    let plugin = HarvestPlugin::new()
        .workflows(workflows![tenant_flow])
        .activities(activities![whoami])
        .sqlite_per_tenant()
        .sqlite_tenant_replica(TenantReplica::new(destination(dir), settings()))
        .api("/harvest");
    let client = TestApp::new()
        .config(app)
        .profile("dev")
        .with_db(sqlite_pool(&control))
        .with_fleet(fleet.clone())
        .plugin(plugin)
        .build();
    Host {
        fleet,
        replication,
        ownership,
        client,
    }
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

async fn start(client: &TestClient, wait: bool) -> String {
    let response = client
        .post("/harvest/tenants/acme/workflows/tenant_flow/start")
        .json(&json!({ "input": wait }))
        .send()
        .await;
    response.assert_status(201);
    let body: Value = response.json();
    body["execution_id"].as_str().unwrap().to_owned()
}

/// Wait until the run's activity completed, so it waits on its signal.
async fn waiting(client: &TestClient, exec: &str) {
    wait_for(
        client,
        &format!("/harvest/tenants/acme/workflows/{exec}/history"),
        |body| {
            body["events"]
                .as_array()
                .is_some_and(|events| events.iter().any(|e| e["type"] == "ActivityCompleted"))
        },
    )
    .await;
}

async fn signal(client: &TestClient, exec: &str) {
    client
        .post(&format!("/harvest/tenants/acme/workflows/{exec}/signal/go"))
        .json(&json!({}))
        .send()
        .await
        .assert_status(202);
}

async fn completed(client: &TestClient, exec: &str) -> Value {
    wait_for(
        client,
        &format!("/harvest/tenants/acme/workflows/{exec}"),
        |body| body["execution"]["state"] == "COMPLETED",
    )
    .await["execution"]["output"]
        .clone()
}

/// Assert that the tenant has no run `exec`.
async fn absent(client: &TestClient, exec: &str) {
    client
        .get(&format!("/harvest/tenants/acme/workflows/{exec}"))
        .send()
        .await
        .assert_status(404);
}

async fn state_of(client: &TestClient, exec: &str) -> Option<String> {
    let response = client
        .get(&format!("/harvest/tenants/acme/workflows/{exec}"))
        .send()
        .await;
    if !response.status.is_success() {
        return None;
    }
    let body: Value = response.json();
    body["execution"]["state"].as_str().map(str::to_owned)
}

/// The activity attempts in the run's history.
async fn activity_runs(client: &TestClient, exec: &str) -> usize {
    let response = client
        .get(&format!("/harvest/tenants/acme/workflows/{exec}/history"))
        .send()
        .await;
    response.assert_ok();
    let body: Value = response.json();
    body["events"].as_array().map_or(0, |events| {
        events
            .iter()
            .filter(|e| e["type"] == "ActivityCompleted")
            .count()
    })
}

/// Every object under the tenant's Harvest replica.
fn harvest_objects(dir: &Path, key: &FleetDbKey) -> Vec<String> {
    let root = tenant_harvest_replica_root(BASE, key);
    let mut objects = destination(dir).list(&format!("{root}/")).expect("list");
    objects.sort();
    objects
}

/// The owner dies mid-run. A standby claims the range, restores the tenant
/// and its Harvest file, and the run finishes from the last shipped
/// generation.
#[tokio::test(flavor = "multi_thread")]
async fn a_standby_takes_over_and_finishes_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    let harvest = a.harvest();
    drop(harvest.open(&key).await.expect("open acme on a"));

    let shipped = start(&a.client, true).await;
    waiting(&a.client, &shipped).await;
    a.ship().await;
    assert!(
        !harvest_objects(dir.path(), &key).is_empty(),
        "the Harvest file shipped under the tenant prefix"
    );
    // A run after the last ship never reaches the replica.
    let unshipped = start(&a.client, true).await;
    waiting(&a.client, &unshipped).await;

    // A stops: no close, no last ship. Its lease lapses, and B claims.
    clock.advance(TTL + Duration::from_secs(1));
    let b = host(dir.path(), &clock, &Options::owned("b")).await;
    let held = b.ownership.as_ref().unwrap().held();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].1.get(), 2, "b claims at epoch 2");

    let tenant = b.harvest().open(&key).await.expect("open acme on b");
    assert!(b.harvest_file().exists(), "b restored the Harvest file");
    drop(tenant);
    signal(&b.client, &shipped).await;
    assert_eq!(completed(&b.client, &shipped).await["who"], "tenant:acme");
    assert_eq!(
        activity_runs(&b.client, &shipped).await,
        1,
        "the activity does not run again"
    );
    // B has the last shipped generation only.
    absent(&b.client, &unshipped).await;
    assert_eq!(
        autumn_harvest_sqlite::fleet_epoch::read(&b.harvest_file()).unwrap(),
        2
    );
    a.fleet.close_all().await;
    b.fleet.close_all().await;
}

/// A host that lost the range ships nothing, not even at its close.
#[tokio::test(flavor = "multi_thread")]
async fn the_old_owner_ships_nothing_after_the_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    let tenant = a.harvest().open(&key).await.expect("open acme on a");
    let first = start(&a.client, true).await;
    waiting(&a.client, &first).await;
    a.ship().await;

    clock.advance(TTL + Duration::from_secs(1));
    let b = host(dir.path(), &clock, &Options::owned("b")).await;
    drop(b.harvest().open(&key).await.expect("open acme on b"));
    b.ship().await;
    let after_takeover = harvest_objects(dir.path(), &key);
    assert!(!after_takeover.is_empty(), "a and b both shipped");

    // A learns that it lost the range, but its tenant is still open.
    let lost = a.ownership.as_ref().unwrap().refresh().await.lost;
    assert_eq!(lost, vec![SlotRange::ALL]);
    tenant
        .start(
            "tenant_flow",
            json!(false),
            autumn_harvest_plugin::sqlite::StartOptions::new(),
        )
        .await
        .expect("a late write on a");
    a.ship().await;
    assert_eq!(
        harvest_objects(dir.path(), &key),
        after_takeover,
        "a fenced host ships nothing"
    );
    drop(tenant);
    a.fleet.close(&key).await;
    assert_eq!(
        harvest_objects(dir.path(), &key),
        after_takeover,
        "a fenced host ships nothing at its close"
    );
    b.fleet.close_all().await;
}

/// A host that gets the range back never opens its own, older Harvest file.
#[tokio::test(flavor = "multi_thread")]
async fn a_failback_never_opens_the_stale_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme on a"));
    let exec = start(&a.client, true).await;
    waiting(&a.client, &exec).await;
    a.fleet.close(&key).await;
    assert!(a.harvest_file().exists(), "a keeps its local file");

    // B takes over at epoch 2 and finishes the run.
    clock.advance(TTL + Duration::from_secs(1));
    let b = host(dir.path(), &clock, &Options::owned("b")).await;
    drop(b.harvest().open(&key).await.expect("open acme on b"));
    signal(&b.client, &exec).await;
    completed(&b.client, &exec).await;
    b.fleet.close(&key).await;
    // B hands the range back, as at its shutdown.
    assert!(b.fleet.release_ownership().await);

    // A gets the range back at a newer epoch and reopens the tenant.
    let ownership = a.ownership.as_ref().unwrap();
    let slot = key.routing_slot();
    for _ in 0..3 {
        if ownership
            .epoch_for(slot)
            .is_some_and(|epoch| epoch.get() == 3)
        {
            break;
        }
        let _ = ownership.refresh().await;
    }
    assert_eq!(
        ownership
            .epoch_for(slot)
            .map(autumn_web::lock::FencingToken::get),
        Some(3)
    );
    assert_eq!(
        autumn_harvest_sqlite::fleet_epoch::read(&a.harvest_file()).unwrap(),
        1,
        "the local file is from epoch 1"
    );
    drop(a.harvest().open(&key).await.expect("reopen acme on a"));
    assert_eq!(
        state_of(&a.client, &exec).await.as_deref(),
        Some("COMPLETED"),
        "a reads the Harvest file of epoch 2, not its own"
    );
    assert_eq!(
        autumn_harvest_sqlite::fleet_epoch::read(&a.harvest_file()).unwrap(),
        3
    );
    a.fleet.close_all().await;
}

/// A delete marks both files. A re-created tenant gets no run back.
#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_tenant_gets_no_deleted_run_back() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let options = Options {
        volume: "a",
        owner: None,
        retention_after_delete: Duration::from_secs(3600),
    };
    let a = host(dir.path(), &clock, &options).await;
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme"));
    let exec = start(&a.client, true).await;
    waiting(&a.client, &exec).await;
    a.ship().await;
    assert!(a.fleet.delete(&key).await.expect("delete"));
    assert!(!a.harvest_file().exists(), "the delete removes the file");
    assert!(
        !harvest_objects(dir.path(), &key).is_empty(),
        "the retention keeps the replica"
    );

    // A fresh volume re-creates the tenant: neither file comes back.
    let fresh = host(
        dir.path(),
        &clock,
        &Options {
            volume: "fresh",
            ..options
        },
    )
    .await;
    drop(fresh.harvest().open(&key).await.expect("re-create acme"));
    absent(&fresh.client, &exec).await;
    a.fleet.close_all().await;
    fresh.fleet.close_all().await;
}

/// A delete, then a restore of the tenant on one host: the Harvest file
/// comes back before the driver starts, and the run finishes.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_of_the_tenant_restores_its_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let options = Options {
        volume: "a",
        owner: None,
        retention_after_delete: Duration::from_secs(3600),
    };
    let a = host(dir.path(), &clock, &options).await;
    let key = a.key();
    let harvest = a.harvest();
    drop(harvest.open(&key).await.expect("open acme"));
    let exec = start(&a.client, true).await;
    waiting(&a.client, &exec).await;
    a.ship().await;
    assert!(a.fleet.delete(&key).await.expect("delete"));

    harvest.restore(&key, None).await.expect("restore acme");
    drop(harvest.open(&key).await.expect("reopen acme"));
    assert!(a.harvest_file().exists());
    signal(&a.client, &exec).await;
    assert_eq!(completed(&a.client, &exec).await["who"], "tenant:acme");
    a.fleet.close_all().await;
}

/// A fleet restore that skips `HarvestFleet` still restores the Harvest
/// file, at its latest state.
#[tokio::test(flavor = "multi_thread")]
async fn a_direct_fleet_restore_restores_the_harvest_file() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let options = Options {
        volume: "a",
        owner: None,
        retention_after_delete: Duration::from_secs(3600),
    };
    let a = host(dir.path(), &clock, &options).await;
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme"));
    let exec = start(&a.client, true).await;
    waiting(&a.client, &exec).await;
    a.ship().await;
    assert!(a.fleet.delete(&key).await.expect("delete"));

    a.fleet.restore(&key, None).await.expect("restore acme");
    drop(a.harvest().open(&key).await.expect("reopen acme"));
    signal(&a.client, &exec).await;
    completed(&a.client, &exec).await;
    a.fleet.close_all().await;
}

/// An old owner that does not know it lost the range still ships to its
/// own epoch. A restore takes the newest epoch first, so its late runs never
/// come back.
#[tokio::test(flavor = "multi_thread")]
async fn a_zombie_owner_never_wins_a_restore() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme on a"));
    let first = start(&a.client, true).await;
    waiting(&a.client, &first).await;
    a.ship().await;

    clock.advance(TTL + Duration::from_secs(1));
    let b = host(dir.path(), &clock, &Options::owned("b")).await;
    drop(b.harvest().open(&key).await.expect("open acme on b"));
    b.ship().await;

    // A never refreshed, so it still holds epoch 1 and ships.
    let late = start(&a.client, true).await;
    waiting(&a.client, &late).await;
    a.ship().await;

    // B restores its tenant from the replica again.
    b.fleet.close(&key).await;
    std::fs::remove_file(b.fleet.path_of(&key)).unwrap();
    std::fs::remove_file(b.harvest_file()).unwrap();
    drop(b.harvest().open(&key).await.expect("restore acme on b"));
    assert!(state_of(&b.client, &first).await.is_some());
    absent(&b.client, &late).await;
    a.fleet.close_all().await;
    b.fleet.close_all().await;
}

/// A restore to a point in time takes the Harvest file to the same point.
/// No delete comes first, so the open must replace the current file.
#[tokio::test(flavor = "multi_thread")]
async fn a_point_in_time_restore_takes_both_files_back() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let options = Options {
        volume: "a",
        owner: None,
        retention_after_delete: Duration::ZERO,
    };
    let a = host(dir.path(), &clock, &options).await;
    let key = a.key();
    let harvest = a.harvest();
    let (kept, later, target) = runs_around_a_target(&a).await;

    harvest
        .restore(&key, Some(target))
        .await
        .expect("restore acme to the target");
    drop(harvest.open(&key).await.expect("reopen acme"));
    absent(&a.client, &later).await;
    signal(&a.client, &kept).await;
    completed(&a.client, &kept).await;
    a.fleet.close_all().await;
}

/// Ship a run before a point in time and a run after it, then close the
/// tenant. Returns the two runs and the point in time.
async fn runs_around_a_target(a: &Host) -> (String, String, chrono::DateTime<chrono::Utc>) {
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme"));
    let kept = start(&a.client, true).await;
    waiting(&a.client, &kept).await;
    a.ship().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let target = chrono::Utc::now();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let later = start(&a.client, true).await;
    waiting(&a.client, &later).await;
    a.ship().await;
    a.fleet.close(&key).await;
    (kept, later, target)
}

/// The text of the restore target beside the Harvest file of `a`.
fn restore_target(a: &Host) -> String {
    let mut name = a.harvest_file().into_os_string();
    name.push(".restore-target");
    std::fs::read_to_string(PathBuf::from(name)).unwrap_or_default()
}

/// A direct restore after a `HarvestFleet::restore` and before an open
/// supersedes the waiting target. The Harvest file opens at its latest
/// state.
#[tokio::test(flavor = "multi_thread")]
async fn a_direct_restore_supersedes_a_waiting_target() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    let (kept, later, target) = runs_around_a_target(&a).await;

    a.harvest()
        .restore(&key, Some(target))
        .await
        .expect("restore acme to the target");
    assert!(
        restore_target(&a).starts_with("committed "),
        "the target waits"
    );
    a.fleet
        .restore(&key, None)
        .await
        .expect("restore acme to its latest state");
    drop(a.harvest().open(&key).await.expect("reopen acme"));
    assert!(state_of(&a.client, &later).await.is_some(), "latest state");
    signal(&a.client, &kept).await;
    completed(&a.client, &kept).await;
    a.fleet.close_all().await;
}

/// A direct restore that fails after its release leaves the database at
/// the target. The next open still takes the Harvest file to the target.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_direct_restore_keeps_the_waiting_target() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    let (kept, later, target) = runs_around_a_target(&a).await;

    a.harvest()
        .restore(&key, Some(target))
        .await
        .expect("restore acme to the target");
    // No replica reaches 1970. The fleet drains and releases the tenant,
    // then refuses the restore.
    let refused = a
        .fleet
        .restore(&key, Some(chrono::DateTime::UNIX_EPOCH))
        .await
        .expect_err("no replica reaches the target");
    assert!(
        matches!(refused, autumn_web::db::fleet::FleetError::Restore { .. }),
        "{refused}"
    );
    drop(a.harvest().open(&key).await.expect("reopen acme"));
    absent(&a.client, &later).await;
    signal(&a.client, &kept).await;
    completed(&a.client, &kept).await;
    a.fleet.close_all().await;
}

/// A caller that drops `HarvestFleet::restore` does not stop it. Both files
/// still go to the target.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_restore_call_still_restores_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let a = host(dir.path(), &clock, &Options::owned("a")).await;
    let key = a.key();
    let (kept, later, target) = runs_around_a_target(&a).await;

    let harvest = a.harvest();
    let call = harvest.restore(&key, Some(target));
    drop(tokio::time::timeout(Duration::from_millis(1), call).await);
    // Wait until the restore of the dropped call ends.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !restore_target(&a).starts_with("committed ") {
        assert!(Instant::now() < deadline, "the dropped restore never ended");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(harvest.open(&key).await.expect("reopen acme"));
    absent(&a.client, &later).await;
    signal(&a.client, &kept).await;
    completed(&a.client, &kept).await;
    a.fleet.close_all().await;
}

/// A delete with no retention purges the Harvest replica with the tenant.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_purges_the_harvest_replica() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TickingClock::starting_at(chrono::Utc::now());
    let options = Options {
        volume: "a",
        owner: None,
        retention_after_delete: Duration::ZERO,
    };
    let a = host(dir.path(), &clock, &options).await;
    let key = a.key();
    drop(a.harvest().open(&key).await.expect("open acme"));
    let exec = start(&a.client, true).await;
    waiting(&a.client, &exec).await;
    a.ship().await;
    assert!(
        !harvest_objects(dir.path(), &key).is_empty(),
        "the Harvest file shipped"
    );
    assert!(a.fleet.delete(&key).await.expect("delete"));
    assert_eq!(harvest_objects(dir.path(), &key), Vec::<String>::new());
    a.fleet.close_all().await;
}
