#![cfg(feature = "db")]
//! The run-default override cache (issue #2387).
//!
//! A start path reads [`RunDefaultsCache`] and sends no query. A refresh loop
//! reloads it once the snapshot is older than [`RUN_DEFAULTS_TTL`]. These
//! tests measure both bounds against Postgres.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! these against a shared local cluster. Otherwise each test starts a fresh
//! testcontainers Postgres 16. On a shared database each test scrubs first.

use std::time::Duration;

use autumn_harvest::run_defaults::{
    RUN_DEFAULTS_TTL, RunDefaultChange, RunDefaultPatch, RunDefaultsCache,
    get_run_default_override, update_run_default_override,
};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const WF: &str = "run_defaults_db_wf";
const THREE_HOURS: Option<Duration> = Some(Duration::from_secs(3 * 3600));

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        diesel::sql_query("DELETE FROM harvest_run_default_overrides")
            .execute(&mut conn)
            .await
            .expect("scrub");
        return (conn, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (conn, Some(container))
}

fn set_sla(secs: u64) -> RunDefaultPatch {
    RunDefaultPatch {
        sla: Some(Some(Duration::from_secs(secs))),
        ..RunDefaultPatch::default()
    }
}

async fn change(conn: &mut AsyncPgConnection, patch: RunDefaultPatch) {
    let change = RunDefaultChange {
        workflow_name: WF,
        patch,
        clear: false,
        actor: "ops",
        reason: None,
    };
    update_run_default_override(conn, &change)
        .await
        .expect("update override");
}

/// AC6: a warm cache sends no query.
///
/// The table is renamed away, so any query fails. A fresh snapshot still
/// answers, and its refresh reports that it sent nothing.
#[tokio::test]
async fn a_warm_cache_sends_no_query() {
    let (mut conn, _container) = setup_db().await;
    change(&mut conn, set_sla(300)).await;
    let cache = RunDefaultsCache::new();
    assert!(cache.refresh_if_stale(&mut conn).await.expect("cold load"));

    conn.batch_execute(
        "ALTER TABLE harvest_run_default_overrides RENAME TO harvest_run_default_overrides_away",
    )
    .await
    .expect("rename away");
    let warm = cache.refresh_if_stale(&mut conn).await;
    let (_, sla) = cache.apply(WF, None, THREE_HOURS);
    conn.batch_execute(
        "ALTER TABLE harvest_run_default_overrides_away RENAME TO harvest_run_default_overrides",
    )
    .await
    .expect("rename back");

    assert!(!warm.expect("a warm refresh sends no query"));
    assert_eq!(sla, Some(Duration::from_secs(300)));

    // A stale snapshot does send its query, so the rename above was a real probe.
    conn.batch_execute(
        "ALTER TABLE harvest_run_default_overrides RENAME TO harvest_run_default_overrides_away",
    )
    .await
    .expect("rename away");
    let cold = RunDefaultsCache::new().refresh_if_stale(&mut conn).await;
    conn.batch_execute(
        "ALTER TABLE harvest_run_default_overrides_away RENAME TO harvest_run_default_overrides",
    )
    .await
    .expect("rename back");
    assert!(cold.is_err(), "a cold refresh must query the table");
}

/// AC1, across processes: a change made elsewhere reaches this cache within
/// one TTL, and not before it.
#[tokio::test]
async fn another_process_sees_a_change_within_the_ttl() {
    let (mut conn, _container) = setup_db().await;
    let cache = RunDefaultsCache::new();
    cache.refresh(&mut conn).await.expect("load");
    assert_eq!(cache.apply(WF, None, THREE_HOURS).1, THREE_HOURS);

    change(&mut conn, set_sla(300)).await;
    assert!(!cache.refresh_if_stale(&mut conn).await.expect("warm"));
    assert_eq!(
        cache.apply(WF, None, THREE_HOURS).1,
        THREE_HOURS,
        "inside the TTL the old snapshot holds"
    );

    tokio::time::sleep(RUN_DEFAULTS_TTL).await;
    assert!(cache.refresh_if_stale(&mut conn).await.expect("stale"));
    assert_eq!(
        cache.apply(WF, None, THREE_HOURS).1,
        Some(Duration::from_secs(300))
    );
}

/// A patch merges over the stored override, and the last field deletes it.
#[tokio::test]
async fn a_patch_merges_and_the_last_field_deletes_the_row() {
    let (mut conn, _container) = setup_db().await;
    change(&mut conn, set_sla(300)).await;
    change(
        &mut conn,
        RunDefaultPatch {
            execution_timeout: Some(Some(Duration::from_secs(600))),
            ..RunDefaultPatch::default()
        },
    )
    .await;
    let stored = get_run_default_override(&mut conn, WF)
        .await
        .expect("read")
        .expect("row");
    assert_eq!(stored.fields.sla, Some(Duration::from_secs(300)));
    assert_eq!(
        stored.fields.execution_timeout,
        Some(Duration::from_secs(600))
    );

    change(
        &mut conn,
        RunDefaultPatch {
            execution_timeout: Some(None),
            sla: Some(None),
        },
    )
    .await;
    assert!(
        get_run_default_override(&mut conn, WF)
            .await
            .expect("read")
            .is_none()
    );
}

/// A zero duration is refused and writes nothing.
#[tokio::test]
async fn a_zero_duration_is_refused() {
    let (mut conn, _container) = setup_db().await;
    let change = RunDefaultChange {
        workflow_name: WF,
        patch: set_sla(0),
        clear: false,
        actor: "ops",
        reason: None,
    };
    let error = update_run_default_override(&mut conn, &change)
        .await
        .expect_err("zero is refused");
    assert!(error.to_string().contains("at least one second"), "{error}");
    assert!(
        get_run_default_override(&mut conn, WF)
            .await
            .expect("read")
            .is_none()
    );
}
