//! Stale-database sweep for the e2e benchmark harness (issue #1288).
//!
//! `ShardCluster::teardown` drops the databases a run created. Every
//! ordinary and error return calls it. A panic, or a Ctrl-C, skips it. That
//! matters because dropping a database is async, so `ShardCluster` cannot
//! have a useful `Drop`. This suite proves the provisioning-time sweep
//! reclaims what a panicked run left behind, and leaves everything else
//! alone. Mirrors `claim_budget_tests.rs`'s own sweep suite for the claim
//! harness.
//!
//! Existing-server mode only (`HARVEST_TEST_DATABASE_URL`), matching its
//! sibling: the testcontainer path gets a private server per process and has
//! nothing to sweep.

#![cfg(feature = "db")]

use diesel::QueryableByName;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use super::claim_bench_support::{SweepStep, db_name_from_url};
use super::e2e_bench_support::db;
use super::e2e_bench_support::{E2E_DB_PREFIX, sweep_step};

#[derive(QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn exists(admin: &mut AsyncPgConnection, datname: &str) -> bool {
    diesel::sql_query("SELECT count(*) AS n FROM pg_database WHERE datname = $1")
        .bind::<diesel::sql_types::Text, _>(datname.to_string())
        .get_result::<ExistsRow>(admin)
        .await
        .expect("pg_database is always readable")
        .n
        > 0
}

fn shard_db_name(cluster: &db::ShardCluster) -> String {
    let url = cluster
        .urls
        .values()
        .next()
        .expect("setup_shards(1) makes one shard");
    db_name_from_url(url).expect("a shard url always carries a database path")
}

/// Wait until the server has actually noticed a dropped connection is gone.
///
/// Dropping a Rust-side `AsyncPgConnection` closes the client socket.
/// PostgreSQL is not guaranteed to remove that backend from
/// `pg_stat_activity` in the same instant, especially on a busy server. A
/// test that drops a lease and immediately re-triggers the sweep can
/// therefore race the server's own cleanup. It could see a
/// stale-but-still-listed backend, and wrongly skip a database that is
/// genuinely abandoned. Bounded, so a server that never clears the backend
/// fails loudly
/// instead of hanging the suite.
async fn wait_for_connections_to_clear(admin: &mut AsyncPgConnection, datname: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while super::claim_bench_support::db::database_has_connections(admin, datname).await {
        assert!(
            std::time::Instant::now() < deadline,
            "{datname} still shows a backend 10s after its lease was dropped"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A panicked run's shard database is reclaimed by a later setup.
///
/// Dropping a `ShardCluster` without calling `teardown` is exactly what a
/// panic leaves behind. The database still exists. Its lease connection is
/// gone, so it looks abandoned to the next setup's sweep.
#[tokio::test]
async fn a_panicked_runs_shard_database_is_reclaimed_by_the_next_setup() {
    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_panicked_runs_shard_database_is_reclaimed_by_the_next_setup: \
             existing-server mode only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!(
            "SKIP a_panicked_runs_shard_database_is_reclaimed_by_the_next_setup: admin connect"
        );
        return;
    };

    let Ok(first) = Box::pin(db::setup_shards(1)).await else {
        eprintln!("SKIP a_panicked_runs_shard_database_is_reclaimed_by_the_next_setup: no shard");
        return;
    };
    let stale_name = shard_db_name(&first);

    // Simulate a panic: drop the cluster without teardown. The database
    // still exists; only its lease connection is gone.
    drop(first);

    assert!(
        exists(&mut admin, &stale_name).await,
        "the database must still exist right after drop; this test is about \
         reclaiming it later, not about drop deleting it"
    );
    // The sweep's own liveness check is exactly `database_has_connections`.
    // Waiting for that same signal here is what makes the next assertion
    // test the sweep, not a race against the server's own cleanup.
    wait_for_connections_to_clear(&mut admin, &stale_name).await;

    let Ok(second) = Box::pin(db::setup_shards(1)).await else {
        eprintln!("SKIP a_panicked_runs_shard_database_is_reclaimed_by_the_next_setup: no shard");
        return;
    };
    assert!(
        !exists(&mut admin, &stale_name).await,
        "{stale_name} survived a later setup: a panicked run's shard database is \
         never reclaimed, so it accumulates on every crash"
    );
    let failures = second.teardown().await;
    assert!(failures.is_empty(), "teardown failures: {failures:?}");
}

/// A cluster still in use survives a concurrent setup's sweep.
///
/// The sweep only asks the server whether anything holds a backend against a
/// candidate database. A live `ShardCluster` holds one lease connection per
/// shard for its whole lifetime. Any backend at all means the database is
/// in use.
#[tokio::test]
async fn a_live_clusters_databases_survive_a_concurrent_setup() {
    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP a_live_clusters_databases_survive_a_concurrent_setup: existing-server mode \
             only (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!("SKIP a_live_clusters_databases_survive_a_concurrent_setup: admin connect");
        return;
    };

    let Ok(live) = Box::pin(db::setup_shards(1)).await else {
        eprintln!("SKIP a_live_clusters_databases_survive_a_concurrent_setup: no shard");
        return;
    };
    let live_name = shard_db_name(&live);

    let Ok(other) = Box::pin(db::setup_shards(1)).await else {
        eprintln!("SKIP a_live_clusters_databases_survive_a_concurrent_setup: no shard");
        return;
    };

    assert!(
        exists(&mut admin, &live_name).await,
        "a live cluster's own database was reclaimed by a concurrent setup's sweep: \
         its lease should have protected {live_name}"
    );

    let _ = other.teardown().await;
    let _ = live.teardown().await;
}

/// A database that merely shares the harness prefix is never dropped.
///
/// The sweep is the only destructive thing this harness does to a server it
/// does not own. What counts as "ours" therefore has to be the full minted
/// shape, not the prefix alone.
#[tokio::test]
async fn sweep_never_touches_a_database_it_did_not_mint() {
    let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!(
            "SKIP sweep_never_touches_a_database_it_did_not_mint: existing-server mode only \
             (HARVEST_TEST_DATABASE_URL unset)"
        );
        return;
    };
    let Ok(mut admin) = AsyncPgConnection::establish(&admin_url).await else {
        eprintln!("SKIP sweep_never_touches_a_database_it_did_not_mint: admin connect");
        return;
    };

    let decoy = format!("{E2E_DB_PREFIX}notmintable_{}", std::process::id());
    assert_eq!(
        sweep_step(&decoy),
        SweepStep::Skip,
        "test bug: {decoy} must not be a name this harness could mint"
    );

    diesel::sql_query(format!("DROP DATABASE IF EXISTS {decoy}"))
        .execute(&mut admin)
        .await
        .expect("clear any earlier failed run's decoy");
    diesel::sql_query(format!("CREATE DATABASE {decoy}"))
        .execute(&mut admin)
        .await
        .expect("create the decoy");

    let cluster = Box::pin(db::setup_shards(1)).await;

    let survived = exists(&mut admin, &decoy).await;
    diesel::sql_query(format!("DROP DATABASE IF EXISTS {decoy}"))
        .execute(&mut admin)
        .await
        .ok();

    let Ok(cluster) = cluster else {
        eprintln!("SKIP sweep_never_touches_a_database_it_did_not_mint: no shard");
        return;
    };
    assert!(
        survived,
        "{decoy} was dropped by the stale-database sweep: a database this harness \
         did not mint must never be touched"
    );
    let _ = cluster.teardown().await;
}
