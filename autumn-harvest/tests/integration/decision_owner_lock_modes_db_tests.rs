//! The Postgres row-lock facts that the owner lease relies on (issue #2249).
//!
//! ADR 0009 picks the lock mode of each owner-row statement:
//!
//! * A decision commit takes the owner row `FOR KEY SHARE`.
//! * A renewal updates `lease_until` only, so it takes `FOR NO KEY UPDATE`.
//! * An acquisition takes the row `FOR UPDATE` before it bumps the epoch.
//! * A batch acquisition takes it `FOR UPDATE SKIP LOCKED` (issue #2296).
//!
//! A renewal must not wait for a commit. An acquisition must wait for every
//! commit in flight, because that wait is the drain. A batch acquisition
//! skips a row that a commit holds, so it never bumps that epoch. These tests prove each
//! rule on a real database. One test proves the trap: an `UPDATE` of the
//! epoch alone does not wait, so the explicit `FOR UPDATE` is required.
//!
//! No Harvest schema is needed. Each test makes its own probe table. Each
//! statement must touch exactly one row, so a "ran" result is never vacuous.
#![cfg(feature = "db")]

use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A commit's lock on the owner row.
const COMMIT_LOCK: &str = "SELECT epoch FROM {t} WHERE partition = 7 FOR KEY SHARE";

/// A renewal: the same holder extends its lease and changes nothing else.
const RENEW: &str = "UPDATE {t} SET lease_until = clock_timestamp() + interval '10 s' \
                     WHERE partition = 7 AND holder = 'w1'";

/// The lock an acquisition takes before it writes the new epoch.
const ACQUIRE_LOCK: &str = "SELECT epoch FROM {t} WHERE partition = 7 FOR UPDATE";

/// The lock of a batch acquisition. It skips a locked row (issue #2296).
const SKIP_ACQUIRE_LOCK: &str = "SELECT epoch FROM {t} WHERE partition = 7 FOR UPDATE SKIP LOCKED";

/// An epoch bump with no explicit row lock first.
const PLAIN_BUMP: &str = "UPDATE {t} SET epoch = epoch + 1, holder = 'w2' WHERE partition = 7";

/// A lock timeout for each session, so a wrong wait fails and does not hang.
const SESSION_LOCK_TIMEOUT: &str = "SET lock_timeout = '5s'";

/// What the second statement did while the first transaction held its lock.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// It ran at once.
    Ran,
    /// It waited for the first transaction and hit the lock timeout.
    Waited,
}

/// Prefer an operator-supplied database and fall back to testcontainers.
async fn database_url() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

async fn connect(url: &str) -> AsyncPgConnection {
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .expect("the test database must be reachable");
    conn.batch_execute(SESSION_LOCK_TIMEOUT)
        .await
        .expect("set the session lock timeout");
    conn
}

/// A probe table name unique to this test and this process.
///
/// Two checkouts can share one operator-supplied database.
fn probe_table(test: &str) -> String {
    format!("owner_probe_{test}_{}", std::process::id())
}

/// `statement` with the probe table name in place of `{t}`.
fn with_table(statement: &str, table: &str) -> String {
    statement.replace("{t}", table)
}

/// Make the probe table with one owner row: partition 7, holder `w1`.
async fn create_probe_table(conn: &mut AsyncPgConnection, table: &str) {
    conn.batch_execute(&with_table(
        "DROP TABLE IF EXISTS {t}; \
         CREATE TABLE {t} (partition INT PRIMARY KEY, holder TEXT NOT NULL, \
             epoch BIGINT NOT NULL, lease_until TIMESTAMPTZ NOT NULL); \
         INSERT INTO {t} VALUES (7, 'w1', 1, clock_timestamp() + interval '10 s')",
        table,
    ))
    .await
    .expect("create the probe table");
}

async fn drop_probe_table(conn: &mut AsyncPgConnection, table: &str) {
    conn.batch_execute(&with_table("DROP TABLE {t}", table))
        .await
        .expect("drop the probe table");
}

/// Run `statement` and check that it touched exactly the one owner row.
async fn run_on_the_row(conn: &mut AsyncPgConnection, statement: &str) -> diesel::QueryResult<()> {
    let rows = diesel::sql_query(statement).execute(conn).await?;
    assert_eq!(rows, 1, "`{statement}` must touch the one owner row");
    Ok(())
}

/// Open a transaction on `conn` and run `statement` on the owner row.
async fn begin_on_the_row(conn: &mut AsyncPgConnection, statement: &str) {
    conn.batch_execute("BEGIN")
        .await
        .expect("open the transaction");
    run_on_the_row(conn, statement)
        .await
        .unwrap_or_else(|err| panic!("`{statement}` must run: {err}"));
}

/// Open a transaction on `conn` and run `statement` under a short lock
/// timeout. The transaction stays open.
///
/// The lock timeout counts lock waits only, so a slow runner cannot turn a
/// "ran" into a "waited".
async fn contend(conn: &mut AsyncPgConnection, statement: &str) -> Outcome {
    let (outcome, rows) = contend_rows(conn, statement).await;
    if outcome == Outcome::Ran {
        assert_eq!(rows, 1, "`{statement}` must touch the one owner row");
    }
    outcome
}

/// As [`contend`], but return the number of rows that the statement
/// touched. A `SKIP LOCKED` statement can touch none.
async fn contend_rows(conn: &mut AsyncPgConnection, statement: &str) -> (Outcome, usize) {
    conn.batch_execute("BEGIN; SET LOCAL lock_timeout = '300ms'")
        .await
        .expect("open the contender transaction");
    match diesel::sql_query(statement).execute(conn).await {
        Ok(rows) => (Outcome::Ran, rows),
        Err(err) if err.to_string().contains("lock timeout") => (Outcome::Waited, 0),
        Err(err) => panic!("the contender failed for another reason: {err}"),
    }
}

/// Wait until `condition` holds in `pg_stat_activity`, or fail.
///
/// One transaction caches its first read of the view, so each pass clears
/// that snapshot.
async fn await_activity(conn: &mut AsyncPgConnection, condition: &str, what: &str) {
    conn.batch_execute(&format!(
        "DO $$ BEGIN \
           FOR i IN 1..200 LOOP \
             PERFORM pg_stat_clear_snapshot(); \
             EXIT WHEN {condition}; \
             PERFORM pg_sleep(0.05); \
           END LOOP; \
           PERFORM pg_stat_clear_snapshot(); \
           IF NOT ({condition}) THEN RAISE EXCEPTION 'timed out'; END IF; \
         END $$"
    ))
    .await
    .unwrap_or_else(|err| panic!("{what}: {err}"));
}

/// Run `held` in an open transaction, then `contender` on a second
/// connection under a short lock timeout.
///
/// `before` runs and commits between the two, on a third connection. Each
/// statement names the probe table as `{t}`.
async fn probe(test: &str, held: &str, before: Option<&str>, contender: &str) -> Outcome {
    let table = probe_table(test);
    let (url, _container) = database_url().await;
    let mut setup = connect(&url).await;
    create_probe_table(&mut setup, &table).await;

    let mut holder = connect(&url).await;
    begin_on_the_row(&mut holder, &with_table(held, &table)).await;

    if let Some(before) = before {
        run_on_the_row(&mut setup, &with_table(before, &table))
            .await
            .expect("the statement before the contender must run");
    }

    let mut contender_conn = connect(&url).await;
    let outcome = contend(&mut contender_conn, &with_table(contender, &table)).await;

    contender_conn
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the contender");
    holder
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the holder");
    drop_probe_table(&mut setup, &table).await;
    outcome
}

#[tokio::test]
async fn a_renewal_does_not_wait_for_an_in_flight_commit() {
    let outcome = probe("renew", COMMIT_LOCK, None, RENEW).await;
    assert_eq!(
        outcome,
        Outcome::Ran,
        "a renewal must not wait for a commit"
    );
}

#[tokio::test]
async fn a_commit_does_not_wait_for_an_in_flight_renewal() {
    let outcome = probe("commit", RENEW, None, COMMIT_LOCK).await;
    assert_eq!(
        outcome,
        Outcome::Ran,
        "a commit must not wait for a renewal"
    );
}

#[tokio::test]
async fn an_acquisition_waits_for_an_in_flight_commit() {
    let outcome = probe("acquire", COMMIT_LOCK, None, ACQUIRE_LOCK).await;
    assert_eq!(
        outcome,
        Outcome::Waited,
        "an acquisition must drain the commits in flight"
    );
}

#[tokio::test]
async fn an_acquisition_still_waits_after_a_renewal() {
    let outcome = probe("carry", COMMIT_LOCK, Some(RENEW), ACQUIRE_LOCK).await;
    assert_eq!(
        outcome,
        Outcome::Waited,
        "a renewal must carry the commit lock to the new row version"
    );
}

#[tokio::test]
async fn a_plain_epoch_update_does_not_wait_for_a_commit() {
    let outcome = probe("trap", COMMIT_LOCK, None, PLAIN_BUMP).await;
    assert_eq!(
        outcome,
        Outcome::Ran,
        "an epoch bump with no `FOR UPDATE` skips the drain, so ADR 0009 requires the lock"
    );
}

#[tokio::test]
async fn a_for_share_commit_blocks_a_renewal() {
    let held = "SELECT epoch FROM {t} WHERE partition = 7 FOR SHARE";
    let outcome = probe("share", held, None, RENEW).await;
    assert_eq!(
        outcome,
        Outcome::Waited,
        "a `FOR SHARE` commit would block each renewal, so ADR 0009 rejects it"
    );
}

#[tokio::test]
async fn a_scanner_style_upsert_does_not_wait_for_a_commit() {
    let upsert = "INSERT INTO {t} VALUES (7, 'w2', 1, clock_timestamp()) \
                  ON CONFLICT (partition) DO UPDATE SET holder = EXCLUDED.holder, \
                  epoch = {t}.epoch + 1";
    let outcome = probe("upsert", COMMIT_LOCK, None, upsert).await;
    assert_eq!(
        outcome,
        Outcome::Ran,
        "the scanner lease upsert skips the drain, so ADR 0009 does not copy it"
    );
}

#[tokio::test]
async fn a_commit_does_not_queue_behind_a_waiting_acquisition() {
    let table = probe_table("queue");
    let (url, _container) = database_url().await;
    let mut setup = connect(&url).await;
    create_probe_table(&mut setup, &table).await;

    let mut holder = connect(&url).await;
    begin_on_the_row(&mut holder, &with_table(COMMIT_LOCK, &table)).await;

    let mut waiter = connect(&url).await;
    let acquire = with_table(ACQUIRE_LOCK, &table);
    let acquisition = tokio::spawn(async move {
        waiter
            .batch_execute("BEGIN; SET LOCAL lock_timeout = '10s'")
            .await
            .expect("open the acquisition transaction");
        let result = run_on_the_row(&mut waiter, &acquire).await;
        (waiter, result)
    });

    await_activity(
        &mut setup,
        &format!(
            "EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' \
             AND query LIKE '%{table}%FOR UPDATE%')"
        ),
        "the acquisition must wait for the first commit",
    )
    .await;

    let mut late = connect(&url).await;
    let outcome = contend(&mut late, &with_table(COMMIT_LOCK, &table)).await;

    late.batch_execute("ROLLBACK")
        .await
        .expect("roll back the late commit");
    holder
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the first commit");
    let (mut waiter, result) = acquisition.await.expect("the acquisition task must join");
    result.expect("the acquisition must get the row once both commits end");
    waiter
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the acquisition");
    drop_probe_table(&mut setup, &table).await;

    assert_eq!(
        outcome,
        Outcome::Ran,
        "a commit that starts while an acquisition waits must pass at the old epoch"
    );
}

#[tokio::test]
async fn a_local_idle_timeout_frees_a_frozen_commit() {
    let table = probe_table("idle");
    let (url, _container) = database_url().await;
    let mut setup = connect(&url).await;
    create_probe_table(&mut setup, &table).await;

    // The holder takes the commit lock, then stops, as a frozen owner does.
    let mut holder = connect(&url).await;
    holder
        .batch_execute(&format!(
            "SET application_name = '{table}'; BEGIN; \
             SET LOCAL idle_in_transaction_session_timeout = '200ms'"
        ))
        .await
        .expect("open the frozen transaction");
    run_on_the_row(&mut holder, &with_table(COMMIT_LOCK, &table))
        .await
        .expect("the frozen commit must lock the row");

    await_activity(
        &mut setup,
        &format!("NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{table}')"),
        "the idle timeout must end the frozen session",
    )
    .await;

    let mut acquirer = connect(&url).await;
    let outcome = contend(&mut acquirer, &with_table(ACQUIRE_LOCK, &table)).await;

    acquirer
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the acquisition");
    drop(holder);
    drop_probe_table(&mut setup, &table).await;

    assert_eq!(
        outcome,
        Outcome::Ran,
        "the idle timeout must end the frozen commit and free the owner row"
    );
}

/// A batch acquisition does not wait for a commit in flight (issue #2296).
/// It skips the row, so it cannot bump the epoch under that commit. Once
/// the commit ends, the same lock gets the row.
#[tokio::test]
async fn a_skip_locked_acquisition_skips_an_in_flight_commit() {
    let table = probe_table("skip");
    let (url, _container) = database_url().await;
    let mut setup = connect(&url).await;
    create_probe_table(&mut setup, &table).await;
    let skip = with_table(SKIP_ACQUIRE_LOCK, &table);

    let mut holder = connect(&url).await;
    begin_on_the_row(&mut holder, &with_table(COMMIT_LOCK, &table)).await;
    let mut acquirer = connect(&url).await;
    let skipped = contend_rows(&mut acquirer, &skip).await;
    acquirer
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the acquisition");

    holder
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the commit");
    let after = contend_rows(&mut acquirer, &skip).await;
    acquirer
        .batch_execute("ROLLBACK")
        .await
        .expect("roll back the acquisition");
    drop_probe_table(&mut setup, &table).await;

    assert_eq!(
        skipped,
        (Outcome::Ran, 0),
        "a batch acquisition must skip a row that a commit holds, with no wait"
    );
    assert_eq!(after, (Outcome::Ran, 1), "a free row is locked");
}
