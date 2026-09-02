#![cfg(feature = "db")]
//! `autumn_harvest::migrate` against a live Postgres (issue #1240).
//!
//! The unit tests in `src/migrate.rs` cover the pure half (version extraction,
//! plan classification, directory reading). What can only be proven against a
//! real database is the half that matters to an operator running `harvest
//! migrate` before rolling replicas: that Harvest's schema actually lands, that
//! a second run is a no-op, that a failing migration takes its ledger row down
//! with it, and that the ledger this writes is the one Diesel and Autumn read.

use autumn_harvest::migrate::{self, MigrationScript};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── helpers ─────────────────────────────────────────────────────────────────

/// An empty Postgres — deliberately NOT pre-seeded with
/// `full_migrations_sql()`, because applying the schema is what is under test.
async fn empty_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[derive(diesel::QueryableByName)]
struct LedgerVersion {
    #[diesel(sql_type = diesel::sql_types::Text)]
    version: String,
}

#[derive(diesel::QueryableByName)]
struct Present {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    present: bool,
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

async fn ledger_versions(url: &str) -> Vec<String> {
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT version FROM __diesel_schema_migrations ORDER BY version")
        .load::<LedgerVersion>(&mut conn)
        .await
        .expect("read ledger")
        .into_iter()
        .map(|row| row.version)
        .collect()
}

async fn relation_exists(url: &str, relation: &str) -> bool {
    let mut conn = connect(url).await;
    let rows = diesel::sql_query("SELECT to_regclass($1) IS NOT NULL AS present")
        .bind::<diesel::sql_types::Text, _>(relation)
        .load::<Present>(&mut conn)
        .await
        .expect("to_regclass");
    rows.into_iter().next().is_some_and(|row| row.present)
}

/// A synthetic migration far past every real timestamp, so it always sorts last
/// and can never collide with a migration this repository adds later.
fn probe_migration(version: &str, sql: &str) -> MigrationScript {
    MigrationScript::new(format!("{version}_harvest_migrate_probe"), sql)
        .expect("probe migration name is well formed")
}

// ── tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn applies_harvests_own_schema_to_an_empty_database_and_is_idempotent() {
    let (_container, url) = empty_postgres().await;
    let scripts = migrate::embedded();

    // Planning alone must not write: a `harvest migrate status` against a
    // database an operator is only inspecting leaves no ledger behind.
    let plan = migrate::plan(&url, &scripts).await.expect("plan");
    assert!(!plan.ledger_exists, "an empty database has no ledger yet");
    assert_eq!(plan.pending.len(), scripts.len());
    assert!(plan.already_applied.is_empty());
    assert!(
        !relation_exists(&url, "__diesel_schema_migrations").await,
        "plan() must not create the ledger"
    );

    let report = migrate::apply(&url, &scripts).await.expect("apply");
    assert_eq!(report.applied.len(), scripts.len());
    assert!(report.already_applied.is_empty());
    assert!(report.unrecognized.is_empty());

    // The schema is really there, not just recorded.
    assert!(relation_exists(&url, "harvest_workflow_executions").await);
    assert!(relation_exists(&url, "harvest_events").await);

    // The ledger is Diesel's, keyed exactly the way Diesel keys it — this is
    // what makes a later `autumn migrate` (or plugin startup) skip these.
    let recorded = ledger_versions(&url).await;
    let expected: Vec<String> = scripts.iter().map(|s| s.version.clone()).collect();
    assert_eq!(recorded, expected);

    // Second run: nothing pending, nothing applied, no error.
    let plan = migrate::plan(&url, &scripts).await.expect("re-plan");
    assert!(!plan.has_pending());
    assert_eq!(plan.already_applied.len(), scripts.len());

    let report = migrate::apply(&url, &scripts).await.expect("re-apply");
    assert!(report.applied.is_empty(), "a second run must be a no-op");
    assert_eq!(report.already_applied.len(), scripts.len());
    assert_eq!(ledger_versions(&url).await, expected);
}

#[tokio::test]
async fn an_extra_set_applies_alongside_the_embedded_one() {
    // The `--include-dir` case: a set this binary does not embed (the plugin's
    // connector dead-letter table, or an application's own) rides along with
    // Harvest's and lands in the same ledger.
    let (_container, url) = empty_postgres().await;

    let mut scripts = migrate::embedded();
    scripts.push(probe_migration(
        "29990101000000",
        "CREATE TABLE harvest_migrate_probe (id INTEGER PRIMARY KEY);",
    ));

    let report = migrate::apply(&url, &scripts).await.expect("apply");
    assert_eq!(report.applied.len(), scripts.len());
    assert_eq!(
        report.applied.last().map(String::as_str),
        Some("29990101000000_harvest_migrate_probe"),
        "the extra set applies last, in version order"
    );
    assert!(relation_exists(&url, "harvest_migrate_probe").await);
    assert!(
        ledger_versions(&url)
            .await
            .contains(&"29990101000000".to_string())
    );
}

#[tokio::test]
async fn a_failing_migration_rolls_back_with_its_ledger_row() {
    let (_container, url) = empty_postgres().await;

    let good = probe_migration(
        "29990101000000",
        "CREATE TABLE harvest_migrate_probe_good (id INTEGER PRIMARY KEY);",
    );
    let bad = MigrationScript::new(
        "29990102000000_harvest_migrate_probe_bad",
        "CREATE TABLE harvest_migrate_probe_bad (id INTEGER PRIMARY KEY); \
         SELECT no_such_function_exists();",
    )
    .expect("well-formed name");

    let error = migrate::apply(&url, &[good, bad])
        .await
        .expect_err("a broken migration must fail the run");
    let message = error.to_string();
    assert!(
        message.contains("29990102000000_harvest_migrate_probe_bad"),
        "the failure must name the migration: {message}"
    );

    // The migration before it stayed applied (ordering is the reason migrations
    // are ordered); the failing one left neither its table nor its ledger row.
    assert!(relation_exists(&url, "harvest_migrate_probe_good").await);
    assert!(!relation_exists(&url, "harvest_migrate_probe_bad").await);
    let recorded = ledger_versions(&url).await;
    assert!(recorded.contains(&"29990101000000".to_string()));
    assert!(
        !recorded.contains(&"29990102000000".to_string()),
        "a rolled-back migration must not be recorded: {recorded:?}"
    );

    // The failure carries what DID apply, not just a count in its message: a
    // multi-shard deploy needs that to say which databases moved.
    assert_eq!(
        error.report.applied,
        vec!["29990101000000_harvest_migrate_probe".to_string()],
        "the partial report must name the migration that committed"
    );
    let failed = error
        .report
        .failed
        .as_ref()
        .expect("the report must name the failure");
    assert_eq!(failed.name, "29990102000000_harvest_migrate_probe_bad");
    assert!(
        failed.rolled_back,
        "a transactional failure leaves the database as it was"
    );

    // And the run is resumable: fixing the migration and re-running applies
    // only what is still pending.
    let fixed = probe_migration(
        "29990102000000",
        "CREATE TABLE harvest_migrate_probe_bad (id INTEGER PRIMARY KEY);",
    );
    let good = probe_migration(
        "29990101000000",
        "CREATE TABLE harvest_migrate_probe_good (id INTEGER PRIMARY KEY);",
    );
    let report = migrate::apply(&url, &[good, fixed])
        .await
        .expect("re-apply");
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.already_applied.len(), 1);
    assert!(relation_exists(&url, "harvest_migrate_probe_bad").await);
}

#[tokio::test]
async fn a_unique_violation_from_the_migrations_own_sql_is_a_failure_not_a_skip() {
    // "Already applied" is decided by an explicit ledger lookup under the
    // table lock, never by matching a unique violation on the transaction's
    // result. A unique violation raised by the migration's own body must not be
    // read as "someone else applied this": that would skip it and report
    // success.
    let (_container, url) = empty_postgres().await;

    let self_conflicting = probe_migration(
        "29990101000000",
        "CREATE TABLE harvest_migrate_probe (id INTEGER PRIMARY KEY); \
         INSERT INTO harvest_migrate_probe VALUES (1); \
         INSERT INTO harvest_migrate_probe VALUES (1);",
    );

    let error = migrate::apply(&url, std::slice::from_ref(&self_conflicting))
        .await
        .expect_err("a self-conflicting migration must fail the run");
    assert!(
        error
            .to_string()
            .contains("29990101000000_harvest_migrate_probe"),
        "the failure must name the migration: {error}"
    );
    assert!(!relation_exists(&url, "harvest_migrate_probe").await);
    assert!(
        !ledger_versions(&url)
            .await
            .contains(&"29990101000000".to_string()),
        "the migration never applied, so it must not be recorded"
    );
}

#[tokio::test]
async fn a_nontransactional_migration_may_create_an_index_concurrently() {
    // The `metadata.toml` case: Postgres rejects `CREATE INDEX CONCURRENTLY`
    // inside a transaction block, so wrapping this body -- as an applier that
    // ignores `run_in_transaction = false` would -- fails a migration that
    // `diesel migration run` applies happily.
    let (_container, url) = empty_postgres().await;

    let table = MigrationScript::new(
        "29990101000000_harvest_migrate_probe_table",
        "CREATE TABLE harvest_migrate_probe (id INTEGER PRIMARY KEY, c INTEGER);",
    )
    .expect("well-formed name");
    let concurrent_index = MigrationScript::with_metadata(
        "29990102000000_harvest_migrate_probe_index",
        "CREATE INDEX CONCURRENTLY idx_harvest_migrate_probe_c ON harvest_migrate_probe (c);",
        "run_in_transaction = false\n",
    )
    .expect("metadata parses");
    assert!(!concurrent_index.run_in_transaction);

    let report = migrate::apply(&url, &[table, concurrent_index])
        .await
        .expect("a non-transactional migration must apply");
    assert_eq!(report.applied.len(), 2);
    assert!(relation_exists(&url, "idx_harvest_migrate_probe_c").await);
    assert!(
        ledger_versions(&url)
            .await
            .contains(&"29990102000000".to_string()),
        "a non-transactional migration must still be recorded"
    );
}

#[tokio::test]
async fn a_failing_nontransactional_migration_is_not_recorded() {
    // Nothing rolls back without a transaction, but the version must not be
    // recorded: a ledger row for a migration that never finished is the one
    // state no later run can repair.
    let (_container, url) = empty_postgres().await;

    let broken = MigrationScript::with_metadata(
        "29990101000000_harvest_migrate_probe_broken",
        "SELECT no_such_function_exists();",
        "run_in_transaction = false\n",
    )
    .expect("metadata parses");

    let error = migrate::apply(&url, std::slice::from_ref(&broken))
        .await
        .expect_err("a broken migration must fail the run");
    let message = error.to_string();
    assert!(
        message.contains("29990101000000_harvest_migrate_probe_broken"),
        "the failure must name the migration: {message}"
    );
    assert!(
        message.contains("NOT rolled back"),
        "the failure must say the body was not rolled back: {message}"
    );
    assert!(
        !ledger_versions(&url)
            .await
            .contains(&"29990101000000".to_string()),
        "a migration that failed must not be recorded"
    );

    // Structurally, not just in the message: nothing rolled back, so the report
    // must say a change may stand that it cannot list.
    let failed = error
        .report
        .failed
        .expect("the report must name the failure");
    assert_eq!(failed.name, "29990101000000_harvest_migrate_probe_broken");
    assert!(
        !failed.rolled_back,
        "a run_in_transaction = false failure is not a rollback"
    );
}

#[tokio::test]
async fn ledger_rows_this_binary_does_not_know_are_reported_not_removed() {
    // The database is ahead of the binary: a newer deploy already migrated it.
    // Diesel ignores such rows; so do we, but an operator is told.
    let (_container, url) = empty_postgres().await;
    let scripts = migrate::embedded();
    migrate::apply(&url, &scripts).await.expect("apply");

    let mut conn = connect(&url).await;
    diesel::sql_query("INSERT INTO __diesel_schema_migrations (version) VALUES ('29990909000000')")
        .execute(&mut conn)
        .await
        .expect("insert a version from a newer build");

    let plan = migrate::plan(&url, &scripts).await.expect("plan");
    assert!(!plan.has_pending());
    assert_eq!(plan.unrecognized, vec!["29990909000000".to_string()]);

    let report = migrate::apply(&url, &scripts).await.expect("apply");
    assert_eq!(report.unrecognized, vec!["29990909000000".to_string()]);
    assert!(
        ledger_versions(&url)
            .await
            .contains(&"29990909000000".to_string()),
        "an unrecognized ledger row must never be removed"
    );
}

#[tokio::test]
async fn a_migration_body_does_not_see_its_own_version_in_the_ledger() {
    // Diesel runs a migration's body first and records its version afterwards,
    // so a body may condition its DDL on its own version being absent. Record
    // the version first and that body reads its own row through the
    // transaction's writes, skips the DDL, and still commits the version —
    // an incomplete schema permanently marked applied, from a file
    // `diesel migration run` applies correctly.
    let (_container, url) = empty_postgres().await;

    let self_inspecting = probe_migration(
        "29990102000000",
        "CREATE TABLE harvest_migrate_seen AS \
         SELECT count(*) AS n FROM __diesel_schema_migrations \
         WHERE version = '29990102000000';",
    );

    let report = migrate::apply(&url, std::slice::from_ref(&self_inspecting))
        .await
        .expect("the migration applies");
    assert_eq!(report.applied.len(), 1);

    let seen = seen_count(&url).await;
    assert_eq!(
        seen, 0,
        "the body must not observe its own version: Diesel records it after \
         the body runs, so a body that checks the ledger sees no row"
    );

    // The version is still recorded once the body has run.
    assert_eq!(
        ledger_versions(&url).await,
        vec!["29990102000000".to_string()]
    );
}

#[tokio::test]
async fn the_ledger_lock_does_not_block_another_migrators_ledger_insert() {
    // The lock this takes while running a body must not conflict with the
    // `ROW EXCLUSIVE` an INSERT takes. Diesel's order is body-then-insert with
    // no ledger lock, so a `diesel migration run` racing us takes the schema
    // object first and the ledger second — the opposite order to ours. A mode
    // that blocked its insert (`SHARE ROW EXCLUSIVE`, the obvious first choice)
    // would close that cycle: we would hold the ledger waiting on its table,
    // it would wait on the ledger to record the body it just finished, and
    // Postgres would abort one — reporting a failed migration while the other
    // migrator applied it correctly.
    //
    // Asserted against a real server's conflict table, not against the mode
    // string, so it stays true if the mode changes.
    let (_container, url) = empty_postgres().await;
    migrate::apply(&url, &[]).await.expect("ledger created");

    let mut holder = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut holder)
        .await
        .expect("begin");
    diesel::sql_query(format!(
        "LOCK TABLE __diesel_schema_migrations IN {} MODE",
        migrate::LEDGER_LOCK_MODE
    ))
    .execute(&mut holder)
    .await
    .expect("the migrator's ledger lock");

    // A second session playing Diesel's part: record a completed body. With a
    // conflicting mode this blocks until `holder` commits, and the timeout
    // fires instead.
    let mut other = connect(&url).await;
    diesel::sql_query("SET statement_timeout = '5s'")
        .execute(&mut other)
        .await
        .expect("timeout");
    diesel::sql_query("INSERT INTO __diesel_schema_migrations (version) VALUES ('29990103000000')")
        .execute(&mut other)
        .await
        .expect(
            "another migrator's ledger insert must not block on our lock: a mode \
         that conflicts with ROW EXCLUSIVE deadlocks against Diesel's own \
         body-then-insert lock order",
        );

    // The other non-conflict the mode is chosen for: `status` reads while a
    // migration is mid-flight.
    let versions: Vec<String> = ledger_versions(&url).await;
    assert!(versions.contains(&"29990103000000".to_string()));

    diesel::sql_query("ROLLBACK")
        .execute(&mut holder)
        .await
        .expect("rollback");
}

#[tokio::test]
async fn a_nontransactional_unique_violation_that_records_nothing_is_a_failure() {
    // With no transaction the body has already run when the ledger insert is
    // attempted, so a unique violation there is normally a concurrent migrator
    // having recorded this version first — both ran the DDL, and the run is
    // honestly reported as applied.
    //
    // But this module is pointed at ledgers it did not create. One may carry an
    // extra constraint or an insert trigger that raises `unique_violation`
    // while recording nothing. Reading that as "someone else recorded it" would
    // exit 0 over a schema change no ledger row covers, and the next run would
    // replay a body that cannot be rolled back.
    let (_container, url) = empty_postgres().await;
    migrate::apply(&url, &[]).await.expect("ledger created");

    // A pre-existing ledger's own guard: raises the same SQLSTATE the primary
    // key would, and records nothing.
    let mut conn = connect(&url).await;
    conn.batch_execute(
        "CREATE FUNCTION harvest_ledger_guard() RETURNS trigger AS $$ \
         BEGIN RAISE EXCEPTION 'ledger is guarded' USING ERRCODE = 'unique_violation'; END; \
         $$ LANGUAGE plpgsql; \
         CREATE TRIGGER harvest_ledger_guard_trigger \
         BEFORE INSERT ON __diesel_schema_migrations \
         FOR EACH ROW EXECUTE FUNCTION harvest_ledger_guard();",
    )
    .await
    .expect("install the ledger guard");

    let nontransactional = MigrationScript::with_metadata(
        "29990104000000_harvest_migrate_probe_guarded",
        "CREATE TABLE harvest_migrate_guarded (id INTEGER PRIMARY KEY);",
        "run_in_transaction = false\n",
    )
    .expect("metadata parses");
    assert!(!nontransactional.run_in_transaction);

    let error = migrate::apply(&url, &[nontransactional])
        .await
        .expect_err("a violation that records nothing must fail the run");
    assert!(
        error.report.applied.is_empty(),
        "nothing may be reported as applied: {:?}",
        error.report.applied
    );
    assert!(
        error.report.applied_concurrently.is_empty(),
        "a violation that recorded no row is not another migrator's work: {:?}",
        error.report.applied_concurrently
    );

    // The state the operator is left to inspect, and which the error must not
    // paper over: the body ran, the version is unrecorded.
    assert!(relation_exists(&url, "harvest_migrate_guarded").await);
    assert!(
        !ledger_versions(&url)
            .await
            .contains(&"29990104000000".to_string())
    );
}

/// The single count the ledger-visibility migration recorded.
async fn seen_count(url: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Seen {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    let mut conn = connect(url).await;
    diesel::sql_query("SELECT n FROM harvest_migrate_seen")
        .load::<Seen>(&mut conn)
        .await
        .expect("read the recorded count")
        .into_iter()
        .next()
        .expect("one row")
        .n
}
