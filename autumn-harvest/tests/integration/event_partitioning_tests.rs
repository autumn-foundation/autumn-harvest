#![cfg(feature = "db")]
//! Partitioned `harvest_events` behavioral tests — issue #958.
//!
//! Drives the opt-in declarative-partitioning layout end-to-end against a real
//! Postgres and asserts, AC by AC, that:
//!
//! - AC1: the layout is opt-in (a migrated DB is unpartitioned until an
//!   operator enables it), both enable modes work, and the large-live-table
//!   migration plan is emitted with the non-blocking steps it promises.
//! - AC2: per-execution event semantics are byte-identical between layouts —
//!   `events_to_rows`' sequential ids, `load_history` / `load_history_since`
//!   ordering and delta results, and the `(workflow_exec_id, event_id)`
//!   uniqueness contract.
//! - AC3: retention reclaims via partition DROP, never row-by-row DELETE.
//! - AC4: the `HistoryArchiver` hook fires per execution before its rows
//!   become unreachable, with cursor-freeze safety preserved.
//! - AC5: legal holds and per-type overrides deterministically block
//!   reclamation of the rows they protect.
//! - AC6: the two sanctioned in-place mutation paths work identically.
//! - AC7: the event JSON at rest is byte-identical between layouts.
//! - AC8: partition maintenance is automated by the engine.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (single-threaded, each test scrubs first); otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::history_export::HistoryExportDocument;
use autumn_harvest::partition::{self, EnableMode, EnableOptions, EventLayout, SweepOptions};
use autumn_harvest::retention::{
    ArchiverFuture, HistoryArchiver, RetentionConfig, RetentionRuntime,
};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::DbPool;
use chrono::{DateTime, TimeZone, Utc};
use diesel::sql_types::{BigInt, Bool, Text, Timestamptz};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── Harness ────────────────────────────────────────────────────────────────

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

/// A live URL to a migrated Postgres, keeping the container (if any) alive.
async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

/// Reset to the pristine UNPARTITIONED layout so each test starts from the
/// same place even on a shared `HARVEST_TEST_DATABASE_URL` database.
async fn reset_to_unpartitioned(conn: &mut AsyncPgConnection) {
    partition::disable_partitioning(conn)
        .await
        .expect("revert to unpartitioned layout");
    for stmt in [
        "DELETE FROM harvest_completion_deliveries",
        "DELETE FROM harvest_dead_letters",
        "DELETE FROM harvest_execution_summaries",
        "DELETE FROM harvest_workflow_executions",
        "DELETE FROM harvest_events",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    v: String,
}

#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = Bool)]
    v: bool,
}

async fn scalar_i64(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .n
}

async fn scalar_bool(conn: &mut AsyncPgConnection, sql: &str) -> bool {
    diesel::sql_query(sql)
        .get_result::<BoolRow>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .v
}

/// `relkind` of `harvest_events`: `r` = ordinary table, `p` = partitioned.
async fn events_relkind(conn: &mut AsyncPgConnection) -> String {
    diesel::sql_query("SELECT relkind::text AS v FROM pg_class WHERE relname = 'harvest_events'")
        .get_result::<TextRow>(conn)
        .await
        .expect("relkind")
        .v
}

/// Insert an execution row with an explicit `created_at` (the cohort anchor)
/// and `completed_at`.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
) -> uuid::Uuid {
    let state = if completed_at.is_some() {
        "COMPLETED"
    } else {
        "RUNNING"
    };
    diesel::sql_query(format!(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, created_at, started_at, completed_at)
         VALUES ($1, $2, 0, '{state}', '{{}}'::jsonb, $3, $3, $4)
         RETURNING id"
    ))
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Timestamptz, _>(created_at)
    .bind::<diesel::sql_types::Nullable<Timestamptz>, _>(completed_at)
    .get_result::<IdRow>(conn)
    .await
    .expect("insert execution")
    .id
}

/// Move an execution's already-appended rows into the cohort they *would* have
/// been written into at `at`.
///
/// The cohort is the row's append instant, so a test cannot conjure an old
/// cohort by back-dating the execution. Updating the partition key moves the
/// rows between partitions (Postgres 11+), which is a faithful stand-in for
/// "these events were appended N days ago" without waiting N days. Ensures the
/// destination partition exists first, exactly as engine maintenance would have
/// when the rows were really written.
///
/// Layout-agnostic: `ensure_cohort` is a no-op on the unpartitioned layout and
/// the column exists there too, so the same helper serves both.
async fn backdate_events(conn: &mut AsyncPgConnection, exec: uuid::Uuid, at: DateTime<Utc>) {
    partition::ensure_cohort(conn, at)
        .await
        .expect("materialize the destination cohort");
    diesel::sql_query(
        "UPDATE harvest_events
            SET cohort = harvest_event_cohort($1), timestamp = $1
          WHERE workflow_exec_id = $2",
    )
    .bind::<Timestamptz, _>(at)
    .bind::<diesel::sql_types::Uuid, _>(exec)
    .execute(conn)
    .await
    .expect("backdate events");
}

/// Seed one terminal execution with a full history already sitting in `at`'s
/// cohort.
async fn seed_expired(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    at: DateTime<Utc>,
) -> uuid::Uuid {
    let exec = insert_execution(conn, workflow_name, workflow_id, at, Some(at)).await;
    autumn_harvest::store::append_events(conn, ExecutionId::from_uuid(exec), &sample_events(), 0)
        .await
        .expect("seed history");
    backdate_events(conn, exec, at).await;
    exec
}

fn sample_events() -> Vec<WorkflowEvent> {
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"customer": "acme", "amount": 42}),
            timestamp: day(2026, 1, 1),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "checkpoint".into(),
            details: serde_json::json!({"step": 3}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ]
}

/// `WorkflowEvent` deliberately has no `PartialEq` (it carries `serde_json`
/// payloads), so cross-layout equality is asserted on the canonical JSON — the
/// same bytes AC7 promises are unchanged.
fn as_json(events: &[WorkflowEvent]) -> Vec<serde_json::Value> {
    events
        .iter()
        .map(|e| serde_json::to_value(e).expect("event serializes"))
        .collect()
}

fn day(y: i32, m: u32, d: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, 12, 0, 0).unwrap()
}

#[derive(Default)]
struct RecordingArchiver {
    docs: Mutex<Vec<HistoryExportDocument>>,
    fail: bool,
}

impl RecordingArchiver {
    const fn failing() -> Self {
        Self {
            docs: Mutex::new(Vec::new()),
            fail: true,
        }
    }
    fn archived_event_counts(&self) -> Vec<usize> {
        self.docs
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.events.len())
            .collect()
    }
}

impl HistoryArchiver for RecordingArchiver {
    fn archive(&self, doc: &HistoryExportDocument) -> ArchiverFuture<'_> {
        let cloned = doc.clone();
        let fail = self.fail;
        self.docs.lock().unwrap().push(cloned);
        Box::pin(async move {
            if fail {
                Err("archival sink unavailable".into())
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Default)]
struct NoopMetrics;
impl MetricsRecorder for NoopMetrics {}

/// Drive one retention tick and return the shard-0 result.
async fn run_one_tick(
    pool: DbPool,
    config: RetentionConfig,
    archiver: Option<Arc<dyn HistoryArchiver>>,
) -> autumn_harvest::retention::RetentionTickResult {
    // The tick publishes its history-retention counters BEFORE partition
    // maintenance runs — a cohort is only droppable once the candidate loop has
    // collected its executions. Waiting on `ran_at` alone would therefore shut
    // the runtime down mid-sweep and make every reclamation assertion below
    // flaky. Wait for a maintenance stamp newer than this call instead, which
    // is also what makes a SECOND `run_one_tick` in the same test observe its
    // own pass rather than the previous one's.
    let started = Utc::now();
    let pools = ShardedDbPool::single(pool);
    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), archiver, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();
    let mut result = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
            && r.partition_maintenance
                .as_ref()
                .and_then(|m| m.at)
                .is_some_and(|at| at >= started)
        {
            result = Some(r.clone());
            break;
        }
    }
    runtime.shutdown();
    result.expect("retention tick did not complete partition maintenance in time")
}

// ══ AC1: opt-in layout, both enable modes, documented migration path ═══════

#[tokio::test]
async fn a_migrated_database_is_unpartitioned_until_an_operator_opts_in() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "AC1: the migration alone must leave harvest_events an ORDINARY table \
         — partitioning is opt-in, so an existing deployment is untouched"
    );
    assert_eq!(
        partition::detect_layout(&mut conn).await.expect("detect"),
        EventLayout::Unpartitioned,
        "AC1: detect_layout must report the unpartitioned layout"
    );
}

#[tokio::test]
async fn enabling_on_an_empty_table_creates_the_partitioned_layout() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let report = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable on an empty table");

    assert!(
        matches!(report.mode, EnableMode::Fresh),
        "AC1: an empty harvest_events must take the instant fresh-create path, got {:?}",
        report.mode
    );
    assert_eq!(events_relkind(&mut conn).await, "p", "AC1: now partitioned");
    assert_eq!(
        partition::detect_layout(&mut conn).await.expect("detect"),
        EventLayout::Partitioned {
            cohort_width_secs: partition::DEFAULT_COHORT_WIDTH_SECS
        },
        "AC1: detect_layout must report the enabled cohort width"
    );

    let parts = partition::list_partitions(&mut conn).await.expect("list");
    assert!(
        parts.iter().any(|p| p.is_default),
        "AC8: a DEFAULT partition must exist so an append can never fail with \
         'no partition of relation found'"
    );
    assert!(
        parts.iter().filter(|p| !p.is_default).count()
            >= partition::DEFAULT_LOOKAHEAD_COHORTS as usize,
        "AC8: the engine must pre-create the lookahead window, got {parts:?}"
    );
}

#[tokio::test]
async fn enabling_on_a_populated_table_preserves_every_existing_event() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "legacy_wf", "legacy-1", day(2026, 1, 5), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("seed legacy history");
    let before = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("load before");

    let report = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable on a populated table");
    assert!(
        matches!(report.mode, EnableMode::AttachLegacy { .. }),
        "AC1: a populated table must take the attach-legacy path, got {:?}",
        report.mode
    );

    let after = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("load after");
    assert_eq!(
        as_json(&before.events),
        as_json(&after.events),
        "AC1/AC2: converting the layout must not change a single stored event"
    );
    assert!(
        partition::list_partitions(&mut conn)
            .await
            .expect("list")
            .iter()
            .any(|p| p.name == partition::LEGACY_PARTITION),
        "AC1: pre-cutover rows must live in the attached legacy partition"
    );
}

#[test]
fn the_migration_plan_documents_a_non_blocking_window_for_large_live_tables() {
    let plan = partition::migration_plan(&EnableOptions::default(), day(2026, 8, 31));
    for needle in [
        "CREATE UNIQUE INDEX CONCURRENTLY",
        "NOT VALID",
        "VALIDATE CONSTRAINT",
        "ATTACH PARTITION",
        "lock_timeout",
    ] {
        assert!(
            plan.contains(needle),
            "AC1: the large-live-table plan must contain `{needle}` — that is \
             what keeps the migration window from being a table-long \
             ACCESS EXCLUSIVE outage. Plan was:\n{plan}"
        );
    }
}

#[tokio::test]
async fn a_unique_index_without_cohort_refuses_the_conversion_instead_of_aborting_mid_transaction()
{
    // Issue #1270 item 10: `capture_index_defs` replays every non-constraint
    // index verbatim onto the new parent. Postgres requires the partition
    // key in every unique index on a partitioned table. A unique index
    // that predates partitioning and does not carry `cohort` is perfectly
    // valid on the flat layout. It would make `enable_sql` fail with a raw
    // Postgres error partway through the conversion. Detect it first and
    // refuse with an explanation instead.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS uq_event_type_no_cohort_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(
        "CREATE UNIQUE INDEX uq_event_type_no_cohort_958 ON harvest_events (id, event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a unique index that does not include cohort");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a unique index missing cohort must refuse the conversion, not \
             abort partway through it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("uq_event_type_no_cohort_958"),
        "the refusal must name the offending index; got {msg}"
    );
    assert!(
        msg.contains("cohort"),
        "the refusal must explain the partition-key requirement; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP INDEX uq_event_type_no_cohort_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending index");
}

#[tokio::test]
async fn a_unique_index_with_cohort_only_as_an_include_column_still_refuses() {
    // Review finding on item 10: `indkey` holds both key columns and
    // INCLUDE columns. Checking membership across the whole array let an
    // index with `cohort` only INCLUDEd -- not a key column -- pass the
    // guard. An INCLUDE column does not participate in uniqueness, so
    // Postgres still rejects the index once the parent is partitioned.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS uq_include_cohort_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(
        "CREATE UNIQUE INDEX uq_include_cohort_958 ON harvest_events (id) INCLUDE (cohort)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a unique index with cohort only as an INCLUDE column");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an INCLUDE-only cohort column must still refuse the conversion, \
             not be mistaken for a real key column",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("uq_include_cohort_958"),
        "the refusal must name the offending index; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP INDEX uq_include_cohort_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending index");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_a_unique_index_without_cohort() {
    // Same guard, the scripted path. `migration_plan_steps` cannot call
    // `enable_partitioning`'s Rust check, so it carries its own phase-1 `DO`
    // block making the identical refusal.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS uq_plan_no_cohort_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query("CREATE UNIQUE INDEX uq_plan_no_cohort_958 ON harvest_events (id)")
        .execute(&mut conn)
        .await
        .expect("seed a unique index that does not include cohort");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect("phase 1 of the plan must refuse a unique index missing cohort");
    // Not just "some phase-1 step failed". Name the offending index. A
    // regression that fails phase 1 for the WRONG reason must not pass
    // this test by accident.
    assert!(
        msg.contains("uq_plan_no_cohort_958"),
        "the refusal must name the offending index; got {msg}"
    );

    diesel::sql_query("DROP INDEX uq_plan_no_cohort_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending index");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_an_include_only_cohort_column() {
    // Same guard, the scripted path. Its phase-1 `DO` block had the
    // identical `indkey`-membership gap: an INCLUDEd `cohort` is not a
    // key column, so it does not satisfy the requirement.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS uq_plan_include_cohort_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(
        "CREATE UNIQUE INDEX uq_plan_include_cohort_958 ON harvest_events (id) INCLUDE (cohort)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a unique index with cohort only as an INCLUDE column");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 1 of the plan must refuse an INCLUDE-only cohort column, \
         not mistake it for a real key column",
    );
    assert!(
        msg.contains("uq_plan_include_cohort_958"),
        "the refusal must name the offending index; got {msg}"
    );

    diesel::sql_query("DROP INDEX uq_plan_include_cohort_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending index");
}

#[tokio::test]
async fn an_operators_constraint_backed_unique_index_without_cohort_still_refuses() {
    // Review finding: the guard exempted ANY constraint-backed index, not
    // just the enable script's own two. `capture_index_defs` separately
    // skips every constraint-backed index on the same assumption. An
    // operator's own `UNIQUE` constraint fell through both checks
    // unnoticed. It survived only on the legacy partition afterward,
    // silently weaker than before.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_operator_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT uq_operator_event_type_958 \
         UNIQUE (id, event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator's own constraint-backed unique index missing cohort");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a constraint-backed unique index must refuse the conversion just like an \
             ordinary one, not be exempted for merely being constraint-backed",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("uq_operator_event_type_958"),
        "the refusal must name the offending index; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT uq_operator_event_type_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_a_constraint_backed_unique_index() {
    // Same guard, the scripted path. Its phase-1 `DO` block had the
    // identical any-constraint exemption.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_plan_operator_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT uq_plan_operator_event_type_958 \
         UNIQUE (id, event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator's own constraint-backed unique index missing cohort");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 1 of the plan must refuse a constraint-backed unique index missing cohort, \
         not exempt it for merely being constraint-backed",
    );
    assert!(
        msg.contains("uq_plan_operator_event_type_958"),
        "the refusal must name the offending index; got {msg}"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT uq_plan_operator_event_type_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn a_compatible_constraint_backed_unique_index_still_refuses() {
    // Review finding: a constraint-backed unique index that DOES include
    // `cohort` passed the missing-cohort check. `capture_index_defs`
    // still excludes every constraint-backed index unconditionally,
    // though, and only harvest's own two are ever recreated. The
    // operator's own otherwise-compatible constraint was silently
    // dropped during conversion. There is no support for replaying an
    // arbitrary constraint, so refuse instead of silently losing it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_compatible_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT uq_compatible_event_type_958 \
         UNIQUE (id, cohort)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator's own constraint-backed unique index that includes cohort");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a compatible constraint-backed unique index must still refuse the \
             conversion -- there is no support for replaying it, so succeeding \
             would silently drop it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("uq_compatible_event_type_958"),
        "the refusal must name the offending index; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT uq_compatible_event_type_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_a_compatible_constraint_backed_index() {
    // Same guard, the scripted path.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_plan_compatible_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT uq_plan_compatible_event_type_958 \
         UNIQUE (id, cohort)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator's own constraint-backed unique index that includes cohort");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 1 of the plan must refuse a compatible constraint-backed unique index \
         too, not exempt it for including cohort",
    );
    assert!(
        msg.contains("uq_plan_compatible_event_type_958"),
        "the refusal must name the offending index; got {msg}"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT uq_plan_compatible_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("drop the offending constraint");
}

#[tokio::test]
async fn an_impostor_reusing_harvests_own_constraint_name_still_refuses() {
    // Review finding: the exemption for harvest's own two constraints
    // matched by NAME alone. An operator could drop
    // `harvest_events_workflow_exec_id_event_id_key` and replace it with
    // a constraint of their own under that exact conventional name but a
    // different shape. Name-only matching would treat it as harvest-owned.
    // It would exclude its real index from replay. The hard-coded `ADD
    // CONSTRAINT` step would then recreate HARVEST's own shape under
    // that name instead, silently discarding the operator's actual
    // uniqueness guarantee.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         IF EXISTS harvest_events_workflow_exec_id_event_id_key",
    )
    .execute(&mut conn)
    .await
    .expect("drop the real constraint to make room for the impostor");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key UNIQUE (id, event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an impostor reusing harvest's own conventional constraint name");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an impostor reusing harvest's own constraint name but a different shape \
             must still refuse -- name alone must not exempt it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_workflow_exec_id_event_id_key"),
        "the refusal must name the offending index; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key",
    )
    .execute(&mut conn)
    .await
    .expect("drop the impostor constraint");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key UNIQUE (workflow_exec_id, event_id)",
    )
    .execute(&mut conn)
    .await
    .expect("restore harvest's own real constraint for later tests");
}

#[tokio::test]
async fn a_deferrable_impostor_of_harvests_own_constraint_still_refuses() {
    // Review finding: `contype` and the key columns are not the whole
    // shape. Harvest's own constraint is a plain, immediate one -- never
    // `DEFERRABLE`. An operator's replacement with the exact same name,
    // type and columns, but made `DEFERRABLE`, still passed a check that
    // verified only type and columns. It would be exempted. Its real
    // (deferrable) definition would then be excluded from replay.
    // Conversion would silently swap it for an immediate constraint,
    // breaking any transaction that relied on deferring the uniqueness
    // check.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         IF EXISTS harvest_events_workflow_exec_id_event_id_key",
    )
    .execute(&mut conn)
    .await
    .expect("drop the real constraint to make room for the deferrable impostor");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key UNIQUE (workflow_exec_id, event_id) \
         DEFERRABLE",
    )
    .execute(&mut conn)
    .await
    .expect("seed a deferrable impostor with harvest's own name, type and columns");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a deferrable impostor of harvest's own constraint must still refuse -- \
             matching name, type and columns must not be enough",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_workflow_exec_id_event_id_key"),
        "the refusal must name the offending index; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key",
    )
    .execute(&mut conn)
    .await
    .expect("drop the deferrable impostor constraint");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT \
         harvest_events_workflow_exec_id_event_id_key UNIQUE (workflow_exec_id, event_id)",
    )
    .execute(&mut conn)
    .await
    .expect("restore harvest's own real constraint for later tests");
}

#[tokio::test]
async fn enable_survives_a_rename_target_collision_on_the_built_in_pkey() {
    // Review finding: the bounded-rename loop discards the actual name
    // `bounded_rename_fn` returns and a later step drops a hard-coded
    // `harvest_events_pkey__pre958` instead. That guess is right only
    // when the disambiguation candidate was free. An operator's own
    // constraint already bearing that exact conventional name forces
    // `bounded_rename_fn` to disambiguate the real renamed pkey to
    // `harvest_events_pkey_1__pre958` instead. The hard-coded DROP then
    // removes the OPERATOR's constraint, leaving the real old primary
    // key in place. `ATTACH PARTITION` later fails with more than one
    // primary key on the leaf.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(
        &mut conn,
        "pkey_collide_wf",
        "pkey-collide-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    // The decoy: an operator's own constraint occupying the exact name
    // `bounded_rename_fn` would otherwise pick for the real pkey. Its name
    // already ends in the rename suffix. The rename loop's own
    // `right(conname, ...) <> suffix` filter therefore leaves it
    // untouched, exactly like a genuine leftover from an unrelated prior
    // run would.
    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         IF EXISTS harvest_events_pkey__pre958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray decoy from a previous run");
    // An EXCLUDE constraint, deliberately. A UNIQUE decoy (with or
    // without `cohort`) is refused by `refuse_if_unique_index_without_cohort`.
    // A CHECK decoy is refused by `refuse_if_unreplayable_constraints`.
    // Both would trip before conversion ever reaches the rename step,
    // testing one of those guards instead of this one. A singleton-range
    // exclusion on `id` is a real `pg_constraint` row neither guard
    // inspects, and it never actually excludes anything since `id` is
    // already unique.
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_pkey__pre958 \
         EXCLUDE USING gist (int8range(id, id, '[]') WITH &&)",
    )
    .execute(&mut conn)
    .await
    .expect("seed the decoy occupying the rename target");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect(
            "enable must survive a rename-target collision on the built-in pkey, \
             not attach a leaf still carrying two primary keys",
        );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "the conversion must succeed despite the collision"
    );

    let pkey = partition::LEGACY_PARTITION;
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint \
                 WHERE conrelid = '{pkey}'::regclass \
                 AND conname = 'harvest_events_pkey__pre958' AND contype = 'x') AS v"
            ),
        )
        .await,
        "the operator's own decoy constraint must survive untouched -- it is not \
         harvest's to drop"
    );
    // Postgres itself gives the leaf its OWN primary key mirroring the
    // parent's (id, cohort) shape the moment `ATTACH PARTITION` succeeds.
    // It auto-names that `{pkey}_pkey` by its own convention, not by
    // anything this engine names explicitly. A leftover primary key is
    // not what the bug left behind; the disambiguated rename target the
    // old hard-coded DROP never looked for is. Prove THAT is gone, under
    // whatever name `bounded_rename_fn` actually returned for it.
    assert!(
        !scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint \
                 WHERE conrelid = '{pkey}'::regclass \
                 AND conname LIKE 'harvest_events_pkey%__pre958' \
                 AND conname <> 'harvest_events_pkey__pre958') AS v"
            ),
        )
        .await,
        "the disambiguated rename target for the real primary key must have been \
         dropped, not left behind under a name the old hard-coded guess never \
         looked for"
    );
}

#[tokio::test]
async fn enable_survives_a_rename_target_collision_on_an_unrelated_relation() {
    // Review finding: `bounded_rename_fn`'s free-name check for a
    // constraint only queried `pg_constraint`, scoped to the table.
    // Renaming a primary-key or unique constraint also renames its
    // backing index, which Postgres resolves against the whole schema's
    // `pg_class`, not just this table's constraints. An unrelated
    // relation already bearing the exact conventional rename target for
    // the pkey passed the old, `pg_constraint`-only free-name check.
    // `ALTER TABLE ... RENAME CONSTRAINT` then failed outright when its
    // implicit index rename collided with that relation, aborting the
    // whole conversion.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(
        &mut conn,
        "relname_collide_wf",
        "relname-collide-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    // The decoy: an unrelated table occupying the exact name
    // `bounded_rename_fn` would otherwise pick for the renamed pkey
    // constraint (and, implicitly, its backing index). No `pg_constraint`
    // row uses this name, so the old, `pg_constraint`-only free-name
    // check reported it available.
    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_pkey__pre958")
        .execute(&mut conn)
        .await
        .expect("clear any stray decoy from a previous run");
    diesel::sql_query("CREATE TABLE harvest_events_pkey__pre958 (id int)")
        .execute(&mut conn)
        .await
        .expect("seed the decoy occupying the rename target's relation name");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect(
            "enable must survive a rename-target collision on an unrelated relation, \
             not abort the whole conversion when the implicit index rename collides",
        );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "the conversion must succeed despite the collision"
    );
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT EXISTS (SELECT 1 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() \
             AND c.relname = 'harvest_events_pkey__pre958' AND c.relkind = 'r') AS v",
        )
        .await,
        "the operator's own decoy table must survive untouched -- it is not \
         harvest's to drop"
    );

    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_pkey__pre958")
        .execute(&mut conn)
        .await
        .expect("clean up the decoy table");
}

#[tokio::test]
async fn disable_survives_a_rename_target_collision_on_an_unrelated_relation() {
    // Review finding: `bounded_rename_name`, the Rust helper
    // `disable_partitioning` uses for its own constraint renames, had the
    // identical `pg_constraint`-only gap `bounded_rename_fn` already
    // closed for the enable path. Renaming the new parent's
    // `harvest_events_pkey` constraint back onto the flat table also
    // renames its backing index, resolved schema-wide against
    // `pg_class`. An unrelated relation already at the conventional
    // target (`harvest_events_pkey__old`) passed the old,
    // `pg_constraint`-only free-name check. It then made the implicit
    // index rename fail outright, aborting the whole revert.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // The decoy: an unrelated table occupying the exact name
    // `bounded_rename_name` would otherwise pick for the renamed pkey
    // constraint (and, implicitly, its backing index). No `pg_constraint`
    // row uses this name, so the old, `pg_constraint`-only free-name
    // check reported it available.
    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_pkey__old")
        .execute(&mut conn)
        .await
        .expect("clear any stray decoy from a previous run");
    diesel::sql_query("CREATE TABLE harvest_events_pkey__old (id int)")
        .execute(&mut conn)
        .await
        .expect("seed the decoy occupying the rename target's relation name");

    partition::disable_partitioning(&mut conn)
        .await
        .expect("revert must not error")
        .expect("the shard was partitioned");
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "the revert must succeed despite the collision"
    );
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT EXISTS (SELECT 1 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() \
             AND c.relname = 'harvest_events_pkey__old' AND c.relkind = 'r') AS v",
        )
        .await,
        "the operator's own decoy table must survive untouched -- it is not \
         harvest's to drop"
    );

    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_pkey__old")
        .execute(&mut conn)
        .await
        .expect("clean up the decoy table");
}

#[tokio::test]
async fn a_dependent_view_refuses_the_conversion_instead_of_silently_going_stale() {
    // Issue #1270 item 14: Postgres tracks a view's dependency by relation
    // OID, not by name. Both conversion paths rename `harvest_events` out
    // of the way, then create the replacement under the original name. A
    // dependent view would keep pointing at the RENAMED relation. On the
    // populated path, that relation is thereafter only the pre-cutover
    // partition. The view keeps returning rows. It silently stops
    // returning any row appended after the conversion.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "view_wf", "view-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_by_type_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(
        "CREATE VIEW harvest_events_by_type_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a dependent view");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a dependent view must refuse the conversion, not silently go \
             stale after it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_by_type_958"),
        "the refusal must name the offending view; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP VIEW harvest_events_by_type_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_a_dependent_view() {
    // Same guard, the scripted path. `migration_plan_steps` cannot call
    // `enable_partitioning`'s Rust check, so it carries its own phase-1
    // `DO` block making the identical refusal.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_plan_view_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(
        "CREATE VIEW harvest_events_plan_view_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a dependent view");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg =
        refusal.expect("phase 1 of the plan must refuse a view that depends on harvest_events");
    assert!(
        msg.contains("harvest_events_plan_view_958"),
        "the refusal must name the offending view; got {msg}"
    );

    diesel::sql_query("DROP VIEW harvest_events_plan_view_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
}

#[tokio::test]
async fn a_dependent_view_refuses_the_revert_too() {
    // The reverse direction: `disable_partitioning` renames the partitioned
    // parent out of the way exactly as `enable` renames the flat table, so
    // it is exactly as vulnerable.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_disable_view_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(
        "CREATE VIEW harvest_events_disable_view_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a dependent view on the partitioned parent");

    let err = partition::disable_partitioning(&mut conn)
        .await
        .expect_err("a dependent view must refuse the revert, not silently go stale after it");
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_disable_view_958"),
        "the refusal must name the offending view; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP VIEW harvest_events_disable_view_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
}

#[tokio::test]
async fn a_view_depending_on_a_leaf_partition_directly_still_refuses_the_revert() {
    // Review finding: the dependent-view check matched only the PARENT
    // name `harvest_events`. Postgres lets a view depend directly on a
    // LEAF partition instead, queried by name like any other table. Such
    // a view used to pass this check unnoticed. `disable_partitioning`'s
    // `DROP ... CASCADE` then took the leaf, and the view with it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. That path renames
    // the flat table to `LEGACY_PARTITION` rather than dropping it.
    // The Fresh (empty-table) path never creates that name at all.
    let exec = insert_execution(
        &mut conn,
        "leaf_view_wf",
        "leaf-view-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_leaf_view_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(format!(
        "CREATE VIEW harvest_events_leaf_view_958 AS SELECT event_type FROM {}",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("seed a view depending directly on a leaf partition");

    let err = partition::disable_partitioning(&mut conn).await.expect_err(
        "a view depending directly on a leaf partition must refuse the revert too, \
         not only one depending on the parent",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_leaf_view_958"),
        "the refusal must name the offending view; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP VIEW harvest_events_leaf_view_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
}

#[tokio::test]
async fn enable_sql_rechecks_a_dependent_view_created_after_the_preflight() {
    // Review finding: `enable_partitioning`'s dependent-view check runs in
    // Rust, a separate round-trip before `enable_sql`'s script even
    // starts. That is well before the script takes ACCESS EXCLUSIVE at
    // the rename. A session that created a view in that gap would have
    // passed the Rust check clean. This test calls `enable_sql` directly,
    // bypassing that Rust check entirely. It proves the script also
    // refuses for itself once it holds the lock, rather than trusting
    // only a check made before the window opened.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. That path keeps the
    // legacy table as a partition rather than dropping it. On the empty
    // (Fresh) path, Postgres's own `DROP TABLE` already refuses over a
    // dependent view, for an unrelated reason. That would mask whether
    // this recheck is what caught it. The populated path never runs
    // that `DROP`, so this is the only guard standing between it and a
    // silently stranded view.
    let exec = insert_execution(&mut conn, "race_view_wf", "race-view-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_race_view_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(
        "CREATE VIEW harvest_events_race_view_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a view as if created in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_race_view_958"),
        "the refusal must name the offending view; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP VIEW harvest_events_race_view_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
}

#[tokio::test]
async fn enable_sql_rechecks_an_operator_trigger_installed_after_the_preflight() {
    // Same race, the trigger guard's half. Both checks run in Rust before
    // `enable_sql` starts, so both need the identical re-check once the
    // script actually holds the lock.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. That path keeps
    // the legacy table as a partition rather than dropping it. See the
    // dependent-view test's sibling comment for why the empty path
    // cannot isolate this recheck from Postgres's own protection.
    let exec = insert_execution(&mut conn, "race_trg_wf", "race-trg-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_race_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_race_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_race_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_race_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_race_trg_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger as if installed in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_race_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_race_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_race_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn enable_sql_rechecks_a_unique_index_added_after_the_preflight() {
    // Same race, the unique-index guard's half. `idx_defs` (the set
    // replayed onto the new parent) is captured before the rename takes
    // ACCESS EXCLUSIVE. That capture is a plain read that does not
    // conflict with a concurrent `ALTER TABLE ... ADD CONSTRAINT ...
    // UNIQUE`. This test calls `enable_sql` directly, bypassing the Rust
    // preflight entirely. It proves the script also refuses for itself
    // once it holds the lock, rather than trusting only a check made
    // before the window opened.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. See the dependent-view
    // test's sibling comment for why the empty path cannot isolate this
    // recheck from Postgres's own protection.
    let exec = insert_execution(&mut conn, "race_idx_wf", "race-idx-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_race_idx_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray constraint from a previous run");
    diesel::sql_query("ALTER TABLE harvest_events ADD CONSTRAINT uq_race_idx_958 UNIQUE (id)")
        .execute(&mut conn)
        .await
        .expect("seed a constraint as if added in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("uq_race_idx_958"),
        "the refusal must name the offending index; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT uq_race_idx_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn enable_sql_rechecks_an_unreplayable_constraint_installed_after_the_preflight() {
    // Same race, the CHECK/foreign-key constraint guard's half. This test
    // calls `enable_sql` directly, bypassing the Rust preflight entirely.
    // It proves the script also refuses for itself once it holds the
    // lock, rather than trusting only a check made before the window
    // opened.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. See the dependent-view
    // test's sibling comment for why the empty path cannot isolate this
    // recheck from Postgres's own protection.
    let exec = insert_execution(&mut conn, "race_con_wf", "race-con-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS harvest_events_race_check_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_race_check_958 \
         CHECK (event_type <> '')",
    )
    .execute(&mut conn)
    .await
    .expect("seed a constraint as if added in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_race_check_958"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_race_check_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn a_plain_compatible_unique_index_survives_the_direct_enable_path() {
    // Review finding: `idx_defs` is the set of index definitions replayed
    // onto the new parent. It used to be captured by a plain read with no
    // lock at all. That does not conflict with a concurrent `CREATE
    // UNIQUE INDEX ... (cohort, ...)`. Such an index is neither
    // constraint-backed nor missing the partition key. No refusal guard
    // catches it either, so a capture taken too early could simply race
    // it and miss it. The fix takes an explicit `LOCK TABLE` before
    // capturing, closing the race. It does not move the capture past the
    // rename. The capture must still name `harvest_events`, not
    // `{LEGACY_PARTITION}`, so the replay lands on the right table. This
    // proves a plain compatible index present at call time still
    // survives onto the new parent.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "plain_idx_wf", "plain-idx-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("DROP INDEX IF EXISTS uq_plain_compat_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query("CREATE UNIQUE INDEX uq_plain_compat_958 ON harvest_events (id, cohort)")
        .execute(&mut conn)
        .await
        .expect("seed a plain unique index that already includes cohort");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("a plain compatible unique index must not block conversion");

    assert!(
        scalar_bool(
            &mut conn,
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'harvest_events' AND indexname = 'uq_plain_compat_958') AS v",
        )
        .await,
        "the plain compatible index must be replayed onto the new parent, not \
         silently dropped because it was captured from the wrong relation"
    );
}

#[tokio::test]
async fn a_dependent_materialized_view_refuses_the_conversion_too() {
    // Review finding on item 14: Postgres records a materialized view's
    // dependency the same way as an ordinary view, by relation OID
    // (`pg_depend`/`pg_rewrite`, `relkind = 'm'`). The guard checked only
    // `relkind = 'v'`, so a materialized view was never caught and would
    // silently stop reflecting new events after conversion.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP MATERIALIZED VIEW IF EXISTS harvest_events_matview_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray materialized view from a previous run");
    diesel::sql_query(
        "CREATE MATERIALIZED VIEW harvest_events_matview_958 AS \
         SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a dependent materialized view");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a dependent materialized view must refuse the conversion, not \
             silently go stale after it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_matview_958"),
        "the refusal must name the offending materialized view; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP MATERIALIZED VIEW harvest_events_matview_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending materialized view");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_a_dependent_materialized_view() {
    // Same guard, the scripted path. Its phase-1 `DO` block had the
    // identical `relkind = 'v'` gap.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP MATERIALIZED VIEW IF EXISTS harvest_events_plan_matview_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray materialized view from a previous run");
    diesel::sql_query(
        "CREATE MATERIALIZED VIEW harvest_events_plan_matview_958 AS \
         SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a dependent materialized view");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 1 of the plan must refuse a materialized view that depends on harvest_events",
    );
    assert!(
        msg.contains("harvest_events_plan_matview_958"),
        "the refusal must name the offending materialized view; got {msg}"
    );

    diesel::sql_query("DROP MATERIALIZED VIEW harvest_events_plan_matview_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending materialized view");
}

#[tokio::test]
async fn list_partitions_parses_bounds_under_a_non_iso_datestyle() {
    // Issue #1270 item 15: `pg_get_expr(relpartbound, ...)` renders a
    // partition's timestamp bounds in the SESSION's `DateStyle`, not a
    // fixed format. The bound parser only accepts ISO year-first forms. A
    // connection (or a pooler that inherited a non-default setting) using a
    // different style used to parse every finite bound as `None`. Existing
    // cohorts would fail the exact-bound check. `ensure_partitions` would
    // error on every tick, and the sweeper would treat bounded partitions
    // as unbounded rather than reclaiming them.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // `DMY` alone would already misparse an ISO string's month/day; `SQL`
    // also changes the rendered separator and field order.
    diesel::sql_query("SET DateStyle = 'SQL, DMY'")
        .execute(&mut conn)
        .await
        .expect("set a non-ISO DateStyle on this session");

    let parts = partition::list_partitions(&mut conn)
        .await
        .expect("list_partitions must not error under a non-ISO DateStyle");
    let non_default: Vec<_> = parts.iter().filter(|p| !p.is_default).collect();
    assert!(
        !non_default.is_empty(),
        "precondition: enable must have created cohort partitions"
    );
    for p in &non_default {
        assert!(
            p.upper.is_some(),
            "every cohort partition must have a parsed upper bound \
             regardless of session DateStyle; got {p:?}"
        );
    }
    // The legacy partition (if this shard took that path) is the one with a
    // `None` lower bound (`MINVALUE`) — everything else must parse both
    // ends.
    for p in non_default
        .iter()
        .filter(|p| p.name != partition::LEGACY_PARTITION)
    {
        assert!(
            p.lower.is_some(),
            "a non-legacy cohort partition must have a parsed lower bound; got {p:?}"
        );
    }
}

#[tokio::test]
async fn list_partitions_restores_date_style_inside_a_caller_transaction() {
    // Review finding: `list_partitions`'s own `SET LOCAL DateStyle = 'ISO,
    // MDY'` runs inside a `conn.transaction()`. A nested `.transaction()`
    // -- one opened while `conn` is already inside a transaction -- is a
    // `SAVEPOINT` under Diesel. `RELEASE SAVEPOINT` does not undo a `SET
    // LOCAL` made inside it; only `ROLLBACK TO SAVEPOINT` does. A caller
    // that already had `conn` inside its own transaction would otherwise
    // see `DateStyle` silently pinned to `ISO, MDY` for the rest of it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    conn.transaction::<(), autumn_harvest::HarvestError, _>(async |conn| {
        diesel::sql_query("SET LOCAL DateStyle = 'SQL, DMY'")
            .execute(conn)
            .await?;

        partition::list_partitions(conn).await?;

        let style = diesel::sql_query("SELECT current_setting('DateStyle') AS v")
            .get_result::<TextRow>(conn)
            .await?
            .v;
        assert_eq!(
            style, "SQL, DMY",
            "list_partitions must restore the caller's DateStyle inside its own \
             savepoint, not leave its 'ISO, MDY' override pinned for the rest of \
             the caller's transaction"
        );
        Ok(())
    })
    .await
    .expect("outer transaction");
}

#[tokio::test]
async fn an_operator_trigger_refuses_the_conversion_instead_of_silently_going_dark() {
    // Issue #1270 item 16: `CREATE TABLE ... (LIKE ...)` does not carry
    // triggers. Both conversion paths build the replacement relation that
    // way. An operator trigger (an audit trigger, say) would stay on the
    // renamed legacy table. That table receives no new rows after cutover.
    // The trigger then stops firing for every event, while still existing.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_op_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_op_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_op_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_op_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_op_trg_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an operator trigger must refuse the conversion, not silently go \
             dark after it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_op_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_op_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_op_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_an_operator_trigger() {
    // Same guard, the scripted path. `migration_plan_steps` cannot call
    // `enable_partitioning`'s Rust check, so it carries its own phase-1
    // `DO` block making the identical refusal.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_plan_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_plan_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_plan_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_plan_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_plan_trg_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg =
        refusal.expect("phase 1 of the plan must refuse an operator trigger on harvest_events");
    assert!(
        msg.contains("harvest_events_plan_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_plan_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_plan_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn an_operator_trigger_refuses_the_revert_too() {
    // The reverse direction: `disable_partitioning` rebuilds a flat table
    // with `LIKE` exactly as `enable` rebuilds the partitioned parent, so
    // it is exactly as vulnerable.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_disable_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_disable_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_disable_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_disable_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_disable_trg_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger on the partitioned parent");

    let err = partition::disable_partitioning(&mut conn)
        .await
        .expect_err("an operator trigger must refuse the revert, not silently go dark after it");
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_disable_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_disable_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_disable_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn an_operator_trigger_sharing_the_reserved_fk_trigger_name_still_refuses() {
    // Review finding: the guard used to exempt EXEC_FK_TRIGGER by NAME
    // alone. On an unpartitioned shard that name cannot yet be harvest's.
    // Only a conversion creates it. So an operator's own trigger sharing
    // the reserved name was silently exempted, then dropped without
    // warning by the LIKE-based rebuild. The guard now excludes a trigger
    // only by its FUNCTION (`harvest_events_require_execution`), which an
    // operator's trigger does not invoke even if its name collides.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_reserved_name_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_reserved_name_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_exec_fk_trg BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_reserved_name_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger reusing the reserved FK-trigger name");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a name collision with EXEC_FK_TRIGGER must not exempt an \
             operator's own trigger from the guard",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_exec_fk_trg"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_reserved_name_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn an_operators_own_trigger_calling_the_reserved_function_still_refuses() {
    // Review finding: the guard exempted `EXEC_FK_TRIGGER` by FUNCTION
    // alone, closing the name-collision gap above but opening the
    // mirror-image one. An operator's own trigger, under its own name,
    // can call `harvest_events_require_execution` too. That function is
    // part of the base migration. It is present whether or not the
    // shard ever converts. The guard now exempts a trigger only when
    // both its name and its function match harvest's own.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS operator_reuses_harvest_fn_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query(
        "CREATE TRIGGER operator_reuses_harvest_fn_trg BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger reusing harvest's own reserved function");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a trigger merely calling the reserved function, under an operator's own \
             name, must not be exempted from the guard",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("operator_reuses_harvest_fn_trg"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER operator_reuses_harvest_fn_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
}

#[tokio::test]
async fn a_trigger_matching_the_reserved_name_and_function_but_not_the_shape_still_refuses() {
    // Review finding: name and function are still not the whole identity.
    // An operator's trigger could carry harvest's exact reserved name
    // and call harvest's exact reserved function. It could still be
    // installed as `BEFORE UPDATE`, rather than `BEFORE INSERT FOR EACH
    // ROW` -- a shape neither earlier check distinguishes. The guard now
    // also requires harvest's exact trigger type, no arguments, and no
    // `WHEN` clause.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_exec_fk_trg BEFORE UPDATE ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()",
    )
    .execute(&mut conn)
    .await
    .expect(
        "seed an operator trigger matching the reserved name and function under a \
         different definition",
    );

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a trigger matching the reserved name and function, but not harvest's \
             exact shape, must not be exempted from the guard",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_exec_fk_trg"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
}

#[tokio::test]
async fn a_disabled_trigger_matching_every_other_check_still_refuses() {
    // Review finding: shape alone still omits `tgenabled`. Harvest's own
    // trigger is always the plain enabled state. An operator's trigger
    // matching the reserved name, function, and shape, but installed
    // `DISABLED`, must not be exempted -- conversion would otherwise
    // silently re-enable it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_exec_fk_trg BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger matching every check but the enabled state");
    diesel::sql_query("ALTER TABLE harvest_events DISABLE TRIGGER harvest_events_exec_fk_trg")
        .execute(&mut conn)
        .await
        .expect("disable the seeded trigger");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a trigger matching every other check, but not harvest's enabled state, \
             must not be exempted from the guard",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_exec_fk_trg"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_exec_fk_trg ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
}

#[tokio::test]
async fn an_operator_trigger_whose_function_lives_in_another_schema_still_refuses() {
    // Review finding: the fix for the reserved-name gap added
    // `p.pronamespace = c.relnamespace` to the join. An INNER JOIN with
    // that condition drops the row entirely for a trigger whose function
    // lives in a DIFFERENT schema (an `audit` schema, say). That does
    // not just fail the name check. It makes the trigger invisible to
    // the guard altogether. Only the exclusion for harvest's own
    // function needs same-schema scoping, not the join itself.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "DROP TRIGGER IF EXISTS harvest_events_other_schema_trg_958 ON harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP SCHEMA IF EXISTS harvest_other_schema_958 CASCADE")
        .execute(&mut conn)
        .await
        .expect("clear any stray schema from a previous run");
    diesel::sql_query("CREATE SCHEMA harvest_other_schema_958")
        .execute(&mut conn)
        .await
        .expect("seed a separate schema");
    diesel::sql_query(
        "CREATE FUNCTION harvest_other_schema_958.trg_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function in another schema");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_other_schema_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_other_schema_958.trg_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator trigger invoking a function in another schema");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an operator trigger whose function lives in another schema \
             must still refuse the conversion, not become invisible to \
             the guard",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_other_schema_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );

    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP TRIGGER harvest_events_other_schema_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP SCHEMA harvest_other_schema_958 CASCADE")
        .execute(&mut conn)
        .await
        .expect("drop the offending schema");
}

#[tokio::test]
async fn a_trigger_installed_directly_on_a_leaf_partition_still_refuses_the_revert() {
    // Review finding: the trigger guard matched only the PARENT name
    // `harvest_events`. Postgres lets a trigger be installed directly on a
    // LEAF partition instead of the parent. Such a trigger used to pass
    // this check unnoticed. `disable_partitioning`'s `DROP ... CASCADE`
    // then destroyed it along with the leaf, silently.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated table takes the AttachLegacy path. That path renames
    // the flat table to `LEGACY_PARTITION` rather than dropping it.
    // The Fresh (empty-table) path never creates that name at all.
    let exec = insert_execution(
        &mut conn,
        "leaf_trg_wf",
        "leaf-trg-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query(format!(
        "DROP TRIGGER IF EXISTS harvest_events_leaf_trg_958 ON {}",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_leaf_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_leaf_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(format!(
        "CREATE TRIGGER harvest_events_leaf_trg_958 BEFORE INSERT ON {} \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_leaf_trg_958_fn()",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("seed a trigger installed directly on a leaf partition");

    let err = partition::disable_partitioning(&mut conn).await.expect_err(
        "a trigger installed directly on a leaf partition must refuse the revert too, \
         not only one on the parent",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_leaf_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query(format!(
        "DROP TRIGGER harvest_events_leaf_trg_958 ON {}",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_leaf_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
}

#[tokio::test]
async fn a_check_constraint_refuses_the_conversion_instead_of_silently_dropping() {
    // Issue #1270 P2: `CREATE TABLE ... (LIKE ...)` carries neither a
    // `CHECK` nor a foreign-key constraint. An operator's own `CHECK` on
    // `harvest_events` would stay enforced on the renamed legacy table,
    // silently no longer applying to any row appended from cutover
    // onward.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS harvest_events_type_check_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_type_check_958 \
         CHECK (event_type <> '')",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator CHECK constraint");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a CHECK constraint not carried by CREATE TABLE ... (LIKE ...) must refuse \
             the conversion, not silently stop applying after it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_type_check_958"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_type_check_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn a_foreign_key_constraint_refuses_the_conversion_too() {
    // Same gap, the other constraint kind `unreplayable_constraints`
    // covers. An operator foreign key is exactly as unreplayable as an
    // operator CHECK, and just as silent about it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_fk_target_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray target table from a previous run");
    diesel::sql_query("CREATE TABLE harvest_events_fk_target_958 (event_type TEXT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("seed a foreign-key target table");
    diesel::sql_query(
        "INSERT INTO harvest_events_fk_target_958 (event_type) VALUES ('operator_type_958')",
    )
    .execute(&mut conn)
    .await
    .expect("seed a matching target row so the constraint can be added");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_fk_958 \
         FOREIGN KEY (event_type) REFERENCES harvest_events_fk_target_958(event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator foreign key");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "a foreign key not carried by CREATE TABLE ... (LIKE ...) must refuse the \
             conversion, not silently stop applying after it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_fk_958"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_fk_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
    diesel::sql_query("DROP TABLE harvest_events_fk_target_958")
        .execute(&mut conn)
        .await
        .expect("drop the target table");
}

#[tokio::test]
async fn the_large_table_plans_phase_1_also_refuses_an_unreplayable_constraint() {
    // Same guard, the scripted path. `migration_plan_steps` cannot call
    // `enable_partitioning`'s Rust check, so it carries its own phase-1
    // `DO` block making the identical refusal.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS harvest_events_plan_check_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_plan_check_958 \
         CHECK (event_type <> '')",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator CHECK constraint");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg =
        refusal.expect("phase 1 of the plan must refuse a constraint that is not carried forward");
    assert!(
        msg.contains("harvest_events_plan_check_958"),
        "the refusal must name the offending constraint; got {msg}"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_plan_check_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
}

#[tokio::test]
async fn an_unreplayable_constraint_refuses_the_revert_too() {
    // The reverse direction: `disable_partitioning` renames the
    // partitioned parent out of the way exactly as `enable` renames the
    // flat table, so it is exactly as vulnerable.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS harvest_events_disable_check_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_disable_check_958 \
         CHECK (event_type <> '')",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator CHECK constraint on the partitioned parent");

    let err = partition::disable_partitioning(&mut conn).await.expect_err(
        "a CHECK constraint must refuse the revert, not silently stop applying after it",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_disable_check_958"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_disable_check_958",
    )
    .execute(&mut conn)
    .await
    .expect("drop the offending constraint");
}

#[tokio::test]
async fn an_impostor_reusing_harvests_own_foreign_key_name_still_refuses() {
    // Review finding: name alone must not identify harvest's own foreign
    // key. An operator's own foreign key could reuse the exact reserved
    // name `harvest_events_workflow_exec_id_fkey`, on different columns
    // or a different target, and pass unnoticed. Name-only matching
    // would treat it as harvest-owned and silently drop it. That is the
    // same gap the shape check closes for the primary key and unique
    // constraint above.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_impostor_fk_target_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray target table from a previous run");
    diesel::sql_query(
        "CREATE TABLE harvest_events_impostor_fk_target_958 (event_type TEXT PRIMARY KEY)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a foreign-key target table");
    diesel::sql_query(
        "INSERT INTO harvest_events_impostor_fk_target_958 (event_type) VALUES ('impostor_958')",
    )
    .execute(&mut conn)
    .await
    .expect("seed a matching target row");

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_workflow_exec_id_fkey",
    )
    .execute(&mut conn)
    .await
    .expect("drop the real foreign key to make room for the impostor");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_workflow_exec_id_fkey \
         FOREIGN KEY (event_type) REFERENCES harvest_events_impostor_fk_target_958(event_type)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an impostor reusing harvest's own conventional foreign-key name");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an impostor reusing harvest's own foreign-key name but a different shape \
             must still refuse -- name alone must not exempt it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_workflow_exec_id_fkey"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_workflow_exec_id_fkey",
    )
    .execute(&mut conn)
    .await
    .expect("drop the impostor constraint");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_workflow_exec_id_fkey \
         FOREIGN KEY (workflow_exec_id) REFERENCES harvest_workflow_executions(id) \
         ON DELETE CASCADE",
    )
    .execute(&mut conn)
    .await
    .expect("restore harvest's own real foreign key for later tests");
    diesel::sql_query("DROP TABLE harvest_events_impostor_fk_target_958")
        .execute(&mut conn)
        .await
        .expect("drop the target table");
}

#[tokio::test]
async fn an_impostor_foreign_key_with_on_update_cascade_still_refuses() {
    // Review finding: the same columns, target and `ON DELETE CASCADE`
    // are not the whole shape either. Harvest's own foreign key relies
    // on the built-in `ON UPDATE NO ACTION` -- it never names an `ON
    // UPDATE` clause. An operator's own foreign key, reusing the
    // reserved name with the right columns, target and delete action,
    // could still add `ON UPDATE CASCADE`. That used to pass this check
    // unnoticed. Conversion would then drop it and replace it with only
    // an insert-validation trigger, silently losing the operator's own
    // cascading update.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_workflow_exec_id_fkey",
    )
    .execute(&mut conn)
    .await
    .expect("drop the real foreign key to make room for the impostor");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_workflow_exec_id_fkey \
         FOREIGN KEY (workflow_exec_id) REFERENCES harvest_workflow_executions(id) \
         ON DELETE CASCADE ON UPDATE CASCADE",
    )
    .execute(&mut conn)
    .await
    .expect("seed an impostor with the right columns, target and delete action");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an impostor foreign key with ON UPDATE CASCADE must still refuse -- the \
             built-in ON UPDATE NO ACTION is part of the shape too",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_workflow_exec_id_fkey"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_workflow_exec_id_fkey",
    )
    .execute(&mut conn)
    .await
    .expect("drop the impostor constraint");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_workflow_exec_id_fkey \
         FOREIGN KEY (workflow_exec_id) REFERENCES harvest_workflow_executions(id) \
         ON DELETE CASCADE",
    )
    .execute(&mut conn)
    .await
    .expect("restore harvest's own real foreign key for later tests");
}

#[tokio::test]
async fn an_impostor_reusing_harvests_reserved_cohort_check_name_still_refuses() {
    // Review finding: name, type, table and column alone must not identify
    // harvest's own generated `{LEGACY_PARTITION}_cohort_ck`. An operator's
    // own `CHECK` on `cohort` could reuse the exact reserved name while
    // enforcing a different expression -- a floor rather than a ceiling,
    // say. Matching on the rendered `pg_get_constraintdef` output too is
    // what closes that gap, the same way the foreign key's shape check
    // does above.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS harvest_events_legacy_cohort_ck",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray decoy from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_legacy_cohort_ck \
         CHECK (cohort > '2020-01-01T00:00:00Z'::timestamptz)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an impostor reusing harvest's reserved cohort-check name");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "an impostor reusing harvest's reserved cohort-check name but a different \
             expression must still refuse -- name, type and column alone must not \
             exempt it",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_legacy_cohort_ck"),
        "the refusal must name the offending constraint; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT harvest_events_legacy_cohort_ck")
        .execute(&mut conn)
        .await
        .expect("drop the impostor constraint");
}

#[tokio::test]
async fn a_user_index_at_the_identifier_length_limit_survives_conversion() {
    // Issue #1270 item 9: Postgres silently truncates an identifier over 63
    // bytes. Appending a suffix to a name already at that limit renames it
    // TO ITSELF. The schema-scoped name is never freed. Replaying the
    // captured index definition onto the new parent then fails with
    // `duplicate_relation`. A user-defined index at exactly the limit must
    // not make `enable` unusable.
    //
    // The proof is the ATTACH-LEGACY path completing at all. `enable_sql`
    // renames the legacy table's indexes to free their names BEFORE
    // replaying the captured definitions, which still name the ORIGINAL
    // identifiers, onto the new parent. A self-colliding rename leaves the
    // legacy copy holding the name. So the replay's `CREATE INDEX
    // <original name>` collides with it, and the whole transaction — one
    // `enable_sql` script — rolls back with `duplicate_relation`. There is
    // no partial-success state to inspect afterward. Either it all
    // committed, or none of it did.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A row, so the table takes the ATTACH-LEGACY path rather than the
    // fresh-table path. Only the former exercises the legacy rename loop
    // this item's fix is in.
    let exec = insert_execution(&mut conn, "longidx_wf", "li-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed");

    // Exactly 63 bytes — Postgres's identifier limit.
    let long_name = format!("idx_{}", "x".repeat(59));
    assert_eq!(long_name.len(), 63, "precondition: exactly at the limit");
    // `IF EXISTS` first. This suite may re-run against a persistent
    // database, rather than a fresh one per run. A stray relation left by
    // an interrupted earlier run must not fail this test for an unrelated
    // reason.
    diesel::sql_query(format!("DROP INDEX IF EXISTS {long_name}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE INDEX {long_name} ON harvest_events (event_type)"
    ))
    .execute(&mut conn)
    .await
    .expect("seed a user index at the identifier limit");

    let report = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect(
            "enable must not fail on a user index whose name is already at \
             Postgres's 63-byte identifier limit — a self-colliding rename \
             would abort the whole conversion with duplicate_relation",
        );
    assert!(
        matches!(report.mode, EnableMode::AttachLegacy { .. }),
        "precondition: must exercise the legacy rename loop, got {:?}",
        report.mode
    );

    assert_eq!(events_relkind(&mut conn).await, "p");

    // The original name is now claimed by the REPLAYED index on the new
    // parent. This is the strongest available proof the rename actually
    // freed it. A self-colliding rename would have failed the transaction
    // above, rather than leave anything to check here.
    let on_new_parent = scalar_bool(
        &mut conn,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'harvest_events' AND indexname = '{long_name}') AS v"
        ),
    )
    .await;
    assert!(
        on_new_parent,
        "the user's index must be replayed onto the new parent under its \
         ORIGINAL name — that is the whole point of freeing it"
    );
}

#[tokio::test]
async fn a_user_index_at_the_identifier_length_limit_survives_disable() {
    // The reverse direction of the item 9 fix. `disable_partitioning`
    // renames every index on the partitioned parent with `__old`. It is
    // exactly as vulnerable to the same silent-self-rename bug. Same proof
    // shape as above: the whole revert is one transaction, so a
    // self-colliding rename aborts it with duplicate_relation rather than
    // leaving a partial state.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let long_name = format!("idx_{}", "y".repeat(59));
    assert_eq!(long_name.len(), 63, "precondition: exactly at the limit");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {long_name}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE INDEX {long_name} ON harvest_events (event_type)"
    ))
    .execute(&mut conn)
    .await
    .expect("seed a user index at the identifier limit on the partitioned parent");

    partition::disable_partitioning(&mut conn)
        .await
        .expect(
            "disable must not fail on a user index whose name is already at \
             Postgres's 63-byte identifier limit",
        )
        .expect("the shard was partitioned, so disable must report a revert");

    assert_eq!(events_relkind(&mut conn).await, "r");

    // The reclaimed flat table carries the index back under its ORIGINAL
    // name.
    let on_flat_table = scalar_bool(
        &mut conn,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'harvest_events' AND indexname = '{long_name}') AS v"
        ),
    )
    .await;
    assert!(
        on_flat_table,
        "the user's index must be replayed onto the reclaimed flat table \
         under its ORIGINAL name"
    );
}

#[tokio::test]
async fn a_multibyte_user_index_name_near_the_limit_survives_conversion() {
    // Review finding on item 9: the PL/pgSQL rename helper budgeted with
    // `length()` and cut with `left()`. Both count CHARACTERS. Postgres's
    // 63 limit counts BYTES. A multibyte name near the limit could still
    // produce a candidate over 63 bytes. Postgres then truncates it
    // itself, landing on a name the preceding uniqueness check never saw.
    // The rename could then still self-collide or hit an unseen one.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "mbidx_wf", "mb-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed");

    // "é" is 2 bytes in UTF-8. `ix_` (3 bytes) plus 30 of them is exactly
    // 63 bytes. Already at the limit, so Postgres stores it unchanged,
    // exactly like the ASCII item-9 test's `long_name`.
    //
    // A character-based budget sees only 33 characters against a
    // 55-character allowance, so it appends the suffix unchanged.
    // Postgres then truncates the 71-byte result back down to precisely
    // this same 63-byte name. The rename self-collides, just as it would
    // with a plain ASCII name.
    let requested = format!("ix_{}", "é".repeat(30));
    assert_eq!(
        requested.len(),
        63,
        "precondition: exactly at the 63-byte limit"
    );
    diesel::sql_query(format!("DROP INDEX IF EXISTS \"{requested}\""))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE INDEX \"{requested}\" ON harvest_events (event_type)"
    ))
    .execute(&mut conn)
    .await
    .expect("seed a user index with a multibyte name at the identifier limit");

    let report = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect(
            "enable must not fail on a multibyte user index name near the \
             63-byte identifier limit",
        );
    assert!(
        matches!(report.mode, EnableMode::AttachLegacy { .. }),
        "precondition: must exercise the legacy rename loop, got {:?}",
        report.mode
    );
    assert_eq!(events_relkind(&mut conn).await, "p");

    let on_new_parent = scalar_bool(
        &mut conn,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'harvest_events' AND indexname = '{requested}') AS v"
        ),
    )
    .await;
    assert!(
        on_new_parent,
        "the user's multibyte-named index must be replayed onto the new \
         parent under its original stored name; a byte-unsafe rename could \
         leave the name unfreed or garbled instead"
    );
}

#[tokio::test]
async fn an_operators_own_function_of_the_same_name_does_not_block_conversion() {
    // Review finding: the rename helper used a bare, fixed name in the
    // application schema (`harvest_bounded_rename_958`). An operator's
    // own schema could already hold a function of that exact name and
    // signature. The plain `CREATE FUNCTION` would then abort the
    // conversion. For the large-table plan, that abort lands inside
    // phase 4, after phases 2 and 3's expensive online preparation had
    // already run.
    // The helper now lives in `pg_temp`, each session's private schema,
    // where no persistent object can ever occupy the name ahead of time.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_bounded_rename_958(text, oid, text, text)")
        .execute(&mut conn)
        .await
        .expect("clear any stray function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_bounded_rename_958(p_kind text, p_relid oid, p_base text, \
         p_suffix text) RETURNS text LANGUAGE sql AS $$ SELECT p_base $$",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator's own function sharing the helper's name and signature");

    let exec = insert_execution(&mut conn, "fn_collide_wf", "fn-collide-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table, to exercise the rename loop that defines the helper");

    let report = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect(
            "an operator's own function sharing the transient helper's name and signature \
             must not block conversion",
        );
    assert!(
        matches!(report.mode, EnableMode::AttachLegacy { .. }),
        "precondition: must exercise the legacy rename loop, got {:?}",
        report.mode
    );
    assert_eq!(events_relkind(&mut conn).await, "p");

    let operator_fn_survives = scalar_bool(
        &mut conn,
        "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE p.proname = 'harvest_bounded_rename_958' AND n.nspname = current_schema()) AS v",
    )
    .await;
    assert!(
        operator_fn_survives,
        "the operator's own function must be left exactly as it was, never touched by the \
         session-scoped helper"
    );

    diesel::sql_query("DROP FUNCTION harvest_bounded_rename_958(text, oid, text, text)")
        .execute(&mut conn)
        .await
        .expect("drop the operator's function");
}

// ══ AC2: byte-identical per-execution event semantics ══════════════════════

#[tokio::test]
async fn history_load_and_delta_load_are_identical_between_layouts() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;

    // Unpartitioned reference run.
    reset_to_unpartitioned(&mut conn).await;
    let exec = insert_execution(&mut conn, "cmp_wf", "cmp-1", day(2026, 3, 2), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("append unpartitioned");
    let ref_full = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("ref full");
    let ref_delta = autumn_harvest::store::load_history_since(&mut conn, exec_id, 1)
        .await
        .expect("ref delta");
    let ref_json: Vec<String> =
        diesel::sql_query("SELECT event_data::text AS v FROM harvest_events ORDER BY event_id")
            .load::<TextRow>(&mut conn)
            .await
            .expect("ref json")
            .into_iter()
            .map(|r| r.v)
            .collect();

    // Partitioned run with the same inputs.
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    let partitioned_exec =
        insert_execution(&mut conn, "cmp_wf", "cmp-1", day(2026, 3, 2), None).await;
    let partitioned_id = ExecutionId::from_uuid(partitioned_exec);
    autumn_harvest::store::append_events(&mut conn, partitioned_id, &sample_events(), 0)
        .await
        .expect("append partitioned");
    let part_full = autumn_harvest::store::load_history(&mut conn, partitioned_id)
        .await
        .expect("part full");
    let part_delta = autumn_harvest::store::load_history_since(&mut conn, partitioned_id, 1)
        .await
        .expect("part delta");
    let part_json: Vec<String> =
        diesel::sql_query("SELECT event_data::text AS v FROM harvest_events ORDER BY event_id")
            .load::<TextRow>(&mut conn)
            .await
            .expect("part json")
            .into_iter()
            .map(|r| r.v)
            .collect();

    assert_eq!(
        as_json(&ref_full.events),
        as_json(&part_full.events),
        "AC2: load_history must be byte-identical between layouts"
    );
    assert_eq!(
        as_json(&ref_delta.events),
        as_json(&part_delta.events),
        "AC2: load_history_since delta results must be byte-identical"
    );
    assert_eq!(
        ref_json, part_json,
        "AC7: the adjacently-tagged event JSON at rest must be unchanged"
    );
}

#[tokio::test]
async fn sequential_per_execution_event_ids_survive_partitioning() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = insert_execution(&mut conn, "seq_wf", "seq-1", day(2026, 4, 1), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("first append");
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 3)
        .await
        .expect("second append");

    let ids: Vec<i64> =
        diesel::sql_query("SELECT event_id::bigint AS n FROM harvest_events ORDER BY event_id")
            .load::<CountRow>(&mut conn)
            .await
            .expect("ids")
            .into_iter()
            .map(|r| r.n)
            .collect();
    assert_eq!(
        ids,
        (0..6).collect::<Vec<i64>>(),
        "AC2: per-execution event ids must stay a dense 0..n sequence"
    );
}

#[tokio::test]
async fn duplicate_event_ids_are_still_rejected_on_the_partitioned_layout() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = insert_execution(&mut conn, "dup_wf", "dup-1", day(2026, 4, 2), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("first append");
    let clash = autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0).await;
    assert!(
        clash.is_err(),
        "AC2: the (workflow_exec_id, event_id) uniqueness contract — the \
         engine's optimistic-concurrency detector — must survive partitioning. \
         Adding the partition key to the unique index is only safe BECAUSE the \
         cohort is functionally dependent on workflow_exec_id."
    );
}

#[tokio::test]
async fn a_closed_partition_cannot_be_raced_by_a_later_append() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Yesterday's cohort exists and its window has closed. The cohort of a new
    // row is its APPEND instant, so nothing can route into it — which is what
    // makes the sweeper's "this partition holds no live execution's rows"
    // proof safe against a concurrent append between the check and the drop.
    let yesterday = Utc::now() - chrono::Duration::days(1);
    partition::ensure_cohort(&mut conn, yesterday)
        .await
        .expect("materialize yesterday");
    let closed = partition::partition_name(partition::cohort_start(
        yesterday,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));

    let exec = insert_execution(&mut conn, "seal_wf", "seal-1", yesterday, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("append");

    let landed_in_closed = scalar_i64(
        &mut conn,
        &format!(
            "SELECT COUNT(*)::bigint AS n FROM harvest_events
              WHERE tableoid::regclass::text = '{closed}'"
        ),
    )
    .await;
    assert_eq!(
        landed_in_closed, 0,
        "AC3: a partition whose window has closed must be sealed — an append \
         landing in it after the sweeper proved it empty would destroy a live \
         execution's history"
    );
    let today = partition::partition_name(partition::cohort_start(
        Utc::now(),
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    assert_eq!(
        scalar_i64(
            &mut conn,
            &format!(
                "SELECT COUNT(*)::bigint AS n FROM harvest_events
                  WHERE tableoid::regclass::text = '{today}'"
            ),
        )
        .await,
        3,
        "and the rows landed in the currently-open cohort"
    );
}

#[tokio::test]
async fn an_execution_whose_events_span_cohorts_loads_its_full_history_in_order() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // A long-running execution writes across a cohort boundary. Partitioning by
    // append instant means its history is genuinely split across partitions —
    // the trade this design makes — so AC2's ordering and delta guarantees have
    // to hold ACROSS that split, not merely within one partition.
    let exec = insert_execution(&mut conn, "span_wf", "span-1", Utc::now(), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("first append");
    backdate_events(&mut conn, exec, Utc::now() - chrono::Duration::days(2)).await;
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 3)
        .await
        .expect("second append");

    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(DISTINCT tableoid)::bigint AS n FROM harvest_events"
        )
        .await,
        2,
        "precondition: the history really is split across two partitions"
    );

    let history = autumn_harvest::store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    assert_eq!(
        history.events.len(),
        6,
        "AC2: load_history must return every event across the split"
    );
    let ids: Vec<i64> =
        diesel::sql_query("SELECT event_id::bigint AS n FROM harvest_events ORDER BY event_id")
            .load::<CountRow>(&mut conn)
            .await
            .expect("ids")
            .into_iter()
            .map(|r| r.n)
            .collect();
    assert_eq!(
        ids,
        (0..6).collect::<Vec<i64>>(),
        "AC2: the per-execution id sequence stays dense across cohorts"
    );
    let delta = autumn_harvest::store::load_history_since(&mut conn, exec_id, 3)
        .await
        .expect("delta");
    assert_eq!(
        as_json(&delta.events),
        as_json(&history.events[3..]),
        "AC2: a delta load spanning the split returns exactly the tail"
    );
}

#[tokio::test]
async fn an_event_for_an_unknown_execution_is_rejected() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let orphan = ExecutionId::from_uuid(uuid::Uuid::new_v4());
    let res = autumn_harvest::store::append_events(&mut conn, orphan, &sample_events(), 0).await;
    assert!(
        res.is_err(),
        "AC1: the partitioned layout trades the FK (whose CASCADE is exactly \
         what we are removing) for a cohort trigger — that trigger must keep \
         the FK's insert-time protection, else events could be written for an \
         execution that never existed"
    );
}

// ══ AC3: reclamation by partition DROP, never row-by-row DELETE ════════════

#[tokio::test]
async fn a_retention_pass_reclaims_an_expired_cohort_by_dropping_its_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    for i in 0..5 {
        seed_expired(&mut conn, "drop_wf", &format!("d-{i}"), old).await;
    }
    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    assert!(
        scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "precondition: the expired cohort's partition exists before the pass"
    );

    let pool = build_pool(&url);
    let config = RetentionConfig::with_max_age(Duration::from_secs(86_400));
    let result = run_one_tick(pool, config, None).await;
    assert_eq!(result.deleted_count, 5, "all five executions collected");

    assert!(
        !scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC3: the expired cohort's partition must be DROPPED — an O(1) \
         metadata operation — not emptied row by row"
    );
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_events"
        )
        .await,
        0,
        "AC3: the events are gone"
    );
}

#[tokio::test]
async fn reclamation_leaves_no_dead_tuples_behind() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    for i in 0..20 {
        seed_expired(&mut conn, "bloat_wf", &format!("b-{i}"), old).await;
    }

    // `pg_stat_all_tables` accumulates for the lifetime of the database, so the
    // counter below has to measure THIS retention pass and nothing else — not
    // an earlier test's scrub on a shared `HARVEST_TEST_DATABASE_URL`, and not
    // this test's own seeding. `backdate_events` in particular moves rows
    // ACROSS partitions, which Postgres implements as delete + insert and which
    // would otherwise be miscounted as reclamation deletes.
    //
    // The flush before the reset is load-bearing: a backend buffers its
    // statistics locally and flushes them lazily, so a bare `pg_stat_reset()`
    // clears the counters and then lets the still-pending seeding stats land on
    // top of the zeroed values.
    diesel::sql_query("SELECT pg_stat_force_next_flush()")
        .execute(&mut conn)
        .await
        .expect("flush pending statistics before resetting");
    diesel::sql_query("SELECT pg_stat_reset()")
        .execute(&mut conn)
        .await
        .expect("reset statistics");

    let pool = build_pool(&url);
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        None,
    )
    .await;

    diesel::sql_query("SELECT pg_stat_force_next_flush()")
        .execute(&mut conn)
        .await
        .ok();
    let deleted_tuples = scalar_i64(
        &mut conn,
        "SELECT COALESCE(SUM(n_tup_del), 0)::bigint AS n
           FROM pg_stat_all_tables
          WHERE relname LIKE 'harvest_events%'",
    )
    .await;
    assert_eq!(
        deleted_tuples, 0,
        "AC3 / Success Metric: the steady-state reclamation path must issue \
         ZERO row deletes against harvest_events — that is the whole point: \
         no dead tuples, no vacuum debt, no index bloat"
    );
}

#[tokio::test]
async fn a_cohort_with_a_surviving_execution_is_never_dropped() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    // One expired execution and one still RUNNING, both with rows in the same
    // cohort.
    seed_expired(&mut conn, "mix_wf", "m-old", old).await;
    let running = insert_execution(&mut conn, "mix_wf", "m-live", old, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(running),
        &sample_events(),
        0,
    )
    .await
    .expect("seed running");
    backdate_events(&mut conn, running, old).await;
    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));

    let pool = build_pool(&url);
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        None,
    )
    .await;

    assert!(
        scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC3: a partition holding a long-running execution's rows must NOT be \
         dropped — correctness first"
    );
    let survivors = autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(running))
        .await
        .expect("running history still loadable");
    assert_eq!(
        survivors.events.len(),
        3,
        "AC3: the long-running execution must not lose a single row"
    );
}

// ══ AC4: the HistoryArchiver hook ══════════════════════════════════════════

#[tokio::test]
async fn the_archiver_receives_full_history_before_the_partition_is_dropped() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    for i in 0..3 {
        seed_expired(&mut conn, "arc_wf", &format!("a-{i}"), old).await;
    }

    let archiver = Arc::new(RecordingArchiver::default());
    let pool = build_pool(&url);
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        Some(archiver.clone() as Arc<dyn HistoryArchiver>),
    )
    .await;

    assert_eq!(
        archiver.archived_event_counts(),
        vec![3, 3, 3],
        "AC4: the archiver must fire ONCE PER EXECUTION with that execution's \
         full history, before its rows become unreachable — archival \
         correctness is independent of drop- vs delete-based reclamation"
    );
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_events"
        )
        .await,
        0,
        "AC4: and the rows are reclaimed afterwards"
    );
}

#[tokio::test]
async fn a_failing_archiver_blocks_both_the_delete_and_the_partition_drop() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    seed_expired(&mut conn, "fail_wf", "f-1", old).await;
    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));

    let archiver = Arc::new(RecordingArchiver::failing());
    let pool = build_pool(&url);
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        Some(archiver as Arc<dyn HistoryArchiver>),
    )
    .await;

    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_workflow_executions"
        )
        .await,
        1,
        "AC4: a failed archive must leave the execution in place (SkipFreeze)"
    );
    assert!(
        scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC4: and its partition must survive — the archive is the last copy, \
         so an unarchived execution's rows must stay reachable for a retry"
    );
}

// ══ AC5: legal holds and per-type overrides ════════════════════════════════

#[tokio::test]
async fn a_legal_hold_deterministically_blocks_the_partition_drop() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    let held = seed_expired(&mut conn, "hold_wf", "h-held", old).await;
    let _free = seed_expired(&mut conn, "hold_wf", "h-free", old).await;
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
            SET legal_hold_set_at = NOW(), legal_hold_reason = 'litigation'
          WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(held)
    .execute(&mut conn)
    .await
    .expect("place hold");

    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    let pool = build_pool(&url);
    run_one_tick(
        pool.clone(),
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        None,
    )
    .await;

    assert!(
        scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC5: a held execution must deterministically block reclamation of \
         its rows — its partition must survive"
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(held))
            .await
            .expect("held history")
            .events
            .len(),
        3,
        "AC5: the held execution keeps every row"
    );

    // Lift the hold: the same pass now reclaims the cohort.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET legal_hold_set_at = NULL WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(held)
    .execute(&mut conn)
    .await
    .expect("lift hold");
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        None,
    )
    .await;
    assert!(
        !scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC5: once the hold lifts, the cohort becomes droppable again"
    );
}

#[tokio::test]
async fn a_longer_per_type_override_blocks_the_partition_drop() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    let _short = seed_expired(&mut conn, "short_wf", "s-1", old).await;
    let long = seed_expired(&mut conn, "long_wf", "l-1", old).await;

    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    let config = RetentionConfig::with_max_age(Duration::from_secs(86_400))
        .with_workflow_override("long_wf", Duration::from_secs(86_400 * 365));

    let pool = build_pool(&url);
    run_one_tick(pool, config, None).await;

    assert!(
        scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC5: a per-type override that is still retaining one execution must \
         block the drop of the cohort its rows live in"
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(long))
            .await
            .expect("retained history")
            .events
            .len(),
        3,
        "AC5: the over-retained execution keeps every row"
    );
}

// ══ AC6: the two sanctioned in-place mutation paths ════════════════════════

#[tokio::test]
async fn pii_erasure_tombstones_partitioned_rows_identically() {
    async fn erase_and_read(conn: &mut AsyncPgConnection, exec: uuid::Uuid) -> Vec<String> {
        autumn_harvest::erase::erase_workflow_payloads(
            conn,
            ExecutionId::from_uuid(exec),
            "gdpr-request",
        )
        .await
        .expect("erase");
        diesel::sql_query("SELECT event_data::text AS v FROM harvest_events ORDER BY event_id")
            .load::<TextRow>(conn)
            .await
            .expect("read back")
            .into_iter()
            .map(|r| r.v)
            .collect()
    }

    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;

    reset_to_unpartitioned(&mut conn).await;
    let past = Utc::now() - chrono::Duration::days(3);
    let a = insert_execution(&mut conn, "pii_wf", "p-1", past, Some(past)).await;
    autumn_harvest::store::append_events(&mut conn, ExecutionId::from_uuid(a), &sample_events(), 0)
        .await
        .expect("seed");
    let unpartitioned = erase_and_read(&mut conn, a).await;

    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    let b = insert_execution(&mut conn, "pii_wf", "p-1", past, Some(past)).await;
    autumn_harvest::store::append_events(&mut conn, ExecutionId::from_uuid(b), &sample_events(), 0)
        .await
        .expect("seed");
    let partitioned = erase_and_read(&mut conn, b).await;

    assert_eq!(
        unpartitioned, partitioned,
        "AC6: erase.rs PII tombstoning — the only sanctioned in-place mutation \
         of harvest_events.event_data — must produce identical rows in both \
         layouts"
    );
    assert!(
        unpartitioned.iter().any(|v| v.contains("_harvest_erased")),
        "precondition: erasure actually tombstoned something"
    );
}

#[tokio::test]
async fn heartbeat_checkpoints_are_unaffected_by_partitioning() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let past = Utc::now();
    let exec = insert_execution(&mut conn, "hb_wf", "hb-1", past, None).await;
    let task: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_task_queue
            (queue_name, task_type, workflow_exec_id, input, state, started_at)
         VALUES ('default', 'activity', $1, '{}'::jsonb, 'RUNNING', NOW())
         RETURNING id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec)
    .get_result::<IdRow>(&mut conn)
    .await
    .expect("insert task")
    .id;

    autumn_harvest::queue::record_heartbeat(&mut conn, task, serde_json::json!({"progress": 0.5}))
        .await
        .expect("AC6: heartbeat checkpoints must work on a partitioned deployment");

    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue
              WHERE heartbeat_details IS NOT NULL"
        )
        .await,
        1,
        "AC6: the checkpoint landed"
    );
}

// ══ AC8: engine-automated partition maintenance ════════════════════════════

#[tokio::test]
async fn the_retention_tick_pre_creates_future_partitions_with_no_operator_cron() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(
        &mut conn,
        &EnableOptions {
            lookahead_cohorts: 1,
            ..EnableOptions::default()
        },
    )
    .await
    .expect("enable");

    let before = partition::list_partitions(&mut conn)
        .await
        .expect("list")
        .len();

    let pool = build_pool(&url);
    run_one_tick(
        pool,
        RetentionConfig::with_max_age(Duration::from_secs(86_400)),
        None,
    )
    .await;

    let after = partition::list_partitions(&mut conn).await.expect("list");
    assert!(
        after.len() > before,
        "AC8: partition creation must be automated by the engine — a retention \
         tick must extend the lookahead window with no operator cron. \
         before={before} after={}",
        after.len()
    );
    let horizon =
        Utc::now() + chrono::Duration::days(i64::from(partition::DEFAULT_LOOKAHEAD_COHORTS));
    assert!(
        after
            .iter()
            .filter(|p| !p.is_default)
            .any(|p| p.upper.is_some_and(|u| u >= horizon)),
        "AC8: the window must reach the configured lookahead horizon"
    );
}

#[tokio::test]
async fn partition_maintenance_runs_at_startup_not_after_a_full_tick_interval() {
    // Issue #1270 item 5: the maintenance block used to live only after
    // the loop's `sleep(tick_interval)`. A shard that restarted after being
    // offline longer than its lookahead window had no covering partition.
    // That lasted up to a full tick interval — an hour, by default —
    // during which every append lands in the DEFAULT partition. `spawn`
    // must trigger the first pass immediately.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let pool = build_pool(&url);
    let pools = ShardedDbPool::single(pool);
    let config = RetentionConfig {
        // Long enough that only a startup trigger — not this test's patience —
        // could make the assertion below pass.
        tick_interval_secs: 3600,
        ..RetentionConfig::default()
    };
    assert!(
        config.partitions.enabled,
        "precondition: partition maintenance must be on by default"
    );

    let started = Utc::now();
    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), None, None)
        .expect("retention runtime should spawn when enabled");
    // Deliberately NOT calling `runtime.run_now()` — that is the crutch
    // every other test in this file uses. The whole point here is that
    // `spawn` alone must be enough.

    let mut saw_maintenance = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.partition_maintenance
                .as_ref()
                .and_then(|m| m.at)
                .is_some_and(|at| at >= started)
        {
            saw_maintenance = true;
            break;
        }
    }
    runtime.shutdown();
    assert!(
        saw_maintenance,
        "partition maintenance must run at startup — waiting up to 5s, against \
         a 3600s tick_interval that only a startup trigger could beat"
    );
}

#[tokio::test]
async fn the_startup_pass_runs_partition_maintenance_only_not_the_whole_tick() {
    // Review finding: the immediate startup pass exists for one stated
    // reason (issue #1270 item 5) — an uncovered write window after a
    // restart. It used to be implemented by pre-loading the shared
    // trigger channel. That made the very first tick win the race
    // against `sleep`. It then ran the WHOLE janitor tick: history
    // retention plus the audit, schedule, summary and rate-limit purges,
    // across every shard. A fleet whose processes restart together turns
    // that into a coordinated scan/delete stampede.
    //
    // Prove the startup pass does ONLY partition maintenance. Seed an
    // expired execution under a history-retention age. Spawn with a long
    // `tick_interval`, and confirm the execution SURVIVES the startup
    // pass even though partition maintenance has already run. Then call
    // `run_now()` and confirm a real full tick still collects it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let old = Utc::now() - chrono::Duration::days(30);
    seed_expired(&mut conn, "startup_wf", "startup-1", old).await;

    let pool = build_pool(&url);
    let pools = ShardedDbPool::single(pool);
    let config = RetentionConfig {
        // Long enough that only the startup pass could touch the shard
        // before the assertion below. Not this test's own patience, and
        // not `run_now()` — it is not called yet.
        tick_interval_secs: 3600,
        ..RetentionConfig::with_max_age(Duration::from_secs(86_400))
    };

    let started = Utc::now();
    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), None, None)
        .expect("retention runtime should spawn when enabled");

    let mut saw_maintenance = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.partition_maintenance
                .as_ref()
                .and_then(|m| m.at)
                .is_some_and(|at| at >= started)
        {
            saw_maintenance = true;
            break;
        }
    }
    assert!(
        saw_maintenance,
        "partition maintenance must still run at startup"
    );

    // A little more room: on this shard, workflow-history retention
    // should never have run yet. There is no stamp to wait for; only
    // time to let it happen, if the startup pass wrongly ran it too.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_workflow_executions \
             WHERE workflow_id = 'startup-1'"
        )
        .await,
        1,
        "the startup pass must not have run workflow-history retention — the \
         expired execution must still be here"
    );

    // Now prove `run_now()` still gets a real, full tick.
    runtime.run_now();
    let mut deleted = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_workflow_executions \
             WHERE workflow_id = 'startup-1'",
        )
        .await
            == 0
        {
            deleted = true;
            break;
        }
    }
    runtime.shutdown();
    assert!(
        deleted,
        "run_now() must still trigger a full tick, collecting the expired execution"
    );
}

#[tokio::test]
async fn partition_maintenance_stays_none_on_a_shard_that_never_converted() {
    // Issue #1270 item 6: the maintenance block is gated on
    // `config.partitions.enabled`, not on the detected layout. `maintain`
    // returns a stamped-but-empty outcome for an unpartitioned shard. So a
    // deployment that turned the config flag on without ever running
    // `harvest partition enable` reported `Some(...)` after every tick.
    // That claimed maintenance was active on a shard that never opted in.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    // Deliberately NOT calling enable_partitioning: this shard stays flat.

    let pool = build_pool(&url);
    let pools = ShardedDbPool::single(pool);
    let config = RetentionConfig::with_max_age(Duration::from_secs(86_400));
    assert!(
        config.partitions.enabled,
        "precondition: partition maintenance must be on so this test actually \
         exercises the gate rather than an already-disabled config"
    );

    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), None, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    let mut ran = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            ran = true;
            break;
        }
    }
    assert!(ran, "the retention tick did not complete in time");
    // A little more room: on this shard `partition_maintenance` should
    // never be set. There is no stamp to wait for, only time to let a
    // maintenance pass run, if it were going to.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let snap = runtime.monitor().snapshot();
    runtime.shutdown();

    let result = snap
        .per_shard
        .iter()
        .find(|r| r.shard == 0)
        .expect("shard 0 result");
    assert!(
        result.partition_maintenance.is_none(),
        "partition_maintenance must stay None on a shard that never opted \
         into the partitioned layout; got {:?}",
        result.partition_maintenance
    );
}

#[tokio::test]
async fn partition_maintenance_reports_failed_when_the_layout_probe_errors() {
    // Test-coverage review finding on item 6: the layout probe before
    // `maintain` has three outcomes -- Unpartitioned (continue, stay
    // None), Partitioned (run maintain), and Err. Only the first two were
    // exercised by a prior test. An Err must report
    // `MaintenanceOutcome::failed`, not silently collapse into looking
    // like a shard that never converted.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Break the probe's catalog lookup without touching data. It matches
    // on relname = 'harvest_events', so a rename alone makes it find no
    // row and return Err(NotFound).
    diesel::sql_query("ALTER TABLE harvest_events RENAME TO harvest_events_probe_break_958")
        .execute(&mut conn)
        .await
        .expect("rename harvest_events so the layout probe cannot find it");

    let pool = build_pool(&url);
    let pools = ShardedDbPool::single(pool);
    let config = RetentionConfig::default();

    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), None, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    let mut outcome = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && let Some(m) = &r.partition_maintenance
        {
            outcome = Some(m.clone());
            break;
        }
    }
    runtime.shutdown();

    diesel::sql_query("ALTER TABLE harvest_events_probe_break_958 RENAME TO harvest_events")
        .execute(&mut conn)
        .await
        .expect("restore the harvest_events name");

    let outcome = outcome.expect(
        "a layout-probe failure must report MaintenanceOutcome::failed, not \
         leave partition_maintenance unset forever",
    );
    assert!(
        outcome.last_error.is_some(),
        "a layout-probe failure must carry the error, not report an empty \
         outcome that looks like a healthy pass; got {outcome:?}"
    );
    assert!(
        outcome.created.is_empty() && outcome.sweep.dropped.is_empty(),
        "a probe failure must not report as if maintain() itself ran; got {outcome:?}"
    );
}

#[tokio::test]
async fn partition_maintenance_clears_to_none_after_a_shard_reverts_to_unpartitioned() {
    // Review finding on item 6: `update_partitions` only ever sets
    // `partition_maintenance`. A shard that ran maintenance and later
    // reverted via `harvest partition disable` kept reporting its last
    // outcome forever, since nothing ever cleared the field back to
    // `None`.
    //
    // `RetentionConfig::default()`, not `with_max_age`, is deliberate. With
    // a history-retention age configured, the tick's OTHER update path
    // (`RetentionMonitor::update`, for the history-retention counters)
    // replaces the whole per-shard result every tick. It would incidentally
    // reset this field too, masking a regression here. No max_age skips
    // that phase, so only the fix under test can clear the field.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let pool = build_pool(&url);
    let pools = ShardedDbPool::single(pool);
    let config = RetentionConfig::default();
    assert!(
        config.max_age_secs.is_none(),
        "precondition: the history-retention phase must be off, or its own \
         update() would mask a regression in the fix under test"
    );

    let runtime = RetentionRuntime::spawn(pools, config, Arc::new(NoopMetrics), None, None)
        .expect("retention runtime should spawn when enabled");
    runtime.run_now();

    let mut got_outcome = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.partition_maintenance.is_some()
        {
            got_outcome = true;
            break;
        }
    }
    assert!(
        got_outcome,
        "precondition: a partitioned shard must report a maintenance outcome"
    );

    partition::disable_partitioning(&mut conn)
        .await
        .expect("disable")
        .expect("shard was partitioned, so disable must report a DisableReport");

    runtime.run_now();
    let mut cleared = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.partition_maintenance.is_none()
        {
            cleared = true;
            break;
        }
    }
    runtime.shutdown();
    assert!(
        cleared,
        "partition_maintenance must clear back to None once the shard \
         reverts to unpartitioned, not keep reporting its last outcome"
    );
}

#[tokio::test]
async fn an_append_for_an_uncovered_cohort_survives_via_the_default_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Simulate a maintenance gap: the partition covering the current cohort is
    // gone. Without a DEFAULT partition the next append would fail with "no
    // partition of relation found" and stall a live workflow — an availability
    // bug, not a storage one.
    let today = partition::partition_name(partition::cohort_start(
        Utc::now(),
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));
    diesel::sql_query(format!("DROP TABLE {today}"))
        .execute(&mut conn)
        .await
        .expect("simulate a maintenance gap");

    let exec = insert_execution(&mut conn, "skew_wf", "sk-1", Utc::now(), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect(
            "AC8: an append whose cohort has no partition must land in the \
             DEFAULT partition rather than failing",
        );

    let in_default = scalar_bool(
        &mut conn,
        &format!(
            "SELECT bool_and(tableoid::regclass::text = '{}') AS v FROM harvest_events",
            partition::DEFAULT_PARTITION
        ),
    )
    .await;
    assert!(in_default, "AC8: the row landed in the DEFAULT partition");

    // Maintenance must then materialize the real partition and drain the row —
    // rows parked in DEFAULT otherwise BLOCK creation of the very partition
    // that would cover them, so a gap would be self-perpetuating.
    let moved = partition::drain_default(&mut conn)
        .await
        .expect("AC8: draining the default partition is engine-automated");
    assert_eq!(moved, 3, "every parked row is moved");
    let drained = scalar_bool(
        &mut conn,
        &format!(
            "SELECT bool_and(tableoid::regclass::text <> '{}') AS v FROM harvest_events",
            partition::DEFAULT_PARTITION
        ),
    )
    .await;
    assert!(
        drained,
        "AC8: after a drain the rows live in their real cohort partition, and \
         the DEFAULT partition is empty again"
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .expect("history after drain")
            .events
            .len(),
        3,
        "AC8: draining must not lose or reorder a single event"
    );
}

#[tokio::test]
async fn the_sweep_is_bounded_and_reports_what_it_dropped_and_blocked() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Three fully-reclaimable cohorts (no execution rows at all) and one held.
    for days in [10_i64, 11, 12] {
        let ts = Utc::now() - chrono::Duration::days(days);
        partition::ensure_cohort(&mut conn, ts)
            .await
            .expect("materialize cohort");
    }
    let live_ts = Utc::now() - chrono::Duration::days(13);
    let live = insert_execution(&mut conn, "live_wf", "lv-1", live_ts, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(live),
        &sample_events(),
        0,
    )
    .await
    .expect("seed");
    backdate_events(&mut conn, live, live_ts).await;

    let outcome = partition::sweep(
        &mut conn,
        Utc::now(),
        &SweepOptions {
            max_drops: 2,
            ..SweepOptions::default()
        },
        None,
    )
    .await
    .expect("sweep");

    assert_eq!(
        outcome.dropped.len(),
        2,
        "AC8: the sweep must honour its per-tick drop budget so one tick can \
         never take an unbounded number of ACCESS EXCLUSIVE locks; got {outcome:?}"
    );
    assert!(
        outcome
            .blocked
            .iter()
            .any(
                |b| b.contains(&partition::partition_name(partition::cohort_start(
                    live_ts,
                    partition::DEFAULT_COHORT_WIDTH_SECS
                )))
            ),
        "AC8: a cohort blocked by a live execution must be reported, not \
         silently skipped; got {outcome:?}"
    );
}

#[tokio::test]
async fn a_truncated_pass_resumes_past_a_permanently_blocked_oldest_run() {
    // Review finding: a fixed oldest-first sweep, restarted from scratch
    // every call, cannot converge when the oldest `max_attempts`
    // partitions are permanently blocked. Every pass would re-spend its
    // whole budget proving the same oldest partitions blocked. A
    // droppable partition further along would never be reached, so
    // storage would grow without bound.
    //
    // Two pinned cohorts (permanently blocked) followed by one droppable
    // cohort prove both halves, with a budget that exhausts exactly on
    // the two blocked ones. The first pass cannot reach the droppable
    // one. Feeding its `next_resume` back into a second pass does.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    for days in [20_i64, 21] {
        let ts = Utc::now() - chrono::Duration::days(days);
        let exec =
            insert_execution(&mut conn, "pinned_wf", &format!("resume-{days}"), ts, None).await;
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(exec),
            &sample_events(),
            0,
        )
        .await
        .expect("seed");
        backdate_events(&mut conn, exec, ts).await;
    }
    let droppable_ts = Utc::now() - chrono::Duration::days(19);
    partition::ensure_cohort(&mut conn, droppable_ts)
        .await
        .expect("materialize the droppable cohort");

    let opts = SweepOptions {
        max_drops: 10,
        max_attempts: 2,
        ..SweepOptions::default()
    };

    let first = partition::sweep(&mut conn, Utc::now(), &opts, None)
        .await
        .expect("first sweep");
    assert!(
        first.dropped.is_empty(),
        "the budget must exhaust on the two blocked partitions before reaching \
         the droppable one; got {first:?}"
    );
    assert!(first.truncated, "the budget was spent; got {first:?}");
    let resume_after = first
        .next_resume
        .expect("a truncated pass must report where to resume");

    let second = partition::sweep(&mut conn, Utc::now(), &opts, Some(resume_after))
        .await
        .expect("second sweep");
    assert_eq!(
        second.dropped.len(),
        1,
        "resuming past the blocked run must reach and drop the droppable \
         partition the first pass never got to; got {second:?}"
    );
}

#[tokio::test]
async fn a_sweep_bounds_blocked_evaluations_too_not_only_drops() {
    // Issue #1270 item 1: `max_drops` bounds successful drops, not the work of
    // finding them. A partition that ends up blocked still costs a full
    // gate evaluation. Five closed cohorts that are ALL blocked must not
    // let the pass examine every one of them, regardless of
    // `max_attempts`.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Five closed cohorts, each pinned by its own still-existing execution —
    // every one of them is blocked, none droppable.
    for days in [20_i64, 21, 22, 23, 24] {
        let ts = Utc::now() - chrono::Duration::days(days);
        let exec = insert_execution(&mut conn, "pinned_wf", &format!("p-{days}"), ts, None).await;
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(exec),
            &sample_events(),
            0,
        )
        .await
        .expect("seed");
        backdate_events(&mut conn, exec, ts).await;
    }

    let outcome = partition::sweep(
        &mut conn,
        Utc::now(),
        &SweepOptions {
            max_drops: 10,
            max_attempts: 2,
            ..SweepOptions::default()
        },
        None,
    )
    .await
    .expect("sweep");

    assert!(outcome.dropped.is_empty(), "nothing here is droppable");
    assert_eq!(
        outcome.blocked.len(),
        2,
        "the pass must stop evaluating once it spends its attempts budget, \
         not keep scanning every remaining blocked partition; got {outcome:?}"
    );
    assert!(
        outcome.truncated,
        "a pass that stopped before considering every partition must say so, \
         or an operator reading zero dropped and two blocked cannot tell a \
         clean shard from one that ran out of budget; got {outcome:?}"
    );
}

#[tokio::test]
async fn exhausting_the_budget_on_the_last_eligible_partition_is_not_truncated() {
    // Review finding on item 1: the budget check ran at the TOP of the
    // loop, before the cheap eligibility filters (DEFAULT, still open,
    // unbounded). A pass that spent its last attempt on the final CLOSED
    // partition would still mark `truncated` on its next iteration. That
    // held even when every remaining partition in the list is DEFAULT or
    // still open. Those are cheap skips that were never going to cost
    // budget anyway. So this reports "ran out of budget" for a pass that
    // in fact finished.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // One past, empty cohort partition: the single real candidate. Every
    // OTHER partition in the list -- `enable`'s own lookahead cohorts and
    // DEFAULT -- covers today or later. `upper > now` (or `is_default`)
    // skips them for free. `max_attempts: 1` exactly exhausts the budget
    // on the one real candidate.
    let (past_partition, _) =
        partition::ensure_cohort(&mut conn, Utc::now() - chrono::Duration::days(5))
            .await
            .expect("materialize a past, empty cohort partition");

    let outcome = partition::sweep(
        &mut conn,
        Utc::now(),
        &SweepOptions {
            max_drops: 10,
            max_attempts: 1,
            ..SweepOptions::default()
        },
        None,
    )
    .await
    .expect("sweep");

    assert!(
        outcome.dropped.contains(&past_partition),
        "the one real candidate, the empty past cohort, must still be \
         dropped within budget; got {outcome:?}"
    );
    assert!(
        !outcome.truncated,
        "nothing else in the list was ever going to cost budget (DEFAULT \
         and every still-open cohort are free skips), so this pass must \
         not report truncated; got {outcome:?}"
    );
}

#[tokio::test]
async fn the_straggler_delete_bounds_its_own_statement_timeout() {
    // Issue #1270 item 2: `delete_orphan_rows` ran with no
    // `statement_timeout` of its own. A straggler execution can pin a
    // cohort. On that partition, a `DELETE` blocked behind a lock could
    // run, or wait, for as long as the blocker lives, inside the retention
    // tick. It must fail safe like every other budget in this module: timed
    // out, not errored, and retried next tick.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // A cohort pinned by a still-existing execution, so the gate reports
    // OWNED_REASON (not droppable). With straggler_grace configured, the
    // sweep attempts the targeted orphan DELETE rather than skipping it.
    let old = Utc::now() - chrono::Duration::days(10);
    let pinning = insert_execution(&mut conn, "pin_wf", "pin-1", old, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(pinning),
        &sample_events(),
        0,
    )
    .await
    .expect("seed");
    backdate_events(&mut conn, pinning, old).await;
    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));

    // A concurrent SHARE lock on that one partition. It conflicts with the
    // ROW EXCLUSIVE the straggler DELETE needs — the same conflict the
    // sweeper's own drop re-check relies on. So the DELETE blocks waiting
    // for it. `statement_timeout` bounds a statement's total time including
    // a lock wait, so this forces the timeout deterministically. No need
    // for a large or slow dataset.
    let mut locker = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin");
    diesel::sql_query(format!("LOCK TABLE {cohort_partition} IN SHARE MODE"))
        .execute(&mut locker)
        .await
        .expect("lock the partition");

    let outcome = partition::sweep(
        &mut conn,
        Utc::now(),
        &SweepOptions {
            straggler_grace: Some(Duration::from_secs(0)),
            exact_scan_timeout: Duration::from_millis(200),
            ..SweepOptions::default()
        },
        None,
    )
    .await;

    diesel::sql_query("ROLLBACK")
        .execute(&mut locker)
        .await
        .expect("release the lock");

    let outcome = outcome.expect(
        "a straggler DELETE that hits its statement_timeout must be reported as \
         blocked, not returned as a hard Err — the pass must retry next tick, \
         exactly like the exact ownership scan's own budget",
    );
    assert_eq!(
        outcome.straggler_rows_deleted, 0,
        "the DELETE never got past the lock wait, so nothing was deleted"
    );
    assert!(
        outcome
            .blocked
            .iter()
            .any(|b| b.contains(&cohort_partition)),
        "the pinned cohort must still be reported blocked; got {outcome:?}"
    );
}

#[tokio::test]
async fn the_straggler_delete_restores_statement_timeout_inside_a_caller_transaction() {
    // Review finding: `delete_orphan_rows`'s own `SET LOCAL
    // statement_timeout` runs inside a `conn.transaction()`. A nested
    // `.transaction()` -- one opened while `conn` is already inside a
    // transaction -- is a `SAVEPOINT` under Diesel. `RELEASE SAVEPOINT`
    // does not undo a `SET LOCAL` made inside it; only `ROLLBACK TO
    // SAVEPOINT` does. A caller already inside its own transaction
    // would otherwise see `statement_timeout` silently pinned to the
    // sweep's own budget for the rest of it.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // A cohort with a genuine straggler, pinning it against the drop, and
    // a real orphan alongside it. The targeted DELETE then has something
    // to remove and actually succeeds, rather than timing out on a lock
    // wait like the sibling test above.
    let old = Utc::now() - chrono::Duration::days(10);
    let pinning = insert_execution(&mut conn, "pin2_wf", "pin2-1", old, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(pinning),
        &sample_events(),
        0,
    )
    .await
    .expect("seed the pinning execution");
    backdate_events(&mut conn, pinning, old).await;

    let orphan = insert_execution(&mut conn, "orphan_wf", "orphan-1", old, None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(orphan),
        &sample_events(),
        0,
    )
    .await
    .expect("seed the soon-to-be-orphaned execution");
    backdate_events(&mut conn, orphan, old).await;
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(orphan)
        .execute(&mut conn)
        .await
        .expect("collect the execution, leaving orphans in the pinned cohort");

    conn.transaction::<(), autumn_harvest::HarvestError, _>(async |conn| {
        diesel::sql_query("SET LOCAL statement_timeout = '54321ms'")
            .execute(conn)
            .await?;

        let outcome = partition::sweep(
            conn,
            Utc::now(),
            &SweepOptions {
                straggler_grace: Some(Duration::from_secs(0)),
                ..SweepOptions::default()
            },
            None,
        )
        .await?;
        assert!(
            outcome.straggler_rows_deleted > 0,
            "precondition: the DELETE must actually remove the orphan rows, \
             not merely attempt to; got {outcome:?}"
        );

        let timeout = diesel::sql_query("SELECT current_setting('statement_timeout') AS v")
            .get_result::<TextRow>(conn)
            .await?
            .v;
        assert_eq!(
            timeout, "54321ms",
            "the straggler DELETE must restore the caller's statement_timeout \
             inside its own savepoint, not leave its own budget pinned for the \
             rest of the caller's transaction"
        );
        Ok(())
    })
    .await
    .expect("outer transaction");
}

// ══ Pure unit coverage for the cohort algebra ══════════════════════════════

#[test]
fn cohort_start_floors_to_the_configured_width() {
    let ts = Utc.with_ymd_and_hms(2026, 8, 31, 17, 42, 9).unwrap();
    assert_eq!(
        partition::cohort_start(ts, 86_400),
        Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap(),
        "a daily cohort floors to UTC midnight"
    );
    assert_eq!(
        partition::cohort_start(ts, 3_600),
        Utc.with_ymd_and_hms(2026, 8, 31, 17, 0, 0).unwrap(),
        "an hourly cohort floors to the hour"
    );
}

#[test]
fn cohort_start_floors_pre_epoch_timestamps_downward() {
    // Rust's integer division truncates toward zero, which would round a
    // pre-1970 timestamp UP into the next cohort and silently route an
    // execution's events to the wrong partition.
    let ts = Utc.with_ymd_and_hms(1969, 12, 31, 23, 0, 0).unwrap();
    assert_eq!(
        partition::cohort_start(ts, 86_400),
        Utc.with_ymd_and_hms(1969, 12, 31, 0, 0, 0).unwrap(),
        "flooring must go DOWN on both sides of the epoch"
    );
}

#[test]
fn partition_names_are_legible_and_collision_free() {
    let a = partition::partition_name(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
    let b = partition::partition_name(Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap());
    assert_ne!(a, b);
    assert!(a.starts_with("harvest_events_p_"), "got {a}");
    assert!(
        a.contains("20260831"),
        "the name must carry its cohort date so an operator can read \
         `\\dt harvest_events*` and know what they are looking at; got {a}"
    );
}

#[test]
fn enable_options_reject_a_nonsensical_cohort_width() {
    assert!(
        EnableOptions {
            cohort_width_secs: 0,
            ..EnableOptions::default()
        }
        .validate()
        .is_err(),
        "a zero-width cohort would divide by zero in the cohort function"
    );
    assert!(
        EnableOptions {
            lookahead_cohorts: 0,
            ..EnableOptions::default()
        }
        .validate()
        .is_err(),
        "a zero lookahead means every append lands in the DEFAULT partition"
    );
    assert!(EnableOptions::default().validate().is_ok());
}

// ══ The CI layout switch itself ════════════════════════════════════════════

/// Guards the `linuxpart` CI pass against the worst failure mode a
/// second test run can have: passing while testing nothing new.
///
/// `HARVEST_TEST_PARTITIONED` is the only thing that distinguishes that pass
/// from the plain `linux` one. If the flag ever stopped taking effect — a typo
/// in the manifest osclass, an env var the runner forgot to export, a
/// `test_init_sql()` that quietly dropped the enable script — every partitioned
/// suite would keep passing while re-running the *unpartitioned* layout, and
/// AC2's "byte-identical between layouts" evidence would silently become a
/// tautology. This test fails loudly in exactly that case.
///
/// It is also meaningful in the default pass, where it pins the other half of
/// the contract: with the flag unset, the bootstrap must be byte-for-byte the
/// plain migration bundle, so an existing deployment's test coverage is
/// unaffected by any of this.
#[tokio::test]
async fn the_test_bootstrap_honours_the_partitioned_layout_switch() {
    let requested = autumn_harvest::test_partitioned_layout_requested();

    if !requested {
        assert_eq!(
            autumn_harvest::test_init_sql(),
            autumn_harvest::full_migrations_sql(),
            "with HARVEST_TEST_PARTITIONED unset the bootstrap must be exactly \
             the migration bundle — partitioning is opt-in, and every existing \
             suite must keep running against the layout operators have today"
        );
        return;
    }

    assert!(
        autumn_harvest::test_init_sql().contains("PARTITION BY RANGE (cohort)"),
        "HARVEST_TEST_PARTITIONED is set, so the bootstrap must carry the \
         enable script"
    );

    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    let layout = partition::detect_layout(&mut conn).await.expect("detect");
    assert!(
        layout.is_partitioned(),
        "HARVEST_TEST_PARTITIONED is set but the database came up UNPARTITIONED \
         ({layout:?}). The partitioned CI pass would then be a second, \
         identical run of the default pass — green, and proving nothing."
    );
    assert!(
        partition::list_partitions(&mut conn)
            .await
            .expect("list")
            .iter()
            .any(|p| p.is_default),
        "and it must be fully set up, DEFAULT partition included"
    );
}

// ══ Success Metric gate ════════════════════════════════════════════════════

/// CI gate for the falsifiable half of issue #958's Success Metric.
///
/// The metric names four quantities. Two are deterministic and are gated here;
/// two are latency percentiles that depend on the host and belong in the
/// benchmark (`benches/retention_reclaim_bench.rs`), not in a CI assertion that
/// would flake on a noisy runner:
///
/// - **Gated**: row-level `DELETE`s against `harvest_events` (must be zero) and
///   the dead-tuple ratio left behind (must be under the metric's 5%).
/// - **Measured, not gated**: concurrent append and claim p99.
///
/// It runs the *unpartitioned* arm too, and asserts it does the opposite. A
/// one-sided assertion would pass just as happily against a corpus that
/// happened to be empty, or a retention pass that collected nothing — the
/// contrast is what makes this evidence rather than a tautology.
///
/// Shares its harness with the benchmark, so the number CI gates on and the
/// number published in `docs/perf-artifacts/` can never come from two different
/// implementations.
#[tokio::test]
async fn the_partitioned_layout_reclaims_without_creating_bloat() {
    use super::retention_reclaim_support as harness;

    /// Deliberately small: this gate is about the SHAPE of the two costs
    /// (zero deletes versus tens of thousands, no bloat versus double digits),
    /// which is already unambiguous at this size. The issue's 10M-row scale
    /// lives behind `HARVEST_BENCH_SCALE=full` in the benchmark.
    const SCALE: harness::Scale = harness::Scale {
        executions: 2_000,
        events_per_execution: 5,
        cohorts: 10,
        expired_fraction: 0.5,
    };

    let mut results = Vec::new();
    for partitioned in [false, true] {
        let (url, _c) = setup_db().await;
        let mut conn = connect(&url).await;
        reset_to_unpartitioned(&mut conn).await;
        if partitioned {
            partition::enable_partitioning(&mut conn, &EnableOptions::default())
                .await
                .expect("enable");
        }
        harness::seed(&mut conn, SCALE, partitioned).await;
        drop(conn);
        results.push(
            harness::measure_pass(&url, partitioned, SCALE, Duration::from_millis(500)).await,
        );
    }

    let (flat, part) = (&results[0], &results[1]);

    assert!(
        flat.events_reclaimed > 0 && part.events_reclaimed > 0,
        "precondition: both layouts must actually reclaim something, else the \
         contrast below is vacuous. flat={flat:?} part={part:?}"
    );
    assert_eq!(
        flat.events_reclaimed, part.events_reclaimed,
        "both layouts must reclaim the SAME events — the mechanism changes, the \
         outcome does not"
    );

    assert!(
        flat.event_rows_deleted > 0,
        "precondition: the unpartitioned baseline must reclaim by row DELETE — \
         if it did not, this test is not measuring the thing it claims to. \
         got {}",
        flat.event_rows_deleted
    );
    assert_eq!(
        part.event_rows_deleted, 0,
        "AC3 / Success Metric: the partitioned layout must reclaim with ZERO \
         row-level deletes against harvest_events. The unpartitioned baseline \
         issued {}.",
        flat.event_rows_deleted
    );

    assert!(
        part.dead_tuple_ratio < 0.05,
        "Success Metric: post-pass dead-tuple ratio must be under 5%, got \
         {:.2}% (the unpartitioned baseline left {:.2}%)",
        part.dead_tuple_ratio * 100.0,
        flat.dead_tuple_ratio * 100.0,
    );
    assert!(
        flat.dead_tuple_ratio > part.dead_tuple_ratio,
        "Success Metric: the row-DELETE baseline must leave MORE bloat than the \
         partition-drop path — otherwise the comparison proves nothing. \
         flat={:.2}% part={:.2}%",
        flat.dead_tuple_ratio * 100.0,
        part.dead_tuple_ratio * 100.0,
    );
}

// ══ Regressions found in review ════════════════════════════════════════════

#[tokio::test]
async fn the_legacy_partition_is_never_dropped_while_it_holds_live_history() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A populated shard with a RUNNING execution and a legal-held one — the
    // shape every real deployment has when an operator opts in.
    let running = insert_execution(&mut conn, "legacy_wf", "lg-run", day(2026, 1, 5), None).await;
    let held = insert_execution(
        &mut conn,
        "legacy_wf",
        "lg-held",
        day(2026, 1, 5),
        Some(day(2026, 1, 5)),
    )
    .await;
    for e in [running, held] {
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(e),
            &sample_events(),
            0,
        )
        .await
        .expect("seed");
    }
    diesel::sql_query(
        "UPDATE harvest_workflow_executions SET legal_hold_set_at = NOW() WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(held)
    .execute(&mut conn)
    .await
    .expect("place hold");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable on a populated table");

    // Every pre-cutover row carries the migration's `-infinity` sentinel, which
    // sorts BELOW every finite timestamptz. An ownership scan that bound a
    // finite lower bound for the legacy partition's `MINVALUE` would match
    // ZERO of its rows, conclude "no live owner", and drop the entire
    // pre-conversion history — running executions and legal holds included —
    // on the first tick after conversion.
    let outcome = partition::sweep(&mut conn, Utc::now(), &SweepOptions::default(), None)
        .await
        .expect("sweep must not error on a converted shard");

    assert!(
        !outcome
            .dropped
            .contains(&partition::LEGACY_PARTITION.to_string()),
        "AC3/AC5: the legacy partition holds a RUNNING and a legal-held \
         execution's entire history — it must never be dropped. Got {outcome:?}"
    );
    assert!(
        outcome
            .blocked
            .iter()
            .any(|b| b.contains(partition::LEGACY_PARTITION)),
        "and it must be REPORTED as blocked, not silently skipped: {outcome:?}"
    );
    for (exec, label) in [(running, "running"), (held, "held")] {
        assert_eq!(
            autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(exec))
                .await
                .unwrap_or_else(|e| panic!("{label} history: {e}"))
                .events
                .len(),
            3,
            "AC5: the {label} execution must not lose a single row"
        );
    }
}

#[tokio::test]
async fn a_duplicate_event_id_is_rejected_even_across_cohorts() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = insert_execution(&mut conn, "dup_wf", "dup-x", Utc::now(), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("first append");

    // Move the existing rows into an older cohort, so the re-append below lands
    // in a DIFFERENT partition from the rows it duplicates. The table
    // constraint is `UNIQUE (workflow_exec_id, event_id, cohort)` — it can only
    // see within one partition, so nothing at the index level rejects this.
    //
    // This is not a contrived race. Immediately after converting a populated
    // shard it is SYSTEMATIC: every pre-cutover row carries the `-infinity`
    // sentinel, so any re-append for a pre-existing execution — a stale worker
    // whose task was reclaimed, a retried workflow task — lands in today's
    // cohort. Without a cross-partition check the engine's split-brain detector
    // silently stops firing for every execution that existed at conversion.
    backdate_events(&mut conn, exec, Utc::now() - chrono::Duration::days(3)).await;

    let clash = autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0).await;
    assert!(
        clash.is_err(),
        "AC2: `(workflow_exec_id, event_id)` uniqueness — the optimistic-\
         concurrency detector — must hold ACROSS partitions, not merely within \
         one cohort"
    );
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_events"
        )
        .await,
        3,
        "AC2: and no duplicate row may survive"
    );
}

#[tokio::test]
async fn reverting_succeeds_on_a_shard_that_has_orphans_and_rebuilds_the_flat_constraints() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Orphan event rows are the partitioned layout's DESIGNED garbage: deleting
    // an execution no longer cascades. So the escape hatch has to cope with
    // them — otherwise it is unavailable on exactly the shards that have run
    // long enough to need it, because the flat layout's foreign key forbids
    // what the partitioned layout deliberately allows.
    let exec = insert_execution(&mut conn, "rev_wf", "rev-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed");
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .execute(&mut conn)
        .await
        .expect("collect the execution, leaving orphans");
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_events"
        )
        .await,
        3,
        "precondition: the events really are orphaned"
    );

    let report = partition::disable_partitioning(&mut conn)
        .await
        .expect("revert must succeed on a shard with orphans")
        .expect("the shard was partitioned");
    assert_eq!(
        report.orphans_removed, 3,
        "the orphans must be discarded — and counted, so an operator is not \
         left to infer that rows disappeared"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "and the layout is back to an ordinary table"
    );
    // The flat layout's constraints must be real again, not silently skipped.
    let live = insert_execution(&mut conn, "rev_wf", "rev-2", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(live),
        &sample_events(),
        0,
    )
    .await
    .expect("append after revert");
    assert!(
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(live),
            &sample_events(),
            0
        )
        .await
        .is_err(),
        "the flat UNIQUE (workflow_exec_id, event_id) must be rebuilt and enforcing"
    );
}

#[tokio::test]
async fn enabling_does_not_lose_events_committed_while_the_probe_ran() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // The conversion decides between "drop the empty legacy table" and "attach
    // it whole" from a row probe. Taken before the rename, that probe holds
    // only ACCESS SHARE — which does not conflict with a concurrent INSERT — so
    // a workflow that started and wrote its first events in the gap would
    // commit before the rename, the probe would still say "empty", and the
    // table containing them would be DROPPED. Enabling on a live-but-currently-
    // empty shard is exactly the recommended rollout, so that window is the
    // common case, not an exotic one.
    //
    // This test cannot open the real window from one connection, but it pins
    // the invariant the fix establishes: whatever rows exist when the exclusive
    // lock is taken must survive the conversion.
    let exec = insert_execution(&mut conn, "toctou_wf", "tc-1", Utc::now(), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("seed");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .expect("history survives the conversion")
            .events
            .len(),
        3,
        "no committed event may be lost by the conversion"
    );
}

#[tokio::test]
async fn reverting_and_re_enabling_round_trips() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // Rolling back must not be a one-way door. `disable` is the escape hatch
    // the documentation offers, and an operator who takes it during an incident
    // has to be able to roll forward again afterwards.
    //
    // The trap is subtle: `disable` builds the flat table with `LIKE …
    // INCLUDING DEFAULTS`, which copies the PARTITIONED parent's cohort default
    // onto it. Every append after the revert then stamps a live cohort into a
    // column the flat layout treats as inert — and the next `enable` attaches
    // the legacy table with `CHECK (cohort < cutover)`, which every row written
    // in the current cohort violates. The conversion fails outright, with an
    // error naming a constraint the operator has never heard of.
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("first enable");
    let exec = insert_execution(&mut conn, "rt_wf", "rt-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("append while partitioned");

    partition::disable_partitioning(&mut conn)
        .await
        .expect("revert")
        .expect("was partitioned");
    assert_eq!(events_relkind(&mut conn).await, "r");

    // Append again on the flat layout — this is what poisons the column if the
    // partitioned default was carried over.
    let flat = insert_execution(&mut conn, "rt_wf", "rt-2", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(flat),
        &sample_events(),
        0,
    )
    .await
    .expect("append while flat");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("re-enable after a revert must succeed");
    assert_eq!(events_relkind(&mut conn).await, "p");
    for (e, label) in [(exec, "pre-revert"), (flat, "post-revert")] {
        assert_eq!(
            autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(e))
                .await
                .unwrap_or_else(|err| panic!("{label} history: {err}"))
                .events
                .len(),
            3,
            "the {label} execution's history must survive the round trip"
        );
    }
}

#[tokio::test]
async fn the_large_table_migration_plan_actually_runs() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // The plan is the path the documentation sends an operator down for any
    // table too big for `enable`'s single transaction. An earlier revision
    // printed its catalog-driven parts as PROSE — "emit one line per object
    // from: SELECT format(…)" — so running the generated file verbatim silently
    // skipped the legacy constraint renames, and step 4 then aborted on
    // `ADD CONSTRAINT harvest_events_pkey` because the old schema-scoped index
    // still held the name. The operator discovers that after spending an hour
    // on the CONCURRENTLY index builds.
    //
    // So this test does not inspect the script's text: it EXECUTES it, against
    // a populated table, and checks the result is the same layout `enable`
    // produces. Executing it is the only assertion that can catch prose
    // masquerading as SQL.
    let exec = insert_execution(&mut conn, "plan_wf", "plan-1", day(2026, 2, 3), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut conn, exec_id, &sample_events(), 0)
        .await
        .expect("seed a populated table");

    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now()) {
        // `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block, so
        // each is sent on its own — exactly as the script instructs.
        diesel::sql_query(&step.sql)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "plan step (phase {}) failed — an operator running the generated \
                     script would hit this:\n{}\nerror: {e}",
                    step.phase, step.sql
                )
            });
    }

    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "running the plan must leave the table partitioned"
    );
    let parts = partition::list_partitions(&mut conn).await.expect("list");
    assert!(
        parts.iter().any(|p| p.name == partition::LEGACY_PARTITION),
        "the pre-conversion rows must be attached as the legacy partition: {parts:?}"
    );
    assert!(
        parts.iter().any(|p| p.is_default),
        "and the DEFAULT catch-all must exist: {parts:?}"
    );
    assert!(
        parts.iter().filter(|p| !p.is_default).count()
            >= partition::DEFAULT_LOOKAHEAD_COHORTS as usize,
        "and the write window must be covered before the first retention tick: {parts:?}"
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, exec_id)
            .await
            .expect("history survives the scripted conversion")
            .events
            .len(),
        3,
    );

    // The layout must be functionally the real thing, not merely partitioned:
    // the trigger and the cohort default are what the sweeper depends on.
    let fresh = insert_execution(&mut conn, "plan_wf", "plan-2", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(fresh),
        &sample_events(),
        0,
    )
    .await
    .expect("append after the scripted conversion");
    assert!(
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(fresh),
            &sample_events(),
            0
        )
        .await
        .is_err(),
        "the uniqueness trigger must be installed by the script too"
    );
}

#[tokio::test]
async fn the_large_table_plans_rename_loop_survives_an_index_at_the_identifier_length_limit() {
    // Issue #1270 item 9, the online path. `migration_plan_steps`'
    // phase-4 rename loop is exactly as vulnerable as `enable_sql`'s. Both
    // fail at a name already at Postgres's 63-byte identifier limit. It is
    // a distinct code path (separate top-level statements, not one `DO`
    // block). That path could have a different bug, even though the
    // underlying flaw is the same.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    let exec = insert_execution(&mut conn, "plan_longidx_wf", "pli-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    let long_name = format!("idx_{}", "z".repeat(59));
    assert_eq!(long_name.len(), 63, "precondition: exactly at the limit");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {long_name}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE INDEX {long_name} ON harvest_events (event_type)"
    ))
    .execute(&mut conn)
    .await
    .expect("seed a user index at the identifier limit");

    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now()) {
        diesel::sql_query(&step.sql)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "plan step (phase {}) failed on an index name at the \
                     identifier limit — a self-colliding rename would abort \
                     here with duplicate_relation:\n{}\nerror: {e}",
                    step.phase, step.sql
                )
            });
    }

    assert_eq!(events_relkind(&mut conn).await, "p");
    let on_new_parent = scalar_bool(
        &mut conn,
        &format!(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
             AND tablename = 'harvest_events' AND indexname = '{long_name}') AS v"
        ),
    )
    .await;
    assert!(
        on_new_parent,
        "the user's index must be replayed onto the new parent under its \
         ORIGINAL name"
    );
}

#[tokio::test]
async fn an_append_racing_the_execution_delete_cannot_commit_an_orphan() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = insert_execution(&mut conn, "race_wf", "race-1", Utc::now(), None).await;

    // The integrity trigger's probe takes `FOR KEY SHARE` — the same lock mode
    // the foreign key it replaces took. Without it the probe is an observation,
    // not a guarantee: the trigger sees the execution, a concurrent delete
    // commits, and the insert commits afterwards, creating exactly the orphan
    // the check exists to prevent.
    //
    // Here the deleter holds its transaction open while the append runs, so the
    // append must BLOCK on the row lock rather than sail past. Once the delete
    // commits, the append's probe finds no row and rejects.
    let mut deleter = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut deleter)
        .await
        .expect("begin");
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .execute(&mut deleter)
        .await
        .expect("delete inside an open transaction");

    let appender_url = url.clone();
    let append = tokio::spawn(async move {
        let mut c = connect(&appender_url).await;
        autumn_harvest::store::append_events(
            &mut c,
            ExecutionId::from_uuid(exec),
            &sample_events(),
            0,
        )
        .await
    });

    // The append must still be blocked on the uncommitted delete's row lock.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !append.is_finished(),
        "the append must BLOCK on the execution row lock while the delete is \
         uncommitted — if it finished, the probe took no lock and the orphan \
         race is open"
    );

    diesel::sql_query("COMMIT")
        .execute(&mut deleter)
        .await
        .expect("commit the delete");

    let result = append.await.expect("append task");
    assert!(
        result.is_err(),
        "once the delete commits, the append must be rejected — its execution \
         no longer exists"
    );
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_events"
        )
        .await,
        0,
        "and no orphan row may have been committed"
    );
}

#[tokio::test]
async fn a_cohort_proved_droppable_by_the_exact_scan_is_actually_dropped() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Force the gate onto its EXACT scan (tier 3) by making more executions
    // survive than the narrow probe will enumerate, then check the partition is
    // still dropped.
    //
    // The bug this pins: the authoritative re-check under the drop lock used to
    // bail out whenever the survivor count exceeded `owner_probe_cap` — which
    // is exactly the condition that sends the gate to tier 3 in the first
    // place. So every partition that needed the exact scan to prove itself
    // droppable was rejected at the last step, forever. Reclamation stopped
    // dead on precisely the high-volume and legal-hold-heavy shards this
    // feature exists for, while the sweep kept cheerfully reporting them as
    // "blocked".
    let old = Utc::now() - chrono::Duration::days(30);
    for i in 0..4 {
        seed_expired(&mut conn, "exact_wf", &format!("x-{i}"), old).await;
    }
    let cohort_partition = partition::partition_name(partition::cohort_start(
        old,
        partition::DEFAULT_COHORT_WIDTH_SECS,
    ));

    // Live executions created BEFORE the cohort's upper bound — strictly
    // older, or the cheap tier-1 probe answers "nothing predates this" and the
    // exact scan is never reached — and more of them than the cap, so tier 2 is
    // skipped too. None of them owns a row in the old cohort (they have no
    // events at all), so the exact scan must prove the partition droppable.
    for i in 0..6 {
        insert_execution(
            &mut conn,
            "survivor_wf",
            &format!("s-{i}"),
            Utc::now() - chrono::Duration::days(40),
            None,
        )
        .await;
    }

    // A cap BELOW the survivor count is what forces tier 3. With the stock cap
    // of 1000 the narrow probe would answer, and this test would pass without
    // ever reaching the code path it exists for.
    let mut config = RetentionConfig::with_max_age(Duration::from_secs(86_400));
    config.partitions.owner_probe_cap = 2;
    assert!(
        config.partitions.owner_probe_cap < 6,
        "precondition: the survivor count must exceed the probe cap, or the \
         exact scan is never reached"
    );

    let pool = build_pool(&url);
    run_one_tick(pool, config, None).await;

    assert!(
        !scalar_bool(
            &mut conn,
            &format!("SELECT to_regclass('{cohort_partition}') IS NOT NULL AS v"),
        )
        .await,
        "AC3: a cohort proved droppable by the exact scan must actually be \
         dropped — the re-check under the lock has to re-run the SAME proof, \
         not a narrower one that can never succeed"
    );
    assert_eq!(
        scalar_i64(
            &mut conn,
            "SELECT COUNT(*)::bigint AS n FROM harvest_workflow_executions \
              WHERE workflow_name = 'survivor_wf'"
        )
        .await,
        6,
        "and the survivors are untouched"
    );
}

#[tokio::test]
async fn the_layout_works_when_harvest_is_installed_outside_public() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;

    // The rest of this module discovers everything through `current_schema()`,
    // so a deployment installed under a non-`public` schema is supported. The
    // integrity trigger has to agree: hard-coding `public` in its body means
    // every append either fails because `public.harvest_workflow_executions`
    // does not exist, or — worse — validates against an unrelated table that
    // happens to sit in `public`. The trigger is created through
    // `format(%I, current_schema())` so its pinned `search_path` names the
    // schema it was actually installed in.
    diesel::sql_query("DROP SCHEMA IF EXISTS harvest_alt CASCADE")
        .execute(&mut conn)
        .await
        .expect("reset");
    diesel::sql_query("CREATE SCHEMA harvest_alt")
        .execute(&mut conn)
        .await
        .expect("create schema");

    let mut alt = connect(&url).await;
    diesel::sql_query("SET search_path = harvest_alt")
        .execute(&mut alt)
        .await
        .expect("pin the session to the alternate schema");
    diesel_async::SimpleAsyncConnection::batch_execute(
        &mut alt,
        autumn_harvest::full_migrations_sql(),
    )
    .await
    .expect("migrate into the alternate schema");

    partition::enable_partitioning(&mut alt, &EnableOptions::default())
        .await
        .expect("enable in a non-public schema");

    let exec = insert_execution(&mut alt, "alt_wf", "alt-1", Utc::now(), None).await;
    let exec_id = ExecutionId::from_uuid(exec);
    autumn_harvest::store::append_events(&mut alt, exec_id, &sample_events(), 0)
        .await
        .expect("append must work — the trigger resolves its relations in this schema");
    assert_eq!(
        autumn_harvest::store::load_history(&mut alt, exec_id)
            .await
            .expect("history")
            .events
            .len(),
        3,
    );

    // And both halves of the trigger must still be enforcing, not merely
    // resolving against something.
    assert!(
        autumn_harvest::store::append_events(&mut alt, exec_id, &sample_events(), 0)
            .await
            .is_err(),
        "the cross-partition uniqueness check must be live here too"
    );
    assert!(
        autumn_harvest::store::append_events(
            &mut alt,
            ExecutionId::from_uuid(uuid::Uuid::new_v4()),
            &sample_events(),
            0,
        )
        .await
        .is_err(),
        "and so must the execution-existence check"
    );

    diesel::sql_query("DROP SCHEMA harvest_alt CASCADE")
        .execute(&mut conn)
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn a_drop_attempt_never_blocks_appends_while_it_waits_for_its_partition() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // One closed, fully orphaned cohort: the drop path's happy case.
    let exec = seed_expired(&mut conn, "lock_wf", "lock-old", day(2026, 1, 5)).await;
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .execute(&mut conn)
        .await
        .expect("collect the execution so the cohort is reclaimable");
    let target =
        diesel::sql_query("SELECT tableoid::regclass::text AS v FROM harvest_events LIMIT 1")
            .get_result::<TextRow>(&mut conn)
            .await
            .expect("locate the partition holding the seeded rows")
            .v;

    // A writer is mid-transaction in that partition — a `drain_default` pass, a
    // long transaction whose clock still stamps the closed cohort, a second
    // maintenance runtime. `ROW EXCLUSIVE` is exactly what an INSERT holds, and
    // it does not conflict with readers. The sweeper is entitled to wait for it
    // (bounded by `lock_timeout`) and to give up; it is NOT entitled to make
    // every append on the shard wait with it.
    let mut blocker = connect(&url).await;
    for stmt in [
        "BEGIN".to_string(),
        format!("LOCK TABLE {target} IN ROW EXCLUSIVE MODE"),
    ] {
        diesel::sql_query(&stmt)
            .execute(&mut blocker)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    let sweep_url = url.clone();
    let sweeper = tokio::spawn(async move {
        let mut c = connect(&sweep_url).await;
        partition::sweep(
            &mut c,
            Utc::now(),
            &SweepOptions {
                lock_timeout: Duration::from_secs(8),
                ..SweepOptions::default()
            },
            None,
        )
        .await
    });

    // Let the sweeper reach its lock wait, then append to a LIVE cohort — a
    // different partition entirely, which the blocker does not touch.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let mut writer = connect(&url).await;
    let live = insert_execution(&mut writer, "lock_wf", "lock-live", Utc::now(), None).await;
    let appended = tokio::time::timeout(
        Duration::from_secs(3),
        autumn_harvest::store::append_events(
            &mut writer,
            ExecutionId::from_uuid(live),
            &sample_events(),
            0,
        ),
    )
    .await;

    // Release the blocker before asserting, so a failure does not leave the
    // sweeper wedged for the rest of the suite.
    diesel::sql_query("ROLLBACK")
        .execute(&mut blocker)
        .await
        .expect("release the blocker");

    let appended = appended.expect(
        "AC3: a drop attempt held an exclusive lock while it waited — for the partition, \
         or for its own occupancy re-check, which is bounded by `exact_scan_timeout` and \
         not by `lock_timeout`. Every append on the shard queues behind it for the whole \
         wait: on the parent directly, or on any child, because the insert trigger's \
         cross-partition uniqueness check reads the parent and so locks every child in \
         ACCESS SHARE. The proof needs a lock that excludes writers to this one closed \
         cohort and nothing else; the exclusive window belongs to the DROP alone.",
    );
    appended.expect("the concurrent append itself must succeed");

    sweeper
        .await
        .expect("sweeper task")
        .expect("a partition it could not lock is a blocked partition, not an error");
}

#[tokio::test]
async fn the_online_migration_phases_bound_their_lock_wait() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // An idle-in-transaction reader — a reporting query, a leaked session. It
    // holds ACCESS SHARE on `harvest_events` and nothing else.
    let mut blocker = connect(&url).await;
    for stmt in ["BEGIN", "SELECT 1 FROM harvest_events LIMIT 1"] {
        diesel::sql_query(stmt)
            .execute(&mut blocker)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    // Phase 3 is documented as the ONLINE phase — the one an operator runs on a
    // live shard before the short exclusive window. `ADD CONSTRAINT ... NOT
    // VALID` skips the table scan but still needs ACCESS EXCLUSIVE to change
    // the catalog, so behind this reader it queues; and because a lock request
    // queues *ahead* of every later one, every append then queues behind the
    // ALTER. Unbounded. The plan has to fail fast instead.
    let opts = EnableOptions {
        lock_timeout: Duration::from_secs(1),
        ..EnableOptions::default()
    };
    let mut saw_lock_timeout = false;
    for step in partition::migration_plan_steps(&opts, Utc::now())
        .into_iter()
        .filter(|s| s.phase == 3)
    {
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            diesel::sql_query(&step.sql).execute(&mut conn),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the online phase queued behind an idle reader instead of failing fast, \
                 taking every append on the shard with it:\n{}",
                step.sql
            )
        });
        if let Err(e) = outcome
            && partition::is_lock_timeout(&e.to_string())
        {
            saw_lock_timeout = true;
        }
    }
    assert!(
        saw_lock_timeout,
        "and it must fail *because of the lock*, so an operator sees a bounded, \
         retryable error rather than a mystery"
    );

    diesel::sql_query("ROLLBACK")
        .execute(&mut blocker)
        .await
        .expect("release the blocker");
}

/// Can the split-role test's runtime role still read and append?
async fn granted(conn: &mut AsyncPgConnection) -> (bool, bool) {
    (
        scalar_bool(
            conn,
            "SELECT has_table_privilege('harvest_runtime_958', 'harvest_events', 'SELECT') AS v",
        )
        .await,
        scalar_bool(
            conn,
            "SELECT has_table_privilege('harvest_runtime_958', 'harvest_events', 'INSERT') AS v",
        )
        .await,
    )
}

#[tokio::test]
async fn conversion_preserves_the_grants_a_split_role_deployment_runs_on() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // The deployment shape this engine documents and probes for: migrations run
    // as the owning role, the runtime connects as a separately granted role.
    // `HARVEST_WRITE_PRIVILEGE_REQUIREMENTS` in the plugin's preflight demands
    // SELECT + INSERT on `harvest_events` precisely because a missing grant
    // there is a total history outage, not a degraded feature.
    for stmt in [
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'harvest_runtime_958') \
           THEN CREATE ROLE harvest_runtime_958 NOLOGIN; END IF; END $$",
        "GRANT SELECT, INSERT ON harvest_events TO harvest_runtime_958",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    assert_eq!(
        granted(&mut conn).await,
        (true, true),
        "precondition: the runtime role can read and append before conversion"
    );

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    assert_eq!(
        granted(&mut conn).await,
        (true, true),
        "AC1: enabling replaces `harvest_events` with a new parent table, and \
         `CREATE TABLE ... LIKE` copies no ACLs. The grants stay on the renamed \
         legacy table and the runtime role loses SELECT/INSERT on the table it \
         reads and appends every event through — a history outage that starts \
         the moment the conversion commits and lasts until someone re-issues \
         the GRANTs by hand."
    );

    let report = partition::disable_partitioning(&mut conn)
        .await
        .expect("revert")
        .expect("the shard was partitioned");
    let _ = report;
    assert_eq!(
        granted(&mut conn).await,
        (true, true),
        "and reverting replaces the table again — the escape hatch must not \
         leave the runtime role locked out either"
    );
}

/// Name of the unique index phase 2 builds for the partitioned primary key.
fn plan_pk_index() -> String {
    format!("{}_pk_idx", partition::LEGACY_PARTITION)
}

/// Mark an index invalid, exactly as a cancelled `CREATE INDEX CONCURRENTLY`
/// leaves it.
///
/// Postgres offers no supported way to produce that state on demand, so the
/// catalog is edited directly. Requires superuser, which the suite's
/// testcontainers Postgres is.
async fn invalidate_index(conn: &mut AsyncPgConnection, name: &str) {
    diesel::sql_query(format!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{name}'::regclass"
    ))
    .execute(conn)
    .await
    .unwrap_or_else(|e| panic!("invalidate {name}: {e}"));
}

async fn index_is_valid(conn: &mut AsyncPgConnection, name: &str) -> bool {
    scalar_bool(
        conn,
        &format!(
            "SELECT COALESCE((SELECT i.indisvalid FROM pg_index i
                               WHERE i.indexrelid = to_regclass('{name}')), false) AS v"
        ),
    )
    .await
}

async fn run_plan_phases(conn: &mut AsyncPgConnection, phases: std::ops::RangeInclusive<u8>) {
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| phases.contains(&s.phase))
    {
        diesel::sql_query(&step.sql)
            .execute(conn)
            .await
            .unwrap_or_else(|e| panic!("plan step (phase {}):\n{}\nerror: {e}", step.phase, e));
    }
}

#[tokio::test]
async fn the_plan_survives_a_rename_target_collision_on_the_built_in_pkey() {
    // Review finding: the scripted plan's phase-4 rename loop had the
    // identical gap enable_sql's did. See
    // `enable_survives_a_rename_target_collision_on_the_built_in_pkey`.
    // A later step dropped a hard-coded `harvest_events_pkey__pre958`
    // rather than the name `bounded_rename_fn` actually returned. An
    // operator's own constraint occupying that exact name then made the
    // hard-coded DROP remove the wrong object, leaving the real old
    // primary key behind.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(
        &mut conn,
        "plan_pkey_collide_wf",
        "plan-pkey-collide-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT \
         IF EXISTS harvest_events_pkey__pre958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray decoy from a previous run");
    // An EXCLUDE constraint, deliberately. See the sibling `enable_sql`
    // test's identical comment: a UNIQUE or CHECK decoy is refused by an
    // earlier preflight before conversion ever reaches the rename step.
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_pkey__pre958 \
         EXCLUDE USING gist (int8range(id, id, '[]') WITH &&)",
    )
    .execute(&mut conn)
    .await
    .expect("seed the decoy occupying the rename target");

    run_plan_phases(&mut conn, 1..=4).await;

    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "the conversion must succeed despite the collision"
    );
    let pkey = partition::LEGACY_PARTITION;
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint \
                 WHERE conrelid = '{pkey}'::regclass \
                 AND conname = 'harvest_events_pkey__pre958' AND contype = 'x') AS v"
            ),
        )
        .await,
        "the operator's own decoy constraint must survive untouched"
    );
    // See the sibling `enable_sql` test's comment. `ATTACH PARTITION`
    // itself gives the leaf its own primary key mirroring the parent's
    // shape, so asserting none exists is the wrong invariant. The bug
    // this proves fixed is the disambiguated rename target going
    // undropped.
    assert!(
        !scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_constraint \
                 WHERE conrelid = '{pkey}'::regclass \
                 AND conname LIKE 'harvest_events_pkey%__pre958' \
                 AND conname <> 'harvest_events_pkey__pre958') AS v"
            ),
        )
        .await,
        "the disambiguated rename target for the real primary key must have been \
         dropped, not left behind under a name the old hard-coded guess never \
         looked for"
    );
}

#[tokio::test]
async fn re_running_the_plan_rebuilds_an_index_a_cancelled_build_left_invalid() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=2).await;
    let pk = plan_pk_index();
    assert!(
        index_is_valid(&mut conn, &pk).await,
        "precondition: a clean phase 2 leaves a valid index"
    );

    // `CREATE INDEX CONCURRENTLY` is the one build an operator is most likely
    // to lose: it runs for hours on a large table and a cancel, a deploy or a
    // lock conflict leaves the index behind, INVALID.
    invalidate_index(&mut conn, &pk).await;

    // Re-running the plan from the top is exactly what the runbook tells them
    // to do. `IF NOT EXISTS` sees the name and reports success, so the invalid
    // index survives — and `ATTACH PARTITION` in phase 4 cannot reuse it, so it
    // silently builds a replacement while holding the parent-wide exclusive
    // lock the plan promises is metadata-only.
    run_plan_phases(&mut conn, 1..=2).await;
    assert!(
        index_is_valid(&mut conn, &pk).await,
        "phase 2 must detect the invalid index and rebuild it — otherwise the \
         plan's 'seconds, metadata only' lock window becomes a full index build \
         with every append queued behind it"
    );
}

#[tokio::test]
async fn phase_2_refuses_when_a_reserved_index_name_is_squatted_elsewhere() {
    // Review finding: matching phase 2's invalid-index cleanup by schema
    // and name alone selected any invalid index bearing one of the three
    // reserved names. It matched on any table. An operator's own
    // cancelled `CREATE INDEX CONCURRENTLY` build could coincidentally
    // land on one of those names, on a completely unrelated table. The
    // fix checks the expected table and column shape per name. It
    // refuses rather than either destroying the unrelated object or
    // silently leaving it squatting on the name this plan needs to
    // build under.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP TABLE IF EXISTS harvest_events_reindex_decoy_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray decoy table from a previous run");
    diesel::sql_query("CREATE TABLE harvest_events_reindex_decoy_958 (x int)")
        .execute(&mut conn)
        .await
        .expect("seed an unrelated table");
    diesel::sql_query(format!(
        "CREATE INDEX {} ON harvest_events_reindex_decoy_958 (x)",
        plan_pk_index()
    ))
    .execute(&mut conn)
    .await
    .expect("seed an index squatting on the reserved name, on an unrelated table");
    invalidate_index(&mut conn, &plan_pk_index()).await;

    run_plan_phases(&mut conn, 1..=1).await;

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 2)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 2 must refuse when a reserved index name is squatted by an unrelated \
         table, not silently destroy or silently skip past it",
    );
    assert!(
        msg.contains(&plan_pk_index()) && msg.contains("harvest_events_reindex_decoy_958"),
        "the refusal must name both the offending index and the table it is actually \
         on; got {msg}"
    );
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_class c \
                 JOIN pg_index i ON i.indexrelid = c.oid \
                 WHERE c.relname = '{}' \
                 AND i.indrelid = 'harvest_events_reindex_decoy_958'::regclass) AS v",
                plan_pk_index()
            ),
        )
        .await,
        "the unrelated decoy index must survive untouched, on its own table"
    );

    diesel::sql_query("DROP TABLE harvest_events_reindex_decoy_958 CASCADE")
        .execute(&mut conn)
        .await
        .expect("drop the decoy table and its squatting index");
}

#[tokio::test]
async fn phase_2_refuses_a_same_table_same_columns_index_with_different_semantics() {
    // Review finding: the expected table and column list are not the
    // whole shape either. An operator's own invalid index could sit at
    // a reserved name, on the right table with the right columns. It
    // could still carry different semantics -- non-unique, here, in
    // place of harvest's own UNIQUE. It must not be treated as this
    // plan's own remnant.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE INDEX {} ON harvest_events (id, cohort)",
        plan_pk_index()
    ))
    .execute(&mut conn)
    .await
    .expect("seed a non-unique index at the reserved name and columns");
    invalidate_index(&mut conn, &plan_pk_index()).await;

    run_plan_phases(&mut conn, 1..=1).await;

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 2)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 2 must refuse a same-table, same-column index with different semantics, \
         not treat it as this plan's own remnant",
    );
    assert!(
        msg.contains(&plan_pk_index()),
        "the refusal must name the offending index; got {msg}"
    );
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pg_class c \
                 JOIN pg_index i ON i.indexrelid = c.oid \
                 WHERE c.relname = '{}' AND NOT i.indisunique) AS v",
                plan_pk_index()
            ),
        )
        .await,
        "the operator's non-unique index must survive untouched"
    );

    diesel::sql_query(format!("DROP INDEX {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the decoy index");
}

#[tokio::test]
async fn phase_2_refuses_an_invalid_nulls_not_distinct_impostor_of_the_pk_index() {
    // Review finding: `NULLS NOT DISTINCT` is checked through
    // `pg_get_indexdef`, not `pg_index.indnullsnotdistinct` directly --
    // that column does not exist before PostgreSQL 15, and
    // `docs/partitioned-events.md` still supports 14. This also proves
    // the check works, not merely that it compiles. An invalid index
    // could sit at the reserved pk-index name, on the right table with
    // the right columns, but `NULLS NOT DISTINCT`. It must still refuse
    // rather than being treated as this plan's own remnant.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE UNIQUE INDEX {} ON harvest_events (id, cohort) NULLS NOT DISTINCT",
        plan_pk_index()
    ))
    .execute(&mut conn)
    .await
    .expect("seed an impostor index with the pk-index shape and columns, but NULLS NOT DISTINCT");
    invalidate_index(&mut conn, &plan_pk_index()).await;

    run_plan_phases(&mut conn, 1..=1).await;

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 2)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 2 must refuse an invalid NULLS NOT DISTINCT impostor of the pk index, \
         not treat it as this plan's own remnant",
    );
    assert!(
        msg.contains(&plan_pk_index()),
        "the refusal must name the offending index; got {msg}"
    );
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT pg_get_indexdef(i.indexrelid) ILIKE '%NULLS NOT DISTINCT%' AS v \
                 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid \
                 WHERE c.relname = '{}'",
                plan_pk_index()
            ),
        )
        .await,
        "the operator's NULLS NOT DISTINCT impostor must survive untouched"
    );

    diesel::sql_query(format!("DROP INDEX {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the impostor index");
}

#[tokio::test]
async fn the_lock_window_refuses_to_open_over_an_invalid_index() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "inv_wf", "inv-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    run_plan_phases(&mut conn, 1..=3).await;
    invalidate_index(&mut conn, &plan_pk_index()).await;

    // An operator who lost a build, fixed something else, and picked the
    // runbook back up at the lock window. Phase 4 must refuse rather than take
    // ACCESS EXCLUSIVE and then discover it has an index to build.
    let mut failed = false;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        if diesel::sql_query(&step.sql)
            .execute(&mut conn)
            .await
            .is_err()
        {
            failed = true;
        }
    }
    assert!(
        failed,
        "phase 4 must abort before the rename when an index it depends on is invalid"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "and it must abort BEFORE changing anything — the table is still flat"
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(exec))
            .await
            .expect("history is untouched by the refused conversion")
            .events
            .len(),
        3
    );
}

#[tokio::test]
async fn phase_4_refuses_a_valid_index_with_the_right_name_and_the_wrong_shape() {
    // Issue #1270 item 11: phase 4 asserted only the NAME and `indisvalid`
    // of the two phase-2 indexes. An operator's own pre-existing, valid
    // index can hold one of those two fixed names. That makes `CREATE ...
    // IF NOT EXISTS` in phase 2 silently skip building the real one. `IF
    // NOT EXISTS` checks only for a name collision, not definition
    // compatibility. The old assertion counted that impostor as ready.
    // `ATTACH PARTITION` would then build a real replacement inside the
    // window this plan advertises as metadata-only.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "shape_wf", "shape-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    // The impostor: right name, valid, but the wrong columns and not even
    // unique.
    let pk = plan_pk_index();
    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!("CREATE INDEX {pk} ON harvest_events (event_type)"))
        .execute(&mut conn)
        .await
        .expect("seed an impostor index with the pk-index name but the wrong shape");
    assert!(
        index_is_valid(&mut conn, &pk).await,
        "precondition: the impostor is VALID, not merely present"
    );

    run_plan_phases(&mut conn, 1..=3).await;

    let mut failure: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        // Phase 4's steps share one transaction (`BEGIN` ... `COMMIT`).
        // Once the assertion raises, every later step also errors with
        // Postgres's generic "current transaction is aborted". That is
        // not the signal, so only the FIRST error is kept. The loop still
        // runs to the trailing `COMMIT`. Postgres always accepts that,
        // even mid-abort — it closes the block, as a rollback would.
        // Skipping it here would leave `conn` mid-transaction for the
        // cleanup statements below.
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && failure.is_none()
        {
            failure = Some(e.to_string());
        }
    }
    let msg = failure.expect(
        "phase 4 must refuse when a valid index of the right name has the \
         wrong shape, not count it as the real one",
    );
    // Not just "some phase-4 step failed": the impostor must be the reason.
    // Only the legitimate `exec_event_idx` counts, so the assertion must
    // report exactly 1 of 2 correctly-shaped indexes, not fail for some
    // unrelated reason.
    assert!(
        msg.contains("phase 2 left 1 of 2 valid, correctly-shaped"),
        "the refusal must report exactly one correctly-shaped index found; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    // Phases 1-3 each commit on their own (unlike phase 4). So the
    // impostor, the real index phase 2 built alongside it, and phase 3's
    // CHECK constraint all survive this refusal. Clean them up: a later
    // test's `reset_to_unpartitioned` only reverses a PARTITIONED shard.
    // Debris left on a still-flat table would otherwise leak into it.
    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("drop the impostor index");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_refuses_a_nulls_not_distinct_impostor_of_the_pk_index() {
    // Review finding: the shape check verified uniqueness and the exact
    // key columns, but not `indnullsnotdistinct`. Harvest's own phase-2
    // indexes are ordinary ones -- `NULLS DISTINCT`, the default. An
    // operator's own `UNIQUE NULLS NOT DISTINCT` index under the
    // reserved pk-index name, with the exact right columns otherwise,
    // still passed every other check. `ATTACH PARTITION` requires that
    // property to match the parent's index. Phase 4 would then find
    // this impostor unattachable. It would build a replacement under
    // `ACCESS EXCLUSIVE` instead -- exactly the unplanned rebuild this
    // assertion exists to catch in advance.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "nnd_wf", "nnd-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    let pk = plan_pk_index();
    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE UNIQUE INDEX {pk} ON harvest_events (id, cohort) NULLS NOT DISTINCT"
    ))
    .execute(&mut conn)
    .await
    .expect(
        "seed an impostor index with the pk-index name, shape and columns, but NULLS NOT DISTINCT",
    );
    assert!(
        index_is_valid(&mut conn, &pk).await,
        "precondition: the impostor is VALID, not merely present"
    );

    run_plan_phases(&mut conn, 1..=3).await;

    let mut failure: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && failure.is_none()
        {
            failure = Some(e.to_string());
        }
    }
    let msg = failure.expect(
        "phase 4 must refuse when a valid, correctly-shaped index of the right \
         name is NULLS NOT DISTINCT, not count it as the real one",
    );
    assert!(
        msg.contains("phase 2 left 1 of 2 valid, correctly-shaped"),
        "the refusal must report exactly one correctly-shaped index found; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("drop the impostor index");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_refuses_a_differently_ordered_impostor_of_the_pk_index() {
    // Review finding: the shape check verified the key columns and their
    // positions, but not their sort direction. Harvest's own phase-2
    // indexes sort every key column in plain ascending order. An
    // operator's own index under the reserved pk-index name, with the
    // exact right columns and positions but one sorted `DESC`, still
    // passed every other check. `ATTACH PARTITION` requires matching
    // sort direction per column, so phase 4 would find this impostor
    // unattachable. It would build a replacement under `ACCESS
    // EXCLUSIVE` instead -- exactly the unplanned rebuild this assertion
    // exists to catch in advance.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "desc_wf", "desc-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    let pk = plan_pk_index();
    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(format!(
        "CREATE UNIQUE INDEX {pk} ON harvest_events (id DESC, cohort)"
    ))
    .execute(&mut conn)
    .await
    .expect("seed an impostor index with the pk-index name, columns and shape, but id DESC");
    assert!(
        index_is_valid(&mut conn, &pk).await,
        "precondition: the impostor is VALID, not merely present"
    );

    run_plan_phases(&mut conn, 1..=3).await;

    let mut failure: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && failure.is_none()
        {
            failure = Some(e.to_string());
        }
    }
    let msg = failure.expect(
        "phase 4 must refuse when a valid, correctly-shaped index of the right \
         name sorts a key column DESC, not count it as the real one",
    );
    assert!(
        msg.contains("phase 2 left 1 of 2 valid, correctly-shaped"),
        "the refusal must report exactly one correctly-shaped index found; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    diesel::sql_query(format!("DROP INDEX IF EXISTS {pk}"))
        .execute(&mut conn)
        .await
        .expect("drop the impostor index");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_rechecks_dependent_views_created_after_phase_1() {
    // Review finding: phase 1's dependent-view check runs hours before
    // phase 4's rename, under this plan's online path. A view created (or
    // repointed) at `harvest_events` in that gap would pass phase 1 clean,
    // then silently go stale after the rename anyway. Phase 4 must repeat
    // the identical check under the lock it actually converts within.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    // Simulate a view showing up in the gap between phase 1 and phase 4.
    diesel::sql_query("DROP VIEW IF EXISTS harvest_events_late_view_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray view from a previous run");
    diesel::sql_query(
        "CREATE VIEW harvest_events_late_view_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut conn)
    .await
    .expect("seed a view created after phase 1's check already passed");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        // Phase 4's steps share one transaction (`BEGIN` ... `COMMIT`).
        // Once the recheck raises, every later step also errors with
        // Postgres's generic "current transaction is aborted". That is
        // not the signal, so only the FIRST error is kept. The loop still
        // runs to the trailing `COMMIT`. Postgres always accepts that,
        // even mid-abort — it closes the block, as a rollback would.
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && refusal.is_none()
        {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 4 must refuse a view that started depending on harvest_events \
         after phase 1's check already passed",
    );
    assert!(
        msg.contains("harvest_events_late_view_958"),
        "the refusal must name the offending view; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    // Phases 1-3 each commit on their own (unlike phase 4). So the two
    // phase-2 indexes and phase 3's CHECK constraint all survive this
    // refusal. Clean them up: a later test's `reset_to_unpartitioned`
    // only reverses a PARTITIONED shard, not debris on a still-flat one.
    diesel::sql_query("DROP VIEW harvest_events_late_view_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending view");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn a_row_security_policy_on_a_leaf_partition_directly_still_refuses_the_revert() {
    // Review finding: `row_security_config` matched only the PARENT name
    // `harvest_events`. Postgres lets an operator enable row security or
    // attach a policy directly on a LEAF partition instead. Such a
    // configuration used to pass this check unnoticed.
    // `disable_partitioning`'s copy-and-`DROP ... CASCADE` then silently
    // dropped the protection along with the leaf it was attached to.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(
        &mut conn,
        "leaf_rls_wf",
        "leaf-rls-1",
        day(2026, 2, 3),
        None,
    )
    .await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    diesel::sql_query(format!(
        "ALTER TABLE {} ENABLE ROW LEVEL SECURITY",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("enable row security directly on a leaf partition");
    diesel::sql_query(format!(
        "DROP POLICY IF EXISTS harvest_events_leaf_rls_958 ON {}",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("clear any stray policy from a previous run");
    diesel::sql_query(format!(
        "CREATE POLICY harvest_events_leaf_rls_958 ON {} USING (true)",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("seed a policy attached directly to a leaf partition");

    let err = partition::disable_partitioning(&mut conn).await.expect_err(
        "row security configured directly on a leaf partition must refuse the revert \
         too, not only row security on the parent",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_leaf_rls_958"),
        "the refusal must name the offending policy; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "a refused revert must leave the shard exactly as it was"
    );

    diesel::sql_query(format!(
        "DROP POLICY harvest_events_leaf_rls_958 ON {}",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the offending policy");
    diesel::sql_query(format!(
        "ALTER TABLE {} DISABLE ROW LEVEL SECURITY",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("disable row security on the leaf partition");
}

#[tokio::test]
async fn enable_sql_rechecks_row_security_enabled_after_the_preflight() {
    // Review finding: the phase-4 (and direct-path) in-lock recheck block
    // rechecked unique indexes, views and triggers, but not row
    // security. `CREATE TABLE ... (LIKE ...)` copies neither
    // `relrowsecurity`/`relforcerowsecurity` nor any `pg_policy` row, the
    // identical gap `refuse_if_row_security` exists to close before the
    // lock. This test calls `enable_sql` directly, bypassing the Rust
    // preflight entirely, to prove the script also refuses for itself
    // once it holds the lock.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "race_rls_wf", "race-rls-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("ALTER TABLE harvest_events ENABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("enable row security as if configured in the gap after the preflight check");
    diesel::sql_query("DROP POLICY IF EXISTS harvest_events_race_rls_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray policy from a previous run");
    diesel::sql_query("CREATE POLICY harvest_events_race_rls_958 ON harvest_events USING (true)")
        .execute(&mut conn)
        .await
        .expect("seed a policy as if added in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_race_rls_958"),
        "the refusal must name the offending policy; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP POLICY harvest_events_race_rls_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending policy");
    diesel::sql_query("ALTER TABLE harvest_events DISABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("disable row security");
}

#[tokio::test]
async fn enable_sql_rechecks_a_publication_added_after_the_preflight() {
    // Review finding: the in-lock recheck block had no publication check
    // at all, unlike the Rust preflight `enable_partitioning` runs
    // first. See `incompatible_publications` for why a leaf-publishing
    // publication silently stops a logical-replication standby. This
    // test calls `enable_sql` directly, bypassing that Rust preflight
    // entirely, to prove the script also refuses for itself once it
    // holds the lock.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "race_pub_wf", "race-pub-1", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");

    diesel::sql_query("DROP PUBLICATION IF EXISTS harvest_events_race_pub_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray publication from a previous run");
    diesel::sql_query("CREATE PUBLICATION harvest_events_race_pub_958 FOR TABLE harvest_events")
        .execute(&mut conn)
        .await
        .expect("seed a publication as if added in the gap after the preflight check");

    let err = diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        &partition::enable_sql(&EnableOptions::default()),
    )
    .await
    .expect_err(
        "enable_sql must refuse under its own lock, not rely solely on a check made \
         before the script started",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("harvest_events_race_pub_958"),
        "the refusal must name the offending publication; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused conversion must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP PUBLICATION harvest_events_race_pub_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending publication");
}

#[tokio::test]
async fn phase_4_rechecks_row_security_enabled_after_phase_1() {
    // Review finding: phase 1's row-security check runs hours before
    // phase 4's rename, under this plan's online path. Row security
    // enabled (or a policy added) at `harvest_events` in that gap would
    // pass phase 1 clean. It would then be silently dropped by phase
    // 4's `CREATE TABLE ... (LIKE ...)` anyway. Phase 4 must repeat the
    // identical check under the lock it actually converts within.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    diesel::sql_query("ALTER TABLE harvest_events ENABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("enable row security after phase 1's check already passed");
    diesel::sql_query("DROP POLICY IF EXISTS harvest_events_late_rls_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray policy from a previous run");
    diesel::sql_query("CREATE POLICY harvest_events_late_rls_958 ON harvest_events USING (true)")
        .execute(&mut conn)
        .await
        .expect("seed a policy created after phase 1's check already passed");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && refusal.is_none()
        {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 4 must refuse row security enabled on harvest_events after phase 1's \
         check already passed",
    );
    assert!(
        msg.contains("harvest_events_late_rls_958"),
        "the refusal must name the offending policy; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP POLICY harvest_events_late_rls_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending policy");
    diesel::sql_query("ALTER TABLE harvest_events DISABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("disable row security");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_rechecks_a_publication_added_after_phase_1() {
    // Review finding: the phase-4 in-lock recheck block had no
    // publication check at all, unlike phase 1's. A publication created
    // (or repointed) in the hours-long gap before phase 4 would pass
    // phase 1 clean. It would still break a logical-replication standby
    // after this plan's cutover.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    diesel::sql_query("DROP PUBLICATION IF EXISTS harvest_events_late_pub_958")
        .execute(&mut conn)
        .await
        .expect("clear any stray publication from a previous run");
    diesel::sql_query("CREATE PUBLICATION harvest_events_late_pub_958 FOR TABLE harvest_events")
        .execute(&mut conn)
        .await
        .expect("seed a publication created after phase 1's check already passed");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && refusal.is_none()
        {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 4 must refuse a publication covering harvest_events added after phase \
         1's check already passed",
    );
    assert!(
        msg.contains("harvest_events_late_pub_958"),
        "the refusal must name the offending publication; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    diesel::sql_query("DROP PUBLICATION harvest_events_late_pub_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending publication");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_holds_access_exclusive_before_the_dependent_view_recheck() {
    // Review finding: the recheck above is only as good as the lock it
    // runs under. `BEGIN` and `SET LOCAL lock_timeout` take no lock on
    // `harvest_events` at all. Without an explicit `LOCK TABLE`, a
    // concurrent `CREATE VIEW` could still commit after the recheck ran.
    // It could still commit before the actual rename several steps
    // later. That is the exact race the recheck exists to close. Prove
    // the lock is real: hold phase 4 open right after its `LOCK TABLE`
    // step. Then show a concurrent `CREATE VIEW` blocks, rather than
    // commits, while it is held.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    run_plan_phases(&mut conn, 1..=3).await;

    let steps: Vec<_> = partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
        .collect();
    let lock_pos = steps
        .iter()
        .position(|s| {
            s.sql
                .contains("LOCK TABLE harvest_events IN ACCESS EXCLUSIVE MODE")
        })
        .expect("phase 4 must have an explicit LOCK TABLE step before the recheck");

    for step in &steps[..=lock_pos] {
        diesel::sql_query(&step.sql)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("phase 4 step up to the lock: {e}"));
    }

    // A second session's `CREATE VIEW` takes only ACCESS SHARE on
    // `harvest_events`, which conflicts with the ACCESS EXCLUSIVE the
    // first session is holding. A short `statement_timeout` turns
    // "blocks" into an observable failure instead of an indefinite hang.
    let mut racer = connect(&url).await;
    diesel::sql_query("SET statement_timeout = '500ms'")
        .execute(&mut racer)
        .await
        .expect("set a short timeout on the racer");
    let err = diesel::sql_query(
        "CREATE VIEW harvest_events_lock_race_958 AS SELECT event_type FROM harvest_events",
    )
    .execute(&mut racer)
    .await
    .expect_err(
        "a concurrent CREATE VIEW must block on the held ACCESS EXCLUSIVE lock, \
         not commit while phase 4 is mid-flight",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("statement timeout") || msg.contains("canceling statement"),
        "must fail because it was blocked waiting on the lock, not some other reason; \
         got {msg}"
    );

    // Roll back the still-open phase-4 transaction so it does not leave
    // the shard mid-conversion for a later test.
    diesel::sql_query("ROLLBACK")
        .execute(&mut conn)
        .await
        .expect("roll back the held-open phase-4 transaction");

    // Phases 1-3 each commit on their own (unlike phase 4). Clean up
    // their artifacts exactly like the sibling recheck test above.
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_rechecks_an_operator_trigger_installed_after_phase_1() {
    // Review finding: phase 4 got a dependent-view recheck but no
    // equivalent operator-trigger recheck. Phase 1's trigger guard has
    // the identical hours-long gap to phase 4's rename, though. An
    // operator trigger installed in that gap would pass phase 1 clean,
    // then silently stop firing for every new row after cutover.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    // Simulate a trigger installed in the gap between phase 1 and phase 4.
    diesel::sql_query("DROP TRIGGER IF EXISTS harvest_events_late_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger from a previous run");
    diesel::sql_query("DROP FUNCTION IF EXISTS harvest_events_late_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("clear any stray trigger function from a previous run");
    diesel::sql_query(
        "CREATE FUNCTION harvest_events_late_trg_958_fn() RETURNS trigger AS $$ \
         BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger function");
    diesel::sql_query(
        "CREATE TRIGGER harvest_events_late_trg_958 BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION harvest_events_late_trg_958_fn()",
    )
    .execute(&mut conn)
    .await
    .expect("seed a trigger installed after phase 1's check already passed");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        // Phase 4's steps share one transaction. Only the FIRST error is
        // the signal; the loop still runs to the trailing `COMMIT`, which
        // Postgres always accepts even mid-abort.
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && refusal.is_none()
        {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal
        .expect("phase 4 must refuse a trigger installed after phase 1's check already passed");
    assert!(
        msg.contains("harvest_events_late_trg_958"),
        "the refusal must name the offending trigger; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    // Phases 1-3 each commit on their own (unlike phase 4). Clean up
    // their artifacts exactly like the sibling recheck test above.
    diesel::sql_query("DROP TRIGGER harvest_events_late_trg_958 ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger");
    diesel::sql_query("DROP FUNCTION harvest_events_late_trg_958_fn()")
        .execute(&mut conn)
        .await
        .expect("drop the offending trigger function");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn phase_4_rechecks_a_constraint_backed_unique_index_installed_after_phase_1() {
    // Review finding (Codex, on the phase-4 view/trigger rechecks above):
    // phase 1's unique-index guard has the identical hours-long gap. An
    // operator could add a constraint-backed unique index — one that even
    // includes `cohort` — in that gap. It would pass phase 1 clean, since
    // phase 1 already ran. `capture_index_defs` still excludes every
    // constraint-backed index unconditionally, though, so phase 4 would
    // silently drop it instead of refusing.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    // Simulate a constraint-backed unique index added in the gap between
    // phase 1 and phase 4.
    diesel::sql_query(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS uq_late_event_type_958",
    )
    .execute(&mut conn)
    .await
    .expect("clear any stray constraint from a previous run");
    diesel::sql_query(
        "ALTER TABLE harvest_events ADD CONSTRAINT uq_late_event_type_958 \
         UNIQUE (id, cohort)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a constraint-backed unique index added after phase 1's check already passed");

    let mut refusal: Option<String> = None;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 4)
    {
        // Phase 4's steps share one transaction. Only the FIRST error is
        // the signal; the loop still runs to the trailing `COMMIT`, which
        // Postgres always accepts even mid-abort.
        if let Err(e) = diesel::sql_query(&step.sql).execute(&mut conn).await
            && refusal.is_none()
        {
            refusal = Some(e.to_string());
        }
    }
    let msg = refusal.expect(
        "phase 4 must refuse a constraint-backed unique index added after phase 1's \
         check already passed",
    );
    assert!(
        msg.contains("uq_late_event_type_958"),
        "the refusal must name the offending index; got {msg}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "a refused window must leave the shard exactly as it was"
    );

    // Phases 1-3 each commit on their own (unlike phase 4). Clean up
    // their artifacts exactly like the sibling recheck tests above.
    diesel::sql_query("ALTER TABLE harvest_events DROP CONSTRAINT uq_late_event_type_958")
        .execute(&mut conn)
        .await
        .expect("drop the offending constraint");
    diesel::sql_query(format!("DROP INDEX IF EXISTS {}", plan_pk_index()))
        .execute(&mut conn)
        .await
        .expect("drop the index phase 2 legitimately built");
    diesel::sql_query(format!(
        "DROP INDEX IF EXISTS {}_exec_event_idx",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the other index phase 2 legitimately built");
    diesel::sql_query(format!(
        "ALTER TABLE harvest_events DROP CONSTRAINT IF EXISTS {}_cohort_ck",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("drop the constraint phase 3 legitimately built");
}

#[tokio::test]
async fn converting_refuses_while_any_publication_covers_harvest_events() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // The exact logical-DR setup `docs/cross-region-dr.md` tells an operator to
    // build. Converting the publisher under it breaks the standby in TWO
    // independent ways, and only the first looks like a configuration problem.
    //
    // 1. `publish_via_partition_root` defaults to false, so a partitioned
    //    table's rows publish under their LEAF names — which the standby, whose
    //    schema came from the inert migrations, has no tables for. The apply
    //    worker stops on the first event.
    //
    // 2. Reclamation stops producing DELETEs at all. The partitioned layout
    //    drops the `ON DELETE CASCADE` foreign key, so deleting an execution no
    //    longer deletes its events, and the rows go away by DROP TABLE — DDL,
    //    which logical replication does not carry. The subscriber's own
    //    cascade cannot cover for that either: apply runs with replica trigger
    //    behaviour, where referential-integrity triggers do not fire. So the
    //    standby's `harvest_events` keeps every event forever, and
    //    `DanglingEventExecution` is an *Incoherent* finding — "do not start
    //    workers". The standby is no longer failover-capable, which is the one
    //    thing it exists for.
    diesel::sql_query("DROP PUBLICATION IF EXISTS harvest_dr_958")
        .execute(&mut conn)
        .await
        .expect("clean slate");
    diesel::sql_query("CREATE PUBLICATION harvest_dr_958 FOR ALL TABLES")
        .execute(&mut conn)
        .await
        .expect("create the documented publication");

    let err = match partition::enable_partitioning(&mut conn, &EnableOptions::default()).await {
        Err(e) => e.to_string(),
        Ok(report) => {
            panic!("enabling must refuse while a publication covers harvest_events, got {report:?}")
        }
    };
    assert!(
        err.contains("harvest_dr_958") && err.contains("publish_via_partition_root"),
        "the refusal must name the publication and the naming remedy: {err}"
    );
    assert_eq!(
        events_relkind(&mut conn).await,
        "r",
        "and it must refuse BEFORE converting anything"
    );

    // Publishing through the partition root fixes the naming half and NOTHING
    // about the deletes, so it must not on its own unlock the conversion. This
    // is the trap: `pubviaroot` looks like the whole answer, the subscription
    // keeps applying, and the standby quietly stops being restorable.
    diesel::sql_query("ALTER PUBLICATION harvest_dr_958 SET (publish_via_partition_root = true)")
        .execute(&mut conn)
        .await
        .expect("set publish_via_partition_root");
    let err = match partition::enable_partitioning(&mut conn, &EnableOptions::default()).await {
        Err(e) => e.to_string(),
        Ok(report) => panic!(
            "publish_via_partition_root fixes the leaf-name problem only — reclamation              still becomes DROP TABLE, which is never replicated, so a flat subscriber              accumulates dangling events and reports Incoherent. Got {report:?}"
        ),
    };
    assert!(
        err.contains("harvest_dr_958"),
        "the refusal must still name the publication: {err}"
    );
    assert_eq!(events_relkind(&mut conn).await, "r");

    // The override is the operator's statement that they have dealt with both
    // halves — typically by running the partitioned layout on the subscriber
    // too, where its own maintenance can reclaim once promoted.
    partition::enable_partitioning(
        &mut conn,
        &EnableOptions {
            allow_incompatible_publications: true,
            ..EnableOptions::default()
        },
    )
    .await
    .expect("the override must let an operator who understands it proceed");
    assert_eq!(events_relkind(&mut conn).await, "p");

    diesel::sql_query("DROP PUBLICATION harvest_dr_958")
        .execute(&mut conn)
        .await
        .expect("drop the publication");
}

#[tokio::test]
async fn the_online_phase_is_resumable_after_its_validation_fails() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    run_plan_phases(&mut conn, 1..=3).await;

    // Phase 3 is two transactions now, because the second one — the validation
    // scan — must be able to fail fast rather than queue every append behind
    // it. So the half-done state is reachable by design: the constraint is
    // added and NOT validated, and the runbook's answer is "clear the blocker
    // and re-run step 3".
    diesel::sql_query(format!(
        "UPDATE pg_constraint SET convalidated = false
          WHERE conname = '{}_cohort_ck'",
        partition::LEGACY_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("simulate a validation that timed out after the constraint landed");

    run_plan_phases(&mut conn, 3..=3).await;
    assert!(
        scalar_bool(
            &mut conn,
            &format!(
                "SELECT COALESCE((SELECT convalidated FROM pg_constraint
                                   WHERE conname = '{}_cohort_ck'), false) AS v",
                partition::LEGACY_PARTITION
            ),
        )
        .await,
        "re-running phase 3 must finish the validation. An unconditional \
         ADD CONSTRAINT fails with duplicate_object before it ever gets there, \
         so bounding the phase's lock wait would have traded a hang for a dead \
         end: the operator can neither re-run the step nor regenerate the plan, \
         which bakes in the same constraint name."
    );
}

#[test]
fn the_inert_migration_builds_no_index_on_the_executions_table() {
    // The migration is documented as inert on apply: a deployment that never
    // opts in keeps its ordinary table and its behaviour. A plain
    // `CREATE INDEX` breaks that promise in the one way that matters
    // operationally — it holds SHARE on `harvest_workflow_executions` for the
    // whole build, which conflicts with the ROW EXCLUSIVE every insert, state
    // update and retention delete takes. On a large deployment every
    // execution-state write stops for the duration, to build an index that
    // exists solely for a feature they may never enable.
    //
    // The index is only needed once the sweeper exists, so it is built by the
    // enable path, and CONCURRENTLY by the plan for the large tables where the
    // difference is felt.
    let up = include_str!("../../migrations/20260901115500_harvest_event_partitioning/up.sql");
    let statements: String = up
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !statements.contains("CREATE INDEX"),
        "the inert migration must not build an index; it would block every \
         write to the table it indexes for the length of the build"
    );
}

#[test]
fn the_down_migration_does_not_drop_an_index_it_never_created() {
    // Issue #1270 item 13: `down.sql` used to run
    // `DROP INDEX IF EXISTS idx_harvest_we_created_at` unconditionally.
    // `up.sql` never creates that index. `harvest partition enable` (and
    // `plan`) does, as part of opting in. Consider an operator who had
    // created an index of that name themselves, before ever opting in.
    // `harvest_workflow_executions (created_at)` is an ordinary index to
    // want. Rolling back an otherwise inert migration would delete it.
    let down = include_str!("../../migrations/20260901115500_harvest_event_partitioning/down.sql");
    let statements: String = down
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !statements.contains("idx_harvest_we_created_at"),
        "rollback of this migration must be as inert as its forward \
         direction: it must not remove an index it never built"
    );
}

#[tokio::test]
async fn disabling_removes_the_index_enabling_created() {
    // The other half of the item 13 fix. `harvest partition enable` (and
    // `plan`) creates `idx_harvest_we_created_at` as part of opting in.
    // `harvest partition disable` -- the reverse operation -- is the path
    // that removes it again, symmetrically. `down.sql` deliberately no
    // longer touches it (see the previous test).
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "precondition: enable created the index"
    );

    partition::disable_partitioning(&mut conn)
        .await
        .expect("disable")
        .expect("the shard was partitioned, so disable must report a revert");

    assert!(
        !scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "disable must remove the index enable created"
    );
}

#[tokio::test]
async fn disable_does_not_drop_an_operator_index_of_the_same_name_but_a_different_shape() {
    // Review finding: `enable`'s `CREATE INDEX IF NOT EXISTS` leaves an
    // operator's own pre-existing index of this exact name untouched, so
    // it never becomes harvest's to remove. The old unconditional `DROP
    // INDEX IF EXISTS idx_harvest_we_created_at` in `disable` deleted it
    // anyway -- the same name-collision hazard item 13 fixed in
    // `down.sql`, still present here.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS idx_harvest_we_created_at")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    // A UNIQUE index: a shape `enable` never creates for this name, so it
    // cannot be mistaken for the one `enable` builds.
    diesel::sql_query(
        "CREATE UNIQUE INDEX idx_harvest_we_created_at ON harvest_workflow_executions (id)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator index under the same name");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "precondition: enable's CREATE INDEX IF NOT EXISTS must leave the \
         operator's index in place, not replace it"
    );

    partition::disable_partitioning(&mut conn)
        .await
        .expect("disable")
        .expect("the shard was partitioned, so disable must report a revert");

    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "disable must not drop an operator's own index just because it \
         shares enable's index name"
    );

    diesel::sql_query("DROP INDEX idx_harvest_we_created_at")
        .execute(&mut conn)
        .await
        .expect("drop the operator's index");
}

#[tokio::test]
async fn disable_does_not_drop_an_identically_shaped_operator_index_either() {
    // Second review finding on the same fix. A shape check alone cannot
    // tell harvest's index apart from an operator's own index of the
    // IDENTICAL shape. A plain btree on
    // harvest_workflow_executions (created_at) is an ordinary thing to
    // build independently. `enable`'s CREATE INDEX IF NOT EXISTS leaves
    // it untouched and does not tag it. Disable must not drop it either,
    // even though the shape check alone would have said yes.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("DROP INDEX IF EXISTS idx_harvest_we_created_at")
        .execute(&mut conn)
        .await
        .expect("clear any stray index from a previous run");
    diesel::sql_query(
        "CREATE INDEX idx_harvest_we_created_at ON harvest_workflow_executions (created_at)",
    )
    .execute(&mut conn)
    .await
    .expect("seed an operator index with the exact shape enable creates");

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "precondition: enable's CREATE INDEX IF NOT EXISTS must leave the \
         operator's index in place, not replace it"
    );

    partition::disable_partitioning(&mut conn)
        .await
        .expect("disable")
        .expect("the shard was partitioned, so disable must report a revert");

    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v"
        )
        .await,
        "disable must not drop an operator's own index just because its \
         shape happens to match the one enable would have built -- only \
         the ownership tag settles that"
    );

    diesel::sql_query("DROP INDEX idx_harvest_we_created_at")
        .execute(&mut conn)
        .await
        .expect("drop the operator's index");
}

#[tokio::test]
async fn enabling_creates_the_drop_gate_index_the_migration_no_longer_ships() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // Without it the sweeper's tier-1 probe — the one that makes the steady
    // state O(1) — is a sequential scan of the executions table, per cohort,
    // per tick. Moving the build off the migration must not lose it.
    assert!(
        scalar_bool(
            &mut conn,
            "SELECT to_regclass('idx_harvest_we_created_at') IS NOT NULL AS v",
        )
        .await,
        "the enable path must build the drop gate's index"
    );
}

#[tokio::test]
async fn maintenance_says_what_is_wrong_when_the_runtime_role_cannot_own_partitions() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // The split-role topology the preflight check exists for: migrations and
    // `harvest partition enable` run as the owning role, the engine connects as
    // a separately granted one that needs only SELECT/INSERT on
    // `harvest_events`.
    //
    // Every maintenance operation is DDL on that table — CREATE TABLE ...
    // PARTITION OF for the lookahead window, DETACH/ATTACH for the drain, DROP
    // TABLE for reclamation — and Postgres requires OWNERSHIP for all of it. So
    // on this topology automatic maintenance fails every tick: the lookahead
    // window stops being extended, appends pile into DEFAULT, and nothing is
    // ever reclaimed. The feature does not work at all.
    //
    // It has to say so in terms an operator can act on. A raw
    // `permission denied` names neither the role that must be granted nor the
    // grant that fixes it.
    for stmt in [
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'harvest_rt_maint_958') \
           THEN CREATE ROLE harvest_rt_maint_958 LOGIN PASSWORD 'maint958'; END IF; END $$",
        "GRANT USAGE ON SCHEMA public TO harvest_rt_maint_958",
        "GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO harvest_rt_maint_958",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable as the owning role");

    let runtime_url = url.replace(
        "postgres://postgres:postgres@",
        "postgres://harvest_rt_maint_958:maint958@",
    );
    let mut runtime = connect(&runtime_url).await;

    let err = partition::maintain(
        &mut runtime,
        Utc::now(),
        partition::DEFAULT_LOOKAHEAD_COHORTS,
        &SweepOptions::default(),
        None,
    )
    .await;
    let msg = match err {
        Err(e) => e.to_string(),
        Ok(outcome) => outcome.last_error.clone().unwrap_or_else(|| {
            panic!(
                "maintenance cannot succeed as a role that does not own harvest_events \
                 — every partition operation is DDL requiring ownership. It reported {outcome:?}"
            )
        }),
    };
    assert!(
        msg.contains("harvest_rt_maint_958") || msg.contains("own"),
        "the failure must name the ownership problem, not just relay a raw \
         permission error: {msg}"
    );
    assert!(
        msg.contains("GRANT"),
        "and it must name the grant that fixes it, so an operator is not left \
         to work out that role membership is what Postgres checks: {msg}"
    );

    // Membership in the owning role is what Postgres actually checks for
    // ownership, so this is the fix — and it must genuinely make maintenance
    // work, not merely quiet the message.
    diesel::sql_query("GRANT postgres TO harvest_rt_maint_958")
        .execute(&mut conn)
        .await
        .expect("grant the owning role");
    let mut runtime = connect(&runtime_url).await;
    let outcome = partition::maintain(
        &mut runtime,
        Utc::now(),
        partition::DEFAULT_LOOKAHEAD_COHORTS,
        &SweepOptions::default(),
        None,
    )
    .await
    .expect("maintenance must work once the runtime role owns the table");
    assert_eq!(
        outcome.last_error, None,
        "and it must report clean: {outcome:?}"
    );
}

#[tokio::test]
async fn a_drop_blocked_by_a_reader_does_not_queue_appends_behind_its_upgrade() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = seed_expired(&mut conn, "up_wf", "up-old", day(2026, 1, 5)).await;
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec)
        .execute(&mut conn)
        .await
        .expect("collect the execution so the cohort is reclaimable");
    let target =
        diesel::sql_query("SELECT tableoid::regclass::text AS v FROM harvest_events LIMIT 1")
            .get_result::<TextRow>(&mut conn)
            .await
            .expect("locate the partition holding the seeded rows")
            .v;

    // One long history query still reading the partition being dropped. It
    // holds ACCESS SHARE, which does NOT conflict with the SHARE the re-check
    // takes — so the drop gets its lock, proves the partition reclaimable, and
    // only then has to upgrade to ACCESS EXCLUSIVE for the DROP itself.
    //
    // That upgrade is where the damage is. Postgres queues a new request behind
    // an existing WAITER it conflicts with, not just behind held locks, so
    // every append arriving while the upgrade waits queues behind it — the
    // insert trigger's cross-partition uniqueness probe reads the parent and
    // takes ACCESS SHARE on every child, so it conflicts whatever cohort the
    // append belongs to. Bounding the upgrade by `lock_timeout` would stall the
    // entire shard for that long, per drop attempt, on every tick.
    let mut reader = connect(&url).await;
    for stmt in [
        "BEGIN".to_string(),
        format!("SELECT count(*) AS n FROM {target}"),
    ] {
        diesel::sql_query(&stmt)
            .execute(&mut reader)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    let sweep_url = url.clone();
    let sweeper = tokio::spawn(async move {
        let mut c = connect(&sweep_url).await;
        partition::sweep(
            &mut c,
            Utc::now(),
            &SweepOptions {
                lock_timeout: Duration::from_secs(8),
                ..SweepOptions::default()
            },
            None,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(750)).await;
    let mut writer = connect(&url).await;
    let live = insert_execution(&mut writer, "up_wf", "up-live", Utc::now(), None).await;
    let appended = tokio::time::timeout(
        Duration::from_secs(3),
        autumn_harvest::store::append_events(
            &mut writer,
            ExecutionId::from_uuid(live),
            &sample_events(),
            0,
        ),
    )
    .await;

    diesel::sql_query("ROLLBACK")
        .execute(&mut reader)
        .await
        .expect("release the reader");

    appended
        .expect(
            "AC3: the DROP's SHARE-to-ACCESS-EXCLUSIVE upgrade waited under the same \
             `lock_timeout` as acquiring the lock, and an exclusive waiter makes every \
             later conflicting request queue behind it. Appends to unrelated cohorts \
             stalled for the whole wait, once per drop attempt. The upgrade needs its own \
             near-zero bound; the partition can wait for the next tick.",
        )
        .expect("the concurrent append itself must succeed");

    sweeper
        .await
        .expect("sweeper task")
        .expect("a partition it could not drop is a blocked partition, not an error");
}

#[tokio::test]
async fn the_inert_migration_fails_fast_rather_than_queueing_every_append() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;

    // An idle-in-transaction reader — a reporting query, a leaked session.
    let mut blocker = connect(&url).await;
    for stmt in ["BEGIN", "SELECT 1 FROM harvest_events LIMIT 1"] {
        diesel::sql_query(stmt)
            .execute(&mut blocker)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    // `ADD COLUMN` with a constant default is metadata-only — no rewrite — but
    // it still takes ACCESS EXCLUSIVE to write the catalog row. Behind this
    // reader the ALTER queues, and because Postgres queues later conflicting
    // requests behind a waiter, every append arriving after it queues behind
    // the ALTER. Unbounded, on a migration whose entire claim is that it is
    // inert: an upgrade that a deployment may never opt into becomes a write
    // outage that ends only when the reader does.
    let up = include_str!("../../migrations/20260901115500_harvest_event_partitioning/up.sql");
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, up),
    )
    .await;

    diesel::sql_query("ROLLBACK")
        .execute(&mut blocker)
        .await
        .expect("release the blocker");

    let outcome = outcome.expect(
        "the migration queued behind an idle reader instead of failing fast, taking \
         every append on the shard with it. It must bound its own lock wait.",
    );
    match outcome {
        Err(e) => assert!(
            partition::is_lock_timeout(&e.to_string()),
            "and it must fail *because of the lock*, so an operator sees a bounded, \
             retryable error rather than a mystery: {e}"
        ),
        Ok(()) => panic!("the ALTER cannot have succeeded while the reader held its lock"),
    }
}

#[tokio::test]
async fn conversion_does_not_widen_who_can_read_event_data() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    // A role the operator deliberately does NOT want reading history, but which
    // the creating role's default privileges would hand access to. Every
    // conversion path replaces `harvest_events` with a freshly created table,
    // and `CREATE TABLE` applies those defaults — so a helper that only ever
    // ADDS the source's grants leaves the replacement carrying access the
    // original never had.
    for stmt in [
        "DO $$ BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'harvest_nosy_958') \
           THEN CREATE ROLE harvest_nosy_958 NOLOGIN; END IF; END $$",
        "REVOKE ALL ON harvest_events FROM harvest_nosy_958",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO harvest_nosy_958",
    ] {
        diesel::sql_query(stmt)
            .execute(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    assert!(
        !scalar_bool(
            &mut conn,
            "SELECT has_table_privilege('harvest_nosy_958', 'harvest_events', 'SELECT') AS v",
        )
        .await,
        "precondition: the role cannot read history before the conversion"
    );

    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");
    let leaked_by_enable = scalar_bool(
        &mut conn,
        "SELECT has_table_privilege('harvest_nosy_958', 'harvest_events', 'SELECT') AS v",
    )
    .await;

    partition::disable_partitioning(&mut conn)
        .await
        .expect("revert")
        .expect("the shard was partitioned");
    let leaked_by_disable = scalar_bool(
        &mut conn,
        "SELECT has_table_privilege('harvest_nosy_958', 'harvest_events', 'SELECT') AS v",
    )
    .await;

    diesel::sql_query(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM harvest_nosy_958",
    )
    .execute(&mut conn)
    .await
    .expect("clean up the default privilege");

    assert!(
        !leaked_by_enable,
        "AC1: enabling must not grant a role access the original table did not \
         give it — the replacement inherits the creating role's default \
         privileges, and replaying the source's grants only adds"
    );
    assert!(
        !leaked_by_disable,
        "and neither must the revert, which replaces the table again"
    );
}

#[tokio::test]
async fn re_running_the_plan_over_a_converted_table_cannot_break_appends() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    let exec = insert_execution(&mut conn, "rerun_wf", "rerun-1", day(2026, 2, 3), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(exec),
        &sample_events(),
        0,
    )
    .await
    .expect("seed a populated table");
    run_plan_phases(&mut conn, 1..=4).await;
    assert_eq!(
        events_relkind(&mut conn).await,
        "p",
        "precondition: converted"
    );

    // The runbook tells an operator to re-run step 3 after a failed validation,
    // and an operator who has lost track of where they were will re-run the
    // plan. Over a CONVERTED table that was not a no-op: step 3's guard looks
    // for its constraint by name on `harvest_events`, the conversion renamed
    // the legacy one onto the legacy partition, so the guard found nothing and
    // added `CHECK (cohort < cutover) NOT VALID` to the live partitioned
    // parent. NOT VALID skips existing rows but still enforces for NEW ones,
    // whose cohort is at or after the cutover by construction — so every
    // append on a working shard failed from that moment.
    let mut refused = false;
    for step in partition::migration_plan_steps(&EnableOptions::default(), Utc::now())
        .into_iter()
        .filter(|s| s.phase == 1 || s.phase == 3)
    {
        if diesel::sql_query(&step.sql)
            .execute(&mut conn)
            .await
            .is_err()
        {
            refused = true;
        }
    }
    assert!(
        refused,
        "the plan must refuse over an already-partitioned table, not proceed"
    );

    // The assertion that actually matters: the shard still works.
    let live = insert_execution(&mut conn, "rerun_wf", "rerun-2", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(live),
        &sample_events(),
        0,
    )
    .await
    .expect(
        "appends must still succeed after a re-run — a cohort CHECK added to the live \
         parent would reject every one of them",
    );
    assert_eq!(
        autumn_harvest::store::load_history(&mut conn, ExecutionId::from_uuid(exec))
            .await
            .expect("history intact")
            .events
            .len(),
        3
    );
}

#[tokio::test]
async fn extending_the_write_window_never_queues_appends_indefinitely() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // A long history query holding ACCESS SHARE on the parent — a report, a
    // stuck session. `CREATE TABLE ... PARTITION OF` takes ACCESS EXCLUSIVE on
    // that parent, and Postgres queues later conflicting requests behind a
    // WAITER, not merely behind held locks. Unbounded, a routine maintenance
    // tick extending the lookahead window becomes a shard-wide write outage
    // that ends only when the reader does.
    let mut reader = connect(&url).await;
    for stmt in ["BEGIN", "SELECT count(*) AS n FROM harvest_events"] {
        diesel::sql_query(stmt)
            .execute(&mut reader)
            .await
            .unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }

    // A cohort far enough ahead that it certainly does not exist yet, so the
    // creation really is attempted.
    let far = Utc::now() + chrono::Duration::days(400);
    let ensure_url = url.clone();
    let ensurer = tokio::spawn(async move {
        let mut c = connect(&ensure_url).await;
        partition::ensure_partitions(&mut c, far, 0, Duration::from_secs(2))
            .await
            .map(|(created, _blocked)| created)
    });

    // Let the creation reach its lock wait before appending — otherwise the
    // append can slip in ahead of it and the test passes without exercising
    // anything.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let mut writer = connect(&url).await;
    let live = insert_execution(&mut writer, "win_wf", "win-live", Utc::now(), None).await;
    let appended = tokio::time::timeout(
        Duration::from_secs(6),
        autumn_harvest::store::append_events(
            &mut writer,
            ExecutionId::from_uuid(live),
            &sample_events(),
            0,
        ),
    )
    .await;

    diesel::sql_query("ROLLBACK")
        .execute(&mut reader)
        .await
        .expect("release the reader");

    appended
        .expect(
            "AC8: extending the lookahead window waited for its parent lock with no bound, \
             and every append arriving meanwhile queued behind that waiting ALTER. \
             Maintenance must bound the attempt and report the cohort blocked for the \
             next tick.",
        )
        .expect("the concurrent append itself must succeed");

    // Blocked, not an error: the cohort is retried next tick.
    ensurer.await.expect("ensure task").ok();
}

#[tokio::test]
async fn ensure_partitions_treats_already_attached_cohorts_as_coverage() {
    // Review finding: `ensure_partitions` tracked `created` and
    // `blocked`, but silently discarded the "already existed" case
    // (`Ok((_, false))`). Say every cohort already existed from a prior
    // pass, except one newly blocked. `created` was then left empty,
    // indistinguishable from total failure, even though the window is
    // mostly covered. `maintain` calls this before `sweep`, so the false
    // "no cohort could be created" error would suppress reclamation on
    // every retry.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // `enable_partitioning` with default options already created steps
    // 0..=3 (today through +3 days). Force step 4 of a wider 0..=4
    // window to collide. This call's only new work is then one blocked
    // cohort, with everything else already attached.
    let collide_at = Utc::now() + chrono::Duration::days(4);
    let collide_cohort = partition::cohort_start(collide_at, partition::DEFAULT_COHORT_WIDTH_SECS);
    let collide_name = partition::partition_name(collide_cohort);
    diesel::sql_query(format!("DROP TABLE IF EXISTS {collide_name}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray relation from a previous run");
    diesel::sql_query(format!("CREATE TABLE {collide_name} (id int)"))
        .execute(&mut conn)
        .await
        .expect("seed a colliding relation");

    let (created, blocked) =
        partition::ensure_partitions(&mut conn, Utc::now(), 4, Duration::from_secs(2))
            .await
            .expect(
                "a window that is already almost fully covered, with one \
                 newly blocked cohort, must not be reported as total \
                 coverage failure",
            );
    assert!(
        created.is_empty(),
        "every cohort except the blocked one already existed from enable; \
         nothing new should have been created; got {created:?}"
    );
    assert_eq!(
        blocked,
        vec![collide_cohort.to_rfc3339()],
        "the one genuinely blocked cohort must still be reported; got {blocked:?}"
    );

    diesel::sql_query(format!("DROP TABLE {collide_name}"))
        .execute(&mut conn)
        .await
        .expect("drop the colliding relation");
}

#[tokio::test]
async fn a_partly_blocked_lookahead_catch_up_is_not_reported_as_a_healthy_pass() {
    // Issue #1270 item 4: `ensure_partitions` keeps creating the REST of the
    // window when one cohort cannot be carved out. That is deliberate, so a
    // maintenance gap does not become self-perpetuating. But `maintain`
    // must not then report a healthy, empty-`last_error` pass just because
    // `created` is non-empty. An operator needs to see that part of the
    // write window is still uncovered.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // `enable_partitioning` with default options already pre-created steps
    // 0..=3 (today through +3 days) — `EnableOptions::default()`'s
    // lookahead. Force step 5 of a wider 0..=6 window to be blocked
    // instead. Use a generated partition name colliding with an unrelated,
    // already-existing relation. That is one of the two ways
    // `ensure_cohort_with_width` reports a cohort blocked (the other is a
    // lock timeout, exercised above).
    let collide_at = Utc::now() + chrono::Duration::days(5);
    let collide_cohort = partition::cohort_start(collide_at, partition::DEFAULT_COHORT_WIDTH_SECS);
    let collide_name = partition::partition_name(collide_cohort);
    // `IF EXISTS` first. This suite may re-run against a persistent
    // database, rather than a fresh one per run. A stray relation left by
    // an interrupted earlier run must not make this test fail for a
    // reason unrelated to what it checks.
    diesel::sql_query(format!("DROP TABLE IF EXISTS {collide_name}"))
        .execute(&mut conn)
        .await
        .expect("clear any stray relation from a previous run");
    diesel::sql_query(format!("CREATE TABLE {collide_name} (id int)"))
        .execute(&mut conn)
        .await
        .expect("seed a colliding relation");

    let outcome = partition::maintain(&mut conn, Utc::now(), 6, &SweepOptions::default(), None)
        .await
        .expect("maintain must not hard-fail on a partial lookahead gap");

    assert!(
        !outcome.created.is_empty(),
        "the OTHER cohorts in the window must still be created; got {outcome:?}"
    );
    assert_eq!(
        outcome.lookahead_blocked,
        vec![collide_cohort.to_rfc3339()],
        "the blocked cohort must be named individually, not folded into a \
         count; got {outcome:?}"
    );
    assert!(
        outcome
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("not fully covered")),
        "a partial catch-up must not look like a healthy pass with no \
         last_error — the CLI and the retention status API both key off it; \
         got {outcome:?}"
    );

    // Review finding: `collide_name` is a real cohort partition's exact,
    // deterministic production name for a future date, not a `_958`-tagged
    // test fixture. Left behind, it would collide with that cohort's real
    // partition when its date arrives, spuriously blocking maintenance for
    // an unrelated reason.
    diesel::sql_query(format!("DROP TABLE {collide_name}"))
        .execute(&mut conn)
        .await
        .expect("drop the colliding relation");
}

#[tokio::test]
async fn maintain_with_progress_ticks_once_per_partition_the_sweep_attempts() {
    // Review finding: a liveness tick recorded once before a shard's whole
    // maintenance pass is not bounded progress. A single shard can spend
    // `max_attempts` partitions at `exact_scan_timeout` each, long enough
    // on its own to cross the scanner's staleness threshold.
    // `maintain_with_progress` exists so a caller can tick once per
    // partition the sweep step attempts instead. Four closed cohorts,
    // each pinned by its own still-existing execution, are all blocked.
    // The sweep step still evaluates every one of them, so it contributes
    // four ticks.
    //
    // `lookahead_cohorts: 0` still makes `ensure_partitions` attempt one
    // cohort (the current one). That loop now ticks too -- see the review
    // finding on the lookahead window, alongside `sweep_inner` and
    // `drain_default_bounded_inner`'s own ticks in `maintain_inner`. So the
    // total here is five: one from ensure_partitions, four from the sweep.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    for days in [20_i64, 21, 22, 23] {
        let ts = Utc::now() - chrono::Duration::days(days);
        let exec = insert_execution(&mut conn, "pinned_wf", &format!("pr-{days}"), ts, None).await;
        autumn_harvest::store::append_events(
            &mut conn,
            ExecutionId::from_uuid(exec),
            &sample_events(),
            0,
        )
        .await
        .expect("seed");
        backdate_events(&mut conn, exec, ts).await;
    }

    let mut ticks = 0usize;
    let outcome = partition::maintain_with_progress(
        &mut conn,
        Utc::now(),
        0,
        &SweepOptions::default(),
        None,
        &mut || ticks += 1,
    )
    .await
    .expect("maintain_with_progress");

    assert_eq!(
        outcome.sweep.blocked.len(),
        4,
        "all four pinned cohorts must be evaluated and reported blocked; \
         got {outcome:?}"
    );
    assert_eq!(
        ticks, 5,
        "the progress callback must fire once per partition the sweep step \
         attempts (four, all blocked) plus once for ensure_partitions's own \
         cohort attempt, not once for the whole shard; got {ticks} ticks for \
         {outcome:?}"
    );
}

#[tokio::test]
async fn maintain_with_progress_ticks_during_the_default_partition_drain_too() {
    // Review finding: the progress callback reached only `sweep_inner`.
    // `drain_default`'s own census (an unbounded full scan of the DEFAULT
    // partition) and its final move (which can process an oversized
    // cohort) run first, in `maintain_inner`. Either can itself cross the
    // scanner's staleness threshold with zero ticks in between.
    //
    // The table starts empty here, so `enable` takes the fresh-table path
    // and there is no legacy partition to catch these cohorts. Every row
    // below lands straight in DEFAULT. The drain then creates a real
    // partition per cohort, which the sweep step -- running right after,
    // in the same pass -- immediately evaluates too. So this cannot
    // isolate the drain's ticks by giving the sweep step nothing to do.
    // It proves the weaker, still sufficient claim instead. The total
    // tick count exceeds what the sweep step's per-partition ticks alone
    // could produce, so the drain step must be contributing some of its
    // own.
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let exec = seed_expired(&mut conn, "drain_tick_wf", "drain-tick-1", day(2026, 3, 1)).await;
    let _ = exec;
    diesel::sql_query(
        "INSERT INTO harvest_events
             (workflow_exec_id, event_id, event_type, event_data, timestamp, cohort)
         SELECT e.workflow_exec_id,
                1000 + (g.i * 10) + e.event_id,
                e.event_type,
                e.event_data,
                e.timestamp,
                '2026-03-01'::timestamptz + (g.i || ' days')::interval
           FROM harvest_events e
           CROSS JOIN generate_series(1, 2) AS g(i)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a small multi-cohort backlog");

    // Every row seeded above already landed in DEFAULT on insert -- there
    // is no partition covering March 2026 yet. This just proves that
    // precondition, rather than relying on it silently.
    let parked = scalar_i64(
        &mut conn,
        &format!(
            "SELECT COUNT(*)::bigint AS n FROM {}",
            partition::DEFAULT_PARTITION
        ),
    )
    .await;
    assert!(
        parked > 0,
        "precondition: the seeded backlog must already sit in DEFAULT"
    );

    let mut ticks = 0usize;
    let outcome = partition::maintain_with_progress(
        &mut conn,
        Utc::now(),
        0,
        &SweepOptions::default(),
        None,
        &mut || ticks += 1,
    )
    .await
    .expect("maintain_with_progress");

    assert!(
        outcome.drained > 0,
        "precondition: the backlog must actually drain; got {outcome:?}"
    );
    let sweep_ticks = outcome.sweep.blocked.len() + outcome.sweep.dropped.len();
    assert!(
        ticks > sweep_ticks,
        "the drain step must also tick -- not leave every tick to the sweep \
         step. Got {ticks} total ticks but only {sweep_ticks} partitions for \
         the sweep step to evaluate; outcome: {outcome:?}"
    );
}

#[tokio::test]
async fn maintain_with_progress_ticks_while_extending_the_lookahead_window() {
    // Review finding: `ensure_partitions`'s per-cohort loop received no
    // progress callback at all. With `lookahead_cohorts` configured high
    // and every attempt waiting out the lock timeout, that loop alone can
    // spend `lookahead_cohorts * lock_timeout`. That is long enough to
    // cross the scanner's staleness threshold with zero ticks in between,
    // unlike the drain and sweep steps around it.
    //
    // The table starts empty and freshly enabled. There is nothing for the
    // drain step to move and no closed cohort for the sweep step to
    // evaluate, so both contribute zero ticks here. Every tick this pass
    // records must therefore come from `ensure_partitions`, one per cohort
    // it attempts: `lookahead_cohorts + 1` (the loop runs `0..=lookahead_
    // cohorts` inclusive).
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    let lookahead_cohorts = 5;
    let mut ticks = 0usize;
    let outcome = partition::maintain_with_progress(
        &mut conn,
        Utc::now(),
        lookahead_cohorts,
        &SweepOptions::default(),
        None,
        &mut || ticks += 1,
    )
    .await
    .expect("maintain_with_progress");

    assert_eq!(
        outcome.drained, 0,
        "precondition: an empty DEFAULT partition has nothing to drain; \
         got {outcome:?}"
    );
    assert_eq!(
        outcome.sweep.blocked.len() + outcome.sweep.dropped.len(),
        0,
        "precondition: a freshly enabled table has no closed cohort for the \
         sweep step to evaluate; got {outcome:?}"
    );
    assert_eq!(
        ticks,
        lookahead_cohorts as usize + 1,
        "the progress callback must fire once per cohort ensure_partitions \
         attempts, not once for the whole shard; got {ticks} ticks for \
         {outcome:?}"
    );
}

#[tokio::test]
async fn a_large_default_backlog_drains_in_bounded_passes() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    // A maintenance outage: events parked in the DEFAULT partition across many
    // closed cohorts. This is precisely the situation the drain exists for, and
    // precisely the one where it was most dangerous — the whole move ran inside
    // the transaction that holds the parent's ACCESS EXCLUSIVE from `DETACH
    // PARTITION`, so every read and append on the shard waited for the entire
    // copy. `lock_timeout` bounds acquiring that lock, never holding it.
    let exec = seed_expired(&mut conn, "drain_wf", "drain-1", day(2026, 3, 1)).await;
    let _ = exec;
    diesel::sql_query(
        "INSERT INTO harvest_events
             (workflow_exec_id, event_id, event_type, event_data, timestamp, cohort)
         SELECT e.workflow_exec_id,
                1000 + (g.i * 10) + e.event_id,
                e.event_type,
                e.event_data,
                e.timestamp,
                '2026-03-01'::timestamptz + (g.i || ' days')::interval
           FROM harvest_events e
           CROSS JOIN generate_series(1, 40) AS g(i)",
    )
    .execute(&mut conn)
    .await
    .expect("seed a multi-cohort backlog");

    // Park them: detach the cohorts' partitions so the rows land in DEFAULT.
    // Simpler and closer to the real shape — move them straight in.
    diesel::sql_query(format!(
        "WITH parked AS (
             DELETE FROM harvest_events WHERE cohort > '2026-03-01'::timestamptz RETURNING *
         )
         INSERT INTO {} SELECT * FROM parked",
        partition::DEFAULT_PARTITION
    ))
    .execute(&mut conn)
    .await
    .expect("park the backlog in DEFAULT");

    let parked = scalar_i64(
        &mut conn,
        &format!(
            "SELECT COUNT(*)::bigint AS n FROM {}",
            partition::DEFAULT_PARTITION
        ),
    )
    .await;
    assert!(parked > 0, "precondition: the backlog really is in DEFAULT");

    // A small budget, so the multi-pass path is exercised without seeding tens
    // of thousands of rows. The FIRST pass must move some but not all of the
    // backlog: that is the whole point — the exclusive window is proportional
    // to the budget, not to however much accumulated during the outage.
    let first = partition::drain_default_bounded(&mut conn, 10)
        .await
        .expect("drain must not error");
    assert!(first > 0, "the first pass must make progress");
    assert!(
        first < usize::try_from(parked).unwrap_or(usize::MAX),
        "the first pass moved the ENTIRE {parked}-row backlog ({first} rows) inside the          transaction holding the parent's ACCESS EXCLUSIVE. Every read and append on the          shard waits for that copy, and `lock_timeout` bounds only acquiring the lock"
    );

    // And the passes together must converge — a bounded drain that never
    // finishes is no better than an unbounded one that blocks the shard.
    let mut passes = 1;
    loop {
        let moved = partition::drain_default_bounded(&mut conn, 10)
            .await
            .expect("drain must not error");
        if moved == 0 {
            break;
        }
        passes += 1;
        assert!(
            passes < 100,
            "the drain must converge, not creep one row at a time"
        );
    }

    assert_eq!(
        scalar_i64(
            &mut conn,
            &format!(
                "SELECT COUNT(*)::bigint AS n FROM {}",
                partition::DEFAULT_PARTITION
            ),
        )
        .await,
        0,
        "every parked row must end up in a real cohort partition"
    );
    // And the layout is still sound: DEFAULT re-attached, appends still route.
    let live = insert_execution(&mut conn, "drain_wf", "drain-live", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(live),
        &sample_events(),
        0,
    )
    .await
    .expect("appends must still work after a multi-pass drain");
}

/// Park a backlog spanning `cohorts` closed cohorts in the DEFAULT partition.
///
/// Few rows per cohort on purpose: that is the shape a long maintenance gap
/// leaves, and the shape the drain's ROW budget cannot bound.
async fn park_backlog_across_cohorts(conn: &mut AsyncPgConnection, cohorts: i32, label: &str) {
    let _exec = seed_expired(conn, "drain_wf", label, day(2026, 3, 1)).await;
    diesel::sql_query(format!(
        "INSERT INTO harvest_events
             (workflow_exec_id, event_id, event_type, event_data, timestamp, cohort)
         SELECT e.workflow_exec_id,
                1000 + (g.i * 10) + e.event_id,
                e.event_type,
                e.event_data,
                e.timestamp,
                '2026-03-01'::timestamptz + (g.i || ' days')::interval
           FROM harvest_events e
           CROSS JOIN generate_series(1, {cohorts}) AS g(i)"
    ))
    .execute(conn)
    .await
    .expect("seed a many-cohort backlog");
    diesel::sql_query(format!(
        "WITH parked AS (
             DELETE FROM harvest_events WHERE cohort > '2026-03-01'::timestamptz RETURNING *
         )
         INSERT INTO {} SELECT * FROM parked",
        partition::DEFAULT_PARTITION
    ))
    .execute(conn)
    .await
    .expect("park the backlog in DEFAULT");
}

/// A pass must bound the DDL it runs while holding the parent's ACCESS EXCLUSIVE.
///
/// The row budget alone does not bound the pass. Every cohort the pass takes
/// needs its partition created — `CREATE TABLE ... PARTITION OF`, one DDL
/// statement each — and all of it runs after `DETACH PARTITION` has taken
/// ACCESS EXCLUSIVE on the parent. A maintenance outage that spans hundreds of
/// closed cohorts parks few rows in each, so the row budget never binds and a
/// single pass ran hundreds of DDL statements with the whole shard stopped
/// behind it.
///
/// The census had the same shape and was worse: a `GROUP BY` over the entire
/// DEFAULT partition, also inside the lock, and unbounded by anything at all.
/// It is now taken before the lock, where it costs bystanders nothing.
#[tokio::test]
async fn a_drain_pass_bounds_the_ddl_it_runs_under_the_parent_lock() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("enable");

    park_backlog_across_cohorts(&mut conn, 120, "drain-ddl-1").await;

    let distinct_cohorts = scalar_i64(
        &mut conn,
        &format!(
            "SELECT COUNT(DISTINCT cohort)::bigint AS n FROM {}",
            partition::DEFAULT_PARTITION
        ),
    )
    .await;
    assert!(
        distinct_cohorts > i64::try_from(partition::DRAIN_MAX_COHORTS).unwrap(),
        "precondition: the backlog must span more cohorts ({distinct_cohorts}) than one \
         pass is allowed to take ({})",
        partition::DRAIN_MAX_COHORTS
    );

    // The generous production row budget, so it is the COHORT bound under test
    // and not the row one.
    let before = partition::list_partitions(&mut conn)
        .await
        .expect("list")
        .len();
    let moved = partition::drain_default_bounded(&mut conn, 50_000)
        .await
        .expect("drain must not error");
    assert!(moved > 0, "the pass must make progress");
    let created = partition::list_partitions(&mut conn)
        .await
        .expect("list")
        .len()
        - before;
    assert!(
        created <= partition::DRAIN_MAX_COHORTS,
        "one pass created {created} partitions inside the transaction holding the parent's \
         ACCESS EXCLUSIVE — every append and read on the shard waits for all of them. A pass \
         may create at most {}",
        partition::DRAIN_MAX_COHORTS
    );
    assert!(
        scalar_i64(
            &mut conn,
            &format!(
                "SELECT COUNT(*)::bigint AS n FROM {}",
                partition::DEFAULT_PARTITION
            ),
        )
        .await
            > 0,
        "a bounded pass must leave the rest of the backlog for the next tick"
    );

    // Still converges, and the layout is sound afterwards.
    let mut passes = 1;
    while partition::drain_default_bounded(&mut conn, 50_000)
        .await
        .expect("drain must not error")
        > 0
    {
        passes += 1;
        assert!(passes < 200, "the drain must converge");
    }
    assert_eq!(
        scalar_i64(
            &mut conn,
            &format!(
                "SELECT COUNT(*)::bigint AS n FROM {}",
                partition::DEFAULT_PARTITION
            ),
        )
        .await,
        0,
        "every parked row must end up in a real cohort partition"
    );
    let live = insert_execution(&mut conn, "drain_wf", "drain-ddl-live", Utc::now(), None).await;
    autumn_harvest::store::append_events(
        &mut conn,
        ExecutionId::from_uuid(live),
        &sample_events(),
        0,
    )
    .await
    .expect("appends must still work after a bounded multi-pass drain");
}

/// Converting must not silently drop row-level security.
///
/// Both conversion paths replace `harvest_events` with a table built by
/// `CREATE TABLE ... (LIKE ...)`, and `LIKE` copies neither
/// `relrowsecurity`/`relforcerowsecurity` nor any `pg_policy` entry — measured
/// against PG16, not assumed. `copy_acl_sql` then faithfully replays the
/// original's owner and grants onto that replacement, so the same roles reach a
/// parent with row security switched off: every row a policy had been filtering
/// becomes readable the instant the conversion commits, and nothing says so.
///
/// The same failure the ACL clearing exists to prevent, by another route. The
/// conversion refuses instead, in both directions, naming the policies.
#[tokio::test]
async fn converting_refuses_to_drop_the_row_security_it_cannot_carry() {
    let (url, _c) = setup_db().await;
    let mut conn = connect(&url).await;
    reset_to_unpartitioned(&mut conn).await;

    diesel::sql_query("ALTER TABLE harvest_events ENABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("enable RLS");
    diesel::sql_query(
        "CREATE POLICY harvest_events_tenant ON harvest_events \
         USING (workflow_exec_id IS NOT NULL)",
    )
    .execute(&mut conn)
    .await
    .expect("create policy");

    let err = partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect_err(
            "converting a table under row-level security replays its GRANTS onto a \
             replacement that has no policies, so rows a policy was filtering become \
             readable. It must refuse.",
        )
        .to_string();
    assert!(
        err.contains("harvest_events_tenant"),
        "the refusal must name the policy that would be lost; got: {err}"
    );
    assert!(
        err.contains("readable"),
        "the refusal must say what the operator is being protected from; got: {err}"
    );

    // And it refused before mutating: the table is still flat, still under RLS,
    // and the policy is still there.
    assert!(
        !partition::detect_layout(&mut conn)
            .await
            .expect("layout")
            .is_partitioned(),
        "the refusal must come before anything mutates"
    );
    let (enabled, policies) = partition::row_security_config(&mut conn)
        .await
        .expect("row security config");
    assert!(enabled, "row security must be left enabled");
    assert_eq!(policies, vec!["harvest_events_tenant".to_string()]);

    // The scripted large-table path refuses in phase 1, before any index build.
    let plan = partition::migration_plan_steps(&EnableOptions::default(), Utc::now());
    let guard = plan
        .iter()
        .find(|s| s.sql.contains("harvest_rls_958"))
        .expect("the plan must carry its own row-security guard — it never calls the Rust one");
    assert_eq!(
        guard.phase, 1,
        "the guard must run before hours of CONCURRENTLY index building, not after"
    );

    // Once the policy is gone the conversion proceeds, so the guard gates on
    // the real condition rather than refusing a table that merely once had one.
    diesel::sql_query("DROP POLICY harvest_events_tenant ON harvest_events")
        .execute(&mut conn)
        .await
        .expect("drop policy");
    diesel::sql_query("ALTER TABLE harvest_events DISABLE ROW LEVEL SECURITY")
        .execute(&mut conn)
        .await
        .expect("disable RLS");
    partition::enable_partitioning(&mut conn, &EnableOptions::default())
        .await
        .expect("conversion must proceed once the row security is gone");
}
