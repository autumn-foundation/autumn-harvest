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
    let outcome = partition::sweep(&mut conn, Utc::now(), &SweepOptions::default())
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
