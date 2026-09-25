#![cfg(feature = "db")]
//! Evidence for [`autumn_harvest::execution::check_and_report_unfinished_handlers_batch`],
//! the batched replacement for the parent-close-cascade "unfinished update
//! handler" diagnostic check.
//!
//! `apply_parent_close_cascade` closes every child a `ParentClosePolicy`
//! still governs when its parent reaches a terminal state. Before this fix,
//! every caller of that cascade (five sites in `worker.rs`, three more in
//! `timeout.rs`) looped over the returned `closed_children` and called
//! `check_and_report_unfinished_handlers` once per child -- one
//! `SELECT * FROM harvest_events WHERE workflow_exec_id = $1` per child,
//! issued strictly after the writing transaction committed (a best-effort,
//! error-ignored diagnostic, not a correctness path). A parent with `N`
//! children under a close policy therefore issued `N` history-load
//! round trips on every one of its own terminal transitions --
//! effectively the hottest write path in the engine.
//!
//! `check_and_report_unfinished_handlers_batch` replaces the loop with one
//! `eq_any` query ([`autumn_harvest::store::load_histories_undecoded_batch`]),
//! backed by the same `idx_harvest_events_exec (workflow_exec_id, event_id)`
//! index the single-execution loader already used, so the fix needs no
//! schema change.
//!
//! Two tests:
//! - [`batched_check_agrees_with_the_per_child_loop`] -- fast, always-run
//!   correctness check. Ties the batched function's reported
//!   `(workflow_name, count)` pairs to the old per-child loop's, over a
//!   fixture that includes both children with a genuinely unfinished update
//!   handler and children with none.
//! - [`zz_capture_parent_close_cascade_unfinished_handlers_evidence`] --
//!   `#[ignore]`d. Seeds a production-shaped fixture of closed children into
//!   a throwaway database and captures a `pg_stat_statements` snapshot for
//!   both strategies.

use diesel::sql_types::{BigInt, Text};
use diesel::{QueryableByName, sql_types};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::execution::{
    check_and_report_unfinished_handlers, check_and_report_unfinished_handlers_batch,
};
use autumn_harvest::store::append_events;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{ExecutionId, UpdateId};

use crate::integration_e2e::setup_test_database_url_or_env;

/// A minimal [`MetricsRecorder`] that only captures
/// `record_workflow_unfinished_handlers` calls, for tying the batched
/// function's reported counts to the per-child loop's.
#[derive(Default)]
struct UnfinishedHandlerRecorder {
    calls: std::sync::Mutex<Vec<(String, u64)>>,
}

impl UnfinishedHandlerRecorder {
    fn sorted_calls(&self) -> Vec<(String, u64)> {
        let mut calls = self.calls.lock().unwrap().clone();
        calls.sort();
        calls
    }
}

impl MetricsRecorder for UnfinishedHandlerRecorder {
    fn record_workflow_unfinished_handlers(&self, workflow_name: &str, _kind: &str, count: u64) {
        self.calls
            .lock()
            .unwrap()
            .push((workflow_name.to_string(), count));
    }
}

/// A deterministic, moderate-depth history: one `WorkflowStarted`, a run of
/// `SignalReceived` filler events (a real, minimal-field event type -- not a
/// synthetic one invented for this fixture), and -- for a fraction of
/// children -- a trailing `UpdateAdmitted` with no matching
/// `UpdateCompleted`/`UpdateFailed`, exactly the shape
/// `unfinished_update_handler_count_at_end` exists to detect.
///
/// `events_per_child` mirrors a real, moderately active workflow's history
/// depth, not a toy 1-2 event fixture: enough that the per-child `SELECT`
/// this test measures touches a realistic number of heap/index pages, not a
/// single-row lookup that would understate the loop's real cost.
fn history_for(i: usize, events_per_child: usize, unfinished: bool) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(events_per_child);
    events.push(WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({ "seed": i }),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });
    let filler = events_per_child.saturating_sub(2);
    for j in 0..filler {
        events.push(WorkflowEvent::SignalReceived {
            signal_name: format!("progress_{j}"),
            payload: serde_json::json!({ "child": i, "step": j, "note": "cascade-fixture" }),
        });
    }
    if unfinished {
        events.push(WorkflowEvent::UpdateAdmitted {
            update_id: UpdateId::new(),
            name: "pending_update".to_string(),
            input: serde_json::json!({ "child": i }),
            timestamp: chrono::Utc::now(),
        });
    } else {
        events.push(WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "child": i }),
        });
    }
    events
}

async fn seed_execution_row(conn: &mut AsyncPgConnection, id: ExecutionId, workflow_name: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, shard_id, state, input) \
         VALUES ($1, $2, $3, 0, 'CANCELLED', '{}'::jsonb)",
    )
    .bind::<sql_types::Uuid, _>(id.as_uuid())
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(format!("{workflow_name}-{}", id.as_uuid()))
    .execute(conn)
    .await
    .expect("seed harvest_workflow_executions row");
}

/// Every 15th child carries a genuinely unfinished update handler -- close
/// to the low, real-world rate this diagnostic exists to catch (it fires on
/// a bug in workflow code, not on ordinary traffic), while still giving the
/// equivalence check a nonzero, deterministic set of positive cases to
/// compare.
fn is_unfinished(i: usize) -> bool {
    i.is_multiple_of(15)
}

// ---------------------------------------------------------------------------
// Fast, always-run correctness check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batched_check_agrees_with_the_per_child_loop() {
    let (database_url, _container) = setup_test_database_url_or_env().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let workflow_name = format!("cascade_child_wf_{run_id}");

    const N: usize = 40;
    const EVENTS_PER_CHILD: usize = 12;

    let mut checks: Vec<(ExecutionId, String)> = Vec::with_capacity(N);
    for i in 0..N {
        let exec_id = ExecutionId::new();
        seed_execution_row(&mut conn, exec_id, &workflow_name).await;
        let events = history_for(i, EVENTS_PER_CHILD, is_unfinished(i));
        append_events(&mut conn, exec_id, &events, 0)
            .await
            .expect("seed child history");
        checks.push((exec_id, workflow_name.clone()));
    }

    let before = UnfinishedHandlerRecorder::default();
    for (exec_id, name) in &checks {
        let _ = check_and_report_unfinished_handlers(&mut conn, *exec_id, name, Some(&before))
            .await;
    }

    let after = UnfinishedHandlerRecorder::default();
    check_and_report_unfinished_handlers_batch(&mut conn, &checks, Some(&after))
        .await
        .expect("batched check");

    let before_calls = before.sorted_calls();
    let after_calls = after.sorted_calls();
    let expected_unfinished = (0..N).filter(|&i| is_unfinished(i)).count();
    assert_eq!(
        before_calls.len(),
        expected_unfinished,
        "fixture's own unfinished-handler count sanity check"
    );
    assert_eq!(
        before_calls, after_calls,
        "batched check must report byte-identical (workflow_name, count) pairs \
         to the per-child loop it replaces"
    );
}

// ---------------------------------------------------------------------------
// Evidence capture: pg_stat_statements before/after
// ---------------------------------------------------------------------------

/// 400 closed children, each with a 30-event history -- a large parent-close
/// cascade (a fan-out workflow whose parent just went terminal), not a toy
/// input. Every history is a deterministic function of its index, so the
/// fixture -- and the before/after result sets it produces -- are
/// byte-identical on every run.
const N_CHILDREN: usize = 400;
const EVENTS_PER_CHILD: usize = 30;

async fn seed_production_shaped_fixture(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
) -> Vec<(ExecutionId, String)> {
    let mut checks = Vec::with_capacity(N_CHILDREN);
    for i in 0..N_CHILDREN {
        let exec_id = ExecutionId::new();
        seed_execution_row(conn, exec_id, workflow_name).await;
        let events = history_for(i, EVENTS_PER_CHILD, is_unfinished(i));
        append_events(conn, exec_id, &events, 0)
            .await
            .expect("seed child history");
        checks.push((exec_id, workflow_name.to_string()));
    }
    conn.batch_execute("ANALYZE harvest_events; ANALYZE harvest_workflow_executions;")
        .await
        .expect("analyze");
    checks
}

#[derive(QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    total_buffers: i64,
}

async fn reset_pg_stat_statements(conn: &mut AsyncPgConnection) {
    // Scoped to this run's own database -- see
    // docs/performance-usage-report-activity-lookback.md for why the bare
    // zero-argument reset is wrong on a shared cluster.
    diesel::sql_query(
        "SELECT pg_stat_statements_reset(0, \
         (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
    )
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset failed");
}

async fn capture_pg_stat_statements(conn: &mut AsyncPgConnection) -> Vec<StatRow> {
    let stats: Vec<StatRow> = diesel::sql_query(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
         (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE query ILIKE '%harvest_events%' AND query NOT ILIKE '%pg_stat_statements%' \
           AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY total_buffers DESC LIMIT 10",
    )
    .load(conn)
    .await
    .expect("pg_stat_statements query failed");
    assert!(
        !stats.is_empty(),
        "pg_stat_statements returned no matching rows -- the ILIKE filter or \
         the reset above is not scoped correctly"
    );
    stats
}

fn format_result_rows(rows: &[(String, u64)]) -> String {
    use std::fmt::Write as _;
    let mut sorted = rows.to_vec();
    sorted.sort();
    let mut out = format!("unfinished-handler reports: {}\n", sorted.len());
    for (workflow_name, count) in sorted {
        let _ = writeln!(out, "{workflow_name} count={count}");
    }
    out
}

fn write_capture_artifacts(
    out_dir: &std::path::Path,
    label: &str,
    header: &str,
    stats: &[StatRow],
    rows: &[(String, u64)],
) {
    std::fs::write(
        out_dir.join(format!("{label}.pg_stat_statements.txt")),
        format!("{header}{stats:#?}\n"),
    )
    .unwrap_or_else(|e| panic!("write {label} pg_stat_statements artifact: {e}"));
    std::fs::write(
        out_dir.join(format!("{label}.result-rows.txt")),
        format_result_rows(rows),
    )
    .unwrap_or_else(|e| panic!("write {label} result-rows artifact: {e}"));
}

/// "Before": loop `check_and_report_unfinished_handlers` once per closed
/// child -- exactly what every pre-fix call site did.
async fn capture_before(
    conn: &mut AsyncPgConnection,
    checks: &[(ExecutionId, String)],
    out_dir: &std::path::Path,
) -> Vec<(String, u64)> {
    reset_pg_stat_statements(conn).await;
    let recorder = UnfinishedHandlerRecorder::default();
    for (exec_id, name) in checks {
        let _ = check_and_report_unfinished_handlers(conn, *exec_id, name, Some(&recorder)).await;
    }
    let stats = capture_pg_stat_statements(conn).await;
    let rows = recorder.sorted_calls();
    write_capture_artifacts(
        out_dir,
        "before",
        &format!(
            "-- before: check_and_report_unfinished_handlers() looped once per \
             closed child (N={}) --\n",
            checks.len()
        ),
        &stats,
        &rows,
    );
    rows
}

/// "After": the real, shipped `check_and_report_unfinished_handlers_batch`.
async fn capture_after(
    conn: &mut AsyncPgConnection,
    checks: &[(ExecutionId, String)],
    out_dir: &std::path::Path,
) -> Vec<(String, u64)> {
    reset_pg_stat_statements(conn).await;
    let recorder = UnfinishedHandlerRecorder::default();
    check_and_report_unfinished_handlers_batch(conn, checks, Some(&recorder))
        .await
        .expect("batched check");
    let stats = capture_pg_stat_statements(conn).await;
    let rows = recorder.sorted_calls();
    write_capture_artifacts(
        out_dir,
        "after",
        &format!(
            "-- after: check_and_report_unfinished_handlers_batch() (N={} closed \
             children, one query) --\n",
            checks.len()
        ),
        &stats,
        &rows,
    );
    rows
}

/// Regenerates `docs/perf-artifacts/parent-close-cascade-unfinished-handlers/`.
/// `#[ignore]`d -- seeds hundreds of full workflow histories and takes real
/// time. Needs `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
/// reachable Docker daemon for `claim_bench_support::db::setup_bench_db`'s
/// testcontainer fallback.
#[tokio::test]
#[ignore = "seeds a production-shaped parent-close-cascade fixture; run explicitly"]
async fn zz_capture_parent_close_cascade_unfinished_handlers_evidence() {
    use super::claim_bench_support::db;

    let bench = match db::setup_bench_db().await {
        Ok(b) => b,
        Err(reason) => {
            eprintln!("no database reachable; nothing captured: {}", reason.0);
            return;
        }
    };

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("parent-close-cascade-unfinished-handlers");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut conn = db::connect(&bench.url).await;
    diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut conn)
        .await
        .ok();

    let workflow_name = "cascade_child_wf";
    let checks = seed_production_shaped_fixture(&mut conn, workflow_name).await;

    let mut before_rows = capture_before(&mut conn, &checks, &out_dir).await;
    let mut after_rows = capture_after(&mut conn, &checks, &out_dir).await;

    before_rows.sort();
    after_rows.sort();
    assert_eq!(
        before_rows, after_rows,
        "before (looped check) and after (batched check) must report \
         byte-identical unfinished-handler counts"
    );

    eprintln!(
        "equivalence confirmed: {} closed children, {} unfinished-handler reports agree \
         across before and after",
        checks.len(),
        before_rows.len()
    );
    eprintln!("== done. Artifacts in {} ==", out_dir.display());
}
