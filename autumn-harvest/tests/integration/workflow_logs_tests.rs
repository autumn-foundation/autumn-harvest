#![cfg(feature = "db")]
//! Durable per-execution workflow logs — issue #790.
//!
//! Exercises the store layer against a real Postgres and asserts the load-bearing
//! contracts:
//!
//! - **AC1**: lines are persisted keyed by `execution_id` with
//!   `(level, message, occurred_at, sequence)` and read back in emission order.
//! - **AC2**: **at most once, deduplicated** — never duplicated, never
//!   reordered. A re-driven decision cycle re-mints the SAME `seq` (a pure call
//!   ordinal), so re-appending an already-stored line is collapsed by the unique
//!   `(workflow_exec_id, seq)` index rather than duplicated.
//!   `randomized_replay_interleavings_never_duplicate_or_reorder_lines` is the
//!   randomized-interleaving replay test the AC mandates: it drives the real
//!   executor over a growing history with a randomized re-drive schedule and
//!   persists through the real store.
//! - **AC3**: level filtering and cursor-bounded pagination.
//! - **AC4**: the per-execution cap drops-newest and surfaces a **truncation
//!   marker** rather than losing lines silently; retention is tied to
//!   workflow-history retention via `ON DELETE CASCADE`.
//! - **#495 interaction**: targeted PII erasure removes log rows.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle (`autumn_harvest::test_init_sql()`).

use autumn_harvest::store::{
    self, WORKFLOW_LOG_TRUNCATION_MESSAGE, WORKFLOW_LOG_TRUNCATION_SEQ, WorkflowLogLine,
};
use autumn_harvest::types::ExecutionId;
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
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

/// Insert a minimal RUNNING execution row so the log rows' FK resolves.
async fn insert_execution(conn: &mut AsyncPgConnection, exec_id: ExecutionId, name: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
             (id, workflow_name, workflow_id, state, input, started_at, queue_name, shard_id)
         VALUES ($1, $2, $2 || '-' || $1::text, 'RUNNING', '{}'::jsonb, NOW(), 'default', 0)",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .bind::<Text, _>(name)
    .execute(conn)
    .await
    .expect("insert execution");
}

fn line(seq: i64, level: &'static str, message: &str) -> WorkflowLogLine {
    WorkflowLogLine {
        seq,
        level,
        message: message.to_string(),
    }
}

async fn read_all(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Vec<(i64, String, String)> {
    store::load_workflow_logs(
        conn,
        exec_id,
        &store::WorkflowLogQuery {
            limit: 10_000,
            ..Default::default()
        },
    )
    .await
    .expect("load logs")
    .into_iter()
    .map(|r| (r.seq, r.level, r.message))
    .collect()
}

// ── AC1: persist + read back in emission order ──────────────────────────────

#[tokio::test]
async fn logs_are_persisted_and_read_back_in_emission_order() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "order_flow").await;

    let lines = vec![
        line(10, "info", "starting"),
        line(11, "warn", "slow downstream"),
        line(12, "error", "charge declined"),
    ];
    let written = store::append_workflow_logs(&mut conn, exec_id, &lines, 1_000)
        .await
        .expect("append");
    assert_eq!(written, 3);

    assert_eq!(
        read_all(&mut conn, exec_id).await,
        vec![
            (10, "info".to_string(), "starting".to_string()),
            (11, "warn".to_string(), "slow downstream".to_string()),
            (12, "error".to_string(), "charge declined".to_string()),
        ],
        "lines must come back in emission (seq) order with level + message intact"
    );
}

#[tokio::test]
async fn logs_are_scoped_to_their_own_execution() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let a = ExecutionId::new();
    let b = ExecutionId::new();
    insert_execution(&mut conn, a, "wf_a").await;
    insert_execution(&mut conn, b, "wf_b").await;

    store::append_workflow_logs(&mut conn, a, &[line(1, "info", "for a")], 1_000)
        .await
        .expect("append a");
    store::append_workflow_logs(&mut conn, b, &[line(1, "info", "for b")], 1_000)
        .await
        .expect("append b");

    // Identical seq values across two executions must NOT collide: the unique
    // index is on (workflow_exec_id, seq), not seq alone.
    assert_eq!(read_all(&mut conn, a).await.len(), 1);
    assert_eq!(read_all(&mut conn, b).await[0].2, "for b");
}

// ── AC2: exactly once ───────────────────────────────────────────────────────

#[tokio::test]
async fn re_driven_cycle_does_not_duplicate_or_reorder_lines() {
    // THE exactly-once test. A decision cycle that logs and then parks can be
    // re-driven at an UNCHANGED history position (a spurious wake, a rolled-back
    // persist). Replay-suppression does not cover that case — the context is
    // genuinely live and re-emits. Because `seq` is a deterministic function of
    // the position, the durable write must collapse the repeat.
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "retry_flow").await;

    let cycle = vec![
        line(100, "info", "step one"),
        line(101, "info", "step two"),
        line(102, "warn", "step three"),
    ];

    // Drive the same cycle five times, interleaving a *later* cycle's lines in
    // the middle so a naive "append everything" implementation would both
    // duplicate AND interleave out of order.
    for round in 0..5 {
        store::append_workflow_logs(&mut conn, exec_id, &cycle, 1_000)
            .await
            .expect("re-drive append");
        if round == 2 {
            store::append_workflow_logs(
                &mut conn,
                exec_id,
                &[line(200, "info", "later cycle")],
                1_000,
            )
            .await
            .expect("later cycle append");
        }
    }

    let rows = read_all(&mut conn, exec_id).await;
    assert_eq!(
        rows,
        vec![
            (100, "info".to_string(), "step one".to_string()),
            (101, "info".to_string(), "step two".to_string()),
            (102, "warn".to_string(), "step three".to_string()),
            (200, "info".to_string(), "later cycle".to_string()),
        ],
        "five re-drives of the same cycle must yield exactly ONE copy of each \
         line, still in emission order"
    );
}

#[tokio::test]
async fn the_return_value_is_the_rows_actually_inserted_not_the_rows_admitted() {
    // Regression (issue #790 review round 2, P2). The function documents its
    // return as "the number of real (non-marker) rows actually inserted", but
    // returned the *admitted* count — so a re-drive, whose rows are all
    // collapsed by `ON CONFLICT DO NOTHING`, reported writes that never
    // happened. Callers that log or meter on it were being told a lie.
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "redrive_count_flow").await;

    let cycle = vec![
        line(0, "info", "step one"),
        line(1, "info", "step two"),
        line(2, "warn", "step three"),
    ];

    let first = store::append_workflow_logs(&mut conn, exec_id, &cycle, 1_000)
        .await
        .expect("first append");
    assert_eq!(first, 3, "a fresh batch genuinely inserts all three rows");

    // Same position, same seqs — every row is collapsed by the conflict clause.
    let second = store::append_workflow_logs(&mut conn, exec_id, &cycle, 1_000)
        .await
        .expect("re-drive append");
    assert_eq!(
        second, 0,
        "a re-drive inserts NOTHING, so the reported count must be 0"
    );

    // Partial overlap: two already-stored seqs plus one genuinely new line.
    let overlapping = vec![
        line(1, "info", "step two"),
        line(2, "warn", "step three"),
        line(3, "info", "step four"),
    ];
    let third = store::append_workflow_logs(&mut conn, exec_id, &overlapping, 1_000)
        .await
        .expect("partial-overlap append");
    assert_eq!(third, 1, "only the genuinely new line is inserted");

    // The critical guard on the fix: the truncation marker keys off ADMISSION,
    // not the inserted count. Using `inserted` for that decision would fire a
    // false marker on every re-drive (0 inserted < 3 lines), telling an operator
    // a healthy run had dropped lines.
    let rows = read_all(&mut conn, exec_id).await;
    assert_eq!(
        rows.len(),
        4,
        "four distinct lines stored, and NO truncation marker: {rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|(_, _, message)| message.contains("cap reached")),
        "a re-drive must never fabricate a truncation marker: {rows:?}"
    );
}

// ── AC3: level filter + cursor pagination ───────────────────────────────────

#[tokio::test]
async fn level_filter_and_cursor_pagination_bound_the_read() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "paged").await;

    let lines: Vec<WorkflowLogLine> = (0..10)
        .map(|i| {
            let level = if i % 3 == 0 { "error" } else { "info" };
            line(i, level, &format!("line {i}"))
        })
        .collect();
    store::append_workflow_logs(&mut conn, exec_id, &lines, 1_000)
        .await
        .expect("append");

    // Level filter.
    let errors = store::load_workflow_logs(
        &mut conn,
        exec_id,
        &store::WorkflowLogQuery {
            levels: &["error"],
            limit: 100,
            ..Default::default()
        },
    )
    .await
    .expect("load errors");
    assert_eq!(errors.len(), 4, "0,3,6,9 are errors");
    assert!(errors.iter().all(|r| r.level == "error"));

    // Cursor pagination: page 1 then resume strictly after its last seq.
    let page1 = store::load_workflow_logs(
        &mut conn,
        exec_id,
        &store::WorkflowLogQuery {
            limit: 4,
            ..Default::default()
        },
    )
    .await
    .expect("page 1");
    assert_eq!(page1.len(), 4);
    assert_eq!(page1[0].seq, 0);
    let cursor = page1.last().expect("last").seq;
    let page2 = store::load_workflow_logs(
        &mut conn,
        exec_id,
        &store::WorkflowLogQuery {
            after_seq: Some(cursor),
            limit: 4,
            ..Default::default()
        },
    )
    .await
    .expect("page 2");
    assert_eq!(page2.len(), 4);
    assert_eq!(
        page2[0].seq,
        cursor + 1,
        "the cursor is exclusive: page 2 must start strictly after page 1"
    );
}

// ── AC4: bounded volume with a visible truncation marker ────────────────────

#[tokio::test]
async fn per_execution_cap_drops_newest_and_records_a_truncation_marker() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "chatty").await;

    let lines: Vec<WorkflowLogLine> = (0..10)
        .map(|i| line(i, "info", &format!("line {i}")))
        .collect();
    let written = store::append_workflow_logs(&mut conn, exec_id, &lines, 4)
        .await
        .expect("append over cap");
    assert_eq!(
        written, 4,
        "only the cap's worth of real lines are admitted"
    );

    let rows = read_all(&mut conn, exec_id).await;
    assert_eq!(rows.len(), 5, "4 real lines + 1 truncation marker");
    assert_eq!(
        rows.iter().map(|r| r.0).take(4).collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "drop-NEWEST: the run's first lines survive"
    );
    let marker = rows.last().expect("marker");
    assert_eq!(
        marker.0, WORKFLOW_LOG_TRUNCATION_SEQ,
        "the marker sorts strictly last so it always renders as the final line"
    );
    assert_eq!(marker.2, WORKFLOW_LOG_TRUNCATION_MESSAGE);
    assert_eq!(
        marker.1, "warn",
        "truncation is operator-visible as a warning"
    );
}

#[tokio::test]
async fn truncation_marker_is_recorded_at_most_once() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "very_chatty").await;

    // Three further over-cap cycles must not stack three markers.
    for base in 0..3 {
        let lines: Vec<WorkflowLogLine> = (0..5)
            .map(|i| line(base * 100 + i, "info", "spam"))
            .collect();
        store::append_workflow_logs(&mut conn, exec_id, &lines, 2)
            .await
            .expect("append");
    }
    let rows = read_all(&mut conn, exec_id).await;
    let markers = rows
        .iter()
        .filter(|r| r.0 == WORKFLOW_LOG_TRUNCATION_SEQ)
        .count();
    assert_eq!(markers, 1, "the truncation marker is idempotent");
    assert_eq!(
        rows.len(),
        3,
        "2 real lines + 1 marker, cap held across cycles"
    );
}

/// The truncation marker is **terminal**: once an execution has truncated, a
/// later batch is rejected even if `max_lines` has since been RAISED.
///
/// Reachable on any rolling deployment. `max_lines` is per-worker-process
/// config, so a run can truncate under an old worker's cap and then have its
/// next decision cycle handled by a new worker with a larger one. Without the
/// latch, the count that decides admission excludes the marker, so the raised
/// cap re-opens the gate and stores a line *after* the one that was dropped —
/// leaving a hole in the stored prefix and a marker whose "subsequent lines
/// were dropped" claim is false.
///
/// Latching keeps two properties that matter more than recovering a raised cap
/// mid-run (the already-dropped lines are gone either way): the stored rows are
/// always a contiguous prefix of the run, and the marker never lies.
#[tokio::test]
async fn a_raised_cap_does_not_reopen_the_gate_after_truncation() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "rolling_deploy").await;

    // Old worker, cap 2: lines 0 and 1 land, line 2 is dropped, marker written.
    store::append_workflow_logs(
        &mut conn,
        exec_id,
        &[
            line(0, "info", "kept 0"),
            line(1, "info", "kept 1"),
            line(2, "info", "dropped 2"),
        ],
        2,
    )
    .await
    .expect("append under the old cap");

    let before = read_all(&mut conn, exec_id).await;
    assert_eq!(before.len(), 3, "2 real lines + the marker: {before:?}");

    // New worker on the SAME still-running execution, cap raised to 100.
    let written = store::append_workflow_logs(
        &mut conn,
        exec_id,
        &[line(3, "info", "must not be stored")],
        100,
    )
    .await
    .expect("append under the raised cap");

    assert_eq!(
        written, 0,
        "a raised cap must not admit a line after the marker exists"
    );

    let rows = read_all(&mut conn, exec_id).await;
    let messages: Vec<&str> = rows.iter().map(|(_, _, m)| m.as_str()).collect();
    assert_eq!(
        messages,
        vec!["kept 0", "kept 1", WORKFLOW_LOG_TRUNCATION_MESSAGE],
        "the stored rows must stay a contiguous prefix + the marker -- storing \
         line 3 while line 2 is missing would leave a hole AND make the \
         marker's claim false: {rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.0 == WORKFLOW_LOG_TRUNCATION_SEQ)
            .count(),
        1,
        "and the marker stays a single row"
    );
}

/// The latch must not swallow a re-drive of lines that are ALREADY stored.
///
/// Rejecting post-marker batches wholesale is only safe because every line in
/// such a batch is either already stored (so `ON CONFLICT DO NOTHING` would
/// have collapsed it anyway) or was deliberately dropped. This pins that: a
/// re-driven cycle after truncation changes nothing, rather than losing rows.
#[tokio::test]
async fn re_driving_a_cycle_after_truncation_preserves_the_stored_prefix() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "redrive_after_cap").await;

    let cycle = vec![
        line(0, "info", "a"),
        line(1, "info", "b"),
        line(2, "info", "c"),
    ];
    store::append_workflow_logs(&mut conn, exec_id, &cycle, 2)
        .await
        .expect("first drive");
    let after_first = read_all(&mut conn, exec_id).await;

    // Same cycle, same cap -- the crash-recovery / spurious-wake shape.
    store::append_workflow_logs(&mut conn, exec_id, &cycle, 2)
        .await
        .expect("re-drive");

    assert_eq!(
        read_all(&mut conn, exec_id).await,
        after_first,
        "a re-drive after truncation must be a no-op, not a row loss"
    );
}

#[tokio::test]
async fn appending_nothing_is_a_no_op() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "quiet").await;
    let written = store::append_workflow_logs(&mut conn, exec_id, &[], 1_000)
        .await
        .expect("empty append");
    assert_eq!(written, 0);
    assert!(
        read_all(&mut conn, exec_id).await.is_empty(),
        "an empty cycle must not create a truncation marker"
    );
}

// ── AC4: retention is tied to workflow-history retention ────────────────────

#[tokio::test]
async fn logs_never_outlive_their_execution() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "doomed").await;
    store::append_workflow_logs(&mut conn, exec_id, &[line(1, "info", "bye")], 1_000)
        .await
        .expect("append");
    assert_eq!(
        store::count_workflow_logs(&mut conn, exec_id)
            .await
            .unwrap(),
        1
    );

    // Deleting the execution is exactly what the retention janitor does; the
    // `ON DELETE CASCADE` FK is what ties log retention to history retention,
    // with no bespoke delete in the janitor.
    diesel::sql_query("DELETE FROM harvest_workflow_executions WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("delete execution");

    assert_eq!(
        store::count_workflow_logs(&mut conn, exec_id)
            .await
            .unwrap(),
        0,
        "log rows must cascade away with the execution — they can never outlive it"
    );
}

// ── issue #495 interaction: targeted PII erasure removes log rows ───────────

#[tokio::test]
async fn payload_erasure_removes_log_rows() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "pii_flow").await;
    store::append_workflow_logs(
        &mut conn,
        exec_id,
        &[line(1, "info", "charged card 4111-1111-1111-1111")],
        1_000,
    )
    .await
    .expect("append");

    let deleted = store::delete_workflow_logs(&mut conn, exec_id)
        .await
        .expect("delete logs");
    assert_eq!(deleted, 1);
    assert_eq!(
        store::count_workflow_logs(&mut conn, exec_id)
            .await
            .unwrap(),
        0,
        "an author log line is free-form text that can carry PII, so erasure \
         must remove it"
    );
}

/// #495 end-to-end: the REAL `erase::erase_workflow_payloads` entry point (the
/// one the `POST /workflows/{id}/erase-payloads` route calls) must remove log
/// rows and REPORT how many, via `EraseOutcome.logs_deleted`.
///
/// The sibling above calls `store::delete_workflow_logs` directly, so it proves
/// the store primitive but not that erasure actually *reaches* it — deleting the
/// `delete_workflow_logs` call from `erase.rs` would keep it green while leaving
/// author-emitted PII on disk after an operator ran an erasure.
#[tokio::test]
async fn erase_workflow_payloads_deletes_logs_and_reports_the_count() {
    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = ExecutionId::new();
    insert_execution(&mut conn, exec_id, "pii_erase_flow").await;
    // Erasure is terminal-only (issue #495), so seal the run first.
    diesel::sql_query(
        "UPDATE harvest_workflow_executions
            SET state = 'COMPLETED', completed_at = NOW()
          WHERE id = $1",
    )
    .bind::<SqlUuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("seal execution");

    store::append_workflow_logs(
        &mut conn,
        exec_id,
        &[
            line(1, "info", "charged card 4111-1111-1111-1111"),
            line(2, "warn", "emailed alice@example.com"),
        ],
        1_000,
    )
    .await
    .expect("append");

    let outcome = autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec_id, "gdpr-req-1")
        .await
        .expect("erase must succeed on a terminal execution");

    assert_eq!(
        outcome.logs_deleted, 2,
        "erasure must REPORT the log rows it removed -- got {outcome:?}"
    );
    assert_eq!(
        store::count_workflow_logs(&mut conn, exec_id)
            .await
            .unwrap(),
        0,
        "and it must actually remove them"
    );
}

/// Best-effort delivery: a FAILED log write must never wedge the workflow.
///
/// `worker::persist_workflow_logs_from_commands` wraps the INSERT in a nested
/// `conn.transaction()` — a Postgres SAVEPOINT — specifically so a failure rolls
/// back only itself. Without it, the failed statement would poison the
/// **enclosing** persist transaction and every later statement in the workflow's
/// own persist would error with "current transaction is aborted", so swallowing
/// the error would corrupt the cycle rather than save it.
///
/// This asserts that property directly against Postgres, and asserts the
/// **control** — without the savepoint the outer transaction is genuinely
/// poisoned — so the test is load-bearing rather than tautological.
#[tokio::test]
async fn a_failed_log_write_in_a_savepoint_leaves_the_outer_transaction_usable() {
    use autumn_harvest::error::HarvestError;
    use diesel_async::AsyncConnection;

    let (url, _c) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let good = ExecutionId::new();
    insert_execution(&mut conn, good, "savepoint_flow").await;
    // No execution row -> the log INSERT violates the FK and fails.
    let orphan = ExecutionId::new();

    // ── With the savepoint (what the worker actually does) ──────────────────
    let outer: Result<(), HarvestError> =
        Box::pin(conn.transaction::<(), HarvestError, _>(async |c| {
            let failed = Box::pin(c.transaction::<(), HarvestError, _>(async |inner| {
                store::append_workflow_logs(inner, orphan, &[line(1, "info", "orphan")], 1_000)
                    .await?;
                Ok(())
            }))
            .await;
            assert!(
                failed.is_err(),
                "a log INSERT for a nonexistent execution must fail (FK violation)"
            );

            // The whole point: the OUTER transaction is still usable.
            store::append_workflow_logs(c, good, &[line(1, "info", "survived")], 1_000).await?;
            Ok(())
        }))
        .await;
    outer.expect("the outer transaction must commit despite the failed log write");

    assert_eq!(
        read_all(&mut conn, good).await,
        vec![(1, "info".to_string(), "survived".to_string())],
        "the workflow's own persist must survive a failed durable-log write"
    );

    // ── Control: WITHOUT the savepoint the outer transaction IS poisoned ────
    let other = ExecutionId::new();
    insert_execution(&mut conn, other, "savepoint_control").await;
    let poisoned: Result<(), HarvestError> =
        Box::pin(conn.transaction::<(), HarvestError, _>(async |c| {
            // Same failing statement, but NOT wrapped in a nested transaction.
            let _ =
                store::append_workflow_logs(c, orphan, &[line(1, "info", "orphan")], 1_000).await;
            store::append_workflow_logs(c, other, &[line(1, "info", "should not land")], 1_000)
                .await?;
            Ok(())
        }))
        .await;
    assert!(
        poisoned.is_err(),
        "without the savepoint the failed statement must poison the outer \
         transaction -- if this ever passes, the savepoint has stopped being \
         load-bearing and the test above proves nothing"
    );
    assert!(
        read_all(&mut conn, other).await.is_empty(),
        "and nothing from the poisoned transaction may be committed"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker-driven end-to-end (AC1 / AC2 / AC6)
//
// The store-level tests above prove the persistence contract in isolation. These
// drive a REAL `Worker` against a REAL `#[workflow]`-shaped handler that calls
// `ctx.log_info` / `log_warn` / `log_error`, so they prove the whole chain:
// context emission -> bookkeeping command -> worker persist -> `harvest_workflow_logs`.
// ─────────────────────────────────────────────────────────────────────────────

use autumn_harvest::WorkflowInfo;
use autumn_harvest::context::{WorkflowContext, WorkflowLogPolicy};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::worker::HandlerRegistry;
use chrono::Utc;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Logs at all three levels, then completes in ONE decision cycle.
fn logging_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.log_info("starting");
        ctx.log_warn("degraded upstream");
        ctx.log_error("giving up on the fast path");
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Logs, parks on a durable timer, then logs again — so the handler body is
/// re-driven by the worker at least twice and the SECOND cycle replays the
/// first cycle's `ctx.log_*` calls.
///
/// This is the AC2 end-to-end shape: replay suppression plus the deterministic
/// positional `seq` must together yield exactly one stored copy of each line.
fn logging_workflow_across_cycles<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.log_info("before the timer");
        ctx.timer("log_gap", 1).await.map_err(|e| e.to_string())?;
        ctx.log_info("after the timer");
        Ok(serde_json::json!({"ok": true}))
    })
}

fn workflow_info(name: &'static str, handler: autumn_harvest::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "workflow_logs_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

/// Seed a RUNNING execution + its `WorkflowStarted` event + a claimable
/// workflow task, then run a real worker until the execution is COMPLETED.
///
/// `policy` is the registry's durable-log policy: `None` reproduces a
/// deployment with the sink DISABLED (AC6).
async fn run_logging_workflow(
    database_url: &str,
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    handler: autumn_harvest::WorkflowHandlerFn,
    policy: Option<WorkflowLogPolicy>,
) -> ExecutionId {
    let exec_id = ExecutionId::new();
    insert_execution(conn, exec_id, workflow_name).await;
    // `insert_execution` writes a fixed workflow_name; overwrite it so the
    // worker's handler lookup resolves.
    diesel::sql_query("UPDATE harvest_workflow_executions SET workflow_name = $2 WHERE id = $1")
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(workflow_name)
        .execute(conn)
        .await
        .expect("set workflow_name");

    let input = serde_json::json!({});
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");

    let mut params = EnqueueParams::new("default", TaskType::Workflow, input);
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
    queue::enqueue(conn, &params).await.expect("enqueue");

    let mut registry = HandlerRegistry::new(vec![workflow_info(workflow_name, handler)], vec![]);
    registry = registry.with_workflow_log_policy(policy);
    let worker = crate::integration_e2e::build_runtime_worker(
        &format!("logs-worker-{workflow_name}"),
        2,
        2,
        Arc::new(registry),
    );

    let pool = crate::integration_e2e::build_test_pool(database_url);
    let runner = Arc::clone(&worker);
    let handle = tokio::spawn(async move {
        runner.run(&pool).await;
    });

    let url = database_url.to_string();
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let execution = crate::integration_e2e::load_execution_from_url(&url, exec_id).await;
            if execution.state == "COMPLETED" {
                break execution;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    worker.shutdown();
    handle.await.expect("worker task should join");
    completed.expect("workflow should complete within the timeout");

    exec_id
}

/// AC1 end-to-end: a real worker persists the lines a real workflow emitted
/// through `ctx.log_*`, keyed by `execution_id`, with the right levels, in
/// emission order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_persists_author_log_lines_end_to_end() {
    let (database_url, _container) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = run_logging_workflow(
        &database_url,
        &mut conn,
        "logging_wf",
        logging_workflow,
        Some(WorkflowLogPolicy::default()),
    )
    .await;

    let rows = read_all(&mut conn, exec_id).await;
    let level_and_message: Vec<(&str, &str)> = rows
        .iter()
        .map(|(_, level, message)| (level.as_str(), message.as_str()))
        .collect();
    assert_eq!(
        level_and_message,
        vec![
            ("info", "starting"),
            ("warn", "degraded upstream"),
            ("error", "giving up on the fast path"),
        ],
        "the worker must persist every ctx.log_* line, in emission order, \
         with its level -- got {rows:?}"
    );
    // `seq` is a deterministic LOGICAL-POSITION identity (epoch << 24 | index),
    // not a 0-based counter, so assert the contract (strictly increasing in
    // emission order) rather than the encoding's concrete values.
    assert!(
        rows.windows(2).all(|w| w[0].0 < w[1].0),
        "seq must strictly increase in emission order -- got {rows:?}"
    );

    // AC7: logs are NOT part of the event history.
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert!(
        !format!("{:?}", history.events).contains("degraded upstream"),
        "a log line must never appear in harvest_events"
    );
}

/// AC6: with the durable sink DISABLED (the default), `ctx.logger()` still
/// works and the workflow runs identically — it simply persists nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_disabled_persists_nothing_and_the_workflow_is_unchanged() {
    let (database_url, _container) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    // Same handler, same worker path — only the registry policy differs.
    let exec_id = run_logging_workflow(
        &database_url,
        &mut conn,
        "logging_wf_off",
        logging_workflow,
        None,
    )
    .await;

    assert!(
        read_all(&mut conn, exec_id).await.is_empty(),
        "with the sink disabled NOTHING may be persisted"
    );
    // The run itself is unaffected.
    let execution = crate::integration_e2e::load_execution_from_url(&database_url, exec_id).await;
    assert_eq!(execution.state, "COMPLETED");
    assert_eq!(execution.output, Some(serde_json::json!({"ok": true})));
}

/// AC2 end-to-end: a workflow whose body is re-driven across MULTIPLE decision
/// cycles (log -> durable timer -> log) stores each line **exactly once**, in
/// emission order — no duplication from the replayed first cycle, no reordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_cycle_workflow_stores_each_line_exactly_once() {
    let (database_url, _container) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = run_logging_workflow(
        &database_url,
        &mut conn,
        "logging_wf_cycles",
        logging_workflow_across_cycles,
        Some(WorkflowLogPolicy::default()),
    )
    .await;

    let rows = read_all(&mut conn, exec_id).await;
    let messages: Vec<&str> = rows.iter().map(|(_, _, m)| m.as_str()).collect();
    assert_eq!(
        messages,
        vec!["before the timer", "after the timer"],
        "each line exactly once, in emission order -- got {rows:?}"
    );

    // Prove the run genuinely spanned more than one decision cycle (otherwise
    // the assertion above would be vacuous: replay suppression was never
    // exercised).
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("history");
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the fixture must actually park on a durable timer so the first cycle \
         is REPLAYED on the second -- history: {:?}",
        history.events
    );
}

// ---------------------------------------------------------------------------
// AC2 — randomized-interleaving replay
// ---------------------------------------------------------------------------

/// Logs several lines per decision cycle across three cycles, parking on a
/// durable timer between them.
///
/// Shape (6 lines / 3 cycles) chosen so a re-drive at cycle 2 or 3 must REPLAY
/// (and therefore suppress) every earlier cycle's lines while re-emitting its
/// own — the exact interleaving AC2's randomized test has to exercise.
fn interleaved_logging_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.log_info("c1-a");
        ctx.log_warn("c1-b");
        ctx.timer("gap-1", 1).await.map_err(|e| e.to_string())?;
        ctx.log_error("c2-a");
        ctx.log_info("c2-b");
        ctx.log_warn("c2-c");
        ctx.timer("gap-2", 1).await.map_err(|e| e.to_string())?;
        ctx.log_info("c3-a");
        Ok(serde_json::json!({"ok": true}))
    })
}

/// The lines `interleaved_logging_workflow` emits, in emission order.
const INTERLEAVED_EXPECTED: [&str; 6] = ["c1-a", "c1-b", "c2-a", "c2-b", "c2-c", "c3-a"];

/// Deterministic xorshift PRNG — a randomized test that cannot be reproduced
/// from its seed is a flake generator, not a regression guard.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    const fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Uniform-ish draw in `1..=n`.
    const fn in_range(&mut self, n: u64) -> u64 {
        (self.next_u64() % n) + 1
    }
}

/// Harvest every `RecordLog` command a drive produced, from BOTH command
/// channels (`Suspended { commands }` and the returned bookkeeping vec) —
/// mirroring how the worker itself collects them.
fn record_log_lines(
    outcome: &autumn_harvest::WorkflowOutcome,
    pending: &[autumn_harvest::context::WorkflowCommand],
) -> Vec<WorkflowLogLine> {
    use autumn_harvest::WorkflowOutcome;
    use autumn_harvest::context::WorkflowCommand;
    let mut out = Vec::new();
    let mut take = |cmds: &[WorkflowCommand]| {
        for cmd in cmds {
            if let WorkflowCommand::RecordLog {
                seq,
                level,
                message,
            } = cmd
            {
                out.push(WorkflowLogLine {
                    seq: i64::try_from(*seq).unwrap_or(i64::MAX),
                    level: level.as_str(),
                    message: message.clone(),
                });
            }
        }
    };
    if let WorkflowOutcome::Suspended { commands } = outcome {
        take(commands);
    }
    take(pending);
    out
}

/// Drive the workflow body ONCE against `history`, exactly as the worker's
/// decision cycle does (same executor entry point, same durable-log policy).
async fn drive_once(
    exec_id: ExecutionId,
    history: &[WorkflowEvent],
    policy: WorkflowLogPolicy,
) -> (
    autumn_harvest::WorkflowOutcome,
    Vec<autumn_harvest::context::WorkflowCommand>,
) {
    let (outcome, pending, _span) =
        autumn_harvest::executor::run_workflow_with_state_history_policy_and_caps(
            exec_id,
            history.to_vec(),
            interleaved_logging_workflow,
            serde_json::json!({}),
            autumn_harvest::context::empty_shared_state(),
            autumn_harvest::context::WorkflowHistoryPolicy::default(),
            None,
            &[],
            &[],
            "interleaved_logging_wf",
            autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            autumn_harvest::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            autumn_harvest::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            autumn_harvest::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            Some(policy),
            std::collections::HashMap::new(),
            None,
            Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            None,
            None,
        )
        .await;
    (outcome, pending)
}

/// Grow `history` by a REPLAY-TRANSPARENT `Paused`/`Resumed` pair (issue #383).
///
/// `HistoryMatcher::new` pre-marks both consumed, so the pair lengthens the
/// history WITHOUT advancing the workflow's own command cursor: the very same
/// lines are then re-emitted LIVE at a different history length.
///
/// This is the dimension that makes the randomized test load-bearing. A `seq`
/// derived from the loaded-history length (the encoding `publish_progress` uses)
/// re-mints a DIFFERENT id for the same logical line here and duplicates it; a
/// pure call ordinal does not. Without this growth, every re-drive sits at an
/// unchanged history length and both encodings look identical.
fn grow_history_replay_transparently(history: &mut Vec<WorkflowEvent>) {
    history.push(WorkflowEvent::WorkflowExecutionPaused {
        paused_at: Utc::now(),
        reason: Some("randomized interleaving".to_string()),
        actor: "test".to_string(),
    });
    history.push(WorkflowEvent::WorkflowExecutionResumed {
        resumed_at: Utc::now(),
        actor: "test".to_string(),
    });
}

/// Resolve the durable timer the body just parked on, so the NEXT drive replays
/// everything up to here and emits the next cycle live.
///
/// Returns `false` when the suspension carried no `StartTimer` — i.e. the
/// fixture can no longer make progress, which the caller asserts on.
fn advance_history_past_timer(
    history: &mut Vec<WorkflowEvent>,
    commands: &[autumn_harvest::context::WorkflowCommand],
) -> bool {
    for cmd in commands {
        if let autumn_harvest::context::WorkflowCommand::StartTimer {
            timer_id,
            duration_secs,
            ..
        } = cmd
        {
            history.push(WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: *duration_secs,
            });
            history.push(WorkflowEvent::TimerFired {
                timer_id: timer_id.clone(),
            });
            return true;
        }
    }
    false
}

/// AC2, the mandated **randomized-interleaving replay** test.
///
/// Each trial drives the workflow body over a GROWING history with a randomized
/// schedule — a randomized number of re-drives at every history position before
/// the history is advanced — and persists each cycle's `RecordLog` commands
/// through the REAL [`store::append_workflow_logs`] into the REAL
/// `harvest_workflow_logs` table.
///
/// This is load-bearing in three ways a simulated store cannot be:
///
/// 1. It genuinely **replays**: once the history has grown past cycle 1, every
///    later drive re-executes cycle 1's `ctx.log_*` calls in replay mode, so
///    replay suppression is exercised on every drive rather than never.
/// 2. It genuinely **interleaves**: the re-drive count varies per position, so
///    a live line and a replayed line land in the same drive in many different
///    combinations across trials.
/// 3. It genuinely depends on **the durable dedup**: collapsing happens in
///    Postgres via `ON CONFLICT (workflow_exec_id, seq) DO NOTHING`, so
///    dropping the unique index fails this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn randomized_replay_interleavings_never_duplicate_or_reorder_lines() {
    let (database_url, _container) = setup_database().await;
    let mut conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect");

    let policy = WorkflowLogPolicy::default();
    // A fixed seed per trial keeps a failure reproducible.
    for trial in 0..12_u64 {
        let mut rng = Rng::new(0xA11C_E790 ^ (trial.wrapping_mul(0x9E37_79B9)));
        let exec_id = ExecutionId::new();
        insert_execution(&mut conn, exec_id, "interleaved_logging_wf").await;

        let mut history = vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let mut drives = 0_u32;
        let mut pause_pairs = 0_u32;
        let mut completed = false;
        // Bounded so a regression that never completes fails loudly rather
        // than hanging the suite.
        for _ in 0..64 {
            // Re-drive the SAME logical position a randomized number of times
            // (2..=4) before advancing — the crash-recovery / task-requeue
            // shape, where a decision cycle is replayed without the workflow
            // having made progress. At least two, so every position offers a
            // window in which to grow the history replay-transparently (see
            // below); a single-drive position would make that dimension
            // unreachable and the trial vacuous.
            let repeats = rng.in_range(3) + 1;
            let mut last: Option<(_, _)> = None;
            for repeat in 0..repeats {
                // Forced on the trial's first opportunity, randomized after,
                // so every trial exercises replay-transparent history growth
                // while the *pattern* still varies from trial to trial.
                if repeat > 0 && (pause_pairs == 0 || rng.in_range(2) == 1) {
                    grow_history_replay_transparently(&mut history);
                    pause_pairs += 1;
                }

                let (outcome, pending) = drive_once(exec_id, &history, policy).await;
                let lines = record_log_lines(&outcome, &pending);
                if !lines.is_empty() {
                    store::append_workflow_logs(&mut conn, exec_id, &lines, policy.max_lines())
                        .await
                        .expect("append_workflow_logs");
                }
                drives += 1;
                last = Some((outcome, pending));
            }

            let (outcome, _pending) = last.expect("at least one drive per position");
            match outcome {
                autumn_harvest::WorkflowOutcome::Completed { .. } => {
                    completed = true;
                    break;
                }
                autumn_harvest::WorkflowOutcome::Suspended { commands } => {
                    assert!(
                        advance_history_past_timer(&mut history, &commands),
                        "trial {trial}: suspended without a StartTimer to resolve -- \
                         the fixture can no longer make progress"
                    );
                }
                other => panic!("trial {trial}: unexpected outcome {other:?}"),
            }
        }

        assert!(
            completed,
            "trial {trial}: the workflow never completed within the drive bound"
        );
        assert!(
            drives > 3,
            "trial {trial}: only {drives} drives -- the schedule must span \
             multiple cycles or replay is never exercised"
        );
        assert!(
            pause_pairs > 0,
            "trial {trial}: the schedule never grew the history \
             replay-transparently, so a history-length-derived `seq` would look \
             identical to a call ordinal -- the test would be vacuous"
        );

        // The whole point: the real table holds each line exactly once, in
        // emission order, no matter how the drives interleaved.
        let rows = read_all(&mut conn, exec_id).await;
        let messages: Vec<&str> = rows.iter().map(|(_, _, m)| m.as_str()).collect();
        assert_eq!(
            messages,
            INTERLEAVED_EXPECTED.to_vec(),
            "trial {trial} ({drives} drives): every line exactly once, in \
             emission order -- got {rows:?}"
        );
        assert!(
            rows.windows(2).all(|w| w[0].0 < w[1].0),
            "trial {trial}: seq must strictly increase in emission order -- got {rows:?}"
        );
    }
}
