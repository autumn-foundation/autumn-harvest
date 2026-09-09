#![cfg(feature = "db")]
//! Ledger performance investigation: the `ScheduleActivity` fan-out enqueue
//! N+1.
//!
//! `persist_scheduled_activities` and `persist_mixed_suspension_batch`
//! (`autumn-harvest/src/worker.rs`) both persist a decision's
//! `ScheduleActivity` commands with a `for` loop. Each loop turn calls
//! `queue::enqueue` once. That is one single-row `INSERT INTO
//! harvest_task_queue` statement per scheduled activity.
//!
//! A workflow can fan out to `N` parallel activities in one suspension,
//! for example via `ctx.execute_activity_fan_out_raw`. Persisting that one
//! decision then cost `N` round trips. This is the same shape this
//! persona's own charter names: bookkeeping queries that are individually
//! trivial but collectively dominant, found by `calls`, not buffers.
//!
//! The fix adds `queue::enqueue_batch`. It builds the same
//! [`autumn_harvest::models::NewTaskQueueItem`] rows the loop built, and
//! inserts them in one multi-row `INSERT`. Both call sites now call it
//! once instead of looping. See `queue::enqueue_batch`'s own doc comment
//! for the rare per-row sticky-pin follow-up it still issues.
//!
//! Evidence here is `pg_stat_statements` call and buffer counts, never
//! wall-clock. Wall-clock is not admissible on a shared-vCPU machine. This
//! harness follows the same shape as `scheduler_overdue_pass_perf.rs` and
//! `schedule_overdue_aux_perf.rs`: a fresh, uniquely-named, fully-migrated
//! database per measurement. `pg_stat_statements` is reset immediately
//! before each measured call and snapshotted immediately after it.

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration as StdDuration;

use autumn_harvest::context::{WorkflowCommand, WorkflowContext};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::types::ExecutionId;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ── DB bootstrap (mirrors scheduler_overdue_pass_perf.rs) ──────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn create_fresh_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let (prefix, _) = admin_url.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// Seeds one real `harvest_workflow_executions` row (the FK
/// `harvest_task_queue.workflow_exec_id` references) so the enqueued rows
/// below are production-shaped, not orphaned test fixtures.
async fn seed_execution(conn: &mut AsyncPgConnection, workflow_name: &str) -> uuid::Uuid {
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
    }
    let row: IdRow = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, \
              started_at, created_at) \
         VALUES (gen_random_uuid(), $1, $1 || '_id', gen_random_uuid(), 0, 'RUNNING', \
                 '{}'::jsonb, 'default', NOW(), NOW()) \
         RETURNING id",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .get_result(conn)
    .await
    .expect("seed workflow execution");
    row.id
}

/// Builds `n` production-shaped [`EnqueueParams`] for one fan-out decision.
/// The fields mirror what `build_activity_enqueue_plan`
/// (`autumn-harvest/src/worker.rs`) sets for a real `ScheduleActivity`
/// command. Each row gets a distinct activity name and id, a resolved
/// retry policy, a heartbeat timeout, a start-to-close timeout, and
/// ambient context headers.
fn build_fanout_params(exec_id: uuid::Uuid, n: usize) -> Vec<EnqueueParams> {
    (0..n)
        .map(|i| {
            let mut params = EnqueueParams::new(
                "default",
                TaskType::Activity,
                json!({"fan_out_index": i, "payload": "enqueue_batch_perf_fixture"}),
            );
            params.workflow_exec_id = Some(exec_id);
            params.activity_name = Some(format!("fan_out_step_{i}"));
            params.activity_id = Some(uuid::Uuid::new_v4());
            params.priority = 0;
            params.max_attempts = 3;
            params.retry_policy = Some(json!({
                "max_attempts": 3,
                "initial_interval_ms": 1000,
                "backoff_coefficient": 2.0,
                "max_interval_ms": 30_000,
            }));
            params.heartbeat_timeout = Some(chrono::Duration::seconds(30));
            params.start_to_close = Some(chrono::Duration::seconds(60));
            params.context_headers = Some(json!({"trace_id": format!("perf-trace-{i}")}));
            params
        })
        .collect()
}

// ── pg_stat_statements capture (mirrors scheduler_overdue_pass_perf.rs) ────

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
}

async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect(
        "pg_stat_statements_reset(...) failed -- the HARVEST_TEST_DATABASE_URL role must be \
         able to reset statistics (superuser, or granted EXECUTE on this function)",
    );
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

/// Whether `row` is the enqueue `INSERT` this investigation targets.
fn is_enqueue_insert_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("insert into harvest_task_queue") || q.contains("insert into \"harvest_task_queue")
}

/// Whether `row` is the whole request's statement set -- everything the
/// enqueue path issues, so the total is comparable to the target's share.
fn is_request_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    q.contains("harvest_task_queue") || q.contains("pg_notify")
}

// ── Direct measurement: queue::enqueue loop vs queue::enqueue_batch ────────

struct SizePoint {
    n: i64,
    enqueue_calls: i64,
    enqueue_buffers: i64,
    request_calls: i64,
    request_buffers: i64,
    wal_bytes: i64,
}

/// Current WAL insert position, in bytes since the log's start. The
/// difference of two readings around one operation is that operation's
/// WAL cost -- the admissible evidence this persona's charter requires
/// for any write-path claim.
async fn wal_bytes(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct WalRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        bytes: i64,
    }
    let row: WalRow =
        diesel::sql_query("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS bytes")
            .get_result(conn)
            .await
            .expect("read WAL insert position");
    row.bytes
}

async fn measure_one_fanout(admin: &str, label: &str, n: usize) -> SizePoint {
    let db_name = unique(&format!("enqueue_batch_perf_{label}_{n}"));
    let url = create_fresh_db(admin, &db_name).await;

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    let exec_id = seed_execution(&mut seed_conn, &unique("enqueue_batch_perf_wf")).await;
    let params = build_fanout_params(exec_id, n);

    let mut op_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("op connection");
    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    // The one real public entry point under test: `queue::enqueue` (looped,
    // "before") or `queue::enqueue_batch` (one call, "after") -- the exact
    // functions `persist_scheduled_activities` /
    // `persist_mixed_suspension_batch` call to persist a fan-out decision.
    let wal_before = wal_bytes(&mut stats_conn).await;
    let ids = if label == "before" {
        let mut ids = Vec::with_capacity(params.len());
        for p in &params {
            ids.push(
                queue::enqueue(&mut op_conn, p)
                    .await
                    .expect("enqueue should succeed"),
            );
        }
        ids
    } else {
        queue::enqueue_batch(&mut op_conn, &params)
            .await
            .expect("enqueue_batch should succeed")
    };
    let wal_after = wal_bytes(&mut stats_conn).await;
    assert_eq!(ids.len(), n, "every fan-out row must be enqueued");

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let enqueue_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_enqueue_insert_statement(r))
        .collect();
    assert!(
        !enqueue_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the enqueue INSERT shape -- check \
         pg_stat_statements.track and shared_preload_libraries",
    );
    let enqueue_calls: i64 = enqueue_rows.iter().map(|r| r.calls).sum();
    let enqueue_buffers: i64 = enqueue_rows.iter().map(|r| r.total_buffers).sum();
    let request_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_request_statement(r))
        .collect();
    let request_calls: i64 = request_rows.iter().map(|r| r.calls).sum();
    let request_buffers: i64 = request_rows.iter().map(|r| r.total_buffers).sum();

    SizePoint {
        n: i64::try_from(n).unwrap(),
        enqueue_calls,
        enqueue_buffers,
        request_calls,
        request_buffers,
        wal_bytes: wal_after - wal_before,
    }
}

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-activity-fanout-enqueue.md"]
async fn zz_capture_activity_enqueue_batch_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("activity-fanout-enqueue");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: fan-out enqueue, pg_stat_statements call/buffer sweep --\n\
         n\tenqueue_calls\tenqueue_buffers\trequest_calls\trequest_buffers\twal_bytes"
    )];
    for n in [5_usize, 20, 200] {
        let point = measure_one_fanout(&admin, &label, n).await;
        eprintln!(
            "label={label} n={} enqueue_calls={} enqueue_buffers={} request_calls={} \
             request_buffers={} wal_bytes={}",
            point.n,
            point.enqueue_calls,
            point.enqueue_buffers,
            point.request_calls,
            point.request_buffers,
            point.wal_bytes
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            point.n,
            point.enqueue_calls,
            point.enqueue_buffers,
            point.request_calls,
            point.request_buffers,
            point.wal_bytes
        ));
    }
    std::fs::write(
        out_dir.join(format!("{label}-sweep.txt")),
        lines.join("\n") + "\n",
    )
    .expect("write sweep artifact");
    eprintln!("evidence capture complete: label={label}");
}

// ── Equivalence: enqueue_batch matches the per-row enqueue loop exactly ────

/// Proves `enqueue_batch`'s inserted rows agree, column for column and in
/// order, with what the original per-row `enqueue()` loop would have
/// written. The fixture exercises the sticky-pin follow-up too (issue
/// #606's worker-session case), not just the common unpinned path.
#[tokio::test]
async fn enqueue_batch_matches_the_per_row_enqueue_loop() {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::SelectableHelper;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("enqueue_batch_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let exec_id = seed_execution(&mut conn, &unique("enqueue_batch_equiv_wf")).await;

    // "Before": the original per-row loop, called directly against the
    // still-`pub`, unmodified `enqueue()`.
    let mut before_params = build_fanout_params(exec_id, 12);
    // Pin one row sticky, like a worker-session-bound activity would be, so
    // the rare follow-up UPDATE path is exercised on both sides too.
    before_params[3] = before_params[3]
        .clone()
        .with_sticky("perf-worker-0", StdDuration::from_secs(5));
    let mut before_ids = Vec::with_capacity(before_params.len());
    for p in &before_params {
        before_ids.push(queue::enqueue(&mut conn, p).await.expect("enqueue"));
    }
    let mut before_rows: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq_any(&before_ids))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load before rows");
    before_rows.sort_by_key(|r| r.activity_name.clone());
    diesel::delete(harvest_task_queue::table.filter(harvest_task_queue::id.eq_any(&before_ids)))
        .execute(&mut conn)
        .await
        .expect("clear before rows");

    // "After": one enqueue_batch call over an identically-shaped params set.
    let mut after_params = build_fanout_params(exec_id, 12);
    after_params[3] = after_params[3]
        .clone()
        .with_sticky("perf-worker-0", StdDuration::from_secs(5));
    let after_ids = queue::enqueue_batch(&mut conn, &after_params)
        .await
        .expect("enqueue_batch");
    assert_eq!(after_ids.len(), 12);
    let mut after_rows: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq_any(&after_ids))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load after rows");
    after_rows.sort_by_key(|r| r.activity_name.clone());

    assert_eq!(before_rows.len(), after_rows.len());
    for (b, a) in before_rows.iter().zip(after_rows.iter()) {
        assert_eq!(b.queue_name, a.queue_name);
        assert_eq!(b.task_type, a.task_type);
        assert_eq!(b.workflow_exec_id, a.workflow_exec_id);
        assert_eq!(b.activity_name, a.activity_name);
        assert_eq!(b.input, a.input);
        assert_eq!(b.priority, a.priority);
        assert_eq!(b.max_attempts, a.max_attempts);
        assert_eq!(b.retry_policy, a.retry_policy);
        assert_eq!(b.heartbeat_timeout, a.heartbeat_timeout);
        assert_eq!(b.start_to_close, a.start_to_close);
        assert_eq!(b.context_headers, a.context_headers);
        assert_eq!(b.state, a.state);
        // Sticky columns: both rows carry the pin on the same logical
        // (sorted) position, both NULL on the rest.
        assert_eq!(b.sticky_worker_id, a.sticky_worker_id);
        assert_eq!(b.sticky_timeout.is_some(), a.sticky_timeout.is_some());
        assert_eq!(
            b.sticky_until.is_some(),
            a.sticky_until.is_some(),
            "sticky_until must be set (via the follow-up UPDATE) on both sides identically"
        );
    }
}

#[tokio::test]
async fn enqueue_batch_on_empty_slice_is_a_no_op() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("enqueue_batch_empty")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let ids = queue::enqueue_batch(&mut conn, &[])
        .await
        .expect("enqueue_batch on empty slice");
    assert!(ids.is_empty());
}

/// Order must round-trip exactly: `persist_scheduled_activities` zips the
/// returned ids back against `scheduled_activities` positionally
/// (`activity_task_ids` doc comment, `autumn-harvest/src/worker.rs`).
#[tokio::test]
async fn enqueue_batch_returns_ids_in_input_order() {
    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("enqueue_batch_order")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let exec_id = seed_execution(&mut conn, &unique("enqueue_batch_order_wf")).await;
    let params = build_fanout_params(exec_id, 25);
    let ids = queue::enqueue_batch(&mut conn, &params)
        .await
        .expect("enqueue_batch");

    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::SelectableHelper;
    for (i, id) in ids.iter().enumerate() {
        let row: TaskQueueItem = harvest_task_queue::table
            .filter(harvest_task_queue::id.eq(id))
            .select(TaskQueueItem::as_select())
            .get_result(&mut conn)
            .await
            .expect("row for id should exist");
        assert_eq!(
            row.activity_name.as_deref(),
            Some(format!("fan_out_step_{i}").as_str()),
            "ids[{i}] must correspond to params[{i}]"
        );
    }
}

// ── End-to-end: a real workflow's fan-out, driven through Worker::run ──────

fn fan_out_handler<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities: Vec<(String, Value, String)> = (0..10)
            .map(|i| (format!("e2e_step_{i}"), json!(i), "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

/// Drives a real workflow through `run_workflow`'s pure engine, with no
/// database involved. It confirms the fan-out handler emits 10
/// `ScheduleActivity` commands in one suspension. That is the exact shape
/// `persist_scheduled_activities` persists with the now-batched insert.
/// This is the public-entry-point proof: a real workflow decision reaches
/// the batched insert, not only this file's direct `EnqueueParams`
/// fixture above.
#[tokio::test]
async fn fan_out_handler_emits_ten_schedule_activity_commands_in_one_suspension() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::executor::{WorkflowOutcome, run_workflow};

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let outcome = run_workflow(exec_id, history, fan_out_handler, Value::Null).await;
    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            let schedule_count = commands
                .iter()
                .filter(|c| matches!(c, WorkflowCommand::ScheduleActivity { .. }))
                .count();
            assert_eq!(
                schedule_count, 10,
                "fan-out handler must emit 10 ScheduleActivity commands in one suspension"
            );
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}
