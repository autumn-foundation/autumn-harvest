#![cfg(feature = "db")]
//! Ledger performance investigation: `sessions::enforce_broken_sessions`.
//!
//! The scanner selects every `ACTIVE` session whose host may be dead in one
//! statement, then calls `resolve_broken_reason` on each candidate. That
//! helper re-reads the host row, re-reads the owning execution state and,
//! for an expired lease, probes for a `RUNNING` member task. Each candidate
//! therefore costs two or three read round trips before the break
//! transaction starts. After a fleet outage every pinned session is a
//! candidate at once.
//!
//! Evidence is `pg_stat_statements` calls and buffers, never wall-clock.
//! Run once per label (`PERF_LABEL=before` / `after`) and diff the
//! `*-state.txt` artifacts for result equivalence. See
//! `autumn-harvest/scripts/broken_session_scan_perf_repro.sh`.

#![allow(clippy::too_many_lines)]

use std::fmt::Write as _;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{ActivityExecId, ExecutionId, SessionId};
use autumn_harvest::{sessions, store};
use diesel::sql_types::{BigInt, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const STALE_SECS: i64 = 120;

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        // The command replaces the image default, so repeat `fsync=off`.
        .with_cmd([
            "-c",
            "shared_preload_libraries=pg_stat_statements",
            "-c",
            "fsync=off",
        ])
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
    url
}

// ── Fixture ────────────────────────────────────────────────────────────────
//
// Production shape after a partial fleet outage. Sessions are pinned to a
// small set of hosts, so worker rows are shared heavily across sessions.
// Shape is chosen by session index modulo 20.
// Indexes 0 to 8 (45%) have no host row: de-registered or collected.
// Indexes 9 to 11 (15%) have a stale host heartbeat.
// Indexes 12 and 13 (10%) have a Draining or Stopped host.
// Indexes 14 and 15 (10%) have a healthy host and an expired lease with no RUNNING member.
// Index 16 (5%) has a healthy host, an expired lease and a RUNNING member. It is not broken.
// Indexes 17 to 19 (15%) are healthy decoys. They are not candidates.
// Every 7th session also has a terminal owning execution.
// Sessions with an odd index carry a second member task.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Ghost,
    Stale,
    Draining,
    LeaseExpired,
    LeaseExpiredRunning,
    Healthy,
}

const fn shape_of(i: u128) -> Shape {
    match i % 20 {
        0..=8 => Shape::Ghost,
        9..=11 => Shape::Stale,
        12..=13 => Shape::Draining,
        14..=15 => Shape::LeaseExpired,
        16 => Shape::LeaseExpiredRunning,
        _ => Shape::Healthy,
    }
}

const fn exec_uuid(i: u128) -> Uuid {
    Uuid::from_u128((1 << 100) | i)
}
const fn session_uuid(i: u128) -> Uuid {
    Uuid::from_u128((2 << 100) | i)
}
const fn activity_uuid(i: u128, k: u128) -> Uuid {
    Uuid::from_u128((3 << 100) | (i << 4) | k)
}

async fn insert_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    heartbeat_age_secs: i64,
    status: &str,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, \
          max_concurrency, host, status, build_id, labels, max_concurrent_sessions) \
         VALUES ($1, NOW(), NOW() - ($2::bigint * INTERVAL '1 second'), '[]'::jsonb, \
                 '[]'::jsonb, 10, 'localhost', $3, '', '{}'::jsonb, 1)",
    )
    .bind::<Text, _>(worker_id)
    .bind::<BigInt, _>(heartbeat_age_secs)
    .bind::<Text, _>(status)
    .execute(conn)
    .await
    .expect("insert worker");
}

async fn insert_execution(conn: &mut AsyncPgConnection, i: u128) -> ExecutionId {
    let id = exec_uuid(i);
    let wf_id = format!("broken-session-perf-{i}");
    let row = NewWorkflowExecution {
        quota_key: None,
        tenant: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id,
        workflow_name: "session_perf_wf",
        workflow_id: &wf_id,
        run_id: Uuid::from_u128((4 << 100) | i),
        shard_id: 0,
        input: serde_json::json!({}).into(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    ExecutionId::from_uuid(id)
}

async fn seed_fixture(conn: &mut AsyncPgConnection, n: i64) {
    insert_worker(conn, "host-stale-a", 3_600, "Active").await;
    insert_worker(conn, "host-stale-b", 7_200, "Active").await;
    insert_worker(conn, "host-drain-a", 5, "Draining").await;
    insert_worker(conn, "host-drain-b", 5, "Stopped").await;
    insert_worker(conn, "host-ok-a", 5, "Active").await;
    insert_worker(conn, "host-ok-b", 5, "Active").await;

    for i in 0..u128::try_from(n).unwrap() {
        let shape = shape_of(i);
        let host = match shape {
            Shape::Ghost => format!("host-ghost-{}", i % 40),
            Shape::Stale => format!("host-stale-{}", if i % 2 == 0 { "a" } else { "b" }),
            Shape::Draining => format!("host-drain-{}", if i % 2 == 0 { "a" } else { "b" }),
            _ => format!("host-ok-{}", if i % 2 == 0 { "a" } else { "b" }),
        };
        let expires_at = match shape {
            Shape::LeaseExpired | Shape::LeaseExpiredRunning => {
                chrono::Utc::now() - chrono::Duration::hours(1)
            }
            _ => chrono::Utc::now() + chrono::Duration::hours(1),
        };
        let exec_id = insert_execution(conn, i).await;
        let members: u128 = if i % 2 == 1 { 2 } else { 1 };

        store::append_events(
            conn,
            exec_id,
            &[WorkflowEvent::WorkflowStarted {
                input: serde_json::Value::Null,
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            0,
        )
        .await
        .expect("append WorkflowStarted");
        for (offset, k) in (0..members).enumerate() {
            let next_event_id = 1 + i32::try_from(offset).unwrap();
            let activity_id = ActivityExecId::from_uuid(activity_uuid(i, k));
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: "transcode_chunk".to_string(),
                    input: serde_json::Value::Null,
                    queue: "gpu-workers".to_string(),
                }],
                next_event_id,
            )
            .await
            .expect("append ActivityScheduled");

            let mut params =
                EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
            params.workflow_exec_id = Some(exec_id.as_uuid());
            params.activity_name = Some("transcode_chunk".to_string());
            params.activity_id = Some(activity_id.as_uuid());
            let params = params
                .with_session_id(session_uuid(i))
                .with_sticky(&host, std::time::Duration::from_secs(24 * 3600));
            let task_id = queue::enqueue(conn, &params).await.expect("enqueue");
            if shape == Shape::LeaseExpiredRunning && k == 0 {
                diesel::sql_query("UPDATE harvest_task_queue SET state = 'RUNNING' WHERE id = $1")
                    .bind::<SqlUuid, _>(task_id)
                    .execute(conn)
                    .await
                    .expect("mark member RUNNING");
            }
        }

        sessions::record_session_acquired(
            conn,
            SessionId::from_uuid(session_uuid(i)),
            exec_id,
            &host,
            "gpu-workers",
            expires_at,
        )
        .await
        .expect("record_session_acquired");

        // Terminal owning execution on every 7th session.
        if i % 7 == 3 {
            diesel::sql_query(
                "UPDATE harvest_workflow_executions SET state = 'FAILED' WHERE id = $1",
            )
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .expect("mark execution terminal");
        }
    }
    seed_bystanders(conn, n * BYSTANDERS_PER_SESSION).await;
    diesel::sql_query("ANALYZE").execute(conn).await.unwrap();
}

/// Queue rows unrelated to any session. A production queue is mostly these:
/// 90% terminal, 7% pending, 3% running, `session_id` NULL. They are what a
/// `session_id` seek must skip when no index serves it.
const BYSTANDERS_PER_SESSION: i64 = 50;

async fn seed_bystanders(conn: &mut AsyncPgConnection, rows: i64) {
    diesel::sql_query(
        "INSERT INTO harvest_task_queue \
             (id, queue_name, task_type, input, state, scheduled_at, created_at, \
              completed_at, started_at, worker_id) \
         SELECT ('00000099-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid, \
                'default', 'activity', '{}'::jsonb, \
                CASE WHEN g % 100 < 90 THEN 'COMPLETED' \
                     WHEN g % 100 < 97 THEN 'PENDING' ELSE 'RUNNING' END, \
                NOW() - (g || ' seconds')::interval, NOW() - (g || ' seconds')::interval, \
                CASE WHEN g % 100 < 90 THEN NOW() END, \
                CASE WHEN g % 100 >= 97 THEN NOW() END, \
                CASE WHEN g % 100 >= 97 THEN 'bystander-worker' END \
         FROM generate_series(1, $1::bigint) AS g",
    )
    .bind::<BigInt, _>(rows)
    .execute(conn)
    .await
    .expect("seed bystander queue rows");
}

// ── pg_stat_statements capture ─────────────────────────────────────────────

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = Text)]
    query: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = BigInt)]
    total_buffers: i64,
    #[diesel(sql_type = BigInt)]
    temp_blks_written: i64,
    #[diesel(sql_type = BigInt)]
    wal_bytes: i64,
}

async fn reset_stats(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect("pg_stat_statements_reset failed -- needs a superuser role");
}

async fn snapshot(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls, rows, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers, \
                temp_blks_written, wal_bytes::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%' \
         ORDER BY total_buffers DESC, query"
    ))
    .load(conn)
    .await
    .expect("pg_stat_statements must be preloaded")
}

/// Statements issued by the re-verify step: every read the per-candidate
/// helper makes. The candidate scan, the break transaction and its writes
/// are excluded.
fn is_reverify_statement(q: &str) -> bool {
    let q = q.to_ascii_lowercase();
    if q.contains("left join")
        || q.contains("update ")
        || q.contains("insert ")
        || q.contains("for update")
    {
        return false;
    }
    q.contains("harvest_workers")
        || (q.contains("harvest_workflow_executions") && q.contains("state"))
        || (q.contains("exists") && q.contains("harvest_task_queue"))
}

/// The two `harvest_task_queue` seeks keyed on `session_id`: the
/// running-member probe and the member-task load.
fn is_session_seek_statement(q: &str) -> bool {
    let q = q.to_ascii_lowercase();
    q.contains("harvest_task_queue")
        && q.contains("session_id")
        && !q.contains("update ")
        && !q.contains("insert ")
}

#[derive(diesel::QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = Text, column_name = "QUERY PLAN")]
    line: String,
}

async fn explain(conn: &mut AsyncPgConnection, sql: &str) -> String {
    let rows: Vec<PlanLine> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS) {sql}"
    ))
    .load(conn)
    .await
    .expect("explain");
    rows.into_iter().map(|r| r.line + "\n").collect()
}

#[derive(diesel::QueryableByName)]
struct DumpRow {
    #[diesel(sql_type = Text)]
    line: String,
}

/// Deterministic dump of every observable outcome, keyed by fixture index.
async fn dump_state(conn: &mut AsyncPgConnection) -> String {
    let rows: Vec<DumpRow> = diesel::sql_query(
        "SELECT format('session %s state=%s reason=%s broken=%s', \
                       (s.id::text), s.state, coalesce(s.broken_reason, '-'), \
                       (s.broken_at IS NOT NULL)::text) AS line \
         FROM harvest_sessions s \
         UNION ALL \
         SELECT format('task %s session=%s state=%s err=%s', \
                       t.activity_id::text, t.session_id::text, t.state, \
                       coalesce(t.error, '-')) \
         FROM harvest_task_queue t \
         UNION ALL \
         SELECT format('events %s %s', e.workflow_exec_id::text, \
                       string_agg(e.event_type, ',' ORDER BY e.event_id)) \
         FROM harvest_events e GROUP BY e.workflow_exec_id \
         ORDER BY 1",
    )
    .load(conn)
    .await
    .expect("dump state");
    rows.into_iter().map(|r| r.line + "\n").collect()
}

#[derive(diesel::QueryableByName)]
struct Cnt {
    #[diesel(sql_type = BigInt)]
    c: i64,
}

struct Point {
    n: i64,
    total_calls: i64,
    reverify_calls: i64,
    reverify_buffers: i64,
    total_buffers: i64,
    temp_written: i64,
    wal_bytes: i64,
    failed: usize,
    candidates: i64,
    seek_calls: i64,
    seek_buffers: i64,
    probe: EnqueueProbe,
    tq_seq_scans: i64,
    tq_seq_tup_read: i64,
    tq_idx_scans: i64,
}

async fn measure(admin: &str, label: &str, n: i64, out_dir: &std::path::Path, full: bool) -> Point {
    let db_name = format!(
        "broken_session_perf_{label}_{n}_{}",
        Uuid::new_v4().simple()
    );
    let url = create_fresh_db(admin, &db_name).await;
    let mut seed = AsyncPgConnection::establish(&url).await.unwrap();
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(&mut seed)
        .await;
    seed_fixture(&mut seed, n).await;

    let mut pass = AsyncPgConnection::establish(&url).await.unwrap();
    let mut stats = AsyncPgConnection::establish(&url).await.unwrap();

    let candidates = diesel::sql_query(format!(
        "SELECT count(*) AS c FROM ({}) q",
        sessions::broken_session_candidates_query().replace("$1", &STALE_SECS.to_string())
    ))
    .get_result::<Cnt>(&mut stats)
    .await
    .unwrap()
    .c;

    if full {
        let plan = explain(
            &mut stats,
            &sessions::broken_session_candidates_query().replace("$1", &STALE_SECS.to_string()),
        )
        .await;
        std::fs::write(
            out_dir.join(format!("{label}-explain-candidates.txt")),
            plan,
        )
        .unwrap();
    }

    if full {
        let ghost = session_uuid(0);
        let running = session_uuid(16);
        let mut plans = String::new();
        for (name, sql) in [
            (
                "running-member probe",
                format!(
                    "SELECT EXISTS (SELECT 1 FROM harvest_task_queue \
                     WHERE session_id = '{running}' AND state = 'RUNNING')"
                ),
            ),
            (
                "member-task load",
                format!(
                    "SELECT * FROM harvest_task_queue WHERE session_id = '{ghost}' \
                     AND state IN ('PENDING', 'RUNNING')"
                ),
            ),
        ] {
            let _ = writeln!(
                plans,
                "=== {name} ===\n{sql}\n{}",
                explain(&mut stats, &sql).await
            );
        }
        std::fs::write(
            out_dir.join(format!("{label}-explain-seeks-n{n}.txt")),
            plans,
        )
        .unwrap();
    }

    reset_stats(&mut stats, &db_name).await;
    let scans_before = task_queue_scans(&mut stats).await;
    let wal_before = wal_lsn(&mut stats).await;
    let failed =
        sessions::enforce_broken_sessions(&mut pass, STALE_SECS, &PayloadCodecs::default())
            .await
            .expect("enforce_broken_sessions");
    let wal_after = wal_lsn(&mut stats).await;
    let scans_after = task_queue_scans(&mut stats).await;
    if full {
        std::fs::write(
            out_dir.join(format!("{label}-index-stats-n{n}.txt")),
            index_scans(&mut stats).await,
        )
        .unwrap();
    }
    let rows = snapshot(&mut stats, &db_name).await;

    let reverify: Vec<&StatRow> = rows
        .iter()
        .filter(|r| is_reverify_statement(&r.query))
        .collect();
    if full {
        let mut out = String::new();
        for r in &rows {
            let _ = writeln!(
                out,
                "calls={} rows={} hit={} read={} total_buffers={} temp_written={} wal={} reverify={}\n  {}",
                r.calls,
                r.rows,
                r.shared_blks_hit,
                r.shared_blks_read,
                r.total_buffers,
                r.temp_blks_written,
                r.wal_bytes,
                is_reverify_statement(&r.query),
                r.query.replace('\n', " ")
            );
        }
        std::fs::write(out_dir.join(format!("{label}-pgss-n{n}.txt")), out).unwrap();
        std::fs::write(
            out_dir.join(format!("{label}-state-n{n}.txt")),
            dump_state(&mut stats).await,
        )
        .unwrap();
    }
    // The probe adds rows, so it runs after the state dump is written.
    let probe = if full {
        enqueue_wal_cost(&mut seed, &mut stats).await
    } else {
        EnqueueProbe::default()
    };
    Point {
        n,
        total_calls: rows.iter().map(|r| r.calls).sum(),
        reverify_calls: reverify.iter().map(|r| r.calls).sum(),
        reverify_buffers: reverify.iter().map(|r| r.total_buffers).sum(),
        total_buffers: rows.iter().map(|r| r.total_buffers).sum(),
        temp_written: rows.iter().map(|r| r.temp_blks_written).sum(),
        wal_bytes: wal_after - wal_before,
        failed,
        candidates,
        seek_calls: rows
            .iter()
            .filter(|r| is_session_seek_statement(&r.query))
            .map(|r| r.calls)
            .sum(),
        seek_buffers: rows
            .iter()
            .filter(|r| is_session_seek_statement(&r.query))
            .map(|r| r.total_buffers)
            .sum(),
        probe,
        tq_seq_scans: scans_after.seq_scan - scans_before.seq_scan,
        tq_seq_tup_read: scans_after.seq_tup_read - scans_before.seq_tup_read,
        tq_idx_scans: scans_after.idx_scan - scans_before.idx_scan,
    }
}

#[derive(diesel::QueryableByName, Clone, Copy)]
struct TableScans {
    #[diesel(sql_type = BigInt)]
    seq_scan: i64,
    #[diesel(sql_type = BigInt)]
    seq_tup_read: i64,
    #[diesel(sql_type = BigInt)]
    idx_scan: i64,
}

async fn task_queue_scans(conn: &mut AsyncPgConnection) -> TableScans {
    // Cumulative statistics reach the shared view after a short idle delay.
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    diesel::sql_query(
        "SELECT coalesce(seq_scan, 0)::bigint AS seq_scan, \
                coalesce(seq_tup_read, 0)::bigint AS seq_tup_read, \
                coalesce(idx_scan, 0)::bigint AS idx_scan \
         FROM pg_stat_user_tables WHERE relname = 'harvest_task_queue'",
    )
    .get_result(conn)
    .await
    .expect("table stats")
}

async fn index_scans(conn: &mut AsyncPgConnection) -> String {
    #[derive(diesel::QueryableByName)]
    struct I {
        #[diesel(sql_type = Text)]
        line: String,
    }
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    let rows: Vec<I> = diesel::sql_query(
        "SELECT format('%s idx_scan=%s size_bytes=%s', indexrelname, idx_scan, \
                       pg_relation_size(indexrelid)) AS line \
         FROM pg_stat_user_indexes WHERE relname = 'harvest_task_queue' \
         ORDER BY indexrelname",
    )
    .load(conn)
    .await
    .expect("index stats");
    rows.into_iter().map(|r| r.line + "\n").collect()
}

const ENQUEUE_PROBE_ROWS: i64 = 500;

/// WAL bytes for the enqueue probe. Cold is the first pass after a
/// checkpoint, so it includes full-page images. Warm is the second pass.
#[derive(Default, Clone, Copy)]
struct EnqueueProbe {
    pinned_cold: i64,
    pinned_warm: i64,
    plain_cold: i64,
    plain_warm: i64,
}

/// WAL bytes for `ENQUEUE_PROBE_ROWS` session-pinned enqueues and the same
/// number of plain ones. The difference is the write tax of any index keyed
/// on `session_id`.
async fn enqueue_wal_cost(
    writer: &mut AsyncPgConnection,
    meter: &mut AsyncPgConnection,
) -> EnqueueProbe {
    let mut out = EnqueueProbe::default();
    for pinned in [true, false] {
        diesel::sql_query("CHECKPOINT")
            .execute(meter)
            .await
            .expect("checkpoint needs a superuser role");
        let cold = enqueue_pass(writer, meter, pinned).await;
        let warm = enqueue_pass(writer, meter, pinned).await;
        if pinned {
            (out.pinned_cold, out.pinned_warm) = (cold, warm);
        } else {
            (out.plain_cold, out.plain_warm) = (cold, warm);
        }
    }
    out
}

async fn enqueue_pass(
    writer: &mut AsyncPgConnection,
    meter: &mut AsyncPgConnection,
    pinned: bool,
) -> i64 {
    let before = wal_lsn(meter).await;
    for _ in 0..ENQUEUE_PROBE_ROWS {
        let mut params = EnqueueParams::new("probe", TaskType::Activity, serde_json::json!(null));
        if pinned {
            params = params.with_session_id(Uuid::new_v4());
        }
        queue::enqueue(writer, &params)
            .await
            .expect("probe enqueue");
    }
    wal_lsn(meter).await - before
}

async fn wal_lsn(conn: &mut AsyncPgConnection) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct L {
        #[diesel(sql_type = BigInt)]
        v: i64,
    }
    diesel::sql_query("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint AS v")
        .get_result::<L>(conn)
        .await
        .unwrap()
        .v
}

/// Evidence generator, not a CI assertion.
#[tokio::test]
#[ignore = "evidence generator -- see scripts/broken_session_scan_perf_repro.sh"]
async fn zz_capture_broken_session_scan_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("docs")
        .join("perf-artifacts")
        .join("broken-session-scan");
    std::fs::create_dir_all(&out_dir).unwrap();

    let mut table = format!(
        "-- {label}: enforce_broken_sessions, pg_stat_statements sweep --\n\
         n\tcandidates\tfailed_members\ttotal_calls\treverify_calls\treverify_buffers\t\
         total_buffers\ttemp_blks_written\twal_bytes\tseek_calls\tseek_buffers\t\
         pinned_cold_wal\tpinned_warm_wal\tplain_cold_wal\tplain_warm_wal\ttq_seq_scans\ttq_seq_tup_read\t\
         tq_idx_scans\n"
    );
    for n in [100_i64, 400, 1_600] {
        let p = measure(&admin, &label, n, &out_dir, true).await;
        let _ = writeln!(
            table,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            p.n,
            p.candidates,
            p.failed,
            p.total_calls,
            p.reverify_calls,
            p.reverify_buffers,
            p.total_buffers,
            p.temp_written,
            p.wal_bytes,
            p.seek_calls,
            p.seek_buffers,
            p.probe.pinned_cold,
            p.probe.pinned_warm,
            p.probe.plain_cold,
            p.probe.plain_warm,
            p.tq_seq_scans,
            p.tq_seq_tup_read,
            p.tq_idx_scans
        );
    }
    eprintln!("{table}");
    std::fs::write(out_dir.join(format!("{label}-sweep.txt")), table).unwrap();
}
