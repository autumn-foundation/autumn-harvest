#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]
//! The default claim path stays flat as the backlog grows (issue #1971).
//!
//! The claim statement must not scan and sort the whole due backlog on each
//! claim. These tests seed a deep, production-shaped backlog (issue #1956). They
//! then measure the buffers of the real claim statement at 1K, 10K and 100K
//! pending rows. They also check that the claim order does not change.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres.
//! Otherwise a fresh testcontainers Postgres boots with the full bundle.

use autumn_harvest::queue::{self, TaskType};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use futures::FutureExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::panic::AssertUnwindSafe;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Setup ─────────────────────────────────────────────────────────────────────

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (connect(&url).await, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = connect(&url).await;
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");
    (conn, Some(container))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// The worker that claims in every test.
const WORKER: &str = "seek-worker";

/// Rows ahead of the row under test in the ordering tests.
///
/// The value is far above any bounded window the claim may use. So the
/// row under test is never in the first page of its queue.
const DEEP: i32 = 300;

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
}

fn queues(n: usize) -> Vec<String> {
    let run = unique("seekq");
    (0..n).map(|i| format!("{run}-{i}")).collect()
}

/// A SQL `text[]` literal. Test names are `[a-z0-9-]` only.
fn text_array(values: &[String]) -> String {
    let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("ARRAY[{}]::text[]", quoted.join(","))
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    conn.batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"));
}

async fn delete_queues(conn: &mut AsyncPgConnection, queues: &[String]) {
    exec(
        conn,
        &format!(
            "DELETE FROM harvest_task_queue WHERE queue_name = ANY({})",
            text_array(queues)
        ),
    )
    .await;
}

/// Seed a deep, production-shaped backlog of `depth` due rows (issue #1956).
///
/// The shape follows a busy fleet, not a uniform drain:
///
/// - 45% workflow new starts, 35% activity continuations, 10% woken workflow
///   tasks pinned to other workers, 10% activities with a concurrency key.
/// - At most 200 of the pins are live. The others have expired, so any
///   worker may claim them. A live pin lasts one sticky timeout, a few
///   seconds by default. So the live pins follow the claim rate, not the
///   backlog depth.
/// - Priorities are skewed: 80% `0`, 15% `1`, 5% `2`.
/// - Due times spread over the last hour.
/// - `depth / 10` more rows are due in the future, as timers and backoffs are.
/// - `depth / 50` rows are `RUNNING` on the concurrency keys.
/// - `depth / 10` rows inserted and then deleted, and `depth / 50` rows
///   claimed after insert, as a busy table has.
///
/// The seed then runs `VACUUM`, so every run starts from the same index
/// state. Without it, the dead index entries of the claimed rows sit at the
/// queue head until a scan can mark them dead. An open snapshot elsewhere
/// blocks that, and the measurement would depend on it.
async fn seed_deep_backlog(conn: &mut AsyncPgConnection, queues: &[String], depth: i64) {
    let q = text_array(queues);
    let n = queues.len();
    exec(
        conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, attempt, \
                max_attempts, scheduled_at, new_start, sticky_worker_id, sticky_until, \
                concurrency_key, concurrency_cap) \
             SELECT ({q})[1 + (i % {n})], \
                    CASE WHEN i % 20 < 9 OR i % 20 IN (16, 17) THEN 'workflow' ELSE 'activity' END, \
                    CASE WHEN i % 20 < 9 OR i % 20 IN (16, 17) THEN NULL ELSE 'deep-a' || (i % 8) END, \
                    '{{}}'::jsonb, 'PENDING', \
                    CASE WHEN i % 20 = 0 THEN 2 WHEN i % 20 IN (3, 7, 11) THEN 1 ELSE 0 END, \
                    0, 3, \
                    NOW() - make_interval(secs => 1 + (i % 3600)), \
                    i % 20 < 9, \
                    CASE WHEN i % 20 IN (16, 17) THEN 'other-' || (i % 7) END, \
                    CASE WHEN i % 20 IN (16, 17) AND i <= 2000 THEN NOW() + INTERVAL '1 hour' \
                         WHEN i % 20 IN (16, 17) THEN NOW() - INTERVAL '1 minute' END, \
                    CASE WHEN i % 20 IN (18, 19) THEN 'deep-ck-' || (i % 64) END, \
                    CASE WHEN i % 20 IN (18, 19) THEN 1000000 END \
             FROM generate_series(1, {depth}) AS i; \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at) \
             SELECT ({q})[1 + (i % {n})], 'activity', 'deep-timer', '{{}}'::jsonb, 'PENDING', \
                    0, 3, NOW() + make_interval(secs => 60 + (i % 3600)) \
             FROM generate_series(1, {future}) AS i; \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, attempt, \
                max_attempts, worker_id, started_at, concurrency_key, concurrency_cap) \
             SELECT ({q})[1 + (i % {n})], 'activity', 'deep-a0', '{{}}'::jsonb, 'RUNNING', 0, 1, \
                    3, 'holder-' || i, NOW(), 'deep-ck-' || (i % 64), 1000000 \
             FROM generate_series(1, {running}) AS i; \
             WITH doomed AS ( \
               INSERT INTO harvest_task_queue \
                 (queue_name, task_type, activity_name, input, state, max_attempts) \
               SELECT ({q})[1 + (i % {n})], 'activity', 'deep-dead', '{{}}'::jsonb, \
                      'PENDING', 3 \
               FROM generate_series(1, {dead}) AS i RETURNING id) \
             DELETE FROM harvest_task_queue WHERE id IN (SELECT id FROM doomed); \
             UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'drained', \
                    attempt = 1, started_at = NOW() \
             WHERE id IN (SELECT id FROM harvest_task_queue \
                          WHERE queue_name = ANY({q}) AND state = 'PENDING' \
                            AND scheduled_at <= NOW() \
                          ORDER BY priority DESC, scheduled_at LIMIT {drained}); \
             ANALYZE harvest_task_queue;",
            future = depth / 10,
            running = depth / 50,
            dead = depth / 10,
            drained = depth / 50,
        ),
    )
    .await;
    exec(conn, "VACUUM harvest_task_queue").await;
}

/// The buffers and temp blocks of one real claim statement.
#[derive(Debug, Clone, Copy)]
struct ClaimCost {
    /// Shared buffers the plan touched (hit plus read).
    buffers: i64,
    /// Temp blocks the plan wrote. A sort that spills writes them.
    temp_written: i64,
    /// Whether the statement claimed a row.
    claimed: bool,
    /// Whether the full scan read any table row.
    fallback_ran: bool,
}

/// Whether the plan subtree under `name` scanned `harvest_task_queue`.
fn cte_scanned_the_table(plan: &serde_json::Value, name: &str, inside: bool) -> bool {
    let inside = inside || plan["Subplan Name"].as_str() == Some(&format!("CTE {name}"));
    let scanned = inside
        && plan["Relation Name"].as_str() == Some("harvest_task_queue")
        && plan["Actual Loops"].as_i64().unwrap_or(0) > 0;
    scanned
        || plan["Plans"]
            .as_array()
            .is_some_and(|plans| plans.iter().any(|p| cte_scanned_the_table(p, name, inside)))
}

/// Run `EXPLAIN (ANALYZE, BUFFERS)` of the claim statement that
/// [`queue::claim_task`] sends, and roll the claim back.
///
/// The statement is prepared with typed parameters, under the planner
/// settings of the claim transaction. So the plan is the generic plan that a
/// real claim reuses, not a plan for substituted literals.
async fn claim_cost(conn: &mut AsyncPgConnection, queues: &[String]) -> ClaimCost {
    claim_cost_of(conn, queues, queue::claim_task_query()).await
}

/// [`claim_cost`] for one form of the claim query.
async fn claim_cost_of(conn: &mut AsyncPgConnection, queues: &[String], sql: &str) -> ClaimCost {
    #[derive(diesel::QueryableByName)]
    struct Plan {
        #[diesel(sql_type = diesel::sql_types::Json, column_name = "QUERY PLAN")]
        plan: serde_json::Value,
    }
    exec(
        conn,
        &format!(
            "BEGIN; {}; \
             PREPARE seek_probe(text, text[], text, bigint, text[], text[]) AS {}",
            queue::CLAIM_PLAN_SETTINGS_SQL,
            sql
        ),
    )
    .await;
    let rows: Vec<Plan> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE seek_probe(\
         '{WORKER}', {}, '', NULL, ARRAY[]::text[], ARRAY[]::text[])",
        text_array(queues)
    ))
    .load(conn)
    .await
    .expect("explain claim");
    exec(conn, "ROLLBACK; DEALLOCATE seek_probe;").await;
    let root = &rows[0].plan[0]["Plan"];
    let num = |key: &str| root[key].as_i64().unwrap_or(0);
    ClaimCost {
        buffers: num("Shared Hit Blocks") + num("Shared Read Blocks"),
        temp_written: num("Temp Written Blocks"),
        claimed: num("Actual Rows") == 1,
        fallback_ran: cte_scanned_the_table(root, "legacy_candidate", false),
    }
}

/// Claim through the public API and return the claimed row id.
async fn claim(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    aging: Option<u32>,
) -> Option<Uuid> {
    queue::claim_task(conn, queues, WORKER, "", aging, &[], &[])
        .await
        .expect("claim")
        .map(|t| t.id)
}

async fn insert_row(conn: &mut AsyncPgConnection, columns: &str, values: &str) -> Uuid {
    #[derive(diesel::QueryableByName)]
    struct Id {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }
    let row: Id = diesel::sql_query(format!(
        "INSERT INTO harvest_task_queue (input, state, max_attempts, {columns}) \
         VALUES ('{{}}'::jsonb, 'PENDING', 3, {values}) RETURNING id"
    ))
    .get_result(conn)
    .await
    .expect("insert row");
    row.id
}

// ── Backlog depth (issue #1971 AC1, AC2) ─────────────────────────────────────

/// Claim cost must stay roughly flat from 1K to 100K pending rows.
///
/// Before issue #1971, the claim scanned and sorted every due row. Its buffers
/// grew about linearly with depth, and the sort spilled to disk at 100K.
///
/// Each depth runs three claims, each rolled back. The assertion reads the
/// median, which is the steady state. The first claim after the seed also
/// walks the dead index entries of the rows the seed drained, and marks them
/// dead for later scans. Its cost is reported, not asserted.
#[tokio::test]
async fn claim_buffers_stay_flat_from_1k_to_100k_pending_rows() {
    let (mut conn, _container) = setup_db().await;
    let mut costs = Vec::new();
    for depth in [1_000_i64, 10_000, 100_000] {
        let qs = queues(4);
        seed_deep_backlog(&mut conn, &qs, depth).await;
        let mut runs = Vec::new();
        for _ in 0..3 {
            runs.push(claim_cost(&mut conn, &qs).await);
        }
        delete_queues(&mut conn, &qs).await;
        eprintln!("depth={depth} claim costs, first run cold: {runs:?}");
        for cost in &runs {
            assert!(cost.claimed, "depth {depth}: the claim takes a row");
            assert!(
                !cost.fallback_ran,
                "depth {depth}: the window decides: {cost:?}"
            );
            assert_eq!(
                cost.temp_written, 0,
                "the claim spills to disk at depth {depth}: {cost:?}"
            );
        }
        let mut buffers: Vec<i64> = runs.iter().map(|c| c.buffers).collect();
        buffers.sort_unstable();
        costs.push((depth, buffers[1]));
    }
    let shallow = costs[0].1.max(1);
    for (depth, median) in &costs {
        assert!(
            *median <= 3 * shallow,
            "claim buffers grow with backlog depth: median {median} at depth {depth} \
             against {shallow} at depth 1000 (all medians: {costs:?})"
        );
    }
}

/// A large activity-pause array must not make the claim sort spill (issue
/// #1215).
///
/// The 199 ballast pauses name no seeded activity, so they exclude no row.
/// Issue #1215 measured a disk spill at this array size from 1K rows up.
#[tokio::test]
async fn a_large_activity_pause_array_does_not_spill_the_claim_at_100k() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(4);
    let ballast = unique("ballast");
    seed_deep_backlog(&mut conn, &qs, 100_000).await;
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_activity_pauses (activity_name, paused_by, reason) \
             SELECT '{ballast}-' || i, 'seek-test', 'issue 1215 ballast' \
             FROM generate_series(1, 199) AS i"
        ),
    )
    .await;
    // The pauses are global. A panic must not leave them for later tests.
    let cost = AssertUnwindSafe(claim_cost(&mut conn, &qs))
        .catch_unwind()
        .await;
    exec(
        &mut conn,
        &format!("DELETE FROM harvest_activity_pauses WHERE activity_name LIKE '{ballast}-%'"),
    )
    .await;
    delete_queues(&mut conn, &qs).await;
    let cost = cost.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    eprintln!("199 paused activities, depth=100000 claim cost: {cost:?}");
    assert!(cost.claimed, "the ballast pauses exclude no row");
    assert!(!cost.fallback_ran, "the window decides: {cost:?}");
    assert_eq!(cost.temp_written, 0, "the claim sort spills: {cost:?}");
}

// ── Claim order (issue #1971 AC3, AC4) ───────────────────────────────────────

/// A continuation behind a deep storm of new starts still claims first
/// (issue #1824).
#[tokio::test]
async fn a_continuation_behind_a_new_start_storm_claims_first() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, input, state, max_attempts, scheduled_at, new_start) \
             SELECT '{q}', 'workflow', '{{}}'::jsonb, 'PENDING', 3, \
                    NOW() - INTERVAL '20 seconds' - make_interval(secs => i / 1000.0), TRUE \
             FROM generate_series(1, {DEEP}) AS i"
        ),
    )
    .await;
    let continuation = insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, scheduled_at",
        &format!("'{q}', 'activity', 'noop', NOW() - INTERVAL '10 seconds'"),
    )
    .await;
    let claimed = claim(&mut conn, &qs, None).await;
    delete_queues(&mut conn, &qs).await;
    assert_eq!(
        claimed,
        Some(continuation),
        "the continuation is due 10 s after the starts, inside the 30 s handicap"
    );
}

/// A row pinned to this worker claims first, wherever it sits in the backlog.
#[tokio::test]
async fn a_row_pinned_to_this_worker_claims_first_from_deep_in_the_backlog() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at) \
             SELECT '{q}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 2, 3, \
                    NOW() - INTERVAL '1 hour' \
             FROM generate_series(1, {DEEP}) AS i"
        ),
    )
    .await;
    let pinned = insert_row(
        &mut conn,
        "queue_name, task_type, priority, scheduled_at, sticky_worker_id, sticky_until",
        &format!(
            "'{q}', 'workflow', 0, NOW() - INTERVAL '1 second', '{WORKER}', \
             NOW() + INTERVAL '1 hour'"
        ),
    )
    .await;
    let claimed = claim(&mut conn, &qs, None).await;
    delete_queues(&mut conn, &qs).await;
    assert_eq!(
        claimed,
        Some(pinned),
        "a live pin to this worker sorts first"
    );
}

/// An eligible row behind a saturated head beats a lower-priority queue.
///
/// The head of queue A is a deep run of rows on a saturated concurrency key.
/// No page-bounded claim may let queue B win on a row that sorts later.
#[tokio::test]
async fn an_eligible_row_behind_a_saturated_head_beats_a_lower_priority_queue() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(2);
    let (a, b) = (&qs[0], &qs[1]);
    let key = unique("sat");
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at, concurrency_key, concurrency_cap) \
             SELECT '{a}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 2, 3, \
                    NOW() - INTERVAL '1 hour', '{key}', 1 \
             FROM generate_series(1, {DEEP}) AS i; \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, attempt, \
                max_attempts, worker_id, started_at, concurrency_key, concurrency_cap) \
             VALUES ('{a}', 'activity', 'noop', '{{}}'::jsonb, 'RUNNING', 2, 1, 3, 'holder', \
                     NOW(), '{key}', 1)"
        ),
    )
    .await;
    let behind = insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, priority, scheduled_at",
        &format!("'{a}', 'activity', 'noop', 2, NOW() - INTERVAL '1 minute'"),
    )
    .await;
    insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, priority, scheduled_at",
        &format!("'{b}', 'activity', 'noop', 0, NOW() - INTERVAL '2 hours'"),
    )
    .await;
    let claimed = claim(&mut conn, &qs, None).await;
    delete_queues(&mut conn, &qs).await;
    assert_eq!(claimed, Some(behind), "priority 2 beats priority 0");
}

/// Priority ageing still lifts an old low-priority row over a deep
/// high-priority backlog (issue #249).
#[tokio::test]
async fn ageing_lifts_an_old_row_over_a_deep_high_priority_backlog() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                scheduled_at) \
             SELECT '{q}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 2, 3, \
                    NOW() - INTERVAL '1 second' \
             FROM generate_series(1, {DEEP}) AS i"
        ),
    )
    .await;
    let old = insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, priority, scheduled_at",
        &format!("'{q}', 'activity', 'noop', -1, NOW() - INTERVAL '1 hour'"),
    )
    .await;
    let claimed = claim(&mut conn, &qs, Some(60)).await;
    delete_queues(&mut conn, &qs).await;
    assert_eq!(
        claimed,
        Some(old),
        "one hour at 60 s ageing adds 60 priority"
    );
}

/// Rows of an ineligible or saturated activity type at the head do not hide
/// the next eligible row (issues #1836, #1971).
///
/// The `$6` array names `nohandler` as ineligible and marks `busy` as
/// saturated. A `nohandler` row with capability requirements is exempt from
/// the `$6` gate, so it is the first eligible row.
#[tokio::test]
async fn ineligible_and_saturated_types_at_the_head_do_not_hide_the_next_row() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, max_attempts, scheduled_at) \
             SELECT '{q}', 'activity', CASE WHEN i % 2 = 0 THEN 'busy' ELSE 'nohandler' END, \
                    '{{}}'::jsonb, 'PENDING', 3, NOW() - INTERVAL '1 hour' \
             FROM generate_series(1, {DEEP}) AS i"
        ),
    )
    .await;
    let exempt = insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, required_capabilities, scheduled_at",
        &format!("'{q}', 'activity', 'nohandler', '[]'::jsonb, NOW() - INTERVAL '2 minutes'"),
    )
    .await;
    let plain = insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, scheduled_at",
        &format!("'{q}', 'activity', 'ok', NOW() - INTERVAL '1 minute'"),
    )
    .await;
    let exclusions = ["nohandler".to_string(), "\u{1}busy".to_string()];
    let mut claimed = Vec::new();
    for _ in 0..3 {
        claimed.push(
            queue::claim_task(&mut conn, &qs, WORKER, "", None, &[], &exclusions)
                .await
                .expect("claim")
                .map(|t| t.id),
        );
    }
    delete_queues(&mut conn, &qs).await;
    assert_eq!(claimed, vec![Some(exempt), Some(plain), None]);
}

/// A backlog of one activity type keeps the window bounded (issue #1971).
///
/// With one activity name in the table, the planner can misjudge an
/// activity-name gate whose array it cannot read at plan time. It then reads
/// the whole head instead of the window. The cost must stay flat.
#[tokio::test]
async fn a_single_activity_type_backlog_keeps_the_window_bounded() {
    let (mut conn, _container) = setup_db().await;
    let mut costs = Vec::new();
    for depth in [1_000_i64, 100_000] {
        let qs = queues(4);
        exec(
            &mut conn,
            &format!(
                "INSERT INTO harvest_task_queue \
                   (queue_name, task_type, activity_name, input, state, max_attempts, \
                    scheduled_at) \
                 SELECT ({q})[1 + (i % 4)], 'activity', 'single', '{{}}'::jsonb, 'PENDING', 3, \
                        NOW() - make_interval(secs => 1 + (i % 3600)) \
                 FROM generate_series(1, {depth}) AS i; \
                 ANALYZE harvest_task_queue;",
                q = text_array(&qs)
            ),
        )
        .await;
        let mut runs = Vec::new();
        for _ in 0..3 {
            runs.push(claim_cost(&mut conn, &qs).await);
        }
        delete_queues(&mut conn, &qs).await;
        eprintln!("single type, depth={depth} claim costs: {runs:?}");
        let mut buffers: Vec<i64> = runs.iter().map(|c| c.buffers).collect();
        buffers.sort_unstable();
        costs.push(buffers[1]);
    }
    assert!(
        costs[1] <= 3 * costs[0].max(1),
        "claim buffers grow with depth on a one-type backlog: {costs:?}"
    );
}

/// Stale statistics do not make the claim read the backlog (issue #1971).
///
/// A drained system keeps its terminal rows for a while. An `ANALYZE` then
/// sees almost no `PENDING` row. A burst below the autoanalyze threshold
/// leaves those statistics in place. The claim plan must stay bounded
/// anyway: a join to the window once put the table on the outer side.
#[tokio::test]
async fn stale_statistics_keep_the_window_bounded() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(4);
    let q = text_array(&qs);
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, max_attempts, \
                scheduled_at, completed_at) \
             SELECT ({q})[1 + (i % 4)], 'activity', 'noop', '{{}}'::jsonb, 'COMPLETED', 3, \
                    NOW() - INTERVAL '1 day', NOW() \
             FROM generate_series(1, 400000) AS i; \
             INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, max_attempts, scheduled_at) \
             SELECT ({q})[1 + (i % 4)], 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 3, \
                    NOW() - INTERVAL '1 hour' \
             FROM generate_series(1, 20) AS i"
        ),
    )
    .await;
    exec(&mut conn, "VACUUM ANALYZE harvest_task_queue").await;
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, max_attempts, \
                scheduled_at, new_start) \
             SELECT ({q})[1 + (i % 4)], \
                    CASE WHEN i % 2 = 0 THEN 'workflow' ELSE 'activity' END, \
                    CASE WHEN i % 2 = 0 THEN NULL ELSE 'noop' END, \
                    '{{}}'::jsonb, 'PENDING', 3, \
                    NOW() - make_interval(secs => i % 3600), i % 2 = 0 \
             FROM generate_series(1, 30000) AS i"
        ),
    )
    .await;
    exec(&mut conn, "VACUUM harvest_task_queue").await;
    let stale = claim_cost(&mut conn, &qs).await;
    exec(&mut conn, "ANALYZE harvest_task_queue").await;
    let fresh = claim_cost(&mut conn, &qs).await;
    delete_queues(&mut conn, &qs).await;
    eprintln!("stale statistics: {stale:?}, fresh: {fresh:?}");
    assert!(stale.claimed && !stale.fallback_ran, "{stale:?}");
    assert!(
        stale.buffers <= 3 * fresh.buffers.max(1),
        "stale statistics change the claim plan: {stale:?} against {fresh:?}"
    );
}

/// A kind-filtered claim stays flat behind a deep run of the other kind
/// (issues #1787, #1971).
///
/// A saturated worker claims for one kind only. The index keys the heads by
/// task type, so the claim never walks the rows of the other kind.
#[tokio::test]
async fn a_kind_filtered_claim_stays_flat_behind_the_other_kind() {
    let (mut conn, _container) = setup_db().await;
    let mut costs = Vec::new();
    for depth in [1_000_i64, 100_000] {
        let qs = queues(4);
        exec(
            &mut conn,
            &format!(
                "INSERT INTO harvest_task_queue \
                   (queue_name, task_type, activity_name, input, state, max_attempts, \
                    scheduled_at) \
                 SELECT ({q})[1 + (i % 4)], 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 3, \
                        NOW() - INTERVAL '1 hour' - make_interval(secs => i % 3600) \
                 FROM generate_series(1, {depth}) AS i; \
                 INSERT INTO harvest_task_queue \
                   (queue_name, task_type, input, state, max_attempts, scheduled_at) \
                 SELECT ({q})[1 + (i % 4)], 'workflow', '{{}}'::jsonb, 'PENDING', 3, \
                        NOW() - INTERVAL '1 second' \
                 FROM generate_series(1, 100) AS i; \
                 ANALYZE harvest_task_queue;",
                q = text_array(&qs)
            ),
        )
        .await;
        let sql = queue::claim_task_query_for_kind(TaskType::Workflow, false);
        let mut runs = Vec::new();
        for _ in 0..3 {
            runs.push(claim_cost_of(&mut conn, &qs, sql).await);
        }
        delete_queues(&mut conn, &qs).await;
        eprintln!("workflow claim behind {depth} activities: {runs:?}");
        for cost in &runs {
            assert!(cost.claimed && !cost.fallback_ran, "{cost:?}");
        }
        let mut buffers: Vec<i64> = runs.iter().map(|c| c.buffers).collect();
        buffers.sort_unstable();
        costs.push(buffers[1]);
    }
    assert!(
        costs[1] <= 3 * costs[0].max(1),
        "a kind-filtered claim grows with the other kind: {costs:?}"
    );
}

/// A kind-filtered claim finds its kind behind a deep run of the other kind
/// (issue #1787).
#[tokio::test]
async fn a_kind_filtered_claim_finds_its_kind_behind_the_other_kind() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, activity_name, input, state, max_attempts, scheduled_at) \
             SELECT '{q}', 'activity', 'noop', '{{}}'::jsonb, 'PENDING', 3, \
                    NOW() - INTERVAL '1 hour' \
             FROM generate_series(1, {DEEP}) AS i"
        ),
    )
    .await;
    let workflow = insert_row(
        &mut conn,
        "queue_name, task_type, scheduled_at",
        &format!("'{q}', 'workflow', NOW() - INTERVAL '1 second'"),
    )
    .await;
    let claimed = queue::claim_task_of_kind_on_shard(
        &mut conn,
        &qs,
        WORKER,
        "",
        None,
        &[],
        &[],
        None,
        Some(TaskType::Workflow),
    )
    .await
    .expect("claim")
    .map(|t| t.id);
    delete_queues(&mut conn, &qs).await;
    assert_eq!(
        claimed,
        Some(workflow),
        "the workflow claimer skips activities"
    );
}

/// The claim sets its planner settings for its own transaction only (issue
/// #1971).
///
/// The connection keeps its session settings after the claim, whether the
/// claim takes a row or not.
#[tokio::test]
async fn the_claim_leaves_the_session_planner_settings_unchanged() {
    #[derive(diesel::QueryableByName)]
    struct Settings {
        #[diesel(sql_type = diesel::sql_types::Text)]
        jit: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        plan_cache_mode: String,
    }
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    insert_row(
        &mut conn,
        "queue_name, task_type, activity_name, scheduled_at",
        &format!("'{q}', 'activity', 'noop', NOW() - INTERVAL '1 second'"),
    )
    .await;
    exec(&mut conn, "SET jit = on; SET plan_cache_mode = auto").await;
    let claimed = claim(&mut conn, &qs, None).await;
    let empty = claim(&mut conn, &qs, None).await;
    let after: Settings = diesel::sql_query(
        "SELECT current_setting('jit') AS jit, \
                current_setting('plan_cache_mode') AS plan_cache_mode",
    )
    .get_result(&mut conn)
    .await
    .expect("read settings");
    exec(&mut conn, "RESET jit; RESET plan_cache_mode").await;
    delete_queues(&mut conn, &qs).await;
    assert!(claimed.is_some() && empty.is_none(), "one row, then none");
    assert_eq!(after.jit, "on", "SET LOCAL must not leak into the session");
    assert_eq!(after.plan_cache_mode, "auto", "SET LOCAL must not leak");
}

/// The default claim reuses one prepared statement per connection (issue
/// #1971).
///
/// The claim statement costs more to plan than to run. A statement that is
/// prepared again on every claim pays that cost every time.
#[tokio::test]
async fn the_claim_statement_is_prepared_once_per_connection() {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    async fn cached(conn: &mut AsyncPgConnection) -> i64 {
        let row: Count = diesel::sql_query(
            "SELECT count(*)::bigint AS n FROM pg_prepared_statements \
             WHERE statement LIKE '%seek_heads AS MATERIALIZED%'",
        )
        .get_result(conn)
        .await
        .expect("prepared statements");
        row.n
    }
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let mut counts = Vec::new();
    for _ in 0..4 {
        for _ in 0..3 {
            claim(&mut conn, &qs, None).await;
        }
        counts.push(cached(&mut conn).await);
    }
    eprintln!("cached claim statements after 3, 6, 9 and 12 claims: {counts:?}");
    assert!(
        counts[0] >= 1,
        "the claim statement must be cached: {counts:?}"
    );
    assert!(
        counts.iter().all(|n| *n == counts[0]),
        "claims must reuse the cached statement: {counts:?}"
    );
}

/// A column added while a claim statement is cached does not break the
/// claim (issue #1971).
///
/// A rolling migration may add a column to `harvest_task_queue` while
/// workers run. A cached statement whose result columns follow the table
/// would then fail with "cached plan must not change result type".
#[tokio::test]
async fn an_added_column_does_not_break_a_cached_claim() {
    let (mut conn, _container) = setup_db().await;
    let qs = queues(1);
    let q = &qs[0];
    let column = format!("seek_probe_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let mut ids = Vec::new();
    for _ in 0..2 {
        ids.push(
            insert_row(
                &mut conn,
                "queue_name, task_type, activity_name, scheduled_at",
                &format!("'{q}', 'activity', 'noop', NOW() - INTERVAL '1 second'"),
            )
            .await,
        );
    }
    let first = claim(&mut conn, &qs, None).await;
    exec(
        &mut conn,
        &format!("ALTER TABLE harvest_task_queue ADD COLUMN {column} INTEGER"),
    )
    .await;
    let second = queue::claim_task(&mut conn, &qs, WORKER, "", None, &[], &[]).await;
    exec(
        &mut conn,
        &format!("ALTER TABLE harvest_task_queue DROP COLUMN {column}"),
    )
    .await;
    delete_queues(&mut conn, &qs).await;
    let second = second
        .expect("the claim after the migration must not fail")
        .map(|t| t.id);
    let mut claimed = vec![first, second];
    claimed.sort();
    ids.sort();
    assert_eq!(
        claimed,
        ids.into_iter().map(Some).collect::<Vec<_>>(),
        "the two claims take the two rows"
    );
}

/// The claim key of the best eligible row, by the reference order.
///
/// The reference is a plain scan and sort over every due row. It applies the
/// gates that [`randomized_drains_follow_the_reference_order`] seeds.
async fn reference_best(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    paused: &str,
    saturated: &str,
) -> Option<(i32, i32, f64)> {
    #[derive(diesel::QueryableByName)]
    struct Key {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        sticky_rank: i32,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        priority: i32,
        #[diesel(sql_type = diesel::sql_types::Double)]
        due: f64,
    }
    let rows: Vec<Key> = diesel::sql_query(format!(
        "SELECT CASE WHEN sticky_worker_id = '{WORKER}' AND sticky_until > NOW() \
                     THEN 1 ELSE 0 END AS sticky_rank, \
                priority, \
                EXTRACT(EPOCH FROM {due})::float8 AS due \
         FROM harvest_task_queue \
         WHERE queue_name = ANY({q}) AND state = 'PENDING' AND scheduled_at <= NOW() \
           AND (sticky_worker_id IS NULL OR sticky_worker_id = '{WORKER}' \
                OR sticky_until <= NOW()) \
           AND (activity_name IS NULL OR activity_name <> '{paused}') \
           AND (concurrency_key IS NULL OR concurrency_key <> '{saturated}') \
         ORDER BY 1 DESC, 2 DESC, 3 ASC LIMIT 1",
        due = queue::CLAIM_ORDER_DUE_SQL,
        q = text_array(queues),
    ))
    .load(conn)
    .await
    .expect("reference order");
    rows.into_iter()
        .next()
        .map(|k| (k.sticky_rank, k.priority, k.due))
}

/// The claim key of a claimed row, as it was before the claim.
///
/// The claim adds one to `attempt`, so the due term tests `attempt = 1`.
async fn key_of(conn: &mut AsyncPgConnection, id: Uuid) -> (i32, i32, f64) {
    #[derive(diesel::QueryableByName)]
    struct Key {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        sticky_rank: i32,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        priority: i32,
        #[diesel(sql_type = diesel::sql_types::Double)]
        due: f64,
    }
    let k: Key = diesel::sql_query(format!(
        "SELECT CASE WHEN sticky_worker_id = '{WORKER}' AND sticky_until > NOW() \
                     THEN 1 ELSE 0 END AS sticky_rank, \
                priority, EXTRACT(EPOCH FROM {due})::float8 AS due \
         FROM harvest_task_queue WHERE id = $1",
        due = queue::CLAIM_ORDER_DUE_SQL.replace("attempt = 0", "attempt = 1"),
    ))
    .bind::<diesel::sql_types::Uuid, _>(id)
    .get_result(conn)
    .await
    .expect("claimed key");
    (k.sticky_rank, k.priority, k.due)
}

/// Random backlogs drain in the reference order.
///
/// Each round seeds two queues with a random mix of rows. The mix covers
/// priorities, new starts, pins to this worker and to others, a paused
/// activity and a saturated concurrency key. Then it drains part of the
/// backlog. Each claim must take
/// a row with the best reference key. Ties may go either way.
#[tokio::test]
async fn randomized_drains_follow_the_reference_order() {
    let (mut conn, _container) = setup_db().await;
    let paused = unique("paused");
    let saturated = unique("sat");
    exec(
        &mut conn,
        &format!(
            "INSERT INTO harvest_activity_pauses (activity_name, paused_by, reason) \
             VALUES ('{paused}', 'seek-test', 'randomized drain')"
        ),
    )
    .await;
    // The pause is global. A panic must not leave it for later tests.
    let drains = AssertUnwindSafe(async {
        for seed in 0..6_u64 {
            let mut rng = StdRng::seed_from_u64(1971 + seed);
            let qs = queues(2);
            let mut values = Vec::new();
            for _ in 0..rng.gen_range(150..400) {
                let q = &qs[rng.gen_range(0..qs.len())];
                let priority = [0, 0, 0, 1, 2][rng.gen_range(0..5)];
                let age = rng.gen_range(0..120);
                let kind = rng.gen_range(0..10);
                let (task_type, activity, new_start) = match kind {
                    0..=3 => ("workflow", "NULL".to_string(), "TRUE"),
                    4 => ("activity", format!("'{paused}'"), "FALSE"),
                    _ => ("activity", "'noop'".to_string(), "FALSE"),
                };
                let (pin, until) = match rng.gen_range(0..10) {
                    0 => (format!("'{WORKER}'"), "NOW() + INTERVAL '1 hour'"),
                    1 | 2 => ("'someone-else'".to_string(), "NOW() + INTERVAL '1 hour'"),
                    3 => ("'someone-else'".to_string(), "NOW() - INTERVAL '1 minute'"),
                    _ => ("NULL".to_string(), "NULL"),
                };
                // The cap counts per key and task type. The holder is an
                // activity, so only an activity row is saturated.
                let (key, cap) = if task_type == "activity" && rng.gen_range(0..4) == 0 {
                    (format!("'{saturated}'"), "1")
                } else {
                    ("NULL".to_string(), "NULL")
                };
                values.push(format!(
                    "('{q}', '{task_type}', {activity}, '{{}}'::jsonb, 'PENDING', {priority}, 3, \
                     NOW() - make_interval(secs => {age}), {new_start}, {pin}, {until}, {key}, {cap})"
                ));
            }
            exec(
                &mut conn,
                &format!(
                    "INSERT INTO harvest_task_queue \
                       (queue_name, task_type, activity_name, input, state, priority, max_attempts, \
                        scheduled_at, new_start, sticky_worker_id, sticky_until, concurrency_key, \
                        concurrency_cap) VALUES {}; \
                     INSERT INTO harvest_task_queue \
                       (queue_name, task_type, activity_name, input, state, attempt, max_attempts, \
                        worker_id, started_at, concurrency_key, concurrency_cap) \
                     VALUES ('{q0}', 'activity', 'noop', '{{}}'::jsonb, 'RUNNING', 1, 3, 'holder', \
                             NOW(), '{saturated}', 1)",
                    values.join(", "),
                    q0 = qs[0],
                ),
            )
            .await;
            for step in 0..60 {
                let expected = reference_best(&mut conn, &qs, &paused, &saturated).await;
                let claimed = claim(&mut conn, &qs, None).await;
                match (expected, claimed) {
                    (None, None) => break,
                    (Some(want), Some(id)) => {
                        let got = key_of(&mut conn, id).await;
                        assert!(
                            got.0 == want.0 && got.1 == want.1 && (got.2 - want.2).abs() < 1e-3,
                            "seed {seed} step {step}: claimed key {got:?}, reference best {want:?}"
                        );
                    }
                    (want, got) => {
                        panic!("seed {seed} step {step}: reference {want:?}, claimed {got:?}")
                    }
                }
            }
            delete_queues(&mut conn, &qs).await;
        }
    })
    .catch_unwind()
    .await;
    exec(
        &mut conn,
        &format!("DELETE FROM harvest_activity_pauses WHERE activity_name = '{paused}'"),
    )
    .await;
    drains.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}
