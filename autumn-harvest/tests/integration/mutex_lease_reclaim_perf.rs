#![cfg(feature = "db")]
//! Ledger performance investigation: `mutex::reclaim_expired_leases_and_wake`.
//!
//! `mutex.rs`'s module doc names two other per-holder loops:
//! `release_all_locks_for_holder` and `delete_waiters_for_holder`. Both
//! carry the same shape as the one this file targets. Both bound the loop
//! to the number of keys *one execution* holds or waits on — small in the
//! common case.
//!
//! `reclaim_expired_leases_and_wake` has no such bound. It is the lease
//! scanner's crash-recovery sweep, called from
//! `timeout::enforce_timeouts_once` on every worker's periodic timeout
//! tick, over **every** expired lease on the shard at once. A worker-fleet
//! restart, or a rolling deploy while many workflows hold `ctx.mutex()`
//! locks, is exactly the burst this function exists to drain. This repo's
//! own performance playbook names the shape directly: bookkeeping queries
//! that are individually trivial but collectively dominant. They do not
//! show up in a buffers ranking, only in a `calls` ranking
//! (`docs/performance.md`).
//!
//! The advisory-first lock stays untouched by this fix — see
//! `reclaim_expired_lock_and_wake_target_stmt`'s doc comment for why the
//! per-key advisory-lock ordering cannot be batched away.
//!
//! After that lock, the pre-fix loop issued three more round trips per
//! key. It paid all three even on the common "no waiter at all" key: the
//! fenced reclaim `DELETE`, a waiter-row cleanup `DELETE`, and a
//! head-of-line `SELECT`.
//!
//! The waiter-row cleanup deletes the reclaimed holder's own stale waiter
//! row, if one exists. The `SELECT` finds the key's new head of line.
//!
//! The fix combines those three into one statement via CTEs. It uses an
//! explicit anti-join against the waiter-delete CTE's `RETURNING` output,
//! instead of a sibling `SELECT` against the base table. A data-modifying
//! CTE's effect is visible only to a statement that names it. It is never
//! visible to a plain scan of the same base table in the same top-level
//! statement. Three separate statements in one transaction behave
//! differently: there, each later statement's own snapshot *does* see the
//! previous statement's writes.
//!
//! Evidence is `pg_stat_statements` call/buffer counts, not wall-clock.
//! Wall-clock is not admissible on a shared-vCPU machine. This mirrors
//! `scheduler_overdue_pass_perf.rs`'s harness for the sibling investigation
//! into a periodic background pass, not a per-HTTP-request path: same
//! tool, same fixture-then-snapshot structure.

#![allow(clippy::too_many_lines)]

use autumn_harvest::mutex::reclaim_expired_leases_and_wake;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

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
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

// ── Fixture generation ──────────────────────────────────────────────────────

/// Deterministic per-index UUIDs, computable identically from SQL (fixture
/// seeding) and Rust (post-run assertions) without a round trip to learn
/// what got inserted. `tag` separates the three UUID families (holder,
/// external waiter, self-waiter) so no two ever collide for the same `i`.
fn deterministic_uuid(tag: u8, i: i64) -> Uuid {
    Uuid::parse_str(&format!(
        "00000000-0000-0000-{tag:04}-{i:012}",
        tag = u32::from(tag),
        i = i
    ))
    .expect("well-formed deterministic UUID")
}

fn holder_uuid(i: i64) -> Uuid {
    deterministic_uuid(1, i)
}
fn external_waiter_uuid(i: i64) -> Uuid {
    deterministic_uuid(2, i)
}

/// `i % NOT_EXPIRED_EVERY == 0` keys keep a live (non-expired) lease —
/// control rows the sweep must leave completely untouched.
const NOT_EXPIRED_EVERY: i64 = 20;
/// `i % PAUSED_EVERY == 0` (and expired) keys are held by a `PAUSED`
/// execution — expired but not reclaimable, exercising the anti-join skip.
const PAUSED_EVERY: i64 = 13;
/// `i % WAITER_EVERY == 0` (and reclaimable) keys carry a genuine external
/// waiter that must become the new head of line.
const WAITER_EVERY: i64 = 5;
/// `i % SELF_WAITER_EVERY == 0` is a subset of `WAITER_EVERY` (15 is a
/// multiple of 5). It additionally seeds the reclaimed holder's own stale
/// waiter row, inserted *first* for a smaller `id`. Consider a naive
/// rewrite of the combined statement that just re-`SELECT`s the base
/// table. It would incorrectly return the holder itself as head of line,
/// instead of the external waiter.
const SELF_WAITER_EVERY: i64 = 15;

/// Seeds `n` mutex-lock rows plus their holder executions, waiters, and
/// parked `harvest_task_queue` rows for every waiter. Pure set-based SQL,
/// not a per-row Rust loop, mirroring
/// `scheduler_overdue_pass_perf.rs`'s `seed_fixture` convention. See the
/// `*_EVERY` constants above for the exact population shape.
async fn seed_fixture(conn: &mut AsyncPgConnection, n: i64) {
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
             started_at, created_at
         )
         SELECT
             ('00000000-0000-0000-0001-' || lpad(gs::text, 12, '0'))::uuid,
             'mutex_reclaim_perf_holder',
             'mutex_reclaim_perf_holder_' || gs,
             gen_random_uuid(),
             0,
             CASE WHEN gs % {PAUSED_EVERY} = 0 THEN 'PAUSED' ELSE 'RUNNING' END,
             '{{}}'::jsonb,
             'default',
             NOW(),
             NOW()
         FROM generate_series(1, {n}) AS gs;

         -- The external waiters also need a `harvest_workflow_executions`
         -- row: `harvest_task_queue.workflow_exec_id` is foreign-keyed to it.
         INSERT INTO harvest_workflow_executions (
             id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name,
             started_at, created_at
         )
         SELECT
             ('00000000-0000-0000-0002-' || lpad(gs::text, 12, '0'))::uuid,
             'mutex_reclaim_perf_waiter',
             'mutex_reclaim_perf_waiter_' || gs,
             gen_random_uuid(),
             0,
             'RUNNING',
             '{{}}'::jsonb,
             'default',
             NOW(),
             NOW()
         FROM generate_series(1, {n}) AS gs
         WHERE gs % {WAITER_EVERY} = 0;

         INSERT INTO harvest_mutex_locks (
             lock_key, holder_exec_id, lock_seq, acquired_at, lease_expires_at
         )
         SELECT
             'mutex_reclaim_perf_key_' || gs,
             ('00000000-0000-0000-0001-' || lpad(gs::text, 12, '0'))::uuid,
             gs,
             NOW() - interval '2 minutes',
             CASE WHEN gs % {NOT_EXPIRED_EVERY} = 0
                  THEN NOW() + interval '1 hour'
                  ELSE NOW() - interval '1 second'
             END
         FROM generate_series(1, {n}) AS gs;

         -- Self-waiter rows FIRST (smaller `id`) for the subset that seeds one,
         -- so a correct combined statement must skip past it to reach the
         -- external waiter below.
         INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at)
         SELECT
             'mutex_reclaim_perf_key_' || gs,
             ('00000000-0000-0000-0001-' || lpad(gs::text, 12, '0'))::uuid,
             NOW() - interval '30 seconds'
         FROM generate_series(1, {n}) AS gs
         WHERE gs % {SELF_WAITER_EVERY} = 0
           AND gs % {NOT_EXPIRED_EVERY} != 0
           AND gs % {PAUSED_EVERY} != 0;

         INSERT INTO harvest_mutex_waiters (lock_key, waiter_exec_id, requested_at)
         SELECT
             'mutex_reclaim_perf_key_' || gs,
             ('00000000-0000-0000-0002-' || lpad(gs::text, 12, '0'))::uuid,
             NOW()
         FROM generate_series(1, {n}) AS gs
         WHERE gs % {WAITER_EVERY} = 0;

         -- One parked `harvest_task_queue` row per holder AND per external
         -- waiter, so a wake is a directly observable state flip
         -- (RUNNING/parked -> PENDING), not an inference.
         INSERT INTO harvest_task_queue (
             queue_name, task_type, workflow_exec_id, input, state, priority,
             max_attempts, scheduled_at, worker_id, started_at
         )
         SELECT 'default', 'workflow',
                ('00000000-0000-0000-0001-' || lpad(gs::text, 12, '0'))::uuid,
                '{{}}'::jsonb, 'RUNNING', 0, 3, NOW(), NULL, NULL
         FROM generate_series(1, {n}) AS gs;

         INSERT INTO harvest_task_queue (
             queue_name, task_type, workflow_exec_id, input, state, priority,
             max_attempts, scheduled_at, worker_id, started_at
         )
         SELECT 'default', 'workflow',
                ('00000000-0000-0000-0002-' || lpad(gs::text, 12, '0'))::uuid,
                '{{}}'::jsonb, 'RUNNING', 0, 3, NOW(), NULL, NULL
         FROM generate_series(1, {n}) AS gs
         WHERE gs % {WAITER_EVERY} = 0;

         ANALYZE harvest_workflow_executions;
         ANALYZE harvest_mutex_locks;
         ANALYZE harvest_mutex_waiters;
         ANALYZE harvest_task_queue;"
    ))
    .await
    .expect("seed mutex lease-reclaim perf fixture");
}

fn expected_reclaimed_count(n: i64) -> i64 {
    (1..=n)
        .filter(|gs| gs % NOT_EXPIRED_EVERY != 0 && gs % PAUSED_EVERY != 0)
        .count() as i64
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

/// The per-key reclaim/waiter-cleanup/head-of-line bookkeeping this
/// investigation targets: everything touching `harvest_mutex_locks` or
/// `harvest_mutex_waiters`, except two statements. The first exception is
/// the one-time, un-parameterized candidate-select (`expired_leases_stmt`).
/// It is the only one of the group with no per-key `$1`, and the only one
/// carrying `order by l.lock_key`. The second exception is the advisory
/// lock: unchanged by this fix either way, and not itself a bookkeeping
/// query against either table.
fn is_reclaim_loop_statement(row: &StatRow) -> bool {
    let q = row.query.to_ascii_lowercase();
    (q.contains("harvest_mutex_locks") || q.contains("harvest_mutex_waiters"))
        && !q.contains("pg_advisory_xact_lock")
        && !q.contains("order by l.lock_key")
}

fn is_advisory_lock_statement(row: &StatRow) -> bool {
    row.query
        .to_ascii_lowercase()
        .contains("pg_advisory_xact_lock")
}

fn is_wake_statement(row: &StatRow) -> bool {
    row.query.to_ascii_lowercase().contains("harvest_task_queue")
}

struct SizePoint {
    n: i64,
    reclaim_loop_calls: i64,
    reclaim_loop_buffers: i64,
    advisory_calls: i64,
    wake_calls: i64,
}

async fn measure_one_pass(admin: &str, label: &str, n: i64) -> SizePoint {
    let db_name = unique(&format!("mutex_reclaim_perf_{label}_{n}"));
    let url = create_fresh_db(admin, &db_name).await;

    let mut seed_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("seed connection");
    ensure_pg_stat_statements(&mut seed_conn).await;
    seed_fixture(&mut seed_conn, n).await;

    // The one real public entry point: the exact function every worker's
    // periodic timeout tick calls, inside its own transaction
    // (`timeout::enforce_timeouts_once`'s call site).
    let mut pass_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("pass connection");

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    let reclaimed = pass_conn
        .transaction::<usize, autumn_harvest::error::HarvestError, _>(async |conn| {
            reclaim_expired_leases_and_wake(conn).await
        })
        .await
        .expect("reclaim_expired_leases_and_wake should succeed");
    assert_eq!(
        i64::try_from(reclaimed).unwrap(),
        expected_reclaimed_count(n),
        "every expired, non-PAUSED-holder key must be reclaimed -- exactly once"
    );

    let all_rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let reclaim_rows: Vec<&StatRow> = all_rows
        .iter()
        .filter(|r| is_reclaim_loop_statement(r))
        .collect();
    assert!(
        !reclaim_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the reclaim-loop shapes -- check \
         pg_stat_statements.track and shared_preload_libraries",
    );
    let reclaim_loop_calls: i64 = reclaim_rows.iter().map(|r| r.calls).sum();
    let reclaim_loop_buffers: i64 = reclaim_rows.iter().map(|r| r.total_buffers).sum();
    let advisory_calls: i64 = all_rows
        .iter()
        .filter(|r| is_advisory_lock_statement(r))
        .map(|r| r.calls)
        .sum();
    let wake_calls: i64 = all_rows
        .iter()
        .filter(|r| is_wake_statement(r))
        .map(|r| r.calls)
        .sum();

    SizePoint {
        n,
        reclaim_loop_calls,
        reclaim_loop_buffers,
        advisory_calls,
        wake_calls,
    }
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

/// Sweeps three fixture sizes so the artifact demonstrates the call-count
/// shape directly, not just one point on the curve. Run once against the
/// pre-fix code (`PERF_LABEL=before`), once against the post-fix code
/// (`PERF_LABEL=after`) -- see
/// `autumn-harvest/scripts/mutex_lease_reclaim_perf_repro.sh`.
#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-mutex-lease-reclaim.md"]
async fn zz_capture_mutex_lease_reclaim_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("mutex-lease-reclaim");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let mut lines = vec![format!(
        "-- {label}: reclaim_expired_leases_and_wake, pg_stat_statements sweep --\n\
         n\treclaim_loop_calls\treclaim_loop_buffers\tadvisory_calls\twake_calls"
    )];
    for n in [200_i64, 1_000, 2_000] {
        let point = measure_one_pass(&admin, &label, n).await;
        eprintln!(
            "label={label} n={} reclaim_loop_calls={} reclaim_loop_buffers={} \
             advisory_calls={} wake_calls={}",
            point.n,
            point.reclaim_loop_calls,
            point.reclaim_loop_buffers,
            point.advisory_calls,
            point.wake_calls
        );
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}",
            point.n,
            point.reclaim_loop_calls,
            point.reclaim_loop_buffers,
            point.advisory_calls,
            point.wake_calls
        ));
    }
    std::fs::write(
        out_dir.join(format!("{label}-sweep.txt")),
        lines.join("\n") + "\n",
    )
    .expect("write sweep artifact");
    eprintln!("evidence capture complete: label={label}");
}

// ── Correctness: exact reclaim/wake targets, not just a call count ─────────

#[derive(diesel::QueryableByName, Debug)]
struct QueueStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
}

async fn task_queue_state(conn: &mut AsyncPgConnection, exec_id: Uuid) -> Option<String> {
    diesel::sql_query(
        "SELECT state FROM harvest_task_queue WHERE workflow_exec_id = $1 AND task_type = 'workflow'",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .get_result::<QueueStateRow>(conn)
    .await
    .ok()
    .map(|r| r.state)
}

async fn lock_exists(conn: &mut AsyncPgConnection, key: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    diesel::sql_query(
        "SELECT EXISTS (SELECT 1 FROM harvest_mutex_locks WHERE lock_key = $1) AS present",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result::<Present>(conn)
    .await
    .expect("lock existence query")
    .present
}

/// Proves the combined statement's exact targets against real seeded
/// state. A non-expired lock is left alone. A `PAUSED`-holder's expired
/// lock is left alone too, and its waiter, if any, stays parked. A plain
/// reclaim with no waiter wakes nobody.
///
/// The last case is the one that would catch a broken anti-join. A
/// reclaimed key can carry **both** the holder's own stale waiter row and
/// a genuine external waiter. That key must wake the **external** waiter,
/// never the holder — even though the holder's row was inserted with the
/// smaller `id`.
#[tokio::test]
async fn reclaim_wakes_the_correct_head_of_line_and_leaves_everything_else_alone() {
    const N: i64 = 90;

    let (admin, _guard) = setup_server().await;
    let url = create_fresh_db(&admin, &unique("mutex_reclaim_equiv")).await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    seed_fixture(&mut conn, N).await;

    let reclaimed = reclaim_expired_leases_and_wake(&mut conn)
        .await
        .expect("reclaim should succeed");
    assert_eq!(reclaimed as i64, expected_reclaimed_count(N));

    let mut checked_not_expired = 0;
    let mut checked_paused = 0;
    let mut checked_plain_reclaim = 0;
    let mut checked_self_and_external = 0;

    for gs in 1..=N {
        let key = format!("mutex_reclaim_perf_key_{gs}");
        let holder = holder_uuid(gs);
        let not_expired = gs % NOT_EXPIRED_EVERY == 0;
        let paused = gs % PAUSED_EVERY == 0;
        let has_waiter = gs % WAITER_EVERY == 0;
        let has_self_waiter = gs % SELF_WAITER_EVERY == 0 && !not_expired && !paused;

        if not_expired {
            assert!(
                lock_exists(&mut conn, &key).await,
                "key {key}: a live lease must never be reclaimed"
            );
            checked_not_expired += 1;
            continue;
        }

        if paused {
            assert!(
                lock_exists(&mut conn, &key).await,
                "key {key}: an expired lease held by a PAUSED execution must not be reclaimed"
            );
            if has_waiter {
                assert_eq!(
                    task_queue_state(&mut conn, external_waiter_uuid(gs)).await.as_deref(),
                    Some("RUNNING"),
                    "key {key}: nothing was reclaimed, so its waiter must stay parked"
                );
            }
            checked_paused += 1;
            continue;
        }

        // Reclaimable: the lock row is gone either way.
        assert!(
            !lock_exists(&mut conn, &key).await,
            "key {key}: an expired lease on a non-PAUSED holder must be reclaimed"
        );

        if !has_waiter {
            checked_plain_reclaim += 1;
            continue;
        }

        // The external waiter is the one true head of line and must be woken.
        assert_eq!(
            task_queue_state(&mut conn, external_waiter_uuid(gs)).await.as_deref(),
            Some("PENDING"),
            "key {key}: the external waiter must be woken (repended to PENDING)"
        );
        // The holder's own parked row must NOT be touched by this key's
        // wake. That holds whether or not this key also seeded a
        // self-waiter row.
        assert_eq!(
            task_queue_state(&mut conn, holder).await.as_deref(),
            Some("RUNNING"),
            "key {key}: the reclaimed holder's own parked task must be untouched by this wake \
             -- a broken anti-join would incorrectly wake the holder instead of the external \
             waiter here, since the holder's stale self-waiter row has the smaller id"
        );
        if has_self_waiter {
            checked_self_and_external += 1;
        } else {
            checked_plain_reclaim += 1;
        }
    }

    assert!(checked_not_expired >= 3, "fixture must seed non-expired control keys");
    assert!(checked_paused >= 3, "fixture must seed PAUSED-holder keys");
    assert!(checked_plain_reclaim >= 3, "fixture must seed plain reclaims");
    assert!(
        checked_self_and_external >= 3,
        "fixture must seed keys with both a self-waiter and an external waiter -- \
         this is the case that actually exercises the anti-join"
    );
}
