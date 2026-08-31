#![cfg(feature = "db")]
#![allow(dead_code)]
//! Shared harness for the issue #958 retention-reclamation measurement.
//!
//! Lives under `tests/` so the manifest-driven CI runner
//! (`.github/ci/integration-suites.txt`) can execute the gate that uses it; the
//! benchmark (`benches/retention_reclaim_bench.rs`) reaches across to the same
//! code rather than duplicating it, so the numbers published in
//! `docs/perf-artifacts/` and the number CI gates on can never be produced by
//! two different implementations.
//!
//! # What it measures
//!
//! Issue #958's Success Metric names four quantities. This harness produces all
//! four, for **both layouts**, from one seeded corpus:
//!
//! 1. Wall time of a retention pass reclaiming ≥ 50% of executions.
//! 2. Dead-tuple ratio left on `harvest_events` afterwards.
//! 3. Concurrent event-append p99 during the pass, against a quiet baseline.
//! 4. Concurrent task-claim p99 during the pass, against a quiet baseline.
//!
//! # Honest scoping
//!
//! The pass is timed **end to end**, which means it includes the per-execution
//! candidate loop (archive hook, legal-hold re-check, summary demotion,
//! auxiliary-row cleanup, execution-row delete). That loop is *unchanged* by
//! issue #958 and is required by the `HistoryArchiver` contract — it must see
//! each execution individually, before its rows become unreachable. So the
//! report separates the two costs rather than blending them: `events_secs` is
//! the reclamation this issue changes, `executions_secs` is the pre-existing
//! per-execution collection. Reporting only the total would let a genuine O(1)
//! event reclamation hide behind an O(n) loop nobody asked us to touch — or,
//! worse, let that loop's cost be read as evidence that partitioning did not
//! help.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use diesel::sql_types::{BigInt, Double};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};

// ── Scale ──────────────────────────────────────────────────────────────────

/// Corpus size for one measured scenario.
#[derive(Debug, Clone, Copy)]
pub struct Scale {
    /// Total executions seeded.
    pub executions: usize,
    /// Events per execution.
    pub events_per_execution: usize,
    /// Distinct daily cohorts the corpus spans.
    pub cohorts: usize,
    /// Fraction of executions that are expired (and so collected by the pass).
    pub expired_fraction: f64,
}

impl Scale {
    /// The issue's headline scale: 10M events / 1M executions on one shard.
    ///
    /// Needs a real server; `HARVEST_BENCH_SCALE=full` selects it.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            executions: 1_000_000,
            events_per_execution: 10,
            cohorts: 20,
            expired_fraction: 0.5,
        }
    }

    /// A laptop/CI-sized corpus with the same *shape* — same events per
    /// execution, same cohort count, same expired fraction — so the per-unit
    /// costs it reports extrapolate honestly.
    #[must_use]
    pub const fn small() -> Self {
        Self {
            executions: 20_000,
            events_per_execution: 10,
            cohorts: 20,
            expired_fraction: 0.5,
        }
    }

    /// Resolve from `HARVEST_BENCH_SCALE` (`full` | `small`, default `small`).
    #[must_use]
    pub fn from_env() -> Self {
        match std::env::var("HARVEST_BENCH_SCALE").as_deref() {
            Ok("full") => Self::full(),
            _ => Self::small(),
        }
    }

    /// Total event rows seeded.
    #[must_use]
    pub const fn events(&self) -> usize {
        self.executions * self.events_per_execution
    }

    /// Executions the pass is expected to collect.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    pub fn expired(&self) -> usize {
        (self.executions as f64 * self.expired_fraction) as usize
    }
}

// ── Measurement ────────────────────────────────────────────────────────────

/// One layout's measured result.
#[derive(Debug, Clone)]
pub struct Measurement {
    /// `"unpartitioned"` or `"partitioned"`.
    pub layout: &'static str,
    /// Executions actually collected.
    pub executions_collected: usize,
    /// Event rows that became unreachable.
    pub events_reclaimed: i64,
    /// Wall time of the whole retention pass.
    pub pass: Duration,
    /// The part of the pass that reclaimed EVENT storage — a partition drop on
    /// the partitioned layout, the cascade on the unpartitioned one. This is
    /// the quantity issue #958 changes.
    pub events_reclaim: Duration,
    /// Row-level deletes Postgres recorded against `harvest_events` during the
    /// pass. The partitioned layout's headline claim is that this is **zero**.
    pub event_rows_deleted: i64,
    /// Dead-tuple ratio on `harvest_events` after the pass, before autovacuum.
    pub dead_tuple_ratio: f64,
    /// Append p99 (ms) while the pass ran.
    pub append_p99_ms: f64,
    /// Append p99 (ms) with no pass running.
    pub append_p99_baseline_ms: f64,
    /// Claim p99 (ms) while the pass ran.
    pub claim_p99_ms: f64,
    /// Claim p99 (ms) with no pass running.
    pub claim_p99_baseline_ms: f64,
}

impl Measurement {
    /// Percentage regression of concurrent append p99 during the pass.
    #[must_use]
    pub fn append_regression_pct(&self) -> f64 {
        pct_change(self.append_p99_baseline_ms, self.append_p99_ms)
    }

    /// Percentage regression of concurrent claim p99 during the pass.
    #[must_use]
    pub fn claim_regression_pct(&self) -> f64 {
        pct_change(self.claim_p99_baseline_ms, self.claim_p99_ms)
    }
}

fn pct_change(baseline: f64, measured: f64) -> f64 {
    if baseline <= 0.0 {
        return 0.0;
    }
    (measured - baseline) / baseline * 100.0
}

/// p99 of a sample set, in milliseconds.
///
/// Nearest-rank on a sorted sample: with the sample counts this harness
/// collects, an interpolated percentile would imply a precision the
/// measurement does not have.
#[must_use]
pub fn p99_ms(samples: &mut [Duration]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    // Integer arithmetic rather than a float multiply: sample counts here are
    // in the thousands, and `ceil(len * 0.99)` on a float is one rounding
    // surprise away from picking the wrong rank. `(len * 99).div_ceil(100)` is
    // the same nearest-rank definition with no float in it at all.
    let rank = (samples.len().saturating_mul(99)).div_ceil(100);
    let idx = rank.saturating_sub(1).min(samples.len() - 1);
    samples[idx].as_secs_f64() * 1000.0
}

// ── Database helpers ───────────────────────────────────────────────────────

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(diesel::QueryableByName)]
struct RatioRow {
    #[diesel(sql_type = Double)]
    v: f64,
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
}

async fn scalar(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
    diesel::sql_query(sql)
        .get_result::<CountRow>(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .n
}

/// Seed the corpus with bulk `INSERT … SELECT generate_series(…)` rather than
/// row-by-row appends.
///
/// Seeding is setup, not measurement, so it is allowed to be fast in ways the
/// engine's own append path is not: one statement per table, and the
/// partitioned layout's integrity trigger disabled for the duration (it would
/// otherwise do a primary-key probe per seeded row, turning a ten-second setup
/// into a ten-minute one without changing a single measured number).
///
/// Executions are spread evenly across `cohorts` daily buckets, oldest first,
/// and the oldest `expired_fraction` of them are given a `completed_at` well
/// past the retention horizon. Because the corpus is laid out oldest-first,
/// the expired set occupies whole cohorts — which is the steady state issue
/// #958 describes, and the only state in which a partition is droppable at all.
pub async fn seed(conn: &mut AsyncPgConnection, scale: Scale, partitioned: bool) {
    let cohorts = i64::try_from(scale.cohorts).unwrap_or(1).max(1);
    let executions = i64::try_from(scale.executions).unwrap_or(0);
    let per_exec = i64::try_from(scale.events_per_execution)
        .unwrap_or(1)
        .max(1);
    let expired = i64::try_from(scale.expired()).unwrap_or(0);

    if partitioned {
        // Materialize every cohort the corpus will occupy. In production the
        // engine's lookahead maintenance did this when the rows were written.
        for bucket in 0..cohorts {
            let at = Utc::now() - chrono::Duration::days(cohorts - bucket);
            autumn_harvest::partition::ensure_cohort(conn, at)
                .await
                .expect("materialize cohort");
        }
        conn.batch_execute("ALTER TABLE harvest_events DISABLE TRIGGER harvest_events_exec_fk_trg")
            .await
            .expect("disable integrity trigger for bulk seed");
    }

    // `created_at` places an execution in a cohort; `completed_at` decides
    // whether retention collects it.
    //
    // The corpus is laid out strictly OLDEST-FIRST — cohort index rises
    // monotonically with `g` — so the expired prefix occupies whole cohorts.
    // That is load-bearing, not cosmetic: with `g % cohorts` (interleaving
    // expired and retained executions through every cohort) not one partition
    // would ever be fully free, so the partitioned arm would measure a sweep
    // that correctly drops nothing and the benchmark would report the wrong
    // thing while looking healthy. Whole-cohort expiry IS the steady state
    // issue #958 describes, and the only state in which a partition is
    // droppable at all.
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions
             (workflow_name, workflow_id, shard_id, state, input, created_at, started_at, completed_at)
         SELECT 'bench_wf',
                'bench-' || g,
                0,
                'COMPLETED',
                '{{}}'::jsonb,
                NOW() - make_interval(days => ({cohorts} - ((g - 1) * {cohorts} / {executions}))::int),
                NOW() - make_interval(days => ({cohorts} - ((g - 1) * {cohorts} / {executions}))::int),
                CASE WHEN g <= {expired}
                     THEN NOW() - make_interval(days => ({cohorts} - ((g - 1) * {cohorts} / {executions}))::int)
                     ELSE NOW()
                END
           FROM generate_series(1, {executions}) AS g"
    ))
    .await
    .expect("seed executions");

    let cohort_expr = if partitioned {
        "harvest_event_cohort(e.created_at)"
    } else {
        "'-infinity'::timestamptz"
    };
    conn.batch_execute(&format!(
        "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp, cohort)
         SELECT e.id,
                (s - 1)::int,
                'MarkerRecorded',
                '{{\"type\":\"MarkerRecorded\",\"data\":{{\"name\":\"bench\",\"details\":{{}}}}}}'::jsonb,
                e.created_at,
                {cohort_expr}
           FROM harvest_workflow_executions e
           CROSS JOIN generate_series(1, {per_exec}) AS s"
    ))
    .await
    .expect("seed events");

    if partitioned {
        conn.batch_execute("ALTER TABLE harvest_events ENABLE TRIGGER harvest_events_exec_fk_trg")
            .await
            .expect("re-enable integrity trigger");
    }

    conn.batch_execute("ANALYZE harvest_workflow_executions; ANALYZE harvest_events")
        .await
        .expect("analyze");
}

/// Dead-tuple ratio across `harvest_events` and all of its partitions.
///
/// `n_dead_tup / (n_live_tup + n_dead_tup)` from `pg_stat_all_tables` — the
/// same quantity the issue's Success Metric names ("post-pass table + index
/// bloat (dead-tuple ratio) is < 5%"). Read before autovacuum has had a chance
/// to run, which is the point: the DELETE path's cost is the window in which
/// that debt exists.
pub async fn dead_tuple_ratio(conn: &mut AsyncPgConnection) -> f64 {
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .ok();
    diesel::sql_query(
        "SELECT CASE WHEN COALESCE(SUM(n_live_tup + n_dead_tup), 0) = 0 THEN 0.0
                     ELSE SUM(n_dead_tup)::float8
                          / SUM(n_live_tup + n_dead_tup)::float8 END AS v
           FROM pg_stat_all_tables
          WHERE relname LIKE 'harvest_events%'",
    )
    .get_result::<RatioRow>(conn)
    .await
    .map(|r| r.v)
    .unwrap_or(0.0)
}

/// Row-level deletes Postgres recorded against `harvest_events` and every
/// partition of it.
pub async fn event_rows_deleted(conn: &mut AsyncPgConnection) -> i64 {
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .ok();
    scalar(
        conn,
        "SELECT COALESCE(SUM(n_tup_del), 0)::bigint AS n FROM pg_stat_all_tables
          WHERE relname LIKE 'harvest_events%'",
    )
    .await
}

/// Count of surviving event rows.
pub async fn event_count(conn: &mut AsyncPgConnection) -> i64 {
    scalar(conn, "SELECT COUNT(*)::bigint AS n FROM harvest_events").await
}

/// Count of surviving event rows belonging to the SEEDED corpus.
///
/// Excludes the concurrent load's own execution: the load appends throughout
/// the measured window, and netting its appends against the reclamation would
/// understate what the pass freed (and, at small scale, report it as negative).
pub async fn seeded_event_count(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT COUNT(*)::bigint AS n FROM harvest_events e
          WHERE EXISTS (SELECT 1 FROM harvest_workflow_executions x
                         WHERE x.id = e.workflow_exec_id AND x.workflow_name = 'bench_wf')
             OR NOT EXISTS (SELECT 1 FROM harvest_workflow_executions x
                             WHERE x.id = e.workflow_exec_id)",
    )
    .await
}

/// Count of surviving executions.
pub async fn execution_count(conn: &mut AsyncPgConnection) -> i64 {
    scalar(
        conn,
        "SELECT COUNT(*)::bigint AS n FROM harvest_workflow_executions",
    )
    .await
}

// ── Concurrent load ────────────────────────────────────────────────────────

/// Latency samples from one concurrent-load window.
#[derive(Debug, Default)]
pub struct LoadSamples {
    /// Per-append latencies.
    pub appends: Vec<Duration>,
    /// Per-claim latencies.
    pub claims: Vec<Duration>,
}

/// Drive appends and claims against a live execution until `stop` is set,
/// recording per-operation latency.
///
/// This is the load whose p99 the Success Metric budgets. It deliberately uses
/// the engine's own `store::append_events` and `queue::claim_task_on_shard`
/// rather than hand-written SQL, so what is measured is the hot path operators
/// actually run — including, on the partitioned layout, tuple routing, the
/// `DEFAULT` cohort expression and the integrity trigger.
pub async fn drive_load(url: &str, stop: Arc<AtomicBool>, ops: Arc<AtomicU64>) -> LoadSamples {
    let mut conn = AsyncPgConnection::establish(url)
        .await
        .expect("load connection");
    let mut samples = LoadSamples::default();

    // A dedicated live execution, created now, so its appends land in the
    // currently-open cohort and are never a retention candidate — the load must
    // measure contention with the pass, not participate in it.
    let exec: uuid::Uuid = diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
             (workflow_name, workflow_id, shard_id, state, input)
         VALUES ('load_wf', 'load-' || gen_random_uuid()::text, 0, 'RUNNING', '{}'::jsonb)
         RETURNING id",
    )
    .get_result::<IdRow>(&mut conn)
    .await
    .expect("load execution")
    .id;
    let exec_id = autumn_harvest::types::ExecutionId::from_uuid(exec);

    let queues = vec!["default".to_string()];
    let mut next_event_id = 0i32;
    while !AtomicBool::load(&stop, Ordering::Relaxed) {
        let event = vec![autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: "load".into(),
            details: serde_json::json!({"n": next_event_id}),
        }];
        let t0 = Instant::now();
        let appended =
            autumn_harvest::store::append_events(&mut conn, exec_id, &event, next_event_id).await;
        if appended.is_ok() {
            samples.appends.push(t0.elapsed());
            next_event_id += 1;
        }

        // An empty queue still exercises the whole claim predicate — the scan,
        // the ordering and every accreted gate — which is what competes with a
        // maintenance pass for locks and buffers.
        let t1 = Instant::now();
        let claimed = autumn_harvest::queue::claim_task_on_shard(
            &mut conn,
            &queues,
            "bench-worker",
            "bench-build",
            None,
            &[],
            &[],
            Some(autumn_harvest::types::ShardId::new(0)),
        )
        .await;
        if claimed.is_ok() {
            samples.claims.push(t1.elapsed());
        }
        ops.fetch_add(1, Ordering::Relaxed);
    }
    samples
}

/// Run the load for `window`, with nothing else happening, to establish the
/// quiet baseline the pass is compared against.
pub async fn measure_baseline(url: &str, window: Duration) -> (f64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let handle = tokio::spawn(drive_load(
        url.to_string().leak(),
        Arc::clone(&stop),
        Arc::clone(&ops),
    ));
    tokio::time::sleep(window).await;
    stop.store(true, Ordering::Relaxed);
    let mut samples = handle.await.expect("load task");
    (p99_ms(&mut samples.appends), p99_ms(&mut samples.claims))
}

// ── The measured pass ──────────────────────────────────────────────────────

/// Run one retention pass against `url` while the concurrent load runs, and
/// report every quantity the Success Metric names.
///
/// `partitioned` describes the layout already applied to `url`; the harness
/// does not convert it, so the caller decides (and so the same code measures
/// both arms of the comparison).
pub async fn measure_pass(
    url: &str,
    partitioned: bool,
    scale: Scale,
    baseline_window: Duration,
) -> Measurement {
    use autumn_harvest::retention::{RetentionConfig, RetentionRuntime};
    use autumn_harvest::shard::ShardedDbPool;

    let url_static: &'static str = url.to_string().leak();
    let mut conn = AsyncPgConnection::establish(url_static)
        .await
        .expect("measure connection");

    // The quiet baseline first: the same load, same duration, nothing else
    // running. Without it a p99 number is unanchored — a slow host would look
    // like a regression caused by the pass.
    let (append_p99_baseline_ms, claim_p99_baseline_ms) =
        measure_baseline(url_static, baseline_window).await;

    // Counted over the SEEDED corpus only. The concurrent load appends to its
    // own live execution throughout the pass, so a bare `COUNT(*)` before/after
    // would net the load's appends against the reclamation and could even
    // report a negative figure.
    let events_before = seeded_event_count(&mut conn).await;
    let executions_before = execution_count(&mut conn).await;

    // Reset AFTER seeding (and after flushing the seed's own pending stats),
    // so the delete counter and dead-tuple ratio below describe THE PASS and
    // not the bulk insert that set it up.
    conn.batch_execute("SELECT pg_stat_force_next_flush()")
        .await
        .ok();
    conn.batch_execute("SELECT pg_stat_reset()").await.ok();

    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let load = tokio::spawn(drive_load(url_static, Arc::clone(&stop), Arc::clone(&ops)));

    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url_static,
        );
    let pool = deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool");

    let mut config = RetentionConfig::with_max_age(Duration::from_secs(3_600));
    // One batch big enough to take the whole expired set, so the measurement is
    // of a single pass rather than of the batch loop's tick cadence.
    config.batch_size = scale.executions.max(1);
    // Phase 1 times the candidate loop ALONE. Partition maintenance is timed
    // separately below, because it is the only part of the pass issue #958
    // changes and blending the two would let an O(1) partition drop hide inside
    // a pre-existing O(n) per-execution loop — or, worse, let that loop's cost
    // read as evidence that partitioning did not help. On the unpartitioned
    // layout there is nothing to separate: the cascade fires inside the
    // candidate loop, so its event reclamation is part of `pass` by
    // construction, and `events_reclaim` stays zero.
    config.partitions.enabled = false;

    let started = Instant::now();
    let runtime = RetentionRuntime::spawn(
        ShardedDbPool::single(pool),
        config,
        Arc::new(NoopMetrics),
        None,
        None,
    )
    .expect("retention runtime");
    runtime.run_now();

    let mut collected = 0usize;
    for _ in 0..(60 * 60) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let snap = runtime.monitor().snapshot();
        if let Some(r) = snap.per_shard.iter().find(|r| r.shard == 0)
            && r.ran_at.is_some()
        {
            collected = r.deleted_count;
            break;
        }
    }
    let candidate_loop = started.elapsed();
    runtime.shutdown();

    // Phase 2: the event-storage reclamation on its own. On the partitioned
    // layout this is the partition drop — the O(number-of-partitions) operation
    // that replaces the delete storm. Run through the engine's own maintenance
    // entry point, not a hand-written DROP, so what is timed is what a
    // retention tick actually does.
    let events_reclaim = if partitioned {
        let t = Instant::now();
        autumn_harvest::partition::maintain(
            &mut conn,
            Utc::now(),
            autumn_harvest::partition::DEFAULT_LOOKAHEAD_COHORTS,
            &autumn_harvest::partition::SweepOptions::default(),
        )
        .await
        .expect("partition maintenance");
        t.elapsed()
    } else {
        Duration::ZERO
    };
    let pass = candidate_loop + events_reclaim;

    stop.store(true, Ordering::Relaxed);
    let mut samples = load.await.expect("load task");

    let events_after = seeded_event_count(&mut conn).await;
    let executions_after = execution_count(&mut conn).await;

    Measurement {
        layout: if partitioned {
            "partitioned"
        } else {
            "unpartitioned"
        },
        executions_collected: usize::try_from(executions_before - executions_after)
            .unwrap_or(collected),
        events_reclaimed: events_before - events_after,
        pass,
        events_reclaim,
        event_rows_deleted: event_rows_deleted(&mut conn).await,
        dead_tuple_ratio: dead_tuple_ratio(&mut conn).await,
        append_p99_ms: p99_ms(&mut samples.appends),
        append_p99_baseline_ms,
        claim_p99_ms: p99_ms(&mut samples.claims),
        claim_p99_baseline_ms,
    }
}

#[derive(Default)]
struct NoopMetrics;
impl autumn_harvest::telemetry::MetricsRecorder for NoopMetrics {}

// ── Reporting ──────────────────────────────────────────────────────────────

/// Render the two arms as the Markdown table published under
/// `docs/perf-artifacts/`.
#[must_use]
pub fn report(scale: Scale, arms: &[Measurement]) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Scale: {} executions x {} events = {} event rows, across {} daily cohorts; \
         {:.0}% expired.\n",
        scale.executions,
        scale.events_per_execution,
        scale.events(),
        scale.cohorts,
        scale.expired_fraction * 100.0
    );
    out.push_str(
        "| layout | pass total | event reclamation | events reclaimed | event-row DELETEs \
         | dead-tuple ratio after | append p99 (quiet -> during) | claim p99 (quiet -> during) |\n\
         |---|---:|---:|---:|---:|---:|---|---|\n",
    );
    for m in arms {
        let reclaim = if m.events_reclaim.is_zero() {
            "(inside the pass)".to_string()
        } else {
            format!("{:.3}s", m.events_reclaim.as_secs_f64())
        };
        let _ = writeln!(
            out,
            "| {} | {:.2}s | {reclaim} | {} | {} | {:.2}% | {:.2} -> {:.2} ms ({:+.1}%) \
             | {:.2} -> {:.2} ms ({:+.1}%) |",
            m.layout,
            m.pass.as_secs_f64(),
            m.events_reclaimed,
            m.event_rows_deleted,
            m.dead_tuple_ratio * 100.0,
            m.append_p99_baseline_ms,
            m.append_p99_ms,
            m.append_regression_pct(),
            m.claim_p99_baseline_ms,
            m.claim_p99_ms,
            m.claim_regression_pct(),
        );
    }
    out.push_str(
        "\n`pass total` is the whole retention pass. `event reclamation` splits out the part \
         issue #958 changes: on the partitioned layout it is the partition drop; on the \
         unpartitioned layout the cascade fires inside the candidate loop and cannot be \
         separated from it, so it is reported as part of the pass. The remainder of the pass \
         is the per-execution candidate loop (archive hook, hold re-check, summary demotion, \
         auxiliary cleanup, execution-row delete), which issue #958 does not change and which \
         the `HistoryArchiver` contract requires to visit each execution individually.\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::{Duration, Measurement, Scale, p99_ms, pct_change};

    #[test]
    fn p99_is_nearest_rank_and_handles_the_empty_sample() {
        let mut empty: Vec<Duration> = Vec::new();
        assert!((p99_ms(&mut empty) - 0.0).abs() < f64::EPSILON);

        let mut one = vec![Duration::from_millis(7)];
        assert!((p99_ms(&mut one) - 7.0).abs() < 1e-9);

        // 100 samples, 1..=100 ms: nearest-rank p99 is the 99th.
        let mut many: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert!((p99_ms(&mut many) - 99.0).abs() < 1e-9);
    }

    #[test]
    fn a_regression_percentage_is_relative_to_the_quiet_baseline() {
        let m = Measurement {
            layout: "partitioned",
            executions_collected: 0,
            events_reclaimed: 0,
            pass: Duration::ZERO,
            events_reclaim: Duration::ZERO,
            event_rows_deleted: 0,
            dead_tuple_ratio: 0.0,
            append_p99_ms: 10.5,
            append_p99_baseline_ms: 10.0,
            claim_p99_ms: 20.0,
            claim_p99_baseline_ms: 20.0,
        };
        assert!((m.append_regression_pct() - 5.0).abs() < 1e-9);
        assert!(m.claim_regression_pct().abs() < 1e-9);
    }

    #[test]
    fn a_zero_baseline_reports_no_regression_rather_than_infinity() {
        // A host so fast (or a window so short) that no sample was collected
        // must not produce a NaN/inf percentage in the published artifact.
        assert!((pct_change(0.0, 5.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn the_small_scale_keeps_the_full_scales_shape() {
        let (s, f) = (Scale::small(), Scale::full());
        assert_eq!(s.events_per_execution, f.events_per_execution);
        assert_eq!(s.cohorts, f.cohorts);
        assert!((s.expired_fraction - f.expired_fraction).abs() < f64::EPSILON);
        assert_eq!(f.events(), 10_000_000, "the issue's headline scale");
        assert_eq!(f.expired(), 500_000);
    }
}
