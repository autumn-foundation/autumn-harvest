//! Shared harness for the task-claim / enqueue throughput benchmark and its
//! CI-gated budget assertion (issue #786).
//!
//! Two consumers share this module so the benchmark and the gate can never
//! measure different things:
//!
//! * `benches/claim_bench.rs` — the exploratory report. Runs every scenario at
//!   every backlog depth and prints the per-gate cost table plus an
//!   `EXPLAIN (ANALYZE, BUFFERS)` of the headline claim.
//! * `tests/integration/claim_budget_tests.rs` — the CI gate. Runs the single
//!   headline scenario and fails the build if p99 exceeds the published budget.
//!
//! # Why a hand-rolled harness rather than criterion
//!
//! `queue::claim_task` is **destructive**: it transitions a row `PENDING ->
//! RUNNING`. Criterion's statistical machinery runs the measured closure
//! thousands of times, which would drain the seeded backlog and end up timing
//! "claim against an empty queue" — the opposite of the thing under test. This
//! harness instead seeds a backlog of `N`, performs a bounded number of claims
//! (never more than a fifth of `N`, see [`measured_claims_for`]), and reports
//! true percentiles over the collected per-claim latencies.
//!
//! # Measurement hygiene
//!
//! * Seeding is set-based (`INSERT ... SELECT FROM generate_series`) so a
//!   100k-row backlog costs one round trip, not 100k.
//! * `ANALYZE` runs after every seed. Without it the planner works from stale
//!   statistics on a freshly bulk-loaded table and picks plans that are neither
//!   representative nor stable.
//! * A tenth of each claimer's observations is discarded as warmup, applied
//!   **after** collection rather than by planned index (see
//!   [`warmup_claims_for`]) — so a scenario cut short by its wall-clock budget
//!   still reports the samples it took instead of discarding all of them and
//!   printing a confident-looking `0.00ms`.
//! * The connection pool is always sized at or above the claimer count, so the
//!   measurement never includes pool-checkout queueing.
//! * Every scenario truncates the queue first, so scenarios cannot contaminate
//!   each other.
//! * A scenario that hits its wall-clock budget sets [`db::ClaimReport::truncated`],
//!   and a scenario that collected no post-warmup samples reports
//!   [`LatencyStats::count`] `== 0`. Both consumers must render those as "n/a"
//!   rather than publishing a zero.

#![allow(dead_code)]

// ---------------------------------------------------------------------------
// Pure section — no database, no `db` feature. Unit-tested below and reachable
// from every build (including `--no-default-features`).
// ---------------------------------------------------------------------------

/// Which accreted claim-path gate a scenario exercises.
///
/// `claim_task`'s `WHERE` clause has grown roughly a predicate per phase since
/// 3.7. These variants let the benchmark attribute cost to each one instead of
/// reporting a single opaque number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimGate {
    /// Plain rows: no build id, no concurrency key, no rate limit, no pauses.
    Baseline,
    /// Rows carry `required_build_id`; the worker's build differs and matches
    /// only via a `harvest_build_compat` declaration, forcing the `EXISTS`
    /// branch of the build-routing filter (issue #171) rather than the cheap
    /// `required_build_id = $3` equality.
    BuildPolicy,
    /// Rows carry `concurrency_key` + `concurrency_cap`, exercising both the
    /// candidate-side `COUNT(*)` subquery and the `pg_try_advisory_xact_lock`
    /// re-check in the `claimed` CTE (issue #247).
    ConcurrencyKey,
    /// Rows carry `rate_limit_key` with a funded bucket, exercising the
    /// candidate-side `EXISTS` gate and the `rate_limit_debit` CTE
    /// (issues #332 / #699).
    RateLimited,
    /// The worker passes a populated circuit-breaker tracked-activity set, so
    /// the rate-limit gate and debit are skipped via `= ANY($5)` (issue #369).
    CircuitBreakerSet,
    /// Half the backlog belongs to `PAUSED` executions, so the scan walks past
    /// unclaimable rows via the `NOT EXISTS` anti-join (issue #383).
    PausedRows,
    /// Every gate above at once — the realistic worst case.
    AllGates,
}

impl ClaimGate {
    /// Stable identifier used in report tables and in `docs/performance.md`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::BuildPolicy => "build_policy",
            Self::ConcurrencyKey => "concurrency_key",
            Self::RateLimited => "rate_limited",
            Self::CircuitBreakerSet => "circuit_breaker_set",
            Self::PausedRows => "paused_rows",
            Self::AllGates => "all_gates",
        }
    }

    /// Every gate, in report order. `Baseline` is first so the table reads as
    /// "baseline, then what each predicate adds".
    #[must_use]
    pub const fn all() -> [Self; 7] {
        [
            Self::Baseline,
            Self::BuildPolicy,
            Self::ConcurrencyKey,
            Self::RateLimited,
            Self::CircuitBreakerSet,
            Self::PausedRows,
            Self::AllGates,
        ]
    }
}

/// One measured configuration of the claim hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scenario {
    /// Number of `PENDING` rows seeded before measurement begins.
    pub backlog: usize,
    /// Number of concurrent claimers (real contention, not a serial loop).
    pub claimers: usize,
    /// Number of distinct queues the backlog is spread across.
    pub queues: usize,
    /// Which accreted gate this scenario exercises.
    pub gate: ClaimGate,
}

/// The published headline scenario: the one number CI defends.
///
/// 10k pending tasks / 8 concurrent claimers / 4 queues. Kept as a function
/// rather than a `const` so the gate test and the benchmark can assert they are
/// measuring the same thing.
#[must_use]
pub const fn headline_scenario() -> Scenario {
    Scenario {
        backlog: 10_000,
        claimers: 8,
        queues: 4,
        gate: ClaimGate::Baseline,
    }
}

/// Backlog depths the exploratory benchmark sweeps.
pub const BACKLOG_SWEEP: [usize; 3] = [1_000, 10_000, 100_000];

/// Upper bound on how much of the backlog a measurement run may consume.
///
/// `claim_task` is destructive, so every measured claim shrinks the backlog.
/// Consuming at most a fifth keeps the queue between 80% and 100% of its seeded
/// depth for the whole run, which is what makes "p99 at a 10k backlog" an
/// honest statement rather than an average over a draining queue.
const MAX_DRAIN_FRACTION: usize = 5;

/// Absolute cap on measured claim operations, so a 100k-row sweep stays a
/// benchmark rather than a load test.
const MAX_MEASURED_CLAIMS: usize = 800;

/// Default wall-clock ceiling for a single scenario's measured phase.
///
/// A hard operation count alone is not a bound: claim latency scales with
/// backlog depth, so a deep sweep — or a regression that makes the query far
/// slower — would otherwise turn a benchmark into a multi-minute load test and
/// a CI gate into a timeout. Claimers stop early when this elapses and the
/// report carries however many samples were collected, with
/// [`ClaimReport::truncated`] set so a short sample is visible rather than
/// silently thin.
const DEFAULT_SCENARIO_BUDGET_SECS: u64 = 60;

/// Environment variable overriding [`DEFAULT_SCENARIO_BUDGET_SECS`].
pub const SCENARIO_BUDGET_ENV_VAR: &str = "HARVEST_BENCH_SCENARIO_SECS";

/// Effective wall-clock ceiling for one scenario's measured phase.
#[must_use]
pub fn scenario_time_budget() -> std::time::Duration {
    let secs = std::env::var(SCENARIO_BUDGET_ENV_VAR)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SCENARIO_BUDGET_SECS);
    std::time::Duration::from_secs(secs)
}

/// Fewest measured samples for a percentile to be worth reporting.
///
/// At 100 samples a nearest-rank p99 is the second-worst observation — coarse,
/// but a real one. Below this the harness is not measuring enough to defend a
/// number.
pub const MIN_MEANINGFUL_SAMPLES: usize = 100;

/// Total claim operations to perform against a backlog of `backlog` rows.
///
/// Guaranteed to be at most `backlog / 5` (see [`MAX_DRAIN_FRACTION`]) and at
/// least 1.
#[must_use]
pub const fn measured_claims_for(backlog: usize) -> usize {
    let by_fraction = backlog / MAX_DRAIN_FRACTION;
    let capped = if by_fraction > MAX_MEASURED_CLAIMS {
        MAX_MEASURED_CLAIMS
    } else {
        by_fraction
    };
    if capped == 0 { 1 } else { capped }
}

/// Fraction of a claimer's observations discarded as warmup (one in
/// `WARMUP_DIVISOR`).
///
/// The first claims on a fresh connection pay plan-cache and buffer-cache costs
/// an order of magnitude above steady state. Left in, they dominate the tail:
/// measured on a 4-core box at the headline scenario, dropping warmup from a
/// tenth to a flat 3 moved p99 from ~420ms to ~2900ms while p50 barely moved —
/// i.e. the "p99" became a cold-start metric rather than a claim-path one.
const WARMUP_DIVISOR: usize = 10;

/// Warmup to discard from a claimer that **actually collected** `collected`
/// observations.
///
/// Deliberately a function of what was collected, not of what was planned. The
/// measured phase stops on a wall-clock budget, so a planned-count warmup could
/// discard every sample a truncated run managed to take and then report a
/// confident-looking `0.00ms` for a run that measured nothing. Taking the
/// fraction after the fact keeps both properties: a full run sheds its cold
/// start, and a short run still reports the samples it has (`collected < 10`
/// discards nothing).
#[must_use]
pub const fn warmup_claims_for(collected: usize) -> usize {
    collected / WARMUP_DIVISOR
}

/// Nearest-rank percentile over a sample of millisecond latencies.
///
/// Nearest-rank (rather than an interpolating definition) is deliberate: it
/// always returns an actually-observed sample, so a reported p99 is a real
/// claim someone waited for, not an interpolation between two.
///
/// Returns `0.0` for an empty sample.
#[must_use]
pub fn percentile_ms(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    // Nearest-rank: ceil(p/100 * n), clamped to [1, n].
    //
    // The clamp is applied in float space *before* the cast, so the value cast
    // to `usize` is always in `[1.0, n]` and therefore exactly representable —
    // the truncation and sign-loss the lints warn about cannot occur here. A
    // NaN percentile clamps to 1.0 (`f64::clamp` returns NaN, so the explicit
    // `is_finite` guard handles it) rather than casting to an unspecified value.
    #[allow(clippy::cast_precision_loss)]
    let n_as_f64 = sorted.len() as f64;
    let scaled = (percentile / 100.0) * n_as_f64;
    let rank_f = if scaled.is_finite() {
        scaled.ceil().clamp(1.0, n_as_f64)
    } else {
        1.0
    };
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "clamped to [1.0, n] above, so the value is an exact integer in usize range"
    )]
    let idx = rank_f as usize - 1;
    sorted[idx]
}

/// Percentile summary of a latency sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatencyStats {
    /// Number of measured samples (warmup excluded).
    pub count: usize,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Mean, reported for context only — never gated on.
    pub mean_ms: f64,
}

impl LatencyStats {
    #[must_use]
    pub fn from_samples(samples: &[f64]) -> Self {
        let count = samples.len();
        #[allow(clippy::cast_precision_loss)]
        let mean_ms = if count == 0 {
            0.0
        } else {
            samples.iter().sum::<f64>() / count as f64
        };
        Self {
            count,
            p50_ms: percentile_ms(samples, 50.0),
            p99_ms: percentile_ms(samples, 99.0),
            max_ms: percentile_ms(samples, 100.0),
            mean_ms,
        }
    }
}

/// Outcome of comparing a measured p99 against the published budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetVerdict {
    WithinBudget,
    Exceeded,
}

/// Decide whether a measured p99 is within budget.
///
/// The budget is an **inclusive** ceiling: exactly-at-budget passes.
#[must_use]
pub fn budget_verdict(measured_p99_ms: f64, budget_ms: f64) -> BudgetVerdict {
    if measured_p99_ms > budget_ms {
        BudgetVerdict::Exceeded
    } else {
        BudgetVerdict::WithinBudget
    }
}

/// Environment variable that overrides the compiled-in claim p99 budget.
pub const BUDGET_ENV_VAR: &str = "HARVEST_CLAIM_BUDGET_P99_MS";

/// Resolve a budget from an optional string override, falling back to
/// `default_ms`.
///
/// A missing, unparseable, non-finite, or non-positive override falls back
/// rather than silently disabling the gate — a `0` budget would make the gate
/// unsatisfiable and a negative one would make it vacuous.
#[must_use]
pub fn budget_from_str(raw: Option<&str>, default_ms: f64) -> f64 {
    raw.and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(default_ms)
}

/// Resolve the effective budget, honouring [`BUDGET_ENV_VAR`].
#[must_use]
pub fn budget_from_env(default_ms: f64) -> f64 {
    budget_from_str(std::env::var(BUDGET_ENV_VAR).ok().as_deref(), default_ms)
}

// ---------------------------------------------------------------------------
// Pure unit tests (run on every OS, no Docker, no `db` feature).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pure_tests {
    // Cargo sets `cfg(test)` for bench targets too, but a `harness = false`
    // bench has no test harness, so rustc strips every `#[test]` fn from this
    // module when it is compiled into `benches/claim_bench.rs` — leaving these
    // imports genuinely unused *there* while they are used in the `integration`
    // test binary. Scoped to this module so a real unused import elsewhere in
    // the file still warns.
    #![allow(unused_imports)]

    use super::{
        BACKLOG_SWEEP, BudgetVerdict, ClaimGate, LatencyStats, budget_from_str, budget_verdict,
        headline_scenario, measured_claims_for, percentile_ms, warmup_claims_for,
    };

    #[test]
    fn warmup_discards_a_tenth_of_what_was_actually_collected() {
        // A full run sheds its cold start...
        assert_eq!(warmup_claims_for(100), 10);
        assert_eq!(warmup_claims_for(97), 9);
    }

    #[test]
    fn warmup_never_discards_a_truncated_runs_entire_sample() {
        // ...but a run cut short by the wall-clock budget must still report
        // something rather than printing a confident 0.00ms for zero samples.
        for collected in [0_usize, 1, 3, 5, 9, 20, 500] {
            let warmup = warmup_claims_for(collected);
            assert!(
                warmup < collected || collected == 0,
                "warmup {warmup} would discard all {collected} collected samples",
            );
        }
    }

    #[test]
    fn percentile_of_a_known_sample_is_exact() {
        // 1..=100 ms. p50 = 50th smallest, p99 = 99th smallest (nearest-rank).
        let samples: Vec<f64> = (1..=100).map(f64::from).collect();
        assert!((percentile_ms(&samples, 50.0) - 50.0).abs() < f64::EPSILON);
        assert!((percentile_ms(&samples, 99.0) - 99.0).abs() < f64::EPSILON);
        assert!((percentile_ms(&samples, 100.0) - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn percentile_is_order_independent() {
        let ascending: Vec<f64> = (1..=10).map(f64::from).collect();
        let descending: Vec<f64> = (1..=10).rev().map(f64::from).collect();
        assert!(
            (percentile_ms(&ascending, 99.0) - percentile_ms(&descending, 99.0)).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn percentile_of_empty_sample_is_zero() {
        assert!((percentile_ms(&[], 99.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn budget_verdict_trips_on_regression() {
        // THE falsifiability test: the gate must actually fail when slow.
        assert_eq!(budget_verdict(120.0, 50.0), BudgetVerdict::Exceeded);
        assert_eq!(budget_verdict(49.9, 50.0), BudgetVerdict::WithinBudget);
        // Exactly at budget is within (the budget is an inclusive ceiling).
        assert_eq!(budget_verdict(50.0, 50.0), BudgetVerdict::WithinBudget);
    }

    #[test]
    fn budget_override_is_read_from_env_value() {
        assert!((budget_from_str(Some("125.5"), 50.0) - 125.5).abs() < f64::EPSILON);
        // Absent or unparseable falls back to the compiled default.
        assert!((budget_from_str(None, 50.0) - 50.0).abs() < f64::EPSILON);
        assert!((budget_from_str(Some("not-a-number"), 50.0) - 50.0).abs() < f64::EPSILON);
        // A non-positive override is nonsense; fall back rather than gate on <= 0.
        assert!((budget_from_str(Some("0"), 50.0) - 50.0).abs() < f64::EPSILON);
        assert!((budget_from_str(Some("-5"), 50.0) - 50.0).abs() < f64::EPSILON);
        // Non-finite must not disable the gate either.
        assert!((budget_from_str(Some("inf"), 50.0) - 50.0).abs() < f64::EPSILON);
        assert!((budget_from_str(Some("NaN"), 50.0) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn latency_stats_report_both_percentiles_and_count() {
        let samples: Vec<f64> = (1..=100).map(f64::from).collect();
        let stats = LatencyStats::from_samples(&samples);
        assert_eq!(stats.count, 100);
        assert!((stats.p50_ms - 50.0).abs() < f64::EPSILON);
        assert!((stats.p99_ms - 99.0).abs() < f64::EPSILON);
        assert!((stats.max_ms - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn headline_scenario_is_the_documented_one() {
        // The published contract: 10k pending / 8 claimers / 4 queues.
        let s = headline_scenario();
        assert_eq!(s.backlog, 10_000);
        assert_eq!(s.claimers, 8);
        assert_eq!(s.queues, 4);
        assert_eq!(s.gate, ClaimGate::Baseline);
    }

    #[test]
    fn claims_never_exceed_a_fifth_of_the_backlog() {
        // Guards the "don't drain the queue" invariant: measuring must leave the
        // backlog at >= 80% of its seeded depth, or we measure an empty queue.
        for backlog in BACKLOG_SWEEP {
            let claims = measured_claims_for(backlog);
            assert!(
                claims * 5 <= backlog,
                "backlog {backlog}: {claims} claims would drain more than 20%",
            );
            assert!(claims > 0, "backlog {backlog}: must measure something");
        }
    }

    #[test]
    fn gate_names_are_unique_and_stable() {
        // `docs/performance.md` and the report table key on these strings.
        let all = ClaimGate::all();
        let mut names: Vec<&str> = all.iter().map(|g| g.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "gate identifiers must be unique");
    }
}

// ---------------------------------------------------------------------------
// DB-backed section — requires the `db` feature and a reachable Postgres.
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
pub mod db {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use diesel::QueryableByName;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::worker::DbPool;

    use super::{ClaimGate, LatencyStats, Scenario, measured_claims_for};

    /// Prefix for every object this harness creates, so a shared database can be
    /// cleaned up unambiguously.
    pub const BENCH_PREFIX: &str = "harvest-bench";
    /// Activity name carried by every seeded activity row.
    pub const BENCH_ACTIVITY: &str = "harvest_bench_activity";
    /// Workflow type name carried by every seeded execution.
    pub const BENCH_WORKFLOW: &str = "harvest_bench_wf";
    /// Build id stamped on rows in the `BuildPolicy` scenario.
    pub const BENCH_OLD_BUILD: &str = "harvest-bench-build-old";
    /// Build id the claiming worker advertises in the `BuildPolicy` scenario.
    /// Deliberately different from [`BENCH_OLD_BUILD`] so the claim resolves
    /// through the `harvest_build_compat` `EXISTS` branch — the expensive path —
    /// rather than the cheap `required_build_id = $3` equality.
    pub const BENCH_NEW_BUILD: &str = "harvest-bench-build-new";

    /// Distinct concurrency / rate-limit keys spread across the backlog.
    ///
    /// Large enough that 8 concurrent claimers rarely collide on the same
    /// `pg_try_advisory_xact_lock`, so the scenario measures the *predicate*
    /// cost rather than degenerating into lock contention.
    const KEY_CARDINALITY: usize = 256;

    /// Concurrency cap high enough never to block, so the `COUNT(*)` subquery
    /// and advisory lock are exercised without throttling the measurement.
    const NON_BLOCKING_CAP: i64 = 1_000_000;

    /// Token-bucket funding high enough never to throttle, for the same reason.
    const NON_BLOCKING_TOKENS: f64 = 1e9;

    static DB_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Why a bench/gate run was skipped rather than executed.
    #[derive(Debug)]
    pub struct SkipReason(pub String);

    /// A live benchmark database plus (when containerised) its keep-alive guard.
    ///
    /// The `ContainerAsync` **must** be held for the lifetime of the run —
    /// dropping it kills the container.
    pub struct BenchDb {
        pub url: String,
        _container: Option<ContainerAsync<Postgres>>,
    }

    /// Connect to a benchmark database, or explain why we cannot.
    ///
    /// * `HARVEST_TEST_DATABASE_URL` set: treated as an **admin** URL. A fresh,
    ///   uniquely-named database is created and migrated, so a 100k-row backlog
    ///   can never leak into a shared database and wreck sibling suites.
    /// * Otherwise: a `postgres:16` testcontainer.
    ///
    /// Returns `Err(SkipReason)` when no Docker daemon and no env URL are
    /// available, so `cargo bench` on a laptop without Docker prints a notice
    /// and exits cleanly instead of failing.
    pub async fn setup_bench_db() -> Result<BenchDb, SkipReason> {
        if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            let mut admin = AsyncPgConnection::establish(&admin_url)
                .await
                .map_err(|e| SkipReason(format!("connect {admin_url}: {e}")))?;
            drop_stale_bench_databases(&mut admin).await;
            let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
            let db = format!("harvest_claim_bench_{}_{}", std::process::id(), n);
            diesel::sql_query(format!("CREATE DATABASE {db}"))
                .execute(&mut admin)
                .await
                .map_err(|e| SkipReason(format!("create database {db}: {e}")))?;
            let url = with_db_name(&admin_url, &db);
            let mut conn = AsyncPgConnection::establish(&url)
                .await
                .map_err(|e| SkipReason(format!("connect {db}: {e}")))?;
            conn.batch_execute(autumn_harvest::full_migrations_sql())
                .await
                .map_err(|e| SkipReason(format!("migrate {db}: {e}")))?;
            return Ok(BenchDb {
                url,
                _container: None,
            });
        }

        let container = Postgres::default()
            .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .map_err(|e| {
                SkipReason(format!(
                    "no Docker daemon and HARVEST_TEST_DATABASE_URL unset ({e})"
                ))
            })?;
        let host = container
            .get_host()
            .await
            .map_err(|e| SkipReason(format!("container host: {e}")))?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|e| SkipReason(format!("container port: {e}")))?;
        Ok(BenchDb {
            url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            _container: Some(container),
        })
    }

    /// Prefix of every database this harness creates against an admin URL.
    const BENCH_DB_PREFIX: &str = "harvest_claim_bench_";

    /// Drop benchmark databases left behind by earlier runs.
    ///
    /// The admin-URL path creates a fresh database per run and — unlike the
    /// testcontainer path, where dropping the container reclaims everything —
    /// has no natural teardown: an async `Drop` cannot run a query. Without
    /// this sweep, repeatedly benchmarking against a long-lived local server
    /// accumulates 100k-row databases forever.
    ///
    /// Sweeping at *setup* rather than teardown is deliberate: it also reclaims
    /// databases orphaned by a run that panicked or was killed mid-measurement,
    /// which a teardown hook could never do.
    ///
    /// Databases belonging to a **live** process are skipped, so concurrent
    /// runs (and the parallel test harness) cannot delete each other's working
    /// set. Every failure here is ignored: a leaked database is untidy, but
    /// failing to reclaim one must never fail a benchmark.
    async fn drop_stale_bench_databases(admin: &mut AsyncPgConnection) {
        #[derive(QueryableByName)]
        struct NameRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            datname: String,
        }

        let Ok(rows) = diesel::sql_query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'harvest_claim_bench_%'",
        )
        .load::<NameRow>(admin)
        .await
        else {
            return;
        };

        let me = std::process::id();
        for row in rows {
            // Name shape: harvest_claim_bench_{pid}_{seq}. Ours, or another
            // live process's, is left alone.
            let owner_pid = row
                .datname
                .strip_prefix(BENCH_DB_PREFIX)
                .and_then(|rest| rest.split('_').next())
                .and_then(|pid| pid.parse::<u32>().ok());
            match owner_pid {
                Some(pid) if pid == me || process_is_alive(pid) => continue,
                Some(_) => {}
                // Unparseable name: not ours to reclaim.
                None => continue,
            }
            let _ = diesel::sql_query(format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                row.datname
            ))
            .execute(admin)
            .await;
        }
    }

    /// Best-effort liveness check for a pid that may own a bench database.
    ///
    /// Conservative by design: anything other than a confident "that process is
    /// gone" is reported as alive, so a database in use is never dropped out
    /// from under a concurrent run.
    fn process_is_alive(pid: u32) -> bool {
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new(&format!("/proc/{pid}")).exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            true
        }
    }

    fn with_db_name(url: &str, db: &str) -> String {
        url.rfind('/')
            .map_or_else(|| format!("{url}/{db}"), |i| format!("{}/{db}", &url[..i]))
    }

    /// Build a pool sized to the claimer count, so a measured claim never
    /// includes pool-checkout queueing.
    #[must_use]
    pub fn build_pool(url: &str, claimers: usize) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        deadpool::managed::Pool::builder(manager)
            .max_size(claimers.max(1) + 2)
            .build()
            .expect("pool build failed")
    }

    /// Establish a single connection.
    ///
    /// # Panics
    /// Panics if the connection cannot be established.
    pub async fn connect(url: &str) -> AsyncPgConnection {
        <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("failed to connect to benchmark database")
    }

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    /// What a seed actually produced.
    #[derive(Debug, Clone, Copy)]
    pub struct SeedOutcome {
        /// Rows inserted into `harvest_task_queue`.
        pub seeded_rows: usize,
        /// Of those, how many are actually claimable by the bench worker.
        /// Differs from `seeded_rows` for scenarios that deliberately seed
        /// unclaimable (PAUSED-execution) rows for the scan to walk past.
        pub claimable_rows: usize,
    }

    async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
        diesel::sql_query(sql)
            .execute(conn)
            .await
            .unwrap_or_else(|e| panic!("bench SQL failed: {e}\n--- sql ---\n{sql}"));
    }

    /// Remove every row this harness creates, so scenarios cannot contaminate
    /// each other. `harvest_task_queue` cascades from executions, but both are
    /// truncated explicitly so a scenario that seeds no executions still starts
    /// from an empty queue.
    pub async fn reset(conn: &mut AsyncPgConnection) {
        exec(
            conn,
            "TRUNCATE harvest_task_queue, harvest_workflow_executions, \
             harvest_rate_limit_buckets, harvest_build_compat, harvest_build_policies, \
             harvest_workers, harvest_queue_pauses RESTART IDENTITY CASCADE",
        )
        .await;
    }

    fn queue_name(index: usize) -> String {
        format!("{BENCH_PREFIX}-q-{index}")
    }

    /// Every queue a scenario spreads its backlog across.
    #[must_use]
    pub fn queue_names(scenario: Scenario) -> Vec<String> {
        (0..scenario.queues.max(1)).map(queue_name).collect()
    }

    /// The build id the claiming worker advertises for this scenario.
    ///
    /// An empty string short-circuits the build-routing filter entirely
    /// (`OR $3 = ''`), which is the legacy-worker fast path; the build scenarios
    /// advertise a real id so the filter is genuinely evaluated.
    #[must_use]
    pub const fn worker_build_id(gate: ClaimGate) -> &'static str {
        match gate {
            ClaimGate::BuildPolicy | ClaimGate::AllGates => BENCH_NEW_BUILD,
            _ => "",
        }
    }

    /// The circuit-breaker tracked-activity set passed to `claim_task`.
    #[must_use]
    pub fn circuit_breaker_set(gate: ClaimGate) -> Vec<String> {
        match gate {
            ClaimGate::CircuitBreakerSet | ClaimGate::AllGates => {
                vec![BENCH_ACTIVITY.to_string()]
            }
            _ => Vec::new(),
        }
    }

    const fn wants_build_id(gate: ClaimGate) -> bool {
        matches!(gate, ClaimGate::BuildPolicy | ClaimGate::AllGates)
    }

    const fn wants_concurrency(gate: ClaimGate) -> bool {
        matches!(gate, ClaimGate::ConcurrencyKey | ClaimGate::AllGates)
    }

    const fn wants_rate_limit(gate: ClaimGate) -> bool {
        matches!(
            gate,
            ClaimGate::RateLimited | ClaimGate::CircuitBreakerSet | ClaimGate::AllGates
        )
    }

    const fn wants_paused_rows(gate: ClaimGate) -> bool {
        matches!(gate, ClaimGate::PausedRows | ClaimGate::AllGates)
    }

    /// Seed the worker fleet.
    ///
    /// `claim_task`'s `worker_info` CTE reads `harvest_workers` for capability
    /// labels on every call, so a realistic measurement needs the rows to exist.
    async fn seed_workers(conn: &mut AsyncPgConnection, gate: ClaimGate) {
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_workers \
                   (worker_id, max_concurrency, host, build_id, queues, labels) \
                 SELECT '{BENCH_PREFIX}-worker-' || i, 16, 'bench-host', '{}', '[]'::jsonb, '{{}}'::jsonb \
                 FROM generate_series(0, 63) AS s(i)",
                worker_build_id(gate),
            ),
        )
        .await;
    }

    /// Seed the build-routing tables so the claim resolves through the
    /// `harvest_build_compat` `EXISTS` branch (issue #171).
    async fn seed_build_routing(conn: &mut AsyncPgConnection, queues: usize) {
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_build_compat (build_id, compatible_with) \
                 VALUES ('{BENCH_NEW_BUILD}', '{BENCH_OLD_BUILD}')"
            ),
        )
        .await;
        for q in 0..queues {
            exec(
                conn,
                &format!(
                    "INSERT INTO harvest_build_policies (queue_name, build_id) \
                     VALUES ('{}', '{BENCH_OLD_BUILD}')",
                    queue_name(q)
                ),
            )
            .await;
        }
    }

    /// Seed token buckets funded high enough never to throttle, so the
    /// rate-limit scenario measures the predicate rather than the throttle.
    async fn seed_rate_limit_buckets(conn: &mut AsyncPgConnection) {
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_rate_limit_buckets \
                   (key, refill_rate, burst, tokens, last_refilled_at) \
                 SELECT '{BENCH_PREFIX}-rl-' || i, {NON_BLOCKING_TOKENS}, {NON_BLOCKING_TOKENS}, {NON_BLOCKING_TOKENS}, NOW() \
                 FROM generate_series(0, {} ) AS s(i)",
                KEY_CARDINALITY - 1
            ),
        )
        .await;
    }

    /// Seed unclaimable ballast: rows belonging to PAUSED executions.
    ///
    /// These stay `PENDING` and so sit in `idx_harvest_tq_poll` alongside real
    /// work — the scan must walk past them via the `NOT EXISTS` anti-join
    /// (issue #383).
    ///
    /// Seeded IN ADDITION to the claimable backlog (rather than replacing part
    /// of it) so the claimable pool stays identical to the baseline and the only
    /// variable is the presence of paused rows.
    async fn seed_paused_ballast(conn: &mut AsyncPgConnection, backlog: usize, queues: usize) {
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_workflow_executions \
                   (workflow_name, workflow_id, shard_id, state, input, queue_name) \
                 SELECT '{BENCH_WORKFLOW}', '{BENCH_PREFIX}-wf-' || i, 0, 'PAUSED', \
                        '{{}}'::jsonb, '{BENCH_PREFIX}-q-' || (i % {queues}) \
                 FROM generate_series(0, {} ) AS s(i)",
                backlog - 1
            ),
        )
        .await;
        exec(
            conn,
            "INSERT INTO harvest_task_queue \
               (queue_name, task_type, workflow_exec_id, input, state, priority, \
                max_attempts, scheduled_at) \
             SELECT e.queue_name, 'workflow', e.id, '{}'::jsonb, 'PENDING', 0, 3, \
                    NOW() - INTERVAL '1 second' \
             FROM harvest_workflow_executions e",
        )
        .await;
    }

    /// Seed the claimable backlog: activity rows carrying whichever gate columns
    /// this scenario exercises.
    async fn seed_backlog(
        conn: &mut AsyncPgConnection,
        gate: ClaimGate,
        backlog: usize,
        queues: usize,
    ) {
        let build_expr = if wants_build_id(gate) {
            format!("'{BENCH_OLD_BUILD}'")
        } else {
            "NULL".to_string()
        };
        let (conc_key_expr, conc_cap_expr) = if wants_concurrency(gate) {
            (
                format!("'{BENCH_PREFIX}-ck-' || (i % {KEY_CARDINALITY})"),
                NON_BLOCKING_CAP.to_string(),
            )
        } else {
            ("NULL".to_string(), "NULL".to_string())
        };
        let rl_expr = if wants_rate_limit(gate) {
            format!("'{BENCH_PREFIX}-rl-' || (i % {KEY_CARDINALITY})")
        } else {
            "NULL".to_string()
        };

        exec(
            conn,
            &format!(
                "INSERT INTO harvest_task_queue \
                   (queue_name, task_type, activity_name, activity_id, input, state, \
                    priority, max_attempts, scheduled_at, required_build_id, \
                    concurrency_key, concurrency_cap, rate_limit_key) \
                 SELECT '{BENCH_PREFIX}-q-' || (i % {queues}), 'activity', '{BENCH_ACTIVITY}', \
                        gen_random_uuid(), '{{}}'::jsonb, 'PENDING', 0, 3, \
                        NOW() - INTERVAL '1 second', {build_expr}, \
                        {conc_key_expr}, {conc_cap_expr}, {rl_expr} \
                 FROM generate_series(0, {} ) AS s(i)",
                backlog - 1
            ),
        )
        .await;
    }

    /// Seed a scenario's backlog.
    ///
    /// Set-based throughout: a 100k-row backlog is one `INSERT ... SELECT FROM
    /// generate_series`, not 100k round trips. `ANALYZE` runs last — without it
    /// the planner works from stale statistics on a freshly bulk-loaded table
    /// and produces plans that are neither representative nor stable.
    ///
    /// # Panics
    /// Panics if any seeding statement fails.
    pub async fn seed(conn: &mut AsyncPgConnection, scenario: Scenario) -> SeedOutcome {
        reset(conn).await;

        let queues = scenario.queues.max(1);
        let backlog = scenario.backlog;
        let gate = scenario.gate;

        seed_workers(conn, gate).await;
        if wants_build_id(gate) {
            seed_build_routing(conn, queues).await;
        }
        if wants_rate_limit(gate) {
            seed_rate_limit_buckets(conn).await;
        }
        seed_backlog(conn, gate, backlog, queues).await;

        let mut seeded_rows = backlog;
        if wants_paused_rows(gate) {
            seed_paused_ballast(conn, backlog, queues).await;
            seeded_rows += backlog;
        }

        exec(conn, "ANALYZE harvest_task_queue").await;
        exec(conn, "ANALYZE harvest_workflow_executions").await;

        SeedOutcome {
            seeded_rows,
            claimable_rows: backlog,
        }
    }

    /// Result of one measured claim scenario.
    #[derive(Debug, Clone)]
    pub struct ClaimReport {
        pub scenario: Scenario,
        pub seed: SeedOutcome,
        pub stats: LatencyStats,
        /// Measured operations that actually returned a task.
        pub claimed: usize,
        /// Measured operations that returned `None` (lost a `SKIP LOCKED` race,
        /// or the queue had nothing eligible).
        pub empty: usize,
        /// Wall time for the whole measured phase.
        pub wall_secs: f64,
        /// At least one claimer hit the wall-clock budget before completing its
        /// planned operations. The percentiles are still real, but they rest on
        /// fewer samples than requested — a signal in itself that the claim path
        /// is slow at this depth.
        pub truncated: bool,
    }

    impl ClaimReport {
        /// Fraction of measured operations that actually claimed a task.
        ///
        /// A run where this is low measured contention or an empty queue rather
        /// than the claim path, so its percentiles are not meaningful. The gate
        /// asserts a floor on this.
        #[must_use]
        pub fn claim_ratio(&self) -> f64 {
            let total = self.claimed + self.empty;
            if total == 0 {
                return 0.0;
            }
            #[allow(clippy::cast_precision_loss)]
            {
                self.claimed as f64 / total as f64
            }
        }

        /// Claims per second across the measured phase.
        #[must_use]
        pub fn claims_per_sec(&self) -> f64 {
            if self.wall_secs <= 0.0 {
                return 0.0;
            }
            #[allow(clippy::cast_precision_loss)]
            {
                self.claimed as f64 / self.wall_secs
            }
        }
    }

    /// Seed and measure one claim scenario end to end.
    ///
    /// # Panics
    /// Panics if seeding or connection acquisition fails.
    pub async fn run_claim_scenario(db: &BenchDb, scenario: Scenario) -> ClaimReport {
        let mut conn = connect(&db.url).await;
        let seed_outcome = seed(&mut conn, scenario).await;
        drop(conn);

        let pool = build_pool(&db.url, scenario.claimers);
        let queues = Arc::new(queue_names(scenario));
        let cb_set = Arc::new(circuit_breaker_set(scenario.gate));
        let build_id = worker_build_id(scenario.gate).to_string();

        let total_ops = measured_claims_for(scenario.backlog);
        let per_claimer = (total_ops / scenario.claimers.max(1)).max(1);
        let budget = super::scenario_time_budget();

        let started = Instant::now();
        let mut handles = Vec::with_capacity(scenario.claimers);
        for c in 0..scenario.claimers.max(1) {
            let pool = pool.clone();
            let queues = Arc::clone(&queues);
            let cb_set = Arc::clone(&cb_set);
            let build_id = build_id.clone();
            handles.push(tokio::spawn(async move {
                let worker = format!("{BENCH_PREFIX}-worker-{c}");
                // (latency_ms, claimed_a_task) in observation order, so warmup
                // can be trimmed from the front once we know how many
                // observations this claimer actually managed to take.
                let mut observed: Vec<(f64, bool)> = Vec::with_capacity(per_claimer);
                let mut conn = pool.get().await.expect("pool checkout");
                let deadline = Instant::now() + budget;
                let mut truncated = false;
                for _ in 0..per_claimer {
                    // Wall-clock ceiling: claim latency scales with backlog
                    // depth, so an operation count alone does not bound a deep
                    // sweep (or a regression). Stop early and report the
                    // samples collected rather than running for minutes.
                    if Instant::now() >= deadline {
                        truncated = true;
                        break;
                    }
                    let t0 = Instant::now();
                    let got = queue::claim_task(
                        &mut conn,
                        &queues,
                        &worker,
                        &build_id,
                        None,
                        &cb_set,
                        &[],
                    )
                    .await
                    .expect("claim_task failed");
                    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                    observed.push((elapsed, got.is_some()));
                }

                // Trim warmup now that the real observation count is known: the
                // first calls on a fresh connection pay plan-cache and
                // buffer-cache costs that are not representative of steady
                // state, and left in they dominate the tail.
                let warmup = super::warmup_claims_for(observed.len());
                let mut samples: Vec<f64> = Vec::with_capacity(observed.len() - warmup);
                let mut claimed = 0usize;
                let mut empty = 0usize;
                for (elapsed, got) in observed.into_iter().skip(warmup) {
                    samples.push(elapsed);
                    if got {
                        claimed += 1;
                    } else {
                        empty += 1;
                    }
                }
                (samples, claimed, empty, truncated)
            }));
        }

        let mut all_samples: Vec<f64> = Vec::with_capacity(total_ops);
        let mut claimed = 0usize;
        let mut empty = 0usize;
        let mut truncated = false;
        for h in handles {
            let (samples, c, e, t) = h.await.expect("claimer task panicked");
            all_samples.extend(samples);
            claimed += c;
            empty += e;
            truncated |= t;
        }
        let wall_secs = started.elapsed().as_secs_f64();

        ClaimReport {
            scenario,
            seed: seed_outcome,
            stats: LatencyStats::from_samples(&all_samples),
            claimed,
            empty,
            wall_secs,
            truncated,
        }
    }

    /// Result of the enqueue throughput measurement.
    #[derive(Debug, Clone)]
    pub struct EnqueueReport {
        /// Backlog depth the writes were issued against.
        pub backlog: usize,
        pub writers: usize,
        pub rows: usize,
        pub stats: LatencyStats,
        pub wall_secs: f64,
    }

    impl EnqueueReport {
        #[must_use]
        pub fn rows_per_sec(&self) -> f64 {
            if self.wall_secs <= 0.0 {
                return 0.0;
            }
            #[allow(clippy::cast_precision_loss)]
            {
                self.rows as f64 / self.wall_secs
            }
        }
    }

    fn bench_enqueue_params(queue: String) -> EnqueueParams {
        EnqueueParams {
            queue_name: queue,
            task_type: TaskType::Activity,
            workflow_exec_id: None,
            activity_name: Some(BENCH_ACTIVITY.to_string()),
            activity_id: Some(uuid::Uuid::new_v4()),
            input: serde_json::json!({}),
            priority: 0,
            max_attempts: 3,
            scheduled_at: chrono::Utc::now(),
            heartbeat_timeout: None,
            start_to_close: None,
            schedule_to_start: None,
            retry_policy: None,
            sticky_worker_id: None,
            sticky_timeout: None,
            trace_context: None,
            concurrency_key: None,
            max_concurrent: None,
            required_build_id: None,
            rate_limit_key: None,
            schedule_to_close_at: None,
            required_capabilities: None,
            context_headers: None,
            session_id: None,
        }
    }

    /// Measure sustained `enqueue` throughput into an already-non-empty queue.
    ///
    /// Start-storms stress the write side, so the backlog is seeded first and
    /// the writes are issued against a queue that is already deep.
    ///
    /// # Panics
    /// Panics if seeding or enqueueing fails.
    pub async fn run_enqueue_scenario(
        db: &BenchDb,
        backlog: usize,
        writers: usize,
        rows_per_writer: usize,
    ) -> EnqueueReport {
        let scenario = Scenario {
            backlog,
            claimers: writers,
            queues: headline_queue_count(),
            gate: ClaimGate::Baseline,
        };
        let mut conn = connect(&db.url).await;
        seed(&mut conn, scenario).await;
        drop(conn);

        let pool = build_pool(&db.url, writers);
        let queues = Arc::new(queue_names(scenario));

        let started = Instant::now();
        let mut handles = Vec::with_capacity(writers);
        for w in 0..writers.max(1) {
            let pool = pool.clone();
            let queues = Arc::clone(&queues);
            handles.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(rows_per_writer);
                let mut conn = pool.get().await.expect("pool checkout");
                for i in 0..rows_per_writer {
                    let q = queues[(w + i) % queues.len()].clone();
                    let params = bench_enqueue_params(q);
                    let t0 = Instant::now();
                    queue::enqueue(&mut conn, &params)
                        .await
                        .expect("enqueue failed");
                    samples.push(t0.elapsed().as_secs_f64() * 1000.0);
                }
                samples
            }));
        }

        let mut all_samples = Vec::with_capacity(writers * rows_per_writer);
        for h in handles {
            all_samples.extend(h.await.expect("writer task panicked"));
        }
        let wall_secs = started.elapsed().as_secs_f64();

        EnqueueReport {
            backlog,
            writers,
            rows: all_samples.len(),
            stats: LatencyStats::from_samples(&all_samples),
            wall_secs,
        }
    }

    const fn headline_queue_count() -> usize {
        super::headline_scenario().queues
    }

    /// `EXPLAIN (ANALYZE, BUFFERS)` of the claim query for a seeded scenario.
    ///
    /// The single most useful artifact for a contributor about to add predicate
    /// number twelve: it shows exactly which index the claim rides and what each
    /// accreted subquery costs.
    ///
    /// Runs inside a transaction that is rolled back, so the `EXPLAIN ANALYZE`
    /// (which really executes the statement, including its `UPDATE` CTEs) does
    /// not consume a task.
    ///
    /// # Panics
    /// Panics if the `EXPLAIN` cannot be run.
    pub async fn explain_claim(conn: &mut AsyncPgConnection, scenario: Scenario) -> String {
        let queues = queue_names(scenario);
        let queue_list = queues
            .iter()
            .map(|q| format!("'{q}'"))
            .collect::<Vec<_>>()
            .join(",");
        let cb = circuit_breaker_set(scenario.gate);
        let cb_list = if cb.is_empty() {
            String::new()
        } else {
            cb.iter()
                .map(|a| format!("'{a}'"))
                .collect::<Vec<_>>()
                .join(",")
        };
        let sql = queue::claim_task_query()
            .replace("$1", &format!("'{BENCH_PREFIX}-worker-0'"))
            .replace("$2", &format!("ARRAY[{queue_list}]::text[]"))
            .replace("$3", &format!("'{}'", worker_build_id(scenario.gate)))
            .replace("$4", "NULL")
            .replace("$5", &format!("ARRAY[{cb_list}]::text[]"))
            .replace("$6", "ARRAY[]::text[]");

        // `EXPLAIN ANALYZE` really executes the statement — including the
        // `UPDATE` CTEs — so it runs inside a transaction that is rolled back.
        // Otherwise producing the plan would itself consume a task.
        exec(conn, "BEGIN").await;
        let loaded: Result<Vec<ExplainRow>, _> = diesel::sql_query(format!(
            "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) {sql}"
        ))
        .load(conn)
        .await;
        exec(conn, "ROLLBACK").await;

        match loaded {
            Ok(rows) => rows
                .into_iter()
                .map(|r| r.query_plan)
                .collect::<Vec<_>>()
                .join("\n"),
            Err(e) => format!("EXPLAIN failed: {e}"),
        }
    }

    #[derive(QueryableByName)]
    struct ExplainRow {
        #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
        query_plan: String,
    }

    /// Count `PENDING` rows currently in the queue — used by tests to assert the
    /// backlog was not drained.
    ///
    /// # Panics
    /// Panics if the count query fails.
    pub async fn pending_count(conn: &mut AsyncPgConnection) -> i64 {
        let rows: Vec<CountRow> = diesel::sql_query(
            "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue WHERE state = 'PENDING'",
        )
        .load(conn)
        .await
        .expect("count pending");
        // `into_iter().next()` rather than `.first()`: diesel's `RunQueryDsl`
        // is in scope and its `first(self, conn)` method shadows `slice::first`.
        rows.into_iter().next().map_or(0, |r| r.n)
    }

    /// Postgres server version string, so a published baseline records which
    /// planner produced it.
    ///
    /// # Panics
    /// Never panics; an unreadable version degrades to `"unknown"`.
    pub async fn server_version(conn: &mut AsyncPgConnection) -> String {
        #[derive(QueryableByName)]
        struct VersionRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            v: String,
        }
        let rows: Result<Vec<VersionRow>, _> =
            diesel::sql_query("SELECT version() AS v").load(conn).await;
        rows.ok()
            .and_then(|r| r.into_iter().next())
            .map_or_else(|| "unknown".to_string(), |r| r.v)
    }

    /// A short description of the machine the numbers were produced on, so a
    /// published baseline is attributable.
    #[must_use]
    pub fn machine_fingerprint() -> String {
        let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get);
        format!("{} / {} logical CPUs", std::env::consts::OS, cpus)
    }
}
