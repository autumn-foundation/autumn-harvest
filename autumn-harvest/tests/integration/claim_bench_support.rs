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
/// 3.7. These variants let the benchmark attribute cost to **five** of them
/// instead of reporting a single opaque number.
///
/// Deliberately **not exhaustive**, and the gap is not visible from this list:
/// capability labels (#382), queue pauses (#619), `schedule_to_close` (#378),
/// worker sessions (#606) and sticky routing (#235) are all in the query on
/// every claim, but `db::seed_backlog` leaves their columns null and nothing
/// ever inserts a `harvest_queue_pauses` row, so those subplans only ever see
/// empty or null input. They are evaluated, not measured — the cheapest path
/// each of them has. Adding one means a seed variant *and* a report row; see
/// the "Known limitations" section of `docs/performance.md`, which ranks them
/// by how much the omission is likely to matter.
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
    /// **Control for [`Self::PausedRows`], not a gate.**
    ///
    /// Seeds `2 * backlog` rows, all of them plain and claimable. This is the
    /// same total table depth `PausedRows` produces, with no PAUSED subplan —
    /// so `DoubleBacklog - Baseline` is the cost of *depth alone* and
    /// `PausedRows - DoubleBacklog` is the cost of the anti-join. Without it,
    /// the `PausedRows` delta conflates the two and overstates the predicate.
    DoubleBacklog,
    /// An *additional* `backlog` rows belong to `PAUSED` executions, on top of
    /// the claimable backlog, so the scan walks past unclaimable rows via the
    /// `NOT EXISTS` anti-join (issue #383). Total seeded depth is
    /// `2 * backlog`; compare against [`Self::DoubleBacklog`], not
    /// [`Self::Baseline`], to isolate the predicate from the depth.
    PausedRows,
    /// Every gate above at once.
    ///
    /// Not a strict upper bound: the circuit-breaker tracked set short-circuits
    /// the rate-limit `EXISTS` and the debit CTE (`= ANY($5)` wins), so a
    /// deployment with rate limiting and *no* breaker executes strictly more
    /// work than this scenario does.
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
            Self::DoubleBacklog => "double_backlog",
            Self::PausedRows => "paused_rows",
            Self::AllGates => "all_gates",
        }
    }

    /// Every gate, in report order. `Baseline` is first so the table reads as
    /// "baseline, then what each predicate adds". `DoubleBacklog` sits directly
    /// before `PausedRows` because it is that row's control.
    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::Baseline,
            Self::BuildPolicy,
            Self::ConcurrencyKey,
            Self::RateLimited,
            Self::CircuitBreakerSet,
            Self::DoubleBacklog,
            Self::PausedRows,
            Self::AllGates,
        ]
    }

    /// Which gate a row's `vs` column should be measured against.
    ///
    /// Every gate compares to `Baseline` except `PausedRows`, whose honest
    /// comparand is the equal-depth [`Self::DoubleBacklog`] control.
    #[must_use]
    pub const fn comparand(self) -> Self {
        match self {
            Self::PausedRows => Self::DoubleBacklog,
            _ => Self::Baseline,
        }
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

/// The published budget CI defends for [`headline_scenario`], in milliseconds.
///
/// **Applied to p50, not p99 — deliberately, from measurement.** The headline
/// scenario runs 8 concurrent claimers against a 4-core reference box, so the
/// database is genuinely oversubscribed and the *tail* stops describing the
/// claim path. Measured across repeated runs on that box:
///
/// | statistic | idle box | loaded box | spread |
/// |:--|--:|--:|--:|
/// | p50 | ~220 ms | ~516 ms | **~2.3x** |
/// | p99 | ~300 ms | ~4 665 ms | **~15x** |
///
/// A p99 gate at this budget failed roughly one run in three during review —
/// on the *same hardware class and Postgres version* that produced the
/// published numbers. It was not measuring a regression; it was measuring the
/// run queue. p50 is the statistic that tracks the query under the same load,
/// so it is the one asserted. p99 is still measured, still published, and still
/// printed in the failure message for diagnosis — it is simply not the
/// assertion, because a gate that flakes is a gate everyone learns to ignore.
///
/// The number itself is derived, not chosen: reference p50 sits near 220 ms
/// idle and 516 ms loaded, so this is ~2.9x the worst observation and ~6.8x the
/// idle one. That headroom is what makes it a **cliff detector, not a drift
/// detector** — it will not catch a predicate that makes claims 50% slower.
/// `docs/performance.md`'s per-gate table is the tool for that.
///
/// Lives here rather than in the gate test so that
/// [`pure_tests::wall_clock_budget_admits_a_run_at_the_latency_budget`] can hold
/// it and [`DEFAULT_SCENARIO_BUDGET_SECS`] to a consistent pair.
pub const HEADLINE_P50_BUDGET_MS: f64 = 1_500.0;

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
///
/// **It must be large enough that a run sitting exactly at
/// [`HEADLINE_P50_BUDGET_MS`] still completes.** Otherwise a slow-but-passing
/// run truncates first and the gate reports "measurement unsound" instead of
/// the latency verdict it exists to give — the wall-clock ceiling would have
/// silently become the real budget. At the headline scenario that floor is
/// `(800 ops / 8 claimers) x 1.5s = 150s`;
/// [`pure_tests::wall_clock_budget_admits_a_run_at_the_latency_budget`] pins the
/// relationship so the two constants cannot drift into an incoherent pair.
const DEFAULT_SCENARIO_BUDGET_SECS: u64 = 240;

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

/// How many claim operations claimer `index` of `claimers` should perform.
///
/// Distributes `total_ops` exactly: the first `total_ops % claimers` claimers
/// take one extra operation and the sum over all claimers is exactly
/// `total_ops`. A naive `(total_ops / claimers).max(1)` per claimer would
/// *exceed* `total_ops` whenever `claimers > total_ops`, breaking the
/// [`MAX_DRAIN_FRACTION`] guarantee that a run never consumes more than a fifth
/// of the backlog. Claimers past `total_ops` get 0 and do no work, which is the
/// correct outcome — the drain bound wins over keeping every task busy.
#[must_use]
pub const fn claims_for_claimer(total_ops: usize, claimers: usize, index: usize) -> usize {
    if claimers == 0 {
        return 0;
    }
    let base = total_ops / claimers;
    let remainder = total_ops % claimers;
    if index < remainder { base + 1 } else { base }
}

/// The measured window for a scenario: earliest resume to last completion.
///
/// Each worker reports the `(resume, finish)` pair from its **own** clock, and
/// this folds them into one interval on the shared monotonic clock. That matters
/// because the two are not interchangeable: with more workers than runtime
/// threads a worker is not polled the instant the start barrier releases, so
/// `max(finish_i - resume_i)` silently drops every worker's release-to-resume
/// delay while still counting all of its work — a too-small denominator, i.e.
/// throughput reported higher than it actually was. The earliest resume is the
/// closest observable stand-in for the release itself, so folding against it
/// charges that delay to every later worker.
///
/// Returns `0.0` for an empty slice, which the callers' `wall_secs <= 0.0`
/// guards already treat as "no measurement".
#[must_use]
pub fn measured_window(spans: &[(std::time::Instant, std::time::Instant)]) -> f64 {
    let Some(start) = spans.iter().map(|&(s, _)| s).min() else {
        return 0.0;
    };
    // `max(start)` keeps the interval non-negative even if a worker somehow
    // reported a finish before the earliest resume; `duration_since` would
    // otherwise saturate to zero anyway, but being explicit documents it.
    let end = spans
        .iter()
        .map(|&(_, e)| e)
        .max()
        .unwrap_or(start)
        .max(start);
    end.duration_since(start).as_secs_f64()
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

/// The one-in-N head trim [`warmup_claims_for`] applies, for report legends.
#[must_use]
pub const fn warmup_divisor() -> usize {
    WARMUP_DIVISOR
}

/// What one claimer task reports back, split into the two windows a report
/// needs.
///
/// Named rather than positional: three of these fields are `usize` and mean
/// different things over different windows, so a tuple would make transposing
/// `claimed` and `total_claimed` at the aggregation site a silent
/// reintroduction of the numerator/denominator mismatch the second counter
/// exists to prevent.
///
/// Lives in the pure section, away from the database code that produces it, so
/// the window split is unit-tested on every OS — including the
/// `--no-default-features` leg, where no claim ever runs.
#[derive(Debug, Clone, PartialEq)]
pub struct ClaimerOutcome {
    /// Post-warmup latencies, milliseconds.
    pub samples: Vec<f64>,
    /// Post-warmup calls that returned a task.
    pub claimed: usize,
    /// Post-warmup calls that returned `None`.
    pub empty: usize,
    /// Every call that returned a task, warmup included.
    pub total_claimed: usize,
    /// This claimer stopped early on the scenario deadline.
    pub truncated: bool,
}

impl ClaimerOutcome {
    /// Split one claimer's observations into the two windows.
    ///
    /// `observed` is `(latency_ms, claimed_a_task)` in call order.
    ///
    /// The head is trimmed as warmup for the *latency* statistics: the first
    /// calls on a fresh connection pay plan-cache and buffer-cache costs that
    /// are not steady state, and left in they dominate the tail. The
    /// *throughput* numerator deliberately does not trim, because its
    /// denominator — the scenario wall clock — starts at the first warmup call.
    /// Trimming one side and not the other understated `claims/s` by the warmup
    /// fraction, about 10%.
    #[must_use]
    pub fn from_observed(observed: Vec<(f64, bool)>, truncated: bool) -> Self {
        let warmup = warmup_claims_for(observed.len());
        let total_claimed = observed.iter().filter(|(_, got)| *got).count();
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
        Self {
            samples,
            claimed,
            empty,
            total_claimed,
            truncated,
        }
    }
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

/// Outcome of comparing a measured latency against the published budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetVerdict {
    WithinBudget,
    Exceeded,
}

/// Decide whether a measured latency is within budget.
///
/// Statistic-agnostic on purpose: the gate applies it to p50 (see
/// [`HEADLINE_P50_BUDGET_MS`] for why), but nothing here depends on that.
///
/// The budget is an **inclusive** ceiling: exactly-at-budget passes.
#[must_use]
pub fn budget_verdict(measured_ms: f64, budget_ms: f64) -> BudgetVerdict {
    if measured_ms > budget_ms {
        BudgetVerdict::Exceeded
    } else {
        BudgetVerdict::WithinBudget
    }
}

/// Environment variable that overrides the compiled-in claim-latency budget.
///
/// Deliberately not named for a statistic: the gate asserts p50 today, and a
/// name like `..._P99_MS` would be a lie the moment it was read.
pub const BUDGET_ENV_VAR: &str = "HARVEST_CLAIM_BUDGET_MS";

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

/// Connection-parameter names whose values are shape, never a credential.
///
/// An **allowlist**, not a denylist of `password`, because this gates what
/// reaches a CI log: a parameter nobody thought of has to default to hidden.
/// libpq alone carries `password` *and* `sslpassword`, and `options` can carry
/// arbitrary text — a denylist would have to stay ahead of all of them forever,
/// while this only has to stay ahead of what is worth printing. `user` is
/// deliberately absent: the URL form already replaces the whole `user:password@`
/// segment, so printing it from the query would be inconsistent.
const PRINTABLE_CONN_PARAMS: &[&str] = &[
    "application_name",
    "connect_timeout",
    "dbname",
    "host",
    "hostaddr",
    "load_balance_hosts",
    "port",
    "sslmode",
    "target_session_attrs",
];

const fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Percent-decode, matching `tokio-postgres`' `UrlParser::decode`.
///
/// That is a plain `percent_decode(..).decode_utf8()`: a `%` not followed by two
/// hex digits is literal, and `+` is **not** a space (this is a URI, not an HTML
/// form). `None` when the result is not valid UTF-8 — precisely when the
/// driver's own `decode` returns `Err` and the connection string is unusable, so
/// a caller can treat that as "the driver will never connect with this anyway".
fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(hi), Some(lo)) = (hex_val(b[i + 1]), hex_val(b[i + 2]))
        {
            out.push(hi * 16 + lo);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// The key of a URL **query** pair, decoded the way the driver decodes it.
///
/// `UrlParser::parse_params` runs `self.decode(key)?` before dispatching to
/// `Config::param`, so `db%6Eame=admin` is a `dbname` override. Comparing the
/// raw key therefore misses it — and for a filter that exists to *strip* an
/// overriding parameter, missing it is the failure that matters.
///
/// An undecodable key falls back to its raw form. The driver errors out on such
/// a string, so it can never connect; matching the raw text is the conservative
/// reading for a filter whose job is to remove things.
fn decoded_query_key(pair: &str) -> String {
    decode_or_raw(pair.split_once('=').map_or(pair, |(k, _)| k))
}

/// [`percent_decode`], falling back to the raw span when the bytes are not UTF-8.
///
/// The driver *rejects* that input rather than falling back, so such a string
/// can never open a connection. Returning the raw span keeps the caller
/// comparing text instead of losing the span entirely.
fn decode_or_raw(s: &str) -> String {
    percent_decode(s).unwrap_or_else(|| s.to_string())
}

/// A connection URL split into the regions **the driver's own parser sees**.
///
/// Deliberately not an RFC 3986 split. `tokio-postgres` hand-rolls its URL
/// parsing (`UrlParser`), and its grammar diverges from the RFC's in two ways
/// that each hide a credential from a naive splitter:
///
/// 1. **There is no fragment.** `parse_host` stops at `/` or `?`, `parse_path`
///    runs to `?`, and `parse_params` terminates each value only at `&`. So in
///    `?password=#hunter2` the `#hunter2` is part of the password, and code
///    that treats it as a fragment and passes it through prints the secret.
/// 2. **Credentials are parsed before the host, not inside an authority.**
///    `parse_credentials` is `take_until(&['@'])` over the whole remaining
///    string, with no `/` or `?` terminator, so `alice:sec/ret@db/postgres`
///    authenticates as `alice` / `sec/ret`. An authority-first split stops at
///    the `/`, finds no `@`, and prints the credential in full.
///
/// Splitting once, here, is what keeps the three helpers below from each
/// re-deriving a slightly different — and slightly wrong — view of the string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DriverUrl<'a> {
    /// Scheme through `://`.
    prefix: &'a str,
    /// Everything `parse_credentials` consumes, `@` excluded. `None` when the
    /// remainder holds no `@` at all, which is when the driver's
    /// `parse_credentials` declines and leaves the whole span to `parse_host`.
    credentials: Option<&'a str>,
    /// What `parse_host` reads: up to the first `/` or `?` *after* credentials.
    host: &'a str,
    /// Path without its leading `/`; `None` when the URL carries no path.
    path: Option<&'a str>,
    /// Everything after the first `?`, verbatim. `None` when there is no `?`.
    query: Option<&'a str>,
}

/// The exact scheme prefixes `UrlParser::remove_url_prefix` accepts.
///
/// Byte-exact and **prefix-only**: the driver strips these with
/// `str::strip_prefix` and, failing that, hands the *whole* string to the
/// keyword/value parser. So the two forms are chosen by how the string begins,
/// never by what it contains — `host=db password=http://hunter2` is keyword
/// form, and a case variant like `POSTGRES://…` is too.
const URL_SCHEME_PREFIXES: [&str; 2] = ["postgres://", "postgresql://"];

/// Split a URL-form connection string, or `None` for the keyword/value form.
///
/// The split follows `UrlParser::parse`'s *order* — credentials, then host,
/// then path, then query — rather than the RFC 3986 authority grammar. The
/// difference is load-bearing: `parse_credentials` scans for the first `@` in
/// the **whole** remaining string, so a password containing `/` or `?` still
/// terminates at that `@`, whereas an authority-first split would stop short
/// and never see it.
///
/// Form selection is likewise the driver's, not a guess: only the prefixes in
/// [`URL_SCHEME_PREFIXES`] make a string a URL. Searching for `://` anywhere
/// would read an ordinary keyword value such as `password=http://hunter2` as a
/// scheme, and then redact none of the string that followed it.
fn split_driver_url(url: &str) -> Option<DriverUrl<'_>> {
    let (prefix, rest) = URL_SCHEME_PREFIXES
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme).map(|rest| (*scheme, rest)))?;

    // `parse_credentials`: `take_until(&['@'])` over everything that is left,
    // so the first `@` wins no matter what precedes it.
    let (credentials, after_credentials) = rest
        .find('@')
        .map_or((None, rest), |at| (Some(&rest[..at]), &rest[at + 1..]));

    // `parse_host`: `take_until(&['/', '?'])`, else the rest. `#` is
    // deliberately absent — the driver has no fragment concept.
    let host_end = after_credentials
        .find(['/', '?'])
        .unwrap_or(after_credentials.len());
    let (host, tail) = after_credentials.split_at(host_end);

    let (path, query) = tail.strip_prefix('/').map_or_else(
        // No leading `/`, so `parse_path` declines: there is no path at all,
        // and whatever follows can only be a query.
        || (None, tail.strip_prefix('?')),
        |after_slash| {
            after_slash
                .split_once('?')
                .map_or((Some(after_slash), None), |(p, q)| (Some(p), Some(q)))
        },
    );

    Some(DriverUrl {
        prefix,
        credentials,
        host,
        path,
        query,
    })
}

/// One `keyword = value` pair from a libpq keyword/value connection string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeywordPair<'a> {
    key: &'a str,
    /// The value's raw span, quotes included if it was quoted.
    raw_value: &'a str,
}

/// Strip one layer of libpq single-quoting from a value span.
///
/// Escapes are deliberately *not* unescaped, so the result stays a borrowed
/// slice. Every name this harness generates is `[a-z0-9_]`, so it never needs
/// either.
fn unquote_span(span: &str) -> &str {
    span.strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(span)
}

/// Byte offset of the first char at or after `i` satisfying `pred`, else `s.len()`.
fn find_from(s: &str, i: usize, pred: impl Fn(char) -> bool) -> usize {
    s[i..].find(pred).map_or(s.len(), |off| i + off)
}

/// Byte offset of the first non-whitespace char at or after `i`.
///
/// The predicate is `char::is_whitespace`, matching `Parser::skip_ws` exactly.
/// An ASCII-only predicate is not a harmless approximation: on
/// `host=db.internal\u{00A0}password=hunter2` it reads the no-break space as an
/// ordinary value char, so the whole tail becomes the value of the *allowlisted*
/// `host` key and the redactor prints the password verbatim.
fn skip_ws(s: &str, i: usize) -> usize {
    find_from(s, i, |c| !c.is_whitespace())
}

/// End offset of an unquoted value starting at `i`, per `Parser::simple_value`.
///
/// A `\` escapes whatever char follows — including a whitespace char, which is
/// why this cannot be a plain "scan to the next space".
fn simple_value_end(s: &str, i: usize) -> usize {
    let mut it = s[i..].char_indices();
    while let Some((off, c)) = it.next() {
        if c == '\\' {
            // Consume the escaped char, whatever its width.
            it.next();
        } else if c.is_whitespace() {
            return i + off;
        }
    }
    s.len()
}

/// End offset *past* the closing quote of a quoted value starting at `i`, or
/// `None` when the quote is never closed (which the driver treats as an error).
fn quoted_value_end(s: &str, i: usize) -> Option<usize> {
    // The opening quote is one ASCII byte.
    let open = i + 1;
    let mut it = s[open..].char_indices();
    while let Some((off, c)) = it.next() {
        if c == '\\' {
            it.next()?;
        } else if c == '\'' {
            return Some(open + off + 1);
        }
    }
    None
}

/// Parse a libpq keyword/value connection string into `(key, raw value)` pairs.
///
/// Returns the pairs together with the offset at which parsing stopped, so a
/// caller can **fail closed** on the remainder rather than printing it.
///
/// Splitting on whitespace is not sufficient, and fails in the direction that
/// leaks. libpq's own grammar (mirrored by `tokio_postgres::Config`'s
/// `Parser::keyword`/`value`) reads a keyword up to whitespace *or* `=`, then
/// skips whitespace on both sides of the `=`, then reads a value that may be
/// single-quoted and may carry backslash escapes. So all of these are one pair:
///
/// ```text
/// password=hunter2
/// password='hunter two'
/// password = 'hunter two'
/// password='hun\'ter two'
/// ```
///
/// A whitespace split turns the third into `password`, `=` and a bare secret —
/// and the secret has no embedded `=`, so a key/value redactor waves it through.
fn keyword_value_pairs(s: &str) -> (Vec<KeywordPair<'_>>, usize) {
    let mut out = Vec::new();
    let mut i = skip_ws(s, 0);

    while i < s.len() {
        // The offset to report if this pair turns out to be malformed: the
        // whole pair is then unparsed, not just its tail.
        let pair_start = i;

        // `Parser::keyword`: everything up to whitespace or `=`.
        let key_end = find_from(s, i, |c| c.is_whitespace() || c == '=');
        if key_end == i {
            // A stray `=` with no keyword before it.
            return (out, pair_start);
        }
        let key = &s[i..key_end];

        // `skip_ws`, `eat('=')`, `skip_ws` — all three, in that order.
        i = skip_ws(s, key_end);
        if !s[i..].starts_with('=') {
            return (out, pair_start);
        }
        i = skip_ws(s, i + 1);
        if i >= s.len() {
            // `simple_value` rejects an empty value; fail closed to match.
            return (out, pair_start);
        }

        let value_start = i;
        if s[i..].starts_with('\'') {
            let Some(end) = quoted_value_end(s, i) else {
                // An unterminated quote swallows the rest of the string; the
                // caller must treat all of it as potentially secret.
                return (out, pair_start);
            };
            i = end;
        } else {
            i = simple_value_end(s, i);
        }

        out.push(KeywordPair {
            key,
            raw_value: &s[value_start..i],
        });
        i = skip_ws(s, i);
    }

    (out, s.len())
}

/// Hide the value of every `key=value` pair outside [`PRINTABLE_CONN_PARAMS`].
///
/// Keys stay visible: knowing *which* options were set is diagnostic, and a key
/// is not a secret. A token with no `=` at all is passed through — it carries no
/// value, and under a *correct* tokenizer it cannot be the tail of one.
///
/// Splits on `&` **only**, because that is the driver's own value terminator
/// (see [`DriverUrl`]); a value containing `#` is one value, not a value plus a
/// fragment. The allowlist is matched against the *decoded* key, so
/// `ho%73t=…` is recognised as `host` rather than hidden under a different
/// spelling.
fn redact_query_values(query: &str) -> String {
    query
        .split('&')
        .map(redact_one_token)
        .collect::<Vec<_>>()
        .join("&")
}

/// The keyword/value counterpart of [`redact_query_values`].
///
/// Rejoins as `key=value` separated by single spaces, so `password = 'x'`
/// normalises to `password=***`. That is a cosmetic change to a log line.
///
/// **Fails closed.** Anything the grammar cannot parse — an unterminated quote,
/// a stray `=` — is replaced wholesale rather than echoed, because an
/// unparseable remainder is exactly where a value would be hiding.
///
/// A string with no `=` anywhere carries no `key=value` secret, so it is
/// printed to keep a malformed-connection-string message diagnostic — *unless*
/// it contains an `@`. That carve-out is not hypothetical: a case variant of a
/// scheme, `POSTGRES://alice:hunter2@db/x`, is keyword form to the driver
/// (`remove_url_prefix` is byte-exact) and reaches this function, and its
/// secret is in userinfo rather than in a `key=value` pair. Everything through
/// the **last** `@` is dropped — more than userinfo strictly requires, which is
/// the right direction for a string whose grammar we have already failed to
/// parse — while the tail after it is kept, so the endpoint stays readable.
fn redact_keyword_values(params: &str) -> String {
    if !params.contains('=') {
        return params.rfind('@').map_or_else(
            || params.to_string(),
            |at| format!("***@{}", &params[at + 1..]),
        );
    }
    let (pairs, stopped) = keyword_value_pairs(params);
    let mut parts: Vec<String> = pairs
        .iter()
        .map(|p| {
            if PRINTABLE_CONN_PARAMS.contains(&p.key.trim().to_ascii_lowercase().as_str()) {
                format!("{}={}", p.key, p.raw_value)
            } else {
                format!("{}=***", p.key)
            }
        })
        .collect();
    if stopped < params.len() {
        parts.push("***".to_string());
    }
    parts.join(" ")
}

fn redact_one_token(token: &str) -> String {
    match token.split_once('=') {
        Some((key, _))
            if !PRINTABLE_CONN_PARAMS
                .contains(&decoded_query_key(token).to_ascii_lowercase().as_str()) =>
        {
            format!("{key}=***")
        }
        _ => token.to_string(),
    }
}

/// Strip credentials from a connection string so it is safe to print.
///
/// `HARVEST_TEST_DATABASE_URL` is an *admin* connection string and normally
/// carries a password. Both the benchmark and the CI gate print the
/// [`db::SkipReason`] they fail with, so an unredacted string in that message is
/// a credential leaked into a CI log that anyone with read access can scroll
/// back through.
///
/// Host, port and database are kept — those are what make the message
/// diagnostic. Three places can carry a secret, and all three are covered:
///
/// 1. **Userinfo**, `postgres://user:pw@host/db` — replaced wholesale.
/// 2. **Query parameters**, `postgres://host/db?user=u&password=pw`. This is a
///    valid form, not a curiosity: `tokio-postgres`'s `UrlParser` feeds every
///    query pair to the same `Config::param` that handles `password`, so it
///    authenticates exactly like userinfo does.
/// 3. **The libpq keyword/value form**, `host=h password=pw` — no `://` at all,
///    which an earlier revision returned verbatim. `Config::from_str` falls
///    through to this form when the URL parser declines, so it is equally live.
///
/// Values are redacted by allowlist (see [`PRINTABLE_CONN_PARAMS`]), so an
/// unrecognised parameter is hidden rather than printed on the assumption it is
/// harmless.
///
/// The drivers themselves were probed and do not echo their input:
/// `diesel`/`tokio-postgres` reported only `"error connecting to server"` /
/// `"invalid connection string"` across connection-refused, DNS-failure and
/// malformed-string cases. So our own interpolation is the only leak — but the
/// call site still passes the redacted form on principle, so a future driver
/// that starts echoing its input cannot silently reopen it.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let Some(parts) = split_driver_url(url) else {
        // No scheme: the libpq keyword/value form, space-separated.
        return redact_keyword_values(url);
    };

    let mut tail = String::new();
    if let Some(path) = parts.path {
        tail.push('/');
        tail.push_str(path);
    }
    if let Some(query) = parts.query {
        tail.push('?');
        tail.push_str(&redact_query_values(query));
    }

    // Credentials are whatever the driver itself would authenticate with, so
    // the whole span goes — a password holding `/`, `?` or a second `:` is
    // covered because the split already followed the driver's parse order.
    if parts.credentials.is_some() {
        format!("{}***@{}{tail}", parts.prefix, parts.host)
    } else {
        format!("{}{}{tail}", parts.prefix, parts.host)
    }
}

/// Rewrite a connection URL to point at a different database.
///
/// Replaces **only** the path component, preserving any query string. Naively
/// splicing at the last `/` breaks two real cases: it silently drops
/// `?sslmode=require` (so a server that requires TLS becomes unreachable via
/// the advertised existing-server mode), and when a parameter value itself
/// contains a slash — `?sslrootcert=/etc/ssl/root.crt` — it splices *inside the
/// query* and produces a malformed URL.
///
/// The authority is located by the driver's own rule — the first `/` or `?`,
/// with `#` deliberately *not* a terminator, because `UrlParser` has no
/// fragment concept (see [`DriverUrl`]).
///
/// # Why the query cannot simply be carried over
///
/// `dbname` is a legal *query* parameter as well as the path, and
/// `tokio-postgres` applies it second: `UrlParser::parse` calls `parse_path`
/// and then `parse_params`, both assigning the same `Config::dbname`. Carrying
/// `?dbname=admin` past a path rewrite therefore leaves the connection pointing
/// at the original admin database while the URL *reads* as though it targets
/// the new one — and this harness runs migrations and `TRUNCATE` through
/// exactly these rewritten strings. Any `dbname` parameter is dropped, so the
/// path is the single selector.
///
/// The match is against the **decoded** key: `parse_params` decodes before
/// dispatching to `Config::param`, so `?db%6Eame=admin` is a `dbname` override
/// that a raw comparison would carry straight through the rewrite.
///
/// The libpq keyword/value form has no path at all, so there the database is
/// selected by replacing `dbname` rather than by appending a path component —
/// and that grammar is *not* percent-encoded, so its keys are compared raw.
///
/// `db` is written verbatim, so it must already be in the form the driver will
/// read back — no percent escapes, since [`db_name_from_url`] (and the driver's
/// own `parse_path`) decodes them. Every name this harness generates is
/// `[a-z0-9_]` by construction — [`BENCH_DB_PREFIX`] plus a pid, a hex run
/// token and a counter — so the constraint is satisfied without an encoder that
/// would never run.
#[must_use]
pub fn with_db_name(url: &str, db: &str) -> String {
    // Keyword/value form (`host=h dbname=admin`): no `://`, no path.
    let Some(parts) = split_driver_url(url) else {
        let (pairs, stopped) = keyword_value_pairs(url);
        let mut out: Vec<String> = pairs
            .iter()
            .filter(|p| !p.key.eq_ignore_ascii_case("dbname"))
            .map(|p| format!("{}={}", p.key, p.raw_value))
            .collect();
        // Anything the parser could not consume is carried verbatim rather
        // than dropped: silently losing `sslmode=require` is the exact
        // unreachable-server failure this function exists to avoid. It is
        // carried *before* the replacement, so libpq's last-wins assignment
        // still resolves to our database even if the remainder hides a
        // `dbname` we could not parse out.
        let rest = url[stopped..].trim();
        if !rest.is_empty() {
            out.push(rest.to_string());
        }
        out.push(format!("dbname={db}"));
        return out.join(" ");
    };

    let kept: Vec<&str> = parts
        .query
        .into_iter()
        .flat_map(|q| q.split('&'))
        .filter(|pair| !pair.is_empty() && !decoded_query_key(pair).eq_ignore_ascii_case("dbname"))
        .collect();

    // Credentials are re-emitted verbatim, `@` included: this rewrites the
    // connection *target*, so dropping them would make the result unusable.
    let mut out = parts.credentials.map_or_else(
        || format!("{}{}/{db}", parts.prefix, parts.host),
        |creds| format!("{}{creds}@{}/{db}", parts.prefix, parts.host),
    );
    if !kept.is_empty() {
        out.push('?');
        out.push_str(&kept.join("&"));
    }
    out
}

/// Read the database name back out of a connection string.
///
/// The inverse of [`with_db_name`], and deliberately its neighbour: reading the
/// name back with a naive `rsplit('/')` reintroduces exactly the bug that
/// function exists to avoid — against `?sslrootcert=/etc/ssl/root.crt` the last
/// slash sits inside the *query*, so the "database name" comes back as
/// `root.crt`. Parse the path first, then stop at the query.
///
/// Handles **both** connection-string forms `libpq` (and so `tokio-postgres`)
/// accepts, because [`db::setup_bench_db`] hands back whichever form the
/// operator supplied in `HARVEST_TEST_DATABASE_URL` and the gate suite unwraps
/// this result — a `None` there is a panic, not a skip:
///
/// * **URL** (`postgres://host/db`): the regions come from
///   [`split_driver_url`], so this reads the *driver's* grammar rather than the
///   RFC's — there is no fragment, and keys and values are percent-decoded (see
///   [`decoded_query_key`]). A `dbname` *query* parameter wins over the path,
///   matching `tokio-postgres`' `UrlParser::parse`, which runs `parse_path`
///   before `parse_params`, so the later assignment survives. This is the same
///   precedence [`with_db_name`] honours by stripping the query parameter; the
///   two must agree or one could strip a `dbname` the other would not have
///   found.
/// * **keyword/value** (`host=h dbname=db`): the *last* `dbname` wins, matching
///   `Config::param`, which simply assigns on each occurrence. That grammar is
///   not percent-encoded, so the unquoted span is already the real name.
///
/// Returns an owned name because decoding (`%2F` and friends) produces bytes
/// that are not a slice of the input.
///
/// `None` only when the string names no database at all (`postgres://host`,
/// `host=h port=5432`).
#[must_use]
pub fn db_name_from_url(url: &str) -> Option<String> {
    // Keyword/value form: no `://`, so there is no path to read — the name can
    // only come from a `dbname` keyword.
    let Some(parts) = split_driver_url(url) else {
        let (pairs, _) = keyword_value_pairs(url);
        return pairs
            .iter()
            .rev()
            .find(|p| p.key.eq_ignore_ascii_case("dbname"))
            .map(|p| unquote_span(p.raw_value).to_string());
    };

    // A `dbname` query parameter overrides the path (see the doc comment). A
    // bare `dbname` with no `=` reads as empty, matching `parse_params`, which
    // stores whatever `take_until(&['&'])` yields.
    if let Some(pair) = parts
        .query
        .into_iter()
        .flat_map(|q| q.split('&'))
        .rfind(|pair| decoded_query_key(pair).eq_ignore_ascii_case("dbname"))
    {
        return Some(decode_or_raw(pair.split_once('=').map_or("", |(_, v)| v)));
    }

    parts.path.map(decode_or_raw)
}

/// Prefix of every database this harness creates against an admin URL.
///
/// Lives beside [`sweep_step`] rather than in [`db`] so the one function that
/// decides whether a name is ours owns the whole shape, and so that decision
/// stays testable with no server and no `db` feature.
pub const BENCH_DB_PREFIX: &str = "harvest_claim_bench_";

/// Width of the [`db::run_token`] field in a bench database name.
///
/// `format!("{:016x}", u64)` is always exactly this many lowercase hex digits.
const RUN_TOKEN_HEX_LEN: usize = 16;

/// What the stale-database sweep should do with one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepStep {
    /// Leave it alone without asking the server.
    Skip,
    /// Let the server-visible lease decide.
    AskServer,
}

/// Decide the fate of one candidate database, without touching a database.
///
/// Split out so the whole decision is testable on every OS, with no server and
/// no Docker. Exactly **one** thing stops the sweep here — a name this harness
/// did not mint, which is not ours to reclaim. Everything else defers to the
/// server-visible lease, the one authority that sees every client regardless of
/// host or PID namespace.
///
/// Two exemptions that earlier revisions carried are deliberately gone, both
/// retired by serialising the sweep, `CREATE DATABASE` and the lease under
/// [`SWEEP_LOCK_KEY`]:
///
/// * **No pid liveness veto.** It protected another local run inside its
///   create-to-lease window, which the lock now makes unobservable. In a
///   container it was actively harmful: the recorded pid is usually 1, pid 1 is
///   always alive, so every stale database a previous container run left behind
///   would be skipped forever.
/// * **No run-token self-exemption.** It was meant to cover the database we are
///   about to create — but the sweep runs *before* `CREATE DATABASE`, so that
///   database does not exist yet and was never a candidate. What the exemption
///   actually did was make a process unable to reclaim its **own** earlier
///   databases: `run_token()` is constant for the life of a process, so a
///   process calling [`db::setup_bench_db`] more than once (the benchmark, and
///   the gate suite) left one migrated database behind per call until some
///   other process happened to sweep. A prior database whose [`db::BenchDb`] is
///   still alive holds its lease and is skipped by the server check anyway, so
///   the lease already draws the line correctly — and draws it by *liveness*
///   rather than by *ownership*, which is the question that actually matters.
///
/// The run token still earns its keep in the database **name**, where it makes
/// two containerised runs on different hosts — both reporting pid 1 — collide
/// neither at `CREATE DATABASE` nor in each other's sweep.
///
/// # Why this takes the whole name
///
/// It used to take an already-parsed `(pid, token)` pair, which put the parse —
/// the part that decides whether a name is *ours at all* — outside everything
/// this function is tested by. The parse was correspondingly loose: it required
/// only a numeric first field and the mere presence of a second, so
/// `harvest_claim_bench_123_production` read as pid `123`, token `production`,
/// and a database nobody here minted reached `pg_terminate_backend` and
/// `DROP DATABASE`. Taking `datname` means the shape check *is* the tested
/// surface.
///
/// A name is ours only if it is exactly
/// `harvest_claim_bench_{pid}_{token}_{seq}` with:
///
/// * `pid` — decimal digits in `u32` range (from `std::process::id`),
/// * `token` — exactly [`RUN_TOKEN_HEX_LEN`] **lowercase** hex digits (from
///   `format!("{:016x}", …)`),
/// * `seq` — decimal digits in `u64` range (from an `AtomicU64` counter),
/// * and **nothing else**: no fourth component, no empty ones.
///
/// Anything else is somebody's database. The asymmetry here is the whole
/// argument for erring strict: refusing to reclaim one of ours leaks a
/// database, while reclaiming one of theirs destroys data, so every ambiguous
/// case resolves to [`SweepStep::Skip`].
#[must_use]
pub fn sweep_step(datname: &str) -> SweepStep {
    let Some(rest) = datname.strip_prefix(BENCH_DB_PREFIX) else {
        return SweepStep::Skip;
    };
    // Exactly three components. The trailing `None` is what rejects a longer
    // name: without it `harvest_claim_bench_1_<16 hex>_0_extra` would pass on
    // its first three fields alone.
    let mut parts = rest.split('_');
    let (Some(pid), Some(token), Some(seq), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return SweepStep::Skip;
    };
    if !is_canonical_decimal::<u32>(pid)
        || !is_run_token(token)
        || !is_canonical_decimal::<u64>(seq)
    {
        return SweepStep::Skip;
    }
    SweepStep::AskServer
}

/// A field whose text is exactly what `format!("{}", v: T)` would have printed.
///
/// The round-trip is the whole check, and it is deliberately stricter than
/// "digits that parse". `parse` is a *superset* of what we print: it accepts a
/// leading `+` and any number of leading zeros, so `+1`, `01`, and `0007` all
/// parse to values we could hold — while `{}` emits `1` and `7` and can never
/// produce those spellings. Comparing against `to_string` rejects every such
/// noncanonical form for free, without enumerating them.
///
/// This gates a `DROP DATABASE`, so "could parse to something we might hold" is
/// the wrong question. The right one is "is this byte-for-byte a name we would
/// have written", and only the round-trip asks it.
fn is_canonical_decimal<T: std::str::FromStr + std::fmt::Display>(s: &str) -> bool {
    s.parse::<T>().is_ok_and(|v| v.to_string() == s)
}

/// The decoy database name used to prove the sweep spares a foreign database.
///
/// Two properties, and they pull in opposite directions, which is why this is
/// one function rather than a `format!` at the call site:
///
/// 1. It must be a **candidate** — inside `BENCH_DB_PREFIX`, and shaped so the
///    two-field check of an earlier round would have accepted it. A decoy the
///    sweep never looks at proves nothing.
/// 2. It must **never** be a name this harness could mint, or the test would
///    assert that a real bench database survives the sweep — the exact opposite
///    of the contract, and a silent way to reintroduce the leak.
///
/// Rejection here is over-determined on purpose: `sweepprobe` can never be
/// 16 hex digits (it contains `s`, `w`, `p`, `r`, `o`), *and* the extra
/// components put the name past the exactly-three split. Either alone suffices.
///
/// `token` makes the name run-unique. The probe drops its decoy by name before
/// creating it, so without one, two runs sharing a cluster from different PID
/// namespaces — both pid 1 under containers — would delete each other's live
/// decoy and fail each other's survival assertion.
pub fn sweep_probe_decoy_name(pid: u32, token: &str) -> String {
    format!("{BENCH_DB_PREFIX}123_sweepprobe_{pid}_{token}")
}

/// Exactly 16 lowercase hex digits, the only shape [`db::run_token`] emits.
///
/// Uppercase is rejected for the same reason a fourth component is: we never
/// produce it, so a name carrying it is not ours.
fn is_run_token(s: &str) -> bool {
    s.len() == RUN_TOKEN_HEX_LEN
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Run one pre-measurement setup step under its own wall-clock ceiling.
///
/// `connect` and `seed` run *before* the measured phase and therefore before
/// its deadline exists, so neither was bounded by anything: a server that
/// accepted the TCP connection and then stopped answering would park the
/// bench — and the CI gate that calls it — until the outer workflow timeout,
/// while the page advertises a per-scenario cap. Every other await in these
/// runners is already bounded; these two were the hole.
///
/// Setup gets its *own* deadline rather than sharing the measured phase's.
/// A single ceiling spanning both would let a slow seed eat the measurement
/// window and report `truncated`, which says "the claim path is slow" about
/// a run whose claim path was never the problem. Two phases, two ceilings:
/// the worst case is bounded at twice the budget instead of unbounded, and
/// each number means what it says. The same [`scenario_time_budget`] value
/// is reused so that raising `HARVEST_BENCH_SCENARIO_SECS` for a slow
/// machine widens setup with it.
///
/// # Panics
///
/// Panics when the step does not finish in time. That is the same contract
/// the step already had — `connect` and `seed` both `expect` on failure —
/// and the right one here: with no seeded backlog there is nothing to
/// measure, so continuing would publish a number describing an empty table.
pub async fn with_setup_deadline<T>(
    what: &str,
    deadline: std::time::Instant,
    fut: impl std::future::Future<Output = T> + Send,
) -> T {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    (tokio::time::timeout(remaining, fut).await).unwrap_or_else(|_| {
        panic!(
            "benchmark setup stalled: {what} did not finish within {remaining:?}. \
             The database accepted the connection but stopped answering, so no \
             measurement is possible; the scenario is abandoned rather than \
             reporting a number taken against an unseeded table."
        )
    })
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
        BACKLOG_SWEEP, BudgetVerdict, ClaimGate, ClaimerOutcome, DEFAULT_SCENARIO_BUDGET_SECS,
        HEADLINE_P50_BUDGET_MS, LatencyStats, SweepStep, budget_from_str, budget_verdict,
        claims_for_claimer, db_name_from_url, headline_scenario, measured_claims_for,
        measured_window, percentile_ms, redact_url, sweep_probe_decoy_name, sweep_step,
        warmup_claims_for, with_db_name, with_setup_deadline,
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

    /// The throughput numerator must span the same window as its denominator.
    ///
    /// `wall_secs` starts at the first warmup call, so counting only
    /// post-warmup successes understated `claims/s` by the warmup fraction.
    #[test]
    fn throughput_numerator_counts_warmup_but_latency_does_not() {
        // 20 observations, all successful: warmup trims 2 (one in ten).
        let observed: Vec<(f64, bool)> = (0..20).map(|i| (f64::from(i), true)).collect();
        let out = ClaimerOutcome::from_observed(observed, false);

        assert_eq!(out.total_claimed, 20, "throughput counts every success");
        assert_eq!(out.claimed, 18, "latency window drops the warmup head");
        assert_eq!(out.samples.len(), 18, "one sample per post-warmup call");
        assert_eq!(out.empty, 0);
        // And the trim really is the *head*: sample 0 and 1 are gone.
        assert!(
            (out.samples[0] - 2.0).abs() < f64::EPSILON,
            "warmup must be trimmed from the front, got {:?}",
            out.samples.first(),
        );
    }

    /// The two counters must disagree only about warmup, never about outcome.
    #[test]
    fn throughput_numerator_ignores_calls_that_claimed_nothing() {
        // Alternating hit/miss over 20 calls: 10 successes, 2 trimmed as warmup
        // (indices 0 and 1 — one hit, one miss).
        let observed: Vec<(f64, bool)> = (0..20).map(|i| (f64::from(i), i % 2 == 0)).collect();
        let out = ClaimerOutcome::from_observed(observed, false);

        assert_eq!(out.total_claimed, 10, "every success, warmup included");
        assert_eq!(out.claimed, 9, "post-warmup successes");
        assert_eq!(out.empty, 9, "post-warmup misses");
        assert_eq!(
            out.claimed + out.empty,
            out.samples.len(),
            "claim_ratio's denominator must match the sample count",
        );
    }

    /// A claimer that never got a connection reports nothing, but truthfully.
    #[test]
    fn a_claimer_with_no_observations_reports_zero_not_a_panic() {
        let out = ClaimerOutcome::from_observed(Vec::new(), true);
        assert_eq!(out.total_claimed, 0);
        assert_eq!(out.claimed, 0);
        assert_eq!(out.empty, 0);
        assert!(out.samples.is_empty());
        assert!(out.truncated, "truncation must survive the split");
    }

    /// Short runs discard nothing, so a truncated scenario still reports.
    #[test]
    fn a_short_run_keeps_every_observation_in_both_windows() {
        let observed: Vec<(f64, bool)> = (0..9).map(|i| (f64::from(i), true)).collect();
        let out = ClaimerOutcome::from_observed(observed, true);
        assert_eq!(warmup_claims_for(9), 0, "fewer than ten discards nothing");
        assert_eq!(out.total_claimed, 9);
        assert_eq!(out.claimed, 9);
        assert_eq!(out.samples.len(), 9);
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
    fn claimer_split_is_exact_so_the_drain_bound_actually_holds() {
        // The invariant above is about the *planned* total; this one is about
        // what the claimers between them actually execute. A naive
        // `(total / claimers).max(1)` per claimer overshoots whenever there are
        // more claimers than operations, which is how a 20-row backlog ends up
        // 40% drained.
        for (total, claimers) in [(800, 8), (800, 7), (4, 8), (0, 8), (1, 1), (13, 5)] {
            let sum: usize = (0..claimers)
                .map(|i| claims_for_claimer(total, claimers, i))
                .sum();
            assert_eq!(
                sum, total,
                "total {total} across {claimers} claimers must be distributed exactly",
            );
        }
        // Fewer operations than claimers: the surplus claimers do nothing rather
        // than each being floored up to one and blowing the bound.
        assert_eq!(claims_for_claimer(4, 8, 3), 1);
        assert_eq!(claims_for_claimer(4, 8, 4), 0);
        // Degenerate input must not divide by zero.
        assert_eq!(claims_for_claimer(10, 0, 0), 0);
    }

    #[test]
    fn wall_clock_budget_admits_a_run_at_the_latency_budget() {
        // The gate asserts both "p99 within budget" AND "the run was not
        // truncated". Those two assertions are only coherent if a run sitting
        // exactly at the p99 budget can finish inside the wall-clock ceiling.
        // If it cannot, the wall clock silently becomes the real budget: a
        // slow-but-passing run trips the truncation assertion first and the
        // gate reports "measurement unsound" instead of a latency verdict.
        let s = headline_scenario();
        let total = measured_claims_for(s.backlog);
        let per_claimer = claims_for_claimer(total, s.claimers, 0);

        #[allow(clippy::cast_precision_loss)]
        let worst_case_secs = (per_claimer as f64) * HEADLINE_P50_BUDGET_MS / 1000.0;
        #[allow(clippy::cast_precision_loss)]
        let ceiling_secs = DEFAULT_SCENARIO_BUDGET_SECS as f64;

        assert!(
            ceiling_secs >= worst_case_secs,
            "incoherent budgets: a claimer performing {per_claimer} ops at the \
             {HEADLINE_P50_BUDGET_MS}ms p99 budget needs {worst_case_secs:.0}s, but the \
             wall-clock ceiling is {ceiling_secs:.0}s. The gate would report \
             truncation instead of the latency verdict. Raise \
             DEFAULT_SCENARIO_BUDGET_SECS or lower HEADLINE_P50_BUDGET_MS.",
        );
    }

    #[test]
    fn redact_url_removes_the_password_but_keeps_the_endpoint() {
        // The whole point: this string ends up in a CI log.
        let redacted = redact_url("postgres://alice:hunter2@db.internal:5432/postgres");
        assert!(
            !redacted.contains("hunter2") && !redacted.contains("alice"),
            "userinfo survived redaction: {redacted}",
        );
        // Still diagnostic — an operator must be able to tell *which* server.
        assert!(
            redacted.contains("db.internal:5432") && redacted.contains("postgres"),
            "redaction destroyed the endpoint: {redacted}",
        );
    }

    #[test]
    fn redact_url_leaves_a_credential_free_url_alone() {
        for url in [
            "postgres://localhost:5432/postgres",
            "postgres://db.internal/harvest?sslmode=require",
            "not-a-url",
        ] {
            assert_eq!(redact_url(url), url, "rewrote a URL with no userinfo");
        }
    }

    #[test]
    fn redact_url_handles_a_password_containing_an_at_sign() {
        // Percent-encoding is the correct spelling, but an operator who pastes
        // a raw '@' must still not have it echoed. The *last* '@' in the
        // authority is the delimiter.
        let redacted = redact_url("postgres://u:p@ss@host:5432/db");
        assert!(!redacted.contains("p@ss"), "password survived: {redacted}");
        assert!(
            redacted.contains("host:5432/db"),
            "endpoint lost: {redacted}"
        );
    }

    /// A password in the *query* authenticates exactly like one in userinfo.
    ///
    /// `tokio-postgres`'s URL parser feeds every query pair to the same
    /// `Config::param` that handles `password`, so redacting only the authority
    /// leaves a fully-working credential in the log line.
    #[test]
    fn redact_url_removes_a_password_given_as_a_query_parameter() {
        let redacted =
            redact_url("postgres://db.internal:5432/harvest?user=alice&password=hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(!redacted.contains("alice"), "username survived: {redacted}");
        assert!(
            redacted.contains("db.internal:5432/harvest"),
            "endpoint lost: {redacted}"
        );
    }

    /// A `#` inside a query value is part of that value, not a fragment.
    ///
    /// `UrlParser::parse_params` terminates a value with `take_until(&['&'])`
    /// and the driver has no fragment concept anywhere, so `password=#hunter2`
    /// authenticates with the literal `#hunter2`. Splitting the URL at `#` and
    /// appending the tail unredacted therefore prints most of a live password
    /// on the one line — a connection failure — that operators paste around.
    #[test]
    fn redact_url_removes_a_password_containing_a_hash() {
        let redacted = redact_url("postgres://db.internal:5432/harvest?password=#hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(
            redacted.contains("db.internal:5432/harvest"),
            "endpoint lost: {redacted}"
        );
    }

    /// A percent-encoded key names the same parameter to the driver.
    ///
    /// `parse_params` runs `self.decode(key)?` before `Config::param`, so
    /// `pass%77ord` *is* `password`. Comparing the raw key against the
    /// printable allowlist would still redact it (anything unrecognised is
    /// redacted), but the allowlist has to be consulted on the decoded key or
    /// the reverse mistake — treating an encoded `sslmode` as unknown — becomes
    /// the same class of bug in the other direction.
    #[test]
    fn redact_url_removes_a_percent_encoded_password_parameter() {
        let redacted = redact_url("postgres://db.internal:5432/harvest?pass%77ord=hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        // An encoded *shape* parameter is recognised, so it stays readable.
        let shown = redact_url("postgres://db.internal:5432/harvest?sslmo%64e=require");
        assert!(
            shown.contains("require"),
            "recognised parameter was hidden: {shown}"
        );
    }

    /// The measured window must span every worker, not the widest single one.
    ///
    /// A worker that resumes late still does all of its work, so charging only
    /// its own `finish - resume` to the denominator drops the release-to-resume
    /// delay while keeping the numerator whole — the throughput figure then
    /// reads higher than the run actually achieved. Here two workers each run
    /// for 1 s but the second resumes 2 s late, so the true window is 3 s while
    /// the per-worker maximum is 1 s.
    #[test]
    fn measured_window_spans_all_workers_not_the_widest_one() {
        let base = std::time::Instant::now();
        let s = |ms: u64| base + std::time::Duration::from_millis(ms);
        let spans = [(s(0), s(1_000)), (s(2_000), s(3_000))];

        let window = measured_window(&spans);
        assert!(
            (window - 3.0).abs() < 1e-6,
            "window must span the earliest resume to the last finish, got {window}"
        );

        // The bug this replaced: the widest single worker span, which here is
        // 1 s and would report three times the real throughput.
        let widest = spans
            .iter()
            .map(|&(a, b)| b.duration_since(a).as_secs_f64())
            .fold(0.0_f64, f64::max);
        assert!(
            window > widest,
            "window {window} must exceed the widest single span {widest}"
        );
    }

    /// Degenerate inputs stay non-negative and finite.
    ///
    /// An empty slice means nothing was measured, and the callers' existing
    /// `wall_secs <= 0.0` guards already read `0.0` as "no measurement".
    #[test]
    fn measured_window_handles_empty_and_out_of_order_spans() {
        assert!((measured_window(&[]) - 0.0).abs() < f64::EPSILON);

        let base = std::time::Instant::now();
        let s = |ms: u64| base + std::time::Duration::from_millis(ms);
        // A worker that finished before the earliest resume cannot make the
        // window negative (which would flip the throughput sign).
        let window = measured_window(&[(s(500), s(100))]);
        assert!(window >= 0.0, "window went negative: {window}");
    }

    /// `://` inside a keyword value does not make the string a URL.
    ///
    /// `UrlParser::remove_url_prefix` strips `postgres://` / `postgresql://`
    /// with `strip_prefix` and otherwise declines, so `Config::from_str` hands
    /// the whole string to the keyword parser. Searching for `://` anywhere
    /// instead reads `password=http://hunter2` as a scheme, which leaves the
    /// password on the far side of a split that redacts nothing.
    #[test]
    fn redact_url_treats_a_scheme_inside_a_value_as_keyword_form() {
        let redacted = redact_url("host=db password=http://hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(redacted.contains("host=db"), "endpoint lost: {redacted}");
    }

    /// The same misclassification corrupts a rewritten target.
    ///
    /// Split at the `://` inside `password=http://hunter2`, everything before
    /// it becomes an opaque "prefix" that is re-emitted untouched — so the real
    /// `dbname=admin` survives and `/bench` is appended to the password,
    /// producing a string that is no longer a connection string in either form.
    /// Read as keyword form, `dbname` is replaced and every other pair is kept.
    #[test]
    fn with_db_name_treats_a_scheme_inside_a_value_as_keyword_form() {
        let rewritten = with_db_name("host=db dbname=admin password=http://hunter2", "bench");
        assert!(
            rewritten.contains("dbname=bench"),
            "target not rewritten: {rewritten}"
        );
        assert!(
            !rewritten.contains("dbname=admin"),
            "old dbname survived: {rewritten}"
        );
        assert!(
            rewritten.contains("host=db") && rewritten.contains("password=http://hunter2"),
            "sibling parameters lost: {rewritten}"
        );
    }

    /// A case-variant scheme is keyword form too — and still must not leak.
    ///
    /// `remove_url_prefix` is byte-exact, so `POSTGRES://` is *not* a URL to the
    /// driver. Such a string reaches the keyword redactor, where it has no `=`
    /// at all: the passthrough that keeps a malformed-string message diagnostic
    /// would otherwise print its userinfo verbatim. The endpoint after the last
    /// `@` is still shown.
    #[test]
    fn redact_url_hides_userinfo_in_an_unparseable_scheme_variant() {
        let redacted = redact_url("POSTGRES://alice:hunter2@db.internal/x");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(!redacted.contains("alice"), "username survived: {redacted}");
        assert!(
            redacted.contains("db.internal/x"),
            "endpoint lost: {redacted}"
        );

        // A genuinely harmless string with no `=` and no `@` still prints, so
        // the diagnostic value of the passthrough is not lost wholesale.
        assert_eq!(redact_url("localhost"), "localhost");
    }

    /// A password containing `/` or `?` still ends at the first `@`.
    ///
    /// `UrlParser::parse` runs `parse_credentials` *before* `parse_host`, and
    /// credentials are `take_until(&['@'])` over the whole remaining string —
    /// no `/` or `?` terminator. So `postgres://alice:sec/ret@db/postgres`
    /// authenticates as `alice`/`sec/ret` against host `db`. Splitting on the
    /// RFC's authority instead stops at the `/`, finds no `@` in `alice:sec`,
    /// and prints the entire credential on a connection-failure line.
    #[test]
    fn redact_url_removes_a_password_containing_a_path_separator() {
        for url in [
            "postgres://alice:sec/ret@db.internal:5432/postgres",
            "postgres://alice:sec?ret@db.internal:5432/postgres",
        ] {
            let redacted = redact_url(url);
            assert!(!redacted.contains("sec"), "password survived: {redacted}");
            assert!(!redacted.contains("alice"), "username survived: {redacted}");
            assert!(
                redacted.contains("db.internal:5432"),
                "endpoint lost: {redacted}"
            );
        }
    }

    /// The same split has to keep the URL *usable*, not just safe.
    ///
    /// `with_db_name` rewrites the connection target, so a credential holding
    /// `/` must be re-emitted verbatim — dropping or truncating it produces a
    /// string that no longer authenticates.
    #[test]
    fn with_db_name_preserves_credentials_containing_a_path_separator() {
        let rewritten = with_db_name(
            "postgres://alice:sec/ret@db.internal:5432/postgres",
            "bench",
        );
        assert_eq!(rewritten, "postgres://alice:sec/ret@db.internal:5432/bench");
    }

    /// `tokio-postgres` separates keyword pairs on `char::is_whitespace`.
    ///
    /// `Parser::skip_ws` is `take_while(char::is_whitespace)` and both value
    /// readers break on `c.is_whitespace()` — all Unicode, not ASCII. So a
    /// no-break space really does end the `host` value and start a new pair,
    /// and an ASCII-only tokenizer folds the following `password=...` into the
    /// allowlisted `host` value, which is then printed in full.
    #[test]
    fn redact_url_splits_keyword_pairs_on_unicode_whitespace() {
        let redacted = redact_url("host=db.internal\u{00A0}password=hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(
            redacted.contains("db.internal"),
            "endpoint lost: {redacted}"
        );
    }

    /// libpq lets a keyword value be quoted, so it may contain spaces.
    ///
    /// Splitting the form on whitespace therefore cuts `password='hunter two'`
    /// in half: the first token redacts, and the second has no `=` in it, so it
    /// is passed through as though it were a separator and the tail of the
    /// password is printed. Round 13 advertised support for this form without
    /// implementing its value grammar, which is the narrow gap this pins.
    #[test]
    fn redact_url_removes_a_quoted_keyword_password_containing_spaces() {
        let redacted = redact_url("host=db.internal password='hunter two' sslmode=require");
        assert!(
            !redacted.contains("hunter"),
            "password survived: {redacted}"
        );
        assert!(
            !redacted.contains("two"),
            "password tail survived: {redacted}"
        );
        // Still diagnostic on both sides of the quoted value, which is the
        // whole reason for parsing rather than dropping the form wholesale.
        assert!(
            redacted.contains("host=db.internal") && redacted.contains("sslmode=require"),
            "endpoint lost: {redacted}"
        );
    }

    /// libpq allows whitespace around the `=`, which splits a naive tokenizer.
    ///
    /// `Parser::keyword` stops at whitespace *or* `=`, then `skip_ws`/`eat('=')`
    /// /`skip_ws` runs before the value — so `password = 'hunter two'` is a
    /// valid string that a whitespace tokenizer shreds into `password`, `=` and
    /// the bare secret. The secret token has no embedded `=`, so the redactor's
    /// pass-through rule printed it in full.
    #[test]
    fn redact_url_removes_a_keyword_password_with_spaces_around_equals() {
        let redacted = redact_url("host=db.internal password = 'hunter two' sslmode=require");
        assert!(
            !redacted.contains("hunter"),
            "password survived: {redacted}"
        );
        assert!(
            !redacted.contains("two"),
            "password tail survived: {redacted}"
        );
        assert!(
            redacted.contains("host=db.internal") && redacted.contains("sslmode=require"),
            "endpoint lost: {redacted}"
        );
    }

    /// Anything the keyword grammar cannot parse is hidden, not printed.
    #[test]
    fn redact_url_fails_closed_on_an_unparseable_keyword_remainder() {
        // An unterminated quote: everything after it is unclassifiable, so it
        // must not be echoed on the assumption it is harmless.
        let redacted = redact_url("host=h password='hunter two sslmode=require");
        assert!(
            !redacted.contains("hunter"),
            "password survived: {redacted}"
        );
        assert!(redacted.contains("host=h"), "endpoint lost: {redacted}");
    }

    /// A backslash-escaped quote keeps the value open past the next `'`.
    #[test]
    fn redact_url_removes_a_keyword_password_containing_an_escaped_quote() {
        let redacted = redact_url(r"host=h password='hun\'ter two' sslmode=require");
        assert!(!redacted.contains("hun"), "password survived: {redacted}");
        assert!(
            !redacted.contains("two"),
            "password tail survived: {redacted}"
        );
        assert!(
            redacted.contains("sslmode=require"),
            "endpoint lost: {redacted}"
        );
    }

    /// The keyword/value form carries the same secret and has no `://` at all.
    ///
    /// `Config::from_str` falls through to it when the URL parser declines, so
    /// an operator can legitimately set `HARVEST_TEST_DATABASE_URL` this way.
    #[test]
    fn redact_url_removes_a_password_from_the_keyword_value_form() {
        let redacted = redact_url("host=db.internal port=5432 dbname=harvest password=hunter2");
        assert!(
            !redacted.contains("hunter2"),
            "password survived: {redacted}"
        );
        assert!(
            redacted.contains("host=db.internal") && redacted.contains("dbname=harvest"),
            "endpoint lost: {redacted}"
        );
    }

    /// Unknown parameters are hidden, known-shape ones are not.
    ///
    /// The allowlist is the whole point: a parameter nobody anticipated must
    /// default to hidden, or every future libpq addition is a potential leak.
    #[test]
    fn redact_url_hides_unknown_parameters_and_keeps_shape_ones() {
        let redacted =
            redact_url("postgres://h/db?sslmode=require&sslpassword=k3y&options=-csearch_path%3Dx");
        assert!(
            !redacted.contains("k3y"),
            "sslpassword survived: {redacted}"
        );
        assert!(
            !redacted.contains("search_path"),
            "options survived: {redacted}"
        );
        assert!(
            redacted.contains("sslmode=require"),
            "a shape parameter was needlessly hidden: {redacted}"
        );
        // Keys stay visible — knowing an option was set is diagnostic, and a
        // key is not a secret.
        assert!(
            redacted.contains("sslpassword=***"),
            "the key should survive redaction: {redacted}"
        );
    }

    /// A process must be able to reclaim its **own** earlier databases.
    ///
    /// `run_token()` is constant for the life of a process, so a self-exemption
    /// keyed on it made every process leak one migrated database per
    /// `setup_bench_db` call — the benchmark alone calls it a dozen times. The
    /// exemption bought nothing: the sweep runs *before* `CREATE DATABASE`, so
    /// the database being created is not a candidate, and a prior database
    /// still in use holds its lease and is skipped by the server check.
    #[test]
    fn sweep_step_lets_the_lease_decide_for_our_own_earlier_databases() {
        assert_eq!(
            sweep_step("harvest_claim_bench_1_0123456789abcdef_0"),
            SweepStep::AskServer,
            "our own token must not exempt a database from the lease check",
        );
        assert_eq!(
            sweep_step(&format!(
                "harvest_claim_bench_{}_0123456789abcdef_7",
                std::process::id()
            )),
            SweepStep::AskServer,
        );
    }

    /// A foreign run's database is always the server's call.
    ///
    /// The pid in the name is only meaningful on the host that minted it, so it
    /// can never answer "is this in use"; only the server sees every client.
    #[test]
    fn sweep_step_asks_the_server_for_a_foreign_run() {
        assert_eq!(
            sweep_step("harvest_claim_bench_4242_fedcba9876543210_3"),
            SweepStep::AskServer,
        );
    }

    /// A live same-host pid must NOT veto the server's judgement.
    #[test]
    fn sweep_step_never_vetoes_on_a_live_local_pid() {
        // `1` is alive on every containerised host, and a stale database from a
        // previous container run records exactly that pid. A liveness veto here
        // would skip it forever; the advisory lock already covers the only
        // window such a veto ever protected.
        assert_eq!(
            sweep_step("harvest_claim_bench_1_fedcba9876543210_0"),
            SweepStep::AskServer,
        );
        assert_eq!(
            sweep_step(&format!(
                "harvest_claim_bench_{}_fedcba9876543210_0",
                std::process::id()
            )),
            SweepStep::AskServer,
        );
    }

    /// A name this harness did not mint is never reclaimed.
    ///
    /// This is the only thing standing between a same-prefix database belonging
    /// to somebody else and `pg_terminate_backend` + `DROP DATABASE`, so it
    /// checks the *whole* shape rather than the first field or two. Each case
    /// below is a name that a looser check would have handed to the sweep.
    #[test]
    fn sweep_step_leaves_names_we_did_not_mint_alone() {
        for name in [
            // The reported case: prefix plus a numeric field and *anything*.
            // Under a two-field check this parsed as pid 123 / token
            // "production" and was dropped.
            "harvest_claim_bench_123_production",
            // Right arity, but the token is not a run token.
            "harvest_claim_bench_123_production_0",
            // Token of the right length that is not hex.
            "harvest_claim_bench_1_zzzzzzzzzzzzzzzz_0",
            // Hex, but not the width `{:016x}` emits.
            "harvest_claim_bench_1_0123456789abcde_0",
            "harvest_claim_bench_1_0123456789abcdef0_0",
            // Uppercase hex: we only ever print lowercase.
            "harvest_claim_bench_1_0123456789ABCDEF_0",
            // A fourth component — a suffix appended to one of ours.
            "harvest_claim_bench_1_0123456789abcdef_0_backup",
            // Missing the sequence entirely.
            "harvest_claim_bench_1_0123456789abcdef",
            // Non-numeric pid, and a non-numeric sequence.
            "harvest_claim_bench_web_0123456789abcdef_0",
            "harvest_claim_bench_1_0123456789abcdef_final",
            // Empty components (a doubled separator).
            "harvest_claim_bench_1__0",
            "harvest_claim_bench__0123456789abcdef_0",
            // Signed/whitespace forms `parse` alone would have accepted.
            "harvest_claim_bench_+1_0123456789abcdef_0",
            // Noncanonical decimal: `parse` accepts a leading zero, but we
            // print with `{}`, which never emits one. A digits-and-parse check
            // takes these; only comparing against the canonical rendering
            // rejects them.
            "harvest_claim_bench_01_0123456789abcdef_0",
            "harvest_claim_bench_007_0123456789abcdef_0",
            "harvest_claim_bench_1_0123456789abcdef_00",
            "harvest_claim_bench_1_0123456789abcdef_0007",
            // Numeric but out of range for the types we mint from.
            "harvest_claim_bench_4294967296_0123456789abcdef_0",
            // The bare prefix, and a database that merely starts like one.
            "harvest_claim_bench_",
            "harvest_claim_bench_backup",
            // No prefix at all. `_` is a single-character wildcard in `LIKE`,
            // so the SQL prefilter really can hand us names like this.
            "harvestXclaimXbenchX1_0123456789abcdef_0",
            "production",
        ] {
            assert_eq!(
                sweep_step(name),
                SweepStep::Skip,
                "{name} is not a name this harness mints, so it must never reach DROP DATABASE",
            );
        }
    }

    /// The shape accepted is exactly the shape minted.
    ///
    /// Built from the same pieces `setup_bench_db` formats its name from, so a
    /// change to either side that the other does not follow fails here rather
    /// than by silently leaking databases nothing will ever reclaim.
    #[test]
    fn sweep_step_accepts_a_freshly_minted_name() {
        let minted = format!(
            "{}{}_{:016x}_{}",
            super::BENCH_DB_PREFIX,
            std::process::id(),
            u64::MAX,
            u64::MAX,
        );
        assert_eq!(sweep_step(&minted), SweepStep::AskServer, "{minted}");
    }

    /// The decoy is a sweep *candidate* that is never a *mintable* name.
    ///
    /// Both halves are load-bearing and neither is obvious from the format
    /// string, so they are pinned here rather than only inside the DB test that
    /// uses them — this runs on every OS, including the `--no-default-features`
    /// leg where no database exists. If the decoy stopped being a candidate the
    /// DB test would pass vacuously; if it started being mintable, the test
    /// would assert that a *real* bench database survives the sweep.
    #[test]
    fn the_sweep_probe_decoy_is_a_candidate_but_never_mintable() {
        let decoy = sweep_probe_decoy_name(1, "0123456789abcdef");
        assert!(
            decoy.starts_with(super::BENCH_DB_PREFIX),
            "the decoy must be inside the swept prefix or the sweep never \
             considers it and the test proves nothing: {decoy}",
        );
        assert_eq!(
            sweep_step(&decoy),
            SweepStep::Skip,
            "the decoy must never parse as a name this harness mints: {decoy}",
        );
        // Distinct runs get distinct decoys — the whole point of the token.
        assert_ne!(
            sweep_probe_decoy_name(1, "0123456789abcdef"),
            sweep_probe_decoy_name(1, "fedcba9876543210"),
        );
    }

    /// The inverse of `with_db_name` must not reintroduce the last-slash bug.
    ///
    /// A slash inside a query value is the exact case that makes `rsplit('/')`
    /// return a certificate filename instead of the database.
    #[test]
    fn db_name_from_url_reads_the_path_not_the_last_slash() {
        assert_eq!(
            db_name_from_url("postgres://u:p@h:5432/bench_1").as_deref(),
            Some("bench_1")
        );
        assert_eq!(
            db_name_from_url("postgres://u:p@h:5432/bench_1?sslmode=require").as_deref(),
            Some("bench_1")
        );
        assert_eq!(
            db_name_from_url("postgres://u:p@h:5432/bench_1?sslrootcert=/etc/ssl/root.crt")
                .as_deref(),
            Some("bench_1"),
        );
    }

    /// `#` is an ordinary character to the driver: `parse_path` runs
    /// `take_until(&['?'])`, so with no query it takes the rest of the string.
    ///
    /// The RFC would call `#frag` a fragment and stop before it. Reading it that
    /// way would have this harness look for a database the driver never asked
    /// for, so the *driver's* grammar is the one that matters.
    #[test]
    fn db_name_from_url_keeps_a_hash_in_the_path() {
        assert_eq!(
            db_name_from_url("postgres://u:p@h:5432/bench_1#frag").as_deref(),
            Some("bench_1#frag")
        );
    }

    /// No path component at all, so there is no name to report.
    #[test]
    fn db_name_from_url_is_none_without_a_path() {
        assert_eq!(db_name_from_url("postgres://host"), None);
        assert_eq!(db_name_from_url("postgres://host?sslmode=require"), None);
    }

    /// Round-trips with the writer it is the inverse of, including the awkward
    /// URLs that motivated both functions.
    #[test]
    fn db_name_from_url_round_trips_with_with_db_name() {
        for base in [
            "postgres://u:p@h:5432/postgres",
            "postgres://u:p@h:5432/postgres?sslmode=require",
            "postgres://u:p@h:5432/postgres?sslrootcert=/etc/ssl/root.crt",
            "postgres://host",
        ] {
            let rewritten = with_db_name(base, "harvest_claim_bench_1_deadbeefdeadbeef_0");
            assert_eq!(
                db_name_from_url(&rewritten).as_deref(),
                Some("harvest_claim_bench_1_deadbeefdeadbeef_0"),
                "round trip failed for {base}",
            );
        }
    }

    #[test]
    fn with_db_name_preserves_query_parameters() {
        // Dropping `?sslmode=require` makes a TLS-requiring server unreachable
        // through the advertised existing-server mode.
        assert_eq!(
            with_db_name("postgres://u:p@h:5432/postgres?sslmode=require", "bench_1"),
            "postgres://u:p@h:5432/bench_1?sslmode=require",
        );
    }

    #[test]
    fn with_db_name_does_not_splice_inside_a_query_value() {
        // `rfind('/')` lands in the middle of the certificate path and yields a
        // malformed URL.
        assert_eq!(
            with_db_name(
                "postgres://h:5432/postgres?sslrootcert=/etc/ssl/root.crt",
                "bench_1",
            ),
            "postgres://h:5432/bench_1?sslrootcert=/etc/ssl/root.crt",
        );
    }

    /// A `dbname` query parameter silently outranks the path we just rewrote.
    ///
    /// `tokio-postgres`'s `UrlParser::parse` runs `parse_path` (`config.rs:997`)
    /// and *then* `parse_params` (`:998`), and both assign the same
    /// `Config::dbname` field — so the query wins. Carrying the query over
    /// verbatim, which round 3 introduced to keep `sslmode` alive, therefore
    /// pointed the sweep lock, the lease and the migration connection back at
    /// the operator's admin database. The harness runs `TRUNCATE` there.
    #[test]
    fn with_db_name_removes_an_overriding_dbname_query_parameter() {
        assert_eq!(
            with_db_name("postgres://h:5432/admin?dbname=admin&sslmode=require", "b"),
            "postgres://h:5432/b?sslmode=require",
        );
        // Case-insensitive, and the sole-parameter case must not leave a `?`.
        assert_eq!(
            with_db_name("postgres://h:5432/admin?DBName=admin", "b"),
            "postgres://h:5432/b",
        );
        // No fragment concept: the driver's `parse_params` terminates a value
        // only at `&`, so `admin#frag` is the whole `dbname` value and the
        // whole thing is what gets dropped.
        assert_eq!(
            with_db_name("postgres://h/admin?dbname=admin#frag", "b"),
            "postgres://h/b",
        );
    }

    /// The overriding parameter can arrive percent-encoded, and still overrides.
    ///
    /// `parse_params` decodes the key (`self.decode(key)?`) before dispatching,
    /// so `db%6Eame=admin` reaches `Config::dbname` exactly like `dbname=admin`
    /// would. A raw-text filter leaves it in place, and then every connection
    /// this harness opens — sweep lock, lease, migrations, and the benchmark's
    /// own `TRUNCATE` — lands on the operator's admin database instead of the
    /// isolated one. That is the same P1 as the plain-text case above, reached
    /// through a spelling the previous filter could not see.
    #[test]
    fn with_db_name_removes_a_percent_encoded_dbname_query_parameter() {
        assert_eq!(
            with_db_name(
                "postgres://h:5432/admin?db%6Eame=admin&sslmode=require",
                "b"
            ),
            "postgres://h:5432/b?sslmode=require",
        );
        // The reader must agree with the writer about which key counts, or one
        // would strip a `dbname` the other would not have found.
        assert_eq!(
            db_name_from_url("postgres://h:5432/admin?db%6Eame=real").as_deref(),
            Some("real"),
        );
    }

    /// A percent-encoded database name decodes to the name the driver opens.
    ///
    /// `parse_path` ends in `self.config.dbname(self.decode(dbname)?)`, so
    /// `%2F` is a slash *in the name*, not a path separator. Reporting the raw
    /// span would have the lease query look up a database that does not exist.
    #[test]
    fn db_name_from_url_decodes_percent_escapes() {
        assert_eq!(
            db_name_from_url("postgres://h/bench%5Fone").as_deref(),
            Some("bench_one"),
        );
        assert_eq!(
            db_name_from_url("postgres://h/admin?dbname=bench%5Ftwo").as_deref(),
            Some("bench_two"),
        );
        // Undecodable bytes fall back to the raw span: the driver rejects such
        // a string outright, so there is no connection to disagree with.
        assert_eq!(
            db_name_from_url("postgres://h/bench%FF").as_deref(),
            Some("bench%FF"),
        );
    }

    /// `db_name_from_url` must invert `with_db_name` for **both** forms.
    ///
    /// Round 14 taught `with_db_name` to emit a proper keyword/value string,
    /// which made its inverse return `None` for exactly the strings it now
    /// produces — and both existing-server tests `.expect()` that value, so the
    /// documented gate suite panicked for a supported connection-string form.
    /// (Before round 14 the rewrite emitted a stray `/`, which this parser
    /// happened to read as a path; the inverse worked only by accident.)
    #[test]
    fn db_name_from_url_reads_the_keyword_value_form() {
        assert_eq!(
            db_name_from_url("host=h port=5432 dbname=harvest_claim_bench_1_abc_0").as_deref(),
            Some("harvest_claim_bench_1_abc_0"),
        );
        // Round-trips with what `with_db_name` emits, which is the property the
        // gate suite actually depends on.
        let rewritten = with_db_name("host=h dbname=admin", "bench_1");
        assert_eq!(db_name_from_url(&rewritten).as_deref(), Some("bench_1"));
        // libpq is last-wins on a repeated keyword.
        assert_eq!(
            db_name_from_url("dbname=first dbname=second").as_deref(),
            Some("second")
        );
        // A quoted value is returned without its quotes. Percent escapes are
        // *not* decoded here: that grammar has no percent-encoding, so `%41`
        // names a database literally called `%41`.
        assert_eq!(
            db_name_from_url("host=h dbname='b 1'").as_deref(),
            Some("b 1")
        );
        assert_eq!(
            db_name_from_url("host=h dbname=%41").as_deref(),
            Some("%41")
        );
        // Genuinely absent stays `None`.
        assert_eq!(db_name_from_url("host=h port=5432"), None);
    }

    /// The URL form must read `dbname` with the same precedence `with_db_name`
    /// strips it: query over path.
    ///
    /// Round 14 established that `tokio-postgres` runs `parse_path` *before*
    /// `parse_params`, so `?dbname=` wins — which is why `with_db_name` drops
    /// it. If the inverse still read only the path, the two neighbours would
    /// disagree about which parameter counts as the database for the same
    /// string, and this reader would name a database the connection never
    /// opens.
    #[test]
    fn db_name_from_url_lets_a_dbname_query_override_the_path() {
        assert_eq!(
            db_name_from_url("postgres://h/admin?dbname=real").as_deref(),
            Some("real"),
        );
        // Case-insensitive. `#frag` is part of the value, not a fragment.
        assert_eq!(
            db_name_from_url("postgres://h/admin?sslmode=require&DBName=real#frag").as_deref(),
            Some("real#frag"),
        );
        // Last-wins, matching repeated `parse_params` assignment.
        assert_eq!(
            db_name_from_url("postgres://h/admin?dbname=first&dbname=second").as_deref(),
            Some("second"),
        );
        // No query `dbname`: the path still answers.
        assert_eq!(
            db_name_from_url("postgres://h/admin?sslmode=require").as_deref(),
            Some("admin"),
        );
    }

    /// The keyword/value form selects the database by keyword, not by path.
    ///
    /// Appending `/{db}` to `host=h dbname=admin` would produce a connection
    /// string whose database is literally `admin/bench_1` — so the rewrite has
    /// to replace the keyword instead.
    #[test]
    fn with_db_name_replaces_dbname_in_the_keyword_value_form() {
        assert_eq!(
            with_db_name("host=h port=5432 dbname=admin", "b"),
            "host=h port=5432 dbname=b",
        );
        // Absent entirely: append it rather than leaving the target implicit
        // (libpq would otherwise default the database to the username).
        assert_eq!(with_db_name("host=h", "b"), "host=h dbname=b");
        // A quoted value elsewhere must survive the round trip intact.
        assert_eq!(
            with_db_name("host=h password='hunter two' dbname=admin", "b"),
            "host=h password='hunter two' dbname=b",
        );
    }

    /// An unparseable remainder must be carried, not silently dropped.
    ///
    /// Dropping it is the *same* failure the URL branch is built to avoid:
    /// losing `?sslmode=require` makes a TLS-requiring server unreachable
    /// through the advertised existing-server mode. The remainder is placed
    /// before the replacement so libpq's last-wins assignment still resolves
    /// `dbname` to ours even if the part we could not parse hides one.
    #[test]
    fn with_db_name_carries_an_unparseable_keyword_remainder() {
        let rewritten = with_db_name("host=h sslmode=require dbname=admin oops", "b");
        assert!(
            rewritten.contains("sslmode=require"),
            "parameter dropped: {rewritten}"
        );
        assert!(rewritten.contains("oops"), "remainder dropped: {rewritten}");
        assert!(
            rewritten.ends_with("dbname=b"),
            "replacement must win last: {rewritten}"
        );
    }

    #[test]
    fn with_db_name_handles_urls_with_no_path_or_no_query() {
        assert_eq!(
            with_db_name("postgres://postgres:postgres@localhost:5432/postgres", "b"),
            "postgres://postgres:postgres@localhost:5432/b",
        );
        assert_eq!(with_db_name("postgres://host", "b"), "postgres://host/b");
        assert_eq!(with_db_name("postgres://host/", "b"), "postgres://host/b");
        assert_eq!(
            with_db_name("postgres://host?sslmode=require", "b"),
            "postgres://host/b?sslmode=require",
        );
    }

    #[test]
    fn every_gate_is_ordered_after_its_comparand() {
        // The report renders a row's `p50 vs` against its comparand's already
        // measured p50, streaming each row as it completes rather than
        // buffering the whole table — a full sweep takes tens of minutes and an
        // operator should not stare at a blank table for all of it. That is
        // only sound if a comparand is always measured first. Reordering
        // `all()` without this test would silently print `n/a` deltas.
        let all = ClaimGate::all();
        for (i, gate) in all.iter().enumerate() {
            let comparand = gate.comparand();
            if comparand == *gate {
                continue;
            }
            let at = all
                .iter()
                .position(|g| *g == comparand)
                .expect("comparand must be in all()");
            assert!(
                at < i,
                "{} is reported against {}, which must be measured first but sits later in all()",
                gate.as_str(),
                comparand.as_str(),
            );
        }
    }

    #[test]
    fn paused_rows_has_an_equal_depth_control() {
        // `paused_rows` seeds 2x the table depth of `baseline`, and claim
        // latency is strongly superlinear in depth — so comparing it against
        // `baseline` charges the anti-join for the extra rows too. Its
        // comparand must be the equal-depth control.
        assert_eq!(ClaimGate::PausedRows.comparand(), ClaimGate::DoubleBacklog);
        for gate in ClaimGate::all() {
            if gate != ClaimGate::PausedRows {
                assert_eq!(
                    gate.comparand(),
                    ClaimGate::Baseline,
                    "{}: only paused_rows needs a non-baseline comparand",
                    gate.as_str(),
                );
            }
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

    /// No cleanup here may reach for `DROP DATABASE ... WITH (FORCE)`.
    ///
    /// That clause is `PostgreSQL` 13+ and the engine supports 12+ (README).
    /// Every cleanup site discards its result — a failed cleanup must not fail
    /// the run — so on 12 the statement is a syntax error that vanishes and the
    /// database leaks instead. [`db::drop_database_version_neutral`] is the
    /// version-neutral replacement.
    ///
    /// A source scan rather than a behavioural test because the claim is about
    /// a server version CI does not run: the behavioural half (evicting a live
    /// backend) is covered by `version_neutral_drop_evicts_a_live_backend` in
    /// `claim_budget_tests.rs`, but nothing there can catch the clause coming
    /// back on `PostgreSQL` 16. Comment lines are exempt — four of them explain
    /// precisely why the clause is absent.
    #[test]
    fn no_cleanup_emits_the_postgres_13_only_force_clause() {
        // Assembled rather than written out: this guard lives inside one of the
        // files it scans, so spelling the needle here would match itself and the
        // test could never pass. Do not "tidy" it back into one literal.
        let needle = concat!("FOR", "CE");

        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/integration");
        for file in ["claim_bench_support.rs", "claim_budget_tests.rs"] {
            let path = dir.join(file);
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            for (index, line) in source.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                assert!(
                    !trimmed.contains(needle),
                    "{}:{} emits the PostgreSQL 13+ clause: {trimmed}\n\
                     The engine supports Postgres 12+ and cleanup results are \
                     discarded, so on 12 this is a syntax error that leaks the \
                     database silently. Use db::drop_database_version_neutral.",
                    file,
                    index + 1,
                );
            }
        }
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

    /// A token unique to this *run*, mixed into every database name.
    ///
    /// A pid alone does not identify a run once an admin URL is shared across
    /// hosts: PID 1 is the norm for a containerised process, so two runs on
    /// different machines would both mint `harvest_claim_bench_1_0` and race at
    /// `CREATE DATABASE` — failing one perfectly valid benchmark. It would also
    /// make the sweep's self-exemption wrong in the other direction, since a
    /// foreign run's database would look like our own.
    ///
    /// `RandomState` is seeded from OS entropy per process, so hashing the
    /// current time and pid through it yields a value that differs across hosts
    /// and across runs on one host. Std-only: the harness deliberately pulls in
    /// no RNG crate for a name suffix.
    ///
    /// Public because the gate suite's probes create databases of their own on
    /// the same shared server and need the *same* uniqueness guarantee. Reusing
    /// this rather than rolling a second uniquifier means there is one
    /// definition of "which run minted this" for a human to reason about, and
    /// nothing that can drift.
    pub fn run_token() -> &'static str {
        static TOKEN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        TOKEN.get_or_init(|| {
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos()),
            );
            h.write_u32(std::process::id());
            format!("{:016x}", h.finish())
        })
    }

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
        /// Idle connection held open for the whole lifetime of the database.
        ///
        /// The stale-database sweep asks the server "does anything hold a
        /// backend against this database?" — but between scenarios (and in the
        /// window right after setup) a run holds none of its own: each scenario
        /// opens a connection, seeds, drops it, builds a pool, and drops that
        /// too. A concurrent run on another host would see zero backends,
        /// conclude the database is abandoned, and `DROP DATABASE ... WITH
        /// (FORCE)` it out from under us.
        ///
        /// So the migration connection is retained rather than dropped: an idle
        /// backend still appears in `pg_stat_activity`, which makes ownership
        /// continuously visible to every other client for exactly as long as
        /// this `BenchDb` lives. Never used for queries — held purely as a
        /// lease.
        ///
        /// `None` on the testcontainer path: nothing outside this process can
        /// see that server, so there is no one to defend against.
        _lease: Option<AsyncPgConnection>,
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
    /// Run one provisioning step under a wall-clock ceiling.
    ///
    /// `setup_bench_db` runs *before* any scenario, so its awaits are outside
    /// both the measured phase's deadline and the setup deadline `connect_and_seed`
    /// applies — the same hole one level up. A server that completes the TCP
    /// handshake and then stops answering during authentication, `CREATE
    /// DATABASE`, the lease connection or the migrations would park the bench
    /// and the CI gate until the outer workflow timeout.
    ///
    /// A timeout is reported as a [`SkipReason`], **not** a panic, because that
    /// is this function's existing contract: every failure to provision skips
    /// the benchmark rather than failing the build. A database that will not
    /// answer is an environment the gate cannot run in, which is exactly what
    /// `SkipReason` means. (`connect_and_seed` panics instead, because by then
    /// provisioning has succeeded and a stall is a real fault.)
    ///
    /// The returned `Result` nests: the outer is the deadline, the inner is
    /// whatever the step itself returns.
    pub async fn with_provision_deadline<T>(
        what: &str,
        deadline: Instant,
        fut: impl std::future::Future<Output = T> + Send,
    ) -> Result<T, SkipReason> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::timeout(remaining, fut).await.map_err(|_| {
            SkipReason(format!(
                "provisioning the benchmark database stalled: {what} did not finish \
                 within {remaining:?}. The server accepted a connection but stopped \
                 answering, so the benchmark cannot run here."
            ))
        })
    }

    pub async fn setup_bench_db() -> Result<BenchDb, SkipReason> {
        // One ceiling for the whole provisioning phase, fixed before the first
        // await on *either* path. See `with_provision_deadline`. Hoisted above
        // the branch deliberately: the container path is the one CI actually
        // runs, so a ceiling that covered only the existing-server path would
        // protect the mode nobody exercises.
        let deadline = Instant::now() + super::scenario_time_budget();
        if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            // Redacted: this message is printed by both the bench and the CI
            // gate, and an admin URL normally carries a password.
            let safe_url = super::redact_url(&admin_url);
            // Through `SWEEP_LOCK_DB`, not the database the operator named.
            //
            // This session is held from here until the lease connects, which
            // includes the whole wait for the sweep lock. If the operator's URL
            // named `template1`, that wait would leave a backend attached to the
            // template — and the lock *holder's* plain `CREATE DATABASE` copies
            // `template1` by default, so it would fail with "source database is
            // being accessed by other users". One client waiting would break
            // another client's create.
            //
            // Costs no reachability: `take_sweep_lock` already refuses to start
            // a run that cannot connect to `SWEEP_LOCK_DB`, so this adds no
            // requirement the harness did not already have. Every statement
            // this session issues — the sweep's catalog query and `DROP`s, the
            // `CREATE DATABASE` — is cluster-scoped and works from anywhere.
            let admin_conn_url = admin_connection_url(&admin_url);
            let mut admin = with_provision_deadline(
                "connect to the admin database",
                deadline,
                AsyncPgConnection::establish(&admin_conn_url),
            )
            .await?
            .map_err(|e| SkipReason(format!("connect {safe_url}: {e}")))?;
            // Everything from here to the lease runs under an advisory lock, so
            // no other client can sweep while we are between `CREATE DATABASE`
            // and holding a backend on it. The lock lives on its own connection
            // to a fixed database — see `SWEEP_LOCK_KEY` for why that matters.
            let lock = with_provision_deadline(
                "take the sweep lock",
                deadline,
                take_sweep_lock(&admin_url),
            )
            .await??;
            with_provision_deadline(
                "sweep stale benchmark databases",
                deadline,
                drop_stale_bench_databases(&mut admin),
            )
            .await?;
            let n = DB_SEQ.fetch_add(1, Ordering::SeqCst);
            // Shape: harvest_claim_bench_{pid}_{run_token}_{seq}. The pid is a
            // cheap same-host liveness handle for the sweep; the run token is
            // what actually makes the name unique when the admin URL is shared
            // across hosts (see `run_token`).
            let db = format!(
                "{BENCH_DB_PREFIX}{}_{}_{}",
                std::process::id(),
                run_token(),
                n
            );
            with_provision_deadline(
                "create the benchmark database",
                deadline,
                diesel::sql_query(format!("CREATE DATABASE {db}")).execute(&mut admin),
            )
            .await?
            .map_err(|e| SkipReason(format!("create database {db}: {e}")))?;
            let url = super::with_db_name(&admin_url, &db);
            let mut conn = with_provision_deadline(
                "connect the ownership lease",
                deadline,
                AsyncPgConnection::establish(&url),
            )
            .await?
            .map_err(|e| SkipReason(format!("connect {db}: {e}")))?;
            // Arm the lease before anything can observe it: a lease the server
            // is free to reap is not a lease. Done under the lock so the
            // database is never visible to a foreign sweep in a state where its
            // only defence has an expiry date.
            with_provision_deadline(
                "arm the ownership lease",
                deadline,
                arm_lease_session(&mut conn),
            )
            .await?;
            // The lease now exists, so the database is visible in
            // `pg_stat_activity` and defends itself. Release before the
            // migration — that is the slow part, and holding the lock across it
            // would serialize concurrent runs for no benefit. Every `?` above
            // drops `lock`, which ends that session and releases the lock too,
            // so an early return cannot strand it.
            with_provision_deadline("release the sweep lock", deadline, release_sweep_lock(lock))
                .await?;
            with_provision_deadline(
                "run migrations",
                deadline,
                conn.batch_execute(autumn_harvest::full_migrations_sql()),
            )
            .await?
            .map_err(|e| SkipReason(format!("migrate {db}: {e}")))?;
            return Ok(BenchDb {
                url,
                _container: None,
                // Retained, not dropped: this is the ownership lease. See the
                // field docs on `BenchDb::_lease`.
                _lease: Some(conn),
            });
        }

        setup_container_bench_db(deadline).await
    }

    /// Provision the Docker-backed database, under the same ceiling.
    ///
    /// Split out so `setup_bench_db` stays readable, but bounded for a reason
    /// that is not cosmetic: this is the path the checked CI manifest runs, so
    /// leaving it unbounded would mean the provisioning ceiling only guards the
    /// mode CI never exercises. A daemon that accepts the socket and then stops
    /// answering — mid-pull, mid-create, mid-metadata — parks the gate until the
    /// outer job timeout otherwise.
    async fn setup_container_bench_db(deadline: Instant) -> Result<BenchDb, SkipReason> {
        let container = with_provision_deadline(
            "start the benchmark container",
            deadline,
            Postgres::default()
                .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
                .with_tag("16")
                .start(),
        )
        .await?
        .map_err(|e| {
            SkipReason(format!(
                "no Docker daemon and HARVEST_TEST_DATABASE_URL unset ({e})"
            ))
        })?;
        let host =
            with_provision_deadline("read the container host", deadline, container.get_host())
                .await?
                .map_err(|e| SkipReason(format!("container host: {e}")))?;
        let port = with_provision_deadline(
            "read the container port",
            deadline,
            container.get_host_port_ipv4(5432),
        )
        .await?
        .map_err(|e| SkipReason(format!("container port: {e}")))?;
        Ok(BenchDb {
            url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            _container: Some(container),
            // No lease: the container is the ownership boundary, and nothing
            // outside this process can reach that server to sweep it.
            _lease: None,
        })
    }

    /// Prefix of every database this harness creates against an admin URL.
    ///
    /// Defined next to [`super::sweep_step`], which validates the rest of the
    /// shape, so minting and reclaiming can never disagree about what our names
    /// look like.
    use super::BENCH_DB_PREFIX;

    /// Make the ownership lease immune to the server's idle-session reaper.
    ///
    /// The lease defends a running benchmark by *being* an idle backend, which
    /// is precisely the thing Postgres 14's `idle_session_timeout` exists to
    /// kill. On a server that sets it — an entirely reasonable thing to do on
    /// the shared cluster this mode is designed to point at — the lease is
    /// terminated mid-run while [`BenchDb`] still holds the (now dead) handle,
    /// so [`database_has_connections`] reports the live run's database as
    /// abandoned and a concurrent sweep drops it out from under us. The failure
    /// is silent on both sides: nothing tells the victim its lease is gone, and
    /// the sweeper is behaving correctly on the evidence available to it.
    ///
    /// `SET` is session-local and `idle_session_timeout` is `USERSET`, so this
    /// overrides a `postgresql.conf`, `ALTER DATABASE` or `ALTER ROLE` default
    /// without needing any privilege, and affects only the lease.
    ///
    /// Disabling the timeout is preferred over heartbeating the lease: a
    /// heartbeat needs a background task whose lifetime is tied to `BenchDb`,
    /// and it would still leave a window one interval wide. This closes the
    /// hole outright with one statement and no task.
    ///
    /// # Version tolerance and the read-back
    ///
    /// The `SET` fails on Postgres 12/13, where the GUC does not exist — and
    /// on those versions there is nothing to defend against, so the error is
    /// ignored. That tolerance is exactly what could hide a *real* failure on
    /// 14+, so the value is read back rather than assumed:
    /// `current_setting(name, missing_ok := true)` returns `NULL` for an
    /// unknown GUC, which distinguishes "too old to have the reaper" from "the
    /// reaper is armed and we failed to disable it". Only the second case
    /// warns, and it warns rather than aborting because an unprotected lease
    /// degrades a concurrent run, and this mode is usually a single run.
    pub async fn arm_lease_session(conn: &mut AsyncPgConnection) {
        #[derive(QueryableByName)]
        struct SettingRow {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            value: Option<String>,
        }

        // Ignored: on PG < 14 this is "unrecognized configuration parameter",
        // which is the benign case. The read-back below is what catches a
        // failure that actually matters.
        let _ = diesel::sql_query("SET idle_session_timeout = 0")
            .execute(&mut *conn)
            .await;

        let Ok(row) =
            diesel::sql_query("SELECT current_setting('idle_session_timeout', true) AS value")
                .get_result::<SettingRow>(&mut *conn)
                .await
        else {
            // Could not read it back. Nothing actionable to report: the lease
            // either has the timeout disabled or the connection is already in
            // trouble, and both surface elsewhere.
            return;
        };

        match row.value.as_deref() {
            // Two ways to be safe, and they are genuinely different facts:
            // `NULL` means the GUC does not exist, so this server has no idle
            // reaper at all, while `"0"` is how Postgres renders a timeout we
            // successfully disabled. Neither leaves the lease exposed, so they
            // share an arm; any other value means the reaper is still armed.
            None | Some("0") => {}
            Some(other) => eprintln!(
                "warning: could not disable idle_session_timeout on the bench \
                 database lease (still {other}). The lease is what tells other \
                 clients this database is in use, so if it is reaped mid-run a \
                 concurrent benchmark's sweep may drop this run's database. \
                 Run benchmarks one at a time against this server, or set \
                 idle_session_timeout = 0 for the role."
            ),
        }
    }

    /// Advisory-lock key serializing sweep-and-create across clients.
    ///
    /// A database is only defended once it has a backend in `pg_stat_activity`,
    /// but it cannot have one until it exists — so between `CREATE DATABASE`
    /// and the lease connection there is a window where a foreign sweep sees a
    /// real database with zero connections and correctly concludes it is
    /// abandoned. No amount of naming fixes that; the window has to be closed
    /// by making the two operations mutually exclusive.
    ///
    /// Postgres advisory locks are scoped to the database the session is
    /// connected to, **not** to the cluster: two sessions on different
    /// databases can hold the same key simultaneously (verified against a live
    /// server — `pg_try_advisory_lock` on the same key returns true from a
    /// second database while the first still holds it). So the lock cannot ride
    /// the admin connection, whose database is whatever the operator happened
    /// to put in `HARVEST_TEST_DATABASE_URL`: two runs pointed at the same
    /// cluster through different admin databases would not serialize at all,
    /// which is the same as no lock. It is taken on its own connection to
    /// [`SWEEP_LOCK_DB`] instead, so every client agrees on where it lives.
    ///
    /// The value is arbitrary but must never change: two harness versions using
    /// different keys would not serialize against each other either.
    ///
    /// Public so the regression tests that prove setup takes this lock, in this
    /// database, reference these constants rather than copies of the literals.
    pub const SWEEP_LOCK_KEY: i64 = 786_0786_0786;

    /// The database the sweep lock is taken in. See [`SWEEP_LOCK_KEY`].
    ///
    /// `postgres` because it is the one database every server is guaranteed to
    /// have and every admin URL can reach. Deliberately **not** `template1`,
    /// which is equally universal but is the default template for
    /// `CREATE DATABASE`: holding a connection to it would make our own
    /// create — the very operation the lock exists to protect — fail with
    /// "source database is being accessed by other users".
    pub const SWEEP_LOCK_DB: &str = "postgres";

    /// How long to wait for a peer's sweep-and-create before giving up.
    ///
    /// Generous: the guarded span is a sweep plus one `CREATE DATABASE` plus
    /// one connect, and the sweep's `DROP`s are the only part that can drag.
    /// The timeout exists so a wedged peer surfaces as a diagnosable failure
    /// rather than an unbounded hang.
    const SWEEP_LOCK_TIMEOUT: &str = "60s";

    /// Where the provisioning session connects, given the operator's admin URL.
    ///
    /// Always [`SWEEP_LOCK_DB`], never the database the operator named. That
    /// session is held from the first connect until the lease is armed, which
    /// spans the whole wait for the sweep lock — so if the operator's URL named
    /// `template1`, a client *waiting* here would keep a backend on the
    /// template, and the lock holder's plain `CREATE DATABASE` (which copies
    /// `template1`) would fail with "source database is being accessed by other
    /// users". One client waiting would break another client's create.
    ///
    /// Costs no reachability: [`take_sweep_lock`] already refuses to start a run
    /// that cannot reach `SWEEP_LOCK_DB`.
    ///
    /// Extracted so the choice is assertable rather than buried in a connect
    /// call — see `provisioning_connects_through_the_fixed_database`.
    pub fn admin_connection_url(admin_url: &str) -> String {
        super::with_db_name(admin_url, SWEEP_LOCK_DB)
    }

    /// Take the sweep lock, blocking until a peer releases it.
    ///
    /// Returns the connection holding it. The lock lives on its own session in
    /// [`SWEEP_LOCK_DB`] rather than on the caller's admin connection, because
    /// advisory locks are database-scoped — see [`SWEEP_LOCK_KEY`]. Dropping
    /// the returned connection releases the lock, so a caller that bails out
    /// with `?` cannot strand it.
    ///
    /// Public so the regression test can prove the lock is taken in the fixed
    /// database rather than in whichever one the admin URL names.
    pub async fn take_sweep_lock(admin_url: &str) -> Result<AsyncPgConnection, SkipReason> {
        // No fallback to the admin URL's own database. That would take a lock
        // scoped to whichever database the operator named, which is exactly the
        // per-database scoping this fixed database exists to escape — two
        // clients arriving through different admin databases would both enter
        // the sweep/create section and one could drop the other's new database
        // before its lease connects. A warning does not protect anybody's data,
        // so an unreachable `SWEEP_LOCK_DB` is terminal: better a run that does
        // not start than a run that silently cannot serialize.
        let mut lock = AsyncPgConnection::establish(&super::with_db_name(admin_url, SWEEP_LOCK_DB))
            .await
            .map_err(|e| {
                SkipReason(format!(
                    "connect {SWEEP_LOCK_DB} for the sweep lock: {e}. The benchmark \
                         serializes its stale-database sweep through `{SWEEP_LOCK_DB}` so \
                         that clients reaching this cluster through different admin \
                         databases still coordinate; without it a concurrent run could \
                         drop this one's database. Grant the role CONNECT on \
                         `{SWEEP_LOCK_DB}`, or point HARVEST_TEST_DATABASE_URL at a \
                         cluster where it is reachable."
                ))
            })?;
        // `lock_timeout` covers advisory locks, so a peer that wedges while
        // holding the lock aborts our wait with an error instead of hanging.
        diesel::sql_query(format!("SET lock_timeout = '{SWEEP_LOCK_TIMEOUT}'"))
            .execute(&mut lock)
            .await
            .map_err(|e| SkipReason(format!("set lock_timeout: {e}")))?;
        diesel::sql_query("SELECT pg_advisory_lock($1)")
            .bind::<diesel::sql_types::BigInt, _>(SWEEP_LOCK_KEY)
            .execute(&mut lock)
            .await
            .map_err(|e| {
                SkipReason(format!(
                    "waited {SWEEP_LOCK_TIMEOUT} for another benchmark run's \
                     sweep-and-create to finish: {e}"
                ))
            })?;
        Ok(lock)
    }

    /// Release the sweep lock. Best-effort: dropping the connection also
    /// releases it, which is why this consumes it.
    pub async fn release_sweep_lock(mut lock: AsyncPgConnection) {
        let _ = diesel::sql_query("SELECT pg_advisory_unlock($1)")
            .bind::<diesel::sql_types::BigInt, _>(SWEEP_LOCK_KEY)
            .execute(&mut lock)
            .await;
    }

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

        for row in rows {
            // The authority on whether this name is ours at all. The `LIKE`
            // above is only a prefilter — and a loose one, since `_` is a
            // single-character wildcard — so every candidate is re-checked here
            // against the full minted shape before anything destructive runs.
            if super::sweep_step(&row.datname) == super::SweepStep::Skip {
                continue;
            }

            // The authority. A pid answers only for this host, so when several
            // machines share one admin URL a foreign run looks dead and a
            // forced drop would terminate it mid-measurement. The server is
            // the one party that can see every client, and a live run holds a
            // lease connection for the whole life of its database (see
            // `BenchDb::_lease`), so any backend at all means "in use".
            if database_has_connections(admin, &row.datname).await {
                continue;
            }
            // We reach this line only for a database just observed to have zero
            // connections, so the terminate inside the helper is belt-and-braces
            // against a backend arriving in the gap between that check and the
            // drop.
            drop_database_version_neutral(admin, &row.datname).await;
        }
    }

    /// `DROP DATABASE`, on every `PostgreSQL` the engine supports.
    ///
    /// `DROP DATABASE ... WITH (FORCE)` is tidier but is `PostgreSQL` 13+, and the
    /// engine supports 12+ (see the README). On 12 it is a *syntax error* — and
    /// every caller here ignores the result, because a cleanup that fails must
    /// not fail the run — so the database silently accumulates forever instead.
    /// Terminate-then-drop is version-neutral and equivalent in effect.
    ///
    /// Shared rather than written out at each site: the two callers leak in
    /// different ways when they disagree. A stale bench database at least
    /// carries the harness prefix and is reclaimed by the next run's sweep; a
    /// probe database does not, so nothing ever collects it.
    ///
    /// Best-effort by design — both statements' results are discarded.
    ///
    /// # Safety
    ///
    /// `name` is interpolated, not bound (`PostgreSQL` does not accept a parameter
    /// there). The caller must have **minted** it, or — for a name read back
    /// from the server — cleared it through [`super::sweep_step`] first, which
    /// is the check that keeps a database this harness did not create out of a
    /// `DROP`.
    pub async fn drop_database_version_neutral(conn: &mut AsyncPgConnection, name: &str) {
        let _ = diesel::sql_query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind::<diesel::sql_types::Text, _>(name)
        .execute(conn)
        .await;

        // One-argument `pg_terminate_backend` *signals*; it does not wait. A
        // backend unwinding a transaction can still hold the database when the
        // `DROP` arrives, which fails with "is being accessed by other users" —
        // and that error is discarded here, so the database would leak exactly
        // as if we had never terminated anything. The two-argument form that
        // waits is `PostgreSQL` 14+, and version-neutrality is this helper's
        // whole reason to exist, so poll instead.
        //
        // Bounded, and the `DROP` runs either way: a backend that refuses to
        // leave is not worth hanging a best-effort cleanup over, and attempting
        // the `DROP` anyway is strictly no worse than the unpolled version.
        let wait_until = Instant::now() + TERMINATE_WAIT;
        while database_has_connections(conn, name).await {
            if Instant::now() >= wait_until {
                break;
            }
            tokio::time::sleep(TERMINATE_POLL).await;
        }

        let _ = diesel::sql_query(format!("DROP DATABASE IF EXISTS {name}"))
            .execute(conn)
            .await;
    }

    /// How long to wait for signalled backends to actually exit.
    ///
    /// Generous relative to the work: a terminated backend normally goes within
    /// milliseconds, and the only slow case is one unwinding a long
    /// transaction. See [`drop_database_version_neutral`].
    const TERMINATE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

    /// Gap between checks while waiting for signalled backends to exit.
    const TERMINATE_POLL: std::time::Duration = std::time::Duration::from_millis(50);

    /// Does any backend currently hold a connection to `datname`?
    ///
    /// Host-agnostic, unlike a local pid probe: the server sees every client
    /// regardless of which machine or PID namespace it runs in.
    ///
    /// Conservative by design — a failed check reports "in use", so the worst
    /// outcome is a leaked database, never a live run dropped out from under
    /// itself.
    async fn database_has_connections(admin: &mut AsyncPgConnection, datname: &str) -> bool {
        #[derive(QueryableByName)]
        struct CountRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }

        diesel::sql_query("SELECT count(*) AS n FROM pg_stat_activity WHERE datname = $1")
            .bind::<diesel::sql_types::Text, _>(datname)
            .get_result::<CountRow>(admin)
            .await
            .map_or(true, |row| row.n > 0)
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

    /// Whether the scenario seeds `2 * backlog` *claimable* rows.
    ///
    /// This is the equal-depth control for [`ClaimGate::PausedRows`]: both seed
    /// the same number of table rows, and only `PausedRows` adds the anti-join.
    const fn wants_double_backlog(gate: ClaimGate) -> bool {
        matches!(gate, ClaimGate::DoubleBacklog)
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

    /// Seed the build-routing compatibility declaration so the claim resolves
    /// through the `harvest_build_compat` `EXISTS` branch (issue #171).
    ///
    /// Deliberately does *not* seed `harvest_build_policies`: that table is read
    /// at workflow-start time to stamp `assigned_build_id`, and the benchmark
    /// writes `required_build_id` onto task rows directly. `claim_task` itself
    /// never touches it, so seeding it would be measurement theatre.
    async fn seed_build_routing(conn: &mut AsyncPgConnection) {
        exec(
            conn,
            &format!(
                "INSERT INTO harvest_build_compat (build_id, compatible_with) \
                 VALUES ('{BENCH_NEW_BUILD}', '{BENCH_OLD_BUILD}')"
            ),
        )
        .await;
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
            seed_build_routing(conn).await;
        }
        if wants_rate_limit(gate) {
            seed_rate_limit_buckets(conn).await;
        }

        // The `DoubleBacklog` control seeds twice the claimable rows so it
        // matches `PausedRows`' table depth with no anti-join.
        let claimable_rows = if wants_double_backlog(gate) {
            backlog * 2
        } else {
            backlog
        };
        seed_backlog(conn, gate, claimable_rows, queues).await;

        let mut seeded_rows = claimable_rows;
        if wants_paused_rows(gate) {
            seed_paused_ballast(conn, backlog, queues).await;
            seeded_rows += backlog;
        }

        exec(conn, "ANALYZE harvest_task_queue").await;
        exec(conn, "ANALYZE harvest_workflow_executions").await;

        SeedOutcome {
            seeded_rows,
            claimable_rows,
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
        /// *Every* call that returned a task, warmup included.
        ///
        /// Separate from [`Self::claimed`] because the two answer different
        /// questions over different windows. `claimed` is a *post-warmup*
        /// count, paired with the post-warmup sample set for latency and for
        /// [`Self::claim_ratio`]. This one is paired with [`Self::wall_secs`],
        /// which runs from the first warmup call, so a throughput fraction
        /// built from it shares a window with its denominator. Dividing the
        /// trimmed count by the untrimmed clock understated `claims/s` by the
        /// warmup fraction — about 10%.
        pub total_claimed: usize,
        /// Wall time for the whole measured phase, warmup included.
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
        ///
        /// Numerator and denominator deliberately cover the *same* window:
        /// every successful claim, warmup included, over the whole wall clock.
        /// This matches [`EnqueueReport::rows_per_sec`], which divides all rows
        /// by the whole clock for the same reason. Latency statistics still
        /// exclude warmup — throughput and latency want different windows, and
        /// the two counters exist so neither has to compromise.
        #[must_use]
        pub fn claims_per_sec(&self) -> f64 {
            if self.wall_secs <= 0.0 {
                return 0.0;
            }
            #[allow(clippy::cast_precision_loss)]
            {
                self.total_claimed as f64 / self.wall_secs
            }
        }
    }

    /// Seed and measure one claim scenario end to end.
    ///
    /// # Panics
    /// Panics if seeding or connection acquisition fails.
    pub async fn run_claim_scenario(db: &BenchDb, scenario: Scenario) -> ClaimReport {
        run_claim_scenario_with_budget(db, scenario, super::scenario_time_budget()).await
    }

    /// Arrive at the scenario start barrier, bounded by the scenario deadline.
    ///
    /// Every task arrives — including one that failed checkout and is about to
    /// return — so a party that is never coming cannot park its peers here. The
    /// deadline bound is the backstop for any future path that forgets: it
    /// keeps this await under the same ceiling as every other await in the
    /// task, so the advertised cap stays real.
    async fn arrive_at_start_gate(gate: &tokio::sync::Barrier, deadline: Instant) -> Instant {
        let _ = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            gate.wait(),
        )
        .await;
        Instant::now()
    }

    /// Connect and seed under a setup ceiling, then hand back the connection.
    ///
    /// Both runners share this so the bound cannot exist on one path and not the
    /// other — the same reason their budgets are passed the same way. The
    /// connection is dropped here rather than returned: neither runner uses it
    /// past seeding, and holding it open would keep a session the pool then has
    /// to work around.
    async fn connect_and_seed(
        db: &BenchDb,
        scenario: Scenario,
        budget: std::time::Duration,
    ) -> SeedOutcome {
        let deadline = Instant::now() + budget;
        let mut conn = super::with_setup_deadline("connect", deadline, connect(&db.url)).await;
        let outcome = super::with_setup_deadline("seed", deadline, seed(&mut conn, scenario)).await;
        drop(conn);
        outcome
    }

    /// [`run_claim_scenario`] with the wall-clock ceiling supplied directly.
    ///
    /// The budget is a *parameter* rather than something this function reads
    /// from the process so that a test wanting a different ceiling can ask for
    /// one without writing to the environment. `HARVEST_BENCH_SCENARIO_SECS` is
    /// process-global and `cargo test` runs a binary's tests in parallel by
    /// default, so a test that set it would be reaching into every sibling
    /// scenario running at that moment — handing them a budget they never asked
    /// for and would report as truncated. No amount of care at the write site
    /// fixes that: a restore-on-drop guard still leaves the window open for as
    /// long as the test runs, which for these is the whole point of them.
    ///
    /// So the environment is read exactly once, at the edge, by the caller that
    /// wants the default; everything below here takes the answer as an
    /// argument.
    ///
    /// # Panics
    ///
    /// Panics if seeding or claiming fails.
    pub async fn run_claim_scenario_with_budget(
        db: &BenchDb,
        scenario: Scenario,
        budget: std::time::Duration,
    ) -> ClaimReport {
        // Setup runs before the measured phase and so before its deadline; it
        // carries its own. See `with_setup_deadline`.
        let seed_outcome = connect_and_seed(db, scenario, budget).await;

        let pool = build_pool(&db.url, scenario.claimers);
        let queues = Arc::new(queue_names(scenario));
        let cb_set = Arc::new(circuit_breaker_set(scenario.gate));
        let build_id = worker_build_id(scenario.gate).to_string();

        let total_ops = measured_claims_for(scenario.backlog);
        let claimers = scenario.claimers.max(1);

        let started = Instant::now();
        // One ceiling for the whole scenario, fixed *before* any claimer runs.
        // Deriving it inside the task (after `pool.get()`) would restart the
        // clock behind an unbounded await: a stalled or exhausted pool would
        // park every claimer indefinitely with no deadline yet in existence,
        // and the advertised cap would be a fiction. A scenario-wide deadline
        // also means the cap bounds the scenario, not each claimer separately.
        let deadline = started + budget;
        // Every claimer checks out its connection and then waits here, so the
        // measured phase begins with all of them contending at once. Without
        // it, claimer 0 could take its first samples while claimer N-1 was
        // still inside `pool.get()`: the head of the sample would be measured
        // under less contention than the scenario nominally models — flattering
        // p50 for exactly the multi-claimer scenarios that exist to measure
        // contention — and the throughput denominator would count the ramp-up
        // as if it were measured work.
        //
        // The deadline above is deliberately *not* re-anchored after this
        // barrier: checkout is an unbounded await, so the ceiling has to exist
        // before it (see the comment on `deadline`). The measurement clock is
        // separate, and starts on release.
        //
        // Safe only because `build_pool` sizes the pool *above* the claimer
        // count: every party can hold a connection at once. A pool smaller than
        // the party count would turn checkout into a genuine deadlock, since
        // the claimers holding connections would be waiting here for one that
        // cannot check out until they release.
        let gate = Arc::new(tokio::sync::Barrier::new(claimers));
        let mut handles = Vec::with_capacity(claimers);
        for c in 0..claimers {
            let pool = pool.clone();
            let queues = Arc::clone(&queues);
            let cb_set = Arc::clone(&cb_set);
            let build_id = build_id.clone();
            let gate = Arc::clone(&gate);
            // Exact split, so the sum across claimers never exceeds the
            // drain bound (see `claims_for_claimer`).
            let per_claimer = super::claims_for_claimer(total_ops, claimers, c);
            handles.push(tokio::spawn(async move {
                let worker = format!("{BENCH_PREFIX}-worker-{c}");
                // (latency_ms, claimed_a_task) in observation order, so warmup
                // can be trimmed from the front once we know how many
                // observations this claimer actually managed to take.
                let mut observed: Vec<(f64, bool)> = Vec::with_capacity(per_claimer);
                // Bounded by the same deadline the claim loop uses: checkout is
                // an unbounded await, so a stalled or exhausted pool would
                // otherwise hang here past the ceiling. Failing to check out is
                // terminal for this claimer — it takes no samples and reports
                // truncated, so the gate calls the measurement unsound rather
                // than publishing a percentile from the claimers that did run.
                let Ok(Ok(mut conn)) = tokio::time::timeout(
                    deadline.saturating_duration_since(Instant::now()),
                    pool.get(),
                )
                .await
                else {
                    // Still reports a span: its resume is a valid observation
                    // of when the gate released, and folding it in can only
                    // move the shared window's start *earlier*, never later.
                    let resumed = arrive_at_start_gate(&gate, deadline).await;
                    return (
                        super::ClaimerOutcome::from_observed(Vec::new(), true),
                        (resumed, resumed),
                    );
                };
                // The measurement clock starts at the barrier, not at
                // `started`: the span before it is setup, not measured work.
                let resumed = arrive_at_start_gate(&gate, deadline).await;
                let mut truncated = false;
                for _ in 0..per_claimer {
                    // Wall-clock ceiling: claim latency scales with backlog
                    // depth, so an operation count alone does not bound a deep
                    // sweep (or a regression). Stop early and report the
                    // samples collected rather than running for minutes.
                    let now = Instant::now();
                    if now >= deadline {
                        truncated = true;
                        break;
                    }
                    let t0 = now;
                    // Bound the call, not just the loop head. `claim_task` is
                    // an unbounded await, so a single stalled claim — exactly
                    // the regression or database stall this ceiling exists to
                    // catch — would sit here for minutes while the loop-top
                    // check never runs again, and the advertised cap would be
                    // a fiction.
                    //
                    // A timeout abandons the connection mid-query, which can
                    // leave it unusable. That is fine and terminal: we break
                    // immediately, the pool recycles it, and `truncated` makes
                    // the gate fail loudly rather than publish a percentile
                    // computed from a partial run.
                    let claimed = tokio::time::timeout(
                        deadline - now,
                        queue::claim_task(
                            &mut conn,
                            &queues,
                            &worker,
                            &build_id,
                            None,
                            &cb_set,
                            &[],
                        ),
                    )
                    .await;
                    let Ok(got) = claimed else {
                        truncated = true;
                        break;
                    };
                    let got = got.expect("claim_task failed");
                    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                    observed.push((elapsed, got.is_some()));
                }

                // Split into the latency window (warmup trimmed) and the
                // throughput window (everything), now that the real observation
                // count is known. See `ClaimerOutcome::from_observed`.
                (
                    super::ClaimerOutcome::from_observed(observed, truncated),
                    (resumed, Instant::now()),
                )
            }));
        }

        let mut all_samples: Vec<f64> = Vec::with_capacity(total_ops);
        let mut claimed = 0usize;
        let mut empty = 0usize;
        let mut total_claimed = 0usize;
        let mut truncated = false;
        // Every claimer's `(resume, finish)` pair, folded into one interval on
        // the shared clock by `measured_window`.
        let mut spans: Vec<(Instant, Instant)> = Vec::with_capacity(claimers);
        for h in handles {
            let (out, span) = h.await.expect("claimer task panicked");
            spans.push(span);
            all_samples.extend(out.samples);
            claimed += out.claimed;
            empty += out.empty;
            total_claimed += out.total_claimed;
            truncated |= out.truncated;
        }
        // Deliberately not `started.elapsed()`: `started` anchors the deadline
        // and therefore precedes pool checkout, and counting that ramp-up as
        // measured work inflated the throughput denominator.
        let wall_secs = super::measured_window(&spans);

        ClaimReport {
            scenario,
            seed: seed_outcome,
            stats: LatencyStats::from_samples(&all_samples),
            claimed,
            empty,
            total_claimed,
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
        /// Total rows written, including the warmup rows excluded from
        /// [`Self::stats`]. This is the throughput numerator.
        pub rows: usize,
        /// Rows discarded from the head of each writer's samples as warmup.
        pub warmup_rows: usize,
        /// Latency over the post-warmup rows only.
        pub stats: LatencyStats,
        pub wall_secs: f64,
        /// At least one writer stopped early on the scenario deadline.
        ///
        /// Same contract as [`ClaimReport::truncated`]: the percentiles above
        /// then describe a partial run, so a caller must treat them as unsound
        /// rather than publishing them.
        pub truncated: bool,
    }

    impl EnqueueReport {
        /// Sustained write rate across the whole measured phase.
        ///
        /// Deliberately divides *all* rows by the *whole* wall clock, warmup
        /// included, so this is a conservative floor on sustained throughput
        /// rather than a peak. It therefore spans a slightly wider window than
        /// [`Self::stats`], which excludes warmup.
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
        run_enqueue_scenario_with_budget(
            db,
            backlog,
            writers,
            rows_per_writer,
            super::scenario_time_budget(),
        )
        .await
    }

    /// [`run_enqueue_scenario`] with the wall-clock ceiling supplied directly.
    ///
    /// See [`run_claim_scenario_with_budget`] for why the budget is injected
    /// rather than read from the environment here. Both paths take it the same
    /// way deliberately: the last two rounds of review on this file were both
    /// cases of the write path missing a property the claim path had, and a
    /// helper that exists on only one of them is how that happens again.
    ///
    /// # Panics
    ///
    /// Panics if seeding or enqueueing fails.
    pub async fn run_enqueue_scenario_with_budget(
        db: &BenchDb,
        backlog: usize,
        writers: usize,
        rows_per_writer: usize,
        budget: std::time::Duration,
    ) -> EnqueueReport {
        let scenario = Scenario {
            backlog,
            // `Scenario` is the seeding vocabulary; `seed` and `queue_names`
            // read only `backlog`/`queues`/`gate`. Carrying the writer count
            // here keeps one struct rather than adding a near-duplicate, but it
            // is never interpreted as a claimer count on this path.
            claimers: writers,
            queues: headline_queue_count(),
            gate: ClaimGate::Baseline,
        };
        // Same two-phase ceiling as the claim path — see `with_setup_deadline`.
        let _ = connect_and_seed(db, scenario, budget).await;

        let pool = build_pool(&db.url, writers);
        let queues = Arc::new(queue_names(scenario));

        let started = Instant::now();
        // One ceiling for the whole scenario, fixed before any writer runs —
        // the same contract as the claim path, and for the same reason: both
        // `pool.get()` and `queue::enqueue` are unbounded awaits, so a stalled
        // server would otherwise hang the bench (and the CI gate that calls
        // this) until the outer workflow timeout, while the page advertises a
        // per-scenario cap.
        let deadline = started + budget;
        // Same start barrier as the claim path, for the same reason: a
        // start-storm measured while some writers are still checking out is a
        // smaller storm than the one advertised, and the ramp-up would inflate
        // the throughput denominator. The deadline stays anchored before
        // checkout; only the measurement clock moves.
        let gate = Arc::new(tokio::sync::Barrier::new(writers.max(1)));
        let mut handles = Vec::with_capacity(writers);
        for w in 0..writers.max(1) {
            let pool = pool.clone();
            let queues = Arc::clone(&queues);
            let gate = Arc::clone(&gate);
            handles.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(rows_per_writer);
                // Failing to check out is terminal for this writer: it takes no
                // samples and reports truncated, so the caller calls the
                // measurement unsound rather than publishing a rate computed
                // only from the writers that did start.
                let Ok(Ok(mut conn)) = tokio::time::timeout(
                    deadline.saturating_duration_since(Instant::now()),
                    pool.get(),
                )
                .await
                else {
                    // Still reports a span; see the claim path's equivalent.
                    let resumed = arrive_at_start_gate(&gate, deadline).await;
                    return (Vec::new(), true, (resumed, resumed));
                };
                let resumed = arrive_at_start_gate(&gate, deadline).await;
                let mut truncated = false;
                for i in 0..rows_per_writer {
                    let now = Instant::now();
                    if now >= deadline {
                        truncated = true;
                        break;
                    }
                    let q = queues[(w + i) % queues.len()].clone();
                    let params = bench_enqueue_params(q);
                    let t0 = now;
                    // Bound the call, not just the loop head: a single stalled
                    // write would otherwise sit here past the ceiling while the
                    // loop-top check never runs again. A timeout abandons the
                    // connection mid-query, which is fine and terminal — we
                    // break, the pool recycles it, and `truncated` propagates.
                    let wrote =
                        tokio::time::timeout(deadline - now, queue::enqueue(&mut conn, &params))
                            .await;
                    let Ok(wrote) = wrote else {
                        truncated = true;
                        break;
                    };
                    wrote.expect("enqueue failed");
                    samples.push(t0.elapsed().as_secs_f64() * 1000.0);
                }
                (samples, truncated, (resumed, Instant::now()))
            }));
        }

        let mut rows = 0usize;
        let mut warmup_rows = 0usize;
        let mut truncated = false;
        // One shared interval across every writer; see `measured_window`.
        let mut spans: Vec<(Instant, Instant)> = Vec::with_capacity(writers);
        let mut all_samples = Vec::with_capacity(writers * rows_per_writer);
        for h in handles {
            let (samples, writer_truncated, span) = h.await.expect("writer task panicked");
            spans.push(span);
            truncated |= writer_truncated;
            rows += samples.len();
            // Same post-hoc warmup trim as the claim path: the first writes on
            // a fresh connection pay plan-cache and connection setup costs that
            // are not representative of a sustained start-storm.
            let warmup = super::warmup_claims_for(samples.len());
            warmup_rows += warmup;
            all_samples.extend(samples.into_iter().skip(warmup));
        }
        // Not `started.elapsed()`: `started` anchors the deadline and precedes
        // pool checkout (see the barrier above).
        let wall_secs = super::measured_window(&spans);

        EnqueueReport {
            backlog,
            writers,
            rows,
            warmup_rows,
            stats: LatencyStats::from_samples(&all_samples),
            wall_secs,
            truncated,
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
        let literals = [
            format!("'{BENCH_PREFIX}-worker-0'"),
            format!("ARRAY[{queue_list}]::text[]"),
            format!("'{}'", worker_build_id(scenario.gate)),
            "NULL".to_string(),
            format!("ARRAY[{cb_list}]::text[]"),
            "ARRAY[]::text[]".to_string(),
        ];
        let raw = queue::claim_task_query();
        // Fail loudly rather than silently explaining a *different* query if the
        // claim query grows a seventh bind. A naive `.replace("$1", ..)` sweep
        // would also corrupt `$10`+, so substitute highest-numbered first.
        assert!(
            !raw.contains(&format!("${}", literals.len() + 1)),
            "claim_task_query() has more than {} binds; extend `literals` in \
             explain_claim before the EXPLAIN can be trusted",
            literals.len(),
        );
        let mut sql = raw.to_string();
        for (i, literal) in literals.iter().enumerate().rev() {
            sql = sql.replace(&format!("${}", i + 1), literal);
        }

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

    /// How many seeded rows actually carry each gate's trigger column.
    ///
    /// The per-gate cost table in `docs/performance.md` is only meaningful if
    /// each scenario genuinely puts its predicate on the execution path. A
    /// scenario that silently stopped setting its column would still claim every
    /// row at a 100% ratio — the claim would just take the cheap `IS NULL` leg —
    /// so "did it claim?" cannot detect that failure. This census can.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SeedCensus {
        /// Rows with `required_build_id` set (build-routing filter, #171).
        pub with_build_id: i64,
        /// Rows with `concurrency_key` set (per-key concurrency, #247).
        pub with_concurrency_key: i64,
        /// Rows with `rate_limit_key` set (rate-limit gate, #332 / #699).
        pub with_rate_limit_key: i64,
        /// Pending rows belonging to a PAUSED execution (pause skip, #383).
        pub paused_ballast: i64,
        /// `harvest_build_compat` declarations, without which a build-routed
        /// claim would take the cheap equality leg instead of the `EXISTS`.
        pub build_compat_rows: i64,
        /// Pending rows *not* blocked by a PAUSED execution — i.e. the rows a
        /// claim can actually take. Load-bearing for the `double_backlog`
        /// control: if it silently seeded one backlog instead of two, it would
        /// stop being an equal-depth comparand and the `paused_rows`
        /// attribution would quietly go back to conflating depth with
        /// predicate cost.
        pub claimable_rows: i64,
    }

    /// Census the currently-seeded backlog.
    ///
    /// # Panics
    /// Panics if any census query fails.
    pub async fn seed_census(conn: &mut AsyncPgConnection) -> SeedCensus {
        async fn count(conn: &mut AsyncPgConnection, sql: &str) -> i64 {
            let rows: Vec<CountRow> = diesel::sql_query(sql)
                .load(conn)
                .await
                .unwrap_or_else(|e| panic!("census query failed: {e}\n--- sql ---\n{sql}"));
            rows.into_iter().next().map_or(0, |r| r.n)
        }

        SeedCensus {
            with_build_id: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
                 WHERE state = 'PENDING' AND required_build_id IS NOT NULL",
            )
            .await,
            with_concurrency_key: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
                 WHERE state = 'PENDING' AND concurrency_key IS NOT NULL",
            )
            .await,
            with_rate_limit_key: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
                 WHERE state = 'PENDING' AND rate_limit_key IS NOT NULL",
            )
            .await,
            paused_ballast: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue t \
                 JOIN harvest_workflow_executions e ON e.id = t.workflow_exec_id \
                 WHERE t.state = 'PENDING' AND e.state = 'PAUSED'",
            )
            .await,
            build_compat_rows: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_build_compat",
            )
            .await,
            claimable_rows: count(
                conn,
                "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue t \
                 LEFT JOIN harvest_workflow_executions e ON e.id = t.workflow_exec_id \
                 WHERE t.state = 'PENDING' \
                   AND (e.id IS NULL OR e.state <> 'PAUSED')",
            )
            .await,
        }
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
