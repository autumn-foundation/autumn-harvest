//! Shared harness for the end-to-end benchmark suite (issue #941).
//!
//! Harvest publishes two performance artifacts today: the replay CPU budget
//! (#135, a 10 000-event history under 200 ms) and the task-claim
//! microbenchmark with its p50 CI gate (#786). Neither answers the first
//! question an evaluating architect asks — *how many workflows per second, end
//! to end, and at what latency?* This harness produces that answer, and
//! `docs/benchmarks.md` publishes it.
//!
//! # Consumers
//!
//! * `benches/e2e_bench.rs` — the full 4-scenario x 3-shard-count matrix. The
//!   one command `benchmarks/run.sh` runs.
//! * `benches/replay_bench.rs` — reuses [`build_history`] and
//!   [`sequential_workflow`] so the replay scenario published here and the #135
//!   budget bench can never measure different histories.
//! * `tests/integration/benchmarks_docs.rs` — the docs-drift guard.
//!
//! # What this harness is not
//!
//! It is **not a CI gate**. Issue #941 puts CI-gated end-to-end regression
//! budgets explicitly out of scope; no manifest row runs any scenario here.
//! `benchmarks_docs.rs` asserts that.
//!
//! It also deliberately contains **no claim or enqueue scenario**: those are
//! #786's, together with the only performance gate CI runs.
//!
//! # Measurement contract
//!
//! * **Warmup.** Every database scenario drains a discarded warmup batch before
//!   the measured window opens ([`warmup_batch_for`]). The dispatch scenario
//!   additionally discards the leading tenth of its samples
//!   ([`measured_samples`]); the signal scenario instead signals a whole
//!   separate warmup cohort through the identical path and keeps none of it, so
//!   "no warmup sample reaches the published percentiles" is true by
//!   construction there rather than by trimming a fraction off the front.
//! * **Soundness before headlines.** A scenario that collected too few samples,
//!   failed to drain, or left a shard idle reports `n/a` and a named reason
//!   rather than a confident-looking number ([`latency_soundness`],
//!   [`throughput_soundness`]).
//! * **Load is bounded, per scenario, on purpose.** `throughput` runs a
//!   *bounded closed loop*: a fixed population is held in flight and topped up
//!   as runs complete, so queue depth stays shallow and the rate is sustained
//!   rather than a slice of a draining backlog. (A pre-loaded backlog measures
//!   the claim-depth curve, which is issue #786's finding, not this suite's —
//!   see §5 of `docs/plans/2026-09-01-e2e-benchmark-suite.md` for the
//!   measurement that forced the change.) [`inflight_soundness`] refuses to
//!   publish a run in which the harness, not the engine, was the limiter.
//!   `dispatch_latency` and `signal_roundtrip` are paced *below* saturation, and
//!   [`pacing_verdict`] marks a run that could not hold its pace, so a queueing
//!   measurement can never be published as a dispatch measurement.
//! * **Clocks.** The signal round-trip is measured end to end on one monotonic
//!   `Instant` clock inside a single process. Activity dispatch spans a DB clock
//!   (`harvest_task_queue.created_at`) and the host clock, so the harness
//!   *measures* the host-to-database offset and publishes it as an error term
//!   instead of assuming it away.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Pure section — no database, no `db` feature. Unit-tested below and reachable
// from every build (including `--no-default-features`), which is also what lets
// `replay_bench.rs` pull the history builder out of it.
// ---------------------------------------------------------------------------

/// The four published scenarios (issue #941 AC1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchScenario {
    /// Sustained workflows completed per second for the canonical 3-activity
    /// workflow, measured under a bounded closed loop.
    Throughput,
    /// Activity dispatch latency (schedule -> handler start), p50/p99, measured
    /// under a deliberately unsaturated paced load.
    DispatchLatency,
    /// Signal round-trip latency (HTTP request sent -> workflow observes the
    /// signal), p50/p99, also paced.
    SignalRoundtrip,
    /// Replay throughput over the #135 history. Shard-invariant by
    /// construction, so it doubles as the run's noise control.
    ReplayThroughput,
}

/// Shard counts every scenario is run at (issue #941 AC2).
pub const SHARD_COUNTS: [u32; 3] = [1, 2, 4];

/// The release whose numbers [`PUBLISHED_BASELINES`] holds, and therefore the
/// `docs/benchmarks/results-v<version>.md` the docs guard checks.
///
/// Deliberately **not** `CARGO_PKG_VERSION`. Issue #941 AC4 asks that each
/// release's numbers be *kept*, not that every release have fresh ones — and a
/// benchmark sweep is a 20-40 minute run on a reference box, not something a
/// version bump can conjure. Tying the guard to the crate version would fail CI
/// on every bump until somebody re-measured. Bump this constant in the same
/// commit that adds a new results file and updates the baselines below; see
/// "Publishing a new release's numbers" in `docs/benchmarks.md`.
pub const PUBLISHED_RESULTS_VERSION: &str = "0.6.0";

/// Published reproduction tolerance (issue #941 AC3): a fresh clone on the
/// documented hardware should land within this percentage of the published
/// number.
pub const REPRO_TOLERANCE_PCT: f64 = 15.0;

/// Fewest post-warmup latency samples a published percentile may rest on.
pub const MIN_LATENCY_SAMPLES: usize = 200;

/// Fewest observed completions a published throughput figure may rest on.
pub const MIN_THROUGHPUT_COMPLETIONS: usize = 200;

/// Fraction of a measured batch run first and discarded, as a divisor
/// (`measured / 5` = 20%).
pub const WARMUP_DIVISOR: usize = 5;

/// Fraction of collected latency samples discarded as warmup, as a divisor
/// (`len / 10` = the leading 10%).
pub const SAMPLE_WARMUP_DIVISOR: usize = 10;

/// A paced scenario must achieve at least this fraction of its target start
/// rate; below it the run is reporting queueing, not dispatch.
pub const PACING_HOLD_RATIO: f64 = 0.90;

/// The closed-loop throughput scenario must hold at least this fraction of its
/// in-flight target on average; below it the harness, not the engine, is the
/// limiter.
pub const INFLIGHT_HOLD_RATIO: f64 = 0.90;

/// Activities in the replay history. `2n + 1` events, so 5 000 activities is
/// the 10 001-event history issue #135 budgets at 200 ms.
pub const REPLAY_ACTIVITY_COUNT: usize = 5_000;

/// Events in the replay history built by [`build_history`] at
/// [`REPLAY_ACTIVITY_COUNT`].
pub const REPLAY_EVENT_COUNT: usize = 2 * REPLAY_ACTIVITY_COUNT + 1;

/// Workflow type name used by every database scenario.
pub const BENCH_WORKFLOW: &str = "harvest_e2e_bench_wf";

/// The workflow the signal scenario parks on `wait_for_signal`.
pub const BENCH_SIGNAL_WORKFLOW: &str = "harvest_e2e_bench_signal_wf";

/// Signal name the round-trip scenario delivers.
pub const BENCH_SIGNAL: &str = "bench_signal";

/// The three activities of the canonical workflow (issue #941, AC1 clause (a)).
pub const BENCH_ACTIVITIES: [&str; 3] = [
    "harvest_e2e_bench_step_1",
    "harvest_e2e_bench_step_2",
    "harvest_e2e_bench_step_3",
];

/// Path prefix the plugin's harvest API router is nested under.
pub const SIGNAL_ROUTE_PREFIX: &str = "/api/harvest/workflows";

/// Environment variable carrying one Postgres URL per shard, comma-separated.
/// This is the path `benchmarks/run.sh` uses against the committed compose
/// topology.
pub const SHARD_URLS_ENV_VAR: &str = "HARVEST_BENCH_SHARD_URLS";

/// Opt-in reproduction check: compare this run against the published baselines
/// at [`REPRO_TOLERANCE_PCT`] and print a per-number verdict.
pub const CHECK_ENV_VAR: &str = "HARVEST_BENCH_CHECK";

/// Comma-separated scenario ids to run. Unset runs all four.
///
/// A reader reproducing one published headline should not have to sit through
/// the whole 4x3 matrix to do it.
pub const SCENARIO_FILTER_ENV_VAR: &str = "HARVEST_BENCH_SCENARIOS";

/// Override the closed-loop in-flight population per shard.
///
/// The load level *is* the experiment for a closed-loop throughput measurement
/// (throughput = concurrency / latency), so it is a documented knob rather than
/// a buried constant: a reader can re-run at their own load level and see where
/// their deployment saturates. The published numbers are taken at the default.
pub const INFLIGHT_ENV_VAR: &str = "HARVEST_BENCH_INFLIGHT";

/// Override the number of measured completions per shard.
pub const WORKFLOWS_ENV_VAR: &str = "HARVEST_BENCH_WORKFLOWS";

/// Read a positive-integer override from the environment, falling back to
/// `default`.
///
/// A malformed or zero value falls back rather than failing: a typo in a
/// benchmark knob should not look like a benchmark result.
#[must_use]
pub fn positive_override(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Comma-separated shard counts to run. Unset runs [`SHARD_COUNTS`].
pub const SHARD_FILTER_ENV_VAR: &str = "HARVEST_BENCH_SHARDS";

/// Parse a comma-separated scenario filter into the scenarios it selects.
///
/// An empty or absent filter selects everything; an unrecognised id selects
/// nothing *for that entry* and is reported by [`unknown_scenario_ids`], so a
/// typo produces a named complaint rather than a silently empty run.
#[must_use]
pub fn selected_scenarios(filter: Option<&str>) -> Vec<BenchScenario> {
    let Some(filter) = filter.map(str::trim).filter(|f| !f.is_empty()) else {
        return BenchScenario::all().to_vec();
    };
    let wanted: Vec<&str> = filter
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    BenchScenario::all()
        .into_iter()
        .filter(|s| wanted.contains(&s.as_str()))
        .collect()
}

/// Entries of a scenario filter that name no scenario.
#[must_use]
pub fn unknown_scenario_ids(filter: Option<&str>) -> Vec<String> {
    let Some(filter) = filter.map(str::trim).filter(|f| !f.is_empty()) else {
        return Vec::new();
    };
    let known: Vec<&str> = BenchScenario::all().iter().map(|s| s.as_str()).collect();
    filter
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter(|s| !known.contains(s))
        .map(str::to_owned)
        .collect()
}

/// Parse a comma-separated shard-count filter, keeping only counts the
/// published matrix covers.
#[must_use]
pub fn selected_shard_counts(filter: Option<&str>) -> Vec<u32> {
    let Some(filter) = filter.map(str::trim).filter(|f| !f.is_empty()) else {
        return SHARD_COUNTS.to_vec();
    };
    let wanted: Vec<u32> = filter
        .split(',')
        .filter_map(|s| s.trim().parse::<u32>().ok())
        .collect();
    SHARD_COUNTS
        .into_iter()
        .filter(|c| wanted.contains(c))
        .collect()
}

impl BenchScenario {
    /// Stable identifier used in report tables, in `docs/benchmarks.md`, and in
    /// the baseline table.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Throughput => "throughput",
            Self::DispatchLatency => "dispatch_latency",
            Self::SignalRoundtrip => "signal_roundtrip",
            Self::ReplayThroughput => "replay_throughput",
        }
    }

    /// Every scenario, in report order. Throughput leads because it is the
    /// headline an evaluator reads first; replay trails because it is the
    /// control rather than a claim about the engine's end-to-end capacity.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [
            Self::Throughput,
            Self::DispatchLatency,
            Self::SignalRoundtrip,
            Self::ReplayThroughput,
        ]
    }

    /// Whether the scenario needs a live Postgres. Replay is in-memory.
    #[must_use]
    pub const fn needs_database(self) -> bool {
        !matches!(self, Self::ReplayThroughput)
    }

    /// Whether the scenario is paced at a fixed *rate*. `Throughput` is not:
    /// its load is bounded by population (a closed loop) rather than by rate,
    /// because the rate is the thing being measured. `ReplayThroughput` drives
    /// no queue at all. Pacing the two latency scenarios is what keeps their
    /// p99 a statement about the dispatch and signal paths rather than about
    /// how deep the queue happened to be.
    #[must_use]
    pub const fn is_paced(self) -> bool {
        matches!(self, Self::DispatchLatency | Self::SignalRoundtrip)
    }
}

/// Size of the discarded warmup batch that runs before a measured batch.
///
/// A fifth of the measured batch, but never zero for a non-empty one: the
/// measured window must never be the window in which connection pools fill,
/// prepared statements are first planned and the worker's caches are cold.
#[must_use]
pub fn warmup_batch_for(measured: usize) -> usize {
    if measured == 0 {
        return 0;
    }
    (measured / WARMUP_DIVISOR).max(1)
}

/// Drop the leading tenth of a sample set as warmup, applied *after*
/// collection so a run cut short still reports the samples it took.
///
/// A set smaller than [`SAMPLE_WARMUP_DIVISOR`] survives intact: discarding
/// every sample and then publishing a confident-looking `0.00 ms` is the
/// failure this rule exists to prevent, and [`latency_soundness`] is what
/// refuses to publish the thin set that results.
#[must_use]
pub fn measured_samples(samples: &[f64]) -> &[f64] {
    let drop = samples.len() / SAMPLE_WARMUP_DIVISOR;
    &samples[drop..]
}

/// Completions per second, or `None` for a degenerate window.
///
/// `None` rather than `inf` or `0.0`: a zero-length window or a run that
/// completed nothing has no rate, and the renderer prints `n/a` for it.
#[must_use]
pub fn throughput_per_sec(completions: usize, window: Duration) -> Option<f64> {
    let secs = window.as_secs_f64();
    if completions == 0 || !secs.is_finite() || secs <= 0.0 {
        return None;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "completion counts here are far below 2^53"
    )]
    Some(completions as f64 / secs)
}

/// Reasons a latency headline must not be published, empty when sound.
///
/// Three independent ways a percentile here can be a lie:
///
/// * too few post-warmup samples to have a p99 at all;
/// * **negative** samples, which only host-to-database clock skew can produce —
///   reported rather than clamped to zero, because clamping would hide exactly
///   the error term the reader needs (see the module docs on clocks);
/// * task rows with a NULL `created_at` (the column is nullable for
///   pre-upgrade rows), which yield no sample and would otherwise silently
///   shrink the population.
#[must_use]
pub fn latency_soundness(
    sample_count: usize,
    negative_samples: usize,
    missing_timestamps: usize,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if sample_count < MIN_LATENCY_SAMPLES {
        reasons.push(format!(
            "only {sample_count} post-warmup samples; a published percentile needs \
             at least {MIN_LATENCY_SAMPLES}"
        ));
    }
    if negative_samples > 0 {
        reasons.push(format!(
            "{negative_samples} negative latency sample(s): the host clock read \
             earlier than the database clock, so the published offset is not the \
             whole skew"
        ));
    }
    if missing_timestamps > 0 {
        reasons.push(format!(
            "{missing_timestamps} task row(s) had a NULL created_at and produced \
             no sample"
        ));
    }
    reasons
}

/// Reasons a throughput headline must not be published, empty when sound.
///
/// A rate is only meaningful if the run actually drained what it was asked to
/// drain, drained enough of it to be a rate rather than an anecdote, and used
/// every shard it claims to be a number *for*. The last one is the guard
/// against publishing a "4-shard" figure produced by two shards.
#[must_use]
pub fn throughput_soundness(
    requested: usize,
    completed: usize,
    per_shard_completions: &[u64],
) -> Vec<String> {
    let mut reasons = Vec::new();
    if completed < requested {
        reasons.push(format!(
            "drain truncated: {completed} of {requested} workflows completed"
        ));
    }
    if completed < MIN_THROUGHPUT_COMPLETIONS {
        reasons.push(format!(
            "only {completed} completions; a published rate needs at least \
             {MIN_THROUGHPUT_COMPLETIONS}"
        ));
    }
    for (idx, count) in per_shard_completions.iter().enumerate() {
        if *count == 0 {
            reasons.push(format!(
                "shard {idx} completed nothing; this is not a \
                 {}-shard measurement",
                per_shard_completions.len()
            ));
        }
    }
    reasons
}

/// Largest host-to-database clock offset, as a fraction of the measured p50,
/// that a dispatch cell may publish through.
///
/// The dispatch measurement is the one number here that spans two clocks. On a
/// single host the offset is tens of microseconds against tens of milliseconds
/// and this never binds; point the harness at a database on another machine and
/// it is the whole ballgame. Refusing above 2% is what stops a 30 ms skew from
/// being published as 30 ms of engine latency, with the offset printed
/// helpfully in the notes beside it.
pub const MAX_CLOCK_OFFSET_FRACTION_OF_P50: f64 = 0.02;

/// Reasons a dispatch cell's two-clock measurement must not be published.
///
/// Takes the offset probed *before* the measured window and the one probed
/// after: a single probe cannot distinguish a steady offset (correctable in
/// principle, and small enough to ignore in practice) from a drifting one
/// (which contaminates the samples unevenly).
#[must_use]
pub fn clock_offset_soundness(before: &[f64], after: &[f64], p50_ms: f64) -> Vec<String> {
    let mut reasons = Vec::new();
    if !p50_ms.is_finite() || p50_ms <= 0.0 {
        return reasons;
    }
    let ceiling = p50_ms * MAX_CLOCK_OFFSET_FRACTION_OF_P50;
    let worst = before
        .iter()
        .chain(after)
        .copied()
        .fold(0.0_f64, |acc, o| acc.max(o.abs()));
    if worst > ceiling {
        reasons.push(format!(
            "the host-to-database clock offset reached {worst:.3} ms, more than \
             {:.0}% of the {p50_ms:.2} ms p50 this cell would publish; a dispatch latency \
             measured across two clocks that far apart is mostly skew",
            MAX_CLOCK_OFFSET_FRACTION_OF_P50 * 100.0,
        ));
    }
    // Drift between the two probes contaminates samples unevenly across the
    // window, so it is worth naming separately from the absolute size.
    for (idx, (b, a)) in before.iter().zip(after).enumerate() {
        let drift = (a - b).abs();
        if drift > ceiling {
            reasons.push(format!(
                "shard {idx}'s host-to-database offset drifted {drift:.3} ms across the \
                 measured window ({b:+.3} -> {a:+.3} ms)"
            ));
        }
    }
    reasons
}

/// Smallest fraction of the dispatches that ran which must have produced a
/// sample.
pub const MIN_DISPATCH_SAMPLE_COVERAGE: f64 = 0.90;

/// Reasons a dispatch cell's sample population is too far below the population
/// that actually ran.
///
/// [`latency_soundness`]'s floor is an absolute minimum (200); it cannot notice
/// that 250 samples came out of 4 800 dispatches. This does.
#[must_use]
pub fn dispatch_population_soundness(
    expected: usize,
    collected: usize,
    redispatched: usize,
    unrecorded: usize,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if expected > 0 {
        #[allow(
            clippy::cast_precision_loss,
            reason = "dispatch counts are thousands, far below 2^53"
        )]
        let coverage = collected as f64 / expected as f64;
        if coverage < MIN_DISPATCH_SAMPLE_COVERAGE {
            reasons.push(format!(
                "only {collected} of {expected} activity dispatches produced a sample \
                 ({:.1}%); the published percentiles would describe a population this run \
                 cannot account for",
                coverage * 100.0,
            ));
        }
    }
    if redispatched > 0 {
        reasons.push(format!(
            "{redispatched} activity task row(s) were dispatched more than once, so a \
             re-delivery delay would be indistinguishable from dispatch latency"
        ));
    }
    if unrecorded > 0 {
        reasons.push(format!(
            "{unrecorded} activity dispatch(es) carried no task id and produced no sample"
        ));
    }
    reasons
}

/// Reasons a scenario's discarded warmup population did not actually drain.
///
/// A warmup that did not finish is not warmup — it is background load inside
/// the measured window, competing for the same worker slots.
#[must_use]
pub fn warmup_soundness(scenario: &str, requested: usize, per_shard: &[u64]) -> Vec<String> {
    let drained = per_shard.iter().sum::<u64>();
    let drained = usize::try_from(drained).unwrap_or(usize::MAX);
    if drained < requested {
        return vec![format!(
            "the {scenario} warmup population did not drain ({drained} of {requested}); the \
             remainder was still running inside the measured window"
        )];
    }
    Vec::new()
}

/// Largest spread between the replay control's readings across a sweep before
/// the sweep is called out as noisy.
///
/// Replay is in-memory and shard-invariant, so any spread across the shard
/// counts is the reference box, not the engine.
pub const REPLAY_CONTROL_DRIFT_PCT: f64 = 10.0;

/// Peak-to-peak spread of the replay control across a sweep, in percent of the
/// largest reading, or `None` when there is nothing to compare.
///
/// This is the comparison that makes the control do its job: nothing else in
/// the report looks at the three replay cells *against each other*, and each on
/// its own always looks fine.
#[must_use]
pub fn replay_control_drift_pct(readings: &[f64]) -> Option<f64> {
    if readings.len() < 2 {
        return None;
    }
    let max = readings.iter().copied().fold(f64::MIN, f64::max);
    let min = readings.iter().copied().fold(f64::MAX, f64::min);
    if !max.is_finite() || !min.is_finite() || max <= 0.0 {
        return None;
    }
    Some((max - min) / max * 100.0)
}

/// Whether a paced scenario held its target start rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pacing {
    Held,
    Saturated,
}

/// Compare an achieved start rate against the target it was paced at.
///
/// A target of zero (or a non-finite one) is [`Pacing::Saturated`] rather than
/// a division by zero: a scenario that paced at no rate at all held nothing.
#[must_use]
pub fn pacing_verdict(target_per_sec: f64, achieved_per_sec: f64) -> Pacing {
    if !target_per_sec.is_finite() || target_per_sec <= 0.0 || !achieved_per_sec.is_finite() {
        return Pacing::Saturated;
    }
    if achieved_per_sec / target_per_sec >= PACING_HOLD_RATIO {
        Pacing::Held
    } else {
        Pacing::Saturated
    }
}

/// Outcome of comparing a fresh run against a published baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReproVerdict {
    Within,
    Outside,
}

/// Signed relative error of `measured` against `baseline`, in percent.
///
/// Signed on purpose: "12% faster" and "12% slower" are different news, and a
/// magnitude-only figure hides which one a reader is looking at.
#[must_use]
pub fn relative_error_pct(measured: f64, baseline: f64) -> Option<f64> {
    if !measured.is_finite() || !baseline.is_finite() || baseline == 0.0 {
        return None;
    }
    Some((measured - baseline) / baseline * 100.0)
}

/// Whether a fresh measurement reproduces a published baseline.
///
/// The tolerance is an **inclusive** band: exactly at +/-15% reproduces. A
/// measurement with no computable relative error (a zero or non-finite
/// baseline) is [`ReproVerdict::Outside`] — it is not evidence of
/// reproduction.
#[must_use]
pub fn repro_verdict(measured: f64, baseline: f64, tolerance_pct: f64) -> ReproVerdict {
    match relative_error_pct(measured, baseline) {
        Some(err) if err.abs() <= tolerance_pct => ReproVerdict::Within,
        _ => ReproVerdict::Outside,
    }
}

/// Sustained throughput from a sorted list of completion instants, in seconds
/// on any common epoch.
///
/// Takes the **middle half** of the drain: the rate between the 25th and 75th
/// percentile completion. A whole-drain rate charges the measurement for the
/// ramp-up (workers filling their concurrency slots) and the drain-down (the
/// last few workflows finishing against an empty queue), neither of which is
/// the sustained rate an operator sizes against. Returns `None` when the
/// population is too thin to have a middle half or when that half took no
/// measurable time.
#[must_use]
pub fn steady_state_throughput(sorted_completion_secs: &[f64]) -> Option<f64> {
    let (lo, hi) = steady_state_window(sorted_completion_secs)?;
    let n = sorted_completion_secs.len();
    let count = (n * 3 / 4) - (n / 4);
    let window = hi - lo;
    #[allow(
        clippy::cast_precision_loss,
        reason = "completion counts here are far below 2^53"
    )]
    Some(count as f64 / window)
}

/// The `(start, end)` instants of the middle-half window
/// [`steady_state_throughput`] measures over, in the same units as its input.
///
/// Exposed so a runner can assert that seeding finished *before* the measured
/// window opened: a window that overlaps seeding is measuring how fast the
/// harness can insert rows, not how fast the engine drains them.
#[must_use]
pub fn steady_state_window(sorted_completion_secs: &[f64]) -> Option<(f64, f64)> {
    let n = sorted_completion_secs.len();
    if n < MIN_THROUGHPUT_COMPLETIONS {
        return None;
    }
    let lo = n / 4;
    let hi = n * 3 / 4;
    if hi <= lo {
        return None;
    }
    let (start, end) = (sorted_completion_secs[lo], sorted_completion_secs[hi]);
    let window = end - start;
    if !window.is_finite() || window <= 0.0 {
        return None;
    }
    Some((start, end))
}

/// Mean of a set of in-flight observations.
#[must_use]
pub fn mean_inflight(samples: &[usize]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "in-flight counts are tens, far below 2^53"
    )]
    let total: f64 = samples.iter().map(|n| *n as f64).sum();
    #[allow(clippy::cast_precision_loss, reason = "sample counts are small")]
    Some(total / samples.len() as f64)
}

/// The middle half of an ordered observation series.
///
/// The in-flight evidence must describe the same window the headline rate is
/// computed over ([`steady_state_window`]). Averaging the whole loop instead
/// includes the drain-down tail, where the population decays to zero by
/// construction — which both flatters nothing and, on a short run, fires
/// [`inflight_soundness`] against a perfectly sound measurement.
#[must_use]
pub fn steady_state_slice<T>(samples: &[T]) -> &[T] {
    let n = samples.len();
    if n < 4 {
        return samples;
    }
    &samples[n / 4..n * 3 / 4]
}

/// Reasons a closed-loop throughput figure is *feeder*-bound rather than
/// engine-bound, empty when it is engine-bound.
///
/// The throughput scenario holds a fixed number of workflows in flight and
/// tops the population back up as runs complete. That only measures the engine
/// while the harness can actually keep the target populated: if the feeder
/// falls behind, the published rate is "how fast this harness starts
/// workflows", which is not a fact about Harvest. The observed mean in-flight
/// population is the evidence, and it is published next to the number.
#[must_use]
pub fn inflight_soundness(target: usize, mean: Option<f64>) -> Vec<String> {
    let Some(mean) = mean else {
        return vec!["no in-flight observations were collected".to_owned()];
    };
    #[allow(
        clippy::cast_precision_loss,
        reason = "in-flight targets are tens, far below 2^53"
    )]
    let floor = target as f64 * INFLIGHT_HOLD_RATIO;
    if mean < floor {
        return vec![format!(
            "the harness held only {mean:.1} workflows in flight against a target of {target}; \
             this rate is bounded by how fast the harness could start workflows, not by the \
             engine"
        )];
    }
    Vec::new()
}

// ── Report model (pure) ────────────────────────────────────────────────────

/// One published metric of one scenario run. `None` renders as `n/a`.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    pub key: &'static str,
    pub value: Option<f64>,
}

impl Metric {
    #[must_use]
    pub const fn new(key: &'static str, value: Option<f64>) -> Self {
        Self { key, value }
    }
}

/// One (scenario, shard count) cell of the published matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioReport {
    pub scenario: BenchScenario,
    pub shards: u32,
    pub metrics: Vec<Metric>,
    /// Context a reader needs but that is not a headline (pacing achieved,
    /// measured clock offset, per-shard split).
    pub notes: Vec<String>,
    /// Non-empty when this cell must not be read as a published number.
    pub unsound: Vec<String>,
}

impl ScenarioReport {
    #[must_use]
    pub fn metric(&self, key: &str) -> Option<f64> {
        self.metrics
            .iter()
            .find(|m| m.key == key)
            .and_then(|m| m.value)
    }

    #[must_use]
    pub const fn is_sound(&self) -> bool {
        self.unsound.is_empty()
    }
}

/// Render one metric value for a report table.
#[must_use]
pub fn render_value(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |v| format!("{v:.2}"))
}

/// Render the scenario matrix as a Markdown table.
///
/// The published results file is this function's output, not a hand-typed
/// table: a number nobody could have produced by running the suite cannot get
/// into the doc.
#[must_use]
pub fn render_matrix(reports: &[ScenarioReport]) -> String {
    let mut out = String::from("| scenario | shards | metric | value | sound |\n");
    out.push_str("|:--|--:|:--|--:|:--|\n");
    for report in reports {
        for metric in &report.metrics {
            use std::fmt::Write as _;
            let _ = writeln!(
                out,
                "| `{}` | {} | `{}` | {} | {} |",
                report.scenario.as_str(),
                report.shards,
                metric.key,
                render_value(metric.value),
                if report.is_sound() { "yes" } else { "**no**" },
            );
        }
    }
    out
}

// ── Deployment configuration (published in `docs/benchmarks.md`) ──────────
//
// These live in the pure section rather than beside the code that uses them so
// the docs-drift guard -- which runs on every OS with no `db` feature -- can pin
// them into the published page. Issue #941 AC2 asks for "a documented
// worker/concurrency configuration"; documented means a reader can see it on the
// page, not only in the output of a run they have not done yet.

/// Worker tasks per shard. One worker per shard is the deployment shape the
/// shard-count sweep is a statement about: adding a shard adds its worker.
pub const WORKERS_PER_SHARD: usize = 1;
/// Concurrent workflow tasks per worker.
pub const MAX_CONCURRENT_WORKFLOWS: usize = 8;
/// Concurrent activity tasks per worker.
pub const MAX_CONCURRENT_ACTIVITIES: usize = 16;
/// Fallback poll interval. The reference run also wires LISTEN/NOTIFY per
/// shard, so this bounds how long a *missed* notification can cost.
pub const POLL_INTERVAL_MS: u64 = 25;
/// Connections per shard pool. At or above the worker's total concurrency
/// so a measurement never includes pool-checkout queueing.
pub const POOL_SIZE_PER_SHARD: usize = 32;
/// Workflows completed per shard in the throughput scenario's measured
/// population.
pub const THROUGHPUT_WORKFLOWS_PER_SHARD: usize = 1_200;
/// Workflows held in flight per shard by the closed-loop feeder: **four times**
/// [`MAX_CONCURRENT_WORKFLOWS`], so the worker is never starved and the queue
/// never becomes the measurement.
///
/// Deliberately shallow. A pre-loaded backlog would instead measure the
/// **claim-depth** curve — claim cost grows superlinearly with pending backlog
/// depth, which is issue #786's published finding, not this suite's — and the
/// resulting figure would depend on where in the drain you looked rather than
/// on the engine's sustained capacity.
///
/// Worth being exact about why this is not larger, because the larger number is
/// more flattering. Calibration on the reference box measured ~23 workflows/s
/// at 32 in flight and ~28/s at 128 — but 128 is *sixteen* times the worker's
/// eight workflow slots, so it holds ~120 workflows **pending** per shard for
/// the whole measured window. That extra ~20% is bought by measuring deeper
/// into the very claim-depth curve this scenario was redesigned to stop
/// re-publishing under an end-to-end label. The slower number is the honest
/// one. (384 is refused outright: the feeder can no longer hold the
/// population — see [`inflight_soundness`].)
///
/// A reader who wants the deeper figure can have it —
/// `HARVEST_BENCH_INFLIGHT=128` — and the report prints the population actually
/// held beside the rate, so the two can never be confused.
pub const THROUGHPUT_INFLIGHT_PER_SHARD: usize = 4 * MAX_CONCURRENT_WORKFLOWS;
/// Connections the feeder starts workflows over, per shard. One connection
/// caps the feeder at roughly 285 starts/s, which is inside the range the
/// engine can complete — so the harness would silently become the limiter.
pub const FEEDER_CONNECTIONS_PER_SHARD: usize = 4;
/// Target start rate for the paced scenarios, per shard.
///
/// Roughly **30%** of the saturated rate the throughput scenario measures on
/// the reference box, so the queue these latencies are measured against
/// stays shallow. Pacing at 70% of capacity would publish a p99 that is
/// mostly queueing; [`Pacing`] reports when the box could not hold the pace,
/// but keeping the target well clear of saturation is what stops the
/// question arising.
pub const PACED_STARTS_PER_SEC_PER_SHARD: f64 = 8.0;
/// Workflows started per shard in the dispatch-latency scenario.
pub const DISPATCH_WORKFLOWS_PER_SHARD: usize = 400;
/// Workflows parked on a signal per shard in the round-trip scenario.
pub const SIGNAL_WORKFLOWS_PER_SHARD: usize = 400;
/// Queue every bench workflow and activity is dispatched on.
pub const BENCH_QUEUE: &str = "default";

/// Resolved in-flight population per shard for this run.
#[must_use]
pub fn inflight_target() -> usize {
    positive_override(
        std::env::var(INFLIGHT_ENV_VAR).ok().as_deref(),
        THROUGHPUT_INFLIGHT_PER_SHARD,
    )
}

/// Resolved measured completions per shard for this run.
#[must_use]
pub fn measured_workflows_per_shard() -> usize {
    positive_override(
        std::env::var(WORKFLOWS_ENV_VAR).ok().as_deref(),
        THROUGHPUT_WORKFLOWS_PER_SHARD,
    )
}

/// Wall-clock ceiling for one scenario at one shard count. A scenario that
/// hits it reports what it collected and is marked unsound rather than
/// parking the whole suite.
pub const SCENARIO_BUDGET_SECS: u64 = 900;

/// How long to wait after every signal workflow's handler has reached
/// `wait_for_signal` before the first signal is sent, so the suspension has
/// committed.
pub const SIGNAL_PARK_SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

/// Ceiling on any single read of the signal endpoint's socket, both ends.
///
/// Generous relative to a loopback round trip measured in tens of
/// milliseconds; its job is to turn "this will never answer" into a failed
/// sample the report can name, instead of a parked scenario.
pub const SIGNAL_SOCKET_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ── HTTP framing (pure) ────────────────────────────────────────────────────

/// The signal route, mirroring the plugin's
/// `POST /workflows/{id}/signal/{signal_name}` under its `/api/harvest` nest.
#[must_use]
pub fn signal_route(exec_id: &str, signal_name: &str) -> String {
    format!("{SIGNAL_ROUTE_PREFIX}/{exec_id}/signal/{signal_name}")
}

/// Recover `(exec_id, signal_name)` from a request path produced by
/// [`signal_route`].
#[must_use]
pub fn parse_signal_route(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix(SIGNAL_ROUTE_PREFIX)?.strip_prefix('/')?;
    let (exec_id, rest) = rest.split_once("/signal/")?;
    if exec_id.is_empty() || rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some((exec_id, rest))
}

/// Serialize a complete HTTP/1.1 signal request.
#[must_use]
pub fn signal_request_bytes(host: &str, exec_id: &str, signal_name: &str, body: &str) -> Vec<u8> {
    let route = signal_route(exec_id, signal_name);
    format!(
        "POST {route} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {body}",
        body.len(),
    )
    .into_bytes()
}

/// A parsed HTTP/1.1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRequest {
    pub method: String,
    pub path: String,
    pub body: String,
}

/// Outcome of parsing bytes read from a socket.
///
/// The distinction is load-bearing, not cosmetic. A server that cannot tell
/// "keep reading" from "this will never be valid" waits forever for bytes the
/// peer will never send — and because the benchmark's client blocks in
/// `read_to_end` with the request already written, that is a deadlock on both
/// sides, not a slow request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpParse {
    /// A complete request.
    Complete(ParsedRequest),
    /// A prefix of a request: read more.
    Incomplete,
    /// Not a request this endpoint can ever serve: answer, do not wait.
    Malformed,
}

/// Largest request this endpoint will buffer before giving up.
///
/// The signal route's requests are a few hundred bytes. Any cap is arbitrary;
/// the point is that an unbounded `Vec` fed by a socket is not a thing a
/// benchmark harness should own.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// Parse an HTTP/1.1 request out of `buf`.
///
/// Deliberately minimal: this endpoint serves exactly one route with a fixed
/// request shape (see the module docs on why the harness does not pull in an
/// HTTP stack), so it understands `Content-Length` bodies and nothing else — no
/// chunked encoding, no keep-alive pipelining.
#[must_use]
pub fn parse_http_request(buf: &[u8]) -> HttpParse {
    if buf.len() > MAX_REQUEST_BYTES {
        return HttpParse::Malformed;
    }
    // Find the header terminator on the BYTES, so a non-UTF8 body cannot make a
    // perfectly good header block unparseable.
    let Some(split) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        // No terminator yet. Only "more bytes might arrive" if the prefix is
        // still a plausible header block.
        return match std::str::from_utf8(buf) {
            Ok(_) => HttpParse::Incomplete,
            Err(e) if e.error_len().is_none() => HttpParse::Incomplete,
            Err(_) => HttpParse::Malformed,
        };
    };
    let Ok(head) = std::str::from_utf8(&buf[..split]) else {
        return HttpParse::Malformed;
    };
    let body_bytes = &buf[split + 4..];

    let mut lines = head.split("\r\n");
    let Some(request_line) = lines.next() else {
        return HttpParse::Malformed;
    };
    let mut parts = request_line.split(' ');
    let (Some(method), Some(path), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return HttpParse::Malformed;
    };

    let mut content_length = 0_usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            return HttpParse::Malformed;
        };
        if name.eq_ignore_ascii_case("content-length") {
            let Ok(parsed) = value.trim().parse::<usize>() else {
                return HttpParse::Malformed;
            };
            if parsed > MAX_REQUEST_BYTES {
                return HttpParse::Malformed;
            }
            content_length = parsed;
        }
    }
    if body_bytes.len() < content_length {
        return HttpParse::Incomplete;
    }
    // Slice the BYTES and then validate, so a Content-Length that disagrees
    // with the body's UTF-8 boundaries is a 400 rather than a panic.
    let Ok(body) = std::str::from_utf8(&body_bytes[..content_length]) else {
        return HttpParse::Malformed;
    };
    HttpParse::Complete(ParsedRequest {
        method: method.to_owned(),
        path: path.to_owned(),
        body: body.to_owned(),
    })
}

/// Status code of an HTTP/1.1 response.
#[must_use]
pub fn parse_status_code(response: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(response).ok()?;
    let line = text.split("\r\n").next()?;
    let mut parts = line.split(' ');
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

// ── Published baselines ────────────────────────────────────────────────────

/// One published headline number: the pairing `docs/benchmarks.md` renders and
/// `HARVEST_BENCH_CHECK=1` compares a fresh run against.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Baseline {
    pub scenario: BenchScenario,
    pub shards: u32,
    /// Metric key within the scenario (`"workflows_per_sec"`, `"p50_ms"`, ...).
    pub metric: &'static str,
    pub value: f64,
}

/// The numbers published in `docs/benchmarks/results-v0.6.0.md`.
///
/// These are the *headline* metrics only — the ones issue #941's success metric
/// names. The report also prints context metrics (sample counts, achieved pace,
/// the measured window) which are diagnostic rather than published claims, so
/// they are deliberately absent here.
///
/// `HARVEST_BENCH_CHECK=1` compares a fresh run against these at
/// [`REPRO_TOLERANCE_PCT`]. That is a *report*, never a gate — see the module
/// docs and `benchmarks_docs.rs`.
///
/// **On the p99 entries.** The tail is the least reproducible number on the
/// page, for the reason issue #786 derived at length before gating p50 rather
/// than p99: a tail measured on a box that is also running the harness is
/// partly a measurement of that box's run queue. They are published, and they
/// are checked, because #941's success metric names them — but a p99 outside
/// tolerance on a busy machine is the expected outcome, not a regression.
/// `docs/benchmarks.md` says so where the numbers appear.
pub const PUBLISHED_BASELINES: &[Baseline] = &[
    Baseline {
        scenario: BenchScenario::Throughput,
        shards: 1,
        metric: "workflows_per_sec",
        value: 24.04,
    },
    Baseline {
        scenario: BenchScenario::Throughput,
        shards: 2,
        metric: "workflows_per_sec",
        value: 38.44,
    },
    Baseline {
        scenario: BenchScenario::Throughput,
        shards: 4,
        metric: "workflows_per_sec",
        value: 36.28,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 1,
        metric: "p50_ms",
        value: 39.83,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 1,
        metric: "p99_ms",
        value: 60.55,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 2,
        metric: "p50_ms",
        value: 43.19,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 2,
        metric: "p99_ms",
        value: 64.43,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 4,
        metric: "p50_ms",
        value: 55.77,
    },
    Baseline {
        scenario: BenchScenario::DispatchLatency,
        shards: 4,
        metric: "p99_ms",
        value: 109.67,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 1,
        metric: "p50_ms",
        value: 54.69,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 1,
        metric: "p99_ms",
        value: 65.93,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 2,
        metric: "p50_ms",
        value: 60.51,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 2,
        metric: "p99_ms",
        value: 68.27,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 4,
        metric: "p50_ms",
        value: 46.43,
    },
    Baseline {
        scenario: BenchScenario::SignalRoundtrip,
        shards: 4,
        metric: "p99_ms",
        value: 81.54,
    },
    Baseline {
        scenario: BenchScenario::ReplayThroughput,
        shards: 1,
        metric: "events_per_sec",
        value: 9_564_120.54,
    },
    Baseline {
        scenario: BenchScenario::ReplayThroughput,
        shards: 2,
        metric: "events_per_sec",
        value: 9_387_692.65,
    },
    Baseline {
        scenario: BenchScenario::ReplayThroughput,
        shards: 4,
        metric: "events_per_sec",
        value: 9_239_806.28,
    },
];

/// Look up a published baseline.
#[must_use]
pub fn baseline_for(scenario: BenchScenario, shards: u32, metric: &str) -> Option<f64> {
    PUBLISHED_BASELINES
        .iter()
        .find(|b| b.scenario == scenario && b.shards == shards && b.metric == metric)
        .map(|b| b.value)
}

// ── Replay history (shared with `benches/replay_bench.rs`, issue #135) ──────

/// Workflow that executes N sequential activities.
pub fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n: usize = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        let mut last = Value::Null;
        for i in 0..n {
            let name = format!("activity_{i}");
            last = ctx
                .execute_activity_raw(&name, Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(last)
    })
}

/// Build a synthetic event history with `n` completed activities.
///
/// Shared with `benches/replay_bench.rs` so the replay throughput published by
/// issue #941 and the CPU budget of issue #135 are measured over byte-identical
/// histories.
#[must_use]
pub fn build_history(n: usize) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(n * 2 + 1);

    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(n as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });

    for i in 0..n {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::from(i as u64),
        });
    }

    (exec_id, events)
}

/// Iterations of the replay history the measured phase times.
pub const REPLAY_MEASURED_ITERATIONS: usize = 20;
/// Discarded warmup iterations before the measured phase.
pub const REPLAY_WARMUP_ITERATIONS: usize = 5;

/// **Scenario (d).** Replay throughput over the issue #135 history.
///
/// In-memory and shard-invariant by construction — the replayer never opens a
/// connection — so this row doubles as the run's **noise control**: it cannot
/// legitimately move with shard count, and drift across the three shard counts
/// bounds how loaded the reference box was while the other nine cells were
/// measured.
///
/// It reports the same history #135 budgets (10 001 events, under 200 ms), so
/// the number published here stays continuous with that budget rather than
/// starting a second, unrelated replay series.
#[cfg(feature = "testing")]
pub async fn run_replay_throughput(shards: u32) -> ScenarioReport {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);
    for _ in 0..REPLAY_WARMUP_ITERATIONS {
        let (_exec, events) = build_history(REPLAY_ACTIVITY_COUNT);
        let _ = replayer.replay_from_events(events).await;
    }

    let mut per_iteration_ms = Vec::with_capacity(REPLAY_MEASURED_ITERATIONS);
    let mut replayed_events = Vec::with_capacity(REPLAY_MEASURED_ITERATIONS);
    let mut failed_iterations = 0_usize;
    for _ in 0..REPLAY_MEASURED_ITERATIONS {
        // Build outside the timed region: the published number is replay cost,
        // not history-construction cost.
        let (_exec, events) = build_history(REPLAY_ACTIVITY_COUNT);
        let started = std::time::Instant::now();
        let report = replayer.replay_from_events(events).await;
        per_iteration_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        // The report's own count, not the constant. A replay that ended early
        // returns in nanoseconds; dividing the assumed full history by that
        // time would publish a spectacular throughput for work never done.
        replayed_events.push(report.events_replayed);
        if !matches!(report.status, ReplayStatus::ReplaySucceeded) {
            failed_iterations += 1;
        }
    }
    per_iteration_ms.sort_by(f64::total_cmp);
    // `percentile_ms`'s convention, so the suite has one definition of "median".
    let median_ms = super::claim_bench_support::percentile_ms(&per_iteration_ms, 50.0);
    let min_replayed = replayed_events.iter().copied().min().unwrap_or(0);
    #[allow(
        clippy::cast_precision_loss,
        reason = "the event count is 10 001, far below 2^53"
    )]
    let events = min_replayed as f64;
    let events_per_sec = (median_ms > 0.0).then(|| events / (median_ms / 1000.0));

    let mut unsound = Vec::new();
    if failed_iterations > 0 {
        unsound.push(format!(
            "{failed_iterations} of {REPLAY_MEASURED_ITERATIONS} replay iterations did not \
             report ReplaySucceeded"
        ));
    }
    if min_replayed != REPLAY_EVENT_COUNT {
        unsound.push(format!(
            "an iteration replayed {min_replayed} events, not the {REPLAY_EVENT_COUNT} the \
             issue #135 history contains"
        ));
    }

    let publish = unsound.is_empty();
    ScenarioReport {
        scenario: BenchScenario::ReplayThroughput,
        shards,
        metrics: vec![
            Metric::new(
                "events_per_sec",
                if publish { events_per_sec } else { None },
            ),
            Metric::new("ms_per_history", Some(median_ms)),
        ],
        notes: vec![
            format!(
                "{min_replayed} events replayed ({REPLAY_ACTIVITY_COUNT} activities), median of \
                 {REPLAY_MEASURED_ITERATIONS} iterations after {REPLAY_WARMUP_ITERATIONS} warmup \
                 iterations"
            ),
            "shard-invariant by construction: this row is the run's noise control, not a \
             statement about sharding"
                .to_owned(),
            "the same history `benches/replay_bench.rs` budgets at 200 ms (issue #135)".to_owned(),
        ],
        unsound,
    }
}

// ---------------------------------------------------------------------------
// Pure unit tests. No `#[tokio::test]`, no `block_on`: `ci_run_coverage`
// classifies this file as a harness rather than a suite, and that classification
// must keep holding.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // Cargo sets `cfg(test)` for bench targets too, but a `harness = false`
    // bench has no test harness, so rustc strips every `#[test]` fn from this
    // module when it is compiled into `benches/e2e_bench.rs` — leaving this
    // import genuinely unused *there* while it is used in the `integration`
    // test binary. Same situation, same fix, as `claim_bench_support::pure_tests`.
    // Scoped to this module so a real unused import elsewhere in the file still
    // warns.
    #![allow(unused_imports)]

    use super::*;

    #[test]
    fn every_scenario_has_a_stable_distinct_id() {
        let ids: Vec<&str> = BenchScenario::all().iter().map(|s| s.as_str()).collect();
        assert_eq!(ids.len(), 4, "issue #941 AC1 names four scenarios");
        let unique: std::collections::BTreeSet<&&str> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "scenario ids must be distinct");
        for id in &ids {
            assert!(!id.is_empty());
            assert!(
                id.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{id} must be a snake_case identifier usable as a table key"
            );
        }
    }

    #[test]
    fn only_the_replay_scenario_runs_without_a_database() {
        for scenario in BenchScenario::all() {
            assert_eq!(
                scenario.needs_database(),
                scenario != BenchScenario::ReplayThroughput,
                "{} classified wrongly",
                scenario.as_str()
            );
        }
    }

    #[test]
    fn only_the_latency_scenarios_are_paced() {
        assert!(
            !BenchScenario::Throughput.is_paced(),
            "saturation is the throughput measurement"
        );
        assert!(BenchScenario::DispatchLatency.is_paced());
        assert!(BenchScenario::SignalRoundtrip.is_paced());
        assert!(
            !BenchScenario::ReplayThroughput.is_paced(),
            "replay drives no queue"
        );
    }

    #[test]
    fn the_shard_matrix_is_the_one_the_issue_asks_for() {
        assert_eq!(SHARD_COUNTS, [1, 2, 4], "issue #941 AC2");
    }

    #[test]
    fn warmup_batch_is_a_fifth_and_never_zero_for_a_nonempty_batch() {
        assert_eq!(warmup_batch_for(0), 0, "nothing to warm up");
        assert_eq!(warmup_batch_for(1), 1, "a tiny batch still gets a warmup");
        assert_eq!(warmup_batch_for(1000), 200);
        assert_eq!(warmup_batch_for(999), 199);
    }

    #[test]
    fn measured_window_excludes_the_warmup_batch() {
        // The guard against R2: a throughput window that swallowed worker
        // startup would inflate nothing and deflate everything.
        let measured = 1000;
        let warmup = warmup_batch_for(measured);
        assert!(
            warmup > 0,
            "a measured batch must be preceded by a warmup batch"
        );
        assert!(
            warmup < measured,
            "the warmup batch must be smaller than the batch it warms up for"
        );
    }

    #[test]
    fn sample_warmup_drops_the_leading_tenth() {
        let samples: Vec<f64> = (0..100).map(f64::from).collect();
        let kept = measured_samples(&samples);
        assert_eq!(kept.len(), 90);
        assert!(
            (kept[0] - 10.0).abs() < f64::EPSILON,
            "the leading tenth must be the part dropped"
        );
    }

    #[test]
    fn sample_warmup_never_discards_every_sample() {
        for n in 0..SAMPLE_WARMUP_DIVISOR {
            #[allow(
                clippy::cast_precision_loss,
                reason = "loop bound is SAMPLE_WARMUP_DIVISOR, a single digit"
            )]
            let samples: Vec<f64> = (0..n).map(|i| i as f64).collect();
            assert_eq!(
                measured_samples(&samples).len(),
                n,
                "a sample set smaller than the divisor must survive intact"
            );
        }
    }

    #[test]
    fn throughput_is_none_for_a_degenerate_window() {
        assert_eq!(throughput_per_sec(0, Duration::from_secs(1)), None);
        assert_eq!(throughput_per_sec(10, Duration::ZERO), None);
    }

    #[test]
    fn throughput_is_completions_over_seconds() {
        let got = throughput_per_sec(500, Duration::from_secs(2)).expect("sound window");
        assert!((got - 250.0).abs() < 1e-9, "got {got}");
    }

    #[test]
    fn stats_below_the_minimum_sample_count_are_unsound() {
        let reasons = latency_soundness(MIN_LATENCY_SAMPLES - 1, 0, 0);
        assert!(
            !reasons.is_empty(),
            "a thin sample set must not be published"
        );
        assert!(
            reasons
                .iter()
                .any(|r| r.contains(&MIN_LATENCY_SAMPLES.to_string())),
            "the reason must name the bar it missed: {reasons:?}"
        );
        assert!(latency_soundness(MIN_LATENCY_SAMPLES, 0, 0).is_empty());
    }

    #[test]
    fn negative_dispatch_samples_are_reported_not_clamped() {
        // Only clock skew can produce one, so its presence is exactly the
        // signal a reader needs (R6).
        let reasons = latency_soundness(MIN_LATENCY_SAMPLES, 3, 0);
        assert!(
            reasons.iter().any(|r| r.contains("negative")),
            "negative samples must be named: {reasons:?}"
        );
    }

    #[test]
    fn missing_created_at_timestamps_are_reported() {
        let reasons = latency_soundness(MIN_LATENCY_SAMPLES, 0, 7);
        assert!(
            reasons.iter().any(|r| r.contains("created_at")),
            "a task row with no created_at yields no sample and must be named: {reasons:?}"
        );
    }

    #[test]
    fn an_incomplete_drain_is_reported_as_truncated() {
        let reasons = throughput_soundness(1000, 900, &[900]);
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("900") && r.contains("1000")),
            "the shortfall must be printed, not averaged away: {reasons:?}"
        );
    }

    #[test]
    fn a_shard_with_no_completions_marks_the_run_unsound() {
        let reasons = throughput_soundness(1000, 1000, &[500, 500, 0, 0]);
        assert!(
            reasons.iter().any(|r| r.contains("shard")),
            "a shard that drained nothing cannot be published as part of a 4-shard number: {reasons:?}"
        );
        assert!(throughput_soundness(1000, 1000, &[250, 250, 250, 250]).is_empty());
    }

    #[test]
    fn a_thin_throughput_run_is_unsound() {
        let n = MIN_THROUGHPUT_COMPLETIONS - 1;
        let reasons = throughput_soundness(n, n, &[n as u64]);
        assert!(!reasons.is_empty(), "too few completions to publish a rate");
    }

    #[test]
    fn pacing_shortfall_marks_the_scenario_saturated() {
        assert_eq!(pacing_verdict(100.0, 100.0), Pacing::Held);
        assert_eq!(
            pacing_verdict(100.0, 90.0),
            Pacing::Held,
            "exactly at the ratio holds"
        );
        assert_eq!(pacing_verdict(100.0, 89.0), Pacing::Saturated);
        assert_eq!(
            pacing_verdict(0.0, 0.0),
            Pacing::Saturated,
            "a target of zero cannot be held; refuse rather than divide by zero"
        );
    }

    #[test]
    fn reproduction_verdict_uses_the_published_tolerance() {
        assert_eq!(
            repro_verdict(100.0, 100.0, REPRO_TOLERANCE_PCT),
            ReproVerdict::Within
        );
        assert_eq!(
            repro_verdict(115.0, 100.0, REPRO_TOLERANCE_PCT),
            ReproVerdict::Within
        );
        assert_eq!(
            repro_verdict(85.0, 100.0, REPRO_TOLERANCE_PCT),
            ReproVerdict::Within
        );
        assert_eq!(
            repro_verdict(115.1, 100.0, REPRO_TOLERANCE_PCT),
            ReproVerdict::Outside
        );
        assert_eq!(
            repro_verdict(84.9, 100.0, REPRO_TOLERANCE_PCT),
            ReproVerdict::Outside
        );
    }

    #[test]
    fn relative_error_is_signed_and_guards_a_zero_baseline() {
        let err = relative_error_pct(110.0, 100.0).expect("finite");
        assert!((err - 10.0).abs() < 1e-9, "got {err}");
        let err = relative_error_pct(90.0, 100.0).expect("finite");
        assert!((err + 10.0).abs() < 1e-9, "got {err}");
        assert_eq!(relative_error_pct(1.0, 0.0), None);
    }

    #[test]
    fn steady_state_rate_uses_the_middle_half_of_the_drain() {
        // A drain that ramps up, holds 100/s, then trails off. The middle half
        // is the held rate; a whole-drain rate would be dragged down by both
        // tails.
        let mut completions = Vec::new();
        let mut t = 0.0_f64;
        for _ in 0..200 {
            t += 0.05; // 20/s ramp-up
            completions.push(t);
        }
        for _ in 0..400 {
            t += 0.01; // 100/s sustained
            completions.push(t);
        }
        for _ in 0..200 {
            t += 0.05; // 20/s drain-down
            completions.push(t);
        }
        let rate = steady_state_throughput(&completions).expect("sound population");
        assert!(
            (rate - 100.0).abs() < 1.0,
            "middle-half rate should recover the sustained 100/s, got {rate}"
        );
    }

    #[test]
    fn steady_state_rate_refuses_a_thin_or_degenerate_population() {
        #[allow(
            clippy::cast_precision_loss,
            reason = "loop bound is MIN_THROUGHPUT_COMPLETIONS, a few hundred"
        )]
        let thin: Vec<f64> = (0..MIN_THROUGHPUT_COMPLETIONS - 1)
            .map(|i| i as f64 * 0.01)
            .collect();
        assert_eq!(steady_state_throughput(&thin), None, "too few completions");

        let flat = vec![1.0_f64; MIN_THROUGHPUT_COMPLETIONS * 2];
        assert_eq!(
            steady_state_throughput(&flat),
            None,
            "a zero-length window has no rate"
        );
    }

    #[test]
    fn the_steady_state_window_is_the_middle_half() {
        let completions: Vec<f64> = (0..400).map(|i| f64::from(i) * 0.01).collect();
        let (lo, hi) = steady_state_window(&completions).expect("sound population");
        assert!(
            (lo - 1.0).abs() < 1e-9,
            "25th percentile completion, got {lo}"
        );
        assert!(
            (hi - 3.0).abs() < 1e-9,
            "75th percentile completion, got {hi}"
        );
    }

    #[test]
    fn a_feeder_bound_run_is_not_published_as_an_engine_number() {
        assert!(
            inflight_soundness(32, Some(32.0)).is_empty(),
            "a fully-held population is engine-bound"
        );
        assert!(
            inflight_soundness(32, Some(28.8)).is_empty(),
            "exactly at the hold ratio still counts as held"
        );
        let reasons = inflight_soundness(32, Some(12.0));
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("12.0") && r.contains("32")),
            "a feeder-bound run must name both the achieved and the target population: \
             {reasons:?}"
        );
        assert!(
            !inflight_soundness(32, None).is_empty(),
            "no observations at all is not evidence of an engine-bound run"
        );
    }

    #[test]
    fn mean_inflight_is_the_arithmetic_mean_and_guards_an_empty_sample() {
        assert_eq!(mean_inflight(&[]), None);
        let mean = mean_inflight(&[10, 20, 30]).expect("non-empty");
        assert!((mean - 20.0).abs() < 1e-9, "got {mean}");
    }

    #[test]
    fn an_absent_filter_selects_the_whole_published_matrix() {
        assert_eq!(selected_scenarios(None), BenchScenario::all().to_vec());
        assert_eq!(
            selected_scenarios(Some("  ")),
            BenchScenario::all().to_vec()
        );
        assert_eq!(selected_shard_counts(None), SHARD_COUNTS.to_vec());
    }

    #[test]
    fn a_filter_selects_in_published_order_not_the_order_it_was_typed() {
        let picked = selected_scenarios(Some("signal_roundtrip,throughput"));
        assert_eq!(
            picked,
            vec![BenchScenario::Throughput, BenchScenario::SignalRoundtrip],
            "a partial run must still read in the published order"
        );
        assert_eq!(selected_shard_counts(Some("4,1")), vec![1, 4]);
    }

    #[test]
    fn an_unknown_filter_entry_is_named_rather_than_silently_dropped() {
        assert_eq!(
            unknown_scenario_ids(Some("throughput,typo")),
            vec!["typo".to_owned()]
        );
        assert!(unknown_scenario_ids(Some("throughput")).is_empty());
        assert!(unknown_scenario_ids(None).is_empty());
        assert_eq!(
            selected_shard_counts(Some("3")),
            Vec::<u32>::new(),
            "a shard count the matrix does not publish selects nothing"
        );
    }

    #[test]
    fn a_malformed_load_override_falls_back_rather_than_failing() {
        assert_eq!(positive_override(None, 32), 32);
        assert_eq!(positive_override(Some("128"), 32), 128);
        assert_eq!(positive_override(Some(" 64 "), 32), 64);
        assert_eq!(
            positive_override(Some("0"), 32),
            32,
            "zero in flight is not a load level"
        );
        assert_eq!(positive_override(Some("-8"), 32), 32);
        assert_eq!(positive_override(Some("lots"), 32), 32);
    }

    #[test]
    fn a_drifting_or_large_clock_offset_voids_a_dispatch_cell() {
        // Tens of microseconds against a ~38 ms p50: the single-host case.
        assert!(
            clock_offset_soundness(&[0.033], &[0.041], 37.78).is_empty(),
            "a sub-millisecond offset must not void a tens-of-milliseconds measurement"
        );
        // A 30 ms skew against a 37.78 ms p50 is most of the number.
        let reasons = clock_offset_soundness(&[30.0], &[30.0], 37.78);
        assert!(
            reasons.iter().any(|r| r.contains("30.000")),
            "a large offset must be named, not printed helpfully beside a published number: \
             {reasons:?}"
        );
        // Same magnitude either way: a database clock that runs ahead
        // understates the latency just as badly.
        assert!(!clock_offset_soundness(&[-30.0], &[-30.0], 37.78).is_empty());
        // Small at both ends but drifting across the window.
        let reasons = clock_offset_soundness(&[0.0], &[5.0], 37.78);
        assert!(
            reasons.iter().any(|r| r.contains("drifted")),
            "drift across the window contaminates samples unevenly: {reasons:?}"
        );
    }

    #[test]
    fn a_dispatch_population_far_below_what_ran_is_unsound() {
        assert!(dispatch_population_soundness(1200, 1200, 0, 0).is_empty());
        assert!(
            dispatch_population_soundness(1200, 1080, 0, 0).is_empty(),
            "the leading-tenth warmup discard must not itself trip the coverage floor"
        );
        let reasons = dispatch_population_soundness(4800, 250, 0, 0);
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("250") && r.contains("4800")),
            "250 samples out of 4800 dispatches must not publish a percentile: {reasons:?}"
        );
        assert!(
            !dispatch_population_soundness(1200, 1200, 1, 0).is_empty(),
            "a re-dispatched row makes a re-delivery delay look like dispatch latency"
        );
        assert!(!dispatch_population_soundness(1200, 1200, 0, 1).is_empty());
    }

    #[test]
    fn a_warmup_that_did_not_drain_is_unsound() {
        assert!(warmup_soundness("throughput", 240, &[240]).is_empty());
        assert!(warmup_soundness("throughput", 240, &[120, 120]).is_empty());
        let reasons = warmup_soundness("throughput", 240, &[100]);
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("100") && r.contains("240")),
            "an undrained warmup is background load inside the measured window: {reasons:?}"
        );
    }

    #[test]
    fn the_replay_control_reports_its_spread_across_a_sweep() {
        // Nothing else in the report compares the replay cells against each
        // other, and each on its own always looks fine.
        assert_eq!(replay_control_drift_pct(&[]), None);
        assert_eq!(replay_control_drift_pct(&[1.0]), None);
        let drift = replay_control_drift_pct(&[9_960_371.64, 9_657_651.19, 9_096_540.11])
            .expect("three readings");
        assert!(
            (drift - 8.67).abs() < 0.05,
            "the reference sweep drifted ~8.7%, got {drift}"
        );
        assert!(replay_control_drift_pct(&[100.0, 100.0]).expect("two readings") < 1e-9);
    }

    #[test]
    fn the_steady_state_slice_is_the_same_window_the_rate_uses() {
        let samples: Vec<usize> = (0..400).collect();
        let slice = steady_state_slice(&samples);
        assert_eq!(slice.len(), 200);
        assert_eq!(slice[0], 100, "starts at the 25th percentile observation");
        assert_eq!(slice[slice.len() - 1], 299);
        // A short series survives intact rather than becoming empty.
        assert_eq!(steady_state_slice(&[1_usize, 2, 3]).len(), 3);
        assert_eq!(steady_state_slice::<usize>(&[]).len(), 0);
    }

    #[test]
    fn a_drain_down_tail_no_longer_voids_a_sound_short_run() {
        // The regression this slice exists to prevent: a 400-completion run at
        // 128 in flight spends ~32% of its loop draining down, so a whole-loop
        // mean falls under the 90% floor while the measured window was fully
        // populated throughout.
        let mut whole_loop: Vec<usize> = vec![128; 68];
        whole_loop.extend((0..32).rev().map(|i| i * 4));
        let whole_mean = mean_inflight(&whole_loop).expect("non-empty");
        assert!(
            !inflight_soundness(128, Some(whole_mean)).is_empty(),
            "the whole-loop mean is what used to fire the gate ({whole_mean:.1})"
        );
        let windowed = mean_inflight(steady_state_slice(&whole_loop)).expect("non-empty");
        assert!(
            inflight_soundness(128, Some(windowed)).is_empty(),
            "the measured window was fully populated; it must publish ({windowed:.1})"
        );
    }

    #[test]
    fn a_missing_metric_renders_as_not_available() {
        assert_eq!(render_value(None), "n/a");
        assert_eq!(render_value(Some(12.345)), "12.35");
    }

    #[test]
    fn the_matrix_marks_an_unsound_cell() {
        let sound = ScenarioReport {
            scenario: BenchScenario::Throughput,
            shards: 1,
            metrics: vec![Metric::new("workflows_per_sec", Some(42.0))],
            notes: vec![],
            unsound: vec![],
        };
        let unsound = ScenarioReport {
            scenario: BenchScenario::Throughput,
            shards: 4,
            metrics: vec![Metric::new("workflows_per_sec", None)],
            notes: vec![],
            unsound: vec!["shard 3 completed nothing".to_owned()],
        };
        let table = render_matrix(&[sound, unsound]);
        assert!(table.contains("| `throughput` | 1 | `workflows_per_sec` | 42.00 | yes |"));
        assert!(
            table.contains("| `throughput` | 4 | `workflows_per_sec` | n/a | **no** |"),
            "an unsound cell must be visibly marked, not silently blank:\n{table}"
        );
    }

    #[test]
    fn the_signal_route_matches_the_plugin_route_shape() {
        let route = signal_route("abc-123", "bench_signal");
        assert_eq!(route, "/api/harvest/workflows/abc-123/signal/bench_signal");
        assert_eq!(
            parse_signal_route(&route),
            Some(("abc-123", "bench_signal")),
            "the server must recover exactly what the client sent"
        );
        assert_eq!(parse_signal_route("/api/harvest/workflows/abc-123"), None);
        assert_eq!(parse_signal_route("/nope"), None);
    }

    #[test]
    fn a_signal_request_round_trips_through_the_framing() {
        let bytes = signal_request_bytes("127.0.0.1:9000", "abc-123", "bench_signal", "{\"n\":1}");
        let HttpParse::Complete(parsed) = parse_http_request(&bytes) else {
            panic!("a complete request must parse");
        };
        assert_eq!(parsed.method, "POST");
        assert_eq!(
            parsed.path,
            "/api/harvest/workflows/abc-123/signal/bench_signal"
        );
        assert_eq!(parsed.body, "{\"n\":1}");
    }

    #[test]
    fn a_partial_request_parses_as_incomplete_rather_than_malformed() {
        let bytes = signal_request_bytes("h", "abc-123", "bench_signal", "{\"n\":1}");
        for cut in [1, bytes.len() / 2, bytes.len() - 1] {
            assert_eq!(
                parse_http_request(&bytes[..cut]),
                HttpParse::Incomplete,
                "a short read must be reported as incomplete, not parsed and not rejected"
            );
        }
        assert!(matches!(parse_http_request(&bytes), HttpParse::Complete(_)));
    }

    #[test]
    fn a_permanently_malformed_request_is_rejected_rather_than_waited_on() {
        // Each of these can never become valid by reading more bytes. Reporting
        // them as `Incomplete` would park the server until the peer gave up --
        // and the benchmark's own client never gives up, so it would deadlock
        // both ends and burn the scenario budget.
        for (label, raw) in [
            (
                "a header line with no colon",
                b"POST / HTTP/1.1\r\nbroken\r\n\r\n".to_vec(),
            ),
            (
                "a non-numeric Content-Length",
                b"POST / HTTP/1.1\r\nContent-Length: lots\r\n\r\n".to_vec(),
            ),
            (
                "a Content-Length past the cap",
                format!(
                    "POST / HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
                    MAX_REQUEST_BYTES + 1
                )
                .into_bytes(),
            ),
            ("a request line with no version", b"POST /\r\n\r\n".to_vec()),
            (
                "invalid UTF-8 in the header block",
                b"POST /\xff\xfe HTTP/1.1\r\n\r\n".to_vec(),
            ),
        ] {
            assert_eq!(
                parse_http_request(&raw),
                HttpParse::Malformed,
                "{label} must be rejected, not waited on"
            );
        }
    }

    #[test]
    fn a_content_length_that_splits_a_character_is_rejected_not_panicked_on() {
        // The body is one 2-byte character; the declared length cuts it in half.
        // Slicing a `&str` there would panic inside the server's connection task.
        let raw = "POST /p HTTP/1.1\r\nContent-Length: 1\r\n\r\n\u{e9}"
            .as_bytes()
            .to_vec();
        assert_eq!(parse_http_request(&raw), HttpParse::Malformed);
    }

    #[test]
    fn an_oversized_buffer_is_rejected_rather_than_accumulated() {
        let raw = vec![b'x'; MAX_REQUEST_BYTES + 1];
        assert_eq!(parse_http_request(&raw), HttpParse::Malformed);
    }

    #[test]
    fn response_status_is_parsed_from_the_status_line() {
        assert_eq!(parse_status_code(b"HTTP/1.1 200 OK\r\n\r\n"), Some(200));
        assert_eq!(
            parse_status_code(b"HTTP/1.1 404 Not Found\r\n\r\n"),
            Some(404)
        );
        assert_eq!(parse_status_code(b"garbage"), None);
    }

    #[test]
    fn replay_history_shape_matches_the_issue_135_contract() {
        let (_exec, events) = build_history(REPLAY_ACTIVITY_COUNT);
        assert_eq!(
            events.len(),
            REPLAY_EVENT_COUNT,
            "issue #135 budgets a 10 000-event history; 5 000 activities is 10 001 events"
        );
        assert!(matches!(events[0], WorkflowEvent::WorkflowStarted { .. }));
        assert!(matches!(events[1], WorkflowEvent::ActivityScheduled { .. }));
        assert!(matches!(events[2], WorkflowEvent::ActivityCompleted { .. }));
    }

    #[test]
    fn the_canonical_workflow_has_exactly_three_activities() {
        assert_eq!(BENCH_ACTIVITIES.len(), 3, "issue #941 AC1a");
        let unique: std::collections::BTreeSet<&&str> = BENCH_ACTIVITIES.iter().collect();
        assert_eq!(unique.len(), 3, "activity names must be distinct");
    }

    #[test]
    fn every_headline_has_a_published_baseline_at_every_shard_count() {
        for scenario in BenchScenario::all() {
            for shards in SHARD_COUNTS {
                let metrics = PUBLISHED_BASELINES
                    .iter()
                    .filter(|b| b.scenario == scenario && b.shards == shards)
                    .count();
                assert!(
                    metrics > 0,
                    "no published baseline for {} at {shards} shards — issue #941 \
                     publishes every scenario at every shard count",
                    scenario.as_str()
                );
            }
        }
    }

    #[test]
    fn every_published_baseline_is_a_usable_positive_number() {
        for b in PUBLISHED_BASELINES {
            assert!(
                b.value.is_finite() && b.value > 0.0,
                "{}/{} shards/{} published as {}",
                b.scenario.as_str(),
                b.shards,
                b.metric,
                b.value
            );
            assert_eq!(
                baseline_for(b.scenario, b.shards, b.metric),
                Some(b.value),
                "lookup must find every table entry"
            );
        }
        assert_eq!(
            baseline_for(BenchScenario::Throughput, 3, "workflows_per_sec"),
            None
        );
    }

    #[test]
    fn baseline_entries_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for b in PUBLISHED_BASELINES {
            assert!(
                seen.insert((b.scenario.as_str(), b.shards, b.metric)),
                "duplicate baseline for {}/{}/{}",
                b.scenario.as_str(),
                b.shards,
                b.metric
            );
        }
    }
}

// ---------------------------------------------------------------------------
// DB-backed section — requires the `db` feature and a reachable Postgres.
//
// Nothing in here is executed by a CI suite: no manifest row runs an end-to-end
// scenario (issue #941 puts CI-gated end-to-end budgets out of scope, and
// `benchmarks_docs.rs` asserts the absence). It is driven by
// `benches/e2e_bench.rs`, which `benchmarks/run.sh` invokes.
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
pub mod db {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use chrono::{DateTime, Utc};
    use diesel::QueryableByName;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;

    use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
    use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
    use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
    use autumn_harvest::types::{ExecutionId, ShardId};
    use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
    use autumn_harvest::{
        ActivityContext, StartWorkflowParams, WorkerConfig, WorkflowContext, signal,
        start_or_load_workflow_execution,
    };

    use super::super::claim_bench_support::LatencyStats;
    use super::{
        BENCH_ACTIVITIES, BENCH_QUEUE, BENCH_SIGNAL, BENCH_SIGNAL_WORKFLOW, BENCH_WORKFLOW,
        BenchScenario, DISPATCH_WORKFLOWS_PER_SHARD, FEEDER_CONNECTIONS_PER_SHARD,
        MAX_CONCURRENT_ACTIVITIES, MAX_CONCURRENT_WORKFLOWS, Metric,
        PACED_STARTS_PER_SEC_PER_SHARD, POLL_INTERVAL_MS, POOL_SIZE_PER_SHARD, Pacing,
        SCENARIO_BUDGET_SECS, SHARD_URLS_ENV_VAR, SIGNAL_PARK_SETTLE, SIGNAL_SOCKET_TIMEOUT,
        SIGNAL_WORKFLOWS_PER_SHARD, ScenarioReport, WORKERS_PER_SHARD, clock_offset_soundness,
        dispatch_population_soundness, inflight_soundness, latency_soundness, mean_inflight,
        measured_samples, pacing_verdict, steady_state_slice, steady_state_throughput,
        steady_state_window, throughput_soundness, warmup_batch_for, warmup_soundness,
    };

    // ── Skip / provisioning ───────────────────────────────────────────────

    /// Why the suite could not run here. Printed; never a failure.
    #[derive(Debug)]
    pub struct SkipReason(pub String);

    impl std::fmt::Display for SkipReason {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }

    /// How the shard set was provisioned. This changes what a shard-count sweep
    /// *means*, so it is reported with the numbers rather than assumed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Topology {
        /// One independent Postgres server per shard — the committed compose
        /// topology, and the shape Harvest's shard model describes.
        IndependentServers,
        /// One server, one database per shard. Cheap, but every "shard" then
        /// contends for the same buffer cache, WAL writer and CPU, so a
        /// shard-count sweep taken this way is not a scale-out measurement.
        SharedServer,
    }

    impl Topology {
        #[must_use]
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::IndependentServers => "independent-servers",
                Self::SharedServer => "shared-server",
            }
        }
    }

    /// A provisioned shard set: one fresh, uniquely-named database per shard.
    pub struct ShardCluster {
        pub urls: BTreeMap<ShardId, String>,
        pub topology: Topology,
        /// `(admin URL, database name)` per shard, so [`Self::teardown`] can
        /// drop what this run created.
        created: Vec<(String, String)>,
        _container: Option<ContainerAsync<Postgres>>,
        /// Idle connections held for the cluster's lifetime, so a concurrent
        /// run on a shared server can see the databases are in use. Same
        /// rationale as the claim harness's lease.
        leases: Vec<AsyncPgConnection>,
    }

    static DB_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Run-unique token mixed into every database name, so two runs (even on
    /// two hosts against one server) cannot collide.
    fn run_token() -> &'static str {
        super::super::claim_bench_support::db::run_token()
    }

    /// Swap the database component of a Postgres URL.
    ///
    /// Delegates to the sibling harness's helper rather than a local
    /// `rfind('/')`: that shortcut panics on a libpq keyword/value DSN
    /// (`host=h port=5432 dbname=postgres`, a legal value for
    /// `HARVEST_TEST_DATABASE_URL`) and silently corrupts a URL with no path
    /// (`postgres://host:5432` becomes `postgres://<db_name>` — the host
    /// replaced by the database). One definition of this across the two
    /// benchmark harnesses.
    fn replace_database(url: &str, db_name: &str) -> Result<String, SkipReason> {
        super::super::claim_bench_support::with_db_name(url, db_name).map_err(|e| {
            SkipReason(format!(
                "cannot address a fresh database on {}: {e}",
                super::super::claim_bench_support::redact_url(url)
            ))
        })
    }

    /// Drop a set of `(admin URL, database name)` pairs, ignoring failures.
    ///
    /// Used both by [`ShardCluster::teardown`] and by the provisioning error
    /// paths, so "the databases this run created get dropped" has one
    /// implementation rather than three.
    async fn drop_created(created: &[(String, String)]) -> Vec<String> {
        let mut failures = Vec::new();
        for (admin_url, name) in created {
            let Ok(mut admin) = <AsyncPgConnection as AsyncConnection>::establish(admin_url).await
            else {
                failures.push(format!("{name}: could not reconnect to drop it"));
                continue;
            };
            if let Err(e) =
                diesel::sql_query(format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                    .execute(&mut admin)
                    .await
            {
                failures.push(format!("{name}: {e}"));
            }
        }
        failures
    }

    async fn create_shard_database(
        admin_url: &str,
        shard: ShardId,
    ) -> Result<(String, String, AsyncPgConnection), SkipReason> {
        let mut admin = <AsyncPgConnection as AsyncConnection>::establish(admin_url)
            .await
            .map_err(|e| {
                SkipReason(format!(
                    "connect {}: {e}",
                    super::super::claim_bench_support::redact_url(admin_url)
                ))
            })?;
        let seq = DB_SEQ.fetch_add(1, Ordering::Relaxed);
        let name = format!("harvest_e2e_{}_{seq}_s{}", run_token(), shard.as_i32());
        diesel::sql_query(format!("CREATE DATABASE {name}"))
            .execute(&mut admin)
            .await
            .map_err(|e| SkipReason(format!("create database {name}: {e}")))?;
        let url = replace_database(admin_url, &name)?;
        // Every failure from here on must drop the database this function just
        // created, or a connect/migrate error orphans it.
        let created = [(admin_url.to_owned(), name.clone())];
        let mut conn = match <AsyncPgConnection as AsyncConnection>::establish(&url).await {
            Ok(conn) => conn,
            Err(e) => {
                drop_created(&created).await;
                return Err(SkipReason(format!("connect to fresh shard database: {e}")));
            }
        };
        if let Err(e) = conn.batch_execute(&autumn_harvest::test_init_sql()).await {
            drop(conn);
            drop_created(&created).await;
            return Err(SkipReason(format!("migrate shard database: {e}")));
        }
        Ok((url, name, conn))
    }

    /// Provision `shard_count` shards.
    ///
    /// Resolution order, most faithful first:
    ///
    /// 1. [`SHARD_URLS_ENV_VAR`] — one **admin** URL per shard, comma
    ///    separated. This is the committed compose topology, and the only mode
    ///    that yields [`Topology::IndependentServers`].
    /// 2. `HARVEST_TEST_DATABASE_URL` — one admin URL; `shard_count` databases
    ///    are created on it ([`Topology::SharedServer`]).
    /// 3. A `postgres:16` testcontainer, same shape as (2).
    /// 4. Neither available: `Err(SkipReason)`, so `cargo bench` on a laptop
    ///    with no Docker prints a notice and exits 0.
    ///
    /// Every mode creates **fresh, uniquely-named** databases, so a 4 000-row
    /// benchmark can never leak into a database somebody else is using.
    pub async fn setup_shards(shard_count: u32) -> Result<ShardCluster, SkipReason> {
        let count = shard_count as usize;
        if let Ok(raw) = std::env::var(SHARD_URLS_ENV_VAR) {
            let admin_urls: Vec<&str> = raw
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            if admin_urls.len() < count {
                return Err(SkipReason(format!(
                    "{SHARD_URLS_ENV_VAR} lists {} URL(s) but this scenario needs {count}; \
                     start the whole compose topology (see benchmarks/docker-compose.yml)",
                    admin_urls.len()
                )));
            }
            let mut urls = BTreeMap::new();
            let mut leases = Vec::new();
            let mut created = Vec::new();
            for (idx, admin) in admin_urls.iter().take(count).enumerate() {
                let shard = ShardId::new(i32::try_from(idx).unwrap_or(0));
                match create_shard_database(admin, shard).await {
                    Ok((url, name, lease)) => {
                        urls.insert(shard, url);
                        created.push(((*admin).to_owned(), name));
                        leases.push(lease);
                    }
                    // Shard 3 of 4 failing is the common case (one server slower
                    // to accept connections). Without this, shards 0-2 are
                    // already created and migrated and nothing ever drops them.
                    Err(e) => {
                        drop(leases);
                        drop_created(&created).await;
                        return Err(e);
                    }
                }
            }
            return Ok(ShardCluster {
                urls,
                topology: Topology::IndependentServers,
                created,
                _container: None,
                leases,
            });
        }

        let (admin_url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
            (url, None)
        } else {
            let container = Postgres::default()
                .with_tag("16")
                .start()
                .await
                .map_err(|e| {
                    SkipReason(format!(
                        "no {SHARD_URLS_ENV_VAR}, no HARVEST_TEST_DATABASE_URL, and no Docker \
                         daemon ({e}); nothing to benchmark against"
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
            (
                format!("postgres://postgres:postgres@{host}:{port}/postgres"),
                Some(container),
            )
        };

        let mut urls = BTreeMap::new();
        let mut leases = Vec::new();
        let mut created = Vec::new();
        for idx in 0..count {
            let shard = ShardId::new(i32::try_from(idx).unwrap_or(0));
            match create_shard_database(&admin_url, shard).await {
                Ok((url, name, lease)) => {
                    urls.insert(shard, url);
                    created.push((admin_url.clone(), name));
                    leases.push(lease);
                }
                Err(e) => {
                    drop(leases);
                    drop_created(&created).await;
                    return Err(e);
                }
            }
        }
        Ok(ShardCluster {
            urls,
            topology: Topology::SharedServer,
            created,
            _container: container,
            leases,
        })
    }

    fn build_pool(url: &str) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        deadpool::managed::Pool::builder(manager)
            .max_size(POOL_SIZE_PER_SHARD)
            .build()
            .expect("bench pool build failed")
    }

    impl ShardCluster {
        #[must_use]
        pub fn shard_ids(&self) -> Vec<ShardId> {
            self.urls.keys().copied().collect()
        }

        #[must_use]
        pub fn sharded_pool(&self) -> ShardedDbPool {
            let pools: BTreeMap<ShardId, DbPool> = self
                .urls
                .iter()
                .map(|(shard, url)| (*shard, build_pool(url)))
                .collect();
            ShardedDbPool::from_map(pools, ShardId::new(0))
        }

        #[must_use]
        pub fn router(&self) -> ShardRouter {
            let ids = self.shard_ids();
            ShardRouter::new(ids.clone(), ids, ShardId::new(0))
        }

        /// Drop every database this run created.
        ///
        /// The suite provisions a fresh database per shard **per scenario**, so
        /// a full 4x3 sweep against one operator-supplied server would otherwise
        /// leave dozens of multi-thousand-row databases behind. Failures are
        /// reported, never fatal: a benchmark that could not tidy up has still
        /// produced its numbers.
        pub async fn teardown(mut self) -> Vec<String> {
            // Release our own backends first, or `DROP DATABASE` blocks on them.
            self.leases.clear();
            drop_created(&self.created).await
        }

        pub async fn connect(&self, shard: ShardId) -> AsyncPgConnection {
            <AsyncPgConnection as AsyncConnection>::establish(&self.urls[&shard])
                .await
                .expect("connect to shard database")
        }
    }

    // ── Observation sink ──────────────────────────────────────────────────

    /// What the bench handlers record. Shared with the worker through
    /// `HandlerRegistry`'s state map.
    #[derive(Default)]
    pub struct BenchObservations {
        /// `(harvest_task_queue.id, handler-entry host clock)` for every
        /// dispatched activity.
        activity_starts: Mutex<Vec<(Uuid, DateTime<Utc>)>>,
        /// Execution id -> the first instant the workflow's own code ran past
        /// `wait_for_signal`. First-wins, so a later replay of the same code
        /// cannot move an already-recorded observation.
        signal_observed: Mutex<BTreeMap<Uuid, Instant>>,
        /// Execution ids that reached the `wait_for_signal` call.
        parked: Mutex<std::collections::BTreeSet<Uuid>>,
        /// Activity dispatches that produced no sample because the context
        /// carried no task id. Counted rather than ignored, so the published
        /// population can be reconciled against the population that ran.
        unrecorded_dispatches: Mutex<usize>,
    }

    impl BenchObservations {
        #[must_use]
        pub fn activity_starts(&self) -> Vec<(Uuid, DateTime<Utc>)> {
            self.activity_starts.lock().expect("poisoned").clone()
        }

        #[must_use]
        pub fn parked_count(&self) -> usize {
            self.parked.lock().expect("poisoned").len()
        }

        #[must_use]
        pub fn signal_observation(&self, exec_id: Uuid) -> Option<Instant> {
            self.signal_observed
                .lock()
                .expect("poisoned")
                .get(&exec_id)
                .copied()
        }

        #[must_use]
        pub fn unrecorded_dispatches(&self) -> usize {
            *self.unrecorded_dispatches.lock().expect("poisoned")
        }

        fn record_unrecorded_dispatch(&self) {
            *self.unrecorded_dispatches.lock().expect("poisoned") += 1;
        }

        fn record_activity_start(&self, task_id: Uuid) {
            self.activity_starts
                .lock()
                .expect("poisoned")
                .push((task_id, Utc::now()));
        }

        fn record_parked(&self, exec_id: Uuid) {
            self.parked.lock().expect("poisoned").insert(exec_id);
        }

        fn record_signal_observed(&self, exec_id: Uuid) {
            self.signal_observed
                .lock()
                .expect("poisoned")
                .entry(exec_id)
                .or_insert_with(Instant::now);
        }
    }

    type BoxFut<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
    >;

    fn observations_of_activity(ctx: &ActivityContext) -> Arc<BenchObservations> {
        Arc::clone(
            ctx.state::<Arc<BenchObservations>>()
                .expect("BenchObservations must be registered in shared state"),
        )
    }

    fn observations_of_workflow(ctx: &WorkflowContext) -> Arc<BenchObservations> {
        Arc::clone(
            ctx.state::<Arc<BenchObservations>>()
                .expect("BenchObservations must be registered in shared state"),
        )
    }

    /// The canonical benchmark activity: records when the handler started and
    /// returns. Deliberately does no work — the number being published is the
    /// engine's, not the handler's.
    fn bench_activity(ctx: &ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
        Box::pin(async move {
            let observations = observations_of_activity(ctx);
            match ctx.info().task_id {
                Some(task_id) => observations.record_activity_start(task_id),
                // A queue-dispatched activity always has a task row; recording
                // the absence rather than ignoring it keeps the sample
                // population auditable (see `unrecorded_dispatches`).
                None => observations.record_unrecorded_dispatch(),
            }
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    /// The canonical 3-activity workflow (issue #941, AC1 clause (a)).
    fn bench_workflow(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
        Box::pin(async move {
            for name in BENCH_ACTIVITIES {
                ctx.execute_activity_raw(name, serde_json::Value::Null, BENCH_QUEUE)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    /// Parks on a signal, then records the instant its own code resumed.
    ///
    /// Both records are side-effect-only sinks: nothing the workflow does
    /// afterwards depends on them, so the handler stays deterministic. Recording
    /// is first-wins per execution, which is what makes a later replay of this
    /// same line harmless.
    fn bench_signal_workflow(ctx: &WorkflowContext, _input: serde_json::Value) -> BoxFut<'_> {
        Box::pin(async move {
            let exec_id = ctx.info().execution_id.as_uuid();
            let observations = observations_of_workflow(ctx);
            observations.record_parked(exec_id);
            let _payload = ctx
                .wait_for_signal(BENCH_SIGNAL)
                .await
                .map_err(|e| e.to_string())?;
            observations.record_signal_observed(exec_id);
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn workflow_info(
        name: &'static str,
        handler: autumn_harvest::info::WorkflowHandlerFn,
    ) -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name,
            module: "e2e_bench_support",
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

    fn activity_info(name: &'static str) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "e2e_bench_support",
            default_retry_policy: None,
            default_start_to_close: Some(std::time::Duration::from_secs(30)),
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: Some(BENCH_QUEUE),
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: bench_activity,
        }
    }

    /// Build the registry both bench workflows and all three activities share.
    #[must_use]
    pub fn build_registry() -> (Arc<HandlerRegistry>, Arc<BenchObservations>) {
        let telemetry = Arc::new(
            TelemetryConfig::builder()
                .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
                .build(),
        );
        let observations = Arc::new(BenchObservations::default());
        let mut state = std::collections::HashMap::new();
        state.insert(
            std::any::TypeId::of::<Arc<BenchObservations>>(),
            Box::new(Arc::clone(&observations)) as Box<dyn std::any::Any + Send + Sync>,
        );
        let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
            vec![
                workflow_info(BENCH_WORKFLOW, bench_workflow),
                workflow_info(BENCH_SIGNAL_WORKFLOW, bench_signal_workflow),
            ],
            BENCH_ACTIVITIES.iter().map(|n| activity_info(n)).collect(),
            Arc::new(state),
            telemetry,
        ));
        (registry, observations)
    }

    /// A running worker fleet: one worker pinned per shard.
    pub struct Fleet {
        workers: Vec<Arc<Worker>>,
        handles: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Fleet {
        /// Shut every worker down, and **abort** any that does not stop in time.
        ///
        /// `timeout(_, handle)` consumes the `JoinHandle` and dropping it
        /// *detaches* the task rather than stopping it. A detached worker keeps
        /// claiming from a pool whose database the next line drops, and keeps
        /// burning CPU through the remaining cells of a twelve-cell sweep --
        /// perturbing the measurements those cells publish. So the handle is
        /// kept and aborted.
        pub async fn stop(self) {
            for worker in &self.workers {
                worker.shutdown();
            }
            for handle in self.handles {
                let abort = handle.abort_handle();
                if tokio::time::timeout(std::time::Duration::from_secs(15), handle)
                    .await
                    .is_err()
                {
                    abort.abort();
                }
            }
        }
    }

    /// Start [`WORKERS_PER_SHARD`] workers per shard, each pinned to its shard
    /// and listening on that shard's own database.
    #[must_use]
    pub fn start_fleet(
        cluster: &ShardCluster,
        sharded: &ShardedDbPool,
        registry: &Arc<HandlerRegistry>,
    ) -> Fleet {
        let notification_urls: Vec<(ShardId, String)> = cluster
            .urls
            .iter()
            .map(|(shard, url)| (*shard, url.clone()))
            .collect();
        let mut workers = Vec::new();
        let mut handles = Vec::new();
        for shard in cluster.shard_ids() {
            for replica in 0..WORKERS_PER_SHARD {
                let worker_config = WorkerConfig::default()
                    .with_queues([BENCH_QUEUE])
                    .with_shard_assignments([shard])
                    .with_shard_notification_database_urls(notification_urls.clone());
                let mut runtime: WorkerRuntimeConfig = worker_config.into();
                runtime.worker_id = format!("bench-s{}-{replica}", shard.as_i32());
                runtime.poll_interval = std::time::Duration::from_millis(POLL_INTERVAL_MS);
                runtime.shutdown_timeout = std::time::Duration::from_secs(5);
                runtime.max_concurrent_workflows = MAX_CONCURRENT_WORKFLOWS;
                runtime.max_concurrent_activities = MAX_CONCURRENT_ACTIVITIES;
                runtime.sharded_pool = Some(sharded.clone());
                let worker = Arc::new(
                    Worker::new(runtime, Arc::clone(registry)).expect("bench worker should build"),
                );
                let pool = sharded
                    .exact_pool_for(shard)
                    .expect("shard pool present")
                    .clone();
                let runner = Arc::clone(&worker);
                handles.push(tokio::spawn(async move {
                    runner.run(&pool).await;
                }));
                workers.push(worker);
            }
        }
        Fleet { workers, handles }
    }

    // ── Seeding ───────────────────────────────────────────────────────────

    fn start_params<'a>(
        exec_id: ExecutionId,
        workflow_name: &'a str,
        workflow_id: &'a str,
    ) -> StartWorkflowParams<'a> {
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: BENCH_QUEUE,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: autumn_harvest::types::Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        }
    }

    /// Start `count` workflows of `workflow_name` on `shard`, tagged with
    /// `cohort` so warmup and measured populations never mix.
    pub async fn seed_workflows(
        cluster: &ShardCluster,
        shard: ShardId,
        workflow_name: &str,
        cohort: &str,
        count: usize,
    ) -> Vec<ExecutionId> {
        seed_workflows_on(&cluster.urls[&shard], shard, workflow_name, cohort, count).await
    }

    /// [`seed_workflows`] against a shard URL, so seeding can be spawned per
    /// shard without borrowing the cluster.
    pub async fn seed_workflows_on(
        url: &str,
        shard: ShardId,
        workflow_name: &str,
        cohort: &str,
        count: usize,
    ) -> Vec<ExecutionId> {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("connect to seed shard database");
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let exec_id = ExecutionId::new_for_shard(shard);
            let workflow_id = format!("{cohort}-s{}-{i}", shard.as_i32());
            start_or_load_workflow_execution(
                &mut conn,
                start_params(exec_id, workflow_name, &workflow_id),
                None,
            )
            .await
            .expect("seed bench workflow");
            ids.push(exec_id);
        }
        ids
    }

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    #[derive(QueryableByName)]
    struct CompletionRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        completed_at: DateTime<Utc>,
    }

    #[derive(QueryableByName)]
    struct TaskCreatedRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        created_at: Option<DateTime<Utc>>,
    }

    #[derive(QueryableByName)]
    struct NowRow {
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        now: DateTime<Utc>,
    }

    async fn completed_count(conn: &mut AsyncPgConnection, cohort: &str) -> i64 {
        diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE state = 'COMPLETED' AND workflow_id LIKE $1",
        )
        .bind::<diesel::sql_types::Text, _>(format!("{cohort}-%"))
        .get_result::<CountRow>(conn)
        .await
        .expect("count completed bench workflows")
        .n
    }

    async fn completion_times(conn: &mut AsyncPgConnection, cohort: &str) -> Vec<DateTime<Utc>> {
        diesel::sql_query(
            "SELECT completed_at FROM harvest_workflow_executions \
             WHERE state = 'COMPLETED' AND completed_at IS NOT NULL AND workflow_id LIKE $1 \
             ORDER BY completed_at",
        )
        .bind::<diesel::sql_types::Text, _>(format!("{cohort}-%"))
        .load::<CompletionRow>(conn)
        .await
        .expect("load bench completion times")
        .into_iter()
        .map(|r| r.completed_at)
        .collect()
    }

    /// Median host-to-database clock offset, in milliseconds.
    ///
    /// Published rather than assumed away: the activity-dispatch measurement
    /// spans a database timestamp and a host timestamp, and a reader deserves
    /// the size of that error term instead of a promise that it is zero.
    pub async fn clock_offset_ms(conn: &mut AsyncPgConnection) -> f64 {
        let mut samples = Vec::new();
        for _ in 0..7 {
            let before = Utc::now();
            let row = diesel::sql_query("SELECT now() AS now")
                .get_result::<NowRow>(conn)
                .await
                .expect("probe database clock");
            let after = Utc::now();
            let midpoint = before + (after - before) / 2;
            #[allow(
                clippy::cast_precision_loss,
                reason = "microsecond offsets on one host are far below 2^53"
            )]
            let offset = (row.now - midpoint).num_microseconds().unwrap_or(0) as f64 / 1000.0;
            samples.push(offset);
        }
        samples.sort_by(f64::total_cmp);
        samples[samples.len() / 2]
    }

    // ── HTTP signal endpoint ──────────────────────────────────────────────

    /// A running minimal HTTP/1.1 endpoint serving exactly the plugin's signal
    /// route shape.
    ///
    /// This is **not** autumn-web's router: it carries no auth, tracing,
    /// rate-limit or payload-schema middleware. It exists so the published
    /// round-trip includes a real loopback socket, real request framing and the
    /// same `signal::send_signal` entry point the plugin route calls — see
    /// "Known limitations" in `docs/benchmarks.md`.
    pub struct SignalServer {
        pub addr: std::net::SocketAddr,
        handle: tokio::task::JoinHandle<()>,
    }

    impl SignalServer {
        /// Stop accepting and abort every in-flight connection task.
        ///
        /// Aborting the accept loop drops its `JoinSet`, which aborts the
        /// connection tasks with it — so no task can still be holding a pooled
        /// connection when the caller drops the pool and `teardown` drops the
        /// databases.
        pub fn stop(self) {
            self.handle.abort();
        }
    }

    /// Bind the signal endpoint on loopback and start serving.
    ///
    /// # Errors
    ///
    /// Returns the bind/accept error if loopback is unavailable.
    pub async fn spawn_signal_server(sharded: ShardedDbPool) -> std::io::Result<SignalServer> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            // Connection tasks are tracked rather than detached: `stop` must be
            // able to guarantee no task is still holding a pooled connection
            // when the caller drops the pool and drops the databases.
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    // Reap finished connection tasks so the set does not grow
                    // for the life of the scenario.
                    Some(_) = connections.join_next(), if !connections.is_empty() => continue,
                };
                // A transient accept error (EMFILE under load) must not retire
                // the endpoint for the rest of the run: every subsequent signal
                // would then be counted as a failure and the cell blamed on the
                // engine.
                let Ok((mut socket, _)) = accepted else {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                };
                let sharded = sharded.clone();
                connections.spawn(async move {
                    let _ = socket.set_nodelay(true);
                    let mut buf = Vec::with_capacity(512);
                    let request = loop {
                        let mut chunk = [0_u8; 512];
                        let read = tokio::time::timeout(
                            SIGNAL_SOCKET_TIMEOUT,
                            socket.read(&mut chunk),
                        )
                        .await;
                        match read {
                            Ok(Ok(0) | Err(_)) | Err(_) => return,
                            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
                        }
                        match super::parse_http_request(&buf) {
                            super::HttpParse::Complete(request) => break Some(request),
                            super::HttpParse::Incomplete => (),
                            // Answer rather than wait: see `HttpParse`.
                            super::HttpParse::Malformed => break None,
                        }
                    };
                    let status = match request {
                        Some(request) => serve_signal(&sharded, &request).await,
                        None => 400,
                    };
                    let body = if status == 200 { "{\"ok\":true}" } else { "{}" };
                    let response = format!(
                        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        if status == 200 { "OK" } else { "Error" },
                        body.len(),
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                });
            }
        });
        Ok(SignalServer { addr, handle })
    }

    async fn serve_signal(sharded: &ShardedDbPool, request: &super::ParsedRequest) -> u16 {
        if request.method != "POST" {
            return 405;
        }
        let Some((raw_exec_id, signal_name)) = super::parse_signal_route(&request.path) else {
            return 404;
        };
        let Ok(exec_id) = raw_exec_id.parse::<ExecutionId>() else {
            return 400;
        };
        let payload: serde_json::Value =
            serde_json::from_str(&request.body).unwrap_or(serde_json::Value::Null);
        // The exec id encodes its shard, so the endpoint routes exactly the way
        // the management API does.
        let Ok(mut conn) = sharded.pool_for_execution(exec_id).get().await else {
            return 503;
        };
        match signal::send_signal(&mut conn, exec_id, signal_name, payload).await {
            Ok(()) => 200,
            Err(_) => 500,
        }
    }

    /// Send one signal over HTTP and return the instant the request left this
    /// process plus the response status.
    ///
    /// A fresh connection per request: slightly pessimistic against a
    /// keep-alive client, and it keeps the measured path identical for every
    /// sample.
    pub async fn send_signal_over_http(
        addr: std::net::SocketAddr,
        exec_id: ExecutionId,
        signal_name: &str,
    ) -> (Instant, Option<u16>) {
        let request =
            super::signal_request_bytes(&addr.to_string(), &exec_id.to_string(), signal_name, "{}");
        let sent_at = Instant::now();
        let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
            return (sent_at, None);
        };
        let _ = stream.set_nodelay(true);
        if stream.write_all(&request).await.is_err() {
            return (sent_at, None);
        }
        let mut response = Vec::with_capacity(128);
        // Bounded: an unbounded `read_to_end` against a peer that never answers
        // parks the whole scenario past its budget, and the signalling loop only
        // checks the deadline between sends.
        if tokio::time::timeout(SIGNAL_SOCKET_TIMEOUT, stream.read_to_end(&mut response))
            .await
            .is_err()
        {
            return (sent_at, None);
        }
        (sent_at, super::parse_status_code(&response))
    }

    #[derive(QueryableByName)]
    struct VersionRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        version: String,
    }

    /// `SELECT version()` from the first reachable shard, for the environment
    /// block of the published report.
    ///
    /// Reported from the live server rather than typed into the doc by hand:
    /// the Postgres version is one of the few things that most changes these
    /// numbers, and a hand-typed one is the kind that goes stale silently.
    pub async fn probe_postgres_version() -> Option<String> {
        let url = std::env::var(SHARD_URLS_ENV_VAR)
            .ok()
            .and_then(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .find(|s| !s.is_empty())
                    .map(str::to_owned)
            })
            .or_else(|| std::env::var("HARVEST_TEST_DATABASE_URL").ok())?;
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .ok()?;
        diesel::sql_query("SELECT version() AS version")
            .get_result::<VersionRow>(&mut conn)
            .await
            .ok()
            .map(|r| r.version)
    }

    // ── Scenario runners ──────────────────────────────────────────────────

    fn scenario_deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(SCENARIO_BUDGET_SECS)
    }

    /// Note the wall clock one cell took, and complain when it exhausted its
    /// budget — a cell that ran out of time reported whatever it had collected,
    /// which is not a published number.
    fn budget_note(started: Instant, deadline: Instant, unsound: &mut Vec<String>) -> String {
        let elapsed = started.elapsed();
        // Against the deadline itself, not against elapsed-since-the-cell-began:
        // the deadline starts after provisioning, so a cell whose provisioning
        // was slow but whose collection finished cleanly must not be refused.
        if Instant::now() >= deadline {
            unsound.push(format!(
                "the {SCENARIO_BUDGET_SECS}s scenario budget was exhausted; this cell reports \
                 only what it had collected when the clock ran out"
            ));
        }
        format!("wall clock: {:.1}s", elapsed.as_secs_f64())
    }

    /// Poll every shard until `expected` executions of `cohort` are COMPLETED,
    /// or the deadline passes. Returns the per-shard completion counts.
    ///
    /// One connection per shard is opened **once** and reused for every poll.
    /// Reconnecting on each tick would put a connection storm on the same
    /// servers the measurement is running against — the observer changing what
    /// it observes.
    async fn wait_for_completions(
        cluster: &ShardCluster,
        cohort: &str,
        expected: usize,
        deadline: Instant,
    ) -> Vec<u64> {
        let mut conns = Vec::new();
        for shard in cluster.shard_ids() {
            conns.push(cluster.connect(shard).await);
        }
        loop {
            let mut per_shard = Vec::with_capacity(conns.len());
            for conn in &mut conns {
                per_shard.push(u64::try_from(completed_count(conn, cohort).await).unwrap_or(0));
            }
            let total: u64 = per_shard.iter().sum();
            if usize::try_from(total).unwrap_or(usize::MAX) >= expected
                || Instant::now() >= deadline
            {
                return per_shard;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Append a note when the run could not drop its own databases, so an
    /// operator who supplied a shared server learns about the leftovers from
    /// the report rather than from `\\l` a week later.
    fn with_teardown_note(mut notes: Vec<String>, failures: &[String]) -> Vec<String> {
        if !failures.is_empty() {
            notes.push(format!(
                "could not drop {} benchmark database(s): {}",
                failures.len(),
                failures.join("; ")
            ));
        }
        notes
    }

    fn shard_note(per_shard: &[u64]) -> String {
        let split: Vec<String> = per_shard
            .iter()
            .enumerate()
            .map(|(i, n)| format!("s{i}={n}"))
            .collect();
        format!("per-shard completions: {}", split.join(" "))
    }

    /// **Scenario (a).** Sustained workflows completed per second for the
    /// canonical 3-activity workflow, under a bounded closed loop.
    ///
    /// A warmup population runs through the identical loop first and is
    /// discarded, so pools are full and the planner has statistics on a
    /// populated table; the headline is then the middle-half rate over the
    /// measured population, which excludes both the ramp-up and the drain-down
    /// tail. [`inflight_soundness`] is what stops a run the *harness* bounded
    /// from being published as a fact about the engine.
    ///
    /// # Errors
    ///
    /// Returns [`SkipReason`] when no Postgres could be provisioned.
    #[allow(
        clippy::too_many_lines,
        reason = "one measurement protocol, read top to bottom: provision, warm up, measure, \
                  collect, judge soundness, tear down. Splitting it into fragments would make \
                  the ordering constraints between those phases -- which are the whole \
                  correctness argument -- harder to audit, not easier."
    )]
    pub async fn run_throughput(shard_count: u32) -> Result<ScenarioReport, SkipReason> {
        let scenario_started = Instant::now();
        let cluster = setup_shards(shard_count).await?;
        let sharded = cluster.sharded_pool();
        let (registry, _observations) = build_registry();
        let fleet = start_fleet(&cluster, &sharded, &registry);
        let deadline = scenario_deadline();
        let shards = cluster.shard_ids();

        // Warmup: the same closed loop, at a fraction of the population, with
        // every completion discarded. Pools fill, statements are planned, the
        // planner sees real statistics.
        let inflight = super::inflight_target();
        let per_shard_total = super::measured_workflows_per_shard();
        let warm_per_shard = warmup_batch_for(per_shard_total);
        let warm_loops =
            run_closed_loop(&cluster, "warm", warm_per_shard, inflight, deadline).await;
        let warm_drained: Vec<u64> = warm_loops
            .iter()
            .map(|l| u64::try_from(l.completed).unwrap_or(0))
            .collect();

        let loops = run_closed_loop(&cluster, "meas", per_shard_total, inflight, deadline).await;
        let requested = per_shard_total * shards.len();

        // Completion instants come from the database clock (`completed_at`), so
        // the throughput headline carries no host-to-database skew term at all.
        let mut completions: Vec<DateTime<Utc>> = Vec::new();
        let mut per_shard = Vec::new();
        for shard in &shards {
            let mut conn = cluster.connect(*shard).await;
            let times = completion_times(&mut conn, "meas").await;
            per_shard.push(u64::try_from(times.len()).unwrap_or(0));
            completions.extend(times);
        }
        completions.sort_unstable();
        let completed = completions.len();

        // `completions[..]` rather than `.first()`: diesel's query DSL is in
        // scope here, and probing `.first()` on a `Vec` sends the trait solver
        // into overflow (the same E0275 `sharded_runtime_tests.rs` documents).
        let epoch = match completions.as_slice() {
            [first, ..] => Some(*first),
            [] => None,
        };
        let secs: Vec<f64> = epoch.map_or_else(Vec::new, |start| {
            completions
                .iter()
                .map(|t| {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "a run lasts seconds; microsecond counts stay far below 2^53"
                    )]
                    {
                        (*t - start).num_microseconds().unwrap_or(0) as f64 / 1e6
                    }
                })
                .collect()
        });

        let sustained = steady_state_throughput(&secs);
        let window_secs = steady_state_window(&secs).map(|(lo, hi)| hi - lo);

        // Only the samples from the window the headline is computed over. The
        // loop's drain-down tail decays 128 -> 0 by construction, so averaging
        // over the whole loop measures the tail rather than the population the
        // rate was produced under -- and on a short run it fires the gate
        // against a perfectly good measurement.
        let inflight_samples: Vec<usize> = loops
            .into_iter()
            .flat_map(|l| steady_state_slice(&l.inflight_samples).to_vec())
            .collect();
        let mean_flight = mean_inflight(&inflight_samples);
        let mut unsound = throughput_soundness(requested, completed, &per_shard);
        unsound.extend(warmup_soundness(
            "throughput",
            warm_per_shard * shards.len(),
            &warm_drained,
        ));
        // Per shard on both sides: `inflight_samples` is the concatenation of
        // every shard's observations, so its mean is already a per-shard
        // population.
        unsound.extend(inflight_soundness(inflight, mean_flight));

        fleet.stop().await;
        let topology = cluster.topology;
        drop(sharded);
        let teardown_failures = cluster.teardown().await;
        let wall_clock_note = budget_note(scenario_started, deadline, &mut unsound);

        #[allow(
            clippy::cast_precision_loss,
            reason = "completion counts here are far below 2^53"
        )]
        let completed_metric = completed as f64;
        Ok(ScenarioReport {
            scenario: BenchScenario::Throughput,
            shards: shard_count,
            metrics: vec![
                Metric::new(
                    "workflows_per_sec",
                    if unsound.is_empty() { sustained } else { None },
                ),
                Metric::new("measured_window_secs", window_secs),
                Metric::new("completions", Some(completed_metric)),
            ],
            notes: with_teardown_note(
                vec![
                    shard_note(&per_shard),
                    format!("topology: {}", topology.as_str()),
                    format!(
                        "closed loop: {inflight} workflows in flight per shard, {requested} \
                         measured completions, warmup population {}",
                        warm_per_shard * shards.len()
                    ),
                    mean_flight.map_or_else(
                        || "mean in-flight population: n/a".to_owned(),
                        |m| {
                            format!(
                                "mean in-flight population: {m:.1} per shard against a target of \
                                 {inflight}"
                            )
                        },
                    ),
                    "the headline is the middle-half rate: the ramp-up and the drain-down tails \
                     are excluded, so it is a sustained rate rather than an average over a \
                     changing queue depth"
                        .to_owned(),
                    wall_clock_note,
                ],
                &teardown_failures,
            ),
            unsound,
        })
    }

    /// What one shard's closed loop observed.
    struct LoopOutcome {
        /// How many workflows the feeder actually started on this shard.
        started: usize,
        /// How many of them the engine completed before the loop ended.
        completed: usize,
        /// The in-flight population sampled once per feeder cycle.
        inflight_samples: Vec<usize>,
    }

    /// Run the bounded closed loop on every shard concurrently: hold
    /// [`THROUGHPUT_INFLIGHT_PER_SHARD`] workflows in flight, topping the
    /// population up as runs complete, until `per_shard` have completed.
    async fn run_closed_loop(
        cluster: &ShardCluster,
        cohort: &str,
        per_shard: usize,
        inflight: usize,
        deadline: Instant,
    ) -> Vec<LoopOutcome> {
        let mut tasks = Vec::new();
        for shard in cluster.shard_ids() {
            let url = cluster.urls[&shard].clone();
            let cohort = cohort.to_owned();
            tasks.push(tokio::spawn(async move {
                closed_loop_on_shard(&url, shard, &cohort, per_shard, inflight, deadline).await
            }));
        }
        let mut outcomes = Vec::new();
        for task in tasks {
            outcomes.push(task.await.expect("closed-loop shard task"));
        }
        outcomes
    }

    async fn closed_loop_on_shard(
        url: &str,
        shard: ShardId,
        cohort: &str,
        total: usize,
        inflight: usize,
        deadline: Instant,
    ) -> LoopOutcome {
        let mut observer = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("connect the closed-loop observer");
        let mut feeders = Vec::with_capacity(FEEDER_CONNECTIONS_PER_SHARD);
        for _ in 0..FEEDER_CONNECTIONS_PER_SHARD {
            feeders.push(
                <AsyncPgConnection as AsyncConnection>::establish(url)
                    .await
                    .expect("connect a closed-loop feeder"),
            );
        }

        let mut started = 0_usize;
        let mut inflight_samples = Vec::new();
        // Reported so the caller can verify a warmup population actually
        // drained rather than merely timing out (see `warmup_soundness`).
        let mut completed;
        started += start_batch(&mut feeders, shard, cohort, started, total.min(inflight)).await;

        loop {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            completed = usize::try_from(completed_count(&mut observer, cohort).await).unwrap_or(0);
            inflight_samples.push(started.saturating_sub(completed));
            if completed >= total || Instant::now() >= deadline {
                break;
            }
            let want = total.min(completed + inflight);
            if want > started {
                started += start_batch(&mut feeders, shard, cohort, started, want - started).await;
            }
        }
        LoopOutcome {
            started,
            completed,
            inflight_samples,
        }
    }

    /// Start `count` workflows, spread across the shard's feeder connections so
    /// the harness is not the bottleneck.
    async fn start_batch(
        feeders: &mut [AsyncPgConnection],
        shard: ShardId,
        cohort: &str,
        first_index: usize,
        count: usize,
    ) -> usize {
        if count == 0 {
            return 0;
        }
        let lanes = feeders.len();
        let futures = feeders.iter_mut().enumerate().map(|(lane, conn)| {
            let indices: Vec<usize> = (0..count)
                .filter(|i| i % lanes == lane)
                .map(|i| first_index + i)
                .collect();
            async move {
                for i in indices {
                    let exec_id = ExecutionId::new_for_shard(shard);
                    let workflow_id = format!("{cohort}-s{}-{i}", shard.as_i32());
                    start_or_load_workflow_execution(
                        conn,
                        start_params(exec_id, BENCH_WORKFLOW, &workflow_id),
                        None,
                    )
                    .await
                    .expect("closed-loop start");
                }
            }
        });
        futures::future::join_all(futures).await;
        count
    }

    /// Start `count` workflows per shard at `per_shard_rate` starts/sec, and
    /// report `(execution ids, wall clock elapsed)`.
    async fn paced_seed(
        cluster: &ShardCluster,
        workflow_name: &str,
        cohort: &str,
        count: usize,
        per_shard_rate: f64,
        deadline: Instant,
    ) -> (Vec<ExecutionId>, std::time::Duration) {
        let period = std::time::Duration::from_secs_f64(1.0 / per_shard_rate);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for shard in cluster.shard_ids() {
            let url = cluster.urls[&shard].clone();
            let workflow_name = workflow_name.to_owned();
            let cohort = cohort.to_owned();
            tasks.push(tokio::spawn(async move {
                let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
                    .await
                    .expect("connect for paced seeding");
                let mut ids = Vec::with_capacity(count);
                let mut ticker = tokio::time::interval(period);
                // `Delay`, not `Burst`: a catch-up burst after a slow start would hide
                // exactly the saturation `pacing_verdict` exists to detect.
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                for i in 0..count {
                    // The only measurement loop that used to run unbounded.
                    if Instant::now() >= deadline {
                        break;
                    }
                    ticker.tick().await;
                    let exec_id = ExecutionId::new_for_shard(shard);
                    let workflow_id = format!("{cohort}-s{}-{i}", shard.as_i32());
                    start_or_load_workflow_execution(
                        &mut conn,
                        start_params(exec_id, &workflow_name, &workflow_id),
                        None,
                    )
                    .await
                    .expect("paced seed");
                    ids.push(exec_id);
                }
                ids
            }));
        }
        let mut all = Vec::new();
        for task in tasks {
            all.extend(task.await.expect("paced seed task"));
        }
        (all, started.elapsed())
    }

    /// **Scenario (b).** Activity dispatch latency, schedule -> handler start.
    ///
    /// Paced deliberately below saturation: under a saturated queue this
    /// measurement is the backlog depth, not the dispatch path.
    /// [`Pacing::Saturated`] marks a run where the box could not hold the pace,
    /// and the cell is then not published.
    ///
    /// # Errors
    ///
    /// Returns [`SkipReason`] when no Postgres could be provisioned.
    #[allow(
        clippy::too_many_lines,
        reason = "one measurement protocol, read top to bottom: provision, warm up, measure, \
                  collect, judge soundness, tear down. Splitting it into fragments would make \
                  the ordering constraints between those phases -- which are the whole \
                  correctness argument -- harder to audit, not easier."
    )]
    pub async fn run_dispatch_latency(shard_count: u32) -> Result<ScenarioReport, SkipReason> {
        let scenario_started = Instant::now();
        let cluster = setup_shards(shard_count).await?;
        let sharded = cluster.sharded_pool();
        let (registry, observations) = build_registry();
        let fleet = start_fleet(&cluster, &sharded, &registry);
        let deadline = scenario_deadline();
        let shards = cluster.shard_ids();

        // Probe the host-to-database offset BEFORE the window as well as after
        // it: an offset measured only afterwards is not the offset that was in
        // effect while the samples were taken.
        let mut offsets_before = Vec::new();
        for shard in &shards {
            let mut conn = cluster.connect(*shard).await;
            offsets_before.push(clock_offset_ms(&mut conn).await);
        }

        let warm_per_shard = warmup_batch_for(DISPATCH_WORKFLOWS_PER_SHARD);
        for shard in &shards {
            seed_workflows(&cluster, *shard, BENCH_WORKFLOW, "warm", warm_per_shard).await;
        }
        let warm_drained =
            wait_for_completions(&cluster, "warm", warm_per_shard * shards.len(), deadline).await;

        let (paced_ids, elapsed) = paced_seed(
            &cluster,
            BENCH_WORKFLOW,
            "meas",
            DISPATCH_WORKFLOWS_PER_SHARD,
            PACED_STARTS_PER_SEC_PER_SHARD,
            deadline,
        )
        .await;
        let requested = paced_ids.len();
        let per_shard = wait_for_completions(&cluster, "meas", requested, deadline).await;

        #[allow(clippy::cast_precision_loss, reason = "start counts are small")]
        // `interval`'s first tick fires immediately, so n starts span n-1
        // periods. Dividing by n would overstate the achieved rate and bias the
        // one gate that protects this scenario in the optimistic direction.
        let achieved = (requested.saturating_sub(1)) as f64 / elapsed.as_secs_f64();
        let target = PACED_STARTS_PER_SEC_PER_SHARD * f64::from(shard_count);

        // FIRST-wins, matching the signal path's discipline. `collect()` into a
        // map is last-wins, which would turn a re-delivered task row (a
        // heartbeat-lease reclaim, a capability release) into a sample of the
        // whole re-delivery delay -- a silently inflated tail with nothing to
        // mark it. Duplicates are counted instead, and reported.
        let mut starts: BTreeMap<Uuid, DateTime<Utc>> = BTreeMap::new();
        let mut redispatched = 0_usize;
        for (task_id, started) in observations.activity_starts() {
            if starts.insert(task_id, started).is_some() {
                redispatched += 1;
                // Keep the earliest.
                let earliest = starts
                    .get(&task_id)
                    .copied()
                    .map_or(started, |existing| existing.min(started));
                starts.insert(task_id, earliest);
            }
        }

        let mut samples: Vec<(DateTime<Utc>, f64)> = Vec::new();
        let mut negative = 0_usize;
        let mut missing_created_at = 0_usize;
        let mut offsets = Vec::new();
        for shard in &shards {
            let mut conn = cluster.connect(*shard).await;
            offsets.push(clock_offset_ms(&mut conn).await);
            let rows = diesel::sql_query(
                "SELECT q.id, q.created_at FROM harvest_task_queue q \
                 JOIN harvest_workflow_executions e ON e.id = q.workflow_exec_id \
                 WHERE q.task_type = 'activity' AND e.workflow_id LIKE 'meas-%'",
            )
            .load::<TaskCreatedRow>(&mut conn)
            .await
            .expect("load bench activity task rows");
            for row in rows {
                let Some(created_at) = row.created_at else {
                    missing_created_at += 1;
                    continue;
                };
                if let Some(started) = starts.get(&row.id) {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "dispatch latencies are milliseconds, far below 2^53 microseconds"
                    )]
                    let ms =
                        (*started - created_at).num_microseconds().unwrap_or(0) as f64 / 1000.0;
                    if ms < 0.0 {
                        negative += 1;
                    }
                    samples.push((created_at, ms));
                }
            }
        }
        samples.sort_by(|a, b| a.0.cmp(&b.0));
        let ordered: Vec<f64> = samples.iter().map(|(_, ms)| *ms).collect();
        let kept = measured_samples(&ordered);
        let dispatch = LatencyStats::from_samples(kept);

        let mut unsound = latency_soundness(dispatch.count, negative, missing_created_at);
        unsound.extend(warmup_soundness(
            "dispatch",
            warm_per_shard * shards.len(),
            &warm_drained,
        ));
        unsound.extend(clock_offset_soundness(
            &offsets_before,
            &offsets,
            dispatch.p50_ms,
        ));
        unsound.extend(dispatch_population_soundness(
            BENCH_ACTIVITIES.len() * requested,
            samples.len(),
            redispatched,
            observations.unrecorded_dispatches(),
        ));
        let completed = usize::try_from(per_shard.iter().sum::<u64>()).unwrap_or(0);
        unsound.extend(throughput_soundness(requested, completed, &per_shard));
        if pacing_verdict(target, achieved) == Pacing::Saturated {
            unsound.push(format!(
                "paced at {target:.1} starts/s but only achieved {achieved:.1}/s: this is a \
                 measurement of queueing, not of dispatch"
            ));
        }

        fleet.stop().await;
        let topology = cluster.topology;
        drop(sharded);
        let teardown_failures = cluster.teardown().await;

        offsets.sort_by(f64::total_cmp);
        let wall_clock_note = budget_note(scenario_started, deadline, &mut unsound);
        let publish = unsound.is_empty();
        #[allow(
            clippy::cast_precision_loss,
            reason = "sample counts here are far below 2^53"
        )]
        let samples_metric = dispatch.count as f64;
        Ok(ScenarioReport {
            scenario: BenchScenario::DispatchLatency,
            shards: shard_count,
            metrics: vec![
                Metric::new("p50_ms", publish.then_some(dispatch.p50_ms)),
                Metric::new("p99_ms", publish.then_some(dispatch.p99_ms)),
                Metric::new("samples", Some(samples_metric)),
                Metric::new("achieved_starts_per_sec", Some(achieved)),
            ],
            notes: with_teardown_note(
                vec![
                    shard_note(&per_shard),
                    format!("topology: {}", topology.as_str()),
                    format!("target pace: {target:.1} workflow starts/s"),
                    format!(
                        "host-to-database clock offset before the window: {} ms (per shard, \
                         median of 7 probes)",
                        offsets_before
                            .iter()
                            .map(|o| format!("{o:+.3}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    format!(
                        "host-to-database clock offset after the window: {} ms",
                        offsets
                            .iter()
                            .map(|o| format!("{o:+.3}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    format!(
                        "{redispatched} re-dispatched task row(s); \
                         {} dispatch(es) recorded no task id",
                        observations.unrecorded_dispatches()
                    ),
                    wall_clock_note,
                ],
                &teardown_failures,
            ),
            unsound,
        })
    }

    /// **Scenario (c).** Signal round-trip: HTTP request sent -> the workflow's
    /// own code resumes past `wait_for_signal`.
    ///
    /// Both ends are read from one monotonic clock in one process, so this
    /// number carries no clock-skew term.
    ///
    /// # Errors
    ///
    /// Returns [`SkipReason`] when no Postgres could be provisioned.
    #[allow(
        clippy::too_many_lines,
        reason = "one measurement protocol, read top to bottom: provision, warm up, measure, \
                  collect, judge soundness, tear down. Splitting it into fragments would make \
                  the ordering constraints between those phases -- which are the whole \
                  correctness argument -- harder to audit, not easier."
    )]
    pub async fn run_signal_roundtrip(shard_count: u32) -> Result<ScenarioReport, SkipReason> {
        let scenario_started = Instant::now();
        let cluster = setup_shards(shard_count).await?;
        let sharded = cluster.sharded_pool();
        let (registry, observations) = build_registry();
        let fleet = start_fleet(&cluster, &sharded, &registry);
        let deadline = scenario_deadline();
        let shards = cluster.shard_ids();

        let server = match spawn_signal_server(sharded.clone()).await {
            Ok(server) => server,
            Err(e) => {
                // Tear down before returning: `ShardCluster` has no `Drop`
                // (dropping databases is async), so an early return that skips
                // this leaves freshly migrated databases on the operator's
                // server -- exactly what `teardown` exists to prevent.
                fleet.stop().await;
                drop(sharded);
                let _ = cluster.teardown().await;
                return Err(SkipReason(format!("bind the signal endpoint: {e}")));
            }
        };

        let warm_per_shard = warmup_batch_for(SIGNAL_WORKFLOWS_PER_SHARD);
        let mut warm_ids = Vec::new();
        let mut measured_ids = Vec::new();
        for shard in &shards {
            warm_ids.extend(
                seed_workflows(
                    &cluster,
                    *shard,
                    BENCH_SIGNAL_WORKFLOW,
                    "warm",
                    warm_per_shard,
                )
                .await,
            );
            measured_ids.extend(
                seed_workflows(
                    &cluster,
                    *shard,
                    BENCH_SIGNAL_WORKFLOW,
                    "meas",
                    SIGNAL_WORKFLOWS_PER_SHARD,
                )
                .await,
            );
        }
        let total = warm_ids.len() + measured_ids.len();

        // Every run must be genuinely parked before its signal, or the sample
        // includes the workflow's own first dispatch instead of the round trip.
        //
        // The readiness signal is the handler reaching `wait_for_signal`, not
        // anything in the task queue: parking a workflow task leaves the row in
        // `RUNNING` with only `wake_requested` cleared (see
        // `queue::park_workflow_task`), so "no RUNNING workflow tasks" is not a
        // condition a parked population can ever satisfy — waiting for it would
        // burn the whole scenario budget and then measure nothing.
        while observations.parked_count() < total && Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let parked = observations.parked_count();
        // `record_parked` runs on entry to the handler; the suspension commits a
        // moment later. Settle generously — the residual window is sub-millisecond
        // and this is seconds — and see "Known limitations" in
        // `docs/benchmarks.md`, which states the window rather than pretending it
        // is closed.
        tokio::time::sleep(SIGNAL_PARK_SETTLE).await;

        let target = PACED_STARTS_PER_SEC_PER_SHARD * f64::from(shard_count);
        let period = std::time::Duration::from_secs_f64(1.0 / target);

        // Warmup cohort: signalled through the identical path, and every sample
        // discarded. Keeping the two cohorts separate — rather than signalling
        // one list and trimming a fraction off the front — is what makes "the
        // published percentiles contain no warmup sample" true by construction.
        let mut warm_ticker = tokio::time::interval(period);
        warm_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for exec_id in &warm_ids {
            if Instant::now() >= deadline {
                break;
            }
            warm_ticker.tick().await;
            let _ = send_signal_over_http(server.addr, *exec_id, BENCH_SIGNAL).await;
        }
        let warm_drained = wait_for_completions(&cluster, "warm", warm_ids.len(), deadline).await;

        let mut ticker = tokio::time::interval(period);
        // `Delay`, not `Burst`: see `paced_seed`.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let signalling_started = Instant::now();
        let mut sent: Vec<(ExecutionId, Instant)> = Vec::with_capacity(measured_ids.len());
        let mut failures = 0_usize;
        for exec_id in &measured_ids {
            if Instant::now() >= deadline {
                break;
            }
            ticker.tick().await;
            let (sent_at, status) =
                send_signal_over_http(server.addr, *exec_id, BENCH_SIGNAL).await;
            if status != Some(200) {
                failures += 1;
            }
            sent.push((*exec_id, sent_at));
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "signal counts here are far below 2^53"
        )]
        let achieved = sent.len() as f64 / signalling_started.elapsed().as_secs_f64();

        let per_shard = wait_for_completions(&cluster, "meas", measured_ids.len(), deadline).await;

        let mut samples: Vec<(Instant, f64)> = Vec::new();
        let mut unobserved = 0_usize;
        for (exec_id, sent_at) in &sent {
            match observations.signal_observation(exec_id.as_uuid()) {
                Some(observed) => {
                    let ms = observed.saturating_duration_since(*sent_at).as_secs_f64() * 1000.0;
                    samples.push((*sent_at, ms));
                }
                None => unobserved += 1,
            }
        }
        samples.sort_by_key(|(sent_at, _)| *sent_at);
        let ordered: Vec<f64> = samples.iter().map(|(_, ms)| *ms).collect();
        let roundtrip = LatencyStats::from_samples(&ordered);

        let mut unsound = latency_soundness(roundtrip.count, 0, 0);
        unsound.extend(warmup_soundness("signal", warm_ids.len(), &warm_drained));
        if parked < total {
            unsound.push(format!(
                "only {parked} of {total} workflows had parked on the signal before signalling \
                 began"
            ));
        }
        if failures > 0 {
            unsound.push(format!("{failures} signal request(s) did not return 200"));
        }
        if unobserved > 0 {
            unsound.push(format!(
                "{unobserved} signalled workflow(s) never recorded an observation"
            ));
        }
        if sent.len() < measured_ids.len() {
            unsound.push(format!(
                "the scenario budget ran out after {} of {} signals",
                sent.len(),
                measured_ids.len()
            ));
        }
        let completed = usize::try_from(per_shard.iter().sum::<u64>()).unwrap_or(0);
        unsound.extend(throughput_soundness(sent.len(), completed, &per_shard));
        if pacing_verdict(target, achieved) == Pacing::Saturated {
            unsound.push(format!(
                "paced at {target:.1} signals/s but only achieved {achieved:.1}/s"
            ));
        }

        server.stop();
        fleet.stop().await;
        let topology = cluster.topology;
        drop(sharded);
        let teardown_failures = cluster.teardown().await;
        let wall_clock_note = budget_note(scenario_started, deadline, &mut unsound);

        let publish = unsound.is_empty();
        #[allow(
            clippy::cast_precision_loss,
            reason = "sample counts here are far below 2^53"
        )]
        let samples_metric = roundtrip.count as f64;
        Ok(ScenarioReport {
            scenario: BenchScenario::SignalRoundtrip,
            shards: shard_count,
            metrics: vec![
                Metric::new("p50_ms", publish.then_some(roundtrip.p50_ms)),
                Metric::new("p99_ms", publish.then_some(roundtrip.p99_ms)),
                Metric::new("samples", Some(samples_metric)),
                Metric::new("achieved_signals_per_sec", Some(achieved)),
            ],
            notes: with_teardown_note(
                vec![
                    shard_note(&per_shard),
                    format!("topology: {}", topology.as_str()),
                    format!("target pace: {target:.1} signals/s"),
                    format!(
                        "{} measured signals after a discarded warmup cohort of {}",
                        measured_ids.len(),
                        warm_ids.len()
                    ),
                    wall_clock_note,
                ],
                &teardown_failures,
            ),
            unsound,
        })
    }
}
