//! Open-loop capacity scenario for the end-to-end benchmark.
//!
//! The closed-loop `throughput` cell holds a fixed population in flight. Its
//! rate is that population divided by the workflow latency. This module
//! measures capacity instead: it offers a fixed start rate, steps the rate up,
//! and keeps the highest step that stays bounded.
//!
//! `docs/design/DESIGN-2419.md` records the plan and the rule.

#![allow(dead_code)]

use super::e2e_bench_support::{BenchScenario, CapacityArm, POOL_SIZE_PER_SHARD, ScenarioReport};

// ---------------------------------------------------------------------------
// Pure section. No database and no `db` feature.
// ---------------------------------------------------------------------------

/// Duration of one ladder step, in seconds.
pub const CAPACITY_STEP_SECS: f64 = 20.0;
/// Each step offers this multiple of the rate of the step before it.
pub const CAPACITY_RATE_MULTIPLIER: f64 = 1.25;
/// Most steps one ladder runs.
pub const CAPACITY_MAX_STEPS: usize = 20;
/// Time a step's runs get to complete after its last arrival.
pub const CAPACITY_DRAIN_GRACE_SECS: u64 = 20;
/// Fewest runs a step must start before a verdict can rest on it. Each
/// judged window then holds at least 100 runs, so its p99 is not its maximum.
pub const CAPACITY_MIN_STEP_RUNS: usize = 400;
// Each judged window holds at least 100 runs, so its p99 is not its maximum.
const _: () = assert!(CAPACITY_MIN_STEP_RUNS / CAPACITY_WINDOWS >= 100);

/// Fraction of the offered rate the generator must achieve.
pub const CAPACITY_PACING_HOLD_RATIO: f64 = 0.95;
/// Largest backlog slope, as a fraction of the offered rate. It has no
/// absolute floor, so a low rate gets no wider margin.
pub const CAPACITY_MAX_BACKLOG_GROWTH_FRACTION: f64 = 0.05;
/// Largest growth of the p99 from the first judged window to the last.
pub const CAPACITY_MAX_P99_GROWTH_RATIO: f64 = 2.0;
/// Absolute slack added to the p99 growth limit, in milliseconds.
pub const CAPACITY_P99_GROWTH_SLACK_MS: f64 = 100.0;
/// Equal windows of a step, by admission. The first window is settle time.
/// The rule does not judge it.
pub const CAPACITY_WINDOWS: usize = 4;
/// Interval of the derived backlog series, in seconds.
pub const CAPACITY_BACKLOG_SAMPLE_SECS: f64 = 0.1;
/// Largest growth of the median latency across the judged windows, in ms per
/// second of step.
pub const CAPACITY_MAX_LATENCY_GROWTH_MS_PER_SEC: f64 = 5.0;
/// Largest p99 generator wait, from schedule to admission, in milliseconds.
pub const CAPACITY_MAX_GENERATOR_WAIT_MS: f64 = 100.0;
/// Connections in the generator pool of each shard.
pub const CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD: usize = 16;
/// Worker processes started per shard.
pub const CAPACITY_WORKER_PROCESSES_PER_SHARD: usize = 4;
/// Override of [`CAPACITY_WORKER_PROCESSES_PER_SHARD`].
pub const WORKER_PROCESSES_ENV_VAR: &str = "HARVEST_BENCH_WORKER_PROCESSES";

/// First offered rate of an arm, in starts per second per shard.
///
/// Each start rate gives [`CAPACITY_MIN_STEP_RUNS`] runs in one step.
#[must_use]
pub const fn start_rate_per_shard(arm: CapacityArm) -> f64 {
    match arm {
        CapacityArm::Noop => 40.0,
        CapacityArm::Canonical => 20.0,
    }
}

/// Offered rate of ladder step `step`, rounded to 0.1 starts per second.
#[must_use]
pub fn ladder_rate(start: f64, step: usize) -> f64 {
    let exponent = i32::try_from(step).unwrap_or(i32::MAX);
    (start * CAPACITY_RATE_MULTIPLIER.powi(exponent) * 10.0).round() / 10.0
}

/// One run of a step, in seconds after the step began.
#[allow(
    clippy::struct_field_names,
    reason = "each field is an instant, and the suffix names its unit"
)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Arrival {
    /// The instant the open loop scheduled the start.
    pub scheduled_secs: f64,
    /// The instant the generator began the start call on a connection.
    pub admitted_secs: f64,
    /// The instant the run completed, or `None` when it did not complete
    /// within the drain grace.
    pub completed_secs: Option<f64>,
}

/// What one shard did during one step.
#[derive(Debug, Clone, PartialEq)]
pub struct StepEvidence {
    /// Offered starts per second on this shard.
    pub offered_per_sec: f64,
    /// Starts per second the generator achieved on this shard.
    pub achieved_per_sec: f64,
    /// Length of the arrival phase, in seconds.
    pub step_secs: f64,
    /// Every run the step started on this shard.
    pub arrivals: Vec<Arrival>,
    /// Runs that ended in a state other than completed.
    pub failed: usize,
}

/// Runs admitted at or before `t` that had not completed at `t`.
///
/// This is the engine's backlog. A run the generator has scheduled but not
/// yet admitted waits in the generator, so it does not count here.
#[must_use]
pub fn backlog_at(arrivals: &[Arrival], t: f64) -> usize {
    arrivals
        .iter()
        .filter(|a| a.admitted_secs <= t && a.completed_secs.is_none_or(|done| done > t))
        .count()
}

/// The backlog sampled every `every` seconds over `[from, to]`.
#[must_use]
pub fn backlog_series(arrivals: &[Arrival], from: f64, to: f64, every: f64) -> Vec<(f64, f64)> {
    if every.is_nan() || every <= 0.0 || !from.is_finite() || !to.is_finite() || to < from {
        return Vec::new();
    }
    // A small tolerance keeps `to` itself in the series despite float error.
    let span = (to - from) / every + 1e-9;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "span is finite and non-negative here, and a step has few samples"
    )]
    let last = span.floor() as usize;
    (0..=last)
        .map(|k| {
            #[allow(clippy::cast_precision_loss, reason = "sample indexes are small")]
            let t = (k as f64).mul_add(every, from);
            #[allow(clippy::cast_precision_loss, reason = "a backlog is far below 2^53")]
            let depth = backlog_at(arrivals, t) as f64;
            (t, depth)
        })
        .collect()
}

/// Least-squares slope of `points`, or `None` for fewer than two distinct
/// abscissas.
#[must_use]
pub fn least_squares_slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    #[allow(clippy::cast_precision_loss, reason = "a series has few points")]
    let n = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|(x, _)| (x - mean_x).powi(2)).sum();
    if sxx <= f64::EPSILON {
        return None;
    }
    let sxy: f64 = points
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    Some(sxy / sxx)
}

/// The p99 of `latencies_secs`, in milliseconds. A `None` entry is a run that
/// never completed, so it ranks above every finite latency.
#[must_use]
pub fn p99_ms(latencies_secs: &[Option<f64>]) -> Option<f64> {
    percentile_ms(latencies_secs, 0.99)
}

/// The nearest-rank `quantile` of `latencies_secs`, in milliseconds. A `None`
/// entry ranks above every finite latency.
#[must_use]
pub fn percentile_ms(latencies_secs: &[Option<f64>], quantile: f64) -> Option<f64> {
    if latencies_secs.is_empty() || !(0.0..=1.0).contains(&quantile) {
        return None;
    }
    let mut ranked: Vec<f64> = latencies_secs
        .iter()
        .map(|l| l.map_or(f64::INFINITY, |secs| secs * 1000.0))
        .collect();
    ranked.sort_by(f64::total_cmp);
    // Nearest rank: the smallest value with the quantile at or below it.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "the rank is positive and at most the set size"
    )]
    let rank = (quantile * ranked.len() as f64).ceil() as usize;
    ranked.get(rank.saturating_sub(1)).copied()
}

/// The p99 latency of each window of a step, in milliseconds.
///
/// A step splits into [`CAPACITY_WINDOWS`] equal windows by admission.
/// Latency runs from admission to completion. A window with no run has no
/// p99.
#[must_use]
pub fn window_p99_ms(arrivals: &[Arrival], step_secs: f64) -> Vec<Option<f64>> {
    window_latencies(arrivals, step_secs)
        .iter()
        .map(|w| p99_ms(w))
        .collect()
}

/// The median latency of each window of a step, in milliseconds.
#[must_use]
pub fn window_p50_ms(arrivals: &[Arrival], step_secs: f64) -> Vec<Option<f64>> {
    window_latencies(arrivals, step_secs)
        .iter()
        .map(|w| percentile_ms(w, 0.5))
        .collect()
}

/// The latencies of each window, grouped by admission.
fn window_latencies(arrivals: &[Arrival], step_secs: f64) -> Vec<Vec<Option<f64>>> {
    let mut windows: Vec<Vec<Option<f64>>> = vec![Vec::new(); CAPACITY_WINDOWS];
    if step_secs.is_nan() || step_secs <= 0.0 {
        return windows;
    }
    #[allow(clippy::cast_precision_loss, reason = "the window count is small")]
    let width = step_secs / CAPACITY_WINDOWS as f64;
    for a in arrivals {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the quotient is clamped to the window range"
        )]
        let index = ((a.admitted_secs / width).floor().max(0.0) as usize).min(CAPACITY_WINDOWS - 1);
        windows[index].push(a.completed_secs.map(|done| done - a.admitted_secs));
    }
    windows
}

/// What one shard's evidence shows for one step.
#[derive(Debug, Clone, PartialEq)]
pub struct ShardJudgement {
    /// Backlog slope over the judged windows, in runs per second.
    pub backlog_slope: Option<f64>,
    /// p99 of each window, in milliseconds.
    pub window_p99_ms: Vec<Option<f64>>,
    /// Reasons the engine did not keep up.
    pub engine: Vec<String>,
    /// Reasons the generator did not offer the load.
    pub generator: Vec<String>,
    /// Growth of the median latency across the judged windows, in ms per
    /// second.
    pub median_growth: Option<f64>,
    /// p99 of the generator wait, from schedule to admission, in ms.
    pub generator_wait_p99_ms: Option<f64>,
    /// Runs that did not complete within the drain grace.
    pub unfinished: usize,
    /// Whether the only engine reason is the p99 growth rule.
    pub tail_only: bool,
}

/// Judge one shard's evidence for one step.
///
/// The generator side asks whether the load was offered. The engine side
/// asks whether the engine kept up with the load it was given. Engine
/// latency and backlog start at admission. The generator wait, from schedule
/// to admission, is judged on the generator side.
#[must_use]
pub fn judge_shard(evidence: &StepEvidence) -> ShardJudgement {
    let mut generator = Vec::new();
    let mut engine = Vec::new();

    let offered = evidence.offered_per_sec;
    if offered.is_nan()
        || offered <= 0.0
        || evidence.achieved_per_sec < offered * CAPACITY_PACING_HOLD_RATIO
    {
        generator.push(format!(
            "the generator achieved {:.1} of {offered:.1} starts/s, under the {:.0}% hold ratio",
            evidence.achieved_per_sec,
            CAPACITY_PACING_HOLD_RATIO * 100.0,
        ));
    }
    if evidence.arrivals.len() < CAPACITY_MIN_STEP_RUNS {
        generator.push(format!(
            "only {} runs started; a verdict needs at least {CAPACITY_MIN_STEP_RUNS} runs",
            evidence.arrivals.len(),
        ));
    }
    let generator_wait_p99_ms = generator_wait_p99_ms(&evidence.arrivals);
    if let Some(wait) = generator_wait_p99_ms.filter(|w| *w > CAPACITY_MAX_GENERATOR_WAIT_MS) {
        generator.push(format!(
            "the generator wait p99 was {wait:.1} ms, over the \
             {CAPACITY_MAX_GENERATOR_WAIT_MS:.0} ms limit"
        ));
    }

    let unfinished = evidence
        .arrivals
        .iter()
        .filter(|a| a.completed_secs.is_none())
        .count();
    if unfinished > 0 {
        engine.push(format!(
            "{unfinished} runs did not complete within the {CAPACITY_DRAIN_GRACE_SECS}s drain grace"
        ));
    }
    if evidence.failed > 0 {
        engine.push(format!("{} runs failed", evidence.failed));
    }

    // The first window is settle time. The judged span is the rest.
    #[allow(clippy::cast_precision_loss, reason = "the window count is small")]
    let width = evidence.step_secs / CAPACITY_WINDOWS as f64;
    let series = backlog_series(
        &evidence.arrivals,
        width,
        evidence.step_secs,
        CAPACITY_BACKLOG_SAMPLE_SECS,
    );
    let backlog_slope = least_squares_slope(&series);
    let limit = offered * CAPACITY_MAX_BACKLOG_GROWTH_FRACTION;
    match backlog_slope {
        Some(slope) if slope > limit => engine.push(format!(
            "the backlog grew {slope:.2} runs/s, over the {limit:.2} runs/s limit"
        )),
        Some(_) => {}
        None => engine.push("the backlog series has too few samples for a slope".to_owned()),
    }

    let medians = window_p50_ms(&evidence.arrivals, evidence.step_secs);
    #[allow(clippy::cast_precision_loss, reason = "the window count is small")]
    let spacing = width * (CAPACITY_WINDOWS - 2) as f64;
    let median_growth = match (medians.get(1), medians.last()) {
        (Some(Some(first)), Some(Some(last))) if spacing > 0.0 => Some((last - first) / spacing),
        _ => None,
    };
    if let Some(growth) = median_growth.filter(|g| *g > CAPACITY_MAX_LATENCY_GROWTH_MS_PER_SEC) {
        engine.push(format!(
            "the median latency grew {growth:.1} ms/s, over the \
             {CAPACITY_MAX_LATENCY_GROWTH_MS_PER_SEC:.0} ms/s limit"
        ));
    }

    let windows = window_p99_ms(&evidence.arrivals, evidence.step_secs);
    let mut tail_failed = false;
    if let (Some(Some(first)), Some(Some(last))) = (windows.get(1), windows.last()) {
        let ceiling = first.mul_add(CAPACITY_MAX_P99_GROWTH_RATIO, CAPACITY_P99_GROWTH_SLACK_MS);
        if *last > ceiling {
            tail_failed = true;
            engine.push(format!(
                "the p99 grew from {first:.1} ms to {last:.1} ms, over the {ceiling:.1} ms limit"
            ));
        }
    }
    let tail_only = tail_failed && engine.len() == 1;

    ShardJudgement {
        backlog_slope,
        window_p99_ms: windows,
        engine,
        generator,
        median_growth,
        generator_wait_p99_ms,
        unfinished,
        tail_only,
    }
}

/// p99 of the generator wait, from schedule to admission, in milliseconds.
#[must_use]
pub fn generator_wait_p99_ms(arrivals: &[Arrival]) -> Option<f64> {
    let waits: Vec<Option<f64>> = arrivals
        .iter()
        .map(|a| Some((a.admitted_secs - a.scheduled_secs).max(0.0)))
        .collect();
    p99_ms(&waits)
}

/// Whether a failed step runs once more at the same rate.
///
/// A step whose only failure is the p99 rule can be one pause in a small
/// window. It runs once more, and the second verdict counts. A backlog or
/// median failure never earns a retry.
#[must_use]
pub fn step_retryable(judgements: &[ShardJudgement]) -> bool {
    let failing: Vec<&ShardJudgement> =
        judgements.iter().filter(|j| !j.engine.is_empty()).collect();
    !failing.is_empty() && failing.iter().all(|j| j.tail_only)
}

/// Reasons a warmup step leaves the cell unsound.
#[must_use]
pub fn warmup_drain_reasons(judgements: &[ShardJudgement]) -> Vec<String> {
    let unfinished: usize = judgements.iter().map(|j| j.unfinished).sum();
    if unfinished == 0 {
        return Vec::new();
    }
    vec![format!(
        "the warmup left {unfinished} runs unfinished, so they ran inside the first step"
    )]
}

/// Connections one shard of a capacity cell needs.
///
/// Each worker process has a full pool and one listener. The generator has
/// its own pool. Four more cover the lease, the drain and the load.
#[must_use]
pub const fn connection_need_per_shard(processes: usize) -> usize {
    processes * (POOL_SIZE_PER_SHARD + 1) + CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD + 4
}

/// Reasons a server cannot hold a cell's connections.
///
/// A pool that cannot open a connection stalls its workers. The ladder would
/// then publish a starved rate, so the cell refuses to run.
#[must_use]
pub fn connection_budget_reasons(
    need: usize,
    max_connections: usize,
    reserved: usize,
) -> Vec<String> {
    let usable = max_connections.saturating_sub(reserved);
    if need <= usable {
        return Vec::new();
    }
    vec![format!(
        "the cell needs {need} connections on one server, over the {usable} that \
         max_connections leaves; raise max_connections or lower {WORKER_PROCESSES_ENV_VAR}"
    )]
}

/// The verdict on one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepVerdict {
    /// Every shard kept its backlog and its p99 bounded.
    Sustainable,
    /// The engine did not keep up on at least one shard.
    Unsustainable(Vec<String>),
    /// The engine kept up, but the generator did not offer the load. The step
    /// measures nothing.
    GeneratorBound(Vec<String>),
}

/// Combine the shard judgements of one step. The weakest shard decides.
///
/// An engine reason wins over a generator reason. A saturated database also
/// slows the start calls. An engine that falls behind a load lower than the
/// offer has failed at a load at or below the offer. A step is generator-bound
/// only when the engine kept up.
#[must_use]
pub fn step_verdict(judgements: &[ShardJudgement]) -> StepVerdict {
    let label = |idx: usize, reason: &String| {
        if judgements.len() > 1 {
            format!("shard {idx}: {reason}")
        } else {
            reason.clone()
        }
    };
    let collect = |pick: fn(&ShardJudgement) -> &Vec<String>| -> Vec<String> {
        judgements
            .iter()
            .enumerate()
            .flat_map(|(idx, j)| pick(j).iter().map(move |r| label(idx, r)))
            .collect()
    };
    let generator = collect(|j| &j.generator);
    let mut engine = collect(|j| &j.engine);
    if !engine.is_empty() {
        engine.extend(generator);
        return StepVerdict::Unsustainable(engine);
    }
    if !generator.is_empty() {
        return StepVerdict::GeneratorBound(generator);
    }
    StepVerdict::Sustainable
}

/// One judged step of a ladder.
#[derive(Debug, Clone, PartialEq)]
pub struct StepRecord {
    /// Offered starts per second per shard.
    pub offered_per_shard: f64,
    /// The step's verdict.
    pub verdict: StepVerdict,
}

/// Why a ladder stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LadderStop {
    /// A step was not sustainable. This is the only stop that finds the
    /// maximum.
    EngineBound,
    /// The generator could not offer a step's load.
    GeneratorBound,
    /// Every step of a full ladder was sustainable.
    LadderExhausted,
    /// The ladder stopped early with every step sustainable, for example on
    /// the wall-clock budget.
    CutShort,
}

/// The outcome of one ladder.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LadderResult {
    /// The sustainable rate per shard, or `None` when no step was.
    pub sustainable_per_shard: Option<f64>,
    /// Why the ladder stopped.
    pub stop: LadderStop,
}

/// The sustainable rate of a ladder.
///
/// It is the offer of the last step in the leading run of sustainable steps.
/// The ladder judges steps in order. A sustainable step after the first
/// failure does not count. A noisy pass above the knee is not capacity.
#[must_use]
pub fn sustainable_rate(steps: &[StepRecord], max_steps: usize) -> LadderResult {
    let mut last_sustainable = None;
    for step in steps {
        let stop = match step.verdict {
            StepVerdict::Sustainable => {
                last_sustainable = Some(step.offered_per_shard);
                continue;
            }
            StepVerdict::Unsustainable(_) => LadderStop::EngineBound,
            StepVerdict::GeneratorBound(_) => LadderStop::GeneratorBound,
        };
        return LadderResult {
            sustainable_per_shard: last_sustainable,
            stop,
        };
    }
    LadderResult {
        sustainable_per_shard: last_sustainable,
        stop: if steps.len() >= max_steps {
            LadderStop::LadderExhausted
        } else {
            LadderStop::CutShort
        },
    }
}

/// Reasons a ladder's sustainable rate must not be published.
#[must_use]
pub fn ladder_soundness(result: &LadderResult) -> Vec<String> {
    let rate = result
        .sustainable_per_shard
        .map_or_else(|| "no rate".to_owned(), |r| format!("{r:.1} starts/s"));
    match (result.stop, result.sustainable_per_shard) {
        (LadderStop::EngineBound, Some(_)) => Vec::new(),
        (LadderStop::EngineBound, None) => vec![
            "the first step was not sustainable; lower the start rate and run again".to_owned(),
        ],
        (LadderStop::GeneratorBound, _) => vec![format!(
            "the generator could not offer the next step, so {rate} is a generator limit, not \
             the engine's"
        )],
        (LadderStop::LadderExhausted, _) => vec![format!(
            "every step of the ladder was sustainable, so {rate} is a lower bound, not the maximum"
        )],
        (LadderStop::CutShort, _) => vec![format!(
            "the ladder stopped before a step failed, so {rate} is a lower bound, not the maximum"
        )],
    }
}

/// Metric key of the aggregate sustainable rate of a capacity cell.
pub const SUSTAINABLE_METRIC: &str = "sustainable_per_sec";
/// Metric key of the distinct database hosts of a capacity cell.
pub const DATABASE_HOSTS_METRIC: &str = "database_hosts";
/// Metric key of the distinct worker hosts of a capacity cell.
pub const WORKER_HOSTS_METRIC: &str = "worker_hosts";

/// The shard-scaling lines of a run.
///
/// A ratio is printed only for sound capacity cells. A multi-shard cell gives
/// no ratio when its shards share a database host, or its workers share a
/// worker host. So a co-located cell never appears in a scaling claim.
#[must_use]
pub fn scaling_lines(reports: &[ScenarioReport]) -> Vec<String> {
    let mut lines = Vec::new();
    for arm in BenchScenario::capacity() {
        let id = arm.as_str();
        let cells: Vec<&ScenarioReport> = reports.iter().filter(|r| r.scenario == arm).collect();
        let base = cells
            .iter()
            .find(|r| r.shards == 1 && r.is_sound())
            .and_then(|r| r.metric(SUSTAINABLE_METRIC));
        for cell in cells.iter().filter(|r| r.shards > 1) {
            let shards = cell.shards;
            let hosts = cell.metric(DATABASE_HOSTS_METRIC).unwrap_or(0.0);
            let worker_hosts = cell.metric(WORKER_HOSTS_METRIC).unwrap_or(0.0);
            let rate = cell.metric(SUSTAINABLE_METRIC);
            let line = match (base, rate) {
                _ if !cell.is_sound() => {
                    format!("{id}: no scaling claim at {shards} shards: the cell is not sound")
                }
                _ if hosts < f64::from(shards) => {
                    format!("{id}: no scaling claim at {shards} shards: the shards share a host")
                }
                _ if worker_hosts < f64::from(shards) => format!(
                    "{id}: no scaling claim at {shards} shards: the workers of every shard share \
                     the bench host"
                ),
                (None, _) => format!(
                    "{id}: no scaling claim at {shards} shards: the 1-shard cell has no sound rate"
                ),
                (_, None) => {
                    format!("{id}: no scaling claim at {shards} shards: the cell has no rate")
                }
                (Some(one), Some(many)) => format!(
                    "{id}: {shards} shards sustain {:.2}x the 1-shard rate",
                    many / one
                ),
            };
            lines.push(line);
        }
    }
    lines
}

/// The results file of the published capacity run.
pub const PUBLISHED_CAPACITY_FILE: &str = "docs/benchmarks/capacity-2026-11.md";

/// One published capacity cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PublishedCapacity {
    /// The arm.
    pub arm: CapacityArm,
    /// Shards in the cell.
    pub shards: u32,
    /// The aggregate sustainable rate, in starts per second.
    pub sustainable_per_sec: f64,
}

/// The capacity cells [`PUBLISHED_CAPACITY_FILE`] publishes.
///
/// The docs guard pins each value to that file and to `docs/comparison.md`.
pub const PUBLISHED_CAPACITY: &[PublishedCapacity] = &[
    PublishedCapacity {
        arm: CapacityArm::Noop,
        shards: 1,
        sustainable_per_sec: 190.7,
    },
    PublishedCapacity {
        arm: CapacityArm::Canonical,
        shards: 1,
        sustainable_per_sec: 39.1,
    },
];

/// Environment variable that turns the bench binary into one worker
/// process. Its value is `shard|worker id`.
pub const CHILD_ENV_VAR: &str = "HARVEST_BENCH_CAPACITY_CHILD";
/// Environment variable with every shard URL of a worker process, one
/// `shard=url` entry per line.
pub const CHILD_URLS_ENV_VAR: &str = "HARVEST_BENCH_CAPACITY_CHILD_URLS";
/// The line a worker process prints once its worker runs.
pub const CHILD_READY_LINE: &str = "ready";

/// Encode the shard and worker id of one worker process.
#[must_use]
pub fn encode_child_spec(shard: i32, worker_id: &str) -> String {
    format!("{shard}|{worker_id}")
}

/// Decode [`encode_child_spec`]. A malformed value gives `None`.
#[must_use]
pub fn parse_child_spec(raw: &str) -> Option<(i32, String)> {
    let (shard, worker_id) = raw.split_once('|')?;
    if worker_id.is_empty() {
        return None;
    }
    Some((shard.parse().ok()?, worker_id.to_owned()))
}

/// Encode shard URLs for a worker process, one `shard=url` per line.
#[must_use]
pub fn encode_child_urls<'a>(urls: impl IntoIterator<Item = (i32, &'a str)>) -> String {
    urls.into_iter()
        .map(|(shard, url)| format!("{shard}={url}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Decode [`encode_child_urls`]. One malformed line gives `None`.
#[must_use]
pub fn parse_child_urls(raw: &str) -> Option<Vec<(i32, String)>> {
    let urls: Option<Vec<(i32, String)>> = raw
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let (shard, url) = line.split_once('=')?;
            Some((shard.parse().ok()?, url.to_owned()))
        })
        .collect();
    urls.filter(|u| !u.is_empty())
}

#[cfg(test)]
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "test fixtures build small, positive run counts from rates"
)]
mod tests {
    use super::super::e2e_bench_support::Metric;
    use super::*;

    /// Arrivals at `rate` per second for `secs`, each completing `latency`
    /// seconds after its scheduled start.
    fn steady(rate: f64, secs: f64, latency: f64) -> Vec<Arrival> {
        let n = (rate * secs).round() as usize;
        (0..n)
            .map(|k| {
                let at = k as f64 / rate;
                Arrival {
                    scheduled_secs: at,
                    admitted_secs: at,
                    completed_secs: Some(at + latency),
                }
            })
            .collect()
    }

    /// Arrivals at `offered` per second served in order by a server that
    /// completes `service` runs per second.
    fn overloaded(offered: f64, service: f64, secs: f64) -> Vec<Arrival> {
        let n = (offered * secs).round() as usize;
        let mut free_at = 0.0_f64;
        (0..n)
            .map(|k| {
                let at = k as f64 / offered;
                let done = at.max(free_at) + 1.0 / service;
                free_at = done;
                Arrival {
                    scheduled_secs: at,
                    admitted_secs: at,
                    completed_secs: Some(done),
                }
            })
            .collect()
    }

    /// Arrivals offered at `offered` per second that the generator admits at
    /// only `ratio` of that rate. The engine completes each run `latency`
    /// seconds after its admission.
    fn lagging_generator(offered: f64, ratio: f64, secs: f64, latency: f64) -> Vec<Arrival> {
        let n = (offered * secs).round() as usize;
        (0..n)
            .map(|k| {
                let at = k as f64 / offered;
                let admitted = k as f64 / (offered * ratio);
                Arrival {
                    scheduled_secs: at,
                    admitted_secs: admitted,
                    completed_secs: Some(admitted + latency),
                }
            })
            .collect()
    }

    fn evidence(offered: f64, arrivals: Vec<Arrival>) -> StepEvidence {
        StepEvidence {
            offered_per_sec: offered,
            achieved_per_sec: offered,
            step_secs: CAPACITY_STEP_SECS,
            arrivals,
            failed: 0,
        }
    }

    fn record(rate: f64, verdict: StepVerdict) -> StepRecord {
        StepRecord {
            offered_per_shard: rate,
            verdict,
        }
    }

    fn engine_fail() -> StepVerdict {
        StepVerdict::Unsustainable(vec!["backlog grew".to_owned()])
    }

    #[test]
    fn a_step_whose_backlog_grows_is_not_sustainable() {
        let e = evidence(50.0, overloaded(50.0, 40.0, CAPACITY_STEP_SECS));
        let judged = judge_shard(&e);
        let slope = judged.backlog_slope.expect("a slope");
        assert!(
            (slope - 10.0).abs() < 1.0,
            "an overload of 10 runs/s grows the backlog 10 runs/s: {slope}"
        );
        assert!(
            judged.engine.iter().any(|r| r.contains("backlog")),
            "{judged:?}"
        );
        assert!(matches!(
            step_verdict(&[judged]),
            StepVerdict::Unsustainable(_)
        ));
    }

    #[test]
    fn a_step_with_a_flat_backlog_and_p99_is_sustainable() {
        let e = evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2));
        let judged = judge_shard(&e);
        assert!(judged.engine.is_empty(), "{judged:?}");
        assert!(judged.generator.is_empty(), "{judged:?}");
        assert_eq!(step_verdict(&[judged]), StepVerdict::Sustainable);
    }

    #[test]
    fn a_growing_p99_fails_the_step() {
        // The latency grows from 0.2 s to 2.2 s across the step.
        let mut arrivals = steady(50.0, CAPACITY_STEP_SECS, 0.2);
        for a in &mut arrivals {
            let extra = (a.scheduled_secs / CAPACITY_STEP_SECS).powi(4) * 2.0;
            a.completed_secs = a.completed_secs.map(|c| c + extra);
        }
        let judged = judge_shard(&evidence(50.0, arrivals));
        assert!(
            judged.engine.iter().any(|r| r.contains("p99")),
            "{judged:?}"
        );
    }

    #[test]
    fn a_run_that_never_completes_fails_the_step() {
        let mut arrivals = steady(50.0, CAPACITY_STEP_SECS, 0.2);
        arrivals[10].completed_secs = None;
        let judged = judge_shard(&evidence(50.0, arrivals));
        assert!(
            judged.engine.iter().any(|r| r.contains("did not complete")),
            "{judged:?}"
        );
    }

    #[test]
    fn a_failed_run_fails_the_step() {
        let mut e = evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2));
        e.failed = 2;
        let judged = judge_shard(&e);
        assert!(
            judged.engine.iter().any(|r| r.contains("failed")),
            "{judged:?}"
        );
    }

    #[test]
    fn a_generator_that_misses_its_rate_bounds_the_step_not_the_engine() {
        let mut e = evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2));
        e.achieved_per_sec = 40.0;
        let judged = judge_shard(&e);
        assert!(!judged.generator.is_empty(), "{judged:?}");
        assert!(matches!(
            step_verdict(&[judged]),
            StepVerdict::GeneratorBound(_)
        ));
    }

    #[test]
    fn an_engine_failure_under_a_lagging_generator_is_still_an_engine_failure() {
        // A saturated database slows the start calls too. The engine fell
        // behind a load lower than the offer, so the step is not sustainable.
        let mut e = evidence(50.0, overloaded(50.0, 40.0, CAPACITY_STEP_SECS));
        e.achieved_per_sec = 45.0;
        let judged = judge_shard(&e);
        assert!(!judged.generator.is_empty(), "{judged:?}");
        let StepVerdict::Unsustainable(reasons) = step_verdict(&[judged]) else {
            panic!("an engine failure wins over a generator lag");
        };
        assert!(reasons.iter().any(|r| r.contains("backlog")), "{reasons:?}");
        assert!(
            reasons.iter().any(|r| r.contains("generator")),
            "{reasons:?}"
        );
    }

    #[test]
    fn a_queue_in_the_generator_is_not_engine_backlog() {
        // The generator admits only 90% of the offer. The engine completes
        // every admitted run in 0.2 s, so the engine kept up.
        let mut e = evidence(50.0, lagging_generator(50.0, 0.9, CAPACITY_STEP_SECS, 0.2));
        e.achieved_per_sec = 45.0;
        let judged = judge_shard(&e);
        assert_eq!(judged.engine, Vec::<String>::new());
        assert!(
            judged.generator.iter().any(|r| r.contains("wait")),
            "{judged:?}"
        );
        assert!(matches!(
            step_verdict(&[judged]),
            StepVerdict::GeneratorBound(_)
        ));
    }

    #[test]
    fn a_five_percent_overload_fails_on_the_median_growth() {
        // A 5% overload stays just under the backlog-slope limit.
        let judged = judge_shard(&evidence(50.0, overloaded(50.0, 47.7, CAPACITY_STEP_SECS)));
        let growth = judged.median_growth.expect("a median growth");
        assert!(growth > CAPACITY_MAX_LATENCY_GROWTH_MS_PER_SEC, "{growth}");
        assert!(
            judged.engine.iter().any(|r| r.contains("latency grew")),
            "{judged:?}"
        );
    }

    #[test]
    fn a_ten_percent_overload_at_the_lowest_start_rate_fails() {
        let offered = start_rate_per_shard(CapacityArm::Canonical);
        let judged = judge_shard(&evidence(
            offered,
            overloaded(offered, offered / 1.1, CAPACITY_STEP_SECS),
        ));
        assert!(
            judged.engine.iter().any(|r| r.contains("backlog grew")),
            "{judged:?}"
        );
    }

    #[test]
    fn a_tail_spike_alone_earns_one_retry_and_an_overload_does_not() {
        let mut arrivals = steady(50.0, CAPACITY_STEP_SECS, 0.2);
        for a in arrivals.iter_mut().rev().take(15) {
            a.completed_secs = a.completed_secs.map(|c| c + 2.0);
        }
        let spike = judge_shard(&evidence(50.0, arrivals));
        assert!(spike.tail_only, "{spike:?}");
        assert!(step_retryable(&[spike]));
        let overload = judge_shard(&evidence(50.0, overloaded(50.0, 40.0, CAPACITY_STEP_SECS)));
        assert!(!overload.tail_only);
        assert!(!step_retryable(&[overload]));
        let sound = judge_shard(&evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2)));
        assert!(
            !step_retryable(&[sound]),
            "a sustainable step needs no retry"
        );
    }

    #[test]
    fn a_warmup_that_did_not_drain_voids_the_cell() {
        let mut arrivals = steady(50.0, CAPACITY_STEP_SECS, 0.2);
        arrivals[3].completed_secs = None;
        let judged = judge_shard(&evidence(50.0, arrivals));
        let reasons = warmup_drain_reasons(&[judged]);
        assert!(reasons.iter().any(|r| r.contains("warmup")), "{reasons:?}");
        let clean = judge_shard(&evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2)));
        assert_eq!(warmup_drain_reasons(&[clean]), Vec::<String>::new());
    }

    #[test]
    fn the_connection_need_counts_every_process_pool_and_the_generator() {
        let need = connection_need_per_shard(4);
        assert_eq!(
            need,
            4 * (POOL_SIZE_PER_SHARD + 1) + CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD + 4
        );
        assert_eq!(
            connection_budget_reasons(need, 200, 3),
            Vec::<String>::new()
        );
        let reasons = connection_budget_reasons(2 * need, 200, 3);
        assert!(
            reasons.iter().any(|r| r.contains("max_connections")),
            "{reasons:?}"
        );
    }

    #[test]
    fn a_thin_step_is_generator_bound() {
        let e = evidence(10.0, steady(10.0, CAPACITY_STEP_SECS, 0.2));
        let judged = judge_shard(&e);
        assert!(
            judged.generator.iter().any(|r| r.contains("runs")),
            "{judged:?}"
        );
    }

    #[test]
    fn the_weakest_shard_decides_a_step() {
        let good = judge_shard(&evidence(50.0, steady(50.0, CAPACITY_STEP_SECS, 0.2)));
        let bad = judge_shard(&evidence(50.0, overloaded(50.0, 40.0, CAPACITY_STEP_SECS)));
        let StepVerdict::Unsustainable(reasons) = step_verdict(&[good, bad]) else {
            panic!("one overloaded shard fails the step");
        };
        assert!(
            reasons.iter().all(|r| r.starts_with("shard 1:")),
            "{reasons:?}"
        );
    }

    #[test]
    fn the_sustainable_rate_stops_at_the_first_growing_backlog() {
        // Each verdict comes from the real rule. A server that completes 45
        // runs/s keeps up with 20 and 25, falls behind 50, and keeps up again
        // with a lighter step after the knee.
        let judged = |offered: f64| {
            let arrivals = if offered < 45.0 {
                steady(offered, CAPACITY_STEP_SECS, 0.2)
            } else {
                overloaded(offered, 45.0, CAPACITY_STEP_SECS)
            };
            record(
                offered,
                step_verdict(&[judge_shard(&evidence(offered, arrivals))]),
            )
        };
        let steps = [judged(20.0), judged(25.0), judged(50.0), judged(30.0)];
        assert!(matches!(steps[2].verdict, StepVerdict::Unsustainable(_)));
        let result = sustainable_rate(&steps, CAPACITY_MAX_STEPS);
        assert_eq!(result.sustainable_per_shard, Some(25.0));
        assert_eq!(result.stop, LadderStop::EngineBound);
        assert_eq!(ladder_soundness(&result), Vec::<String>::new());
    }

    #[test]
    fn a_first_step_that_fails_has_no_sustainable_rate() {
        let result = sustainable_rate(&[record(10.0, engine_fail())], CAPACITY_MAX_STEPS);
        assert_eq!(result.sustainable_per_shard, None);
        assert_eq!(result.stop, LadderStop::EngineBound);
        assert_ne!(ladder_soundness(&result), Vec::<String>::new());
    }

    #[test]
    fn a_ladder_that_never_fails_reports_only_a_lower_bound() {
        let steps = [
            record(10.0, StepVerdict::Sustainable),
            record(12.5, StepVerdict::Sustainable),
        ];
        let result = sustainable_rate(&steps, 2);
        assert_eq!(result.sustainable_per_shard, Some(12.5));
        assert_eq!(result.stop, LadderStop::LadderExhausted);
        let reasons = ladder_soundness(&result);
        assert!(
            reasons.iter().any(|r| r.contains("lower bound")),
            "{reasons:?}"
        );
    }

    #[test]
    fn a_ladder_cut_short_is_unsound() {
        let result = sustainable_rate(&[record(10.0, StepVerdict::Sustainable)], 5);
        assert_eq!(result.stop, LadderStop::CutShort);
        assert_ne!(ladder_soundness(&result), Vec::<String>::new());
    }

    #[test]
    fn a_generator_bound_ladder_is_unsound() {
        let steps = [
            record(10.0, StepVerdict::Sustainable),
            record(12.5, StepVerdict::GeneratorBound(vec!["lag".to_owned()])),
        ];
        let result = sustainable_rate(&steps, CAPACITY_MAX_STEPS);
        assert_eq!(result.sustainable_per_shard, Some(10.0));
        assert_eq!(result.stop, LadderStop::GeneratorBound);
        let reasons = ladder_soundness(&result);
        assert!(
            reasons.iter().any(|r| r.contains("generator")),
            "{reasons:?}"
        );
    }

    #[test]
    fn the_ladder_is_geometric_and_rounded() {
        assert!((ladder_rate(10.0, 0) - 10.0).abs() < 1e-9);
        assert!((ladder_rate(10.0, 1) - 12.5).abs() < 1e-9);
        assert!((ladder_rate(10.0, 2) - 15.6).abs() < 1e-9);
        assert!((ladder_rate(20.0, 3) - 39.1).abs() < 1e-9);
    }

    #[test]
    fn each_start_rate_fills_a_step_with_enough_runs() {
        for arm in [CapacityArm::Noop, CapacityArm::Canonical] {
            let runs = start_rate_per_shard(arm) * CAPACITY_STEP_SECS;
            assert!(runs >= CAPACITY_MIN_STEP_RUNS as f64, "{arm:?}: {runs}");
        }
    }

    #[test]
    fn the_backlog_counts_scheduled_runs_that_have_not_completed() {
        let arrivals = [
            Arrival {
                scheduled_secs: 0.0,
                admitted_secs: 0.0,
                completed_secs: Some(1.0),
            },
            Arrival {
                scheduled_secs: 0.5,
                admitted_secs: 0.5,
                completed_secs: None,
            },
            Arrival {
                scheduled_secs: 2.0,
                admitted_secs: 2.0,
                completed_secs: Some(2.5),
            },
        ];
        assert_eq!(backlog_at(&arrivals, 0.25), 1);
        assert_eq!(backlog_at(&arrivals, 0.75), 2);
        assert_eq!(backlog_at(&arrivals, 1.0), 1, "a completion at t is done");
        assert_eq!(backlog_at(&arrivals, 2.0), 2, "an arrival at t counts");
        assert_eq!(backlog_at(&arrivals, 3.0), 1);
        let series = backlog_series(&arrivals, 0.0, 1.0, 0.5);
        assert_eq!(series, vec![(0.0, 1.0), (0.5, 2.0), (1.0, 1.0)]);
    }

    #[test]
    fn the_slope_fits_a_line_and_refuses_a_point() {
        let line: Vec<(f64, f64)> = (0..10)
            .map(|x| (f64::from(x), 3.0_f64.mul_add(f64::from(x), 1.0)))
            .collect();
        let slope = least_squares_slope(&line).expect("a slope");
        assert!((slope - 3.0).abs() < 1e-9, "{slope}");
        assert_eq!(least_squares_slope(&[(1.0, 2.0)]), None);
        assert_eq!(least_squares_slope(&[(1.0, 2.0), (1.0, 3.0)]), None);
    }

    #[test]
    fn the_p99_ranks_an_unfinished_run_above_every_latency() {
        let mut latencies: Vec<Option<f64>> =
            (1..=100).map(|ms| Some(f64::from(ms) / 1000.0)).collect();
        let p99 = p99_ms(&latencies).expect("a p99");
        assert!((p99 - 99.0).abs() < 1e-6, "{p99}");
        latencies[0] = None;
        latencies[1] = None;
        assert_eq!(p99_ms(&latencies), Some(f64::INFINITY));
        assert_eq!(p99_ms(&[]), None);
    }

    #[test]
    fn the_windows_split_the_step_by_scheduled_arrival() {
        let arrivals = steady(10.0, 20.0, 0.1);
        let windows = window_p99_ms(&arrivals, 20.0);
        assert_eq!(windows.len(), CAPACITY_WINDOWS);
        for w in windows {
            let p99 = w.expect("every window has arrivals");
            assert!((p99 - 100.0).abs() < 1e-6, "{p99}");
        }
    }

    fn capacity_cell(shards: u32, rate: f64, hosts: f64) -> ScenarioReport {
        let mut cell = capacity_cell_on(shards, rate, hosts);
        cell.metrics
            .push(Metric::new(WORKER_HOSTS_METRIC, Some(f64::from(shards))));
        cell
    }

    fn capacity_cell_on(shards: u32, rate: f64, hosts: f64) -> ScenarioReport {
        ScenarioReport {
            scenario: BenchScenario::Capacity(CapacityArm::Noop),
            shards,
            metrics: vec![
                Metric::new(SUSTAINABLE_METRIC, Some(rate)),
                Metric::new(DATABASE_HOSTS_METRIC, Some(hosts)),
            ],
            notes: Vec::new(),
            unsound: Vec::new(),
        }
    }

    #[test]
    fn a_co_located_cell_never_enters_a_scaling_claim() {
        let lines = scaling_lines(&[capacity_cell(1, 100.0, 1.0), capacity_cell(2, 150.0, 1.0)]);
        assert!(lines.iter().all(|l| !l.contains("x the")), "{lines:?}");
        assert!(
            lines.iter().any(|l| l.contains("share a host")),
            "{lines:?}"
        );
    }

    #[test]
    fn cells_on_their_own_hosts_give_a_ratio() {
        let lines = scaling_lines(&[capacity_cell(1, 100.0, 1.0), capacity_cell(2, 190.0, 2.0)]);
        assert!(lines.iter().any(|l| l.contains("1.90x")), "{lines:?}");
    }

    #[test]
    fn a_cell_whose_workers_share_the_bench_host_gives_no_ratio() {
        let mut shared = capacity_cell_on(2, 190.0, 2.0);
        shared
            .metrics
            .push(Metric::new(WORKER_HOSTS_METRIC, Some(1.0)));
        let lines = scaling_lines(&[capacity_cell(1, 100.0, 1.0), shared]);
        assert!(lines.iter().all(|l| !l.contains("x the")), "{lines:?}");
        assert!(lines.iter().any(|l| l.contains("workers")), "{lines:?}");
    }

    #[test]
    fn an_unsound_cell_never_enters_a_scaling_claim() {
        let mut bad = capacity_cell(2, 190.0, 2.0);
        bad.unsound.push("cut short".to_owned());
        let lines = scaling_lines(&[capacity_cell(1, 100.0, 1.0), bad]);
        assert!(lines.iter().all(|l| !l.contains("1.90x")), "{lines:?}");
    }

    #[test]
    fn the_capacity_alias_selects_both_arms_and_the_default_matrix_does_not() {
        use super::super::e2e_bench_support::{selected_scenarios, unknown_scenario_ids};
        assert_eq!(
            selected_scenarios(Some("capacity")),
            BenchScenario::capacity().to_vec()
        );
        assert_eq!(
            selected_scenarios(Some("capacity_noop")),
            vec![BenchScenario::Capacity(CapacityArm::Noop)]
        );
        assert!(
            selected_scenarios(None)
                .iter()
                .all(|s| !matches!(s, BenchScenario::Capacity(_)))
        );
        assert_eq!(
            unknown_scenario_ids(Some("capacity,capacity_canonical")),
            Vec::<String>::new()
        );
        assert_eq!(unknown_scenario_ids(Some("capacity_x")), vec!["capacity_x"]);
    }

    #[test]
    fn a_child_spec_round_trips_and_refuses_junk() {
        let raw = encode_child_spec(3, "cap-s3-p1");
        assert_eq!(parse_child_spec(&raw), Some((3, "cap-s3-p1".to_owned())));
        assert_eq!(parse_child_spec("x|id"), None);
        assert_eq!(parse_child_spec("3|"), None);
        assert_eq!(parse_child_spec("3"), None);
    }

    #[test]
    fn child_urls_round_trip_with_an_equals_sign_in_a_url() {
        let urls = [
            (0, "postgres://u:p@h/a?options=-c%20x=1"),
            (1, "postgres://h/b"),
        ];
        let raw = encode_child_urls(urls);
        assert_eq!(
            parse_child_urls(&raw),
            Some(urls.iter().map(|(s, u)| (*s, (*u).to_owned())).collect())
        );
        assert_eq!(parse_child_urls(""), None);
        assert_eq!(parse_child_urls("zero=postgres://h/a"), None);
    }
}

// ---------------------------------------------------------------------------
// DB-backed section. It needs the `db` feature and a reachable Postgres.
// Only `benches/e2e_bench.rs` drives it. No CI suite runs it.
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
pub mod db {
    use std::collections::{BTreeMap, HashMap};
    use std::process::Stdio;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use chrono::{DateTime, Utc};
    use diesel::QueryableByName;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    use tokio::process::{Child, ChildStdin, ChildStdout, Command};

    use autumn_harvest::lifecycle::WorkflowState;
    use autumn_harvest::start_or_load_workflow_execution;
    use autumn_harvest::types::{ExecutionId, ShardId};

    use super::super::e2e_bench_support::db::{
        self as e2e, ShardCluster, SkipReason, Topology, budget_note, scenario_deadline,
        start_params, with_teardown_note,
    };
    use super::super::e2e_bench_support::{
        BENCH_NOOP_WORKFLOW, BENCH_WORKFLOW, BenchScenario, COMMIT_PROBE_ENV_VAR, CapacityArm,
        CensusGuard, Metric, ScenarioReport, TaskCensus, positive_override,
    };
    use super::{
        Arrival, CAPACITY_DRAIN_GRACE_SECS, CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD,
        CAPACITY_MAX_STEPS, CAPACITY_STEP_SECS, CAPACITY_WORKER_PROCESSES_PER_SHARD, CHILD_ENV_VAR,
        CHILD_READY_LINE, CHILD_URLS_ENV_VAR, DATABASE_HOSTS_METRIC, SUSTAINABLE_METRIC,
        ShardJudgement, StepEvidence, StepRecord, StepVerdict, WORKER_HOSTS_METRIC,
        WORKER_PROCESSES_ENV_VAR, connection_budget_reasons, connection_need_per_shard,
        encode_child_spec, encode_child_urls, judge_shard, ladder_rate, ladder_soundness, p99_ms,
        parse_child_spec, parse_child_urls, start_rate_per_shard, step_retryable, step_verdict,
        sustainable_rate, warmup_drain_reasons,
    };

    /// Time a worker process gets to print [`CHILD_READY_LINE`].
    const READY_TIMEOUT: Duration = Duration::from_secs(60);
    /// Time a worker process gets to stop after its stdin closes.
    const STOP_TIMEOUT: Duration = Duration::from_secs(15);
    /// Lead time between filling the generator pool and the first arrival.
    const STEP_LEAD: Duration = Duration::from_millis(200);
    /// Interval of the drain poll.
    const DRAIN_POLL: Duration = Duration::from_millis(250);
    /// Extra time a step needs beyond its arrivals and its drain grace.
    const STEP_OVERHEAD_SECS: f64 = 10.0;
    /// Exit code of a worker process whose worker stopped before stdin closed.
    const WORKER_LOST_EXIT: i32 = 3;

    // ── Worker processes ──────────────────────────────────────────────────

    /// One worker process of a capacity cell.
    pub struct ChildSpec {
        /// The shard the worker claims from.
        pub shard: i32,
        /// The worker id.
        pub worker_id: String,
        /// Every shard URL of the cell.
        pub urls: Vec<(i32, String)>,
    }

    /// The worker-process spec of this process, when the parent set one.
    #[must_use]
    pub fn child_spec_from_env() -> Option<ChildSpec> {
        let (shard, worker_id) = parse_child_spec(&std::env::var(CHILD_ENV_VAR).ok()?)?;
        let urls = parse_child_urls(&std::env::var(CHILD_URLS_ENV_VAR).ok()?)?;
        Some(ChildSpec {
            shard,
            worker_id,
            urls,
        })
    }

    /// Run one worker until stdin closes, and return the exit code.
    ///
    /// The child prints [`CHILD_READY_LINE`] only after a pool checkout
    /// succeeds. The parent closes stdin to stop the worker. A parent that
    /// dies also closes it, so a worker process never outlives its run. A
    /// worker that stops first gives a non-zero exit code, so the parent
    /// sees the loss.
    pub async fn run_worker_child(spec: ChildSpec) -> i32 {
        let urls: BTreeMap<ShardId, String> = spec
            .urls
            .into_iter()
            .map(|(shard, url)| (ShardId::new(shard), url))
            .collect();
        let notification_urls: Vec<(ShardId, String)> =
            urls.iter().map(|(s, u)| (*s, u.clone())).collect();
        let sharded = e2e::sharded_pool_from(&urls);
        let (registry, _observations) = e2e::build_registry();
        let shard = ShardId::new(spec.shard);
        let worker = e2e::build_worker(
            shard,
            &notification_urls,
            &sharded,
            &registry,
            spec.worker_id,
        );
        let Some(pool) = sharded.exact_pool_for(shard).cloned() else {
            eprintln!("worker process: shard {} has no pool", spec.shard);
            return WORKER_LOST_EXIT;
        };
        if let Err(e) = pool.get().await {
            eprintln!("worker process: the first pool checkout failed: {e}");
            return WORKER_LOST_EXIT;
        }
        let runner = Arc::clone(&worker);
        let mut handle = tokio::spawn(async move { runner.run(&pool).await });
        println!("{CHILD_READY_LINE}");

        let stdin_closed = async {
            let mut stdin = tokio::io::stdin();
            let mut buf = [0_u8; 64];
            while let Ok(n) = stdin.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        };
        tokio::select! {
            () = stdin_closed => {
                worker.shutdown();
                let _ = tokio::time::timeout(STOP_TIMEOUT, handle).await;
                0
            }
            joined = &mut handle => {
                eprintln!("worker process: the worker stopped before stdin closed ({joined:?})");
                WORKER_LOST_EXIT
            }
        }
    }

    struct WorkerProcess {
        label: String,
        child: Child,
        stdin: Option<ChildStdin>,
        guard: Option<CensusGuard>,
    }

    /// The worker processes of one capacity cell.
    ///
    /// Each child holds a census guard. When the fleet drops without
    /// [`ProcessFleet::stop`], a task kills each child and holds its guard
    /// until the child exits. So a forced abort waits for the child's
    /// connections to close.
    pub struct ProcessFleet {
        children: Vec<WorkerProcess>,
    }

    impl ProcessFleet {
        /// Worker processes in the fleet.
        #[must_use]
        pub const fn len(&self) -> usize {
            self.children.len()
        }

        /// Whether the fleet has no worker process.
        #[must_use]
        pub const fn is_empty(&self) -> bool {
            self.children.is_empty()
        }

        /// A reason for each worker process that has already exited.
        pub fn exited(&mut self) -> Vec<String> {
            let mut reasons = Vec::new();
            for process in &mut self.children {
                match process.child.try_wait() {
                    Ok(Some(status)) => reasons.push(format!(
                        "worker process {} exited during the ladder ({status})",
                        process.label
                    )),
                    Ok(None) => {}
                    Err(e) => reasons.push(format!(
                        "worker process {} could not be polled ({e})",
                        process.label
                    )),
                }
            }
            reasons
        }

        /// Close each child's stdin, then wait for it. Kill a child that
        /// does not stop in time.
        pub async fn stop(mut self) {
            for process in &mut self.children {
                drop(process.stdin.take());
            }
            for mut process in std::mem::take(&mut self.children) {
                if tokio::time::timeout(STOP_TIMEOUT, process.child.wait())
                    .await
                    .is_err()
                {
                    let _ = process.child.kill().await;
                }
                drop(process.guard.take());
            }
        }
    }

    impl Drop for ProcessFleet {
        fn drop(&mut self) {
            let runtime = tokio::runtime::Handle::try_current().ok();
            for mut process in std::mem::take(&mut self.children) {
                let _ = process.child.start_kill();
                let guard = process.guard.take();
                if let Some(runtime) = &runtime {
                    runtime.spawn(async move {
                        let _guard = guard;
                        let _ = process.child.wait().await;
                    });
                }
            }
        }
    }

    /// Start `per_shard` worker processes on each shard and wait until each
    /// one is ready.
    ///
    /// # Errors
    ///
    /// Returns a reason when a child does not start or does not report
    /// ready in time. The partial fleet is dropped, which kills it.
    pub async fn start_process_fleet(
        cluster: &ShardCluster,
        per_shard: usize,
        census: &TaskCensus,
    ) -> Result<ProcessFleet, String> {
        let exe = std::env::current_exe().map_err(|e| format!("locate the bench binary: {e}"))?;
        let urls = encode_child_urls(
            cluster
                .urls
                .iter()
                .map(|(shard, url)| (shard.as_i32(), url.as_str())),
        );
        let mut fleet = ProcessFleet {
            children: Vec::new(),
        };
        let mut stdouts: Vec<(String, BufReader<ChildStdout>)> = Vec::new();
        for shard in cluster.shard_ids() {
            for index in 0..per_shard {
                let label = format!("cap-s{}-p{index}", shard.as_i32());
                // A child must not record commit-probe samples that no one
                // reads.
                let mut child = Command::new(&exe)
                    .env(CHILD_ENV_VAR, encode_child_spec(shard.as_i32(), &label))
                    .env(CHILD_URLS_ENV_VAR, &urls)
                    .env_remove(COMMIT_PROBE_ENV_VAR)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .kill_on_drop(true)
                    .spawn()
                    .map_err(|e| format!("spawn worker process {label}: {e}"))?;
                let stdin = child.stdin.take();
                let stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| format!("worker process {label} has no stdout"))?;
                stdouts.push((label.clone(), BufReader::new(stdout)));
                fleet.children.push(WorkerProcess {
                    label,
                    child,
                    stdin,
                    guard: Some(census.enter()),
                });
            }
        }
        for (label, mut stdout) in stdouts {
            let mut line = String::new();
            let read = tokio::time::timeout(READY_TIMEOUT, stdout.read_line(&mut line))
                .await
                .map_err(|_| format!("worker process {label} was not ready in time"))?;
            match read {
                Ok(_) if line.trim() == CHILD_READY_LINE => {}
                Ok(_) => {
                    return Err(format!(
                        "worker process {label} printed {line:?} instead of {CHILD_READY_LINE:?}"
                    ));
                }
                Err(e) => return Err(format!("read worker process {label}: {e}")),
            }
            // Read the rest of stdout, so a later write never blocks the child.
            tokio::spawn(async move {
                let mut sink = tokio::io::sink();
                let _ = tokio::io::copy(&mut stdout, &mut sink).await;
            });
        }
        Ok(fleet)
    }

    // ── Connection budget ─────────────────────────────────────────────────

    #[derive(QueryableByName)]
    struct SettingRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        setting: String,
    }

    async fn integer_setting(conn: &mut AsyncPgConnection, name: &str) -> Option<usize> {
        let row: SettingRow = diesel::sql_query("SELECT setting FROM pg_settings WHERE name = $1")
            .bind::<diesel::sql_types::Text, _>(name)
            .get_result(conn)
            .await
            .ok()?;
        row.setting.parse().ok()
    }

    /// Reasons the cell's servers cannot hold its connections.
    async fn connection_reasons(cluster: &ShardCluster, processes: usize) -> Vec<String> {
        let per_shard = connection_need_per_shard(processes);
        // On a shared server every shard of the cell lands on one server.
        let sharing = match cluster.topology {
            Topology::SharedServer => cluster.shard_ids().len(),
            Topology::IndependentServers => 1,
        };
        let need = per_shard * sharing;
        let mut reasons = Vec::new();
        for shard in cluster.shard_ids() {
            let mut conn = cluster.connect(shard).await;
            let max = integer_setting(&mut conn, "max_connections").await;
            let reserved = integer_setting(&mut conn, "superuser_reserved_connections")
                .await
                .unwrap_or(3);
            match max {
                Some(max) => reasons.extend(
                    connection_budget_reasons(need, max, reserved)
                        .into_iter()
                        .map(|r| format!("shard {}: {r}", shard.as_i32())),
                ),
                None => reasons.push(format!(
                    "shard {}: max_connections could not be read",
                    shard.as_i32()
                )),
            }
            if sharing > 1 {
                break;
            }
        }
        reasons
    }

    // ── One step ──────────────────────────────────────────────────────────

    /// One run the generator started.
    struct Sent {
        workflow_id: String,
        scheduled_secs: f64,
        admitted_secs: f64,
        done: Instant,
    }

    /// What the generator did on one shard during one step.
    struct ShardOffer {
        /// Workflow id to `(scheduled, admitted)`, in seconds after the step
        /// began.
        sent: HashMap<String, (f64, f64)>,
        achieved_per_sec: f64,
    }

    /// What one step measured.
    struct StepOutcome {
        judgements: Vec<ShardJudgement>,
        achieved: Vec<f64>,
        runs: Vec<usize>,
        /// p99 latency from admission over every run, pooled across shards.
        pooled_p99_ms: Option<f64>,
    }

    #[derive(QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    #[derive(QueryableByName)]
    struct RunRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        completed_at: Option<DateTime<Utc>>,
    }

    /// Whether a stored state is terminal. An unknown state is not.
    fn is_terminal(state: &str) -> bool {
        WorkflowState::ALL
            .iter()
            .any(|s| s.as_str() == state && s.is_terminal())
    }

    /// The SQL list of every terminal state.
    fn terminal_state_list() -> String {
        WorkflowState::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Runs of `cohort` in a terminal state.
    async fn terminal_count(conn: &mut AsyncPgConnection, cohort: &str) -> usize {
        let row: CountRow = diesel::sql_query(format!(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions \
             WHERE workflow_id LIKE $1 AND state IN ({})",
            terminal_state_list()
        ))
        .bind::<diesel::sql_types::Text, _>(format!("{cohort}-%"))
        .get_result(conn)
        .await
        .expect("count terminal capacity runs");
        usize::try_from(row.n).unwrap_or(0)
    }

    async fn load_runs(conn: &mut AsyncPgConnection, cohort: &str) -> Vec<RunRow> {
        diesel::sql_query(
            "SELECT workflow_id, state, completed_at FROM harvest_workflow_executions \
             WHERE workflow_id LIKE $1",
        )
        .bind::<diesel::sql_types::Text, _>(format!("{cohort}-%"))
        .load(conn)
        .await
        .expect("load capacity runs")
    }

    /// Offer `rate` starts per second on each shard for one step.
    ///
    /// Each arrival is its own task. It waits for its scheduled instant,
    /// then for a connection from the generator pool. The instant it gets
    /// one is its admission. So a slow start call never delays the schedule
    /// of a later arrival.
    async fn offer_step(
        cluster: &ShardCluster,
        workflow: &'static str,
        cohort: &str,
        rate: f64,
        deadline: Instant,
        census: &TaskCensus,
    ) -> (DateTime<Utc>, Vec<ShardOffer>) {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "rate and step length are small positive constants"
        )]
        let total = (rate * CAPACITY_STEP_SECS).round() as usize;
        let period = 1.0 / rate;
        let shards = cluster.shard_ids();

        let mut pools = Vec::new();
        for shard in &shards {
            let pool = e2e::build_pool_sized(
                &cluster.urls[shard],
                CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD,
            );
            // Fill the pool before the step, so no arrival pays a connect.
            let mut held = Vec::new();
            for _ in 0..CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD {
                held.push(pool.get().await.expect("fill the generator pool"));
            }
            drop(held);
            pools.push(pool);
        }

        // One instant on both clocks. Arrivals are scheduled on the
        // monotonic clock and stamped on the host wall clock.
        let t0 = Instant::now() + STEP_LEAD;
        let t0_utc = Utc::now()
            + chrono::Duration::from_std(STEP_LEAD).unwrap_or_else(|_| chrono::Duration::zero());

        // A `JoinSet` aborts every arrival if one panics. Each arrival holds
        // a census guard, so a forced abort can wait for its connection.
        let mut tasks = tokio::task::JoinSet::new();
        for (idx, shard) in shards.iter().enumerate() {
            for k in 0..total {
                #[allow(clippy::cast_precision_loss, reason = "arrival indexes are small")]
                let at_secs = k as f64 * period;
                let at = t0 + Duration::from_secs_f64(at_secs);
                if at >= deadline {
                    break;
                }
                let pool = pools[idx].clone();
                let shard = *shard;
                let workflow_id = format!("{cohort}-s{}-{k}", shard.as_i32());
                let guard = census.enter();
                tasks.spawn(async move {
                    let _guard = guard;
                    tokio::time::sleep_until(at.into()).await;
                    let mut conn = pool.get().await.expect("a generator connection");
                    let admitted = Instant::now();
                    start_or_load_workflow_execution(
                        &mut conn,
                        start_params(ExecutionId::new_for_shard(shard), workflow, &workflow_id),
                        None,
                    )
                    .await
                    .expect("capacity start");
                    let sent = Sent {
                        workflow_id,
                        scheduled_secs: at_secs,
                        admitted_secs: admitted.saturating_duration_since(t0).as_secs_f64(),
                        done: Instant::now(),
                    };
                    (idx, sent)
                });
            }
        }
        let mut per_shard: Vec<Vec<Sent>> = shards.iter().map(|_| Vec::new()).collect();
        while let Some(joined) = tasks.join_next().await {
            let (idx, sent) = joined.expect("capacity arrival");
            per_shard[idx].push(sent);
        }

        let offers = per_shard
            .into_iter()
            .map(|sent| {
                let finished = sent.iter().map(|s| s.done).max().unwrap_or(t0);
                let elapsed = finished
                    .saturating_duration_since(t0)
                    .as_secs_f64()
                    .max(CAPACITY_STEP_SECS);
                #[allow(clippy::cast_precision_loss, reason = "run counts are far below 2^53")]
                let achieved_per_sec = sent.len() as f64 / elapsed;
                ShardOffer {
                    sent: sent
                        .into_iter()
                        .map(|s| (s.workflow_id, (s.scheduled_secs, s.admitted_secs)))
                        .collect(),
                    achieved_per_sec,
                }
            })
            .collect();
        (t0_utc, offers)
    }

    /// Wait until every run of `cohort` is terminal on each shard, or the
    /// drain grace passes.
    async fn drain(cluster: &ShardCluster, cohort: &str, expected: &[usize]) {
        let until = Instant::now() + Duration::from_secs(CAPACITY_DRAIN_GRACE_SECS);
        let mut conns = Vec::new();
        for shard in cluster.shard_ids() {
            conns.push(cluster.connect(shard).await);
        }
        loop {
            let mut done = true;
            for (conn, want) in conns.iter_mut().zip(expected) {
                if terminal_count(conn, cohort).await < *want {
                    done = false;
                    break;
                }
            }
            if done || Instant::now() >= until {
                return;
            }
            tokio::time::sleep(DRAIN_POLL).await;
        }
    }

    /// Offer one step, drain it, and judge each shard.
    async fn run_step(
        cluster: &ShardCluster,
        workflow: &'static str,
        cohort: &str,
        rate: f64,
        deadline: Instant,
        census: &TaskCensus,
    ) -> StepOutcome {
        let (t0_utc, offers) = offer_step(cluster, workflow, cohort, rate, deadline, census).await;
        let expected: Vec<usize> = offers.iter().map(|o| o.sent.len()).collect();
        drain(cluster, cohort, &expected).await;

        let mut judgements = Vec::new();
        let mut pooled: Vec<Option<f64>> = Vec::new();
        for (shard, offer) in cluster.shard_ids().into_iter().zip(&offers) {
            let mut conn = cluster.connect(shard).await;
            let rows: HashMap<String, RunRow> = load_runs(&mut conn, cohort)
                .await
                .into_iter()
                .map(|r| (r.workflow_id.clone(), r))
                .collect();
            let mut failed = 0;
            let arrivals: Vec<Arrival> = offer
                .sent
                .iter()
                .map(|(workflow_id, (scheduled_secs, admitted_secs))| {
                    let completed_secs = rows.get(workflow_id).and_then(|row| {
                        let terminal = is_terminal(&row.state);
                        if terminal && row.state != WorkflowState::Completed.as_str() {
                            failed += 1;
                        }
                        let at = row.completed_at.filter(|_| terminal)?;
                        #[allow(
                            clippy::cast_precision_loss,
                            reason = "a step lasts seconds; microsecond counts stay far below 2^53"
                        )]
                        let secs =
                            (at - t0_utc).num_microseconds().unwrap_or(i64::MAX) as f64 / 1e6;
                        Some(secs)
                    });
                    Arrival {
                        scheduled_secs: *scheduled_secs,
                        admitted_secs: *admitted_secs,
                        completed_secs,
                    }
                })
                .collect();
            pooled.extend(
                arrivals
                    .iter()
                    .map(|a| a.completed_secs.map(|done| done - a.admitted_secs)),
            );
            judgements.push(judge_shard(&StepEvidence {
                offered_per_sec: rate,
                achieved_per_sec: offer.achieved_per_sec,
                step_secs: CAPACITY_STEP_SECS,
                arrivals,
                failed,
            }));
        }
        StepOutcome {
            judgements,
            achieved: offers.iter().map(|o| o.achieved_per_sec).collect(),
            runs: expected,
            pooled_p99_ms: p99_ms(&pooled),
        }
    }

    fn join_values(values: impl IntoIterator<Item = String>) -> String {
        values.into_iter().collect::<Vec<_>>().join(", ")
    }

    fn ms(value: Option<f64>) -> String {
        value.map_or_else(|| "n/a".to_owned(), |v| format!("{v:.1}"))
    }

    /// One line per step for the cell's notes.
    fn step_note(label: &str, rate: f64, outcome: &StepOutcome, verdict: &StepVerdict) -> String {
        let per_shard = |pick: fn(&ShardJudgement) -> Option<f64>| {
            join_values(outcome.judgements.iter().map(|j| ms(pick(j))))
        };
        let windows = join_values(outcome.judgements.iter().map(|j| {
            j.window_p99_ms
                .iter()
                .skip(1)
                .map(|w| ms(*w))
                .collect::<Vec<_>>()
                .join("/")
        }));
        let verdict = match verdict {
            StepVerdict::Sustainable => "sustainable".to_owned(),
            StepVerdict::Unsustainable(r) => format!("**not sustainable**: {}", r.join("; ")),
            StepVerdict::GeneratorBound(r) => format!("**generator-bound**: {}", r.join("; ")),
        };
        format!(
            "{label}: offered {rate:.1}/s per shard; achieved {}; runs {}; backlog slope {} \
             runs/s; median growth {} ms/s; judged-window p99 {} ms; step p99 {} ms; generator \
             wait p99 {} ms; {verdict}",
            join_values(outcome.achieved.iter().map(|a| format!("{a:.1}"))),
            join_values(outcome.runs.iter().map(ToString::to_string)),
            per_shard(|j| j.backlog_slope),
            per_shard(|j| j.median_growth),
            windows,
            ms(outcome.pooled_p99_ms),
            per_shard(|j| j.generator_wait_p99_ms),
        )
    }

    /// Worker processes per shard for this run.
    #[must_use]
    pub fn worker_processes_per_shard() -> usize {
        positive_override(
            std::env::var(WORKER_PROCESSES_ENV_VAR).ok().as_deref(),
            CAPACITY_WORKER_PROCESSES_PER_SHARD,
        )
    }

    /// What a ladder found.
    struct Ladder {
        steps: Vec<StepRecord>,
        sustainable_p99: Option<f64>,
        notes: Vec<String>,
        unsound: Vec<String>,
    }

    /// Warm up, then climb the ladder until a step is not sustainable.
    async fn climb(
        cluster: &ShardCluster,
        workflow: &'static str,
        start: f64,
        deadline: Instant,
        census: &TaskCensus,
    ) -> Ladder {
        let mut ladder = Ladder {
            steps: Vec::new(),
            sustainable_p99: None,
            notes: Vec::new(),
            unsound: Vec::new(),
        };
        let warm = run_step(cluster, workflow, "capwm", start, deadline, census).await;
        let warm_verdict = step_verdict(&warm.judgements);
        ladder
            .notes
            .push(step_note("warmup (discarded)", start, &warm, &warm_verdict));
        ladder
            .unsound
            .extend(warmup_drain_reasons(&warm.judgements));

        let needed = Duration::from_secs_f64(
            CAPACITY_STEP_SECS
                + Duration::from_secs(CAPACITY_DRAIN_GRACE_SECS).as_secs_f64()
                + STEP_OVERHEAD_SECS,
        );
        for k in 0..CAPACITY_MAX_STEPS {
            if Instant::now() + needed >= deadline {
                break;
            }
            let rate = ladder_rate(start, k);
            let label = format!("step {k}");
            let mut outcome = run_step(
                cluster,
                workflow,
                &format!("cap{k:02}"),
                rate,
                deadline,
                census,
            )
            .await;
            let mut verdict = step_verdict(&outcome.judgements);
            if step_retryable(&outcome.judgements) && Instant::now() + needed < deadline {
                ladder.notes.push(step_note(
                    &format!("{label} (tail only, retried)"),
                    rate,
                    &outcome,
                    &verdict,
                ));
                outcome = run_step(
                    cluster,
                    workflow,
                    &format!("cap{k:02}r"),
                    rate,
                    deadline,
                    census,
                )
                .await;
                verdict = step_verdict(&outcome.judgements);
            }
            ladder
                .notes
                .push(step_note(&label, rate, &outcome, &verdict));
            let stop = !matches!(verdict, StepVerdict::Sustainable);
            if !stop {
                ladder.sustainable_p99 = outcome.pooled_p99_ms;
            }
            ladder.steps.push(StepRecord {
                offered_per_shard: rate,
                verdict,
            });
            if stop {
                break;
            }
        }
        ladder
    }

    // ── The scenario ──────────────────────────────────────────────────────

    /// **Capacity.** The highest offered start rate each shard sustains.
    ///
    /// The ladder offers `start x 1.25^k` starts per second per shard. Each
    /// step drains before the next one starts. The ladder stops at the first
    /// step that is not sustainable. `docs/design/DESIGN-2419.md` states the
    /// rule.
    ///
    /// # Errors
    ///
    /// Returns [`SkipReason`] when no Postgres could be provisioned.
    pub async fn run_capacity(
        arm: CapacityArm,
        shard_count: u32,
        census: TaskCensus,
    ) -> Result<ScenarioReport, SkipReason> {
        let scenario = BenchScenario::Capacity(arm);
        let scenario_started = Instant::now();
        let cluster = Box::pin(e2e::setup_shards(shard_count)).await?;
        let layout = cluster.host_layout();
        let processes = worker_processes_per_shard();
        let workflow = match arm {
            CapacityArm::Noop => BENCH_NOOP_WORKFLOW,
            CapacityArm::Canonical => BENCH_WORKFLOW,
        };
        let start = start_rate_per_shard(arm);

        let mut notes = vec![
            format!("topology: {}", cluster.topology.as_str()),
            layout.note(),
            "worker layout: every worker process runs on the bench host".to_owned(),
            format!(
                "open loop: {processes} worker process(es) per shard, a \
                 {CAPACITY_GENERATOR_CONNECTIONS_PER_SHARD}-connection generator pool per \
                 shard, {CAPACITY_STEP_SECS:.0}s steps from {start:.1} starts/s per shard"
            ),
        ];
        let deadline = scenario_deadline();
        let mut unsound = connection_reasons(&cluster, processes).await;
        let mut ladder = Ladder {
            steps: Vec::new(),
            sustainable_p99: None,
            notes: Vec::new(),
            unsound: Vec::new(),
        };
        if unsound.is_empty() {
            match start_process_fleet(&cluster, processes, &census).await {
                Ok(mut fleet) => {
                    ladder = climb(&cluster, workflow, start, deadline, &census).await;
                    unsound.extend(fleet.exited());
                    fleet.stop().await;
                }
                Err(reason) => unsound.push(format!("the worker fleet did not start: {reason}")),
            }
        }
        notes.append(&mut ladder.notes);
        unsound.append(&mut ladder.unsound);

        let result = sustainable_rate(&ladder.steps, CAPACITY_MAX_STEPS);
        unsound.extend(ladder_soundness(&result));
        notes.push(format!(
            "ladder: {} step(s), stopped {:?}; sustainable {} starts/s per shard",
            ladder.steps.len(),
            result.stop,
            ms(result.sustainable_per_shard),
        ));
        notes.push(
            "engine latency runs from admission to `completed_at`, both on the bench host \
             clock; the generator wait, from schedule to admission, is judged separately"
                .to_owned(),
        );
        let teardown = cluster.teardown_with_stats(scenario).await;
        notes.push(budget_note(scenario_started, deadline, &mut unsound));

        let sound = unsound.is_empty();
        let per_shard = result.sustainable_per_shard.filter(|_| sound);
        #[allow(clippy::cast_precision_loss, reason = "host and step counts are small")]
        let metrics = vec![
            Metric::new(
                SUSTAINABLE_METRIC,
                per_shard.map(|r| r * f64::from(shard_count)),
            ),
            Metric::new("sustainable_per_shard_per_sec", per_shard),
            Metric::new(
                "p99_ms_at_sustainable",
                ladder.sustainable_p99.filter(|_| sound),
            ),
            Metric::new("steps", Some(ladder.steps.len() as f64)),
            Metric::new(DATABASE_HOSTS_METRIC, Some(layout.hosts.len() as f64)),
            // Every worker process runs on the bench host.
            Metric::new(WORKER_HOSTS_METRIC, Some(1.0)),
        ];
        Ok(ScenarioReport {
            scenario,
            shards: shard_count,
            metrics,
            notes: with_teardown_note(notes, &teardown),
            unsound,
        })
    }
}
