//! The world simulation (issue #2002).
//!
//! The oracle harness drives single store statements. This module drives
//! whole actors: workers, schedulers, the orphan reclaimer, the timeout
//! sweeper and a client that sends signals. A [`World`] applies each
//! action. The Postgres world in `tests/integration/dst_world_tests.rs`
//! runs the real `worker.rs` poll loop, so the seed drives `worker.rs`.
//!
//! A seed fixes every action, every clock advance and every fault. Each
//! action runs to completion before the next one starts. The run is thus a
//! function of the seed, and a failing seed replays exactly.
//!
//! The clock moves in ticks of [`TICK_SECS`]. A world must make every
//! deadline in its workload a whole number of ticks. A world can also move
//! its clock a little in each step, if a whole run moves it less than one
//! tick. The Postgres world moves its clock by a shift of every stored
//! instant. See `docs/testing/simulation.md`.
//!
//! A run has two phases. The fault phase can stall or crash a worker. The
//! drain phase injects no fault, and it ends when all work is complete.
//! [`WorldInvariant::Converges`] fails a run whose work does not finish.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;

use super::rng::SplitMix64;
use super::sweep::{Nondeterminism, SEED_VAR, SeedPlan, TAIL_LINES, diverged};
use crate::event::WorkflowEvent;

/// The length of one clock tick, in seconds: one day.
pub const TICK_SECS: u64 = 86_400;

/// The planted defect: `none` (the default) or `foreign-state`.
pub const PLANT_VAR: &str = "HARVEST_DST_WORLD_PLANT";

/// A comma list of world invariant names. The default is all of them.
pub const WORLD_CHECKS_VAR: &str = "HARVEST_DST_WORLD_CHECKS";

/// A defect that a world plants in its workload on purpose.
///
/// A plant proves that the harness finds a real class of bug, and that the
/// failure replays from its seed alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plant {
    /// No defect.
    None,
    /// A workflow branches on state of the worker that runs it.
    ///
    /// Resident state hides the defect while the workflow stays on one
    /// worker. A fault moves the workflow, a cold replay takes the other
    /// branch, and the engine blocks the run on non-determinism.
    ForeignState,
}

impl Plant {
    /// The name that [`PLANT_VAR`] accepts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ForeignState => "foreign-state",
        }
    }

    /// Parse a name from [`Plant::as_str`].
    ///
    /// # Errors
    ///
    /// Returns a message that names the accepted values.
    pub fn parse(name: &str) -> Result<Self, String> {
        [Self::None, Self::ForeignState]
            .into_iter()
            .find(|plant| plant.as_str() == name)
            .ok_or_else(|| format!("unknown plant {name:?}: use none or foreign-state"))
    }
}

/// A safety or liveness property of a world run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorldInvariant {
    /// An execution has at most one terminal event, and it is the last one.
    OneTerminal,
    /// An execution has a terminal status exactly when it has a terminal
    /// event.
    StatusMatchesHistory,
    /// An activity has at most one `ActivityCompleted` event.
    ActivityResultOnce,
    /// A timer fires at most once.
    TimerFiresOnce,
    /// A timer fires no earlier than its start plus its duration.
    TimerNotEarly,
    /// A schedule slot fires at most once.
    ScheduleSlotOnce,
    /// A schedule slot fires no earlier than its due time.
    ScheduleNotEarly,
    /// A fire claim that wins starts exactly one execution.
    FireStartsRun,
    /// No execution fails or blocks on non-determinism.
    Deterministic,
    /// A completed execution returns the value that its world expects.
    ExpectedOutput,
    /// The drain phase completes all work.
    Converges,
}

impl WorldInvariant {
    /// Every invariant.
    pub const ALL: [Self; 11] = [
        Self::OneTerminal,
        Self::StatusMatchesHistory,
        Self::ActivityResultOnce,
        Self::TimerFiresOnce,
        Self::TimerNotEarly,
        Self::ScheduleSlotOnce,
        Self::ScheduleNotEarly,
        Self::FireStartsRun,
        Self::Deterministic,
        Self::ExpectedOutput,
        Self::Converges,
    ];

    /// The name that [`WORLD_CHECKS_VAR`] accepts.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::OneTerminal => "OneTerminal",
            Self::StatusMatchesHistory => "StatusMatchesHistory",
            Self::ActivityResultOnce => "ActivityResultOnce",
            Self::TimerFiresOnce => "TimerFiresOnce",
            Self::TimerNotEarly => "TimerNotEarly",
            Self::ScheduleSlotOnce => "ScheduleSlotOnce",
            Self::ScheduleNotEarly => "ScheduleNotEarly",
            Self::FireStartsRun => "FireStartsRun",
            Self::Deterministic => "Deterministic",
            Self::ExpectedOutput => "ExpectedOutput",
            Self::Converges => "Converges",
        }
    }

    /// Parse a name from [`WorldInvariant::name`].
    ///
    /// # Errors
    ///
    /// Returns a message that names the accepted values.
    pub fn parse(name: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|invariant| invariant.name() == name)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.iter().map(|i| i.name()).collect();
                format!(
                    "unknown world invariant {name:?}: use one of {}",
                    names.join(", ")
                )
            })
    }
}

impl fmt::Display for WorldInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The first failed invariant of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldViolation {
    /// The invariant.
    pub invariant: WorldInvariant,
    /// The step that broke it.
    pub step: usize,
    /// What broke it.
    pub detail: String,
}

impl fmt::Display for WorldViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at step {}: {}",
            self.invariant, self.step, self.detail
        )
    }
}

/// The parameters of one world run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldConfig {
    /// The seed. Equal configs give equal runs.
    pub seed: u64,
    /// The number of worker processes.
    pub workers: usize,
    /// The number of client workflows. The schedule adds more.
    pub workflows: usize,
    /// The number of scheduler replicas. They race for each fire claim.
    pub schedulers: usize,
    /// The steps of the fault phase.
    pub fault_steps: usize,
    /// The step limit of the drain phase.
    pub drain_steps: usize,
    /// A worker whose last beat is this many ticks old is dead.
    pub stale_ticks: u64,
    /// The planted defect.
    pub plant: Plant,
    /// The invariants to check.
    pub checks: Vec<WorldInvariant>,
}

impl WorldConfig {
    /// The default config for `seed`: 3 workers, 3 workflows, 2 schedulers.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            workers: 3,
            workflows: 3,
            schedulers: 2,
            fault_steps: 120,
            drain_steps: 400,
            stale_ticks: 2,
            plant: Plant::None,
            checks: WorldInvariant::ALL.to_vec(),
        }
    }

    /// This config with `plant`.
    #[must_use]
    pub const fn with_plant(mut self, plant: Plant) -> Self {
        self.plant = plant;
        self
    }

    /// This config, checking only `checks`.
    #[must_use]
    pub fn checking(mut self, checks: &[WorldInvariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }
}

/// One action of one actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldAction {
    /// Move the clock one tick forward.
    Advance,
    /// Worker `n` refreshes its liveness row.
    Beat(usize),
    /// Worker `n` runs one poll-loop iteration to completion.
    Poll(usize),
    /// Worker `n` pauses. It keeps its memory but does no work.
    Stall(usize),
    /// Worker `n` dies. Its memory goes with it.
    Crash(usize),
    /// Worker `n` claims a task and dies before it runs the task.
    ///
    /// The claim stays on the row, so the reclaimer or the sweeper must
    /// recover it.
    Abandon(usize),
    /// Worker `n` starts again with a new id.
    Restart(usize),
    /// The client sends a signal to workflow `n`.
    Signal(usize),
    /// Scheduler `n` reads the due schedules.
    ScheduleScan(usize),
    /// Scheduler `n` claims and fires the slot that its last scan read.
    ScheduleFire(usize),
    /// The orphan reclaimer runs one pass.
    Reclaim,
    /// The timeout sweeper runs one pass.
    Sweep,
}

/// What a poll ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ran {
    /// An activity task.
    Activity,
    /// A workflow decision after a cache miss. It replayed the full history.
    Cold,
    /// A workflow decision on a cache hit that resumed the resident
    /// workflow. No replay ran.
    Warm,
    /// A workflow decision on a cache hit that dropped the resident
    /// workflow and replayed.
    Declined,
    /// A task that ran neither an activity nor a decision, such as a stale
    /// workflow task.
    Other,
}

/// The result of one action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// The action took effect.
    Done,
    /// The poll claimed no task.
    Idle,
    /// The poll ran one task.
    Polled(Ran),
    /// The scan read this many due schedules.
    Scanned(usize),
    /// The fire claim won the slot with this due tick, or lost (`None`).
    Fired(Option<u64>),
    /// The reclaimer requeued this many rows.
    Reclaimed(u64),
    /// The sweeper acted on this many rows.
    Swept(u64),
}

impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Done => f.write_str("ok"),
            Self::Idle => f.write_str("idle"),
            Self::Polled(Ran::Activity) => f.write_str("activity"),
            Self::Polled(Ran::Cold) => f.write_str("cold"),
            Self::Polled(Ran::Warm) => f.write_str("warm"),
            Self::Polled(Ran::Declined) => f.write_str("declined"),
            Self::Polled(Ran::Other) => f.write_str("other"),
            Self::Scanned(due) => write!(f, "due {due}"),
            Self::Fired(Some(slot)) => write!(f, "fired slot t={slot:03}"),
            Self::Fired(None) => f.write_str("lost"),
            Self::Reclaimed(rows) => write!(f, "requeued {rows}"),
            Self::Swept(rows) => write!(f, "swept {rows}"),
        }
    }
}

/// One history event, with no id, time or worker in it.
///
/// Activity ids become ordinals in order of first appearance, so equal runs
/// give equal facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fact {
    /// `WorkflowStarted`.
    Started,
    /// `ActivityScheduled`.
    ActivityScheduled {
        /// The activity ordinal.
        activity: usize,
        /// The activity name.
        name: String,
    },
    /// `ActivityStarted`.
    ActivityStarted {
        /// The activity ordinal.
        activity: usize,
    },
    /// `ActivityCompleted`.
    ActivityCompleted {
        /// The activity ordinal.
        activity: usize,
    },
    /// `ActivityFailed`.
    ActivityFailed {
        /// The activity ordinal.
        activity: usize,
    },
    /// `TimerStarted`.
    TimerStarted {
        /// The timer id.
        timer: String,
        /// The duration.
        secs: u64,
    },
    /// `TimerFired`.
    TimerFired {
        /// The timer id.
        timer: String,
    },
    /// `SignalReceived`.
    Signal {
        /// The signal name.
        name: String,
    },
    /// `WorkflowCompleted`.
    Completed {
        /// The output as JSON text.
        output: String,
    },
    /// `WorkflowFailed`. The error text can hold ids, so it is left out.
    Failed,
    /// `WorkflowCancelled`.
    Cancelled,
    /// Any other event, by its type name.
    Other(String),
}

impl Fact {
    /// The facts of `events`, in order.
    #[must_use]
    pub fn from_events(events: &[WorkflowEvent]) -> Vec<Self> {
        let mut ordinals = BTreeMap::new();
        let mut ordinal = |id: uuid::Uuid| {
            let next = ordinals.len();
            *ordinals.entry(id).or_insert(next)
        };
        events
            .iter()
            .map(|event| match event {
                WorkflowEvent::WorkflowStarted { .. } => Self::Started,
                WorkflowEvent::ActivityScheduled {
                    activity_id, name, ..
                } => Self::ActivityScheduled {
                    activity: ordinal(activity_id.as_uuid()),
                    name: name.clone(),
                },
                WorkflowEvent::ActivityStarted { activity_id, .. } => Self::ActivityStarted {
                    activity: ordinal(activity_id.as_uuid()),
                },
                WorkflowEvent::ActivityCompleted { activity_id, .. } => Self::ActivityCompleted {
                    activity: ordinal(activity_id.as_uuid()),
                },
                WorkflowEvent::ActivityFailed { activity_id, .. } => Self::ActivityFailed {
                    activity: ordinal(activity_id.as_uuid()),
                },
                WorkflowEvent::TimerStarted {
                    timer_id,
                    duration_secs,
                } => Self::TimerStarted {
                    timer: timer_id.as_str().to_string(),
                    secs: *duration_secs,
                },
                WorkflowEvent::TimerFired { timer_id } => Self::TimerFired {
                    timer: timer_id.as_str().to_string(),
                },
                WorkflowEvent::SignalReceived { signal_name, .. } => Self::Signal {
                    name: signal_name.clone(),
                },
                WorkflowEvent::WorkflowCompleted { output } => Self::Completed {
                    output: output.to_string(),
                },
                WorkflowEvent::WorkflowFailed { .. } => Self::Failed,
                WorkflowEvent::WorkflowCancelled { .. } => Self::Cancelled,
                other => Self::Other(type_name(other)),
            })
            .collect()
    }

    /// Whether the fact ends the execution.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Completed { .. } | Self::Failed | Self::Cancelled => true,
            Self::Other(name) => OTHER_TERMINALS.contains(&name.as_str()),
            _ => false,
        }
    }
}

/// The other events that end an execution, by type name.
const OTHER_TERMINALS: [&str; 3] = [
    "WorkflowExecutionTimedOut",
    "WorkflowContinuedAsNew",
    "WorkflowResetTerminated",
];

/// The `state` values of an execution that has not ended.
pub const ACTIVE_STATES: [&str; 2] = ["RUNNING", "PAUSED"];

/// The serde tag of `event`, such as `DecisionCommitted`.
fn type_name(event: &WorkflowEvent) -> String {
    serde_json::to_value(event)
        .ok()
        .and_then(|value| value.get("type").and_then(|t| t.as_str()).map(String::from))
        .unwrap_or_else(|| "Unknown".to_string())
}

impl fmt::Display for Fact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Started => f.write_str("WorkflowStarted"),
            Self::ActivityScheduled { activity, name } => {
                write!(f, "ActivityScheduled a{activity} {name}")
            }
            Self::ActivityStarted { activity } => write!(f, "ActivityStarted a{activity}"),
            Self::ActivityCompleted { activity } => write!(f, "ActivityCompleted a{activity}"),
            Self::ActivityFailed { activity } => write!(f, "ActivityFailed a{activity}"),
            Self::TimerStarted { timer, secs } => write!(f, "TimerStarted {timer} {secs}s"),
            Self::TimerFired { timer } => write!(f, "TimerFired {timer}"),
            Self::Signal { name } => write!(f, "SignalReceived {name}"),
            Self::Completed { output } => write!(f, "WorkflowCompleted {output}"),
            Self::Failed => f.write_str("WorkflowFailed"),
            Self::Cancelled => f.write_str("WorkflowCancelled"),
            Self::Other(name) => f.write_str(name),
        }
    }
}

/// One execution as a world sees it after a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecFacts {
    /// A stable label, such as `c0` or `s1`. It holds no id.
    pub label: String,
    /// The `state` column, such as `RUNNING` or `COMPLETED`.
    pub status: String,
    /// Whether the engine blocked the execution on non-determinism.
    pub blocked: bool,
    /// The output that the world expects, if it knows one.
    pub expected: Option<serde_json::Value>,
    /// The history, as facts.
    pub events: Vec<Fact>,
}

/// The state of a world after a step.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Every execution, in a stable order.
    pub executions: Vec<ExecFacts>,
    /// Whether every schedule has fired all its runs.
    pub schedules_done: bool,
}

/// A system under simulation.
///
/// The driver calls one method at a time, on one thread, and awaits it to
/// completion. A world must not let real time or a random id change an
/// effect or a snapshot.
pub trait World {
    /// Apply `action` at virtual time `now_tick` and return its effect.
    ///
    /// For [`WorldAction::Advance`], `now_tick` is the new time.
    fn apply(&mut self, action: WorldAction, now_tick: u64) -> impl Future<Output = Effect>;

    /// Read the state of every execution.
    fn snapshot(&mut self) -> impl Future<Output = Snapshot>;
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorldStats {
    /// Polls that found no task.
    pub idle_polls: u64,
    /// Activity tasks.
    pub activities: u64,
    /// Workflow decisions that replayed the full history.
    pub cold: u64,
    /// Workflow decisions that resumed a resident workflow.
    pub warm: u64,
    /// Workflow decisions that dropped a resident workflow.
    pub declined: u64,
    /// Polls that ran a task with no activity and no decision.
    pub other_tasks: u64,
    /// `TimerFired` events.
    pub timers_fired: u64,
    /// Signals sent.
    pub signals: u64,
    /// Schedule scans that read a due slot.
    pub scans: u64,
    /// Fire claims that won.
    pub fires: u64,
    /// Fire claims that lost.
    pub lost_fires: u64,
    /// Rows that the reclaimer requeued.
    pub reclaimed: u64,
    /// Rows that the sweeper acted on.
    pub swept: u64,
    /// Clock ticks.
    pub advances: u64,
    /// Injected stalls.
    pub stalls: u64,
    /// Injected crashes.
    pub crashes: u64,
    /// Injected crashes that left a claim behind.
    pub abandons: u64,
}

impl WorldStats {
    /// Add the counters of `other`.
    pub const fn merge(&mut self, other: &Self) {
        self.idle_polls += other.idle_polls;
        self.activities += other.activities;
        self.cold += other.cold;
        self.warm += other.warm;
        self.declined += other.declined;
        self.other_tasks += other.other_tasks;
        self.timers_fired += other.timers_fired;
        self.signals += other.signals;
        self.scans += other.scans;
        self.fires += other.fires;
        self.lost_fires += other.lost_fires;
        self.reclaimed += other.reclaimed;
        self.swept += other.swept;
        self.advances += other.advances;
        self.stalls += other.stalls;
        self.crashes += other.crashes;
        self.abandons += other.abandons;
    }
}

/// The result of one world run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldReport {
    /// The config of the run.
    pub config: WorldConfig,
    /// One line per action, plus one line per new fact.
    pub trace: Vec<String>,
    /// The first failed invariant, if any. The run stops there.
    pub violation: Option<WorldViolation>,
    /// Coverage counters.
    pub stats: WorldStats,
    /// Whether all work completed.
    pub converged: bool,
    /// The last snapshot.
    pub last: Snapshot,
}

impl WorldReport {
    /// The last `lines` trace lines, joined by newlines.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// Run `config` against `world`.
pub async fn run<W: World>(config: &WorldConfig, world: W) -> WorldReport {
    Driver::new(config, world).run().await
}

/// Run `config` twice, each time on a world from `make`, and compare.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub async fn run_twice<W: World>(
    config: &WorldConfig,
    mut make: impl AsyncFnMut() -> W,
) -> Result<WorldReport, Nondeterminism> {
    let first = run(config, make().await).await;
    let second = run(config, make().await).await;
    compare(config.seed, first, &second)
}

/// The first report when `first` and `second` are equal.
fn compare(
    seed: u64,
    first: WorldReport,
    second: &WorldReport,
) -> Result<WorldReport, Nondeterminism> {
    let note = "equal trace, different stats or snapshot";
    diverged(seed, &first.trace, &second.trace, first == *second, note).map_or(Ok(first), Err)
}

/// The names of `checks` as a comma list, as [`WORLD_CHECKS_VAR`] takes them.
#[must_use]
pub fn checks_arg(checks: &[WorldInvariant]) -> String {
    let names: Vec<&str> = checks.iter().map(|i| i.name()).collect();
    names.join(",")
}

/// The config for `seed` with the values of [`PLANT_VAR`] and
/// [`WORLD_CHECKS_VAR`]. A missing value keeps the default.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_vars(
    seed: u64,
    plant: Option<&str>,
    checks: Option<&str>,
) -> Result<WorldConfig, String> {
    let mut config = WorldConfig::new(seed);
    if let Some(name) = plant {
        config.plant = Plant::parse(name.trim())?;
    }
    if let Some(list) = checks {
        config.checks = list
            .split(',')
            .map(|name| WorldInvariant::parse(name.trim()))
            .collect::<Result<_, _>>()?;
    }
    Ok(config)
}

/// [`config_from_vars`] with the values from the environment.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_env(seed: u64) -> Result<WorldConfig, String> {
    let plant = std::env::var(PLANT_VAR).ok();
    let checks = std::env::var(WORLD_CHECKS_VAR).ok();
    config_from_vars(seed, plant.as_deref(), checks.as_deref())
}

/// The shell command that replays `config` on a local Postgres.
///
/// It sets every variable that [`config_from_env`] reads.
#[must_use]
pub fn repro_command(config: &WorldConfig) -> String {
    format!(
        "{SEED_VAR}={} {PLANT_VAR}={} {WORLD_CHECKS_VAR}={} cargo test -p autumn-harvest \
         --test integration dst_world_tests::replay_one_world_seed -- --nocapture \
         --test-threads=1",
        config.seed,
        config.plant.as_str(),
        checks_arg(&config.checks)
    )
}

/// A seed that failed a world sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldSweepFailure {
    /// The config of the failed run.
    pub config: WorldConfig,
    /// The failed invariant or the determinism error.
    pub reason: String,
    /// The last trace lines of the run.
    pub trace_tail: String,
}

impl fmt::Display for WorldSweepFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed: {}\nreproduce: {}\nlast steps:\n{}",
            self.config.seed,
            self.reason,
            repro_command(&self.config),
            self.trace_tail
        )
    }
}

/// The result of a world sweep with no failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorldSweepSummary {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The merged coverage counters.
    pub stats: WorldStats,
}

/// Run every seed of `plan` twice, each run on a world from `make`.
///
/// # Errors
///
/// Returns the first seed that breaks an invariant or is not deterministic.
pub async fn sweep<W: World>(
    plan: &SeedPlan,
    config: impl Fn(u64) -> WorldConfig,
    mut make: impl AsyncFnMut(&WorldConfig) -> W,
) -> Result<WorldSweepSummary, Box<WorldSweepFailure>> {
    let mut summary = WorldSweepSummary::default();
    for seed in plan.seeds() {
        let config = config(seed);
        let first = run(&config, make(&config).await).await;
        let second = run(&config, make(&config).await).await;
        let note = "equal trace, different stats or snapshot";
        if let Some(error) = diverged(seed, &first.trace, &second.trace, first == second, note) {
            // Print the first run up to the line that differs.
            let end = (error.line + 1).min(first.trace.len());
            let start = end.saturating_sub(TAIL_LINES);
            return Err(Box::new(WorldSweepFailure {
                trace_tail: first.trace[start..end].join("\n"),
                config,
                reason: error.to_string(),
            }));
        }
        let report = first;
        if let Some(violation) = &report.violation {
            return Err(Box::new(WorldSweepFailure {
                config,
                reason: violation.to_string(),
                trace_tail: report.trace_tail(TAIL_LINES),
            }));
        }
        summary.seeds += 1;
        summary.stats.merge(&report.stats);
    }
    Ok(summary)
}

/// The life of one worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Life {
    Up,
    Stalled { until: u64 },
    Down { until: u64 },
}

#[derive(Debug, Clone, Copy)]
struct Actor {
    incarnation: u32,
    life: Life,
}

/// What the driver saw of one execution in earlier steps.
#[derive(Debug, Clone, Default)]
struct Seen {
    status: String,
    blocked: bool,
    /// The tick at which each event appeared.
    ticks: Vec<u64>,
}

/// The weight of each action kind in the scheduler draw.
const W_ADVANCE: u64 = 3;
const W_POLL: u64 = 8;
const W_BEAT: u64 = 2;
const W_STALL: u64 = 1;
const W_CRASH: u64 = 1;
const W_ABANDON: u64 = 1;
const W_RESTART: u64 = 4;
const W_SIGNAL: u64 = 1;
const W_SCAN: u64 = 2;
const W_FIRE: u64 = 3;
const W_RECLAIM: u64 = 1;
const W_SWEEP: u64 = 1;

struct Driver<'a, W> {
    config: &'a WorldConfig,
    world: W,
    rng: SplitMix64,
    tick: u64,
    step: usize,
    workers: Vec<Actor>,
    signalled: Vec<bool>,
    held: Vec<bool>,
    fired_slots: BTreeSet<u64>,
    seen: BTreeMap<String, Seen>,
    report: WorldReport,
}

impl<'a, W: World> Driver<'a, W> {
    fn new(config: &'a WorldConfig, world: W) -> Self {
        Self {
            config,
            world,
            rng: SplitMix64::new(config.seed),
            tick: 0,
            step: 0,
            workers: vec![
                Actor {
                    incarnation: 1,
                    life: Life::Up,
                };
                config.workers
            ],
            signalled: vec![false; config.workflows],
            held: vec![false; config.schedulers],
            fired_slots: BTreeSet::new(),
            seen: BTreeMap::new(),
            report: WorldReport {
                config: config.clone(),
                trace: Vec::new(),
                violation: None,
                stats: WorldStats::default(),
                converged: false,
                last: Snapshot::default(),
            },
        }
    }

    async fn run(mut self) -> WorldReport {
        let snapshot = self.world.snapshot().await;
        self.log(&format!(
            "world start -> {} executions",
            snapshot.executions.len()
        ));
        self.observe(snapshot);
        let limit = self.config.fault_steps + self.config.drain_steps;
        while self.step < limit && self.report.violation.is_none() {
            if self.draining() && self.done() {
                self.report.converged = true;
                self.log("all work complete");
                break;
            }
            let action = self.choose();
            let before = self.report.last.executions.len();
            let effect = self.act(action).await;
            if self.report.violation.is_none() {
                let snapshot = self.world.snapshot().await;
                self.observe(snapshot);
                self.fire_started_one(effect, before);
            }
            self.step += 1;
        }
        if self.report.violation.is_none() && !self.report.converged {
            self.report.converged = self.done();
            if !self.report.converged {
                let detail = format!("work remains after {limit} steps");
                self.fail(vec![(WorldInvariant::Converges, detail)]);
            }
        }
        self.report
    }

    const fn draining(&self) -> bool {
        self.step >= self.config.fault_steps
    }

    /// Whether all work is complete.
    fn done(&self) -> bool {
        let last = &self.report.last;
        last.schedules_done
            && self.signalled.iter().all(|sent| *sent)
            && last.executions.len() >= self.config.workflows
            && last
                .executions
                .iter()
                .all(|exec| !ACTIVE_STATES.contains(&exec.status.as_str()) && !exec.blocked)
    }

    fn worker_id(&self, w: usize) -> String {
        format!("w{}.{}", w + 1, self.workers[w].incarnation)
    }

    /// Wake each worker whose stall has ended.
    fn wake_stalled(&mut self) {
        for worker in &mut self.workers {
            if let Life::Stalled { until } = worker.life
                && self.tick >= until
            {
                worker.life = Life::Up;
            }
        }
    }

    /// Every enabled action with its weight, in a fixed order.
    fn enabled(&self) -> Vec<(u64, WorldAction)> {
        let faults = !self.draining();
        let mut actions = vec![(W_ADVANCE, WorldAction::Advance)];
        for (w, worker) in self.workers.iter().enumerate() {
            match worker.life {
                Life::Up => {
                    actions.push((W_POLL, WorldAction::Poll(w)));
                    actions.push((W_BEAT, WorldAction::Beat(w)));
                    if faults {
                        actions.push((W_STALL, WorldAction::Stall(w)));
                        actions.push((W_CRASH, WorldAction::Crash(w)));
                        actions.push((W_ABANDON, WorldAction::Abandon(w)));
                    }
                }
                Life::Down { until } if self.tick >= until || !faults => {
                    actions.push((W_RESTART, WorldAction::Restart(w)));
                }
                Life::Stalled { .. } | Life::Down { .. } => {}
            }
        }
        for (i, sent) in self.signalled.iter().enumerate() {
            if !sent {
                actions.push((W_SIGNAL, WorldAction::Signal(i)));
            }
        }
        for (s, held) in self.held.iter().enumerate() {
            actions.push((W_SCAN, WorldAction::ScheduleScan(s)));
            if *held {
                actions.push((W_FIRE, WorldAction::ScheduleFire(s)));
            }
        }
        actions.push((W_RECLAIM, WorldAction::Reclaim));
        actions.push((W_SWEEP, WorldAction::Sweep));
        actions
    }

    fn choose(&mut self) -> WorldAction {
        self.wake_stalled();
        let actions = self.enabled();
        let total: u64 = actions.iter().map(|(weight, _)| weight).sum();
        let mut pick = self.rng.below(total);
        for (weight, action) in actions {
            if pick < weight {
                return action;
            }
            pick -= weight;
        }
        WorldAction::Advance
    }

    /// A fault length: 1 to `stale_ticks + 2` ticks.
    const fn fault_ticks(&mut self) -> u64 {
        1 + self.rng.below(self.config.stale_ticks.saturating_add(2))
    }

    /// Apply `action`, log it, and return its effect.
    async fn act(&mut self, action: WorldAction) -> Effect {
        let until = self.prepare(action);
        let effect = self.world.apply(action, self.tick).await;
        self.record(action, effect, until);
        effect
    }

    /// Update the driver state before the world applies `action`.
    ///
    /// Returns the end tick of a stall or a crash.
    fn prepare(&mut self, action: WorldAction) -> u64 {
        let mut until = self.tick;
        match action {
            WorldAction::Advance => {
                self.tick += 1;
                self.report.stats.advances += 1;
            }
            WorldAction::Stall(w) => {
                until += self.fault_ticks();
                self.workers[w].life = Life::Stalled { until };
                self.report.stats.stalls += 1;
            }
            WorldAction::Crash(w) | WorldAction::Abandon(w) => {
                until += self.fault_ticks();
                self.workers[w].life = Life::Down { until };
                self.report.stats.crashes += 1;
            }
            WorldAction::Restart(w) => {
                self.workers[w].incarnation += 1;
                self.workers[w].life = Life::Up;
            }
            WorldAction::Signal(i) => {
                self.signalled[i] = true;
                self.report.stats.signals += 1;
            }
            WorldAction::ScheduleFire(s) => self.held[s] = false,
            WorldAction::Beat(_)
            | WorldAction::Poll(_)
            | WorldAction::ScheduleScan(_)
            | WorldAction::Reclaim
            | WorldAction::Sweep => {}
        }
        until
    }

    /// Count and log `effect`, the result of `action`.
    fn record(&mut self, action: WorldAction, effect: Effect, until: u64) {
        let line = match action {
            WorldAction::Advance => format!("clock advance -> {effect}"),
            WorldAction::Stall(w) => format!("{} stall -> until t={until:03}", self.worker_id(w)),
            WorldAction::Crash(w) => {
                format!("{} crash -> restart at t={until:03}", self.worker_id(w))
            }
            WorldAction::Abandon(w) => {
                if effect == Effect::Done {
                    self.report.stats.abandons += 1;
                }
                let id = self.worker_id(w);
                format!("{id} abandon -> {effect}, restart at t={until:03}")
            }
            WorldAction::Restart(w) => format!("{} restart -> {effect}", self.worker_id(w)),
            WorldAction::Beat(w) => format!("{} beat -> {effect}", self.worker_id(w)),
            WorldAction::Poll(w) => {
                self.count_poll(effect);
                format!("{} poll -> {effect}", self.worker_id(w))
            }
            WorldAction::Signal(i) => format!("client signal c{i} -> {effect}"),
            WorldAction::ScheduleScan(s) => {
                self.held[s] = matches!(effect, Effect::Scanned(due) if due > 0);
                if self.held[s] {
                    self.report.stats.scans += 1;
                }
                format!("sched{} scan -> {effect}", s + 1)
            }
            WorldAction::ScheduleFire(s) => format!("sched{} fire -> {effect}", s + 1),
            WorldAction::Reclaim => {
                if let Effect::Reclaimed(rows) = effect {
                    self.report.stats.reclaimed += rows;
                }
                format!("reclaimer pass -> {effect}")
            }
            WorldAction::Sweep => {
                if let Effect::Swept(rows) = effect {
                    self.report.stats.swept += rows;
                }
                format!("sweeper pass -> {effect}")
            }
        };
        self.log(&line);
        self.check_fire(effect);
    }

    /// Check that a won fire claim started exactly one execution.
    ///
    /// `before` is the number of executions before the step.
    fn fire_started_one(&mut self, effect: Effect, before: usize) {
        let Effect::Fired(Some(slot)) = effect else {
            return;
        };
        let started = self.report.last.executions.len().saturating_sub(before);
        if started != 1 {
            let detail = format!("slot t={slot:03} won its claim and started {started} runs");
            self.fail(vec![(WorldInvariant::FireStartsRun, detail)]);
        }
    }

    const fn count_poll(&mut self, effect: Effect) {
        let stats = &mut self.report.stats;
        match effect {
            Effect::Idle => stats.idle_polls += 1,
            Effect::Polled(Ran::Activity) => stats.activities += 1,
            Effect::Polled(Ran::Cold) => stats.cold += 1,
            Effect::Polled(Ran::Warm) => stats.warm += 1,
            Effect::Polled(Ran::Declined) => stats.declined += 1,
            Effect::Polled(Ran::Other) => stats.other_tasks += 1,
            _ => {}
        }
    }

    /// Check a fire claim. A slot fires once, and not before it is due.
    fn check_fire(&mut self, effect: Effect) {
        let Effect::Fired(slot) = effect else {
            return;
        };
        let Some(slot) = slot else {
            self.report.stats.lost_fires += 1;
            return;
        };
        self.report.stats.fires += 1;
        let mut found = Vec::new();
        if !self.fired_slots.insert(slot) {
            let detail = format!("slot t={slot:03} fired twice");
            found.push((WorldInvariant::ScheduleSlotOnce, detail));
        }
        if slot > self.tick {
            let detail = format!("slot t={slot:03} fired at t={:03}", self.tick);
            found.push((WorldInvariant::ScheduleNotEarly, detail));
        }
        self.fail(found);
    }

    fn log(&mut self, line: &str) {
        let text = format!("{:04} t={:03} {line}", self.step, self.tick);
        self.report.trace.push(text);
    }

    fn fail(&mut self, found: Vec<(WorldInvariant, String)>) {
        if self.report.violation.is_some() {
            return;
        }
        let checked = found
            .into_iter()
            .find(|(invariant, _)| self.config.checks.contains(invariant));
        if let Some((invariant, detail)) = checked {
            self.log(&format!("VIOLATION {invariant}: {detail}"));
            self.report.violation = Some(WorldViolation {
                invariant,
                step: self.step,
                detail,
            });
        }
    }

    /// Log what changed since the last snapshot, and check each execution.
    fn observe(&mut self, snapshot: Snapshot) {
        let mut found = Vec::new();
        for exec in &snapshot.executions {
            let mut seen = self.seen.remove(&exec.label).unwrap_or_default();
            let first_new = seen.ticks.len().min(exec.events.len());
            for fact in &exec.events[first_new..] {
                if matches!(fact, Fact::TimerFired { .. }) {
                    self.report.stats.timers_fired += 1;
                }
                self.log(&format!("  {} + {fact}", exec.label));
                seen.ticks.push(self.tick);
            }
            if exec.status != seen.status {
                self.log(&format!("  {} status {}", exec.label, exec.status));
                seen.status.clone_from(&exec.status);
            }
            if exec.blocked && !seen.blocked {
                self.log(&format!("  {} blocked", exec.label));
            }
            seen.blocked = exec.blocked;
            found.extend(check_exec(exec, &seen.ticks, first_new));
            self.seen.insert(exec.label.clone(), seen);
        }
        self.report.last = snapshot;
        self.fail(found);
    }
}

/// Check one execution. `ticks` holds the tick at which each event
/// appeared. Events from `first_new` on are new in this step.
fn check_exec(exec: &ExecFacts, ticks: &[u64], first_new: usize) -> Vec<(WorldInvariant, String)> {
    let label = &exec.label;
    let mut found = Vec::new();
    let events = &exec.events;

    let terminals = events.iter().filter(|fact| fact.is_terminal()).count();
    let last_is_terminal = events.last().is_some_and(Fact::is_terminal);
    if terminals > 1 || (terminals == 1 && !last_is_terminal) {
        let detail = format!(
            "{label} has {terminals} terminal events, last {:?}",
            events.last()
        );
        found.push((WorldInvariant::OneTerminal, detail));
    }
    if ACTIVE_STATES.contains(&exec.status.as_str()) == (terminals > 0) {
        let detail = format!(
            "{label} is {} with {terminals} terminal events",
            exec.status
        );
        found.push((WorldInvariant::StatusMatchesHistory, detail));
    }

    let mut results = BTreeMap::new();
    let mut fires = BTreeMap::new();
    for fact in events {
        match fact {
            Fact::ActivityCompleted { activity } => *results.entry(*activity).or_insert(0) += 1,
            Fact::TimerFired { timer } => *fires.entry(timer.as_str()).or_insert(0) += 1,
            _ => {}
        }
    }
    if let Some((activity, count)) = results.iter().find(|(_, count)| **count > 1) {
        let detail = format!("{label} a{activity} completed {count} times");
        found.push((WorldInvariant::ActivityResultOnce, detail));
    }
    if let Some((timer, count)) = fires.iter().find(|(_, count)| **count > 1) {
        let detail = format!("{label} timer {timer} fired {count} times");
        found.push((WorldInvariant::TimerFiresOnce, detail));
    }

    for (index, fact) in events.iter().enumerate().skip(first_new) {
        let Fact::TimerFired { timer } = fact else {
            continue;
        };
        let start = events[..index]
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, f)| match f {
                Fact::TimerStarted { timer: t, secs } if t == timer => Some((i, *secs)),
                _ => None,
            });
        let due = start.map(|(i, secs)| ticks[i] + secs.div_ceil(TICK_SECS));
        if due.is_none_or(|due| ticks[index] < due) {
            let detail = format!(
                "{label} timer {timer} fired at t={:03}, due {due:?}",
                ticks[index]
            );
            found.push((WorldInvariant::TimerNotEarly, detail));
        }
    }

    if exec.blocked || events.contains(&Fact::Failed) {
        let detail = format!("{label} blocked or failed in state {}", exec.status);
        found.push((WorldInvariant::Deterministic, detail));
    }

    let output = events.iter().find_map(|fact| match fact {
        Fact::Completed { output } => Some(output),
        _ => None,
    });
    if let (Some(output), Some(expected)) = (output, &exec.expected) {
        let parsed: Option<serde_json::Value> = serde_json::from_str(output).ok();
        if parsed.as_ref() != Some(expected) {
            let detail = format!("{label} returned {output}, expected {expected}");
            found.push((WorldInvariant::ExpectedOutput, detail));
        }
    }
    found
}
