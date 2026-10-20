//! Seeded interleaving simulation of user workflow code (issue #2033).
//!
//! [`WorkflowTestEnv`] lands every result of a suspension in one task, in
//! command order. Production can land the same results across many tasks, in
//! any order. A [`Simulation`] holds each result in a pending set. At each task
//! boundary a seed picks which results land, and in what order.
//!
//! The simulation holds activity results, child results, timer fires,
//! mid-run signals and a cancel. It produces only orders that the worker can
//! produce. See [`wake_order`] and `docs/testing/workflow-simulation.md`.
//!
//! [`Simulation::sweep`] runs a range of seeds. Each seed runs twice and the
//! traces must match. Each history must replay, and a crash at a seeded task
//! must not change the outcome. A failure prints a one-line replay command.
//!
//! [`always!`](crate::always) is a hard invariant. [`sometimes!`](crate::sometimes)
//! is a reachability target: a sweep fails when a label is never true. Both
//! macros live in [`sim_probe`](crate::sim_probe), so a workflow body can use
//! them in any build.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{
    BatchFlags, Deferred, DeliveryKey, RecordedSideOutput, ReplayStatus, RunMocks, TestRunOutcome,
    WorkflowTestEnv, fired_timer_duration_secs, virtual_now,
};
use crate::context::WorkflowCommand;
use crate::dst::{SeedPlan, SeedVars, SplitMix64, first_divergence};
use crate::event::WorkflowEvent;
use crate::executor::WorkflowOutcome;
use crate::info::WorkflowHandlerFn;
use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};

/// The one seed to run. It overrides every other seed setting.
pub const SEED_VAR: &str = "HARVEST_SIM_SEED";
/// The number of seeds in a sweep.
pub const SEEDS_VAR: &str = "HARVEST_SIM_SEEDS";
/// The first seed of a sweep.
pub const SEED_BASE_VAR: &str = "HARVEST_SIM_SEED_BASE";
/// The number of seeds in a sweep when nothing else sets it.
pub const DEFAULT_SEEDS: u64 = 64;
/// The attempts of an activity with no retry policy override.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// The number of trace lines that a failure prints.
pub const TAIL_LINES: usize = 40;

/// The seed variables of the workflow simulation.
const SIM_SEED_VARS: SeedVars = SeedVars {
    seed: SEED_VAR,
    seeds: SEEDS_VAR,
    base: SEED_BASE_VAR,
};

/// Salts that give each random stream of one seed its own sequence.
const FAULT_SALT: u64 = 0x2033_fa17_0000_0001;
const CRASH_SALT: u64 = 0x2033_c4a5_0000_0002;
const MOCK_SALT: u64 = 0x2033_70c4_0000_0003;
const EXEC_SALT: u64 = 0x2033_e7ec_0000_0004;
const SIDE_EFFECT_SALT: u64 = 0x2033_5eed_0000_0005;

/// The error of an attempt that the fault lane fails.
const TRANSIENT_FAULT: &str = "simulated transient fault";

/// A mock that gets a seeded generator and the virtual clock.
pub(crate) type SeededMockFn =
    Arc<dyn Fn(Value, &mut MockContext) -> Result<Value, String> + Send + Sync>;

// ---------------------------------------------------------------------------
// Seeded mocks
// ---------------------------------------------------------------------------

/// What a seeded mock knows about its call.
///
/// [`WorkflowTestEnv::mock_activity_seeded`] passes it to the mock. The
/// generator is a function of the seed, the activity name, the call number
/// and the attempt. So a mock that draws from it gives the same answer on
/// both runs of a seed.
pub struct MockContext {
    rng: SplitMix64,
    now: DateTime<Utc>,
    call: u32,
    attempt: u32,
}

impl MockContext {
    /// The seeded generator of this call.
    pub const fn rng(&mut self) -> &mut SplitMix64 {
        &mut self.rng
    }

    /// The virtual time of the call. It is the start time plus the fired timers.
    #[must_use]
    pub const fn now(&self) -> DateTime<Utc> {
        self.now
    }

    /// The 1-based number of this call of the activity name.
    #[must_use]
    pub const fn call(&self) -> u32 {
        self.call
    }

    /// The 1-based worker attempt of this call.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }
}

impl fmt::Debug for MockContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockContext")
            .field("now", &self.now)
            .field("call", &self.call)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// One call of an activity mock.
pub(crate) struct MockCall<'a> {
    pub(crate) name: &'a str,
    pub(crate) call: u32,
    attempt: u32,
    now: DateTime<Utc>,
    seed: u64,
}

impl<'a> MockCall<'a> {
    pub(crate) const fn new(
        name: &'a str,
        call: u32,
        attempt: u32,
        now: DateTime<Utc>,
        seed: u64,
    ) -> Self {
        Self {
            name,
            call,
            attempt,
            now,
            seed,
        }
    }

    /// The context that a seeded mock gets for this call.
    pub(crate) fn context(&self) -> MockContext {
        let mix = self.seed
            ^ MOCK_SALT
            ^ fnv1a(self.name.as_bytes())
            ^ (u64::from(self.call) << 32)
            ^ u64::from(self.attempt);
        let mut rng = SplitMix64::new(mix);
        // One draw spreads close inputs apart.
        let start = rng.next_u64();
        MockContext {
            rng: SplitMix64::new(start),
            now: self.now,
            call: self.call,
            attempt: self.attempt,
        }
    }
}

/// The 64-bit FNV-1a hash. It is stable on every platform and release.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

impl WorkflowTestEnv {
    /// Start a seeded simulation of this env (issue #2033).
    ///
    /// The mocks, state and settings of the env apply to every run. See
    /// [`Simulation`].
    #[must_use]
    pub fn simulation(&self) -> Simulation<'_> {
        Simulation {
            env: self,
            signals: Vec::new(),
            cancel: None,
            fault_percent: 0,
            faults: FaultPlan::new(),
            seeds: None,
            test_name: None,
            cargo_args: String::new(),
            required: BTreeSet::new(),
            replay_handler: None,
        }
    }

    /// Register a mock that gets a seeded generator and the virtual clock.
    ///
    /// Use it in place of a mock that reads `rand` or the wall clock. Such a
    /// mock gives a different answer on each run, so a seed does not replay.
    /// Outside a simulation the generator uses seed 0.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::testing::WorkflowTestEnv;
    /// # use serde_json::json;
    /// let env = WorkflowTestEnv::new()
    ///     .mock_activity_seeded("flip", |_, mock| Ok(json!(mock.rng().chance(50))));
    /// ```
    #[must_use]
    pub fn mock_activity_seeded<F>(mut self, name: impl Into<String>, mock: F) -> Self
    where
        F: Fn(Value, &mut MockContext) -> Result<Value, String> + Send + Sync + 'static,
    {
        let name = name.into();
        self.activity_mocks.remove(&name);
        self.seeded_mocks.insert(name, Arc::new(mock));
        self
    }
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

/// Faults by ordinal, such as "fail the second call of `send_invoice`".
///
/// The ordinal is the 1-based call number of the activity name, as in
/// [`WorkflowTestEnv::mock_activity_attempt`]. [`SimOutcome::faults`] lists
/// each fault that a run applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultPlan {
    entries: Vec<PlannedFault>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedFault {
    name: String,
    ordinal: u32,
    kind: FaultKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FaultKind {
    /// The call fails with this error. No retry follows.
    Fail(String),
    /// The first `n` attempts fail and the worker retries them.
    Transient(u32),
}

impl FaultPlan {
    /// An empty plan.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Call `ordinal` of `name` fails with `error`. The mock does not run.
    #[must_use]
    pub fn fail(mut self, name: &str, ordinal: u32, error: &str) -> Self {
        self.entries.push(PlannedFault {
            name: name.to_string(),
            ordinal,
            kind: FaultKind::Fail(error.to_string()),
        });
        self
    }

    /// The first `failures` attempts of call `ordinal` of `name` fail.
    ///
    /// The worker retries each one. A retryable failure writes only an
    /// `ActivityStarted` event. When the failures use every attempt, the call
    /// ends with `ActivityFailed`.
    #[must_use]
    pub fn transient(mut self, name: &str, ordinal: u32, failures: u32) -> Self {
        self.entries.push(PlannedFault {
            name: name.to_string(),
            ordinal,
            kind: FaultKind::Transient(failures),
        });
        self
    }

    fn find(&self, name: &str, ordinal: u32) -> Option<&FaultKind> {
        self.entries
            .iter()
            .find(|f| f.name == name && f.ordinal == ordinal)
            .map(|f| &f.kind)
    }
}

// ---------------------------------------------------------------------------
// Invariants
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The probe of the run that the current task drives.
    static PROBE: Arc<Mutex<Probe>>;
}

/// What `always!` and `sometimes!` record during one run.
#[derive(Debug, Default)]
struct Probe {
    /// The current task, so a violation names it. `None` in the check closure.
    task: Option<usize>,
    /// One message per false `always!` label, in first-seen order.
    violations: Vec<String>,
    failed: BTreeSet<String>,
    /// Each `sometimes!` label, and whether it was true at least once.
    sometimes: BTreeMap<String, bool>,
}

fn with_probe(f: impl FnOnce(&mut Probe)) {
    let _ = PROBE.try_with(|probe| {
        if let Ok(mut probe) = probe.lock() {
            f(&mut probe);
        }
    });
}

/// Record a false `always!` label in the current run, if any.
pub(crate) fn note_always<L: AsRef<str>>(label: impl FnOnce() -> L) {
    with_probe(|probe| {
        let label = label();
        let label = label.as_ref();
        if probe.failed.insert(label.to_string()) {
            let place = probe.task.map_or_else(
                || "in the check".to_string(),
                |task| format!("at task {task}"),
            );
            probe
                .violations
                .push(format!("always!({label:?}) is false {place}"));
        }
    });
}

/// Record a `sometimes!` label in the current run, if any.
pub(crate) fn note_sometimes<L: AsRef<str>>(cond: bool, label: impl FnOnce() -> L) {
    with_probe(|probe| {
        let label = label().as_ref().to_string();
        *probe.sometimes.entry(label).or_insert(false) |= cond;
    });
}

// ---------------------------------------------------------------------------
// The legal-order model
// ---------------------------------------------------------------------------

/// The events that one task-prep ingest appends (issue #2033).
///
/// This is the simulation's model of `worker::ingest_due_timers_and_signals`.
/// `clock` and every offset are seconds from one anchor:
///
/// - A timer is due when its deadline is at or before `clock`. Due timers
///   fire by `(deadline, timer_id)`. A timer that is not due waits.
/// - Every signal in `signals` lands, in its arrival order.
/// - A signal received strictly before a deadline lands before that fire.
///   A tie goes to the timer.
///
/// The model sorts timer ids by bytes. Postgres sorts them by collation, so
/// keep tied ids to plain ASCII letters and digits. Two tests tie this model
/// to the worker: one to `merge_wake_events`, one to the real ingest.
#[must_use]
pub fn wake_order(
    clock: u64,
    timers: &[(TimerId, u64)],
    signals: &[(String, Value, u64)],
) -> Vec<WorkflowEvent> {
    // Rank 0 sorts a timer before a signal of the same time.
    let mut items: Vec<(u64, u8, String, WorkflowEvent)> = timers
        .iter()
        .filter(|(_, deadline)| *deadline <= clock)
        .map(|(id, deadline)| {
            let fire = WorkflowEvent::TimerFired {
                timer_id: id.clone(),
            };
            (*deadline, 0, id.as_str().to_string(), fire)
        })
        .collect();
    items.extend(signals.iter().map(|(name, payload, received)| {
        let signal = WorkflowEvent::SignalReceived {
            signal_name: name.clone(),
            payload: payload.clone(),
        };
        (*received, 1, String::new(), signal)
    }));
    // A stable sort keeps the arrival order of equal signals.
    items.sort_by(|a, b| (a.0, a.1, &a.2).cmp(&(b.0, b.1, &b.2)));
    items.into_iter().map(|(_, _, _, event)| event).collect()
}

/// Check that a history obeys the rules that every simulated order keeps.
///
/// - A result lands after its scheduling event, and an activity or a child
///   has one terminal.
/// - A timer fires only while a `TimerStarted` for its id is active.
/// - No event follows `WorkflowCancelled`, except the cancel at index 1
///   that [`WorkflowTestEnv::with_cancellation`] adds.
///
/// Every sweep runs this check on each history. A failure is a bug in the
/// simulation, not in the workflow.
///
/// # Errors
///
/// Returns the first broken rule.
pub fn check_legal(events: &[WorkflowEvent]) -> Result<(), String> {
    let mut scheduled = HashSet::new();
    let mut finished = HashSet::new();
    let mut children = HashSet::new();
    let mut children_done = HashSet::new();
    let mut timers: HashMap<&str, usize> = HashMap::new();
    let mut cancelled = false;
    for (index, event) in events.iter().enumerate() {
        let kind = event.type_name();
        if cancelled {
            return Err(format!("event {index} ({kind}) comes after the cancel"));
        }
        match event {
            WorkflowEvent::ActivityScheduled { activity_id, .. } => {
                scheduled.insert(*activity_id);
            }
            WorkflowEvent::ActivityStarted { activity_id, .. } => {
                if !scheduled.contains(activity_id) {
                    return Err(format!(
                        "event {index} ({kind}) lands before it was scheduled"
                    ));
                }
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
                if !scheduled.contains(activity_id) {
                    return Err(format!(
                        "event {index} ({kind}) lands before it was scheduled"
                    ));
                }
                if !finished.insert(*activity_id) {
                    return Err(format!("event {index} ({kind}) is a second terminal"));
                }
            }
            WorkflowEvent::ChildWorkflowStarted { child_id, .. } => {
                children.insert(*child_id);
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                if !children.contains(child_id) {
                    return Err(format!(
                        "event {index} ({kind}) lands before it was scheduled"
                    ));
                }
                if !children_done.insert(*child_id) {
                    return Err(format!("event {index} ({kind}) is a second terminal"));
                }
            }
            WorkflowEvent::TimerStarted { timer_id, .. } => {
                *timers.entry(timer_id.as_str()).or_default() += 1;
            }
            WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id } => {
                match timers.get_mut(timer_id.as_str()) {
                    Some(active) if *active > 0 => *active -= 1,
                    _ => {
                        return Err(format!(
                            "event {index} ({kind}) has no active TimerStarted for {}",
                            timer_id.as_str()
                        ));
                    }
                }
            }
            // `with_cancellation` puts its cancel at index 1, and the body
            // still runs after it. Any later cancel is terminal.
            WorkflowEvent::WorkflowCancelled { .. } if index > 1 => cancelled = true,
            _ => {}
        }
    }
    Ok(())
}

/// The deadline of each active timer in `events`, in seconds from the start.
///
/// Each entry also holds the index of its `TimerStarted`. It walks the
/// history as `fired_timer_duration_secs` does, so a `TimerStarted` anchors
/// its deadline at the clock of that moment.
fn active_timer_deadlines(events: &[WorkflowEvent]) -> HashMap<String, VecDeque<(u64, usize)>> {
    let mut active: HashMap<String, VecDeque<(u64, usize)>> = HashMap::new();
    let mut now: u64 = 0;
    for (index, event) in events.iter().enumerate() {
        match event {
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => active
                .entry(timer_id.as_str().to_string())
                .or_default()
                .push_back((now.saturating_add(*duration_secs), index)),
            WorkflowEvent::TimerFired { timer_id } => {
                if let Some((deadline, _)) = active
                    .get_mut(timer_id.as_str())
                    .and_then(VecDeque::pop_front)
                {
                    now = now.max(deadline);
                }
            }
            WorkflowEvent::TimerCancelled { timer_id } => {
                if let Some(queue) = active.get_mut(timer_id.as_str()) {
                    queue.pop_front();
                }
            }
            _ => {}
        }
    }
    active
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// How a simulation run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SimStatus {
    /// The body returned `Ok`.
    Completed,
    /// The body returned `Err`, or the run diverged.
    Failed,
    /// The body continued as new.
    ContinuedAsNew,
    /// A scripted cancel landed. On Postgres a cancel is terminal.
    Cancelled,
    /// The body waits for an input that the simulation cannot supply.
    Blocked,
    /// A cycle deadlocked (issue #1797).
    TaskFailed,
    /// The test setup is wrong, for example a mock is missing.
    HarnessError,
}

impl SimStatus {
    /// The word that the trace uses for this status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::ContinuedAsNew => "continued_as_new",
            Self::Cancelled => "cancelled",
            Self::Blocked => "blocked",
            Self::TaskFailed => "task_failed",
            Self::HarnessError => "harness_error",
        }
    }

    /// Whether the history ends with a terminal event, so a strict replay applies.
    const fn is_sealed(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::ContinuedAsNew)
    }
}

impl fmt::Display for SimStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outcome of one simulation run.
pub struct SimOutcome {
    seed: u64,
    status: SimStatus,
    outcome: TestRunOutcome,
    /// The task of each event in history.
    event_tasks: Vec<usize>,
    /// A digest of each event, payloads included, with ids as ordinals.
    payloads: Vec<u64>,
    tasks: usize,
    trace: Vec<String>,
    faults: Vec<String>,
    crashed_at: Option<usize>,
    violations: Vec<String>,
    sometimes: BTreeMap<String, bool>,
}

impl SimOutcome {
    /// The seed of the run.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// How the run ended.
    #[must_use]
    pub const fn status(&self) -> SimStatus {
        self.status
    }

    /// The result of the workflow, or the reason that the run ended.
    #[must_use]
    pub const fn result(&self) -> &Result<Value, String> {
        &self.outcome.result
    }

    /// The history that the run built.
    #[must_use]
    pub fn events(&self) -> &[WorkflowEvent] {
        self.outcome.events()
    }

    /// The run as a [`TestRunOutcome`], for its other accessors.
    #[must_use]
    pub const fn outcome(&self) -> &TestRunOutcome {
        &self.outcome
    }

    /// The number of tasks, one per cycle. A crashed cycle does not count.
    #[must_use]
    pub const fn tasks(&self) -> usize {
        self.tasks
    }

    /// The tasks in which the events at `positions` landed.
    ///
    /// A position past the history is ignored.
    #[must_use]
    pub fn tasks_of_events(&self, positions: &[usize]) -> BTreeSet<usize> {
        positions
            .iter()
            .filter_map(|&position| self.event_tasks.get(position).copied())
            .collect()
    }

    /// The virtual time that the run took.
    #[must_use]
    pub fn elapsed(&self) -> chrono::Duration {
        self.outcome.elapsed()
    }

    /// The trace: one line per event, a crash line, and an end line.
    ///
    /// A line names the task, the virtual time and the event. Ids become
    /// ordinals such as `a1` and `c1`. No payload is in the trace, so a random
    /// value such as `ctx.new_uuid()` does not change it.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        &self.trace
    }

    /// The faults that the run applied, in order.
    #[must_use]
    pub fn faults(&self) -> &[String] {
        &self.faults
    }

    /// The task at which the run crashed and resumed, if it did.
    #[must_use]
    pub const fn crashed_at(&self) -> Option<usize> {
        self.crashed_at
    }

    /// The false `always!` labels of the run, as messages.
    #[must_use]
    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    /// Compare this run with `other`. The crash line does not count.
    ///
    /// # Errors
    ///
    /// Returns the first difference of the traces.
    pub fn same_outcome(&self, other: &Self) -> Result<(), String> {
        let a = comparable(&self.trace);
        let b = comparable(&other.trace);
        if let Some(line) = first_divergence(&a, &b) {
            return Err(format!(
                "trace line {line} differs\n  first:  {}\n  second: {}",
                a.get(line).map_or("", String::as_str),
                b.get(line).map_or("", String::as_str)
            ));
        }
        let index = self
            .payloads
            .iter()
            .zip(&other.payloads)
            .position(|(x, y)| x != y);
        index.map_or(Ok(()), |index| {
            Err(format!(
                "the payload of event {index} differs: {}",
                self.trace.get(index).map_or("", String::as_str)
            ))
        })
    }

    /// Replay the history of this run with `handler`.
    ///
    /// A finished history replays strictly, as
    /// [`TestRunOutcome::replay_check`] does. A cancelled, blocked or
    /// deadlocked history replays as an in-flight run, as the deploy canary
    /// does. The trailing `WorkflowCancelled` is left out, because the body
    /// never ran after it.
    ///
    /// The `ActivityStarted` events of an activity with no terminal are left
    /// out too. The matcher does not consume them at the frontier, so a
    /// canary replay reports them as unconsumed.
    #[must_use]
    pub async fn replay_check(&self, handler: WorkflowHandlerFn) -> super::ReplayReport {
        let (replayer, mut snapshot) = self.outcome.replay_parts(handler);
        if self.status.is_sealed() {
            return replayer.replay_from_snapshot(snapshot).await;
        }
        if self.status == SimStatus::Cancelled {
            snapshot.events.pop();
        }
        let finished: HashSet<ActivityExecId> = snapshot
            .events
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityFailed { activity_id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .collect();
        snapshot.events.retain(|event| match event {
            WorkflowEvent::ActivityStarted { activity_id, .. } => finished.contains(activity_id),
            _ => true,
        });
        replayer.replay_canary_snapshot(snapshot).await
    }
}

impl fmt::Debug for SimOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimOutcome")
            .field("seed", &self.seed)
            .field("status", &self.status)
            .field("result", self.result())
            .field("tasks", &self.tasks)
            .field("crashed_at", &self.crashed_at)
            .finish_non_exhaustive()
    }
}

/// The trace lines that two equal outcomes share.
fn comparable(trace: &[String]) -> Vec<String> {
    trace
        .iter()
        .filter(|line| !line.starts_with("crash "))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

/// The result of a sweep with no failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SweepReport {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The number of histories that passed the replay check.
    pub replayed: u64,
    /// The `sometimes!` labels that were true on at least one seed.
    pub reached: BTreeSet<String>,
    /// The number of runs that ended with each status.
    pub statuses: BTreeMap<SimStatus, u64>,
}

/// A sweep that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimFailure {
    /// The failed seed. For a sweep-wide failure, the first seed.
    pub seed: u64,
    /// What failed.
    pub reason: String,
    /// The shell command that replays the failure.
    pub repro: String,
    /// The last trace lines of the failed run.
    pub trace_tail: String,
    whole_sweep: bool,
}

impl fmt::Display for SimFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.whole_sweep {
            write!(f, "the sweep failed: {}", self.reason)?;
        } else {
            write!(f, "seed {} failed: {}", self.seed, self.reason)?;
        }
        write!(f, "\nreproduce: {}", self.repro)?;
        if !self.trace_tail.is_empty() {
            write!(f, "\nlast trace lines:\n{}", self.trace_tail)?;
        }
        Ok(())
    }
}

impl std::error::Error for SimFailure {}

/// The shell command that replays `seed` of the test `test_name`.
///
/// `cargo_args` comes before the test name, for example `--features testing`.
/// The command names the package when `CARGO_PKG_NAME` is set, as
/// `cargo test` sets it. It names the test target of the running binary. It
/// is one line, in POSIX shell syntax.
#[must_use]
pub fn repro_command(seed: u64, test_name: Option<&str>, cargo_args: &str) -> String {
    format!(
        "{SEED_VAR}={seed} {}",
        cargo_test_command(test_name, cargo_args)
    )
}

/// `cargo test` for `test_name`, with the package and target when known.
fn cargo_test_command(test_name: Option<&str>, cargo_args: &str) -> String {
    let package = std::env::var("CARGO_PKG_NAME").ok();
    let mut parts = vec!["cargo test".to_string()];
    if let Some(name) = &package {
        parts.push(format!("-p {name}"));
    }
    if !cargo_args.is_empty() {
        parts.push(cargo_args.to_string());
    }
    let names_target = ["--test", "--lib", "--bin", "--example", "--bench", "--doc"]
        .iter()
        .any(|flag| cargo_args.split_whitespace().any(|arg| arg == *flag));
    if !names_target {
        parts.extend(test_target(package.as_deref()));
    }
    match test_name {
        Some(name) => parts.push(format!("{name} -- --exact --nocapture")),
        None => parts.push("-- --nocapture".to_string()),
    }
    parts.join(" ")
}

/// The cargo target flag of the running test binary, such as `--test dst`.
///
/// Cargo names a test binary `<target>-<hash>`. The binary of the library's
/// unit tests carries the crate name, so it maps to `--lib`. A binary name
/// uses `_` where a target name can use `-`. For such a target, or a `bin`
/// target, name it in [`Simulation::cargo_args`] instead.
fn test_target(package: Option<&str>) -> Option<String> {
    let exe = std::env::current_exe().ok()?;
    let stem = exe.file_stem()?.to_str()?;
    let (target, hash) = stem.rsplit_once('-')?;
    if hash.len() != 16 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let lib = package.map(|name| name.replace('-', "_"));
    if lib.as_deref() == Some(target) {
        return Some("--lib".to_string());
    }
    Some(format!("--test {target}"))
}

// ---------------------------------------------------------------------------
// Simulation
// ---------------------------------------------------------------------------

/// A seeded simulation of a [`WorkflowTestEnv`] (issue #2033).
///
/// [`WorkflowTestEnv::simulation`] creates it. A seed fixes every delivery
/// choice, so a failing seed replays exactly.
///
/// ```rust,no_run
/// # use autumn_harvest::testing::WorkflowTestEnv;
/// # use autumn_harvest::context::WorkflowContext;
/// # use autumn_harvest::always;
/// # use serde_json::{Value, json};
/// # use std::pin::Pin;
/// # fn wf<'a>(ctx: &'a WorkflowContext, _: Value)
/// #   -> Pin<Box<dyn std::future::Future<Output=Result<Value,String>>+Send+'a>>
/// # { Box::pin(async move { Ok(json!(null)) }) }
/// # #[tokio::main] async fn main() {
/// let env = WorkflowTestEnv::new().mock_activity("charge", |_| Ok(json!("ok")));
/// env.simulation()
///     .signal("cancel_order", json!({}))
///     .sweep(wf, json!(null), |run| {
///         always!(run.result().is_ok(), "the order completes");
///     })
///     .await
///     .unwrap_or_else(|failure| panic!("{failure}"));
/// # }
/// ```
pub struct Simulation<'env> {
    env: &'env WorkflowTestEnv,
    signals: Vec<(String, Value)>,
    cancel: Option<String>,
    fault_percent: u8,
    faults: FaultPlan,
    seeds: Option<SeedPlan>,
    test_name: Option<String>,
    cargo_args: String,
    required: BTreeSet<String>,
    replay_handler: Option<WorkflowHandlerFn>,
}

impl Simulation<'_> {
    /// Send a signal at a seed-chosen task boundary.
    ///
    /// Signals arrive in the order of these calls. Each one lands at a
    /// task-prep ingest, ordered against due timers as the worker orders
    /// them. A signal that [`WorkflowTestEnv::queue_signal`] queued lands
    /// before the first task instead.
    #[must_use]
    pub fn signal(mut self, name: &str, payload: Value) -> Self {
        self.signals.push((name.to_string(), payload));
        self
    }

    /// Cancel the run at a seed-chosen task boundary.
    ///
    /// The cancel is terminal, as on Postgres. It appends
    /// `WorkflowCancelled`, and the body does not run again. To test a body
    /// that reacts to a cancel, use [`WorkflowTestEnv::with_cancellation`].
    #[must_use]
    pub fn cancel(mut self, reason: &str) -> Self {
        self.cancel = Some(reason.to_string());
        self
    }

    /// Fail each activity attempt with this chance, in percent.
    ///
    /// The worker retries a failed attempt. The attempts come from the
    /// retry policy override of the call, or [`DEFAULT_MAX_ATTEMPTS`]. When
    /// every attempt fails, the call ends with `ActivityFailed`. The mock
    /// does not run for a failed attempt. Values over 100 count as 100.
    #[must_use]
    pub fn fault_percent(mut self, percent: u8) -> Self {
        self.fault_percent = percent.min(100);
        self
    }

    /// Apply these faults by ordinal. See [`FaultPlan`].
    #[must_use]
    pub fn faults(mut self, plan: FaultPlan) -> Self {
        self.faults = plan;
        self
    }

    /// Sweep these seeds when no seed variable is set.
    ///
    /// [`SEED_VAR`] overrides every plan, so the replay command works for any
    /// test. [`SEEDS_VAR`] and [`SEED_BASE_VAR`] also override this plan, so
    /// a nightly job can widen it. With no plan and no variable, the sweep
    /// runs [`DEFAULT_SEEDS`] seeds from 0.
    #[must_use]
    pub const fn seeds(mut self, plan: SeedPlan) -> Self {
        self.seeds = Some(plan);
        self
    }

    /// The test name that the replay command names.
    ///
    /// The default is the name of the current thread. The libtest harness
    /// names each test thread after its test. A tokio worker thread does not
    /// count, so the command then runs every test.
    #[must_use]
    pub fn test_name(mut self, name: &str) -> Self {
        self.test_name = Some(name.to_string());
        self
    }

    /// Cargo arguments that the replay command needs, such as
    /// `--features testing`.
    #[must_use]
    pub fn cargo_args(mut self, args: &str) -> Self {
        self.cargo_args = args.to_string();
        self
    }

    /// Fail the sweep unless `sometimes!(_, label)` is true on some seed.
    ///
    /// A label counts only after a run evaluates it. A `sometimes!` on a path
    /// that no seed reaches is not seen, so the sweep cannot catch it. Name
    /// such a label here.
    #[must_use]
    pub fn require_reached(mut self, label: &str) -> Self {
        self.required.insert(label.to_string());
        self
    }

    /// Replay each history with `handler`, not with the handler that ran.
    ///
    /// Use it to check a new build of a workflow against the orders that the
    /// old build explored.
    #[must_use]
    pub fn replay_with(mut self, handler: WorkflowHandlerFn) -> Self {
        self.replay_handler = Some(handler);
        self
    }

    /// Run one seed.
    #[must_use]
    pub async fn run(&self, seed: u64, handler: WorkflowHandlerFn, input: Value) -> SimOutcome {
        self.run_scoped(seed, None, handler, input).await.0
    }

    /// Run one seed and crash after the cycle of `task`, before it persists.
    ///
    /// The cycle's commands are lost. The run then resumes from the history
    /// so far, as a new worker does. A correct body gives the same outcome as
    /// [`run`](Self::run). When the run has fewer tasks, no crash happens.
    #[must_use]
    pub async fn run_with_crash(
        &self,
        seed: u64,
        task: usize,
        handler: WorkflowHandlerFn,
        input: Value,
    ) -> SimOutcome {
        self.run_scoped(seed, Some(task), handler, input).await.0
    }

    /// Run `seed` inside a fresh probe scope.
    async fn run_scoped(
        &self,
        seed: u64,
        crash_at: Option<usize>,
        handler: WorkflowHandlerFn,
        input: Value,
    ) -> (SimOutcome, Arc<Mutex<Probe>>) {
        let probe = Arc::new(Mutex::new(Probe::default()));
        let run = SimRun::new(self, seed, crash_at, probe.clone(), &input);
        let mut outcome = PROBE.scope(probe.clone(), run.drive(handler, input)).await;
        let state = std::mem::take(&mut *lock(&probe));
        outcome.trace.extend(
            state
                .violations
                .iter()
                .map(|violation| format!("violation {violation}")),
        );
        outcome.trace.push(format!("end {}", outcome.status));
        outcome.violations = state.violations;
        outcome.sometimes = state.sometimes;
        (outcome, probe)
    }

    /// Sweep the seed plan and check each run (issue #2033).
    ///
    /// For each seed, the sweep:
    ///
    /// 1. runs the seed and calls `check` on the outcome;
    /// 2. runs the seed again and compares the traces;
    /// 3. checks the history with [`check_legal`];
    /// 4. fails on a false [`always!`](crate::always);
    /// 5. replays the history (see [`SimOutcome::replay_check`]);
    /// 6. runs the seed with a crash at a seeded task and compares the outcome.
    ///
    /// After the last seed, each [`sometimes!`](crate::sometimes) label must
    /// have been true at least once. A one-seed replay with [`SEED_VAR`] skips
    /// this check. A harness error, such as a missing mock, fails the sweep at
    /// once.
    ///
    /// # Errors
    ///
    /// Returns the first failure. Its `Display` holds a one-line replay
    /// command with [`SEED_VAR`]. A failure of the whole sweep names its
    /// seeds with [`SEED_BASE_VAR`] and [`SEEDS_VAR`] instead.
    pub async fn sweep(
        &self,
        handler: WorkflowHandlerFn,
        input: Value,
        mut check: impl FnMut(&SimOutcome),
    ) -> Result<SweepReport, Box<SimFailure>> {
        let test_name = self.test_name.clone().or_else(|| {
            std::thread::current()
                .name()
                .filter(|name| *name != "main" && !name.starts_with("tokio-"))
                .map(str::to_string)
        });
        let sweep_failure = |reason: String, plan: Option<SeedPlan>| {
            let command = cargo_test_command(test_name.as_deref(), &self.cargo_args);
            // A target can be true on one window and false on the next, so
            // the command names the window.
            let repro = match plan {
                Some(plan) => format!(
                    "{SEED_BASE_VAR}={} {SEEDS_VAR}={} {command}",
                    plan.first, plan.count
                ),
                None => command,
            };
            Box::new(SimFailure {
                seed: plan.map_or(0, |plan| plan.first),
                reason,
                repro,
                trace_tail: String::new(),
                whole_sweep: true,
            })
        };
        let plan = self.plan().map_err(|reason| sweep_failure(reason, None))?;
        let one_seed = std::env::var(SEED_VAR).is_ok();
        let mut report = SweepReport::default();
        let mut seen = self.required.clone();
        for seed in plan.seeds() {
            let first = self
                .check_seed(seed, handler, input.clone(), &mut check, one_seed)
                .await
                .map_err(|failure| {
                    let (outcome, reason) = *failure;
                    let tail = outcome.trace.len().saturating_sub(TAIL_LINES);
                    Box::new(SimFailure {
                        seed,
                        reason,
                        repro: repro_command(seed, test_name.as_deref(), &self.cargo_args),
                        trace_tail: outcome.trace[tail..].join("\n"),
                        whole_sweep: false,
                    })
                })?;
            report.replayed += 1;
            for (label, hit) in &first.sometimes {
                seen.insert(label.clone());
                if *hit {
                    report.reached.insert(label.clone());
                }
            }
            *report.statuses.entry(first.status).or_insert(0) += 1;
            report.seeds += 1;
        }

        if one_seed {
            eprintln!("{SEED_VAR} is set, so the sweep skips the sometimes! check");
        } else if let Some(label) = seen.difference(&report.reached).next() {
            let reason = format!(
                "sometimes!({label:?}) was never true in {} seeds",
                report.seeds
            );
            return Err(sweep_failure(reason, Some(plan)));
        }
        Ok(report)
    }

    /// Run and check one seed of a sweep. See [`sweep`](Self::sweep).
    async fn check_seed(
        &self,
        seed: u64,
        handler: WorkflowHandlerFn,
        input: Value,
        check: &mut impl FnMut(&SimOutcome),
        verbose: bool,
    ) -> Result<SimOutcome, Box<(SimOutcome, String)>> {
        let (mut first, probe) = self.run_scoped(seed, None, handler, input.clone()).await;
        if verbose {
            eprintln!("seed {seed} trace:\n{}", first.trace.join("\n"));
        }
        if first.status == SimStatus::HarnessError {
            let reason = first.result().clone().err().unwrap_or_default();
            return Err(Box::new((first, reason)));
        }
        PROBE.sync_scope(probe.clone(), || check(&first));
        let state = std::mem::take(&mut *lock(&probe));
        first.violations.extend(state.violations);
        for (label, hit) in state.sometimes {
            *first.sometimes.entry(label).or_insert(false) |= hit;
        }

        let second = self.run(seed, handler, input.clone()).await;
        if let Some(line) = first_divergence(&first.trace, &second.trace) {
            let at = |trace: &[String]| trace.get(line).cloned().unwrap_or_default();
            let reason = format!(
                "seed {seed} is not deterministic: trace line {line} differs\n  run 1: {}\n  run 2: {}",
                at(&first.trace),
                at(&second.trace)
            );
            return Err(Box::new((first, reason)));
        }
        if let Err(diff) = first.same_outcome(&second) {
            let reason = format!("seed {seed} is not deterministic: {diff}");
            return Err(Box::new((first, reason)));
        }
        if let Err(rule) = check_legal(first.events()) {
            return Err(Box::new((
                first,
                format!("the simulation built an illegal history: {rule}"),
            )));
        }
        if let Some(violation) = first.violations.first().cloned() {
            return Err(Box::new((first, violation)));
        }
        let replay = first
            .replay_check(self.replay_handler.unwrap_or(handler))
            .await;
        if !matches!(replay.status, ReplayStatus::ReplaySucceeded) {
            return Err(Box::new((
                first,
                format!("the history does not replay: {replay}"),
            )));
        }
        if first.tasks > 0 {
            let mut rng = SplitMix64::new(seed ^ CRASH_SALT);
            let task = usize::try_from(rng.below(first.tasks as u64))
                .unwrap_or(0)
                .saturating_add(1);
            let resumed = self.run_with_crash(seed, task, handler, input).await;
            if let Err(diff) = first.same_outcome(&resumed) {
                return Err(Box::new((
                    first,
                    format!("a crash at task {task} changes the outcome: {diff}"),
                )));
            }
        }
        Ok(first)
    }

    /// The seeds to sweep. Each seed variable wins over its part of the
    /// plan in code.
    fn plan(&self) -> Result<SeedPlan, String> {
        let code = self.seeds.unwrap_or(SeedPlan {
            first: 0,
            count: DEFAULT_SEEDS,
        });
        let var = |name: &str| std::env::var(name).ok();
        let (seed, seeds, base) = (var(SEED_VAR), var(SEEDS_VAR), var(SEED_BASE_VAR));
        let mut plan = SeedPlan::parse_named(
            SIM_SEED_VARS,
            seed.as_deref(),
            seeds.as_deref(),
            base.as_deref(),
            code.count,
        )?;
        if seed.is_none() && base.is_none() {
            plan.first = code.first;
        }
        if plan.count == 0 {
            return Err("the seed plan has no seed, so the sweep would test nothing".to_string());
        }
        Ok(plan)
    }
}

fn lock(probe: &Mutex<Probe>) -> std::sync::MutexGuard<'_, Probe> {
    probe
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// One run
// ---------------------------------------------------------------------------

/// A result or a timer fire that waits for a seed-chosen task.
struct Held {
    key: DeliveryKey,
    /// The events, one delivery each. An activity lands each attempt's
    /// `ActivityStarted` before its terminal.
    steps: VecDeque<Vec<WorkflowEvent>>,
    /// A timer fire that only lands with another delivery. See [`SimRun::process`].
    piggyback: bool,
}

/// A held timer at one task boundary.
struct HeldTimer {
    id: TimerId,
    deadline: u64,
    /// The task that started the timer. Ties between tasks follow it.
    armed_in: usize,
    piggyback: bool,
}

/// What the worker does after it persists a suspension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Next {
    /// A local activity ran inline. The worker drives the body again in the
    /// same decision, with no ingest between.
    Redrive,
    /// A command resolved in this task, such as a mutex grant or an external
    /// operation. A batch of bookkeeping only also requeues. A new task runs
    /// even when no delivery lands.
    Requeue,
    /// The batch waits. It parks even when it appended a marker or a side
    /// effect. With no delivery, the run is blocked.
    Park,
}

/// What a task boundary delivered.
enum Delivery {
    /// This many events landed.
    Events(usize),
    /// Only `ActivityStarted` events landed. The worker appends them without
    /// a wake, so no cycle runs.
    Quiet,
    /// The scripted cancel landed.
    Cancelled,
}

/// Whether a batch parks the task with no wake-up time.
///
/// The worker persists a batch with one kind of wait and no classic timer
/// on its own path. The kinds are new activities, activity waits, signal
/// waits and new children. Each such path ignores the deadline of an awaited
/// timer, so the task stays parked until another event wakes it.
/// A mixed batch takes the mixed path, which reschedules the task to the
/// earliest deadline.
fn parks_without_deadline(commands: &[WorkflowCommand]) -> bool {
    let mut kinds = BTreeSet::new();
    for cmd in commands {
        let kind = match cmd {
            WorkflowCommand::StartTimer { .. } => return false,
            WorkflowCommand::ScheduleActivity { .. } => 0,
            WorkflowCommand::WaitForActivity { .. } => 1,
            WorkflowCommand::WaitForSignal { .. } => 2,
            WorkflowCommand::StartChildWorkflow { .. } => 3,
            WorkflowCommand::ScheduleExternalActivity { .. } => 4,
            _ => continue,
        };
        kinds.insert(kind);
    }
    kinds.len() == 1
}

/// The start time of a simulation run, unless the env froze its own.
///
/// A fixed start makes `ctx.now()` the same on every machine and day. It is
/// Monday 2024-01-01 00:00 UTC.
fn sim_start(env: &WorkflowTestEnv) -> DateTime<Utc> {
    if env.anchor_frozen {
        return env.simulated_now;
    }
    DateTime::from_timestamp(1_704_067_200, 0).unwrap_or(env.simulated_now)
}

/// The state of one simulation run.
struct SimRun<'a, 'env> {
    sim: &'a Simulation<'env>,
    env: &'env WorkflowTestEnv,
    seed: u64,
    start: DateTime<Utc>,
    /// The delivery choices.
    rng: SplitMix64,
    /// The fault lane.
    fault_rng: SplitMix64,
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    event_tasks: Vec<usize>,
    held: Vec<Held>,
    signals: VecDeque<(String, Value)>,
    cancel: Option<String>,
    mocks: RunMocks,
    side_output: RecordedSideOutput,
    faults: Vec<String>,
    task: usize,
    crash_at: Option<usize>,
    crashed_at: Option<usize>,
    probe: Arc<Mutex<Probe>>,
}

impl<'a, 'env> SimRun<'a, 'env> {
    fn new(
        sim: &'a Simulation<'env>,
        seed: u64,
        crash_at: Option<usize>,
        probe: Arc<Mutex<Probe>>,
        input: &Value,
    ) -> Self {
        let env = sim.env;
        let start = sim_start(env);
        let mut history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: start,
            last_completion_result: env.last_completion_result.clone(),
            last_error: env.last_error.clone(),
            scheduled_time: env.scheduled_time,
        }];
        if let Some(reason) = &env.cancellation_reason {
            history.push(WorkflowEvent::WorkflowCancelled {
                reason: reason.clone(),
            });
        }
        // A queued signal lands at the task-prep of the first task.
        for (name, payload) in &env.queued_signals {
            history.push(WorkflowEvent::SignalReceived {
                signal_name: name.clone(),
                payload: payload.clone(),
            });
        }
        let event_tasks = vec![0; history.len()];
        Self {
            sim,
            env,
            seed,
            start,
            rng: SplitMix64::new(seed),
            fault_rng: SplitMix64::new(seed ^ FAULT_SALT),
            exec_id: ExecutionId::from_uuid(uuid::Uuid::from_u64_pair(EXEC_SALT, seed)),
            history,
            event_tasks,
            held: Vec::new(),
            signals: sim.signals.iter().cloned().collect(),
            cancel: sim.cancel.clone(),
            mocks: env.run_mocks(seed, start),
            side_output: RecordedSideOutput::default(),
            faults: Vec::new(),
            task: 0,
            crash_at,
            crashed_at: None,
            probe,
        }
    }

    /// Drive the run to its end.
    async fn drive(mut self, handler: WorkflowHandlerFn, input: Value) -> SimOutcome {
        let span_meta = self.env.span_meta();
        // A crashed cycle does not count against the cap.
        while self.task < self.env.max_iterations {
            self.task += 1;
            lock(&self.probe).task = Some(self.task);
            // The draws of a cycle depend on the seed and the history only.
            // A first draw spreads the seeds apart, so no two seeds share a cycle.
            let spread = SplitMix64::new(self.seed ^ SIDE_EFFECT_SALT).next_u64();
            let draws = spread ^ (self.history.len() as u64);
            let now = virtual_now(self.start, &self.history);
            let ctx = self
                .env
                .cycle_context(
                    self.exec_id,
                    self.history.clone(),
                    span_meta.as_ref(),
                    self.env.metrics.clone(),
                    self.env.workflow_log_policy,
                )
                .with_seeded_side_effects(draws, now);
            let (outcome, pending_cmds, _span, _router) =
                crate::executor::drive_workflow(ctx, handler, input.clone(), span_meta.as_ref())
                    .await;

            // The worker dies before the cycle persists. A new worker then
            // replays the same history and runs the cycle again.
            if self.crash_at == Some(self.task) && self.crashed_at.is_none() {
                self.crashed_at = Some(self.task);
                self.task -= 1;
                continue;
            }

            if let WorkflowOutcome::Suspended { commands } = &outcome {
                self.side_output.accumulate(commands);
            }
            self.side_output.accumulate(&pending_cmds);

            let WorkflowOutcome::Suspended { commands } = outcome else {
                return self.finish_terminal(outcome, &pending_cmds);
            };
            let next = match self.process(commands) {
                Ok(next) => next,
                Err(error) => return self.finish(SimStatus::HarnessError, Err(error)),
            };
            // The worker runs the body again in the same decision. No ingest
            // runs between the two drives.
            if next == Next::Redrive {
                continue;
            }
            let mut delivery = self.deliver();
            while matches!(delivery, Delivery::Quiet) {
                delivery = self.deliver();
            }
            match delivery {
                Delivery::Quiet => unreachable!("the loop above lands a waking delivery"),
                Delivery::Cancelled => {
                    let reason = self.cancel.take().unwrap_or_default();
                    return self.finish(SimStatus::Cancelled, Err(reason));
                }
                Delivery::Events(0) if next == Next::Park => {
                    let error = "the workflow is blocked: no held result, timer, \
                                 signal or cancel can unblock it"
                        .to_string();
                    return self.finish(SimStatus::Blocked, Err(error));
                }
                Delivery::Events(_) => {}
            }
        }
        let cap = self.env.max_iterations;
        let error = format!("the simulation exceeded {cap} tasks");
        self.finish(SimStatus::HarnessError, Err(error))
    }

    /// Tag the events that this task appended.
    fn tag(&mut self, task: usize) {
        self.event_tasks.resize(self.history.len(), task);
    }

    /// Persist one suspension. Results and fires join the held set.
    ///
    /// The sim holds every awaited cancellable timer, because the worker
    /// stores a timer row for each one. See [`parks_without_deadline`] for
    /// the batches that set no wake-up time. Their timers fire only when
    /// another delivery wakes the task. The latest batch decides this for
    /// every held timer.
    ///
    /// It returns what the worker does next, as [`Next`] tells.
    fn process(&mut self, commands: Vec<WorkflowCommand>) -> Result<Next, String> {
        let batch = BatchFlags::of(&commands);
        let before = self.history.len();
        let mut redrive = false;
        let mut requeue = false;
        let piggyback = parks_without_deadline(&commands);
        // The sim orders timers by deadline itself, so it holds every awaited
        // fire, not only the earliest.
        let flags = BatchFlags {
            competing_suspension: false,
            min_await_deadline_secs: None,
            ..batch
        };
        let mut deferred = Vec::new();
        for cmd in commands {
            self.drop_cancelled(&cmd);
            if let WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                retry_policy_override,
                ..
            } = cmd
            {
                self.history.push(WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue,
                });
                let max_attempts = retry_policy_override
                    .map_or(DEFAULT_MAX_ATTEMPTS, |policy| policy.max_attempts.max(1));
                let events = self.resolve_activity(activity_id, &name, input, max_attempts)?;
                deferred.push(Deferred::new(DeliveryKey::Activity(activity_id), events));
                continue;
            }
            let inline = matches!(cmd, WorkflowCommand::RunLocalActivity { .. });
            let resolves_now = matches!(
                cmd,
                WorkflowCommand::AcquireMutex { .. }
                    | WorkflowCommand::SignalExternalWorkflow { .. }
                    | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                    | WorkflowCommand::AwaitExternalWorkflow { .. }
            );
            let progress = self.env.process_command(
                cmd,
                &flags,
                &mut self.history,
                &mut deferred,
                &mut self.mocks,
            )?;
            redrive |= inline && progress;
            requeue |= resolves_now && progress;
        }
        for held in &mut self.held {
            held.piggyback = piggyback && matches!(held.key, DeliveryKey::Timer(_));
        }
        for Deferred { key, events } in deferred {
            match key {
                DeliveryKey::Now => self.history.extend(events),
                key if self.held.iter().any(|h| h.key == key) => {}
                // A fire needs an active `TimerStarted`. A re-arm of a timer
                // that already fired gets no second fire.
                DeliveryKey::Timer(id)
                    if !WorkflowTestEnv::timer_is_active_in_history(&self.history, &id) => {}
                key => {
                    let steps = match key {
                        // Each event of an activity lands in its own step.
                        DeliveryKey::Activity(_) => events.into_iter().map(|e| vec![e]).collect(),
                        _ => VecDeque::from([events]),
                    };
                    let piggyback = piggyback && matches!(key, DeliveryKey::Timer(_));
                    self.held.push(Held {
                        key,
                        steps,
                        piggyback,
                    });
                }
            }
        }
        requeue |= !batch.competing_suspension && self.history.len() != before;
        self.tag(self.task);
        Ok(if redrive {
            Next::Redrive
        } else if requeue {
            Next::Requeue
        } else {
            Next::Park
        })
    }

    /// Drop the held results and fires that `cmd` cancels.
    fn drop_cancelled(&mut self, cmd: &WorkflowCommand) {
        match cmd {
            WorkflowCommand::CancelTimer { timer_id } => {
                self.drop_held(&DeliveryKey::Timer(timer_id.clone()));
            }
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                timers,
                ..
            } => {
                for id in activities {
                    self.drop_held(&DeliveryKey::Activity(*id));
                }
                for id in children {
                    self.drop_held(&DeliveryKey::Child(*id));
                }
                for id in timers {
                    self.drop_held(&DeliveryKey::Timer(id.clone()));
                }
            }
            _ => {}
        }
    }

    fn drop_held(&mut self, key: &DeliveryKey) {
        self.held.retain(|h| &h.key != key);
    }

    /// The attempt events and the terminal of one activity call.
    ///
    /// The worker appends `ActivityStarted` when each attempt starts. A
    /// retryable failure writes no other event. The last attempt writes the
    /// terminal. A mock `Err` ends the call, as in the default env.
    fn resolve_activity(
        &mut self,
        activity_id: ActivityExecId,
        name: &str,
        input: Value,
        max_attempts: u32,
    ) -> Result<Vec<WorkflowEvent>, String> {
        let call = WorkflowTestEnv::next_call_count(&mut self.mocks.call_counts, name);
        let started = WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("test-worker"),
        };
        let mut events = Vec::new();

        let mut failed_attempts = 0;
        match self.sim.faults.find(name, call).cloned() {
            Some(FaultKind::Fail(error)) => {
                self.faults.push(format!("{name}#{call} failed: {error}"));
                events.push(started);
                events.push(failed(activity_id, error, 1));
                return Ok(events);
            }
            Some(FaultKind::Transient(failures)) => {
                failed_attempts = failures.min(max_attempts);
                for attempt in 1..=failed_attempts {
                    self.faults
                        .push(format!("{name}#{call} attempt {attempt} failed"));
                }
            }
            None => {
                if let Some(attempts) = self
                    .mocks
                    .retry_sequences
                    .get_mut(name)
                    .and_then(VecDeque::pop_front)
                {
                    WorkflowTestEnv::push_activity_retry_sequence(
                        &mut events,
                        activity_id,
                        attempts,
                    );
                    return Ok(events);
                }
                while failed_attempts < max_attempts
                    && self.sim.fault_percent > 0
                    && self.fault_rng.chance(u64::from(self.sim.fault_percent))
                {
                    failed_attempts += 1;
                    self.faults.push(format!(
                        "{name}#{call} attempt {failed_attempts} failed by the fault lane"
                    ));
                }
            }
        }

        for _ in 0..failed_attempts {
            events.push(started.clone());
        }
        if failed_attempts == max_attempts {
            self.faults.push(format!(
                "{name}#{call} failed after {max_attempts} attempts"
            ));
            events.push(failed(
                activity_id,
                TRANSIENT_FAULT.to_string(),
                max_attempts,
            ));
            return Ok(events);
        }
        let attempt = failed_attempts + 1;
        events.push(started);
        let now = virtual_now(self.start, &self.history);
        let mock = MockCall::new(name, call, attempt, now, self.seed);
        match self.env.resolve_activity(&mock, input)? {
            Ok(output) => events.push(WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            }),
            Err(error) => events.push(failed(activity_id, error, attempt)),
        }
        Ok(events)
    }

    /// The held timers, with their deadlines and the tasks that armed them.
    fn held_timers(&self, clock: u64) -> Vec<HeldTimer> {
        let deadlines = active_timer_deadlines(&self.history);
        self.held
            .iter()
            .filter_map(|h| match &h.key {
                DeliveryKey::Timer(id) => {
                    let (deadline, started_at) = deadlines
                        .get(id.as_str())
                        .and_then(VecDeque::front)
                        .copied()
                        .unwrap_or((clock, self.history.len()));
                    Some(HeldTimer {
                        id: id.clone(),
                        deadline,
                        armed_in: self
                            .event_tasks
                            .get(started_at)
                            .copied()
                            .unwrap_or(self.task),
                        piggyback: h.piggyback,
                    })
                }
                _ => None,
            })
            .collect()
    }

    /// Land a seed-chosen part of the held set at the next task boundary.
    ///
    /// The rules are in `DESIGN-2033.md` section 2. Results land first, in
    /// a seeded order. The ingest of signals and due fires follows.
    fn deliver(&mut self) -> Delivery {
        let clock = fired_timer_duration_secs(&self.history);
        let timers = self.held_timers(clock);
        let wakes = timers.iter().any(|t| !t.piggyback);
        let mut results: Vec<usize> = (0..self.held.len())
            .filter(|&i| !matches!(self.held[i].key, DeliveryKey::Timer(_)))
            .collect();
        // A Fisher-Yates shuffle sets the delivery order.
        for i in (1..results.len()).rev() {
            let j = usize::try_from(self.rng.below(i as u64 + 1)).unwrap_or(0);
            results.swap(i, j);
        }

        let left = self.signals.len() as u64;
        let mut take = usize::try_from(self.rng.below(results.len() as u64 + 1)).unwrap_or(0);
        let mut advance = wakes && self.rng.chance(50);
        let mut arrive = if left > 0 && self.rng.chance(50) {
            1 + self.rng.below(left)
        } else {
            0
        };
        let mut cancel = self.cancel.is_some() && self.rng.chance(25);
        if take == 0 && !advance && arrive == 0 && !cancel {
            // Each boundary lands something, so the run makes progress.
            let mut options = Vec::new();
            if !results.is_empty() {
                options.push(0);
            }
            if wakes {
                options.push(1);
            }
            if left > 0 {
                options.push(2);
            }
            if options.is_empty() {
                cancel = self.cancel.is_some();
            } else {
                let pick = usize::try_from(self.rng.below(options.len() as u64)).unwrap_or(0);
                match options[pick] {
                    0 => take = 1,
                    1 => advance = true,
                    _ => arrive = 1,
                }
            }
        }
        // A result or a signal wakes the task, so time can pass to any
        // deadline. An `ActivityStarted` alone wakes nothing.
        let woken = arrive > 0
            || results[..take].iter().any(|&i| {
                self.held[i].steps.front().is_some_and(|step| {
                    step.iter()
                        .any(|e| !matches!(e, WorkflowEvent::ActivityStarted { .. }))
                })
            });
        if woken && !advance && timers.iter().any(|t| t.piggyback) {
            advance = self.rng.chance(50);
        }

        let before = self.history.len();
        self.land_results(&results[..take]);
        if cancel {
            self.held.clear();
            self.history.push(WorkflowEvent::WorkflowCancelled {
                reason: self.cancel.clone().unwrap_or_default(),
            });
            self.tag(self.task + 1);
            return Delivery::Cancelled;
        }
        let ingested = self.history.len();
        self.ingest(clock, &timers, advance, woken, arrive);
        self.tag(self.task + 1);
        let quiet = self.history.len() == ingested
            && self.history[before..ingested]
                .iter()
                .all(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }));
        if quiet && ingested > before {
            return Delivery::Quiet;
        }
        Delivery::Events(self.history.len() - before)
    }

    /// Land the next step of each chosen result, in the chosen order.
    fn land_results(&mut self, chosen: &[usize]) {
        for &i in chosen {
            if let Some(step) = self.held[i].steps.pop_front() {
                self.history.extend(step);
            }
        }
        self.held.retain(|h| !h.steps.is_empty());
    }

    /// The task-prep ingest: the arrived signals and the due timer fires.
    ///
    /// Only the next deadline can bring timers due. A piggyback timer counts
    /// only when another delivery woke the task. Signals arrive before the new
    /// deadline or at it, as the seed picks.
    fn ingest(
        &mut self,
        clock: u64,
        timers: &[HeldTimer],
        advance: bool,
        woken: bool,
        arrive: u64,
    ) {
        let pool = timers.iter().filter(|t| woken || !t.piggyback);
        let target = if advance {
            pool.map(|t| t.deadline).min().unwrap_or(clock)
        } else {
            clock
        };
        let new_clock = clock.max(target);
        let early = if new_clock > clock {
            self.rng.below(arrive + 1)
        } else {
            arrive
        };
        // Offsets are in milliseconds here. Equal deadlines from different
        // tasks then fire in arm order, as distinct `NOW()` values do. A
        // signal sorts after a timer of the same second.
        let keyed: Vec<(TimerId, u64)> = timers
            .iter()
            .filter(|t| t.deadline <= new_clock && (woken || advance || !t.piggyback))
            .map(|t| {
                let rank = u64::try_from(t.armed_in).unwrap_or(u64::MAX).min(998);
                (
                    t.id.clone(),
                    t.deadline.saturating_mul(1000).saturating_add(rank),
                )
            })
            .collect();
        let signals: Vec<(String, Value, u64)> = (0..arrive)
            .filter_map(|n| {
                let (name, payload) = self.signals.pop_front()?;
                let received = if n < early { clock } else { new_clock };
                Some((
                    name,
                    payload,
                    received.saturating_mul(1000).saturating_add(999),
                ))
            })
            .collect();
        let wake = wake_order(
            new_clock.saturating_mul(1000).saturating_add(999),
            &keyed,
            &signals,
        );
        for event in &wake {
            if let WorkflowEvent::TimerFired { timer_id } = event {
                self.drop_held(&DeliveryKey::Timer(timer_id.clone()));
            }
        }
        self.history.extend(wake);
    }

    /// End the run on a terminal cycle.
    fn finish_terminal(
        mut self,
        outcome: WorkflowOutcome,
        pending: &[WorkflowCommand],
    ) -> SimOutcome {
        let status = match &outcome {
            WorkflowOutcome::Completed { .. } => SimStatus::Completed,
            WorkflowOutcome::Failed { .. } => SimStatus::Failed,
            WorkflowOutcome::ContinuedAsNew { .. } => SimStatus::ContinuedAsNew,
            WorkflowOutcome::TaskFailed { .. } => SimStatus::TaskFailed,
            WorkflowOutcome::Suspended { .. } => SimStatus::Blocked,
        };
        let run = self.env.finish_terminal_outcome(
            outcome,
            pending,
            std::mem::take(&mut self.history),
            self.exec_id,
            self.start,
            std::mem::take(&mut self.side_output),
        );
        self.event_tasks.resize(run.events.len(), self.task);
        self.outcome(status, run)
    }

    /// End the run with `status` and `result`, keeping the history so far.
    fn finish(mut self, status: SimStatus, result: Result<Value, String>) -> SimOutcome {
        self.tag(self.task);
        let run = self.env.outcome(
            result,
            std::mem::take(&mut self.history),
            self.exec_id,
            self.start,
            std::mem::take(&mut self.side_output),
        );
        self.outcome(status, run)
    }

    fn outcome(self, status: SimStatus, run: TestRunOutcome) -> SimOutcome {
        let mut trace = render_trace(&run.events, &self.event_tasks);
        let payloads = payload_digests(&run.events);
        if let Some(task) = self.crashed_at {
            trace.push(format!("crash after the cycle of task {task}"));
        }
        SimOutcome {
            seed: self.seed,
            status,
            outcome: run,
            event_tasks: self.event_tasks,
            payloads,
            tasks: self.task,
            trace,
            faults: self.faults,
            crashed_at: self.crashed_at,
            violations: Vec::new(),
            sometimes: BTreeMap::new(),
        }
    }
}

/// A terminal `ActivityFailed` after `attempt` attempts.
fn failed(activity_id: ActivityExecId, error: String, attempt: u32) -> WorkflowEvent {
    WorkflowEvent::ActivityFailed {
        activity_id,
        error,
        attempt,
        error_type: "Error".to_string(),
        non_retryable: false,
        details: None,
    }
}

// ---------------------------------------------------------------------------
// Trace
// ---------------------------------------------------------------------------

/// Ordinals for the ids of one history, by first use.
#[derive(Default)]
struct Ordinals {
    activities: HashMap<ActivityExecId, usize>,
    children: HashMap<ExecutionId, usize>,
}

impl Ordinals {
    fn activity(&mut self, id: ActivityExecId) -> String {
        let next = self.activities.len() + 1;
        format!("a{}", *self.activities.entry(id).or_insert(next))
    }

    fn child(&mut self, id: ExecutionId) -> String {
        let next = self.children.len() + 1;
        format!("c{}", *self.children.entry(id).or_insert(next))
    }
}

/// One trace line per event: the task, the virtual time and the event.
fn render_trace(events: &[WorkflowEvent], tasks: &[usize]) -> Vec<String> {
    let mut ids = Ordinals::default();
    events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let task = tasks.get(index).copied().unwrap_or(0);
            let clock = fired_timer_duration_secs(&events[..=index]);
            format!("{task:04} +{clock}s {}", render_event(event, &mut ids))
        })
        .collect()
}

/// A digest of each event with its payloads.
///
/// The trace holds no payload, so these digests catch a value that changes
/// on the same path. The harness and the context mint random UUIDs, such as
/// child ids and the successor of a continue-as-new. The body can pass such
/// an id on, for example in an activity input.
///
/// So each `*_id` field of an event that holds a UUID names a generated id.
/// Each JSON string equal to that id becomes its ordinal by first use. A
/// stable id, such as a timer id, is not a UUID and stays as it is. A string
/// that only contains an id also stays, so no other payload text changes.
fn payload_digests(events: &[WorkflowEvent]) -> Vec<u64> {
    let mut values: Vec<Value> = events
        .iter()
        .map(|event| serde_json::to_value(event).unwrap_or(Value::Null))
        .collect();
    let mut generated: Vec<String> = Vec::new();
    for value in &values {
        if let Some(Value::Object(fields)) = value.get("data") {
            for (key, field) in fields {
                if let (true, Value::String(id)) = (key.ends_with("_id"), field)
                    && uuid::Uuid::parse_str(id).is_ok()
                    && !generated.contains(id)
                {
                    generated.push(id.clone());
                }
            }
        }
    }
    values
        .iter_mut()
        .map(|value| {
            normalize_ids(value, &generated);
            fnv1a(value.to_string().as_bytes())
        })
        .collect()
}

/// Replace each string equal to a generated id with its ordinal.
fn normalize_ids(value: &mut Value, generated: &[String]) {
    match value {
        Value::String(text) => {
            if let Some(index) = generated.iter().position(|id| id == text) {
                *text = format!("<generated-id-{}>", index + 1);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_ids(item, generated);
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                normalize_ids(field, generated);
            }
        }
        _ => {}
    }
}

fn render_event(event: &WorkflowEvent, ids: &mut Ordinals) -> String {
    let kind = event.type_name();
    match event {
        WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        }
        | WorkflowEvent::LocalActivityScheduled {
            activity_id, name, ..
        } => format!("{kind} {} {name}", ids.activity(*activity_id)),
        WorkflowEvent::ActivityFailed {
            activity_id,
            attempt,
            ..
        } => format!("{kind} {} attempt {attempt}", ids.activity(*activity_id)),
        WorkflowEvent::ActivityStarted { activity_id, .. }
        | WorkflowEvent::ActivityCompleted { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. }
        | WorkflowEvent::LocalActivityCompleted { activity_id, .. }
        | WorkflowEvent::LocalActivityFailed { activity_id, .. }
        | WorkflowEvent::LocalActivityExhausted { activity_id, .. } => {
            format!("{kind} {}", ids.activity(*activity_id))
        }
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name,
            ..
        } => format!("{kind} {} {workflow_name}", ids.child(*child_id)),
        WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
        | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
            format!("{kind} {}", ids.child(*child_id))
        }
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } => format!("{kind} {} {duration_secs}s", timer_id.as_str()),
        WorkflowEvent::TimerFired { timer_id } | WorkflowEvent::TimerCancelled { timer_id } => {
            format!("{kind} {}", timer_id.as_str())
        }
        WorkflowEvent::SignalReceived { signal_name, .. } => format!("{kind} {signal_name}"),
        WorkflowEvent::MarkerRecorded { name, .. } => format!("{kind} {name}"),
        WorkflowEvent::SideEffectRecorded {
            kind: effect,
            name: Some(name),
            ..
        } => format!("{kind} {effect:?} {name}"),
        WorkflowEvent::SideEffectRecorded { kind: effect, .. } => format!("{kind} {effect:?}"),
        _ => kind.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn timer(id: &str) -> TimerId {
        TimerId::new(id)
    }

    fn names(events: &[WorkflowEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                WorkflowEvent::TimerFired { timer_id } => format!("fire {}", timer_id.as_str()),
                WorkflowEvent::SignalReceived { signal_name, .. } => {
                    format!("signal {signal_name}")
                }
                other => other.type_name().to_string(),
            })
            .collect()
    }

    #[test]
    fn the_payload_digest_normalizes_generated_uuids_only() {
        let events = |payload: Value| {
            vec![
                WorkflowEvent::TimerFired {
                    timer_id: timer("a"),
                },
                WorkflowEvent::SignalReceived {
                    signal_name: "go".to_string(),
                    payload,
                },
            ]
        };
        // A timer id is stable, so payload text that matches it still counts.
        // A substring rewrite made "a" equal to the ordinal "id1".
        assert_ne!(
            payload_digests(&events(json!("a"))),
            payload_digests(&events(json!("id1")))
        );
        assert_ne!(
            payload_digests(&events(json!("abc"))),
            payload_digests(&events(json!("xbc")))
        );

        // A generated UUID becomes its ordinal wherever a value equals it.
        let child = |id: &str| WorkflowEvent::ChildWorkflowStarted {
            child_id: id.parse().unwrap(),
            workflow_name: "audit".to_string(),
            input: json!({"nested": [id]}),
        };
        let first = "0190b6a0-0000-7000-8000-000000000001";
        let second = "0190b6a0-0000-7000-8000-000000000002";
        assert_eq!(
            payload_digests(&[child(first)]),
            payload_digests(&[child(second)])
        );
    }

    #[test]
    fn a_signal_received_before_a_deadline_goes_first() {
        let order = wake_order(60, &[(timer("t"), 60)], &[("go".to_string(), json!(1), 59)]);
        assert_eq!(names(&order), ["signal go", "fire t"]);
    }

    #[test]
    fn a_tie_goes_to_the_timer() {
        let order = wake_order(60, &[(timer("t"), 60)], &[("go".to_string(), json!(1), 60)]);
        assert_eq!(names(&order), ["fire t", "signal go"]);
    }

    #[test]
    fn due_timers_fire_by_deadline_then_id_and_later_ones_wait() {
        let order = wake_order(
            30,
            &[
                (timer("b"), 30),
                (timer("late"), 31),
                (timer("a"), 30),
                (timer("z"), 5),
            ],
            &[],
        );
        assert_eq!(names(&order), ["fire z", "fire a", "fire b"]);
    }

    #[test]
    fn signals_keep_their_arrival_order() {
        let order = wake_order(
            0,
            &[],
            &[
                ("first".to_string(), json!(1), 0),
                ("second".to_string(), json!(2), 0),
            ],
        );
        assert_eq!(names(&order), ["signal first", "signal second"]);
    }

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "a".into(),
            input: Value::Null,
            queue: "q".into(),
        }
    }

    fn completed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        }
    }

    #[test]
    fn a_result_before_its_schedule_is_illegal() {
        let id = ActivityExecId::new();
        assert!(check_legal(&[started(), scheduled(id), completed(id)]).is_ok());
        let error = check_legal(&[started(), completed(id), scheduled(id)]).unwrap_err();
        assert!(error.contains("before it was scheduled"), "{error}");
    }

    #[test]
    fn a_second_terminal_is_illegal() {
        let id = ActivityExecId::new();
        let error =
            check_legal(&[started(), scheduled(id), completed(id), completed(id)]).unwrap_err();
        assert!(error.contains("second terminal"), "{error}");
    }

    #[test]
    fn a_fire_needs_an_active_timer() {
        let start = |id: &str, secs| WorkflowEvent::TimerStarted {
            timer_id: timer(id),
            duration_secs: secs,
        };
        let fire = |id: &str| WorkflowEvent::TimerFired {
            timer_id: timer(id),
        };
        assert!(check_legal(&[started(), start("a", 10), fire("a")]).is_ok());
        let error = check_legal(&[started(), fire("a")]).unwrap_err();
        assert!(error.contains("no active TimerStarted"), "{error}");
        let twice = [started(), start("a", 10), fire("a"), fire("a")];
        assert!(check_legal(&twice).is_err());
    }

    #[test]
    fn nothing_follows_a_cancel() {
        let id = ActivityExecId::new();
        let cancel = WorkflowEvent::WorkflowCancelled {
            reason: "stop".into(),
        };
        assert!(check_legal(&[started(), scheduled(id), cancel.clone()]).is_ok());
        let error = check_legal(&[started(), scheduled(id), cancel, completed(id)]).unwrap_err();
        assert!(error.contains("after the cancel"), "{error}");
    }

    #[test]
    fn the_repro_command_is_one_line_and_names_the_seed_test_and_args() {
        let command = repro_command(42, Some("tests::race"), "--features testing");
        assert!(
            command.starts_with("HARVEST_SIM_SEED=42 cargo test"),
            "{command}"
        );
        assert!(command.contains(" --features testing "), "{command}");
        assert!(
            command.ends_with("tests::race -- --exact --nocapture"),
            "{command}"
        );
        assert!(!command.contains('\n'), "{command}");
        let bare = repro_command(7, None, "");
        assert!(bare.starts_with("HARVEST_SIM_SEED=7 cargo test"), "{bare}");
        assert!(bare.ends_with(" -- --nocapture"), "{bare}");
    }

    #[test]
    fn the_unit_test_binary_maps_to_lib() {
        // This test runs in the library's unit-test binary.
        assert_eq!(
            test_target(Some("autumn-harvest")).as_deref(),
            Some("--lib")
        );
    }

    #[test]
    fn the_macros_do_nothing_outside_a_run() {
        assert!(!crate::always!(false, "outside"));
        assert!(crate::sometimes!(true, "outside"));
    }

    #[test]
    fn a_label_is_evaluated_only_when_a_run_records_it() {
        let calls = std::cell::Cell::new(0);
        let label = || {
            calls.set(calls.get() + 1);
            "counted"
        };
        crate::always!(false, label());
        crate::sometimes!(true, label());
        assert_eq!(calls.get(), 0, "no run records outside a probe scope");

        let probe = Arc::new(Mutex::new(Probe::default()));
        PROBE.sync_scope(probe, || {
            crate::always!(true, label());
            assert_eq!(calls.get(), 0, "a true always! records nothing");
            crate::always!(false, label());
            crate::sometimes!(false, label());
        });
        assert_eq!(calls.get(), 2);

        // The label borrows its value, so a `String` label stays usable.
        let owned = String::from("owned");
        crate::always!(true, owned);
        assert_eq!(owned, "owned");
    }

    #[test]
    fn the_macros_record_inside_a_probe_scope() {
        let probe = Arc::new(Mutex::new(Probe::default()));
        PROBE.sync_scope(probe.clone(), || {
            crate::always!(false, "never");
            crate::always!(false, "never");
            crate::sometimes!(false, "rare");
            crate::sometimes!(true, "common");
        });
        let state = lock(&probe);
        assert_eq!(state.violations.len(), 1, "one message per label");
        assert_eq!(state.sometimes.get("rare"), Some(&false));
        assert_eq!(state.sometimes.get("common"), Some(&true));
    }

    #[test]
    fn a_seeded_mock_context_depends_on_seed_name_call_and_attempt() {
        let now = Utc::now();
        let draw = |name, call, attempt, seed| {
            MockCall::new(name, call, attempt, now, seed)
                .context()
                .rng()
                .next_u64()
        };
        let base = draw("a", 1, 1, 7);
        assert_eq!(base, draw("a", 1, 1, 7));
        assert_ne!(base, draw("b", 1, 1, 7));
        assert_ne!(base, draw("a", 2, 1, 7));
        assert_ne!(base, draw("a", 1, 2, 7));
        assert_ne!(base, draw("a", 1, 1, 8));
    }

    #[test]
    fn the_trace_replaces_ids_with_ordinals() {
        let a = ActivityExecId::new();
        let b = ActivityExecId::new();
        let events = [
            started(),
            scheduled(a),
            scheduled(b),
            completed(b),
            completed(a),
        ];
        let trace = render_trace(&events, &[0, 1, 1, 2, 3]);
        assert_eq!(
            trace,
            [
                "0000 +0s WorkflowStarted",
                "0001 +0s ActivityScheduled a1 a",
                "0001 +0s ActivityScheduled a2 a",
                "0002 +0s ActivityCompleted a2",
                "0003 +0s ActivityCompleted a1",
            ]
        );
    }
}
