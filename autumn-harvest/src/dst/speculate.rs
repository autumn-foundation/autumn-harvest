//! Speculative decisions over an in-flight commit (issue #2011).
//!
//! This module is an R&D spike. It models the engine, and the engine does
//! not use it. A worker runs the next decision on resident state while the
//! previous commit flushes. The worker discards that decision when the
//! commit or the claim fence fails.
//!
//! The model is a discrete-event simulation with no database. The clock
//! counts virtual microseconds. A seed fixes every duration and every fault,
//! so a failing seed replays exactly.
//!
//! [`Mode::Serial`] is the engine today. [`Mode::Gated`] speculates and
//! releases effects only after their commit lands. [`Mode::Eager`] releases
//! effects before the commit, as libDSE does. [`Fence::PrefixOnly`] is the
//! check of the suspended commit path today. [`Logging::ReadsOnly`] is the
//! asymmetric logging of Halfmoon.
//!
//! See `docs/rnd/speculative-execution-spike.md`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt;

use super::rng::SplitMix64;
use super::sweep::{Nondeterminism, SEED_VAR, SeedPlan, TAIL_LINES, diverged};

/// The speculation mode: `serial`, `gated` or `eager`.
pub const MODE_VAR: &str = "HARVEST_DST_SPEC_MODE";
/// The commit fence: `epoch` or `prefix-only`.
pub const FENCE_VAR: &str = "HARVEST_DST_SPEC_FENCE";
/// The log content: `full` or `reads-only`.
pub const LOGGING_VAR: &str = "HARVEST_DST_SPEC_LOGGING";
/// The planted defect: `none` or `keep-on-failure`.
pub const PLANT_VAR: &str = "HARVEST_DST_SPEC_PLANT";
/// The workload: `chain` or `fan-out`.
pub const WORKLOAD_VAR: &str = "HARVEST_DST_SPEC_WORKLOAD";
/// A comma list of invariant names. The default is all of them.
pub const CHECKS_VAR: &str = "HARVEST_DST_SPEC_CHECKS";

/// The most decisions that one execution can have in its commit chain.
pub const MAX_CHAIN: usize = 4;

/// The most events that one run can process.
pub const MAX_EVENTS: usize = 50_000;

/// An enum with a fixed list of names.
macro_rules! named {
    (
        $(#[$meta:meta])*
        $name:ident {
            $($(#[$vmeta:meta])* $variant:ident => $text:literal,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: [Self; [$(named!(@unit $variant)),+].len()] = [$(Self::$variant),+];

            /// The name that `parse` accepts.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            /// Parse a name from `as_str`.
            ///
            /// # Errors
            ///
            /// Returns a message that names the accepted values.
            pub fn parse(name: &str) -> Result<Self, String> {
                Self::ALL
                    .into_iter()
                    .find(|value| value.as_str() == name)
                    .ok_or_else(|| {
                        let names: Vec<&str> = Self::ALL.iter().map(|v| v.as_str()).collect();
                        format!(
                            "unknown {} {name:?}: use {}",
                            stringify!($name),
                            names.join(", ")
                        )
                    })
            }
        }
    };
    (@unit $variant:ident) => { () };
}

named! {
    /// When a worker decides and when it releases effects.
    Mode {
        /// Decide, commit, wait for the commit, then decide again. This is
        /// the engine today.
        Serial => "serial",
        /// Decide again while a commit flushes. Release the effects of a
        /// decision only after its commit lands.
        Gated => "gated",
        /// As `Gated`, but release the effects of a decision when it is
        /// made, before its commit.
        Eager => "eager",
    }
}

named! {
    /// The check that a commit must pass.
    Fence {
        /// The decision prefix must not have moved, and the committer must
        /// hold the current claim.
        Epoch => "epoch",
        /// Only the prefix check. The suspended commit path has only this
        /// check today, through the event id uniqueness of the history.
        PrefixOnly => "prefix-only",
    }
}

named! {
    /// What a decision record stores.
    Logging {
        /// The inputs that the decision read and the effects it scheduled.
        Full => "full",
        /// Only the inputs that it read. Replay derives the effects.
        ReadsOnly => "reads-only",
    }
}

named! {
    /// A defect that a run plants on purpose.
    Plant {
        /// No defect.
        None => "none",
        /// After a failed commit, the worker keeps its resident state. It
        /// drops the chain but does not reload from the durable log.
        KeepOnFailure => "keep-on-failure",
    }
}

named! {
    /// The shape of each execution.
    Workload {
        /// Three activities in sequence, as in the e2e bench workflow.
        Chain => "chain",
        /// Two rounds of four parallel activities, and two signals.
        FanOut => "fan-out",
    }
}

named! {
    /// A safety or liveness property of a run.
    SpecInvariant {
        /// An applied commit comes from the current claim holder.
        CommitByOwner => "CommitByOwner",
        /// Replay of the durable log gives each decision that it holds.
        ReplayEquivalent => "ReplayEquivalent",
        /// An effect leaves the worker only after the commit that schedules
        /// it.
        EffectAfterCommit => "EffectAfterCommit",
        /// An effect runs at most once.
        EffectOnce => "EffectOnce",
        /// A completed execution returns the value that its workload
        /// defines.
        ExpectedOutput => "ExpectedOutput",
        /// Every execution completes within [`MAX_EVENTS`] events.
        Converges => "Converges",
    }
}

impl SpecInvariant {
    /// The name of the invariant. It is equal to [`SpecInvariant::as_str`].
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.as_str()
    }
}

impl Workload {
    /// The number of rounds of one execution.
    const fn rounds(self) -> u32 {
        match self {
            Self::Chain => 3,
            Self::FanOut => 2,
        }
    }

    /// The activities of one round.
    const fn fanout(self) -> u32 {
        match self {
            Self::Chain => 1,
            Self::FanOut => 4,
        }
    }

    /// The signals that one execution gets.
    const fn signals(self) -> u32 {
        match self {
            Self::Chain => 0,
            Self::FanOut => 2,
        }
    }
}

/// The durations of the model, in microseconds.
///
/// A pair is an inclusive range. The seed draws each value from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// One decision on resident state.
    pub decide_us: u64,
    /// One commit, from send to the outcome at the worker.
    pub commit_us: (u64, u64),
    /// From a released effect to the start of its activity.
    pub dispatch_us: u64,
    /// One activity body.
    pub activity_us: (u64, u64),
    /// From a durable input to the start of the next decision.
    pub wake_us: u64,
}

impl Timing {
    /// The durations that `docs/rnd/speculative-execution-spike.md`
    /// calibrates from the e2e bench.
    pub const BENCH: Self = Self {
        decide_us: 200,
        commit_us: (1_500, 4_000),
        dispatch_us: 16_000,
        activity_us: (50, 150),
        wake_us: 60_000,
    };

    /// Durations with no spread.
    #[must_use]
    pub const fn fixed(
        decide_us: u64,
        commit_us: u64,
        dispatch_us: u64,
        activity_us: u64,
        wake_us: u64,
    ) -> Self {
        Self {
            decide_us,
            commit_us: (commit_us, commit_us),
            dispatch_us,
            activity_us: (activity_us, activity_us),
            wake_us,
        }
    }

    /// The longest time from one decision to the next one of a chain.
    #[must_use]
    pub const fn hop_us(&self) -> u64 {
        self.decide_us + self.commit_us.1 + self.dispatch_us + self.activity_us.1 + self.wake_us
    }
}

/// The faults of a run. Each commit send draws them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Faults {
    /// The chance in percent that the worker crashes while the commit is in
    /// flight.
    pub crash_pct: u64,
    /// The chance in percent that the worker stalls just before the send.
    pub stall_pct: u64,
    /// The chance in percent that the commit fails and applies nothing.
    pub commit_fail_pct: u64,
    /// The most crashes and stalls of one run.
    pub max_faults: u32,
}

impl Faults {
    /// No faults.
    pub const NONE: Self = Self {
        crash_pct: 0,
        stall_pct: 0,
        commit_fail_pct: 0,
        max_faults: 0,
    };

    /// The faults of a default run.
    pub const DEFAULT: Self = Self {
        crash_pct: 5,
        stall_pct: 5,
        commit_fail_pct: 5,
        max_faults: 3,
    };
}

/// The parameters of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecConfig {
    /// The seed. Equal configs give equal runs.
    pub seed: u64,
    /// When a worker decides and releases effects.
    pub mode: Mode,
    /// The commit check.
    pub fence: Fence,
    /// What a decision record stores.
    pub logging: Logging,
    /// The planted defect.
    pub plant: Plant,
    /// The shape of each execution.
    pub workload: Workload,
    /// The durations.
    pub timing: Timing,
    /// The faults.
    pub faults: Faults,
    /// The number of workers.
    pub workers: usize,
    /// The number of executions.
    pub executions: usize,
    /// A worker that is silent this long loses its claims.
    pub lease_us: u64,
    /// The invariants to check.
    pub checks: Vec<SpecInvariant>,
}

impl SpecConfig {
    /// The default config for `seed`: the engine today, under faults.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            mode: Mode::Serial,
            fence: Fence::Epoch,
            logging: Logging::Full,
            plant: Plant::None,
            workload: Workload::Chain,
            timing: Timing::BENCH,
            faults: Faults::DEFAULT,
            workers: 3,
            executions: 4,
            lease_us: 2 * Timing::BENCH.hop_us(),
            checks: SpecInvariant::ALL.to_vec(),
        }
    }

    /// This config with `mode`.
    #[must_use]
    pub const fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    /// This config with `fence`.
    #[must_use]
    pub const fn with_fence(mut self, fence: Fence) -> Self {
        self.fence = fence;
        self
    }

    /// This config with `logging`.
    #[must_use]
    pub const fn with_logging(mut self, logging: Logging) -> Self {
        self.logging = logging;
        self
    }

    /// This config with `plant`.
    #[must_use]
    pub const fn with_plant(mut self, plant: Plant) -> Self {
        self.plant = plant;
        self
    }

    /// This config with `workload`.
    #[must_use]
    pub const fn with_workload(mut self, workload: Workload) -> Self {
        self.workload = workload;
        self
    }

    /// This config with `timing`. The lease follows the new durations.
    #[must_use]
    pub const fn with_timing(mut self, timing: Timing) -> Self {
        self.timing = timing;
        self.lease_us = 2 * timing.hop_us();
        self
    }

    /// This config with `faults`.
    #[must_use]
    pub const fn with_faults(mut self, faults: Faults) -> Self {
        self.faults = faults;
        self
    }

    /// This config, checking only `checks`.
    #[must_use]
    pub fn checking(mut self, checks: &[SpecInvariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }
}

/// A broken invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecViolation {
    /// The invariant.
    pub invariant: SpecInvariant,
    /// The virtual time of the break.
    pub at_us: u64,
    /// What happened.
    pub detail: String,
}

impl fmt::Display for SpecViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} broken at t={}: {}",
            self.invariant.name(),
            self.at_us,
            self.detail
        )
    }
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpecStats {
    /// Decisions made.
    pub decisions: u64,
    /// Decisions that started while a commit of the same execution was in
    /// flight.
    pub speculative: u64,
    /// Commits that applied.
    pub commits: u64,
    /// Decisions that a failed commit or a lost claim discarded.
    pub discarded: u64,
    /// Commits that the claim fence rejected.
    pub fenced: u64,
    /// Commits that the prefix check rejected.
    pub prefix_lost: u64,
    /// Commits that failed and applied nothing.
    pub failed_commits: u64,
    /// Commits that applied from a worker without the current claim.
    pub stale_commits: u64,
    /// Injected crashes.
    pub crashes: u64,
    /// Injected stalls.
    pub stalls: u64,
    /// In-flight commits that a crash lost.
    pub lost_in_crash: u64,
    /// In-flight commits that applied after their worker crashed.
    pub landed_in_crash: u64,
    /// Claims that the reclaimer moved.
    pub reclaims: u64,
    /// Resident states rebuilt from the durable log.
    pub cold_loads: u64,
    /// Effect runs.
    pub effects: u64,
    /// Effect results that the store rejected: the schedule was not
    /// durable.
    pub orphan_results: u64,
    /// Effect results that the store ignored: a result was already durable.
    pub duplicate_results: u64,
    /// Rows of the durable logs: inputs, decisions and stored writes.
    pub log_rows: u64,
    /// Rows that hold a scheduled effect.
    pub write_rows: u64,
    /// Executions that completed.
    pub completed: u64,
}

impl SpecStats {
    /// Add the counters of `other`.
    pub fn merge(&mut self, other: &Self) {
        self.decisions += other.decisions;
        self.speculative += other.speculative;
        self.commits += other.commits;
        self.discarded += other.discarded;
        self.fenced += other.fenced;
        self.prefix_lost += other.prefix_lost;
        self.failed_commits += other.failed_commits;
        self.stale_commits += other.stale_commits;
        self.crashes += other.crashes;
        self.stalls += other.stalls;
        self.lost_in_crash += other.lost_in_crash;
        self.landed_in_crash += other.landed_in_crash;
        self.reclaims += other.reclaims;
        self.cold_loads += other.cold_loads;
        self.effects += other.effects;
        self.orphan_results += other.orphan_results;
        self.duplicate_results += other.duplicate_results;
        self.log_rows += other.log_rows;
        self.write_rows += other.write_rows;
        self.completed += other.completed;
    }
}

/// The result of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecReport {
    /// The config of the run.
    pub config: SpecConfig,
    /// One line for each event that did something.
    pub trace: Vec<String>,
    /// The first broken invariant, if any.
    pub violation: Option<SpecViolation>,
    /// Coverage counters.
    pub stats: SpecStats,
    /// The latency of each completed execution, from its start to the commit
    /// that completes it. The order is the order of completion.
    pub latencies_us: Vec<u64>,
}

impl SpecReport {
    /// The mean latency of the completed executions.
    #[must_use]
    pub fn mean_latency_us(&self) -> Option<u64> {
        let count = u64::try_from(self.latencies_us.len()).ok()?;
        (count > 0).then(|| self.latencies_us.iter().sum::<u64>() / count)
    }

    /// The last `lines` trace lines, joined by newlines.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// An effect: one activity run that a decision schedules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Effect {
    exec: usize,
    round: u32,
    index: u32,
    input: u64,
}

impl Effect {
    /// The identity of the run. A second run of one key runs it twice.
    const fn key(self) -> (usize, u32, u32) {
        (self.exec, self.round, self.index)
    }

    /// The result of the activity. It depends only on the effect.
    const fn value(self) -> u64 {
        let mut rng = SplitMix64::new(
            (self.exec as u64) << 40 ^ (self.round as u64) << 20 ^ self.index as u64 ^ self.input,
        );
        rng.next_u64() >> 16
    }
}

impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}/r{}.{}", self.exec, self.round, self.index)
    }
}

/// A durable input of an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Input {
    Result { effect: Effect, value: u64 },
    Signal { value: u64 },
}

/// The deterministic state of one execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct State {
    started: bool,
    round: u32,
    pending: BTreeSet<(u32, u32)>,
    acc: u64,
    signals: u64,
    seen: usize,
    done: Option<u64>,
}

/// What one decision did.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Decision {
    /// The number of durable decisions that this one follows.
    base: usize,
    /// The number of inputs that it has read, its own included.
    seen: usize,
    writes: Vec<Effect>,
    complete: Option<u64>,
}

/// Apply `inputs` to `state`, then schedule the next round or complete.
///
/// This is the workflow code. It is deterministic.
fn decide(
    workload: Workload,
    exec: usize,
    state: &State,
    inputs: &[Input],
) -> (State, Vec<Effect>, Option<u64>) {
    let mut next = state.clone();
    next.started = true;
    for input in inputs {
        match *input {
            Input::Result { effect, value } => {
                if next.pending.remove(&(effect.round, effect.index)) {
                    next.acc = next.acc.wrapping_add(value);
                }
            }
            Input::Signal { value } => next.signals = next.signals.wrapping_add(value),
        }
    }
    next.seen += inputs.len();
    let mut writes = Vec::new();
    let mut complete = None;
    if next.done.is_none() && next.pending.is_empty() {
        if next.round < workload.rounds() {
            next.round += 1;
            for index in 0..workload.fanout() {
                let effect = Effect {
                    exec,
                    round: next.round,
                    index,
                    input: next.acc,
                };
                next.pending.insert((next.round, index));
                writes.push(effect);
            }
        } else {
            next.done = Some(next.acc);
            complete = next.done;
        }
    }
    (next, writes, complete)
}

/// The output that `workload` defines for `exec`.
fn expected_output(workload: Workload, exec: usize) -> u64 {
    let mut acc = 0_u64;
    for round in 1..=workload.rounds() {
        let input = acc;
        for index in 0..workload.fanout() {
            let effect = Effect {
                exec,
                round,
                index,
                input,
            };
            acc = acc.wrapping_add(effect.value());
        }
    }
    acc
}

/// A worker identity: its index and its incarnation.
type Holder = (usize, u32);

/// One execution in the store.
#[derive(Debug, Clone)]
struct Exec {
    owner: Holder,
    epoch: u32,
    inputs: Vec<Input>,
    decisions: usize,
    /// The state after replay of every durable decision.
    replayed: State,
    scheduled: BTreeSet<Effect>,
    results: BTreeSet<(u32, u32)>,
    started_at: u64,
    done: bool,
}

/// The resident state of one execution on one worker.
#[derive(Debug, Clone)]
struct Resident {
    /// Bumped on each reload, so a stale event finds a mismatch.
    generation: u64,
    epoch: u32,
    state: State,
    /// The durable decisions that the chain head follows.
    base: usize,
    chain: VecDeque<Decision>,
    deciding: bool,
}

#[derive(Debug, Clone)]
struct WorkerState {
    incarnation: u32,
    alive: bool,
    stalled_until: u64,
    stalled_since: u64,
    resident: BTreeMap<usize, Resident>,
}

/// One scheduled event.
#[derive(Debug, Clone)]
enum Event {
    /// An execution starts on its first owner.
    Start { exec: usize },
    /// A new durable input wakes the owner.
    Arrive { exec: usize },
    /// The owner takes a moved claim and loads cold.
    Adopt { worker: Holder, exec: usize },
    /// A decision finishes. `upto` is the input count at its start.
    Decided {
        worker: Holder,
        exec: usize,
        generation: u64,
        upto: usize,
    },
    /// The worker sends the head of the chain after a stall.
    Send {
        worker: Holder,
        exec: usize,
        generation: u64,
    },
    /// A commit reaches the store.
    Commit {
        worker: Holder,
        exec: usize,
        generation: u64,
        epoch: u32,
        decision: Decision,
        fail: bool,
        id: u64,
    },
    /// An activity starts.
    EffectStart { effect: Effect },
    /// An activity finishes.
    EffectDone { effect: Effect },
    /// A client sends a signal.
    Signal { exec: usize, value: u64 },
    /// A worker crashes.
    Crash { worker: usize },
    /// A crashed worker restarts with a new incarnation.
    Restart { worker: usize },
    /// The reclaimer scans for claims of dead or stalled workers.
    Scan,
}

#[derive(Debug, Clone)]
struct Queued {
    at: u64,
    seq: u64,
    event: Event,
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for Queued {}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Queued {
    /// The earliest event first. The heap is a max-heap, so this is
    /// reversed.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

/// The outcome of a commit at the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitOutcome {
    Applied,
    Failed,
    Fenced,
    PrefixLost,
}

impl CommitOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Fenced => "fenced",
            Self::PrefixLost => "prefix-lost",
        }
    }
}

struct Model<'a> {
    config: &'a SpecConfig,
    rng: SplitMix64,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Queued>,
    execs: Vec<Exec>,
    workers: Vec<WorkerState>,
    /// The runs of each effect key.
    ledger: BTreeMap<(usize, u32, u32), u32>,
    /// Commits that a crash lost.
    lost: BTreeSet<u64>,
    /// Commits in flight, by worker.
    in_flight: BTreeMap<u64, Holder>,
    next_commit: u64,
    next_generation: u64,
    faults: u32,
    trace: Vec<String>,
    violation: Option<SpecViolation>,
    stats: SpecStats,
    latencies: Vec<u64>,
}

impl<'a> Model<'a> {
    fn new(config: &'a SpecConfig) -> Self {
        let workers = (0..config.workers)
            .map(|_| WorkerState {
                incarnation: 0,
                alive: true,
                stalled_until: 0,
                stalled_since: 0,
                resident: BTreeMap::new(),
            })
            .collect();
        let mut model = Self {
            config,
            rng: SplitMix64::new(config.seed),
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            execs: Vec::new(),
            workers,
            ledger: BTreeMap::new(),
            lost: BTreeSet::new(),
            in_flight: BTreeMap::new(),
            next_commit: 0,
            next_generation: 0,
            faults: 0,
            trace: Vec::new(),
            violation: None,
            stats: SpecStats::default(),
            latencies: Vec::new(),
        };
        let hop = config.timing.hop_us();
        let workers = u64::try_from(config.workers).unwrap_or(1);
        for exec in 0..config.executions {
            let started_at = model.rng.below(hop);
            let owner = usize::try_from(model.rng.below(workers)).unwrap_or(0);
            model.execs.push(Exec {
                owner: (owner, 0),
                epoch: 1,
                inputs: Vec::new(),
                decisions: 0,
                replayed: State::default(),
                scheduled: BTreeSet::new(),
                results: BTreeSet::new(),
                started_at,
                done: false,
            });
            model.push(started_at, Event::Start { exec });
            for _ in 0..config.workload.signals() {
                let at = started_at + model.rng.below(4 * hop);
                let value = model.rng.next_u64() >> 16;
                model.push(at, Event::Signal { exec, value });
            }
        }
        model.push(config.lease_us / 2, Event::Scan);
        model
    }

    fn push(&mut self, at: u64, event: Event) {
        self.seq += 1;
        self.queue.push(Queued {
            at,
            seq: self.seq,
            event,
        });
    }

    fn log(&mut self, actor: &str, text: &str) {
        self.trace
            .push(format!("{:>9} {actor:<5} {text}", self.now));
    }

    fn checks(&self, invariant: SpecInvariant) -> bool {
        self.config.checks.contains(&invariant)
    }

    fn fail(&mut self, invariant: SpecInvariant, detail: String) {
        if self.violation.is_none() && self.checks(invariant) {
            self.log("check", &format!("{} broken: {detail}", invariant.name()));
            self.violation = Some(SpecViolation {
                invariant,
                at_us: self.now,
                detail,
            });
        }
    }

    fn draw(&mut self, (low, high): (u64, u64)) -> u64 {
        low + self.rng.below(high.saturating_sub(low) + 1)
    }

    fn run(mut self) -> SpecReport {
        let mut events = 0;
        while let Some(queued) = self.queue.pop() {
            if self.violation.is_some() || self.execs.iter().all(|e| e.done) {
                break;
            }
            events += 1;
            if events > MAX_EVENTS {
                break;
            }
            self.now = queued.at;
            self.handle(queued.event);
        }
        if self.violation.is_none() {
            let stuck: Vec<String> = (0..self.execs.len())
                .filter(|&exec| !self.execs[exec].done)
                .map(|exec| format!("e{exec}"))
                .collect();
            if !stuck.is_empty() {
                let detail = format!("{} did not complete", stuck.join(", "));
                self.fail(SpecInvariant::Converges, detail);
            }
        }
        SpecReport {
            config: self.config.clone(),
            trace: self.trace,
            violation: self.violation,
            stats: self.stats,
            latencies_us: self.latencies,
        }
    }

    /// The worker that `holder` names, if that incarnation is alive.
    fn live(&self, holder: Holder) -> bool {
        let worker = &self.workers[holder.0];
        worker.alive && worker.incarnation == holder.1
    }

    /// The stall end of `holder`, if it is stalled now.
    fn stalled(&self, holder: Holder) -> Option<u64> {
        let until = self.workers[holder.0].stalled_until;
        (until > self.now).then_some(until)
    }

    fn label(holder: Holder) -> String {
        format!("w{}.{}", holder.0 + 1, holder.1)
    }

    fn handle(&mut self, event: Event) {
        // A stalled worker handles nothing. Its events wait for the stall end.
        let worker = match &event {
            Event::Adopt { worker, .. }
            | Event::Decided { worker, .. }
            | Event::Send { worker, .. }
            | Event::Commit { worker, .. } => Some(*worker),
            Event::Arrive { exec } | Event::Start { exec } => Some(self.execs[*exec].owner),
            _ => None,
        };
        if let Some(until) = worker.and_then(|holder| self.stalled(holder)) {
            self.push(until, event);
            return;
        }
        match event {
            Event::Start { exec } => {
                let owner = self.execs[exec].owner;
                self.adopt(owner, exec, false);
            }
            Event::Arrive { exec } => {
                let owner = self.execs[exec].owner;
                if self.live(owner) {
                    self.try_decide(owner, exec);
                }
            }
            Event::Adopt { worker, exec } => {
                if self.live(worker) && self.execs[exec].owner == worker {
                    self.adopt(worker, exec, true);
                }
            }
            Event::Decided {
                worker,
                exec,
                generation,
                upto,
            } => self.decided(worker, exec, generation, upto),
            Event::Send {
                worker,
                exec,
                generation,
            } => {
                if self.resident(worker, exec, generation).is_some() {
                    self.send(worker, exec, false);
                }
            }
            Event::Commit {
                worker,
                exec,
                generation,
                epoch,
                decision,
                fail,
                id,
            } => self.commit(worker, exec, generation, epoch, &decision, fail, id),
            Event::EffectStart { effect } => self.effect_start(effect),
            Event::EffectDone { effect } => self.effect_done(effect),
            Event::Signal { exec, value } => {
                if !self.execs[exec].done {
                    self.execs[exec].inputs.push(Input::Signal { value });
                    self.stats.log_rows += 1;
                    self.log("client", &format!("e{exec} signal"));
                    self.push(
                        self.now + self.config.timing.wake_us,
                        Event::Arrive { exec },
                    );
                }
            }
            Event::Crash { worker } => self.crash(worker),
            Event::Restart { worker } => {
                let state = &mut self.workers[worker];
                state.alive = true;
                state.incarnation += 1;
                let holder = (worker, state.incarnation);
                self.log(&Self::label(holder), "restart");
            }
            Event::Scan => self.scan(),
        }
    }

    /// The resident state of `exec` on `worker`, if `generation` is current.
    fn resident(&self, worker: Holder, exec: usize, generation: u64) -> Option<&Resident> {
        if !self.live(worker) {
            return None;
        }
        self.workers[worker.0]
            .resident
            .get(&exec)
            .filter(|resident| resident.generation == generation)
    }

    /// Build resident state for `exec` from the durable log, then decide.
    fn adopt(&mut self, worker: Holder, exec: usize, cold: bool) {
        self.next_generation += 1;
        let durable = &self.execs[exec];
        let resident = Resident {
            generation: self.next_generation,
            epoch: durable.epoch,
            state: durable.replayed.clone(),
            base: durable.decisions,
            chain: VecDeque::new(),
            deciding: false,
        };
        self.workers[worker.0].resident.insert(exec, resident);
        if cold {
            self.stats.cold_loads += 1;
            self.log(&Self::label(worker), &format!("e{exec} cold load"));
        }
        self.try_decide(worker, exec);
    }

    /// Start a decision if one is due and the mode allows it.
    fn try_decide(&mut self, worker: Holder, exec: usize) {
        let mode = self.config.mode;
        let inputs = self.execs[exec].inputs.len();
        let Some(resident) = self.workers[worker.0].resident.get_mut(&exec) else {
            return;
        };
        let due = !resident.state.started || inputs > resident.state.seen;
        let room = match mode {
            Mode::Serial => resident.chain.is_empty(),
            Mode::Gated | Mode::Eager => resident.chain.len() < MAX_CHAIN,
        };
        if resident.deciding || !due || !room || resident.state.done.is_some() {
            return;
        }
        resident.deciding = true;
        let speculative = !resident.chain.is_empty();
        let generation = resident.generation;
        self.stats.speculative += u64::from(speculative);
        let event = Event::Decided {
            worker,
            exec,
            generation,
            upto: inputs,
        };
        self.push(self.now + self.config.timing.decide_us, event);
    }

    fn decided(&mut self, worker: Holder, exec: usize, generation: u64, upto: usize) {
        if self.resident(worker, exec, generation).is_none() {
            return;
        }
        let workload = self.config.workload;
        let inputs = self.execs[exec].inputs.clone();
        let resident = self.workers[worker.0]
            .resident
            .get_mut(&exec)
            .expect("checked above");
        resident.deciding = false;
        let read = inputs.get(resident.state.seen..upto).unwrap_or_default();
        let (state, writes, complete) = decide(workload, exec, &resident.state, read);
        let decision = Decision {
            base: resident.base + resident.chain.len(),
            seen: state.seen,
            writes,
            complete,
        };
        resident.state = state;
        let speculative = !resident.chain.is_empty();
        resident.chain.push_back(decision.clone());
        self.stats.decisions += 1;
        let names: Vec<String> = decision.writes.iter().map(ToString::to_string).collect();
        self.log(
            &Self::label(worker),
            &format!(
                "e{exec} decide d{} seen {} [{}]{}{}",
                decision.base,
                decision.seen,
                names.join(" "),
                decision.complete.map_or(String::new(), |_| " done".into()),
                if speculative { " spec" } else { "" }
            ),
        );
        if self.config.mode == Mode::Eager {
            for effect in decision.writes.clone() {
                self.release(effect);
            }
        }
        if !speculative {
            self.send(worker, exec, true);
        }
        self.try_decide(worker, exec);
    }

    /// Send the head of the chain of `exec`. `faults` lets the send draw
    /// faults.
    fn send(&mut self, worker: Holder, exec: usize, faults: bool) {
        let room = self.faults < self.config.faults.max_faults;
        let draws = self.config.faults;
        let stall = faults && room && self.rng.chance(draws.stall_pct);
        let resident = self.workers[worker.0]
            .resident
            .get(&exec)
            .expect("the caller holds resident state");
        let generation = resident.generation;
        if stall {
            self.faults += 1;
            self.stats.stalls += 1;
            let lease = self.config.lease_us;
            let until = self.now + lease / 2 + self.rng.below(2 * lease);
            let state = &mut self.workers[worker.0];
            state.stalled_until = until;
            state.stalled_since = self.now;
            self.log(&Self::label(worker), &format!("stall until {until}"));
            self.push(
                until,
                Event::Send {
                    worker,
                    exec,
                    generation,
                },
            );
            return;
        }
        let Some(decision) = resident.chain.front().cloned() else {
            return;
        };
        let epoch = resident.epoch;
        let flight = self.draw(self.config.timing.commit_us);
        let fail = self.rng.chance(draws.commit_fail_pct);
        let crash = faults && room && self.rng.chance(draws.crash_pct);
        self.next_commit += 1;
        let id = self.next_commit;
        self.in_flight.insert(id, worker);
        if crash {
            self.faults += 1;
            let at = self.now + self.rng.below(flight);
            self.push(at, Event::Crash { worker: worker.0 });
        }
        let event = Event::Commit {
            worker,
            exec,
            generation,
            epoch,
            decision,
            fail,
            id,
        };
        self.push(self.now + flight, event);
    }

    #[allow(clippy::too_many_arguments, reason = "the fields of one commit event")]
    fn commit(
        &mut self,
        worker: Holder,
        exec: usize,
        generation: u64,
        epoch: u32,
        decision: &Decision,
        fail: bool,
        id: u64,
    ) {
        self.in_flight.remove(&id);
        if self.lost.remove(&id) {
            return;
        }
        let outcome = self.apply(worker, exec, epoch, decision, fail);
        self.log(
            &Self::label(worker),
            &format!("e{exec} commit d{} {}", decision.base, outcome.as_str()),
        );
        if self.resident(worker, exec, generation).is_none() {
            return;
        }
        match outcome {
            CommitOutcome::Applied => {
                let resident = self.workers[worker.0]
                    .resident
                    .get_mut(&exec)
                    .expect("checked above");
                resident.chain.pop_front();
                resident.base += 1;
                if resident.chain.is_empty() {
                    self.try_decide(worker, exec);
                } else {
                    self.send(worker, exec, true);
                }
            }
            CommitOutcome::Failed | CommitOutcome::Fenced | CommitOutcome::PrefixLost => {
                self.repair(worker, exec);
            }
        }
    }

    /// Apply a commit at the store.
    fn apply(
        &mut self,
        worker: Holder,
        exec: usize,
        epoch: u32,
        decision: &Decision,
        fail: bool,
    ) -> CommitOutcome {
        let durable = &self.execs[exec];
        let owner = durable.owner == worker && durable.epoch == epoch;
        if fail {
            self.stats.failed_commits += 1;
            return CommitOutcome::Failed;
        }
        if self.config.fence == Fence::Epoch && !owner {
            self.stats.fenced += 1;
            return CommitOutcome::Fenced;
        }
        if decision.base != durable.decisions {
            self.stats.prefix_lost += 1;
            return CommitOutcome::PrefixLost;
        }
        if !owner {
            self.stats.stale_commits += 1;
            let detail = format!(
                "e{exec} d{} from {} applied, but {} holds the claim",
                decision.base,
                Self::label(worker),
                Self::label(durable.owner)
            );
            self.fail(SpecInvariant::CommitByOwner, detail);
        }
        self.record(exec, decision);
        CommitOutcome::Applied
    }

    /// Append an applied decision to the durable log of `exec`.
    fn record(&mut self, exec: usize, decision: &Decision) {
        let workload = self.config.workload;
        let durable = &self.execs[exec];
        let read = durable
            .inputs
            .get(durable.replayed.seen..decision.seen)
            .unwrap_or_default();
        let (state, writes, complete) = decide(workload, exec, &durable.replayed, read);
        let replayed = Decision {
            base: decision.base,
            seen: state.seen,
            writes,
            complete,
        };
        if replayed != *decision {
            let detail = format!(
                "e{exec} d{}: the worker decided {decision:?}, replay gives {replayed:?}",
                decision.base
            );
            self.fail(SpecInvariant::ReplayEquivalent, detail);
        }
        let stored = match self.config.logging {
            Logging::Full => u64::try_from(decision.writes.len()).unwrap_or(0),
            Logging::ReadsOnly => 0,
        };
        self.stats.commits += 1;
        self.stats.log_rows += 1 + stored;
        self.stats.write_rows += stored;
        let release = self.config.mode != Mode::Eager;
        let durable = &mut self.execs[exec];
        durable.decisions += 1;
        durable.replayed = state;
        durable.scheduled.extend(replayed.writes.iter().copied());
        if release {
            for effect in replayed.writes {
                self.release(effect);
            }
        }
        if let Some(output) = decision.complete {
            let durable = &mut self.execs[exec];
            durable.done = true;
            let latency = self.now - durable.started_at;
            self.latencies.push(latency);
            self.stats.completed += 1;
            self.log("store", &format!("e{exec} complete in {latency}"));
            let expected = expected_output(workload, exec);
            if output != expected {
                let detail = format!("e{exec} returned {output}, the workload defines {expected}");
                self.fail(SpecInvariant::ExpectedOutput, detail);
            }
        }
    }

    /// Repair after a commit of `exec` failed on `worker`.
    fn repair(&mut self, worker: Holder, exec: usize) {
        let owner = self.execs[exec].owner == worker;
        let keep = self.config.plant == Plant::KeepOnFailure;
        let durable_base = self.execs[exec].decisions;
        let state = &mut self.workers[worker.0];
        let Some(resident) = state.resident.get_mut(&exec) else {
            return;
        };
        self.stats.discarded += u64::try_from(resident.chain.len()).unwrap_or(0);
        if !owner {
            state.resident.remove(&exec);
            self.log(&Self::label(worker), &format!("e{exec} drop, claim lost"));
            return;
        }
        if keep {
            // The plant: drop the chain, keep the state that it built.
            resident.chain.clear();
            resident.base = durable_base;
            resident.deciding = false;
            self.next_generation += 1;
            resident.generation = self.next_generation;
            self.try_decide(worker, exec);
            return;
        }
        self.adopt(worker, exec, true);
    }

    /// Hand `effect` to dispatch. The gate of issue #1796 applies here.
    fn release(&mut self, effect: Effect) {
        if !self.execs[effect.exec].scheduled.contains(&effect) {
            let detail = format!("{effect} left the worker before a commit scheduled it");
            self.fail(SpecInvariant::EffectAfterCommit, detail);
        }
        let at = self.now + self.config.timing.dispatch_us;
        self.push(at, Event::EffectStart { effect });
    }

    fn effect_start(&mut self, effect: Effect) {
        let runs = self.ledger.entry(effect.key()).or_insert(0);
        *runs += 1;
        let runs = *runs;
        self.stats.effects += 1;
        self.log("act", &format!("{effect} start"));
        if runs > 1 {
            self.fail(
                SpecInvariant::EffectOnce,
                format!("{effect} ran {runs} times"),
            );
        }
        let duration = self.draw(self.config.timing.activity_us);
        self.push(self.now + duration, Event::EffectDone { effect });
    }

    fn effect_done(&mut self, effect: Effect) {
        let durable = &mut self.execs[effect.exec];
        if !durable.scheduled.contains(&effect) {
            self.stats.orphan_results += 1;
            self.log("act", &format!("{effect} result rejected"));
            return;
        }
        if !durable.results.insert((effect.round, effect.index)) || durable.done {
            self.stats.duplicate_results += 1;
            return;
        }
        let value = effect.value();
        durable.inputs.push(Input::Result { effect, value });
        self.stats.log_rows += 1;
        self.log("act", &format!("{effect} result"));
        let exec = effect.exec;
        self.push(
            self.now + self.config.timing.wake_us,
            Event::Arrive { exec },
        );
    }

    fn crash(&mut self, worker: usize) {
        if !self.workers[worker].alive {
            return;
        }
        let holder = (worker, self.workers[worker].incarnation);
        self.stats.crashes += 1;
        let state = &mut self.workers[worker];
        state.alive = false;
        state.resident.clear();
        let ids: Vec<u64> = self
            .in_flight
            .iter()
            .filter(|(_, owner)| **owner == holder)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if self.rng.chance(50) {
                self.stats.landed_in_crash += 1;
            } else {
                self.stats.lost_in_crash += 1;
                self.lost.insert(id);
            }
        }
        self.log(&Self::label(holder), "crash");
        let lease = self.config.lease_us;
        let at = self.now + lease / 2 + self.rng.below(lease);
        self.push(at, Event::Restart { worker });
    }

    /// Move each claim whose holder is dead or stalled past the lease.
    fn scan(&mut self) {
        let lease = self.config.lease_us;
        for exec in 0..self.execs.len() {
            let durable = &self.execs[exec];
            if durable.done {
                continue;
            }
            let owner = durable.owner;
            let state = &self.workers[owner.0];
            let stale = state.stalled_until > self.now && self.now - state.stalled_since >= lease;
            if self.live(owner) && !stale {
                continue;
            }
            let candidates: Vec<Holder> = (0..self.workers.len())
                .filter(|&w| self.workers[w].alive && (w, self.workers[w].incarnation) != owner)
                .filter(|&w| self.workers[w].stalled_until <= self.now)
                .map(|w| (w, self.workers[w].incarnation))
                .collect();
            if candidates.is_empty() {
                continue;
            }
            let count = u64::try_from(candidates.len()).unwrap_or(1);
            let pick = usize::try_from(self.rng.below(count)).unwrap_or(0);
            let next = candidates[pick];
            let durable = &mut self.execs[exec];
            durable.owner = next;
            durable.epoch += 1;
            self.stats.reclaims += 1;
            self.log(
                "scan",
                &format!("e{exec} {} -> {}", Self::label(owner), Self::label(next)),
            );
            self.push(self.now, Event::Adopt { worker: next, exec });
        }
        self.push(self.now + lease / 2, Event::Scan);
    }
}

/// Run `config`.
#[must_use]
pub fn run(config: &SpecConfig) -> SpecReport {
    Model::new(config).run()
}

/// Run `config` twice and compare the reports.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice(config: &SpecConfig) -> Result<SpecReport, Nondeterminism> {
    run_twice_with(config, run)
}

/// [`run_twice`] with `runner` in place of [`run`].
///
/// A test passes a runner that changes its output to prove the check.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice_with(
    config: &SpecConfig,
    mut runner: impl FnMut(&SpecConfig) -> SpecReport,
) -> Result<SpecReport, Nondeterminism> {
    let first = runner(config);
    let second = runner(config);
    let note = "equal trace, different stats or latencies";
    diverged(
        config.seed,
        &first.trace,
        &second.trace,
        first == second,
        note,
    )
    .map_or(Ok(first), Err)
}

/// The latency of the completed executions of a sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencySummary {
    /// The number of completed executions.
    pub runs: u64,
    /// The mean latency.
    pub mean_us: u64,
    /// The median latency.
    pub p50_us: u64,
    /// The 99th percentile latency.
    pub p99_us: u64,
}

impl LatencySummary {
    /// The summary of `latencies`.
    #[must_use]
    pub fn of(latencies: &[u64]) -> Self {
        let mut sorted = latencies.to_vec();
        sorted.sort_unstable();
        let runs = u64::try_from(sorted.len()).unwrap_or(0);
        if runs == 0 {
            return Self::default();
        }
        let at = |percent: usize| sorted[(sorted.len() - 1) * percent / 100];
        Self {
            runs,
            mean_us: sorted.iter().sum::<u64>() / runs,
            p50_us: at(50),
            p99_us: at(99),
        }
    }
}

/// The result of a sweep with no failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpecSummary {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The merged coverage counters.
    pub stats: SpecStats,
    /// The latency over every seed.
    pub latency: LatencySummary,
}

/// A seed that failed a sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecFailure {
    /// The config of the failed run.
    pub config: SpecConfig,
    /// The broken invariant or the determinism error.
    pub reason: String,
    /// The last trace lines of the run.
    pub trace_tail: String,
}

impl fmt::Display for SpecFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed: {}\nreproduce: {}\nlast events:\n{}",
            self.config.seed,
            self.reason,
            repro_command(&self.config),
            self.trace_tail
        )
    }
}

/// Run every seed of `plan` twice.
///
/// # Errors
///
/// Returns the first seed that breaks an invariant or is not deterministic.
pub fn sweep(
    plan: &SeedPlan,
    config: impl Fn(u64) -> SpecConfig,
) -> Result<SpecSummary, Box<SpecFailure>> {
    let mut summary = SpecSummary::default();
    let mut latencies = Vec::new();
    for seed in plan.seeds() {
        let config = config(seed);
        let report = match run_twice(&config) {
            Ok(report) => report,
            Err(error) => {
                let report = run(&config);
                return Err(Box::new(SpecFailure {
                    trace_tail: report.trace_tail(TAIL_LINES),
                    config,
                    reason: error.to_string(),
                }));
            }
        };
        if let Some(violation) = &report.violation {
            return Err(Box::new(SpecFailure {
                reason: violation.to_string(),
                trace_tail: report.trace_tail(TAIL_LINES),
                config,
            }));
        }
        summary.seeds += 1;
        summary.stats.merge(&report.stats);
        latencies.extend(report.latencies_us);
    }
    summary.latency = LatencySummary::of(&latencies);
    Ok(summary)
}

/// The config for `seed` with the values that `var` gives. A missing value
/// keeps the default.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_vars(
    seed: u64,
    var: impl Fn(&str) -> Option<String>,
) -> Result<SpecConfig, String> {
    let mut config = SpecConfig::new(seed);
    if let Some(name) = var(MODE_VAR) {
        config.mode = Mode::parse(name.trim())?;
    }
    if let Some(name) = var(FENCE_VAR) {
        config.fence = Fence::parse(name.trim())?;
    }
    if let Some(name) = var(LOGGING_VAR) {
        config.logging = Logging::parse(name.trim())?;
    }
    if let Some(name) = var(PLANT_VAR) {
        config.plant = Plant::parse(name.trim())?;
    }
    if let Some(name) = var(WORKLOAD_VAR) {
        config.workload = Workload::parse(name.trim())?;
    }
    if let Some(list) = var(CHECKS_VAR) {
        config.checks = list
            .split(',')
            .map(|name| SpecInvariant::parse(name.trim()))
            .collect::<Result<_, _>>()?;
    }
    Ok(config)
}

/// [`config_from_vars`] with the values from the environment.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_env(seed: u64) -> Result<SpecConfig, String> {
    config_from_vars(seed, |name| std::env::var(name).ok())
}

/// The shell command that replays `config`.
///
/// It sets every variable that [`config_from_env`] reads.
#[must_use]
pub fn repro_command(config: &SpecConfig) -> String {
    let checks: Vec<&str> = config.checks.iter().map(|c| c.name()).collect();
    format!(
        "{SEED_VAR}={} {MODE_VAR}={} {FENCE_VAR}={} {LOGGING_VAR}={} {PLANT_VAR}={} \
         {WORKLOAD_VAR}={} {CHECKS_VAR}={} cargo test -p autumn-harvest --test dst \
         speculate::replay_one_speculation_seed -- --nocapture",
        config.seed,
        config.mode.as_str(),
        config.fence.as_str(),
        config.logging.as_str(),
        config.plant.as_str(),
        config.workload.as_str(),
        checks.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::{Effect, Input, State, Workload, decide, expected_output};

    #[test]
    fn a_serial_replay_of_the_chain_gives_the_expected_output() {
        let workload = Workload::Chain;
        let mut state = State::default();
        let mut inputs = Vec::new();
        loop {
            let read = &inputs[state.seen..];
            let (next, writes, complete) = decide(workload, 0, &state, read);
            state = next;
            if let Some(output) = complete {
                assert_eq!(output, expected_output(workload, 0));
                break;
            }
            assert_eq!(writes.len(), 1);
            let effect: Effect = writes[0];
            inputs.push(Input::Result {
                effect,
                value: effect.value(),
            });
        }
        assert_eq!(state.round, 3);
    }

    #[test]
    fn a_duplicate_result_changes_nothing() {
        let workload = Workload::FanOut;
        let (state, writes, _) = decide(workload, 1, &State::default(), &[]);
        let result = Input::Result {
            effect: writes[0],
            value: writes[0].value(),
        };
        let (once, _, _) = decide(workload, 1, &state, &[result]);
        let (twice, _, _) = decide(workload, 1, &once, &[result]);
        assert_eq!(once.acc, twice.acc);
        assert_eq!(once.pending, twice.pending);
    }
}
