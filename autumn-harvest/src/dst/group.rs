//! Group commit of one decision partition (issue #2256).
//!
//! Two owners share one decision partition of several executions. Each
//! owner commits its decisions through one lane, as
//! `crate::group_commit::GroupCommit` does. A seed fixes every step, so a
//! failing seed replays exactly.
//!
//! The lane plans each batch with `group_commit::plan_batch`. Each decision
//! gets its result through `group_commit::acknowledges`. So the simulator
//! checks the engine's own rules. The store models the rest:
//!
//! - The start fence locks the owner row `FOR KEY SHARE` until the batch
//!   ends. An acquisition waits for that lock.
//! - Each stage runs in a savepoint. A failed stage rolls back alone.
//! - The event-id backstop fails a stage whose decision read a history that
//!   its transaction no longer sees.
//!
//! The three nemeses bias the scheduler:
//!
//! 1. [`Nemesis::FailDecision`] fails one stage of a batch.
//! 2. [`Nemesis::FailCommit`] fails the `COMMIT` of a batch.
//! 3. [`Nemesis::Takeover`] ends the lease while the holder has work in its
//!    lane, so another owner can take over.
//!
//! A run has a fault phase and a drain, as the owner simulator has. The
//! drain injects no fault and is weakly fair. A run must consume every
//! signal before the drain ends.
//!
//! See `docs/testing/simulation.md` and `docs/design/DESIGN-2256.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::fair::Waits;
use super::owner::{Epoch, LeaseRow, WorkerId};
use super::rng::SplitMix64;
use super::sweep::{Nondeterminism, SeedPlan, SeedVars, TAIL_LINES, diverged};
use crate::group_commit::{self, Queued};

/// The one seed to run.
pub const SEED_VAR: &str = "HARVEST_DST_GROUP_SEED";
/// The number of seeds in a sweep.
pub const SEEDS_VAR: &str = "HARVEST_DST_GROUP_SEEDS";
/// The first seed of a sweep.
pub const SEED_BASE_VAR: &str = "HARVEST_DST_GROUP_SEED_BASE";
/// The defect: `none` (the default) or a mutant name.
pub const DEFECT_VAR: &str = "HARVEST_DST_GROUP_DEFECT";
/// A comma list of nemesis names. The default is all of them.
pub const NEMESES_VAR: &str = "HARVEST_DST_GROUP_NEMESES";
/// A comma list of invariant names. The default is all of them.
pub const CHECKS_VAR: &str = "HARVEST_DST_GROUP_CHECKS";

/// The seed variables of this harness.
pub const GROUP_SEED_VARS: SeedVars = SeedVars {
    seed: SEED_VAR,
    seeds: SEEDS_VAR,
    base: SEED_BASE_VAR,
};

named! {
    /// The design rule that a run breaks.
    Defect {
        /// The design. No rule is broken.
        None => "none",
        /// A failed stage keeps its writes.
        NoRollback => "no-rollback",
        /// A failed stage rolls the whole batch back.
        AbortOnFailure => "abort-on-failure",
        /// A decision reports success when its savepoint is released.
        EarlyAck => "early-ack",
        /// The batch does not check the owner row.
        NoFence => "no-fence",
        /// The batch checks the owner row but does not lock it.
        UnlockedFence => "unlocked-fence",
        /// The plan admits two decisions of one execution.
        SharedExecution => "shared-execution",
    }
}

named! {
    /// A fault schedule.
    Nemesis {
        /// One stage of a batch fails.
        FailDecision => "fail-decision",
        /// The `COMMIT` of a batch fails.
        FailCommit => "fail-commit",
        /// The lease ends while the holder has work in its lane, so another
        /// owner can take over.
        Takeover => "takeover",
    }
}

named! {
    /// A safety invariant.
    GroupInvariant {
        /// No durable event comes from a failed stage.
        FailedDecisionWritesNothing => "FailedDecisionWritesNothing",
        /// A batch with no batch fault commits each stage that passed.
        FailureIsolated => "FailureIsolated",
        /// Each acknowledged decision has a durable event.
        AckAfterCommit => "AckAfterCommit",
        /// Each event commits at the epoch and under the holder that its
        /// decision pinned.
        CommitAtPinnedEpoch => "CommitAtPinnedEpoch",
        /// No batch holds two decisions of one execution.
        OneDecisionPerExecution => "OneDecisionPerExecution",
        /// No stage fails on a write of its own batch.
        NoConflictInsideABatch => "NoConflictInsideABatch",
        /// No two events consume one signal.
        NoDuplicateEvent => "NoDuplicateEvent",
    }
}

/// The parameters of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupConfig {
    /// The seed.
    pub seed: u64,
    /// The rule that the run breaks.
    pub defect: Defect,
    /// The number of workers.
    pub workers: usize,
    /// The number of executions in the partition.
    pub executions: usize,
    /// The signals of each execution.
    pub signals: usize,
    /// The slots of a lane.
    pub width: usize,
    /// The most decisions in one batch.
    pub max_batch: usize,
    /// The length of the fault phase, in steps.
    pub fault_steps: usize,
    /// The step limit of the drain.
    pub drain_steps: usize,
    /// The fault schedules.
    pub nemeses: Vec<Nemesis>,
    /// The invariants to check.
    pub checks: Vec<GroupInvariant>,
}

impl GroupConfig {
    /// The design for `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            defect: Defect::None,
            workers: 2,
            executions: 4,
            signals: 3,
            width: 1,
            max_batch: 4,
            fault_steps: 300,
            drain_steps: 600,
            nemeses: Nemesis::ALL.to_vec(),
            checks: GroupInvariant::ALL.to_vec(),
        }
    }

    /// This config with `defect`.
    #[must_use]
    pub const fn with_defect(mut self, defect: Defect) -> Self {
        self.defect = defect;
        self
    }

    /// This config with only `nemeses`.
    #[must_use]
    pub fn with_nemeses(mut self, nemeses: &[Nemesis]) -> Self {
        self.nemeses = nemeses.to_vec();
        self
    }

    /// This config, checking only `checks`.
    #[must_use]
    pub fn checking(mut self, checks: &[GroupInvariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupStats {
    /// Batches that ran.
    pub batches: u64,
    /// Batches of two or more decisions.
    pub grouped_batches: u64,
    /// The largest batch.
    pub largest: u64,
    /// Committed batches with one failed stage or more and one committed
    /// stage or more.
    pub isolated_failures: u64,
    /// Stages that the `fail-decision` nemesis failed.
    pub injected_failures: u64,
    /// Batches whose `COMMIT` failed.
    pub failed_commits: u64,
    /// Batches that failed the start fence.
    pub fenced_batches: u64,
    /// Acquisitions that took the row from another worker.
    pub takeovers: u64,
    /// Committed batches with one injected failure or more and one
    /// committed stage or more.
    pub isolated_injected: u64,
    /// Stages that failed on the event-id backstop.
    pub conflicts: u64,
    /// Batches that a worker led while another batch of its lane was open.
    pub overlaps: u64,
    /// Stages that waited for a write of another open batch.
    pub waits: u64,
    /// Stages that a wait cycle failed, as Postgres fails a deadlock victim.
    pub deadlocks: u64,
    /// Runs that reached the drain.
    pub drains: u64,
    /// The steps that each nemesis took, in [`Nemesis::ALL`] order.
    pub fired: [u64; Nemesis::ALL.len()],
}

impl GroupStats {
    /// Add the counters of `other`.
    pub fn merge(&mut self, other: &Self) {
        self.batches += other.batches;
        self.grouped_batches += other.grouped_batches;
        self.largest = self.largest.max(other.largest);
        self.isolated_failures += other.isolated_failures;
        self.injected_failures += other.injected_failures;
        self.failed_commits += other.failed_commits;
        self.fenced_batches += other.fenced_batches;
        self.takeovers += other.takeovers;
        self.isolated_injected += other.isolated_injected;
        self.conflicts += other.conflicts;
        self.overlaps += other.overlaps;
        self.waits += other.waits;
        self.deadlocks += other.deadlocks;
        self.drains += other.drains;
        for (mine, theirs) in self.fired.iter_mut().zip(other.fired) {
            *mine += theirs;
        }
    }

    /// The steps that `nemesis` took.
    #[must_use]
    pub fn fired(&self, nemesis: Nemesis) -> u64 {
        Nemesis::ALL
            .iter()
            .position(|n| *n == nemesis)
            .map_or(0, |at| self.fired[at])
    }
}

/// A failed invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupViolation {
    /// The invariant.
    pub invariant: GroupInvariant,
    /// The step after which it failed.
    pub step: usize,
    /// What the check found.
    pub detail: String,
}

impl fmt::Display for GroupViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "step {}: {} failed: {}",
            self.step,
            self.invariant.as_str(),
            self.detail
        )
    }
}

/// The result of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupReport {
    /// The config of the run.
    pub config: GroupConfig,
    /// One line per step.
    pub trace: Vec<String>,
    /// The coverage counters.
    pub stats: GroupStats,
    /// The first failed invariant, if any.
    pub violation: Option<GroupViolation>,
    /// `true` when every signal was consumed before the drain ended.
    pub completed: bool,
}

impl GroupReport {
    /// Why the run fails, if it does.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        if let Some(violation) = &self.violation {
            return Some(violation.to_string());
        }
        (!self.completed).then(|| "the run did not consume every signal".to_string())
    }

    /// The last `lines` trace lines, joined.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// The chance in percent that a triggered nemesis takes the step.
const NEMESIS_PERCENT: u64 = 35;
/// The chance in percent that the clock may end a live lease this step.
const EXPIRE_PERCENT: u64 = 4;
/// The most decisions of one execution that one tenure keeps in flight.
///
/// A task row allows one. Two model a re-take, so the plan rule matters.
const PER_EXECUTION: usize = 2;
/// In the drain, an action that stays enabled this many steps without a run
/// is overdue.
const FAIR_STEPS: usize = 8;

/// One decision event, as the store holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Event {
    decision: u64,
    execution: usize,
    signal: usize,
    by: WorkerId,
    pinned: Epoch,
    /// The epoch of the owner row at the commit.
    at: Epoch,
    /// The holder of the owner row at the commit.
    holder_at: Option<WorkerId>,
    /// The write of a stage that failed. Only `no-rollback` keeps it.
    failed: bool,
}

/// The owner row, the fence locks, the histories and the signals.
#[derive(Debug, Clone)]
struct Store {
    row: LeaseRow,
    /// Batches that hold the owner row `FOR KEY SHARE`.
    fence_locks: usize,
    history: Vec<Vec<Event>>,
    pending: Vec<BTreeSet<usize>>,
}

/// Where a decision is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    Queued,
    Batched,
    Done,
}

#[derive(Debug, Clone, Copy)]
struct Decision {
    worker: WorkerId,
    epoch: Epoch,
    execution: usize,
    /// The history length that the decision read.
    base: usize,
    signal: usize,
    place: Place,
}

/// The phase of a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Fence,
    Stage(usize),
    Commit,
}

#[derive(Debug, Clone)]
struct Batch {
    id: u64,
    worker: WorkerId,
    epoch: Epoch,
    entries: Vec<u64>,
    phase: Phase,
    locked: bool,
    /// The writes of the open transaction.
    writes: Vec<Event>,
    passed: Vec<u64>,
    failed: Vec<u64>,
    /// The failed stages that the `fail-decision` nemesis failed.
    injected: Vec<u64>,
    /// The stage waits for a write of another open batch.
    waiting: bool,
}

#[derive(Debug, Clone, Default)]
struct Worker {
    pinned: Option<Epoch>,
    /// The queued decisions of its lane, in arrival order.
    lane: Vec<u64>,
}

/// How a batch ended.
#[derive(Debug, Clone)]
struct BatchEnd {
    batch: u64,
    passed: Vec<u64>,
    /// The fence or the `COMMIT` failed.
    batch_fault: bool,
}

/// One step of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Action {
    Acquire(WorkerId),
    Expire,
    Begin(WorkerId, usize),
    Lead(WorkerId),
    Fence(u64),
    Stage(u64, bool),
    Commit(u64, bool),
    Tick,
}

struct Sim<'a> {
    config: &'a GroupConfig,
    rng: SplitMix64,
    store: Store,
    workers: Vec<Worker>,
    decisions: BTreeMap<u64, Decision>,
    batches: Vec<Batch>,
    next_id: u64,
    acked: BTreeSet<u64>,
    ended: Option<BatchEnd>,
    /// Stages that failed on a write of their own batch.
    self_conflicts: Vec<u64>,
    step: usize,
    waiting: Waits<Action>,
    trace: Vec<String>,
    stats: GroupStats,
}

impl<'a> Sim<'a> {
    fn new(config: &'a GroupConfig) -> Self {
        let all: BTreeSet<usize> = (0..config.signals).collect();
        Self {
            config,
            rng: SplitMix64::new(config.seed),
            store: Store {
                row: LeaseRow {
                    holder: None,
                    epoch: Epoch(0),
                    live: false,
                },
                fence_locks: 0,
                history: vec![Vec::new(); config.executions],
                pending: vec![all; config.executions],
            },
            workers: vec![Worker::default(); config.workers],
            decisions: BTreeMap::new(),
            batches: Vec::new(),
            next_id: 0,
            acked: BTreeSet::new(),
            ended: None,
            self_conflicts: Vec::new(),
            step: 0,
            waiting: Waits::new(),
            trace: Vec::new(),
            stats: GroupStats::default(),
        }
    }

    const fn draining(&self) -> bool {
        self.step >= self.config.fault_steps
    }

    fn has(&self, nemesis: Nemesis) -> bool {
        self.config.nemeses.contains(&nemesis)
    }

    fn log(&mut self, line: &str) {
        self.trace.push(format!("{:04} {line}", self.step));
    }

    const fn locks_the_row(&self) -> bool {
        !matches!(self.config.defect, Defect::NoFence | Defect::UnlockedFence)
    }

    const fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn batch(&self, id: u64) -> Option<&Batch> {
        self.batches.iter().find(|b| b.id == id)
    }

    fn open_batches(&self, worker: WorkerId) -> usize {
        self.batches.iter().filter(|b| b.worker == worker).count()
    }

    /// The decisions of `execution` that `worker` keeps in flight at `epoch`.
    fn in_flight(&self, worker: WorkerId, epoch: Epoch, execution: usize) -> usize {
        self.decisions
            .values()
            .filter(|d| {
                d.worker == worker
                    && d.epoch == epoch
                    && d.execution == execution
                    && d.place != Place::Done
            })
            .count()
    }

    /// The run is over: every signal is consumed, and nothing is in flight.
    fn quiescent(&self) -> bool {
        self.store.pending.iter().all(BTreeSet::is_empty)
            && self.batches.is_empty()
            && self.decisions.values().all(|d| d.place == Place::Done)
    }

    /// The execution of the next stage of batch `b`, if it runs a stage.
    fn next_execution(&self, b: u64) -> Option<usize> {
        let batch = self.batch(b)?;
        let Phase::Stage(at) = batch.phase else {
            return None;
        };
        let d = batch.entries.get(at)?;
        self.decisions.get(d).map(|d| d.execution)
    }

    /// The open batches that hold an uncommitted write that the next stage
    /// of `b` must wait for. Postgres holds that stage on the unique index.
    fn waits_for(&self, b: u64) -> Vec<u64> {
        let Some(x) = self.next_execution(b) else {
            return Vec::new();
        };
        self.batches
            .iter()
            .filter(|c| c.id != b && c.writes.iter().any(|e| e.execution == x))
            .map(|c| c.id)
            .collect()
    }

    /// The next stage of `b` waits for another open batch.
    fn blocked(&self, b: u64) -> bool {
        !self.waits_for(b).is_empty()
    }

    /// The next stage of `b` waits in a cycle. Postgres then fails one
    /// stage of the cycle as a deadlock victim.
    fn in_cycle(&self, b: u64) -> bool {
        let mut seen = BTreeSet::new();
        let mut next = self.waits_for(b);
        while let Some(c) = next.pop() {
            if c == b {
                return true;
            }
            if seen.insert(c) {
                next.extend(self.waits_for(c));
            }
        }
        false
    }

    /// The actions that the design allows now, in a fixed order.
    fn enabled(&mut self) -> Vec<Action> {
        let row = self.store.row;
        let takeable = (row.holder.is_none() || !row.live) && self.store.fence_locks == 0;
        let mut actions = Vec::new();
        for w in 0..self.config.workers {
            let id = WorkerId(w);
            let worker = &self.workers[w];
            match worker.pinned {
                None if takeable => actions.push(Action::Acquire(id)),
                None => {}
                Some(epoch) => {
                    for x in 0..self.config.executions {
                        if !self.store.pending[x].is_empty()
                            && self.in_flight(id, epoch, x) < PER_EXECUTION
                        {
                            actions.push(Action::Begin(id, x));
                        }
                    }
                }
            }
            if !worker.lane.is_empty() && self.open_batches(id) < self.config.width {
                actions.push(Action::Lead(id));
            }
        }
        for batch in &self.batches {
            match batch.phase {
                Phase::Fence => actions.push(Action::Fence(batch.id)),
                Phase::Stage(_) if self.blocked(batch.id) && !self.in_cycle(batch.id) => {}
                Phase::Stage(_) => actions.push(Action::Stage(batch.id, false)),
                Phase::Commit => actions.push(Action::Commit(batch.id, false)),
            }
        }
        if !self.draining() && row.live && self.rng.chance(EXPIRE_PERCENT) {
            actions.push(Action::Expire);
        }
        actions
    }

    /// The nemesis actions whose trigger holds now, in a fixed order.
    fn triggered(&self) -> Vec<(Nemesis, Action)> {
        let mut actions = Vec::new();
        for batch in &self.batches {
            match batch.phase {
                Phase::Stage(_) if self.has(Nemesis::FailDecision) && !self.blocked(batch.id) => {
                    actions.push((Nemesis::FailDecision, Action::Stage(batch.id, true)));
                }
                Phase::Commit if self.has(Nemesis::FailCommit) => {
                    actions.push((Nemesis::FailCommit, Action::Commit(batch.id, true)));
                }
                _ => {}
            }
        }
        let row = self.store.row;
        if self.has(Nemesis::Takeover)
            && row.live
            && let Some(holder) = row.holder
        {
            let busy = !self.workers[holder.0].lane.is_empty()
                || self.batches.iter().any(|b| b.worker == holder);
            if busy {
                actions.push((Nemesis::Takeover, Action::Expire));
            }
        }
        actions
    }

    fn pick_index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        usize::try_from(self.rng.below(len)).unwrap_or(0)
    }

    fn pick(&mut self) -> Action {
        if self.draining() {
            let enabled = self.enabled();
            self.waiting.track(self.step, &enabled);
            let action = match self.waiting.overdue(self.step, FAIR_STEPS, &enabled) {
                Some(action) => action,
                None if enabled.is_empty() => Action::Tick,
                None => enabled[self.pick_index(enabled.len())],
            };
            let _ = self.waiting.ran(&action);
            return action;
        }
        let triggered = self.triggered();
        if !triggered.is_empty() && self.rng.chance(NEMESIS_PERCENT) {
            let (nemesis, action) = triggered[self.pick_index(triggered.len())];
            if let Some(at) = Nemesis::ALL.iter().position(|n| *n == nemesis) {
                self.stats.fired[at] += 1;
            }
            self.log(&format!("NEMESIS {}", nemesis.as_str()));
            return action;
        }
        let enabled = self.enabled();
        if enabled.is_empty() {
            return Action::Tick;
        }
        enabled[self.pick_index(enabled.len())]
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Acquire(id) => self.acquire(id),
            Action::Expire => {
                self.store.row.live = false;
                self.log("clock expire");
            }
            Action::Begin(id, x) => self.begin(id, x),
            Action::Lead(id) => self.lead(id),
            Action::Fence(b) => self.fence(b),
            Action::Stage(b, fail) => self.stage(b, fail),
            Action::Commit(b, fail) => self.commit(b, fail),
            Action::Tick => self.log("tick"),
        }
    }

    fn acquire(&mut self, id: WorkerId) {
        let row = self.store.row;
        let epoch = Epoch(row.epoch.0 + 1);
        if row.holder.is_some_and(|h| h != id) {
            self.stats.takeovers += 1;
        }
        self.store.row = LeaseRow {
            holder: Some(id),
            epoch,
            live: true,
        };
        self.workers[id.0].pinned = Some(epoch);
        self.log(&format!("{id} acquire {epoch}"));
    }

    fn begin(&mut self, id: WorkerId, x: usize) {
        let (Some(epoch), Some(&signal)) =
            (self.workers[id.0].pinned, self.store.pending[x].first())
        else {
            return;
        };
        let base = self.store.history[x].len();
        let d = self.id();
        self.decisions.insert(
            d,
            Decision {
                worker: id,
                epoch,
                execution: x,
                base,
                signal,
                place: Place::Queued,
            },
        );
        self.workers[id.0].lane.push(d);
        self.log(&format!(
            "{id} begin d{d} x{x} s{signal} base {base} at {epoch}"
        ));
    }

    /// The batch that the head of the lane leads, by the engine's plan.
    fn plan(&self, lane: &[u64]) -> Vec<usize> {
        let views: Vec<Queued<Epoch, usize>> = lane
            .iter()
            .filter_map(|d| self.decisions.get(d))
            .map(|d| Queued {
                tenure: d.epoch,
                execution: d.execution,
            })
            .collect();
        let max_batch = group_commit::share(views.len(), self.config.width, self.config.max_batch);
        if self.config.defect != Defect::SharedExecution {
            return group_commit::plan_batch(&views, 0, max_batch);
        }
        // The mutant drops the execution rule.
        let Some(head) = views.first() else {
            return Vec::new();
        };
        (0..views.len())
            .filter(|&at| views[at].tenure == head.tenure)
            .take(max_batch)
            .collect()
    }

    fn lead(&mut self, id: WorkerId) {
        let lane = self.workers[id.0].lane.clone();
        let order = self.plan(&lane);
        let entries: Vec<u64> = order
            .iter()
            .filter_map(|&at| lane.get(at).copied())
            .collect();
        let Some(epoch) = entries
            .first()
            .and_then(|d| self.decisions.get(d))
            .map(|d| d.epoch)
        else {
            return;
        };
        self.workers[id.0].lane.retain(|d| !entries.contains(d));
        for d in &entries {
            if let Some(decision) = self.decisions.get_mut(d) {
                decision.place = Place::Batched;
            }
        }
        let b = self.id();
        let len = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        if self.open_batches(id) > 0 {
            self.stats.overlaps += 1;
        }
        self.stats.batches += 1;
        if len > 1 {
            self.stats.grouped_batches += 1;
        }
        self.stats.largest = self.stats.largest.max(len);
        let names = names(&entries);
        self.batches.push(Batch {
            id: b,
            worker: id,
            epoch,
            entries,
            phase: Phase::Fence,
            locked: false,
            writes: Vec::new(),
            passed: Vec::new(),
            failed: Vec::new(),
            injected: Vec::new(),
            waiting: false,
        });
        self.log(&format!("{id} lead b{b} at {epoch}: [{names}]"));
    }

    fn fence(&mut self, b: u64) {
        let Some(batch) = self.batch(b).cloned() else {
            return;
        };
        let row = self.store.row;
        let passes = self.config.defect == Defect::NoFence
            || (row.holder == Some(batch.worker) && row.epoch == batch.epoch);
        if !passes {
            self.stats.fenced_batches += 1;
            let worker = &mut self.workers[batch.worker.0];
            if worker.pinned == Some(batch.epoch) {
                worker.pinned = None;
            }
            self.log(&format!(
                "{} b{b} fenced: row {} holds",
                batch.worker, row.epoch
            ));
            self.end(b, false, true);
            return;
        }
        let locks = self.locks_the_row();
        if locks {
            self.store.fence_locks += 1;
        }
        if let Some(open) = self.batches.iter_mut().find(|x| x.id == b) {
            open.locked = locks;
            open.phase = Phase::Stage(0);
        }
        self.log(&format!("{} b{b} fence held", batch.worker));
    }

    fn stage(&mut self, b: u64, fail: bool) {
        let Some(batch) = self.batch(b).cloned() else {
            return;
        };
        let Phase::Stage(at) = batch.phase else {
            return;
        };
        let Some(&d) = batch.entries.get(at) else {
            return;
        };
        let Some(decision) = self.decisions.get(&d).copied() else {
            return;
        };
        let x = decision.execution;
        let event = Event {
            decision: d,
            execution: x,
            signal: decision.signal,
            by: decision.worker,
            pinned: decision.epoch,
            at: decision.epoch,
            holder_at: None,
            failed: false,
        };
        // A stage that waits in a cycle is the deadlock victim.
        let deadlock = !fail && self.in_cycle(b);
        // The event-id backstop: the decision must have read the history
        // that its transaction sees.
        let own = batch.writes.iter().filter(|e| e.execution == x).count();
        let local = self.store.history[x].len() + own;
        let conflict = !fail && !deadlock && local != decision.base;
        let failed = fail || deadlock || conflict;
        let keep = (!failed || self.config.defect == Defect::NoRollback) && !deadlock;
        if fail {
            self.stats.injected_failures += 1;
        }
        if deadlock {
            self.stats.deadlocks += 1;
        }
        if conflict {
            self.stats.conflicts += 1;
            if own > 0 {
                self.self_conflicts.push(d);
            }
        }
        let early = !failed && self.config.defect == Defect::EarlyAck;
        if early {
            self.acked.insert(d);
        }
        let abort = failed && self.config.defect == Defect::AbortOnFailure;
        if let Some(open) = self.batches.iter_mut().find(|x| x.id == b) {
            if keep {
                open.writes.push(Event { failed, ..event });
            }
            if failed {
                open.failed.push(d);
                if fail {
                    open.injected.push(d);
                }
            } else {
                open.passed.push(d);
            }
            open.phase = if at + 1 < open.entries.len() {
                Phase::Stage(at + 1)
            } else {
                Phase::Commit
            };
        }
        let what = match (fail, deadlock, conflict) {
            (true, _, _) => "fails, rolled back",
            (_, true, _) => "is a deadlock victim, rolled back",
            (_, _, true) => "conflicts, rolled back",
            _ => "passes",
        };
        self.log(&format!("{} b{b} stage d{d} {what}", batch.worker));
        if abort {
            self.log(&format!("{} b{b} aborts on the failed stage", batch.worker));
            self.end(b, false, false);
        }
    }

    fn commit(&mut self, b: u64, fail: bool) {
        let Some(batch) = self.batch(b).cloned() else {
            return;
        };
        if fail {
            self.stats.failed_commits += 1;
            self.log(&format!("{} b{b} commit fails", batch.worker));
            self.end(b, false, true);
            return;
        }
        let row = self.store.row;
        for event in &batch.writes {
            let x = event.execution;
            self.store.history[x].push(Event {
                at: row.epoch,
                holder_at: row.holder,
                ..*event
            });
            self.store.pending[x].remove(&event.signal);
        }
        if !batch.failed.is_empty() && !batch.passed.is_empty() {
            self.stats.isolated_failures += 1;
        }
        if !batch.injected.is_empty() && !batch.passed.is_empty() {
            self.stats.isolated_injected += 1;
        }
        self.log(&format!(
            "{} b{b} commit: committed [{}], rolled back [{}], injected [{}]",
            batch.worker,
            names(&batch.passed),
            names(&batch.failed),
            names(&batch.injected)
        ));
        self.end(b, true, false);
    }

    /// End batch `b`. Each decision gets its result. The fence lock goes.
    fn end(&mut self, b: u64, committed: bool, batch_fault: bool) {
        let Some(at) = self.batches.iter().position(|x| x.id == b) else {
            return;
        };
        let batch = self.batches.remove(at);
        if batch.locked {
            self.store.fence_locks -= 1;
        }
        for d in &batch.entries {
            if group_commit::acknowledges(batch.passed.contains(d), committed) {
                self.acked.insert(*d);
            }
            if let Some(decision) = self.decisions.get_mut(d) {
                decision.place = Place::Done;
            }
        }
        self.ended = Some(BatchEnd {
            batch: b,
            passed: batch.passed,
            batch_fault,
        });
    }

    /// Count each stage that starts to wait for another open batch.
    fn track_waits(&mut self) {
        let ids: Vec<u64> = self.batches.iter().map(|b| b.id).collect();
        for b in ids {
            let blocked = self.blocked(b);
            if let Some(batch) = self.batches.iter_mut().find(|x| x.id == b) {
                if blocked && !batch.waiting {
                    self.stats.waits += 1;
                }
                batch.waiting = blocked;
            }
        }
    }

    const fn violation(&self, invariant: GroupInvariant, detail: String) -> GroupViolation {
        GroupViolation {
            invariant,
            step: self.step,
            detail,
        }
    }

    fn durable(&self) -> impl Iterator<Item = &Event> {
        self.store.history.iter().flatten()
    }

    fn check(&mut self) -> Option<GroupViolation> {
        let ended = self.ended.take();
        for &invariant in &self.config.checks {
            let found = match invariant {
                GroupInvariant::FailedDecisionWritesNothing => self
                    .durable()
                    .find(|e| e.failed)
                    .map(|e| format!("d{} failed, but its write committed", e.decision)),
                GroupInvariant::FailureIsolated => ended.as_ref().and_then(|end| {
                    if end.batch_fault {
                        return None;
                    }
                    end.passed
                        .iter()
                        .find(|d| !self.durable().any(|e| e.decision == **d))
                        .map(|d| {
                            format!(
                                "b{} had no batch fault, but d{d} passed and did not commit",
                                end.batch
                            )
                        })
                }),
                GroupInvariant::AckAfterCommit => self
                    .acked
                    .iter()
                    .find(|d| !self.durable().any(|e| e.decision == **d))
                    .map(|d| format!("d{d} is acknowledged with no durable event")),
                GroupInvariant::CommitAtPinnedEpoch => self
                    .durable()
                    .find(|e| e.at != e.pinned || e.holder_at != Some(e.by))
                    .map(|e| {
                        format!(
                            "d{} pinned {} by {}, but committed at {} under {:?}",
                            e.decision, e.pinned, e.by, e.at, e.holder_at
                        )
                    }),
                GroupInvariant::OneDecisionPerExecution => self.batches.iter().find_map(|b| {
                    let mut seen = BTreeSet::new();
                    b.entries
                        .iter()
                        .filter_map(|d| self.decisions.get(d))
                        .find(|d| !seen.insert(d.execution))
                        .map(|d| format!("b{} holds two decisions of x{}", b.id, d.execution))
                }),
                GroupInvariant::NoConflictInsideABatch => self
                    .self_conflicts
                    .first()
                    .map(|d| format!("d{d} failed on a write of its own batch")),
                GroupInvariant::NoDuplicateEvent => {
                    let mut seen = BTreeSet::new();
                    self.durable()
                        .find(|e| !seen.insert((e.execution, e.signal)))
                        .map(|e| format!("x{} s{} is consumed twice", e.execution, e.signal))
                }
            };
            if let Some(detail) = found {
                return Some(self.violation(invariant, detail));
            }
        }
        None
    }

    fn run(mut self) -> GroupReport {
        let mut violation = None;
        let limit = self
            .config
            .fault_steps
            .saturating_add(self.config.drain_steps);
        while self.step < limit && !self.quiescent() {
            if self.step == self.config.fault_steps {
                self.stats.drains += 1;
                self.log("drain starts");
            }
            let action = self.pick();
            self.apply(action);
            self.track_waits();
            if let Some(found) = self.check() {
                self.trace
                    .push(format!("{:04} VIOLATION {found}", self.step));
                violation = Some(found);
                break;
            }
            self.step += 1;
        }
        let completed = violation.is_none() && self.quiescent();
        GroupReport {
            config: self.config.clone(),
            trace: self.trace,
            stats: self.stats,
            violation,
            completed,
        }
    }
}

/// The decision ids `ids`, as `d1 d2`.
fn names(ids: &[u64]) -> String {
    ids.iter()
        .map(|d| format!("d{d}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Run `config`.
#[must_use]
pub fn run(config: &GroupConfig) -> GroupReport {
    Sim::new(config).run()
}

/// Run `config` twice and compare the reports.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice(config: &GroupConfig) -> Result<GroupReport, Nondeterminism> {
    let first = run(config);
    let second = run(config);
    diverged(
        config.seed,
        &first.trace,
        &second.trace,
        first == second,
        "the reports differ",
    )
    .map_or(Ok(first), Err)
}

/// The result of a sweep with no failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupSummary {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The merged coverage counters.
    pub stats: GroupStats,
}

/// A seed that failed a sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupFailure {
    /// The config of the failed run.
    pub config: GroupConfig,
    /// The failed invariant or the determinism error.
    pub reason: String,
    /// The last trace lines of the run.
    pub trace_tail: String,
}

impl fmt::Display for GroupFailure {
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

/// Run every seed of `plan` twice.
///
/// # Errors
///
/// Returns the first seed that fails.
pub fn sweep(
    plan: &SeedPlan,
    config: impl Fn(u64) -> GroupConfig,
) -> Result<GroupSummary, Box<GroupFailure>> {
    let mut summary = GroupSummary::default();
    for seed in plan.seeds() {
        let config = config(seed);
        let report = match run_twice(&config) {
            Ok(report) => report,
            Err(error) => {
                let report = run(&config);
                return Err(Box::new(GroupFailure {
                    trace_tail: report.trace_tail(TAIL_LINES),
                    config,
                    reason: error.to_string(),
                }));
            }
        };
        if let Some(reason) = report.failure() {
            return Err(Box::new(GroupFailure {
                reason,
                trace_tail: report.trace_tail(TAIL_LINES),
                config,
            }));
        }
        summary.seeds += 1;
        summary.stats.merge(&report.stats);
    }
    Ok(summary)
}

/// Parse a comma list of names with `parse`. An empty list is allowed.
fn parse_list<T>(text: &str, parse: impl Fn(&str) -> Result<T, String>) -> Result<Vec<T>, String> {
    text.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(parse)
        .collect()
}

/// The config for `seed` with the given variable values.
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_vars(
    seed: u64,
    defect: Option<&str>,
    nemeses: Option<&str>,
    checks: Option<&str>,
) -> Result<GroupConfig, String> {
    let mut config = GroupConfig::new(seed);
    if let Some(defect) = defect {
        config.defect = Defect::parse(defect.trim())?;
    }
    if let Some(nemeses) = nemeses {
        config.nemeses = parse_list(nemeses, Nemesis::parse)?;
    }
    if let Some(checks) = checks {
        config.checks = parse_list(checks, GroupInvariant::parse)?;
    }
    Ok(config)
}

/// The config for `seed` with the values of [`DEFECT_VAR`],
/// [`NEMESES_VAR`] and [`CHECKS_VAR`].
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_env(seed: u64) -> Result<GroupConfig, String> {
    let var = |name: &str| std::env::var(name).ok();
    config_from_vars(
        seed,
        var(DEFECT_VAR).as_deref(),
        var(NEMESES_VAR).as_deref(),
        var(CHECKS_VAR).as_deref(),
    )
}

/// The command that replays `config` locally.
#[must_use]
pub fn repro_command(config: &GroupConfig) -> String {
    let names = |items: Vec<&str>| items.join(",");
    format!(
        "{SEED_VAR}={} {DEFECT_VAR}={} {NEMESES_VAR}={} {CHECKS_VAR}={} \
         cargo test -p autumn-harvest --no-default-features --test dst \
         replay_one_group_seed -- --nocapture",
        config.seed,
        config.defect.as_str(),
        names(config.nemeses.iter().map(|n| n.as_str()).collect()),
        names(config.checks.iter().map(|c| c.as_str()).collect()),
    )
}
