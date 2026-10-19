//! The fenced owner lease of one decision partition (issue #2250).
//!
//! This module is the simulator half of `formal/tla/OwnerLease.tla`. It has
//! the same actions, defects and invariants. A seed fixes every step, so a
//! failing seed replays exactly.
//!
//! [`OwnerStore`] is the seam for stage 1a (issue #2251). The
//! [`OracleOwnerStore`] keeps the owner row in memory. Stage 1a implements
//! the trait on Postgres and passes it to [`run_with`] or [`sweep_with`].
//! The store reports only durable facts. The simulator keeps the ghost
//! fields that the invariants compare.
//!
//! The four nemeses of the issue bias the scheduler:
//!
//! 1. [`Nemesis::StaleOwner`] pauses an owner mid-decision and ends its
//!    lease, so another worker can take over.
//! 2. [`Nemesis::LeaseFlap`] ends the lease during an append. It also lets
//!    a worker with a stale decision take the lease back.
//! 3. [`Nemesis::RenewInFlight`] renews while a commit holds the fence lock.
//! 4. [`Nemesis::AckBeforeBump`] ends the lease while an acknowledged signal
//!    waits, so the epoch bumps before a decision consumes it.
//!
//! An unloaded worker keeps its decisions in flight, as a paused thread
//! does. A new tenure of that worker can begin a decision beside them. The
//! oracle has no event-id backstop, so the fence alone keeps the
//! invariants.
//!
//! See `docs/testing/simulation.md` and `docs/adr/0009-fenced-owner-lease.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::rng::SplitMix64;
use super::sweep::{Nondeterminism, SeedPlan, SeedVars, TAIL_LINES, diverged};

/// The one seed to run.
pub const SEED_VAR: &str = "HARVEST_DST_OWNER_SEED";
/// The number of seeds in a sweep.
pub const SEEDS_VAR: &str = "HARVEST_DST_OWNER_SEEDS";
/// The first seed of a sweep.
pub const SEED_BASE_VAR: &str = "HARVEST_DST_OWNER_SEED_BASE";
/// The defect: `none` (the default) or a mutant name.
pub const DEFECT_VAR: &str = "HARVEST_DST_OWNER_DEFECT";
/// A comma list of nemesis names. The default is all of them.
pub const NEMESES_VAR: &str = "HARVEST_DST_OWNER_NEMESES";
/// A comma list of invariant names. The default is all of them.
pub const CHECKS_VAR: &str = "HARVEST_DST_OWNER_CHECKS";

/// The seed variables of this harness.
pub const OWNER_SEED_VARS: SeedVars = SeedVars {
    seed: SEED_VAR,
    seeds: SEEDS_VAR,
    base: SEED_BASE_VAR,
};

/// The chance in percent that a triggered nemesis takes the step.
const NEMESIS_PERCENT: u64 = 35;
/// The chance in percent that the clock may end a live lease this step.
const EXPIRE_PERCENT: u64 = 15;
/// The chance in percent that a holder may release the row this step.
const RELEASE_PERCENT: u64 = 5;
/// The shortest and the longest stall of a paused owner, in steps.
const STALL_STEPS: (u64, u64) = (4, 16);

named! {
    /// The design rule that a run breaks. The names match the `Defect`
    /// values of `formal/tla/OwnerLease.tla`.
    Defect {
        /// The design. No rule is broken.
        None => "none",
        /// The commit does not check the owner row (golem#3766).
        NoFence => "no-fence",
        /// The fence reads the owner row with no lock.
        UnlockedFence => "unlocked-fence",
        /// The fence compares the holder, not the epoch.
        HolderFence => "holder-fence",
        /// An acquisition keeps the old resident state.
        KeepCache => "keep-cache",
        /// A tenure begins a decision before its last one commits.
        Pipeline => "pipeline",
        /// A renewal adds 1 to the epoch.
        RenewBumps => "renew-bumps",
        /// The owner acknowledges a signal from memory (restate#4566).
        MemoryAck => "memory-ack",
    }
}

named! {
    /// A fault schedule of issue #2250.
    Nemesis {
        /// A stale owner commits after a takeover.
        StaleOwner => "stale-owner",
        /// The lease flaps during an append.
        LeaseFlap => "lease-flap",
        /// A renewal happens while a commit is in flight.
        RenewInFlight => "renew-in-flight",
        /// A signal is acknowledged just before an epoch bump.
        AckBeforeBump => "ack-before-bump",
    }
}

named! {
    /// A safety invariant. The names match `formal/tla/OwnerLease.tla`.
    OwnerInvariant {
        /// Each event comes from the worker that took its epoch.
        OneOwnerPerEpoch => "OneOwnerPerEpoch",
        /// A decision commits only at the epoch that it pinned.
        CommitAtPinnedEpoch => "CommitAtPinnedEpoch",
        /// Each decision saw every earlier event, so no event is lost.
        DecisionOnFreshHistory => "DecisionOnFreshHistory",
        /// No two events consume one signal.
        NoDuplicateEvent => "NoDuplicateEvent",
        /// A signal is acknowledged only after its durable write.
        AckAfterDurableWrite => "AckAfterDurableWrite",
        /// An acknowledged signal is pending, consumed, or in the memory of
        /// the worker that holds the row.
        NoAckedSignalLost => "NoAckedSignalLost",
        /// Only a takeover fences a commit.
        OwnerNeverFencesItself => "OwnerNeverFencesItself",
    }
}

/// A worker id. Workers count from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerId(pub usize);

/// An owner epoch. Each acquisition adds 1. Epoch 0 has no owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

/// A signal id. Signals count from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignalId(pub usize);

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "w{}", self.0)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.0)
    }
}

impl fmt::Display for SignalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}", self.0)
    }
}

/// The owner row of the partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRow {
    /// The worker that holds the row, or `None`.
    pub holder: Option<WorkerId>,
    /// The epoch of the last acquisition.
    pub epoch: Epoch,
    /// `true` while `lease_until` is in the future.
    pub live: bool,
}

/// The result of a renewal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Renewal {
    /// The lease is extended. The worker holds this epoch.
    Renewed(Epoch),
    /// The renewal waits for a lock. Nothing changed.
    Busy,
    /// The row moved. The worker does not own it.
    Lost,
}

/// The result of the fence check in a commit transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fence {
    /// The check passed. The commit may append.
    Held,
    /// The row moved. The commit appends nothing.
    Fenced,
}

/// A decision that a commit appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    /// The deciding worker.
    pub worker: WorkerId,
    /// The epoch that the worker held when the decision began.
    pub epoch: Epoch,
    /// The history length in the worker's resident state.
    pub base: usize,
    /// The signal that the decision consumes.
    pub signal: SignalId,
}

/// One decision event, as the simulator records it at the commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// The worker that committed it.
    pub by: WorkerId,
    /// The epoch that its decision pinned.
    pub pinned: Epoch,
    /// The epoch of the owner row that the commit saw.
    pub at: Epoch,
    /// The history length that its decision saw.
    pub base: usize,
    /// The history length in the store before the commit.
    pub index: usize,
    /// The signal that it consumed.
    pub signal: SignalId,
}

/// The store side of the owner lease.
///
/// Stage 1a (issue #2251) implements this trait on Postgres. The simulator
/// drives it from one thread, so no method may block:
///
/// - [`acquire`](Self::acquire) and [`renew`](Self::renew) take their row
///   lock with `NOWAIT` or a short `lock_timeout`. A lock miss is `None`
///   or [`Renewal::Busy`].
/// - [`lock`](Self::lock) opens a transaction that stays open until
///   [`commit`](Self::commit). A worker can have several decisions in
///   flight, so a store keys its open transactions by [`Decision`].
///
/// An async store runs each call to its end inside the method.
pub trait OwnerStore {
    /// The takeover: `SELECT ... FOR UPDATE` on the owner row, then the
    /// write. It takes a free row or a row whose lease ended, adds 1 to the
    /// epoch and returns the new epoch.
    ///
    /// Returns `None` when the lease is live or a commit holds the fence
    /// lock.
    fn acquire(&mut self, worker: WorkerId) -> Option<Epoch>;

    /// The graceful release. It frees the row when `worker` holds `epoch`.
    /// Returns `false` when the row moved.
    fn release(&mut self, worker: WorkerId, epoch: Epoch) -> bool;

    /// The renewal `UPDATE`. It matches `worker` and `epoch`.
    fn renew(&mut self, worker: WorkerId, epoch: Epoch) -> Renewal;

    /// The lease time runs out. The holder and the epoch stay.
    fn expire(&mut self);

    /// The durable `harvest_signals` write.
    fn write_signal(&mut self, signal: SignalId);

    /// Open the commit transaction and check the fence. After
    /// [`Fence::Held`], the lock stays until [`commit`](Self::commit).
    fn lock(&mut self, decision: &Decision) -> Fence;

    /// Append the event, consume the signal, commit and release the lock.
    /// Returns the epoch of the owner row that the commit saw.
    fn commit(&mut self, decision: &Decision) -> Epoch;

    /// The owner row.
    fn row(&self) -> LeaseRow;

    /// The pending durable signals, in id order.
    fn pending(&self) -> Vec<SignalId>;

    /// The signal of each decision event, in commit order.
    fn consumed(&self) -> Vec<SignalId>;
}

/// An in-memory model of the owner row, the signal rows and the history.
///
/// It applies the store side of a [`Defect`]: `no-fence`, `unlocked-fence`,
/// `holder-fence` and `renew-bumps`. The simulator applies the owner side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleOwnerStore {
    defect: Defect,
    row: LeaseRow,
    locks: Vec<Decision>,
    pending: BTreeSet<SignalId>,
    consumed: Vec<SignalId>,
}

impl OracleOwnerStore {
    /// An empty store. No worker holds the row.
    #[must_use]
    pub const fn new(defect: Defect) -> Self {
        Self {
            defect,
            row: LeaseRow {
                holder: None,
                epoch: Epoch(0),
                live: false,
            },
            locks: Vec::new(),
            pending: BTreeSet::new(),
            consumed: Vec::new(),
        }
    }

    const fn locks_the_row(&self) -> bool {
        !matches!(self.defect, Defect::NoFence | Defect::UnlockedFence)
    }
}

impl OwnerStore for OracleOwnerStore {
    fn acquire(&mut self, worker: WorkerId) -> Option<Epoch> {
        let takeable = self.row.holder.is_none() || !self.row.live;
        if !takeable || !self.locks.is_empty() {
            return None;
        }
        let epoch = Epoch(self.row.epoch.0 + 1);
        self.row = LeaseRow {
            holder: Some(worker),
            epoch,
            live: true,
        };
        Some(epoch)
    }

    fn release(&mut self, worker: WorkerId, epoch: Epoch) -> bool {
        if self.row.holder != Some(worker) || self.row.epoch != epoch {
            return false;
        }
        self.row.holder = None;
        self.row.live = false;
        true
    }

    fn renew(&mut self, worker: WorkerId, epoch: Epoch) -> Renewal {
        if self.row.holder != Some(worker) || self.row.epoch != epoch {
            return Renewal::Lost;
        }
        if self.defect == Defect::RenewBumps {
            // A bumping renewal writes the epoch, so it waits for the fence
            // locks.
            if !self.locks.is_empty() {
                return Renewal::Busy;
            }
            self.row.epoch = Epoch(epoch.0 + 1);
        }
        self.row.live = true;
        Renewal::Renewed(self.row.epoch)
    }

    fn expire(&mut self) {
        self.row.live = false;
    }

    fn write_signal(&mut self, signal: SignalId) {
        self.pending.insert(signal);
    }

    fn lock(&mut self, decision: &Decision) -> Fence {
        let holder = self.row.holder == Some(decision.worker);
        let passes = match self.defect {
            Defect::NoFence => true,
            Defect::HolderFence => holder,
            _ => holder && self.row.epoch == decision.epoch,
        };
        if !passes {
            return Fence::Fenced;
        }
        if self.locks_the_row() {
            self.locks.push(*decision);
        }
        Fence::Held
    }

    fn commit(&mut self, decision: &Decision) -> Epoch {
        self.consumed.push(decision.signal);
        self.pending.remove(&decision.signal);
        if let Some(at) = self.locks.iter().position(|d| d == decision) {
            self.locks.swap_remove(at);
        }
        self.row.epoch
    }

    fn row(&self) -> LeaseRow {
        self.row
    }

    fn pending(&self) -> Vec<SignalId> {
        self.pending.iter().copied().collect()
    }

    fn consumed(&self) -> Vec<SignalId> {
        self.consumed.clone()
    }
}

/// The parameters of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerConfig {
    /// The seed. Equal configs give equal runs.
    pub seed: u64,
    /// The rule that the run breaks.
    pub defect: Defect,
    /// The number of workers.
    pub workers: usize,
    /// The number of signals that clients send.
    pub signals: usize,
    /// The step limit.
    pub max_steps: usize,
    /// The fault schedules.
    pub nemeses: Vec<Nemesis>,
    /// The invariants to check.
    pub checks: Vec<OwnerInvariant>,
}

impl OwnerConfig {
    /// The design for `seed`: 3 workers, 4 signals and all four nemeses.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            defect: Defect::None,
            workers: 3,
            signals: 4,
            max_steps: 400,
            nemeses: Nemesis::ALL.to_vec(),
            checks: OwnerInvariant::ALL.to_vec(),
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
    pub fn checking(mut self, checks: &[OwnerInvariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnerStats {
    /// Acquisitions that took the row.
    pub acquisitions: u64,
    /// Acquisitions that took the row from another worker.
    pub takeovers: u64,
    /// Graceful releases.
    pub releases: u64,
    /// Renewals that kept the row.
    pub renewals: u64,
    /// Renewals that waited for a lock.
    pub renewals_busy: u64,
    /// Commits that appended an event.
    pub commits: u64,
    /// Nemesis 1: commits fenced because another worker took over.
    pub stale_commits_fenced: u64,
    /// Nemesis 2: commits fenced because their worker took the lease back.
    pub flaps_fenced: u64,
    /// Nemesis 2: commits that appended after the lease ended.
    pub appends_after_expiry: u64,
    /// Nemesis 3: renewals that landed while their worker held a fence lock.
    pub renewals_in_flight: u64,
    /// Nemesis 4: acquisitions while an acknowledged signal was pending.
    pub acks_before_bump: u64,
    /// Commits that the holder fenced in its own tenure.
    pub self_fences: u64,
    /// The steps that each nemesis took, in [`Nemesis::ALL`] order.
    pub fired: [u64; Nemesis::ALL.len()],
}

impl OwnerStats {
    /// Add the counters of `other`.
    pub fn merge(&mut self, other: &Self) {
        self.acquisitions += other.acquisitions;
        self.takeovers += other.takeovers;
        self.releases += other.releases;
        self.renewals += other.renewals;
        self.renewals_busy += other.renewals_busy;
        self.commits += other.commits;
        self.stale_commits_fenced += other.stale_commits_fenced;
        self.flaps_fenced += other.flaps_fenced;
        self.appends_after_expiry += other.appends_after_expiry;
        self.renewals_in_flight += other.renewals_in_flight;
        self.acks_before_bump += other.acks_before_bump;
        self.self_fences += other.self_fences;
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
pub struct OwnerViolation {
    /// The invariant.
    pub invariant: OwnerInvariant,
    /// The step after which it failed.
    pub step: usize,
    /// What the check found.
    pub detail: String,
}

impl fmt::Display for OwnerViolation {
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
pub struct OwnerReport {
    /// The config of the run.
    pub config: OwnerConfig,
    /// One line per step.
    pub trace: Vec<String>,
    /// The decision events, in commit order.
    pub history: Vec<Event>,
    /// The coverage counters.
    pub stats: OwnerStats,
    /// The first failed invariant, if any.
    pub violation: Option<OwnerViolation>,
    /// `true` when every signal was consumed before the step limit.
    pub completed: bool,
}

impl OwnerReport {
    /// The last `lines` trace lines, joined.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// The phase of a decision in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The decision read its state. The commit has not checked the fence.
    Read,
    /// The commit passed the fence and holds the lock.
    Locked,
}

/// A decision in flight and the ghost tenure that began it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InFlight {
    /// A ghost id. It names the decision in the trace.
    id: u64,
    decision: Decision,
    tenure: u64,
    phase: Phase,
}

/// The owner side of one worker process.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Worker {
    /// The epoch that the worker thinks it holds.
    pinned: Option<Epoch>,
    /// The ghost tenure of its last acquisition.
    tenure: u64,
    /// The history length in its resident state.
    cache: usize,
    /// Signals acknowledged from memory (`memory-ack` only).
    memory: BTreeSet<SignalId>,
    /// The worker does nothing before this step.
    paused_until: usize,
}

/// One step of the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Acquire(WorkerId),
    Release(WorkerId),
    Renew(WorkerId),
    Begin(WorkerId),
    Lock(u64),
    Commit(u64),
    Write(SignalId),
    Ack(SignalId),
    Receive(WorkerId, SignalId),
    Expire,
    Stall(WorkerId),
    Tick,
}

/// One run in progress.
struct Sim<'a, S: OwnerStore> {
    config: &'a OwnerConfig,
    store: &'a mut S,
    rng: SplitMix64,
    workers: Vec<Worker>,
    /// Every decision in flight, in the order it began.
    decisions: Vec<InFlight>,
    next_decision: u64,
    /// The ghost tenure of the owner row: the number of acquisitions.
    tenure: u64,
    /// Ghost: the worker that took each epoch.
    owners: BTreeMap<Epoch, WorkerId>,
    history: Vec<Event>,
    sent: BTreeSet<SignalId>,
    acked: BTreeSet<SignalId>,
    step: usize,
    trace: Vec<String>,
    stats: OwnerStats,
}

impl<'a, S: OwnerStore> Sim<'a, S> {
    fn new(config: &'a OwnerConfig, store: &'a mut S) -> Self {
        Self {
            config,
            store,
            rng: SplitMix64::new(config.seed),
            workers: vec![Worker::default(); config.workers],
            decisions: Vec::new(),
            next_decision: 0,
            tenure: 0,
            owners: BTreeMap::new(),
            history: Vec::new(),
            sent: BTreeSet::new(),
            acked: BTreeSet::new(),
            step: 0,
            trace: Vec::new(),
            stats: OwnerStats::default(),
        }
    }

    fn ids(&self) -> impl Iterator<Item = WorkerId> + use<'a, S> {
        (0..self.config.workers).map(WorkerId)
    }

    fn signals(&self) -> impl Iterator<Item = SignalId> + use<'a, S> {
        (0..self.config.signals).map(SignalId)
    }

    fn worker(&self, id: WorkerId) -> &Worker {
        &self.workers[id.0]
    }

    fn worker_mut(&mut self, id: WorkerId) -> &mut Worker {
        &mut self.workers[id.0]
    }

    fn awake(&self, id: WorkerId) -> bool {
        self.worker(id).paused_until <= self.step
    }

    fn has(&self, nemesis: Nemesis) -> bool {
        self.config.nemeses.contains(&nemesis)
    }

    fn log(&mut self, line: &str) {
        self.trace.push(format!("{:04} {line}", self.step));
    }

    fn flight(&self, id: u64) -> Option<InFlight> {
        self.decisions.iter().find(|d| d.id == id).copied()
    }

    fn drop_flight(&mut self, id: u64) {
        self.decisions.retain(|d| d.id != id);
    }

    /// `worker` holds a fence lock now.
    fn holds_lock(&self, worker: WorkerId) -> bool {
        self.decisions
            .iter()
            .any(|d| d.decision.worker == worker && d.phase == Phase::Locked)
    }

    /// `worker` may begin a decision in its current tenure.
    fn may_begin(&self, worker: WorkerId) -> bool {
        let tenure = self.worker(worker).tenure;
        let running = self
            .decisions
            .iter()
            .filter(|d| d.decision.worker == worker && d.tenure == tenure)
            .count();
        let limit = if self.config.defect == Defect::Pipeline {
            2
        } else {
            1
        };
        running < limit
    }

    /// The signals that the store holds durably: pending or consumed.
    fn written(&self) -> BTreeSet<SignalId> {
        let mut written: BTreeSet<SignalId> = self.store.pending().into_iter().collect();
        written.extend(self.store.consumed());
        written
    }

    /// The run is over: every signal is sent, acknowledged and consumed,
    /// and no decision is in flight.
    fn quiescent(&self) -> bool {
        self.sent.len() == self.config.signals
            && self.acked.len() == self.config.signals
            && self.store.pending().is_empty()
            && self.decisions.is_empty()
            && self.workers.iter().all(|w| w.memory.is_empty())
    }

    /// The actions that the design allows now, in a fixed order.
    fn enabled(&mut self) -> Vec<Action> {
        let row = self.store.row();
        let takeable = row.holder.is_none() || !row.live;
        let pending = self.store.pending();
        let written = self.written();
        let memory_ack = self.config.defect == Defect::MemoryAck;
        let mut actions = Vec::new();
        for id in self.ids() {
            if !self.awake(id) {
                continue;
            }
            let worker = self.worker(id);
            match worker.pinned {
                None if takeable => actions.push(Action::Acquire(id)),
                None => {}
                Some(_) => {
                    actions.push(Action::Renew(id));
                    if self.may_begin(id) && (!pending.is_empty() || !worker.memory.is_empty()) {
                        actions.push(Action::Begin(id));
                    }
                }
            }
            if memory_ack && worker.pinned.is_some() {
                for signal in self.signals() {
                    if !self.sent.contains(&signal) {
                        actions.push(Action::Receive(id, signal));
                    }
                }
            }
        }
        for flight in &self.decisions {
            if self.awake(flight.decision.worker) {
                actions.push(match flight.phase {
                    Phase::Read => Action::Lock(flight.id),
                    Phase::Locked => Action::Commit(flight.id),
                });
            }
        }
        for signal in self.signals() {
            if !memory_ack && !self.sent.contains(&signal) {
                actions.push(Action::Write(signal));
            }
            if written.contains(&signal) && !self.acked.contains(&signal) {
                actions.push(Action::Ack(signal));
            }
        }
        if let Some(holder) = row.holder.filter(|h| self.awake(*h))
            && self.worker(holder).pinned.is_some()
            && self.rng.chance(RELEASE_PERCENT)
        {
            actions.push(Action::Release(holder));
        }
        if row.holder.is_some() && row.live && self.rng.chance(EXPIRE_PERCENT) {
            actions.push(Action::Expire);
        }
        actions
    }

    /// The nemesis actions whose trigger holds now, in a fixed order.
    fn triggered(&self) -> Vec<(Nemesis, Action)> {
        let row = self.store.row();
        let takeable = row.holder.is_none() || !row.live;
        let holder = row.holder.filter(|h| self.awake(*h));
        let mut actions = Vec::new();
        if self.has(Nemesis::StaleOwner)
            && row.live
            && let Some(h) = holder.filter(|h| {
                self.decisions
                    .iter()
                    .any(|d| d.decision.worker == *h && d.phase == Phase::Read)
            })
        {
            actions.push((Nemesis::StaleOwner, Action::Stall(h)));
        }
        if self.has(Nemesis::LeaseFlap) {
            if row.live && self.decisions.iter().any(|d| d.phase == Phase::Locked) {
                actions.push((Nemesis::LeaseFlap, Action::Expire));
            }
            for id in self.ids() {
                let stale = self.decisions.iter().any(|d| d.decision.worker == id);
                if takeable && self.awake(id) && self.worker(id).pinned.is_none() && stale {
                    actions.push((Nemesis::LeaseFlap, Action::Acquire(id)));
                }
            }
        }
        if self.has(Nemesis::RenewInFlight)
            && let Some(h) = holder
            && self.holds_lock(h)
            && self.worker(h).pinned.is_some()
        {
            actions.push((Nemesis::RenewInFlight, Action::Renew(h)));
        }
        if self.has(Nemesis::AckBeforeBump) && row.live {
            let pending = self.store.pending();
            let waiting = self.acked.iter().any(|s| pending.contains(s))
                || self.workers.iter().any(|w| !w.memory.is_empty());
            if waiting {
                actions.push((Nemesis::AckBeforeBump, Action::Expire));
            }
        }
        actions
    }

    fn pick(&mut self) -> Action {
        let triggered = self.triggered();
        if !triggered.is_empty() && self.rng.chance(NEMESIS_PERCENT) {
            let (nemesis, action) = triggered[self.pick_index(triggered.len())];
            if let Some(at) = Nemesis::ALL.iter().position(|n| *n == nemesis) {
                self.stats.fired[at] += 1;
            }
            return action;
        }
        let enabled = self.enabled();
        if enabled.is_empty() {
            return Action::Tick;
        }
        enabled[self.pick_index(enabled.len())]
    }

    fn pick_index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        usize::try_from(self.rng.below(len)).unwrap_or(0)
    }

    /// The worker drops its ownership. Its decisions in flight stay.
    fn unload(&mut self, id: WorkerId) {
        let keep_cache = self.config.defect == Defect::KeepCache;
        let worker = self.worker_mut(id);
        worker.pinned = None;
        worker.memory.clear();
        if !keep_cache {
            worker.cache = 0;
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Acquire(id) => self.acquire(id),
            Action::Release(id) => self.release(id),
            Action::Renew(id) => self.renew(id),
            Action::Begin(id) => self.begin(id),
            Action::Lock(id) => self.lock(id),
            Action::Commit(id) => self.commit(id),
            Action::Write(signal) => {
                self.store.write_signal(signal);
                self.sent.insert(signal);
                self.log(&format!("client write {signal}"));
            }
            Action::Ack(signal) => {
                self.acked.insert(signal);
                self.log(&format!("client ack {signal}"));
            }
            Action::Receive(id, signal) => {
                self.worker_mut(id).memory.insert(signal);
                self.sent.insert(signal);
                self.acked.insert(signal);
                self.log(&format!("{id} receive {signal} -> acked from memory"));
            }
            Action::Expire => {
                self.store.expire();
                self.log("clock expire");
            }
            Action::Stall(id) => {
                let (low, high) = STALL_STEPS;
                let steps = low + self.rng.below(high - low + 1);
                let steps = usize::try_from(steps).unwrap_or(usize::MAX);
                self.worker_mut(id).paused_until = self.step.saturating_add(steps);
                self.store.expire();
                self.log(&format!("{id} stall {steps} steps, clock expire"));
            }
            Action::Tick => self.log("tick"),
        }
    }

    fn acquire(&mut self, id: WorkerId) {
        let before = self.store.row();
        let Some(epoch) = self.store.acquire(id) else {
            self.log(&format!("{id} acquire -> busy"));
            return;
        };
        self.tenure += 1;
        self.owners.insert(epoch, id);
        self.stats.acquisitions += 1;
        if before.holder.is_some_and(|h| h != id) {
            self.stats.takeovers += 1;
        }
        let pending = self.store.pending();
        if self.acked.iter().any(|s| pending.contains(s)) {
            self.stats.acks_before_bump += 1;
        }
        // The owner loads its state only after the acquisition commits.
        let reload = self.store.consumed().len();
        let keep_cache = self.config.defect == Defect::KeepCache;
        let tenure = self.tenure;
        let worker = self.worker_mut(id);
        worker.pinned = Some(epoch);
        worker.tenure = tenure;
        if !keep_cache {
            worker.cache = reload;
        }
        let cache = worker.cache;
        self.log(&format!("{id} acquire -> {epoch} cache {cache}"));
    }

    fn release(&mut self, id: WorkerId) {
        let Some(epoch) = self.worker(id).pinned else {
            return;
        };
        if self.store.release(id, epoch) {
            self.stats.releases += 1;
            self.unload(id);
            self.log(&format!("{id} release {epoch}"));
        } else {
            self.unload(id);
            self.log(&format!("{id} release {epoch} -> lost, unload"));
        }
    }

    fn renew(&mut self, id: WorkerId) {
        let Some(pinned) = self.worker(id).pinned else {
            return;
        };
        match self.store.renew(id, pinned) {
            Renewal::Renewed(epoch) => {
                self.stats.renewals += 1;
                if self.holds_lock(id) {
                    self.stats.renewals_in_flight += 1;
                }
                if epoch != pinned {
                    self.owners.insert(epoch, id);
                }
                self.worker_mut(id).pinned = Some(epoch);
                self.log(&format!("{id} renew -> {epoch}"));
            }
            Renewal::Busy => {
                self.stats.renewals_busy += 1;
                self.log(&format!("{id} renew -> busy"));
            }
            Renewal::Lost => {
                self.unload(id);
                self.log(&format!("{id} renew -> lost, unload"));
            }
        }
    }

    fn begin(&mut self, id: WorkerId) {
        let worker = self.worker(id);
        let Some(epoch) = worker.pinned else {
            return;
        };
        let mut candidates: BTreeSet<SignalId> = self.store.pending().into_iter().collect();
        candidates.extend(worker.memory.iter().copied());
        let candidates: Vec<SignalId> = candidates.into_iter().collect();
        if candidates.is_empty() {
            return;
        }
        let signal = candidates[self.pick_index(candidates.len())];
        let worker = self.worker(id);
        let decision = Decision {
            worker: id,
            epoch,
            base: worker.cache,
            signal,
        };
        let flight = InFlight {
            id: self.next_decision,
            decision,
            tenure: worker.tenure,
            phase: Phase::Read,
        };
        self.next_decision += 1;
        self.decisions.push(flight);
        self.log(&format!(
            "{id} begin d{} {signal} at {epoch} base {}",
            flight.id, decision.base
        ));
    }

    fn lock(&mut self, flight_id: u64) {
        let Some(flight) = self.flight(flight_id) else {
            return;
        };
        let decision = flight.decision;
        let id = decision.worker;
        match self.store.lock(&decision) {
            Fence::Held => {
                if let Some(f) = self.decisions.iter_mut().find(|d| d.id == flight_id) {
                    f.phase = Phase::Locked;
                }
                self.log(&format!(
                    "{id} lock d{flight_id} at {} -> held",
                    decision.epoch
                ));
            }
            Fence::Fenced => {
                let row = self.store.row();
                let tag = if row.holder != Some(id) {
                    self.stats.stale_commits_fenced += 1;
                    "stale-commit"
                } else if flight.tenure == self.tenure {
                    self.stats.self_fences += 1;
                    "self-fence"
                } else {
                    self.stats.flaps_fenced += 1;
                    "flap"
                };
                self.drop_flight(flight_id);
                if self.worker(id).pinned == Some(decision.epoch) {
                    self.unload(id);
                }
                self.log(&format!(
                    "{id} lock d{flight_id} at {} -> fenced ({tag}), row at {}",
                    decision.epoch, row.epoch
                ));
            }
        }
    }

    fn commit(&mut self, flight_id: u64) {
        let Some(flight) = self.flight(flight_id) else {
            return;
        };
        let decision = flight.decision;
        let id = decision.worker;
        let live = self.store.row().live;
        if !live {
            self.stats.appends_after_expiry += 1;
        }
        let index = self.store.consumed().len();
        let at = self.store.commit(&decision);
        self.history.push(Event {
            by: id,
            pinned: decision.epoch,
            at,
            base: decision.base,
            index,
            signal: decision.signal,
        });
        self.stats.commits += 1;
        self.drop_flight(flight_id);
        let worker = self.worker_mut(id);
        worker.cache = index + 1;
        worker.memory.remove(&decision.signal);
        self.log(&format!(
            "{id} commit d{flight_id} {} at {at}{}",
            decision.signal,
            if live { "" } else { " (lease ended)" }
        ));
    }

    /// The first failed invariant of `config.checks`, if any.
    fn check(&self) -> Option<OwnerViolation> {
        let pending: BTreeSet<SignalId> = self.store.pending().into_iter().collect();
        let log = self.store.consumed();
        let consumed: BTreeSet<SignalId> = log.iter().copied().collect();
        // Only the worker that still holds the row at its epoch can commit a
        // signal from memory.
        let row = self.store.row();
        let memory: BTreeSet<SignalId> = row
            .holder
            .filter(|h| self.worker(*h).pinned == Some(row.epoch))
            .map(|h| self.worker(h).memory.clone())
            .unwrap_or_default();
        let fail = |invariant, detail: String| {
            Some(OwnerViolation {
                invariant,
                step: self.step,
                detail,
            })
        };
        for invariant in &self.config.checks {
            match invariant {
                OwnerInvariant::OneOwnerPerEpoch => {
                    if let Some(e) = self
                        .history
                        .iter()
                        .find(|e| self.owners.get(&e.at) != Some(&e.by))
                    {
                        let owner = self
                            .owners
                            .get(&e.at)
                            .map_or_else(|| "nobody".to_string(), ToString::to_string);
                        return fail(
                            *invariant,
                            format!("{} commits at {}, which {owner} took", e.by, e.at),
                        );
                    }
                }
                OwnerInvariant::CommitAtPinnedEpoch => {
                    if let Some(e) = self.history.iter().find(|e| e.pinned != e.at) {
                        return fail(
                            *invariant,
                            format!("{} pinned {} and committed at {}", e.by, e.pinned, e.at),
                        );
                    }
                }
                OwnerInvariant::DecisionOnFreshHistory => {
                    if let Some(e) = self.history.iter().find(|e| e.base != e.index) {
                        return fail(
                            *invariant,
                            format!("event {} by {} decided on {} events", e.index, e.by, e.base),
                        );
                    }
                }
                OwnerInvariant::NoDuplicateEvent => {
                    if consumed.len() != log.len() {
                        return fail(*invariant, "two events consume one signal".to_string());
                    }
                }
                OwnerInvariant::AckAfterDurableWrite => {
                    if let Some(s) = self
                        .acked
                        .iter()
                        .find(|s| !pending.contains(s) && !consumed.contains(s))
                    {
                        return fail(*invariant, format!("{s} is acked with no durable write"));
                    }
                }
                OwnerInvariant::NoAckedSignalLost => {
                    if let Some(s) = self.acked.iter().find(|s| {
                        !pending.contains(s) && !consumed.contains(s) && !memory.contains(s)
                    }) {
                        return fail(*invariant, format!("{s} is acked and lost"));
                    }
                }
                OwnerInvariant::OwnerNeverFencesItself => {
                    if self.stats.self_fences > 0 {
                        return fail(*invariant, "the holder fenced its own commit".to_string());
                    }
                }
            }
        }
        None
    }

    fn run(mut self) -> OwnerReport {
        let row = self.store.row();
        debug_assert!(
            row.holder.is_none()
                && row.epoch == Epoch(0)
                && self.store.pending().is_empty()
                && self.store.consumed().is_empty(),
            "run_with needs an empty store"
        );
        let mut violation = None;
        while self.step < self.config.max_steps && !self.quiescent() {
            let action = self.pick();
            self.apply(action);
            if let Some(found) = self.check() {
                self.trace
                    .push(format!("{:04} VIOLATION {found}", self.step));
                violation = Some(found);
                break;
            }
            self.step += 1;
        }
        let completed = self.quiescent();
        OwnerReport {
            config: self.config.clone(),
            trace: self.trace,
            history: self.history,
            stats: self.stats,
            violation,
            completed,
        }
    }
}

/// Run `config` on a fresh [`OracleOwnerStore`].
#[must_use]
pub fn run(config: &OwnerConfig) -> OwnerReport {
    let mut store = OracleOwnerStore::new(config.defect);
    run_with(config, &mut store)
}

/// Run `config` on `store`. The store must start empty.
///
/// The owner side of [`OwnerConfig::defect`] applies here. A store applies
/// the store side itself.
pub fn run_with<S: OwnerStore>(config: &OwnerConfig, store: &mut S) -> OwnerReport {
    Sim::new(config, store).run()
}

/// Run `config` twice and compare the reports.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice(config: &OwnerConfig) -> Result<OwnerReport, Nondeterminism> {
    run_twice_with(config, |config| OracleOwnerStore::new(config.defect))
}

/// [`run_twice`] on two fresh stores from `store`.
///
/// # Errors
///
/// Returns [`Nondeterminism`] when the two runs differ.
pub fn run_twice_with<S: OwnerStore>(
    config: &OwnerConfig,
    mut store: impl FnMut(&OwnerConfig) -> S,
) -> Result<OwnerReport, Nondeterminism> {
    let first = run_with(config, &mut store(config));
    let second = run_with(config, &mut store(config));
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
pub struct OwnerSummary {
    /// The number of seeds that ran.
    pub seeds: u64,
    /// The merged coverage counters.
    pub stats: OwnerStats,
}

/// A seed that failed a sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerFailure {
    /// The config of the failed run.
    pub config: OwnerConfig,
    /// The failed invariant or the determinism error.
    pub reason: String,
    /// The last trace lines of the run.
    pub trace_tail: String,
}

impl fmt::Display for OwnerFailure {
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

/// Run every seed of `plan` twice on the oracle.
///
/// # Errors
///
/// Returns the first seed that fails an invariant, is not deterministic,
/// or does not consume every signal within the step limit.
pub fn sweep(
    plan: &SeedPlan,
    config: impl Fn(u64) -> OwnerConfig,
) -> Result<OwnerSummary, Box<OwnerFailure>> {
    sweep_with(plan, config, |config| OracleOwnerStore::new(config.defect))
}

/// [`sweep`] on fresh stores from `store`. Stage 1a passes its Postgres
/// store here.
///
/// # Errors
///
/// Returns the first seed that fails an invariant, is not deterministic,
/// or does not consume every signal within the step limit.
pub fn sweep_with<S: OwnerStore>(
    plan: &SeedPlan,
    config: impl Fn(u64) -> OwnerConfig,
    mut store: impl FnMut(&OwnerConfig) -> S,
) -> Result<OwnerSummary, Box<OwnerFailure>> {
    let mut summary = OwnerSummary::default();
    for seed in plan.seeds() {
        let config = config(seed);
        let report = match run_twice_with(&config, &mut store) {
            Ok(report) => report,
            Err(error) => {
                let report = run_with(&config, &mut store(&config));
                return Err(Box::new(OwnerFailure {
                    trace_tail: report.trace_tail(TAIL_LINES),
                    config,
                    reason: error.to_string(),
                }));
            }
        };
        if let Some(violation) = &report.violation {
            return Err(Box::new(OwnerFailure {
                reason: violation.to_string(),
                trace_tail: report.trace_tail(TAIL_LINES),
                config,
            }));
        }
        // A store that makes no progress must not pass.
        if !report.completed {
            return Err(Box::new(OwnerFailure {
                reason: format!(
                    "the run did not consume every signal within {} steps",
                    config.max_steps
                ),
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

/// The config for `seed` with the given variable values. A missing value
/// keeps the default of [`OwnerConfig::new`].
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_vars(
    seed: u64,
    defect: Option<&str>,
    nemeses: Option<&str>,
    checks: Option<&str>,
) -> Result<OwnerConfig, String> {
    let mut config = OwnerConfig::new(seed);
    if let Some(defect) = defect {
        config.defect = Defect::parse(defect.trim())?;
    }
    if let Some(nemeses) = nemeses {
        config.nemeses = parse_list(nemeses, Nemesis::parse)?;
    }
    if let Some(checks) = checks {
        config.checks = parse_list(checks, OwnerInvariant::parse)?;
    }
    Ok(config)
}

/// The config for `seed` with the values of [`DEFECT_VAR`],
/// [`NEMESES_VAR`] and [`CHECKS_VAR`].
///
/// # Errors
///
/// Returns a message when a value is not a known name.
pub fn config_from_env(seed: u64) -> Result<OwnerConfig, String> {
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
pub fn repro_command(config: &OwnerConfig) -> String {
    let names = |items: Vec<&str>| items.join(",");
    format!(
        "{SEED_VAR}={} {DEFECT_VAR}={} {NEMESES_VAR}={} {CHECKS_VAR}={} \
         cargo test -p autumn-harvest --no-default-features --test dst \
         replay_one_owner_seed -- --nocapture",
        config.seed,
        config.defect.as_str(),
        names(config.nemeses.iter().map(|n| n.as_str()).collect()),
        names(config.checks.iter().map(|c| c.as_str()).collect()),
    )
}
