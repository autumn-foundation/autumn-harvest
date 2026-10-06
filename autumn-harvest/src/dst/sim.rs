//! The seeded simulation of workers and the orphan reclaimer.

use std::collections::BTreeSet;

use super::invariant::{Ghost, Invariant, Live, Violation, WriteKind};
use super::rng::SplitMix64;
use super::store::{
    Claim, ClaimStore, Fencing, Op, OracleStore, Orphan, Outcome, Row, TaskState, WriteOutcome,
};

/// The parameters of one simulated run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    /// The seed. Equal configs give equal runs.
    pub seed: u64,
    /// The owner-write guard of the store.
    pub fencing: Fencing,
    /// The number of worker processes.
    pub workers: usize,
    /// The activity slots per worker.
    pub slots: usize,
    /// The number of activity tasks.
    pub tasks: usize,
    /// The step limit.
    pub max_steps: usize,
    /// A worker whose last beat is this old is dead to the reclaimer.
    pub stale_after_ms: u64,
    /// The liveness beat period of a running worker.
    pub beat_every_ms: u64,
    /// The orphan scan period.
    pub scan_every_ms: u64,
    /// The largest clock advance per step.
    pub max_tick_ms: u64,
    /// The invariants to check.
    pub checks: Vec<Invariant>,
}

impl SimConfig {
    /// The default config for `seed`: 3 workers, 2 slots each, 3 tasks.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            fencing: Fencing::ClaimEpoch,
            workers: 3,
            slots: 2,
            tasks: 3,
            max_steps: 400,
            stale_after_ms: 10_000,
            beat_every_ms: 2_000,
            scan_every_ms: 3_000,
            max_tick_ms: 1_000,
            checks: Invariant::ALL.to_vec(),
        }
    }

    /// This config with `fencing`.
    #[must_use]
    pub const fn with_fencing(mut self, fencing: Fencing) -> Self {
        self.fencing = fencing;
        self
    }

    /// This config, checking only `checks`.
    #[must_use]
    pub fn checking(mut self, checks: &[Invariant]) -> Self {
        self.checks = checks.to_vec();
        self
    }

    /// A fresh oracle store for this config.
    #[must_use]
    pub fn oracle(&self) -> OracleStore {
        OracleStore::new(self.tasks, self.fencing, self.stale_after_ms)
    }
}

/// One store operation of a run, its outcome, and the rows after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRecord {
    /// The step index. It matches the trace line.
    pub step: usize,
    /// The operation.
    pub op: Op,
    /// The outcome that the store returned.
    pub outcome: Outcome,
    /// Every task row after the operation.
    pub rows: Vec<Row>,
}

/// Counters that show what a run covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimStats {
    /// Claims that took a row.
    pub claims: u64,
    /// Start fences that took effect.
    pub starts: u64,
    /// Heartbeats that took effect.
    pub heartbeats: u64,
    /// Terminal writes that took effect.
    pub completes: u64,
    /// Owner writes that a stale claim lost.
    pub stale_rejected: u64,
    /// Terminal writes that a stale claim lost.
    pub stale_completes_rejected: u64,
    /// Orphan requeues that moved a row.
    pub reclaims: u64,
    /// Injected stalls.
    pub stalls: u64,
    /// Injected crashes.
    pub crashes: u64,
    /// The most distinct workers that claimed a row in one run.
    pub claimers: usize,
}

impl SimStats {
    /// Add the counters of `other`. `claimers` keeps the larger value.
    pub fn merge(&mut self, other: &Self) {
        self.claims += other.claims;
        self.starts += other.starts;
        self.heartbeats += other.heartbeats;
        self.completes += other.completes;
        self.stale_rejected += other.stale_rejected;
        self.stale_completes_rejected += other.stale_completes_rejected;
        self.reclaims += other.reclaims;
        self.stalls += other.stalls;
        self.crashes += other.crashes;
        self.claimers = self.claimers.max(other.claimers);
    }
}

/// The result of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    /// The config of the run.
    pub config: SimConfig,
    /// Every store operation, in order.
    pub steps: Vec<StepRecord>,
    /// One line per step, faults included.
    pub trace: Vec<String>,
    /// The first failed invariant, if any. The run stops there.
    pub violation: Option<Violation>,
    /// Coverage counters.
    pub stats: SimStats,
}

impl SimReport {
    /// The last `lines` trace lines, joined by newlines.
    #[must_use]
    pub fn trace_tail(&self, lines: usize) -> String {
        let start = self.trace.len().saturating_sub(lines);
        self.trace[start..].join("\n")
    }
}

/// Run `config` against the oracle store.
#[must_use]
pub fn run(config: &SimConfig) -> SimReport {
    run_with(config, config.oracle())
}

/// Run `config` against `store`.
pub fn run_with<S: ClaimStore>(config: &SimConfig, store: S) -> SimReport {
    Sim::new(config, store).run()
}

/// A claim that a worker slot acts on.
#[derive(Debug, Clone)]
struct Slot {
    claim: Claim,
    seq: u64,
    started: bool,
}

/// One worker process. A restart gives it a new incarnation and a new id.
#[derive(Debug, Clone)]
struct Worker {
    index: usize,
    incarnation: u32,
    slots: Vec<Option<Slot>>,
    last_beat_ms: u64,
    stalled_until_ms: u64,
    restart_at_ms: Option<u64>,
}

impl Worker {
    fn id(&self) -> String {
        format!("w{}.{}", self.index + 1, self.incarnation)
    }
}

/// What an actor does in one step.
#[derive(Debug, Clone, Copy)]
enum Action {
    Idle,
    Beat(usize),
    Claim(usize, usize),
    Start(usize, usize),
    Heartbeat(usize, usize),
    Complete(usize, usize),
    Stall(usize),
    Crash(usize),
    Restart(usize),
    Scan,
    Requeue,
}

/// The weight of each action kind in the scheduler draw.
const W_IDLE: u64 = 2;
const W_BEAT: u64 = 10;
const W_CLAIM: u64 = 4;
const W_START: u64 = 6;
const W_HEARTBEAT: u64 = 4;
const W_COMPLETE: u64 = 2;
const W_STALL: u64 = 1;
const W_CRASH: u64 = 1;
const W_RESTART: u64 = 6;
const W_SCAN: u64 = 6;
const W_REQUEUE: u64 = 6;

struct Sim<'a, S> {
    config: &'a SimConfig,
    store: S,
    rng: SplitMix64,
    now_ms: u64,
    step: usize,
    next_seq: u64,
    workers: Vec<Worker>,
    last_scan_ms: Option<u64>,
    orphans: Vec<Orphan>,
    ghost: Ghost,
    claimers: BTreeSet<usize>,
    report: SimReport,
}

impl<'a, S: ClaimStore> Sim<'a, S> {
    fn new(config: &'a SimConfig, store: S) -> Self {
        let workers = (0..config.workers)
            .map(|index| Worker {
                index,
                incarnation: 1,
                slots: vec![None; config.slots],
                last_beat_ms: 0,
                stalled_until_ms: 0,
                restart_at_ms: None,
            })
            .collect();
        Self {
            config,
            store,
            rng: SplitMix64::new(config.seed),
            now_ms: 0,
            step: 0,
            next_seq: 0,
            workers,
            last_scan_ms: None,
            orphans: Vec::new(),
            ghost: Ghost::new(config.tasks),
            claimers: BTreeSet::new(),
            report: SimReport {
                config: config.clone(),
                steps: Vec::new(),
                trace: Vec::new(),
                violation: None,
                stats: SimStats::default(),
            },
        }
    }

    fn run(mut self) -> SimReport {
        for w in 0..self.workers.len() {
            self.beat(w);
            self.step += 1;
        }
        while self.step < self.config.max_steps && self.report.violation.is_none() {
            if self.all_done() {
                self.log("all tasks completed");
                break;
            }
            self.now_ms += self.rng.below(self.config.max_tick_ms + 1);
            let action = self.choose();
            self.act(action);
            self.step += 1;
        }
        self.report.stats.claimers = self.claimers.len();
        self.report
    }

    fn all_done(&self) -> bool {
        self.store
            .rows()
            .iter()
            .all(|row| row.state == TaskState::Completed)
    }

    /// Every enabled action with its weight, in a fixed order.
    fn enabled(&self) -> Vec<(u64, Action)> {
        let mut actions = vec![(W_IDLE, Action::Idle)];
        for (w, worker) in self.workers.iter().enumerate() {
            if let Some(at) = worker.restart_at_ms {
                if self.now_ms >= at {
                    actions.push((W_RESTART, Action::Restart(w)));
                }
                continue;
            }
            if self.now_ms < worker.stalled_until_ms {
                continue;
            }
            if self.now_ms - worker.last_beat_ms >= self.config.beat_every_ms {
                actions.push((W_BEAT, Action::Beat(w)));
            }
            for (i, slot) in worker.slots.iter().enumerate() {
                match slot {
                    None => actions.push((W_CLAIM, Action::Claim(w, i))),
                    Some(slot) if !slot.started => actions.push((W_START, Action::Start(w, i))),
                    Some(_) => {
                        actions.push((W_HEARTBEAT, Action::Heartbeat(w, i)));
                        actions.push((W_COMPLETE, Action::Complete(w, i)));
                    }
                }
            }
            actions.push((W_STALL, Action::Stall(w)));
            actions.push((W_CRASH, Action::Crash(w)));
        }
        let scan_due = self
            .last_scan_ms
            .is_none_or(|at| self.now_ms - at >= self.config.scan_every_ms);
        if scan_due {
            actions.push((W_SCAN, Action::Scan));
        }
        if !self.orphans.is_empty() {
            actions.push((W_REQUEUE, Action::Requeue));
        }
        actions
    }

    fn choose(&mut self) -> Action {
        let actions = self.enabled();
        let total: u64 = actions.iter().map(|(weight, _)| weight).sum();
        let mut pick = self.rng.below(total);
        for (weight, action) in actions {
            if pick < weight {
                return action;
            }
            pick -= weight;
        }
        Action::Idle
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Idle => self.log("idle"),
            Action::Beat(w) => self.beat(w),
            Action::Claim(w, i) => self.claim(w, i),
            Action::Start(w, i) => self.owner_write(w, i, WriteKind::Start),
            Action::Heartbeat(w, i) => self.owner_write(w, i, WriteKind::Heartbeat),
            Action::Complete(w, i) => self.owner_write(w, i, WriteKind::Complete),
            Action::Stall(w) => self.stall(w),
            Action::Crash(w) => self.crash(w),
            Action::Restart(w) => self.restart(w),
            Action::Scan => self.scan(),
            Action::Requeue => self.requeue(),
        }
    }

    fn log(&mut self, line: &str) {
        let text = format!("{:04} t={:06} {line}", self.step, self.now_ms);
        self.report.trace.push(text);
    }

    /// Apply `op`, record it, and log `actor` with the outcome.
    fn apply(&mut self, actor: &str, op: Op) -> Outcome {
        let outcome = self.store.apply(&op);
        self.log(&format!("{actor} {} -> {outcome}", describe(&op)));
        self.report.steps.push(StepRecord {
            step: self.step,
            op,
            outcome: outcome.clone(),
            rows: self.store.rows(),
        });
        outcome
    }

    fn fail(&mut self, found: Vec<(Invariant, String)>) {
        let checked = found
            .into_iter()
            .find(|(invariant, _)| self.config.checks.contains(invariant));
        if let Some((invariant, detail)) = checked {
            self.log(&format!("VIOLATION {invariant}: {detail}"));
            self.report.violation = Some(Violation {
                invariant,
                step: self.step,
                detail,
            });
        }
    }

    fn beat(&mut self, w: usize) {
        let worker = &mut self.workers[w];
        worker.last_beat_ms = self.now_ms;
        let id = worker.id();
        let op = Op::Beat {
            worker: id.clone(),
            at_ms: self.now_ms,
        };
        self.apply(&id, op);
    }

    fn claim(&mut self, w: usize, i: usize) {
        let task = usize::try_from(self.rng.below(self.config.tasks as u64)).unwrap_or(0);
        let id = self.workers[w].id();
        let op = Op::Claim {
            worker: id.clone(),
            task,
        };
        let Outcome::Claimed(Some(claim)) = self.apply(&format!("{id}/s{i}"), op) else {
            return;
        };
        self.next_seq += 1;
        let seq = self.next_seq;
        self.ghost.claimed(claim.task, seq);
        self.claimers.insert(w);
        self.report.stats.claims += 1;
        self.log(&format!("{id}/s{i} holds claim #{seq}"));
        self.workers[w].slots[i] = Some(Slot {
            claim,
            seq,
            started: false,
        });
        let found = self.unique_ids();
        self.fail(found.into_iter().collect());
    }

    fn unique_ids(&self) -> Option<(Invariant, String)> {
        let live: Vec<Live<'_>> = self
            .workers
            .iter()
            .flat_map(|worker| worker.slots.iter().flatten())
            .map(|slot| Live {
                claim: &slot.claim,
                seq: slot.seq,
            })
            .collect();
        Ghost::unique_ids(&live)
    }

    fn owner_write(&mut self, w: usize, i: usize, kind: WriteKind) {
        let Some(slot) = self.workers[w].slots[i].clone() else {
            return;
        };
        let tag = slot.seq;
        let claim = slot.claim.clone();
        let op = match kind {
            WriteKind::Start => Op::Start { claim },
            WriteKind::Heartbeat => Op::Heartbeat { claim, tag },
            WriteKind::Complete => Op::Complete { claim, tag },
        };
        let actor = format!("{}/s{i} #{}", self.workers[w].id(), slot.seq);
        let Outcome::Write(outcome) = self.apply(&actor, op) else {
            return;
        };
        let found = self.ghost.wrote(kind, slot.claim.task, slot.seq, outcome);
        self.count(kind, outcome);
        self.workers[w].slots[i] = self.next_slot(slot, kind, outcome);
        self.fail(found);
    }

    const fn count(&mut self, kind: WriteKind, outcome: WriteOutcome) {
        let stats = &mut self.report.stats;
        match (kind, outcome) {
            (WriteKind::Start, WriteOutcome::Applied) => stats.starts += 1,
            (WriteKind::Heartbeat, WriteOutcome::Applied) => stats.heartbeats += 1,
            (WriteKind::Complete, WriteOutcome::Applied) => stats.completes += 1,
            (WriteKind::Complete, WriteOutcome::LeaseLost) => {
                stats.stale_rejected += 1;
                stats.stale_completes_rejected += 1;
            }
            (_, WriteOutcome::LeaseLost) => stats.stale_rejected += 1,
        }
    }

    /// The slot after an owner write.
    ///
    /// A lost start stops the attempt. A lost heartbeat cancels the activity
    /// under the claim epoch. Before issue #1789 the worker only logged it.
    fn next_slot(&self, mut slot: Slot, kind: WriteKind, outcome: WriteOutcome) -> Option<Slot> {
        match (kind, outcome) {
            (WriteKind::Complete, _) | (WriteKind::Start, WriteOutcome::LeaseLost) => None,
            (WriteKind::Start, WriteOutcome::Applied) => {
                slot.started = true;
                Some(slot)
            }
            (WriteKind::Heartbeat, WriteOutcome::LeaseLost)
                if self.config.fencing == Fencing::ClaimEpoch =>
            {
                None
            }
            (WriteKind::Heartbeat, _) => Some(slot),
        }
    }

    /// Pause the worker, as a long GC pause or a partition does.
    ///
    /// It keeps its claims but sends no beat, so it can look dead.
    fn stall(&mut self, w: usize) {
        let stale = self.config.stale_after_ms;
        let pause = stale / 2 + self.rng.below(stale * 2);
        let worker = &mut self.workers[w];
        worker.stalled_until_ms = self.now_ms + pause;
        let line = format!(
            "{} stall until t={:06}",
            worker.id(),
            worker.stalled_until_ms
        );
        self.report.stats.stalls += 1;
        self.log(&line);
    }

    /// Kill the worker. Its claims die with it. It restarts later.
    fn crash(&mut self, w: usize) {
        let down = 1_000 + self.rng.below(self.config.stale_after_ms * 2);
        let worker = &mut self.workers[w];
        worker.slots.iter_mut().for_each(|slot| *slot = None);
        worker.restart_at_ms = Some(self.now_ms + down);
        let line = format!(
            "{} crash, restart at t={:06}",
            worker.id(),
            self.now_ms + down
        );
        self.report.stats.crashes += 1;
        self.log(&line);
    }

    fn restart(&mut self, w: usize) {
        let worker = &mut self.workers[w];
        worker.incarnation += 1;
        worker.restart_at_ms = None;
        worker.stalled_until_ms = 0;
        self.beat(w);
    }

    fn scan(&mut self) {
        self.last_scan_ms = Some(self.now_ms);
        let op = Op::Scan {
            now_ms: self.now_ms,
        };
        if let Outcome::Orphans(orphans) = self.apply("reclaimer", op) {
            self.orphans = orphans;
        }
    }

    /// Requeue the first orphan that the last scan found.
    ///
    /// The scan and the requeue are separate steps, as in production. A
    /// worker can beat between them, and the requeue must then keep the row.
    fn requeue(&mut self) {
        let orphan = self.orphans.remove(0);
        let task = orphan.task;
        let op = Op::Requeue {
            orphan,
            now_ms: self.now_ms,
        };
        if self.apply("reclaimer", op) == Outcome::Requeued(true) {
            self.ghost.requeued(task);
            self.report.stats.reclaims += 1;
        }
    }
}

/// A short trace form of `op`.
fn describe(op: &Op) -> String {
    match op {
        Op::Beat { .. } => "beat".to_string(),
        Op::Claim { task, .. } => format!("claim t{task}"),
        Op::Start { claim } => format!("start t{} a{}", claim.task, claim.attempt),
        Op::Heartbeat { claim, .. } => format!("heartbeat t{} a{}", claim.task, claim.attempt),
        Op::Complete { claim, .. } => format!("complete t{} a{}", claim.task, claim.attempt),
        Op::Scan { .. } => "scan".to_string(),
        Op::Requeue { orphan, .. } => format!(
            "requeue t{} {} s{}",
            orphan.task, orphan.worker, orphan.crash_strikes
        ),
    }
}
