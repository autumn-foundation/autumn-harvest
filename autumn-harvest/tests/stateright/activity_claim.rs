//! The activity claim protocol of issue #1789, on [`OracleStore`].
//!
//! The model drives the same operations as `dst::run`: beat, claim, start,
//! heartbeat, release, complete, crash, restart, orphan scan and orphan
//! requeue. Each step applies one operation to the oracle. The seeded
//! sweep picks one schedule per seed. This model takes every schedule.
//!
//! The clock is a bound tick count, and a worker is dead one tick after
//! its last beat. A stall needs no action, because a worker may skip any
//! number of steps. The ghost checker is [`Ghost`], the one that `dst::run`
//! uses, so both harnesses judge a run by the same rules.

use std::collections::BTreeSet;

use autumn_harvest::dst::{
    Claim, ClaimStore, Fencing, Ghost, Invariant, Live, Op, OracleStore, Orphan, Outcome,
    TaskState, WriteKind, WriteOutcome,
};
use stateright::{Checker, Model, Property};

use super::{check_all, check_until, deep, tla_constants, tla_red_rows};

/// A worker is dead when its last beat is this many ticks old.
const STALE_TICKS: u64 = 1;

/// The bounds of one check.
#[derive(Debug, Clone, Copy)]
struct ActivityClaim {
    /// The guard of every owner write.
    fencing: Fencing,
    /// The number of worker processes.
    workers: usize,
    /// The activity slots of each worker.
    slots: usize,
    /// The number of task rows.
    tasks: usize,
    /// The number of successful claims in one behavior.
    max_claims: u64,
    /// The last tick of the clock.
    max_tick: u64,
    /// The incarnations of each worker. A restart starts the next one.
    incarnations: u32,
}

impl ActivityClaim {
    /// The design: 2 workers with 2 slots each, on 1 task row.
    ///
    /// Two slots let a stalled worker claim its own requeued row again. That
    /// is the trace of issue #1789. The nightly run adds a clock tick.
    fn design() -> Self {
        Self {
            fencing: Fencing::ClaimEpoch,
            workers: 2,
            slots: 2,
            tasks: 1,
            max_claims: 3,
            max_tick: if deep() { 2 } else { 1 },
            incarnations: 2,
        }
    }

    /// The design on 2 task rows, with 1 slot for each worker.
    ///
    /// It checks the invariants across rows. A worker cannot hold two
    /// claims, so no crash is needed to make one stale. The nightly run
    /// adds a clock tick and a restart.
    fn two_tasks() -> Self {
        Self {
            slots: 1,
            tasks: 2,
            max_tick: if deep() { 2 } else { 1 },
            incarnations: if deep() { 2 } else { 1 },
            ..Self::design()
        }
    }

    /// The design with the guard from before issue #1789.
    fn pre_fix() -> Self {
        Self {
            fencing: Fencing::StateOnly,
            ..Self::design()
        }
    }
}

/// A claim that a worker acts on, with its ghost sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Slot {
    claim: Claim,
    seq: u64,
    started: bool,
}

/// One worker process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Worker {
    incarnation: u32,
    slots: Vec<Option<Slot>>,
    /// The worker crashed and has not restarted.
    down: bool,
}

/// The oracle, the actors and the ghost state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    store: OracleStore,
    tick: u64,
    workers: Vec<Worker>,
    /// The rows that the last scan found and the reclaimer has not requeued.
    orphans: Vec<Orphan>,
    ghost: Ghost,
    next_seq: u64,
    /// Every invariant that a step has broken. A broken invariant stays here.
    broken: BTreeSet<Invariant>,
    /// The guard rejected a write by a claim that the row no longer holds.
    stale_fenced: bool,
}

/// One step. Workers and slots count from 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Beat(usize),
    Claim {
        worker: usize,
        slot: usize,
        task: usize,
    },
    Write {
        worker: usize,
        slot: usize,
        kind: WriteKind,
    },
    Crash(usize),
    Restart(usize),
    Scan,
    Requeue,
    Tick,
}

/// The id of a worker in the store: `w<index + 1>.<incarnation>`, as in
/// `dst::run`.
fn worker_id(index: usize, worker: &Worker) -> String {
    format!("w{}.{}", index + 1, worker.incarnation)
}

impl ActivityClaim {
    /// The slot after an owner write. It copies `next_slot` in `dst::run`.
    ///
    /// A release or a terminal write ends the attempt. So does a lost start.
    /// A lost heartbeat cancels the activity under the claim epoch only.
    fn next_slot(self, mut slot: Slot, kind: WriteKind, outcome: WriteOutcome) -> Option<Slot> {
        match (kind, outcome) {
            (WriteKind::Complete | WriteKind::Release, _)
            | (WriteKind::Start, WriteOutcome::LeaseLost) => None,
            (WriteKind::Start, WriteOutcome::Applied) => {
                slot.started = true;
                Some(slot)
            }
            (WriteKind::Heartbeat, WriteOutcome::LeaseLost)
                if self.fencing == Fencing::ClaimEpoch =>
            {
                None
            }
            (WriteKind::Heartbeat, _) => Some(slot),
        }
    }

    fn beat(state: &mut State, w: usize) {
        let worker = worker_id(w, &state.workers[w]);
        state.store.apply(&Op::Beat {
            worker,
            at_ms: state.tick,
        });
    }

    fn claim(state: &mut State, w: usize, i: usize, task: usize) -> bool {
        let worker = worker_id(w, &state.workers[w]);
        let Outcome::Claimed(Some(claim)) = state.store.apply(&Op::Claim { worker, task }) else {
            return false;
        };
        state.next_seq += 1;
        let seq = state.next_seq;
        state.ghost.claimed(claim.task, seq);
        state.workers[w].slots[i] = Some(Slot {
            claim,
            seq,
            started: false,
        });
        let live: Vec<Live<'_>> = state
            .workers
            .iter()
            .flat_map(|worker| worker.slots.iter().flatten())
            .map(|slot| Live {
                claim: &slot.claim,
                seq: slot.seq,
            })
            .collect();
        if let Some((invariant, _)) = Ghost::unique_ids(&live) {
            state.broken.insert(invariant);
        }
        true
    }

    fn write(self, state: &mut State, w: usize, i: usize, kind: WriteKind) -> bool {
        let Some(slot) = state.workers[w].slots[i].clone() else {
            return false;
        };
        let claim = slot.claim.clone();
        let tag = slot.seq;
        let op = match kind {
            WriteKind::Start => Op::Start { claim },
            WriteKind::Heartbeat => Op::Heartbeat { claim, tag },
            WriteKind::Release => Op::Release { claim },
            WriteKind::Complete => Op::Complete { claim, tag },
        };
        let Outcome::Write(outcome) = state.store.apply(&op) else {
            return false;
        };
        let found = state.ghost.wrote(kind, slot.claim.task, slot.seq, outcome);
        state
            .broken
            .extend(found.into_iter().map(|(invariant, _)| invariant));
        if outcome == WriteOutcome::LeaseLost {
            state.stale_fenced = true;
        }
        state.workers[w].slots[i] = self.next_slot(slot, kind, outcome);
        true
    }

    fn requeue(state: &mut State) -> bool {
        if state.orphans.is_empty() {
            return false;
        }
        let orphan = state.orphans.remove(0);
        let task = orphan.task;
        let op = Op::Requeue {
            orphan,
            now_ms: state.tick,
        };
        if state.store.apply(&op) == Outcome::Requeued(true) {
            state.ghost.requeued(task);
        }
        true
    }
}

impl Model for ActivityClaim {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let workers = vec![
            Worker {
                incarnation: 1,
                slots: vec![None; self.slots],
                down: false,
            };
            self.workers
        ];
        let mut state = State {
            store: OracleStore::new(self.tasks, self.fencing, STALE_TICKS),
            tick: 0,
            workers,
            orphans: Vec::new(),
            ghost: Ghost::new(self.tasks),
            next_seq: 0,
            broken: BTreeSet::new(),
            stale_fenced: false,
        };
        // `dst::run` beats every worker before its first step.
        for w in 0..self.workers {
            Self::beat(&mut state, w);
        }
        vec![state]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        for (w, worker) in state.workers.iter().enumerate() {
            if worker.down {
                if worker.incarnation < self.incarnations {
                    actions.push(Action::Restart(w));
                }
                continue;
            }
            actions.push(Action::Beat(w));
            for (i, slot) in worker.slots.iter().enumerate() {
                match slot {
                    None if state.next_seq < self.max_claims => {
                        actions.extend((0..self.tasks).map(|task| Action::Claim {
                            worker: w,
                            slot: i,
                            task,
                        }));
                    }
                    None => {}
                    Some(slot) => {
                        let kinds: &[WriteKind] = if slot.started {
                            &[WriteKind::Heartbeat, WriteKind::Complete]
                        } else {
                            &[WriteKind::Start, WriteKind::Release]
                        };
                        actions.extend(kinds.iter().map(|kind| Action::Write {
                            worker: w,
                            slot: i,
                            kind: *kind,
                        }));
                    }
                }
            }
            if worker.incarnation < self.incarnations {
                actions.push(Action::Crash(w));
            }
        }
        actions.push(Action::Scan);
        actions.push(Action::Requeue);
        if state.tick < self.max_tick {
            actions.push(Action::Tick);
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        let changed = match action {
            Action::Beat(w) => {
                Self::beat(&mut state, w);
                true
            }
            Action::Claim { worker, slot, task } => Self::claim(&mut state, worker, slot, task),
            Action::Write { worker, slot, kind } => self.write(&mut state, worker, slot, kind),
            Action::Crash(w) => {
                // The claims die with the worker. Its rows stay `RUNNING`
                // until the reclaimer finds them.
                let worker = &mut state.workers[w];
                worker.slots.iter_mut().for_each(|slot| *slot = None);
                worker.down = true;
                true
            }
            Action::Restart(w) => {
                let worker = &mut state.workers[w];
                worker.incarnation += 1;
                worker.down = false;
                Self::beat(&mut state, w);
                true
            }
            Action::Scan => {
                if let Outcome::Orphans(orphans) =
                    state.store.apply(&Op::Scan { now_ms: state.tick })
                {
                    state.orphans = orphans;
                }
                true
            }
            Action::Requeue => Self::requeue(&mut state),
            Action::Tick => {
                state.tick += 1;
                true
            }
        };
        if !changed {
            return None;
        }
        if let Some((invariant, _)) = state.ghost.terminal_states(&state.store.rows()) {
            state.broken.insert(invariant);
        }
        (state != *last).then_some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        macro_rules! always {
            ($($invariant:ident),+) => {
                vec![$(Property::<Self>::always(
                    Invariant::$invariant.name(),
                    |_, s: &State| !s.broken.contains(&Invariant::$invariant),
                )),+]
            };
        }
        let mut properties = always!(
            AtMostOneTerminal,
            TerminalStateHasOneEvent,
            TerminalByCurrentClaim,
            OwnerWritesByCurrentClaim,
            HeartbeatByCurrentClaim,
            ClaimIdsAreUnique
        );
        properties.push(Property::sometimes(TASK_COMPLETED, |_, s: &State| {
            s.store
                .rows()
                .iter()
                .any(|row| row.state == TaskState::Completed)
        }));
        properties.push(Property::sometimes(STALE_WRITE_FENCED, |_, s: &State| {
            s.stale_fenced
        }));
        properties.push(Property::sometimes(
            STALE_OWNER_AFTER_FINISH,
            |_, s: &State| stale_owner_after_finish(s),
        ));
        properties
    }
}

/// A task row reaches `COMPLETED`.
const TASK_COMPLETED: &str = "TaskCompleted";
/// The guard rejects a write by a claim that the row no longer holds.
const STALE_WRITE_FENCED: &str = "StaleWriteFenced";
/// A stale owner still runs after a later claim finished the task.
const STALE_OWNER_AFTER_FINISH: &str = "StaleOwnerAfterFinish";
/// The invariant that `ActivityClaimReach.cfg` must break. It is the
/// negation of [`STALE_OWNER_AFTER_FINISH`].
const TLA_REACH: &str = "NoStaleOwnerAfterFinish";

/// A started claim still runs on a row that another claim finished.
///
/// A terminal write empties its own slot, so any started slot on a
/// completed row belongs to another claim.
fn stale_owner_after_finish(state: &State) -> bool {
    let rows = state.store.rows();
    state
        .workers
        .iter()
        .flat_map(|worker| worker.slots.iter().flatten())
        .any(|slot| slot.started && rows[slot.claim.task].state == TaskState::Completed)
}

#[test]
fn the_claim_epoch_keeps_every_invariant() {
    let checker = check_all(ActivityClaim::design());
    checker.assert_properties();
    assert!(
        checker.unique_state_count() > 1_000,
        "the bound is too small to mean much: {} states",
        checker.unique_state_count()
    );
}

#[test]
fn the_claim_epoch_keeps_every_invariant_across_two_rows() {
    let checker = check_all(ActivityClaim::two_tasks());
    for invariant in Invariant::ALL {
        checker.assert_no_discovery(invariant.name());
    }
}

#[test]
fn the_pre_fix_guard_lets_a_stale_owner_finish_the_task() {
    let checker = check_until(
        ActivityClaim::pre_fix(),
        Invariant::TerminalByCurrentClaim.name(),
    );
    let path = checker.assert_any_discovery(Invariant::TerminalByCurrentClaim.name());
    let actions = path.into_actions();
    // The trace of issue #1789: claim, reclaim, claim again, stale finish.
    let requeue = actions.iter().position(|a| *a == Action::Requeue);
    let complete = actions.iter().rposition(|a| {
        matches!(
            a,
            Action::Write {
                kind: WriteKind::Complete,
                ..
            }
        )
    });
    let claims = actions
        .iter()
        .filter(|a| matches!(a, Action::Claim { .. }))
        .count();
    assert!(
        requeue.is_some() && complete > requeue && claims == 2,
        "unexpected counter-example: {actions:?}"
    );
}

#[test]
fn the_pre_fix_guard_breaks_the_other_owner_invariants() {
    for invariant in [
        Invariant::OwnerWritesByCurrentClaim,
        Invariant::HeartbeatByCurrentClaim,
        Invariant::ClaimIdsAreUnique,
    ] {
        let checker = check_until(ActivityClaim::pre_fix(), invariant.name());
        checker.assert_any_discovery(invariant.name());
    }
}

#[test]
fn every_dst_invariant_is_a_property() {
    let names: BTreeSet<&str> = ActivityClaim::design()
        .properties()
        .iter()
        .map(|p| p.name)
        .collect();
    for invariant in Invariant::ALL {
        assert!(names.contains(invariant.name()), "{invariant} is unchecked");
    }
}

/// The fence that a TLC config of `ActivityClaim.tla` sets.
///
/// A constant that this model does not map is a new defect. It fails here,
/// so a red row cannot pass on its invariant name alone.
fn tla_fencing(config: &str) -> Fencing {
    let mut fencing = None;
    for (name, value) in tla_constants(config) {
        match (name.as_str(), value.as_str()) {
            ("Workers" | "NoWorker" | "MaxClaims", _) => {}
            ("Fenced", "TRUE") => fencing = Some(Fencing::ClaimEpoch),
            ("Fenced", "FALSE") => fencing = Some(Fencing::StateOnly),
            _ => panic!("{config}: the Stateright model has no match for {name} = {value}"),
        }
    }
    fencing.unwrap_or_else(|| panic!("{config} sets no Fenced"))
}

#[test]
fn every_tla_counter_example_has_a_stateright_match() {
    let rows = tla_red_rows("ActivityClaim");
    assert!(!rows.is_empty(), "models.txt lists ActivityClaim rows");
    for (config, invariant) in rows {
        let model = ActivityClaim {
            fencing: tla_fencing(&config),
            ..ActivityClaim::design()
        };
        let property = Invariant::parse(&invariant).map_or_else(
            |_| {
                assert_eq!(
                    invariant, TLA_REACH,
                    "{config}: a new reachability row needs a Stateright witness"
                );
                STALE_OWNER_AFTER_FINISH
            },
            Invariant::name,
        );
        let checker = check_until(model, property);
        let path = checker.assert_any_discovery(property);
        assert!(
            !path.into_actions().is_empty(),
            "{config}: the initial state reaches {property}"
        );
    }
}
