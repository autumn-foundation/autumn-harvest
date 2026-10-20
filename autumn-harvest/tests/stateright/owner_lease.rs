//! The fenced owner lease of issue #2250, on [`OracleOwnerStore`].
//!
//! The store half is the oracle that stage 1a (issue #2251) must match on
//! Postgres. The owner half copies the scheduler of `dst::owner`. That
//! scheduler draws one action per step from a seed. This model takes every
//! enabled action, so it needs no nemesis bias and no stall: a worker may
//! skip any number of steps.
//!
//! The actions, defects and invariants follow `formal/tla/OwnerLease.tla`.
//! Each red `OwnerLease` row of `formal/tla/models.txt` must fail here on
//! the same invariant.

use std::collections::{BTreeMap, BTreeSet};

use autumn_harvest::dst::owner::{
    Decision, Defect, Epoch, Event, Fence, OracleOwnerStore, OwnerInvariant, OwnerStore, Renewal,
    SignalId, WorkerId,
};
use stateright::{Checker, Model, Property};

use super::{cfg_lines, check_all, check_until, deep, read, tla_red_rows};

/// The bounds of one check.
#[derive(Debug, Clone, Copy)]
struct OwnerLease {
    /// The design rule that the check breaks.
    defect: Defect,
    /// The number of workers.
    workers: usize,
    /// The number of signals that clients send.
    signals: usize,
    /// The last epoch that an acquisition or a renewal may reach.
    max_epoch: u64,
}

impl OwnerLease {
    /// The bound of `OwnerLeaseReach.cfg` and every defect config: 2
    /// workers, 2 signals and 3 epochs. The nightly run checks 4 epochs.
    fn design() -> Self {
        Self {
            defect: Defect::None,
            workers: 2,
            signals: 2,
            max_epoch: if deep() { 4 } else { 3 },
        }
    }

    fn with_defect(defect: Defect) -> Self {
        Self {
            defect,
            ..Self::design()
        }
    }
}

/// How far a decision got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Phase {
    /// The decision read its state. The commit has not checked the fence.
    Read,
    /// The commit passed the fence and holds the lock.
    Locked,
}

/// A decision in flight and the ghost tenure that began it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InFlight {
    decision: Decision,
    tenure: u64,
    phase: Phase,
}

/// The owner side of one worker process.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct Worker {
    /// The epoch that the worker thinks it holds.
    pinned: Option<Epoch>,
    /// The ghost tenure of its last acquisition.
    tenure: u64,
    /// The history length in its resident state.
    cache: usize,
    /// Signals acknowledged from memory (`memory-ack` only).
    memory: BTreeSet<SignalId>,
}

/// A fault of issue #2250 that a behavior reached. The tags match the
/// `seen` set of `formal/tla/OwnerLease.tla`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Tag {
    /// A stale owner reaches its commit after a takeover and is fenced.
    StaleCommit,
    /// A worker takes the lease back while its old decision is in flight.
    Flap,
    /// The holder fenced its own commit.
    SelfFence,
    /// The lease ends during an append, and the append lands.
    AppendAfterExpiry,
    /// A renewal lands while a commit holds the fence lock.
    RenewInFlight,
    /// The epoch bumps while an acknowledged signal waits.
    AckBeforeBump,
}

impl Tag {
    /// The tags of `WitnessTags` in the TLA+ spec: one behavior of the
    /// design must reach every one.
    const WITNESSES: [Self; 5] = [
        Self::StaleCommit,
        Self::Flap,
        Self::AppendAfterExpiry,
        Self::RenewInFlight,
        Self::AckBeforeBump,
    ];
}

/// The store, the owner side and the ghost state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    store: OracleOwnerStore,
    workers: Vec<Worker>,
    /// Every decision in flight, in the order it began.
    decisions: Vec<InFlight>,
    /// The ghost tenure of the owner row: the number of acquisitions.
    tenure: u64,
    /// Ghost: the worker that took each epoch.
    owners: BTreeMap<Epoch, WorkerId>,
    history: Vec<Event>,
    sent: BTreeSet<SignalId>,
    acked: BTreeSet<SignalId>,
    seen: BTreeSet<Tag>,
}

/// One step. A decision is named by its index in [`State::decisions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Acquire(WorkerId),
    Release(WorkerId),
    Renew(WorkerId),
    Begin(WorkerId, SignalId),
    Lock(usize),
    Commit(usize),
    Write(SignalId),
    Ack(SignalId),
    Receive(WorkerId, SignalId),
    Expire,
}

impl State {
    /// The signals that the store holds durably: pending or consumed.
    fn written(&self) -> BTreeSet<SignalId> {
        let mut written: BTreeSet<SignalId> = self.store.pending().into_iter().collect();
        written.extend(self.store.consumed());
        written
    }

    /// `worker` holds a fence lock now.
    fn holds_lock(&self, worker: WorkerId) -> bool {
        self.decisions
            .iter()
            .any(|d| d.decision.worker == worker && d.phase == Phase::Locked)
    }
}

impl OwnerLease {
    /// `worker` may begin a decision in its current tenure.
    fn may_begin(self, state: &State, worker: WorkerId) -> bool {
        let tenure = state.workers[worker.0].tenure;
        let running = state
            .decisions
            .iter()
            .filter(|d| d.decision.worker == worker && d.tenure == tenure)
            .count();
        let limit = if self.defect == Defect::Pipeline {
            2
        } else {
            1
        };
        running < limit
    }

    /// The worker drops its ownership. Its decisions in flight stay.
    fn unload(self, state: &mut State, worker: WorkerId) {
        let keep_cache = self.defect == Defect::KeepCache;
        let worker = &mut state.workers[worker.0];
        worker.pinned = None;
        worker.memory.clear();
        if !keep_cache {
            worker.cache = 0;
        }
    }

    fn acquire(self, state: &mut State, id: WorkerId) -> bool {
        let Some(epoch) = state.store.acquire(id) else {
            return false;
        };
        state.tenure += 1;
        state.owners.insert(epoch, id);
        let pending = state.store.pending();
        if state.acked.iter().any(|s| pending.contains(s)) {
            state.seen.insert(Tag::AckBeforeBump);
        }
        // The owner loads its state only after the acquisition commits.
        let reload = state.store.consumed().len();
        let keep_cache = self.defect == Defect::KeepCache;
        let tenure = state.tenure;
        let worker = &mut state.workers[id.0];
        worker.pinned = Some(epoch);
        worker.tenure = tenure;
        if !keep_cache {
            worker.cache = reload;
        }
        true
    }

    fn release(self, state: &mut State, id: WorkerId) {
        let Some(epoch) = state.workers[id.0].pinned else {
            return;
        };
        // A lost release unloads too.
        state.store.release(id, epoch);
        self.unload(state, id);
    }

    fn renew(self, state: &mut State, id: WorkerId) {
        let Some(pinned) = state.workers[id.0].pinned else {
            return;
        };
        match state.store.renew(id, pinned) {
            Renewal::Renewed(epoch) => {
                if state.holds_lock(id) {
                    state.seen.insert(Tag::RenewInFlight);
                }
                if epoch != pinned {
                    state.owners.insert(epoch, id);
                }
                state.workers[id.0].pinned = Some(epoch);
            }
            Renewal::Busy => {}
            Renewal::Lost => self.unload(state, id),
        }
    }

    fn begin(state: &mut State, id: WorkerId, signal: SignalId) {
        let worker = &state.workers[id.0];
        let Some(epoch) = worker.pinned else {
            return;
        };
        let decision = Decision {
            worker: id,
            epoch,
            base: worker.cache,
            signal,
        };
        state.decisions.push(InFlight {
            decision,
            tenure: worker.tenure,
            phase: Phase::Read,
        });
    }

    fn lock(self, state: &mut State, index: usize) {
        let flight = state.decisions[index];
        let decision = flight.decision;
        let id = decision.worker;
        match state.store.lock(&decision) {
            Fence::Held => state.decisions[index].phase = Phase::Locked,
            Fence::Fenced => {
                let row = state.store.row();
                let tag = if row.holder != Some(id) {
                    Tag::StaleCommit
                } else if flight.tenure == state.tenure {
                    Tag::SelfFence
                } else {
                    Tag::Flap
                };
                state.seen.insert(tag);
                state.decisions.remove(index);
                if state.workers[id.0].pinned == Some(decision.epoch) {
                    self.unload(state, id);
                }
            }
        }
    }

    fn commit(state: &mut State, index: usize) {
        let decision = state.decisions.remove(index).decision;
        if !state.store.row().live {
            state.seen.insert(Tag::AppendAfterExpiry);
        }
        let index = state.store.consumed().len();
        let at = state.store.commit(&decision);
        state.history.push(Event {
            by: decision.worker,
            pinned: decision.epoch,
            at,
            base: decision.base,
            index,
            signal: decision.signal,
        });
        let worker = &mut state.workers[decision.worker.0];
        worker.cache = index + 1;
        worker.memory.remove(&decision.signal);
    }
}

impl Model for OwnerLease {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            store: OracleOwnerStore::new(self.defect),
            workers: vec![Worker::default(); self.workers],
            decisions: Vec::new(),
            tenure: 0,
            owners: BTreeMap::new(),
            history: Vec::new(),
            sent: BTreeSet::new(),
            acked: BTreeSet::new(),
            seen: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        let row = state.store.row();
        let takeable = row.holder.is_none() || !row.live;
        let pending = state.store.pending();
        let written = state.written();
        let memory_ack = self.defect == Defect::MemoryAck;
        let signals = || (0..self.signals).map(SignalId);
        for (index, worker) in state.workers.iter().enumerate() {
            let id = WorkerId(index);
            if worker.pinned.is_none() {
                if takeable && row.epoch.0 < self.max_epoch {
                    actions.push(Action::Acquire(id));
                }
                continue;
            }
            actions.push(Action::Renew(id));
            if row.holder == Some(id) {
                actions.push(Action::Release(id));
            }
            if self.may_begin(state, id) {
                let mut candidates: BTreeSet<SignalId> = pending.iter().copied().collect();
                candidates.extend(worker.memory.iter().copied());
                actions.extend(candidates.into_iter().map(|s| Action::Begin(id, s)));
            }
            if memory_ack {
                actions.extend(
                    signals()
                        .filter(|s| !state.sent.contains(s))
                        .map(|s| Action::Receive(id, s)),
                );
            }
        }
        for (index, flight) in state.decisions.iter().enumerate() {
            actions.push(match flight.phase {
                Phase::Read => Action::Lock(index),
                Phase::Locked => Action::Commit(index),
            });
        }
        for signal in signals() {
            if !memory_ack && !state.sent.contains(&signal) {
                actions.push(Action::Write(signal));
            }
            if written.contains(&signal) && !state.acked.contains(&signal) {
                actions.push(Action::Ack(signal));
            }
        }
        if row.holder.is_some() && row.live {
            actions.push(Action::Expire);
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        match action {
            Action::Acquire(id) => {
                if !self.acquire(&mut state, id) {
                    return None;
                }
            }
            Action::Release(id) => self.release(&mut state, id),
            Action::Renew(id) => self.renew(&mut state, id),
            Action::Begin(id, signal) => Self::begin(&mut state, id, signal),
            Action::Lock(index) => self.lock(&mut state, index),
            Action::Commit(index) => Self::commit(&mut state, index),
            Action::Write(signal) => {
                state.store.write_signal(signal);
                state.sent.insert(signal);
            }
            Action::Ack(signal) => {
                state.acked.insert(signal);
            }
            Action::Receive(id, signal) => {
                state.workers[id.0].memory.insert(signal);
                state.sent.insert(signal);
                state.acked.insert(signal);
            }
            Action::Expire => state.store.expire(),
        }
        (state != *last).then_some(state)
    }

    fn within_boundary(&self, state: &State) -> bool {
        // A `renew-bumps` renewal can pass the bound. Acquire stops at it.
        state.store.row().epoch.0 <= self.max_epoch
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(OwnerInvariant::OneOwnerPerEpoch.as_str(), |_, s: &State| {
                s.history.iter().all(|e| s.owners.get(&e.at) == Some(&e.by))
            }),
            Property::always(
                OwnerInvariant::CommitAtPinnedEpoch.as_str(),
                |_, s: &State| s.history.iter().all(|e| e.pinned == e.at),
            ),
            Property::always(
                OwnerInvariant::DecisionOnFreshHistory.as_str(),
                |_, s: &State| s.history.iter().all(|e| e.base == e.index),
            ),
            Property::always(OwnerInvariant::NoDuplicateEvent.as_str(), |_, s: &State| {
                let log = s.store.consumed();
                log.iter().collect::<BTreeSet<_>>().len() == log.len()
            }),
            Property::always(
                OwnerInvariant::AckAfterDurableWrite.as_str(),
                |_, s: &State| s.acked.is_subset(&s.written()),
            ),
            Property::always(
                OwnerInvariant::NoAckedSignalLost.as_str(),
                |_, s: &State| no_acked_signal_lost(s),
            ),
            Property::always(
                OwnerInvariant::OwnerNeverFencesItself.as_str(),
                |_, s: &State| !s.seen.contains(&Tag::SelfFence),
            ),
            Property::sometimes(EVERY_NEMESIS_SEEN, |_, s: &State| {
                Tag::WITNESSES.iter().all(|tag| s.seen.contains(tag))
            }),
        ]
    }
}

/// One behavior reaches every nemesis.
const EVERY_NEMESIS_SEEN: &str = "EveryNemesisSeen";
/// The invariant that `OwnerLeaseReach.cfg` must break. It is the negation
/// of [`EVERY_NEMESIS_SEEN`].
const TLA_REACH: &str = "SomeNemesisUnseen";

/// An acknowledged signal is pending, consumed, or in the memory of the
/// worker that holds the row at its epoch. A stale owner can never commit
/// from memory, so its memory does not count.
fn no_acked_signal_lost(state: &State) -> bool {
    let row = state.store.row();
    let memory = row
        .holder
        .map(|h| &state.workers[h.0])
        .filter(|w| w.pinned == Some(row.epoch))
        .map(|w| &w.memory);
    let pending = state.store.pending();
    let consumed = state.store.consumed();
    state.acked.iter().all(|s| {
        pending.contains(s) || consumed.contains(s) || memory.is_some_and(|m| m.contains(s))
    })
}

/// The `Defect` value that a TLC config sets.
fn tla_defect(config: &str) -> Defect {
    let cfg = read(&format!("formal/tla/{config}"));
    let value = cfg_lines(&cfg)
        .find_map(|l| l.strip_prefix("Defect = "))
        .unwrap_or_else(|| panic!("{config} sets no Defect"));
    Defect::parse(value.trim_matches('"')).unwrap_or_else(|e| panic!("{config}: {e}"))
}

#[test]
fn the_design_keeps_every_invariant_and_reaches_every_nemesis() {
    let checker = check_all(OwnerLease::design());
    checker.assert_properties();
}

#[test]
fn every_tla_defect_fails_here_on_the_same_invariant() {
    let rows = tla_red_rows("OwnerLease");
    let mut defects = BTreeSet::new();
    for (config, invariant) in rows {
        let defect = tla_defect(&config);
        if defect == Defect::None {
            assert_eq!(
                invariant, TLA_REACH,
                "{config}: a new design row needs a Stateright witness"
            );
            continue;
        }
        let property = OwnerInvariant::parse(&invariant)
            .unwrap_or_else(|e| panic!("{config}: {e}"))
            .as_str();
        let checker = check_until(OwnerLease::with_defect(defect), property);
        let path = checker.assert_any_discovery(property);
        assert!(
            !path.into_actions().is_empty(),
            "{config}: the initial state breaks {property}"
        );
        defects.insert(defect);
    }
    let want: BTreeSet<Defect> = Defect::ALL
        .into_iter()
        .filter(|d| *d != Defect::None)
        .collect();
    assert_eq!(defects, want, "every defect has a TLA+ counter-example");
}

#[test]
fn the_no_fence_counter_example_is_a_stale_owner_commit() {
    let property = OwnerInvariant::OneOwnerPerEpoch.as_str();
    let checker = check_until(OwnerLease::with_defect(Defect::NoFence), property);
    let path = checker.assert_any_discovery(property);
    let last = path.last_state();
    let event = last
        .history
        .iter()
        .find(|e| last.owners.get(&e.at) != Some(&e.by))
        .expect("an event by a worker that did not take its epoch");
    // The trace of `formal-methods.md`: the worker pinned an older epoch.
    assert!(event.pinned < event.at, "{event:?}");
}

#[test]
fn every_simulator_invariant_is_a_property() {
    let names: BTreeSet<&str> = OwnerLease::design()
        .properties()
        .iter()
        .map(|p| p.name)
        .collect();
    for invariant in OwnerInvariant::ALL {
        assert!(
            names.contains(invariant.as_str()),
            "{} is unchecked",
            invariant.as_str()
        );
    }
}
