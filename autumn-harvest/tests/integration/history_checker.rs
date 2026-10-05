//! Client-history recording and a linearizability checker (issue #1829).
//!
//! This is a small Jepsen-style checker in the style of Porcupine. A test
//! records each client operation as an invocation and a completion. The
//! checker then searches for one total order of the operations. That order
//! must respect real time, and a sequential model must accept it.
//!
//! An operation completes in one of three ways, as in Jepsen:
//!
//! - `ok`: the operation took effect and returned an output.
//! - `fail`: the operation did not take effect. The checker drops it.
//! - `info`: the outcome is unknown, for example after a crash. The checker
//!   may place it at any point after its invocation, with any outcome the
//!   model allows for an unknown output.
//!
//! The search is per key (P-compositionality), so the cost stays small.
//! Linearizability is a local property, so a history is linearizable when
//! each per-key sub-history is.
//!
//! The module is pure and has no database. The crash suites call it with
//! their recorded histories, and the self-tests below feed it forged
//! violations.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// A sequential specification of one key.
pub trait Model {
    /// The abstract state of one key.
    type State: Clone + Eq + Hash + Debug;
    /// An operation request.
    type Input: Clone + Debug;
    /// An operation response.
    type Output: Clone + Debug;

    /// The state of a key before any operation.
    fn init(&self) -> Self::State;

    /// Every state the key can move to when `input` runs on `state`.
    ///
    /// `output` is `None` for an `info` operation. An empty result means the
    /// model rejects the step. The checker already treats an `info`
    /// operation as one that may never have happened, so a model need not
    /// return the unchanged state for it.
    ///
    /// The checker places an `ok` step that leaves the state unchanged at
    /// once, with no search. That is valid only if the same step, in a later
    /// state, is also unchanged or is rejected. Both models here obey it.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Vec<Self::State>;
}

/// How an operation completed.
#[derive(Debug, Clone)]
pub enum Outcome<O> {
    /// The operation took effect and returned this output.
    Ok(O),
    /// The operation did not take effect.
    Fail,
    /// The outcome is unknown.
    Info,
}

/// One recorded client operation.
#[derive(Debug, Clone)]
pub struct Operation<I, O> {
    /// The client that issued the operation.
    pub process: usize,
    /// The key the operation reads or writes.
    pub key: String,
    /// The request.
    pub input: I,
    /// The logical time of the invocation.
    pub invoked: u64,
    /// The logical time of the completion. `None` until it completes. For
    /// an `info` operation it is a bound: after this time the operation can
    /// no longer take effect. `None` means no bound is known.
    pub completed: Option<u64>,
    /// The completion. `None` until it completes.
    pub outcome: Option<Outcome<O>>,
}

/// A handle to an operation that is in flight.
#[derive(Debug, Clone, Copy)]
#[must_use = "complete the operation with ok, fail or info"]
pub struct Pending(usize);

/// A thread-safe history recorder.
///
/// Time is a logical counter. Each invocation and each completion takes the
/// next value. A test takes the invocation time before it sends a request
/// and the completion time after it gets the response. The counter order is
/// then the real-time order.
#[derive(Debug)]
pub struct Recorder<I, O> {
    clock: AtomicU64,
    ops: Mutex<Vec<Operation<I, O>>>,
}

impl<I: Clone + Debug, O: Clone + Debug> Default for Recorder<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone + Debug, O: Clone + Debug> Recorder<I, O> {
    /// An empty history.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            clock: AtomicU64::new(0),
            ops: Mutex::new(Vec::new()),
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    /// Record an invocation. Call it before the request is sent.
    pub fn invoke(&self, process: usize, key: impl Into<String>, input: I) -> Pending {
        let mut ops = self.ops.lock().unwrap();
        let invoked = self.tick();
        ops.push(Operation {
            process,
            key: key.into(),
            input,
            invoked,
            completed: None,
            outcome: None,
        });
        Pending(ops.len() - 1)
    }

    fn complete(&self, pending: Pending, outcome: Outcome<O>) {
        let mut ops = self.ops.lock().unwrap();
        let completed = self.tick();
        let op = &mut ops[pending.0];
        assert!(op.outcome.is_none(), "operation completed twice: {op:?}");
        if !matches!(outcome, Outcome::Info) {
            op.completed = Some(completed);
        }
        op.outcome = Some(outcome);
        drop(ops);
    }

    /// Record that the operation took effect with `output`.
    pub fn ok(&self, pending: Pending, output: O) {
        self.complete(pending, Outcome::Ok(output));
    }

    /// Record that the operation did not take effect.
    pub fn fail(&self, pending: Pending) {
        self.complete(pending, Outcome::Fail);
    }

    /// Record that the outcome of the operation is unknown.
    pub fn info(&self, pending: Pending) {
        self.complete(pending, Outcome::Info);
    }

    /// Bound every open `info` operation at the current time.
    ///
    /// Call it only when nothing those operations started can still run,
    /// for example after the test waits until the server has no busy
    /// session. A later read then constrains them.
    pub fn bound_open_infos(&self) {
        let mut ops = self.ops.lock().unwrap();
        let now = self.tick();
        for op in ops.iter_mut() {
            if matches!(op.outcome, Some(Outcome::Info)) && op.completed.is_none() {
                op.completed = Some(now);
            }
        }
        drop(ops);
    }

    /// A copy of the history. An operation still in flight counts as `info`.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Operation<I, O>> {
        let mut ops = self.ops.lock().unwrap().clone();
        for op in &mut ops {
            if op.outcome.is_none() {
                op.outcome = Some(Outcome::Info);
            }
        }
        ops
    }
}

/// A history that no valid order explains.
#[derive(Debug)]
pub struct Violation {
    /// The key whose sub-history fails.
    pub key: String,
    /// The operations of that key, in invocation order.
    pub operations: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "history of key {:?} is not linearizable: {}",
            self.key, self.operations
        )
    }
}

/// A history split by key. Each group keeps invocation order.
type KeyGroups<I, O> = BTreeMap<String, Vec<Operation<I, O>>>;

/// The stack of the search thread before the per-operation part.
const SEARCH_STACK_BASE: usize = 1024 * 1024;

/// The search stack per operation of the longest key. One recursion level
/// takes about 1 KiB in a debug build, so this leaves a wide margin.
const SEARCH_STACK_PER_OP: usize = 8 * 1024;

/// Check that `history` is linearizable against `model`.
///
/// The search recurses once per placed operation. A crash suite with fast
/// ticks records thousands of operations for one key, which can overflow
/// the 2 MiB stack of a test thread. So the search runs on its own thread,
/// with a stack sized to the longest key.
///
/// # Errors
///
/// Returns the first key whose sub-history has no valid order.
pub fn check<M>(model: &M, history: &[Operation<M::Input, M::Output>]) -> Result<(), Violation>
where
    M: Model + Sync,
    M::Input: Sync,
    M::Output: Sync,
{
    let groups = group_by_key(history);
    let longest = groups.values().map(Vec::len).max().unwrap_or(0);
    let stack = SEARCH_STACK_BASE.saturating_add(longest.saturating_mul(SEARCH_STACK_PER_OP));
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("history-check".into())
            .stack_size(stack)
            .spawn_scoped(scope, || check_groups(model, &groups))
            .expect("spawn the history check thread")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// The search of [`check`] over each key group, on the calling thread.
fn check_groups<M: Model>(
    model: &M,
    groups: &KeyGroups<M::Input, M::Output>,
) -> Result<(), Violation> {
    for (key, ops) in groups {
        // A failed operation has no effect, so no order needs to hold it.
        let ops: Vec<_> = ops
            .iter()
            .filter(|op| !matches!(op.outcome, Some(Outcome::Fail)))
            .cloned()
            .collect();
        let mut search = Search {
            model,
            ops: &ops,
            required: ops
                .iter()
                .filter(|op| !matches!(op.outcome, Some(Outcome::Info)))
                .count(),
            seen: HashSet::new(),
        };
        let done = vec![0_u64; ops.len().div_ceil(64)];
        if !search.linearize(&done, &model.init(), 0) {
            return Err(Violation {
                key: key.clone(),
                operations: format!("{ops:?}"),
            });
        }
    }
    Ok(())
}

/// A depth-first search for a valid order of one key, after Wing and Gong.
/// `seen` memoizes each (linearized set, state) pair that has no valid
/// order, as Lowe suggests. The same pair always gives the same answer.
///
/// An `info` operation may never have happened, so the search can leave it
/// out. It places an `info` operation only where the operation changes the
/// state. Without this rule, each subset of no-effect `info` operations is
/// a new search node, and a crash-heavy violation takes exponential time.
struct Search<'a, M: Model> {
    model: &'a M,
    ops: &'a [Operation<M::Input, M::Output>],
    /// The number of operations that are not `info`. The search must place
    /// each one.
    required: usize,
    seen: HashSet<(Vec<u64>, M::State)>,
}

impl<M: Model> Search<'_, M> {
    /// `count` is the number of placed operations that are not `info`.
    fn linearize(&mut self, done: &[u64], state: &M::State, count: usize) -> bool {
        if count == self.required {
            return true;
        }
        if !self.seen.insert((done.to_vec(), state.clone())) {
            return false;
        }
        let is_done = |i: usize| done[i / 64] & (1 << (i % 64)) != 0;
        let is_info = |i: usize| matches!(self.ops[i].outcome, Some(Outcome::Info));
        // An operation can go next only if no open operation completed
        // before its invocation. An `info` operation may never happen, so
        // its bound does not hold back other operations.
        let deadline = (0..self.ops.len())
            .filter(|&i| !is_done(i) && !is_info(i))
            .filter_map(|i| self.ops[i].completed)
            .min()
            .unwrap_or(u64::MAX);
        // The latest invocation already placed. A bounded `info` operation
        // cannot take effect after an operation invoked past its bound.
        let latest = (0..self.ops.len())
            .filter(|&i| is_done(i))
            .map(|i| self.ops[i].invoked)
            .max();
        for i in 0..self.ops.len() {
            let op = &self.ops[i];
            if is_done(i) || op.invoked > deadline {
                continue;
            }
            let (output, info) = match &op.outcome {
                Some(Outcome::Ok(output)) => (Some(output), false),
                _ => (None, true),
            };
            if info && op.completed.is_some_and(|bound| latest > Some(bound)) {
                continue;
            }
            let mut next_done = done.to_vec();
            next_done[i / 64] |= 1 << (i % 64);
            let nexts = self.model.step(state, &op.input, output);
            // A minimal `ok` step that changes nothing can go first in any
            // valid order (see `Model::step`), so it needs no branch.
            if !info && nexts.len() == 1 && nexts[0] == *state {
                return self.linearize(&next_done, state, count + 1);
            }
            for next in nexts {
                if info && next == *state {
                    continue;
                }
                let placed = count + usize::from(!info);
                if self.linearize(&next_done, &next, placed) {
                    return true;
                }
            }
        }
        false
    }
}

/// Check `history` and panic with the violation and `context` if it fails.
pub fn assert_linearizable<M>(model: &M, history: &[Operation<M::Input, M::Output>], context: &str)
where
    M: Model + Sync,
    M::Input: Sync,
    M::Output: Sync,
{
    if let Err(violation) = check(model, history) {
        panic!("{context}: {violation}");
    }
}

// ── Models ──────────────────────────────────────────────────────────────────

/// A request against a request-scoped idempotency key (issue #808).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartInput {
    /// Start a run. `candidate` is the execution id this request proposes.
    Start {
        /// The execution id the request proposes for a fresh run.
        candidate: uuid::Uuid,
    },
    /// Read every execution that the key owns.
    Read,
}

/// A response for [`StartInput`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartOutput {
    /// This request created the run.
    Started(uuid::Uuid),
    /// An earlier request created the run. This request returned it.
    Deduplicated(uuid::Uuid),
    /// The executions that the key owns.
    Read(Vec<uuid::Uuid>),
}

/// Start idempotency: one key creates at most one run, and every response
/// names that run. A fresh run must carry the candidate id of its request.
/// A read must return the run ids sorted and without duplicates.
#[derive(Debug, Clone, Copy, Default)]
pub struct StartIdempotency;

impl Model for StartIdempotency {
    type State = Option<uuid::Uuid>;
    type Input = StartInput;
    type Output = StartOutput;

    fn init(&self) -> Self::State {
        None
    }

    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Vec<Self::State> {
        match (input, output, state) {
            (StartInput::Start { candidate }, Some(StartOutput::Started(id)), None)
                if id == candidate =>
            {
                vec![Some(*id)]
            }
            (StartInput::Start { .. }, Some(StartOutput::Deduplicated(id)), Some(run))
                if id == run =>
            {
                vec![Some(*run)]
            }
            (StartInput::Start { candidate }, None, None) => vec![None, Some(*candidate)],
            (StartInput::Start { .. }, None, Some(run)) => vec![Some(*run)],
            (StartInput::Read, Some(StartOutput::Read(ids)), None) if ids.is_empty() => {
                vec![None]
            }
            (StartInput::Read, Some(StartOutput::Read(ids)), Some(run)) if ids == &[*run] => {
                vec![Some(*run)]
            }
            (StartInput::Read, None, _) => vec![*state],
            _ => Vec::new(),
        }
    }
}

/// A request against one schedule slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FireInput {
    /// One scheduler tick that may fire the slot.
    Fire,
    /// Read every execution that the slot owns.
    Read,
    /// Read the slot after recovery. The slot must have fired.
    FinalRead,
}

/// A response for [`FireInput`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FireOutput {
    /// The tick returned. It does not report which slots it fired.
    Ticked,
    /// The executions that the slot owns.
    Read(Vec<uuid::Uuid>),
}

/// The state of one schedule slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SlotState {
    /// No run exists for the slot.
    Unfired,
    /// A tick fired the slot, but no read has seen the run yet.
    FiredUnseen,
    /// A read saw this run.
    Fired(uuid::Uuid),
}

/// Exactly-once schedule fires: a slot gets at most one run, the run never
/// changes, and after recovery the slot has fired. A read must return the
/// run ids without duplicates.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExactlyOnceFire;

impl Model for ExactlyOnceFire {
    type State = SlotState;
    type Input = FireInput;
    type Output = FireOutput;

    fn init(&self) -> Self::State {
        SlotState::Unfired
    }

    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: Option<&Self::Output>,
    ) -> Vec<Self::State> {
        match (input, output) {
            // A tick may skip the slot, for example under a live claim held
            // by a crashed peer. So a tick may or may not fire it.
            (FireInput::Fire, None | Some(FireOutput::Ticked)) => match state {
                SlotState::Unfired => vec![SlotState::Unfired, SlotState::FiredUnseen],
                fired => vec![fired.clone()],
            },
            (FireInput::Read | FireInput::FinalRead, Some(FireOutput::Read(ids))) => {
                match (state, ids.as_slice()) {
                    (SlotState::Unfired, []) if *input == FireInput::Read => {
                        vec![SlotState::Unfired]
                    }
                    (SlotState::FiredUnseen, [id]) => vec![SlotState::Fired(*id)],
                    (SlotState::Fired(run), [id]) if run == id => vec![state.clone()],
                    _ => Vec::new(),
                }
            }
            (FireInput::Read | FireInput::FinalRead, None) => vec![state.clone()],
            _ => Vec::new(),
        }
    }
}

/// Group a history by key, in key order. Each group keeps invocation order.
fn group_by_key<I: Clone, O: Clone>(history: &[Operation<I, O>]) -> KeyGroups<I, O> {
    let mut groups: KeyGroups<I, O> = BTreeMap::new();
    for op in history {
        groups.entry(op.key.clone()).or_default().push(op.clone());
    }
    groups
}

// ── Self-tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    /// The length of the long history in the stack test.
    const LONG_HISTORY: usize = 5_000;

    fn start(n: u128) -> StartInput {
        StartInput::Start { candidate: id(n) }
    }

    #[test]
    fn sequential_dedup_history_is_linearizable() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.ok(a, StartOutput::Started(id(1)));
        let b = h.invoke(1, "k", start(2));
        h.ok(b, StartOutput::Deduplicated(id(1)));
        let r = h.invoke(2, "k", StartInput::Read);
        h.ok(r, StartOutput::Read(vec![id(1)]));
        assert!(check(&StartIdempotency, &h.snapshot()).is_ok());
    }

    #[test]
    fn two_creators_for_one_key_are_rejected() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        let b = h.invoke(1, "k", start(2));
        h.ok(a, StartOutput::Started(id(1)));
        h.ok(b, StartOutput::Started(id(2)));
        let err = check(&StartIdempotency, &h.snapshot()).unwrap_err();
        assert_eq!(err.key, "k");
    }

    #[test]
    fn a_read_that_sees_two_runs_is_rejected() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.ok(a, StartOutput::Started(id(1)));
        let r = h.invoke(1, "k", StartInput::Read);
        h.ok(r, StartOutput::Read(vec![id(1), id(2)]));
        assert!(check(&StartIdempotency, &h.snapshot()).is_err());
    }

    /// A dedup completes before the invocation of the creator. No order puts the
    /// creator first, so the history is not linearizable.
    #[test]
    fn a_real_time_inversion_is_rejected() {
        let h = Recorder::new();
        let b = h.invoke(1, "k", start(2));
        h.ok(b, StartOutput::Deduplicated(id(1)));
        let a = h.invoke(0, "k", start(1));
        h.ok(a, StartOutput::Started(id(1)));
        assert!(check(&StartIdempotency, &h.snapshot()).is_err());
    }

    /// The same two operations, but concurrent. The checker may put the
    /// creator first, so the history is linearizable.
    #[test]
    fn concurrent_operations_may_reorder() {
        let h = Recorder::new();
        let b = h.invoke(1, "k", start(2));
        let a = h.invoke(0, "k", start(1));
        h.ok(b, StartOutput::Deduplicated(id(1)));
        h.ok(a, StartOutput::Started(id(1)));
        assert!(check(&StartIdempotency, &h.snapshot()).is_ok());
    }

    /// A crashed creator may have committed. A later dedup to its candidate
    /// is then valid.
    #[test]
    fn an_info_operation_may_take_effect() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.info(a);
        let b = h.invoke(1, "k", start(2));
        h.ok(b, StartOutput::Deduplicated(id(1)));
        assert!(check(&StartIdempotency, &h.snapshot()).is_ok());
    }

    /// A crashed creator may also have rolled back.
    #[test]
    fn an_info_operation_may_have_no_effect() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.info(a);
        let r = h.invoke(1, "k", StartInput::Read);
        h.ok(r, StartOutput::Read(vec![]));
        assert!(check(&StartIdempotency, &h.snapshot()).is_ok());
    }

    /// A failed operation has no effect. A run with its candidate id is a
    /// violation.
    #[test]
    fn a_failed_operation_has_no_effect() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.fail(a);
        let r = h.invoke(1, "k", StartInput::Read);
        h.ok(r, StartOutput::Read(vec![id(1)]));
        assert!(check(&StartIdempotency, &h.snapshot()).is_err());
    }

    #[test]
    fn keys_are_checked_independently() {
        let h = Recorder::new();
        let a = h.invoke(0, "k1", start(1));
        let b = h.invoke(1, "k2", start(2));
        h.ok(a, StartOutput::Started(id(1)));
        h.ok(b, StartOutput::Started(id(2)));
        let c = h.invoke(2, "k2", start(3));
        h.ok(c, StartOutput::Started(id(3)));
        let err = check(&StartIdempotency, &h.snapshot()).unwrap_err();
        assert_eq!(err.key, "k2", "only k2 has two creators");
    }

    #[test]
    fn crashed_fire_then_recovery_is_exactly_once() {
        let h = Recorder::new();
        let crash = h.invoke(0, "s", FireInput::Fire);
        h.info(crash);
        let r = h.invoke(1, "s", FireInput::Read);
        h.ok(r, FireOutput::Read(vec![]));
        let peer = h.invoke(1, "s", FireInput::Fire);
        h.ok(peer, FireOutput::Ticked);
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(7)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_ok());
    }

    /// The search recurses once per placed operation. A crash suite with
    /// fast ticks records thousands of operations for one key. The check
    /// must not overflow the 2 MiB stack of a test thread (issue #1829).
    #[test]
    fn a_long_history_does_not_overflow_a_test_thread_stack() {
        let h = Recorder::new();
        let first = h.invoke(0, "s", FireInput::Fire);
        h.ok(first, FireOutput::Ticked);
        for _ in 0..LONG_HISTORY {
            let op = h.invoke(0, "s", FireInput::Fire);
            h.ok(op, FireOutput::Ticked);
        }
        let f = h.invoke(0, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(1)]));
        let history = h.snapshot();
        let result = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || check(&ExactlyOnceFire, &history).is_ok())
            .expect("spawn")
            .join()
            .expect("the check returns");
        assert!(result);
    }

    /// A crashed fire that has stopped cannot take effect later. Once the
    /// test bounds it, a run that appears after an empty read is a
    /// violation.
    #[test]
    fn a_bounded_info_operation_cannot_take_effect_after_a_later_read() {
        let h = Recorder::new();
        let crash = h.invoke(0, "s", FireInput::Fire);
        h.info(crash);
        h.bound_open_infos();
        let r = h.invoke(1, "s", FireInput::Read);
        h.ok(r, FireOutput::Read(vec![]));
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(1)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_err());
    }

    /// Without the bound the crashed fire may still be running, so the same
    /// history is valid.
    #[test]
    fn an_unbounded_info_operation_may_take_effect_after_a_later_read() {
        let h = Recorder::new();
        let crash = h.invoke(0, "s", FireInput::Fire);
        h.info(crash);
        let r = h.invoke(1, "s", FireInput::Read);
        h.ok(r, FireOutput::Read(vec![]));
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(1)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_ok());
    }

    /// A bounded crash may still have taken effect before its bound.
    #[test]
    fn a_bounded_info_operation_may_take_effect_before_its_bound() {
        let h = Recorder::new();
        let crash = h.invoke(0, "s", FireInput::Fire);
        h.info(crash);
        h.bound_open_infos();
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(1)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_ok());
    }

    #[test]
    fn a_double_fire_is_rejected() {
        let h = Recorder::new();
        let a = h.invoke(0, "s", FireInput::Fire);
        h.ok(a, FireOutput::Ticked);
        let r = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(r, FireOutput::Read(vec![id(1), id(2)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_err());
    }

    #[test]
    fn a_lost_fire_is_rejected() {
        let h = Recorder::new();
        let a = h.invoke(0, "s", FireInput::Fire);
        h.info(a);
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_err());
    }

    #[test]
    fn a_replaced_run_is_rejected() {
        let h = Recorder::new();
        let a = h.invoke(0, "s", FireInput::Fire);
        h.ok(a, FireOutput::Ticked);
        let r = h.invoke(1, "s", FireInput::Read);
        h.ok(r, FireOutput::Read(vec![id(1)]));
        let f = h.invoke(1, "s", FireInput::FinalRead);
        h.ok(f, FireOutput::Read(vec![id(2)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_err());
    }

    /// A run that a read sees after the history completes must come from a
    /// fire. A read cannot invent one.
    #[test]
    fn a_run_without_a_fire_is_rejected() {
        let h = Recorder::new();
        let f = h.invoke(1, "s", FireInput::Read);
        h.ok(f, FireOutput::Read(vec![id(1)]));
        assert!(check(&ExactlyOnceFire, &h.snapshot()).is_err());
    }

    /// The search must stay fast on a crash-heavy history. Forty concurrent
    /// `info` starts give a large search space without memoization.
    /// Many concurrent dedups change nothing. On a violation the search
    /// must not try each subset of them.
    #[test]
    fn a_violation_among_many_concurrent_dedups_checks_quickly() {
        let h = Recorder::new();
        let a = h.invoke(0, "k", start(1));
        h.ok(a, StartOutput::Started(id(1)));
        let pending: Vec<_> = (0..24)
            .map(|n| h.invoke(n + 1, "k", start(n as u128 + 10)))
            .collect();
        let r = h.invoke(99, "k", StartInput::Read);
        for p in pending {
            h.ok(p, StartOutput::Deduplicated(id(1)));
        }
        h.ok(r, StartOutput::Read(vec![id(2)]));
        let begin = std::time::Instant::now();
        assert!(check(&StartIdempotency, &h.snapshot()).is_err());
        assert!(begin.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn many_concurrent_info_operations_check_quickly() {
        let h = Recorder::new();
        let pending: Vec<_> = (0..40)
            .map(|n| h.invoke(n, "k", start(n as u128)))
            .collect();
        for p in pending {
            h.info(p);
        }
        let r = h.invoke(99, "k", StartInput::Read);
        h.ok(r, StartOutput::Read(vec![id(39)]));
        let begin = std::time::Instant::now();
        assert!(check(&StartIdempotency, &h.snapshot()).is_ok());
        assert!(begin.elapsed() < std::time::Duration::from_secs(5));
    }

    /// A violation must also check fast. Two creators plus thirty crashed
    /// starts make 2^30 subsets if the search tries each no-effect subset.
    #[test]
    fn a_violation_among_many_info_operations_checks_quickly() {
        let h = Recorder::new();
        let pending: Vec<_> = (0..30)
            .map(|n| h.invoke(n, "k", start(n as u128)))
            .collect();
        let a = h.invoke(98, "k", start(100));
        h.ok(a, StartOutput::Started(id(100)));
        let b = h.invoke(99, "k", start(101));
        h.ok(b, StartOutput::Started(id(101)));
        for p in pending {
            h.info(p);
        }
        let begin = std::time::Instant::now();
        assert!(check(&StartIdempotency, &h.snapshot()).is_err());
        assert!(begin.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn an_operation_still_in_flight_counts_as_info() {
        let h = Recorder::<StartInput, StartOutput>::new();
        let _ = h.invoke(0, "k", start(1));
        let snapshot = h.snapshot();
        assert!(matches!(snapshot[0].outcome, Some(Outcome::Info)));
        assert!(snapshot[0].completed.is_none());
    }

    #[test]
    fn group_by_key_keeps_invocation_order() {
        let h = Recorder::<StartInput, StartOutput>::new();
        let a = h.invoke(0, "b", start(1));
        let b = h.invoke(0, "a", start(2));
        h.fail(a);
        h.fail(b);
        let grouped = group_by_key(&h.snapshot());
        let keys: Vec<_> = grouped.keys().cloned().collect();
        assert_eq!(keys, ["a", "b"]);
    }
}
