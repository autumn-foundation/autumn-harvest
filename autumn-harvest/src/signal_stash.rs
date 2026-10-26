//! The signals that `HistoryMatcher` stashed before their wait ran (issue
//! #2349).
//!
//! A scan stashes each signal that it crosses. A later wait takes the first
//! stashed signal of its own name. A linear search of the stash costs O(n)
//! for each wait, so waits in reverse order of n stashed signals cost O(n²).
//! A large [`SignalStash`] keeps a queue of entries for each name. A wait
//! then finds its entry in O(1).

use std::collections::{HashMap, VecDeque, vec_deque};

use serde_json::Value;

/// The most slots that a stash searches with a plain scan.
///
/// A stash usually holds one or two signals. A name map costs a hash, and
/// often a clone of the name, for each push and each remove. That made a
/// loop of one activity and one signal cost 12% more. A scan of 16 short
/// names costs about as much as one hash.
const SMALL_STASH: usize = 16;

/// The slot count above which a stash keeps a name map.
#[allow(
    clippy::missing_const_for_fn,
    reason = "a test build reads a thread-local override"
)]
fn small_stash() -> usize {
    #[cfg(test)]
    if let Some(limit) = tests::small_stash_override() {
        return limit;
    }
    SMALL_STASH
}

/// One stashed signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StashedSignal {
    /// The signal name.
    pub name: String,
    /// The signal payload.
    pub payload: Value,
    /// The event index of its `SignalReceived`.
    pub index: usize,
}

/// The push numbers of the entries of each name, in push order.
type NameQueues = HashMap<String, VecDeque<usize>>;

/// The stashed signals in push order.
///
/// Each entry has a push number, and the numbers grow with each push. A
/// search visits the entries of one name in push order. That is the order of
/// the old linear search. A small stash scans its slots. A large one walks
/// the queue of the name.
#[derive(Default)]
pub struct SignalStash {
    /// The entries in push order. A remove leaves `None`. The deque drops
    /// each `None` that reaches its front or its back.
    slots: VecDeque<Option<StashedSignal>>,
    /// The push number of `slots[0]`.
    front: usize,
    /// The number of entries.
    live: usize,
    /// The queue of each name, once the stash has more than
    /// [`SMALL_STASH`] slots. The stash drops it when it empties.
    ///
    /// An empty queue keeps its key. A loop that stashes and takes one name
    /// then allocates no new key for each signal.
    names: Option<NameQueues>,
}

impl SignalStash {
    /// Whether the stash has no entry.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Adds a signal at the end.
    #[inline(always)]
    #[allow(
        clippy::inline_always,
        reason = "callgrind: a call costs about 50 instructions on each stash"
    )]
    pub fn push(&mut self, name: String, payload: Value, index: usize) {
        let push = self.front + self.slots.len();
        if let Some(names) = &mut self.names {
            queue_push(names, &name, push);
        }
        self.slots.push_back(Some(StashedSignal {
            name,
            payload,
            index,
        }));
        self.live += 1;
        if self.names.is_none() && self.slots.len() > small_stash() {
            let mut names = NameQueues::new();
            for (push, entry) in self.numbered() {
                queue_push(&mut names, &entry.name, push);
            }
            self.names = Some(names);
        }
    }

    /// The entries in push order.
    pub fn iter(&self) -> impl Iterator<Item = &StashedSignal> {
        self.slots.iter().flatten()
    }

    /// The entries in push order, each with its push number.
    pub fn numbered(&self) -> impl Iterator<Item = (usize, &StashedSignal)> {
        let front = self.front;
        self.slots
            .iter()
            .enumerate()
            .filter_map(move |(at, slot)| Some((front + at, slot.as_ref()?)))
    }

    /// Whether the stash has an entry of `name`.
    pub fn contains_name(&self, name: &str) -> bool {
        self.of_name(name).next().is_some()
    }

    /// The entries of `name` in push order, each with its push number.
    #[inline]
    pub fn of_name<'s>(&'s self, name: &'s str) -> OfName<'s> {
        self.of_name_from(name, self.front)
    }

    /// The entries of `name` with a push number of `from` or more, in push
    /// order, each with its push number.
    #[inline]
    pub fn of_name_from<'s>(&'s self, name: &'s str, from: usize) -> OfName<'s> {
        let walk = self.names.as_ref().map_or_else(
            || Walk::Scan(from.saturating_sub(self.front)),
            |names| {
                Walk::Queue(
                    names
                        .get(name)
                        .map(|queue| queue.range(queue.partition_point(|&push| push < from)..)),
                )
            },
        );
        OfName {
            stash: self,
            name,
            walk,
        }
    }

    /// The entry with push number `push`.
    #[inline]
    fn entry(&self, push: usize) -> Option<&StashedSignal> {
        self.slots.get(push.checked_sub(self.front)?)?.as_ref()
    }

    /// Removes and returns the entry with push number `push`.
    ///
    /// A wait takes the first entry of its name, so the push number is
    /// usually at the front of its queue. Waits in reverse order take the
    /// last slot. Both cost O(1).
    #[inline(always)]
    #[allow(
        clippy::inline_always,
        reason = "callgrind: a call costs about 50 instructions on each wait"
    )]
    pub fn remove(&mut self, push: usize) -> Option<StashedSignal> {
        let at = push.checked_sub(self.front)?;
        self.slots.get(at)?.as_ref()?;
        // An end slot pops at once, with no `None` left to trim.
        let entry = if at == 0 {
            self.front += 1;
            self.slots.pop_front()??
        } else if at + 1 == self.slots.len() {
            self.slots.pop_back()??
        } else {
            self.slots.get_mut(at)?.take()?
        };
        self.live -= 1;
        if let Some(queue) = self
            .names
            .as_mut()
            .and_then(|names| names.get_mut(entry.name.as_str()))
        {
            if queue.front() == Some(&push) {
                queue.pop_front();
            } else if let Ok(at) = queue.binary_search(&push) {
                queue.remove(at);
            }
        }
        while matches!(self.slots.front(), Some(None)) {
            self.slots.pop_front();
            self.front += 1;
        }
        while matches!(self.slots.back(), Some(None)) {
            self.slots.pop_back();
        }
        if self.slots.is_empty() {
            self.names = None;
        }
        Some(entry)
    }

    /// Removes and returns every entry with an event index of `horizon` or
    /// more, in push order.
    pub fn split_off_from(&mut self, horizon: usize) -> Vec<StashedSignal> {
        let hidden = self
            .numbered()
            .filter(|(_, entry)| entry.index >= horizon)
            .map(|(push, _)| push)
            .collect();
        self.remove_all(hidden)
    }

    /// Removes and returns every entry of `name` that `take` accepts, in
    /// push order.
    pub fn take_of_name(
        &mut self,
        name: &str,
        take: impl Fn(&StashedSignal) -> bool,
    ) -> Vec<StashedSignal> {
        let taken = self
            .of_name(name)
            .filter(|(_, entry)| take(entry))
            .map(|(push, _)| push)
            .collect();
        self.remove_all(taken)
    }

    /// Removes and returns the entries with the push numbers `pushes`, in
    /// that order.
    pub fn remove_all(&mut self, pushes: Vec<usize>) -> Vec<StashedSignal> {
        pushes
            .into_iter()
            .filter_map(|push| self.remove(push))
            .collect()
    }

    /// Adds `hidden` after the entries, then sorts every entry by event
    /// index.
    ///
    /// The sort is stable, so this is the order of the old
    /// `VecDeque::extend` and `sort_by_key`. The push numbers start again.
    pub fn restore(&mut self, hidden: Vec<StashedSignal>) {
        let mut all: Vec<StashedSignal> = std::mem::take(&mut self.slots)
            .into_iter()
            .flatten()
            .collect();
        all.extend(hidden);
        all.sort_by_key(|entry| entry.index);
        self.names = None;
        self.live = 0;
        for entry in all {
            self.push(entry.name, entry.payload, entry.index);
        }
    }

    /// Panics if the queues and the slots disagree.
    #[cfg(test)]
    pub fn check(&self) {
        assert!(
            !matches!(self.slots.front(), Some(None)) && !matches!(self.slots.back(), Some(None)),
            "an end slot is empty"
        );
        assert_eq!(self.live, self.iter().count(), "the entry count");
        let Some(names) = &self.names else {
            assert!(
                self.slots.len() <= small_stash(),
                "a large stash has no map"
            );
            return;
        };
        assert!(!self.slots.is_empty(), "an empty stash keeps its map");
        let mut queued = 0;
        for (name, queue) in names {
            assert!(
                queue.iter().zip(queue.iter().skip(1)).all(|(a, b)| a < b),
                "the queue of {name} is not in push order"
            );
            for &push in queue {
                let entry = self.entry(push).expect("a queued push number has an entry");
                assert_eq!(&entry.name, name, "push {push} is in the wrong queue");
            }
            queued += queue.len();
        }
        assert_eq!(queued, self.live, "an entry is missing from its queue");
    }
}

/// Adds `push` to the queue of `name`.
fn queue_push(names: &mut NameQueues, name: &str, push: usize) {
    if let Some(queue) = names.get_mut(name) {
        queue.push_back(push);
    } else {
        names.insert(name.to_owned(), VecDeque::from([push]));
    }
}

/// The entries of one name, from [`SignalStash::of_name`].
pub struct OfName<'s> {
    stash: &'s SignalStash,
    name: &'s str,
    walk: Walk<'s>,
}

/// How [`OfName`] finds the next entry.
enum Walk<'s> {
    /// The rest of the queue of the name, or `None` for a name with no
    /// queue.
    Queue(Option<vec_deque::Iter<'s, usize>>),
    /// The next slot to scan.
    Scan(usize),
}

impl<'s> Iterator for OfName<'s> {
    type Item = (usize, &'s StashedSignal);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.walk {
            Walk::Queue(queue) => {
                let push = *queue.as_mut()?.next()?;
                Some((push, self.stash.entry(push)?))
            }
            Walk::Scan(at) => {
                while let Some(slot) = self.stash.slots.get(*at) {
                    let push = self.stash.front + *at;
                    *at += 1;
                    if let Some(entry) = slot
                        && entry.name == self.name
                    {
                        return Some((push, entry));
                    }
                }
                None
            }
        }
    }
}

/// Prints the entries as `(name, payload, index)` in push order, the format
/// of the old `VecDeque`.
impl std::fmt::Debug for SignalStash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.iter()
                    .map(|entry| (&entry.name, &entry.payload, entry.index)),
            )
            .finish()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cell::Cell;

    thread_local! {
        /// The small stash limit that `small_stash` returns on this thread.
        static SMALL: Cell<Option<usize>> = const { Cell::new(None) };
    }

    /// The small stash limit that a test set on this thread, if any.
    pub fn small_stash_override() -> Option<usize> {
        SMALL.with(Cell::get)
    }

    /// Runs `run` with the small stash limit `limit`. `None` keeps
    /// [`SMALL_STASH`]. A limit of 0 gives every stash a name map.
    pub fn with_small_stash<R>(limit: Option<usize>, run: impl FnOnce() -> R) -> R {
        struct Restore(Option<usize>);
        impl Drop for Restore {
            fn drop(&mut self) {
                SMALL.with(|small| small.set(self.0));
            }
        }
        let _restore = Restore(SMALL.with(|small| small.replace(limit)));
        run()
    }

    /// One call on the stash and on the model.
    #[derive(Debug, Clone)]
    enum Op {
        Push(usize, usize),
        /// Removes the first entry of a name, as a wait does.
        TakeFirst(usize),
        /// Removes the last entry of a name.
        TakeLast(usize),
        /// Removes the entry at this position among all entries.
        RemoveAt(usize),
        /// Removes every entry of a name, as a push handler claim does.
        TakeAll(usize),
        Cut(usize),
        Restore,
    }

    fn op() -> impl Strategy<Value = Op> {
        let name = || 0..4_usize;
        prop_oneof![
            6 => (name(), 0..40_usize).prop_map(|(n, i)| Op::Push(n, i)),
            3 => name().prop_map(Op::TakeFirst),
            1 => name().prop_map(Op::TakeLast),
            2 => (0..12_usize).prop_map(Op::RemoveAt),
            1 => name().prop_map(Op::TakeAll),
            1 => (0..40_usize).prop_map(Op::Cut),
            1 => Just(Op::Restore),
        ]
    }

    type Model = VecDeque<(String, Value, usize)>;

    /// The entries of the stash as model rows.
    fn rows(stash: &SignalStash) -> Vec<(String, Value, usize)> {
        stash
            .iter()
            .map(|e| (e.name.clone(), e.payload.clone(), e.index))
            .collect()
    }

    /// The push number of the entry at `position` among all entries.
    fn push_at(stash: &SignalStash, position: usize) -> Option<usize> {
        stash.numbered().nth(position).map(|(push, _)| push)
    }

    /// Applies `ops` to a stash and to a plain `VecDeque` with linear
    /// searches, and compares them after every call.
    #[allow(clippy::too_many_lines)]
    fn run_against_the_model(ops: &[Op]) -> Result<(), TestCaseError> {
        let mut stash = SignalStash::default();
        let mut model = Model::new();
        let mut hidden: Option<(Vec<StashedSignal>, Model)> = None;
        for (step, op) in ops.iter().enumerate() {
            match op {
                Op::Push(n, index) => {
                    let payload = serde_json::json!(step);
                    stash.push(format!("n{n}"), payload.clone(), *index);
                    model.push_back((format!("n{n}"), payload, *index));
                }
                Op::TakeFirst(n) => {
                    let name = format!("n{n}");
                    let got = stash.of_name(&name).next().map(|(push, _)| push);
                    let got = got.and_then(|push| stash.remove(push));
                    let want = model
                        .iter()
                        .position(|(m, _, _)| *m == name)
                        .and_then(|at| model.remove(at));
                    prop_assert_eq!(
                        got.map(|e| (e.name, e.payload, e.index)),
                        want,
                        "step {}",
                        step
                    );
                }
                Op::TakeLast(n) => {
                    let name = format!("n{n}");
                    let got = stash.of_name(&name).last().map(|(push, _)| push);
                    let got = got.and_then(|push| stash.remove(push));
                    let want = model
                        .iter()
                        .rposition(|(m, _, _)| *m == name)
                        .and_then(|at| model.remove(at));
                    prop_assert_eq!(
                        got.map(|e| (e.name, e.payload, e.index)),
                        want,
                        "step {}",
                        step
                    );
                }
                Op::RemoveAt(position) => {
                    let got = push_at(&stash, *position).and_then(|push| stash.remove(push));
                    let want = model.remove(*position);
                    prop_assert_eq!(
                        got.map(|e| (e.name, e.payload, e.index)),
                        want,
                        "step {}",
                        step
                    );
                }
                Op::TakeAll(n) => {
                    let name = format!("n{n}");
                    let pushes = stash.of_name(&name).map(|(push, _)| push).collect();
                    let got: Vec<_> = stash
                        .remove_all(pushes)
                        .into_iter()
                        .map(|e| (e.name, e.payload, e.index))
                        .collect();
                    let (want, rest): (Model, Model) = std::mem::take(&mut model)
                        .into_iter()
                        .partition(|(m, _, _)| *m == name);
                    model = rest;
                    prop_assert_eq!(got, Vec::from(want), "step {}", step);
                }
                Op::Cut(horizon) => {
                    if hidden.is_none() {
                        let cut = stash.split_off_from(*horizon);
                        let (kept, gone): (Model, Model) = std::mem::take(&mut model)
                            .into_iter()
                            .partition(|(_, _, index)| index < horizon);
                        model = kept;
                        hidden = Some((cut, gone));
                    }
                }
                Op::Restore => {
                    if let Some((cut, gone)) = hidden.take() {
                        stash.restore(cut);
                        model.extend(gone);
                        model.make_contiguous().sort_by_key(|(_, _, index)| *index);
                    }
                }
            }
            stash.check();
            prop_assert_eq!(
                rows(&stash),
                Vec::from(model.clone()),
                "step {} {:?}",
                step,
                op
            );
            prop_assert_eq!(stash.is_empty(), model.is_empty());
            for n in 0..4 {
                let name = format!("n{n}");
                let want: Vec<_> = model
                    .iter()
                    .filter(|(m, _, _)| *m == name)
                    .cloned()
                    .collect();
                let got: Vec<_> = stash
                    .of_name(&name)
                    .map(|(_, e)| (e.name.clone(), e.payload.clone(), e.index))
                    .collect();
                prop_assert_eq!(&got, &want, "step {} {}", step, name);
                prop_assert_eq!(stash.contains_name(&name), !want.is_empty());
                // A lookup that resumes after each entry gives the same list.
                let mut resumed = Vec::new();
                let mut from = 0;
                while let Some((push, e)) = stash.of_name_from(&name, from).next() {
                    resumed.push((e.name.clone(), e.payload.clone(), e.index));
                    from = push + 1;
                }
                prop_assert_eq!(&resumed, &want, "step {} {} resumed", step, name);
            }
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// The stash gives the same entries as a plain `VecDeque` with linear
        /// searches, after every call. Each case runs with a scan, with a
        /// name map, and with a switch between them.
        #[test]
        fn the_signal_stash_matches_a_plain_deque(
            ops in proptest::collection::vec(op(), 1..80),
            limit in proptest::sample::select(vec![None, Some(0_usize), Some(3)]),
        ) {
            with_small_stash(limit, || run_against_the_model(&ops))?;
        }
    }

    #[test]
    fn a_remove_in_the_middle_keeps_the_order() {
        for limit in [None, Some(0)] {
            with_small_stash(limit, || {
                let mut stash = SignalStash::default();
                for (i, name) in ["a", "b", "a", "c"].into_iter().enumerate() {
                    stash.push(name.into(), serde_json::json!(i), i);
                }
                let second_a = stash.of_name("a").nth(1).map(|(push, _)| push);
                assert_eq!(
                    second_a
                        .and_then(|push| stash.remove(push))
                        .map(|e| e.index),
                    Some(2)
                );
                stash.check();
                let order: Vec<usize> = stash.iter().map(|e| e.index).collect();
                assert_eq!(order, vec![0, 1, 3], "{limit:?}");
            });
        }
    }

    /// A stash builds its name map when it outgrows the scan, and drops it
    /// when it empties.
    #[test]
    fn a_stash_builds_and_drops_its_name_map() {
        let mut stash = SignalStash::default();
        for i in 0..=SMALL_STASH {
            assert!(stash.names.is_none(), "{i} slots");
            stash.push(format!("n{i}"), Value::Null, i);
        }
        assert!(stash.names.is_some());
        stash.check();
        for i in 0..=SMALL_STASH {
            let name = format!("n{i}");
            let push = stash.of_name(&name).next().map(|(push, _)| push);
            assert_eq!(
                push.and_then(|push| stash.remove(push)).map(|e| e.index),
                Some(i)
            );
            stash.check();
        }
        assert!(stash.is_empty());
        assert!(stash.names.is_none());
    }
}
