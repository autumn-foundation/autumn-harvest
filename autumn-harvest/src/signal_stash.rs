//! The signals that `HistoryMatcher` stashed before their wait ran (issue
//! #2349).
//!
//! A scan stashes each signal that it crosses. A later wait takes the first
//! stashed signal of its own name. A linear search of the stash costs O(n)
//! for each wait, so waits in reverse order of n stashed signals cost O(n²).
//! [`SignalStash`] keeps a queue of entries for each name. A wait then finds
//! its entry in O(1).

use std::collections::{HashMap, VecDeque};

use serde_json::Value;

/// One stashed signal.
#[derive(Debug, Clone, PartialEq)]
pub struct StashedSignal {
    /// The signal name.
    pub name: String,
    /// The signal payload.
    pub payload: Value,
    /// The event index of its `SignalReceived`.
    pub index: usize,
}

/// The stashed signals in push order, with a queue for each name.
///
/// Each entry has a push number, and the numbers grow with each push. A
/// search walks the queue of one name, so it visits the entries of that name
/// in push order. That is the order of the old linear search.
#[derive(Default)]
pub struct SignalStash {
    /// The entries in push order. A remove leaves `None`. The deque drops
    /// each `None` that reaches its front or its back.
    slots: VecDeque<Option<StashedSignal>>,
    /// The push number of `slots[0]`.
    front: usize,
    /// The number of entries.
    live: usize,
    /// The push numbers of the entries of each name, in push order.
    ///
    /// An empty queue keeps its key. A loop that stashes and takes one name
    /// then allocates no new key for each signal.
    names: HashMap<String, VecDeque<usize>>,
}

impl SignalStash {
    /// Whether the stash has no entry.
    pub const fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Adds a signal at the end.
    pub fn push(&mut self, name: String, payload: Value, index: usize) {
        let push = self.front + self.slots.len();
        if let Some(queue) = self.names.get_mut(name.as_str()) {
            queue.push_back(push);
        } else {
            self.names.insert(name.clone(), VecDeque::from([push]));
        }
        self.slots.push_back(Some(StashedSignal {
            name,
            payload,
            index,
        }));
        self.live += 1;
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
        self.names.get(name).is_some_and(|queue| !queue.is_empty())
    }

    /// The entries of `name` with a push number of `from` or more, in push
    /// order, each with its push number.
    pub fn of_name_from<'s>(
        &'s self,
        name: &str,
        from: usize,
    ) -> impl Iterator<Item = (usize, &'s StashedSignal)> + 's {
        self.names
            .get(name)
            .into_iter()
            .flat_map(move |queue| queue.range(queue.partition_point(|&push| push < from)..))
            .filter_map(move |&push| Some((push, self.entry(push)?)))
    }

    /// The entries of `name` in push order, each with its push number.
    pub fn of_name<'s>(
        &'s self,
        name: &str,
    ) -> impl Iterator<Item = (usize, &'s StashedSignal)> + 's {
        self.names
            .get(name)
            .into_iter()
            .flatten()
            .filter_map(move |&push| Some((push, self.entry(push)?)))
    }

    /// The entry with push number `push`.
    fn entry(&self, push: usize) -> Option<&StashedSignal> {
        self.slots.get(push.checked_sub(self.front)?)?.as_ref()
    }

    /// Removes and returns the entry with push number `push`.
    ///
    /// A wait takes the first entry of its name, so the push number is
    /// usually at the front of its queue. Waits in reverse order take the
    /// last slot. Both cost O(1).
    pub fn remove(&mut self, push: usize) -> Option<StashedSignal> {
        let entry = self.slots.get_mut(push.checked_sub(self.front)?)?.take()?;
        self.live -= 1;
        if let Some(queue) = self.names.get_mut(entry.name.as_str()) {
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
        Some(entry)
    }

    /// Removes and returns every entry with an event index of `horizon` or
    /// more, in push order.
    pub fn split_off_from(&mut self, horizon: usize) -> Vec<StashedSignal> {
        let hidden: Vec<usize> = self
            .numbered()
            .filter(|(_, entry)| entry.index >= horizon)
            .map(|(push, _)| push)
            .collect();
        hidden
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
        self.names.values_mut().for_each(VecDeque::clear);
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
        let mut queued = 0;
        for (name, queue) in &self.names {
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
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        /// The stash gives the same entries as a plain `VecDeque` with linear
        /// searches, after every call.
        #[test]
        fn the_signal_stash_matches_a_plain_deque(ops in proptest::collection::vec(op(), 1..80)) {
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
                            "step {}", step
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
                            "step {}", step
                        );
                    }
                    Op::RemoveAt(position) => {
                        let got = push_at(&stash, *position).and_then(|push| stash.remove(push));
                        let want = model.remove(*position);
                        prop_assert_eq!(
                            got.map(|e| (e.name, e.payload, e.index)),
                            want,
                            "step {}", step
                        );
                    }
                    Op::TakeAll(n) => {
                        let name = format!("n{n}");
                        let pushes: Vec<usize> = stash.of_name(&name).map(|(push, _)| push).collect();
                        let got: Vec<_> = pushes
                            .into_iter()
                            .filter_map(|push| stash.remove(push))
                            .map(|e| (e.name, e.payload, e.index))
                            .collect();
                        let (want, rest): (Model, Model) =
                            std::mem::take(&mut model).into_iter().partition(|(m, _, _)| *m == name);
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
                prop_assert_eq!(rows(&stash), Vec::from(model.clone()), "step {} {:?}", step, op);
                prop_assert_eq!(stash.is_empty(), model.is_empty());
                for n in 0..4 {
                    let name = format!("n{n}");
                    let want: Vec<_> = model.iter().filter(|(m, _, _)| *m == name).cloned().collect();
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
        }
    }

    #[test]
    fn a_remove_in_the_middle_keeps_the_order() {
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
        assert_eq!(order, vec![0, 1, 3]);
    }

    #[test]
    fn an_empty_queue_keeps_its_key_and_reports_no_entry() {
        let mut stash = SignalStash::default();
        stash.push("a".into(), Value::Null, 0);
        let push = stash.of_name("a").next().map(|(push, _)| push);
        assert!(push.and_then(|push| stash.remove(push)).is_some());
        assert!(stash.is_empty());
        assert!(!stash.contains_name("a"));
        assert_eq!(stash.of_name("a").count(), 0);
        stash.check();
    }
}
