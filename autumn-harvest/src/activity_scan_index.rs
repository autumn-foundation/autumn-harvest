//! A position index for the terminal scans of `HistoryMatcher` (issues
//! #2330 and #2338).
//!
//! `HistoryMatcher::scan_activity_terminal` looks for the terminal event of
//! one activity. A plain walk visits every event between the schedule and
//! the terminal. In a fan-out of N activities, each walk steps over the
//! later schedules and the consumed completions, so a replay costs O(N²).
//! The timer fire scan, the child terminal scan and the external stash scan
//! have the same problem.
//!
//! This index keeps the positions that can change the result of a scan.
//! Once a scan leaves its walk, it reads only those positions. All other
//! events are steps that have no effect. `HistoryMatcher::activity_scan_role` sorts each
//! event into one role, and `docs/performance-match-activity-fanout.md`
//! records the measurement.

use std::collections::HashMap;
use std::ops::Range;

use crate::event::WorkflowEvent;
use crate::types::{
    ActivityExecId, ExecutionId, ExternalAwaitId, ExternalCancelId, ExternalSignalId, TimerId,
};

/// The cost of indexing one event, in walk steps.
///
/// Callgrind gives about 1,100 instructions to index an event and later
/// drop its entry. A walk step costs about 90. The factor rounds up, so a
/// small fan-out stays on the walk.
const BUILD_COST_IN_WALK_STEPS: usize = 16;

/// What one event means to the terminal scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanRole<'e> {
    /// An `ActivityScheduled`. It stops the scan for its own activity. For
    /// any other scan, it is an interleaved command.
    Scheduled(ActivityExecId),
    /// An activity terminal event. It ends the scan for its own activity.
    /// Any other scan steps over it.
    Terminal(ActivityExecId),
    /// An `ActivityStarted` or `ActivityHeartbeat`. Every scan steps over
    /// it. A settle of its own activity consumes it.
    Progress(ActivityExecId),
    /// A `TimerFired`. It ends the scan for its own timer. Any other scan
    /// steps over it.
    Fired(&'e TimerId),
    /// A `ChildWorkflowCompleted` or `ChildWorkflowFailed`. The child scan
    /// ends at it for its own child. Any other scan steps over it.
    ChildTerminal(ExecutionId),
    /// A command that the scan steps over. The first one becomes the
    /// cursor after the scan.
    Interleaved,
    /// A terminal event of an external signal, cancel or await. A scan
    /// stashes it, as a barrier. The external stash scan resolves it.
    ExternalTerminal(ExternalKey),
    /// An event that the scan stashes, or an event that stops it.
    Barrier,
    /// An event that the scan steps over.
    Pass,
}

/// The kind and the id of one external call (issue #2346).
///
/// A terminal event resolves only a stash entry with the same key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalKey {
    Signal(ExternalSignalId),
    Cancel(ExternalCancelId),
    Await(ExternalAwaitId),
}

/// What an activity or timer scan looks for (issue #2338).
#[derive(Debug, Clone, Copy)]
pub enum ScanTarget<'t> {
    /// The schedule or the terminal event of this activity.
    Activity(ActivityExecId),
    /// The fire of the timer with this id.
    Timer(&'t str),
}

impl ScanTarget<'_> {
    /// Whether an event with `role` ends a scan for this target.
    #[inline]
    pub fn stops_at(self, role: ScanRole<'_>) -> bool {
        match (self, role) {
            (Self::Activity(target), ScanRole::Scheduled(id) | ScanRole::Terminal(id)) => {
                id == target
            }
            (Self::Timer(target), ScanRole::Fired(id)) => id.as_str() == target,
            _ => false,
        }
    }
}

/// The positions of one activity.
#[derive(Debug, Default)]
struct ActivityPositions {
    /// The `ActivityScheduled` and terminal events, in history order.
    stops: Vec<usize>,
    /// The `ActivityStarted` and `ActivityHeartbeat` events, in history
    /// order.
    progress: Vec<usize>,
}

/// The positions of the events that can change a terminal scan.
#[derive(Debug, Default)]
pub struct ActivityScanIndex {
    /// The length of the history prefix that the index holds.
    indexed: usize,
    /// The events that scans and settles walked one by one.
    walked: usize,
    activities: HashMap<ActivityExecId, ActivityPositions>,
    /// The `TimerFired` events of each timer. A workflow can reuse a timer
    /// id, so one timer can have many fires. A skip list then passes over
    /// the consumed ones.
    fires: HashMap<String, SkipList>,
    /// The terminal events of each child, in history order.
    children: HashMap<ExecutionId, Vec<usize>>,
    interleaved: SkipList,
    barriers: SkipList,
    /// The external terminal events. Each one is a barrier too.
    external_terminals: SkipList,
    /// The external terminal events of each key, in history order (issue
    /// #2346). The ids are unique, so a key has few terminals.
    external_by_key: HashMap<ExternalKey, Vec<usize>>,
}

impl ActivityScanIndex {
    /// Adds the events after the indexed prefix.
    ///
    /// The events of a matcher change in two ways. `append_consumed` adds
    /// consumed events at the end. `cut_view` hides a tail, and
    /// `restore_view` brings back the same tail. So the prefix that the
    /// index holds never changes. The caller ignores a position at or after
    /// the visible length, which covers a cut.
    pub fn extend(
        &mut self,
        events: &[WorkflowEvent],
        role: impl for<'e> Fn(&'e WorkflowEvent) -> ScanRole<'e>,
    ) {
        for (position, event) in events.iter().enumerate().skip(self.indexed) {
            #[cfg(test)]
            steps::count();
            match role(event) {
                ScanRole::Scheduled(id) => {
                    self.activities.entry(id).or_default().stops.push(position);
                    self.interleaved.push(position);
                }
                ScanRole::Terminal(id) => {
                    self.activities.entry(id).or_default().stops.push(position);
                }
                ScanRole::Progress(id) => {
                    self.activities
                        .entry(id)
                        .or_default()
                        .progress
                        .push(position);
                }
                ScanRole::Fired(id) => {
                    if let Some(fires) = self.fires.get_mut(id.as_str()) {
                        fires.push(position);
                    } else {
                        let mut fires = SkipList::default();
                        fires.push(position);
                        self.fires.insert(id.as_str().to_owned(), fires);
                    }
                }
                ScanRole::ChildTerminal(id) => self.children.entry(id).or_default().push(position),
                ScanRole::Interleaved => self.interleaved.push(position),
                ScanRole::ExternalTerminal(key) => {
                    self.barriers.push(position);
                    self.external_terminals.push(position);
                    self.external_by_key.entry(key).or_default().push(position);
                }
                ScanRole::Barrier => self.barriers.push(position),
                ScanRole::Pass => {}
            }
        }
        self.indexed = self.indexed.max(events.len());
    }

    /// Counts `events` that a scan or a settle walked one by one.
    pub const fn note_walk(&mut self, events: usize) {
        self.walked = self.walked.saturating_add(events);
    }

    /// Whether the index exists.
    pub const fn is_built(&self) -> bool {
        self.indexed > 0
    }

    /// Whether the index pays off for a history of `len` events.
    ///
    /// An index build costs about [`BUILD_COST_IN_WALK_STEPS`] walk steps for
    /// each event. Until the walks cost that much, a walk is cheaper. This
    /// rule keeps a small fan-out on the walk, at its old cost. It also
    /// bounds the extra cost of a large fan-out by a constant factor.
    pub const fn pays_off(&self, len: usize) -> bool {
        self.is_built() || self.walked >= len.saturating_mul(BUILD_COST_IN_WALK_STEPS)
    }

    /// The first unconsumed stop of `target` at or after `from`.
    ///
    /// The stops of an activity are its `ActivityScheduled` and terminal
    /// events. Activity ids are unique, so an activity has few stops, and a
    /// plain walk is enough. The stops of a timer are its fires. Their skip
    /// list passes over the consumed fires of a reused timer id.
    pub fn first_stop(
        &mut self,
        target: ScanTarget<'_>,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        match target {
            ScanTarget::Activity(id) => {
                let stops = &self.activities.get(&id)?.stops;
                let start = stops.partition_point(|&p| p < from);
                stops[start..].iter().copied().find(|&p| !consumed(p))
            }
            ScanTarget::Timer(id) => self.fires.get_mut(id)?.first_unconsumed(from, consumed),
        }
    }

    /// The first terminal event of `child` at or after `from`.
    ///
    /// The child scan does not skip a consumed terminal, so this query does
    /// not either.
    pub fn first_child_terminal(&self, child: ExecutionId, from: usize) -> Option<usize> {
        let terminals = self.children.get(&child)?;
        terminals
            .get(terminals.partition_point(|&p| p < from))
            .copied()
    }

    /// The first unconsumed external terminal event at or after `from`.
    pub fn first_external_terminal(
        &mut self,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        self.external_terminals.first_unconsumed(from, consumed)
    }

    /// The first unconsumed external terminal event of `key` at or after
    /// `from` (issue #2346).
    pub fn first_external_terminal_of(
        &self,
        key: ExternalKey,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        let terminals = self.external_by_key.get(&key)?;
        let start = terminals.partition_point(|&p| p < from);
        terminals[start..].iter().copied().find(|&p| !consumed(p))
    }

    /// The first unconsumed interleaved command at or after `from`.
    pub fn first_interleaved(
        &mut self,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        self.interleaved.first_unconsumed(from, consumed)
    }

    /// The first unconsumed barrier at or after `from`.
    pub fn first_barrier(
        &mut self,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        self.barriers.first_unconsumed(from, consumed)
    }

    /// The progress events of `id` inside `range`.
    pub fn progress(&self, id: ActivityExecId, range: Range<usize>) -> &[usize] {
        let Some(positions) = self.activities.get(&id) else {
            return &[];
        };
        let progress = &positions.progress;
        let start = progress.partition_point(|&p| p < range.start);
        let end = progress.partition_point(|&p| p < range.end);
        &progress[start..end.max(start)]
    }
}

/// A sorted list of positions that skips the consumed ones.
///
/// `next[k]` links entry `k` to entry `k` or a later one. Every entry that
/// a link jumps over is consumed. The last slot of `next` is the end, and
/// it links to itself. A consumed event never becomes unconsumed again, so
/// a link never goes stale. A query follows and shortens the links, so a
/// run of consumed entries costs about one step after its first query.
/// `formal/verus/src/scan_skip.rs` proves that the links stay sound.
#[derive(Debug, Default)]
struct SkipList {
    positions: Vec<usize>,
    /// It is empty until the first push. It then holds one slot more than
    /// `positions`.
    next: Vec<usize>,
}

impl SkipList {
    /// Adds `position`. It must be greater than every position in the list.
    fn push(&mut self, position: usize) {
        if self.next.is_empty() {
            self.next.push(0);
        }
        self.positions.push(position);
        self.next.push(self.positions.len());
    }

    /// The first position at or after `from` that `consumed` does not hold.
    fn first_unconsumed(&mut self, from: usize, consumed: impl Fn(usize) -> bool) -> Option<usize> {
        if self.positions.is_empty() {
            return None;
        }
        let positions = &self.positions;
        let start = positions.partition_point(|&p| p < from);
        let entry = first_live(&mut self.next, start, |k| consumed(positions[k]));
        positions.get(entry).copied()
    }
}

/// The first entry at or after `from` that `is_dead` does not hold, or the
/// end slot.
///
/// `next` holds the skip links of [`SkipList`]. The loop follows them and
/// halves each path that it walks. It links a dead entry that it finds to
/// the next entry. `formal/verus/src/scan_skip.rs` proves that the result is
/// the first live entry or the end, and that the links stay sound.
fn first_live(next: &mut [usize], from: usize, is_dead: impl Fn(usize) -> bool) -> usize {
    let mut i = from;
    loop {
        let up = next[i];
        if up != i {
            next[i] = next[up];
            i = next[i];
        } else if i + 1 == next.len() || !is_dead(i) {
            return i;
        } else {
            next[i] = i + 1;
            i += 1;
        }
    }
}

/// A count of the steps that the terminal scans take on this thread (issues
/// #2330 and #2338). A scaling test reads it. A step is a consumed-set
/// probe, a child scan step, a settle step or an index build step.
#[cfg(test)]
pub mod steps {
    use std::cell::Cell;

    thread_local! {
        static STEPS: Cell<u64> = const { Cell::new(0) };
    }

    /// Counts one step.
    pub fn count() {
        STEPS.with(|steps| steps.set(steps.get() + 1));
    }

    /// The steps counted on this thread.
    pub fn total() -> u64 {
        STEPS.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plain walk that `first_unconsumed` replaces.
    fn first_by_walk(positions: &[usize], from: usize, consumed: &[usize]) -> Option<usize> {
        positions
            .iter()
            .copied()
            .find(|p| *p >= from && !consumed.contains(p))
    }

    #[test]
    fn an_empty_list_has_no_entry() {
        let mut list = SkipList::default();
        assert_eq!(list.first_unconsumed(0, |_| false), None);
    }

    #[test]
    fn a_query_skips_consumed_entries_and_stops_at_the_end() {
        let mut list = SkipList::default();
        for p in [2, 4, 6, 8] {
            list.push(p);
        }
        assert_eq!(list.first_unconsumed(0, |_| false), Some(2));
        assert_eq!(list.first_unconsumed(3, |_| false), Some(4));
        assert_eq!(list.first_unconsumed(0, |p| p < 7), Some(8));
        assert_eq!(list.first_unconsumed(9, |_| false), None);
        assert_eq!(list.first_unconsumed(0, |_| true), None);
    }

    #[test]
    fn a_push_after_a_query_keeps_the_links_sound() {
        let mut list = SkipList::default();
        list.push(1);
        list.push(3);
        assert_eq!(list.first_unconsumed(0, |_| true), None);
        list.push(5);
        assert_eq!(list.first_unconsumed(0, |p| p < 5), Some(5));
        assert_eq!(list.first_unconsumed(4, |p| p < 5), Some(5));
    }

    proptest::proptest! {
        /// Under any order of pushes, consumptions and queries, a query
        /// gives the result of a plain walk.
        #[test]
        fn a_query_matches_a_plain_walk(
            steps in proptest::collection::vec((0..4_u8, 0..1024_usize), 1..300),
        ) {
            let mut list = SkipList::default();
            let mut positions: Vec<usize> = Vec::new();
            let mut consumed = Vec::new();
            for (kind, value) in steps {
                // Consume and query inside the pushed range, so that long
                // dead runs form.
                let span = positions.last().map_or(1, |last| last + 2);
                match kind {
                    0 => {
                        let position = positions.last().map_or(0, |last| last + 1 + value % 3);
                        positions.push(position);
                        list.push(position);
                    }
                    1 if !positions.is_empty() => {
                        consumed.push(positions[value % positions.len()]);
                    }
                    _ => {
                        let from = value % span;
                        let got = list.first_unconsumed(from, |p| consumed.contains(&p));
                        proptest::prop_assert_eq!(got, first_by_walk(&positions, from, &consumed));
                    }
                }
            }
        }
    }

    #[test]
    fn progress_returns_the_positions_inside_the_range() {
        let id = ActivityExecId::new();
        let mut index = ActivityScanIndex::default();
        let events: Vec<WorkflowEvent> = (0..6)
            .map(|_| WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::Value::Null,
            })
            .collect();
        index.extend(&events, |_| ScanRole::Progress(id));
        assert_eq!(index.progress(id, 1..4), &[1, 2, 3]);
        assert_eq!(index.progress(id, 4..4), &[] as &[usize]);
        assert_eq!(index.progress(ActivityExecId::new(), 0..6), &[] as &[usize]);
    }

    /// A role function for the tests below. It sorts the events that the
    /// tests use.
    fn test_role(event: &WorkflowEvent) -> ScanRole<'_> {
        match event {
            WorkflowEvent::TimerFired { timer_id } => ScanRole::Fired(timer_id),
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
            | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                ScanRole::ChildTerminal(*child_id)
            }
            WorkflowEvent::ExternalSignalDelivered { signal_id } => {
                ScanRole::ExternalTerminal(ExternalKey::Signal(*signal_id))
            }
            _ => ScanRole::Barrier,
        }
    }

    fn fired(id: &str) -> WorkflowEvent {
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new(id),
        }
    }

    #[test]
    fn the_index_keeps_the_fires_of_each_timer() {
        let mut index = ActivityScanIndex::default();
        index.extend(&[fired("t"), fired("u"), fired("t")], test_role);
        let t = ScanTarget::Timer("t");
        assert_eq!(index.first_stop(t, 0, |_| false), Some(0));
        assert_eq!(index.first_stop(t, 1, |_| false), Some(2));
        assert_eq!(index.first_stop(t, 0, |p| p == 0), Some(2));
        assert_eq!(index.first_stop(ScanTarget::Timer("v"), 0, |_| false), None);
    }

    #[test]
    fn a_child_query_returns_each_terminal_in_order() {
        let child = ExecutionId::new();
        let events = [
            WorkflowEvent::ChildWorkflowFailed {
                child_id: child,
                error: "e".into(),
                error_type: None,
                details: None,
                non_retryable: None,
            },
            WorkflowEvent::ChildWorkflowCompleted {
                child_id: child,
                output: serde_json::Value::Null,
            },
        ];
        let mut index = ActivityScanIndex::default();
        index.extend(&events, test_role);
        assert_eq!(index.first_child_terminal(child, 0), Some(0));
        assert_eq!(index.first_child_terminal(child, 1), Some(1));
        assert_eq!(index.first_child_terminal(child, 2), None);
        assert_eq!(index.first_child_terminal(ExecutionId::new(), 0), None);
    }

    #[test]
    fn an_external_terminal_is_a_barrier_too() {
        let delivered = WorkflowEvent::ExternalSignalDelivered {
            signal_id: ExternalSignalId::new(),
        };
        let mut index = ActivityScanIndex::default();
        index.extend(&[fired("t"), delivered.clone(), delivered], test_role);
        assert_eq!(index.first_external_terminal(0, |_| false), Some(1));
        assert_eq!(index.first_external_terminal(0, |p| p == 1), Some(2));
        assert_eq!(index.first_barrier(0, |_| false), Some(1));
        assert_eq!(index.first_external_terminal(3, |_| false), None);
    }

    #[test]
    fn a_key_query_returns_the_terminals_of_its_key_only() {
        let a = ExternalSignalId::new();
        let b = ExternalSignalId::new();
        let delivered = |signal_id| WorkflowEvent::ExternalSignalDelivered { signal_id };
        let mut index = ActivityScanIndex::default();
        index.extend(&[delivered(a), delivered(b), delivered(a)], test_role);
        let key = ExternalKey::Signal(a);
        assert_eq!(index.first_external_terminal_of(key, 0, |_| false), Some(0));
        assert_eq!(index.first_external_terminal_of(key, 1, |_| false), Some(2));
        assert_eq!(
            index.first_external_terminal_of(key, 0, |p| p == 0),
            Some(2)
        );
        assert_eq!(index.first_external_terminal_of(key, 3, |_| false), None);
        let other = ExternalKey::Signal(ExternalSignalId::new());
        assert_eq!(index.first_external_terminal_of(other, 0, |_| false), None);
        let cancel = ExternalKey::Cancel(ExternalCancelId::new());
        assert_eq!(index.first_external_terminal_of(cancel, 0, |_| false), None);
    }

    #[test]
    fn a_target_stops_at_its_own_events_only() {
        let a = ActivityExecId::new();
        let timer = TimerId::new("t");
        let other = TimerId::new("u");
        let activity = ScanTarget::Activity(a);
        assert!(activity.stops_at(ScanRole::Scheduled(a)));
        assert!(activity.stops_at(ScanRole::Terminal(a)));
        assert!(!activity.stops_at(ScanRole::Progress(a)));
        assert!(!activity.stops_at(ScanRole::Terminal(ActivityExecId::new())));
        assert!(!activity.stops_at(ScanRole::Fired(&timer)));
        let t = ScanTarget::Timer("t");
        assert!(t.stops_at(ScanRole::Fired(&timer)));
        assert!(!t.stops_at(ScanRole::Fired(&other)));
        assert!(!t.stops_at(ScanRole::Terminal(a)));
    }
}
