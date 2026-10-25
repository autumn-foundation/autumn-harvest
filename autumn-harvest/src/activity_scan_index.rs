//! A position index for the activity terminal scan (issue #2330).
//!
//! `HistoryMatcher::scan_activity_terminal` looks for the terminal event of
//! one activity. A plain walk visits every event between the schedule and
//! the terminal. In a fan-out of N activities, each walk steps over the
//! later schedules and the consumed completions, so a replay costs O(N²).
//!
//! This index keeps the positions that can change the result of a scan.
//! After its first `LINEAR_SCAN_BUDGET` events, the scan reads only those
//! positions. All other events are steps that have no effect. `HistoryMatcher::activity_scan_role` sorts each
//! event into one role, and `docs/performance-match-activity-fanout.md`
//! records the measurement.

use std::collections::HashMap;
use std::ops::Range;

use crate::event::WorkflowEvent;
use crate::types::ActivityExecId;

/// What one event means to the activity terminal scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanRole {
    /// An `ActivityScheduled`. It stops the scan for its own activity. For
    /// any other activity, it is an interleaved command.
    Scheduled(ActivityExecId),
    /// A terminal event. It ends the scan for its own activity. Any other
    /// scan steps over it.
    Terminal(ActivityExecId),
    /// An `ActivityStarted` or `ActivityHeartbeat`. Every scan steps over
    /// it. A settle of its own activity consumes it.
    Progress(ActivityExecId),
    /// A command that the scan steps over. The first one becomes the
    /// cursor after the scan.
    Interleaved,
    /// An event that the scan stashes, or an event that stops it.
    Barrier,
    /// An event that the scan steps over.
    Pass,
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

/// The positions of the events that can change an activity scan.
#[derive(Debug, Default)]
pub struct ActivityScanIndex {
    /// The length of the history prefix that the index holds.
    indexed: usize,
    activities: HashMap<ActivityExecId, ActivityPositions>,
    interleaved: SkipList,
    barriers: SkipList,
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
        role: impl Fn(&WorkflowEvent) -> ScanRole,
    ) {
        for (position, event) in events.iter().enumerate().skip(self.indexed) {
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
                ScanRole::Interleaved => self.interleaved.push(position),
                ScanRole::Barrier => self.barriers.push(position),
                ScanRole::Pass => {}
            }
        }
        self.indexed = self.indexed.max(events.len());
    }

    /// The first unconsumed `ActivityScheduled` or terminal event of `id`
    /// at or after `from`.
    ///
    /// One activity has few such events, so a plain walk is enough.
    pub fn first_stop(
        &self,
        id: ActivityExecId,
        from: usize,
        consumed: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        let stops = &self.activities.get(&id)?.stops;
        let start = stops.partition_point(|&p| p < from);
        stops[start..].iter().copied().find(|&p| !consumed(p))
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
            steps in proptest::collection::vec((0..4_u8, 0..64_usize), 1..200),
        ) {
            let mut list = SkipList::default();
            let mut positions = Vec::new();
            let mut consumed = Vec::new();
            for (kind, value) in steps {
                match kind {
                    0 => {
                        let position = positions.last().map_or(value, |last| last + 1 + value);
                        positions.push(position);
                        list.push(position);
                    }
                    1 => consumed.push(value),
                    _ => {
                        let got = list.first_unconsumed(value, |p| consumed.contains(&p));
                        proptest::prop_assert_eq!(got, first_by_walk(&positions, value, &consumed));
                    }
                }
            }
        }
    }

    #[test]
    fn progress_returns_the_positions_inside_the_range() {
        let id = ActivityExecId::new();
        let mut index = ActivityScanIndex::default();
        let progress = |_: &WorkflowEvent| ScanRole::Progress(id);
        let events: Vec<WorkflowEvent> = (0..6)
            .map(|_| WorkflowEvent::ActivityHeartbeat {
                activity_id: id,
                details: serde_json::Value::Null,
            })
            .collect();
        index.extend(&events, progress);
        assert_eq!(index.progress(id, 1..4), &[1, 2, 3]);
        assert_eq!(index.progress(id, 4..4), &[] as &[usize]);
        assert_eq!(index.progress(ActivityExecId::new(), 0..6), &[] as &[usize]);
    }
}
