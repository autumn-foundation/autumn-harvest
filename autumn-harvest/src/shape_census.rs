//! Shape census — find the rare paths in a corpus of runs (Nova).
//!
//! # Overview
//!
//! [`history_shape`](crate::history_shape) fingerprints one history. This module
//! counts many of them. Feed it the histories of one workflow type, and it tells
//! which path is the usual one and which paths are rare. It also judges a new run:
//! how often its shape occurs, and the event index where it leaves the usual path.
//!
//! ```rust
//! use autumn_harvest::event::WorkflowEvent;
//! use autumn_harvest::shape_census::ShapeCensus;
//!
//! let done = WorkflowEvent::WorkflowCompleted { output: serde_json::json!(1) };
//! let mut census = ShapeCensus::new();
//! census.observe(&[done.clone()]);
//! census.observe(&[done.clone()]);
//! census.observe(&[done.clone(), done.clone()]);
//!
//! assert_eq!(census.total(), 3);
//! assert_eq!(census.dominant().map(|s| s.count), Some(2));
//! assert_eq!(census.rare_shapes(0.5).len(), 1);
//! assert_eq!(census.judge(&[done.clone(), done]).divergence, Some(1));
//! ```
//!
//! # Scope
//!
//! The module is pure. It reads events and writes nothing. It adds no
//! `WorkflowEvent` variant, no SQL and no migration, so replay is unaffected.
//!
//! The census keys each shape by its full sequence of event type names. Two
//! different shapes with one FNV-1a fingerprint thus stay apart. The fingerprint
//! is still reported, so a caller can match it with `shape_fingerprint`.
//!
//! Every order is deterministic. A tie on count falls back to the shape order,
//! so two runs over one corpus give the same report.

use std::collections::BTreeMap;

use crate::event::WorkflowEvent;
use crate::history_shape::shape_fingerprint;

/// One distinct shape in a [`ShapeCensus`].
#[derive(Debug, Clone, PartialEq)]
pub struct ShapeSummary {
    /// The `shape_fingerprint` of the shape.
    pub fingerprint: u64,
    /// The number of observed histories with this shape.
    pub count: usize,
    /// The fraction of all observed histories with this shape, in `0.0..=1.0`.
    pub share: f64,
    /// The ordered event type names of the shape.
    pub path: Vec<&'static str>,
}

/// The census view of one history, from [`ShapeCensus::judge`].
#[derive(Debug, Clone, PartialEq)]
pub struct ShapeVerdict {
    /// The `shape_fingerprint` of the judged history.
    pub fingerprint: u64,
    /// The number of observed histories with the same shape.
    pub seen: usize,
    /// The fraction of observed histories with the same shape.
    ///
    /// It is `0.0` for an empty census and for a shape that the census never saw.
    pub share: f64,
    /// The first event index where the history leaves the dominant path.
    ///
    /// It is `None` when the history follows the dominant path exactly, and
    /// when the census is empty.
    pub divergence: Option<usize>,
}

#[derive(Debug, Clone)]
struct Entry {
    fingerprint: u64,
    count: usize,
}

/// Counts of history shapes over a corpus of runs.
#[derive(Debug, Clone, Default)]
pub struct ShapeCensus {
    shapes: BTreeMap<Vec<&'static str>, Entry>,
    total: usize,
}

fn path_of(events: &[WorkflowEvent]) -> Vec<&'static str> {
    events.iter().map(WorkflowEvent::type_name).collect()
}

fn first_name_divergence(a: &[&str], b: &[&str]) -> Option<usize> {
    let common = a.len().min(b.len());
    (0..common)
        .find(|&i| a[i] != b[i])
        .or_else(|| (a.len() != b.len()).then_some(common))
}

impl ShapeCensus {
    /// Return an empty census.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one history and return its fingerprint.
    pub fn observe(&mut self, events: &[WorkflowEvent]) -> u64 {
        let entry = self.shapes.entry(path_of(events)).or_insert_with(|| Entry {
            fingerprint: shape_fingerprint(events),
            count: 0,
        });
        entry.count += 1;
        self.total += 1;
        entry.fingerprint
    }

    /// Return the number of observed histories.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.total
    }

    /// Return the number of distinct shapes.
    #[must_use]
    pub fn distinct(&self) -> usize {
        self.shapes.len()
    }

    fn share_of(&self, count: usize) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        let share = count as f64 / self.total as f64;
        share
    }

    fn summary(&self, path: &[&'static str], entry: &Entry) -> ShapeSummary {
        ShapeSummary {
            fingerprint: entry.fingerprint,
            count: entry.count,
            share: self.share_of(entry.count),
            path: path.to_vec(),
        }
    }

    /// Return every shape, the most common first.
    ///
    /// A tie on count keeps the shape order, so the result is deterministic.
    #[must_use]
    pub fn shapes(&self) -> Vec<ShapeSummary> {
        let mut all: Vec<ShapeSummary> = self
            .shapes
            .iter()
            .map(|(path, entry)| self.summary(path, entry))
            .collect();
        all.sort_by_key(|s| std::cmp::Reverse(s.count));
        all
    }

    /// Return the most common shape, or `None` for an empty census.
    #[must_use]
    pub fn dominant(&self) -> Option<ShapeSummary> {
        self.dominant_entry()
            .map(|(path, entry)| self.summary(path, entry))
    }

    /// Return the first shape in shape order that has the highest count.
    ///
    /// It matches the head of `shapes()` and copies no path.
    fn dominant_entry(&self) -> Option<(&Vec<&'static str>, &Entry)> {
        let mut best: Option<(&Vec<&'static str>, &Entry)> = None;
        for (path, entry) in &self.shapes {
            if best.is_none_or(|(_, b)| entry.count > b.count) {
                best = Some((path, entry));
            }
        }
        best
    }

    /// Return the shapes whose share is `max_share` or less, the rarest first.
    ///
    /// A tie on count keeps the shape order.
    #[must_use]
    pub fn rare_shapes(&self, max_share: f64) -> Vec<ShapeSummary> {
        let mut rare: Vec<ShapeSummary> = self
            .shapes()
            .into_iter()
            .filter(|s| s.share <= max_share)
            .collect();
        rare.sort_by_key(|s| s.count);
        rare
    }

    /// Judge one history against the census. The census does not change.
    #[must_use]
    pub fn judge(&self, events: &[WorkflowEvent]) -> ShapeVerdict {
        let path = path_of(events);
        let seen = self.shapes.get(&path).map_or(0, |e| e.count);
        let divergence = self
            .dominant_entry()
            .and_then(|(dominant, _)| first_name_divergence(dominant, &path));
        ShapeVerdict {
            fingerprint: shape_fingerprint(events),
            seen,
            share: self.share_of(seen),
            divergence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn done() -> WorkflowEvent {
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!(null),
        }
    }

    fn failed() -> WorkflowEvent {
        WorkflowEvent::WorkflowFailed {
            error: "boom".into(),
            error_type: None,
            details: None,
            non_retryable: Some(false),
        }
    }

    fn corpus() -> ShapeCensus {
        let mut census = ShapeCensus::new();
        for _ in 0..8 {
            census.observe(&[done()]);
        }
        census.observe(&[failed()]);
        census.observe(&[done(), failed()]);
        census
    }

    #[test]
    fn an_empty_census_reports_nothing() {
        let census = ShapeCensus::new();
        assert_eq!(census.total(), 0);
        assert_eq!(census.distinct(), 0);
        assert!(census.dominant().is_none());
        assert_eq!(census.rare_shapes(1.0), Vec::new());
        let verdict = census.judge(&[done()]);
        assert_eq!(verdict.seen, 0);
        assert!(verdict.share.abs() < f64::EPSILON);
        assert_eq!(verdict.divergence, None);
    }

    #[test]
    fn observe_returns_the_history_shape_fingerprint() {
        let mut census = ShapeCensus::new();
        let history = [done(), failed()];
        assert_eq!(census.observe(&history), shape_fingerprint(&history));
    }

    #[test]
    fn the_dominant_shape_is_the_most_common() {
        let census = corpus();
        assert_eq!(census.total(), 10);
        assert_eq!(census.distinct(), 3);
        let dominant = census.dominant().expect("a dominant shape");
        assert_eq!(dominant.count, 8);
        assert_eq!(dominant.path, vec!["WorkflowCompleted"]);
        assert!((dominant.share - 0.8).abs() < 1e-9);
    }

    #[test]
    fn payload_changes_do_not_split_a_shape() {
        let mut census = ShapeCensus::new();
        census.observe(&[done()]);
        census.observe(&[WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"x": 1}),
        }]);
        assert_eq!(census.distinct(), 1);
    }

    #[test]
    fn rare_shapes_are_the_rarest_first_and_deterministic() {
        let census = corpus();
        let rare = census.rare_shapes(0.1);
        assert_eq!(rare.len(), 2);
        assert!(rare.iter().all(|s| s.count == 1));
        assert_eq!(rare, census.rare_shapes(0.1));
        assert_eq!(census.rare_shapes(0.05), Vec::new());
        assert_eq!(census.rare_shapes(1.0).len(), 3);
        assert_eq!(census.rare_shapes(1.0).last().map(|s| s.count), Some(8));
    }

    #[test]
    fn judge_reports_share_and_divergence_from_the_dominant_path() {
        let census = corpus();
        let usual = census.judge(&[done()]);
        assert_eq!(usual.seen, 8);
        assert_eq!(usual.divergence, None);

        let odd = census.judge(&[done(), failed()]);
        assert_eq!(odd.seen, 1);
        assert!((odd.share - 0.1).abs() < 1e-9);
        assert_eq!(odd.divergence, Some(1));

        let novel = census.judge(&[failed(), failed()]);
        assert_eq!(novel.seen, 0);
        assert_eq!(novel.divergence, Some(0));
        assert_eq!(census.total(), 10);
    }

    #[test]
    fn a_count_tie_keeps_the_shape_order() {
        let mut census = ShapeCensus::new();
        census.observe(&[failed()]);
        census.observe(&[done()]);
        let first = census.dominant().expect("a dominant shape");
        let mut reversed = ShapeCensus::new();
        reversed.observe(&[done()]);
        reversed.observe(&[failed()]);
        assert_eq!(Some(first), reversed.dominant());
    }
}
