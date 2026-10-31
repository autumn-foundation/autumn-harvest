//! Cost-aware deferral of activities (autumn#1720).
//!
//! A cost signal is a live value, such as grid carbon intensity or a power
//! price. While the signal is high, a worker with a [`CostHold`] claims no
//! task of a deferrable activity type. The task stays `PENDING`. It uses no
//! attempt and holds no slot. When the signal falls, the next poll claims it.
//!
//! The hold is in process, per worker. It reads the signal before each
//! claim. The `autumn-harvest-plugin` crate feeds it from the autumn
//! `CostSignal`, so `[cost] defer_threshold` and the runtime-config key
//! `autumn_cost_signal` drive Harvest work and autumn jobs alike.
//!
//! ## How the claim skips a type
//!
//! The hold reuses the claim's skip list. The adaptive limit (issue #1836)
//! adds each saturated type to that list with
//! `queue::SATURATED_ACTIVITY_MARKER` in
//! front. A held type goes into the same list with the same marker. So the
//! hold adds no SQL, no claim gate and no migration.
//!
//! ## What the hold does not do
//!
//! - It holds activity tasks only. A workflow task is cheap and holds up its
//!   run, so it always runs.
//! - It cannot hold a local activity. A local activity runs inline in its
//!   workflow task. `try_build` rejects a local activity in the hold.
//! - It does not stop a `schedule_to_start` timer. That timer is a start
//!   deadline. A window longer than the deadline fails the task. A queue
//!   pause (issue #619) suspends the timer. A cost hold does not, because a
//!   high price is not an outage.
//! - A task that a worker claimed before the signal rose runs.
//! - The database cannot see the hold. A held task counts in the queue depth
//!   and in the age of the oldest pending task.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

/// The default time before a dispatch reference of a held task returns.
///
/// Only a worker with a dispatch channel (issue #1312) uses it. The Postgres
/// claim loop polls on its own `poll_interval`.
pub const DEFAULT_COST_HOLD_RECHECK: Duration = Duration::from_secs(5);

/// The shortest recheck time. A shorter value would spin the channel.
pub const MIN_COST_HOLD_RECHECK: Duration = Duration::from_secs(1);

/// The claim skip-list marker. It equals
/// `queue::SATURATED_ACTIVITY_MARKER`, which needs the `db` feature. The
/// builder checks a hold without that feature, so this module keeps its own
/// copy. A `db` test pins the two equal.
const CLAIM_MARKER: char = '\u{1}';

/// A live cost signal that a [`CostHold`] reads before each claim.
///
/// Make each read cheap. A worker reads it once per claim. A closure
/// `Fn() -> bool` is a source.
pub trait CostSignalSource: Send + Sync + 'static {
    /// `true` while deferrable activities must wait.
    fn is_high(&self) -> bool;
}

impl<F> CostSignalSource for F
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    fn is_high(&self) -> bool {
        self()
    }
}

/// The hold state that the worker saw last. It starts unknown, so the first
/// read always reports.
const STATE_UNKNOWN: u8 = 0;
const STATE_OFF: u8 = 1;
const STATE_ON: u8 = 2;

/// Holds the claims of deferrable activity types while a cost signal is high.
///
/// Set it with [`WorkerConfig::with_cost_hold`]. A clone shares the signal
/// and the last state.
///
/// [`WorkerConfig::with_cost_hold`]: crate::builder::WorkerConfig::with_cost_hold
///
/// ```
/// use autumn_harvest::cost_hold::CostHold;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
///
/// let high = Arc::new(AtomicBool::new(false));
/// let signal = Arc::clone(&high);
/// let hold = CostHold::new(
///     move || signal.load(Ordering::Relaxed),
///     ["rebuild_search_index", "train_model"],
/// );
/// assert!(!hold.holds());
/// high.store(true, Ordering::Relaxed);
/// assert!(hold.holds());
/// assert!(hold.is_deferrable("train_model"));
/// assert!(!hold.is_deferrable("charge_card"));
/// ```
#[derive(Clone)]
pub struct CostHold {
    signal: Arc<dyn CostSignalSource>,
    /// Sorted and without duplicates.
    activities: Arc<[String]>,
    /// The same names with the claim marker in front. Only the `db` claim
    /// path reads them.
    #[cfg_attr(not(feature = "db"), allow(dead_code))]
    marked: Arc<[String]>,
    recheck: Duration,
    state: Arc<AtomicU8>,
    metrics: Option<Arc<dyn crate::telemetry::MetricsRecorder>>,
}

impl std::fmt::Debug for CostHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CostHold")
            .field("activities", &self.activities)
            .field("recheck", &self.recheck)
            .field("held", &self.last_state())
            .finish_non_exhaustive()
    }
}

impl CostHold {
    /// Hold `activities` while `signal` is high.
    ///
    /// The names are activity type names, as `#[activity]` registers them.
    /// Duplicates have no effect.
    #[must_use]
    pub fn new<I, S>(signal: impl CostSignalSource, activities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::from_arc(Arc::new(signal), activities)
    }

    /// [`Self::new`] with a shared signal.
    #[must_use]
    pub fn from_arc<I, S>(signal: Arc<dyn CostSignalSource>, activities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut names: Vec<String> = activities.into_iter().map(Into::into).collect();
        names.sort_unstable();
        names.dedup();
        let marked: Vec<String> = names
            .iter()
            .map(|name| format!("{CLAIM_MARKER}{name}"))
            .collect();
        Self {
            signal,
            activities: names.into(),
            marked: marked.into(),
            recheck: DEFAULT_COST_HOLD_RECHECK,
            state: Arc::new(AtomicU8::new(STATE_UNKNOWN)),
            metrics: None,
        }
    }

    /// Set the time before a held dispatch reference returns. The minimum is
    /// [`MIN_COST_HOLD_RECHECK`].
    #[must_use]
    pub fn with_recheck(mut self, recheck: Duration) -> Self {
        self.recheck = recheck.max(MIN_COST_HOLD_RECHECK);
        self
    }

    /// Report each change of the hold to `metrics`. The handler registry
    /// sets it.
    #[cfg(feature = "db")]
    #[must_use]
    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<dyn crate::telemetry::MetricsRecorder>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The held activity type names, sorted.
    #[must_use]
    pub fn activities(&self) -> &[String] {
        &self.activities
    }

    /// `true` when the hold covers `activity`.
    #[must_use]
    pub fn is_deferrable(&self, activity: &str) -> bool {
        self.activities
            .binary_search_by(|name| name.as_str().cmp(activity))
            .is_ok()
    }

    /// The time before a held dispatch reference returns.
    #[must_use]
    pub const fn recheck(&self) -> Duration {
        self.recheck
    }

    /// The state that the last [`Self::holds`] call saw. `None` before the
    /// first call.
    #[must_use]
    pub fn last_state(&self) -> Option<bool> {
        match self.state.load(Ordering::Relaxed) {
            STATE_ON => Some(true),
            STATE_OFF => Some(false),
            _ => None,
        }
    }

    /// Read the signal. `true` while the deferrable types must wait.
    ///
    /// Each change logs one line and sets the `harvest.cost.hold` gauge. The
    /// swap is atomic, so one caller sees each change, also with several
    /// claim loops.
    pub fn holds(&self) -> bool {
        let held = self.signal.is_high();
        let next = if held { STATE_ON } else { STATE_OFF };
        let previous = self.state.swap(next, Ordering::Relaxed);
        if previous != next {
            if held {
                tracing::info!(
                    activities = ?self.activities,
                    "cost signal is high; deferrable activities wait"
                );
            } else if previous == STATE_ON {
                tracing::info!(
                    activities = ?self.activities,
                    "cost signal fell; deferrable activities run"
                );
            }
            if let Some(metrics) = &self.metrics {
                metrics.record_cost_hold(held);
            }
        }
        held
    }

    /// The claim skip-list entries of the held types, or an empty slice
    /// when the signal is low.
    #[cfg(feature = "db")]
    pub(crate) fn exclusions(&self) -> &[String] {
        if self.holds() { &self.marked } else { &[] }
    }

    /// Check the hold against the registered activities.
    ///
    /// A local activity, an empty name or a name with the claim marker is an
    /// error. An unregistered name and a `schedule_to_start` default only
    /// log a warning.
    ///
    /// # Errors
    ///
    /// Returns a message for the first invalid name.
    pub(crate) fn validate(&self, activities: &[crate::info::ActivityInfo]) -> Result<(), String> {
        for name in self.activities.iter() {
            if name.is_empty() {
                return Err("cost hold names an empty activity".to_owned());
            }
            if name.starts_with(CLAIM_MARKER) {
                return Err(format!(
                    "cost hold activity {name:?} starts with a reserved control character"
                ));
            }
            let Some(info) = activities.iter().find(|info| info.name == name) else {
                tracing::warn!(
                    activity_name = %name,
                    "cost hold names an activity that is not registered; it has no effect"
                );
                continue;
            };
            if info.is_local {
                return Err(format!(
                    "cost hold names local activity '{name}'; a local activity runs inline \
                     in its workflow task, so a hold cannot defer it"
                ));
            }
            if let Some(deadline) = info.default_schedule_to_start {
                tracing::warn!(
                    activity_name = %name,
                    schedule_to_start = ?deadline,
                    "cost hold activity has a schedule_to_start; a window longer than it \
                     times the task out"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CostHold, DEFAULT_COST_HOLD_RECHECK, MIN_COST_HOLD_RECHECK};
    use crate::info::ActivityInfo;
    use std::time::Duration;
    #[cfg(feature = "db")]
    use {
        crate::telemetry::MetricsRecorder,
        std::sync::atomic::{AtomicBool, Ordering},
        std::sync::{Arc, Mutex},
    };

    fn activity(name: &'static str, is_local: bool) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "cost_hold_tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            default_schedule_to_close: None,
            is_local,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            input_schema: None,
            output_schema: None,
        }
    }

    /// The local copy of the claim marker matches the claim's own.
    #[cfg(feature = "db")]
    #[test]
    fn the_claim_marker_matches_the_queue() {
        assert_eq!(super::CLAIM_MARKER, crate::queue::SATURATED_ACTIVITY_MARKER);
    }

    #[cfg(feature = "db")]
    fn switch() -> (Arc<AtomicBool>, impl Fn() -> bool + Send + Sync + 'static) {
        let high = Arc::new(AtomicBool::new(false));
        let read = Arc::clone(&high);
        (high, move || read.load(Ordering::Relaxed))
    }

    #[cfg(feature = "db")]
    #[derive(Debug, Default)]
    struct Gauge(Mutex<Vec<bool>>);

    #[cfg(feature = "db")]
    impl MetricsRecorder for Gauge {
        fn record_cost_hold(&self, held: bool) {
            self.0.lock().expect("gauge lock").push(held);
        }
    }

    #[test]
    fn names_are_sorted_and_deduplicated() {
        let hold = CostHold::new(|| false, ["b", "a", "b"]);
        assert_eq!(hold.activities(), ["a".to_owned(), "b".to_owned()]);
        assert!(hold.is_deferrable("a"));
        assert!(!hold.is_deferrable("c"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn the_skip_list_follows_the_signal() {
        let (high, read) = switch();
        let hold = CostHold::new(read, ["train_model"]);
        assert!(hold.exclusions().is_empty());
        high.store(true, Ordering::Relaxed);
        assert_eq!(hold.exclusions(), ["\u{1}train_model".to_owned()]);
        high.store(false, Ordering::Relaxed);
        assert!(hold.exclusions().is_empty());
    }

    #[cfg(feature = "db")]
    #[test]
    fn each_change_sets_the_gauge_once() {
        let (high, read) = switch();
        let gauge = Arc::new(Gauge::default());
        let hold = CostHold::new(read, ["train_model"]).with_metrics(gauge.clone());
        assert_eq!(hold.last_state(), None);
        // The first read reports, so the gauge is set before any change.
        hold.holds();
        hold.holds();
        high.store(true, Ordering::Relaxed);
        hold.holds();
        hold.clone().holds();
        high.store(false, Ordering::Relaxed);
        hold.holds();
        assert_eq!(*gauge.0.lock().expect("gauge lock"), [false, true, false]);
        assert_eq!(hold.last_state(), Some(false));
    }

    #[test]
    fn the_recheck_has_a_floor() {
        let hold = CostHold::new(|| false, ["a"]);
        assert_eq!(hold.recheck(), DEFAULT_COST_HOLD_RECHECK);
        let hold = hold.with_recheck(Duration::from_millis(10));
        assert_eq!(hold.recheck(), MIN_COST_HOLD_RECHECK);
    }

    #[test]
    fn validation_rejects_a_local_activity_and_the_marker() {
        let infos = [
            activity("inline_step", true),
            activity("train_model", false),
        ];
        assert!(
            CostHold::new(|| false, ["train_model", "not_registered"])
                .validate(&infos)
                .is_ok()
        );
        let error = CostHold::new(|| false, ["inline_step"])
            .validate(&infos)
            .expect_err("a local activity cannot be held");
        assert!(error.contains("local activity 'inline_step'"), "{error}");
        assert!(
            CostHold::new(|| false, ["\u{1}x"])
                .validate(&infos)
                .is_err()
        );
        assert!(CostHold::new(|| false, [""]).validate(&infos).is_err());
    }
}
