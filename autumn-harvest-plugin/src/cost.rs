//! Feed the autumn cost signal into Harvest (autumn#1720).
//!
//! autumn-web keeps a [`CostSignal`] in the app state. Its value is grid
//! carbon intensity or a price. `[cost] defer_threshold` sets its threshold,
//! and the runtime-config key `autumn_cost_signal` can set its value with no
//! redeploy. autumn uses it to defer `#[job(deferrable)]` work.
//!
//! [`HarvestPlugin::cost_deferrable_activities`] connects the same signal to
//! the Harvest workers. While the signal is high, the workers claim no task
//! of a named activity type. See `docs/cost-deferral.md`.
//!
//! [`HarvestPlugin::cost_deferrable_activities`]: crate::HarvestPlugin::cost_deferrable_activities

use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::cost_hold::{CostHold, CostSignalSource};
use autumn_web::AppState;
use autumn_web::cost::CostSignal;
use std::time::Duration;

/// A [`CostSignalSource`] that reads an autumn [`CostSignal`].
///
/// The signal is high while its value is above `[cost] defer_threshold`. It
/// is also high until the first runtime-config value arrives, so held work
/// does not start before the app knows the signal.
#[derive(Clone, Debug)]
pub struct AutumnCostSignal(CostSignal);

impl AutumnCostSignal {
    /// Read `signal`. A clone of a `CostSignal` shares its value.
    #[must_use]
    pub const fn new(signal: CostSignal) -> Self {
        Self(signal)
    }
}

impl CostSignalSource for AutumnCostSignal {
    fn is_high(&self) -> bool {
        self.0.is_high()
    }
}

/// Hold `activities` while the autumn `signal` is high.
///
/// Use it with [`autumn_harvest::builder::WorkerConfig::with_cost_hold`] when
/// you build the harvest yourself, for example for a
/// [`HarvestEmbedding`](crate::HarvestEmbedding).
#[must_use]
pub fn autumn_cost_hold<I, S>(signal: CostSignal, activities: I) -> CostHold
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    CostHold::new(AutumnCostSignal::new(signal), activities)
}

/// The cost deferral settings of a `HarvestPlugin`.
#[derive(Clone, Debug, Default)]
pub(crate) struct CostDeferral {
    pub(crate) activities: Vec<String>,
    pub(crate) recheck: Option<Duration>,
}

impl CostDeferral {
    /// The hold on the app's own signal.
    ///
    /// autumn-web inserts the signal before any startup hook runs. A state
    /// without one gets a new signal with no threshold, which is never high.
    /// The app can then still set a threshold on that shared signal.
    pub(crate) fn hold(&self, state: &AppState) -> CostHold {
        if state.extension::<CostSignal>().is_none() {
            tracing::warn!(
                "the app state has no autumn cost signal; deferrable activities run \
                 until the app sets a threshold"
            );
        }
        let signal = state.extension_or_insert_with(CostSignal::default);
        let hold = autumn_cost_hold((*signal).clone(), self.activities.iter().cloned());
        match self.recheck {
            Some(recheck) => hold.with_recheck(recheck),
            None => hold,
        }
    }

    /// Install the hold on `builder`'s worker config.
    ///
    /// A hold that the worker config already sets is replaced, with a
    /// warning, because the plugin call is the later, narrower one.
    pub(crate) fn apply(&self, builder: &mut HarvestBuilder, state: &AppState) {
        if self.activities.is_empty() {
            return;
        }
        let config = builder.worker_config_mut();
        if config.cost_hold.is_some() {
            tracing::warn!(
                "WorkerConfig sets a cost hold; HarvestPlugin::cost_deferrable_activities \
                 replaces it"
            );
        }
        config.cost_hold = Some(self.hold(state));
    }
}

#[cfg(test)]
mod tests {
    use super::{CostDeferral, autumn_cost_hold};
    use autumn_harvest::builder::HarvestBuilder;
    use autumn_web::AppState;
    use autumn_web::cost::CostSignal;
    use std::time::Duration;

    #[test]
    fn the_hold_follows_the_autumn_signal() {
        let signal = CostSignal::new(Some(400.0));
        let hold = autumn_cost_hold(signal.clone(), ["train_model"]);
        signal.set(250.0);
        assert!(!hold.holds());
        signal.set(520.0);
        assert!(hold.holds());
        signal.set(399.0);
        assert!(!hold.holds());
    }

    #[test]
    fn a_signal_with_no_threshold_never_holds() {
        let signal = CostSignal::default();
        signal.set(1.0e9);
        assert!(!autumn_cost_hold(signal, ["train_model"]).holds());
    }

    #[test]
    fn apply_reads_the_signal_in_the_app_state() {
        let state = AppState::for_test();
        let signal = state.extension_or_insert_with(|| CostSignal::new(Some(400.0)));
        let deferral = CostDeferral {
            activities: vec!["train_model".to_owned()],
            recheck: Some(Duration::from_secs(9)),
        };
        let mut builder = HarvestBuilder::new();
        deferral.apply(&mut builder, &state);
        let hold = builder
            .worker_config()
            .cost_hold
            .clone()
            .expect("the plugin installs a hold");
        assert_eq!(hold.activities(), ["train_model".to_owned()]);
        assert_eq!(hold.recheck(), Duration::from_secs(9));
        signal.set(520.0);
        assert!(hold.holds());
        signal.set(100.0);
        assert!(!hold.holds());
    }

    #[test]
    fn apply_with_no_activities_installs_nothing() {
        let state = AppState::for_test();
        let mut builder = HarvestBuilder::new();
        CostDeferral::default().apply(&mut builder, &state);
        assert!(builder.worker_config().cost_hold.is_none());
    }

    #[test]
    fn a_state_with_no_signal_gets_one_that_the_app_can_arm() {
        let state = AppState::for_test();
        let deferral = CostDeferral {
            activities: vec!["train_model".to_owned()],
            recheck: None,
        };
        let hold = deferral.hold(&state);
        assert!(!hold.holds());
        let signal = state
            .extension::<CostSignal>()
            .expect("the hold inserts a shared signal");
        signal.set_threshold(Some(10.0));
        signal.set(20.0);
        assert!(hold.holds());
    }
}
