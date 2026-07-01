//! Adaptive worker dispatch-slot tuner (issue #548).
//!
//! A harvest worker bounds concurrent workflow/activity dispatch with two
//! `tokio::sync::Semaphore`s sized from `WorkerConfig::max_concurrent_workflows`
//! / `max_concurrent_activities`. Those are static numbers an operator picks
//! once and almost always picks wrong: too high exhausts the worker DB pool
//! under a burst; too low leaves capacity idle while schedule-to-start latency
//! climbs.
//!
//! This module is the opt-in *act* half of issue #531 (slot-utilization
//! gauges, the *observe* half): a [`SlotTuner`] resizes a worker's live
//! dispatch semaphore within an operator-configured `[min_slots, max_slots]`
//! band, driven only by in-process signals the worker already owns — slot
//! utilization, worker DB-pool acquisition pressure, and recent
//! claim-to-dispatch permit-wait latency. No new external dependency, no
//! `execution.id` sampling.
//!
//! This module is pure / no-DB except for [`TunedSlotRuntime`] and
//! [`spawn_slot_tuner_loop`], which operate on an in-memory `Semaphore` and a
//! caller-supplied pool-pressure closure — there is still no direct database
//! dependency here (the closure and the DB connection live in `worker.rs`).
//!
//! # Resize mechanics
//!
//! When a tuner is configured, a dispatch semaphore is created with
//! `max_slots` total permits. The runtime immediately withholds
//! `max_slots - initial_target` of them as owned permits it holds itself.
//! **Grow** drops withheld permits (making them available for dispatch).
//! **Shrink** opportunistically re-acquires free permits back into the
//! withheld set — it never blocks and never revokes a permit already held by
//! an in-flight task, so a shrink decision only withholds *new* permits until
//! the in-flight count naturally falls to the new target (issue #548 AC:
//! graceful shutdown and draining are unaffected). On cancellation the loop
//! drops every withheld permit before returning so `drain_in_flight`'s
//! `acquire_many(max_slots)` can complete.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::telemetry::{SlotType, TelemetryConfig, TunerDecision};

/// A cheap, deadpool-free mirror of the worker DB pool's saturation status.
///
/// Constructed by the caller (`worker.rs`) from `deadpool::managed::Pool::status()`
/// so this module has no direct dependency on the pool implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolPressure {
    /// The pool's configured maximum size.
    pub max_size: usize,
    /// The pool's current size (objects created so far, up to `max_size`).
    pub size: usize,
    /// The number of idle, immediately-available objects in the pool.
    pub available: usize,
    /// The number of futures currently waiting for a pool connection.
    pub waiting: usize,
}

impl PoolPressure {
    /// The pool has at least one caller blocked waiting for a connection, or
    /// every connection slot is created and none is idle.
    #[must_use]
    pub const fn is_saturated(self) -> bool {
        self.waiting > 0 || (self.available == 0 && self.size >= self.max_size)
    }
}

/// The observations a [`SlotTuner`] decides from on each control-loop tick.
///
/// Every field is a signal harvest already owns in-process. Deliberately
/// excludes `execution.id` and any other high-cardinality identifier — per
/// ADR-0001 §7 that never belongs on a metric label, and by construction it
/// is not even collected here.
#[derive(Debug, Clone, Copy)]
pub struct SlotObservations {
    /// The dispatch slot count currently in effect for this slot type.
    pub current_target: usize,
    /// The operator-configured liveness floor.
    pub min_slots: usize,
    /// The operator-configured hard safety cap.
    pub max_slots: usize,
    /// How many of `current_target` slots are occupied by in-flight work.
    pub in_use: usize,
    /// Worker DB-pool saturation, when available.
    pub pool: Option<PoolPressure>,
    /// The longest claim-to-dispatch permit-wait observed since the previous
    /// tick, when any task was dispatched.
    pub max_permit_wait: Option<Duration>,
}

/// What a [`SlotTuner`] wants to do to the live target this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotTunerAction {
    /// Increase the live target by this many slots (before band clamping).
    Grow(usize),
    /// Decrease the live target by this many slots (before band clamping).
    Shrink(usize),
    /// Leave the live target unchanged.
    Hold,
}

/// A pluggable controller that decides how to resize a worker's dispatch
/// slots.
///
/// `decide` is called once per slot type per control-loop tick (on the
/// worker's existing monitoring cadence — see [`spawn_slot_tuner_loop`]) and
/// must be a **pure, fast, non-blocking** function: it runs on the same task
/// as the loop's sleep/cancel select, off the hot dispatch path, but a slow
/// or panicking implementation still delays the next tick.
pub trait SlotTuner: Send + Sync {
    /// Decide the next action given the current observations.
    fn decide(&self, observations: &SlotObservations) -> SlotTunerAction;

    /// A short, stable name for diagnostics (e.g. surfaced in `Debug` output
    /// and logs). Does not need to be unique.
    fn name(&self) -> &'static str {
        "custom"
    }
}

/// The harvest-provided default controller.
///
/// Decision order (first match wins):
/// 1. **Pool pressure** ([`PoolPressure::is_saturated`]) — shrink. Protecting
///    the worker DB pool from exhaustion takes priority over slot
///    utilization: a burst that is about to exhaust the pool must back off
///    even if dispatch slots still look busy.
/// 2. **Saturated and waiting** — every current slot is in use *and* the
///    longest recent permit wait has reached [`Self::permit_wait_grow_threshold`]
///    — grow. Mere full occupancy with no observed wait is not, by itself, a
///    signal to grow (a burst that drains within a tick shouldn't ratchet
///    capacity up).
/// 3. Otherwise — hold.
#[derive(Debug, Clone, Copy)]
pub struct DefaultSlotTuner {
    /// How many slots to add on a grow decision.
    pub grow_step: usize,
    /// How many slots to remove on a shrink decision.
    pub shrink_step: usize,
    /// Minimum observed permit-wait latency, at full slot occupancy, before
    /// growing.
    pub permit_wait_grow_threshold: Duration,
}

impl Default for DefaultSlotTuner {
    fn default() -> Self {
        Self {
            grow_step: 2,
            shrink_step: 2,
            permit_wait_grow_threshold: Duration::from_millis(50),
        }
    }
}

impl SlotTuner for DefaultSlotTuner {
    fn decide(&self, observations: &SlotObservations) -> SlotTunerAction {
        if observations.pool.is_some_and(PoolPressure::is_saturated) {
            return SlotTunerAction::Shrink(self.shrink_step);
        }

        let saturated = observations.in_use >= observations.current_target;
        let waited_long_enough = observations
            .max_permit_wait
            .is_some_and(|w| w >= self.permit_wait_grow_threshold);

        if saturated && waited_long_enough {
            return SlotTunerAction::Grow(self.grow_step);
        }

        SlotTunerAction::Hold
    }

    fn name(&self) -> &'static str {
        "harvest-default"
    }
}

/// Operator configuration for an adaptive dispatch-slot band.
///
/// Install via `WorkerConfig::with_slot_tuner`. When unset the worker keeps
/// today's fixed-concurrency semaphore behavior byte-for-byte (issue #548 AC).
#[derive(Clone)]
pub struct SlotTunerConfig {
    /// The controller never resizes below this many slots (liveness floor).
    pub min_slots: usize,
    /// The controller never resizes above this many slots (hard safety cap).
    pub max_slots: usize,
    /// The controller instance. Both the workflow and activity dispatch
    /// semaphores get their own independent instance of this same
    /// configuration and share this one `[min_slots, max_slots]` band
    /// (per-type bands are a documented follow-up, not this slice).
    pub tuner: Arc<dyn SlotTuner>,
}

impl std::fmt::Debug for SlotTunerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotTunerConfig")
            .field("min_slots", &self.min_slots)
            .field("max_slots", &self.max_slots)
            .field("tuner", &self.tuner.name())
            .finish()
    }
}

impl SlotTunerConfig {
    /// Build a config using the harvest-provided [`DefaultSlotTuner`].
    #[must_use]
    pub fn new(min_slots: usize, max_slots: usize) -> Self {
        Self {
            min_slots,
            max_slots,
            tuner: Arc::new(DefaultSlotTuner::default()),
        }
    }

    /// Build a config with a custom controller implementation.
    #[must_use]
    pub fn with_tuner(min_slots: usize, max_slots: usize, tuner: Arc<dyn SlotTuner>) -> Self {
        Self {
            min_slots,
            max_slots,
            tuner,
        }
    }
}

/// Clamp `configured_max` (the worker's static `max_concurrent_*` value) into
/// `[min_slots, max_slots]` to produce the initial live target when a tuner is
/// installed.
#[must_use]
pub const fn initial_target(configured_max: usize, min_slots: usize, max_slots: usize) -> usize {
    if configured_max < min_slots {
        min_slots
    } else if configured_max > max_slots {
        max_slots
    } else {
        configured_max
    }
}

/// Apply a [`SlotTunerAction`] to `current`, clamped to `[min_slots, max_slots]`.
///
/// Returns the new target and the [`TunerDecision`] that actually took effect
/// — a `Grow`/`Shrink` that is fully absorbed by the clamp (the target does
/// not move) is reported as `Hold`, so the decision-counter metric reflects
/// what happened, not merely what was requested.
#[must_use]
pub const fn apply_action(
    current: usize,
    action: SlotTunerAction,
    min_slots: usize,
    max_slots: usize,
) -> (usize, TunerDecision) {
    let proposed = match action {
        SlotTunerAction::Grow(step) => current.saturating_add(step),
        SlotTunerAction::Shrink(step) => current.saturating_sub(step),
        SlotTunerAction::Hold => current,
    };
    let clamped = if proposed < min_slots {
        min_slots
    } else if proposed > max_slots {
        max_slots
    } else {
        proposed
    };

    if clamped == current {
        (clamped, TunerDecision::Hold)
    } else if clamped > current {
        (clamped, TunerDecision::Grow)
    } else {
        (clamped, TunerDecision::Shrink)
    }
}

/// Pure config-time sanity checks for a `[min_slots, max_slots]` band against
/// the worker's statically-configured `max_concurrent_*` value.
///
/// Returns human-readable warning strings; never errors — a misconfigured
/// band degrades to an inert (but harmless) tuner rather than failing worker
/// startup, matching the `queue_weights` precedent.
#[must_use]
pub fn validate_band(min_slots: usize, max_slots: usize, configured_max: usize) -> Vec<String> {
    let mut warnings = Vec::new();
    if min_slots > max_slots {
        warnings.push(format!(
            "slot_tuner min_slots ({min_slots}) is greater than max_slots ({max_slots}); \
             the tuner will be inert at max_slots"
        ));
    }
    if max_slots == 0 {
        warnings.push("slot_tuner max_slots is 0; dispatch will be permanently blocked".into());
    }
    if configured_max < min_slots || configured_max > max_slots {
        warnings.push(format!(
            "configured max_concurrent value ({configured_max}) is outside the slot_tuner band \
             [{min_slots}, {max_slots}]; the initial live target will be clamped into the band"
        ));
    }
    warnings
}

/// Available dispatch permits for a slot type, accounting for permits the
/// tuner is currently withholding.
///
/// `raw_available` is `Semaphore::available_permits()`, which counts
/// withheld permits as "available" even though they are not offered to
/// dispatch. The true dispatchable count is `raw_available` minus whatever
/// is currently withheld, i.e. `permit_total - live_target`.
#[must_use]
pub const fn tuned_available(
    permit_total: usize,
    live_target: usize,
    raw_available: usize,
) -> usize {
    let withheld = permit_total.saturating_sub(live_target);
    raw_available.saturating_sub(withheld)
}

/// Runtime handle for one tuned dispatch semaphore.
///
/// Owns the permits currently withheld from dispatch (the gap between
/// `max_slots` and the live target) and the shared `live_target` cell the
/// worker's slot-occupancy sampler (issue #531) reads so its gauges stay
/// consistent with the tuned value rather than the static configured max.
#[derive(Debug)]
pub struct TunedSlotRuntime {
    semaphore: Arc<Semaphore>,
    live_target: Arc<AtomicUsize>,
    withheld: Vec<OwnedSemaphorePermit>,
    min_slots: usize,
    max_slots: usize,
}

impl TunedSlotRuntime {
    /// Create a runtime over a semaphore that already has `max_slots` total
    /// permits, immediately withholding permits down to the clamped initial
    /// target derived from `configured_max`.
    #[must_use]
    pub fn new(
        semaphore: Arc<Semaphore>,
        configured_max: usize,
        min_slots: usize,
        max_slots: usize,
    ) -> Self {
        let target = initial_target(configured_max, min_slots, max_slots);
        let to_withhold = max_slots.saturating_sub(target);
        let mut withheld = Vec::with_capacity(to_withhold);
        for _ in 0..to_withhold {
            match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => withheld.push(permit),
                // The semaphore was constructed with fewer than max_slots
                // permits (a caller bug) — stop withholding rather than
                // panicking; the live target will simply read higher than
                // the true capacity until corrected.
                Err(_) => break,
            }
        }
        Self {
            semaphore,
            live_target: Arc::new(AtomicUsize::new(target)),
            withheld,
            min_slots,
            max_slots,
        }
    }

    /// The shared live-target cell, for handing to the slot-occupancy sampler.
    #[must_use]
    pub fn live_target_cell(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.live_target)
    }

    /// Current live target.
    #[must_use]
    pub fn live_target(&self) -> usize {
        self.live_target.load(Ordering::Relaxed)
    }

    /// Move the live target toward `desired` (already band-clamped by the
    /// caller), growing by releasing withheld permits or shrinking by
    /// opportunistically re-acquiring free ones. Never blocks and never
    /// revokes a permit held by an in-flight task — a shrink that cannot
    /// fully reach `desired` this tick will keep trying on subsequent calls
    /// as permits are returned.
    pub fn resize_toward(&mut self, desired: usize) -> usize {
        let desired = desired.clamp(self.min_slots, self.max_slots);
        let current = self.live_target();

        if desired > current {
            let mut released = 0;
            while self.live_target.load(Ordering::Relaxed) < desired {
                if self.withheld.pop().is_none() {
                    break;
                }
                self.live_target.fetch_add(1, Ordering::Relaxed);
                released += 1;
            }
            let _ = released;
        } else if desired < current {
            while self.live_target.load(Ordering::Relaxed) > desired {
                match Arc::clone(&self.semaphore).try_acquire_owned() {
                    Ok(permit) => {
                        self.withheld.push(permit);
                        self.live_target.fetch_sub(1, Ordering::Relaxed);
                    }
                    // No free permit right now — every remaining slot at
                    // the current target is occupied by in-flight work.
                    // Stop; a later tick will retry as permits return.
                    Err(_) => break,
                }
            }
        }

        self.live_target()
    }

    /// Drop every withheld permit, releasing them all back to the semaphore.
    /// Called on tuner-loop cancellation so `drain_in_flight`'s
    /// `acquire_many(max_slots)` can complete without waiting on slots the
    /// tuner itself was holding back.
    pub fn release_all_withheld(&mut self) {
        self.withheld.clear();
        self.live_target.store(self.max_slots, Ordering::Relaxed);
    }
}

/// Spawn the adaptive slot-tuner control loop for one dispatch slot type.
///
/// Runs on the worker's existing monitoring cadence (`interval`, the same
/// `poll_interval` the timeout/poison-pill checkers use) via a
/// `tokio::select! { cancel, sleep }` loop, mirroring
/// `poison_pill::spawn_poison_pill_reclaimer`. This decouples the control
/// loop from the hot dispatch path entirely: dispatch only ever touches a
/// lock-free `AtomicU64` (`permit_wait_micros`) to record the longest recent
/// permit wait, never the tuner itself.
///
/// Runs regardless of whether a metrics recorder is configured — unlike a
/// pure sampler, the tuner is a controller with a real effect on dispatch
/// capacity, so it must not be silently disabled by `is_enabled() == false`.
/// Telemetry emission (not the control decision) is what's gated.
///
/// On cancellation, every withheld permit is released before the task
/// returns so a subsequent `drain_in_flight` can observe full capacity.
///
/// Best-effort, not a hard rate guarantee: a controller that misbehaves (or a
/// custom `SlotTuner` implementation) can at most leave the target anywhere
/// within `[min_slots, max_slots]` — it can never withhold below the floor.
#[allow(clippy::too_many_arguments)]
pub fn spawn_slot_tuner_loop(
    mut runtime: TunedSlotRuntime,
    tuner: Arc<dyn SlotTuner>,
    slot_type: SlotType,
    pool_pressure: impl Fn() -> Option<PoolPressure> + Send + 'static,
    permit_wait_micros: Arc<AtomicU64>,
    cancel: CancellationToken,
    interval: Duration,
    telemetry: Arc<TelemetryConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            let current_target = runtime.live_target();
            let raw_available = runtime.semaphore.available_permits();
            let in_use = current_target.saturating_sub(tuned_available(
                runtime.max_slots,
                current_target,
                raw_available,
            ));

            let wait_micros = permit_wait_micros.swap(0, Ordering::Relaxed);
            let max_permit_wait = if wait_micros == 0 {
                None
            } else {
                Some(Duration::from_micros(wait_micros))
            };

            let observations = SlotObservations {
                current_target,
                min_slots: runtime.min_slots,
                max_slots: runtime.max_slots,
                in_use,
                pool: pool_pressure(),
                max_permit_wait,
            };

            let action = tuner.decide(&observations);
            let (new_target, decision) =
                apply_action(current_target, action, runtime.min_slots, runtime.max_slots);
            runtime.resize_toward(new_target);

            if telemetry.metrics.is_enabled() {
                telemetry
                    .metrics
                    .record_worker_slot_target(slot_type, runtime.live_target() as u64);
                telemetry.metrics.record_tuner_decision(slot_type, decision);
            }
        }

        runtime.release_all_withheld();
    })
}

// `TunedSlotRuntime` holds `OwnedSemaphorePermit`s, which clippy treats as a
// "significant drop" type; every test below intentionally keeps a runtime
// (and, in one case, a `Vec` of permits standing in for in-flight tasks)
// alive across multiple assertions, which is the whole point of the test.
// Both lints are false positives in this file.
#[cfg(test)]
#[allow(clippy::significant_drop_tightening, clippy::collection_is_never_read)]
mod tests {
    use super::*;

    fn observations(
        current_target: usize,
        min_slots: usize,
        max_slots: usize,
        in_use: usize,
        pool: Option<PoolPressure>,
        max_permit_wait: Option<Duration>,
    ) -> SlotObservations {
        SlotObservations {
            current_target,
            min_slots,
            max_slots,
            in_use,
            pool,
            max_permit_wait,
        }
    }

    // -----------------------------------------------------------------------
    // DefaultSlotTuner::decide
    // -----------------------------------------------------------------------

    #[test]
    fn default_tuner_holds_when_idle() {
        let tuner = DefaultSlotTuner::default();
        let obs = observations(10, 2, 40, 1, None, None);
        assert_eq!(tuner.decide(&obs), SlotTunerAction::Hold);
    }

    #[test]
    fn default_tuner_grows_when_saturated_with_permit_wait() {
        let tuner = DefaultSlotTuner::default();
        let obs = observations(10, 2, 40, 10, None, Some(Duration::from_millis(100)));
        assert_eq!(tuner.decide(&obs), SlotTunerAction::Grow(tuner.grow_step));
    }

    #[test]
    fn default_tuner_holds_when_saturated_but_no_permit_wait() {
        let tuner = DefaultSlotTuner::default();
        let obs = observations(10, 2, 40, 10, None, None);
        assert_eq!(tuner.decide(&obs), SlotTunerAction::Hold);
    }

    #[test]
    fn default_tuner_shrinks_on_pool_waiting() {
        let tuner = DefaultSlotTuner::default();
        let pool = PoolPressure {
            max_size: 10,
            size: 8,
            available: 2,
            waiting: 3,
        };
        // Saturated slots + long permit wait would normally grow, but pool
        // pressure must win.
        let obs = observations(10, 2, 40, 10, Some(pool), Some(Duration::from_secs(1)));
        assert_eq!(
            tuner.decide(&obs),
            SlotTunerAction::Shrink(tuner.shrink_step)
        );
    }

    #[test]
    fn default_tuner_shrinks_on_pool_exhaustion() {
        let tuner = DefaultSlotTuner::default();
        let pool = PoolPressure {
            max_size: 10,
            size: 10,
            available: 0,
            waiting: 0,
        };
        let obs = observations(10, 2, 40, 5, Some(pool), None);
        assert_eq!(
            tuner.decide(&obs),
            SlotTunerAction::Shrink(tuner.shrink_step)
        );
    }

    #[test]
    fn default_tuner_holds_without_pool_signal() {
        let tuner = DefaultSlotTuner::default();
        let obs = observations(10, 2, 40, 3, None, None);
        assert_eq!(tuner.decide(&obs), SlotTunerAction::Hold);
    }

    // -----------------------------------------------------------------------
    // apply_action / initial_target
    // -----------------------------------------------------------------------

    #[test]
    fn apply_action_clamps_grow_at_max() {
        let (target, decision) = apply_action(40, SlotTunerAction::Grow(5), 2, 40);
        assert_eq!(target, 40);
        assert_eq!(decision, TunerDecision::Hold);
    }

    #[test]
    fn apply_action_clamps_shrink_at_min() {
        let (target, decision) = apply_action(2, SlotTunerAction::Shrink(5), 2, 40);
        assert_eq!(target, 2);
        assert_eq!(decision, TunerDecision::Hold);
    }

    #[test]
    fn apply_action_partial_step_at_band_edge() {
        let (target, decision) = apply_action(39, SlotTunerAction::Grow(4), 2, 40);
        assert_eq!(target, 40);
        assert_eq!(decision, TunerDecision::Grow);
    }

    #[test]
    fn apply_action_shrink_reports_shrink_when_it_moves() {
        let (target, decision) = apply_action(10, SlotTunerAction::Shrink(2), 2, 40);
        assert_eq!(target, 8);
        assert_eq!(decision, TunerDecision::Shrink);
    }

    #[test]
    fn apply_action_hold_never_moves() {
        let (target, decision) = apply_action(10, SlotTunerAction::Hold, 2, 40);
        assert_eq!(target, 10);
        assert_eq!(decision, TunerDecision::Hold);
    }

    #[test]
    fn initial_target_clamps_configured_max_into_band() {
        assert_eq!(initial_target(20, 4, 10), 10);
        assert_eq!(initial_target(20, 30, 100), 30);
        assert_eq!(initial_target(20, 4, 100), 20);
    }

    // -----------------------------------------------------------------------
    // SlotTunerConfig
    // -----------------------------------------------------------------------

    #[test]
    fn slot_tuner_config_debug_does_not_require_tuner_debug() {
        let cfg = SlotTunerConfig::new(2, 8);
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("min_slots: 2"));
        assert!(rendered.contains("max_slots: 8"));
        assert!(rendered.contains("harvest-default"));
    }

    #[test]
    fn slot_tuner_config_new_uses_default_controller() {
        let cfg = SlotTunerConfig::new(2, 8);
        assert_eq!(cfg.tuner.name(), "harvest-default");
    }

    // -----------------------------------------------------------------------
    // validate_band
    // -----------------------------------------------------------------------

    #[test]
    fn validate_band_flags_min_gt_max() {
        let warnings = validate_band(10, 5, 8);
        assert!(warnings.iter().any(|w| w.contains("greater than")));
    }

    #[test]
    fn validate_band_flags_zero_max() {
        let warnings = validate_band(0, 0, 0);
        assert!(warnings.iter().any(|w| w.contains("max_slots is 0")));
    }

    #[test]
    fn validate_band_flags_configured_outside_band() {
        let warnings = validate_band(4, 10, 20);
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("outside the slot_tuner band"))
        );
    }

    #[test]
    fn validate_band_is_clean_for_sane_config() {
        let warnings = validate_band(2, 40, 20);
        assert!(warnings.is_empty());
    }

    // -----------------------------------------------------------------------
    // tuned_available
    // -----------------------------------------------------------------------

    #[test]
    fn tuned_available_subtracts_withheld_from_raw() {
        // permit_total 100, live_target 20 => 80 withheld; raw_available 100
        // (withheld permits still count toward available_permits()).
        assert_eq!(tuned_available(100, 20, 100), 20);
    }

    #[test]
    fn tuned_available_reflects_in_flight_usage() {
        // permit_total 100, live_target 20, 5 of the 20 dispatchable permits
        // are in flight => raw_available is 95 (100 - 5 in-flight).
        assert_eq!(tuned_available(100, 20, 95), 15);
    }

    // -----------------------------------------------------------------------
    // TunedSlotRuntime — real tokio::sync::Semaphore, no DB
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tuned_runtime_withholds_down_to_initial_target() {
        let semaphore = Arc::new(Semaphore::new(100));
        let runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        assert_eq!(semaphore.available_permits(), 20);
        assert_eq!(runtime.live_target(), 20);
    }

    #[tokio::test]
    async fn resize_grow_releases_withheld_permits() {
        let semaphore = Arc::new(Semaphore::new(100));
        let mut runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        let new_target = runtime.resize_toward(30);
        assert_eq!(new_target, 30);
        assert_eq!(semaphore.available_permits(), 30);
    }

    #[tokio::test]
    async fn resize_shrink_is_opportunistic_and_never_cancels_in_flight() {
        let semaphore = Arc::new(Semaphore::new(100));
        let mut runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        assert_eq!(runtime.live_target(), 20);

        // Simulate 18 of the 20 dispatchable permits being held by in-flight
        // tasks; 2 remain free for the shrink to reclaim.
        let mut in_flight = Vec::new();
        for _ in 0..18 {
            in_flight.push(Arc::clone(&semaphore).try_acquire_owned().unwrap());
        }
        assert_eq!(semaphore.available_permits(), 2);

        // Ask to shrink to 10: only the 2 free permits can be withheld this
        // tick — the 18 in-flight permits are untouched.
        let new_target = runtime.resize_toward(10);
        assert_eq!(
            new_target, 18,
            "shrink is opportunistic, bounded by free permits"
        );
        assert_eq!(semaphore.available_permits(), 0);

        // Release 8 in-flight permits; a follow-up resize call catches up
        // toward the original target of 10.
        for _ in 0..8 {
            in_flight.pop();
        }
        let new_target = runtime.resize_toward(10);
        assert_eq!(new_target, 10);
    }

    #[tokio::test]
    async fn cancellation_releases_all_withheld_permits_for_drain() {
        let semaphore = Arc::new(Semaphore::new(100));
        let runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        assert_eq!(semaphore.available_permits(), 20);

        let cancel = CancellationToken::new();
        let telemetry = Arc::new(TelemetryConfig::default());
        let handle = spawn_slot_tuner_loop(
            runtime,
            Arc::new(DefaultSlotTuner::default()),
            SlotType::Workflow,
            || None,
            Arc::new(AtomicU64::new(0)),
            cancel.clone(),
            Duration::from_secs(3600),
            telemetry,
        );

        cancel.cancel();
        handle.await.expect("tuner loop task must not panic");

        // All 100 permits must now be acquirable — drain_in_flight's
        // acquire_many(max_slots) must be able to complete.
        let _all = semaphore.acquire_many(100).await.unwrap();
    }

    #[tokio::test]
    async fn tuner_loop_applies_decision_each_tick() {
        struct AlwaysGrow;
        impl SlotTuner for AlwaysGrow {
            fn decide(&self, _observations: &SlotObservations) -> SlotTunerAction {
                SlotTunerAction::Grow(3)
            }
            fn name(&self) -> &'static str {
                "always-grow"
            }
        }

        let semaphore = Arc::new(Semaphore::new(30));
        let runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 5, 5, 30);
        let cancel = CancellationToken::new();
        let telemetry = Arc::new(TelemetryConfig::default());
        let handle = spawn_slot_tuner_loop(
            runtime,
            Arc::new(AlwaysGrow),
            SlotType::Activity,
            || None,
            Arc::new(AtomicU64::new(0)),
            cancel.clone(),
            Duration::from_millis(5),
            telemetry,
        );

        // Give the loop several ticks to converge and clamp at max_slots.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(semaphore.available_permits(), 30);

        cancel.cancel();
        handle.await.expect("tuner loop task must not panic");
    }
}
