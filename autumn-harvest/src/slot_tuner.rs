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
//! **Grow** releases withheld permits (making them available for dispatch).
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

/// Pick the most-saturated reading from a set of per-pool pressure samples
/// (issue #548 review).
///
/// A multi-shard worker dispatches against several independent DB pools; the
/// tuner must shrink if *any* of them is under pressure, not merely whichever
/// pool happens to be sampled. Readings are ordered by
/// `(is_saturated, waiting)`, so a saturated reading always outranks a
/// non-saturated one, and among saturated readings the one with the most
/// waiters wins. Returns `None` for an empty slice.
#[must_use]
pub fn worst_pool_pressure(readings: &[PoolPressure]) -> Option<PoolPressure> {
    readings
        .iter()
        .copied()
        .max_by_key(|p| (p.is_saturated(), p.waiting))
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
/// or panicking implementation still delays the next tick for both slot
/// types (workflow and activity share one control-loop task).
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
    /// semaphores share this same instance (its `decide` method is pure and
    /// stateless, so sharing is safe) and this one `[min_slots, max_slots]`
    /// band (per-type bands are a documented follow-up, not this slice).
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

/// Normalize a possibly-degenerate `[min_slots, max_slots]` band.
///
/// `max_slots` is the hard safety cap and always wins: when `min_slots >
/// max_slots` the band collapses to the single point `max_slots` rather than
/// producing an invalid `(lo, hi)` pair with `lo > hi` — capacity is never
/// widened past the operator's stated ceiling just because the floor was
/// misconfigured above it. This makes `[min_slots, max_slots]` a true
/// mathematical interval (`lo <= hi`) everywhere it's consumed, so callers
/// (notably `.clamp()`, which panics on `min > max`) never see an invalid
/// range.
#[must_use]
pub const fn effective_band(min_slots: usize, max_slots: usize) -> (usize, usize) {
    if min_slots <= max_slots {
        (min_slots, max_slots)
    } else {
        (max_slots, max_slots)
    }
}

/// Clamp `configured_max` into the initial live target for a tuned slot.
///
/// `configured_max` is the worker's static `max_concurrent_*` value; the
/// result is clamped into `[min_slots, max_slots]`. Tolerates a degenerate
/// (`min_slots > max_slots`) band via [`effective_band`].
#[must_use]
pub const fn initial_target(configured_max: usize, min_slots: usize, max_slots: usize) -> usize {
    let (min_slots, max_slots) = effective_band(min_slots, max_slots);
    if configured_max < min_slots {
        min_slots
    } else if configured_max > max_slots {
        max_slots
    } else {
        configured_max
    }
}

/// Apply a [`SlotTunerAction`] to `current`, clamped to `[min_slots, max_slots]`.
/// Tolerates a degenerate (`min_slots > max_slots`) band via [`effective_band`].
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
    let (min_slots, max_slots) = effective_band(min_slots, max_slots);
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
/// Returns human-readable warning strings for conditions that degrade the
/// tuner but leave the worker able to dispatch (a degenerate `min_slots >
/// max_slots` band, or a configured value outside the band) — these never
/// fail worker startup, matching the `queue_weights` precedent. `max_slots ==
/// 0` is **not** included here: it makes the dispatch semaphore permanently
/// empty, which is a hard startup failure handled separately by
/// `WorkerRuntimeConfig::validate` (a warning would leave a "healthy-looking"
/// worker that can never dispatch a single task).
#[must_use]
pub fn validate_band(min_slots: usize, max_slots: usize, configured_max: usize) -> Vec<String> {
    let mut warnings = Vec::new();
    if min_slots > max_slots {
        warnings.push(format!(
            "slot_tuner min_slots ({min_slots}) is greater than max_slots ({max_slots}); \
             the tuner will be inert at max_slots"
        ));
    }
    if configured_max < min_slots || configured_max > max_slots {
        warnings.push(format!(
            "configured max_concurrent value ({configured_max}) is outside the slot_tuner band \
             [{min_slots}, {max_slots}]; the initial live target will be clamped into the band"
        ));
    }
    warnings
}

/// Compute `(in_use, available)` dispatch-slot counts from a semaphore
/// reading against a target (issue #531; shared with `worker.rs`'s
/// slot-occupancy sampler and this module's own control loop so both derive
/// occupancy the same way).
///
/// `available` is clamped to `target` so a transient over-count (e.g. a
/// permit released just before the read) can never produce a negative
/// `in_use`. Invariant: `in_use + available == target`.
///
/// Note: `available_permits()` on a semaphore behind a [`TunedSlotRuntime`]
/// already excludes any withheld permits (they are genuinely acquired and
/// held, not merely reserved), so no separate "subtract withheld" step is
/// needed — passing the live target as `target` and a raw
/// `Semaphore::available_permits()` reading as `available_permits` here is
/// sufficient and correct for both a tuned and an untuned semaphore.
#[must_use]
pub(crate) const fn slot_occupancy(target: usize, available_permits: usize) -> (u64, u64) {
    let available = if available_permits > target {
        target
    } else {
        available_permits
    };
    let in_use = target - available;
    (in_use as u64, available as u64)
}

/// Runtime handle for one tuned dispatch semaphore.
///
/// Owns the permits currently withheld from dispatch (the gap between
/// `max_slots` and the live target) and the shared `live_target` cell the
/// worker's slot-occupancy sampler (issue #531) reads so its gauges stay
/// consistent with the tuned value rather than the static configured max.
///
/// `min_slots`/`max_slots` are normalized via [`effective_band`] at
/// construction, so `min_slots <= max_slots` is a class invariant for every
/// live `TunedSlotRuntime` — callers (notably `resize_toward`'s `.clamp()`,
/// which panics on an invalid range) never need to re-check band validity.
#[derive(Debug)]
pub struct TunedSlotRuntime {
    semaphore: Arc<Semaphore>,
    live_target: Arc<AtomicUsize>,
    /// Permits currently withheld from dispatch. May contain a mix of one
    /// "large" merged permit (from the bulk acquire at construction) and
    /// several single-permit entries (each pushed by a later opportunistic
    /// shrink) — `resize_toward`'s grow path is permit-count-aware and
    /// correctly releases only part of an entry when needed.
    withheld: Vec<OwnedSemaphorePermit>,
    min_slots: usize,
    max_slots: usize,
}

impl TunedSlotRuntime {
    /// Create a runtime over a semaphore that already has `max_slots` total
    /// permits, immediately withholding permits down to the clamped initial
    /// target derived from `configured_max`.
    ///
    /// The initial withhold is a single bulk `try_acquire_many_owned` call
    /// (not a per-permit loop): the semaphore was just created with exactly
    /// `max_slots` permits and nothing has been dispatched yet, so the full
    /// `to_withhold` amount is always available in one shot.
    #[must_use]
    pub fn new(
        semaphore: Arc<Semaphore>,
        configured_max: usize,
        min_slots: usize,
        max_slots: usize,
    ) -> Self {
        let (min_slots, max_slots) = effective_band(min_slots, max_slots);
        let target = initial_target(configured_max, min_slots, max_slots);
        let to_withhold = max_slots.saturating_sub(target);
        // Tracks what was actually withheld, not just what was intended.
        // The semaphore was constructed with fewer than `max_slots` permits
        // (a caller bug) is the only way the acquire below can fail — stop
        // withholding rather than panicking, but fall the live target back
        // to the semaphore's true available count so `slot_occupancy`
        // accounting (which trusts `live_target`) can't silently diverge
        // from the semaphore's actual, unconstrained capacity.
        let (withheld, actual_target) = match u32::try_from(to_withhold) {
            Ok(0) => (Vec::new(), target),
            Ok(n) => Arc::clone(&semaphore)
                .try_acquire_many_owned(n)
                .map_or_else(
                    |_| (Vec::new(), semaphore.available_permits()),
                    |permit| (vec![permit], target),
                ),
            Err(_) => (Vec::new(), semaphore.available_permits()),
        };
        Self {
            semaphore,
            live_target: Arc::new(AtomicUsize::new(actual_target)),
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
            let mut remaining = desired - current;
            while remaining > 0 {
                let Some(mut permit) = self.withheld.pop() else {
                    break;
                };
                let held = permit.num_permits();
                if held <= remaining {
                    remaining -= held;
                    drop(permit);
                } else if let Some(release) = permit.split(remaining) {
                    drop(release);
                    self.withheld.push(permit);
                    remaining = 0;
                } else {
                    // Should be unreachable (held > remaining implies
                    // split(remaining) succeeds), but never lose the
                    // permit if it somehow does.
                    self.withheld.push(permit);
                    break;
                }
            }
            let released = (desired - current) - remaining;
            self.live_target.fetch_add(released, Ordering::Relaxed);
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

/// One dispatch semaphore's tuned runtime plus its permit-wait accumulator.
///
/// Bundled so [`spawn_slot_tuner_loop`] can drive both slot types from one
/// shared control loop.
pub struct TunedSlot {
    /// The runtime this tick resizes.
    pub runtime: TunedSlotRuntime,
    /// Longest permit wait observed since the last tick; reset to 0 each
    /// tick via `swap`.
    pub permit_wait_micros: Arc<AtomicU64>,
    /// The bounded label this slot's telemetry is emitted under.
    pub slot_type: SlotType,
}

/// Apply one control-loop tick to a single [`TunedSlot`], given the tick's
/// shared pool-pressure sample. Returns the [`TunerDecision`] that took
/// effect, for the caller to emit telemetry with.
fn tick_one(
    slot: &mut TunedSlot,
    tuner: &dyn SlotTuner,
    pressure: Option<PoolPressure>,
) -> TunerDecision {
    let current_target = slot.runtime.live_target();
    let raw_available = slot.runtime.semaphore.available_permits();
    let (in_use, _available) = slot_occupancy(current_target, raw_available);

    let wait_micros = slot.permit_wait_micros.swap(0, Ordering::Relaxed);
    let max_permit_wait = if wait_micros == 0 {
        None
    } else {
        Some(Duration::from_micros(wait_micros))
    };

    let observations = SlotObservations {
        current_target,
        min_slots: slot.runtime.min_slots,
        max_slots: slot.runtime.max_slots,
        in_use: usize::try_from(in_use).unwrap_or(usize::MAX),
        pool: pressure,
        max_permit_wait,
    };

    let action = tuner.decide(&observations);
    let (new_target, decision) = apply_action(
        current_target,
        action,
        slot.runtime.min_slots,
        slot.runtime.max_slots,
    );
    slot.runtime.resize_toward(new_target);
    decision
}

/// Spawn the adaptive slot-tuner control loop for both dispatch slot types on
/// one shared cadence.
///
/// Runs as a single task with one `poll_interval` timer, via a
/// `tokio::select! { cancel, sleep }` loop mirroring
/// `poison_pill::spawn_poison_pill_reclaimer`. Worker DB-pool pressure is
/// sampled **exactly once per tick** and applied to both the workflow and
/// activity controller decisions — pool pressure is a worker-wide signal
/// (both slot types dispatch against the same connection pool), so sampling
/// it twice per tick (once per slot type, on independent timers) would
/// double the pool status-lock contention for no benefit. Each slot type
/// still gets its own independent `SlotObservations`, `decide()` call,
/// target, and telemetry emission — only the tick cadence and the
/// pool-pressure sample are shared.
///
/// This decouples the control loop from the hot dispatch path entirely:
/// dispatch only ever touches a lock-free `AtomicU64` per slot type to
/// record the longest recent permit wait, never the tuner itself.
///
/// Runs regardless of whether a metrics recorder is configured — unlike a
/// pure sampler, the tuner is a controller with a real effect on dispatch
/// capacity, so it must not be silently disabled by `is_enabled() == false`.
/// Telemetry emission (not the control decision) is what's gated.
///
/// On cancellation, every withheld permit (for both slot types) is released
/// before the task returns so a subsequent `drain_in_flight` can observe
/// full capacity.
///
/// Best-effort, not a hard rate guarantee: a controller that misbehaves (or a
/// custom `SlotTuner` implementation) can at most leave a target anywhere
/// within its own `[min_slots, max_slots]` — it can never withhold below the
/// floor.
pub fn spawn_slot_tuner_loop(
    mut workflow: TunedSlot,
    mut activity: TunedSlot,
    tuner: Arc<dyn SlotTuner>,
    pool_pressure: impl Fn() -> Option<PoolPressure> + Send + 'static,
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

            let pressure = pool_pressure();
            let workflow_decision = tick_one(&mut workflow, &*tuner, pressure);
            let activity_decision = tick_one(&mut activity, &*tuner, pressure);

            if telemetry.metrics.is_enabled() {
                telemetry.metrics.record_worker_slot_target(
                    workflow.slot_type,
                    workflow.runtime.live_target() as u64,
                );
                telemetry
                    .metrics
                    .record_tuner_decision(workflow.slot_type, workflow_decision);
                telemetry.metrics.record_worker_slot_target(
                    activity.slot_type,
                    activity.runtime.live_target() as u64,
                );
                telemetry
                    .metrics
                    .record_tuner_decision(activity.slot_type, activity_decision);
            }
        }

        workflow.runtime.release_all_withheld();
        activity.runtime.release_all_withheld();
    })
}

#[cfg(test)]
impl TunedSlot {
    fn new_for_test(runtime: TunedSlotRuntime, slot_type: SlotType) -> Self {
        Self {
            runtime,
            permit_wait_micros: Arc::new(AtomicU64::new(0)),
            slot_type,
        }
    }
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
    fn apply_action_tolerates_degenerate_band_without_panicking() {
        // min > max: effective_band collapses to (5, 5); a Grow request must
        // clamp to the single point rather than panicking on an invalid range.
        let (target, decision) = apply_action(5, SlotTunerAction::Grow(10), 50, 5);
        assert_eq!(target, 5);
        assert_eq!(decision, TunerDecision::Hold);
    }

    #[test]
    fn initial_target_clamps_configured_max_into_band() {
        assert_eq!(initial_target(20, 4, 10), 10);
        assert_eq!(initial_target(20, 30, 100), 30);
        assert_eq!(initial_target(20, 4, 100), 20);
    }

    #[test]
    fn initial_target_tolerates_degenerate_band() {
        assert_eq!(initial_target(20, 50, 5), 5);
    }

    // -----------------------------------------------------------------------
    // effective_band
    // -----------------------------------------------------------------------

    #[test]
    fn effective_band_passes_through_valid_range() {
        assert_eq!(effective_band(2, 40), (2, 40));
        assert_eq!(effective_band(5, 5), (5, 5));
    }

    #[test]
    fn effective_band_collapses_degenerate_range_to_max() {
        assert_eq!(effective_band(50, 5), (5, 5));
    }

    // -----------------------------------------------------------------------
    // slot_occupancy
    // -----------------------------------------------------------------------

    #[test]
    fn slot_occupancy_invariant_holds() {
        for max in [0usize, 1, 8, 20, 100] {
            for avail in 0..=max {
                let (in_use, available) = slot_occupancy(max, avail);
                assert_eq!(in_use + available, max as u64);
                assert_eq!(available, avail as u64);
                assert_eq!(in_use, (max - avail) as u64);
            }
        }
    }

    #[test]
    fn slot_occupancy_clamps_over_count() {
        let (in_use, available) = slot_occupancy(8, 12);
        assert_eq!(in_use, 0);
        assert_eq!(available, 8);
    }

    #[test]
    fn slot_occupancy_reports_full_availability_when_nothing_withheld_or_in_flight() {
        // Regression test for the tuned_available double-subtraction bug
        // (issue #548 review): a real semaphore's available_permits() after
        // TunedSlotRuntime withholding already excludes withheld permits, so
        // occupancy against the live target alone (no separate "subtract
        // withheld" step) must report the full target as available when
        // nothing is in flight.
        let (in_use, available) = slot_occupancy(20, 20);
        assert_eq!(in_use, 0);
        assert_eq!(available, 20);
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
    // worst_pool_pressure (issue #548 review: multi-shard aggregation)
    // -----------------------------------------------------------------------

    fn idle_pressure() -> PoolPressure {
        PoolPressure {
            max_size: 10,
            size: 5,
            available: 5,
            waiting: 0,
        }
    }

    fn saturated_pressure(waiting: usize) -> PoolPressure {
        PoolPressure {
            max_size: 10,
            size: 10,
            available: 0,
            waiting,
        }
    }

    #[test]
    fn worst_pool_pressure_empty_slice_is_none() {
        assert_eq!(worst_pool_pressure(&[]), None);
    }

    #[test]
    fn worst_pool_pressure_all_idle_returns_idle() {
        let readings = [idle_pressure(), idle_pressure()];
        let worst = worst_pool_pressure(&readings).unwrap();
        assert!(!worst.is_saturated());
    }

    #[test]
    fn worst_pool_pressure_picks_the_saturated_shard_among_idle_ones() {
        // Simulates a 3-shard worker where only shard 2 is under pressure —
        // the tuner must still see it, not whichever pool sorts first.
        let readings = [idle_pressure(), saturated_pressure(1), idle_pressure()];
        let worst = worst_pool_pressure(&readings).unwrap();
        assert!(worst.is_saturated());
    }

    #[test]
    fn worst_pool_pressure_breaks_ties_toward_more_waiters() {
        let readings = [
            saturated_pressure(2),
            saturated_pressure(9),
            saturated_pressure(4),
        ];
        let worst = worst_pool_pressure(&readings).unwrap();
        assert_eq!(worst.waiting, 9);
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
    fn validate_band_does_not_flag_zero_max() {
        // max_slots == 0 is a hard startup error (WorkerRuntimeConfig::validate),
        // not a soft warning here — see the doc comment on validate_band.
        let warnings = validate_band(0, 0, 0);
        assert!(!warnings.iter().any(|w| w.contains("max_slots is 0")));
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
    async fn tuned_runtime_construction_uses_single_bulk_acquire() {
        // The withheld set should be a single merged permit (bulk acquire),
        // not 80 individual entries, after construction.
        let semaphore = Arc::new(Semaphore::new(100));
        let runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        assert_eq!(runtime.withheld.len(), 1);
        assert_eq!(runtime.withheld[0].num_permits(), 80);
    }

    #[tokio::test]
    async fn tuned_runtime_falls_back_live_target_when_withhold_acquire_fails() {
        // Regression test (issue #548 review): a semaphore constructed with
        // fewer than `max_slots` permits (a caller bug) makes the withhold
        // acquire fail. The live target must then fall back to what the
        // semaphore actually reports, not the lower intended `target` — a
        // stale lower target would make `slot_occupancy` silently
        // under-report `in_use` since more capacity is truly available than
        // the target claims.
        let semaphore = Arc::new(Semaphore::new(3));
        let runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 2, 2, 10);
        // target = clamp(2, [2, 10]) = 2, to_withhold = 10 - 2 = 8, but only
        // 3 permits actually exist — the acquire fails.
        assert!(runtime.withheld.is_empty());
        assert_eq!(runtime.live_target(), 3);
        assert_eq!(runtime.live_target(), semaphore.available_permits());
    }

    #[tokio::test]
    async fn tuned_runtime_with_degenerate_band_does_not_panic_and_settles_at_max() {
        let semaphore = Arc::new(Semaphore::new(5));
        let mut runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 50, 5);
        // effective_band(50, 5) == (5, 5): target must be 5, not panic.
        assert_eq!(runtime.live_target(), 5);
        // resize_toward must not panic even when asked to move far outside
        // the (collapsed) band.
        let new_target = runtime.resize_toward(100);
        assert_eq!(new_target, 5);
        let new_target = runtime.resize_toward(0);
        assert_eq!(new_target, 5);
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
    async fn resize_grow_partially_splits_the_bulk_withheld_permit() {
        // Growing by less than the full withheld amount must release only
        // the requested portion, keeping the remainder withheld (not release
        // the whole merged permit).
        let semaphore = Arc::new(Semaphore::new(100));
        let mut runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 20, 10, 100);
        assert_eq!(semaphore.available_permits(), 20);

        let new_target = runtime.resize_toward(23);
        assert_eq!(new_target, 23);
        assert_eq!(semaphore.available_permits(), 23);

        // A further partial grow continues to split from the same
        // remaining withheld permit correctly.
        let new_target = runtime.resize_toward(25);
        assert_eq!(new_target, 25);
        assert_eq!(semaphore.available_permits(), 25);
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
        let workflow_semaphore = Arc::new(Semaphore::new(100));
        let activity_semaphore = Arc::new(Semaphore::new(50));
        let workflow_runtime = TunedSlotRuntime::new(Arc::clone(&workflow_semaphore), 20, 10, 100);
        let activity_runtime = TunedSlotRuntime::new(Arc::clone(&activity_semaphore), 10, 5, 50);
        assert_eq!(workflow_semaphore.available_permits(), 20);
        assert_eq!(activity_semaphore.available_permits(), 10);

        let cancel = CancellationToken::new();
        let telemetry = Arc::new(TelemetryConfig::default());
        let handle = spawn_slot_tuner_loop(
            TunedSlot::new_for_test(workflow_runtime, SlotType::Workflow),
            TunedSlot::new_for_test(activity_runtime, SlotType::Activity),
            Arc::new(DefaultSlotTuner::default()),
            || None,
            cancel.clone(),
            Duration::from_secs(3600),
            telemetry,
        );

        cancel.cancel();
        handle.await.expect("tuner loop task must not panic");

        // All permits must now be acquirable — drain_in_flight's
        // acquire_many(max_slots) must be able to complete for both semaphores.
        let _wf = workflow_semaphore.acquire_many(100).await.unwrap();
        let _act = activity_semaphore.acquire_many(50).await.unwrap();
    }

    #[tokio::test]
    async fn tuner_loop_applies_decision_each_tick_for_both_slot_types() {
        struct AlwaysGrow;
        impl SlotTuner for AlwaysGrow {
            fn decide(&self, _observations: &SlotObservations) -> SlotTunerAction {
                SlotTunerAction::Grow(3)
            }
            fn name(&self) -> &'static str {
                "always-grow"
            }
        }

        let workflow_semaphore = Arc::new(Semaphore::new(30));
        let activity_semaphore = Arc::new(Semaphore::new(15));
        let workflow_runtime = TunedSlotRuntime::new(Arc::clone(&workflow_semaphore), 5, 5, 30);
        let activity_runtime = TunedSlotRuntime::new(Arc::clone(&activity_semaphore), 3, 3, 15);
        let cancel = CancellationToken::new();
        let telemetry = Arc::new(TelemetryConfig::default());
        let handle = spawn_slot_tuner_loop(
            TunedSlot::new_for_test(workflow_runtime, SlotType::Workflow),
            TunedSlot::new_for_test(activity_runtime, SlotType::Activity),
            Arc::new(AlwaysGrow),
            || None,
            cancel.clone(),
            Duration::from_millis(5),
            telemetry,
        );

        // Give the loop several ticks to converge and clamp both slot types
        // at their respective max_slots.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(workflow_semaphore.available_permits(), 30);
        assert_eq!(activity_semaphore.available_permits(), 15);

        cancel.cancel();
        handle.await.expect("tuner loop task must not panic");
    }

    #[tokio::test]
    async fn tuner_loop_samples_pool_pressure_once_per_tick_for_both_slot_types() {
        // Regression test: pool pressure must be sampled ONCE per tick and
        // shared between the workflow and activity decisions, not sampled
        // independently per slot type.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = Arc::clone(&calls);

        let workflow_semaphore = Arc::new(Semaphore::new(10));
        let activity_semaphore = Arc::new(Semaphore::new(10));
        let workflow_runtime = TunedSlotRuntime::new(Arc::clone(&workflow_semaphore), 5, 5, 10);
        let activity_runtime = TunedSlotRuntime::new(Arc::clone(&activity_semaphore), 5, 5, 10);
        let cancel = CancellationToken::new();
        let telemetry = Arc::new(TelemetryConfig::default());
        let handle = spawn_slot_tuner_loop(
            TunedSlot::new_for_test(workflow_runtime, SlotType::Workflow),
            TunedSlot::new_for_test(activity_runtime, SlotType::Activity),
            Arc::new(DefaultSlotTuner::default()),
            move || {
                calls_for_closure.fetch_add(1, Ordering::Relaxed);
                None
            },
            cancel.clone(),
            Duration::from_millis(20),
            telemetry,
        );

        tokio::time::sleep(Duration::from_millis(110)).await;
        cancel.cancel();
        handle.await.expect("tuner loop task must not panic");

        // ~5 ticks elapsed (110ms / 20ms); pool_pressure must be called
        // exactly once per tick, not twice (once per slot type).
        let observed = calls.load(Ordering::Relaxed);
        assert!(
            (3..=7).contains(&observed),
            "expected ~5 pool_pressure calls (one per tick), got {observed}"
        );
    }
}
