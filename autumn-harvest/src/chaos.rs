//! Deterministic chaos / fault-injection test harness (issue #940).
//!
//! This module lets tests inject faults — a killed worker task, an injected
//! Diesel/connection error, a dropped `LISTEN`/`NOTIFY` wake, an expired lease —
//! at *named* points in the production code path, deterministically and
//! reproducibly, to prove that the engine's convergence guarantees hold under
//! adversarial timing.
//!
//! ## Two halves
//!
//! - [`points`] is **unconditional** (compiled into every build). It is a
//!   const catalogue of named injection points; a [`points::ChaosPoint`] value
//!   can only ever be a catalogue const, so a typo is a *compile error*, never
//!   a silent runtime no-op (issue #940 AC2). In a non-`chaos` build the
//!   `chaos_point!` / `chaos_fallible!` macros expand to a compile-time const
//!   check that emits **zero** runtime code — the hot path is untouched.
//! - The controller ([`arm`], [`hit`], [`ChaosPlan`], [`ChaosGuard`], ...) is
//!   `#[cfg(feature = "chaos")]`. It exists only in a build with the `chaos`
//!   feature enabled — a test-only build. It is never part of a production
//!   binary.
//!
//! ## Zero production impact (AC6)
//!
//! The `chaos` feature is off by default and never in `default`. When it is
//! off, an injection point is a `const _: ChaosPoint = ...;` item that the
//! compiler discards, so there is no branch, no atomic load, no code at all at
//! the call site. When the feature is *on* but the harness is disarmed, [`hit`]
//! is a single `Relaxed` atomic load followed by an early return with no lock
//! and no `.await` yield.
//!
//! ## Determinism (AC3)
//!
//! Randomised plans are derived from a `u64` seed with a hand-rolled
//! [`splitmix64`] over per-point independent streams (`splitmix64(seed ^
//! fnv1a(point))`). The same seed always produces the same plan. `rand`'s
//! `StdRng` is deliberately *not* used: its output is not guaranteed stable
//! across crate versions, which would silently invalidate a recorded
//! reproducer seed.
#![cfg_attr(
    feature = "chaos",
    allow(
        // The `Chaos*` prefix is the harness's deliberate public vocabulary
        // (`chaos::ChaosPlan`, `chaos::ChaosGuard`, ...); renaming to bare
        // `Plan`/`Guard`/`Error` would collide with the crate's own `Error`
        // and read worse at the call site.
        clippy::module_name_repetitions,
        // `ChaosGuard` deliberately holds the process-wide serialization guard
        // for its whole lifetime (RAII); that is the point, not a mistake.
        clippy::significant_drop_tightening,
    )
)]

pub mod points {
    //! Const catalogue of chaos injection points (compiled into every build).

    use core::fmt;

    /// Primitive classes a [`ChaosPoint`] supports when a *seeded* plan picks an
    /// action for it. Explicit (scripted) plans ignore these caps — a
    /// `hold_at`/`kill_at` on any point is honoured regardless.
    type Caps = u8;

    /// The point runs inside a spawned task, so a panic there simulates a task
    /// crash rather than crashing the test driver.
    pub const CAP_KILL: Caps = 1 << 0;
    /// The point is at a `?`-returning site (a `chaos_fallible!`), so it can
    /// return an injected error.
    pub const CAP_ERROR: Caps = 1 << 1;
    /// The point is a `LISTEN`/`NOTIFY` send site whose wake can be dropped.
    pub const CAP_DROP_NOTIFY: Caps = 1 << 2;
    /// The point can tolerate a bounded artificial delay.
    pub const CAP_DELAY: Caps = 1 << 3;

    /// A named chaos injection point.
    ///
    /// A newtype over `&'static str` with a **private** name field: a value can
    /// only be constructed inside this module, so downstream code can only ever
    /// reference a real catalogue const. A misspelled point name is therefore a
    /// compile error (issue #940 AC2), never a silent runtime miss.
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ChaosPoint {
        name: &'static str,
        caps: Caps,
    }

    impl ChaosPoint {
        /// The point's stable string name (the key used in plans and traces).
        #[must_use]
        pub const fn name(self) -> &'static str {
            self.name
        }

        /// The primitive-class bitset a seeded plan may pick from.
        #[must_use]
        pub const fn caps(self) -> u8 {
            self.caps
        }
    }

    impl fmt::Display for ChaosPoint {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.name)
        }
    }

    impl fmt::Debug for ChaosPoint {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ChaosPoint({})", self.name)
        }
    }

    // ---- catalogue ----------------------------------------------------------
    //
    // Add a point here, wire exactly one `chaos_point!`/`chaos_fallible!` call
    // site for it, add it to `ALL`, and (when it earns its keep) document it in
    // docs/testing/chaos.md. `CHAOS_POINTS_MAX` is a ratchet: bump it
    // deliberately, never silently.

    /// Top of `queue::park_workflow_task_inner`, before the park CTE runs.
    ///
    /// Race window for issue #601 (a wake landing between the pre-park check
    /// and the park's atomic `UPDATE`). Shared by both park callers
    /// (`park_workflow_task` and `park_workflow_task_preserving_capability_misses`),
    /// so a `hits()`-count reproducer must isolate the path it drives.
    pub const QUEUE_PARK_BEFORE_UPDATE: ChaosPoint = ChaosPoint {
        name: "queue.park.before_update",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// Inside `worker::process_workflow_task`'s persist transaction, just before
    /// the outer commit `.await`. Race window for issue #367 (worker death
    /// after claim but before the terminal transition commits).
    pub const WORKER_PERSIST_BEFORE_COMMIT: ChaosPoint = ChaosPoint {
        name: "worker.persist.before_commit",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// After the worker's outer persist commit, before the deferred-trigger
    /// fan-out. Discovery/convergence point (a crash here leaves committed work
    /// whose follow-up side effects have not fired).
    pub const WORKER_AFTER_OUTER_COMMIT: ChaosPoint = ChaosPoint {
        name: "worker.persist.after_outer_commit",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// Inside `worker::persist_external_signal_inline`, on the fresh-request
    /// (`!already_requested`) `Signal` arm.
    ///
    /// Fires after the `ExternalSignalRequested` event is appended but before
    /// the terminal delivery/failure event — still inside the (uncommitted)
    /// transaction. Race window for issue #492
    /// (an outbox sweep observing a half-written external-signal). Only the
    /// same-shard signal path is instrumented; the cancel/await arms are not.
    pub const OUTBOX_INLINE_AFTER_REQUESTED: ChaosPoint = ChaosPoint {
        name: "outbox.inline.after_requested",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// In `scheduler::claim_and_fire_workflow_schedule`, after the claim
    /// `UPDATE` commits but before the fresh re-read. Race window for issue
    /// #350 (the claiming replica crashing mid-fire).
    pub const SCHED_AFTER_CLAIM: ChaosPoint = ChaosPoint {
        name: "scheduler.after_claim",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// In `scheduler::claim_and_fire_workflow_schedule`, after the workflow
    /// start commits but before `next_run_at` is advanced. Race window for
    /// issue #350 (double-fire on crash-recovery).
    pub const SCHED_AFTER_START_BEFORE_ADVANCE: ChaosPoint = ChaosPoint {
        name: "scheduler.after_start.before_advance",
        caps: CAP_KILL | CAP_DELAY,
    };

    /// Top of `poison_pill::reclaim_orphaned_tasks`, before the orphan SELECT.
    ///
    /// A `chaos_fallible!` site: an injected Diesel/connection error surfaces
    /// here (AC1(b)) exactly like a real DB failure and is returned to the poll
    /// loop, which retries on its next tick. The reclaim is idempotent, so the
    /// retry converges (no orphan is left stranded by the transient error).
    pub const POISON_RECLAIM_BEFORE_LOAD: ChaosPoint = ChaosPoint {
        name: "poison.reclaim.before_load",
        caps: CAP_ERROR,
    };

    /// In `notify::notify_task_enqueued`, guarding the `pg_notify` send.
    ///
    /// A dropped wake (AC1(c)) means a listening worker never receives the
    /// `LISTEN`/`NOTIFY` and must fall back to its poll loop to claim the task.
    /// The invariant it stresses: dispatch converges even when every wake is
    /// lost, because the poll loop is the source of truth and NOTIFY is only a
    /// latency optimization.
    pub const NOTIFY_TASK_ENQUEUED: ChaosPoint = ChaosPoint {
        name: "notify.task_enqueued",
        caps: CAP_DROP_NOTIFY,
    };

    /// Every catalogue point, in a stable order.
    pub const ALL: &[ChaosPoint] = &[
        QUEUE_PARK_BEFORE_UPDATE,
        WORKER_PERSIST_BEFORE_COMMIT,
        WORKER_AFTER_OUTER_COMMIT,
        OUTBOX_INLINE_AFTER_REQUESTED,
        SCHED_AFTER_CLAIM,
        SCHED_AFTER_START_BEFORE_ADVANCE,
        POISON_RECLAIM_BEFORE_LOAD,
        NOTIFY_TASK_ENQUEUED,
    ];

    /// Ratchet on the catalogue size. Bump deliberately when adding points.
    pub const CHAOS_POINTS_MAX: usize = 16;

    #[cfg(test)]
    mod point_tests {
        use super::*;

        #[test]
        fn catalogue_within_ratchet() {
            assert!(
                ALL.len() <= CHAOS_POINTS_MAX,
                "catalogue has {} points, exceeds CHAOS_POINTS_MAX={CHAOS_POINTS_MAX}; \
                 bump the ratchet deliberately",
                ALL.len()
            );
        }

        #[test]
        fn catalogue_names_unique() {
            let mut names: Vec<&str> = ALL.iter().map(|p| p.name()).collect();
            let before = names.len();
            names.sort_unstable();
            names.dedup();
            assert_eq!(before, names.len(), "duplicate point name in ALL");
        }

        #[test]
        fn catalogue_names_are_dotted_and_nonempty() {
            for p in ALL {
                assert!(!p.name().is_empty(), "empty point name");
                assert!(
                    p.name().contains('.'),
                    "point name {} should use dotted namespacing",
                    p.name()
                );
            }
        }
    }
}

#[cfg(feature = "chaos")]
pub use controller::{
    ChaosError, ChaosGuard, ChaosPlan, HoldHandle, arm, hit, hit_fallible, should_drop_notify,
};

#[cfg(feature = "chaos")]
mod controller {
    use super::points::{ALL, CAP_DELAY, CAP_DROP_NOTIFY, CAP_ERROR, CAP_KILL, ChaosPoint};
    use std::collections::BTreeMap;
    use std::fmt::Write as _;
    use std::panic::PanicHookInfo;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, LazyLock, Mutex, Once, PoisonError, RwLock};
    use std::time::Duration;

    /// Hard ceiling on how long a [`Hold`](Action::Hold) rendezvous blocks the
    /// production point before continuing anyway. A correctly-written test
    /// releases well within this; it exists only so a buggy test cannot hang
    /// the harness forever.
    const HOLD_MAX: Duration = Duration::from_secs(15);

    static ARMED: AtomicBool = AtomicBool::new(false);
    static STATE: RwLock<Option<Arc<ChaosState>>> = RwLock::new(None);
    static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));
    static HOOK_INSTALLED: Once = Once::new();

    /// An injected fault error. Surfaces at a [`chaos_fallible!`] site as
    /// whatever error type the enclosing function returns, via `From`, so the
    /// production error path handles it exactly like a real failure.
    #[derive(Debug, Clone)]
    pub enum ChaosError {
        /// A generic injected connection/query failure.
        Generic,
        /// A simulated `harvest_events (workflow_exec_id, event_id)` unique
        /// violation (the append-id race, issue #601 / #492).
        EventIdUnique,
    }

    impl std::fmt::Display for ChaosError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Generic => f.write_str("harvest-chaos-error: injected generic failure"),
                Self::EventIdUnique => {
                    f.write_str("harvest-chaos-error: injected event_id unique violation")
                }
            }
        }
    }

    impl std::error::Error for ChaosError {}

    impl From<ChaosError> for crate::error::HarvestError {
        fn from(e: ChaosError) -> Self {
            Self::Database(e.to_string())
        }
    }

    impl From<ChaosError> for diesel::result::Error {
        fn from(e: ChaosError) -> Self {
            match e {
                ChaosError::EventIdUnique => Self::DatabaseError(
                    diesel::result::DatabaseErrorKind::UniqueViolation,
                    Box::new(e.to_string()),
                ),
                ChaosError::Generic => Self::QueryBuilderError(Box::new(e)),
            }
        }
    }

    /// When (which hit ordinal) a directive fires.
    #[derive(Debug, Clone, Copy)]
    enum Trigger {
        /// Fire exactly on the `n`th hit (1-based), then pass.
        OnHit(u64),
        /// Fire on every hit.
        Every,
    }

    /// The action a directive takes when it fires.
    #[derive(Debug, Clone)]
    enum Action {
        /// Panic with `harvest-chaos-kill: {point}` — simulates a task crash.
        Kill,
        /// Two-phase rendezvous: signal the test the point was reached, then
        /// block until the test releases.
        Hold,
        /// Return an injected error (only meaningful at a `chaos_fallible!`).
        Error(ChaosError),
        /// Report that a `LISTEN`/`NOTIFY` wake should be dropped.
        DropNotify,
        /// Sleep for `n` milliseconds.
        Delay(u64),
    }

    #[derive(Debug, Clone)]
    struct Directive {
        trigger: Trigger,
        action: Action,
    }

    /// Two-phase rendezvous gate shared between the production point and the
    /// test that drives it.
    struct HoldGate {
        reached: tokio::sync::Notify,
        release: tokio::sync::Notify,
        reached_flag: AtomicBool,
        released: AtomicBool,
    }

    impl HoldGate {
        fn new() -> Self {
            Self {
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                reached_flag: AtomicBool::new(false),
                released: AtomicBool::new(false),
            }
        }

        /// Production side: mark reached, then block (bounded by [`HOLD_MAX`])
        /// until the test releases.
        async fn rendezvous(&self) {
            self.reached_flag.store(true, Ordering::Release);
            self.reached.notify_one();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            // Bounded so a buggy test can never hang the harness; a correct
            // test releases well inside this.
            let _ = tokio::time::timeout(HOLD_MAX, self.release.notified()).await;
        }

        /// Test side: wait until the production point reaches the rendezvous.
        async fn wait_reached(&self) {
            if self.reached_flag.load(Ordering::Acquire) {
                return;
            }
            self.reached.notified().await;
        }

        /// Test side: release the production point.
        fn do_release(&self) {
            self.released.store(true, Ordering::Release);
            self.release.notify_one();
        }
    }

    /// A test-side handle to a [`Hold`](Action::Hold) gate. Dropping it releases
    /// the gate, so a forgotten `release()` can never hang a parked point.
    pub struct HoldHandle {
        gate: Arc<HoldGate>,
    }

    impl HoldHandle {
        /// Wait until the production point reaches the rendezvous.
        pub async fn reached(&self) {
            self.gate.wait_reached().await;
        }

        /// Release the production point so it continues.
        pub fn release(&self) {
            self.gate.do_release();
        }
    }

    impl Drop for HoldHandle {
        fn drop(&mut self) {
            self.gate.do_release();
        }
    }

    /// A fault-injection plan: a set of per-point directives plus (optionally)
    /// the seed they were derived from.
    ///
    /// Build one with [`ChaosPlan::scripted`] (explicit directives, for a
    /// targeted reproducer) or [`ChaosPlan::seeded`] (a deterministic randomised
    /// plan, for the convergence sweep), then install it with [`arm`].
    pub struct ChaosPlan {
        seed: Option<u64>,
        directives: BTreeMap<&'static str, Directive>,
        holds: BTreeMap<&'static str, Arc<HoldGate>>,
    }

    impl ChaosPlan {
        /// An empty plan; add directives with the `*_at` builders.
        #[must_use]
        pub const fn scripted() -> Self {
            Self {
                seed: None,
                directives: BTreeMap::new(),
                holds: BTreeMap::new(),
            }
        }

        /// A deterministic randomised plan derived from `seed`.
        ///
        /// Each catalogue point gets an independent `splitmix64(seed ^
        /// fnv1a(name))` stream; roughly half the points are activated, each
        /// with an action drawn from its declared caps and a fire-on-hit
        /// ordinal. `Hold` is never selected (it needs a test to release it);
        /// this plan is for the unattended convergence sweep.
        #[must_use]
        pub fn seeded(seed: u64) -> Self {
            let mut directives = BTreeMap::new();
            for &p in ALL {
                let stream = splitmix64(seed ^ fnv1a(p.name()));
                // Activate ~half the eligible points.
                if stream & 1 == 0 {
                    continue;
                }
                let Some(action) = pick_seeded_action(p, stream) else {
                    continue;
                };
                let trigger = Trigger::OnHit(1 + (stream >> 32) % 3);
                directives.insert(p.name(), Directive { trigger, action });
            }
            Self {
                seed: Some(seed),
                directives,
                holds: BTreeMap::new(),
            }
        }

        /// Fire a [`Kill`](Action::Kill) on the first hit of `point`.
        #[must_use]
        pub fn kill_at(self, point: ChaosPoint) -> Self {
            self.kill_at_hit(point, 1)
        }

        /// Fire a [`Kill`](Action::Kill) on the `n`th hit of `point`.
        #[must_use]
        pub fn kill_at_hit(mut self, point: ChaosPoint, n: u64) -> Self {
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(n),
                    action: Action::Kill,
                },
            );
            self
        }

        /// Install a [`Hold`](Action::Hold) rendezvous on the first hit of
        /// `point`. Retrieve the test-side handle from the guard with
        /// [`ChaosGuard::hold`].
        #[must_use]
        pub fn hold_at(mut self, point: ChaosPoint) -> Self {
            self.holds.insert(point.name(), Arc::new(HoldGate::new()));
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(1),
                    action: Action::Hold,
                },
            );
            self
        }

        /// Inject `err` on every hit of `point` (only meaningful at a
        /// `chaos_fallible!` site).
        #[must_use]
        pub fn error_at(mut self, point: ChaosPoint, err: ChaosError) -> Self {
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::Every,
                    action: Action::Error(err),
                },
            );
            self
        }

        /// Drop the `LISTEN`/`NOTIFY` wake on the first hit of `point`.
        #[must_use]
        pub fn drop_notify_at(mut self, point: ChaosPoint) -> Self {
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(1),
                    action: Action::DropNotify,
                },
            );
            self
        }

        /// Delay `ms` milliseconds on the first hit of `point`.
        #[must_use]
        pub fn delay_at(mut self, point: ChaosPoint, ms: u64) -> Self {
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(1),
                    action: Action::Delay(ms),
                },
            );
            self
        }
    }

    /// Pick a seeded action for `p` from its declared caps (never `Hold`).
    fn pick_seeded_action(p: ChaosPoint, stream: u64) -> Option<Action> {
        let caps = p.caps();
        let mut eligible: Vec<Action> = Vec::new();
        if caps & CAP_ERROR != 0 {
            eligible.push(Action::Error(ChaosError::Generic));
        }
        if caps & CAP_DROP_NOTIFY != 0 {
            eligible.push(Action::DropNotify);
        }
        if caps & CAP_DELAY != 0 {
            eligible.push(Action::Delay(5));
        }
        if caps & CAP_KILL != 0 {
            eligible.push(Action::Kill);
        }
        if eligible.is_empty() {
            return None;
        }
        let len = u64::try_from(eligible.len()).unwrap_or(1);
        let idx = usize::try_from((stream >> 16) % len).unwrap_or(0);
        eligible.get(idx).cloned()
    }

    /// The armed, in-flight chaos state. One exists at a time (serialized by
    /// [`SERIAL`]).
    struct ChaosState {
        seed: Option<u64>,
        directives: BTreeMap<&'static str, Directive>,
        holds: BTreeMap<&'static str, Arc<HoldGate>>,
        hits: Mutex<BTreeMap<&'static str, u64>>,
        trace: Mutex<Vec<&'static str>>,
        actions_fired: AtomicU64,
    }

    /// What [`hit`] does after consulting the plan for a point.
    enum Resolved {
        Continue,
        Kill,
        Hold(Arc<HoldGate>),
        Error(ChaosError),
        DropNotify,
        Delay(u64),
    }

    impl Resolved {
        /// A short action name for a site-mismatch `debug_assert!` message.
        const fn kind(&self) -> &'static str {
            match self {
                Self::Continue => "Continue",
                Self::Kill => "Kill",
                Self::Hold(_) => "Hold",
                Self::Error(_) => "Error",
                Self::DropNotify => "DropNotify",
                Self::Delay(_) => "Delay",
            }
        }
    }

    impl ChaosState {
        fn from_plan(plan: ChaosPlan) -> Self {
            Self {
                seed: plan.seed,
                directives: plan.directives,
                holds: plan.holds,
                hits: Mutex::new(BTreeMap::new()),
                trace: Mutex::new(Vec::new()),
                actions_fired: AtomicU64::new(0),
            }
        }

        /// Record a hit on `point` and resolve the action to take. The hit
        /// counters lock is released before returning, so a subsequent panic
        /// (Kill) never leaves it poisoned across a lock guard.
        fn resolve(&self, point: ChaosPoint) -> Resolved {
            let name = point.name();
            let ordinal = {
                let mut counters = self.hits.lock().unwrap_or_else(PoisonError::into_inner);
                let n = counters.entry(name).or_insert(0);
                *n += 1;
                *n
            };
            self.trace
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(name);

            let Some(directive) = self.directives.get(name) else {
                return Resolved::Continue;
            };
            let fire = match directive.trigger {
                Trigger::OnHit(k) => ordinal == k,
                Trigger::Every => true,
            };
            if !fire {
                return Resolved::Continue;
            }
            // NB: `actions_fired` is NOT bumped here. It is bumped by the entry
            // point (`hit`/`hit_fallible`/`should_drop_notify`) only when that
            // site can actually *honor* the resolved action — so a caps/site
            // mismatch (e.g. an `Error` scripted at an infallible `chaos_point!`
            // site) is caught by a `debug_assert!` there and does not silently
            // inflate the anti-vacuity counter.
            match &directive.action {
                Action::Kill => Resolved::Kill,
                Action::Hold => self
                    .holds
                    .get(name)
                    .map_or(Resolved::Continue, |g| Resolved::Hold(Arc::clone(g))),
                Action::Error(e) => Resolved::Error(e.clone()),
                Action::DropNotify => Resolved::DropNotify,
                Action::Delay(ms) => Resolved::Delay(*ms),
            }
        }

        /// Record that a resolved directive was actually honored at its site
        /// (feeds the anti-vacuity `actions_fired` guard).
        fn mark_fired(&self) {
            self.actions_fired.fetch_add(1, Ordering::Relaxed);
        }

        fn hits_of(&self, point: ChaosPoint) -> u64 {
            self.hits
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(point.name())
                .copied()
                .unwrap_or(0)
        }

        fn release_all_holds(&self) {
            for gate in self.holds.values() {
                gate.do_release();
            }
        }

        fn describe(&self) -> String {
            let mut out = String::new();
            let _ = write!(out, "chaos plan seed={:?} directives=[", self.seed);
            for (i, (name, d)) in self.directives.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let _ = write!(out, "{name}->{:?}@{:?}", d.action, d.trigger);
            }
            let _ = write!(
                out,
                "] fired={} hits={:?} trace={:?}",
                self.actions_fired.load(Ordering::Relaxed),
                self.hits.lock().unwrap_or_else(PoisonError::into_inner),
                self.trace.lock().unwrap_or_else(PoisonError::into_inner),
            );
            out
        }
    }

    fn current_state() -> Option<Arc<ChaosState>> {
        STATE.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Install a chaining panic hook (once, idempotently) that prints a compact
    /// one-line note for an expected chaos kill and delegates every other panic
    /// to the hook captured at install time. Transparent for non-chaos panics,
    /// so parallel non-chaos tests are unaffected.
    fn install_quiet_hook_once() {
        HOOK_INSTALLED.call_once(|| {
            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
                if is_chaos_kill(info) {
                    eprintln!("[chaos] simulated crash ({})", panic_message(info));
                } else {
                    prev(info);
                }
            }));
        });
    }

    fn is_chaos_kill(info: &PanicHookInfo<'_>) -> bool {
        panic_message(info).starts_with("harvest-chaos-kill:")
    }

    fn panic_message(info: &PanicHookInfo<'_>) -> String {
        // Mirrors the shared `crate::error::panic_message` idiom, but for the
        // borrowed `&(dyn Any + Send)` a panic hook exposes (that helper takes an
        // owned `Box<dyn Any>` from `catch_unwind`, so it cannot be reused here).
        let payload = info.payload();
        payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| String::from("<non-string panic payload>"))
    }

    /// Install `plan` and arm the harness. Returns a [`ChaosGuard`] that
    /// disarms on drop.
    ///
    /// Chaos runs are serialized process-wide: `arm` awaits a global mutex, so
    /// at most one plan is armed at a time and cross-test bleed is impossible.
    /// The guard owns that mutex for its lifetime.
    pub async fn arm(plan: ChaosPlan) -> ChaosGuard {
        install_quiet_hook_once();
        let serial = SERIAL.lock().await;
        let state = Arc::new(ChaosState::from_plan(plan));
        *STATE.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&state));
        ARMED.store(true, Ordering::SeqCst);
        ChaosGuard {
            _serial: serial,
            state,
        }
    }

    /// The RAII handle returned by [`arm`]. Disarms the harness and releases the
    /// process-wide serialization lock on drop.
    pub struct ChaosGuard {
        _serial: tokio::sync::MutexGuard<'static, ()>,
        state: Arc<ChaosState>,
    }

    impl ChaosGuard {
        /// The test-side [`HoldHandle`] for a point armed with
        /// [`ChaosPlan::hold_at`].
        ///
        /// # Panics
        ///
        /// Panics if `point` was not installed with `hold_at` in the armed plan.
        #[must_use]
        pub fn hold(&self, point: ChaosPoint) -> HoldHandle {
            let gate = self
                .state
                .holds
                .get(point.name())
                .unwrap_or_else(|| panic!("no hold armed for {}", point.name()));
            HoldHandle {
                gate: Arc::clone(gate),
            }
        }

        /// How many times `point` has been hit since arming.
        #[must_use]
        pub fn hits(&self, point: ChaosPoint) -> u64 {
            self.state.hits_of(point)
        }

        /// How many directive actions have fired since arming (anti-vacuity
        /// guard: a reproducer asserts this is `>= 1`).
        #[must_use]
        pub fn actions_fired(&self) -> u64 {
            self.state.actions_fired.load(Ordering::Relaxed)
        }

        /// The seed this plan was derived from, if any.
        #[must_use]
        pub fn seed(&self) -> Option<u64> {
            self.state.seed
        }

        /// A compact, deterministic description of the plan and what has fired —
        /// embed it in a reproducer's assert message so a failure prints the
        /// seed and hit trace for one-command local replay (issue #940 AC3).
        #[must_use]
        pub fn diagnostics(&self) -> String {
            self.state.describe()
        }
    }

    impl Drop for ChaosGuard {
        fn drop(&mut self) {
            // Clear armed state first, *then* let `_serial` drop (after this
            // body) so another `arm()` can never observe a half-torn-down
            // state.
            ARMED.store(false, Ordering::SeqCst);
            *STATE.write().unwrap_or_else(PoisonError::into_inner) = None;
            self.state.release_all_holds();
        }
    }

    /// A named injection point in the production code path.
    ///
    /// The disarmed fast path is a single `Relaxed` atomic load and early
    /// return — no lock, no `.await` yield. Invoked via the `chaos_point!`
    /// macro; call sites never reference this directly.
    ///
    /// # Panics
    ///
    /// Panics with `harvest-chaos-kill: {point}` when the armed plan resolves a
    /// [`Kill`](Action::Kill) at `point`. That is the intended fault: it
    /// simulates a task crash and must be triggered inside a spawned task.
    pub async fn hit(point: ChaosPoint) {
        if !ARMED.load(Ordering::Relaxed) {
            return;
        }
        let Some(state) = current_state() else {
            return;
        };
        match state.resolve(point) {
            Resolved::Continue => {}
            Resolved::Kill => {
                state.mark_fired();
                panic!("harvest-chaos-kill: {}", point.name());
            }
            Resolved::Hold(gate) => {
                state.mark_fired();
                gate.rendezvous().await;
            }
            Resolved::Delay(ms) => {
                state.mark_fired();
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            // An infallible `chaos_point!` site cannot honor Error (needs a
            // `chaos_fallible!` site) or DropNotify (needs `chaos_drop_notify!`).
            // Catch such a caps/site mismatch loudly instead of silently
            // swallowing it and inflating the anti-vacuity counter.
            other @ (Resolved::Error(_) | Resolved::DropNotify) => {
                debug_assert!(
                    false,
                    "chaos: a {} directive is armed at `{}`, an infallible chaos_point!() site \
                     that cannot honor it — check the point's declared caps",
                    other.kind(),
                    point.name()
                );
            }
        }
    }

    /// A named injection point at a `?`-returning site.
    ///
    /// Like [`hit`], but an armed [`Error`](Action::Error) directive returns the
    /// injected [`ChaosError`], which the `?` at the call site converts (via
    /// `From`) into whatever error the enclosing function returns.
    ///
    /// # Errors
    ///
    /// Returns [`ChaosError`] when the armed plan resolves an
    /// [`Error`](Action::Error) at `point`.
    ///
    /// # Panics
    ///
    /// Panics with `harvest-chaos-kill: {point}` when the armed plan resolves a
    /// [`Kill`](Action::Kill) at `point`.
    pub async fn hit_fallible(point: ChaosPoint) -> Result<(), ChaosError> {
        if !ARMED.load(Ordering::Relaxed) {
            return Ok(());
        }
        let Some(state) = current_state() else {
            return Ok(());
        };
        match state.resolve(point) {
            Resolved::Continue => Ok(()),
            Resolved::Kill => {
                state.mark_fired();
                panic!("harvest-chaos-kill: {}", point.name());
            }
            Resolved::Hold(gate) => {
                state.mark_fired();
                gate.rendezvous().await;
                Ok(())
            }
            Resolved::Error(e) => {
                state.mark_fired();
                Err(e)
            }
            Resolved::Delay(ms) => {
                state.mark_fired();
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            }
            // A `chaos_fallible!` site cannot honor DropNotify (needs a
            // `chaos_drop_notify!` site) — catch the caps/site mismatch.
            other @ Resolved::DropNotify => {
                debug_assert!(
                    false,
                    "chaos: a {} directive is armed at `{}`, a fallible chaos_fallible!() site \
                     that cannot honor it — check the point's declared caps",
                    other.kind(),
                    point.name()
                );
                Ok(())
            }
        }
    }

    /// Whether the `LISTEN`/`NOTIFY` wake at `point` should be dropped.
    ///
    /// Consumes a hit and returns `true` only when the armed plan resolves a
    /// [`DropNotify`](Action::DropNotify) at `point`. The disarmed fast path is
    /// a single `Relaxed` atomic load.
    #[must_use]
    pub fn should_drop_notify(point: ChaosPoint) -> bool {
        if !ARMED.load(Ordering::Relaxed) {
            return false;
        }
        let Some(state) = current_state() else {
            return false;
        };
        match state.resolve(point) {
            Resolved::DropNotify => {
                state.mark_fired();
                true
            }
            Resolved::Continue => false,
            // A synchronous drop-notify site can only honor DropNotify; it cannot
            // await a Hold rendezvous, panic a Kill, sleep a Delay, or return an
            // Error. Catch the caps/site mismatch loudly (a scripted Hold here
            // would otherwise hang the test's `reached().await` forever).
            other => {
                debug_assert!(
                    false,
                    "chaos: a {} directive is armed at `{}`, a synchronous chaos_drop_notify!() \
                     site that can only honor DropNotify — check the point's declared caps",
                    other.kind(),
                    point.name()
                );
                false
            }
        }
    }

    /// splitmix64 — a fast, value-stable 64-bit mixer. Deliberately hand-rolled
    /// (not `rand::StdRng`) so a recorded reproducer seed stays reproducible
    /// across crate/toolchain versions.
    #[must_use]
    const fn splitmix64(seed: u64) -> u64 {
        let x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// FNV-1a 64-bit hash of a point name — mixed with the seed to give each
    /// point an independent `splitmix64` stream.
    #[must_use]
    const fn fnv1a(s: &str) -> u64 {
        let bytes = s.as_bytes();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut i = 0;
        while i < bytes.len() {
            hash ^= bytes[i] as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            i += 1;
        }
        hash
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::chaos::points::{
            NOTIFY_TASK_ENQUEUED, POISON_RECLAIM_BEFORE_LOAD, QUEUE_PARK_BEFORE_UPDATE,
        };

        // ---- deterministic mixers (AC3) ------------------------------------

        #[test]
        fn splitmix64_is_deterministic_and_disperses() {
            assert_eq!(splitmix64(0), splitmix64(0));
            assert_eq!(splitmix64(42), splitmix64(42));
            assert_ne!(splitmix64(0), splitmix64(1));
            assert_ne!(splitmix64(1), splitmix64(2));
        }

        #[test]
        fn fnv1a_is_deterministic_and_distinguishes_names() {
            assert_eq!(
                fnv1a("queue.park.before_update"),
                fnv1a("queue.park.before_update")
            );
            assert_ne!(
                fnv1a("queue.park.before_update"),
                fnv1a("notify.task_enqueued")
            );
            // Every catalogue name hashes to a distinct stream base, so no two
            // points share a seeded stream.
            let mut hashes: Vec<u64> = ALL.iter().map(|p| fnv1a(p.name())).collect();
            let before = hashes.len();
            hashes.sort_unstable();
            hashes.dedup();
            assert_eq!(before, hashes.len(), "two point names collide under fnv1a");
        }

        // ---- seeded plans (AC3) --------------------------------------------

        #[test]
        fn seeded_plan_is_reproducible_for_a_fixed_seed() {
            let a = ChaosPlan::seeded(12345);
            let b = ChaosPlan::seeded(12345);
            assert_eq!(a.seed, Some(12345));
            let ka: Vec<&&'static str> = a.directives.keys().collect();
            let kb: Vec<&&'static str> = b.directives.keys().collect();
            assert_eq!(ka, kb, "seeded plan point set differs across builds");
            for (name, da) in &a.directives {
                let db = b.directives.get(name).expect("same key set");
                assert_eq!(
                    format!("{:?}{:?}", da.action, da.trigger),
                    format!("{:?}{:?}", db.action, db.trigger),
                    "directive for {name} differs across builds",
                );
            }
        }

        #[test]
        fn seeded_plans_differ_across_seeds() {
            // Not a per-pair guarantee, but across a spread of seeds the
            // activated-point signatures must not all be identical.
            let sig = |seed: u64| -> Vec<&'static str> {
                let mut v: Vec<&'static str> =
                    ChaosPlan::seeded(seed).directives.keys().copied().collect();
                v.sort_unstable();
                v
            };
            let sigs: std::collections::BTreeSet<Vec<&'static str>> = (0u64..32).map(sig).collect();
            assert!(
                sigs.len() > 1,
                "every seed produced the same activated-point set"
            );
        }

        #[test]
        fn seeded_plan_never_selects_hold_and_respects_caps() {
            for seed in 0u64..200 {
                for (name, d) in &ChaosPlan::seeded(seed).directives {
                    assert!(
                        !matches!(d.action, Action::Hold),
                        "seeded plan chose Hold for {name} (seed {seed})",
                    );
                    let caps = ALL
                        .iter()
                        .find(|p| p.name() == *name)
                        .expect("directive names a catalogue point")
                        .caps();
                    let ok = match &d.action {
                        Action::Kill => caps & CAP_KILL != 0,
                        Action::Error(_) => caps & CAP_ERROR != 0,
                        Action::DropNotify => caps & CAP_DROP_NOTIFY != 0,
                        Action::Delay(_) => caps & CAP_DELAY != 0,
                        Action::Hold => false,
                    };
                    assert!(
                        ok,
                        "seeded action {:?} violates caps of {name} (seed {seed})",
                        d.action
                    );
                }
            }
        }

        #[test]
        fn pick_seeded_action_respects_caps_and_never_holds() {
            for stream in 0u64..64 {
                if let Some(a) = pick_seeded_action(POISON_RECLAIM_BEFORE_LOAD, stream) {
                    assert!(matches!(a, Action::Error(_)));
                }
                if let Some(a) = pick_seeded_action(NOTIFY_TASK_ENQUEUED, stream) {
                    assert!(matches!(a, Action::DropNotify));
                }
                if let Some(a) = pick_seeded_action(QUEUE_PARK_BEFORE_UPDATE, stream) {
                    assert!(matches!(a, Action::Kill | Action::Delay(_)));
                }
            }
        }

        // ---- scripted builders + resolve accounting ------------------------

        #[test]
        fn scripted_kill_at_fires_on_the_nth_hit_only() {
            let state = ChaosState::from_plan(
                ChaosPlan::scripted().kill_at_hit(QUEUE_PARK_BEFORE_UPDATE, 2),
            );
            assert!(matches!(
                state.resolve(QUEUE_PARK_BEFORE_UPDATE),
                Resolved::Continue
            ));
            assert!(matches!(
                state.resolve(QUEUE_PARK_BEFORE_UPDATE),
                Resolved::Kill
            ));
            assert!(matches!(
                state.resolve(QUEUE_PARK_BEFORE_UPDATE),
                Resolved::Continue
            ));
            assert_eq!(state.hits_of(QUEUE_PARK_BEFORE_UPDATE), 3);
            assert_eq!(state.actions_fired.load(Ordering::Relaxed), 1);
        }

        #[test]
        fn scripted_error_at_fires_every_hit() {
            let state = ChaosState::from_plan(
                ChaosPlan::scripted()
                    .error_at(POISON_RECLAIM_BEFORE_LOAD, ChaosError::EventIdUnique),
            );
            for _ in 0..3 {
                assert!(matches!(
                    state.resolve(POISON_RECLAIM_BEFORE_LOAD),
                    Resolved::Error(ChaosError::EventIdUnique)
                ));
            }
            assert_eq!(state.actions_fired.load(Ordering::Relaxed), 3);
        }

        #[test]
        fn scripted_drop_notify_and_delay_resolve_expected() {
            let s1 =
                ChaosState::from_plan(ChaosPlan::scripted().drop_notify_at(NOTIFY_TASK_ENQUEUED));
            assert!(matches!(
                s1.resolve(NOTIFY_TASK_ENQUEUED),
                Resolved::DropNotify
            ));

            let s2 =
                ChaosState::from_plan(ChaosPlan::scripted().delay_at(QUEUE_PARK_BEFORE_UPDATE, 7));
            assert!(matches!(
                s2.resolve(QUEUE_PARK_BEFORE_UPDATE),
                Resolved::Delay(7)
            ));
        }

        #[test]
        fn unarmed_point_resolves_continue() {
            let state = ChaosState::from_plan(ChaosPlan::scripted());
            assert!(matches!(
                state.resolve(NOTIFY_TASK_ENQUEUED),
                Resolved::Continue
            ));
            assert_eq!(state.actions_fired.load(Ordering::Relaxed), 0);
        }

        // ---- error conversions (AC1(b)) ------------------------------------

        #[test]
        fn chaos_error_maps_into_harvest_and_diesel() {
            let h: crate::error::HarvestError = ChaosError::Generic.into();
            assert!(matches!(h, crate::error::HarvestError::Database(_)));

            let d: diesel::result::Error = ChaosError::EventIdUnique.into();
            assert!(matches!(
                d,
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::UniqueViolation,
                    _
                )
            ));
            let d2: diesel::result::Error = ChaosError::Generic.into();
            assert!(matches!(d2, diesel::result::Error::QueryBuilderError(_)));
        }

        #[test]
        fn describe_embeds_seed_and_fired_count() {
            let state =
                ChaosState::from_plan(ChaosPlan::seeded(99).kill_at(QUEUE_PARK_BEFORE_UPDATE));
            let _ = state.resolve(QUEUE_PARK_BEFORE_UPDATE);
            let d = state.describe();
            assert!(d.contains("seed=Some(99)"), "describe: {d}");
            assert!(d.contains("fired="), "describe: {d}");
        }
    }
}
