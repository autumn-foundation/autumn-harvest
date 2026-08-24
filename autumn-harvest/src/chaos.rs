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
//! is a single `SeqCst` atomic load followed by an early return with no lock
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

    /// Primitive classes a [`ChaosPoint`] supports.
    ///
    /// A *seeded* plan picks an action for a point only from its declared caps.
    /// A *scripted* plan (`kill_at`/`error_at`/`hold_at`/...) is likewise
    /// validated against them at plan-build time: a directive whose action a
    /// point's caps do not allow panics inside the `*_at` builder, right next to
    /// the mistake — never a silent caps/site mismatch that only surfaces far
    /// away at the entry point when the point is first hit.
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

    static STATE: RwLock<Option<Arc<ChaosState>>> = RwLock::new(None);
    static SERIAL: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));
    static HOOK_INSTALLED: Once = Once::new();
    /// Monotonic source of per-arm generation ids. Starts at 1 so `0` is a
    /// never-armed sentinel (see [`ARMED_GEN`]).
    static NEXT_GEN: AtomicU64 = AtomicU64::new(1);
    /// The generation of the currently-armed plan, or `0` when disarmed. This
    /// is the *sole* armed/disarmed signal — see [`entry_snapshot`] for why an
    /// earlier revision's separate `ARMED: AtomicBool` was removed. A resolved
    /// action only fires when the state it was resolved from still carries
    /// this generation — so a fault resolved by a spawned task that outlived
    /// its plan can never leak into a later test (issue #940, Codex review
    /// round 1).
    static ARMED_GEN: AtomicU64 = AtomicU64::new(0);

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

    impl Action {
        /// A short action name for a scripted caps-mismatch assert message.
        const fn kind(&self) -> &'static str {
            match self {
                Self::Kill => "Kill",
                Self::Hold => "Hold",
                Self::Error(_) => "Error",
                Self::DropNotify => "DropNotify",
                Self::Delay(_) => "Delay",
            }
        }
    }

    /// Assert `point`'s declared caps allow a scripted `action`, panicking at
    /// plan-build time otherwise.
    ///
    /// Scripted plans used to ignore caps, so a `kill_at` on a DROP_NOTIFY-only
    /// point was accepted silently and only blew up far away at the entry point
    /// (the caps/site-mismatch `panic!` in [`hit`]/[`should_drop_notify`]). This
    /// surfaces the mistake next to the offending `*_at` call instead.
    fn assert_scripted_cap(point: ChaosPoint, action: &Action) {
        let caps = point.caps();
        let ok = match action {
            Action::Kill => caps & CAP_KILL != 0,
            Action::Error(_) => caps & CAP_ERROR != 0,
            Action::DropNotify => caps & CAP_DROP_NOTIFY != 0,
            Action::Delay(_) => caps & CAP_DELAY != 0,
            // A `Hold` rendezvous parks the point on an `.await`, so it needs a
            // site that can pause: any of KILL/ERROR/DELAY marks an async point.
            // A DROP_NOTIFY-only site is synchronous and cannot host a Hold (its
            // `should_drop_notify` is not `async` — a scripted Hold there would
            // hang the test's `reached().await` forever).
            Action::Hold => caps & (CAP_KILL | CAP_ERROR | CAP_DELAY) != 0,
        };
        assert!(
            ok,
            "chaos: a {} directive is scripted at `{}`, whose declared caps do \
             not allow it — check the point's caps in the catalogue",
            action.kind(),
            point.name(),
        );
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

        /// Whether this plan has a directive armed at `point` (regardless of its
        /// trigger ordinal). Lets a test compute a non-vacuous seed set against
        /// the points its workload actually reaches, rather than hardcoding
        /// magic seeds that could silently go vacuous if the seeded logic or the
        /// catalogue changes.
        #[must_use]
        pub fn activates(&self, point: ChaosPoint) -> bool {
            self.directives.contains_key(point.name())
        }

        /// Whether this plan arms a [`Kill`](Action::Kill) at `point`. Unlike
        /// [`activates`](Self::activates), this ignores convergence-benign
        /// actions (a `Delay`, or a `DropNotify`): the seeded convergence sweep
        /// uses it so its non-vacuity guard demands a *disruptive* fault — a
        /// pre-commit crash that actually strands an orphan — rather than being
        /// satisfiable by a 5&nbsp;ms sleep that leaves the run healthy.
        #[must_use]
        pub fn kills_at(&self, point: ChaosPoint) -> bool {
            self.directives
                .get(point.name())
                .is_some_and(|d| matches!(d.action, Action::Kill))
        }

        /// Fire a [`Kill`](Action::Kill) on the first hit of `point`.
        #[must_use]
        pub fn kill_at(self, point: ChaosPoint) -> Self {
            self.kill_at_hit(point, 1)
        }

        /// Fire a [`Kill`](Action::Kill) on the `n`th hit of `point`.
        ///
        /// # Panics
        ///
        /// Panics if `point`'s declared caps do not include `CAP_KILL`.
        #[must_use]
        pub fn kill_at_hit(mut self, point: ChaosPoint, n: u64) -> Self {
            let action = Action::Kill;
            assert_scripted_cap(point, &action);
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(n),
                    action,
                },
            );
            self
        }

        /// Install a [`Hold`](Action::Hold) rendezvous on the first hit of
        /// `point`. Retrieve the test-side handle from the guard with
        /// [`ChaosGuard::hold`].
        ///
        /// # Panics
        ///
        /// Panics if `point` is a synchronous site (no `CAP_KILL`/`CAP_ERROR`/
        /// `CAP_DELAY` cap) that cannot host an `.await` rendezvous.
        #[must_use]
        pub fn hold_at(mut self, point: ChaosPoint) -> Self {
            assert_scripted_cap(point, &Action::Hold);
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
        ///
        /// # Panics
        ///
        /// Panics if `point`'s declared caps do not include `CAP_ERROR`.
        #[must_use]
        pub fn error_at(mut self, point: ChaosPoint, err: ChaosError) -> Self {
            let action = Action::Error(err);
            assert_scripted_cap(point, &action);
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::Every,
                    action,
                },
            );
            self
        }

        /// Drop the `LISTEN`/`NOTIFY` wake on the first hit of `point`.
        ///
        /// # Panics
        ///
        /// Panics if `point`'s declared caps do not include `CAP_DROP_NOTIFY`.
        #[must_use]
        pub fn drop_notify_at(mut self, point: ChaosPoint) -> Self {
            assert_scripted_cap(point, &Action::DropNotify);
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
        ///
        /// # Panics
        ///
        /// Panics if `point`'s declared caps do not include `CAP_DELAY`.
        #[must_use]
        pub fn delay_at(mut self, point: ChaosPoint, ms: u64) -> Self {
            let action = Action::Delay(ms);
            assert_scripted_cap(point, &action);
            self.directives.insert(
                point.name(),
                Directive {
                    trigger: Trigger::OnHit(1),
                    action,
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
        /// The per-arm generation this state belongs to; compared against
        /// [`ARMED_GEN`] in [`ChaosState::fire_if_current`] to fence a stale
        /// resolved action.
        generation: u64,
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
        fn from_plan(plan: ChaosPlan, generation: u64) -> Self {
            Self {
                seed: plan.seed,
                generation,
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
            // NB: `actions_fired` is NOT bumped here. It is bumped via
            // `fire_if_current` at the entry point (`hit`/`hit_fallible`/
            // `should_drop_notify`) only when that site can actually *honor* the
            // resolved action AND this state's generation is still armed — so a
            // caps/site mismatch (e.g. an `Error` scripted at an infallible
            // `chaos_point!` site) hard-`panic!`s there, and a stale action
            // resolved after the plan was disarmed fires nothing. Neither
            // silently inflates the anti-vacuity counter.
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
        /// (feeds the anti-vacuity `actions_fired` guard). Called only via
        /// [`ChaosState::fire_if_current`], so the counter and the side effect
        /// are gated by the same generation check.
        ///
        /// `Relaxed` is sufficient: the counter is a per-plan monotonic tally
        /// read only after the armed plan is dropped (the `SERIAL` mutex is
        /// released, establishing a happens-before edge). No test observes it
        /// concurrently with a firing to order it against other memory, so no
        /// acquire/release fence is needed.
        fn mark_fired(&self) {
            self.actions_fired.fetch_add(1, Ordering::Relaxed);
        }

        /// Whether a directive resolved from *this* state may still fire, and —
        /// when it may — count it toward the anti-vacuity guard.
        ///
        /// Returns `true` (and bumps [`mark_fired`](Self::mark_fired)) only when
        /// this state's [`generation`](Self::generation) is still the armed one
        /// ([`ARMED_GEN`]). A stale state — its plan disarmed by a guard drop,
        /// possibly with a newer plan armed since — returns `false` and fires
        /// nothing: an action a spawned task resolved while armed can no longer
        /// leak a fault into a later test after its plan is gone. `SeqCst`
        /// pairs with the store in [`arm`]/`Drop` so the fence orders against
        /// the disarm.
        ///
        /// This gate is intentionally *not* one serialized commit with the side
        /// effect that follows it (the panic/sleep/rendezvous/error a caller runs
        /// after a `true`). Two things make a serialized commit both unnecessary
        /// and, as literally suggested, unimplementable:
        ///
        /// - *No stateful bleed.* Which plan a resolved action belongs to is
        ///   settled by the entry snapshot ([`entry_snapshot`]) plus this
        ///   generation fence, both *before* any side effect runs. A side effect
        ///   that executes in the tiny gap after a `true` runs on the *spawned
        ///   task* and touches no global chaos state and no later test's
        ///   workload, so it cannot make a later test non-deterministic — the
        ///   overlap is temporal only.
        /// - *A lock across the fire would deadlock.* Holding a lock from this
        ///   check through the side effect would have to span `Hold`'s
        ///   `gate.rendezvous().await` and `Delay`'s `sleep().await`; the guard's
        ///   `Drop` needs that same lock to disarm and `release_all_holds`, so the
        ///   rendezvous could never complete.
        ///
        /// The practical guarantee against a fault outliving its test is
        /// *await-discipline*: a reproducer drives the faulted work in a
        /// `tokio::spawn`ed task and awaits its `JoinHandle` before dropping the
        /// guard (see `worker::chaos_drive_one_workflow_task`), so no side effect
        /// executes past disarm in correct usage.
        fn fire_if_current(&self) -> bool {
            if self.generation != ARMED_GEN.load(Ordering::SeqCst) {
                return false;
            }
            self.mark_fired();
            true
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

    /// Atomically snapshot the armed state *and* the generation it belongs to.
    ///
    /// The disarmed fast path is a single `SeqCst` load of [`ARMED_GEN`] and
    /// early return — no lock, no clone — so the zero-cost-when-disarmed
    /// property the entry points ([`hit`]/[`hit_fallible`]/[`should_drop_notify`])
    /// document still holds. `ARMED_GEN` is the *sole* armed/disarmed sentinel
    /// (`0` == disarmed, see its own doc comment) as well as the generation to
    /// fence the subsequent `STATE` read against — there is deliberately no
    /// separate flag.
    ///
    /// An earlier revision of this function read a standalone `ARMED: AtomicBool`
    /// (with `Relaxed` ordering) *and* `ARMED_GEN` as two separate atomics. A
    /// task that read `ARMED` while true, was then preempted across a full
    /// guard drop + re-arm to a *different* plan, and resumed reading
    /// `ARMED_GEN`/`STATE` would observe the *incoming* plan's pair — which is
    /// trivially self-consistent (both belong to the same, newer generation) —
    /// and wrongly resolve (firing) a directive belonging to a plan it was
    /// never entered under. The two-flag design also had a false-negative
    /// mirror image: `arm` published `STATE` and `ARMED_GEN` before flipping
    /// `ARMED` true, so a reader observing exactly that window would see a
    /// stale `ARMED == false` and silently skip fault injection under a plan
    /// that was, by every other measure, already live. Deleting the
    /// standalone flag and using `ARMED_GEN` alone eliminates both of *these*
    /// directions by construction: there is no longer a second,
    /// independently-ordered *read* left to go stale relative to it.
    ///
    /// A narrower, structurally different window remains in [`arm`] itself:
    /// `STATE` and `ARMED_GEN` are still two separate atomics, so a
    /// concurrent reader can observe `ARMED_GEN == 0` for an instant after
    /// `STATE` has already been overwritten with the new plan — a real-time
    /// interleaving, not a stale cached read. This is inherent to publishing
    /// two atomics rather than one (closing it fully would need something
    /// like a single `ArcSwap` carrying both the state and its generation in
    /// one atomic swap) and is benign: unlike the deleted `ARMED` bool, it
    /// can only ever produce a transient, self-detecting *false negative* — a
    /// request momentarily sees "disarmed" and skips injection, which the
    /// reproducer's own `actions_fired() >= 1` anti-vacuity assert would
    /// catch — never the false-positive misfire the two-flag design risked.
    /// It is also no wider than the window that already existed pre-fix.
    ///
    /// The state read that remains — `ARMED_GEN` then `STATE` — is fenced by
    /// the generation-mismatch check exactly as before: [`arm`] publishes
    /// `STATE` before `ARMED_GEN`, so observing a generation implies its state
    /// is already visible; the only rejected pairs are genuinely torn (entry
    /// generation from an outgoing plan, `STATE` already the incoming one's).
    /// The generation is fenced *again* at fire time via
    /// [`ChaosState::fire_if_current`], so a disarm landing after this snapshot
    /// still fires nothing.
    fn entry_snapshot() -> Option<Arc<ChaosState>> {
        let entry_gen = ARMED_GEN.load(Ordering::SeqCst);
        if entry_gen == 0 {
            return None;
        }
        let state = current_state()?;
        if state.generation != entry_gen {
            return None;
        }
        Some(state)
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
        let generation = NEXT_GEN.fetch_add(1, Ordering::SeqCst);
        let state = Arc::new(ChaosState::from_plan(plan, generation));
        *STATE.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&state));
        // Publish STATE, then ARMED_GEN — the sole armed/disarmed signal a
        // reader checks (see `entry_snapshot`).
        ARMED_GEN.store(generation, Ordering::SeqCst);
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

        /// A compact description of the plan and what has fired — embed it in a
        /// reproducer's assert message so a failure prints the seed for
        /// one-command local replay (issue #940 AC3).
        ///
        /// The *plan* (seed → directives) is fully deterministic and is what
        /// makes a failure reproducible. The observed `hits`/`trace` reflect the
        /// actual runtime hit *order*, which under concurrent workers can vary
        /// run-to-run; they are a diagnostic of what happened this run, not a
        /// replay key. Replay off the seed, not the trace.
        #[must_use]
        pub fn diagnostics(&self) -> String {
            self.state.describe()
        }
    }

    impl Drop for ChaosGuard {
        fn drop(&mut self) {
            // Clear armed state first, *then* let `_serial` drop (after this
            // body) so another `arm()` can never observe a half-torn-down
            // state. Disarm the generation (the sole armed/disarmed signal —
            // see `entry_snapshot`) so any resolved action still in flight
            // against this state is fenced by `fire_if_current`.
            ARMED_GEN.store(0, Ordering::SeqCst);
            *STATE.write().unwrap_or_else(PoisonError::into_inner) = None;
            self.state.release_all_holds();
        }
    }

    /// A named injection point in the production code path.
    ///
    /// The disarmed fast path is a single `SeqCst` atomic load and early
    /// return — no lock, no `.await` yield. Invoked via the `chaos_point!`
    /// macro; call sites never reference this directly.
    ///
    /// # Panics
    ///
    /// Panics with `harvest-chaos-kill: {point}` when the armed plan resolves a
    /// [`Kill`](Action::Kill) at `point`. That is the intended fault: it
    /// simulates a task crash and must be triggered inside a spawned task.
    pub async fn hit(point: ChaosPoint) {
        // One consistent (state, generation) snapshot — see `entry_snapshot`:
        // rejects a task that straddled a disarm+rearm so it can't fire a
        // newer plan's directive.
        let Some(state) = entry_snapshot() else {
            return;
        };
        match state.resolve(point) {
            Resolved::Continue => {}
            Resolved::Kill => {
                // A simulated task crash: panic with the chaos-kill marker the
                // quiet panic hook recognizes — but only while this state's
                // generation is still armed (`fire_if_current`), so a stale
                // resolved Kill can never crash a later test.
                assert!(
                    !state.fire_if_current(),
                    "harvest-chaos-kill: {}",
                    point.name(),
                );
            }
            Resolved::Hold(gate) => {
                if state.fire_if_current() {
                    gate.rendezvous().await;
                }
            }
            Resolved::Delay(ms) => {
                if state.fire_if_current() {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
            }
            // An infallible `chaos_point!` site cannot honor Error (needs a
            // `chaos_fallible!` site) or DropNotify (needs `chaos_drop_notify!`).
            // Panic loudly (in debug AND release test builds) instead of
            // silently swallowing it and inflating the anti-vacuity counter —
            // a mis-scripted directive is an author bug, not a fault to hide.
            other @ (Resolved::Error(_) | Resolved::DropNotify) => {
                panic!(
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
        // One consistent (state, generation) snapshot — see `entry_snapshot`.
        let Some(state) = entry_snapshot() else {
            return Ok(());
        };
        match state.resolve(point) {
            Resolved::Continue => Ok(()),
            Resolved::Kill => {
                // A simulated task crash (see `hit`): fire only while armed.
                assert!(
                    !state.fire_if_current(),
                    "harvest-chaos-kill: {}",
                    point.name(),
                );
                Ok(())
            }
            Resolved::Hold(gate) => {
                if state.fire_if_current() {
                    gate.rendezvous().await;
                }
                Ok(())
            }
            Resolved::Error(e) => {
                if state.fire_if_current() {
                    Err(e)
                } else {
                    Ok(())
                }
            }
            Resolved::Delay(ms) => {
                if state.fire_if_current() {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                Ok(())
            }
            // A `chaos_fallible!` site cannot honor DropNotify (needs a
            // `chaos_drop_notify!` site) — panic loudly on the caps/site
            // mismatch rather than silently swallowing a mis-scripted directive.
            other @ Resolved::DropNotify => {
                panic!(
                    "chaos: a {} directive is armed at `{}`, a fallible chaos_fallible!() site \
                     that cannot honor it — check the point's declared caps",
                    other.kind(),
                    point.name()
                );
            }
        }
    }

    /// Whether the `LISTEN`/`NOTIFY` wake at `point` should be dropped.
    ///
    /// Consumes a hit and returns `true` only when the armed plan resolves a
    /// [`DropNotify`](Action::DropNotify) at `point`. The disarmed fast path is
    /// a single `SeqCst` atomic load.
    #[must_use]
    pub fn should_drop_notify(point: ChaosPoint) -> bool {
        // One consistent (state, generation) snapshot — see `entry_snapshot`.
        let Some(state) = entry_snapshot() else {
            return false;
        };
        match state.resolve(point) {
            // `fire_if_current` returns `true` (drop the wake, counted) only
            // while this state's generation is still armed; a stale state
            // returns `false`, so no wake is dropped after disarm.
            Resolved::DropNotify => state.fire_if_current(),
            Resolved::Continue => false,
            // A synchronous drop-notify site can only honor DropNotify; it cannot
            // await a Hold rendezvous, panic a Kill, sleep a Delay, or return an
            // Error. Panic loudly on the caps/site mismatch (a scripted Hold here
            // would otherwise hang the test's `reached().await` forever), in
            // release test builds too — not just under `debug_assertions`.
            other => {
                panic!(
                    "chaos: a {} directive is armed at `{}`, a synchronous chaos_drop_notify!() \
                     site that can only honor DropNotify — check the point's declared caps",
                    other.kind(),
                    point.name()
                );
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
            WORKER_PERSIST_BEFORE_COMMIT,
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
                1,
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
            // `resolve` is side-effect-free w.r.t. the anti-vacuity counter: only
            // an entry point (`hit`/`hit_fallible`/`should_drop_notify`) that can
            // actually *honor* the resolved action bumps `actions_fired`. Resolving
            // to `Kill` here (never reaching a spawned `hit` site) fires nothing.
            // Honored-arm accounting is exercised in
            // `honored_entry_points_bump_actions_fired_but_resolve_alone_does_not`.
            assert_eq!(state.actions_fired.load(Ordering::Relaxed), 0);
        }

        #[test]
        fn scripted_error_at_fires_every_hit() {
            let state = ChaosState::from_plan(
                ChaosPlan::scripted()
                    .error_at(POISON_RECLAIM_BEFORE_LOAD, ChaosError::EventIdUnique),
                1,
            );
            for _ in 0..3 {
                assert!(matches!(
                    state.resolve(POISON_RECLAIM_BEFORE_LOAD),
                    Resolved::Error(ChaosError::EventIdUnique)
                ));
            }
            // See `scripted_kill_at_fires_on_the_nth_hit_only`: `resolve` alone
            // never bumps the counter; the honoring `hit_fallible` site does.
            assert_eq!(state.actions_fired.load(Ordering::Relaxed), 0);
        }

        /// The P2 fix (`mark_fired` is bumped by the entry point only when it can
        /// honor the resolved action) locked in at the real entry points: a
        /// honored directive bumps the anti-vacuity counter, a resolve that does
        /// not fire (wrong ordinal) does not, and each honoring site increments
        /// exactly once per honored hit.
        #[tokio::test]
        async fn honored_entry_points_bump_actions_fired_but_resolve_alone_does_not() {
            // DropNotify at a synchronous drop-notify site: `OnHit(1)`, so the
            // first call is honored (returns true, bumps to 1) and the second
            // resolves `Continue` (returns false, no bump).
            {
                let guard = arm(ChaosPlan::scripted().drop_notify_at(NOTIFY_TASK_ENQUEUED)).await;
                assert!(should_drop_notify(NOTIFY_TASK_ENQUEUED));
                assert_eq!(guard.actions_fired(), 1);
                assert!(!should_drop_notify(NOTIFY_TASK_ENQUEUED));
                assert_eq!(
                    guard.actions_fired(),
                    1,
                    "a non-firing hit must not inflate the anti-vacuity counter"
                );
                assert_eq!(guard.hits(NOTIFY_TASK_ENQUEUED), 2);
            }

            // Error at a `?`-returning site: `Every`, so each hit is honored.
            {
                let guard = arm(ChaosPlan::scripted()
                    .error_at(POISON_RECLAIM_BEFORE_LOAD, ChaosError::EventIdUnique))
                .await;
                assert!(matches!(
                    hit_fallible(POISON_RECLAIM_BEFORE_LOAD).await,
                    Err(ChaosError::EventIdUnique)
                ));
                assert!(matches!(
                    hit_fallible(POISON_RECLAIM_BEFORE_LOAD).await,
                    Err(ChaosError::EventIdUnique)
                ));
                assert_eq!(guard.actions_fired(), 2);
            }
        }

        // ---- Fix 2 (Codex review round 1): scripted directives are validated
        //      against the point's declared caps at plan-build time -----------

        #[test]
        #[should_panic(expected = "declared caps do not allow it")]
        fn scripting_a_kill_at_a_drop_notify_only_point_panics() {
            // NOTIFY_TASK_ENQUEUED declares only CAP_DROP_NOTIFY.
            let _ = ChaosPlan::scripted().kill_at(NOTIFY_TASK_ENQUEUED);
        }

        #[test]
        #[should_panic(expected = "declared caps do not allow it")]
        fn scripting_a_drop_notify_at_a_kill_only_point_panics() {
            // WORKER_PERSIST_BEFORE_COMMIT declares CAP_KILL | CAP_DELAY, no DROP_NOTIFY.
            let _ = ChaosPlan::scripted().drop_notify_at(WORKER_PERSIST_BEFORE_COMMIT);
        }

        #[test]
        #[should_panic(expected = "declared caps do not allow it")]
        fn scripting_an_error_at_a_non_fallible_point_panics() {
            // QUEUE_PARK_BEFORE_UPDATE declares no CAP_ERROR.
            let _ = ChaosPlan::scripted().error_at(QUEUE_PARK_BEFORE_UPDATE, ChaosError::Generic);
        }

        #[test]
        #[should_panic(expected = "declared caps do not allow it")]
        fn scripting_a_hold_at_a_synchronous_drop_notify_point_panics() {
            // A DROP_NOTIFY-only site is synchronous and cannot host a Hold await.
            let _ = ChaosPlan::scripted().hold_at(NOTIFY_TASK_ENQUEUED);
        }

        #[test]
        fn scripting_a_cap_compatible_directive_is_accepted() {
            // Each directive matches its point's declared caps — none panics.
            let _ = ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT);
            let _ = ChaosPlan::scripted().delay_at(QUEUE_PARK_BEFORE_UPDATE, 3);
            let _ = ChaosPlan::scripted().error_at(POISON_RECLAIM_BEFORE_LOAD, ChaosError::Generic);
            let _ = ChaosPlan::scripted().drop_notify_at(NOTIFY_TASK_ENQUEUED);
            // Hold needs an async-capable site (KILL/ERROR/DELAY); this point has KILL+DELAY.
            let _ = ChaosPlan::scripted().hold_at(QUEUE_PARK_BEFORE_UPDATE);
        }

        // ---- Fix 1 (Codex review round 1): a resolved action is fenced by the
        //      armed generation, so a stale state cannot fire into a later plan -

        #[tokio::test]
        async fn a_stale_state_is_fenced_after_its_plan_is_disarmed() {
            // Capture a live state's Arc and resolve a directive while armed (as
            // a slow spawned task would), then drop the guard and arm a *newer*
            // plan. The stale state must refuse to fire and must not bump its
            // anti-vacuity counter.
            let stale: Arc<ChaosState> = {
                let _guard = arm(ChaosPlan::scripted().drop_notify_at(NOTIFY_TASK_ENQUEUED)).await;
                let s = current_state().expect("armed state present");
                assert!(matches!(
                    s.resolve(NOTIFY_TASK_ENQUEUED),
                    Resolved::DropNotify
                ));
                s
            }; // guard dropped -> this generation disarmed (ARMED_GEN -> 0)

            // A newer plan is armed; the stale state's generation no longer
            // matches ARMED_GEN, so its resolved action is fenced.
            let newer = arm(ChaosPlan::scripted()).await;
            assert!(
                !stale.fire_if_current(),
                "a stale state must not fire after its generation is disarmed",
            );
            assert_eq!(
                stale.actions_fired.load(Ordering::Relaxed),
                0,
                "a fenced action must not inflate the anti-vacuity counter",
            );
            // The newer state, in contrast, is current and fires.
            let current = current_state().expect("newer state present");
            assert!(current.fire_if_current());
            assert_eq!(current.actions_fired.load(Ordering::Relaxed), 1);
            drop(newer);
        }

        // ---- Fix 3 (Codex review round 2): the entry snapshot rejects a state
        //      whose generation != the armed generation, so a task that
        //      straddled a disarm+rearm between the `ARMED_GEN` read and the
        //      `STATE` read cannot adopt a *newer* plan's directive. Unlike
        //      Fix 1 (a *captured* Arc fenced at fire time), this rejects the
        //      torn read at the entry point, before `resolve` even bumps the
        //      hit counter. (Fix 4, below, later removed a separate `ARMED`
        //      bool this window used to sit behind; this generation-mismatch
        //      fence is unchanged and still the mechanism this test exercises.) --

        #[tokio::test]
        async fn entry_snapshot_rejects_a_torn_state_from_a_different_generation() {
            // Holding the guard keeps `SERIAL` for the whole test, so no other
            // arming test can observe the STATE we poke below.
            let _guard = arm(ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT)).await;
            let armed_gen = ARMED_GEN.load(Ordering::SeqCst);

            // The torn read a straddling task would observe: STATE holds a state
            // whose generation is NOT the currently-armed ARMED_GEN, as if a
            // disarm+rearm slipped between the entry generation read and the
            // STATE read. `entry_snapshot` must reject it (return `None`) so the
            // task resolves nothing against the mismatched plan.
            let mismatched = Arc::new(ChaosState::from_plan(
                ChaosPlan::scripted(),
                armed_gen.wrapping_add(1),
            ));
            *STATE.write().unwrap_or_else(PoisonError::into_inner) = Some(mismatched);
            assert!(
                entry_snapshot().is_none(),
                "a state whose generation != ARMED_GEN is a torn straddle and must be rejected",
            );

            // A state whose generation matches the armed generation is accepted.
            let consistent = Arc::new(ChaosState::from_plan(ChaosPlan::scripted(), armed_gen));
            *STATE.write().unwrap_or_else(PoisonError::into_inner) = Some(consistent);
            assert!(
                entry_snapshot().is_some(),
                "a state whose generation == ARMED_GEN is consistent and accepted",
            );
            // `_guard` drop restores STATE=None / disarmed for the next test.
        }

        // ---- Fix 4 (Codex review round 4, issue #1202 / #940 follow-up): the
        //      entry point used to read `ARMED` (a bare bool) and `ARMED_GEN`
        //      (the generation) as two SEPARATE atomics. A task preempted
        //      between those two reads could observe `ARMED == true` under an
        //      outgoing plan, resume after a *full* disarm + re-arm to a
        //      different plan, and then read the *incoming* plan's
        //      `ARMED_GEN`/`STATE` pair -- which is trivially self-consistent
        //      (both belong to the same, newer generation) -- so the mismatch
        //      fence (Fix 3, above) never fires and the straggling task
        //      wrongly adopts the newer plan's directive.
        //
        //      `vulnerable_two_read_shape` below is a byte-for-byte copy of
        //      the *removed* algorithm (what `entry_snapshot` used to be),
        //      resurrected here only so this test can prove the straddle was
        //      real: it takes a caller-supplied `observed_armed` bool --
        //      standing in for the stale `ARMED` read a preempted task would
        //      have captured before the transition -- then reads whatever
        //      `ARMED_GEN`/`STATE` are CURRENT. Driving it through a real
        //      arm -> disarm -> re-arm sequence, with the captured boolean
        //      held stale across that transition, shows it wrongly returns
        //      `Some` for the newer generation.
        //
        //      The fix deletes the standalone boolean entirely: `ARMED_GEN`
        //      alone is now both the armed/disarmed sentinel (`0` = disarmed)
        //      and the generation to fence against (see `entry_snapshot`), so
        //      there is no separate first read left for a task to straddle --
        //      the vulnerable shape can no longer be constructed from
        //      `entry_snapshot`'s actual (single-read) sequence. A task can
        //      still be preempted *between* the `ARMED_GEN` read and the
        //      `STATE` read, but that is exactly the pre-existing Fix-3 window
        //      (a mismatched generation), already covered above.
        //
        //      The straddle also has a *false-negative* mirror image, verified
        //      directly against the real (pre-fix) `entry_snapshot` during
        //      development of this fix: `arm` publishes `STATE` and
        //      `ARMED_GEN` *before* flipping `ARMED` true, so a reader
        //      observing exactly that window -- `ARMED_GEN`/`STATE` already a
        //      valid, self-consistent pair, `ARMED` still stale-`false` --
        //      would incorrectly see the harness as disarmed and silently
        //      skip fault injection the plan believes is live. Poking
        //      `ARMED.store(false, ..)` on a freshly-armed guard and asserting
        //      `entry_snapshot().is_some()` failed against the pre-fix code
        //      and passes once `ARMED` no longer exists to read; that
        //      transient check is not kept here (it referenced a static this
        //      fix deletes) but is recorded in this comment for anyone
        //      re-deriving the fix's justification.

        /// The exact two-read shape `entry_snapshot` used before this fix,
        /// resurrected here only to prove the straddle it permitted. Not
        /// reachable from production code; deleting the real `ARMED` static
        /// (this fix) makes this shape unconstructable there.
        fn vulnerable_two_read_shape(observed_armed: bool) -> Option<Arc<ChaosState>> {
            if !observed_armed {
                return None;
            }
            let entry_gen = ARMED_GEN.load(Ordering::SeqCst);
            let state = current_state()?;
            if state.generation != entry_gen {
                return None;
            }
            Some(state)
        }

        #[tokio::test]
        async fn entry_snapshot_transitions_cleanly_where_the_removed_two_read_shape_straddled() {
            let guard1 = arm(ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT)).await;
            let gen1 = ARMED_GEN.load(Ordering::SeqCst);
            assert_ne!(gen1, 0, "gen1 must be armed here");
            assert!(
                entry_snapshot().is_some_and(|s| s.generation == gen1),
                "a fresh call while gen1 is armed observes gen1",
            );

            // "Step 1" of the removed shape: a task observes the harness armed
            // under gen1 and captures that bare boolean, intending to read the
            // generation and state next.
            let observed_armed_under_gen1 = true;

            // The task is preempted here: a *full* disarm (guard1 drop)
            // happens before it resumes. The real entry point, called fresh
            // in this fully-disarmed window, correctly observes nothing armed.
            drop(guard1);
            assert!(
                entry_snapshot().is_none(),
                "a fresh call in the disarmed window observes nothing armed",
            );

            // ...then a re-arm to a *different* plan completes, still before
            // the preempted task resumes.
            let guard2 = arm(ChaosPlan::scripted()).await;
            let gen2 = ARMED_GEN.load(Ordering::SeqCst);
            assert_ne!(gen2, gen1, "gen2 must be a genuinely different generation");
            assert!(
                entry_snapshot().is_some_and(|s| s.generation == gen2),
                "a fresh call while gen2 is armed observes gen2, not gen1",
            );

            // Resumed: "step 2" of the removed shape reads whatever is CURRENT
            // now (gen2's), using the stale boolean captured under gen1. This
            // is the bug: the removed shape treats a stale "yes, something was
            // armed" as license to adopt whatever is armed NOW, regardless of
            // which plan that stale observation actually belonged to.
            let vulnerable_result = vulnerable_two_read_shape(observed_armed_under_gen1);
            assert!(
                vulnerable_result.is_some_and(|s| s.generation == gen2),
                "documents the bug: the removed two-read shape wrongly adopts \
                 gen2's state for a task whose entry belonged to gen1",
            );

            drop(guard2);
        }

        #[test]
        fn scripted_drop_notify_and_delay_resolve_expected() {
            let s1 = ChaosState::from_plan(
                ChaosPlan::scripted().drop_notify_at(NOTIFY_TASK_ENQUEUED),
                1,
            );
            assert!(matches!(
                s1.resolve(NOTIFY_TASK_ENQUEUED),
                Resolved::DropNotify
            ));

            let s2 = ChaosState::from_plan(
                ChaosPlan::scripted().delay_at(QUEUE_PARK_BEFORE_UPDATE, 7),
                1,
            );
            assert!(matches!(
                s2.resolve(QUEUE_PARK_BEFORE_UPDATE),
                Resolved::Delay(7)
            ));
        }

        #[test]
        fn unarmed_point_resolves_continue() {
            let state = ChaosState::from_plan(ChaosPlan::scripted(), 1);
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
                ChaosState::from_plan(ChaosPlan::seeded(99).kill_at(QUEUE_PARK_BEFORE_UPDATE), 1);
            let _ = state.resolve(QUEUE_PARK_BEFORE_UPDATE);
            let d = state.describe();
            assert!(d.contains("seed=Some(99)"), "describe: {d}");
            assert!(d.contains("fired="), "describe: {d}");
        }
    }
}
