//! Per-activity circuit breaker that stops dispatch during downstream
//! outages (issue #369).
//!
//! When a downstream service an activity depends on goes hard-down, harvest's
//! default behaviour is to retry every failing attempt across its full
//! [`RetryPolicy`](crate::policy::RetryPolicy) curve. Across thousands of
//! in-flight workflows that all call the same activity, this floods the task
//! queue with retries against a dead target and piles up identical DLQ entries.
//!
//! A [`CircuitBreakerPolicy`](crate::policy::CircuitBreakerPolicy) attached to
//! an activity lets the worker track that activity's recent failures and
//! **trip open** once they cross a threshold within a rolling window. While the
//! breaker is open, new dispatches short-circuit instead of running the doomed
//! work. The policy's `open_mode` decides how (issue #1809). In `Defer` mode,
//! the default, the task goes back to `PENDING` until the next probe. In
//! `FailFast` mode, it fails with a non-retryable `"CircuitOpen"` failure;
//! workflows that handle the failure (Saga compensation, branching) see it
//! within seconds.
//!
//! ## State model
//!
//! ```text
//!            failures >= threshold in window
//!   Closed ─────────────────────────────────► Open
//!     ▲                                          │
//!     │ probe succeeds                           │ cooldown elapsed
//!     │                                          ▼
//!     └──────────────── HalfOpen ◄───────────────┘
//!                          │ probe fails
//!                          └──────────► Open
//! ```
//!
//! ## Scope and durability
//!
//! State is tracked **in-process and per-shard** (`Mutex<HashMap>`). It never
//! touches the workflow event log. A deferral appends no event. A fail-fast
//! short circuit records an ordinary `ActivityFailed` event with a typed
//! `"CircuitOpen"` payload. So the append-only contract and deterministic
//! replay are both unaffected. Each shard / worker process tracks its own
//! breaker. An outage that hits every shard trips each independently,
//! matching the per-shard ACID model.

// Each public method intentionally holds the state lock for its whole body: it
// reads and mutates the same `BreakerState` and returns a value derived from
// it, so there is no meaningful window in which the guard could be released
// earlier. The drop-tightening lint is a false positive here.
#![allow(clippy::significant_drop_tightening)]

use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

// `Mutex`/`MutexGuard` resolve to `std::sync` under a normal build and to
// `loom::sync` under `RUSTFLAGS="--cfg loom"` so `tests/loom_models.rs` can
// model-check the breaker's concurrent state machine. See `crate::loom_sync`.
use crate::loom_sync::{Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::policy::CircuitBreakerPolicy;

/// The three observable phases of a breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitPhase {
    /// Normal operation: dispatches proceed unchanged.
    Closed,
    /// Tripped: dispatches short-circuit until the cooldown elapses.
    Open,
    /// Cooldown elapsed: a single probe dispatch is admitted.
    HalfOpen,
}

impl CircuitPhase {
    /// Stable, low-cardinality string used in API responses and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

/// Opaque token returned by [`CircuitBreakerRegistry::on_dispatch`] and handed
/// back to [`CircuitBreakerRegistry::on_result`].
///
/// It carries the breaker's *generation* at dispatch time. Every state-resetting
/// transition (trip, recover-to-closed, operator force-open, operator
/// force-close) bumps the generation, so a result whose token predates the
/// current generation is a **stale straggler** — an attempt that started before
/// a trip or before a manual reset — and is fenced out: it can neither resolve
/// the half-open probe early nor immediately re-trip a just-closed breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchToken {
    generation: u64,
    is_probe: bool,
}

impl DispatchToken {
    /// Whether this dispatch was admitted as the single half-open probe.
    #[must_use]
    pub const fn is_probe(self) -> bool {
        self.is_probe
    }
}

/// One claim of a task: the queue row and the attempt that the claim wrote.
///
/// A worker registers each claim it dispatches with
/// [`CircuitBreakerRegistry::begin_claim`]. The timeout enforcer marks a
/// claim when it times out (issue #1809). The result of a marked claim does
/// not move the breaker, because the enforcer already counted that attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClaimKey {
    /// The task queue row.
    pub task_id: Uuid,
    /// The row's `attempt` value that the claim wrote.
    pub attempt: i32,
    /// The claim's `started_at` epoch. A deferral lowers `attempt`, so the
    /// epoch keeps two claims of one row apart.
    pub started_at: Option<DateTime<Utc>>,
}

/// A claim that this process dispatched and that has not reported yet, or
/// a claim that an enforcer here marked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InFlightClaim {
    /// The token of its dispatch. A confirmed timeout releases a probe slot
    /// with it, before the handler returns.
    ///
    /// `None` until this process registers the claim. An enforcer can mark a
    /// claim before its worker registers it, or mark a claim of another
    /// process. The entry then holds the mark, so a later registration
    /// still sees it.
    token: Option<DispatchToken>,
    /// The breaker generation when the entry was made. A confirm with no
    /// token is fenced by it, so a reset after the mark counts nothing.
    generation: u64,
    state: ClaimState,
}

/// Where a claim of this process stands against the timeout enforcer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimState {
    /// No enforcement touches the claim.
    Running,
    /// Enforcers are deciding, this many of them. Two scanners can race on
    /// one claim. A result that arrives now waits for their decision.
    Provisional(u32),
    /// The result that arrived while enforcers were deciding, and how many
    /// still decide.
    Held(AttemptOutcome, DispatchToken, u32),
    /// The enforcer timed the claim out. A later result does not count.
    TimedOut,
    /// The worker found the claim lost to a timeout while enforcers here were
    /// still deciding, this many of them. A confirm settles it as their
    /// timeout. When the last one rolls back, another process enforced it,
    /// and it counts here then.
    LostPending(u32),
    /// The worker ended the claim with no result while enforcers here were
    /// still deciding, this many of them. The entry keeps the dispatch
    /// token, so a confirm still counts through the generation fence.
    Ended(u32),
}

/// Outcome of consulting the breaker before dispatching an activity attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchDecision {
    /// Proceed with the dispatch. The caller must pass `token` back to
    /// [`CircuitBreakerRegistry::on_result`] so the breaker can fence stale
    /// results by generation.
    Allow {
        /// Generation-stamped token (see [`DispatchToken`]).
        token: DispatchToken,
    },
    /// Short-circuit the attempt: the breaker is open.
    ShortCircuit {
        /// Wall-clock instant at which the breaker last tripped, if known.
        opened_at: Option<DateTime<Utc>>,
        /// How long until a half-open probe will be admitted. `None` when the
        /// breaker is **operator-forced open**: no probe is admitted on any
        /// timer, so there is no meaningful retry-after to advertise — recovery
        /// requires an explicit `force-close`.
        retry_after: Option<Duration>,
    },
}

/// Classification of a completed attempt for breaker accounting.
///
/// Only [`RetryableFailure`](Self::RetryableFailure) — a transient,
/// downstream-style error — contributes to tripping the breaker. A
/// [`NonRetryableFailure`](Self::NonRetryableFailure) is a permanent per-request
/// error (bad input, validation) and proves the downstream is reachable enough
/// to give a definitive answer, so it never trips the breaker (and counts as a
/// healthy outcome for a half-open probe).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The attempt succeeded.
    Success,
    /// The attempt failed with a transient/downstream-style error (retryable).
    RetryableFailure,
    /// The attempt failed with a permanent per-request error (non-retryable).
    NonRetryableFailure,
}

/// A state transition worth reporting to the metrics surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitTransition {
    /// The breaker moved into the open state (tripped or re-tripped).
    Tripped,
    /// The breaker recovered to the closed state.
    Closed,
}

/// Snapshot of a single activity's breaker for the management API.
#[derive(Debug, Clone, Serialize)]
pub struct CircuitSnapshot {
    /// The activity name.
    pub activity_name: String,
    /// Current phase: `"closed"`, `"open"`, or `"half_open"`.
    pub state: &'static str,
    /// Whether an operator has pinned the breaker open via the management API.
    pub forced_open: bool,
    /// Wall-clock timestamp of the most recent trip, if the breaker has tripped.
    pub last_trip: Option<DateTime<Utc>>,
    /// Failures currently counted inside the rolling window (closed-phase signal).
    pub rolling_failure_count: u32,
    /// Seconds until a half-open probe is admitted (only set while open and not
    /// forced).
    pub time_until_probe_secs: Option<f64>,
    /// Configured failure threshold.
    pub failure_threshold: u32,
    /// Configured rolling window, in seconds.
    pub window_secs: f64,
    /// Configured cooldown, in seconds.
    pub cooldown_secs: f64,
    /// What a dispatch does while the breaker is open: `"defer"` or
    /// `"fail_fast"` (issue #1809).
    pub open_mode: crate::policy::CircuitOpenMode,
}

#[derive(Debug)]
struct BreakerState {
    phase: CircuitPhase,
    /// Failure timestamps inside the rolling window (closed-phase counter).
    failures: VecDeque<Instant>,
    /// Monotonic instant of the last trip (drives cooldown math).
    opened_at: Option<Instant>,
    /// Wall-clock instant of the last trip (for the observable snapshot).
    opened_at_wall: Option<DateTime<Utc>>,
    /// `true` while a half-open probe is in flight so concurrent dispatches
    /// short-circuit and only one probe runs.
    probe_in_flight: bool,
    /// Operator pin: while set, dispatches always short-circuit and results are
    /// ignored until an operator force-closes.
    forced_open: bool,
    /// Monotonic generation counter bumped on every state-resetting transition
    /// (trip / close / force-open / force-close). A result whose dispatch token
    /// carries an older generation is a stale straggler and is fenced out.
    generation: u64,
    /// Claims that this process dispatched and that have not reported yet,
    /// each with its timed-out mark (issue #1809). A worker holds at most its
    /// concurrency limit of claims, so the map stays small. Every exit of a
    /// dispatch removes its entry, so no mark can expire while its claim can
    /// still report.
    in_flight_claims: HashMap<ClaimKey, InFlightClaim>,
}

impl Default for BreakerState {
    fn default() -> Self {
        Self {
            phase: CircuitPhase::Closed,
            failures: VecDeque::new(),
            opened_at: None,
            opened_at_wall: None,
            probe_in_flight: false,
            forced_open: false,
            generation: 0,
            in_flight_claims: HashMap::new(),
        }
    }
}

impl BreakerState {
    fn prune(&mut self, now: Instant, window: Duration) {
        while let Some(&front) = self.failures.front() {
            if now.saturating_duration_since(front) > window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }

    /// Bump the generation so any attempt dispatched before this transition is
    /// fenced out of `on_result`.
    const fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    fn trip(&mut self, now: Instant) {
        self.phase = CircuitPhase::Open;
        self.opened_at = Some(now);
        self.opened_at_wall = Some(Utc::now());
        self.failures.clear();
        self.probe_in_flight = false;
        self.bump_generation();
    }

    fn close(&mut self) {
        self.phase = CircuitPhase::Closed;
        self.failures.clear();
        self.opened_at = None;
        self.opened_at_wall = None;
        self.probe_in_flight = false;
        self.bump_generation();
    }
}

/// In-process registry of per-activity circuit breakers.
///
/// Constructed once from the registered activities' policies and shared (behind
/// an `Arc`) between the worker dispatch path and the management API so that
/// both observe the same state. Activities without a declared policy are never
/// tracked: [`on_dispatch`](Self::on_dispatch) returns [`DispatchDecision::Allow`]
/// and [`on_result`](Self::on_result) is a no-op for them.
#[derive(Debug)]
pub struct CircuitBreakerRegistry {
    policies: HashMap<String, CircuitBreakerPolicy>,
    /// Sorted list of every activity name with a declared policy. Precomputed
    /// once so the worker's claim hot path can pass it to `claim_task` without
    /// allocating per poll. These activities have their rate limiting enforced
    /// at *dispatch* (issue #369), so the claim query skips the rate-limit gate
    /// and token debit for them.
    tracked_names: Vec<String>,
    states: Mutex<HashMap<String, BreakerState>>,
}

impl CircuitBreakerRegistry {
    /// Build a registry from `(activity_name, policy)` pairs.
    #[must_use]
    pub fn new(policies: HashMap<String, CircuitBreakerPolicy>) -> Self {
        let mut tracked_names: Vec<String> = policies.keys().cloned().collect();
        tracked_names.sort_unstable();
        Self {
            policies,
            tracked_names,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// An empty registry that tracks nothing (every dispatch is allowed).
    #[must_use]
    pub fn empty() -> Self {
        Self::new(HashMap::new())
    }

    /// `true` if no activity has a circuit-breaker policy declared.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    /// Whether `activity_name` has a declared policy.
    #[must_use]
    pub fn has_policy(&self, activity_name: &str) -> bool {
        self.policies.contains_key(activity_name)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, BreakerState>> {
        // Poisoning only happens if a thread panicked while holding the lock.
        // The state is just counters/timestamps, so recovering the inner value
        // is always safe and far preferable to cascading a panic across the
        // worker hot path.
        self.states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Decide whether to allow a dispatch of `activity_name` at `now`.
    ///
    /// Activities without a policy always return [`DispatchDecision::Allow`].
    /// When the breaker is open and the cooldown has elapsed this call performs
    /// the open → half-open transition and admits the returned dispatch as the
    /// single probe.
    #[must_use]
    pub fn on_dispatch(&self, activity_name: &str, now: Instant) -> DispatchDecision {
        let Some(&policy) = self.policies.get(activity_name) else {
            return DispatchDecision::Allow {
                token: DispatchToken {
                    generation: 0,
                    is_probe: false,
                },
            };
        };
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();

        if st.forced_open {
            // Operator-forced: no probe is admitted on any timer, so advertise
            // no retry-after — recovery requires an explicit force-close.
            return DispatchDecision::ShortCircuit {
                opened_at: st.opened_at_wall,
                retry_after: None,
            };
        }

        let allow = |st: &BreakerState, is_probe: bool| DispatchDecision::Allow {
            token: DispatchToken {
                generation: st.generation,
                is_probe,
            },
        };

        match st.phase {
            CircuitPhase::Closed => allow(st, false),
            CircuitPhase::Open => {
                let opened = st.opened_at.unwrap_or(now);
                let elapsed = now.saturating_duration_since(opened);
                if elapsed >= policy.cooldown {
                    // Cooldown elapsed: admit exactly one probe.
                    st.phase = CircuitPhase::HalfOpen;
                    st.probe_in_flight = true;
                    allow(st, true)
                } else {
                    DispatchDecision::ShortCircuit {
                        opened_at: st.opened_at_wall,
                        retry_after: Some(policy.cooldown.saturating_sub(elapsed)),
                    }
                }
            }
            CircuitPhase::HalfOpen => {
                if st.probe_in_flight {
                    // A probe is already running; no further dispatch is admitted
                    // until it completes and transitions the breaker. Advertise
                    // no retry-after so callers don't busy-loop on a `0s` hint —
                    // the outcome is gated on the probe, not a timer.
                    DispatchDecision::ShortCircuit {
                        opened_at: st.opened_at_wall,
                        retry_after: None,
                    }
                } else {
                    st.probe_in_flight = true;
                    allow(st, true)
                }
            }
        }
    }

    /// Record the outcome of a dispatched attempt.
    ///
    /// `token` must be the [`DispatchToken`] returned by the matching
    /// [`on_dispatch`](Self::on_dispatch) call. Its generation fences stale
    /// stragglers: any result whose token predates the breaker's current
    /// generation — an attempt dispatched before a trip, a recovery, or an
    /// operator force-open/force-close — is ignored, so it can neither resolve
    /// the half-open probe early nor immediately re-trip a just-reset breaker.
    /// The probe transition is additionally gated on `token.is_probe`.
    ///
    /// Only [`AttemptOutcome::RetryableFailure`] contributes to tripping the
    /// breaker. A [`AttemptOutcome::NonRetryableFailure`] is a permanent
    /// per-request error (bad input) that proves the downstream is reachable,
    /// so it never trips the breaker and counts as a healthy probe result.
    ///
    /// Returns `Some(transition)` when the breaker changed open/closed state so
    /// the caller can emit the corresponding metric. Activities without a
    /// policy are ignored and always return `None`.
    pub fn on_result(
        &self,
        activity_name: &str,
        outcome: AttemptOutcome,
        token: DispatchToken,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        apply_result(st, policy, outcome, token, now)
    }

    /// Record the outcome of a dispatched attempt that holds `claim`.
    ///
    /// This is [`on_result`](Self::on_result) with one more fence. When the
    /// timeout enforcer already timed out `claim`, the result does not move
    /// the breaker (issue #1809). The enforcer counted that attempt, and a late
    /// success must not clear the failure window. A timed-out probe releases
    /// its slot as [`on_cancelled`](Self::on_cancelled) does. The check and the
    /// update run under one lock, so they cannot race the enforcer. The claim
    /// leaves the in-flight set.
    pub fn on_claim_result(
        &self,
        activity_name: &str,
        outcome: AttemptOutcome,
        token: DispatchToken,
        claim: ClaimKey,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        match st.in_flight_claims.get(&claim).map(|entry| entry.state) {
            Some(ClaimState::Provisional(deciding)) => {
                if let Some(entry) = st.in_flight_claims.get_mut(&claim) {
                    entry.state = ClaimState::Held(outcome, token, deciding);
                }
                None
            }
            Some(ClaimState::TimedOut) => {
                st.in_flight_claims.remove(&claim);
                apply_cancelled(st, token, now);
                None
            }
            _ => {
                st.in_flight_claims.remove(&claim);
                apply_result(st, policy, outcome, token, now)
            }
        }
    }

    /// Record that `claim` wrote no outcome, because another path settled the
    /// attempt first (issue #1809). The result does not count.
    ///
    /// - A loss that was not a timeout (a cancellation, an operator action,
    ///   an orphan reclaim) only releases the claim's probe slot.
    /// - A timeout that an enforcer in this process confirmed is settled by
    ///   that enforcer. This only releases the probe slot. While an enforcer
    ///   here is still deciding, the loss waits for its decision.
    /// - A timeout that no enforcer here marked was enforced by another
    ///   process. Breaker state is per process, so it counts here as a
    ///   failure. A failed probe re-opens the breaker. A closed breaker adds
    ///   the failure to its window.
    ///
    /// Returns [`CircuitTransition::Tripped`] when a counted timeout opens the
    /// breaker.
    pub fn on_claim_lost(
        &self,
        activity_name: &str,
        token: DispatchToken,
        claim: ClaimKey,
        lost_to_timeout: bool,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        let state = st.in_flight_claims.get(&claim).map(|entry| entry.state);
        if lost_to_timeout
            && let Some(ClaimState::Provisional(deciding) | ClaimState::Held(_, _, deciding)) =
                state
        {
            // An enforcer here is still deciding. Keep the loss until it
            // confirms the timeout, or the last one rolls back.
            if let Some(entry) = st.in_flight_claims.get_mut(&claim) {
                entry.state = ClaimState::LostPending(deciding);
            }
            return None;
        }
        if let Some(ClaimState::Provisional(deciding) | ClaimState::Held(_, _, deciding)) = state {
            // Not a timeout as far as this worker knows, but an enforcer
            // here is still deciding. Its verdict wins, so the entry keeps
            // the token for a fenced count.
            if let Some(entry) = st.in_flight_claims.get_mut(&claim) {
                entry.state = ClaimState::Ended(deciding);
            }
            apply_cancelled(st, token, now);
            return None;
        }
        st.in_flight_claims.remove(&claim);
        if !lost_to_timeout || state == Some(ClaimState::TimedOut) {
            apply_cancelled(st, token, now);
            return None;
        }
        count_remote_timeout(st, policy, token, now)
    }

    /// Register `claim` as dispatched by this process (issue #1809).
    ///
    /// Call [`end_claim`](Self::end_claim) or a claim-aware report on every
    /// exit of the dispatch.
    ///
    /// An enforcer may have marked the claim already. The claim then keeps
    /// that mark, so its loss or result waits for the enforcer. Otherwise
    /// the owner could count the timeout, and the enforcer count it again.
    pub fn begin_claim(&self, activity_name: &str, claim: ClaimKey, token: DispatchToken) {
        if !self.policies.contains_key(activity_name) {
            return;
        }
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.in_flight_claims
            .entry(claim)
            .and_modify(|entry| entry.token = Some(token))
            .or_insert(InFlightClaim {
                token: Some(token),
                generation: token.generation,
                state: ClaimState::Running,
            });
    }

    /// Remove `claim` from the in-flight set without a result (issue #1809).
    /// A claim that already reported is not there, so this is then a no-op.
    /// A held result stays, because the enforcer still settles it.
    ///
    /// While an enforcer here still decides, the entry stays as
    /// [`ClaimState::Ended`]. Its token then still fences a confirm by
    /// generation. The last rollback removes it.
    pub fn end_claim(&self, activity_name: &str, claim: ClaimKey) {
        if !self.policies.contains_key(activity_name) {
            return;
        }
        let mut states = self.lock();
        let Some(st) = states.get_mut(activity_name) else {
            return;
        };
        match st.in_flight_claims.get(&claim).map(|entry| entry.state) {
            Some(ClaimState::Held(..) | ClaimState::LostPending(_) | ClaimState::Ended(_))
            | None => {}
            Some(ClaimState::Provisional(deciding)) => {
                if let Some(entry) = st.in_flight_claims.get_mut(&claim) {
                    entry.state = ClaimState::Ended(deciding);
                }
            }
            Some(ClaimState::Running | ClaimState::TimedOut) => {
                st.in_flight_claims.remove(&claim);
            }
        }
    }

    /// Mark `claim` provisionally, before the enforcer decides on it (issue
    /// #1809).
    ///
    /// A result that arrives now is held. Each enforcer that marks then calls
    /// [`confirm_claim_timed_out`](Self::confirm_claim_timed_out) or
    /// [`unmark_claim_timed_out`](Self::unmark_claim_timed_out) once. One
    /// confirm wins over any number of rollbacks, in any order.
    ///
    /// A claim that this process has not registered gets an entry with no
    /// token. Its worker may register it before the enforcer decides, and
    /// then keeps the mark. The confirm or the last rollback removes an entry
    /// that no worker registered.
    pub fn mark_claim_timed_out(&self, activity_name: &str, claim: ClaimKey) {
        if !self.policies.contains_key(activity_name) {
            return;
        }
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        let generation = st.generation;
        let entry = st.in_flight_claims.entry(claim).or_insert(InFlightClaim {
            token: None,
            generation,
            state: ClaimState::Running,
        });
        entry.state = match entry.state {
            ClaimState::Running => ClaimState::Provisional(1),
            ClaimState::Provisional(deciding) => {
                ClaimState::Provisional(deciding.saturating_add(1))
            }
            ClaimState::Held(outcome, token, deciding) => {
                ClaimState::Held(outcome, token, deciding.saturating_add(1))
            }
            ClaimState::TimedOut => ClaimState::TimedOut,
            ClaimState::LostPending(deciding) => {
                ClaimState::LostPending(deciding.saturating_add(1))
            }
            ClaimState::Ended(deciding) => ClaimState::Ended(deciding.saturating_add(1)),
        };
    }

    /// The enforcer timed `claim` out (issue #1809). The claim's probe slot,
    /// if it holds one, is released now, so a stuck handler cannot keep the
    /// breaker half-open. A held result is dropped. A later result does not
    /// count. A timed-out probe is a failed probe, so this returns
    /// [`CircuitTransition::Tripped`] when it re-opens the breaker.
    ///
    /// With `count_failure`, the timeout also counts as a failure, under the
    /// same lock. A claim of this process counts through its dispatch token,
    /// so a token from before a trip or a reset counts nothing. A claim that
    /// no worker here registered has no token. It counts as an external
    /// failure, fenced by the generation at its mark.
    ///
    /// A claim whose handler started was registered before the enforcer read
    /// that fact. So an entry with no token here belongs to another process,
    /// and this removes it.
    pub fn confirm_claim_timed_out(
        &self,
        activity_name: &str,
        claim: ClaimKey,
        count_failure: bool,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        // Every enforcer marks before it confirms, and every path keeps a
        // marked entry until its enforcers decide. So a missing entry has
        // no generation to fence by, and counts nothing.
        let entry = st.in_flight_claims.get(&claim).copied()?;
        match (entry.state, entry.token) {
            (ClaimState::TimedOut, _) => return None,
            (ClaimState::Running | ClaimState::Provisional(_), Some(_)) => {
                if let Some(entry) = st.in_flight_claims.get_mut(&claim) {
                    entry.state = ClaimState::TimedOut;
                }
            }
            _ => {
                st.in_flight_claims.remove(&claim);
            }
        }
        if entry
            .token
            .is_some_and(|token| apply_cancelled(st, token, now))
        {
            return Some(CircuitTransition::Tripped);
        }
        let generation = entry
            .token
            .map_or(entry.generation, |token| token.generation);
        if !count_failure || generation != st.generation {
            return None;
        }
        apply_external_failure(st, policy, now)
    }

    /// The enforcer did not time `claim` out after all (issue #1809). A held
    /// result now counts as usual. Returns its transition, if any.
    pub fn unmark_claim_timed_out(
        &self,
        activity_name: &str,
        claim: ClaimKey,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.get_mut(activity_name)?;
        let entry = st.in_flight_claims.get_mut(&claim)?;
        match entry.state {
            ClaimState::Held(outcome, token, deciding) if deciding > 1 => {
                entry.state = ClaimState::Held(outcome, token, deciding - 1);
                None
            }
            ClaimState::Held(outcome, token, _) => {
                st.in_flight_claims.remove(&claim);
                apply_result(st, policy, outcome, token, now)
            }
            ClaimState::Provisional(deciding) if deciding > 1 => {
                entry.state = ClaimState::Provisional(deciding - 1);
                None
            }
            ClaimState::Provisional(_) if entry.token.is_none() => {
                // No worker here registered the claim.
                st.in_flight_claims.remove(&claim);
                None
            }
            ClaimState::Provisional(_) => {
                entry.state = ClaimState::Running;
                None
            }
            ClaimState::LostPending(deciding) if deciding > 1 => {
                entry.state = ClaimState::LostPending(deciding - 1);
                None
            }
            ClaimState::LostPending(_) => {
                // No enforcer here timed it out, so another process did.
                let token = entry.token;
                st.in_flight_claims.remove(&claim);
                token.and_then(|token| count_remote_timeout(st, policy, token, now))
            }
            ClaimState::Ended(deciding) if deciding > 1 => {
                entry.state = ClaimState::Ended(deciding - 1);
                None
            }
            ClaimState::Ended(_) => {
                // The worker reported nothing, and no timeout counts.
                st.in_flight_claims.remove(&claim);
                None
            }
            ClaimState::Running | ClaimState::TimedOut => None,
        }
    }

    /// Record a retryable failure observed **out of band** — i.e. not from a
    /// handler return value with a dispatch token, but from an enforcement path
    /// such as an activity start-to-close / heartbeat timeout (issue #369).
    ///
    /// A timeout against a protected downstream is a transient/downstream-style
    /// failure that should count toward tripping the breaker, but the worker
    /// that dispatched the attempt may be gone, so there is no token to pass.
    /// This applies closed-phase accounting (and trips at threshold) without a
    /// token; it intentionally does **not** resolve a half-open probe (only the
    /// admitted probe's own result does that) and is ignored while the breaker
    /// is already open or operator-forced.
    ///
    /// Returns `Some(CircuitTransition::Tripped)` if this failure opened the
    /// breaker. Activities without a policy are ignored (`None`).
    pub fn on_external_failure(
        &self,
        activity_name: &str,
        now: Instant,
    ) -> Option<CircuitTransition> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        apply_external_failure(st, policy, now)
    }

    /// Release breaker accounting for a dispatch that was **cancelled** mid-flight
    /// (the workflow or task was cancelled out from under the attempt).
    ///
    /// A cancellation is not evidence about downstream health, so it never trips
    /// the breaker and never resolves a probe as a successful downstream call.
    /// Ordinary closed-state cancellations are therefore a no-op.
    ///
    /// The one case that *must* be handled: if the cancelled attempt held the
    /// single half-open probe, simply dropping its outcome would leave the
    /// breaker stuck in `HalfOpen` with `probe_in_flight = true` forever, so
    /// every later dispatch would short-circuit and no probe could ever be
    /// admitted. Here we release the probe slot and re-arm the cooldown (via the
    /// same state reset as a trip) so a fresh probe is admitted after the next
    /// cooldown — without emitting a trip/close transition.
    ///
    /// Generation-fenced like [`on_result`](Self::on_result): a token predating
    /// the breaker's current generation is stale and ignored.
    pub fn on_cancelled(&self, activity_name: &str, token: DispatchToken, now: Instant) {
        if !self.policies.contains_key(activity_name) {
            return;
        }
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        apply_cancelled(st, token, now);
    }

    /// Operator action: pin the breaker open for manual incident response.
    pub fn force_open(&self, activity_name: &str, now: Instant) {
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.forced_open = true;
        st.phase = CircuitPhase::Open;
        st.opened_at = Some(now);
        st.opened_at_wall = Some(Utc::now());
        st.probe_in_flight = false;
        // Fence any attempt dispatched before this manual pin.
        st.bump_generation();
    }

    /// Operator action: clear any pin and reset the breaker to closed so normal
    /// tracking resumes ("I know the downstream is back, close it now").
    ///
    /// Idempotent: if the breaker is **already** closed and not operator-forced,
    /// this is a no-op. A repeated force-close (operator retry / idempotency
    /// replay) must not bump the generation or clear the failure window again —
    /// doing so would fence out the results of attempts admitted by the first
    /// close and discard failures they have since accumulated, which could mask
    /// an outage re-emerging right after the reset. Only the first close fences
    /// pre-close work.
    pub fn force_close(&self, activity_name: &str) {
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        if !st.forced_open && st.phase == CircuitPhase::Closed {
            return;
        }
        st.forced_open = false;
        st.close();
    }

    /// The sorted set of every activity name with a declared circuit-breaker
    /// policy. Passed by the worker to [`crate::queue::claim_task`] so the claim
    /// query can skip the rate-limit gate **and** token debit for these
    /// activities: their rate limiting is enforced authoritatively at *dispatch*
    /// instead (issue #369). Enforcing at dispatch — gated on the real
    /// [`on_dispatch`](Self::on_dispatch) decision — is what lets `CircuitOpen`
    /// short-circuits propagate at full speed during an outage while still
    /// guaranteeing a genuine downstream call never runs without a token. The
    /// set is static (fixed at construction), so this returns a cheap slice.
    #[must_use]
    pub fn tracked_activity_names(&self) -> &[String] {
        &self.tracked_names
    }

    /// Observable snapshot for a single activity, or `None` if it has no policy.
    #[must_use]
    pub fn snapshot(&self, activity_name: &str, now: Instant) -> Option<CircuitSnapshot> {
        let &policy = self.policies.get(activity_name)?;
        let mut states = self.lock();
        let st = states.entry(activity_name.to_string()).or_default();
        st.prune(now, policy.window);
        Some(Self::snapshot_inner(activity_name, &policy, st, now))
    }

    /// Observable snapshots for every activity with a declared policy, sorted by
    /// name.
    #[must_use]
    pub fn list(&self, now: Instant) -> Vec<CircuitSnapshot> {
        let mut states = self.lock();
        let mut out: Vec<CircuitSnapshot> = self
            .policies
            .iter()
            .map(|(name, policy)| {
                let st = states.entry(name.clone()).or_default();
                st.prune(now, policy.window);
                Self::snapshot_inner(name, policy, st, now)
            })
            .collect();
        out.sort_by(|a, b| a.activity_name.cmp(&b.activity_name));
        out
    }

    fn snapshot_inner(
        activity_name: &str,
        policy: &CircuitBreakerPolicy,
        st: &BreakerState,
        now: Instant,
    ) -> CircuitSnapshot {
        // Only a non-forced open breaker counts down toward a probe; a forced
        // pin and the closed/half-open phases report no ETA.
        let time_until_probe_secs = if st.phase == CircuitPhase::Open && !st.forced_open {
            st.opened_at.map(|opened| {
                let elapsed = now.saturating_duration_since(opened);
                policy.cooldown.saturating_sub(elapsed).as_secs_f64()
            })
        } else {
            None
        };
        CircuitSnapshot {
            activity_name: activity_name.to_string(),
            state: st.phase.as_str(),
            forced_open: st.forced_open,
            last_trip: st.opened_at_wall,
            rolling_failure_count: u32::try_from(st.failures.len()).unwrap_or(u32::MAX),
            time_until_probe_secs,
            failure_threshold: policy.failure_threshold,
            window_secs: policy.window.as_secs_f64(),
            cooldown_secs: policy.cooldown.as_secs_f64(),
            open_mode: policy.open_mode,
        }
    }
}

impl Default for CircuitBreakerRegistry {
    fn default() -> Self {
        Self::empty()
    }
}

// ---------------------------------------------------------------------------
// Tests (red phase: written before the implementation above existed)
// ---------------------------------------------------------------------------

/// Apply one attempt outcome to an activity's breaker state.
fn apply_result(
    st: &mut BreakerState,
    policy: CircuitBreakerPolicy,
    outcome: AttemptOutcome,
    token: DispatchToken,
    now: Instant,
) -> Option<CircuitTransition> {
    if st.forced_open {
        // Operator-pinned: ignore organic results until force-closed.
        return None;
    }

    // Generation fence: an attempt dispatched before the breaker's last
    // state-resetting transition (trip / close / force-open / force-close)
    // is stale and must not move the breaker. This subsumes the half-open
    // straggler case AND the "pre-force-close failure re-trips the reset"
    // case in one check.
    if token.generation != st.generation {
        return None;
    }

    match st.phase {
        CircuitPhase::Closed => match outcome {
            // A success clears the rolling failure window.
            AttemptOutcome::Success => {
                st.failures.clear();
                None
            }
            // A non-retryable (permanent per-request) error is excluded from
            // trip counts entirely: it is neither downstream sickness nor
            // proof of health, so it leaves the rolling window untouched.
            AttemptOutcome::NonRetryableFailure => None,
            AttemptOutcome::RetryableFailure => {
                st.failures.push_back(now);
                st.prune(now, policy.window);
                if st.failures.len() >= policy.failure_threshold as usize {
                    st.trip(now);
                    Some(CircuitTransition::Tripped)
                } else {
                    None
                }
            }
        },
        CircuitPhase::HalfOpen => {
            // Only the admitted probe decides the half-open outcome. A
            // same-generation non-probe (shouldn't normally happen, but be
            // defensive) must not close the breaker early or restart cooldown.
            if !token.is_probe {
                return None;
            }
            match outcome {
                // The probe reached the downstream and it answered: recovered.
                AttemptOutcome::Success => {
                    st.close();
                    Some(CircuitTransition::Closed)
                }
                // The probe failed transiently: the downstream is still down.
                AttemptOutcome::RetryableFailure => {
                    st.trip(now);
                    Some(CircuitTransition::Tripped)
                }
                // A non-retryable per-request error (e.g. bad input) does NOT
                // prove the downstream recovered — the error may have been
                // produced before the dependency was even touched. Treat the
                // probe as inconclusive: release the probe slot and re-arm the
                // cooldown so a fresh probe is admitted later, but emit no
                // transition (it is neither a recovery nor a downstream trip).
                AttemptOutcome::NonRetryableFailure => {
                    st.trip(now);
                    None
                }
            }
        }
        // A result arriving while fully open (no probe admitted) is a
        // stale straggler; leave the breaker untouched.
        CircuitPhase::Open => None,
    }
}

/// Count a timeout that another process enforced on a claim of this one
/// (issue #1809). A failed probe re-opens the breaker. Otherwise a closed
/// breaker adds the failure to its window.
fn count_remote_timeout(
    st: &mut BreakerState,
    policy: CircuitBreakerPolicy,
    token: DispatchToken,
    now: Instant,
) -> Option<CircuitTransition> {
    if apply_cancelled(st, token, now) {
        return Some(CircuitTransition::Tripped);
    }
    // Generation fence: a claim dispatched before a trip or a reset counts
    // nothing, as a stale handler result does.
    if token.generation != st.generation {
        return None;
    }
    apply_external_failure(st, policy, now)
}

/// Count one out-of-band failure, such as an enforced timeout.
fn apply_external_failure(
    st: &mut BreakerState,
    policy: CircuitBreakerPolicy,
    now: Instant,
) -> Option<CircuitTransition> {
    // Only count while closed and not operator-pinned. While open/half-open
    // the cooldown/probe machinery already governs recovery.
    if st.forced_open || st.phase != CircuitPhase::Closed {
        return None;
    }
    st.failures.push_back(now);
    st.prune(now, policy.window);
    if st.failures.len() >= policy.failure_threshold as usize {
        st.trip(now);
        Some(CircuitTransition::Tripped)
    } else {
        None
    }
}

/// Release the breaker accounting of a cancelled or timed-out dispatch.
/// Returns whether it released a probe and re-opened the breaker.
fn apply_cancelled(st: &mut BreakerState, token: DispatchToken, now: Instant) -> bool {
    if st.forced_open || token.generation != st.generation {
        return false;
    }
    // Only the in-flight half-open probe needs releasing; everything else
    // (closed-state cancellation, fully-open straggler) is a no-op.
    if token.is_probe && st.phase == CircuitPhase::HalfOpen && st.probe_in_flight {
        st.trip(now);
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> CircuitBreakerPolicy {
        // Trip after 3 failures in 30s; re-probe after 60s.
        CircuitBreakerPolicy::new(3, Duration::from_secs(30), Duration::from_secs(60))
    }

    fn registry() -> CircuitBreakerRegistry {
        let mut p = HashMap::new();
        p.insert("send_email".to_string(), policy());
        CircuitBreakerRegistry::new(p)
    }

    /// Dispatch and return the admitted token, panicking if short-circuited.
    fn dispatch(reg: &CircuitBreakerRegistry, now: Instant) -> DispatchToken {
        match reg.on_dispatch("send_email", now) {
            DispatchDecision::Allow { token } => token,
            DispatchDecision::ShortCircuit { .. } => {
                panic!("expected Allow, got ShortCircuit")
            }
        }
    }

    // Convenience wrappers that model the real flow (dispatch → report) with a
    // fresh, current-generation token so closed-phase tests stay readable.
    fn fail(reg: &CircuitBreakerRegistry, now: Instant) -> Option<CircuitTransition> {
        let token = dispatch(reg, now);
        reg.on_result("send_email", AttemptOutcome::RetryableFailure, token, now)
    }
    fn succeed(reg: &CircuitBreakerRegistry, now: Instant) -> Option<CircuitTransition> {
        let token = dispatch(reg, now);
        reg.on_result("send_email", AttemptOutcome::Success, token, now)
    }

    /// `true` if the decision allows dispatch (regardless of probe flag).
    fn allowed(d: &DispatchDecision) -> bool {
        matches!(d, DispatchDecision::Allow { .. })
    }

    #[test]
    fn untracked_activity_always_allows_and_ignores_results() {
        let reg = CircuitBreakerRegistry::empty();
        let now = Instant::now();
        assert_eq!(
            reg.on_dispatch("anything", now),
            DispatchDecision::Allow {
                token: DispatchToken {
                    generation: 0,
                    is_probe: false,
                },
            }
        );
        let untracked_token = DispatchToken {
            generation: 0,
            is_probe: false,
        };
        assert_eq!(
            reg.on_result(
                "anything",
                AttemptOutcome::RetryableFailure,
                untracked_token,
                now
            ),
            None
        );
        assert!(reg.snapshot("anything", now).is_none());
        assert!(reg.is_empty());
    }

    /// A token for claims whose probe role does not matter to the test.
    const TOKEN: DispatchToken = DispatchToken {
        generation: 0,
        is_probe: false,
    };

    fn claim(attempt: i32) -> ClaimKey {
        ClaimKey {
            task_id: Uuid::from_u128(1809),
            attempt,
            started_at: None,
        }
    }

    fn rolling(reg: &CircuitBreakerRegistry, now: Instant) -> u32 {
        reg.snapshot("send_email", now)
            .expect("tracked")
            .rolling_failure_count
    }

    /// Issue #1809: the enforcer marks the claim, then counts the timeout. A
    /// late success of that claim must not clear the counted failure.
    #[test]
    fn late_success_of_a_timed_out_claim_keeps_the_failure() {
        let reg = registry();
        let t0 = Instant::now();
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        let _ = reg.confirm_claim_timed_out("send_email", claim(1), false, t0);
        let _ = reg.on_external_failure("send_email", t0);
        assert_eq!(rolling(&reg, t0), 1);

        let late = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
        assert_eq!(late, None);
        assert_eq!(rolling(&reg, t0), 1, "the late success is fenced");

        // The mark is used up. Another claim of the same task still counts.
        let next = dispatch(&reg, t0);
        let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, next, claim(2), t0);
        assert_eq!(rolling(&reg, t0), 0);
    }

    /// A result that lands before the mark counts as usual. The timeout then
    /// counts too, so no order loses the timeout.
    #[test]
    fn result_before_the_mark_counts_and_the_timeout_still_counts() {
        let reg = registry();
        let t0 = Instant::now();
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), TOKEN);
        let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
        reg.mark_claim_timed_out("send_email", claim(1));
        let _ = reg.confirm_claim_timed_out("send_email", claim(1), false, t0);
        let _ = reg.on_external_failure("send_email", t0);
        assert_eq!(rolling(&reg, t0), 1);
    }

    /// A timed-out probe releases its slot, so a later probe can run.
    #[test]
    fn timed_out_probe_releases_its_slot() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let t1 = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, t1);
        assert!(probe.is_probe());
        reg.begin_claim("send_email", claim(1), probe);
        reg.mark_claim_timed_out("send_email", claim(1));
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(1), false, t1),
            Some(CircuitTransition::Tripped),
            "a timed-out probe re-trips the breaker"
        );
        // The handler has not returned. The confirm alone frees the slot.
        assert_eq!(
            reg.snapshot("send_email", t1).expect("tracked").state,
            "open"
        );
        let t2 = t1 + Duration::from_secs(61);
        assert!(dispatch(&reg, t2).is_probe(), "a fresh probe is admitted");
        let late = reg.on_claim_result("send_email", AttemptOutcome::Success, probe, claim(1), t2);
        assert_eq!(late, None, "a timed-out probe does not close the breaker");
    }

    /// The enforcer marks before its transaction and unmarks when the
    /// transaction does not time the claim out. The result then counts.
    #[test]
    fn unmarked_claim_counts_again() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        let _ = reg.unmark_claim_timed_out("send_email", claim(1), t0);
        let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
        assert_eq!(
            rolling(&reg, t0),
            0,
            "the success counts and clears the window"
        );
    }

    /// A result that arrives while the enforcer decides is held. A no-op
    /// enforcement then applies it, so a successful probe still closes the
    /// breaker.
    #[test]
    fn held_result_counts_when_the_enforcer_does_nothing() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let t1 = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, t1);
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        let held = reg.on_claim_result("send_email", AttemptOutcome::Success, probe, claim(1), t1);
        assert_eq!(held, None, "the result waits for the enforcer");
        reg.end_claim("send_email", claim(1));
        let applied = reg.unmark_claim_timed_out("send_email", claim(1), t1);
        assert_eq!(applied, Some(CircuitTransition::Closed));
    }

    /// A held result is dropped when the enforcer times the claim out.
    #[test]
    fn held_result_is_dropped_when_the_enforcer_acts() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
        let _ = reg.confirm_claim_timed_out("send_email", claim(1), false, t0);
        assert_eq!(
            rolling(&reg, t0),
            1,
            "the late success does not clear the window"
        );
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());
    }

    /// Two enforcers race on one claim. One times it out and the other does
    /// nothing. In either order, the late result stays fenced.
    #[test]
    fn racing_enforcers_keep_the_fence_in_either_order() {
        for rollback_first in [true, false] {
            let reg = registry();
            let t0 = Instant::now();
            fail(&reg, t0);
            let token = dispatch(&reg, t0);
            reg.begin_claim("send_email", claim(1), TOKEN);
            reg.mark_claim_timed_out("send_email", claim(1));
            reg.mark_claim_timed_out("send_email", claim(1));
            if rollback_first {
                let _ = reg.unmark_claim_timed_out("send_email", claim(1), t0);
                let _ = reg.confirm_claim_timed_out("send_email", claim(1), false, t0);
            } else {
                let _ = reg.confirm_claim_timed_out("send_email", claim(1), false, t0);
                let _ = reg.unmark_claim_timed_out("send_email", claim(1), t0);
            }
            let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
            assert_eq!(rolling(&reg, t0), 1, "rollback first: {rollback_first}");
        }
    }

    /// A result held during two rollbacks counts once, after the last one.
    #[test]
    fn held_result_waits_for_every_racing_enforcer() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        reg.mark_claim_timed_out("send_email", claim(1));
        let _ = reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(1), t0);
        let _ = reg.unmark_claim_timed_out("send_email", claim(1), t0);
        assert_eq!(rolling(&reg, t0), 1, "one enforcer still decides");
        let _ = reg.unmark_claim_timed_out("send_email", claim(1), t0);
        assert_eq!(rolling(&reg, t0), 0, "the held success counts now");
    }

    /// A lost claim never counts its own result (issue #1809). Only a timeout
    /// that no enforcer here marked counts, because another process enforced
    /// it and breaker state is per process.
    #[test]
    fn a_lost_claim_counts_only_a_timeout_enforced_elsewhere() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);

        // Not a timeout: an operator action, say. Nothing counts.
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), token);
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(1), false, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 1);

        // A timeout that an enforcer here confirmed: that enforcer counts it.
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(2), token);
        reg.mark_claim_timed_out("send_email", claim(2));
        let _ = reg.confirm_claim_timed_out("send_email", claim(2), false, t0);
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(2), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 1, "the local enforcer counts it");

        // A timeout enforced elsewhere counts here, and trips at threshold.
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(3), token);
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(3), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 2);
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(4), token);
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(4), true, t0),
            Some(CircuitTransition::Tripped)
        );

        // A probe lost to a timeout enforced elsewhere is a failed probe.
        let t1 = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, t1);
        reg.begin_claim("send_email", claim(5), probe);
        assert_eq!(
            reg.on_claim_lost("send_email", probe, claim(5), true, t1),
            Some(CircuitTransition::Tripped)
        );
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());
    }

    /// A timeout of a claim dispatched before a reset counts nothing, in the
    /// enforcer and in the owner (issue #1809).
    #[test]
    fn a_timeout_from_before_a_reset_counts_nothing() {
        let reg = registry();
        let t0 = Instant::now();
        let stale = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), stale);
        reg.begin_claim("send_email", claim(2), stale);
        for _ in 0..3 {
            fail(&reg, t0);
        }
        reg.force_close("send_email");

        reg.mark_claim_timed_out("send_email", claim(1));
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(1), true, t0),
            None
        );
        assert_eq!(
            reg.on_claim_lost("send_email", stale, claim(2), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 0, "the reset fences both counts");

        // A fresh claim's timeout counts.
        let fresh = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(3), fresh);
        reg.mark_claim_timed_out("send_email", claim(3));
        let _ = reg.confirm_claim_timed_out("send_email", claim(3), true, t0);
        assert_eq!(rolling(&reg, t0), 1);
    }

    /// A worker can end its claim with no result while an enforcer decides.
    /// The entry keeps the token, so the generation fence still holds
    /// (issue #1809).
    #[test]
    fn an_ended_claim_keeps_its_fence_until_the_enforcer_decides() {
        let reg = registry();
        let t0 = Instant::now();
        let stale = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), stale);
        reg.mark_claim_timed_out("send_email", claim(1));
        reg.end_claim("send_email", claim(1));
        for _ in 0..3 {
            fail(&reg, t0);
        }
        reg.force_close("send_email");
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(1), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 0, "the reset fences the ended claim");
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        // A current claim still counts, and a rollback leaves nothing.
        let fresh = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(2), fresh);
        reg.mark_claim_timed_out("send_email", claim(2));
        reg.end_claim("send_email", claim(2));
        let _ = reg.confirm_claim_timed_out("send_email", claim(2), true, t0);
        assert_eq!(rolling(&reg, t0), 1);
        reg.begin_claim("send_email", claim(3), fresh);
        reg.mark_claim_timed_out("send_email", claim(3));
        reg.end_claim("send_email", claim(3));
        assert_eq!(reg.unmark_claim_timed_out("send_email", claim(3), t0), None);
        assert_eq!(rolling(&reg, t0), 1);
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());
    }

    /// A mark on another process's claim records the generation. A reset
    /// before the confirm fences that count too (issue #1809).
    #[test]
    fn an_unregistered_mark_is_fenced_by_a_reset() {
        let reg = registry();
        let t0 = Instant::now();
        reg.mark_claim_timed_out("send_email", claim(1));
        for _ in 0..3 {
            fail(&reg, t0);
        }
        reg.force_close("send_email");
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(1), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 0, "the reset fences the remote count");
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        // A claim that is not marked counts nothing at all.
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(2), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 0);
    }

    /// A worker that sees no timeout while an enforcer here still decides
    /// keeps the token. The enforcer's verdict counts once, fenced by
    /// generation (issue #1809).
    #[test]
    fn a_loss_seen_as_no_timeout_still_waits_for_the_enforcer() {
        let reg = registry();
        let t0 = Instant::now();
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), token);
        reg.mark_claim_timed_out("send_email", claim(1));
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(1), false, t0),
            None
        );
        let _ = reg.confirm_claim_timed_out("send_email", claim(1), true, t0);
        assert_eq!(rolling(&reg, t0), 1, "the enforcer's verdict counts once");
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        let stale = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(2), stale);
        reg.mark_claim_timed_out("send_email", claim(2));
        assert_eq!(
            reg.on_claim_lost("send_email", stale, claim(2), false, t0),
            None
        );
        for _ in 0..2 {
            fail(&reg, t0);
        }
        reg.force_close("send_email");
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(2), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 0, "the reset fences the stale claim");
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());
    }

    /// A mark on a claim that no worker here registered leaves nothing behind
    /// once its enforcer decides (issue #1809).
    #[test]
    fn an_unregistered_mark_leaves_nothing_behind() {
        let reg = registry();
        let t0 = Instant::now();
        reg.mark_claim_timed_out("send_email", claim(8));
        assert_eq!(reg.unmark_claim_timed_out("send_email", claim(8), t0), None);
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        // A claim of another process: the confirm counts it once.
        reg.mark_claim_timed_out("send_email", claim(9));
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(9), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 1);
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        // An ended claim waits for its enforcer, then leaves nothing.
        reg.begin_claim("send_email", claim(1), TOKEN);
        reg.mark_claim_timed_out("send_email", claim(1));
        reg.end_claim("send_email", claim(1));
        assert!(!reg.lock()["send_email"].in_flight_claims.is_empty());
        assert_eq!(reg.unmark_claim_timed_out("send_email", claim(1), t0), None);
        let states = reg.lock();
        assert!(states["send_email"].in_flight_claims.is_empty());
    }

    /// An enforcer can mark a claim before its worker registers it. The
    /// worker keeps the mark, so its loss waits for the enforcer, and the
    /// timeout counts once (issue #1809).
    #[test]
    fn a_claim_registered_after_its_mark_counts_its_timeout_once() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);

        reg.mark_claim_timed_out("send_email", claim(1));
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(1), token);
        assert_eq!(
            reg.on_claim_lost("send_email", token, claim(1), true, t0),
            None,
            "the loss waits for the enforcer that marked the claim"
        );
        assert_eq!(rolling(&reg, t0), 1);
        assert_eq!(
            reg.confirm_claim_timed_out("send_email", claim(1), true, t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 2, "one timeout, one failure");
        assert!(reg.lock()["send_email"].in_flight_claims.is_empty());

        // A late result of such a claim waits for the enforcer too.
        reg.mark_claim_timed_out("send_email", claim(2));
        let token = dispatch(&reg, t0);
        reg.begin_claim("send_email", claim(2), token);
        assert_eq!(
            reg.on_claim_result("send_email", AttemptOutcome::Success, token, claim(2), t0),
            None
        );
        assert_eq!(rolling(&reg, t0), 2, "the success is held");
        assert_eq!(reg.unmark_claim_timed_out("send_email", claim(2), t0), None);
        assert_eq!(rolling(&reg, t0), 0, "no timeout, so the success counts");
    }

    #[test]
    fn closed_breaker_allows_dispatch() {
        let reg = registry();
        let now = Instant::now();
        let token = dispatch(&reg, now);
        assert!(!token.is_probe());
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert_eq!(snap.rolling_failure_count, 0);
    }

    #[test]
    fn success_does_not_trip_and_resets_failures() {
        let reg = registry();
        let now = Instant::now();
        fail(&reg, now);
        fail(&reg, now);
        assert_eq!(
            reg.snapshot("send_email", now)
                .unwrap()
                .rolling_failure_count,
            2
        );
        // A success clears the rolling window.
        assert_eq!(succeed(&reg, now), None);
        assert_eq!(
            reg.snapshot("send_email", now)
                .unwrap()
                .rolling_failure_count,
            0
        );
    }

    #[test]
    fn trips_open_at_threshold() {
        let reg = registry();
        let now = Instant::now();
        assert_eq!(fail(&reg, now), None);
        assert_eq!(fail(&reg, now), None);
        assert_eq!(fail(&reg, now), Some(CircuitTransition::Tripped));
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "open");
        assert!(snap.last_trip.is_some());
    }

    #[test]
    fn non_retryable_failures_never_trip_the_breaker() {
        // A burst of permanent per-request errors (bad input) must not open the
        // circuit: the downstream is healthy enough to give definitive answers.
        let reg = registry();
        let now = Instant::now();
        for _ in 0..50 {
            let token = dispatch(&reg, now);
            assert_eq!(
                reg.on_result(
                    "send_email",
                    AttemptOutcome::NonRetryableFailure,
                    token,
                    now
                ),
                None
            );
        }
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert_eq!(snap.rolling_failure_count, 0);
        assert!(allowed(&reg.on_dispatch("send_email", now)));
    }

    #[test]
    fn open_breaker_short_circuits_until_cooldown() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        // Immediately after trip: short-circuit with retry_after ~= cooldown.
        match reg.on_dispatch("send_email", t0) {
            DispatchDecision::ShortCircuit { retry_after, .. } => {
                let after = retry_after.expect("cooldown-based open advertises a retry-after");
                assert!(after <= Duration::from_secs(60));
                assert!(after > Duration::from_secs(59));
            }
            DispatchDecision::Allow { .. } => panic!("expected ShortCircuit, got Allow"),
        }
        // Half-way through cooldown: still short-circuit.
        assert!(matches!(
            reg.on_dispatch("send_email", t0 + Duration::from_secs(30)),
            DispatchDecision::ShortCircuit { .. }
        ));
    }

    #[test]
    fn failures_outside_window_do_not_count() {
        let reg = registry();
        let t0 = Instant::now();
        fail(&reg, t0);
        fail(&reg, t0);
        // Third failure arrives after the 30s window — the first two have aged out.
        assert_eq!(
            fail(&reg, t0 + Duration::from_secs(31)),
            None,
            "stale failures should not contribute to the threshold"
        );
        assert_eq!(
            reg.snapshot("send_email", t0 + Duration::from_secs(31))
                .unwrap()
                .state,
            "closed"
        );
    }

    #[test]
    fn half_open_admits_single_probe_then_closes_on_success() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        // First dispatch after cooldown is admitted as the probe.
        let probe = dispatch(&reg, probe_time);
        assert!(probe.is_probe());
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "half_open"
        );
        // While the probe is in flight, other dispatches short-circuit.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));
        // Probe succeeds: breaker closes.
        assert_eq!(
            reg.on_result("send_email", AttemptOutcome::Success, probe, probe_time),
            Some(CircuitTransition::Closed)
        );
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "closed"
        );
        assert!(allowed(&reg.on_dispatch("send_email", probe_time)));
    }

    #[test]
    fn half_open_reopens_on_probe_failure() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, probe_time);
        assert!(probe.is_probe());
        // Probe fails: breaker re-opens.
        assert_eq!(
            reg.on_result(
                "send_email",
                AttemptOutcome::RetryableFailure,
                probe,
                probe_time
            ),
            Some(CircuitTransition::Tripped)
        );
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "open"
        );
        // And the cooldown clock restarts from the probe failure.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));
    }

    #[test]
    fn stale_straggler_result_does_not_resolve_half_open_probe() {
        // Scenario: the breaker trips while an earlier attempt is still running.
        // After the cooldown a probe is admitted; then that pre-trip straggler
        // finishes. Its token carries the old generation, so it must NOT close
        // or re-open the breaker — only the admitted probe decides.
        let reg = registry();
        let t0 = Instant::now();
        // A real in-flight attempt dispatched while still closed (pre-trip).
        let straggler = dispatch(&reg, t0);
        // Independently, three failures trip the breaker (bumping the generation).
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        // Admit the probe (a new, current-generation token).
        let probe = dispatch(&reg, probe_time);
        assert!(probe.is_probe());
        // The straggler SUCCESS (old generation) arrives — ignored, stays half-open.
        assert_eq!(
            reg.on_result("send_email", AttemptOutcome::Success, straggler, probe_time),
            None,
            "a stale-generation straggler success must not close the breaker"
        );
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "half_open"
        );
        // The real probe now succeeds and closes the breaker.
        assert_eq!(
            reg.on_result("send_email", AttemptOutcome::Success, probe, probe_time),
            Some(CircuitTransition::Closed)
        );
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "closed"
        );
    }

    #[test]
    fn pre_force_close_failure_does_not_re_trip_after_reset() {
        // Operator force-opens during an outage, then force-closes while an
        // older attempt is still in flight. That pre-close failure must not
        // immediately re-trip the just-reset breaker (issue: low threshold).
        let reg = registry();
        let now = Instant::now();
        // An attempt dispatched while closed, before any operator action.
        let stale = dispatch(&reg, now);
        // Operator pins open, then resets to closed (each bumps the generation).
        reg.force_open("send_email", now);
        reg.force_close("send_email");
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "closed");
        // The stale pre-close failure lands — fenced out by generation, so the
        // breaker stays closed and the operator's reset stands.
        assert_eq!(
            reg.on_result("send_email", AttemptOutcome::RetryableFailure, stale, now),
            None,
            "a pre-force-close failure must not re-trip the reset breaker"
        );
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "closed");
        assert_eq!(
            reg.snapshot("send_email", now)
                .unwrap()
                .rolling_failure_count,
            0
        );
    }

    #[test]
    fn force_open_short_circuits_and_ignores_results() {
        let reg = registry();
        let now = Instant::now();
        reg.force_open("send_email", now);
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "open");
        assert!(snap.forced_open);
        // Forced-open advertises no retry-after: no probe is admitted on any
        // timer, so callers must not derive a wait interval.
        assert_eq!(
            reg.on_dispatch("send_email", now),
            DispatchDecision::ShortCircuit {
                opened_at: snap.last_trip,
                retry_after: None,
            }
        );
        // Even far past the cooldown, a forced-open breaker never probes.
        assert!(matches!(
            reg.on_dispatch("send_email", now + Duration::from_secs(600)),
            DispatchDecision::ShortCircuit { .. }
        ));
        // Organic successes do not auto-close a forced-open breaker. (Dispatch
        // short-circuits while forced, so feed a result with any token — the
        // forced-open guard ignores it regardless.)
        let any_token = DispatchToken {
            generation: 0,
            is_probe: false,
        };
        assert_eq!(
            reg.on_result("send_email", AttemptOutcome::Success, any_token, now),
            None
        );
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "open");
    }

    #[test]
    fn force_close_resets_to_closed() {
        let reg = registry();
        let now = Instant::now();
        for _ in 0..3 {
            fail(&reg, now);
        }
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "open");
        reg.force_close("send_email");
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert!(!snap.forced_open);
        assert_eq!(snap.rolling_failure_count, 0);
        assert!(allowed(&reg.on_dispatch("send_email", now)));
    }

    // A repeated force-close on an already-closed, unforced breaker must be a
    // no-op: it must not bump the generation (fencing post-close attempts) or
    // clear failures those attempts have legitimately accumulated.
    #[test]
    fn duplicate_force_close_is_a_noop() {
        let reg = registry();
        let now = Instant::now();
        for _ in 0..3 {
            fail(&reg, now);
        }
        reg.force_close("send_email");
        assert_eq!(reg.snapshot("send_email", now).unwrap().state, "closed");

        // After the close, fresh attempts begin and a couple legitimately fail
        // (a new outage is emerging). These must keep accumulating toward a trip.
        fail(&reg, now);
        fail(&reg, now);
        assert_eq!(
            reg.snapshot("send_email", now)
                .unwrap()
                .rolling_failure_count,
            2,
            "post-close failures must accumulate toward a re-trip"
        );

        // An operator/idempotency retry of force-close on the already-closed
        // breaker must NOT wipe that accumulating window...
        reg.force_close("send_email");
        assert_eq!(
            reg.snapshot("send_email", now)
                .unwrap()
                .rolling_failure_count,
            2,
            "duplicate force-close must not clear the post-close failure window"
        );

        // ...and the generation must be unchanged, so a third failure still trips
        // the breaker (its in-flight token from this generation is not fenced).
        assert_eq!(
            fail(&reg, now),
            Some(CircuitTransition::Tripped),
            "the re-emerging outage must still trip after a duplicate force-close"
        );
    }

    #[test]
    fn list_returns_all_policies_sorted() {
        let mut p = HashMap::new();
        p.insert("zeta".to_string(), policy());
        p.insert("alpha".to_string(), policy());
        let reg = CircuitBreakerRegistry::new(p);
        let now = Instant::now();
        let snaps = reg.list(now);
        assert_eq!(snaps.len(), 2);
        assert_eq!(snaps[0].activity_name, "alpha");
        assert_eq!(snaps[1].activity_name, "zeta");
    }

    #[test]
    fn snapshot_reports_time_until_probe() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let snap = reg
            .snapshot("send_email", t0 + Duration::from_secs(20))
            .unwrap();
        let remaining = snap
            .time_until_probe_secs
            .expect("open breaker reports probe ETA");
        // 60s cooldown - 20s elapsed ~= 40s.
        assert!((remaining - 40.0).abs() < 1.0, "remaining was {remaining}");
    }

    #[test]
    fn threshold_clamped_to_at_least_one() {
        let p = CircuitBreakerPolicy::new(0, Duration::from_secs(10), Duration::from_secs(10));
        assert_eq!(p.failure_threshold, 1);
    }

    // tracked_activity_names returns the static, sorted set of every activity
    // with a declared policy regardless of current breaker phase — the worker
    // passes it to claim_task to skip claim-time rate limiting (enforced at
    // dispatch instead).
    #[test]
    fn tracked_activity_names_lists_all_policies_sorted() {
        let mut p = HashMap::new();
        p.insert("zeta".to_string(), policy());
        p.insert("alpha".to_string(), policy());
        let reg = CircuitBreakerRegistry::new(p);
        assert_eq!(reg.tracked_activity_names(), ["alpha", "zeta"]);

        // The set is phase-independent: tripping a breaker does not change it.
        let now = Instant::now();
        for _ in 0..3 {
            let token = match reg.on_dispatch("alpha", now) {
                DispatchDecision::Allow { token } => token,
                DispatchDecision::ShortCircuit { .. } => unreachable!(),
            };
            reg.on_result("alpha", AttemptOutcome::RetryableFailure, token, now);
        }
        assert_eq!(reg.snapshot("alpha", now).unwrap().state, "open");
        assert_eq!(reg.tracked_activity_names(), ["alpha", "zeta"]);

        assert_eq!(
            CircuitBreakerRegistry::empty()
                .tracked_activity_names()
                .len(),
            0
        );
    }

    // A half-open probe that returns a non-retryable (bad-input) error must NOT
    // close the breaker: a per-request error doesn't prove the downstream
    // recovered. It is inconclusive — the breaker re-arms its cooldown and waits
    // for another probe, emitting no transition.
    #[test]
    fn half_open_nonretryable_probe_is_inconclusive_and_rearms() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, probe_time);
        assert!(probe.is_probe());

        // A non-retryable probe result neither closes nor trips: no transition.
        assert_eq!(
            reg.on_result(
                "send_email",
                AttemptOutcome::NonRetryableFailure,
                probe,
                probe_time
            ),
            None,
            "non-retryable probe must not close (or trip) the breaker"
        );
        // The breaker stays open (re-armed) — it did NOT close on bad input.
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "open",
            "inconclusive probe must leave the breaker open, not closed"
        );
        // Cooldown restarts from the inconclusive probe; immediate dispatch still
        // short-circuits, and a fresh probe is admitted only after another cooldown.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));
        let next_probe = dispatch(&reg, probe_time + Duration::from_secs(61));
        assert!(
            next_probe.is_probe(),
            "a fresh probe must be admitted after the re-armed cooldown"
        );
    }

    // A cancellation that hits the single half-open probe must release the probe
    // slot, or the breaker would stay HalfOpen with probe_in_flight=true forever
    // and short-circuit every later dispatch with no probe ever admitted.
    #[test]
    fn cancelled_half_open_probe_is_released_and_rearms() {
        let reg = registry();
        let t0 = Instant::now();
        for _ in 0..3 {
            fail(&reg, t0);
        }
        let probe_time = t0 + Duration::from_secs(61);
        let probe = dispatch(&reg, probe_time);
        assert!(probe.is_probe());
        // While the probe is "in flight", further dispatches short-circuit.
        assert!(matches!(
            reg.on_dispatch("send_email", probe_time),
            DispatchDecision::ShortCircuit { .. }
        ));

        // The probe attempt is cancelled out from under us.
        reg.on_cancelled("send_email", probe, probe_time);

        // The breaker is re-armed (open), NOT wedged half-open with a dangling
        // probe. A fresh probe is admitted after the next cooldown.
        assert_eq!(
            reg.snapshot("send_email", probe_time).unwrap().state,
            "open"
        );
        let next_probe = dispatch(&reg, probe_time + Duration::from_secs(61));
        assert!(
            next_probe.is_probe(),
            "after a cancelled probe, a fresh probe must eventually be admitted"
        );
    }

    // A cancellation in the ordinary closed state must be a complete no-op: it is
    // not a downstream failure and must not count toward tripping the breaker.
    #[test]
    fn cancelled_closed_state_attempt_is_a_noop() {
        let reg = registry();
        let now = Instant::now();
        let token = dispatch(&reg, now);
        reg.on_cancelled("send_email", token, now);
        let snap = reg.snapshot("send_email", now).unwrap();
        assert_eq!(snap.state, "closed");
        assert_eq!(
            snap.rolling_failure_count, 0,
            "a closed-state cancellation must not count as a failure"
        );
    }
}
