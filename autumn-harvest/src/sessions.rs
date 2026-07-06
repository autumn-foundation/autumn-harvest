//! Worker sessions (issue #606): co-locate an activity pipeline on one worker.
//!
//! Complements the author-facing `WorkflowContext::create_session`/`Session`
//! API in `context.rs`. This module owns the fleet-side, engine-internal
//! concerns:
//!
//! - Pure eligibility and broken-session decision functions (no database
//!   dependency, unit-tested directly — mirroring `poison_pill.rs`'s
//!   `quarantine_decision` and `circuit_breaker.rs`'s pure state machine).
//! - (later steps) the `harvest_sessions` row model, the in-process
//!   session-slot semaphore registry, and the broken-session scanner folded
//!   into `timeout::enforce_timeouts_once`.
//!
//! A worker session pins a *sequence of activities* — not a single unit of
//! work — to one physical worker so they can share machine-local state (a
//! downloaded file, a warmed cache, GPU memory). See the "Fan-out vs worker
//! sessions vs local activity vs claim-check" decision matrix in the crate
//! docs for when to reach for a session instead.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Session-slot acquire eligibility
// ---------------------------------------------------------------------------

/// Whether a worker with `in_use` active sessions out of `max` advertised
/// slots may claim a new session-acquire task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireEligibility {
    /// The worker has a free slot and may claim the acquire task.
    Eligible,
    /// `max_concurrent_sessions <= 0` — sessions are disabled on this worker
    /// (the default; zero behavior change for existing deployments).
    SessionsDisabled,
    /// The worker's advertised capacity is fully in use.
    AtCapacity,
}

/// Pure eligibility check for a session-acquire task.
///
/// Given a worker's advertised `max_concurrent_sessions`
/// (`WorkerConfig::max_concurrent_sessions`) and current `in_use_sessions`
/// count. A negative `in_use_sessions` (should never happen — bookkeeping
/// bug) is treated as `0` rather than allowed to make the worker spuriously
/// over-eligible.
#[must_use]
pub const fn session_acquire_eligible(
    max_concurrent_sessions: i32,
    in_use_sessions: i32,
) -> AcquireEligibility {
    if max_concurrent_sessions <= 0 {
        AcquireEligibility::SessionsDisabled
    } else if (if in_use_sessions < 0 { 0 } else { in_use_sessions }) < max_concurrent_sessions {
        AcquireEligibility::Eligible
    } else {
        AcquireEligibility::AtCapacity
    }
}

impl AcquireEligibility {
    /// `true` iff this worker may claim the acquire task right now.
    #[must_use]
    pub const fn is_eligible(self) -> bool {
        matches!(self, Self::Eligible)
    }
}

// ---------------------------------------------------------------------------
// Broken-session detection
// ---------------------------------------------------------------------------

/// Why a session was (or should be) declared `BROKEN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokenSessionReason {
    /// The host worker's `harvest_workers` heartbeat is stale (crashed or
    /// was killed without a graceful drain).
    HostWorkerLostHeartbeat,
    /// The host worker's status transitioned to `Draining` or `Stopped`.
    HostWorkerDraining,
    /// The session's lease (`expires_at`) elapsed — e.g. a wedged workflow
    /// that never called `Session::complete()`.
    LeaseExpired,
}

impl std::fmt::Display for BrokenSessionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HostWorkerLostHeartbeat => write!(f, "host worker lost heartbeat"),
            Self::HostWorkerDraining => write!(f, "host worker draining"),
            Self::LeaseExpired => write!(f, "session lease expired"),
        }
    }
}

/// Pure decision: should an `ACTIVE` session be marked `BROKEN`?
///
/// Checked in priority order — a dead host is reported before a merely
/// draining one, and an expired lease is only reported once the host is
/// confirmed live (a dead host's session is a liveness problem, not a lease
/// problem). Returns `None` when none of the three conditions hold.
#[must_use]
pub const fn broken_session_reason(
    host_heartbeat_stale: bool,
    host_status_draining_or_stopped: bool,
    lease_expired: bool,
) -> Option<BrokenSessionReason> {
    if host_heartbeat_stale {
        Some(BrokenSessionReason::HostWorkerLostHeartbeat)
    } else if host_status_draining_or_stopped {
        Some(BrokenSessionReason::HostWorkerDraining)
    } else if lease_expired {
        Some(BrokenSessionReason::LeaseExpired)
    } else {
        None
    }
}

/// Pure lease-expiry check: has `now` reached or passed `expires_at`?
#[must_use]
pub fn lease_expired(now: chrono::DateTime<chrono::Utc>, expires_at: chrono::DateTime<chrono::Utc>) -> bool {
    now >= expires_at
}

// ---------------------------------------------------------------------------
// Acquire-retry backoff
// ---------------------------------------------------------------------------

/// Randomized backoff for the session-acquire activity's reschedule after
/// losing an in-process semaphore race.
///
/// The SQL ineligible-activities gate (`queue::claim_task`'s `$6` param) that
/// keeps a slot-full worker from claiming the acquire task is a best-effort
/// optimization only: a task carrying `required_capabilities` is exempt from
/// that gate, so a session-acquire task can still land on a full worker,
/// which loses the in-process semaphore race and must reschedule. A fixed
/// backoff would let two racing acquires hot-loop in lockstep against the
/// same full worker; randomizing within `[min, max]` desyncs them.
///
/// `rng_fraction` is an explicit `[0.0, 1.0]` input (clamped) rather than
/// sampled internally, keeping this function pure and deterministically
/// testable — the caller supplies real randomness (e.g. `rand::random()`).
#[must_use]
pub fn acquire_retry_backoff(rng_fraction: f64, min: Duration, max: Duration) -> Duration {
    let min_ms = u64::try_from(min.as_millis()).unwrap_or(u64::MAX);
    let max_ms = u64::try_from(max.as_millis()).unwrap_or(u64::MAX).max(min_ms);
    let span = max_ms.saturating_sub(min_ms);
    // `span as f64` and `rng_fraction.clamp(0.0, 1.0)` are both non-negative,
    // so the product is always >= 0 -- sign loss cannot actually occur.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let jitter = (span as f64 * rng_fraction.clamp(0.0, 1.0)) as u64;
    Duration::from_millis(min_ms + jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── session_acquire_eligible ─────────────────────────────────────────

    #[test]
    fn session_acquire_eligible_disabled_when_max_is_zero() {
        assert_eq!(
            session_acquire_eligible(0, 0),
            AcquireEligibility::SessionsDisabled
        );
    }

    #[test]
    fn session_acquire_eligible_disabled_when_max_is_negative() {
        assert_eq!(
            session_acquire_eligible(-1, 0),
            AcquireEligibility::SessionsDisabled
        );
    }

    #[test]
    fn session_acquire_eligible_when_under_capacity() {
        assert_eq!(session_acquire_eligible(5, 3), AcquireEligibility::Eligible);
    }

    #[test]
    fn session_acquire_at_capacity_when_equal() {
        assert_eq!(session_acquire_eligible(5, 5), AcquireEligibility::AtCapacity);
    }

    #[test]
    fn session_acquire_at_capacity_when_over() {
        // Should never happen in practice, but must not wrap or panic.
        assert_eq!(session_acquire_eligible(5, 6), AcquireEligibility::AtCapacity);
    }

    #[test]
    fn session_acquire_eligible_negative_in_use_treated_as_zero() {
        assert_eq!(session_acquire_eligible(1, -3), AcquireEligibility::Eligible);
    }

    #[test]
    fn acquire_eligibility_is_eligible_helper() {
        assert!(AcquireEligibility::Eligible.is_eligible());
        assert!(!AcquireEligibility::SessionsDisabled.is_eligible());
        assert!(!AcquireEligibility::AtCapacity.is_eligible());
    }

    // ── broken_session_reason ────────────────────────────────────────────

    #[test]
    fn broken_session_reason_none_when_healthy_and_leased() {
        assert_eq!(broken_session_reason(false, false, false), None);
    }

    #[test]
    fn broken_session_reason_stale_heartbeat_wins_priority() {
        // All three conditions true -- heartbeat loss is reported first.
        assert_eq!(
            broken_session_reason(true, true, true),
            Some(BrokenSessionReason::HostWorkerLostHeartbeat)
        );
    }

    #[test]
    fn broken_session_reason_draining_reported_when_alive_but_draining() {
        assert_eq!(
            broken_session_reason(false, true, false),
            Some(BrokenSessionReason::HostWorkerDraining)
        );
    }

    #[test]
    fn broken_session_reason_lease_expired_reported_when_host_otherwise_fine() {
        assert_eq!(
            broken_session_reason(false, false, true),
            Some(BrokenSessionReason::LeaseExpired)
        );
    }

    #[test]
    fn broken_session_reason_draining_takes_priority_over_lease_expiry() {
        assert_eq!(
            broken_session_reason(false, true, true),
            Some(BrokenSessionReason::HostWorkerDraining)
        );
    }

    #[test]
    fn broken_session_reason_display_strings() {
        assert_eq!(
            BrokenSessionReason::HostWorkerLostHeartbeat.to_string(),
            "host worker lost heartbeat"
        );
        assert_eq!(
            BrokenSessionReason::HostWorkerDraining.to_string(),
            "host worker draining"
        );
        assert_eq!(
            BrokenSessionReason::LeaseExpired.to_string(),
            "session lease expired"
        );
    }

    // ── lease_expired ────────────────────────────────────────────────────

    #[test]
    fn lease_expired_true_when_now_past_expiry() {
        let expires_at = chrono::Utc::now();
        let now = expires_at + chrono::Duration::seconds(1);
        assert!(lease_expired(now, expires_at));
    }

    #[test]
    fn lease_expired_true_at_exact_boundary() {
        let t = chrono::Utc::now();
        assert!(lease_expired(t, t));
    }

    #[test]
    fn lease_expired_false_when_still_within_lease() {
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(60);
        assert!(!lease_expired(chrono::Utc::now(), expires_at));
    }

    // ── acquire_retry_backoff ────────────────────────────────────────────

    #[test]
    fn acquire_retry_backoff_at_zero_fraction_returns_min() {
        assert_eq!(
            acquire_retry_backoff(0.0, Duration::from_millis(50), Duration::from_millis(500)),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn acquire_retry_backoff_at_one_fraction_returns_max() {
        assert_eq!(
            acquire_retry_backoff(1.0, Duration::from_millis(50), Duration::from_millis(500)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn acquire_retry_backoff_midpoint_is_between_bounds() {
        let d = acquire_retry_backoff(0.5, Duration::from_millis(100), Duration::from_millis(300));
        assert!(d >= Duration::from_millis(100) && d <= Duration::from_millis(300));
    }

    #[test]
    fn acquire_retry_backoff_clamps_out_of_range_fraction() {
        let below = acquire_retry_backoff(-1.0, Duration::from_millis(10), Duration::from_millis(20));
        let above = acquire_retry_backoff(2.0, Duration::from_millis(10), Duration::from_millis(20));
        assert_eq!(below, Duration::from_millis(10));
        assert_eq!(above, Duration::from_millis(20));
    }

    #[test]
    fn acquire_retry_backoff_never_panics_when_max_less_than_min() {
        // Defensive: a misconfigured caller passing max < min must not panic
        // or underflow -- it degrades to always returning min.
        let d = acquire_retry_backoff(0.5, Duration::from_millis(500), Duration::from_millis(100));
        assert_eq!(d, Duration::from_millis(500));
    }
}
