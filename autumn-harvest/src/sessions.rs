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

// `Arc`/`Mutex` resolve to `std::sync` under a normal build and to `loom::sync`
// under `RUSTFLAGS="--cfg loom"` so `tests/loom_models.rs` can model-check the
// concurrent session-slot registry. See `crate::loom_sync`.
use crate::loom_sync::{Arc, Mutex};

/// Sticky-lease duration applied to a session member activity's task row
/// (`harvest_task_queue.sticky_timeout`).
///
/// Purely a bookkeeping value to satisfy the `sticky_worker_id IS NULL <=>
/// sticky_until IS NULL` check constraint: the hard-pin claim gate (`AND
/// (session_id IS NULL OR sticky_worker_id = $1)`) never consults
/// `sticky_until` for a session-tagged row, so this never causes a
/// mid-session failover the way an ordinary sticky lease would. Generously
/// long so operator-facing views of `sticky_until` never read as "about to
/// expire" for a legitimately still-open session.
pub const SESSION_MEMBER_STICKY_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

/// Lower/upper bounds for [`acquire_retry_backoff`]'s randomized reschedule
/// delay after a session-acquire task loses the in-process semaphore race.
pub const ACQUIRE_RETRY_BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const ACQUIRE_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);

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
    } else if (if in_use_sessions < 0 {
        0
    } else {
        in_use_sessions
    }) < max_concurrent_sessions
    {
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
    /// The session's owning workflow execution already reached a terminal
    /// state (completed, failed, cancelled, timed out, terminated, or
    /// continued-as-new) without ever calling `Session::complete()` -- e.g.
    /// a member activity error propagated via `?`, or an operator
    /// cancelled/terminated the workflow mid-pipeline. The workflow can
    /// never dispatch another member activity or a release for this
    /// session, so waiting out the full lease would only delay reclaiming
    /// the slot for no benefit.
    OwningWorkflowTerminal,
    /// The session's lease (`expires_at`) elapsed — e.g. a wedged workflow
    /// that never called `Session::complete()`.
    LeaseExpired,
}

impl std::fmt::Display for BrokenSessionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HostWorkerLostHeartbeat => write!(f, "host worker lost heartbeat"),
            Self::HostWorkerDraining => write!(f, "host worker draining"),
            Self::OwningWorkflowTerminal => {
                write!(f, "owning workflow execution reached a terminal state")
            }
            Self::LeaseExpired => write!(f, "session lease expired"),
        }
    }
}

/// Pure decision: should an `ACTIVE` session be marked `BROKEN`?
///
/// Checked in priority order — a dead host is reported before a merely
/// draining one; a terminal owning workflow is reported next (it can never
/// legitimately use or release this session again, regardless of host
/// health or lease); an expired lease is checked last, and only once none
/// of the above apply. Returns `None` when none of the conditions hold.
#[must_use]
#[allow(clippy::fn_params_excessive_bools)]
pub const fn broken_session_reason(
    host_heartbeat_stale: bool,
    host_status_draining_or_stopped: bool,
    owning_workflow_terminal: bool,
    lease_expired: bool,
) -> Option<BrokenSessionReason> {
    if host_heartbeat_stale {
        Some(BrokenSessionReason::HostWorkerLostHeartbeat)
    } else if host_status_draining_or_stopped {
        Some(BrokenSessionReason::HostWorkerDraining)
    } else if owning_workflow_terminal {
        Some(BrokenSessionReason::OwningWorkflowTerminal)
    } else if lease_expired {
        Some(BrokenSessionReason::LeaseExpired)
    } else {
        None
    }
}

/// Pure lease-expiry check: has `now` reached or passed `expires_at`?
#[must_use]
pub fn lease_expired(
    now: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> bool {
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
    let max_ms = u64::try_from(max.as_millis())
        .unwrap_or(u64::MAX)
        .max(min_ms);
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

// ---------------------------------------------------------------------------
// In-process session-slot registry
// ---------------------------------------------------------------------------

/// Per-worker in-process registry of sessions currently hosted by this
/// worker (issue #606).
///
/// A plain `HashSet<SessionId>` guarded by a `Mutex`, not a
/// `tokio::sync::Semaphore` + `OwnedSemaphorePermit` map: a session's
/// acquire and release happen in *different* task-dispatch invocations
/// (potentially different tokio tasks, arbitrarily far apart in time), so
/// there is no async-fn scope to hold an owned permit against. Storing a
/// `HashMap<SessionId, OwnedSemaphorePermit>` directly as a `Worker` field
/// hits the exact `clippy::significant_drop_tightening` trap already
/// documented for `crate::slot_tuner::TunedSlotRuntime` (issue #548) -- but
/// worse, since unlike that type (constructed and consumed within one
/// function), a session registry must live for the worker's entire
/// lifetime as a real struct field, so the lint fires on every single test
/// in the crate that merely constructs a `Worker`. `SessionId` is a plain
/// `Copy` newtype with no custom `Drop`, so a `HashSet<SessionId>` has no
/// significant drop and sidesteps the trap entirely -- while, unlike a bare
/// counter, still tracking *which* sessions are held. That identity is
/// required for [`reconcile_local_sessions`] to correct the leak a bare
/// counter cannot: the broken-session scanner (`enforce_broken_sessions`)
/// runs from a plain DB connection with no reference to any specific
/// worker's registry, so it can never release a slot itself when it
/// reclaims a session hosted by a *different* (possibly still-healthy)
/// worker process. Each worker instead periodically reconciles its own
/// registry against `harvest_sessions` and drops any entry that left
/// `ACTIVE` through any path -- scanner reclaim, or a release whose
/// `record_session_completed` call failed after committing.
pub type SessionSlotRegistry = Arc<Mutex<std::collections::HashSet<crate::types::SessionId>>>;

/// Build a new, empty session-slot registry.
#[must_use]
pub fn new_session_slot_registry() -> SessionSlotRegistry {
    Arc::new(Mutex::new(std::collections::HashSet::new()))
}

/// Attempt to claim `session_id`'s slot, bounded by `max_concurrent_sessions`.
///
/// Returns `true` on success (either newly inserted, or already held by
/// this worker -- idempotent, so a retried acquire dispatch that already
/// won locally on an earlier attempt this process doesn't need to re-claim
/// a slot) -- the caller now holds the slot and must eventually call
/// [`release_session_slot`]. Returns `false` without side effects when
/// `max <= 0` (sessions disabled) or the registry is already at `max` and
/// does not already contain `session_id`.
#[must_use]
pub fn try_acquire_session_slot(
    registry: &SessionSlotRegistry,
    max: i32,
    session_id: crate::types::SessionId,
) -> bool {
    if max <= 0 {
        return false;
    }
    let Ok(max) = usize::try_from(max) else {
        return false;
    };
    let mut held = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if held.contains(&session_id) {
        return true;
    }
    if held.len() >= max {
        return false;
    }
    held.insert(session_id);
    true
}

/// Release a previously claimed session slot.
///
/// Idempotent: releasing a `session_id` this registry doesn't (or no
/// longer) hold is a no-op, so a defensive double-release (e.g. a
/// reconcile sweep racing an in-flight `Session::complete()`) is safe.
pub fn release_session_slot(registry: &SessionSlotRegistry, session_id: crate::types::SessionId) {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&session_id);
}

/// Current number of sessions this worker's registry believes it is
/// hosting -- the live value reported as `harvest_workers.in_use_sessions`
/// on each heartbeat (issue #606 AC8).
#[must_use]
pub fn session_slot_count(registry: &SessionSlotRegistry) -> i32 {
    let held = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    i32::try_from(held.len()).unwrap_or(i32::MAX)
}

/// Reconcile this worker's local session-slot registry against durable state
/// (issue #606).
///
/// Drops any locally-held `session_id` whose `harvest_sessions` row has left
/// `ACTIVE` through a path this worker's own release call never observed --
/// the broken-session scanner reclaiming it (host died/drained/lease-expired,
/// possibly detected by a *different* worker's timeout-checker loop), or a
/// `Session::complete()` dispatch whose `record_session_completed` call
/// errored after the row had already committed COMPLETED on a retry.
///
/// Returns the number of slots actually released.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on failure.
#[cfg(feature = "db")]
pub async fn reconcile_local_sessions(
    conn: &mut diesel_async::AsyncPgConnection,
    registry: &SessionSlotRegistry,
) -> crate::error::HarvestResult<usize> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_sessions::dsl as s;

    let candidate_ids: Vec<uuid::Uuid> = {
        let held = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.iter().map(crate::types::SessionId::as_uuid).collect()
    };
    if candidate_ids.is_empty() {
        return Ok(0);
    }

    let non_active: Vec<uuid::Uuid> = s::harvest_sessions
        .filter(s::id.eq_any(&candidate_ids))
        .filter(s::state.ne("ACTIVE"))
        .select(s::id)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    if non_active.is_empty() {
        return Ok(0);
    }

    let mut held = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut released = 0usize;
    for id in non_active {
        if held.remove(&crate::types::SessionId::from_uuid(id)) {
            released += 1;
        }
    }
    Ok(released)
}

/// Spawn a background task that periodically reconciles this worker's local
/// session-slot registry against `harvest_sessions` (issue #606).
///
/// Releases any slot the registry believes is still held but whose row has
/// left `ACTIVE` -- see [`reconcile_local_sessions`] for why a worker cannot
/// rely solely on its own release paths. Mirrors
/// [`crate::poison_pill::spawn_poison_pill_reclaimer`]'s loop shape. Stops
/// when `cancel` is triggered.
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_session_slot_reconciler(
    pool: crate::worker::DbPool,
    registry: SessionSlotRegistry,
    cancel: tokio_util::sync::CancellationToken,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            match pool.get().await {
                Ok(mut conn) => match reconcile_local_sessions(&mut conn, &registry).await {
                    Ok(released) if released > 0 => {
                        tracing::warn!(
                            released,
                            "reconciled local worker-session registry against harvest_sessions"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "worker-session slot reconcile sweep failed");
                    }
                },
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "failed to acquire DB connection for worker-session slot reconcile"
                    );
                }
            }
            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// DB-gated session persistence
// ---------------------------------------------------------------------------

/// Outcome of [`record_session_acquired`]'s idempotent read-back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionAcquireRecordOutcome {
    /// The session is (still) `ACTIVE`. `host_worker_id` is the durably
    /// recorded host -- `host_worker_id` on a fresh insert, or a *different*
    /// worker's id if this call lost the race to an earlier attempt.
    Active {
        /// The durably-recorded host worker id.
        host_worker_id: String,
    },
    /// The session already left `ACTIVE` (e.g. the broken-session scanner
    /// reclaimed it) before this call resolved -- the crash-then-retry race
    /// described on [`record_session_acquired`]. The caller must not treat
    /// this as a successful acquisition.
    NotActive {
        /// Why the session is no longer usable (its `broken_reason` when
        /// available, else a description of the unexpected state).
        reason: String,
    },
}

/// Record a newly acquired session (issue #606), idempotently.
///
/// Called by the worker's session-acquire interception after it wins the
/// in-process semaphore race. Uses `ON CONFLICT (id) DO NOTHING` and reads
/// the row back rather than a plain insert: a session-acquire task can be
/// claimed and (partially) processed more than once for the same
/// `session_id` -- e.g. the poison-pill reclaimer requeues the task row
/// after its original worker crashes between committing this insert and
/// completing the task -- and a second, honest attempt on a *different*
/// worker must not error out on a duplicate-key violation or silently
/// overwrite the first winner's `host_worker_id`.
///
/// **The read-back also re-checks `state`.** In the crash window above, the
/// broken-session scanner can mark the row `BROKEN` (the original host lost
/// its heartbeat) *before* the poison-pill reclaimer requeues the stuck
/// acquire task -- a retried acquire on a different worker must not resolve
/// the session as successfully acquired against a host it already knows is
/// dead; that would hard-pin every later member activity to an
/// unreachable worker with no further scanner coverage (the scanner's
/// candidate query only ever selects `state = 'ACTIVE'` rows, so a session
/// already `BROKEN` is never revisited). Returns
/// [`SessionAcquireRecordOutcome::NotActive`] in that case so the caller can
/// fail the acquire activity with a typed `SessionBroken` failure instead.
///
/// Callers must compare an [`SessionAcquireRecordOutcome::Active`] host
/// against their own worker id and release any local slot reservation they
/// are not actually entitled to when they differ (see
/// `handle_session_acquire` in `worker.rs`).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on failure.
#[cfg(feature = "db")]
pub async fn record_session_acquired(
    conn: &mut diesel_async::AsyncPgConnection,
    session_id: crate::types::SessionId,
    workflow_exec_id: crate::types::ExecutionId,
    host_worker_id: &str,
    queue_name: &str,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> crate::error::HarvestResult<SessionAcquireRecordOutcome> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_sessions::dsl;

    let row = crate::models::NewHarvestSession {
        id: session_id.as_uuid(),
        workflow_exec_id: workflow_exec_id.as_uuid(),
        host_worker_id,
        queue_name,
        expires_at,
    };
    diesel::insert_into(crate::schema::harvest_sessions::table)
        .values(&row)
        .on_conflict(dsl::id)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    let (recorded_host, state, broken_reason): (String, String, Option<String>) =
        dsl::harvest_sessions
            .find(session_id.as_uuid())
            .select((dsl::host_worker_id, dsl::state, dsl::broken_reason))
            .first(conn)
            .await
            .map_err(crate::error::database_error)?;

    if state == "ACTIVE" {
        Ok(SessionAcquireRecordOutcome::Active {
            host_worker_id: recorded_host,
        })
    } else {
        let reason =
            broken_reason.unwrap_or_else(|| format!("session state is {state}, not ACTIVE"));
        Ok(SessionAcquireRecordOutcome::NotActive { reason })
    }
}

/// Mark a session `COMPLETED` (issue #606).
///
/// Called by the worker's session-release interception. Idempotent-shaped:
/// only an `ACTIVE` row transitions; a session already `BROKEN` (reclaimed
/// by the scanner concurrently with a late release) is left alone.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on failure.
#[cfg(feature = "db")]
pub async fn record_session_completed(
    conn: &mut diesel_async::AsyncPgConnection,
    session_id: crate::types::SessionId,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_sessions::dsl;

    diesel::update(
        dsl::harvest_sessions
            .find(session_id.as_uuid())
            .filter(dsl::state.eq("ACTIVE")),
    )
    .set(crate::models::SessionCompletedUpdate {
        state: "COMPLETED".to_string(),
        completed_at: chrono::Utc::now(),
    })
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Push a session's lease (`expires_at`) forward from now (issue #606).
///
/// Called after each member-activity completion so a long-running, still
/// legitimately active pipeline isn't reclaimed by the broken-session
/// scanner's `expires_at < NOW()` check purely because its member activities
/// individually take longer than one [`SESSION_MEMBER_STICKY_TIMEOUT`]
/// window to finish. Idempotent-shaped: only an `ACTIVE` row is refreshed —
/// a session already `BROKEN`/`COMPLETED` is left alone (there is nothing to
/// extend, and refreshing a `BROKEN` row's lease could otherwise race the
/// scanner into believing a just-reclaimed session is still alive).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on failure.
#[cfg(feature = "db")]
pub async fn refresh_session_lease(
    conn: &mut diesel_async::AsyncPgConnection,
    session_id: crate::types::SessionId,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_sessions::dsl;

    let expires_at = chrono::Utc::now()
        + chrono::Duration::from_std(SESSION_MEMBER_STICKY_TIMEOUT)
            .unwrap_or_else(|_| chrono::Duration::hours(24));
    diesel::update(
        dsl::harvest_sessions
            .find(session_id.as_uuid())
            .filter(dsl::state.eq("ACTIVE")),
    )
    .set(crate::models::SessionLeaseRefresh { expires_at })
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Broken-session scanner
// ---------------------------------------------------------------------------

/// Raw SQL selecting `ACTIVE` sessions whose host may no longer be viable.
///
/// A host is a candidate when it has no `harvest_workers` row at all, a
/// stale heartbeat, a `Draining`/`Stopped` status, or an elapsed lease
/// (`expires_at < NOW()`).
///
/// A broad, best-effort scan -- [`enforce_broken_sessions`] re-verifies each
/// candidate (without a row lock, since `harvest_sessions` carries no
/// meaningful concurrent-writer other than this scanner and
/// `record_session_completed`, both idempotent-shaped via the `state =
/// 'ACTIVE'` guard) before acting, mirroring the poison-pill reclaimer's
/// broad-scan-then-reverify shape (issue #367).
///
/// `$1` is the worker-stale threshold in seconds (BIGINT), matching
/// [`crate::poison_pill::orphaned_running_tasks_query`]'s convention.
#[cfg(feature = "db")]
#[must_use]
pub const fn broken_session_candidates_query() -> &'static str {
    "SELECT s.* FROM harvest_sessions s \
     LEFT JOIN harvest_workers w ON w.worker_id = s.host_worker_id \
     LEFT JOIN harvest_workflow_executions e ON e.id = s.workflow_exec_id \
     WHERE s.state = 'ACTIVE' \
       AND ( \
         w.worker_id IS NULL \
         OR w.last_heartbeat_at <= NOW() - ($1::bigint * INTERVAL '1 second') \
         OR w.status IN ('Draining', 'Stopped') \
         OR s.expires_at < NOW() \
         OR e.state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'TERMINATED', \
                         'CONTINUED_AS_NEW') \
       )"
}

/// Re-resolve why a candidate session's host is broken, re-reading
/// `harvest_workers` fresh (guards against acting on a stale broad-scan
/// snapshot, e.g. the host resurrected its heartbeat between the scan and
/// this check). Returns `None` when the session is no longer actually
/// broken.
#[cfg(feature = "db")]
async fn resolve_broken_reason(
    conn: &mut diesel_async::AsyncPgConnection,
    session: &crate::models::HarvestSession,
    worker_stale_secs: i64,
) -> crate::error::HarvestResult<Option<BrokenSessionReason>> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_workers::dsl as w;
    use crate::schema::harvest_workflow_executions::dsl as e;

    let worker: Option<(chrono::DateTime<chrono::Utc>, String)> = w::harvest_workers
        .filter(w::worker_id.eq(&session.host_worker_id))
        .select((w::last_heartbeat_at, w::status))
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let (host_heartbeat_stale, host_status_draining_or_stopped) = match worker {
        // No worker row at all -- treat exactly like a dead host (a worker
        // can be de-registered/GC'd without ever transitioning to Stopped).
        None => (true, false),
        Some((last_heartbeat_at, status)) => {
            let cutoff = chrono::Utc::now() - chrono::Duration::seconds(worker_stale_secs);
            (
                last_heartbeat_at <= cutoff,
                status == "Draining" || status == "Stopped",
            )
        }
    };

    // The owning workflow may have reached a terminal state without ever
    // calling `Session::complete()` -- a member activity error propagated
    // via `?`, or an operator cancelled/terminated the workflow mid-pipeline
    // (issue #929 review). Such a workflow can never dispatch another
    // member activity or a release for this session, so there is no reason
    // to wait out the full lease before reclaiming the slot.
    let owning_workflow_state: Option<String> = e::harvest_workflow_executions
        .find(session.workflow_exec_id)
        .select(e::state)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    let owning_workflow_terminal = owning_workflow_state
        .as_deref()
        .is_some_and(crate::erase::is_terminal_state);

    let lease_expired_naive = lease_expired(chrono::Utc::now(), session.expires_at);

    // A naive lease-expiry check alone would misclassify a healthy pipeline
    // whose member activity legitimately runs longer than
    // `SESSION_MEMBER_STICKY_TIMEOUT` (24h) as broken, since the lease is
    // currently only refreshed on member *completion*
    // (`refresh_session_lease`, called from `finalize_activity_completion`),
    // never while a member is still executing. A currently-`RUNNING` member
    // task is independent proof the pipeline is still making real progress,
    // so only report `LeaseExpired` when no member task is in flight --
    // this never masks a genuinely dead/draining host, since that check
    // takes priority in `broken_session_reason` regardless of this flag.
    let expired = if lease_expired_naive {
        use crate::schema::harvest_task_queue::dsl as q;
        let has_running_member: bool = diesel::select(diesel::dsl::exists(
            q::harvest_task_queue
                .filter(q::session_id.eq(Some(session.id)))
                .filter(q::state.eq("RUNNING")),
        ))
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;
        !has_running_member
    } else {
        false
    };

    Ok(broken_session_reason(
        host_heartbeat_stale,
        host_status_draining_or_stopped,
        owning_workflow_terminal,
        expired,
    ))
}

/// Mark one session `BROKEN` and fail every `PENDING`/`RUNNING` member
/// activity task pinned to it (issue #606).
///
/// Runs in a single transaction (session row + member task rows always
/// share one shard by construction): re-checks the session is still
/// `ACTIVE` before acting (idempotent against a concurrent
/// `record_session_completed` late-release racing this scan), transitions
/// it to `BROKEN`, then for each affected member task appends an
/// `ActivityFailed { error_type: "SessionBroken", non_retryable: true }`
/// event (reusing the existing event variant -- no new `WorkflowEvent`),
/// fails the task row, and wakes the workflow so it observes the failure on
/// its very next decision cycle. `non_retryable` is required: a
/// hard-pinned member task can never fail over to a different host, so a
/// retryable failure would simply re-pend forever against the same dead
/// host.
///
/// Returns the number of member tasks failed.
#[cfg(feature = "db")]
async fn break_session_and_fail_members(
    conn: &mut diesel_async::AsyncPgConnection,
    session_id: crate::types::SessionId,
    reason: BrokenSessionReason,
    // Issue #1243: `ActivityFailed` carries `details`, a payload-bearing field,
    // so this write encodes through the configured registry like every other.
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<usize> {
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    use crate::error::HarvestError;
    use crate::event::WorkflowEvent;
    use crate::models::TaskQueueItem;

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        use crate::schema::harvest_sessions::dsl as s;
        use crate::schema::harvest_task_queue::dsl as q;

        let still_active: Option<String> = s::harvest_sessions
            .find(session_id.as_uuid())
            .for_update()
            .select(s::state)
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
        if still_active.as_deref() != Some("ACTIVE") {
            return Ok(0);
        }

        diesel::update(s::harvest_sessions.find(session_id.as_uuid()))
            .set(crate::models::SessionBrokenUpdate {
                state: "BROKEN".to_string(),
                broken_at: chrono::Utc::now(),
                broken_reason: reason.to_string(),
            })
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        let member_tasks: Vec<TaskQueueItem> = q::harvest_task_queue
            .filter(q::session_id.eq(Some(session_id.as_uuid())))
            .filter(q::state.eq_any(["PENDING", "RUNNING"]))
            .select(TaskQueueItem::as_select())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

        let mut failed = 0usize;
        for task in member_tasks {
            let Some(exec_uuid) = task.workflow_exec_id else {
                continue;
            };
            let Some(activity_name) = task.activity_name.as_deref() else {
                continue;
            };
            let exec_id: crate::types::ExecutionId = exec_uuid
                .to_string()
                .parse()
                .expect("database UUIDs must round-trip into ExecutionId");

            let history =
                crate::timeout::lock_workflow_execution_and_load_history(conn, exec_id).await?;
            let Some(activity_id) = crate::timeout::pending_activity_id_for_task(
                &history.events,
                &task,
                activity_name,
            )?
            else {
                continue;
            };
            let Some(state) = crate::timeout::task_state_for_update(conn, task.id).await? else {
                continue;
            };
            if state != "PENDING" && state != "RUNNING" {
                continue;
            }

            let attempt = u32::try_from(task.attempt.max(1)).unwrap_or(1);
            let failed_event = WorkflowEvent::ActivityFailed {
                activity_id,
                error: reason.to_string(),
                attempt,
                error_type: crate::failure::ERROR_TYPE_SESSION_BROKEN.to_string(),
                non_retryable: true,
                details: None,
            };
            crate::store::append_events_with_codecs(
                conn,
                exec_id,
                &[failed_event],
                history.next_event_id,
                codecs,
            )
            .await?;
            crate::queue::fail_task(conn, task.id, &reason.to_string()).await?;
            crate::queue::wake_workflow_task(conn, exec_id).await?;
            failed += 1;
        }

        Ok(failed)
    }))
    .await
}

/// Scan for and reclaim `ACTIVE` sessions whose host is no longer viable
/// (issue #606) -- folded into [`crate::timeout::enforce_timeouts_once`], no
/// separate poll loop.
///
/// `worker_stale_secs` mirrors the poison-pill reclaimer's convention: `2 ×
/// worker_heartbeat_interval`, computed once by the caller.
///
/// Returns the number of member activity tasks failed across all sessions
/// reclaimed this sweep (0 when no session was broken).
///
/// # Errors
///
/// Returns the first database error encountered.
#[cfg(feature = "db")]
pub async fn enforce_broken_sessions(
    conn: &mut diesel_async::AsyncPgConnection,
    worker_stale_secs: i64,
    // Issue #1243: forwarded to the member-failure write below.
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<usize> {
    use diesel_async::RunQueryDsl;

    let candidates: Vec<crate::models::HarvestSession> =
        diesel::sql_query(broken_session_candidates_query())
            .bind::<diesel::sql_types::BigInt, _>(worker_stale_secs)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;

    let mut total = 0usize;
    for candidate in candidates {
        let Some(reason) = resolve_broken_reason(conn, &candidate, worker_stale_secs).await? else {
            continue;
        };
        let session_id = crate::types::SessionId::from_uuid(candidate.id);
        total += break_session_and_fail_members(conn, session_id, reason, codecs).await?;
    }
    Ok(total)
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
        assert_eq!(
            session_acquire_eligible(5, 5),
            AcquireEligibility::AtCapacity
        );
    }

    #[test]
    fn session_acquire_at_capacity_when_over() {
        // Should never happen in practice, but must not wrap or panic.
        assert_eq!(
            session_acquire_eligible(5, 6),
            AcquireEligibility::AtCapacity
        );
    }

    #[test]
    fn session_acquire_eligible_negative_in_use_treated_as_zero() {
        assert_eq!(
            session_acquire_eligible(1, -3),
            AcquireEligibility::Eligible
        );
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
        assert_eq!(broken_session_reason(false, false, false, false), None);
    }

    #[test]
    fn broken_session_reason_stale_heartbeat_wins_priority() {
        // All four conditions true -- heartbeat loss is reported first.
        assert_eq!(
            broken_session_reason(true, true, true, true),
            Some(BrokenSessionReason::HostWorkerLostHeartbeat)
        );
    }

    #[test]
    fn broken_session_reason_draining_reported_when_alive_but_draining() {
        assert_eq!(
            broken_session_reason(false, true, false, false),
            Some(BrokenSessionReason::HostWorkerDraining)
        );
    }

    #[test]
    fn broken_session_reason_lease_expired_reported_when_host_otherwise_fine() {
        assert_eq!(
            broken_session_reason(false, false, false, true),
            Some(BrokenSessionReason::LeaseExpired)
        );
    }

    #[test]
    fn broken_session_reason_draining_takes_priority_over_lease_expiry() {
        assert_eq!(
            broken_session_reason(false, true, false, true),
            Some(BrokenSessionReason::HostWorkerDraining)
        );
    }

    #[test]
    fn broken_session_reason_owning_workflow_terminal_reported_when_host_otherwise_fine() {
        assert_eq!(
            broken_session_reason(false, false, true, false),
            Some(BrokenSessionReason::OwningWorkflowTerminal)
        );
    }

    #[test]
    fn broken_session_reason_draining_takes_priority_over_owning_workflow_terminal() {
        assert_eq!(
            broken_session_reason(false, true, true, false),
            Some(BrokenSessionReason::HostWorkerDraining)
        );
    }

    #[test]
    fn broken_session_reason_owning_workflow_terminal_takes_priority_over_lease_expiry() {
        assert_eq!(
            broken_session_reason(false, false, true, true),
            Some(BrokenSessionReason::OwningWorkflowTerminal)
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
            BrokenSessionReason::OwningWorkflowTerminal.to_string(),
            "owning workflow execution reached a terminal state"
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
        let below =
            acquire_retry_backoff(-1.0, Duration::from_millis(10), Duration::from_millis(20));
        let above =
            acquire_retry_backoff(2.0, Duration::from_millis(10), Duration::from_millis(20));
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

    // ── session slot registry ────────────────────────────────────────────

    #[test]
    fn new_session_slot_registry_starts_empty() {
        let registry = new_session_slot_registry();
        assert_eq!(session_slot_count(&registry), 0);
    }

    #[test]
    fn try_acquire_session_slot_fails_when_max_is_zero() {
        let registry = new_session_slot_registry();
        assert!(!try_acquire_session_slot(
            &registry,
            0,
            crate::types::SessionId::new()
        ));
        assert_eq!(session_slot_count(&registry), 0);
    }

    #[test]
    fn try_acquire_session_slot_fails_when_max_is_negative() {
        let registry = new_session_slot_registry();
        assert!(!try_acquire_session_slot(
            &registry,
            -1,
            crate::types::SessionId::new()
        ));
        assert_eq!(session_slot_count(&registry), 0);
    }

    #[test]
    fn try_acquire_session_slot_succeeds_under_capacity_and_increments() {
        let registry = new_session_slot_registry();
        assert!(try_acquire_session_slot(
            &registry,
            2,
            crate::types::SessionId::new()
        ));
        assert_eq!(session_slot_count(&registry), 1);
        assert!(try_acquire_session_slot(
            &registry,
            2,
            crate::types::SessionId::new()
        ));
        assert_eq!(session_slot_count(&registry), 2);
    }

    #[test]
    fn try_acquire_session_slot_fails_at_capacity_without_incrementing() {
        let registry = new_session_slot_registry();
        assert!(try_acquire_session_slot(
            &registry,
            1,
            crate::types::SessionId::new()
        ));
        assert!(!try_acquire_session_slot(
            &registry,
            1,
            crate::types::SessionId::new()
        ));
        assert_eq!(session_slot_count(&registry), 1);
    }

    #[test]
    fn try_acquire_session_slot_is_idempotent_for_the_same_session_id() {
        // Re-acquiring the same id (e.g. a retried dispatch on the same
        // worker) must not double-count against capacity.
        let registry = new_session_slot_registry();
        let id = crate::types::SessionId::new();
        assert!(try_acquire_session_slot(&registry, 1, id));
        assert!(try_acquire_session_slot(&registry, 1, id));
        assert_eq!(session_slot_count(&registry), 1);
    }

    #[test]
    fn release_session_slot_decrements() {
        let registry = new_session_slot_registry();
        let id = crate::types::SessionId::new();
        assert!(try_acquire_session_slot(&registry, 1, id));
        release_session_slot(&registry, id);
        assert_eq!(session_slot_count(&registry), 0);
    }

    #[test]
    fn release_session_slot_of_unknown_id_is_a_no_op() {
        let registry = new_session_slot_registry();
        release_session_slot(&registry, crate::types::SessionId::new());
        release_session_slot(&registry, crate::types::SessionId::new());
        assert_eq!(session_slot_count(&registry), 0);
    }

    #[test]
    fn acquire_then_release_frees_a_slot_for_reacquire() {
        let registry = new_session_slot_registry();
        let first = crate::types::SessionId::new();
        let second = crate::types::SessionId::new();
        assert!(try_acquire_session_slot(&registry, 1, first));
        assert!(!try_acquire_session_slot(&registry, 1, second));
        release_session_slot(&registry, first);
        assert!(try_acquire_session_slot(&registry, 1, second));
    }

    // ── broken_session_candidates_query ──────────────────────────────────

    #[cfg(feature = "db")]
    #[test]
    fn broken_session_candidates_query_matches_active_sessions_with_dead_or_expired_hosts() {
        let sql = broken_session_candidates_query();
        assert!(sql.contains("s.state = 'ACTIVE'"));
        assert!(sql.contains("w.worker_id IS NULL"));
        assert!(sql.contains("w.last_heartbeat_at"));
        assert!(sql.contains("Draining"));
        assert!(sql.contains("Stopped"));
        assert!(sql.contains("s.expires_at < NOW()"));
        assert!(sql.contains("LEFT JOIN harvest_workers"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn broken_session_candidates_query_also_matches_sessions_whose_workflow_is_terminal() {
        let sql = broken_session_candidates_query();
        assert!(sql.contains("LEFT JOIN harvest_workflow_executions"));
        assert!(sql.contains("e.id = s.workflow_exec_id"));
        assert!(sql.contains("e.state IN"));
        assert!(sql.contains("'COMPLETED'"));
        assert!(sql.contains("'CONTINUED_AS_NEW'"));
    }
}
