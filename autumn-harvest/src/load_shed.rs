//! Automatic load shedding driven by backlog age (issue #1794).
//!
//! A [`LoadShedder`] holds one hysteresis state per configured queue. A
//! sampler feeds it the age of the oldest claimable `PENDING` task. The start
//! primitive asks it whether to shed a fresh admission.
//!
//! `docs/operations/load-shedding.md` is the specification. It states the trip
//! and clear conditions and the exemption rules.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{PoisonError, RwLock};
use std::time::{Duration, Instant};

/// The default time between two samples.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// The shortest accepted time between two samples.
pub const MIN_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// The gate ignores a state older than this many sample intervals.
///
/// The gate then admits starts. It fails open, so a dead sampler cannot keep a
/// queue shed.
pub const STALE_AFTER_SAMPLES: u32 = 3;

// ── LoadShedPolicy ────────────────────────────────────────────────────────────

/// The trip and clear thresholds for one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadShedPolicy {
    trip_age: Duration,
    clear_age: Duration,
    retry_after: Duration,
}

/// Why a [`LoadShedPolicy`] is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadShedPolicyError {
    /// `clear_age` is zero. A queue with a backlog never reaches age zero.
    ZeroClearAge,
    /// `clear_age` is not below `trip_age`, so there is no hysteresis band.
    ClearNotBelowTrip {
        /// The rejected trip threshold.
        trip_age: Duration,
        /// The rejected clear threshold.
        clear_age: Duration,
    },
    /// `retry_after` is zero.
    ZeroRetryAfter,
}

impl fmt::Display for LoadShedPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroClearAge => write!(f, "load shed clear_age must be greater than zero"),
            Self::ClearNotBelowTrip {
                trip_age,
                clear_age,
            } => write!(
                f,
                "load shed clear_age ({clear_age:?}) must be less than trip_age ({trip_age:?})"
            ),
            Self::ZeroRetryAfter => write!(f, "load shed retry_after must be greater than zero"),
        }
    }
}

impl std::error::Error for LoadShedPolicyError {}

impl LoadShedPolicy {
    /// Make a policy.
    ///
    /// A queue trips at `age >= trip_age` and clears at `age <= clear_age`.
    /// A shed caller gets `retry_after` in the `Retry-After` header.
    ///
    /// # Errors
    ///
    /// Returns [`LoadShedPolicyError`] unless `0 < clear_age < trip_age` and
    /// `retry_after > 0`.
    pub fn new(
        trip_age: Duration,
        clear_age: Duration,
        retry_after: Duration,
    ) -> Result<Self, LoadShedPolicyError> {
        if clear_age.is_zero() {
            return Err(LoadShedPolicyError::ZeroClearAge);
        }
        if clear_age >= trip_age {
            return Err(LoadShedPolicyError::ClearNotBelowTrip {
                trip_age,
                clear_age,
            });
        }
        if retry_after.is_zero() {
            return Err(LoadShedPolicyError::ZeroRetryAfter);
        }
        Ok(Self {
            trip_age,
            clear_age,
            retry_after,
        })
    }

    /// The age at which the queue trips.
    #[must_use]
    pub const fn trip_age(&self) -> Duration {
        self.trip_age
    }

    /// The age at which a shedding queue clears.
    #[must_use]
    pub const fn clear_age(&self) -> Duration {
        self.clear_age
    }

    /// The configured retry delay.
    #[must_use]
    pub const fn retry_after(&self) -> Duration {
        self.retry_after
    }

    /// The `Retry-After` value in whole seconds, rounded up, at least 1.
    #[must_use]
    pub fn retry_after_secs(&self) -> u64 {
        let whole = self.retry_after.as_secs();
        let rounded = if self.retry_after.subsec_nanos() > 0 {
            whole.saturating_add(1)
        } else {
            whole
        };
        rounded.max(1)
    }

    /// The next state after one sample of `age_secs`.
    ///
    /// This is the hysteresis rule. Between the two thresholds the state does
    /// not change.
    #[must_use]
    pub fn next_shedding(&self, shedding: bool, age_secs: f64) -> bool {
        if shedding {
            age_secs > self.clear_age.as_secs_f64()
        } else {
            age_secs >= self.trip_age.as_secs_f64()
        }
    }
}

// ── LoadShedConfig ────────────────────────────────────────────────────────────

/// The per-queue policies and the sample interval.
///
/// An empty config turns load shedding off. No sampler runs then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadShedConfig {
    sample_interval: Duration,
    policies: BTreeMap<String, LoadShedPolicy>,
}

impl Default for LoadShedConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadShedConfig {
    /// An empty config with [`DEFAULT_SAMPLE_INTERVAL`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            policies: BTreeMap::new(),
        }
    }

    /// Set the sample interval.
    ///
    /// The setter raises a value below [`MIN_SAMPLE_INTERVAL`] to that minimum.
    /// Each sample runs one query per shard pool.
    #[must_use]
    pub fn with_sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval.max(MIN_SAMPLE_INTERVAL);
        self
    }

    /// Add or replace the policy for `queue`.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>, policy: LoadShedPolicy) -> Self {
        self.policies.insert(queue.into(), policy);
        self
    }

    /// Whether at least one queue has a policy.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.policies.is_empty()
    }

    /// The time between two samples.
    #[must_use]
    pub const fn sample_interval(&self) -> Duration {
        self.sample_interval
    }

    /// The age after which the gate ignores a state.
    #[must_use]
    pub fn stale_after(&self) -> Duration {
        self.sample_interval
            .checked_mul(STALE_AFTER_SAMPLES)
            .unwrap_or(Duration::MAX)
    }

    /// The policy for `queue`, if any.
    #[must_use]
    pub fn policy(&self, queue: &str) -> Option<&LoadShedPolicy> {
        self.policies.get(queue)
    }

    /// The configured queue names, in order.
    #[must_use]
    pub fn queues(&self) -> Vec<String> {
        self.policies.keys().cloned().collect()
    }
}

// ── LoadShedder ───────────────────────────────────────────────────────────────

/// A state change that one sample caused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShedTransition {
    /// The queue started to shed.
    Tripped,
    /// The queue stopped shedding.
    Cleared,
}

/// The reason to shed one start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShedDecision {
    /// The shed queue.
    pub queue: String,
    /// The last sampled age of the oldest claimable task, in whole seconds.
    pub oldest_pending_age_secs: u64,
    /// The `Retry-After` value in whole seconds.
    pub retry_after_secs: u64,
}

#[derive(Debug, Clone, Copy)]
struct QueueState {
    shedding: bool,
    age_secs: f64,
    observed_at: Instant,
}

#[derive(Debug, Default)]
struct Inner {
    config: LoadShedConfig,
    states: HashMap<String, QueueState>,
}

/// The per-queue shed state of one process.
///
/// The process-global [`crate::admission_gate::AdmissionGateCache`] owns one.
/// Each replica samples the same database. A replica that starts during an
/// incident admits starts until its own samples reach `trip_age`.
#[derive(Debug, Default)]
pub struct LoadShedder {
    inner: RwLock<Inner>,
}

impl LoadShedder {
    /// A shedder with no policy. It never sheds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the config and forget every queue state.
    pub fn configure(&self, config: LoadShedConfig) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        inner.config = config;
        inner.states.clear();
    }

    /// A copy of the current config.
    #[must_use]
    pub fn config(&self) -> LoadShedConfig {
        self.inner
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .config
            .clone()
    }

    /// Record one sample for `queue` taken at `now`.
    ///
    /// Returns the transition the sample caused, if any. The shedder ignores a
    /// queue with no policy.
    ///
    /// A stale shedding state counts as admitting, because the gate failed open.
    /// The next sample must reach `trip_age` to shed again, and that is a new
    /// trip. A sample below `trip_age` reports the clear that closes the trip.
    pub fn observe(&self, queue: &str, age_secs: f64, now: Instant) -> Option<ShedTransition> {
        let (recorded, effective, shedding) = {
            let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
            let policy = *inner.config.policy(queue)?;
            let stale_after = inner.config.stale_after();
            let prior = inner.states.get(queue).copied();
            let recorded = prior.is_some_and(|p| p.shedding);
            let effective = prior.is_some_and(|p| {
                p.shedding && now.saturating_duration_since(p.observed_at) <= stale_after
            });
            let shedding = policy.next_shedding(effective, age_secs);
            inner.states.insert(
                queue.to_owned(),
                QueueState {
                    shedding,
                    age_secs,
                    observed_at: now,
                },
            );
            drop(inner);
            (recorded, effective, shedding)
        };
        match (recorded, effective, shedding) {
            (_, false, true) => Some(ShedTransition::Tripped),
            (true, _, false) => Some(ShedTransition::Cleared),
            _ => None,
        }
    }

    /// Whether `queue` sheds at `now`.
    ///
    /// Returns `None` for a queue with no policy, an admitting queue, or a
    /// state older than [`LoadShedConfig::stale_after`].
    #[must_use]
    pub fn check(&self, queue: &str, now: Instant) -> Option<ShedDecision> {
        let (policy, state, stale_after) = {
            let inner = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            let policy = *inner.config.policy(queue)?;
            let state = *inner.states.get(queue)?;
            let stale_after = inner.config.stale_after();
            drop(inner);
            (policy, state, stale_after)
        };
        let fresh = now.saturating_duration_since(state.observed_at) <= stale_after;
        (state.shedding && fresh).then(|| ShedDecision {
            queue: queue.to_owned(),
            oldest_pending_age_secs: whole_secs(state.age_secs),
            retry_after_secs: policy.retry_after_secs(),
        })
    }
}

// ── Sampler ───────────────────────────────────────────────────────────────────

/// Take one sample for every configured queue and apply it to `shedder`.
///
/// The sample reads [`crate::queue::oldest_pending_ages`] on every pool and
/// keeps the maximum age per queue. A configured queue with no claimable task
/// has age 0. Each trip or clear writes one audit row to `audit_pool`.
///
/// Returns `false` when a pool read fails. No state changes then, so a partial
/// read cannot clear a shedding queue. The active gauge is still written, so
/// it shows 0 once a stale state fails open.
#[cfg(feature = "db")]
pub async fn sample_once(
    shedder: &LoadShedder,
    pools: &[crate::worker::DbPool],
    audit_pool: &crate::worker::DbPool,
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
    circuit_breaker_activities: &[String],
) -> bool {
    let config = shedder.config();
    if !config.is_enabled() {
        return true;
    }
    let queues = config.queues();
    let Some(ages) = read_ages(pools, &queues, circuit_breaker_activities).await else {
        record_active(shedder, &queues, metrics, Instant::now());
        return false;
    };

    let now = Instant::now();
    for queue in &queues {
        let age_secs = ages.get(queue).copied().unwrap_or(0.0);
        if let Some(transition) = shedder.observe(queue, age_secs, now) {
            record_transition(audit_pool, queue, transition, age_secs).await;
        }
    }
    record_active(shedder, &queues, metrics, now);
    true
}

/// Read the maximum oldest-pending age per queue across `pools`.
///
/// The pools are read concurrently, so one slow shard sets the sample time.
/// Returns `None` when any pool read fails.
#[cfg(feature = "db")]
async fn read_ages(
    pools: &[crate::worker::DbPool],
    queues: &[String],
    circuit_breaker_activities: &[String],
) -> Option<HashMap<String, f64>> {
    let reads = pools
        .iter()
        .map(|pool| read_pool_ages(pool, queues, circuit_breaker_activities));
    let mut ages: HashMap<String, f64> = HashMap::new();
    for rows in futures::future::join_all(reads).await {
        for (queue, age_secs) in rows? {
            let slot = ages.entry(queue).or_insert(0.0);
            *slot = slot.max(age_secs);
        }
    }
    Some(ages)
}

/// Read the oldest-pending age per queue on one pool.
#[cfg(feature = "db")]
async fn read_pool_ages(
    pool: &crate::worker::DbPool,
    queues: &[String],
    circuit_breaker_activities: &[String],
) -> Option<Vec<(String, f64)>> {
    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::warn!(error = %error, "load shed sample could not get a connection");
            return None;
        }
    };
    match crate::queue::oldest_pending_ages(&mut conn, queues, circuit_breaker_activities).await {
        Ok(rows) => Some(rows),
        Err(error) => {
            tracing::warn!(error = %error, "load shed sample query failed");
            None
        }
    }
}

/// Write the active gauge for every configured queue.
#[cfg(feature = "db")]
fn record_active(
    shedder: &LoadShedder,
    queues: &[String],
    metrics: Option<&dyn crate::telemetry::MetricsRecorder>,
    now: Instant,
) {
    if let Some(m) = metrics {
        for queue in queues {
            m.record_load_shed_active(queue, shedder.check(queue, now).is_some());
        }
    }
}

/// Log one transition and write its audit row.
///
/// The audit write is best effort. A failed write logs a warning and does not
/// undo the transition.
#[cfg(feature = "db")]
async fn record_transition(
    audit_pool: &crate::worker::DbPool,
    queue: &str,
    transition: ShedTransition,
    age_secs: f64,
) {
    let age = whole_secs(age_secs);
    let operation = match transition {
        ShedTransition::Tripped => {
            tracing::warn!(queue, oldest_pending_age_secs = age, "load shed tripped");
            crate::audit::OP_LOAD_SHED_TRIP
        }
        ShedTransition::Cleared => {
            tracing::info!(queue, oldest_pending_age_secs = age, "load shed cleared");
            crate::audit::OP_LOAD_SHED_CLEAR
        }
    };
    let summary = format!("oldest pending age {age}s");
    let record = crate::models::NewAuditRecord {
        actor: AUDIT_ACTOR,
        operation,
        target_type: crate::audit::TARGET_QUEUE,
        target_id: Some(queue),
        route_or_command: AUDIT_ROUTE,
        request_id: None,
        idempotency_key: None,
        status: crate::audit::STATUS_SUCCEEDED,
        error_summary: Some(&summary),
        shard_id: None,
        // The table accepts only `api`, `cli` and `ui`. The sampler runs in
        // the API process, and the actor and route mark it as automatic.
        source: crate::audit::SOURCE_API,
    };
    let result = match audit_pool.get().await {
        Ok(mut conn) => crate::audit::insert_audit(&mut conn, &record)
            .await
            .map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = result {
        tracing::warn!(queue, operation, error = %error, "load shed audit write failed");
    }
}

/// The audit actor of a trip or clear row.
#[cfg(feature = "db")]
const AUDIT_ACTOR: &str = "system";

/// The audit route of a trip or clear row.
#[cfg(feature = "db")]
const AUDIT_ROUTE: &str = "background.load_shed_sampler";

/// `age_secs` as whole seconds, rounded down. Negative and NaN become 0.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
const fn whole_secs(age_secs: f64) -> u64 {
    // The `as` cast saturates, and NaN becomes 0.
    age_secs.max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn policy() -> LoadShedPolicy {
        LoadShedPolicy::new(secs(60), secs(10), secs(7)).expect("valid policy")
    }

    fn shedder() -> LoadShedder {
        let shedder = LoadShedder::new();
        shedder.configure(LoadShedConfig::new().queue("q", policy()));
        shedder
    }

    #[test]
    fn policy_rejects_clear_not_below_trip() {
        assert_eq!(
            LoadShedPolicy::new(secs(10), secs(10), secs(1)),
            Err(LoadShedPolicyError::ClearNotBelowTrip {
                trip_age: secs(10),
                clear_age: secs(10),
            })
        );
        assert!(LoadShedPolicy::new(secs(10), secs(20), secs(1)).is_err());
    }

    #[test]
    fn policy_rejects_zero_values() {
        assert_eq!(
            LoadShedPolicy::new(secs(10), Duration::ZERO, secs(1)),
            Err(LoadShedPolicyError::ZeroClearAge)
        );
        assert_eq!(
            LoadShedPolicy::new(secs(10), secs(5), Duration::ZERO),
            Err(LoadShedPolicyError::ZeroRetryAfter)
        );
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        let p = LoadShedPolicy::new(secs(10), secs(5), Duration::from_millis(1_200)).unwrap();
        assert_eq!(p.retry_after_secs(), 2);
        let p = LoadShedPolicy::new(secs(10), secs(5), Duration::from_millis(1)).unwrap();
        assert_eq!(p.retry_after_secs(), 1);
        assert_eq!(policy().retry_after_secs(), 7);
    }

    #[test]
    fn next_shedding_applies_hysteresis() {
        let p = policy();
        // Admitting: trips at the trip threshold, not below it.
        assert!(!p.next_shedding(false, 59.9));
        assert!(p.next_shedding(false, 60.0));
        // Shedding: holds inside the band, clears at the clear threshold.
        assert!(p.next_shedding(true, 30.0));
        assert!(p.next_shedding(true, 10.1));
        assert!(!p.next_shedding(true, 10.0));
        // Admitting inside the band stays admitting.
        assert!(!p.next_shedding(false, 30.0));
    }

    #[test]
    fn observe_reports_trip_and_clear_once() {
        let s = shedder();
        let t0 = Instant::now();
        assert_eq!(s.observe("q", 5.0, t0), None);
        assert_eq!(s.observe("q", 61.0, t0), Some(ShedTransition::Tripped));
        assert_eq!(s.observe("q", 90.0, t0), None);
        assert_eq!(s.observe("q", 30.0, t0), None);
        assert_eq!(s.observe("q", 2.0, t0), Some(ShedTransition::Cleared));
        assert_eq!(s.observe("q", 2.0, t0), None);
    }

    #[test]
    fn check_sheds_only_a_tripped_queue() {
        let s = shedder();
        let t0 = Instant::now();
        assert_eq!(s.check("q", t0), None, "no sample yet");
        s.observe("q", 61.4, t0);
        assert_eq!(
            s.check("q", t0),
            Some(ShedDecision {
                queue: "q".to_owned(),
                oldest_pending_age_secs: 61,
                retry_after_secs: 7,
            })
        );
        s.observe("q", 30.0, t0);
        assert!(s.check("q", t0).is_some(), "inside the band it holds");
        s.observe("q", 0.0, t0);
        assert_eq!(s.check("q", t0), None, "cleared");
    }

    #[test]
    fn unconfigured_queue_is_never_shed() {
        let s = shedder();
        let t0 = Instant::now();
        assert_eq!(s.observe("other", 1_000.0, t0), None);
        assert_eq!(s.check("other", t0), None);
    }

    #[test]
    fn stale_state_fails_open() {
        let s = shedder();
        let t0 = Instant::now();
        s.observe("q", 61.0, t0);
        let stale_after = s.config().stale_after();
        assert!(
            s.check("q", t0 + stale_after).is_some(),
            "at the bound it holds"
        );
        assert_eq!(
            s.check("q", t0 + stale_after + Duration::from_millis(1)),
            None,
            "past the bound it admits"
        );
    }

    #[test]
    fn stale_shedding_state_needs_a_new_trip() {
        let s = shedder();
        let t0 = Instant::now();
        s.observe("q", 61.0, t0);
        let late = t0 + s.config().stale_after() + Duration::from_secs(1);
        // Inside the band after a stale gap: the gate stays open and closes the trip.
        assert_eq!(s.observe("q", 30.0, late), Some(ShedTransition::Cleared));
        assert_eq!(s.check("q", late), None);
        // A later trip is a new trip.
        assert_eq!(s.observe("q", 61.0, late), Some(ShedTransition::Tripped));
    }

    #[test]
    fn stale_shedding_state_over_trip_age_trips_again() {
        let s = shedder();
        let t0 = Instant::now();
        s.observe("q", 61.0, t0);
        let late = t0 + s.config().stale_after() + Duration::from_secs(1);
        assert_eq!(s.observe("q", 90.0, late), Some(ShedTransition::Tripped));
        assert!(s.check("q", late).is_some());
    }

    #[test]
    fn stale_after_saturates() {
        let c = LoadShedConfig::new().with_sample_interval(Duration::MAX);
        assert_eq!(c.stale_after(), Duration::MAX);
    }

    #[test]
    fn nan_age_never_trips() {
        let s = shedder();
        assert_eq!(s.observe("q", f64::NAN, Instant::now()), None);
        assert_eq!(whole_secs(f64::NAN), 0);
        assert_eq!(whole_secs(-3.0), 0);
    }

    #[test]
    fn configure_resets_state() {
        let s = shedder();
        let t0 = Instant::now();
        s.observe("q", 61.0, t0);
        s.configure(LoadShedConfig::new().queue("q", policy()));
        assert_eq!(s.check("q", t0), None);
    }

    #[test]
    fn config_reports_enabled_queues() {
        assert!(!LoadShedConfig::new().is_enabled());
        let c = LoadShedConfig::new()
            .with_sample_interval(Duration::from_millis(10))
            .queue("b", policy())
            .queue("a", policy());
        assert!(c.is_enabled());
        assert_eq!(c.queues(), vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(c.sample_interval(), MIN_SAMPLE_INTERVAL);
        assert_eq!(c.stale_after(), MIN_SAMPLE_INTERVAL * STALE_AFTER_SAMPLES);
    }
}
