//! One active background scanner per shard (issue #1795).
//!
//! Without election, every replica runs every per-shard scanner on every
//! tick. Scan load then grows with fleet size. Adding workers to clear a
//! backlog adds database load in proportion, which can feed the overload it
//! was meant to fix.
//!
//! This module gives a scanner three controls:
//!
//! - **A lease.** One row per `(shard_id, scanner)` in
//!   `harvest_scanner_leases` names the replica that runs the scanner now.
//!   The holder renews the row on each tick. Other replicas stand by. When
//!   the holder stops renewing, any replica takes the row after
//!   `lease_until`. A graceful stop expires the row at once.
//! - **Jitter.** Each sleep is the interval times a random factor in
//!   `[1 - jitter, 1 + jitter]`. The mean stays at the interval, so the
//!   default enforcement latency does not change. Replica ticks stop lining
//!   up.
//! - **A role metric.** `harvest.scanner.pass` counts each tick that reaches
//!   the database, by role. An operator can see which replica leads.
//!
//! # The lease is a load control, not a fence
//!
//! Two replicas can both run a pass for a short time. For example, a slow
//! pass outlives its lease and a standby takes over. That is safe, because
//! every scanner that uses this lease must already stay correct with many
//! concurrent runners. Before issue #1795, every replica ran every pass, and
//! that was correct. The lease only removes the duplicate work.
//!
//! # Fail open
//!
//! When the lease query fails, the replica runs the pass anyway. A missing
//! table or a lost grant must not stop timeout enforcement fleet-wide. The
//! cost is duplicate scan load until the fault clears. This matches the
//! behavior before issue #1795.

use std::time::Duration;

/// How long a scanner lease lasts without renewal by default.
///
/// With [`MIN_LEASE_TICKS`], this sets the failover bound when a holder dies
/// without a graceful stop.
pub const DEFAULT_SCANNER_LEASE_TTL: Duration = Duration::from_secs(10);

/// Longest lease TTL a scanner accepts.
///
/// A dead holder blocks its scanner until the lease ends. A large TTL then
/// stops enforcement on that shard for that long.
pub const MAX_SCANNER_LEASE_TTL: Duration = Duration::from_secs(300);

/// Default random spread of each scanner sleep, as a fraction of the interval.
pub const DEFAULT_SCANNER_JITTER: f64 = 0.2;

/// Largest jitter fraction a scanner accepts.
///
/// A fraction of 1 or more could give a zero sleep, which busy-spins.
pub const MAX_SCANNER_JITTER: f64 = 0.9;

/// The fewest scanner ticks one lease must cover.
///
/// A TTL shorter than one tick expires between renewals, and leadership then
/// moves on every tick. Three ticks leave room for one slow pass and one
/// late wakeup.
pub const MIN_LEASE_TICKS: u32 = 3;

/// Shortest scanner interval a loop accepts.
///
/// A zero interval busy-spins. It also bounds the connection checkout to
/// zero, so every tick times out and skips the pass, yet still looks alive.
pub const MIN_SCANNER_INTERVAL: Duration = Duration::from_millis(10);

/// Longest scanner interval a loop accepts.
///
/// The lease TTL must cover [`MIN_LEASE_TICKS`] of the longest sleeps, and it
/// is capped at one day. Four hours at the largest jitter needs 22.8 hours.
/// A longer interval would let a live leader wake after its lease ends.
pub const MAX_SCANNER_INTERVAL: Duration = Duration::from_secs(4 * 3600);

/// `interval`, kept within [`MIN_SCANNER_INTERVAL`] and
/// [`MAX_SCANNER_INTERVAL`].
#[must_use]
pub fn scanner_interval(interval: Duration) -> Duration {
    interval.clamp(MIN_SCANNER_INTERVAL, MAX_SCANNER_INTERVAL)
}

/// Longest time a graceful stop spends on releasing its lease.
///
/// The release is best effort. A stop that runs out of time leaves the lease
/// to expire after its TTL.
pub const LEASE_RELEASE_BOUND: Duration = Duration::from_secs(1);

/// Failed passes in a row after which a leader gives up its lease.
///
/// A pass can fail on one replica alone, for example on a codec only that
/// replica lacks. The leader then stands by for one TTL, and another
/// replica can take over.
pub const ABDICATE_AFTER_FAILED_PASSES: u32 = 3;

/// Upper bound on any lease TTL, the floor included.
///
/// The TTL goes into a Postgres `make_interval`. An unbounded value can
/// overflow the timestamp, and every tick then fails open.
const LEASE_TTL_HARD_CAP: Duration = Duration::from_secs(86_400);

/// How replicas share one per-shard background scanner.
#[derive(Debug, Clone, PartialEq)]
pub struct ScannerCoordination {
    /// The lease holder id, unique per worker. `None` turns election off.
    pub holder: Option<String>,
    /// How long a lease lasts without renewal.
    pub lease_ttl: Duration,
    /// Random spread of each sleep, as a fraction of the interval.
    pub jitter: f64,
}

impl ScannerCoordination {
    /// No election and no jitter. Every replica runs every pass on a fixed
    /// cadence. This is the behavior before issue #1795.
    #[must_use]
    pub const fn unelected() -> Self {
        Self {
            holder: None,
            lease_ttl: DEFAULT_SCANNER_LEASE_TTL,
            jitter: 0.0,
        }
    }

    /// Election under `holder`, with the default TTL and jitter.
    #[must_use]
    pub fn elected(holder: impl Into<String>) -> Self {
        Self {
            holder: Some(holder.into()),
            lease_ttl: DEFAULT_SCANNER_LEASE_TTL,
            jitter: DEFAULT_SCANNER_JITTER,
        }
    }
}

impl Default for ScannerCoordination {
    fn default() -> Self {
        Self::unelected()
    }
}

/// Operator settings for the worker's per-shard scanners (issue #1795).
///
/// Set via [`crate::builder::WorkerConfig::with_scanner_config`].
#[derive(Debug, Clone, PartialEq)]
pub struct ScannerConfig {
    /// Elect one replica per shard to run each scanner. Defaults to `true`.
    ///
    /// Set `false` to make every replica run every pass, as before #1795.
    pub elect: bool,
    /// How long a lease lasts without renewal. Defaults to
    /// [`DEFAULT_SCANNER_LEASE_TTL`]. Capped at [`MAX_SCANNER_LEASE_TTL`],
    /// then raised to at least [`MIN_LEASE_TICKS`] times the longest sleep.
    pub lease_ttl: Duration,
    /// Random spread of each sleep, as a fraction of the interval. Defaults
    /// to [`DEFAULT_SCANNER_JITTER`]. Clamped to `[0, MAX_SCANNER_JITTER]`.
    pub jitter: f64,
    /// Mean time between timeout-checker ticks. `None`, the default, uses the
    /// worker poll interval (500 ms by default). At least
    /// [`MIN_SCANNER_INTERVAL`]. With `elect`, at most [`MAX_SCANNER_INTERVAL`].
    pub timeout_interval: Option<Duration>,
    /// Most rows per timeout reason that one timeout pass enforces. Defaults
    /// to [`DEFAULT_TIMEOUT_SCAN_BATCH_SIZE`]. Kept within 1 and 100,000.
    pub timeout_batch_size: u32,
}

/// Default cap on the rows per timeout reason that one checker pass enforces.
pub const DEFAULT_TIMEOUT_SCAN_BATCH_SIZE: u32 = 500;

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            elect: true,
            lease_ttl: DEFAULT_SCANNER_LEASE_TTL,
            jitter: DEFAULT_SCANNER_JITTER,
            timeout_interval: None,
            timeout_batch_size: DEFAULT_TIMEOUT_SCAN_BATCH_SIZE,
        }
    }
}

impl ScannerConfig {
    /// The coordination for one worker, with `holder` as its lease id.
    #[must_use]
    pub fn coordination(&self, holder: impl Into<String>) -> ScannerCoordination {
        ScannerCoordination {
            holder: self.elect.then(|| holder.into()),
            lease_ttl: self.lease_ttl,
            jitter: self.jitter,
        }
    }
}

/// What a scanner tick did, for the `harvest.scanner.pass` role label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannerRole {
    /// This replica holds the lease and ran the pass.
    Leader,
    /// Another replica holds the lease. This replica skipped the pass.
    Standby,
    /// Election is off. This replica ran the pass.
    Unelected,
    /// The lease query failed. This replica ran the pass anyway.
    FailOpen,
}

/// Failed leader passes in a row (issue #1795).
///
/// The count covers consecutive leader passes only. A tick in any other role,
/// or without a connection, ends the run. The lease may have moved away and
/// back. See
/// [`ABDICATE_AFTER_FAILED_PASSES`].
#[cfg(feature = "db")]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaderFailures(u32);

#[cfg(feature = "db")]
impl LeaderFailures {
    /// Records one tick in `role`. `failed` says whether the tick's pass
    /// failed. Returns `true` when the leader must give up its lease.
    pub(crate) fn record(&mut self, role: ScannerRole, failed: bool) -> bool {
        if role != ScannerRole::Leader || !failed {
            self.0 = 0;
            return false;
        }
        self.0 += 1;
        if self.0 >= ABDICATE_AFTER_FAILED_PASSES {
            self.0 = 0;
            return true;
        }
        false
    }
}

impl ScannerRole {
    /// The bounded metric label for this role.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Standby => "standby",
            Self::Unelected => "unelected",
            Self::FailOpen => "fail_open",
        }
    }

    /// `true` when a tick in this role runs the scanner's pass.
    #[must_use]
    pub const fn runs_pass(self) -> bool {
        !matches!(self, Self::Standby)
    }
}

/// Clamps `jitter` to `[0, MAX_SCANNER_JITTER]`. A NaN, infinite or
/// negative value gives 0.
#[must_use]
pub fn clamp_jitter(jitter: f64) -> f64 {
    if jitter.is_finite() && jitter > 0.0 {
        jitter.min(MAX_SCANNER_JITTER)
    } else {
        0.0
    }
}

/// `duration` times `factor`, saturating at [`Duration::MAX`].
fn scale(duration: Duration, factor: f64) -> Duration {
    Duration::try_from_secs_f64(duration.as_secs_f64() * factor).unwrap_or(Duration::MAX)
}

/// The longest sleep [`jittered_interval`] can return.
#[must_use]
pub fn max_jittered_interval(interval: Duration, jitter: f64) -> Duration {
    scale(interval, 1.0 + clamp_jitter(jitter))
}

/// One scanner sleep: `interval` times a factor in `[1 - j, 1 + j]`.
///
/// `unit` is a random draw in `[0, 1]`. The caller passes it, so this stays a
/// pure function a test can pin.
#[must_use]
pub fn jittered_interval(interval: Duration, jitter: f64, unit: f64) -> Duration {
    let jitter = clamp_jitter(jitter);
    let unit = if unit.is_finite() {
        unit.clamp(0.0, 1.0)
    } else {
        0.5
    };
    scale(interval, 2.0f64.mul_add(jitter * unit, 1.0 - jitter))
}

/// The TTL a lease actually uses.
///
/// `ttl` is capped at [`MAX_SCANNER_LEASE_TTL`]. It is then raised to at
/// least [`MIN_LEASE_TICKS`] times the longest sleep, so a live holder
/// renews before its lease ends. The floor wins over the cap.
#[must_use]
pub fn effective_lease_ttl(ttl: Duration, interval: Duration, jitter: f64) -> Duration {
    let floor = scale(
        max_jittered_interval(interval, jitter),
        f64::from(MIN_LEASE_TICKS),
    );
    ttl.min(MAX_SCANNER_LEASE_TTL)
        .max(floor)
        .min(LEASE_TTL_HARD_CAP)
}

/// The `scanner` column of a checker's lease row.
///
/// A checker that scans exactly its lease shard uses the bare scanner name.
/// Any other scope adds its sorted shard ids, for example `timeout:1,2`. A
/// long list becomes `timeout:sha256:<hex>`, so the key stays short. An
/// empty scope counts as shard 0, because the checker then scans shard 0. Two
/// checkers then share a lease only when they scan the same shards. This
/// matters when workers share one pool but have different shard assignments.
/// A shared lease there would leave the standbys' shards unscanned.
#[must_use]
pub fn lease_scanner_key(
    scanner: crate::scanner_health::Scanner,
    lease_shard: crate::types::ShardId,
    scope: &[crate::types::ShardId],
) -> String {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;

    let mut ids: Vec<i32> = scope.iter().map(|s| s.as_i32()).collect();
    // An empty scope scans shard 0, so it is keyed as shard 0.
    if ids.is_empty() {
        ids.push(0);
    }
    ids.sort_unstable();
    ids.dedup();
    if ids == [lease_shard.as_i32()] {
        return scanner.as_str().to_owned();
    }
    let ids: Vec<String> = ids.iter().map(i32::to_string).collect();
    let ids = ids.join(",");
    if ids.len() <= MAX_READABLE_SCOPE {
        return format!("{}:{ids}", scanner.as_str());
    }
    // A long list could pass the index row limit, and then every lease
    // insert fails. A digest keeps the key short and still one per scope.
    let digest = Sha256::digest(ids.as_bytes());
    let mut key = format!("{}:sha256:", scanner.as_str());
    for byte in digest {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

/// Longest shard list that a lease key spells out (issue #1795).
///
/// A longer list is replaced by its SHA-256 digest.
const MAX_READABLE_SCOPE: usize = 64;

#[cfg(feature = "db")]
pub use db::ScannerLease;

#[cfg(feature = "db")]
mod db {
    use std::time::Duration;

    use diesel::QueryableByName;
    use diesel::sql_types::{BigInt, Double, Integer, Text};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::types::ShardId;

    /// Take the lease, or renew it when this holder has it.
    ///
    /// The `ON CONFLICT ... WHERE` admits the update only for the same
    /// holder or an expired lease. Postgres serializes upserts on the row,
    /// so one holder wins. `RETURNING` gives a row only to the winner.
    ///
    /// `clock_timestamp()` is the database clock, which every replica
    /// shares. Replica clock skew cannot give two holders a live lease.
    ///
    /// The update reads the clock after it takes the row lock. `NOW()` and
    /// `EXCLUDED` hold the time before the lock wait. A renewal that waited
    /// longer than the TTL would then return an expired lease.
    const ACQUIRE_SQL: &str = "INSERT INTO harvest_scanner_leases AS l \
             (shard_id, scanner, holder, epoch, lease_until, acquired_at) \
         VALUES ($1, $2, $3, 1, clock_timestamp() + make_interval(secs => $4), \
                 clock_timestamp()) \
         ON CONFLICT (shard_id, scanner) DO UPDATE SET \
             holder = EXCLUDED.holder, \
             epoch = CASE WHEN l.holder = EXCLUDED.holder \
                          THEN l.epoch ELSE l.epoch + 1 END, \
             lease_until = clock_timestamp() + make_interval(secs => $4), \
             acquired_at = CASE WHEN l.holder = EXCLUDED.holder \
                                THEN l.acquired_at ELSE clock_timestamp() END \
         WHERE l.holder = EXCLUDED.holder OR l.lease_until <= clock_timestamp() \
         RETURNING l.epoch";

    /// Bound on the wait for the lease row lock.
    ///
    /// A stuck transaction can hold that row lock. Without a bound, every
    /// replica then blocks on the upsert, and no replica runs the pass or
    /// refreshes its codec key. A timeout is an error, so the replica fails
    /// open and runs the pass.
    const LOCK_TIMEOUT_SQL: &str = "SET LOCAL lock_timeout = '1s'";

    /// Expire the lease now, if this holder has it.
    ///
    /// An update, not a delete, so the epoch keeps counting holders.
    const RELEASE_SQL: &str = "UPDATE harvest_scanner_leases SET lease_until = NOW() \
         WHERE shard_id = $1 AND scanner = $2 AND holder = $3";

    #[derive(QueryableByName)]
    struct EpochRow {
        #[diesel(sql_type = BigInt)]
        epoch: i64,
    }

    /// One replica's claim on one `(shard_id, scanner)` lease.
    #[derive(Debug, Clone)]
    pub struct ScannerLease {
        shard: ShardId,
        scanner: String,
        holder: String,
        ttl: Duration,
    }

    impl ScannerLease {
        /// A lease on `scanner` for `shard`, held as `holder` for `ttl`.
        ///
        /// `scanner` is the row key, from [`super::lease_scanner_key`].
        #[must_use]
        pub fn new(
            shard: ShardId,
            scanner: impl Into<String>,
            holder: impl Into<String>,
            ttl: Duration,
        ) -> Self {
            Self {
                shard,
                scanner: scanner.into(),
                holder: holder.into(),
                ttl,
            }
        }

        /// The holder id this lease claims as.
        #[must_use]
        pub fn holder(&self) -> &str {
            &self.holder
        }

        /// Take or renew the lease. `Some(epoch)` when this holder has it.
        ///
        /// Runs in its own short transaction, so the lock wait is bounded.
        ///
        /// # Errors
        ///
        /// Returns [`crate::error::HarvestError::Database`] on query failure
        /// or when the row lock is not free within one second.
        pub async fn try_acquire(
            &self,
            conn: &mut AsyncPgConnection,
        ) -> HarvestResult<Option<i64>> {
            conn.transaction::<Option<i64>, HarvestError, _>(async |conn| {
                diesel::sql_query(LOCK_TIMEOUT_SQL)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                let rows: Vec<EpochRow> = diesel::sql_query(ACQUIRE_SQL)
                    .bind::<Integer, _>(self.shard.as_i32())
                    .bind::<Text, _>(&self.scanner)
                    .bind::<Text, _>(&self.holder)
                    .bind::<Double, _>(self.ttl.as_secs_f64())
                    .load(conn)
                    .await
                    .map_err(database_error)?;
                Ok(rows.into_iter().next().map(|r| r.epoch))
            })
            .await
        }

        /// Expire the lease now, so a standby takes over on its next tick.
        ///
        /// A no-op when another holder has the lease.
        ///
        /// # Errors
        ///
        /// Returns [`crate::error::HarvestError::Database`] on query failure.
        pub async fn release(&self, conn: &mut AsyncPgConnection) -> HarvestResult<()> {
            // The same bounded lock wait as `try_acquire`, so a stuck row
            // lock cannot hang the release either.
            conn.transaction::<(), HarvestError, _>(async |conn| {
                diesel::sql_query(LOCK_TIMEOUT_SQL)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                diesel::sql_query(RELEASE_SQL)
                    .bind::<Integer, _>(self.shard.as_i32())
                    .bind::<Text, _>(&self.scanner)
                    .bind::<Text, _>(&self.holder)
                    .execute(conn)
                    .await
                    .map_err(database_error)?;
                Ok(())
            })
            .await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn acquire_admits_only_the_same_holder_or_an_expired_lease() {
            assert!(ACQUIRE_SQL.contains("ON CONFLICT (shard_id, scanner) DO UPDATE"));
            assert!(ACQUIRE_SQL.contains(
                "WHERE l.holder = EXCLUDED.holder OR l.lease_until <= clock_timestamp()"
            ));
            assert!(ACQUIRE_SQL.contains("RETURNING l.epoch"));
        }

        #[test]
        fn acquire_reads_the_clock_after_the_lock_wait() {
            assert!(!ACQUIRE_SQL.contains("NOW()"));
            assert!(!ACQUIRE_SQL.contains("EXCLUDED.lease_until"));
            assert!(
                ACQUIRE_SQL.contains("lease_until = clock_timestamp() + make_interval(secs => $4)")
            );
        }

        #[test]
        fn acquire_bounds_its_lock_wait() {
            assert_eq!(LOCK_TIMEOUT_SQL, "SET LOCAL lock_timeout = '1s'");
        }

        #[test]
        fn release_expires_only_this_holders_row_and_keeps_the_epoch() {
            assert!(RELEASE_SQL.starts_with("UPDATE harvest_scanner_leases"));
            assert!(RELEASE_SQL.contains("holder = $3"));
            assert!(!RELEASE_SQL.contains("DELETE"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_secs(1);

    #[cfg(feature = "db")]
    #[test]
    fn a_leader_gives_up_after_three_failed_leader_passes_in_a_row() {
        let mut failures = LeaderFailures::default();
        assert!(!failures.record(ScannerRole::Leader, true));
        assert!(!failures.record(ScannerRole::Leader, true));
        assert!(failures.record(ScannerRole::Leader, true));
        // The count starts again after the release.
        assert!(!failures.record(ScannerRole::Leader, true));
        // A good pass also starts it again.
        assert!(!failures.record(ScannerRole::Leader, false));
        assert!(!failures.record(ScannerRole::Leader, true));
        assert!(!failures.record(ScannerRole::Leader, true));
    }

    #[cfg(feature = "db")]
    #[test]
    fn a_tick_in_any_other_role_ends_the_run_of_failures() {
        for other in [
            ScannerRole::Standby,
            ScannerRole::Unelected,
            ScannerRole::FailOpen,
        ] {
            for other_failed in [false, true] {
                let mut failures = LeaderFailures::default();
                assert!(!failures.record(ScannerRole::Leader, true));
                assert!(!failures.record(ScannerRole::Leader, true));
                // The lease moves away and comes back.
                assert!(!failures.record(other, other_failed));
                assert!(
                    !failures.record(ScannerRole::Leader, true),
                    "one failure after a {other:?} tick must not give up the lease"
                );
            }
        }
    }

    #[test]
    fn the_lease_outlives_the_longest_sleep_at_any_interval() {
        let hours = |h: u64| Duration::from_secs(h * 3600);
        for interval in [hours(1), hours(4), hours(20), hours(30), hours(1000)] {
            for jitter in [0.0, DEFAULT_SCANNER_JITTER, MAX_SCANNER_JITTER] {
                let interval = scanner_interval(interval);
                let longest = max_jittered_interval(interval, jitter);
                let ttl = effective_lease_ttl(DEFAULT_SCANNER_LEASE_TTL, interval, jitter);
                assert!(
                    ttl >= longest * MIN_LEASE_TICKS,
                    "{interval:?} at jitter {jitter}: TTL {ttl:?} must cover three sleeps of {longest:?}"
                );
            }
        }
    }

    #[test]
    fn jitter_spans_the_band_and_keeps_its_mean() {
        assert_eq!(
            jittered_interval(BASE, 0.2, 0.0),
            Duration::from_millis(800)
        );
        assert_eq!(jittered_interval(BASE, 0.2, 0.5), BASE);
        assert_eq!(
            jittered_interval(BASE, 0.2, 1.0),
            Duration::from_millis(1200)
        );
    }

    #[test]
    fn zero_jitter_gives_the_fixed_interval() {
        for unit in [0.0, 0.3, 1.0] {
            assert_eq!(jittered_interval(BASE, 0.0, unit), BASE);
        }
    }

    #[test]
    fn bad_inputs_are_clamped_never_a_zero_or_wild_sleep() {
        assert!(clamp_jitter(f64::NAN).abs() < f64::EPSILON);
        assert!(clamp_jitter(-1.0).abs() < f64::EPSILON);
        assert!((clamp_jitter(5.0) - MAX_SCANNER_JITTER).abs() < f64::EPSILON);
        assert!(jittered_interval(BASE, 5.0, 0.0) >= Duration::from_millis(99));
        assert_eq!(
            jittered_interval(BASE, 0.2, 7.0),
            Duration::from_millis(1200)
        );
        assert_eq!(jittered_interval(BASE, 0.2, f64::NAN), BASE);
    }

    #[test]
    fn lease_ttl_covers_at_least_three_of_the_longest_sleeps() {
        let short = effective_lease_ttl(Duration::from_millis(100), BASE, 0.2);
        assert_eq!(short, Duration::from_millis(3600));
        let long = effective_lease_ttl(Duration::from_secs(30), BASE, 0.2);
        assert_eq!(long, Duration::from_secs(30));
    }

    #[test]
    fn lease_ttl_is_capped_and_never_overflows() {
        let huge = effective_lease_ttl(Duration::MAX, BASE, 0.2);
        assert_eq!(huge, MAX_SCANNER_LEASE_TTL);
        // The floor wins over the cap, up to the hard cap.
        let slow = effective_lease_ttl(Duration::from_secs(1), Duration::from_secs(200), 0.0);
        assert_eq!(slow, Duration::from_secs(600));
        let absurd = effective_lease_ttl(Duration::from_secs(1), Duration::MAX, 0.2);
        assert_eq!(absurd, LEASE_TTL_HARD_CAP);
        assert_eq!(max_jittered_interval(Duration::MAX, 0.5), Duration::MAX);
        assert_eq!(jittered_interval(Duration::MAX, 0.5, 1.0), Duration::MAX);
    }

    #[test]
    fn only_standby_skips_the_pass() {
        assert!(ScannerRole::Leader.runs_pass());
        assert!(ScannerRole::Unelected.runs_pass());
        assert!(ScannerRole::FailOpen.runs_pass());
        assert!(!ScannerRole::Standby.runs_pass());
    }

    #[test]
    fn config_turns_election_off_by_dropping_the_holder() {
        let on = ScannerConfig::default().coordination("w1");
        assert_eq!(on.holder.as_deref(), Some("w1"));
        let off = ScannerConfig {
            elect: false,
            ..ScannerConfig::default()
        }
        .coordination("w1");
        assert_eq!(off.holder, None);
    }

    #[test]
    fn lease_key_adds_the_scope_only_when_it_differs_from_the_lease_shard() {
        use crate::scanner_health::Scanner;
        use crate::types::ShardId;
        let s = |n| ShardId::new(n);
        assert_eq!(lease_scanner_key(Scanner::Timeout, s(0), &[]), "timeout");
        assert_eq!(
            lease_scanner_key(Scanner::Timeout, s(3), &[s(3)]),
            "timeout"
        );
        assert_eq!(
            lease_scanner_key(Scanner::Timeout, s(0), &[s(2), s(1), s(2)]),
            "timeout:1,2",
            "sorted and deduplicated, so the same set gives the same key"
        );
        assert_ne!(
            lease_scanner_key(Scanner::Timeout, s(0), &[s(1)]),
            lease_scanner_key(Scanner::Timeout, s(0), &[s(2)]),
            "different scopes must not share a lease"
        );
    }

    #[test]
    fn a_large_scope_gets_a_short_key() {
        use crate::scanner_health::Scanner;
        use crate::types::ShardId;
        let scope = |from: i32| (from..from + 1000).map(ShardId::new).collect::<Vec<_>>();
        let key = lease_scanner_key(Scanner::Timeout, ShardId::new(0), &scope(1));
        // The primary key index holds at most about 2.7 kB per row.
        assert!(key.len() <= 128, "{} bytes", key.len());
        // Distinct scopes keep distinct keys, and one scope keeps one key.
        assert_ne!(
            key,
            lease_scanner_key(Scanner::Timeout, ShardId::new(0), &scope(2))
        );
        let mut reversed = scope(1);
        reversed.reverse();
        assert_eq!(
            key,
            lease_scanner_key(Scanner::Timeout, ShardId::new(0), &reversed)
        );
    }

    #[test]
    fn an_empty_scope_is_keyed_as_the_shard_it_scans() {
        use crate::scanner_health::Scanner;
        use crate::types::ShardId;
        let s = |n| ShardId::new(n);
        // An empty assignment list scans shard 0, not the lease shard.
        assert_ne!(
            lease_scanner_key(Scanner::Timeout, s(5), &[]),
            lease_scanner_key(Scanner::Timeout, s(5), &[s(5)]),
            "an empty scope and a shard-5 scope do different work"
        );
        assert_eq!(
            lease_scanner_key(Scanner::Timeout, s(5), &[]),
            lease_scanner_key(Scanner::Timeout, s(5), &[s(0)])
        );
    }

    #[test]
    fn a_zero_interval_is_raised_to_the_floor() {
        assert_eq!(scanner_interval(Duration::ZERO), MIN_SCANNER_INTERVAL);
        assert_eq!(scanner_interval(BASE), BASE);
        assert_eq!(scanner_interval(Duration::MAX), MAX_SCANNER_INTERVAL);
    }

    #[test]
    fn default_timeout_interval_follows_the_worker_poll_interval() {
        assert_eq!(ScannerConfig::default().timeout_interval, None);
    }
}
