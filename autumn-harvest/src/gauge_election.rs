//! One elected gauge sampler per shard.
//!
//! Each worker runs every database gauge sampler. The fleet-wide samplers
//! read every shard. Without election, W workers pay W reads of each shard
//! for one answer.
//!
//! This module elects one reader per shard and sampler kind through the
//! scanner lease. The leader reads the shard and writes the read to
//! `harvest_gauge_samples`. Each other worker uses that row. Every worker
//! still emits each gauge, so each series keeps its meaning.
//!
//! # Why every worker still emits
//!
//! A leader-only emitter breaks on a handover. `metrics-rs` cannot drop a
//! series, so the old leader keeps its last value. A `max` then reads a
//! stale value, and a `sum` counts it twice. With a shared row, a handover
//! changes who reads, not who emits.
//!
//! # Fail open
//!
//! A failed lease query, a missing row, an old row and a row that does not
//! decode all make the worker read the shard itself. That is the behavior
//! before election. The cost is duplicate load until the fault clears.
//!
//! # DR fence
//!
//! A held shard takes no write, so each worker reads it as before. Under a
//! DR pin, each lease write and each row write first checks the fence on
//! the same connection. A fenced process writes nothing and reads the shard
//! itself.
//!
//! # Staleness bound
//!
//! A standby uses a row only while it is younger than [`MIN_LEASE_TICKS`]
//! sample intervals, and never older than the lease TTL. So a dead leader
//! leaves a gauge at most that bound plus one interval old. A standby emits
//! the value of the leader's read. A value that grows with time, such as the
//! oldest pending age, is then up to one interval behind.
//!
//! [`MIN_LEASE_TICKS`]: crate::scanner_lease::MIN_LEASE_TICKS

use std::time::Duration;

use crate::scanner_lease::{MIN_LEASE_TICKS, ScannerConfig, effective_lease_ttl};

/// The version of the sample format.
///
/// The version is part of every lease key. A new version gives new keys, so
/// workers of two versions never read each other's rows.
pub const GAUGE_SAMPLE_FORMAT: u32 = 1;

/// How long a row stays after its last write.
///
/// Each write deletes the older rows of its shard. A row that no leader
/// writes for this long belongs to an input set that no worker uses now.
pub const GAUGE_SAMPLE_RETENTION: Duration = Duration::from_hours(24);

/// One database gauge sampler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GaugeSampler {
    /// `harvest.queue.depth` and `harvest.queue.oldest_pending_age`.
    QueueDepth,
    /// `harvest.concurrency.in_flight` and `harvest.concurrency.deferred`.
    Concurrency,
    /// `harvest.rate_limit.tokens_available` and the refill rate.
    RateLimit,
    /// `harvest.dlq.entries`.
    Dlq,
    /// `harvest.queue.paused`.
    QueuePause,
    /// `harvest.workflow.history_oversized`.
    HistoryOversized,
    /// `harvest.workflow.active`.
    WorkflowActive,
    /// `harvest.shard.stranded_pending`.
    StrandedWork,
}

impl GaugeSampler {
    /// Every sampler, in a stable order.
    pub const ALL: [Self; 8] = [
        Self::QueueDepth,
        Self::Concurrency,
        Self::RateLimit,
        Self::Dlq,
        Self::QueuePause,
        Self::HistoryOversized,
        Self::WorkflowActive,
        Self::StrandedWork,
    ];

    /// The sampler name in a lease key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueDepth => "queue_depth",
            Self::Concurrency => "concurrency",
            Self::RateLimit => "rate_limit",
            Self::Dlq => "dlq",
            Self::QueuePause => "queue_pause",
            Self::HistoryOversized => "history_oversized",
            Self::WorkflowActive => "workflow_active",
            Self::StrandedWork => "stranded_work",
        }
    }

    /// The `scanner` label of `harvest.scanner.pass` for this sampler.
    #[must_use]
    pub const fn scanner_label(self) -> &'static str {
        match self {
            Self::QueueDepth => "gauge_queue_depth",
            Self::Concurrency => "gauge_concurrency",
            Self::RateLimit => "gauge_rate_limit",
            Self::Dlq => "gauge_dlq",
            Self::QueuePause => "gauge_queue_pause",
            Self::HistoryOversized => "gauge_history_oversized",
            Self::WorkflowActive => "gauge_workflow_active",
            Self::StrandedWork => "gauge_stranded_work",
        }
    }
}

/// The lease key of one sampler for one input set.
///
/// The key is `gauge:v<format>:<sampler>`. A non-null `inputs` adds
/// `:sha256:<hex>` of its canonical JSON. Object keys are sorted first, so
/// map order does not change the key. The caller sorts each list whose order
/// has no meaning.
#[must_use]
pub fn gauge_lease_key(sampler: GaugeSampler, inputs: &serde_json::Value) -> String {
    use sha2::{Digest as _, Sha256};
    use std::fmt::Write as _;

    let mut key = format!("gauge:v{GAUGE_SAMPLE_FORMAT}:{}", sampler.as_str());
    if inputs.is_null() {
        return key;
    }
    let mut canonical = String::new();
    write_canonical(inputs, &mut canonical);
    key.push_str(":sha256:");
    for byte in Sha256::digest(canonical.as_bytes()) {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

/// Writes `value` as JSON with each object's keys in sorted order.
///
/// `serde_json` keeps insertion order when a crate turns on its
/// `preserve_order` feature. The sort keeps one input set at one key.
fn write_canonical(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (name, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::String(name.clone()).to_string());
                out.push(':');
                write_canonical(item, out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Election settings of the gauge samplers of one worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaugeElection {
    /// The lease holder id. `None` turns election off.
    pub holder: Option<String>,
    /// How long a lease lasts without renewal.
    pub lease_ttl: Duration,
    /// The oldest row a standby uses.
    pub max_age: Duration,
}

impl GaugeElection {
    /// No election. Every worker reads every shard, as before.
    #[must_use]
    pub const fn unelected() -> Self {
        Self {
            holder: None,
            lease_ttl: Duration::ZERO,
            max_age: Duration::ZERO,
        }
    }

    /// The election of a worker that samples each `interval`.
    ///
    /// `scanner.elect` turns election on. The TTL follows
    /// [`crate::scanner_lease::effective_lease_ttl`] with no jitter. The
    /// gauge samplers sleep a fixed interval. A standby uses a row younger
    /// than [`crate::scanner_lease::MIN_LEASE_TICKS`] intervals, and never
    /// older than the TTL.
    #[must_use]
    pub fn new(scanner: &ScannerConfig, holder: impl Into<String>, interval: Duration) -> Self {
        if !scanner.elect {
            return Self::unelected();
        }
        let lease_ttl = effective_lease_ttl(scanner.lease_ttl, interval, 0.0);
        let max_age = interval.saturating_mul(MIN_LEASE_TICKS).min(lease_ttl);
        Self {
            holder: Some(holder.into()),
            lease_ttl,
            max_age,
        }
    }
}

#[cfg(feature = "db")]
pub use db::{GaugeSlot, fresh_gauge_sample, publish_gauge_sample};

#[cfg(feature = "db")]
mod db {
    use std::time::Duration;

    use diesel::QueryableByName;
    use diesel::sql_types::{Double, Integer, Jsonb, Text};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::{GaugeElection, GaugeSampler};
    use crate::cached_stmt::CachePreparedDsl as _;
    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::scanner_lease::{LEASE_RELEASE_BOUND, LeaderFailures, ScannerLease, ScannerRole};
    use crate::telemetry::MetricsRecorder;
    use crate::types::ShardId;

    /// Write one row while `$4` holds a live lease on the same key. Delete
    /// the rows of the shard that no leader wrote for one day.
    ///
    /// The lease check stops an outdated leader. Its read can outlive its
    /// lease, and a later leader can write first. The outdated read then
    /// writes nothing.
    ///
    /// The check locks the lease row `FOR SHARE` until the write commits. A
    /// handover in flight holds that row, so the write waits for it. Postgres
    /// then checks the new row version, and a superseded holder writes
    /// nothing. Without the lock, the check reads an old version and the
    /// outdated read can overwrite the new one.
    ///
    /// The delete skips the row of `$2`. One statement must not delete and
    /// then update the same row. `clock_timestamp()` is the database clock,
    /// which every worker shares.
    pub(super) const PUBLISH_SQL: &str = "WITH pruned AS ( \
             DELETE FROM harvest_gauge_samples \
             WHERE shard_id = $1 AND sampler <> $2 \
               AND sampled_at < clock_timestamp() - INTERVAL '1 day') \
         INSERT INTO harvest_gauge_samples (shard_id, sampler, sample, sampled_at) \
         SELECT $1, $2, $3, clock_timestamp() \
         WHERE EXISTS (SELECT 1 FROM harvest_scanner_leases l \
                       WHERE l.shard_id = $1 AND l.scanner = $2 AND l.holder = $4 \
                         AND l.lease_until > clock_timestamp() \
                       FOR SHARE) \
         ON CONFLICT (shard_id, sampler) DO UPDATE SET \
             sample = EXCLUDED.sample, sampled_at = EXCLUDED.sampled_at";

    /// Bound on the wait for a row lock during a write.
    ///
    /// Another transaction can hold the row lock of a sample or of a pruned
    /// row. Without a bound, the leader then waits, and no gauge is emitted.
    pub(super) const WRITE_LOCK_TIMEOUT_SQL: &str = "SET LOCAL lock_timeout = '1s'";

    /// The row of one shard and key, when it is younger than `$3` seconds.
    pub(super) const FRESH_SQL: &str = "SELECT sample FROM harvest_gauge_samples \
         WHERE shard_id = $1 AND sampler = $2 \
           AND sampled_at >= clock_timestamp() - make_interval(secs => $3)";

    #[derive(QueryableByName)]
    struct SampleRow {
        #[diesel(sql_type = Jsonb)]
        sample: serde_json::Value,
    }

    /// Write the read of one shard, when `holder` holds the lease of `key`.
    ///
    /// Returns `false` when `holder` has no live lease, so nothing was
    /// written.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn publish_gauge_sample(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        key: &str,
        holder: &str,
        sample: &serde_json::Value,
    ) -> HarvestResult<bool> {
        // A short transaction with a bounded lock wait, as for the lease. A
        // stuck row lock then fails the write, and the leader still emits
        // its own read.
        conn.transaction::<bool, HarvestError, _>(async |conn| {
            diesel::sql_query(WRITE_LOCK_TIMEOUT_SQL)
                .execute(conn)
                .await
                .map_err(database_error)?;
            let written = diesel::sql_query(PUBLISH_SQL)
                .bind::<Integer, _>(shard.as_i32())
                .bind::<Text, _>(key)
                .bind::<Jsonb, _>(sample)
                .bind::<Text, _>(holder)
                .cache_prepared()
                .execute(conn)
                .await
                .map_err(database_error)?;
            Ok(written > 0)
        })
        .await
    }

    /// The row of one shard and key, when it is younger than `max_age`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::HarvestError::Database`] on query failure.
    pub async fn fresh_gauge_sample(
        conn: &mut AsyncPgConnection,
        shard: ShardId,
        key: &str,
        max_age: Duration,
    ) -> HarvestResult<Option<serde_json::Value>> {
        let rows: Vec<SampleRow> = diesel::sql_query(FRESH_SQL)
            .bind::<Integer, _>(shard.as_i32())
            .bind::<Text, _>(key)
            .bind::<Double, _>(max_age.as_secs_f64())
            .cache_prepared()
            .load(conn)
            .await
            .map_err(database_error)?;
        Ok(rows.into_iter().next().map(|row| row.sample))
    }

    /// `value` as JSON, when it decodes back to its type.
    ///
    /// `serde_json` writes a NaN or an infinity as `null`, and no standby can
    /// decode that row. So such a read is not shared.
    fn shareable<T: Serialize + DeserializeOwned>(
        value: &T,
    ) -> serde_json::Result<serde_json::Value> {
        let json = serde_json::to_value(value)?;
        serde_json::from_value::<T>(json.clone())?;
        Ok(json)
    }

    /// One worker's election state for one shard and sampler.
    #[derive(Debug)]
    pub struct GaugeSlot {
        sampler: GaugeSampler,
        shard: ShardId,
        shard_label: String,
        key: String,
        /// `None` turns election off for this slot.
        lease: Option<ScannerLease>,
        fence_shard: Option<ShardId>,
        lease_ttl: Duration,
        max_age: Duration,
        failures: LeaderFailures,
        abdicated_until: Option<tokio::time::Instant>,
        led: bool,
        last_role: Option<ScannerRole>,
    }

    impl GaugeSlot {
        /// A slot for `sampler` on `shard`, keyed by `key`.
        ///
        /// `fence_shard` names the shard for the DR write fence. `None`
        /// means the worker database, as in
        /// [`crate::replication::shard_writes_held`].
        #[must_use]
        pub fn new(
            election: &GaugeElection,
            sampler: GaugeSampler,
            key: impl Into<String>,
            shard: ShardId,
            fence_shard: Option<ShardId>,
        ) -> Self {
            let key = key.into();
            let lease = election
                .holder
                .as_ref()
                .map(|holder| ScannerLease::new(shard, key.clone(), holder, election.lease_ttl));
            Self {
                sampler,
                shard,
                shard_label: shard.as_i32().to_string(),
                key,
                lease,
                fence_shard,
                lease_ttl: election.lease_ttl,
                max_age: election.max_age,
                failures: LeaderFailures::default(),
                abdicated_until: None,
                led: false,
                last_role: None,
            }
        }

        /// The shard of this slot.
        #[must_use]
        pub const fn shard(&self) -> ShardId {
            self.shard
        }

        /// The lease key of this slot.
        #[must_use]
        pub fn key(&self) -> &str {
            &self.key
        }

        /// The sampler of this slot.
        #[must_use]
        pub const fn sampler(&self) -> GaugeSampler {
            self.sampler
        }

        /// `true` after this slot held the lease at least once.
        #[must_use]
        pub const fn has_led(&self) -> bool {
            self.led
        }

        /// One sample of this shard, and the role that gave it.
        ///
        /// A leader reads the shard and writes the row. A standby uses a
        /// young row. Every other case reads the shard. Records the role on
        /// `harvest.scanner.pass`.
        ///
        /// # Errors
        ///
        /// Returns the error of `read` when this worker reads the shard.
        #[allow(
            clippy::future_not_send,
            reason = "generic over `read`; `tokio::spawn` checks `Send` at each caller"
        )]
        pub async fn sample<T, F>(
            &mut self,
            conn: &mut AsyncPgConnection,
            metrics: &dyn MetricsRecorder,
            read: F,
        ) -> HarvestResult<(T, ScannerRole)>
        where
            T: Serialize + DeserializeOwned,
            F: AsyncFnOnce(&mut AsyncPgConnection) -> HarvestResult<T>,
        {
            let mut role = self.decide(conn).await;
            let result = match role {
                ScannerRole::Leader => {
                    self.led = true;
                    let result = read(conn).await;
                    let failed = match &result {
                        Ok(value) => !self.publish(conn, shareable(value)).await,
                        Err(_) => true,
                    };
                    if self.failures.record(role, failed) {
                        self.abdicate(conn).await;
                    }
                    result
                }
                ScannerRole::Standby => {
                    let _ = self.failures.record(role, false);
                    if let Some(value) = self.shared(conn).await {
                        Ok(value)
                    } else {
                        role = ScannerRole::FailOpen;
                        read(conn).await
                    }
                }
                ScannerRole::Unelected | ScannerRole::FailOpen => {
                    let _ = self.failures.record(role, false);
                    read(conn).await
                }
            };
            if self.last_role != Some(role) {
                tracing::debug!(
                    sampler = self.sampler.as_str(),
                    shard = %self.shard_label,
                    role = role.as_str(),
                    "gauge sampler role changed"
                );
                self.last_role = Some(role);
            }
            metrics.record_scanner_pass(
                self.sampler.scanner_label(),
                &self.shard_label,
                role.as_str(),
            );
            result.map(|value| (value, role))
        }

        /// Expire the lease when this slot led.
        ///
        /// A standby then leads on its next tick, not after the TTL.
        ///
        /// # Errors
        ///
        /// Returns [`crate::error::HarvestError::Database`] on query failure.
        pub async fn release(&mut self, conn: &mut AsyncPgConnection) -> HarvestResult<()> {
            if !self.led || !self.writes_allowed(conn).await {
                return Ok(());
            }
            match &self.lease {
                Some(lease) => lease.release(conn).await,
                None => Ok(()),
            }
        }

        /// `true` when this process may write the lease and the row now.
        ///
        /// A held shard can be an unpromoted standby, so it takes no write.
        /// Under a DR pin, the connection must pass the fence first. The
        /// lease is a load control and the row is a cache. So a check right
        /// before each write is enough, and no barrier is held.
        async fn writes_allowed(&self, conn: &mut AsyncPgConnection) -> bool {
            if crate::replication::shard_writes_held(self.fence_shard) {
                return false;
            }
            let fence_key = self.fence_shard.unwrap_or(ShardId::UNENCODED);
            if crate::replication::FenceRegistry::expected(fence_key).is_none() {
                return true;
            }
            match crate::replication::assert_fence_group(conn, fence_key).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::debug!(
                        sampler = self.sampler.as_str(),
                        shard = %self.shard_label,
                        error = %error,
                        "gauge sampler is fenced; reading the shard without election"
                    );
                    false
                }
            }
        }

        /// The role of this tick.
        async fn decide(&self, conn: &mut AsyncPgConnection) -> ScannerRole {
            let Some(lease) = &self.lease else {
                return ScannerRole::Unelected;
            };
            if !self.writes_allowed(conn).await {
                return ScannerRole::Unelected;
            }
            if self
                .abdicated_until
                .is_some_and(|until| tokio::time::Instant::now() < until)
            {
                return ScannerRole::Standby;
            }
            match lease.try_acquire(conn).await {
                Ok(Some(_epoch)) => ScannerRole::Leader,
                Ok(None) => ScannerRole::Standby,
                Err(error) => {
                    if self.last_role != Some(ScannerRole::FailOpen) {
                        tracing::warn!(
                            sampler = self.sampler.as_str(),
                            shard = %self.shard_label,
                            error = %error,
                            "gauge sampler lease query failed; reading the shard until it recovers"
                        );
                    }
                    ScannerRole::FailOpen
                }
            }
        }

        /// The leader's row, when it is young and decodes.
        async fn shared<T: DeserializeOwned>(&self, conn: &mut AsyncPgConnection) -> Option<T> {
            match fresh_gauge_sample(conn, self.shard, &self.key, self.max_age).await {
                Ok(Some(value)) => match serde_json::from_value(value) {
                    Ok(decoded) => Some(decoded),
                    Err(error) => {
                        tracing::debug!(
                            sampler = self.sampler.as_str(),
                            shard = %self.shard_label,
                            error = %error,
                            "gauge sample row does not decode; reading the shard"
                        );
                        None
                    }
                },
                Ok(None) => None,
                Err(error) => {
                    tracing::debug!(
                        sampler = self.sampler.as_str(),
                        shard = %self.shard_label,
                        error = %error,
                        "gauge sample row read failed; reading the shard"
                    );
                    None
                }
            }
        }

        /// Write the leader's read. Returns `false` when the write fails.
        ///
        /// A read that [`shareable`] refuses is a failure. A lost lease or a
        /// fence writes nothing, and that is not a failure. The next tick
        /// then stands by.
        async fn publish(
            &self,
            conn: &mut AsyncPgConnection,
            json: serde_json::Result<serde_json::Value>,
        ) -> bool {
            let written = match json {
                Err(error) => Err(error.to_string()),
                Ok(_) if !self.writes_allowed(conn).await => Ok(false),
                Ok(json) => {
                    let holder = self.lease.as_ref().map_or("", ScannerLease::holder);
                    publish_gauge_sample(conn, self.shard, &self.key, holder, &json)
                        .await
                        .map_err(|e| e.to_string())
                }
            };
            match written {
                Ok(true) => true,
                Ok(false) => {
                    tracing::debug!(
                        sampler = self.sampler.as_str(),
                        shard = %self.shard_label,
                        "gauge sample not written: the lease moved or the shard is fenced"
                    );
                    true
                }
                Err(error) => {
                    tracing::debug!(
                        sampler = self.sampler.as_str(),
                        shard = %self.shard_label,
                        error = %error,
                        "gauge sample write failed"
                    );
                    false
                }
            }
        }

        /// Give up the lease after failed passes, and stand by for one TTL.
        ///
        /// A pass can fail on one worker alone. Another worker can then lead.
        async fn abdicate(&mut self, conn: &mut AsyncPgConnection) {
            self.abdicated_until = Some(tokio::time::Instant::now() + self.lease_ttl);
            tracing::warn!(
                sampler = self.sampler.as_str(),
                shard = %self.shard_label,
                "gauge sampler gave up its lease after failed passes"
            );
            let Some(lease) = &self.lease else {
                return;
            };
            match tokio::time::timeout(LEASE_RELEASE_BOUND, lease.release(conn)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "gauge sampler lease release failed");
                }
                Err(_elapsed) => {
                    tracing::warn!("gauge sampler lease release timed out");
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_write_keeps_rows_for_the_retention_and_reads_the_shared_clock() {
            assert!(PUBLISH_SQL.contains("INTERVAL '1 day'"));
            assert_eq!(super::super::GAUGE_SAMPLE_RETENTION.as_secs(), 86_400);
            assert!(PUBLISH_SQL.contains("sampler <> $2"));
            assert!(PUBLISH_SQL.contains("l.holder = $4 AND l.lease_until > clock_timestamp()"));
            assert!(PUBLISH_SQL.contains("FOR SHARE"));
            assert!(!PUBLISH_SQL.contains("NOW()"));
            assert!(FRESH_SQL.contains("clock_timestamp() - make_interval(secs => $3)"));
            assert_eq!(WRITE_LOCK_TIMEOUT_SQL, "SET LOCAL lock_timeout = '1s'");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_sampler_has_a_distinct_name_and_scanner_label() {
        let names: std::collections::BTreeSet<_> =
            GaugeSampler::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(names.len(), GaugeSampler::ALL.len());
        for sampler in GaugeSampler::ALL {
            assert_ne!(sampler.as_str(), "");
            assert_eq!(
                sampler.scanner_label(),
                format!("gauge_{}", sampler.as_str())
            );
        }
    }

    #[test]
    fn a_key_without_inputs_names_the_format_and_the_sampler() {
        assert_eq!(
            gauge_lease_key(GaugeSampler::Dlq, &serde_json::Value::Null),
            "gauge:v1:dlq"
        );
        assert_eq!(
            gauge_lease_key(GaugeSampler::QueueDepth, &serde_json::Value::Null),
            "gauge:v1:queue_depth"
        );
    }

    #[test]
    fn different_inputs_give_different_keys() {
        let a = gauge_lease_key(GaugeSampler::QueueDepth, &json!({"queues": ["a"]}));
        let b = gauge_lease_key(GaugeSampler::QueueDepth, &json!({"queues": ["b"]}));
        assert_ne!(a, b);
        assert!(a.starts_with("gauge:v1:queue_depth:sha256:"), "{a}");
        // The hex digest keeps the key short.
        assert_eq!(a.len(), "gauge:v1:queue_depth:sha256:".len() + 64);
    }

    #[test]
    fn object_key_order_does_not_change_the_key() {
        let mut forward = serde_json::Map::new();
        forward.insert("a".into(), json!(1));
        forward.insert("b".into(), json!({"y": 2, "x": 3}));
        let mut backward = serde_json::Map::new();
        backward.insert("b".into(), json!({"x": 3, "y": 2}));
        backward.insert("a".into(), json!(1));
        assert_eq!(
            gauge_lease_key(
                GaugeSampler::StrandedWork,
                &serde_json::Value::Object(forward)
            ),
            gauge_lease_key(
                GaugeSampler::StrandedWork,
                &serde_json::Value::Object(backward)
            ),
        );
    }

    #[test]
    fn one_input_set_gives_one_key_per_sampler() {
        let inputs = json!({"queues": ["a"]});
        assert_ne!(
            gauge_lease_key(GaugeSampler::QueueDepth, &inputs),
            gauge_lease_key(GaugeSampler::Concurrency, &inputs),
        );
    }

    #[test]
    fn election_follows_the_scanner_switch() {
        let interval = Duration::from_secs(15);
        let on = GaugeElection::new(&ScannerConfig::default(), "w1", interval);
        assert_eq!(on.holder.as_deref(), Some("w1"));
        let off = GaugeElection::new(
            &ScannerConfig {
                elect: false,
                ..ScannerConfig::default()
            },
            "w1",
            interval,
        );
        assert_eq!(off.holder, None);
    }

    #[test]
    fn the_lease_and_the_row_age_cover_three_intervals() {
        let interval = Duration::from_secs(15);
        let election = GaugeElection::new(&ScannerConfig::default(), "w1", interval);
        assert_eq!(election.lease_ttl, Duration::from_secs(45));
        assert_eq!(election.max_age, Duration::from_secs(45));
    }

    #[test]
    fn a_long_configured_ttl_does_not_widen_the_row_age() {
        let interval = Duration::from_secs(15);
        let scanner = ScannerConfig {
            lease_ttl: Duration::from_secs(300),
            ..ScannerConfig::default()
        };
        let election = GaugeElection::new(&scanner, "w1", interval);
        assert_eq!(election.lease_ttl, Duration::from_secs(300));
        assert_eq!(election.max_age, Duration::from_secs(45));
    }

    #[test]
    fn the_row_age_never_passes_the_lease_ttl() {
        // The TTL hard cap is one day. Three intervals of ten hours pass it.
        let interval = Duration::from_hours(10);
        let election = GaugeElection::new(&ScannerConfig::default(), "w1", interval);
        assert!(election.max_age <= election.lease_ttl);
    }
}
