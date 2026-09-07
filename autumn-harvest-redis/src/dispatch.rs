//! Redis Streams implementation of `autumn_harvest::dispatch::TaskDispatch`
//! (issue #1312).
//!
//! ## What travels on the channel
//!
//! Postgres holds every `harvest_task_queue` row and stays the source of
//! truth. A stream entry here carries a **reference** only: the task id, the
//! queue, the due time, the priority, the redelivery count and the shard slot.
//! A worker reads a reference, claims the named row in Postgres with the full
//! claim predicate, and then acks the reference. No workflow state and no
//! event history ever reaches Redis.
//!
//! ## Key family
//!
//! - `{prefix}:dispatch:{queue}` — the stream of claimable references.
//! - `{prefix}:dispatch:{queue}:delayed` — a sorted set of references that are
//!   not yet due, scored by due time in unix milliseconds.
//! - `{prefix}:dispatch:{queue}:delayed:payloads` — the payload of each
//!   delayed reference, keyed by task id.
//! - `{prefix}:dispatch:marker:{task_id}` — the dedupe marker.
//!
//! The standalone [`crate::RedisTaskQueue`] owns `{prefix}:queue:*` and
//! `{prefix}:scheduled:*`. The two key families do not overlap, so one Redis
//! and one prefix can serve both.
//!
//! ## The dedupe marker
//!
//! A publish is idempotent per task id. The marker records that the channel
//! already holds a reference for the row. Its **value** is the `scheduled_at`
//! of the held reference, in unix milliseconds. `ack` deletes the marker,
//! which is what lets the reconcile sweep republish the row on its next pass.
//! The marker also expires after `dedupe_ttl`. A leaked marker therefore
//! cannot block a republish for ever. A worker that dies between the read and
//! the ack leaks one.
//!
//! ## Publish idempotency (contract C1)
//!
//! The rule is keyed on `scheduled_at`. The marker value makes the rule
//! decidable for a live stream entry as well as for a parked one.
//!
//! - A hint with the **same** `scheduled_at` as the held reference is a no-op.
//!   The marker TTL is refreshed. The reconcile sweep republishes a row it
//!   has already published, so this is the common case.
//! - A hint with a **different** `scheduled_at` replaces the held reference.
//!   The row state changed, so the reference moves to the new due time and its
//!   `redeliveries` resets to 0. A parked entry is moved in place. A live
//!   stream entry cannot be removed safely, so the marker moves and a second
//!   entry is added; the by-id claim drops the stale one.
//!
//! A released reference keeps the row's `scheduled_at` in its payload and in
//! its marker. The backoff moves the delayed-set score only. A reconcile
//! republish therefore never disturbs a backoff. A wake and a retry both move
//! the row's `scheduled_at`, so each of them moves the reference.
//!
//! ## Delivery
//!
//! Delivery is at least once. A duplicate reference is harmless: the by-id
//! claim finds the row already `RUNNING` or terminal and the worker acks the
//! reference without running anything.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use autumn_harvest::dispatch::{DispatchHint, DispatchLease, DispatchMaintenance, TaskDispatch};
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::types::ShardId;
use chrono::{DateTime, Utc};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::streams::{
    StreamClaimReply, StreamPendingCountReply, StreamReadOptions, StreamReadReply,
};
use redis::{AsyncCommands, RedisError, Script};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{RedisAdapterError, RedisAdapterResult};
use crate::naming::{
    dispatch_delayed_key, dispatch_marker_key, dispatch_payloads_key, dispatch_stream_key,
};
use crate::redis_queue::{PROMOTE_LUA, is_busygroup};

const DEFAULT_KEY_PREFIX: &str = "harvest";
const DEFAULT_CONSUMER_GROUP: &str = "harvest_workers";
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_DEDUPE_TTL: Duration = Duration::from_secs(600);

/// Stream field that holds the JSON reference.
const PAYLOAD_FIELD: &str = "payload";
/// Consumer name that owns entries between the claim and the re-add during
/// recovery. It never runs work.
const RECOVERY_CONSUMER: &str = "__recovered__";
/// Pending entries inspected in one recovery pass per queue.
const RECOVER_BATCH: usize = 128;

/// Deadline for one connection attempt, and for [`RedisDispatch::connect`] as
/// a whole (contract C4).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Deadline for one command on an open connection (contract C4).
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Separator between the entry id and the payload inside a lease handle.
///
/// A stream entry id is `{milliseconds}-{sequence}`, so it never holds this
/// character. The split therefore takes the first occurrence and the payload
/// may contain the separator itself.
const HANDLE_SEPARATOR: char = '|';

/// Configuration for [`RedisDispatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDispatchConfig {
    /// Key prefix. Every key lives under `{prefix}:dispatch:*`.
    pub key_prefix: String,
    /// Consumer group name. Every worker shares one group, so `XREADGROUP`
    /// delivers each reference to exactly one worker.
    pub consumer_group: String,
    /// How long a delivered reference may sit unacked before
    /// [`TaskDispatch::maintain`] recovers it.
    pub visibility_timeout: Duration,
    /// Lifetime of a dedupe marker. It bounds how long a leaked marker can
    /// suppress a republish of the same row.
    pub dedupe_ttl: Duration,
}

impl Default for RedisDispatchConfig {
    fn default() -> Self {
        Self {
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            consumer_group: DEFAULT_CONSUMER_GROUP.to_string(),
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            dedupe_ttl: DEFAULT_DEDUPE_TTL,
        }
    }
}

impl RedisDispatchConfig {
    /// Reject a configuration that cannot name a key or a consumer group.
    ///
    /// An empty prefix produces keys that collide with another tenant's. An
    /// empty group name makes `XGROUP CREATE` fail at the first publish, far
    /// from the mistake. Both are rejected at construction instead.
    fn validate(&self) -> RedisAdapterResult<()> {
        if self.key_prefix.trim().is_empty() {
            return Err(RedisAdapterError::InvalidConfig(
                "key_prefix must not be empty".to_string(),
            ));
        }
        if self.consumer_group.trim().is_empty() {
            return Err(RedisAdapterError::InvalidConfig(
                "consumer_group must not be empty".to_string(),
            ));
        }
        Ok(())
    }
}

/// One reference as it is stored in a stream entry or in the delayed set.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DispatchRef {
    task_id: Uuid,
    queue_name: String,
    scheduled_at: DateTime<Utc>,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    redeliveries: u32,
    #[serde(default)]
    shard: Option<i32>,
}

impl DispatchRef {
    fn from_hint(hint: &DispatchHint) -> Self {
        Self {
            task_id: hint.task_id,
            queue_name: hint.queue_name.clone(),
            scheduled_at: hint.scheduled_at,
            priority: hint.priority,
            redeliveries: 0,
            shard: hint.shard.map(ShardId::as_i32),
        }
    }

    /// Rebuild a reference from a lease alone.
    ///
    /// Used when the handle carries no payload, which happens only for a lease
    /// this channel did not produce. Priority degrades to the default, which
    /// the v1 design already treats as best effort. The row's `scheduled_at`
    /// is unknown, so `now` stands in for it; a reconcile republish then moves
    /// the reference instead of leaving it alone.
    fn from_lease(lease: &DispatchLease) -> Self {
        Self {
            task_id: lease.task_id,
            queue_name: lease.queue_name.clone(),
            scheduled_at: Utc::now(),
            priority: 0,
            redeliveries: lease.redeliveries,
            shard: lease.shard.map(ShardId::as_i32),
        }
    }

    fn into_lease(self, handle: String) -> DispatchLease {
        DispatchLease {
            task_id: self.task_id,
            queue_name: self.queue_name,
            redeliveries: self.redeliveries,
            handle,
            shard: self.shard.map(ShardId::new),
        }
    }

    /// Marker value for this reference: the row's due time in milliseconds.
    const fn marker_value(&self) -> i64 {
        self.scheduled_at.timestamp_millis()
    }
}

/// Redis Streams implementation of the dispatch channel.
///
/// Cheap to clone: the connections, the config and the caches are
/// `Arc`-shared.
#[derive(Clone)]
pub struct RedisDispatch {
    /// Shared multiplexed connection for every non-blocking command.
    conn: ConnectionManager,
    /// Dedicated connection for the blocking read.
    ///
    /// `XREADGROUP ... BLOCK` occupies its connection for the whole wait. On
    /// the shared multiplexed connection it would stall every other command
    /// of every other caller, so the read gets a connection of its own.
    blocking: ConnectionManager,
    config: Arc<RedisDispatchConfig>,
    publish_script: Arc<Script>,
    promote_script: Arc<Script>,
    /// Queues whose consumer group this process already created.
    ensured: Arc<Mutex<HashSet<String>>>,
    /// Unix milliseconds of the last promotion pass driven by a read.
    last_promote_ms: Arc<AtomicI64>,
    /// Unix milliseconds of the last recovery pass.
    last_recover_ms: Arc<AtomicI64>,
    /// Number of reads served. It rotates the queue order of the next read.
    reads: Arc<AtomicU64>,
}

impl std::fmt::Debug for RedisDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ConnectionManager and Script do not implement Debug.
        f.debug_struct("RedisDispatch")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RedisDispatch {
    /// Build a channel from two open connection managers.
    ///
    /// `blocking` must be a second connection. See the field documentation for
    /// why the blocking read cannot share the general-purpose connection.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::InvalidConfig`] when `config` has an empty
    /// `key_prefix` or an empty `consumer_group`.
    pub fn from_connection(
        conn: ConnectionManager,
        blocking: ConnectionManager,
        config: RedisDispatchConfig,
    ) -> RedisAdapterResult<Self> {
        config.validate()?;
        Ok(Self {
            conn,
            blocking,
            config: Arc::new(config),
            publish_script: Arc::new(Script::new(PUBLISH_LUA)),
            promote_script: Arc::new(Script::new(PROMOTE_LUA)),
            ensured: Arc::new(Mutex::new(HashSet::new())),
            last_promote_ms: Arc::new(AtomicI64::new(0)),
            last_recover_ms: Arc::new(AtomicI64::new(0)),
            reads: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Open two Redis connections and build a channel.
    ///
    /// Both connections carry a 5 second connection timeout and a 5 second
    /// response timeout (contract C4). The connection manager retries a lost
    /// connection with its own backoff. This call is bounded on top of those
    /// retries. A black-holed address therefore fails in about
    /// [`CONNECT_TIMEOUT`], not after the whole retry budget.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::InvalidConfig`] for an unusable `config`.
    /// Returns [`RedisAdapterError::TlsUnavailable`] for a `rediss://` URL
    /// without the crate's `tls` feature. Returns
    /// [`RedisAdapterError::ConnectTimeout`] when the server does not answer
    /// inside the connect timeout. Returns [`RedisAdapterError::Redis`] when
    /// the URL cannot be parsed, or when the server refuses the connection.
    pub async fn connect(url: &str, config: RedisDispatchConfig) -> RedisAdapterResult<Self> {
        config.validate()?;
        check_tls_support(url)?;
        let client = redis::Client::open(url)?;
        let conn = open_manager(&client).await?;
        let blocking = open_manager(&client).await?;
        Self::from_connection(conn, blocking, config)
    }

    /// Currently configured key prefix.
    #[must_use]
    pub fn key_prefix(&self) -> &str {
        &self.config.key_prefix
    }

    /// Currently configured consumer group name.
    #[must_use]
    pub fn consumer_group(&self) -> &str {
        &self.config.consumer_group
    }

    /// Currently configured visibility timeout.
    #[must_use]
    pub fn visibility_timeout(&self) -> Duration {
        self.config.visibility_timeout
    }

    fn stream_key(&self, queue_name: &str) -> String {
        dispatch_stream_key(&self.config.key_prefix, queue_name)
    }

    fn delayed_key(&self, queue_name: &str) -> String {
        dispatch_delayed_key(&self.config.key_prefix, queue_name)
    }

    fn payloads_key(&self, queue_name: &str) -> String {
        dispatch_payloads_key(&self.config.key_prefix, queue_name)
    }

    fn marker_key(&self, task_id: Uuid) -> String {
        dispatch_marker_key(&self.config.key_prefix, &task_id.to_string())
    }

    fn dedupe_ttl_secs(&self) -> i64 {
        i64::try_from(self.config.dedupe_ttl.as_secs())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    fn visibility_ms(&self) -> u64 {
        u64::try_from(self.config.visibility_timeout.as_millis()).unwrap_or(u64::MAX)
    }

    /// Create the consumer group for `queue_name` if this process has not yet
    /// created it. `force` re-runs the command and ignores the cache.
    ///
    /// The group starts at `0`, not at the stream tail. Every live entry in a
    /// dispatch stream is an outstanding reference, because `ack` and
    /// `release` both delete the entry they finish with. Starting at the tail
    /// would strand every live entry when a group is recreated. That happens
    /// after an operator deletes the group, or after Redis loses the group
    /// but keeps the stream. Starting at `0` redelivers them instead, which
    /// the at-least-once contract already covers.
    async fn ensure_group(&self, queue_name: &str, force: bool) -> RedisAdapterResult<()> {
        if !force {
            let cached = self
                .ensured
                .lock()
                .is_ok_and(|seen| seen.contains(queue_name));
            if cached {
                return Ok(());
            }
        }

        let mut conn = self.conn.clone();
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(self.stream_key(queue_name))
            .arg(&self.config.consumer_group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        match result {
            Ok(()) => {}
            Err(err) if is_busygroup(&err) => {}
            Err(err) => return Err(err.into()),
        }
        if let Ok(mut seen) = self.ensured.lock() {
            seen.insert(queue_name.to_string());
        }
        Ok(())
    }

    async fn ensure_groups(&self, queues: &[String], force: bool) -> RedisAdapterResult<()> {
        for queue in queues {
            self.ensure_group(queue, force).await?;
        }
        Ok(())
    }

    /// One `EVALSHA` of the promotion script per queue, in one pipeline.
    fn promote_pipeline(&self, queues: &[String], now_ms: i64) -> redis::Pipeline {
        let mut pipe = redis::pipe();
        for queue in queues {
            pipe.cmd("EVALSHA")
                .arg(self.promote_script.get_hash())
                .arg(3)
                .arg(self.delayed_key(queue))
                .arg(self.payloads_key(queue))
                .arg(self.stream_key(queue))
                .arg(now_ms);
        }
        pipe
    }

    /// Promote every due delayed reference for every queue, in one round trip.
    async fn promote_queues(&self, queues: &[String]) -> RedisAdapterResult<usize> {
        if queues.is_empty() {
            return Ok(0);
        }
        // The groups must exist before the script adds entries.
        self.ensure_groups(queues, false).await?;
        let now_ms = Utc::now().timestamp_millis();
        let mut conn = self.conn.clone();
        let counts: Vec<i64> = match self
            .promote_pipeline(queues, now_ms)
            .query_async(&mut conn)
            .await
        {
            Ok(counts) => counts,
            Err(err) if err.kind() == redis::ErrorKind::NoScriptError => {
                // The server forgot the script. A restart or `SCRIPT FLUSH`
                // does that. Load it once and run the pipeline again.
                let _: String = redis::cmd("SCRIPT")
                    .arg("LOAD")
                    .arg(PROMOTE_LUA)
                    .query_async(&mut conn)
                    .await?;
                self.promote_pipeline(queues, now_ms)
                    .query_async(&mut conn)
                    .await?
            }
            Err(err) => return Err(err.into()),
        };
        Ok(counts
            .into_iter()
            .map(|count| usize::try_from(count).unwrap_or(0))
            .sum())
    }

    /// Run a promotion pass at most once per `interval`.
    ///
    /// A read happens on every poll, and a promotion costs one round trip. The
    /// rate limit keeps an idle worker's cost proportional to the poll
    /// interval rather than to the number of reads. `maintain` shares the same
    /// slot with a zero interval. It therefore always promotes, and it claims
    /// the slot. The read that follows it in the same iteration then skips its
    /// own pass.
    ///
    /// Returns the number of promoted references, or `0` when the pass is
    /// skipped.
    async fn promote_rate_limited(
        &self,
        queues: &[String],
        interval: Duration,
    ) -> RedisAdapterResult<usize> {
        let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
        if !claim_rate_limit_slot(
            &self.last_promote_ms,
            Utc::now().timestamp_millis(),
            interval_ms,
        ) {
            return Ok(0);
        }
        self.promote_queues(queues).await
    }

    /// Whether a recovery pass is due, claiming the slot when it is.
    ///
    /// Recovery runs twice per visibility timeout, so a reference left by a
    /// dead consumer waits at most one and a half timeouts.
    fn recovery_is_due(&self) -> bool {
        let interval_ms = i64::try_from(self.config.visibility_timeout.as_millis() / 2)
            .unwrap_or(i64::MAX)
            .max(1);
        claim_rate_limit_slot(
            &self.last_recover_ms,
            Utc::now().timestamp_millis(),
            interval_ms,
        )
    }

    async fn read_group(
        &self,
        keys: &[String],
        consumer: &str,
        count: usize,
        wait: Duration,
    ) -> redis::RedisResult<StreamReadReply> {
        let mut options = StreamReadOptions::default()
            .group(&self.config.consumer_group, consumer)
            .count(count);
        if !wait.is_zero() {
            // BLOCK 0 waits for ever, so a sub-millisecond wait rounds up.
            let wait_ms = usize::try_from(wait.as_millis())
                .unwrap_or(usize::MAX)
                .max(1);
            options = options.block(wait_ms);
        }
        let ids = vec![">"; keys.len()];
        let mut conn = self.blocking.clone();
        conn.xread_options(keys, &ids, &options).await
    }

    /// Read one batch, healing the consumer groups once on `NOGROUP`.
    async fn read_with_heal(
        &self,
        queues: &[String],
        keys: &[String],
        consumer: &str,
        count: usize,
        wait: Duration,
    ) -> RedisAdapterResult<StreamReadReply> {
        match self.read_group(keys, consumer, count, wait).await {
            Ok(reply) => Ok(reply),
            Err(err) if is_nogroup(&err) => {
                self.ensure_groups(queues, true).await?;
                // The healed read does not wait again: the caller's wait
                // budget was already spent on the first attempt.
                Ok(self
                    .read_group(keys, consumer, count, Duration::ZERO)
                    .await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Queue the commands that give one entry back to its stream.
    ///
    /// The delivered entry is acked and deleted first, so the pending entries
    /// list never holds a reference the worker no longer owns. The marker is
    /// rewritten, not deleted. The row is still un-claimed, so a republish of
    /// the same `scheduled_at` stays a no-op until the new entry is
    /// delivered.
    ///
    /// `due` moves the delivery time only. `reference.scheduled_at` keeps the
    /// row's due time, which is what contract C1 compares against.
    fn push_requeue(
        &self,
        pipe: &mut redis::Pipeline,
        handle: &str,
        reference: &DispatchRef,
        due: DateTime<Utc>,
    ) -> RedisAdapterResult<()> {
        let queue = &reference.queue_name;
        let key = self.stream_key(queue);
        let entry_id = handle_entry_id(handle);
        let payload = serde_json::to_string(reference)?;
        let task_id = reference.task_id.to_string();
        pipe.xack(&key, &self.config.consumer_group, &[entry_id])
            .ignore()
            .xdel(&key, &[entry_id])
            .ignore();
        if due <= Utc::now() {
            pipe.xadd(&key, "*", &[(PAYLOAD_FIELD, payload.as_str())])
                .ignore();
        } else {
            pipe.zadd(self.delayed_key(queue), &task_id, due.timestamp_millis())
                .ignore()
                .hset(self.payloads_key(queue), &task_id, payload.as_str())
                .ignore();
        }
        pipe.cmd("SET")
            .arg(self.marker_key(reference.task_id))
            .arg(reference.marker_value())
            .arg("EX")
            .arg(self.dedupe_ttl_secs())
            .ignore();
        Ok(())
    }

    /// Give one entry back to its stream, as a fresh entry due at `due`.
    async fn requeue(
        &self,
        handle: &str,
        reference: &DispatchRef,
        due: DateTime<Utc>,
    ) -> RedisAdapterResult<()> {
        let entries = [(handle.to_string(), reference.clone())];
        self.requeue_batch(&entries, due).await
    }

    /// Give several entries back to their streams in one round trip.
    async fn requeue_batch(
        &self,
        entries: &[(String, DispatchRef)],
        due: DateTime<Utc>,
    ) -> RedisAdapterResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (handle, reference) in entries {
            self.push_requeue(&mut pipe, handle, reference, due)?;
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn publish_inner(&self, hints: &[DispatchHint]) -> RedisAdapterResult<()> {
        if hints.is_empty() {
            return Ok(());
        }
        let mut by_queue: HashMap<&str, Vec<&DispatchHint>> = HashMap::new();
        for hint in hints {
            validate_queue_name(&hint.queue_name)?;
            by_queue
                .entry(hint.queue_name.as_str())
                .or_default()
                .push(hint);
        }

        let now_ms = Utc::now().timestamp_millis();
        let ttl = self.dedupe_ttl_secs();
        for (queue, batch) in by_queue {
            // The group must exist before the first entry lands on the stream.
            self.ensure_group(queue, false).await?;
            let mut invocation = self.publish_script.prepare_invoke();
            invocation
                .key(self.stream_key(queue))
                .key(self.delayed_key(queue))
                .key(self.payloads_key(queue));
            for hint in &batch {
                invocation.key(self.marker_key(hint.task_id));
            }
            invocation.arg(now_ms).arg(ttl);
            for hint in &batch {
                let payload = serde_json::to_string(&DispatchRef::from_hint(hint))?;
                invocation
                    .arg(hint.task_id.to_string())
                    .arg(hint.scheduled_at.timestamp_millis())
                    .arg(payload);
            }
            let mut conn = self.conn.clone();
            let _: i64 = invocation.invoke_async(&mut conn).await?;
        }
        Ok(())
    }

    async fn next_inner(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> RedisAdapterResult<Vec<DispatchLease>> {
        let keys: Vec<String> = queues.iter().map(|queue| self.stream_key(queue)).collect();
        // `COUNT` bounds one stream, not the whole read. Sizing it per stream
        // keeps the read close to `max` entries in total. The order rotates
        // per call, so the queue that fills the batch changes. No queue
        // therefore starves behind a busy peer.
        let offset = usize::try_from(self.reads.fetch_add(1, Ordering::Relaxed)).unwrap_or(0);
        let ordered = rotate(&keys, offset);
        let count = per_stream_count(max, ordered.len());
        let reply = self
            .read_with_heal(queues, &ordered, consumer, count, wait)
            .await?;

        let mut leases = Vec::new();
        let mut surplus = Vec::new();
        let mut malformed = Vec::new();
        for stream in reply.keys {
            let stream_key = stream.key;
            for entry in stream.ids {
                let Some(payload) = entry_payload(&entry.map) else {
                    tracing::warn!(
                        entry_id = %entry.id,
                        stream = %stream_key,
                        "discarding dispatch entry with no payload field"
                    );
                    malformed.push((stream_key.clone(), entry.id));
                    continue;
                };
                let Ok(reference) = serde_json::from_str::<DispatchRef>(&payload) else {
                    tracing::warn!(
                        entry_id = %entry.id,
                        stream = %stream_key,
                        "discarding dispatch entry with an unreadable payload"
                    );
                    malformed.push((stream_key.clone(), entry.id));
                    continue;
                };
                if leases.len() < max {
                    let handle = encode_handle(&entry.id, &payload);
                    leases.push(reference.into_lease(handle));
                } else {
                    surplus.push((entry.id, reference));
                }
            }
        }

        // The caller's `max` is its free concurrency, so a surplus goes back
        // on the stream at once rather than waiting for the visibility
        // timeout. One pipeline carries the whole surplus. A failure there
        // costs one redelivery per entry, which the visibility timeout already
        // covers, so the leases already collected are returned either way.
        if !surplus.is_empty()
            && let Err(error) = self.requeue_batch(&surplus, Utc::now()).await
        {
            tracing::warn!(
                error = %error,
                surplus = surplus.len(),
                "failed to requeue surplus dispatch references"
            );
        }

        if let Err(error) = self.discard_entries(&malformed).await {
            tracing::warn!(
                error = %error,
                malformed = malformed.len(),
                "failed to discard unreadable dispatch entries"
            );
        }
        Ok(leases)
    }

    /// Acknowledge and delete entries that carry no readable reference.
    ///
    /// `XREADGROUP` puts every delivered entry in the pending entries list. A
    /// consumer that only drops an unreadable entry leaves it there for good,
    /// because the entry never becomes a lease and so is never acked. The
    /// recovery pass then claims it on every sweep and leaves it pending again.
    /// `XPENDING` reads a fixed window of `RECOVER_BATCH` entries, so enough
    /// such entries hide every legitimate abandoned lease below them, and the
    /// crash recovery this channel promises stops working.
    ///
    /// The delete names each entry id, so nothing else leaves the stream. The
    /// dedupe marker is left alone: a reference that cannot be read does not
    /// say which task it belongs to.
    async fn discard_entries(&self, entries: &[(String, String)]) -> RedisAdapterResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        pipe.atomic();
        for (key, entry_id) in entries {
            pipe.xack(key, &self.config.consumer_group, &[entry_id])
                .ignore()
                .xdel(key, &[entry_id])
                .ignore();
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn ack_inner(&self, lease: &DispatchLease) -> RedisAdapterResult<()> {
        let key = self.stream_key(&lease.queue_name);
        let entry_id = handle_entry_id(&lease.handle);
        let mut conn = self.conn.clone();
        redis::pipe()
            .atomic()
            .xack(&key, &self.config.consumer_group, &[entry_id])
            .ignore()
            .xdel(&key, &[entry_id])
            .ignore()
            .del(self.marker_key(lease.task_id))
            .ignore()
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    async fn release_inner(
        &self,
        lease: &DispatchLease,
        delay: Duration,
    ) -> RedisAdapterResult<()> {
        // The handle carries the payload (contract C2), so no read-back is
        // needed and the priority survives the release.
        let mut reference = handle_payload(&lease.handle)
            .and_then(|payload| serde_json::from_str::<DispatchRef>(payload).ok())
            .unwrap_or_else(|| DispatchRef::from_lease(lease));
        reference.redeliveries = lease.redeliveries.saturating_add(1);
        let chrono_delay = chrono::Duration::from_std(delay).map_err(|err| {
            RedisAdapterError::DurationOutOfRange(format!("release delay: {err}"))
        })?;
        // `scheduled_at` stays the row's due time. Only the delivery time
        // moves, so a reconcile republish does not disturb the backoff (C1).
        self.requeue(&lease.handle, &reference, Utc::now() + chrono_delay)
            .await
    }

    /// Re-add every entry that has been idle in the pending entries list
    /// longer than the visibility timeout.
    async fn recover_queue(&self, queue_name: &str) -> RedisAdapterResult<usize> {
        self.ensure_group(queue_name, false).await?;
        let key = self.stream_key(queue_name);
        let mut conn = self.conn.clone();

        let pending: StreamPendingCountReply = conn
            .xpending_count(&key, &self.config.consumer_group, "-", "+", RECOVER_BATCH)
            .await?;
        if pending.ids.is_empty() {
            return Ok(0);
        }
        let visibility_ms = self.visibility_ms();
        let threshold = usize::try_from(visibility_ms).unwrap_or(usize::MAX);
        let idle: Vec<String> = pending
            .ids
            .iter()
            .filter(|entry| entry.last_delivered_ms >= threshold)
            .map(|entry| entry.id.clone())
            .collect();
        if idle.is_empty() {
            return Ok(0);
        }

        // XCLAIM moves the entries to a sentinel consumer so their payloads
        // can be read. `XREADGROUP >` never returns a pending entry, so the
        // only way to make the work deliverable again is to re-add it.
        let claimed: StreamClaimReply = conn
            .xclaim(
                &key,
                &self.config.consumer_group,
                RECOVERY_CONSUMER,
                visibility_ms,
                &idle,
            )
            .await?;

        let mut recovered = Vec::new();
        let mut malformed = Vec::new();
        for entry in claimed.ids {
            let Some(payload) = entry_payload(&entry.map) else {
                tracing::warn!(
                    queue = %queue_name,
                    entry_id = %entry.id,
                    "discarding recovered entry with no payload field"
                );
                malformed.push((key.clone(), entry.id));
                continue;
            };
            let Ok(mut reference) = serde_json::from_str::<DispatchRef>(&payload) else {
                tracing::warn!(
                    queue = %queue_name,
                    entry_id = %entry.id,
                    "discarding recovered entry with an unreadable payload"
                );
                malformed.push((key.clone(), entry.id));
                continue;
            };
            reference.redeliveries = reference.redeliveries.saturating_add(1);
            // `scheduled_at` keeps the row's due time (C1). The entry is due
            // now, which the `due` argument below says.
            recovered.push((entry.id, reference));
        }
        // An entry the pass cannot read stays pending unless it is discarded
        // here. See [`RedisDispatch::discard_entries`].
        self.discard_entries(&malformed).await?;
        let count = recovered.len();
        self.requeue_batch(&recovered, Utc::now()).await?;
        Ok(count)
    }

    async fn recover_queues(&self, queues: &[String]) -> RedisAdapterResult<usize> {
        let mut total = 0;
        for queue in queues {
            total += self.recover_queue(queue).await?;
        }
        Ok(total)
    }
}

#[async_trait]
impl TaskDispatch for RedisDispatch {
    async fn publish(&self, hints: &[DispatchHint]) -> HarvestResult<()> {
        harvest(self.publish_inner(hints).await)
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> HarvestResult<Vec<DispatchLease>> {
        if queues.is_empty() || max == 0 {
            return Ok(Vec::new());
        }
        for queue in queues {
            harvest(validate_queue_name(queue))?;
        }
        harvest(self.promote_rate_limited(queues, wait).await)?;
        harvest(self.next_inner(queues, consumer, max, wait).await)
    }

    async fn ack(&self, lease: &DispatchLease) -> HarvestResult<()> {
        harvest(self.ack_inner(lease).await)
    }

    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()> {
        harvest(self.release_inner(lease, delay).await)
    }

    async fn maintain(&self, queues: &[String]) -> HarvestResult<DispatchMaintenance> {
        if queues.is_empty() {
            return Ok(DispatchMaintenance::default());
        }
        let promoted = harvest(self.promote_rate_limited(queues, Duration::ZERO).await)?;
        let recovered = if self.recovery_is_due() {
            harvest(self.recover_queues(queues).await)?
        } else {
            0
        };
        Ok(DispatchMaintenance {
            promoted,
            recovered,
        })
    }
}

/// Open one connection manager with the contract C4 timeouts.
async fn open_manager(client: &redis::Client) -> RedisAdapterResult<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(CONNECT_TIMEOUT)
        .set_response_timeout(RESPONSE_TIMEOUT);
    // The manager retries the first connection with its own backoff, so the
    // per-attempt timeout alone does not bound this call. The outer deadline
    // does. A black-holed address therefore fails in about `CONNECT_TIMEOUT`
    // rather than after the whole retry budget.
    match tokio::time::timeout(
        CONNECT_TIMEOUT,
        ConnectionManager::new_with_config(client.clone(), config),
    )
    .await
    {
        Ok(result) => Ok(result?),
        Err(_) => Err(RedisAdapterError::ConnectTimeout(CONNECT_TIMEOUT)),
    }
}

/// Whether this build carries the crate's `tls` feature.
const TLS_ENABLED: bool = cfg!(feature = "tls");

/// Whether a URL asks for TLS.
fn is_tls_url(url: &str) -> bool {
    url.trim_start()
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("rediss"))
}

/// Reject a TLS URL when the crate is built without the `tls` feature.
///
/// `redis` only speaks TLS when its own TLS feature is on. Without it a
/// `rediss://` URL fails deep inside the client with a message that does not
/// name the cause. This check names the feature instead.
fn check_tls_support(url: &str) -> RedisAdapterResult<()> {
    if is_tls_url(url) && !TLS_ENABLED {
        return Err(RedisAdapterError::TlsUnavailable);
    }
    Ok(())
}

/// Reject a queue name that cannot be part of a key.
///
/// `:` separates the segments of every key in the family, so a queue name that
/// holds one can address another queue's keys.
fn validate_queue_name(queue_name: &str) -> RedisAdapterResult<()> {
    if queue_name.is_empty() || queue_name.contains(':') {
        return Err(RedisAdapterError::InvalidQueueName(queue_name.to_string()));
    }
    Ok(())
}

/// `COUNT` for one stream of a read that wants `max` entries in total.
fn per_stream_count(max: usize, queues: usize) -> usize {
    if queues == 0 {
        return max.max(1);
    }
    max.div_ceil(queues).max(1)
}

/// `items`, rotated left by `offset` positions.
fn rotate<T: Clone>(items: &[T], offset: usize) -> Vec<T> {
    if items.is_empty() {
        return Vec::new();
    }
    let start = offset % items.len();
    items[start..]
        .iter()
        .chain(items[..start].iter())
        .cloned()
        .collect()
}

/// Build the opaque lease handle from an entry id and its payload.
///
/// Carrying the payload lets `release` rebuild the reference without an
/// `XRANGE` read-back (contract C2).
fn encode_handle(entry_id: &str, payload: &str) -> String {
    format!("{entry_id}{HANDLE_SEPARATOR}{payload}")
}

/// The stream entry id inside a lease handle.
fn handle_entry_id(handle: &str) -> &str {
    handle
        .split_once(HANDLE_SEPARATOR)
        .map_or(handle, |(entry_id, _)| entry_id)
}

/// The stored payload inside a lease handle, if it carries one.
fn handle_payload(handle: &str) -> Option<&str> {
    handle
        .split_once(HANDLE_SEPARATOR)
        .map(|(_, payload)| payload)
}

/// Map an adapter result onto the engine's dispatch result.
///
/// The worker treats any dispatch error as a signal to fall back to the
/// Postgres claim path. The message is diagnostic only.
fn harvest<T>(result: RedisAdapterResult<T>) -> HarvestResult<T> {
    result.map_err(|err| HarvestError::Dispatch(err.to_string()))
}

/// Claim a rate-limited slot, returning whether the caller may run the pass.
///
/// The compare and exchange makes exactly one of several concurrent callers
/// win, so a busy worker never runs the same pass twice at once.
fn claim_rate_limit_slot(slot: &AtomicI64, now_ms: i64, interval_ms: i64) -> bool {
    let last = slot.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) < interval_ms {
        return false;
    }
    slot.compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Relaxed)
        .is_ok()
}

/// Read the `payload` field of a stream entry.
fn entry_payload(map: &HashMap<String, redis::Value>) -> Option<String> {
    match map.get(PAYLOAD_FIELD)? {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok().map(ToString::to_string),
        redis::Value::SimpleString(text) => Some(text.clone()),
        _ => None,
    }
}

/// Whether a Redis error reports a missing consumer group.
fn is_nogroup(err: &RedisError) -> bool {
    err.code() == Some("NOGROUP")
        || err
            .detail()
            .is_some_and(|detail| detail.contains("NOGROUP"))
}

/// Lua script that publishes a batch of references for one queue.
///
/// Keys:
/// - `KEYS[1]`: the queue's dispatch stream.
/// - `KEYS[2]`: the queue's delayed sorted set.
/// - `KEYS[3]`: the queue's delayed payload hash.
/// - `KEYS[4..]`: one dedupe marker per hint, in hint order.
///
/// Arguments:
/// - `ARGV[1]`: now, in unix milliseconds.
/// - `ARGV[2]`: dedupe marker TTL, in seconds.
/// - `ARGV[3n..]`: task id, due time in unix milliseconds, and payload, per
///   hint, in hint order.
///
/// Behaviour per hint (contract C1). The marker holds the `scheduled_at` of
/// the reference the channel already carries. A hint whose due time equals the
/// marker is a no-op, and the marker TTL is refreshed. Any other hint writes.
/// The marker then takes the new due time. A parked entry moves in place, and
/// a due hint is added to the stream. Returns the number of hints that wrote.
const PUBLISH_LUA: &str = r"
local stream = KEYS[1]
local delayed = KEYS[2]
local payloads = KEYS[3]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local written = 0
local count = (#ARGV - 2) / 3
for i = 1, count do
    local marker = KEYS[3 + i]
    local task_id = ARGV[3 * i]
    local due_text = ARGV[3 * i + 1]
    local payload = ARGV[3 * i + 2]
    local held = redis.call('GET', marker)
    if held == due_text then
        redis.call('EXPIRE', marker, ttl)
    else
        local due = tonumber(due_text)
        redis.call('SET', marker, due_text, 'EX', ttl)
        if due <= now then
            redis.call('ZREM', delayed, task_id)
            redis.call('HDEL', payloads, task_id)
            redis.call('XADD', stream, '*', 'payload', payload)
        else
            redis.call('ZADD', delayed, due, task_id)
            redis.call('HSET', payloads, task_id, payload)
        end
        written = written + 1
    end
end
return written
";

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(redeliveries: u32) -> DispatchLease {
        DispatchLease {
            task_id: Uuid::nil(),
            queue_name: "default".to_string(),
            redeliveries,
            handle: "1-0".to_string(),
            shard: Some(ShardId::new(3)),
        }
    }

    #[test]
    fn default_config_uses_the_documented_constants() {
        let config = RedisDispatchConfig::default();
        assert_eq!(config.key_prefix, DEFAULT_KEY_PREFIX);
        assert_eq!(config.consumer_group, DEFAULT_CONSUMER_GROUP);
        assert_eq!(config.visibility_timeout, DEFAULT_VISIBILITY_TIMEOUT);
        assert_eq!(config.dedupe_ttl, DEFAULT_DEDUPE_TTL);
    }

    #[test]
    fn publish_script_compiles() {
        let _ = Script::new(PUBLISH_LUA);
    }

    #[test]
    fn a_reference_round_trips_through_its_payload() {
        let hint = DispatchHint {
            task_id: Uuid::new_v4(),
            queue_name: "email".to_string(),
            scheduled_at: Utc::now(),
            priority: 7,
            shard: Some(ShardId::new(2)),
        };
        let payload = serde_json::to_string(&DispatchRef::from_hint(&hint)).unwrap();
        let decoded: DispatchRef = serde_json::from_str(&payload).unwrap();
        assert_eq!(decoded.task_id, hint.task_id);
        assert_eq!(decoded.queue_name, hint.queue_name);
        assert_eq!(decoded.priority, 7);
        assert_eq!(decoded.redeliveries, 0);
        assert_eq!(decoded.shard, Some(2));
    }

    /// Finding F7 (issue #1312 review round 1). The payload carries the pool
    /// the reference needs, so a worker weighs it before it claims.
    #[test]
    fn a_payload_carries_the_reference_kind() {
        let hint = DispatchHint {
            task_id: Uuid::new_v4(),
            queue_name: "email".to_string(),
            scheduled_at: Utc::now(),
            priority: 0,
            shard: None,
            kind: Some(DispatchKind::Activity),
        };
        let payload = serde_json::to_string(&DispatchRef::from_hint(&hint)).unwrap();
        let decoded: DispatchRef = serde_json::from_str(&payload).unwrap();
        assert_eq!(decoded.kind, Some(DispatchKind::Activity));
        assert_eq!(
            decoded.into_lease("1-0".to_string()).kind,
            Some(DispatchKind::Activity),
            "the lease must name the pool the reference needs"
        );
    }

    /// An entry written before the kind existed still parses.
    #[test]
    fn a_payload_without_a_kind_is_untyped() {
        let payload = format!(
            r#"{{"task_id":"{}","queue_name":"q","scheduled_at":"2026-09-07T12:00:00Z"}}"#,
            Uuid::nil()
        );
        let decoded: DispatchRef = serde_json::from_str(&payload).expect("an old payload parses");
        assert_eq!(decoded.kind, None, "an old entry carries no kind");
    }

    #[test]
    fn a_payload_stores_the_due_time_as_rfc_3339() {
        let hint = DispatchHint {
            task_id: Uuid::nil(),
            queue_name: "q".to_string(),
            scheduled_at: DateTime::parse_from_rfc3339("2026-09-07T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            priority: 0,
            shard: None,
        };
        let payload = serde_json::to_string(&DispatchRef::from_hint(&hint)).unwrap();
        assert!(
            payload.contains("2026-09-07T12:00:00Z"),
            "payload must carry an RFC 3339 due time: {payload}"
        );
    }

    #[test]
    fn a_marker_value_is_the_due_time_in_milliseconds() {
        let hint = DispatchHint {
            task_id: Uuid::nil(),
            queue_name: "q".to_string(),
            scheduled_at: DateTime::parse_from_rfc3339("2026-09-07T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            priority: 0,
            shard: None,
        };
        // The publish script compares the marker against the same encoding of
        // `scheduled_at` that a hint carries, so the two must agree.
        assert_eq!(
            DispatchRef::from_hint(&hint).marker_value(),
            hint.scheduled_at.timestamp_millis()
        );
    }

    #[test]
    fn a_lease_keeps_the_shard_slot() {
        let hint = DispatchHint {
            task_id: Uuid::nil(),
            queue_name: "q".to_string(),
            scheduled_at: Utc::now(),
            priority: 0,
            shard: Some(ShardId::new(5)),
        };
        let built = DispatchRef::from_hint(&hint).into_lease("9-1".to_string());
        assert_eq!(built.shard, Some(ShardId::new(5)));
        assert_eq!(built.handle, "9-1");
    }

    #[test]
    fn a_reference_rebuilt_from_a_lease_keeps_its_identity() {
        let rebuilt = DispatchRef::from_lease(&lease(4));
        assert_eq!(rebuilt.task_id, Uuid::nil());
        assert_eq!(rebuilt.queue_name, "default");
        assert_eq!(rebuilt.redeliveries, 4);
        assert_eq!(rebuilt.shard, Some(3));
    }

    #[test]
    fn a_handle_carries_the_entry_id_and_the_payload() {
        let hint = DispatchHint {
            task_id: Uuid::new_v4(),
            queue_name: "email".to_string(),
            scheduled_at: Utc::now(),
            priority: 9,
            shard: None,
        };
        let payload = serde_json::to_string(&DispatchRef::from_hint(&hint)).unwrap();
        let handle = encode_handle("1700000000000-3", &payload);
        assert_eq!(handle_entry_id(&handle), "1700000000000-3");
        let decoded: DispatchRef =
            serde_json::from_str(handle_payload(&handle).expect("a payload")).unwrap();
        assert_eq!(decoded.task_id, hint.task_id);
        assert_eq!(decoded.priority, 9, "release must keep the priority");
    }

    #[test]
    fn a_handle_with_no_payload_is_still_an_entry_id() {
        assert_eq!(handle_entry_id("5-0"), "5-0");
        assert!(handle_payload("5-0").is_none());
    }

    #[test]
    fn a_payload_that_holds_the_separator_survives_the_split() {
        let payload = r#"{"queue_name":"a|b"}"#;
        let handle = encode_handle("7-1", payload);
        assert_eq!(handle_entry_id(&handle), "7-1");
        assert_eq!(handle_payload(&handle), Some(payload));
    }

    #[test]
    fn a_payload_field_is_read_from_either_string_shape() {
        let mut map = HashMap::new();
        map.insert(
            PAYLOAD_FIELD.to_string(),
            redis::Value::BulkString(b"bulk".to_vec()),
        );
        assert_eq!(entry_payload(&map).as_deref(), Some("bulk"));

        let mut map = HashMap::new();
        map.insert(
            PAYLOAD_FIELD.to_string(),
            redis::Value::SimpleString("simple".to_string()),
        );
        assert_eq!(entry_payload(&map).as_deref(), Some("simple"));

        assert!(entry_payload(&HashMap::new()).is_none());
    }

    #[test]
    fn nogroup_is_recognised_by_its_code() {
        // Build the error from the wire form the server sends, so the test
        // exercises the same `ErrorRepr` a real reply produces.
        let reply = redis::parse_redis_value(b"-NOGROUP No such consumer group\r\n")
            .expect("an error reply must parse");
        let err = reply
            .extract_error()
            .expect_err("an error reply must extract into a RedisError");
        assert_eq!(err.code(), Some("NOGROUP"));
        assert!(is_nogroup(&err));
    }

    #[test]
    fn a_rate_limit_slot_admits_one_caller_per_interval() {
        let slot = AtomicI64::new(0);
        assert!(
            claim_rate_limit_slot(&slot, 1_000, 500),
            "the first pass always runs"
        );
        assert!(
            !claim_rate_limit_slot(&slot, 1_400, 500),
            "a second pass inside the interval is skipped"
        );
        assert!(
            claim_rate_limit_slot(&slot, 1_500, 500),
            "a pass at the interval runs again"
        );
    }

    #[test]
    fn a_zero_interval_never_rate_limits() {
        let slot = AtomicI64::new(0);
        assert!(claim_rate_limit_slot(&slot, 10, 0));
        assert!(claim_rate_limit_slot(&slot, 10, 0));
    }

    #[test]
    fn an_adapter_error_maps_onto_the_dispatch_error() {
        let err = RedisAdapterError::InvalidQueueName("bad name".to_string());
        let mapped = harvest::<()>(Err(err)).expect_err("an error must stay an error");
        match mapped {
            HarvestError::Dispatch(message) => {
                assert!(
                    message.contains("bad name"),
                    "the message must carry the cause: {message}"
                );
            }
            other => panic!("expected a dispatch error, got {other:?}"),
        }
    }

    #[test]
    fn another_error_is_not_read_as_nogroup() {
        let other = RedisError::from((redis::ErrorKind::IoError, "connection reset"));
        assert!(
            !is_nogroup(&other),
            "a transport failure must not trigger a group heal"
        );
    }

    #[test]
    fn a_read_is_sized_per_stream() {
        assert_eq!(per_stream_count(64, 1), 64);
        assert_eq!(per_stream_count(64, 4), 16);
        assert_eq!(per_stream_count(3, 2), 2, "the split rounds up");
        assert_eq!(per_stream_count(1, 8), 1, "a stream always reads one");
        assert_eq!(per_stream_count(0, 0), 1, "no queue still reads one");
    }

    #[test]
    fn the_queue_order_rotates() {
        let queues = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(rotate(&queues, 0), vec!["a", "b", "c"]);
        assert_eq!(rotate(&queues, 1), vec!["b", "c", "a"]);
        assert_eq!(rotate(&queues, 2), vec!["c", "a", "b"]);
        assert_eq!(rotate(&queues, 3), vec!["a", "b", "c"], "the offset wraps");
        assert!(rotate::<String>(&[], 5).is_empty());
    }

    #[test]
    fn a_queue_name_with_a_colon_is_rejected() {
        assert!(validate_queue_name("default").is_ok());
        for bad in ["", "a:b", ":", "harvest:dispatch"] {
            let err = validate_queue_name(bad).expect_err("a bad name must be rejected");
            assert!(
                matches!(err, RedisAdapterError::InvalidQueueName(_)),
                "unexpected error for {bad:?}: {err}"
            );
        }
    }

    #[test]
    fn an_empty_prefix_or_group_is_rejected() {
        let empty_prefix = RedisDispatchConfig {
            key_prefix: "  ".to_string(),
            ..RedisDispatchConfig::default()
        };
        assert!(matches!(
            empty_prefix.validate(),
            Err(RedisAdapterError::InvalidConfig(_))
        ));
        let empty_group = RedisDispatchConfig {
            consumer_group: String::new(),
            ..RedisDispatchConfig::default()
        };
        assert!(matches!(
            empty_group.validate(),
            Err(RedisAdapterError::InvalidConfig(_))
        ));
        assert!(RedisDispatchConfig::default().validate().is_ok());
    }

    #[test]
    fn a_tls_url_is_recognised_by_its_scheme() {
        assert!(is_tls_url("rediss://host:6379"));
        assert!(is_tls_url("REDISS://host:6379"));
        assert!(!is_tls_url("redis://host:6379"));
        assert!(!is_tls_url("host:6379"));
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn a_tls_url_is_rejected_without_the_tls_feature() {
        let err = check_tls_support("rediss://host:6379").expect_err("TLS must be rejected");
        assert!(
            matches!(err, RedisAdapterError::TlsUnavailable),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string().contains("tls"),
            "the message must name the feature: {err}"
        );
        assert!(check_tls_support("redis://host:6379").is_ok());
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn connect_rejects_a_tls_url_without_the_tls_feature() {
        let err = RedisDispatch::connect("rediss://127.0.0.1:6379", RedisDispatchConfig::default())
            .await
            .expect_err("TLS must be rejected before any connection attempt");
        assert!(
            matches!(err, RedisAdapterError::TlsUnavailable),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn connect_rejects_an_empty_key_prefix() {
        let config = RedisDispatchConfig {
            key_prefix: String::new(),
            ..RedisDispatchConfig::default()
        };
        let err = RedisDispatch::connect("redis://127.0.0.1:6379", config)
            .await
            .expect_err("an empty prefix must be rejected before connecting");
        assert!(
            matches!(err, RedisAdapterError::InvalidConfig(_)),
            "unexpected error: {err}"
        );
    }
}
