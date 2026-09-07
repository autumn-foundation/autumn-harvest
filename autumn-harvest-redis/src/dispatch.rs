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
//! already holds a reference for the row, so a second hint for the same row
//! adds no second entry. `ack` deletes the marker, which is what lets the
//! reconcile sweep republish the row on its next pass. The marker also
//! expires after `dedupe_ttl`. A leaked marker therefore cannot block a
//! republish for ever. A worker that dies between the read and the ack
//! leaks one.
//!
//! ## The earlier-due override
//!
//! A hint whose due time is earlier than a parked entry's moves that entry
//! forward. A signal can arrive for a workflow that waits on a timer. The
//! row's `scheduled_at` then moves back to now, and the parked reference
//! must move with it. Without the override the marker would suppress the new
//! hint, and the run would wait for the original timer.
//!
//! ## Delivery
//!
//! Delivery is at least once. A duplicate reference is harmless: the by-id
//! claim finds the row already `RUNNING` or terminal and the worker acks the
//! reference without running anything.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use autumn_harvest::dispatch::{DispatchHint, DispatchLease, DispatchMaintenance, TaskDispatch};
use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::types::ShardId;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{
    StreamClaimReply, StreamPendingCountReply, StreamRangeReply, StreamReadOptions, StreamReadReply,
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
    /// Used when the delivered entry is gone from the stream, so the priority
    /// and the original due time cannot be read back. Priority degrades to the
    /// default, which the v1 design already treats as best effort.
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
    #[must_use]
    pub fn from_connection(
        conn: ConnectionManager,
        blocking: ConnectionManager,
        config: RedisDispatchConfig,
    ) -> Self {
        Self {
            conn,
            blocking,
            config: Arc::new(config),
            publish_script: Arc::new(Script::new(PUBLISH_LUA)),
            promote_script: Arc::new(Script::new(PROMOTE_LUA)),
            ensured: Arc::new(Mutex::new(HashSet::new())),
            last_promote_ms: Arc::new(AtomicI64::new(0)),
            last_recover_ms: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Open two Redis connections and build a channel.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::Redis`] if the URL cannot be parsed or a
    /// connection cannot be established.
    pub async fn connect(url: &str, config: RedisDispatchConfig) -> RedisAdapterResult<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client.clone()).await?;
        let blocking = ConnectionManager::new(client).await?;
        Ok(Self::from_connection(conn, blocking, config))
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

    /// Promote every due delayed reference for one queue onto its stream.
    async fn promote_queue(&self, queue_name: &str) -> RedisAdapterResult<usize> {
        // The group must exist before the script adds entries.
        self.ensure_group(queue_name, false).await?;
        let mut conn = self.conn.clone();
        let promoted: i64 = self
            .promote_script
            .key(self.delayed_key(queue_name))
            .key(self.payloads_key(queue_name))
            .key(self.stream_key(queue_name))
            .arg(Utc::now().timestamp_millis())
            .invoke_async(&mut conn)
            .await?;
        Ok(usize::try_from(promoted).unwrap_or(0))
    }

    async fn promote_queues(&self, queues: &[String]) -> RedisAdapterResult<usize> {
        let mut total = 0;
        for queue in queues {
            total += self.promote_queue(queue).await?;
        }
        Ok(total)
    }

    /// Run a promotion pass at most once per `interval`.
    ///
    /// A read happens on every poll, and a promotion costs one round trip per
    /// queue. The rate limit keeps an idle worker's cost proportional to the
    /// poll interval rather than to the number of reads.
    async fn promote_rate_limited(
        &self,
        queues: &[String],
        interval: Duration,
    ) -> HarvestResult<()> {
        let now = Utc::now().timestamp_millis();
        let interval_ms = i64::try_from(interval.as_millis()).unwrap_or(i64::MAX);
        let last = self.last_promote_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < interval_ms {
            return Ok(());
        }
        if self
            .last_promote_ms
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // Another task took this pass.
            return Ok(());
        }
        self.promote_queues(queues)
            .await
            .map_err(|err| to_harvest(&err))?;
        Ok(())
    }

    /// Whether a recovery pass is due, claiming the slot when it is.
    fn recovery_is_due(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let interval_ms = i64::try_from(self.config.visibility_timeout.as_millis() / 2)
            .unwrap_or(i64::MAX)
            .max(1);
        let last = self.last_recover_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < interval_ms {
            return false;
        }
        self.last_recover_ms
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    async fn read_group(
        &self,
        keys: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> redis::RedisResult<StreamReadReply> {
        let mut options = StreamReadOptions::default()
            .group(&self.config.consumer_group, consumer)
            .count(max);
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
        max: usize,
        wait: Duration,
    ) -> RedisAdapterResult<StreamReadReply> {
        match self.read_group(keys, consumer, max, wait).await {
            Ok(reply) => Ok(reply),
            Err(err) if is_nogroup(&err) => {
                self.ensure_groups(queues, true).await?;
                // The healed read does not wait again: the caller's wait
                // budget was already spent on the first attempt.
                Ok(self.read_group(keys, consumer, max, Duration::ZERO).await?)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Give one entry back to its stream, as a fresh entry due at `due`.
    ///
    /// The delivered entry is acked and deleted first, so the pending entries
    /// list never holds a reference the worker no longer owns. The marker's
    /// TTL is refreshed, not deleted: the row is still un-claimed, so a
    /// republish must stay a no-op until the new entry is delivered.
    async fn requeue(
        &self,
        handle: &str,
        reference: &DispatchRef,
        due: DateTime<Utc>,
    ) -> RedisAdapterResult<()> {
        let queue = &reference.queue_name;
        let key = self.stream_key(queue);
        let payload = serde_json::to_string(reference)?;
        let task_id = reference.task_id.to_string();
        let mut pipe = redis::pipe();
        pipe.atomic()
            .xack(&key, &self.config.consumer_group, &[handle])
            .ignore()
            .xdel(&key, &[handle])
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
            .arg("1")
            .arg("EX")
            .arg(self.dedupe_ttl_secs())
            .ignore();
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    /// Read the stored reference of a delivered entry.
    ///
    /// Returns `None` when the entry is gone, which happens if a peer's
    /// recovery pass already re-added it.
    async fn read_entry(
        &self,
        queue: &str,
        handle: &str,
    ) -> RedisAdapterResult<Option<DispatchRef>> {
        let mut conn = self.conn.clone();
        let reply: StreamRangeReply = conn.xrange(self.stream_key(queue), handle, handle).await?;
        let Some(entry) = reply.ids.first() else {
            return Ok(None);
        };
        let Some(payload) = entry_payload(&entry.map) else {
            return Ok(None);
        };
        Ok(serde_json::from_str(&payload).ok())
    }

    async fn publish_inner(&self, hints: &[DispatchHint]) -> RedisAdapterResult<()> {
        if hints.is_empty() {
            return Ok(());
        }
        let mut by_queue: HashMap<&str, Vec<&DispatchHint>> = HashMap::new();
        for hint in hints {
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
        let reply = self
            .read_with_heal(queues, &keys, consumer, max, wait)
            .await?;

        let mut leases = Vec::new();
        let mut surplus = Vec::new();
        for stream in reply.keys {
            for entry in stream.ids {
                let Some(payload) = entry_payload(&entry.map) else {
                    tracing::warn!(
                        entry_id = %entry.id,
                        "dropping dispatch entry with no payload field"
                    );
                    continue;
                };
                let Ok(reference) = serde_json::from_str::<DispatchRef>(&payload) else {
                    tracing::warn!(
                        entry_id = %entry.id,
                        "dropping dispatch entry with an unreadable payload"
                    );
                    continue;
                };
                if leases.len() < max {
                    leases.push(reference.into_lease(entry.id));
                } else {
                    surplus.push((entry.id, reference));
                }
            }
        }

        // `COUNT` bounds one stream, not the whole read, so a read across
        // several queues can return more than the caller asked for. The
        // caller's `max` is its free concurrency, so the surplus goes back on
        // the stream at once rather than waiting for the visibility timeout.
        for (handle, reference) in surplus {
            self.requeue(&handle, &reference, Utc::now()).await?;
        }
        Ok(leases)
    }

    async fn ack_inner(&self, lease: &DispatchLease) -> RedisAdapterResult<()> {
        let key = self.stream_key(&lease.queue_name);
        let mut conn = self.conn.clone();
        redis::pipe()
            .atomic()
            .xack(&key, &self.config.consumer_group, &[lease.handle.as_str()])
            .ignore()
            .xdel(&key, &[lease.handle.as_str()])
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
        let stored = self.read_entry(&lease.queue_name, &lease.handle).await?;
        let mut reference = stored.unwrap_or_else(|| DispatchRef::from_lease(lease));
        reference.redeliveries = lease.redeliveries.saturating_add(1);
        let chrono_delay = chrono::Duration::from_std(delay).map_err(|err| {
            RedisAdapterError::DurationOutOfRange(format!("release delay: {err}"))
        })?;
        let due = Utc::now() + chrono_delay;
        reference.scheduled_at = due;
        self.requeue(&lease.handle, &reference, due).await
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

        let mut recovered = 0;
        for entry in claimed.ids {
            let Some(payload) = entry_payload(&entry.map) else {
                tracing::warn!(
                    queue = %queue_name,
                    entry_id = %entry.id,
                    "dropping recovered entry with no payload field"
                );
                continue;
            };
            let Ok(mut reference) = serde_json::from_str::<DispatchRef>(&payload) else {
                tracing::warn!(
                    queue = %queue_name,
                    entry_id = %entry.id,
                    "dropping recovered entry with an unreadable payload"
                );
                continue;
            };
            reference.redeliveries = reference.redeliveries.saturating_add(1);
            let now = Utc::now();
            reference.scheduled_at = now;
            self.requeue(&entry.id, &reference, now).await?;
            recovered += 1;
        }
        Ok(recovered)
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
        self.publish_inner(hints)
            .await
            .map_err(|err| to_harvest(&err))
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
        self.promote_rate_limited(queues, wait).await?;
        self.next_inner(queues, consumer, max, wait)
            .await
            .map_err(|err| to_harvest(&err))
    }

    async fn ack(&self, lease: &DispatchLease) -> HarvestResult<()> {
        self.ack_inner(lease).await.map_err(|err| to_harvest(&err))
    }

    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()> {
        self.release_inner(lease, delay)
            .await
            .map_err(|err| to_harvest(&err))
    }

    async fn maintain(&self, queues: &[String]) -> HarvestResult<DispatchMaintenance> {
        if queues.is_empty() {
            return Ok(DispatchMaintenance::default());
        }
        let promoted = self
            .promote_queues(queues)
            .await
            .map_err(|err| to_harvest(&err))?;
        let recovered = if self.recovery_is_due() {
            self.recover_queues(queues)
                .await
                .map_err(|err| to_harvest(&err))?
        } else {
            0
        };
        Ok(DispatchMaintenance {
            promoted,
            recovered,
        })
    }
}

/// Map an adapter error onto the engine's dispatch error.
///
/// The worker treats any dispatch error as a signal to fall back to the
/// Postgres claim path. The message is diagnostic only.
fn to_harvest(err: &RedisAdapterError) -> HarvestError {
    HarvestError::Dispatch(err.to_string())
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
/// Behaviour per hint: a hint whose task id is parked in the delayed set with
/// a later due time moves forward. A hint whose marker exists otherwise is a
/// no-op. Any other hint sets the marker and is added to the stream when it is
/// due, or to the delayed set when it is not. Returns the number of hints that
/// wrote.
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
    local due = tonumber(ARGV[3 * i + 1])
    local payload = ARGV[3 * i + 2]
    local parked = redis.call('ZSCORE', delayed, task_id)
    local write = false
    if parked then
        if due < tonumber(parked) then
            write = true
        end
    elseif redis.call('EXISTS', marker) == 0 then
        write = true
    end
    if write then
        redis.call('SET', marker, '1', 'EX', ttl)
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
    fn another_error_is_not_read_as_nogroup() {
        let other = RedisError::from((redis::ErrorKind::IoError, "connection reset"));
        assert!(
            !is_nogroup(&other),
            "a transport failure must not trigger a group heal"
        );
    }
}
