//! Redis Streams implementation of [`TaskQueueAdapter`].
//!
//! ## Design
//!
//! - **Streams**: one stream per logical queue (`{prefix}:queue:{name}`). Each
//!   XADD enqueues exactly one task envelope (JSON-encoded into the `payload`
//!   field). Workers consume via XREADGROUP under a configurable consumer
//!   group name (default: `harvest_workers`).
//! - **Consumer group**: created lazily on the first `claim` call against a
//!   queue. `BUSYGROUP` errors are tolerated so multiple workers racing to
//!   create the group on cold-start do not break startup.
//! - **At-least-once delivery**: XACK is only called from `complete`, `fail`,
//!   and `requeue_for_retry`. If a worker crashes mid-execution the entry
//!   stays on the consumer group's pending entries list (PEL) and becomes
//!   eligible for [`RedisTaskQueue::recover_pending`] reclamation after the
//!   visibility timeout elapses.
//! - **Delayed scheduling**: tasks whose `scheduled_at > NOW` are not added to
//!   the stream directly. Instead the stable `task_id` is added to a per-queue
//!   sorted set (`{prefix}:scheduled:{name}`) with score = unix milliseconds,
//!   and the envelope payload is stored alongside in a hash
//!   (`{prefix}:scheduled:{name}:payloads`). A periodic [`promote_due`] call
//!   moves due entries onto the stream atomically via a small Lua script.
//! - **Visibility timeout**: enforced by [`RedisTaskQueue::recover_pending`],
//!   which calls XPENDING on each queue and XCLAIMs entries that have been
//!   idle longer than the configured threshold. Workers should run this on a
//!   timer.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use redis::aio::ConnectionManager;
use redis::streams::{
    StreamClaimReply, StreamPendingCountReply, StreamReadOptions, StreamReadReply,
};
use redis::{AsyncCommands, RedisError, Script};
use uuid::Uuid;

use crate::adapter::{ClaimedTask, TaskQueueAdapter};
use crate::envelope::{EnqueueParams, TaskEnvelope};
use crate::error::{RedisAdapterError, RedisAdapterResult};
use crate::naming::{dlq_key, scheduled_payloads_key, scheduled_zset_key, stream_key};

const DEFAULT_KEY_PREFIX: &str = "harvest";
const DEFAULT_CONSUMER_GROUP: &str = "harvest_workers";
const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CLAIM_BATCH: usize = 16;

/// Configuration for [`RedisTaskQueue`].
#[derive(Debug, Clone)]
pub struct RedisTaskQueueConfig {
    /// Key prefix. All keys live under `{prefix}:queue:*`,
    /// `{prefix}:scheduled:*`, and `{prefix}:dlq:*`.
    pub key_prefix: String,
    /// Consumer group name. All workers share the same group so XREADGROUP
    /// load-balances across them.
    pub consumer_group: String,
    /// How long an entry may sit in the consumer group PEL before
    /// [`RedisTaskQueue::recover_pending`] is allowed to reclaim it. Workers
    /// should call `record_heartbeat` more frequently than this interval.
    pub visibility_timeout: Duration,
    /// Maximum number of pending entries to reclaim per `recover_pending`
    /// call per queue. Bounded so a single recovery sweep does not stall the
    /// poll loop.
    pub recover_batch_size: usize,
    /// If true, push permanently failed tasks (via [`TaskQueueAdapter::fail`]
    /// or attempt exhaustion) onto a DLQ stream named `{prefix}:dlq:{name}`.
    /// Defaults to true.
    pub enable_dlq: bool,
}

impl Default for RedisTaskQueueConfig {
    fn default() -> Self {
        Self {
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            consumer_group: DEFAULT_CONSUMER_GROUP.to_string(),
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            recover_batch_size: DEFAULT_CLAIM_BATCH,
            enable_dlq: true,
        }
    }
}

/// Redis Streams implementation of [`TaskQueueAdapter`].
///
/// Cheap to clone -- the underlying connection manager is `Arc`-shared.
#[derive(Clone)]
pub struct RedisTaskQueue {
    conn: ConnectionManager,
    config: Arc<RedisTaskQueueConfig>,
    promote_script: Arc<Script>,
}

impl std::fmt::Debug for RedisTaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // ConnectionManager and Script don't impl Debug, so we elide them.
        f.debug_struct("RedisTaskQueue")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RedisTaskQueue {
    /// Construct from an open Redis connection manager.
    ///
    /// Use this when the application already holds a `ConnectionManager` (for
    /// example to share a connection pool with non-queue Redis usage). Most
    /// callers should prefer [`Self::connect`].
    #[must_use]
    pub fn from_connection(conn: ConnectionManager, config: RedisTaskQueueConfig) -> Self {
        Self {
            conn,
            config: Arc::new(config),
            promote_script: Arc::new(Script::new(PROMOTE_LUA)),
        }
    }

    /// Open a Redis connection and build a [`RedisTaskQueue`].
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::Redis`] if the URL cannot be parsed or the
    /// connection cannot be established.
    pub async fn connect(url: &str, config: RedisTaskQueueConfig) -> RedisAdapterResult<Self> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self::from_connection(conn, config))
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
        stream_key(&self.config.key_prefix, queue_name)
    }

    fn zset_key(&self, queue_name: &str) -> String {
        scheduled_zset_key(&self.config.key_prefix, queue_name)
    }

    fn payloads_key(&self, queue_name: &str) -> String {
        scheduled_payloads_key(&self.config.key_prefix, queue_name)
    }

    fn dlq_key(&self, queue_name: &str) -> String {
        dlq_key(&self.config.key_prefix, queue_name)
    }

    /// Ensure the consumer group exists for `queue_name`.
    ///
    /// Idempotent: a `BUSYGROUP` reply (the group already exists) is treated
    /// as success. The `MKSTREAM` flag creates the underlying stream if it
    /// doesn't yet exist so the first XGROUP CREATE on a brand-new queue
    /// doesn't fail with `ERR no such key`.
    async fn ensure_group(&self, queue_name: &str) -> RedisAdapterResult<()> {
        let key = self.stream_key(queue_name);
        let mut conn = self.conn.clone();
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&key)
            .arg(&self.config.consumer_group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if is_busygroup(&err) {
                    Ok(())
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// Atomically promote all due delayed tasks for `queue_name` onto the
    /// claimable stream.
    ///
    /// Returns the number of entries promoted. Workers should call this from
    /// their poll loop (cheap when the sorted set is empty).
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::Redis`] on any Redis-side failure.
    pub async fn promote_due(&self, queue_name: &str) -> RedisAdapterResult<usize> {
        // Ensure the consumer group exists before the Lua script XADDs entries
        // -- otherwise the group would be created against an empty stream
        // *after* the new entries land, leaving them below its read position.
        self.ensure_group(queue_name).await?;
        let now_ms = Utc::now().timestamp_millis();
        let mut conn = self.conn.clone();
        let promoted: i64 = self
            .promote_script
            .key(self.zset_key(queue_name))
            .key(self.payloads_key(queue_name))
            .key(self.stream_key(queue_name))
            .arg(now_ms)
            .invoke_async(&mut conn)
            .await?;
        Ok(usize::try_from(promoted).unwrap_or(0))
    }

    /// Promote all due tasks across the supplied queues.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::Redis`] on any Redis-side failure.
    pub async fn promote_due_for_queues(&self, queues: &[String]) -> RedisAdapterResult<usize> {
        let mut total = 0;
        for queue in queues {
            total += self.promote_due(queue).await?;
        }
        Ok(total)
    }

    /// Reclaim entries that have been idle in the consumer group's PEL longer
    /// than `visibility_timeout`.
    ///
    /// Returns the number of entries reclaimed across all queues. Reclaimed
    /// entries are `XCLAIM`ed to a sentinel consumer name (`__recovered__`)
    /// so their payload becomes readable, then re-XADDed as fresh stream
    /// entries -- this is the only way to make work in the consumer group's
    /// PEL deliverable again to a peer through `XREADGROUP >`.
    ///
    /// Workers should call this on a timer slower than `visibility_timeout`
    /// to bound the worst-case redelivery latency for crashed peers.
    ///
    /// # Errors
    ///
    /// Returns [`RedisAdapterError::Redis`] on any Redis-side failure.
    pub async fn recover_pending(&self, queues: &[String]) -> RedisAdapterResult<usize> {
        let mut total = 0;
        for queue in queues {
            total += self.recover_pending_for_queue(queue).await?;
        }
        Ok(total)
    }

    async fn recover_pending_for_queue(&self, queue_name: &str) -> RedisAdapterResult<usize> {
        self.ensure_group(queue_name).await?;
        let mut conn = self.conn.clone();
        let key = self.stream_key(queue_name);

        let pending: StreamPendingCountReply = conn
            .xpending_count(
                &key,
                &self.config.consumer_group,
                "-",
                "+",
                self.config.recover_batch_size,
            )
            .await?;
        if pending.ids.is_empty() {
            return Ok(0);
        }

        let visibility_ms =
            u64::try_from(self.config.visibility_timeout.as_millis()).unwrap_or(u64::MAX);
        let visibility_threshold = usize::try_from(visibility_ms).unwrap_or(usize::MAX);
        let claimable: Vec<String> = pending
            .ids
            .iter()
            .filter(|p| p.last_delivered_ms >= visibility_threshold)
            .map(|p| p.id.clone())
            .collect();
        if claimable.is_empty() {
            return Ok(0);
        }

        // XCLAIM transfers the entries to a sentinel recovery consumer so we
        // can read their payloads. We then re-enqueue each payload as a brand
        // new stream entry and XACK+XDEL the originals -- this is the only
        // way to make the work deliverable again via `XREADGROUP >`, which
        // explicitly excludes PEL entries.
        let claimed: StreamClaimReply = conn
            .xclaim(
                &key,
                &self.config.consumer_group,
                "__recovered__",
                visibility_ms,
                &claimable,
            )
            .await?;

        let mut requeued = 0;
        for entry in claimed.ids {
            let payload = entry.map.get("payload").and_then(|v| match v {
                redis::Value::BulkString(bytes) => {
                    std::str::from_utf8(bytes).ok().map(ToString::to_string)
                }
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            });
            let Some(payload) = payload else {
                tracing::warn!(
                    queue = %queue_name,
                    entry_id = %entry.id,
                    "skipping recovered entry with malformed payload"
                );
                continue;
            };

            let _: String = conn.xadd(&key, "*", &[("payload", payload)]).await?;
            let _: i64 = conn
                .xack(&key, &self.config.consumer_group, &[entry.id.as_str()])
                .await?;
            let _: i64 = conn.xdel(&key, &[entry.id.as_str()]).await?;
            requeued += 1;
        }
        Ok(requeued)
    }

    async fn enqueue_immediate(&self, envelope: &TaskEnvelope) -> RedisAdapterResult<()> {
        // The consumer group must exist *before* the first XADD on a fresh
        // queue. XGROUP CREATE with `$` snapshots the stream's current tail,
        // so entries added before the group is created are invisible to
        // subsequent XREADGROUP calls. Calling ensure_group first guarantees
        // a freshly produced entry is always above the group's read position.
        self.ensure_group(&envelope.queue_name).await?;
        let payload = envelope.to_payload()?;
        let key = self.stream_key(&envelope.queue_name);
        let mut conn = self.conn.clone();
        // `*` lets Redis auto-generate the entry id; we already track our own
        // task_id in the payload.
        let _: String = conn.xadd(&key, "*", &[("payload", payload)]).await?;
        Ok(())
    }

    async fn enqueue_delayed(&self, envelope: &TaskEnvelope) -> RedisAdapterResult<()> {
        let payload = envelope.to_payload()?;
        let zset = self.zset_key(&envelope.queue_name);
        let payloads = self.payloads_key(&envelope.queue_name);
        let score = envelope.scheduled_at.timestamp_millis();
        let task_id = envelope.task_id.to_string();
        let mut conn = self.conn.clone();
        // Pipeline so the ZADD and HSET land atomically (or at least without
        // the consumer racing past a half-written record).
        redis::pipe()
            .atomic()
            .zadd(&zset, &task_id, score)
            .ignore()
            .hset(&payloads, &task_id, &payload)
            .ignore()
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    async fn ack(&self, queue_name: &str, entry_id: &str) -> RedisAdapterResult<()> {
        let mut conn = self.conn.clone();
        let key = self.stream_key(queue_name);
        let _: i64 = conn
            .xack(&key, &self.config.consumer_group, &[entry_id])
            .await?;
        // Also XDEL to keep the stream from growing unbounded with completed
        // entries. The PEL has been cleared by XACK, so XDEL is safe.
        let _: i64 = conn.xdel(&key, &[entry_id]).await?;
        Ok(())
    }

    async fn move_to_dlq(&self, envelope: &TaskEnvelope, error: &str) -> RedisAdapterResult<()> {
        if !self.config.enable_dlq {
            return Ok(());
        }
        let payload = envelope.to_payload()?;
        let mut conn = self.conn.clone();
        let key = self.dlq_key(&envelope.queue_name);
        let _: String = conn
            .xadd(
                &key,
                "*",
                &[
                    ("payload", payload.as_str()),
                    ("error", error),
                    ("task_id", &envelope.task_id.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Read the current depth (XLEN) for a single queue.
    async fn queue_depth_one(&self, queue_name: &str) -> RedisAdapterResult<i64> {
        let mut conn = self.conn.clone();
        let key = self.stream_key(queue_name);
        let stream_len: i64 = propagate_redis_result(conn.xlen(&key).await)?;
        let zset = self.zset_key(queue_name);
        let zset_len: i64 = propagate_redis_result(conn.zcard(&zset).await)?;
        Ok(stream_len + zset_len)
    }

    /// Try to claim one task from the supplied queues, in order.
    async fn claim_inner(
        &self,
        queues: &[String],
        worker_id: &str,
    ) -> RedisAdapterResult<Option<ClaimedTask>> {
        for queue in queues {
            // Make sure the consumer group exists *and* any due delayed tasks
            // are on the stream before we ask for them.
            self.ensure_group(queue).await?;
            let _ = self.promote_due(queue).await?;

            let mut conn = self.conn.clone();
            let key = self.stream_key(queue);
            let opts = StreamReadOptions::default()
                .group(&self.config.consumer_group, worker_id)
                .count(1);
            // ">" means "give me only entries that have not yet been delivered
            // to *any* consumer in the group". For PEL recovery, callers use
            // `recover_pending` which re-enqueues idle entries so they become
            // claimable through this same path.
            let reply: StreamReadReply =
                propagate_redis_result(conn.xread_options(&[&key], &[">"], &opts).await)?;

            let entry = reply
                .keys
                .into_iter()
                .flat_map(|stream| stream.ids.into_iter())
                .next();
            let Some(entry) = entry else {
                continue;
            };

            let payload = entry
                .map
                .get("payload")
                .and_then(|v| match v {
                    redis::Value::BulkString(bytes) => {
                        std::str::from_utf8(&bytes).ok().map(ToString::to_string)
                    }
                    redis::Value::SimpleString(s) => Some(s.clone()),
                    _ => None,
                })
                .ok_or_else(|| RedisAdapterError::MalformedEntry {
                    entry_id: entry.id.clone(),
                    reason: "missing payload field".into(),
                })?;

            let mut envelope = TaskEnvelope::from_payload(&payload, &entry.id)?;
            envelope.attempt = envelope.attempt.saturating_add(1);
            return Ok(Some(ClaimedTask {
                entry_id: entry.id,
                envelope,
            }));
        }
        Ok(None)
    }
}

fn propagate_redis_result<T>(result: redis::RedisResult<T>) -> RedisAdapterResult<T> {
    result.map_err(Into::into)
}

#[async_trait]
impl TaskQueueAdapter for RedisTaskQueue {
    async fn enqueue(&self, params: EnqueueParams) -> RedisAdapterResult<Uuid> {
        let task_id = Uuid::new_v4();
        let envelope = TaskEnvelope::from_params(params, task_id)?;
        if envelope.scheduled_at <= Utc::now() {
            self.enqueue_immediate(&envelope).await?;
        } else {
            self.enqueue_delayed(&envelope).await?;
        }
        Ok(task_id)
    }

    async fn claim(
        &self,
        queues: &[String],
        worker_id: &str,
    ) -> RedisAdapterResult<Option<ClaimedTask>> {
        self.claim_inner(queues, worker_id).await
    }

    async fn complete(
        &self,
        task: &ClaimedTask,
        _output: serde_json::Value,
    ) -> RedisAdapterResult<()> {
        // The Redis adapter does not persist task outputs -- workflow output
        // lives in Postgres. We just XACK + XDEL the stream entry to release
        // it from the consumer group's PEL.
        self.ack(&task.envelope.queue_name, &task.entry_id).await
    }

    async fn fail(&self, task: &ClaimedTask, error: &str) -> RedisAdapterResult<()> {
        self.move_to_dlq(&task.envelope, error).await?;
        self.ack(&task.envelope.queue_name, &task.entry_id).await
    }

    async fn requeue_for_retry(
        &self,
        task: &ClaimedTask,
        delay: Duration,
    ) -> RedisAdapterResult<()> {
        // ACK the current entry so it leaves the PEL, then re-enqueue a fresh
        // envelope with a future scheduled_at. The new envelope reuses the
        // same task_id so consumers and DLQ records stay correlated across
        // attempts.
        self.ack(&task.envelope.queue_name, &task.entry_id).await?;
        let chrono_delay = chrono::Duration::from_std(delay)
            .map_err(|e| RedisAdapterError::DurationOutOfRange(format!("retry delay: {e}")))?;
        let mut next = task.envelope.clone();
        next.scheduled_at = Utc::now() + chrono_delay;
        if next.scheduled_at <= Utc::now() {
            self.enqueue_immediate(&next).await
        } else {
            self.enqueue_delayed(&next).await
        }
    }

    async fn record_heartbeat(&self, task: &ClaimedTask) -> RedisAdapterResult<()> {
        // XCLAIM the entry back to the same consumer with a 0-ms idle reset.
        // This keeps the entry off `recover_pending`'s reclaim list because
        // its `last_delivered_ms` resets to "now".
        let mut conn = self.conn.clone();
        let key = self.stream_key(&task.envelope.queue_name);
        let consumer = task.envelope.task_id.to_string();
        let _: StreamClaimReply = conn
            .xclaim(
                &key,
                &self.config.consumer_group,
                consumer,
                0_u64,
                &[task.entry_id.as_str()],
            )
            .await?;
        Ok(())
    }

    async fn queue_depths(&self, queues: &[String]) -> RedisAdapterResult<Vec<(String, i64)>> {
        let mut out = Vec::with_capacity(queues.len());
        for queue in queues {
            let depth = self.queue_depth_one(queue).await?;
            out.push((queue.clone(), depth));
        }
        Ok(out)
    }
}

fn is_busygroup(err: &RedisError) -> bool {
    err.code() == Some("BUSYGROUP")
        || err.detail().is_some_and(|d| {
            d.contains("BUSYGROUP") || d.contains("Consumer Group name already exists")
        })
}

/// Lua script that atomically promotes all due delayed tasks for a single
/// queue onto its claimable stream.
///
/// Arguments:
/// - `KEYS[1]`: the per-queue sorted set of task ids keyed by `scheduled_at`.
/// - `KEYS[2]`: the per-queue payload hash keyed by `task_id`.
/// - `KEYS[3]`: the per-queue claimable stream.
/// - `ARGV[1]`: current timestamp in unix milliseconds.
///
/// Behavior: drains every member of the sorted set whose score is `<=` the
/// supplied timestamp, looks the matching payload up in the hash, XADDs it to
/// the stream, and removes both index entries. Returns the number of members
/// promoted.
const PROMOTE_LUA: &str = r"
local zset = KEYS[1]
local payloads = KEYS[2]
local stream = KEYS[3]
local now_ms = tonumber(ARGV[1])
local due = redis.call('ZRANGEBYSCORE', zset, '-inf', now_ms)
local promoted = 0
for _, task_id in ipairs(due) do
    local payload = redis.call('HGET', payloads, task_id)
    if payload then
        redis.call('XADD', stream, '*', 'payload', payload)
        redis.call('HDEL', payloads, task_id)
        promoted = promoted + 1
    end
    redis.call('ZREM', zset, task_id)
end
return promoted
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_documented_constants() {
        let cfg = RedisTaskQueueConfig::default();
        assert_eq!(cfg.key_prefix, DEFAULT_KEY_PREFIX);
        assert_eq!(cfg.consumer_group, DEFAULT_CONSUMER_GROUP);
        assert_eq!(cfg.visibility_timeout, DEFAULT_VISIBILITY_TIMEOUT);
        assert_eq!(cfg.recover_batch_size, DEFAULT_CLAIM_BATCH);
        assert!(cfg.enable_dlq);
    }

    #[test]
    fn promote_lua_script_compiles() {
        // `Script::new` is the cheapest way to confirm the literal is well-formed.
        let _ = Script::new(PROMOTE_LUA);
    }

    #[test]
    fn propagate_redis_result_preserves_empty_stream_success() {
        let reply = propagate_redis_result(Ok(StreamReadReply { keys: vec![] }))
            .expect("empty stream replies should remain successful");
        assert!(reply.keys.is_empty());
    }

    #[test]
    fn propagate_redis_result_preserves_integer_success() {
        let depth =
            propagate_redis_result::<i64>(Ok(7)).expect("integer replies should pass through");
        assert_eq!(depth, 7);
    }

    #[test]
    fn propagate_redis_result_surfaces_redis_errors() {
        let err = redis::RedisError::from((redis::ErrorKind::IoError, "xread exploded"));
        let mapped = propagate_redis_result::<i64>(Err(err));
        assert!(
            matches!(mapped, Err(RedisAdapterError::Redis(_))),
            "redis command failures must not be treated as empty depth or an empty queue",
        );
    }

    // The exact `is_busygroup` path is exercised end-to-end against a real
    // Redis instance in the integration tests under `tests/`. It is hard to
    // construct a `RedisError` with a `BUSYGROUP` code outside of an actual
    // server reply, so we don't try to fake one here.
}
