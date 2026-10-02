//! Postgres LISTEN/NOTIFY helpers for wake-on-enqueue.
//!
//! Instead of polling the task queue on a fixed interval, workers can subscribe
//! to a Postgres NOTIFY channel and wake immediately when a new task is enqueued.
//! This module provides the channel naming convention, the notification payload
//! type, and a [`QueueListener`] that wraps `tokio-postgres` for async LISTEN.
//!
//! # Post-commit delivery
//!
//! A transaction that calls `pg_notify` takes a database-wide lock at commit,
//! so these commits run one at a time. A full notification queue also fails
//! the commit (issue #1796). Thus [`notify_task_enqueued`],
//! [`notify_tasks_enqueued`] and [`notify_workflow_events_appended`] never
//! send inside the write transaction, and never fail the write.
//!
//! Each call stages a note with the transaction id of the write. The sender
//! of a pool from [`register_pool`] reads `txid_status` on its own connection.
//! It sends a note only after the write commits, and drops the note of a
//! write that rolls back. One statement sends a batch, with one wake per
//! queue and one per execution. That statement writes no data, so its commit
//! does not wait for a WAL flush and holds the NOTIFY lock only briefly.
//!
//! Call a `notify_*` function after the write in the same transaction. A note
//! staged in a savepoint that later rolls back is still sent. That costs one
//! extra wake. A wake can also stand for several writes: one queue wake
//! carries the nil task id, and one event wake carries the summed count.
//!
//! These writes use the fallback:
//!
//! - a write to a database that no ready sender serves,
//! - a write before the first read of a new sender,
//! - a write after the pool of its sender drops.
//!
//! The fallback sends on the write connection. Outside a transaction, the
//! write already committed, so the wake goes at once. Inside one, the wake
//! goes in a savepoint, so its error cannot abort the write. A full queue can
//! still fail that commit.
//!
//! Polling stays the correctness floor. A lost wake costs latency, not work.
//! [`send_failures`] counts lost wakes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use deadpool::managed::WeakPool;
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};

// ---------------------------------------------------------------------------
// Channel naming
// ---------------------------------------------------------------------------

/// Convert a queue name to its Postgres NOTIFY channel name.
///
/// The convention is `harvest_queue_{name}` with hyphens replaced by
/// underscores (Postgres identifiers cannot contain hyphens).
///
/// A name longer than 63 bytes is cut on a character boundary, as Postgres
/// cuts an identifier. `LISTEN` cuts the name, but `pg_notify` rejects it, so
/// both sides must use the cut name (issue #1796). Two long names with the
/// same first 63 bytes share a channel. That costs only extra wakes.
///
/// # Examples
///
/// ```
/// # use autumn_harvest::notify::queue_channel;
/// assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
/// ```
#[must_use]
pub fn queue_channel(queue_name: &str) -> String {
    let mut channel = format!("harvest_queue_{}", queue_name.replace('-', "_"));
    if channel.len() >= PG_NAMEDATALEN {
        let mut end = PG_NAMEDATALEN - 1;
        while !channel.is_char_boundary(end) {
            end -= 1;
        }
        channel.truncate(end);
    }
    channel
}

/// Postgres NOTIFY channel used when workflow event history advances.
///
/// `store::append_events` sends this notification after the append commits.
/// An embedder can LISTEN once and wake when any workflow changes state.
#[must_use]
pub const fn workflow_events_channel() -> &'static str {
    "harvest_events"
}

/// Postgres NOTIFY channel used for a single execution's ephemeral progress
/// stream (issue #791).
///
/// The convention is `harvest_progress_{exec_hex}` where `exec_hex` is the
/// execution UUID as 32 lowercase hex digits (no hyphens — Postgres identifiers
/// cannot contain them). The result is `17 + 32 = 49` characters, comfortably
/// within Postgres' 63-byte identifier limit.
///
/// Unlike [`workflow_events_channel`] (a single global channel fired on real
/// event appends), progress is per-execution so a subscriber LISTENs only to
/// the one run it is streaming and is never woken by unrelated workflows.
///
/// # Examples
///
/// ```
/// # use autumn_harvest::notify::workflow_progress_channel;
/// # use uuid::Uuid;
/// let id = Uuid::parse_str("0191c1a2-3b4c-7d5e-8f60-112233445566").unwrap();
/// assert_eq!(
///     workflow_progress_channel(id),
///     "harvest_progress_0191c1a23b4c7d5e8f60112233445566"
/// );
/// ```
#[must_use]
pub fn workflow_progress_channel(exec_id: Uuid) -> String {
    format!("harvest_progress_{}", exec_id.simple())
}

#[must_use]
fn quote_pg_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// NotifyPayload
// ---------------------------------------------------------------------------

/// Payload sent via Postgres NOTIFY when a task is enqueued.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotifyPayload {
    /// The UUID of the newly enqueued task. The nil UUID when one wake stands
    /// for several tasks (issue #1796).
    pub task_id: Uuid,
}

/// Payload sent on [`workflow_events_channel`] after events are appended.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkflowEventNotifyPayload {
    /// Workflow execution whose history advanced.
    pub workflow_exec_id: Uuid,
    /// Number of events appended since the last wake for this execution.
    pub event_count: usize,
    /// Type name of the last appended event.
    pub last_event_type: String,
}

/// Payload sent on [`workflow_progress_channel`] for each published progress
/// chunk (issue #791).
///
/// This is the wire envelope carried in the Postgres `NOTIFY` payload. The
/// whole envelope must fit within Postgres' 8000-byte `NOTIFY` limit; the
/// `chunk` is size-capped by the context (see
/// [`PROGRESS_CHUNK_MAX_BYTES`](crate::context::PROGRESS_CHUNK_MAX_BYTES)) to
/// leave headroom for the envelope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProgressNotifyPayload {
    /// Monotonic, epoch-prefixed sequence number (strictly increasing over the
    /// execution's lifetime, so a reconnecting subscriber can detect gaps).
    pub seq: u64,
    /// The published chunk (JSON; possibly a truncation marker if the original
    /// exceeded the size cap).
    pub chunk: serde_json::Value,
}

/// Outcome of waiting on a queue listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueWaitOutcome {
    /// A notification payload arrived before the timeout elapsed.
    Notification(NotifyPayload),
    /// No payload arrived before `poll_interval` elapsed.
    TimedOut,
    /// The listener channel closed because the underlying connection died.
    ChannelClosed,
}

/// Outcome of waiting on the workflow event listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowEventWaitOutcome {
    /// A workflow event notification arrived before the timeout elapsed.
    Notification(WorkflowEventNotifyPayload),
    /// No notification arrived before the caller's timeout elapsed.
    TimedOut,
    /// The LISTEN connection closed.
    ChannelClosed,
}

/// Outcome of waiting on a [`WorkflowProgressListener`] (issue #791).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressWaitOutcome {
    /// A progress chunk arrived before the timeout elapsed.
    Chunk(ProgressNotifyPayload),
    /// No chunk arrived before the caller's timeout elapsed (send a keepalive).
    TimedOut,
    /// The LISTEN connection closed (the SSE stream should end with an error).
    ChannelClosed,
}

// ---------------------------------------------------------------------------
// Send notification (via diesel connection)
// ---------------------------------------------------------------------------

/// Wake the workers that listen on `queue_name`, after the write commits.
///
/// Call this in the transaction that wrote the task row. See
/// [post-commit delivery](crate::notify#post-commit-delivery) for how the wake
/// is sent.
///
/// # Errors
///
/// A failed send never fails the write. It is counted in [`send_failures`].
/// The call returns an error only when the write transaction has already
/// failed, so the write cannot commit.
pub async fn notify_task_enqueued(
    conn: &mut AsyncPgConnection,
    queue_name: &str,
    task_id: Uuid,
) -> HarvestResult<()> {
    // Chaos: drop this wake (issue #940 AC1(c)). The poll loop still finds the
    // task, because NOTIFY only reduces latency.
    if crate::chaos_drop_notify!(NOTIFY_TASK_ENQUEUED) {
        return Ok(());
    }

    stage(
        conn,
        vec![Note::Task {
            channel: queue_channel(queue_name),
            task_id,
        }],
    )
    .await
}

/// Wake the workers of several queues, after the write commits.
///
/// Same contract as [`notify_task_enqueued`].
///
/// # Errors
///
/// Same as [`notify_task_enqueued`].
pub async fn notify_tasks_enqueued(
    conn: &mut AsyncPgConnection,
    queue_names: &[String],
    task_id: Uuid,
) -> HarvestResult<()> {
    if queue_names.is_empty() {
        return Ok(());
    }
    let notes = queue_names
        .iter()
        .map(|queue_name| Note::Task {
            channel: queue_channel(queue_name),
            task_id,
        })
        .collect();
    stage(conn, notes).await
}

/// Tell the listeners of [`workflow_events_channel`] that history advanced,
/// after the write commits.
///
/// Same contract as [`notify_task_enqueued`]. A listener wakes only after the
/// execution row and the event rows are visible.
///
/// # Errors
///
/// Same as [`notify_task_enqueued`].
pub async fn notify_workflow_events_appended(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    event_count: usize,
    last_event_type: &str,
) -> HarvestResult<()> {
    stage(
        conn,
        vec![Note::Events {
            exec_id: workflow_exec_id,
            count: event_count,
            last_event_type: last_event_type.to_string(),
        }],
    )
    .await
}

// ---------------------------------------------------------------------------
// Post-commit delivery (issue #1796)
// ---------------------------------------------------------------------------

/// Postgres rejects a channel name of this many bytes or more (`NAMEDATALEN`).
const PG_NAMEDATALEN: usize = 64;

/// Most notes one sender holds. A note over the bound is dropped and counted.
const MAX_PENDING_NOTES: usize = 10_000;

/// How long the sender first waits before it reads an open transaction again.
const GATE_RETRY_INTERVAL: Duration = Duration::from_millis(5);

/// The retry wait doubles on each idle tick up to this bound.
///
/// Nothing tells the sender that a write committed. The bound therefore caps
/// the wake latency that the sender adds after a commit.
pub(crate) const GATE_RETRY_MAX: Duration = Duration::from_millis(25);

/// How often an idle sender samples the queue usage and checks its pool.
const IDLE_INTERVAL: Duration = Duration::from_secs(1);

/// The sender drops a note whose transaction stays open longer than this.
const MAX_GATE_WAIT: Duration = Duration::from_secs(60);

/// How long the sender waits after it fails to get a connection.
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// How long the sender waits for a connection from its pool.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// A sender with no successful read for this long gets no new notes.
const HEALTHY_WITHIN: Duration = Duration::from_secs(3);

/// Shortest interval between two warnings about lost notifications.
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Shortest delay before a worker claims after a wake.
pub(crate) const SETTLE_DELAY_MIN: Duration = Duration::from_millis(50);

/// Longest delay before a worker claims after a wake.
pub(crate) const SETTLE_DELAY_MAX: Duration = Duration::from_millis(75);

/// Notifications lost to an error since the process started.
static SEND_FAILURES: AtomicU64 = AtomicU64::new(0);

/// When the lost-notification warning was last emitted.
static FAILURE_LOGGED: Mutex<Option<Instant>> = Mutex::new(None);

/// Every registered sender in the process.
static SINKS: Mutex<Vec<Arc<SinkShared>>> = Mutex::new(Vec::new());

/// A notification that waits for its write to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Note {
    /// A task became claimable on a queue channel.
    Task {
        /// The channel from [`queue_channel`].
        channel: String,
        /// The task, or the nil id for a wake that names no task.
        task_id: Uuid,
    },
    /// History of one execution advanced.
    Events {
        /// The execution.
        exec_id: Uuid,
        /// Number of events appended.
        count: usize,
        /// Type name of the last appended event.
        last_event_type: String,
    },
}

impl Note {
    /// The channel the note goes to.
    fn channel(&self) -> &str {
        match self {
            Self::Task { channel, .. } => channel,
            Self::Events { .. } => workflow_events_channel(),
        }
    }
}

/// True when Postgres accepts `channel` as a channel name.
const fn valid_channel(channel: &str) -> bool {
    !channel.is_empty() && channel.len() < PG_NAMEDATALEN
}

/// Merge notes into one `(channel, payload)` pair per wake, in first-seen
/// order.
///
/// Task notes merge per channel. One task keeps its id. Several tasks give
/// the nil id, as [`notify_tasks_enqueued`] does. Event notes merge per
/// execution: the counts add up, and the last type wins. A note on a channel
/// that Postgres rejects is dropped.
fn coalesce(notes: Vec<Note>) -> Vec<(String, String)> {
    enum Merged {
        Task(String, Uuid),
        Events(Uuid, usize, String),
    }
    let mut merged: Vec<Merged> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    for note in notes {
        if !valid_channel(note.channel()) {
            continue;
        }
        match note {
            Note::Task { channel, task_id } => {
                if let Some(&i) = index.get(&channel) {
                    if let Merged::Task(_, id) = &mut merged[i] {
                        *id = Uuid::nil();
                    }
                } else {
                    index.insert(channel.clone(), merged.len());
                    merged.push(Merged::Task(channel, task_id));
                }
            }
            Note::Events {
                exec_id,
                count,
                last_event_type,
            } => {
                let key = format!("events:{exec_id}");
                if let Some(&i) = index.get(&key) {
                    if let Merged::Events(_, total, last) = &mut merged[i] {
                        *total += count;
                        *last = last_event_type;
                    }
                } else {
                    index.insert(key, merged.len());
                    merged.push(Merged::Events(exec_id, count, last_event_type));
                }
            }
        }
    }
    merged
        .into_iter()
        .filter_map(|m| match m {
            Merged::Task(channel, task_id) => serde_json::to_string(&NotifyPayload { task_id })
                .ok()
                .map(|payload| (channel, payload)),
            Merged::Events(workflow_exec_id, event_count, last_event_type) => {
                serde_json::to_string(&WorkflowEventNotifyPayload {
                    workflow_exec_id,
                    event_count,
                    last_event_type,
                })
                .ok()
                .map(|payload| (workflow_events_channel().to_string(), payload))
            }
        })
        .collect()
}

/// A random delay from [`SETTLE_DELAY_MIN`] to [`SETTLE_DELAY_MAX`].
///
/// A worker sleeps this long after a wake and before it claims. Host clocks
/// can run ahead of the database `NOW()`, so a new task needs a short time to
/// become claimable. The floor keeps the 50 ms margin the fixed delay gave.
/// The jitter above it stops every worker from claiming at the same instant
/// after one wake.
#[must_use]
pub(crate) fn settle_delay() -> Duration {
    use rand::Rng as _;
    let min = u64::try_from(SETTLE_DELAY_MIN.as_micros()).unwrap_or(u64::MAX);
    let max = u64::try_from(SETTLE_DELAY_MAX.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::thread_rng().gen_range(min..=max))
}

/// Notifications lost to an error since the process started.
///
/// A failed send adds one for each merged wake. A dropped or rejected note
/// adds one. A lost notification costs latency, not work, because the poll
/// loop still finds the row.
#[must_use]
pub fn send_failures() -> u64 {
    AtomicU64::load(&SEND_FAILURES, Ordering::Relaxed)
}

/// The largest `pg_notification_queue_usage()` that a live sender read last.
///
/// `None` when no sender has read it yet. A value near `1.0` means the queue
/// is almost full. A listener that does not read its notifications causes
/// this.
#[must_use]
pub fn queue_usage() -> Option<f64> {
    lock(&SINKS)
        .iter()
        .filter(|sink| sink.alive())
        .filter_map(|sink| *lock(&sink.queue_usage))
        .reduce(f64::max)
}

/// Count `count` lost notifications and log the cause at a bounded rate.
fn record_failures(count: usize, cause: &dyn std::fmt::Display) {
    if count == 0 {
        return;
    }
    let total = SEND_FAILURES.fetch_add(count as u64, Ordering::Relaxed) + count as u64;
    let due = {
        let mut logged = lock(&FAILURE_LOGGED);
        let due = logged.is_none_or(|at| at.elapsed() >= FAILURE_LOG_INTERVAL);
        if due {
            *logged = Some(Instant::now());
        }
        due
    };
    if due {
        tracing::warn!(
            lost = count,
            total,
            cause = %cause,
            "harvest: notifications lost; workers find the rows on their next poll"
        );
    }
}

/// Lock `mutex` and ignore poisoning. The guarded data stays valid after a
/// panic, because every update is a single assignment or push.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The identity of one database, as both sides of a sender read it.
///
/// A database name alone is not unique across servers. The start time of the
/// server tells two servers apart.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fingerprint {
    /// `current_database()`.
    database: String,
    /// `pg_postmaster_start_time()`.
    started_at: DateTime<Utc>,
}

/// State one sender shares with the stage calls.
struct SinkShared {
    /// The pool the sender sends on. Weak, so the sender never keeps a pool
    /// alive.
    pool: WeakPool<AsyncDieselConnectionManager<AsyncPgConnection>>,
    /// The address of the pool manager. It identifies the pool.
    pool_key: usize,
    /// The database the sender sends to. `None` until its first read.
    fingerprint: Mutex<Option<Fingerprint>>,
    /// Notes that wait for the sender.
    pending: Mutex<Vec<Staged>>,
    /// Wakes the sender when a note arrives.
    wake: tokio::sync::Notify,
    /// True after the first read of the fingerprint.
    ready: tokio::sync::watch::Sender<bool>,
    /// The last `pg_notification_queue_usage()` the sender read.
    queue_usage: Mutex<Option<f64>>,
    /// The sender task.
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Notifications this sender lost to an error.
    failures: AtomicU64,
    /// When the sender last read the commit state.
    last_ok: Mutex<Option<Instant>>,
    /// Notes the sender task holds. [`NotifySink::flush`] reads it.
    held: AtomicUsize,
}

impl SinkShared {
    /// True while the pool exists but no sender task runs for it. A runtime
    /// that ends stops its tasks, so a later runtime starts the sender again.
    fn deferred(&self) -> bool {
        lock(&self.task)
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
            && self.pool.upgrade().is_some()
    }

    /// Count `count` notifications this sender lost.
    fn record(&self, count: usize, cause: &dyn std::fmt::Display) {
        self.failures.fetch_add(count as u64, Ordering::Relaxed);
        record_failures(count, cause);
    }

    /// True while the sender runs and its pool exists.
    fn alive(&self) -> bool {
        lock(&self.task)
            .as_ref()
            .is_some_and(|task| !task.is_finished())
            && self.pool.upgrade().is_some()
    }

    /// True when the sender read the commit state recently.
    fn healthy(&self) -> bool {
        self.alive() && lock(&self.last_ok).is_some_and(|at| at.elapsed() < HEALTHY_WITHIN)
    }

    /// Queue `notes` for the sender. `fingerprint` names the database of the
    /// write.
    fn push(&self, txid: Option<i64>, fingerprint: &Arc<Fingerprint>, notes: Vec<Note>) {
        let queued_at = Instant::now();
        let mut pending = lock(&self.pending);
        let room = MAX_PENDING_NOTES.saturating_sub(pending.len());
        let dropped = notes.len().saturating_sub(room);
        pending.extend(notes.into_iter().take(room).map(|note| Staged {
            txid,
            fingerprint: Arc::clone(fingerprint),
            queued_at,
            note,
        }));
        drop(pending);
        self.record(dropped, &"the notify sender queue is full");
        self.wake.notify_one();
    }
}

/// A note with the transaction id of its write.
struct Staged {
    /// The transaction id of the write. `None` when the write had already
    /// committed.
    txid: Option<i64>,
    /// The database of the write. A transaction id means nothing on another
    /// server.
    fingerprint: Arc<Fingerprint>,
    /// When the note was staged.
    queued_at: Instant,
    /// The note.
    note: Note,
}

/// A handle to the post-commit sender of one pool.
///
/// Dropping the handle does not stop the sender. The sender stops when its
/// pool drops.
#[derive(Clone)]
pub struct NotifySink {
    /// The shared sender state.
    shared: Arc<SinkShared>,
}

impl std::fmt::Debug for NotifySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotifySink")
            .field("pool_key", &self.shared.pool_key)
            .field("ready", &*self.shared.ready.borrow())
            .finish_non_exhaustive()
    }
}

impl NotifySink {
    /// Wait until the sender knows its database, or until `timeout` elapses.
    ///
    /// Returns `true` when the sender is ready. Before that, writes to the
    /// database use the fallback.
    #[must_use]
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        let mut ready = self.shared.ready.subscribe();
        matches!(
            tokio::time::timeout(timeout, ready.wait_for(|ready| *ready)).await,
            Ok(Ok(_))
        )
    }

    /// Notifications this sender lost to an error. [`send_failures`] is the
    /// total for the process.
    #[must_use]
    pub fn send_failures(&self) -> u64 {
        AtomicU64::load(&self.shared.failures, Ordering::Relaxed)
    }

    /// The last `pg_notification_queue_usage()` this sender read.
    #[must_use]
    pub fn queue_usage(&self) -> Option<f64> {
        *lock(&self.shared.queue_usage)
    }

    /// Wait until the sender holds no notes, or until `timeout` elapses.
    ///
    /// Call this before a runtime stops. A sender task stops with its
    /// runtime, and loses the notes it holds. Returns `true` when no note
    /// waits.
    pub async fn flush(&self, timeout: Duration) -> bool {
        let shared = &self.shared;
        // The sender moves notes and publishes their count under the
        // pending lock. Both reads under that lock therefore see one state.
        let drained = || {
            let pending = lock(&shared.pending);
            pending.is_empty() && AtomicUsize::load(&shared.held, Ordering::Relaxed) == 0
        };
        let deadline = tokio::time::Instant::now() + timeout;
        while !drained() {
            if tokio::time::Instant::now() >= deadline || !shared.alive() {
                return drained();
            }
            shared.wake.notify_one();
            tokio::time::sleep(GATE_RETRY_INTERVAL).await;
        }
        true
    }
}

/// Start the post-commit sender for `pool`, or return the registered one.
///
/// After the sender is ready, a notification for a write to the database of
/// `pool` goes after commit, on a connection from `pool`. This holds for a
/// write on any connection to that database, pooled or not. `Worker::run`
/// and `WorkflowHandleClient::new` call this. Call it for any other pool
/// that writes history or tasks.
///
/// The sender runs on the current Tokio runtime. Called outside a runtime,
/// the sender starts at the first notification that a runtime stages.
pub fn register_pool(pool: &crate::worker::DbPool) -> NotifySink {
    let pool_key = std::ptr::from_ref(pool.manager()).addr();
    let runtime = tokio::runtime::Handle::try_current().ok();
    let mut sinks = lock(&SINKS);
    sinks.retain(|sink| sink.alive() || sink.deferred());
    if let Some(sink) = sinks.iter().find(|sink| sink.pool_key == pool_key) {
        let shared = Arc::clone(sink);
        drop(sinks);
        if let Some(runtime) = runtime {
            start_deferred(&runtime);
        }
        return NotifySink { shared };
    }
    let shared = Arc::new(SinkShared {
        pool: pool.weak(),
        pool_key,
        fingerprint: Mutex::new(None),
        pending: Mutex::new(Vec::new()),
        wake: tokio::sync::Notify::new(),
        ready: tokio::sync::watch::Sender::new(false),
        queue_usage: Mutex::new(None),
        task: Mutex::new(None),
        failures: AtomicU64::new(0),
        last_ok: Mutex::new(None),
        held: AtomicUsize::new(0),
    });
    sinks.push(Arc::clone(&shared));
    drop(sinks);
    if let Some(runtime) = runtime {
        start_deferred(&runtime);
    } else {
        ANY_DEFERRED.store(true, Ordering::Relaxed);
        tracing::debug!("harvest: no Tokio runtime; the notify sender starts later");
    }
    NotifySink { shared }
}

/// True while some registered sender waits for a runtime.
static ANY_DEFERRED: AtomicBool = AtomicBool::new(false);

/// Start every registered sender that has no task yet, on `runtime`.
fn start_deferred(runtime: &tokio::runtime::Handle) {
    ANY_DEFERRED.store(false, Ordering::Relaxed);
    for sink in lock(&SINKS).iter().filter(|sink| sink.deferred()) {
        let task = runtime.spawn(run_sender(Arc::clone(sink)));
        *lock(&sink.task) = Some(task);
    }
}

/// The live sender for the database `fingerprint` names.
fn sink_for(fingerprint: &Fingerprint) -> Option<Arc<SinkShared>> {
    let sinks = lock(&SINKS);
    let found = sinks
        .iter()
        .filter(|sink| sink.healthy() && lock(&sink.fingerprint).as_ref() == Some(fingerprint))
        .max_by_key(|sink| *lock(&sink.last_ok))
        .cloned();
    if found.is_none() && sinks.iter().any(|sink| sink.deferred()) {
        ANY_DEFERRED.store(true, Ordering::Relaxed);
    }
    drop(sinks);
    found
}

/// True when the process has at least one healthy sender.
fn any_sink() -> bool {
    lock(&SINKS).iter().any(|sink| sink.healthy())
}

/// True unless `conn` is certainly outside a transaction.
///
/// A broken transaction state counts as inside, so the caller takes the safe
/// path.
fn in_transaction(conn: &mut AsyncPgConnection) -> bool {
    use diesel_async::TransactionManager as _;
    let status =
        <AsyncPgConnection as diesel_async::AsyncConnection>::TransactionManager::transaction_manager_status_mut(conn);
    !matches!(status.transaction_depth(), Ok(None))
}

/// The row the write connection reads to stage a note.
#[derive(diesel::QueryableByName)]
struct StageRow {
    /// The transaction id of the write, or `NULL` outside a transaction.
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    txid: Option<i64>,
    /// `current_database()`.
    #[diesel(sql_type = Text)]
    database: String,
    /// `pg_postmaster_start_time()`.
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    started_at: DateTime<Utc>,
}

/// Stage `notes` for the write on `conn`.
///
/// # Errors
///
/// Returns an error only when the write transaction has already failed.
/// Postgres answers `COMMIT` on a failed transaction with a silent rollback.
/// The error therefore tells the caller that its write did not commit.
async fn stage(conn: &mut AsyncPgConnection, notes: Vec<Note>) -> HarvestResult<()> {
    if AtomicBool::load(&ANY_DEFERRED, Ordering::Relaxed)
        && let Ok(runtime) = tokio::runtime::Handle::try_current()
    {
        start_deferred(&runtime);
    }
    if any_sink() {
        let in_tx = in_transaction(conn);
        // Inside a diesel transaction, `txid_current()` assigns an id when
        // the transaction has none yet. Thus the gate never sends before the
        // commit. Outside one, `txid_current_if_assigned()` still finds a raw
        // `BEGIN` block that wrote. After an autocommit write it reads NULL,
        // because that write already committed.
        let txid = if in_tx {
            "txid_current()"
        } else {
            "txid_current_if_assigned()"
        };
        let row = diesel::sql_query(format!(
            "SELECT {txid} AS txid, \
                    current_database()::text AS database, \
                    pg_postmaster_start_time() AS started_at"
        ))
        .get_result::<StageRow>(conn)
        .await;
        match row {
            Ok(row) => {
                let fingerprint = Fingerprint {
                    database: row.database,
                    started_at: row.started_at,
                };
                if let Some(sink) = sink_for(&fingerprint) {
                    sink.push(row.txid, &Arc::new(fingerprint), notes);
                    return Ok(());
                }
            }
            // The failed statement aborted the transaction, so it cannot commit.
            Err(error) if in_tx => return Err(crate::error::database_error(error)),
            Err(_) => {}
        }
    }
    send_on_write_connection(conn, notes).await
}

/// The fallback: send `notes` on the write connection.
///
/// Outside a transaction the write already committed, so the send goes at
/// once. Inside one, the send runs in a savepoint. Its error then rolls back
/// only the savepoint, and the write can still commit. This holds for a raw
/// `BEGIN` block too.
///
/// # Errors
///
/// Returns an error only when the savepoint cannot roll back. The write
/// transaction then cannot commit.
async fn send_on_write_connection(
    conn: &mut AsyncPgConnection,
    notes: Vec<Note>,
) -> HarvestResult<()> {
    let rejected = notes.iter().filter(|n| !valid_channel(n.channel())).count();
    record_failures(rejected, &"Postgres rejects the channel name");
    let wakes = coalesce(notes);
    if wakes.is_empty() {
        return Ok(());
    }
    let count = wakes.len();
    if !in_transaction(conn) {
        return send_outside_diesel_transaction(conn, wakes, count).await;
    }
    let result = Box::pin(
        conn.transaction::<(), diesel::result::Error, _>(async |conn| {
            send_wakes(conn, wakes).await
        }),
    )
    .await;
    if let Err(error) = result {
        // A failed rollback to the savepoint leaves the transaction failed.
        if transaction_failed(conn) {
            return Err(crate::error::database_error(error));
        }
        record_failures(count, &error);
    }
    Ok(())
}

/// The fallback when diesel tracks no transaction on `conn`.
///
/// A raw `BEGIN` opens a block that diesel does not see. `SAVEPOINT` works
/// only inside a block, so it tells the two cases apart. In a block, the send
/// runs behind the savepoint, so its error cannot abort the write. Outside
/// one, the write already committed, so the send goes at once.
///
/// # Errors
///
/// Returns an error only when the savepoint cannot roll back. The raw block
/// then cannot commit.
async fn send_outside_diesel_transaction(
    conn: &mut AsyncPgConnection,
    wakes: Vec<(String, String)>,
    count: usize,
) -> HarvestResult<()> {
    use diesel_async::SimpleAsyncConnection as _;
    if conn
        .batch_execute("SAVEPOINT harvest_notify_fallback")
        .await
        .is_err()
    {
        if let Err(error) = send_wakes(conn, wakes).await {
            record_failures(count, &error);
        }
        return Ok(());
    }
    let undo = match send_wakes(conn, wakes).await {
        Ok(()) => "RELEASE SAVEPOINT harvest_notify_fallback",
        Err(error) => {
            record_failures(count, &error);
            // The rollback keeps the savepoint, so release it next.
            "ROLLBACK TO SAVEPOINT harvest_notify_fallback; \
             RELEASE SAVEPOINT harvest_notify_fallback"
        }
    };
    conn.batch_execute(undo)
        .await
        .map_err(crate::error::database_error)
}

/// True when the transaction manager of `conn` is in its error state.
///
/// Diesel enters that state when a rollback fails. Then the transaction
/// cannot commit.
fn transaction_failed(conn: &mut AsyncPgConnection) -> bool {
    use diesel_async::TransactionManager as _;
    type Manager = <AsyncPgConnection as diesel_async::AsyncConnection>::TransactionManager;
    Manager::transaction_manager_status_mut(conn)
        .transaction_depth()
        .is_err()
}

/// Send each `(channel, payload)` pair in one statement.
async fn send_wakes(
    conn: &mut AsyncPgConnection,
    wakes: Vec<(String, String)>,
) -> Result<(), diesel::result::Error> {
    let (channels, payloads): (Vec<String>, Vec<String>) = wakes.into_iter().unzip();
    diesel::sql_query("SELECT pg_notify(t.c, t.p) FROM unnest($1::text[], $2::text[]) AS t(c, p)")
        .bind::<diesel::sql_types::Array<Text>, _>(channels)
        .bind::<diesel::sql_types::Array<Text>, _>(payloads)
        .execute(conn)
        .await
        .map(drop)
}

/// The row the sender reads on its own connection.
#[derive(diesel::QueryableByName)]
struct GateRow {
    /// `current_database()`.
    #[diesel(sql_type = Text)]
    database: String,
    /// `pg_postmaster_start_time()`.
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    started_at: DateTime<Utc>,
    /// `pg_notification_queue_usage()`.
    #[diesel(sql_type = diesel::sql_types::Double)]
    queue_usage: f64,
    /// `txid_status()` of each staged id, in order.
    #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Nullable<Text>>)]
    statuses: Vec<Option<String>>,
}

/// The notes a sender task holds.
///
/// A runtime that stops cancels the task. The drop then counts the held
/// notes as lost. Notes still pending stay for the next sender task.
struct Held {
    /// The sender state.
    sink: Arc<SinkShared>,
    /// The notes.
    notes: Vec<Staged>,
}

impl Held {
    /// Publish the number of held notes for [`NotifySink::flush`].
    fn publish(&self) {
        self.sink.held.store(self.notes.len(), Ordering::Relaxed);
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.sink.held.store(0, Ordering::Relaxed);
        self.sink
            .record(self.notes.len(), &"the notify sender task stopped");
        // A task that stops while its pool exists ended with its runtime. The
        // next note staged in a runtime starts the sender again.
        if self.sink.pool.upgrade().is_some() {
            *lock(&self.sink.task) = None;
            ANY_DEFERRED.store(true, Ordering::Relaxed);
        }
    }
}

/// Run the sender of one pool until the pool drops.
async fn run_sender(sink: Arc<SinkShared>) {
    let mut held = Held {
        sink: Arc::clone(&sink),
        notes: Vec::new(),
    };
    let mut retry = GATE_RETRY_INTERVAL;
    loop {
        if *sink.ready.borrow() {
            let wait = if held.notes.is_empty() {
                IDLE_INTERVAL
            } else {
                retry
            };
            tokio::select! {
                () = sink.wake.notified() => retry = GATE_RETRY_INTERVAL,
                () = tokio::time::sleep(wait) => retry = (retry * 2).min(GATE_RETRY_MAX),
            }
        }
        let Some(pool) = sink.pool.upgrade() else {
            break;
        };
        {
            let mut pending = lock(&sink.pending);
            held.notes.append(&mut pending);
            let excess = held.notes.len().saturating_sub(MAX_PENDING_NOTES);
            held.notes.drain(..excess);
            held.publish();
            drop(pending);
            sink.record(excess, &"the notify sender queue is full");
        }

        let read = match tokio::time::timeout(CONNECT_TIMEOUT, pool.get()).await {
            Ok(Ok(mut conn)) => {
                drop(pool);
                tick(&sink, &mut conn, &mut held.notes).await
            }
            Ok(Err(error)) => {
                tracing::debug!(error = %error, "harvest: notify sender has no connection");
                false
            }
            Err(_) => {
                tracing::debug!("harvest: notify sender timed out waiting for a connection");
                false
            }
        };
        if !read {
            age_out(&sink, &mut held.notes);
        }
        held.publish();
        if !read {
            tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
        }
    }
    let pending = std::mem::take(&mut *lock(&sink.pending));
    sink.record(pending.len(), &"the pool of the notify sender dropped");
}

/// Drop and count the held notes older than [`MAX_GATE_WAIT`].
fn age_out(sink: &SinkShared, held: &mut Vec<Staged>) {
    let before = held.len();
    held.retain(|staged| staged.queued_at.elapsed() < MAX_GATE_WAIT);
    sink.record(before - held.len(), &"a notification waited too long");
}

/// Read the commit state of every held note, then send the committed ones.
///
/// A committed note, a note with no transaction id and a note too old for
/// `txid_status` go now. An aborted note is dropped. A note whose
/// transaction is still open waits for the next tick, up to
/// [`MAX_GATE_WAIT`].
///
/// A note staged against another server is dropped and counted, because its
/// transaction id means nothing here.
///
/// `txid_status` raises an error for an id that this server has not assigned
/// yet. The `CASE` guard therefore reads no id at or past the snapshot `xmax`.
/// No such transaction has completed, so the guard reports it in progress.
///
/// Returns `false` when the commit state cannot be read. The notes then stay
/// held for the next tick.
async fn tick(sink: &SinkShared, conn: &mut AsyncPgConnection, held: &mut Vec<Staged>) -> bool {
    let txids: Vec<i64> = held.iter().filter_map(|staged| staged.txid).collect();
    let gate = diesel::sql_query(
        "SELECT current_database()::text AS database, \
                pg_postmaster_start_time() AS started_at, \
                pg_notification_queue_usage() AS queue_usage, \
                ARRAY(SELECT CASE WHEN t.x < txid_snapshot_xmax(txid_current_snapshot()) \
                                  THEN txid_status(t.x) ELSE 'in progress' END \
                      FROM unnest($1::bigint[]) WITH ORDINALITY AS t(x, n) \
                      ORDER BY t.n) AS statuses",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(txids)
    .get_result::<GateRow>(conn)
    .await;
    let gate = match gate {
        Ok(gate) => gate,
        Err(error) => {
            tracing::debug!(error = %error, "harvest: notify sender cannot read commit state");
            return false;
        }
    };
    let fingerprint = Fingerprint {
        database: gate.database,
        started_at: gate.started_at,
    };
    *lock(&sink.queue_usage) = Some(gate.queue_usage);
    *lock(&sink.last_ok) = Some(Instant::now());

    let mut statuses = gate.statuses.into_iter();
    let mut ready_notes = Vec::new();
    let mut waiting = Vec::new();
    let mut stale = 0;
    let mut moved = 0;
    for staged in held.drain(..) {
        let status = match staged.txid {
            Some(_) => statuses.next().flatten(),
            None => None,
        };
        if *staged.fingerprint != fingerprint {
            moved += 1;
            continue;
        }
        match status.as_deref() {
            Some("aborted") => {}
            Some("in progress") => {
                if staged.queued_at.elapsed() < MAX_GATE_WAIT {
                    waiting.push(staged);
                } else {
                    stale += 1;
                }
            }
            _ => ready_notes.push(staged.note),
        }
    }
    *held = waiting;
    *lock(&sink.fingerprint) = Some(fingerprint);
    sink.ready.send_replace(true);
    sink.record(stale, &"a write transaction stays open too long");
    sink.record(moved, &"the database of the notify sender changed");

    let rejected = ready_notes
        .iter()
        .filter(|n| !valid_channel(n.channel()))
        .count();
    sink.record(rejected, &"Postgres rejects the channel name");
    let wakes = coalesce(ready_notes);
    let count = wakes.len();
    if count > 0
        && let Err(error) = send_wakes(conn, wakes).await
    {
        sink.record(count, &error);
    }
    true
}

/// Fire a `NOTIFY` carrying one ephemeral progress chunk on the per-execution
/// progress channel (issue #791).
///
/// Delivered on commit when called inside a transaction — so a rolled-back
/// workflow-decision cycle's progress is discarded (never delivered) and the
/// retried cycle re-fires live, and a committed chunk is delivered exactly once
/// per commit. Best-effort by contract: callers should treat a failure as
/// non-fatal (the chunk is disposable).
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if payload serialization or `pg_notify`
/// fails.
pub async fn notify_workflow_progress(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    seq: u64,
    chunk: &serde_json::Value,
) -> HarvestResult<()> {
    let channel = workflow_progress_channel(workflow_exec_id);
    let payload = serde_json::to_string(&ProgressNotifyPayload {
        seq,
        chunk: chunk.clone(),
    })
    .map_err(|e| HarvestError::Database(format!("failed to serialize progress payload: {e}")))?;

    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(&channel)
        .bind::<Text, _>(&payload)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Listener connections (TLS: issue #1717)
// ---------------------------------------------------------------------------

/// The transport a listener connection uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenTransport {
    /// Plaintext, through `NoTls`.
    Plain,
    /// TLS, with a verified certificate chain and hostname.
    Tls,
}

/// Select the listener transport from the DSN's own `sslmode`.
///
/// Only `require` selects TLS, because `NoTls` cannot satisfy it. `disable`,
/// `prefer` and an absent `sslmode` stay plaintext. That is the behavior before
/// issue #1717, and it matches a pool built with `NoTls`. Thus a server with a
/// self-signed certificate does not break a `prefer` DSN.
fn listen_transport(config: &tokio_postgres::Config) -> ListenTransport {
    match config.get_ssl_mode() {
        tokio_postgres::config::SslMode::Require => ListenTransport::Tls,
        _ => ListenTransport::Plain,
    }
}

/// Render an error and each error in its `source()` chain.
///
/// `tokio_postgres` shows a TLS failure as "error performing TLS handshake".
/// The real cause is only in `source()`. A cause that the text already
/// contains is not added again.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

/// The error for a listener connection that failed to open.
fn connect_error(error: &tokio_postgres::Error) -> HarvestError {
    HarvestError::Database(format!("pg connect failed: {}", error_chain(error)))
}

/// An open LISTEN connection.
struct ListenConnection {
    /// Client handle. The connection closes when it drops.
    client: tokio_postgres::Client,
    /// Notifications that the driver task forwards.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// The task that drives the connection.
    driver: tokio::task::JoinHandle<()>,
}

/// Open a LISTEN connection with the transport that the DSN asks for.
///
/// `error_message` is the log message for a connection error after the open.
async fn open_listen_connection(
    database_url: &str,
    error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    // A DSN that does not parse is a permanent misconfiguration, not an outage.
    // A result wait returns a `Config` error and does not poll over it.
    let config: tokio_postgres::Config = database_url.parse().map_err(|e| {
        HarvestError::Config(format!(
            "invalid notification database URL: {}",
            error_chain(&e)
        ))
    })?;
    match listen_transport(&config) {
        ListenTransport::Plain => {
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| connect_error(&e))?;
            Ok(spawn_listen_driver(client, connection, error_message))
        }
        ListenTransport::Tls => open_tls_listen_connection(&config, error_message).await,
    }
}

/// Open a verified TLS LISTEN connection.
#[cfg(feature = "tls")]
async fn open_tls_listen_connection(
    config: &tokio_postgres::Config,
    error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_client_config()?);
    let (client, connection) = config.connect(tls).await.map_err(|e| connect_error(&e))?;
    Ok(spawn_listen_driver(client, connection, error_message))
}

/// Refuse `sslmode=require` when the crate has no TLS support.
#[cfg(not(feature = "tls"))]
#[allow(clippy::unused_async, reason = "the signature matches the `tls` build")]
async fn open_tls_listen_connection(
    _config: &tokio_postgres::Config,
    _error_message: &'static str,
) -> HarvestResult<ListenConnection> {
    Err(HarvestError::Config(
        "sslmode=require needs the `tls` feature of autumn-harvest".to_string(),
    ))
}

/// The rustls configuration for listener connections.
///
/// The trust store is read once per process. A failed read is not cached, so
/// a later connection tries again.
#[cfg(feature = "tls")]
fn tls_client_config() -> HarvestResult<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<rustls::ClientConfig> = std::sync::OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let built = build_tls_client_config()?;
    Ok(CONFIG.get_or_init(|| built).clone())
}

/// Build a rustls configuration that trusts the platform trust store.
///
/// The chain and the hostname are always verified, as in `harvest migrate`
/// (issue #1240). `SSL_CERT_FILE` or `SSL_CERT_DIR` can point at a private CA.
/// The `ring` provider is explicit, because `ClientConfig::builder()` panics
/// when no process-wide provider is installed.
#[cfg(feature = "tls")]
fn build_tls_client_config() -> HarvestResult<rustls::ClientConfig> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(native.certs);
    if roots.is_empty() {
        return Err(HarvestError::Config(format!(
            "sslmode=require: the platform trust store has no usable certificates. \
             Install the ca-certificates package, or set SSL_CERT_FILE. \
             Loader errors: {:?}",
            native.errors
        )));
    }
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| HarvestError::Config(format!("rustls configuration failed: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(config)
}

/// Spawn the task that drives a LISTEN connection.
///
/// The task calls `poll_message()` to get each notification. The default
/// `Future` implementation of the connection discards them.
fn spawn_listen_driver<S, T>(
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<S, T>,
    error_message: &'static str,
) -> ListenConnection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    let driver = tokio::spawn(async move {
        use futures::future::poll_fn;

        loop {
            match poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    // A send error means the listener dropped. Shut down.
                    if tx.send(n).await.is_err() {
                        break;
                    }
                }
                // Notices and other async messages are ignored.
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::error!(error = %error_chain(&e), "{error_message}");
                    break;
                }
                // The connection closed cleanly.
                None => break,
            }
        }
    });
    ListenConnection { client, rx, driver }
}

// ---------------------------------------------------------------------------
// QueueListener (using tokio-postgres)
// ---------------------------------------------------------------------------

/// Async listener for Postgres NOTIFY events on task queue channels.
///
/// Uses a dedicated `tokio-postgres` connection (separate from the diesel pool)
/// because `LISTEN` requires a long-lived connection that receives async
/// notifications. The connection is driven by a background task that forwards
/// notifications through an `mpsc` channel.
pub struct QueueListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle -- kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
    /// Queue names this listener is subscribed to.
    queues: Vec<String>,
}

impl QueueListener {
    /// Connect to Postgres and subscribe to NOTIFY channels for the given queues.
    ///
    /// Spawns a background task that drives the connection and forwards
    /// [`Notification`]s through an internal channel. The connection stays
    /// alive as long as this `QueueListener` is held.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if the URL does not parse, or if TLS
    /// cannot be configured.
    pub async fn connect(database_url: &str, queues: &[String]) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres listener connection error").await?;

        // Subscribe to all queue channels.
        for queue in queues {
            let channel = queue_channel(queue);
            let quoted_channel = quote_pg_identifier(&channel);
            client
                .batch_execute(&format!("LISTEN {quoted_channel}"))
                .await
                .map_err(|e| {
                    HarvestError::Database(format!(
                        "LISTEN {quoted_channel} failed: {}",
                        error_chain(&e)
                    ))
                })?;
        }

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
            queues: queues.to_vec(),
        })
    }

    /// Wait for a notification or timeout after `poll_interval`.
    ///
    /// Returns `Some(payload)` if a notification arrived, or `None` on timeout.
    /// Workers use this in a loop: wake on notification or fall back to polling.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload fails to
    /// deserialize.
    pub async fn wait_for_notification(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<Option<NotifyPayload>> {
        match self.wait_for_notification_outcome(poll_interval).await? {
            QueueWaitOutcome::Notification(payload) => Ok(Some(payload)),
            QueueWaitOutcome::TimedOut | QueueWaitOutcome::ChannelClosed => Ok(None),
        }
    }

    /// Wait for a notification and distinguish timeout from listener shutdown.
    ///
    /// This is useful for callers that need to reconnect after the underlying
    /// LISTEN connection dies instead of treating every wake miss as a normal
    /// timeout.
    pub async fn wait_for_notification_outcome(
        &mut self,
        poll_interval: Duration,
    ) -> HarvestResult<QueueWaitOutcome> {
        match tokio::time::timeout(poll_interval, self.rx.recv()).await {
            Ok(Some(notification)) => {
                let payload: NotifyPayload = serde_json::from_str(notification.payload())
                    .map_err(|e| HarvestError::Database(format!("bad notify payload: {e}")))?;
                Ok(QueueWaitOutcome::Notification(payload))
            }
            Ok(None) => Ok(QueueWaitOutcome::ChannelClosed),
            Err(_elapsed) => Ok(QueueWaitOutcome::TimedOut),
        }
    }

    /// The queue names this listener is subscribed to.
    #[must_use]
    pub fn queues(&self) -> &[String] {
        &self.queues
    }
}

/// Async listener for [`workflow_events_channel`] notifications.
pub struct WorkflowEventListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl WorkflowEventListener {
    /// Connect to Postgres and subscribe to the `harvest_events` channel.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if the URL does not parse, or if TLS
    /// cannot be configured.
    pub async fn connect(database_url: &str) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres workflow event listener error").await?;

        let channel = quote_pg_identifier(workflow_events_channel());
        client
            .batch_execute(&format!("LISTEN {channel}"))
            .await
            .map_err(|e| {
                HarvestError::Database(format!("LISTEN {channel} failed: {}", error_chain(&e)))
            })?;

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
        })
    }

    /// Wait indefinitely for the next workflow event notification.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_notification(&mut self) -> HarvestResult<WorkflowEventWaitOutcome> {
        match self.rx.recv().await {
            Some(notification) => {
                let payload: WorkflowEventNotifyPayload =
                    serde_json::from_str(notification.payload()).map_err(|e| {
                        HarvestError::Database(format!("bad workflow notify payload: {e}"))
                    })?;
                Ok(WorkflowEventWaitOutcome::Notification(payload))
            }
            None => Ok(WorkflowEventWaitOutcome::ChannelClosed),
        }
    }

    /// Wait for a notification up to `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_notification_timeout(
        &mut self,
        timeout: Duration,
    ) -> HarvestResult<WorkflowEventWaitOutcome> {
        match tokio::time::timeout(timeout, self.wait_for_notification()).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Ok(WorkflowEventWaitOutcome::TimedOut),
        }
    }
}

/// Async listener for a single execution's ephemeral progress stream (issue
/// #791).
///
/// Subscribes to the per-execution [`workflow_progress_channel`] and yields each
/// [`ProgressNotifyPayload`] the worker fires via [`notify_workflow_progress`].
/// Intended for the `GET /workflows/{id}/stream` SSE route: connect once per
/// streamed run, then poll [`wait_for_progress_timeout`](Self::wait_for_progress_timeout)
/// so the route can interleave keepalive ticks and terminal-state checks.
pub struct WorkflowProgressListener {
    /// Client handle kept alive so the LISTEN connection stays open.
    _client: tokio_postgres::Client,
    /// Receiver for notifications forwarded by the connection driver task.
    rx: tokio::sync::mpsc::Receiver<tokio_postgres::Notification>,
    /// Background connection driver handle kept alive for the connection's lifetime.
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl WorkflowProgressListener {
    /// Connect to Postgres and subscribe to `exec_id`'s progress channel.
    ///
    /// `sslmode=require` in `database_url` selects verified TLS. Other modes
    /// connect in plaintext (issue #1717).
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the connection or LISTEN fails.
    /// Returns [`HarvestError::Config`] if the URL does not parse, or if TLS
    /// cannot be configured.
    pub async fn connect(database_url: &str, exec_id: Uuid) -> HarvestResult<Self> {
        let ListenConnection { client, rx, driver } =
            open_listen_connection(database_url, "postgres workflow progress listener error")
                .await?;

        let channel = quote_pg_identifier(&workflow_progress_channel(exec_id));
        client
            .batch_execute(&format!("LISTEN {channel}"))
            .await
            .map_err(|e| {
                HarvestError::Database(format!("LISTEN {channel} failed: {}", error_chain(&e)))
            })?;

        Ok(Self {
            _client: client,
            rx,
            _connection_handle: driver,
        })
    }

    /// Wait indefinitely for the next progress chunk.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_progress(&mut self) -> HarvestResult<ProgressWaitOutcome> {
        match self.rx.recv().await {
            Some(notification) => {
                let payload: ProgressNotifyPayload = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                    HarvestError::Database(format!("bad progress notify payload: {e}"))
                })?;
                Ok(ProgressWaitOutcome::Chunk(payload))
            }
            None => Ok(ProgressWaitOutcome::ChannelClosed),
        }
    }

    /// Wait for a progress chunk up to `timeout`.
    ///
    /// A [`ProgressWaitOutcome::TimedOut`] lets the SSE route send a keepalive
    /// and re-check the execution's terminal state before waiting again.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] if the notification payload is invalid.
    pub async fn wait_for_progress_timeout(
        &mut self,
        timeout: Duration,
    ) -> HarvestResult<ProgressWaitOutcome> {
        match tokio::time::timeout(timeout, self.wait_for_progress()).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Ok(ProgressWaitOutcome::TimedOut),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_name_for_queue() {
        assert_eq!(queue_channel("default"), "harvest_queue_default");
        assert_eq!(queue_channel("email-queue"), "harvest_queue_email_queue");
        assert_eq!(
            queue_channel("billing-high-priority"),
            "harvest_queue_billing_high_priority"
        );
    }

    #[test]
    fn notify_payload_roundtrips() {
        let original = NotifyPayload {
            task_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: NotifyPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original.task_id, deserialized.task_id);
    }

    #[test]
    fn channel_name_no_hyphens_in_output() {
        let channel = queue_channel("a-b-c");
        assert!(
            !channel.contains('-'),
            "channel name must not contain hyphens: {channel}"
        );
    }

    #[test]
    fn quoted_identifier_escapes_embedded_quotes() {
        assert_eq!(
            quote_pg_identifier("harvest_queue_priority\"queue"),
            "\"harvest_queue_priority\"\"queue\""
        );
    }

    #[test]
    fn workflow_events_channel_is_stable() {
        assert_eq!(workflow_events_channel(), "harvest_events");
    }

    #[test]
    fn workflow_event_notify_payload_roundtrips() {
        let original = WorkflowEventNotifyPayload {
            workflow_exec_id: Uuid::new_v4(),
            event_count: 2,
            last_event_type: "WorkflowCompleted".to_string(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: WorkflowEventNotifyPayload =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, deserialized);
    }

    // ── Post-commit sender (issue #1796) ─────────────────────────────────

    fn task_note(queue_name: &str, task_id: Uuid) -> Note {
        Note::Task {
            channel: queue_channel(queue_name),
            task_id,
        }
    }

    fn events_note(exec_id: Uuid, count: usize, last_event_type: &str) -> Note {
        Note::Events {
            exec_id,
            count,
            last_event_type: last_event_type.to_string(),
        }
    }

    fn task_payload(task_id: Uuid) -> String {
        serde_json::to_string(&NotifyPayload { task_id }).expect("serialize")
    }

    #[test]
    fn one_task_on_a_channel_keeps_its_task_id() {
        let id = Uuid::new_v4();
        let sent = coalesce(vec![task_note("default", id)]);
        assert_eq!(
            sent,
            vec![("harvest_queue_default".to_string(), task_payload(id))]
        );
    }

    #[test]
    fn several_tasks_on_a_channel_merge_into_one_nil_wake() {
        let sent = coalesce(vec![
            task_note("default", Uuid::new_v4()),
            task_note("email", Uuid::new_v4()),
            task_note("default", Uuid::new_v4()),
        ]);
        assert_eq!(sent.len(), 2);
        assert_eq!(
            sent[0],
            (
                "harvest_queue_default".to_string(),
                task_payload(Uuid::nil())
            )
        );
        assert_eq!(sent[1].0, "harvest_queue_email");
        assert_ne!(sent[1].1, task_payload(Uuid::nil()));
    }

    #[test]
    fn event_notes_merge_for_each_execution() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let sent = coalesce(vec![
            events_note(a, 2, "ActivityScheduled"),
            events_note(b, 1, "WorkflowStarted"),
            events_note(a, 1, "WorkflowCompleted"),
        ]);
        let payloads: Vec<WorkflowEventNotifyPayload> = sent
            .iter()
            .map(|(channel, payload)| {
                assert_eq!(channel, workflow_events_channel());
                serde_json::from_str(payload).expect("payload parses")
            })
            .collect();
        assert_eq!(
            payloads,
            vec![
                WorkflowEventNotifyPayload {
                    workflow_exec_id: a,
                    event_count: 3,
                    last_event_type: "WorkflowCompleted".to_string(),
                },
                WorkflowEventNotifyPayload {
                    workflow_exec_id: b,
                    event_count: 1,
                    last_event_type: "WorkflowStarted".to_string(),
                },
            ]
        );
    }

    #[test]
    fn a_channel_name_postgres_rejects_is_not_valid() {
        assert!(valid_channel("harvest_queue_default"));
        assert!(valid_channel(&"c".repeat(63)));
        assert!(!valid_channel(&"c".repeat(64)));
        assert!(!valid_channel(""));
    }

    #[test]
    fn a_long_queue_channel_is_cut_like_a_postgres_identifier() {
        let channel = queue_channel(&"q".repeat(80));
        assert_eq!(channel.len(), 63);
        assert!(valid_channel(&channel));
        // A two-byte character that crosses byte 63 is dropped whole.
        let channel = queue_channel(&format!("{}é", "q".repeat(48)));
        assert_eq!(channel.len(), 62);
        assert!(channel.ends_with('q'));
        assert_eq!(queue_channel("default"), "harvest_queue_default");
    }

    #[test]
    fn coalesce_drops_a_channel_postgres_rejects() {
        let sent = coalesce(vec![
            Note::Task {
                channel: "c".repeat(64),
                task_id: Uuid::new_v4(),
            },
            task_note("default", Uuid::nil()),
        ]);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "harvest_queue_default");
    }

    #[test]
    fn the_settle_delay_is_jittered_within_its_bounds() {
        let delays: Vec<Duration> = (0..200).map(|_| settle_delay()).collect();
        for delay in &delays {
            assert!(
                (SETTLE_DELAY_MIN..=SETTLE_DELAY_MAX).contains(delay),
                "{delay:?}"
            );
        }
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "the delay must vary between wakes"
        );
    }

    // ── TLS for listener connections (issue #1717) ───────────────────────

    fn transport_for(dsn: &str) -> ListenTransport {
        let config: tokio_postgres::Config = dsn.parse().expect("test DSN parses");
        listen_transport(&config)
    }

    #[test]
    fn only_sslmode_require_selects_tls() {
        let base = "postgres://u:p@db.internal/harvest";
        assert_eq!(transport_for(base), ListenTransport::Plain);
        assert_eq!(
            transport_for(&format!("{base}?sslmode=disable")),
            ListenTransport::Plain
        );
        assert_eq!(
            transport_for(&format!("{base}?sslmode=prefer")),
            ListenTransport::Plain
        );
        assert_eq!(
            transport_for(&format!("{base}?sslmode=require")),
            ListenTransport::Tls
        );
    }

    #[test]
    fn keyword_dsn_with_sslmode_require_selects_tls() {
        assert_eq!(
            transport_for("host=db.internal dbname=harvest sslmode=require"),
            ListenTransport::Tls
        );
        assert_eq!(
            transport_for("host=db.internal dbname=harvest"),
            ListenTransport::Plain
        );
    }

    /// An `SSLRequest` message: length 8, then the code 80877103.
    #[cfg(feature = "tls")]
    const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

    /// Open a listener connection to a fake server, and record what arrives.
    ///
    /// The fake server reads the first message header. When `answer_tls` is
    /// true, it accepts TLS with `S` and also reads the next byte.
    async fn first_bytes_sent(sslmode: &str, answer_tls: bool) -> ([u8; 8], Option<u8>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake server");
        let port = server.local_addr().expect("fake server address").port();
        let accept = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.expect("accept");
            let mut header = [0_u8; 8];
            socket
                .read_exact(&mut header)
                .await
                .expect("message header");
            if !answer_tls {
                return (header, None);
            }
            socket.write_all(b"S").await.expect("accept TLS");
            let mut next = [0_u8; 1];
            let next = socket.read_exact(&mut next).await.ok().map(|_| next[0]);
            (header, next)
        });
        let url = format!("postgres://u@127.0.0.1:{port}/db?sslmode={sslmode}");
        let client = tokio::spawn(async move {
            open_listen_connection(&url, "test listener error")
                .await
                .map(|_| ())
        });
        let seen = tokio::time::timeout(Duration::from_secs(10), accept)
            .await
            .expect("fake server sees the client")
            .expect("fake server task");
        client.abort();
        seen
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn sslmode_require_starts_a_tls_handshake() {
        let (header, next) = first_bytes_sent("require", true).await;
        assert_eq!(header, SSL_REQUEST);
        // 0x16 is the TLS handshake record type, so this is a ClientHello.
        // A `NoTls` connector sends nothing after the server accepts TLS.
        assert_eq!(next, Some(0x16), "sslmode=require must send a ClientHello");
    }

    #[tokio::test]
    async fn sslmode_prefer_stays_plaintext() {
        let (header, _) = first_bytes_sent("prefer", false).await;
        // A startup message carries protocol version 3.0 after its length.
        assert_eq!(header[4..], [0, 3, 0, 0], "prefer must not send SSLRequest");
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn sslmode_require_without_the_tls_feature_is_a_config_error() {
        let result = open_listen_connection(
            "postgres://u@127.0.0.1:1/db?sslmode=require",
            "test listener error",
        )
        .await
        .map(|_| ());
        assert!(
            matches!(&result, Err(HarvestError::Config(m)) if m.contains("`tls` feature")),
            "{:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn an_unparseable_dsn_is_a_config_error() {
        let result = open_listen_connection(
            "postgres://u@127.0.0.1:1/db?sslmode=verify-full",
            "test listener error",
        )
        .await
        .map(|_| ());
        assert!(
            matches!(&result, Err(HarvestError::Config(m)) if m.contains("sslmode")),
            "{:?}",
            result.err()
        );
    }

    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Self>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn error_chain_names_every_cause() {
        let error = Layer(
            "error performing TLS handshake",
            Some(Box::new(Layer(
                "invalid peer certificate",
                Some(Box::new(Layer("UnknownIssuer", None))),
            ))),
        );
        assert_eq!(
            error_chain(&error),
            "error performing TLS handshake: invalid peer certificate: UnknownIssuer"
        );
    }

    // ── publish_progress channel (issue #791) ────────────────────────────

    #[test]
    fn workflow_progress_channel_naming() {
        let exec_id = Uuid::parse_str("0191c1a2-3b4c-7d5e-8f60-112233445566").expect("valid uuid");
        let channel = workflow_progress_channel(exec_id);
        assert_eq!(channel, "harvest_progress_0191c1a23b4c7d5e8f60112233445566");
        assert!(
            !channel.contains('-'),
            "progress channel must not contain hyphens: {channel}"
        );
        // Postgres identifiers are limited to NAMEDATALEN-1 = 63 bytes.
        assert!(
            channel.len() <= 63,
            "progress channel {} exceeds Postgres 63-byte identifier limit ({} bytes)",
            channel,
            channel.len()
        );
    }

    #[test]
    fn progress_notify_payload_roundtrips() {
        let original = ProgressNotifyPayload {
            seq: 0x0000_0005_00FF_FFFF,
            chunk: serde_json::json!({"phase": "mid", "pct": 50}),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let deserialized: ProgressNotifyPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, deserialized);
    }

    #[test]
    fn progress_notify_payload_stays_within_pg_notify_limit() {
        // Postgres `pg_notify` payloads are hard-capped at 8000 bytes. The
        // context caps each chunk's serialized JSON at
        // `PROGRESS_CHUNK_MAX_BYTES` (7000) and this envelope wraps it as
        // `{"seq":..,"chunk":..}`. Pin the worst case directly: a max-`u64` seq
        // (20 decimal digits) plus a chunk serialized right at the cap must
        // still leave the whole envelope under the 8000-byte NOTIFY limit.
        let cap = crate::context::PROGRESS_CHUNK_MAX_BYTES;
        // A JSON string of (cap - 2) chars serializes (with its two quotes) to
        // exactly `cap` bytes — the largest chunk the context will forward.
        let max_chunk = serde_json::Value::String("x".repeat(cap - 2));
        assert_eq!(
            serde_json::to_vec(&max_chunk).unwrap().len(),
            cap,
            "test fixture: chunk must serialize to exactly the cap"
        );
        let payload = ProgressNotifyPayload {
            seq: u64::MAX,
            chunk: max_chunk,
        };
        let serialized = serde_json::to_string(&payload).expect("serialize");
        assert!(
            serialized.len() < 8000,
            "progress NOTIFY envelope must stay under the Postgres 8000-byte \
             pg_notify limit (max-u64 seq + max chunk), was {} bytes",
            serialized.len()
        );
    }
}
