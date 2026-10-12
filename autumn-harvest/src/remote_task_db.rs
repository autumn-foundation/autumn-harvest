//! Postgres side of remote task calls: scan, poll and settle (issue #2006).
//!
//! The handle of a pending remote task is the input of its
//! `ActivityAwaitingExternal` event. The scan reads it back through the
//! payload codecs. So the change needs no migration, and it keeps no
//! plaintext copy of the handle.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl};
use futures::StreamExt as _;

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::payload_codec::PayloadCodecs;
use crate::remote_task::{
    AWAIT_ACTIVITY, RemoteTaskError, RemoteTaskHandle, RemoteTaskPoll, RemoteTaskState,
    RemoteTaskTransport, RemoteTasks,
};
use crate::schema::{harvest_events, harvest_external_tasks, harvest_workflow_executions};
use crate::types::{ExecutionId, ExternalActivityToken};
use crate::worker::DbPool;

/// The run states in which a token can still settle.
const LIVE_STATES: [&str; 3] = ["RUNNING", "SUSPENDED", "PAUSED"];

/// The largest failure message that a settle writes, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 4096;

/// The longest wait before the poller reads a failing task again.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// The longest poll interval that the poller takes from a server.
const MAX_SERVER_POLL_INTERVAL: Duration = Duration::from_secs(3600);

/// The most tokens that the poller tracks with a backoff.
const MAX_BACKOFF_ENTRIES: usize = 10_000;

/// One pending remote task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRemoteTask {
    /// The external task token.
    pub token: ExternalActivityToken,
    /// The run that waits.
    pub execution_id: ExecutionId,
    /// The remote task handle.
    pub handle: RemoteTaskHandle,
    /// When the token was recorded.
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

/// What one poll did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Tokens due for a remote read in this poll.
    pub polled: usize,
    /// Tokens that this poll settled.
    pub settled: usize,
    /// Remote reads and settles that failed. Their tokens stay pending.
    pub errors: usize,
    /// Due reads not sent, because a read of their server failed with a
    /// retryable error earlier in this poll. Their tokens stay pending, with
    /// no backoff.
    pub skipped: usize,
}

/// One row of the page query: token, run, token time, database time.
type PageRow = (
    uuid::Uuid,
    uuid::Uuid,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

/// One page of the scan.
struct Page {
    items: Vec<PendingRemoteTask>,
    /// The last token that the query returned, decoded or not.
    last: Option<ExternalActivityToken>,
    /// `true` when the query returned a full page.
    full: bool,
    /// The database time of the query. The first-read wait uses it, so
    /// both sides of the compare use one clock.
    db_now: chrono::DateTime<chrono::Utc>,
}

/// `first` with "no row" as `None`.
trait OptionalRow<T> {
    fn optional_row(self) -> HarvestResult<Option<T>>;
}

impl<T> OptionalRow<T> for Result<T, diesel::result::Error> {
    fn optional_row(self) -> HarvestResult<Option<T>> {
        use diesel::OptionalExtension as _;
        self.optional().map_err(database_error)
    }
}

async fn pending_page(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    after: Option<ExternalActivityToken>,
    limit: i64,
) -> HarvestResult<Page> {
    let mut query = harvest_external_tasks::table
        .inner_join(harvest_workflow_executions::table)
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .filter(harvest_external_tasks::name.eq(AWAIT_ACTIVITY))
        .filter(harvest_workflow_executions::state.eq_any(LIVE_STATES))
        .select((
            harvest_external_tasks::token,
            harvest_external_tasks::workflow_exec_id,
            harvest_external_tasks::created_at,
            diesel::dsl::sql::<diesel::sql_types::Timestamptz>("now()"),
        ))
        .order(harvest_external_tasks::token.asc())
        .limit(limit)
        .into_boxed();
    if let Some(after) = after {
        query = query.filter(harvest_external_tasks::token.gt(after.as_uuid()));
    }
    let rows: Vec<PageRow> = query.load(conn).await.map_err(database_error)?;
    let db_now = rows
        .as_slice()
        .first()
        .map_or_else(chrono::Utc::now, |row| row.3);
    let full = i64::try_from(rows.len()).is_ok_and(|n| n >= limit);
    let last = rows
        .last()
        .map(|(token, ..)| ExternalActivityToken::from_uuid(*token));
    let mut items = Vec::with_capacity(rows.len());
    for (token, exec, recorded_at, _) in rows {
        let token = ExternalActivityToken::from_uuid(token);
        let execution_id = ExecutionId::from_uuid(exec);
        match load_handle(conn, codecs, execution_id, token).await {
            Ok(Some(handle)) => items.push(PendingRemoteTask {
                token,
                execution_id,
                handle,
                recorded_at,
            }),
            Ok(None) => {
                tracing::warn!(
                    execution_id = %execution_id,
                    "remote task: no readable handle in history; skipped"
                );
            }
            Err(e @ HarvestError::Database(_)) => return Err(e),
            Err(_) => {
                tracing::warn!(
                    execution_id = %execution_id,
                    "remote task: the handle does not decode; check the poller codecs; skipped"
                );
            }
        }
    }
    Ok(Page {
        items,
        last,
        full,
        db_now,
    })
}

/// Read the handle from the `ActivityAwaitingExternal` event of `token`.
///
/// The token is outside the payload fields, so the query matches it as
/// plaintext. Other external activities of the run stay out of the decode.
async fn load_handle(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    execution_id: ExecutionId,
    token: ExternalActivityToken,
) -> HarvestResult<Option<RemoteTaskHandle>> {
    let row: Option<serde_json::Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(execution_id.as_uuid()))
        .filter(harvest_events::event_type.eq("ActivityAwaitingExternal"))
        .filter(
            diesel::dsl::sql::<diesel::sql_types::Bool>("event_data -> 'data' ->> 'token' = ")
                .bind::<diesel::sql_types::Text, _>(token.to_string()),
        )
        .select(harvest_events::event_data)
        .order(harvest_events::event_id.asc())
        .first(conn)
        .await
        .optional_row()?;
    let Some(row) = row else {
        return Ok(None);
    };
    match codecs.decode_event(row)? {
        WorkflowEvent::ActivityAwaitingExternal {
            token: recorded,
            input,
            ..
        } if recorded == token => Ok(Some(serde_json::from_value(input)?)),
        _ => Ok(None),
    }
}

/// One page of [`pending_remote_tasks`].
#[derive(Debug, Clone)]
pub struct PendingPage {
    /// The pending remote tasks whose handle decodes, in token order.
    pub items: Vec<PendingRemoteTask>,
    /// The `after` value for the next page, or `None` at the end. It moves
    /// past the rows whose handle does not decode too.
    pub next: Option<ExternalActivityToken>,
}

/// List pending remote tasks of live runs in token order, after `after`.
///
/// The poller uses it. A push relay can use it to find the token of a
/// remote task. Match on the server and the task id together. A row whose
/// handle does not decode is skipped. Page with [`PendingPage::next`], not
/// with the last item, so a page of skipped rows does not stop the scan.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when a query fails.
pub async fn pending_remote_tasks(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    after: Option<ExternalActivityToken>,
    limit: i64,
) -> HarvestResult<PendingPage> {
    let page = pending_page(conn, codecs, after, limit).await?;
    Ok(PendingPage {
        items: page.items,
        next: if page.full { page.last } else { None },
    })
}

/// Settle a token from a remote task state.
///
/// - A completed task completes the token with its
///   [`RemoteTaskOutcome`](crate::remote_task::RemoteTaskOutcome). An outcome over
///   [`DEFAULT_MAX_ACTIVITY_RESULT_BYTES`](crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES)
///   fails the token instead.
/// - A failed or cancelled task fails the token, not retryable. The message
///   keeps at most [`MAX_MESSAGE_BYTES`].
/// - A state that has not ended settles nothing.
///
/// Returns `true` on the first settlement and `false` after it, as the
/// durable promise resolvers do (issue #1985). A token of a run that has
/// ended also returns `false`.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the token is unknown on this shard.
/// - [`HarvestError::Config`] when the token is not a remote task token.
/// - [`HarvestError::Database`] when a query or a write fails.
pub async fn resolve(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    state: &RemoteTaskState,
    codecs: &PayloadCodecs,
) -> HarvestResult<bool> {
    settle(
        conn,
        token,
        state,
        codecs,
        crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
    )
    .await
}

async fn settle(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    state: &RemoteTaskState,
    codecs: &PayloadCodecs,
    max_result_bytes: u64,
) -> HarvestResult<bool> {
    let action = match state {
        RemoteTaskState::Working | RemoteTaskState::InputRequired => return Ok(false),
        RemoteTaskState::Completed(outcome) => {
            let output = serde_json::to_value(outcome)?;
            let size = serde_json::to_vec(&output).map_or(u64::MAX, |b| b.len() as u64);
            // A cap of 0 means no cap, as on the worker result paths.
            if max_result_bytes > 0 && size > max_result_bytes {
                Settle::Fail(format!(
                    "remote task result is {size} bytes; the limit is {max_result_bytes}"
                ))
            } else {
                Settle::Complete(output)
            }
        }
        RemoteTaskState::Failed(message) => {
            Settle::Fail(format!("remote task failed: {}", truncate(message)))
        }
        RemoteTaskState::Cancelled(message) => {
            Settle::Fail(format!("remote task cancelled: {}", truncate(message)))
        }
    };
    // One transaction holds the token row from the name check to the settle.
    // The core settle locks the execution row next and returns `false` for an
    // ended run, so a cancel cannot slip in between. The settle wakes the
    // workflow, so the dispatch hint waits for this commit. The inner
    // buffering scope passes its hints to this one.
    crate::dispatch::buffered_settled(Box::pin(conn.transaction::<bool, HarvestError, _>(
        async |conn| {
            let task = crate::external_task::find_by_token_locked(conn, token)
                .await?
                .ok_or_else(|| HarvestError::NotFound(format!("external task token {token}")))?;
            if task.name != AWAIT_ACTIVITY {
                return Err(HarvestError::Config(format!(
                    "external task {token} is not a remote task token"
                )));
            }
            match action {
                Settle::Complete(output) => {
                    crate::external_task::complete_externally_with_codecs(
                        conn, token, output, codecs,
                    )
                    .await
                }
                Settle::Fail(message) => {
                    crate::external_task::fail_externally(conn, token, message, false).await
                }
            }
        },
    )))
    .await
}

/// What a settle writes.
enum Settle {
    Complete(serde_json::Value),
    Fail(String),
}

/// `message`, cut to at most [`MAX_MESSAGE_BYTES`] on a char boundary.
fn truncate(message: &str) -> &str {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message;
    }
    let mut end = MAX_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}

/// The backoff of one token whose remote read failed.
#[derive(Debug, Clone, Copy)]
struct Backoff {
    errors: u32,
    until: Instant,
}

/// Record the wait of `token`. A full map first drops its expired waits.
/// When it is still full, a new token gets no wait, and no live wait is lost.
fn record_wait(
    waits: &mut HashMap<ExternalActivityToken, Backoff>,
    token: ExternalActivityToken,
    entry: Backoff,
    now: Instant,
) {
    if waits.len() >= MAX_BACKOFF_ENTRIES && !waits.contains_key(&token) {
        waits.retain(|_, wait| wait.until > now);
        if waits.len() >= MAX_BACKOFF_ENTRIES {
            return;
        }
    }
    waits.insert(token, entry);
}

/// Order `due` so that the servers take turns: the first task of each
/// server, then the second, and so on. A slow server then holds at most its
/// share of the read slots, and the reads of other servers still start.
fn interleave_by_server(due: Vec<PendingRemoteTask>) -> Vec<PendingRemoteTask> {
    let mut queues: Vec<(String, std::collections::VecDeque<PendingRemoteTask>)> = Vec::new();
    for pending in due {
        match queues
            .iter_mut()
            .find(|(server, _)| *server == pending.handle.server)
        {
            Some((_, queue)) => queue.push_back(pending),
            None => queues.push((pending.handle.server.clone(), [pending].into())),
        }
    }
    let mut order = Vec::new();
    while !queues.is_empty() {
        queues.retain_mut(|(_, queue)| {
            if let Some(pending) = queue.pop_front() {
                order.push(pending);
            }
            !queue.is_empty()
        });
    }
    order
}

/// Polls pending remote tasks and settles the ones that ended.
///
/// Run one poller for each shard pool. Two pollers on one shard are safe:
/// the first settlement wins. Pass the worker codecs with
/// [`Self::with_codecs`], or the poller cannot read an encoded handle.
pub struct RemoteTaskPoller {
    transport: Arc<dyn RemoteTaskTransport>,
    codecs: PayloadCodecs,
    batch: i64,
    interval: Duration,
    concurrency: usize,
    get_timeout: Duration,
    max_result_bytes: u64,
    /// The last token of the previous page. `None` starts from the first.
    cursor: Mutex<Option<ExternalActivityToken>>,
    /// Tokens that wait: after a failed read, or for the poll interval of
    /// the server.
    backoff: Mutex<HashMap<ExternalActivityToken, Backoff>>,
}

impl std::fmt::Debug for RemoteTaskPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTaskPoller")
            .field("batch", &self.batch)
            .field("interval", &self.interval)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl RemoteTaskPoller {
    /// Make a poller with the transport of `remote`.
    ///
    /// The defaults:
    ///
    /// - a page of 100 tokens, and a poll every 5 s;
    /// - 8 remote reads at a time, each with a limit of 30 s;
    /// - a result limit of
    ///   [`DEFAULT_MAX_ACTIVITY_RESULT_BYTES`](crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES);
    /// - the identity codec.
    #[must_use]
    pub fn new(remote: &RemoteTasks) -> Self {
        Self {
            transport: Arc::clone(remote.transport()),
            codecs: PayloadCodecs::default(),
            batch: 100,
            interval: Duration::from_secs(5),
            concurrency: 8,
            get_timeout: Duration::from_secs(30),
            max_result_bytes: crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
            cursor: Mutex::new(None),
            backoff: Mutex::new(HashMap::new()),
        }
    }

    /// Use `codecs` to read handles and to write results. Pass the codecs
    /// of the worker.
    #[must_use]
    pub fn with_codecs(mut self, codecs: PayloadCodecs) -> Self {
        self.codecs = codecs;
        self
    }

    /// Read at most `batch` tokens in one poll. A value below 1 counts as 1.
    #[must_use]
    pub const fn with_batch_size(mut self, batch: i64) -> Self {
        self.batch = if batch < 1 { 1 } else { batch };
        self
    }

    /// Wait `interval` between two polls in [`Self::run`].
    #[must_use]
    pub const fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Send at most `concurrency` remote reads at a time. A value of 0
    /// counts as 1.
    #[must_use]
    pub const fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = if concurrency == 0 { 1 } else { concurrency };
        self
    }

    /// Stop a remote read after `timeout`. The token stays pending.
    #[must_use]
    pub const fn with_get_timeout(mut self, timeout: Duration) -> Self {
        self.get_timeout = timeout;
        self
    }

    /// Fail a token whose remote result is over `bytes`. Set it to the
    /// worker `max_activity_result_bytes`. A value of 0 means no cap.
    #[must_use]
    pub const fn with_max_result_bytes(mut self, bytes: u64) -> Self {
        self.max_result_bytes = bytes;
        self
    }

    /// Poll one page, after the last page. Then start again from the first.
    ///
    /// The poll holds no database connection while it waits for a remote
    /// server. A failed remote read leaves its token pending, and the next
    /// reads of that token back off up to 5 minutes. A server poll interval
    /// longer than ours delays the next read, up to 1 hour. The deadline of
    /// the token still ends the wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the page query fails. A failed settle of one
    /// token is logged and counted in [`PollReport::errors`].
    pub async fn poll_once(&self, pool: &DbPool) -> HarvestResult<PollReport> {
        self.poll_until(pool, &tokio_util::sync::CancellationToken::new())
            .await
    }

    /// [`Self::poll_once`] that stops early when `cancel` fires. It stops
    /// between reads: the remote reads in flight are dropped, but a settle
    /// that has begun always ends, so no transaction is cut off.
    async fn poll_until(
        &self,
        pool: &DbPool,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> HarvestResult<PollReport> {
        let page = {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| HarvestError::Database(format!("remote task poll: {e}")))?;
            let after = *self.cursor.lock().unwrap_or_else(PoisonError::into_inner);
            let mut page = pending_page(&mut conn, &self.codecs, after, self.batch).await?;
            if page.last.is_none() && after.is_some() {
                page = pending_page(&mut conn, &self.codecs, None, self.batch).await?;
            }
            page
        };
        *self.cursor.lock().unwrap_or_else(PoisonError::into_inner) =
            if page.full { page.last } else { None };

        let now = Instant::now();
        let db_now = page.db_now;
        let due: Vec<PendingRemoteTask> = {
            let backoff = self.backoff.lock().unwrap_or_else(PoisonError::into_inner);
            page.items
                .into_iter()
                .filter(|p| backoff.get(&p.token).is_none_or(|b| b.until <= now))
                .filter(|p| Self::first_read_due(p, db_now))
                .collect()
        };
        let mut report = PollReport {
            polled: due.len(),
            ..PollReport::default()
        };
        // A server whose read failed with a retryable error (a timeout, a
        // lost connection, 429 or 5xx) gets no more reads in this poll. A
        // hanging or busy server then costs one wave of reads, not one per
        // task, and the next page comes sooner.
        let failing: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
        let failing = &failing;
        let mut reads = futures::stream::iter(interleave_by_server(due))
            .map(|pending| async move {
                let server = &pending.handle.server;
                if failing
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(server)
                {
                    return (pending, None);
                }
                let read =
                    tokio::time::timeout(self.get_timeout, self.transport.get(&pending.handle))
                        .await
                        .unwrap_or_else(|_| {
                            Err(RemoteTaskError::retryable("the remote read timed out"))
                        });
                if read.as_ref().is_err_and(|e| e.retryable) {
                    failing
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(server.clone());
                }
                (pending, Some(read))
            })
            .buffer_unordered(self.concurrency);
        // Settle each ended task when its read lands. A slow read must not
        // keep a result back until the token deadline passes.
        loop {
            let next = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                next = reads.next() => next,
            };
            let Some((pending, read)) = next else {
                break;
            };
            let Some(read) = read else {
                report.skipped += 1;
                continue;
            };
            match read {
                Ok(RemoteTaskPoll {
                    state,
                    poll_interval,
                }) => {
                    if state.is_terminal() {
                        self.clear_backoff(pending.token);
                        self.settle_ended(pool, &pending, &state, &mut report).await;
                    } else {
                        self.wait_for_server(pending.token, poll_interval);
                    }
                }
                Err(e) => {
                    report.errors += 1;
                    self.back_off(pending.token);
                    tracing::warn!(
                        execution_id = %pending.execution_id,
                        retryable = e.retryable,
                        "remote task: state read failed; the token stays pending"
                    );
                }
            }
        }
        Ok(report)
    }

    /// Settle one ended task. The connection goes back to the pool at once.
    async fn settle_ended(
        &self,
        pool: &DbPool,
        pending: &PendingRemoteTask,
        state: &RemoteTaskState,
        report: &mut PollReport,
    ) {
        let settled = match pool.get().await {
            Ok(mut conn) => {
                settle(
                    &mut conn,
                    pending.token,
                    state,
                    &self.codecs,
                    self.max_result_bytes,
                )
                .await
            }
            Err(e) => Err(HarvestError::Database(format!("remote task settle: {e}"))),
        };
        match settled {
            Ok(true) => report.settled += 1,
            // Another poller, a push or the timeout scan settled it first.
            Ok(false) | Err(HarvestError::NotFound(_)) => {}
            Err(e) => {
                report.errors += 1;
                tracing::warn!(
                    execution_id = %pending.execution_id,
                    error = %e,
                    "remote task: settle failed; the token stays pending"
                );
            }
        }
    }

    fn back_off(&self, token: ExternalActivityToken) {
        let mut backoff = self.backoff.lock().unwrap_or_else(PoisonError::into_inner);
        let errors = backoff
            .get(&token)
            .map_or(1, |b| b.errors.saturating_add(1));
        let delay = self
            .interval
            .saturating_mul(1_u32 << errors.min(16))
            .min(MAX_BACKOFF);
        let now = Instant::now();
        let entry = Backoff {
            errors,
            until: now + delay,
        };
        record_wait(&mut backoff, token, entry, now);
        drop(backoff);
    }

    /// `false` while the start poll interval of `pending` runs at the
    /// database time `db_now`. The token time is in the database, so a
    /// restart keeps the wait.
    fn first_read_due(pending: &PendingRemoteTask, db_now: chrono::DateTime<chrono::Utc>) -> bool {
        let Some(wait) = pending.handle.poll_interval_ms.map(Duration::from_millis) else {
            return true;
        };
        chrono::Duration::from_std(wait.min(MAX_SERVER_POLL_INTERVAL))
            .ok()
            .and_then(|wait| pending.recorded_at.checked_add_signed(wait))
            .is_none_or(|due| db_now >= due)
    }

    /// Respect the poll interval of the server, when it is longer than ours.
    fn wait_for_server(&self, token: ExternalActivityToken, interval: Option<Duration>) {
        let Some(interval) = interval.filter(|i| *i > self.interval) else {
            self.clear_backoff(token);
            return;
        };
        let now = Instant::now();
        let entry = Backoff {
            errors: 0,
            until: now + interval.min(MAX_SERVER_POLL_INTERVAL),
        };
        let mut waits = self.backoff.lock().unwrap_or_else(PoisonError::into_inner);
        record_wait(&mut waits, token, entry, now);
        drop(waits);
    }

    fn clear_backoff(&self, token: ExternalActivityToken) {
        self.backoff
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&token);
    }

    /// Poll until `cancel` fires. The poller logs a failed poll, and the
    /// next poll runs after the interval. A cancel also ends the poll in
    /// flight, between two reads.
    pub async fn run(&self, pool: &DbPool, cancel: tokio_util::sync::CancellationToken) {
        while !cancel.is_cancelled() {
            match self.poll_until(pool, &cancel).await {
                Ok(report) if report.settled > 0 => {
                    tracing::debug!(
                        polled = report.polled,
                        settled = report.settled,
                        "remote task: poll settled tokens"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "remote task: poll failed"),
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(self.interval) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use super::{
        Backoff, MAX_BACKOFF_ENTRIES, MAX_MESSAGE_BYTES, PendingRemoteTask, RemoteTaskPoller,
        interleave_by_server, record_wait, truncate,
    };
    use crate::remote_task::{RemoteProtocol, RemoteTaskHandle};
    use crate::types::{ExecutionId, ExternalActivityToken};

    fn full_map(until: Instant) -> HashMap<ExternalActivityToken, Backoff> {
        (0..MAX_BACKOFF_ENTRIES)
            .map(|_| (ExternalActivityToken::new(), Backoff { errors: 1, until }))
            .collect()
    }

    #[test]
    fn a_full_map_keeps_its_live_waits() {
        let now = Instant::now();
        let mut waits = full_map(now + Duration::from_secs(60));
        let token = ExternalActivityToken::new();
        let entry = Backoff {
            errors: 0,
            until: now + Duration::from_secs(60),
        };
        record_wait(&mut waits, token, entry, now);
        assert_eq!(waits.len(), MAX_BACKOFF_ENTRIES, "no live wait is dropped");
        assert!(!waits.contains_key(&token));
    }

    #[test]
    fn a_full_map_drops_its_expired_waits() {
        let now = Instant::now();
        let mut waits = full_map(now);
        let token = ExternalActivityToken::new();
        let entry = Backoff {
            errors: 0,
            until: now + Duration::from_secs(60),
        };
        record_wait(&mut waits, token, entry, now);
        assert_eq!(waits.len(), 1);
        assert!(waits.contains_key(&token));
    }

    fn pending(server: &str, task_id: &str) -> PendingRemoteTask {
        PendingRemoteTask {
            token: ExternalActivityToken::new(),
            execution_id: ExecutionId::new(),
            handle: RemoteTaskHandle {
                server: server.into(),
                protocol: RemoteProtocol::Mcp,
                task_id: task_id.into(),
                poll_interval_ms: None,
            },
            recorded_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn reads_alternate_between_servers() {
        let due = vec![
            pending("slow", "s1"),
            pending("slow", "s2"),
            pending("slow", "s3"),
            pending("fast", "f1"),
            pending("other", "o1"),
            pending("fast", "f2"),
        ];
        let order: Vec<String> = interleave_by_server(due)
            .into_iter()
            .map(|p| p.handle.task_id)
            .collect();
        assert_eq!(order, ["s1", "f1", "o1", "s2", "f2", "s3"]);
    }

    #[test]
    fn the_first_read_uses_the_database_clock() {
        // A database clock one day ahead of this host.
        let recorded_at = chrono::Utc::now() + chrono::Duration::days(1);
        let pending = PendingRemoteTask {
            token: ExternalActivityToken::new(),
            execution_id: ExecutionId::new(),
            handle: RemoteTaskHandle {
                server: "reports".into(),
                protocol: RemoteProtocol::Mcp,
                task_id: "t-1".into(),
                poll_interval_ms: Some(5_000),
            },
            recorded_at,
        };
        let early = recorded_at + chrono::Duration::seconds(4);
        let late = recorded_at + chrono::Duration::seconds(6);
        assert!(!RemoteTaskPoller::first_read_due(&pending, early));
        assert!(RemoteTaskPoller::first_read_due(&pending, late));
    }

    #[test]
    fn truncate_cuts_on_a_char_boundary() {
        let long = "é".repeat(MAX_MESSAGE_BYTES);
        let cut = truncate(&long);
        assert!(cut.len() <= MAX_MESSAGE_BYTES);
        assert!(cut.chars().all(|c| c == 'é'));
        assert_eq!(truncate("short"), "short");
    }
}
