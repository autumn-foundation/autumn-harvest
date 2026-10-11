//! Postgres side of remote task calls: scan, poll and settle (issue #2006).
//!
//! The handle of a pending remote task is the input of its
//! `ActivityAwaitingExternal` event. The scan reads it back through the
//! payload codecs. So the change needs no migration, and it keeps no
//! plaintext copy of the handle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::StreamExt as _;

use super::{
    AWAIT_ACTIVITY, RemoteTaskError, RemoteTaskHandle, RemoteTaskState, RemoteTaskTransport,
    RemoteTasks,
};
use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::payload_codec::PayloadCodecs;
use crate::schema::{harvest_events, harvest_external_tasks, harvest_workflow_executions};
use crate::types::{ExecutionId, ExternalActivityToken};
use crate::worker::DbPool;

/// The run states in which a token can still settle.
const LIVE_STATES: [&str; 3] = ["RUNNING", "SUSPENDED", "PAUSED"];

/// The largest failure message that a settle writes, in bytes.
pub const MAX_MESSAGE_BYTES: usize = 4096;

/// The longest wait before the poller reads a failing task again.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

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
}

/// What one poll did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Remote reads that this poll sent.
    pub polled: usize,
    /// Tokens that this poll settled.
    pub settled: usize,
    /// Remote reads and settles that failed. Their tokens stay pending.
    pub errors: usize,
}

/// One page of the scan.
struct Page {
    items: Vec<PendingRemoteTask>,
    /// The last token that the query returned, decoded or not.
    last: Option<ExternalActivityToken>,
    /// `true` when the query returned a full page.
    full: bool,
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
        ))
        .order(harvest_external_tasks::token.asc())
        .limit(limit)
        .into_boxed();
    if let Some(after) = after {
        query = query.filter(harvest_external_tasks::token.gt(after.as_uuid()));
    }
    let rows: Vec<(uuid::Uuid, uuid::Uuid)> = query.load(conn).await.map_err(database_error)?;
    let full = i64::try_from(rows.len()).is_ok_and(|n| n >= limit);
    let last = rows
        .last()
        .map(|(token, _)| ExternalActivityToken::from_uuid(*token));
    let mut items = Vec::with_capacity(rows.len());
    for (token, exec) in rows {
        let token = ExternalActivityToken::from_uuid(token);
        let execution_id = ExecutionId::from_uuid(exec);
        match load_handle(conn, codecs, execution_id, token).await {
            Ok(Some(handle)) => items.push(PendingRemoteTask {
                token,
                execution_id,
                handle,
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
    Ok(Page { items, last, full })
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

/// List pending remote tasks of live runs in token order, after `after`.
///
/// The poller uses it. A push relay can use it to find the token of a
/// remote task. Match on the server and the task id together. A row whose
/// handle does not decode is skipped.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when a query fails.
pub async fn pending_remote_tasks(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    after: Option<ExternalActivityToken>,
    limit: i64,
) -> HarvestResult<Vec<PendingRemoteTask>> {
    Ok(pending_page(conn, codecs, after, limit).await?.items)
}

/// Settle a token from a remote task state.
///
/// - A completed task completes the token with its
///   [`RemoteTaskOutcome`](super::RemoteTaskOutcome). An outcome over
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
    if !state.is_terminal() {
        return Ok(false);
    }
    let task = crate::external_task::find_by_token(conn, token)
        .await?
        .ok_or_else(|| HarvestError::NotFound(format!("external task token {token}")))?;
    if task.name != AWAIT_ACTIVITY {
        return Err(HarvestError::Config(format!(
            "external task {token} is not a remote task token"
        )));
    }
    let run_state: Option<String> = harvest_workflow_executions::table
        .find(task.workflow_exec_id)
        .select(harvest_workflow_executions::state)
        .first(conn)
        .await
        .optional_row()?;
    if !run_state.is_some_and(|s| LIVE_STATES.contains(&s.as_str())) {
        return Ok(false);
    }
    let message = match state {
        RemoteTaskState::Working | RemoteTaskState::InputRequired => return Ok(false),
        RemoteTaskState::Completed(outcome) => {
            let output = serde_json::to_value(outcome)?;
            let size = serde_json::to_vec(&output).map_or(u64::MAX, |b| b.len() as u64);
            if size <= max_result_bytes {
                return crate::external_task::complete_externally_with_codecs(
                    conn, token, output, codecs,
                )
                .await;
            }
            format!("remote task result is {size} bytes; the limit is {max_result_bytes}")
        }
        RemoteTaskState::Failed(message) => format!("remote task failed: {}", truncate(message)),
        RemoteTaskState::Cancelled(message) => {
            format!("remote task cancelled: {}", truncate(message))
        }
    };
    crate::external_task::fail_externally(conn, token, message, false).await
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
    /// Tokens whose last remote read failed.
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
    /// worker `max_activity_result_bytes`.
    #[must_use]
    pub const fn with_max_result_bytes(mut self, bytes: u64) -> Self {
        self.max_result_bytes = bytes;
        self
    }

    /// Poll one page, after the last page. Then start again from the first.
    ///
    /// The poll holds no database connection while it waits for a remote
    /// server. A failed remote read leaves its token pending, and the next
    /// reads of that token back off up to 5 minutes. The deadline of the
    /// token still ends the wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the page query fails. A failed settle of one
    /// token is logged and counted in [`PollReport::errors`].
    pub async fn poll_once(&self, pool: &DbPool) -> HarvestResult<PollReport> {
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
        let due: Vec<PendingRemoteTask> = {
            let backoff = self.backoff.lock().unwrap_or_else(PoisonError::into_inner);
            page.items
                .into_iter()
                .filter(|p| backoff.get(&p.token).is_none_or(|b| b.until <= now))
                .collect()
        };
        let mut report = PollReport {
            polled: due.len(),
            ..PollReport::default()
        };
        let reads: Vec<(PendingRemoteTask, Result<RemoteTaskState, RemoteTaskError>)> =
            futures::stream::iter(due)
                .map(|pending| async move {
                    let read =
                        tokio::time::timeout(self.get_timeout, self.transport.get(&pending.handle))
                            .await
                            .unwrap_or_else(|_| {
                                Err(RemoteTaskError::retryable("the remote read timed out"))
                            });
                    (pending, read)
                })
                .buffer_unordered(self.concurrency)
                .collect()
                .await;

        let mut ended = Vec::new();
        for (pending, read) in reads {
            match read {
                Ok(state) => {
                    self.clear_backoff(pending.token);
                    if state.is_terminal() {
                        ended.push((pending, state));
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
        if ended.is_empty() {
            return Ok(report);
        }
        let mut conn = pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(format!("remote task settle: {e}")))?;
        for (pending, state) in ended {
            let settled = settle(
                &mut conn,
                pending.token,
                &state,
                &self.codecs,
                self.max_result_bytes,
            )
            .await;
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
        Ok(report)
    }

    fn back_off(&self, token: ExternalActivityToken) {
        let mut backoff = self.backoff.lock().unwrap_or_else(PoisonError::into_inner);
        if backoff.len() >= MAX_BACKOFF_ENTRIES {
            backoff.clear();
        }
        let errors = backoff
            .get(&token)
            .map_or(1, |b| b.errors.saturating_add(1));
        let delay = self
            .interval
            .saturating_mul(1_u32 << errors.min(16))
            .min(MAX_BACKOFF);
        backoff.insert(
            token,
            Backoff {
                errors,
                until: Instant::now() + delay,
            },
        );
    }

    fn clear_backoff(&self, token: ExternalActivityToken) {
        self.backoff
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&token);
    }

    /// Poll until `cancel` fires. The poller logs a failed poll, and the
    /// next poll runs after the interval.
    pub async fn run(&self, pool: &DbPool, cancel: tokio_util::sync::CancellationToken) {
        while !cancel.is_cancelled() {
            match self.poll_once(pool).await {
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
    use super::{MAX_MESSAGE_BYTES, truncate};

    #[test]
    fn truncate_cuts_on_a_char_boundary() {
        let long = "é".repeat(MAX_MESSAGE_BYTES);
        let cut = truncate(&long);
        assert!(cut.len() <= MAX_MESSAGE_BYTES);
        assert!(cut.chars().all(|c| c == 'é'));
        assert_eq!(truncate("short"), "short");
    }
}
