//! Postgres side of remote task calls: scan, poll and settle (issue #2006).
//!
//! The handle of a pending remote task is the input of its
//! `ActivityAwaitingExternal` event. The scan reads it back through the
//! payload codecs, so no migration and no plaintext copy is needed.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use super::{AWAIT_ACTIVITY, RemoteTaskHandle, RemoteTaskState, RemoteTaskTransport, RemoteTasks};
use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::payload_codec::PayloadCodecs;
use crate::schema::{harvest_events, harvest_external_tasks};
use crate::types::{ExecutionId, ExternalActivityToken};
use crate::worker::DbPool;

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
}

/// One page of the scan.
struct Page {
    items: Vec<PendingRemoteTask>,
    /// The last token that the query returned, decoded or not.
    last: Option<ExternalActivityToken>,
    /// `true` when the query returned a full page.
    full: bool,
}

async fn pending_page(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    after: Option<ExternalActivityToken>,
    limit: i64,
) -> HarvestResult<Page> {
    let mut query = harvest_external_tasks::table
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .filter(harvest_external_tasks::name.eq(AWAIT_ACTIVITY))
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
                    "remote task: the handle does not decode; skipped"
                );
            }
        }
    }
    Ok(Page { items, last, full })
}

/// Read the handle from the `ActivityAwaitingExternal` event of `token`.
async fn load_handle(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    execution_id: ExecutionId,
    token: ExternalActivityToken,
) -> HarvestResult<Option<RemoteTaskHandle>> {
    let rows: Vec<serde_json::Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(execution_id.as_uuid()))
        .filter(harvest_events::event_type.eq("ActivityAwaitingExternal"))
        .select(harvest_events::event_data)
        .order(harvest_events::event_id.asc())
        .load(conn)
        .await
        .map_err(database_error)?;
    for row in rows {
        if let WorkflowEvent::ActivityAwaitingExternal {
            token: recorded,
            input,
            ..
        } = codecs.decode_event(row)?
            && recorded == token
        {
            return Ok(Some(serde_json::from_value(input)?));
        }
    }
    Ok(None)
}

/// List pending remote tasks in token order, after `after`.
///
/// The poller uses it. A push relay can use it to find the token of a
/// remote task id. A row whose handle does not decode is skipped.
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
/// A completed task completes the token with its
/// [`RemoteTaskOutcome`](super::RemoteTaskOutcome). A failed or cancelled
/// task fails the token, not retryable. A state that has not ended settles nothing.
///
/// Returns `true` on the first settlement and `false` after it, as the
/// durable promise resolvers do (issue #1985).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the token is unknown on this
/// shard, or [`HarvestError::Database`] when a write fails.
pub async fn resolve(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    state: &RemoteTaskState,
    codecs: &PayloadCodecs,
) -> HarvestResult<bool> {
    match state {
        RemoteTaskState::Working | RemoteTaskState::InputRequired => Ok(false),
        RemoteTaskState::Completed(outcome) => {
            crate::external_task::complete_externally_with_codecs(
                conn,
                token,
                serde_json::to_value(outcome)?,
                codecs,
            )
            .await
        }
        RemoteTaskState::Failed(message) => {
            crate::external_task::fail_externally(
                conn,
                token,
                format!("remote task failed: {message}"),
                false,
            )
            .await
        }
        RemoteTaskState::Cancelled(message) => {
            crate::external_task::fail_externally(
                conn,
                token,
                format!("remote task cancelled: {message}"),
                false,
            )
            .await
        }
    }
}

/// Polls pending remote tasks and settles the ones that ended.
///
/// Run one poller for each shard pool. Two pollers on one shard are safe:
/// the first settlement wins.
pub struct RemoteTaskPoller {
    transport: Arc<dyn RemoteTaskTransport>,
    codecs: PayloadCodecs,
    batch: i64,
    interval: Duration,
    /// The last token of the previous page. `None` starts from the first.
    cursor: Mutex<Option<ExternalActivityToken>>,
}

impl std::fmt::Debug for RemoteTaskPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTaskPoller")
            .field("batch", &self.batch)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl RemoteTaskPoller {
    /// Make a poller with the transport of `remote`.
    ///
    /// The defaults are a page of 100 tokens, a poll every 5 s and the
    /// identity codec.
    #[must_use]
    pub fn new(remote: &RemoteTasks) -> Self {
        Self {
            transport: Arc::clone(remote.transport()),
            codecs: PayloadCodecs::default(),
            batch: 100,
            interval: Duration::from_secs(5),
            cursor: Mutex::new(None),
        }
    }

    /// Use `codecs` to read handles and to write results.
    ///
    /// Pass the codecs of the worker.
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

    /// Poll one page, after the last page. Then start again from the first.
    ///
    /// A transport error leaves its token pending. The deadline of the
    /// token still ends the wait.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Database`] when a query or a write fails.
    pub async fn poll_once(&self, conn: &mut AsyncPgConnection) -> HarvestResult<PollReport> {
        let after = *self.cursor.lock().unwrap_or_else(PoisonError::into_inner);
        let mut page = pending_page(conn, &self.codecs, after, self.batch).await?;
        if page.last.is_none() && after.is_some() {
            page = pending_page(conn, &self.codecs, None, self.batch).await?;
        }
        *self.cursor.lock().unwrap_or_else(PoisonError::into_inner) =
            if page.full { page.last } else { None };

        let mut report = PollReport::default();
        for pending in page.items {
            report.polled += 1;
            let state = match self.transport.get(&pending.handle).await {
                Ok(state) => state,
                Err(e) => {
                    tracing::warn!(
                        execution_id = %pending.execution_id,
                        retryable = e.retryable,
                        "remote task: state read failed; the token stays pending"
                    );
                    continue;
                }
            };
            if !state.is_terminal() {
                continue;
            }
            match resolve(conn, pending.token, &state, &self.codecs).await {
                Ok(true) => report.settled += 1,
                // Another poller or a push settled it first.
                Ok(false) | Err(HarvestError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(report)
    }

    /// Poll until `cancel` fires. A failed poll is logged, and the next
    /// poll runs after the interval.
    pub async fn run(&self, pool: &DbPool, cancel: tokio_util::sync::CancellationToken) {
        while !cancel.is_cancelled() {
            match pool.get().await {
                Ok(mut conn) => match self.poll_once(&mut conn).await {
                    Ok(report) => {
                        if report.settled > 0 {
                            tracing::debug!(
                                polled = report.polled,
                                settled = report.settled,
                                "remote task: poll settled tokens"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "remote task: poll failed");
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "remote task: no database connection for the poll");
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(self.interval) => {}
            }
        }
    }
}
