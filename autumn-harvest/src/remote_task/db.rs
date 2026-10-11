//! Postgres side of remote task calls: scan, poll and settle (issue #2006).

use std::sync::Arc;
use std::time::Duration;

use diesel_async::AsyncPgConnection;

use super::{RemoteTaskHandle, RemoteTaskState, RemoteTaskTransport, RemoteTasks};
use crate::error::HarvestResult;
use crate::payload_codec::PayloadCodecs;
use crate::types::ExternalActivityToken;
use crate::worker::DbPool;

/// One pending remote task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRemoteTask {
    /// The external task token.
    pub token: ExternalActivityToken,
    /// The remote task handle.
    pub handle: RemoteTaskHandle,
}

/// What one poll did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PollReport {
    /// Tokens read from the remote server.
    pub polled: usize,
    /// Tokens settled by this poll.
    pub settled: usize,
}

/// List pending remote tasks in token order, after `after`.
///
/// # Errors
///
/// Returns an error when a query fails.
pub async fn pending_remote_tasks(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    after: Option<ExternalActivityToken>,
    limit: i64,
) -> HarvestResult<Vec<PendingRemoteTask>> {
    let _ = (conn, codecs, after, limit);
    todo!("issue #2006")
}

/// Settle a token from a remote task state.
///
/// # Errors
///
/// Returns an error when the token is unknown or a write fails.
pub async fn resolve(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    state: &RemoteTaskState,
    codecs: &PayloadCodecs,
) -> HarvestResult<bool> {
    let _ = (conn, token, state, codecs);
    todo!("issue #2006")
}

/// Polls pending remote tasks and settles the ones that ended.
pub struct RemoteTaskPoller {
    transport: Arc<dyn RemoteTaskTransport>,
    codecs: PayloadCodecs,
    batch: i64,
    interval: Duration,
}

impl RemoteTaskPoller {
    /// Make a poller.
    #[must_use]
    pub fn new(remote: &RemoteTasks) -> Self {
        Self {
            transport: Arc::clone(remote.transport()),
            codecs: PayloadCodecs::default(),
            batch: 100,
            interval: Duration::from_secs(5),
        }
    }

    /// Use `codecs` to read handles and to write results.
    #[must_use]
    pub fn with_codecs(mut self, codecs: PayloadCodecs) -> Self {
        self.codecs = codecs;
        self
    }

    /// Read at most `batch` tokens in one poll.
    #[must_use]
    pub const fn with_batch_size(mut self, batch: i64) -> Self {
        self.batch = batch;
        self
    }

    /// Wait `interval` between two polls in [`Self::run`].
    #[must_use]
    pub const fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Poll one page.
    ///
    /// # Errors
    ///
    /// Returns an error when a query fails.
    pub async fn poll_once(&self, conn: &mut AsyncPgConnection) -> HarvestResult<PollReport> {
        let _ = (conn, &self.transport, &self.codecs, self.batch);
        todo!("issue #2006")
    }

    /// Poll until `cancel` fires.
    pub async fn run(&self, pool: &DbPool, cancel: tokio_util::sync::CancellationToken) {
        let _ = (pool, cancel, self.interval);
        todo!("issue #2006")
    }
}
