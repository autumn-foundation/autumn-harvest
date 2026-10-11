//! Physical backout: one transaction, one savepoint per step (issue #2012).

use diesel_async::AsyncPgConnection;
use futures::future::BoxFuture;

use crate::error::{HarvestError, HarvestResult};
use crate::telemetry::MetricsRecorder;
use crate::tx_retry::TxRetryPolicy;

/// The steps of one run, inside its open transaction.
pub struct Steps<'c> {
    conn: &'c mut AsyncPgConnection,
}

impl Steps<'_> {
    /// Run `step` in its own savepoint.
    ///
    /// # Errors
    ///
    /// Returns the step error after the rollback to the savepoint.
    pub async fn step<T, F>(&mut self, step: F) -> HarvestResult<T>
    where
        F: for<'r> FnOnce(&'r mut AsyncPgConnection) -> BoxFuture<'r, HarvestResult<T>> + Send,
        T: Send,
    {
        let _ = (&mut self.conn, step);
        Err(HarvestError::Config("not implemented".into()))
    }
}

/// Run `body` as one transaction.
///
/// # Errors
///
/// Returns the body error after the rollback.
pub async fn run_backout<T, F>(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    policy: TxRetryPolicy,
    body: F,
) -> HarvestResult<T>
where
    F: for<'s> FnMut(&'s mut Steps<'_>) -> BoxFuture<'s, HarvestResult<T>> + Send,
    T: Send,
{
    let _ = (conn, metrics, policy, body);
    Err(HarvestError::Config("not implemented".into()))
}
