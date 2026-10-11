//! Physical backout: one transaction, one savepoint per step (issue #2012).
//!
//! [`run_backout`] opens one transaction and passes the body a [`Steps`].
//! [`Steps::step`] runs each step in a nested `diesel-async` transaction, so
//! Diesel issues the `SAVEPOINT`. A step error rolls back to that savepoint
//! only. The body then decides: it can propagate the error, which rolls back
//! the whole run, or it can go on with the next step.
//!
//! A deadlock or a serialization abort inside a step aborts only that
//! savepoint in Postgres. [`Steps`] still records the conflict. After the
//! body returns, [`run_backout`] rolls back the whole transaction and runs
//! the body again from the start, through [`run_with_conflict_retry`]. This
//! holds even when the body ignores the step error. The runner never
//! retries a step in place. The outer locks stay held, so the same cycle
//! could form again.
//!
//! Inside an open transaction, such as the one that `run_transactional`
//! gives, the run becomes one savepoint of the outer transaction. The owner
//! of the outer transaction then owns the retry.

use std::future::Future;

use diesel_async::{AsyncConnection, AsyncPgConnection};

use crate::error::{HarvestError, HarvestResult};
use crate::telemetry::MetricsRecorder;
use crate::tx_retry::{TxRetryPolicy, classify_conflict, run_with_conflict_retry};

/// The `site` label of a backout run in the retry metrics.
pub const SITE_BACKOUT: &str = "atomicity_backout";

/// One step: an async closure that runs once on the savepoint connection.
///
/// The bound names the future type, so a caller can require `Send` on it.
/// It mirrors [`crate::tx_retry::TxAttempt`].
pub trait StepFn<A, R>: AsyncFnOnce(A) -> R + FnOnce(A) -> <Self as StepFn<A, R>>::Fut {
    /// The future of the step.
    type Fut: Future<Output = R>;
}

impl<F, A, Fut, R> StepFn<A, R> for F
where
    F: AsyncFnOnce(A) -> R + FnOnce(A) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

/// The body of one run. A conflict retry runs it again, so it is `Fn`.
pub trait BodyFn<A, R>: AsyncFn(A) -> R + Fn(A) -> <Self as BodyFn<A, R>>::Fut {
    /// The future of one run of the body.
    type Fut: Future<Output = R>;
}

impl<F, A, Fut, R> BodyFn<A, R> for F
where
    F: AsyncFn(A) -> R + Fn(A) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

/// The steps of one run, inside its open transaction.
pub struct Steps<'c> {
    conn: &'c mut AsyncPgConnection,
    /// The first conflict abort of a step in this run.
    conflict: Option<HarvestError>,
}

impl Steps<'_> {
    /// Run `step` in its own savepoint.
    ///
    /// # Errors
    ///
    /// Returns the step error after the rollback to the savepoint. A conflict
    /// abort also marks the whole run for a retry.
    pub async fn step<T, F>(&mut self, step: F) -> HarvestResult<T>
    where
        for<'r> F: StepFn<&'r mut AsyncPgConnection, HarvestResult<T>, Fut: Send> + Send,
        T: Send,
    {
        let result = Box::pin(
            self.conn
                .transaction::<T, HarvestError, _>(async move |savepoint| step(savepoint).await),
        )
        .await;
        if let Err(error @ HarvestError::Database(message)) = &result
            && self.conflict.is_none()
            && classify_conflict(error).is_some()
        {
            // `classify_conflict` matches only `Database`, so this copy is exact.
            self.conflict = Some(HarvestError::Database(message.clone()));
        }
        result
    }
}

/// Run `body` as one transaction, and run it again after a conflict abort.
///
/// `body` can run more than once, so it must not have an effect outside the
/// database.
///
/// # Errors
///
/// Returns the body error after the rollback. A step conflict wins over the
/// body result. After the last conflict retry, returns the conflict error.
pub async fn run_backout<T, F>(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    policy: TxRetryPolicy,
    body: F,
) -> HarvestResult<T>
where
    for<'s, 'c> F: BodyFn<&'s mut Steps<'c>, HarvestResult<T>, Fut: Send> + Send + Sync,
    T: Send,
{
    Box::pin(run_with_conflict_retry(
        conn,
        SITE_BACKOUT,
        metrics,
        policy,
        async |conn| {
            Box::pin(conn.transaction::<T, HarvestError, _>(async |tx| {
                let mut steps = Steps {
                    conn: tx,
                    conflict: None,
                };
                let result = body(&mut steps).await;
                // A conflict wins over any result, so the run always retries.
                steps.conflict.map_or(result, Err)
            }))
            .await
        },
    ))
    .await
}
