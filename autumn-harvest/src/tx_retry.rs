//! Retry a transaction that Postgres aborts to break a conflict (issue #1822).
//!
//! Postgres aborts one transaction of a lock cycle with SQLSTATE `40P01`.
//! It aborts a transaction that cannot serialize with SQLSTATE `40001`.
//! In both cases the server rolls the transaction back in full.
//! The same work can then run again from the start.
//!
//! [`run_with_conflict_retry`] runs one top-level transaction.
//! It runs the transaction again after a `40P01` or `40001` abort, with a
//! capped and jittered backoff. Each retry increments
//! `harvest.db.transaction_retry{site, reason}`. A conflict after the last
//! retry increments `harvest.db.transaction_retry_exhausted{site, reason}`.
//!
//! # Safety contract
//!
//! The caller passes the whole transaction, from `BEGIN` to `COMMIT`.
//! Collect follow-up work and run it after the helper returns.
//! An effect outside the database that runs before the commit repeats on each
//! retry. Keep such effects to counters and logs.
//!
//! # Latency
//!
//! With [`TxRetryPolicy::DEFAULT`] the sleeps add at most 300 ms. Postgres
//! also waits `deadlock_timeout` (1 s by default) before it aborts a victim.
//! Five deadlocked runs can therefore take about 5 s.
//!
//! The helper does not retry inside an open transaction. A savepoint retry
//! keeps the locks of the outer transaction, so the same cycle can form again.
//! The outermost caller owns the retry.

use std::time::Duration;

use diesel_async::AsyncPgConnection;

use crate::error::{HarvestError, HarvestResult};
use crate::telemetry::MetricsRecorder;

/// Site label for a persist transaction that runs again in place.
pub const SITE_PERSIST: &str = "persist";
/// Site label for a workflow-task persist that the dispatcher runs again.
///
/// The task resets to `PENDING`, and replay derives the same decision. The
/// persist itself does not run under [`run_with_conflict_retry`].
pub const SITE_WORKFLOW_TASK: &str = "workflow_task";
/// Site label for a task claim transaction.
pub const SITE_CLAIM: &str = "claim";
/// Site label for a scanner fire transaction.
pub const SITE_SCANNER: &str = "scanner";

/// A server abort that a full re-run of the transaction can resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxConflict {
    /// `deadlock_detected`, SQLSTATE `40P01`.
    Deadlock,
    /// `serialization_failure`, SQLSTATE `40001`.
    SerializationFailure,
}

impl TxConflict {
    /// The `reason` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deadlock => "deadlock",
            Self::SerializationFailure => "serialization_failure",
        }
    }

    /// The SQLSTATE code.
    #[must_use]
    pub const fn sqlstate(self) -> &'static str {
        match self {
            Self::Deadlock => "40P01",
            Self::SerializationFailure => "40001",
        }
    }
}

/// Classify `error` as a retryable conflict abort.
///
/// Returns `None` for every other error.
///
/// The check reads the English message text, because Diesel keeps no
/// SQLSTATE. A server with a non-English `lc_messages` never matches, so the
/// retry is off there. See [`crate::pool::is_session_timeout`] for the same
/// limit. A bare code is not matched. A number such as `140001` in a message
/// would otherwise match it.
#[must_use]
pub fn classify_conflict(error: &HarvestError) -> Option<TxConflict> {
    let HarvestError::Database(msg) = error else {
        return None;
    };
    if msg.contains("deadlock detected") {
        Some(TxConflict::Deadlock)
    } else if msg.contains("could not serialize access") {
        Some(TxConflict::SerializationFailure)
    } else {
        None
    }
}

/// Attempt limit and backoff bounds for [`run_with_conflict_retry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxRetryPolicy {
    /// Total runs, the first run included. A value of 1 disables retry.
    pub max_attempts: u32,
    /// Backoff ceiling before the first retry.
    pub base_delay: Duration,
    /// Largest backoff ceiling.
    pub max_delay: Duration,
}

impl TxRetryPolicy {
    /// The engine default.
    pub const DEFAULT: Self = Self {
        max_attempts: 5,
        base_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(500),
    };

    /// The sleep before retry number `retry`, counted from 1.
    ///
    /// `jitter` is a sample in `[0, 1)`.
    ///
    /// The ceiling is `base_delay * 2^(retry - 1)`, capped at `max_delay`.
    /// The sleep is half the ceiling plus a jittered share of the other half.
    /// The fixed half gives the winner time to commit. The jitter spreads out
    /// retries that start together. With the default policy, the first retry
    /// sleeps 10 to 20 ms.
    #[must_use]
    pub fn backoff(&self, retry: u32, jitter: f64) -> Duration {
        let doublings = retry.saturating_sub(1).min(31);
        let ceiling = self
            .base_delay
            .saturating_mul(1_u32 << doublings)
            .min(self.max_delay);
        let jitter = if jitter.is_nan() {
            0.0
        } else {
            jitter.clamp(0.0, 1.0)
        };
        let half = ceiling / 2;
        half + half.mul_f64(jitter)
    }
}

impl Default for TxRetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One run of a transaction body, with a `Send` future.
///
/// The bound mirrors the `AsyncFunc` bound of `diesel_async`. It names the
/// future type, so a caller can require `Send` on it.
pub trait TxAttempt<A, R>: AsyncFnMut(A) -> R + FnMut(A) -> <Self as TxAttempt<A, R>>::Fut {
    /// The future of one run.
    type Fut: Future<Output = R>;
}

impl<F, A, Fut, R> TxAttempt<A, R> for F
where
    F: AsyncFnMut(A) -> R + FnMut(A) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

/// Run `attempt` and run it again after a conflict abort.
///
/// `attempt` opens and commits one top-level transaction on the connection
/// it receives. See the module docs for the safety contract.
///
/// # Errors
///
/// Returns the error of the last run. That is the conflict error when the
/// attempts run out.
pub async fn run_with_conflict_retry<T, F>(
    conn: &mut AsyncPgConnection,
    site: &'static str,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    policy: TxRetryPolicy,
    mut attempt: F,
) -> HarvestResult<T>
where
    for<'r> F: AsyncFnMut(&'r mut AsyncPgConnection) -> HarvestResult<T>
        + TxAttempt<&'r mut AsyncPgConnection, HarvestResult<T>, Fut: Send>
        + Send,
    T: Send,
{
    if !at_top_level(conn) {
        return attempt(&mut *conn).await;
    }
    let mut run: u32 = 1;
    loop {
        let error = match attempt(&mut *conn).await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let Some(conflict) = classify_conflict(&error) else {
            return Err(error);
        };
        if run >= policy.max_attempts || !at_top_level(conn) {
            metrics.record_db_transaction_retry_exhausted(site, conflict.as_str());
            tracing::warn!(
                site,
                reason = conflict.as_str(),
                attempts = run,
                error = %error,
                "transaction conflict retries exhausted"
            );
            return Err(error);
        }
        metrics.record_db_transaction_retry(site, conflict.as_str());
        let delay = policy.backoff(run, rand::random::<f64>());
        tracing::warn!(
            site,
            reason = conflict.as_str(),
            sqlstate = conflict.sqlstate(),
            attempt = run,
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "Postgres aborted the transaction; running it again"
        );
        tokio::time::sleep(delay).await;
        run += 1;
    }
}

/// Whether `conn` has no open transaction and a usable transaction manager.
///
/// A broken manager reports an error here. The helper then stops, because a
/// new `BEGIN` on that connection cannot succeed.
fn at_top_level(conn: &mut AsyncPgConnection) -> bool {
    use diesel_async::{AnsiTransactionManager, TransactionManager};
    matches!(
        <AnsiTransactionManager as TransactionManager<AsyncPgConnection>>::transaction_manager_status_mut(conn)
            .transaction_depth(),
        Ok(None)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(msg: &str) -> HarvestError {
        HarvestError::Database(msg.to_owned())
    }

    #[test]
    fn a_deadlock_abort_classifies_as_deadlock() {
        assert_eq!(
            classify_conflict(&db("deadlock detected")),
            Some(TxConflict::Deadlock)
        );
    }

    #[test]
    fn a_serialization_abort_classifies_as_serialization_failure() {
        assert_eq!(
            classify_conflict(&db("could not serialize access due to concurrent update")),
            Some(TxConflict::SerializationFailure)
        );
        assert_eq!(
            classify_conflict(&db(
                "could not serialize access due to read/write dependencies among transactions"
            )),
            Some(TxConflict::SerializationFailure)
        );
    }

    #[test]
    fn a_number_that_looks_like_a_code_does_not_classify() {
        assert_eq!(classify_conflict(&db("value 140001 is out of range")), None);
        assert_eq!(classify_conflict(&db("order 40P01 not found")), None);
    }

    #[test]
    fn other_errors_do_not_classify() {
        assert_eq!(classify_conflict(&db("lock timeout")), None);
        assert_eq!(classify_conflict(&db("connection closed")), None);
        assert_eq!(
            classify_conflict(&db("duplicate key value violates unique constraint")),
            None
        );
        // Only a database error is a server abort. Other variants never retry.
        assert_eq!(
            classify_conflict(&HarvestError::NotFound("deadlock detected".to_owned())),
            None
        );
    }

    #[test]
    fn labels_and_codes_are_stable() {
        assert_eq!(TxConflict::Deadlock.as_str(), "deadlock");
        assert_eq!(TxConflict::Deadlock.sqlstate(), "40P01");
        assert_eq!(
            TxConflict::SerializationFailure.as_str(),
            "serialization_failure"
        );
        assert_eq!(TxConflict::SerializationFailure.sqlstate(), "40001");
    }

    #[test]
    fn backoff_starts_at_half_the_base_ceiling() {
        let policy = TxRetryPolicy::DEFAULT;
        assert_eq!(policy.backoff(1, 0.0), policy.base_delay / 2);
        assert!(policy.backoff(1, 0.999) < policy.base_delay);
    }

    #[test]
    fn backoff_ceiling_doubles_per_retry() {
        let policy = TxRetryPolicy::DEFAULT;
        assert_eq!(policy.backoff(2, 0.0), policy.base_delay);
        assert_eq!(policy.backoff(3, 0.0), policy.base_delay * 2);
    }

    #[test]
    fn backoff_never_exceeds_the_cap() {
        let policy = TxRetryPolicy::DEFAULT;
        for retry in 1..64 {
            assert!(policy.backoff(retry, 0.999) <= policy.max_delay);
        }
        assert_eq!(policy.backoff(u32::MAX, 0.0), policy.max_delay / 2);
    }

    #[test]
    fn backoff_clamps_an_out_of_range_jitter_sample() {
        let policy = TxRetryPolicy::DEFAULT;
        assert_eq!(policy.backoff(1, -1.0), policy.backoff(1, 0.0));
        assert!(policy.backoff(1, 7.0) <= policy.base_delay);
        assert_eq!(policy.backoff(1, f64::NAN), policy.backoff(1, 0.0));
    }

    #[test]
    fn the_default_policy_retries() {
        assert!(TxRetryPolicy::default().max_attempts >= 2);
        assert!(TxRetryPolicy::DEFAULT.base_delay <= TxRetryPolicy::DEFAULT.max_delay);
    }
}
