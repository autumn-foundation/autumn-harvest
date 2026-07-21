#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Durable per-key mutex via `ctx.mutex(key).acquire()` — issue #691.
//!
//! ## The problem
//!
//! Two workflows that both read-modify-write the *same* shared resource — here a
//! per-account ledger balance — race: interleaved read-modify-write cycles lose
//! updates. A `#[workflow]` cannot reach for a `std::sync::Mutex` (it would not
//! survive a worker restart, and holding a std lock across a durable `.await`
//! breaks replay). `ctx.mutex(key)` is the durable, replay-safe answer:
//! mutual exclusion across executions, workers, and restarts, coordinated
//! through Postgres.
//!
//! ## The RAII pattern
//!
//! ```rust,ignore
//! // Acquire suspends durably until THIS execution holds the lock:
//! let guard = ctx.mutex(format!("ledger:{account}")).acquire().await?;
//! // ── critical section ── the guard is held across the durable activity,
//! //    so no other execution can enter for the same key.
//! let new_balance = ctx.execute_activity(&apply_delta_info(), delta).await?;
//! // The lock is released when `guard` is dropped at scope end, or explicitly
//! // via `guard.release()`. `guard.lock_seq()` is the fencing token minted for
//! // this grant.
//! ```
//!
//! The guard is a `#[must_use]` RAII handle: dropping it pushes exactly one
//! event-less `ReleaseMutex` (no `harvest_events` footprint), and the engine's
//! timeout-guard contract keeps the lock **held across suspensions** — a guard
//! held while the workflow parks on an `.await` is NOT released under the still-
//! parked holder.
//!
//! ## Semantics
//!
//! | Property | Behaviour |
//! |---|---|
//! | **No new event variant** | A grant records the existing `MutexGranted` anchor; release is event-less bookkeeping. Replay recovers the guard from the recorded grant regardless of which fencing token (`lock_seq`) was minted. |
//! | **Lease-based liveness** | A held lock carries an `expires_at` lease that the holder renews each decision cycle; if the holder crashes, an expiring lease is reclaimed and the next FIFO waiter is granted. Cancel/terminate/completion all release the lock via a terminal sweep. |
//! | **FIFO fairness** | Contenders are granted in arrival order (a BIGSERIAL head-of-line queue). |
//! | **Shard-local** | The lock is scoped to the workflow's own shard — a single logical resource that must serialize across executions must live on one shard. Cross-shard global mutual exclusion is out of scope. |
//! | **Non-reentrant** | Re-acquiring a key this execution already holds returns `HarvestError::MutexSelfDeadlock` **synchronously** (before any history match), so a self-deadlock surfaces the same typed error live and on replay rather than hanging. |
//!
//! Run with:
//!   cargo run --example mutex_ledger

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerOp {
    account: String,
    amount: i64,
}

// ---------------------------------------------------------------------------
// Two workflows serialize read-modify-write updates to the SAME per-account
// balance. Both acquire `ledger:{account}` before touching the balance, so a
// concurrent credit and debit for one account can never interleave their
// critical sections — the second waits FIFO for the first to release.
// ---------------------------------------------------------------------------

/// Credit an account. Holds the per-account lock across the balance update.
#[workflow]
async fn credit_account(ctx: &WorkflowContext, op: LedgerOp) -> Result<i64, String> {
    // Acquire the durable per-account lock. Suspends until THIS execution holds
    // it; the RAII guard keeps it held for the whole critical section below.
    let guard = ctx
        .mutex(format!("ledger:{}", op.account))
        .acquire()
        .await
        .map_err(|e| e.to_string())?;

    // `guard.lock_seq()` is the fencing token — usable to detect a stale writer
    // in the downstream store if the lock were ever reclaimed mid-flight.
    let _fencing_token = guard.lock_seq();

    // Critical section: the balance update runs while the lock is held, so a
    // concurrent debit for the same account cannot race this read-modify-write.
    let new_balance = ctx
        .execute_activity(&apply_delta_info(), op)
        .await
        .map_err(|e| e.to_string())?;

    // The lock is released when `guard` drops here at scope end. (An explicit
    // `guard.release()` would release earlier, before the function returns.)
    Ok(new_balance)
}

/// Debit an account. Same lock discipline as `credit_account`, so the two
/// serialize per account.
#[workflow]
async fn debit_account(ctx: &WorkflowContext, op: LedgerOp) -> Result<i64, String> {
    let guard = ctx
        .mutex(format!("ledger:{}", op.account))
        .acquire()
        .await
        .map_err(|e| e.to_string())?;

    let debit = LedgerOp {
        account: op.account,
        amount: -op.amount,
    };
    let new_balance = ctx
        .execute_activity(&apply_delta_info(), debit)
        .await
        .map_err(|e| e.to_string())?;

    // Release explicitly before the (trivial) tail work below to shorten the
    // critical section — the trailing `drop` is then a no-op.
    guard.release();
    Ok(new_balance)
}

/// Apply a signed delta to the account balance and return the new balance. In a
/// real system this is the read-modify-write against the ledger store that the
/// mutex protects.
#[activity(start_to_close = "10s")]
async fn apply_delta(_ctx: &ActivityContext, op: LedgerOp) -> Result<i64, String> {
    // A stub: a production activity would read the current balance for
    // `op.account`, add `op.amount`, and persist it — all under the durable lock
    // the caller holds, so no other execution's activity can interleave.
    Ok(op.amount)
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![credit_account, debit_account];
    let _acts = activities![apply_delta];
    println!("mutex_ledger example compiled successfully");
    println!();
    println!("Both credit_account and debit_account acquire `ledger:{{account}}`");
    println!("before touching the balance, so per-account updates serialize:");
    println!("  let guard = ctx.mutex(format!(\"ledger:{{account}}\")).acquire().await?;");
    println!("  // critical section runs while the lock is held ...");
    println!("  // guard drops (or guard.release()) => lock released, FIFO next waiter granted");
}

// Gated on the `testing` feature as well as `test`: the example itself must keep
// building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    fn op() -> serde_json::Value {
        json!({"account": "acct-1", "amount": 250})
    }

    /// Happy path: the harness auto-grants the uncontended lock, the workflow
    /// runs its critical section under the lock, releases it (event-less), and
    /// completes — with exactly one `MutexGranted` recorded. The recorded
    /// history then replays cleanly against the same handler (the falsifiable
    /// replay-safety bar #691's AC6 demands).
    #[tokio::test]
    async fn credit_acquires_lock_runs_critical_section_and_replays() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("apply_delta", |_| Ok(json!(250)))
            .run(credit_account_info().handler, op())
            .await;

        assert_eq!(
            outcome.result,
            Ok(json!(250)),
            "credit under an uncontended lock must complete: {:?}",
            outcome.result
        );

        let grants = outcome
            .events()
            .iter()
            .filter(
                |e| matches!(e, WorkflowEvent::MutexGranted { key, .. } if key == "ledger:acct-1"),
            )
            .count();
        assert_eq!(
            grants, 1,
            "exactly one MutexGranted must be recorded for the per-account key"
        );

        let report = outcome.replay_check(credit_account_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "a mutex-holding workflow must replay deterministically:\n{report}"
        );
    }

    /// Contended path: with the per-account key marked contended, the grant is
    /// withheld — the workflow parks on `acquire()` (never completes) and no
    /// `MutexGranted` for that key is ever recorded. This models a second
    /// execution waiting FIFO behind a live holder.
    #[tokio::test]
    async fn debit_parks_when_the_account_lock_is_held() {
        let outcome = WorkflowTestEnv::new()
            .with_mutex_contended("ledger:acct-1")
            .mock_activity("apply_delta", |_| Ok(json!(-250)))
            .run(debit_account_info().handler, op())
            .await;

        assert!(
            outcome.result.is_err(),
            "a contended per-account lock must leave the debit parked, not \
             complete: {:?}",
            outcome.result
        );

        assert!(
            !outcome.events().iter().any(
                |e| matches!(e, WorkflowEvent::MutexGranted { key, .. } if key == "ledger:acct-1")
            ),
            "a withheld (contended) grant must never record a MutexGranted"
        );
    }
}
