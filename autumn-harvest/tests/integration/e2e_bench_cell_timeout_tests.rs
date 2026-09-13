//! Hard wall-clock ceiling on one benchmark cell (issue #1288).
//!
//! `SCENARIO_BUDGET_SECS` is a *cooperative* deadline. Every scenario runner
//! checks it between its own awaits. Nothing bounds a single await, so a
//! wedged database can park a cell forever. `await_cell` is the outer stop.
//! Past budget, it aborts the cell's task instead of waiting forever. This
//! suite proves that with a deliberately-hanging fake task, so it needs
//! neither a database nor the `db` feature.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use super::e2e_bench_support::{CellOutcome, await_cell};

async fn hang_forever(ticks: Arc<AtomicUsize>) -> Result<(), String> {
    loop {
        ticks.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
    }
}

/// Sets its flag on drop. Stands in for a real cell's owned resources: a
/// shard lease connection, a worker pool. Such resources must actually be
/// released, not merely have their release requested.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn hang_holding(released: Arc<AtomicBool>) -> Result<(), String> {
    let _guard = DropFlag(released);
    loop {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_cell_that_finishes_in_time_reports_its_value() {
    let handle = tokio::spawn(async { Ok::<u32, String>(42) });
    let outcome = await_cell(handle, Duration::from_secs(5)).await;
    assert!(matches!(outcome, CellOutcome::Report(42)));
}

#[tokio::test]
async fn a_cell_that_skips_reports_the_reason() {
    let handle = tokio::spawn(async { Err::<u32, String>("no server".to_owned()) });
    let outcome = await_cell(handle, Duration::from_secs(5)).await;
    match outcome {
        CellOutcome::Skipped(reason) => assert_eq!(reason, "no server"),
        other => panic!("expected Skipped, got {other:?}"),
    }
}

#[tokio::test]
async fn a_cell_that_panics_is_reported_not_propagated() {
    let handle = tokio::spawn(async { panic!("boom") });
    let outcome = await_cell::<u32, String>(handle, Duration::from_secs(5)).await;
    match outcome {
        CellOutcome::Panicked(msg) => assert!(msg.contains("boom"), "{msg}"),
        other => panic!("expected Panicked, got {other:?}"),
    }
}

/// The core claim of issue #1288's fix: a wedged cell is actually stopped,
/// not merely detached. `tokio::time::timeout` alone would let a dropped
/// `JoinHandle`'s task keep running in the background forever; only
/// `abort()` on the handle actually halts it.
#[tokio::test]
async fn a_timed_out_cell_is_actually_aborted_not_just_detached() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let handle = tokio::spawn(hang_forever(ticks.clone()));

    let outcome = await_cell(handle, Duration::from_millis(20)).await;
    assert!(
        matches!(outcome, CellOutcome::TimedOut),
        "expected TimedOut, got {outcome:?}"
    );

    let just_after_timeout = ticks.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_a_settling_wait = ticks.load(Ordering::SeqCst);

    assert_eq!(
        just_after_timeout, after_a_settling_wait,
        "the task kept ticking after its reported timeout: abort_handle.abort() did not \
         actually stop it, so a wedged cell would still run forever in the background"
    );
}

/// `abort()` only requests cancellation; it does not by itself wait for the
/// task to finish dropping. `await_cell` must join the aborted task before
/// it returns, so the next cell never starts while this one's resources
/// (shard leases, pools) are still open. Multi-thread runtime: the race this
/// guards against is between this task's own drop and a task actually
/// running on another worker thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_cells_resources_are_released_before_await_cell_returns() {
    let released = Arc::new(AtomicBool::new(false));
    let handle = tokio::spawn(hang_holding(released.clone()));

    let outcome = await_cell(handle, Duration::from_millis(20)).await;
    assert!(
        matches!(outcome, CellOutcome::TimedOut),
        "expected TimedOut, got {outcome:?}"
    );

    assert!(
        released.load(Ordering::SeqCst),
        "await_cell returned before the timed-out task's resources were actually \
         released; the next cell could start while a stale shard lease is still open"
    );
}
