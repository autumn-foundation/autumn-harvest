//! `ctx.info()` / `ctx.deadline()` / `ctx.time_remaining()` — issue #783.
//!
//! Two things an activity author could not do before:
//!
//! **(a) One-hop correlation.** An activity can log the *owning* run's
//! `execution_id` — the exact `{exec_id}` segment of
//! `GET /api/harvest/workflows/{exec_id}` — so an on-call engineer paging from
//! a log line opens the run with **zero** directory lookups. Alongside it,
//! `task_id` is the `harvest_task_queue` row id the operator `retry-now` /
//! `fail-now` routes address, so the pair is directly actionable.
//!
//! **(b) Deadline-aware checkpointing.** A long activity can ask how much of
//! its attempt budget is left and bail out *cleanly* — flushing a checkpoint
//! via `ctx.heartbeat(...)` — instead of being killed mid-item by the
//! start-to-close scanner and losing everything it had done. The retry resumes
//! from the checkpoint, so **no completed work is ever re-executed**.
//!
//! Run it:
//!
//! ```bash
//! cargo run -p autumn-harvest --example activity_ctx_info --features testing
//! ```

use std::time::Duration;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// (a) One-hop correlation.
// ---------------------------------------------------------------------------

/// Emits the identifiers an operator actually needs, straight from the context.
///
/// The activity does not have to be *told* which run it belongs to — the worker
/// resolves that at dispatch and `ctx.info()` hands it back.
#[activity(start_to_close = "30s")]
async fn charge_card(ctx: &ActivityContext, amount_cents: i64) -> Result<String, String> {
    let info = ctx.info();

    // In real code these would be `tracing` fields. `execution_id` renders as
    // the bare UUID, so it can be pasted straight into a management-API URL.
    println!(
        "  charge_card: execution_id={} workflow={}/{} activity={} attempt={}/{} \
         queue={} task_id={:?}",
        info.execution_id,
        info.workflow_type,
        info.workflow_id,
        info.activity_type,
        info.attempt,
        info.max_attempts,
        info.queue_name,
        info.task_id,
    );
    println!(
        "    -> open the run: GET /api/harvest/workflows/{}",
        info.execution_id
    );

    // A run-scoped idempotency key derived from stable identity: the same
    // logical invocation reuses it across every retry attempt.
    let idempotency_key = format!("charge-{}-{}", info.execution_id, info.activity_id);
    println!("    -> downstream Idempotency-Key: {idempotency_key}");

    // Stand-in for the real payment call this activity would make.
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(format!("charged {amount_cents} cents"))
}

// ---------------------------------------------------------------------------
// (b) Deadline-aware checkpointing.
// ---------------------------------------------------------------------------

/// How much of the attempt budget to keep in reserve so the checkpoint has time
/// to flush before the attempt is requeued. The batched heartbeat flusher ticks
/// about once a second, so leave comfortably more than that.
const RESERVE: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ImportCheckpoint {
    /// Index of the next unprocessed row. Everything below this is durable.
    next: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportRequest {
    total_rows: u32,
}

/// A long import that yields the attempt back to the engine rather than being
/// killed by the start-to-close scanner.
///
/// The shape to copy:
///
/// 1. resume from `ctx.heartbeat_details()` (empty on the first attempt);
/// 2. before each unit of work, ask `ctx.is_expiring_within(RESERVE)`;
/// 3. if the budget is nearly spent, return a **retryable** error — the
///    checkpoint already written by `ctx.heartbeat(...)` survives the requeue;
/// 4. otherwise do the work and checkpoint.
///
/// Because step 3 happens *before* the deadline rather than at it, the last
/// completed unit is always durable: the retry re-executes **nothing**.
#[activity(start_to_close = "30s", heartbeat_timeout = "60s")]
async fn import_rows(ctx: &ActivityContext, req: ImportRequest) -> Result<u32, String> {
    let mut next = ctx
        .heartbeat_details::<ImportCheckpoint>()
        .map_err(|e| e.to_string())?
        .unwrap_or_default()
        .next;

    println!(
        "  import_rows attempt {}: resuming at row {next} (deadline {:?}, {:?} left)",
        ctx.attempt(),
        ctx.deadline(),
        ctx.time_remaining(),
    );

    while next < req.total_rows {
        // Prefer this over comparing `time_remaining()` yourself: `Option`
        // orders `None` BELOW `Some(_)`, so a naive
        // `ctx.time_remaining() < Some(RESERVE)` would checkpoint immediately
        // on an *unbounded* activity. `is_expiring_within` is `false` when
        // there is no deadline at all.
        if ctx.is_expiring_within(RESERVE) {
            println!("    budget nearly spent at row {next}; checkpointing and yielding");
            // Give the batched flusher a moment to persist the last checkpoint.
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            return Err(format!("deadline reserve reached at row {next}"));
        }

        // ... the real unit of work ...
        next += 1;
        ctx.heartbeat(serde_json::to_value(ImportCheckpoint { next }).map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
    }

    Ok(next)
}

// ---------------------------------------------------------------------------
// Workflow.
// ---------------------------------------------------------------------------

#[workflow]
async fn order_flow(ctx: &WorkflowContext, total_rows: u32) -> Result<String, String> {
    let receipt: String = ctx
        .execute_activity(&charge_card_info(), 4_200_i64)
        .await
        .map_err(|e| e.to_string())?;
    let imported: u32 = ctx
        .execute_activity(&import_rows_info(), ImportRequest { total_rows })
        .await
        .map_err(|e| e.to_string())?;
    Ok(format!("{receipt}; imported {imported} rows"))
}

#[tokio::main]
async fn main() {
    println!("This example is exercised by its embedded tests:");
    println!("  cargo test -p autumn-harvest --features testing --example activity_ctx_info");
    println!();
    println!("See the module docs for the two patterns it demonstrates.");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::{ActivityIdentity, ExecutionId};
    use std::sync::{Arc, Mutex};

    /// (a) The correlation pattern: every identifier an operator needs is on
    /// `ctx.info()`, and `execution_id` renders as the bare management-API path
    /// segment (it round-trips back through `FromStr`).
    #[tokio::test]
    async fn info_carries_the_operator_correlation_pair() {
        let exec_id = ExecutionId::new();
        let ctx = ActivityContext::new_test()
            .with_execution_id(exec_id)
            .with_workflow_id("order-42")
            .with_workflow_type("order_flow")
            .with_activity_type("charge_card")
            .with_queue_name("payments")
            .with_task_id(Some(uuid::Uuid::new_v4()));

        let info = ctx.info();
        assert_eq!(info.execution_id, exec_id);
        assert_eq!(info.workflow_id, "order-42");
        assert_eq!(info.workflow_type, "order_flow");
        assert_eq!(info.activity_type, "charge_card");
        assert_eq!(info.queue_name, "payments");
        assert!(info.task_id.is_some(), "a queue dispatch has a task row");

        // The rendered id is exactly what goes in the URL.
        let rendered = info.execution_id.to_string();
        assert_eq!(rendered.parse::<ExecutionId>().unwrap(), exec_id);

        // The activity really can run with that context.
        let out = charge_card(&ctx, 4_200).await.expect("charge_card");
        assert_eq!(out, "charged 4200 cents");
    }

    /// The identity is stable across attempts, so an idempotency key derived
    /// from it is reused by a retry rather than minted afresh.
    #[test]
    fn identity_derived_idempotency_key_is_retry_stable() {
        let identity = ActivityIdentity::for_test();
        let key_of = |attempt: u32| {
            let ctx = ActivityContext::new_test_with_identity(identity.clone())
                .with_attempt(attempt)
                .with_max_attempts(3);
            let info = ctx.info();
            format!("charge-{}-{}", info.execution_id, info.activity_id)
        };
        assert_eq!(key_of(1), key_of(2), "the key must not change on a retry");
    }

    /// (b) The guard is `false` when the activity is unbounded — the trap the
    /// `Option` ordering sets for a hand-rolled comparison.
    #[test]
    fn unbounded_activity_never_checkpoints_early() {
        let ctx = ActivityContext::new_test();
        assert_eq!(ctx.deadline(), None);
        assert_eq!(ctx.time_remaining(), None);
        assert!(
            !ctx.is_expiring_within(RESERVE),
            "no deadline means we are never close to one"
        );
        // The trap this method exists to avoid:
        assert!(None::<Duration> < Some(RESERVE));
    }

    /// (b) The whole checkpoint cycle, without a worker or a database:
    /// attempt 1 does work and flushes checkpoints; attempt 2 RESUMES from the
    /// last one and re-executes nothing.
    #[tokio::test]
    async fn import_resumes_from_the_checkpoint_and_re_executes_nothing() {
        // ── Attempt 1: ample budget for 4 rows, then the reserve trips. ──
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let flushed: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&flushed);
        let drain = tokio::spawn(async move {
            while let Some(v) = rx.recv().await {
                sink.lock().unwrap().push(v);
            }
        });
        let ctx1 = ActivityContext::new_test_with_heartbeat(tx)
            .with_deadline(Some(chrono::Utc::now() + chrono::Duration::seconds(300)))
            .with_attempt(1)
            .with_max_attempts(3);
        let done = import_rows(&ctx1, ImportRequest { total_rows: 4 })
            .await
            .expect("attempt 1 finishes 4 rows under an ample budget");
        assert_eq!(done, 4);
        drop(ctx1);
        drain.await.expect("drain");

        // Every row flushed a checkpoint; the last one is what a retry resumes from.
        let checkpoints = flushed.lock().unwrap().clone();
        assert_eq!(
            checkpoints.len(),
            4,
            "one checkpoint per row: {checkpoints:?}"
        );
        let last = checkpoints.last().cloned().expect("a last checkpoint");
        assert_eq!(last, serde_json::json!({"next": 4}));

        // ── Attempt 2: same activity, resumed from that checkpoint, but with
        // almost no budget left. It must resume at row 4 (NOT row 0) and yield
        // immediately — proving the first four rows are never re-executed. ──
        // A real retry attempt always has a flusher, so build one here too.
        let (tx2, mut rx2) = tokio::sync::mpsc::channel(64);
        let drain2 = tokio::spawn(async move { while rx2.recv().await.is_some() {} });
        let ctx2 = ActivityContext::new_test_with_heartbeat(tx2)
            .with_heartbeat_details(Some(last))
            .with_deadline(Some(chrono::Utc::now() + chrono::Duration::seconds(1)))
            .with_attempt(2)
            .with_max_attempts(3);
        assert!(ctx2.is_expiring_within(RESERVE));

        let err = import_rows(&ctx2, ImportRequest { total_rows: 100 })
            .await
            .expect_err("the activity must yield, not run to the wire");
        assert!(
            err.contains("deadline reserve reached at row 4"),
            "the retry must resume at row 4, not re-execute from 0; got: {err}"
        );
    }

    /// With a generous budget it simply completes — the guard never trips.
    #[tokio::test]
    async fn import_completes_when_the_budget_is_ample() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

        let ctx = ActivityContext::new_test_with_heartbeat(tx)
            .with_deadline(Some(chrono::Utc::now() + chrono::Duration::seconds(300)));

        assert!(!ctx.is_expiring_within(RESERVE));
        let imported = import_rows(&ctx, ImportRequest { total_rows: 5 })
            .await
            .expect("import_rows");
        assert_eq!(imported, 5);
        drop(ctx);
        drain.await.expect("drain");
    }
}
