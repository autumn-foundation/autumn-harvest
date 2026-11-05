#![allow(dead_code, unused)]
mod stub {
use autumn_harvest::prelude::*;
#[workflow]
pub async fn daily_billing_report(ctx: &WorkflowContext, input: serde_json::Value) -> HarvestResult<()> { Ok(()) }
}
mod schedule {
use super::stub::*;
fn wiring() -> Result<(), Box<dyn std::error::Error>> {
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::prelude::*;

// Register a daily billing run at 03:00 UTC with at-most-1 concurrent run.
let sched = WorkflowSchedule::new(
    "daily_billing_report",
    Schedule::Cron("0 3 * * *".to_string()),
)
.with_input(serde_json::json!({"region": "us-east"}))
.with_max_active_runs(1);

// Wire it into `HarvestBuilder` alongside your workflow registration.
let harvest = HarvestBuilder::new()
    .workflows(workflows![daily_billing_report])
    .workflow_schedule(sched)
    .worker(WorkerConfig::default());
Ok(()) }
}
mod retry {
use autumn_harvest::prelude::*;
use std::time::Duration;

#[activity(retry = RetryPolicy::exponential(5, Duration::from_secs(1)))]
async fn charge_card(ctx: &ActivityContext, amount: u32) -> Result<(), ActivityFailure> {
    // Transient — let the retry policy keep working.
    if amount == 0 {
        return Err(ActivityFailure::retryable(
            "UpstreamTimeout",
            "payment gateway timed out",
        ));
    }
    // Permanent — skip remaining retries, route straight to DLQ.
    if amount > 1_000_000 {
        return Err(ActivityFailure::non_retryable(
            "InvalidInput",
            "amount exceeds per-transaction ceiling",
        ));
    }
    Ok(())
}
}
fn main() {}
