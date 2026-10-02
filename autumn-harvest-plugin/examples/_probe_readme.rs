#![allow(dead_code, unused)]
mod b1 {
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<()> {
    ctx.execute_activity_raw(
        "send_welcome_email",
        serde_json::json!({ "user_id": user_id }),
        "default",
    )
    .await?;
    Ok(())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, std::time::Duration::from_secs(1)))]
async fn send_welcome_email(_ctx: &ActivityContext, input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … real I/O. Failure here is retried per the policy above.
    Ok(serde_json::json!({ "sent": true }))
}
}
mod b2 { use super::b1::*;
use autumn_web::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![onboarding])
                .activities(activities![send_welcome_email])
                .api("/api/harvest"),
        )
        .run()
        .await;
}
}
mod b10 {
use autumn_harvest::prelude::*;

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
mod b15 { use autumn_harvest::prelude::*; use autumn_web::prelude::*; #[autumn_harvest::workflow] async fn daily_billing_report(ctx:&WorkflowContext, i: serde_json::Value)->HarvestResult<()>{Ok(())} fn f() {
use autumn_harvest::policy::{Schedule, WorkflowSchedule};

// Register a daily billing run at 03:00 UTC with at-most-1 concurrent run.
let sched = WorkflowSchedule::new(
    "daily_billing_report",
    Schedule::Cron("0 3 * * *".to_string()),
)
.with_input(serde_json::json!({"region": "us-east"}))
.with_max_active_runs(1);

// Wire it into the builder alongside your workflow registration.
let app = autumn_web::app()
    .workflows(workflows![daily_billing_report])
    .workflow_schedule(sched)
    .worker(WorkerConfig::default());
}}
fn main(){}
