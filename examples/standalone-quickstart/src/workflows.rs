use std::time::Duration;
use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<String> {
    let result = ctx
        .execute_activity_raw(
            "send_welcome_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    Ok(result["status"].as_str().unwrap_or("sent").to_owned())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
async fn send_welcome_email(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let user_id = input["user_id"].as_i64().unwrap_or_default();
    tracing::info!(user_id, "sending welcome email");
    Ok(serde_json::json!({ "status": "sent" }))
}

/// Register the workflow and the activity. On the plugin path,
/// `HarvestPlugin::workflows` and `HarvestPlugin::activities` do this.
pub fn harvest_builder() -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows![onboarding])
        .activities(activities![send_welcome_email])
        .worker(WorkerConfig::default())
}

#[cfg(test)]
mod tests;
