//! autumn-harvest quickstart: one workflow, one activity, durable timer.
//!
//! Run against a local Postgres (see compose.yaml) with:
//!
//!   AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p quickstart

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

/// Greets `name` with a welcome activity, pauses for 30 seconds on a durable
/// timer, then delivers a farewell activity.
///
/// The 30-second pause is intentional: kill the process and restart while the
/// timer is counting down. The engine replays the welcome step from Postgres
/// history and resumes exactly at the timer — without re-running the activity.
#[workflow]
async fn greeting(ctx: &WorkflowContext, name: String) -> HarvestResult<String> {
    let welcome = ctx
        .execute_activity_raw(
            "send_greeting",
            serde_json::json!({ "name": name, "kind": "welcome" }),
            "default",
        )
        .await?;

    // Durable 30-second pause — the workflow suspends here.
    // Restart the process mid-timer; the engine resumes without re-executing
    // the welcome activity above.
    ctx.timer("greeting-pause", 30).await?;

    let farewell = ctx
        .execute_activity_raw(
            "send_greeting",
            serde_json::json!({ "name": name, "kind": "farewell" }),
            "default",
        )
        .await?;

    Ok(format!(
        "{} — {}",
        welcome["message"].as_str().unwrap_or("(welcome)"),
        farewell["message"].as_str().unwrap_or("(farewell)"),
    ))
}

/// Logs a greeting step and returns a confirmation message.
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
async fn send_greeting(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let name = input["name"].as_str().unwrap_or("world");
    let kind = input["kind"].as_str().unwrap_or("greeting");
    let message = format!("{kind} to {name}!");
    tracing::info!(name, kind, message, "greeting sent");
    Ok(serde_json::json!({ "message": message }))
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![greeting])
                .activities(activities![send_greeting])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quickstart_components_compile_and_register() {
        let wf = __autumn_workflow_info_greeting();
        let act = __autumn_activity_info_send_greeting();
        assert_eq!(wf.name, "greeting");
        assert_eq!(act.name, "send_greeting");
        // The default worker listens on "default", matching execute_activity_raw's queue arg.
        assert!(
            WorkerConfig::default()
                .queues
                .contains(&"default".to_string())
        );
    }
}
