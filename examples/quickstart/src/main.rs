//! autumn-harvest quickstart: one workflow, one activity, durable timer.
//!
//! Run against a local Postgres (see compose.yaml) with:
//!
//!   AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p quickstart

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use autumn_harvest::prelude::*;
use autumn_harvest::{
    ExecutionId, ShardRouter, StartWorkflowParams, WorkflowHandleClient, WorkflowIdReusePolicy,
};
use autumn_harvest_plugin::{HarvestDbPool, HarvestPlugin};
use autumn_web::prelude::*;
use autumn_web::reexports::axum::extract::State;

/// Greets `name` with a welcome activity, pauses for 30 seconds on a durable
/// timer, then delivers a farewell activity.
///
/// The 30-second pause is intentional: kill the process and restart while the
/// timer is counting down. The engine replays the welcome step from Postgres
/// history and resumes exactly at the timer — without re-running the activity.
#[workflow]
async fn greeting(ctx: &WorkflowContext, name: String) -> HarvestResult<String> {
    let welcome: serde_json::Value = ctx
        .execute_activity(
            &send_greeting_info(),
            serde_json::json!({ "name": name, "kind": "welcome" }),
        )
        .await?;

    // Durable 30-second pause — the workflow suspends here.
    // Restart the process mid-timer; the engine resumes without re-executing
    // the welcome activity above.
    ctx.timer("greeting-pause", 30).await?;

    let farewell: serde_json::Value = ctx
        .execute_activity(
            &send_greeting_info(),
            serde_json::json!({ "name": name, "kind": "farewell" }),
        )
        .await?;

    Ok(format!(
        "{} — {}",
        welcome["message"].as_str().unwrap_or("(welcome)"),
        farewell["message"].as_str().unwrap_or("(farewell)"),
    ))
}

/// Fast request/response variant used by `POST /greet`.
#[workflow]
async fn instant_greeting(ctx: &WorkflowContext, name: String) -> HarvestResult<String> {
    let greeting: serde_json::Value = ctx
        .execute_activity(
            &send_greeting_info(),
            serde_json::json!({ "name": name, "kind": "hello" }),
        )
        .await?;

    Ok(greeting["message"]
        .as_str()
        .unwrap_or("hello from Harvest")
        .to_string())
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

#[post("/greet")]
async fn greet(
    State(state): State<autumn_web::AppState>,
    Json(request): Json<serde_json::Value>,
) -> AutumnResult<Json<serde_json::Value>> {
    let name = request["name"].as_str().unwrap_or("world").to_string();
    let workflow_id = request_response_workflow_id()?;
    let router = state
        .extension::<ShardRouter>()
        .map(|router| router.as_ref().clone())
        .unwrap_or_default();
    let shard = router.pick_for_new_workflow("instant_greeting", &workflow_id);
    let exec_id = ExecutionId::new_for_shard(shard);
    let harvest_pool = state.extension::<HarvestDbPool>().ok_or_else(|| {
        AutumnError::service_unavailable_msg("HarvestDbPool extension is not installed")
    })?;
    let client = state.extension::<WorkflowHandleClient>().ok_or_else(|| {
        AutumnError::service_unavailable_msg("WorkflowHandleClient extension is not installed")
    })?;
    let mut conn = harvest_pool
        .pool_for(shard)
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    let started = client
        .start_or_load(
            &mut conn,
            StartWorkflowParams {
                workflow_name: "instant_greeting",
                workflow_id: &workflow_id,
                exec_id,
                input: serde_json::json!(name),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: None,
                concurrency_limit: None,
            },
        )
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    let result = started
        .handle
        .result_raw()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    Ok(Json(result))
}

fn request_response_workflow_id() -> AutumnResult<String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?
        .as_nanos();
    Ok(format!("quickstart-greet-{nanos}"))
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .routes(routes![greet])
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![greeting, instant_greeting])
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
        let instant = __autumn_workflow_info_instant_greeting();
        let act = __autumn_activity_info_send_greeting();
        assert_eq!(wf.name, "greeting");
        assert_eq!(instant.name, "instant_greeting");
        assert_eq!(act.name, "send_greeting");
        // The activity has no explicit queue attribute, so the typed helper defaults to "default".
        assert!(
            WorkerConfig::default()
                .queues
                .contains(&"default".to_string())
        );
    }
}
