//! The built-in sample workflow (issue #525, AC2/AC3).
//!
//! What a first-time reader needs to see in their first minute is not
//! throughput — it is **durability**. So the sample runs an activity, waits on a
//! durable timer, and runs another: kill the process mid-timer, start it again,
//! and the run resumes from history instead of re-executing. That is the one
//! property that distinguishes this engine from a job queue, and the banner
//! points straight at it.

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_web::AppState;
use autumn_web::extract::State;
use autumn_web::prelude::*;
use autumn_web::reexports::axum::response::Redirect;

/// Name of the sample workflow, shared by the banner and the registration so
/// the copy-pasteable command can never name a workflow that is not registered.
pub const SAMPLE_WORKFLOW: &str = "dev_greeting";

/// How long the sample pauses on its durable timer.
///
/// Short enough that the whole run finishes inside the onboarding budget,
/// long enough to Ctrl-C into.
const TIMER_SECONDS: u64 = 10;

/// Greets `name`, pauses on a durable timer, then says goodbye.
#[workflow(owner = "dev-runtime", severity = "sev4")]
async fn dev_greeting(ctx: &WorkflowContext, name: String) -> HarvestResult<String> {
    // Replay-aware: this logs once even though the history is replayed on every
    // decision cycle (guardrail HVG009).
    ctx.logger().info("dev_greeting started");

    let welcome: serde_json::Value = ctx
        .execute_activity(
            &dev_greet_info(),
            serde_json::json!({ "name": name, "kind": "welcome" }),
        )
        .await?;

    ctx.timer("dev-greeting-pause", TIMER_SECONDS).await?;

    let farewell: serde_json::Value = ctx
        .execute_activity(
            &dev_greet_info(),
            serde_json::json!({ "name": name, "kind": "farewell" }),
        )
        .await?;

    Ok(format!(
        "{} — {}",
        welcome["message"].as_str().unwrap_or("(welcome)"),
        farewell["message"].as_str().unwrap_or("(farewell)"),
    ))
}

/// Produces one greeting line.
// The body is pure formatting; the `async` is the activity ABI, not a choice.
#[allow(clippy::unused_async)]
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
async fn dev_greet(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let name = input["name"].as_str().unwrap_or("world");
    let kind = input["kind"].as_str().unwrap_or("greeting");
    let message = format!("{kind} to {name}!");
    tracing::info!(name, kind, "dev runtime sample activity ran");
    Ok(serde_json::json!({ "message": message }))
}

/// Where `GET /` should send a browser, published as an app extension so the
/// landing route can honour a non-default `--api-path`.
#[derive(Debug, Clone)]
pub struct DevDashboardPath(pub String);

/// Sends a browser straight to the Vantage dashboard.
///
/// This route is also what makes the app *legal*: `AppBuilder::run` asserts at
/// least one route is registered, and `HarvestPlugin::api()` mounts through
/// `nest`, which does not count. Without it the whole runtime panicked on boot
/// and then timed out waiting for a server that had already given up.
#[get("/")]
async fn dev_index(State(state): State<AppState>) -> Redirect {
    let path = state
        .extension::<DevDashboardPath>()
        .map_or_else(|| "/api/harvest/ui".to_owned(), |path| path.0.clone());
    Redirect::temporary(&path)
}

/// The routes the dev runtime serves outside the management API.
#[must_use]
pub fn routes() -> Vec<autumn_web::Route> {
    routes![dev_index]
}

/// The sample workflows the dev runtime registers.
#[must_use]
pub fn workflows() -> Vec<WorkflowInfo> {
    vec![dev_greeting_info()]
}

/// The sample activities the dev runtime registers.
#[must_use]
pub fn activities() -> Vec<ActivityInfo> {
    vec![dev_greet_info()]
}
