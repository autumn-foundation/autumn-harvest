//! Curl the built-in Prometheus scrape endpoint after a single workflow run
//! (issue #355).
//!
//! Run with:
//! ```bash
//! export AUTUMN_DATABASE__URL=postgres://localhost/harvest_demo
//! cargo run -p autumn-harvest-plugin --example metrics_scrape_quickstart --features metrics
//! ```
//!
//! Wait a few seconds for the app to boot and the workflow below to run
//! once, then in another terminal:
//! ```bash
//! curl http://localhost:3000/actuator/prometheus | grep ^harvest_
//! ```
//!
//! `HarvestPlugin::with_metrics_scrape()` alone puts Harvest's nine ADR-0001
//! §7 catalogue metrics on the app's shared `/actuator/prometheus` endpoint.
//! There is no exporter to install, no route to wire, no new dependency. This
//! example drives all nine within a few seconds of boot:
//!
//! - `harvest_workflow_started_total` / `harvest_workflow_duration_{count,sum}`
//!   -- the `onboarding` workflow below is started once, right after boot.
//! - `harvest_activity_duration_{count,sum}` -- `onboarding` calls one activity.
//! - `harvest_timer_started_total` -- `onboarding` also waits on a durable timer.
//! - `harvest_queue_depth` / `harvest_dlq_entries` -- sampled every
//!   `poll_interval` (500ms by default), even at zero. Both appear within
//!   about a second of boot with no dedicated setup.
//! - `harvest_schedule_runs_total` / `harvest_schedule_skipped_total` -- the
//!   `every_3_seconds` DAG below fires every 3s. Its activity takes ~4s, so
//!   every other fire collides with the previous run and is skipped. The
//!   observed reason is `max_active_runs_reached`, a DAG's default
//!   concurrency cap of 1.
//! - `harvest_retention_deleted_total` -- retention uses a 1s
//!   `max_age`/tick interval so the family appears quickly. A live run
//!   confirms that the family and its `# HELP`/`# TYPE` lines are always
//!   present once retention is configured. The *count* can stay at 0 for a while. A
//!   completed execution keeps its last-processed worker's id. The retention
//!   candidate query deliberately excludes such rows. So deletion proceeds
//!   only after normal fleet churn clears that stamp. Present-but-zero is
//!   still a correct sample, as for `harvest_dlq_entries` and
//!   `harvest_queue_depth` above when nothing is backlogged.
//!
//! None of this is production retention/schedule tuning advice. The intervals
//! are short so the demo is observable in seconds, not hours.

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::retention::RetentionConfig;
use autumn_harvest_plugin::HarvestPlugin;

#[activity]
#[allow(clippy::unused_async)] // #[activity] handlers must be async fn
async fn send_email(_ctx: &ActivityContext, addr: String) -> Result<(), String> {
    tracing::info!(%addr, "sent email");
    Ok(())
}

/// The workflow this example starts once, right after boot -- drives
/// `harvest_workflow_started_total`, `harvest_workflow_duration_{count,sum}`,
/// `harvest_activity_duration_{count,sum}`, and `harvest_timer_started_total`.
#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    ctx.execute_activity::<_, ()>(&send_email_info(), format!("user-{user_id}@example.com"))
        .await
        .map_err(|e| e.to_string())?;
    ctx.timer("welcome_followup", 1)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[activity]
async fn slow_background_task(_ctx: &ActivityContext) -> Result<(), String> {
    // Outlasts the schedule's 3s interval, so every other fire collides with
    // this one and is skipped. That drives `harvest_schedule_skipped_total`.
    tokio::time::sleep(Duration::from_secs(4)).await;
    Ok(())
}

/// A unified DAG (issue #256) scheduled to fire every 3 seconds -- drives
/// `harvest_schedule_runs_total` and, via the slow activity above,
/// `harvest_schedule_skipped_total`.
#[dag(schedule = "*/3 * * * * *")]
fn every_3_seconds(dag: &mut DagBuilder) {
    let _ = dag.activity(slow_background_task);
}

/// autumn-web requires at least one top-level `.routes(...)` registration
/// (unrelated to Harvest); this is that one route for the app.
#[autumn_web::get("/")]
async fn index() -> &'static str {
    "metrics_scrape_quickstart is running -- see /actuator/prometheus"
}

#[autumn_web::main]
async fn main() {
    let app = autumn_web::app().routes(autumn_web::routes![index]).plugin(
        HarvestPlugin::new()
            .workflows(workflows![onboarding])
            .activities(activities![send_email, slow_background_task])
            .dags(dags![every_3_seconds])
            .worker(WorkerConfig::default())
            .retention(RetentionConfig {
                max_age_secs: Some(1),
                tick_interval_secs: 1,
                ..RetentionConfig::default()
            })
            .api("/api/harvest")
            .with_metrics_scrape(),
    );

    tokio::spawn(async move {
        // Give the plugin's on_startup hook time to install the runtime.
        // Then start the single demo run over the HTTP API a real caller uses.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let resp = autumn_harvest_plugin::http::client_builder()
            .build()
            .expect("client")
            .post("http://localhost:3000/api/harvest/workflows/onboarding/start")
            .json(&serde_json::json!({ "input": 42 }))
            .send()
            .await
            .expect("start the demo workflow over HTTP");
        tracing::info!(
            status = %resp.status(),
            "started the onboarding workflow -- curl /actuator/prometheus in a few seconds"
        );
    });

    // The `run` future is large since autumn-web 0.8, so it is boxed.
    // That keeps it off the stack and satisfies `clippy::large_futures`.
    Box::pin(app.run()).await;
}
