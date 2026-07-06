//! No-database HTTP tests for the built-in Prometheus scrape endpoint
//! (issue #355).
//!
//! `HarvestPlugin::with_metrics_scrape()` registers a `HarvestMetricsRecorder`
//! as both the engine's `MetricsRecorder` and an autumn-web `MetricsSource`
//! feeding the shared `/actuator/prometheus` endpoint. That registration
//! happens synchronously inside `Plugin::build` -- only the `on_startup`
//! hook that installs the live Harvest runtime needs Postgres.
//!
//! NOTE: `TestApp::plugin(HarvestPlugin)` cannot be used here for the same
//! reason `mcp_tools_http_tests.rs` cannot use it -- `TestApp` replays
//! plugin startup hooks and `start_harvest_runtime` requires a live
//! Postgres. This suite reproduces exactly the metrics-source-registration
//! half of `HarvestPlugin::build` through a tiny local `Plugin`, so the real
//! autumn-web rendering pipeline (HELP/TYPE lines, label sorting/escaping,
//! dedup-by-family-name) is exercised end to end without a database. The
//! full plugin-wired flow (a live workflow run driving the same recorder
//! through `HarvestBuilder::telemetry`) is covered by the runnable example
//! `examples/metrics_scrape_quickstart.rs`.

#![cfg(feature = "metrics")]

use std::sync::Arc;

use autumn_harvest::telemetry::{ActivityStatus, MetricsRecorder, WorkflowStatus};
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_web::app::AppBuilder;
use autumn_web::plugin::Plugin;
use autumn_web::test::{TestApp, TestClient};

/// Mirrors the metrics-source-registration half of `HarvestPlugin::build`
/// without the DB-dependent `on_startup`/`nest` wiring.
struct MetricsScrapeTestPlugin(HarvestMetricsRecorder);

impl Plugin for MetricsScrapeTestPlugin {
    fn build(self, app: AppBuilder) -> AppBuilder {
        app.metrics_source("harvest", Arc::new(self.0))
    }
}

async fn scrape(client: &TestClient) -> String {
    let resp = client.get("/actuator/prometheus").send().await;
    resp.assert_ok();
    resp.text()
}

#[tokio::test]
async fn scrape_endpoint_renders_all_nine_catalogue_metrics_after_recording() {
    let recorder = HarvestMetricsRecorder::new();
    recorder.record_workflow_started("onboarding", "default");
    recorder.record_workflow_completed("onboarding", "default", 1.5, WorkflowStatus::Completed);
    recorder.record_activity_completed(
        "send_email",
        "email-workers",
        0.25,
        ActivityStatus::Completed,
    );
    recorder.record_timer_started(30.0);
    recorder.record_queue_depth("default", 3);
    recorder.record_dlq_entries(0, 2);
    recorder.record_schedule_run("cron", "nightly_report");
    recorder.record_schedule_skipped("cron", "nightly_report", "overlap");
    recorder.record_retention_tick(0, 100, 42, 0.5);

    let client = TestApp::new()
        .plugin(MetricsScrapeTestPlugin(recorder))
        .build();
    let body = scrape(&client).await;

    // The nine ADR-0001 §7 catalogue metrics (issue #355 AC2). Duration
    // histograms render as `_count`/`_sum` counter pairs since
    // `autumn_web::actuator::MetricKind` has no histogram variant.
    for expected in [
        "# TYPE harvest_workflow_started_total counter",
        "harvest_workflow_started_total{queue=\"default\",workflow=\"onboarding\"} 1",
        "# TYPE harvest_workflow_duration_count counter",
        "harvest_workflow_duration_count{queue=\"default\",status=\"completed\",workflow=\"onboarding\"} 1",
        "# TYPE harvest_workflow_duration_sum counter",
        "harvest_workflow_duration_sum{queue=\"default\",status=\"completed\",workflow=\"onboarding\"} 1.5",
        "# TYPE harvest_activity_duration_count counter",
        "harvest_activity_duration_count{activity=\"send_email\",queue=\"email-workers\",status=\"completed\"} 1",
        "# TYPE harvest_activity_duration_sum counter",
        "harvest_activity_duration_sum{activity=\"send_email\",queue=\"email-workers\",status=\"completed\"} 0.25",
        "# TYPE harvest_timer_started_total counter",
        "harvest_timer_started_total 1",
        "# TYPE harvest_queue_depth gauge",
        "harvest_queue_depth{queue=\"default\"} 3",
        "# TYPE harvest_dlq_entries gauge",
        "harvest_dlq_entries{shard=\"0\"} 2",
        "# TYPE harvest_schedule_runs_total counter",
        "harvest_schedule_runs_total{kind=\"cron\",name=\"nightly_report\"} 1",
        "# TYPE harvest_schedule_skipped_total counter",
        "harvest_schedule_skipped_total{kind=\"cron\",name=\"nightly_report\",reason=\"overlap\"} 1",
        "# TYPE harvest_retention_deleted_total counter",
        "harvest_retention_deleted_total{shard=\"0\"} 42",
    ] {
        assert!(body.contains(expected), "missing `{expected}` in:\n{body}");
    }

    // Cardinality rule (issue #355 AC2): no execution.id label ever appears
    // -- the `MetricsRecorder` methods this recorder implements never
    // accept one, so this also guards against a future accidental addition.
    assert!(
        !body.contains("execution.id") && !body.contains("execution_id"),
        "execution.id leaked into scrape:\n{body}"
    );

    // Harvest's samples coexist with the app's own built-in families on the
    // one shared endpoint (issue #355: "one endpoint for autumn web apps
    // and its plugins like harvest").
    assert!(body.contains("autumn_http_requests_total"));
}

#[tokio::test]
async fn scrape_endpoint_omits_harvest_families_when_metrics_source_not_registered() {
    // No plugin, no `metrics_source` registration -- mirrors an app that
    // never calls `HarvestPlugin::with_metrics_scrape()`. `/actuator/prometheus`
    // still exists (it's the app's own endpoint, always mounted by
    // default) but carries no `harvest_*` family, and there is zero runtime
    // cost paid for the unused aggregator (issue #355 AC3/AC8).
    let client = TestApp::new().build();
    let body = scrape(&client).await;
    assert!(
        !body.contains("harvest_"),
        "unexpected harvest_* family with no HarvestMetricsRecorder registered:\n{body}"
    );
    assert!(body.contains("autumn_http_requests_total"));
}

#[tokio::test]
async fn no_family_emitted_for_a_metric_that_was_never_recorded() {
    // A cold aggregator contributes zero families -- there's no way to
    // synthesize a meaningful zero-value default for a labeled series
    // before its label values are known (see `metrics_scrape.rs` module
    // docs).
    let client = TestApp::new()
        .plugin(MetricsScrapeTestPlugin(HarvestMetricsRecorder::new()))
        .build();
    let body = scrape(&client).await;
    assert!(!body.contains("harvest_workflow_started_total"));
    assert!(!body.contains("harvest_queue_depth"));
}
