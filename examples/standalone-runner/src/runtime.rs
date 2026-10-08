use std::sync::Arc;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::*;

use crate::domain::RUNNER_QUEUE;
use crate::{activities, workflows};

pub fn standalone_runtime_config(database_url: String) -> HarvestRuntimeConfig {
    HarvestRuntimeConfig {
        mode: HarvestMode::External,
        worker_enabled: true,
        scheduler_enabled: true,
        database: HarvestDatabaseConfig {
            url: Some(database_url),
        },
        outbox: HarvestOutboxConfig {
            enabled: false,
            ..HarvestOutboxConfig::default()
        },
        // `HarvestEmbedding::start` applies the operator's `[harvest.startup]`
        // settings over this code config (issue #1613).
        ..HarvestRuntimeConfig::default()
    }
}

/// `metrics` is the same `HarvestMetricsRecorder` instance `server.rs` wires
/// into the `/metrics` route (issue #1611). The plugin path's
/// `HarvestPlugin::with_metrics_scrape()` makes this same
/// `HarvestBuilder::telemetry(..)` call for the caller. A standalone
/// embedder has no plugin to do it, so it is one explicit argument here.
pub fn standalone_builder(metrics: HarvestMetricsRecorder) -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows::workflows())
        .activities(activities::activities())
        .worker(WorkerConfig::default().with_queues([RUNNER_QUEUE]))
        .telemetry(
            TelemetryConfig::builder()
                .metrics(Arc::new(metrics))
                .build(),
        )
}
