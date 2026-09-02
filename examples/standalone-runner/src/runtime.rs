use autumn_harvest::prelude::*;
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
        // Issue #1128: `HarvestRunner::start` runs the boot-time
        // orphaned-workflow-type gate from the config it is HANDED — unlike the
        // plugin, nothing loads configuration on this path. A standalone
        // embedder that builds its config in code, as this example does, must
        // therefore thread the operator's `[harvest.startup] orphaned_workflows`
        // setting (and its `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS`
        // override) through itself; otherwise `fail` in a TOML file is inert and
        // the action silently stays `warn`. Falling back to the default rather
        // than failing keeps a missing/unparsable config file from stopping a
        // process over a boot check, which is the same spirit as the gate's own
        // crash-loop rule.
        startup: HarvestRuntimeConfig::load()
            .map(|loaded| loaded.startup)
            .unwrap_or_default(),
        ..HarvestRuntimeConfig::default()
    }
}

pub fn standalone_builder() -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows::workflows())
        .activities(activities::activities())
        .worker(WorkerConfig::default().with_queues([RUNNER_QUEUE]))
}
