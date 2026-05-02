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
        batch: autumn_harvest_plugin::config::HarvestBatchConfig::default(),
    }
}

pub fn standalone_builder() -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows::workflows())
        .activities(activities::activities())
        .worker(WorkerConfig::default().with_queues([RUNNER_QUEUE]))
}
