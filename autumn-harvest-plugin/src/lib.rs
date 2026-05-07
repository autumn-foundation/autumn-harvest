//! Autumn plugin crate for autumn-harvest.

pub mod api;
pub mod config;
pub mod outbox;
pub mod plugin;
pub mod preflight;
pub mod prelude;
pub mod runner;
pub mod shard_health;
pub mod state;
pub mod ui;
pub mod version_usage;
pub mod version_gate_retirement;

pub use api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router};
pub use config::{
    HarvestBatchConfig, HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig,
    HarvestReadinessConfig, HarvestRuntimeConfig,
};
pub use outbox::{
    WorkflowStartRequest, drain_workflow_start_outbox_once, enqueue_workflow_start_outbox,
    flush_workflow_start_outbox,
};
pub use plugin::HarvestPlugin;
pub use runner::{HarvestRunner, HarvestRunnerResources};
pub use state::{AppDbPool, HarvestDbPool};
pub use ui::harvest_ui_router;
