//! Autumn plugin crate for autumn-harvest.

pub mod api;
/// Configuration types for the Harvest runtime plugin.
pub mod config;
/// Outbox relay for reliable workflow publication.
pub mod outbox;
pub mod plugin;
pub mod prelude;
pub mod runner;
/// Shared state containers and connection pools for Harvest.
pub mod state;
pub mod ui;

pub use api::{HarvestApiRuntime, HarvestApiState, harvest_api_router};
pub use config::{HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig, HarvestRuntimeConfig};
pub use outbox::{
    WorkflowStartRequest, drain_workflow_start_outbox_once, enqueue_workflow_start_outbox,
    flush_workflow_start_outbox,
};
pub use plugin::HarvestPlugin;
pub use runner::{HarvestRunner, HarvestRunnerResources};
pub use state::{AppDbPool, HarvestDbPool};
pub use ui::harvest_ui_router;
