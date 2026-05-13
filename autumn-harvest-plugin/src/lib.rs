//! Autumn plugin crate for autumn-harvest.

/// Management HTTP API and Axum routing.
pub mod api;
/// Runtime configuration structures for the Harvest plugin.
pub mod config;
/// Outbox pattern implementation for reliable workflow start requests.
pub mod outbox;
pub mod plugin;
pub mod preflight;
pub mod prelude;
pub mod runner;
pub mod shard_health;
/// Shared state and connection pooling for the Harvest plugin.
pub mod state;
pub mod ui;
pub mod version_gate_retirement;
pub mod version_usage;

pub use api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
    management_api_request_fields, management_api_response_fields, management_api_routes,
};
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
