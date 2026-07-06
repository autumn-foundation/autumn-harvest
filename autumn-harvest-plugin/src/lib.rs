//! Autumn plugin crate for autumn-harvest.

pub mod api;
/// Default `reqwest`-based completion-callback deliverer (issue #605).
///
/// Implements [`autumn_harvest::completion_callback::CompletionCallbackDeliverer`],
/// auto-wired by [`crate::plugin::HarvestPlugin`].
pub mod callback_deliverer;
pub mod config;
pub mod dag_retry;
pub mod outbox;
pub mod plugin;
pub mod preflight;
pub mod prelude;
pub mod runner;
pub mod schedule_runs;
pub mod shard_fanout;
pub mod shard_health;
pub mod state;
pub mod ui;
pub mod usage;
pub mod version_gate_retirement;
pub mod version_usage;
pub mod workflow_count;
pub mod workflow_reachability;

#[cfg(feature = "webhooks")]
pub mod webhook;

/// Inbound HTTP webhook receiver route generation and dispatch (issue #344).
#[cfg(feature = "webhooks")]
pub mod webhook_receiver;

#[cfg(feature = "mcp")]
pub mod mcp_tools;

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
