//! Autumn plugin crate for autumn-harvest.

pub mod api;
/// Scoped API tokens + rotation for the management API (issue #942).
pub mod api_token;
/// Default `reqwest`-based completion-callback deliverer (issue #605).
///
/// Implements [`autumn_harvest::completion_callback::CompletionCallbackDeliverer`],
/// auto-wired by [`crate::plugin::HarvestPlugin`].
pub mod callback_deliverer;

/// Default `reqwest`-based signed-webhook sink for audit-record export.
///
/// Implements [`autumn_harvest::audit_export::AuditSink`], auto-wired by
/// `HarvestPlugin` when an embedder configures `audit_export_webhook(...)`
/// without supplying their own sink (issue #953).
pub mod audit_sink;
pub mod canary;
pub mod config;
/// Broker event-source connectors for workflow triggers (issue #944).
///
/// The core `autumn-harvest` crate gains **zero** broker dependencies: every
/// broker client lives behind this plugin's `kafka` / `sqs` features.
#[cfg(feature = "connectors")]
pub mod connector;
pub mod dag_graph;
pub mod dag_retry;
/// Zero-setup local dev runtime (issue #525).
///
/// Provisions an ephemeral PostgreSQL, applies the ordinary embedded
/// migrations, runs a worker and serves the management API + Vantage UI — with
/// no Docker, no `compose.yaml`, no `DATABASE_URL` and no `diesel migration
/// run`. Development and evaluation only; see the module docs.
#[cfg(feature = "dev-runtime")]
pub mod dev;
pub mod lineage;
pub mod outbox;
pub mod plugin;
pub mod preflight;
pub mod prelude;
/// Fleet-wide task-queue coverage read model (issue #774).
pub mod queue_coverage;
pub mod replay_diagnosis;
pub mod runner;
pub mod schedule_runs;
pub mod shard_fanout;
pub mod shard_health;
pub mod state;
pub mod status_summary;
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

/// Built-in Prometheus scrape endpoint (issue #355).
///
/// Registers the nine ADR-0001 §7 catalogue metrics as an autumn-web
/// `MetricsSource` feeding the app's shared `/actuator/prometheus` endpoint.
#[cfg(feature = "metrics")]
pub mod metrics_scrape;

pub use api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
    management_api_request_fields, management_api_response_fields, management_api_routes,
};
pub use config::{
    HarvestBatchConfig, HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig,
    HarvestReadinessConfig, HarvestRuntimeConfig, HarvestStartupConfig, OrphanStartupAction,
};
pub use outbox::{
    WorkflowStartRequest, drain_workflow_start_outbox_once, enqueue_workflow_start_outbox,
    flush_workflow_start_outbox,
};
pub use plugin::HarvestPlugin;
pub use runner::{HarvestRunner, HarvestRunnerResources};
pub use state::{AppDbPool, HarvestDbPool};
pub use ui::harvest_ui_router;
