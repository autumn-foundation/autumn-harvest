//! Autumn plugin crate for autumn-harvest.

// Issue #1821: a panic on a request path drops that request. A panic that
// poisons a shared lock can fail every later request on the replica. Non-test
// code returns an error instead. Each remaining site carries an `expect`
// attribute with its reason.
#![warn(clippy::expect_used, clippy::unwrap_used)]

pub mod api;
/// Optional per-client rate limiting for the management API (issue #1827).
pub mod api_rate_limit;
/// Scoped API tokens + rotation for the management API (issue #942).
pub mod api_token;
/// Pluggable authorizer hook for the management API (issue #1803).
pub mod authz;
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
/// AWS KMS binding for the core AES-256-GCM payload codec (issue #1825).
#[cfg(feature = "aws-kms")]
pub mod aws_kms;
/// Startup and shutdown steps that `HarvestPlugin` and `HarvestEmbedding`
/// share (issue #1613).
mod boot;
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
/// One entry point for a standalone embedding (issue #1613).
pub mod embedding;
/// One test suite for every KMS binding (issue #1981).
#[cfg(all(test, any(feature = "aws-kms", feature = "vault-transit")))]
mod kms_conformance;
pub mod lineage;
/// OIDC login for Vantage and the management API (issue #1978).
#[cfg(feature = "oidc")]
pub mod oidc;
pub mod outbox;
pub mod plugin;
pub mod preflight;
pub mod prelude;
/// Fleet-wide task-queue coverage read model (issue #774).
pub mod queue_coverage;
pub mod replay_diagnosis;
/// Custom roles for the management API and Vantage (issue #1978).
pub mod roles;
pub mod runner;
/// Cross-site request rejection for Vantage and DLQ mutations (issue #1278).
pub mod same_origin;
pub mod schedule_runs;
pub mod shard_fanout;
pub mod shard_health;
pub mod state;
pub mod status_summary;
/// Strict percent-decoding for raw HTTP query strings.
///
/// Shared by every management API route that filters on a raw `(key, value)`
/// pair list (issue #1151, extracted from the issue #774 `queue-coverage`
/// fix).
pub mod strict_query;
pub mod ui;
pub mod usage;
#[cfg(feature = "vault-transit")]
pub mod vault_transit;
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

/// OpenAPI 3.1 document for the management API (issue #694).
///
/// Derived from `docs/api-contract.json` and served read-only at
/// `GET /openapi.json`, so an integrator can generate a typed client.
pub mod openapi;

/// Built-in Prometheus scrape endpoint (issue #355).
///
/// Registers the nine ADR-0001 §7 catalogue metrics as an autumn-web
/// `MetricsSource` feeding the app's shared `/actuator/prometheus` endpoint.
#[cfg(feature = "metrics")]
pub mod metrics_scrape;

/// Compiles each Rust block in `docs/embedding.md` as a doctest (issue #1614).
///
/// Run it with `cargo test -p autumn-harvest-plugin --features metrics,webhooks
/// --doc EmbeddingDocSnippets`. The `standalone-chapter` job in CI runs that
/// command.
#[cfg(all(doctest, feature = "metrics", feature = "webhooks"))]
#[doc = include_str!("../../docs/embedding.md")]
struct EmbeddingDocSnippets;

pub use api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router, management_api_request_fields, management_api_response_fields,
    management_api_routes,
};
pub use config::{
    HarvestBatchConfig, HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig,
    HarvestReadinessConfig, HarvestRedisConfig, HarvestRuntimeConfig, HarvestStartupConfig,
    OrphanStartupAction,
};
pub use embedding::{HarvestEmbedding, HarvestEmbeddingRuntime};
pub use outbox::{
    WorkflowStartRequest, drain_workflow_start_outbox_once, enqueue_workflow_start_outbox,
    flush_workflow_start_outbox,
};
pub use plugin::HarvestPlugin;
pub use runner::{HarvestRunner, HarvestRunnerResources};
pub use state::{AppDbPool, HarvestDbPool};
pub use ui::harvest_ui_router;
