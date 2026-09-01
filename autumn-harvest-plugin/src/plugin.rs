//! `HarvestPlugin` — the [`Plugin`] implementation that wires
//! the Harvest workflow engine into an Autumn [`AppBuilder`].

use std::any::Any;
#[cfg(feature = "connectors")]
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use autumn_web::AppState;
use autumn_web::app::AppBuilder;
use autumn_web::config::{AutumnConfig, DatabaseConfig};
use autumn_web::db;
use autumn_web::error::AutumnError;
use autumn_web::migrate::{EmbeddedMigrations, embed_migrations};
use autumn_web::plugin::Plugin;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{HarvestApiState, acquire_conn, harvest_api_router};
use crate::config::{HarvestMode, HarvestRuntimeConfig};
use crate::outbox::spawn_workflow_start_outbox_relay;
use crate::runner::{HarvestRunner, HarvestRunnerResources};
use crate::ui::harvest_ui_router;
use autumn_harvest::WorkflowHandleClient;
use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
use autumn_harvest::info::{ActivityInfo, DagInfo, WorkflowInfo};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::DbPool;

const HARVEST_MIGRATIONS: EmbeddedMigrations = autumn_harvest::MIGRATIONS;
/// Plugin migrations that belong to the **application** database — the
/// transactional-outbox table an embedder writes to in their own business
/// transaction.
const OUTBOX_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/app");

/// Plugin migrations that belong to the **harvest** database.
///
/// Split from [`OUTBOX_MIGRATIONS`] because the two are different databases
/// under `HarvestMode::Split`/`External`. The connector dead-letter table
/// (issue #944) is written by `PostgresDeadLetterSink`, which is built from
/// the *harvest* pool — applying it only to the app database left the table
/// missing where the sink actually writes, so every dead-letter write failed,
/// `settle` downgraded the poison message to a retry, and it was redelivered
/// forever: exactly the partition wedge the feature exists to prevent.
const PLUGIN_HARVEST_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations/harvest");

/// Hand Harvest's embedded migration sets to Autumn, which owns applying them
/// (autumn-web 0.7's [`AppBuilder::plugin_migrations`]).
///
/// Autumn applies every registered set inside `setup_database`, i.e. *before*
/// any `on_startup` hook runs, so `start_harvest_runtime` always observes an
/// already-migrated database. It also keeps the same dev/prod policy the plugin
/// applied by hand before: auto-apply under the `dev` profile, warn-only
/// otherwise (run a one-shot `autumn migrate` before rolling prod replicas).
///
/// Registering here rather than migrating ourselves is what lets Autumn resolve
/// **version collisions across plugins**. Diesel's `__diesel_schema_migrations`
/// is keyed by version alone, so two independently authored migrations picking
/// the same timestamp would otherwise silently skip one of them forever. Autumn
/// sees every registered set at once and tracks the loser under a substitute
/// version so both still apply.
///
/// # Why the harvest sets are conditional
///
/// `plugin_migrations` can only target the application's primary database.
/// Under [`HarvestMode::Embedded`] (the default) that *is* the Harvest
/// database, so Autumn can own all three sets. Under [`HarvestMode::Split`] /
/// [`HarvestMode::External`], Harvest storage is a **separate** database that
/// Autumn has no handle on — those two sets stay on the plugin's own apply path
/// in [`ensure_runtime_migrations`], which connects to `harvest.database.url`
/// directly.
///
/// The outbox set is unconditional: the transactional-outbox table an embedder
/// writes to in their own business transaction always lives in the application
/// database, in every mode.
///
/// If the Harvest config cannot be loaded here, registration fails immediately
/// rather than guessing: applying the sets to the app database for an unknown
/// mode could create Harvest tables in the wrong place, while registering only
/// the outbox would let migration-only commands report false success.
fn register_plugin_migrations(app: AppBuilder) -> AppBuilder {
    let app = app.plugin_migrations("autumn-harvest-plugin (outbox)", OUTBOX_MIGRATIONS);

    match migration_registration_mode(HarvestRuntimeConfig::load()) {
        HarvestMode::Embedded => app
            .plugin_migrations("autumn-harvest", HARVEST_MIGRATIONS)
            .plugin_migrations("autumn-harvest-plugin", PLUGIN_HARVEST_MIGRATIONS),
        HarvestMode::Split | HarvestMode::External => app,
    }
}

/// Resolve the topology before migration registration. This must be fail-fast:
/// `autumn migrate` builds plugins but does not execute their startup hooks, so
/// deferring a configuration error would let that command report success after
/// registering only the application outbox migrations.
fn migration_registration_mode(
    config: Result<HarvestRuntimeConfig, autumn_web::config::ConfigError>,
) -> HarvestMode {
    config
        .unwrap_or_else(|error| panic!("cannot register Harvest migrations: {error}"))
        .mode
}

struct OutboxRuntime {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

struct GateRefreshRuntime {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

struct HarvestRuntime {
    runner: HarvestRunner,
    outbox: Option<OutboxRuntime>,
    gate_refresh: Option<GateRefreshRuntime>,
    /// Running broker connector consumer loops (issue #944).
    #[cfg(feature = "connectors")]
    connectors: Vec<ConnectorRuntimeHandle>,
}

/// Plugin-local shared slot: holds the pre-built `HarvestBuilder` until the
/// first `on_startup` call consumes it, then holds the running `HarvestRuntime`
/// until `on_shutdown` stops it.
#[derive(Default)]
struct HarvestRuntimeSlot {
    builder: Option<HarvestBuilder>,
    runtime: Option<HarvestRuntime>,
    /// Broker connector registrations (issue #944), stashed alongside the
    /// builder so `start_harvest_runtime` can spawn one consumer loop each
    /// once the API state and pools exist.
    #[cfg(feature = "connectors")]
    connectors: Vec<ConnectorRegistration>,
}

type ApiMiddlewareFn = Box<
    dyn FnOnce(
            autumn_web::reexports::axum::Router<autumn_web::AppState>,
        ) -> autumn_web::reexports::axum::Router<autumn_web::AppState>
        + Send
        + Sync,
>;

/// Applies the same auth layer configured via [`HarvestPlugin::api_with_auth`]
/// to one generated MCP tool route's `MethodRouter` (issue #597 hardening).
///
/// The MCP tool routes are registered via `AppBuilder::routes(...)`, not
/// `nest()`, so they never pass through the `ApiMiddlewareFn` above (which
/// only wraps the nested management-API router). Without this, a caller
/// could hit a mutating tool route's own HTTP path directly and bypass both
/// the configured admin auth and autumn-web's `secure_mcp` (which only gates
/// the `/mcp` JSON-RPC envelope, not a typed route's direct path). `Fn` (not
/// `FnOnce`) so it can be cloned onto every generated route.
pub(crate) type McpToolMiddlewareFn = std::sync::Arc<
    dyn Fn(
            autumn_web::reexports::axum::routing::MethodRouter<autumn_web::AppState>,
        ) -> autumn_web::reexports::axum::routing::MethodRouter<autumn_web::AppState>
        + Send
        + Sync,
>;

/// Autumn plugin that embeds the Harvest workflow engine in an application.
///
/// # Example
///
/// ```rust,no_run
/// use autumn_harvest_plugin::HarvestPlugin;
/// use autumn_harvest::prelude::*;
///
/// # #[autumn_web::main]
/// # async fn main() {
/// autumn_web::app()
///     .plugin(
///         HarvestPlugin::new()
///             .worker(WorkerConfig::default())
///             .api("/api/harvest"),
///     )
///     .run()
///     .await;
/// # }
/// ```
// Each bool is an independent, orthogonal opt-in mirrored onto HarvestApiState
// (mcp tools, read-path decode, read-only role, metrics scrape); they are not a
// state machine that a bitfield/enum would model better. The count crosses the
// clippy threshold only under the `metrics` feature.
#[allow(clippy::struct_excessive_bools)]
pub struct HarvestPlugin {
    builder: HarvestBuilder,
    api_path: Option<String>,
    api_middleware: Option<ApiMiddlewareFn>,
    /// Same auth layer as `api_middleware`, applicable to a `MethodRouter` so
    /// it can also wrap the generated MCP tool routes (issue #597
    /// hardening). Populated alongside `api_middleware` by
    /// [`Self::api_with_auth`].
    mcp_tool_middleware: Option<McpToolMiddlewareFn>,
    /// Register MCP tool routes for `#[workflow(mcp)]` workflows (issue #597).
    /// Set via [`Self::mcp_tools`] / [`Self::mcp_tools_at`] (feature `mcp`).
    mcp_tools_enabled: bool,
    /// Optional prefix override for the generated MCP tool routes.
    mcp_tools_prefix: Option<String>,
    /// Deployment-level opt-in for operator read-path payload decoding
    /// (issue #608). Set via [`Self::decode_payloads_on_read`]; default off.
    decode_payloads_on_read: bool,
    /// Opt-in for the read-only operator role (issue #776). Set true by
    /// [`Self::api_with_role_auth`]; installs the class-aware enforcement
    /// layer. Default off — the router is byte-for-byte unchanged.
    role_auth_enabled: bool,
    /// Opt-in for scoped API tokens (issue #942). Set true by
    /// [`Self::enable_api_tokens`]; installs the token verification + scope
    /// layer. Default off — the router is byte-for-byte unchanged (AC7).
    api_tokens_enabled: bool,
    /// Thresholds for the rolled-up `GET /admin/status` verdict (issue #679).
    /// Set via [`Self::with_status_thresholds`]; starter defaults otherwise.
    status_thresholds: crate::status_summary::StatusThresholds,
    /// Built-in synthetic liveness canary configuration (issue #796). Set via
    /// [`Self::synthetic_canary`]; `None` (default) leaves the canary entirely
    /// off and the runtime byte-for-byte unchanged (AC1).
    canary_config: Option<crate::canary::CanaryConfig>,
    /// Inbound webhook trigger bindings produced by `autumn_harvest::webhooks!`
    /// (issue #344). Set via [`Self::webhooks`] (feature `webhooks`).
    #[cfg(feature = "webhooks")]
    webhook_triggers: Vec<autumn_harvest::webhook_trigger::WebhookTriggerInfo>,
    /// Broker event-source connectors (issue #944). Set via
    /// [`Self::connector`] (feature `connectors`). Each entry pairs one
    /// binding with the adapter that feeds it.
    #[cfg(feature = "connectors")]
    connectors: Vec<ConnectorRegistration>,
    /// Built-in Prometheus scrape endpoint (issue #355). Set via
    /// [`Self::with_metrics_scrape`] (feature `metrics`).
    #[cfg(feature = "metrics")]
    metrics_scrape_enabled: bool,
}

/// One registered broker connector: a binding plus the source that feeds it.
#[cfg(feature = "connectors")]
struct ConnectorRegistration {
    binding: std::sync::Arc<crate::connector::SourceBinding>,
    source: std::sync::Arc<dyn crate::connector::EventSource>,
    config: Option<crate::connector::ConnectorRuntimeConfig>,
}

/// A running connector consumer loop.
#[cfg(feature = "connectors")]
struct ConnectorRuntimeHandle {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

/// Stop every connector consumer loop, cancelling *all* of them before
/// awaiting *any*.
///
/// Cancelling and awaiting one at a time would serialize the drains:
/// connector *N* keeps consuming — and therefore keeps accruing more in-flight
/// work of its own to drain — for as long as connectors *1..N* take to finish.
/// Each drain is individually bounded
/// ([`ConnectorRuntime::drain_in_flight`](crate::connector::ConnectorRuntime)),
/// but serialized those bounds *sum*, so a fleet of bindings turns a
/// per-connector deadline into an N-times-longer shutdown — and the messages
/// the late connectors consumed in the meantime are the ones most likely to be
/// hard-stopped when their own turn finally comes. Cancelling first starts
/// every drain deadline at the same instant, so the total is the slowest
/// connector rather than their sum.
#[cfg(feature = "connectors")]
async fn stop_connectors(connectors: Vec<ConnectorRuntimeHandle>) {
    let draining: Vec<JoinHandle<()>> = connectors
        .into_iter()
        .map(|connector| {
            connector.shutdown.cancel();
            connector.handle
        })
        .collect();

    for handle in draining {
        if let Err(error) = handle.await
            && !error.is_cancelled()
        {
            tracing::warn!(error = %error, "harvest connector failed during shutdown");
        }
    }
}

impl Default for HarvestPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl HarvestPlugin {
    /// Create a plugin with no workflows, activities, dags, or API mount.
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: HarvestBuilder::default(),
            api_path: None,
            api_middleware: None,
            mcp_tool_middleware: None,
            mcp_tools_enabled: false,
            mcp_tools_prefix: None,
            decode_payloads_on_read: false,
            role_auth_enabled: false,
            api_tokens_enabled: false,
            status_thresholds: crate::status_summary::StatusThresholds::default(),
            canary_config: None,
            #[cfg(feature = "webhooks")]
            webhook_triggers: Vec::new(),
            #[cfg(feature = "connectors")]
            connectors: Vec::new(),
            #[cfg(feature = "metrics")]
            metrics_scrape_enabled: false,
        }
    }

    /// Register workflow definitions produced by `autumn_harvest::workflows!`.
    #[must_use]
    pub fn workflows(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        self.builder = self.builder.workflows(workflows);
        self
    }

    /// Register activity definitions produced by `autumn_harvest::activities!`.
    #[must_use]
    pub fn activities(mut self, activities: Vec<ActivityInfo>) -> Self {
        self.builder = self.builder.activities(activities);
        self
    }

    /// Register DAG definitions produced by `autumn_harvest::dags!`.
    #[must_use]
    pub fn dags(mut self, dags: Vec<DagInfo>) -> Self {
        self.builder = self.builder.dags(dags);
        self
    }

    /// Register declarative update handlers produced by `autumn_harvest::updates!`.
    #[must_use]
    pub fn updates(mut self, updates: Vec<autumn_harvest::UpdateHandlerInfo>) -> Self {
        self.builder = self.builder.updates(updates);
        self
    }

    /// Register declarative query handlers produced by `autumn_harvest::queries!`.
    #[must_use]
    pub fn queries(mut self, queries: Vec<autumn_harvest::QueryHandlerInfo>) -> Self {
        self.builder = self.builder.queries(queries);
        self
    }

    /// Register declarative signal handlers produced by `autumn_harvest::signals!`.
    #[must_use]
    pub fn signals(mut self, signals: Vec<autumn_harvest::SignalHandlerInfo>) -> Self {
        self.builder = self.builder.signals(signals);
        self
    }

    /// Register typed shared state visible to workflow and activity handlers.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.builder = self.builder.state(value);
        self
    }

    /// Configure the worker runtime.
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.builder = self.builder.worker(config);
        self
    }

    /// Configure the background retention job (history/audit pruning).
    #[must_use]
    pub fn retention(mut self, config: autumn_harvest::retention::RetentionConfig) -> Self {
        self.builder = self.builder.retention(config);
        self
    }

    /// How long a keyed workflow start's idempotency claim is retained
    /// (issue #808; default 24 h).
    ///
    /// Load-bearing for broker connectors (issue #944): a `BrokerCoordinates`
    /// binding dedupes through this claim, so the "one execution per message"
    /// guarantee lasts exactly as long as the window. Brokers replay much
    /// older than a day — a Kafka consumer-group reset or topic replay, an SQS
    /// message redelivered across many receives — and a redelivery arriving
    /// after the claim is purged reserves the key as *fresh*, starting a
    /// second execution. Size this to how far back your broker can replay;
    /// harvest warns at startup if a coordinate-dedupe binding is registered
    /// while this is left at its default.
    #[must_use]
    pub fn start_idempotency_window(mut self, window: std::time::Duration) -> Self {
        self.builder = self.builder.start_idempotency_window(window);
        self
    }

    /// Enable the **opt-in durable per-execution workflow log sink** (issue
    /// #790).
    ///
    /// With it, lines emitted through the existing
    /// `ctx.logger()` / `ctx.log_info` / `ctx.log_warn` / `ctx.log_error`
    /// entry points (issue #379) are additionally persisted per execution and
    /// readable in one call via `GET /api/harvest/workflows/{id}/logs`, the
    /// `harvest workflow logs` CLI command, or the Vantage execution-detail
    /// **Logs** panel — no external log-aggregation correlation.
    ///
    /// **Off by default, and additive.** Without this call `ctx.logger()`
    /// behaves exactly as before (`tracing`-only), and the workflow body does
    /// not change either way.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest_plugin::HarvestPlugin;
    /// # use autumn_harvest::WorkflowLogPolicy;
    /// // Defaults: 1,000 lines per execution, 4 KiB per line.
    /// let plugin = HarvestPlugin::new().workflow_log_persistence(WorkflowLogPolicy::new());
    /// ```
    ///
    /// See [`docs/workflow-logs.md`](https://github.com/autumn-foundation/autumn-harvest/blob/main/docs/workflow-logs.md)
    /// for the full contract.
    #[must_use]
    pub fn workflow_log_persistence(
        mut self,
        policy: autumn_harvest::context::WorkflowLogPolicy,
    ) -> Self {
        self.builder = self.builder.workflow_log_persistence(policy);
        self
    }

    /// Mount the Harvest management API under `path`.
    #[must_use]
    pub fn api(mut self, path: impl Into<String>) -> Self {
        self.api_path = Some(path.into());
        self
    }

    /// Mount the Harvest management API under `path`, protected by the given
    /// tower middleware layer.
    ///
    /// The same layer is also applied to every generated MCP tool route
    /// (issue #597 hardening) when [`Self::mcp_tools`] /
    /// [`Self::mcp_tools_at`] is used — those routes are registered via
    /// `AppBuilder::routes(...)`, not `nest()`, so without this they would
    /// never pass through this middleware and would be reachable
    /// unauthenticated at their own HTTP path even when the rest of the
    /// Harvest API requires a credential.
    #[must_use]
    pub fn api_with_auth<M>(mut self, path: impl Into<String>, middleware: M) -> Self
    where
        M: tower::Layer<autumn_web::reexports::axum::routing::Route>
            + Clone
            + Send
            + Sync
            + 'static,
        M::Service: tower::Service<autumn_web::reexports::axum::extract::Request>
            + Clone
            + Send
            + Sync
            + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Response:
            autumn_web::reexports::axum::response::IntoResponse + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Error:
            Into<std::convert::Infallible> + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Future:
            Send + 'static,
    {
        self.api_path = Some(path.into());
        let mcp_middleware = middleware.clone();
        self.mcp_tool_middleware = Some(std::sync::Arc::new(move |method_router| {
            method_router.layer(mcp_middleware.clone())
        }));
        self.api_middleware = Some(Box::new(move |router| router.layer(middleware)));
        self
    }

    /// Mount the Harvest management API under `path`, protected by the given
    /// tower middleware layer, **with the read-only operator role enabled**
    /// (issue #776).
    ///
    /// This behaves exactly like [`Self::api_with_auth`] — `middleware` is the
    /// embedder's authentication layer, applied to the whole management router
    /// (and to every generated MCP tool route) — and additionally installs a
    /// class-aware enforcement layer that gives a **least-privilege read-only
    /// tier** in one call.
    ///
    /// # The read-only principal contract
    ///
    /// The restriction applies **only** to a principal the embedder's
    /// `middleware` explicitly marks read-only on the autumn-web `Session`:
    /// either `role` set to one of `harvest_readonly` / `harvest_operator` /
    /// `operator` / `readonly` / `read_only`, or a truthy
    /// `is_harvest_readonly` / `is_harvest_operator` session value. Such a
    /// principal may reach every [`RouteClass::ReadOnly`](autumn_harvest::audit::RouteClass)
    /// route (100% of the read surface) but receives **403 Forbidden** on every
    /// mutating route. A principal carrying an admin marker (`role` ∈
    /// {`admin`,`harvest_admin`} or a truthy `is_harvest_admin`/`is_admin`), or
    /// a principal your middleware lets through with no marker at all, keeps
    /// full access — exactly as under [`Self::api_with_auth`] today.
    ///
    /// Enforcement is driven by
    /// [`autumn_harvest::audit::CLASSIFIED_ROUTES`] as the single source of
    /// truth and **fails closed**: a route with no classification entry is
    /// treated as a mutation and denied to read-only principals.
    ///
    /// Read-only principals get admin-level **reads** (payload decode, the SSE
    /// event stream) by design — this slice restricts *verbs* (read vs mutate),
    /// not *fields* or *rows*. The nested `/ui` sub-router is not classified, so
    /// read-only principals receive 403 on `/ui` (a documented follow-up).
    #[must_use]
    pub fn api_with_role_auth<M>(mut self, path: impl Into<String>, middleware: M) -> Self
    where
        M: tower::Layer<autumn_web::reexports::axum::routing::Route>
            + Clone
            + Send
            + Sync
            + 'static,
        M::Service: tower::Service<autumn_web::reexports::axum::extract::Request>
            + Clone
            + Send
            + Sync
            + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Response:
            autumn_web::reexports::axum::response::IntoResponse + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Error:
            Into<std::convert::Infallible> + 'static,
        <M::Service as tower::Service<autumn_web::reexports::axum::extract::Request>>::Future:
            Send + 'static,
    {
        self = self.api_with_auth(path, middleware);
        self.role_auth_enabled = true;
        self
    }

    /// Enable scoped API tokens for the management API (issue #942).
    ///
    /// Installs the token verification + scope-enforcement layer
    /// ([`crate::api_token::enforce_token_scope`]) on the nested
    /// `harvest_api_router`. A bearer that begins with `hvst_` is verified
    /// against the `harvest_api_tokens` table (rejecting an unknown/expired/
    /// revoked token 401, a `read`-scoped token attempting a mutating route
    /// 403), and its stable identity is threaded into the audit trail as
    /// `token:{id}`. When combined with `api_with_auth`, a bearer that does
    /// **not** begin with `hvst_` (or an absent bearer) is passed through to the
    /// embedder's boundary. Without that boundary, token-only mode requires a
    /// valid Harvest token for every non-OPTIONS management API request.
    ///
    /// Default off: an embedder that never calls this sees byte-for-byte
    /// unchanged behavior (no layer installed).
    #[must_use]
    pub const fn enable_api_tokens(mut self) -> Self {
        self.api_tokens_enabled = true;
        self
    }

    /// Expose every `#[workflow(mcp)]` workflow, and every `#[dag(mcp)]`
    /// unified DAG, as MCP tools (issue #597; DAG support added in issue #601
    /// follow-up).
    ///
    /// Generates typed `start_{wf}` / `{wf}_status` / `signal_{wf}` /
    /// `{wf}_watch` routes (plus one `{wf}_update_{name}` per
    /// `#[update(workflow = "…", mcp)]` handler) under `{api_path}/mcp`
    /// (default `/api/harvest/mcp`) and registers them as app-level routes so
    /// autumn-web's `AppBuilder::mount_mcp("/mcp")` projects them into the MCP
    /// tool catalog. The app author still mounts (and secures, via
    /// `secure_mcp`) the MCP endpoint itself.
    ///
    /// A `#[dag(mcp)]` DAG gets a reduced three-tool set instead --
    /// `start_{dag}` / `{dag}_status` / `{dag}_watch`, no `signal_{dag}` and
    /// no update tools (a DAG never consumes signals and has no update
    /// handlers) -- and its `start_{dag}` tool routes through the DAG trigger
    /// contract (admission gates, `max_active_runs`/paused enforcement)
    /// rather than the generic workflow-start path.
    ///
    /// Opt-in only: workflows and DAGs without the `mcp` attribute never
    /// surface, and the mutating tools are never part of autumn-web's
    /// read-only `expose_all_as_mcp` hatch.
    #[cfg(feature = "mcp")]
    #[must_use]
    pub const fn mcp_tools(mut self) -> Self {
        self.mcp_tools_enabled = true;
        self
    }

    /// Like [`Self::mcp_tools`], mounting the generated tool routes under an
    /// explicit prefix instead of `{api_path}/mcp`.
    #[cfg(feature = "mcp")]
    #[must_use]
    pub fn mcp_tools_at(mut self, prefix: impl Into<String>) -> Self {
        self.mcp_tools_enabled = true;
        self.mcp_tools_prefix = Some(prefix.into());
        self
    }

    /// Decode codec-encrypted payloads on the operator read path (issue #608).
    ///
    /// Explicit opt-in, default **off** — without this call, responses are
    /// byte-for-byte identical to today and no handler ever consults the
    /// codec registry. With it enabled, the management API read surfaces
    /// (`/result`, history export under `payload_policy=full`, describe,
    /// `/history`, `/stack`, `GET /dead-letters`, the SSE event stream) and
    /// the Vantage UI decode stored codec envelopes **only** for requests
    /// that pass the same harvest-admin predicate the `require_admin` layer
    /// uses — a non-admin caller on an ungated route still sees the stored
    /// bytes. Every request that actually decodes (or degrades) at least one
    /// envelope writes a best-effort `payload.decode_read` audit record, so
    /// plaintext reads are accountable. A per-field decode failure (bad key,
    /// rotated-away codec id) degrades to an `_harvest_undecodable` marker —
    /// never a 500. Stored rows are never mutated.
    ///
    /// Note: with [`Self::api_with_auth`], the embedder boundary makes every
    /// request that reaches the API an admin — decode then applies to all of
    /// them, which is exactly the boundary's contract.
    ///
    /// See `docs/operations/read-path-decode.md` for the full operator guide,
    /// including the provenance caveat (decoded values are not authenticated)
    /// and the deliberately-undecoded list surfaces.
    #[must_use]
    pub const fn decode_payloads_on_read(mut self) -> Self {
        self.decode_payloads_on_read = true;
        self
    }

    /// Override the thresholds for the rolled-up `GET /admin/status` verdict
    /// (issue #679).
    ///
    /// The defaults are **starter values, not universal SLOs**; tune DLQ depth,
    /// queue backlog, stalled-run, and worker-health thresholds per deployment.
    #[must_use]
    pub const fn with_status_thresholds(
        mut self,
        thresholds: crate::status_summary::StatusThresholds,
    ) -> Self {
        self.status_thresholds = thresholds;
        self
    }

    /// Enable the built-in synthetic liveness canary (issue #796).
    ///
    /// Registers a throwaway probe workflow + activity per configured queue and
    /// schedules them on `config`'s interval across every writable shard, so the
    /// live `start → dispatch → activity → durable-timer → complete` path is
    /// exercised continuously and a wedged pipeline surfaces within one probe
    /// interval — with zero operator-authored workflow code.
    ///
    /// Off by default: without this call the canary is entirely absent and the
    /// runtime is byte-for-byte unchanged (AC1). Distinct from the #512 replay
    /// canary — see [`crate::canary`].
    #[must_use]
    pub fn synthetic_canary(mut self, config: crate::canary::CanaryConfig) -> Self {
        self.canary_config = Some(config);
        self
    }

    /// Register inbound webhook triggers produced by `autumn_harvest::webhooks!`
    /// (issue #344).
    ///
    /// Each `#[webhook]`-annotated function is mounted as an app-level route
    /// at its declared `path`, verified by autumn-web's `[security.webhooks]`
    /// signed-webhook layer (`SignedWebhook`) before this crate's dispatch
    /// code ever runs. Call `.workflows(...)` with every trigger's target
    /// workflow *before* calling this method -- `HarvestPlugin::build` fails
    /// fast (panics) if a trigger targets an unregistered workflow, or if two
    /// triggers declare the same binding path.
    ///
    /// Accumulates across repeated calls (mirrors `.workflows`/`.activities`/
    /// `.queries`/`.updates`), so registering webhook bindings from separate
    /// modules via multiple `.webhooks(...)` calls keeps every prior
    /// registration instead of the last call silently discarding them.
    ///
    /// See `docs/getting-started/12-webhooks.md`.
    #[cfg(feature = "webhooks")]
    #[must_use]
    pub fn webhooks(
        mut self,
        triggers: Vec<autumn_harvest::webhook_trigger::WebhookTriggerInfo>,
    ) -> Self {
        self.webhook_triggers.extend(triggers);
        self
    }

    /// Expose the ADR-0001 §7 catalogue metrics on the app's shared
    /// `/actuator/prometheus` scrape endpoint (issue #355).
    ///
    /// Installs a `HarvestMetricsRecorder` as both the engine's
    /// `MetricsRecorder` (via `HarvestBuilder::telemetry`) and an
    /// `autumn_web::actuator::MetricsSource` registered under the name
    /// `"harvest"`. There is exactly **one** metrics scrape endpoint for the
    /// whole app — Harvest's samples appear alongside the app's own
    /// `autumn_http_*` families and any other plugin's `MetricsSource`, not on
    /// a second, Harvest-owned route. Auth posture is therefore governed
    /// entirely by the app's own `[actuator]` config (`actuator.prometheus`,
    /// profile-aware sensitive-endpoint gating) rather than a
    /// Harvest-specific toggle.
    ///
    /// Opt-in and additive: without this call, telemetry stays wired to the
    /// zero-cost `NoOpMetrics` default exactly as before. Calling it a second
    /// time is idempotent (autumn-web's `MetricsSourceRegistry` rejects the
    /// duplicate `"harvest"` registration with a `tracing::warn!` and keeps
    /// the first).
    ///
    /// This does **not** back the full starter alert pack
    /// (`docs/alerts/starter-pack-v0.1.0.json`) — it covers the metrics
    /// named in issue #355 plus a few more that the engine's own
    /// background samplers already compute for free once enabled (queue
    /// backlog age, worker slot occupancy, stranded-work demand), but not
    /// the wider catalogue (e.g. `harvest.workflow.terminal`,
    /// `harvest.activity.attempts`/`.retries`, `harvest.schedule.fire_attempts`).
    /// For OTLP export, a custom `MetricsRecorder`, the full metric surface,
    /// or a full bucketed histogram (this endpoint renders
    /// `harvest.workflow.duration` / `harvest.activity.duration` as
    /// `_count`/`_sum` counter pairs, since `MetricsSource`'s `MetricKind`
    /// has no histogram variant), use the `metrics-rs` adapter escape hatch
    /// documented in `docs/telemetry.md` instead.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub const fn with_metrics_scrape(mut self) -> Self {
        self.metrics_scrape_enabled = true;
        self
    }

    /// Bind a broker topic/queue to a workflow (issue #944).
    ///
    /// `binding` says *what* a message maps to; `source` is the adapter that
    /// feeds it ([`KafkaSource`][k], [`SqsSource`][s], [`MockSource`][m], or
    /// your own [`EventSource`][e]). A consumer loop is spawned per binding at
    /// startup and cancelled on shutdown.
    ///
    /// Call `.workflows(...)` with every binding's target workflow **before**
    /// this — `HarvestPlugin::build` fails fast (panics) on an unregistered
    /// target, a DAG target, a duplicate binding name, a missing mapping
    /// function, or a `BrokerCoordinates` dedupe mode explicitly requested on a
    /// workflow whose admission is deferred by a throttle/debounce/batch
    /// policy (the start route rejects that combination with `400`).
    ///
    /// Accumulates across repeated calls, mirroring `.workflows` / `.webhooks`.
    ///
    /// [k]: crate::connector::KafkaSource
    /// [s]: crate::connector::SqsSource
    /// [m]: crate::connector::MockSource
    /// [e]: crate::connector::EventSource
    ///
    /// See `docs/getting-started/13-broker-connectors.md`.
    #[cfg(feature = "connectors")]
    #[must_use]
    pub fn connector(
        self,
        binding: crate::connector::SourceBinding,
        source: std::sync::Arc<dyn crate::connector::EventSource>,
    ) -> Self {
        self.connector_with_config(binding, source, None)
    }

    /// [`Self::connector`] with explicit polling knobs.
    ///
    /// `None` uses [`ConnectorRuntimeConfig::default`][c], which is tuned for
    /// a cheap idle binding (a ≥ 1s poll so SQS *long*-polls rather than
    /// short-polls, an idle backoff, and a throttled consumer-lag sample).
    /// Override it to trade idle API cost against pickup latency, or to raise
    /// `max_batch` for a high-throughput topic.
    ///
    /// [c]: crate::connector::ConnectorRuntimeConfig
    #[cfg(feature = "connectors")]
    #[must_use]
    pub fn connector_with_config(
        mut self,
        binding: crate::connector::SourceBinding,
        source: std::sync::Arc<dyn crate::connector::EventSource>,
        config: Option<crate::connector::ConnectorRuntimeConfig>,
    ) -> Self {
        self.connectors.push(ConnectorRegistration {
            binding: std::sync::Arc::new(binding),
            source,
            config,
        });
        self
    }
}

/// Validate every registered binding, then spawn one consumer loop per binding.
///
/// Split out of `start_harvest_runtime` so the wiring stays readable, and so
/// the validation half is reachable from `build()` for fail-fast.
///
/// # Panics
///
/// Panics when a binding is invalid or when its adapter's `stream()` does not
/// match the binding's declared `stream` — both are startup misconfigurations
/// that would otherwise surface as a silently idle consumer.
/// What actually bounds a binding's dedupe guarantee, when that bound is left
/// somewhere the operator has probably not weighed (issue #944, Codex review).
///
/// A binding's dedupe rests on one of two records with *different lifetimes*,
/// so one knob cannot cover both.
#[cfg(feature = "connectors")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UntunedDedupeBound {
    /// A coordinate-keyed `Starts` binding reserves a
    /// `harvest_start_idempotency` row (#808), purged after
    /// `HarvestBuilder::start_idempotency_window` (default 24 h).
    StartIdempotencyWindow,
    /// The dedupe record lives on — or cascades from — the **execution row**,
    /// so retention deleting the run deletes the only evidence a redelivery was
    /// already handled. `start_idempotency_window` governs a claim table this
    /// path never touches, so it does nothing here. Two bindings land here:
    ///
    /// * `SignalsWithStart`, which persists its key *only* on
    ///   `harvest_signals` (`ON DELETE CASCADE` on its execution) and never
    ///   reserves a start-idempotency claim.
    /// * Any [`IdempotencyMode::WorkflowId`][m] binding, which passes no
    ///   idempotency key at all: dedupe rests entirely on the
    ///   `(workflow_name, workflow_id)` reuse-policy attach against the
    ///   execution row.
    ///
    /// [m]: crate::connector::IdempotencyMode::WorkflowId
    ExecutionRetention {
        /// The effective retention age that bounds the claim.
        max_age: std::time::Duration,
    },
}

/// Resolve which bound — if any — is worth naming at startup for one binding.
///
/// Exactly one shape rides the start-idempotency claim table: a
/// **coordinate-keyed `Starts`** binding. Everything else — `SignalsWithStart`,
/// and any `WorkflowId` binding, which passes no idempotency key at all — rests
/// on the execution row and is bounded by retention.
///
/// Returns `None` when the guarantee needs no caveat:
///
/// * A coordinate-keyed `Starts` binding with an explicitly configured window:
///   an explicit value is a considered one.
/// * An execution-row-bound binding with retention **off** (harvest's default):
///   the row — and with it the claim — is never deleted, so the dedupe is
///   *unbounded*, which is stronger than the claim-table case rather than
///   weaker.
///
/// An execution-row-bound binding under active retention always reports its
/// bound, deliberately: tuning `start_idempotency_window` must not silence it,
/// because that knob does not govern this path at all. Naming the wrong remedy
/// is the failure mode this exists to prevent.
///
/// Pure so the decision is unit-testable without capturing `tracing` output.
#[cfg(feature = "connectors")]
fn untuned_dedupe_bound(
    mode: crate::connector::IdempotencyMode,
    target: crate::connector::ConnectorTarget,
    configured_window: Option<std::time::Duration>,
    execution_retention_age: Option<std::time::Duration>,
) -> Option<UntunedDedupeBound> {
    match (mode, target) {
        (
            crate::connector::IdempotencyMode::BrokerCoordinates,
            crate::connector::ConnectorTarget::Starts { .. },
        ) => configured_window
            .is_none()
            .then_some(UntunedDedupeBound::StartIdempotencyWindow),
        _ => execution_retention_age
            .map(|max_age| UntunedDedupeBound::ExecutionRetention { max_age }),
    }
}

/// The first pair of registrations that share one event source, if any
/// (issue #944, Codex review).
///
/// Each registration gets its own [`ConnectorRuntime`][r] and therefore its own
/// `OffsetTracker`, so handing the same `Arc<dyn EventSource>` to two of them
/// starts two receive loops over **one client** with two independent views of
/// what is durable. On Kafka that is a message-loss hazard: the loops split the
/// stream nondeterministically, so one tracker can commit a contiguous mark
/// covering offsets the *other* loop is still dispatching, and a crash then
/// skips them permanently. On SQS it is a silent fan-out failure: each message
/// goes to exactly one of the two bindings, so neither target sees the whole
/// queue.
///
/// Callers pass source identities (`Arc::as_ptr` addresses); taking plain
/// integers keeps the decision unit-testable without constructing sources,
/// mirroring [`binding_stream_matches_adapter`].
///
/// [r]: crate::connector::ConnectorRuntime
#[cfg(feature = "connectors")]
fn first_shared_source(identities: &[usize]) -> Option<(usize, usize)> {
    for (i, id) in identities.iter().enumerate() {
        if let Some(j) = identities[i + 1..].iter().position(|other| other == id) {
            return Some((i, i + 1 + j));
        }
    }
    None
}

/// Index pair of the first two entries that declare the same identity.
///
/// The sibling of [`first_shared_source`] for a *declared* identity rather
/// than an object pointer: two separately constructed sources over one
/// physical subscription have distinct pointers, so the pointer check cannot
/// see them, but they still compete for the same messages.
///
/// A `None` entry declares no identity and never matches — not even another
/// `None`. An adapter that cannot name its subscription is not evidence of a
/// clash, and treating absence as a match would reject every pair of custom
/// adapters on sight.
#[cfg(feature = "connectors")]
fn first_shared_identity<T: PartialEq>(identities: &[Option<T>]) -> Option<(usize, usize)> {
    for (i, id) in identities.iter().enumerate() {
        let Some(id) = id else { continue };
        if let Some(j) = identities[i + 1..]
            .iter()
            .position(|other| other.as_ref().is_some_and(|o| o == id))
        {
            return Some((i, i + 1 + j));
        }
    }
    None
}

/// Index pair of the first two bindings that consume the same logical
/// `(stream, target)` pair (issue #944).
///
/// Not an error, unlike a shared *physical* subscription: two independent
/// brokers can expose the same stream name and legitimately feed one workflow
/// (active/active, or a cluster migration), and only the operator knows which
/// they meant. But on ONE broker it double-dispatches every message — each
/// binding derives its own idempotency key, because the binding name
/// namespaces it — so it is worth saying out loud once at startup.
///
/// Callers pass `(stream, workflow, signal_name)` triples; taking plain tuples
/// keeps the decision unit-testable without constructing bindings, mirroring
/// [`first_shared_source`].
#[cfg(feature = "connectors")]
fn first_duplicate_stream_target<'a>(
    pairs: &[(&'a str, &'a str, Option<&'a str>)],
) -> Option<(usize, usize)> {
    for (i, p) in pairs.iter().enumerate() {
        if let Some(j) = pairs[i + 1..].iter().position(|other| other == p) {
            return Some((i, i + 1 + j));
        }
    }
    None
}

/// Index registered workflows by name the way the engine itself resolves them:
/// **last-wins** (issue #944).
///
/// `HandlerRegistry` collapses the builder's `Vec<WorkflowInfo>` into a
/// `HashMap`, so a name registered twice executes under the *last* definition.
/// A `.find()` would be first-wins, which is not a cosmetic difference here:
/// the target's info decides the effective [`IdempotencyMode`][im], so a first
/// throttled definition followed by a plain one makes the runtime dedupe on
/// broker coordinates while a first-wins lookup describes a definition that
/// will never run — suppressing the 24-hour claim-lifetime warning and leaving
/// the operator unaware that a late replay can start a second execution.
///
/// One helper so every connector path that reads a target's info agrees by
/// construction rather than by comment.
///
/// [im]: crate::connector::IdempotencyMode
#[cfg(feature = "connectors")]
fn workflow_infos_by_name(workflows: &[WorkflowInfo]) -> HashMap<&str, &WorkflowInfo> {
    workflows.iter().map(|w| (w.name, w)).collect()
}

/// Whether a binding's declared stream matches the stream its adapter
/// actually consumes (issue #944).
///
/// A mismatch is silent and total: the consumer connects, polls forever, and
/// delivers nothing, with no error anywhere. Pure so the decision is
/// unit-testable without standing up a whole `Plugin::build`.
#[cfg(feature = "connectors")]
const fn binding_stream_matches_adapter(declared: &str, adapter: &str) -> bool {
    // `const fn` cannot use `==` on `&str`.
    let (a, b) = (declared.as_bytes(), adapter.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(feature = "connectors")]
fn spawn_connectors(
    registrations: &[ConnectorRegistration],
    api_state: &crate::api::HarvestApiState,
    metrics: &std::sync::Arc<dyn autumn_harvest::telemetry::MetricsRecorder>,
    workflows: &[WorkflowInfo],
    dead_letter_pool: &DbPool,
) -> Vec<ConnectorRuntimeHandle> {
    let mut handles = Vec::with_capacity(registrations.len());
    let sink: std::sync::Arc<dyn crate::connector::DeadLetterSink> = std::sync::Arc::new(
        crate::connector::PostgresDeadLetterSink::new(dead_letter_pool.clone()),
    );

    // Last-wins, matching `HandlerRegistry` and the validation pass -- see
    // `workflow_infos_by_name` for why first-wins is a correctness bug here.
    let by_name = workflow_infos_by_name(workflows);

    for registration in registrations {
        let binding = std::sync::Arc::clone(&registration.binding);
        let info = by_name.get(binding.target.workflow()).copied();
        let runtime = crate::connector::ConnectorRuntime::for_binding(
            std::sync::Arc::clone(&binding),
            std::sync::Arc::clone(&registration.source),
            api_state.clone(),
            std::sync::Arc::clone(metrics),
            info,
        )
        .with_dead_letter_sink(std::sync::Arc::clone(&sink));
        let runtime = match registration.config {
            Some(config) => runtime.with_config(config),
            None => runtime,
        };

        let shutdown = CancellationToken::new();
        let cancel = shutdown.child_token();
        let handle = tokio::spawn(async move { runtime.run(cancel).await });
        handles.push(ConnectorRuntimeHandle { shutdown, handle });
    }
    handles
}

/// Whether the generated MCP tool routes are about to be registered with no
/// route-level auth applied (issue #597, PR #908 hardening). Pure so the
/// decision is unit-testable without capturing `tracing` output.
#[cfg(feature = "mcp")]
const fn mcp_tools_unprotected(mcp_tools_enabled: bool, has_tool_middleware: bool) -> bool {
    mcp_tools_enabled && !has_tool_middleware
}

impl Plugin for HarvestPlugin {
    #[allow(clippy::too_many_lines)]
    fn build(self, app: AppBuilder) -> AppBuilder {
        let Self {
            builder,
            api_path,
            api_middleware,
            mcp_tool_middleware,
            mcp_tools_enabled,
            mcp_tools_prefix,
            decode_payloads_on_read,
            role_auth_enabled,
            api_tokens_enabled,
            status_thresholds,
            canary_config,
            #[cfg(feature = "webhooks")]
            webhook_triggers,
            #[cfg(feature = "connectors")]
            connectors,
            #[cfg(feature = "metrics")]
            metrics_scrape_enabled,
        } = self;
        #[cfg(not(feature = "mcp"))]
        let _ = (mcp_tool_middleware, mcp_tools_enabled, mcp_tools_prefix);

        // Autumn owns migrations for every set that lives in the application
        // database (autumn-web 0.7). See `register_plugin_migrations`.
        let app = register_plugin_migrations(app);

        let api_state = HarvestApiState::new();

        // Issue #355: one shared `HarvestMetricsRecorder` instance backs both
        // the engine-side `MetricsRecorder` (installed on `builder` before it
        // is stashed in the runtime slot -- `HarvestBuilder::telemetry(..)`
        // must be applied before `try_build()` runs inside
        // `start_harvest_runtime`) and the app-level `MetricsSource`
        // registration, so the shared `/actuator/prometheus` endpoint always
        // renders the exact samples the engine recorded. There is
        // deliberately no separate Harvest-owned scrape route.
        #[cfg(feature = "metrics")]
        let metrics_recorder =
            metrics_scrape_enabled.then(crate::metrics_scrape::HarvestMetricsRecorder::new);
        #[cfg(feature = "metrics")]
        let app = if let Some(recorder) = &metrics_recorder {
            app.metrics_source("harvest", Arc::new(recorder.clone()))
        } else {
            app
        };
        #[cfg(feature = "metrics")]
        let builder = if let Some(recorder) = metrics_recorder {
            builder.telemetry(
                autumn_harvest::telemetry::TelemetryConfig::builder()
                    .metrics(Arc::new(recorder))
                    .build(),
            )
        } else {
            builder
        };

        // Issue #344: generate the inbound webhook receiver routes before the
        // builder is stashed in the runtime slot (same ordering constraint as
        // the MCP tool routes below -- `builder.workflow_infos()` is only
        // available pre-build). Fails fast (panics) on a duplicate/malformed
        // binding path or a trigger targeting an unregistered workflow.
        #[cfg(feature = "webhooks")]
        let webhook_routes = if webhook_triggers.is_empty() {
            Vec::new()
        } else {
            crate::webhook_receiver::build_webhook_routes(
                &webhook_triggers,
                builder.workflow_infos(),
                builder.dag_infos(),
                &api_state,
            )
        };

        // Issue #944: validate broker bindings before the builder is stashed
        // in the runtime slot (`builder.workflow_infos()` is only available
        // pre-build), so a misconfigured binding is a startup panic rather
        // than a consumer that silently never dispatches anything.
        #[cfg(feature = "connectors")]
        if !connectors.is_empty() {
            let bindings: Vec<_> = connectors
                .iter()
                .map(|c| std::sync::Arc::clone(&c.binding))
                .collect();
            let borrowed: Vec<&crate::connector::SourceBinding> =
                bindings.iter().map(std::sync::Arc::as_ref).collect();
            let dag_names: Vec<String> = builder
                .dag_infos()
                .iter()
                .filter(|d| d.workflow_handler.is_some())
                .map(|d| d.name.to_string())
                .collect();
            // `validate_bindings` takes a slice of values; rebuild one from
            // the Arc'd registrations without cloning the mappers.
            if let Err(e) = crate::connector::validate_bindings_refs(
                &borrowed,
                builder.workflow_infos(),
                &dag_names,
            ) {
                panic!("harvest connector configuration error: {e}");
            }
            // One source per binding. Sharing one across registrations gives
            // each its own `OffsetTracker` over a single client, which loses
            // messages on Kafka and silently splits the queue on SQS.
            let identities: Vec<usize> = connectors
                .iter()
                .map(|r| std::sync::Arc::as_ptr(&r.source).cast::<()>() as usize)
                .collect();
            if let Some((i, j)) = first_shared_source(&identities) {
                panic!(
                    "harvest connector bindings '{}' and '{}' share one event source: each \
                     binding gets its own consumer loop and its own offset tracker, so two \
                     loops over one client would split the stream between them -- on Kafka \
                     one tracker can commit past offsets the other has not made durable \
                     (lost on a crash), and on SQS neither target would see every message. \
                     Give each binding its own source. To fan one stream out to two targets, \
                     use two Kafka consumers with distinct group ids, or two SQS queues \
                     behind an SNS topic",
                    connectors[i].binding.name, connectors[j].binding.name,
                );
            }
            // The same clash reached a different way: two *separately
            // constructed* sources over one physical subscription have
            // distinct pointers, so the identity check above cannot see them,
            // but they still compete for the same messages. An adapter that
            // declares no identity keeps the pointer check only.
            let subscriptions: Vec<Option<String>> = connectors
                .iter()
                .map(|r| r.source.subscription_identity())
                .collect();
            if let Some((i, j)) = first_shared_identity(&subscriptions) {
                panic!(
                    "harvest connector bindings '{}' and '{}' consume the same physical \
                     subscription ('{}'): both receive loops compete for the same messages, \
                     so each binding sees an arbitrary SUBSET rather than the whole stream -- \
                     on SQS every receiver on a queue competes, and on Kafka two consumers in \
                     one group split the partitions between them. To fan one stream out to \
                     two targets, use two Kafka consumers with DISTINCT group ids, or two SQS \
                     queues behind an SNS topic",
                    connectors[i].binding.name,
                    connectors[j].binding.name,
                    subscriptions[i].as_deref().unwrap_or("<unknown>"),
                );
            }
            // Two bindings on DISTINCT subscriptions consuming the same
            // logical stream into the same target is legitimate fan-in across
            // independent brokers, so it is not rejected -- but on one broker
            // it double-dispatches every message (each binding derives its own
            // idempotency key, since the name namespaces it), so say it once.
            // Reached only when the identity check above passed, i.e. the two
            // are genuinely different subscriptions.
            let pairs: Vec<(&str, &str, Option<&str>)> = connectors
                .iter()
                .map(|r| {
                    (
                        r.binding.stream.as_str(),
                        r.binding.target.workflow(),
                        r.binding.target.signal_name(),
                    )
                })
                .collect();
            if let Some((i, j)) = first_duplicate_stream_target(&pairs) {
                tracing::warn!(
                    first = connectors[i].binding.name,
                    second = connectors[j].binding.name,
                    stream = connectors[i].binding.stream,
                    workflow = connectors[i].binding.target.workflow(),
                    "two harvest connector bindings consume the same stream into the same \
                     target from different subscriptions: every message is dispatched TWICE \
                     (each binding namespaces its own idempotency key by binding name, so \
                     the two do not dedupe each other). Intentional for fan-in across \
                     independent brokers; otherwise consolidate them or retarget one"
                );
            }
            // Last-wins, so this warning describes the definition the engine
            // will actually execute -- see `workflow_infos_by_name`.
            let target_infos = workflow_infos_by_name(builder.workflow_infos());
            for registration in &connectors {
                assert!(
                    binding_stream_matches_adapter(
                        &registration.binding.stream,
                        registration.source.stream(),
                    ),
                    "harvest connector binding '{}' declares stream '{}' but its adapter \
                     consumes '{}': the two must match or the consumer would silently \
                     never deliver anything",
                    registration.binding.name,
                    registration.binding.stream,
                    registration.source.stream()
                );
                // Not fatal: a bounded dedupe guarantee is correct for most
                // deployments, and only the operator knows how far back their
                // broker can replay. But the bound is invisible otherwise, so
                // say it out loud once at startup -- naming the knob that
                // actually governs THIS binding's claim.
                match untuned_dedupe_bound(
                    crate::connector::resolve_idempotency_mode(
                        registration.binding.target,
                        registration.binding.idempotency_mode,
                        target_infos
                            .get(registration.binding.target.workflow())
                            .copied(),
                    ),
                    registration.binding.target,
                    builder.start_idempotency_window_config(),
                    builder
                        .retention_config()
                        .effective_max_age(registration.binding.target.workflow()),
                ) {
                    Some(UntunedDedupeBound::StartIdempotencyWindow) => {
                        tracing::warn!(
                            binding = registration.binding.name,
                            stream = registration.binding.stream,
                            "harvest connector dedupes on broker coordinates, but \
                             `start_idempotency_window` is the 24h default: a redelivery \
                             arriving after the claim is purged would start a SECOND \
                             execution. Size the window to how far back your broker can \
                             replay (Kafka retention / SQS message retention) via \
                             `HarvestBuilder::start_idempotency_window`"
                        );
                    }
                    Some(UntunedDedupeBound::ExecutionRetention { max_age }) => {
                        tracing::warn!(
                            binding = registration.binding.name,
                            stream = registration.binding.stream,
                            workflow = registration.binding.target.workflow(),
                            retention_secs = max_age.as_secs(),
                            "harvest connector binding's dedupe record lives with its \
                             EXECUTION ROW (a `signals_with_start` claim cascade-deletes \
                             with the run; a `workflow_id` binding passes no claim at all \
                             and dedupes on the reuse-policy attach): retention is the \
                             bound here, NOT `start_idempotency_window`, which this path \
                             never reserves. A redelivery arriving after the run is \
                             collected would start a SECOND execution. Size \
                             `RetentionConfig` for this workflow to at least how far back \
                             your broker can replay"
                        );
                    }
                    None => {}
                }
                assert!(
                    crate::connector::broker_native_dead_letter_is_supported(
                        registration.binding.dead_letter_mode,
                        registration.source.has_native_dead_letter(),
                    ),
                    "harvest connector binding '{}' asks for broker-native dead-lettering, but \
                     its adapter for stream '{}' reports no dead-letter destination that \
                     abandoning a message feeds, so a poison message would be re-read forever \
                     instead of being quarantined. Kafka has no per-message nack at all. SQS \
                     needs a redrive policy on the queue: add one, or (when the source was built \
                     with `SqsSource::new`, or its `GetQueueAttributes` probe was denied) declare \
                     it with `SqsSourceConfig::has_redrive_policy(true)`. Otherwise drop \
                     `.broker_native_dead_letter()` so poison messages land in \
                     `harvest_connector_dead_letters` instead",
                    registration.binding.name,
                    registration.binding.stream,
                );
            }
        }

        // Issue #597: generate the MCP tool routes before the builder is
        // stashed in the runtime slot. These are app-level typed routes
        // (registered via `AppBuilder::routes`, not `nest`) so autumn-web's
        // `mount_mcp` can project them into the tool catalog; handlers fail
        // closed until `on_startup` installs the runtime.
        #[cfg(feature = "mcp")]
        let mcp_routes = if mcp_tools_enabled {
            // Codex review (PR #908, P1): `HarvestPlugin` cannot detect or
            // intercept `AppBuilder::secure_mcp(...)` -- that's configured on
            // the outer app builder, after this `Plugin::build` call returns
            // -- so there is no way to *guarantee* these routes are
            // protected. `secure_mcp` only gates the `/mcp` JSON-RPC
            // envelope, not a generated tool route's own direct HTTP path
            // (these are registered via `AppBuilder::routes`, not `nest`).
            // Surface the gap loudly at startup instead of leaving it silent.
            if mcp_tools_unprotected(mcp_tools_enabled, mcp_tool_middleware.is_some()) {
                tracing::warn!(
                    "HarvestPlugin::mcp_tools() is enabled with no HarvestPlugin::api_with_auth(..) \
                     configured -- the generated start_/signal_/update_ tool routes are reachable \
                     UNAUTHENTICATED at their own HTTP path, even if the app also configures \
                     autumn-web's secure_mcp(...) (which only gates the /mcp JSON-RPC envelope, not \
                     these routes' direct paths). Configure HarvestPlugin::api_with_auth(path, mw) \
                     to protect the generated tool routes, or disregard this warning if \
                     unauthenticated access is intentional (e.g. local development)."
                );
            }
            let prefix =
                crate::mcp_tools::tools_prefix(api_path.as_deref(), mcp_tools_prefix.as_deref());
            // A unified DAG's shadow `WorkflowInfo` (auto-registered by
            // `.dags(...)`) lives in `builder.workflow_infos()` alongside
            // ordinary workflows; `collect_descriptors` cross-references
            // `builder.dag_infos()` to tell them apart, routing a DAG's start
            // tool through the DAG trigger contract and omitting its signal
            // tool (issue #601 follow-up).
            let descriptors = crate::mcp_tools::collect_descriptors(
                builder.workflow_infos(),
                builder.update_handlers(),
                builder.dag_infos(),
            );
            crate::mcp_tools::record_schemas(&descriptors);
            Some(crate::mcp_tools::build_mcp_tool_routes(
                &prefix,
                &descriptors,
                &api_state,
                mcp_tool_middleware.as_ref(),
                // Issue #776 (F1): gate mutating MCP tools for read-only
                // principals. These routes are app-level (outside the
                // `enforce_read_only_class` layer on the nested management
                // router), so the read-only class boundary is applied here too.
                role_auth_enabled,
            ))
        } else {
            None
        };

        // Issue #608: deployment-level opt-in for read-path payload decoding,
        // co-located with the codec-registry mirror so both are in place
        // before the HTTP server binds — no boot window where a
        // decode-eligible request sees a default identity-only registry
        // (`try_build` clones the registry verbatim, so this pre-build view
        // is identical to the post-build one). Mirroring is unconditional
        // and cheap; behavior is controlled by the flag + per-request admin
        // gate. Must run before `builder` is moved into the runtime slot.
        api_state.set_payload_codecs(builder.payload_codecs().clone());
        api_state.set_decode_payloads_on_read(decode_payloads_on_read);
        // Issue #679: mirror the rolled-up status thresholds into the API state.
        api_state.set_status_thresholds(status_thresholds);
        // Issue #796: mirror the synthetic liveness canary config so
        // `start_harvest_runtime` can register the probe workflow/activity/
        // schedule, and the (follow-up) `GET /admin/canary` read model can
        // resolve the interval/staleness window. `None` leaves the canary off.
        api_state.set_canary_config(canary_config);

        let slot = Arc::new(Mutex::new(HarvestRuntimeSlot {
            builder: Some(builder),
            runtime: None,
            #[cfg(feature = "connectors")]
            connectors,
        }));
        // issue #377: arm fail-closed so any request in the window between
        // HTTP server bind and the boot-time gate load is safely rejected.
        api_state.arm_gate_cache_fail_closed();
        api_state.set_admin_auth_boundary(api_middleware.is_some());

        let startup_slot = Arc::clone(&slot);
        let shutdown_slot = Arc::clone(&slot);
        let startup_api_state = api_state.clone();
        let shutdown_api_state = api_state.clone();

        let app = app
            .on_startup(move |state| {
                let slot = Arc::clone(&startup_slot);
                let api_state = startup_api_state.clone();
                async move {
                    tracing::info!("on_startup hook: executing start_harvest_runtime");
                    // `start_harvest_runtime` returns a large future (clippy::large_futures under
                    // the workspace pedantic lints); box it to keep this closure's future small.
                    let res = Box::pin(start_harvest_runtime(&state, &slot, &api_state)).await;
                    match &res {
                        Ok(()) => tracing::info!(
                            "on_startup hook: start_harvest_runtime completed successfully"
                        ),
                        Err(e) => tracing::error!(
                            "on_startup hook: start_harvest_runtime failed with error: {:?}",
                            e
                        ),
                    }
                    res
                }
            })
            .on_shutdown(move || {
                let slot = Arc::clone(&shutdown_slot);
                let api_state = shutdown_api_state.clone();
                async move {
                    stop_harvest_runtime(slot, api_state).await;
                }
            });

        #[cfg(feature = "mcp")]
        let app = match mcp_routes {
            Some(routes) => app.routes(routes),
            None => app,
        };

        #[cfg(feature = "webhooks")]
        let app = if webhook_routes.is_empty() {
            app
        } else {
            app.routes(webhook_routes)
        };

        if let Some(path) = api_path {
            let ui_router = harvest_ui_router(api_state.clone());
            // Clone the state for the token layer only when it will be installed,
            // so a disabled deployment does an identical amount of work as before.
            let token_layer_state = api_tokens_enabled.then(|| api_state.clone());
            let mut router = harvest_api_router(api_state).nest("/ui", ui_router);
            // Issue #776: install the class-aware read-only enforcement layer
            // BEFORE the embedder's auth middleware wraps the router, so the
            // request order is: embedder auth mw (sets Session) → this layer
            // (reads Session + method + nest-stripped path) → per-route
            // require_admin → handler. Applied to the combined router so it
            // also covers the nested /ui sub-router (which, being unclassified,
            // fails closed → 403 for read-only principals). Only installed
            // under api_with_role_auth; the default and api_with_auth paths are
            // byte-for-byte unchanged (AC6).
            if role_auth_enabled {
                router = router.layer(autumn_web::reexports::axum::middleware::from_fn(
                    crate::api::enforce_read_only_class,
                ));
            }
            // Issue #942: install the scoped-API-token verification + scope layer
            // OUTSIDE the read-only-class layer (so it runs first: verify token →
            // set TokenPrincipal / authoritative actor → deny read-scope mutation)
            // and INSIDE the embedder's auth middleware. Only installed under
            // enable_api_tokens(); the default path is byte-for-byte unchanged (AC7).
            if let Some(state) = token_layer_state {
                router = router.layer(autumn_web::reexports::axum::middleware::from_fn_with_state(
                    state,
                    crate::api_token::enforce_token_scope,
                ));
            }
            if let Some(mw) = api_middleware {
                router = mw(router);
            }
            app.nest(&path, router)
        } else {
            let _ = api_tokens_enabled;
            app
        }
    }
}

/// RAII guard (issue #618, F-round7) that clears the process-global admission
/// gate cache and metrics recorder if `start_harvest_runtime` returns early with
/// an error AFTER publishing them — so a failed startup never leaves the globals
/// pointing at a dead runtime's stale cache/recorder. Defused with `.commit()`
/// only once startup fully succeeds. Each clear is ptr-eq-guarded against THIS
/// runtime's `Arc`s (mirroring `stop_harvest_runtime`) so a concurrently-live
/// sibling runtime's globals are never clobbered.
struct AdmissionGlobalsGuard {
    gate_cache: Arc<autumn_harvest::admission_gate::AdmissionGateCache>,
    /// The global gate cache installed BEFORE this guard published its own (a
    /// concurrently-live sibling runtime's, or `None`). Restored — not cleared to
    /// `None` — on a drop-without-commit, so a failed second-runtime startup in the
    /// same process does not wipe a still-running sibling's enforcement (issue #618
    /// final pass).
    prev_gate_cache: Option<Arc<autumn_harvest::admission_gate::AdmissionGateCache>>,
    metrics: Option<Arc<dyn autumn_harvest::telemetry::MetricsRecorder>>,
    /// The global metrics recorder installed before `publish_metrics` ran (restored
    /// on a drop-without-commit, same rationale as `prev_gate_cache`).
    prev_metrics: Option<Arc<dyn autumn_harvest::telemetry::MetricsRecorder>>,
    committed: bool,
}

impl AdmissionGlobalsGuard {
    /// Publish the gate cache to the global static and begin guarding it.
    fn publish_gate_cache(cache: Arc<autumn_harvest::admission_gate::AdmissionGateCache>) -> Self {
        // Capture the previous global BEFORE overwriting it, so a failed startup
        // restores a live sibling's cache rather than clearing to None.
        let prev_gate_cache = autumn_harvest::admission_gate::global_admission_gate_cache();
        autumn_harvest::admission_gate::set_global_admission_gate_cache(Some(cache.clone()));
        Self {
            gate_cache: cache,
            prev_gate_cache,
            metrics: None,
            prev_metrics: None,
            committed: false,
        }
    }

    /// Publish the metrics recorder to the global static and begin guarding it.
    fn publish_metrics(&mut self, recorder: Arc<dyn autumn_harvest::telemetry::MetricsRecorder>) {
        self.prev_metrics = autumn_harvest::admission_gate::global_admission_metrics();
        autumn_harvest::admission_gate::set_global_admission_metrics(Some(recorder.clone()));
        self.metrics = Some(recorder);
    }

    /// Defuse the guard: startup succeeded, keep the published globals.
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for AdmissionGlobalsGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // RESTORE the previous global (a still-running sibling runtime's cache, or
        // `None` when there was no prior) instead of clearing to `None`: a failed
        // second-runtime startup in the same process must not wipe a live sibling's
        // enforcement (issue #618 final pass). Guard on ptr_eq so we only touch the
        // global while it is still OUR published value (a third party may have
        // replaced it — then leave it, as before).
        if let Some(installed) = autumn_harvest::admission_gate::global_admission_gate_cache()
            && Arc::ptr_eq(&installed, &self.gate_cache)
        {
            autumn_harvest::admission_gate::set_global_admission_gate_cache(
                self.prev_gate_cache.take(),
            );
        }
        if let Some(ref m) = self.metrics
            && let Some(installed) = autumn_harvest::admission_gate::global_admission_metrics()
            && Arc::ptr_eq(&installed, m)
        {
            autumn_harvest::admission_gate::set_global_admission_metrics(self.prev_metrics.take());
        }
    }
}

#[allow(clippy::too_many_lines, clippy::unused_async)]
async fn start_harvest_runtime(
    state: &AppState,
    slot: &Arc<Mutex<HarvestRuntimeSlot>>,
    api_state: &HarvestApiState,
) -> autumn_web::AutumnResult<()> {
    api_state.set_deployment_profile(state.profile().to_string());
    api_state.set_admin_auth_session_key(state.auth_session_key());
    let app_config = AutumnConfig::load()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    let harvest_config = HarvestRuntimeConfig::load()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    let workflow_result_notification_url = harvest_database_url(&app_config, &harvest_config)?;
    api_state.set_health_requires_shard_readiness(harvest_config.readiness.require_shard_readiness);
    api_state
        .set_workflow_result_notification_database_url(workflow_result_notification_url.clone());
    ensure_runtime_migrations(state.profile(), &app_config, &harvest_config).await?;

    let runtime_state = state.clone();
    let app_pool = state.pool().cloned();
    let harvest_pool = resolve_harvest_pool(state, &harvest_config)?;
    let router = ShardRouter::single();

    let (builder, runtime_already_started) = {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        (guard.builder.take(), guard.runtime.is_some())
    };
    #[cfg(feature = "connectors")]
    let connector_registrations = {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        std::mem::take(&mut guard.connectors)
    };

    if runtime_already_started {
        tracing::warn!("harvest runtime already started; skipping duplicate startup");
        return Ok(());
    }

    let Some(builder) = builder else {
        return Err(AutumnError::service_unavailable_msg(
            "harvest plugin builder was already consumed",
        ));
    };

    #[allow(unused_mut)]
    let mut builder = builder;
    #[cfg(feature = "webhooks")]
    {
        #[allow(unused_imports)]
        use crate::webhook::{
            __autumn_activity_info_deliver_webhook, __autumn_workflow_info_webhook_delivery,
            deliver_webhook, webhook_delivery,
        };
        builder = builder
            .workflows(autumn_harvest::prelude::workflows![webhook_delivery])
            .activities(autumn_harvest::prelude::activities![deliver_webhook]);

        if !builder
            .worker_config_mut()
            .queues
            .iter()
            .any(|q| q == "webhooks")
        {
            builder
                .worker_config_mut()
                .queues
                .push("webhooks".to_string());
        }
    }

    // Issue #796: register the built-in synthetic liveness canary when enabled.
    // `None` (the default) leaves the builder untouched, so a deployment that
    // never calls `synthetic_canary` is byte-for-byte identical (AC1).
    if let Some(cfg) = api_state.canary_config() {
        builder = crate::canary::register_canary(builder, &cfg);
    }

    let mut built = builder
        .try_build()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    // Issue #944: capture what the connector consumer loops need before
    // `built` is moved into the runner. `metrics` is Arc-identical to the
    // recorder the workers use, so connector samples land in the same sink.
    #[cfg(feature = "connectors")]
    let (built_metrics, built_workflow_infos) = (
        Arc::clone(&built.telemetry().metrics),
        built.workflow_infos().to_vec(),
    );

    // Derive the API stale threshold from the worker heartbeat interval so that
    // /workers correctly classifies workers under non-default configurations.
    api_state.set_worker_stale_threshold(built.worker_config().worker_heartbeat_interval * 2);
    // Mirror the configured shutdown timeout so drain requests can compute a
    // sensible default deadline without the caller having to supply one.
    api_state.set_worker_shutdown_timeout(built.worker_config().shutdown_timeout);
    // Propagate the per-query timeout from WorkerConfig (issue #234).
    api_state.set_query_timeout(built.worker_config().query_timeout);
    // Propagate the server-side execution timeout ceiling (issue #243).
    api_state.set_max_workflow_execution_timeout(built.max_workflow_execution_timeout);
    // Propagate the server-side chain-cap ceiling / fleet-wide default (issue #617).
    api_state.set_max_workflow_chain_timeout(built.max_workflow_chain_timeout);
    // Propagate the hard history event ceiling (issue #493).
    // Prefer the builder-level value; fall back to the WorkerConfig value so
    // that /admin/preflight accurately reflects the ceiling even when it was
    // configured via WorkerConfig::with_max_workflow_history_events rather
    // than HarvestBuilder::max_workflow_history_events.
    api_state.set_max_workflow_history_events(
        built
            .max_workflow_history_events
            .or_else(|| built.worker_config().max_workflow_history_events),
    );
    // Propagate the server-side start delay ceiling (issue #322).
    api_state.set_max_workflow_start_delay(built.worker_config().max_workflow_start_delay);
    // Propagate the default debounce max-wait cap (issue #499).
    api_state.set_default_debounce_max_wait(built.worker_config().default_debounce_max_wait);
    // Propagate the server-side workflow retry attempt ceiling (issue #523).
    api_state.set_max_workflow_attempts(built.max_workflow_attempts);
    // Propagate the request-scoped start-idempotency retention window (issue
    // #808): the HTTP start route reads it to dedup a repeated idempotency_key,
    // and the background expiry sweep reads the same value from a process-global
    // static (mirroring GLOBAL_CALLBACK_CONFIG) so it needs no extra param
    // threaded through enforce_timeouts_once.
    api_state.set_start_idempotency_window(built.start_idempotency_window);
    autumn_harvest::start_idempotency::set_purge_window_secs(built.start_idempotency_window);
    // Propagate the GET /admin/usage window ceiling (issue #596).
    api_state.set_usage_window_ceiling(built.usage_window_ceiling);
    // Propagate the GET /admin/usage group-count cap (issue #596).
    api_state.set_usage_max_groups(built.usage_max_groups);
    // Propagate batch start caps from builder config (issue #357).
    api_state.set_batch_start_config(&built.batch_start_config);
    // Propagate the completion-callback SSRF policy (issue #605) so the
    // HTTP start route validates a per-execution target against the same
    // allowlist the scanner re-validates at delivery time. The full runtime
    // config (deliverer/secret/defaults/retry policy) is installed by
    // `PreparedHarvestRuntime::build` inside `HarvestRunner::start` below,
    // which every `BuiltHarvest` consumer (this plugin and the standalone
    // runner) funnels through.
    api_state.set_completion_callback_ssrf_policy(built.completion_callback_config().ssrf_policy());
    // The read-path decode codec registry (issue #608) is mirrored in
    // `Plugin::build`, together with the opt-in flag, so it is in place
    // before the HTTP server binds — not here.

    // Apply the api_state audit retention override only when explicitly set,
    // so that builder-level retention config is not silently clobbered.
    if let Some(days) = api_state.audit_retention_days() {
        built.set_audit_retention_days(days);
    }

    // The resolved effective runtime configuration (issue #695) served by
    // `GET /admin/config` is captured inside `PreparedHarvestRuntime::build`
    // (in `HarvestRunner::start` below) and rides on the resulting
    // `HarvestApiRuntime`, so it is populated on any deployment that installs
    // that runtime — the plugin web-app path here and the standalone runner
    // alike — without a separate `set_effective_config` call to remember.

    state.insert_extension(harvest_config.outbox.clone());
    state.insert_extension(router.clone());

    // issue #618 (F1 re-review): populate the gate-cache snapshot with the
    // persisted active gates BEFORE the cache Arc is published to the global
    // static and BEFORE `HarvestRunner::start` spawns the worker poll loops and
    // the timeout scanner (which fire completion-trigger / debounce / throttle /
    // event-batch starts). Otherwise a scanner tick or a completion trigger
    // firing in the boot window would run `check_cached()` against an EMPTY
    // snapshot and PROCEED, bypassing persisted Fleet/queue/name gates uncounted
    // at startup. This runs on `harvest_pool` because it must complete before
    // that pool is moved into `runner_resources` on the next line. The runner's
    // own storage pool (`harvest_db_pool` below) wraps the very same underlying
    // pool, so the snapshot loaded here is the one workers see.
    //
    // Boot-load-FAILS case (a genuine gate-DB outage at boot): the snapshot
    // stays empty and `check_cached()` proceeds — the same documented bounded
    // caveat as a mid-run fail-closed with no cached match. We deliberately do
    // NOT block-drop on boot-load failure (that reintroduces the permanent-drop
    // fixed in F3); the direct HTTP start path still fails closed via `check()`.
    if let Ok(mut boot_conn) = acquire_conn(&harvest_pool).await {
        match autumn_harvest::admission_gate::db::load_active_gates(&mut boot_conn).await {
            Ok(gates) => {
                api_state.gate_cache().refresh(gates);
                tracing::debug!("admission gate cache populated at startup (before workers spawn)");
            }
            Err(e) => tracing::warn!(
                error = %e,
                "could not load admission gates at startup; cache stays empty until first refresh"
            ),
        }
    }

    // Issue #700 AC4 (P1 fix): capture a shard-0 pool handle for the boot-time
    // orphaned-workflow gate (run just below, before HarvestRunner::start
    // spawns workers) BEFORE `harvest_pool` is moved into the runner resources.
    // `DbPool` (a deadpool handle) is cheap to clone.
    let gate_pool = harvest_pool.clone();

    let mut runner_resources = HarvestRunnerResources::new(harvest_pool)
        .with_app_state(runtime_state.clone())
        .with_shard_router(router);
    if let Some(app_pool) = app_pool.as_ref() {
        runner_resources = runner_resources.with_app_pool(app_pool.clone());
    }
    let payload_codecs = built.payload_codecs().clone();
    let query_handlers = built.query_handlers().to_vec();
    let update_handlers = built.update_handlers().to_vec();
    let max_workflow_input_bytes = built.max_workflow_input_bytes;
    let max_workflow_execution_timeout = built.max_workflow_execution_timeout;
    let max_workflow_chain_timeout = built.max_workflow_chain_timeout;
    let max_workflow_attempts = built.max_workflow_attempts;
    let max_workflow_start_delay = built.max_workflow_start_delay;
    let max_signal_payload_bytes = built.max_signal_payload_bytes;
    let query_timeout = built.worker_config().query_timeout;
    let default_debounce_max_wait = built.worker_config().default_debounce_max_wait;
    // Issue #763 review (Codex finding): the transactional-start idempotency
    // dedup window must honor the operator's configured
    // `HarvestBuilder::start_idempotency_window`, the same value the HTTP
    // start route and the purge scanner already use (see `api_state.set_start_idempotency_window`
    // and `start_idempotency::set_purge_window_secs` above) -- not a hardcoded
    // default.
    let start_idempotency_window = built.start_idempotency_window;
    // Pre-flight: reject a multi-shard WorkerConfig before spawning any background
    // tasks.  The plugin configures WorkflowHandleClient with only shard-0's
    // LISTEN/NOTIFY URL; workflows hashed to non-zero shards would fail
    // wait/result/SSE paths.  Checking here avoids spawning worker/scheduler/batch
    // tasks that would need to be immediately stopped on error.
    if let Some(sp) = built.worker_config().sharded_pool.as_ref() {
        let shard_count = sp.iter_shards().count();
        if shard_count > 1 {
            return Err(AutumnError::service_unavailable_msg(format!(
                "HarvestPlugin does not support multi-shard deployments: the configured \
                 sharded pool spans {shard_count} shards but only shard-0's LISTEN/NOTIFY \
                 notification URL is available; workflows hashed to non-zero shards would \
                 fail wait/result/SSE paths. Use HarvestRunner::start with \
                 HarvestRunnerResources::with_sharded_pool and a WorkflowHandleClient \
                 configured with per-shard notification URLs instead."
            )));
        }
    }

    // Issue #700 AC4 (P1 fix): boot-time orphaned-workflow-type reachability gate.
    // Run it HERE — BEFORE `HarvestRunner::start` spawns the worker poll loop and
    // the scanners — not after. Under `orphaned_workflows = fail` + `worker_enabled`,
    // a worker spawned by `HarvestRunner::start` could CLAIM and terminally FAIL an
    // orphaned-type execution (no registered handler -> WorkflowFailed + FAILED
    // state) during the boot window before an after-the-fact abort tore the runner
    // down — defeating the gate's protective purpose. Placing the gate before any
    // task can be claimed closes that window; on Abort we `return Err` with nothing
    // to tear down (no runner / gate-refresh / outbox spawned yet, no admission
    // globals published).
    //
    // Both gate inputs are available before the runner starts: `registered` is
    // derived directly from the owned `built` (workflow names UNION unified-DAG
    // names, replicating `build_workflow_reachability_report`'s registry-key +
    // `registered_dag_names` union EXACTLY — `registry.workflows` does NOT hold
    // unified-DAG names, so the DAG union is load-bearing), and `gate_pool` is the
    // shard-0 pool clone captured above. The plugin is single-shard-only (multi-shard
    // configs were rejected just above), so a single-shard report reads the identical
    // rows the started-runtime cross-shard fan-out would. `off` skips the check;
    // default `warn` never blocks boot. Crash-loop safety: a DB read failure is
    // encoded as `status == Unavailable`, and `startup_orphan_decision(Fail, .,
    // Unavailable) == Warn` (never Abort).
    {
        use crate::config::OrphanStartupAction;
        use crate::workflow_reachability::{
            ReachabilityVerdict, StartupOrphanDecision, build_reachability_report_single_shard,
            startup_orphan_decision,
        };

        let orphan_action = harvest_config.startup.orphaned_workflows;
        if orphan_action != OrphanStartupAction::Off {
            let registered: std::collections::BTreeSet<String> = built
                .workflow_infos()
                .iter()
                .map(|info| info.name.to_string())
                .chain(
                    built
                        .dags()
                        .iter()
                        .filter(|dag| dag.workflow_handler.is_some())
                        .map(|dag| dag.name.to_string()),
                )
                .collect();

            // Select the SAME shard-0 storage pool `HarvestRunner::start` will
            // run the workers against (issue #700 P2). `PreparedHarvestRuntime::build`
            // gives `WorkerConfig::with_sharded_pool` PRECEDENCE over
            // `harvest_pool`, so a `HarvestBuilder::with_sharded_pool` deployment
            // would otherwise have the gate query `harvest_pool` while the
            // workers poll a DIFFERENT database — the gate could validate the
            // wrong DB, miss an orphan, and the worker would then start
            // polling+failing it. `select_runtime_shard0_pool` shares the exact
            // precedence `build` uses (the runner-resources override, which the
            // plugin never sets; WorkerConfig's sharded pool; the harvest_pool
            // clone) via `pick_runtime_pool_source`, so gate and runner agree by
            // construction. Critically it is READ-ONLY (issue #700 P4): it reads
            // shard 0 from an already-constructed `ShardedDbPool`, or returns the
            // `harvest_pool` handle directly, and never calls
            // `ShardedDbPool::single`/`from_map` — so an aborting gate installs
            // no `GLOBAL_SHARDED_POOL` and stays side-effect-free (the real
            // global install happens later in `HarvestRunner::start`, unchanged).
            let gate_shard0_pool = crate::runner::select_runtime_shard0_pool(
                runner_resources.sharded_pool_override(),
                built.worker_config().sharded_pool.as_ref(),
                &gate_pool,
            );
            let report =
                build_reachability_report_single_shard(&registered, &gate_shard0_pool, None).await;
            let orphaned_types: Vec<&str> = report
                .items
                .iter()
                .filter(|item| item.verdict == ReachabilityVerdict::Orphaned)
                .map(|item| item.workflow_type.as_str())
                .collect();
            match startup_orphan_decision(orphan_action, report.orphaned, report.status) {
                StartupOrphanDecision::Continue => {}
                StartupOrphanDecision::Warn => {
                    tracing::warn!(
                        orphaned_types = ?orphaned_types,
                        total_orphaned_executions = report.total_orphaned_executions,
                        status = ?report.status,
                        "orphaned workflow types detected at startup: their #[workflow] \
                         handlers are no longer registered and in-flight runs would wedge \
                         on replay. See docs/runbooks/safe-deploy.md \
                         (Pre-cutover handler-coverage gate) and safe-handler-removal.md."
                    );
                }
                StartupOrphanDecision::Abort => {
                    tracing::error!(
                        orphaned_types = ?orphaned_types,
                        total_orphaned_executions = report.total_orphaned_executions,
                        "refusing startup (harvest.startup.orphaned_workflows = fail): \
                         orphaned workflow types have in-flight runs but no registered \
                         handler, so those runs would wedge on replay. The gate runs \
                         before workers spawn, so no run was claimed or failed."
                    );
                    return Err(AutumnError::service_unavailable_msg(format!(
                        "refusing startup: {} orphaned workflow type(s) with in-flight \
                         runs have no registered handler ({} stranded executions): {:?}",
                        orphaned_types.len(),
                        report.total_orphaned_executions,
                        orphaned_types,
                    )));
                }
            }
        }
    }

    // issue #618 (F11 + F1 re-review): publish the SAME gate-cache Arc the
    // management API uses into the process-global static BEFORE
    // HarvestRunner::start spawns the worker poll loops and the timeout scanner
    // (which fire completion-trigger / debounce / throttle / event-batch starts).
    // The snapshot was already populated with persisted gates just above (before
    // `harvest_pool` was moved), so the Arc published here — and mutated in place
    // by the background refresh loop — already carries the boot-time gates. A
    // single publish is sufficient; no re-publish per refresh.
    // issue #618 (F-round7): publish via an RAII guard so any early-return error
    // AFTER this point clears the globals rather than leaving them pointing at a
    // failed startup's stale cache/recorder. Defused with `.commit()` on success.
    let mut admission_guard = AdmissionGlobalsGuard::publish_gate_cache(api_state.gate_cache());
    // issue #618 (F-round5, F-round14): publish the SAME metrics recorder the
    // workers/scanners use so the completion-trigger block path counts a block even
    // when its caller passes `metrics: None` (the cancel / terminate /
    // parent-close-cascade paths, which rely on the process-global recorder). Publish
    // it BEFORE `HarvestRunner::start` spawns the worker poll loops and the timeout
    // scanner — symmetric with the gate cache above (F-round14). Otherwise, in the
    // boot window between the runner spawning those background tasks and a later
    // metrics publish, a completion-trigger block on one of those paths would find
    // `global_admission_metrics() == None` and silently drop the
    // `harvest.admission.blocked` count. `built.telemetry().metrics` is the very same
    // `Arc<dyn MetricsRecorder>` the registry hands the workers — both come from
    // `built.telemetry` via `into_worker_parts_with_extra_state` — so this counts
    // through the identical sink the runner-derived accessor would have, while
    // `built` is still owned here (consumed by `HarvestRunner::start` on the next
    // line). The RAII guard keeps clearing BOTH globals on any early-return error and
    // keeping both only on `.commit()`.
    admission_guard.publish_metrics(std::sync::Arc::clone(&built.telemetry().metrics));

    let runner = HarvestRunner::start(built, &harvest_config, runner_resources).await?;
    let harvest_db_pool = runner.storage_pool();
    // Defense-in-depth: the pre-flight above catches WorkerConfig::with_sharded_pool;
    // this catches any future path that sets runner_resources.sharded_pool.
    // runner.stop().await cancels the spawned tasks before propagating the error so
    // the process is not left with orphaned background tasks.
    let shard_count = harvest_db_pool.iter_shards().count();
    if shard_count > 1 {
        runner.stop().await;
        return Err(AutumnError::service_unavailable_msg(format!(
            "HarvestPlugin does not support multi-shard deployments: the resolved pool spans \
             {shard_count} shards but only shard-0's LISTEN/NOTIFY notification URL is \
             configured; workflows hashed to non-zero shards would fail wait/result/SSE paths. \
             Use HarvestRunner::start with HarvestRunnerResources::with_sharded_pool and a \
             WorkflowHandleClient with per-shard notification URLs instead."
        )));
    }
    let workflow_handle_client = WorkflowHandleClient::new(
        harvest_db_pool.sharded_pool().clone(),
        runner.api_runtime().router().clone(),
        [(
            harvest_db_pool.sharded_pool().default_shard(),
            workflow_result_notification_url,
        )],
    )
    .with_codecs(payload_codecs)
    .with_shared_state(runner.api_runtime().registry().shared_state())
    .with_handlers(query_handlers, update_handlers)
    // Issue #763: registered `WorkflowInfo`s so the in-process transactional
    // start API can resolve defaults (execution_timeout, sla, concurrency key)
    // and published input-schema validation, the same way the HTTP start
    // route does.
    //
    // Issue #763 review ("Exclude registered DAGs from transactional workflow
    // starts"): unified DAGs are filtered out by `transactional_client_workflows`
    // below — see its doc comment for the full rationale.
    .with_workflows(transactional_client_workflows(
        runner.api_runtime().registry().workflows.values(),
        runner.api_runtime().registered_dag_names(),
    ))
    // Issue #763 review ("Carry the default queue on the handle client"): the
    // SAME runtime-carried queue list `POST /workflows/{name}/start` resolves
    // its own default queue from, so a start with no explicit
    // `TransactionalStartOptions::queue_name` lands on a queue this fleet's
    // workers actually poll instead of the (possibly unset or stale)
    // process-global default.
    .with_queues(runner.api_runtime().queues().iter().map(String::as_str))
    .with_max_workflow_input_bytes(max_workflow_input_bytes)
    .with_max_workflow_execution_timeout(max_workflow_execution_timeout)
    .with_max_workflow_chain_timeout(max_workflow_chain_timeout)
    .with_max_workflow_start_delay(max_workflow_start_delay)
    .with_max_signal_payload_bytes(max_signal_payload_bytes)
    .with_query_timeout(query_timeout)
    .with_history_policy(runner.api_runtime().registry().history_policy())
    .with_default_debounce_max_wait(default_debounce_max_wait)
    .with_max_workflow_attempts(max_workflow_attempts)
    // Issue #763 review: use the operator-configured idempotency window for
    // transactional-start dedup, matching the HTTP start route.
    .with_start_idempotency_window(start_idempotency_window)
    // Issue #684: wire the engine recorder so the in-process typed-update path
    // emits harvest.update.admitted, matching the HTTP/UI admission paths.
    .with_metrics(runner.api_runtime().registry().telemetry().metrics.clone());
    state.insert_extension(harvest_db_pool.clone());
    state.insert_extension(runner.api_runtime().registry().clone());

    #[cfg(feature = "webhooks")]
    let client = workflow_handle_client.clone();
    state.insert_extension(workflow_handle_client);

    #[cfg(feature = "webhooks")]
    {
        tracing::info!("HarvestPlugin: inserting WebhookDelegateExt into AppState extensions");
        let delegate = std::sync::Arc::new(
            move |state: &AppState,
                  sub: autumn_web::webhook_outbound::WebhookSubscription,
                  log: autumn_web::webhook_outbound::WebhookDeliveryLog| {
                let client = client.clone();
                let harvest_db = state.extension::<crate::state::HarvestDbPool>();

                let registry =
                    state.extension::<std::sync::Arc<autumn_harvest::worker::HandlerRegistry>>();
                // Metrics recorder for the admission-gate block counter (issue #618,
                // Finding A). Best-effort: absent only in a degenerate boot window
                // where the registry extension isn't installed yet.
                let metrics = registry
                    .as_ref()
                    .map(|r| std::sync::Arc::clone(&r.telemetry().metrics));
                let (owner, runbook_url, severity, info_sla, info_retry_policy) = registry
                    .and_then(|registry| {
                        registry.workflows.get("webhook_delivery").map(|wf| {
                            (
                                wf.owner,
                                wf.runbook_url,
                                wf.severity,
                                wf.sla,
                                wf.retry_policy.clone(),
                            )
                        })
                    })
                    .unwrap_or((None, None, None, None, None));
                let sla = info_sla.and_then(|d| autumn_harvest::chrono::Duration::from_std(d).ok());
                let webhook_retry_policy = info_retry_policy;

                Box::pin(async move {
                    let workflow_id = format!("webhook-delivery-{}", log.id);
                    let shard =
                        client.pick_shard_for_new_workflow("webhook_delivery", &workflow_id);
                    let exec_id = autumn_harvest::types::ExecutionId::new_for_shard(shard);

                    // Acquire the target-shard connection first: the admission
                    // decision below needs a read-only existence check, and
                    // `start_or_load` reuses the same connection.
                    let Some(harvest_db) = harvest_db else {
                        return Err(autumn_web::error::AutumnError::internal_server_error_msg(
                            "HarvestDbPool not found on AppState extensions",
                        ));
                    };
                    let pool = harvest_db.pool_for(shard).clone();
                    let mut conn = pool.get().await.map_err(|e| {
                        autumn_web::error::AutumnError::internal_server_error_msg(e.to_string())
                    })?;

                    // Issue #618, Finding A (round 9 → round 12): the OUTBOUND
                    // webhook-delivery producer is gated as a FRESH in-process start —
                    // but an idempotent RETRY for the same webhook log (deterministic
                    // workflow id `webhook-delivery-{log.id}`, `AllowDuplicate` reuse)
                    // does NOT admit a fresh run: `start_or_load` just LOADS the existing
                    // execution, so gating that retry would wrongly block a non-admission
                    // and inflate the block counter.
                    //
                    // Round 9 decided this with an UNLOCKED `try_load_by_key`, which had a
                    // TOCTOU: if the existing run sealed (CONTINUED_AS_NEW / TERMINATED)
                    // between the read-only check and `start_or_load`, the core start path
                    // (default `AllowDuplicate`) admitted a FRESH replacement — and because
                    // the unlocked check had already decided to skip the gate, that fresh
                    // start slipped an active gate uncounted. Round 12 closes the window by
                    // making the gate-skip decision CONSISTENT with `start_or_load`'s own
                    // create-vs-attach decision: `gate_checked_start_or_load` locks the
                    // existing active run (`FOR UPDATE`) so it cannot seal underneath us,
                    // decides the gate on that LOCKED state, and runs `start_or_load` in the
                    // SAME transaction — so a live run is attached (gate skipped) and a
                    // sealed/absent run's fresh create is gated (matching gate → block+count;
                    // fail-closed sentinel → block a fresh start, per round 5).
                    let start_params = autumn_harvest::execution::StartWorkflowParams {
                        workflow_name: "webhook_delivery",
                        workflow_id: &workflow_id,
                        exec_id,
                        input: serde_json::json!({
                            "subscription_id": sub.id,
                            "topic": log.topic,
                            "payload": log.payload,
                        }),
                        parent_id: None,
                        queue_name: "webhooks",
                        execution_timeout: None,
                        memo: None,
                        search_attrs: None,
                        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
                        conflict_policy:
                            autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        chain_execution_timeout: None,
                        max_workflow_chain_timeout_ceiling: None,
                        inherited_chain_deadline_at: None,
                        concurrency_key: None,
                        concurrency_limit: None,
                        concurrency_on_conflict:
                            autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
                        priority: autumn_harvest::prelude::Priority::default(),
                        max_workflow_input_bytes,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner,
                        runbook_url,
                        severity,
                        context_headers: None,
                        sla,
                        schedule_id: None,
                        scheduled_for: None,
                        workflow_attempt: 1,
                        workflow_retry_policy: webhook_retry_policy,
                        retry_of_exec_id: None,
                        max_workflow_attempts_ceiling: client.max_workflow_attempts(),
                        origin: None,
                        completion_callbacks: None,
                        start_source: autumn_harvest::StartSource::Api,
                        start_source_ref: None,
                        started_by: None,
                    };

                    // The metrics recorder (`Arc<dyn MetricsRecorder>`) coerced to the
                    // `+ Send + Sync` reference the core start path expects (the trait
                    // requires both supertraits), mirroring how the outbox relay passes it.
                    let metrics_ref = metrics.as_ref().map(|m| {
                        m.as_ref()
                            as &(dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync)
                    });

                    // Issue #618, PR #1014: the gate is now enforced AUTHORITATIVELY
                    // inside the core start primitive. `Some(GateMode::CheckCached)`
                    // makes `_collect` apply the gate iff it will CREATE a new
                    // execution — an idempotent `AllowDuplicate` retry that attaches
                    // to (or reloads) an existing run admits nothing and is never
                    // gated, while a genuinely fresh admission (incl. a seal-race
                    // replacement, now caught under the primitive's `FOR UPDATE` lock)
                    // is blocked + counted by the primitive itself. `CheckCached`
                    // (not the fail-closed `Check`) is used because the outbound
                    // webhook delivery is in-flight continuation of already-committed
                    // work and must not be permanently dropped by a boot/DB blip it
                    // cannot retry past.
                    match autumn_harvest::execution::start_or_load_workflow_execution_with_metrics(
                        &mut conn,
                        start_params,
                        metrics_ref,
                        Some(autumn_harvest::admission_gate::GateMode::CheckCached),
                    )
                    .await
                    {
                        Ok(_started) => Ok(()),
                        Err(autumn_harvest::HarvestError::AdmissionBlocked { reason, .. }) => {
                            // The primitive already recorded the block count.
                            tracing::info!(
                                reason = %reason,
                                "admission gate active; blocking outbound webhook_delivery start"
                            );
                            Err(autumn_web::error::AutumnError::internal_server_error_msg(
                                format!(
                                    "admission gate active; webhook_delivery start blocked ({reason})"
                                ),
                            ))
                        }
                        Err(e) => Err(autumn_web::error::AutumnError::internal_server_error_msg(
                            format!("failed to start Harvest webhook workflow: {e}"),
                        )),
                    }
                })
                    as std::pin::Pin<
                        Box<dyn std::future::Future<Output = autumn_web::AutumnResult<()>> + Send>,
                    >
            },
        );
        state.insert_extension(autumn_web::webhook_outbound::WebhookDelegateExt(delegate));
    }

    api_state.install_storage_pool(harvest_db_pool.clone());

    // issue #618 (F11 + F1 re-review): the process-global gate cache was
    // published above with its snapshot already populated from persisted gates,
    // BEFORE HarvestRunner::start spawned the workers/scanners — so a
    // completion trigger firing in the boot window sees the real gates rather
    // than an empty snapshot.

    // issue #377: spawn background gate-cache refresh (≤2 s p95 cross-replica propagation).
    let gate_refresh = {
        let cache = api_state.gate_cache();
        let api_state_for_metrics = api_state.clone();
        let pool = harvest_db_pool.clone_inner();
        let shutdown = CancellationToken::new();
        let cancel_for_task = shutdown.child_token();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel_for_task.cancelled() => return,
                    () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
                // Fail-closed on any error: if the gate table is
                // unreadable the cache transitions to uninitialized so
                // check() blocks new starts rather than silently admitting
                // them with a stale open snapshot.
                match acquire_conn(&pool).await {
                    Ok(mut conn) => {
                        match autumn_harvest::admission_gate::db::load_active_gates(&mut conn).await
                        {
                            Ok(gates) => {
                                let count = i64::try_from(gates.len()).unwrap_or(0);
                                cache.refresh(gates);
                                if let Ok(rt) = api_state_for_metrics.runtime() {
                                    rt.registry()
                                        .telemetry()
                                        .metrics
                                        .record_admission_gates_active(count);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "admission gate refresh failed; entering fail-closed mode"
                                );
                                cache.set_fail_closed();
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "admission gate refresh: could not acquire DB connection; \
                             entering fail-closed mode"
                        );
                        cache.set_fail_closed();
                    }
                }
            }
        });
        Some(GateRefreshRuntime { shutdown, handle })
    };

    let outbox = app_pool.as_ref().and_then(|_| {
        if harvest_config.outbox.enabled {
            let shutdown = CancellationToken::new();
            let handle =
                spawn_workflow_start_outbox_relay(runtime_state.clone(), shutdown.child_token());
            Some(OutboxRuntime { shutdown, handle })
        } else {
            None
        }
    });
    api_state.install(runner.api_runtime());

    // The boot-time orphaned-workflow-type reachability gate (issue #700 AC4)
    // runs earlier — BEFORE `HarvestRunner::start` spawns any worker — so a
    // `fail` action cannot let a worker claim and terminally fail an orphaned
    // run before the abort. See the gate block above the runner start.

    // Issue #944: spawn one consumer loop per registered broker binding, after
    // `api_state.install(...)` above so the very first message a connector
    // pulls can already reach the dispatch path.
    #[cfg(feature = "connectors")]
    let connectors = if connector_registrations.is_empty() {
        Vec::new()
    } else {
        spawn_connectors(
            &connector_registrations,
            api_state,
            &built_metrics,
            &built_workflow_infos,
            &harvest_db_pool.clone_inner(),
        )
    };

    {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        guard.runtime = Some(HarvestRuntime {
            runner,
            outbox,
            gate_refresh,
            #[cfg(feature = "connectors")]
            connectors,
        });
    }

    // Startup succeeded — keep the published admission globals (issue #618,
    // F-round7); `stop_harvest_runtime` clears them (ptr-eq guarded) on shutdown.
    admission_guard.commit();
    Ok(())
}

/// Workflows to expose on the in-process transactional-start client (issue
/// #763), with unified DAGs excluded (issue #763 review, "Exclude registered
/// DAGs from transactional workflow starts").
///
/// A unified DAG's shadow `WorkflowInfo` (issue #256) lives in
/// `registry.workflows` right alongside ordinary workflows, but
/// `POST /workflows/{name}/start` rejects a DAG name outright and points
/// callers at `POST /dags/{name}/trigger` instead — the only path that runs
/// `trigger_unified_dag`'s pause / `max_active_runs` admission checks and
/// schedule bookkeeping. Passing every registry workflow straight through
/// would let `start_workflow_transactional` start a paused or
/// capacity-saturated DAG as an ordinary workflow execution, silently
/// bypassing those checks. Filtering here — against the exact same
/// `registered_dag_names()` set `HarvestApiRuntime::is_registered_dag` checks
/// against for the HTTP route — means the transactional client never
/// registers a DAG name at all; a caller naming one is rejected the same way
/// any other unregistered workflow name is. Extracted as a pure function
/// (rather than an inline closure in the builder chain) so the exclusion is
/// directly unit-testable without booting a plugin/database.
fn transactional_client_workflows<'a>(
    workflows: impl Iterator<Item = &'a WorkflowInfo>,
    registered_dag_names: &HashSet<String>,
) -> Vec<WorkflowInfo> {
    workflows
        .filter(|info| !registered_dag_names.contains(info.name))
        .cloned()
        .collect()
}

fn resolve_harvest_pool(
    state: &AppState,
    config: &HarvestRuntimeConfig,
) -> autumn_web::AutumnResult<DbPool> {
    match config.mode {
        HarvestMode::Embedded => state.pool().cloned().ok_or_else(|| {
            AutumnError::service_unavailable_msg("autumn-harvest requires a configured database")
        }),
        HarvestMode::Split | HarvestMode::External => {
            let database = DatabaseConfig {
                url: config.database.url.clone(),
                ..DatabaseConfig::default()
            };
            db::create_pool(&database)
                .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?
                .ok_or_else(|| {
                    AutumnError::service_unavailable_msg(
                        "harvest.database.url must resolve to a dedicated database pool",
                    )
                })
        }
    }
}

fn harvest_database_url(
    app_config: &AutumnConfig,
    config: &HarvestRuntimeConfig,
) -> autumn_web::AutumnResult<String> {
    match config.mode {
        HarvestMode::Embedded => app_config.database.url.clone().ok_or_else(|| {
            AutumnError::service_unavailable_msg(
                "autumn-harvest requires database.url when harvest.mode is embedded",
            )
        }),
        HarvestMode::Split | HarvestMode::External => config.database.url.clone().ok_or_else(|| {
            AutumnError::service_unavailable_msg(
                "harvest.database.url must be configured when harvest.mode is split or external",
            )
        }),
    }
}

async fn stop_harvest_runtime(slot: Arc<Mutex<HarvestRuntimeSlot>>, api_state: HarvestApiState) {
    let runtime = { slot.lock().expect("harvest lock poisoned").runtime.take() };

    let Some(runtime) = runtime else {
        // No runtime to stop (never started, or already stopped by a prior call).
        // No worker/scanner can be evaluating a completion trigger, so clearing the
        // global gate cache now is safe. Ptr-eq guarded (F5) so tearing down this
        // plugin never clobbers a concurrently-live sibling's cache. The metrics
        // recorder is not cleared here: without a runtime we have no recorder Arc to
        // ptr-eq against (it was either never published by us or already cleared).
        if let Some(installed) = autumn_harvest::admission_gate::global_admission_gate_cache()
            && Arc::ptr_eq(&installed, &api_state.gate_cache())
        {
            autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
        }
        api_state.clear();
        return;
    };

    // Capture this runtime's metrics recorder Arc BEFORE `HarvestRunner::stop(self)`
    // consumes the runner, so we can ptr-eq against it when clearing the global
    // recorder after the runner has stopped.
    let stopped_metrics = runtime
        .runner
        .api_runtime()
        .registry()
        .telemetry()
        .metrics
        .clone();

    // Issue #944: stop the broker consumers FIRST, before the runner. Each
    // `ConnectorRuntime::run` drains its in-flight dispatches before
    // returning, so a message whose dispatch already committed still gets
    // acknowledged rather than being redelivered after the restart.
    #[cfg(feature = "connectors")]
    stop_connectors(runtime.connectors).await;

    if let Some(gate_refresh) = runtime.gate_refresh {
        gate_refresh.shutdown.cancel();
        let _ = gate_refresh.handle.await;
    }
    if let Some(outbox) = runtime.outbox {
        outbox.shutdown.cancel();
        if let Err(error) = outbox.handle.await
            && !error.is_cancelled()
        {
            tracing::warn!(error = %error, "harvest outbox relay failed during shutdown");
        }
    }

    // issue #618 (F-round9): stop the runner — all worker + timeout-scanner tasks —
    // BEFORE clearing the process-global admission gate cache / metrics recorder. A
    // worker or timeout scanner evaluating a completion trigger reads
    // `global_admission_gate_cache()` in that window; clearing the globals while those
    // tasks are still alive would let a trigger start its target unblocked +
    // uncounted under an active gate during shutdown/restart. Once `stop()` returns,
    // no task can evaluate a trigger, so it is safe to clear the globals.
    runtime.runner.stop().await;

    // issue #618 (F5, reordered by F-round9): clear the process-global gate cache and
    // metrics recorder so a stopped plugin's cache (whose refresh loop is now
    // cancelled and would go stale) never leaks into a sibling plugin's
    // completion-trigger gate check. Each clear is ptr-eq guarded so tearing down one
    // plugin never clobbers a concurrently-live sibling's cache/recorder. Mirrors
    // #921's teardown of GLOBAL_CALLBACK_CONFIG.
    if let Some(installed) = autumn_harvest::admission_gate::global_admission_gate_cache()
        && Arc::ptr_eq(&installed, &api_state.gate_cache())
    {
        autumn_harvest::admission_gate::set_global_admission_gate_cache(None);
    }
    if let Some(installed) = autumn_harvest::admission_gate::global_admission_metrics()
        && Arc::ptr_eq(&installed, &stopped_metrics)
    {
        autumn_harvest::admission_gate::set_global_admission_metrics(None);
    }
    api_state.clear();
}

/// Runs pending-migration checks for the outbox, Harvest storage, and plugin
/// Harvest storage databases, awaiting the result off the calling task's own
/// async executor thread.
///
/// `autumn_web::migrate::run_pending`/`pending_migrations` are synchronous and
/// genuinely block the calling thread: as of autumn-web 0.7 both route through
/// `with_migration_connection!`, which runs the connect *and* the migration
/// body on a freshly spawned `std::thread::scope` thread and `join()`s it.
/// `spawn_blocking` keeps that join off an async worker thread, where it would
/// otherwise stall the worker for the full duration of the migration.
///
/// Historical note (issue #1232): under autumn-web 0.6 this wrapper was also
/// required for *correctness*, not just for scheduling hygiene. A
/// TLS-requiring `sslmode` (`require`/`verify-ca`/`verify-full`) used to reuse
/// the *current* Tokio runtime handle and call `Handle::block_on` on it --
/// which Tokio forbids (and panics on) from a thread already driving that
/// runtime's tasks, exactly what an `on_startup` hook is. autumn-web 0.7 fixed
/// that upstream by moving the whole operation onto a thread that has never
/// entered a runtime, so the TLS path is now safe from any caller context.
/// `migration_body_is_safe_to_call_directly_from_an_async_context` below pins
/// that upstream guarantee, so a regression in it is caught here rather than
/// resurfacing as a production startup panic.
async fn ensure_runtime_migrations(
    profile: &str,
    app_config: &AutumnConfig,
    harvest_config: &HarvestRuntimeConfig,
) -> autumn_web::AutumnResult<()> {
    let profile = profile.to_owned();
    let app_config = app_config.clone();
    let harvest_config = harvest_config.clone();

    match tokio::task::spawn_blocking(move || {
        ensure_runtime_migrations_blocking(&profile, &app_config, &harvest_config)
    })
    .await
    {
        Ok(result) => result,
        Err(join_error) => Err(AutumnError::service_unavailable_msg(format!(
            "harvest runtime migrations task failed to join: {join_error}"
        ))),
    }
}

/// The synchronous migration-check body. **Must never be called from a thread
/// that is currently driving a Tokio runtime's async tasks** -- against a
/// TLS-requiring database URL it panics (see [`ensure_runtime_migrations`]).
/// Sync contexts (the CLI) and `spawn_blocking` tasks are both fine; call
/// [`ensure_runtime_migrations`] from an async context instead.
fn ensure_runtime_migrations_blocking(
    profile: &str,
    app_config: &AutumnConfig,
    harvest_config: &HarvestRuntimeConfig,
) -> autumn_web::AutumnResult<()> {
    // Autumn owns every set that lives in the APPLICATION database -- the
    // outbox always, plus the two Harvest sets under `Embedded`, where the
    // application database *is* the Harvest database. They are registered in
    // `register_plugin_migrations` and applied during `setup_database`, before
    // this startup hook runs. Under `Embedded` there is therefore nothing left
    // for the plugin to migrate.
    let harvest_database_url = match harvest_config.mode {
        HarvestMode::Embedded => {
            // Still validate the invariant Autumn cannot check for us: with no
            // `database.url` there is no Harvest storage at all, and failing
            // here is far clearer than a missing-table error later.
            if app_config.database.url.is_none() {
                return Err(AutumnError::service_unavailable_msg(
                    "autumn-harvest requires database.url when harvest.mode is embedded",
                ));
            }
            return Ok(());
        }
        // A dedicated Harvest database Autumn has no handle on: `plugin_migrations`
        // only ever targets the application's primary database, so these two sets
        // are applied here, against `harvest.database.url`.
        HarvestMode::Split | HarvestMode::External => {
            harvest_config.database.url.as_deref().ok_or_else(|| {
                AutumnError::service_unavailable_msg(
                    "harvest.database.url is required for dedicated Harvest storage",
                )
            })?
        }
    };

    apply_migrations_for_profile(
        profile,
        harvest_database_url,
        HARVEST_MIGRATIONS,
        "Harvest storage",
    )?;

    // Plugin-owned tables that live in the harvest database (issue #944's
    // connector dead-letter table). Applied to the same dedicated URL as the
    // core migrations above so `Split`/`External` deployments get them where
    // the code that writes them actually connects.
    apply_migrations_for_profile(
        profile,
        harvest_database_url,
        PLUGIN_HARVEST_MIGRATIONS,
        "Harvest plugin storage",
    )
}

fn apply_migrations_for_profile(
    profile: &str,
    database_url: &str,
    migrations: EmbeddedMigrations,
    label: &str,
) -> autumn_web::AutumnResult<()> {
    if profile == "dev" {
        let result = autumn_web::migrate::run_pending(database_url, migrations)
            .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
        if result.applied.is_empty() {
            tracing::info!(target = label, "No pending migrations");
        } else {
            for migration in result.applied {
                tracing::info!(target = label, migration = %migration, "Applied migration");
            }
        }
        return Ok(());
    }

    match autumn_web::migrate::pending_migrations(database_url, migrations) {
        Ok(pending) if pending.is_empty() => {
            tracing::info!(target = label, "Database migrations are up to date");
        }
        Ok(pending) => {
            tracing::warn!(
                target = label,
                count = pending.len(),
                "Pending migrations detected. Run `autumn migrate` to apply them."
            );
            for migration in pending {
                tracing::warn!(target = label, migration = %migration, "Pending migration");
            }
        }
        Err(error) => {
            tracing::warn!(target = label, error = %error, "Could not check migration status");
        }
    }

    Ok(())
}

#[cfg(test)]
mod admission_globals_guard_tests {
    use super::AdmissionGlobalsGuard;
    use autumn_harvest::admission_gate::{
        AdmissionGateCache, global_admission_gate_cache, global_admission_metrics,
        set_global_admission_gate_cache, set_global_admission_metrics,
    };
    use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
    use std::sync::Arc;

    // The guard mutates the process-global admission statics; serialize the tests.
    static GUARD_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn recorder() -> Arc<dyn MetricsRecorder> {
        Arc::new(NoOpMetrics)
    }

    /// F-round7: an early-return error path (guard dropped without `commit()`)
    /// clears BOTH published globals.
    #[test]
    fn guard_drop_clears_both_globals() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
        {
            let mut guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            guard.publish_metrics(recorder());
            assert!(global_admission_gate_cache().is_some());
            assert!(global_admission_metrics().is_some());
            // dropped here WITHOUT commit -> simulates a startup error after publish
        }
        assert!(
            global_admission_gate_cache().is_none(),
            "guard drop clears the gate cache"
        );
        assert!(
            global_admission_metrics().is_none(),
            "guard drop clears the metrics recorder"
        );
    }

    /// F-round7: an error while only the gate cache has been published (before the
    /// adjacent `publish_metrics` call runs) still clears the gate cache. As of
    /// F-round14 both publishes happen back-to-back BEFORE `HarvestRunner::start`, so
    /// this guards the guard's own Drop semantics in isolation rather than a specific
    /// `start_harvest_runtime` error site.
    #[test]
    fn guard_drop_before_metrics_publish_clears_the_gate_cache() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
        {
            let _guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            // dropped before publish_metrics
        }
        assert!(
            global_admission_gate_cache().is_none(),
            "early drop (pre-metrics-publish) clears the gate cache"
        );
        assert!(global_admission_metrics().is_none());
    }

    /// F-round7: `commit()` (successful startup) keeps both globals published.
    #[test]
    fn guard_commit_keeps_both_globals() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
        {
            let mut guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            guard.publish_metrics(recorder());
            guard.commit();
        }
        assert!(
            global_admission_gate_cache().is_some(),
            "commit keeps the gate cache"
        );
        assert!(
            global_admission_metrics().is_some(),
            "commit keeps the metrics recorder"
        );
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
    }

    /// F-round14: both globals are visible after the two publishes run back-to-back,
    /// BEFORE any consumer (the worker poll loops / timeout scanner spawned by
    /// `HarvestRunner::start`) could observe them. In `start_harvest_runtime` both
    /// `publish_gate_cache` and `publish_metrics` now run before the runner starts, so
    /// a cancel / terminate / parent-close-cascade completion-trigger block firing in
    /// the boot window finds a live `global_admission_metrics()` and counts the block
    /// rather than dropping it.
    #[test]
    fn guard_publishes_both_globals_before_any_consumer_runs() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
        let mut guard =
            AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
        guard.publish_metrics(recorder());
        // Both must be live at this point — this is the state the runner (and its
        // workers/scanner) is started against in `start_harvest_runtime`.
        assert!(
            global_admission_gate_cache().is_some(),
            "gate cache is published before the runner starts"
        );
        assert!(
            global_admission_metrics().is_some(),
            "metrics recorder is published before the runner starts (F-round14)"
        );
        drop(guard);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
    }

    /// F-round7: a stopping guard must NOT clobber a concurrently-live sibling's
    /// globals — the clear is ptr-eq-guarded against THIS guard's Arcs.
    #[test]
    fn guard_drop_does_not_clobber_a_sibling_cache() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
        let sibling = Arc::new(AdmissionGateCache::new());
        {
            let _guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            // A sibling runtime overwrites the global with its OWN cache.
            set_global_admission_gate_cache(Some(Arc::clone(&sibling)));
            // our guard drops here -> ptr-eq(our cache, sibling) is false -> no clear
        }
        assert!(
            global_admission_gate_cache().is_some_and(|c| Arc::ptr_eq(&c, &sibling)),
            "guard drop must not clobber a sibling's installed cache"
        );
        set_global_admission_gate_cache(None);
    }

    /// issue #618 (final pass): when THIS guard published its cache OVER a
    /// still-running sibling's (the sibling was installed first), a
    /// drop-without-commit must RESTORE the sibling's cache — not clear the global
    /// to `None`, which would wipe the live sibling's gate enforcement. Same for
    /// the metrics recorder.
    #[test]
    fn guard_drop_restores_the_previous_sibling_globals_not_none() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Sibling runtime A is already live with its own cache + metrics.
        let sibling_cache = Arc::new(AdmissionGateCache::new());
        let sibling_metrics: Arc<dyn MetricsRecorder> = recorder();
        set_global_admission_gate_cache(Some(Arc::clone(&sibling_cache)));
        set_global_admission_metrics(Some(Arc::clone(&sibling_metrics)));
        {
            // Second runtime B publishes its OWN globals over A's, then fails
            // (dropped without commit).
            let mut guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            guard.publish_metrics(recorder());
            // Sanity: B's globals are installed now (not A's).
            assert!(
                global_admission_gate_cache().is_some_and(|c| !Arc::ptr_eq(&c, &sibling_cache)),
                "B's cache is installed over A's before the failure"
            );
        }
        // B's guard dropped without commit -> A's cache + metrics must be restored,
        // NOT cleared to None (the live sibling keeps its enforcement).
        assert!(
            global_admission_gate_cache().is_some_and(|c| Arc::ptr_eq(&c, &sibling_cache)),
            "a failed second-runtime startup must RESTORE the sibling's cache, not clear it"
        );
        assert!(
            global_admission_metrics().is_some_and(|m| Arc::ptr_eq(&m, &sibling_metrics)),
            "a failed second-runtime startup must RESTORE the sibling's metrics recorder"
        );
        set_global_admission_gate_cache(None);
        set_global_admission_metrics(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    use crate::config::{
        HarvestDatabaseConfig, HarvestMode, HarvestOutboxConfig, HarvestRuntimeConfig,
    };
    use crate::runner::injected_runtime_state;
    use crate::{AppDbPool, HarvestDbPool};
    use autumn_harvest::dag::DagBuilder;
    use autumn_harvest::policy::Schedule;
    use autumn_web::config::DatabaseConfig;

    /// Shutting a fleet of connectors down must not serialize their drains.
    ///
    /// Deliberately *not* a wall-clock threshold: the first connector's drain
    /// is made to genuinely depend on the second having been cancelled. Under
    /// a cancel-then-await-one-at-a-time shutdown that dependency can never be
    /// satisfied — the second connector's cancellation is still queued behind
    /// the first connector's `await` — so the shutdown deadlocks and the
    /// timeout below fires. It is exactly the deadlock a real fleet risks in
    /// slow motion, and it either happens or it does not.
    #[cfg(feature = "connectors")]
    #[tokio::test]
    async fn every_connector_is_cancelled_before_any_drain_is_awaited() {
        let (second_cancelled_tx, second_cancelled_rx) = tokio::sync::oneshot::channel::<()>();

        let first_shutdown = CancellationToken::new();
        let first = ConnectorRuntimeHandle {
            shutdown: first_shutdown.clone(),
            handle: tokio::spawn(async move {
                first_shutdown.cancelled().await;
                // Stand-in for "this drain outlives the next connector's
                // cancellation" — true of any real drain that is still
                // finishing when the next connector is asked to stop.
                let _ = second_cancelled_rx.await;
            }),
        };

        let second_shutdown = CancellationToken::new();
        let second = ConnectorRuntimeHandle {
            shutdown: second_shutdown.clone(),
            handle: tokio::spawn(async move {
                second_shutdown.cancelled().await;
                let _ = second_cancelled_tx.send(());
            }),
        };

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stop_connectors(vec![first, second]),
        )
        .await
        .expect(
            "shutdown must cancel every connector before awaiting any of their drains; \
             awaiting one at a time leaves later connectors running — and consuming — \
             for the whole of the earlier ones' bounded drains",
        );
    }

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "echo",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            sla: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }
    }

    fn fake_activity_info() -> ActivityInfo {
        ActivityInfo {
            name: "echo_activity",
            module: "tests",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn fake_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily",
            module: "tests",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
            workflow_handler: None,
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: autumn_harvest::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        }
    }

    /// Issue #763 review, "Exclude registered DAGs from transactional workflow
    /// starts": `transactional_client_workflows` must drop exactly the names
    /// present in `registered_dag_names` and keep every other registered
    /// workflow untouched, mirroring what `HarvestApiRuntime::is_registered_dag`
    /// checks against for the HTTP start route.
    #[test]
    fn transactional_client_workflows_excludes_registered_dag_names() {
        let plain = fake_workflow_info();
        let dag_shadow = WorkflowInfo {
            name: "daily",
            ..fake_workflow_info()
        };
        let registered_dags: HashSet<String> = HashSet::from(["daily".to_string()]);

        let kept =
            transactional_client_workflows([&plain, &dag_shadow].into_iter(), &registered_dags);

        let kept_names: Vec<&str> = kept.iter().map(|w| w.name).collect();
        assert_eq!(
            kept_names,
            vec!["echo"],
            "the DAG-shadow WorkflowInfo (name \"daily\", present in \
             registered_dag_names) must be excluded so a caller naming it hits the \
             same 'not registered' rejection as any other unregistered workflow \
             name -- never a silent bypass of trigger_unified_dag's pause / \
             max_active_runs admission checks"
        );
    }

    /// Regression guard for the fix above: with no DAGs registered at all, every
    /// workflow passes through unchanged -- the filter must not accidentally cut
    /// out ordinary workflows.
    #[test]
    fn transactional_client_workflows_keeps_everything_when_no_dags_registered() {
        let a = fake_workflow_info();
        let b = WorkflowInfo {
            name: "other",
            ..fake_workflow_info()
        };
        let registered_dags: HashSet<String> = HashSet::new();

        let kept = transactional_client_workflows([&a, &b].into_iter(), &registered_dags);

        let kept_names: Vec<&str> = kept.iter().map(|w| w.name).collect();
        assert_eq!(kept_names, vec!["echo", "other"]);
    }

    #[cfg(feature = "webhooks")]
    fn fake_webhook_trigger(
        name: &'static str,
        path: &'static str,
    ) -> autumn_harvest::webhook_trigger::WebhookTriggerInfo {
        // Must match `WebhookHandlerFn`'s Result-returning signature.
        #[allow(clippy::unnecessary_wraps)]
        fn dispatch(
            _ctx: &autumn_harvest::webhook_trigger::WebhookCtx,
            _payload: &serde_json::Value,
        ) -> Result<autumn_harvest::WorkflowId, autumn_harvest::webhook_trigger::WebhookHandlerError>
        {
            Ok(autumn_harvest::WorkflowId::new("wf"))
        }

        autumn_harvest::webhook_trigger::WebhookTriggerInfo {
            name,
            module: "tests",
            path,
            target: autumn_harvest::webhook_trigger::WebhookTarget::Starts { workflow: "echo" },
            handler: dispatch,
            queue: None,
        }
    }

    #[test]
    fn workflow_log_persistence_threads_the_policy_into_the_built_harvest() {
        // Issue #790 (review P1): the durable log sink lives on
        // `HarvestBuilder`, and `HarvestPlugin` holds its builder privately
        // behind a hand-picked forwarder set. Without this forwarder the
        // feature has NO enabling path for a plugin-wired app -- which is the
        // primary documented deployment shape, and exactly what every doc
        // snippet shows.
        let built = HarvestPlugin::new()
            .workflow_log_persistence(
                autumn_harvest::WorkflowLogPolicy::new()
                    .with_max_lines(7)
                    .with_max_message_bytes(64),
            )
            .builder
            .try_build()
            .expect("builder must build");

        let policy = built
            .workflow_log_policy
            .expect("the forwarded policy must reach BuiltHarvest");
        assert_eq!(policy.max_lines(), 7);
        assert_eq!(policy.max_message_bytes(), 64);
    }

    #[test]
    fn workflow_log_persistence_is_off_unless_the_forwarder_is_called() {
        // The AC6 opt-in boundary: absent the call the sink is disabled and
        // `ctx.logger()` is byte-for-byte today's tracing-only behaviour.
        let built = HarvestPlugin::new()
            .builder
            .try_build()
            .expect("builder must build");
        assert!(built.workflow_log_policy.is_none());
    }

    #[cfg(feature = "webhooks")]
    #[test]
    fn webhooks_accumulates_across_repeated_calls() {
        // Codex review (PR #918): unlike .workflows/.activities/.queries/
        // .updates, .webhooks used to *replace* rather than extend, so a
        // second call from a separate module silently dropped the first
        // call's bindings.
        let plugin = HarvestPlugin::new()
            .webhooks(vec![fake_webhook_trigger("a", "/hooks/a")])
            .webhooks(vec![fake_webhook_trigger("b", "/hooks/b")]);
        assert_eq!(
            plugin.webhook_triggers.len(),
            2,
            "both .webhooks() calls must be preserved"
        );
        let paths: Vec<_> = plugin
            .webhook_triggers
            .iter()
            .map(|t| t.path)
            .collect::<Vec<_>>();
        assert!(paths.contains(&"/hooks/a"));
        assert!(paths.contains(&"/hooks/b"));
    }

    fn test_pool(database_url: &str, pool_size: usize) -> DbPool {
        autumn_web::db::create_pool(&DatabaseConfig {
            url: Some(database_url.to_owned()),
            pool_size,
            ..DatabaseConfig::default()
        })
        .expect("test pool config should build")
        .expect("test pool should exist")
    }

    #[test]
    fn harvest_plugin_accumulates_registrations_fluently() {
        let plugin = HarvestPlugin::new()
            .workflows(vec![fake_workflow_info()])
            .activities(vec![fake_activity_info()])
            .dags(vec![fake_dag_info()])
            .state(String::from("haunted"))
            .worker(WorkerConfig::default().with_queues(["harvest"]))
            .api("/api/harvest");

        assert_eq!(plugin.builder.workflow_count(), 1);
        assert_eq!(plugin.builder.activity_count(), 1);
        assert_eq!(plugin.builder.dag_count(), 1);
        assert_eq!(plugin.api_path.as_deref(), Some("/api/harvest"));

        let built = plugin.builder.build();
        assert_eq!(
            built.worker_config().queues.first().map(String::as_str),
            Some("harvest")
        );
        assert_eq!(built.state::<String>().map(String::as_str), Some("haunted"));
    }

    #[test]
    fn harvest_plugin_api_with_auth_sets_path_and_middleware() {
        let plugin =
            HarvestPlugin::new().api_with_auth("/api", autumn_web::auth::RequireAuth::new("test"));

        assert_eq!(plugin.api_path.as_deref(), Some("/api"));
        assert!(plugin.api_middleware.is_some());
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn harvest_plugin_api_with_auth_also_sets_mcp_tool_middleware() {
        let plugin =
            HarvestPlugin::new().api_with_auth("/api", autumn_web::auth::RequireAuth::new("test"));

        assert!(
            plugin.mcp_tool_middleware.is_some(),
            "api_with_auth must configure the same layer for generated MCP tool routes"
        );
    }

    /// Regression test (issue #597, PR #908 review): `mcp_tools()` combined
    /// with no `api_with_auth(..)` must be flagged so the generated tool
    /// routes' unauthenticated-at-their-own-path gap is never silent.
    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_tools_unprotected_flags_enabled_without_middleware_only() {
        assert!(mcp_tools_unprotected(true, false));
        assert!(!mcp_tools_unprotected(true, true));
        assert!(!mcp_tools_unprotected(false, false));
        assert!(!mcp_tools_unprotected(false, true));
    }

    /// Issue #608: read-path payload decoding is a deliberate, explicit
    /// opt-in (mirrors the `mcp_tools()` const-fn flag precedent). The flag
    /// must default off so a plugin that never mentions it produces
    /// byte-for-byte identical responses to today.
    #[test]
    fn decode_payloads_on_read_flag_defaults_off_and_sets_on_builder() {
        let plugin = HarvestPlugin::new();
        assert!(
            !plugin.decode_payloads_on_read,
            "decode_payloads_on_read must default to false (issue #608 AC1)"
        );

        let plugin = plugin.decode_payloads_on_read();
        assert!(
            plugin.decode_payloads_on_read,
            "HarvestPlugin::decode_payloads_on_read() must set the opt-in flag"
        );
    }

    /// The synthetic liveness canary (issue #796) must be off by default so a
    /// plugin that never calls `synthetic_canary` produces a byte-for-byte
    /// identical runtime (AC1), and the opt-in must store the config.
    #[test]
    fn synthetic_canary_defaults_off_and_sets_on_builder() {
        let plugin = HarvestPlugin::new();
        assert!(
            plugin.canary_config.is_none(),
            "canary must default off (issue #796 AC1)"
        );

        let cfg = crate::canary::CanaryConfig::new(std::time::Duration::from_secs(30))
            .with_queues(vec!["email".to_string()]);
        let plugin = plugin.synthetic_canary(cfg);
        let stored = plugin
            .canary_config
            .as_ref()
            .expect("synthetic_canary() must store the config");
        assert_eq!(stored.queues(), &["email".to_string()]);
    }

    /// Issue #679: the rolled-up status thresholds default to the starter
    /// values and are overridable via the builder.
    #[test]
    fn status_thresholds_default_and_override_on_builder() {
        let plugin = HarvestPlugin::new();
        assert_eq!(plugin.status_thresholds.dlq_critical_total, 100);

        let custom = crate::status_summary::StatusThresholds {
            dlq_critical_total: 7,
            ..crate::status_summary::StatusThresholds::default()
        };
        let plugin = plugin.with_status_thresholds(custom);
        assert_eq!(
            plugin.status_thresholds.dlq_critical_total, 7,
            "HarvestPlugin::with_status_thresholds() must override the thresholds"
        );
    }

    #[test]
    fn harvest_plugin_build_registers_startup_and_shutdown_hooks() {
        let app = autumn_web::app().plugin(
            HarvestPlugin::new()
                .workflows(vec![fake_workflow_info()])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        );

        assert!(app.has_plugin(std::any::type_name::<HarvestPlugin>()));
    }

    #[tokio::test]
    async fn harvest_runner_rejects_classic_dags_without_unified_handler() {
        let built = HarvestBuilder::new().dags(vec![fake_dag_info()]).build();
        let pool = test_pool("postgres://harvest:harvest@localhost:5432/harvest", 4);
        let result = HarvestRunner::start(
            built,
            &HarvestRuntimeConfig {
                mode: HarvestMode::External,
                worker_enabled: false,
                scheduler_enabled: false,
                database: HarvestDatabaseConfig {
                    url: Some("postgres://harvest:harvest@localhost:5432/harvest".to_string()),
                },
                outbox: HarvestOutboxConfig::default(),
                batch: crate::config::HarvestBatchConfig::default(),
                readiness: crate::config::HarvestReadinessConfig::default(),
                startup: crate::config::HarvestStartupConfig::default(),
            },
            HarvestRunnerResources::new(pool),
        )
        .await;

        let Err(err) = result else {
            panic!("classic DAG runtime should be rejected before startup");
        };
        assert!(
            err.to_string().contains("classic DAG"),
            "error should identify unsupported classic DAG configuration: {err}"
        );
    }

    #[test]
    fn injected_runtime_state_contains_app_state() {
        let state = AppState::for_test();
        let harvest_pool = test_pool("postgres://harvest:harvest@localhost:5432/harvest", 4);
        let injected = injected_runtime_state(
            Some(state.clone()),
            None,
            HarvestDbPool::from(harvest_pool),
            ShardRouter::single(),
        );
        let stored = injected
            .get(&TypeId::of::<AppState>())
            .and_then(|value| value.downcast_ref::<AppState>())
            .expect("app state should be injected");

        assert_eq!(stored.profile(), state.profile());
    }

    #[test]
    fn harvest_plugin_embedded_mode_reuses_app_pool() {
        let app_pool = test_pool("postgres://app:app@localhost:5432/app", 3);
        let state = AppState::for_test().with_pool(app_pool);
        let config = HarvestRuntimeConfig::default();

        let harvest_pool =
            resolve_harvest_pool(&state, &config).expect("embedded mode should reuse app pool");

        assert_eq!(harvest_pool.status().max_size, 3);
    }

    #[test]
    fn harvest_plugin_split_mode_builds_dedicated_harvest_pool() {
        let app_pool = test_pool("postgres://app:app@localhost:5432/app", 3);
        let state = AppState::for_test().with_pool(app_pool.clone());
        let config = HarvestRuntimeConfig {
            mode: HarvestMode::Split,
            database: HarvestDatabaseConfig {
                url: Some("postgres://harvest:harvest@localhost:5432/harvest".to_owned()),
            },
            ..HarvestRuntimeConfig::default()
        };

        let harvest_pool = resolve_harvest_pool(&state, &config)
            .expect("split mode should resolve a dedicated harvest pool");

        assert_eq!(app_pool.status().max_size, 3);
        assert_eq!(harvest_pool.status().max_size, 10);
    }

    #[test]
    fn injected_runtime_state_contains_explicit_app_and_harvest_pool_roles() {
        let app_pool = test_pool("postgres://app:app@localhost:5432/app", 3);
        let harvest_pool = test_pool("postgres://harvest:harvest@localhost:5432/harvest", 7);
        let app_state = AppState::for_test().with_pool(app_pool.clone());
        let injected = injected_runtime_state(
            Some(app_state),
            Some(app_pool),
            HarvestDbPool::from(harvest_pool),
            ShardRouter::single(),
        );

        let app_db = injected
            .get(&TypeId::of::<AppDbPool>())
            .and_then(|value| value.downcast_ref::<AppDbPool>())
            .expect("app db pool should be injected");
        let harvest_db = injected
            .get(&TypeId::of::<HarvestDbPool>())
            .and_then(|value| value.downcast_ref::<HarvestDbPool>())
            .expect("harvest db pool should be injected");
        let legacy_harvest_db = injected
            .get(&TypeId::of::<DbPool>())
            .and_then(|value| value.downcast_ref::<DbPool>())
            .expect("legacy harvest db pool should still be injected");

        assert_eq!(app_db.status().max_size, 3);
        assert_eq!(harvest_db.status().max_size, 7);
        assert_eq!(legacy_harvest_db.status().max_size, 7);
    }

    #[test]
    fn harvest_plugin_external_mode_builds_dedicated_harvest_pool() {
        let app_pool = test_pool("postgres://app:app@localhost:5432/app", 3);
        let state = AppState::for_test().with_pool(app_pool);
        let config = HarvestRuntimeConfig {
            mode: HarvestMode::External,
            worker_enabled: false,
            scheduler_enabled: false,
            database: HarvestDatabaseConfig {
                url: Some("postgres://harvest:harvest@localhost:5432/harvest".to_owned()),
            },
            outbox: HarvestOutboxConfig::default(),
            batch: crate::config::HarvestBatchConfig::default(),
            readiness: crate::config::HarvestReadinessConfig::default(),
            startup: crate::config::HarvestStartupConfig::default(),
        };

        let harvest_pool = resolve_harvest_pool(&state, &config)
            .expect("external mode should resolve a dedicated harvest pool");

        assert_eq!(harvest_pool.status().max_size, 10);
    }

    /// Fixture for the two tests below: a `Split`-mode `AutumnConfig`/
    /// `HarvestRuntimeConfig` pair whose **Harvest** database URL requires
    /// TLS (`sslmode=require`), pointed at a port nothing is listening on.
    /// `127.0.0.1:1` refuses the connection almost instantly on every
    /// platform, so these tests are hermetic and fast -- no real
    /// TLS-enabled Postgres server is needed. Reaching
    /// `autumn_web::migrate`'s rustls connection path at all is what
    /// triggers the "`block_on` from within a runtime" hazard, before any
    /// network I/O happens -- so whether the target is actually reachable
    /// is irrelevant to what these tests exercise.
    ///
    /// `Split` (rather than `Embedded`) is load bearing: since Autumn took
    /// ownership of the application database's migrations via
    /// `plugin_migrations`, a dedicated Harvest database is the *only* thing
    /// the plugin still migrates itself, so it is the only mode that reaches
    /// the connection path this regression is about. Under `Embedded`,
    /// `ensure_runtime_migrations_blocking` returns before connecting and both
    /// tests below would pass vacuously.
    fn tls_url_fixture() -> (AutumnConfig, HarvestRuntimeConfig) {
        let app_config = AutumnConfig {
            database: DatabaseConfig {
                url: Some("postgres://user:pass@127.0.0.1:1/app".to_owned()),
                ..DatabaseConfig::default()
            },
            ..AutumnConfig::default()
        };
        let harvest_config = HarvestRuntimeConfig {
            mode: HarvestMode::Split,
            database: crate::config::HarvestDatabaseConfig {
                url: Some("postgres://user:pass@127.0.0.1:1/harvest?sslmode=require".to_owned()),
            },
            ..HarvestRuntimeConfig::default()
        };
        (app_config, harvest_config)
    }

    /// Migration-only commands build plugins without running startup hooks, so
    /// invalid Harvest configuration must fail during registration rather than
    /// silently producing an incomplete migration plan.
    #[test]
    #[should_panic(expected = "cannot register Harvest migrations: configuration error: bad mode")]
    fn migration_registration_rejects_unreadable_harvest_config() {
        migration_registration_mode(Err(autumn_web::config::ConfigError::Validation(
            "bad mode".to_owned(),
        )));
    }

    /// Pins the ownership split itself: under `Embedded` the plugin migrates
    /// nothing (Autumn applied the registered sets during `setup_database`,
    /// before this startup hook ran), so the body returns `Ok` **without**
    /// dialling the database -- proven here by a URL that would otherwise
    /// fail to connect. This is what makes `tls_url_fixture`'s use of
    /// `Split` necessary rather than incidental.
    #[test]
    fn embedded_mode_migrates_nothing_and_never_connects() {
        let app_config = AutumnConfig {
            database: DatabaseConfig {
                url: Some("postgres://user:pass@127.0.0.1:1/app".to_owned()),
                ..DatabaseConfig::default()
            },
            ..AutumnConfig::default()
        };
        let harvest_config = HarvestRuntimeConfig {
            mode: HarvestMode::Embedded,
            ..HarvestRuntimeConfig::default()
        };

        let result = ensure_runtime_migrations_blocking("dev", &app_config, &harvest_config);

        assert!(
            result.is_ok(),
            "embedded mode must short-circuit before connecting: {result:?}"
        );
    }

    /// The one invariant Autumn cannot check for us: `Embedded` means "Harvest
    /// lives in the application database", so with no `database.url` there is
    /// no Harvest storage at all. Failing here beats a missing-table error
    /// deep in the first workflow start.
    #[test]
    fn embedded_mode_without_a_database_url_is_rejected() {
        let harvest_config = HarvestRuntimeConfig {
            mode: HarvestMode::Embedded,
            ..HarvestRuntimeConfig::default()
        };

        let result =
            ensure_runtime_migrations_blocking("dev", &AutumnConfig::default(), &harvest_config);

        assert!(
            result.is_err(),
            "embedded mode with no database.url must be rejected: {result:?}"
        );
    }

    /// Pins the autumn-web 0.7 upstream fix that the wrapper's original
    /// rationale depended on (issue #1232): the synchronous migration body is
    /// now safe to call *directly* from an async task -- the shape a
    /// `#[tokio::test]` gives, matching the real `on_startup` hook -- even
    /// against a TLS-requiring URL. Under 0.6 the rustls branch reused this
    /// task's own runtime handle and panicked with "Cannot start a runtime
    /// from within a runtime"; 0.7 runs connect and body on a freshly spawned
    /// thread that has never entered a runtime.
    ///
    /// `ensure_runtime_migrations` still wraps this in `spawn_blocking`, now
    /// purely so the blocking `join()` never stalls an async worker thread.
    /// If a future autumn-web reintroduces the ambient-runtime dependency,
    /// this test fails loudly here instead of at a customer's startup.
    #[tokio::test]
    async fn migration_body_is_safe_to_call_directly_from_an_async_context() {
        let (app_config, harvest_config) = tls_url_fixture();

        let result = ensure_runtime_migrations_blocking("dev", &app_config, &harvest_config);

        assert!(
            result.is_err(),
            "127.0.0.1:1 refuses the connection, so this must return an ordinary \
             error rather than panicking the calling task: {result:?}"
        );
    }

    /// Regression test for `ensure_runtime_migrations` failing on a
    /// TLS-enabled Postgres: it must be safely callable from within an
    /// async context (replicating the real `start_harvest_runtime`/
    /// `on_startup` call site) against a TLS-requiring database URL.
    /// Before the fix this panicked the calling task instead of returning
    /// an ordinary connection error.
    #[tokio::test]
    async fn ensure_runtime_migrations_does_not_panic_on_a_tls_requiring_database_url() {
        let (app_config, harvest_config) = tls_url_fixture();

        let result = ensure_runtime_migrations("dev", &app_config, &harvest_config).await;

        assert!(
            result.is_err(),
            "127.0.0.1:1 refuses the connection, so this must fail cleanly with an \
             ordinary error rather than panicking the calling task: {result:?}"
        );
    }

    #[test]
    fn harvest_plugin_forwards_updates_and_queries_to_builder() {
        // Issue #597 gap-fill: #[update(..., mcp)] handlers must reach the
        // builder through the plugin's fluent surface.
        fn fake_update() -> autumn_harvest::UpdateHandlerInfo {
            autumn_harvest::UpdateHandlerInfo {
                name: "approve",
                workflow: "echo",
                module: "tests",
                input_type_hint: "ApproveRequest",
                output_type_hint: "bool",
                has_validator: false,
                handler: |_ctx, _args| Box::pin(async move { Ok(serde_json::Value::Null) }),
                validator: None,
                mcp: true,
                description: None,
                arg_schema: None,
                response_schema: None,
            }
        }
        fn fake_query() -> autumn_harvest::QueryHandlerInfo {
            autumn_harvest::QueryHandlerInfo {
                name: "progress",
                workflow: "echo",
                module: "tests",
                input_type_hint: "()",
                output_type_hint: "u64",
                handler: |_ctx, _args| Ok(serde_json::Value::Null),
                description: None,
                arg_schema: None,
                response_schema: None,
            }
        }

        let plugin = HarvestPlugin::new()
            .workflows(vec![fake_workflow_info()])
            .updates(vec![fake_update()])
            .queries(vec![fake_query()]);
        assert_eq!(plugin.builder.update_handlers().len(), 1);
        assert_eq!(plugin.builder.update_handlers()[0].name, "approve");
        assert!(plugin.builder.update_handlers()[0].mcp);
        assert_eq!(plugin.builder.query_handlers().len(), 1);
        assert_eq!(plugin.builder.query_handlers()[0].name, "progress");
    }

    #[test]
    fn harvest_plugin_forwards_signals_to_builder_and_registry() {
        // Issue #610 gap-fill: `#[signal(..)]` handlers registered through the
        // plugin's fluent surface must reach both the builder AND the runtime
        // `HandlerRegistry` that the `GET /interface` route and the pre-enqueue
        // payload-validation gates read from. Without the `.signals()`
        // forwarding method this test fails to compile (the method does not
        // exist), and even if it did, a registry-level assertion catches a
        // forwarding method that drops the handler on the floor.
        fn fake_signal() -> autumn_harvest::SignalHandlerInfo {
            autumn_harvest::SignalHandlerInfo {
                name: "cancel",
                workflow: "echo",
                module: "tests",
                arg_type_hint: "CancelRequest",
                description: None,
                arg_schema: None,
            }
        }

        let plugin = HarvestPlugin::new()
            .workflows(vec![fake_workflow_info()])
            .signals(vec![fake_signal()]);

        // Builder level: the forwarding method appended the handler.
        assert_eq!(plugin.builder.signal_handlers().len(), 1);
        assert_eq!(plugin.builder.signal_handlers()[0].name, "cancel");
        assert_eq!(plugin.builder.signal_handlers()[0].workflow, "echo");

        // Runtime registry level: the same handler survives through
        // `build()` into the `HandlerRegistry` that the `GET /interface` route
        // (`runtime.registry.signal_handlers`) and the boundary gates consult.
        let (registry, _dags, _schedules, _worker) = plugin.builder.build().into_worker_parts();
        assert_eq!(registry.signal_handlers.len(), 1);
        assert_eq!(registry.signal_handlers[0].name, "cancel");
        assert_eq!(registry.signal_handlers[0].workflow, "echo");
    }

    #[test]
    fn harvest_plugin_forwards_retention_to_builder() {
        // Issue #355 review follow-up: `.retention(...)` had no dedicated
        // coverage of its own -- only the metrics_scrape_quickstart example
        // (Docker-requiring) exercised it. `try_build()` only validates
        // config in-memory (no DB connection), so this stays a fast unit test.
        let custom = autumn_harvest::retention::RetentionConfig {
            max_age_secs: Some(42),
            tick_interval_secs: 7,
            ..autumn_harvest::retention::RetentionConfig::default()
        };
        let plugin = HarvestPlugin::new().retention(custom);
        let built = plugin
            .builder
            .try_build()
            .expect("valid retention config should build");
        assert_eq!(built.retention().max_age_secs, Some(42));
        assert_eq!(built.retention().tick_interval_secs, 7);
    }

    // ── Connector build-time validation (issue #944) ──────────────────────
    //
    // These guard the two inline `assert!`s in `Plugin::build`, which are
    // otherwise only reachable by standing up a whole plugin. Both failures
    // are SILENT in production if they slip through: a stream mismatch makes
    // the consumer poll forever and deliver nothing, and an unsupported
    // broker-native mode re-reads a poison message forever.

    #[cfg(feature = "connectors")]
    #[test]
    fn a_binding_whose_stream_does_not_match_its_adapter_is_rejected() {
        assert!(binding_stream_matches_adapter("orders", "orders"));
        assert!(!binding_stream_matches_adapter("orders", "orders.v2"));
        assert!(!binding_stream_matches_adapter("orders", "payments"));
        // Length-equal but different, so a naive length check cannot pass.
        assert!(!binding_stream_matches_adapter("orders", "ordera"));
        assert!(binding_stream_matches_adapter("", ""));
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn two_registrations_sharing_one_source_are_rejected() {
        // Two receive loops over ONE client, each with its own OffsetTracker,
        // is the failure this catches: on Kafka one loop can commit a mark
        // covering offsets the other has not made durable, permanently
        // skipping them on a crash; on SQS the stream is split between the
        // bindings so neither target sees every message.
        assert_eq!(first_shared_source(&[1, 2, 3]), None);
        assert_eq!(first_shared_source(&[1, 2, 1]), Some((0, 2)));
        assert_eq!(first_shared_source(&[7, 7]), Some((0, 1)));
        // Reports the FIRST offending pair, so the panic names a stable pair
        // rather than whichever one the iteration order happened to reach.
        assert_eq!(first_shared_source(&[4, 4, 5, 5]), Some((0, 1)));
        assert_eq!(first_shared_source(&[]), None);
        assert_eq!(first_shared_source(&[9]), None);
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn two_sources_on_one_physical_subscription_are_rejected() {
        // Pointer identity only catches a *shared* `Arc`. Two separately
        // constructed sources over the same physical subscription have
        // distinct pointers and compete for the same messages, so each target
        // sees an arbitrary subset -- the same split the shared-`Arc` case
        // produces, reached a different way.
        let q = |s: &str| Some(s.to_string());
        assert_eq!(
            first_shared_identity(&[q("sqs:a"), q("sqs:b")]),
            None,
            "distinct subscriptions are the normal case"
        );
        assert_eq!(
            first_shared_identity(&[q("sqs:a"), q("sqs:a")]),
            Some((0, 1)),
            "two sources on one queue url must be rejected"
        );
        // Two Kafka consumers on ONE topic under DISTINCT group ids is the
        // sanctioned fan-out -- the existing panic message recommends it -- so
        // the identity must include the group, not just the topic.
        assert_eq!(
            first_shared_identity(&[q("kafka:g1/orders"), q("kafka:g2/orders")]),
            None,
            "distinct consumer groups on one topic are a legitimate fan-out"
        );
        assert_eq!(
            first_shared_identity(&[q("kafka:g1/orders"), q("kafka:g1/orders")]),
            Some((0, 1)),
            "two consumers in one group compete for the same partitions"
        );
        // An adapter that declares no identity is not evidence of a clash:
        // two `None`s must never match each other, or every pair of custom
        // adapters would be rejected on sight.
        assert_eq!(first_shared_identity::<String>(&[None, None]), None);
        assert_eq!(first_shared_identity(&[None, q("sqs:a")]), None);
        // Reports the FIRST offending pair, matching `first_shared_source`.
        assert_eq!(
            first_shared_identity(&[q("a"), q("a"), q("b"), q("b")]),
            Some((0, 1))
        );
        assert_eq!(first_shared_identity::<String>(&[]), None);
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn a_duplicate_stream_target_pair_is_warned_about_not_rejected() {
        // Rejecting this would break legitimate fan-in from two independent
        // brokers exposing the same stream name, which no operator can work
        // around (a Kafka source's `stream()` IS the topic, and the plugin
        // requires the binding to match it). It still double-dispatches on one
        // broker, so it is worth a warning -- which is what this drives.
        let p = |stream, wf, sig| (stream, wf, sig);
        assert_eq!(
            first_duplicate_stream_target(&[
                p("orders", "order_flow", None),
                p("orders", "order_flow", None),
            ]),
            Some((0, 1)),
        );
        // A different target on one stream is an ordinary fan-out.
        assert_eq!(
            first_duplicate_stream_target(&[
                p("orders", "order_flow", None),
                p("orders", "audit_flow", None),
            ]),
            None,
        );
        // The signal name is part of the target: two `signals_with_start`
        // bindings delivering DIFFERENT signals from one stream is normal.
        assert_eq!(
            first_duplicate_stream_target(&[
                p("orders", "cart", Some("add_item")),
                p("orders", "cart", Some("remove_item")),
            ]),
            None,
        );
        assert_eq!(
            first_duplicate_stream_target(&[
                p("orders", "cart", Some("add_item")),
                p("orders", "cart", Some("add_item")),
            ]),
            Some((0, 1)),
        );
        // Reports the FIRST offending pair, matching its siblings.
        assert_eq!(
            first_duplicate_stream_target(&[
                p("a", "w", None),
                p("a", "w", None),
                p("b", "w", None),
                p("b", "w", None),
            ]),
            Some((0, 1)),
        );
        assert_eq!(first_duplicate_stream_target(&[]), None);
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn a_duplicate_workflow_name_resolves_last_wins_everywhere() {
        // `HandlerRegistry` collapses the builder's `Vec<WorkflowInfo>` into a
        // `HashMap`, so a name registered twice resolves LAST-wins -- that is
        // the info the engine actually executes. Every connector path that
        // reads a target's info must agree, or the startup warning describes a
        // definition the runtime will never run: a first *throttled* definition
        // followed by a plain one makes the runtime dedupe on broker
        // coordinates while a first-wins lookup suppresses the 24h
        // claim-lifetime warning, leaving the operator unaware that a late
        // replay can start a second execution.
        let named = |name| WorkflowInfo {
            name,
            ..fake_workflow_info()
        };
        let throttled = named("order_flow").with_throttle(
            autumn_harvest::throttle::ThrottlePolicy::from_rate_str("100/m", None, None, None)
                .unwrap(),
        );
        let plain = named("order_flow");
        let other = named("cart");

        let registered = [throttled, plain, other];
        let map = workflow_infos_by_name(&registered);
        assert!(
            map["order_flow"].throttle.is_none(),
            "the LAST definition of a duplicated name must win, matching \
             HandlerRegistry -- a first-wins lookup would return the throttled one"
        );
        assert_eq!(map["cart"].name, "cart");
        assert_eq!(map.len(), 2, "a duplicated name occupies one entry");
        assert!(workflow_infos_by_name(&[]).is_empty());
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn a_signals_with_start_binding_is_bounded_by_retention_not_the_window() {
        use crate::connector::{ConnectorTarget, IdempotencyMode};
        use std::time::Duration;

        const STARTS: ConnectorTarget = ConnectorTarget::Starts {
            workflow: "order_flow",
        };
        const SIGNALS: ConnectorTarget = ConnectorTarget::SignalsWithStart {
            workflow: "cart",
            signal_name: "add_item",
        };
        let week = Duration::from_secs(7 * 24 * 60 * 60);

        // `Starts` reserves a `harvest_start_idempotency` row, so the window IS
        // the bound and an untuned one is the thing worth saying.
        assert_eq!(
            untuned_dedupe_bound(IdempotencyMode::BrokerCoordinates, STARTS, None, None),
            Some(UntunedDedupeBound::StartIdempotencyWindow),
        );
        assert_eq!(
            untuned_dedupe_bound(IdempotencyMode::BrokerCoordinates, STARTS, Some(week), None),
            None,
            "an explicit window is a considered one",
        );
        // Even a *shorter* explicit window is a deliberate choice.
        assert_eq!(
            untuned_dedupe_bound(
                IdempotencyMode::BrokerCoordinates,
                STARTS,
                Some(Duration::from_secs(60)),
                None,
            ),
            None,
        );
        // Retention is irrelevant to a `Starts` binding: its claim is its own
        // `harvest_start_idempotency` row, which outlives the execution.
        assert_eq!(
            untuned_dedupe_bound(
                IdempotencyMode::BrokerCoordinates,
                STARTS,
                Some(week),
                Some(Duration::from_secs(60)),
            ),
            None,
        );

        // `SignalsWithStart` persists its key ONLY on `harvest_signals`, which
        // is ON DELETE CASCADE on its execution. So retention deleting the run
        // deletes the claim, and the window does nothing for this path -- which
        // is exactly why tuning it must NOT silence the warning.
        assert_eq!(
            untuned_dedupe_bound(
                IdempotencyMode::BrokerCoordinates,
                SIGNALS,
                Some(week),
                Some(Duration::from_secs(3 * 24 * 60 * 60)),
            ),
            Some(UntunedDedupeBound::ExecutionRetention {
                max_age: Duration::from_secs(3 * 24 * 60 * 60),
            }),
            "a tuned window must not silence the retention bound it does not govern",
        );

        // Retention is OFF by default, and then the signal row -- and with it
        // the claim -- outlives every window. Nothing to warn about.
        assert_eq!(
            untuned_dedupe_bound(IdempotencyMode::BrokerCoordinates, SIGNALS, None, None),
            None,
            "with retention off the claim is unbounded, which is stronger, not weaker",
        );

        // `WorkflowId` rides neither *claim table*, but that does not make it
        // unbounded -- see `a_workflow_id_binding_is_bounded_by_retention_too`.
        assert_eq!(
            untuned_dedupe_bound(IdempotencyMode::WorkflowId, STARTS, None, None),
            None,
            "with retention off nothing deletes the execution row, so the attach is unbounded",
        );
    }

    /// `WorkflowId` dedupe is bounded by retention, not by nothing.
    ///
    /// The dispatcher passes `idempotency_key: None` in this mode, so dedupe
    /// rests entirely on the `(workflow_name, workflow_id)` reuse-policy attach
    /// -- resolved against the **execution row**. Retention deleting that row
    /// deletes the only record that a redelivery was already handled, so the
    /// next one starts a fresh run. `start_idempotency_window` governs a claim
    /// table this path never touches, so naming it would be the wrong remedy.
    #[cfg(feature = "connectors")]
    #[test]
    fn a_workflow_id_binding_is_bounded_by_retention_too() {
        use crate::connector::{ConnectorTarget, IdempotencyMode};
        use std::time::Duration;

        const STARTS: ConnectorTarget = ConnectorTarget::Starts {
            workflow: "order_flow",
        };
        let three_days = Duration::from_secs(3 * 24 * 60 * 60);
        let week = Duration::from_secs(7 * 24 * 60 * 60);

        assert_eq!(
            untuned_dedupe_bound(IdempotencyMode::WorkflowId, STARTS, None, Some(three_days)),
            Some(UntunedDedupeBound::ExecutionRetention {
                max_age: three_days,
            }),
            "an execution-row attach is bounded by how long that row survives",
        );

        // Same reason the SignalsWithStart case reports through a tuned window:
        // this knob does not govern the execution row at all.
        assert_eq!(
            untuned_dedupe_bound(
                IdempotencyMode::WorkflowId,
                STARTS,
                Some(week),
                Some(three_days),
            ),
            Some(UntunedDedupeBound::ExecutionRetention {
                max_age: three_days,
            }),
            "a tuned start-idempotency window must not silence a bound it does not govern",
        );
    }

    #[cfg(feature = "connectors")]
    #[test]
    fn connector_target_workflow_resolution_is_last_wins_like_the_registry() {
        // `spawn_connectors` resolves a binding's target `WorkflowInfo` from a
        // last-wins map because that is what `HandlerRegistry` itself does
        // when it collapses the builder's `Vec` into a `HashMap`. A first-wins
        // `.find()` would hand the connector a DIFFERENT info than the engine
        // will actually execute -- e.g. an un-throttled one -- silently
        // resolving the wrong `IdempotencyMode` and defeating the very
        // backpressure the operator configured.
        let mut plain = fake_workflow_info();
        plain.name = "dup";
        let mut throttled = fake_workflow_info();
        throttled.name = "dup";
        throttled.throttle = Some(autumn_harvest::throttle::ThrottlePolicy {
            refill_per_sec: 1.0,
            burst: 1.0,
            key_expr: None,
            schedule_to_start: None,
        });

        let workflows = [plain, throttled];
        let by_name: std::collections::HashMap<&str, &WorkflowInfo> =
            workflows.iter().map(|w| (w.name, w)).collect();
        let resolved = by_name.get("dup").copied().expect("registered");

        assert!(
            resolved.throttle.is_some(),
            "last-wins must win: the engine executes the LAST registration, so \
             the connector must resolve its idempotency mode from that one",
        );
        // ...and that really does change the mode, so the distinction matters.
        assert_eq!(
            crate::connector::resolve_idempotency_mode(
                crate::connector::ConnectorTarget::Starts { workflow: "dup" },
                None,
                Some(resolved),
            ),
            crate::connector::IdempotencyMode::WorkflowId,
            "a deferred-admission target cannot carry a keyed start",
        );
        assert_eq!(
            crate::connector::resolve_idempotency_mode(
                crate::connector::ConnectorTarget::Starts { workflow: "dup" },
                None,
                Some(&workflows[0]),
            ),
            crate::connector::IdempotencyMode::BrokerCoordinates,
            "the FIRST registration would have resolved differently -- which is \
             exactly the silent bug last-wins resolution prevents",
        );
    }
}
