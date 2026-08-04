//! `HarvestPlugin` — the [`Plugin`] implementation that wires
//! the Harvest workflow engine into an Autumn [`AppBuilder`].

use std::any::Any;
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
const OUTBOX_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

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
}

/// Plugin-local shared slot: holds the pre-built `HarvestBuilder` until the
/// first `on_startup` call consumes it, then holds the running `HarvestRuntime`
/// until `on_shutdown` stops it.
#[derive(Default)]
struct HarvestRuntimeSlot {
    builder: Option<HarvestBuilder>,
    runtime: Option<HarvestRuntime>,
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
    /// Built-in Prometheus scrape endpoint (issue #355). Set via
    /// [`Self::with_metrics_scrape`] (feature `metrics`).
    #[cfg(feature = "metrics")]
    metrics_scrape_enabled: bool,
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
    /// `token:{id}`. A bearer that does **not** begin with `hvst_` (or an absent
    /// bearer) is passed through untouched, so token auth **composes with** — it
    /// never replaces — an embedder's own `api_with_auth` middleware (AC7).
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
            #[cfg(feature = "metrics")]
            metrics_scrape_enabled,
        } = self;
        #[cfg(not(feature = "mcp"))]
        let _ = (mcp_tool_middleware, mcp_tools_enabled, mcp_tools_prefix);

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
    ensure_runtime_migrations(state.profile(), &app_config, &harvest_config)?;

    let runtime_state = state.clone();
    let app_pool = state.pool().cloned();
    let harvest_pool = resolve_harvest_pool(state, &harvest_config)?;
    let router = ShardRouter::single();

    let (builder, runtime_already_started) = {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        (guard.builder.take(), guard.runtime.is_some())
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
    .with_workflows(runner.api_runtime().registry().workflows.values().cloned())
    .with_max_workflow_input_bytes(max_workflow_input_bytes)
    .with_max_workflow_execution_timeout(max_workflow_execution_timeout)
    .with_max_workflow_chain_timeout(max_workflow_chain_timeout)
    .with_max_workflow_start_delay(max_workflow_start_delay)
    .with_max_signal_payload_bytes(max_signal_payload_bytes)
    .with_query_timeout(query_timeout)
    .with_history_policy(runner.api_runtime().registry().history_policy())
    .with_default_debounce_max_wait(default_debounce_max_wait)
    .with_max_workflow_attempts(max_workflow_attempts)
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

    {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        guard.runtime = Some(HarvestRuntime {
            runner,
            outbox,
            gate_refresh,
        });
    }

    // Startup succeeded — keep the published admission globals (issue #618,
    // F-round7); `stop_harvest_runtime` clears them (ptr-eq guarded) on shutdown.
    admission_guard.commit();
    Ok(())
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

fn ensure_runtime_migrations(
    profile: &str,
    app_config: &AutumnConfig,
    harvest_config: &HarvestRuntimeConfig,
) -> autumn_web::AutumnResult<()> {
    if let Some(app_database_url) = app_config.database.url.as_deref() {
        apply_migrations_for_profile(
            profile,
            app_database_url,
            OUTBOX_MIGRATIONS,
            "Harvest workflow outbox",
        )?;
    }

    let harvest_database_url = match harvest_config.mode {
        HarvestMode::Embedded => app_config.database.url.as_deref().ok_or_else(|| {
            AutumnError::service_unavailable_msg(
                "autumn-harvest requires database.url when harvest.mode is embedded",
            )
        })?,
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

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
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
}
