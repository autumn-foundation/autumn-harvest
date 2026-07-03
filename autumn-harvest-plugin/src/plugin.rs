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
pub struct HarvestPlugin {
    builder: HarvestBuilder,
    api_path: Option<String>,
    api_middleware: Option<ApiMiddlewareFn>,
    /// Register MCP tool routes for `#[workflow(mcp)]` workflows (issue #597).
    /// Set via [`Self::mcp_tools`] / [`Self::mcp_tools_at`] (feature `mcp`).
    mcp_tools_enabled: bool,
    /// Optional prefix override for the generated MCP tool routes.
    mcp_tools_prefix: Option<String>,
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
            mcp_tools_enabled: false,
            mcp_tools_prefix: None,
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

    /// Mount the Harvest management API under `path`.
    #[must_use]
    pub fn api(mut self, path: impl Into<String>) -> Self {
        self.api_path = Some(path.into());
        self
    }

    /// Mount the Harvest management API under `path`, protected by the given
    /// tower middleware layer.
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
        self.api_middleware = Some(Box::new(move |router| router.layer(middleware)));
        self
    }

    /// Expose every `#[workflow(mcp)]` workflow as MCP tools (issue #597).
    ///
    /// Generates typed `start_{wf}` / `{wf}_status` / `signal_{wf}` /
    /// `{wf}_watch` routes (plus one `{wf}_update_{name}` per
    /// `#[update(workflow = "…", mcp)]` handler) under `{api_path}/mcp`
    /// (default `/api/harvest/mcp`) and registers them as app-level routes so
    /// autumn-web's `AppBuilder::mount_mcp("/mcp")` projects them into the MCP
    /// tool catalog. The app author still mounts (and secures, via
    /// `secure_mcp`) the MCP endpoint itself.
    ///
    /// Opt-in only: workflows without the `mcp` attribute never surface, and
    /// the mutating tools are never part of autumn-web's read-only
    /// `expose_all_as_mcp` hatch.
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
}

impl Plugin for HarvestPlugin {
    fn build(self, app: AppBuilder) -> AppBuilder {
        let Self {
            builder,
            api_path,
            api_middleware,
            mcp_tools_enabled,
            mcp_tools_prefix,
        } = self;
        #[cfg(not(feature = "mcp"))]
        let _ = (mcp_tools_enabled, mcp_tools_prefix);

        let api_state = HarvestApiState::new();

        // Issue #597: generate the MCP tool routes before the builder is
        // stashed in the runtime slot. These are app-level typed routes
        // (registered via `AppBuilder::routes`, not `nest`) so autumn-web's
        // `mount_mcp` can project them into the tool catalog; handlers fail
        // closed until `on_startup` installs the runtime.
        #[cfg(feature = "mcp")]
        let mcp_routes = if mcp_tools_enabled {
            let prefix =
                crate::mcp_tools::tools_prefix(api_path.as_deref(), mcp_tools_prefix.as_deref());
            let descriptors = crate::mcp_tools::collect_descriptors(
                builder.workflow_infos(),
                builder.update_handlers(),
            );
            crate::mcp_tools::record_schemas(&descriptors);
            Some(crate::mcp_tools::build_mcp_tool_routes(
                &prefix,
                &descriptors,
                &api_state,
            ))
        } else {
            None
        };

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
                    let res = start_harvest_runtime(&state, &slot, &api_state).await;
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

        if let Some(path) = api_path {
            let ui_router = harvest_ui_router(api_state.clone());
            let mut router = harvest_api_router(api_state).nest("/ui", ui_router);
            if let Some(mw) = api_middleware {
                router = mw(router);
            }
            app.nest(&path, router)
        } else {
            app
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
    // Propagate the GET /admin/usage window ceiling (issue #596).
    api_state.set_usage_window_ceiling(built.usage_window_ceiling);
    // Propagate the GET /admin/usage group-count cap (issue #596).
    api_state.set_usage_max_groups(built.usage_max_groups);
    // Propagate batch start caps from builder config (issue #357).
    api_state.set_batch_start_config(&built.batch_start_config);

    // Apply the api_state audit retention override only when explicitly set,
    // so that builder-level retention config is not silently clobbered.
    if let Some(days) = api_state.audit_retention_days() {
        built.set_audit_retention_days(days);
    }

    state.insert_extension(harvest_config.outbox.clone());
    state.insert_extension(router.clone());
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
    .with_max_workflow_input_bytes(max_workflow_input_bytes)
    .with_max_workflow_execution_timeout(max_workflow_execution_timeout)
    .with_max_workflow_start_delay(max_workflow_start_delay)
    .with_max_signal_payload_bytes(max_signal_payload_bytes)
    .with_query_timeout(query_timeout)
    .with_history_policy(runner.api_runtime().registry().history_policy())
    .with_default_debounce_max_wait(default_debounce_max_wait)
    .with_max_workflow_attempts(max_workflow_attempts);
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

                let (owner, runbook_url, severity, info_sla, info_retry_policy) = state
                    .extension::<std::sync::Arc<autumn_harvest::worker::HandlerRegistry>>()
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
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
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
                    };

                    let Some(harvest_db) = harvest_db else {
                        return Err(autumn_web::error::AutumnError::internal_server_error_msg(
                            "HarvestDbPool not found on AppState extensions",
                        ));
                    };
                    let pool = harvest_db.pool_for(shard).clone();
                    let mut conn = pool.get().await.map_err(|e| {
                        autumn_web::error::AutumnError::internal_server_error_msg(e.to_string())
                    })?;

                    client
                        .start_or_load(&mut conn, start_params)
                        .await
                        .map_err(|e| {
                            autumn_web::error::AutumnError::internal_server_error_msg(format!(
                                "failed to start Harvest webhook workflow: {e}"
                            ))
                        })?;

                    Ok(())
                })
                    as std::pin::Pin<
                        Box<dyn std::future::Future<Output = autumn_web::AutumnResult<()>> + Send>,
                    >
            },
        );
        state.insert_extension(autumn_web::webhook_outbound::WebhookDelegateExt(delegate));
    }

    api_state.install_storage_pool(harvest_db_pool.clone());

    // issue #377: boot-time gate load — populate the cache before any traffic hits.
    if let Ok(mut boot_conn) = acquire_conn(harvest_db_pool.default_pool()).await {
        match autumn_harvest::admission_gate::db::load_active_gates(&mut boot_conn).await {
            Ok(gates) => {
                api_state.gate_cache().refresh(gates);
                tracing::debug!("admission gate cache populated at startup");
            }
            Err(e) => tracing::warn!(error = %e, "could not load admission gates at startup"),
        }
    }

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

    {
        let mut guard = slot.lock().expect("harvest lock poisoned");
        guard.runtime = Some(HarvestRuntime {
            runner,
            outbox,
            gate_refresh,
        });
    }

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
        api_state.clear();
        return;
    };

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
    runtime.runner.stop().await;
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
            concurrency: None,

            debounce: None,
            batch: None,
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
        }
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
}
