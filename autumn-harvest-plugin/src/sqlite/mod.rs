//! `HarvestPlugin` on the SQLite backend (issue #2467, epic #2466).
//!
//! The `sqlite` feature is a backend flip, like `autumn-web/sqlite`, which it
//! turns on. Under it `HarvestPlugin::build` takes this path, not the Postgres
//! one:
//!
//! - It registers no migration set. The Postgres sets would fail on the
//!   app's SQLite database. `autumn-harvest-sqlite` applies its own schema
//!   when it opens its file.
//! - Harvest storage is its own file, next to the app database: `app.db`
//!   gives `app.harvest.db`. `harvest.sqlite.path` overrides it. The
//!   runtime holds an exclusive writer lock on that file, so it cannot share
//!   the app's pooled file.
//! - One driver task owns the runtime. See [`driver`].
//! - The management API serves the core routes. See [`api`].
//! - API tokens, roles, OIDC, the authorizer and the rate limiter run the
//!   admin-auth layer stack of the Postgres path (issue #2469). Tokens and
//!   audit rows live in the Harvest file.
//! - The driver fires schedules and runs retention (issue #2471). See
//!   [`ops`] for the admin routes.
//! - MCP tools and tasks, webhook receivers and broker connectors start runs
//!   through the [`HarvestSqlite`] handle (issue #2471).
//!
//! Every builder option and mode that this slice cannot honour fails the
//! build or the boot, and names the issue that ports it. A silent no-op would
//! let an app believe a protection or a feature is on.

mod api;
mod driver;
mod fleet;
mod handle;
#[cfg(feature = "mcp")]
pub(crate) mod mcp;
mod ops;
mod reads;
#[cfg(any(feature = "webhooks", feature = "connectors"))]
pub(crate) mod triggers;
mod ui;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use autumn_harvest::builder::HarvestBuilder;
use autumn_web::AppState;
use autumn_web::app::AppBuilder;
use autumn_web::error::AutumnError;

pub use driver::{RoutedSignal, RoutedUpdate, SignalWithStartParams, StopAck};
pub use fleet::{HarvestFleet, HarvestTenant, tenant_harvest_file};
pub use handle::{HarvestSqlite, HarvestSqliteError};

/// The `autumn-harvest-sqlite` types that [`HarvestSqlite`] takes and returns.
pub use autumn_harvest_sqlite::{
    AdmittedUpdate, ExecutionDescription, ExecutionFilter, ExecutionPage, ExecutionSummary,
    HistoryFilter, HistoryPage, ListCursor, PagedEvent, SignalDelivery, SignalWithStartOutcome,
    SqliteError, StartOptions, StartOutcome, Stored,
};

use crate::api::HarvestApiState;
use crate::config::{HarvestMode, HarvestRuntimeConfig, HarvestSqliteConfig};
use driver::{Driver, Registry, Retention};

/// The epic that tracks every surface this slice does not port.
pub(crate) const EPIC_ISSUE: u32 = 2466;

/// The plugin settings the SQLite path reads.
pub(crate) struct SqliteParts {
    pub builder: HarvestBuilder,
    pub api_path: Option<String>,
    pub api_middleware: Option<crate::plugin::ApiMiddlewareFn>,
    pub allow_unauthenticated_mutations: bool,
    /// The Harvest file that the app chose with `HarvestPlugin::sqlite_path`.
    pub path: Option<PathBuf>,
    /// One Harvest per open fleet tenant (issue #2472).
    pub per_tenant: bool,
    /// The admin-auth layers (issue #2469). They run as on Postgres.
    pub auth: crate::api::AdminAuthLayers,
    /// The MCP tools and tasks (issue #2471). The build fills in the roles,
    /// the login and the API path.
    #[cfg(feature = "mcp")]
    pub mcp: crate::plugin::McpMount,
    /// The webhook receivers (issue #2471).
    #[cfg(feature = "webhooks")]
    pub webhooks: Vec<autumn_harvest::webhook_trigger::WebhookTriggerInfo>,
    /// The broker connectors (issue #2471).
    #[cfg(feature = "connectors")]
    pub connectors: Vec<crate::plugin::ConnectorRegistration>,
    /// The OIDC login (issue #2469).
    #[cfg(feature = "oidc")]
    pub oidc: Option<crate::oidc::OidcLogin>,
    /// Each option that was set but has no SQLite port: its name and issue.
    pub refused: Vec<(&'static str, u32)>,
}

/// The state the startup hook fills and the shutdown hook drains.
#[derive(Default)]
struct Slot {
    parts: Option<StartParts>,
    driver: Option<Driver>,
    /// The path from `HarvestPlugin::sqlite_path`, if any.
    code_path: Option<PathBuf>,
    /// Whether to run one Harvest per open fleet tenant.
    per_tenant: bool,
    /// The per-tenant Harvests. The shutdown hook releases them.
    fleet: Option<HarvestFleet>,
    /// The broker connectors. The startup hook starts them after the
    /// driver. The shutdown hook stops them before it (issue #2471).
    #[cfg(feature = "connectors")]
    connectors: Vec<crate::plugin::ConnectorRegistration>,
    /// The running connector loops.
    #[cfg(feature = "connectors")]
    connector_loops: Vec<crate::plugin::ConnectorRuntimeHandle>,
}

struct StartParts {
    workflows: Vec<autumn_harvest::info::WorkflowInfo>,
    activities: Vec<autumn_harvest::info::ActivityInfo>,
    updates: Vec<autumn_harvest::UpdateHandlerInfo>,
    /// The declarative `#[signal]` handlers, for their payload schemas.
    signals: Vec<autumn_harvest::SignalHandlerInfo>,
    /// The declarative `#[query]` handlers (issue #2468).
    queries: Vec<autumn_harvest::QueryHandlerInfo>,
    /// The durable log sink policy (issue #2468).
    log_policy: Option<autumn_harvest::context::WorkflowLogPolicy>,
    /// The days to keep an audit row, as on Postgres. Zero keeps it.
    audit_retention_days: i64,
    /// The code schedules (issue #2471).
    schedules: Vec<autumn_harvest::policy::WorkflowSchedule>,
    /// `HarvestPlugin::retention` (issue #2471).
    retention: autumn_harvest::retention::RetentionConfig,
    /// The `HarvestPlugin::state` values. The startup hook adds `AppState`.
    state: autumn_harvest::context::SharedStateMap,
}

/// Build the plugin on the SQLite backend.
///
/// # Panics
///
/// Panics when an option or a mode has no SQLite port. The message names it
/// and its issue. The Postgres path panics on a bad registration the same
/// way, so a misconfigured app never starts.
pub(crate) fn build(app: AppBuilder, parts: SqliteParts) -> AppBuilder {
    let SqliteParts {
        mut builder,
        api_path,
        api_middleware,
        allow_unauthenticated_mutations,
        path: code_path,
        per_tenant,
        auth,
        #[cfg(feature = "mcp")]
        mut mcp,
        #[cfg(feature = "webhooks")]
        webhooks,
        #[cfg(feature = "connectors")]
        connectors,
        #[cfg(feature = "oidc")]
        oidc,
        mut refused,
    } = parts;

    refused.extend(builder_refusals(&builder));
    if let Some(message) = refusal_message(&refused) {
        panic!("{message}");
    }
    // Fail now on a bad `[harvest.sqlite]`, not at the first request.
    HarvestSqliteConfig::load()
        .unwrap_or_else(|error| panic!("cannot load the Harvest SQLite configuration: {error}"));

    #[cfg(feature = "connectors")]
    crate::plugin::validate_connectors(&connectors, &builder);

    let app = app.config_section(crate::config::HARVEST_CONFIG_ROOT);

    let start_parts = StartParts {
        workflows: builder.workflow_infos().to_vec(),
        activities: builder.take_activities(),
        updates: builder.update_handlers().to_vec(),
        signals: builder.signal_handlers().to_vec(),
        queries: builder.query_handlers().to_vec(),
        log_policy: builder.workflow_log_policy(),
        state: builder.take_state(),
        audit_retention_days: audit_retention_days(builder.retention_config()),
        schedules: builder.workflow_schedules().to_vec(),
        retention: builder.retention_config().clone(),
    };

    // A login declares the boundary, as on Postgres.
    #[cfg(feature = "oidc")]
    let boundary = api_middleware.is_some() || oidc.is_some();
    #[cfg(not(feature = "oidc"))]
    let boundary = api_middleware.is_some();
    let api_state = sqlite_api_state(allow_unauthenticated_mutations, boundary);
    // The roles of a login replace those of `with_roles`, as on Postgres.
    #[cfg(feature = "oidc")]
    let auth = crate::api::AdminAuthLayers {
        roles: oidc
            .as_ref()
            .map(|login| login.roles().clone())
            .or(auth.roles),
        ..auth
    };
    let handles = Handles::default();

    // The trigger routes are app-level routes, as on Postgres. Each one
    // reaches the handle through `api_state` (issue #2471).
    #[cfg(feature = "webhooks")]
    let app = if webhooks.is_empty() {
        app
    } else {
        app.routes(crate::webhook_receiver::build_webhook_routes(
            &webhooks,
            builder.workflow_infos(),
            builder.dag_infos(),
            &api_state,
        ))
    };
    #[cfg(feature = "mcp")]
    let app = {
        mcp.roles.clone_from(&auth.roles);
        #[cfg(feature = "oidc")]
        mcp.oidc.clone_from(&oidc);
        mcp.api_path.clone_from(&api_path);
        match crate::plugin::build_mcp_routes(mcp, &builder, &api_state) {
            Some(routes) => app.routes(routes),
            None => app,
        }
    };

    let slot = Arc::new(Mutex::new(Slot {
        parts: Some(start_parts),
        driver: None,
        code_path,
        per_tenant,
        fleet: None,
        #[cfg(feature = "connectors")]
        connectors,
        #[cfg(feature = "connectors")]
        connector_loops: Vec::new(),
    }));
    let app = lifecycle_hooks(app, slot, &handles, &api_state);

    let Some(path) = api_path else {
        return app;
    };
    let router = api_router(
        handles,
        &api_state,
        &auth,
        api_middleware,
        #[cfg(feature = "oidc")]
        oidc.as_ref(),
    );
    // `AppBuilder::nest` takes a `Router<AppState>`. No handler reads it.
    app.nest(&path, router.with_state(()))
}

/// The API state of a SQLite app.
fn sqlite_api_state(allow_unauthenticated_mutations: bool, boundary: bool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    // MCP, webhook and connector starts go through the SQLite handle
    // (issue #2471). Before startup they answer `503`.
    api_state.set_sqlite_backend();
    api_state.set_allow_unauthenticated_mutations(allow_unauthenticated_mutations);
    api_state.set_admin_auth_boundary(boundary);
    // The SQLite store keeps no run tenant, so tenant binding fails closed.
    api_state.set_no_run_tenants();
    api_state
}

/// Register the startup and the shutdown hook.
fn lifecycle_hooks(
    app: AppBuilder,
    slot: Arc<Mutex<Slot>>,
    handles: &Handles,
    api_state: &HarvestApiState,
) -> AppBuilder {
    let startup_slot = Arc::clone(&slot);
    let startup_handles = handles.clone();
    let startup_api_state = api_state.clone();
    app.on_startup(move |state| {
        let slot = Arc::clone(&startup_slot);
        let handles = startup_handles.clone();
        let api_state = startup_api_state.clone();
        async move { start(&state, &slot, &handles, &api_state).await }
    })
    .on_shutdown(move || {
        let slot = Arc::clone(&slot);
        async move { stop(&slot).await }
    })
}

/// Stop the connectors, then release the fleet, then stop the driver.
///
/// The connectors stop first, so no dispatch meets a stopped driver.
async fn stop(slot: &Mutex<Slot>) {
    #[cfg(feature = "connectors")]
    {
        let loops = std::mem::take(
            &mut slot
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .connector_loops,
        );
        crate::plugin::stop_connectors(loops).await;
    }
    let (driver, fleet) = {
        let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        (slot.driver.take(), slot.fleet.take())
    };
    if let Some(fleet) = fleet {
        fleet.release();
    }
    if let Some(driver) = driver {
        driver.shutdown().await;
    }
}

/// The management router with its auth (issue #2469).
///
/// The one layer stack of both backends wraps the routes, and the OIDC
/// session boundary wraps the stack, as on Postgres. Host auth runs outside
/// them. A login and host auth are alternatives, so one of them is `None`.
fn api_router(
    handles: Handles,
    api_state: &HarvestApiState,
    auth: &crate::api::AdminAuthLayers,
    api_middleware: Option<crate::plugin::ApiMiddlewareFn>,
    #[cfg(feature = "oidc")] oidc: Option<&crate::oidc::OidcLogin>,
) -> autumn_web::reexports::axum::Router<()> {
    // One limiter for the control and the tenant routes, so a client gets
    // one budget.
    let limiter = auth
        .rate_limit
        .clone()
        .map(crate::api_rate_limit::ApiRateLimiter::new);
    // The OIDC session boundary joins the stack, inside the tenant nest too.
    // It then sees the core path, so a tenant health route stays public.
    let stack = |router| {
        let router =
            crate::api::apply_admin_auth_layers_with(router, api_state, auth, limiter.clone());
        #[cfg(feature = "oidc")]
        let router = match oidc {
            Some(login) => crate::oidc::session_boundary(router, login, auth.api_tokens),
            None => router,
        };
        router
    };
    let mut router = api::router(
        api::ApiState {
            handle: handles.control,
            fleet: handles.fleet,
            flags: api_state.clone(),
        },
        &stack,
    );
    if let Some(middleware) = api_middleware {
        router = middleware(router);
    }
    // The login routes sit once, at the top, outside the boundary.
    #[cfg(feature = "oidc")]
    if let Some(login) = oidc {
        router = router.merge(crate::oidc::login_router(login.clone()));
    }
    router
}

/// The options set on the builder that have no SQLite port.
fn builder_refusals(builder: &HarvestBuilder) -> Vec<(&'static str, u32)> {
    let mut refused = Vec::new();
    if !builder.dag_infos().is_empty() {
        // DAGs are an engine non-goal of the SQLite backend (#2019).
        refused.push(("HarvestPlugin::dags", 2019));
    }
    refused.extend(retention_refusals(builder.retention_config()));
    // The SQLite worker reads no cost hold, so a held type would run
    // (autumn#1720).
    if builder.worker_config().cost_hold.is_some() {
        refused.push(("WorkerConfig::with_cost_hold", EPIC_ISSUE));
    }
    refused
}

/// The days to keep an audit row. A dry run keeps every row.
const fn audit_retention_days(config: &autumn_harvest::retention::RetentionConfig) -> i64 {
    if config.dry_run {
        0
    } else {
        config.audit_retention_days
    }
}

/// The retention fields that this backend cannot honour (issue #2471).
///
/// The driver honours `max_age_secs`, `overrides`, `dry_run` and
/// `tick_interval_secs`. The audit store honours `audit_retention_days`.
/// These fields need a store that SQLite does not have:
///
/// - `tenant_overrides`: no SQLite run has a tenant.
/// - `summary`: no summary table, so a purge would lose the run.
/// - `payload_orphan_grace_secs`: no payload store.
///
/// The other fields tune Postgres machinery, such as partitions and the
/// task-queue janitor. They change nothing here.
fn retention_refusals(
    config: &autumn_harvest::retention::RetentionConfig,
) -> Vec<(&'static str, u32)> {
    [
        (
            "RetentionConfig::tenant_overrides",
            !config.tenant_overrides.is_empty(),
        ),
        ("RetentionConfig::summary", config.summary.is_some()),
        (
            "RetentionConfig::payload_orphan_grace_secs",
            config.payload_orphan_grace_secs.is_some(),
        ),
    ]
    .into_iter()
    .filter_map(|(field, set)| set.then_some((field, EPIC_ISSUE)))
    .collect()
}

/// The panic message for a refused option, or `None` when there is none.
fn refusal_message(refused: &[(&'static str, u32)]) -> Option<String> {
    if refused.is_empty() {
        return None;
    }
    let list = refused
        .iter()
        .map(|(option, issue)| format!("{option} (#{issue})"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "HarvestPlugin on the SQLite backend does not support: {list}. Remove the option, or \
         build on Postgres. The SQLite plugin epic is autumn-harvest#{EPIC_ISSUE}"
    ))
}

/// The plugin path that serves a mode on a SQLite app (issue #2472).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Backend {
    /// Harvest in its own SQLite file. See [`build`].
    Sqlite,
    /// Harvest in a Postgres database: the Postgres path, with its own pool.
    Postgres,
}

/// Embedded mode keeps Harvest next to the app, so it runs on SQLite. Split
/// and external mode keep Harvest in a Postgres database.
pub(crate) const fn backend_for(mode: HarvestMode) -> Backend {
    match mode {
        HarvestMode::Embedded => Backend::Sqlite,
        HarvestMode::Split | HarvestMode::External => Backend::Postgres,
    }
}

/// The refusal of a SQLite-only setting in split or external mode, or
/// `None`.
///
/// Neither setting has an effect on a Postgres Harvest database. A refusal
/// keeps an operator from believing that the setting works.
pub(crate) fn split_refusal(
    mode: HarvestMode,
    per_tenant: bool,
    configured_path: Option<&str>,
) -> Option<String> {
    let mut set = Vec::new();
    if configured_path.is_some() {
        set.push("harvest.sqlite.path");
    }
    if per_tenant {
        set.push("HarvestPlugin::sqlite_per_tenant");
    }
    if set.is_empty() {
        return None;
    }
    Some(format!(
        "harvest.mode = {mode:?} keeps Harvest in Postgres, so these SQLite settings have no \
         effect: {}. Remove them, or use harvest.mode = \"embedded\" (autumn-harvest#2472)",
        set.join(", ")
    ))
}

/// Where the Harvest file goes (issue #2467).
///
/// `harvest.sqlite.path` wins, then the path the app set in code with
/// `HarvestPlugin::sqlite_path`. Otherwise the app's primary database URL
/// gives the directory and the stem: `sqlite:///srv/app.db` gives
/// `/srv/app.harvest.db`. In a fleet app that URL is the control database.
///
/// # Errors
///
/// Returns a message when neither source names a file. An in-memory app
/// database has no file to sit next to.
pub(crate) fn resolve_path(
    configured: Option<&str>,
    code: Option<&Path>,
    app_url: Option<&str>,
) -> Result<PathBuf, String> {
    if let Some(path) = configured {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = code {
        return Ok(path.to_path_buf());
    }
    let Some(url) = app_url else {
        return Err("the SQLite Harvest backend needs harvest.sqlite.path, \
             HarvestPlugin::sqlite_path, or a file-backed database.url to derive it from"
            .to_owned());
    };
    let target = sqlite_file_of(url).ok_or_else(|| {
        format!(
            "database.url {url:?} names no SQLite file, so the Harvest file cannot be placed \
             next to it. Set harvest.sqlite.path"
        )
    })?;
    Ok(sibling_harvest_file(&target))
}

/// The file a SQLite URL names, or `None` for an in-memory database.
///
/// It accepts the spellings autumn-web accepts: `sqlite:///path`,
/// `sqlite://path`, `sqlite:path` and `file:path?query`.
fn sqlite_file_of(url: &str) -> Option<PathBuf> {
    let rest = if let Some(uri) = url.strip_prefix("file:") {
        if uri.contains("mode=memory") {
            return None;
        }
        uri.split('?').next().unwrap_or(uri)
    } else {
        url.strip_prefix("sqlite://")
            .or_else(|| url.strip_prefix("sqlite:"))
            .unwrap_or(url)
    };
    if rest.is_empty() || rest.starts_with(":memory:") {
        return None;
    }
    Some(PathBuf::from(rest))
}

/// `dir/app.db` gives `dir/app.harvest.db`. `dir/app` gives
/// `dir/app.harvest.db`.
fn sibling_harvest_file(app_file: &Path) -> PathBuf {
    let stem = app_file
        .file_stem()
        .map_or_else(|| "app".into(), |stem| stem.to_string_lossy().into_owned());
    app_file.with_file_name(format!("{stem}.harvest.db"))
}

/// The handles that the startup hook fills and the API reads.
#[derive(Clone, Default)]
struct Handles {
    /// The control Harvest.
    control: Arc<OnceLock<HarvestSqlite>>,
    /// The per-tenant Harvests, when `sqlite_per_tenant` is on.
    fleet: Arc<OnceLock<HarvestFleet>>,
}

async fn start(
    state: &AppState,
    slot: &Arc<Mutex<Slot>>,
    handles: &Handles,
    api_state: &HarvestApiState,
) -> autumn_web::AutumnResult<()> {
    api_state.set_deployment_profile(state.profile().to_string());
    api_state.set_admin_auth_session_key(state.auth_session_key());
    warn_if_open(state.profile(), api_state);

    let runtime_config = HarvestRuntimeConfig::load()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    if backend_for(runtime_config.mode) != Backend::Sqlite {
        // The build read `embedded`. A mode that changed since then would
        // need the Postgres path, which this build did not mount.
        return Err(AutumnError::service_unavailable_msg(format!(
            "harvest.mode changed to {:?} after the build. Restart the app",
            runtime_config.mode
        )));
    }
    if !runtime_config.worker_enabled {
        // One process holds the writer lock. A process with no driver could
        // not write at all, so an API-only replica is not a SQLite topology.
        return Err(AutumnError::service_unavailable_msg(
            "harvest.worker_enabled = false is not supported on the SQLite backend: the app \
             process is the only writer, so it always runs the driver",
        ));
    }
    let sqlite_config = HarvestSqliteConfig::load()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    let app_config = state.config();
    let (parts, code_path, per_tenant) = {
        let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        (slot.parts.take(), slot.code_path.clone(), slot.per_tenant)
    };
    let Some(parts) = parts else {
        // A second startup in one process keeps the running driver.
        return Ok(());
    };
    let fleet = if per_tenant {
        Some(app_fleet(state).map_err(AutumnError::service_unavailable_msg)?)
    } else {
        None
    };
    let path = resolve_path(
        sqlite_config.path.as_deref(),
        code_path.as_deref(),
        app_config.database.effective_primary_url(),
    )
    .map_err(AutumnError::service_unavailable_msg)?;
    let registry = Arc::new(Registry {
        workflows: parts.workflows,
        activities: parts.activities,
        updates: parts.updates,
        signals: parts.signals,
        queries: parts.queries,
        log_policy: parts.log_policy,
        state: Arc::new(with_app_state(parts.state, state)),
        schedules: parts.schedules,
        scheduler_enabled: runtime_config.scheduler_enabled,
        retention: parts.retention,
    });
    let catalog = Arc::new(handle::Catalog::of(&registry));
    let tick = Duration::from_millis(sqlite_config.tick_ms);
    let (runtime, auth_store) = open_control(
        path.clone(),
        Arc::clone(&registry),
        parts.audit_retention_days,
    )
    .await?;
    let retention = Arc::new(Retention::new(&registry.retention));
    let driver = Driver::start(runtime, tick, Arc::clone(&retention));
    let reader = match autumn_harvest_sqlite::SqliteReader::open(&path) {
        Ok(reader) => reader,
        Err(error) => {
            driver.shutdown().await;
            return Err(AutumnError::service_unavailable_msg(format!(
                "cannot open a reader on {}: {error}",
                path.display()
            )));
        }
    };
    let auth_store = Arc::new(auth_store);
    api_state.install_sqlite_auth_store(Arc::clone(&auth_store));
    let handle = HarvestSqlite::new(
        driver.sender.clone(),
        reader,
        path.clone(),
        catalog,
        retention,
    );
    slot.lock().unwrap_or_else(PoisonError::into_inner).driver = Some(driver);
    state.insert_extension(handle.clone());
    api_state.install_sqlite_handle(handle.clone());
    #[cfg(feature = "connectors")]
    start_connectors(slot, api_state, &registry, auth_store);
    let _ = handles.control.set(handle);
    tracing::info!(path = %path.display(), "harvest is running on SQLite");

    if let Some(fleet) = fleet {
        let tenants = HarvestFleet::new(fleet, registry, tick);
        tenants.install().await;
        state.insert_extension(tenants.clone());
        slot.lock().unwrap_or_else(PoisonError::into_inner).fleet = Some(tenants.clone());
        let _ = handles.fleet.set(tenants);
        tracing::info!("harvest runs one SQLite file per open fleet tenant");
    }
    Ok(())
}

/// Open the control file on a blocking thread: the runtime with its code
/// schedules, then the token and audit store.
///
/// Tokens and audit rows live in the Harvest file (issue #2469). The store
/// opens before the driver starts, so its schema step never meets a driver
/// write.
async fn open_control(
    path: PathBuf,
    registry: Arc<Registry>,
    audit_retention_days: i64,
) -> autumn_web::AutumnResult<(
    autumn_harvest_sqlite::SqliteRuntime,
    autumn_harvest_sqlite::SqliteAuthStore,
)> {
    tokio::task::spawn_blocking(move || {
        let mut runtime = Driver::open(&path, &registry)?;
        Driver::upsert_code_schedules(&mut runtime, &registry)?;
        let auth_store = autumn_harvest_sqlite::SqliteAuthStore::open(&path)
            .and_then(|store| store.with_audit_retention_days(audit_retention_days))
            .map_err(|error| {
                format!(
                    "cannot open the token and audit store on {}: {error}",
                    path.display()
                )
            })?;
        Ok::<_, String>((runtime, auth_store))
    })
    .await
    .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?
    .map_err(AutumnError::service_unavailable_msg)
}

/// Start the broker connectors on the control Harvest (issue #2471).
///
/// The handle is installed first, so no message meets a backend that is
/// not running. Poison messages go to the Harvest file. SQLite has no
/// telemetry registry, so the loops record no metric.
#[cfg(feature = "connectors")]
fn start_connectors(
    slot: &Arc<Mutex<Slot>>,
    api_state: &HarvestApiState,
    registry: &Registry,
    store: Arc<autumn_harvest_sqlite::SqliteAuthStore>,
) {
    let registrations = std::mem::take(
        &mut slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connectors,
    );
    if registrations.is_empty() {
        return;
    }
    let sink: Arc<dyn crate::connector::DeadLetterSink> =
        Arc::new(crate::connector::SqliteDeadLetterSink::new(store));
    let metrics: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        Arc::new(autumn_harvest::NoOpMetrics);
    let loops = crate::plugin::spawn_connectors(
        &registrations,
        api_state,
        &metrics,
        &registry.workflows,
        &sink,
    );
    slot.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .connector_loops = loops;
    tracing::info!(
        bindings = registrations.len(),
        "harvest connectors run on SQLite"
    );
}

/// The app's SQLite fleet, for `sqlite_per_tenant` (issue #2472).
///
/// # Errors
///
/// Returns a message when the app has no fleet.
fn app_fleet(state: &AppState) -> Result<autumn_web::db::fleet::DatabaseFleet, String> {
    state
        .shards()
        .and_then(autumn_web::sharding::ShardSet::fleet)
        .cloned()
        .ok_or_else(|| {
            "HarvestPlugin::sqlite_per_tenant needs a SQLite fleet: configure \
             [database.fleet], or remove the option (autumn-harvest#2472)"
                .to_owned()
        })
}

/// Add `AppState` to the activity state, as the Postgres worker does.
///
/// An activity reads the app's pool or fleet with `ctx.state::<AppState>()`.
/// Those exist only once the app boots, so a value given to
/// `HarvestPlugin::state` at build time cannot carry them. A value of the
/// same type that the app registered wins.
fn with_app_state(
    mut map: autumn_harvest::context::SharedStateMap,
    state: &AppState,
) -> autumn_harvest::context::SharedStateMap {
    map.entry(std::any::TypeId::of::<AppState>())
        .or_insert_with(|| Box::new(state.clone()));
    map
}

/// Announce an open management API, as the Postgres path does.
fn warn_if_open(profile: &str, api_state: &HarvestApiState) {
    let boundary = api_state.admin_auth_boundary();
    if crate::boot::dev_admin_api_is_open(profile, boundary) {
        tracing::warn!(
            "AUTUMN_PROFILE=dev with no HarvestPlugin::api_with_auth(..) boundary: the Harvest \
             SQLite management API, including every mutating route, is reachable \
             UNAUTHENTICATED. Do not expose this process beyond localhost."
        );
    }
    crate::boot::warn_if_mutation_opt_out_is_open(
        profile,
        boundary,
        api_state.allow_unauthenticated_mutations(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_path_wins_then_the_code_path() {
        let code = Path::new("/code/h.db");
        let app = Some("sqlite:///srv/app.db");
        let path = resolve_path(Some("/data/h.db"), Some(code), app).unwrap();
        assert_eq!(path, PathBuf::from("/data/h.db"));
        assert_eq!(resolve_path(None, Some(code), app).unwrap(), code);
        let memory = Some("sqlite::memory:");
        assert_eq!(resolve_path(None, Some(code), memory).unwrap(), code);
    }

    #[test]
    fn the_file_sits_next_to_the_app_database() {
        for (url, want) in [
            ("sqlite:///srv/app.db", "/srv/app.harvest.db"),
            ("sqlite://data/app.sqlite", "data/app.harvest.db"),
            ("sqlite:control.db", "control.harvest.db"),
            ("file:/srv/app.db?mode=rwc", "/srv/app.harvest.db"),
            ("sqlite:///srv/app", "/srv/app.harvest.db"),
        ] {
            assert_eq!(
                resolve_path(None, None, Some(url)).unwrap(),
                PathBuf::from(want),
                "{url}"
            );
        }
    }

    #[test]
    fn an_in_memory_app_database_needs_an_explicit_path() {
        for url in [
            "sqlite::memory:",
            "sqlite://:memory:",
            "sqlite://",
            "file::memory:?cache=shared",
            "file:x?mode=memory",
        ] {
            let error = resolve_path(None, None, Some(url)).unwrap_err();
            assert!(error.contains("harvest.sqlite.path"), "{url}: {error}");
        }
        let error = resolve_path(None, None, None).unwrap_err();
        assert!(error.contains("harvest.sqlite.path"), "{error}");
    }

    /// Split and external mode keep Harvest in Postgres, so they take the
    /// Postgres path (issue #2472). Only embedded mode runs on SQLite.
    #[test]
    fn embedded_takes_the_sqlite_path_and_split_takes_postgres() {
        assert_eq!(backend_for(HarvestMode::Embedded), Backend::Sqlite);
        assert_eq!(backend_for(HarvestMode::Split), Backend::Postgres);
        assert_eq!(backend_for(HarvestMode::External), Backend::Postgres);
    }

    /// A SQLite-only setting has no effect on a Postgres Harvest database.
    /// The build refuses it, so it is never ignored (issue #2472).
    #[test]
    fn split_mode_refuses_the_sqlite_only_settings() {
        for mode in [HarvestMode::Split, HarvestMode::External] {
            assert!(split_refusal(mode, false, None).is_none());
            let path = split_refusal(mode, false, Some("/srv/h.db")).unwrap();
            assert!(path.contains("harvest.sqlite.path"), "{path}");
            assert!(path.contains("#2472"), "{path}");
            let tenant = split_refusal(mode, true, None).unwrap();
            assert!(tenant.contains("sqlite_per_tenant"), "{tenant}");
            let both = split_refusal(mode, true, Some("/srv/h.db")).unwrap();
            assert!(
                both.contains("harvest.sqlite.path") && both.contains("sqlite_per_tenant"),
                "{both}"
            );
        }
    }

    #[test]
    fn a_refusal_names_every_option_and_its_issue() {
        assert!(refusal_message(&[]).is_none());
        let message =
            refusal_message(&[("HarvestPlugin::dags", 2019), ("enable_api_tokens", 2469)]).unwrap();
        assert!(message.contains("HarvestPlugin::dags (#2019)"), "{message}");
        assert!(message.contains("enable_api_tokens (#2469)"), "{message}");
        assert!(message.contains("#2466"), "{message}");
    }

    #[test]
    fn a_plain_builder_has_no_refusal() {
        assert!(builder_refusals(&HarvestBuilder::default()).is_empty());
    }

    /// The SQLite worker reads no cost hold, so a hold is refused rather than
    /// silently ignored (autumn#1720).
    #[test]
    fn a_cost_hold_is_refused_with_the_epic() {
        let builder = HarvestBuilder::default().worker(
            autumn_harvest::builder::WorkerConfig::default().with_cost_hold(
                autumn_harvest::cost_hold::CostHold::new(|| true, ["train_model"]),
            ),
        );
        assert_eq!(
            builder_refusals(&builder),
            [("WorkerConfig::with_cost_hold", EPIC_ISSUE)]
        );
    }

    /// The SQLite replay serves declarative queries (issue #2468), so the
    /// build no longer refuses them.
    #[test]
    fn a_declarative_query_is_not_refused() {
        fn answer(
            _ctx: &autumn_harvest::context::WorkflowContext,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, String> {
            Ok(serde_json::Value::Null)
        }
        let query = autumn_harvest::QueryHandlerInfo {
            name: "stage",
            workflow: "greet",
            module: module_path!(),
            input_type_hint: "()",
            output_type_hint: "()",
            handler: answer,
            description: None,
            arg_schema: None,
            response_schema: None,
        };
        let builder = HarvestBuilder::default().queries(vec![query]);
        assert!(builder_refusals(&builder).is_empty());
    }

    /// A run purge is honoured now (issue #2471). Only the fields with no
    /// SQLite store are refused.
    #[test]
    fn a_run_purge_is_honoured_and_its_storeless_fields_are_refused() {
        use autumn_harvest::retention::{RetentionConfig, SummaryPolicy};
        let config = RetentionConfig {
            max_age_secs: Some(60),
            overrides: [("greet".to_owned(), 30)].into(),
            dry_run: true,
            ..RetentionConfig::default()
        };
        let builder = HarvestBuilder::default().retention(config.clone());
        assert_eq!(builder_refusals(&builder), []);

        let storeless = RetentionConfig {
            tenant_overrides: [("acme".to_owned(), 30)].into(),
            summary: Some(SummaryPolicy::for_days(7)),
            payload_orphan_grace_secs: Some(3600),
            ..config
        };
        let builder = HarvestBuilder::default().retention(storeless);
        assert_eq!(
            builder_refusals(&builder),
            [
                ("RetentionConfig::tenant_overrides", EPIC_ISSUE),
                ("RetentionConfig::summary", EPIC_ISSUE),
                ("RetentionConfig::payload_orphan_grace_secs", EPIC_ISSUE),
            ]
        );
    }

    /// The driver maps the Postgres ages and the dry run (issue #2471).
    #[test]
    fn the_purge_policy_takes_the_postgres_ages() {
        use autumn_harvest::retention::RetentionConfig;
        assert!(driver::purge_policy(&RetentionConfig::default()).is_none());
        let config = RetentionConfig {
            max_age_secs: Some(60),
            overrides: [("greet".to_owned(), 30)].into(),
            dry_run: true,
            ..RetentionConfig::default()
        };
        let policy = driver::purge_policy(&config).expect("an age is set");
        assert_eq!(policy.max_age, Some(Duration::from_secs(60)));
        assert_eq!(
            policy.overrides.get("greet"),
            Some(&Duration::from_secs(30))
        );
        assert!(policy.dry_run);
        let only = RetentionConfig {
            overrides: [("greet".to_owned(), 30)].into(),
            ..RetentionConfig::default()
        };
        assert_eq!(driver::purge_policy(&only).unwrap().max_age, None);
    }
}
