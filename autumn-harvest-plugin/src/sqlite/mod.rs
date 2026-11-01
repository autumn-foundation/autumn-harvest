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
//!
//! Every builder option and mode that this slice cannot honour fails the
//! build or the boot, and names the issue that ports it. A silent no-op would
//! let an app believe a protection or a feature is on.

mod api;
mod driver;
mod handle;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use autumn_harvest::builder::HarvestBuilder;
use autumn_web::AppState;
use autumn_web::app::AppBuilder;
use autumn_web::error::AutumnError;

pub use driver::SignalWithStartParams;
pub use handle::{HarvestSqlite, HarvestSqliteError};

/// The `autumn-harvest-sqlite` types that [`HarvestSqlite`] takes and returns.
pub use autumn_harvest_sqlite::{
    ExecutionDescription, ExecutionFilter, ExecutionPage, ExecutionSummary, ListCursor,
    SignalDelivery, SignalWithStartOutcome, SqliteError, StartOptions, StartOutcome, Stored,
};

use crate::api::HarvestApiState;
use crate::config::{HarvestMode, HarvestRuntimeConfig, HarvestSqliteConfig};
use driver::{Driver, Registry};

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
}

struct StartParts {
    workflows: Vec<autumn_harvest::info::WorkflowInfo>,
    activities: Vec<autumn_harvest::info::ActivityInfo>,
    updates: Vec<autumn_harvest::UpdateHandlerInfo>,
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
        mut refused,
    } = parts;

    refused.extend(builder_refusals(&builder));
    if let Some(message) = refusal_message(&refused) {
        panic!("{message}");
    }
    let mode = HarvestRuntimeConfig::load()
        .unwrap_or_else(|error| panic!("cannot load the Harvest configuration: {error}"))
        .mode;
    if let Some(message) = mode_refusal(mode) {
        panic!("{message}");
    }
    // Fail now on a bad `[harvest.sqlite]`, not at the first request.
    HarvestSqliteConfig::load()
        .unwrap_or_else(|error| panic!("cannot load the Harvest SQLite configuration: {error}"));

    let app = app.config_section(crate::config::HARVEST_CONFIG_ROOT);

    let start_parts = StartParts {
        workflows: builder.workflow_infos().to_vec(),
        activities: builder.take_activities(),
        updates: builder.update_handlers().to_vec(),
        state: builder.take_state(),
    };

    let api_state = HarvestApiState::new();
    api_state.set_allow_unauthenticated_mutations(allow_unauthenticated_mutations);
    api_state.set_admin_auth_boundary(api_middleware.is_some());
    let handle_slot: Arc<OnceLock<HarvestSqlite>> = Arc::new(OnceLock::new());

    let slot = Arc::new(Mutex::new(Slot {
        parts: Some(start_parts),
        driver: None,
        code_path,
    }));
    let startup_slot = Arc::clone(&slot);
    let startup_handle = Arc::clone(&handle_slot);
    let startup_api_state = api_state.clone();
    let app = app
        .on_startup(move |state| {
            let slot = Arc::clone(&startup_slot);
            let handle_slot = Arc::clone(&startup_handle);
            let api_state = startup_api_state.clone();
            async move { start(&state, &slot, &handle_slot, &api_state).await }
        })
        .on_shutdown(move || {
            let slot = Arc::clone(&slot);
            async move {
                let driver = slot
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .driver
                    .take();
                if let Some(driver) = driver {
                    driver.shutdown().await;
                }
            }
        });

    let Some(path) = api_path else {
        return app;
    };
    let mut router = api::router(api::ApiState {
        handle: handle_slot,
        api_state,
    });
    if let Some(middleware) = api_middleware {
        router = middleware(router);
    }
    // `AppBuilder::nest` takes a `Router<AppState>`. No handler reads it.
    app.nest(&path, router.with_state(()))
}

/// The options set on the builder that have no SQLite port.
fn builder_refusals(builder: &HarvestBuilder) -> Vec<(&'static str, u32)> {
    let mut refused = Vec::new();
    if !builder.dag_infos().is_empty() {
        // DAGs are an engine non-goal of the SQLite backend (#2019).
        refused.push(("HarvestPlugin::dags", 2019));
    }
    // The SQLite replay passes no declarative query handler, so a call to one
    // would answer `QueryNotFound`. A query that the workflow body registers
    // with `ctx.register_query` works.
    if !builder.query_handlers().is_empty() {
        refused.push(("HarvestPlugin::queries", 2468));
    }
    if !builder.workflow_schedules().is_empty() {
        refused.push(("workflow schedules", 2471));
    }
    // Only a run purge counts. The default config also runs the audit and
    // schedule janitors, which this backend has no tables for, so `enabled`
    // is true for every plugin.
    if purges_runs(builder.retention_config()) {
        refused.push(("HarvestPlugin::retention", 2471));
    }
    refused
}

/// Whether `config` asks to purge terminal runs by age.
fn purges_runs(config: &autumn_harvest::retention::RetentionConfig) -> bool {
    config.max_age_secs.is_some()
        || !config.overrides.is_empty()
        || !config.tenant_overrides.is_empty()
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

/// The refusal of a mode other than `embedded`, or `None`.
fn mode_refusal(mode: HarvestMode) -> Option<String> {
    match mode {
        HarvestMode::Embedded => None,
        HarvestMode::Split | HarvestMode::External => Some(format!(
            "harvest.mode = {mode:?} needs a Postgres Harvest database, which a SQLite app \
             cannot reach yet (autumn-harvest#2472). Use harvest.mode = \"embedded\" with \
             harvest.sqlite.path"
        )),
    }
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

async fn start(
    state: &AppState,
    slot: &Arc<Mutex<Slot>>,
    handle_slot: &Arc<OnceLock<HarvestSqlite>>,
    api_state: &HarvestApiState,
) -> autumn_web::AutumnResult<()> {
    api_state.set_deployment_profile(state.profile().to_string());
    api_state.set_admin_auth_session_key(state.auth_session_key());
    warn_if_open(state.profile(), api_state);

    let runtime_config = HarvestRuntimeConfig::load()
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
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
    let (parts, code_path) = {
        let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
        (slot.parts.take(), slot.code_path.clone())
    };
    let Some(parts) = parts else {
        // A second startup in one process keeps the running driver.
        return Ok(());
    };
    let path = resolve_path(
        sqlite_config.path.as_deref(),
        code_path.as_deref(),
        app_config.database.effective_primary_url(),
    )
    .map_err(AutumnError::service_unavailable_msg)?;
    let infos = parts.workflows.clone();
    let registry = Registry {
        workflows: parts.workflows,
        activities: parts.activities,
        updates: parts.updates,
        state: Arc::new(with_app_state(parts.state, state)),
    };
    let tick = Duration::from_millis(sqlite_config.tick_ms);
    let open_path = path.clone();
    let runtime = tokio::task::spawn_blocking(move || Driver::open(&open_path, registry))
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?
        .map_err(AutumnError::service_unavailable_msg)?;
    let driver = Driver::start(runtime, tick);
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
    let handle = HarvestSqlite::new(driver.sender.clone(), reader, path.clone(), infos);
    slot.lock().unwrap_or_else(PoisonError::into_inner).driver = Some(driver);
    state.insert_extension(handle.clone());
    let _ = handle_slot.set(handle);
    tracing::info!(path = %path.display(), "harvest is running on SQLite");
    Ok(())
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

    #[test]
    fn split_and_external_modes_are_refused_with_their_issue() {
        assert!(mode_refusal(HarvestMode::Embedded).is_none());
        for mode in [HarvestMode::Split, HarvestMode::External] {
            let message = mode_refusal(mode).unwrap();
            assert!(message.contains("#2472"), "{message}");
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

    #[test]
    fn a_declarative_query_is_refused_with_its_issue() {
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
        assert_eq!(
            builder_refusals(&builder),
            vec![("HarvestPlugin::queries", 2468)]
        );
    }

    #[test]
    fn a_run_purge_is_refused_with_its_issue() {
        let config = autumn_harvest::retention::RetentionConfig {
            max_age_secs: Some(60),
            ..autumn_harvest::retention::RetentionConfig::default()
        };
        let builder = HarvestBuilder::default().retention(config);
        assert_eq!(
            builder_refusals(&builder),
            vec![("HarvestPlugin::retention", 2471)]
        );
    }
}
