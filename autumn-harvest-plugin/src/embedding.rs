//! One entry point for a standalone embedding (issue #1613).
//!
//! `HarvestPlugin` runs a long startup sequence for an autumn-web app. A
//! standalone embedder that calls [`HarvestRunner::start`] directly had to
//! repeat the rest of that sequence by hand. [`HarvestEmbedding`] runs it,
//! in the plugin order, through the same shared steps in `boot.rs`.
//!
//! ```rust,no_run
//! # async fn run(
//! #     built: autumn_harvest::BuiltHarvest,
//! #     config: autumn_harvest_plugin::HarvestRuntimeConfig,
//! #     pool: autumn_harvest::worker::DbPool,
//! # ) -> autumn_web::AutumnResult<()> {
//! use autumn_harvest_plugin::api::StandaloneAdminAuth;
//! use autumn_harvest_plugin::{HarvestEmbedding, HarvestRunnerResources};
//!
//! let harvest = HarvestEmbedding::new(built, config, HarvestRunnerResources::new(pool))
//!     .with_admin_auth(StandaloneAdminAuth::new().with_api_tokens())
//!     .start()
//!     .await?;
//! let app = autumn_web::reexports::axum::Router::new().nest("/api/harvest", harvest.router());
//! // Serve `app`. After the server stops:
//! harvest.stop().await;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::BuiltHarvest;
use autumn_harvest::types::ShardId;
use autumn_web::config::{Env, OsEnv, normalize_profile_name};
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum::Router;

use crate::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use crate::boot::{self, AdmissionGlobalsGuard, GateRefreshRuntime};
use crate::config::{HarvestRuntimeConfig, HarvestStartupConfig, OrphanStartupAction};
use crate::runner::{HarvestRunner, HarvestRunnerResources, run_startup_orphan_gate};
use crate::ui::harvest_ui_router;

/// The profile a new `HarvestApiState` reports until one is declared.
const UNDECLARED_PROFILE: &str = "unknown";

/// The inputs of a standalone Harvest runtime, before it starts.
///
/// It takes the inputs of [`HarvestRunner::start`] and the posture settings.
/// [`Self::start`] runs the startup sequence and returns a
/// [`HarvestEmbeddingRuntime`]: the runner and a mounted router.
pub struct HarvestEmbedding {
    built: BuiltHarvest,
    config: HarvestRuntimeConfig,
    resources: HarvestRunnerResources,
    admin_auth: StandaloneAdminAuth,
    api_state: HarvestApiState,
    mount_ui: bool,
    ambient_profile: bool,
    notification_urls: BTreeMap<ShardId, String>,
}

impl HarvestEmbedding {
    /// Collect the inputs of [`HarvestRunner::start`].
    ///
    /// `config.startup` is the code default. The operator config file and
    /// the `AUTUMN_HARVEST_STARTUP__*` variables override it at start. See
    /// [`crate::config::HarvestStartupConfig::with_operator_overrides`].
    ///
    /// Some steps belong to the plugin only, because they need an autumn-web
    /// `AppState`. This entry point does not run the outbox relay, the broker
    /// connectors, the webhook delivery workflow or the MCP tool routes.
    /// [`Self::start`] logs a warning when `config.outbox.enabled` is set.
    #[must_use]
    pub fn new(
        built: BuiltHarvest,
        config: HarvestRuntimeConfig,
        resources: HarvestRunnerResources,
    ) -> Self {
        Self {
            built,
            config,
            resources,
            admin_auth: StandaloneAdminAuth::new(),
            api_state: HarvestApiState::new(),
            mount_ui: true,
            ambient_profile: false,
            notification_urls: BTreeMap::new(),
        }
    }

    /// Declare the admin credential and the deployment posture.
    ///
    /// With no declared profile, the profile stays `unknown` and the admin
    /// API fails closed. See [`Self::with_ambient_profile`] to read it from
    /// the environment instead.
    #[must_use]
    pub fn with_admin_auth(mut self, auth: StandaloneAdminAuth) -> Self {
        self.admin_auth = auth;
        self
    }

    /// Use `api_state` instead of a new one.
    ///
    /// Use this to apply a setting that has no builder input, for example
    /// [`HarvestApiState::set_actor_extractor`]. Do not install a runtime or
    /// a storage pool on it. [`Self::start`] installs both.
    ///
    /// Declare the auth boundary through [`Self::with_admin_auth`], not on the
    /// state. [`Self::start`] sets the boundary from that declaration. A
    /// profile already set on the state is kept.
    #[must_use]
    pub fn with_api_state(mut self, api_state: HarvestApiState) -> Self {
        self.api_state = api_state;
        self
    }

    /// Read the deployment profile from the environment.
    ///
    /// [`Self::start`] reads `AUTUMN_ENV`, then `AUTUMN_PROFILE`, as autumn-web
    /// does. It does not read the command line. A profile that
    /// [`Self::with_admin_auth`] declares, or that the API state already
    /// holds, always wins.
    ///
    /// This is an opt-in because the `dev` profile opens the admin API to a
    /// caller with no credential. A stray variable must not do that silently.
    #[must_use]
    pub const fn with_ambient_profile(mut self) -> Self {
        self.ambient_profile = true;
        self
    }

    /// Leave the Vantage dashboard out of the returned router.
    #[must_use]
    pub const fn without_ui(mut self) -> Self {
        self.mount_ui = false;
        self
    }

    /// Set the database URL that result waits and SSE streams listen on, per
    /// shard.
    ///
    /// A single-shard embedding can omit this. It then uses
    /// `config.database.url` for the default shard. Explicit URLs replace
    /// `config.database.url` for every shard, the default shard too. A
    /// multi-shard embedding must name every shard in its pool, or
    /// [`Self::start`] refuses. The `embedded` mode does not read the
    /// autumn-web `[database] url`, so set `config.database.url` or call this.
    #[must_use]
    pub fn with_notification_database_urls<I, S>(mut self, urls: I) -> Self
    where
        I: IntoIterator<Item = (ShardId, S)>,
        S: Into<String>,
    {
        self.notification_urls = urls
            .into_iter()
            .map(|(shard, url)| (shard, url.into()))
            .collect();
        self
    }

    /// Start the runtime and mount its router.
    ///
    /// The order is the `HarvestPlugin` order, with one difference. The
    /// builder limits are copied after the orphan gate, not first, because
    /// the copy sets a process global. The gate cache loads before any worker
    /// spawns. The orphan gate runs before any admission global is published.
    /// The storage pool is installed before the API runtime.
    ///
    /// # Errors
    ///
    /// Returns an error in these cases:
    ///
    /// - The operator startup config is invalid.
    /// - A shard has no notification URL.
    /// - The orphan gate refuses boot.
    /// - [`HarvestRunner::start`] fails.
    ///
    /// The first three cases publish no admission global and change no other
    /// process global. In the last case, the admission globals are restored
    /// to their previous values. The start-idempotency purge window keeps the
    /// value of the failed start. An API state from [`Self::with_api_state`]
    /// can keep the posture settings of a failed start. It holds no runtime,
    /// so its routes that need the runtime or the database fail.
    pub fn start(
        self,
    ) -> impl Future<Output = autumn_web::AutumnResult<HarvestEmbeddingRuntime>> + Send {
        // The startup future is large, so it lives on the heap. A caller's own
        // future then stays small (`clippy::large_futures`).
        Box::pin(async move {
            // Read the environment before the first await. `&dyn Env` is not
            // `Sync`, so holding it would make this future not `Send`.
            let operator = OperatorInputs::read(self.config.startup, &OsEnv)?;
            Box::pin(self.start_with(operator)).await
        })
    }

    async fn start_with(
        self,
        operator: OperatorInputs,
    ) -> autumn_web::AutumnResult<HarvestEmbeddingRuntime> {
        let Self {
            mut built,
            mut config,
            resources,
            admin_auth,
            api_state,
            mount_ui,
            ambient_profile,
            notification_urls,
        } = self;

        warn_if_operator_weakens_the_orphan_gate(config.startup, operator.startup);
        config.startup = operator.startup;
        if config.outbox.enabled {
            tracing::warn!(
                "HarvestEmbedding does not run the workflow-start outbox relay. The relay needs \
                 an autumn-web AppState, so only HarvestPlugin runs it. Set \
                 config.outbox.enabled = false to silence this warning."
            );
        }

        // Check the URL coverage first, so that a refusal spawns nothing.
        let (default_shard, default_pool) = resources.default_shard_pool(&built);
        let urls = notification_urls_or_default(
            notification_urls,
            config.database.url.as_deref(),
            default_shard,
        );
        let missing = missing_notification_shards(resources.pool_shards(&built), &urls);
        if !missing.is_empty() {
            return Err(AutumnError::service_unavailable_msg(missing_url_message(
                &missing,
            )));
        }

        let router = mount_router(&api_state, &admin_auth, mount_ui);
        if ambient_profile {
            apply_ambient_profile(&api_state, &admin_auth, operator.profile.as_deref());
        }
        warn_if_dev_admin_api_is_open(&api_state);
        // Reject a start that arrives before the boot gate load completes.
        api_state.arm_gate_cache_fail_closed();
        api_state.set_health_requires_shard_readiness(config.readiness.require_shard_readiness);
        api_state.set_workflow_result_notification_database_urls(urls);
        api_state.set_payload_codecs(built.payload_codecs().clone());

        boot::load_boot_admission_gates(&api_state, &default_pool).await;
        run_startup_orphan_gate(config.startup.orphaned_workflows, &built, &resources).await?;
        // The mirror sets a process global, so it runs after the last refusal
        // that needs no rollback.
        boot::mirror_built_config(&api_state, &mut built);

        let mut admission_guard = AdmissionGlobalsGuard::publish_gate_cache(api_state.gate_cache());
        admission_guard.publish_metrics(Arc::clone(&built.telemetry().metrics));
        let runner = HarvestRunner::start(
            built,
            &config,
            resources.with_startup_orphan_gate_already_run(),
        )
        .await?;

        let storage_pool = runner.storage_pool();
        api_state.install_storage_pool(storage_pool.clone());
        // The load-shed sampler takes its registry data from the runtime here,
        // not from the install below, so its first tick cannot race the install.
        let gate_refresh =
            boot::spawn_gate_refresh(&api_state, &storage_pool, &runner.api_runtime());
        api_state.install(runner.api_runtime());
        admission_guard.commit();

        Ok(HarvestEmbeddingRuntime {
            runner,
            api_state,
            router,
            gate_refresh,
        })
    }
}

/// How many times any Harvest runtime in this process has published its
/// admission gate cache. For tests only.
#[doc(hidden)]
#[must_use]
pub fn __admission_gate_cache_publish_count() -> u64 {
    boot::gate_cache_publish_count()
}

/// A running standalone Harvest runtime.
///
/// Call [`Self::stop`] when the server stops. A dropped runtime keeps its
/// background tasks and its process globals.
#[must_use = "call `stop()` on shutdown, or the background tasks and process globals remain"]
pub struct HarvestEmbeddingRuntime {
    runner: HarvestRunner,
    api_state: HarvestApiState,
    router: Router<()>,
    gate_refresh: GateRefreshRuntime,
}

impl HarvestEmbeddingRuntime {
    /// The management router, with the declared auth layers applied.
    ///
    /// Nest it under a path of your choice. Apply your own auth middleware
    /// outside it, so that the layer order is: your auth, the token layer,
    /// the read-only-role layer, the admin guard.
    ///
    /// The router holds the Vantage dashboard unless
    /// [`HarvestEmbedding::without_ui`] was called. The admin guard is on
    /// selected high-impact routes only. Other routes, for example a workflow
    /// start or a signal, need your own auth layer, as on the plugin path.
    pub fn router(&self) -> Router<()> {
        self.router.clone()
    }

    /// The API state the router reads.
    #[must_use]
    pub const fn api_state(&self) -> &HarvestApiState {
        &self.api_state
    }

    /// The runner that owns the worker and the scheduler.
    #[must_use]
    pub const fn runner(&self) -> &HarvestRunner {
        &self.runner
    }

    /// Stop the runtime and remove what it published.
    ///
    /// The order is the `HarvestPlugin` order. The gate refresh stops first.
    /// The runner then drains its worker, up to `WorkerConfig::shutdown_timeout`.
    /// The admission globals are cleared only after the runner stops. The API
    /// state is emptied last, so the routes that need the runtime or the
    /// database fail.
    pub async fn stop(self) {
        let metrics = Arc::clone(&self.runner.api_runtime().registry().telemetry().metrics);
        self.gate_refresh.stop().await;
        self.runner.stop().await;
        boot::clear_admission_globals(&self.api_state, Some(&metrics));
        self.api_state.clear();
    }
}

/// Compose the API router, Vantage when asked, and the declared auth layers.
///
/// Vantage is nested before the layers apply, so they cover it too. This is
/// the `HarvestPlugin` composition.
fn mount_router(
    api_state: &HarvestApiState,
    admin_auth: &StandaloneAdminAuth,
    mount_ui: bool,
) -> Router<()> {
    let mut router = harvest_api_router(api_state.clone());
    if mount_ui {
        router = router.nest("/ui", harvest_ui_router(api_state.clone()));
    }
    admin_auth.mount(router, api_state)
}

/// The operator inputs [`HarvestEmbedding::start`] reads from the
/// environment and the config files.
struct OperatorInputs {
    /// The code startup config with the operator overrides applied.
    startup: HarvestStartupConfig,
    /// The profile from the environment, if any.
    profile: Option<String>,
}

impl OperatorInputs {
    fn read(code_startup: HarvestStartupConfig, env: &dyn Env) -> autumn_web::AutumnResult<Self> {
        let startup = code_startup
            .with_operator_overrides(env)
            .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
        Ok(Self {
            startup,
            profile: ambient_profile(env),
        })
    }
}

/// The profile from `AUTUMN_ENV`, else `AUTUMN_PROFILE`, as autumn-web
/// resolves it.
///
/// The command line is not read, because the host program can own a
/// `--profile` flag of its own.
fn ambient_profile(env: &dyn Env) -> Option<String> {
    ["AUTUMN_ENV", "AUTUMN_PROFILE"]
        .into_iter()
        .filter_map(|key| env.var(key).ok())
        .find_map(|value| normalize_profile_name(&value))
}

/// Set `profile` on `api_state` unless a profile is already declared.
///
/// A profile that `admin_auth` declares wins. A profile already set on
/// `api_state` also wins.
fn apply_ambient_profile(
    api_state: &HarvestApiState,
    admin_auth: &StandaloneAdminAuth,
    profile: Option<&str>,
) {
    if admin_auth.declared_deployment_profile().is_some()
        || api_state.deployment_profile() != UNDECLARED_PROFILE
    {
        return;
    }
    if let Some(profile) = profile {
        api_state.set_deployment_profile(profile);
    }
}

/// Log the open admin API that the `dev` profile allows with no boundary.
fn warn_if_dev_admin_api_is_open(api_state: &HarvestApiState) {
    if boot::dev_admin_api_is_open(
        &api_state.deployment_profile(),
        api_state.admin_auth_boundary(),
    ) {
        tracing::warn!(
            "The dev deployment profile is active with no admin auth boundary declared: the \
             Harvest management API (every /admin route and the Vantage dashboard) is reachable \
             UNAUTHENTICATED by any caller that can open a socket to this process. Do not \
             expose this process beyond localhost. To close it, declare \
             StandaloneAdminAuth::with_admin_auth_boundary() behind your own auth layer, or \
             declare a non-dev profile."
        );
    }
}

/// Log an operator override that weakens a code-set orphan action.
///
/// The override is valid, so it applies. The log line makes the change
/// visible, because `fail` protects in-flight runs.
fn warn_if_operator_weakens_the_orphan_gate(
    code: HarvestStartupConfig,
    resolved: HarvestStartupConfig,
) {
    if orphan_action_rank(resolved.orphaned_workflows) < orphan_action_rank(code.orphaned_workflows)
    {
        tracing::warn!(
            code = ?code.orphaned_workflows,
            operator = ?resolved.orphaned_workflows,
            "operator config weakens the orphaned-workflow startup action set in code"
        );
    }
}

const fn orphan_action_rank(action: OrphanStartupAction) -> u8 {
    match action {
        OrphanStartupAction::Off => 0,
        OrphanStartupAction::Warn => 1,
        OrphanStartupAction::Fail => 2,
    }
}

/// The explicit URLs, or `config_url` for the default shard when none are
/// given.
fn notification_urls_or_default(
    explicit: BTreeMap<ShardId, String>,
    config_url: Option<&str>,
    default_shard: ShardId,
) -> BTreeMap<ShardId, String> {
    if !explicit.is_empty() {
        return explicit;
    }
    config_url
        .map(|url| BTreeMap::from([(default_shard, url.to_owned())]))
        .unwrap_or_default()
}

/// The shards in `shards` that have no URL in `urls`.
fn missing_notification_shards(
    shards: impl IntoIterator<Item = ShardId>,
    urls: &BTreeMap<ShardId, String>,
) -> Vec<ShardId> {
    shards
        .into_iter()
        .filter(|shard| !urls.contains_key(shard))
        .collect()
}

fn missing_url_message(missing: &[ShardId]) -> String {
    let shards = missing
        .iter()
        .map(|shard| format!("shard {}", shard.as_i32()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "HarvestEmbedding has no result-notification database URL for {shards}. Result waits \
         and SSE streams for runs on a shard with no URL fail. Set config.database.url for a \
         single-shard pool, or call HarvestEmbedding::with_notification_database_urls with \
         one URL per shard."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_web::config::MockEnv;

    fn urls(pairs: &[(i32, &str)]) -> BTreeMap<ShardId, String> {
        pairs
            .iter()
            .map(|(shard, url)| (ShardId::new(*shard), (*url).to_owned()))
            .collect()
    }

    #[test]
    fn config_url_covers_the_default_shard_when_no_url_is_explicit() {
        let resolved =
            notification_urls_or_default(BTreeMap::new(), Some("pg://a"), ShardId::new(0));
        assert_eq!(resolved, urls(&[(0, "pg://a")]));
    }

    #[test]
    fn explicit_urls_replace_the_config_url() {
        let explicit = urls(&[(0, "pg://a"), (1, "pg://b")]);
        let resolved =
            notification_urls_or_default(explicit.clone(), Some("pg://c"), ShardId::new(0));
        assert_eq!(resolved, explicit);
    }

    #[test]
    fn no_url_at_all_leaves_every_shard_missing() {
        let resolved = notification_urls_or_default(BTreeMap::new(), None, ShardId::new(0));
        assert_eq!(
            missing_notification_shards([ShardId::new(0)], &resolved),
            vec![ShardId::new(0)]
        );
    }

    #[test]
    fn a_second_shard_without_a_url_is_reported() {
        let missing = missing_notification_shards(
            [ShardId::new(0), ShardId::new(1)],
            &urls(&[(0, "pg://a")]),
        );
        assert_eq!(missing, vec![ShardId::new(1)]);
        assert!(missing_url_message(&missing).contains("shard 1"));
    }

    #[test]
    fn a_declared_profile_is_not_replaced_by_the_environment() {
        let api_state = HarvestApiState::new();
        let auth = StandaloneAdminAuth::new().with_deployment_profile("prod");
        let _router = mount_router(&api_state, &auth, false);
        let env = MockEnv::new().with("AUTUMN_PROFILE", "dev");
        apply_ambient_profile(&api_state, &auth, ambient_profile(&env).as_deref());
        assert_eq!(api_state.deployment_profile(), "prod");
    }

    #[test]
    fn a_profile_already_on_the_state_is_not_replaced_by_the_environment() {
        let api_state = HarvestApiState::new();
        api_state.set_deployment_profile("prod");
        apply_ambient_profile(&api_state, &StandaloneAdminAuth::new(), Some("dev"));
        assert_eq!(api_state.deployment_profile(), "prod");
    }

    #[test]
    fn autumn_env_wins_over_autumn_profile() {
        let env = MockEnv::new()
            .with("AUTUMN_ENV", "prod")
            .with("AUTUMN_PROFILE", "dev");
        assert_eq!(ambient_profile(&env).as_deref(), Some("prod"));
    }

    #[test]
    fn an_undeclared_profile_comes_from_the_environment() {
        let api_state = HarvestApiState::new();
        let auth = StandaloneAdminAuth::new();
        let env = MockEnv::new().with("AUTUMN_PROFILE", "dev");
        apply_ambient_profile(&api_state, &auth, ambient_profile(&env).as_deref());
        assert_eq!(api_state.deployment_profile(), "dev");
    }

    #[test]
    fn an_undeclared_profile_with_no_environment_stays_unknown() {
        let api_state = HarvestApiState::new();
        apply_ambient_profile(
            &api_state,
            &StandaloneAdminAuth::new(),
            ambient_profile(&MockEnv::new()).as_deref(),
        );
        assert_eq!(api_state.deployment_profile(), UNDECLARED_PROFILE);
    }

    #[test]
    fn the_undeclared_profile_matches_a_new_api_state() {
        assert_eq!(
            HarvestApiState::new().deployment_profile(),
            UNDECLARED_PROFILE
        );
    }

    #[test]
    fn orphan_action_rank_orders_off_warn_fail() {
        assert!(
            orphan_action_rank(OrphanStartupAction::Off)
                < orphan_action_rank(OrphanStartupAction::Warn)
        );
        assert!(
            orphan_action_rank(OrphanStartupAction::Warn)
                < orphan_action_rank(OrphanStartupAction::Fail)
        );
    }
}
