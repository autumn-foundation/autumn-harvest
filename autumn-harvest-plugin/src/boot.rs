//! Startup and shutdown steps both boot paths share (issue #1613).
//!
//! `HarvestPlugin` and [`crate::embedding::HarvestEmbedding`] call these
//! functions. The two paths therefore run one implementation of each step,
//! and the step order stays the one each caller documents.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::BuiltHarvest;
use autumn_harvest::admission_gate::{
    AdmissionGateCache, global_admission_gate_cache, global_admission_metrics,
    set_global_admission_gate_cache, set_global_admission_metrics,
};
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{HarvestApiRuntime, HarvestApiState, acquire_conn};
use crate::state::HarvestDbPool;

/// Copy the limits of `built` into `api_state`.
///
/// The management routes read these values. A mount that skips this step
/// serves the default limits, not the configured ones. The step also applies
/// an audit-retention override set on `api_state`, and it installs the
/// start-idempotency purge window, which is process global.
pub fn mirror_built_config(api_state: &HarvestApiState, built: &mut BuiltHarvest) {
    // `/workers` classifies a worker as stale after two missed heartbeats.
    api_state.set_worker_stale_threshold(built.worker_config().worker_heartbeat_interval * 2);
    // A drain request with no `deadline_at` uses the worker shutdown timeout.
    api_state.set_worker_shutdown_timeout(built.worker_config().shutdown_timeout);
    // Per-query timeout (issue #234).
    api_state.set_query_timeout(built.worker_config().query_timeout);
    // Server-side execution timeout ceiling (issue #243).
    api_state.set_max_workflow_execution_timeout(built.max_workflow_execution_timeout);
    // Chain-cap ceiling and fleet-wide default (issue #617).
    api_state.set_max_workflow_chain_timeout(built.max_workflow_chain_timeout);
    // Hard history event ceiling (issue #493). Prefer the builder ceiling.
    // Fall back to the `WorkerConfig` ceiling, so that `/admin/preflight`
    // reports a ceiling set on either one.
    api_state.set_max_workflow_history_events(
        built
            .max_workflow_history_events
            .or_else(|| built.worker_config().max_workflow_history_events),
    );
    // Start delay ceiling (issue #322).
    api_state.set_max_workflow_start_delay(built.worker_config().max_workflow_start_delay);
    // Default debounce max-wait cap (issue #499).
    api_state.set_default_debounce_max_wait(built.worker_config().default_debounce_max_wait);
    // Workflow retry attempt ceiling (issue #523).
    api_state.set_max_workflow_attempts(built.max_workflow_attempts);
    // Start-idempotency window (issue #808). The HTTP start route reads it to
    // dedup a repeated `idempotency_key`. The expiry sweep reads the same
    // value from a process global, like `GLOBAL_CALLBACK_CONFIG`. So no extra
    // parameter goes through `enforce_timeouts_once`.
    api_state.set_start_idempotency_window(built.start_idempotency_window);
    autumn_harvest::start_idempotency::set_purge_window_secs(built.start_idempotency_window);
    // `GET /admin/usage` window ceiling and group-count cap (issue #596).
    api_state.set_usage_window_ceiling(built.usage_window_ceiling);
    api_state.set_usage_max_groups(built.usage_max_groups);
    // Batch start caps (issue #357).
    api_state.set_batch_start_config(&built.batch_start_config);
    // Automatic load shedding (issue #1794). An empty config turns it off.
    api_state
        .gate_cache()
        .load_shedder()
        .configure(built.load_shed.clone());
    // Build ramp guard (issue #1814). The default config is disabled.
    api_state.set_ramp_guard_config(built.ramp_guard);
    // Completion-callback SSRF policy (issue #605). The HTTP start route
    // validates a per-execution target against the allowlist that the scanner
    // uses at delivery time. `PreparedHarvestRuntime::build`, inside
    // `HarvestRunner::start`, installs the rest of the callback config for
    // every `BuiltHarvest` consumer.
    api_state.set_completion_callback_ssrf_policy(built.completion_callback_config().ssrf_policy());
    // Apply the audit-retention override only when it is set, so that the
    // builder retention config stays in effect otherwise.
    if let Some(days) = api_state.audit_retention_days() {
        built.set_audit_retention_days(days);
    }
}

/// Load the persisted admission gates into the cache of `api_state`.
///
/// Call this before `HarvestRunner::start` spawns a worker or a scanner. A
/// completion trigger that fires in the boot window then sees the gates of a
/// previous process lifetime, not an empty snapshot (issue #618).
///
/// A load failure leaves the cache as it was and does not block startup. A
/// permanent drop on a boot blip is the defect issue #618 fixed. Both boot
/// paths arm the cache fail-closed first. The direct HTTP start path then
/// stays fail-closed until the first refresh. `check_cached()` admits, which
/// is the documented bounded caveat. Either failure logs a warning.
pub async fn load_boot_admission_gates(api_state: &HarvestApiState, pool: &DbPool) {
    let mut conn = match acquire_conn(pool).await {
        Ok(conn) => conn,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "could not acquire a connection to load admission gates at startup; \
                 cache unchanged until first refresh"
            );
            return;
        }
    };
    match autumn_harvest::admission_gate::db::load_active_gates(&mut conn).await {
        Ok(gates) => {
            api_state.gate_cache().refresh(gates);
            tracing::debug!("admission gate cache populated at startup (before workers spawn)");
        }
        Err(error) => tracing::warn!(
            error = %error,
            "could not load admission gates at startup; cache unchanged until first refresh"
        ),
    }
}

/// The background loops that keep the gate cache current (issue #377).
///
/// The load-shed sampler (issue #1794) shares the shutdown token.
pub struct GateRefreshRuntime {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    load_shed: Option<JoinHandle<()>>,
    ramp_guard: Option<JoinHandle<()>>,
}

impl GateRefreshRuntime {
    /// Cancel the loops and wait for them to end.
    pub async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
        if let Some(load_shed) = self.load_shed {
            let _ = load_shed.await;
        }
        if let Some(ramp_guard) = self.ramp_guard {
            let _ = ramp_guard.await;
        }
    }
}

/// Spawn the loop that reloads the gate cache of `api_state` every second.
///
/// The loop fails closed. When the gate table is unreadable, the cache
/// becomes uninitialized, so `check()` blocks new starts. A stale open
/// snapshot would admit them.
///
/// It also spawns the load-shed sampler when a queue has a policy. The
/// sampler takes its registry data from `runtime`, so a caller can spawn it
/// before `api_state.install(runtime)`.
pub fn spawn_gate_refresh(
    api_state: &HarvestApiState,
    pools: &HarvestDbPool,
    runtime: &HarvestApiRuntime,
) -> GateRefreshRuntime {
    let shutdown = CancellationToken::new();
    let cancel = shutdown.child_token();
    let inputs = LoadShedSamplerInputs::from_registry(runtime.registry());
    let load_shed = spawn_load_shed_sampler(api_state, pools, inputs, shutdown.child_token());
    let ramp_guard = spawn_ramp_guard(api_state, pools, runtime, shutdown.child_token());
    let pool = pools.clone_inner();
    let cache = api_state.gate_cache();
    let api_state = api_state.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
            match acquire_conn(&pool).await {
                Ok(mut conn) => {
                    match autumn_harvest::admission_gate::db::load_active_gates(&mut conn).await {
                        Ok(gates) => {
                            let count = i64::try_from(gates.len()).unwrap_or(0);
                            cache.refresh(gates);
                            if let Ok(runtime) = api_state.runtime() {
                                runtime
                                    .registry()
                                    .telemetry()
                                    .metrics
                                    .record_admission_gates_active(count);
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                error = %error,
                                "admission gate refresh failed; entering fail-closed mode"
                            );
                            cache.set_fail_closed();
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "admission gate refresh: could not acquire DB connection; \
                         entering fail-closed mode"
                    );
                    cache.set_fail_closed();
                }
            }
        }
    });
    GateRefreshRuntime {
        shutdown,
        handle,
        load_shed,
        ramp_guard,
    }
}

/// Spawn the build ramp guard of `api_state` (issue #1814).
///
/// Returns `None` when the guard is disabled, so a default deployment runs no
/// guard SQL. The guard reads each physical pool once per pass and writes its
/// audit rows to the default pool. It takes the metrics recorder from
/// `runtime`, so a caller can spawn it before `api_state.install(runtime)`.
fn spawn_ramp_guard(
    api_state: &HarvestApiState,
    pools: &HarvestDbPool,
    runtime: &HarvestApiRuntime,
    cancel: CancellationToken,
) -> Option<JoinHandle<()>> {
    let config = api_state.ramp_guard_config();
    if !config.is_enabled() {
        return None;
    }
    let shard_pools: Vec<DbPool> = pools
        .sharded_pool()
        .pool_groups()
        .into_iter()
        .map(|(pool, _)| pool.clone())
        .collect();
    let metrics = Arc::clone(&runtime.registry().telemetry().metrics);
    Some(tokio::spawn(autumn_harvest::ramp_guard::run_ramp_guard(
        shard_pools,
        pools.clone_inner(),
        config,
        metrics,
        cancel,
    )))
}

/// The registry data that every load-shed sample reads (issue #1794).
///
/// The sampler takes this data once, at construction, from the registry of
/// the runner. It does not resolve `HarvestApiState::runtime()` per tick.
/// That accessor is empty until `install` runs, and both boot paths spawn
/// the sampler before the install. On a multithreaded runtime the first
/// tick can run in that window. A tick with an empty breaker list treats an
/// old circuit-breaker activity behind an empty rate-limit bucket as
/// unclaimable. The worker bypasses that bucket, so the queue would stay
/// open until the next sample.
pub struct LoadShedSamplerInputs {
    /// The recorder for the `load_shed_active` gauge.
    metrics: Arc<dyn MetricsRecorder>,
    /// The activities that skip the claim-time rate-limit gate.
    circuit_breaker_activities: Vec<String>,
}

impl LoadShedSamplerInputs {
    /// Copy the sampler data out of `registry`.
    pub fn from_registry(registry: &HandlerRegistry) -> Self {
        Self {
            metrics: Arc::clone(&registry.telemetry().metrics),
            circuit_breaker_activities: registry
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec(),
        }
    }

    /// The activities that skip the claim-time rate-limit gate.
    pub fn circuit_breaker_activities(&self) -> &[String] {
        &self.circuit_breaker_activities
    }
}

/// Spawn the load-shed sampler of `api_state` (issue #1794).
///
/// Returns `None` when no queue has a policy, so a default deployment runs no
/// sampler SQL. The sampler reads each physical pool once per tick. Each
/// tick reads `inputs`, so the first tick sees the registry data whatever
/// the install order.
///
/// `sample_once` bounds its own read and audit writes. Ticks keep a fixed
/// period, so a slow sample does not push the next one out. An overdue tick
/// fires at once. The staleness bound exceeds the read and audit bounds
/// together by one interval. The shed state therefore stays fresh across a
/// slow sample and the next read. The config clamps the interval to
/// `MAX_SAMPLE_INTERVAL`, so the tick deadline is finite.
fn spawn_load_shed_sampler(
    api_state: &HarvestApiState,
    pools: &HarvestDbPool,
    inputs: LoadShedSamplerInputs,
    cancel: CancellationToken,
) -> Option<JoinHandle<()>> {
    let shedder = Arc::clone(api_state.gate_cache().load_shedder());
    let config = shedder.config();
    if !config.is_enabled() {
        return None;
    }
    let interval = config.sample_interval();
    let shard_pools: Vec<DbPool> = pools
        .sharded_pool()
        .pool_groups()
        .into_iter()
        .map(|(pool, _)| pool.clone())
        .collect();
    let audit_pool = pools.clone_inner();
    Some(tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let sample = autumn_harvest::load_shed::sample_once(
                &shedder,
                &shard_pools,
                &audit_pool,
                Some(inputs.metrics.as_ref()),
                inputs.circuit_breaker_activities(),
            );
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = sample => {}
            }
        }
    }))
}

/// Clear the admission globals that this runtime published.
///
/// Call this only after `HarvestRunner::stop` returns. A worker or a scanner
/// that evaluates a completion trigger reads these globals. Clearing them
/// while such a task runs lets a trigger start its target past an active gate
/// (issue #618).
///
/// Each clear compares pointers. A sibling runtime in the same process keeps
/// its own globals. `metrics` is `None` when this runtime never started a
/// runner, so it has no recorder to compare.
pub fn clear_admission_globals(
    api_state: &HarvestApiState,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) {
    if let Some(installed) = global_admission_gate_cache()
        && Arc::ptr_eq(&installed, &api_state.gate_cache())
    {
        set_global_admission_gate_cache(None);
    }
    if let Some(metrics) = metrics
        && let Some(installed) = global_admission_metrics()
        && Arc::ptr_eq(&installed, metrics)
    {
        set_global_admission_metrics(None);
    }
}

/// Whether the `dev` profile opens the admin API to a caller with no
/// credential.
///
/// This is the exact condition under which `has_harvest_admin_access` admits
/// such a caller. `preflight::check_admin_auth_boundary` reports the same
/// condition in its `unauthenticated_access` field.
pub fn dev_admin_api_is_open(profile: &str, auth_boundary_present: bool) -> bool {
    profile == "dev" && !auth_boundary_present
}

/// Whether a caller with no credential reaches a mutating route (issue #1802).
///
/// The `dev` profile and the `allow_unauthenticated_mutations` opt-out open
/// the routes. A declared auth boundary hands the decision to the embedder's
/// middleware. The predicate then returns false, and the gate admits every
/// caller that middleware passes. Every other posture fails closed. The
/// mutation gate, the opt-out warning and `preflight` read this predicate.
pub fn unauthenticated_mutations_open(
    profile: &str,
    auth_boundary_present: bool,
    allow_unauthenticated_mutations: bool,
) -> bool {
    !auth_boundary_present && (profile == "dev" || allow_unauthenticated_mutations)
}

/// Log the `allow_unauthenticated_mutations` opt-out when it opens the routes.
///
/// It fires only outside `dev`. In `dev` the routes are open anyway, and the
/// `dev` warning names them. A declared boundary keeps it silent.
pub fn warn_if_mutation_opt_out_is_open(
    profile: &str,
    auth_boundary_present: bool,
    allow_unauthenticated_mutations: bool,
) {
    if mutation_opt_out_opens_routes(
        profile,
        auth_boundary_present,
        allow_unauthenticated_mutations,
    ) {
        tracing::warn!(
            profile,
            "allow_unauthenticated_mutations is set and no auth boundary is declared: every \
             Harvest mutating route without an admin gate (workflow start, signal, reset, \
             update, DAG trigger, schedule changes, external-activity callbacks, worker drain, \
             Vantage and MCP tool mutations) is reachable UNAUTHENTICATED. Remove the opt-out \
             once an auth layer wraps the management API."
        );
    }
}

/// Whether the opt-out, and not the `dev` profile, opens the mutating routes.
///
/// The opt-out warning and `preflight` read it.
pub fn mutation_opt_out_opens_routes(
    profile: &str,
    auth_boundary_present: bool,
    allow_unauthenticated_mutations: bool,
) -> bool {
    profile != "dev"
        && unauthenticated_mutations_open(
            profile,
            auth_boundary_present,
            allow_unauthenticated_mutations,
        )
}

/// How many times a guard has published a gate cache in this process.
///
/// A refused boot restores the previous globals, so the globals alone cannot
/// show whether a publish happened. Tests read this count to prove that the
/// orphan gate runs before the publish.
static GATE_CACHE_PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// The value of [`GATE_CACHE_PUBLISHES`].
pub fn gate_cache_publish_count() -> u64 {
    GATE_CACHE_PUBLISHES.load(Ordering::Relaxed)
}

/// Publishes the admission globals and undoes the publish on an early error
/// (issue #618).
///
/// A startup error after the publish drops the guard. The drop then restores
/// the globals that were there before. A failed startup therefore never
/// leaves the globals on a dead runtime. [`Self::commit`] keeps the globals
/// once startup succeeds.
///
/// Each restore compares pointers first. A global that a third party has
/// replaced since the publish stays as it is.
pub struct AdmissionGlobalsGuard {
    gate_cache: Arc<AdmissionGateCache>,
    /// The gate cache that was global before the publish. A live sibling
    /// runtime can own it, so the drop restores it and does not clear it.
    prev_gate_cache: Option<Arc<AdmissionGateCache>>,
    metrics: Option<Arc<dyn MetricsRecorder>>,
    /// The recorder that was global before `publish_metrics`, for the same
    /// reason.
    prev_metrics: Option<Arc<dyn MetricsRecorder>>,
    committed: bool,
}

impl AdmissionGlobalsGuard {
    /// Publish the gate cache and start to guard it.
    pub fn publish_gate_cache(cache: Arc<AdmissionGateCache>) -> Self {
        GATE_CACHE_PUBLISHES.fetch_add(1, Ordering::Relaxed);
        let prev_gate_cache = global_admission_gate_cache();
        set_global_admission_gate_cache(Some(cache.clone()));
        Self {
            gate_cache: cache,
            prev_gate_cache,
            metrics: None,
            prev_metrics: None,
            committed: false,
        }
    }

    /// Publish the metrics recorder and start to guard it.
    pub fn publish_metrics(&mut self, recorder: Arc<dyn MetricsRecorder>) {
        self.prev_metrics = global_admission_metrics();
        set_global_admission_metrics(Some(recorder.clone()));
        self.metrics = Some(recorder);
    }

    /// Keep the published globals. Call this when startup succeeds.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for AdmissionGlobalsGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Restore the previous global, which a live sibling can own. Touch the
        // global only while it still holds the value this guard published.
        if let Some(installed) = global_admission_gate_cache()
            && Arc::ptr_eq(&installed, &self.gate_cache)
        {
            set_global_admission_gate_cache(self.prev_gate_cache.take());
        }
        if let Some(ref m) = self.metrics
            && let Some(installed) = global_admission_metrics()
            && Arc::ptr_eq(&installed, m)
        {
            set_global_admission_metrics(self.prev_metrics.take());
        }
    }
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

    /// A drop with no `commit()` clears both published globals.
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
            // The drop with no commit stands for a startup error.
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

    /// A drop before `publish_metrics` still clears the gate cache.
    ///
    /// Both publishes now run back to back, so no startup error site falls
    /// between them. This test checks the drop logic in isolation.
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
            // The drop comes before `publish_metrics`.
        }
        assert!(
            global_admission_gate_cache().is_none(),
            "early drop (pre-metrics-publish) clears the gate cache"
        );
        assert!(global_admission_metrics().is_none());
    }

    /// `commit()` keeps both globals published.
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

    /// Both globals are live before the runner starts (issue #618).
    ///
    /// A completion trigger that a worker blocks in the boot window then finds
    /// a live recorder. The block is counted, not dropped.
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
        // The runner starts against this state.
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

    /// A drop leaves a sibling cache in place. The clear compares pointers.
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
            // A sibling runtime replaces the global with its own cache.
            set_global_admission_gate_cache(Some(Arc::clone(&sibling)));
            // The pointers differ, so the drop does not clear the global.
        }
        assert!(
            global_admission_gate_cache().is_some_and(|c| Arc::ptr_eq(&c, &sibling)),
            "guard drop must not clobber a sibling's installed cache"
        );
        set_global_admission_gate_cache(None);
    }

    /// A drop restores the sibling globals that were there before (issue #618).
    ///
    /// The sibling was installed first. Clearing to `None` would remove its gate
    /// enforcement. The same rule applies to the metrics recorder.
    #[test]
    fn guard_drop_restores_the_previous_sibling_globals_not_none() {
        let _g = GUARD_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Sibling runtime A is live with its own cache and recorder.
        let sibling_cache = Arc::new(AdmissionGateCache::new());
        let sibling_metrics: Arc<dyn MetricsRecorder> = recorder();
        set_global_admission_gate_cache(Some(Arc::clone(&sibling_cache)));
        set_global_admission_metrics(Some(Arc::clone(&sibling_metrics)));
        {
            // Runtime B publishes over A, then fails with no commit.
            let mut guard =
                AdmissionGlobalsGuard::publish_gate_cache(Arc::new(AdmissionGateCache::new()));
            guard.publish_metrics(recorder());
            // B owns the globals now.
            assert!(
                global_admission_gate_cache().is_some_and(|c| !Arc::ptr_eq(&c, &sibling_cache)),
                "B's cache is installed over A's before the failure"
            );
        }
        // The drop restores A. It does not clear the globals.
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
mod load_shed_sampler_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use autumn_harvest::info::ActivityInfo;
    use autumn_harvest::load_shed::{LoadShedConfig, LoadShedPolicy, sample_once};
    use autumn_harvest::policy::CircuitBreakerPolicy;
    use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
    use autumn_harvest::shard::ShardRouter;
    use autumn_harvest::worker::{DbPool, HandlerRegistry};
    use diesel::sql_types::Text;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncPgConnection, RunQueryDsl};

    use super::LoadShedSamplerInputs;
    use crate::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
    use crate::state::HarvestDbPool;

    const QUEUE: &str = "ls_boot_queue";
    const BREAKER_ACTIVITY: &str = "ls_boot_breaker_activity";
    const BUCKET_KEY: &str = "ls_boot_empty_bucket";

    /// An activity with a circuit breaker, so the registry tracks its name.
    fn breaker_activity(name: &'static str) -> ActivityInfo {
        ActivityInfo {
            name,
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
            circuit_breaker: Some(CircuitBreakerPolicy::new(
                3,
                Duration::from_secs(30),
                Duration::from_secs(60),
            )),
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            input_schema: None,
            output_schema: None,
        }
    }

    fn registry() -> Arc<HandlerRegistry> {
        Arc::new(HandlerRegistry::new(
            vec![],
            vec![breaker_activity(BREAKER_ACTIVITY)],
        ))
    }

    fn runtime(registry: Arc<HandlerRegistry>) -> HarvestApiRuntime {
        HarvestApiRuntime::new(
            registry,
            Arc::new(DagCatalog::default()),
            Arc::new(Vec::new()),
            Some("ls-boot-test".to_owned()),
            vec![],
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
            ShardRouter::default(),
        )
    }

    /// Trip at 60 s with a 1 s sample interval, so the first tick decides.
    fn config() -> LoadShedConfig {
        let policy = LoadShedPolicy::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            Duration::from_secs(7),
        )
        .expect("valid policy");
        LoadShedConfig::new()
            .with_sample_interval(Duration::from_secs(1))
            .queue(QUEUE, policy)
    }

    /// The sampler inputs come from the registry, not from the installed
    /// runtime, so the first tick sees the circuit-breaker list.
    #[test]
    fn sampler_inputs_carry_the_registry_breakers() {
        let inputs = LoadShedSamplerInputs::from_registry(&registry());
        assert_eq!(
            inputs.circuit_breaker_activities(),
            [BREAKER_ACTIVITY.to_owned()],
            "the sampler must read the breaker list straight from the registry"
        );
    }

    fn build_pool(url: &str) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        deadpool::managed::Pool::builder(manager)
            .max_size(4)
            .build()
            .expect("pool build failed")
    }

    /// Remove the rows this test owns.
    async fn scrub(conn: &mut AsyncPgConnection) {
        diesel::sql_query("DELETE FROM harvest_task_queue WHERE queue_name = $1")
            .bind::<Text, _>(QUEUE)
            .execute(conn)
            .await
            .expect("scrub tasks");
        diesel::sql_query("DELETE FROM harvest_rate_limit_buckets WHERE key = $1")
            .bind::<Text, _>(BUCKET_KEY)
            .execute(conn)
            .await
            .expect("scrub bucket");
    }

    /// One old circuit-breaker activity task behind an empty rate-limit
    /// bucket. The worker bypasses the bucket for a breaker activity, so the
    /// task is claimable. The age query counts it only when the activity is
    /// in the breaker list.
    async fn seed_old_breaker_task(conn: &mut AsyncPgConnection) {
        diesel::sql_query(
            "INSERT INTO harvest_rate_limit_buckets \
                 (key, refill_rate, burst, tokens, last_refilled_at) \
             VALUES ($1, 0, 1, 0, NOW())",
        )
        .bind::<Text, _>(BUCKET_KEY)
        .execute(conn)
        .await
        .expect("insert bucket");
        diesel::sql_query(
            "INSERT INTO harvest_task_queue \
                 (queue_name, task_type, activity_name, input, state, \
                  scheduled_at, created_at, rate_limit_key) \
             VALUES ($1, 'activity', $2, '{}'::jsonb, 'PENDING', \
                     NOW() - INTERVAL '120 seconds', NOW() - INTERVAL '120 seconds', $3)",
        )
        .bind::<Text, _>(QUEUE)
        .bind::<Text, _>(BREAKER_ACTIVITY)
        .bind::<Text, _>(BUCKET_KEY)
        .execute(conn)
        .await
        .expect("insert task");
    }

    /// The sampler spawned before `install` trips on its first tick.
    ///
    /// A sample with an empty breaker list sees no claimable task, so the
    /// queue stays open. The spawned sampler reads the registry data from
    /// the runtime it was given, so it trips without an install.
    #[tokio::test]
    async fn sampler_first_tick_sees_the_registry_before_install() {
        let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
            return;
        };
        let pool = build_pool(&url);
        let mut conn = pool.get().await.expect("connect to test DB");
        scrub(&mut conn).await;
        seed_old_breaker_task(&mut conn).await;

        let api_state = HarvestApiState::new();
        let shedder = Arc::clone(api_state.gate_cache().load_shedder());
        shedder.configure(config());

        // The fallback the race used to take: no breaker list, no trip.
        let ok = sample_once(&shedder, std::slice::from_ref(&pool), &pool, None, &[]).await;
        assert!(ok, "the control sample must read the pool");
        assert!(
            shedder.check(QUEUE, std::time::Instant::now()).is_none(),
            "an empty breaker list hides the task behind the empty bucket"
        );

        // No `api_state.install(...)` here: the sampler must not need it.
        let runtime = runtime(registry());
        let gate_refresh =
            super::spawn_gate_refresh(&api_state, &HarvestDbPool::from(pool.clone()), &runtime);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut tripped = false;
        while tokio::time::Instant::now() < deadline {
            if shedder.check(QUEUE, std::time::Instant::now()).is_some() {
                tripped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        gate_refresh.stop().await;
        scrub(&mut conn).await;
        assert!(
            tripped,
            "the sampler must trip on the old breaker task without an install"
        );
    }
}

#[cfg(test)]
mod ramp_guard_spawn_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use autumn_harvest::ramp_guard::RampGuardConfig;
    use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
    use autumn_harvest::shard::ShardRouter;
    use autumn_harvest::worker::{DbPool, HandlerRegistry};
    use diesel_async::AsyncPgConnection;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use tokio_util::sync::CancellationToken;

    use super::spawn_ramp_guard;
    use crate::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
    use crate::state::HarvestDbPool;

    /// A pool that never connects: the pool is lazy, and the tests do not
    /// need a database.
    fn lazy_pool() -> HarvestDbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://nobody@127.0.0.1:1/none",
        );
        let pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(1)
            .build()
            .expect("lazy pool");
        HarvestDbPool::from(pool)
    }

    fn runtime() -> HarvestApiRuntime {
        HarvestApiRuntime::new(
            Arc::new(HandlerRegistry::new(vec![], vec![])),
            Arc::new(DagCatalog::default()),
            Arc::new(Vec::new()),
            Some("ramp-guard-boot-test".to_owned()),
            vec![],
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
            ShardRouter::default(),
        )
    }

    /// Issue #1814: the default config spawns no guard, so a default
    /// deployment runs no guard SQL.
    #[tokio::test]
    async fn default_config_spawns_no_ramp_guard() {
        let api_state = HarvestApiState::new();
        let handle = spawn_ramp_guard(
            &api_state,
            &lazy_pool(),
            &runtime(),
            CancellationToken::new(),
        );
        assert!(handle.is_none());
    }

    /// Issue #1814: an enabled config spawns the guard loop, and a cancel
    /// stops it.
    #[tokio::test]
    async fn enabled_config_spawns_a_ramp_guard_that_stops_on_cancel() {
        let api_state = HarvestApiState::new();
        api_state
            .set_ramp_guard_config(RampGuardConfig::new().with_interval(Duration::from_secs(3600)));
        let cancel = CancellationToken::new();
        let handle = spawn_ramp_guard(&api_state, &lazy_pool(), &runtime(), cancel.clone())
            .expect("an enabled guard spawns");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("the guard stops on cancel")
            .expect("the guard task does not panic");
    }
}
