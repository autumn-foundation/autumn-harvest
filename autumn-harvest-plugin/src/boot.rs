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
use autumn_harvest::worker::DbPool;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{HarvestApiState, acquire_conn};
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
}

impl GateRefreshRuntime {
    /// Cancel the loops and wait for them to end.
    pub async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.handle.await;
        if let Some(load_shed) = self.load_shed {
            let _ = load_shed.await;
        }
    }
}

/// Spawn the loop that reloads the gate cache of `api_state` every second.
///
/// The loop fails closed. When the gate table is unreadable, the cache
/// becomes uninitialized, so `check()` blocks new starts. A stale open
/// snapshot would admit them.
///
/// It also spawns the load-shed sampler when a queue has a policy.
pub fn spawn_gate_refresh(
    api_state: &HarvestApiState,
    pools: &HarvestDbPool,
) -> GateRefreshRuntime {
    let shutdown = CancellationToken::new();
    let cancel = shutdown.child_token();
    let load_shed = spawn_load_shed_sampler(api_state, pools, shutdown.child_token());
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
    }
}

/// Spawn the load-shed sampler of `api_state` (issue #1794).
///
/// Returns `None` when no queue has a policy, so a default deployment runs no
/// sampler SQL. The sampler reads each physical pool once per tick.
fn spawn_load_shed_sampler(
    api_state: &HarvestApiState,
    pools: &HarvestDbPool,
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
    let api_state = api_state.clone();
    Some(tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            // The runtime is installed after boot, so resolve it per tick.
            let runtime = api_state.runtime().ok();
            let metrics = runtime
                .as_ref()
                .map(|r| Arc::clone(&r.registry().telemetry().metrics));
            let breakers = runtime
                .as_ref()
                .map(|r| {
                    r.registry()
                        .circuit_breakers()
                        .tracked_activity_names()
                        .to_vec()
                })
                .unwrap_or_default();
            let sample = autumn_harvest::load_shed::sample_once(
                &shedder,
                &shard_pools,
                &audit_pool,
                metrics.as_deref(),
                &breakers,
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
