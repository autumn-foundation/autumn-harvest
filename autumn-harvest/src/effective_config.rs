//! Effective runtime-configuration introspection (issue #695).
//!
//! Answers the operator question "what configuration is this fleet actually
//! running with, right now?" without reading source, env files, or the
//! builder chain. The [`EffectiveConfigView`] is the serialisable, **secret-free**
//! snapshot of the resolved [`WorkerConfig`](crate::builder::WorkerConfig),
//! payload caps, shard topology, compiled feature flags, and pool sizing that
//! the plugin captures once at startup and serves from
//! `GET /api/harvest/admin/config`.
//!
//! ## Redaction by construction
//!
//! Secret-bearing configuration — the LISTEN/NOTIFY [`notification_database_url`],
//! the per-shard [`shard_notification_database_urls`], and the live
//! [`sharded_pool`] handle — is **never stored in the view**. Instead it is
//! surfaced only as presence booleans and counts
//! ([`WorkerConfigView::notification_channel_configured`],
//! [`WorkerConfigView::shard_notification_channels_configured`],
//! [`WorkerConfigView::sharded_pool_configured`]). A connection URL (which can
//! embed a password) can therefore never appear in a serialized response.
//!
//! [`notification_database_url`]: crate::builder::WorkerConfig::notification_database_url
//! [`shard_notification_database_urls`]: crate::builder::WorkerConfig::shard_notification_database_urls
//! [`sharded_pool`]: crate::builder::WorkerConfig
//!
//! ## Coverage guard (the falsifiable #695 acceptance criterion)
//!
//! [`WorkerConfigView::from_worker_config`] destructures [`WorkerConfig`] with
//! an **exhaustive pattern and NO `..`**. Adding a new operator-tunable field to
//! `WorkerConfig` is a **compile error** here until the author decides how to
//! surface it (a value, a derived boolean, or an explicit
//! presence-only-for-secrets mapping). This is stronger than any runtime test.
//!
//! ## Duration representation
//!
//! Every duration is serialized as **whole milliseconds** (`u64`) with a
//! `_ms`-suffixed field name — one tunable (the worker poll interval) is 500 ms,
//! so a seconds representation would lose precision. Unset `Option<Duration>`
//! ceilings serialize as an explicit JSON `null` with the key still present, so
//! "unbounded" is unambiguous and distinguishable from "field omitted".

use std::collections::BTreeMap;
use std::time::Duration;

use crate::builder::WorkerConfig;

/// Convert a [`Duration`] to whole milliseconds, saturating at [`u64::MAX`].
fn dur_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Convert an optional [`Duration`] to `Option<u64>` milliseconds. `None` maps
/// to `None` (serialized as an explicit JSON `null`, key preserved).
fn opt_dur_ms(d: Option<Duration>) -> Option<u64> {
    d.map(dur_ms)
}

/// Compiled-in Cargo feature flags of the **core** `autumn-harvest` crate.
///
/// Evaluated with `cfg!` *inside* this crate so the result reflects how the
/// engine binary was actually compiled — not the plugin crate's own feature
/// set, which can legitimately differ.
#[must_use]
pub const fn compiled_feature_flags() -> FeatureFlagsView {
    FeatureFlagsView {
        db: cfg!(feature = "db"),
        unified_dag_execution: cfg!(feature = "unified-dag-execution"),
        schema: cfg!(feature = "schema"),
        metrics_rs: cfg!(feature = "metrics-rs"),
        testing: cfg!(feature = "testing"),
    }
}

/// Resolved effective runtime configuration of the fleet, secret-free.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EffectiveConfigView {
    /// Resolved worker concurrency, queue, timeout, and routing configuration.
    pub worker: WorkerConfigView,
    /// Payload size caps and server-side ceilings.
    pub payload_caps: PayloadCapsView,
    /// Shard read/write topology resolved from the [`ShardRouter`](crate::shard::ShardRouter).
    pub shard_topology: ShardTopologyView,
    /// Compiled-in core-crate feature flags.
    pub features: FeatureFlagsView,
    /// Resolved database pool sizing.
    pub pool: PoolConfigView,
}

/// Secret-free projection of [`WorkerConfig`].
///
/// This is a flat serialization DTO: each boolean is an independent, orthogonal
/// configuration flag (or a secret-redaction presence marker), not a state
/// machine, so the excessive-bools lint does not apply.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerConfigView {
    /// Task queues this worker polls.
    pub queues: Vec<String>,
    /// Per-queue dispatch weights (sorted for stable output). Empty = equal share.
    pub queue_weights: BTreeMap<String, u32>,
    /// Maximum concurrent workflow executions.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions.
    pub max_concurrent_activities: usize,
    /// Worker poll interval, milliseconds.
    pub poll_interval_ms: u64,
    /// Graceful shutdown timeout, milliseconds.
    pub shutdown_timeout_ms: u64,
    /// In-memory workflow LRU cache size (entries).
    pub workflow_cache_size: usize,
    /// Whether sticky cross-worker routing is enabled (`sticky_timeout > 0`).
    pub sticky_routing_enabled: bool,
    /// Sticky routing lease TTL, milliseconds (0 = disabled).
    pub sticky_timeout_ms: u64,
    /// Activity cooperative-cancellation grace period, milliseconds.
    pub cancellation_grace_period_ms: u64,
    /// Shards this worker polls.
    pub shard_assignments: Vec<i32>,
    /// Hard cap on local-activity `start_to_close`, milliseconds.
    pub max_local_activity_start_to_close_ms: u64,
    /// Worker liveness heartbeat interval, milliseconds.
    pub worker_heartbeat_interval_ms: u64,
    /// Immutable build identifier (may be empty; not a secret).
    pub build_id: String,
    /// Optional human-readable deployment name.
    pub deployment_name: Option<String>,
    /// Per-query execution timeout, milliseconds.
    pub query_timeout_ms: u64,
    /// Priority-aging period in seconds (`null` = aging disabled).
    pub priority_aging_secs: Option<u32>,
    /// Maximum workflow start delay, milliseconds.
    pub max_workflow_start_delay_ms: u64,
    /// Grace window before cross-workflow signaling fails for an unknown target, milliseconds.
    pub unknown_target_grace_window_ms: u64,
    /// Consecutive crash strikes before poison-pill quarantine.
    pub poison_pill_threshold: i32,
    /// Whether poison-pill quarantine is enabled (`poison_pill_threshold > 0`).
    pub poison_pill_quarantine_enabled: bool,
    /// Wall-clock budget for a single workflow-task dispatch, milliseconds (0 = disabled).
    pub workflow_task_timeout_ms: u64,
    /// Bounded-pause auto-resume ceiling, milliseconds.
    pub max_workflow_pause_duration_ms: u64,
    /// Capability labels for hardware/regional routing (sorted for stable output).
    pub labels: BTreeMap<String, String>,
    /// `WorkerConfig`-level hard history-event ceiling (`null` = unbounded).
    ///
    /// This is the **raw `WorkerConfig` knob**. The *resolved* effective ceiling
    /// (builder value falling back to this one) is surfaced separately as
    /// [`PayloadCapsView::max_workflow_history_events`]; both are reported so an
    /// operator can distinguish the worker-level default from the value actually
    /// enforced.
    pub max_workflow_history_events: Option<u64>,
    /// Default debounce max-wait cap, milliseconds.
    pub default_debounce_max_wait_ms: u64,
    /// Whether the adaptive dispatch-slot tuner is enabled.
    pub slot_tuner_enabled: bool,
    /// Advertised concurrent worker-session capacity (0 = sessions disabled).
    pub max_concurrent_sessions: i32,
    /// REDACTED: whether a LISTEN/NOTIFY notification URL is configured (never the URL).
    pub notification_channel_configured: bool,
    /// REDACTED: count of per-shard notification channels configured (never the URLs).
    pub shard_notification_channels_configured: usize,
    /// REDACTED: whether a sharded database pool is configured (never the handle).
    pub sharded_pool_configured: bool,
    /// REDACTED: number of shards in the configured sharded pool (0 if none).
    pub sharded_pool_shard_count: usize,
}

impl WorkerConfigView {
    /// Project a [`WorkerConfig`] into its secret-free view.
    ///
    /// The exhaustive destructure below is the **#695 coverage guard** — do NOT
    /// add `..`. A new [`WorkerConfig`] field must break compilation here until
    /// it is deliberately surfaced (or, for a secret-bearing field, mapped to a
    /// presence boolean/count).
    #[must_use]
    pub fn from_worker_config(worker: &WorkerConfig, poll_interval: Duration) -> Self {
        // Bind the sharded-pool presence/count before the (feature-gated)
        // destructure so both `db` and non-`db` builds produce the same view.
        #[cfg(feature = "db")]
        let (sharded_pool_configured, sharded_pool_shard_count) = worker
            .sharded_pool
            .as_ref()
            .map_or((false, 0), |p| (true, p.iter_shards().count()));
        #[cfg(not(feature = "db"))]
        let (sharded_pool_configured, sharded_pool_shard_count) = (false, 0usize);

        // exhaustive destructure is the #695 coverage guard — do NOT add `..`.
        let WorkerConfig {
            queues,
            queue_weights,
            // REDACTED — presence only, never the URL (may embed a password).
            notification_database_url,
            // REDACTED — count only, never the URLs.
            shard_notification_database_urls,
            max_concurrent_workflows,
            max_concurrent_activities,
            shutdown_timeout,
            workflow_cache_size,
            sticky_timeout,
            cancellation_grace_period,
            shard_assignments,
            max_local_activity_start_to_close,
            worker_heartbeat_interval,
            build_id,
            deployment_name,
            query_timeout,
            priority_aging_secs,
            max_workflow_start_delay,
            unknown_target_grace_window,
            poison_pill_threshold,
            workflow_task_timeout,
            max_workflow_pause_duration,
            labels,
            max_workflow_history_events,
            default_debounce_max_wait,
            slot_tuner,
            // REDACTED — presence/count only, never the live handle. Bound above.
            #[cfg(feature = "db")]
                sharded_pool: _,
            max_concurrent_sessions,
        } = worker;

        Self {
            queues: queues.clone(),
            queue_weights: queue_weights.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            max_concurrent_workflows: *max_concurrent_workflows,
            max_concurrent_activities: *max_concurrent_activities,
            poll_interval_ms: dur_ms(poll_interval),
            shutdown_timeout_ms: dur_ms(*shutdown_timeout),
            workflow_cache_size: *workflow_cache_size,
            sticky_routing_enabled: !sticky_timeout.is_zero(),
            sticky_timeout_ms: dur_ms(*sticky_timeout),
            cancellation_grace_period_ms: dur_ms(*cancellation_grace_period),
            shard_assignments: shard_assignments.iter().map(|s| s.as_i32()).collect(),
            max_local_activity_start_to_close_ms: dur_ms(*max_local_activity_start_to_close),
            worker_heartbeat_interval_ms: dur_ms(*worker_heartbeat_interval),
            build_id: build_id.clone(),
            deployment_name: deployment_name.clone(),
            query_timeout_ms: dur_ms(*query_timeout),
            priority_aging_secs: *priority_aging_secs,
            max_workflow_start_delay_ms: dur_ms(*max_workflow_start_delay),
            unknown_target_grace_window_ms: dur_ms(*unknown_target_grace_window),
            poison_pill_threshold: *poison_pill_threshold,
            poison_pill_quarantine_enabled: *poison_pill_threshold > 0,
            workflow_task_timeout_ms: dur_ms(*workflow_task_timeout),
            max_workflow_pause_duration_ms: dur_ms(*max_workflow_pause_duration),
            labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            max_workflow_history_events: *max_workflow_history_events,
            default_debounce_max_wait_ms: dur_ms(*default_debounce_max_wait),
            slot_tuner_enabled: slot_tuner.is_some(),
            max_concurrent_sessions: *max_concurrent_sessions,
            notification_channel_configured: notification_database_url.is_some(),
            shard_notification_channels_configured: shard_notification_database_urls.len(),
            sharded_pool_configured,
            sharded_pool_shard_count,
        }
    }
}

/// Payload size caps and server-side ceilings.
///
/// `Option<_>` ceilings serialize as an explicit JSON `null` when unset
/// ("unbounded"); the key is always present.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PayloadCapsView {
    /// Maximum activity input payload, bytes.
    pub max_activity_input_bytes: u64,
    /// Maximum activity result payload, bytes.
    pub max_activity_result_bytes: u64,
    /// Maximum signal payload, bytes.
    pub max_signal_payload_bytes: u64,
    /// Maximum workflow start input payload, bytes.
    pub max_workflow_input_bytes: u64,
    /// Maximum `current_details` string, bytes.
    pub max_current_details_bytes: usize,
    /// Server-side `execution_timeout` ceiling, milliseconds (`null` = unbounded).
    pub max_workflow_execution_timeout_ms: Option<u64>,
    /// Server-side workflow retry-attempt ceiling (`null` = unbounded).
    pub max_workflow_attempts: Option<u32>,
    /// Server-side hard history-event ceiling (`null` = unbounded).
    ///
    /// This is the **resolved effective ceiling** — the builder value falling
    /// back to the raw `WorkerConfig` knob, which is also reported separately as
    /// [`WorkerConfigView::max_workflow_history_events`]. Both appear
    /// deliberately (raw knob vs. enforced ceiling), not by mistake.
    pub max_workflow_history_events: Option<u64>,
    /// `GET /admin/usage` window ceiling, milliseconds.
    pub usage_window_ceiling_ms: u64,
    /// `GET /admin/usage` group-count cap.
    pub usage_max_groups: usize,
    /// Whether large-payload offloading (a `PayloadStore`) is configured.
    pub payload_offload_enabled: bool,
    /// Byte threshold above which payload fields are offloaded.
    pub payload_offload_threshold_bytes: u64,
}

impl PayloadCapsView {
    /// Build a [`PayloadCapsView`] from the resolved
    /// [`BuiltHarvest`](crate::builder::BuiltHarvest) cap values, converting the
    /// duration-typed ceilings to milliseconds here so the millisecond
    /// representation stays single-sourced (issue #695).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_activity_input_bytes: u64,
        max_activity_result_bytes: u64,
        max_signal_payload_bytes: u64,
        max_workflow_input_bytes: u64,
        max_current_details_bytes: usize,
        max_workflow_execution_timeout: Option<Duration>,
        max_workflow_attempts: Option<u32>,
        max_workflow_history_events: Option<u64>,
        usage_window_ceiling: Duration,
        usage_max_groups: usize,
        payload_offload_enabled: bool,
        payload_offload_threshold_bytes: u64,
    ) -> Self {
        Self {
            max_activity_input_bytes,
            max_activity_result_bytes,
            max_signal_payload_bytes,
            max_workflow_input_bytes,
            max_current_details_bytes,
            max_workflow_execution_timeout_ms: opt_dur_ms(max_workflow_execution_timeout),
            max_workflow_attempts,
            max_workflow_history_events,
            usage_window_ceiling_ms: dur_ms(usage_window_ceiling),
            usage_max_groups,
            payload_offload_enabled,
            payload_offload_threshold_bytes,
        }
    }
}

/// Shard read/write topology resolved from the [`ShardRouter`](crate::shard::ShardRouter).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardTopologyView {
    /// Shards the deployment can read from.
    pub readable_shards: Vec<i32>,
    /// Shards that accept new workflows.
    pub writable_shards: Vec<i32>,
    /// The default shard used to resolve unencoded execution IDs.
    pub default_shard: i32,
}

impl ShardTopologyView {
    /// Project a [`ShardRouter`](crate::shard::ShardRouter) into its view.
    #[must_use]
    pub fn from_router(router: &crate::shard::ShardRouter) -> Self {
        Self {
            readable_shards: router
                .readable_shards()
                .iter()
                .map(|s| s.as_i32())
                .collect(),
            writable_shards: router
                .writable_shards()
                .iter()
                .map(|s| s.as_i32())
                .collect(),
            default_shard: router.default_shard().as_i32(),
        }
    }
}

/// Compiled-in core-crate feature flags.
///
/// A flat serialization DTO: each boolean reports one independent compiled-in
/// Cargo feature, not a state machine, so the excessive-bools lint does not apply.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct FeatureFlagsView {
    /// The `db` (Diesel/Postgres) feature.
    pub db: bool,
    /// The `unified-dag-execution` feature.
    pub unified_dag_execution: bool,
    /// The `schema` (JSON Schema publishing) feature.
    pub schema: bool,
    /// The `metrics-rs` adapter feature.
    pub metrics_rs: bool,
    /// The `testing` harness feature.
    pub testing: bool,
}

/// Resolved database pool sizing.
///
/// The plugin resolves a **single** database pool, so
/// [`worker_pool_max_connections`](PoolConfigView::worker_pool_max_connections)
/// is the effective *total* connection budget for the deployment — there is no
/// separate `max_total_connections` field because there is no second pool to
/// aggregate. [`shard_pool_count`](PoolConfigView::shard_pool_count) is derived
/// from the resolved shard topology (always `1` on the plugin path, which
/// rejects multi-shard deployments).
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct PoolConfigView {
    /// Maximum connections in the worker/default-shard pool. In the single-pool
    /// model this is also the effective total connection budget.
    pub worker_pool_max_connections: usize,
    /// Number of shard pools resolved.
    pub shard_pool_count: usize,
}

/// Connection-budget sizing read off one resolved database pool.
///
/// This is the **DB-free** input to [`resolve_pool_view`]: the capture site
/// reads the two numbers off a live pool (which needs a DB), then hands them to
/// the pure precedence decision, which is testable without ever constructing a
/// real pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSizing {
    /// Maximum connections in the (default-shard) pool.
    pub max_connections: usize,
    /// Number of shard pools the pool spans.
    pub shard_count: usize,
}

/// Resolve the reported [`PoolConfigView`], mirroring the pool-selection
/// precedence in `HarvestRunner::start` (issue #695 review).
///
/// The runner gives a configured sharded pool
/// (`WorkerConfig::with_sharded_pool`) precedence over the fallback
/// single-shard pool derived from `harvest_pool`. The effective-config snapshot
/// must therefore report the sizing of the pool the runtime *actually* uses:
/// when a sharded pool is present its sizing wins, otherwise the fallback
/// sizing is reported.
#[must_use]
pub fn resolve_pool_view(sharded_pool: Option<PoolSizing>, fallback: PoolSizing) -> PoolConfigView {
    let chosen = sharded_pool.unwrap_or(fallback);
    PoolConfigView {
        worker_pool_max_connections: chosen.max_connections,
        shard_pool_count: chosen.shard_count,
    }
}

impl EffectiveConfigView {
    /// Assemble the full effective-config view from its resolved parts.
    ///
    /// `poll_interval` is the runtime-resolved worker poll interval; `caps` and
    /// `pool` are built by the caller from the resolved
    /// [`BuiltHarvest`](crate::builder::BuiltHarvest) and database pool.
    #[must_use]
    pub fn capture(
        worker: &WorkerConfig,
        caps: PayloadCapsView,
        router: &crate::shard::ShardRouter,
        pool: PoolConfigView,
        poll_interval: Duration,
    ) -> Self {
        Self {
            worker: WorkerConfigView::from_worker_config(worker, poll_interval),
            payload_caps: caps,
            shard_topology: ShardTopologyView::from_router(router),
            features: compiled_feature_flags(),
            pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_caps() -> PayloadCapsView {
        PayloadCapsView {
            max_activity_input_bytes: 111,
            max_activity_result_bytes: 222,
            max_signal_payload_bytes: 333,
            max_workflow_input_bytes: 444,
            max_current_details_bytes: 555,
            max_workflow_execution_timeout_ms: None,
            max_workflow_attempts: None,
            max_workflow_history_events: None,
            usage_window_ceiling_ms: 90 * 24 * 3600 * 1000,
            usage_max_groups: 10_000,
            payload_offload_enabled: false,
            payload_offload_threshold_bytes: 262_144,
        }
    }

    #[test]
    fn redaction_never_leaks_a_connection_url() {
        let worker = WorkerConfig {
            notification_database_url: Some(
                "postgres://user:password@dbhost:5432/harvest".to_string(),
            ),
            shard_notification_database_urls: vec![(
                crate::types::ShardId::new(0),
                "postgres://user:secret@shardhost:5432/harvest".to_string(),
            )],
            ..Default::default()
        };

        let view = WorkerConfigView::from_worker_config(&worker, Duration::from_millis(500));
        let json = serde_json::to_string(&view).expect("serialize");

        assert!(!json.contains("password"), "leaked password: {json}");
        assert!(!json.contains("secret"), "leaked secret: {json}");
        assert!(!json.contains("dbhost"), "leaked host: {json}");
        assert!(!json.contains("shardhost"), "leaked shard host: {json}");
        assert!(!json.contains("postgres://"), "leaked URL scheme: {json}");

        assert!(view.notification_channel_configured);
        assert_eq!(view.shard_notification_channels_configured, 1);
    }

    #[test]
    fn sentinel_coverage_surfaces_distinctive_values() {
        let worker = WorkerConfig::default()
            .with_queues(["alpha", "beta"])
            .with_queue_weights([("alpha", 7u32)])
            .with_build_id("build-sentinel-9001")
            .with_deployment_name("prod-blue-sentinel")
            .with_worker_heartbeat_interval(Duration::from_secs(11))
            .with_query_timeout(Duration::from_secs(13));

        let view = WorkerConfigView::from_worker_config(&worker, Duration::from_millis(500));
        let json = serde_json::to_value(&view).expect("serialize");

        assert_eq!(json["queues"], serde_json::json!(["alpha", "beta"]));
        assert_eq!(json["queue_weights"]["alpha"], 7);
        assert_eq!(json["build_id"], "build-sentinel-9001");
        assert_eq!(json["deployment_name"], "prod-blue-sentinel");
        assert_eq!(json["worker_heartbeat_interval_ms"], 11_000);
        assert_eq!(json["query_timeout_ms"], 13_000);
        assert_eq!(json["poll_interval_ms"], 500);
    }

    #[test]
    fn unbounded_ceilings_serialize_as_explicit_null_with_key_present() {
        // These ceilings are `None` in the default config; assert they surface
        // as an explicit JSON null (key present) rather than being omitted.
        let worker = WorkerConfig {
            priority_aging_secs: None,
            max_workflow_history_events: None,
            ..Default::default()
        };

        let worker_view = WorkerConfigView::from_worker_config(&worker, Duration::from_millis(500));
        let worker_json = serde_json::to_value(&worker_view).expect("serialize");

        // Key must be present AND value must be JSON null (not omitted).
        assert!(
            worker_json
                .as_object()
                .unwrap()
                .contains_key("priority_aging_secs")
        );
        assert!(worker_json["priority_aging_secs"].is_null());
        assert!(
            worker_json
                .as_object()
                .unwrap()
                .contains_key("max_workflow_history_events")
        );
        assert!(worker_json["max_workflow_history_events"].is_null());

        let caps = sample_caps();
        let caps_json = serde_json::to_value(&caps).expect("serialize");
        for key in [
            "max_workflow_execution_timeout_ms",
            "max_workflow_attempts",
            "max_workflow_history_events",
        ] {
            assert!(
                caps_json.as_object().unwrap().contains_key(key),
                "missing key {key}"
            );
            assert!(caps_json[key].is_null(), "expected null for {key}");
        }
    }

    #[test]
    fn derived_bools_track_their_source_tunable() {
        // Off: zero sticky timeout, zero poison-pill threshold, no tuner.
        let off = WorkerConfig {
            sticky_timeout: Duration::ZERO,
            poison_pill_threshold: 0,
            slot_tuner: None,
            ..Default::default()
        };
        let off_view = WorkerConfigView::from_worker_config(&off, Duration::from_millis(500));
        assert!(!off_view.sticky_routing_enabled);
        assert!(!off_view.poison_pill_quarantine_enabled);
        assert!(!off_view.slot_tuner_enabled);

        // On: non-zero sticky timeout and poison-pill threshold.
        let on = WorkerConfig {
            sticky_timeout: Duration::from_secs(15),
            poison_pill_threshold: 3,
            ..Default::default()
        };
        let on_view = WorkerConfigView::from_worker_config(&on, Duration::from_millis(500));
        assert!(on_view.sticky_routing_enabled);
        assert_eq!(on_view.sticky_timeout_ms, 15_000);
        assert!(on_view.poison_pill_quarantine_enabled);
    }

    #[test]
    fn shard_topology_reflects_router() {
        let single = ShardTopologyView::from_router(&crate::shard::ShardRouter::single());
        assert_eq!(single.readable_shards, vec![0]);
        assert_eq!(single.writable_shards, vec![0]);
        assert_eq!(single.default_shard, 0);

        let multi = crate::shard::ShardRouter::new(
            vec![
                crate::types::ShardId::new(0),
                crate::types::ShardId::new(1),
                crate::types::ShardId::new(2),
            ],
            vec![crate::types::ShardId::new(0), crate::types::ShardId::new(1)],
            crate::types::ShardId::new(0),
        );
        let view = ShardTopologyView::from_router(&multi);
        assert_eq!(view.readable_shards, vec![0, 1, 2]);
        assert_eq!(view.writable_shards, vec![0, 1]);
    }

    #[test]
    fn feature_flags_reflect_compiled_cfg() {
        let flags = compiled_feature_flags();
        assert_eq!(flags.db, cfg!(feature = "db"));
        assert_eq!(
            flags.unified_dag_execution,
            cfg!(feature = "unified-dag-execution")
        );
        assert_eq!(flags.schema, cfg!(feature = "schema"));
        assert_eq!(flags.metrics_rs, cfg!(feature = "metrics-rs"));
        assert_eq!(flags.testing, cfg!(feature = "testing"));
    }

    #[test]
    fn payload_caps_new_converts_durations_to_ms() {
        let bounded = PayloadCapsView::new(
            1,
            2,
            3,
            4,
            5,
            Some(Duration::from_secs(30)),
            Some(7),
            Some(9),
            Duration::from_secs(60),
            42,
            true,
            262_144,
        );
        assert_eq!(bounded.max_workflow_execution_timeout_ms, Some(30_000));
        assert_eq!(bounded.usage_window_ceiling_ms, 60_000);
        assert_eq!(bounded.max_workflow_attempts, Some(7));
        assert_eq!(bounded.max_workflow_history_events, Some(9));
        assert!(bounded.payload_offload_enabled);

        let unbounded = PayloadCapsView::new(
            1,
            2,
            3,
            4,
            5,
            None,
            None,
            None,
            Duration::from_secs(1),
            1,
            false,
            0,
        );
        assert_eq!(unbounded.max_workflow_execution_timeout_ms, None);
    }

    #[test]
    fn capture_assembles_all_sections() {
        let worker = WorkerConfig::default();
        let view = EffectiveConfigView::capture(
            &worker,
            sample_caps(),
            &crate::shard::ShardRouter::single(),
            PoolConfigView {
                worker_pool_max_connections: 10,
                shard_pool_count: 1,
            },
            Duration::from_millis(500),
        );
        let json = serde_json::to_value(&view).expect("serialize");
        for key in [
            "worker",
            "payload_caps",
            "shard_topology",
            "features",
            "pool",
        ] {
            assert!(
                json.as_object().unwrap().contains_key(key),
                "top-level key missing: {key}"
            );
        }
        assert_eq!(json["pool"]["worker_pool_max_connections"], 10);
        assert_eq!(json["pool"]["shard_pool_count"], 1);
    }

    #[test]
    fn resolve_pool_view_prefers_sharded_pool_sizing_when_present() {
        // Mirrors the runner precedence: a configured sharded pool
        // (WorkerConfig::with_sharded_pool) wins over the fallback pool, so the
        // snapshot must report the sharded pool's sizing — not the fallback's.
        let fallback = PoolSizing {
            max_connections: 10,
            shard_count: 1,
        };
        let sharded = PoolSizing {
            max_connections: 42,
            shard_count: 3,
        };
        let view = resolve_pool_view(Some(sharded), fallback);
        assert_eq!(view.worker_pool_max_connections, 42);
        assert_eq!(view.shard_pool_count, 3);
    }

    #[test]
    fn resolve_pool_view_falls_back_when_no_sharded_pool() {
        // No WorkerConfig::with_sharded_pool → the runtime uses the single-shard
        // wrapper of harvest_pool, so the snapshot reports the fallback sizing.
        let fallback = PoolSizing {
            max_connections: 17,
            shard_count: 1,
        };
        let view = resolve_pool_view(None, fallback);
        assert_eq!(view.worker_pool_max_connections, 17);
        assert_eq!(view.shard_pool_count, 1);
    }
}
