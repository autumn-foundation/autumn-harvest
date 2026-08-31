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
    /// Builder-level default activity retry `max_attempts` (issue #620);
    /// `null` when no builder-default retry floor is configured.
    pub default_activity_retry_max_attempts: Option<u32>,
    /// Builder-level default activity `start_to_close`, milliseconds (issue #620);
    /// `null` when no builder-default timeout floor is configured.
    pub default_activity_start_to_close_ms: Option<u64>,
    /// Ceiling on an author-supplied `Retry-After` delay hint, milliseconds
    /// (issue #744). Always present (not opt-in); default 15 minutes.
    pub retry_after_ceiling_ms: u64,
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
    /// Whether cross-region DR write-authority fencing is enabled (issue #954).
    ///
    /// The single most consequential DR setting to be able to read back from a
    /// running fleet: with it off, a failover fence does not bite on this
    /// worker at all.
    pub dr_fencing: bool,
    /// DR sampler cadence, milliseconds — the RPO's resolution floor and the
    /// bound on fence-detection latency (issue #954).
    pub replication_sample_interval_ms: u64,
    /// Trailing watermark retention, milliseconds — the ceiling on measurable
    /// replication lag (issue #954).
    pub replication_watermark_retain_ms: u64,
    /// Slot-name prefix identifying this shard's DR replication (issue #954).
    ///
    /// Worth reading back from a live fleet: a prefix that matches nothing
    /// reports the shard as having no standby, and a prefix that is too broad
    /// counts an unrelated walsender as one.
    pub replication_slot_prefix: String,
    /// Maximum workflow start delay, milliseconds.
    pub max_workflow_start_delay_ms: u64,
    /// Grace window before cross-workflow signaling fails for an unknown target, milliseconds.
    pub unknown_target_grace_window_ms: u64,
    /// Consecutive crash strikes before poison-pill quarantine.
    pub poison_pill_threshold: i32,
    /// Whether poison-pill quarantine is enabled (`poison_pill_threshold > 0`).
    pub poison_pill_quarantine_enabled: bool,
    /// Consecutive capability misses (claims by a worker with no handler
    /// registered for the task's type) before a task escalates to the ordinary
    /// terminal-failure path with a `no_capable_worker:` reason (issue #804).
    /// `0` escalates on the first miss.
    ///
    /// **No dead-letter row is written.** The escalation routes through
    /// `fail_task_and_execution_with_history`, which fails the task and the
    /// execution without inserting into `harvest_dead_letters` — the reason
    /// lives on the failed execution row, so an operator diagnosing an
    /// exhausted budget queries failed workflows, not the DLQ. (A DLQ entry on
    /// this path would also be indistinguishable from a poison-pill
    /// quarantine, #367, which a capability miss deliberately is not.)
    pub capability_miss_max_redeliveries: u32,
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
    /// Durable-mutex (`ctx.mutex`) lease TTL, milliseconds (issue #691).
    pub mutex_lease_ttl_ms: u64,
    /// Whether the adaptive dispatch-slot tuner is enabled.
    pub slot_tuner_enabled: bool,
    /// Advertised concurrent worker-session capacity (0 = sessions disabled).
    pub max_concurrent_sessions: i32,
    /// Max panic strikes before a panicking workflow task fails terminally
    /// (0 = terminal on first panic).
    pub workflow_panic_max_attempts: u32,
    /// REDACTED: whether a LISTEN/NOTIFY notification URL is configured (never the URL).
    pub notification_channel_configured: bool,
    /// REDACTED: count of per-shard notification channels configured (never the URLs).
    pub shard_notification_channels_configured: usize,
    /// REDACTED: whether the **resolved runtime** database pool is sharded
    /// (never the handle).
    ///
    /// This reflects the pool the runtime *actually* uses — the runner-provided
    /// [`HarvestRunnerResources::with_sharded_pool`] override, then
    /// [`WorkerConfig::with_sharded_pool`], then the fallback single pool — not
    /// solely the `WorkerConfig` knob. The capture site passes the resolved
    /// value through [`ShardedInfo`]; when no override is supplied (the pure
    /// no-DB mapping path) it falls back to the `WorkerConfig::sharded_pool`
    /// field.
    pub sharded_pool_configured: bool,
    /// REDACTED: number of shards in the resolved runtime sharded pool (0 if not
    /// sharded). See [`sharded_pool_configured`](Self::sharded_pool_configured).
    pub sharded_pool_shard_count: usize,
}

/// Shard ids of the pool the runtime actually uses (issue #961).
///
/// A resolved-runtime override wins over the raw [`WorkerConfig::sharded_pool`]
/// knob, because a runner can supply a sharded pool the `WorkerConfig` never
/// saw. This is what makes `GET /admin/config` report the *effective*
/// `shard_assignments` an auto-configured worker resolves to.
fn resolved_pool_shards(
    worker: &WorkerConfig,
    resolved: Option<&ShardedInfo>,
) -> Vec<crate::types::ShardId> {
    if let Some(info) = resolved {
        return info
            .shard_ids
            .iter()
            .copied()
            .map(crate::types::ShardId::new)
            .collect();
    }
    #[cfg(feature = "db")]
    {
        worker
            .sharded_pool
            .as_ref()
            .map(crate::shard::ShardedDbPool::shard_ids)
            .unwrap_or_default()
    }
    #[cfg(not(feature = "db"))]
    {
        let _ = worker;
        Vec::new()
    }
}

impl WorkerConfigView {
    /// Project a [`WorkerConfig`] into its secret-free view, sourcing the two
    /// sharded-pool fields solely from the `WorkerConfig::sharded_pool` knob.
    ///
    /// This is the **pure, no-DB mapping** used by unit tests. Runtimes call
    /// [`from_worker_config_with_resolved_sharding`](Self::from_worker_config_with_resolved_sharding)
    /// with the resolved-pool override so the reported values describe the pool
    /// the runtime actually uses.
    #[must_use]
    pub fn from_worker_config(worker: &WorkerConfig, poll_interval: Duration) -> Self {
        Self::from_worker_config_with_resolved_sharding(worker, poll_interval, None)
    }

    /// Project a [`WorkerConfig`] into its secret-free view, preferring an
    /// optional resolved-runtime-pool override for the two sharded-pool fields.
    ///
    /// When `resolved` is `Some`, [`sharded_pool_configured`] and
    /// [`sharded_pool_shard_count`] take the resolved runtime pool's values —
    /// covering the runner-provided `HarvestRunnerResources::with_sharded_pool`
    /// override, `WorkerConfig::with_sharded_pool`, and the fallback single pool
    /// alike. When `None`, they fall back to the `WorkerConfig::sharded_pool`
    /// field (keeping the pure no-DB mapping path valid).
    ///
    /// The exhaustive destructure below is the **#695 coverage guard** — do NOT
    /// add `..`. A new [`WorkerConfig`] field must break compilation here until
    /// it is deliberately surfaced (or, for a secret-bearing field, mapped to a
    /// presence boolean/count). `sharded_pool` is still bound (as `_`) so the
    /// guard stays intact even though its reported value prefers the override.
    ///
    /// [`sharded_pool_configured`]: Self::sharded_pool_configured
    /// [`sharded_pool_shard_count`]: Self::sharded_pool_shard_count
    #[must_use]
    // The body is one exhaustive destructure plus one field-for-field mapping,
    // so the line count is the size of `WorkerConfig`, not of any control flow.
    // Splitting it would mean splitting the `..`-free pattern that IS the #695
    // coverage guard, which is the one thing this function must not do.
    #[allow(clippy::too_many_lines)]
    pub fn from_worker_config_with_resolved_sharding(
        worker: &WorkerConfig,
        poll_interval: Duration,
        resolved: Option<ShardedInfo>,
    ) -> Self {
        // Bind the sharded-pool presence/count before the (feature-gated)
        // destructure so both `db` and non-`db` builds produce the same view.
        // A resolved-runtime override, when supplied, wins over the raw
        // `WorkerConfig::sharded_pool` knob (issue #695 review).
        #[cfg(feature = "db")]
        let wc_sharding = worker
            .sharded_pool
            .as_ref()
            .map_or((false, 0), |p| (true, p.iter_shards().count()));
        #[cfg(not(feature = "db"))]
        let wc_sharding = (false, 0usize);
        let pool_shards = resolved_pool_shards(worker, resolved.as_ref());
        let (sharded_pool_configured, sharded_pool_shard_count) =
            resolved.map_or(wc_sharding, |info| (info.configured, info.shard_count));

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
            default_activity_retry_policy,
            default_activity_start_to_close,
            retry_after_ceiling,
            worker_heartbeat_interval,
            build_id,
            deployment_name,
            query_timeout,
            priority_aging_secs,
            dr_fencing,
            replication_sample_interval,
            replication_watermark_retain,
            replication_slot_prefix,
            max_workflow_start_delay,
            unknown_target_grace_window,
            poison_pill_threshold,
            capability_miss_max_redeliveries,
            workflow_task_timeout,
            max_workflow_pause_duration,
            labels,
            max_workflow_history_events,
            default_debounce_max_wait,
            mutex_lease_ttl,
            slot_tuner,
            // REDACTED — presence/count only, never the live handle. Bound above.
            #[cfg(feature = "db")]
                sharded_pool: _,
            max_concurrent_sessions,
            workflow_panic_max_attempts,
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
            // The **effective** assignments, not the raw knob: an empty
            // `shard_assignments` means "auto: cover every pool shard"
            // (issue #961). `GET /admin/config` must report what the worker
            // actually polls, because the add-a-shard runbook's coverage
            // verification reads exactly this field (AC8).
            shard_assignments: crate::builder::resolve_shard_assignments(
                shard_assignments.clone(),
                &pool_shards,
            )
            .into_iter()
            .map(crate::types::ShardId::as_i32)
            .collect(),
            max_local_activity_start_to_close_ms: dur_ms(*max_local_activity_start_to_close),
            default_activity_retry_max_attempts: default_activity_retry_policy
                .as_ref()
                .map(|p| p.max_attempts),
            default_activity_start_to_close_ms: default_activity_start_to_close.map(dur_ms),
            retry_after_ceiling_ms: dur_ms(*retry_after_ceiling),
            worker_heartbeat_interval_ms: dur_ms(*worker_heartbeat_interval),
            build_id: build_id.clone(),
            deployment_name: deployment_name.clone(),
            query_timeout_ms: dur_ms(*query_timeout),
            priority_aging_secs: *priority_aging_secs,
            dr_fencing: *dr_fencing,
            replication_sample_interval_ms: dur_ms(*replication_sample_interval),
            replication_watermark_retain_ms: dur_ms(*replication_watermark_retain),
            replication_slot_prefix: replication_slot_prefix.clone(),
            max_workflow_start_delay_ms: dur_ms(*max_workflow_start_delay),
            unknown_target_grace_window_ms: dur_ms(*unknown_target_grace_window),
            poison_pill_threshold: *poison_pill_threshold,
            poison_pill_quarantine_enabled: *poison_pill_threshold > 0,
            capability_miss_max_redeliveries: *capability_miss_max_redeliveries,
            workflow_task_timeout_ms: dur_ms(*workflow_task_timeout),
            max_workflow_pause_duration_ms: dur_ms(*max_workflow_pause_duration),
            labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            max_workflow_history_events: *max_workflow_history_events,
            default_debounce_max_wait_ms: dur_ms(*default_debounce_max_wait),
            // Mirror the sibling `_ms` fields: surface the resolved TTL straight
            // from the bound `WorkerConfig` field (the authoritative per-config
            // value), rather than reading the process-global — the global is set
            // from this same field at `From<WorkerConfig>` time (issue #691).
            mutex_lease_ttl_ms: dur_ms(*mutex_lease_ttl),
            slot_tuner_enabled: slot_tuner.is_some(),
            max_concurrent_sessions: *max_concurrent_sessions,
            workflow_panic_max_attempts: *workflow_panic_max_attempts,
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
    /// The declared residency key → shard mapping (issue #697).
    ///
    /// Empty unless the deployment declared one. This is **placement-affecting
    /// configuration**, not a secret: the keys are operator-declared
    /// jurisdiction labels (`"eu"`, `"us"`), never caller input or credentials,
    /// and this endpoint is admin-gated.
    ///
    /// Surfacing it is what lets an operator detect the failure mode a
    /// topology-only snapshot hides: two replicas deployed with **different**
    /// key → shard maps report identical `readable`/`writable`/`default` sets
    /// while placing the same `residency_key` in **different jurisdictions**
    /// depending on which replica serves the request. Diff this field across
    /// replicas before accepting pinned starts. `BTreeMap` ordering makes the
    /// projection stable, so a byte comparison of two snapshots is meaningful.
    pub residency_map: BTreeMap<String, i32>,
}

impl ShardTopologyView {
    /// Project a [`ShardRouter`](crate::shard::ShardRouter) into its view.
    #[must_use]
    pub fn from_router(router: &crate::shard::ShardRouter) -> Self {
        // Coverage guard (issue #695 pattern, extended to the router by the
        // issue #697 review): destructure EXHAUSTIVELY so adding a
        // placement-affecting accessor to `ShardRouter` without surfacing it
        // here is a compile error rather than a silent snapshot gap -- which is
        // exactly how `residency_map` slipped through when it was introduced.
        // Do NOT add `..`.
        let crate::shard::ShardRouterParts {
            readable_shards,
            writable_shards,
            default_shard,
            residency_map,
        } = router.parts();
        Self {
            readable_shards: readable_shards.iter().map(|s| s.as_i32()).collect(),
            writable_shards: writable_shards.iter().map(|s| s.as_i32()).collect(),
            default_shard: default_shard.as_i32(),
            residency_map: residency_map
                .iter()
                .map(|(key, shard)| (key.clone(), shard.as_i32()))
                .collect(),
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

/// Resolved sharded-pool presence + shard count read off the runtime's
/// *actual* storage pool (issue #695 review).
///
/// This is the **DB-free override input** to the [`WorkerConfigView`]
/// sharded-pool fields: the capture site resolves which pool the runtime
/// actually uses (runner-provided `HarvestRunnerResources::with_sharded_pool`,
/// then `WorkerConfig::with_sharded_pool`, then the fallback single pool) and
/// hands the result here, so the reported values describe the resolved runtime
/// pool rather than solely the `WorkerConfig` knob — and the pure decision stays
/// testable without ever constructing a real pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardedInfo {
    /// Whether the resolved runtime storage pool is a sharded pool.
    pub configured: bool,
    /// Number of shards in the resolved sharded pool (0 when not sharded).
    pub shard_count: usize,
    /// Shard ids present in the **resolved runtime pool**, ascending.
    ///
    /// Carried so the snapshot can report the worker's *effective* shard
    /// assignments (issue #961, AC1/AC8): an empty
    /// [`WorkerConfig::shard_assignments`] means "auto: cover every pool
    /// shard", and only the resolved runtime pool knows which shards those
    /// are. This is always populated from the resolved pool — including the
    /// single-shard fallback wrapper — so `GET /admin/config` reports the same
    /// list the worker actually polls, which is what the add-a-shard runbook's
    /// coverage check reads.
    pub shard_ids: Vec<i32>,
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
    /// `resolved_sharding` is the resolved-runtime-pool override for the two
    /// [`WorkerConfigView`] sharded-pool fields (`None` = fall back to the
    /// `WorkerConfig::sharded_pool` knob; the pure no-DB path).
    #[must_use]
    pub fn capture(
        worker: &WorkerConfig,
        caps: PayloadCapsView,
        router: &crate::shard::ShardRouter,
        pool: PoolConfigView,
        poll_interval: Duration,
        resolved_sharding: Option<ShardedInfo>,
    ) -> Self {
        Self {
            worker: WorkerConfigView::from_worker_config_with_resolved_sharding(
                worker,
                poll_interval,
                resolved_sharding,
            ),
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
    fn retry_after_ceiling_ms_surfaces_the_configured_value_issue_744() {
        // The ceiling is not opt-in (always present, unlike the sibling
        // default_activity_* floors) -- confirm the default AND a configured
        // override both surface through the introspection snapshot.
        let default_view = WorkerConfigView::from_worker_config(
            &WorkerConfig::default(),
            Duration::from_millis(500),
        );
        assert_eq!(
            default_view.retry_after_ceiling_ms,
            u64::try_from(crate::builder::DEFAULT_RETRY_AFTER_CEILING.as_millis()).unwrap(),
        );

        let configured = WorkerConfig::default().with_retry_after_ceiling(Duration::from_secs(90));
        let view = WorkerConfigView::from_worker_config(&configured, Duration::from_millis(500));
        assert_eq!(view.retry_after_ceiling_ms, 90_000);
    }

    #[cfg(feature = "db")]
    #[test]
    fn poll_interval_ms_matches_the_side_effect_free_constant() {
        // The effective-config snapshot sources `poll_interval` from
        // `worker::DEFAULT_WORKER_POLL_INTERVAL` (a side-effect-free constant)
        // rather than constructing a `WorkerRuntimeConfig` (whose conversion
        // locks the write-once `GLOBAL_DEFAULT_WORKFLOW_QUEUE`, issue #695).
        // This test pins that the surfaced value equals that single source of
        // truth, so the value cannot drift from the runtime's own default.
        let expected_ms =
            u64::try_from(crate::worker::DEFAULT_WORKER_POLL_INTERVAL.as_millis()).unwrap();
        let view = WorkerConfigView::from_worker_config(
            &WorkerConfig::default(),
            crate::worker::DEFAULT_WORKER_POLL_INTERVAL,
        );
        assert_eq!(view.poll_interval_ms, expected_ms);
        assert_eq!(expected_ms, 500);
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
        assert!(
            view.residency_map.is_empty(),
            "a router with no declared map must report an empty projection"
        );
    }

    /// Issue #697 review (Codex P2): two replicas deployed with DIFFERENT
    /// residency maps place the same key in different jurisdictions while
    /// reporting identical readable/writable/default sets. The snapshot must
    /// surface the map so an operator can diff it across the fleet.
    #[test]
    fn shard_topology_surfaces_the_residency_map_so_replica_drift_is_visible() {
        let shards = vec![crate::types::ShardId::new(0), crate::types::ShardId::new(1)];
        let replica_a = crate::shard::ShardRouter::new(
            shards.clone(),
            shards.clone(),
            crate::types::ShardId::new(0),
        )
        .with_residency_map([
            ("eu".to_string(), crate::types::ShardId::new(0)),
            ("us".to_string(), crate::types::ShardId::new(1)),
        ]);
        // Same topology, MIRRORED map -- the misconfiguration this exists to catch.
        let replica_b =
            crate::shard::ShardRouter::new(shards.clone(), shards, crate::types::ShardId::new(0))
                .with_residency_map([
                    ("eu".to_string(), crate::types::ShardId::new(1)),
                    ("us".to_string(), crate::types::ShardId::new(0)),
                ]);

        let view_a = ShardTopologyView::from_router(&replica_a);
        let view_b = ShardTopologyView::from_router(&replica_b);

        assert_eq!(view_a.residency_map.get("eu"), Some(&0));
        assert_eq!(view_a.residency_map.get("us"), Some(&1));

        // Topology alone is identical -- exactly why the map is load-bearing.
        assert_eq!(view_a.readable_shards, view_b.readable_shards);
        assert_eq!(view_a.writable_shards, view_b.writable_shards);
        assert_eq!(view_a.default_shard, view_b.default_shard);
        assert_ne!(
            view_a.residency_map, view_b.residency_map,
            "a mirrored residency map MUST be visible in the snapshot; without \
             this field two conflicting replicas serialize identically"
        );

        // Stable ordering, so a byte comparison across replicas is meaningful.
        let json_a = serde_json::to_string(&view_a).expect("serialize");
        assert_eq!(
            json_a,
            serde_json::to_string(&ShardTopologyView::from_router(&replica_a)).expect("serialize"),
            "the projection must be byte-stable across calls"
        );
        assert!(json_a.contains("residency_map"), "{json_a}");
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
            None,
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
    fn worker_view_sharded_fields_prefer_resolved_override() {
        // The two sharded-pool fields must reflect the resolved runtime pool
        // (e.g. a runner-provided HarvestRunnerResources::with_sharded_pool,
        // which the WorkerConfig knob cannot see). A default WorkerConfig has no
        // sharded pool, yet a resolved override must still surface configured=true
        // with the resolved shard count. Pure, no live pool needed.
        let worker = WorkerConfig::default();

        let overridden = WorkerConfigView::from_worker_config_with_resolved_sharding(
            &worker,
            Duration::from_millis(500),
            Some(ShardedInfo {
                configured: true,
                shard_count: 3,
                shard_ids: vec![0, 1, 2],
            }),
        );
        assert!(overridden.sharded_pool_configured);
        assert_eq!(overridden.sharded_pool_shard_count, 3);

        // A resolved *single* (non-sharded) fallback pool → not configured, 0.
        let single = WorkerConfigView::from_worker_config_with_resolved_sharding(
            &worker,
            Duration::from_millis(500),
            Some(ShardedInfo {
                configured: false,
                shard_count: 0,
                shard_ids: vec![0],
            }),
        );
        assert!(!single.sharded_pool_configured);
        assert_eq!(single.sharded_pool_shard_count, 0);

        // No override (None) → fall back to the WorkerConfig::sharded_pool knob,
        // which the default config leaves unset.
        let fallback = WorkerConfigView::from_worker_config(&worker, Duration::from_millis(500));
        assert!(!fallback.sharded_pool_configured);
        assert_eq!(fallback.sharded_pool_shard_count, 0);
    }

    #[test]
    fn worker_view_reports_effective_shard_assignments_not_the_auto_sentinel() {
        // AC8: `docs/sharding.md` step 4 tells an operator to verify coverage
        // with `GET /admin/config` -> `worker.shard_assignments`. That check is
        // only meaningful if the view reports the *resolved* list, so a worker
        // left on auto (empty `shard_assignments`) shows the concrete shards it
        // will poll rather than an empty array the operator cannot act on.
        let auto = WorkerConfig::default();
        assert!(
            auto.shard_assignments.is_empty(),
            "the default must be the auto sentinel, else this test is vacuous"
        );

        let view = WorkerConfigView::from_worker_config_with_resolved_sharding(
            &auto,
            Duration::from_millis(500),
            Some(ShardedInfo {
                configured: true,
                shard_count: 3,
                shard_ids: vec![0, 1, 2],
            }),
        );
        assert_eq!(
            view.shard_assignments,
            vec![0, 1, 2],
            "auto must resolve to every shard of the pool the runtime actually uses"
        );
    }

    #[test]
    fn worker_view_never_widens_an_explicit_shard_assignment() {
        // The mirror of the test above: a deliberately narrowed worker (the
        // one-worker-process-per-shard shape) must be reported as narrowed, so
        // an operator flipping a shard writable can *see* the missing id.
        let explicit =
            WorkerConfig::default().with_shard_assignments([crate::types::ShardId::new(1)]);

        let view = WorkerConfigView::from_worker_config_with_resolved_sharding(
            &explicit,
            Duration::from_millis(500),
            Some(ShardedInfo {
                configured: true,
                shard_count: 3,
                shard_ids: vec![0, 1, 2],
            }),
        );
        assert_eq!(
            view.shard_assignments,
            vec![1],
            "an explicit assignment must never be widened to the pool"
        );
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
