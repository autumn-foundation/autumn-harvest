//! Tests for sticky cross-worker routing (issue #235 / spec #35).
//!
//! ## Structure
//!
//! **Pure-logic tests (no DB required):** verify the public API surface —
//! `StickyRoutingConfig`, `WorkerConfig::with_sticky_routing`, metric constants,
//! and the `MetricsRecorder` trait methods.  The integration section at the
//! bottom uses a `CountingMetrics` recorder and `WorkflowCache` to simulate the
//! cache hit/miss lifecycle without a database.
//!
//! **DB integration tests (`db` feature, bottom of file):** validate the
//! acceptance criteria from the Vantage spec at the Postgres level —
//! the pin mechanism, exclusive claim within TTL, graceful fallback after
//! expiry, and `park`/`wake` sticky lifecycle.  Written RED-first; they
//! pass GREEN against the implementation in `queue.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autumn_harvest::builder::{StickyRoutingConfig, WorkerConfig};
use autumn_harvest::cache::{CachedWorkflowState, WorkflowCache};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::telemetry::{
    METRIC_WORKFLOW_CACHE_HIT, METRIC_WORKFLOW_CACHE_MISS, MetricsRecorder, NoOpMetrics,
};

// ---------------------------------------------------------------------------
// CountingMetrics — a MetricsRecorder that accumulates hit/miss counts.
// Used by the integration tests below to assert that the correct metric is
// emitted on each task cycle.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct CountingMetrics {
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl CountingMetrics {
    fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::SeqCst)
    }
    fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::SeqCst)
    }
}

impl MetricsRecorder for CountingMetrics {
    fn record_workflow_cache_hit(&self, _workflow_name: &str, _queue: &str) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }
    fn record_workflow_cache_miss(&self, _workflow_name: &str, _queue: &str) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// StickyRoutingConfig
// ---------------------------------------------------------------------------

#[test]
fn sticky_routing_config_can_be_constructed() {
    let config = StickyRoutingConfig {
        lease_ttl: Duration::from_secs(10),
    };
    assert_eq!(config.lease_ttl, Duration::from_secs(10));
}

#[test]
fn sticky_routing_config_zero_lease_ttl_is_valid() {
    let config = StickyRoutingConfig {
        lease_ttl: Duration::ZERO,
    };
    assert!(config.lease_ttl.is_zero());
}

// ---------------------------------------------------------------------------
// WorkerConfig default + with_sticky_routing
// ---------------------------------------------------------------------------

#[test]
fn worker_config_default_has_sticky_routing_disabled() {
    let config = WorkerConfig::default();
    assert!(
        config.sticky_timeout.is_zero(),
        "sticky routing must be off by default; got sticky_timeout={:?}",
        config.sticky_timeout
    );
}

#[test]
fn with_sticky_routing_enables_the_lease_ttl() {
    let config = WorkerConfig::default().with_sticky_routing(StickyRoutingConfig {
        lease_ttl: Duration::from_secs(10),
    });
    assert_eq!(
        config.sticky_timeout,
        Duration::from_secs(10),
        "with_sticky_routing should set sticky_timeout to lease_ttl"
    );
}

#[test]
fn with_sticky_routing_can_be_called_multiple_times_and_last_wins() {
    let config = WorkerConfig::default()
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(5),
        })
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(15),
        });
    assert_eq!(config.sticky_timeout, Duration::from_secs(15));
}

#[test]
fn with_sticky_routing_zero_lease_ttl_disables_sticky() {
    let config = WorkerConfig::default()
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::from_secs(10),
        })
        .with_sticky_routing(StickyRoutingConfig {
            lease_ttl: Duration::ZERO,
        });
    assert!(
        config.sticky_timeout.is_zero(),
        "zero lease_ttl should disable sticky routing"
    );
}

// ---------------------------------------------------------------------------
// Metric constants
// ---------------------------------------------------------------------------

#[test]
fn cache_hit_metric_constant_has_correct_name() {
    assert_eq!(
        METRIC_WORKFLOW_CACHE_HIT, "harvest.workflow.cache_hit",
        "cache hit metric must follow harvest.<noun>.<instrument> naming"
    );
}

#[test]
fn cache_miss_metric_constant_has_correct_name() {
    assert_eq!(
        METRIC_WORKFLOW_CACHE_MISS, "harvest.workflow.cache_miss",
        "cache miss metric must follow harvest.<noun>.<instrument> naming"
    );
}

// ---------------------------------------------------------------------------
// MetricsRecorder trait — default no-op implementations
// ---------------------------------------------------------------------------

#[test]
fn no_op_metrics_implements_cache_hit_without_panic() {
    NoOpMetrics.record_workflow_cache_hit("my_workflow", "default");
}

#[test]
fn no_op_metrics_implements_cache_miss_without_panic() {
    NoOpMetrics.record_workflow_cache_miss("my_workflow", "default");
}

// ---------------------------------------------------------------------------
// Cache lifecycle integration tests (no DB required).
//
// These tests simulate the cache hit/miss lifecycle that the worker hot path
// executes on every task cycle:
//
//   Task 1 (fresh execution):
//     cache.get() → None → cold full-history load → record_miss
//     workflow suspends → cache.insert(exec_id, snapshot)
//
//   Task 2 (same execution on same worker):
//     cache.get() → Some → delta load → record_hit
//     workflow suspends again → cache.insert(exec_id, updated_snapshot)
//
//   Task 3 (terminal outcome):
//     cache.get() → Some → delta load → record_hit
//     workflow completes → cache.remove(exec_id)
//
// The "3-worker scenario" from the issue acceptance criteria is modelled as
// three independent caches: only the cache that saw Task 1 produces a hit
// on Task 2.  The other two caches each produce a miss, matching the
// expected distribution (miss_count ≤ 1 on the owning worker; miss_count = 1
// on all others).
// ---------------------------------------------------------------------------

/// Helper that models a single task cycle on one worker's local cache:
///   1. Check cache (hit/miss).
///   2. Emit the appropriate metric.
///   3. Update the cache based on the simulated outcome.
fn simulate_task_cycle(
    cache: &mut WorkflowCache,
    metrics: &CountingMetrics,
    exec_id: uuid::Uuid,
    suspends: bool, // true → Suspended (cache insert), false → Completed (cache evict)
    events_after: Vec<WorkflowEvent>,
    next_event_id: i32,
) -> bool {
    let is_hit = cache.get(&exec_id).is_some();
    if is_hit {
        metrics.record_workflow_cache_hit("test_workflow", "default");
    } else {
        metrics.record_workflow_cache_miss("test_workflow", "default");
    }
    if suspends {
        cache.insert(
            exec_id,
            CachedWorkflowState {
                events: events_after,
                next_event_id,
            },
        );
    } else {
        cache.remove(&exec_id);
    }
    is_hit
}

#[test]
fn first_task_is_cache_miss_subsequent_tasks_on_same_worker_are_hits() {
    let metrics = CountingMetrics::default();
    let mut cache = WorkflowCache::new(64);
    let exec_id = uuid::Uuid::new_v4();

    // Task 1: cold path — execution not in cache yet.
    let hit = simulate_task_cycle(
        &mut cache,
        &metrics,
        exec_id,
        true,
        vec![WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: chrono::Utc::now(),
        }],
        2,
    );
    assert!(!hit, "first task must be a cache miss");
    assert_eq!(metrics.miss_count(), 1);
    assert_eq!(metrics.hit_count(), 0);

    // Task 2: warm path — cache was populated by Task 1's suspension.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_id, true, vec![], 4);
    assert!(hit, "second task on same worker must be a cache hit");
    assert_eq!(metrics.miss_count(), 1);
    assert_eq!(metrics.hit_count(), 1);

    // Task 3: warm path again before terminal completion.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_id, false, vec![], 6);
    assert!(hit, "third task on same worker must be a cache hit");
    assert_eq!(metrics.miss_count(), 1, "miss count must stay at 1");
    assert_eq!(metrics.hit_count(), 2);

    // After completion the entry must be evicted.
    assert!(
        cache.get(&exec_id).is_none(),
        "completed execution must be evicted"
    );
}

#[test]
fn three_worker_scenario_only_owning_worker_gets_cache_hits() {
    // Simulate a 3-worker fleet.  Each worker has its own independent cache.
    // Execution is "sticky" to worker-0: it is the only one that retains the
    // warm cache entry, so it is the only one that records hits on follow-up tasks.
    let metrics_w0 = CountingMetrics::default();
    let metrics_w1 = CountingMetrics::default();
    let metrics_w2 = CountingMetrics::default();

    let mut cache_w0 = WorkflowCache::new(64);
    let mut cache_w1 = WorkflowCache::new(64);
    let mut cache_w2 = WorkflowCache::new(64);

    let exec_id = uuid::Uuid::new_v4();

    // --- Round 1: task lands on worker-0 (cold path) ---
    simulate_task_cycle(&mut cache_w0, &metrics_w0, exec_id, true, vec![], 2);
    assert_eq!(metrics_w0.miss_count(), 1);
    assert_eq!(metrics_w0.hit_count(), 0);

    // --- Round 2: follow-up task lands on worker-0 (warm path) ---
    simulate_task_cycle(&mut cache_w0, &metrics_w0, exec_id, true, vec![], 4);
    assert_eq!(metrics_w0.miss_count(), 1, "owner: still only 1 miss");
    assert_eq!(metrics_w0.hit_count(), 1, "owner: first hit");

    // --- Hypothetical: same task lands on worker-1 instead (miss, no cache) ---
    simulate_task_cycle(&mut cache_w1, &metrics_w1, exec_id, true, vec![], 4);
    assert_eq!(metrics_w1.miss_count(), 1, "w1 has no cache entry: miss");
    assert_eq!(metrics_w1.hit_count(), 0);

    // --- Hypothetical: same task lands on worker-2 instead (miss, no cache) ---
    simulate_task_cycle(&mut cache_w2, &metrics_w2, exec_id, true, vec![], 4);
    assert_eq!(metrics_w2.miss_count(), 1, "w2 has no cache entry: miss");
    assert_eq!(metrics_w2.hit_count(), 0);

    // Workers w1 and w2 each got exactly 1 miss (the full reload).
    // Worker w0 (the sticky owner) accumulated 1 miss + 1 hit.
    // Total hit rate on w0 after two tasks = 50%; with N follow-up tasks → approaches 100%.
    assert_eq!(metrics_w0.miss_count() + metrics_w0.hit_count(), 2);
    assert_eq!(metrics_w1.miss_count() + metrics_w1.hit_count(), 1);
    assert_eq!(metrics_w2.miss_count() + metrics_w2.hit_count(), 1);
}

#[test]
fn cache_eviction_by_lru_pressure_causes_miss_on_next_task() {
    let metrics = CountingMetrics::default();
    let mut cache = WorkflowCache::new(2); // tiny cache to force eviction
    let exec_a = uuid::Uuid::new_v4();
    let exec_b = uuid::Uuid::new_v4();
    let exec_c = uuid::Uuid::new_v4();

    // Populate cache: A first, then B.  A is now LRU, B is MRU.
    simulate_task_cycle(&mut cache, &metrics, exec_a, true, vec![], 2);
    simulate_task_cycle(&mut cache, &metrics, exec_b, true, vec![], 2);
    assert_eq!(cache.len(), 2);

    // Touch A to make it MRU.  B is now LRU.
    assert!(cache.get(&exec_a).is_some());

    // Insert C — cache is full, so B (the LRU) is evicted.
    simulate_task_cycle(&mut cache, &metrics, exec_c, true, vec![], 2);
    assert_eq!(cache.len(), 2);

    // A is still present (was MRU before the eviction).
    assert!(cache.get(&exec_a).is_some(), "exec_a should be in cache");
    // C was just inserted.
    assert!(cache.get(&exec_c).is_some(), "exec_c should be in cache");

    // B follow-up: evicted → miss even though same worker.
    let hit = simulate_task_cycle(&mut cache, &metrics, exec_b, true, vec![], 4);
    assert!(!hit, "evicted entry must cause a cache miss");
}

#[test]
fn counting_metrics_tracks_hit_miss_independently() {
    let m = CountingMetrics::default();
    assert_eq!(m.hit_count(), 0);
    assert_eq!(m.miss_count(), 0);

    m.record_workflow_cache_hit("wf", "q");
    m.record_workflow_cache_hit("wf", "q");
    m.record_workflow_cache_miss("wf", "q");

    assert_eq!(m.hit_count(), 2);
    assert_eq!(m.miss_count(), 1);
}

// ---------------------------------------------------------------------------
// DB integration tests — validate acceptance criteria at the Postgres level.
//
// RED phase: these tests were written against the spec acceptance criteria
//   before being run, defining the expected DB-level behaviour.
// GREEN phase: they pass against the implementation in `queue.rs` and the
//   sticky routing columns in the initial migration.
//
// Acceptance criteria (from docs/plans/vantage-spec-sticky-routing.md):
//   AC#1 – Pin mechanism: sticky_worker_id + sticky_until set in DB for TTL.
//   AC#2 – Graceful fallback: after sticky_until elapses any worker can claim.
//   AC#3 – No external cache required (satisfied architecturally).
//   AC#4 – SKIP LOCKED integration: sticky worker gets priority; non-sticky
//            workers cannot claim within TTL; park/wake maintain the pin.
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
mod db_tests {
    use autumn_harvest::models::NewWorkflowExecution;
    use autumn_harvest::queue::{self, EnqueueParams, StickyHint, TaskType};
    use autumn_harvest::schema::harvest_workflow_executions;
    use autumn_harvest::types::ExecutionId;
    use diesel::sql_types::{Nullable, Text, Timestamptz};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use std::time::Duration;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

    // All migrations needed for harvest_task_queue + harvest_workflow_executions
    // + harvest_build_compat (required by claim_task's build-routing filter).
    const INIT_SQL: &str = concat!(
        include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
        "\n",
        include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
        "\n",
        include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
        "\n",
        include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
        "\n",
        include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
        "\n",
        include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
        "\n",
        include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
        "\n",
        include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
        "\n",
        include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
        "\n",
        include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
        "\n",
        include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
        "\n",
        include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
        "\n",
        include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
        "\n",
        include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
        "\n",
        include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
        "\n",
        include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
        "\n",
        include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
        "\n",
        include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
        "\n",
        include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
        include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
        include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
        include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
        include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
        "\n",
        include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
        "\n",
        include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
        "\n",
        include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql")
    );

    async fn setup() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_init_sql(INIT_SQL.to_string().into_bytes())
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (conn, container)
    }

    async fn insert_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
        let exec_id = ExecutionId::new();
        let row = NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "sticky_test_wf",
            workflow_id: &Uuid::new_v4().to_string(),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            assigned_build_id: None,
            parent_close_policy: None,

            owner: None,
            runbook_url: None,
            severity: None,
        };
        diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .execute(conn)
            .await
            .expect("insert execution");
        exec_id
    }

    /// Read back the sticky columns for a task row.
    #[derive(diesel::QueryableByName, Debug)]
    struct StickyColumns {
        #[diesel(sql_type = Nullable<Text>)]
        sticky_worker_id: Option<String>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        sticky_until: Option<chrono::DateTime<chrono::Utc>>,
    }

    async fn read_sticky(conn: &mut AsyncPgConnection, task_id: Uuid) -> StickyColumns {
        diesel::sql_query(
            "SELECT sticky_worker_id, sticky_until \
             FROM harvest_task_queue WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(conn)
        .await
        .expect("read sticky columns")
    }

    // ── AC#1 — Pin mechanism ─────────────────────────────────────────────────

    /// Enqueueing with a sticky hint writes `sticky_worker_id` and `sticky_until`
    /// to the DB row (not just in memory).  This is the foundation of AC#1: the
    /// pin must survive round-trips through Postgres so any future claim query
    /// sees it.
    #[tokio::test]
    async fn enqueue_with_sticky_pin_writes_worker_id_and_ttl_to_db() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params.with_sticky("worker-alpha", Duration::from_secs(30));

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        let cols = read_sticky(&mut conn, task_id).await;

        assert_eq!(
            cols.sticky_worker_id.as_deref(),
            Some("worker-alpha"),
            "AC#1: sticky_worker_id must be persisted in DB"
        );
        assert!(
            cols.sticky_until.is_some(),
            "AC#1: sticky_until must be set to a future timestamp"
        );
        // Sanity: sticky_until must be in the future.
        let until = cols.sticky_until.unwrap();
        assert!(
            until > chrono::Utc::now(),
            "AC#1: sticky_until={until:?} must be in the future"
        );
    }

    // ── AC#1 + AC#4 — Sticky worker claims pinned task ───────────────────────

    /// The worker named in `sticky_worker_id` must be able to claim its pinned
    /// task within the TTL window.  Without this guarantee, sticky routing would
    /// break the happy path entirely.
    #[tokio::test]
    async fn sticky_worker_can_claim_its_pinned_task_within_ttl() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params.with_sticky("worker-beta", Duration::from_secs(60));

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // The owning worker claims the task.
        let claimed = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-beta",
            "", // legacy build id — claims anything
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");

        assert!(
            claimed.is_some(),
            "AC#1+AC#4: sticky worker must be able to claim its pinned task"
        );
        assert_eq!(
            claimed.unwrap().id,
            task_id,
            "AC#4: claimed task must be the one that was enqueued"
        );
    }

    // ── AC#2 prerequisite — Non-sticky worker cannot claim during TTL ─────────

    /// Within the TTL window, a worker that is NOT the sticky owner must not be
    /// able to claim the pinned task.  This exclusivity is what makes sticky
    /// routing useful: the owning worker (which holds the LRU cache) gets first
    /// pick before any other worker can steal the work.
    #[tokio::test]
    async fn non_sticky_worker_cannot_claim_task_within_ttl() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params.with_sticky("worker-gamma", Duration::from_secs(60));

        queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // A different worker tries to claim — must get nothing.
        let claimed = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-delta", // not the sticky worker
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");

        assert!(
            claimed.is_none(),
            "AC#2 prerequisite: non-sticky worker must not claim a task within its TTL"
        );
    }

    // ── AC#2 — Graceful fallback after sticky TTL expires ────────────────────

    /// Once `sticky_until` elapses, ANY worker must be eligible to claim the
    /// task.  This prevents a single point of failure: if the sticky worker
    /// crashes or is slow, the task is not lost — another worker picks it up
    /// after the grace period, at the cost of a full history reload.
    #[tokio::test]
    async fn any_worker_can_claim_after_sticky_expires() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params.with_sticky("worker-epsilon", Duration::from_secs(60));

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // Fast-forward the sticky_until to the past so the TTL has "expired".
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET sticky_until = NOW() - INTERVAL '1 second' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("expire sticky_until");

        // A completely different worker can now claim it (fallback).
        let claimed = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-zeta", // not the original sticky worker
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");

        assert!(
            claimed.is_some(),
            "AC#2: any worker must be able to claim after sticky_until expires"
        );
        assert_eq!(
            claimed.unwrap().id,
            task_id,
            "AC#2: the claimed task must be the one whose TTL expired"
        );
    }

    // ── AC#4 — park_workflow_task writes sticky affinity ─────────────────────

    /// After a workflow suspends (parks), the task row must have the worker's
    /// sticky affinity written to `sticky_worker_id` so that the next resume
    /// cycle preferentially routes back to this worker (the one holding the
    /// in-process LRU cache snapshot).
    #[tokio::test]
    async fn park_workflow_task_writes_sticky_affinity_to_db() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        // Enqueue with no initial pin — simulates a fresh start on any worker.
        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // Worker claims the task.
        queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-eta",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task")
        .expect("must claim");

        // Worker parks the task, pinning itself as the sticky worker.
        let hint = StickyHint::new("worker-eta", Duration::from_secs(20));
        queue::park_workflow_task(&mut conn, task_id, Some(hint))
            .await
            .expect("park_workflow_task");

        let cols = read_sticky(&mut conn, task_id).await;

        assert_eq!(
            cols.sticky_worker_id.as_deref(),
            Some("worker-eta"),
            "AC#4: park must write sticky_worker_id = claiming worker"
        );
        assert!(
            cols.sticky_until.is_some(),
            "AC#4: park must set sticky_until to a future timestamp"
        );
        let until = cols.sticky_until.unwrap();
        assert!(
            until > chrono::Utc::now(),
            "AC#4: sticky_until={until:?} must be in the future after park"
        );
    }

    // ── AC#1 + AC#4 — wake_workflow_task refreshes the sticky window ─────────

    /// When the runtime wakes a parked task (e.g., an activity completes or a
    /// timer fires), `wake_workflow_task` must refresh `sticky_until` to
    /// `NOW() + sticky_timeout`.  Without the refresh, the pin would expire
    /// mid-execution and the next task might be routed to a cold worker.
    #[tokio::test]
    async fn wake_workflow_task_refreshes_sticky_until() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // Claim → park with a sticky pin.
        queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-theta",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task")
        .expect("must claim");

        let hint = StickyHint::new("worker-theta", Duration::from_secs(30));
        queue::park_workflow_task(&mut conn, task_id, Some(hint))
            .await
            .expect("park_workflow_task");

        // Deliberately expire the sticky window so a no-op wake cannot pass
        // the assertion below by simply preserving the parked value.
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET sticky_until = NOW() - INTERVAL '5 seconds' \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("expire sticky_until before wake");

        // Wake the task (simulates an activity completion or timer fire).
        queue::wake_workflow_task(&mut conn, exec_id)
            .await
            .expect("wake_workflow_task");

        // After wake, the task is PENDING again. Re-read the sticky columns.
        let after = read_sticky(&mut conn, task_id).await;

        // sticky_worker_id must still be set — wake does not clear the pin.
        assert_eq!(
            after.sticky_worker_id.as_deref(),
            Some("worker-theta"),
            "AC#1+AC#4: wake must preserve sticky_worker_id"
        );
        // sticky_until must be strictly in the future — a no-op (or preserve)
        // would leave it 5 s in the past and fail this assertion.
        let until_after_wake = after.sticky_until.expect("sticky_until must survive wake");
        assert!(
            until_after_wake > chrono::Utc::now(),
            "AC#1: wake must refresh sticky_until to the future; got {until_after_wake:?}"
        );
    }

    // ── AC#1 — Enqueue without sticky leaves columns NULL ────────────────────

    /// A task enqueued without `.with_sticky()` must have `sticky_worker_id = NULL`
    /// and `sticky_until = NULL`, so it is claimable by any worker.  This is the
    /// baseline (sticky routing disabled) that must never be broken.
    #[tokio::test]
    async fn enqueue_without_sticky_leaves_pin_columns_null() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        // Note: no .with_sticky() call.

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        let cols = read_sticky(&mut conn, task_id).await;

        assert!(
            cols.sticky_worker_id.is_none(),
            "no sticky: sticky_worker_id must be NULL, got {:?}",
            cols.sticky_worker_id
        );
        assert!(
            cols.sticky_until.is_none(),
            "no sticky: sticky_until must be NULL"
        );
    }

    // ── AC#4 — Sticky task preferred over unpinned task for the owning worker ─

    /// When a worker has both a sticky task (pinned to itself) and an unpinned
    /// task available in the queue, it must claim the sticky task first.  This
    /// is the AC#4 priority-ordering guarantee: SKIP LOCKED elects candidates,
    /// then the `ORDER BY sticky_worker_id = $1 DESC` clause ensures the
    /// owning worker always maximises cache locality when it has a choice.
    ///
    /// Without this ordering, sticky routing would only improve locality when
    /// no other work is pending — useless under any real load.
    #[tokio::test]
    async fn sticky_task_preferred_over_unpinned_task_for_owning_worker() {
        let (mut conn, _c) = setup().await;

        // Two independent executions so FK constraints are satisfied.
        let exec_a = insert_execution(&mut conn).await;
        let exec_b = insert_execution(&mut conn).await;

        // Task 1: unpinned, enqueued first (earlier scheduled_at under default ordering).
        let mut p1 = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        p1.workflow_exec_id = Some(exec_a.as_uuid());
        let task_unpinned = queue::enqueue(&mut conn, &p1)
            .await
            .expect("enqueue unpinned");

        // Task 2: pinned to "worker-iota" — enqueued second (later scheduled_at).
        let mut p2 = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        p2.workflow_exec_id = Some(exec_b.as_uuid());
        let p2 = p2.with_sticky("worker-iota", Duration::from_secs(60));
        let task_sticky = queue::enqueue(&mut conn, &p2)
            .await
            .expect("enqueue sticky");

        // Worker-iota claims: must get the STICKY task (higher priority in ORDER BY)
        // even though the unpinned task was enqueued earlier.
        let claimed = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-iota",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task")
        .expect("must claim something");

        assert_eq!(
            claimed.id, task_sticky,
            "AC#4: sticky task must be preferred over unpinned task for the owning worker \
             (claimed {:?}, expected sticky {:?}, unpinned {:?})",
            claimed.id, task_sticky, task_unpinned
        );
    }

    // ── AC#4 — set_task_sticky_affinity(None) clears an existing pin ─────────

    /// The `set_task_sticky_affinity` function must be able to clear a sticky
    /// pin (pass `None`) so that a task becomes claimable by any worker again.
    /// This is used in the worker's signal and timer re-queue paths when sticky
    /// routing is disabled after a task has already been pinned.
    #[tokio::test]
    async fn set_task_sticky_affinity_none_clears_existing_pin() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        // Enqueue with a sticky pin.
        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params.with_sticky("worker-kappa", Duration::from_secs(60));
        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        // Verify pin is set.
        let before = read_sticky(&mut conn, task_id).await;
        assert!(
            before.sticky_worker_id.is_some(),
            "precondition: sticky_worker_id must be set before clearing"
        );

        // Clear the pin.
        queue::set_task_sticky_affinity(&mut conn, task_id, None)
            .await
            .expect("set_task_sticky_affinity(None)");

        // Verify pin is gone.
        let after = read_sticky(&mut conn, task_id).await;
        assert!(
            after.sticky_worker_id.is_none(),
            "AC#4: set_task_sticky_affinity(None) must clear sticky_worker_id"
        );
        assert!(
            after.sticky_until.is_none(),
            "AC#4: set_task_sticky_affinity(None) must clear sticky_until"
        );

        // The task is now claimable by any worker.
        let claimed = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-lambda",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            claimed.is_some(),
            "AC#4: task with cleared pin must be claimable by any worker"
        );
    }
}
