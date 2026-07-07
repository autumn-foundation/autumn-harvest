//! Tests for worker build-id routing (issue #171).
//!
//! Covers: `BuildId`, `DeploymentName`, `BuildCompatibilitySet` pure logic,
//! and (when `db` feature is enabled) the full DB-backed lifecycle:
//! worker registration with build identity, build policy CRUD, build
//! compatibility declaration, build-aware task claiming, and build
//! reachability reporting.
//!
//! TDD structure:
//!   RED  – these tests drove the initial API design
//!   GREEN – implementations in `build_routing.rs`, `types.rs`, et al.

// ── Pure-logic tests (no DB required) ─────────────────────────────────────

use autumn_harvest::build_routing::BuildCompatibilitySet;
use autumn_harvest::types::{BuildId, DeploymentName};

#[test]
fn build_id_display_and_as_str() {
    let id = BuildId::new("v1.0.0-abc");
    assert_eq!(id.as_str(), "v1.0.0-abc");
    assert_eq!(id.to_string(), "v1.0.0-abc");
}

#[test]
fn build_id_equality_and_clone() {
    let a = BuildId::new("sha-cafebabe");
    let b = a.clone();
    assert_eq!(a, b);
    assert_ne!(a, BuildId::new("sha-deadbeef"));
}

#[test]
fn build_id_serde_roundtrip() {
    let id = BuildId::new("deploy-20260509-001");
    let json = serde_json::to_string(&id).expect("serialize");
    let back: BuildId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(id, back);
}

#[test]
fn build_id_empty_is_valid_legacy_sentinel() {
    let empty = BuildId::legacy();
    assert!(empty.is_legacy());
    assert!(!BuildId::new("v1.0").is_legacy());
}

#[test]
fn deployment_name_display_and_as_str() {
    let name = DeploymentName::new("prod-blue");
    assert_eq!(name.as_str(), "prod-blue");
    assert_eq!(name.to_string(), "prod-blue");
}

#[test]
fn deployment_name_serde_roundtrip() {
    let name = DeploymentName::new("canary");
    let json = serde_json::to_string(&name).expect("serialize");
    let back: DeploymentName = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(name, back);
}

// ── BuildCompatibilitySet ──────────────────────────────────────────────────

#[test]
fn same_build_is_always_eligible() {
    let compat = BuildCompatibilitySet::new();
    // Any worker whose build matches the execution's required build can claim.
    assert!(compat.is_eligible("v1.0", Some("v1.0")));
    assert!(compat.is_eligible("v2.0", Some("v2.0")));
}

#[test]
fn different_build_not_eligible_without_declaration() {
    let compat = BuildCompatibilitySet::new();
    assert!(!compat.is_eligible("v2.0", Some("v1.0")));
}

#[test]
fn declared_compat_makes_new_build_eligible_for_old_tasks() {
    let mut compat = BuildCompatibilitySet::new();
    // v2.0 workers are explicitly declared compatible with v1.0 executions.
    compat.add_declaration("v2.0", "v1.0");
    assert!(compat.is_eligible("v2.0", Some("v1.0")));
    // v1.0 workers are NOT automatically compatible with v2.0 (asymmetric).
    assert!(!compat.is_eligible("v1.0", Some("v2.0")));
}

#[test]
fn compat_set_supports_multiple_declarations() {
    let mut compat = BuildCompatibilitySet::new();
    compat.add_declaration("v3.0", "v1.0");
    compat.add_declaration("v3.0", "v2.0");
    assert!(compat.is_eligible("v3.0", Some("v1.0")));
    assert!(compat.is_eligible("v3.0", Some("v2.0")));
    assert!(compat.is_eligible("v3.0", Some("v3.0")));
    assert!(!compat.is_eligible("v1.0", Some("v3.0")));
}

#[test]
fn task_with_no_required_build_claimed_by_any_worker() {
    let compat = BuildCompatibilitySet::new();
    // `None` means the task has no build requirement (pre-policy or legacy).
    assert!(compat.is_eligible("v2.0", None));
    assert!(compat.is_eligible("", None));
}

#[test]
fn legacy_worker_empty_build_id_can_claim_any_task() {
    let compat = BuildCompatibilitySet::new();
    // Workers with an empty build id are treated as legacy workers that
    // pre-date build routing; they retain the ability to claim any task.
    assert!(compat.is_eligible("", Some("v1.0")));
    assert!(compat.is_eligible("", Some("v2.0")));
}

#[test]
fn compat_set_can_remove_a_declaration() {
    let mut compat = BuildCompatibilitySet::new();
    compat.add_declaration("v2.0", "v1.0");
    assert!(compat.is_eligible("v2.0", Some("v1.0")));
    compat.remove_declaration("v2.0", "v1.0");
    assert!(!compat.is_eligible("v2.0", Some("v1.0")));
}

// ── merge_reachability (cross-shard aggregation, no DB required) ───────────

#[test]
fn merge_reachability_sums_counters_across_shards() {
    use autumn_harvest::build_routing::{BuildReachability, merge_reachability};

    let shard_0 = vec![
        BuildReachability {
            build_id: "v1.0".into(),
            open_executions: 3,
            pending_tasks: 2,
            active_workers: 1,
            stale_workers: 0,
            safe_to_retire: false,
        },
        BuildReachability {
            build_id: "v2.0".into(),
            open_executions: 1,
            pending_tasks: 1,
            active_workers: 1,
            stale_workers: 0,
            safe_to_retire: false,
        },
    ];
    let shard_1 = vec![
        BuildReachability {
            build_id: "v1.0".into(),
            open_executions: 2,
            pending_tasks: 1,
            active_workers: 0,
            stale_workers: 1,
            safe_to_retire: false,
        },
        // v3.0 only on shard 1
        BuildReachability {
            build_id: "v3.0".into(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 2,
            stale_workers: 0,
            safe_to_retire: true,
        },
    ];

    let merged = merge_reachability(vec![shard_0, shard_1]);
    assert_eq!(merged.len(), 3, "v1.0, v2.0, v3.0");

    let v1 = merged.iter().find(|r| r.build_id == "v1.0").unwrap();
    assert_eq!(v1.open_executions, 5);
    assert_eq!(v1.pending_tasks, 3);
    assert_eq!(v1.active_workers, 1);
    assert_eq!(v1.stale_workers, 1);
    assert!(!v1.safe_to_retire, "has open executions");

    let v2 = merged.iter().find(|r| r.build_id == "v2.0").unwrap();
    assert_eq!(v2.open_executions, 1);
    assert!(!v2.safe_to_retire);

    let v3 = merged.iter().find(|r| r.build_id == "v3.0").unwrap();
    assert_eq!(v3.open_executions, 0);
    assert_eq!(v3.pending_tasks, 0);
    assert!(v3.safe_to_retire, "no open work on any shard");
}

#[test]
fn merge_reachability_recomputes_safe_to_retire_from_totals() {
    use autumn_harvest::build_routing::{BuildReachability, merge_reachability};

    // Shard 0 says safe (0/0), shard 1 has pending tasks — merged must be false.
    let shard_0 = vec![BuildReachability {
        build_id: "v1.0".into(),
        open_executions: 0,
        pending_tasks: 0,
        active_workers: 0,
        stale_workers: 0,
        safe_to_retire: true,
    }];
    let shard_1 = vec![BuildReachability {
        build_id: "v1.0".into(),
        open_executions: 0,
        pending_tasks: 5,
        active_workers: 1,
        stale_workers: 0,
        safe_to_retire: false,
    }];

    let merged = merge_reachability(vec![shard_0, shard_1]);
    let v1 = merged.iter().find(|r| r.build_id == "v1.0").unwrap();
    assert_eq!(v1.pending_tasks, 5);
    assert!(
        !v1.safe_to_retire,
        "pending tasks on shard 1 must flip safe_to_retire to false"
    );
}

#[test]
fn merge_reachability_empty_input_returns_empty() {
    use autumn_harvest::build_routing::merge_reachability;
    assert!(merge_reachability(vec![]).is_empty());
    assert!(merge_reachability(vec![vec![]]).is_empty());
}

// ── Percent-based build ramp (issue #604) ──────────────────────────────────
//
// TDD structure:
//   RED  – these tests drove the ramp decision API design
//   GREEN – `BuildPolicy::resolve_assigned_build`, `ramp_bucket`,
//           `validate_ramp_percent` in `build_routing.rs`

#[test]
fn ramp_percent_zero_always_resolves_to_base_build() {
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: Some("canary-v2".to_string()),
        ramp_percent: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    for _ in 0..500 {
        let exec_id = ExecutionId::new();
        assert_eq!(policy.resolve_assigned_build(exec_id), "base-v1");
    }
}

#[test]
fn ramp_percent_hundred_always_resolves_to_target_build() {
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: Some("canary-v2".to_string()),
        ramp_percent: Some(100),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    for _ in 0..500 {
        let exec_id = ExecutionId::new();
        assert_eq!(policy.resolve_assigned_build(exec_id), "canary-v2");
    }
}

#[test]
fn no_ramp_configured_resolves_to_base_build() {
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: None,
        ramp_percent: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    for _ in 0..50 {
        let exec_id = ExecutionId::new();
        assert_eq!(policy.resolve_assigned_build(exec_id), "base-v1");
    }
}

#[test]
fn cleared_ramp_percent_with_stale_target_resolves_to_base() {
    // `ramp_percent = None` (cleared) must win even if a stale `target_build_id`
    // value is still present on the row — clearing the percent alone must be
    // enough to immediately stop new starts going to the target (AC6).
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: Some("canary-v2".to_string()),
        ramp_percent: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    for _ in 0..50 {
        let exec_id = ExecutionId::new();
        assert_eq!(policy.resolve_assigned_build(exec_id), "base-v1");
    }
}

#[test]
fn ramp_decision_is_deterministic_for_a_given_execution_id() {
    // An outbox/start retry for the same execution must never flip its build
    // (AC3). The decision must be a pure function of exec_id.
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: Some("canary-v2".to_string()),
        ramp_percent: Some(37),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    for _ in 0..200 {
        let exec_id = ExecutionId::new();
        let first = policy.resolve_assigned_build(exec_id);
        for _ in 0..25 {
            assert_eq!(
                policy.resolve_assigned_build(exec_id),
                first,
                "repeated resolution for the same exec_id must be stable"
            );
        }
    }
}

#[test]
fn ramp_decision_differs_across_two_independent_policies_with_disjoint_targets() {
    // Sanity check that the ramp decision is actually keyed off the exec_id
    // (not e.g. always true/false regardless of input) by asserting that at
    // 50% ramp, both outcomes are actually observed across many exec_ids.
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    let policy = BuildPolicy {
        id: Uuid::new_v4(),
        queue_name: "default".to_string(),
        build_id: "base-v1".to_string(),
        deployment_name: None,
        target_build_id: Some("canary-v2".to_string()),
        ramp_percent: Some(50),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let mut saw_base = false;
    let mut saw_target = false;
    for _ in 0..200 {
        let exec_id = ExecutionId::new();
        match policy.resolve_assigned_build(exec_id).as_str() {
            "base-v1" => saw_base = true,
            "canary-v2" => saw_target = true,
            other => panic!("unexpected build id: {other}"),
        }
    }
    assert!(
        saw_base,
        "expected some exec_ids to resolve to the base build"
    );
    assert!(
        saw_target,
        "expected some exec_ids to resolve to the target build"
    );
}

#[test]
fn ramp_distribution_is_within_two_percentage_points_at_various_percents() {
    // Success metric: observed target-build share within +/-2pp of the
    // configured percentage across a large sample of distinct new starts.
    use autumn_harvest::build_routing::BuildPolicy;
    use autumn_harvest::types::ExecutionId;
    use chrono::Utc;
    use uuid::Uuid;

    for percent in [5_i32, 25, 50, 75, 95] {
        let policy = BuildPolicy {
            id: Uuid::new_v4(),
            queue_name: "default".to_string(),
            build_id: "base-v1".to_string(),
            deployment_name: None,
            target_build_id: Some("canary-v2".to_string()),
            ramp_percent: Some(percent),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let total = 10_000;
        let mut target_count = 0;
        for _ in 0..total {
            let exec_id = ExecutionId::new();
            if policy.resolve_assigned_build(exec_id) == "canary-v2" {
                target_count += 1;
            }
        }
        let observed_pct = f64::from(target_count) * 100.0 / f64::from(total);
        let diff = (observed_pct - f64::from(percent)).abs();
        assert!(
            diff <= 2.0,
            "percent={percent}: observed {observed_pct:.2}% deviates by {diff:.2}pp (> 2pp)"
        );
    }
}

#[test]
fn validate_ramp_percent_accepts_full_range() {
    use autumn_harvest::build_routing::validate_ramp_percent;
    assert!(validate_ramp_percent(0).is_ok());
    assert!(validate_ramp_percent(50).is_ok());
    assert!(validate_ramp_percent(100).is_ok());
}

#[test]
fn validate_ramp_percent_rejects_out_of_range() {
    use autumn_harvest::build_routing::validate_ramp_percent;
    assert!(validate_ramp_percent(-1).is_err());
    assert!(validate_ramp_percent(101).is_err());
    assert!(validate_ramp_percent(i32::MIN).is_err());
    assert!(validate_ramp_percent(i32::MAX).is_err());
}

// ── WorkerFilters build filtering ──────────────────────────────────────────

#[cfg(feature = "db")]
#[test]
fn parse_worker_filters_accepts_build_id_param() {
    use autumn_harvest::workers::parse_worker_filters;

    let pairs = vec![
        ("build_id".to_string(), "v2.0".to_string()),
        ("limit".to_string(), "10".to_string()),
    ];
    let filters = parse_worker_filters(&pairs).expect("should parse");
    assert_eq!(filters.build_id.as_deref(), Some("v2.0"));
    assert_eq!(filters.limit, 10);
}

#[cfg(feature = "db")]
#[test]
fn parse_worker_filters_accepts_deployment_name_param() {
    use autumn_harvest::workers::parse_worker_filters;

    let pairs = vec![("deployment_name".to_string(), "prod-blue".to_string())];
    let filters = parse_worker_filters(&pairs).expect("should parse");
    assert_eq!(filters.deployment_name.as_deref(), Some("prod-blue"));
}

// ── DB integration tests ───────────────────────────────────────────────────

#[cfg(feature = "db")]
mod db_tests {
    use autumn_harvest::build_routing::{
        build_reachability, declare_compat, get_build_policy, load_compat_set, revoke_compat,
        set_build_policy,
    };
    use autumn_harvest::models::{NewWorkflowExecution, WorkflowExecution};
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::harvest_workflow_executions;
    use autumn_harvest::workers::{WorkerFilters, list_workers, register_worker};
    use diesel::{QueryDsl, SelectableHelper};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use std::time::Duration;
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

    const INIT_SQL: &str = concat!(
        include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
        "\n",
        include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
        "\n",
        include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
        "\n",
        include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
        "\n",
        include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
        "\n",
        include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
        "\n",
        include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
        "\n",
        include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
        "\n",
        include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
        "\n",
        include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
        "\n",
        include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
        "\n",
        include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
        "\n",
        include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
        "\n",
        include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
        "\n",
        include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
        "\n",
        include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
        "\n",
        include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
        "\n",
        include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
        "\n",
        include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
        include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
        include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
        include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
        include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
        "\n",
        include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
        "\n",
        include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
        "\n",
        include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
        "\n",
        include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
        "\n",
        include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
        "\n",
        include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
        "\n",
        // issue #523: workflow-level retry policy columns.
        include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
        "\n",
        // issue #534: origin column + per-schedule run-history index.
        include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
        include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
        include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
        include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
        "\n",
        include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
        include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    );

    async fn setup() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_init_sql(INIT_SQL.to_string().into_bytes())
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (conn, container)
    }

    const STALE: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn worker_registration_stores_build_id_and_deployment_name() {
        let (mut conn, _c) = setup().await;

        register_worker(
            &mut conn,
            "worker-001",
            &["default".to_string()],
            &[0_i32],
            4,
            "host-a",
            Some("0.3.0"),
            "v1.0",
            Some("prod-blue"),
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .expect("register_worker");

        let workers = list_workers(&mut conn, &WorkerFilters::new(), STALE)
            .await
            .expect("list_workers");
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].worker.build_id, "v1.0");
        assert_eq!(
            workers[0].worker.deployment_name.as_deref(),
            Some("prod-blue")
        );
    }

    #[tokio::test]
    async fn list_workers_filters_by_build_id() {
        let (mut conn, _c) = setup().await;

        register_worker(
            &mut conn,
            "w-v1",
            &["q".to_string()],
            &[],
            2,
            "h",
            None,
            "v1.0",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();
        register_worker(
            &mut conn,
            "w-v2",
            &["q".to_string()],
            &[],
            2,
            "h",
            None,
            "v2.0",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();

        let mut filters = WorkerFilters::new();
        filters.build_id = Some("v1.0".to_string());
        let workers = list_workers(&mut conn, &filters, STALE).await.unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].worker.worker_id, "w-v1");
    }

    #[tokio::test]
    async fn set_and_get_build_policy_for_queue() {
        let (mut conn, _c) = setup().await;

        let policy = set_build_policy(&mut conn, "default", "v1.0", None)
            .await
            .expect("set_build_policy");
        assert_eq!(policy.queue_name, "default");
        assert_eq!(policy.build_id, "v1.0");
        assert!(policy.deployment_name.is_none());

        let loaded = get_build_policy(&mut conn, "default")
            .await
            .expect("get_build_policy")
            .expect("should be Some");
        assert_eq!(loaded.build_id, "v1.0");

        // Upsert: update the policy
        let updated = set_build_policy(&mut conn, "default", "v2.0", Some("prod-green"))
            .await
            .expect("upsert");
        assert_eq!(updated.build_id, "v2.0");
        assert_eq!(updated.deployment_name.as_deref(), Some("prod-green"));
    }

    #[tokio::test]
    async fn declare_and_load_compat_set() {
        let (mut conn, _c) = setup().await;

        let entry = declare_compat(&mut conn, "v2.0", "v1.0")
            .await
            .expect("declare_compat");
        assert_eq!(entry.build_id, "v2.0");
        assert_eq!(entry.compatible_with, "v1.0");

        let set = load_compat_set(&mut conn).await.expect("load_compat_set");
        assert!(set.is_eligible("v2.0", Some("v1.0")));
        assert!(!set.is_eligible("v1.0", Some("v2.0")));
    }

    #[tokio::test]
    async fn revoke_compat_removes_entry() {
        let (mut conn, _c) = setup().await;

        declare_compat(&mut conn, "v2.0", "v1.0").await.unwrap();
        let removed = revoke_compat(&mut conn, "v2.0", "v1.0").await.unwrap();
        assert!(removed);

        let set = load_compat_set(&mut conn).await.unwrap();
        assert!(!set.is_eligible("v2.0", Some("v1.0")));
    }

    // Helper: insert a workflow execution row and enqueue a workflow task.
    async fn insert_exec_and_task(
        conn: &mut AsyncPgConnection,
        exec_id: Uuid,
        required_build_id: Option<&str>,
    ) {
        diesel::insert_into(harvest_workflow_executions::table)
            .values(NewWorkflowExecution {
                id: exec_id,
                workflow_name: "test_wf",
                workflow_id: &exec_id.to_string(),
                run_id: Uuid::new_v4(),
                shard_id: 0,
                input: serde_json::json!({}),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                deadline_at: None,
                memo: None,
                search_attrs: None,
                assigned_build_id: required_build_id.map(str::to_string),
                parent_close_policy: None,

                owner: None,
                runbook_url: None,
                severity: None,
                context_headers: None,

                sla: None,

                sla_deadline_at: None,
                schedule_id: None,
                scheduled_for: None,
                workflow_attempt: 1,
                workflow_retry_policy: None,
                retry_of_exec_id: None,
                origin: None,
                completion_callbacks: None,
            })
            .execute(conn)
            .await
            .expect("insert execution");

        let mut params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!({}));
        params.workflow_exec_id = Some(exec_id);
        params.required_build_id = required_build_id.map(str::to_string);
        queue::enqueue(conn, &params).await.expect("enqueue");
    }

    #[tokio::test]
    async fn compatible_worker_can_claim_task_with_required_build() {
        let (mut conn, _c) = setup().await;

        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, Some("v1.0")).await;

        // Worker running v1.0 should claim its own task.
        let task = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-a",
            "v1.0",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(task.is_some(), "v1.0 worker should claim v1.0 task");
    }

    #[tokio::test]
    async fn incompatible_worker_cannot_claim_required_build_task() {
        let (mut conn, _c) = setup().await;

        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, Some("v1.0")).await;

        // Worker running v2.0 with no declared compatibility should get nothing.
        let task = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-b",
            "v2.0",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            task.is_none(),
            "v2.0 worker must not claim v1.0 task without compat declaration"
        );
    }

    #[tokio::test]
    async fn declared_compat_allows_new_worker_to_claim_old_task() {
        let (mut conn, _c) = setup().await;

        declare_compat(&mut conn, "v2.0", "v1.0").await.unwrap();

        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, Some("v1.0")).await;

        let task = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-c",
            "v2.0",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            task.is_some(),
            "v2.0 worker with compat declaration should claim v1.0 task"
        );
    }

    #[tokio::test]
    async fn task_without_required_build_claimed_by_any_worker() {
        let (mut conn, _c) = setup().await;

        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, None).await;

        // Any worker (including one with a build_id) can claim an untagged task.
        let task = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-d",
            "v99.0",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            task.is_some(),
            "any worker should claim task with no required build"
        );
    }

    #[tokio::test]
    async fn legacy_worker_empty_build_id_claims_any_task() {
        let (mut conn, _c) = setup().await;

        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, Some("v1.0")).await;

        // Legacy worker with empty build_id can claim anything.
        let task = queue::claim_task(
            &mut conn,
            &["default".to_string()],
            "worker-legacy",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            task.is_some(),
            "legacy worker should claim build-tagged task"
        );
    }

    #[tokio::test]
    async fn build_reachability_counts_open_executions_and_pending_tasks() {
        let (mut conn, _c) = setup().await;

        // Insert two v1.0 executions, one v2.0 execution.
        for _ in 0..2_u32 {
            let exec_id = Uuid::new_v4();
            insert_exec_and_task(&mut conn, exec_id, Some("v1.0")).await;
        }
        let exec_id = Uuid::new_v4();
        insert_exec_and_task(&mut conn, exec_id, Some("v2.0")).await;

        // Register one v1.0 worker, one v2.0 worker.
        register_worker(
            &mut conn,
            "w-v1",
            &["default".to_string()],
            &[],
            2,
            "h",
            None,
            "v1.0",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();
        register_worker(
            &mut conn,
            "w-v2",
            &["default".to_string()],
            &[],
            2,
            "h",
            None,
            "v2.0",
            None,
            &std::collections::HashMap::new(),
            0,
        )
        .await
        .unwrap();

        let v1_reach = build_reachability(&mut conn, "v1.0", Duration::from_secs(60))
            .await
            .expect("reachability");
        assert_eq!(v1_reach.open_executions, 2);
        assert_eq!(v1_reach.pending_tasks, 2);
        assert_eq!(v1_reach.active_workers, 1);
        assert!(!v1_reach.safe_to_retire);

        let v2_reach = build_reachability(&mut conn, "v2.0", Duration::from_secs(60))
            .await
            .expect("reachability");
        assert_eq!(v2_reach.open_executions, 1);
        assert_eq!(v2_reach.pending_tasks, 1);
        assert_eq!(v2_reach.active_workers, 1);
        assert!(!v2_reach.safe_to_retire);
    }

    #[tokio::test]
    async fn safe_to_retire_true_when_no_open_executions_or_pending_tasks() {
        let (mut conn, _c) = setup().await;

        // No executions for v1.0, no v1.0 workers registered.
        let reach = build_reachability(&mut conn, "v1.0", Duration::from_secs(60))
            .await
            .expect("reachability");
        assert_eq!(reach.open_executions, 0);
        assert_eq!(reach.pending_tasks, 0);
        assert_eq!(reach.active_workers, 0);
        assert!(
            reach.safe_to_retire,
            "should be safe to retire with zero open work"
        );
    }

    // ── Percent-based build ramp: DB-backed lifecycle (issue #604) ─────────
    //
    // These tests exercise `set_build_ramp`/`clear_build_ramp` and the
    // `start_or_load_workflow_execution` stamp point end-to-end against a
    // real Postgres instance. In this sandbox (no Docker/testcontainers)
    // they are compile-checked only via `cargo test --no-run`.

    use autumn_harvest::build_routing::{clear_build_ramp, set_build_ramp};
    use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
    use autumn_harvest::types::{ExecutionId, Priority, ShardId, WorkflowIdReusePolicy};

    fn start_params<'a>(
        workflow_name: &'a str,
        workflow_id: &'a str,
        exec_id: ExecutionId,
    ) -> StartWorkflowParams<'a> {
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,

            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
        }
    }

    #[tokio::test]
    async fn set_build_ramp_updates_target_and_percent_on_existing_policy() {
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .expect("set_build_policy");

        let ramped = set_build_ramp(&mut conn, "default", "canary-v2", 25)
            .await
            .expect("set_build_ramp");
        assert_eq!(ramped.build_id, "base-v1", "base build untouched by ramp");
        assert_eq!(ramped.target_build_id.as_deref(), Some("canary-v2"));
        assert_eq!(ramped.ramp_percent, Some(25));

        let loaded = get_build_policy(&mut conn, "default")
            .await
            .expect("get_build_policy")
            .expect("should be Some");
        assert_eq!(loaded.target_build_id.as_deref(), Some("canary-v2"));
        assert_eq!(loaded.ramp_percent, Some(25));
    }

    #[tokio::test]
    async fn set_build_ramp_without_base_policy_is_config_error() {
        let (mut conn, _c) = setup().await;

        // No base policy has been set for "default" yet.
        let err = set_build_ramp(&mut conn, "default", "canary-v2", 25)
            .await
            .expect_err("ramp on a queue with no base policy must fail");
        assert!(
            matches!(err, autumn_harvest::error::HarvestError::Config(_)),
            "expected Config error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn clear_build_ramp_nulls_target_and_percent() {
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .unwrap();
        set_build_ramp(&mut conn, "default", "canary-v2", 50)
            .await
            .unwrap();

        let cleared = clear_build_ramp(&mut conn, "default")
            .await
            .expect("clear_build_ramp")
            .expect("policy should exist");
        assert!(cleared.target_build_id.is_none());
        assert!(cleared.ramp_percent.is_none());
        assert_eq!(cleared.build_id, "base-v1", "base build survives clear");

        let loaded = get_build_policy(&mut conn, "default")
            .await
            .unwrap()
            .unwrap();
        assert!(loaded.target_build_id.is_none());
        assert!(loaded.ramp_percent.is_none());
    }

    #[tokio::test]
    async fn clear_build_ramp_on_unknown_queue_returns_none() {
        let (mut conn, _c) = setup().await;

        let result = clear_build_ramp(&mut conn, "no-such-queue")
            .await
            .expect("clear_build_ramp should not error");
        assert!(result.is_none());
    }

    async fn reload_assigned_build_id(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> Option<String> {
        let row: WorkflowExecution = harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(conn)
            .await
            .expect("reload execution");
        row.assigned_build_id
    }

    #[tokio::test]
    async fn start_stamps_target_build_for_fully_ramped_queue() {
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .unwrap();
        set_build_ramp(&mut conn, "default", "canary-v2", 100)
            .await
            .unwrap();

        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            start_params("ramped_wf", "ramped-wf-001", exec_id),
        )
        .await
        .expect("start_or_load_workflow_execution");

        assert_eq!(
            reload_assigned_build_id(&mut conn, exec_id)
                .await
                .as_deref(),
            Some("canary-v2"),
            "100% ramp must stamp the target build"
        );
    }

    #[tokio::test]
    async fn start_stamps_base_build_when_ramp_is_zero() {
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .unwrap();
        set_build_ramp(&mut conn, "default", "canary-v2", 0)
            .await
            .unwrap();

        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            start_params("ramped_wf", "ramped-wf-002", exec_id),
        )
        .await
        .expect("start_or_load_workflow_execution");

        assert_eq!(
            reload_assigned_build_id(&mut conn, exec_id)
                .await
                .as_deref(),
            Some("base-v1"),
            "0% ramp must stamp the base build"
        );
    }

    #[tokio::test]
    async fn ramp_back_to_zero_stops_new_starts_from_reaching_target() {
        // AC6: setting ramp back to 0 (or clearing it) must immediately stop
        // new starts going to the target; verified by a follow-up start
        // landing on the base build.
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .unwrap();
        set_build_ramp(&mut conn, "default", "canary-v2", 100)
            .await
            .unwrap();

        let first_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            start_params("ramped_wf", "ramped-wf-003", first_exec_id),
        )
        .await
        .unwrap();
        assert_eq!(
            reload_assigned_build_id(&mut conn, first_exec_id)
                .await
                .as_deref(),
            Some("canary-v2")
        );

        // Abort the canary.
        clear_build_ramp(&mut conn, "default").await.unwrap();

        let second_exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            start_params("ramped_wf", "ramped-wf-004", second_exec_id),
        )
        .await
        .unwrap();
        assert_eq!(
            reload_assigned_build_id(&mut conn, second_exec_id)
                .await
                .as_deref(),
            Some("base-v1"),
            "follow-up start after clearing the ramp must land on the base build"
        );

        // The first (already-started) execution must not be re-routed —
        // in-flight assignment is immutable (AC5).
        assert_eq!(
            reload_assigned_build_id(&mut conn, first_exec_id)
                .await
                .as_deref(),
            Some("canary-v2"),
            "in-flight execution's assigned build must remain immutable"
        );
    }

    #[tokio::test]
    async fn pre_ramp_policy_row_without_ramp_columns_still_routes_to_base_build() {
        // Back-compat (AC additive migration): a policy row created before
        // this feature shipped (ramp columns NULL by default) must keep
        // routing 100% of new starts to the base build, unchanged.
        let (mut conn, _c) = setup().await;

        set_build_policy(&mut conn, "default", "base-v1", None)
            .await
            .unwrap();

        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            start_params("ramped_wf", "ramped-wf-005", exec_id),
        )
        .await
        .unwrap();
        assert_eq!(
            reload_assigned_build_id(&mut conn, exec_id)
                .await
                .as_deref(),
            Some("base-v1")
        );
    }
}
