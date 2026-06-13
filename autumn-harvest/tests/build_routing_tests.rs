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
    use autumn_harvest::models::NewWorkflowExecution;
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::harvest_workflow_executions;
    use autumn_harvest::workers::{WorkerFilters, list_workers, register_worker};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use std::time::Duration;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

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
        include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
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
        include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
        "\n",
        include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
        "\n",
        include_str!("../migrations/20260615000001_harvest_context_headers/up.sql")
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
}
