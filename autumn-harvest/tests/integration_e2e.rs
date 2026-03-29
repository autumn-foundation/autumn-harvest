//! End-to-end integration tests using testcontainers for a real Postgres instance.
//!
//! These tests spin up a throwaway Postgres container per test, run the harvest
//! migration SQL via `with_init_sql`, and exercise the full store/queue/DLQ stack
//! against a real database.

use autumn_harvest::dlq::{self, NewDeadLetterEntry};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{EnqueueParams, TaskType};
use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use autumn_harvest::{HarvestError, queue};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The migration SQL embedded at compile time.
const INIT_SQL: &str = include_str!("../migrations/00000000000000_harvest_initial/up.sql");

/// Start a Postgres container with the harvest schema applied and return
/// an `AsyncPgConnection` ready for use.
///
/// CRITICAL: the returned `ContainerAsync` must be held alive for the duration
/// of the test -- dropping it kills the container.
async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to Postgres container");

    (conn, container)
}

/// Insert a minimal `harvest_workflow_executions` row and return its UUID.
async fn insert_workflow_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "e2e_test_workflow",
        workflow_id: "e2e-wf-001",
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({"test": true}),
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("failed to insert workflow execution");

    exec_id
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_workflow_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    // 1. Append WorkflowStarted event
    let started_events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({"user": "alice"}),
        timestamp: Utc::now(),
    }];
    let inserted = store::append_events(&mut conn, exec_id, &started_events, 0)
        .await
        .expect("append WorkflowStarted failed");
    assert_eq!(inserted, 1);

    // 2. Load history -- verify 1 event
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 1);
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert_eq!(history.next_event_id, 1);

    // 3. Enqueue an activity task
    //    Set scheduled_at slightly in the past to avoid clock skew between
    //    the host (where Utc::now() runs) and the Docker container (where
    //    Postgres NOW() runs).
    let mut params = EnqueueParams::new(
        "default",
        TaskType::Activity,
        serde_json::json!({"to": "bob@example.com"}),
    );
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.activity_name = Some("send_email".into());
    params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);

    let task_id = queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue failed");

    // 4. Claim the task
    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-e2e-1")
        .await
        .expect("claim_task failed");
    let claimed = claimed.expect("no task claimed");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.activity_name.as_deref(), Some("send_email"));
    assert_eq!(claimed.state, "RUNNING");

    // 5. Complete the task
    queue::complete_task(&mut conn, task_id, serde_json::json!({"sent": true}))
        .await
        .expect("complete_task failed");

    // 6. Append activity completion + workflow completion events
    let activity_id = ActivityExecId::new();
    let completion_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "send_email".into(),
            input: serde_json::json!({"to": "bob@example.com"}),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!({"sent": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"status": "ok"}),
        },
    ];
    let inserted = store::append_events(&mut conn, exec_id, &completion_events, 1)
        .await
        .expect("append completion events failed");
    assert_eq!(inserted, 3);

    // 7. Load final history -- verify 4 events total
    //    (Started + ActivityScheduled + ActivityCompleted + WorkflowCompleted)
    let final_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("final load_history failed");
    assert_eq!(final_history.events.len(), 4);
    assert!(matches!(
        final_history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    assert!(matches!(
        final_history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    assert!(matches!(
        final_history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    assert!(matches!(
        final_history.events[3],
        WorkflowEvent::WorkflowCompleted { .. }
    ));
    assert_eq!(final_history.next_event_id, 4);

    // 8. Verify the completed task in the queue has COMPLETED state
    let task: Vec<autumn_harvest::models::TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::id.eq(task_id))
        .load(&mut conn)
        .await
        .expect("failed to query task");
    assert_eq!(task.len(), 1);
    assert_eq!(task[0].state, "COMPLETED");
}

#[tokio::test]
async fn claim_task_returns_none_on_empty_queue() {
    let (mut conn, _container) = setup_test_db().await;

    let queues = vec!["default".to_string()];
    let claimed = queue::claim_task(&mut conn, &queues, "worker-empty-1")
        .await
        .expect("claim_task failed");
    assert!(
        claimed.is_none(),
        "expected None from empty queue, got {claimed:?}"
    );
}

#[tokio::test]
async fn dead_letter_queue_lifecycle() {
    let (mut conn, _container) = setup_test_db().await;

    // Verify DLQ starts empty
    let initial_count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(initial_count, 0);

    // Insert a dead letter entry
    let entry = NewDeadLetterEntry {
        original_task_id: Uuid::new_v4(),
        queue_name: "default".into(),
        task_type: "ACTIVITY".into(),
        workflow_exec_id: None,
        activity_name: Some("flaky_step".into()),
        input: serde_json::json!({"attempt": 3}),
        error: "SMTP connection refused after 3 retries".into(),
        attempts: 3,
    };

    let dlq_id = dlq::dead_letter(&mut conn, &entry)
        .await
        .expect("dead_letter insert failed");
    assert!(!dlq_id.is_nil(), "DLQ entry should have a valid UUID");

    // Verify count is now 1
    let count = dlq::dead_letter_count(&mut conn)
        .await
        .expect("dead_letter_count failed");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn event_store_round_trip() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let activity_id_1 = ActivityExecId::new();
    let activity_id_2 = ActivityExecId::new();

    // Append 3 events in one batch
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({"batch": "round_trip"}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_1,
            name: "step_1".into(),
            input: serde_json::json!(1),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_1,
            output: serde_json::json!({"result": "done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("append failed");
    assert_eq!(inserted, 3);

    // Load and verify count
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load_history failed");
    assert_eq!(history.events.len(), 3);
    assert_eq!(history.next_event_id, 3);

    // Verify deserialization fidelity
    assert!(matches!(
        history.events[0],
        WorkflowEvent::WorkflowStarted { .. }
    ));
    if let WorkflowEvent::WorkflowStarted { ref input, .. } = history.events[0] {
        assert_eq!(input, &serde_json::json!({"batch": "round_trip"}));
    }

    assert!(matches!(
        history.events[1],
        WorkflowEvent::ActivityScheduled { .. }
    ));
    if let WorkflowEvent::ActivityScheduled { ref name, .. } = history.events[1] {
        assert_eq!(name, "step_1");
    }

    assert!(matches!(
        history.events[2],
        WorkflowEvent::ActivityCompleted { .. }
    ));
    if let WorkflowEvent::ActivityCompleted { ref output, .. } = history.events[2] {
        assert_eq!(output, &serde_json::json!({"result": "done"}));
    }

    // Append more events and verify continuity
    let more_events = vec![
        WorkflowEvent::ActivityScheduled {
            activity_id: activity_id_2,
            name: "step_2".into(),
            input: serde_json::json!(2),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: activity_id_2,
            output: serde_json::json!({"result": "also done"}),
        },
    ];

    let inserted = store::append_events(&mut conn, exec_id, &more_events, 3)
        .await
        .expect("second append failed");
    assert_eq!(inserted, 2);

    let full_history = store::load_history(&mut conn, exec_id)
        .await
        .expect("full load_history failed");
    assert_eq!(full_history.events.len(), 5);
    assert_eq!(full_history.next_event_id, 5);
}

#[tokio::test]
async fn duplicate_event_id_is_rejected() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_workflow_execution(&mut conn).await;

    let events = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
    }];

    // First insert succeeds
    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("first append should succeed");

    // Second insert with same start_id should fail (unique constraint)
    let result = store::append_events(&mut conn, exec_id, &events, 0).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HarvestError::Database(_)));
}
