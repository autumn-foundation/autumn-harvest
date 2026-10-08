#![cfg(feature = "db")]
#![allow(
    clippy::doc_markdown,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::items_after_statements,
    clippy::default_trait_access,
    clippy::significant_drop_tightening
)]

use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use serde_json::json;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::event_batch::{AdmitBatchParams, admit_batched_start, fire_due_event_batches};
use autumn_harvest::telemetry::NoOpMetrics;

async fn setup_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("migrations");

    (conn, container)
}

#[tokio::test]
async fn test_event_batch_accumulation_and_size_flush() {
    let (mut conn, _container) = setup_db().await;

    // Call 1
    let p1 = AdmitBatchParams {
        workflow_name: "test_batch".to_string(),
        batch_key: "key-1".to_string(),
        workflow_id: "wf-1".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 1}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 3,
        shard_id: 0,
    };
    let (o1, _deferred_starts1) = admit_batched_start(&mut conn, p1, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o1.pending_count, 1);
    assert!(!o1.is_flushed);

    // Call 2
    let p2 = AdmitBatchParams {
        workflow_name: "test_batch".to_string(),
        batch_key: "key-1".to_string(),
        workflow_id: "wf-1".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 2}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 3,
        shard_id: 0,
    };
    let (o2, _deferred_starts2) = admit_batched_start(&mut conn, p2, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o2.pending_count, 2);
    assert!(!o2.is_flushed);

    // Call 3 (should trigger immediate size flush)
    let p3 = AdmitBatchParams {
        workflow_name: "test_batch".to_string(),
        batch_key: "key-1".to_string(),
        workflow_id: "wf-1".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 3}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 3,
        shard_id: 0,
    };
    let (o3, _deferred_starts3) = admit_batched_start(&mut conn, p3, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o3.pending_count, 3);
    assert!(o3.is_flushed);
    assert!(o3.flushed_execution_id.is_some());
}

/// Read back the `completion_callbacks` column for a started execution.
async fn started_execution_completion_callbacks(
    conn: &mut AsyncPgConnection,
    wf_id: &str,
) -> Option<serde_json::Value> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        completion_callbacks: Option<serde_json::Value>,
    }

    diesel::sql_query(
        "SELECT completion_callbacks FROM harvest_workflow_executions WHERE workflow_id=$1",
    )
    .bind::<diesel::sql_types::Text, _>(wf_id)
    .get_result::<Row>(conn)
    .await
    .expect("execution row query")
    .completion_callbacks
}

// Regression (issue #605 code review): a per-execution completion_callbacks
// target must survive both batch admission paths, not just the direct
// start path -- neither a size-triggered flush nor a scanner-driven
// time-based flush may silently drop it.
#[tokio::test]
async fn size_triggered_flush_threads_completion_callbacks_into_the_started_execution() {
    let (mut conn, _container) = setup_db().await;
    let callbacks = json!([
        { "url": "https://api.example.com/hook", "filter": { "type": "AnyTerminal" } }
    ]);

    for i in 1..=3 {
        let p = AdmitBatchParams {
            workflow_name: "callback_batch_wf".to_string(),
            batch_key: "key-cb".to_string(),
            workflow_id: "callback-batch-001".to_string(),
            queue_name: "default".to_string(),
            payload: json!({"val": i}),
            start_options: DebounceStartOptions {
                completion_callbacks: Some(callbacks.clone()),
                ..Default::default()
            },
            max_wait: Duration::from_secs(10),
            max_size: 3,
            shard_id: 0,
        };
        let (o, _deferred_starts) = admit_batched_start(&mut conn, p, None)
            .await
            .unwrap()
            .unwrap();
        if i == 3 {
            assert!(o.is_flushed);
            assert!(o.flushed_execution_id.is_some());
        }
    }

    // Production CONCATENATES each admission's completion_callbacks (issue #921)
    // and dedups only at DELIVERY time — so admitting the same single-target
    // array on all 3 batch admissions stores the concatenated union
    // `[hook, hook, hook]`, NOT `[hook]`. The passing sibling
    // `a_later_admissions_completion_callbacks_are_merged_not_dropped` encodes the
    // same concatenate-and-merge contract with two *distinct* targets (→ len 2).
    let stored = started_execution_completion_callbacks(&mut conn, "callback-batch-001").await;
    let hook = callbacks.as_array().expect("callbacks is an array")[0].clone();
    let expected = json!([hook, hook, hook]);
    assert_eq!(
        stored,
        Some(expected),
        "a size-flushed batch start must carry every admission's completion_callbacks \
         through, concatenated (deduped only at delivery)"
    );
}

// Regression (issue #921 review, Codex P2): the ON CONFLICT upsert only
// ever updated buffered_payloads/fire_at, leaving the *first* admission's
// start_options (including completion_callbacks) untouched. A later
// admission into the same batch group that specified a *different*
// completion_callbacks target would therefore be silently dropped -- that
// caller's callback would never fire when the collapsed execution
// completes. The two admissions' target arrays must instead be merged.
#[tokio::test]
async fn a_later_admissions_completion_callbacks_are_merged_not_dropped() {
    let (mut conn, _container) = setup_db().await;
    let first_target = json!([
        { "url": "https://a.example.com/hook", "filter": { "type": "AnyTerminal" } }
    ]);
    let second_target = json!([
        { "url": "https://b.example.com/hook", "filter": { "type": "CompletedOnly" } }
    ]);

    let p1 = AdmitBatchParams {
        workflow_name: "callback_merge_batch_wf".to_string(),
        batch_key: "key-cb-merge".to_string(),
        workflow_id: "callback-merge-batch-001".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 1}),
        start_options: DebounceStartOptions {
            completion_callbacks: Some(first_target.clone()),
            ..Default::default()
        },
        max_wait: Duration::from_secs(10),
        max_size: 2,
        shard_id: 0,
    };
    let (o1, _deferred_starts1) = admit_batched_start(&mut conn, p1, None)
        .await
        .unwrap()
        .unwrap();
    assert!(!o1.is_flushed);

    let p2 = AdmitBatchParams {
        workflow_name: "callback_merge_batch_wf".to_string(),
        batch_key: "key-cb-merge".to_string(),
        workflow_id: "callback-merge-batch-001".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 2}),
        start_options: DebounceStartOptions {
            completion_callbacks: Some(second_target.clone()),
            ..Default::default()
        },
        max_wait: Duration::from_secs(10),
        max_size: 2,
        shard_id: 0,
    };
    let (o2, _deferred_starts2) = admit_batched_start(&mut conn, p2, None)
        .await
        .unwrap()
        .unwrap();
    assert!(
        o2.is_flushed,
        "second admission reaches max_size and flushes"
    );

    let stored = started_execution_completion_callbacks(&mut conn, "callback-merge-batch-001")
        .await
        .expect("flushed execution must carry completion_callbacks");
    let stored_targets = stored.as_array().expect("completion_callbacks is an array");
    assert_eq!(
        stored_targets.len(),
        2,
        "both admissions' callback targets must survive, merged: {stored_targets:?}"
    );
    assert!(
        stored_targets
            .iter()
            .any(|t| t["url"] == "https://a.example.com/hook"),
        "the first admission's target must survive: {stored_targets:?}"
    );
    assert!(
        stored_targets
            .iter()
            .any(|t| t["url"] == "https://b.example.com/hook"),
        "the second (later) admission's target must survive, not be silently dropped: \
         {stored_targets:?}"
    );
}

#[tokio::test]
async fn time_triggered_flush_threads_completion_callbacks_into_the_started_execution() {
    let (mut conn, _container) = setup_db().await;
    let callbacks = json!([
        { "url": "https://api.example.com/hook", "filter": { "type": "CompletedOnly" } }
    ]);

    let p = AdmitBatchParams {
        workflow_name: "callback_time_batch_wf".to_string(),
        batch_key: "key-cb-time".to_string(),
        workflow_id: "callback-time-batch-001".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 1}),
        start_options: DebounceStartOptions {
            completion_callbacks: Some(callbacks.clone()),
            ..Default::default()
        },
        max_wait: Duration::from_secs(10),
        max_size: 5,
        shard_id: 0,
    };
    let (o, _deferred_starts) = admit_batched_start(&mut conn, p, None)
        .await
        .unwrap()
        .unwrap();
    assert!(!o.is_flushed);

    diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
        .execute(&mut conn)
        .await
        .unwrap();

    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .unwrap();
    assert_eq!(fired, 1);

    let stored = started_execution_completion_callbacks(&mut conn, "callback-time-batch-001").await;
    assert_eq!(
        stored,
        Some(callbacks),
        "a time-flushed batch start must carry the caller's completion_callbacks through"
    );
}

#[tokio::test]
async fn test_event_batch_time_flush() {
    let (mut conn, _container) = setup_db().await;

    let p = AdmitBatchParams {
        workflow_name: "test_time_batch".to_string(),
        batch_key: "key-1".to_string(),
        workflow_id: "wf-2".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"val": 100}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 5,
        shard_id: 0,
    };
    let (o, _deferred_starts) = admit_batched_start(&mut conn, p, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(o.pending_count, 1);
    assert!(!o.is_flushed);

    // Update fire_at to be in the past
    diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
        .execute(&mut conn)
        .await
        .unwrap();

    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .unwrap();
    assert_eq!(fired, 1);
}
