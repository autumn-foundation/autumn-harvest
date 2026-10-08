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
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::debounce::DebounceStartOptions;
use autumn_harvest::event_batch::{AdmitBatchParams, admit_batched_start, fire_due_event_batches};
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::DbPool;

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
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migrations");

    (conn, container)
}

/// Derive a running container's own Postgres URL (issue #1362). Needed to
/// build a `DbPool` aimed at the same physical database `setup_db` already
/// connected to, for the multi-shard test below.
async fn container_url(container: &ContainerAsync<Postgres>) -> String {
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    format!("postgresql://postgres:postgres@{host}:{port}/postgres")
}

/// Build a connection pool for a database URL (issue #1362), to construct a
/// `ShardedDbPool` test fixture.
fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
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

// ── Empty workflow_id admission (issue #1353) ─────────────────────────────────

// Admission must reject an empty id before it writes a row. A written row
// with no id could only be discarded on fire, never started.
#[tokio::test]
async fn admit_batched_start_rejects_empty_workflow_id_before_writing_a_row() {
    let (mut conn, _container) = setup_db().await;

    let p = AdmitBatchParams {
        workflow_name: "empty_id_batch_wf".to_string(),
        batch_key: "key-empty-id".to_string(),
        workflow_id: String::new(),
        queue_name: "default".to_string(),
        payload: json!({}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 3,
        shard_id: 0,
    };
    let err = admit_batched_start(&mut conn, p, None)
        .await
        .expect_err("empty workflow_id must be rejected");
    assert!(matches!(
        err,
        autumn_harvest::error::HarvestError::EmptyWorkflowId
    ));

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let n =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_event_batches WHERE batch_key = $1")
            .bind::<diesel::sql_types::Text, _>("key-empty-id")
            .get_result::<Count>(&mut conn)
            .await
            .expect("count query")
            .n;
    assert_eq!(n, 0, "no row was written");
}

// ── Legacy empty-workflow_id row healing (issue #1430) ──────────────────────
//
// A row admitted before #1353's guard shipped can hold a stored empty
// workflow_id. The ON CONFLICT upsert never touched that stored id. A later
// valid request for the same key then merged its payload into the poisoned
// row. Its own accepted outcome came back with an empty id. The scanner
// later deleted the whole row as unfireable. That silently dropped the new
// request's payload along with the legacy one. Admission must instead heal
// the stored id to the new request's valid id.

/// Seed a pre-#1353 event-batch row directly. The empty-id check postdates
/// such a row, so only a direct table write can produce one.
///
/// `max_size` is first-request-wins: the ON CONFLICT upsert never touches
/// it. Set it here to whatever a later admission in the same test needs to
/// reach or stay under.
async fn seed_legacy_empty_id_batch_row(
    conn: &mut AsyncPgConnection,
    wf: &str,
    key: &str,
    max_size: i32,
) {
    diesel::sql_query(
        "INSERT INTO harvest_event_batches
            (workflow_name, batch_key, workflow_id, queue_name, buffered_payloads,
             start_options, fire_at, max_size)
         VALUES ($1, $2, '', 'default', '[{\"legacy\": true}]'::jsonb,
                 '{}'::jsonb, NOW() + INTERVAL '1 hour', $3)",
    )
    .bind::<diesel::sql_types::Text, _>(wf)
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Integer, _>(max_size)
    .execute(conn)
    .await
    .expect("seed legacy row");
}

/// Count rows in `harvest_workflow_executions` for a given `workflow_id`.
async fn execution_count(conn: &mut AsyncPgConnection, wf_id: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id=$1")
        .bind::<diesel::sql_types::Text, _>(wf_id)
        .get_result::<Count>(conn)
        .await
        .expect("execution count query")
        .n
}

#[tokio::test]
async fn admit_heals_a_legacy_empty_workflow_id_row_to_the_new_valid_id() {
    let (mut conn, _container) = setup_db().await;

    let wf = "legacy_heal_batch_wf";
    let key = "key-legacy-heal";
    seed_legacy_empty_id_batch_row(&mut conn, wf, key, 5).await;

    let new_id = "legacy-heal-batch-valid-001";
    let p = AdmitBatchParams {
        workflow_name: wf.to_string(),
        batch_key: key.to_string(),
        workflow_id: new_id.to_string(),
        queue_name: "default".to_string(),
        payload: json!({"fresh": true}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(60),
        max_size: 5,
        shard_id: 0,
    };
    let (outcome, _deferred_starts) = admit_batched_start(&mut conn, p, None)
        .await
        .expect("admit onto the legacy row")
        .expect("admission accepted");

    assert!(
        !outcome.is_flushed,
        "two buffered payloads must stay under max_size 5"
    );
    assert_eq!(
        outcome.workflow_id, new_id,
        "a new valid request must heal a legacy empty-id row's stored id, not inherit the poison"
    );

    diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
        .execute(&mut conn)
        .await
        .expect("force fire_at into the past");

    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("fire");
    assert_eq!(
        fired, 1,
        "the healed row must fire, not be dropped as poison"
    );
    assert_eq!(
        execution_count(&mut conn, new_id).await,
        1,
        "the execution must start under the new request's valid id"
    );
}

// The healing admission itself can be the one that reaches max_size and
// flushes synchronously, inside the same upsert transaction. The started
// execution must use the HEALED id read back from the upsert's RETURNING
// clause, not a stale empty id captured before healing.
#[tokio::test]
async fn a_healing_admission_that_reaches_max_size_flushes_under_the_healed_id() {
    let (mut conn, _container) = setup_db().await;

    let wf = "legacy_heal_flush_batch_wf";
    let key = "key-legacy-heal-flush";
    // max_size is first-admission-wins, so it is set here at seed time; the
    // admission below's own max_size is ignored once the row already exists.
    seed_legacy_empty_id_batch_row(&mut conn, wf, key, 2).await;

    let new_id = "legacy-heal-flush-valid-001";
    let p = AdmitBatchParams {
        workflow_name: wf.to_string(),
        batch_key: key.to_string(),
        workflow_id: new_id.to_string(),
        queue_name: "default".to_string(),
        payload: json!({"fresh": true}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(60),
        // The seeded legacy row already holds one buffered payload; this
        // admission is the second, so max_size 2 triggers an immediate flush.
        max_size: 2,
        shard_id: 0,
    };
    let (outcome, _deferred_starts) = admit_batched_start(&mut conn, p, None)
        .await
        .expect("admit onto the legacy row")
        .expect("admission accepted");

    assert!(
        outcome.is_flushed,
        "the second payload must reach max_size and flush immediately"
    );
    assert_eq!(
        outcome.workflow_id, new_id,
        "the immediate flush must use the healed id, not the legacy empty one"
    );
    assert!(outcome.flushed_execution_id.is_some());
    assert_eq!(
        execution_count(&mut conn, new_id).await,
        1,
        "the synchronously-flushed execution must exist under the healed id"
    );
}

// Healing must only replace a stored empty id. It must not turn the
// workflow_id column into always-overwrite. A healed row is a normal row
// from that point on. A second, later admission with a different valid id
// must still lose to the row's own, now-healed, first id. That matches how
// two ordinary admissions already behave.
#[tokio::test]
async fn a_second_distinct_valid_id_does_not_override_an_already_healed_row() {
    let (mut conn, _container) = setup_db().await;

    let wf = "legacy_heal_second_batch_wf";
    let key = "key-legacy-heal-second";
    // max_size large enough that neither admission below flushes.
    let max_size: usize = 10;
    seed_legacy_empty_id_batch_row(&mut conn, wf, key, max_size as i32).await;

    let healed_id = "legacy-heal-batch-first-001";
    let other_id = "legacy-heal-batch-second-002";

    let first = AdmitBatchParams {
        workflow_name: wf.to_string(),
        batch_key: key.to_string(),
        workflow_id: healed_id.to_string(),
        queue_name: "default".to_string(),
        payload: json!({"seq": 1}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(60),
        max_size,
        shard_id: 0,
    };
    let (o1, _d1) = admit_batched_start(&mut conn, first, None)
        .await
        .expect("heal the legacy row")
        .expect("admission accepted");
    assert!(!o1.is_flushed);
    assert_eq!(o1.workflow_id, healed_id);

    let second = AdmitBatchParams {
        workflow_name: wf.to_string(),
        batch_key: key.to_string(),
        workflow_id: other_id.to_string(),
        queue_name: "default".to_string(),
        payload: json!({"seq": 2}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(60),
        max_size,
        shard_id: 0,
    };
    let (o2, _d2) = admit_batched_start(&mut conn, second, None)
        .await
        .expect("second admission onto the now-healed row")
        .expect("admission accepted");
    assert!(!o2.is_flushed);
    assert_eq!(
        o2.workflow_id, healed_id,
        "a second, distinct valid id must not override an already-healed row"
    );

    diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
        .execute(&mut conn)
        .await
        .expect("force fire_at into the past");

    let fired = fire_due_event_batches(&mut conn, &None, &[], &NoOpMetrics)
        .await
        .expect("fire");
    assert_eq!(fired, 1);
    assert_eq!(
        execution_count(&mut conn, healed_id).await,
        1,
        "the execution must start under the FIRST healed id"
    );
    assert_eq!(execution_count(&mut conn, other_id).await, 0);
}

// ── Multi-shard scanning (issue #1362) ──────────────────────────────────────
//
// `harvest_event_batches` filters by an explicit `shard_id` column when the
// scanner is given one (see `fire_due_on_conn`'s `due_sql`), unlike
// debounce/throttle's shard-per-database model. So, mirroring
// `completion_callback_tests.rs`'s sharded test, this uses ONE physical
// database with two `ShardId`s pointing at pools built from the same URL.

// The multi-shard branch had no integration coverage before this test:
// every existing call in this file passes `&None` and `&[]`. Each assigned
// shard's own due row must fire on a single scanner tick.
#[tokio::test]
async fn fire_due_event_batches_fires_each_assigned_shards_own_due_row() {
    let (mut conn, container) = setup_db().await;

    let p0 = AdmitBatchParams {
        workflow_name: "shard_batch_wf".to_string(),
        batch_key: "key-shard0".to_string(),
        workflow_id: "shard-batch-0".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"shard": 0}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 5,
        shard_id: 0,
    };
    admit_batched_start(&mut conn, p0, None)
        .await
        .unwrap()
        .unwrap();

    let p1 = AdmitBatchParams {
        workflow_name: "shard_batch_wf".to_string(),
        batch_key: "key-shard1".to_string(),
        workflow_id: "shard-batch-1".to_string(),
        queue_name: "default".to_string(),
        payload: json!({"shard": 1}),
        start_options: DebounceStartOptions::default(),
        max_wait: Duration::from_secs(10),
        max_size: 5,
        shard_id: 1,
    };
    admit_batched_start(&mut conn, p1, None)
        .await
        .unwrap()
        .unwrap();

    diesel::sql_query("UPDATE harvest_event_batches SET fire_at = NOW() - INTERVAL '1 minute'")
        .execute(&mut conn)
        .await
        .unwrap();

    let url = container_url(&container).await;
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(&url));
    pools.insert(ShardId::new(1), build_test_pool(&url));
    let sharded_pool = ShardedDbPool::from_map(pools, ShardId::new(0));

    let fired = fire_due_event_batches(
        &mut conn,
        &Some(sharded_pool),
        &[ShardId::new(0), ShardId::new(1)],
        &NoOpMetrics,
    )
    .await
    .unwrap();

    assert_eq!(fired, 2, "each assigned shard's own due row must fire");

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    for wf_id in ["shard-batch-0", "shard-batch-1"] {
        let n = diesel::sql_query(
            "SELECT COUNT(*) AS n FROM harvest_workflow_executions WHERE workflow_id = $1",
        )
        .bind::<diesel::sql_types::Text, _>(wf_id)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count query")
        .n;
        assert_eq!(
            n, 1,
            "{wf_id} must have been started by its own shard's fire"
        );
    }
}
