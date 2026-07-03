#![cfg(feature = "db")]

//! DB-backed integration tests for large-payload offloading (issue #524).
//!
//! Verifies, against a real Postgres container:
//! 1. `append_events_offloaded` writes a tiny reference envelope to
//!    `harvest_events` (success metric: row < 4 KB) and records a
//!    `harvest_payload_refs` row, while `load_history_inflated` reconstructs
//!    the original payload byte-for-byte.
//! 2. The GC primitives (`load_payload_refs`, `blob_key_still_referenced`)
//!    drive orphan-safe blob deletion: a blob shared by two executions stays
//!    referenced until the last execution's refs are gone.
//!
//! Requires Docker (testcontainers); skipped automatically where unavailable.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::types::{ActivityExecId, ExecutionId};

use chrono::Utc;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
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
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #524: offloaded-payload reference table.
    include_str!("../migrations/20260627000001_harvest_payload_refs/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
);

#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemStore {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &'static str {
        "memdb"
    }
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        // Content-addressed so identical bytes share a key (carry-forward).
        let key = format!("k{}", bytes.len());
        self.blobs
            .lock()
            .unwrap()
            .insert(key.clone(), bytes.to_vec());
        Box::pin(async move { Ok(key) })
    }
    fn get(&self, key: &str) -> PayloadStoreFuture<'_, Vec<u8>> {
        let found = self.blobs.lock().unwrap().get(key).cloned();
        let key = key.to_string();
        Box::pin(async move { found.ok_or_else(|| PayloadStoreError(format!("missing {key}"))) })
    }
    fn delete(&self, key: &str) -> PayloadStoreFuture<'_, ()> {
        self.blobs.lock().unwrap().remove(key);
        Box::pin(async move { Ok(()) })
    }
}

async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");
    (conn, container)
}

async fn insert_execution(conn: &mut AsyncPgConnection, name: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: name,
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
        context_headers: None,
        sla: None,
        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

fn offloader(store: Arc<MemStore>) -> PayloadOffloader {
    PayloadOffloader::new(store, 256 * 1024, Arc::new(NoOpMetrics))
}

fn big_output() -> serde_json::Value {
    serde_json::json!({ "blob": "Q".repeat(2_000_000) })
}

#[tokio::test]
async fn offload_round_trips_through_postgres_with_tiny_event_row() {
    let (mut conn, _c) = setup_test_db().await;
    let exec_id = insert_execution(&mut conn, "offload_wf").await;
    let store = MemStore::new();
    let off = offloader(store.clone());

    let activity_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "produce".into(),
            input: serde_json::json!({}),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: big_output(),
        },
    ];

    store::append_events_offloaded(&mut conn, exec_id, &events, 0, Some(&off))
        .await
        .expect("append offloaded");

    // The stored ActivityCompleted row must be tiny (the envelope), not 2 MB.
    let stored: Vec<serde_json::Value> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(harvest_events::event_data)
        .load(&mut conn)
        .await
        .expect("load raw rows");
    let completed_row_bytes = serde_json::to_vec(&stored[2]).unwrap().len();
    assert!(
        completed_row_bytes < 4096,
        "offloaded event row must stay small, was {completed_row_bytes} bytes"
    );
    assert_eq!(stored[2]["data"]["output"]["_harvest_offload_envelope"], 1);

    // A payload_refs row was recorded for the blob.
    let refs = store::load_payload_refs(&mut conn, exec_id)
        .await
        .expect("load refs");
    assert_eq!(refs.len(), 1, "one blob reference recorded");

    // Inflated load reconstructs the exact original output.
    let history =
        store::load_history_inflated(&mut conn, exec_id, &PayloadCodecs::default(), Some(&off))
            .await
            .expect("inflated load");
    match &history.events[2] {
        WorkflowEvent::ActivityCompleted { output, .. } => {
            assert_eq!(*output, big_output(), "100% byte fidelity through Postgres");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn shared_blob_stays_referenced_until_last_execution_gone() {
    let (mut conn, _c) = setup_test_db().await;
    let store = MemStore::new();
    let off = offloader(store.clone());

    // Two executions offload identical bytes -> same content-addressed key.
    let exec_a = insert_execution(&mut conn, "gc_a").await;
    let exec_b = insert_execution(&mut conn, "gc_b").await;
    let payload = serde_json::json!("R".repeat(1_000_000));
    let mk = |id: ActivityExecId| {
        vec![WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: payload.clone(),
        }]
    };
    store::append_events_offloaded(&mut conn, exec_a, &mk(ActivityExecId::new()), 0, Some(&off))
        .await
        .unwrap();
    store::append_events_offloaded(&mut conn, exec_b, &mk(ActivityExecId::new()), 0, Some(&off))
        .await
        .unwrap();

    let key = store::load_payload_refs(&mut conn, exec_a).await.unwrap()[0]
        .blob_key
        .clone();
    assert_eq!(
        store::load_payload_refs(&mut conn, exec_b).await.unwrap()[0].blob_key,
        key,
        "identical bytes share a content-addressed key"
    );

    // Delete exec_a's row (cascade removes its refs). The blob is still
    // referenced by exec_b, so the GC residual check must keep it.
    diesel::delete(harvest_workflow_executions::table.find(exec_a.as_uuid()))
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        store::blob_key_still_referenced(&mut conn, &key)
            .await
            .unwrap(),
        "blob must survive while exec_b still references it"
    );

    // Delete exec_b too — now no refs remain, so GC may delete the blob.
    diesel::delete(harvest_workflow_executions::table.find(exec_b.as_uuid()))
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        !store::blob_key_still_referenced(&mut conn, &key)
            .await
            .unwrap(),
        "blob is unreferenced once the last execution is gone"
    );
}
