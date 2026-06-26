//! Replay-fidelity test for large-payload offloading (issue #524).
//!
//! Demonstrates that a recorded history whose payloads were offloaded to an
//! external [`PayloadStore`] replays with 100% byte fidelity once a
//! [`PayloadOffloader`] is attached to the [`WorkflowReplayer`]: the stored
//! event stays tiny (a reference envelope) while replay reconstructs the exact
//! original bytes by fetching from the store.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_store::{
    PayloadOffloader, PayloadStore, PayloadStoreError, PayloadStoreFuture,
};
use autumn_harvest::telemetry::NoOpMetrics;
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

/// In-memory content-addressed payload store for the test.
#[derive(Default)]
struct MemStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemStore {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

fn key_for(bytes: &[u8]) -> String {
    // A simple stable key; the store is content-addressed by construction.
    format!("blob/{}", bytes.len())
}

impl PayloadStore for MemStore {
    fn store_id(&self) -> &str {
        "memtest"
    }
    fn put(&self, bytes: &[u8]) -> PayloadStoreFuture<'_, String> {
        let key = key_for(bytes);
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

/// The large payload an upstream activity produces — what we expect replay to
/// reconstruct exactly.
fn big_value() -> Value {
    serde_json::json!({
        "document": "X".repeat(2_000_000),
        "vector": (0..1000).collect::<Vec<i64>>(),
    })
}

/// Workflow that runs one activity and asserts its output is the exact large
/// payload — a mismatch (e.g. an un-inflated envelope) makes replay FAIL.
fn fidelity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let out = ctx
            .execute_activity_raw("produce_blob", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        if out == big_value() {
            Ok(serde_json::json!("verified"))
        } else {
            Err("activity output did not match the original payload".to_string())
        }
    })
}

/// Serialize → offload → deserialize each event to produce the "stored" history,
/// exactly as the worker persists it.
async fn offload_events(off: &PayloadOffloader, events: &[WorkflowEvent]) -> Vec<WorkflowEvent> {
    let mut out = Vec::with_capacity(events.len());
    for ev in events {
        let mut v = serde_json::to_value(ev).unwrap();
        off.offload_event_value(&mut v).await.unwrap();
        out.push(serde_json::from_value(v).unwrap());
    }
    out
}

#[tokio::test]
async fn offloaded_history_replays_with_full_byte_fidelity() {
    let store = MemStore::new();
    // 256 KiB threshold (the default).
    let off = PayloadOffloader::new(store.clone(), 256 * 1024, Arc::new(NoOpMetrics));

    let exec_id = ExecutionId::new();
    let activity_id = ActivityExecId::new();
    let original_events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "produce_blob".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: big_value(),
        },
    ];

    // Persist-shaped history: payloads offloaded to the store.
    let stored_events = offload_events(&off, &original_events).await;

    // The offloaded ActivityCompleted event must be tiny (success metric: < 4 KB),
    // even though the original output is ~2 MB.
    let stored_completed = serde_json::to_vec(&stored_events[2]).unwrap();
    assert!(
        stored_completed.len() < 4096,
        "offloaded event row must stay small, was {} bytes",
        stored_completed.len()
    );

    // Replay WITHOUT the offloader: the workflow sees the envelope, not the
    // payload, so it fails — proving inflation is what restores fidelity.
    let bare = WorkflowReplayer::new().register_fn("fidelity", fidelity_workflow);
    let bare_report = bare
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "fidelity".into(),
            execution_id: exec_id,
            events: stored_events.clone(),
            context_headers: None,
        })
        .await;
    assert!(
        !matches!(bare_report.status, ReplayStatus::ReplaySucceeded),
        "without the offloader the envelope must NOT match the original payload"
    );

    // Replay WITH the offloader: payloads inflate from the store and the
    // workflow observes byte-identical output.
    let replayer = WorkflowReplayer::new()
        .register_fn("fidelity", fidelity_workflow)
        .with_payload_offloader(Arc::new(off));
    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "fidelity".into(),
            execution_id: exec_id,
            events: stored_events,
            context_headers: None,
        })
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "offloaded payload must replay with 100% fidelity, got: {report}"
    );
}
