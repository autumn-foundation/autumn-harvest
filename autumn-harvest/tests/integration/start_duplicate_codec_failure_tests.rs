#![cfg(feature = "db")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! A duplicate start must not depend on the active codec (issue #2054).
//!
//! `start_workflow` encodes `input` and `memo` for the row it may insert.
//! A duplicate start inserts nothing, so a failing encoder must not change
//! its result. A start that writes a new run still reports the codec error.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use autumn_harvest::error::HarvestError;
use autumn_harvest::execution::{
    StartedWorkflowExecution, start_or_load_workflow_execution_with_codecs,
};
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::types::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy};
use autumn_harvest::{ExecutionId, ShardId, StartWorkflowParams};

use crate::integration_e2e::setup_test_database_url_or_env;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use uuid::Uuid;

/// A codec whose `encode` fails while the shared flag is set.
#[derive(Debug)]
struct SwitchCodec(Arc<AtomicBool>);

impl PayloadCodec for SwitchCodec {
    fn codec_id(&self) -> &'static str {
        "switch"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        if AtomicBool::load(&self.0, Ordering::SeqCst) {
            return Err(CodecError("simulated key outage".to_string()));
        }
        Ok(raw.to_vec())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.to_vec())
    }
}

/// Codecs that encode the columns and fail on demand.
fn switch_codecs() -> (PayloadCodecs, Arc<AtomicBool>) {
    let broken = Arc::new(AtomicBool::new(false));
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(SwitchCodec(Arc::clone(&broken))));
    codecs.set_column_encoding(true);
    (codecs, broken)
}

async fn connect() -> (AsyncPgConnection, Option<impl Sized>) {
    let (url, container) = setup_test_database_url_or_env().await;
    let conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect failed");
    (conn, container)
}

async fn start(
    conn: &mut AsyncPgConnection,
    codecs: &PayloadCodecs,
    workflow_id: &str,
    reuse: WorkflowIdReusePolicy,
    conflict: WorkflowIdConflictPolicy,
) -> Result<StartedWorkflowExecution, HarvestError> {
    let params = StartWorkflowParams {
        memo: Some(json!({"account": "memo-secret"})),
        reuse_policy: reuse,
        conflict_policy: conflict,
        ..StartWorkflowParams::new(
            "dup_codec_wf",
            workflow_id,
            ExecutionId::new_for_shard(ShardId::new(0)),
            json!({"ssn": "input-secret"}),
            "dup_codec_q",
        )
    };
    start_or_load_workflow_execution_with_codecs(conn, params, None, codecs).await
}

async fn set_state(conn: &mut AsyncPgConnection, exec_id: Uuid, state: &str) {
    use autumn_harvest::schema::harvest_workflow_executions as t;
    diesel::update(t::table.find(exec_id))
        .set(t::state.eq(state))
        .execute(conn)
        .await
        .expect("set state");
}

async fn run_count(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
    use autumn_harvest::schema::harvest_workflow_executions as t;
    t::table
        .filter(t::workflow_id.eq(workflow_id))
        .count()
        .get_result(conn)
        .await
        .expect("count")
}

async fn state_of(conn: &mut AsyncPgConnection, exec_id: Uuid) -> String {
    use autumn_harvest::schema::harvest_workflow_executions as t;
    t::table
        .find(exec_id)
        .select(t::state)
        .get_result(conn)
        .await
        .expect("state")
}

/// Starts one run with a working codec, then breaks the codec.
async fn seeded(
    conn: &mut AsyncPgConnection,
    state: &str,
) -> (PayloadCodecs, String, StartedWorkflowExecution) {
    let (codecs, broken) = switch_codecs();
    let workflow_id = Uuid::new_v4().to_string();
    let first = start(
        conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect("seed start");
    if state != "RUNNING" {
        set_state(conn, first.exec_id.as_uuid(), state).await;
    }
    broken.store(true, Ordering::SeqCst);
    (codecs, workflow_id, first)
}

#[tokio::test]
async fn use_existing_attaches_while_the_codec_fails() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, "RUNNING").await;

    let again = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdConflictPolicy::UseExisting,
    )
    .await
    .expect("a duplicate start must attach, not fail on the codec");

    assert_eq!(again.exec_id, first.exec_id);
    assert!(!again.created);
}

#[tokio::test]
async fn failed_only_attaches_to_a_completed_run_while_the_codec_fails() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, "COMPLETED").await;

    let again = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect("a duplicate start must attach, not fail on the codec");

    assert_eq!(again.exec_id, first.exec_id);
    assert!(!again.created);
}

#[tokio::test]
async fn failed_only_attaches_to_a_running_run_while_the_codec_fails() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, "RUNNING").await;

    let again = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect("a duplicate start must attach, not fail on the codec");

    assert_eq!(again.exec_id, first.exec_id);
    assert!(!again.created);
}

#[tokio::test]
async fn reject_duplicate_reports_already_exists_while_the_codec_fails() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, "RUNNING").await;

    let err = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::RejectDuplicate,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect_err("a duplicate start must be refused");

    match err {
        HarvestError::AlreadyExists {
            existing_exec_id, ..
        } => assert_eq!(existing_exec_id, first.exec_id),
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

#[tokio::test]
async fn reject_duplicate_reports_already_exists_for_a_terminal_run_while_the_codec_fails() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, _first) = seeded(&mut conn, "COMPLETED").await;

    let err = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::RejectDuplicate,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect_err("a duplicate start must be refused");

    assert!(matches!(err, HarvestError::AlreadyExists { .. }), "{err:?}");
}

#[tokio::test]
async fn a_fresh_start_still_reports_the_codec_error_and_stores_nothing() {
    let (mut conn, _db) = connect().await;
    let (codecs, broken) = switch_codecs();
    broken.store(true, Ordering::SeqCst);
    let workflow_id = Uuid::new_v4().to_string();

    let err = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect_err("a new run needs the codec");

    assert!(err.is_codec_failure(), "{err:?}");
    assert_eq!(run_count(&mut conn, &workflow_id).await, 0);
}

#[tokio::test]
async fn a_replacing_start_still_reports_the_codec_error_and_keeps_the_prior_run() {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, "FAILED").await;

    let err = start(
        &mut conn,
        &codecs,
        &workflow_id,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await
    .expect_err("a replacement needs the codec");

    assert!(err.is_codec_failure(), "{err:?}");
    assert_eq!(run_count(&mut conn, &workflow_id).await, 1);
    let state = state_of(&mut conn, first.exec_id.as_uuid()).await;
    assert_eq!(state, "FAILED", "the failed replace must roll back");
}

/// A terminating start needs a new run, so it needs the codec. The prior
/// run must stay as it was, not cancelled with no successor.
async fn assert_terminate_refused(
    prior_state: &str,
    reuse: WorkflowIdReusePolicy,
    conflict: WorkflowIdConflictPolicy,
) {
    let (mut conn, _db) = connect().await;
    let (codecs, workflow_id, first) = seeded(&mut conn, prior_state).await;

    let err = start(&mut conn, &codecs, &workflow_id, reuse, conflict)
        .await
        .expect_err("a terminating start needs the codec");

    assert!(err.is_codec_failure(), "{err:?}");
    assert_eq!(run_count(&mut conn, &workflow_id).await, 1);
    assert_eq!(
        state_of(&mut conn, first.exec_id.as_uuid()).await,
        prior_state
    );
}

#[tokio::test]
async fn terminate_if_running_keeps_a_running_prior_while_the_codec_fails() {
    assert_terminate_refused(
        "RUNNING",
        WorkflowIdReusePolicy::TerminateIfRunning,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await;
}

#[tokio::test]
async fn terminate_if_running_keeps_a_completed_prior_while_the_codec_fails() {
    assert_terminate_refused(
        "COMPLETED",
        WorkflowIdReusePolicy::TerminateIfRunning,
        WorkflowIdConflictPolicy::Unspecified,
    )
    .await;
}

#[tokio::test]
async fn terminate_existing_keeps_a_running_prior_while_the_codec_fails() {
    assert_terminate_refused(
        "RUNNING",
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdConflictPolicy::TerminateExisting,
    )
    .await;
}
