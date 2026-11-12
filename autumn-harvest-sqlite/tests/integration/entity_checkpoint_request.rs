//! A checkpoint that the state of an entity asks for.
//!
//! `Entity::run_with_wakes_and_checkpoints` calls `take` on the state after
//! each op. A `true` ends the run at a checkpoint after that op. The ops that
//! wait move to the next run. Both runs replay on the core replayer.

use autumn_harvest::WorkflowInfo;
use autumn_harvest::entity::{
    ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY, ENTITY_STATS_QUERY, Entity, EntityMessage,
};
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{ExecutionId, WorkflowIdReusePolicy};
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionSummary, RunState, SignalWithStart, SqliteRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// The state: each applied op, and a request that no snapshot holds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Log {
    ops: Vec<String>,
    #[serde(skip)]
    cut: bool,
}

/// An entity whose op `cut` asks for a checkpoint after it.
#[workflow]
async fn cut_log(ctx: &WorkflowContext, input: serde_json::Value) -> Result<Log, String> {
    Entity::<Log>::from_input(ctx, input)
        .run_with_wakes_and_checkpoints(
            |mut log: Log, op: String| async move {
                log.cut = op == "cut";
                log.ops.push(op);
                Ok(log)
            },
            |_: &Log| None,
            |log: &mut Log| std::mem::take(&mut log.cut),
        )
        .await
        .map_err(|err| err.to_string())
}

/// The state of an entity that counts its ops and keeps no op text.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Count {
    applied: u64,
    #[serde(skip)]
    cut: bool,
}

/// An entity whose op `cut` asks for a checkpoint. Its state stays small,
/// so only the waiting ops can pass the checkpoint input cap.
#[workflow]
async fn cut_count(ctx: &WorkflowContext, input: serde_json::Value) -> Result<Count, String> {
    Entity::<Count>::from_input(ctx, input)
        .run_with_wakes_and_checkpoints(
            |mut count: Count, op: String| async move {
                count.cut = op == "cut";
                count.applied += 1;
                Ok(count)
            },
            |_: &Count| None,
            |count: &mut Count| std::mem::take(&mut count.cut),
        )
        .await
        .map_err(|err| err.to_string())
}

const KEY: &str = "log-1";

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&cut_log_info());
    rt.register_workflow(&cut_count_info());
    rt
}

fn send(rt: &mut SqliteRuntime, op: &str) {
    stage(rt, EntityMessage::op(op.to_owned()));
}

fn stage(rt: &mut SqliteRuntime, message: EntityMessage<String>) {
    stage_to(rt, "cut_log", message);
}

fn stage_to(rt: &mut SqliteRuntime, workflow_name: &str, message: EntityMessage<String>) {
    rt.signal_with_start(SignalWithStart {
        workflow_name,
        workflow_id: KEY,
        input: json!({}),
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        signal_name: ENTITY_OP_SIGNAL,
        signal_payload: serde_json::to_value(message).unwrap(),
        idempotency_key: None,
    })
    .unwrap();
}

/// Every run of the key, oldest first.
fn runs(rt: &SqliteRuntime) -> Vec<ExecutionId> {
    let filter = ExecutionFilter::new().workflow_id(KEY);
    let mut runs: Vec<_> = rt
        .list_executions(&filter)
        .unwrap()
        .executions
        .iter()
        .filter_map(ExecutionSummary::execution_id)
        .collect();
    runs.reverse();
    runs
}

/// Drive the live run of the key, and each run after it, until it blocks.
async fn drive(rt: &mut SqliteRuntime) -> RunState {
    let mut exec = *runs(rt).last().unwrap();
    loop {
        match rt.run_until_blocked(exec).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return other,
        }
    }
}

/// End the entity, then replay each of its runs. A strict replay needs a
/// whole history.
async fn assert_replays(rt: &mut SqliteRuntime, info: &WorkflowInfo) {
    stage_to(rt, info.name, EntityMessage::delete());
    let ended = drive(rt).await;
    assert!(matches!(ended, RunState::Completed(_)), "{ended:?}");
    for exec in runs(rt) {
        let report = WorkflowReplayer::new()
            .register_fn(info.name, info.handler)
            .with_workflow_id(KEY)
            .replay_from_events(rt.load_history(exec).unwrap())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "the run {exec} replays:\n{report}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_ends_the_run_after_its_op_and_carries_the_rest() {
    let mut rt = runtime();
    for op in ["a", "cut", "b"] {
        send(&mut rt, op);
    }
    let blocked = drive(&mut rt).await;
    assert!(matches!(blocked, RunState::WaitingSignal(_)), "{blocked:?}");
    let runs = runs(&rt);
    assert_eq!(runs.len(), 2, "one checkpoint, after the op `cut`");
    let state = rt
        .query(runs[1], ENTITY_STATE_QUERY, json!(null))
        .await
        .unwrap();
    assert_eq!(state["ops"], json!(["a", "cut", "b"]));
    let stats = rt
        .query(runs[1], ENTITY_STATS_QUERY, json!(null))
        .await
        .unwrap();
    assert_eq!(stats["checkpoints"], json!(1));
    assert_replays(&mut rt, &cut_log_info()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_request_keeps_one_run() {
    let mut rt = runtime();
    for op in ["a", "b", "c"] {
        send(&mut rt, op);
    }
    drive(&mut rt).await;
    assert_eq!(runs(&rt).len(), 1);
    assert_replays(&mut rt, &cut_log_info()).await;
}

/// A request stays until the checkpoint. Here the waiting ops do not fit
/// the checkpoint input at once, so the loop runs one of them first. The
/// checkpoint still follows.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_waits_for_the_waiting_ops_to_fit() {
    let mut rt = runtime();
    // Nine ops of 250 kB pass the 2 MiB input cap. Each one fits the 256 KiB
    // signal cap.
    let big = "x".repeat(250_000);
    stage_to(&mut rt, "cut_count", EntityMessage::op("cut".to_owned()));
    for _ in 0..9 {
        stage_to(&mut rt, "cut_count", EntityMessage::op(big.clone()));
    }
    let blocked = drive(&mut rt).await;
    assert!(matches!(blocked, RunState::WaitingSignal(_)), "{blocked:?}");
    let runs = runs(&rt);
    assert_eq!(runs.len(), 2, "the request still ends the first run");
    let state = rt
        .query(runs[1], ENTITY_STATE_QUERY, json!(null))
        .await
        .unwrap();
    assert_eq!(state["applied"], json!(10));
    assert_replays(&mut rt, &cut_count_info()).await;
}
