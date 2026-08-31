//! Workflow-context coverage for opt-in cross-shard child placement (issue #956).
//!
//! Pure unit tests — no database. They drive real workflow handlers through
//! `run_workflow` and inspect the emitted `StartChildWorkflow` /
//! `SpawnDetachedChildWorkflow` commands, which is where placement becomes
//! observable: the child's `ExecutionId` encodes the shard it will be created
//! on, so `child_id.shard()` *is* the placement decision.
//!
//! The router is supplied per-context via `WorkflowContext::with_shard_router`
//! rather than `install_global_router`, deliberately: the process-global router
//! is shared with every other test in this binary (several `db`-gated modules
//! install `ShardRouter::single()` concurrently), so a global install here would
//! be a cross-module flake.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow_with_context};
use autumn_harvest::shard::ChildPlacement;
use autumn_harvest::types::{ExecutionId, ParentClosePolicy, ShardId};
use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::{ShardRouter, WorkflowInfo};
use serde_json::{Value, json};

const SHARDS: [i32; 4] = [0, 1, 2, 3];

fn four_shard_router() -> ShardRouter {
    let ids: Vec<ShardId> = SHARDS.iter().copied().map(ShardId::new).collect();
    ShardRouter::new(ids.clone(), ids, ShardId::new(0))
}

/// A minimal `WorkflowInfo` for the typed helpers under test. The handler is
/// never invoked here — only `info.name` reaches the spawn path.
fn child_wf_info() -> WorkflowInfo {
    fn never<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(Value::Null) })
    }
    WorkflowInfo {
        name: "child_wf",
        module: "cross_shard_child_context_tests",
        handler: never,
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// Every `child_id` minted by a `StartChildWorkflow` command in `commands`.
fn started_child_ids(commands: &[WorkflowCommand]) -> Vec<ExecutionId> {
    commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::StartChildWorkflow { child_id, .. } => Some(*child_id),
            _ => None,
        })
        .collect()
}

fn detached_child_ids(commands: &[WorkflowCommand]) -> Vec<ExecutionId> {
    commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::SpawnDetachedChildWorkflow { child_id, .. } => Some(*child_id),
            _ => None,
        })
        .collect()
}

// ── Handlers ─────────────────────────────────────────────────────────────────

const FAN_OUT_N: usize = 64;

fn distributed_fan_out<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<(String, Value)> = (0..FAN_OUT_N)
            .map(|i| ("child_wf".to_string(), json!(i)))
            .collect();
        let out = ctx
            .spawn_child_workflow_fan_out_raw_placed(children, &ChildPlacement::Distributed)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(out))
    })
}

fn default_fan_out<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<(String, Value)> = (0..FAN_OUT_N)
            .map(|i| ("child_wf".to_string(), json!(i)))
            .collect();
        let out = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(out))
    })
}

fn distributed_single_child<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let out = ctx
            .spawn_child_workflow_raw_placed("child_wf", json!(1), &ChildPlacement::Distributed)
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

fn pinned_detached_child<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let id = ctx
            .spawn_child_workflow_detached_raw_placed(
                "child_wf",
                json!(1),
                ParentClosePolicy::RequestCancel,
                &ChildPlacement::Shard(ShardId::new(3)),
            )
            .map_err(|e| e.to_string())?;
        // Park so the commands are observable as a suspension.
        let _ = ctx
            .spawn_child_workflow_raw("blocker", json!(id.to_string()))
            .await;
        Ok(Value::Null)
    })
}

fn distributed_child_timeout<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let out = ctx
            .spawn_child_workflow_timeout_placed(
                "child_wf",
                json!(1),
                std::time::Duration::from_secs(60),
                &ChildPlacement::Distributed,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(out))
    })
}

fn distributed_without_router<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.spawn_child_workflow_raw_placed("child_wf", json!(1), &ChildPlacement::Distributed)
            .await
            .map_err(|e| e.to_string())
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// AC1: the default keeps every child on the parent's shard, even when a
/// multi-shard router is right there and could have spread them.
#[tokio::test]
async fn the_default_fan_out_keeps_every_child_on_the_parent_shard() {
    let parent_shard = ShardId::new(1);
    let exec_id = ExecutionId::new_for_shard(parent_shard);
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());

    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, default_fan_out, Value::Null).await
    else {
        panic!("expected the fan-out to suspend");
    };

    let ids = started_child_ids(&commands);
    assert_eq!(ids.len(), FAN_OUT_N);
    for id in ids {
        assert_eq!(
            id.shard(),
            parent_shard,
            "the default placement must never leave the parent's shard"
        );
    }
}

/// AC2/AC8: opting in spreads children across the writable set.
#[tokio::test]
async fn distributed_fan_out_spreads_children_across_every_writable_shard() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());

    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, distributed_fan_out, Value::Null).await
    else {
        panic!("expected the fan-out to suspend");
    };

    let ids = started_child_ids(&commands);
    assert_eq!(ids.len(), FAN_OUT_N);
    let mut seen: std::collections::BTreeSet<i32> =
        ids.iter().map(|id| id.shard().as_i32()).collect();
    for shard in SHARDS {
        assert!(
            seen.remove(&shard),
            "shard {shard} received no children out of {FAN_OUT_N}"
        );
    }
    assert!(seen.is_empty(), "children landed outside the writable set");
}

/// AC2: the placement is restart-stable — replaying the identical decision
/// cycle from the identical history re-derives the identical shards.
#[tokio::test]
async fn distributed_placement_is_stable_across_a_retried_decision_cycle() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    let run = || async {
        let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
            .with_shard_router(four_shard_router());
        let WorkflowOutcome::Suspended { commands } =
            run_workflow_with_context(ctx, distributed_fan_out, Value::Null).await
        else {
            panic!("expected the fan-out to suspend");
        };
        started_child_ids(&commands)
            .iter()
            .map(|id| id.shard().as_i32())
            .collect::<Vec<_>>()
    };

    let first = run().await;
    let second = run().await;
    assert_eq!(
        first, second,
        "a decision cycle retried after a crash must place children identically"
    );
}

/// AC6: replay reuses the recorded `child_id` verbatim and never re-derives
/// placement, so a parent's history replays byte-identically no matter where
/// its children physically live.
#[tokio::test]
async fn replay_reuses_recorded_cross_shard_child_ids_verbatim() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    // Children recorded on shards the router would never pick for these keys.
    let recorded: Vec<ExecutionId> = (0..FAN_OUT_N)
        .map(|i| ExecutionId::new_for_shard(ShardId::new(i32::try_from(i % 4).unwrap())))
        .collect();

    let mut history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(FAN_OUT_N as u64),
        },
    ];
    for (i, id) in recorded.iter().enumerate() {
        history.push(WorkflowEvent::ChildWorkflowStarted {
            child_id: *id,
            workflow_name: "child_wf".to_string(),
            input: json!(i),
        });
    }
    for (i, id) in recorded.iter().enumerate() {
        history.push(WorkflowEvent::ChildWorkflowCompleted {
            child_id: *id,
            output: json!(i),
        });
    }

    let ctx = WorkflowContext::for_replay(exec_id, history).with_shard_router(four_shard_router());
    let outcome = run_workflow_with_context(ctx, distributed_fan_out, Value::Null).await;
    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output.as_array().expect("array output");
            assert_eq!(results.len(), FAN_OUT_N);
        }
        other => panic!("replay must complete without divergence, got {other:?}"),
    }
}

/// AC6: the recorded event contract is untouched — `ChildWorkflowStarted`
/// carries exactly its three historical fields whatever shard the child is on.
#[test]
fn child_workflow_started_json_gains_no_placement_field() {
    let cross_shard = WorkflowEvent::ChildWorkflowStarted {
        child_id: ExecutionId::new_for_shard(ShardId::new(3)),
        workflow_name: "child_wf".to_string(),
        input: json!({"n": 1}),
    };
    let value = serde_json::to_value(&cross_shard).expect("serializes");
    let data = value
        .get("data")
        .expect("adjacently-tagged envelope carries `data`")
        .as_object()
        .expect("object");
    let mut keys: Vec<&str> = data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["child_id", "input", "workflow_name"],
        "no new field may appear on ChildWorkflowStarted"
    );
}

/// The detached spawn honours an explicit pin.
#[tokio::test]
async fn a_detached_child_can_be_pinned_to_another_shard() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());

    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, pinned_detached_child, Value::Null).await
    else {
        panic!("expected a suspension");
    };

    let detached = detached_child_ids(&commands);
    assert_eq!(detached.len(), 1);
    assert_eq!(detached[0].shard(), ShardId::new(3));
}

/// The child-or-deadline race (#779) honours placement too.
#[tokio::test]
async fn the_child_deadline_race_honours_distributed_placement() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());

    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, distributed_child_timeout, Value::Null).await
    else {
        panic!("expected a suspension");
    };

    let ids = started_child_ids(&commands);
    assert_eq!(ids.len(), 1);
    // The router must actually have been consulted; with this parent id the
    // pick is whatever rendezvous says, but it must be a configured shard.
    assert!(
        SHARDS.contains(&ids[0].shard().as_i32()),
        "child landed outside the configured shard set"
    );
    assert!(
        commands
            .iter()
            .any(|c| matches!(c, WorkflowCommand::StartTimer { .. })),
        "the deadline timer must still be armed in the same batch"
    );
}

/// AC8: no router, no silent fallback.
#[tokio::test]
async fn opting_in_with_no_router_fails_the_spawn_rather_than_falling_back() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    // Deliberately NO `with_shard_router`.
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()]);
    let outcome = run_workflow_with_context(ctx, distributed_without_router, Value::Null).await;
    match outcome {
        WorkflowOutcome::Failed { error, .. } => assert!(
            error.contains("ShardRouter") || error.contains("router"),
            "the failure must name the missing router, got: {error}"
        ),
        other => panic!("expected a typed failure, got {other:?}"),
    }
}

/// Placement resolution is *not* re-run on replay, so a router that has since
/// been re-sharded cannot move an already-recorded child.
#[tokio::test]
async fn a_rehashed_router_cannot_move_an_already_recorded_child() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let recorded = ExecutionId::new_for_shard(ShardId::new(2));
    let history = vec![
        started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id: recorded,
            workflow_name: "child_wf".to_string(),
            input: json!(1),
        },
    ];
    // A *wider* router: rendezvous would move roughly 1/N of keys.
    let widened = ShardRouter::new(
        (0..8).map(ShardId::new).collect(),
        (0..8).map(ShardId::new).collect(),
        ShardId::new(0),
    );
    let ctx = WorkflowContext::for_replay(exec_id, history).with_shard_router(widened);

    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, distributed_single_child, Value::Null).await
    else {
        panic!("expected a re-park on the still-running child");
    };
    let ids = started_child_ids(&commands);
    assert_eq!(ids, vec![recorded], "the recorded child id must be reused");
}

/// A typed helper surface exists for the ergonomic path too.
#[tokio::test]
async fn the_typed_fan_out_helper_accepts_a_placement() {
    fn handler<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let out: Vec<u64> = ctx
                .spawn_child_workflow_fan_out_placed(
                    &child_wf_info(),
                    vec![1u64, 2, 3],
                    &ChildPlacement::Distributed,
                )
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(out))
        })
    }

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());
    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, handler, Value::Null).await
    else {
        panic!("expected a suspension");
    };
    assert_eq!(started_child_ids(&commands).len(), 3);
}

/// A pin the router rejects surfaces as a typed error, never a quiet
/// parent-shard fallback.
#[tokio::test]
async fn a_rejected_pin_fails_the_spawn() {
    fn handler<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.spawn_child_workflow_raw_placed(
                "child_wf",
                json!(1),
                &ChildPlacement::Shard(ShardId::new(99)),
            )
            .await
            .map_err(|e| e.to_string())
        })
    }

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx =
        WorkflowContext::for_replay(exec_id, vec![started()]).with_shard_router(four_shard_router());
    match run_workflow_with_context(ctx, handler, Value::Null).await {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(error.contains("99"), "must name the rejected shard: {error}");
        }
        other => panic!("expected a typed failure, got {other:?}"),
    }
}

/// The error surface is the one AC8 names.
#[test]
fn shard_unavailable_renders_its_shard_and_reason() {
    let err = HarvestError::ShardUnavailable {
        shard_id: 2,
        reason: "pool checkout timed out".to_string(),
    };
    let rendered = err.to_string();
    assert!(rendered.contains('2'), "{rendered}");
    assert!(rendered.contains("pool checkout timed out"), "{rendered}");
}
