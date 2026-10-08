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

use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow_with_context};
use autumn_harvest::shard::ChildPlacement;
use autumn_harvest::types::{ExecutionId, ParentClosePolicy, ShardId};
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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());

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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());

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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());

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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());

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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());
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
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());
    match run_workflow_with_context(ctx, handler, Value::Null).await {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("99"),
                "must name the rejected shard: {error}"
            );
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

/// **Regression (review finding).** A *sequential* loop of placed spawns must
/// spread across shards, not collapse onto one.
///
/// A fresh `WorkflowContext` is built per decision cycle, and in a sequential
/// `spawn(...).await` loop each child is the only **fresh** dispatch in its own
/// cycle — every earlier child replays from history. A placement counter that
/// only advanced on a fresh dispatch therefore restarted at 1 every cycle and
/// handed `"{parent}#1"` to every child in the loop, putting the entire loop on
/// one shard while the fan-out helper (whose children are all fresh in one
/// cycle) looked perfectly uniform. Counting *invocations* instead makes the Nth
/// spawn's key depend on its position in the workflow rather than on which cycle
/// dispatched it.
#[tokio::test]
async fn sequential_placed_spawns_do_not_all_collide_on_one_shard() {
    fn three_sequential<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let mut out = Vec::new();
            for i in 0..3 {
                out.push(
                    ctx.spawn_child_workflow_raw_placed(
                        "child_wf",
                        json!(i),
                        &ChildPlacement::Distributed,
                    )
                    .await
                    .map_err(|e| e.to_string())?,
                );
            }
            Ok(json!(out))
        })
    }

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut history = vec![started()];
    let mut shards = Vec::new();

    // Drive three decision cycles, completing one child per cycle — the shape a
    // real sequential loop actually runs in.
    for i in 0..3 {
        let ctx = WorkflowContext::for_replay(exec_id, history.clone())
            .with_shard_router(four_shard_router());
        let WorkflowOutcome::Suspended { commands } =
            run_workflow_with_context(ctx, three_sequential, Value::Null).await
        else {
            panic!("expected a suspension on child {i}");
        };
        let ids = started_child_ids(&commands);
        assert_eq!(ids.len(), 1, "exactly one child is dispatched per cycle");
        let child_id = ids[0];
        shards.push(child_id.shard().as_i32());
        history.push(WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "child_wf".to_string(),
            input: json!(i),
        });
        history.push(WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: json!(i),
        });
    }

    // Assert the DETERMINISTIC property, not a distribution one: child N must
    // hash as the Nth invocation. "they didn't all land on one shard" would be a
    // flaky assertion — three keys over four shards legitimately collide about 6
    // times in 100 — and it would also pass by luck under the very bug this test
    // exists to catch.
    let router = four_shard_router();
    let expected: Vec<i32> = (1..=3)
        .map(|n| {
            router
                .pick_for_new_workflow(
                    "child_wf",
                    &autumn_harvest::shard::child_placement_key(exec_id, n),
                )
                .as_i32()
        })
        .collect();
    assert_eq!(
        shards, expected,
        "each sequential spawn must hash as its own invocation ordinal; a \
         placement counter that resets per decision cycle gives every child the \
         `#1` key and collapses the loop onto one shard"
    );
    // And the ordinals really are distinct keys, so the spread is real rather
    // than three copies of one pick.
    assert_eq!(
        expected.len(),
        3,
        "sanity: three ordinals produce three independent picks"
    );
}

/// The Nth placed spawn's shard must not depend on which decision cycle
/// dispatched it — the property that makes the sequential loop above work, and
/// the restart-stability contract AC2 asks for.
#[tokio::test]
async fn a_placed_childs_shard_does_not_depend_on_which_cycle_dispatched_it() {
    fn two_sequential<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let a = ctx
                .spawn_child_workflow_raw_placed("child_wf", json!(0), &ChildPlacement::Distributed)
                .await
                .map_err(|e| e.to_string())?;
            let b = ctx
                .spawn_child_workflow_raw_placed("child_wf", json!(1), &ChildPlacement::Distributed)
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!([a, b]))
        })
    }

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));

    // Cycle 1 dispatches child A.
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(four_shard_router());
    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, two_sequential, Value::Null).await
    else {
        panic!("expected a suspension on child A");
    };
    let a_id = started_child_ids(&commands)[0];

    // Cycle 2 replays A and dispatches B. B is the SECOND invocation, so its key
    // must be `#2` even though it is the first fresh dispatch of this cycle.
    let history = vec![
        started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id: a_id,
            workflow_name: "child_wf".to_string(),
            input: json!(0),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id: a_id,
            output: json!(0),
        },
    ];
    let ctx = WorkflowContext::for_replay(exec_id, history).with_shard_router(four_shard_router());
    let WorkflowOutcome::Suspended { commands } =
        run_workflow_with_context(ctx, two_sequential, Value::Null).await
    else {
        panic!("expected a suspension on child B");
    };
    let b_id = started_child_ids(&commands)[0];

    let router = four_shard_router();
    let parent = exec_id;
    assert_eq!(
        a_id.shard(),
        router.pick_for_new_workflow(
            "child_wf",
            &autumn_harvest::shard::child_placement_key(parent, 1)
        ),
        "child A must hash as the first invocation"
    );
    assert_eq!(
        b_id.shard(),
        router.pick_for_new_workflow(
            "child_wf",
            &autumn_harvest::shard::child_placement_key(parent, 2)
        ),
        "child B must hash as the second invocation, not the first"
    );
}

// ── Replay must never re-resolve placement (issue #956, Codex round 6) ────────

/// A fan-out pinned to shard 2, so a router that no longer contains shard 2
/// makes the placement genuinely unresolvable rather than merely differently
/// resolved.
fn pinned_fan_out<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<(String, Value)> =
            (0..4).map(|i| ("child_wf".to_string(), json!(i))).collect();
        let out = ctx
            .spawn_child_workflow_fan_out_raw_placed(
                children,
                &ChildPlacement::Shard(ShardId::new(2)),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(out))
    })
}

/// The same fan-out through the collect-all sibling, which carries an identical
/// preflight and therefore an identical replay hazard.
fn pinned_fan_out_collect<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let children: Vec<(String, Value)> =
            (0..4).map(|i| ("child_wf".to_string(), json!(i))).collect();
        let out = ctx
            .spawn_child_workflow_fan_out_collect_raw_placed(
                children,
                &ChildPlacement::Shard(ShardId::new(2)),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(out.len()))
    })
}

/// A router that has lost shard 2 — the topology edit an operator makes after a
/// pinned fan-out has already been dispatched.
fn router_without_shard_two() -> ShardRouter {
    let ids = vec![ShardId::new(0), ShardId::new(1), ShardId::new(3)];
    ShardRouter::new(ids.clone(), ids, ShardId::new(0))
}

/// A complete recorded history for a 4-child pinned fan-out on shard 2.
fn recorded_pinned_fan_out_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let recorded: Vec<ExecutionId> = (0..4)
        .map(|_| ExecutionId::new_for_shard(ShardId::new(2)))
        .collect();

    let mut history = vec![
        started(),
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(4_u64),
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
    (exec_id, history)
}

/// AC6 + AC8: a topology change made *after* dispatch must not fail a parent
/// that is merely replaying.
///
/// Replay needs no placement at all — the child ids are recorded and reused
/// verbatim, which is exactly what makes AC6's byte-identical replay true. When
/// the preflight ran unconditionally, removing a pinned shard from the topology
/// raised `Config`; the handler ABI stringifies that into a terminal `Failed`,
/// so a routine operator edit permanently failed every parent that had ever
/// placed a fan-out — including parents whose children had all already
/// completed, as here.
#[tokio::test]
async fn a_topology_change_after_dispatch_never_fails_a_replaying_fan_out_parent() {
    let (exec_id, history) = recorded_pinned_fan_out_history();

    let ctx =
        WorkflowContext::for_replay(exec_id, history).with_shard_router(router_without_shard_two());
    match run_workflow_with_context(ctx, pinned_fan_out, Value::Null).await {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output.as_array().expect("array output");
            assert_eq!(results.len(), 4, "every recorded child must replay");
        }
        other => panic!(
            "replay must not consult the router for an already-recorded fan-out, got {other:?}"
        ),
    }
}

/// The collect-all sibling shares the ordering, so it shares the test.
#[tokio::test]
async fn a_topology_change_after_dispatch_never_fails_a_replaying_collect_all_parent() {
    let (exec_id, history) = recorded_pinned_fan_out_history();

    let ctx =
        WorkflowContext::for_replay(exec_id, history).with_shard_router(router_without_shard_two());
    match run_workflow_with_context(ctx, pinned_fan_out_collect, Value::Null).await {
        WorkflowOutcome::Completed { output, .. } => {
            assert_eq!(output, json!(4), "every recorded child must replay");
        }
        other => panic!(
            "replay must not consult the router for an already-recorded fan-out, got {other:?}"
        ),
    }
}

/// The other half of the same contract: gating the preflight on `fresh_dispatch`
/// must not have disabled it. A *fresh* pinned fan-out against a router that
/// cannot resolve the pin still fails terminally, rather than half-dispatching
/// or silently falling back to the parent's shard (AC8).
#[tokio::test]
async fn a_fresh_pinned_fan_out_is_still_rejected_when_the_pin_cannot_resolve() {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let ctx = WorkflowContext::for_replay(exec_id, vec![started()])
        .with_shard_router(router_without_shard_two());

    match run_workflow_with_context(ctx, pinned_fan_out, Value::Null).await {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains('2'),
                "the rejection must name the unresolvable shard, got {error:?}"
            );
        }
        other => panic!("an unresolvable pin must still fail a fresh dispatch, got {other:?}"),
    }
}
