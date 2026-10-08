//! Issue #746 — declarative DAG signal/timer gate nodes.
//!
//! **Phase 1 = RED.** These tests reference the not-yet-implemented gate API
//! (`DagBuilder::signal_gate`, `DagBuilder::signal_gate_with_timeout`,
//! `DagTask::signal`, `DagSignalGate`, `GateTimeoutAction`,
//! `HarvestBuilderError::DagSignalGateRequiresUnifiedExecution`). Per this
//! repo's TDD convention a compile error against not-yet-existing API is an
//! acceptable RED — the GREEN implementation lands these symbols.
//!
//! Acceptance-criteria coverage:
//! * AC1/AC2/Gap B — signal payload lands in `__outputs`, addressable by
//!   downstream nodes (`.map` fan-out, conditions); optional timeout with a
//!   per-gate `on_timeout` choice (fail the run vs continue with a null gate
//!   output).
//! * AC5 — deterministic replay via `WorkflowReplayer` over the generated DAG
//!   handler (signal-first and timer-first histories).
//! * AC6 — a classic (non-unified) DAG with a gate is rejected at build time.
//! * Level isolation — a gate occupies its own singleton execution level so its
//!   `WaitForSignal` suspension is never batched with a level's activity
//!   `ScheduleActivity` dispatches. (Originally the worker's homogeneous-batch
//!   requirement; since issue #950 the mixed batch is persistable, but the
//!   singleton-level split is retained so in-flight DAG histories keep replaying
//!   against the command order they recorded.)

#![allow(clippy::unused_async, clippy::missing_const_for_fn)]

use autumn_harvest::dag::{DagBuilder, DagSignalGate, GateTimeoutAction};
use std::time::Duration;

// Distinct fns → distinct activity node names in the pure-builder tests.
fn a() {}
fn b() {}
fn c() {}
fn d() {}

// ── Pure builder-API tests (no macro, no features) ───────────────────────────

/// AC1 surface — `signal_gate` records a `DagSignalGate` on the task with the
/// signal name as the node identity, no timeout, and no activity dispatch.
#[test]
fn signal_gate_stores_gate_metadata_on_task() {
    let mut builder = DagBuilder::new();
    let extract = builder.activity(a);
    let gate = builder.signal_gate("approval").upstream(&extract);
    let _load = builder.activity(b).upstream(&gate);

    let def = builder.build().expect("gate DAG builds");
    let tasks = def.tasks();

    let gate_task = &tasks[gate.index()];
    let signal = gate_task
        .signal
        .as_ref()
        .expect("gate task must carry a DagSignalGate");
    assert_eq!(signal.signal_name, "approval");
    assert_eq!(signal.timeout, None);
    assert_eq!(signal.on_timeout, GateTimeoutAction::FailRun);
    // A gate is not a map/activity dispatch node.
    assert_eq!(gate_task.map_upstream, None);
    // Node identity is the signal name.
    assert_eq!(gate_task.activity_name, "approval");
    // Non-gate tasks carry no signal.
    assert!(tasks[extract.index()].signal.is_none());
}

/// AC2 surface — `signal_gate_with_timeout` stores the timeout and the author's
/// `on_timeout` action.
#[test]
fn signal_gate_with_timeout_stores_timeout_and_action() {
    let mut builder = DagBuilder::new();
    let extract = builder.activity(a);
    let gate = builder
        .signal_gate_with_timeout(
            "approval",
            Duration::from_secs(3600),
            GateTimeoutAction::Continue,
        )
        .upstream(&extract);

    let def = builder.build().expect("gate DAG builds");
    let signal: &DagSignalGate = def.tasks()[gate.index()]
        .signal
        .as_ref()
        .expect("gate carries signal metadata");
    assert_eq!(signal.timeout, Some(Duration::from_secs(3600)));
    assert_eq!(signal.on_timeout, GateTimeoutAction::Continue);
}

/// **Level isolation (highest-risk design point).** A gate that shares a
/// topological level with an independent activity must be split into its own
/// singleton execution level so its `WaitForSignal` suspension is never batched
/// with the sibling activity's `ScheduleActivity`.
///
/// Topology: `a → {activity b, gate g}` (b and g both depend only on a, so Kahn
/// places them in the same level 1) `→ c`. After isolation no level may contain
/// both a gate and a non-gate, and the gate's level must be a singleton.
#[test]
fn signal_gate_is_isolated_into_its_own_singleton_level() {
    let mut builder = DagBuilder::new();
    let root = builder.activity(a);
    let sibling = builder.activity(b).upstream(&root);
    let gate = builder.signal_gate("go").upstream(&root);
    let _join = builder.activity(c).upstream(&sibling).upstream(&gate);

    let def = builder.build().expect("gate DAG builds");
    let tasks = def.tasks();
    let levels = def.execution_levels();

    let gate_idx = gate.index();

    // The gate lives alone in its level.
    let gate_level = levels
        .iter()
        .find(|level| level.contains(&gate_idx))
        .expect("gate index must appear in some execution level");
    assert_eq!(
        gate_level.len(),
        1,
        "a signal gate must occupy its own singleton execution level, got {gate_level:?}"
    );

    // No level mixes a gate with a non-gate task.
    for level in levels {
        let has_gate = level.iter().any(|&i| tasks[i].signal.is_some());
        let has_activity = level.iter().any(|&i| tasks[i].signal.is_none());
        assert!(
            !(has_gate && has_activity),
            "no execution level may batch a gate with an activity: {level:?}"
        );
    }
}

/// Gap B surface — a gate returns a `DagTaskRef`, so it composes as an upstream
/// and as a `.map_activity(...).over(&gate)` fan-out source.
#[test]
fn signal_gate_is_usable_as_upstream_and_map_source() {
    let mut builder = DagBuilder::new();
    let root = builder.activity(a);
    let gate = builder.signal_gate("items").upstream(&root);
    // Fan out a downstream mapped activity over the gate's (array) payload.
    let mapped = builder.map_activity(b).over(&gate);
    let _sink = builder.activity(c).upstream(&mapped);

    let def = builder.build().expect("gate+map DAG builds");
    let tasks = def.tasks();
    // The mapped node maps over the gate's output slot.
    assert_eq!(tasks[mapped.index()].map_upstream, Some(gate.index()));
    // The gate is a declared upstream of the mapped node.
    assert!(tasks[mapped.index()].upstreams.contains(&gate.index()));
}

/// AC6 — a classic (non-unified) DAG (`workflow_handler: None`) containing a
/// signal gate is rejected at `try_build` time with an error naming the DAG and
/// the signal (a gate requires the unified workflow-handler path).
#[test]
fn classic_dag_with_gate_is_rejected_at_build_time() {
    use autumn_harvest::DagInfo;
    use autumn_harvest::OverlapPolicy;
    use autumn_harvest::builder::HarvestBuilder;

    // A hand-built *classic* DagInfo: workflow_handler is None (the
    // unified-vs-classic discriminator), but its builder adds a signal gate.
    let classic_gate_dag = DagInfo {
        name: "classic_gate_dag",
        module: "tests",
        schedule: None,
        catchup: false,
        max_active_runs: 1,
        default_queue: None,
        builder: |dag: &mut DagBuilder| {
            let root = dag.activity(a);
            let _gate = dag.signal_gate("approval").upstream(&root);
        },
        workflow_handler: None,
        jitter: Duration::ZERO,
        overlap_policy: OverlapPolicy::Skip,
        buffer_all_max: 100,
        owner: None,
        runbook_url: None,
        severity: None,
        mcp: false,
        execution_timeout: None,
        sla: None,
    };

    let result = HarvestBuilder::new()
        .dags(vec![classic_gate_dag])
        .try_build();
    let err = result.expect_err("a classic DAG with a signal gate must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("classic_gate_dag") && msg.contains("approval"),
        "AC6 rejection must name the DAG and the signal, got: {msg}"
    );
}

/// Codex round-2 (#746): a gate stores its signal name in `activity_name`, so
/// the local-activity validator (`validate_dags_do_not_use_local_activities`)
/// must skip gate tasks — a gate never dispatches that activity. Registering a
/// LOCAL activity named `approval` alongside a unified DAG `signal_gate("approval")`
/// must therefore still build; before the fix it false-rejected with
/// `HarvestBuilderError::LocalActivityInDag`.
mod gate_signal_name_is_not_a_local_activity {
    use super::a;
    use autumn_harvest::builder::HarvestBuilder;
    use autumn_harvest::dag::DagBuilder;
    use autumn_harvest::prelude::*;
    use autumn_harvest::{DagInfo, OverlapPolicy};
    use serde_json::Value;
    use std::time::Duration;

    // A LOCAL activity whose name collides with the gate's signal name.
    #[activity(local = true, start_to_close = "5s")]
    async fn approval(_ctx: &ActivityContext) -> Result<Value, String> {
        Ok(Value::Null)
    }

    #[test]
    fn unified_gate_and_same_named_local_activity_builds() {
        // A *unified* DagInfo (workflow_handler: Some) whose gate awaits signal
        // `approval`, upstream of an activity node. The gate task's
        // `activity_name` is "approval" — the same string as the local activity.
        let gate_dag = DagInfo {
            name: "gate_flow",
            module: "tests",
            schedule: None,
            catchup: false,
            max_active_runs: 1,
            default_queue: None,
            builder: |dag: &mut DagBuilder| {
                let root = dag.activity(a);
                let _gate = dag.signal_gate("approval").upstream(&root);
            },
            workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
            jitter: Duration::ZERO,
            overlap_policy: OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        };

        let result = HarvestBuilder::new()
            .dags(vec![gate_dag])
            .activities(vec![approval_info()])
            .try_build();

        assert!(
            result.is_ok(),
            "a gate's signal name must not be validated as a local-activity dispatch: {:?}",
            result.err()
        );
    }
}

// ── Walker / replay tests (require the #[dag] macro's unified handler) ────────

#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]
mod unified {
    use autumn_harvest::context::WorkflowCommand;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
    use autumn_harvest::prelude::*;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use autumn_harvest::types::{ExecutionId, TimerId};
    use chrono::Utc;
    use serde_json::{Value, json};
    use std::time::Duration;

    #[activity]
    async fn extract_users(_ctx: &ActivityContext) -> Result<Value, String> {
        Ok(json!("extracted"))
    }

    #[activity]
    async fn load_users(_ctx: &ActivityContext) -> Result<Value, String> {
        Ok(json!("loaded"))
    }

    #[activity]
    async fn process_item(_ctx: &ActivityContext) -> Result<Value, String> {
        Ok(json!("processed"))
    }

    /// `extract → signal_gate("approval") → load` — no timeout.
    #[dag(default_queue = "gate-q")]
    fn approval_gate_dag(dag: &mut DagBuilder) {
        let extract = dag.activity(extract_users);
        let gate = dag.signal_gate("approval").upstream(&extract);
        let _load = dag.activity(load_users).upstream(&gate);
    }

    /// `extract → signal_gate(timeout, Continue) → load`.
    #[dag(default_queue = "gate-q")]
    fn approval_timeout_continue_dag(dag: &mut DagBuilder) {
        let extract = dag.activity(extract_users);
        let gate = dag
            .signal_gate_with_timeout(
                "approval",
                Duration::from_secs(3600),
                GateTimeoutAction::Continue,
            )
            .upstream(&extract);
        let _load = dag.activity(load_users).upstream(&gate);
    }

    /// `extract → signal_gate(timeout, FailRun)`.
    #[dag(default_queue = "gate-q")]
    fn approval_timeout_failrun_dag(dag: &mut DagBuilder) {
        let extract = dag.activity(extract_users);
        let _gate = dag
            .signal_gate_with_timeout(
                "approval",
                Duration::from_secs(3600),
                GateTimeoutAction::FailRun,
            )
            .upstream(&extract);
    }

    /// `source → signal_gate("items") → map(process).over(gate)` — Gap B.
    #[dag(default_queue = "gate-q")]
    fn gate_map_dag(dag: &mut DagBuilder) {
        let source = dag.activity(extract_users);
        let gate = dag.signal_gate("items").upstream(&source);
        let _mapped = dag.map_activity(process_item).over(&gate);
    }

    /// `extract → signal_gate(timeout, Continue) → load[.condition(payload)]`.
    ///
    /// The downstream `load` is gated on the *signal payload* — it dispatches
    /// only when the gate's stored output is `{"approved": true}`. This makes the
    /// gate's payload binding **observable in history**: a bug that stored
    /// `Value::Null` for a signal-win (instead of the real payload) flips the
    /// condition to `false`, so `load` records a `dag_skip` marker instead of an
    /// `ActivityScheduled` — a replay divergence. Used by the AC5 "both-events,
    /// signal wins" money fixture below.
    #[dag(default_queue = "gate-q")]
    fn gate_signal_payload_dag(dag: &mut DagBuilder) {
        let extract = dag.activity(extract_users);
        let gate = dag
            .signal_gate_with_timeout(
                "approval",
                Duration::from_secs(3600),
                GateTimeoutAction::Continue,
            )
            .upstream(&extract);
        let _load = dag
            .activity(load_users)
            .upstream(&gate)
            .condition(|ups| ups[0].get("approved") == Some(&json!(true)));
    }

    /// `extract → {load (activity), go (gate)} → process` — the sibling activity
    /// `load` and the gate `go` share a Kahn level (both depend only on
    /// `extract`). Level isolation must split them so the gate's `WaitForSignal`
    /// is never batched with `load`'s `ScheduleActivity`.
    #[dag(default_queue = "gate-q")]
    fn mixed_level_gate_dag(dag: &mut DagBuilder) {
        let root = dag.activity(extract_users);
        let sibling = dag.activity(load_users).upstream(&root);
        let gate = dag.signal_gate("go").upstream(&root);
        let _join = dag
            .activity(process_item)
            .upstream(&sibling)
            .upstream(&gate);
    }

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    // Level isolation, proven at the worker-suspension-shape level: reaching the
    // gate must suspend with EXACTLY one `WaitForSignal` command — never batched
    // with a `ScheduleActivity`.
    #[tokio::test]
    async fn gate_suspends_with_only_a_wait_for_signal_command() {
        let id_extract = ActivityExecId::new();
        let history = vec![
            started(Value::Null),
            WorkflowEvent::ActivityScheduled {
                activity_id: id_extract,
                name: "extract_users".into(),
                input: json!({"conf": Value::Null, "dag_task": "extract_users"}),
                queue: "gate-q".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: id_extract,
                output: json!("extracted"),
            },
        ];

        let outcome = run_workflow(
            ExecutionId::new(),
            history,
            __autumn_workflow_info_approval_gate_dag().handler,
            Value::Null,
        )
        .await;

        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("gate should suspend awaiting its signal, got {outcome:?}");
        };
        assert_eq!(
            commands.len(),
            1,
            "an isolated gate must suspend with exactly one command, got {commands:?}"
        );
        assert!(
            matches!(&commands[0], WorkflowCommand::WaitForSignal { signal_name, .. } if signal_name == "approval"),
            "the sole command must be WaitForSignal(approval), got {:?}",
            commands[0]
        );
    }

    // AC1/AC5 signal branch: live run unblocked by a queued signal, then the
    // captured history replays deterministically down the signal branch.
    #[tokio::test]
    async fn gate_signal_branch_unblocks_downstream_and_replays() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("load_users", |_| Ok(json!("loaded")))
            .queue_signal("approval", json!({"approved": true}))
            .run(
                __autumn_workflow_info_approval_gate_dag().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "signal-delivered gate DAG should complete, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        // Downstream load_users ran (gate unblocked it).
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "load_users")),
            "load_users must be scheduled after the gate unblocks"
        );

        let report = WorkflowReplayer::new()
            .register_fn(
                "approval_gate_dag",
                __autumn_workflow_info_approval_gate_dag().handler,
            )
            .replay_from_events(events.to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: signal-branch history must replay deterministically, got: {report}"
        );
    }

    // AC2b/AC5 timeout-continue branch: no signal queued → the deadline timer
    // auto-fires; downstream still runs (gate output = null sentinel); the
    // captured timer-first history replays deterministically.
    #[tokio::test]
    async fn gate_timeout_continue_branch_runs_downstream_and_replays() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("load_users", |_| Ok(json!("loaded")))
            .run(
                __autumn_workflow_info_approval_timeout_continue_dag().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "timeout+Continue gate DAG should complete, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "load_users")),
            "timeout+Continue must proceed to load_users"
        );

        let report = WorkflowReplayer::new()
            .register_fn(
                "approval_timeout_continue_dag",
                __autumn_workflow_info_approval_timeout_continue_dag().handler,
            )
            .replay_from_events(events.to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: timer-first history must replay deterministically, got: {report}"
        );
    }

    // AC2a timeout-failrun branch: no signal → timer fires → the DAG run fails.
    #[tokio::test]
    async fn gate_timeout_failrun_branch_fails_the_dag() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .run(
                __autumn_workflow_info_approval_timeout_failrun_dag().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_err(),
            "timeout+FailRun gate DAG must fail the run when the deadline fires first, got: {:?}",
            outcome.result
        );
    }

    // Gap B: a signal delivering a JSON array fans out a downstream `.map` node.
    #[tokio::test]
    async fn gate_map_fans_out_over_signal_array_payload() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("process_item", |_| Ok(json!("processed")))
            .queue_signal("items", json!([1, 2, 3]))
            .run(__autumn_workflow_info_gate_map_dag().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_ok(),
            "map-over-gate DAG should complete, got: {:?}",
            outcome.result
        );
        // Three process_item instances scheduled — one per array element.
        let mapped = outcome
            .events()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "process_item"))
            .count();
        assert_eq!(
            mapped, 3,
            "map node must fan out one instance per signal-array element, got {mapped}"
        );
    }

    // ── AC5: both-events replay fixtures (issue #746 review, mutation-proven) ──
    //
    // The `WorkflowTestEnv`-generated money tests above deliberately do NOT
    // arm the timer when a signal will win (see the harness): the signal-branch
    // test has a *no-timeout* gate (no timer in history) and the timeout tests
    // queue *no signal*. So a BOUNDED gate that RECEIVES its signal — the
    // production-realistic "approval-with-SLA that gets approved" shape, where
    // both a `TimerStarted` (the armed deadline) AND a `SignalReceived` sit in
    // history — had ZERO replay coverage. A bug storing `Value::Null` instead of
    // the signal payload on a bounded-gate signal-win passed every generated
    // test. These hand-built fixtures feed the exact both-events history and
    // replay the DAG's own generated handler against it.

    /// The conf-wrapped input the unified DAG walker synthesizes for a plain
    /// (non-mapped) activity node: `{"conf": <dag input>, "dag_task": <name>}`.
    fn conf_input(dag_task: &str) -> Value {
        json!({ "conf": Value::Null, "dag_task": dag_task })
    }

    fn sched(name: &str, id: ActivityExecId, input: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: name.into(),
            input,
            queue: "gate-q".into(),
        }
    }

    fn completed(id: ActivityExecId, output: Value) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output,
        }
    }

    /// **AC5 money fixture — bounded gate, both events present, signal wins.**
    ///
    /// History: `extract` completes, the gate's deadline timer is armed
    /// (`TimerStarted`), then the `approval` signal arrives (`SignalReceived`)
    /// before any `TimerFired`, so the signal wins the race. Its payload
    /// `{"approved": true}` is bound into the gate's output slot, satisfying the
    /// downstream `load`'s `.condition(...)` and scheduling `load`.
    ///
    /// This is the fixture that a `Value::Null`-for-signal-win bug MUST fail: a
    /// null gate output flips the condition to `false`, so the walker would
    /// record a `dag_skip` marker where history has `ActivityScheduled(load)` —
    /// a replay divergence. (Verified falsifiable by temporarily applying that
    /// mutation during review.)
    #[tokio::test]
    async fn gate_bounded_signal_win_both_events_replays_and_binds_payload() {
        let id_e = ActivityExecId::new();
        let id_l = ActivityExecId::new();
        let history = vec![
            started(Value::Null),
            sched("extract_users", id_e, conf_input("extract_users")),
            completed(id_e, json!("extracted")),
            // The gate's armed deadline timer (issue #476 race id convention).
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("__signal_timeout:1:approval"),
                duration_secs: 3600,
            },
            // Signal arrives FIRST (before any TimerFired) → signal wins.
            WorkflowEvent::SignalReceived {
                signal_name: "approval".into(),
                payload: json!({ "approved": true }),
            },
            // The payload satisfied load's condition → load was dispatched.
            sched("load_users", id_l, conf_input("load_users")),
            completed(id_l, json!("loaded")),
        ];

        let report = WorkflowReplayer::new()
            .register_fn(
                "gate_signal_payload_dag",
                __autumn_workflow_info_gate_signal_payload_dag().handler,
            )
            .replay_from_events(history)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: a bounded gate whose signal wins (armed timer + SignalReceived \
             both in history) must replay deterministically down the signal \
             branch with the payload bound, got: {report}"
        );
    }

    /// **AC5 — bounded gate, both timer events present, timer wins (Continue).**
    ///
    /// History has `TimerStarted` then `TimerFired` and NO signal, so the
    /// deadline wins; `on_timeout = Continue` proceeds past the gate (null gate
    /// output) and the unconditional downstream `load` still runs.
    #[tokio::test]
    async fn gate_bounded_timer_win_continue_replays_downstream() {
        let id_e = ActivityExecId::new();
        let id_l = ActivityExecId::new();
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            started(Value::Null),
            sched("extract_users", id_e, conf_input("extract_users")),
            completed(id_e, json!("extracted")),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 3600,
            },
            WorkflowEvent::TimerFired { timer_id },
            // Continue → load runs even though the gate timed out.
            sched("load_users", id_l, conf_input("load_users")),
            completed(id_l, json!("loaded")),
        ];

        let report = WorkflowReplayer::new()
            .register_fn(
                "approval_timeout_continue_dag",
                __autumn_workflow_info_approval_timeout_continue_dag().handler,
            )
            .replay_from_events(history)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: a bounded gate whose deadline wins with Continue must replay \
             deterministically and still run the downstream, got: {report}"
        );
    }

    /// **AC5 — bounded gate, timer wins (`FailRun`) → deterministic failure.**
    ///
    /// The existing `gate_timeout_failrun_branch_fails_the_dag` only asserts a
    /// one-shot `is_err()`. This replays the timer-first history and asserts the
    /// run reaches the SAME failed outcome deterministically across repeated
    /// replays — a clean `WorkflowFailed`, never a `NonDeterminismDetected`.
    #[tokio::test]
    async fn gate_bounded_timer_win_failrun_replays_to_failed_outcome() {
        let id_e = ActivityExecId::new();
        let timer_id = TimerId::new("__signal_timeout:1:approval");
        let history = vec![
            started(Value::Null),
            sched("extract_users", id_e, conf_input("extract_users")),
            completed(id_e, json!("extracted")),
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 3600,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];

        // Deterministic across repeated replays: same failed outcome every time.
        for _ in 0..3 {
            let report = WorkflowReplayer::new()
                .register_fn(
                    "approval_timeout_failrun_dag",
                    __autumn_workflow_info_approval_timeout_failrun_dag().handler,
                )
                .replay_from_events(history.clone())
                .await;
            assert!(
                matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
                "AC5: a FailRun gate whose deadline fires must replay to the same \
                 deterministic FAILED outcome, got: {report}"
            );
            assert!(
                !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
                "the FailRun timeout must be a clean deterministic failure, never \
                 a non-determinism divergence, got: {report}"
            );
        }
    }

    // ── Cluster 4: payload binding, defensive walker, edge coverage ──────────

    fn scheduled(events: &[WorkflowEvent], name: &str) -> bool {
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { name: n, .. } if n == name))
    }

    /// **AC1 direct payload binding.** `gate_signal_payload_dag`'s downstream
    /// `load` is `.condition`-gated on the gate's stored output, so whether it
    /// dispatches is a direct, live-run function of the *signal payload* — not a
    /// null sentinel. A matching payload dispatches `load`; a non-matching one
    /// skips it. (This is the live-run twin of the both-events replay money
    /// fixture above.)
    ///
    /// **AC3 note.** The gate lowers onto `wait_for_signal`, which consumes a
    /// `SignalReceived` event regardless of how it was delivered — the standalone
    /// signal route, `signal_with_start` (#244), or in-workflow
    /// `signal_external_workflow` all append the same `SignalReceived`. Delivery
    /// is therefore route-agnostic at the gate, so a payload delivered via
    /// signal-with-start binds identically to the queued signal here; the
    /// both-events replay fixtures replay exactly such a `SignalReceived`.
    #[tokio::test]
    async fn gate_signal_payload_binds_into_downstream_condition() {
        // Matching payload → condition true → load dispatched.
        let approved = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("load_users", |_| Ok(json!("loaded")))
            .queue_signal("approval", json!({ "approved": true }))
            .run(
                __autumn_workflow_info_gate_signal_payload_dag().handler,
                Value::Null,
            )
            .await;
        assert!(approved.result.is_ok(), "{:?}", approved.result);
        assert!(
            scheduled(approved.events(), "load_users"),
            "a matching signal payload must bind into the gate output and \
             dispatch the conditioned downstream"
        );

        // Non-matching payload → condition false → load skipped. (A null gate
        // output would also skip it, which is exactly why the matching case
        // above — where load DOES run — proves the real payload, not a sentinel,
        // is bound.)
        let rejected = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("load_users", |_| Ok(json!("loaded")))
            .queue_signal("approval", json!({ "approved": false }))
            .run(
                __autumn_workflow_info_gate_signal_payload_dag().handler,
                Value::Null,
            )
            .await;
        assert!(rejected.result.is_ok(), "{:?}", rejected.result);
        assert!(
            !scheduled(rejected.events(), "load_users"),
            "a non-matching signal payload must skip the conditioned downstream"
        );
    }

    /// **Level isolation, end-to-end.** The builder-level
    /// `signal_gate_is_isolated_into_its_own_singleton_level` proves the split;
    /// this proves it *matters*: a DAG whose Kahn topology co-locates an activity
    /// and a gate in one level runs cleanly (the gate's `WaitForSignal` is never
    /// batched with the sibling's `ScheduleActivity`, which the worker rejects).
    #[tokio::test]
    async fn mixed_level_gate_and_activity_run_cleanly_end_to_end() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("load_users", |_| Ok(json!("loaded")))
            .mock_activity("process_item", |_| Ok(json!("processed")))
            .queue_signal("go", json!({ "ok": true }))
            .run(
                __autumn_workflow_info_mixed_level_gate_dag().handler,
                Value::Null,
            )
            .await;
        assert!(
            outcome.result.is_ok(),
            "a co-located gate+activity level must run cleanly, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        assert!(
            scheduled(events, "load_users"),
            "the sibling activity must run"
        );
        assert!(
            scheduled(events, "process_item"),
            "the join must run after both the sibling and the gate resolve"
        );
    }

    /// A gate feeding `.map` must fail cleanly (not panic) when the signal
    /// payload is not a JSON array.
    #[tokio::test]
    async fn gate_map_over_non_array_payload_errors_cleanly() {
        for payload in [json!("not-an-array"), Value::Null, json!(7)] {
            let outcome = WorkflowTestEnv::new()
                .mock_activity("extract_users", |_| Ok(json!("extracted")))
                .mock_activity("process_item", |_| Ok(json!("processed")))
                .queue_signal("items", payload.clone())
                .run(__autumn_workflow_info_gate_map_dag().handler, Value::Null)
                .await;
            let err = outcome
                .result
                .expect_err(&format!("map over non-array {payload} must error"));
            assert!(
                err.contains("not a JSON array"),
                "map over a non-array payload must fail cleanly, got: {err}"
            );
        }
    }

    /// A gate feeding `.map` over an EMPTY-array payload succeeds with zero
    /// fanned-out instances.
    #[tokio::test]
    async fn gate_map_over_empty_array_succeeds_with_no_instances() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_users", |_| Ok(json!("extracted")))
            .mock_activity("process_item", |_| Ok(json!("processed")))
            .queue_signal("items", json!([]))
            .run(__autumn_workflow_info_gate_map_dag().handler, Value::Null)
            .await;
        assert!(
            outcome.result.is_ok(),
            "map over an empty array must succeed, got: {:?}",
            outcome.result
        );
        let mapped = outcome
            .events()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "process_item"))
            .count();
        assert_eq!(
            mapped, 0,
            "an empty-array gate payload must fan out zero instances, got {mapped}"
        );
    }
}
