//! Resident workflow state (issue #1798, step 2).
//!
//! A cold decision builds a new [`WorkflowContext`] from the full history and
//! runs the handler from the top. The replay work of a decision therefore
//! grows with history length, and a run of n decisions costs O(n²).
//!
//! A resident workflow keeps the suspended handler future and its context
//! alive between decisions. A warm decision sends the new result to the
//! parked future and polls it once more. It does not replay history, so its
//! cost does not grow with history length.

use serde_json::Value;

use crate::event::WorkflowEvent;
use crate::executor::WorkflowOutcome;
use crate::info::WorkflowHandlerFn;
use crate::types::ExecutionId;

/// Why a resident workflow did not resume (issue #1798).
///
/// A decline is never an error. The worker drops the resident state and runs
/// a cold replay, which is always correct.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResumeDeclined {
    /// The resident path is not available.
    Unsupported,
}

/// A suspended workflow that stays in memory between decisions (issue #1798).
pub struct ResidentWorkflow {
    _private: (),
}

impl std::fmt::Debug for ResidentWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResidentWorkflow").finish_non_exhaustive()
    }
}

/// Runs one cold decision and keeps the workflow resident when it can.
///
/// Test and bench entry point. The worker uses the crate-internal path.
#[cfg(any(test, feature = "testing"))]
pub async fn start(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> (WorkflowOutcome, Option<ResidentWorkflow>) {
    let outcome = crate::executor::run_workflow(exec_id, history, handler, input).await;
    (outcome, None)
}

#[cfg(any(test, feature = "testing"))]
impl ResidentWorkflow {
    /// Resumes this workflow with the events appended since it suspended.
    ///
    /// # Errors
    ///
    /// Returns [`ResumeDeclined`] when the events do not resolve the parked
    /// future exactly as a replay would. The caller then runs a cold replay.
    pub async fn resume(
        self,
        _delta: &[WorkflowEvent],
    ) -> Result<(WorkflowOutcome, Option<Self>), ResumeDeclined> {
        Err(ResumeDeclined::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{WorkflowCommand, WorkflowContext};
    use crate::types::{ActivityExecId, TimerId};
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type HandlerFuture<'a> =
        Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc
                .with_ymd_and_hms(2026, 10, 3, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    /// The events a worker appends for a suspension, in command order.
    fn own_events(commands: &[WorkflowCommand]) -> Vec<WorkflowEvent> {
        commands
            .iter()
            .filter_map(|cmd| match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id,
                    name,
                    input,
                    queue,
                    ..
                } => Some(WorkflowEvent::ActivityScheduled {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                }),
                WorkflowCommand::StartTimer {
                    timer_id,
                    duration_secs,
                    ..
                } => Some(WorkflowEvent::TimerStarted {
                    timer_id: timer_id.clone(),
                    duration_secs: *duration_secs,
                }),
                WorkflowCommand::RecordMarker { name, details } => {
                    Some(WorkflowEvent::MarkerRecorded {
                        name: name.clone(),
                        details: details.clone(),
                    })
                }
                WorkflowCommand::RecordSideEffect { kind, name, value } => {
                    Some(WorkflowEvent::SideEffectRecorded {
                        kind: *kind,
                        name: name.clone(),
                        value: value.clone(),
                    })
                }
                _ => None,
            })
            .collect()
    }

    /// The event that resolves the one awaiting command of a suspension.
    fn resolution(commands: &[WorkflowCommand]) -> WorkflowEvent {
        commands
            .iter()
            .find_map(|cmd| match cmd {
                WorkflowCommand::ScheduleActivity {
                    activity_id, input, ..
                } => Some(WorkflowEvent::ActivityCompleted {
                    activity_id: *activity_id,
                    output: json!({ "echo": input }),
                }),
                WorkflowCommand::WaitForActivity { activity_id, .. } => {
                    Some(WorkflowEvent::ActivityCompleted {
                        activity_id: *activity_id,
                        output: json!({ "echo": null }),
                    })
                }
                WorkflowCommand::StartTimer { timer_id, .. } => Some(WorkflowEvent::TimerFired {
                    timer_id: timer_id.clone(),
                }),
                WorkflowCommand::WaitForSignal { signal_name, .. } => {
                    Some(WorkflowEvent::SignalReceived {
                        signal_name: signal_name.clone(),
                        payload: json!({ "signal": signal_name }),
                    })
                }
                _ => None,
            })
            .expect("a suspension awaits one command")
    }

    /// A stable text form of a command. It leaves out random ids.
    fn shape(cmd: &WorkflowCommand) -> String {
        match cmd {
            WorkflowCommand::ScheduleActivity { name, input, .. } => {
                format!("ScheduleActivity({name}, {input})")
            }
            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => format!("StartTimer({}, {duration_secs})", timer_id.as_str()),
            WorkflowCommand::WaitForSignal { signal_name, .. } => {
                format!("WaitForSignal({signal_name})")
            }
            WorkflowCommand::PublishProgress { seq, chunk } => {
                format!("PublishProgress({seq}, {chunk})")
            }
            WorkflowCommand::RecordMarker { name, .. } => format!("RecordMarker({name})"),
            WorkflowCommand::RecordSideEffect { kind, name, .. } => {
                format!("RecordSideEffect({kind:?}, {name:?})")
            }
            other => format!("{other:?}"),
        }
    }

    fn outcome_shape(outcome: &WorkflowOutcome) -> String {
        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                let shapes: Vec<String> = commands.iter().map(shape).collect();
                format!("Suspended[{}]", shapes.join(", "))
            }
            WorkflowOutcome::Completed { output, .. } => format!("Completed({output})"),
            WorkflowOutcome::Failed {
                error,
                handler_panic,
                non_deterministic_details,
                ..
            } => format!(
                "Failed({error}, panic={handler_panic}, nd={})",
                non_deterministic_details.is_some()
            ),
            WorkflowOutcome::TaskFailed { error } => format!("TaskFailed({error})"),
            WorkflowOutcome::ContinuedAsNew { input, .. } => format!("ContinuedAsNew({input})"),
        }
    }

    /// The decisions of one run, and the history that the run wrote.
    struct Trace {
        shapes: Vec<String>,
        history: Vec<WorkflowEvent>,
        resumes: usize,
    }

    /// Drives a run with a cold replay for every decision.
    async fn drive_cold(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        for _ in 0..32 {
            let outcome =
                crate::executor::run_workflow(exec_id, history.clone(), handler, input.clone())
                    .await;
            shapes.push(outcome_shape(&outcome));
            let WorkflowOutcome::Suspended { commands } = outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes: 0,
                };
            };
            history.extend(own_events(&commands));
            history.push(resolution(&commands));
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// Drives a run that resumes the resident workflow when it can.
    async fn drive_warm(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let exec_id = ExecutionId::new();
        let mut history = vec![started(input.clone())];
        let mut shapes = Vec::new();
        let mut resumes = 0;
        let (mut outcome, mut resident) =
            start(exec_id, history.clone(), handler, input.clone()).await;
        for _ in 0..32 {
            shapes.push(outcome_shape(&outcome));
            let WorkflowOutcome::Suspended { commands } = &outcome else {
                return Trace {
                    shapes,
                    history,
                    resumes,
                };
            };
            let start_len = history.len();
            history.extend(own_events(commands));
            history.push(resolution(commands));
            let delta = &history[start_len..];
            let resumed = match resident.take() {
                Some(live) => live.resume(delta).await.ok(),
                None => None,
            };
            (outcome, resident) = match resumed {
                Some(next) => {
                    resumes += 1;
                    next
                }
                None => start(exec_id, history.clone(), handler, input.clone()).await,
            };
        }
        panic!("the run did not finish in 32 decisions");
    }

    /// Asserts that the warm run decided like the cold run, and that its
    /// history replays cold to the same end.
    async fn assert_warm_matches_cold(handler: WorkflowHandlerFn, input: Value) -> Trace {
        let cold = drive_cold(handler, input.clone()).await;
        let warm = drive_warm(handler, input.clone()).await;
        assert_eq!(warm.shapes, cold.shapes, "warm decisions must equal cold");
        let replayed =
            crate::executor::run_workflow(ExecutionId::new(), warm.history.clone(), handler, input)
                .await;
        assert_eq!(
            Some(outcome_shape(&replayed)),
            warm.shapes.last().cloned(),
            "the warm history must replay cold to the same end"
        );
        warm
    }

    static CHAIN_BODY_STARTS: AtomicUsize = AtomicUsize::new(0);

    /// Three activities with side effects and progress, then a timer and a
    /// signal. Returns values that depend on history length.
    fn chain_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            CHAIN_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
            let steps = input["steps"].as_u64().ok_or("missing steps")?;
            let mut echoes = Vec::new();
            for i in 0..steps {
                let _ = ctx.new_uuid();
                let out = ctx
                    .execute_activity_raw("step", json!({ "i": i }), "default")
                    .await
                    .map_err(|e| e.to_string())?;
                ctx.publish_progress(json!({ "done": i }))
                    .map_err(|e| e.to_string())?;
                echoes.push(out);
            }
            ctx.timer("pause", 5).await.map_err(|e| e.to_string())?;
            let signal = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
            Ok(json!({
                "echoes": echoes,
                "signal": signal,
                "events": ctx.history_event_count(),
            }))
        })
    }

    #[tokio::test]
    async fn warm_activity_timer_signal_chain_matches_cold_replay() {
        let warm = assert_warm_matches_cold(chain_workflow, json!({ "steps": 3 })).await;
        assert_eq!(
            warm.resumes, 4,
            "every decision after the first must resume the resident future"
        );
    }

    static LOOP_BODY_STARTS: AtomicUsize = AtomicUsize::new(0);

    /// A long-lived signal loop, the O(n²) case of issue #1798.
    fn signal_loop_workflow<'a>(ctx: &'a WorkflowContext, input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            LOOP_BODY_STARTS.fetch_add(1, Ordering::SeqCst);
            let rounds = input["rounds"].as_u64().ok_or("missing rounds")?;
            let mut seen = 0_u64;
            for _ in 0..rounds {
                let _ = ctx.wait_for_signal("tick").await.map_err(|e| e.to_string())?;
                seen += 1;
            }
            Ok(json!(seen))
        })
    }

    #[tokio::test]
    async fn warm_signal_loop_runs_the_body_once() {
        LOOP_BODY_STARTS.store(0, Ordering::SeqCst);
        let warm = drive_warm(signal_loop_workflow, json!({ "rounds": 6 })).await;
        assert_eq!(warm.shapes.last().map(String::as_str), Some("Completed(6)"));
        assert_eq!(warm.resumes, 6);
        assert_eq!(
            LOOP_BODY_STARTS.load(Ordering::SeqCst),
            1,
            "a warm decision must not run the body from the top"
        );
    }

    #[tokio::test]
    async fn warm_signal_loop_matches_cold_replay() {
        assert_warm_matches_cold(signal_loop_workflow, json!({ "rounds": 4 })).await;
    }

    /// Fails after the first activity, so the warm run must fail like the cold run.
    fn fail_after_activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            Err("business rule".to_string())
        })
    }

    #[tokio::test]
    async fn warm_failure_after_resume_matches_cold_replay() {
        let warm = assert_warm_matches_cold(fail_after_activity_workflow, json!({})).await;
        assert_eq!(warm.resumes, 1);
    }

    /// Panics after the first activity.
    fn panic_after_activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            panic!("boom after resume");
        })
    }

    #[tokio::test]
    async fn warm_panic_after_resume_matches_cold_replay() {
        let warm = assert_warm_matches_cold(panic_after_activity_workflow, json!({})).await;
        assert_eq!(warm.resumes, 1);
        assert!(
            warm.shapes
                .last()
                .is_some_and(|s| s.contains("panic=true")),
            "the resumed panic must be contained: {:?}",
            warm.shapes
        );
    }

    /// Waits on a foreign future after the first activity.
    fn foreign_wait_after_activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.execute_activity_raw("step", json!({}), "default")
                .await
                .map_err(|e| e.to_string())?;
            std::future::pending::<()>().await;
            Ok(Value::Null)
        })
    }

    #[tokio::test(start_paused = true)]
    async fn warm_foreign_wait_after_resume_fails_the_task() {
        let exec_id = ExecutionId::new();
        let history = vec![started(Value::Null)];
        let (outcome, resident) = start(
            exec_id,
            history,
            foreign_wait_after_activity_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let mut delta = own_events(&commands);
        delta.push(resolution(&commands));
        let resident = resident.expect("an activity suspension is resident");
        let (outcome, next) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(outcome, WorkflowOutcome::TaskFailed { .. }),
            "a foreign wait must fail the task: {outcome:?}"
        );
        assert!(next.is_none(), "a failed task must drop the resident state");
    }

    // ── Shapes that must never stay resident ─────────────────────────

    /// Awaits two activities at once.
    fn join_workflow<'a>(ctx: &'a WorkflowContext, _input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            let (a, b) = futures::join!(
                ctx.execute_activity_raw("a", json!({}), "default"),
                ctx.execute_activity_raw("b", json!({}), "default"),
            );
            Ok(json!([a.map_err(|e| e.to_string())?, b.map_err(|e| e.to_string())?]))
        })
    }

    /// Registers a push signal handler, then awaits an activity.
    fn handler_workflow<'a>(ctx: &'a WorkflowContext, _input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.register_signal_handler_raw("note", |_payload: Value| {});
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Parks on a condition, which holds a park token.
    fn condition_workflow<'a>(ctx: &'a WorkflowContext, _input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.await_condition(|| false).await;
            Ok(Value::Null)
        })
    }

    #[tokio::test]
    async fn ineligible_suspensions_are_not_resident() {
        for (name, handler) in [
            ("join", join_workflow as WorkflowHandlerFn),
            ("signal handler", handler_workflow),
            ("condition", condition_workflow),
        ] {
            let (outcome, resident) =
                start(ExecutionId::new(), vec![started(Value::Null)], handler, Value::Null).await;
            assert!(
                matches!(outcome, WorkflowOutcome::Suspended { .. }),
                "{name}: the fixture must suspend: {outcome:?}"
            );
            assert!(resident.is_none(), "{name}: must not stay resident");
        }
    }

    // ── Deltas that must decline ─────────────────────────────────────

    /// One activity, then completes with its output.
    fn one_activity_workflow<'a>(ctx: &'a WorkflowContext, _input: Value) -> HandlerFuture<'a> {
        Box::pin(async move {
            ctx.execute_activity_raw("a", json!({}), "default")
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Suspends `one_activity_workflow` and returns its own events and id.
    async fn suspended_activity() -> (ResidentWorkflow, Vec<WorkflowEvent>, ActivityExecId) {
        let (outcome, resident) = start(
            ExecutionId::new(),
            vec![started(Value::Null)],
            one_activity_workflow,
            Value::Null,
        )
        .await;
        let WorkflowOutcome::Suspended { commands } = outcome else {
            panic!("the first decision must suspend");
        };
        let id = commands
            .iter()
            .find_map(|c| match c {
                WorkflowCommand::ScheduleActivity { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .expect("the decision schedules an activity");
        let own = own_events(&commands);
        (resident.expect("an activity suspension is resident"), own, id)
    }

    fn completed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: json!(1),
        }
    }

    #[tokio::test]
    async fn deltas_that_replay_could_read_differently_decline() {
        let other = ActivityExecId::new();
        let cases: Vec<(&str, Box<dyn Fn(&[WorkflowEvent], ActivityExecId) -> Vec<WorkflowEvent>>)> = vec![
            ("empty delta", Box::new(|_own, _id| Vec::new())),
            ("no own events", Box::new(|_own, id| vec![completed(id)])),
            ("own events only", Box::new(|own, _id| own.to_vec())),
            (
                "wrong activity id",
                Box::new(move |own, _id| [own.to_vec(), vec![completed(other)]].concat()),
            ),
            (
                "activity failure",
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![WorkflowEvent::ActivityFailed {
                            activity_id: id,
                            error: "boom".into(),
                            attempt: 1,
                            error_type: "Error".into(),
                            details: None,
                            non_retryable: false,
                        }],
                    ]
                    .concat()
                }),
            ),
            (
                "two resolutions",
                Box::new(|own, id| [own.to_vec(), vec![completed(id), completed(id)]].concat()),
            ),
            (
                "an extra signal",
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![
                            completed(id),
                            WorkflowEvent::SignalReceived {
                                signal_name: "x".into(),
                                payload: Value::Null,
                            },
                        ],
                    ]
                    .concat()
                }),
            ),
            (
                "a cancel request",
                Box::new(|own, id| {
                    [
                        own.to_vec(),
                        vec![
                            WorkflowEvent::WorkflowCancelled { reason: "op".into() },
                            completed(id),
                        ],
                    ]
                    .concat()
                }),
            ),
            (
                "a timer fire for another timer",
                Box::new(|own, _id| {
                    [
                        own.to_vec(),
                        vec![WorkflowEvent::TimerFired {
                            timer_id: TimerId::new("other"),
                        }],
                    ]
                    .concat()
                }),
            ),
        ];
        for (name, build) in cases {
            let (resident, own, id) = suspended_activity().await;
            let delta = build(&own, id);
            assert!(
                resident.resume(&delta).await.is_err(),
                "{name}: the resume must decline"
            );
        }
    }

    #[tokio::test]
    async fn exact_delta_resumes_and_completes() {
        let (resident, own, id) = suspended_activity().await;
        let delta = [own, vec![completed(id)]].concat();
        let (outcome, next) = resident.resume(&delta).await.expect("the delta resolves");
        assert!(
            matches!(&outcome, WorkflowOutcome::Completed { output, .. } if *output == json!(1)),
            "{outcome:?}"
        );
        assert!(next.is_none(), "a completed run is not resident");
    }
}
