//! Cancellation scopes (issue #1984).
//!
//! A [`CancellationScope`] cancels a group of in-flight operations as a unit.
//! [`WorkflowContext::non_cancellable`] shields a block from a workflow
//! cancel. See `DESIGN-1984.md` for the design record.

use std::future::Future;

use crate::context::WorkflowContext;
use crate::error::HarvestResult;

/// A group of workflow operations that cancel together (issue #1984).
///
/// Create one with [`WorkflowContext::cancellation_scope`].
pub struct CancellationScope<'a> {
    ctx: &'a WorkflowContext,
}

impl<'a> CancellationScope<'a> {
    pub(crate) const fn new(ctx: &'a WorkflowContext) -> Self {
        Self { ctx }
    }

    /// Run `body` inside this scope.
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::Cancelled` when the scope is cancelled first.
    pub async fn run<F: Future>(&self, body: F) -> HarvestResult<F::Output> {
        let _ = self.ctx;
        Ok(body.await)
    }

    /// Cancel every operation that the body started.
    pub const fn cancel(&self) {}
}

impl WorkflowContext {
    /// Create a [`CancellationScope`] (issue #1984).
    #[must_use]
    pub const fn cancellation_scope(&self) -> CancellationScope<'_> {
        CancellationScope::new(self)
    }

    /// Run `body` so that a workflow cancel cannot stop it (issue #1984).
    ///
    /// # Errors
    ///
    /// Returns `HarvestError::Config` inside a cancellable scope.
    pub async fn non_cancellable<F: Future>(&self, body: F) -> HarvestResult<F::Output> {
        Ok(body.await)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use serde_json::{Value, json};

    use crate::context::{WorkflowCommand, WorkflowContext};
    use crate::error::{HarvestError, HarvestResult};
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId, TimerId};

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(activity_id: ActivityExecId, name: &str) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: name.into(),
            input: Value::Null,
            queue: "default".into(),
        }
    }

    /// Fails the test when `fut` does not resolve.
    async fn bounded<F: std::future::Future>(fut: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .expect("the future must resolve")
    }

    /// Runs `body` in a new scope. A sibling branch cancels the scope after
    /// the first poll of the body.
    async fn run_then_cancel<F: std::future::Future>(
        ctx: &WorkflowContext,
        body: F,
    ) -> HarvestResult<F::Output> {
        let scope = ctx.cancellation_scope();
        let (result, ()) = tokio::join!(scope.run(body), async { scope.cancel() });
        result
    }

    struct Losers {
        activities: Vec<ActivityExecId>,
        children: Vec<ExecutionId>,
        timers: Vec<TimerId>,
    }

    fn losers(commands: &[WorkflowCommand]) -> Option<Losers> {
        commands.iter().find_map(|c| match c {
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                timers,
                ..
            } => Some(Losers {
                activities: activities.clone(),
                children: children.clone(),
                timers: timers.clone(),
            }),
            _ => None,
        })
    }

    fn marker(commands: &[WorkflowCommand], wanted: &str) -> Option<Value> {
        commands.iter().find_map(|c| match c {
            WorkflowCommand::RecordMarker { name, details } if name == wanted => {
                Some(details.clone())
            }
            _ => None,
        })
    }

    fn count(commands: &[WorkflowCommand], pred: impl Fn(&WorkflowCommand) -> bool) -> usize {
        commands.iter().filter(|c| pred(c)).count()
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_activity() {
        let a = ActivityExecId::new();
        let ctx =
            WorkflowContext::for_replay(ExecutionId::new(), vec![started(), scheduled(a, "charge")]);

        let result = bounded(run_then_cancel(
            &ctx,
            ctx.execute_activity_raw("charge", Value::Null, "default"),
        ))
        .await;

        assert!(matches!(result, Err(HarvestError::Cancelled(_))), "{result:?}");
        let commands = ctx.drain_commands();
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.activities, vec![a]);
        assert!(marker(&commands, "cancel_scope:1").is_some(), "{commands:?}");
        assert_eq!(
            count(&commands, |c| matches!(c, WorkflowCommand::WaitForActivity { .. })),
            0,
            "a cancelled member must not park again: {commands:?}"
        );
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_timer() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new("deadline"),
                    duration_secs: 60,
                },
            ],
        );

        let result = bounded(run_then_cancel(&ctx, ctx.timer("deadline", 60))).await;

        assert!(matches!(result, Err(HarvestError::Cancelled(_))), "{result:?}");
        let commands = ctx.drain_commands();
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.timers, vec![TimerId::new("deadline")]);
        assert_eq!(
            count(&commands, |c| matches!(c, WorkflowCommand::StartTimer { .. })),
            0,
            "a cancelled timer must not be armed again: {commands:?}"
        );
    }

    #[tokio::test]
    async fn scope_cancel_cancels_an_in_flight_child_workflow() {
        let child = ExecutionId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: child,
                    workflow_name: "child".into(),
                    input: Value::Null,
                },
            ],
        );

        let result = bounded(run_then_cancel(
            &ctx,
            ctx.spawn_child_workflow_raw("child", Value::Null),
        ))
        .await;

        assert!(matches!(result, Err(HarvestError::Cancelled(_))), "{result:?}");
        let commands = ctx.drain_commands();
        let losers = losers(&commands).expect("the scope must cancel its members");
        assert_eq!(losers.children, vec![child]);
        assert_eq!(
            count(&commands, |c| matches!(c, WorkflowCommand::StartChildWorkflow { .. })),
            0,
            "a cancelled child must not be started again: {commands:?}"
        );
    }

    /// Reverse-brainstorm R1: an operation that starts and is cancelled in
    /// the same cycle never reaches the worker.
    #[tokio::test]
    async fn scope_cancel_withdraws_operations_started_in_the_same_cycle() {
        let ctx = WorkflowContext::new_test();

        let result = bounded(run_then_cancel(&ctx, async {
            tokio::join!(
                ctx.execute_activity_raw("charge", Value::Null, "default"),
                ctx.timer("deadline", 60),
                ctx.spawn_child_workflow_raw("child", Value::Null),
            )
        }))
        .await;

        assert!(matches!(result, Err(HarvestError::Cancelled(_))), "{result:?}");
        let commands = ctx.drain_commands();
        assert!(marker(&commands, "cancel_scope:1").is_some(), "{commands:?}");
        assert_eq!(
            count(&commands, |c| matches!(
                c,
                WorkflowCommand::ScheduleActivity { .. }
                    | WorkflowCommand::StartTimer { .. }
                    | WorkflowCommand::StartChildWorkflow { .. }
            )),
            0,
            "a withdrawn start must not reach the worker: {commands:?}"
        );
    }

    #[tokio::test]
    async fn scope_completes_normally_and_a_late_cancel_records_nothing() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let value = bounded(scope.run(async { 7 })).await;
        scope.cancel();

        assert!(matches!(value, Ok(7)), "{value:?}");
        assert!(ctx.drain_commands().is_empty());
    }

    #[tokio::test]
    async fn scope_cancelled_before_its_first_poll_returns_cancelled() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();
        scope.cancel();

        let result = bounded(scope.run(ctx.timer("deadline", 60))).await;

        assert!(matches!(result, Err(HarvestError::Cancelled(_))), "{result:?}");
        let commands = ctx.drain_commands();
        assert_eq!(
            count(&commands, |c| matches!(c, WorkflowCommand::StartTimer { .. })),
            0,
            "{commands:?}"
        );
    }

    #[tokio::test]
    async fn a_scope_runs_only_once() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let first = bounded(scope.run(async { 1 })).await;
        let second = bounded(scope.run(async { 2 })).await;

        assert!(matches!(first, Ok(1)), "{first:?}");
        assert!(matches!(second, Err(HarvestError::Config(_))), "{second:?}");
    }

    /// The workflow under the replay tests. Cycle 1 starts `charge` and
    /// waits for `abort`. The signal cancels the scope. The workflow then
    /// runs `cleanup`.
    async fn charge_or_abort(ctx: &WorkflowContext) -> HarvestResult<Value> {
        let scope = ctx.cancellation_scope();
        let (charged, ()) = tokio::join!(
            scope.run(ctx.execute_activity_raw("charge", Value::Null, "default")),
            async {
                let _ = ctx.wait_for_signal("abort").await;
                scope.cancel();
            }
        );
        match charged {
            Err(HarvestError::Cancelled(_)) => {
                ctx.execute_activity_raw("cleanup", Value::Null, "default")
                    .await
            }
            other => other?,
        }
    }

    fn scheduled_id(commands: &[WorkflowCommand], wanted: &str) -> ActivityExecId {
        commands
            .iter()
            .find_map(|c| match c {
                WorkflowCommand::ScheduleActivity {
                    activity_id, name, ..
                } if name == wanted => Some(*activity_id),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no ScheduleActivity({wanted}) in {commands:?}"))
    }

    /// Drives `charge_or_abort` through its three cycles. `concurrent`
    /// inserts a completion of `charge` that the cancel cycle did not see.
    async fn replay_after_scope_cancel(concurrent: bool) {
        let exec_id = ExecutionId::new();
        let abort = WorkflowEvent::SignalReceived {
            signal_name: "abort".into(),
            payload: Value::Null,
        };

        // Cycle 1: start `charge`, park on the signal.
        let ctx = WorkflowContext::for_replay(exec_id, vec![started()]);
        let first = tokio::time::timeout(Duration::from_millis(100), charge_or_abort(&ctx)).await;
        assert!(first.is_err(), "cycle 1 must park");
        let charge = scheduled_id(&ctx.drain_commands(), "charge");

        // Cycle 2: the signal cancels the scope live.
        let mut history = vec![started(), scheduled(charge, "charge"), abort];
        let ctx = WorkflowContext::for_replay(exec_id, history.clone());
        let second = tokio::time::timeout(Duration::from_millis(100), charge_or_abort(&ctx)).await;
        assert!(second.is_err(), "cycle 2 must park on cleanup");
        let commands = ctx.drain_commands();
        let details = marker(&commands, "cancel_scope:1").expect("cycle 2 records the cancel");
        let cleanup = scheduled_id(&commands, "cleanup");

        // Persist cycle 2 the way the worker does.
        if concurrent {
            history.push(WorkflowEvent::ActivityCompleted {
                activity_id: charge,
                output: json!("charged"),
            });
        }
        history.push(WorkflowEvent::MarkerRecorded {
            name: "cancel_scope:1".into(),
            details,
        });
        history.push(scheduled(cleanup, "cleanup"));
        if !concurrent {
            history.push(WorkflowEvent::ActivityFailed {
                activity_id: charge,
                error: "cancelled by its cancellation scope".into(),
                attempt: 1,
                error_type: "Error".into(),
                non_retryable: true,
                details: None,
            });
        }
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: cleanup,
            output: json!("cleaned"),
        });

        // Cycle 3: a full replay gives the same result and no new command.
        let ctx = WorkflowContext::for_replay(exec_id, history);
        let third = bounded(charge_or_abort(&ctx)).await;
        assert_eq!(third.ok(), Some(json!("cleaned")));
        assert!(ctx.take_nd_details().is_none());
        let commands = ctx.drain_commands();
        assert!(commands.is_empty(), "replay must add nothing: {commands:?}");
        assert!(
            !ctx.history_has_unconsumed_events(),
            "replay must consume the whole history"
        );
    }

    #[tokio::test]
    async fn replay_of_a_scope_cancel_is_deterministic() {
        replay_after_scope_cancel(false).await;
    }

    /// Reverse-brainstorm R2: a member completes during the cancel cycle.
    #[tokio::test]
    async fn replay_ignores_a_member_completion_the_cancel_cycle_did_not_see() {
        replay_after_scope_cancel(true).await;
    }

    #[tokio::test]
    async fn non_cancellable_records_open_and_close_markers() {
        let ctx = WorkflowContext::new_test();

        let value = bounded(ctx.non_cancellable(async { 3 })).await;

        assert!(matches!(value, Ok(3)), "{value:?}");
        let commands = ctx.drain_commands();
        assert!(marker(&commands, "non_cancellable_open:1").is_some(), "{commands:?}");
        assert!(marker(&commands, "non_cancellable_close:1").is_some(), "{commands:?}");
    }

    #[tokio::test]
    async fn non_cancellable_replays_its_markers_without_new_commands() {
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_open:1".into(),
                    details: Value::Null,
                },
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_close:1".into(),
                    details: Value::Null,
                },
            ],
        );

        let value = bounded(ctx.non_cancellable(async { 3 })).await;

        assert!(matches!(value, Ok(3)), "{value:?}");
        assert!(ctx.drain_commands().is_empty());
        assert!(!ctx.history_has_unconsumed_events());
    }

    /// Reverse-brainstorm R6.
    #[tokio::test]
    async fn non_cancellable_inside_a_cancellable_scope_is_rejected() {
        let ctx = WorkflowContext::new_test();
        let scope = ctx.cancellation_scope();

        let result = bounded(scope.run(ctx.non_cancellable(async { 3 }))).await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Config(_)))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_scope_inside_non_cancellable_cancels_normally() {
        let a = ActivityExecId::new();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![
                started(),
                WorkflowEvent::MarkerRecorded {
                    name: "non_cancellable_open:1".into(),
                    details: Value::Null,
                },
                scheduled(a, "charge"),
            ],
        );

        let result = bounded(ctx.non_cancellable(run_then_cancel(
            &ctx,
            ctx.execute_activity_raw("charge", Value::Null, "default"),
        )))
        .await;

        assert!(
            matches!(result, Ok(Err(HarvestError::Cancelled(_)))),
            "{result:?}"
        );
        let losers = losers(&ctx.drain_commands()).expect("the inner scope cancels");
        assert_eq!(losers.activities, vec![a]);
    }
}
