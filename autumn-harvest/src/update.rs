//! Update handler registry for the workflow Update primitive (issue #140).
//!
//! An `UpdateRegistry` stores type-erased validators and async handlers keyed
//! by name. Registration is idempotent — calling `register` twice for the same
//! name is a no-op, making it safe to call at the top of every `#[workflow]`
//! function on each replay cycle.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::context::{WorkflowCommand, WorkflowContext};
use crate::executor::WorkflowOutcome;
use crate::types::UpdateId;

/// Longest time that one admitted update handler may run in a cycle (issue #2035).
///
/// A handler that does not finish in this time records `UpdateFailed`. When
/// less budget is left, the limit is half of what is left.
pub(crate) const UPDATE_HANDLER_TIMEOUT: Duration = Duration::from_secs(5);

/// How the executor dispatches admitted updates in one drive (issue #2035).
#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateDispatch {
    /// The end of the workflow-task budget. With `None`, the budget is off and
    /// the window is a fixed 30 seconds.
    pub(crate) cutoff: Option<DateTime<Utc>>,
    /// Whether the first update may run after the window closes. Only the first
    /// drive of a decision sets it. A re-drive of the same decision shares the
    /// budget, so it must not run another update past the window.
    pub(crate) run_first: bool,
}

/// The dispatch window when the task has no budget (issue #2035).
///
/// An operator can turn the workflow-task timeout off. A burst of slow
/// handlers could then hold the task for a long time before any result is
/// persisted. This window ends the batch, and the worker wakes the task for
/// the rest.
const UNBUDGETED_WINDOW: Duration = Duration::from_secs(30);

/// Pinned boxed async future returned by an update handler invocation.
pub type UpdateHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;

/// Type-erased async update handler: takes JSON input, returns JSON result or string error.
pub type BoxUpdateHandler = Arc<dyn Fn(Value) -> UpdateHandlerFuture + Send + Sync>;

/// Type-erased synchronous validator: takes JSON input, returns Ok or a rejection reason.
pub type BoxUpdateValidator = Arc<dyn Fn(&Value) -> Result<(), String> + Send + Sync>;

/// An entry in the update registry.
struct UpdateEntry {
    validator: Option<BoxUpdateValidator>,
    handler: BoxUpdateHandler,
}

/// In-memory registry of update handlers and their optional validators.
///
/// Stored on [`WorkflowContext`](crate::context::WorkflowContext) behind a
/// `Mutex`. Registration is idempotent — the first registration wins and
/// subsequent calls with the same `name` are ignored.
#[derive(Default)]
pub struct UpdateRegistry {
    entries: HashMap<String, UpdateEntry>,
}

impl UpdateRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a handler (and optional validator) under `name`.
    ///
    /// If `name` is already registered, this is a no-op (first registration wins).
    pub fn register(
        &mut self,
        name: &str,
        validator: Option<BoxUpdateValidator>,
        handler: BoxUpdateHandler,
    ) {
        // Idempotent: first registration wins.
        self.entries
            .entry(name.to_string())
            .or_insert(UpdateEntry { validator, handler });
    }

    /// Returns `true` if a handler is registered under `name`.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Run the validator for `name` against `input`.
    ///
    /// # Errors
    ///
    /// - Returns `Err("update handler 'name' not found")` if `name` is unknown.
    /// - Returns `Err(reason)` if the validator rejects.
    pub fn validate(&self, name: &str, input: &Value) -> Result<(), String> {
        let entry = self
            .entries
            .get(name)
            .ok_or_else(|| format!("update handler '{name}' not found"))?;

        if let Some(validator) = &entry.validator {
            validator(input)?;
        }
        Ok(())
    }

    /// Invoke the handler for `name` with `input`.
    ///
    /// Returns `None` if `name` is not registered.
    #[must_use]
    pub fn invoke(&self, name: &str, input: Value) -> Option<UpdateHandlerFuture> {
        self.entries.get(name).map(|e| (e.handler)(input))
    }

    /// Returns the validator for `name`, if registered and present.
    #[must_use]
    pub fn get_validator(&self, name: &str) -> Option<BoxUpdateValidator> {
        self.entries.get(name).and_then(|e| e.validator.clone())
    }

    /// Returns the handler for `name`, if registered.
    #[must_use]
    pub fn get_handler(&self, name: &str) -> Option<BoxUpdateHandler> {
        self.entries.get(name).map(|e| e.handler.clone())
    }
}

/// Runs the admitted updates that have no result and adds their results to the
/// cycle commands (issue #2035).
///
/// The executor calls this after the body parks or returns. The handlers of
/// the body then exist, and the cycle commands are known. A suspended cycle
/// keeps the results in its commands. A terminal cycle keeps them in
/// `pending`, and the worker records them before the final event.
///
/// A cycle whose commands the worker discards runs no handler. A dispatch in
/// such a cycle loses its result, and the repeat of the cycle runs the handler
/// again. Those cycles are a divergence, a panic and a deadlock.
///
/// The cycle runs under the workflow-task budget. The dispatch therefore
/// starts a handler only while a full [`UPDATE_HANDLER_TIMEOUT`] is left in
/// half of the remaining budget. The first update of a decision always runs,
/// so each decision makes progress. Every handler stops at half of the budget
/// that is left, so the persist keeps the other half.
///
/// Returns `true` when updates remain. The caller must wake the task, so a
/// later cycle runs them.
pub(crate) async fn dispatch_pending_updates(
    ctx: &WorkflowContext,
    outcome: &mut WorkflowOutcome,
    pending: &mut Vec<WorkflowCommand>,
) -> bool {
    let Some(dispatch) = ctx.update_dispatch() else {
        return false;
    };
    let commands = match outcome {
        WorkflowOutcome::Suspended { commands } => commands,
        WorkflowOutcome::Completed { .. }
        | WorkflowOutcome::ContinuedAsNew { .. }
        | WorkflowOutcome::Failed {
            non_deterministic_details: None,
            handler_panic: false,
            ..
        } => pending,
        WorkflowOutcome::Failed { .. } | WorkflowOutcome::TaskFailed { .. } => return false,
    };
    // The body can run an update with `execute_admitted_update`. The batch
    // then holds its result, and the handler must not run twice.
    let mut recorded: HashSet<UpdateId> = commands
        .iter()
        .filter_map(|command| match command {
            WorkflowCommand::RecordUpdateResult { update_id, .. } => Some(*update_id),
            _ => None,
        })
        .collect();
    let window = window_end(dispatch.cutoff);
    let mut started = !dispatch.run_first;
    for (update_id, name, input) in ctx.pending_updates() {
        if !recorded.insert(update_id) {
            continue;
        }
        if started && !window_allows(window) {
            return true;
        }
        started = true;
        let result = run_bounded(ctx, &name, input, handler_limit(dispatch.cutoff)).await;
        commands.push(WorkflowCommand::RecordUpdateResult { update_id, result });
    }
    false
}

/// The end of the dispatch window.
///
/// The window is half of the budget that is left. The persist needs the rest.
/// A task with no budget gets a fixed [`UNBUDGETED_WINDOW`].
fn window_end(cutoff: Option<DateTime<Utc>>) -> tokio::time::Instant {
    let window = cutoff.map_or(UNBUDGETED_WINDOW, |cutoff| {
        (cutoff - Utc::now()).to_std().unwrap_or(Duration::ZERO) / 2
    });
    tokio::time::Instant::now() + window
}

/// The time that one handler may run.
///
/// It is [`UPDATE_HANDLER_TIMEOUT`], or half of what is left of the budget when
/// that is less. A body that used most of the budget thus cannot push the cycle
/// past its cut-off, and the persist keeps the other half.
fn handler_limit(cutoff: Option<DateTime<Utc>>) -> Duration {
    cutoff.map_or(UPDATE_HANDLER_TIMEOUT, |cutoff| {
        let left = (cutoff - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        UPDATE_HANDLER_TIMEOUT.min(left / 2)
    })
}

/// Whether a handler may start. It needs a full [`UPDATE_HANDLER_TIMEOUT`].
fn window_allows(end: tokio::time::Instant) -> bool {
    end.saturating_duration_since(tokio::time::Instant::now()) >= UPDATE_HANDLER_TIMEOUT
}

/// Runs one handler. A panic or a timeout becomes an error result.
///
/// The error text is part of the history and the HTTP answer. A panic
/// message can hold internal detail, so the text is fixed and the log keeps
/// the detail.
async fn run_bounded(
    ctx: &WorkflowContext,
    name: &str,
    input: Value,
    limit: Duration,
) -> Result<Value, String> {
    use futures::FutureExt as _;

    // The wrapped future builds the handler future on its first poll. One
    // `catch_unwind` call therefore covers a panic in the build and a panic in
    // the poll.
    let run = std::panic::AssertUnwindSafe(ctx.run_update_handler(name, input)).catch_unwind();
    match tokio::time::timeout(limit, run).await {
        Ok(Ok(result)) => result,
        Ok(Err(panic)) => {
            tracing::error!(
                update = name,
                panic_message = %crate::error::panic_message(panic),
                "update handler panicked"
            );
            Err("update handler panicked".to_string())
        }
        Err(_elapsed) => Err(format!(
            "update handler timed out after {:?}",
            Duration::from_millis(u64::try_from(limit.as_millis()).unwrap_or(u64::MAX))
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_register_and_invoke_update_handler_successfully() {
        let mut registry = UpdateRegistry::new();
        assert!(
            !registry.contains("my_update"),
            "registry should be initially empty"
        );

        let validator = Arc::new(|val: &Value| {
            if val == &serde_json::json!("valid") {
                Ok(())
            } else {
                Err("invalid input".to_string())
            }
        });

        let handler = Arc::new(|val: Value| {
            Box::pin(async move {
                if val == serde_json::json!("valid") {
                    Ok(serde_json::json!("success"))
                } else {
                    Err("handler error".to_string())
                }
            }) as UpdateHandlerFuture
        });

        registry.register("my_update", Some(validator.clone()), handler.clone());
        assert!(
            registry.contains("my_update"),
            "registry should contain my_update after registration"
        );

        // Second registration is a no-op
        registry.register("my_update", None, handler.clone());

        // Test validate
        assert_eq!(
            registry
                .validate("unknown", &serde_json::json!("valid"))
                .unwrap_err(),
            "update handler 'unknown' not found",
            "validation should fail for unknown handler"
        );
        assert_eq!(
            registry
                .validate("my_update", &serde_json::json!("invalid"))
                .unwrap_err(),
            "invalid input",
            "validation should fail when validator rejects"
        );
        assert!(
            registry
                .validate("my_update", &serde_json::json!("valid"))
                .is_ok(),
            "validation should succeed when validator accepts"
        );

        // Test invoke
        assert!(
            registry
                .invoke("unknown", serde_json::json!("valid"))
                .is_none(),
            "invocation should return None for unknown handler"
        );

        let _future = registry
            .invoke("my_update", serde_json::json!("valid"))
            .expect("handler should exist");
    }

    #[test]
    fn should_succeed_validation_when_no_validator_provided() {
        let mut registry = UpdateRegistry::new();

        let handler =
            Arc::new(|val: Value| Box::pin(async move { Ok(val) }) as UpdateHandlerFuture);

        registry.register("my_update", None, handler);
        assert!(
            registry
                .validate("my_update", &serde_json::json!("valid"))
                .is_ok(),
            "validation should trivially succeed if no validator is provided"
        );
    }

    // ── Dispatch of admitted updates (issue #2035) ────────────────────

    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use serde_json::json;

    use crate::event::WorkflowEvent;
    use crate::info::UpdateHandlerInfo;
    use crate::types::{ActivityExecId, ExecutionId, UpdateId};

    type BodyFuture<'a> =
        Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn admitted(update_id: UpdateId, name: &str, input: Value) -> WorkflowEvent {
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: name.to_string(),
            input,
            timestamp: Utc::now(),
        }
    }

    fn info(name: &'static str, handler: crate::info::UpdateHandlerFn) -> UpdateHandlerInfo {
        UpdateHandlerInfo {
            name,
            workflow: "wf",
            module: "test",
            input_type_hint: "",
            output_type_hint: "",
            has_validator: false,
            handler,
            validator: None,
            mcp: false,
            description: None,
            arg_schema: None,
            response_schema: None,
        }
    }

    /// A worker-style context: declarative handlers registered, dispatch on or off.
    fn context(
        history: Vec<WorkflowEvent>,
        handlers: &[&UpdateHandlerInfo],
        dispatch: bool,
    ) -> WorkflowContext {
        let ctx = crate::executor::default_task_context(ExecutionId::new(), history);
        for handler in handlers {
            ctx.register_declarative_update_handler(handler);
        }
        if dispatch {
            ctx.with_update_dispatch(UpdateDispatch {
                cutoff: None,
                run_first: true,
            })
        } else {
            ctx
        }
    }

    /// A worker-style context whose task budget ends at `cutoff`.
    fn context_with_cutoff(
        history: Vec<WorkflowEvent>,
        handlers: &[&UpdateHandlerInfo],
        cutoff: DateTime<Utc>,
    ) -> WorkflowContext {
        let ctx = crate::executor::default_task_context(ExecutionId::new(), history);
        for handler in handlers {
            ctx.register_declarative_update_handler(handler);
        }
        ctx.with_update_dispatch(UpdateDispatch {
            cutoff: Some(cutoff),
            run_first: true,
        })
    }

    /// A context for a re-drive of a decision that already ran an update.
    fn redrive_context(
        history: Vec<WorkflowEvent>,
        handlers: &[&UpdateHandlerInfo],
        cutoff: DateTime<Utc>,
    ) -> WorkflowContext {
        let ctx = crate::executor::default_task_context(ExecutionId::new(), history);
        for handler in handlers {
            ctx.register_declarative_update_handler(handler);
        }
        ctx.with_update_dispatch(UpdateDispatch {
            cutoff: Some(cutoff),
            run_first: false,
        })
    }

    async fn drive_full(
        ctx: WorkflowContext,
        body: crate::info::WorkflowHandlerFn,
    ) -> crate::executor::DriveResult {
        crate::executor::drive_workflow_keep(ctx, body, Value::Null, None, None).await
    }

    async fn drive(
        ctx: WorkflowContext,
        body: crate::info::WorkflowHandlerFn,
    ) -> (WorkflowOutcome, Vec<WorkflowCommand>) {
        let drive = drive_full(ctx, body).await;
        (drive.outcome, drive.pending)
    }

    /// Every `RecordUpdateResult` in the cycle batch, in order.
    fn results(
        outcome: &WorkflowOutcome,
        pending: &[WorkflowCommand],
    ) -> Vec<(UpdateId, Result<Value, String>)> {
        let commands = match outcome {
            WorkflowOutcome::Suspended { commands } => commands.as_slice(),
            _ => pending,
        };
        commands
            .iter()
            .filter_map(|command| match command {
                WorkflowCommand::RecordUpdateResult { update_id, result } => {
                    Some((*update_id, result.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn parked(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move {
            ctx.wait_for_signal("go")
                .await
                .map_err(|error| error.to_string())
        })
    }

    static SUSPEND_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn echo_once(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        SUSPEND_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(json!({ "echo": input })) })
    }

    #[tokio::test]
    async fn a_suspended_cycle_runs_a_pending_update_once() {
        let id = UpdateId::new();
        let handler = info("echo", echo_once);
        let history = vec![started(), admitted(id, "echo", json!({ "n": 1 }))];
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert_eq!(
            results(&outcome, &pending),
            vec![(id, Ok(json!({ "echo": { "n": 1 } })))]
        );
        assert_eq!(SUSPEND_RUNS.load(Ordering::SeqCst), 1);
    }

    static REPLAY_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_replay(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        REPLAY_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn a_recorded_result_is_not_run_again() {
        let id = UpdateId::new();
        let handler = info("count", count_replay);
        let history = vec![
            started(),
            admitted(id, "count", json!(1)),
            WorkflowEvent::UpdateCompleted {
                update_id: id,
                output: json!(1),
            },
        ];
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert_eq!(results(&outcome, &pending), Vec::new());
        assert_eq!(REPLAY_RUNS.load(Ordering::SeqCst), 0);
    }

    static OFF_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_off(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        OFF_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn a_context_without_dispatch_runs_no_handler() {
        let id = UpdateId::new();
        let handler = info("count", count_off);
        let history = vec![started(), admitted(id, "count", json!(1))];
        let (outcome, pending) = drive(context(history, &[&handler], false), parked).await;

        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert_eq!(results(&outcome, &pending), Vec::new());
        assert_eq!(OFF_RUNS.load(Ordering::SeqCst), 0);
    }

    fn reject(_ctx: Arc<WorkflowContext>, _input: Value) -> UpdateHandlerFuture {
        Box::pin(async move { Err("insufficient funds".to_string()) })
    }

    #[tokio::test]
    async fn a_handler_error_is_recorded_as_a_failure() {
        let id = UpdateId::new();
        let handler = info("pay", reject);
        let history = vec![started(), admitted(id, "pay", json!({}))];
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert_eq!(
            results(&outcome, &pending),
            vec![(id, Err("insufficient funds".to_string()))]
        );
    }

    fn panic_in_future(_ctx: Arc<WorkflowContext>, _input: Value) -> UpdateHandlerFuture {
        Box::pin(async move { panic!("boom in future") })
    }

    fn panic_in_construction(_ctx: Arc<WorkflowContext>, _input: Value) -> UpdateHandlerFuture {
        panic!("boom in construction")
    }

    #[tokio::test]
    async fn a_handler_panic_is_recorded_as_a_failure() {
        for (name, handler, needle) in [
            (
                "future",
                panic_in_future as crate::info::UpdateHandlerFn,
                "boom in future",
            ),
            ("build", panic_in_construction, "boom in construction"),
        ] {
            let id = UpdateId::new();
            let registered = info(name, handler);
            let history = vec![started(), admitted(id, name, json!({}))];
            let (outcome, pending) = drive(context(history, &[&registered], true), parked).await;

            let recorded = results(&outcome, &pending);
            assert_eq!(recorded.len(), 1, "{name}: one result");
            let error = recorded[0].1.clone().expect_err("a panic is a failure");
            assert_eq!(error, "update handler panicked", "{name}");
            assert!(
                !error.contains(needle),
                "{name}: the panic text stays out of history"
            );
        }
    }

    fn never_finishes(_ctx: Arc<WorkflowContext>, _input: Value) -> UpdateHandlerFuture {
        Box::pin(std::future::pending())
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_that_never_finishes_records_a_timeout() {
        let id = UpdateId::new();
        let handler = info("hang", never_finishes);
        let history = vec![started(), admitted(id, "hang", json!({}))];
        let started = tokio::time::Instant::now();
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        assert_eq!(
            results(&outcome, &pending),
            vec![(id, Err("update handler timed out after 5s".to_string()))]
        );
        assert!(started.elapsed() >= UPDATE_HANDLER_TIMEOUT);
    }

    #[tokio::test]
    async fn an_unknown_update_name_records_not_found() {
        let id = UpdateId::new();
        let history = vec![started(), admitted(id, "ghost", json!({}))];
        let (outcome, pending) = drive(context(history, &[], true), parked).await;

        assert_eq!(
            results(&outcome, &pending),
            vec![(id, Err("update handler 'ghost' not found".to_string()))]
        );
    }

    fn echo_order(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn updates_run_in_history_order() {
        let (first, second, third) = (UpdateId::new(), UpdateId::new(), UpdateId::new());
        let handler = info("order", echo_order);
        let history = vec![
            started(),
            admitted(first, "order", json!(1)),
            admitted(second, "order", json!(2)),
            admitted(third, "order", json!(3)),
        ];
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert_eq!(
            results(&outcome, &pending),
            vec![
                (first, Ok(json!(1))),
                (second, Ok(json!(2))),
                (third, Ok(json!(3)))
            ]
        );
    }

    fn finished(_ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move { Ok(json!("done")) })
    }

    fn failing(_ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move { Err("business rule".to_string()) })
    }

    #[tokio::test]
    async fn a_terminal_cycle_records_pending_updates_with_its_commands() {
        for (label, body) in [
            ("completed", finished as crate::info::WorkflowHandlerFn),
            ("failed", failing),
        ] {
            let id = UpdateId::new();
            let handler = info("order", echo_order);
            let history = vec![started(), admitted(id, "order", json!(7))];
            let (outcome, pending) = drive(context(history, &[&handler], true), body).await;

            assert!(
                matches!(
                    outcome,
                    WorkflowOutcome::Completed { .. } | WorkflowOutcome::Failed { .. }
                ),
                "{label}: {outcome:?}"
            );
            assert_eq!(
                results(&outcome, &pending),
                vec![(id, Ok(json!(7)))],
                "{label}"
            );
        }
    }

    fn continue_then_park(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move {
            ctx.continue_as_new(json!(1))
                .await
                .map_err(|error| error.to_string())?;
            Ok(Value::Null)
        })
    }

    #[tokio::test]
    async fn a_continue_as_new_cycle_records_pending_updates() {
        let id = UpdateId::new();
        let handler = info("order", echo_order);
        let history = vec![started(), admitted(id, "order", json!(9))];
        let (outcome, pending) =
            drive(context(history, &[&handler], true), continue_then_park).await;

        assert!(
            matches!(outcome, WorkflowOutcome::ContinuedAsNew { .. }),
            "{outcome:?}"
        );
        assert_eq!(results(&outcome, &pending), vec![(id, Ok(json!(9)))]);
    }

    static DIVERGE_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_diverge(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        DIVERGE_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn a_divergent_cycle_runs_no_handler() {
        let activity_id = ActivityExecId::new();
        let id = UpdateId::new();
        let handler = info("count", count_diverge);
        let history = vec![
            started(),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send".to_string(),
                input: json!({}),
                queue: "default".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: json!({}),
            },
            admitted(id, "count", json!(1)),
        ];
        let (outcome, pending) = drive(context(history, &[&handler], true), finished).await;

        assert!(
            matches!(
                outcome,
                WorkflowOutcome::Failed {
                    non_deterministic_details: Some(_),
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert_eq!(results(&outcome, &pending), Vec::new());
        assert_eq!(DIVERGE_RUNS.load(Ordering::SeqCst), 0);
    }

    static PANIC_BODY_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_panic_body(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        PANIC_BODY_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    fn panicking_body(_ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move { panic!("boom in body") })
    }

    #[tokio::test]
    async fn a_panicking_body_runs_no_handler() {
        let id = UpdateId::new();
        let handler = info("count", count_panic_body);
        let history = vec![started(), admitted(id, "count", json!(1))];
        let (outcome, pending) = drive(context(history, &[&handler], true), panicking_body).await;

        assert!(
            matches!(
                outcome,
                WorkflowOutcome::Failed {
                    handler_panic: true,
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert_eq!(results(&outcome, &pending), Vec::new());
        assert_eq!(PANIC_BODY_RUNS.load(Ordering::SeqCst), 0);
    }

    const BODY_DRIVEN_ID: uuid::Uuid =
        uuid::Uuid::from_u128(0x2035_0000_0000_7000_8000_0000_0000_0001);
    static BODY_DRIVEN_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_body_driven(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        BODY_DRIVEN_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    fn drives_its_own_update(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move {
            let _ = ctx
                .execute_admitted_update(
                    UpdateId::from_uuid(BODY_DRIVEN_ID),
                    "count",
                    json!("mine"),
                )
                .await;
            ctx.wait_for_signal("go")
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[tokio::test]
    async fn a_body_driven_update_runs_once() {
        let id = UpdateId::from_uuid(BODY_DRIVEN_ID);
        let handler = info("count", count_body_driven);
        let history = vec![started(), admitted(id, "count", json!("mine"))];
        let (outcome, pending) =
            drive(context(history, &[&handler], true), drives_its_own_update).await;

        assert_eq!(results(&outcome, &pending), vec![(id, Ok(json!("mine")))]);
        assert_eq!(BODY_DRIVEN_RUNS.load(Ordering::SeqCst), 1);
    }

    static LATE_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn registers_late(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move {
            ctx.register_update_handler_no_validator("late", |input: Value| async move {
                LATE_RUNS.fetch_add(1, Ordering::SeqCst);
                Ok::<Value, String>(input)
            });
            ctx.wait_for_signal("go")
                .await
                .map_err(|error| error.to_string())
        })
    }

    #[tokio::test]
    async fn a_body_registered_handler_runs() {
        let id = UpdateId::new();
        let history = vec![started(), admitted(id, "late", json!("hi"))];
        let (outcome, pending) = drive(context(history, &[], true), registers_late).await;

        assert_eq!(results(&outcome, &pending), vec![(id, Ok(json!("hi")))]);
        assert_eq!(LATE_RUNS.load(Ordering::SeqCst), 1);
    }

    static NEXT_CYCLE_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_next_cycle(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        NEXT_CYCLE_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    /// The events that the worker writes for the `RecordUpdateResult` commands.
    fn result_events(recorded: &[(UpdateId, Result<Value, String>)]) -> Vec<WorkflowEvent> {
        recorded
            .iter()
            .map(|(update_id, result)| match result {
                Ok(output) => WorkflowEvent::UpdateCompleted {
                    update_id: *update_id,
                    output: output.clone(),
                },
                Err(error) => WorkflowEvent::UpdateFailed {
                    update_id: *update_id,
                    error: error.clone(),
                },
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn the_results_of_one_cycle_stop_the_next_cycle_from_running_them() {
        let ok = info("ok", count_next_cycle);
        let pay = info("pay", reject);
        let hang = info("hang", never_finishes);
        let boom = info("boom", panic_in_future);
        let handlers = [&ok, &pay, &hang, &boom];
        let mut history = vec![started()];
        for name in ["ok", "pay", "hang", "boom", "ghost"] {
            history.push(admitted(UpdateId::new(), name, json!({})));
        }

        let first = drive_full(context(history.clone(), &handlers, true), parked).await;
        let recorded = results(&first.outcome, &first.pending);
        assert_eq!(recorded.len(), 5, "cycle one runs every update");
        history.extend(result_events(&recorded));

        let second = drive_full(context(history, &handlers, true), parked).await;
        assert!(matches!(second.outcome, WorkflowOutcome::Suspended { .. }));
        assert_eq!(results(&second.outcome, &second.pending), Vec::new());
        assert!(!second.updates_deferred);
        assert_eq!(NEXT_CYCLE_RUNS.load(Ordering::SeqCst), 1);
    }

    static CLOSED_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_closed(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        CLOSED_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn a_closed_window_still_runs_the_first_update_and_defers_the_rest() {
        let (first, second, third) = (UpdateId::new(), UpdateId::new(), UpdateId::new());
        let handler = info("count", count_closed);
        let history = vec![
            started(),
            admitted(first, "count", json!(1)),
            admitted(second, "count", json!(2)),
            admitted(third, "count", json!(3)),
        ];
        let cutoff = Utc::now() - chrono::Duration::seconds(1);
        let drive = drive_full(context_with_cutoff(history, &[&handler], cutoff), parked).await;

        assert_eq!(
            results(&drive.outcome, &drive.pending),
            vec![(first, Ok(json!(1)))]
        );
        assert!(drive.updates_deferred);
        assert_eq!(CLOSED_RUNS.load(Ordering::SeqCst), 1);
    }

    /// With 4 s of budget left and a closed window, the first update still runs.
    /// Its limit is half of what is left, so it cannot pass the cut-off.
    #[tokio::test(start_paused = true)]
    async fn the_first_update_is_bounded_by_the_budget_that_is_left() {
        let id = UpdateId::new();
        let handler = info("hang", never_finishes);
        let history = vec![started(), admitted(id, "hang", json!({}))];
        let cutoff = Utc::now() + chrono::Duration::seconds(4);
        let began = tokio::time::Instant::now();
        let drive = drive_full(context_with_cutoff(history, &[&handler], cutoff), parked).await;

        let recorded = results(&drive.outcome, &drive.pending);
        assert_eq!(recorded.len(), 1);
        let error = recorded[0].1.clone().expect_err("a timeout is a failure");
        assert!(
            error.starts_with("update handler timed out after "),
            "{error}"
        );
        assert!(
            began.elapsed() < Duration::from_secs(3),
            "the handler stops before the 5 s limit: {:?}",
            began.elapsed()
        );
    }

    fn four_seconds_uncounted(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(input)
        })
    }

    /// With no task budget the window is 30 s. Handlers start at 0 s, 4 s and
    /// so on up to 24 s, when 6 s are left. At 28 s only 2 s are left.
    #[tokio::test(start_paused = true)]
    async fn a_task_without_a_budget_still_stops_a_burst_at_the_fallback_window() {
        let ids: Vec<UpdateId> = (0..10).map(|_| UpdateId::new()).collect();
        let handler = info("slow", four_seconds_uncounted);
        let mut history = vec![started()];
        for (n, id) in ids.iter().enumerate() {
            history.push(admitted(*id, "slow", json!(n)));
        }
        let drive = drive_full(context(history, &[&handler], true), parked).await;

        let recorded = results(&drive.outcome, &drive.pending);
        let ran: Vec<UpdateId> = recorded.iter().map(|(id, _)| *id).collect();
        assert_eq!(ran, ids[..7].to_vec(), "seven run, in order");
        assert!(drive.updates_deferred);
    }

    /// Consumes one signal, then waits on an activity. Such a wait can stay
    /// resident, and a redundant wake resumes it warm.
    fn signal_then_activity(ctx: &WorkflowContext, _input: Value) -> BodyFuture<'_> {
        Box::pin(async move {
            ctx.wait_for_signal("a")
                .await
                .map_err(|error| error.to_string())?;
            ctx.execute_activity_raw("probe", Value::Null, "default")
                .await
                .map_err(|error| error.to_string())
        })
    }

    /// A re-drive with a closed window defers and runs no update, so its batch
    /// holds no `RecordUpdateResult`. Such a cycle can stay resident. A redundant
    /// wake resumes an activity wait warm, and a warm cycle skips the dispatch,
    /// so the update would wait for an unrelated event. The cycle must go cold.
    #[tokio::test(start_paused = true)]
    async fn a_cycle_that_defers_an_update_does_not_stay_resident() {
        let id = UpdateId::new();
        let handler = info("quick", echo_once);
        let history = vec![
            started(),
            admitted(id, "quick", json!({})),
            WorkflowEvent::SignalReceived {
                signal_name: "a".to_string(),
                payload: json!({}),
            },
        ];
        // 4 s left: half of it is under one handler limit, so the window is closed.
        let cutoff = Utc::now() + chrono::Duration::seconds(4);
        let ctx = redrive_context(history, &[&handler], cutoff);
        let key = crate::resident::ResidentKey::new(
            signal_then_activity,
            None,
            &std::collections::HashMap::new(),
        );
        let drive = crate::executor::drive_workflow_keep(
            ctx,
            signal_then_activity,
            Value::Null,
            None,
            Some(key),
        )
        .await;

        assert!(
            drive.updates_deferred,
            "the closed window defers the update"
        );
        let ran: Vec<UpdateId> = results(&drive.outcome, &drive.pending)
            .into_iter()
            .map(|(update_id, _)| update_id)
            .collect();
        assert_eq!(
            ran,
            Vec::<UpdateId>::new(),
            "the closed window runs no update"
        );
        assert!(
            !drive.resident.is_parked(),
            "a deferred update needs a cold cycle to run"
        );
    }

    static REDRIVE_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_redrive(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        REDRIVE_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn a_redrive_with_a_closed_window_runs_no_update() {
        let id = UpdateId::new();
        let handler = info("count", count_redrive);
        let history = vec![started(), admitted(id, "count", json!(1))];
        let cutoff = Utc::now() - chrono::Duration::seconds(1);
        let drive = drive_full(redrive_context(history, &[&handler], cutoff), parked).await;

        assert_eq!(results(&drive.outcome, &drive.pending), Vec::new());
        assert!(drive.updates_deferred);
        assert_eq!(REDRIVE_RUNS.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_open_window_runs_every_pending_update() {
        let ids = [UpdateId::new(), UpdateId::new(), UpdateId::new()];
        let handler = info("order", echo_order);
        let mut history = vec![started()];
        for (n, id) in ids.iter().enumerate() {
            history.push(admitted(*id, "order", json!(n)));
        }
        let cutoff = Utc::now() + chrono::Duration::hours(1);
        let drive = drive_full(context_with_cutoff(history, &[&handler], cutoff), parked).await;

        assert_eq!(results(&drive.outcome, &drive.pending).len(), 3);
        assert!(!drive.updates_deferred);
    }

    static REPEAT_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_repeat(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        REPEAT_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(input) })
    }

    #[tokio::test]
    async fn an_update_id_that_history_repeats_runs_once() {
        let id = UpdateId::new();
        let handler = info("count", count_repeat);
        let history = vec![
            started(),
            admitted(id, "count", json!(1)),
            admitted(id, "count", json!(1)),
        ];
        let (outcome, pending) = drive(context(history, &[&handler], true), parked).await;

        assert_eq!(results(&outcome, &pending), vec![(id, Ok(json!(1)))]);
        assert_eq!(REPEAT_RUNS.load(Ordering::SeqCst), 1);
    }

    static WINDOW_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn four_seconds(_ctx: Arc<WorkflowContext>, input: Value) -> UpdateHandlerFuture {
        WINDOW_RUNS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(4)).await;
            Ok(input)
        })
    }

    /// A 30 s budget gives a 15 s window. Handlers start at 0 s, 4 s and 8 s.
    /// At 12 s only 3 s are left, which is less than one handler limit.
    #[tokio::test(start_paused = true)]
    async fn the_window_closes_when_less_than_one_handler_limit_is_left() {
        let ids: Vec<UpdateId> = (0..5).map(|_| UpdateId::new()).collect();
        let handler = info("slow", four_seconds);
        let mut history = vec![started()];
        for (n, id) in ids.iter().enumerate() {
            history.push(admitted(*id, "slow", json!(n)));
        }
        let cutoff = Utc::now() + chrono::Duration::seconds(30);
        let drive = drive_full(context_with_cutoff(history, &[&handler], cutoff), parked).await;

        let recorded = results(&drive.outcome, &drive.pending);
        let ran: Vec<UpdateId> = recorded.iter().map(|(id, _)| *id).collect();
        assert_eq!(ran, ids[..3].to_vec(), "the first three run, in order");
        assert!(drive.updates_deferred);
        assert_eq!(WINDOW_RUNS.load(Ordering::SeqCst), 3);
    }
}
