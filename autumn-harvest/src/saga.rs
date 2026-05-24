//! Saga compensation helper for workflow code.
//!
//! A [`Saga`] records compensating actions for successful forward steps. If a
//! later step fails, the recorded actions run in reverse order before the
//! original error is returned.
//!
//! See [`docs/saga.md`](https://github.com/madmax983/autumn-harvest/blob/trunk/docs/saga.md)
//! for the full cancellation + idempotency contract.

use std::future::Future;

use futures::future::{BoxFuture, FutureExt};

use crate::context::WorkflowContext;
use crate::error::{HarvestError, HarvestResult};

type Compensation<'ctx> = Box<dyn FnOnce() -> BoxFuture<'ctx, HarvestResult<()>> + Send + 'ctx>;

/// Builder for saga-style workflows with explicit compensating actions.
///
/// Each successful [`step`](Self::step) pushes a compensation callback. If a
/// later step returns an error, all previously recorded callbacks run in LIFO
/// order. Compensation actions are ordinary workflow actions, so calling
/// `ctx.execute_activity_raw(...)` inside a compensation records the activity in
/// workflow history just like any other activity.
///
/// For the full narrative, worked examples, and test coverage table see
/// [`docs/saga.md`](https://github.com/madmax983/autumn-harvest/blob/trunk/docs/saga.md).
///
/// # Cancellation interaction
///
/// **Cancellation does not auto-compensate.** When an operator calls
/// `cancel_workflow_execution`, a `WorkflowCancelled` event is appended to
/// history and the executor replays the workflow function with a context where
/// [`WorkflowContext::is_cancelled`] returns `true`.  The `Saga` struct never
/// observes this directly; its compensation stack is left intact.  The workflow
/// author must check for cancellation and invoke [`compensate_all`](Self::compensate_all)
/// explicitly:
///
/// ```rust,ignore
/// // Recommended pattern — check for cancellation after each step or at the end.
/// if ctx.is_cancelled() {
///     saga.compensate_all().await?;
///     return Err(HarvestError::Cancelled("workflow cancelled".into()));
/// }
/// ```
///
/// This matches Temporal's documented model and avoids surprising partial-unwind
/// behaviour in long sagas where automatic, silent compensation could be worse
/// than no compensation at all.
///
/// # Idempotency contract
///
/// Compensation closures are re-registered on **every** workflow replay.  If a
/// worker crashes mid-[`compensate_all`](Self::compensate_all), the next worker
/// replays the workflow function from scratch, re-registers all compensations,
/// and calls `compensate_all()` again — including compensations that already ran
/// before the crash.
///
/// **Compensation activities must therefore be idempotent.**
///
/// * **Good — release by ID:** `release_reservation("rsv-abc")` is a no-op when
///   the reservation is already released.  Running it twice is safe.
/// * **Bad — release most-recent:** `release_last_reservation()` on a second
///   invocation would release a *different* reservation that may belong to
///   another order entirely.
///
/// # Replay-determinism contract
///
/// The `compensate` closure receives the forward step's `T` result, which on
/// replay is returned from the recorded `ActivityCompleted` event rather than
/// re-executing the activity.  Any non-deterministic or side-effecting logic
/// placed *directly inside* the compensation closure (rather than inside an
/// activity invoked by the closure) will break replay.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::context::WorkflowContext;
/// use autumn_harvest::saga::Saga;
/// use autumn_harvest::ExecutionId;
///
/// // Assuming ctx is a valid &WorkflowContext from your workflow function
/// # let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
/// let mut saga = Saga::new(&ctx);
///
/// // You can now use `saga.step` to build compensated operations.
/// ```
pub struct Saga<'ctx> {
    ctx: &'ctx WorkflowContext,
    compensations: Vec<Compensation<'ctx>>,
}

impl<'ctx> Saga<'ctx> {
    /// Create a saga builder tied to the current workflow context.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::saga::Saga;
    ///
    /// # let ctx = WorkflowContext::new_test();
    /// let saga = Saga::new(&ctx);
    /// ```
    #[must_use]
    pub const fn new(ctx: &'ctx WorkflowContext) -> Self {
        Self {
            ctx,
            compensations: Vec::new(),
        }
    }

    /// Return the workflow context this saga is associated with.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::saga::Saga;
    ///
    /// # let ctx = WorkflowContext::new_test();
    /// let saga = Saga::new(&ctx);
    /// assert!(!saga.context().is_cancelled());
    /// ```
    #[must_use]
    pub const fn context(&self) -> &'ctx WorkflowContext {
        self.ctx
    }

    /// Return the number of successful steps that can still be compensated.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::saga::Saga;
    ///
    /// # let ctx = WorkflowContext::new_test();
    /// let saga = Saga::new(&ctx);
    /// assert_eq!(saga.pending_compensation_count(), 0);
    /// ```
    #[must_use]
    pub fn pending_compensation_count(&self) -> usize {
        self.compensations.len()
    }

    /// Run one forward step and register its compensation on success.
    ///
    /// If `step` fails, previously registered compensations run in reverse
    /// (LIFO) order.  When all compensations succeed, the original step error is
    /// returned.  When any compensation fails, all remaining compensations are
    /// still attempted before returning
    /// [`HarvestError::SagaCompensationFailed`].
    ///
    /// **Cancellation:** calling this method when
    /// [`WorkflowContext::is_cancelled`] is `true` does **not** trigger
    /// automatic compensation.  See the [`Saga`] type-level documentation for
    /// the recommended cancel-and-compensate pattern.
    ///
    /// **Idempotency:** the `compensate` closure is re-registered on every
    /// workflow replay; if the worker crashes mid-[`compensate_all`](Self::compensate_all)
    /// the compensation will run again on the next replay.  The compensation
    /// activity must be idempotent (e.g., release-by-id rather than
    /// release-most-recent).
    ///
    /// **Replay determinism:** the `compensate` closure receives the forward
    /// step's `T` result, which on replay is sourced from recorded history
    /// rather than re-executing the activity.  Do not place non-deterministic
    /// side effects directly inside the closure body; invoke an activity via
    /// `ctx.execute_activity_raw(...)` instead.
    ///
    /// # Errors
    ///
    /// Returns the forward step error after successful compensation. Returns
    /// [`HarvestError::SagaCompensationFailed`] if any compensation fails while
    /// unwinding.
    ///
    /// `T` must be [`Clone`] because the forward result is both returned to the
    /// workflow and retained for the compensation callback.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::saga::Saga;
    /// use autumn_harvest::error::HarvestError;
    ///
    /// # async fn doc_example() -> Result<(), HarvestError> {
    /// # let ctx = WorkflowContext::new_test();
    /// let mut saga = Saga::new(&ctx);
    ///
    /// saga.step(
    ///     || async { Ok::<_, HarvestError>("success") },
    ///     |_| async { Ok::<_, HarvestError>(()) },
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "add_step")]
    pub async fn step<T, C, Step, StepFuture, Compensate, CompensationFuture>(
        &mut self,
        step: Step,
        compensate: Compensate,
    ) -> HarvestResult<T>
    where
        T: Clone + Send + 'ctx,
        C: Send + 'ctx,
        Step: FnOnce() -> StepFuture + Send,
        StepFuture: Future<Output = HarvestResult<T>> + Send,
        Compensate: FnOnce(T) -> CompensationFuture + Send + 'ctx,
        CompensationFuture: Future<Output = HarvestResult<C>> + Send + 'ctx,
    {
        match step().await {
            Ok(output) => {
                let compensated_output = output.clone();
                self.compensations.push(Box::new(move || {
                    async move { compensate(compensated_output).await.map(|_| ()) }.boxed()
                }));
                Ok(output)
            }
            Err(error) => Err(self.rollback_after(error).await),
        }
    }

    /// Run all pending compensations in reverse registration order.
    ///
    /// Call this explicitly when the workflow needs to abort after successful
    /// forward steps — either because a later step failed outside the saga, or
    /// because the workflow was cancelled (see [`Saga`] for the recommended
    /// cancel-and-compensate pattern).
    ///
    /// **Cancellation:** `compensate_all` does **not** detect cancellation
    /// automatically.  The author is responsible for calling it after checking
    /// [`WorkflowContext::is_cancelled`].
    ///
    /// **Idempotency under replay:** if the worker crashes after some but not
    /// all compensations have run, the next worker will call `compensate_all`
    /// again on a fresh replay, re-running *all* compensations from the
    /// beginning of the stack.  Compensation activities must therefore be
    /// idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::SagaCompensationFailed`] if any compensation
    /// fails. All pending compensations are still attempted before returning.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::context::WorkflowContext;
    /// use autumn_harvest::saga::Saga;
    /// use autumn_harvest::error::HarvestError;
    ///
    /// # async fn doc_example() -> Result<(), HarvestError> {
    /// # let ctx = WorkflowContext::new_test();
    /// let mut saga = Saga::new(&ctx);
    ///
    /// // simulate a failure or cancellation
    /// saga.compensate_all().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[doc(alias = "compensate")]
    pub async fn compensate_all(&mut self) -> HarvestResult<()> {
        match self.run_compensations().await {
            Ok(()) => Ok(()),
            Err(compensation_errors) => Err(HarvestError::SagaCompensationFailed {
                original: "manual compensation requested".into(),
                compensation_errors,
            }),
        }
    }

    async fn rollback_after(&mut self, original: HarvestError) -> HarvestError {
        let original_message = original.to_string();

        match self.run_compensations().await {
            Ok(()) => original,
            Err(compensation_errors) => HarvestError::SagaCompensationFailed {
                original: original_message,
                compensation_errors,
            },
        }
    }

    async fn run_compensations(&mut self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        while let Some(compensation) = self.compensations.pop() {
            if let Err(error) = compensation().await {
                errors.push(error.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_saga_successful_steps_do_not_compensate() {
        let ctx = WorkflowContext::new_test();
        let mut saga = Saga::new(&ctx);

        let compensated = Arc::new(Mutex::new(false));
        let c = compensated.clone();

        let result = saga
            .step(
                || async { Ok::<_, HarvestError>(42) },
                move |_| {
                    let comp = c;
                    async move {
                        *comp.lock().await = true;
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(saga.pending_compensation_count(), 1);
        assert!(!*compensated.lock().await);
    }
    #[tokio::test]
    async fn test_saga_failing_step_triggers_compensations() {
        let ctx = WorkflowContext::new_test();
        let mut saga = Saga::new(&ctx);

        let comp1 = Arc::new(Mutex::new(false));
        let comp2 = Arc::new(Mutex::new(false));

        let c1 = comp1.clone();
        let _ = saga
            .step(
                || async { Ok::<_, HarvestError>("step1") },
                move |_| {
                    let c = c1;
                    async move {
                        *c.lock().await = true;
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await;

        let c2 = comp2.clone();
        let _ = saga
            .step(
                || async { Ok::<_, HarvestError>("step2") },
                move |_| {
                    let c = c2;
                    async move {
                        *c.lock().await = true;
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await;

        assert_eq!(saga.pending_compensation_count(), 2);

        // Third step fails, triggering compensation
        let err_result: HarvestResult<()> = saga
            .step(
                || async {
                    Err(HarvestError::WorkflowFailed {
                        name: "test".into(),
                        reason: "error".into(),
                    })
                },
                |()| async { Ok::<_, HarvestError>(()) },
            )
            .await;

        assert!(err_result.is_err());
        match err_result.unwrap_err() {
            HarvestError::WorkflowFailed { reason, .. } => assert_eq!(reason, "error"),
            _ => panic!("Expected WorkflowFailed error"),
        }

        // Both compensations should have run
        assert!(*comp1.lock().await);
        assert!(*comp2.lock().await);
        // And pending count should be 0 because compensations were popped
        assert_eq!(saga.pending_compensation_count(), 0);
    }

    #[tokio::test]
    async fn test_saga_compensation_failure_returns_saga_compensation_failed() {
        let ctx = WorkflowContext::new_test();
        let mut saga = Saga::new(&ctx);

        let _ = saga
            .step(
                || async { Ok::<_, HarvestError>("step1") },
                |_| async {
                    Err::<(), _>(HarvestError::WorkflowFailed {
                        name: "comp".into(),
                        reason: "comp error".into(),
                    })
                },
            )
            .await;

        let err_result: HarvestResult<()> = saga
            .step(
                || async {
                    Err(HarvestError::WorkflowFailed {
                        name: "test".into(),
                        reason: "step2 error".into(),
                    })
                },
                |()| async { Ok::<_, HarvestError>(()) },
            )
            .await;

        assert!(err_result.is_err());
        let err = err_result.unwrap_err();
        match err {
            HarvestError::SagaCompensationFailed {
                original,
                compensation_errors,
            } => {
                assert!(original.contains("step2 error"));
                assert_eq!(compensation_errors.len(), 1);
                assert!(compensation_errors[0].contains("comp error"));
            }
            _ => panic!("Expected SagaCompensationFailed error"),
        }
    }

    #[tokio::test]
    async fn test_saga_compensate_all() {
        let ctx = WorkflowContext::new_test();
        let mut saga = Saga::new(&ctx);

        let comp1 = Arc::new(Mutex::new(false));
        let c1 = comp1.clone();

        let _ = saga
            .step(
                || async { Ok::<_, HarvestError>("step1") },
                move |_| {
                    let c = c1;
                    async move {
                        *c.lock().await = true;
                        Ok::<_, HarvestError>(())
                    }
                },
            )
            .await;

        assert_eq!(saga.pending_compensation_count(), 1);

        let res = saga.compensate_all().await;
        assert!(res.is_ok());

        assert!(*comp1.lock().await);
        assert_eq!(saga.pending_compensation_count(), 0);
    }
    #[test]
    fn test_saga_context_accessor() {
        let ctx = WorkflowContext::new_test();
        let saga = Saga::new(&ctx);
        // We can't easily assert on the context itself without PartialEq,
        // but we can verify the method exists and returns a reference.
        let _ = saga.context();
    }
}
