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
/// # Observability (issue #801)
///
/// Every **non-empty** unwind (a [`compensate_all`](Self::compensate_all)
/// call or an automatic step-failure rollback with at least one pending
/// compensation) emits the counter `harvest.saga.compensated{workflow, queue}`
/// **exactly once per real compensation sequence**, and an unwind that
/// finishes with at least one compensation error additionally emits
/// `harvest.saga.compensation_failed{workflow, queue}` — the alertable
/// dangling-state signal, fired even when the author catches
/// [`HarvestError::SagaCompensationFailed`] and completes normally.
///
/// Exactly-once across replays is keyed to durable `MarkerRecorded` dedup
/// markers (`saga_compensated:{seq}` at unwind start, in the same command
/// batch as the first compensation's own dispatch; `saga_compensation_failed:{seq}`
/// at failed-unwind end). Replays — the only resume mechanism Harvest has —
/// observe the marker and stay silent, while pre-#801 marker-less histories
/// replay untouched and uncounted (the matcher's tolerant `Absent` arm never
/// moves the cursor). No new `WorkflowEvent` variant and no migration are
/// involved; the markers are opaque names on the existing `MarkerRecorded`
/// variant, exactly like `fan_out:{n}` / `race:{seq}` / `patch:{id}`.
///
/// Accepted edges (documented in `docs/saga.md`): an unwind entered with
/// unconsumed non-marker events at the cursor is conservatively uncounted; a
/// **pure in-memory** unwind (zero durable footprint) that crash-resumes
/// within one decision cycle can re-count (at-least-once) — the metric
/// mirror of the "compensations re-run wholesale" idempotency contract; and
/// a history recorded by #801+ code does not replay under pre-#801 builds
/// (roll forward, never back — the same forward-compat rule as every marker
/// feature).
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
    #[must_use]
    pub const fn new(ctx: &'ctx WorkflowContext) -> Self {
        Self {
            ctx,
            compensations: Vec::new(),
        }
    }

    /// Return the workflow context this saga is associated with.
    #[must_use]
    pub const fn context(&self) -> &'ctx WorkflowContext {
        self.ctx
    }

    /// Return the number of successful steps that can still be compensated.
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
        // AC6 (issue #801): an empty unwind is not a compensation sequence —
        // no seq allocated, no marker, no metric. Behaviorally identical to
        // the zero-iteration loop this early return replaces.
        if self.compensations.is_empty() {
            return Ok(());
        }

        // Counted at unwind start (the earliest outage signal), deduped
        // across replays by the durable `saga_compensated:{seq}` marker —
        // recorded in the same command batch as the first compensation's own
        // dispatch, so a crash at any point mid-unwind resumes silent. The
        // returned observation carries the unwind's disposition; the failure
        // observe below follows it (invariant: failed ≤ compensated).
        let observation = self.ctx.observe_saga_unwind_start(self.compensations.len());

        let mut errors = Vec::new();

        while let Some(compensation) = self.compensations.pop() {
            if let Err(error) = compensation().await {
                errors.push(error.to_string());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            // The dangling-state signal (`harvest.saga.compensation_failed`),
            // emitted here rather than at the worker terminal boundary so an
            // author-caught failure is still observed. Keyed to the unwind's
            // start disposition: a counted unwind's failure is always counted
            // (even past a trailing un-awaited signal), an uncounted unwind's
            // failure stays uncounted.
            self.ctx
                .observe_saga_unwind_failed(observation, errors.len());
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
                || async { Err(HarvestError::workflow_failed_untyped("test", "error")) },
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
                    Err::<(), _>(HarvestError::workflow_failed_untyped("comp", "comp error"))
                },
            )
            .await;

        let err_result: HarvestResult<()> = saga
            .step(
                || async { Err(HarvestError::workflow_failed_untyped("test", "step2 error")) },
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

    // ── Issue #780 — register-only compensation + caller-supplied original ──

    /// T21 — `push_compensation` registers a compensation WITHOUT running a
    /// forward step (the DAG unwind already knows which nodes succeeded from
    /// recorded history, so it must not re-execute them). The closure is stored,
    /// not invoked.
    #[tokio::test]
    async fn push_compensation_registers_without_a_forward_step() {
        let ctx = WorkflowContext::new_test();
        let mut saga = Saga::new(&ctx);

        let ran = Arc::new(Mutex::new(false));
        let flag = Arc::clone(&ran);
        saga.push_compensation(move || {
            let flag = Arc::clone(&flag);
            async move {
                *flag.lock().await = true;
                Ok::<(), HarvestError>(())
            }
        });

        assert_eq!(
            saga.pending_compensation_count(),
            1,
            "push_compensation must register exactly one pending compensation"
        );
        assert!(
            !*ran.lock().await,
            "push_compensation must REGISTER only — the closure must not run \
             until the unwind"
        );

        // And it does run on the unwind.
        saga.compensate_all().await.expect("compensation succeeds");
        assert!(
            *ran.lock().await,
            "the registered compensation must run on unwind"
        );
        assert_eq!(saga.pending_compensation_count(), 0);
    }

    /// T22 — `compensate_all_after(original)` reports the CALLER's original
    /// error (the DAG unwind passes `"one or more DAG tasks failed"`), while
    /// `compensate_all()` keeps delegating with the legacy manual string.
    #[tokio::test]
    async fn compensate_all_after_carries_the_caller_supplied_original() {
        let ctx = WorkflowContext::new_test();

        let mut saga = Saga::new(&ctx);
        saga.push_compensation(|| async {
            Err::<(), _>(HarvestError::workflow_failed_untyped("comp", "comp boom"))
        });

        let err = saga
            .compensate_all_after("one or more DAG tasks failed")
            .await
            .expect_err("a failing compensation must surface SagaCompensationFailed");
        match err {
            HarvestError::SagaCompensationFailed {
                original,
                compensation_errors,
            } => {
                assert_eq!(
                    original, "one or more DAG tasks failed",
                    "compensate_all_after must carry the caller-supplied original verbatim"
                );
                assert_eq!(compensation_errors.len(), 1);
                assert!(
                    compensation_errors[0].contains("comp boom"),
                    "compensation error must be preserved, got {:?}",
                    compensation_errors[0]
                );
            }
            other => panic!("expected SagaCompensationFailed, got {other:?}"),
        }

        // `compensate_all()` delegates to the same machinery with the legacy
        // manual-request original — unchanged behaviour for existing callers.
        let mut manual = Saga::new(&ctx);
        manual.push_compensation(|| async {
            Err::<(), _>(HarvestError::workflow_failed_untyped("comp", "comp boom"))
        });
        let err = manual
            .compensate_all()
            .await
            .expect_err("a failing compensation must surface SagaCompensationFailed");
        match err {
            HarvestError::SagaCompensationFailed { original, .. } => assert_eq!(
                original, "manual compensation requested",
                "compensate_all() must keep its legacy original string"
            ),
            other => panic!("expected SagaCompensationFailed, got {other:?}"),
        }
    }
}
