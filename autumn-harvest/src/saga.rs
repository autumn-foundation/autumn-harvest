//! Saga compensation helper for workflow code.
//!
//! A [`Saga`] records compensating actions for successful forward steps. If a
//! later step fails, the recorded actions run in reverse order before the
//! original error is returned.

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
    /// order. When compensation succeeds, the original error is returned. When
    /// any compensation fails, all remaining compensations are still attempted
    /// and the result is [`HarvestError::SagaCompensationFailed`].
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
    /// This is useful when workflow code decides to abort after successful
    /// steps without expressing that abort as a failing saga step.
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
