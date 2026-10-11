//! Stub of the `autumn_harvest` API for the saga graph fixture (issue #2010).
//!
//! It is compiled to an rlib without `--emit=mir`, as a real dependency is.
//! So the analysis sees no body of it, and its crate name makes it trusted.
#![allow(dead_code, unused_variables)]

use std::future::{Future, Ready, ready};

/// Stand-in for `autumn_harvest::WorkflowContext`.
pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: &str, input: u64) -> Ready<Result<u64, String>> {
        ready(Ok(input))
    }
    pub fn receive_signal(&self, name: &str) -> Ready<Result<u64, String>> {
        ready(Ok(0))
    }
    pub fn register_signal_handler<H>(&self, name: &str, handler: H)
    where
        H: Fn(u64) + Send + Sync + 'static,
    {
    }
}

/// Stand-in for `autumn_harvest::Saga`. Its methods are `async`, as the
/// real ones are.
pub struct Saga<'ctx> {
    ctx: &'ctx WorkflowContext,
    pending: usize,
}

impl<'ctx> Saga<'ctx> {
    pub const fn new(ctx: &'ctx WorkflowContext) -> Self {
        Self { ctx, pending: 0 }
    }

    pub async fn step<T, Step, StepFuture, Compensate, CompensationFuture>(
        &mut self,
        step: Step,
        compensate: Compensate,
    ) -> Result<T, String>
    where
        Step: FnOnce() -> StepFuture,
        StepFuture: Future<Output = Result<T, String>>,
        Compensate: FnOnce(T) -> CompensationFuture,
        CompensationFuture: Future<Output = Result<(), String>>,
    {
        let out = step().await;
        if out.is_ok() {
            self.pending += 1;
        }
        out
    }

    pub async fn compensate_all(&mut self) -> Result<(), String> {
        self.pending = 0;
        Ok(())
    }
}
