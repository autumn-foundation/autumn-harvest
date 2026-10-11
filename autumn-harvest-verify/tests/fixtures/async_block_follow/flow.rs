//! Fixture: the `async` block a closure returns (issue #2010).
//!
//! The analysis follows that block only when a bodyless callee polls it. It
//! must not turn a former `unknown` into a false `proven-deterministic`.
//! From this directory:
//!
//!   rustc --crate-type lib --edition 2024 --crate-name autumn_harvest \
//!         -o libautumn_harvest.rlib harvest_stub.rs
//!   rustc --crate-type lib --edition 2024 --crate-name other_crate \
//!         -o libother_crate.rlib other_crate.rs
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         --extern autumn_harvest=libautumn_harvest.rlib \
//!         --extern other_crate=libother_crate.rlib -o flow.mir flow.rs
//!   rm libautumn_harvest.rlib libother_crate.rlib
#![allow(dead_code, unused)]

use std::future::Future;

use autumn_harvest::{Saga, WorkflowContext};

type Out = Result<u64, String>;

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

pub fn __autumn_workflow_info_wf_saga_clock() -> u8 {
    0
}

/// A clock read inside the `async` block of a saga step.
pub async fn wf_saga_clock(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(
        || async { ctx.execute_activity_raw("r", now_nanos()).await },
        |_| async { Ok(()) },
    )
    .await
}

/// A first-party `async fn` that reads the clock.
async fn reserve(ctx: &WorkflowContext) -> Out {
    ctx.execute_activity_raw("reserve", now_nanos()).await
}

pub fn __autumn_workflow_info_wf_saga_async_fn() -> u8 {
    0
}

/// The saga step closure returns the future of a first-party `async fn`.
pub async fn wf_saga_async_fn(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(|| reserve(ctx), |_| async { Ok(()) }).await
}

pub fn __autumn_workflow_info_wf_external() -> u8 {
    0
}

/// An untrusted callee with an `async` block in its turbofish.
pub async fn wf_external(ctx: &WorkflowContext) -> Out {
    other_crate::wrap(async { ctx.execute_activity_raw("a", 1).await }).await
}

macro_rules! blk {
    ($ctx:expr, $v:expr) => {
        async move { $ctx.execute_activity_raw("m", $v).await }
    };
}

fn helper(ctx: &WorkflowContext) -> impl Future<Output = Out> + '_ {
    blk!(ctx, now_nanos())
}

pub fn __autumn_workflow_info_wf_shared_span() -> u8 {
    0
}

/// Two `async` blocks share one macro span. One of them reads the clock.
pub async fn wf_shared_span(ctx: &WorkflowContext) -> Out {
    let _ = blk!(ctx, 1).await?;
    helper(ctx).await
}

pub fn __autumn_workflow_info_wf_block_write() -> u8 {
    0
}

/// An awaited `async` block writes the clock into a captured local.
pub async fn wf_block_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    async {
        seen = now_nanos();
    }
    .await;
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_write() -> u8 {
    0
}

/// A saga step block writes the clock through a captured reference.
pub async fn wf_step_write(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let s = &mut seen;
                async move {
                    *s = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_foreign_block() -> u8 {
    0
}

/// The step closure returns a future built in another crate.
pub async fn wf_foreign_block(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    saga.step(|| ctx.rpit_block(1), |_| async { Ok(()) }).await
}
