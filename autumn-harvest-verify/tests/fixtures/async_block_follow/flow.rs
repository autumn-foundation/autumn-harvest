//! Fixture: the `async` block a closure returns (issue #2010).
//!
//! The analysis follows that block only when a bodyless callee polls it. It
//! must not turn a former `unknown` into a false `proven-deterministic`.
//! `../RUSTC_VERSION.txt` gives the build commands.
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

pub fn __autumn_workflow_info_wf_step_write_tuple() -> u8 {
    0
}

/// The same write, through a `&mut` packed in a tuple.
pub async fn wf_step_write_tuple(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let refs = (&mut seen,);
                async move {
                    *refs.0 = now_nanos();
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

/// A named type that hides a `&mut`.
pub struct Refs<'a> {
    seen: &'a mut u64,
}

fn record(refs: Refs<'_>) {
    *refs.seen = now_nanos();
}

pub fn __autumn_workflow_info_wf_step_write_struct() -> u8 {
    0
}

/// The same write, through a `&mut` inside a named struct.
pub async fn wf_step_write_struct(ctx: &WorkflowContext) -> Out {
    let mut seen = 0_u64;
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let refs = Refs { seen: &mut seen };
                async move {
                    record(refs);
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen).await
}

pub fn __autumn_workflow_info_wf_step_write_cell() -> u8 {
    0
}

/// The same write, through a shared `&Cell` capture.
pub async fn wf_step_write_cell(ctx: &WorkflowContext) -> Out {
    let seen = std::cell::Cell::new(0_u64);
    {
        let mut saga = Saga::new(ctx);
        saga.step(
            || {
                let cell = &seen;
                async move {
                    cell.set(now_nanos());
                    ctx.execute_activity_raw("a", 1).await
                }
            },
            |_| async { Ok(()) },
        )
        .await?;
    }
    ctx.execute_activity_raw("b", seen.get()).await
}
