//! Fixture: the flow graph and saga compensation coverage (issue #2010).
//!
//! `harvest_stub.rs` stands in for the `autumn_harvest` crate. It is built
//! first, without `--emit=mir`, so no body of it is in the analyzed set.
//! From this directory:
//!
//!   rustc --crate-type lib --edition 2024 --crate-name autumn_harvest \
//!         -o libautumn_harvest.rlib harvest_stub.rs
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         --extern autumn_harvest=libautumn_harvest.rlib -o flow.mir flow.rs
//!   rm libautumn_harvest.rlib
#![allow(dead_code)]

use autumn_harvest::{Saga, WorkflowContext};

type Out = Result<u64, String>;

pub fn __autumn_workflow_info_wf_no_saga() -> u8 {
    0
}

/// No saga.
pub async fn wf_no_saga(ctx: &WorkflowContext) -> Out {
    ctx.execute_activity_raw("charge", 1).await
}

pub fn __autumn_workflow_info_wf_covered() -> u8 {
    0
}

/// Two saga steps. A failed step unwinds every earlier step.
pub async fn wf_covered(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    let b = saga
        .step(
            || async move { ctx.execute_activity_raw("charge", a).await },
            |b| async move { ctx.execute_activity_raw("refund", b).await.map(|_| ()) },
        )
        .await?;
    Ok(b)
}

pub fn __autumn_workflow_info_wf_gap_after_step() -> u8 {
    0
}

/// A plain activity after the saga step. Its failure skips the unwind.
pub async fn wf_gap_after_step(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    let b = ctx.execute_activity_raw("ship", a).await?;
    Ok(b)
}

pub fn __autumn_workflow_info_wf_gap_explicit() -> u8 {
    0
}

/// An explicit error after the saga step, with no unwind.
pub async fn wf_gap_explicit(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    if a > 10 {
        return Err("too big".to_string());
    }
    Ok(a)
}

pub fn __autumn_workflow_info_wf_gap_tail() -> u8 {
    0
}

/// The last activity result is the workflow result. It can be an error.
pub async fn wf_gap_tail(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    ctx.execute_activity_raw("ship", a).await
}

pub fn __autumn_workflow_info_wf_compensated() -> u8 {
    0
}

/// Each exit after the saga step unwinds first.
pub async fn wf_compensated(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    match ctx.receive_signal("approved").await {
        Ok(v) if v > 0 => Ok(a),
        _ => {
            saga.compensate_all().await?;
            Err("rejected".to_string())
        }
    }
}

pub fn __autumn_workflow_info_wf_loop() -> u8 {
    0
}

/// A saga step in a loop.
pub async fn wf_loop(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let mut total = 0;
    for i in 0..3_u64 {
        total += saga
            .step(
                || async move { ctx.execute_activity_raw("reserve", i).await },
                |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
            )
            .await?;
    }
    Ok(total)
}

pub fn __autumn_workflow_info_wf_noop_compensation() -> u8 {
    0
}

/// The compensation emits no command.
pub async fn wf_noop_compensation(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("approve", 1).await },
            |_| async { Ok(()) },
        )
        .await?;
    Ok(a)
}

pub fn __autumn_workflow_info_wf_untracked() -> u8 {
    0
}

/// The step result is matched, not passed to `?`.
pub async fn wf_untracked(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let step = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await;
    match step {
        Ok(a) => Ok(a),
        Err(e) => Err(e),
    }
}

/// A helper that takes the saga and adds a step.
async fn more(saga: &mut Saga<'_>, ctx: &WorkflowContext) -> Out {
    saga.step(
        || async { ctx.execute_activity_raw("charge", 2).await },
        |b| async move { ctx.execute_activity_raw("refund", b).await.map(|_| ()) },
    )
    .await
}

pub fn __autumn_workflow_info_wf_escapes() -> u8 {
    0
}

/// The saga leaves the body that owns it.
pub async fn wf_escapes(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    let b = more(&mut saga, ctx).await?;
    Ok(a + b)
}

/// A helper with its own error.
async fn ship(ctx: &WorkflowContext, v: u64) -> Out {
    ctx.execute_activity_raw("ship", v).await
}

pub fn __autumn_workflow_info_wf_gap_helper() -> u8 {
    0
}

/// The helper error reaches the workflow exit with no unwind.
pub async fn wf_gap_helper(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    let b = ship(ctx, a).await?;
    Ok(b)
}

pub fn __autumn_workflow_info_wf_handlers() -> u8 {
    0
}

/// A signal handler and a signal wait.
pub async fn wf_handlers(ctx: &WorkflowContext) -> Out {
    ctx.register_signal_handler("cancel", |v: u64| {
        let _ = v;
    });
    ctx.receive_signal("go").await
}

pub fn __autumn_workflow_info_wf_update_handler() -> u8 {
    0
}

/// An update handler with a validator.
pub async fn wf_update_handler(ctx: &WorkflowContext) -> Out {
    ctx.register_update_handler(
        "set_limit",
        |v: &u64| if *v > 0 { Ok(()) } else { Err("zero".to_string()) },
        |v: u64| async move { Ok(v) },
    );
    ctx.receive_signal("go").await
}

pub fn __autumn_workflow_info_wf_unit() -> u8 {
    0
}

/// A covered saga that returns `Result<(), _>`.
pub async fn wf_unit(ctx: &WorkflowContext) -> Result<(), String> {
    let mut saga = Saga::new(ctx);
    saga.step(
        || async { ctx.execute_activity_raw("reserve", 1).await },
        |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
    )
    .await?;
    Ok(())
}

pub fn __autumn_workflow_info_wf_rebind() -> u8 {
    0
}

/// The saga moves to a second binding before its first step.
pub async fn wf_rebind(ctx: &WorkflowContext) -> Out {
    let first = Saga::new(ctx);
    let mut saga = first;
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    Ok(a)
}

pub fn __autumn_workflow_info_wf_unwind_then_err() -> u8 {
    0
}

/// The usual shape: unwind, then return the original error.
pub async fn wf_unwind_then_err(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    match ctx.execute_activity_raw("ship", a).await {
        Ok(b) => Ok(b),
        Err(e) => {
            let _ = saga.compensate_all().await;
            Err(e)
        }
    }
}

pub fn __autumn_workflow_info_wf_not_awaited() -> u8 {
    0
}

/// The unwind future is built but never awaited, so nothing unwinds.
pub async fn wf_not_awaited(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    if a > 10 {
        let _ = saga.compensate_all();
        return Err("too big".to_string());
    }
    Ok(a)
}

pub fn __autumn_workflow_info_wf_loop_new() -> u8 {
    0
}

/// A new saga in each iteration. A later failure leaves the earlier step.
pub async fn wf_loop_new(ctx: &WorkflowContext) -> Out {
    for i in 0..3_u64 {
        let mut saga = Saga::new(ctx);
        saga.step(
            || async move { ctx.execute_activity_raw("reserve", i).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    }
    Ok(1)
}

/// Builds a saga that does not reach a caller.
fn fresh(ctx: &WorkflowContext) -> Saga<'_> {
    Saga::new(ctx)
}

pub fn __autumn_workflow_info_wf_reassign() -> u8 {
    0
}

/// A second saga replaces the first after a step.
pub async fn wf_reassign(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    saga = fresh(ctx);
    let b = saga
        .step(
            || async move { ctx.execute_activity_raw("charge", a).await },
            |b| async move { ctx.execute_activity_raw("refund", b).await.map(|_| ()) },
        )
        .await?;
    Ok(b)
}

/// A helper that owns a saga and drops it on success.
async fn reserve_all(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    Ok(a)
}

pub fn __autumn_workflow_info_wf_helper_owns() -> u8 {
    0
}

/// The workflow fails after the helper dropped its saga.
pub async fn wf_helper_owns(ctx: &WorkflowContext) -> Out {
    let a = reserve_all(ctx).await?;
    let b = ctx.execute_activity_raw("ship", a).await?;
    Ok(b)
}

pub fn __autumn_workflow_info_wf_prebuilt() -> u8 {
    0
}

/// A future built before the step, and a step result read by `match`.
pub async fn wf_prebuilt(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let ship = ctx.execute_activity_raw("ship", 2);
    let a = match saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await
    {
        Ok(a) => a,
        Err(e) => return Err(e),
    };
    let b = ship.await?;
    Ok(a + b)
}

pub fn __autumn_workflow_info_wf_pre_err() -> u8 {
    0
}

/// A result built before the step, read by `?` after it.
pub async fn wf_pre_err(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let pre: Out = Err("later failure".to_string());
    let _step = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await;
    let v = pre?;
    Ok(v)
}

pub fn __autumn_workflow_info_wf_map_err() -> u8 {
    0
}

/// `map_err` sits between the step and `?`.
pub async fn wf_map_err(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await
        .map_err(|e| format!("reserve: {e}"))?;
    Ok(a)
}

pub fn __autumn_workflow_info_wf_saga_in_block() -> u8 {
    0
}

/// The saga moves into an `async` block.
pub async fn wf_saga_in_block(ctx: &WorkflowContext) -> Out {
    let mut saga = Saga::new(ctx);
    let a = saga
        .step(
            || async { ctx.execute_activity_raw("reserve", 1).await },
            |a| async move { ctx.execute_activity_raw("release", a).await.map(|_| ()) },
        )
        .await?;
    let keep = async move {
        let _held = saga;
        0_u64
    };
    let k = keep.await;
    Ok(a + k)
}
