//! Fixture: the candidate build for the structure manifest (issue #1995).
//!
//! It is the next build of `../upgrade_baseline/flow.rs`. Each change is marked
//! with `CHANGED`. The extra lines in this header shift every span, so
//! the digest of an unchanged body must ignore spans.
//!
//! Line shift one.
//! Line shift two.
//!
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o flow.mir flow.rs
#![allow(dead_code)]

use std::future::{Ready, ready};

/// Stand-in for `autumn_harvest::WorkflowContext`. The model matches sinks by
/// receiver and method name. Each method returns a std future, so its body,
/// like the real one, is not in the analyzed set.
pub struct WorkflowContext;

/// Stand-in for `autumn_harvest::ActivityInfo`.
pub struct ActivityInfo {
    pub name: &'static str,
}

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: &str, input: u64) -> Ready<Result<u64, String>> {
        ready(Ok(input))
    }
    pub fn execute_activity(&self, info: &ActivityInfo, input: u64) -> Ready<Result<u64, String>> {
        ready(Ok(input))
    }
    pub fn timer(&self, timer_id: &str, secs: u64) -> Ready<Result<(), String>> {
        ready(Ok(()))
    }
    pub fn receive_signal(&self, name: &str) -> Ready<Result<u64, String>> {
        ready(Ok(0))
    }
    pub fn await_condition(&self) -> Ready<Result<(), String>> {
        ready(Ok(()))
    }
}

/// The activity body. The workflow never calls it.
pub async fn charge(amount: u64) -> u64 {
    amount + 2 // CHANGED
}

pub fn __autumn_activity_info_charge() -> ActivityInfo {
    ActivityInfo { name: "charge" }
}

pub fn charge_info() -> ActivityInfo {
    __autumn_activity_info_charge()
}

pub fn __autumn_workflow_info_wf_activity_only() -> u8 {
    0
}

/// A typed activity, then a timer.
pub async fn wf_activity_only(ctx: &WorkflowContext) -> Result<u64, String> {
    let v = ctx.execute_activity(&charge_info(), 5).await?;
    ctx.timer("settle", 60).await?;
    Ok(v)
}

/// First step. The candidate changes what it does after the activity.
pub async fn reserve(ctx: &WorkflowContext) -> Result<u64, String> {
    let v = ctx.execute_activity_raw("reserve", 1).await?;
    Ok(v + 10) // CHANGED
}

/// Second step. The candidate changes it too.
pub async fn ship(ctx: &WorkflowContext, v: u64) -> Result<u64, String> {
    ctx.timer("ship_wait", 120).await?; // CHANGED
    ctx.execute_activity_raw("ship", v).await
}

pub fn __autumn_workflow_info_wf_steps() -> u8 {
    0
}

/// Two helper steps, each called once.
pub async fn wf_steps(ctx: &WorkflowContext) -> Result<u64, String> {
    let v = reserve(ctx).await?;
    ship(ctx, v).await
}

pub fn __autumn_workflow_info_wf_root_changed() -> u8 {
    0
}

/// The candidate changes this body itself.
pub async fn wf_root_changed(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.execute_activity_raw("audit", 2).await // CHANGED
}

/// A step that the workflow calls in a loop.
pub async fn poll_once(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.execute_activity_raw("poll", 2).await // CHANGED
}

pub fn __autumn_workflow_info_wf_loop() -> u8 {
    0
}

/// Calls `poll_once` three times.
pub async fn wf_loop(ctx: &WorkflowContext) -> Result<u64, String> {
    let mut total = 0;
    for _ in 0..3 {
        total += poll_once(ctx).await?;
    }
    Ok(total)
}

/// A step whose activity name comes from a parameter.
pub async fn dispatch(ctx: &WorkflowContext, name: &str) -> Result<u64, String> {
    ctx.execute_activity_raw(name, 2).await // CHANGED
}

pub fn __autumn_workflow_info_wf_param_key() -> u8 {
    0
}

/// The step key is not a constant at the sink.
pub async fn wf_param_key(ctx: &WorkflowContext) -> Result<u64, String> {
    dispatch(ctx, "approve").await
}

pub fn __autumn_workflow_info_wf_signal() -> u8 {
    0
}

/// A signal wait has no provable key.
pub async fn wf_signal(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.receive_signal("go").await
}

pub fn __autumn_workflow_info_wf_match() -> u8 {
    0
}

/// Branches on a match. The candidate changes one case value.
pub async fn wf_match(ctx: &WorkflowContext, v: u64) -> Result<u64, String> {
    match v {
        2 => ctx.execute_activity_raw("one", 1).await, // CHANGED
        _ => ctx.execute_activity_raw("other", 2).await,
    }
}

/// A `const` item. The candidate changes its value.
const LIMIT: u64 = 4; // CHANGED

/// A step that reads `LIMIT`.
pub async fn limited(ctx: &WorkflowContext) -> Result<u64, String> {
    let v = ctx.execute_activity_raw("limited", 1).await?;
    Ok(v.min(LIMIT))
}

pub fn __autumn_workflow_info_wf_const() -> u8 {
    0
}

/// Calls the step that reads `LIMIT`.
pub async fn wf_const(ctx: &WorkflowContext) -> Result<u64, String> {
    limited(ctx).await
}

pub fn __autumn_workflow_info_wf_closure() -> u8 {
    0
}

/// Passes a closure to `map`, which can call it many times.
pub async fn wf_closure(ctx: &WorkflowContext) -> Result<u64, String> {
    let total: u64 = (0..3_u64).map(|i| i + 1).sum();
    ctx.execute_activity_raw("sum", total).await
}

/// A step that can park with no command.
pub async fn wait_ready(ctx: &WorkflowContext) -> Result<(), String> {
    ctx.await_condition().await
}

pub fn __autumn_workflow_info_wf_condition() -> u8 {
    0
}

/// Waits on a condition, then runs one activity.
pub async fn wf_condition(ctx: &WorkflowContext) -> Result<u64, String> {
    wait_ready(ctx).await?;
    ctx.execute_activity_raw("after", 1).await
}
