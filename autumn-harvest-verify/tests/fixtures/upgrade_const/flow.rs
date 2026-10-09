//! Fixture: a workflow that reads a `const` from another crate (issue #1995).
//!
//! MIR prints the read as `const limits::ATTEMPTS`, not as its value. So the
//! workflow's own MIR is the same against both builds of `limits`. From this
//! directory, on rustc 1.99.0 (b940084d7 2026-09-28):
//!
//! ```sh
//!   for v in 1 2; do
//!     rustc --crate-type lib --edition 2024 --crate-name limits \
//!           --emit=mir -o limits_v$v.mir limits_v$v.rs
//!     rustc --crate-type lib --edition 2024 --crate-name limits \
//!           --emit=link -o liblimits_v$v.rlib limits_v$v.rs
//!     rustc --crate-type lib --edition 2024 --crate-name flow --emit=mir \
//!           --extern limits=liblimits_v$v.rlib -o flow_v$v.mir flow.rs
//!   done
//!   cmp flow_v1.mir flow_v2.mir && mv flow_v1.mir flow.mir
//!   rm flow_v2.mir liblimits_v1.rlib liblimits_v2.rlib
//! ```
//!
//! The `cmp` is the point: the workflow's MIR does not change.
#![allow(dead_code, unused_variables)]

use std::future::{Ready, ready};

/// Stand-in for `autumn_harvest::WorkflowContext`.
pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: &str, input: u64) -> Ready<Result<u64, String>> {
        ready(Ok(input))
    }
}

pub fn __autumn_workflow_info_wf_dep_const() -> u8 {
    0
}

/// A branch on a dependency's `const`, and a read of a std `const`.
pub async fn wf_dep_const(ctx: &WorkflowContext, attempt: u64) -> Result<u64, String> {
    if attempt < limits::ATTEMPTS {
        return ctx.execute_activity_raw("retry", attempt).await;
    }
    ctx.execute_activity_raw("give_up", u64::MAX).await
}
