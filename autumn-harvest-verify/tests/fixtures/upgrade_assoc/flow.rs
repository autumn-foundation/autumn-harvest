//! Fixture: a workflow that reads an associated `const` (issue #1995).
//!
//! MIR prints the read as `const <types::Plan as limits::Limits>::MAX`. The
//! impl lives in `limits`, and `types` holds an unrelated `MAX`. The
//! workflow's own MIR is the same against both builds of `limits`. Both
//! builds compile from one file name, because the impl header names its
//! file. From this directory, on rustc 1.99.0 (b940084d7 2026-09-28):
//!
//! ```sh
//!   rustc --crate-type lib --edition 2024 --crate-name types \
//!         --emit=mir -o types.mir types.rs
//!   rustc --crate-type lib --edition 2024 --crate-name types \
//!         --emit=link -o libtypes.rlib types.rs
//!   for v in 1 2; do
//!     cp limits_v$v.rs limits.rs
//!     rustc --crate-type lib --edition 2024 --crate-name limits --emit=mir \
//!           --extern types=libtypes.rlib -o limits_v$v.mir limits.rs
//!     rustc --crate-type lib --edition 2024 --crate-name limits --emit=link \
//!           --extern types=libtypes.rlib -o liblimits_v$v.rlib limits.rs
//!     rustc --crate-type lib --edition 2024 --crate-name flow --emit=mir -L . \
//!           --extern types=libtypes.rlib --extern limits=liblimits_v$v.rlib \
//!           -o flow_v$v.mir flow.rs
//!   done
//!   cmp flow_v1.mir flow_v2.mir && mv flow_v1.mir flow.mir
//!   rm limits.rs flow_v2.mir libtypes.rlib liblimits_v1.rlib liblimits_v2.rlib
//! ```
#![allow(dead_code, unused_variables)]

use std::future::{Ready, ready};

/// Stand-in for `autumn_harvest::WorkflowContext`.
pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: &str, input: u64) -> Ready<Result<u64, String>> {
        ready(Ok(input))
    }
}

pub fn __autumn_workflow_info_wf_assoc_const() -> u8 {
    0
}

/// A branch on an associated `const` from another crate.
pub async fn wf_assoc_const(ctx: &WorkflowContext, attempt: u64) -> Result<u64, String> {
    if attempt < <types::Plan as limits::Limits>::MAX {
        return ctx.execute_activity_raw("retry", attempt).await;
    }
    ctx.execute_activity_raw("give_up", attempt).await
}
