//! Fixture: two modules, each with a `struct Guard` and its own `impl Drop`.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o shadowed_drops.mir shadowed_drops.rs
//!
//! Drop glue is a call the user never wrote and MIR never spells out: the only
//! trace of it is a `drop(_4)` terminator on a place whose type has a user
//! `impl Drop`. The glue is looked up by the type's LAST segment, so
//! `clean::Guard` and `ambient::Guard` answer to the same key — and a lookup
//! that insists on exactly one answer then finds none and treats the drop as
//! inert, which silently drops the wall-clock read and the command emission in
//! `ambient::Guard::drop` on the floor.
//!
//! The same fixture is also analyzed with **no source root**, which is how a
//! pre-emitted `.mir` file is analyzed. Every `<impl at FILE:L:C>` header is
//! then unreadable, so nothing says these `::drop` bodies are `Drop` impls at
//! all; the drop must become a `drop-glue` boundary, never a silent `proven`.
//!
//! `WorkflowContext` is a stand-in for the real `autumn_harvest::WorkflowContext`
//! (see `format_and_outparams.rs`), and each workflow-like fn has the
//! `__autumn_workflow_info_<name>` companion `entry::discover` keys on.
#![allow(dead_code, unused_variables)]

use std::time::{SystemTime, UNIX_EPOCH};

pub struct WorkflowContext {
    pub version: u32,
}

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
}

pub fn wall_clock_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// A guard whose glue emits a command built from data it was handed.
pub mod clean {
    pub struct Guard<'a> {
        pub ctx: &'a super::WorkflowContext,
        pub n: u64,
    }

    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            let _ = self.ctx.execute_activity_raw("cleanup".to_string(), self.n);
        }
    }
}

/// A guard whose glue reads the wall clock and emits a command built from it.
pub mod ambient {
    pub struct Guard<'a> {
        pub ctx: &'a super::WorkflowContext,
        pub n: u64,
    }

    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            let secs = super::wall_clock_secs();
            let _ = self.ctx.execute_activity_raw("cleanup".to_string(), secs);
        }
    }
}

pub fn __autumn_workflow_info_wf_drop_clean_guard() -> u8 {
    0
}

/// The glue emits a command, but from clean data.
pub async fn wf_drop_clean_guard(ctx: &WorkflowContext) -> Result<u64, String> {
    let guard = clean::Guard { ctx, n: 1 };
    let _ = &guard;
    ctx.execute_activity_raw("charge".to_string(), 1)
}

pub fn __autumn_workflow_info_wf_drop_ambient_guard() -> u8 {
    0
}

/// The glue reads the wall clock and emits a command built from it.
pub async fn wf_drop_ambient_guard(ctx: &WorkflowContext) -> Result<u64, String> {
    let guard = ambient::Guard { ctx, n: 1 };
    let _ = &guard;
    ctx.execute_activity_raw("charge".to_string(), 1)
}
