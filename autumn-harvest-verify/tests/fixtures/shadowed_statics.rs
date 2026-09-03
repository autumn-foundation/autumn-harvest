//! Fixture (review round 2, finding 1): two statics that share a LAST path
//! segment but differ in module and in type.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o shadowed_statics.mir shadowed_statics.rs
//!
//! rustc 1.98 prints both the `static` item header and the `allocN (static: ..)`
//! footer with the FULL path (`static b::COUNTER: Atomic<u64>`,
//! `alloc2 (static: b::COUNTER, ..)`), so indexing statics by their last segment
//! collapses `a::COUNTER` and `b::COUNTER` onto one entry. Whichever the index
//! kept first then decided the verdict, and the immutable `a::COUNTER` made a
//! read of the atomic `b::COUNTER` look clean.
//!
//! `WorkflowContext` is a stand-in, as in `format_and_outparams.rs`; every
//! workflow-like fn has its `__autumn_workflow_info_<name>` companion.
#![allow(dead_code, unused_variables)]

pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
}

/// Sorts BEFORE `b` in every index, and is immutable: the shadowing candidate.
pub mod a {
    pub static COUNTER: u64 = 7;
}

/// The real ambient source, reachable only under its own module path.
pub mod b {
    use std::sync::atomic::AtomicU64;
    pub static COUNTER: AtomicU64 = AtomicU64::new(0);
}

pub fn __autumn_workflow_info_wf_reads_shadowed_atomic() -> u8 {
    0
}

/// Reads `b::COUNTER` — an ambient `AtomicU64` — and puts it in a command.
pub async fn wf_reads_shadowed_atomic(ctx: &WorkflowContext) -> Result<u64, String> {
    let n = b::COUNTER.load(std::sync::atomic::Ordering::SeqCst);
    ctx.execute_activity_raw("charge".to_string(), n)
}

pub fn __autumn_workflow_info_wf_reads_shadowing_plain() -> u8 {
    0
}

/// Reads `a::COUNTER` — plain immutable data — and must stay clean.
pub async fn wf_reads_shadowing_plain(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.execute_activity_raw("charge".to_string(), a::COUNTER)
}
