//! Fixture (review round 2, finding 3): two inherent impls whose SELF TYPE
//! shares a last path segment, with different bodies.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o shadowed_impls.mir shadowed_impls.rs
//!
//! rustc 1.98 prints the impl body path WITH its module prefix
//! (`a::<impl at shadowed_impls.rs:..>::run`) and the call site fully qualified
//! (`b::Worker::run(move _1)`), so the module path is recoverable on both sides.
//! Keying the impl index on the bare self-type name (`Worker`) collapsed the two
//! and `or_insert_with` kept whichever came first — a call to `b::Worker::run`
//! was then analyzed as `a::Worker::run`'s clean body.
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

/// Sorts BEFORE `b` in every index, and is clean: the shadowing candidate.
pub mod a {
    pub struct Worker;
    impl Worker {
        pub fn run(&self) -> u64 {
            1
        }
    }
}

/// The real ambient source, reachable only under its own module path.
pub mod b {
    use std::time::{SystemTime, UNIX_EPOCH};
    pub struct Worker;
    impl Worker {
        pub fn run(&self) -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }
    }
}

pub fn __autumn_workflow_info_wf_calls_ambient_worker() -> u8 {
    0
}

/// Calls `b::Worker::run`, which reads the wall clock.
pub async fn wf_calls_ambient_worker(ctx: &WorkflowContext) -> Result<u64, String> {
    let w = b::Worker;
    ctx.execute_activity_raw("charge".to_string(), w.run())
}

pub fn __autumn_workflow_info_wf_calls_clean_worker() -> u8 {
    0
}

/// Calls `a::Worker::run`, which is a constant.
pub async fn wf_calls_clean_worker(ctx: &WorkflowContext) -> Result<u64, String> {
    let w = a::Worker;
    ctx.execute_activity_raw("charge".to_string(), w.run())
}
