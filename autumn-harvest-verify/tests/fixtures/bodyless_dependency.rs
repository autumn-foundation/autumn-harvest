//! Fixture (review round 2, finding 2): a body-less callee from a dependency
//! that was never asked for MIR must NOT be trusted just because a type at the
//! call site is std-rooted.
//!
//! Compiled standalone (no workspace deps) with, from this directory:
//!   rustc --crate-type lib --edition 2024 --crate-name bodyless_dep_stub \
//!         -o libbodyless_dep_stub.rlib bodyless_dep_stub.rs
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         --extern bodyless_dep_stub=libbodyless_dep_stub.rlib \
//!         -o bodyless_dependency.mir bodyless_dependency.rs
//!
//! The stub is deliberately compiled WITHOUT `--emit=mir`: `now_ish` therefore
//! has no body in the analyzed set, exactly like a dependency outside the
//! `--package` scope. It prints as `bodyless_dep_stub::now_ish` (rooted, so the
//! `external-crate-body` boundary already fires on the path) and, once aliased
//! through a `use`, the destination type `std::string::String` is the ONLY
//! std-rooted text at the site — which is what used to buy it trust.
//!
//! The std control cases (`String::len`, `Vec::push`, `format`) are body-less
//! too and must stay trusted, or every workflow in the workspace goes `unknown`.
#![allow(dead_code, unused_variables)]

pub struct WorkflowContext;

impl WorkflowContext {
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
}

pub fn __autumn_workflow_info_wf_calls_bodyless_dependency() -> u8 {
    0
}

/// `now_ish` has no body here; its `String` result is not evidence about it.
pub async fn wf_calls_bodyless_dependency(ctx: &WorkflowContext) -> Result<u64, String> {
    let name: String = bodyless_dep_stub::now_ish();
    ctx.execute_activity_raw(name, 1)
}

pub fn __autumn_workflow_info_wf_std_receivers_stay_trusted() -> u8 {
    0
}

/// Only body-less std items: a method on a `String` receiver, a method on a
/// `Vec<u32>` receiver, and the `format` free function.
pub async fn wf_std_receivers_stay_trusted(ctx: &WorkflowContext) -> Result<u64, String> {
    let s: String = format!("{}-{}", "charge", 1_u64);
    let n = s.len() as u64;
    let mut v: Vec<u32> = Vec::new();
    v.push(3);
    ctx.execute_activity_raw(s, n + u64::from(v.len() as u32))
}
