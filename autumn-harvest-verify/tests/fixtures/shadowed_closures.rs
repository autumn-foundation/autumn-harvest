//! Fixture: two distinct closure bodies that rustc prints with the **same**
//! `{closure@FILE:L:C}` span.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o shadowed_closures.mir shadowed_closures.rs
//!
//! A closure written inside a `macro_rules!` body carries the span of the
//! *macro definition*, not of the expansion site, so every instantiation of
//! `mk_add!` yields a body whose printed type is
//! `{closure@shadowed_closures.rs:L:C}` for one and the same `L:C`. The bodies
//! themselves are different — `$extra` is call-site tokens — and exactly one of
//! them reads the wall clock.
//!
//! Indexing closures by that span with "first one wins" therefore resolves the
//! ambient closure's call site to the *clean* body and reports
//! `proven-deterministic` over a wall-clock read: the one verdict this tool
//! must never print. The span has to be a multimap, disambiguated by the body
//! path the call site sits in.
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

/// One macro, two expansions, one printed span: `$extra` is the only thing that
/// differs between the two closure bodies it produces.
macro_rules! mk_add {
    ($extra:expr) => {
        |x: u64| x + $extra
    };
}

pub fn __autumn_workflow_info_wf_macro_closure_clean() -> u8 {
    0
}

/// The expansion adds a constant: deterministic.
pub async fn wf_macro_closure_clean(
    ctx: &WorkflowContext,
    o: Option<u64>,
) -> Result<u64, String> {
    let v = o.map(mk_add!(1)).unwrap_or_default();
    ctx.execute_activity_raw("charge".to_string(), v)
}

pub fn __autumn_workflow_info_wf_macro_closure_ambient() -> u8 {
    0
}

/// The expansion adds a wall-clock read, *inside* the closure body — and the
/// closure's printed span is byte-identical to the clean one's.
pub async fn wf_macro_closure_ambient(
    ctx: &WorkflowContext,
    o: Option<u64>,
) -> Result<u64, String> {
    let v = o.map(mk_add!(wall_clock_secs())).unwrap_or_default();
    ctx.execute_activity_raw("charge".to_string(), v)
}
