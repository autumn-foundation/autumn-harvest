//! Fixture: MIR format drift *inside* a block (the soundness review's P1-2).
//!
//! Compiled standalone with:
//!   rustc --crate-type lib --edition 2024 --emit=mir -o parse_drift.mir parse_drift.rs
//!
//! `parse_drift_garbled.mir` is a byte-for-byte copy of `parse_drift.mir` with a
//! single call terminator rewritten from `_N = wall_clock_secs() -> [..]` to
//! `_N <== wall_clock_secs() -> [..]`, simulating a future rustc that prints a
//! call differently. The clean dump must be `nondeterminism-found`; the garbled
//! one must be `unknown: mir-parse` — never `proven-deterministic`.
#![allow(dead_code, unused_variables)]

use std::time::{SystemTime, UNIX_EPOCH};

pub struct WorkflowContext;

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

pub async fn wf_helper_parse_target(ctx: &WorkflowContext) -> u64 {
    let stamp = wall_clock_secs();
    ctx.execute_activity_raw("x".to_string(), stamp).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_helper_parse_target() -> u8 {
    1
}
