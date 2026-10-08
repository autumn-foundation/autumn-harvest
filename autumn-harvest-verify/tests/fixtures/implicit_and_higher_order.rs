//! Fixture: the false-negative classes the adversarial soundness review found.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o implicit_and_higher_order.mir implicit_and_higher_order.rs
//!
//! `WorkflowContext` is a *stand-in* for the real `autumn_harvest::WorkflowContext`,
//! exactly as in `format_and_outparams.rs`: the model matches sinks and
//! sanctioned primitives on the receiver's LAST path segment plus the method
//! name, so a local struct with the same names exercises the same rules.
//!
//! Every workflow-like fn has an `__autumn_workflow_info_<name>` companion,
//! which is what `entry::discover` keys on.
//!
//! Groups:
//!   * `wf_implicit_*`  — implicit flow: a value produced *by* a tainted branch.
//!   * `wf_fn_item_*`   — a bare `fn` item passed as a higher-order argument.
//!   * `wf_closure_env_*` — a closure's writes to its captured environment.
//!   * `wf_drop_glue_*` — a user `Drop` impl containing a sink.
//!   * `wf_hashset_*`   — `HashSet` set-operation iteration order.
//!   * `wf_thread_*`    — `std::thread::sleep` / `std::thread::spawn`.
//!   * `wf_fp_*`        — false-positive traps that must stay proven.
#![allow(dead_code, unused_variables, unused_mut, clippy::all)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── The stand-in workflow context ───────────────────────────────────────────

pub struct WorkflowContext {
    pub version: u32,
}

impl WorkflowContext {
    /// Sink: emits a command; both arguments are checked.
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
    /// Non-sink history metadata (branching on it is clean).
    pub fn version(&self) -> u32 {
        self.version
    }
}

// ── Ambient state ───────────────────────────────────────────────────────────

pub static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn wall_clock_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Clean fallible helper — the `?` on its result must not become control taint.
pub fn parse_amount(raw: &str) -> Result<u64, String> {
    raw.parse::<u64>().map_err(|_| "bad amount".to_string())
}

// ── Implicit flow ───────────────────────────────────────────────────────────

/// Reads the clock, branches on it, returns one of two *constants*.
pub fn parity_shard() -> u64 {
    if wall_clock_secs() % 2 == 0 { 0 } else { 1 }
}

/// The laundering happens in a helper.
pub async fn wf_implicit_flow_helper(ctx: &WorkflowContext) -> u64 {
    ctx.execute_activity_raw("charge".to_string(), parity_shard())
        .unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_implicit_flow_helper() -> u8 {
    1
}

/// The shape a real user writes: entirely inside the workflow body.
pub async fn wf_implicit_flow_inline(ctx: &WorkflowContext) -> u64 {
    let shard = if COUNTER.load(Ordering::SeqCst) % 2 == 0 {
        0_u64
    } else {
        1_u64
    };
    ctx.execute_activity_raw("charge".to_string(), shard)
        .unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_implicit_flow_inline() -> u8 {
    1
}

/// The laundered value is the activity *name*, not its argument.
pub fn shard_name() -> String {
    if wall_clock_secs() % 2 == 0 {
        "a".to_string()
    } else {
        "b".to_string()
    }
}
pub async fn wf_implicit_flow_name(ctx: &WorkflowContext) -> u64 {
    ctx.execute_activity_raw(shard_name(), 0).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_implicit_flow_name() -> u8 {
    1
}

// ── Higher-order fn items ───────────────────────────────────────────────────

pub fn add_clock(x: u64) -> u64 {
    x.wrapping_add(wall_clock_secs())
}

/// `o.map(add_clock)` — a ZST fn item, not a `{closure@..}`.
pub async fn wf_fn_item_map(ctx: &WorkflowContext) -> u64 {
    let o: Option<u64> = Some(1);
    let v = o.map(add_clock).unwrap_or(0);
    ctx.execute_activity_raw("m".to_string(), v).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_fn_item_map() -> u8 {
    1
}

/// `.or_insert_with(wall_clock_secs)` — the same shape in the wild.
pub async fn wf_fn_item_or_insert_with(ctx: &WorkflowContext) -> u64 {
    let mut m: HashMap<String, u64> = HashMap::new();
    let v = *m.entry("a".to_string()).or_insert_with(wall_clock_secs);
    ctx.execute_activity_raw("e".to_string(), v).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_fn_item_or_insert_with() -> u8 {
    1
}

// ── Closure environment write-back ──────────────────────────────────────────

/// A closure declared *and invoked* in the workflow body, writing a capture.
pub async fn wf_closure_env_direct(ctx: &WorkflowContext) -> u64 {
    let mut seen = 0_u64;
    {
        let mut bump = || {
            seen = wall_clock_secs();
        };
        bump();
    }
    ctx.execute_activity_raw("c".to_string(), seen).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_closure_env_direct() -> u8 {
    1
}

/// `HashMap::retain` — an `Order` source converted to a `Value` by a closure.
pub async fn wf_closure_env_retain(ctx: &WorkflowContext) -> u64 {
    let mut m: HashMap<String, u64> = HashMap::new();
    m.insert("a".to_string(), 1);
    m.insert("b".to_string(), 2);
    let mut seen = 0_u64;
    m.retain(|_k, v| {
        seen = *v;
        true
    });
    ctx.execute_activity_raw("r".to_string(), seen).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_closure_env_retain() -> u8 {
    1
}

// ── Drop glue ───────────────────────────────────────────────────────────────

pub struct Bomb<'a> {
    pub ctx: &'a WorkflowContext,
}

impl<'a> Drop for Bomb<'a> {
    fn drop(&mut self) {
        let _ = self
            .ctx
            .execute_activity_raw("bomb".to_string(), wall_clock_secs());
    }
}

/// The sink and its source live only in the `Drop` impl.
pub async fn wf_drop_glue_sink(ctx: &WorkflowContext) -> u64 {
    let _bomb = Bomb { ctx };
    0
}
pub fn __autumn_workflow_info_wf_drop_glue_sink() -> u8 {
    1
}

// ── HashSet set operations ──────────────────────────────────────────────────

pub async fn wf_hashset_difference(ctx: &WorkflowContext) -> u64 {
    let a: HashSet<u64> = HashSet::from([1, 2, 3]);
    let b: HashSet<u64> = HashSet::from([2]);
    let mut last = 0_u64;
    for x in a.difference(&b) {
        last = *x;
    }
    ctx.execute_activity_raw("d".to_string(), last).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_hashset_difference() -> u8 {
    1
}

pub async fn wf_hashset_union(ctx: &WorkflowContext) -> u64 {
    let a: HashSet<u64> = HashSet::from([1, 2, 3]);
    let b: HashSet<u64> = HashSet::from([2]);
    let mut last = 0_u64;
    for x in a.union(&b) {
        last = *x;
    }
    ctx.execute_activity_raw("u".to_string(), last).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_hashset_union() -> u8 {
    1
}

// ── Forbidden effects rustc prints trimmed to one segment ───────────────────

pub async fn wf_thread_sleep(ctx: &WorkflowContext) -> u64 {
    std::thread::sleep(Duration::from_millis(1));
    ctx.execute_activity_raw("s".to_string(), 1).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_thread_sleep() -> u8 {
    1
}

pub async fn wf_thread_spawn(ctx: &WorkflowContext) -> u64 {
    let handle = std::thread::spawn(|| 7_u64);
    let v = handle.join().unwrap_or(0);
    ctx.execute_activity_raw("j".to_string(), v).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_thread_spawn() -> u8 {
    1
}

/// A *user* fn named `sleep`, with a body: the single-segment `[[forbidden]]`
/// row must not fire on it.
pub fn sleep(ticks: u64) -> u64 {
    ticks
}
pub async fn wf_user_named_sleep(ctx: &WorkflowContext) -> u64 {
    let v = sleep(3);
    ctx.execute_activity_raw("us".to_string(), v).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_user_named_sleep() -> u8 {
    1
}

// ── False-positive traps: these must stay proven ────────────────────────────

/// C04-style: branching on `ctx.version()` is history-clean.
pub async fn wf_fp_version_branch(ctx: &WorkflowContext) -> u64 {
    let n = if ctx.version() > 1 { 10_u64 } else { 20_u64 };
    ctx.execute_activity_raw("v".to_string(), n).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_fp_version_branch() -> u8 {
    1
}

/// `?` on a clean `Result` must not make the rest of the body control-tainted.
pub async fn wf_fp_try_chain(ctx: &WorkflowContext) -> Result<u64, String> {
    let amount = parse_amount("17")?;
    ctx.execute_activity_raw("t".to_string(), amount)
}
pub fn __autumn_workflow_info_wf_fp_try_chain() -> u8 {
    1
}

/// A clean `if` over clean data decides a clean value.
pub async fn wf_fp_clean_branch(ctx: &WorkflowContext) -> u64 {
    let n = if parse_amount("2").unwrap_or(0) > 1 {
        10_u64
    } else {
        20_u64
    };
    ctx.execute_activity_raw("cb".to_string(), n).unwrap_or(0)
}
pub fn __autumn_workflow_info_wf_fp_clean_branch() -> u8 {
    1
}
