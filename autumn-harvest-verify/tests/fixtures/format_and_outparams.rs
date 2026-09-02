//! Fixture: every laundering / boundary MIR shape the analyzer must handle.
//!
//! Compiled standalone (no workspace deps) with:
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o format_and_outparams.mir format_and_outparams.rs
//!
//! `WorkflowContext` is a *stand-in* for the real `autumn_harvest::WorkflowContext`:
//! the model matches sinks/sanctioned primitives on the receiver's LAST path
//! segment plus the method name, so a local struct with the same name and the
//! same method names exercises the same rules without pulling the engine in.
//!
//! Every workflow-like fn has an `__autumn_workflow_info_<name>` companion, which
//! is what `entry::discover` keys on.
#![allow(dead_code, unused_variables, unused_unsafe, static_mut_refs)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ── The stand-in workflow context ───────────────────────────────────────────

pub struct WorkflowContext {
    pub version: u32,
}

impl WorkflowContext {
    /// Sink: emits a command; both arguments are checked.
    pub fn execute_activity_raw(&self, name: String, arg: u64) -> Result<u64, String> {
        Ok(arg)
    }
    /// Sanctioned: recorded once, replayed verbatim.
    pub fn system_now(&self) -> u64 {
        0
    }
    /// Sanctioned: the closure is NOT descended; the return is clean.
    pub fn side_effect<T, F: FnOnce() -> T>(&self, key: &str, f: F) -> T {
        f()
    }
    /// Sink (durable timer).
    pub fn timer(&self, secs: u64) {}
    /// Non-sink observability.
    pub fn metrics(&self) -> u64 {
        0
    }
    /// Non-sink history metadata (branch on it is clean).
    pub fn version(&self) -> u32 {
        self.version
    }
}

// ── Ambient state: the four printed shapes of a static read ────────────────

/// Immutable plain data — reading it is deterministic.
pub static PLAIN_LIMIT: u64 = 7;
/// Interior-mutable — reading it is an ambient `Value` source.
pub static COUNTER: AtomicU64 = AtomicU64::new(0);
/// The initializer is four resolution hops from the use site.
pub static LAZY_START: LazyLock<u64> = LazyLock::new(wall_clock_secs);
pub static ONCE_START: OnceLock<u64> = OnceLock::new();
/// `static mut` read through a raw pointer.
pub static mut RAW_COUNTER: u64 = 0;

thread_local! {
    pub static TL_SEQ: RefCell<u64> = RefCell::new(0);
}

// ── FFI ─────────────────────────────────────────────────────────────────────

#[repr(C)]
pub struct Timespec {
    pub tv_sec: i64,
    pub tv_nsec: i64,
}

unsafe extern "C" {
    pub fn clock_gettime(clk_id: i32, tp: *mut Timespec) -> i32;
}

// ── Helpers (the trace must name these) ─────────────────────────────────────

pub fn wall_clock_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The canonical laundering helper: a tainted `u64` reaches a `String` through
/// a tuple aggregate, `Argument::new_display`, an array aggregate and `format`.
pub fn stamped_name(prefix: &str) -> String {
    let secs = wall_clock_secs();
    format!("{prefix}-{secs}")
}

/// `&mut` out-param: the return type is `()`, the taint lands in `*dst`.
pub fn fill_seq(dst: &mut u64) {
    *dst = COUNTER.load(Ordering::SeqCst);
}

/// Clean fallible helper — the `?` on its result must not be control taint.
pub fn parse_amount(raw: &str) -> Result<u64, String> {
    raw.parse::<u64>().map_err(|_| "bad amount".to_string())
}

pub fn one() -> u64 {
    1
}

pub fn pick_source(sel: bool) -> fn() -> u64 {
    if sel { wall_clock_secs } else { one }
}

// ── A trait with exactly ONE impl unsized to `dyn` (devirtualizable) ────────

pub trait Clock {
    fn now_secs(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> u64 {
        wall_clock_secs()
    }
}

// ── A trait with TWO impls unsized to `dyn` (a real boundary) ──────────────

pub trait Namer {
    fn name(&self) -> String;
}

pub struct StaticNamer;

impl Namer for StaticNamer {
    fn name(&self) -> String {
        "static".to_string()
    }
}

pub struct CountingNamer;

impl Namer for CountingNamer {
    fn name(&self) -> String {
        format!("n{}", COUNTER.load(Ordering::SeqCst))
    }
}

// ── Workflow-like fns, one laundering mechanism each ────────────────────────

pub fn __autumn_workflow_info_wf_format_into_activity_name() -> u8 {
    0
}

/// `format!` of a wall-clock read into the activity NAME.
pub async fn wf_format_into_activity_name(ctx: &WorkflowContext) -> Result<u64, String> {
    let name = stamped_name("charge");
    ctx.execute_activity_raw(name, 1)
}

pub fn __autumn_workflow_info_wf_out_param_launder() -> u8 {
    0
}

/// Taint arrives through a `&mut` out-param on a `()`-returning call.
pub async fn wf_out_param_launder(ctx: &WorkflowContext) -> Result<u64, String> {
    let mut seq = 0_u64;
    fill_seq(&mut seq);
    ctx.execute_activity_raw("charge".to_string(), seq)
}

pub fn __autumn_workflow_info_wf_side_effect_is_clean() -> u8 {
    0
}

/// The same wall-clock read, captured once through `side_effect` — clean.
pub async fn wf_side_effect_is_clean(ctx: &WorkflowContext) -> Result<u64, String> {
    let secs = ctx.side_effect("start", || wall_clock_secs());
    ctx.execute_activity_raw("charge".to_string(), secs)
}

pub fn __autumn_workflow_info_wf_hashmap_iteration() -> u8 {
    0
}

/// Hash-seeded iteration order decides the command SEQUENCE.
pub async fn wf_hashmap_iteration(
    ctx: &WorkflowContext,
    m: HashMap<String, u64>,
) -> Result<u64, String> {
    let mut total = 0_u64;
    for (k, v) in &m {
        total += ctx.execute_activity_raw(k.clone(), *v)?;
    }
    Ok(total)
}

pub fn __autumn_workflow_info_wf_sorted_keys() -> u8 {
    0
}

/// The same map, iterated in sorted order — deterministic.
pub async fn wf_sorted_keys(ctx: &WorkflowContext, m: HashMap<String, u64>) -> Result<u64, String> {
    let mut keys: Vec<String> = m.keys().cloned().collect();
    keys.sort();
    let mut total = 0_u64;
    for k in keys {
        total += ctx.execute_activity_raw(k, 1)?;
    }
    Ok(total)
}

pub fn __autumn_workflow_info_wf_branch_on_wallclock() -> u8 {
    0
}

/// The command is emitted on one side of a wall-clock branch only.
pub async fn wf_branch_on_wallclock(ctx: &WorkflowContext) -> Result<u64, String> {
    if wall_clock_secs() % 2 == 0 {
        ctx.execute_activity_raw("even".to_string(), 0)
    } else {
        Ok(0)
    }
}

pub fn __autumn_workflow_info_wf_branch_on_version() -> u8 {
    0
}

/// Branching on history-recorded metadata is the *sanctioned* versioning idiom.
pub async fn wf_branch_on_version(ctx: &WorkflowContext) -> Result<u64, String> {
    if ctx.version() >= 2 {
        ctx.execute_activity_raw("v2".to_string(), 0)
    } else {
        Ok(0)
    }
}

pub fn __autumn_workflow_info_wf_try_on_clean_result() -> u8 {
    0
}

/// `?` desugars to `Try::branch` + `switchInt(discriminant(..))` on clean data.
pub async fn wf_try_on_clean_result(ctx: &WorkflowContext, raw: String) -> Result<u64, String> {
    let amount = parse_amount(&raw)?;
    ctx.execute_activity_raw("charge".to_string(), amount)
}

pub fn __autumn_workflow_info_wf_await_is_clean() -> u8 {
    0
}

pub async fn tick(v: u64) -> u64 {
    v + 1
}

/// The coroutine's own `switchInt(discriminant((*_N)))` is not control taint.
pub async fn wf_await_is_clean(ctx: &WorkflowContext) -> Result<u64, String> {
    let a = tick(1).await;
    let b = tick(a).await;
    ctx.execute_activity_raw("charge".to_string(), a + b)
}

pub fn __autumn_workflow_info_wf_lazylock_deref() -> u8 {
    0
}

/// `*LAZY_START` prints as an innocuous `Deref::deref`.
pub async fn wf_lazylock_deref(ctx: &WorkflowContext) -> Result<u64, String> {
    let start = *LAZY_START;
    ctx.execute_activity_raw("charge".to_string(), start)
}

pub fn __autumn_workflow_info_wf_oncelock_get_or_init() -> u8 {
    0
}

/// The initializer identity lives only in the turbofish / `const ZeroSized:` operand.
pub async fn wf_oncelock_get_or_init(ctx: &WorkflowContext) -> Result<u64, String> {
    let start = *ONCE_START.get_or_init(|| wall_clock_secs());
    ctx.execute_activity_raw("charge".to_string(), start)
}

pub fn __autumn_workflow_info_wf_plain_static_is_clean() -> u8 {
    0
}

/// Textually identical to the `COUNTER` read; deterministic because of its TYPE.
pub async fn wf_plain_static_is_clean(ctx: &WorkflowContext) -> Result<u64, String> {
    ctx.execute_activity_raw("charge".to_string(), PLAIN_LIMIT)
}

pub fn __autumn_workflow_info_wf_static_mut_raw_read() -> u8 {
    0
}

pub async fn wf_static_mut_raw_read(ctx: &WorkflowContext) -> Result<u64, String> {
    let n = unsafe { *(&raw const RAW_COUNTER) };
    ctx.execute_activity_raw("charge".to_string(), n)
}

pub fn __autumn_workflow_info_wf_ffi_clock() -> u8 {
    0
}

pub async fn wf_ffi_clock(ctx: &WorkflowContext) -> Result<u64, String> {
    let mut ts = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { clock_gettime(0, &raw mut ts) };
    let _ = rc;
    ctx.execute_activity_raw("charge".to_string(), ts.tv_sec as u64)
}

pub fn __autumn_workflow_info_wf_fn_pointer() -> u8 {
    0
}

pub async fn wf_fn_pointer(ctx: &WorkflowContext, sel: bool) -> Result<u64, String> {
    let f = pick_source(sel);
    ctx.execute_activity_raw("charge".to_string(), f())
}

pub fn __autumn_workflow_info_wf_dyn_two_impls() -> u8 {
    0
}

pub async fn wf_dyn_two_impls(ctx: &WorkflowContext, sel: bool) -> Result<u64, String> {
    let n: Box<dyn Namer> = if sel {
        Box::new(StaticNamer)
    } else {
        Box::new(CountingNamer)
    };
    ctx.execute_activity_raw(n.name(), 0)
}

pub fn __autumn_workflow_info_wf_dyn_single_impl() -> u8 {
    0
}

/// Exactly one type is unsized to `dyn Clock` in this crate, and it reads time.
pub async fn wf_dyn_single_impl(ctx: &WorkflowContext) -> Result<u64, String> {
    let c: Box<dyn Clock> = Box::new(SystemClock);
    ctx.execute_activity_raw("charge".to_string(), c.now_secs())
}

pub fn __autumn_workflow_info_wf_option_map_closure() -> u8 {
    0
}

/// The closure body is a separate item; the call site shows only the turbofish.
pub async fn wf_option_map_closure(
    ctx: &WorkflowContext,
    o: Option<u64>,
) -> Result<u64, String> {
    let v = o
        .map(|x| x + COUNTER.load(Ordering::SeqCst))
        .unwrap_or_default();
    ctx.execute_activity_raw("charge".to_string(), v)
}

pub fn __autumn_workflow_info_wf_sort_by_ambient_comparator() -> u8 {
    0
}

/// The VALUES are clean; the ORDER is decided by an ambient counter.
pub async fn wf_sort_by_ambient_comparator(
    ctx: &WorkflowContext,
    mut xs: Vec<u64>,
) -> Result<u64, String> {
    xs.sort_by(|a, b| {
        let skew = COUNTER.load(Ordering::SeqCst);
        (a ^ skew).cmp(&(b ^ skew))
    });
    let mut total = 0_u64;
    for x in xs {
        total += ctx.execute_activity_raw("charge".to_string(), x)?;
    }
    Ok(total)
}

pub fn __autumn_workflow_info_wf_thread_local_counter() -> u8 {
    0
}

/// The `LocalKey` operand is a promoted constant two hops from the read.
pub async fn wf_thread_local_counter(ctx: &WorkflowContext) -> Result<u64, String> {
    let seq = TL_SEQ.with(|c| *c.borrow());
    ctx.execute_activity_raw("charge".to_string(), seq)
}

pub fn __autumn_workflow_info_wf_observability_is_clean() -> u8 {
    0
}

/// `metrics` is a non-sink; `system_now` is sanctioned; `timer` is a sink with
/// a clean argument.
pub async fn wf_observability_is_clean(ctx: &WorkflowContext) -> Result<u64, String> {
    let _ = ctx.metrics();
    let now = ctx.system_now();
    ctx.timer(30);
    ctx.execute_activity_raw("charge".to_string(), now)
}
