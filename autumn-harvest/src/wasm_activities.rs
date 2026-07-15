//! Pure, feature-gated WebAssembly activity runtime (issue #965).
//!
//! This module provides the sandboxed execution primitive for polyglot
//! activities: a wasmtime [`Engine`] configured for deterministic resource
//! bounding (CPU fuel + wall-clock epoch interruption + a memory ceiling) and a
//! capability-gated host surface that is **deny-all by default**. It is the
//! runtime half of issue #965; the storage layer (module publishing/resolution)
//! and the worker dispatch seam are built on top of it in later milestones.
//!
//! Everything here is gated behind the `wasm-activities` Cargo feature, so a
//! default build pulls in neither `wasmtime` nor `wat` and is byte-for-byte
//! unchanged.
//!
//! # Guest ABI — JSON over linear memory
//!
//! A guest module participates in the runtime by exporting exactly three
//! things:
//!
//! - `memory` — its linear memory.
//! - `alloc(len: i32) -> i32` — return a pointer to `len` writable bytes inside
//!   guest memory (typically a bump allocator). The host calls this once to
//!   place the serialized activity input.
//! - `run(in_ptr: i32, in_len: i32) -> i64` — execute the activity. The input
//!   JSON bytes live at `in_ptr..in_ptr+in_len`. The return value packs the
//!   output location as `((out_ptr as i64) << 32) | (out_len as i64)`; the host
//!   reads `out_len` bytes at `out_ptr` and deserializes them as JSON.
//!
//! The host unpacks the returned `i64` by reinterpreting it as `u64` **before**
//! shifting, so a high bit in `out_ptr` never sign-extends. Every host-side read
//! and write is bounds-checked against live guest memory: an out-of-range
//! `(ptr, len)` becomes a typed [`ActivityFailure::wasm_trap`] rather than a
//! host out-of-bounds read or panic.
//!
//! # Sandbox model
//!
//! The [`Linker`] starts empty (deny-all). Host functions are linked **only**
//! for capabilities the caller explicitly granted via [`WasmCapabilities`]. A
//! guest that imports an ungranted host function fails at instantiation with a
//! non-retryable [`ActivityFailure::sandbox_denied`]. Filesystem and network
//! access are **not grantable** in this spike — there is no host function for
//! them, so any import naming them is unsatisfied and denied.
//!
//! Randomness (`env::random_u64`) is a non-cryptographic xorshift generator and
//! must not be used for security-sensitive draws.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use lru::LruCache;
use tokio_util::sync::CancellationToken;
use wasmtime::{
    Caller, Config, Engine, Extern, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap,
    UpdateDeadline,
};

use crate::error::HarvestError;
use crate::failure::ActivityFailure;

/// Default guest linear-memory ceiling: 16 MiB.
pub const DEFAULT_MEMORY_BYTES: usize = 16 * 1024 * 1024;

/// Default CPU fuel budget for a single guest invocation.
pub const DEFAULT_FUEL: u64 = 100_000_000;

/// Default hard wall-clock ceiling for a single guest invocation: 5 minutes.
///
/// This is the *mandatory* upper bound — even a caller that passes no
/// per-call deadline (or a larger one) is clamped to this. A guest can never be
/// bounded by fuel alone, so this ceiling guarantees termination.
pub const DEFAULT_MAX_WALL_CLOCK: Duration = Duration::from_secs(300);

/// How often the shared epoch ticker advances the engine's epoch counter.
///
/// Each store's wall-clock deadline is expressed as a number of these ticks
/// beyond the current epoch, so the resolution of the wall-clock bound is one
/// tick interval.
pub const EPOCH_TICK_INTERVAL: Duration = Duration::from_millis(1);

/// Default maximum number of distinct compiled module versions held in the
/// in-process content-hash cache (issue #965 review).
///
/// Old module versions are deliberately kept fetchable in Postgres, so without
/// a bound a long-lived worker that is hot-swapped many times would accumulate
/// compiled code for every historical version it ever invoked. The cache is an
/// LRU: at capacity, the least-recently-used compiled module is evicted.
/// Evicting an `Arc<Module>` is safe even while an in-flight invocation still
/// holds a clone — the invocation keeps its module alive, and a future miss for
/// that hash simply recompiles.
pub const WASM_MODULE_CACHE_CAP: usize = 64;

/// Host capabilities granted to a single WASM activity invocation.
///
/// The default is **deny-all**: no clock, no randomness, no environment access.
/// A guest that imports a host function it was not granted fails at
/// instantiation with a non-retryable [`ActivityFailure::sandbox_denied`].
///
/// Filesystem and network access are intentionally not representable here: no
/// host function backs them in this spike, so a guest importing them is denied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WasmCapabilities {
    /// Grant `env::now_millis() -> i64` (Unix-epoch milliseconds).
    pub allow_clock: bool,
    /// Grant `env::random_u64() -> i64` (non-cryptographic xorshift draw).
    pub allow_random: bool,
    /// Grant `env::env_get(...)`, restricted to this exact-match allowlist of
    /// process-environment keys. Empty means the host function is not linked.
    pub allow_env: Vec<String>,
}

impl WasmCapabilities {
    /// Whether the `env::env_get` host function should be linked at all.
    #[must_use]
    pub const fn allows_env(&self) -> bool {
        !self.allow_env.is_empty()
    }
}

/// Resource budget for a single WASM activity invocation.
///
/// All three bounds are enforced per attempt against a fresh store. `fuel`
/// bounds CPU work deterministically; `memory_bytes` caps linear-memory growth;
/// `max_wall_clock` is the hard termination ceiling (a guest can spin without
/// consuming fuel indefinitely, so a wall-clock bound is mandatory).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmLimits {
    /// Maximum bytes of guest linear memory.
    pub memory_bytes: usize,
    /// CPU fuel budget consumed by executed instructions.
    pub fuel: u64,
    /// Hard wall-clock ceiling for the invocation.
    pub max_wall_clock: Duration,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            memory_bytes: DEFAULT_MEMORY_BYTES,
            fuel: DEFAULT_FUEL,
            max_wall_clock: DEFAULT_MAX_WALL_CLOCK,
        }
    }
}

/// Per-invocation host state carried on the wasmtime [`Store`].
///
/// Holds the store's resource limiter (required so `Store::limiter` can hand
/// back a `&mut StoreLimits`) and the invocation-local RNG state used by the
/// `env::random_u64` host function.
struct HostState {
    limits: StoreLimits,
    rng: u64,
}

/// Process-wide RNG seed source. Advanced by a golden-ratio increment per
/// invocation so successive stores get distinct (non-cryptographic) seeds
/// without depending on the wall clock.
static RNG_SEED_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Non-cryptographic xorshift64 step. Requires a nonzero state.
const fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Convert a wall-clock [`Duration`] into a number of epoch ticks.
///
/// Rounds up and clamps to at least 1 (a deadline of 0 ticks has always
/// "elapsed" and would trap immediately). Saturates to [`u64::MAX`] for
/// durations that do not fit, so an absurd deadline never wraps to a tiny one.
#[must_use]
pub fn deadline_ticks(d: Duration) -> u64 {
    let interval = EPOCH_TICK_INTERVAL.as_nanos();
    // EPOCH_TICK_INTERVAL is a fixed nonzero constant.
    debug_assert!(interval > 0, "epoch tick interval must be nonzero");
    let want = d.as_nanos();
    // Ceiling division, then clamp to >= 1.
    let ticks = want.div_ceil(interval);
    u64::try_from(ticks).unwrap_or(u64::MAX).max(1)
}

/// Owns the wasmtime [`Engine`], a content-addressed compiled-module cache, and
/// a single background epoch ticker.
///
/// One `WasmModuleStore` should be created per worker process and shared across
/// invocations. The engine is configured once for fuel consumption and epoch
/// interruption; compiled modules are cached by their content hash so a given
/// module version is compiled at most once. A single named background thread
/// (`harvest-wasm-epoch`) advances the engine's epoch every
/// [`EPOCH_TICK_INTERVAL`] for the store's lifetime, so every concurrent
/// invocation's independent wall-clock deadline is driven by the same monotonic
/// clock without one guest's expiry affecting another.
pub struct WasmModuleStore {
    engine: Engine,
    modules: RwLock<LruCache<String, Arc<Module>>>,
    ticker_stop: Arc<AtomicBool>,
    ticker: Option<JoinHandle<()>>,
}

impl WasmModuleStore {
    /// Construct a store: configure the engine, start the epoch ticker thread.
    ///
    /// Uses the default compiled-module cache capacity
    /// ([`WASM_MODULE_CACHE_CAP`]); call [`WasmModuleStore::with_cache_capacity`]
    /// to override it.
    ///
    /// # Panics
    ///
    /// Panics if the wasmtime engine cannot be built from its fixed, valid
    /// configuration (fuel + epoch interruption), or if the OS refuses to spawn
    /// the epoch ticker thread. Both are unrecoverable process-startup faults,
    /// not guest-controlled conditions.
    #[must_use]
    pub fn new() -> Self {
        Self::with_cache_capacity(WASM_MODULE_CACHE_CAP)
    }

    /// Construct a store with an explicit compiled-module cache capacity.
    ///
    /// A `cap` of 0 is floored to 1 (the LRU always holds at least one entry).
    ///
    /// # Panics
    ///
    /// Panics under the same fixed-config / thread-spawn faults as
    /// [`WasmModuleStore::new`].
    #[must_use]
    pub fn with_cache_capacity(cap: usize) -> Self {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config)
            .expect("wasmtime engine construction from a fixed valid config never fails");

        let ticker_stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&ticker_stop);
        let ticker_engine = engine.clone();
        let ticker = std::thread::Builder::new()
            .name("harvest-wasm-epoch".to_string())
            .spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    std::thread::sleep(EPOCH_TICK_INTERVAL);
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    ticker_engine.increment_epoch();
                }
            })
            .expect("failed to spawn the harvest-wasm-epoch ticker thread");

        // Floor at 1 so the LRU is always non-empty; `NonZeroUsize::new(1)` is
        // infallible so the fallback branch is never taken.
        let cap = NonZeroUsize::new(cap.max(1)).unwrap_or(NonZeroUsize::MIN);
        Self {
            engine,
            modules: RwLock::new(LruCache::new(cap)),
            ticker_stop,
            ticker: Some(ticker),
        }
    }

    /// Borrow the underlying wasmtime engine.
    #[must_use]
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Compute the lowercase-hex SHA-256 content hash of module bytes.
    #[must_use]
    pub fn compute_hash(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        let digest = Sha256::digest(bytes);
        let mut out = String::with_capacity(64);
        for b in digest {
            let _ = write!(out, "{b:02x}");
        }
        out
    }

    /// Return a cached compiled module by content hash, if present.
    ///
    /// A hit marks the entry most-recently-used (LRU recency), so the modules a
    /// worker actually dispatches stay resident and idle historical versions age
    /// out first. Takes the write lock because the LRU recency bump mutates the
    /// map.
    #[must_use]
    pub fn cached(&self, hash: &str) -> Option<Arc<Module>> {
        self.modules
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .get(hash)
            .cloned()
    }

    /// Number of compiled module versions currently resident in the LRU cache.
    ///
    /// Bounded by the store's cache capacity ([`WASM_MODULE_CACHE_CAP`] by
    /// default); primarily an observability/test accessor.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.modules
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Resolve `hash` to a compiled [`Module`], compiling and caching on first
    /// use.
    ///
    /// The claimed `hash` is verified against `bytes` **before** compilation, so
    /// a mismatch (corruption, or a lookup returning bytes for a different
    /// version) is rejected as a content-integrity error rather than silently
    /// compiling the wrong code.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if the bytes do not match the claimed
    /// hash, or if wasmtime fails to compile them.
    pub fn get_or_compile(&self, hash: &str, bytes: &[u8]) -> Result<Arc<Module>, HarvestError> {
        if let Some(module) = self.cached(hash) {
            return Ok(module);
        }
        let actual = Self::compute_hash(bytes);
        if actual != hash {
            return Err(HarvestError::Config(format!(
                "wasm module content integrity check failed: claimed hash {hash}, actual {actual}"
            )));
        }
        let module = Module::new(&self.engine, bytes).map_err(|e| {
            HarvestError::Config(format!("failed to compile wasm module {hash}: {e}"))
        })?;
        let module = Arc::new(module);
        // `put` inserts and, at capacity, evicts the least-recently-used entry.
        self.modules
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .put(hash.to_string(), Arc::clone(&module));
        Ok(module)
    }
}

impl Default for WasmModuleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WasmModuleStore {
    fn drop(&mut self) {
        self.ticker_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.ticker.take() {
            let _ = handle.join();
        }
    }
}

/// Which phase produced a wasmtime error, deciding how a non-trap,
/// non-resource-exhaustion error is classified.
#[derive(Clone, Copy)]
enum WasmErrPhase {
    /// An error from a guest `alloc`/`run` call, or from the store's epoch
    /// callback: a residual (non-trap, non-resource) error is a guest trap.
    Runtime,
    /// An error from `Linker::instantiate`: a residual error is an unsatisfied
    /// import / link failure — a non-retryable [`ActivityFailure::sandbox_denied`].
    /// Wasmtime also runs the module's **start section** during `instantiate`,
    /// so a start-time fuel/epoch/memory/trap failure is classified as the
    /// matching retryable failure below, exactly like `alloc`/`run`.
    Instantiate,
}

/// Classify a wasmtime execution error into a typed [`ActivityFailure`],
/// phase-aware.
///
/// Traps and resource-exhaustion errors are classified identically regardless
/// of phase; only a residual error (neither a trap nor a memory-growth failure)
/// differs — a `Runtime` residual is a `WasmTrap`, an `Instantiate` residual is
/// a `SandboxDenied` (unsatisfied import). This is why a start-section trap is
/// never misreported as a permanent capability denial (issue #965 review).
fn classify_wasmtime_err_phase(err: &wasmtime::Error, phase: WasmErrPhase) -> ActivityFailure {
    if let Some(trap) = err.downcast_ref::<Trap>() {
        match trap {
            Trap::OutOfFuel => return ActivityFailure::resource_exhausted("cpu fuel exhausted"),
            Trap::Interrupt => {
                return ActivityFailure::resource_exhausted("wall-clock deadline exceeded");
            }
            // A memory-grow failure never surfaces as a `Trap` variant (it comes
            // from the store limiter), so any other trap here is a genuine guest
            // trap regardless of phase.
            _ => return ActivityFailure::wasm_trap(format!("wasm guest trapped: {err}")),
        }
    }
    let debug = format!("{err:?}");
    if debug.contains("growing memory") || debug.contains("growing table") {
        return ActivityFailure::resource_exhausted("memory limit exceeded");
    }
    match phase {
        WasmErrPhase::Runtime => ActivityFailure::wasm_trap(format!("wasm guest trapped: {err}")),
        WasmErrPhase::Instantiate => {
            ActivityFailure::sandbox_denied(format!("wasm instantiation denied: {err}"))
        }
    }
}

/// Link the granted host functions into `linker` for one invocation.
///
/// Only capabilities present in `caps` are linked; an ungranted host function
/// is simply absent, so a guest importing it fails at instantiation.
fn link_host_functions(
    linker: &mut Linker<HostState>,
    caps: &WasmCapabilities,
) -> Result<(), ActivityFailure> {
    let map_link_err = |what: &str, e: &wasmtime::Error| {
        ActivityFailure::wasm_trap(format!("failed to link {what}: {e}"))
    };

    if caps.allow_clock {
        linker
            .func_wrap("env", "now_millis", || -> i64 {
                let millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis());
                i64::try_from(millis).unwrap_or(i64::MAX)
            })
            .map_err(|e| map_link_err("env::now_millis", &e))?;
    }

    if caps.allow_random {
        linker
            .func_wrap(
                "env",
                "random_u64",
                |mut caller: Caller<'_, HostState>| -> i64 {
                    let state = &mut caller.data_mut().rng;
                    xorshift64(state).cast_signed()
                },
            )
            .map_err(|e| map_link_err("env::random_u64", &e))?;
    }

    if caps.allows_env() {
        let allowed = caps.allow_env.clone();
        linker
            .func_wrap(
                "env",
                "env_get",
                move |mut caller: Caller<'_, HostState>,
                      key_ptr: i32,
                      key_len: i32,
                      out_ptr: i32,
                      out_cap: i32|
                      -> i32 {
                    let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
                        return -1;
                    };
                    let (Ok(key_ptr), Ok(key_len)) =
                        (usize::try_from(key_ptr), usize::try_from(key_len))
                    else {
                        return -1;
                    };
                    // SECURITY (issue #965): NEVER allocate a host buffer sized
                    // to the guest-controlled `key_len`. A malicious/buggy guest
                    // can pass a huge positive `key_len` (e.g. i32::MAX ~2 GiB);
                    // a `vec![0u8; key_len]` allocation would bypass the wasm
                    // memory limit and OOM the worker process. Instead we SLICE
                    // guest memory directly: an out-of-range `(key_ptr, key_len)`
                    // yields `None` -> in-band miss (-1) with zero allocation.
                    // The only owned copy is `key.to_owned()`, taken solely after
                    // the exact-match allowlist passes, so it is bounded by the
                    // length of an allowlisted key (tiny). The immutable borrow
                    // of guest memory is scoped inside this block so the later
                    // `memory.write` can re-borrow the caller mutably.
                    let key_owned = {
                        let mem = memory.data(&caller);
                        let Some(key_bytes) = key_ptr
                            .checked_add(key_len)
                            .and_then(|end| mem.get(key_ptr..end))
                        else {
                            return -1;
                        };
                        let Ok(key) = std::str::from_utf8(key_bytes) else {
                            return -1;
                        };
                        if !allowed.iter().any(|k| k == key) {
                            return -1;
                        }
                        key.to_owned()
                    };
                    let Ok(value) = std::env::var(&key_owned) else {
                        return -1;
                    };
                    let bytes = value.as_bytes();
                    let (Ok(out_ptr), Ok(out_cap)) =
                        (usize::try_from(out_ptr), usize::try_from(out_cap))
                    else {
                        return -1;
                    };
                    // Only write when the value fits; always report the full
                    // length so a guest can re-allocate and call again.
                    if bytes.len() <= out_cap && memory.write(&mut caller, out_ptr, bytes).is_err()
                    {
                        return -1;
                    }
                    i32::try_from(bytes.len()).unwrap_or(-1)
                },
            )
            .map_err(|e| map_link_err("env::env_get", &e))?;
    }

    Ok(())
}

/// Invoke a compiled WASM activity module against a JSON input under the given
/// capabilities and resource limits.
///
/// A fresh [`Store`] is created per call with its own fuel, memory limiter, and
/// wall-clock epoch deadline. The effective deadline is
/// `deadline.map_or(limits.max_wall_clock, |d| d.min(limits.max_wall_clock))` —
/// always finite, so a guest that never consumes fuel still terminates. The
/// guest runs on the calling thread and cannot be torn down before this ceiling.
///
/// Every guest-controlled failure maps to a typed [`ActivityFailure`]: a denied
/// capability is a non-retryable [`ActivityFailure::sandbox_denied`]; a fuel,
/// memory, or wall-clock overrun is a retryable
/// [`ActivityFailure::resource_exhausted`]; a trap, ABI violation, or non-JSON
/// output is a retryable [`ActivityFailure::wasm_trap`]. A panic in the host
/// glue is caught and mapped to `wasm_trap` so the worker can never crash.
///
/// # Cooperative cancellation
///
/// When `cancel` is `Some`, the store installs a per-invocation epoch-deadline
/// callback that polls the token roughly every [`EPOCH_TICK_INTERVAL`]. If the
/// token is cancelled while the guest is running, the guest is trapped at the
/// next safe point (within ~1 tick) and the call returns a retryable
/// [`ActivityFailure::resource_exhausted`] instead of running to the wall-clock
/// ceiling. The callback is per-`Store`, so one invocation's cancellation never
/// affects another's, and it still enforces the mandatory
/// `limits.max_wall_clock` ceiling as the hard backstop.
///
/// # Errors
///
/// Returns an [`ActivityFailure`] classifying any sandbox denial, resource
/// exhaustion, guest trap, ABI violation, or contained host-glue panic.
///
/// This is the non-cancellable convenience form; the worker dispatch seam uses
/// [`invoke_wasm_activity_cancellable`] to thread a cancellation token in.
pub fn invoke_wasm_activity(
    store: &WasmModuleStore,
    module: &Module,
    input: &serde_json::Value,
    caps: &WasmCapabilities,
    limits: &WasmLimits,
    deadline: Option<Duration>,
) -> Result<serde_json::Value, ActivityFailure> {
    invoke_wasm_activity_cancellable(store, module, input, caps, limits, deadline, None)
}

/// Cancellable form of [`invoke_wasm_activity`].
///
/// Identical, but when `cancel` is `Some` the guest is cooperatively
/// interrupted (within ~1 [`EPOCH_TICK_INTERVAL`]) if the token fires while it
/// is running — instead of running to the mandatory `limits.max_wall_clock`
/// ceiling — returning a retryable
/// [`ActivityFailure::resource_exhausted`]. The interrupt is per-`Store`, so
/// one invocation's cancellation never affects another's, and the ceiling
/// remains the hard backstop for a guest that ignores the (implicit) signal.
///
/// # Errors
///
/// Returns an [`ActivityFailure`] classifying any sandbox denial, resource
/// exhaustion, guest trap, ABI violation, cooperative cancellation, or
/// contained host-glue panic.
pub fn invoke_wasm_activity_cancellable(
    store: &WasmModuleStore,
    module: &Module,
    input: &serde_json::Value,
    caps: &WasmCapabilities,
    limits: &WasmLimits,
    deadline: Option<Duration>,
    cancel: Option<CancellationToken>,
) -> Result<serde_json::Value, ActivityFailure> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        invoke_wasm_activity_inner(
            store,
            module,
            input,
            caps,
            limits,
            deadline,
            cancel.as_ref(),
        )
    }));
    match result {
        Ok(inner) => inner,
        Err(payload) => Err(ActivityFailure::wasm_trap(format!(
            "host glue panicked during wasm invocation: {}",
            crate::error::panic_message(payload)
        ))),
    }
}

#[allow(clippy::too_many_lines)]
fn invoke_wasm_activity_inner(
    store: &WasmModuleStore,
    module: &Module,
    input: &serde_json::Value,
    caps: &WasmCapabilities,
    limits: &WasmLimits,
    deadline: Option<Duration>,
    cancel: Option<&CancellationToken>,
) -> Result<serde_json::Value, ActivityFailure> {
    let engine = store.engine();

    let input_bytes = serde_json::to_vec(input).map_err(|e| {
        ActivityFailure::wasm_trap(format!("failed to serialize activity input as JSON: {e}"))
    })?;

    // Per-attempt fresh store with an independent limiter, fuel budget, and
    // wall-clock epoch deadline.
    let seed = RNG_SEED_COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed) | 1;
    let host = HostState {
        limits: StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            .trap_on_grow_failure(true)
            .build(),
        rng: seed,
    };
    let mut wasm_store = Store::new(engine, host);
    wasm_store.limiter(|h| &mut h.limits);
    wasm_store
        .set_fuel(limits.fuel)
        .map_err(|e| ActivityFailure::wasm_trap(format!("failed to set wasm fuel: {e}")))?;
    let effective = deadline.map_or(limits.max_wall_clock, |d| d.min(limits.max_wall_clock));

    // Wall-clock bound + cooperative cancellation via a per-invocation
    // epoch-deadline callback (issue #965 review). Arm the first deadline one
    // tick out, then drive the countdown from the callback: each tick it polls
    // the cancellation token and decrements the remaining budget, trapping (as
    // an epoch interrupt → retryable `ResourceExhausted`) when either the token
    // fires or the `effective` ceiling is reached. This is per-`Store`, so N
    // concurrent invocations keep independent deadlines off the single shared
    // ticker, and a cancelled guest is interrupted within ~1 tick instead of
    // running to the ceiling. A guest that completes before the first tick never
    // invokes the callback, so the fast path pays nothing.
    wasm_store.set_epoch_deadline(1);
    let mut remaining_ticks = deadline_ticks(effective);
    let cancel_for_cb = cancel.cloned();
    // Set by the callback when the wall-clock ceiling is reached, so the
    // post-call classifier reports "wall-clock deadline exceeded" without
    // depending on how wasmtime wraps the callback's returned error.
    let deadline_hit = Arc::new(AtomicBool::new(false));
    let deadline_hit_cb = Arc::clone(&deadline_hit);
    wasm_store.epoch_deadline_callback(move |_ctx| {
        if cancel_for_cb
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            // Trap at a safe point; the post-call cancellation check reclassifies
            // this into a "cancelled" `ResourceExhausted`.
            return Err(wasmtime::Error::from(Trap::Interrupt));
        }
        remaining_ticks = remaining_ticks.saturating_sub(1);
        if remaining_ticks == 0 {
            // Hard wall-clock ceiling reached → epoch interrupt.
            deadline_hit_cb.store(true, Ordering::Relaxed);
            return Err(wasmtime::Error::from(Trap::Interrupt));
        }
        Ok(UpdateDeadline::Continue(1))
    });

    // Classify an `instantiate`/`alloc`/`run` error, phase-aware. The epoch
    // callback's trap is token-agnostic, so reclassify here: a fired cancel
    // token → a retryable "cancelled" `ResourceExhausted` (the worker's own
    // cancellation handling supersedes it); a reached wall-clock ceiling →
    // `ResourceExhausted` (robust to error wrapping via the `deadline_hit`
    // flag); otherwise defer to `classify_wasmtime_err_phase` (fuel/memory/trap,
    // and — for `instantiate` — an unsatisfied import → `SandboxDenied`).
    let classify = |e: &wasmtime::Error, phase: WasmErrPhase| -> ActivityFailure {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            ActivityFailure::resource_exhausted("wasm activity cancelled before completion")
        } else if deadline_hit.load(Ordering::Relaxed) {
            ActivityFailure::resource_exhausted("wall-clock deadline exceeded")
        } else {
            classify_wasmtime_err_phase(e, phase)
        }
    };

    // Deny-all linker; only granted capabilities are linked.
    let mut linker: Linker<HostState> = Linker::new(engine);
    link_host_functions(&mut linker, caps)?;

    // An unsatisfied import (an ungranted host function) is denied here; a trap
    // or resource overrun in the module's start section is classified as the
    // matching retryable failure, NOT a permanent capability denial.
    let instance = linker
        .instantiate(&mut wasm_store, module)
        .map_err(|e| classify(&e, WasmErrPhase::Instantiate))?;

    let memory = instance
        .get_memory(&mut wasm_store, "memory")
        .ok_or_else(|| ActivityFailure::wasm_trap("wasm module does not export 'memory'"))?;

    let alloc = instance
        .get_typed_func::<i32, i32>(&mut wasm_store, "alloc")
        .map_err(|e| {
            ActivityFailure::wasm_trap(format!("wasm module missing valid 'alloc' export: {e}"))
        })?;
    let run = instance
        .get_typed_func::<(i32, i32), i64>(&mut wasm_store, "run")
        .map_err(|e| {
            ActivityFailure::wasm_trap(format!("wasm module missing valid 'run' export: {e}"))
        })?;

    let in_len = i32::try_from(input_bytes.len()).map_err(|_| {
        ActivityFailure::wasm_trap("activity input too large for the wasm abi (exceeds i32)")
    })?;
    let in_ptr = alloc
        .call(&mut wasm_store, in_len)
        .map_err(|e| classify(&e, WasmErrPhase::Runtime))?;
    let in_ptr_usize = usize::try_from(in_ptr)
        .map_err(|_| ActivityFailure::wasm_trap("alloc returned a negative pointer"))?;
    // memory.write is itself bounds-checked against live guest memory.
    memory
        .write(&mut wasm_store, in_ptr_usize, &input_bytes)
        .map_err(|_| {
            ActivityFailure::wasm_trap("alloc returned an out-of-bounds pointer for the input")
        })?;

    let packed = run
        .call(&mut wasm_store, (in_ptr, in_len))
        .map_err(|e| classify(&e, WasmErrPhase::Runtime))?;

    // Reinterpret as unsigned BEFORE shifting so a high bit never sign-extends.
    let bits = packed.cast_unsigned();
    let out_ptr = usize::try_from(bits >> 32)
        .map_err(|_| ActivityFailure::wasm_trap("wasm output pointer out of range"))?;
    let out_len = usize::try_from(bits & 0xFFFF_FFFF)
        .map_err(|_| ActivityFailure::wasm_trap("wasm output length out of range"))?;

    // Bounds-check the output range against live guest memory.
    let data = memory.data(&wasm_store);
    let end = out_ptr
        .checked_add(out_len)
        .ok_or_else(|| ActivityFailure::wasm_trap("wasm output pointer+length overflows"))?;
    let out_bytes = data
        .get(out_ptr..end)
        .ok_or_else(|| ActivityFailure::wasm_trap("wasm output range is out of bounds"))?;

    serde_json::from_slice(out_bytes)
        .map_err(|e| ActivityFailure::wasm_trap(format!("wasm output is not valid JSON: {e}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::failure::{
        ERROR_TYPE_RESOURCE_EXHAUSTED, ERROR_TYPE_SANDBOX_DENIED, ERROR_TYPE_WASM_TRAP,
    };
    use std::time::Instant;

    /// A correct bump-allocator echo guest: `run` returns `packed(in_ptr,
    /// in_len)`, so the host reads back the exact input bytes it wrote.
    const ECHO_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))
          (func (export "alloc") (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $len)))
            (local.get $ptr))
          (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $in_ptr)) (i64.const 32))
              (i64.extend_i32_u (local.get $in_len)))))
    "#;

    fn compile(store: &WasmModuleStore, wat: &str) -> Arc<Module> {
        let bytes = wat::parse_str(wat).expect("wat must assemble");
        let hash = WasmModuleStore::compute_hash(&bytes);
        store
            .get_or_compile(&hash, &bytes)
            .expect("module must compile")
    }

    fn fast_limits(fuel: u64) -> WasmLimits {
        WasmLimits {
            memory_bytes: DEFAULT_MEMORY_BYTES,
            fuel,
            max_wall_clock: Duration::from_secs(10),
        }
    }

    // ---- deadline_ticks -------------------------------------------------

    #[test]
    fn deadline_ticks_rounds_up_and_clamps_to_one() {
        assert_eq!(deadline_ticks(Duration::from_millis(1)), 1);
        // Sub-tick rounds up to a full tick, never zero.
        assert_eq!(deadline_ticks(Duration::from_nanos(1)), 1);
        assert_eq!(deadline_ticks(Duration::from_micros(1)), 1);
        assert_eq!(deadline_ticks(Duration::ZERO), 1);
        // 300s / 1ms = 300_000 ticks.
        assert_eq!(deadline_ticks(Duration::from_secs(300)), 300_000);
        // 1.5ms rounds up to 2 ticks.
        assert_eq!(deadline_ticks(Duration::from_micros(1500)), 2);
    }

    #[test]
    fn deadline_ticks_saturates_for_absurd_durations() {
        assert_eq!(deadline_ticks(Duration::MAX), u64::MAX);
    }

    // ---- hash / cache ---------------------------------------------------

    #[test]
    fn compute_hash_is_stable_lowercase_hex_sha256() {
        let h = WasmModuleStore::compute_hash(b"hello");
        // SHA-256 of "hello".
        assert_eq!(
            h,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(h.len(), 64);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn get_or_compile_caches_by_hash() {
        let store = WasmModuleStore::new();
        let bytes = wat::parse_str(ECHO_WAT).unwrap();
        let hash = WasmModuleStore::compute_hash(&bytes);
        let a = store.get_or_compile(&hash, &bytes).unwrap();
        let b = store.get_or_compile(&hash, &bytes).unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "second compile must return the cached Arc"
        );
        assert!(store.cached(&hash).is_some());
    }

    #[test]
    fn get_or_compile_rejects_hash_mismatch() {
        let store = WasmModuleStore::new();
        let bytes = wat::parse_str(ECHO_WAT).unwrap();
        let err = store
            .get_or_compile("deadbeef", &bytes)
            .expect_err("claimed hash does not match bytes");
        assert!(matches!(err, HarvestError::Config(_)));
    }

    // ---- echo roundtrip -------------------------------------------------

    #[test]
    fn echo_roundtrips_json_through_the_abi() {
        let store = WasmModuleStore::new();
        let module = compile(&store, ECHO_WAT);
        let input = serde_json::json!({"hello": "world", "n": 42});
        let out = invoke_wasm_activity(
            &store,
            &module,
            &input,
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("echo must succeed");
        assert_eq!(out, input);
    }

    // ---- sandbox deny: three categories ---------------------------------

    fn deny_guest_importing(module_fn: &str) -> String {
        format!(
            r#"
            (module
              (import "env" "{module_fn}" (func $imported (param i32) (result i32)))
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
            "#
        )
    }

    #[test]
    fn ungranted_fs_import_is_sandbox_denied() {
        let store = WasmModuleStore::new();
        let module = compile(&store, &deny_guest_importing("fs_read"));
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("ungranted fs import must be denied");
        assert_eq!(err.error_type, ERROR_TYPE_SANDBOX_DENIED);
        assert!(err.non_retryable);
    }

    #[test]
    fn ungranted_net_import_is_sandbox_denied() {
        let store = WasmModuleStore::new();
        let module = compile(&store, &deny_guest_importing("net_connect"));
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("ungranted net import must be denied");
        assert_eq!(err.error_type, ERROR_TYPE_SANDBOX_DENIED);
    }

    #[test]
    fn ungranted_env_get_import_is_sandbox_denied() {
        // env_get is not linked when the allowlist is empty.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (import "env" "env_get" (func $e (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("ungranted env_get must be denied");
        assert_eq!(err.error_type, ERROR_TYPE_SANDBOX_DENIED);
    }

    // ---- capability grant paths -----------------------------------------

    /// A guest that calls a granted host function, then returns a pre-stored
    /// JSON literal so the output is valid JSON regardless of the host value.
    fn grant_guest_calling(import: &str, params: &str, call: &str) -> String {
        format!(
            r#"
            (module
              (import "env" "{import}" (func $h {params}))
              (memory (export "memory") 1)
              (data (i32.const 2048) "true")
              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "run") (param i32 i32) (result i64)
                {call}
                (i64.or (i64.shl (i64.const 2048) (i64.const 32)) (i64.const 4))))
            "#
        )
    }

    #[test]
    fn granted_clock_instantiates_and_runs() {
        let store = WasmModuleStore::new();
        let wat = grant_guest_calling("now_millis", "(result i64)", "(drop (call $h))");
        let module = compile(&store, &wat);
        let caps = WasmCapabilities {
            allow_clock: true,
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("granted clock guest must run");
        assert_eq!(out, serde_json::json!(true));
    }

    #[test]
    fn granted_random_instantiates_and_runs() {
        let store = WasmModuleStore::new();
        let wat = grant_guest_calling("random_u64", "(result i64)", "(drop (call $h))");
        let module = compile(&store, &wat);
        let caps = WasmCapabilities {
            allow_random: true,
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("granted random guest must run");
        assert_eq!(out, serde_json::json!(true));
    }

    /// A guest that calls `env_get` for a fixed key and returns `true` (JSON)
    /// when the call returned -1 (denied/missing), else `false`.
    const ENV_PROBE_WAT: &str = r#"
        (module
          (import "env" "env_get" (func $e (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 100) "DENIED_KEY")
          (data (i32.const 200) "true")
          (data (i32.const 300) "false")
          (func (export "alloc") (param i32) (result i32) (i32.const 8192))
          (func (export "run") (param i32 i32) (result i64)
            (local $r i32)
            (local.set $r (call $e (i32.const 100) (i32.const 10) (i32.const 1000) (i32.const 64)))
            (if (result i64) (i32.eq (local.get $r) (i32.const -1))
              (then (i64.or (i64.shl (i64.const 200) (i64.const 32)) (i64.const 4)))
              (else (i64.or (i64.shl (i64.const 300) (i64.const 32)) (i64.const 5))))))
    "#;

    #[test]
    fn granted_env_instantiates_and_runs() {
        // A non-empty allowlist links env_get; the guest runs regardless of
        // whether its probed key matches.
        let store = WasmModuleStore::new();
        let module = compile(&store, ENV_PROBE_WAT);
        let caps = WasmCapabilities {
            allow_env: vec!["ALLOWED_KEY".to_string()],
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("granted env guest must run");
        // The probed key "DENIED_KEY" is not on the allowlist, so env_get
        // returns -1 and the guest reports `true`.
        assert_eq!(out, serde_json::json!(true));
    }

    #[test]
    fn env_get_denies_non_allowlisted_key_in_band() {
        // env_get is linked (allowlist non-empty) but "DENIED_KEY" is not on it,
        // so the host returns -1 without touching the process environment.
        let store = WasmModuleStore::new();
        let module = compile(&store, ENV_PROBE_WAT);
        let caps = WasmCapabilities {
            allow_env: vec!["SOME_OTHER_KEY".to_string()],
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("env guest must run");
        assert_eq!(
            out,
            serde_json::json!(true),
            "denied key returns -1 in band"
        );
    }

    /// SECURITY regression (issue #965): a guest that calls the granted
    /// `env_get` with an enormous positive `key_len` (`i32::MAX` ~2 GiB) must NOT
    /// cause the host to allocate a `key_len`-sized buffer. Pre-fix the host did
    /// `vec![0u8; key_len]` before the memory bounds check, so this call would
    /// try to allocate ~2 GiB and could OOM the worker. Post-fix the host slices
    /// guest memory directly: the out-of-range range yields a miss (-1) with zero
    /// allocation, the guest reports `true`, and no panic/OOM occurs.
    const ENV_HUGE_KEYLEN_WAT: &str = r#"
        (module
          (import "env" "env_get" (func $e (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 100) "ALLOWED_KEY")
          (data (i32.const 200) "true")
          (data (i32.const 300) "false")
          (func (export "alloc") (param i32) (result i32) (i32.const 8192))
          (func (export "run") (param i32 i32) (result i64)
            (local $r i32)
            ;; key_len = i32::MAX: a huge, positive, out-of-range length.
            (local.set $r
              (call $e (i32.const 100) (i32.const 2147483647) (i32.const 1000) (i32.const 64)))
            (if (result i64) (i32.eq (local.get $r) (i32.const -1))
              (then (i64.or (i64.shl (i64.const 200) (i64.const 32)) (i64.const 4)))
              (else (i64.or (i64.shl (i64.const 300) (i64.const 32)) (i64.const 5))))))
    "#;

    #[test]
    fn env_get_with_huge_key_len_is_a_miss_without_allocating() {
        let store = WasmModuleStore::new();
        let module = compile(&store, ENV_HUGE_KEYLEN_WAT);
        // "ALLOWED_KEY" IS on the allowlist, so env_get is linked and the key
        // WOULD match — but the absurd key_len makes the slice out-of-range, so
        // the host returns -1 before ever touching the allowlist, with no
        // allocation and no panic.
        let caps = WasmCapabilities {
            allow_env: vec!["ALLOWED_KEY".to_string()],
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("guest must run without OOM or panic despite the huge key_len");
        assert_eq!(
            out,
            serde_json::json!(true),
            "an out-of-range key_len must be an in-band miss (-1)"
        );
    }

    /// Regression: a normal granted `env_get` for an allowlisted key whose value
    /// exists in the process environment still writes the value and returns its
    /// full length. The guest echoes the written bytes back as its JSON output,
    /// proving both the returned length and the correct in-bounds write survive
    /// the slice-not-allocate rewrite.
    const ENV_HIT_ECHO_WAT: &str = r#"
        (module
          (import "env" "env_get" (func $e (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 100) "HARVEST_WASM_ENVGET_TEST")
          (func (export "alloc") (param i32) (result i32) (i32.const 8192))
          (func (export "run") (param i32 i32) (result i64)
            (local $r i32)
            ;; key at 100, len 24; write the value at 1000 (cap 64).
            (local.set $r
              (call $e (i32.const 100) (i32.const 24) (i32.const 1000) (i32.const 64)))
            ;; Return packed(out_ptr=1000, out_len=$r): the host reads back the
            ;; written value as the JSON output, so the returned length must be
            ;; exactly the value's byte length.
            (i64.or
              (i64.shl (i64.const 1000) (i64.const 32))
              (i64.extend_i32_u (local.get $r)))))
    "#;

    #[test]
    fn env_get_allowlisted_key_returns_value_length() {
        // Value is the JSON string literal `"hi"` (4 bytes incl. quotes) so the
        // guest's echoed output parses as json!("hi"). A unique key name avoids
        // clashing with any other test's process-env usage.
        // SAFETY (edition 2024): no other test reads this uniquely-named var; we
        // remove it after the invocation.
        unsafe {
            std::env::set_var("HARVEST_WASM_ENVGET_TEST", "\"hi\"");
        }
        let store = WasmModuleStore::new();
        let module = compile(&store, ENV_HIT_ECHO_WAT);
        let caps = WasmCapabilities {
            allow_env: vec!["HARVEST_WASM_ENVGET_TEST".to_string()],
            ..Default::default()
        };
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &caps,
            &fast_limits(DEFAULT_FUEL),
            None,
        );
        unsafe {
            std::env::remove_var("HARVEST_WASM_ENVGET_TEST");
        }
        let out = out.expect("granted env_get for an existing allowlisted key must run");
        assert_eq!(
            out,
            serde_json::json!("hi"),
            "env_get must write the value and return its full length"
        );
    }

    // ---- resource exhaustion --------------------------------------------

    const INFINITE_LOOP_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "run") (param i32 i32) (result i64)
            (loop $l (br $l))
            (i64.const 0)))
    "#;

    #[test]
    fn fuel_exhaustion_is_retryable_resource_exhausted() {
        let store = WasmModuleStore::new();
        let module = compile(&store, INFINITE_LOOP_WAT);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(10_000),
            None,
        )
        .expect_err("infinite loop must exhaust fuel");
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        assert!(!err.non_retryable, "resource exhaustion is retryable");
    }

    #[test]
    fn memory_limit_is_resource_exhausted() {
        // Guest grows linear memory far past its ceiling.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64)
                (drop (memory.grow (i32.const 1000)))
                (i64.const 0)))
        "#;
        let module = compile(&store, wat);
        let limits = WasmLimits {
            memory_bytes: 128 * 1024,
            fuel: DEFAULT_FUEL,
            max_wall_clock: Duration::from_secs(10),
        };
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &limits,
            None,
        )
        .expect_err("growing past the memory ceiling must fail");
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
    }

    #[test]
    fn wall_clock_deadline_is_resource_exhausted() {
        let store = WasmModuleStore::new();
        let module = compile(&store, INFINITE_LOOP_WAT);
        let start = Instant::now();
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &WasmLimits {
                memory_bytes: DEFAULT_MEMORY_BYTES,
                fuel: u64::MAX,
                max_wall_clock: Duration::from_secs(10),
            },
            Some(Duration::from_millis(200)),
        )
        .expect_err("wall-clock deadline must fire");
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "deadline must fire promptly"
        );
    }

    #[test]
    fn mandatory_wall_clock_ceiling_bounds_a_deadlineless_call() {
        // No per-call deadline and effectively infinite fuel: the mandatory
        // `limits.max_wall_clock` ceiling must still terminate the guest.
        let store = WasmModuleStore::new();
        let module = compile(&store, INFINITE_LOOP_WAT);
        let start = Instant::now();
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &WasmLimits {
                memory_bytes: DEFAULT_MEMORY_BYTES,
                fuel: u64::MAX,
                max_wall_clock: Duration::from_millis(200),
            },
            None,
        )
        .expect_err("mandatory ceiling must terminate the guest");
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the mandatory ceiling must fire without hanging"
        );
    }

    // ---- concurrent deadline isolation (the key M1 test) ----------------

    #[test]
    fn concurrent_deadlines_are_independent() {
        // On ONE store (one engine, one shared epoch ticker), a short-deadline
        // spin-loop must expire while a concurrent long-deadline bounded-work
        // guest completes: one guest's expiry must not trip another's.
        let store = WasmModuleStore::new();
        let spin = compile(&store, INFINITE_LOOP_WAT);
        let echo = compile(&store, ECHO_WAT);
        let input = serde_json::json!({"ok": true});

        std::thread::scope(|scope| {
            let short = scope.spawn(|| {
                invoke_wasm_activity(
                    &store,
                    &spin,
                    &serde_json::json!(null),
                    &WasmCapabilities::default(),
                    &WasmLimits {
                        memory_bytes: DEFAULT_MEMORY_BYTES,
                        fuel: u64::MAX,
                        max_wall_clock: Duration::from_secs(30),
                    },
                    Some(Duration::from_millis(100)),
                )
            });
            let long = scope.spawn(|| {
                // Long deadline, but the guest does tiny bounded work and
                // returns almost immediately — it must NOT be tripped by the
                // sibling's short-deadline expiry.
                invoke_wasm_activity(
                    &store,
                    &echo,
                    &input,
                    &WasmCapabilities::default(),
                    &fast_limits(DEFAULT_FUEL),
                    Some(Duration::from_secs(5)),
                )
            });

            let short_res = short.join().expect("short thread must not panic");
            let long_res = long.join().expect("long thread must not panic");

            assert_eq!(
                short_res
                    .expect_err("short-deadline spin must expire")
                    .error_type,
                ERROR_TYPE_RESOURCE_EXHAUSTED
            );
            assert_eq!(
                long_res.expect("long-deadline bounded guest must complete"),
                input
            );
        });
    }

    // ---- containment ----------------------------------------------------

    #[test]
    fn guest_unreachable_trap_is_wasm_trap_not_panic() {
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64)
                (unreachable)))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("unreachable must trap");
        assert_eq!(err.error_type, ERROR_TYPE_WASM_TRAP);
    }

    #[test]
    fn out_of_bounds_output_pointer_is_wasm_trap_not_host_read() {
        // run returns a packed (ptr, len) pointing far outside guest memory.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64)
                ;; ptr = 0x7FFF_FFFF, len = 1024 -> far out of a 1-page memory
                (i64.or (i64.shl (i64.const 0x7FFFFFFF) (i64.const 32)) (i64.const 1024))))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("out-of-bounds output must be a trap, not a host OOB read");
        assert_eq!(err.error_type, ERROR_TYPE_WASM_TRAP);
    }

    #[test]
    fn missing_run_export_is_typed_error() {
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024)))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("missing run export must be a typed error");
        assert_eq!(err.error_type, ERROR_TYPE_WASM_TRAP);
    }

    #[test]
    fn non_json_output_is_wasm_trap() {
        // Guest returns bytes that are not valid JSON.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (data (i32.const 512) "not json {{{")
              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "run") (param i32 i32) (result i64)
                (i64.or (i64.shl (i64.const 512) (i64.const 32)) (i64.const 11))))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("non-JSON output must be a wasm trap");
        assert_eq!(err.error_type, ERROR_TYPE_WASM_TRAP);
    }

    #[test]
    fn capabilities_default_is_deny_all() {
        let caps = WasmCapabilities::default();
        assert!(!caps.allow_clock);
        assert!(!caps.allow_random);
        assert!(!caps.allows_env());
    }

    #[test]
    fn limits_default_matches_constants() {
        let limits = WasmLimits::default();
        assert_eq!(limits.memory_bytes, DEFAULT_MEMORY_BYTES);
        assert_eq!(limits.fuel, DEFAULT_FUEL);
        assert_eq!(limits.max_wall_clock, DEFAULT_MAX_WALL_CLOCK);
    }

    // ---- Finding 3: start-section traps are retryable, not SandboxDenied ----

    /// A guest whose module **start section** hits `unreachable`: the trap fires
    /// during `instantiate`, before `run`. It must classify as a retryable
    /// `WasmTrap`, NOT a permanent `SandboxDenied` capability denial.
    const START_TRAP_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func $init (unreachable))
          (start $init)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
    "#;

    /// A guest whose start section spins forever: it must exhaust CPU fuel
    /// during `instantiate` and classify as a retryable `ResourceExhausted`.
    const START_FUEL_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func $init (loop $l (br $l)))
          (start $init)
          (func (export "alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
    "#;

    #[test]
    fn start_section_trap_is_retryable_wasm_trap_not_sandbox_denied() {
        let store = WasmModuleStore::new();
        let module = compile(&store, START_TRAP_WAT);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("a start-section trap must fail the attempt");
        assert_eq!(
            err.error_type, ERROR_TYPE_WASM_TRAP,
            "a start-section trap is a retryable WasmTrap, not SandboxDenied"
        );
        assert!(
            !err.non_retryable,
            "a start-section trap must remain retryable"
        );
    }

    #[test]
    fn start_section_fuel_exhaustion_is_retryable_resource_exhausted() {
        let store = WasmModuleStore::new();
        let module = compile(&store, START_FUEL_WAT);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(10_000),
            None,
        )
        .expect_err("a start-section fuel overrun must fail the attempt");
        assert_eq!(
            err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED,
            "a start-section fuel overrun is retryable ResourceExhausted, not SandboxDenied"
        );
        assert!(!err.non_retryable);
    }

    #[test]
    fn ungranted_import_is_still_sandbox_denied_regression() {
        // The Finding-3 fix must NOT weaken the genuine unsatisfied-import case:
        // an ungranted host import is still a non-retryable SandboxDenied.
        let store = WasmModuleStore::new();
        let module = compile(&store, &deny_guest_importing("fs_read"));
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect_err("ungranted import must be denied");
        assert_eq!(err.error_type, ERROR_TYPE_SANDBOX_DENIED);
        assert!(err.non_retryable);
    }

    // ---- Finding 4: cooperative cancellation of a running guest -------------

    #[test]
    fn cancellation_interrupts_a_long_running_guest_promptly() {
        // A guest with a LONG wall-clock ceiling (30s) + infinite fuel would,
        // without cooperative cancellation, hold the calling thread until the
        // ceiling. With the epoch-callback cancel signal, firing the token
        // interrupts it within ~1 tick, well before the ceiling.
        let store = WasmModuleStore::new();
        let module = compile(&store, INFINITE_LOOP_WAT);
        let cancel = CancellationToken::new();
        let cancel_for_thread = cancel.clone();
        let start = Instant::now();
        std::thread::scope(|scope| {
            // Fire the cancel shortly after the guest starts spinning.
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                cancel_for_thread.cancel();
            });
            let err = invoke_wasm_activity_cancellable(
                &store,
                &module,
                &serde_json::json!(null),
                &WasmCapabilities::default(),
                &WasmLimits {
                    memory_bytes: DEFAULT_MEMORY_BYTES,
                    fuel: u64::MAX,
                    max_wall_clock: Duration::from_secs(30),
                },
                None,
                Some(cancel),
            )
            .expect_err("a cancelled guest must be interrupted");
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_secs(5),
                "cancellation must interrupt promptly (took {elapsed:?}), not run to the ceiling"
            );
            assert_eq!(
                err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED,
                "a cancelled guest surfaces as retryable ResourceExhausted"
            );
            assert!(!err.non_retryable);
        });
    }

    #[test]
    fn an_uncancelled_token_does_not_interrupt_a_normal_guest() {
        // With a live (never-fired) token, a well-behaved guest still completes
        // normally — the cancel plumbing has no effect on the happy path.
        let store = WasmModuleStore::new();
        let module = compile(&store, ECHO_WAT);
        let cancel = CancellationToken::new();
        let input = serde_json::json!({"ok": 1});
        let out = invoke_wasm_activity_cancellable(
            &store,
            &module,
            &input,
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
            Some(cancel),
        )
        .expect("an uncancelled guest completes normally");
        assert_eq!(out, input);
    }

    // ---- Finding 7: the compiled-module cache is LRU-bounded ----------------

    /// A byte-distinct echo guest per index (unique scratch global), so each has
    /// a unique content hash to occupy its own cache slot while echoing input.
    fn unique_echo_guest(i: usize) -> String {
        format!(
            r#"
            (module
              (memory (export "memory") 1)
              (global $bump (mut i32) (i32.const 1024))
              (global $tag (mut i32) (i32.const {i}))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                (local.set $ptr (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (local.get $ptr))
              (func (export "run") (param $in_ptr i32) (param $in_len i32) (result i64)
                (i64.or
                  (i64.shl (i64.extend_i32_u (local.get $in_ptr)) (i64.const 32))
                  (i64.extend_i32_u (local.get $in_len)))))
            "#
        )
    }

    #[test]
    fn compiled_module_cache_is_bounded_and_evicts_lru() {
        const CAP: usize = 4;
        let store = WasmModuleStore::with_cache_capacity(CAP);

        // Compile CAP distinct modules; hashes[0] is the least-recently-used.
        let mut versions = Vec::new();
        for i in 0..CAP {
            let bytes = wat::parse_str(&unique_echo_guest(i)).expect("assemble");
            let hash = WasmModuleStore::compute_hash(&bytes);
            store.get_or_compile(&hash, &bytes).expect("compile");
            versions.push((hash, bytes));
        }
        assert_eq!(store.cache_len(), CAP, "cache filled to capacity");

        // Hand out an Arc for the soon-to-be-evicted (LRU) module BEFORE it is
        // evicted, to prove an in-flight invocation is unaffected by eviction.
        let (lru_hash, lru_bytes) = versions[0].clone();
        let held = store.cached(&lru_hash).expect("lru cached");
        // `cached` bumps recency, so touch the others to keep hashes[0] LRU.
        for (hash, _) in &versions[1..] {
            let _ = store.cached(hash);
        }

        // Compiling one more distinct module must evict, not grow the cache.
        let extra = wat::parse_str(&unique_echo_guest(CAP)).expect("assemble");
        let extra_hash = WasmModuleStore::compute_hash(&extra);
        store.get_or_compile(&extra_hash, &extra).expect("compile");
        assert_eq!(
            store.cache_len(),
            CAP,
            "cache stays bounded at capacity after an over-cap insert"
        );
        assert!(
            store.cached(&lru_hash).is_none(),
            "the least-recently-used entry was evicted"
        );

        // The Arc handed out before eviction is still alive and invokable.
        let input = serde_json::json!({"held": true});
        let out = invoke_wasm_activity(
            &store,
            &held,
            &input,
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("a module Arc handed out before eviction is still usable");
        assert_eq!(out, input);

        // Re-inserting the evicted hash recompiles cleanly.
        store
            .get_or_compile(&lru_hash, &lru_bytes)
            .expect("re-inserting an evicted hash recompiles");
        assert!(store.cached(&lru_hash).is_some());
    }
}
