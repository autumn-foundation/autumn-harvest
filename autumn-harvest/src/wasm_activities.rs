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
use std::sync::{Arc, OnceLock, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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

/// Maximum size, in bytes, of a guest's returned output buffer
/// (issue #965 review round 8).
///
/// The guest's `run` returns a packed `(out_ptr, out_len)`; the host reads
/// `out_len` bytes and deserializes them as JSON. `out_len` is already
/// bounds-checked against live guest memory (≤ the linear-memory ceiling), but
/// a guest returning a large *in-bounds* buffer — e.g. a 16 MiB JSON array of
/// tiny values — would otherwise make the host spend CPU (outside the guest's
/// wasmtime fuel/epoch budget) and balloon host memory parsing it into a
/// `serde_json::Value`. This caps `out_len` *before* deserialization: an
/// oversized output is rejected as a non-retryable
/// [`ActivityFailure::wasm_output_too_large`] rather than parsed. Set well
/// below the 16 MiB memory ceiling; a spike default, not a per-activity knob.
pub const WASM_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// Default CPU fuel budget for a single guest invocation.
pub const DEFAULT_FUEL: u64 = 100_000_000;

/// Maximum number of elements in any single guest table (issue #965 review).
///
/// The store limiter caps linear-memory bytes, but a `funcref`/`externref` table
/// is host-side storage *outside* that byte ceiling — an untrusted module could
/// otherwise declare or `table.grow` a huge table and consume host memory beyond
/// the 16 MiB sandbox. This bounds any single table's element count; combined
/// with `trap_on_grow_failure`, an over-limit declared or grown table traps
/// (classified as a retryable `ResourceExhausted`) rather than allocating.
pub const WASM_MAX_TABLE_ELEMENTS: usize = 100_000;

/// Maximum number of table instances a single guest may create
/// (issue #965 review).
pub const WASM_MAX_TABLES: usize = 16;

/// Maximum number of linear memories a single guest may instantiate
/// (issue #965 review round 3).
///
/// The JSON-over-linear-memory ABI uses exactly one exported `memory`. The
/// `memory_size` limit caps each memory's *bytes*, but wasmtime otherwise
/// permits many memories per store, so N individually sub-ceiling memories
/// could collectively exceed the sandbox. This caps the memory *count*; the
/// multi-memory proposal is *also* disabled at the engine (`wasm_multi_memory`)
/// so a multi-memory module fails validation outright — belt-and-braces.
pub const WASM_MAX_MEMORIES: usize = 1;

/// Maximum number of module instances a single guest invocation may create
/// (issue #965 review round 3).
///
/// One instance per invocation — the runtime instantiates exactly one module.
pub const WASM_MAX_INSTANCES: usize = 1;

/// Guest call-stack ceiling in bytes (issue #965 review round 3).
///
/// Matches wasmtime's own default (512 KiB); set explicitly so deeply-recursive
/// guest code always traps (a retryable `WasmTrap`) rather than overflowing the
/// host worker thread's stack, independent of any future wasmtime default
/// change. Host-function stack usage counts toward this bound too.
pub const WASM_MAX_STACK_BYTES: usize = 512 * 1024;

/// Default value of the per-invocation wall-clock ceiling: 5 minutes.
///
/// The ceiling itself is [`WasmLimits::max_wall_clock`], and it is *mandatory* in
/// the sense that it always applies: a caller that passes no per-call deadline —
/// or a larger one — is clamped to it, so a guest can never run unbounded. A
/// guest can spin without consuming fuel, so this bound (not fuel) is what
/// guarantees termination.
///
/// This constant is the **default** that ceiling takes. An embedder may raise or
/// lower `WasmLimits::max_wall_clock` for an activity, so this value is not
/// itself an upper bound on every invocation in the process (issue #965 review
/// round 10 — the doc previously overstated it as one).
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

/// Process-wide RNG seed source for the `env::random_u64` capability.
///
/// Advanced by a golden-ratio increment per invocation so successive stores get
/// distinct (non-cryptographic) seeds without depending on the wall clock.
///
/// Initialised **lazily from OS entropy** on first use (issue #965 review round
/// 10). A fixed start value made the whole stream reproducible across process
/// restarts — invocation *N* in any process drew the identical sequence — which
/// is a stronger and more surprising property than the documented
/// "non-cryptographic" caveat implies. Seeding from entropy keeps draws
/// unpredictable across restarts; the stream is still **not** cryptographically
/// secure (xorshift64), so guests must not use it for tokens or nonces.
static RNG_SEED_COUNTER: OnceLock<AtomicU64> = OnceLock::new();

/// Draw the next per-invocation RNG seed, initialising the counter from OS
/// entropy on first use. Always returns a nonzero seed (xorshift64 requires it).
fn next_rng_seed() -> u64 {
    let counter = RNG_SEED_COUNTER.get_or_init(|| AtomicU64::new(rand::random::<u64>()));
    counter.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed) | 1
}

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
        // CPU + wall-clock bounding (retained).
        config.consume_fuel(true);
        config.epoch_interruption(true);
        // Bound the guest call stack so deep recursion traps (retryable) rather
        // than overflowing the host worker thread. Matches wasmtime's default;
        // set explicitly so a future default change cannot silently unbound it.
        config.max_wasm_stack(WASM_MAX_STACK_BYTES);
        // Minimal-proposal core-WASM engine (issue #965 review round 3): disable
        // every post-MVP proposal the JSON-over-linear-memory `alloc`/`run` ABI
        // does not use, so an untrusted guest cannot reach a resource dimension
        // *outside* the store limiter (extra linear memories, a GC heap, shared
        // memory, fibers, ...) and the module-validation surface stays minimal.
        // Each setter below is only reached because the corresponding wasmtime
        // crate feature (`gc`/`threads`/`component-model`) is enabled by our
        // default-features dependency.
        config.wasm_multi_memory(false); // one memory per module (P1)
        config.wasm_gc(false); // GC heap lives outside the memory-size ceiling (P1)
        config.wasm_function_references(false); // typed non-null refs; GC prereq
        config.wasm_threads(false); // shared memory + atomics — no cross-invocation sharing
        config.wasm_shared_everything_threads(false);
        config.wasm_component_model(false); // core modules only, never components
        config.wasm_relaxed_simd(false); // host-nondeterministic instructions
        config.wasm_tail_call(false); // unused by the ABI / rustc-emitted guests
        config.wasm_wide_arithmetic(false);
        config.wasm_stack_switching(false); // fibers — a separate stack resource
        config.wasm_custom_page_sizes(false); // fixed 64 KiB pages
        config.wasm_extended_const(false);
        config.wasm_memory64(false); // 32-bit addressing keeps the memory bound meaningful
        config.wasm_exceptions(false); // requires GC (disabled above)
        // (`wasm_legacy_exceptions` is a deprecated internal spec-testsuite knob,
        // not a guest-reachable proposal — deliberately not toggled.)
        // Deliberately KEPT enabled: required by the ABI or rustc-emitted guests
        // and bounded by the store limits and the wall-clock ceiling, so not
        // host-resource-escape vectors — `reference_types` (funcref tables +
        // `ref.null func`), `bulk_memory` (`memory.copy`/`fill`, and a
        // `reference_types` prerequisite), `multi_value`, `simd` (`v128` is a
        // bounded value type), `backtrace` (trap diagnostics).
        //
        // Caveat on `bulk_memory` and FUEL specifically (issue #965 review round
        // 10): fuel is charged per *instruction*, not per byte, so `memory.fill`
        // / `memory.copy` / `memory.init` each cost ONE unit no matter how many
        // bytes they move. Fuel therefore does NOT bound the work a bulk-memory
        // guest performs, and must not be relied on to. What bounds it is the
        // mandatory wall-clock ceiling (armed against a real `Instant`, see
        // `invoke_wasm_activity_inner`) plus the linear-memory cap that limits any
        // single bulk operation. We deliberately do not re-price these operators
        // via `Config::operator_cost`: fuel would still not be length-proportional
        // (the cost is per instruction either way), so it would buy no real bound
        // while silently changing what `DEFAULT_FUEL` means for every guest.
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
        // SECURITY (issue #965 review, Finding 26): the longest allowlisted key
        // is the tightest cap on `key_len` — a key longer than every allowlisted
        // entry can never exact-match, so it is a guaranteed miss and we reject
        // it BEFORE reading or UTF-8-validating the guest slice. `allows_env()`
        // guarantees a non-empty allowlist here, so `max()` is `Some`; `0` is a
        // defensive floor.
        let max_allowed_key_len = allowed.iter().map(String::len).max().unwrap_or(0);
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
                    // SECURITY (issue #965 review, Finding 26): reject an
                    // over-long key BEFORE slicing/`from_utf8`-ing the guest
                    // bytes. `from_utf8` over a guest-controlled multi-MiB slice
                    // is host CPU that is NOT charged to wasmtime fuel, so a loop
                    // of `env_get` calls with a huge in-bounds `key_len` could
                    // burn host CPU to the wall-clock deadline. A key longer than
                    // any allowlisted entry can never match, so bail with the
                    // in-band miss (-1) without touching the slice. Round 1 only
                    // avoided the `vec![0u8; key_len]` allocation; this also
                    // bounds the validation scan.
                    if key_len > max_allowed_key_len {
                        return -1;
                    }
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
    invoke_wasm_activity_cancellable(store, module, input, caps, limits, deadline, None, None)
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
/// # Pre-guest budget accounting
///
/// `deadline` is the activity's raw start-to-close budget and `dispatch_start`
/// (when `Some`) is the instant that clock began — captured by the worker as it
/// records `ActivityStarted`, *before* dispatch resolution/fetch/compile ran.
/// The invoke path measures `dispatch_start.elapsed()` at the last host-only
/// moment before the guest's epoch deadline is armed — after input
/// serialization and store setup — and charges that whole pre-guest interval
/// against `deadline`. If the budget is already spent (`elapsed >= deadline`)
/// the attempt fails fast as a retryable
/// [`ActivityFailure::resource_exhausted`] **without invoking the guest**,
/// closing the window where a fast guest handed a zero→one-tick deadline could
/// race to completion past its own start-to-close (issue #965 review round 9).
/// `dispatch_start = None` charges nothing and uses `deadline` verbatim as the
/// guest budget (the convenience/test call shape).
///
/// # Errors
///
/// Returns an [`ActivityFailure`] classifying any sandbox denial, resource
/// exhaustion, guest trap, ABI violation, cooperative cancellation, or
/// contained host-glue panic.
#[allow(clippy::too_many_arguments)]
pub fn invoke_wasm_activity_cancellable(
    store: &WasmModuleStore,
    module: &Module,
    input: &serde_json::Value,
    caps: &WasmCapabilities,
    limits: &WasmLimits,
    deadline: Option<Duration>,
    dispatch_start: Option<Instant>,
    cancel: Option<&CancellationToken>,
) -> Result<serde_json::Value, ActivityFailure> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        invoke_wasm_activity_inner(
            store,
            module,
            input,
            caps,
            limits,
            deadline,
            dispatch_start,
            cancel,
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

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn invoke_wasm_activity_inner(
    store: &WasmModuleStore,
    module: &Module,
    input: &serde_json::Value,
    caps: &WasmCapabilities,
    limits: &WasmLimits,
    deadline: Option<Duration>,
    dispatch_start: Option<Instant>,
    cancel: Option<&CancellationToken>,
) -> Result<serde_json::Value, ActivityFailure> {
    let engine = store.engine();

    let input_bytes = serde_json::to_vec(input).map_err(|e| {
        ActivityFailure::wasm_trap(format!("failed to serialize activity input as JSON: {e}"))
    })?;

    // Per-attempt fresh store with an independent limiter, fuel budget, and
    // wall-clock epoch deadline.
    let seed = next_rng_seed();
    let host = HostState {
        limits: StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            // Cap every store resource dimension, not just linear-memory bytes
            // (issue #965 review round 3): a table lives in host storage outside
            // the byte ceiling, and multiple memories/instances would each be
            // individually sub-cap yet collectively exceed the sandbox.
            .memories(WASM_MAX_MEMORIES)
            .instances(WASM_MAX_INSTANCES)
            .tables(WASM_MAX_TABLES)
            .table_elements(WASM_MAX_TABLE_ELEMENTS)
            .trap_on_grow_failure(true)
            .build(),
        rng: seed,
    };
    let mut wasm_store = Store::new(engine, host);
    wasm_store.limiter(|h| &mut h.limits);
    wasm_store
        .set_fuel(limits.fuel)
        .map_err(|e| ActivityFailure::wasm_trap(format!("failed to set wasm fuel: {e}")))?;
    // Last host-only moment before the guest's deadline is armed: charge ALL
    // pre-guest overhead against the start-to-close budget in ONE measurement
    // (issue #965 review rounds 7 & 9). By now `dispatch_start.elapsed()` covers
    // resolution + cold-cache byte fetch + compile (all before this call) plus
    // input serialization and store setup (just above). Everything after this —
    // instantiate, alloc, the guest-memory write, and `run` — executes under the
    // single epoch armed just below, so it is cumulatively bounded by
    // `effective` and can never exceed the mandatory `max_wall_clock` ceiling.
    //
    // Fail fast when the whole budget is already spent before the guest can
    // start: arming a zero (→ clamped one-tick) deadline would let a fast guest
    // race to completion and be recorded successful past its own start-to-close.
    let pre_guest_elapsed = dispatch_start.map_or(Duration::ZERO, |start| start.elapsed());
    let effective = match crate::wasm_store::effective_invoke_deadline(deadline, pre_guest_elapsed)
    {
        crate::wasm_store::InvokeBudget::Exhausted => {
            return Err(ActivityFailure::resource_exhausted(
                "wasm activity start-to-close budget exhausted before guest start",
            ));
        }
        // Cap the remaining budget at the mandatory ceiling (issue #965 round 3).
        crate::wasm_store::InvokeBudget::Remaining(remaining) => {
            remaining.min(limits.max_wall_clock)
        }
        crate::wasm_store::InvokeBudget::Unbounded => limits.max_wall_clock,
    };

    // Wall-clock bound + cooperative cancellation via a per-invocation
    // epoch-deadline callback (issue #965 review). Arm the first deadline one
    // tick out; on each callback poll the cancellation token and compare the
    // *real clock* against an absolute deadline, trapping (as an epoch interrupt
    // → retryable `ResourceExhausted`) when either the token fires or the
    // `effective` ceiling is reached. This is per-`Store`, so N concurrent
    // invocations keep independent deadlines off the single shared ticker, and a
    // cancelled guest is interrupted within ~1 tick instead of running to the
    // ceiling. A guest that completes before the first tick never invokes the
    // callback, so the fast path pays nothing.
    //
    // The bound is an absolute `Instant`, NOT a countdown of callback
    // invocations (issue #965 review round 10). wasmtime emits epoch checks only
    // at function entry, loop headers, and host libcalls, so a guest controls how
    // much work sits between two consecutive callbacks. Counting invocations
    // therefore measures "check points crossed", not time: a guest that does more
    // work per check point than one tick's worth stretches the ceiling by that
    // ratio. Measured overrun with a `memory.fill`-heavy guest was ~1.2-2.3x the
    // configured ceiling — bounded (each bulk fill is itself a check point, and a
    // single fill is capped by the 16 MiB memory ceiling), but real. Reading the
    // clock makes the ceiling mean what its documentation says, and caps overrun
    // at whatever single uninterruptible operation is in flight.
    wasm_store.set_epoch_deadline(1);
    let hard_deadline = Instant::now().checked_add(effective);
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
        // `checked_add` overflowed only for an absurd `effective`; treat that as
        // "no representable deadline" and let fuel/cancellation bound the guest
        // rather than trapping immediately.
        if hard_deadline.is_some_and(|d| Instant::now() >= d) {
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
        // A permanent capability denial outranks a concurrent cancellation
        // (issue #965 review round 10). An ungranted import is a deterministic
        // misconfiguration: reporting it as a retryable "cancelled" just because
        // the token happened to fire during `instantiate` would spend the whole
        // retry budget rediscovering the same denial. Classify first and keep a
        // `SandboxDenied` verdict; otherwise fall back to the run-state reasons.
        let classified = classify_wasmtime_err_phase(e, phase);
        if classified.error_type == crate::failure::ERROR_TYPE_SANDBOX_DENIED {
            return classified;
        }
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            ActivityFailure::resource_exhausted("wasm activity cancelled before completion")
        } else if deadline_hit.load(Ordering::Relaxed) {
            ActivityFailure::resource_exhausted("wall-clock deadline exceeded")
        } else {
            classified
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

    // Cap the output size BEFORE bounds-checking or parsing (issue #965 review
    // round 8): `out_len` is guest-controlled and, while bounded by live guest
    // memory (≤ the 16 MiB ceiling), a large in-bounds buffer would otherwise
    // make the host spend CPU (outside the guest's fuel/epoch budget) and
    // balloon host memory in `serde_json::from_slice`. An oversized output is a
    // deterministic guest bug, so reject it non-retryably rather than parsing.
    if out_len > WASM_MAX_OUTPUT_BYTES {
        return Err(ActivityFailure::wasm_output_too_large(format!(
            "wasm activity output ({out_len} bytes) exceeds the {WASM_MAX_OUTPUT_BYTES}-byte limit"
        )));
    }

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
        ERROR_TYPE_RESOURCE_EXHAUSTED, ERROR_TYPE_SANDBOX_DENIED, ERROR_TYPE_WASM_OUTPUT_TOO_LARGE,
        ERROR_TYPE_WASM_TRAP,
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
    fn granted_env_links_the_host_fn_and_does_not_deny_instantiation() {
        // Issue #965 review: this test's job is narrow and worth naming exactly.
        // It proves ONLY that a non-empty allowlist LINKS `env::env_get`, so the
        // guest instantiates instead of being denied. It deliberately does NOT
        // prove the grant returns data — the probed key is not on the allowlist,
        // so the in-band check denies it and the guest reports `true`, which is
        // the same value `env_get_denies_non_allowlisted_key_in_band` asserts.
        // The genuine positive path (an allowlisted key returning its value) is
        // covered by `env_get_allowlisted_key_returns_value_length`.
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
        .expect("a granted env guest must instantiate and run, not be denied");
        // The load-bearing assertion is the `expect` above (no SandboxDenied).
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

    /// SECURITY regression (issue #965 review, Finding 26): a guest that calls
    /// the granted `env_get` with a huge but fully **in-bounds** `key_len`
    /// (~16 MiB, pointing at valid guest memory) must be rejected by the
    /// longest-allowlisted-key cap BEFORE the host reads/`from_utf8`-validates
    /// the slice. This is the case the round-1 no-alloc fix left open: the
    /// `i32::MAX` sibling test is *out of range* (the slice read yields `None`),
    /// so it never reached `from_utf8` even pre-fix; here the slice IS in range,
    /// so pre-cap the host would `from_utf8`-scan the whole ~16 MiB (host CPU
    /// uncharged to fuel — a `DoS` in a tight loop). Post-fix the length cap makes
    /// it an in-band miss (-1) with no scan, no OOM, and no panic.
    const ENV_HUGE_INBOUNDS_KEYLEN_WAT: &str = r#"
        (module
          (import "env" "env_get" (func $e (param i32 i32 i32 i32) (result i32)))
          ;; 255 pages = 16,711,680 bytes, within the 16 MiB memory limit.
          (memory (export "memory") 255)
          (data (i32.const 200) "true")
          (data (i32.const 300) "false")
          (func (export "alloc") (param i32) (result i32) (i32.const 8192))
          (func (export "run") (param i32 i32) (result i64)
            (local $r i32)
            ;; key_ptr = 0, key_len = 16,000,000: a huge, positive, fully
            ;; IN-BOUNDS length (0 + 16,000,000 < 16,711,680).
            (local.set $r
              (call $e (i32.const 0) (i32.const 16000000) (i32.const 1000000) (i32.const 64)))
            (if (result i64) (i32.eq (local.get $r) (i32.const -1))
              (then (i64.or (i64.shl (i64.const 200) (i64.const 32)) (i64.const 4)))
              (else (i64.or (i64.shl (i64.const 300) (i64.const 32)) (i64.const 5))))))
    "#;

    #[test]
    fn env_get_with_huge_inbounds_key_len_is_capped_before_validation() {
        let store = WasmModuleStore::new();
        let module = compile(&store, ENV_HUGE_INBOUNDS_KEYLEN_WAT);
        // "K" IS on the allowlist (longest allowlisted key = 1 byte), so env_get
        // is linked. The 16 MB key_len far exceeds that cap, so the host bails
        // with -1 BEFORE reading/from_utf8-ing the 16 MB in-bounds slice.
        let caps = WasmCapabilities {
            allow_env: vec!["K".to_string()],
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
        .expect("guest must run promptly without a CPU-burning scan, OOM, or panic");
        assert_eq!(
            out,
            serde_json::json!(true),
            "a key longer than any allowlisted entry is an in-band miss (-1), \
             rejected before the guest slice is ever scanned"
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

    /// Build an infinite loop whose *body* is expensive but crosses only ONE
    /// epoch check point per iteration (issue #965 review: wall-clock ceiling).
    ///
    /// wasmtime emits epoch checks only at function entry and loop headers, so a
    /// guest is charged one callback per back-edge no matter how long the body
    /// takes. `memory.fill` is the amplifier: it costs a single fuel unit
    /// regardless of length, so `fills` bulk fills of ~16 MiB each buy tens of
    /// milliseconds of real work per check point for a handful of fuel.
    ///
    /// A ceiling that counts callback *invocations* lets this guest run for
    /// `ticks x body_duration`; a ceiling anchored to a real instant bounds it at
    /// the configured duration plus at most one body.
    fn expensive_body_loop_wat(fills: usize) -> String {
        let body =
            "(memory.fill (i32.const 0) (i32.const 65) (i32.const 16000000))\n".repeat(fills);
        format!(
            r#"
        (module
          (memory (export "memory") 256)
          (func (export "alloc") (param i32) (result i32) (i32.const 0))
          (func (export "run") (param i32 i32) (result i64)
            (loop $l
              {body}
              (br $l))
            (i64.const 0)))
    "#
        )
    }

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
    fn huge_table_declaration_is_bounded_resource_exhausted() {
        // A module that DECLARES a funcref table far larger than
        // WASM_MAX_TABLE_ELEMENTS must be bounded at instantiation (the store's
        // table limiter denies it) — a table is host storage OUTSIDE the linear-
        // memory byte ceiling, so an unbounded one would let a guest consume host
        // memory past its 16 MiB sandbox. It fails as a retryable
        // ResourceExhausted (composing with the Finding-3 instantiate
        // reclassification — a table-limit failure is NOT a SandboxDenied), with
        // no host OOM or panic.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (table 200000 funcref)
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
        .expect_err("an over-cap table declaration must be bounded");
        assert_eq!(
            err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED,
            "an over-cap declared table is retryable ResourceExhausted, not SandboxDenied"
        );
        assert!(!err.non_retryable);
    }

    #[test]
    fn table_grow_past_cap_is_bounded_resource_exhausted() {
        // A module that `table.grow`s a funcref table past WASM_MAX_TABLE_ELEMENTS
        // must trap (trap_on_grow_failure) rather than silently returning -1 and
        // allocating host memory. Mirrors `memory_limit_is_resource_exhausted` for
        // tables.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (table 1 funcref)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64)
                (drop (table.grow 0 (ref.null func) (i32.const 200000)))
                (i64.const 0)))
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
        .expect_err("an over-cap table.grow must be bounded");
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        assert!(!err.non_retryable);
    }

    #[test]
    fn multi_memory_module_is_rejected_at_validation() {
        // The JSON-over-linear-memory ABI uses exactly one memory. A module
        // declaring TWO linear memories must be rejected — `memory_size` caps
        // each memory's bytes individually, so N sub-cap memories would
        // collectively exceed the sandbox. The multi-memory proposal is disabled
        // at the engine, so this fails at COMPILE/validation (a typed
        // `HarvestError::Config`), not a host OOM or panic; the store's
        // `.memories(1)` cap is the belt-and-braces instantiation backstop.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (memory 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat assembles a 2-memory module");
        let hash = WasmModuleStore::compute_hash(&bytes);
        let err = store
            .get_or_compile(&hash, &bytes)
            .expect_err("a multi-memory module must be rejected at validation");
        assert!(
            matches!(err, HarvestError::Config(_)),
            "multi-memory rejection is a typed config error, got: {err:?}"
        );
    }

    #[test]
    fn gc_module_is_rejected_at_validation() {
        // The ABI does not use Wasm GC. GC allocations go into a separate GC heap
        // NOT covered by the `memory_size` ceiling, so a tiny GC-using module
        // could allocate beyond the sandbox. GC is disabled at the engine, so a
        // module declaring a GC (`struct`) type is rejected at validation (a
        // typed `HarvestError::Config`) with no unbounded GC-heap allocation.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (type $pair (struct (field i32) (field i32)))
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64) (i64.const 0)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat assembles a GC-typed module");
        let hash = WasmModuleStore::compute_hash(&bytes);
        let err = store
            .get_or_compile(&hash, &bytes)
            .expect_err("a GC-using module must be rejected at validation");
        assert!(
            matches!(err, HarvestError::Config(_)),
            "GC rejection is a typed config error, got: {err:?}"
        );
    }

    #[test]
    fn deep_recursion_is_bounded_trap_not_host_stack_overflow() {
        // Unbounded recursion must trap on the bounded guest stack
        // (`max_wasm_stack`), a retryable `WasmTrap`, rather than overflowing the
        // host worker thread. Fuel is set effectively unlimited so the STACK
        // bound — not fuel — is what stops the guest, exercising the stack
        // ceiling specifically.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func $rec (param $n i64) (result i64)
                (local $a i64) (local $b i64) (local $c i64) (local $d i64)
                (local $e i64) (local $f i64) (local $g i64) (local $h i64)
                (call $rec (local.get $n)))
              (func (export "run") (param i32 i32) (result i64)
                (call $rec (i64.const 0))))
        "#;
        let module = compile(&store, wat);
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(u64::MAX),
            None,
        )
        .expect_err("deep recursion must trap on the bounded stack");
        assert_eq!(
            err.error_type, ERROR_TYPE_WASM_TRAP,
            "a stack overflow on the bounded guest stack is a retryable WasmTrap"
        );
        assert!(!err.non_retryable, "a stack-overflow trap is retryable");
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

    #[test]
    fn wall_clock_ceiling_bounds_elapsed_time_not_callback_count() {
        // Issue #965 review (P2): the ceiling must be anchored to a real instant.
        //
        // wasmtime emits epoch checks only at function entry and loop headers, so
        // counting callback *invocations* charges one tick per back-edge however
        // long the body took. With a 100 ms ceiling that is 100 ticks; a body of
        // tens of milliseconds then runs for SECONDS - the documented "mandatory
        // hard ceiling" silently multiplied by the body duration.
        //
        // A guest that spins with a cheap body (INFINITE_LOOP_WAT) hides this,
        // because there one tick ~= one trivial iteration. This guest makes the
        // two definitions diverge by more than an order of magnitude.
        let store = WasmModuleStore::new();
        // ~256 fills x 16 MiB = ~4 GiB of memset per back-edge: tens of ms of
        // real work for ~256 units of fuel.
        // ~1024 fills x 16 MiB per back-edge. Each bulk fill is itself an epoch
        // check point, so this maximises work-per-check-point: measured ~2.3x
        // overrun before the fix, ~1.03x after.
        let module = compile(&store, &expensive_body_loop_wat(1024));
        let ceiling = Duration::from_millis(100);
        let start = Instant::now();
        let err = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &WasmLimits {
                memory_bytes: DEFAULT_MEMORY_BYTES,
                // Effectively unbounded fuel: `memory.fill` costs one unit per
                // instruction regardless of length, so fuel cannot bound this.
                fuel: u64::MAX,
                max_wall_clock: ceiling,
            },
            None,
        )
        .expect_err("the mandatory wall-clock ceiling must terminate the guest");
        let elapsed = start.elapsed();
        assert_eq!(err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED);
        // Anchored to a real instant, elapsed tracks the ceiling regardless of how
        // much work the guest packs between check points OR how fast the machine
        // is - measured 103 ms for this 100 ms ceiling. Counting callback
        // invocations instead measured 229 ms, and gets WORSE on a slower machine.
        // 2x the ceiling therefore separates the two with real margin on both
        // sides rather than being a timing coin-flip.
        assert!(
            elapsed < ceiling * 2,
            "wall-clock ceiling of {ceiling:?} must bound ELAPSED time, but the guest ran for \
             {elapsed:?}; the ceiling is counting epoch-callback invocations rather than time"
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
    fn oversized_guest_output_is_rejected_before_parse() {
        // The guest grows memory so a 5 MiB output region is genuinely
        // in-bounds, then returns packed(out_ptr=0, out_len=5*1024*1024) — over
        // the 4 MiB WASM_MAX_OUTPUT_BYTES cap. Proving the cap fires here (an
        // in-bounds buffer that WOULD otherwise be sliced and parsed) shows the
        // size check runs BEFORE the bounds check and `serde_json::from_slice`,
        // so an oversized output can never balloon host parse CPU/memory.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 1024))
              (func (export "run") (param i32 i32) (result i64)
                ;; grow to 81 pages (~5.3 MiB) so 0..5_242_880 is in-bounds
                (drop (memory.grow (i32.const 80)))
                ;; packed(out_ptr=0, out_len=5_242_880) = 5 MiB, over the 4 MiB cap
                (i64.const 5242880)))
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
        .expect_err("an over-cap output must be rejected, not parsed");
        assert_eq!(err.error_type, ERROR_TYPE_WASM_OUTPUT_TOO_LARGE);
        assert!(
            err.non_retryable,
            "an oversized output is a deterministic guest bug — non-retryable"
        );
        assert!(
            err.message.contains("exceeds"),
            "message should name the limit, was: {}",
            err.message
        );
    }

    #[test]
    fn small_output_at_cap_still_round_trips() {
        // Regression: an output well under WASM_MAX_OUTPUT_BYTES parses normally.
        // The guest writes the 4-byte JSON literal `true` and returns its length.
        let store = WasmModuleStore::new();
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (data (i32.const 512) "true")
              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "run") (param i32 i32) (result i64)
                (i64.or (i64.shl (i64.const 512) (i64.const 32)) (i64.const 4))))
        "#;
        let module = compile(&store, wat);
        let out = invoke_wasm_activity(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            None,
        )
        .expect("an under-cap output must parse normally");
        assert_eq!(out, serde_json::json!(true));
    }

    #[test]
    fn rng_seed_stream_is_not_a_fixed_process_constant() {
        // Issue #965 review round 10: the seed counter used to start from a fixed
        // constant, so invocation N in ANY process drew an identical stream. It is
        // now initialised from OS entropy. We cannot observe a "restart" in-process,
        // so assert the property that made the old behaviour reproducible: the very
        // first seed is not the hard-coded golden-ratio start value, and successive
        // seeds advance (and stay nonzero, which xorshift64 requires).
        const OLD_FIXED_START: u64 = 0x9E37_79B9_7F4A_7C15;
        let first = next_rng_seed();
        let second = next_rng_seed();
        assert_ne!(
            first,
            OLD_FIXED_START | 1,
            "the first seed must come from entropy, not a compile-time constant"
        );
        assert_ne!(
            first, second,
            "successive invocations must get distinct seeds"
        );
        assert_ne!(first, 0, "xorshift64 requires a nonzero seed");
        assert_ne!(second, 0, "xorshift64 requires a nonzero seed");
    }

    #[test]
    fn sandbox_denial_outranks_a_concurrent_cancellation() {
        // Issue #965 review round 10: an ungranted import is a permanent
        // misconfiguration. If the cancellation token fires while `instantiate`
        // is failing on that import, the failure must still be reported as the
        // NON-retryable `SandboxDenied` — reporting a retryable "cancelled"
        // would spend the whole retry budget rediscovering the same denial.
        let store = WasmModuleStore::new();
        let module = compile(&store, &deny_guest_importing("now_millis"));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = invoke_wasm_activity_cancellable(
            &store,
            &module,
            &serde_json::json!(null),
            &WasmCapabilities::default(),
            &WasmLimits::default(),
            None,
            None,
            Some(&cancel),
        )
        .expect_err("an ungranted import must fail");
        assert_eq!(
            err.error_type, ERROR_TYPE_SANDBOX_DENIED,
            "a cancelled attempt must still report the permanent capability denial, got: {err:?}"
        );
        assert!(
            err.non_retryable,
            "SandboxDenied must stay non-retryable even under cancellation"
        );
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
                None,
                Some(&cancel),
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
            None,
            Some(&cancel),
        )
        .expect("an uncancelled guest completes normally");
        assert_eq!(out, input);
    }

    #[test]
    fn dispatch_start_overhead_fails_fast_before_the_guest() {
        // Finding 21 (issue #965 review round 9): the invoke path charges ALL
        // pre-guest overhead — resolution + fetch + compile + input
        // serialization + store setup — against the start-to-close deadline,
        // measured at the last host-only moment before the guest's epoch is
        // armed. A `dispatch_start` already past the deadline must fail fast as a
        // retryable ResourceExhausted WITHOUT invoking the guest, proven with an
        // ECHO module that would otherwise return Ok(input) within a single tick.
        let store = WasmModuleStore::new();
        let module = compile(&store, ECHO_WAT);
        let input = serde_json::json!({ "sentinel": "must-not-echo" });
        let err = invoke_wasm_activity_cancellable(
            &store,
            &module,
            &input,
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            Some(Duration::from_millis(50)), // start-to-close budget
            // Overhead (whatever the source: resolve/fetch/compile/serialize)
            // already spent the whole budget before the guest could start.
            Some(Instant::now().checked_sub(Duration::from_secs(5)).unwrap()),
            None,
        )
        .expect_err("an over-budget invoke must fail fast, not echo the input");
        assert_eq!(
            err.error_type, ERROR_TYPE_RESOURCE_EXHAUSTED,
            "an exhausted start-to-close budget is retryable ResourceExhausted"
        );
        assert!(
            err.message.contains("budget exhausted before guest start"),
            "expected the fail-fast message, got: {}",
            err.message
        );
    }

    #[test]
    fn dispatch_start_within_budget_runs_the_guest() {
        // The complement: negligible pre-guest overhead leaves the guest
        // essentially its full budget, so the ECHO module runs and returns
        // Ok(input) (round-7 in-budget behaviour preserved through the move).
        let store = WasmModuleStore::new();
        let module = compile(&store, ECHO_WAT);
        let input = serde_json::json!({ "echo": "me" });
        let out = invoke_wasm_activity_cancellable(
            &store,
            &module,
            &input,
            &WasmCapabilities::default(),
            &fast_limits(DEFAULT_FUEL),
            Some(Duration::from_secs(10)),
            Some(Instant::now()),
            None,
        )
        .expect("an in-budget invoke runs the guest and echoes the input");
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
            let bytes = wat::parse_str(unique_echo_guest(i)).expect("assemble");
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
        let extra = wat::parse_str(unique_echo_guest(CAP)).expect("assemble");
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
