# WASM-sandboxed polyglot activities — R&D spike recommendation (issue #965)

**Status: R&D spike, behind the `wasm-activities` Cargo feature.** This is not a
committed GA feature. This document is the written R&D exit-criteria recommendation
required by issue #965's AC8, backed by the working spike. It covers runtime
choice, ABI, guest interface, heartbeat/cancellation delivery, cold-start cost,
security posture, storage/distribution, a GA recommendation, and open questions.

All measurements and behaviours below are from the spike as shipped:
`src/wasm_activities.rs` (runtime), `src/wasm_store.rs` (storage + dispatch seam),
the builder/worker seam, and the tests enumerated at the end.

---

## 1. Runtime choice — wasmtime vs wasmer

**Recommendation: wasmtime (adopted).** The spike embeds **wasmtime 46**.

Why wasmtime:

- **Governance & longevity.** wasmtime is the Bytecode Alliance reference runtime,
  the same engine behind Fermyon Spin and wasmCloud; it has the broadest
  production deployment and the most active security review of the two.
- **First-class deterministic resource metering.** `Config::consume_fuel(true)`
  gives per-instruction CPU fuel accounting, and `Config::epoch_interruption(true)`
  gives a cheap monotonic-counter wall-clock interrupt. The spike uses **both**
  together (`Store::set_fuel` + `Store::set_epoch_deadline`) because neither alone
  is sufficient: a guest can spin without consuming fuel (a tight `loop`/host-call
  pattern), so a wall-clock bound is mandatory for a hard termination guarantee.
- **`StoreLimits` for memory bounding.** `StoreLimitsBuilder::memory_size(...).trap_on_grow_failure(true)`
  caps linear-memory growth and turns an over-ceiling `memory.grow` into a trap the
  host classifies, rather than an OOM.
- **Cranelift** compiles guests to native code, so a cached module runs at native
  speed (see §5).

wasmer tradeoffs considered and rejected for this spike: comparable raw
performance and a similar embedding API, but fuel metering and epoch-style
interruption are less central to its design, its component-model story trails
wasmtime's, and the Bytecode Alliance's security-response cadence was the
deciding factor for a sandboxing-first feature.

---

## 2. Core-WASM vs component-model ABI

**Spike: core-WASM + JSON over linear memory. GA recommendation: migrate to the
component model + WIT.**

The spike deliberately uses the **core-WASM** module model with a hand-rolled
JSON-over-linear-memory ABI rather than the WebAssembly Component Model. A guest
exports exactly three things:

- `memory` — its linear memory.
- `alloc(len: i32) -> i32` — return a pointer to `len` writable guest bytes (a
  bump allocator suffices). The host calls this once to place the serialized JSON
  input.
- `run(in_ptr: i32, in_len: i32) -> i64` — execute the activity. The input JSON
  lives at `in_ptr..in_ptr+in_len`; the return packs the output location as
  `((out_ptr as u64) << 32) | (out_len as u64)`. The host reinterprets the returned
  `i64` as `u64` **before** shifting so a high pointer bit never sign-extends, and
  every host-side read/write is bounds-checked against live guest memory — an
  out-of-range `(ptr, len)` becomes a typed `WasmTrap`, never a host OOB read.

Why core-WASM for the spike:

- **Simplicity and language reach.** Every language that targets `wasm32-unknown`
  can export three functions and a linear memory. No WIT toolchain, no
  `wasm-tools component new`, no per-language bindings generator is required to
  write a guest — the AssemblyScript demo in `examples/wasm-guests/` is a handful
  of lines, and the WAT demo is hand-written.
- **Zero ABI ceremony to prove the shape.** The point of an R&D spike is to prove
  the dispatch/sandbox/metering/storage shape end-to-end; a typed IDL is
  orthogonal to that and would have front-loaded toolchain cost.

Why the component model for GA:

- **Typed, versioned interfaces.** A WIT world (`activity: func(input) -> result`)
  gives the guest/host contract a real type system and a versioning story, instead
  of the current "both sides must agree JSON-over-`i64`-packing" convention.
- **Canonical ABI** handles string/list/record lifting/lowering, removing the
  hand-rolled pointer packing and the per-guest bump allocator.
- **Host imports as typed interfaces** (see §4) — heartbeat/cancel become WIT
  imports rather than ad-hoc `env::` functions.

Migration note: the JSON-over-buffer ABI and the component ABI can coexist behind
the same `WasmBinding` — the runtime already isolates ABI details inside
`invoke_wasm_activity`, so a component-model path is an additive second invoke
strategy, not a rewrite of the storage/dispatch/metering layers.

---

## 3. Guest interface shape — JSON-over-buffer vs WIT-typed

**Spike: JSON-over-buffer. GA recommendation: WIT-typed records, with JSON as a
fallback for dynamically-shaped payloads.**

The spike passes the activity input as `serde_json::to_vec(input)` and expects
`serde_json::from_slice`-able output. This mirrors the native activity contract
exactly (both native and WASM activities are JSON-in/JSON-out over the standard
dispatch path), which is precisely why a WASM activity is **indistinguishable from
a native one in history** (see §7): the worker records the same
`ActivityScheduled { input }` / `ActivityCompleted { output }` events for both.

Tradeoffs:

- **JSON-over-buffer (spike):** language-agnostic, matches Harvest's existing
  JSON activity payloads for free, trivial for a guest author, but untyped (a
  malformed output surfaces as a `WasmTrap` "output is not valid JSON" only at
  runtime) and pays serialize/parse cost on both sides.
- **WIT-typed (GA):** compile-time-checked guest signatures, no JSON parse in the
  hot path for structured payloads, versionable — at the cost of a per-language
  bindings toolchain the spike deliberately avoided.

Recommendation: default to WIT-typed records at GA for the ergonomic and
correctness win, but keep a JSON/`any`-shaped escape hatch so a guest handling
free-form payloads (the common webhook/ETL case) needn't define a schema.

---

## 4. Heartbeat / cancellation delivery into the guest — **cancellation lands; heartbeat remains the gap**

The spike delivers **timeouts** and **cancellation** into the guest, but **not**
cooperative heartbeat.

What the spike does:

- **`start_to_close` → epoch wall-clock deadline.** The worker passes the
  activity's effective deadline; `invoke_wasm_activity` clamps it by the mandatory
  `limits.max_wall_clock` ceiling. A guest that overruns is interrupted and the
  attempt fails as a retryable `ResourceExhausted("wall-clock deadline exceeded")`.
- **Fuel → CPU bound.** A runaway-CPU guest exhausts fuel and fails as a retryable
  `ResourceExhausted("cpu fuel exhausted")`.
- **Cooperative cancellation via a per-invocation epoch-deadline callback.** The
  worker threads the task's `CancellationToken` into the invocation. Rather than
  arming a one-shot deadline, the store installs an `epoch_deadline_callback` that
  fires roughly every `EPOCH_TICK_INTERVAL` (1 ms) while the guest runs: each tick
  it polls the token and decrements the remaining wall-clock budget, trapping the
  guest at the next safe point when **either** the token fires **or** the ceiling
  is reached. So a cancelled WASM guest is interrupted within ~1 tick and returns
  a retryable `ResourceExhausted("wasm activity cancelled before completion")` —
  it no longer holds a blocking-pool thread until its wall-clock ceiling. The
  callback is per-`Store`, so one invocation's cancellation never affects
  another's, and the ceiling remains the hard backstop for a guest that could
  ignore a cooperative signal (there is no way for core-WASM guest code to "ignore"
  an epoch trap, so the interrupt is unconditional). Exercised by
  `cancellation_interrupts_a_long_running_guest_promptly`.
- **Per-invocation deadline isolation via a single global epoch ticker.** One
  named background thread (`harvest-wasm-epoch`) advances the engine's epoch every
  `EPOCH_TICK_INTERVAL` for the store's whole lifetime. Each store drives its own
  countdown from its own callback, so N concurrent invocations each get an
  **independent** wall-clock deadline (and independent cancellation) off the
  **same** monotonic clock — one guest's expiry never trips another's. Exercised by
  `concurrent_deadlines_are_independent`.

What it does **not** do — and the honest limitation:

- There is **no** `harvest_heartbeat` host import, so a guest cannot report
  progress mid-run, and there is no *cooperative* cancel-check import the guest can
  poll to unwind its own resources gracefully before the epoch trap fires. The
  guest still runs on the calling thread (the worker invokes
  `PreparedWasmActivity::invoke` inside `spawn_blocking`); it is now interrupted
  promptly on cancellation, but it is interrupted by a *trap*, not by returning
  from `run` — so a guest that needs to run cleanup (flush, release an external
  handle) on cancellation cannot yet do so.

What a full implementation still needs:

- A `harvest_heartbeat(ptr, len)` host import the guest calls periodically,
  wired to the existing activity heartbeat channel, so progress and
  liveness flow through the same path as native activities.
- A cooperative cancel-check host import (or a host-set flag the guest polls) so a
  well-behaved guest can observe a cancel request and return early with cleanup —
  the epoch-trap interrupt stays as the hard backstop for a guest that ignores it.

The component model (§2) is the right vehicle for both, as typed WIT imports.

---

## 5. Cold-start / instantiation cost

**Measured overhead is well under the issue's < 10 ms p99 target.** The spike's
`#[ignore]`d microbenchmark (`dispatch_overhead_wasm_echo_vs_native`) times 500
invocations of a trivial **cached** echo guest through `invoke_wasm_activity`
(module compiled/cached **once** up front, so compilation is excluded — the loop
measures per-invocation instantiate + call) against a native Rust closure baseline:

| path | p50 | p99 |
|------|-----|-----|
| WASM cached echo | ~243 µs | ~427 µs |
| native closure | ~3.2 µs | ~10.6 µs |
| **overhead (WASM − native)** | **~240 µs** | **~416 µs** |

Success-metric target: **p99 overhead < 10 ms → MET** (≈ 0.42 ms, ~24× headroom).
These numbers are from a **debug** (`--unoptimized`) build; a release worker would
be faster still. They are a microbenchmark and vary run-to-run, but the order of
magnitude (sub-millisecond per-invocation instantiate) is stable.

Two distinct costs to keep separate:

- **Per-invocation instantiation** (what the ~0.24 ms measures): a fresh `Store`,
  linker, and instance per attempt. This is the dominant WASM-over-native cost and
  is what the number above quantifies.
- **First-compile cold start** (excluded above): the *first* time a module hash is
  seen, wasmtime Cranelift-compiles it (tens of ms for a real guest). The spike
  amortizes this with the content-hash compiled-module cache
  (`WasmModuleStore::get_or_compile` compiles at most once per hash, process-wide),
  so only the very first attempt for a given version pays it.

GA mitigation for the per-invocation cost: **instance pooling** —
`wasmtime::InstancePre` (pre-linked instantiation) plus the pooling instance
allocator would cut the per-attempt instantiate substantially and is the standard
production pattern (Shopify Functions / Fermyon Spin both pool). Not needed to
meet the spike's bar, recommended for GA.

---

## 6. Security posture

**Deny-by-default capability model, proven by the sandbox test suite.**

- **Empty linker = deny-all.** `invoke_wasm_activity` starts with a fresh
  `Linker::new(engine)` and links a host function **only** for a capability the
  caller explicitly granted via `WasmCapabilities`. A guest that imports an
  ungranted host function fails at **instantiation** with a **non-retryable**
  `SandboxDenied` — before `run` ever executes.
- **Filesystem and network are not grantable.** There is no host function backing
  them in this spike, so any guest importing (say) `env::fs_read` or
  `env::net_connect` is unsatisfied and denied. Evidence:
  `ungranted_fs_import_is_sandbox_denied`, `ungranted_net_import_is_sandbox_denied`.
- **Environment is allowlisted, not ambient.** `env::env_get` is linked only when
  the per-activity allowlist is non-empty, and it returns `-1` in-band for any key
  not on the exact-match allowlist **without touching the process environment**.
  Evidence: `ungranted_env_get_import_is_sandbox_denied`,
  `env_get_denies_non_allowlisted_key_in_band`.
- **Content-hash integrity.** `get_or_compile` verifies the claimed SHA-256 against
  the bytes **before** compilation, so corruption or a mismatched lookup is
  rejected rather than silently compiling the wrong code
  (`get_or_compile_rejects_hash_mismatch`).
- **Randomness is explicitly non-cryptographic.** The granted `env::random_u64` is
  a per-invocation xorshift64 draw, documented as unsuitable for security-sensitive
  use.
- **Minimal-proposal core-WASM engine + fully-bounded store.** The `alloc`/`run`
  JSON-over-linear-memory ABI needs only MVP core WASM plus funcref tables, so the
  engine `Config` **disables every post-MVP proposal it does not use** — the
  narrower the enabled feature set, the smaller both the module-validation attack
  surface and the set of resource dimensions a guest can reach. Disabled:
  `wasm_multi_memory`, `wasm_gc`, `wasm_function_references`, `wasm_threads`,
  `wasm_shared_everything_threads`, `wasm_component_model`, `wasm_relaxed_simd`,
  `wasm_tail_call`, `wasm_wide_arithmetic`, `wasm_stack_switching`,
  `wasm_custom_page_sizes`, `wasm_extended_const`, `wasm_memory64`,
  `wasm_exceptions`. Kept enabled because the ABI or
  rustc-emitted guests require them and each is already bounded by fuel/memory/table
  limits (not a host-resource-escape vector): `reference_types` (funcref tables +
  `ref.null func`), `bulk_memory` (`memory.copy`/`fill`, and a `reference_types`
  prerequisite), `multi_value`, `simd` (`v128` is a bounded value type),
  `backtrace`. This closes two escapes that a single `memory_size` cap misses: a
  multi-memory module (N individually sub-cap linear memories) and a GC-using module
  (allocations live in a separate GC heap **outside** the linear-memory ceiling) —
  both now fail **validation** outright. Complementing the engine config, the
  per-invocation `StoreLimits` caps **every** store resource dimension, not just
  linear-memory bytes: `memory_size` (16 MiB default), `memories(1)` and
  `instances(1)` (the ABI instantiates exactly one memory and one module),
  `tables(16)` + `table_elements(100_000)` (a table is host storage outside the byte
  ceiling), and `trap_on_grow_failure(true)` (an over-cap `memory.grow`/`table.grow`
  traps as a retryable `ResourceExhausted` rather than allocating). The guest call
  stack is bounded by `max_wasm_stack` (512 KiB) so deep recursion traps
  (`WasmTrap`) rather than overflowing the host worker thread. Evidence:
  `multi_memory_module_is_rejected_at_validation`, `gc_module_is_rejected_at_validation`,
  `deep_recursion_is_bounded_trap_not_host_stack_overflow`,
  `huge_table_declaration_is_bounded_resource_exhausted`,
  `table_grow_past_cap_is_bounded_resource_exhausted`,
  `memory_limit_is_resource_exhausted`.

Follow-up (out of scope per the issue): cryptographic module **signing/provenance**
(the spike provides content-hash **integrity** only — it proves bytes weren't
corrupted, not who authored them). A signature/attestation layer over the module
table is the natural next step.

---

## 7. Storage & distribution — Postgres content-hash table

**No new infrastructure: modules live as content-addressed rows in the same
Postgres that holds every other durable artifact (the Postgres-only core-storage
invariant).**

- **Table `harvest_wasm_modules`** (migration `20260711000000_harvest_wasm_modules`,
  the single additive migration this feature introduces) with a **composite primary
  key `(hash, activity_name)`** — identical bytes can bind to two different activity
  names independently (`identical_bytes_bound_to_two_names_resolve_independently`).
- **Single-active invariant** via a partial unique index (`WHERE active`): at most
  one active version per activity name, so a hot-swap can never leave two active
  versions racing.
- **Per-name advisory-lock publish.** `publish_wasm_module` serialises concurrent
  publishes for the same name with a transaction-scoped
  `pg_advisory_xact_lock(hashtext(name))`, so two workers publishing different
  versions converge on exactly one active row
  (`concurrent_publishes_leave_exactly_one_active_row`). The publish is one
  transaction: deactivate every active row for the name, then upsert the requested
  `(hash, name)` as active.
- **Fetch-and-cache-by-hash, resolve-hash-first, compile-off-the-async-path.**
  The dispatch seam (`resolve_wasm_dispatch`) cheaply resolves the active **hash**
  (selecting only the hash column), then serves a compiled module from the
  in-process cache **without ever fetching bytes on a cache hit**. Only a cache
  miss loads the bytes — and even then the CPU-bound wasmtime **compile is
  deferred** to `PreparedWasmActivity::invoke`, which the worker runs on the
  blocking pool, so wasmtime compilation of a large module never stalls an async
  worker thread (and its polling / heartbeats / cancellation handling). The async
  resolve only ever does the hash resolve, the cache probe, and (on a miss) the
  byte fetch. This is the module-granularity analogue of build-routing (#171).
- **Bounded compiled-module cache.** The in-process cache is an LRU capped at
  `WASM_MODULE_CACHE_CAP` (64) versions, so repeated operator hot-swaps do not
  accumulate compiled code for every historical version until worker restart (old
  versions stay fetchable in Postgres, so an evicted hash simply recompiles on the
  next miss). Evicting a cached `Arc<Module>` is safe while an in-flight invocation
  still holds a clone — the invocation keeps its module alive. Proven by
  `compiled_module_cache_is_bounded_and_evicts_lru`.
- **Atomic hot-swap without worker restart.** Publishing a new version flips the
  active row; the **next** attempt resolves the new hash while an **already-resolved
  in-flight attempt keeps running its pinned compiled module** — proven
  deterministically (no wall-clock race) by
  `in_flight_dispatch_is_pinned_across_a_mid_flight_republish`.
- **Startup-seed (not publish).** `Worker::run` **seeds** every builder-registered
  WASM module to its shard database before polling
  (`seed_registered_wasm_modules`), so an embedder gets a working WASM activity by
  calling `HarvestBuilder::wasm_activity(...)` alone — no separate publish step.
  Seeding is *activate-only-if-absent*: it makes the embedded bytes available
  (fetchable-by-hash, so in-flight pinned attempts and this worker can run them)
  and activates them **only when no active version already exists** for the name.
  This is deliberately **not** a blind publish: in a rolling deploy where the DB is
  already hot-swapped to v2, a restarted older worker embedding v1 must not flip
  the shard back to v1. Proven by `seed_activates_only_when_no_active_version_exists`
  and `seed_does_not_clobber_an_existing_active_version`. (The always-activate
  `publish_wasm_module` remains the operator hot-swap primitive.)
- **Module-size cap.** `MAX_WASM_MODULE_BYTES` (32 MiB) is enforced **before** any
  hashing or DB work (`oversized_module_is_rejected_before_insert`).

Follow-ups (out of scope per the issue): an embedder-supplied blob backend for
large modules composing via the #524 `PayloadStore` pattern (keeping the module row
a reference), and a management HTTP publish route (trivial — the storage functions
already exist; deferred here to avoid churning the plugin `api.rs` hot region).

---

## 8. Failure model — every failure is an ordinary `ActivityFailed`

**Zero new `WorkflowEvent` variants, zero replay impact.** A WASM attempt records
the existing `ActivityScheduled` / `ActivityCompleted` / `ActivityFailed` events
and is indistinguishable from a native activity in history — verified by
`runs_one_native_and_one_wasm_activity_to_completion` (exactly two ordinary
`ActivityScheduled` + two `ActivityCompleted`, no wasm-specific type) and
`worker_runs_wasm_echo_to_completion_with_ordinary_events`.

Every guest-controlled failure maps to a typed `ActivityFailure` the worker records
as an ordinary `ActivityFailed`, honouring the activity's retry policy:

| condition | `error_type` | retryable? |
|-----------|--------------|-----------|
| ungranted capability / unsatisfied import (a genuine link failure) | `SandboxDenied` | no |
| fuel / wall-clock / memory-growth overrun, **or a fuel/epoch/memory failure in the module start section** | `ResourceExhausted` | yes |
| guest trap (incl. **a trap in the module start section**), ABI violation, non-JSON output, contained host-glue panic | `WasmTrap` | yes |
| cooperative cancellation (token fired mid-run) | `ResourceExhausted` | yes |
| no active module published | `WasmModuleUnavailable` | no |
| bytes fail integrity/compile (surfaces from `invoke`, since compile is deferred to the blocking pool) | `WasmModuleInvalid` | no |
| DB error resolving hash / fetching bytes | `WasmModuleLookupFailed` | yes |

A subtlety the classification is careful about (issue #965 review): wasmtime runs
a module's **start section** during `instantiate`, so an `instantiate` failure is
**not** unconditionally a capability denial. A fuel/epoch/memory/trap failure in
the start section is classified as the matching retryable `ResourceExhausted` /
`WasmTrap`, exactly like `alloc`/`run`; `SandboxDenied` is reserved for a genuine
unsatisfied-import link failure. Proven by
`start_section_trap_is_retryable_wasm_trap_not_sandbox_denied`,
`start_section_fuel_exhaustion_is_retryable_resource_exhausted`, and the
regression `ungranted_import_is_still_sandbox_denied_regression`.

A panic in the host glue is caught (`catch_unwind`) and mapped to `WasmTrap`, so a
guest can **never crash the worker process** and a runaway module **never trips the
poison-pill quarantine (#367)** — poison-pill remains defense-in-depth only, as the
issue requires. Note the **memory-growth classification** is currently a
string-match on the wasmtime error's `Debug` form (`"growing memory"` /
`"growing table"`), because a `StoreLimits` grow failure surfaces as a generic error
rather than a distinct `Trap` variant; this is guarded by
`memory_limit_is_resource_exhausted` and should be revisited on a wasmtime bump in
case the wording changes.

---

## 9. GA recommendation & open questions

**Recommendation:** the spike proves the shape is sound and harvest-shaped — one
Rust fleet, guest modules as content-addressed Postgres rows, dispatch through the
unchanged task queue, and every failure mode surfacing as an ordinary
`ActivityFailed` so history/replay/retries/circuit-breakers/DLQ all work unchanged.
The dispatch overhead bar is met with ~24× headroom. The three things standing
between the spike and GA are, in priority order:

1. **Cooperative heartbeat + graceful-cancel host imports** (§4) — the remaining
   functional gap. Cancellation now *interrupts* a running guest promptly (via the
   epoch-deadline callback), so a cancelled WASM activity no longer holds a
   blocking thread until its ceiling; what is still missing is a `harvest_heartbeat`
   import for mid-run progress and a *cooperative* cancel-check the guest can poll
   to run cleanup before the trap fires.
2. **Component-model + WIT ABI** (§2/§3) — typed, versioned guest contracts and a
   real per-language bindings story, replacing the JSON-over-`i64`-packing
   convention.
3. **Instance pooling** (§5) — to drive per-invocation instantiate down for
   high-throughput activities.

Then, in a second wave: module signing/provenance (§6), a management HTTP publish
route + `PayloadStore` blob backend (§7), and a second demo guest language with a
real SDK (the issue scopes SDK ergonomics beyond one demo language as follow-up).

**Open questions for GA:**

- Should heartbeat/cancel be cooperative-only (guest polls), or should the runtime
  also support hard preemption (run the guest on a dedicated OS thread the host can
  interrupt at the epoch, rather than `spawn_blocking`)? The latter removes the
  "holds a blocking thread until the ceiling" limitation but complicates the
  execution model.
- WIT versioning policy: how does a `name@hash` binding interact with a WIT world
  version — is the world version part of the content hash, or a separate axis?
- Do we expose per-activity capability grants to operators at runtime (a
  management surface to tighten/loosen a live activity's sandbox), or are they
  compile-time-only via the builder as today?
- Fuel-to-wall-clock calibration: fuel is deterministic but not time; should
  `start_to_close` alone bound a WASM activity, or should operators also tune a
  fuel budget per activity class?

---

## 10. Known limitations (spike)

- **Mandatory 300 s wall-clock ceiling (as the hard backstop).** A guest runs on a
  blocking-pool thread; the mandatory `limits.max_wall_clock` (default 300 s) is the
  hard termination guarantee for a guest that neither completes, exhausts fuel, nor
  is cancelled. Cancellation itself is now delivered promptly (within ~1 epoch tick)
  via the epoch-deadline callback (§4), so it no longer takes the ceiling to reclaim
  a cancelled guest — but the interrupt is a *trap*, so a guest still cannot run its
  own cleanup on cancel, and there is no mid-run heartbeat import.
- **No mid-run heartbeat / graceful cancel.** There is no `harvest_heartbeat` host
  import for progress reporting, and no cooperative cancel-check the guest can poll
  to unwind gracefully before the epoch trap (§4).
- **Local WASM activities are out of scope.** `ActivityInfo::wasm` builds a normal
  (non-local) registered activity dispatched through the worker's task-queue WASM
  seam; a `local = true` WASM activity (inline on the workflow task) is not
  supported.
- **Memory-growth failure classification is a string match** on the wasmtime error
  Debug form (§8) — guarded by a test, but revisit on a wasmtime upgrade.
- **CI executes the DB test suite via the `linux` integration manifest row**
  (`.github/ci/integration-suites.txt`), i.e. against a Docker Postgres, since the
  suite needs a live migrated database. Locally it runs against any migrated
  Postgres via `HARVEST_TEST_DATABASE_URL`.
- **Guest randomness is non-cryptographic** (xorshift64), by design.

---

## 11. Test evidence

Recounted precisely (do not extrapolate):

- **`src/wasm_activities.rs`** (runtime unit tests): **34** `#[test]` fns —
  `deadline_ticks` rounding/clamping/saturation, hash/cache, echo round-trip, the
  three sandbox-deny categories (fs/net/env), the three capability grant paths
  (clock/random/env) + in-band env denial + the huge-`key_len` and value-length
  `env_get` cases, fuel/memory/wall-clock/mandatory-ceiling resource exhaustion,
  the concurrent-deadline-isolation test, and the containment suite (unreachable-
  trap / OOB-output / missing-export / non-JSON-output / deny-all-default /
  limits-default). Plus the round-2 review additions: **start-section
  retryability** (`start_section_trap_is_retryable_wasm_trap_not_sandbox_denied`,
  `start_section_fuel_exhaustion_is_retryable_resource_exhausted`,
  `ungranted_import_is_still_sandbox_denied_regression`), **cooperative
  cancellation** (`cancellation_interrupts_a_long_running_guest_promptly`,
  `an_uncancelled_token_does_not_interrupt_a_normal_guest`), the **LRU-bounded
  cache** (`compiled_module_cache_is_bounded_and_evicts_lru`), and **table resource
  bounding** (`huge_table_declaration_is_bounded_resource_exhausted`,
  `table_grow_past_cap_is_bounded_resource_exhausted`).
- **`src/wasm_store.rs`** (storage unit tests): **4** `#[test]` fns — registration
  defaults, fluent setters, binding projection, module-size-cap constant.
- **`tests/integration/wasm_activities_tests.rs`** (DB + worker-seam integration):
  **20** tests — **19 run in CI** via the `linux  autumn-harvest  integration
  wasm-activities  wasm_activities_tests` manifest row (storage round-trips,
  hot-swap + single-active + composite-PK independence, the concurrent-publish
  race, oversized-module reject, startup-seed, `resolve_wasm_dispatch`
  unavailable/invoke, the **deferred-compile-on-miss** and
  **invalid-surfaces-at-invoke** cases, the two **startup-seed activate-only-if-
  absent** cases, the worker-e2e echo, the sandbox-denial terminal, the
  in-flight-pin, the fuel-retry, and the **1-native + 1-WASM success-metric e2e**),
  plus **1 `#[ignore]`d** dispatch-overhead microbenchmark (`--ignored`, not a CI
  gate).

All integration tests ran **green against a local Postgres 16** during this
milestone via `HARVEST_TEST_DATABASE_URL`; CI runs them Docker-backed via the
manifest row above.

---

## Runtime choice: wasmtime vs wasmer

This section is the **deep** runtime comparison that §1 summarizes. §1 records the
decision ("wasmtime, adopted"); this section shows the evidence, maps each of the
spike's ten runtime requirements to the concrete mechanism in each runtime, and
scopes the port cost of switching. All claims are dated; where a fact could not be
verified from a primary source it is marked *unverified*.

**Recommendation: STAY on wasmtime.** Not "offer both behind the flag" — see the
end of this section for why a dual-runtime spike is a net negative here.

### The five load-bearing reasons

1. **Hard wall-clock termination has no clean wasmer equivalent (the deciding
   factor).** The mandatory 300 s ceiling (§4, §10) is enforced by wasmtime's
   *epoch interruption* — a background counter the guest's compiled code checks at
   function prologues and loop back-edges, so a guest is trapped at a safe point
   *even while it is spinning or blocked in a host call*, and it is enforced
   per-`Store` so N concurrent invocations get independent deadlines. wasmer has
   **no built-in mechanism to interrupt a running instance at a wall-clock
   deadline** (see the parity table, requirement 4). Its only CPU bound is the
   Metering middleware (operator-count "points"), which does **not** stop a guest
   that spins without executing counted operators or that blocks in a host import —
   exactly the case the spike calls out as why a wall-clock bound is *mandatory*
   rather than optional. Reproducing our hard-termination guarantee on wasmer
   requires an external watchdog thread that cannot cleanly interrupt a running
   instance, i.e. a weaker guarantee for a sandboxing-first feature.

2. **The GA path is the component model, and wasmtime is its reference
   implementation.** §2/§3 name component-model + WIT + WASI 0.2 as the GA
   direction. Wasmtime is the Bytecode Alliance reference implementation of the
   component model and tracks WASI 0.2.x release-for-release (46.0.0 ships WASI
   0.2.12, 2026-06-22). Wasmer's component-model / WASI-P2 support has historically
   trailed the reference implementation. Switching to wasmer now would move *away*
   from the runtime we will lean on hardest at GA.

3. **wasmer's one differentiator for our use case — Singlepass fast compile — is
   now source-available (BUSL-1.1), not open source.** As of Wasmer 6.0
   (relicensing announced 2025-04-24), the Singlepass compiler moved from MIT to
   the Business Source License 1.1; the rest of wasmer stays MIT. The single reason
   we might switch (near-instant compile for untrusted, frequently-changing guest
   bytes) is the one component we could not adopt as a clean OSS dependency.
   wasmtime is `Apache-2.0 WITH LLVM-exception` end-to-end (verified against the
   repo `LICENSE`), which is unambiguously fine for an embeddable dependency.

4. **Caching already neutralizes the compile-time argument.** The spike compiles
   each module once and caches it by SHA-256 (§5, §7), so Cranelift's slower cold
   compile is a **one-time per-hash** cost, not a per-invocation cost. Singlepass's
   fast compile buys essentially nothing at steady state for our workload — and it
   is BUSL-1.1 anyway (reason 3). The metric the spike actually has to hit is
   per-invocation instantiate overhead (< 10 ms p99), which is met with ~24×
   headroom on wasmtime and is a runtime-agnostic instantiation cost, not a
   compile-time one.

5. **Governance and security cadence favor the Bytecode Alliance.** wasmtime is
   maintained by the vendor-neutral Bytecode Alliance with a published,
   time-boxed advisory process (coordinated advisories 2026-04-09; 46.0.1 shipped a
   WASI security fix, GHSA-4ch3-9j33-3pmj, two days after 46.0.0 on 2026-06-24) and
   an "N-1 LTS" release policy. wasmer is maintained by Wasmer, Inc.; the Singlepass
   BUSL relicensing is itself evidence of a commercial pivot ("sponsorship provided
   less value than the effort," 2025-04-24), which is a long-term-maintenance signal
   worth weighing for a dependency that is the trust boundary for untrusted code.

**Where wasmer genuinely wins (stated fairly).** Raw steady-state run speed is
marginally better in the 2026 measurements (Wasmer 7.1.0 ≈ 1.33× native vs
Wasmtime 46.0.0 ≈ 1.46× native on a `wide_arithmetic`-enabled crypto benchmark,
2026-06-23) — a small edge, on a workload unlike ours, and both are "close to
native." wasmer 6.0+ can also compile multiple backends into one binary and swap
them at runtime, and Singlepass compiles far faster than Cranelift. None of these
outweigh reasons 1–3 for a *sandboxing-first, cache-amortized, component-model-bound*
feature, and the fast-compile advantage is both neutralized by our cache and
encumbered by BUSL-1.1.

### Requirement → wasmtime mechanism → wasmer mechanism → gap

| # | Requirement | wasmtime mechanism (what the spike uses) | wasmer equivalent | Parity / gap |
|---|-------------|------------------------------------------|-------------------|--------------|
| 1 | **Deny-by-default sandbox** | Fresh empty `Linker::new(engine)`; an ungranted import is unsatisfied → instantiate error → non-retryable `SandboxDenied`. No WASI, no ambient FS/net/env. | Empty `Imports` (formerly `ImportObject`); an unsatisfied import fails instantiation with a `LinkError`. Headless/no-WASI by simply not adding WASI imports. | **Parity.** Deny-by-default is standard core-WASM linking; both make an ungranted import fail at instantiation. |
| 2 | **Per-invocation CPU/fuel metering** | `Config::consume_fuel(true)` + `Store::set_fuel(...)` per call; deterministic, per-`Store`, resettable; bounds a pure-compute loop. | `wasmer-middlewares` **Metering** (points/gas): `get_remaining_points`/`set_remaining_points`, `MeteringPoints`. Per-run budget, resettable per invocation via `set_remaining_points`. | **Near-parity with a caveat.** Metering is **applied at module compile time** (instrumentation baked into the compiled artifact), so the cost function is fixed per *cached* module — you reset the *counter* per invocation, but you cannot change the cost model without recompiling. Interacts with the content-hash compiled-module cache: the metering config becomes part of what the cached artifact encodes. |
| 3 | **Memory limits** | `StoreLimitsBuilder::new().memory_size(...).trap_on_grow_failure(true)` + `Store::limiter`; over-ceiling `memory.grow` becomes a classifiable trap. | `Tunables` / `BaseTunables` memory bounds; exceeding the limit raises a `MemoryError` at the failing `memory.grow`. | **Parity, different surface.** Both cap linear-memory growth; wasmer surfaces a `MemoryError` rather than a `StoreLimits` trap, so the error-classification glue (`classify_wasmtime_err_phase`, §8) would be rewritten (and the §8 "memory-growth is a string match" caveat changes shape). |
| 4 | **Wall-clock deadline with true per-invocation isolation** | `Config::epoch_interruption(true)` + `Store::set_epoch_deadline(...)` off a single global `increment_epoch()` ticker; traps a spinning/host-blocked guest at a safe point; per-`Store` independent deadlines. | **No built-in equivalent.** No epoch-style interrupt; CPU is bounded only by Metering *points*, which do not fire for a guest that spins on host-blocked work or otherwise executes few counted operators. The maintainer-documented path is an external watchdog thread and/or points-as-a-CPU-proxy. | **NO CLEAN WASMER EQUIVALENT — the deciding gap.** Our *mandatory* 300 s hard-termination ceiling (§4/§10) cannot be reproduced cleanly: an external watchdog cannot interrupt a running wasmer instance at a safe point the way epoch interruption does. This is a *weaker security guarantee*, not just a different API. |
| 5 | **Content-hash hot-swap** | `Module::new(engine, bytes)`, cache by SHA-256, swap active module for the next attempt; Cranelift native-speed cached modules. | `Module::new` + serialize/deserialize; cache by hash identically. Singlepass compiles much faster than Cranelift. | **Parity (runtime-agnostic).** The swap logic lives in our storage layer, not the runtime. Singlepass's fast compile is neutralized by our compile-once cache (compile is one-time per hash) and is BUSL-1.1 (requirement 8). |
| 6 | **MSRV 1.88 compat** | wasmtime supports the latest three stable Rust releases (~3 months); the feature is `wasm-activities`-gated so the default `cargo check --workspace` (MSRV 1.88.0 job) never compiles the runtime. | wasmer tracks recent stable Rust (exact current MSRV *unverified*). | **No blocker either way.** The runtime is feature-gated out of the MSRV job, so neither runtime's own MSRV constrains the default build. Adopting either only matters for a consumer who enables the feature. |
| 7 | **Dependency weight / compile time / CI disk** | wasmtime + Cranelift: ~6 min cold build, ~8 GB in our sandbox. | wasmer: Singlepass backend is lighter than LLVM and comparable to / lighter than Cranelift; multi-backend builds (6.0+) can pull more. LLVM backend is heavy. | **Roughly comparable; wasmer-Singlepass is lighter** — but Singlepass is BUSL-1.1, and the feature is gated so the cost lands only on consumers who opt in. Not decisive. |
| 8 | **Licensing** | `Apache-2.0 WITH LLVM-exception` (verified against repo `LICENSE`). | Core + `wasmer-middlewares` are **MIT** (verified: wasmer-middlewares 7.2.0 is MIT). **Singlepass is BUSL-1.1** (source-available, *not* OSS) since Wasmer 6.0 (2025-04-24). | **wasmtime is cleaner.** wasmer's MIT parts are fine, but its *fast-compile differentiator* (Singlepass) is the encumbered one — the one component we would switch *for* is the one we could not adopt as clean OSS. |
| 9 | **Maintenance / security posture** | Bytecode Alliance (vendor-neutral); coordinated advisories (2026-04-09), fast patch cadence (46.0.1 WASI fix GHSA-4ch3-9j33-3pmj, 2026-06-24), LTS policy. | Wasmer, Inc. (single-vendor); active releases (7.1.0 in 2026) but a documented commercial pivot (Singlepass BUSL relicensing rationale, 2025-04-24). | **Favors wasmtime** for a trust-boundary dependency: vendor-neutral governance + published advisory process vs single-vendor with a commercial-pivot signal. |
| 10 | **Component-model GA path** | Reference implementation of the component model; tracks WASI 0.2.x (0.2.12 in 46.0.0, 2026-06-22). | Functional WASI Preview 2 in recent releases but historically trails the reference implementation. | **Strongly favors wasmtime**, which is exactly the GA direction §2/§3 commit to. |

### Port cost, if we ever did switch (scoping only — not a plan of record)

The wasmtime coupling is **well-isolated to two files**; `worker.rs`, `failure.rs`,
and `info.rs` are runtime-agnostic (they dispatch through the seam and use our own
`ActivityFailure` error-type constants, not wasmtime types):

- **`src/wasm_activities.rs`** (~1159 lines) — the entire runtime. Direct wasmtime
  surface: `Config`/`Engine`/`Store`/`Linker`/`Module`/`Caller`/`Extern`/`Trap`,
  `consume_fuel`+`set_fuel`, `epoch_interruption`+`set_epoch_deadline`+
  `increment_epoch`, `StoreLimits`/`StoreLimitsBuilder`, and
  `classify_wasmtime_err_phase`.
- **`src/wasm_store.rs`** — only the cached-module type (`wasmtime::Module`). Trivial.

Per-mechanism rewrite cost:

| Mechanism | Port difficulty | Notes |
|-----------|-----------------|-------|
| `Engine`/`Config`, deny-all `Linker` → `Imports`, `Module` cache | **Low** (mechanical) | 1:1 API shape. |
| `StoreLimits` memory ceiling → `Tunables`/`BaseTunables` | **Moderate** | New error surface (`MemoryError`); rewrites part of `classify_wasmtime_err_phase`. |
| Fuel → Metering middleware | **Moderate** | Compile-time instrumentation → must be baked into the *cached* artifact; per-invocation reset via `set_remaining_points`; cost function fixed per module hash. |
| Error classification (`classify_wasmtime_err_phase`) | **Moderate** | Rewrite against wasmer error/trap types; the §8 memory-growth string-match caveat changes. |
| **Epoch wall-clock deadline → ???** | **High — a redesign, not a port** | No drop-in. Requires an external watchdog thread plus a cooperative points-check, or running each guest on a dedicated killable OS thread — a change to the execution model that **degrades** the hard-termination guarantee the spike depends on. This one item is why "switch" is expensive *and* risky. |

### Why not "offer both behind the flag"

Offering wasmer as a second flagged backend would (a) **double the sandbox trust
surface** — two sets of interruption/metering/memory-limit code to audit for the
most security-sensitive feature in the tree — and (b) ship a second path whose
**wall-clock hard-termination guarantee is provably weaker** (requirement 4), which
undermines the single most load-bearing security property. For an R&D spike whose
whole point is to prove *one* sound sandbox shape, a second runtime is a net
negative. Revisit only if a concrete GA requirement emerges that wasmtime cannot
meet and wasmer can — none does today.

### Sources (dated)

- wasmtime `LICENSE` — `Apache-2.0 WITH LLVM-exception` (verified against
  `github.com/bytecodealliance/wasmtime`, fetched 2026-07).
- wasmtime epoch interruption / interrupt semantics — *Interrupting Execution*,
  docs.wasmtime.dev; *Safe Module Termination with Wasmtime Epoch-Based
  Interruption*, systemshardening.com; `Config` docs, docs.wasmtime.dev.
- "wasmer provides no way to interrupt execution" — *Interrupt wasmer/wasmtime*,
  users.rust-lang.org thread (2021-05-27/28); reaffirmed by wasmer issue #2885
  *Time and memory limits* (opened 2022-05-14, closed/stale — memory via
  `Tunables`, no built-in wall-clock deadline).
- wasmer Metering middleware — `wasmer_middlewares::metering::Metering`
  (docs.rs, wasmer-middlewares 7.2.0, **MIT**): compile-time instrumentation,
  per-module, operator-count only, no wall-clock; `get_remaining_points`/
  `set_remaining_points`/`MeteringPoints`.
- wasmer Singlepass BUSL-1.1 relicensing — *Singlepass Relicensing*, wasmer.io
  (2025-04-24; MIT → BUSL-1.1, rest of wasmer stays MIT).
- wasmer 6.0 (backends, LLVM-for-production) — *Announcing Wasmer 6.0*, wasmer.io
  (2025-04-25).
- wasmtime 46.0.0 release (WASI 0.2.12, component model) — 2026-06-22;
  46.0.1 WASI security fix GHSA-4ch3-9j33-3pmj — 2026-06-24
  (github.com/bytecodealliance/wasmtime releases).
- wasmtime security cadence / LTS — Bytecode Alliance advisories (2026-04-09);
  *Wasmtime LTS Releases*, bytecodealliance.org; wasmtime MSRV policy (latest 3
  stable Rust), docs.wasmtime.dev *Coding Guidelines*.
- 2026 performance snapshot (Wasmtime 46.0.0 ≈ 1.46× native, Wasmer 7.1.0 ≈ 1.33×
  native with `wide_arithmetic`) — *Performance of WebAssembly runtimes in 2026*,
  00f.net (2026-06-23).
- WASI 0.2 / component-model status (wasmtime = reference implementation; wasmer
  trails) — *WASI and the WebAssembly Component Model: Current Status*, eunomia.dev
  (2025-02-16); *WASI 0.2.0 and Why It Matters*, wasmCloud (2024).
