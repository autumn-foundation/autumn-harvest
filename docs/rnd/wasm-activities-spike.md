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

## 4. Heartbeat / cancellation delivery into the guest — **partial in the spike**

This is the most honest gap. The spike delivers **timeouts** into the guest but
**not** cooperative heartbeat/cancellation.

What the spike does:

- **`start_to_close` → epoch wall-clock deadline.** The worker passes the
  activity's effective deadline; `invoke_wasm_activity` clamps it by the mandatory
  `limits.max_wall_clock` ceiling and sets `Store::set_epoch_deadline`. A guest
  that overruns is interrupted and the attempt fails as a retryable
  `ResourceExhausted("wall-clock deadline exceeded")`.
- **Fuel → CPU bound.** A runaway-CPU guest exhausts fuel and fails as a retryable
  `ResourceExhausted("cpu fuel exhausted")`.
- **Per-invocation deadline isolation via a single global epoch ticker.** One
  named background thread (`harvest-wasm-epoch`) advances the engine's epoch every
  `EPOCH_TICK_INTERVAL` (1 ms) for the store's whole lifetime. Each store expresses
  its own deadline as a number of ticks beyond the current epoch
  (`set_epoch_deadline(deadline_ticks(effective))`), so N concurrent invocations
  each get an **independent** wall-clock deadline driven by the **same** monotonic
  clock — one guest's expiry never trips another's. This is a single-global-ticker
  model (the ticker only ever calls `increment_epoch()`; it never reads or bumps
  per-invocation state), and it is exercised directly by
  `concurrent_deadlines_are_independent`.

What it does **not** do — and the honest limitation:

- There is **no** `harvest_heartbeat` host import and **no** cooperative
  cancel-check import, so a guest cannot report progress mid-run, and an operator
  cancel/heartbeat-timeout cannot interrupt a guest **before** its wall-clock
  ceiling. The guest runs to completion on the calling thread (the worker invokes
  `PreparedWasmActivity::invoke` inside `spawn_blocking`), and **it cannot be torn
  down before `limits.max_wall_clock`** — which is exactly why that ceiling is
  *mandatory* (defaulting to 300 s) rather than optional. A "cancelled" WASM
  activity keeps consuming a blocking-pool thread until its deadline fires.

What a full implementation needs:

- A `harvest_heartbeat(ptr, len)` host import the guest calls periodically,
  wired to the existing activity heartbeat channel, so progress and
  liveness flow through the same path as native activities.
- A cooperative cancel-check host import (or a host-set flag the guest polls) so a
  well-behaved guest can observe a cancel request and return early — the epoch
  deadline stays as the hard backstop for a guest that ignores the cooperative
  signal.

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
- **Fetch-and-cache-by-hash, resolve-hash-first.** The dispatch seam
  (`resolve_wasm_dispatch`) cheaply resolves the active **hash** (selecting only the
  hash column), then serves a compiled module from the in-process cache **without
  ever fetching bytes on a cache hit**. Only a cache miss loads the bytes and
  compiles. This is the module-granularity analogue of build-routing (#171).
- **Atomic hot-swap without worker restart.** Publishing a new version flips the
  active row; the **next** attempt resolves the new hash while an **already-resolved
  in-flight attempt keeps running its pinned compiled module** — proven
  deterministically (no wall-clock race) by
  `in_flight_dispatch_is_pinned_across_a_mid_flight_republish`.
- **Startup-publish is wired.** `Worker::run` publishes every builder-registered
  WASM module to its shard database before polling
  (`publish_registered_wasm_modules`), so an embedder gets a working WASM activity
  by calling `HarvestBuilder::wasm_activity(...)` alone — no separate publish step.
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
| ungranted capability / unsatisfied import | `SandboxDenied` | no |
| fuel / wall-clock / memory-growth overrun | `ResourceExhausted` | yes |
| guest trap, ABI violation, non-JSON output, contained host-glue panic | `WasmTrap` | yes |
| no active module published | `WasmModuleUnavailable` | no |
| bytes fail integrity/compile | `WasmModuleInvalid` | no |
| DB error resolving hash / fetching bytes | `WasmModuleLookupFailed` | yes |

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

1. **Cooperative heartbeat + cancellation host imports** (§4) — the biggest
   functional gap; without them a "cancelled" WASM activity holds a blocking thread
   until its wall-clock ceiling.
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

- **Mandatory 300 s wall-clock ceiling + `spawn_blocking` uncancellability.** A
  guest runs to completion on a blocking-pool thread and cannot be torn down before
  `limits.max_wall_clock` (default 300 s); there is no pre-ceiling cooperative
  interrupt (§4).
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

- **`src/wasm_activities.rs`** (runtime unit tests): **24** `#[test]` fns —
  `deadline_ticks` rounding/clamping/saturation, hash/cache, echo round-trip, the
  three sandbox-deny categories (fs/net/env), the three capability grant paths
  (clock/random/env) + in-band env denial, fuel/memory/wall-clock/mandatory-ceiling
  resource exhaustion, the concurrent-deadline-isolation test, and the containment
  suite (unreachable-trap / OOB-output / missing-export / non-JSON-output /
  deny-all-default / limits-default).
- **`src/wasm_store.rs`** (storage unit tests): **4** `#[test]` fns — registration
  defaults, fluent setters, binding projection, module-size-cap constant.
- **`tests/integration/wasm_activities_tests.rs`** (DB + worker-seam integration):
  **17** tests — **16 run in CI** via the `linux  autumn-harvest  integration
  wasm-activities  wasm_activities_tests` manifest row (storage round-trips,
  hot-swap + single-active + composite-PK independence, the concurrent-publish
  race, oversized-module reject, startup-publish, `resolve_wasm_dispatch`
  unavailable/invalid/invoke, the worker-e2e echo, the sandbox-denial terminal, the
  in-flight-pin, the fuel-retry, and the **1-native + 1-WASM success-metric e2e**),
  plus **1 `#[ignore]`d** dispatch-overhead microbenchmark (`--ignored`, not a CI
  gate).

All integration tests ran **green against a local Postgres 16** during this
milestone via `HARVEST_TEST_DATABASE_URL`; CI runs them Docker-backed via the
manifest row above.
