## Phase 3.55 — Run WASM-sandboxed polyglot activities inside the Rust worker (issue #965)

**R&D spike (not a committed GA feature)** proving polyglot activities: an activity
implemented as a WebAssembly module — compiled from any guest language — runs
sandboxed, metered, and hot-swappable inside the existing Rust worker, dispatched
through the **standard task-queue path** (JSON in, JSON out, honouring the
activity's queue/retry/start-to-close exactly like a native activity). One fleet,
any guest language, capability-based sandboxing, per-invocation resource metering,
and content-hash hot swap — capabilities no per-language-SDK durable-execution
engine offers. Everything is behind the `wasm-activities` Cargo feature; the
**default build carries zero new dependencies and is byte-for-byte unchanged**
(the feature adds `wasmtime 46` + `wat`, and implies `db`).

**Runtime (`src/wasm_activities.rs`).** A wasmtime 46 `Engine` configured once for
deterministic resource bounding — `consume_fuel` (per-instruction CPU) + a single
global epoch ticker (`harvest-wasm-epoch`, `increment_epoch()` every 1 ms) for
wall-clock interruption + `StoreLimits` for a linear-memory ceiling. Each
invocation gets a fresh `Store` with its own fuel budget and an **independent**
epoch deadline expressed as ticks beyond the current epoch, so N concurrent guests
each get their own wall-clock deadline off the same monotonic clock — one guest's
expiry never trips another's (`concurrent_deadlines_are_independent`). The
wall-clock ceiling (`max_wall_clock`, default **300 s**) is **mandatory**: a guest
can spin without consuming fuel, and it runs on `spawn_blocking` and cannot be torn
down before the ceiling — so fuel alone can never guarantee termination. **Guest
ABI is core-WASM + JSON over linear memory:** the guest exports `memory`,
`alloc(len)->ptr`, and `run(in_ptr,in_len)->i64` (packed `(ptr<<32)|len`); the host
reinterprets the return as `u64` before shifting and bounds-checks every read/write
against live guest memory (an OOB `(ptr,len)` is a typed `WasmTrap`, never a host
OOB read). **Sandbox is deny-all by default:** a fresh empty `Linker` links a host
function only for a capability explicitly granted via `WasmCapabilities`
(`allow_clock`/`allow_random`/`allow_env` allowlist); filesystem and network are
**not grantable** (no host function backs them), so any guest importing them is
denied at instantiation.

**Storage + dispatch seam (`src/wasm_store.rs`).** Modules are content-hash
(SHA-256) versioned and distributed via Postgres — no CDN/S3/broker in core. Table
`harvest_wasm_modules` (the single additive migration
`20260711000000_harvest_wasm_modules`) has a **composite PK `(hash, activity_name)`**
so identical bytes bind to two names independently, plus a **partial unique index
`WHERE active`** guaranteeing at most one active version per name. `publish_wasm_module`
serialises concurrent publishes for the same name with a per-name transaction-scoped
`pg_advisory_xact_lock`, so two workers converge on exactly one active row.
`resolve_wasm_dispatch` is **resolve-hash-first**: it resolves the active hash
cheaply and serves a compiled module from the in-process content-addressed cache,
fetching bytes **only** on a cache miss. Hot swap is atomic and restart-free — a new
publish flips the active row; the next attempt sees it while an already-resolved
in-flight attempt keeps running its pinned compiled module
(`in_flight_dispatch_is_pinned_across_a_mid_flight_republish`). **Startup-publish is
wired into `Worker::run`** (`publish_registered_wasm_modules`), so an embedder gets
a working WASM activity by calling `HarvestBuilder::wasm_activity(WasmActivityRegistration::new(...))`
alone. `MAX_WASM_MODULE_BYTES` (32 MiB) is enforced before any hashing or DB work.

**Typed failures, no new event surface (AC6).** Every guest-controlled failure maps
to a typed `ActivityFailure` recorded as an **ordinary `ActivityFailed`**: sandbox
denial → non-retryable `SandboxDenied`; fuel/wall-clock/memory-growth →
retryable `ResourceExhausted`; trap/ABI-violation/non-JSON-output/contained
host-glue panic → retryable `WasmTrap`; plus `WasmModuleUnavailable` /
`WasmModuleInvalid` (non-retryable) / `WasmModuleLookupFailed` (retryable) for the
storage layer. A host-glue panic is caught (`catch_unwind`), so a guest can never
crash the worker and a runaway module never trips the poison-pill quarantine (#367).
A WASM activity is **indistinguishable from a native one in history**: it records
the same `ActivityScheduled`/`ActivityCompleted`/`ActivityFailed` events. **Zero new
`WorkflowEvent` variants, zero replay impact, exactly one additive migration.**
(The memory-growth classification is a string match on the wasmtime error Debug
form, guarded by a test — revisit on a wasmtime bump.)

**R&D exit criteria (AC8).** Written recommendation in `docs/rnd/wasm-activities-spike.md`
covering runtime choice (wasmtime vs wasmer), core-WASM vs component-model ABI,
guest interface shape (JSON-over-buffer vs WIT-typed), the **partial**
heartbeat/cancellation story (timeouts delivered via epoch/fuel; cooperative
heartbeat/cancel host imports are the biggest GA gap — a "cancelled" guest holds a
blocking thread until its ceiling), the measured cold-start/instantiation cost, the
security posture, and the storage/distribution model — all backed by the working
spike.

**Success metric — met.** `runs_one_native_and_one_wasm_activity_to_completion`
drives a single workflow running **1 native + 1 WASM** activity to COMPLETED
through the real worker loop, asserting both outputs are correct **and** that
history carries only ordinary `ActivityScheduled`/`ActivityCompleted` (no
wasm-specific type). The `#[ignore]`d `dispatch_overhead_wasm_echo_vs_native`
microbenchmark measures per-invocation overhead of a **cached** echo (compilation
excluded) vs a native closure: WASM p50 ≈ 243 µs / p99 ≈ 427 µs, native p50 ≈ 3 µs
/ p99 ≈ 11 µs, **overhead p99 ≈ 0.42 ms — well under the < 10 ms target** (~24×
headroom; debug build, so a release worker is faster). The sandbox suite denies
100 % of ungranted FS/network/env attempts.

**Example.** `autumn-harvest/examples/wasm_activity.rs` (`required-features =
["wasm-activities"]`) assembles a WAT guest inline via `wat::parse_str`, registers
it via `HarvestBuilder::wasm_activity(...)`, and demonstrates a capability grant and
a sandbox denial. `autumn-harvest/examples/wasm-guests/` holds the WAT guest source
(the one CI executes), an AssemblyScript (`.ts`) guest implementing the same
`alloc`/`run` ABI (illustrative, not CI-compiled), and a `README.md` documenting the
ABI and the `asc` build command.

**Test evidence (recounted precisely).** 24 unit tests in `src/wasm_activities.rs`
(runtime: deadline math, hash/cache, echo round-trip, the three sandbox-deny
categories, the three capability grants + in-band env denial, fuel/memory/wall-clock/
mandatory-ceiling exhaustion, concurrent-deadline isolation, and the containment
suite); 4 unit tests in `src/wasm_store.rs` (registration defaults/setters, binding
projection, size cap); **17** tests in `tests/integration/wasm_activities_tests.rs`
— **16 run in CI** via the `linux  autumn-harvest  integration  wasm-activities
wasm_activities_tests` manifest row (storage round-trips, hot-swap + single-active +
composite-PK independence, the concurrent-publish race, oversized-module reject,
startup-publish, `resolve_wasm_dispatch` unavailable/invalid/invoke, the worker-e2e
echo, the sandbox-denial terminal, the in-flight pin, the fuel-retry, and the
1-native + 1-WASM success-metric e2e) plus 1 `#[ignore]`d overhead microbenchmark.
All integration tests **ran green against a local Postgres 16** during development
(via `HARVEST_TEST_DATABASE_URL`); CI executes them Docker-backed via the manifest
row. The `wasm-activities` feature is out of the default build, so the default build
and `cargo tree -i wasmtime` (default features) are unchanged / empty.
