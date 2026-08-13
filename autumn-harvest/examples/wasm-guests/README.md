# WASM activity guests (issue #965)

Demo guest modules for Harvest's sandboxed WASM activities (the `wasm-activities`
Cargo feature, an R&D spike — see `docs/rnd/wasm-activities-spike.md`). These guests
show that the host/guest contract is **language-agnostic**: a guest is any WASM
module — from any source language — that satisfies the ABI below.

## Files

| File | Language | Role |
|------|----------|------|
| `echo.wat` | WebAssembly text | **Executed by CI.** `include_str!`d by the `wasm-activities` integration tests and the `wasm_activity` example, assembled with `wat::parse_str(...)`, then run through the worker's WASM dispatch seam. Hand-written, no toolchain. |
| `echo.ts` | AssemblyScript | **Source** for `echo.wasm`. The same `alloc`/`run` contract in a real source language. Rebuild with the `asc` command below. |
| `echo.wasm` | (compiled from `echo.ts`) | **Executed by CI.** `include_bytes!`d by `worker_runs_an_assemblyscript_compiled_guest_to_completion`, which runs it end-to-end through the standard dispatch path. Committed so the suite needs no npm toolchain and the bytes are deterministic. |

Every file here is referenced by code — nothing in this directory is decorative.
If you change `echo.wat`, the tests and the example pick it up automatically; if
you change `echo.ts`, **recompile and commit `echo.wasm`** or the test keeps
running the old bytes.

## Why both a `.wat` and a real-language guest?

`.wat` is the *textual encoding of the wasm binary format itself* — `wat::parse_str`
is an assembler, not a compiler from a distinct language. It proves the host is not
Rust-specific, but it exercises no real toolchain's codegen, allocator, or memory
layout. The AssemblyScript guest does, and that difference is not academic: the
first compiled build of `echo.ts` **failed** against the host, because
`asc --runtime stub` emits a module with **zero initial memory pages**, so the
bump pointer handed back an address no memory backed. A hand-written `.wat` that
declares `(memory 1)` can never surface that class of bug. `alloc` in `echo.ts`
now grows memory explicitly — see the comment there.

Both implement an **echo** activity (output = input) so the ABI itself is the point.

## The ABI — JSON over linear memory

A guest module exports exactly three things:

- **`memory`** — its linear memory.
- **`alloc(len: i32) -> i32`** — return a pointer to `len` writable bytes inside
  guest memory (a bump allocator suffices). The host calls this **once** to place
  the serialized JSON activity input.
- **`run(in_ptr: i32, in_len: i32) -> i64`** — execute the activity. The input JSON
  bytes live at `in_ptr .. in_ptr + in_len`. The return value packs the output
  location as `((out_ptr as u64) << 32) | (out_len as u64)`; the host reads
  `out_len` bytes at `out_ptr` and deserializes them as JSON.

Host-side robustness (you don't need to do anything for this, but it's why a
malformed guest can't hurt the worker):

- The host reinterprets the returned `i64` as `u64` **before** shifting, so a high
  bit in `out_ptr` never sign-extends.
- Every host read/write is bounds-checked against live guest memory; an out-of-range
  `(ptr, len)` becomes a typed `WasmTrap` (recorded as an ordinary `ActivityFailed`),
  never a host out-of-bounds read.
- Output that isn't valid JSON is a `WasmTrap`, not a crash.

## Sandbox & capabilities

The guest runs **deny-all** by default: no filesystem, no network, no environment,
no clock, no randomness. A guest that imports a host function it wasn't granted is
**denied at instantiation** (non-retryable `SandboxDenied`). Grantable capabilities
in this spike are `env::now_millis` (clock), `env::random_u64` (non-crypto), and
`env::env_get` (allowlisted keys); filesystem and network are **not grantable** (no
host function backs them). Grants are configured per-activity on the builder via
`WasmActivityRegistration::with_capabilities(...)` — see `../wasm_activity.rs`.

## Building the AssemblyScript guest

`echo.ts` is illustrative and not built by CI. To compile it yourself:

```bash
# One-time: install the AssemblyScript compiler.
npm install -g assemblyscript

# Compile to a raw .wasm implementing the alloc/run ABI. `--runtime stub` keeps a
# minimal allocator (we bump-allocate ourselves), and memory is exported by default.
asc echo.ts -o echo.wasm --optimize --runtime stub
```

Then publish `echo.wasm` for an activity the same way the example publishes the WAT
guest (`HarvestBuilder::wasm_activity(WasmActivityRegistration::new("echo", bytes))`).

Other source languages reach the same ABI similarly — TinyGo (`//export alloc` /
`//export run`), Rust (`#[no_mangle] pub extern "C" fn ...` targeting
`wasm32-unknown-unknown`), etc. A component-model + WIT contract is the recommended
GA replacement for this hand-rolled ABI (see the spike recommendation, §2/§3).
