## Phase 4 — Loom concurrency model checking for the in-process state machines (testing infra)

Bootstraps a [loom](https://github.com/tokio-rs/loom) model-checking workstream that exhaustively explores the thread interleavings of harvest's two genuinely-concurrent, in-process, `Mutex`-guarded data structures — the per-activity circuit breaker (`circuit_breaker.rs`) and the worker session-slot registry (`sessions.rs`). Loom `= "0.7.2"` was already an (unused) dev-dependency; this wires it up. **No production behavior change, no new `WorkflowEvent` variant, no migration** — the entire change is test infrastructure plus a `cfg`-gated sync-primitive alias.

**Contained `cfg(loom)` shim.** New `src/loom_sync.rs` re-exports `Arc`/`Mutex`/`MutexGuard` — `std::sync` under a normal build (byte-for-byte identical to before), `loom::sync` under `RUSTFLAGS="--cfg loom"`. Only `circuit_breaker.rs` and `sessions.rs` import from it; the swap is deliberately **not** swept across the crate. Loom's `Mutex::lock()` returns the same `std::sync::LockResult` and never poisons, so the existing `.unwrap_or_else(std::sync::PoisonError::into_inner)` call sites compile and behave identically under both configs with no `cfg`-gating of the lock call. `std::time::Instant` is deliberately left as std (loom does not model time; real `Instant`s are passed through).

**Models (`tests/loom_models.rs`, `#![cfg(loom)]`-gated so `cargo test` never runs them).** Four models, each two `loom::thread` threads / ≤2 ops, driving the real types:
1. `circuit_breaker_stale_failure_cannot_retrip_a_reset_breaker` (headline) — a generation-stamped `DispatchToken` captured before an operator reset races the `force_close`; the stale retryable failure is fenced under every interleaving (breaker ends CLOSED, empty window). Proves the generation fence + `forced_open` guard compose under concurrency.
2. `circuit_breaker_admits_at_most_one_half_open_probe` — two dispatches race for the single half-open probe slot; exactly one is admitted.
3. `session_slot_bound_admits_at_most_one_at_capacity_one` — the capacity bound holds under a concurrent acquire race.
4. `session_slot_acquire_release_balances_and_never_underflows` — concurrent acquire/release drains the registry to empty.

**Build wiring.** loom is moved to `[target.'cfg(loom)'.dependencies]` (not `[dev-dependencies]`) because the shim references `loom::sync::*` from the *library* sources (the `SessionSlotRegistry` type alias is production code), and dev-deps are not available to the normal library build the integration test links against. Under a global `--cfg loom`, tokio's `net` module is compiled out (`#![cfg(not(loom))]`), so every `tokio::net` consumer must be kept out of the loom graph: the run uses `--no-default-features` (drops `db`→`tokio-postgres`) and `testcontainers` is gated to `[target.'cfg(not(loom))'.dev-dependencies]` (it pulls `hyper-util`). `cfg(loom)` is registered in `[workspace.lints.rust]` `check-cfg` so `unexpected_cfgs` does not fire under normal `-D warnings` builds.

**No interleaving bug found** — all four models pass. Run manually:
```
RUSTFLAGS="--cfg loom" cargo test -p autumn-harvest --no-default-features --test loom_models --release
```

**CI.** New `.github/workflows/loom.yml` runs the suite on `workflow_dispatch` **only** — never in the required-status matrix, so zero added cost on normal PR/push runs (respecting #999).

**Docs.** `docs/testing/loom.md` (what loom can/can't do, the module include/exclude rationale, an honest coverage caveat that the majority of harvest's concurrency lives in Postgres and is out of loom's reach, run instructions, how to add a model, and model-size/preemption-bound guidance) and `docs/testing/concurrency-model-checking.md` (an honest loom/Shuttle/Turmoil evaluation: loom adopted now; Shuttle recommended fast-follow for the async `slot_tuner.rs` semaphore-accounting invariant loom structurally cannot reach — backlogged rather than shipping a non-running PoC; Turmoil recommended against, since harvest coordinates through Postgres and has no custom peer networking for Turmoil to simulate — verified by repo search — with a recommendation matrix).
