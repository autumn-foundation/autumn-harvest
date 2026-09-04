# Loom concurrency model checking

[Loom](https://github.com/tokio-rs/loom) is a permutation-testing tool for
concurrent Rust. Given a small test that uses loom's instrumented `Arc`,
`Mutex`, and atomics, loom **exhaustively explores every meaningful thread
interleaving** and every legal memory ordering, and re-runs the test body once
per interleaving. A safety assertion that holds across all of them is a much
stronger guarantee than a stress test that happened to pass a few thousand
random times.

This repo uses loom to model-check the two in-process, `Mutex`-guarded data
structures that are genuinely shared across threads.

## TL;DR — running the models

```bash
# From the repo root. No database required. --release is much faster (loom's
# instrumentation is heavy in debug).
RUSTFLAGS="--cfg loom" cargo test -p autumn-harvest --no-default-features --test loom_models --release
```

Normal `cargo test` **never** runs these models: `tests/loom_models.rs` is
gated with `#![cfg(loom)]`, so without the `--cfg loom` flag it compiles to an
empty crate and runs nothing. There is a manual, `workflow_dispatch`-only CI
job at `.github/workflows/loom.yml` (see "CI" below).

## What loom can and cannot check

**Loom models in-process synchronization only** — its instrumented `Arc`,
`Mutex`, `RwLock`, atomics, and `thread::spawn`. It does not model time
(`std::time::Instant` is passed through as an ordinary value), async runtimes,
`tokio` primitives, real OS threads at scale, or anything outside the process.

### Honest coverage caveat

**The majority of harvest's concurrency is coordinated through Postgres, not
in-process locks, and is therefore entirely out of loom's reach.** That includes,
non-exhaustively:

- the task-queue claim race (`SELECT ... FOR UPDATE SKIP LOCKED`),
- the `wake_requested` re-pend / park race between a completing sibling and a
  parking workflow task (issue #601 hardening),
- the HA scheduler's `fire_claim_token` / `fire_claimed_until` exclusivity guard
  (issue #350),
- the start-idempotency `ON CONFLICT` upsert (issue #808),
- per-key concurrency and start-throttle token debits.

These are exercised by the Docker-backed integration tests against a real
Postgres, **not** by loom. Loom is a complement to those, targeting the handful
of places where harvest coordinates threads with a plain in-memory `Mutex`.
Do not read a green loom run as "harvest's concurrency is verified" — read it as
"these specific in-process state machines are interleaving-safe."

## Modules under test — include / exclude rationale

| Module | loom target? | Why |
|--------|--------------|-----|
| `circuit_breaker.rs` | **Included** | `Mutex<HashMap<String, BreakerState>>` shared (behind an `Arc`) between the worker dispatch path and the management API. The generation-fence and single-half-open-probe invariants are exactly the kind of lock-ordering-sensitive safety properties loom is built to verify. |
| `sessions.rs` (slot registry) | **Included** | `SessionSlotRegistry = Arc<Mutex<HashSet<SessionId>>>` shared between concurrent session-acquire task claims on one worker. The capacity bound and acquire/release balance are in-process invariants. |
| `slot_tuner.rs` | **Excluded (Shuttle candidate)** | Coordinates through `tokio::sync::Semaphore` / `OwnedSemaphorePermit` and async atomics. **Loom cannot model tokio async primitives**, so it cannot reach the withheld-permit accounting invariant (`withheld + available + in-flight == max`). This is the strongest argument for a Shuttle fast-follow — see `concurrency-model-checking.md`. |
| `cache.rs` | **Excluded** | The LRU is a single-threaded `&mut self` structure with no internal locking; there is no interleaving to explore. Concurrency is provided externally by whoever owns the cache. |
| `heartbeat.rs` | **Excluded** | Coordinates through a `tokio::sync::mpsc` channel and the tokio runtime. Loom does not model tokio channels; this is async-runtime territory (Shuttle, not loom). |

## The models (`tests/loom_models.rs`)

Each test wraps its body in `loom::model(|| { ... })`, uses **exactly two**
`loom::thread` threads doing **at most two** operations each, and drives the
**real** production types (`CircuitBreakerRegistry`, the real `sessions`
functions) — not a re-implementation.

1. **`circuit_breaker_stale_failure_cannot_retrip_a_reset_breaker`** (headline).
   A generation-stamped `DispatchToken` captured before an operator reset races
   the reset itself; the stale retryable failure must never re-trip the breaker
   under any interleaving. Proves the generation fence + `forced_open` guard
   compose correctly under concurrency.
2. **`circuit_breaker_admits_at_most_one_half_open_probe`.** Two dispatches race
   for the single half-open probe slot after cooldown; exactly one is admitted.
3. **`session_slot_bound_admits_at_most_one_at_capacity_one`.** Two acquires at
   capacity 1 (distinct ids) race; exactly one wins and the bound is never
   exceeded.
4. **`session_slot_acquire_release_balances_and_never_underflows`.** At capacity
   two, both threads acquire distinct ids and release their own; under every
   interleaving the registry drains back to empty and a fresh third acquire then
   succeeds — proving release genuinely frees reusable capacity, not just a
   counter.

## The `cfg(loom)` shim (`src/loom_sync.rs`)

```rust
#[cfg(loom)]      pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(loom))] pub(crate) use std::sync::{Arc, Mutex, MutexGuard};
```

Only `circuit_breaker.rs` and `sessions.rs` import from this shim — the swap is
**contained to the modules under test**, never swept across the whole crate.

Two properties keep production completely unaffected:

- Under a normal build the alias is `std::sync`, so the generated code is
  byte-for-byte identical to before.
- loom's `Mutex::lock()` returns the *same* `std::sync::LockResult` type as std
  and never actually poisons, so the existing poison-tolerant
  `.unwrap_or_else(std::sync::PoisonError::into_inner)` call sites compile and
  behave identically under both configurations — the lock call itself needs no
  `cfg`-gating.

`std::time::Instant` is deliberately **not** routed through the shim: loom does
not model time, so the breaker keeps using real `Instant`s (passed in as plain
values).

### Why loom lives under `[target.'cfg(loom)'.dependencies]`

`loom` is declared as:

```toml
[target.'cfg(loom)'.dependencies]
loom = "0.7.2"
```

not as a `[dev-dependencies]`. The `loom_sync` shim references `loom::sync::*`
from the **library** sources (the `SessionSlotRegistry` type alias is production
code, not `#[cfg(test)]` code). `[dev-dependencies]` are *not* available to the
normal library build that the `tests/loom_models.rs` integration target links
against, so a dev-dependency would fail to resolve `loom` from `src/`. The
`cfg(loom)` target dependency is inert (not fetched, not compiled) on every
normal build and only pulled in under `RUSTFLAGS="--cfg loom"`.

## Adding a new model

1. Confirm the target is genuinely in-process and lock/atomic-based (not tokio
   async — that's a Shuttle candidate).
2. Route its `Arc`/`Mutex`/atomics through `crate::loom_sync` (or `loom::sync::*`
   in an atomic's case), gated so the non-loom path stays `std`.
3. Add a `#[test]` in `tests/loom_models.rs` wrapping the body in
   `loom::model(|| ...)`. Keep it to two threads and the fewest ops that still
   expresses the invariant.
4. Assert a **safety invariant** (something true in every interleaving), not a
   specific ordering.

## Model size and preemption bounds

Loom explores every interleaving, so state spaces grow fast with thread count
and per-thread operations. Guidance, strongest first:

- **Two threads, ≤ 2 ops each** (what every model here uses). Exhaustive and
  fast (well under a second per model in `--release`).
- If a model is slow, **cut operations** or shrink the shared state before
  reaching for a bound — a smaller exhaustive model is worth more than a large
  bounded one.
- As a **last resort**, cap exploration with `loom::model::Builder`:

  ```rust
  let mut b = loom::model::Builder::new();
  b.preemption_bound = Some(3); // explore up to 3 preemptions only
  b.check(|| { /* model body */ });
  ```

  A `preemption_bound` makes exploration **non-exhaustive** — document the loss
  of guarantee in a comment when you use one. (The bound can also be set from the
  environment via `LOOM_MAX_PREEMPTIONS`.)

## Candidate backlog

- **`slot_tuner.rs` withheld-permit accounting** — needs Shuttle (async /
  tokio semaphore), not loom. Top of the fast-follow list; see
  [`concurrency-model-checking.md`](concurrency-model-checking.md).
- **`heartbeat.rs` mpsc flush ordering** — Shuttle candidate (tokio mpsc).
- Any future in-process lock-coordinated state machine added to the engine.

## CI

`.github/workflows/loom.yml` runs the suite on **`workflow_dispatch` only** —
it is never part of the required-status matrix in `ci.yml`, so it adds no cost
to normal PR/push runs. Trigger it manually from the GitHub Actions tab
("Loom model checking" → "Run workflow") or with the CLI:

```bash
gh workflow run loom.yml --ref <branch>
```
