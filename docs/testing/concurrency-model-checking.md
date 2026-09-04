# Concurrency model-checking: tool evaluation (loom / Shuttle / Turmoil)

This note records an honest evaluation of three model-checking / simulation
tools for harvest, and the resulting adoption decisions. The companion
[`loom.md`](loom.md) documents the loom workstream that this evaluation stands up now.

The single most important framing fact, repeated throughout: **the large
majority of harvest's concurrency is coordinated through Postgres** — `SELECT
... FOR UPDATE SKIP LOCKED` claims, the `wake_requested` re-pend/park race, the
HA scheduler `fire_claim_token` guard, the start-idempotency `ON CONFLICT`
upsert. None of these three tools can model a Postgres server. They only reach
harvest's comparatively small in-process concurrency surface. The durable /
cross-process races remain the province of the Docker-backed integration tests
against a real database.

## loom — adopted now

**What it is.** Exhaustive permutation testing of in-process synchronization
(instrumented `Arc`/`Mutex`/`RwLock`/atomics + `thread::spawn`). Explores every
meaningful interleaving and memory ordering.

**Fit here.** Good, for the narrow-but-real set of in-process, lock-guarded
state machines:

- `circuit_breaker.rs` — generation fence + single-half-open-probe under
  concurrent worker-dispatch/management-API access.
- `sessions.rs` slot registry — capacity bound + acquire/release balance.

**Status: shipped.** Four models in `tests/loom_models.rs`, `#![cfg(loom)]`-gated
so normal `cargo test` never runs them, wired through the `src/loom_sync.rs`
shim so production stays byte-identical. See `loom.md`.

**Limits.** Cannot model async / tokio primitives or time, so it cannot reach
`slot_tuner.rs` (tokio `Semaphore`) or `heartbeat.rs` (tokio `mpsc`). Under a
global `RUSTFLAGS="--cfg loom"`, tokio compiles in loom-mode (its `net` module
is gated out), so any dependency that uses `tokio::net` — `tokio-postgres`
(the `db` feature) and `testcontainers`→`hyper-util` (a dev-dep) — fails to
compile. The loom target therefore builds with `--no-default-features` and
gates testcontainers to `cfg(not(loom))` (see `loom.md`).

## Shuttle (aws/shuttle) — recommended fast-follow

**What it is.** A randomized concurrency-testing library from AWS with the same
shape of API as loom (drop-in `shuttle::sync`, `shuttle::thread`). Instead of
loom's *exhaustive* search it uses **randomized scheduling with probabilistic
concurrency testing (PCT)**, which trades loom's completeness for the ability to
**scale to much larger executions** — more threads, more operations, longer
runs — and, crucially for harvest, **it models async / futures execution**.

**Why it's the strongest complement.** The one module loom provably cannot
reach is `slot_tuner.rs` (issue #548): its withheld-permit accounting is built
on `tokio::sync::Semaphore` and `OwnedSemaphorePermit`, and the invariant worth
checking —

> `withheld_permits + available_permits + in_flight == max_slots`

across concurrent grow/shrink/dispatch/return — is exactly an async-semaphore
property. Shuttle can model an async semaphore; loom cannot. `heartbeat.rs`
(tokio `mpsc` flush ordering) is a second Shuttle-shaped candidate.

**Shared cost is low.** Because Shuttle mirrors loom's API, the same
`cfg`-aliased shim pattern (`loom_sync.rs`) extends to it with a third arm
(`#[cfg(shuttle)] use shuttle::sync::...`), so adoption does not fork the
codebase.

**PoC status — backlogged, not shipped (deliberately).** The task allowed
shipping a *small, genuinely-running* Shuttle PoC on the slot-tuner **accounting
algorithm** (explicitly an algorithm model, not the real `tokio`-typed code).
It is **not** shipped in this PR, for two honest reasons:

1. The real `slot_tuner.rs` accounting is entangled with `tokio::sync`
   `OwnedSemaphorePermit` ownership semantics; a faithful PoC would either model
   the algorithm abstractly (risking "passes but doesn't mirror the real code")
   or require refactoring `slot_tuner.rs` to route its semaphore through a shim —
   a larger change than this bootstrapping PR should carry.
2. The guidance was explicit: **do not ship a Shuttle test that doesn't run.**
   Rather than commit a non-running or misleading-abstraction PoC, this is filed
   as a concrete fast-follow with the sketch below.

**Concrete fast-follow sketch.**

```toml
[target.'cfg(shuttle)'.dependencies]
shuttle = "0.8"
```

```rust
// src/loom_sync.rs gains a third arm:
#[cfg(shuttle)]  pub(crate) use shuttle::sync::{Arc, Mutex, MutexGuard};

// slot_tuner.rs routes its Semaphore through the shim under cfg(shuttle),
// then tests/shuttle_models.rs:
#![cfg(shuttle)]
#[test]
fn slot_accounting_conserves_permits() {
    shuttle::check_random(|| {
        // spawn concurrent grow / shrink / dispatch(acquire) / return(release)
        // over a TunedSlotRuntime; assert after quiescence:
        //   withheld + available + in_flight == max
    }, 10_000 /* iterations */);
}
```

**Recommendation: adopt as a fast-follow**, prioritizing the `slot_tuner.rs`
accounting invariant that loom structurally cannot express.

## Turmoil (tokio-rs/turmoil) — recommend against (poor fit)

**What it is.** A **network** simulation harness. It intercepts `tokio::net`
(TCP/UDP) so a test can run many simulated hosts in one process, inject
partitions and latency, and deterministically test **distributed protocols
between peers that talk to each other over sockets** (gossip, Raft, custom
replication).

**Why it's a poor fit for harvest — with evidence.** Harvest nodes do **not**
talk to each other over custom peer sockets. Workers and schedulers coordinate
**exclusively through Postgres**; the only "network" is the client→Postgres
connection, which Turmoil cannot simulate (it intercepts `tokio::net`, not a
Postgres *server*). A repo search confirms there is no peer-to-peer networking
to simulate:

- No `TcpListener` / `UdpSocket` / custom `tokio::net` server in
  `autumn-harvest/src` or `autumn-harvest-plugin/src`. (The only `TcpStream` /
  `UdpSocket` string matches in the repo are forbidden-API *pattern literals* in
  `det_check.rs`'s determinism deny-list — not a running server — so a future
  grep hitting them is not a contradiction of this point.)
- The only `std::net` usage is `completion_callback.rs`'s SSRF IP-literal
  validation (`IpAddr`/`Ipv4Addr`/`Ipv6Addr` parsing), which is not networking.
- No `gossip` / `raft` / peer-replication module; no `turmoil`/`quinn` in any
  manifest.

The management API is HTTP, but it is a request/response surface exercised by
the plugin's HTTP integration tests, not a peer protocol whose partition
behavior needs simulating.

**Recommendation: do not adopt.** There is no distributed peer protocol for
Turmoil to model; its capability doesn't intersect harvest's architecture. If a
future feature introduces genuine worker-to-worker networking (it does not exist
today), revisit.

## Recommendation matrix

| Tool | What it models | Coverage of harvest's concurrency **here** | Decision |
|------|----------------|--------------------------------------------|----------|
| **loom** | In-process locks/atomics, exhaustive interleavings | `circuit_breaker` generation fence + single probe; `sessions` slot bound/balance. Cannot reach async (`slot_tuner`, `heartbeat`) or any Postgres-coordinated race. | **Adopt now** (shipped in this PR) |
| **Shuttle** | In-process locks **+ async/futures**, randomized PCT (scales past loom) | Everything loom reaches, **plus** `slot_tuner.rs` semaphore accounting and `heartbeat.rs` mpsc ordering that loom structurally cannot. Still cannot model Postgres. | **Fast-follow** (sketch above; not shipped to avoid a non-running PoC) |
| **Turmoil** | Simulated peer TCP/UDP networks, partitions/latency | ~none — harvest has no custom peer networking; it coordinates through Postgres, which Turmoil cannot simulate. | **No** |

**Bottom line.** loom now, Shuttle next (for the async slot-tuner invariant),
Turmoil not at all. And none of the three substitutes for the Docker-backed
integration tests that exercise harvest's Postgres-coordinated concurrency — the
bulk of the real surface.
