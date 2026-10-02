# Concurrency model-checking: tool evaluation (loom / Shuttle / Turmoil)

This note records an honest evaluation of three model-checking / simulation
tools for harvest, and the resulting adoption decisions. The companions
[`loom.md`](loom.md) and [`shuttle.md`](shuttle.md) document the two tools in
use.

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
shim so production stays byte-identical. The `loom` job in
`.github/workflows/ci.yml` runs them on every PR (issue #1800). See `loom.md`.

**Limits.** Cannot model async / tokio primitives or time, so it cannot reach
`slot_tuner.rs` (tokio `Semaphore`) or `heartbeat.rs` (tokio `mpsc`). Under a
global `RUSTFLAGS="--cfg loom"`, tokio compiles in loom-mode (its `net` module
is gated out), so any dependency that uses `tokio::net` — `tokio-postgres`
(the `db` feature) and `testcontainers`→`hyper-util` (a dev-dep) — fails to
compile. The loom target therefore builds with `--no-default-features` and
gates testcontainers to `cfg(not(loom))` (see `loom.md`).

## Shuttle (awslabs/shuttle) — adopted (issue #1800)

**What it is.** A randomized concurrency-testing library from AWS with the same
shape of API as loom. Instead of loom's *exhaustive* search it uses
**randomized scheduling and probabilistic concurrency testing (PCT)**. That
trades completeness for scale, and, crucially for harvest, **it models async
tasks and tokio-style primitives** through `shuttle-tokio`.

**Why it complements loom.** loom cannot reach `slot_tuner.rs` (issue #548).
Its withheld-permit accounting is built on `tokio::sync::Semaphore` and
`OwnedSemaphorePermit`. `heartbeat.rs` drains a `tokio::sync::mpsc` channel.
Both are async-runtime properties.

**Status: shipped.** Three models in `tests/shuttle_models.rs`, each under the
random and the PCT scheduler (six tests). They drive the real code through
the `src/shuttle_sync.rs` shim:

- `slot_tuner_conserves_permits_*`: `withheld + live_target == max_slots`,
  dispatch never above the live target, and a full drain after
  `release_all_withheld`.
- `heartbeat_flush_keeps_send_order_*`: flushes keep send order and the newest
  heartbeat is flushed.
- `heartbeat_lease_lost_stops_the_flusher_*`: a lost lease (issue #1789)
  cancels the activity and stops the flusher.

The `shuttle` job in `.github/workflows/ci.yml` runs them on every PR. See
`shuttle.md`.

**Defects found.** The slot-tuner model found two defects in `resize_toward`.
Both are fixed in the same change:

1. A grow released withheld permits before it raised `live_target`. A dispatch
   task could take a released permit and run above the live target that
   readers saw. `release_all_withheld` had the same order.
2. A shrink could reach its target through `try_acquire` while an older
   background shrink still waited in the semaphore queue. That task then took
   a free permit and held it until the next tuner tick.

**Limits.** Shuttle samples schedules. A pass is strong evidence, not a proof.
It does not model time (`sleep` is one yield) or Postgres.

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
| **loom** | In-process locks/atomics, exhaustive interleavings | `circuit_breaker` generation fence + single probe; `sessions` slot bound/balance. Cannot reach async (`slot_tuner`, `heartbeat`) or any Postgres-coordinated race. | **Adopted** (runs on every PR since issue #1800) |
| **Shuttle** | In-process locks **+ async/futures**, randomized PCT (scales past loom) | Everything loom reaches, **plus** `slot_tuner.rs` semaphore accounting and `heartbeat.rs` mpsc ordering that loom structurally cannot. Still cannot model Postgres. | **Adopted** (issue #1800; runs on every PR) |
| **Turmoil** | Simulated peer TCP/UDP networks, partitions/latency | ~none — harvest has no custom peer networking; it coordinates through Postgres, which Turmoil cannot simulate. | **No** |

**Bottom line.** loom for in-process locks, Shuttle for async primitives,
Turmoil not at all. Both loom and Shuttle run on every PR. And none of the three substitutes for the Docker-backed
integration tests that exercise harvest's Postgres-coordinated concurrency — the
bulk of the real surface.
