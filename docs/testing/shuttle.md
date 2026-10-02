# Shuttle concurrency model checking

[Shuttle](https://github.com/awslabs/shuttle) tests concurrent Rust under a
controlled scheduler. Unlike loom, it models async tasks and tokio-style
primitives. This repo uses it for two modules that loom cannot reach:
`slot_tuner.rs` (tokio `Semaphore`) and `heartbeat.rs` (tokio `mpsc`).

See [`concurrency-model-checking.md`](concurrency-model-checking.md) for the
tool evaluation, and [`loom.md`](loom.md) for the loom models.

## Running the models

```bash
# From the repo root. No database is necessary.
RUSTFLAGS="--cfg shuttle" cargo test -p autumn-harvest --no-default-features --test shuttle_models --release
```

A normal `cargo test` never runs these models. `tests/shuttle_models.rs` has
the gate `#![cfg(shuttle)]`, so without the flag it compiles to an empty
crate. The `shuttle` job in `.github/workflows/ci.yml` runs them on every PR.

Use `--no-default-features`. Under `--cfg shuttle`, `TunedSlotRuntime` and the
heartbeat core take Shuttle types. `worker.rs` (the `db` feature) passes tokio
types, so a `db` build does not compile under `--cfg shuttle`.

## What Shuttle checks, and what it does not

- Loom explores every schedule. Shuttle samples them, so a pass is strong
  evidence, not a proof.
- Each model runs under two schedulers. The random scheduler picks a random
  task at each step. The PCT scheduler finds bugs of small depth with a
  known probability.
- Shuttle reports a deadlock (all tasks blocked) and a run that does not end
  within its step limit as a failure. The models use this to check liveness.
- Shuttle does not model time. `sleep` yields and then completes, so a timer
  tick is one scheduling point.
- Shuttle does not model Postgres. The heartbeat models replace the database
  write with a recording sink.

## The `cfg(shuttle)` shim (`src/shuttle_sync.rs`)

A normal build re-exports tokio and tokio-util. Under `--cfg shuttle` the
same names resolve to `shuttle-tokio` and `shuttle-tokio-util`:

| Name | Normal build | `--cfg shuttle` |
|------|--------------|-----------------|
| `Semaphore`, `OwnedSemaphorePermit`, `mpsc` | `tokio::sync` | `shuttle_tokio::sync` |
| `spawn`, `JoinHandle` | `tokio::task` | `shuttle_tokio::task` |
| `sleep` | `tokio::time` | `shuttle_tokio::time` |
| `select!` | `tokio` | `shuttle_tokio` |
| `CancellationToken` | `tokio_util::sync` | `shuttle_tokio_util::sync` |

Only `slot_tuner.rs` and `heartbeat.rs` import from the shim. A normal build
is unchanged. The Shuttle crates are `[target.'cfg(shuttle)'.dependencies]`,
so a normal build does not fetch or compile them. They are target
dependencies, not dev-dependencies, because the shim names them from the
library sources.

`select!` goes through the shim because tokio's `select!` picks a branch with
its own random number generator. Shuttle cannot replay a schedule that
depends on a generator it does not control.

## The models (`tests/shuttle_models.rs`)

Each model is one function. Two `#[test]`s run it: one with
`shuttle::check_random` and one with `shuttle::check_pct` (depth 3).

1. **`slot_tuner_conserves_permits_*`.** Two dispatch tasks acquire and
   release permits. In parallel, the tuner grows, shrinks, grows and shrinks
   the live target. The model checks:
   - `withheld_permits() + live_target() == max_slots` after each resize;
   - dispatch never holds more permits than the live target;
   - after the race, the tuner settles on its target;
   - with no further tuner call, the free permit count then reaches the
     target. A permit that a stray background shrink keeps stops it short;
   - after `release_all_withheld`, a drain of all permits completes.
2. **`heartbeat_flush_keeps_send_order_*`.** An activity sends five numbered
   heartbeats into a channel of capacity two. The flusher drains it in
   parallel. The model checks that flushes keep send order, that the newest
   heartbeat is flushed, and that the flusher stops on cancellation.
3. **`heartbeat_lease_lost_stops_the_flusher_*`.** The sink reports a lost
   lease (issue #1789) on the first flush. The model checks that the flusher
   cancels the activity token and flushes nothing more.

## Defects found

The slot-tuner model found two defects in `TunedSlotRuntime::resize_toward`.
Both are fixed in the change that added the model (issue #1800).

1. **Grow order.** A grow released the withheld permits, then raised
   `live_target`. A dispatch task could take a released permit before the
   raise. It then ran above the live target that the occupancy sampler and
   other readers saw. `release_all_withheld` had the same order. The fix
   raises `live_target` first. The semaphore release orders that store
   before the dispatch task's acquire.
2. **Stray background shrink.** A shrink can reach its target through
   `try_acquire` while an older background shrink still waits in the
   semaphore queue. That task then takes a free permit and holds it until the
   next tuner tick. Dispatch loses one slot, and the next tick counts one
   extra slot as in use. The fix settles the background task a second time,
   after the `try_acquire` loop.

Revert either fix and the model fails. The random scheduler catches both. The
PCT scheduler catches the first. The tokio unit test
`resize_toward_cancels_a_stray_background_shrink_once_the_target_lands` also
catches the second, without Shuttle.

## The heartbeat seam

`heartbeat.rs` used to hold one loop that mixed the channel drain, the timer,
cancellation and the database write. The loop is now `run_heartbeat_flusher`,
which takes a `HeartbeatSink`. The database write is one sink
(`spawn_heartbeat_flusher` uses it). The models use a recording sink. The
loop code is the same in production and under Shuttle.

## Reproducing a failure

Shuttle prints a failing schedule. To replay it, pass it to
`shuttle::replay(model, "<schedule>")` in a scratch test, with the same
`--cfg shuttle` command.

## Adding a model

1. Confirm the target is in-process and async. For plain locks and atomics,
   prefer loom. It gives an exhaustive check.
2. Route the target's tokio types through `crate::shuttle_sync`.
3. Add one model function and two `#[test]`s (random and PCT) in
   `tests/shuttle_models.rs`.
4. Assert a property that holds in every schedule. Do not assert one order.
