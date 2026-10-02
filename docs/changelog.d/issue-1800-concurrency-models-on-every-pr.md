## Testing — loom and Shuttle model checking on every PR (issue #1800)

**CI.** The loom models used to run only on manual dispatch
(`.github/workflows/loom.yml`), and had never run. `ci.yml` now has two jobs,
`loom` ("Loom models") and `shuttle` ("Shuttle models"). Both run on every PR
and push, with the same draft and docs-only skips as `msrv`. `loom.yml` is
deleted. The guard `tests/integration/concurrency_model_ci.rs` fails if a job
stops running its target or loses its `--cfg`. Branch protection lists
required checks by name, so an admin must add the two job names there.

**Shuttle.** New `tests/shuttle_models.rs`, gated with `#![cfg(shuttle)]`.
Three models, each under the random and the PCT scheduler, drive the real code:

- `slot_tuner_conserves_permits_*`: `withheld + live_target == max_slots`,
  dispatch never above the live target, and a full drain after
  `release_all_withheld`.
- `heartbeat_flush_keeps_send_order_*`: flushes keep send order, and the
  newest heartbeat is flushed.
- `heartbeat_lease_lost_stops_the_flusher_*`: a lost lease (issue #1789)
  cancels the activity and stops the flusher.

New `src/shuttle_sync.rs` re-exports tokio and tokio-util under a normal
build, and `shuttle-tokio` / `shuttle-tokio-util` under `--cfg shuttle`. Only
`slot_tuner.rs` and `heartbeat.rs` use it. The Shuttle crates are
`[target.'cfg(shuttle)'.dependencies]`, so a normal build does not compile
them. `cfg(shuttle)` is in the workspace `check-cfg` list.

**Heartbeat seam.** `heartbeat.rs` no longer needs the `db` feature for its
loop. `run_heartbeat_flusher` drains the channel and writes through a
`HeartbeatSink`. The Postgres sink keeps the old log lines, the lease-lost
cancel and the retry-on-error behaviour. `spawn_heartbeat_flusher` has the
same signature.

**Defects fixed.** The slot-tuner model found two defects in
`TunedSlotRuntime::resize_toward`:

1. A grow released withheld permits before it raised `live_target`, so
   dispatch could run above the live target that readers saw.
   `release_all_withheld` had the same order. Both now raise the target first.
2. A shrink could reach its target through `try_acquire` while an older
   background shrink still waited in the queue. That task then held a free
   permit until the next tick. `resize_toward` now settles the background task
   again after the `try_acquire` loop. The tokio unit test
   `resize_toward_cancels_a_stray_background_shrink_once_the_target_lands`
   reproduces it without Shuttle.

New `TunedSlotRuntime::withheld_permits()` accessor. No new `WorkflowEvent`
variant, no migration.

**Docs.** New `docs/testing/shuttle.md`. `docs/testing/loom.md` and
`docs/testing/concurrency-model-checking.md` now describe both tools as
adopted and running in CI.
