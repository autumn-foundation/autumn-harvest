## Feature — liveness and readiness probes with a draining state (issue #1812)

Harvest now has `GET /health/live` and `GET /health/ready`. Both are
`PublicSafe`.

- `/health/live` always returns `200`. It reads one in-memory flag and does
  no I/O.
- `/health/ready` returns `503` before the runtime starts, while the replica
  drains, when the default shard does not answer `SELECT 1` within 1 second,
  and, under `require_shard_readiness`, when the shard report is not `ready`.
  The body names each failure in `reasons`.

`GET /health` does not change. It still returns `200` before the runtime
starts.

Design decisions:

- The draining flag is an `AtomicBool` on `HarvestApiState`.
  `begin_draining()` sets it. `install()` clears it, so a restart becomes
  ready again. `clear()` keeps it.
- `HarvestPlugin` shutdown and `HarvestEmbeddingRuntime::stop` set the flag
  first. Readiness therefore fails before the worker stops in-flight work.
- The probe reads only the default shard. One bad non-default shard does not
  remove every replica from the load balancer. `require_shard_readiness`
  still gates on every writable shard.
- A failed runtime or drain check skips the database checks.
- A remote worker drain (`POST /workers/{id}/drain`) does not change
  readiness.

New doc: `docs/operations/kubernetes-probes.md`. It covers the probes,
`preStop` and `terminationGracePeriodSeconds`.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not
touched.

Test evidence: `health_probe_tests` in `autumn-harvest-plugin/src/api.rs`
(no database), and `ready_probe_*` in
`autumn-harvest-plugin/tests/shard_health_integration.rs` (Postgres).
