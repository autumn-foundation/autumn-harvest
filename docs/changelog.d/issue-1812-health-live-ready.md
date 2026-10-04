## Feature — liveness and readiness probes with a draining state (issue #1812)

Harvest now has `GET /health/live` and `GET /health/ready`. Both are
`PublicSafe`.

- `/health/live` always returns `200`. It reads in-memory flags and does no
  I/O.
- `/health/ready` returns `503` in four cases. The runtime has not started.
  The replica drains. The default shard does not answer `SELECT 1`. Under
  `require_shard_readiness`, the shard verdict is not `ready`. The body names
  each failure in `reasons`.

`GET /health` does not change. It still returns `200` before the runtime
starts and during a drain.

Design decisions:

- The draining flag is an `AtomicBool` on `HarvestApiState`.
  `begin_draining()` sets it. Each runtime start clears it. `install()` and
  `clear()` keep it.
- On the `HarvestPlugin` path, the state also reads the autumn-web probe
  state. autumn-web marks that state at SIGTERM. It closes the listener
  before it runs the shutdown hook, so the hook alone is too late.
- `HarvestEmbeddingRuntime::stop` sets the flag first. Readiness therefore
  fails before the worker stops in-flight work.
- The database checks share one 1 second budget. The pool checkout counts
  against it.
- The database result is cached for 1 second, with one check at a time. The
  route is public, so this limits its database load.
- The body carries the shard verdict only. The full report stays on the
  admin-gated `GET /admin/shards/health`.
- Without `require_shard_readiness`, the probe reads only the default shard.
  One bad non-default shard does not remove every replica from the load
  balancer.
- A remote worker drain (`POST /workers/{id}/drain`) does not change
  readiness.

New doc: `docs/operations/kubernetes-probes.md`. It covers the probes,
`preStop` and `terminationGracePeriodSeconds`. The `docs/security-posture.md`
auth-bypass example now matches the nest-stripped path.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not
touched.

Test evidence:

- `health_probe_tests` in `autumn-harvest-plugin/src/api.rs` (no database).
- `ready_probe_*` in `autumn-harvest-plugin/tests/shard_health_integration.rs`.
- `stop_drops_readiness_before_in_flight_work_stops` in
  `autumn-harvest-plugin/tests/standalone_embedding.rs`. It holds a workflow
  in flight and sees `503` with `draining` while `stop()` waits.
- `stop_harvest_runtime_sets_the_drain` in `autumn-harvest-plugin/src/plugin.rs`.
- `standalone_token_mode_preserves_the_probe_routes` in
  `autumn-harvest-plugin/tests/standalone_admin_auth.rs`.
