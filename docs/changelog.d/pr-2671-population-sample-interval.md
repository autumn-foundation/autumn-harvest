## Population samplers get their own interval (issue #2668, PR #2671)

Two worker samplers count the whole active population of each shard:
`harvest.workflow.history_oversized` and `harvest.workflow.active`. They ran
on `poll_interval`, 500 ms by default, on every worker. At 1M executions they
used 77% of the buffers of the workload.

- New knob `WorkerConfig::population_sample_interval`, default 15 s
  (`builder::DEFAULT_POPULATION_SAMPLE_INTERVAL`). Set it with
  `with_population_sample_interval`. At the default poll interval the two
  samplers now make 30 times fewer calls.
- Every other sampler keeps `poll_interval`.
- The gauge semantics do not change. Each worker still emits the fleet-wide
  value, so `max` and `last` queries read as before.
- The first sample comes one interval after the worker starts.
- `HarvestBuilder::build` and `WorkerRuntimeConfig::validate` reject a zero
  interval. A zero interval spins the samplers.
- `GET /api/harvest/admin/config` reports `worker.population_sample_interval_ms`.
- The oversized query no longer names `'SUSPENDED'`. No row stores that
  state, so the result does not change.

**Upgrade note.** `WorkerConfig` and `WorkerRuntimeConfig` have the new
public field `population_sample_interval`. A struct literal must set it. Use
`builder::DEFAULT_POPULATION_SAMPLE_INTERVAL` to keep the default.

No migration. No new `WorkflowEvent` variant. Design record:
`docs/design/DESIGN-2668.md`.
