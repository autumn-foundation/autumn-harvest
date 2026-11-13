## Every database gauge sampler runs on one interval (follow-up to issue #2668)

PR #2671 moved two population samplers off the 500 ms poll interval. Seven
more gauge samplers still read the database on each poll tick of each worker.
A replay of poll ticks measured them:

- The stranded-work demand query had a plan defect. The planner scanned every
  execution for each pending row. At 100,000 pending rows one call ran for
  more than 300 s. Its `PAUSED` gate is now a `NOT EXISTS` probe. The same
  call reads 114,965 buffers in 0.23 s and returns the same groups. The
  plugin shard-health route runs this query too.
- At 1,000,000 dead letters the DLQ depth count read 744,158 buffers per
  call. At 300,000 pending rows the concurrency stats read 20,230.

Changes:

- `WorkerConfig::population_sample_interval` from PR #2671 is now
  `WorkerConfig::gauge_sample_interval`, with
  `builder::DEFAULT_GAUGE_SAMPLE_INTERVAL` (15 s) and
  `with_gauge_sample_interval`. The knob did not ship under its old name.
- `spawn_gauge_samplers` starts each sampler that reads the database for a
  gauge on that interval: queue depth and age, concurrency, rate limit, DLQ
  depth, queue pause, stranded work, history oversized and active workflows.
  At the default poll interval that is 30 times fewer calls.
- The schedule-overdue sampler keeps its adaptive cadence. The replication
  sampler keeps its own knob. The DB-pool, worker-slot and dispatch samplers
  read no table, so they keep the poll interval.
- The DEBUG trace `concurrency cap saturated` fires once per gauge interval.
- `GET /api/harvest/admin/config` reports `worker.gauge_sample_interval_ms`.
- The telemetry reference now lists the concurrency and rate-limit gauges.

No migration. No new `WorkflowEvent` variant. No new metric. Design record:
`docs/design/DESIGN-gauge-sample-interval.md`. Ledger page:
`docs/performance-gauge-sample-interval.md`.
