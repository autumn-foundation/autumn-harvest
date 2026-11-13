# Design — every database gauge sampler runs on one sample interval

Issue #2668 moved two population samplers off `poll_interval`. Five more
gauge samplers still read the database on each poll tick of each worker:

| sampler | statement | gauge |
|:--|:--|:--|
| `spawn_queue_depth_sampler` | `bounded_queue_depths`, `oldest_pending_ages` | `harvest.queue.depth`, `harvest.queue.oldest_pending_age` |
| `spawn_concurrency_sampler` | `concurrency_key_stats` | `harvest.concurrency.in_flight`, `harvest.concurrency.deferred` |
| `spawn_rate_limit_sampler` | `sample_rate_limit_buckets` | `harvest.rate_limit.tokens_available`, `harvest.rate_limit.refill_rate` |
| `spawn_dlq_depth_sampler` | `dead_letter_count` | `harvest.dlq.entries` |
| `spawn_queue_pause_sampler` | `paused_queue_names` | `harvest.queue.paused` |
| `spawn_stranded_work_sampler` | `claimable_pending_demand_by_queue` | `harvest.shard.stranded_pending` |

Only gauge recorders read these values. Load shedding and shard health run
their own reads.

**No migration. No new `WorkflowEvent` variant. No route change. No new
metric.**

## 0. Measurement

`worker_sampler_cadence_perf` replays poll ticks of two workers.
`docs/performance-gauge-sample-interval.md` holds the numbers. In short:

- **The stranded-work query has a plan defect.** The planner estimates one
  row out of the queue-pause anti-join. It then runs a sequential scan of
  `harvest_workflow_executions` for each pending row. At 100,000 pending rows
  one call ran for more than 300 s. With a `NOT EXISTS` probe it reads
  121,068 buffers in 0.9 s and returns the same 740 groups.
- At 10,000 pending rows the stranded-work query was 80% of all buffers. The
  age query was the next cost.
- Every sampler runs on each 500 ms tick of each worker, and the
  fleet-wide samplers run once for each shard.

## 1. Planning record

### 1.1 Brainstorm — how can the cost go down?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Rewrite the `LEFT JOIN` on executions in the stranded-work query as a `NOT EXISTS` probe for a `PAUSED` row. | **Adopted.** It removes the quadratic plan. The age query already uses this form. |
| B2 | `ANALYZE` the two pause tables, so the planner sees zero rows. | Rejected. Autovacuum never analyzes an empty table that never changes. A dump and restore loses the statistics again. |
| B3 | `SET LOCAL enable_nestloop = off` around the query. | Rejected. It is blunt, and it hides the cause. |
| B4 | One sample interval for every sampler that reads the database. | **Adopted.** It cuts the calls by 30 times at the default poll interval. |
| B5 | A separate knob for each sampler. | Rejected. Nine knobs for one trade-off. |
| B6 | Keep `population_sample_interval` and add a second knob. | Rejected. Two knobs with the same default and the same meaning. |
| B7 | Rename `population_sample_interval` to `gauge_sample_interval`. | **Adopted.** The knob has not shipped, so the rename breaks no user. |
| B8 | Elect one sampler per shard through `scanner_lease`. | Deferred, as in DESIGN-2668. It changes who emits the gauge. |
| B9 | Cache one sample per process for all workers. | Rejected. Most deployments run one worker per process. |

### 1.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Slow a sampler that drives behaviour, not a gauge. | Each moved sampler only records gauges. Load shedding and the schedule-overdue sampler keep their own cadence. |
| R2 | Slow a gauge that an alert needs within seconds. | The alert pack reads these gauges over windows of 10 minutes or more. |
| R3 | An integration test waits for a gauge with a short timeout. | Those tests set `gauge_sample_interval` to their poll interval. |
| R4 | The rewrite changes which rows count as demand. | A database test compares the new query with the old one on edge cases. |
| R5 | A later change wires a sampler to `poll_interval` again. | One helper spawns every database gauge sampler. A test counts real connections through it. |
| R6 | The DEBUG saturation trace of the concurrency sampler goes quiet. | It now fires once per sample interval. The change log says so. |
| R7 | A zero interval spins every sampler. | The builder and `WorkerRuntimeConfig::validate` reject zero, as before. |
| R8 | A sampler without a database read moves too. | The DB-pool, worker-slot and dispatch samplers read no table, so they keep `poll_interval`. |

### 1.3 Six thinking hats

- **White (facts).** See §0. The stranded-work query is also the shard-health
  coverage read of the plugin, so the plan fix helps that route too.
- **Red (feelings).** Operators expect a gauge to refresh at about the scrape
  interval. 15 s meets that.
- **Black (risks).** The rename touches each struct literal again. The
  stranded-work gauge reacts 15 s later to lost coverage. Its alert has a
  longer window, so this is acceptable.
- **Yellow (benefits).** One knob with one meaning. Thirty times fewer calls.
  One quadratic plan removed.
- **Green (ideas).** B8 stays open for a later change.
- **Blue (process).** Tests first, then the query fix, then the cadence and the
  rename, then the docs.

## 2. Decision

1. Rewrite the `PAUSED` gate of `claimable_pending_demand_query` as a
   `NOT EXISTS` probe.
2. Rename `population_sample_interval` to `gauge_sample_interval`, default
   `DEFAULT_GAUGE_SAMPLE_INTERVAL` (15 s).
3. `spawn_gauge_samplers` spawns every sampler that reads the database for a
   gauge, on that interval. The schedule-overdue and replication samplers
   keep their own cadence. Samplers that read no table keep `poll_interval`.

## 3. Acceptance criteria

1. The demand query has no `LEFT JOIN` on executions, and it returns the same
   rows as the old query on the edge cases.
2. With a 50 ms poll interval and a 3 s gauge interval, no gauge sampler opens
   a connection before 3 s. Each opens one after 3 s.
3. `WorkerConfigView` reports `gauge_sample_interval_ms`.
4. Zero is rejected by the builder and by `validate`.
5. The telemetry reference names the new cadence for each moved gauge.
