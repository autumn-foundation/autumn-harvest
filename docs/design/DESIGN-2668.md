# Design — Issue #2668: population samplers run on the poll interval

> **Superseded in part.** `docs/design/DESIGN-gauge-sample-interval.md`
> renamed the knob to `gauge_sample_interval` before its release. It also moved
> every other database gauge sampler onto that interval. The names below are the
> names of this record.

Two worker samplers count the whole active population of each shard. They
run on `poll_interval`, 500 ms by default, on every worker. At 1M
executions they use 77% of the buffers of the workload.

| sampler | statement | buffers per call at 1M |
|:--|:--|--:|
| `spawn_history_oversized_sampler` (#493) | `sample_history_oversized_counts` | 164,171 |
| `spawn_workflow_active_sampler` (#770) | `sample_active_workflow_counts` | 26,039 |

The count sets the cost per call. The waste is the call rate. Prometheus
scrapes a gauge each 15 to 60 s, so no scrape reads most samples.

**No migration. No new `WorkflowEvent` variant. No route change. No new
metric.**

---

## 0. Planning record

### 0.1 Brainstorm — how can the call rate go down?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Option A of the issue: a separate sample interval, 15 s by default. | **Adopted.** 30 times fewer calls. No metric semantics change. |
| B2 | Option B of the issue: elect one sampler per shard through `scanner_lease`. | Rejected for now. See §0.3, black hat. |
| B3 | Adapt the interval to the scrape cadence, as `next_overdue_sample_interval` does for schedules. | Rejected. The worker does not know the scrape cadence. |
| B4 | Share one sample between the workers of one process. | Rejected. Most deployments run one worker per process. |
| B5 | Keep counts in a projection table that a trigger writes. | Rejected. It adds a write to the hot path and a migration. |
| B6 | A covering partial index for the active sampler. | Rejected by the issue. It is worse than no index before a vacuum. |
| B7 | An early-exit rewrite of the oversized check. | Rejected by the issue. It costs 74% more. |
| B8 | Move every `poll_interval` sampler to the new interval. | Deferred. The others were not measured. Some feed alerts that need a fresh value. |

### 0.2 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Set the interval to zero. The sampler then spins and sends SQL in a hot loop. | `HarvestBuilder::build` rejects zero. `WorkerRuntimeConfig::validate` also rejects it, so a config that skips the builder fails in `Worker::new`. |
| R2 | A later change passes `poll_interval` to the samplers again. | A test drives the samplers from a runtime config and counts real pool connections. Zero connections before the population interval ends. |
| R3 | A long sleep delays shutdown. | The sleep selects on the cancel token. A test cancels in the sleep and the task ends. |
| R4 | The new knob is not visible to an operator. | `WorkerConfigView` (`GET /api/harvest/admin/config`, key `worker.population_sample_interval_ms`) reports it. The exhaustive destructure forces this. |
| R5 | A dashboard or alert needs a value faster than 15 s. | The starter alert reads `harvest_workflow_active` over one hour. The history-ceiling runbook alerts on `harvest_workflow_history_oversized > 0`. Both are coarse. An operator can lower the interval. |
| R6 | A query across workers changes. | Option A keeps one emitter per worker, so `sum`, `max` and `last` give the same value as before. Use `max`: each worker emits the fleet-wide value. |
| R7 | The docs still say "poll interval". | The telemetry table and the history-ceiling runbook change in the same commit. |
| R8 | The name suggests that the knob governs every metric sampler. | The name is `population_sample_interval`. It governs the two population samplers only. |

### 0.3 Six thinking hats

- **White (facts).** The issue measured 76.9% of buffers at 1M executions.
  The statements are #1 and #2 by buffers. Each worker computes the same
  fleet-wide value. The first sample comes one interval after start.
- **Red (feelings).** A gauge that refreshes each 15 s feels normal. It
  matches the 15 s scrape interval of the Prometheus example config and
  `replication_sample_interval`.
- **Black (risks).** Option B changes who emits the gauge. The starter alert
  `harvest_workflow_active` sums over workers. A single emitter changes its
  value and its threshold. Option B also needs a lease per shard while
  each worker reads every shard. That is a larger change with its own
  failure modes. A first sample 15 s after start leaves the gauge empty for
  that time.
- **Yellow (benefits).** Option A cuts the calls by 30 times at the default
  poll interval. It needs no lease, no index and no migration. It keeps the
  gauge semantics.
- **Green (ideas).** A follow-up can apply the same interval to the queue,
  DLQ, rate-limit and concurrency samplers after a measurement (B8).
- **Blue (process).** Write tests first. Add the knob. Wire it. Fix the docs.
  Record the change in a changelog fragment.

### 0.4 Decision

Adopt Option A only. Add `WorkerConfig::population_sample_interval`, default
`DEFAULT_POPULATION_SAMPLE_INTERVAL` (15 s). The two population samplers
sleep for this interval. Every other sampler keeps `poll_interval`.

Also remove `'SUSPENDED'` from the oversized predicate. No row stores that
state (see `replay_sample.rs`). The result does not change.

## 1. Acceptance criteria

1. `WorkerConfig::default().population_sample_interval` is 15 s.
2. `with_population_sample_interval` sets it, and the runtime config copies it.
3. `HarvestBuilder::build` rejects a zero interval.
4. `WorkerRuntimeConfig::validate` rejects a zero interval.
5. With a 50 ms poll interval and the default population interval, the two
   samplers open no connection for 15 s, then open one.
6. A cancel during the sleep ends both samplers.
7. `WorkerConfigView` reports `population_sample_interval_ms`.
8. `docs/telemetry.md` and `docs/runbooks/history-ceiling.md` name the new
   cadence.
