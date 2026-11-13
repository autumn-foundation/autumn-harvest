# Two population samplers ran on the poll interval

This page records issue #2668. Two worker samplers count the whole active
population of each shard. They ran on `poll_interval`, 500 ms by default, on
every worker. The fix gives them their own interval, 15 s by default.

## 🎯 Workload

| sampler | function | statement |
|:--|:--|:--|
| `spawn_history_oversized_sampler` (#493) | `sample_history_oversized_counts` | `COUNT(*)` of `harvest_events` per RUNNING run, over `continue_as_new_threshold` |
| `spawn_workflow_active_sampler` (#770) | `sample_active_workflow_counts` | `COUNT(*) GROUP BY workflow_name, state` over RUNNING and PAUSED runs |

The samplers run only when a metrics recorder is set. Issue #2668 seeded
`harvest_workflow_executions` at 100k, 300k and 1M rows: 96% terminal, 3.5%
RUNNING, 0.5% PAUSED. Two real workers ran 200 workflows of 3 activities
each. PG 16, `pg_stat_statements`, default `shared_buffers`.

## 📈 Profile

| executions | total buffers | oversized sampler | active sampler | both |
|--:|--:|--:|--:|--:|
| 100k | 754,897 | 208,888 | 51,092 | 34.5% |
| 300k | 1,183,672 | 516,369 | 152,740 | 56.5% |
| 1M | 2,548,217 | 1,502,995 | 456,640 | 76.9% |

At 1M executions, with 13.6M events on the active runs, one call costs:

- `sample_history_oversized_counts`: 164,171 buffers.
- `sample_active_workflow_counts`: 26,039 buffers.

## 💡 Hypothesis

The cost per call is inherent to the count. The call rate is the waste.
Prometheus scrapes a gauge each 15 to 60 s. A sample at 500 ms is almost
always discarded.

The issue records two rejected rewrites. An early-exit oversized check costs
74% more. A covering partial index is worse than no index before a vacuum.

## 🔧 Change

- `WorkerConfig::population_sample_interval`, default 15 s, drives both
  samplers. Every other sampler keeps `poll_interval`.
- The gauge semantics do not change. Each worker still emits the fleet-wide
  value.
- The oversized predicate names `RUNNING` only. `SUSPENDED` is not a stored
  state.

`docs/design/DESIGN-2668.md` records the options and the decision.

## 📊 Measurement

The change does not alter the cost per call. It alters the call count.

| | interval | calls per worker per hour, each sampler |
|:--|--:|--:|
| Before | 500 ms | 7,200 |
| After | 15 s | 240 |

That is 30 times fewer calls. At 1M executions the buffer touches per worker
per hour go from about 1.37 billion to about 46 million.

`population_samplers_wait_for_the_population_interval_not_the_poll_interval`
in `autumn-harvest/src/worker.rs` pins the after state. It counts real
loopback connections. With a 50 ms poll interval, both samplers open no
connection for 15 s, then open one. The test fails when the samplers use
`poll_interval`.

## 🔬 Reproduce

```sh
cargo test -p autumn-harvest --lib -- worker::tests::population_samplers
```

The fixture SQL and the worker-driven harness are in issue #2668.

## Reference

- Issue [#2668](https://github.com/autumn-foundation/autumn-harvest/issues/2668).
- `docs/performance-metrics-sampler-guard.md` (issue #1428) added the
  `is_enabled()` guard to the oversized sampler.
