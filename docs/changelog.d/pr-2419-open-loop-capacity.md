## Open-loop capacity per shard in `e2e_bench` (issue #2419)

The closed-loop `throughput` cell divides its in-flight population by the
workflow latency. It is a latency figure, not a capacity figure. Its shard
columns also share one 4-core host.

What shipped:

- Two opt-in arms of a new capacity scenario: `capacity_noop` (zero
  activities) and `capacity_canonical` (the 3-activity workflow).
  `HARVEST_BENCH_SCENARIOS=capacity` selects both. The default matrix is
  unchanged.
- An open loop offers a fixed start rate per shard and steps it up by
  1.25x. Each step lasts 20 s, then drains for up to 20 s. A run that does
  not finish in that time fails the step. Each arrival records its schedule
  and its admission, so a late generator is judged on its own and never
  hides a queue.
- The sustainable rate is the last step before the first step that is not
  sustainable. A step fails on a growing engine backlog, a growing median
  latency, a growing p99, or an unfinished or failed run. The rule is pure
  and unit-tested in `capacity_bench_support.rs`.
- Each shard runs 4 worker processes (`HARVEST_BENCH_WORKER_PROCESSES`).
  Each process is the bench binary again, with its own runtime and pool.
  A cell checks `max_connections` before it starts.
- Each database cell prints its host layout. A cell whose shards share a
  host says that it is not a scaling result. The run's shard-scaling section
  prints a ratio only for a sound cell with one database host and one worker
  host per shard.
- `docs/benchmarks/capacity-2026-11.md` publishes one measured run on the
  reference box. `docs/benchmarks.md` states that the closed-loop cell is
  latency-bound and that co-located shard cells are not a scaling result.
  `docs/comparison.md` cites the DBOS figure next to the Harvest number,
  with the hardware of each.

No migration. No new `WorkflowEvent` variant. No engine change.

Tests: unit tests for the rule, the ladder, the host layout, the child
codec and the scaling claim; `benchmarks_docs` and `capacity_docs` guards.
