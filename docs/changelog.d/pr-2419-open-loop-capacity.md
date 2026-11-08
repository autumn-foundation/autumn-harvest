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
  1.25x. Each step lasts 20 s and drains to zero before the next one.
  Latency runs from the scheduled arrival to `completed_at`, on one clock.
- The sustainable rate is the last step before the first step that is not
  sustainable. A step fails on a growing backlog, a growing p99, an
  unfinished or failed run, or a generator that missed its rate. The rule
  is pure and unit-tested in `capacity_bench_support.rs`.
- Each shard runs 4 worker processes (`HARVEST_BENCH_WORKER_PROCESSES`).
  Each process is the bench binary again, with its own runtime and pool.
- Each database cell prints its host layout. A cell whose shards share a
  host says that it is not a scaling result. The run's shard-scaling section
  prints a ratio only for sound cells with one database host per shard.
- `docs/benchmarks/capacity-2026-11.md` publishes one measured run on the
  reference box. `docs/benchmarks.md` states that the closed-loop cell is
  latency-bound and that co-located shard cells are not a scaling result.
  `docs/comparison.md` cites the DBOS figure next to the Harvest number,
  with the hardware of each.

No migration. No new `WorkflowEvent` variant. No engine change.

Tests: 29 unit tests for the rule, the ladder, the child codec and the
scaling claim; 4 host-layout tests; 3 `benchmarks_docs` guards; 4
`capacity_docs` guards.
