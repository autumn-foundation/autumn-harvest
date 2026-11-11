## The heartbeat task-stats prune reads only rows past the retention

Each heartbeat of each worker runs `workers::prune_worker_task_stats` (issue
#1815). The threshold of each row comes from the `peer_stale_secs` in its
stored cohort key. So no index could serve the predicate, and each prune
read, decompressed and parsed every key in `harvest_worker_task_stats`. The
cost of each heartbeat grew with the fleet.

- The prune now also states `updated_at < NOW() - retention`. The threshold
  of a row is never shorter than the retention, so the prune deletes the same
  rows. `harvest_worker_task_stats_updated_at_idx` serves the new predicate.
- Measured with a worker-driven harness,
  `tests/integration/worker_task_stats_prune_perf.rs`: four real workers at a
  5 s heartbeat beside a seeded fleet with orphan rows, slow cohorts and dead
  tuples. Prune buffers fall 97.5%, 98.3% and 98.2% for live fleets of 500,
  2,000 and 8,000 workers. Before, the prune was 42.0%, 73.6% and 91.7% of
  all buffers of the workers. Rows read per prune fall from 10,467 to 65 at
  8,000. See `docs/performance-worker-task-stats-prune.md`.
- The surviving rows have the same md5 before and after at each size. A
  structural test asserts the kept rows of 19 edge rows with two fallbacks.
- A stored key that is not JSON now fails the prune only when its row is past
  the retention. Before, it failed every prune.
- No migration, no index, no new `WorkflowEvent` variant, no schema change.
