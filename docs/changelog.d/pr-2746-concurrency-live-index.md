## Concurrency gauges read only the live keyed rows (issue #2678, PR #2746)

`queue::concurrency_key_stats` feeds the gauges `harvest.concurrency.in_flight`
and `harvest.concurrency.deferred`. No index held its `PENDING` rows with a
key. So each call read the whole task queue: 20,230 buffers at 300,000
pending rows.

- `20261119233226` builds `idx_harvest_tq_concurrency_live` on
  `harvest_task_queue (concurrency_key, task_type)`. Its predicate holds only
  the `PENDING` and `RUNNING` rows with a key and a cap. The migration uses
  the guarded plain build (`SHARE` on `harvest_task_queue`, `lock_timeout`
  5 s). Its header gives the out-of-band `CONCURRENTLY` recipe. The upgrade
  guide has a row for it.
- Four statements use the index: the gauge read, the admin attribution read
  `concurrency_key_workflows`, the `concurrency_pending_keys` step of a
  fallback claim, and the operator cap raise `raise_waiting_concurrency_caps`
  (635,675 to 3,422 buffers on the large fixture). The claim cap re-check has no `concurrency_cap IS NOT NULL`
  filter, so it keeps `harvest_task_queue_concurrency_key_running`.
- Read cost per gauge call falls by 93% at each size: 672 to 44, 6,794 to
  499 and 20,230 to 1,473 buffers at 10,000, 100,000 and 300,000 pending
  rows. See `docs/performance-concurrency-live-index.md`.
- Write cost: a keyed enqueue and a keyed claim each add one WAL record, 79
  to 92 bytes. One keyed task writes 4.8% more WAL. A completion and every write of a row with no key do not change.
  The two operator cap rewrites are no longer HOT updates, because the
  predicate names `concurrency_cap`.
- Gates: `concurrency_live_index_tests` plans the gauge and attribution reads
  and bounds the buffers of the gauge read and of the pending-keys step. Each
  gate failed before the migration and passes after it. A group test pins
  the gauge results.
- The write harness found that a keyed claim read every `RUNNING` row. This
  change does not touch that cost. Issue #2744 tracked it, and PR #2761 fixed
  it.
- No query text changes. No new `WorkflowEvent` variant. No column changes.
