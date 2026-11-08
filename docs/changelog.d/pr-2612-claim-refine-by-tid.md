## The claim window refines by TID (issue #2547, PR #2612)

The default claim reads its window rows again to apply every gate and to
lock one row. That refine read each row through `harvest_task_queue_pkey`.
On the e2e bench it was 84% of a claim, at about 4.2 buffers per row. The
design is in `docs/design/DESIGN-2547.md`.

- Each window row now carries the `ctid` that its head scan read. The
  window, the deep window and `seek_held` read their rows with
  `ctid = ANY(ARRAY(...))`, a TID scan. A TID scan reads no index page.
- The claim order, the gates, the lock set and the indexes do not change.
  No migration.
- A peer can commit an update to a window row inside the claim statement.
  Postgres 13.23, 14.20, 15.15, 16.11, 17.7 and later then skip that row,
  as `SKIP LOCKED` skips a held row. The claim can then return no task for
  one poll. Older releases recheck the new version, as before.
- The claim now turns sorts off (`SET LOCAL enable_sort = off`). Each
  head writes its order so that one index alone gives it. A queue head
  tests the new-start flag with a range and sorts on it first. The pin
  head reads its pins in a subquery, ordered by worker first.
- After a backlog drained, `ANALYZE` saw no `PENDING` row. Each head then
  sorted all of `idx_harvest_tq_live_created`, or walked its queue on
  `idx_harvest_tq_poll`. On the test fixture the claim read 18,576 buffers
  on `trunk-dev` and reads 247 now. On its 64 queues of one kind each, it
  read 974,412 and reads 4,097 now.
- `stale_statistics_keep_the_window_bounded` failed at random on that
  case. It now owns its database. `only_the_ordered_index_gives_a_head_its_order`
  drops the ordered indexes and asserts that each head must sort.
- Evidence: `the_window_refine_reads_about_one_buffer_per_row` on the issue
  #1956 fixture. The refine fell from 2.55 to 1.09 buffers per row at 32
  pending rows, and from 3.03 to 1.04 at 1,000. With 32 held claims the
  whole claim fell from 1,726 to 779 buffers. `docs/performance.md` has the
  tables.
- The e2e profile of the issue ran again, with `throughput` at 1 shard.
  The claim fell from 141.1 to 94.7 buffers per call in one run. It fell
  from 238.6 to 101.2 in a second run. Sampled plans show the refine at
  3.89 buffers per row before and 1.00 after. Artifacts are in
  `docs/perf-artifacts/claim-refine-tid/`.
