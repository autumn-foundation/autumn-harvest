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
- The claim now turns sorts off (`SET LOCAL enable_sort = off`), and the
  pin head reads its pins in the key order of `idx_harvest_tq_sticky_poll`.
  After a backlog drained, `ANALYZE` saw no `PENDING` row, and each head
  scan sorted all of `idx_harvest_tq_live_created`. The claim read 197,266
  buffers at 30K pending rows, against 244. It now reads 245.
  `stale_statistics_keep_the_window_bounded` failed at random on that
  case. It now owns its database and fails every time on the old code.
- Evidence: `the_window_refine_reads_about_one_buffer_per_row` on the issue
  #1956 fixture. The refine fell from 2.55 to 1.09 buffers per row at 32
  pending rows, and from 3.03 to 1.04 at 1,000. With 32 held claims the
  whole claim fell from 1,726 to 781 buffers. `docs/performance.md` has the
  tables.
