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
  The TID scan then skips that row, as `SKIP LOCKED` skips a held row. A
  later claim reads the new version.
- Evidence: `the_window_refine_reads_about_one_buffer_per_row` on the issue
  #1956 fixture. The refine fell from 2.55 to 1.09 buffers per row at 32
  pending rows, and from 3.03 to 1.04 at 1,000. With 32 held claims the
  whole claim fell from 1,726 to 781 buffers. `docs/performance.md` has the
  tables.
