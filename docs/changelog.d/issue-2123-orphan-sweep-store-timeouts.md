## Engine — Timeouts on the orphan sweep's store calls (issue #2123)

**Behavior change.** None unless the orphan sweep is on and its store hangs.

A hung `PayloadStore::list`, `delete` or `delete_if_older` held the retention
tick and the fence guards that the tick owns. The sweep waited on each blob in
turn, so it could stay in the call for hours.

- `OrphanSweepOptions::call_timeout` is the longest that one store call may
  take. The retention runtime sets it to `RetentionConfig::archival_timeout`.
- A call that takes longer ends the sweep after one timeout. The report
  carries the error, and the next tick starts again from the top.
- Shutdown also ends a pending call. A page that shutdown cuts short keeps its
  cursor.

`OrphanSweepOptions` gains a field. Code that builds it literally must set
`call_timeout`.

**Tests.** `payload_orphan_sweep_tests` hangs `list`, `delete` and
`delete_if_older`, and cancels the sweep during a hung delete.
