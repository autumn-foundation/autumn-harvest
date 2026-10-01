## Engine — Lazy audit claim-scan index (issue #1667)

An unconfigured deployment no longer pays for the audit export index.

- `harvest_audit_log_unexported_idx` matched every audit row while export was
  off. It cost about 6% more buffer accesses per insert and about 40 bytes of
  storage per row. It served no read (issue #1272).
- Migration `20261001190405_harvest_audit_unexported_idx_lazy` sets a 3 s
  `lock_timeout` and drops the index
  when `harvest_audit_export_cursor` has no row. A cursor row means export ran,
  so those databases keep the index.
- `ensure_unexported_index` builds the index with `CREATE INDEX CONCURRENTLY`.
  It rebuilds an invalid index. It turns `statement_timeout` off for the build and restores the old value.
  A session advisory lock stops two exporters from dropping each other's build.
- A pool of four or more connections runs the build in a detached task. The
  build never delays a claim. A smaller pool builds inline on the tick
  connection, so no other session loses a connection. A failure logs a warning and waits five
  minutes. Export continues without the index. The caller drops the connection
  if the unlock fails.
- No `WorkflowEvent` change. No `harvest_events` write. No replay impact.
- Tests: `audit_export_tests` cover the migration with and without a cursor row,
  the first-tick build, no build when unconfigured, idempotence, invalid-index
  rebuild, and the lock skip.
