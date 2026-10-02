## Engine — Lazy audit claim-scan index (issue #1667)

An unconfigured deployment no longer pays for the audit export index.

- `harvest_audit_log_unexported_idx` matched every audit row while export was
  off. It cost about 6% more buffer accesses per insert and about 40 bytes of
  storage per row. It served no read (issue #1272).
- Migration `20261001190405_harvest_audit_unexported_idx_lazy` drops the index
  when `harvest_audit_export_cursor` has no row, under a 5 s `lock_timeout`. A
  cursor row means export ran, so those databases keep the index.
- `ensure_unexported_index` builds the index with `CREATE INDEX CONCURRENTLY`.
  It rebuilds an invalid index. It turns `statement_timeout` off for the build
  and restores the exact previous session value. A session advisory lock stops
  two exporters from dropping each other's build. The result is `Ready`,
  `LockBusy` or an error, and `Ready` is confirmed from `pg_index.indisvalid`.
- The dedicated export task builds in a detached task, on its own connection
  opened from the shard's notification database URL. No pooled connection is
  held for the build, so a one-connection shard pool exports while the build
  runs. Shutdown closes the build connection. Only a confirmed valid index
  reopens the per-shard retry gate; a lost lock race or a failure waits five
  minutes. A worker role that does not own the table gets one `error!` line
  with the `CREATE INDEX CONCURRENTLY` and `ALTER TABLE ... OWNER TO`
  statements, then an hour's wait.
- Without a notification URL for the shard, and on the embedder-driven
  `fire_due_audit_exports` path, the exporter never builds. It logs the
  statement once an hour. Export is correct without the index.
- No `WorkflowEvent` change. No `harvest_events` write. No replay impact.
- Tests: `audit_export_tests` cover the migration with and without a cursor row
  and under a held table lock, the first-tick build, no build when unconfigured
  or without a build URL, idempotence, invalid-index rebuild, the lock-busy
  result, the `statement_timeout` restore, the one-connection pool, and the
  refused build of a non-owner role.
