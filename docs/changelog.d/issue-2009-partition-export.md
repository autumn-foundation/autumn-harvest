## Storage — export aged partitions before the drop (issue #2009)

On the partitioned layout, the sweep dropped an aged `harvest_events`
partition, and its history was gone. A `PartitionArchiver` now keeps that
history in object storage.

**The seam.** `partition_archive::PartitionArchiver` has two calls,
`put(key, bytes)` and `get(key)`. Core owns the keys, the manifest and every
check. `HarvestBuilder::partition_archiver` registers one.
`DirectoryPartitionArchiver` is a working backend. The S3 and GCS backends of
#1983 fit the same two calls.

**The order.** For each partition that the ownership gate marks as
droppable:

1. Export. Segments of up to 10,000 rows or 8 MiB, one `to_jsonb(row)::text`
   line per row, in key order. The manifest goes last.
2. Verify. Read back each segment and the manifest, and compare the bytes and
   the SHA-256 hashes.
3. Drop. Under the partition's `SHARE` lock, hash the partition again. Drop
   only when the row count and the hash match the manifest.

A failure keeps the partition, with the reason `export failed: ...` or
`changed since export` in `SweepOutcome::blocked`. `SweepOutcome::exported`
lists the manifest key of each dropped partition. Each backend call has the
`archival_timeout_secs` limit. `partition_archive::read_back` checks every
hash and returns typed rows. `ArchivedPartition::history` returns one run's
events.

**Safety.** The key holds the shard and the cohort bounds, so a later
partition with the same name cannot replace an old export. `try_build`
refuses a partition archiver together with
`partitions.straggler_grace_secs`, because a straggler delete removes rows
that no export holds. The sweep also skips straggler deletes when it exports.

**Delta encoding: measured, declined.** On a 40-turn agent loop
(`autumn-harvest-agent/tests/history_delta_measure.rs`), a delta on the
previous transcript stores 11.4% of the plain bytes. Under the AES-GCM codec
it saves nothing, because each field gets a fresh nonce. gzip stores 12.6% to
47.9%, by text entropy. The repeat comes from the agent layer, which can
record only the new messages of each turn. See
`docs/rnd/2026-10-11-agent-history-delta-encoding.md`.

**Invariants.** No migration. No new `WorkflowEvent` variant. No new route.
The export reads rows and the drop is DDL, so the two sanctioned
`event_data` writers do not change.

**Also fixed.** #2102 and #2104 merged tests that set
`WorkflowResetRequest::refuse_erased_source`, which #2103 had removed. The
core `integration` target and `mcp_tasks_integration` did not compile. This
change removes the stale field from both tests.

**Tests.** `partition_archive_tests` (8 DB tests: export, verify, drop and
read back; the legacy partition; a failed upload; a lost object; changed
bytes; a row changed after the export; a live owner; the retention runtime).
13 unit tests in `partition_archive::tests`. The builder test
`a_partition_archiver_with_straggler_deletes_fails_the_build`.
