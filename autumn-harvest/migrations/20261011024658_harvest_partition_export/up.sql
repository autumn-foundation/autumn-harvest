-- Issue #2009. Two new, empty tables for the export of aged event partitions.
--
-- `harvest_partition_export` holds one row at most. The row marks a shard
-- whose aged partitions go to a partition archiver before the sweep drops
-- them. A retention runtime with a `PartitionArchiver` writes the row before
-- its first export. From then on, a sweep with no archiver on this shard
-- drops nothing. That covers `harvest partition maintain`, an embedder that
-- calls `RetentionRuntime::spawn`, and a process started without the
-- archiver. Each of these would otherwise drop a partition that no export
-- holds. Delete the row to stop the requirement, for example after you
-- remove the archiver for good.
--
-- `harvest_partition_exports` records each export that a drop checked. The
-- sweep inserts the row in the same transaction as the `DROP TABLE`. So a row
-- exists exactly when its partition was dropped, and no late archive write
-- can change it. `manifest_key` is the archive key of the manifest.
--
-- No hot table is locked, no data moves, no `WorkflowEvent` variant is
-- added, and replay does not change.
CREATE TABLE IF NOT EXISTS harvest_partition_export (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    required_since TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS harvest_partition_exports (
    manifest_key TEXT PRIMARY KEY,
    partition_name TEXT NOT NULL,
    lower_bound TIMESTAMPTZ,
    upper_bound TIMESTAMPTZ NOT NULL,
    row_count BIGINT NOT NULL,
    dropped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
