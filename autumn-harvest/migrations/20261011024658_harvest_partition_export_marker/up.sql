-- Issue #2009. One row marks a shard whose aged event partitions go to a
-- partition archiver before the sweep drops them.
--
-- A retention runtime with a `PartitionArchiver` writes the row before its
-- first export. From then on, a sweep with no archiver on this shard drops
-- nothing. That covers `harvest partition maintain`, an embedder that calls
-- `RetentionRuntime::spawn`, and a process started without the archiver.
-- Each of these would otherwise drop a partition that no export holds.
--
-- The table holds one row at most. Delete the row to stop the requirement,
-- for example after you remove the archiver for good.
--
-- A new, empty table. No hot table is locked, no data moves, no
-- `WorkflowEvent` variant is added, and replay does not change.
CREATE TABLE IF NOT EXISTS harvest_partition_export (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    required_since TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
