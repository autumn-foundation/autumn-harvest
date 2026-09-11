-- Namespace the replication watermark trail by fence generation (issue
-- #1249, finding 10).
--
-- docs/cross-region-dr.md's setup SQL replicates every table with
-- `CREATE PUBLICATION ... FOR ALL TABLES`, which includes
-- harvest_replication_heartbeat. A standby can therefore carry beats
-- written by the OLD primary before a promotion. Those beats' LSNs belong
-- to the old cluster's WAL stream, and the new primary's WAL stream after
-- promotion is unrelated: the two are not comparable, but the RPO query
-- compared them anyway, ordering both sets of LSNs together and risking a
-- stale beat from before the promotion outranking one from after it.
--
-- `fence_generation` stamps each beat with the write-authority epoch in
-- force when it was written, reusing the epoch harvest_shard_generation
-- already tracks for fencing. measure_rpo filters on it, so a beat from a
-- superseded generation is never read back as if it were part of the
-- current WAL stream.

ALTER TABLE harvest_replication_heartbeat
    ADD COLUMN IF NOT EXISTS fence_generation BIGINT NOT NULL DEFAULT 0;

COMMENT ON COLUMN harvest_replication_heartbeat.fence_generation IS
    'The harvest_shard_generation epoch in force when this beat was written '
    '(issue #1249). Scopes the watermark trail to one WAL stream: a beat '
    'from a superseded generation must never be compared against the '
    'current primary''s LSNs.';
