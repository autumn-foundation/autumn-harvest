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

-- Backfill existing rows that are safely known to belong to the CURRENT
-- generation: a beat written on or after the last recorded fence bump was
-- written by this primary, under the epoch now in force. A beat older than
-- that predates the current epoch (or the shard has never been fenced, in
-- which case every row already matches the `0` default) and is left alone,
-- so it stays correctly excluded by measure_rpo's generation filter rather
-- than being mislabeled as current. Without this backfill every existing
-- row reads as generation 0 until the next beat, which is safe -- the RPO
-- is unknown, never wrong -- but this makes the safe rows visible sooner
-- on an upgrade of an already-fenced deployment.
UPDATE harvest_replication_heartbeat h
SET fence_generation = g.generation
FROM harvest_shard_generation g
WHERE g.shard_id = h.shard_id
  AND h.beat_at >= g.fenced_at;

COMMENT ON COLUMN harvest_replication_heartbeat.fence_generation IS
    'The harvest_shard_generation epoch in force when this beat was written '
    '(issue #1249). Scopes the watermark trail to one WAL stream: a beat '
    'from a superseded generation must never be compared against the '
    'current primary''s LSNs.';
