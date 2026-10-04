-- Durable abort marker for the build ramp guard (issue #1814).
--
-- The ramp guard clears an aborted ramp on every shard pool. A guard can stop
-- after it cleared some pools, for example in a restart. The other pools then
-- still hold the ramp. Their own counts can be too few for a new verdict, so
-- routing stays split across pools.
--
-- The compare-and-swap clear now also sets `ramp_aborted_target` to the target
-- build that it removed. The marker and the clear are one UPDATE, so the
-- marker cannot be lost when the clear commits. A later guard sees a ramp on
-- one pool and a marker for the same target on another pool. When the marker
-- is newer than the ramp's step, the guard finishes the clear.
--
-- Additive and nullable. Only the guard writes the column.
ALTER TABLE harvest_build_policies
    ADD COLUMN IF NOT EXISTS ramp_aborted_target TEXT NULL;
