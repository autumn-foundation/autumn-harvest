-- Ramp identity and durable abort markers for the build ramp guard
-- (issue #1814).
--
-- The ramp guard clears an aborted ramp on every shard pool. A guard can stop
-- after it cleared some pools, for example in a restart. The other pools then
-- still hold the ramp. Their own counts can be too few for a new verdict, so
-- routing stays split across pools.
--
-- `ramp_id` names one operator ramp. The fan-out that sets a ramp writes the
-- same id to every pool. The guard's compare-and-swap clear adds the marker
-- `{"id": ramp_id, "base": build_id}` to `ramp_aborted` in the same UPDATE, so
-- the marker commits with the clear. A later guard finishes a ramp whose
-- `ramp_id` and base build match a marker on another pool. The match uses the
-- id, not database clocks, so clock skew between pools cannot clear a newer
-- ramp or strand an old one.
--
-- `ramp_aborted` is a list, newest first, so a newer abort on a pool keeps the
-- older markers. The guard keeps the 8 newest.
--
-- Additive. A ramp set before this migration has no id, so the guard cannot
-- finish its partial abort after a restart.
ALTER TABLE harvest_build_policies
    ADD COLUMN IF NOT EXISTS ramp_id UUID NULL,
    ADD COLUMN IF NOT EXISTS ramp_aborted JSONB NOT NULL DEFAULT '[]'::jsonb;
