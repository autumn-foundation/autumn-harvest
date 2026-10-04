-- Ramp identity and durable abort markers for the build ramp guard
-- (issue #1814).
--
-- The ramp guard clears an aborted ramp on every shard pool. A guard can stop
-- after it cleared some pools, for example in a restart. The other pools then
-- still hold the ramp. Their own counts can be too few for a new verdict, so
-- routing stays split across pools.
--
-- `ramp_id` names one operator ramp. The fan-out that sets a ramp writes the
-- same id to every pool. The guard's compare-and-swap clear adds a marker
-- (`id`, `base`, `target`, `reported`, `at`) to `ramp_aborted` in the same UPDATE, so
-- the marker commits with the clear. A later guard finishes a ramp whose
-- `ramp_id` and base build match a marker on another pool. The match uses the
-- id, not database clocks, so clock skew between pools cannot clear a newer
-- ramp or strand an old one.
--
-- `ramp_aborted` is a list, newest first, so a newer abort on a pool keeps the
-- older markers. A guard pass removes a marker once no pool holds its ramp.
--
-- Additive. A ramp set before this migration has no id, so the guard cannot
-- finish its partial abort after a restart.
ALTER TABLE harvest_build_policies
    ADD COLUMN IF NOT EXISTS ramp_id UUID NULL,
    ADD COLUMN IF NOT EXISTS ramp_aborted JSONB NOT NULL DEFAULT '[]'::jsonb;

-- A writer from before this migration changes a ramp with its old UPDATE.
-- That UPDATE sets the target, the percentage or `updated_at`, but it keeps
-- the old `ramp_id`. An old abort marker could then match the new ramp and
-- clear it with no verdict. This trigger clears `ramp_id` on such a write,
-- so a ramp keeps an id only when an id-aware writer set it. A ramp with no
-- id is judged as usual. The guard's own UPDATEs on the marker list do not
-- change these columns, so the trigger leaves them alone.
CREATE OR REPLACE FUNCTION harvest_build_policies_reset_ramp_id()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.ramp_id IS NOT DISTINCT FROM OLD.ramp_id
       AND (NEW.target_build_id IS DISTINCT FROM OLD.target_build_id
            OR NEW.ramp_percent IS DISTINCT FROM OLD.ramp_percent
            OR NEW.updated_at IS DISTINCT FROM OLD.updated_at) THEN
        NEW.ramp_id := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS harvest_build_policies_reset_ramp_id_trigger
    ON harvest_build_policies;
CREATE TRIGGER harvest_build_policies_reset_ramp_id_trigger
    BEFORE UPDATE ON harvest_build_policies
    FOR EACH ROW
    EXECUTE FUNCTION harvest_build_policies_reset_ramp_id();
