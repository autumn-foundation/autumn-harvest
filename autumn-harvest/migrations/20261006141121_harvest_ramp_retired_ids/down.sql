-- Revert: drop the retired ramp ids (issue #1814).
-- Restore the reset trigger function first, as it names `ramp_caller_id`.
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

DROP TABLE IF EXISTS harvest_ramp_retired_ids;
ALTER TABLE harvest_build_policies DROP COLUMN IF EXISTS ramp_caller_id;
