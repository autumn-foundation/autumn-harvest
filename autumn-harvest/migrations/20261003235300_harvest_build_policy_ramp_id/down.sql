-- Revert: drop the ramp identity, its trigger and the abort markers (issue #1814).
DROP TRIGGER IF EXISTS harvest_build_policies_reset_ramp_id_trigger
    ON harvest_build_policies;
DROP FUNCTION IF EXISTS harvest_build_policies_reset_ramp_id();

ALTER TABLE harvest_build_policies
    DROP COLUMN IF EXISTS ramp_aborted,
    DROP COLUMN IF EXISTS ramp_id;
