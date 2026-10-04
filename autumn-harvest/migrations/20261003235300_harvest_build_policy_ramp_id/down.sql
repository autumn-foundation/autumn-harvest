-- Revert: drop the ramp identity and the abort markers (issue #1814).
ALTER TABLE harvest_build_policies
    DROP COLUMN IF EXISTS ramp_aborted,
    DROP COLUMN IF EXISTS ramp_id;
