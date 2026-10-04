-- Revert: drop the ramp identity and the abort marker (issue #1814).
ALTER TABLE harvest_build_policies
    DROP COLUMN IF EXISTS ramp_aborted_id,
    DROP COLUMN IF EXISTS ramp_id;
