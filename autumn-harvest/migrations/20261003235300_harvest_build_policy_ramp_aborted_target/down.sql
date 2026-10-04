-- Revert: drop the ramp guard abort marker (issue #1814).
ALTER TABLE harvest_build_policies DROP COLUMN IF EXISTS ramp_aborted_target;
