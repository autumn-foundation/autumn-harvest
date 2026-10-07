-- Revert: drop the build ramp guard outcome index (issue #1814).
DROP INDEX IF EXISTS idx_harvest_we_ramp_guard_outcome;
