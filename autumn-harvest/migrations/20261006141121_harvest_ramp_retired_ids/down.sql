-- Revert: drop the retired ramp ids (issue #1814).
DROP TABLE IF EXISTS harvest_ramp_retired_ids;
ALTER TABLE harvest_build_policies DROP COLUMN IF EXISTS ramp_caller_id;
