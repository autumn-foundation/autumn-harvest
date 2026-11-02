-- Revert issue #2395. Drop the six-state index.
--
-- `20261101012649` restores the five-state index when it is reverted. That
-- revert runs first. So the candidate scan reads the whole executions table
-- again after this revert.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2395 a revert drops the index this migration built
DROP INDEX IF EXISTS idx_harvest_we_retention_candidates;
