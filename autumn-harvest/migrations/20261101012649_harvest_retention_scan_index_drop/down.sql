-- Revert the second step of issue #2395. Restore the five-state index.
--
-- `idx_harvest_we_retention_candidates` stays until `20261029034500` is
-- reverted too.
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2395 a revert restores the old index as it was built
CREATE INDEX IF NOT EXISTS idx_harvest_we_retention_scan
    ON harvest_workflow_executions (completed_at, id)
    WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW')
      AND completed_at IS NOT NULL;
