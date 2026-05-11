-- Support retention candidate scans filtered by state and completed_at cutoff.
CREATE INDEX idx_harvest_we_retention_scan
    ON harvest_workflow_executions (completed_at, id)
    WHERE state IN ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','CONTINUED_AS_NEW')
      AND completed_at IS NOT NULL;
