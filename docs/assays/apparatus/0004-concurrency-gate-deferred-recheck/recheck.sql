-- The authoritative recheck, isolated: identical shape to queue.rs:762-768's
-- `claimed` CTE recheck (the piece this whole shape reuses unmodified,
-- just called more often than "once per successful claim"). Parameterized
-- via psql variables so the same script measures the idle key (never
-- matches) and a real hot key (matches up to running_rows / keys times).
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)
SELECT COUNT(*) FROM harvest_task_queue recheck
WHERE recheck.concurrency_key = :'probe_key'
  AND recheck.task_type = 'activity'
  AND recheck.state = 'RUNNING'
  AND recheck.worker_id IS NOT NULL;
