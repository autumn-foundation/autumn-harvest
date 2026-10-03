-- Revert per-worker task stats (issue #1815). The stats are rebuilt from the
-- next heartbeats after a later upgrade, so no data is lost for good.
DROP TABLE IF EXISTS harvest_worker_task_stats;
