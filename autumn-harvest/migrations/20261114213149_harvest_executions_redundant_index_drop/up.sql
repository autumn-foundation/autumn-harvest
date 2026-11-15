-- Drop four indexes of `harvest_workflow_executions` that serve no query
-- another index does not serve as well (#2684).
--
-- - `idx_harvest_we_state` has the predicate `state = 'RUNNING'` of
--   `idx_harvest_we_canary_order`, so both hold the same rows.
-- - `idx_harvest_we_shard (shard_id)` is a prefix of
--   `idx_harvest_we_shard_started (shard_id, started_at)`.
-- - `idx_harvest_we_quota_reconcile_candidates` served the global candidate
--   scan. `20261001192155` replaced that scan with the name-led one, which
--   `idx_harvest_we_quota_reconcile_name_id` serves.
-- - `harvest_wf_exec_detached_running` has the predicate `state = 'RUNNING'`.
--   Both parent-close readers select `state IN ('RUNNING','PAUSED')`, which
--   does not imply it, so the planner never picks it.
--   `idx_harvest_we_parent_id` serves them.
--
-- The drops run in their own migration, apart from the `harvest_events` drop.
-- So no transaction holds the exclusive lock of one table while it waits for
-- the other.
--
-- ## Lock cost
--
-- Each plain `DROP INDEX` takes `ACCESS EXCLUSIVE` on
-- `harvest_workflow_executions` for a short time. `lock_timeout` bounds the
-- wait. When it fires, run the migration again. On a large deployment, drop
-- the indexes out of band first, one statement each:
--
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_state;
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_shard;
--     DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_quota_reconcile_candidates;
--     DROP INDEX CONCURRENTLY IF EXISTS harvest_wf_exec_detached_running;
SET LOCAL lock_timeout = '5s';

-- lock-safety: allow blocking-index #2684 operators drop it CONCURRENTLY first, see the header
DROP INDEX IF EXISTS idx_harvest_we_state;

-- lock-safety: allow blocking-index #2684 operators drop it CONCURRENTLY first, see the header
DROP INDEX IF EXISTS idx_harvest_we_shard;

-- lock-safety: allow blocking-index #2684 operators drop it CONCURRENTLY first, see the header
DROP INDEX IF EXISTS idx_harvest_we_quota_reconcile_candidates;

-- lock-safety: allow blocking-index #2684 operators drop it CONCURRENTLY first, see the header
DROP INDEX IF EXISTS harvest_wf_exec_detached_running;
