-- Slot-key index for the per-schedule run-history endpoint
-- (GET /admin/schedules/{id}/runs, issue #762 / #980 Codex P2).
--
-- The list query orders by the logical-slot key rather than started_at:
--
--   SELECT ... FROM harvest_workflow_executions
--   WHERE schedule_id = $1 AND shard_id = $2 ...
--   ORDER BY COALESCE(scheduled_for, started_at) DESC, id DESC
--   LIMIT $n;
--
-- The pre-existing idx_harvest_wfx_schedule_runs is keyed on
-- (schedule_id, started_at DESC, id DESC), which no longer matches this
-- ORDER BY (nor the keyset cursor predicate on the same coalesced key), so a
-- high-frequency schedule would sort all of its retained runs before applying
-- LIMIT. This expression index mirrors the query's equality predicates and
-- ORDER BY / cursor key exactly so the planner can serve the filter, the
-- ordering, and the keyset predicate from the index. Both equality columns
-- (schedule_id, shard_id) lead the sort columns because the query filters on
-- both (two logical shards can share one physical database, so the per-shard
-- fan-out scopes each query by shard_id) — the planner needs the equality
-- columns first, then the (COALESCE(...) DESC, id DESC) sort key + tiebreak.
--
-- The pre-existing idx_harvest_wfx_schedule_runs is deliberately retained: the
-- scheduled-cadence summary aggregate and origin/started_at filters still use
-- the started_at-keyed index.
CREATE INDEX IF NOT EXISTS idx_harvest_wfx_schedule_runs_slot
    ON harvest_workflow_executions (schedule_id, shard_id, (COALESCE(scheduled_for, started_at)) DESC, id DESC)
    WHERE schedule_id IS NOT NULL;
