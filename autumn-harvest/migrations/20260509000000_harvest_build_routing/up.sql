-- Worker build-id routing (issue #171)
--
-- Adds build identity columns to workers and executions, a denormalized
-- required_build_id on task queue items, and two new tables for build
-- policies and compatibility declarations.

-- 1. Workers advertise their build identity.
ALTER TABLE harvest_workers
    ADD COLUMN build_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN deployment_name TEXT;

-- 2. Executions record which build they were started on.
ALTER TABLE harvest_workflow_executions
    ADD COLUMN assigned_build_id TEXT;

-- 3. Task queue items carry a denormalized required_build_id so the
--    claiming SQL can filter without a JOIN.
ALTER TABLE harvest_task_queue
    ADD COLUMN required_build_id TEXT;

-- Index for build-aware claiming (partial: only non-NULL rows need it).
CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_task_queue_required_build_id_pending
    ON harvest_task_queue (required_build_id)
    WHERE state = 'PENDING' AND required_build_id IS NOT NULL;

-- Index for reachability queries on executions.
CREATE INDEX CONCURRENTLY IF NOT EXISTS harvest_wfe_assigned_build_id
    ON harvest_workflow_executions (assigned_build_id)
    WHERE assigned_build_id IS NOT NULL;

-- 4. Build policies: per-queue record of which build new starts get.
CREATE TABLE IF NOT EXISTS harvest_build_policies (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name      TEXT        NOT NULL UNIQUE,
    build_id        TEXT        NOT NULL,
    deployment_name TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Build compatibility declarations.
--    Row means: "workers running build_id may process executions assigned
--    to compatible_with."
CREATE TABLE IF NOT EXISTS harvest_build_compat (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    build_id        TEXT        NOT NULL,
    compatible_with TEXT        NOT NULL,
    declared_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (build_id, compatible_with)
);
