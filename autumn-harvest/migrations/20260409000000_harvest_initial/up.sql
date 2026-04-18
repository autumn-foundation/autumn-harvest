-- autumn-harvest/migrations/20260409000000_harvest_initial/up.sql
-- Workflow execution tracking and event history for autumn-harvest.
-- All tables prefixed with harvest_ to avoid collisions with application tables.

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Workflow executions (one row per run)
CREATE TABLE harvest_workflow_executions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name       TEXT NOT NULL,
    workflow_id         TEXT NOT NULL,
    run_id              UUID NOT NULL DEFAULT gen_random_uuid(),
    shard_id            INT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'RUNNING'
                            CHECK (state IN ('RUNNING','COMPLETED','FAILED','CANCELLED','TIMED_OUT')),
    input               JSONB NOT NULL,
    output              JSONB,
    error               TEXT,
    parent_id           UUID REFERENCES harvest_workflow_executions(id),
    sticky_worker_id    TEXT,
    queue_name          TEXT NOT NULL DEFAULT 'default',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    execution_timeout   INTERVAL,
    memo                JSONB,
    search_attrs        JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_name, workflow_id)
);

CREATE INDEX idx_harvest_we_shard  ON harvest_workflow_executions (shard_id);
CREATE INDEX idx_harvest_we_state  ON harvest_workflow_executions (state)
    WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_we_search ON harvest_workflow_executions USING GIN (search_attrs);

-- Event history (append-only log, one sequence per execution)
CREATE TABLE harvest_events (
    id               BIGSERIAL PRIMARY KEY,
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    event_id         INT NOT NULL,      -- 0, 1, 2, ... within a workflow
    event_type       TEXT NOT NULL,
    event_data       JSONB NOT NULL,
    timestamp        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_exec_id, event_id)
);

CREATE INDEX idx_harvest_events_exec ON harvest_events (workflow_exec_id, event_id);

-- Task queue (Postgres-backed work queue)
CREATE TABLE harvest_task_queue (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name          TEXT NOT NULL,
    task_type           TEXT NOT NULL CHECK (task_type IN ('workflow','activity')),
    workflow_exec_id    UUID REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    activity_name       TEXT,
    input               JSONB NOT NULL,
    state               TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (state IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    priority            INT NOT NULL DEFAULT 0,
    worker_id           TEXT,
    attempt             INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 1,
    scheduled_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    last_heartbeat_at   TIMESTAMPTZ,
    heartbeat_timeout   INTERVAL,
    start_to_close      INTERVAL,
    schedule_to_start   INTERVAL,
    retry_policy        JSONB,
    output              JSONB,
    error               TEXT
);

CREATE INDEX idx_harvest_tq_poll ON harvest_task_queue
    (queue_name, state, priority DESC, scheduled_at)
    WHERE state = 'PENDING';
CREATE INDEX idx_harvest_tq_running ON harvest_task_queue
    (state, last_heartbeat_at)
    WHERE state = 'RUNNING';
CREATE INDEX idx_harvest_tq_workflow ON harvest_task_queue (workflow_exec_id);

-- DAG runs
CREATE TABLE harvest_dag_runs (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name              TEXT NOT NULL,
    workflow_exec_id      UUID REFERENCES harvest_workflow_executions(id),
    state                 TEXT NOT NULL DEFAULT 'QUEUED'
                              CHECK (state IN ('QUEUED','RUNNING','SUCCESS','FAILED')),
    logical_date          TIMESTAMPTZ NOT NULL,
    data_interval_start   TIMESTAMPTZ NOT NULL,
    data_interval_end     TIMESTAMPTZ NOT NULL,
    conf                  JSONB,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_name, logical_date)
);

CREATE INDEX idx_harvest_dr_schedule ON harvest_dag_runs (dag_name, state, logical_date);

-- DAG schedules registry
CREATE TABLE harvest_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_name        TEXT NOT NULL UNIQUE,
    schedule_expr   TEXT,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    catchup         BOOLEAN NOT NULL DEFAULT FALSE,
    max_active_runs INT NOT NULL DEFAULT 1,
    is_paused       BOOLEAN NOT NULL DEFAULT FALSE,
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pending signals for running workflows
CREATE TABLE harvest_signals (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    signal_name      TEXT NOT NULL,
    payload          JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consumed         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_signals_pending ON harvest_signals (workflow_exec_id, signal_name)
    WHERE NOT consumed;

-- Durable timers
CREATE TABLE harvest_timers (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_exec_id UUID NOT NULL REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    timer_id         TEXT NOT NULL,
    fires_at         TIMESTAMPTZ NOT NULL,
    fired            BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_harvest_timers_pending ON harvest_timers (fires_at)
    WHERE NOT fired;

-- Dead letter queue
CREATE TABLE harvest_dead_letters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_task_id UUID NOT NULL,
    queue_name       TEXT NOT NULL,
    task_type        TEXT NOT NULL,
    workflow_exec_id UUID,
    activity_name    TEXT,
    input            JSONB NOT NULL,
    error            TEXT NOT NULL,
    attempts         INT NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
