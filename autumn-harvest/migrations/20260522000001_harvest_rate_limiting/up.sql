-- Create rate limiting buckets table for downstream API protection
CREATE TABLE harvest_rate_limit_buckets (
    key VARCHAR PRIMARY KEY,
    refill_rate DOUBLE PRECISION NOT NULL,
    burst DOUBLE PRECISION NOT NULL,
    tokens DOUBLE PRECISION NOT NULL,
    last_refilled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add rate_limit_key column to harvest_task_queue for task-level mapping
ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS rate_limit_key VARCHAR NULL;

-- Index on PENDING tasks with a rate_limit_key to optimize queue poll queries
CREATE INDEX IF NOT EXISTS idx_harvest_task_queue_rate_limit_key
    ON harvest_task_queue (rate_limit_key)
    WHERE state = 'PENDING' AND rate_limit_key IS NOT NULL;
