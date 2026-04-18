CREATE TABLE harvest_workflow_outbox (
    id BIGSERIAL PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    input JSONB NOT NULL,
    memo JSONB,
    search_attrs JSONB,
    delivery_attempts BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    delivered_execution_id TEXT,
    delivered_at TIMESTAMP,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMP,
    claimed_by TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_name, workflow_id)
);

CREATE INDEX idx_harvest_workflow_outbox_due
    ON harvest_workflow_outbox (next_attempt_at, id)
    WHERE delivered_at IS NULL;

CREATE INDEX idx_harvest_workflow_outbox_claimed
    ON harvest_workflow_outbox (claimed_at)
    WHERE delivered_at IS NULL AND claimed_at IS NOT NULL;
