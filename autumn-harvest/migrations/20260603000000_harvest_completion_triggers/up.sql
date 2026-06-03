CREATE TABLE harvest_completion_triggers (
    id UUID PRIMARY KEY,
    source_workflow_name VARCHAR(255) NOT NULL,
    terminal_states JSONB NOT NULL,
    target_workflow_name VARCHAR(255) NOT NULL,
    input_mapping JSONB NOT NULL,
    queue_name VARCHAR(255),
    is_static BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE harvest_completion_trigger_fires (
    source_exec_id UUID NOT NULL,
    trigger_id UUID NOT NULL,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_exec_id, trigger_id)
);

CREATE TABLE harvest_completion_trigger_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_exec_id UUID NOT NULL,
    trigger_id UUID NOT NULL,
    target_shard INT NOT NULL,
    target_workflow_name VARCHAR(255) NOT NULL,
    target_workflow_id VARCHAR(255) NOT NULL,
    target_input JSONB NOT NULL,
    queue_name VARCHAR(255),
    concurrency_key VARCHAR(255),
    concurrency_limit INT,
    priority JSONB NOT NULL,
    max_workflow_input_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_harvest_completion_triggers_source ON harvest_completion_triggers(source_workflow_name);
