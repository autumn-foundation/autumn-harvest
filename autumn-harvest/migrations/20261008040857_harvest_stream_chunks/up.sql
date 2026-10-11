-- Durable workflow output streams (issue #1974).
--
-- Stores the chunks of `ctx.publish_durable_progress`. A reader of
-- `GET /workflows/{id}/stream/durable` resumes after its last offset.
--
-- A side table, not a `WorkflowEvent` variant. Chunks do not count toward the
-- history caps, replay never reads them, and `harvest_events` does not change.
-- Rows live on the shard of their execution.
--
-- The foreign key takes a SHARE ROW EXCLUSIVE lock on the hot executions
-- table for a moment. A held lock fails the migration after 5 s.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS harvest_stream_chunks (
    id               BIGSERIAL   PRIMARY KEY,
    -- Chunks never outlive their execution. Retention needs no extra delete.
    workflow_exec_id UUID        NOT NULL
        REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    -- The 0-based call ordinal. It orders the chunks, it is the resume
    -- cursor, and it is the dedup key for a re-driven decision cycle.
    -- The value 9223372036854775807 marks a stream cut by the chunk cap.
    stream_offset    BIGINT      NOT NULL,
    chunk            JSONB       NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The dedup constraint and the index of the resume read in one.
CREATE UNIQUE INDEX IF NOT EXISTS harvest_stream_chunks_exec_offset
    ON harvest_stream_chunks (workflow_exec_id, stream_offset);
