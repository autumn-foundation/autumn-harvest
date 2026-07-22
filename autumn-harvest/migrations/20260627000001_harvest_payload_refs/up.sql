-- Per-execution references to offloaded payload blobs (issue #524).
--
-- When a payload-bearing field exceeds the offload threshold it is written to an
-- embedder-supplied PayloadStore and replaced inline with a reference envelope.
-- One row is recorded here per (blob_key, workflow_exec_id) so the retention
-- sweep can garbage-collect a blob exactly when no surviving execution still
-- references it.
--
-- The ON DELETE CASCADE makes the live COUNT(*) over blob_key the authoritative
-- reference count: deleting an execution row (retention) drops its refs in the
-- same transaction, with no manual decrement and no drift. continue_as_new and
-- child-workflow boundaries that carry a reference forward insert an additional
-- row for the new execution WITHOUT re-uploading the blob, so the blob survives
-- until the last referencing execution is collected.
--
-- Scoping: shard-local (the refs live in the same database as the events they
-- describe), consistent with the rest of the per-execution state model.

CREATE TABLE IF NOT EXISTS harvest_payload_refs (
    blob_key          TEXT        NOT NULL,
    workflow_exec_id  UUID        NOT NULL
        REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    store_id          TEXT        NOT NULL,
    byte_len          BIGINT      NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (blob_key, workflow_exec_id)
);

-- Drop all of an execution's refs in one indexed delete (cascade + GC discovery).
CREATE INDEX IF NOT EXISTS idx_harvest_payload_refs_exec
    ON harvest_payload_refs (workflow_exec_id);

-- Residual-reference check during GC: is this blob still referenced by anyone?
CREATE INDEX IF NOT EXISTS idx_harvest_payload_refs_key
    ON harvest_payload_refs (blob_key);
