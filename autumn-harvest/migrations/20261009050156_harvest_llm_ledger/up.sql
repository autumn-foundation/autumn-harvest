-- Agent cost ledger (issue #1996).
--
-- One row per LLM call that an activity records with
-- `ActivityContext::record_llm_call`. The engine writes the rows in the
-- transaction that appends the completion event. The key is
-- `(workflow_exec_id, event_id, call_index)`: `event_id` names that event.
--
-- A separate table, not a `WorkflowEvent` variant and not a field of
-- `harvest_events.event_data`. History does not change, so replay does not
-- change. The payload codec does not cover this table, so usage reports sum
-- it in SQL without a key. The model id and the counts are in clear. See
-- `docs/security-posture.md`.
--
-- No foreign key to `harvest_events`: the partitioned layout has no unique
-- key on `(workflow_exec_id, event_id)` alone. `ON DELETE CASCADE` from the
-- execution ties the rows to retention.
--
-- The foreign key takes `SHARE ROW EXCLUSIVE` on
-- `harvest_workflow_executions` for the create only. The timeout bounds the
-- wait. No data migration, no `WorkflowEvent` variant, no replay impact.

SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_llm_ledger (
    workflow_exec_id UUID        NOT NULL
        REFERENCES harvest_workflow_executions(id) ON DELETE CASCADE,
    event_id         INT         NOT NULL,
    call_index       INT         NOT NULL,
    activity_name    TEXT        NOT NULL,
    model            TEXT        NOT NULL,
    input_tokens     BIGINT      NOT NULL,
    output_tokens    BIGINT      NOT NULL,
    -- Millionths of a US dollar. NULL means unpriced.
    cost_usd_micros  BIGINT      NULL,
    latency_ms       BIGINT      NOT NULL,
    -- NOW(), as for `harvest_events.timestamp`: the row and its completion
    -- event carry the same instant, so both fall in the same report window.
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_exec_id, event_id, call_index),
    -- The bounds match `llm_ledger.rs`. They keep a group sum far below the
    -- BIGINT limit, so one bad row cannot break the usage report.
    CONSTRAINT harvest_llm_ledger_counts_check CHECK (
        call_index BETWEEN 0 AND 255
        AND input_tokens BETWEEN 0 AND 1000000000000
        AND output_tokens BETWEEN 0 AND 1000000000000
        AND (cost_usd_micros IS NULL OR cost_usd_micros BETWEEN 0 AND 1000000000000000)
        AND latency_ms BETWEEN 0 AND 10000000000
    ),
    -- A model id is a token: no space, no control character, no NUL.
    CONSTRAINT harvest_llm_ledger_model_check CHECK (
        octet_length(model) BETWEEN 1 AND 200
        AND model ~ '^[A-Za-z0-9._:/@+-]+$'
    )
);

-- The usage report windows the ledger by `recorded_at`.
CREATE INDEX harvest_llm_ledger_recorded_at
    ON harvest_llm_ledger (recorded_at);

COMMENT ON TABLE harvest_llm_ledger IS
    'Agent cost ledger: model, tokens, cost and latency per LLM call, in clear (issue #1996).';
