-- Durable completion-callback delivery (issue #605).
--
-- A workflow start option and/or a HarvestBuilder default attach one or more
-- completion callback targets (URL + terminal-state event filter). On the
-- workflow's terminal transition, one row per matching target is inserted
-- here inside the SAME transaction that persists the terminal event
-- (folded into evaluate_triggers_for_execution, mirroring the completion-
-- trigger dedup pattern) -- so a background scanner sees only committed
-- rows and delivery is durable across a worker crash. The scanner
-- (fire_due_completion_deliveries) POSTs the frozen `payload` envelope,
-- HMAC-signed, and records the outcome: DELIVERED on 2xx, PENDING with a
-- future next_attempt_at on a retryable failure, or FAILED (+ a
-- harvest_dead_letters row) on retry exhaustion.
--
-- No new WorkflowEvent variant. harvest_events is never touched by this
-- table or its scanner -- delivery is a post-terminal operational
-- side-channel, not part of the append-only event log.
--
-- Idempotency: UNIQUE (workflow_exec_id, callback_index) makes the enqueue
-- insert (ON CONFLICT DO NOTHING) safe to re-run, e.g. if the parent-close
-- cascade re-enters evaluate_triggers_for_execution for the same execution.
-- `id` doubles as the stable `delivery_id` in the envelope, so a retry or an
-- operator-triggered redrive resends the exact same delivery_id -- the
-- at-least-once + receiver-side-dedup contract.

CREATE TABLE IF NOT EXISTS harvest_completion_deliveries (
    id                UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workflow_exec_id  UUID        NOT NULL,
    shard_id          INT         NOT NULL DEFAULT 0,
    -- Stable index into the deterministic effective-target union computed
    -- at enqueue time (see completion_callback::resolve_all_targets). Part
    -- of the anti-double-enqueue uniqueness key below.
    callback_index    INT         NOT NULL,
    workflow_name     TEXT        NOT NULL,
    workflow_id       TEXT        NOT NULL,
    target_url        TEXT        NOT NULL,
    -- Snapshot of the CallbackTarget's EventFilter at enqueue time.
    event_filter      JSONB       NOT NULL,
    -- The terminal state that actually fired this delivery.
    terminal_state    TEXT        NOT NULL,
    -- Frozen CompletionEnvelope JSON -- signed and POSTed verbatim on every
    -- attempt (including redeliveries), so a later mutation to the
    -- execution row (or its retention-driven deletion) can never change
    -- what gets delivered.
    payload           JSONB       NOT NULL,
    -- PENDING | INFLIGHT | DELIVERED | FAILED
    state             TEXT        NOT NULL DEFAULT 'PENDING',
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL,
    -- Frozen RetryPolicy (initial_interval/backoff_coefficient/max_interval)
    -- at enqueue time, so a later change to the builder's configured policy
    -- never affects an already-enqueued delivery's backoff schedule.
    retry_policy      JSONB       NOT NULL,
    -- Scanner probe column: a row is claimable when this is <= NOW() and
    -- state IN ('PENDING','INFLIGHT'). INFLIGHT rows carry a short lease
    -- here so a crashed scanner's claim is eventually re-claimed.
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_status       INT         NULL,
    last_error        TEXT        NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at      TIMESTAMPTZ NULL,

    CONSTRAINT harvest_completion_deliveries_unique_target
        UNIQUE (workflow_exec_id, callback_index)
);

-- Scanner probe index: claimable rows ordered by due time, scoped per shard.
CREATE INDEX IF NOT EXISTS idx_harvest_completion_deliveries_due
    ON harvest_completion_deliveries (shard_id, next_attempt_at)
    WHERE state IN ('PENDING', 'INFLIGHT');

-- Operator lookup: list deliveries for a given execution (management API).
CREATE INDEX IF NOT EXISTS idx_harvest_completion_deliveries_exec
    ON harvest_completion_deliveries (workflow_exec_id);

-- Per-execution completion-callback registration (issue #605).
--
-- JSON array of `{"url": "...", "filter": {"type": "...", ...}}` objects
-- (see completion_callback::CallbackTarget). NULL = no per-execution
-- targets configured; the effective target set is still the union with any
-- builder-wide defaults (resolved at enqueue time, not stored here).
ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS completion_callbacks JSONB NULL;
