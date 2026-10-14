-- Weighted fairness keys within a queue (issue #1976).
--
-- A fairness key names the tenant of a task. A worker with fairness keys on
-- rotates its claims across the keys of a queue in weighted round robin.
-- The rules are start-time fair queuing. See DESIGN-1976.md.
--
-- A nullable column with no default changes only the catalog. The ALTER
-- still takes an ACCESS EXCLUSIVE lock on a hot table, so it waits at most
-- 5 s for that lock. Then the migration fails, and the operator runs it
-- again. Traffic queues behind the waiting ALTER for at most those 5 s.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_task_queue ADD COLUMN IF NOT EXISTS fairness_key TEXT;

COMMENT ON COLUMN harvest_task_queue.fairness_key IS
    'The fairness key of the task (issue #1976). Set at start, or taken from '
    'the quota key. Activities take the key of their workflow task. NULL is '
    'the default key. Read only by a worker with fairness keys on.';

-- One row per (queue, key) that a fair claim charged. Only the claim and
-- prune write it. The queue clock V is the largest last_start in a queue.
CREATE TABLE harvest_fairness_state (
    queue_name   TEXT             NOT NULL,
    fairness_key TEXT             NOT NULL,
    pass         DOUBLE PRECISION NOT NULL,
    last_start   DOUBLE PRECISION NOT NULL,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, fairness_key)
);

-- The claim reads the queue clock V, the largest last_start, and the keys
-- whose pass is above V. These two indexes serve both reads without a scan of
-- every key of the queue. The table is new in this migration, so a plain
-- CREATE INDEX takes no lock that live traffic waits on.
CREATE INDEX harvest_fairness_state_clock
    ON harvest_fairness_state (queue_name, last_start);
CREATE INDEX harvest_fairness_state_pass
    ON harvest_fairness_state (queue_name, pass);

COMMENT ON TABLE harvest_fairness_state IS
    'Start-time fair queuing state per (queue, fairness key) (issue #1976). '
    'pass is the virtual time at which the key may start next. last_start is '
    'the start tag of its last claim.';

-- Operator weight overrides. A key with no row has weight 1. The API caps a
-- queue at 1,000 rows.
CREATE TABLE harvest_fairness_weights (
    queue_name   TEXT             NOT NULL,
    fairness_key TEXT             NOT NULL,
    weight       DOUBLE PRECISION NOT NULL
        CHECK (weight >= 0.001 AND weight <= 1000),
    updated_by   TEXT             NOT NULL DEFAULT 'anonymous',
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, fairness_key)
);

COMMENT ON TABLE harvest_fairness_weights IS
    'Runtime weight overrides per (queue, fairness key) (issue #1976). The '
    'next fair claim of the key reads the new weight. No restart is needed.';
