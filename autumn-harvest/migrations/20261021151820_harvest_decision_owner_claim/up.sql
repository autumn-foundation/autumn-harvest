-- Owned workflow-task claims and the owner-mode switch (issue #2252, ADR 0009
-- decision 11).
--
-- `harvest_task_queue.decision_partition` holds the decision partition of a
-- workflow task. The writer stamps it at insert. Park and wake keep the row,
-- so one stamp lasts for the whole run. NULL means "not stamped": a row from
-- before this migration, an activity row, or a row that an old binary wrote.
-- An owned claim may take a NULL row. The worker then stamps it and re-pends
-- it to its owner.
--
-- `harvest_decision_owner_mode` is the per-database switch. It has exactly
-- one row. `harvest_decision_owner_candidates` lists the live owner loops. `partitions` is the partition count of this database. A process
-- that reads another count refuses owner mode.
--
-- `harvest_task_queue_owner_claim_guard` is the guard on the claim of a
-- workflow task. The migration creates the function only. The enable step
-- attaches it as a trigger, and the disable step drops the trigger. So a
-- database with owner mode off pays nothing.
--
-- No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.
--
-- A nullable column with no default needs no table rewrite.
-- A held lock fails the migration after 5 s.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_task_queue
    ADD COLUMN IF NOT EXISTS decision_partition SMALLINT;

CREATE TABLE IF NOT EXISTS harvest_decision_owner_mode (
    singleton  BOOLEAN     PRIMARY KEY DEFAULT TRUE,
    enabled    BOOLEAN     NOT NULL DEFAULT FALSE,
    partitions INTEGER     NOT NULL DEFAULT 512,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_decision_owner_mode_one_row CHECK (singleton),
    CONSTRAINT harvest_decision_owner_mode_partitions CHECK (partitions > 0)
);

INSERT INTO harvest_decision_owner_mode (singleton) VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

-- The owner candidates of this database. Each owner loop writes its holder
-- token here on each scan, about once a second. The balance counts the
-- candidates seen in the last few seconds. A crashed worker drops out
-- quickly, and a worker that cannot own partitions never writes a row.
CREATE TABLE IF NOT EXISTS harvest_decision_owner_candidates (
    holder  TEXT        PRIMARY KEY,
    seen_at TIMESTAMPTZ NOT NULL
);

-- The claim guard. It runs only for a claim of a workflow task, through the
-- `WHEN` clause of the trigger that the enable step creates.
--
-- An owned claim sets `harvest.owner_claim` to its holder token for its own
-- transaction. A claim with no token is a legacy claim, so the guard skips
-- the row: the claim then selects nothing. `RETURN NULL` skips the row and
-- raises no error. A new binary with a stale view of the switch misses its
-- workflow claims until its next scan, about one second.
--
-- For a stamped row, the guard takes the owner row FOR KEY SHARE and matches
-- the holder. SKIP LOCKED means the claim never waits for an owner row. Only
-- an acquisition holds that row FOR UPDATE, so a skip is a takeover in
-- progress, and the guard skips the task row too. The lock stays until the
-- claim commits, so an acquisition waits for it and then re-pends the task.
--
-- An unstamped row passes. The worker stamps it and re-pends it to its owner.
CREATE OR REPLACE FUNCTION harvest_task_queue_owner_claim_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    token TEXT := current_setting('harvest.owner_claim', true);
BEGIN
    IF token IS NULL OR token = '' THEN
        RETURN NULL;
    END IF;
    IF NEW.decision_partition IS NULL THEN
        RETURN NEW;
    END IF;
    PERFORM 1 FROM harvest_decision_owner o
     WHERE o.partition = NEW.decision_partition
       AND o.holder = token
       FOR KEY SHARE OF o SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;

COMMENT ON COLUMN harvest_task_queue.decision_partition IS
    'The decision partition of a workflow task (issue #2252). NULL: not stamped.';
COMMENT ON TABLE harvest_decision_owner_candidates IS
    'The live owner loops of this database (issue #2252). The balance counts them.';
COMMENT ON TABLE harvest_decision_owner_mode IS
    'The owner-mode switch of this database (issue #2252, ADR 0009 decision 11).';
