-- Decision owner leases (issue #2251, ADR 0009).
--
-- One row per decision partition, 0 to 511. The row names the process that
-- decides for the executions of that partition, and its lease end on the
-- database clock. The row lives in the database of the history that it
-- fences, so it fails over with that history.
--
-- `epoch` counts acquisitions. Each acquisition adds 1, also when the same
-- holder takes back its own lapsed row. A renewal and a release keep it. A
-- decision commit takes the row FOR KEY SHARE and compares holder and epoch.
--
-- The migration seeds every row. No later statement inserts a row, so no
-- upsert can change an epoch.
--
-- The table is new, so the migration locks no existing table. The guard
-- below refuses an epoch that falls or grows by more than 1, a new holder at
-- the old epoch, and a changed partition.
-- No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS harvest_decision_owner (
    partition   INTEGER     PRIMARY KEY,
    holder      TEXT,
    epoch       BIGINT      NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    CONSTRAINT harvest_decision_owner_partition_range
        CHECK (partition >= 0 AND partition < 512),
    CONSTRAINT harvest_decision_owner_epoch_nonnegative CHECK (epoch >= 0)
);

INSERT INTO harvest_decision_owner (partition)
SELECT generate_series(0, 511)
ON CONFLICT (partition) DO NOTHING;

CREATE OR REPLACE FUNCTION harvest_decision_owner_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.partition <> OLD.partition THEN
        RAISE EXCEPTION 'harvest_decision_owner: partition % cannot change', OLD.partition;
    END IF;
    -- A difference, not OLD.epoch + 1, so the check cannot overflow.
    IF NEW.epoch - OLD.epoch NOT IN (0, 1) THEN
        RAISE EXCEPTION 'harvest_decision_owner: epoch of partition % moves from % to %',
            OLD.partition, OLD.epoch, NEW.epoch;
    END IF;
    -- Only an acquisition names a new holder, and it adds 1 to the epoch.
    IF NEW.holder IS NOT NULL AND NEW.holder IS DISTINCT FROM OLD.holder
       AND NEW.epoch = OLD.epoch THEN
        RAISE EXCEPTION 'harvest_decision_owner: a new holder of partition % needs a new epoch',
            OLD.partition;
    END IF;
    RETURN NEW;
END;
$$;

-- The commit fence (ADR 0009 decision 5). It takes the owner row FOR KEY
-- SHARE and returns the holder and the epoch. The caller compares them.
--
-- The SET clause bounds the lock wait to 250 ms, only inside this call.
-- Only an acquisition holds the row FOR UPDATE. A lapsed lease gets that
-- lock for the whole takeover. A live lease gets it for one recheck only.
-- So a short wait passes a healthy owner, and a miss means a takeover.
-- The caller then fails closed.
--
-- The handler catches lock_not_available by its SQLSTATE, not by message
-- text, so a server language does not change the result. It also sets
-- idle_in_transaction_session_timeout for the caller's transaction. A
-- frozen owner then loses its lock (ADR 0009 decision 8).
CREATE OR REPLACE FUNCTION harvest_decision_owner_fence(p_partition INTEGER, p_idle TEXT)
RETURNS TABLE (out_holder TEXT, out_epoch BIGINT, out_locked BOOLEAN)
LANGUAGE plpgsql
SET lock_timeout = '250ms'
AS $$
BEGIN
    PERFORM set_config('idle_in_transaction_session_timeout', p_idle, true);
    RETURN QUERY
        SELECT o.holder, o.epoch, TRUE
          FROM harvest_decision_owner o
         WHERE o.partition = p_partition
           FOR KEY SHARE OF o;
EXCEPTION WHEN lock_not_available THEN
    RETURN QUERY SELECT NULL::TEXT, NULL::BIGINT, FALSE;
END;
$$;

DROP TRIGGER IF EXISTS harvest_decision_owner_guard_trg ON harvest_decision_owner;
CREATE TRIGGER harvest_decision_owner_guard_trg
    BEFORE UPDATE ON harvest_decision_owner
    FOR EACH ROW EXECUTE FUNCTION harvest_decision_owner_guard();

COMMENT ON TABLE harvest_decision_owner IS
    'The owner lease of each decision partition (issue #2251, ADR 0009). '
    'A fence: each decision commit checks holder and epoch FOR KEY SHARE.';
