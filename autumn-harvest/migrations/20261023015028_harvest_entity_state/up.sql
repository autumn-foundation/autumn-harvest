-- Keyed-entity state projection (issue #2259, ADR 0006 amendment).
--
-- The entity loop records each state change as a history event:
-- `SideEffectRecorded` with kind `EntityState`. This table holds the latest
-- snapshot of each entity key. A trigger on `harvest_events` writes it in
-- the transaction that inserts the event. So the projection and history
-- commit together or not at all, on every insert path.
--
-- The table is not `harvest_events`, so it is not an append-only exception.
-- Codec rotation sweeps `snapshot`, and erasure tombstones it.
--
-- `snapshot` is the event `value`, byte for byte. It is ciphertext when a
-- codec is set, and an offload reference when the payload store holds it.
--
-- Ordering. A row of the same run moves forward by `event_id`. A row of
-- another run moves when the new `harvest_events.id` is larger. One sequence
-- feeds that id, and one key has one writer at a time. Worker clocks set
-- `created_at`, so that column cannot order runs.
--
-- The foreign key removes the row when retention removes its run. A row then
-- never points at a payload that retention collected.
--
-- `CREATE TRIGGER` takes a SHARE ROW EXCLUSIVE lock on `harvest_events`, and
-- the foreign key takes one on `harvest_workflow_executions`. A held lock
-- fails the migration after 5 s, and the operator runs it again.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_entity_state (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT        NOT NULL,
    workflow_id   TEXT        NOT NULL,
    execution_id  UUID        NOT NULL
        REFERENCES harvest_workflow_executions (id) ON DELETE CASCADE,
    event_id      INTEGER     NOT NULL,
    event_row_id  BIGINT      NOT NULL,
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    snapshot      JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_entity_state_key UNIQUE (workflow_name, workflow_id)
);

-- Erasure and the cascade find rows by run.
CREATE INDEX idx_harvest_entity_state_execution
    ON harvest_entity_state (execution_id);

COMMENT ON TABLE harvest_entity_state IS
    'Issue #2259: the latest keyed-entity snapshot of each key. A trigger on harvest_events writes it.';

-- The projection function. `SET search_path` pins name resolution to the
-- schema Harvest is installed in, as `harvest_events_require_execution` does.
-- The body can then use unqualified names, and a session cannot shadow them.
DO $harvest_entity_state_2259$
BEGIN
    EXECUTE pg_catalog.format($harvest_entity_state_fn$
CREATE OR REPLACE FUNCTION harvest_entity_state_project()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = %I, pg_catalog
AS $harvest_entity_state_body$
DECLARE
    run record;
BEGIN
    SELECT workflow_name, workflow_id INTO run
      FROM harvest_workflow_executions
     WHERE id = NEW.workflow_exec_id;
    -- A row with no run is the insert-time check's error, not this one.
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    INSERT INTO harvest_entity_state AS s
        (workflow_name, workflow_id, execution_id, event_id, event_row_id,
         deleted, snapshot, updated_at)
    VALUES
        (run.workflow_name, run.workflow_id, NEW.workflow_exec_id, NEW.event_id,
         NEW.id,
         coalesce((NEW.event_data -> 'data' ->> 'name') = 'harvest.entity.deleted', false),
         coalesce(NEW.event_data -> 'data' -> 'value', 'null'::jsonb), now())
    ON CONFLICT (workflow_name, workflow_id) DO UPDATE
       SET execution_id = EXCLUDED.execution_id,
           event_id     = EXCLUDED.event_id,
           event_row_id = EXCLUDED.event_row_id,
           deleted      = EXCLUDED.deleted,
           snapshot     = EXCLUDED.snapshot,
           updated_at   = EXCLUDED.updated_at
     WHERE (s.execution_id = EXCLUDED.execution_id AND s.event_id < EXCLUDED.event_id)
        OR (s.execution_id <> EXCLUDED.execution_id AND s.event_row_id < EXCLUDED.event_row_id);
    RETURN NULL;
END
$harvest_entity_state_body$
$harvest_entity_state_fn$, current_schema());
END
$harvest_entity_state_2259$;

COMMENT ON FUNCTION harvest_entity_state_project() IS
    'Issue #2259: writes harvest_entity_state from an entity snapshot event. See ADR 0006.';

-- The `WHEN` clause runs in the executor, so every other event skips the
-- function call. The partition layout changes reinstall this trigger with
-- the append-only guard.
CREATE TRIGGER harvest_entity_state_projection_trg
    AFTER INSERT ON harvest_events
    FOR EACH ROW
    WHEN (NEW.event_type = 'SideEffectRecorded'
          AND (NEW.event_data -> 'data' ->> 'kind') = 'EntityState')
    EXECUTE FUNCTION harvest_entity_state_project();
