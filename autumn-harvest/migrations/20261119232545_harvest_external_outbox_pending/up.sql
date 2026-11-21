-- The pending table of the external outbox: its table, indexes and
-- function. The next migration installs the trigger and the backfill.
--
-- The three outbox claims searched `harvest_events` for requests that have
-- no resolution. A request row stays in that table after it resolves, so
-- each claim walked every request of the deployment. An idle claim paid for
-- the whole history.
--
-- This table holds one row for each unresolved request. A trigger on
-- `harvest_events` writes it in the transaction that inserts the event. So
-- the table and history commit together, on every insert path.
--
-- A request inserts a row, unless its run already holds a resolution with
-- the same key. A resolution deletes each row of its run with the same type
-- and key. The check at the request makes the insert order unimportant: a
-- drain of the `DEFAULT` partition inserts old rows again, in any order.
--
-- The table is not `harvest_events`, so it is not an append-only exception.
-- The claim keeps its own resolution check, so a stale row cannot cause a
-- second delivery.
--
-- The foreign key removes the rows of a run with the run.
--
-- ## Lock cost
--
-- The foreign key takes `SHARE ROW EXCLUSIVE` on
-- `harvest_workflow_executions` until the commit. This migration reads no
-- rows, so the lock is short. The trigger goes in the next migration, so no
-- transaction holds locks on both hot tables. A held lock fails the
-- migration after 5 s, and the operator runs it again.
SET LOCAL lock_timeout = '5s';

-- Each resolution deletes a row, so the table churns. The claim reads the
-- leaf pages of its dead index entries until vacuum removes them: about one
-- page for each 140 entries. The default threshold scales with the table, so
-- a drained backlog can leave many dead entries. A fixed threshold starts
-- vacuum after 1,000 dead rows, whatever the table size. Analyze uses the
-- same threshold, so the estimate follows a drained backlog.
CREATE TABLE harvest_external_outbox_pending (
    event_row_id     BIGINT      PRIMARY KEY,
    workflow_exec_id UUID        NOT NULL
        REFERENCES harvest_workflow_executions (id) ON DELETE CASCADE,
    request_type     TEXT        NOT NULL
        CHECK (request_type IN ('ExternalSignalRequested',
                                'ExternalCancelRequested',
                                'ExternalAwaitRequested')),
    correlation_id   TEXT        NOT NULL,
    requested_at     TIMESTAMPTZ NOT NULL
) WITH (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 1000
);

-- The claim reads one type in request order, oldest first.
CREATE INDEX idx_harvest_external_outbox_pending_claim
    ON harvest_external_outbox_pending (request_type, requested_at, event_row_id);

-- A resolution finds its rows by run and key. The cascade finds them by run.
CREATE INDEX idx_harvest_external_outbox_pending_resolve
    ON harvest_external_outbox_pending (workflow_exec_id, request_type, correlation_id);

COMMENT ON TABLE harvest_external_outbox_pending IS
    'One row for each unresolved external request. A trigger on harvest_events writes it.';

-- The projection function. `SET search_path` pins name resolution to the
-- schema Harvest is installed in, as `harvest_entity_state_project` does.
--
-- Each family has its own branch with literal keys. So each resolution
-- check reads through its own expression index.
DO $harvest_external_outbox$
BEGIN
    EXECUTE pg_catalog.format($harvest_external_outbox_fn$
CREATE OR REPLACE FUNCTION harvest_external_outbox_project()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = %I, pg_catalog
AS $harvest_external_outbox_body$
DECLARE
    kind        TEXT;
    correlation TEXT;
    resolved    BOOLEAN;
BEGIN
    CASE
    WHEN NEW.event_type IN ('ExternalSignalRequested',
                            'ExternalSignalDelivered',
                            'ExternalSignalFailed') THEN
        kind := 'ExternalSignalRequested';
        correlation := NEW.event_data -> 'data' ->> 'signal_id';
    WHEN NEW.event_type IN ('ExternalCancelRequested',
                            'ExternalCancelDelivered',
                            'ExternalCancelFailed') THEN
        kind := 'ExternalCancelRequested';
        correlation := NEW.event_data -> 'data' ->> 'cancel_id';
    WHEN NEW.event_type IN ('ExternalAwaitRequested',
                            'ExternalAwaitResolved',
                            'ExternalAwaitFailed') THEN
        kind := 'ExternalAwaitRequested';
        correlation := NEW.event_data -> 'data' ->> 'await_id';
    ELSE
        RETURN NULL;
    END CASE;
    -- The claim never selects a request with no key.
    IF correlation IS NULL THEN
        RETURN NULL;
    END IF;

    IF NEW.event_type <> kind THEN
        DELETE FROM harvest_external_outbox_pending p
         WHERE p.workflow_exec_id = NEW.workflow_exec_id
           AND p.request_type = kind
           AND p.correlation_id = correlation;
        RETURN NULL;
    END IF;

    CASE kind
    WHEN 'ExternalSignalRequested' THEN
        resolved := EXISTS (
            SELECT 1 FROM harvest_events res
             WHERE res.workflow_exec_id = NEW.workflow_exec_id
               AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
               AND res.event_data -> 'data' ->> 'signal_id' = correlation);
    WHEN 'ExternalCancelRequested' THEN
        resolved := EXISTS (
            SELECT 1 FROM harvest_events res
             WHERE res.workflow_exec_id = NEW.workflow_exec_id
               AND res.event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed')
               AND res.event_data -> 'data' ->> 'cancel_id' = correlation);
    ELSE
        resolved := EXISTS (
            SELECT 1 FROM harvest_events res
             WHERE res.workflow_exec_id = NEW.workflow_exec_id
               AND res.event_type IN ('ExternalAwaitResolved', 'ExternalAwaitFailed')
               AND res.event_data -> 'data' ->> 'await_id' = correlation);
    END CASE;
    IF resolved THEN
        RETURN NULL;
    END IF;

    -- A drain of the partitioned layout can move a row whose run is gone.
    -- Skip it, so the foreign key does not fail the drain. `FOR KEY SHARE`
    -- waits for a delete in flight, so a run that it removes is skipped too.
    INSERT INTO harvest_external_outbox_pending
        (event_row_id, workflow_exec_id, request_type, correlation_id, requested_at)
    SELECT NEW.id, NEW.workflow_exec_id, kind, correlation, NEW.timestamp
     WHERE EXISTS (SELECT 1 FROM harvest_workflow_executions x
                    WHERE x.id = NEW.workflow_exec_id
                      FOR KEY SHARE)
    ON CONFLICT (event_row_id) DO NOTHING;
    RETURN NULL;
END
$harvest_external_outbox_body$
$harvest_external_outbox_fn$, current_schema());
END
$harvest_external_outbox$;

COMMENT ON FUNCTION harvest_external_outbox_project() IS
    'Writes harvest_external_outbox_pending from an external request or resolution event.';
