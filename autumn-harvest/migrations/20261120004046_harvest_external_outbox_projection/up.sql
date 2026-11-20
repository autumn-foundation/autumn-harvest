-- The trigger and the backfill of `harvest_external_outbox_pending`.
--
-- The previous migration adds the table and the function. This one installs
-- the trigger on `harvest_events`, then inserts a row for each unresolved
-- request.
--
-- `idx_harvest_events_external_outbox_pending` stays. A worker of the
-- previous release still claims through it during a rolling deploy.
--
-- ## Lock cost
--
-- `CREATE TRIGGER` takes `SHARE ROW EXCLUSIVE` on `harvest_events` until the
-- commit. So no event insert runs while the backfill reads, and the backfill
-- sees each committed request. Event appends wait for the backfill. It reads
-- each request once: 1.1 to 1.4 s for 200,000 requests in measured runs. This
-- migration locks no other hot table, so it cannot deadlock a transaction
-- that appends an event and then updates its run. A held lock fails the
-- migration after 5 s, and the operator runs it again.
--
-- Count the requests first, to size the wait:
--
--   SELECT count(*) FROM harvest_events
--    WHERE event_type IN ('ExternalSignalRequested',
--                         'ExternalCancelRequested',
--                         'ExternalAwaitRequested');
SET LOCAL lock_timeout = '5s';

-- The `WHEN` clause runs in the executor, so every other event skips the
-- function call. The partition layout changes reinstall this trigger with
-- the append-only guard.
CREATE TRIGGER harvest_external_outbox_projection_trg
    AFTER INSERT ON harvest_events
    FOR EACH ROW
    WHEN (NEW.event_type IN ('ExternalSignalRequested',
                             'ExternalSignalDelivered',
                             'ExternalSignalFailed',
                             'ExternalCancelRequested',
                             'ExternalCancelDelivered',
                             'ExternalCancelFailed',
                             'ExternalAwaitRequested',
                             'ExternalAwaitResolved',
                             'ExternalAwaitFailed'))
    EXECUTE FUNCTION harvest_external_outbox_project();

-- The backfill. The trigger above already holds its lock, so no request
-- commits between this read and the commit.
INSERT INTO harvest_external_outbox_pending
    (event_row_id, workflow_exec_id, request_type, correlation_id, requested_at)
SELECT e.id, e.workflow_exec_id, e.event_type, e.event_data -> 'data' ->> 'signal_id', e.timestamp
  FROM harvest_events e
 WHERE e.event_type = 'ExternalSignalRequested'
   AND (e.event_data -> 'data' ->> 'signal_id') IS NOT NULL
   AND EXISTS (SELECT 1 FROM harvest_workflow_executions x WHERE x.id = e.workflow_exec_id)
   AND NOT EXISTS (
       SELECT 1 FROM harvest_events res
        WHERE res.workflow_exec_id = e.workflow_exec_id
          AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
          AND res.event_data -> 'data' ->> 'signal_id' = e.event_data -> 'data' ->> 'signal_id')
UNION ALL
SELECT e.id, e.workflow_exec_id, e.event_type, e.event_data -> 'data' ->> 'cancel_id', e.timestamp
  FROM harvest_events e
 WHERE e.event_type = 'ExternalCancelRequested'
   AND (e.event_data -> 'data' ->> 'cancel_id') IS NOT NULL
   AND EXISTS (SELECT 1 FROM harvest_workflow_executions x WHERE x.id = e.workflow_exec_id)
   AND NOT EXISTS (
       SELECT 1 FROM harvest_events res
        WHERE res.workflow_exec_id = e.workflow_exec_id
          AND res.event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed')
          AND res.event_data -> 'data' ->> 'cancel_id' = e.event_data -> 'data' ->> 'cancel_id')
UNION ALL
SELECT e.id, e.workflow_exec_id, e.event_type, e.event_data -> 'data' ->> 'await_id', e.timestamp
  FROM harvest_events e
 WHERE e.event_type = 'ExternalAwaitRequested'
   AND (e.event_data -> 'data' ->> 'await_id') IS NOT NULL
   AND EXISTS (SELECT 1 FROM harvest_workflow_executions x WHERE x.id = e.workflow_exec_id)
   AND NOT EXISTS (
       SELECT 1 FROM harvest_events res
        WHERE res.workflow_exec_id = e.workflow_exec_id
          AND res.event_type IN ('ExternalAwaitResolved', 'ExternalAwaitFailed')
          AND res.event_data -> 'data' ->> 'await_id' = e.event_data -> 'data' ->> 'await_id')
ON CONFLICT (event_row_id) DO NOTHING;
