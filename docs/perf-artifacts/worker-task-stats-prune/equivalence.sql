-- Edge rows for the prune equivalence check. The harness binds $1 to a
-- cohort key and runs the public prune once with each fallback below.
--
-- Each row name says its age and its cohort's peer_stale_secs. The
-- retention is 3600 s. Ages keep 2 s from each threshold, so the gap
-- between this insert and the prune cannot move a row across one.
--   fast      peer_stale_secs 120
--   slow      peer_stale_secs 10800
--   nofield   no peer_stale_secs, so the fallback applies
--   null      peer_stale_secs is JSON null, so the fallback applies
--   negative  peer_stale_secs -50, clamped to 0
--   huge      peer_stale_secs 99999999999, clamped to one year
--   empty     an empty JSON object

INSERT INTO harvest_workers
    (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, max_concurrency,
     in_flight_count, host, status, build_id, labels)
SELECT id, NOW() - INTERVAL '1 day', NOW(), '["eq"]'::jsonb, '[0]'::jsonb, 1, 0, 'h',
       'Active', '', '{}'::jsonb
FROM unnest(ARRAY[
    'fast-0', 'fast-3598', 'fast-3602', 'fast-7200',
    'slow-3602', 'slow-10798', 'slow-10802',
    'nofield-3602', 'nofield-7198', 'nofield-7202',
    'null-3602', 'null-7202',
    'negative-3598', 'negative-3602',
    'huge-3602', 'huge-31535998', 'huge-31536002',
    'empty-3602', 'empty-7202'
]) AS id;

INSERT INTO harvest_worker_task_stats (worker_id, window_tasks, window_failures, cohort, updated_at)
SELECT worker_id, 1, 0,
       CASE split_part(worker_id, '-', 1)
           WHEN 'fast' THEN jsonb_set($1::jsonb, '{peer_stale_secs}', '120')::text
           WHEN 'slow' THEN jsonb_set($1::jsonb, '{peer_stale_secs}', '10800')::text
           WHEN 'nofield' THEN ($1::jsonb - 'peer_stale_secs')::text
           WHEN 'null' THEN jsonb_set($1::jsonb, '{peer_stale_secs}', 'null')::text
           WHEN 'negative' THEN jsonb_set($1::jsonb, '{peer_stale_secs}', '-50')::text
           WHEN 'huge' THEN jsonb_set($1::jsonb, '{peer_stale_secs}', '99999999999')::text
           ELSE '{}'
       END,
       NOW() - split_part(worker_id, '-', 2)::bigint * INTERVAL '1 second'
FROM harvest_workers WHERE queues = '["eq"]'::jsonb;
