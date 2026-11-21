-- The worker fleet that the measured workers heartbeat beside.
--
-- The harness replaces __FLEET__ with the live fleet size and binds $1 to the
-- cohort key of a measured worker. Each fleet key is that key with its
-- build_id changed, so every key has the width of a real one.
--
-- Shape, for a fleet of F:
--   F       live workers, heartbeat 0-4 s old, 8 builds (1 in 8 shares the
--           measured cohort), 2% in a slow cohort with peer_stale_secs 10800
--   0.30 F  orphans of restarted pods with random ids, 1-59 min old
--   0.02 F  rows past the one-hour retention, half of them in the slow cohort
-- Every live stats row is then written once more, as one heartbeat round
-- does. updated_at is indexed, so each write is a non-HOT update, and about
-- half of the heap is dead tuples.
--
-- Autovacuum is off on the two tables for the fixture only. A vacuum during
-- the window would change the dead-tuple ratio and the buffer counts.

ALTER TABLE harvest_workers SET (autovacuum_enabled = false);

ALTER TABLE harvest_worker_task_stats SET (autovacuum_enabled = false);

INSERT INTO harvest_workers
    (worker_id, started_at, last_heartbeat_at, queues, shard_assignments, max_concurrency,
     in_flight_count, host, version, status, build_id, labels)
SELECT id, NOW() - INTERVAL '3 hours', NOW() - age, '["task-stats-prune"]'::jsonb, '[0]'::jsonb,
       70, (g % 7), 'pod-' || md5(id), '0.5.0', 'Active', 'build-' || (g % 8), '{}'::jsonb
FROM (
    SELECT g, 'fleet-' || g AS id, (g % 5) * INTERVAL '1 second' AS age
    FROM generate_series(1, __FLEET__) g
    UNION ALL
    SELECT g, 'orphan-' || md5(g::text), (60 + (g * 37) % 3480) * INTERVAL '1 second'
    FROM generate_series(1, (__FLEET__ * 3) / 10) g
    UNION ALL
    SELECT g, 'expired-' || md5(g::text), (3700 + (g * 53) % 3600) * INTERVAL '1 second'
    FROM generate_series(1, GREATEST(__FLEET__ / 50, 2)) g
) w;

INSERT INTO harvest_worker_task_stats
    (worker_id, window_tasks, window_failures, p99_latency_ms, snapshot_seq, cohort, updated_at)
SELECT w.worker_id, 40 + (g % 60), (g % 3), 20 + (g % 400), 1,
       CASE
           WHEN g % 8 = 0 THEN $1
           WHEN g % 50 = 7 OR (w.worker_id LIKE 'expired-%' AND g % 2 = 0)
               THEN jsonb_set(jsonb_set($1::jsonb, '{build_id}', to_jsonb(w.build_id)),
                              '{peer_stale_secs}', '10800')::text
           ELSE jsonb_set($1::jsonb, '{build_id}', to_jsonb(w.build_id))::text
       END,
       w.last_heartbeat_at
FROM (
    SELECT worker_id, build_id, last_heartbeat_at,
           row_number() OVER (ORDER BY worker_id) AS g
    FROM harvest_workers WHERE worker_id NOT LIKE 'measured-%'
) w;

UPDATE harvest_worker_task_stats
SET snapshot_seq = snapshot_seq + 1, updated_at = updated_at + INTERVAL '1 millisecond'
WHERE worker_id LIKE 'fleet-%';

ANALYZE harvest_workers;

ANALYZE harvest_worker_task_stats;
