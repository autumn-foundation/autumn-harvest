-- Candidate: batched seek-and-refine. Each batch is ONE round trip: fetch
-- the next `batch_size` ordered candidates via idx_harvest_tq_poll (no
-- residual predicate in the scan itself, `candidates` forced MATERIALIZED
-- so it is computed once and reused for both the winner pick and the
-- keyset cursor below, not rescanned per reference), scope the concurrency
-- recheck CTE to only the distinct keys present in *this batch*, and pick
-- the first eligible row -- returning, in the same query, the
-- (priority, scheduled_at) of the batch's own last row so the caller can
-- seek past it on a second batch without a second round trip to compute
-- that cursor. Only fetches a second batch if the whole first one is
-- exhausted with no eligible row.
--
-- Mixed sort directions (priority DESC, scheduled_at ASC) mean the keyset
-- predicate is an explicit OR, not a single row-comparison `<` -- a plain
-- `(priority, scheduled_at) < (cursor)` only works when every column in the
-- key shares one sort direction.
CREATE OR REPLACE FUNCTION claim_batched(
    queues text[],
    batch_size int,
    max_batches int DEFAULT 50
) RETURNS TABLE(claimed_id bigint, batches int) AS $$
DECLARE
    won_id bigint;
    batch_last_priority int;
    batch_last_scheduled_at timestamptz;
    cur_priority int;
    cur_scheduled_at timestamptz;
    have_cursor boolean := false;
    row_count int;
    b int := 0;
BEGIN
    LOOP
        b := b + 1;

        WITH candidates AS MATERIALIZED (
            SELECT id, task_type, concurrency_key, concurrency_cap, priority, scheduled_at
            FROM harvest_task_queue
            WHERE queue_name = ANY(queues)
              AND state = 'PENDING'
              AND scheduled_at <= NOW()
              AND (NOT have_cursor
                   OR priority < cur_priority
                   OR (priority = cur_priority AND scheduled_at > cur_scheduled_at))
            ORDER BY priority DESC, scheduled_at ASC
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        batch_keys AS MATERIALIZED (
            SELECT DISTINCT concurrency_key, task_type FROM candidates
            WHERE concurrency_key IS NOT NULL AND concurrency_cap IS NOT NULL
        ),
        running_counts AS MATERIALIZED (
            SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
            FROM harvest_task_queue t
            WHERE t.state = 'RUNNING'
              AND t.worker_id IS NOT NULL
              AND (t.concurrency_key, t.task_type) IN (SELECT concurrency_key, task_type FROM batch_keys)
            GROUP BY t.concurrency_key, t.task_type
        ),
        winner AS (
            SELECT c.id
            FROM candidates c
            WHERE c.concurrency_key IS NULL OR c.concurrency_cap IS NULL
               OR COALESCE((SELECT rc.running_count FROM running_counts rc
                             WHERE rc.concurrency_key = c.concurrency_key AND rc.task_type = c.task_type), 0)
                  < c.concurrency_cap
            ORDER BY c.priority DESC, c.scheduled_at ASC
            LIMIT 1
        ),
        batch_end AS (
            SELECT priority, scheduled_at FROM candidates
            ORDER BY priority ASC, scheduled_at DESC
            LIMIT 1
        )
        SELECT (SELECT id FROM winner), (SELECT priority FROM batch_end), (SELECT scheduled_at FROM batch_end),
               (SELECT count(*) FROM candidates)
        INTO won_id, batch_last_priority, batch_last_scheduled_at, row_count;

        IF won_id IS NOT NULL THEN
            UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'bench-worker-0'
            WHERE id = won_id;
            RETURN QUERY SELECT won_id, b;
            RETURN;
        END IF;

        IF row_count = 0 OR b >= max_batches THEN
            -- This batch came back empty (backlog exhausted) or bailout hit.
            RETURN QUERY SELECT NULL::bigint, b;
            RETURN;
        END IF;

        cur_priority := batch_last_priority;
        cur_scheduled_at := batch_last_scheduled_at;
        have_cursor := true;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
