-- Candidate: batched seek-and-refine. Each batch is one server-side step:
-- fetch the next `batch_size` ordered candidates (`candidates` forced
-- MATERIALIZED so it is computed once and reused for both the winner pick
-- and the keyset cursor below, not rescanned per reference), scope the
-- concurrency recheck CTE to only the distinct keys present in *this
-- batch*, and pick the first eligible row -- returning, in the same query,
-- the (priority, scheduled_at, id) of the batch's own last row so the
-- caller can seek past it on a second batch without a second query to
-- compute that cursor. Only fetches a second batch if the whole first one
-- is exhausted with no eligible row.
--
-- NOTE (post-review, Codex): the whole multi-batch loop below runs inside
-- this one plpgsql function, called once via `SELECT * FROM
-- claim_batched(...)`. That means a 2- or 5-batch resolution in this
-- apparatus costs one client/database round trip total, not N -- this
-- measures each batch's own server-side query cost, not the network-latency
-- cost a real caller issuing one batch per round trip would pay. See the
-- report's Assay section for what this does and does not establish.
--
-- Mixed sort directions (priority DESC, scheduled_at ASC) mean the keyset
-- predicate is an explicit OR, not a single row-comparison `<` -- a plain
-- `(priority, scheduled_at, id) < (cursor)` only works when every column in
-- the key shares one sort direction. `id` is included as a third keyset
-- column (and as a third ORDER BY key) because `priority`/`scheduled_at`
-- alone are not unique: `seed.sql`/`seed_adversarial_50.sql` assign one
-- statement-stable `NOW()` value to many rows, so ties are the common case,
-- not an edge case. Without a unique tiebreaker, a batch boundary landing
-- inside a tied group would make the next batch's strict `>` predicate
-- silently exclude every unvisited row in that tie -- caught in post-review
-- (Codex): this apparatus's own L4 fixture only avoided tripping it because
-- `batch_size` (50) happened to equal the tied group's size (50) exactly.
CREATE OR REPLACE FUNCTION claim_batched(
    queues text[],
    batch_size int,
    max_batches int DEFAULT 50
) RETURNS TABLE(claimed_id bigint, batches int) AS $$
DECLARE
    won_id bigint;
    batch_last_priority int;
    batch_last_scheduled_at timestamptz;
    batch_last_id bigint;
    cur_priority int;
    cur_scheduled_at timestamptz;
    cur_id bigint;
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
                   OR (priority = cur_priority AND scheduled_at > cur_scheduled_at)
                   OR (priority = cur_priority AND scheduled_at = cur_scheduled_at AND id > cur_id))
            ORDER BY priority DESC, scheduled_at ASC, id ASC
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
            ORDER BY c.priority DESC, c.scheduled_at ASC, c.id ASC
            LIMIT 1
        ),
        batch_end AS (
            SELECT priority, scheduled_at, id FROM candidates
            ORDER BY priority ASC, scheduled_at DESC, id DESC
            LIMIT 1
        )
        SELECT (SELECT id FROM winner),
               (SELECT priority FROM batch_end), (SELECT scheduled_at FROM batch_end), (SELECT id FROM batch_end),
               (SELECT count(*) FROM candidates)
        INTO won_id, batch_last_priority, batch_last_scheduled_at, batch_last_id, row_count;

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
        cur_id := batch_last_id;
        have_cursor := true;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
