-- Candidate: batched seek-and-refine. Each batch is one server-side fetch
-- (up to `batch_size` ordered candidates, one query, `FOR UPDATE SKIP
-- LOCKED`), then an in-memory procedural walk over that already-fetched,
-- already-locked batch -- no further table scans, only a per-candidate
-- advisory-lock-and-recheck exactly like the production path's.
--
-- NOTE (post-review, Codex, round 2): the first version of this function
-- picked its winner from a single batch-wide snapshot CTE
-- (`running_counts`, computed once from a pre-claim read) instead of the
-- production path's per-candidate `pg_try_advisory_xact_lock` +
-- fresh-COUNT recheck (`autumn-harvest/src/queue.rs:750-770`). That is a
-- genuine correctness gap, not just an unmeasured-performance one: two
-- concurrent callers landing on different candidates that share a
-- concurrency key could both read the same stale count and both commit,
-- exceeding the cap -- exactly what the advisory lock exists to prevent.
-- Ledger #4's `claim_deferred()` already had this right; this version now
-- matches it, applied per-candidate *within* each fetched batch instead of
-- across the whole backlog, so the batch-fetch property (one scan per up
-- to `batch_size` candidates) is kept alongside the authoritative check.
--
-- Mixed sort directions (priority DESC, scheduled_at ASC) mean the keyset
-- predicate is an explicit OR, not a single row-comparison `<` -- a plain
-- `(priority, scheduled_at, id) < (cursor)` only works when every column in
-- the key shares one sort direction. `id` is included as a third keyset
-- column (and as a third ORDER BY key) because `priority`/`scheduled_at`
-- alone are not unique: `seed.sql`/`seed_adversarial_50.sql` assign one
-- statement-stable `NOW()` value to many rows, so ties are the common case,
-- not an edge case (see the report's post-review section for what this
-- costs and a direct test showing it is not, on its own, what forces the
-- `Sort` node the natural/forced-index plans both show).
CREATE OR REPLACE FUNCTION claim_batched(
    queues text[],
    batch_size int,
    max_batches int DEFAULT 50
) RETURNS TABLE(claimed_id bigint, batches int) AS $$
DECLARE
    cand RECORD;
    running_ct bigint;
    cur_priority int;
    cur_scheduled_at timestamptz;
    cur_id bigint;
    have_cursor boolean := false;
    n_in_batch int;
    b int := 0;
BEGIN
    LOOP
        b := b + 1;
        n_in_batch := 0;

        FOR cand IN
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
        LOOP
            n_in_batch := n_in_batch + 1;
            cur_priority := cand.priority;
            cur_scheduled_at := cand.scheduled_at;
            cur_id := cand.id;

            IF cand.concurrency_key IS NULL OR cand.concurrency_cap IS NULL THEN
                UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'bench-worker-0'
                WHERE id = cand.id;
                RETURN QUERY SELECT cand.id, b;
                RETURN;
            END IF;

            IF pg_try_advisory_xact_lock(hashtext(cand.concurrency_key)::bigint) THEN
                SELECT COUNT(*) INTO running_ct
                FROM harvest_task_queue recheck
                WHERE recheck.concurrency_key = cand.concurrency_key
                  AND recheck.task_type = cand.task_type
                  AND recheck.state = 'RUNNING'
                  AND recheck.worker_id IS NOT NULL;

                IF running_ct < cand.concurrency_cap THEN
                    UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'bench-worker-0'
                    WHERE id = cand.id;
                    RETURN QUERY SELECT cand.id, b;
                    RETURN;
                END IF;
            END IF;
            -- Lock unavailable (another session is deciding this key right
            -- now) or the fresh count is already at cap: move on to the
            -- next candidate already fetched in this same batch -- no new
            -- query against harvest_task_queue's full backlog either way.
        END LOOP;

        have_cursor := true;

        IF n_in_batch = 0 OR b >= max_batches THEN
            -- This batch came back empty (backlog exhausted) or bailout hit.
            RETURN QUERY SELECT NULL::bigint, b;
            RETURN;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
