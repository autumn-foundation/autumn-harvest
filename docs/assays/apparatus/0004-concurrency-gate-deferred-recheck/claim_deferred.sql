-- Candidate: the deferred-recheck shape under test. No concurrency
-- predicate in candidate selection at all; the cap is enforced only in the
-- authoritative recheck (ported by value from queue.rs:750-778's `claimed`
-- CTE), retried against the next-highest-priority row on a failed recheck.
--
-- Same-transaction SELECT ... FOR UPDATE does not get skipped by a later
-- SKIP LOCKED in the same transaction (you already hold your own lock), so
-- `tried` explicitly excludes already-attempted ids -- without it this loop
-- would re-select the same blocked row forever.
CREATE OR REPLACE FUNCTION claim_deferred(
    queues text[],
    max_attempts int DEFAULT 200
) RETURNS TABLE(claimed_id bigint, attempts int) AS $$
DECLARE
    cand RECORD;
    tried bigint[] := ARRAY[]::bigint[];
    running_ct bigint;
    n int := 0;
BEGIN
    LOOP
        n := n + 1;

        SELECT id, task_type, concurrency_key, concurrency_cap
        INTO cand
        FROM harvest_task_queue
        WHERE queue_name = ANY(queues)
          AND state = 'PENDING'
          AND scheduled_at <= NOW()
          AND NOT (id = ANY(tried))
        ORDER BY priority DESC, scheduled_at ASC
        LIMIT 1 FOR UPDATE SKIP LOCKED;

        IF NOT FOUND THEN
            RETURN QUERY SELECT NULL::bigint, n;
            RETURN;
        END IF;

        IF cand.concurrency_key IS NULL OR cand.concurrency_cap IS NULL THEN
            UPDATE harvest_task_queue
            SET state = 'RUNNING', worker_id = 'bench-worker-0'
            WHERE id = cand.id;
            RETURN QUERY SELECT cand.id, n;
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
                UPDATE harvest_task_queue
                SET state = 'RUNNING', worker_id = 'bench-worker-0'
                WHERE id = cand.id;
                RETURN QUERY SELECT cand.id, n;
                RETURN;
            END IF;
        END IF;

        tried := tried || cand.id;
        IF n >= max_attempts THEN
            RETURN QUERY SELECT NULL::bigint, n;
            RETURN;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
