-- Revert: drop the vacated-row linkage (issue #1596 review, follow-up).
--
-- Safe once no in-flight migration record still needs it. A record whose
-- `phase` has reached `DONE` or `ABORTED` never reads this column again;
-- dropping it there only loses historical detail, not correctness. A
-- record still in flight (`PENDING` through `COMMITTED`) can be a
-- force-terminated target awaiting a delayed `activate_target` retry,
-- and that retry needs this column to finalize the right row's vacate
-- marker without guessing. Refuse instead of stranding it.
DO $$
DECLARE
    in_flight_count integer;
BEGIN
    SELECT count(*) INTO in_flight_count
      FROM harvest_shard_migrations
     WHERE vacated_execution_id IS NOT NULL
       AND phase NOT IN ('DONE', 'ABORTED');
    IF in_flight_count > 0 THEN
        RAISE EXCEPTION
            'cannot roll back 20260920010144_harvest_shard_migrations_vacated_execution_id: '
            '% in-flight migration record(s) still need this column to finalize '
            'a vacated row precisely. Let them finish or abort first, then retry.',
            in_flight_count;
    END IF;
END $$;

ALTER TABLE harvest_shard_migrations
    DROP COLUMN IF EXISTS vacated_execution_id;
