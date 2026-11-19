-- The DLQ depth counter.
--
-- The `harvest.dlq.entries` gauge read `COUNT(*)` of `harvest_dead_letters`.
-- That read scans the whole DLQ, and a DLQ is largest in an incident. Now the
-- gauge reads the sum of `harvest_dead_letter_counts.entries`.
--
-- Triggers keep the sum exact. Each `INSERT` and `DELETE` statement on
-- `harvest_dead_letters` adds its row count to one counter row, in the same
-- transaction. A rollback undoes both. So a reader sees the exact committed
-- count. A `TRUNCATE` empties the counter. An `UPDATE` changes no count.
--
-- ## No writer waits for the counter
--
-- A writer locks a free counter row with `FOR UPDATE SKIP LOCKED`. When every
-- row is locked by another open transaction, the writer inserts a new row.
-- So the counter adds no lock wait to a failure transaction, and so it cannot
-- close a deadlock with the execution row locks of that transaction.
--
-- A writer adds a row only when all rows are locked by other transactions.
-- So the row count stays at or below the peak count of concurrent writers.
-- `harvest_dead_letter_recount()` folds all rows into one.
--
-- An `UPDATE` of a counter row in a `REPEATABLE READ` transaction can fail
-- with a serialization error. Every engine writer uses `READ COMMITTED`.
--
-- ## Grants
--
-- The trigger runs as the writer of the dead letter. A role that writes
-- dead letters needs `SELECT`, `INSERT` and `UPDATE` on the counter table.
-- A role that truncates the DLQ or runs the recount also needs `DELETE`.
--
-- ## Lock
--
-- `LOCK TABLE` takes `SHARE ROW EXCLUSIVE` on `harvest_dead_letters`. It waits
-- for each open dead-letter write, and then blocks new ones until the commit.
-- So no write falls between the seed count and the triggers. The seed reads
-- the whole DLQ once: about 150 ms at 1,000,000 rows. A failure transaction
-- that writes a dead letter waits for that time. `lock_timeout` bounds the
-- wait for the lock. When it fires, run the migration again.
SET LOCAL lock_timeout = '5s';

LOCK TABLE harvest_dead_letters IN SHARE ROW EXCLUSIVE MODE;

-- `slot` is a random key, so a new row never waits on a unique check. The
-- primary key also gives logical replication its replica identity.
CREATE TABLE harvest_dead_letter_counts (
    slot    UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
    entries BIGINT NOT NULL
);

COMMENT ON TABLE harvest_dead_letter_counts IS
    'The sum of entries is the row count of harvest_dead_letters. Triggers on that table keep it exact.';

-- `SET search_path` pins name resolution to the schema of Harvest, as
-- `harvest_entity_state_project` does. A session cannot shadow a name.
DO $harvest_dead_letter_counts$
BEGIN
    EXECUTE pg_catalog.format($harvest_dead_letter_count_apply_fn$
CREATE FUNCTION harvest_dead_letter_count_apply()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = %I, pg_catalog
AS $harvest_dead_letter_count_apply_body$
DECLARE
    delta BIGINT;
BEGIN
    -- A truncate holds `ACCESS EXCLUSIVE`, so no counter row is in use.
    IF TG_OP = 'TRUNCATE' THEN
        DELETE FROM harvest_dead_letter_counts;
        RETURN NULL;
    END IF;
    -- `changed` is the transition table of the statement.
    SELECT count(*) INTO delta FROM changed;
    IF delta = 0 THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        delta := -delta;
    END IF;
    -- Lock a free row, or a row that this transaction holds. Never wait.
    UPDATE harvest_dead_letter_counts
       SET entries = entries + delta
     WHERE slot = (SELECT slot FROM harvest_dead_letter_counts
                   LIMIT 1 FOR UPDATE SKIP LOCKED);
    IF NOT FOUND THEN
        INSERT INTO harvest_dead_letter_counts (entries) VALUES (delta);
    END IF;
    RETURN NULL;
END
$harvest_dead_letter_count_apply_body$
$harvest_dead_letter_count_apply_fn$, current_schema());

    EXECUTE pg_catalog.format($harvest_dead_letter_recount_fn$
CREATE FUNCTION harvest_dead_letter_recount()
RETURNS BIGINT
LANGUAGE plpgsql
SET search_path = %I, pg_catalog
AS $harvest_dead_letter_recount_body$
DECLARE
    total BIGINT;
BEGIN
    -- Wait for each open dead-letter write, then block new ones until the
    -- commit. A writer locks this table before the counter, so the order
    -- matches and the two cannot deadlock.
    LOCK TABLE harvest_dead_letters IN SHARE ROW EXCLUSIVE MODE;
    SELECT count(*) INTO total FROM harvest_dead_letters;
    DELETE FROM harvest_dead_letter_counts;
    INSERT INTO harvest_dead_letter_counts (entries) VALUES (total);
    RETURN total;
END
$harvest_dead_letter_recount_body$
$harvest_dead_letter_recount_fn$, current_schema());
END
$harvest_dead_letter_counts$;

COMMENT ON FUNCTION harvest_dead_letter_count_apply() IS
    'Adds the row count of each INSERT or DELETE statement on harvest_dead_letters to harvest_dead_letter_counts.';
COMMENT ON FUNCTION harvest_dead_letter_recount() IS
    'Sets harvest_dead_letter_counts to the exact count of harvest_dead_letters and folds it into one row. Repairs a drift.';

CREATE TRIGGER harvest_dead_letter_count_insert_trg
    AFTER INSERT ON harvest_dead_letters
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT
    EXECUTE FUNCTION harvest_dead_letter_count_apply();

CREATE TRIGGER harvest_dead_letter_count_delete_trg
    AFTER DELETE ON harvest_dead_letters
    REFERENCING OLD TABLE AS changed
    FOR EACH STATEMENT
    EXECUTE FUNCTION harvest_dead_letter_count_apply();

CREATE TRIGGER harvest_dead_letter_count_truncate_trg
    AFTER TRUNCATE ON harvest_dead_letters
    FOR EACH STATEMENT
    EXECUTE FUNCTION harvest_dead_letter_count_apply();

SELECT harvest_dead_letter_recount();
