-- The DLQ depth counter.
--
-- The `harvest.dlq.entries` gauge read `COUNT(*)` of `harvest_dead_letters`.
-- That read scans the whole DLQ, and a DLQ is largest in an incident. Now the
-- gauge reads `harvest_dead_letter_depth()`, the sum of
-- `harvest_dead_letter_counts.entries`.
--
-- ## Delta rows
--
-- Each `INSERT` and `DELETE` statement on `harvest_dead_letters` inserts one
-- delta row: its row count, negative for a delete. The row commits or rolls
-- back with the statement, so the sum is the exact committed count. A
-- `TRUNCATE` truncates the counter too. An `UPDATE` changes no count.
--
-- A writer only inserts. It reads no counter row and locks none. So a writer
-- never waits for another writer, and it cannot raise a serialization failure
-- at `REPEATABLE READ` or `SERIALIZABLE`. A writer also never reads a dead row
-- version, so a long transaction elsewhere does not slow it.
--
-- ## The fold
--
-- `harvest_dead_letter_fold()` deletes each delta row that no open
-- transaction holds, and inserts their sum as one row. The timeout pass of
-- each shard calls it once per tick, in its own `READ COMMITTED`
-- transaction. So the gauge reads the delta rows of about one tick. A fold
-- scans the whole counter, so no writer runs it.
--
-- The fold takes `ROW EXCLUSIVE` on `harvest_dead_letters`, as a writer does.
-- So it cannot run during a `TRUNCATE` or a recount, which would miss the row
-- that it inserts.
--
-- ## Privileges
--
-- The trigger function, the fold and the depth read are `SECURITY DEFINER`.
-- So a role that writes or reads dead letters needs no grant on the counter
-- table, and an older binary keeps working after this migration. Each function
-- pins `search_path` with `pg_temp` last, so a temporary table cannot shadow
-- the counter. `harvest_dead_letter_recount()` is for the table owner only.
--
-- ## Lock
--
-- `LOCK TABLE` takes `SHARE ROW EXCLUSIVE` on `harvest_dead_letters`. It waits
-- for each open dead-letter write, and then blocks new ones until the commit.
-- So no write falls between the seed count and the triggers. The seed reads
-- the whole DLQ once. `lock_timeout` bounds the wait for the lock. When it
-- fires, run the migration again.
SET LOCAL lock_timeout = '5s';

-- A `DO` block, because the test bundle runs this file in autocommit mode,
-- where a bare `LOCK TABLE` is an error. In the migration transaction, the
-- lock holds until the commit.
DO $harvest_dead_letter_counts_lock$
BEGIN
    LOCK TABLE harvest_dead_letters IN SHARE ROW EXCLUSIVE MODE;
END
$harvest_dead_letter_counts_lock$;

-- `slot` is a random key, so an insert never waits on a unique check, and no
-- sequence must follow a promotion. The primary key also gives logical
-- replication its replica identity.
CREATE TABLE harvest_dead_letter_counts (
    slot    UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
    entries BIGINT NOT NULL
);

COMMENT ON TABLE harvest_dead_letter_counts IS
    'The sum of entries is the row count of harvest_dead_letters. Triggers on that table keep it exact.';

DO $harvest_dead_letter_counts$
BEGIN
    EXECUTE pg_catalog.format($harvest_dead_letter_depth_fn$
CREATE FUNCTION harvest_dead_letter_depth()
RETURNS BIGINT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = %I, pg_catalog, pg_temp
AS $harvest_dead_letter_depth_body$
    SELECT COALESCE(SUM(entries), 0)::BIGINT FROM harvest_dead_letter_counts
$harvest_dead_letter_depth_body$
$harvest_dead_letter_depth_fn$, current_schema());

    EXECUTE pg_catalog.format($harvest_dead_letter_fold_fn$
CREATE FUNCTION harvest_dead_letter_fold()
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = %I, pg_catalog, pg_temp
AS $harvest_dead_letter_fold_body$
DECLARE
    removed BIGINT;
    total BIGINT;
BEGIN
    -- A recount and a truncate take a stronger lock, so they wait for the fold.
    LOCK TABLE harvest_dead_letters IN ROW EXCLUSIVE MODE;
    IF (SELECT count(*) FROM harvest_dead_letter_counts) < 2 THEN
        RETURN 0;
    END IF;
    WITH gone AS (
        DELETE FROM harvest_dead_letter_counts
         WHERE slot IN (SELECT slot FROM harvest_dead_letter_counts
                        FOR UPDATE SKIP LOCKED)
        RETURNING entries
    )
    SELECT count(*), COALESCE(sum(entries), 0) INTO removed, total FROM gone;
    IF removed > 0 THEN
        INSERT INTO harvest_dead_letter_counts (entries) VALUES (total);
    END IF;
    RETURN removed;
END
$harvest_dead_letter_fold_body$
$harvest_dead_letter_fold_fn$, current_schema());

    EXECUTE pg_catalog.format($harvest_dead_letter_count_apply_fn$
CREATE FUNCTION harvest_dead_letter_count_apply()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = %I, pg_catalog, pg_temp
AS $harvest_dead_letter_count_apply_body$
DECLARE
    delta BIGINT;
BEGIN
    -- `TRUNCATE` removes each dead letter, also one that committed after the
    -- snapshot of this transaction. A `DELETE` would keep the delta row of
    -- such a letter, so the reset is a `TRUNCATE` too. The truncate of the
    -- DLQ holds `ACCESS EXCLUSIVE`, so no writer or fold runs now.
    IF TG_OP = 'TRUNCATE' THEN
        BEGIN
            TRUNCATE harvest_dead_letter_counts;
        EXCEPTION WHEN object_in_use THEN
            -- The same statement truncates the counter, so its rows are gone.
            DELETE FROM harvest_dead_letter_counts;
        END;
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
    -- No fold here. A fold scans the whole counter, and a long snapshot
    -- elsewhere keeps its dead rows. The timeout pass folds instead.
    INSERT INTO harvest_dead_letter_counts (entries) VALUES (delta);
    RETURN NULL;
END
$harvest_dead_letter_count_apply_body$
$harvest_dead_letter_count_apply_fn$, current_schema());

    EXECUTE pg_catalog.format($harvest_dead_letter_recount_fn$
CREATE FUNCTION harvest_dead_letter_recount()
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = %I, pg_catalog, pg_temp
AS $harvest_dead_letter_recount_body$
DECLARE
    total BIGINT;
BEGIN
    -- At a higher level the count would read the snapshot from before the
    -- lock. It would then miss each row that committed in the wait.
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'harvest_dead_letter_recount() needs READ COMMITTED, not %%',
            current_setting('transaction_isolation');
    END IF;
    -- Wait for each open dead-letter write and fold, then block new ones until
    -- the commit. Each writer locks this table before the counter.
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

-- The recount locks the DLQ against writes, so only the owner runs it.
REVOKE ALL ON FUNCTION harvest_dead_letter_recount() FROM PUBLIC;

COMMENT ON FUNCTION harvest_dead_letter_depth() IS
    'The row count of harvest_dead_letters, read from harvest_dead_letter_counts.';
COMMENT ON FUNCTION harvest_dead_letter_fold() IS
    'Folds the free delta rows of harvest_dead_letter_counts into one row. Returns the rows removed.';
COMMENT ON FUNCTION harvest_dead_letter_count_apply() IS
    'Inserts the row count of each INSERT or DELETE statement on harvest_dead_letters into harvest_dead_letter_counts.';
COMMENT ON FUNCTION harvest_dead_letter_recount() IS
    'Sets harvest_dead_letter_counts to the exact count of harvest_dead_letters in one row. Repairs a drift. Needs READ COMMITTED.';

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

-- The seed runs after the lock, so its snapshot sees every committed row.
INSERT INTO harvest_dead_letter_counts (entries)
SELECT count(*) FROM harvest_dead_letters;
