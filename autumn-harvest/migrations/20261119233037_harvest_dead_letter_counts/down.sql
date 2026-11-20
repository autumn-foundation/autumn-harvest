-- Removes the DLQ depth counter. Deploy the older release first, because
-- this release reads `harvest_dead_letter_depth()`.
--
-- `DROP TRIGGER` takes `ACCESS EXCLUSIVE` on `harvest_dead_letters` for a
-- short time. Each read and write of the DLQ queues behind it while it waits.
SET LOCAL lock_timeout = '5s';

DROP TRIGGER IF EXISTS harvest_dead_letter_count_truncate_trg ON harvest_dead_letters;
DROP TRIGGER IF EXISTS harvest_dead_letter_count_delete_trg ON harvest_dead_letters;
DROP TRIGGER IF EXISTS harvest_dead_letter_count_insert_trg ON harvest_dead_letters;
DROP FUNCTION IF EXISTS harvest_dead_letter_recount();
DROP FUNCTION IF EXISTS harvest_dead_letter_count_apply();
DROP FUNCTION IF EXISTS harvest_dead_letter_fold();
DROP FUNCTION IF EXISTS harvest_dead_letter_depth();
DROP TABLE IF EXISTS harvest_dead_letter_counts;
