-- Removes the DLQ depth counter. Deploy the older release first, because
-- this release reads `harvest_dead_letter_counts`.
SET LOCAL lock_timeout = '5s';

DROP TRIGGER IF EXISTS harvest_dead_letter_count_truncate_trg ON harvest_dead_letters;
DROP TRIGGER IF EXISTS harvest_dead_letter_count_delete_trg ON harvest_dead_letters;
DROP TRIGGER IF EXISTS harvest_dead_letter_count_insert_trg ON harvest_dead_letters;
DROP FUNCTION IF EXISTS harvest_dead_letter_recount();
DROP FUNCTION IF EXISTS harvest_dead_letter_count_apply();
DROP TABLE IF EXISTS harvest_dead_letter_counts;
