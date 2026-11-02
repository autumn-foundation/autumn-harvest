-- Revert issue #2401. Drop the three DLQ indexes.
--
-- After this revert, the DLQ list, the callback redrive delete, each queue
-- filter and the newest-age probe read the whole DLQ again.
SET LOCAL lock_timeout = '5s';

DROP INDEX IF EXISTS idx_harvest_dl_failed_at;

DROP INDEX IF EXISTS idx_harvest_dl_original_task;

DROP INDEX IF EXISTS idx_harvest_dl_queue_failed;
