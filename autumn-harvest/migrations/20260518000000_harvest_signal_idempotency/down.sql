DROP INDEX IF EXISTS uq_harvest_signals_idem;
ALTER TABLE harvest_signals DROP COLUMN IF EXISTS idempotency_key;
