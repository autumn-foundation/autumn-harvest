SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_wasm_modules DROP COLUMN IF EXISTS signature;
