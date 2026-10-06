-- Publisher signature on stored WASM modules (issue #1838).
--
-- Lowercase hex of an Ed25519 signature over the activity name and module
-- hash. NULL means unsigned. A worker with no trust policy ignores the column.
-- A worker with a trust policy refuses to run a module whose signature is
-- NULL or does not verify.
--
-- No `WorkflowEvent` variant, no change to `harvest_events`, no replay impact.
--
-- A nullable column with no default needs no table rewrite. ADD COLUMN takes
-- an ACCESS EXCLUSIVE lock for a moment. A held lock fails the migration after
-- 5 s, so dispatch lookups do not queue behind it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_wasm_modules
    ADD COLUMN IF NOT EXISTS signature TEXT;

COMMENT ON COLUMN harvest_wasm_modules.signature IS
    'Hex Ed25519 publisher signature over the activity name and hash '
    '(issue #1838). NULL means unsigned.';
