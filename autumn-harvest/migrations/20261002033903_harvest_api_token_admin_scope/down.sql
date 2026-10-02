-- Revert the admin scope (issue #1803).
--
-- The old CHECK rejects `admin`, so narrow those rows to `mutate` first.
-- Under the old code a `mutate` token can mint tokens, so no admin token
-- loses access to the token routes.
UPDATE harvest_api_tokens SET scope = 'mutate' WHERE scope = 'admin';
ALTER TABLE harvest_api_tokens
    DROP CONSTRAINT IF EXISTS harvest_api_tokens_scope_check;
ALTER TABLE harvest_api_tokens
    ADD CONSTRAINT harvest_api_tokens_scope_check
    CHECK (scope IN ('read', 'mutate'));
