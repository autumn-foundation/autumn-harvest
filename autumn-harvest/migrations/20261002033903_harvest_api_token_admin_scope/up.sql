-- Admin scope for API tokens (issue #1803).
--
-- An `admin` token can do all that a `mutate` token can do. It alone can mint
-- and revoke tokens. Before this change a `mutate` token could mint more
-- tokens, so one leaked token could create others.
--
-- Config state only: no `WorkflowEvent` variant, no change to
-- `harvest_events`, no replay impact. Existing rows keep their scope.
ALTER TABLE harvest_api_tokens
    DROP CONSTRAINT IF EXISTS harvest_api_tokens_scope_check;
ALTER TABLE harvest_api_tokens
    ADD CONSTRAINT harvest_api_tokens_scope_check
    CHECK (scope IN ('read', 'mutate', 'admin'));
