-- The operator policies of each agent kind (issue #2439, epic #2438).
--
-- `harvest_agent_policies` holds one row per kind and policy name. The
-- sampling policy of #2439 is `sampling`. The refine policy of #2394 goes
-- in the same table, so the name has no CHECK.
--
-- `body` is the JSON of the policy. A reset stores NULL, so every setting
-- reads its default. `revision` counts each set and reset of the row, from
-- 1. A reset keeps the row, so the revision never goes back.
--
-- `harvest_agent_policy_audit` holds one row for each set and reset, with
-- the old and the new body. The store writes the change and its audit row
-- in one transaction, so no change goes unaudited.
--
-- The tables are new and not hot. No other session holds a lock on them.
-- The timeout bounds the DDL all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_agent_policies (
    kind       TEXT        NOT NULL CHECK (kind <> ''),
    policy     TEXT        NOT NULL CHECK (policy <> ''),
    body       JSONB,
    revision   BIGINT      NOT NULL CHECK (revision >= 1),
    updated_by TEXT        NOT NULL CHECK (updated_by <> ''),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, policy)
);

CREATE TABLE harvest_agent_policy_audit (
    id       BIGSERIAL   PRIMARY KEY,
    kind     TEXT        NOT NULL CHECK (kind <> ''),
    policy   TEXT        NOT NULL CHECK (policy <> ''),
    revision BIGINT      NOT NULL CHECK (revision >= 1),
    action   TEXT        NOT NULL CHECK (action IN ('set', 'reset')),
    actor    TEXT        NOT NULL CHECK (actor <> ''),
    old_body JSONB,
    new_body JSONB,
    at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- `show` reads the newest audit rows of one kind and policy.
CREATE INDEX harvest_agent_policy_audit_kind_idx
    ON harvest_agent_policy_audit (kind, policy, id DESC);

COMMENT ON TABLE harvest_agent_policies IS
    'Issue #2439: the operator policy of each agent kind, keyed by (kind, policy).';
COMMENT ON TABLE harvest_agent_policy_audit IS
    'Issue #2439: each set and reset of an agent policy, with the old and new body.';
