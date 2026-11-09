-- The durable stores of the agent crate.
--
-- `harvest_agent_store_docs` holds one document per store and scope. The
-- `episodes` store keeps the whole episode log of a scope. The `memory`
-- store keeps the blocks of a scope. Each document holds its applied edit
-- keys, so one write saves an edit and its key together. `version` counts
-- each write from 1. A writer names the version that it read, so two
-- writers never lose an edit.
--
-- `harvest_agent_transcripts` holds the windows of model turns. A window
-- is keyed by its digest and its session. An empty session means a run
-- with no session. An erase deletes the rows of one session.
--
-- `body` is `BYTEA`, so a NUL in model text is safe. The stores keep the
-- text in clear, outside the payload codecs.
--
-- The tables are new and not hot. The timeout bounds the DDL all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_agent_store_docs (
    store      TEXT        NOT NULL CHECK (store IN ('episodes', 'memory')),
    scope      TEXT        NOT NULL CHECK (scope <> ''),
    version    BIGINT      NOT NULL CHECK (version >= 1),
    body       BYTEA       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store, scope)
);

CREATE TABLE harvest_agent_transcripts (
    digest     TEXT        NOT NULL CHECK (digest <> ''),
    session    TEXT        NOT NULL,
    run_id     TEXT        NOT NULL,
    body       BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (digest, session)
);

-- A put prunes the old windows of its run. An erase reads one session.
CREATE INDEX harvest_agent_transcripts_run_idx
    ON harvest_agent_transcripts (session, run_id, created_at DESC);

-- A put deletes the windows past the age limit.
CREATE INDEX harvest_agent_transcripts_created_idx
    ON harvest_agent_transcripts (created_at);

COMMENT ON TABLE harvest_agent_store_docs IS
    'The episode logs and memory blocks of agents, one document per store and scope.';
COMMENT ON TABLE harvest_agent_transcripts IS
    'The model-turn windows of agents, keyed by digest and session.';
