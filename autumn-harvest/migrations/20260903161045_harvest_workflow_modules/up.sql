-- Registry of hot-swappable workflow modules (issue #967, R&D spike).
--
-- Postgres-only, plugin-free, no new infrastructure: the "registry" a worker
-- discovers and fetches runtime workflow modules from is one table in the
-- database the engine already owns.
--
-- Primary key (build_id, workflow_name) is the design's central invariant, not
-- an incidental uniqueness choice. It says:
--
--   1. A build id names exactly ONE module for a given workflow, immutably.
--      That mirrors the start-time immutability of
--      `harvest_workflow_executions.assigned_build_id`: an execution's build is
--      fixed when it starts, so the code that build denotes must be fixed too,
--      or an in-flight execution's meaning would change underneath it.
--   2. Two modules can never claim one workflow name outside build-id
--      governance -- the determinism hazard the spike's safety analysis names.
--      They can coexist only under DIFFERENT build ids, which is precisely the
--      case the shipped ramp / compatibility / reachability machinery governs.
--
-- There is deliberately NO `active` flag. Which module a new execution lands on
-- is decided by `harvest_build_policies` (the percent ramp from issue #604), and
-- which module an in-flight execution keeps is decided by its recorded
-- `assigned_build_id`. Adding a second switch here would be a second source of
-- truth for the same question.
CREATE TABLE harvest_workflow_modules (
    build_id      TEXT NOT NULL,
    workflow_name TEXT NOT NULL,
    -- Lowercase-hex SHA-256 of `module_bytes`. Re-derived and compared on every
    -- load, so a row whose payload was altered without updating this column
    -- fails closed rather than executing unreviewed code.
    module_hash   TEXT NOT NULL,
    -- Bounded by a CHECK, not only by the publish path: the publish API's
    -- ceiling is irrelevant to an attacker with INSERT, and a worker syncing a
    -- build materialises whatever the row holds. Keep the two numbers in step
    -- with `hot_swap::MAX_WORKFLOW_MODULE_BYTES`.
    module_bytes  BYTEA NOT NULL CHECK (octet_length(module_bytes) <= 33554432),
    -- Detached lowercase-hex HMAC-SHA256 over the whole BINDING --
    -- (build_id, workflow_name, module_hash), length-prefixed and
    -- domain-separated -- under the operator's signing key. Signing the
    -- identity rather than the bytes alone is what stops a table writer copying
    -- an existing row's (hash, signature) pair and re-binding it under a
    -- different build id or workflow name. NULL = unsigned; a worker
    -- configured with a key refuses to load such a row.
    signature     TEXT,
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Retirement is SOFT, and the row is why. The primary key is what stops a
    -- build id being re-pointed at different bytes; deleting the row would
    -- quietly restore the ability to publish `wf-v1` again with new code, and
    -- an execution still parked on a long timer under `wf-v1` would resume on
    -- logic it never started under. A retired row is a tombstone: republishing
    -- the SAME bytes revives it, republishing DIFFERENT bytes is refused
    -- forever. Retired rows are invisible to every read path, so a worker will
    -- not load one -- which is the operational effect retirement is for.
    retired_at    TIMESTAMPTZ,
    PRIMARY KEY (build_id, workflow_name)
);

-- Workers sync a whole build at once ("load everything build X provides"), and
-- retirement stamps by build, so the PK's leading column already serves both.
-- This index instead serves the content-addressed question: "which builds ship
-- these exact bytes?", used when deciding whether a compiled module can be
-- shared across builds rather than recompiled.
CREATE INDEX idx_harvest_workflow_modules_hash
    ON harvest_workflow_modules (module_hash);
