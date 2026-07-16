-- Content-addressed storage for sandboxed WebAssembly activity modules
-- (issue #965, milestone 2).
--
-- Composite primary key (hash, activity_name): identical WASM bytes can be
-- published under two different activity names independently, so each
-- (hash, activity_name) pair is a distinct row.
--
-- The partial unique index enforces the single-active-version invariant: at
-- most one row per activity_name may have active = true, so a hot-swap
-- (publish v2 -> deactivate v1, activate v2) can never leave two active
-- versions racing.
CREATE TABLE harvest_wasm_modules (
    hash          TEXT NOT NULL,
    activity_name TEXT NOT NULL,
    wasm_bytes    BYTEA NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT false,
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hash, activity_name)
);

CREATE UNIQUE INDEX uq_harvest_wasm_modules_active
    ON harvest_wasm_modules (activity_name)
    WHERE active;
