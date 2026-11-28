-- The board of an agent swarm (issue #2814).
--
-- A swarm is a set of agents that work on one goal. Its board has four
-- parts, keyed by the swarm name:
--
-- - `harvest_swarm_items` holds the work items. A claim sets `holder`,
--   `lease_until_ms` and a higher `epoch`, and the main snapshot that it
--   checked out. Each write names the `rev` that it read, so two writers
--   never lose a change.
-- - `harvest_swarm_posts` holds the posts of the channels. `seq` counts the
--   posts of one swarm. A reader passes the last `seq` it read.
-- - `harvest_swarm_mains` holds the head of the shared files of each swarm:
--   a workspace reference and its score. A merge names the version that it
--   read.
-- - `harvest_swarm_merges` holds every verdict of the merge gate. A merge
--   keeps the reference of the files that it made.
--
-- `harvest_swarm_counters` hands out `seq` values. An insert takes the lock
-- of its counter row until it commits. So the posts of one swarm commit in
-- `seq` order, and a reader with a cursor never skips one.
--
-- A trigger refuses each UPDATE of a post or a verdict. The trigger stops
-- mistakes. It is not a security boundary.
--
-- The times are epoch milliseconds from the clock of the app, as the
-- leases need one clock.
--
-- The tables are new and not hot. The timeout bounds each lock all the same.
SET LOCAL lock_timeout = '5s';

CREATE TABLE harvest_swarm_items (
    swarm          TEXT   NOT NULL CHECK (swarm <> ''),
    item           TEXT   NOT NULL CHECK (item <> ''),
    lane           TEXT   NOT NULL CHECK (lane <> ''),
    title          TEXT   NOT NULL,
    body           TEXT   NOT NULL,
    status         TEXT   NOT NULL CHECK (status IN ('open', 'claimed', 'merged', 'dropped')),
    holder         TEXT,
    epoch          BIGINT NOT NULL CHECK (epoch >= 0),
    lease_until_ms BIGINT NOT NULL,
    base_version   BIGINT NOT NULL CHECK (base_version >= 0),
    base_snapshot  TEXT   NOT NULL,
    rev            BIGINT NOT NULL CHECK (rev >= 1),
    created_by     TEXT   NOT NULL CHECK (created_by <> ''),
    created_at_ms  BIGINT NOT NULL,
    updated_at_ms  BIGINT NOT NULL,
    PRIMARY KEY (swarm, item),
    CHECK (status <> 'claimed' OR holder IS NOT NULL),
    CHECK (status <> 'open' OR holder IS NULL)
);

-- The board lists the items of one swarm by status and lane.
CREATE INDEX harvest_swarm_items_board_idx
    ON harvest_swarm_items (swarm, status, lane, item);

-- A claim counts the items that its member holds.
CREATE INDEX harvest_swarm_items_holder_idx
    ON harvest_swarm_items (swarm, holder)
    WHERE status = 'claimed';

CREATE TABLE harvest_swarm_counters (
    swarm  TEXT   PRIMARY KEY CHECK (swarm <> ''),
    posts  BIGINT NOT NULL DEFAULT 0 CHECK (posts >= 0),
    merges BIGINT NOT NULL DEFAULT 0 CHECK (merges >= 0)
);

CREATE TABLE harvest_swarm_posts (
    swarm         TEXT   NOT NULL CHECK (swarm <> ''),
    seq           BIGINT NOT NULL CHECK (seq >= 1),
    post_key      TEXT   NOT NULL CHECK (post_key <> ''),
    channel       TEXT   NOT NULL CHECK (channel <> ''),
    author        TEXT   NOT NULL CHECK (author <> ''),
    body          TEXT   NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (swarm, seq),
    UNIQUE (swarm, post_key)
);

-- A reader of one channel reads its posts after a cursor.
CREATE INDEX harvest_swarm_posts_channel_idx
    ON harvest_swarm_posts (swarm, channel, seq);

CREATE TABLE harvest_swarm_mains (
    swarm         TEXT   PRIMARY KEY CHECK (swarm <> ''),
    version       BIGINT NOT NULL CHECK (version >= 1),
    snapshot      TEXT   NOT NULL,
    score         BIGINT NOT NULL,
    item          TEXT,
    author        TEXT,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE harvest_swarm_merges (
    swarm           TEXT   NOT NULL CHECK (swarm <> ''),
    seq             BIGINT NOT NULL CHECK (seq >= 1),
    submit_key      TEXT   NOT NULL CHECK (submit_key <> ''),
    item            TEXT   NOT NULL,
    holder          TEXT   NOT NULL,
    epoch           BIGINT NOT NULL CHECK (epoch >= 0),
    base_version    BIGINT NOT NULL CHECK (base_version >= 0),
    main_version    BIGINT NOT NULL CHECK (main_version >= 0),
    verdict         TEXT   NOT NULL CHECK (verdict IN (
                        'merged', 'rejected', 'conflict', 'fenced', 'judge_failed', 'contended')),
    candidate_score BIGINT,
    main_score      BIGINT,
    merged_version  BIGINT,
    snapshot        TEXT,
    detail          TEXT   NOT NULL,
    created_at_ms   BIGINT NOT NULL,
    PRIMARY KEY (swarm, seq),
    UNIQUE (swarm, submit_key),
    CHECK ((verdict = 'merged') = (merged_version IS NOT NULL)),
    CHECK ((verdict = 'merged') = (snapshot IS NOT NULL))
);

CREATE OR REPLACE FUNCTION harvest_swarm_guard_immutable()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $harvest_swarm_2814$
BEGIN
    RAISE EXCEPTION '% is append-only: row % of swarm % never changes', TG_TABLE_NAME, OLD.seq, OLD.swarm
        USING ERRCODE = 'restrict_violation',
              HINT = 'Add a row instead. See CLAUDE.md, Engine Invariants.';
END
$harvest_swarm_2814$;

COMMENT ON FUNCTION harvest_swarm_guard_immutable() IS
    'Issue #2814: refuses each UPDATE of a swarm post or merge verdict.';

CREATE TRIGGER harvest_swarm_posts_immutable_trg
    BEFORE UPDATE ON harvest_swarm_posts
    FOR EACH ROW EXECUTE FUNCTION harvest_swarm_guard_immutable();

CREATE TRIGGER harvest_swarm_merges_immutable_trg
    BEFORE UPDATE ON harvest_swarm_merges
    FOR EACH ROW EXECUTE FUNCTION harvest_swarm_guard_immutable();

COMMENT ON TABLE harvest_swarm_items IS
    'Issue #2814: the work items of each agent swarm, with fenced claims.';
COMMENT ON TABLE harvest_swarm_counters IS
    'Issue #2814: the post and verdict counters of each agent swarm.';
COMMENT ON TABLE harvest_swarm_posts IS
    'Issue #2814: the append-only posts of each agent swarm, keyed by (swarm, seq).';
COMMENT ON TABLE harvest_swarm_mains IS
    'Issue #2814: the head of the shared files of each agent swarm, and its score.';
COMMENT ON TABLE harvest_swarm_merges IS
    'Issue #2814: the append-only verdicts of the merge gate of each agent swarm.';
