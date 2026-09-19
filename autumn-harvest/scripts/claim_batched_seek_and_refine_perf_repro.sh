#!/usr/bin/env bash
# Reproduction harness for `docs/performance-claim-batched-seek-and-refine.md`
# (issue #1340).
#
# Unlike the other `*_claim_perf_repro.sh` scripts, this one compares two
# queries that BOTH exist in the tree today -- `queue::claim_task_query()`
# (control) and `queue::claim_task_batched_candidates_query()` (candidate)
# -- so it needs no git-stash dance to reconstruct a "before" state.
#
# Usage:
#   HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
#     ./autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh
#
# Writes into docs/perf-artifacts/claim-batched-seek-and-refine/:
#   control_idle.explain.txt   -- claim_task_query() at 256 keys, 0 RUNNING
#   batch_idle.explain.txt     -- claim_task_batched_candidates_query(), same
#   control_hot.explain.txt    -- claim_task_query() at 256 keys, 2,000 RUNNING
#   batch_hot.explain.txt      -- claim_task_batched_candidates_query(), same
#   end_to_end_latency.txt     -- real claim_task vs claim_task_batched calls,
#                                  same hot-contention fixture (the source for
#                                  the doc page's headline milliseconds)
#
# Preconditions: a reachable Postgres 16 named by HARVEST_TEST_DATABASE_URL,
# migrated with the full bundle (`autumn_harvest::test_init_sql()`), and a
# Rust toolchain that can build this crate.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

: "${HARVEST_TEST_DATABASE_URL:?set HARVEST_TEST_DATABASE_URL to a migrated Postgres 16 admin connection string}"

OUT_DIR="$REPO_ROOT/docs/perf-artifacts/claim-batched-seek-and-refine"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "== dumping claim_task_query() and claim_task_batched_candidates_query() =="
cat > "$WORK_DIR/dump_queries.rs" <<'RS'
fn main() {
    println!("=== CONTROL ===");
    println!("{}", autumn_harvest::queue::claim_task_query());
    println!("=== BATCH ===");
    println!("{}", autumn_harvest::queue::claim_task_batched_candidates_query());
}
RS
mkdir -p autumn-harvest/examples
cp "$WORK_DIR/dump_queries.rs" autumn-harvest/examples/dump_queries.rs
trap 'rm -f autumn-harvest/examples/dump_queries.rs; rm -rf "$WORK_DIR"' EXIT
cargo run -p autumn-harvest --features db --example dump_queries > "$WORK_DIR/queries.txt"
rm -f autumn-harvest/examples/dump_queries.rs

python3 - "$WORK_DIR/queries.txt" "$WORK_DIR" <<'PY'
import sys
path, work_dir = sys.argv[1], sys.argv[2]
lines = open(path).read().splitlines()
idx = {l: i for i, l in enumerate(lines) if l.startswith("===")}
open(f"{work_dir}/control.sql", "w").write(lines[idx["=== CONTROL ==="] + 1])
open(f"{work_dir}/batch.sql", "w").write(lines[idx["=== BATCH ==="] + 1])
PY

echo "== seeding fixture (10,000-row backlog, 4 queues, 256 keys) =="
psql "$HARVEST_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
TRUNCATE harvest_task_queue;
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, concurrency_key,
   concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'PENDING', (i % 100), 0, 3,
       NOW() - INTERVAL '1 second', 'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 10000) AS i;
ANALYZE harvest_task_queue;
SQL

python3 - "$WORK_DIR" <<'PY'
import sys
work_dir = sys.argv[1]
control = open(f"{work_dir}/control.sql").read()
batch = open(f"{work_dir}/batch.sql").read()
lines = [
    "SET jit = off;",
    "DEALLOCATE ALL;",
    f"PREPARE control_q (text, text[], text, int8, text[], text[]) AS {control};",
    "PREPARE batch_q (text, text[], text, int8, text[], text[], bool, int4, int4, "
    f"timestamptz, uuid, int8) AS {batch};",
    "BEGIN;",
    r"\o control_idle.explain.txt",
    "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE control_q("
    "'w1', ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, "
    "ARRAY[]::text[], ARRAY[]::text[]);",
    r"\o",
    "ROLLBACK;",
    "BEGIN;",
    r"\o batch_idle.explain.txt",
    "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE batch_q("
    "'w1', ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, "
    "ARRAY[]::text[], ARRAY[]::text[], false, 0, 0, NULL, NULL, 50);",
    r"\o",
    "ROLLBACK;",
]
open(f"{work_dir}/explain_idle.sql", "w").write("\n".join(lines))
PY

echo "== capturing idle scenario =="
(cd "$WORK_DIR" && psql "$HARVEST_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f explain_idle.sql >/dev/null)

echo "== seeding hot-contention addition (2,000 RUNNING rows, same 256 keys) =="
psql "$HARVEST_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, worker_id, attempt, max_attempts, scheduled_at, started_at,
   concurrency_key, concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'RUNNING', 0, 'holder-' || i, 1, 3,
       NOW() - INTERVAL '10 second', NOW() - INTERVAL '5 second',
       'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 2000) AS i;
ANALYZE harvest_task_queue;
SQL

python3 - "$WORK_DIR" <<'PY'
import sys
work_dir = sys.argv[1]
control = open(f"{work_dir}/control.sql").read()
batch = open(f"{work_dir}/batch.sql").read()
lines = [
    "SET jit = off;",
    "DEALLOCATE ALL;",
    f"PREPARE control_q (text, text[], text, int8, text[], text[]) AS {control};",
    "PREPARE batch_q (text, text[], text, int8, text[], text[], bool, int4, int4, "
    f"timestamptz, uuid, int8) AS {batch};",
    "BEGIN;",
    r"\o control_hot.explain.txt",
    "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE control_q("
    "'w1', ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, "
    "ARRAY[]::text[], ARRAY[]::text[]);",
    r"\o",
    "ROLLBACK;",
    "BEGIN;",
    r"\o batch_hot.explain.txt",
    "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE batch_q("
    "'w1', ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, "
    "ARRAY[]::text[], ARRAY[]::text[], false, 0, 0, NULL, NULL, 50);",
    r"\o",
    "ROLLBACK;",
]
open(f"{work_dir}/explain_hot.sql", "w").write("\n".join(lines))
PY

echo "== capturing hot-contention scenario =="
(cd "$WORK_DIR" && psql "$HARVEST_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f explain_hot.sql >/dev/null)

mkdir -p "$OUT_DIR"
cp "$WORK_DIR"/control_idle.explain.txt "$WORK_DIR"/batch_idle.explain.txt \
   "$WORK_DIR"/control_hot.explain.txt "$WORK_DIR"/batch_hot.explain.txt \
   "$OUT_DIR/"

echo "== capturing real end-to-end latency (claim_task vs claim_task_batched) =="
# The test itself seeds its own fixture (same shape as above) and writes
# end_to_end_latency.txt directly into $OUT_DIR -- nothing to copy here.
cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --nocapture zz_capture_claim_batched_end_to_end_latency

echo "== done. Artifacts in $OUT_DIR =="
ls -la "$OUT_DIR"
