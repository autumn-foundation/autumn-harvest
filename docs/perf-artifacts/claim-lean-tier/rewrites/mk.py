"""Builds the lean claim text with each subset of the plan rewrites.

Each rewrite is a set of exact-text replacements with a count assertion,
the same pieces as `LEAN_PLAN_REWRITES` in autumn-harvest/src/queue.rs.
The input is a lean text with no rewrite, as in texts-before/. The `all`
output of the `any` variant is byte-identical to the shipped lean text.
Usage: python3 mk.py <dir of <variant>-lean.sql> <out-dir>

Sets: base (no rewrite), i (scalar subqueries for the pause arrays),
ii (ARRAY() for array_agg), iii (marked names merged into the
paused-activity array), i_ii, i_iii, all. The out-dir gets one text per
variant and set, and for the `any` variant a pgbench script (pgb-<set>.sql)
and an EXPLAIN script (explain-<set>.sql).
"""
import re, sys, os, itertools

PQ = "(SELECT names FROM paused_queues)::text[]"
PA = "(SELECT names FROM paused_activities)::text[]"
MARKED = "SELECT substr(marked, 2) FROM unnest($6::text[]) AS marked WHERE left(marked, 1) = chr(1)"

def must(t, old, new, n):
    assert t.count(old) == n, (old, t.count(old), n)
    return t.replace(old, new)

def scalar(t):
    # (i) scalar subqueries instead of the pause cross joins
    t = must(t, "CROSS JOIN paused_queues CROSS JOIN paused_activities ", "", 2)
    t = must(t, "FROM paused_queues CROSS JOIN paused_activities CROSS JOIN LATERAL ( ", "FROM ( ", 1)
    t = must(t, "paused_queues.names", PQ, 3)
    t = must(t, "paused_activities.names", PA, 3)
    return t

def array(t):
    # (ii) ARRAY(SELECT ...) instead of COALESCE(array_agg(...), ...)
    t = must(t, "SELECT COALESCE(array_agg(queue_name), ARRAY[]::text[]) AS names FROM harvest_queue_pauses WHERE queue_name = ANY($2)",
             "SELECT ARRAY( SELECT queue_name FROM harvest_queue_pauses WHERE queue_name = ANY($2) ) AS names", 1)
    t = must(t, "SELECT COALESCE(array_agg(activity_name), ARRAY[]::text[]) AS names FROM harvest_activity_pauses",
             "SELECT ARRAY( SELECT activity_name FROM harvest_activity_pauses ) AS names", 1)
    return t

def merged(t):
    # (iii) the marked names of $6 join the paused-activity array
    t = must(t, "FROM harvest_activity_pauses",
             "FROM ( SELECT activity_name FROM harvest_activity_pauses UNION ALL " + MARKED + " ) AS held(activity_name)", 1)
    t = must(t, " WHEN task_type = 'activity' AND activity_name = ANY(ARRAY( " + MARKED + " )) THEN FALSE", "", 2)
    t = must(t, " AND ( task_type != 'activity' OR activity_name IS NULL OR NOT (activity_name = ANY(ARRAY( " + MARKED + " ))) )", "", 1)
    return t

STEPS = {"i": scalar, "ii": array, "iii": merged}
VARIANTS = {"base": [], "i": ["i"], "ii": ["ii"], "iii": ["iii"], "i_ii": ["i", "ii"], "i_iii": ["i", "iii"], "all": ["i", "ii", "iii"]}

PRE = ("\\set a 0\nBEGIN ISOLATION LEVEL READ COMMITTED;\nSET LOCAL jit = off;\n"
       "SET LOCAL plan_cache_mode = force_generic_plan;\nSET LOCAL enable_bitmapscan = off;\n"
       "SET LOCAL enable_sort = off;\n")

def pgb(body):
    t = body
    for k, v in {'$1': ':w', '$2': ':q', '$3': "''", '$4': ':a', '$5': ':r', '$6': ':m'}.items():
        t = re.sub(re.escape(k) + r'(?!\d)', v, t)
    return t

def main(src, out):
    os.makedirs(out, exist_ok=True)
    for f in sorted(os.listdir(src)):
        if not f.endswith("-lean.sql"):
            continue
        base = open(os.path.join(src, f)).read()
        name = f[:-len("-lean.sql")]
        for v, steps in VARIANTS.items():
            t = base
            for s in steps:
                t = STEPS[s](t)
            open(os.path.join(out, f"{name}-{v}.sql"), "w").write(t)
            if name == "any":
                open(os.path.join(out, f"pgb-{v}.sql"), "w").write(PRE + pgb(t) + ";\nROLLBACK;\n")
                open(os.path.join(out, f"pgb2-{v}.sql"), "w").write(
                    PRE + pgb(t) + " \\gset c_\nCOMMIT;\n"
                    "UPDATE harvest_task_queue SET state = 'PENDING', worker_id = NULL, started_at = NULL, "
                    "attempt = GREATEST(attempt - 1, 0), scheduled_at = now() WHERE id = :c_id::uuid;\n")
                open(os.path.join(out, f"explain-{v}.sql"), "w").write(
                    "BEGIN; SET LOCAL jit = off; SET LOCAL plan_cache_mode = force_generic_plan; "
                    "SET LOCAL enable_bitmapscan = off; SET LOCAL enable_sort = off;\n"
                    "PREPARE c(text, text[], text, bigint, text[], text[]) AS \n" + t +
                    ";\nEXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) EXECUTE c('w1', ARRAY['default'], '', NULL, "
                    "ARRAY[]::text[], ARRAY[]::text[]); ROLLBACK;\n")

main(sys.argv[1], sys.argv[2])
