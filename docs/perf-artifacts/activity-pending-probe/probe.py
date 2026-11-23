"""Per-call shared buffers for the full history read and the narrow probes.

Usage: python3 -I probe.py <label>. Runs each statement twice, reports the second (warm) run.
"""
import json, subprocess, sys

PSQL = ["psql", "-h", "/tmp/kspg", "-p", "55440", "-U", "postgres", "-d", "ks", "-At", "-v", "ON_ERROR_STOP=1"]

def q(sql):
    return subprocess.run(PSQL + ["-c", "SET max_parallel_workers_per_gather = 0", "-c", sql],
                          check=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]

STMTS = {
    "full_read": "SELECT * FROM harvest_events WHERE workflow_exec_id = '{e}' ORDER BY event_id",
    "narrow": ("SELECT event_id, event_type FROM harvest_events WHERE workflow_exec_id = '{e}' "
               "AND event_type IN ('ActivityScheduled','ActivityCompleted','ActivityFailed','ActivityTimedOut') "
               "AND event_data #>> '{{data,activity_id}}' = '{a}'"),
    "next_id": "SELECT max(event_id) FROM harvest_events WHERE workflow_exec_id = '{e}'",
}

def node(p):
    s = p["Node Type"] + (f"({p['Index Name']})" if "Index Name" in p else "")
    return s + "".join(" > " + node(c) for c in p.get("Plans", []))

def blocks(p):
    return p.get("Shared Hit Blocks", 0) + p.get("Shared Read Blocks", 0)

for acts, run in (("10", "run500"), ("50", "run1500"), ("200", "run2500")):
    e = q(f"SELECT md5('{run}')::uuid")
    a = q(f"SELECT event_data #>> '{{data,activity_id}}' FROM harvest_events WHERE workflow_exec_id = '{e}' "
          "AND event_type = 'ActivityScheduled' ORDER BY event_id DESC LIMIT 1")
    for name, tmpl in STMTS.items():
        sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + tmpl.format(e=e, a=a)
        q(sql)  # warm-up run, discarded
        out = subprocess.run(PSQL + ["-c", "SET max_parallel_workers_per_gather = 0", "-c", sql],
                             check=True, capture_output=True, text=True).stdout
        plan = json.loads(out.split("\n", 1)[1])[0]
        print(f"{sys.argv[1]}\tacts={acts}\t{name}\tbuffers={blocks(plan['Plan'])}\trows={plan['Plan']['Actual Rows']}"
              f"\tms={plan['Execution Time']:.3f}\t{node(plan['Plan'])}")
