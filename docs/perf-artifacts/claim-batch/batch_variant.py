"""Usage: batch_variant.py <window.sql> <rows> <out.sql>

Applies the batch transform of queue::claim_window::batch_claim_sql to a
pgbench copy of the window form, for pgbench.sh.
"""
import re,sys
src,k,out=sys.argv[1],int(sys.argv[2]),sys.argv[3]
s=open(src).read()
cols="SELECT id, task_type, concurrency_key, concurrency_cap, rate_limit_key, activity_name, workflow_exec_id FROM harvest_task_queue"
i=s.index("seek_candidate AS ("); j=s.index("seek_held AS")
sc=s[i:j]
sc=re.sub(r'\s+',' ',sc)
assert sc.count(cols)==1, sc.count(cols)
sw=":w" if ":w" in s else "$1"
key=(f", CASE WHEN sticky_worker_id = {sw} AND sticky_until > NOW() THEN 1 ELSE 0 END AS claim_rank, "
     "priority AS claim_priority, (scheduled_at + CASE WHEN new_start AND attempt = 0 THEN INTERVAL '30 seconds' ELSE INTERVAL '0 seconds' END) AS claim_due")
sc=sc.replace(cols, cols.replace(" FROM harvest_task_queue", key+" FROM harvest_task_queue"))
assert sc.count("LIMIT 1 FOR UPDATE SKIP LOCKED")==1
sc=sc.replace("LIMIT 1 FOR UPDATE SKIP LOCKED", f"LIMIT {k} FOR UPDATE SKIP LOCKED")
s=s[:i]+sc+" "+s[j:]
old="candidate AS ( SELECT * FROM seek_candidate )"
assert s.count(old)==1
new=(f"claim_ranked AS MATERIALIZED ( SELECT c.*, row_number() OVER ( ORDER BY c.claim_rank DESC, c.claim_priority DESC, c.claim_due ASC, c.id ASC ) AS claim_rn FROM seek_candidate c ), "
     f"candidate AS MATERIALIZED ( SELECT r.* FROM claim_ranked r WHERE r.claim_rn < COALESCE(( SELECT min(x.claim_rn) FROM claim_ranked x WHERE x.claim_rn > 1 AND (x.concurrency_key IS NOT NULL OR x.rate_limit_key IS NOT NULL) ), {k+1}) )")
s=s.replace(old,new)
open(out,'w').write(s)
