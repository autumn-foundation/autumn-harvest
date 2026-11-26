"""Write a pgbench script for one dumped claim text.

Usage: pgbench_text.py <claim text file> > script.sql
The script sends the claim the way a claim loop does: the claim plan
settings, the claim, then a rollback. The binds become pgbench variables:
w (worker), q (queues), a (ageing, null), r (breaker names), m (marked names).
"""
import re
import sys

text = open(sys.argv[1]).read().strip().rstrip(";")
for bind, var in {"$1": ":w", "$2": ":q", "$3": "''", "$4": "NULL", "$5": ":r", "$6": ":m"}.items():
    text = re.sub(re.escape(bind) + r"(?!\d)", var, text)
assert "$" not in text, "the text holds a bind that this script does not know"
print("BEGIN ISOLATION LEVEL READ COMMITTED;")
print("SET LOCAL jit = off;")
print("SET LOCAL plan_cache_mode = force_generic_plan;")
print("SET LOCAL enable_bitmapscan = off;")
print("SET LOCAL enable_sort = off;")
print(text + ";")
print("ROLLBACK;")
