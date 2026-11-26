"""Write one psql script that replays a claim loop against the prepared texts.

Usage: gen.py <rule> <pattern> <claims> <forms.sql>
  rule     off      the window form first, as with claim_lean = false
           shared   one skip for the loop; a fallback doubles it, a lean claim
                    resets it (the rule of the first three commits)
           final    one skip for each claim kind; a fallback adds 4 claims,
                    up to 64, and a lean claim takes 1 off (the shipped rule)
  pattern  the claim kinds of the loop in turn: N (any), W (workflow)
The fixture (seed.sql) keeps one keyed activity row at its cap at the head of
the queue. So a lean claim of any kind sets its flag, and a lean workflow
claim takes a row. The script checks each expected answer.
"""
import sys

rule, pattern, claims, forms = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
CAP, STEP = 64, 4


def fallback(span):
    if rule == "shared":
        return 1 if span == 0 else (span * 2 if span < CAP // 2 else CAP)
    return CAP if span >= CAP - STEP else span + STEP


def claimed(span):
    return 0 if rule == "shared" else max(span - 1, 0)


BEGIN = (r"begin transaction isolation level read committed\; SET LOCAL jit = off\; "
         r"SET LOCAL plan_cache_mode = force_generic_plan\; SET LOCAL enable_bitmapscan = off\; "
         "SET LOCAL enable_sort = off;")
ARGS = "('W', array['q0'], 'b1', null, array[]::text[], array[]::text[])"
KINDS = {"N": "any", "W": "workflow"}
out = [f"\\i {forms}", "\\pset format unaligned", "\\pset tuples_only on"]
state = {}
lean_claimed = lean_flag = window = 0
for i in range(claims):
    k = pattern[i % len(pattern)]
    key = k if rule == "final" else "loop"
    left, span = state.get(key, (0, 0))
    out.append(BEGIN)
    need_window = True
    if rule != "off" and left == 0:
        out.append("\\unset l_id")
        out.append(f"execute lean_{KINDS[k]}{ARGS} \\gset l_")
        if k == "W":
            out.append("\\if :{?l_id}\n\\else\n\\echo MISMATCH expected a lean claim\n\\endif")
            span = claimed(span)
            lean_claimed += 1
            need_window = False
        else:
            out.append("\\if :{?l_id}\n\\echo MISMATCH expected a lean flag\n\\endif")
            out.append("\\if :l_claim_fallback\n\\else\n\\echo MISMATCH expected a lean flag\n\\endif")
            span = fallback(span)
            left = span
            lean_flag += 1
    elif rule != "off":
        left -= 1
    state[key] = (left, span)
    if need_window:
        window += 1
        out.append("\\unset w_id")
        out.append(f"execute window_{KINDS[k]}{ARGS} \\gset w_")
        out.append("\\if :{?w_id}\n\\else\n\\echo MISMATCH the window form took no row\n\\endif")
    out.append("commit;")
out.append("select 'running rows', count(*) from harvest_task_queue where state = 'RUNNING' and worker_id = 'W';")
print("\n".join(out))
sys.stderr.write(
    f"rule={rule} pattern={pattern} claims={claims} lean_claimed={lean_claimed} "
    f"lean_flag={lean_flag} window={window} statements={lean_claimed + lean_flag + window}\n")
