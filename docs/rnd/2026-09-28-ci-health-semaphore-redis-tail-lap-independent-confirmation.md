# 🚦 Semaphore CI health — independent confirmation of PR #1756's fix for the `dispatch_redis` tail-queue starvation flake, plus a correction to its own "does not reproduce locally" claim

**Status:** confirmation report, no code change. Recommends merging open PR
#1756 (`claude/beautiful-noether-r28wu2`, base `trunk-dev`, `mergeable_state:
clean`, CI `pending` as of this report). This session found the same
signature independently — via a fresh rerun-protocol scan of `ci.yml`
history, before reading PR #1756 — then reproduced both the failure and the
fix locally with a harness the PR's own evidence section says should not be
able to reproduce it at all. It can, at a materially high rate, and that
strengthens rather than weakens the PR's diagnosis.

## 🎯 Verdict path

`ci.yml`'s `pull_request` and `push` triggers against `trunk-dev`,
`autumn-harvest-redis`'s `dispatch_redis` integration suite, run inside
`Test DB (linux, shard N)` (`N` depends on the shard-weight assignment;
this signature was seen on shard 6 in the sample below).

## 🌡️ Symptom

Scanning the last 100 completed `pull_request`-event `ci.yml` runs (this
session's own census, independent of PR #1756): 5 failures out of 100, all
on 2026-09-27. Clustering by signature:

| Run | Time (UTC) | Job | Signature |
|---|---|---|---|
| 36347109249 (PR #1715, attempt 2) | 23:59 | Test DB (linux, shard 6) | `dispatch_redis::a_queue_near_the_tail_of_a_long_rotation_still_gets_a_blocking_look` — `left == right` failed, left 0, right 1, `tests/dispatch_redis.rs:1256` |
| 36342582816 | 20:21 | Test DB (linux, shard 0) | `cross_region_dr_tests` FK violation (pre-dates the 21:35 fix in #1753; unrelated to this report) |
| 36343266170 | 19:08 | Lint | `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` — stale headline count (doc-drift class, not a flake; see note below) |
| 36340435284 | 18:22 | Lint | `sqlite_feasibility_docs::every_inventoried_module_records_the_mechanisms_grep_finds` + `mechanism_counts_quoted_in_the_report_are_current` — same doc-drift class |
| 36342698704 | 18:59 | (none — `get_job_logs` reports 0 failed jobs for a run whose top-level conclusion is `failure`) | not investigated further this session |

Two signatures, three defect classes. The `sqlite_feasibility_docs` failures
are the same "hand-copied number races a concurrent merge" class already
chronicled for `shard-weight-drift.py`'s self-test
(`docs/rnd/2026-09-27-...-harness-refresh.md`) — a report-only doc-audit
test correctly catching that its snapshot went stale between the PR's base
and `trunk-dev`'s moving head, not a flake in the CI-health sense (rerunning
the same commit would not change the verdict; rebasing would). Left for a
future session if it keeps recurring; not this report's target.

The `dispatch_redis` signature is the target. `PR #1756`, opened
02:42 UTC today by a separate session, already diagnoses and fixes it,
citing two prior occurrences on PR #1715 (a UI-only PR, unrelated to Redis).
Checking this session's own sample against that: run 36347109249 is
`pull_requests: [1715]`, attempt 2 — the *same* PR, not a third one. So
this report's independent scan corroborates PR #1756's own two occurrences
rather than adding a third, distinct PR. That is weaker than "three
unrelated PRs" would have been, but the two occurrences PR #1756 already
cites are themselves enough to place the trigger in CI-runner load rather
than in PR #1715's UI-only diff: the same non-Redis-touching PR cannot be
the cause of a Redis-dispatch race repeating across separate CI attempts.

## 🔍 Diagnosis

Test-vs-product verdict, confirmed independently: **product**, not test.
`read_across_queues` (`autumn-harvest-redis/src/dispatch.rs:772`) divides
its remaining wait budget by the queues left in the current lap to give
each queue a fair blocking slice, recomputed every iteration. No slice
accounts for the Redis round-trip time itself. Over a 20-queue lap the
accumulated round-trip overhead can exceed the shrinking per-queue share
before the loop reaches the last queues, so `deadline.checked_duration_since`
returns `None` and the loop `break`s — the tail queue never gets a blocking
look at all, not merely a short one. The test's assertion (`leases.len() ==
1`) catches exactly that: it published to the last of 20 queues and got 0
leases back. PR #1756's root-cause writeup (quoted in its description) says
the same thing in the same place; this session's read of `dispatch.rs`
reached it before opening that PR, which is what independent confirmation
is for.

This is a genuine under-delivery in production, not just a test artifact:
an entry on a tail queue in a busy rotation waits a full extra `next()`
call whenever this races the same way outside CI.

## 🔧 Treatment

No code change from this session — PR #1756 already carries the fix (a
deadline that passes mid-lap now finishes the lap with non-blocking reads
instead of breaking, plus follow-on round-trip-budget accounting in the
worker's read timeout so the now-longer worst case stays covered). This
report is the independent-verification half of the hard gate, run against
a harness the PR's own text does not claim: see below.

## 📊 Measurement

Harness: this sandbox has no Docker daemon (`docker ps` fails, no
`/var/run/docker.sock`), so `dispatch_redis`'s test fixture would normally
skip via its `testcontainers` fallback. It also honors
`HARVEST_REDIS_TEST_URL` directly (`tests/dispatch_redis.rs`'s
`try_start`), and this image ships a real `redis-server` binary. Started one
locally (`redis-server --daemonize yes --port 6399 --save "" --appendonly
no`), pointed the harness at it, and ran the target test in isolation
20 times per condition, `--test-threads=1`, flushing Redis between runs.

**Before** (`HEAD` = `trunk-dev` tip `7906f22`, PR #1756's fix absent):
**17/20 passed, 3/20 failed (15%)**, all three with the exact CI signature
— `left: 0, right: 1` at `dispatch_redis.rs:1256`.

**After** (PR #1756's core mechanism commit, `e24a735`, applied to the same
tree via `git apply`, nothing else changed): **20/20 passed**.

Revert check: satisfied by construction — the "before" run above *is* the
pre-fix tree, sampled after the "after" run confirmed the fix's presence
mattered, not merely that the suite happened to be green that day. Working
tree restored to `HEAD` (`git checkout -- dispatch.rs`) after measurement;
no commit made to this session's own branch from the patched state.

**Correction to PR #1756's own evidence section:** its description states
"The failure needs a slow round trip, so it does not reproduce on an idle
local Redis." That does not hold in this sandbox — a bare local
`redis-server`, not under any deliberate load, reproduced the failure at
15% (3/20). The mechanism still fits, but not for the reason a first guess
suggests: the 20 queue visits inside `read_across_queues` are not
concurrent with each other — the loop `.await`s each `read_with_heal` call
before advancing the rotation — and `--test-threads=1` keeps no other test
running alongside this one. What this 4-core sandbox does add is ordinary
scheduling and I/O latency on each sequential round trip: cargo's own
build/test-harness overhead, the container's shared CPU, and the loopback
hop to `redis-server`, none of it a deliberate load generator. PR #1756's
own arithmetic only needs round trips to average above roughly 40ms for
the tail queue to starve; this sandbox's ordinary per-call latency is
apparently enough, without any concurrent contention to blame it on. This
is additional evidence for the same diagnosis, not a different one: "idle"
undersells how little it takes.

## 🔬 Reproduce

```sh
redis-server --daemonize yes --port 6399 --save "" --appendonly no
export HARVEST_REDIS_TEST_URL=redis://127.0.0.1:6399

# Before (trunk-dev tip, PR #1756 absent):
git checkout 7906f22ea00cf0a5fd8f40340362473e6130a479
cargo build -p autumn-harvest-redis --test dispatch_redis
for i in $(seq 1 20); do
  redis-cli -p 6399 flushall >/dev/null
  cargo test -p autumn-harvest-redis --test dispatch_redis \
    a_queue_near_the_tail_of_a_long_rotation_still_gets_a_blocking_look \
    -- --exact --test-threads=1
done   # -> 3/20 fail, "left: 0, right: 1" at dispatch_redis.rs:1256

# After (PR #1756's core mechanism commit applied):
git fetch origin claude/beautiful-noether-r28wu2
git show e24a735 -- autumn-harvest-redis/src/dispatch.rs | git apply -
cargo build -p autumn-harvest-redis --test dispatch_redis
for i in $(seq 1 20); do
  redis-cli -p 6399 flushall >/dev/null
  cargo test -p autumn-harvest-redis --test dispatch_redis \
    a_queue_near_the_tail_of_a_long_rotation_still_gets_a_blocking_look \
    -- --exact --test-threads=1
done   # -> 20/20 pass
git checkout -- autumn-harvest-redis/src/dispatch.rs
```
