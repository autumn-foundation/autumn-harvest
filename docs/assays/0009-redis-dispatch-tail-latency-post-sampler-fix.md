# ⛏️ Prospect: does Redis dispatch clear the L3 tail-latency line now that the sampler confound is fixed? (pursue: 63.69 ms mean vs 250 ms line, ledger #9)

## 🎯 Question

Re-charter of assay ledger #8's L3 line, exactly as invited by issue #1429
item 1 ("re-charter once the sampler defect (#1428) is fixed, because it
distorts both arms"). Ledger #8 killed the integrated Redis dispatch path on
L3: paced at the drain arm's own sustained rate (86.52 workflows/s), the
Redis arm's dispatch-latency p99 was 426.96 ms against a 250 ms line, while
the no-channel control arm held 146.09 ms at the same pace. That same run
also surfaced issue #1428: four of ten `Worker` metrics samplers issued SQL
on every poll tick with no `metrics.is_enabled()` guard, one of them a
correlated subquery over every `RUNNING` execution, dominating
`pg_stat_activity` during the run. #1428 is closed, fixed by PR #1468
("guard 4 metrics samplers on `is_enabled()`"), merged 2026-09-11 and
present on `trunk-dev` (confirmed an ancestor of this branch's HEAD before
this apparatus was rebuilt).

**Falsifiable question:** with the sampler defect fixed and every other
condition held identical to ledger #8's own paced sweep, does the Redis
dispatch arm's p99 dispatch latency now clear the original 250 ms line?

**Decision this feeds:** whether issue #1429 item 1 still needs the
mechanism work it proposes (the per-iteration `maintain` promote round
trips, the permit-sized poll read, the reconcile sweep holding a
connection) before that item can be closed. **Decider:** whoever triages
#1429.

## ⚖️ Pre-registration

Committed 2026-09-14T09:09:34Z (commit `704c8c5`'s real timestamp; an
earlier draft of this line and of the pre-registration document itself
gave a fabricated round `00:00:00Z`, caught in PR review and corrected in
both places), before any measurement was taken:
[`docs/rnd/2026-09-14-redis-dispatch-tail-latency-post-sampler-fix-preregistration.md`](../rnd/2026-09-14-redis-dispatch-tail-latency-post-sampler-fix-preregistration.md)
(commit `704c8c5`). That document also now discloses a sequencing lapse
against the hard gate's letter: the archived apparatus's background
rebuild (unmodified source) had already started a few minutes earlier,
though no output existed yet and nothing about the plan below was written
with any measurement in hand.

- **Line (unchanged from #8's L3):** Redis arm p99 dispatch latency **≤ 250
  ms**, mean over 3 reps, paced shape. Kill if above.
- **Recorded, not gating:** control-arm p99 at the same pace, and the delta
  against #8's own original means (426.96 ms redis / 146.09 ms control), to
  size what the fix bought.
- **Conditions:** ledger #8's own paced-sweep invocation, byte-for-byte —
  same apparatus (`docs/assays/apparatus/0008-redis-dispatch-integrated/`,
  no code changes), same pool shape (4 workers, 8 workflow/16 activity
  slots, 64-connection shared pool, 20 ms Redis poll / 25 ms control poll),
  same paced shape (`ASSAY6_SHAPES=paced ASSAY6_REPS=3 ASSAY6_PACED_SECS=30
  ASSAY6_PACED_RATE_MILLI=86520 ASSAY6_CONTROL_CAP_SECS=180`), same
  durability settings (`fsync=off`, `synchronous_commit=off`, Redis
  `save ""` / `appendonly no`).
- **Acknowledged stub (pre-registered, not discovered after the fact):**
  this session's container is a different physical/virtual host than #8's
  own run, so absolute numbers are compared against #8's own line and mean,
  not claimed millisecond-reproducible across machines. Both are 4-core
  boxes.
- **Scope:** paced shape only. The drain-shape lines (L1 throughput, L2
  multiplier) are out of scope — #1429 item 1 names tail latency
  specifically, and the mechanism candidates it lists are latency
  mechanisms, not throughput ones.
- **Time box:** one session, single pass, paced shape only.

## 🔍 Prior art

- `docs/assays/0008-redis-dispatch-integrated-throughput.md` — the original
  L3 kill, its per-run p99 numbers, and the sampler defect found on the way.
- Issue #1428 (closed 2026-09-11, fixed by PR #1468) and issue #1429 (open,
  names this exact re-charter condition).
- No other ledger entry or open issue duplicates this question.

## 🧪 Apparatus

Unchanged from ledger #8: the same
`docs/assays/apparatus/0008-redis-dispatch-integrated/` binary, rebuilt in
this session (`cargo build --release`, 5m26s, exit 0) against the current
`autumn-harvest` / `autumn-harvest-redis` path dependencies, which carry PR
#1468. No apparatus source changed. Building against this container's
local registry cache did move `Cargo.lock` (committed separately,
`c19003a`): `lru` 0.16.4 → 0.18.4 (a direct runtime dependency of
`autumn-harvest`) and, pulled in with it, `hashbrown` 0.16.1 → 0.17.1 —
both minor-version bumps, not a patch-only refresh as an earlier draft of
this report claimed. That is an additional, uncontrolled confound in the
measured binary relative to #8's, alongside the different host (below);
this report does not attempt to separate its effect from either.

Local services matched to the pre-registered conditions: Postgres 16
(`ALTER SYSTEM SET fsync = off; synchronous_commit = off;`, restarted and
confirmed via `current_setting`) and Redis 7.0.15 (`save ""`,
`appendonly no`, confirmed via `CONFIG GET`), both on loopback in this
session's container (4 logical CPUs).

**Stubs, same as #8:** loopback only, one Postgres, one Redis, no
replication, no TLS, no auth, a single queue, a no-op activity, four
workers in one process. **Added stub, pre-registered:** a different host
than #8's own run — see the attribution caveat in the Verdict below.

A short (5 s) smoke run preceded the registered 30 s sweep to catch a
broken build or a misconfigured service before spending the full time box;
its numbers are not reported as data (n=432 per arm, well under the
registered rep's n=2,595) and did not affect the registered run's
configuration.

## 📊 Assay

Paced shape, 86.52 workflows/s target, 30 s per rep, 3 reps, alternating
arms, same invocation as #8's own reproduce block:

| arm | rep | target wf/s | started | achieved wf/s | n | activity p50 ms | **activity p99 ms** | all-task p50 ms | all-task p99 ms | negative samples |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| redis | 1 | 86.52 | 2,595 | 86.09 | 2,595 | 5.711 | **81.091** | 5.228 | 83.433 | 0 |
| control | 1 | 86.52 | 2,595 | 85.78 | 2,595 | 7.316 | 34.020 | 7.973 | 35.774 | 176 |
| redis | 2 | 86.52 | 2,595 | 86.22 | 2,595 | 5.131 | **54.082** | 4.660 | 50.020 | 0 |
| control | 2 | 86.52 | 2,595 | 85.35 | 2,595 | 7.888 | 30.535 | 8.506 | 34.319 | 147 |
| redis | 3 | 86.52 | 2,595 | 86.09 | 2,595 | 5.915 | **55.890** | 5.479 | 54.653 | 0 |
| control | 3 | 86.52 | 2,595 | 85.86 | 2,595 | 7.449 | 41.076 | 7.990 | 42.636 | 173 |

Redis arm mean **p99 63.69 ms** (range 54.08–81.09 ms) on the activity
population; control arm mean **p99 35.21 ms** (range 30.54–41.08 ms).
Redis arm mean p50 5.59 ms, control arm mean p50 7.55 ms. `negative`
samples on the control arm (event-ordering artifacts already present and
unexplained in #8) recur at similar counts; the Redis arm shows none in
either assay, consistent with #8.

**Correctness precondition:** every run of both arms passed —
`completed_execs == side_effects == n == started` in all six rows, and
`stream=0 pending=0 markers=0 complete=true` after every run. No run is
voided.

**Against #8's own original paced numbers**, same line, same shape,
different host:

| | #8 (2026-09-07) | #9 (this run) | ratio |
|:--|--:|--:|--:|
| Redis p99 mean | 426.96 ms | 63.69 ms | 6.7x lower |
| Control p99 mean | 146.09 ms | 35.21 ms | 4.1x lower |
| Redis p50 mean | 6.55 ms | 5.59 ms | 1.2x lower |

## 🏁 Verdict

**Pursue, against the pre-set line — clear.** Redis arm p99 mean 63.69 ms
against a ≤250 ms line: 3.9x of headroom on the mean, and every individual
rep clears independently (worst rep 81.091 ms, still 3.1x under the line).
The correctness precondition held in all six runs. L3, as re-chartered,
passes.

**A correction to an earlier draft of this report, caught in review: the
control arm's improvement is not unexplained by the sampler fix, and this
report should not have implied it pointed away from the fix.** Both arms
of this apparatus start the same four-`Worker` pool under `NoOpMetrics`
(`src/main.rs:342-353, 637-651`); the choice of arm only changes whether
`RedisDispatch` is installed, not whether the four unguarded samplers ran.
Ledger #8's own diagnostic already measured their cost on the *control*
arm directly: quieting them (by decoupling the sampler cadence from
`poll_interval`) took the Postgres-only control from completing zero task
rows in 600 s to 26.09 tasks/s at the drain shape — a Postgres-arm-specific
effect, not a Redis-arm one. So a large control-arm improvement here is
exactly what fixing #1428 would predict, not a surprise that needs a host
explanation instead.

**What this run genuinely cannot separate is how much of *this specific
paced-shape* result is the sampler fix versus everything else that moved
between #8's baseline (`df4bd0d`) and this run's tree — caught late, and
incompletely, by this report's own review: not just the different
physical/container host and the `lru` 0.16.4→0.18.4 /
`hashbrown` 0.16.1→0.17.1 `Cargo.lock` bump, but at least two intervening
`worker.rs`-path changes.** #1478 (`39e5455`) added a periodic, per-shard
quota-key backfill reconciler that runs on the worker heartbeat cadence in
every `Worker`, in both arms, regardless of dispatch channel. Checked
directly against `quota_reconcile.rs`: this apparatus's one workflow
registers `quota: None` (`src/main.rs:289-292`), and
`registered_quota_workflow_names()` returning empty makes
`reconcile_quota_keys_from` return before issuing any SQL
(`quota_reconcile.rs:374-440`) — so, corrected from an earlier draft of
this paragraph that guessed "a near-bare `SELECT`," **the actual exposure
is not a query at all: the spawned loop still checks out a pooled
connection every heartbeat tick before that early return**
(`quota_reconcile.rs:584-586`), so the added cost is heartbeat-cadence
pool-checkout contention, not a recurring query.
#1447 (`1553c15`) replaced `persist_scheduled_activities`'s per-row
enqueue loop with a single `queue::enqueue_batch` call — built to remove
N round trips for a fan-out of N activities, and this apparatus's workflow
schedules exactly one activity per suspension (N=1), so the round-trip
count it targets is unchanged, but the SQL shape (batch-insert path vs.
the old single-row path) is not proven identical at N=1 and this report
does not attempt that proof. Neither addition is evidenced by this run's
own numbers as a real contributor the way the control arm's drop is
evidenced by #8's own diagnostic — they are named because they exist on
the path this workload exercises and their cost was never measured here,
not because anything in this run's data points to them specifically. This
list is not asserted complete either; it is what a first review pass
found, not an audited enumeration of every commit between the two trees.
**The pre-registered line is about the Redis arm's absolute number
regardless of attribution, so the pursue verdict does not depend on
resolving any of this.** A same-host, same-lockfile, same-source-tree
ablation (paced sweep with the sampler guard reverted, immediately
followed by the sweep with it restored, both on this container, nothing
else in the tree different between the two runs) would isolate the fix's
own contribution precisely and is the correct cheap follow-up if that
specific attribution matters to the decider; it was out of this assay's
time box.

**What this does answer directly:** on this machine, with the current
`trunk-dev` code, the Redis dispatch arm does not blow the 250 ms tail
line under the registered paced load. Issue #1429 item 1 can be downgraded
from "known miss, needs a mechanism fix" to "clears the line as measured
here; re-open if a same-host ablation or production telemetry shows the
sampler fix alone isn't carrying the result." That is a decision-grade
answer to the falsifiable question as pre-registered, with the attribution
caveat carried alongside it rather than smoothed over.

## 🔬 Reproduce

```bash
# Postgres 16 with fsync=off, synchronous_commit=off; Redis 7 with no
# persistence; both on loopback. (This session used the container's
# system postgresql/redis-server packages, not a fresh install script.)
sudo -u postgres psql -c "ALTER SYSTEM SET fsync = off;" \
                       -c "ALTER SYSTEM SET synchronous_commit = off;"
service postgresql restart
redis-server --daemonize yes --port 6379 --save "" --appendonly no

cd docs/assays/apparatus/0008-redis-dispatch-integrated
cargo build --release

# The paced sweep, identical invocation to ledger #8's own reproduce block.
# ~3 minutes.
ASSAY6_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/assay6" \
ASSAY6_ADMIN_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres" \
ASSAY6_SHAPES=paced ASSAY6_REPS=3 ASSAY6_PACED_SECS=30 \
ASSAY6_PACED_RATE_MILLI=86520 ASSAY6_CONTROL_CAP_SECS=180 \
  cargo run --release
```

Each run prints one `ASSAY6 ...` line with every field the table above
uses, then a summary table. The binary drops and recreates `assay6` before
every run, and deletes only the Redis keys under its own per-run prefix;
it never calls `FLUSHALL`.
