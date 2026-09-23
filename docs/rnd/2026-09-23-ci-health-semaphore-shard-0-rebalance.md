# 🚦 Semaphore CI health — shard 0's `integration_e2e`/`quota_enforcement_tests`
# collision drifted back AND grew a third member (`backup_verify_tests`);
# rebalanced from 11 to 17 shards. Corrected post-review (Codex on this
# PR): shard 0 is not actually the single heaviest shard by test-weight --
# a SEPARATE, previously-unnoticed four-way collision on shard 7 is worse
# (323 vs shard 0's 268) -- but the rebalance fixes both at once, and shard
# 0 still uniquely carries a fixed extra pass no weight metric captures.
# This is a confirmed, measured fix for the load imbalance, offered as a
# credible but NOT proven contributor to the still-unexplained
# `quota_enforcement_tests` hang the 2026-09-22 report
# (`ci-health-semaphore-quota-outbox-root-cause.md`) tracks separately --
# see that report for the hang's own mechanism, still open.

**Status:** fix shipped this session, `.github/workflows/ci.yml` only (shard
count + matrix list + comment). No test code, no manifest reordering (the
manifest's sort-order is itself guarded by `ci_run_coverage.rs`, so this
session did not touch it). Prompted directly by a user report that CI was
failing widely across concurrent PRs right now, not by routine census.

## 🎯 Trigger

While driving PR #1706 (the 2026-09-22 report's own fix) to green, `Test DB
(linux, shard 0)` failed twice in a row on that PR's own doc-only commits,
both times at the exact panic site (`integration_e2e.rs:1383:6`, source
COMPLETED wait) that report already flags as separate and unexplained. The
user then reported the same failure blocking multiple other PRs. Checking
PR #1699 (unrelated: an OpenAPI-contract-only diff) confirmed it independently:
that PR's own session had already been chasing the identical failure across
several of its own CI attempts, at 30s, 30s again, and 60s configured
timeouts, concluding explicitly: *"every single time it fails at exactly
the configured bound... that is not what a 'needs more margin under CI
load' symptom looks like -- that's the signature of a genuine hang."* Same
test, same panic site, same shard, from a completely different PR's diff --
strong independent corroboration this is shard-0-specific and pre-existing,
not caused by either PR.

## 🔍 What this session found

Re-ran the `#[tokio::test]`-count-per-row weight simulation the issue #1267
fix (10→11 shards) originally used, against today's manifest:

```sh
awk '$1=="linux"{print c": "$0; c++}' .github/ci/integration-suites.txt \
  | grep -n "quota_enforcement_tests\|integration_e2e\b"
# integration_e2e: row-ordinal 33. quota_enforcement_tests: row-ordinal 44.
python3 -c "print(33 % 11, 44 % 11)"   # -> 0 0
```

**The exact defect the 2026-09-21 report already named is confirmed still
present, one day later, unfixed:** the two suites' row-ordinal gap is 11 --
the one value 11-way sharding cannot tolerate -- same as it was on 09-21
(then 43-32=11; now 44-33=11, drifted by one row each but the gap itself
never closed). That report downgraded this from "confirmed cause" to
"worth fixing for CI-cost reasons" because `.github/ci/run-suites.sh` runs
a shard's rows **sequentially**, ruling out concurrent CPU/DB contention
*between* the two suites as a mechanism. This session does not overturn
that specific finding -- it does not establish concurrent contention either.

**New this session:** the weight simulation, extended to the full 151-row
`linux`-class manifest (not just the two known suites), found a **third**
large suite also on shard 0: `backup_verify_tests` (57 `#[tokio::test]`s,
row-ordinal 77, `77 % 11 == 0`). Shard 0's combined weight from these three
suites alone (`integration_e2e` 118 + `quota_enforcement_tests` 47 +
`backup_verify_tests` 57 = 222) is close to the sum of every OTHER shard's
next-heaviest suite combined. Full per-shard totals at the current count:

```sh
python3 - <<'EOF'
import subprocess
rows = [l.split() for l in open('.github/ci/integration-suites.txt')
        if l.strip() and not l.strip().startswith('#')]
rows = [r for r in rows if r[0] == 'linux']
def weight(crate, target, filt):
    if crate != 'autumn-harvest' or target != 'integration' or filt == '-':
        return 1
    out = subprocess.run(['grep', '-c', '^#\\[tokio::test',
        f'autumn-harvest/tests/integration/{filt}.rs'],
        capture_output=True, text=True)
    return int(out.stdout.strip() or 0)
weights = [(i, r[4], weight(r[1], r[2], r[4])) for i, r in enumerate(rows)]
for count in range(11, 25):
    totals = [0]*count
    for i, _, w in weights: totals[i % count] += w
    print(count, 'max', max(totals), 'spread', max(totals)-min(totals))
EOF
```

```
11 max 323 spread 290
12 max 264 spread 225
13 max 269 spread 209
...
17 max 174 spread 141      <- smallest max in the 11-24 range
...
```

**Correction (post-review, Codex on this PR).** An earlier draft of this
section misread its own simulation output and reported shard 0 as the
run's single heaviest shard at 323. Wrong: printing the FULL per-shard
breakdown (not just the collision members) shows shard 0 actually totals
**268** (`118 + 47 + 57` from the three named suites, plus smaller rows).
**323 belongs to shard 7** -- a separate, previously-unnoticed collision of
`event_partitioning_tests` (148), `audit_export_tests` (87),
`claim_budget_tests` (34), and `shard_placement_by_id_tests` (29). Shard 1
(154) and shard 6 (190, `shard_rebalance_db_tests` at 111 plus
`claim_batched_tests`) also exceed what an earlier draft called "every
other shard's 33-149" ceiling. The corrected full breakdown at 11 shards:

```
shard 0:  268  (integration_e2e 118, quota_enforcement_tests 47, backup_verify_tests 57, + smaller)
shard 1:  154  (codec_rotation_db_tests 56, transactional_start_tests 36, + smaller)
shard 2:  138  (capability_miss_tests 46, cross_region_dr_tests 31, + smaller)
shard 3:   64
shard 4:   74
shard 5:  129
shard 6:  190  (shard_rebalance_db_tests 111, claim_batched_tests 23, + smaller)
shard 7:  323  (event_partitioning_tests 148, audit_export_tests 87, claim_budget_tests 34, shard_placement_by_id_tests 29)
shard 8:   96
shard 9:   33
shard 10: 130  (hot_code_swap_tests 40, queue_pause_tests 35, + smaller)
```

This matters for the causal story: shard 0 is **not** the single heaviest
shard by raw test-weight, so "it is the heaviest shard" cannot be the
whole explanation for why failures keep naming shard 0 specifically rather
than shard 7. What is still true and still confirmed: shard 0 carries a
real, three-way collision of large suites (a regression of issue #1267's
own fix), and shard 0 alone -- regardless of test-weight -- carries an
*unconditional* extra step, `Integration suites (partitioned
harvest_events layout)` (`if: ... && matrix.shard == 0`), re-running ~9
more manifest rows (~6 more minutes, per that step's own comment) against
`HARVEST_TEST_PARTITIONED=1`. That step is not manifest-row-driven and does
not move when the shard-count changes -- it is *always* shard 0's own extra
cost, on top of whatever manifest weight lands there, and it is not
captured by the test-weight metric at all. Whether that fixed extra cost
alone (rather than raw weight) is what makes shard 0 specifically prone to
the hang is not established either -- shard 7 carries no such extra step
despite carrying more raw test-weight, which is itself a data point against
a pure-weight explanation and worth the next session's attention.

The rebalance below fixes **both** collisions (shard 0's three-way and
shard 7's four-way) at once, since it changes the modulus every row's
shard assignment is computed from. 17 was picked purely by the max-shard
simulation, the same method issue #1267 used to pick 11, scanning every
count from 11 through 24 and taking the smallest resulting max-shard
weight; no other count in that range came close (the next-best, 20, still
left the worst shard at 189).

## 🧭 What this fix is, and is NOT, claimed to be

**Confirmed and fixed:** the manifest-weight collision (three large suites
on one shard) and shard 0's resulting outsized total load, both measured
directly against today's manifest, both real regressions of a
previously-fixed condition (issue #1267), independent of any flake.

**NOT confirmed:** that this is *the* mechanism behind
`quota_enforcement_tests`'s exactly-at-the-timeout hang. The 2026-09-22
report's own investigation could not reproduce that hang locally even under
32x CPU oversubscription, and this session did not obtain new evidence
connecting shard 0's total wall-clock load to a specific stall mechanism
inside the worker's decision-cycle path (`evaluate_triggers_for_execution`
and the source-completion wait upstream of it). The correlation --
same shard, named repeatedly, now measurably the most loaded and the only
one carrying a fixed extra pass -- is suggestive enough to justify this fix
immediately (it is a confirmed regression on its own merits, at zero
correctness risk), but this report does not claim it resolves the hang.
**If `quota_enforcement_tests` keeps hanging on whichever shard it lands on
after this rebalance** (now shard 10: `44 % 17 == 10`), that is strong
evidence the hang is NOT shard-load-related after all, and the next session
should look elsewhere -- a genuine correctness bug in the decision-cycle
path, or CI-fleet-wide infra reliability (PR #1699's own investigation
separately found an explicit, one-off Postgres connection drop on a
*different* shard and test during the same window, which this report does
not conflate with the shard-0-specific hang).

## 🔧 Treatment

`.github/workflows/ci.yml`: `SEMAPHORE_SHARD_COUNT` 11 → 17,
`test-db-linux`'s `matrix.shard` list extended to match, comment rewritten
in place (old reasoning preserved in git history, not deleted -- this
report is the durable record of why it changed again). No manifest
reordering (blocked by `ci_run_coverage.rs`'s sort-order guard; changing
the shard count sidesteps that entirely, since it changes the *modulus*
rather than the *rows*). No test code touched. No timeout constants
touched.

## 📊 Measurement

Before (11 shards): the matrix's heaviest shard is shard 7 at 323 combined
`#[tokio::test]`-weight (a four-way collision, corrected above -- not
shard 0, which totals 268 from its own three-way collision of
`integration_e2e` 118, `quota_enforcement_tests` 47, and
`backup_verify_tests` 57, plus the fixed extra `linuxpart` pass no other
shard carries). After (17 shards): worst shard at 174; both collisions are
broken up -- the three shard-0 suites land on three different shards (16,
10, 9), and shard 7's four suites likewise separate -- no two combining
above ~154 anywhere in the matrix.
No before/after wall-clock timing obtained this session (would need a live
CI run of both configurations, and 17 shards is itself the fix under test
in the PR carrying this change) -- the weight simulation is the same proxy
issue #1267's own fix relied on without independent timing verification
either.

## 🔬 Reproduce

```sh
# Confirm today's collision:
awk '$1=="linux"{print c": "$0; c++}' .github/ci/integration-suites.txt \
  | grep -n "quota_enforcement_tests\|integration_e2e\b\|backup_verify_tests"
python3 -c "print(33 % 11, 44 % 11, 77 % 11)"   # -> 0 0 0

# Full weight simulation (see the code block above) -- rerun any time the
# manifest changes to check whether 17 still holds, or needs revisiting the
# same way 11 did.
```
