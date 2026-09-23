# 🚦 Semaphore CI health — shard 0's `integration_e2e`/`quota_enforcement_tests`
# collision drifted back AND grew a third member (`backup_verify_tests`);
# rebalanced from 11 to 17 shards. This is a confirmed, measured fix for
# the load imbalance, offered as a credible but NOT proven contributor to
# the still-unexplained `quota_enforcement_tests` hang the 2026-09-22
# report (`ci-health-semaphore-quota-outbox-root-cause.md`) tracks
# separately -- see that report for the hang's own mechanism, still open.

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

At the current count (11), shard 0 carries **323** combined
`#[tokio::test]`-weight; every other shard carries 33-149. At 17 shards,
the worst shard carries 174 -- roughly half -- and no single shard collides
two suites above a combined ~154. 17 was picked purely by this simulation,
the same method issue #1267 used to pick 11, scanning every count from 11
through 24 and taking the smallest resulting max-shard weight; no other
count in that range came close (the next-best, 20, still left the worst
shard at 189).

**Also newly noted, independent of the manifest drift:** shard 0 alone
carries an *unconditional* extra step, `Integration suites (partitioned
harvest_events layout)` (`if: ... && matrix.shard == 0`), re-running ~9
more manifest rows (~6 more minutes, per that step's own comment) against
`HARVEST_TEST_PARTITIONED=1`. That step is not manifest-row-driven and does
not move when the shard-count changes -- it is *always* shard 0's own extra
cost on top of whatever manifest weight lands there. Combined with the
weight-drift above, shard 0 was structurally the heaviest-loaded and
longest-running shard in the whole matrix by a wide margin, independent of
and in addition to the collision.

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

Before: shard 0 at 323 combined `#[tokio::test]`-weight (11 shards), the
heaviest by a wide margin, carrying `integration_e2e` (118),
`quota_enforcement_tests` (47), and `backup_verify_tests` (57), plus the
fixed extra `linuxpart` pass. After: 17 shards, worst shard at 174; the
three named suites land on three different shards (16, 10, 9 respectively)
under the new modulus, no two combining above ~154 anywhere in the matrix.
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
