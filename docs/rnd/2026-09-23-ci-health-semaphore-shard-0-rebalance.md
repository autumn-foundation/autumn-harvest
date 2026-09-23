# 🚦 Semaphore CI health — shard 0's `integration_e2e`/`quota_enforcement_tests`
# collision drifted back; rebalanced from 11 to 20 shards for a real,
# measured (not eliminated) load-balance improvement. Corrected twice
# post-review (Codex on this PR) after this report's own weight simulation
# was found flawed: it undercounted every `autumn-harvest-plugin` row as
# weight 1, which hid that shard 7 -- not shard 0 -- was the actual
# heaviest shard, and that PR #1703 (open, unmerged, a proper harness)
# had already run the correct version of this exact analysis and reached
# a stronger, humbler conclusion this report now defers to: no shard
# count from 9-24 eliminates the manifest's heavy-suite collisions. This
# fix is a real, partial improvement, not a solved sharding problem, and
# not a proven fix for the `quota_enforcement_tests` hang tracked
# separately in `ci-health-semaphore-quota-outbox-root-cause.md`.
#
# UPDATE, same session, this PR's own first live CI run: the falsifiable
# prediction below came true almost immediately.
# `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
# hung again -- identical panic site, identical signature -- on `Test DB
# (linux, shard 4)` under the NEW 20-shard layout, where this session's own
# harness run confirmed it lands **completely isolated**, no other heavy
# suite on that shard at all. Shard load/collision is now confirmed NOT
# the cause of this specific hang. See the "🔬 Live CI result" section
# near the end for the full writeup.

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
PR #1699 (unrelated: an OpenAPI-contract-only diff) confirmed it
independently: that PR's own session had already been chasing the identical
failure across several of its own CI attempts, at 30s, 30s again, and 60s
configured timeouts, concluding explicitly: *"every single time it fails at
exactly the configured bound... that is not what a 'needs more margin under
CI load' symptom looks like -- that's the signature of a genuine hang."*
Same test, same panic site, same shard, from a completely different PR's
diff -- strong independent corroboration this is shard-0-specific and
pre-existing, not caused by either PR.

## 🔍 What this session found -- and got wrong twice before landing here

**First pass (wrong, corrected by Codex's first review round):** a quick
weight simulation using `grep -c '^#\[tokio::test' autumn-harvest/tests/
integration/<filter>.rs` per manifest row, counting only
`autumn-harvest/integration` rows and treating every other row (including
every `autumn-harvest-plugin` row) as flat weight 1. This found shard 0 at
"323, the matrix's heaviest shard" and picked 17 shards as a claimed
collision-free fix. Codex's review re-ran the same simulation and got
different numbers: shard 0 totals 268, and a **separate, previously-missed
four-way collision on shard 7** (`event_partitioning_tests` 148 +
`audit_export_tests` 87 + `claim_budget_tests` 34 +
`shard_placement_by_id_tests` 29 = 298, plus smaller rows to 323) is
actually the heaviest. First correction: fixed the numbers, kept 17 shards
(still computed as collision-free for the *known* heavy suites).

**Second pass (also wrong, corrected by Codex's second review round):**
Codex's own follow-up review caught the deeper problem: five manifest
`filter` values (`admission_gate_authoritative`, `force_fail`,
`rate_limit_key`, `dag_compensation`, `nd_block`) are Cargo test-name
**prefixes**, not filenames -- `run-suites.sh` passes them straight through
to `cargo test <filter>`, which matches every test whose name *contains*
that string, potentially across several files sharing one compiled binary
(`tests/integration/mod.rs` declares ~200 modules in one target). The
naive `grep -c ... {filt}.rs` silently returns 0 for a row like this
(wrong file, or no such file at all), understating those five rows' real
weight (34/18/9/27/19 respectively). Recomputing over 9-24 shards with
this fixed, the true global max at 17 shards is **not** 174 -- it is
higher, and 17 is not the best available count either.

**What actually resolves this properly, and was already sitting in an
open, unmerged PR this session had not checked before its first pass:**
PR #1703 (`🚦 Semaphore: shard-weight drift harness`, opened the day
before this one, still open) built exactly this analysis correctly --
`docs/audits/shard-weight-drift.py`, which resolves every manifest row's
real weight, including plugin-crate rows and prefix-filtered substring
matches, self-tests, and sweeps `SEMAPHORE_SHARD_COUNT` 9 through 24. Its
own conclusion, reached independently and a day earlier, is stronger and
more honest than either of this report's first two attempts: **no shard
count in that range eliminates the manifest's heavy-suite collisions.**
Running that harness (fetched from the open PR's branch, not merged into
this one -- see Treatment) against today's manifest:

```
N= 9  spread= 294  max= 472  heavy_collisions=5
N=11  spread= 328  max= 426  heavy_collisions=7
N=17  spread= 237  max= 318  heavy_collisions=7
N=20  spread= 179  max= 247  heavy_collisions=5   <- best max AND best spread in 9-24
N=24  spread= 210  max= 253  heavy_collisions=6
```

20 is the best-performing count in the swept range on both metrics, but it
still leaves **5** shards carrying two or more heavy (>=30-test) suites --
this is an improvement (max drops from 426 to 247, spread from 328 to 179),
not a fix for the underlying structural problem. PR #1703's own comment
says it plainly: *"bumping SEMAPHORE_SHARD_COUNT again would likely just
relocate the collision, not remove the failure mode... the durable fix is
a weight-aware assignment... a bigger change than one CI-health session
should make unilaterally without live-CI validation."* This report agrees
and does not attempt that bigger change.

**One concrete, specific win this rebalance does achieve:** at N=20,
`quota_enforcement_tests` (the test actually motivating this fix) lands
alone on its own shard, isolated from every other heavy suite -- versus
sharing shard 0 with `integration_e2e` and `backup_verify_tests` at N=11.
Whether that isolation actually helps with the hang is not established
(see below).

## 🧭 What this fix is, and is NOT, claimed to be

**Confirmed and fixed:** a real, measured reduction in the worst-case
shard's total test-weight (426 → 247) and in the spread across shards
(328 → 179), using PR #1703's own validated harness. `quota_enforcement_tests`
specifically no longer shares a shard with another heavy suite.

**NOT confirmed, and not claimed:**
- That the sharding is now collision-free. It is not -- 5 shards still
  carry 2+ heavy suites at N=20, matching PR #1703's own finding that no
  N in 9-24 achieves that.
- That this is *the* mechanism behind `quota_enforcement_tests`'s
  exactly-at-the-timeout hang. The 2026-09-22 report's own investigation
  could not reproduce that hang locally even under 32x CPU
  oversubscription. PR #1703's own further findings (its companion health
  report, `docs/rnd/2026-09-22-ci-health-semaphore-shard-weight-harness-
  and-materialized-cte-bug.md`) include a data point *against* a pure
  shard-weight explanation: a failure cascade that hit even the manifest's
  *lightest* shard in one observed run. This report does not dismiss that
  counter-evidence -- it is a real reason to doubt that shard load alone
  explains the flakes, and the next session should weigh it seriously
  before assuming this rebalance fixes anything beyond the load imbalance
  itself.
- That shard 0's specific, oft-named role in the hang reports is explained
  by test-weight at all -- shard 7 carries comparable or greater raw
  weight under the old scheme without a comparable hang report. What
  remains specific to shard 0 is the *unconditional* extra step,
  `Integration suites (partitioned harvest_events layout)`
  (`if: ... && matrix.shard == 0`), re-running ~9 more manifest rows (~6
  more minutes) that no test-weight metric captures and no other shard
  runs -- a real structural asymmetry, but still not proven to cause the
  hang.

**If `quota_enforcement_tests` keeps hanging on whichever shard it lands on
after this rebalance**, that is real evidence against any shard-load
correlation, and the next session should look elsewhere entirely -- a
genuine correctness bug in the worker's decision-cycle path, or CI-fleet
infra reliability (PR #1699's own investigation separately found an
explicit, one-off Postgres connection drop on a *different* shard and test
in the same window, which this report does not conflate with the
shard-0-specific hang pattern).

## 🔧 Treatment

`.github/workflows/ci.yml`: `SEMAPHORE_SHARD_COUNT` 11 → 20,
`test-db-linux`'s `matrix.shard` list extended to match, comment rewritten
in place -- including an explicit note of this report's own two correction
rounds, kept there rather than only in git history, since a future reader
of the live comment is the one who most needs to know a bigger modulus was
already shown not to fully solve this. No manifest reordering (blocked by
`ci_run_coverage.rs`'s sort-order guard; changing the shard count sidesteps
that entirely, since it changes the *modulus* rather than the *rows*). No
test code touched. No timeout constants touched. `docs/audits/
shard-weight-drift.py` itself is **not** copied into this PR -- it belongs
to PR #1703, which is still open; duplicating the file here would conflict
whichever PR merges second. This report's numbers come from running that
harness locally against PR #1703's branch, not from committing a copy.

## 📊 Measurement

Before (11 shards, PR #1703's harness, the corrected numbers): max
shard weight 426, spread 328, 7 shards with heavy-suite collisions. After
(20 shards): max 247, spread 179, 5 shards with heavy-suite collisions.
`quota_enforcement_tests` moves from a 3-way collision to isolation. No
before/after wall-clock timing obtained this session (would need a live CI
run of both configurations, and 20 shards is itself the fix under test in
the PR carrying this change) -- the weight simulation is the same proxy
issue #1267's own fix relied on without independent timing verification
either, now computed with PR #1703's more careful methodology instead of
this report's own first two flawed attempts.

## 🔬 Reproduce

```sh
# This session's own two simulation bugs, for the record -- do not repeat
# them:
# 1. Counting only `autumn-harvest/integration` rows and flat-1 everything
#    else undercounts every autumn-harvest-plugin row (some, like
#    ui_integration at 148 tests, are huge).
# 2. Resolving a manifest `filter` column to a file named `<filter>.rs`
#    breaks for prefix/substring filters (`run-suites.sh` passes these
#    straight to `cargo test <filter>`, which does substring matching
#    across every test in the compiled binary, per PR #1703's own README
#    section on this).

# The correct tool, from the open PR that already built it properly:
git fetch origin claude/youthful-mendel-oykopw
git show origin/claude/youthful-mendel-oykopw:docs/audits/shard-weight-drift.py \
  > /tmp/shard-weight-drift.py
# Must run from the repo root with a matching relative path structure --
# REPO_ROOT is computed from the script's own file location, so either
# copy it to docs/audits/ temporarily (do not commit) or adjust that
# constant:
cp /tmp/shard-weight-drift.py docs/audits/shard-weight-drift.py
python3 docs/audits/shard-weight-drift.py --self-test   # -> self-test: ok
python3 docs/audits/shard-weight-drift.py --sweep        # -> the N=9..24 table
rm docs/audits/shard-weight-drift.py   # do not commit this copy
```

## 🔬 Live CI result: the falsifiable prediction came true

This report's own "What this fix is NOT claimed to be" section said: *"If
`quota_enforcement_tests` keeps hanging on whichever shard it lands on
after this rebalance, that is real evidence against any shard-load
correlation, and the next session should look elsewhere."* This PR's own
first CI run answered that question before the PR even finished review.

`Test DB (linux, shard 4)` (run `35807566215`, commit `7eed00e`, this PR's
own branch) failed: `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
panicked at the identical site, `integration_e2e.rs:1383:6` (the
SOURCE-completion wait), the same signature every prior occurrence has
shown. Per this session's own `shard-weight-drift.py` run at N=20 (see
above), shard 4 carries `quota_enforcement_tests` **completely isolated**
-- no other heavy suite, total shard weight 115, nowhere near the matrix's
247 maximum. This PR's own manifest change does not touch
`quota_enforcement_tests.rs` at all, so this run exercises the SAME
unmodified test code the 2026-09-22 report already investigated, now on a
shard purpose-built to rule out collision as a factor.

**This confirms, as directly as a live CI run can, that shard load and
suite collisions are not the cause of this hang.** The correlation with
shard 0 that motivated this whole PR was real (shard 0 did carry a
confirmed collision) but coincidental to the actual mechanism, not causal
to it -- consistent with, and now reinforcing, PR #1703's own companion
report finding a cascade that hit even the manifest's *lightest* shard.

**What this means going forward:** this PR's shard rebalance remains a
valid, measured CI-cost improvement (worst-shard load down ~42%) and is
being kept for that reason alone. It is retracted as any kind of
mitigation for `quota_enforcement_tests`'s hang. The hang itself is
unexplained, reproduces on isolated shards under normal (not elevated)
load, and needs a fundamentally different investigation than anything
tried across this report, the 2026-09-22 report, or PR #1703's own
session -- something in the worker's decision-cycle path itself
(`evaluate_triggers_for_execution` and everything upstream of it up to
task claim), or an environment difference this session's local
reproduction never exercised (CI's `testcontainers`-managed, freshly
started-per-suite Postgres container, versus every local reproduction
attempt across this whole report series, which used a persistent,
already-migrated Postgres via `HARVEST_TEST_DATABASE_URL` -- a materially
different code path through `setup_test_database_url_or_env()` that no
session has yet tried to reproduce locally with an actual Docker
container in the loop).
