# 🚦 Semaphore CI health — cache audit follow-up + new windows-nodb long pole

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
up on `docs/rnd/2026-09-05-ci-health-semaphore-cache-audit.md` (cache-correctness
finding, routed rather than fixed) and confirms/updates it against fresh production
data one day later, after two more CI-shaping merges landed in the interim
(`9ce7ce8`, "shard the test job's non-DB work across a new `test-nodb` matrix",
and the cache-audit report's own commit `6133540`). Two things changed since
yesterday's report and one didn't:

- **Changed, for the better:** end-to-end PR wall time is now ~59–66 minutes,
  down from the 123–177 minutes measured in the 2026-09-03/09-04 reports — a
  real, large win, credited to the `test-nodb` split, not to anything in this
  report.
- **Changed, for the worse:** the `test-nodb` split added 12 more independently
  cache-saving job legs (4 shards × 3 OSes) on top of the ~19 already in `ci.yml`,
  which is the wrong direction for the fixed-10GB-cache-budget hypothesis the
  prior report raised — and even though `test-nodb` and `test-db-linux` already
  use a **shared cache key across their own shards** (a mitigation already in
  the code, not something this report is proposing), the miss rate is unchanged.
- **Unchanged:** `Swatinem/rust-cache` still finds nothing to restore, on every
  sampled leg, on every sampled run, including the ones added since yesterday.

## 🎯 Verdict path (unchanged)

Same as both prior reports. Branch-protection status for `test-db-linux`'s 10
shards is still unconfirmed from this session (no branch-protection-read tool
available here — checked again today; still not among the GitHub MCP tools
exposed to this session). New since yesterday: the `test-nodb` job carries the
**identical** "not yet enforced" comment for its own 12 shards
(`ci.yml:384-390`), so the same unconfirmed gap now potentially applies to 22
non-required-by-name checks, not 10. Flagging again, not re-diagnosing — this
report has no more access to that setting than the 09-04 report did.

## 🌡️ Symptom

### 1. The sharding win, verified on three fresh full-matrix `pull_request` runs

| run | PR | event | workflow wall time (`created_at`→`updated_at`) | long-pole leg |
|---|---|---|---:|---|
| `34007199800` | #1213 TDD branch | pull_request | **58.9 min** | `Test (no-db, windows-latest, shard 2)` 46.9 min |
| `34014276155` | (pensive-brahmagupta branch) | pull_request | **64.9 min** | `Test (no-db, windows-latest, shard 3)` 50.2 min |
| `34018870064` | (cool-noether branch) | pull_request | **66.5 min** | `Test (no-db, windows-latest, shard 3)` 48.8 min |

All three are full-matrix runs (`needs.changes.outputs.code == 'true'`), 34 jobs
each. Compare against the pre-`test-nodb`-split baseline this role measured
09-03/09-04: 123.8–177.0 min. That's a **~47–67% cut** on the number that
actually gates a PR, comfortably clearing the ≥15% impact floor — via a fix
this role didn't ship (crediting `9ce7ce8` and the earlier `test-db-linux`
split, `ba3abe1`/PR #1336).

### 2. New long pole: `Test (no-db, windows-latest, shard N)`, consistently, all three samples

Full per-job breakdown of `34018870064` (34/34 jobs, sorted by wall time) shows
`Test (no-db, windows-latest, shard 3)` (48.8 min) atop every other leg by a
wide margin; the next 3 slots are also windows no-db shards (40.4, 37.5, 32.7
min). The other two samples show the identical shape — **windows no-db shards
are the top 3-4 slowest legs in all three runs**, ahead of every `test-db-linux`
shard and every ubuntu/macos no-db shard:

| leg | `34007199800` | `34014276155` | `34018870064` |
|---|---:|---:|---:|
| `Test (no-db, windows-latest, shard 0..3)` range | 29.0–46.9 min | 37.6–50.2 min | 32.7–48.8 min |
| `Test (no-db, ubuntu-latest, shard 0..3)` range | 27.3–33.0 min | 32.7–35.8 min | 27.0–33.2 min |
| `Test (no-db, macos-latest, shard 0..3)` range | not in top 10 | not in top 10 | 16.9–29.1 min |
| `Test DB (linux, shard 0..9)` range | 18–32 min | 16–34 min | 18–31 min |
| `Test (windows-latest)` (the leg `test-nodb` was split out of) | — | — | 11.0 min |

Windows no-db shards run consistently ~30–45% slower than their ubuntu
counterparts in every sample, and the *original*, now-slimmed `Test
(windows-latest)` leg (11.0 min) shows the split correctly moved the weight
out — this isn't the old windows-vs-other-OS gap resurfacing in the leg that
was already fixed, it's the same underlying tax following the work into its
new home.

### 3. The cache-miss finding reproduces, unchanged, including on the new legs

Grepped raw job logs (same method as the 09-05 report — `get_job_logs`'s tail
window doesn't reach the cache-restore step, so full log archives via
`get_workflow_run_logs_url` were pulled and grepped locally) for run
`34018870064`, five representative legs spanning four different job families:

| leg | cache result |
|---|---|
| `Lint` | **No cache found.** (new sample — not checked in the 09-05 report) |
| `Test (ubuntu-latest)` | **No cache found.** |
| `Test (windows-latest)` | **No cache found.** |
| `Test DB (linux, shard 0)` | **No cache found.** |
| `Test (no-db, windows-latest, shard 3)` | **No cache found.** (new job family, added since the 09-05 report) |

5/5 sampled legs, 0 hits. Combined with the 09-05 report's own 4/5-miss sample
(different run, different legs), that's **9/10 sampled leg-runs across two
independent days finding no cache at all**, now spanning every job family this
role has checked directly (`lint`, `test`, `test-db-linux`, and the new
`test-nodb`).

### 4. The shared-key mitigation is already in place and hasn't moved the needle

`ci.yml:411-417` (`test-nodb`) and the equivalent block in `test-db-linux` both
set `shared-key: test-nodb-${{ matrix.os }}` / a shared key across all shards,
specifically so a shard needing a binary another shard already built doesn't
start cold — i.e., the "narrow the number of independently-cached legs"
remedy the 09-05 report listed as candidate #2 is **already implemented** for
these two job families, pre-dating or concurrent with that report. It hasn't
helped: shard 0 and shard 3 of the same OS, in the same run, sharing the same
cache key, both report `No cache found.` This rules out per-shard key
multiplicity as a sufficient explanation for the miss rate on its own — the
09-05 report's "shared 10GB repository-wide budget across ~19 (now ~34)
independently-saving job legs" hypothesis is the one this new data is
consistent with; a per-family key-sharing fix alone doesn't reach it because
the contention is across families and across concurrent runs, not within one
family's own shards.

## 🔍 Diagnosis

**Category: cache correctness/capacity — same category as the 09-05 report,
not a new finding, but a wider and more resistant-to-the-obvious-fix one.**
Not a flake (every sampled run is green; this is deterministic, reproducible
absence of a cache hit, not nondeterminism) and not a product bug (nothing
about the execution engine is implicated; this is CI configuration). The
`test-nodb` split — itself a good, already-landed, measured win — mechanically
made the capacity problem this report is tracking bigger: it added 12 more
`save-if: true` legs contending for the same fixed cap, on the same day the
09-05 report's hypothesis named that exact mechanism as the likely cause.
Windows paying the largest share of the miss's cost (longest per-family
compile times to begin with, per every prior report in this series, now
cache-cold on top of that) is consistent with, not independent of, this
finding.

## 🔧 Treatment — still routed, not applied

Same reasoning as the 09-05 report, sharpened by one new data point: **do not
propose "narrow the cache scope per job" as a standalone remedy any more** —
it's already done for the two job families that could most easily do it, and
it didn't help, so it isn't a free fix waiting to be picked up. What's left
still needs the same thing the 09-05 report couldn't get: **actual cache-usage
bytes and eviction frequency**, which requires either the Settings → Actions →
Caches UI or `gh api repos/autumn-foundation/autumn-harvest/actions/caches`
with admin-scoped auth — neither available to this session (checked again;
the GitHub MCP tools exposed here still have no cache-usage or cache-listing
method). Candidate remedies for whoever has that access, updated:

1. **Confirm total bytes and eviction frequency first** (unchanged ask).
2. If confirmed capacity-bound: **concentrate saves rather than spreading
   them** — e.g. designate one canonical job per OS per job-family (or even
   per OS across families, if `target/` layouts overlap enough) as the only
   `save-if: true` writer, with the rest restore-only. This is a different,
   more targeted shape than "narrow scope," which this report now has
   evidence against. Still new CI-config policy, still routed rather than
   shipped, for the same "ask before: caching services" reason as yesterday.
3. Or accept the cost and pay for GitHub's larger cache tier (explicit new
   spend — ask before, as always).
4. Branch protection: same unresolved ask as the 09-04/09-05 reports, now
   covering 22 non-`test`/`test-db-linux` shard checks (10 + 12) instead of
   10 — a repo admin needs to check Settings → Branches once for both job
   families together.

## 📊 Measurement

- **Before (09-03/09-04 reports' baseline):** 123.8–177.0 min end-to-end PR
  wall time, pre-`test-nodb`-split.
- **After (this report, 3 fresh samples):** 58.9–66.5 min. **~47–67% reduction**,
  clearing the ≥15% floor by a wide margin. Not this report's fix — crediting
  `9ce7ce8` and PR #1336's `test-db-linux` split.
- **Cache hit rate, cumulative across two independent report-days:** 0/9 sampled
  leg-runs found an exact-match cache; 1/10 total found a prefix-fallback hit
  (09-05 report, `windows-latest`, not reproduced today). No revert check
  applies here — this is a report, not a fix, so there is nothing to verify
  went red-then-green; the "after" measurement above is the sharding win's,
  not this report's own.
- **Rerun-button census (same protocol as prior reports):** 0/100 sampled
  `pull_request`-event `ci.yml` runs show `run_attempt > 1`. Unchanged from
  the 09-03 report's finding — still no reflexive-rerun culture.

## 🔬 Reproduce

```sh
# Full run logs (get_job_logs's tail window doesn't reach the cache-restore
# step in a job this long):
# via actions_get(method="get_workflow_run_logs_url", resource_id=<run_id>)
unzip -o logs.zip -d logs
grep -n "Restoring cache\|Cache hit\|No cache found\|Cache Key:" logs/*.txt

# Rerun-button census:
# via actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event: "pull_request", status: "completed"}, perPage=100)
# then: jq '[.workflow_runs[] | select(.run_attempt > 1)]'

# Per-job wall time for a run:
# via actions_list(method="list_workflow_jobs", resource_id=<run_id>, perPage=100)
```

Cache-usage confirmation: still not run in this session, still no tool access
(checked again today — same gap as the 09-05 report). A repo admin can confirm
via Settings → Actions → Caches, or
`gh api repos/autumn-foundation/autumn-harvest/actions/caches --paginate`.
