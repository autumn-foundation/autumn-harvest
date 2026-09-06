# 🚦 Semaphore CI health — cache audit follow-up + new windows-nodb long pole

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
up on `docs/rnd/2026-09-05-ci-health-semaphore-cache-audit.md` (cache-correctness
finding, routed rather than fixed) and confirms/updates it against fresh production
data one day later, after two more CI-shaping merges landed in the interim
(`9ce7ce8`, "shard the test job's non-DB work across a new `test-nodb` matrix",
and the cache-audit report's own commit `6133540`). Two things changed since
yesterday's report and one didn't:

- **Changed, for the better:** full-workflow wall time is now ~59–66 minutes,
  down from the 123–177 minutes measured in the 2026-09-03/09-04 reports — a
  real, large win, credited to the `test-nodb` split, not to anything in this
  report. (Not necessarily *gating* time — see §1's caveat.)
- **Changed, for the worse:** the `test-nodb` split added 3 more distinct
  persisted cache entries (one per OS — the job already uses a **shared cache
  key across its 4 shards**, `ci.yml:411-417`, so this is 3 entries, not 12)
  on top of roughly **10**, not the 09-05 report's "~19" (a Codex review
  comment on this PR caught that the same job-count-vs-distinct-key
  conflation applies to that inherited figure too: `test-db-linux`'s 10
  shards already collapse to 1 entry via their own shared key, so ~10 distinct
  key *names* pre-`test-nodb`, ~13 after, **per branch** — a second Codex
  comment then caught that this is still not the repository-wide total: the
  10GB cap is shared across every branch, and each of this repo's many
  concurrently open PR branches can persist its own copy of these same ~13
  names, so the true total is unknown and plausibly a multiple of 13 — see
  Diagnosis). Still the wrong direction for the fixed-10GB-cache-budget
  hypothesis the prior report raised, just a smaller wrong direction than
  either report claimed, and likely an understatement in the other direction
  too.
- **Unchanged:** no sample in this report — 5/5 fresh legs today, plus 3/3
  downstream restores in §4 — found any cache at all, exact or fallback (a
  Codex review comment on this PR caught an earlier draft overgeneralizing
  this to "every sampled run," which contradicts the one prefix-fallback hit
  the 09-05 report found and this report's own cumulative 9/10 tally
  includes). That includes a save this report can show raised no error
  anywhere in its path on the shared base branch, under a stable key three
  later PR runs all restored against — and still wasn't there 9-14 hours on
  (§4).

**Correction record for this PR (`#1395`):** eight separate Codex review
comments caught real problems in earlier drafts of this report — two
methodology errors in §4's cache-eviction comparison (a same-run comparison
that couldn't show what was claimed, then a cross-PR-branch comparison
GitHub's cache scoping rules make invalid), one overclaim in §1 (calling
full-workflow wall time "the number that actually gates a PR" while this
report's own Verdict-path section leaves that unconfirmed), one overstated
certainty in §4 (treating an upload-progress line as proof of a finalized
cache save), one miscounted baseline (the inherited "~19" job-count figure
conflated with distinct persisted keys the same way this report's own
"12 vs 3" fix already corrected for `test-nodb`, just not carried back to
the older number), one further scoping gap in that same count (~13 is a
per-branch figure, not the unknown repository-wide total the shared 10GB cap
actually contends over), one internally-inconsistent denominator in the
Measurement section's cache-hit tally, and one overgeneralized summary claim
(the top bullets said no cache was found "on every sampled run," which
contradicts the one prefix-fallback hit this report's own cumulative tally
already counts). All eight are fixed below, in place, with the retractions
left visible rather than edited away.

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
09-03/09-04: 123.8–177.0 min. That's a **~47–67% cut** in full-workflow wall
time — the same `created_at`→`updated_at` proxy the 09-04 report used for
"time to green" — clearing the ≥15% impact floor on that measure, via a fix
this role didn't ship (crediting `9ce7ce8` and the earlier `test-db-linux`
split, `ba3abe1`/PR #1336).

**Caveat, raised by a Codex review comment on this PR:** calling this "the
number that actually gates a PR" (an earlier draft's wording) overclaims
given the Verdict-path section's own unconfirmed branch-protection status —
`test-nodb`'s 12 checks, including the very long-pole legs this table cites,
are explicitly *not* required status checks per `ci.yml:384-390` unless a
repo admin has since added them (still unconfirmed, same gap as the 09-04/
09-05 reports). If they're not required, a PR could become mergeable before
the full workflow — including the long-pole `test-nodb` shard — finishes,
and the true gating-time reduction could be smaller than the full-workflow
number above (though `test-db-linux`'s status is equally unconfirmed in the
other direction, so this can't be resolved without the same branch-protection
read this report already can't get). Retracted the stronger claim; reporting
this as full-workflow completion time, not confirmed gating time.

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

### 4. The shared-key mitigation, checked correctly this time — and a confirmed base-branch save that still didn't survive

**Two corrections, both from Codex review comments on this PR (`#1395`),
both correct, kept here rather than silently folded away:**

1. An earlier draft compared shard 0 and shard 3 of `test-nodb`'s
   `windows-latest` group *within the same run*, both showing
   `No cache found.`, and argued that ruled out per-shard key multiplicity.
   Invalid: every shard in a matrix restores near its own job's start,
   seconds apart, before any sibling can possibly have finished and saved —
   two same-run misses are guaranteed regardless of whether the shared key
   works. Retracted.
2. The next draft replaced it with a cross-run comparison between
   `34007199800` and `34018870064` — but those are two **different PR head
   branches** (`claude/issue-1213-tdd-b76wjx` and `claude/cool-noether-mqc5qx`
   respectively). GitHub Actions scopes a cache to the branch that created
   it, plus (for a `pull_request`-triggered run) that PR's base branch —
   **not** to other PRs' branches. A cache saved on one PR's branch is
   structurally invisible to a different PR's branch regardless of
   capacity, so that comparison couldn't have shown eviction either. Also
   retracted, per the reviewer's own suggested fix: "use a later run/rerun
   of the same PR or a cache saved on the default branch."

**The valid version, using exactly that suggested fix:** trunk-dev is the
base branch both retracted PRs (and every PR sampled in this report) target
— confirmed for `34007199800`'s branch via its PR, #1385, which lists
`trunk-dev` as base. The most recent *completed* (non-cancelled) `push`-event
`ci.yml` run on `trunk-dev` before any of today's three samples is
`33981875800` (commit `6133540`, the 09-05 cache-audit report's own docs
commit — completed 2026-09-05T18:05:34Z). Its `test-nodb`/`windows-latest`
shard 0 restores under `Restore Key`
`v0-rust-test-nodb-windows-latest-Windows_NT-x64-8918a2f9` /
**exact** `Cache Key: ...-78f0168d` — the identical exact key all three of
today's PR runs restored against (§ table above) — gets `No cache found.` on
restore, then attempts a save: `Sent 149848672 of 149848672 (100.0%)` at
2026-09-05T18:04:43Z, immediately followed by normal job cleanup with no
error.

**A third Codex review comment on this PR correctly notes** that `Sent ...
(100.0%)` only confirms the archive *upload* completed, not that the cache
service's separate commit/finalize step succeeded — that step can still fail
after a 100%-complete transfer, especially under shared-key contention. This
report can't inspect the cache-usage API to see the finalized entry directly
(the same access gap noted throughout). What it can point to: the exact same
job step, for the three sibling shards under this identical key, explicitly
logs `Failed to save: Unable to reserve cache with key ...-78f0168d, another
job may be creating this cache` the moment ANY part of the save fails — this
run's winning shard shows no such message, or any other error, anywhere
after `Saving cache ...`. That's evidence the whole save path (reserve,
upload, and whatever commit step follows) raised nothing this tool's error
path would have caught and logged, which is short of a positive "cache saved
successfully" confirmation (this rust-cache/toolkit version doesn't appear
to log one, checked in both this log and the 09-06 samples) but is stronger
than upload-progress alone. Treating this as best-available, not certain,
evidence that a cache existed under this key at this time.

Every one of today's three PR runs restores against that same exact key,
later, from a different branch that has trunk-dev as its base (so trunk-dev's
cache is in scope per GitHub's documented fallback) — and all three still
report `No cache found.`:

| PR run | branch | restore time | elapsed since the `trunk-dev` save |
|---|---|---|---:|
| `34007199800` | `claude/issue-1213-tdd-b76wjx` | 2026-09-06T02:55:39Z | 8h51m |
| `34014276155` | `claude/pensive-brahmagupta-knhvu2` | 2026-09-06T05:48:29Z | 11h44m |
| `34018870064` | `claude/cool-noether-mqc5qx` | 2026-09-06T07:38:28Z | 13h34m |

This is the comparison the reviewer asked for — a cache saved on the default
branch, checked against later runs — and it reproduces the same conclusion a
third time, on markedly firmer ground than the two retracted drafts: **a
cache save that raised no error anywhere in its path, on the shared base
branch, under the exact key three separate PR runs later restored against,
was unavailable to all three within 9-14 hours.**
That's consistent with the 09-05 report's shared-10GB-repository-wide-budget
hypothesis (fast enough turnover elsewhere evicting this entry before any PR
got to use it) and not with a branch-scoping or key-configuration mistake —
the shared-key mechanism and the base-branch fallback both work as designed;
the entry just doesn't live long enough to be useful to anyone downstream of
`trunk-dev`.

## 🔍 Diagnosis

**Category: cache correctness/capacity — same category as the 09-05 report,
not a new finding, but now confirmed with a sounder method.** Not a flake
(every sampled run is green; this is deterministic, reproducible absence of
a cache hit, not nondeterminism) and not a product bug (nothing about the
execution engine is implicated; this is CI configuration). The `test-nodb`
split — itself a good, already-landed, measured win — mechanically made the
capacity problem this report is tracking somewhat bigger: it added 3 more
distinct persisted cache entries (one per OS; the error-free upload sizes
seen in §4 range from 150MB on a docs-only trunk-dev push to 853MB on a full
compile) on top of roughly 10 pre-existing distinct entries (not the 09-05
report's ~19, which counted matrix legs rather than distinct keys — that
report's own text already noted `test-db-linux`'s 10 shards share one key,
but summed as if they didn't; a Codex review comment on this PR caught the
inconsistency with this report's own corrected `test-nodb` count), so ~13
distinct key *names* this workflow uses **per branch, per current
`Cargo.lock` hash** — not the repository-wide total contending for the
shared cap. A Codex review comment on this PR correctly flagged that gap:
GitHub's cache access is scoped per branch/ref, but its 10GB capacity limit
is shared across the *whole repository* — so every one of the dozens of
concurrently open PR branches this report has seen in this repo's history
(§4 alone samples three) can independently persist its own copy of these
same ~13 key names, and any not-yet-evicted older `Cargo.lock`-hash
generation adds more on top. The true repository-wide entry count is
unknown — this report still has no cache-listing API access to measure it
— and is very plausibly a multiple of 13, not 13 itself, which if anything
makes the capacity hypothesis this section is arguing for *more*, not less,
plausible. Corrected to avoid overstating a number this report can't
actually measure. §4's
cross-run evidence — an apparently error-free save on the shared base
branch, gone within 9-14 hours under the exact key three separate downstream
PR runs restored against — is direct support for that hypothesis, not just
consistent with it. Windows paying the
largest share of the miss's cost (longest per-family compile times to begin
with, per every prior report in this series, now cache-cold on top of that)
is consistent with, not independent of, this finding.

## 🔧 Treatment — still routed, not applied

Same reasoning as the 09-05 report, sharpened by the §4 cross-run evidence:
**"narrow the cache scope per job" (already done for `test-nodb`/
`test-db-linux` via shared shard keys) demonstrably isn't sufficient on its
own** — a correctly-keyed, error-free save still didn't survive to the next
run. What's left still needs the same thing the 09-05 report
couldn't get: **actual cache-usage bytes and eviction frequency**, which
requires either the Settings → Actions →
Caches UI or `gh api repos/autumn-foundation/autumn-harvest/actions/caches`
with admin-scoped auth — neither available to this session (checked again;
the GitHub MCP tools exposed here still have no cache-usage or cache-listing
method). Candidate remedies for whoever has that access, updated:

1. **Confirm total bytes and eviction frequency first** (unchanged ask).
2. If confirmed capacity-bound: **consolidate entries across job families**
   — e.g. one shared cache key across `test-nodb`, `test-db-linux`, `test`,
   and `lint` for a given OS, if their `target/` layouts overlap enough,
   reducing the number of distinct key *names* below the current ~13 per
   branch (multiplied by however many branches actually hold a copy — the
   unknown repository-wide total from the Diagnosis section above; this
   remedy shrinks that multiplier's per-branch factor, which helps regardless
   of the unknown total)
   (a Codex review comment on this PR correctly flagged that doing this
   *within* a family — e.g. a designated canonical shard for `test-nodb` —
   is not this fix: `ci.yml:411-417`'s shared shard-key already limits each
   family to one persisted entry per OS per run via the reserve-race this
   report observed directly; a canonical writer would stop the other three
   shards from wastefully attempting a save that was always going to fail,
   which is worth doing for its own sake, but it doesn't reduce stored bytes
   or relieve capacity pressure, so it isn't a capacity remedy on its own).
   Still new CI-config policy, still routed rather than shipped, for the
   same "ask before: caching services" reason as yesterday.
3. Or accept the cost and pay for GitHub's larger cache tier (explicit new
   spend — ask before, as always).
4. Branch protection: same unresolved ask as the 09-04/09-05 reports, now
   covering 22 non-`test`/`test-db-linux` shard checks (10 + 12) instead of
   10 — a repo admin needs to check Settings → Branches once for both job
   families together.

## 📊 Measurement

- **Before (09-03/09-04 reports' baseline):** 123.8–177.0 min end-to-end PR
  wall time, pre-`test-nodb`-split.
- **After (this report, 3 fresh samples):** 58.9–66.5 min full-workflow wall
  time (not confirmed gating time — see §1's caveat). **~47–67% reduction**,
  clearing the ≥15% floor on that measure by a wide margin. Not this report's
  fix — crediting `9ce7ce8` and PR #1336's `test-db-linux` split.
- **Cache hit rate, cumulative across two independent report-days:** 0/10
  sampled leg-runs found an exact-match cache (a Codex review comment on this
  PR caught an earlier draft's `0/9` as inconsistent with its own `1/10` —
  both figures need the same 10-sample denominator); within those same 10,
  1 found a prefix-fallback hit, still not exact (09-05 report,
  `windows-latest`, not reproduced today), so 9/10 found no cache at all.
  Additionally: 3
  downstream PR runs (§4), on 3 different branches all based on `trunk-dev`,
  each restoring against the exact key an apparently error-free `trunk-dev`
  base-branch save produced 9-14 hours earlier — 0/3 got a hit, exact or
  prefix-fallback. No revert check applies here — this is a report, not a
  fix, so there is nothing to verify went red-then-green; the "after"
  measurement above is the sharding win's, not this report's own.
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

# Cross-run save/restore check for one shared key (the §4 comparison):
grep -n "Restore Key:\|Cache Key:\|Restoring cache\|No cache found\|Saving cache\|Failed to save\|Sent .* (100.0%)" \
  logs/*"no-db, windows"*.txt
```

Cache-usage confirmation: still not run in this session, still no tool access
(checked again today — same gap as the 09-05 report). A repo admin can confirm
via Settings → Actions → Caches, or
`gh api repos/autumn-foundation/autumn-harvest/actions/caches --paginate`.
