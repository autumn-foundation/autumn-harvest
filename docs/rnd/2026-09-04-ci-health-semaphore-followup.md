# 🚦 Semaphore CI health follow-up

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
up on `docs/rnd/2026-09-03-ci-health-semaphore.md` and the fix it led to
(`ba3abe1`, PR #1336, merged 2026-09-03T14:35 UTC). One finding below needs a
human action this report cannot take (branch protection); the other is routed
the same way its predecessor was (a timing decomposition, not a fix).

**Scope examined:** Actions run/job history for the `CI` workflow via the
GitHub API — the ~100 most recent `push`-event runs on `trunk-dev`, and
check-run detail for PRs #1336, #1337, #1334, #1346, and #1354 (the most
recent PR run against the current `trunk-dev` head at the time of writing,
`b36f023`).

## 🎯 Verdict path (unchanged since the prior report)

Still gated by the `test` matrix (`Test (ubuntu-latest/windows-latest/macos-latest)`,
required by name in branch protection since before this report's scope). The
new `test-db-linux` job (`Test DB (linux, shard N)`, both `needs: [lint,
changes]`) runs on every PR and reports real pass/fail — but whether it's
*part of* the verdict path a merge actually waits on depends on the
branch-protection question this report can't settle (see Diagnosis below):
if those 10 checks aren't in the required-status-checks list, the true gate
is still just the pre-existing `test` matrix, and a red shard is currently
informational only, not gating. No merge queue, no ambient retry either way.
One change since
the prior report: `push`-event runs on `trunk-dev` are landing fast enough
(commit cadence sampled at multiple per hour on 2026-09-03/04) that
`concurrency.cancel-in-progress` cancels nearly every one before it
completes — of the 10 most recent `push` runs at time of writing, all 10 show
`conclusion: cancelled`. This isn't new (the prior report already measured
23/30 cancelled), but it means **trunk-dev's own post-merge verification
essentially never completes right now** — the only runs that reach a real
conclusion are PR-branch `pull_request`-event runs, checked below instead.

## 📊 Measurement — the sharding fix, verified with production data

The prior report routed the ubuntu-latest leg's 54–58%-in-one-step cost to a
findings report; PR #1336 (same day, separate authorization) shipped the
parallelization anyway; that PR's own description says shard-correctness was
"verified locally," not from a real run. It now has one:

| run | event | `Test (ubuntu-latest)` job span | `Test (windows-latest)` job span | **workflow wall time** (`created_at`→`updated_at`, the actual PR-wait number: queue + `changes` + `lint` + the slowest parallel `test`/`test-db-linux` leg) |
|---|---|---:|---:|---:|
| `33712626699` (2026-09-03, pre-fix) | push, trunk-dev | 155.3 min | 103.8 min | **177.0 min** |
| `33787332718` (PR #1337, stale branch — pre-fix workflow) | pull_request | 156.8 min | 101.0 min | 173.7 min |
| `33783544293` (PR #1334, stale branch — pre-fix workflow) | pull_request | 156.7 min | 107.9 min | 172.5 min |
| `33785335533` (PR #1336 itself, post-fix workflow) | pull_request | **65.8 min** | 110.9 min | **123.8 min** |

(An earlier version of this table reported only the `Test` job's own elapsed
time as "time to green" — a Codex review on this PR correctly flagged that
as wrong, since both `test` and `test-db-linux` wait on `lint`+`changes`
first, and runner queuing adds more on top; a per-job span isn't the number
a PR author actually waits on. The **workflow wall time** column above is
pulled from each run's own `created_at`/`updated_at` via the GitHub API,
i.e. queue time + `changes` + `lint` + whichever `test`/`test-db-linux` leg
finishes last — the real end-to-end figure.)

The fix works as diagnosed: `Test (ubuntu-latest)`'s own span drops from
~155–157 min to 65.8 min — a 57.7% cut, matching the 54–58% the removed step
was measured to own. The `test-db-linux` shards themselves (10/10, PR #1336
and again on PR #1354 below) complete in 16–35 min each, well under the old
serial step's 77–84 min. Net effect on the number that actually matters —
true end-to-end workflow wall time — is **~174 min → 123.8 min, a ~29%
reduction** (174.4 min is the 3-sample pre-fix average; the single pre-fix
push sample alone gives 177.0→123.8, 30.1%), clearing this role's ≥15%
impact floor with a real measured before/after on the same critical path.
This is one post-fix sample; the mechanism (removing a step that owned
54–58% of one leg) predicts the direction and rough magnitude reliably, but
a second sample would firm up the exact number.

### New bottleneck: `windows-latest`, unaffected by the fix, now the long pole

Windows was already the second-slowest leg (101–108 min across the three
pre-fix samples above) and the fix didn't touch it — the sharded step is
Linux-Docker-only. It's now the single longest of the parallel `test`/
`test-db-linux` legs (110.9 min job span on PR #1336's own run, the leg that
sets the floor under the 123.8 min end-to-end figure above once `lint`'s own
~13 min is added back in). Step-level breakdown of that run's `Test
(windows-latest)` job (steps summing to 6649s against a 6653s job total, so
this accounts for effectively the whole leg):

- `Compile plugin integration test suites (manifest, all OSes)` — 1700s
  (28.3 min, 25.6% of the leg)
- `Run all-OS manifest suites (no live DB)` — 826s (13.8 min, 12.4%)
- `Compile db-backed tests` — 734s (12.2 min)
- `Run plugin lib tests` — 600s (10.0 min)
- `Compile plugin SQS connector example` — 459s (7.7 min)
- `Run CLI tests` / `Run classic (non-unified) DAG tests` / `Run no-db
  tests` — 300s / 298s / 243s

The top two steps alone are 38% of the leg. **Not diagnosed further here** —
same reasoning as the prior report's treatment of the ubuntu leg: these are
the `manifest, all OSes` compile+run steps (not the Linux-Docker-backed rows
that got sharded), they run identically on all three OSes, and before
recommending anything I'd want the matching ubuntu/macos step timings for the
same run (not yet pulled) plus a check of whether `Swatinem/rust-cache` is
actually hitting on the Windows runner class at a comparable rate to Linux —
a cold or partial cache would show up as exactly this shape and would be a
cache-correctness finding, not a parallelism one. Flagging as the next
candidate for the same treatment (timing decomposition → owner decides
parallelize-or-audit-or-accept), not fixing blind.

## 🔍 Diagnosis — branch protection gap (status unconfirmed)

PR #1336 shipped with this comment in `ci.yml` (still present, unchanged, at
`b36f023`):

> IMPORTANT — not yet enforced: this job is NOT a required status check.
> Branch protection must be updated by a repo admin (Settings → Branches) to
> require `Test DB (linux, shard N)` for every N below, or a red shard will
> not block merging.

That's a **test-vs-verdict-path** finding, not a test bug: I re-checked PR
#1354 (opened 2026-09-04, base `b36f023` — today's `trunk-dev` tip) and its
10 `Test DB (linux, shard N)` checks are present and green, confirming the
job still runs on every current PR. I found no changelog fragment, no commit,
and no further edit to that `ci.yml` comment block acknowledging branch
protection was updated in the ~19 hours since PR #1336 merged
(2026-09-03T14:35 UTC) — `docs/changelog.d/` has no `pr-1336-*` entry, and
`grep -rn "branch protection" .github/workflows/ci.yml` still only turns up
that same comment, unedited.

**That absence of evidence does not confirm the gap is still open.**
Branch-protection settings are changed in GitHub's Settings → Branches UI
(or its API), out of band from commits — an admin could have added the 10
contexts already and nothing in this repository's commit history, check-run
data, or the `ci.yml` comment itself would change to reflect it (the comment
is prose someone would have to remember to go back and edit; it isn't
generated from the live setting). So the evidence above is equally
consistent with "still open" and "fixed silently via the UI." I couldn't
read GitHub branch-protection settings directly to settle it either way: this
session has no `gh` CLI at all (checked — `gh` isn't on `PATH` in this
environment; that's a tooling gap in this session, not this repository's own
policy, which only restricts `gh auth status` and routes issue/PR *operations*
through the connector — a read of repo settings isn't one of those, I simply
don't have the binary to make it with), and the GitHub MCP server tools
available here don't expose a branch-protection-read method either. So
**this report cannot distinguish the two — it is flagging an unconfirmed
status, not a confirmed-still-open gap.** Per this role's charter, changing
required checks is something to ask a human for rather than infer or do
myself; the same applies to confirming the current setting where I lack read
access.

**Why this matters, concretely, if it does turn out to still be open:** the
shard split that just cut ~50 minutes off end-to-end PR wall time would also — as an
unintended side effect of *how* it shipped, not of the split itself — have
turned 10 real correctness checks into checks that report a verdict nobody's
merge decision depends on. A red shard would show red in the PR's checks
list but would not block the merge button, which is exactly the
"untrustworthy green" shape this role's first law is about, except worse:
the checks wouldn't even be green by default, they'd be *decorative* until
someone flips the required-checks switch.

## 🔧 Treatment — routed, not applied

1. **Branch protection**: needs a repo admin to check whether the 10
   `Test DB (linux, shard 0..9)` contexts are already in `trunk-dev`'s
   required-status-checks list and, if not, add them (and ideally verify
   whether PR branches are required to be up to date with `trunk-dev` before
   merge — PRs #1337 and #1334 both merged having run CI against a `ci.yml`
   predating the shard split entirely, on branches opened before PR #1336
   landed; not itself a defect, but it's the same "which pipeline did this
   green actually mean" question the prior report raised about docs-only
   skips). Flagging, not doing — this is exactly the "ask before: changing
   merge requirements, required checks, or branch protection" case, and I
   can't even read the current setting from here to know if it's needed.
2. **`windows-latest` timing**: not routed as a fix candidate yet — needs the
   ubuntu/macos comparison timings and a cache-hit check before it's even
   clear whether this is a parallelism question or a cache-correctness one.
   Next Semaphore pass should pull those before proposing anything.

## 🔬 Reproduce

Check-run timings for a specific PR:

```sh
# Via the GitHub MCP server's pull_request_read(method="get_check_runs"),
# or gh api repos/autumn-foundation/autumn-harvest/commits/<sha>/check-runs
```

Push-run cancellation census (same query shape as the prior report):

```sh
gh api --paginate \
  "repos/autumn-foundation/autumn-harvest/actions/workflows/ci.yml/runs?event=push&branch=trunk-dev&per_page=100" \
  | jq '[.workflow_runs[].conclusion] | group_by(.) | map({(.[0]): length}) | add'
```

Branch protection required-checks list (not run in this session — no tool
access; a repo admin can confirm via Settings → Branches, or `gh api
repos/autumn-foundation/autumn-harvest/branches/trunk-dev/protection` with
admin-scoped auth).
