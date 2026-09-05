# 🚦 Semaphore CI health — cache correctness audit

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows up on
`docs/rnd/2026-09-03-ci-health-semaphore.md` (ubuntu-latest sharding) and
`docs/rnd/2026-09-04-ci-health-semaphore-followup.md` (sharding verified;
`windows-latest` flagged as the next timing-decomposition candidate, with an
explicit note that a cold/partial cache "would be a cache-correctness finding, not
a parallelism one"). This report pulls that thread and finds something bigger than
a `windows-latest`-specific problem: **the `Swatinem/rust-cache` restore is
missing on every sampled `ubuntu-latest` and `macos-latest` `test` leg, and is
inconsistent even on `windows-latest`.** Every `Test`/`Test DB` leg on every
sampled run appears to be doing a full cold compile, every time.

**Scope examined:** raw Actions log archives (`get_workflow_run_logs_url`,
downloaded and grepped locally — the MCP `get_job_logs` tool's `tail_lines`
window is too short to reach the cache-restore step, which runs near the start
of a multi-hour job log) for two independent `pull_request`-event runs against
the current `trunk-dev` tip (`bc2d5ad`) and its immediate predecessor:

- Run `33948067972` (PR #1372, "Bolt" perf PR, commit `d5f3bbc`, merged)
- Run `33952527436` (PR #1374, "Snag" sqlite fix, commit `b7a4955`, merged)

Both are full-matrix runs (real Rust source changes, so `needs.changes.outputs.code
== 'true'` and every step actually executes, not the docs-only skip path).

## 🎯 Verdict path (unchanged)

Same as the prior report: gated by the `test` matrix
(`Test (ubuntu-latest/windows-latest/macos-latest)`), `test-db-linux`'s
required-check status is still unconfirmed from this session (no
branch-protection-read tool available here, same gap as before — flagging again
rather than re-diagnosing). Not revisited further in this report; the finding
below is orthogonal to which checks are required.

## 📊 Measurement — leg wall time, two independent samples

| leg | run `33948067972` | run `33952527436` |
|---|---:|---:|
| `Test (ubuntu-latest)` | 65.9 min | 62.5 min |
| `Test (macos-latest)` | 65.6 min | 73.8 min |
| `Test (windows-latest)` | 102.1 min | 104.4 min |

`windows-latest` is consistently the long pole by a wide margin (+36–42 min over
the faster of the other two), confirming the prior report's finding on a second,
independent sample. That part is not new. What's new is *why the other two legs
aren't faster than this* — they should be, if the cache were doing its job.

### The `Swatinem/rust-cache` restore step, both runs, all three OSes

Pulled directly from each job's raw log (`... Restoring cache ...` /
`Cache hit for restore-key: ...` / `No cache found.`), not inferred from step
duration:

| leg | run `33948067972` | run `33952527436` |
|---|---|---|
| `Test (ubuntu-latest)` | **No cache found.** (0.11s restore attempt) | **No cache found.** (~0s) |
| `Test (macos-latest)` | **No cache found.** (0.18s) | **No cache found.** (~0s) |
| `Test (windows-latest)` | **Cache hit** for restore-key `v0-rust-test-Windows_NT-x64-8918a2f9` (not the exact key — a prefix fallback), 2145 MB, 84s to restore | **No cache found.** (~0.4s) |
| `Test DB (linux, shard 0)` | **No cache found.** (shared-key cache, run `33948067972` only sampled) | not sampled |

The cache **key** is stable across both runs for a given leg (verified
byte-for-byte, e.g. `Test (windows-latest)`'s `Cache Key:
v0-rust-test-Windows_NT-x64-8918a2f9-cad07847` is identical in both logs — same
Rust toolchain version, same `Cargo.lock`, so this isn't a key-instability bug).
That rules out the most common cause of this shape (a job-id or run-id baked
into the key by mistake) — the key is fine; something upstream of key-matching
is emptying the cache between runs.

**4 of 5 sampled leg-runs found no cache at all; the 1 that did only matched via
restore-key prefix fallback (an older, non-exact-match cache), and even that hit
did not correlate with faster compiles** — the two steps most likely to benefit
from a warm `target/`, `Compile db-backed tests` and `Compile plugin integration
test suites (manifest, all OSes)`, ran within 1–6% of each other across the
hit run and the miss run on `windows-latest` (675s vs. 681s; 1506s vs. 1601s
respectively) — no unrelated variable in the second, and if a same-key hit isn't
producing a measurably faster build, the cache isn't earning the minutes it's
supposed to. **Noted as an observation, not asserted as a separate confirmed
finding** — two samples isn't enough to rule out coincidence (e.g. a source
change between the two commits forcing a rebuild regardless of cache state);
a controlled same-commit rerun pair would be needed to confirm it, and this
report doesn't have CI-trigger access to produce one.

### Mechanism (best-supported hypothesis — not independently confirmed)

This workflow runs **19 separate jobs that each call `Swatinem/rust-cache@v2`
with `save-if: true`** on every `push`/`pull_request` event with code changes:
`lint` (1), `test` (3 OSes), `test-db-linux` (10 shards, one shared cache key),
`quickstart` (1), `scaffold-smoke` (1), `msrv` (1), `harvest-verify` (2). Each
saves a multi-GB `target/` — the one size this report directly observed is
`windows-latest`'s restored cache at 2145 MB, and `ubuntu-latest`'s own
end-of-job save transferred 2,762,635,721 bytes (2.57 GiB) compressed. GitHub
Actions enforces a **fixed 10 GB cache cap per repository**, evicting the
least-recently-used entries once exceeded. Even a conservative per-job
estimate (a few of these builds are much smaller single-crate compiles) puts
one run's total save volume at noticeably more than 10 GB — well before
accounting for the fact that this repository is landing multiple PR/push runs
per hour (15 sampled `pull_request` runs on `ci.yml` between 05:26 and 09:39
UTC on 2026-09-05 alone, most superseding each other via
`concurrency.cancel-in-progress` but still each attempting saves for the
commits that do complete). Multiple concurrent PRs each saving 19 caches per
completed run, against a shared 10 GB repository-wide budget, is a plausible
and sufficient explanation for near-total eviction between a leg's own
consecutive runs.

**This is a hypothesis, not a confirmed number.** Confirming it needs the
Actions cache-usage API (`GET /repos/{owner}/{repo}/actions/cache/usage`) or
the Settings → Actions → Caches UI — this session has neither `gh` nor an MCP
tool exposing it (the same access gap the prior report hit for
branch-protection settings). Flagging for whoever has that access, per this
role's charter of reporting rather than guessing past a tooling gap.

## 🔍 Diagnosis

**Category: cache correctness / capacity — not a parallelism question, and not
(primarily) the `windows-latest`-specific mechanism the prior report
speculated about.** The prior report's hypothesis was framed as "is
`windows-latest` specifically cache-cold" — the answer turns out to be
"functionally, every leg is cache-cold, all the time, on every OS," which is a
strictly bigger finding: if true, **every `Test`/`Test DB` leg on every commit
is paying a full cold-compile cost**, not just the flagged one. `windows-latest`
being the slowest leg is still real and still matches the step-level
decomposition in the prior report (the `manifest, all OSes` compile+run steps
own ~35% of that leg specifically, worse there than on the other two OSes) —
but a correct, warm cache would very plausibly narrow the gap between all
three legs, not just `windows-latest`'s.

This is squarely a **product-vs-test** question answered on the product side:
nothing about the *test suite* is nondeterministic here — every leg's tests
pass, every time. The nondeterminism is in the **CI configuration's own cache
hit rate**, which is this role's territory (a "test" in the CI-health sense),
not a workflow bug and not a flaky test to quarantine.

## 🔧 Treatment — routed, not applied

Not opening a fix PR: per the hard gate, a cache-correctness fix needs a
**confirmed** cache-usage number and a chosen remedy, and both are cost/policy
decisions this role is chartered to route rather than take unilaterally
("Ask before: New CI spend: runner classes, parallelism, paid minutes, **caching
services**" — changing what gets cached, how many legs cache independently, or
paying for a larger cache tier are all instances of this). Candidate remedies,
for whoever has cache-usage visibility to pick between:

1. **Confirm the hypothesis first** — pull actual cache usage from Settings →
   Actions → Caches (or the cache-usage API) to see current total bytes and
   eviction frequency before choosing a remedy.
2. If confirmed: reduce the number of independently-cached legs (e.g. do the 10
   `test-db-linux` shards need `cache-bin: true` each, or would sharing more
   aggressively — or restricting the shared cache to fewer, larger artifacts —
   free headroom?), or narrow `cache-targets`/`cache-all-crates` scope so each
   save is smaller, or accept the cost and pay for GitHub's larger cache tier.
3. Independently of the capacity question: the observation that a genuine
   cache hit didn't measurably speed up the two heaviest compile steps on
   `windows-latest` is worth a dedicated controlled test (same commit, forced
   cache hit vs. forced cache miss, N≥2 each) before spending any engineering
   time on capacity — if the cache isn't earning its keep even when warm,
   fixing capacity alone won't move the timing numbers this report is really
   trying to cut.

## 🔬 Reproduce

Full run logs (needed because `get_job_logs`'s tail window doesn't reach the
cache-restore step in a job this long):

```sh
# Via the GitHub MCP server's actions_get(method="get_workflow_run_logs_url"),
# or: gh api repos/autumn-foundation/autumn-harvest/actions/runs/<run_id>/logs \
#       -o logs.zip
unzip -o logs.zip -d logs
grep -n "Restoring cache\|Cache hit\|No cache found\|Cache Key:" \
  "logs/Test (ubuntu-latest).txt" "logs/Test (macos-latest).txt" \
  "logs/Test (windows-latest).txt"
```

Cache-usage confirmation (not run in this session — no tool access; a repo
admin can confirm via Settings → Actions → Caches, or
`gh api repos/autumn-foundation/autumn-harvest/actions/cache/usage`):

```sh
gh api repos/autumn-foundation/autumn-harvest/actions/caches --paginate \
  | jq '[.actions_caches[] | {key, size_in_bytes, last_accessed_at}]'
```
