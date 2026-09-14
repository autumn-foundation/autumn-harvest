# 🚦 Semaphore CI health — a stale migration count deterministically red on
# every PR for 5+ hours, plus a duplicate-fix collision on issue #1459

**Status:** deterministic-defect fix, shipped (PR #1532), plus a health report.
Continues the series in `docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md`,
`docs/rnd/2026-09-11-ci-health-semaphore-cancelled-run-census.md`, and
`docs/rnd/2026-09-12-ci-health-semaphore-wall-clock-bound-recurrence.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request`/`push`
triggers against `trunk`/`trunk-dev`. This report's finding lives in the
ungated `lint` job — specifically the `docs/rnd/sqlite-feasibility.md`
guards step, which runs unconditionally (including on docs-only changes) by
design, per that step's own comment in `ci.yml`.

## 🌡️ Symptom

### 1. `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree`: deterministically red since ~05:10 UTC

Sampled the ~24h window since the prior report's cutoff (2026-09-12 09:41
through 2026-09-13 09:44 UTC): 269 `pull_request`-event `ci.yml` runs — 183
cancelled, 60 explicit failure, 26 success. Job-logged the 13 explicit
failures that landed after `#1510` merged to `trunk-dev` (2026-09-13T04:33:45Z)
as a spot check of "does this window's Lint failure rate hold up post-merge":

**8 of 13 (62%) failed on the identical assertion**, across 8 unrelated
branches/commits with nothing else in common:

```
thread 'sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree' panicked at
autumn-harvest/tests/integration/sqlite_feasibility_docs.rs:777:5:
the report should state "**105 migrations**"; a live count finds 105 migration directories
```

Root cause, confirmed directly: `origin/trunk-dev` at `2924c79` has 105
migration directories on disk (`ls autumn-harvest/migrations | wc -l` → 105),
last one `20260913010332_harvest_audit_export_seq_idx_covering`. But
`docs/rnd/sqlite-feasibility.md` still said `**104 migrations**` — stale since
whichever merge added the 105th migration without updating this report. (The
panic message's own wording is a pre-existing red herring — it interpolates
the *live* count into both halves of the sentence instead of quoting the
report's actual stale text, so "should state 105 ... finds 105" reads as
contradictory at a glance. The real defect is that the report said 104, not
105; `grep -rn "104 migrations" docs/ autumn-harvest/` found only that one
line.)

This is a **deterministic doc/code-sync defect**, not suite flakiness — the
same class the 09-11 report already named for `docs/performance.md` and
`migration_hygiene` guard failures ("Deterministic," not a flake candidate).
No rerun protocol or revert-check machinery applies; the fix is verified by
running the guard before and after the one-line edit (below).

Of the other 5/13 explicit failures in the same spot-check sample: 4 were
unrelated deterministic compile/lint breaks on their own branches (a
too-long doc-paragraph clippy lint, a too-many-lines clippy lint, two
feature-gating fixes already being worked by their own PRs per their commit
messages), and 1 was the pre-existing `linuxpart` `integration_e2e` flake this
series has not yet root-caused (noted, not investigated further here — no
budget spent on it this session).

### 2. Cancelled-run sample: mostly legitimate concurrency-group supersession, not hidden failures of item 1

The 09-11 report found 28% of cancelled runs hid a real job failure and
routed forward a full census of this window's 183 cancelled runs as an open
item. Time budget this session went to item 1 and item 3 instead; only a
3-run spot-check was done here (not a census — do not read the following as
a rate). All 3 sampled cancelled runs show the `Lint` job (and others)
transitioning to `cancelled` mid-Clippy-step, **before** reaching the
`sqlite_feasibility_docs` guards step later in the job — consistent with
`ci.yml`'s `cancel-in-progress` concurrency group reacting to a newer push on
an actively-iterating branch, not with a hidden failure of item 1's
assertion. The 68% cancellation rate in this window (vs. 54% in the 09-11
report's sample) is plausibly explained by higher same-branch push velocity
in this window rather than by item 1, but this is a 3-run spot-check, not a
census — **the full cancelled-run audit is still routed forward, not
performed**, same as the 09-11 report left it.

### 3. Issue #1459: two independent, uncoordinated fix PRs collided

While checking whether `worker_completes_ten_child_fan_out_within_wall_clock_bound`
(the subject of the 09-12 report) had recurred post-fix, found that it had
already been fixed **twice**, seemingly by two sessions unaware of each
other:

- `#1510` ("Widen reset_timed_out_workflow_task pool-retry budget (issue
  #1459)") — **merged** to `trunk-dev` 2026-09-12T23:33:45Z. Widened the
  retry schedule to 10 attempts / ~32s.
- `#1517` (same title, same issue) — opened 2026-09-12T23:57:17Z, **24
  minutes after `#1510` merged**, proposing 8 attempts / ~16s against the
  since-moved `trunk-dev`. Its `mergeable_state` is `dirty` — it conflicts
  with the retry-schedule constant `#1510` already changed.

`#1510`'s schedule is a strict superset of `#1517`'s proposal, so there is
nothing left in `#1517` to land as-is. Left a comment on `#1517` pointing this
out and suggesting either closing it or rebasing down to the parts `#1510`
didn't cover (issue #1459's still-open "backstop" direction). Not treating
this as this role's problem to fix — flagging it because leaving two
independent agents to burn review cycles on a collision neither could see is
exactly the kind of cross-session waste a CI-health investigation is
positioned to catch in passing.

## 🔍 Diagnosis

**Item 1:** deterministic — a checked-in report's prose fell out of sync with
a live-counted fact (migration directory count) after a merge added the 105th
migration without updating the report. Test-vs-product verdict: the *guard*
is correct and doing its job; the *document* was wrong. Fixed the document.

**Item 3:** not a CI-suite defect at all — a coordination gap between two
independent fix attempts on the same issue. No suite change follows from it;
flagged to the PR author via comment.

## 🔧 Treatment

- **Shipped:** PR #1532 — `**104 migrations**` → `**105 migrations**` in
  `docs/rnd/sqlite-feasibility.md`. One line, no test change, no `ci.yml`
  change.
- **Comment posted** on PR #1517 explaining the collision with already-merged
  #1510 and proposing next steps for its author.
- **No action taken** on item 2 (cancelled-run census) — explicitly routed
  forward again, unchanged in status from the 09-11 report.

## 📊 Measurement

- **Item 1 — before:** `cargo test -p autumn-harvest --no-default-features
  --features testing --test integration sqlite_feasibility_docs::` against
  `origin/trunk-dev` tip (`2924c79`) → 14 passed, 1 failed
  (`derived_totals_agree_with_the_table_and_the_tree`).
- **Item 1 — after:** same invocation, one-line fix applied → **15 passed, 0
  failed**, `finished in 8.46s`.
- **Item 1 — CI sample:** 8/13 (62%) of explicit-failure runs in the
  post-`#1510`-merge portion of the 24h window shared this exact signature,
  across 8 unrelated commits/branches.
- `python3 docs/audits/comment-hygiene.py --self-test && python3
  docs/audits/comment-hygiene.py --base origin/trunk-dev` on the fix → clean.
- Not a flake, so this role's rerun-protocol/revert-check hard gate does not
  apply to item 1's fix — the assertion is deterministic and was verified
  failing-then-passing directly.
- **Item 3:** verified by reading both PRs' diffs and `worker.rs:28894-28910`
  directly — `#1510`'s merged schedule (10 attempts) supersedes `#1517`'s
  proposed one (8 attempts) attempt-for-attempt through its own length.

## 🔬 Reproduce

```sh
# Item 1 root cause:
ls autumn-harvest/migrations | wc -l   # 105, on origin/trunk-dev tip
grep -n "104 migrations\|105 migrations" docs/rnd/sqlite-feasibility.md
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration sqlite_feasibility_docs::

# Item 1 CI sample: actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# paged back to the 09-12 report's 09:41 UTC cutoff; get_job_logs(run_id=<id>,
# failed_only=true, return_content=true) on every run with conclusion=="failure"
# created after 2026-09-13T04:33:45Z (the #1510 merge timestamp).

# Item 3: pull_request_read on #1510 and #1517; diff their bodies against
# autumn-harvest/src/worker.rs:28894-28910 (RESET_RETRY_BACKOFF_MS or
# equivalent constant) on origin/trunk-dev.
```
