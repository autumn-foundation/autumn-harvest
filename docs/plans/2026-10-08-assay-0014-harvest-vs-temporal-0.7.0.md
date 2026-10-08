# Plan: rerun assay 0011 against 0.7.0 at every depth (issue #1972)

Issue #1972 asks for four things:

1. A pre-registration, committed before the runs.
2. Results at every depth, in both modes, in `docs/assays/`.
3. 0.7.0 headline figures in `docs/benchmarks.md`.
4. A citation of the result in `docs/comparison.md`, in place of the disclaimer.

This plan is assay ledger #14. Assay #11 stays as it is. Its record is
immutable.

## Brainstorm

Ways to meet each criterion:

- **Trees.** Measure three trees, not one. The claim-path fix (#1971) is in
  PR #2052 and is not merged. Its base is `0aeb887`. Trunk has since moved by
  about 11,000 lines (DR fencing, #1823). A before/after on one base needs
  `0aeb887` and the PR head `513b7aa`. The tree this PR ships on is `9f444b7`.
- **Arms.** Harvest `postgres` (default mode), harvest `redis_pg` (best mode)
  and `temporal_go`. The Temporal arm is assay #11's binary, unchanged.
- **Depths.** 250, 500, 1,000 and 2,000, three repetitions per cell. Assay
  #11's depth diagnostic was one repetition per cell, which review called too
  coarse.
- **Attribution.** Capture the #1815 signals in the harness: claim and persist
  durations, pool wait and pool occupancy.
- **Grading.** A committed script grades the lines from the raw output. The
  grade then cannot depend on a choice made after the numbers exist.
- **Headline.** Run the end-to-end suite on `9f444b7` with one native cluster
  per shard, as for 0.6.0. Publish `results-v0.7.0.md`.
- **Guards.** A docs guard pins every registered cell, the ledger row and the
  citation. `doc-claim-drift.py` pins the old disclaimer as stale.

## Reverse brainstorm: how would this go wrong?

| way to fail | guard |
|:--|:--|
| Edit the pre-registration after a number exists | Commit and push it first. The report quotes its commit. |
| Compare against assay #11's Temporal figure from another host | Rerun Temporal on this box. This host is 2.80 GHz, not 2.10 GHz. |
| Run a build or `git push` during a measurement | Build every binary first. Run no agent, no build and no git command while a sweep runs. |
| Turn on sampler SQL by installing a recorder | The capture recorder returns `is_enabled() = false`. Only per-event timings reach it. |
| Mix the claim fix with 11,000 unrelated lines | Grade the fix only as `513b7aa` against its own base `0aeb887`. |
| Smooth an outlier into a mean | Print every repetition. Report the range beside the mean. |
| Merge the two tables on a rounded ratio | Compute ratios from unrounded means in the grader. |
| Drift over a long sweep | Interleave: each repetition round runs every tree and every arm once. |
| Publish a noisy headline | Publish only if the replay control spreads 10% or less. |
| Name a competitor on `benchmarks.md` | `benchmarks_docs.rs` already forbids it. Link the ledger instead. |
| Claim a general speed result | Keep assay #11's bounding section in force. Say so in the report. |

## Six thinking hats

- **White (facts).** Assay #11: Temporal 43.29 against harvest 5.47 at depth
  2,000. Best mode, 1.5x to 2x behind. #1796, #1797, #1798 and #1815 have
  landed since. #1971 is in review.
- **Red (instinct).** The fix should flatten the default mode. Temporal will
  still lead. A rerun that only confirms a loss is still worth publishing.
- **Black (risks).** One box, one shape and one Temporal version. A different
  CPU from assay #11. The PR head is not trunk. Docker Hub rate limits forced
  the Google mirror of the same image tag.
- **Yellow (value).** Before and after are both on record. The comparison
  page gets a number with its bounds. Operators learn whether the default
  mode now matches the Redis mode.
- **Green (options).** Stretch goals stay out of scope: crash-recovery time,
  a long run that exposes bloat and a tuned Temporal arm. The report lists
  them as re-charters.
- **Blue (process).** Red phase: guards fail. Pre-register and push. Build.
  Measure on an idle box. Green phase: publish. Refactor and review.

## Test plan (red, then green)

| guard | red because |
|:--|:--|
| `assay_rerun_docs::the_preregistration_is_committed_and_cited` | the files do not exist |
| `assay_rerun_docs::the_report_publishes_every_registered_cell` | no report |
| `assay_rerun_docs::the_ledger_lists_the_rerun` | no row 14 |
| `assay_rerun_docs::comparison_cites_the_rerun_instead_of_disclaiming` | the page disclaims |
| `benchmarks_docs` (version `0.7.0`) | no `results-v0.7.0.md` |
| `doc-claim-drift.py` stale pin | the disclaimer is still there |
