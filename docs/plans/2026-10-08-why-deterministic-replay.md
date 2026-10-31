# Plan — Why Harvest keeps deterministic replay (issue #1993)

Status: implementation plan (TDD: red, green, refactor).

## Goal

Several engines sell "no deterministic replay" as a feature. An evaluator who
compares Harvest with a checkpoint-only engine sees only the constraint. A new
page states what replay buys and how the compile-time tooling lowers its cost.

Done when: the page exists and `docs/comparison.md` links it.

## 1. Brainstorming — candidate approaches

1. **A new section in `docs/comparison.md`.** Rejected. That page is already
   long, and it compares six engines on eleven axes. The argument needs its own
   space. The issue also asks for a page.
2. **A new page, `docs/why-deterministic-replay.md`.** Chosen. The comparison
   page links it from the determinism row, the determinism narrative and the
   related list.
3. **An ADR.** Rejected. Replay is not a new decision. The reader is an
   evaluator, not a maintainer, and an ADR does not argue a position to them.
4. **Extend `docs/workflow-determinism-guide.md`.** Rejected. That page tells an
   author how to write safe code. This page tells an evaluator why the model is
   worth its cost. The new page links the guide.
5. **Guard: a Python audit in `docs/audits/`.** Rejected. Each per-page guard
   in this tree is a Rust `*_docs.rs` integration module, and a Rust test can
   read the macro source to check the HVG range.
6. **Guard: `tests/integration/replay_positioning_docs.rs`.** Chosen. It runs
   in the ungated `lint` job, so a docs-only change cannot skip it.

## 2. Reverse brainstorming — how to make this fail

- **R1. The page reads as a sales pitch.** Foreclosed: the guard requires a
  "What replay costs" section and a "When checkpoint-only is the better
  choice" section.
- **R2. The page cites work that has not shipped.** Foreclosed: each `#NNN`
  on the page must appear in `docs/shipped-work.md` or name a fragment in
  `docs/changelog.d/`. An anchor such as `page.md#5-title` is not a citation.
- **R3. The HVG range drifts.** A new HVG012 lands and the page still says
  HVG011. Foreclosed: the guard reads the highest code from
  `guardrail::catalog()` and checks that the macro agrees. Each range on the
  page must match it.
- **R4. A named asset is dropped in a later edit.** Foreclosed: the guard
  requires each asset from the issue: the HVG range, `det_check`,
  `harvest-verify`, replay canaries, drift gates and the park state.
- **R5. Nobody can find the page.** Foreclosed: the guard requires links from
  the determinism row and the Related list of `docs/comparison.md`. The
  determinism guide links it too. `corpus-link-check.py` checks each link
  target and reports an orphan page.
- **R6. The guard never runs on a docs-only change.** Foreclosed: an ungated
  `lint` step runs the module. A guard test fails if that step is removed, gains
  an `if:` or `continue-on-error`, or narrows its filter.
- **R7. The prose drifts away from short STE sentences.** Foreclosed: the guard
  fails on a prose sentence over 25 words.
- **R8. A competitor claim goes stale.** Foreclosed in part: the page dates its
  market claims and cites a source for each. It names the model, not a vendor
  weakness.

## 3. Six thinking hats

- **White (facts).** The issue has one acceptance criterion. The report names
  Sayiir, Absurd, Trigger.dev and Golem. The assets to cite are HVG001–HVG011,
  `det_check`, `harvest-verify`, replay canaries, drift gates and parked
  non-deterministic runs. The issue also names Golem. Golem's own page does not
  say that it avoids replay, so the page leaves Golem out.
- **Red (feelings).** An evaluator fears that replay is a trap that fires at
  2 a.m. The page must meet that fear first, not bury it.
- **Black (risks).** Overclaiming, stale competitor facts, a defensive tone, and
  a copy of the comparison page's determinism narrative.
- **Yellow (benefits).** The constraint becomes an argued trade-off. The page
  also gives one entry point to tooling that six other pages describe.
- **Green (ideas).** A table of questions each model can answer. A cost ledger
  that pairs each cost of replay with the tool that lowers it. An honest "choose
  checkpoint-only when" list.
- **Blue (process).** Red: write the guard and see it fail. Green: write the
  page and the links. Refactor: tighten prose to STE and run the audits. Then a
  multi-angle review.

## 4. Steps

1. **Red.** Add `replay_positioning_docs.rs`, register it in `mod.rs`, add the
   `lint` step. Run it and record the failures.
2. **Green.** Write `docs/why-deterministic-replay.md`. Link it from
   `docs/comparison.md` and the determinism guide. Fix the stale claims in
   the pages it links. Add the changelog fragment.
3. **Refactor.** Tighten the prose and the guard. Run the guard,
   `corpus-link-check.py`, `doc-claim-drift.py`, `workflow-yaml-parse.py` and
   `comment-hygiene.py --base origin/trunk-dev`.

## 5. Review round

Four review agents read the change: facts, guard code, writing, and CI. Codex
also reviewed it. The fixes:

- The page scopes drift detection to a changed command sequence. It no longer
  says that a checkpoint engine "cannot copy" it.
- The history ceiling moved from the remedies to the costs. `harvest-verify` is
  marked as a prototype. `det_check` reaches one call only.
- The replay gate is `ReplayVerifier` (#251), not the `WorkflowReplayer` harness.
- `docs/replay-verify.md` no longer offers `harvest-replay` as a batch gate. Its
  CI snippet splits the batch envelope into one file per run.
- The guard reads the HVG range from the catalog and checks each range. Its
  sentence parser handles code spans, abbreviations, blockquotes and wrapped
  `#` lines. Its CI check rejects `continue-on-error` and a narrowed filter.
