## Tooling — Comment-hygiene CI harness and a corpus audit

**Tooling + comment-only source changes** (implemented). Adds
`docs/audits/comment-hygiene.py`, wired into CI's ungated `lint` job
alongside the other Folio corpus harnesses, and fixes every Tier A defect it
found across the 785 `*.rs` files (822k lines, 175k comment lines).

**The design decision that shaped it.** A blanket "comments must be short"
gate was rejected. Measured over the corpus, 17,899 comment sentences exceed
ASD-STE100's 25-word ceiling, and the longest blocks are the ones carrying
the engine's correctness arguments — the ABBA lock-ordering proof for
`materialize_due_child_timeout_deadlines`, the `cohort` partition-key
argument in `partition.rs`, the codec-rotation scope guarantee `CLAUDE.md`
cites as the proof that `harvest_events` exception #3 is safe. A length cap
over a comment *block* would reward deleting exactly those. So the harness
measures per **sentence** and never caps block length: a thorough rationale
passes once it is written as several sentences.

**Two tiers.**

- **Tier A — absolute, at zero, a new one fails the build.** `CH001`
  commented-out code, `CH002` a TODO/FIXME/XXX/HACK with no `#<issue>` or
  URL, `CH003` a narrative aside, `CH004` a blank `//` line at a block edge.
- **Tier B — ratcheted** against `docs/audits/comment-hygiene-baseline.json`
  (per-file, per-rule counts; may fall freely, never rise). `CH005`
  review-round archaeology (1,370 — "Codex round 8" is process trivia a
  future reader cannot look up; the issue number is the durable handle),
  `CH006` contractions (331), `CH007` sentences over 25 words (17,899).

**What the audit fixed** (24 Tier A sites, all comment-only):

- A 52-line commented-out "API GAP" scaffold in
  `autumn-harvest-verify/tests/resolve_fixtures.rs` proposing an API that has
  since landed — every item (`Resolution`, `Substitution`, `resolve_call`,
  `resolve_terminator`, `call_substitution`, `substituted_callees`,
  `body_paths`) is present in `src/resolve/`, and the tests below the block
  are already active against it.
- A self-contradicting aside in `autumn-harvest-cli/src/lib.rs` arguing to
  represent DELETE as a POST, directly above code already using
  `ApiMethod::Delete`.
- A wrong event count in
  `autumn-harvest-plugin/tests/workflow_history_pagination_integration.rs`:
  three comment lines claimed "16 total" and "append 2 more" where the test
  asserts 15 (`1` WorkflowStarted + `14` timer events).
- A doc comment in `throttle_tests.rs` that stated a bypass rule and then
  reversed itself mid-sentence ("... actually it DOES bypass").
- 13 further first-person/deliberation asides, and 6 blank comment lines at
  block edges.

**Invariants.** No behaviour change: every edit is a comment, and no
`WorkflowEvent` variant, migration, or SQL is touched. `cargo check` passes
for `autumn-harvest` (`--all-features --tests`), `autumn-harvest-cli`,
`autumn-harvest-plugin` and `autumn-harvest-verify` (`--all-targets`);
`cargo fmt --all -- --check` is clean.

**Test evidence.** The gate was verified in both directions rather than
assumed: a seeded contraction plus an over-long sentence produces exit 1 and
names both rules and the file; a seeded `// let stale = ...` line trips
`CH001` for exit 1; the unmodified tree exits 0. Rule tuning was measured
against the corpus, not guessed — fenced code blocks, markdown tables and
headings are excluded (a naive scan mistook ~670 doc-example lines for
commented-out code), `CH003` and `CH007` are evaluated per sentence rather
than per wrapped line, and non-contiguous comment blocks separated by code
are no longer merged into one prose unit.

**Codex review follow-up (PR #1380).** Two P2 findings, both verified real
against the corpus and both fixed by replacing the line-regex comment matcher
with a real Rust lexer (`extract_comments`):

- *Exclude string contents.* A `//` line inside a raw string was being read as
  a comment. Confirmed on 10 live sites — `det_check_tests.rs` fixtures embed
  `// harvest-suppress: DET001 ...`, and `chaos_catalogue_drift.rs:200` carries
  a literal commented-out call as test data. None trip a rule today, but Tier A
  gates at zero, so one new fixture of that shape would have blocked an
  innocent PR.
- *Inspect trailing and block comments.* The old matcher was anchored to the
  start of a line, so `let n = 1; // TODO: fix` and `/* TODO: fix */` were
  invisible — every gated defect class could be introduced through either form
  with CI green. 897 comment pieces were out of scope; they are now covered.

The lexer handles nested block comments, raw strings of any hash count,
byte/C-string prefixes, escapes, and the lifetime-vs-char-literal ambiguity.
Writing it surfaced a third defect of my own: a backslash-newline continuation
inside a string (used throughout the long SQL and `#[error(...)]` strings) was
consuming the newline without counting it, drifting every subsequent line
number — `error.rs` was off by 11 by line 700. Fixed and pinned; all 175,892
comment pieces now report a line number that really contains them.

`--self-test` pins 14 lexer fixtures and CI runs it before the scan, so a
silent regression in comment-finding fails loudly instead of quietly ceasing
to gate. Tier A remained at zero under the widened coverage. The Tier B
baseline was regenerated for the rule-definition change (+2 CH006, +10 CH007,
all in newly visible trailing and block comments). The gate was re-verified
through the new paths: CH001/CH002 via a trailing comment, CH002/CH003 via a
block comment, each exit 1, while a raw-string fixture correctly exits 0.

**Tier B is scoped to changed files (PR #1380 CI).** The harness's own first
CI run failed — correctly, and on a design flaw rather than a bug. While the
PR was open, `trunk-dev` merged #1377, which added 4 long sentences to
`cross_region_dr_tests.rs`. CI evaluates the merge of the branch onto the base,
so those sentences appeared in the scan while the locally-generated baseline
knew nothing of them, and the gate failed on a file the PR never touched.

That is inherent to a whole-corpus count: it is a shared mutable number, so
one merge adding a long comment anywhere turns every open PR red, and the
predictable response is to regenerate the baseline — which defeats the ratchet
entirely. Fixed by scoping Tier B to the files a change actually touches, via
`--base <ref>` (merge-base + `git diff --name-only`), which CI passes as the
PR's target branch. Tier A is never scoped and gates everywhere.

Failing safe matters here as much as failing correctly. When the diff cannot
be computed — no `--base`, an unknown ref, a shallow clone with no reachable
merge base — Tier B reports and never fails, because gating the whole corpus
at exactly the moment the tool cannot tell what changed is the worst available
option. The `lint` checkout takes `fetch-depth: 0` so the merge base is
actually reachable; without it the step would silently degrade to report-only
and quietly stop gating.

Verified across all five paths: base drift on an untouched file passes; a
CH006/CH007 regression in a file the branch does touch exits 1 naming both
rules; a Tier A defect exits 1 regardless of scope; an unknown ref and a
missing `--base` both degrade to report-only at exit 0.
