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

**Second Codex round (PR #1380): four more P2 findings, all verified real.**

- *`/*!` evaded the gate.* The lexer recognised the marker but advanced only
  past `/*`, leaving `!` on the body so the anchored rules missed
  `/*! TODO */` and `/*! let stale = 1; */`. Now advances past the whole
  marker, with a guard so `/**/` (an empty comment, not a doc marker) does not
  eat its own terminator and swallow the rest of the file.
- *CH001 false-positived on prose.* The `fn` alternative accepted anything
  after the opening paren, so `// fn foo() is called by the wrapper.` was
  reported as commented-out code — a false positive that fails CI on ordinary
  prose, contradicting the terminator-bearing heuristic every other
  alternative follows. It now requires a real terminator (`{`, `;`, or an
  open paren at end of line for a wrapped signature).
- *A count-neutral swap passed.* Removing one legacy violation and adding a
  different one in the same file left the count unchanged and the gate green.
  Demonstrated live: a new `// A newly introduced defect: this isn't
  compliant.` in `event.rs` passed because CH006 stayed at 1.
- *Renames failed.* A baselined file moved to a new path had no entry, so all
  its legacy findings read as new — 24 spurious regressions for a pure rename
  of `history_export.rs`, comments untouched.

The last two share a root cause, so both are fixed by one change rather than
two patches: **the stored baseline is gone.** The harness now reads the merge
base out of git (`git show <merge-base>:<path>`) and re-scans each changed
file as it stood there, matching findings by fingerprint (rule + normalized
text) instead of counting them. That kills the whole class at once — it cannot
go stale when the base moves, there is no regeneration ritual to launder a
violation through, a renamed file is compared against its own previous path,
and 532 KB of generated fingerprints stay out of the tree. Identities rather
than counts also mean a swap is caught: the total never moves, but the new
fingerprint is not in the allowed set.

Verified across eight cases in an isolated worktree: count-neutral swap fails;
pure rename passes (and reports "1 renamed"); a new Tier B finding in a
touched file fails; Tier A fails anywhere; `/*! TODO */` fails; `// fn foo()
is called by the wrapper.` passes; a newly added file is allowed nothing, so
its findings fail; base drift on an untouched file passes; and both
degradation paths (no `--base`, unknown ref) report at exit 0.

**Third Codex round (PR #1380): three more P2 findings.** One
("regenerating the baseline launders a violation") was already answered by
dropping the stored baseline. Two were real and outstanding:

- *Push runs scoped from the wrong boundary.* The push branch diffed
  `HEAD~1`, so on a multi-commit push to `trunk-dev` a violation introduced by
  an earlier commit slipped through in any file the final commit did not also
  touch. Now uses `github.event.before`, the boundary the workflow's own
  `changes` job already uses, with a fallback to report-only on a branch's
  first push (no before-SHA exists). Proven: with the violation in commit 1
  and an unrelated commit 2, `--base HEAD~1` exits 0 while
  `--base $BEFORE` exits 1 and names the file.
- *The prescribed local check gated almost nothing.* `CLAUDE.md` told
  contributors to run `python3 docs/audits/comment-hygiene.py`, which without
  `--base` leaves Tier B report-only — so the documented pre-push command
  checked Tier A and little else. It now prescribes
  `--base origin/trunk-dev` and says plainly why the flag matters.

**Fourth Codex round (PR #1380): two more P2 findings, both real.** Both came
from state that was scoped to the file when it should have been scoped to the
comment block.

- *An unclosed fence disabled the whole file.* `fence` was one boolean across
  every comment in a file, so a `/// ```rust` block that never closed left it
  set for everything after it. Reproduced: with an unclosed fence above them,
  `// TODO: issue required` and `// let stale = compute();` produced **no
  findings at all** — every Tier A rule silently off for the rest of the file.
  That is the exact failure mode this harness is supposed to avoid: a gate
  that stops gating without failing.
- *Adjacent trailing comments merged into one sentence.* Prose units were
  joined on line adjacency alone, so two short trailing notes on consecutive
  lines became one long unit and could trip CH007 as a sentence neither author
  wrote.

Both are fixed by one concept: `comment_runs()`. A run is what a reader sees
as one comment — consecutive lines, same kind — and a trailing comment is
always its own run. Fence state resets per run, and prose units never span
one. CH004 uses the same grouping instead of re-deriving it.

Three rule-level fixtures now pin this alongside the 14 lexer ones, since
neither behaviour is expressible as a lexer test: an unclosed fence does not
leak past its block, fenced example code stays exempt, and adjacent trailing
comments are not merged. Tier A stayed at zero; CH007 fell 17,913 → 17,903 as
wrongly-merged trailing units split into the separate short notes they always
were.

**Fifth Codex round (PR #1380): two more P2 findings, both real.**

- *The wrapped-signature branch accepted prose.* The earlier CH001 fix added
  an alternative for `fn foo(` at end of line, but wrote it as `[^)]*` — any
  line with no closing paren. So `// fn resolve_call(the caller name appears
  in diagnostics` was reported as commented-out code. A narrower fix for the
  original false positive that reintroduced a smaller one. It now accepts only
  an open paren at end of line, or parameters ending in a trailing comma.
- *`////` was misread as `///`.* Four or more slashes is an ordinary comment
  in Rust, not a doc comment, but the marker match took the first three and
  left a stray `/` on the body — so `//// let stale = compute();` extracted as
  `/ let stale = compute();` and CH001 missed it. The marker is now the whole
  leading slash run, so `////` and `/////` both extract cleanly.

Three more fixtures pin these (20 in total): `////` is an ordinary comment,
prose after an open paren is not a signature, and a wrapped commented-out
signature is still caught on its opening line.

**Proactive CH001 hardening.** Two review rounds had found CH001 false-
positiving on prose that opens with a Rust keyword, so rather than wait for a
third the boundary was swept adversarially: 210 generated prose lines (every
keyword the rule keys on, crossed with the sentence shapes this corpus
actually writes) against 23 genuine commented-out shapes.

That found six more false positives before review did — `use the caller
decide, since the row may be gone;` and `use T: Send is required here;` (the
`use` alternative allowed bare spaces, so any prose sentence starting with
"use" and ending in `;` matched), plus `let x = the value the operator
supplied;` and the `type` equivalent (a right-hand side of bare words read as
an initializer). It also found one missed true positive: `});` is two closers,
and the rule allowed only one.

Fixed by requiring a real use-path shape, rejecting a `let`/`type` right-hand
side of three or more bare words, and allowing a run of closers. The sweep now
reports 0 false positives and 0 misses, and 26 of those shapes are pinned in
`--self-test` so the next narrowing of CH001 cannot quietly re-widen it.
