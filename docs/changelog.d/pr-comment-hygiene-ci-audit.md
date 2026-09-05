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

**Sixth Codex round (PR #1380): four more P2 findings, all real.**

- *Wrapped parameters excluded valid Rust.* `// fn f(x: impl Send + Sync,` and
  `// fn f(x: [u8; 4],` evaded CH001 because the parameter character class had
  no `+`, `;` or `=`. Widened, and gated on a `:` or a `self` receiver so that
  prose ending in a comma still cannot match.
- *A change of comment marker did not end a run.* `/// ```rust` immediately
  followed by `// TODO: issue required` stayed one run, so the doc block's
  unclosed fence suppressed the ordinary comment below it — the same class as
  the previous round's fence leak, one level down. The marker is now part of
  the run boundary.
- *Nested block comment bodies were buried.* `/* outer /* let stale =
  compute(); */ */` handed the rules a single string starting "outer", so the
  nested code was never anchored. A nested opener or closer now ends the
  segment, and the inner body starts its own piece.
- *Any fence delimiter closed any fence.* A `~~~` line inside a ` ``` ` block
  is literal content under CommonMark, but it toggled the fence off — so the
  example's own sample text was then read as real comments and reported. The
  opening delimiter is now tracked and only its match closes.

The marker-boundary fix unmasked **three genuine CH004 defects** that run
merging had been hiding: a `///` doc block closing on a blank `///` before a
`//` block (`runner.rs`, `scheduler.rs`) and one opening on a blank `///`
(`worker.rs`). All three removed, so Tier A is back at zero.

The adversarial sweep was extended to 240 prose lines and still reports 0
false positives, and the self-test now carries 24 lexer/rule fixtures plus 30
code-vs-prose shapes.

**Seventh Codex round (PR #1380): three more P2 findings, all real, all in
fence and nested-comment handling.**

- *A nested inner-doc marker kept its `!`.* The nested-body fix advanced two
  characters at every nested opener, so `/* outer /*! TODO */ */` extracted as
  `! TODO`. The same whole-marker handling the top level already had now
  applies to nested openers.
- *Fence openers accepted any indentation.* CommonMark allows at most three
  spaces; four is an indented code line. `///     ```rust` therefore opened a
  fence that never closed and suppressed the rest of the run.
- *Any closer of the same character closed a fence.* A closer must be at least
  as long as its opener, so a ` ``` ` line inside a ` ````rust ` example is
  content — it was ending the fence early and the example's own sample text
  was then read as real comments.

Fence state is now a (character, length) pair applied through one
`fence_transition()` helper shared by `comment_lines()` and `prose_units()`,
rather than a boolean duplicated across both. Five fixtures added.

**Eighth Codex round (PR #1380): two more P2 findings.**

- *A multiline trailing block comment was split across runs.* When
  `/* ... */` opens after code and continues onto later lines, the
  `run[-1].trailing` test separated its first physical line from the rest of
  the same comment, resetting fence state mid-block. Each block comment now
  carries a group id, and one block is one run however many lines it spans and
  wherever it starts.
- *CH001 missed commented-out statements* — `// cleanup();`,
  `// client.send(value).await?;`, `// return Err(error);`. This was a
  documented limitation, but the documentation was written for the
  *unterminated* form (`// foo(bar)`); these end in `;` and are unambiguous.

The statement rule was measured before being added rather than reasoned
about: applied to all 176k corpus comments it produced exactly **one** hit,
`examples/progress_query.rs:76`, and that was genuine — an illustrative
`// ctx.execute_activity_raw("process_batch_chunk", ...).await?;` sketch
(with `...`, so not even valid Rust). Reworded as prose with an inline code
span, which is what it always was.

The first draft of the rule then failed the project's own adversarial sweep:
`return|break|continue` followed by anything up to a `;` matched 18 prose
lines such as `break this module owns the sweep;`. Narrowed to a single-token
operand (`return Err(error);` yes, `return the caller decide, since ...;` no).
The sweep is now 399 prose lines with 0 false positives.

**Ninth Codex round (PR #1380): two more P2 findings, both oversights of mine.**

- *`can't` was never matched.* CH006's stem list built `can` + `n't` =
  "cann't". The most common English contraction went unreported in **93
  corpus comments**. Fixed to the `ca` stem; the corpus count moves 333 → 426.
- *Distinct block comments merged into one run.* The previous round's `group`
  id was only used to *prevent* a split, never to cause one, so
  `/* ```rust */` followed by `/* TODO: issue required */` stayed one run and
  the first comment's unclosed fence suppressed the second.

Worth recording what the `can't` fix demonstrated about the baseline design:
correcting a rule mid-review widened it by 93 findings across the corpus and
produced **zero** Tier B regressions, because the merge base is re-scanned
with the same corrected code. A stored baseline would have reported all 93 as
newly introduced and blocked the fix that found them.

**Tenth Codex round (PR #1380): two P2 findings, plus the structural fix that
should end this class.**

- *More commented-out statements missed* — `// value = compute();`,
  `// if ready {`, `// anyhow::bail!("oops");`. Three new alternatives (any
  macro statement, block-opening control flow, single-token assignment), each
  measured at **0 corpus hits** before being added.
- *A backtick in a backtick fence's info string.* CommonMark forbids it, so
  ` ```foo`bar ` is not a fence — but it opened one that never closed and
  suppressed the rest of the run.

**The adversarial prose sweep is now part of `--self-test`, so CI runs it.**
Hand-editing CH001 produced a false positive in three separate review rounds;
generating the keyword × sentence-shape cross-product catches them first. It
did so again here: the first draft of the control-flow branch matched `if the
queue is paused, the worker parks {`, and tightening that exposed the same
latent flaw in the pre-existing `impl` branch. Both were fixed before pushing.
644 generated prose lines, 0 false positives, checked on every CI run.

CH001's contract is now written down in KNOWN LIMITATIONS: **deliberately
high-precision and incomplete.** A false positive fails CI on ordinary
English, which is worse than missing one commented-out line — so recall is
added by new narrowly-anchored alternatives measured against the corpus and
the sweep, never by relaxing an existing anchor, which has produced a false
positive every time it has been tried.

**Eleventh Codex round (PR #1380): two more P2 findings, both real.**

- *Modal perfect contractions were missed.* CH006's `'ve` alternation listed
  only pronouns, so `should've`, `could've`, `would've`, `must've` and
  `might've` went unreported. No corpus instances, so this one is purely
  preventive.
- *A nested comment's fence leaked into the enclosing one.*
  `/* outer /* ```rust */ TODO: issue required */` produced no finding:
  `block_group` incremented only at the outer `/*`, so every nesting depth
  shared one group and the inner fence suppressed the outer text after the
  inner comment had already closed. Each nesting level now gets its own group,
  on both entry and exit.

Checked against the four neighbouring cases that pull against this, since
nesting and run-grouping fixes have repeatedly broken each other: nested
bodies are still inspected, a nested `/*!` is still stripped, one multiline
block is still one run, and two distinct blocks are still two runs.

**Twelfth Codex round (PR #1380): two more P2 findings, both real.**

- *A non-Rust → Rust rename inherited an allowance it never earned.* The
  rename lookup did not check the OLD path's extension, so renaming a fixture
  from `.txt` to `.rs` scanned the text file at the merge base and granted its
  comments as legacy debt — smuggling contractions and long sentences into the
  audited corpus with the gate green. A rename now inherits only when the old
  path was also `.rs`; renamed *into* the corpus means no allowance, so every
  finding belongs to the change.
- *Interrogative contractions were missed.* `how's`, `where's`, `when's` and
  `why's` were absent from the apostrophe-s stems (`who's` and `what's` were
  already there, which is what made the gap easy to miss).

The rename fix was verified against a real git rename in both directions,
because the two cases are one line apart and pull opposite ways: `.txt` → `.rs`
now exits 1 with both findings attributed to the change (`0 at the merge
base`), while a pure `.rs` → `.rs` rename still exits 0 with its allowance
intact.

**CH006 enumerated, before a thirteenth round found more.** CH006 produced a
finding in three consecutive review rounds — `can't`, the modal perfects, the
interrogatives — each because the rule was spot-checked rather than
enumerated. Applying the lesson from the CH001 sweep, its coverage is now
generated rather than sampled: 61 English contractions it must match and 10
possessive or abbreviation forms it must not, checked by `--self-test` on
every CI run.

That audit found one further genuine miss (`daren't`) and, more usefully,
settled a question it would otherwise have hit later. The noun + `'s` forms
(`one's`, `someone's`, `everything's`) are ambiguous: "someone's waiting" is a
contraction, "someone's row" is a possessive, which STE permits. Measured over
the corpus, **all 21 occurrences are possessives** — "the previous one's
outcome", "the worst one's" — so matching those stems would report 21 false
positives against correct prose. They are now excluded deliberately, with that
measurement written down as the reason, rather than left to look like an
oversight.

**Thirteenth Codex round (PR #1380): four P2 findings.** Two are gaps, one is
a false positive I introduced, and one is a fix from round ten I only did half
of.

- *Compound assignment missed.* `count += 1;`, `retries -= 1;`,
  `flags |= READY;`, `bits <<= 2;` — the assignment branch accepted only a
  bare `=`.
- *`world's` was reported as a contraction.* `world` was in the pronoun stem
  list, so an ordinary possessive tripped CH006 — directly contradicting the
  rationale written two lines above it about possessives being permitted. A
  false positive of my own making; removed.
- *An invalid fence opener was still exempted.* Round ten taught
  `fence_transition()` to reject ` ```foo`bar `, but `comment_lines()` still
  yielded the line as fenced, so the TODO embedded in it was skipped anyway.
  Rejecting an opener now means the line is ordinary text and gets scanned —
  otherwise the rejection hides the very defect it exists to expose.
- *Typographic apostrophes bypassed CH006 entirely.* `can’t`, `isn’t`, `we’re`
  matched nothing, because every branch required an ASCII `'`. Editors
  substitute these automatically, so this was a bypass anyone could trip
  without meaning to.

The last one lands one commit after the CH006 enumeration was added, and is
the sharper lesson: enumerating the contraction *list* while leaving the
apostrophe *character* assumed still left a whole class open. The inventory
now carries both apostrophe forms.

**Applying the pattern instead of just naming it.** Thirteen review rounds
produced a consistent shape: a rule gets checked on the axis its author was
thinking about and stays blind to the orthogonal one. The prose sweep closed
CH001's *wording* axis, the inventory closed CH006's *word-list* axis, and
neither touched *encoding* — which is exactly how `can’t` walked through a
rule one commit after it was declared enumerated.

So the other rules were audited on those same axes rather than waiting for
review to find them. Two live bugs, both fixed here:

- **CH002 was case-sensitive.** `// todo: fix this` and `// fixme: fix`
  produced no finding at all. Lowercase markers are ordinary in real code, so
  this was a trivial bypass of a Tier A gate. Measured at 0 new corpus hits
  before the change.
- **CH003 matched only the ASCII apostrophe.** `We’ll group stats by queue
  name` slipped through while `We'll` was caught — the identical defect to
  CH006's, in a rule nobody had connected to it.

`_APOS` now lives above the Tier A patterns and is shared by both rules that
need it, and the self-test pins the marker-case and apostrophe axes directly
rather than only pinning content.

**Fourteenth Codex round (PR #1380): two P2 findings.**

- *Macros take any delimiter.* The macro-statement branch hard-coded `(...)`,
  so `// vec![1, 2];` and `// my_macro!{ a: 1 };` were missed. Now matches
  bracket and brace forms too, while `see vec![1, 2] for the shape;` stays
  clean because the anchor still requires the macro to open the line.
- *A Tier B failure did not say what to fix.* This one is a usability defect
  rather than a correctness one, and it mattered more than its severity
  suggests. The output named the rule, the file and the totals —
  `worker.rs: 1 new finding(s), 30 total vs 29 at the merge base` — and never
  the line. In a file carrying 30 legacy findings a contributor had to bisect
  by hand to discover which comment they had added. A gate that fails without
  telling you what to fix is one people learn to route around.

Findings are now indexed by fingerprint, so a regression prints its location:

```
CH006 autumn-harvest/src/worker.rs: 1 new finding(s), 30 total vs 29 at the merge base
    autumn-harvest/src/worker.rs:36855: This one isn't compliant.
```

**Fifteenth Codex round (PR #1380): fence indentation is container-relative.**

CommonMark's "at most three spaces before a fence" is measured against the
enclosing block container, not the line. Inside a list item the content starts
past the marker, so an opener indented four spaces there is a perfectly valid
fence — and the flat `^ {0,3}` from round seven rejected it, scanning the
example's own sample text and reporting CH002. A false positive on a Tier A
gate, blocking ordinary Rustdoc.

This one is in direct tension with round seven's fix, which is why the flat
limit looked right at the time: reject deep indentation and you get this false
positive; accept it and a four-space line with no container opens a fence that
never closes and suppresses the rest of the run. Neither is correct without
tracking the container, so that is now tracked — a list marker opens a
container at its content indent, a dedent below it closes one, and the
three-space allowance is applied relative to whichever is open.

Both directions verified together, along with the seven other fence
behaviours accumulated over rounds six to fourteen, since each has broken a
neighbour at least once:

```
/// - Example: + 4-space ```rust        -> exempt   (this fix)
///     ```rust, no list                -> CH002    (round 7 preserved)
///    ```rust  (3 spaces)              -> exempt
```

### Round sixteen — the container fix, applied to closers and to list-marker lines

Review caught two false positives created by round fifteen, both verified to
reproduce before being fixed.

The container allowance was applied to openers only. A delimiter reached while
a fence was already open was accepted at any indentation, so an over-indented
` ``` ` inside a fence closed it early and the example's own sample text was
read as real comments:

```
/// ```rust
///     ```                             -> closed the fence, so
/// TODO: fixture placeholder           -> CH002 on sample text
/// ```
```

Separately, the fence pattern still matched against the raw line, so a fence
opening on the same line as its list marker was never seen at all:

```
/// - ```rust
///   TODO: fixture placeholder         -> CH002 on sample text
///   ```
```

`fence_indent_ok` is replaced by `fence_delimiter`, which skips one leading
list marker before matching, measures the delimiter's indent from the marker's
end, and applies the three-space allowance to closers as well as openers. An
over-indented delimiter is now content: fenced content while a fence is open,
an indented code line while one is not.

All ten fence behaviours re-verified together, the eight from rounds six to
fifteen plus these two.

### Round seventeen — block quotes, ordered-list markers, duplicate locations

Three review findings, all verified to reproduce first.

**A fence inside a block quote was not a fence.** Rustdoc writes quoted
examples, and the fence pattern ran against a body still carrying its `>`:

```
/// > ~~~rust
/// > TODO: fixture placeholder         -> CH002 on sample text
/// > ~~~
```

A block quote is a container like a list item, so it is stripped before any
container or fence judgement. Its marker must be followed by a space, another
marker, or the end of the line — CommonMark would read `>=foo` as a quote of
`=foo`, but in a Rust comment a line wrapping onto a leading `>=` is an
operator, and stripping that `>` rewrites the text every rule then judges. The
corpus contains exactly that in `scheduler.rs` and `start_idempotency.rs`.

**Prose kept a second list pattern of its own**, and it had drifted: it knew
`1.` but not `1)`, so two short items merged into one sentence long enough to
report a CH007 neither author wrote. It now uses `LIST_MARKER_RE`, the one the
container logic already uses, which structurally prevents the drift.

Accepting `1)` has a cost the finding did not mention, and the corpus proved
it: `202)` and `503)` also match, and both occur here as the tail of a wrapped
parenthesis. Two guards, verified against the whole corpus:

- A `N)` marker with an unmatched `(` earlier in the paragraph closes that
  paren; it is not a marker.
- CommonMark's own rule — only a bullet, or the number one, may interrupt a
  paragraph. Inside a list any number continues it.

That second guard exposed a third defect: a `// ─────` section rule was being
joined into the sentence below it. A thematic break now ends the block. This
is the round's largest effect — 63 findings lose a leading rule from their
quoted text and point at the prose line instead of the rule line, and four
sentences that were only over the limit because the rule counted as a word
drop out. No rule's count rose in any file.

**A Tier B failure could name the wrong line.** The fingerprint *is* the
comment text, so identical comments share one. When the merge base already
carried a copy, reporting the first occurrence pointed at the legacy line —
telling a contributor to edit a comment they never wrote. Every candidate is
named now, with a count of how many are new. The self-test grew its first
direct check of the ratchet's reporting, not just its arithmetic.

### Round eighteen — list padding, a verb read as deliberation, and per-sentence lines

Three findings, all verified to reproduce first.

**A list marker followed by five spaces opened a fence that suppressed the
gate.** CommonMark counts one to four spaces after a marker as padding; five
or more means the content starts one space past the marker and the rest is an
indented code block. The marker pattern consumed all of it, so:

```
/// -     ```rust
/// TODO: issue required        -> silently exempt
```

That is a Tier A bypass, the failure mode this harness exists to prevent.
`list_content` now returns the CommonMark content column, and both the
container logic and the fence detector measure from it.

**`lets` as an ordinary verb was read as deliberation.** `// The semaphore
lets just one claimant proceed.` failed CH003, an absolute gate, for stating
behaviour. The alternative is there for the misspelling of `let's just`, so it
now has to open a sentence — the same constraint `actually` already carries,
for the same reason from the other direction.

**Every sentence in a paragraph reported the paragraph's first line.** Joining
wrapped lines into a prose unit lost which line each came from, so a long
sentence three lines into a doc comment was reported against line one. The
join now carries each fragment's offset and line, and sentence splitting keeps
offsets, so a finding points at the sentence that caused it.

This is the round's largest effect and the third output-quality finding in a
row: **6514 of 19542** findings — a third of the corpus — now name a different,
correct line. `debug.rs` is typical: the flagged sentence begins on line 7 of a
paragraph opening on line 6, and was reported at 6.

The set of findings is otherwise byte-identical to round seventeen's — same
rules, paths and text, only lines moved — and no rule's count rose in any file.

### Round nineteen — three more synthetic fences, and destructuring `let`

Four findings, all verified to reproduce first. Three are the same failure
class: a line that is not a fence opener was read as one, and everything after
it was silently exempt from the absolute gates.

**A fence did not end when its container did.** CommonMark closes a fenced
block with the list item or block quote holding it, closing delimiter or not.
The container was only recomputed while no fence was open, so:

```
/// - ~~~rust
///   let x = 1;
/// TODO: issue required        -> silently exempt
```

An open fence now records the container column and quote depth it started in,
and any later line that dedents below either ends it.

**Padding was measured in characters, not columns.** A tab is up to four
columns wide, so `- \t\t~~~rust` is three characters of padding and seven
columns of it — past the four-column limit round eighteen added, and through
it. Columns are expanded from the start of the line, because a tab's width
depends on the column it sits in.

**A marker's own indentation was unbounded.** `///     - ~~~rust` is an
indented code line, not a list item, but any amount of leading whitespace was
accepted. It is now limited to three columns past the container, like every
other CommonMark indent here.

All three live in `list_content`, which now applies the three rules together.

**Destructuring `let` bypassed CH001.** The binding had to be a single `\w+`,
so `let (left, right) = split();`, `let [a, b] = arr;`, `let Foo { x, y } =
value;` and the let-else forms were all missed. Two anchored alternatives
added, and thirteen shapes added to the code-vs-prose boundary test — eight
code, five prose, including `let (or rather, allow) the worker retry;`.

Corpus effect: none at all. All four were latent, and the finding set is
identical to round eighteen's.

One round-eighteen fixture was wrong and is corrected here. It asserted that
an unindented line after a list-item fence stays fenced; it does not, and the
container fix above is what exposed it.

### Round twenty — columns everywhere, a container stack, and tuple let-else

Three findings, all verified to reproduce first. Two are follow-ons to the
previous round's fixes, in the two places the fix did not reach.

**Indentation was compared in characters again.** `list_content` learned to
count columns last round; `leaves_container` did not, so a tab-indented fence
body read as one column against a four-column container and the fence was
judged to have ended. That reports an example's own sample `TODO` as CH002 —
a false positive on an absolute gate, the opposite failure from the one the
container fix was for. `leading_columns` is now the single place indentation
becomes a number, and both callers use it.

**A dedent out of a nested list item reset the container to zero.** After
`- outer` and `  - inner`, a line back at the outer item's content column
dropped the container from 4 to 0, so a fence opened there was recorded as
top-level and outlived the list entirely. The container is a stack now, popped
to the nearest surviving item rather than emptied:

```
- outer               -> [2]
  - inner             -> [2, 4]
  ~~~rust             -> [2]      (was 0)
TODO: issue required  -> []
```

**Tuple and slice let-else still bypassed CH001.** The destructuring branch
added last round required a terminating semicolon, so `let (Some(a), Some(b))
= pair else {` and `let [first, ..] = slice else {` were missed while the
capitalised-pattern branch beside it already accepted `else {`. Both branches
take the same terminator now.

Corpus effect: none. All three were latent, and the finding set is identical
to round nineteen's.

### Round twenty-one — the container stack learns quote depth and paragraphs

Two findings, both in the stack added last round, both verified to reproduce
first.

**A list container outlived the block quote it opened in.** A stack entry was
a bare column, so `/// > - quoted item` left a container at column 2 standing
after the quote ended. A later four-space top-level line then measured as a
fence against a list that is no longer open, and suppressed the `TODO` below
it. Entries now carry their quote depth and are dropped when the line's depth
falls below it.

**A marker that cannot interrupt a paragraph still opened a container.**
`interrupts_paragraph` was applied to sentence splitting and not to the
container stack, so `2.` partway through a list item's paragraph pushed a
synthetic nested container. A valid fence then recorded against the fake
column, and the example's own body appeared to dedent out of it — CH002 on
fenced sample text.

The rule needs paragraph state, and the subtle part is when to clear it: any
pop clears it, because dedenting out of an item ends the paragraph inside it.
That is what keeps `2.` on the line after `1.` opening its own item while `2.`
inside an item's paragraph does not:

```
1. first                     -> [(3, 0)]   pops, so "2." may interrupt
2. second                    -> [(3, 0)]
- outer paragraph            -> [(2, 0)]
  2. still the same          -> [(2, 0)]   no pop, so "2." may not
```

Corpus effect: none. Both were latent, and the finding set is identical to
round twenty's.
