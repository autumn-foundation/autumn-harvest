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

### Round twenty-two — quote depth in the pop rule, fences end paragraphs, turbofish

Three findings, all verified to reproduce first.

**A block quote nested inside a list item closed the item.** `/// - outer`
then `///   > quoted`: stripping the quote marker puts the body at column
zero, which read as a dedent out of the item, so a fence opened afterwards was
recorded as top-level and outlived the list.

The rule is that columns only compare within one quote depth. A shallower line
has left the quote outright; a deeper one is *inside* the container, so its
stripped column says nothing about leaving it. Both the pop loop and
`leaves_container` now require equal depth before comparing columns.

**A fence did not end the paragraph before it.** The opener line set
`paragraph`, and nothing cleared it, so an ordered list starting at a number
other than one was refused a container immediately after a fenced block — and
the fence nested under that list then measured against the wrong column, so
its own sample text failed CH002. A fence delimiter now clears the paragraph,
in both loops.

**Turbofish calls bypassed CH001.** `Type::method::<T>(value);` and
`iter.collect::<Vec<_>>();` stopped the call pattern at `<`. A constrained
turbofish segment is allowed before the call parenthesis, on the receiver and
on each method. `see collect::<Vec<_>>() for the shape;` stays clean — the
pattern still has to open the line.

Corpus effect: none. All three latent, finding set identical to round
twenty-one's.

### Round twenty-three — literal markers, empty items, quote indent

Three findings, all verified to reproduce first.

**A list marker inside an open fence was stripped as a container.** Sample
text beginning `- ``` ` had its marker removed and the backticks read as a
closing delimiter, so the example's own `TODO` was scanned. List syntax is
literal inside a fence, so the marker is skipped only when looking for an
*opener*; `fence_delimiter` takes the fence state now.

**A marker alone on its line opened nothing.** `LIST_MARKER_RE` required
trailing whitespace, so `/// -` was not an item and an indented fence beneath
it was recorded as top-level. The pattern accepts end of line, and an empty
item's content column is one past the marker, as CommonMark specifies.

**A quote marker's indent was absolute.** Three columns, but three columns
from the line rather than from the container, so a quote inside a list item
whose content starts at column four was missed — and with it the fence inside
that quote, reporting the example's sample text. `quote_marker` measures
against the container in force, as every other marker here now does.

Corpus effect: none. All three latent, finding set identical to round
twenty-two's.

**On the trajectory.** Rounds seventeen to twenty-three have all been
CommonMark container modelling, all found by review rather than by the corpus,
and none has changed a single finding in this tree. They are real defects and
each was verified before fixing, but they are edge cases of a Markdown parser
that this harness only needs in order to know what *not* to scan. If they keep
coming, the better answer than a twenty-fourth round is an issue proposing a
real CommonMark block parser for the container layer, or a decision that the
remaining cases are out of scope for a comment linter.

### Round twenty-four — quote markers inside fences, quotes as prose blocks

Two findings, both verified to reproduce first.

**A quoted delimiter inside a fence closed it.** Round twenty-three stopped
stripping *list* markers inside an open fence but kept stripping every quote
marker, so a top-level fenced example containing a literal `> ``` ` had that
sample line read as a closer. Only the quote levels belonging to the fence's
own container are continuation syntax; anything deeper is sample text.
`strip_quote_levels` removes exactly the fence's depth and no more.

**A block quote merged into the paragraph above it.** `prose_units` stripped
the marker and appended the text, so an intro line plus a quoted 25-word
sentence counted as one 26-word sentence — a CH007 the author never wrote.
A quote is its own CommonMark block, so crossing into or out of one flushes
the unit, in both directions.

Corpus effect: none. Both latent, finding set identical to round
twenty-three's — the sixth consecutive round with byte-identical output. The
note at the end of round twenty-three still stands.

### Round twenty-five — the fence scope needs two frames, not one

One finding, verified to reproduce first, and its fix exposed a second defect
that the fixtures caught before it shipped.

A quoted fence inside a list item recorded quote depth zero, because the
`quote_depth` call that saves the fence's scope omitted the container the
marker is indented to. Its own closing delimiter then read as literal content
and the fence never closed.

Passing the container alone was not enough, and the round-twenty-three fixture
failed immediately: the saved *column* came from a different frame than the
saved depth. A quote marker is measured against the container it sits in (a
list item, at column four), while the content behind it starts again at column
zero. One number cannot be both.

The scope carries both now — `(outer, container, depth)` — with `outer` for
reading the marker and `container` for comparing columns once it is stripped.
`container_at_depth` picks the column recorded at the fence's own depth, since
a stack entry pushed while unquoted is a raw column and one pushed inside a
quote is measured after the marker.

Nine accumulated fence behaviours re-verified together. Corpus effect: none;
the seventh consecutive round with byte-identical output.

### Round twenty-six — nested container markers, and where this stops

One finding: several container markers on one line. `/// - 1. ```rust` and
`/// - > ```rust` open a list holding a list, and a list holding a quote, but
only one marker of each kind was consumed, so the fence never opened and the
example's own `TODO` failed CH002.

`strip_containers` peels markers one at a time now, measuring each in the
frame the last one left. Which frame that is depends on the marker: a list
marker on this line opens an item with nothing in it, so the next frame starts
at zero, while crossing a quote enters a depth the stack may already hold a
container for. Getting that wrong broke the round-seventeen quoted-list
fixture on the first attempt.

**Half of this finding is fixed and half is not, deliberately.**
`- 1. ```rust` works. `- > ```rust` opens the fence but its *body* is still
scanned, because `update_containers` also consumes one marker per line, so the
stack never records the quote container the list line opens and the fence's
saved scope is a frame off. Nothing regressed — that case behaves exactly as
it did before this commit — but it is not fixed.

Fixing it means giving `update_containers` the same recursive peel, which is
CommonMark's block-continuation algorithm: walk the line against the open
container stack, consume each container it continues, then open what is left.
That is a rewrite of the container layer, it re-frames every existing
container behaviour, and it is precisely option (a) of the choice raised at
the end of round twenty-three, which is still unanswered. Landing it
unprompted on the eighth consecutive round with no corpus effect is a larger
call than this PR should make on its own.

Corpus effect: none, for the eighth round running.

### Round twenty-seven — the fence's depth comes from the peel; headings are not paragraphs

Two findings, both verified to reproduce first. One of them answers a question
this PR asked two rounds ago.

**A fence behind a list and a quote recorded depth zero.** `/// - > ~~~rust`
opens after round twenty-six's peel, but the saved scope still read the depth
off the *unpeeled* line, where the quote follows a list marker and so is not
seen at all. The quoted closer then read as literal content and the fence never
closed. `strip_containers` returns the depth it reached, and the scope takes it
from there.

That is most of the half left unfixed last round, without the rewrite. What
remains is the artificial form where every line repeats both markers
(`- > ...` on the body as well as the opener), which is not how a list
continuation is written; the indented form Codex used here is, and it works.

**A heading left a paragraph open.** Round twenty-two clears paragraph state at
a fence, and I declined the broader "and other block-level structural lines"
because I could not construct a case — and asked for one on the thread. Here
it is: `# Heading` then `22. item` refuses the list its container, and a fence
under that item then measures against the wrong column and reports its own
sample text. Headings, thematic breaks and table rows no longer count as
paragraph content. A list marker's own text still does.

Worth recording that the question was the right thing to ask rather than
guessing at the class: the answer names three block types, and two of them
(`SEPARATOR_RE`, `TABLE_RE`) already existed in the file for the prose path and
simply were not consulted here.

Corpus effect: none, for the ninth round running.

### Round twenty-eight — counting quote levels, scoping to the inner item, peeling prose

Three findings, all verified to reproduce first, all in code written in the
previous two rounds.

**A multi-level quote counted as one.** `quote_marker` matches `> >` in a
single match; the peel added one to the depth per match rather than per
marker, so a two-deep fence stripped one level from its closer and never
closed. The depth advances by the number of markers consumed.

**A fence inside `- 1. ` was scoped to the outer item.** The peel reset the
container to zero per list marker without recording where the content actually
landed, so a body line that dedents out of the inner item but not the outer
one kept the fence open. `strip_containers` returns the column it reached, and
the fence's scope takes both column and depth from the peel — neither is
recoverable from the raw line.

**Prose kept the second marker as a word.** `- > <25 words>` stripped whichever
marker came first and left the other in the sentence, reporting 26 words. The
prose path peels the same way the fence path does now.

That last one regressed three `context.rs` findings on the first attempt, and
the corpus diff caught it: the peel applies CommonMark's indent limit while
`LIST_MARKER_RE` does not, so a bullet indented six columns with no container
open is a list item to one and indented code to the other. Its `*` stayed in
the sentence. The peel is used only when it actually consumes something, so
such a line reads exactly as it always has.

Corpus effect: none, for the tenth round running.

### Round twenty-nine — nesting is a scope, not a boundary; uninitialized bindings

Two findings, both verified to reproduce first.

**A nested comment inside a fenced example lost the fence.** A doc comment
holding a Rust example that itself contains a block comment —

```
/**
```rust
/* TODO: fixture placeholder */
```
*/
```

— had the inner comment split into its own run with fresh fence state, so the
example's own `TODO` failed CH002.

Round twelve gave a nested comment a new group precisely so its fence could
not leak outward, and that was half right: nesting is a *scope*, not a
boundary. A piece now records how deep it sits, its run is the whole outermost
comment, and the fence state is stacked per level — going deeper inherits the
enclosing fence, coming back out restores what was saved. Both directions hold
at once, which the group split could not express.

**Uninitialized bindings bypassed CH001.** `let mut retries: usize;` has no
`=` for the destructuring or assignment branches to anchor on. The new
alternative anchors on the type annotation instead, since English does not put
a colon between two bare words — and the 644-line adversarial prose sweep
immediately caught the first attempt on `let T: Send is required here;`, so it
carries the same three-bare-words rejection the other `let` branches use.

Corpus effect: none, for the eleventh round running.

### Round thirty — a bullet is not a fence; an empty marker is not an item

Two findings, both verified to reproduce first.

**A container marker exempted commented-out code.** `/// - let stale =
compute();` produced no CH001, because the rule is anchored to the start of
the stripped line and saw the bullet rather than the `let`. A list or a quote
is not a code fence, and the fence is the documented exemption — so the line
is peeled before CH001 is applied, with no container, so only markers
CommonMark would accept at the left margin are removed. Ordinary bulleted
prose stays clean, and the 644-line adversarial sweep and 79 boundary shapes
pass unchanged.

**An empty marker interrupted a paragraph.** Round twenty-three taught the
harness that `-` alone opens a list item, which is right at the start of a
block and wrong in the middle of a paragraph: CommonMark requires an
interrupting item's first line to carry content. A lone `-` after a prose line
was inventing a container, and the four-column fence beneath it then measured
against an allowance the rendered document does not have.

The two round-twenty-three behaviours now stand side by side — a lone marker
opens a list where a list may start, and stays paragraph text where one may
not.

Corpus effect: none, for the twelfth round running. CH001 is still zero, so
the first finding was a latent hole rather than a live miss.

### Round thirty-one — marker limits, marker values, spaced breaks, lazy quotes

Four findings, all verified to reproduce first. The first three are the same
mistake in three places: reading a marker's *spelling* instead of what
CommonMark says it means.

- **Ten digits is not an ordered marker.** The cap is nine; `\d+` accepted any
  run, so `1234567890.` opened a list and a fence allowance the rendered
  document has neither of.
- **`01.` may interrupt a paragraph.** CommonMark reads the marker's value,
  and the rule compared its spelling against the string `"1"`.
- **A thematic break may be spaced.** `* * *` is a horizontal rule, and it was
  read as a bullet — twice over, since the break pattern required contiguous
  characters *and* `list_content` did not give a break precedence over an item.
  CommonMark does.

**A quoted paragraph continues lazily.** A line with no `>` of its own carries
on the quoted paragraph above it, provided it is ordinary paragraph text.
Flushing there split one 30-word quoted sentence into two short units, and a
long sentence slipped past CH007.

That one exposed a fixture this PR added in round twenty-four, on its own
initiative rather than from a finding. Round twenty-four's finding was about
*entering* a quote; the reply claimed the rule held "in both directions" and
added a fixture asserting that leaving one flushes too. It does not — leaving a
quote onto paragraph text is exactly a lazy continuation. The fixture asserted
the bug. It is replaced by two that assert what actually holds: leaving onto
prose continues, leaving onto a block start flushes.

Corpus effect: none, for the thirteenth round running.

### Round thirty-two — the nesting snapshot has to hold everything

One finding, verified to reproduce first, in the fix from round twenty-nine.

That round made comment nesting a *scope*: going deeper inherits the enclosing
state, coming back out restores it. The snapshot held the fence and its scope,
and nothing else. So a list marker inside a nested comment — ordinary
paragraph text as far as Rustdoc is concerned, since the `/* */` delimiters
survive into the rendered documentation — pushed a container onto the
enclosing run's stack and left it there. The next delimiter measured against
that phantom container, opened a fence, and swallowed the `TODO` below it.

The snapshot now carries every piece of block state the loop holds: fence,
scope, container stack and paragraph flag, plus the quote depth and list flag
in the prose path. The rule is that a nested comment cannot change *anything*
about the block state of the comment containing it — which is what "scope"
meant, and the previous fix only implemented for one field of it.

Corpus effect: none, for the fourteenth round running.

### Round thirty-three — a marker-only heading, and two meanings of "separator"

Two findings, both verified to reproduce first.

**`#` alone is a heading.** The pattern required trailing whitespace, so a
marker-only ATX heading read as paragraph text and the `22.` after it was
refused a container. Same shape as round twenty-three's marker-only list item,
in the neighbouring pattern.

**A decorative rule is not a thematic break.** `SEPARATOR_RE` was widened in
round seventeen to cover `===` and the box-drawing rules this tree draws
sections with, because a section rule is not a word of the sentence beneath
it. That is right for splitting prose and wrong for container state: Rustdoc
renders `===` as ordinary paragraph text, so letting it clear the paragraph
flag hands the next `22.` a container, and its fence an allowance, that the
rendered document does not have.

The two meanings are separate patterns now. `SEPARATOR_RE` keeps the broad
decorative set and still ends a prose unit; `THEMATIC_BREAK_RE` is the
CommonMark subset — `-`, `_`, `*` only — and is what container state and the
break-beats-list-item precedence consult. Codex proposed exactly this split;
it is the right line, and it is one I had blurred by reusing a pattern written
for the prose path in the container path two rounds ago.

Corpus effect: none, for the fifteenth round running.

### Round thirty-four — four block rules, three of them already written elsewhere

Four findings, all verified to reproduce first.

**A quote marker needs no space.** `>~~~rust` opens a quoted fence in
CommonMark; round twenty-three's lookahead demanded whitespace, another
marker, or end of line. That lookahead exists only to keep a wrapped `>=`
operator from being read as a quote, so it is narrowed to exactly that: the
exception is `>=` and nothing else.

**A thematic break is indented like every other marker** — at most three
columns past its container. It accepted any indentation, so an indented `* * *`
inside a paragraph cleared the paragraph state and invented a list below it.

**A pipe-prefixed line is not a block.** Tables are a GFM extension, not
CommonMark, and `| not a table` is prose. It no longer touches container
state; `TABLE_RE` still ends a prose unit, which is the job it was written for.
Same split as round thirty-three's separator, one pattern along.

**A Setext underline ends a paragraph.** `===` under a paragraph line makes
that paragraph a heading. Round thirty-three established that `===` at the
start of a block is decorative text, and both readings are correct — this is
the position-not-shape split again, and the paragraph flag already
distinguishes them, so no lookahead is needed.

Three of the four are rules this file already applies somewhere else: the
container-relative indent limit, the CommonMark-only test for container state,
and the position split. The pattern named in round thirty-three — a rule fixed
in one place and unexamined in the next — is now the most productive one in
this review.

Corpus effect: none, for the sixteenth round running.

### Round thirty-five — the same three fixes, one pattern to the left

Three findings, all verified to reproduce first, and each is the neighbour of
something fixed in the previous two rounds.

**A container marker exempted an unreferenced TODO.** Round thirty peeled
containers before CH001 for exactly this reason and stopped there; CH002's
unpunctuated form is anchored the same way, four lines down, and saw the
bullet instead of the `TODO`. It uses the same peel now. A TODO carrying an
issue reference is still clean, and a fenced one is still exempt — the fence
remains the only exemption.

**A single pipe is not a table row.** Round thirty-four took pipe lines out of
*container* state and left them ending a prose unit, on the reasoning that a
table row is not a sentence. True of a row; `| foo` mid-paragraph is just a
word. `TABLE_RE` requires a second pipe now, so a sentence carries on across
it and a long one is reported.

**A Setext underline may be hyphens.** Round thirty-four's fix accepted only
`=`. `---` happened to work because it is also a thematic break, but `--` is
neither three characters nor recognised, so a hyphen-underlined heading left
its paragraph open.

All three are shape (iv) from the running list — a rule fixed in one place and
unexamined in the adjacent one — and two of the three are neighbours of my own
fix from the round before. The list of shapes is in the check-in notes; this
round is the clearest evidence yet that it is worth consulting before pushing
rather than after being told.

Corpus effect: none, for the seventeenth round running.

### Round thirty-six — a table is a structure, and Setext is a marker like any other

Two findings, both verified to reproduce first.

**Two pipes are not a table.** Round thirty-five narrowed `TABLE_RE` from one
pipe to two, which is a better guess and still a guess: a wrapped sentence
carrying `| ... |` was classified as a table row, dropped from its paragraph,
and the long sentence it belonged to went unreported.

Guessing is now replaced with the actual GFM rule. `table_rows` finds each
delimiter row (`|---|:--:|`), takes the header line above it, and extends
through the rows that follow. A pipe with no delimiter row anywhere is prose,
however many pipes it has. This is the structural check declined in round
thirty-four — correctly, for *container* state, where ignoring pipes entirely
is both safe and CommonMark-accurate; the prose path needs the real answer
because both of its wrong answers lose text.

**A Setext underline is measured against its container.** Absolute three
columns, so an underline inside a list item whose content starts past column
three was missed.

This is the fifth marker pattern in this file to need the container-relative
limit, and the fifth added without it — including, this time, one added in the
same commit whose review reply stated the property as a general rule. Writing
the rule down did not make it operate. It is a helper now, next to
`thematic_break`, which is the form the other four eventually took.

Corpus effect: none, for the eighteenth round running.

### Round thirty-seven — the same two lessons, applied where they had not been

Two findings, both verified to reproduce first, and both instances of patterns
this file already had names for.

**A block quote did not end the paragraph for container state.** Round
twenty-four taught the *prose* path that a quote is its own block and flushes
the unit; the container path never learned it, so a `22.` after a quoted line
was refused a container and the fence beneath it reported its own sample text.
Shape (iv) — fixed in one path, unexamined in the other.

**A table delimiter row is a marker.** Round thirty-six added `table_delimiter`
and stripped the line before judging it, so an over-indented delimiter made a
table out of the paragraph above and dropped its sentence. Sixth instance of
the container-relative rule, in the helper added one round earlier.

The delimiter check also moved out of the up-front `table_rows` pass and into
the loop, because that is the only place the container in force is known. The
table state is now one flag carried across lines rather than a precomputed set
— which is also how every other block in this file is tracked, so it should
have been written that way to begin with.

Corpus effect: none, for the nineteenth round running.

### Round thirty-eight — a blank segment is not a blank line

Four findings, all verified to reproduce first.

**The nesting snapshot omitted the quote depth** that round thirty-seven added
one commit earlier. Adding it to the snapshot was not enough, and chasing why
found a lexer defect underneath: a nested comment closing at the end of a line
leaves a **zero-length segment**, which the lexer emitted as a piece. A blank
segment is not a blank line — but every rule that ends a paragraph at a blank
line believed it was, so `Intro /* > inner */` ended its own paragraph. The
lexer now suppresses an empty segment when the line has already produced one,
and still emits the genuinely blank comment line CH004 depends on.

**A delimiter row needs a hyphen run in every cell.** `| | --- |` has an empty
first cell and is not a delimiter, so the pipe line above it is not a header.

**A confirmed table ends the paragraph.** Round thirty-four took pipe lines out
of container state entirely and argued the omission was safe because erring
open only costs an exemption. That was wrong: a real table before a list left
the paragraph open, refused the list its container, and reported the fence's
own sample text. The asymmetry stands — a bare pipe still changes nothing —
but the structure now exists to tell the two apart, so the container path uses
it.

**A sentence crossing an inline nested comment is one sentence.** The prose
path flushed at every nesting transition. Rustdoc renders the delimiters and
the text between them literally, so `20 words /* note */ 10 words` is one
30-word sentence. Only the *block* state is isolated by nesting; the sentence
is not.

Corpus effect: none, for the twentieth round running.

### Round thirty-nine — the container rule, the second loop, and round thirty-two again

Three findings, and none of them is new. Each is a rule already written down
in this file, applied everywhere except the one place the round found.

**An indented `#` is not a heading.** `HEADING_RE` matched `^\s*#`, with no
limit on the indent. Four columns into a paragraph that is indented content,
so reading it as a heading closed a paragraph the rendered document still
held open, and the `22.` under it then took a container — and its fence an
allowance — that nothing opened. `heading(text, container)` now measures the
indent the way `thematic_break`, `setext_underline`, `table_delimiter`,
`list_content`, `quote_marker` and `fence_delimiter` already do. That is the
seventh marker pattern here to need the container rule and the seventh
written without it.

A sweep of the rest follows the same rule, with one deliberate exception now
stated at the pattern: `SEPARATOR_RE` may match at any indent, because it
opens no container and closes no paragraph. A section rule is not a word of
the sentence wherever it sits.

**The table lookahead reads in the container it is in.** `comment_lines`
called `table_delimiter(strip_quote(next.text), 0)` — container zero, hard
coded — while `prose_units` passed the real container. A table nested in a
list item was therefore a table to one loop and prose to the other. Two loops
walking the same structure need the same arguments, and a literal `0` where
the other passes a variable is the shape of that defect.

**`in_table` belongs in the nesting snapshot.** Round thirty-two's title was
"the nesting snapshot has to hold everything", and round thirty-eight added a
variable to both loops without adding it there. A table inside a nested
comment left `in_table` true after the comment closed, so the enclosing run
read ordinary pipe lines as table rows. Both snapshots now carry it, and
`nesting_shift` says plainly that "everything" is checked by hand and leaks
silently when it is not.

Corpus effect: none, for the twenty-first round running.

### Round forty — what rustdoc actually renders

Two findings. One prescribed the wrong fix for a real defect, and the check
that settled it was rendering the shape with the compiler in the tree rather
than reasoning about the specification.

**A delimiter row must match its header's width.** The report asked for three
hyphens per cell. Rustdoc 1.94 disagrees: `| a |` over `| - |` renders as a
table, so one hyphen is valid and requiring three would make the harness miss
real tables. The reported *shape* is still a defect, for a different reason —
`Intro | header |` has two cells and `| - |` has one, and a delimiter row is
only a delimiter row for a header of the same width. `table_delimiter` now
takes the header and compares cell counts. Cells are split on unescaped pipes
only, and one list marker is peeled first, because rustdoc renders
`- | a | b |` as an item holding a two-column table.

**A checkbox is not two words.** Rustdoc renders `- [ ] text` with an
`<input>`, so the brackets are not prose. Counting them added two words to
every task item and reported a complying 24-word sentence as 26 — a CH007
regression that would fail CI on a correct comment. A valid marker is exactly
`[ ]`, `[x]` or `[X]` with a space after it; `[]`, `[y]` and `[ ]no-space`
stay literal, which is again what rustdoc does. A quote following the list
marker suppresses the strip, since `[ ]` in quoted prose is two real words.

Every claim above was checked by compiling a doc comment and reading the
generated HTML. That is the primary source for a tool whose whole job is to
agree with the renderer, and it should have been the first check in this seam
rather than the fortieth round's.

Corpus effect: none, for the twenty-second round running.

### Round forty-one — the other side of the nested comment, and compound types

**A gutter before a nested opener is not a blank line.** Round thirty-eight
suppressed the empty segment a nested comment leaves when it CLOSES at the end
of a line. The opening side had the same defect: `* /* note */` emitted the
` * ` in front of the opener as a piece, whose normalized text is empty, so
both scanners read a blank line and ended the paragraph one line early. The
`22.` under it then took a container and its fence an allowance, and a TODO
inside the invented fence went unreported. Rustdoc renders the whole sequence
as one paragraph, delimiters and all — checked, not assumed.

`gutter_only` asks the question `Piece.text` answers, and all four segment
emitters use it: before a nested opener, after a nested close, at the closing
line, and — unchanged from round thirty-eight — at a newline, where a
genuinely blank line must still count. A nested opener now also marks the line
as started, so a line holding only `/*` is not blank either.

**CH001 missed every compound type.** The uninitialized-binding rule needed a
type annotation to tell `let mut retries: usize;` from `let the reader
decide;`, and its character class admitted only scalar-shaped ones. An array
length needs `;`, a trait object `+`, a function pointer `->`, a raw pointer
`*`, so `let bytes: [u8; 32];` and its kin passed an absolute gate. The class
now carries them, with two constraints: the inner `;` is allowed only where a
`]` closes before the next one, so the statement's own terminator still ends
it; and a hyphen is admitted only as `->`. The first cut allowed a bare hyphen
and the adversarial prose sweep immediately produced `let a::b is re-exported
for callers;` — the sweep earning its place again.

Corpus effect: none, for the twenty-third round running.

### Round forty-two — one place decides what a table is

Both findings are the table lookahead, read once from each side.

**A delimiter row must be in the same comment as its header.** The lookahead
took the next piece without checking its nesting level, so `/* | --- | */`
under `| h |` was read as the outer header's delimiter. Rustdoc renders a
nested comment's delimiters literally — it even smart-quotes the `---` into an
em dash, which is proof enough that the text is inline — so the sequence is
one paragraph. Reading it as a table cleared that paragraph before the nesting
snapshot round thirty-nine added could restore anything, and a TODO under the
invented fence went unreported.

**A pipe does not end a lazy quote continuation.** The lazy test rejected any
line containing a pipe. Rustdoc renders a quoted sentence carrying on across
an unmarked `continued | ...` line as one paragraph inside the block quote, so
flushing there split a 28-word sentence into units of 20 and 8 and CH007 saw
neither. A newly added long sentence could pass the Tier B ratchet that way.

The fix for both is one function. `table_header` decides whether a pipe row is
a header — same comment, delimiter row underneath, matching width — and the
container path, the prose path and the lazy test all ask it. Three call sites
that each re-derived the answer are why rounds thirty-four to forty-two kept
finding the same question answered differently in different places.

Corpus effect: none, for the twenty-fourth round running.

### Round forty-three — a piece is not always a line

One finding, and the general defect under it: a nested comment splits one
source line into several pieces, and both scanners treated every piece as a
line of its own. Every Markdown BLOCK marker must start a line, so a piece
beginning after a literal `/*` or `*/` carries none of them.

`/** Outer /* ```rust` renders as a paragraph — the backticks sit after text,
so they open nothing — but the audit read the nested piece as a line, opened a
fence, and exempted the TODO under it. `Piece.line_start` now records whether
a piece begins its own line, and a piece that does not opens no fence, no
list, no quote and no table; its text joins the prose around it, which is
where Rustdoc puts it.

Fixing the reported case exposed the same loss of line context in the other
direction, and it was the worse of the two. A closing fence may be followed
only by spaces, so Rustdoc keeps the fence open across `` ``` /* note */ ``
— while the audit saw a piece reading `` ``` `` with nothing after it, closed
the fence, and reported a fenced example's own TODO. That is a **false
positive on a Tier A gate**, which fails CI on a legitimate example, and it is
the failure this harness must never produce. `line_tail` reassembles the rest
of the line past any nested comment, and a delimiter is now judged against the
whole line: no trailing text for a closer, no backtick in an opener's info
string.

Corpus effect: none, for the twenty-fifth round running.

### Round forty-four — an HTML block, a table, and the two abbreviations

Three findings, and the first corpus movement in twenty-five rounds.

**An HTML block ends the paragraph above it.** Rustdoc renders `Intro.` then
`<pre>raw</pre>` as a paragraph and a block, so the `22.` under them opens a
list and the fence inside it is a fence. The audit knew nothing of HTML, kept
the paragraph open, and reported a 26-word *code sample* inside that valid
fence as CH007. `html_block` recognizes CommonMark's type 1 and type 6 tag
names, and deliberately not type 7 (any complete tag alone on a line): type 7
cannot interrupt a paragraph, and `<T>` in a Rust comment is a type parameter.
A fixture pins that side too.

**The prose scanner did not pass the table flag.** `comment_lines` has decided
the table before the containers since round thirty-eight; `prose_units` still
decided it afterwards, so a `22.` after a real table was refused its container
and the same false CH007 appeared on the fenced sample. Rule (B) again, in the
one loop pair this review keeps finding it in. The two loops now read the same
way, in the same order.

**"e.g." does not end a sentence.** The header called the naive split a known
limitation and argued the fix would over-report. That argument was against
*requiring a following capital*, which would merge "... the row. Postgres ...".
Excluding two named abbreviations does not: neither "e.g." nor "i.e." ever ends
an English sentence, and this corpus writes both constantly. A 27-word sentence
carrying `e.g.` produced no finding at all. Only those two are excluded —
"etc." and "vs." do end sentences, so excluding them would merge two real ones.

Corpus effect: **19690 to 19887**, all CH007, and the first change in
twenty-five rounds. It is the abbreviation fix, and it is the sentences already
in the tree being measured at their true length: 274 findings restated as 471
longer ones. The ratchet recomputes both sides with the same code, so the gate
stays clean — 5018 in changed files against 5020 at the merge base.

### Round forty-five — a table row without a pipe, and how far an HTML block reaches

Both findings are the same mistake in two features added the round before:
recognizing where a block STARTS and never asking where it ends.

**A table body row needs no pipe.** Rustdoc renders `ordinary row with no
separator` under a table as a cell and fills the missing ones. The scanners
cleared the table state on any pipe-less line, so the table ended a line early,
the `22.` after it was refused its container, and a 26-word code sample inside
the fence below reported CH007. A GFM table runs to a blank line or the next
block, which is what `starts_block` now decides — and a fixture pins that a
blank line still ends one.

**An HTML block runs to its closer.** Round forty-four taught the audit that
`<pre>` opens a block and stopped there, so the preformatted line under it was
still counted as prose and reported. Both loops now carry the block: types 1
to 5 end on the line holding their closer, which may be the opening line
itself (`<pre>raw</pre>`), and a type-6 tag-name block ends at a blank line,
which remains a block boundary in its own right. Inside one, nothing is
Markdown and nothing is prose.

Three of the four shapes here are CH007 or CH002 reported on content that is
not prose. That is the false-positive direction, on a gate meant to run in CI,
and it is worth noting that both defects were introduced by the fixes for the
two rounds before them. Recognizing a block opener without its extent is a
half-implemented block, and a half-implemented block reports the inside of it.

Corpus effect: none. The tree writes no tables in `*.rs` comments and no raw
HTML in them, which is also why nothing caught these until they were rendered.

### Round forty-six — four false positives in one round, and the loop diff

Four findings, every one of them CH002 reported on preformatted content, and
three of the four are defects in the code round forty-five added.

- **A quoted HTML block was never recognized.** `comment_lines` asked
  `html_block` with the quote marker still on the line, while `prose_units`
  asked with it peeled. `/// > <pre>` opened nothing, so the TODO inside was
  read as prose.
- **A verbatim block closed on any tag.** Rustdoc keeps a `<pre>` block open
  across a literal `</style>` line. The kind is now the tag itself, and only
  its own closer ends it.
- **A block complete on one line still scanned that line.**
  `<pre>TODO: x</pre>` renders preformatted, and the opener's own line is
  inside the block it opens.
- **An empty nested comment left no trace.** `/**/` produces no piece, so
  `` ``` /**/ `` looked like a clean closing fence. Rustdoc keeps the fence
  open, because a closer may be followed only by spaces. `Piece.line_end`
  now records that a nested delimiter follows, whether or not it wrapped any
  text.

**The loop diff.** Round forty-four's reply said that if the
`comment_lines`/`prose_units` divergence recurred, the loops would be
reconciled wholesale rather than patched again. It recurred, so the two were
listed side by side and compared operation by operation. One divergence was
left beyond the reported one: `comment_lines` derived `quoted` from the raw
line while `prose_units` derived it from the container peel. In `- > text`
the marker comes first, so the raw line reports depth zero and one loop
believed the line had left the quote. Both now read the peel. Rustdoc renders
the shape that exposes it — `- > intro`, then `22.` and a fence in the item —
with the TODO inside `<code>`, so the audit was reporting a fourth false
positive that no review round had found.

Corpus effect: none. Every fix here is in a shape this tree does not write,
which is exactly why only the renderer finds them.

### Round forty-seven — one fix, and a line drawn

Three findings, all in the HTML block layer. One is fixed; two are recorded
and left.

**Fixed: a list marker is peeled before the HTML opener.** Round forty-six
taught `html_block` to peel a quote marker and stopped there, so `- <pre>`
opened nothing and the indented TODO under it reported CH002. Rustdoc renders
that content preformatted inside the item. Both scanners now peel through
every container, as the fence test already did. This is a false positive on a
Tier A gate, which is why it is fixed rather than deferred.

**Recorded, not fixed:** an HTML block is not scoped to the container that
opened it, so `> <pre>` followed by an unquoted line keeps the block open;
and an HTML closer inside an inline nested comment is not seen, because a
mid-line piece carries no block syntax. Both UNDER-report, which is the safe
direction for a gate, and both are recorded in KNOWN LIMITATIONS and in the
follow-up issue.

**Why stop here.** Rounds seventeen to forty-seven have all been in this
hand-rolled block layer, and rounds forty-four to forty-seven were largely
self-inflicted: each round's fix produced the next round's findings. The
corpus has not moved for any of them except round forty-four's sentence
splitter, and the reason is plain — no `*.rs` comment in this tree contains a
table or raw HTML. The gate does its job today: Tier A is at zero, the ratchet
is clean, and CH005 to CH007 measure the real corpus. What remains is a
Markdown parser being reimplemented one review comment at a time, in shapes
the repository does not contain. Either the block layer is replaced with a
real CommonMark parser -- which costs `docs/audits/` its deliberate
no-dependency property and is not a decision this PR should make -- or the
remainder is out of scope. This PR takes the second.

### Round forty-eight — indented code, and which comments are Markdown at all

One finding, and it is a false positive on a Tier A gate, so it is fixed under
the line drawn last round rather than deferred with the rest.

Markdown's other code block is four spaces of indent after a blank line, and
the audit did not know it. `///     let x = compute();` renders as a Rust
example and failed CH001; an indented TODO failed CH002. No unfenced rustdoc
example could be added to this tree. `indented_code` follows CommonMark: four
columns past the container, only where no paragraph is open — a block cannot
interrupt one, so a wrapped line indented under its own paragraph stays prose
— and a blank line stays inside the block until the indent ends.

**The first cut removed nineteen real findings, and that is the interesting
part.** Applied to every comment, the rule exempted indented passages in plain
`//` comments — `chaos.rs` and `context.rs` lay out long arguments that way —
and eighteen CH007 sentences and one CH005 stopped being measured. Those are
prose. Rustdoc renders no `//` comment at all, so nothing there is Markdown
and indentation is just how the argument is laid out. The rule is therefore
limited to the markers Rustdoc renders: `///`, `//!`, `/**`, `/*!`.

A ``` fence stays exempt in every comment, and the difference is not an
inconsistency. An author writes a fence to say "this is an example", whatever
the marker. Nobody indents a paragraph to say it.

Corpus effect: none, once the rule is limited to doc comments.

### Round forty-nine — the marker line, and the third scoping gap

Two findings, sorted by direction as the line drawn in round forty-seven says.

**Fixed: a code block may begin on the list marker's own line.**
`-     let x = compute();` is an item holding four columns of indented code —
CommonMark puts the item's content one column past the marker when the padding
exceeds four, and everything past that is code. Round forty-eight measured the
indent on the unpeeled line, so the bullet counted as content and the example
failed CH001 and CH002. The measurement is now taken in the frame the peel
leaves: `strip_containers` returns both the text and the container it ends in,
and the four columns are counted from there. That is the same peel-then-measure
shape rounds forty-six and forty-seven applied to the HTML opener, arriving one
round later at the feature added in between.

**Deferred to the follow-up issue: a table is not scoped to its container.** A
quoted table survives the line that leaves the quote, so a `22.` after it takes
a container the rendered document does not give it and a TODO under the
invented fence goes unreported. It is the exact sibling of the HTML scoping gap
already recorded there, it under-reports, and no `*.rs` comment in this tree
contains a table at all. KNOWN LIMITATIONS now names all three.

Corpus effect: none.

### Round fifty — `/***` is not a doc comment

Two findings, sorted by direction as before, but the fixed one is not in the
block layer at all.

**Fixed: the lexer called `/***` a doc comment.** Rustdoc documents nothing
for `/*** ... */` — compiled and checked, the item's page carries no docblock
— but the lexer recorded its marker as `/**`. Round forty-eight's indented
code exemption keys on that marker, so an indented `let x = compute();` and an
unreferenced TODO inside an ordinary `/***` block were treated as a rendered
example and Tier A came off the whole comment. It is a marker-classification
defect in the lexer rather than a Markdown one, and it silently disables the
absolute gate, so it is fixed rather than deferred. `/**/` was already
excluded for its own reason; `/***` now joins it, in the nested marker too.

That is the shape of the risk in keying anything on `marker`: round
forty-eight limited indented code to the markers Rustdoc renders, which was
right, and inherited a lexer bug that had been harmless until something
depended on it.

**Deferred to the follow-up issue:** an empty nested comment leaves the
fragments around it adjacent at the same nesting level, so `| h | /**/ | - |`
lets `next_row` take the resumed fragment on the *same physical line* as the
header's delimiter row. A delimiter row must be line-leading and on a later
line. It under-reports, and the shape is a table with an inline empty comment
in it.

Corpus effect: none.

### Round fifty-one — three rule defects, and three exclusions the corpus chose

Three findings, none of them in the Markdown block layer, and all three fixed.

**A sentence may end inside emphasis.** `**strong emphasis.** This second ...`
puts two asterisks between the full stop and the space, so the splitter merged
two compliant sentences and reported one long one — a CH007 that could fail
the ratchet on correct prose.

The interesting part is what is *not* in the fix. Three other closing
delimiters look equally reasonable and each produced false splits, found by
measuring rather than by argument: a backtick cut three hundred sentences,
because a code span carries punctuation constantly (`` `?` ``, `` `.` ``); a
closing paren cut the tree's own `(first is `0`, second is `1`, ...) -- so
prefix explicitly`, where the ellipsis is mid-sentence; and a quote cut
`answer "is `charge_card` held?" during an incident`, which is one sentence.
Only `*` and `_` survive, and the reasoning for each exclusion is recorded at
the pattern.

**A marker inside a code span is documentation.** ``Parse the `TODO:` prefix``
failed CH002, so the syntax of the marker could not be documented without an
issue reference. CH002 now blanks inline code spans before matching. CH005 and
CH006 deliberately do not, and the KNOWN LIMITATIONS entry that says so now
records why CH002 differs: it is absolute, and a false positive there fails the
build outright.

**`pub(in crate::foo)` is a visibility.** The prefix shared by five CH001
alternatives allowed word characters and colons but not the space in
`pub(in path)`, so an unambiguous commented-out declaration passed an absolute
gate.

Corpus effect: 19887 to 19825, all CH007 — 275 merged sentences restated as
213 correctly split ones. The ratchet recomputes both sides, so the gate stays
clean.

### Round fifty-two — three false positives, and the doc-marker limit again

Three findings, all of them the audit reporting a defect on correct code, and
all three fixed.

**Only a backtick fence forbids a backtick in its info string.** Round
forty-three's whole-line delimiter check applied that rule to every fence, so
`~~~rust /* note */ `info`` was discarded as an invalid opener and the TODO
inside the example reported CH002. CommonMark restricts the info string only
for backtick fences, because a backtick there would be ambiguous with the
delimiter itself; a tilde fence has no such problem.

**A heading on a list-marker line is a heading.** Rustdoc renders `- # text`
as an `<h2>` inside the item. The flush test read the unpeeled line, so the
heading fell through to the list branch and its title was added to the prose
run — a 26-word heading reported CH007. The test now classifies the peeled
content, like the HTML test beside it.

**A Setext title is a heading, not a sentence.** A long line followed by `===`
renders as a heading, but the underline reached the separator branch, which
*flushes* — emitting the accumulated title as a prose unit. An ATX heading
never reaches the run at all, so the same title reported CH007 in one form and
not the other. The run is now discarded rather than flushed.

**And the same limit as round forty-eight, for the same reason.** The first
cut of the Setext fix removed sixty-six real findings: this tree closes plain
`//` banner comments with a rule of hyphens, and every paragraph above one was
discarded as a heading title. Rustdoc renders no `//` comment, so nothing
there underlines anything. The discard is limited to `///`, `//!`, `/**` and
`/*!`, and a fixture pins the banner case.

That is twice now that a correct Markdown rule, applied to every comment, has
quietly stopped the tool measuring real prose — and twice the corpus diff was
the only thing that noticed.

Corpus effect: none, once limited.

### Round fifty-three — a tuple struct, and the second absolute rule to blank code spans

Two findings, neither in the Markdown block layer, both fixed.

**A tuple struct is a declaration.** The item-header alternative accepted `(`
only where it ended the line, so `struct CountingLayer(Arc<Mutex<u64>>);` — an
ordinary form, and one this repository writes throughout — passed the absolute
gate. It gets its own narrowly-anchored alternative rather than a relaxation of
the existing one, which is what this file's own guidance asks for: the name
stays anchored against `struct`, the field list may not contain a brace or a
semicolon, and the line must end at the terminator. `struct fields are
described below;` and `struct (or enum) definitions live here;` still read as
prose.

**CH003 blanks inline code spans, as CH002 does.** ``Parse the `let's` token``
failed the narrative-aside rule, so a literal containing a narrative phrase
could not be documented. The two rules that fail the build outright now agree
on this; CH005 and CH006 still read the raw text, because they are ratcheted,
and the KNOWN LIMITATIONS entry already records that split. `// Actually,
let's just skip the retry here.` still reports.

Corpus effect: none.

### Round fifty-four — a code span may wrap, and an array field carries a semicolon

Two findings, both fixed.

**A code span may wrap onto the next line.** Rustdoc renders ``` `literal ```
and ``` TODO: marker` ``` on consecutive lines as one `<code>` span, but the
blanking round fifty-one added ran a line at a time and never saw the opener,
so CH002 failed the build on a marker inside a literal. The blanking now
happens over the joined lines and splits back, so a span that opens on one
line closes on the next. Two properties are kept and pinned: an unmatched
backtick blanks nothing, which is what CommonMark does with it, and a blank
line ends the paragraph and so ends any span. Fenced lines are emptied before
joining, so a fence's backticks cannot open a span over the prose after it.

**An array field carries a semicolon.** Last round's tuple-struct alternative
excluded every `;` from the field list, so `struct Packet([u8; 32]);` passed
the gate. The uninitialized-binding rule has had the answer since round
forty-one — admit a `;` where a `]` closes before the next one — and the new
alternative was written without it. One round, one sibling, again.

Corpus effect: none.
