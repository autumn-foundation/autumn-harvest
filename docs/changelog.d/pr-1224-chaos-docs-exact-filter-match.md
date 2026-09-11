## Phase — exact filter-argument equality in the chaos docs/CI parity guard (issue #1224)

`chaos_docs::doc_example_filter_matches_chaos_workflow_filter` tied
`docs/testing/chaos.md`'s local-iteration example to
`.github/workflows/chaos.yml`'s filter using two independent
`contains("chaos_tests::")` checks. Both are prefix checks, so if either
side narrowed its filter (e.g. CI to
`chaos_tests::chaos_seeded_convergence_sweep` while the doc kept the
broader `chaos_tests::`, or the reverse), both checks still passed and the
divergence went undetected (Codex finding on PR #1223, deferred from
issue #1202 as P2 round 4).

**What shipped.**

- Added `extract_filter_argument(command: &str) -> &str`
  (`autumn-harvest/tests/integration/chaos_docs.rs`): extracts the exact
  token following `--test integration`, skipping whitespace and backslash
  line continuations. A token that is itself another flag (e.g. `--no-run`)
  or an empty gap counts as no filter.
- `doc_example_filter_matches_chaos_workflow_filter` now isolates each
  side's specific command (`workflow_step_stanza` for CI,
  `command_containing` for the doc, both already used elsewhere in this
  file) and asserts the two extracted arguments are **exactly equal**, not
  merely prefix-matching.
- The CI side now strips `#` comment lines before searching for the
  `chaos_tests::` step, matching the doc side's existing guard against a
  comment mentioning the filter as prose.
- TDD: unit tests for `extract_filter_argument` (same-line, continuation-
  wrapped, both no-token panic branches) plus two fixture regressions
  reproducing the Codex scenario in both directions (CI narrows / doc
  narrows), and one end-to-end synthetic pipeline test, were written and
  confirmed red before the fix existed, then green after.
- Manually reproduced the real-world scenario by narrowing
  `.github/workflows/chaos.yml`'s filter to
  `chaos_tests::chaos_seeded_convergence_sweep` — the new equality test
  failed as expected (`left: "chaos_tests::"`, `right:
  "chaos_tests::chaos_seeded_convergence_sweep"`), then reverted.
- Reviewed from three independent angles (correctness, STE/style,
  test-coverage) by separate review passes; no blocking findings. Fixed:
  one untested panic branch, one unnumbered issue reference in a comment,
  and the CI-side comment-stripping gap above.
- Codex's automated PR review (PR #1474) found a further real gap, in two
  rounds: `cargo test --help` documents `[OPTIONS] [TESTNAME]`, so a cargo
  option between the flag and the filter extracted as if it were the
  filter itself. A shared option on both sides masked a real divergence —
  the same failure class this issue closed. Round 1 (`-q`, a valueless
  option) was fixed by skipping any token starting with `-`; round 2
  (`--color always`, a value-taking option) showed that fix incomplete,
  since a value-taking option's *value* doesn't start with `-` either and
  would itself be returned as the filter.
- Rather than enumerate which cargo options take a value (an open-ended,
  never-quite-complete list), `extract_filter_argument` now requires the
  filter immediately after the flag and panics, naming the offending
  token, on any option found there instead. Neither real file (chaos.yml,
  chaos.md) ever places an option in that position, so this is not a
  behavior change for either — only a stricter, honest contract that
  fails loud instead of guessing. TDD: both Codex scenarios are fixture
  tests confirmed red against the prior (skip-based) implementation, then
  green after the fix.
- CI's `Lint` job caught two `-D warnings` clippy findings
  (`map_unwrap_or`, `manual_assert`) that a local ad-hoc clippy run had
  missed (it failed first on unrelated pre-existing files under a
  different feature-flag combination, before ever reaching this one).
  Fixed by switching to `map_or_else` and a direct `assert!`, then
  verified locally with CI's exact invocation: `cargo clippy -p
  autumn-harvest --all-features --tests -- -D warnings` (clean).
- A third Codex round found two more gaps in the same area. (1) A plain
  `command.find("--test integration")` matched as a prefix of a longer,
  different target name (`--test integration_tests`); a new
  `find_flag_end` helper now requires a token boundary right after the
  flag and keeps searching past a false match. (2) The CI-side extraction
  read only the single physical line containing the flag out of the step
  stanza, so a `run:` command wrapped across lines (a `\` continuation or
  a folded `>-` block) — a shape chaos.yml doesn't use today, but the
  doc's own example does — would panic even with matching filters; now
  passes the whole stanza, which `extract_filter_argument` already
  tolerates via its whitespace/continuation skip. Both are fixture tests,
  confirmed against a real boundary case and a real continuation case.
- A fourth Codex round found the symmetric case of (1) above, plus a
  soundness gap in the continuation skip itself. (3) `find_flag_end`
  only checked the boundary *after* the flag; a missing space could glue
  it onto a preceding token (`test--test integration`) and still match.
  It now checks both sides. (4) The whitespace skip between the flag and
  filter treated a bare newline the same as a real `\`-continuation. If
  the doc's example ever lost its continuation backslash, the real
  `cargo test` invocation would run unfiltered, but this guard would
  still extract a filter and pass. A new `skip_continuation_gap` skips
  only `\` immediately followed by a newline, plus ordinary horizontal
  whitespace; a bare newline now stops the skip and the empty token that
  results panics, exactly matching what the real shell would do. Both
  are fixture tests, confirmed against a glued-flag case and a
  dropped-backslash case.
- A fifth Codex round found the mirror gap in `find_flag_end`'s own
  boundary check (round 4 only tightened the flag-to-filter gap): a
  lone `\` immediately before the flag was still accepted as a
  boundary, even though bash never treats a bare `\` as a separator
  (only `\` + newline). `cargo test\--test integration` is one glued
  argument, not a real flag. The before-check now requires plain
  whitespace, or the exact `\` + newline ending of a real continuation.
  Fixture test confirmed against a glued-backslash command.
- The same round raised a second point -- a YAML `run: >-` folded
  scalar joins its source lines with a space, not a real newline, so
  round 4's bare-newline panic would misfire on a workflow reformatted
  that way. Verified and **not applied**: chaos.yml's `run:` is a
  single physical line today, so this is not a live bug; implementing
  it would mean parsing YAML block-scalar styles inside a docs/CI
  parity smoke test; and the failure mode is a loud, immediately
  diagnosable CI failure, not a silent wrong pass -- the opposite of
  every other finding in this issue. Documented as a known, accepted
  trade-off in `skip_continuation_gap`'s doc comment instead.
- A sixth Codex round found a real gap the round-3 "pass the whole
  stanza" fix introduced: `extract_filter_argument` matches the FIRST
  occurrence of the flag, so a step `- name:` line that itself reads
  like `Run --test integration chaos_tests:: suite` would match there
  instead of the real `run:` command, silently comparing against stale
  prose if the actual command has since narrowed. Added `run_command`,
  which anchors to the stanza's `run:` key specifically (to the end of
  the stanza, so multi-line/continuation commands still work). This
  subsumes the round-3 fix rather than replacing it. Fixture test
  confirmed against a step name containing look-alike text.

No production code changed — `chaos_docs.rs` is a test-only doc/CI parity
guard behind no feature flag. `cargo test -p autumn-harvest --test
integration chaos_docs::` (24/24), `cargo fmt -p autumn-harvest -- --check`,
`cargo clippy -p autumn-harvest --all-features --tests -- -D warnings`,
and `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` are
all clean.
