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

No production code changed — `chaos_docs.rs` is a test-only doc/CI parity
guard behind no feature flag. `cargo test -p autumn-harvest --test
integration chaos_docs::` (18/18), `cargo fmt -p autumn-harvest -- --check`,
`cargo clippy -p autumn-harvest --all-features --tests -- -D warnings`,
and `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` are
all clean.
