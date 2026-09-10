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
- TDD: unit tests for `extract_filter_argument` (same-line, continuation-
  wrapped, no-following-token panic) plus two fixture regressions
  reproducing the Codex scenario in both directions (CI narrows / doc
  narrows) were written and confirmed red before the helper existed, then
  green after.
- Manually reproduced the real-world scenario by narrowing
  `.github/workflows/chaos.yml`'s filter to
  `chaos_tests::chaos_seeded_convergence_sweep` — the new equality test
  failed as expected (`left: "chaos_tests::"`, `right:
  "chaos_tests::chaos_seeded_convergence_sweep"`), then reverted.

No production code changed — `chaos_docs.rs` is a test-only doc/CI parity
guard behind no feature flag. `cargo test -p autumn-harvest --test
integration chaos_docs::` (13/13), `cargo fmt -p autumn-harvest -- --check`,
and `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` are
all clean.
