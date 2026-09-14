## Phase — print the actual history shape on a flaky assertion mismatch (issue #1558)

`worker_fails_workflow_when_activity_start_to_close_timeout_elapses`
(`autumn-harvest/tests/integration/integration_e2e.rs`) failed once after a
prior hardening fix. The issue ruled out all four candidate root-cause
mechanisms by reading source and found no supported hypothesis. Its
highest-value requested follow-up: the failing `assert!(matches!(...))`
printed nothing on mismatch, so the one occurrence gave no evidence of the
actual wrong event shape.

**What shipped.**

- Replaced the bare `assert!(matches!(history.events.as_slice(), [...]))`
  with a `match` whose fallback arm panics with the debug-formatted actual
  event slice: `other => panic!("... got {other:#?}")`. The next occurrence
  of this flake now shows the real event sequence instead of a bare
  assertion failure.
- TDD: confirmed red (a reference to the diagnostic helper failed to
  compile), then green, before wiring the diagnostic into the assertion.
- Reviewed from three angles (correctness/borrow-checking, comment-hygiene
  and CLAUDE.md conventions, issue-scope adequacy). The scope review found
  this same file already prints the actual value inline on a match failure
  (`other => panic!("...", other)`, used twice elsewhere in this file and
  across the suite) with no prior helper function anywhere in the test
  corpus. Dropped the newly added helper function and its unit test in
  favor of the same inline idiom, to avoid an unneeded abstraction.
- A comment-hygiene review flagged one sentence using past tense
  ("found") instead of the required present tense; fixed before the
  helper was removed as part of the idiom-matching fix above.

No production code changed — this is a test-only diagnostic. The other
three requested follow-ups (a same-commit rerun campaign, a test-vs-product
verdict, and confirming branch hygiene on the two pre-hardening
occurrences) are manual/operational or contingent on a future failure that
has not happened yet; none require a code change here.

`cargo check -p autumn-harvest --test integration --features db`,
`cargo clippy -p autumn-harvest --test integration --features db -- -D
warnings`, `cargo fmt -p autumn-harvest -- --check`, and `python3
docs/audits/comment-hygiene.py --base origin/trunk-dev` are all clean. The
sandbox this change was authored in has no Docker daemon, so the
DB-backed integration test itself could not be executed directly; the
change was verified through the compile/lint/format checks above and an
isolated unit test for the diagnostic helper before it was folded into
the inline idiom.
