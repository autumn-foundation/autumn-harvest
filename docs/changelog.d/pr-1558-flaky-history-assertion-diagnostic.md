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

**The diagnostic worked on its first CI run.** It caught a live occurrence
and printed the actual wrong shape for the first time:
`ActivityStarted` was missing entirely between `ActivityScheduled` and
`ActivityTimedOut`. Source read confirmed the mechanism: `claim_task`
(`queue.rs`) stamps `started_at` — the `StartToClose` deadline anchor
(`timeout.rs`'s `start_to_close_timeout_query`) — in the same query that
sets the task `RUNNING`, before the dispatch pipeline appends
`ActivityStarted` (`worker.rs`'s `append_activity_started_if_pending`,
which correctly no-ops if enforcement already won). This test's
`default_start_to_close` (100ms) is tight enough for ordinary CI dispatch
overhead to occasionally win that race. Both resulting shapes are correct
engine behavior, not a bug.

**Fix:** the test's assertion now accepts both shapes (`ActivityStarted`
present or absent before the terminal `ActivityTimedOut`/`WorkflowFailed`
pair), instead of asserting only the one that assumes `ActivityStarted`
always wins the race. This removes the flake without relying on a timing
margin — widening `default_start_to_close` would only have shrunk the
race's probability, not eliminated it.

No other production code changed. The remaining requested follow-ups (a
same-commit rerun campaign, and confirming branch hygiene on the two
pre-hardening occurrences) are manual/operational; the test-vs-product
verdict is now answered above.

`cargo check -p autumn-harvest --test integration --features db`,
`cargo clippy -p autumn-harvest --test integration --features db -- -D
warnings`, `cargo fmt -p autumn-harvest -- --check`, and `python3
docs/audits/comment-hygiene.py --base origin/trunk-dev` are all clean. The
sandbox this change was authored in has no Docker daemon, so the
DB-backed integration test itself could not be executed directly here;
CI's own run confirmed both the original flake and, after the fix, is the
authority the diagnostic and repair are verified against.
