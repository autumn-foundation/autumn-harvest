## Fix — `backup verify` text output names both causes when unreadable and skipped-no-handler samples mix (issue #1410)

**Text-output wording fix only** (implemented). `format_backup_verify_text` in
`autumn-harvest-cli/src/lib.rs` had one branch for "every sampled history was
unreadable" that fired whenever `unreadable > 0` and nothing replayed,
regardless of `skipped_no_handler`. `ReplaySummary` is a fleet-wide merge
across shards (`ReplaySummary::merge`), so a run where one shard's samples
were all unreadable and another shard's samples were all skipped for lack of
a registered handler hit that branch. The message then claimed "every
sampled history failed to read" (false — some were skipped, not unreadable)
and "registering workflow handlers will not fix this" (false — it would fix
the skipped subset).

**The fix.** The all-unreadable branch now also requires
`skipped_no_handler == 0`. A new branch covers the mixed case: nothing
replayed, but for two separate reasons at once. It names both counts and
says registering handlers may fix part of this, not all of it.

**Scope, unchanged from the issue.** `backup_verify_json` serialises the raw
`ReplaySummary` fields directly, so machine consumers were never misled.
`ReplaySummary::verified()`, the report `status`, and the CLI exit code are
all unaffected — this was a human-readable wording gap in an
already-degraded ("nothing replayed") informational message.

**Test evidence.** `autumn-harvest-cli/tests/integration/backup_verify_cli.rs`
adds `text_output_names_both_causes_when_unreadable_and_skipped_combine`,
covering a summary with `unreadable: 2` and `skipped_no_handler: 3`. It
asserts the message names both counts, does not claim every history was
unreadable, and does not claim handlers will not fix any of it. Written
first against the unfixed branch and confirmed failing (TDD red phase)
before the fix landed. A second test,
`text_output_reports_partial_coverage_even_when_skipped_is_also_present`,
confirms the PARTIALLY VERIFIED branch still wins when something replayed
alongside both an unreadable and a skipped count. The existing
all-unreadable and all-skipped tests were re-run unchanged to confirm no
regression.

**Review.** Checked from three angles (branch-logic correctness, operator
wording, test coverage) after the fix landed. Correctness and coverage
came back clean; wording review caught one inconsistency against the
sibling branch's phrasing, fixed in the same change.
