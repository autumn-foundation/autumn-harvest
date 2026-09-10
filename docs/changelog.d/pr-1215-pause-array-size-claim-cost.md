## Docs — pause-array-size claim cost (issue #1215)

`docs/performance.md`'s "Known limitations" section listed five claim-path
predicates the attribution table does not measure (capability labels, queue
pauses, `schedule_to_close`, worker sessions, sticky routing). A sixth —
the per-activity-type pause (#807) — was not on that list at all, and the
queue-pause fix's own "resolved" entry (#619) had been measured against
exactly one active pause, never a realistically wide pause table.

Issue #1215 found both gaps share one root cause: the claim sort's
disk-spill trigger depends on the width of the `<> ALL(...)` anti-join
array, not only on backlog depth. A new evidence-capture test,
`claim_budget_tests::zz_capture_pause_array_size_claim_evidence`, sweeps
array size (0/1/20/199 ballast rows, 0% selectivity) at the 10,000-row
headline backlog for both pause tables and commits the resulting `EXPLAIN`
plans under `docs/perf-artifacts/pause-array-size/`.

**The two predicates are not equally exposed.** `paused_activities` (#807)
reads `harvest_activity_pauses` in full on every claim, with no bind to
keep the array small — the sort spills to disk once the table holds around
20 rows, regardless of the calling worker. `paused_queues` (#619) is
pre-filtered by `$2` (the worker's own polled queues, typically single
digits), so a fleet-wide pause of 199 *other* queues never widened its
array past zero elements in this sweep and the sort stayed in memory. The
same disk-spill mechanism does reappear for `paused_queues` once a worker's
own `$2` bind is itself wide (199 polled queues, all paused) — confirmed,
not merely inferred, but a deployment shape this page does not otherwise
measure or expect.

`docs/performance.md` now documents both findings: a new "Activity pauses
(#807)" bullet in "Known limitations", a corrected "Queue pauses (#619)"
bullet noting the array-size caveat, and a new "The pause-array-size sweep
(issue #1215)" section with the measured table. The `ClaimGate` doc comment
in `claim_bench_support.rs` now names #807 alongside its other five known
gaps. `queue::claim_task_query()`'s own doc comment no longer calls
`paused_activities` unconditionally "cheap" — it cites the measured
disk-spill threshold instead.

Along the way, `claim_bench_support.rs::reset()` was found to never
truncate `harvest_activity_pauses` (unlike its sibling
`harvest_queue_pauses`), which would have silently contaminated this same
evidence capture across repeated runs. Fixed, and pinned by a new
`reset_clears_every_pause_table` test that fails against the prior
behavior. A second new test,
`claim_excludes_paused_rows_with_a_realistically_wide_pause_array`, proves
`claim_task()` still excludes exactly the paused queue and the paused
activity when both pause tables hold 199 rows — correctness coverage this
scale previously had none of.

**Zero engine impact.** Like every other finding on this page, this changes
nothing about `claim_task_query()`: no new `WorkflowEvent` variant, no
migration, no schema change, no public API change, and no query-shape fix.
The change is entirely new test/benchmark infrastructure and documentation
— see `docs/performance.md`'s own "Zero engine impact" note on this
section for why no code fix is proposed here. Per this repo's
measure-before-tune discipline, whether a fix is warranted at all — and
what it would cost `paused_activities`' correctness guarantees — is a
separate, unopened question.
