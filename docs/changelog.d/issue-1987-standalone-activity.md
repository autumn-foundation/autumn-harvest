## Phase — Standalone durable activity: measured, decided (issue #1987)

**Decision.** [ADR 0006](../adr/0006-standalone-activity.md) records it:
build a standalone-activity API, as a follow-up under epic #1969. Until it
ships, run one durable job as a one-step workflow. The activities guide
shows the pattern and its local-activity fast path.

**Measurement.**
[One-step workflow overhead against a bare activity](../performance-standalone-activity-overhead.md)
compares three arms per job. A one-step workflow with a regular activity
writes 17 rows and 13453 WAL bytes. With a local activity it writes 10
rows and 7879 WAL bytes. The bare floor, one task row with no workflow,
writes 3 rows and 2327 WAL bytes. `DESIGN-1987.md` fixed a 2.0x line
before the first measurement. The local-activity arm is 3.33x on rows and
3.39x on WAL, so the line gives "build".

**No migration. No new `WorkflowEvent` variant. No route change. No
engine change.**

**Tests.** `standalone_activity_overhead_perf` asserts the events, task
rows and claims per job for each arm against a live database. Its
ignored `zz_capture_standalone_activity_overhead_evidence` writes the raw
evidence to `docs/perf-artifacts/standalone-activity-overhead/`.
`standalone_activity_docs` checks that the page quotes those counts, that
its ratios match its cost table, that the cost table is the captured one,
and that the ADR states the verdict that the line gives.
