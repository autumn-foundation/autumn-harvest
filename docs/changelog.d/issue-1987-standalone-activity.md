## Perf — Standalone durable activity: measured, decided (issue #1987)

**Decision.** [ADR 0006](../adr/0006-standalone-activity.md) records it:
document the one-step-workflow pattern, and do not build a
standalone-activity start path now. The activities guide shows the pattern
and its local-activity fast path.

**Measurement.**
[One-step workflow overhead against a bare activity](../performance-standalone-activity-overhead.md)
compares four arms per job:

| Arm | Shape | Rows | WAL bytes |
|---|---|--:|--:|
| A | One-step workflow, regular activity | 17 | 13429 |
| B | One-step workflow, local activity | 10 | 7890 |
| C | Bare task row | 3 | 2326 |
| D | Task row, job record, handler-start marker | 6 | 5073 |

`DESIGN-1987.md` fixed a 2.0x line before each floor ran. Against arm C,
arm B is 3.33x, and that line gives "build". Review showed that the line
could not fail, so a re-charter against arm D decides. Arm B is 1.67x on
rows and 1.56x on WAL against arm D, so that line gives "document". The
decision covers jobs that fit a local activity. Arm A is 2.83x on rows and
2.65x on WAL against arm D. That gap is recorded as an open question.

**No migration. No new `WorkflowEvent` variant. No route change. No
engine change.**

**Tests.** `standalone_activity_overhead_perf` asserts the events, task
rows and claims per job of each arm against a live database. The ignored
`zz_capture_standalone_activity_overhead_evidence` writes the raw evidence
to `docs/perf-artifacts/standalone-activity-overhead/`.
`standalone_activity_docs` runs in the `lint` job, so it also runs on
docs-only PRs. Among other things, it checks these:

- The perf page quotes the asserted counts.
- The ratios match the cost table.
- The cost table is the captured one.
- The ADR states the verdict that the deciding line gives, and keeps the
  first verdict on record.
- The ADR, this fragment, the guide and the performance index quote the
  cost table.
