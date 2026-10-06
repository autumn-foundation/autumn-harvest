# ADR 0004: Detect, do not prevent, a duplicate append across cohorts

## Status

Accepted (issue #1839).

## Context

On the partitioned `harvest_events` layout, the unique constraint is
`(workflow_exec_id, event_id, cohort)`. It covers one cohort only. The insert
trigger rejects a duplicate in any cohort, but it sees committed rows only.

Two appends of one `event_id` can therefore both commit. All of these must be
true:

- Two workers append for one execution at the same time. This is a
  split-brain, for example a stale worker whose task was reclaimed.
- Neither append is committed when the other runs its check.
- A cohort boundary falls between the two insert instants.

The window is a few microseconds per cohort. The migration
`20260901115500_harvest_event_partitioning` and
[`partitioned-events.md`](../partitioned-events.md) record it as a known limit.

Issue #1839 asks for one of two choices: fix it, or detect it.

## Options

1. **Lock every append.** Take an advisory lock on the execution id for each
   append. This serializes the append hot path. The admission and mutex paths
   already take advisory locks, so a new lock order can deadlock.
2. **Lock near a boundary only.** Take the lock in a short window around each
   cohort boundary. The window comes from the clock, and clock skew between
   hosts makes it unsafe. It also keeps the deadlock risk of option 1.
3. **Detect.** `harvest backup verify` finds duplicate pairs. The hot path
   does not change.

## Decision

**Detection only (option 3).**

`harvest backup verify` gets the finding class `duplicate_event_id`, with
severity `incoherent`. The probe runs on the partitioned layout only. On the
flat layout the unique constraint makes a duplicate impossible. When the
layout is unknown, the probe does not run and the shard is `undetermined`.

## Consequences

- The append hot path takes no new lock and has no new deadlock risk.
- The window stays open. A duplicate is found at the next restore drill or
  pre-flight check, not at write time.
- A found duplicate fails the drill. The operator must repair the history by
  hand before workers start.
- The probe groups all rows of `harvest_events`. It runs in a read-only
  session, usually on a restored copy or a standby, not on the primary.
- A future fix can still add a lock. This ADR then changes to "superseded".
