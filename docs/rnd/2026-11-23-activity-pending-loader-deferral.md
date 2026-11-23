# 🏛️ Keystone [deferred]: architecture review of the activity pending-check loader, issue #2784 (trigger: a long-run e2e share ≥20%, or execution-row lock waits in tier 1)

**Status:** deferral record. No RFC. The change is a two-way door for the
implementing team. **Date:** 2026-11-23, on `trunk-dev` `a3e5ed1`.

## Decision left open

Issue #2784 asks for a human decision. Can the start and completion paths
answer "is activity X pending" from stored event columns instead of a full
decoded history? Today `pending_activity_id_for_task` (`worker.rs`, and its
copy in `timeout.rs`) scans the whole history under the execution row lock.

## The "may we" part is already decided

- ADR 0003 encodes only the payload fields (`PAYLOAD_FIELD_KEYS`). Ids stay
  in clear.
- Migration `20260905181020` indexes `event_data #>> '{data,activity_id}'`
  for `ActivityStarted`. The usage report reads it.
- Migration `20260911213344` answers "is signal, cancel or await X resolved"
  with three partial expression indexes on JSON ids.

This record adds the fitness function that these reads lacked:
`a_codec_leaves_every_non_payload_key_in_clear` in `payload_codec.rs`.

## Evidence

Tier 1: none (no production). Tier 2: issue #2784 measures the full reads at
2.8% of buffers and 3.5% of calls on the 3-activity bench. Tier 3: a spike on
the #2784 fixture (PG 16.15, 722,740 events). Shared buffers per call:

| run size | full read | narrow probe, no new index | narrow probe, candidate index | `max(event_id)` |
|--:|--:|--:|--:|--:|
| 10 activities | 37 | 34 | 5 | 4 |
| 50 activities | 158 | 155 | 5 | 4 |
| 200 activities | 609 | 606 | 5 | 4 |

- **Without a new index, the narrow SQL saves 0.5% of buffers.** The heap
  is interleaved, so the filter still visits every page of the run. A SQL
  rewrite alone moves the decode cost, not the I/O.
- The candidate is a partial index on `(workflow_exec_id, activity_id)` over
  the scheduled and terminal types. It is 22 MB on this fixture. Its own WAL
  is 0.96 to 1.38 MB per 10,000 appended events, 1.3% to 1.9% of that WAL.
  The existing `ActivityStarted` lookup index writes 0.59 to 0.85 MB.
- The default history policy (`context.rs`) allows 10,000 events before the
  continue-as-new advice, and 50,000 at the hard cap. At those sizes one
  full read is about 10,100 and 50,500 buffers. That is derived, not run.

## Do nothing for 12 months

Nothing breaks before launch. No latency budget for a long run is committed.
Long runs keep paying about 3n² rows over a run of n activities. The bench
does not see it. Lock hold time on the execution row grows with the history.
That effect is a hypothesis with no sample.

## Door class

Two-way. The change is two call sites, one query and one partial index.
Reversal restores the decoded scan and drops the index `CONCURRENTLY`. That
costs under one engineer-week. It does not clear the impact floor: 2.8% at
tier 2, and no committed requirement fails at tier 3.

## Default path

The implementing team decides it in a PR description. That PR needs:

1. The partial index. Follow `migration_lock_safety` and the partitioned
   recipe of `20260905181020`.
2. One shared check for `worker.rs` and `timeout.rs`. A task row with no
   `activity_id` keeps the name-based full read.
3. A Ledger profile for the new query, and a long-run e2e baseline.

DESIGN-2257 B12 (skip the reload after an empty wake ingest) needs no index.
It can ship first.

## Seam kept open

`data.activity_id` stays in clear under every codec. The new test guards it.

## Trigger to revisit as an RFC

- An e2e run at 50 or 200 activities puts these reads at ≥20% of workload
  buffers, or
- tier 1 shows waits on the `harvest_workflow_executions` row lock, or
- a dated requirement commits a latency budget for runs past 1,000 events.

## Reproduce

`docs/perf-artifacts/activity-pending-probe/`: `fixture.sql` (from #2784),
`candidate_index.sql`, `probe.py` (buffers per call), and `wal.sql` with
`wal_by_relation.sh` (index WAL via `pg_waldump --relation`). The raw
outputs sit beside them. `wal-runs.txt` holds whole-range WAL totals. Their
drift between runs is larger than the index effect, so this record does not
use them.
