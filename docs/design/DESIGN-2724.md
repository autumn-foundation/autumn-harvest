# DESIGN-2724: a pending table for the external outbox

Issue #2724. An idle external-outbox claim reads every request that the
deployment ever made. At 200,000 resolved requests and no pending request,
one claim reads 942,191 buffers. The lease holder of each shard runs three
such claims about twice a second.

## Facts

- `harvest_events` is append-only. A request row stays in
  `idx_harvest_events_external_outbox_pending` after it resolves.
- The index predicate tests `event_type` only. It cannot express
  "resolved".
- Each claim walks the whole index and probes the resolution index once for
  each row. The cost is O(H) for H lifetime requests, also with no backlog.
- Retention reaps the requests of terminal runs only. A long-lived run keeps
  its requests.
- The SQLite backend has no outbox. The change is Postgres only.

## Brainstorm

1. A pending table, written by the code that appends the request and the
   resolution.
2. A pending table, written by an `AFTER INSERT` trigger on
   `harvest_events`. `harvest_entity_state` uses the same pattern.
3. A persisted low-water mark `(timestamp, id)` for each shard and family.
4. A resolved flag on the request row.
5. A longer `scanner.timeout_interval`.
6. A partial index on a resolved flag kept in another table.
7. A count precheck: requests against resolutions.

## Reverse brainstorm: how would the change fail?

| Failure | Guard |
|---|---|
| An insert path forgets the pending row, so a request is never delivered. | A trigger writes the row (option 2). No code path can skip it. |
| A resolution arrives in the same statement before its request. The row stays, and the claim delivers twice. | The trigger fires after the statement. A request row checks for a resolution before it inserts. |
| A partition move re-inserts old rows. Each resolved request comes back. | The same resolution check. `ON CONFLICT DO NOTHING` absorbs a pending request. |
| A layout change rebuilds `harvest_events` with `LIKE`. The trigger is lost. | Each layout path reinstalls it. `operator_triggers` exempts it. |
| A rolling deploy runs the old claim on the new schema. | The old index stays. The old claim keeps its plan. A later release can drop the index. |
| A stale pending row makes the claim deliver a resolved request. | The claim keeps the legacy `NOT EXISTS` check. The table is a candidate set, not an authority. |
| A pending row outlives its run. | A foreign key cascades the delete. A shard move deletes the rows of a staged copy. |
| A moved row has no run (partitioned layout). The foreign key fails the move. | The trigger skips a row with no run, as the entity projection does. |
| The backfill misses a request that commits during the migration. | `CREATE TRIGGER` takes `SHARE ROW EXCLUSIVE`. No insert runs until the commit, so the backfill sees each committed request. |
| The migration deadlocks a transaction that appends an event and then updates its run. | The foreign key and the trigger go in two migrations, so no transaction locks both tables. |
| A drain inserts a row for a run that a concurrent delete removes. The foreign key fails the drain. | The trigger probes the run `FOR KEY SHARE`, so it waits for the delete and then skips the row. |
| The worker role cannot write the new table. Every request insert fails. | The upgrade guide lists the grants, as for `harvest_entity_state`. |
| Concurrent resolutions deadlock on the pending row. | The claim locks the event row only, as before. A resolution deletes the pending row last, after its other locks. |
| The claim locks a row that it then discards. | The checks run in an unlocked probe. The lock stays at the top, on the one row that the claim returns. |

## Six thinking hats

- **White (facts).** 942,191 buffers for each idle claim at H = 200k. The
  completion trigger outbox already uses a table. The entity projection
  already uses a trigger on `harvest_events`.
- **Red (feelings).** A trigger on the largest table feels risky. The
  `WHEN` clause limits it to nine rare event types.
- **Black (risks).** Lock time of the backfill. The grant for worker roles.
  Partition paths. Write cost on each request. The reverse brainstorm
  covers each one.
- **Yellow (benefits).** The idle claim becomes O(1). A drain becomes O(B).
  No clock argument. No write to `harvest_events`.
- **Green (alternatives).** Option 3 needs a full rescan as a safety net,
  so H comes back. Option 4 adds a third `event_data` writer. Option 5 only
  divides the cost. Option 7 is not exact. Option 1 needs each insert path
  to change, and a new path can forget it.
- **Blue (process).** Choose option 2. Write the tests first. Keep the
  legacy check in the claim. Measure before and after with the fixture of
  the issue.

## Decision

Option 2, the remedy that the issue recommends, written by a trigger:

- Table `harvest_external_outbox_pending`: one row for each unresolved
  request, keyed by `harvest_events.id`.
- Function `harvest_external_outbox_project` and trigger
  `harvest_external_outbox_projection_trg`. The trigger fires `AFTER
  INSERT` for the three request types and the six resolution types only.
- A request inserts a row, unless a resolution with the same key is already
  in the run.
- A resolution deletes each row of the run with the same type and key.
- The claim reads the table in `(requested_at, event_row_id)` order. It
  keeps every legacy predicate, so it selects a subset of the legacy rows.
  The table holds each unresolved request, so the subset is the whole set.
- Two migrations. The first adds the table, its foreign key and the
  function. The second installs the trigger and backfills each unresolved
  request in one transaction. So no transaction locks both hot tables.
- A stranded request keeps its row until retention. A redrive can reopen a
  `FAILED` run, so a row cannot go when its run ends.
- `idx_harvest_events_external_outbox_pending` stays for a rolling deploy.

## Acceptance criteria

1. An idle claim reads a constant number of buffers. The count does not
   grow with H.
2. The pending row commits with its request. The row goes in the same
   transaction as the resolution.
3. The migration backfills each pending request, and only those.
4. All three families use the table.
5. The claim selects the same rows as the legacy query, in the same order.
6. No code writes to `harvest_events`. The append-only invariant holds.
7. Each partition layout path keeps the trigger.
8. A pending row goes with its run: retention and a shard move.
9. The write cost of each request and resolution is measured.
10. The issue fixture is measured again at H = 10k, 50k and 200k.
