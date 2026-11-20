## External outbox pending table (issue #2724)

An idle external-outbox claim no longer reads the request history. Before,
each claim walked every request the deployment ever made: 942,191 buffers
at 200,000 resolved requests and no pending request. After a vacuum of the
new table, it reads 0.

Migrations `20261119232545` and `20261120004046` add
`harvest_external_outbox_pending`, one row
for each unresolved signal, cancel or await request. An `AFTER INSERT`
trigger on `harvest_events` adds the row with the request. It deletes the
row with the resolution, in the same transaction, on every insert path. A
request whose run already holds its resolution adds no row, so the insert
order is unimportant. The migration backfills each unresolved request.

The three claims read the table and keep each legacy predicate, so they
select the same rows in the same order. The event lookup is a `LATERAL`
probe by key, so no estimate can turn it into a walk of the history.
`idx_harvest_events_external_outbox_pending` stays for a rolling deploy.

Invariants: no `WorkflowEvent` variant and no write to `harvest_events`. The
table is not an append-only exception. Each partition layout change
reinstalls the trigger, and `operator_triggers` exempts it. The startup
preflight checks `SELECT`, `INSERT` and `DELETE` on the table.

Evidence: `external_outbox_pending_tests` gates the trigger, the backfill,
each layout change and a `DEFAULT` partition drain. It also bounds the idle
claim, and a property test compares the table with the legacy set. Write
cost: about 490 bytes of WAL and about 40 µs for each request. See
`docs/performance-external-outbox-idle.md` and `docs/design/DESIGN-2724.md`.
