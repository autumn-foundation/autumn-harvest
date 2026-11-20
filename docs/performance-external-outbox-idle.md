# An idle external-outbox claim no longer reads the request history (issue #2724)

The three external-outbox claims read a pending table now. An idle claim
read every request that the deployment ever made. After a vacuum of the
table, it reads 0 buffers for each call, whatever the history holds.

## 🎯 Workload

`enforce_timeouts_once` runs `enforce_external_signals_outbox`,
`enforce_external_cancels_outbox` and `enforce_external_awaits_outbox` on the
timeout checker of each shard. The lease holder ticks at
`scanner.timeout_interval`, 500 ms by default. Most ticks find no pending
request.

The fixture is the one from the issue. The scripts are in
[`perf-artifacts/external-outbox-idle/`](perf-artifacts/external-outbox-idle/):

- `seed.sql`: 20,000 RUNNING runs with 50 events each, and 100,000
  COMPLETED runs with 20 events each.
- `history.sql`: H `ExternalSignalRequested` rows, each with its
  `ExternalSignalDelivered` marker. 80% are on terminal runs.
- No pending request.

Postgres 16.15. `sweep-after.txt` holds the per-size output. Each figure is `pg_stat_statements` shared buffers over 10
calls of the generic plan, as the scanner keeps the statement prepared.

## 📈 Profile (before)

`claim-before.sql` is the claim as it shipped. Its outer scan walked
`idx_harvest_events_external_outbox_pending` and ran one resolution probe
for each request. The plan at H = 200k is in `before-h200k.txt`:

```
Index Scan using idx_harvest_events_external_outbox_pending on harvest_events e
     (actual rows=200000 loops=1)
Index Scan using idx_harvest_events_external_signal_resolved on harvest_events res
     (actual rows=1 loops=200000)
```

The legacy figures match the issue to the buffer.

## 🔧 Change

Migrations `20261119232545_harvest_external_outbox_pending` and
`20261120004046_harvest_external_outbox_projection` add
`harvest_external_outbox_pending`, one row for each unresolved request. The
trigger `harvest_external_outbox_projection_trg` keeps it on `INSERT` into
`harvest_events`:

- A request adds a row, unless its run already holds its resolution.
- A resolution deletes each row of its run with the same type and key.

The claim reads the table in `(requested_at, event_row_id)` order. A
`LATERAL` probe checks each event by its id with each legacy predicate, after
the executions probe. The event row then joins by that id alone, and the
lock stays at the top: it falls only on the row that the claim returns. See
`external_outbox_claim_query!` in `timeout.rs`.

## 📊 Measurement

Buffers for each idle claim:

| resolved requests (H) | before | after, steady state | after, churn not yet vacuumed |
|--:|--:|--:|--:|
| 10,000 | 34,387 | 0 | 90 |
| 50,000 | 222,042 | 0 | 361 |
| 200,000 | 942,191 | 0 | 1,394 |

**Steady state** is the table after a vacuum pass. The table is empty, so
the plan is a scan and a sort of no rows, and a call reads no buffer. The
first `EXPLAIN` of a session reports 6 buffers. Once a backlog grows, the
plan reads `idx_harvest_external_outbox_pending_claim` in order instead.

**Churn not yet vacuumed** is the worst case. `history.sql` loads the whole
history in two statements with no vacuum between them. So each request adds
a row, and its resolution deletes it. The claim then reads the pages of the
dead entries: about one page for each 140 entries. The first claim
also visits each dead heap tuple once and marks its entry dead. That claim
read 4,323, 17,369 and 90,017 buffers at the three sizes. Later claims read
only the leaf pages.

In a live deployment, autovacuum removes the dead entries. The migration
sets a fixed threshold of 1,000 dead rows on the table, so a pass starts
after 1,000 resolutions, whatever the table size. That keeps the read under
ten pages.

One pending request at H = 200k (`one-pending-h200k.txt`): **13 buffers**
for each claim. The legacy claim reads the whole history first, so it costs
at least 942,191.

## 💸 Write cost

`wal.sql` inserts 10,000 requests, then 10,000 resolutions, with a
`CHECKPOINT` before each step. Three runs on each schema, in `wal-runs.txt`:

| step | WAL bytes per row, before | after | time per 10,000 rows, before | after |
|:--|--:|--:|--:|--:|
| request | 1,007 | 1,493 | 188 to 215 ms | 559 to 605 ms |
| resolution | 1,123 | 1,271 | 191 to 244 ms | 304 to 323 ms |

So a request costs about 490 more bytes of WAL and about 40 µs. A
resolution costs about 150 bytes and 10 µs. The request adds one heap row
and three index entries, and the trigger runs one resolution probe. The
other event types do not call the function: the `WHEN` clause filters them
in the executor.

## 🗄️ Migration cost

The backfill reads each request once and probes its resolution. It took
about 1.1 to 1.4 s at H = 200k (`backfill-h200k.txt`). `CREATE TRIGGER`
holds `SHARE ROW EXCLUSIVE` on `harvest_events` until the commit, so event
appends wait that long. The table and its foreign key come in the previous
migration. So no transaction locks both hot tables, and the migration
cannot deadlock a transaction that appends an event and then updates its
run.

## ✅ Equivalence

- `external_outbox_scan_tests::outbox_claim_queries_match_the_legacy_anti_join`
  runs the pre-change SQL and the new SQL on one fixture, for each family.
  The two select the same rows.
- `external_outbox_scan_tests::outbox_claim_returns_the_oldest_pending_request_first`
  keeps the drain order.
- `external_outbox_pending_tests::the_table_matches_the_legacy_unresolved_set`
  is a property test. It inserts random requests and resolutions, in random
  statements and orders, and compares the table with the legacy predicate.
- `external_outbox_pending_tests::an_idle_claim_does_not_read_the_resolved_history`
  bounds the idle claim of each family to 16 buffers over 3,000 resolved
  requests. In the red run of this gate, before the change, the legacy
  claim read 12,035.

## 🔬 Reproduce

```bash
cd docs/perf-artifacts/external-outbox-idle
# Apply each autumn-harvest/migrations/*/up.sql in order to a fresh DB.
psql "$DB" -f seed.sql
psql "$DB" -v lo=1 -v hi=10000 -f history.sql   # then 10001..50000, 50001..200000
psql "$DB" -f claim.sql                          # the shipped claim
psql "$DB" -c "VACUUM harvest_external_outbox_pending"
psql "$DB" -f claim.sql                          # steady state
```

`claim-before.sql` runs the legacy claim. Run it on a database migrated up
to `20261114213149`. Run `wal.sql` on a fresh database at each schema.

## 🚧 Known limitations

- **A stranded request keeps its row.** A request whose run ends before
  delivery stays unresolved. Its row stays until retention removes the run.
  Each claim reads it and drops it at the executions probe: about 3 buffers.
  A review measured 15,103 buffers for each claim over 5,000 cancelled runs
  with an open await. The cost follows the number of such requests, not H.
  A row cannot go when its run ends: a redrive reopens a `FAILED` run under
  the same id, and the legacy claim then delivers its open request.
- **The cancel and await families are covered by construction.** One
  template builds the three claims, and the gates run each family. Only the
  signal family was measured on the issue fixture.
- **Measured in isolation.** This is the cost of the scanner alone, not its
  share of a mixed workload, as the issue states.
- **`idx_harvest_events_external_outbox_pending` stays.** A worker of the
  previous release claims through it during a rolling deploy. A later
  release can drop it. `performance-external-outbox-scan.md` measured its
  cost at about 103 bytes of WAL for each request.

## See also

- [`performance-external-outbox-scan.md`](performance-external-outbox-scan.md):
  the indexes and plan pins of the claim before this change.
- [`design/DESIGN-2724.md`](design/DESIGN-2724.md): the options and the
  decision.
