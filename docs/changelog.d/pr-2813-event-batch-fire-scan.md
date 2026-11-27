## Ledger: the unsharded event-batch scanner reads its next due row by index (PR #2813)

`event_batch::fire_due_on_conn` fires one due batch per transaction. Its
unsharded read filtered on `fire_at <= $1` and ordered on `fire_at`. The
only scanner index was `(shard_id, fire_at)`, so each fire read and sorted
every due row. A tick that fired `D` rows read about `D * D / 2` rows.

- **Migration `20261127173207_harvest_event_batches_fire_at_index`.** One
  index, `idx_harvest_event_batches_fire_at`, on `(fire_at)`. Guarded
  build: an operator can prebuild it `CONCURRENTLY`. The plain build takes
  `SHARE` on `harvest_event_batches`, 74 ms at 100,000 rows. No query text
  change, no `WorkflowEvent` variant, no replay impact. The sharded read
  keeps the `(shard_id, fire_at)` index.
- **Measurement.** One tick and 200 admissions. The due read takes
  28.2 → 5.8, 220.1 → 5.9 and 1,628.0 → 11.4 buffers per fire at 1,000,
  10,000 and 100,000 open batches. Workload buffers drop 3.0%, 72.5% and
  94.7%. Each admission writes about 84 more WAL bytes.
  `docs/performance-event-batch-fire-scan.md` has the full record.
- **Harness.** `cargo xtask perf event-batch-fire-scan` runs
  `event_batch_fire_scan_perf::zz_capture_event_batch_fire_scan_evidence`.
  It checks that both arms start the same runs in the same order and leave
  the same rows. `event_batch_fire_index_does_not_change_what_fires` always
  runs and covers shard ids, `fire_at` ties and rows not yet due.
