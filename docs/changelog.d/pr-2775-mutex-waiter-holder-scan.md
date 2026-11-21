## Ledger: the mutex terminal sweep finds the waits of a run by index (PR #2775)

`mutex::sweep_terminal_holder_and_wake` runs at each workflow terminal
transition. Its key read filtered `harvest_mutex_waiters` on
`waiter_exec_id` alone. No index led with that column, so each run end
scanned the whole waiter heap, also for a run that never took a mutex.

- **Migration `20261121172305_harvest_mutex_waiters_waiter_index`.** One
  index, `idx_harvest_mutex_waiters_waiter`, on `(waiter_exec_id)`. Guarded
  build: an operator can prebuild it `CONCURRENTLY`. The plain build takes
  `SHARE` on `harvest_mutex_waiters`, 51 ms at 100,000 rows. No query text
  change, no `WorkflowEvent` variant, no replay impact.
- **Measurement.** 200 runs on a fixture of other runs' mutex rows. The key
  read takes 22 → 6, 189 → 5 and 1,872 → 6 buffers per run end at 1,000,
  10,000 and 100,000 waiter rows. Workload buffers drop 1.2%, 10.8% and
  55.7%. Each queued waiter insert writes about 74 more WAL bytes.
  `docs/performance-mutex-waiter-holder-scan.md` has the full record.
- **Harness.** `cargo xtask perf mutex-waiter-holder-scan` runs
  `mutex_waiter_holder_scan_perf::zz_capture_mutex_waiter_holder_scan_evidence`.
  It checks that a sample sweep leaves the same fixture rows with and
  without the index. `terminal_key_read_probes_the_waiter_index` always
  runs and fails on a seq scan of the waiter heap.
