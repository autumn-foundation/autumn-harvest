## Feature — Owner-local disk cache, encrypted and disposable (issue #2258)

Stage 2 of epic #2247. A decision owner can keep the history of each live
run in a local SQLite file. A memory miss then reads the file and the
Postgres delta, not the full Postgres history. See
`docs/adr/0012-owner-disk-cache.md`.

- **Opt-in.** Build with the `owner-disk-cache` feature and set
  `WorkerConfig::with_owner_disk_cache(dir)`. The cache acts only in owner
  mode, on the partitions that the worker owns. A directory set on a build
  without the feature fails the worker start.
- **Encrypted.** AES-256-GCM seals every row under a node key that lives
  in memory only. The cache stores decoded events, so no codec ciphertext
  reaches the disk.
- **Disposable.** The worker deletes every cache file at start, on a lost
  tenure and at shutdown. A terminal commit and an evict wake purge the
  run. A reconcile pass every 30 s purges each cached run that is no
  longer `RUNNING`.
- **Validity stamps.** Each file holds the owner epoch and the codec key
  generation. Each run holds its state and the hash of its last event. A
  mismatch or a damaged row purges the entry, and the decision replays
  cold from Postgres.
- **`harvest.workflow.disk_cache{workflow, queue, outcome}`.** A new counter
  of disk reads: `hit`, `miss`, `stale` or `corrupt`. `MetricsRecorder`
  gains `record_workflow_disk_cache` with a no-op default. The starter pack
  gains a panel.
- **`store::load_event_inflated`.** Reads one decoded event. The cache
  checks its last event with it.
- **`examples/history-load-gate`.** The warm-up step also times the disk
  cache.

No migration. No new `WorkflowEvent` variant. Off by default.
