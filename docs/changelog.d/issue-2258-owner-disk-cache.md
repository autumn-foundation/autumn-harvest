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
- **Disposable.** The worker deletes every cache file at start. A lost
  tenure deletes the file of its partition. A stopped worker deletes the
  files that it wrote. A terminal commit and an evict wake purge the run.
  An erasure sends the owner an evict wake. The owner scan runs a reconcile
  pass at most once in 30 s per shard. It purges each cached run that is
  not `RUNNING`, and each file of an older codec key generation.
- **Long histories only.** `WorkerConfig::with_owner_disk_cache_min_events`
  sets the fewest events that a run needs before it goes to disk. The
  default is 100. A shorter history loads faster from Postgres.
- **Validity stamps.** Each file holds the owner epoch and the codec key
  generation. Each run holds its state and the hash of its last event. A
  mismatch or a damaged row purges the entry, and the decision replays
  cold from Postgres.
- **`harvest.workflow.disk_cache{workflow, queue, outcome}`.** A new counter
  of disk reads: `hit`, `miss`, `stale` or `corrupt`. `MetricsRecorder`
  gains `record_workflow_disk_cache` with a no-op default. The starter pack
  gains a panel.
- **`examples/history-load-gate`.** The warm-up step also times the disk
  cache. From about 100 events, the next pass after a restart is 1.5 to
  1.9 times faster than a Postgres load. Below that, it is not faster. See
  `docs/rnd/2026-10-23-owner-disk-cache.md`.

**Tests.** `an_erased_run_leaves_no_local_bytes`,
`an_erasure_purges_a_run_that_a_missed_purge_left`,
`a_killed_owner_leaves_no_readable_bytes`,
`a_retired_codec_key_leaves_no_ciphertext`,
`a_rotated_codec_key_leaves_no_ciphertext_on_disk` and
`a_corrupt_file_replays_cold` cover the four red tests of the issue.

No migration. No new `WorkflowEvent` variant. Off by default.
