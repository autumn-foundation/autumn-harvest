## Phase 5.x — one timeout scanner per shard, jittered and bounded (issue #1795)

Every replica ran the timeout checker on every shard every 500 ms, with no
jitter, and its task-timeout scans had no `LIMIT`. Scan load grew with fleet
size, so adding workers to clear a backlog added database load in proportion.

- **Lease.** New table `harvest_scanner_leases`, one row per
  `(shard_id, scanner)` (migration `20261001191830_harvest_scanner_leases`).
  Each tick takes or renews the `timeout` lease with one atomic upsert on the
  database clock. Only the holder runs the pass. A standby still refreshes
  its active codec key, because codec key retirement counts on every process
  to do that once per tick. A graceful stop expires the lease at once. After
  a crash, a standby takes over within the TTL (default 10 s, raised to at
  least three of the longest sleeps). A lease query error fails open: the
  replica runs the pass, as before.
- **Not a fence.** Two holders for a short time are safe, because every
  resident of the pass already tolerates concurrent runners.
- **Bounded scans.** The checker reads at most one batch per timeout reason
  per pass (default 500). A keyset cursor walks the backlog in `id` order and
  wraps at the end, so a row that stays expired cannot starve the rest. The
  four predicate consts are unchanged, so the backup drill's `UNION` still
  works. The public `enforce_timeouts_once` keeps its full scan.
- **Jitter.** Each sleep is the interval times a factor in `[0.8, 1.2]`. The
  mean is unchanged, so default enforcement latency does not change.
  Liveness registers the longest sleep.
- **Settings.** `WorkerConfig::with_scanner_config(ScannerConfig { elect,
  lease_ttl, jitter, timeout_interval, timeout_batch_size })`, reported by
  `GET /admin/config`. The worker uses its `worker_id` as the holder id.
- **Metric.** `harvest.scanner.pass{scanner, shard, role}` with `role` one of
  `leader`, `standby`, `unelected`, `fail_open`. The metrics-rs bridge emits
  it, `docs/telemetry.md` lists it, and the starter dashboard has a "Scanner
  passes by role" panel.
- **Public API.** `timeout::spawn_coordinated_timeout_checker_for_shard`,
  `timeout::find_timed_out_tasks_batch`, `timeout::TimeoutScanCursor`, and the
  `scanner_lease` module. The older spawn helpers keep running unelected with
  a fixed cadence. They now use the bounded scan.
- **Preflight.** `harvest_scanner_leases` joins the write-privilege probe.
- **Scope.** Only the timeout checker uses the lease. The other per-shard
  loops still run on every replica. `docs/runbooks/ha-deployment.md` lists
  them.

No new `WorkflowEvent` variant. No `harvest_events` mutation. No replay impact.

Tests (`scanner_lease_tests`, against Postgres 16): three checkers on one
shard run about one checker's worth of passes (RED: 60 passes over 20 ticks);
a 7-row backlog with a batch of 3 is read 3 rows per pass, every row once per
cycle, and drains in 3 passes (RED: 7 rows in one pass); aborting the holder
hands the lease to a standby within the TTL with a higher epoch, and a
graceful stop hands it over at once (RED: no lease taken). Unit tests cover
jitter bounds and clamping, the TTL floor, the role table, and the SQL shape.
