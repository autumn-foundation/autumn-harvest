## Phase 5.x — one timeout scanner per shard, jittered and bounded (issue #1795)

Every replica ran the timeout checker on every shard every 500 ms, with no
jitter, and its task-timeout scans had no `LIMIT`. Scan load grew with fleet
size, so adding workers to clear a backlog added database load in proportion.

- **Lease.** New table `harvest_scanner_leases`, one row per
  `(shard_id, scanner)` (migration `20261001191830_harvest_scanner_leases`).
  Each tick takes or renews the `timeout` lease with one atomic upsert on the
  database clock, under a 1 s `lock_timeout`. The upsert reads the clock
  after its row-lock wait. Only the holder runs the pass.
  A standby still refreshes its active codec key, because codec key
  retirement counts on every process to do that once per tick. A graceful
  stop expires the lease at once. After a crash, a standby takes over within
  the TTL plus one tick. The TTL defaults to 10 s, is capped at 300 s, and is
  raised to at least three times the longest sleep. A lease query error fails
  open: the replica runs the pass, as before.
- **Abdication.** A holder whose pass fails three times in a row gives up the
  lease and stands by for one TTL. A pass can fail on one replica alone, for
  example on a codec only that replica lacks. A row that fails to enforce is
  tried again first in the next batch, so its failures run in a row. It
  shares the batch limit, and it gets at most three passes in a row. Then it
  waits for the next sweep, and the other rows drain. A batch that fails to
  load stays queued. A tick in any other role, or without a connection,
  ends the run. A replica back from standby, or back from a tick without a
  connection, starts a new sweep.
- **Not a fence.** Two holders for a short time are safe, because every
  sub-pass is already safe with concurrent runners.
- **Bounded scans.** The checker enforces at most one batch per timeout
  reason per pass (default 500). A sweep walks the live task rows in
  creation order, one page of up to 64 batches per refill, through the new
  partial index `idx_harvest_tq_live_created` (migration
  `20261002110913_harvest_task_queue_live_created_index`). Each refill queues the
  expired rows of its page, and each pass loads one batch by primary key and
  checks it again. So the work of a pass does not grow with the backlog, and
  one sweep reads each live row once. The cursor wraps at the end, so a row
  that stays expired cannot starve the rest. A sweep reads only the rows
  created before it started, and tests expiry against the database clock at
  its start. The index scan applies the creation bound, so a refill never
  reads newer rows, and they cannot stretch a sweep or displace older rows.
  Each predicate reads the page, not the table, so no predicate index can
  scan past it.
  A row that matches two reasons gets the first one, as in the full scan,
  if that reason's sweep can still claim it: the row is ahead of its cursor
  or in its queue. Otherwise the later reason takes it. A lane keeps its
  clock until its last queue drains. A queued row keeps its reason when an
  earlier one starts to match. If its reason stops matching, it moves to the
  first other reason that still matches, within that reason's limit. The
  lanes give up their batch ids only after every load of the pass succeeds. The four
  predicate consts are unchanged, so the backup drill's `UNION` still works.
  The public `enforce_timeouts_once` keeps its full scan.
- **Jitter.** By default, each sleep is the interval times a factor in
  `[0.8, 1.2]`. The mean is unchanged, so default enforcement latency does
  not change. The liveness check judges the loop against its longest sleep,
  not its mean.
- **Settings.** `WorkerConfig::with_scanner_config(ScannerConfig { elect,
  lease_ttl, jitter, timeout_interval, timeout_batch_size })`, reported by
  `GET /admin/config`. `timeout_interval: None` keeps the poll-interval
  cadence. The interval is at least 10 ms. A leased checker caps it at 4 h,
  so the lease always covers three of the longest sleeps. The batch size is kept within 1 and
  100,000, so a refill never reads more than 100,000 rows. The worker uses its
  `worker_id` as the holder id.
- **Metric.** `harvest.scanner.pass{scanner, shard, role}` with `role` one of
  `leader`, `standby`, `unelected`, `fail_open`. The metrics-rs bridge emits
  it, `docs/telemetry.md` lists it, and the starter dashboard has a "Scanner
  passes by role" panel.
- **Public API.** `timeout::spawn_coordinated_timeout_checker_for_shard`,
  `timeout::find_timed_out_tasks_batch`, `timeout::TimeoutScanCursor`, and the
  `scanner_lease` module. The coordinated helper takes a `pool_shard`: pass
  the shard whose own pool you give it, so a pass reuses its connection for
  that shard and a one-connection pool cannot wedge. The older spawn helpers keep running unelected with
  a fixed cadence. They now use the bounded scan.
- **Preflight.** `harvest_scanner_leases` joins the write-privilege probe.
- **Scope.** Only the timeout checker uses the lease. The other per-shard
  loops still run on every replica. `docs/runbooks/ha-deployment.md` lists
  them, and `docs/upgrading/0.5.0.md` §3.9 lists the behavior changes.

No new `WorkflowEvent` variant. No `harvest_events` mutation. No replay impact.

Tests run in `scanner_lease_tests` against Postgres 16:

- Three checkers on one shard run about one checker's worth of passes, and
  the leader never loses the lease (RED: 60 passes over 20 ticks).
- A batch of 3 reads a 7-row backlog 3 rows per pass. One sweep reads every
  row once, in id order, and a short page wraps the cursor. Failing each
  batch drains the backlog in 3 passes (RED: 7 rows in one pass).
- Rows that expire after a sweep starts, with lower ids, do not displace the
  rows the sweep counted (RED: the highest counted row is skipped).
- A refill reads one page of live rows: with 200 live rows created before
  it, an expired row is found on the fourth pass at a batch of 1 (RED: the
  first pass, because each refill scanned every live row).
- A spawned checker with a batch of 3 enforces exactly 3 of 7 rows in one
  pass.
- A row whose history cannot be decoded is retried on the next passes. The
  20 other rows still drain, and the leader gives up its lease (RED: the row
  failed once per sweep, so the leader never gave up).
- An aborted holder keeps its lease until the TTL, then a standby takes over
  with a higher epoch. A renewal keeps the epoch. A graceful stop expires the
  lease, and a standby leads on its next tick (RED: no lease taken).
- A failed lease query fails open, and a standby picks up a new active codec
  key.
- A renewal that waits 400 ms for the row lock on a 100 ms lease returns a
  live lease (RED: the lease had expired).
- A queued row that an earlier reason starts to match is still handed out
  (RED: the batch load dropped it).
- A batch whose load fails stays queued, and the next pass hands it out
  (RED: the pass skipped it).
- A row that misses its heartbeat after the heartbeat lane passed it goes
  to the start-to-close lane's new sweep (RED: it waited for the next
  heartbeat sweep). The same holds while the heartbeat lane drains the
  queue of its last page (RED: it waited for the drain).
- A row queued for its heartbeat that heartbeats again before its batch
  loads goes to start-to-close (RED: it was dropped). Moved rows count
  against the limit of their new reason (RED: 4 start-to-close rows in one
  pass at a limit of 2).
- A row that gets a missed heartbeat timeout after the heartbeat lane read
  it goes to start-to-close (RED: it waited for the next heartbeat sweep).

Unit tests cover jitter bounds and clamping, the TTL floor and caps, the role
table, the lease SQL shape, and the batched query shape.
