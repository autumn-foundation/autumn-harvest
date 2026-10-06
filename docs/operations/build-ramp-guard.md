# Build ramp guard (issue #1814)

A build ramp (issue #604) sends a percentage of new starts on a queue to a
target build. The rest go to the base build. An operator aborts a ramp with
one command: `DELETE /admin/build-routing/ramp/{queue_name}`.

The ramp guard aborts a ramp automatically. It compares the target build with
the base build during the current ramp step. When the target build is worse
by more than a threshold, the guard clears the ramp and writes an audit row.
This is the "one-box" deployment pattern: the alarm is scoped to the new
code, and the rollback needs no operator.

The guard is opt-in. A deployment with no guard config runs no guard SQL.

## Turn it on

```rust,ignore
use autumn_harvest::ramp_guard::RampGuardConfig;

let plugin = HarvestPlugin::new().ramp_guard(
    RampGuardConfig::new()
        .with_interval(Duration::from_secs(30))
        .with_min_samples(20)
        .with_max_failure_rate_increase(0.05)
        .with_max_nd_block_rate_increase(0.05),
);
```

The plugin boot and the embedding boot spawn the guard loop in the API
process, beside the load-shed sampler. `HarvestBuilder::ramp_guard` stores the
same config, but a bare builder spawns nothing. Without the plugin, run
`ramp_guard::run_ramp_guard` yourself, or drive `ramp_guard::RampGuard::pass`
from your own loop.

| Setting | Default | Range | Meaning |
|---------|---------|-------|---------|
| `interval` | 30 s | 1 s to 1 h | Time between two guard passes. |
| `min_samples` | 20 | 1 or more | Runs of each build needed for a verdict. |
| `max_failure_rate_increase` | 0.05 | 0 to 1 | Allowed failure-rate increase over the base build. |
| `max_nd_block_rate_increase` | 0.05 | 0 to 1 | Allowed ND-block-rate increase over the base build. |
| `report_grace` | 10 min | 0 to 24 h | Age after which a pass reports an abort that a stopped guard did not report. |

The setters clamp a value outside its range. A `NaN` rate keeps the value
that the config had before. A threshold of 1 turns its check off, because no
rate can be more than 1 above another rate.

## Signal

The signal comes from `harvest_workflow_executions`, not from a metrics
backend. The table is fleet-wide and durable, so every replica sees the same
data. One query per shard pool groups the runs of the ramp by
`assigned_build_id`. It counts only runs that meet all of these conditions:

- The run is on the ramp's queue.
- The run started at or after the ramp step. The step is the policy row's
  `updated_at`, which `set_build_ramp` sets.
- The run is not a canary probe.

For each build the query returns these counts:

| Count | Definition |
|-------|------------|
| `started` | All counted runs. |
| `completed` | `state = 'COMPLETED'`. |
| `failed` | `state IN ('FAILED', 'TIMED_OUT')`. |
| `nd_blocked` | `state IN ('RUNNING', 'PAUSED')` and `nd_blocked_at IS NOT NULL`. |

Cancelled and terminated runs are operator actions, so they do not count as
failures. An operator can pause a blocked run, so a paused blocked run still
counts as blocked.

The two rates are:

- failure rate = `failed / (completed + failed)`
- ND-block rate = `nd_blocked / started`

A rate with a zero denominator is 0.

The guard adds up the counts of every pool that holds the same ramp
generation. A generation is the queue, both builds and the `ramp_id`. A
partial fan-out can leave two generations with the same builds on different
pools. The guard judges each generation on its own counts, so the failures of
an old ramp cannot abort a new one.

The migration `20261003212318_harvest_ramp_guard_outcome_index` adds the
index `idx_harvest_we_ramp_guard_outcome` on
`(queue_name, assigned_build_id, created_at)`. The query reads only the runs
of the current step. Each read runs with a server-side `statement_timeout`, so
a slow scan stops on the server too.

### Limits of the signal

- A run counts against the build it was **assigned** at start. A target
  worker can replay a base run through `harvest_build_compat`. An ND block in
  that run counts against the base build. The
  `harvest.workflow.nondeterministic_block` metric labels the block with the
  worker build instead.
- A child workflow and a continue-as-new run keep the build of their parent.
  One bad parent with many children therefore adds many correlated samples.
- Any change of the policy row starts a new step. That includes a new
  percentage, a repeated `set_build_ramp` and a new base build. The evidence
  of the old step then no longer counts.

## Verdict

`ramp_guard::evaluate` is a pure function. It uses the 95 % Wilson score
lower bound of the target rate, not the point rate. With few runs the bound is
low, so a few unlucky runs cannot abort a healthy ramp.

The function checks the rates in this order:

1. The failure check needs `min_samples` settled runs on each build. Then,
   when the target lower bound is more than `max_failure_rate_increase` above
   the base failure rate, the verdict is **abort** with reason
   `failure_rate`.
2. The ND-block check needs `min_samples` started runs on each build. Then,
   when the target lower bound is more than `max_nd_block_rate_increase`
   above the base ND-block rate, the verdict is **abort** with reason
   `nd_block_rate`.
3. When neither check has enough runs, the verdict is **insufficient data**.
4. Otherwise the verdict is **healthy**.

The threshold is an increase over the base build, not an absolute rate. A
shared outage that fails both builds does not abort the ramp.

The base build needs `min_samples` runs too. At or near 100 % the base build
gets almost no new runs, so its rate is noise. The guard then gives no
verdict. Watch a full cutover with the `build_id` metrics instead. A ramp
whose target is its own base build is a promotion in progress, and the guard
skips it.

## Abort

An abort clears `target_build_id` and `ramp_percent` on every shard pool that
holds the ramp. The base build then takes all new starts. In-flight runs keep
their assigned build, as with a manual clear.

The clear is a compare-and-swap on the queue, the base build, the target
build and the step (`updated_at`). It changes a row only when the row still
holds the step that the verdict used. A verdict about an old target or an old
step therefore cannot clear a newer ramp that an operator set.

After the clear, the guard does these steps once per abort:

- It writes one row to the report ledger `harvest_ramp_abort_reports`, keyed
  by the `ramp_id`, and one audit row, in one transaction on the audit pool.
  When the ledger already holds the abort, another guard reported it, and
  this guard writes nothing.
- The audit operation is `build_routing.ramp.auto_abort`.
  The target is `build_routing` and the queue name. The actor is `system`,
  and the route is `background.ramp_guard`.
- The audit summary holds the reason, both rates, the target lower bound,
  both builds and the sample counts.
- After the audit row commits, it increments
  `harvest.build.ramp_aborted{queue, reason}` and logs a warning. The audit
  row is the durable record. The counter is process-local: a process that
  stops between the commit and the increment loses that one count, and its
  restart resets the counter anyway.
- `RampGuard::pass` and `guard_once` return only the aborts that this pass
  reported. An abort whose report failed is not returned, and a later pass
  reports it.
- It then marks the abort markers of the ramp as reported. Every clear
  writes an unreported marker, and only a committed report, or a ledger row
  from another guard, marks it. A failed audit write keeps the markers
  unreported, so a later pass reports the abort.

Many replicas can run the guard. A replica reports an abort only after it
cleared a pool itself, so a failed clear never reports a change that did not
happen. The first decisive clear, in pool order, elects the reporter. A
clear is decisive when this guard won the pool, or when another guard won
it. A failed clear, an operator change or an unknown change decides
nothing. The abort marker tells a guard clear from an operator change. The
report ledger makes the report exactly-once for a ramp with a `ramp_id`,
even when two replicas each elect themselves. So
replicas that race over the same pools agree on one reporter, on a first
attempt and on a retry alike.

## Failure behaviour

The guard fails safe: when it cannot read, it does not abort.

- A pass with any failed shard read changes nothing.
- All reads of one pass must end within one bound. The bound is the interval
  or 60 s (`MAX_READ_TIMEOUT`), whichever is less.
- Each clear and each audit write has the same bound. A clear runs with a
  server-side `lock_timeout` and `statement_timeout`, so a slow clear fails
  and rolls back on the server. It cannot commit after the guard gave up.
- A clear first takes a pool connection within the bound. A checkout that
  fails sent nothing to the server, so it is a plain failed clear.
- The client waits twice the bound for a clear. When the client still gives
  up, the outcome is unknown. A retry that then finds the row changed counts
  the change as the guard's own clear and reports it. An extra audit row is
  better than an abort with none.
- A cancel stops a pass during its read. A pass that has started to clear
  runs to its end, so a clear and its audit row are not split.
- A clear that fails on one pool stays pending. The next pass of the same
  guard retries it with no new verdict. The audit row of such an abort has
  status `failed` and names the pending pools by index.
  A pending clear blocks only its own ramp generation. A newer ramp with
  the same builds is still judged on its own counts.
- A guard that cleared a pool owns the report. When its audit write fails
  while another pool is still pending, the next pass retries the report.
  The retry does not wait for the pending clear, so a pool that keeps
  rejecting the clear does not leave the abort unaudited.
- When no clear of a pass succeeds, the guard reports nothing yet. It reports
  the abort when a retry clears a pool.
- A failed audit write logs a warning and does not undo the clear.
- A guard keeps its pending clears in memory. Each operator ramp also has a
  `ramp_id`, which the API fan-out writes to every shard pool. Each finished
  clear adds an abort marker to the list
  `harvest_build_policies.ramp_aborted` in the same `UPDATE`, so the marker
  cannot be lost. A marker holds `id` (the `ramp_id`), `base`, `target`,
  `reported` and `at`, the clear time. A newer abort on the same row keeps the older
  markers. A pass that reads every pool removes a reported marker once no
  pool holds its ramp. A marker therefore stays until its abort finishes. After a
  restart, a pool can still hold a ramp whose `ramp_id` and base build match a marker on
  another pool. The guard then clears that ramp with no new verdict and no
  new audit row. The marker also holds the base build, so it matches only a
  ramp with the same base. The
  match uses ids, not database clocks, so clock skew between pools does not
  matter. An operator ramp set after the abort has a new `ramp_id`, so the
  guard does not clear it. A ramp set before the migration has no id. The
  guard derives a report id for its abort from the queue, the builds and
  the step of each pool that holds the ramp. Every replica reads the same
  rows, so every replica derives the same id, and the ledger reports the
  abort once. Before any clear, the guard writes that id as the `ramp_id`
  of every pool that still holds the ramp. The clear writes its marker
  under the same id. So a later guard can finish a pool that did not
  clear, also after a restart, and a failed report stays recoverable. A
  guard can stop after it stamped only some pools. The stamp keeps each
  step, so a later read derives the id over the stamped and the unstamped
  pools together. When it matches the stamp, the read treats them as one
  ramp under that id, and no second id appears. A pool that rejects every
  write keeps no id, and the guard cannot finish it after a restart. A later ramp with the same builds has new steps, so an
  old marker never matches it.
- A pool can hold an abort marker and a newer operator ramp at the same time.
  The guard reads the marker anyway. It clears only the pools whose `ramp_id`
  matches a marker. The newer ramp stays, and the next pass judges it on its
  own counts.
- A trigger on `harvest_build_policies` clears `ramp_id` when an `UPDATE`
  changes the target, the percentage or `updated_at` but keeps the old
  `ramp_id`. A ramp therefore keeps an id only when an id-aware writer set
  it. This covers an API replica from before the migration during a rolling
  upgrade. Such a ramp has no id, so no old marker can clear it. The guard
  judges it as usual.
- Enable the guard only after every API replica runs this release. A
  replica from before the migration writes ramps with no id. Its fan-out
  can reach a pool after the guard aborted the same ramp on another pool.
  The guard cannot tie that late ramp to the abort: it would have to match
  on the builds alone, and that would also clear a new operator ramp with
  no verdict. So the late ramp is judged on its own counts, as before the
  guard existed. Check `GET /admin/build-routing` after the upgrade, and
  clear such a ramp with `DELETE /admin/build-routing/ramp/{queue_name}`.
- A ramp fan-out and a policy fan-out each pass one caller id to every
  pool. Each pool stores an id derived from that caller id, the queue, its
  base build and its target (`build_routing::ramp_generation_id`). The hash
  input prefixes the queue and each build id with its byte length, so a `/`
  in a name cannot make two inputs share an id. One caller id on two queues
  gives two ids, so the report ledger keeps a report for each. So pools with
  the same base and target share one identity. A partial fan-out can leave
  pools with different bases or targets. Those ramps then get different
  ids, as the guard judges them apart. An abort of one cannot finish the
  other, and the report ledger reports each abort.
- A base-build change through `set_build_policy` keeps an active ramp and
  starts a new step. It gives the ramp a fresh `ramp_id`, so no old marker
  matches it.
- A policy write never re-ids a kept ramp to a generation that the guard
  aborted. A retried policy fan-out can reach a pool that the first attempt
  missed, after the guard aborted the new generation on another pool. That
  pool then gets the new base and loses the ramp, as the abort decided. The
  write reads the row's abort markers and the local ledger with its
  tombstones.
- Each ramp write and each policy write with a ramp id is idempotent. A row
  that already holds the same write is left as is, and its step stays. So
  two logical shards on one pool cannot split the ramp identity.
- A request with an `Idempotency-Key` header gets a caller id derived from
  the route, the queue, the key and the request body. A reused key with a
  changed body gets a new caller id, so every pool rewrites the ramp under
  it. The UI set-policy form does the same with a hidden operation id. Retry a partial fan-out (`207`) with the
  same key. Every pool then stores the same `ramp_id`, and the guard judges
  one generation. A retry without a key gets a new caller id. The pools that
  the first request reached keep the old id, so the ramp splits into two
  generations. Use a new key for each new ramp.
- A keyed ramp never restores a generation that the guard aborted. A retry
  after an abort, for example after a lost response, gets `409 Conflict`
  and changes no pool. Before any write, each pool checks its abort markers
  for the generation id of its own base. The audit pool then checks the
  report ledger, which outlives the markers. The pool write runs the same
  check in its `UPDATE`, so an abort that lands between the check and the
  write is also refused. Send a new key to ramp again. The check fails
  closed: when it cannot read a pool, the request gets `503` and changes
  no pool. Retry it when the pool is back.
- A keyed ramp also never undoes an operator's change. A ramp write, a
  policy write and `DELETE /admin/build-routing/ramp/{queue_name}` record
  the `ramp_id` that they remove in `harvest_ramp_retired_ids`, on that
  pool. A late retry of the request that set that id gets `409 Conflict`,
  by the same checks as an aborted id. The retired row holds the queue, so
  a caller id that a library reuses on another queue is not refused there.
- The key protects the ramp only. `PUT /admin/build-routing/policy` stays
  last-writer-wins for the base build, as before #1814. A late retry of an
  old policy request sets its base again. Only the ramp that it carries is
  refused.
- A conflict on one pool while another pool takes the ramp gives `207`.
  The refusing pool is listed in `shard_errors`.
- The pool also keeps the request's own id in
  `harvest_build_policies.ramp_caller_id`. The stored `ramp_id` mixes in
  the base build, so a base change alone gives a new stored id. The
  writer retires the old request id too, so a retry after a base change
  is refused all the same. The guard's abort retires it in the same
  statement as the clear, so an aborted ramp also stays aborted on a new
  base.
- A guard can stop after its clear commits and before it reports, or its
  audit write can fail. Its marker then stays unreported. A pass finds a
  marker that is unreported, older than `report_grace`, and whose ramp no
  pool holds. The pass looks at all markers of the abort on all pools. A
  claim younger than the lease on any pool holds the whole abort back. It
  claims the abort with a lease, and only the guard that took the claim
  reports. It tries the marker pools in order. A claim that fails or times
  out moves to the next pool. A claim that another guard holds stops the
  attempt. The lease covers every write that the claimer can
  still make: one claim, one audit row and one mark per pool. It is at least
  `report_grace`. With one pool and a bound of 30 s, the lease is 150 s. The claim leaves the marker
  unreported. That guard reports the abort with reason `unreported`, no
  rates and `ramp_percent=0`, because the verdict is gone. After the audit
  row commits, it marks the markers as reported. A guard that stops before
  that leaves the claim to expire, and another guard reports the abort.
- When some markers of an abort are reported, a guard reported it and
  stopped while it marked them. A pass marks the rest and reports nothing.
  A pass removes the markers of an abort only when all are reported and
  older than the retention. The retention is `report_grace`, and at least
  `MIN_MARKER_RETENTION` (10 minutes), so a zero grace still keeps them. A
  ramp fan-out that is still in flight can write the same `ramp_id` to a
  later pool. Within the retention, the markers still finish that late
  ramp. Before it removes any marker, the guard writes a tombstone of the
  abort into the report ledger table of every pool. The ramp write reads
  the local ledger, so a fan-out write or a library retry with the aborted
  `ramp_id` is refused on every pool, also after the retention. When a
  pool misses its tombstone, the guard keeps the markers of that queue and
  tries again on the next pass.
  The tombstone write and both ramp writers take one advisory lock per
  queue. A writer that starts after the tombstone meets it. A writer can
  also commit just before the tombstone, on a pool that holds no marker,
  for example a pool that the first fan-out missed. Under the same lock,
  the tombstone write clears a live ramp with a tombstoned id on its pool.
- A guard that reported but could not mark any of its markers causes a
  second report after the grace. A failed audit write also makes the counter count
  the abort twice. An extra report is better than an abort with none.
- After a cancel, a pass lets the clear in flight finish and starts no new
  clear. Shutdown therefore waits for one bounded clear at most, plus the
  audit write of a clear that the pass made.

## `build_id` metric label

The worker labels these metric families with its own build id (`build_id`):

| Metric | Kind |
|--------|------|
| `harvest.workflow.terminal` | task success and failure |
| `harvest.activity.attempts` | task success and failure |
| `harvest.workflow.nondeterministic_block` | ND block |
| `harvest.workflow.duration` | latency |
| `harvest.activity.duration` | latency |

The label value is the build of the worker that ran the task. A series for an
outcome that no worker code produced has `build_id="none"`. Examples are the
execution-timeout scanner, a cancel through a signal and a race-loser cancel.
Every series in a family therefore has the same label set.
`harvest.workflow.non_determinism` already had the label, and it now goes
through the same cap.

The `metrics-rs` adapter labels all five families. The built-in scrape
endpoint (`HarvestPlugin::with_metrics_scrape`) has only the two duration
families, and it labels both.

### Cardinality cap

`telemetry::build_id_label` caps the label values. A process admits the first
16 distinct build ids that it sees (`MAX_BUILD_ID_LABELS`). It never evicts a
build. A later build id, or one longer than 128 bytes, gets `__other__`. An
empty build id gets `none`.

A real build id equal to a sentinel gets the escape prefix `build:`. A real
build id that already starts with `build:` gets the prefix too. So `none`
reports `build:none`, and `build:none` reports `build:build:none`. The
encoding is one-to-one: no two builds share a series, and no build shares a
sentinel. The length bound applies to the label after the prefix.

A worker keeps one build id for its whole life, and a process normally hosts
one build. The cap therefore holds the active builds in practice. A process
that starts workers with more than 16 builds reports the rest as `__other__`.

A custom recorder that forwards to another recorder must forward the
`*_for_build` methods too. Otherwise the inner recorder reports `none`.

Compare the builds of a ramp with a query such as:

```promql
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome="failed"}[5m]))
/
sum by (build_id) (rate(harvest_workflow_terminal_total{outcome!="continued_as_new"}[5m]))
```
