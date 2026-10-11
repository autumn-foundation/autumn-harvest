# Harvest safety report

This report states four safety guarantees of the Harvest engine on Postgres.
For each guarantee, it gives the claim, the tests, the results and the known
limits. Every result has a command that reproduces it. Like a
[Jepsen](https://jepsen.io/analyses) analysis, the report states what fails,
not only what passes.

The Harvest team wrote this report. It is not an independent audit. Run the
commands and check the results yourself.

## Summary

| Guarantee | Claim in one line | Result | Main limit |
|---|---|---|---|
| Leases | A crashed or cut-off worker loses its task. A worker that renews its lease in time keeps it. | All checks pass. | A live worker can look dead. Then its activity can run twice. |
| Fencing | Only the current claim of a task row can write it. | All checks pass. Each pre-fix model fails, as designed. | No model covers shard-generation fencing. |
| Exactly-once completion | Each task records at most one terminal event. | All checks pass. | Activity side effects are at least once. #1871 is fixed only in part. |
| Signal ordering | If each send starts after the previous send commits, history records the signals in send order. | All checks pass. | Two signals sent in one transaction have no defined order. |

The results come from a run on 2026-10-09, on `trunk-dev` at `24621be` with
this change applied. The run used Linux x86-64 with 4 cores, Rust 1.99,
Postgres 16, Docker 29, toxiproxy 2.9.0, Java 21 and TLC 1.7.4.

## Scope and fault model

The report covers the Postgres backend. It does not cover the SQLite backend
or the Redis dispatch channel. Redis carries only task references, so it does
not change the four guarantees.

The tests inject these faults:

- A panic in a worker task at a named point, through the chaos harness
  ([`chaos.md`](testing/chaos.md)).
- A SIGKILL of a worker process.
- `pg_terminate_backend` while a COMMIT waits, before or after the COMMIT
  completes.
- A lost COMMIT acknowledgement.
- A Postgres crash and restart, and a `docker pause` longer than the lease.
- Network latency and a network partition between a worker and Postgres,
  through toxiproxy.
- Seeded random schedules of workers, stalls and crashes, through the
  simulator ([`simulation.md`](testing/simulation.md)).

TLC also checks every interleaving of claims, heartbeats and crashes in a
bounded model.

The tests do not inject these faults:

- A clock step or clock skew on the Postgres host. Leases and timeouts use
  the database clock, so the report assumes that this clock does not jump.
- Disk faults, a lost `fsync` or a torn page.
- A failover to a replica under load. The DR tests promote a
  logical-replication standby only.
- A TCP reset that the client reads as "error communicating with the
  server". See the #1871 limit below.

## How to reproduce

You need these tools:

- Rust, as `rust-toolchain.toml` pins it.
- Docker, for the Postgres 16 and toxiproxy containers.
- Java 11 or later, for TLC.

A DB test starts its own Postgres container. Most suites also accept
`HARVEST_TEST_DATABASE_URL` for a migrated database. The `chaos_tests`
infrastructure tests always start their own containers. They run on Unix
only.

Without Docker, some DB suites print `SKIPPED` and pass. One example is
`cross_region_dr_tests`. Look for `SKIPPED` in the output.

`scripts/check-formal-models.sh` downloads TLC and checks its SHA-256. To run
one config by hand, set `TLA2TOOLS_JAR` to the path of `tla2tools.jar` first.

The last word of each Check cell says how often CI runs the check:

- **PR**: CI runs it on each change.
- **Nightly**: a scheduled workflow runs it.

## Leases

### Claim

A worker claims a task row and renews its lease with heartbeats. The
heartbeat stamp uses the database clock (#1807). The orphan reclaimer
requeues a task when its worker misses heartbeats for two intervals. Each
reclaim adds a crash strike. Before the last strike, a reclaim needs proof
that the worker is dead (#1879). A worker that loses its lease stops its
activity.

The protocol is design decision 9 in
[`architecture.md`](architecture.md#key-design-decisions).

The lease is a liveness device, not a safety device. A live worker can look
dead, so two workers can run one task at once. The fence in the next section
rejects the writes of the stale worker.

### Tests

- **TLA+.** `ActivityClaim.tla` models claim, heartbeat, orphan reclaim and
  finish for one task row ([`formal-methods.md`](testing/formal-methods.md)).
  Each spec also has a `Reach` config that must fail. Its trace proves that
  the model reaches the race, so the pass of the fixed model is not vacuous.
- **Infrastructure faults.** `chaos_tests::infra_faults` pauses Postgres,
  partitions a worker and kills a worker process. Workers use a 500 ms
  heartbeat, so the lease is 1 s. In the partition test, worker A loses
  Postgres and worker B takes over.
- **Integration tests.** `poison_pill_tests` covers the reclaimer.
  `activity_claim_epoch_tests` covers a lost lease.

### Results

| Check | Result | Reproduce |
|---|---|---|
| TLC, `ActivityClaim.cfg` (3 workers, 5 claims), PR | No error. 1,324,284 distinct states. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config ActivityClaim.cfg ActivityClaim.tla` |
| TLC, `ActivityClaimReach.cfg`, PR | TLC reports a violation of `NoStaleOwnerAfterFinish`, as designed. A stale owner still runs after a later claim finishes. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config ActivityClaimReach.cfg ActivityClaim.tla` |
| Partition longer than the lease, Nightly | Worker B reclaims all three tasks. All workflows complete. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::toxiproxy_partition_longer_than_lease_ttl -- --nocapture` |
| Postgres paused for 4 s, Nightly | All workflows complete. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::postgres_pause_longer_than_lease_ttl -- --nocapture` |
| SIGKILL of a worker process, Nightly | Another worker reclaims the task. The workflow completes. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::sigkill_child_worker_mid_activity -- --nocapture` |
| Orphan reclaimer, PR | A live worker keeps its task. At the last crash strike, a late worker that heartbeats again also keeps its task. | `cargo test -p autumn-harvest --test integration poison_pill_tests:: -- --test-threads=1` |
| Lost lease stops the attempt, PR | The worker stops an attempt whose claim moved, and drops its result. | `cargo test -p autumn-harvest --test integration activity_claim_epoch_tests::worker_stops_an_attempt_whose_claim_moved_and_drops_its_result` |

### Known limits

- After a long pause or a partition, a live worker can look dead. The
  reclaimer then gives its task to another worker. The activity body can run
  twice.
- The old attempt continues until it sees the lost lease. During a partition
  it cannot see it, so it runs until the partition ends. A dropped handler
  stops only at an await point. Blocking work and spawned tasks continue.
- Below the last crash strike, a worker that is only late loses its task at
  once.
- The reclaimer uses its own heartbeat interval to judge every worker.
  `harvest_workers` does not record the interval of each worker. So a worker
  with a slower interval looks dead while it is alive. Use one interval for
  the whole fleet.
- No test steps or skews the database clock.
- A partitioned session keeps its row locks until TCP keepalive ends it
  (#1876). Reclaim skips a locked row, so its task stays claimed until the
  session ends. The partition test sets
  `idle_in_transaction_session_timeout = 5s` for this reason.
- The timeout sweeper, cancellation and operator actions write without a
  lease check ([design decision 9](architecture.md#key-design-decisions)).
- A mutex lease can expire under a live holder. The engine cannot fence the
  external side effects of that holder.
- The scanner lease is a load control, not a fence. If the lease query
  fails, scanners run without it.
- The simulator drives store statements, not the `worker.rs` loop. See
  [`simulation.md`](testing/simulation.md#limits).
- The world simulation crashes a worker only between two steps. No worker
  dies inside a task body.

## Fencing

### Claim

The fencing token of a task row is the pair `(worker_id, attempt)`. Each
claim adds 1 to `attempt`. One SQL predicate, `claim_held`, guards every
owner write. A write from a claim that is no longer current changes nothing.

The workflow-task path also checks `crash_strikes` (#1806). The
capability-miss release checks `attempt` (#1917). `complete_task` requires
the claim of the caller (#1992).

Cross-region failover adds a second fence. `replication::assert_fence`
rejects a worker that holds an old shard generation.

### Tests

- **TLA+.** `ActivityClaim.tla` and `WorkflowTaskClaim.tla` check the fixed
  guards. A pre-fix config turns each fix off and must fail.
- **Simulation.** A seeded simulator checks the claim invariants against an
  in-memory oracle. A differential test replays each run on Postgres.
- **Model-based property test.** `lifecycle_model_props` runs random
  operation sequences against Postgres and a reference model
  ([`property-and-fuzz.md`](testing/property-and-fuzz.md)).
- **Infrastructure faults.** The partition test releases stale results after
  the partition ends. Only the fence can reject them.
- **Integration tests.** Each fenced write has a stale-claim test.

### Results

| Check | Result | Reproduce |
|---|---|---|
| TLC, `ActivityClaimPreFix.cfg` (fence off), PR | TLC reports a violation of `TerminalByCurrentClaim`, as designed. It prints the #1789 trace. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config ActivityClaimPreFix.cfg ActivityClaim.tla` |
| TLC, `WorkflowTaskClaim.cfg` (2 workers, 5 claims, 2 strikes), PR | No error. 18,596 distinct states. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config WorkflowTaskClaim.cfg WorkflowTaskClaim.tla` |
| TLC, `WorkflowTaskClaimPreFix.cfg` (no `attempt` term), PR | TLC reports a violation of `TerminalByCurrentClaim`, as designed. This is the #1806 trace. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config WorkflowTaskClaimPreFix.cfg WorkflowTaskClaim.tla` |
| TLC, `WorkflowTaskClaimCapMissPreFix.cfg`, PR | TLC reports a violation of `OwnerWritesByCurrentClaim`, as designed. This is the #1917 gap. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config WorkflowTaskClaimCapMissPreFix.cfg WorkflowTaskClaim.tla` |
| TLC, `WorkflowTaskClaimReach.cfg`, PR | TLC reports a violation of `NoStaleCycleOfTheHolder`, as designed. The fixed model reaches the race. | `cd formal/tla && java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc -config WorkflowTaskClaimReach.cfg WorkflowTaskClaim.tla` |
| Simulator, 200 seeds, PR | Every invariant holds on every seed. Each seed runs twice and gives the same trace. | `cargo test -p autumn-harvest --no-default-features --test dst` |
| Simulator, pre-fix guard, PR | Seed 3 breaks `TerminalByCurrentClaim` under the pre-fix guard. | `cargo test -p autumn-harvest --no-default-features --test dst pre_fix_guard_reproduces_issue_1789` |
| Simulator, claim epoch on seed 3, PR | The claim epoch rejects the stale write on the same seed. | `cargo test -p autumn-harvest --no-default-features --test dst claim_epoch_guard_rejects_the_stale_write_on_the_same_seed` |
| Differential test, 32 seeds, PR | Postgres matches the oracle after every step. | `cargo test -p autumn-harvest --test integration dst_differential_tests:: -- --test-threads=1` |
| Model-based property test, 128 cases, PR | Postgres matches the model after every operation. | `cargo test -p autumn-harvest --test integration lifecycle_model_props:: -- --test-threads=1` |
| Stale activity claims, PR | No stale completion, failure, start or heartbeat changes the row. | `cargo test -p autumn-harvest --test integration activity_claim_epoch_tests:: -- --test-threads=1` |
| Stale workflow-task claims, PR | A workflow-task cycle that lost its claim writes no terminal event. | `cargo test -p autumn-harvest --test integration terminal_write_ownership_tests:: -- --test-threads=1` |
| Shard-generation fence, PR | A fenced worker cannot claim, persist or re-encrypt. | `cargo test -p autumn-harvest --features testing --test integration cross_region_dr_tests:: -- --test-threads=1` |
| Partition, stale results after it ends, Nightly | Worker A writes three stale results. History gets no `ActivityCompleted` from worker A. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::toxiproxy_partition_longer_than_lease_ttl -- --nocapture` |

The partition test fails with the fence off ([`chaos.md`](testing/chaos.md)).

### Known limits

- No model covers shard-generation fencing or the rebalance cutover. See
  [`formal-methods.md`](testing/formal-methods.md#not-modelled-yet).
- Issue #2003 checks chaos traces against `ActivityClaim` and
  `WorkflowTaskClaim` only. No check compares test traces with the
  `CodecRotation` model. A code change can leave that model out of date.
- The models are bounded. `ActivityClaim` has 3 workers and 5 claims.
  `WorkflowTaskClaim` has 2 workers, 5 claims and 2 strikes.
- The model `SelfRelease` uses a stronger guard than the code.
  [`formal-methods.md`](testing/formal-methods.md) argues that the two are
  equal inside the claim transaction.
- `fail_task`, `requeue_for_retry` and `defer_rate_limited_task` have no
  fence by design. The timeout sweeper acts only on the claim it scanned
  (#1809). The model `TimeoutFail` has no such check.
- The simulator does not model the timeout sweeper, the `FAILED` state or
  quarantine. It draws actions from fixed weights. It does not use PCT
  (probabilistic concurrency testing).
- The world simulation never fails an activity, and its reclaimer never
  quarantines. So it does not reach the `FAILED` state.
- A 0.6 worker has no claim fence. A mixed 0.6 and 0.7 fleet must keep
  timeouts terminal until the upgrade ends
  ([ADR 0005](adr/0005-activity-timeout-retry-and-open-circuit.md)).
- The DR fence cannot stop writes to the old primary. The operator must
  isolate it ([`cross-region-dr.md`](cross-region-dr.md)).
- The mutex `lock_seq` fences the lock table only. No Postgres test releases
  a lock from a stale holder.

## Exactly-once completion

### Claim

Each activity task and each workflow task records at most one terminal
event. A completed activity records exactly one `ActivityCompleted`. If the
server ends the session during the result write, the worker writes the
result again. The claim fence makes the repeat safe.

This is exactly-once **recording**. Activity **execution** is at least once.
A crash or a timeout runs the activity again
([README](../README.md#activity-idempotency-keys)). Use an idempotency key.
For a write to the same database, use `run_transactional`
([`transactional-activities.md`](transactional-activities.md)).

Idempotent starts and schedule fires are also exactly-once. The history
checks in [`chaos.md`](testing/chaos.md#history-checks-issue-1829) test them
for linearizability.

### Tests

- **TLA+.** `AtMostOneTerminal` is an invariant of both claim models.
  `ActivityClaim` also checks `TerminalStateHasOneEvent`.
- **Infrastructure faults.** Three tests kill the backend during a COMMIT.
  Three more lose the COMMIT acknowledgement. The table below shows the
  activity and workflow cases. The oracle counts terminal events itself,
  because the table key allows a duplicate at a new event id.
- **Convergence sweep.** The sweep crashes workers at seeded points. Then it
  checks that each workflow recovers.
- **History checks.** A Porcupine-style linearizability checker reads client
  histories under random backend kills.

### Results

| Check | Result | Reproduce |
|---|---|---|
| Kill during the COMMIT of `ActivityCompleted`, Nightly | The worker writes the result again. History holds one attempt and one `ActivityCompleted`. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::terminate_backend_mid_commit_append -- --nocapture` |
| Lost acknowledgement of `ActivityCompleted`, Nightly | The repeat changes nothing. History holds one attempt and one `ActivityCompleted`. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::terminate_backend_after_commit_ack_lost_append -- --nocapture` |
| Kill during the COMMIT of `WorkflowCompleted`, Nightly | Each workflow has exactly one terminal event. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::terminate_backend_mid_commit_complete -- --nocapture` |
| Postgres crash and restart, Nightly | Every workflow completes with one terminal event. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::infra_faults::postgres_crash_restart_mid_workload -- --nocapture` |
| Oracle self-test, Nightly | The oracle flags a forged duplicate terminal event. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::oracle_flags_a_duplicate_terminal_event` |
| Convergence sweep, seed 8, Nightly | Every workflow converges after a crash before commit. | `CHAOS_SEEDS=8 cargo test -p autumn-harvest --features chaos --test integration chaos_seeded_convergence_sweep -- --nocapture` |
| Stale `complete_task` (#1992), PR | A stale owner cannot complete a reclaimed row. | `cargo test -p autumn-harvest --test integration activity_claim_epoch_tests::stale_owner_cannot_complete_a_reclaimed_row_through_complete_task` |
| History checks under backend kills, PR | Starts satisfy `StartIdempotency`. Schedule fires satisfy `ExactlyOnceFire`. | `cargo test -p autumn-harvest --test integration history_crash_tests:: -- --test-threads=1` |

### Known limits

- #1871 is fixed only in part. The result write repeats only when the server
  ends the session. A TCP reset reads as "error communicating with the
  server", and that write is still lost. The task then waits for
  `start_to_close`, and the activity runs again. No test injects a TCP reset.
- Error detection reads English message text. With a non-English
  `lc_messages`, the engine misses the error, so the result write does not
  repeat.
- A result write that offloaded a payload gets one try. On a failure, a new
  attempt runs the handler again.
- After a crash and restart, every repeat can fail. Then `start_to_close`
  and the retry policy start a new attempt (#1870).
- `run_transactional` needs the same database. No fault test covers it.
- No database constraint stops a second terminal event at a new event id.
  The execution row lock and the fence enforce the rule.
- The history checker has no model for activity completion. It covers starts
  and schedule fires only.
- The infrastructure workloads are small: one to six workflows per test.

## Signal ordering

### Claim

A send inserts a `harvest_signals` row. The target workflow moves pending
rows into history as `SignalReceived` events when it claims its next task.
The ingest sorts rows by `received_at`, then by row id.

`received_at` is the Postgres `NOW()` of the send transaction, which is its
start time. A send that starts after the previous send commits sorts after
it. Replay reads history order, so each replay sees the same order.

When a signal and a timer are both due, database time sets their order. A
signal received before the timer deadline is recorded first. A tie goes to
the timer.

A keyed send is recorded exactly once per execution. A retried unkeyed send
adds a second signal
([`management-api.md`](management-api.md#idempotent-delivery-issues-521--753)).

A reset with the `Buffer` policy moves pending signals to the new run. The
new rows keep the source `received_at`, so signals with different times keep
their order. Signals that share one time have no defined order on either run.

### Tests

- **Integration tests.** `signal_tests` checks send order, reset order and
  dedupe on Postgres. Other suites check signal-with-start and a signal
  during a shard move.
- **Unit tests.** They cover the wake merge and the handler dispatch order.
  An integration test replays a history with reordered signals.
- **Chaos.** A reproducer checks that the outbox cannot deliver an external
  signal twice.

### Results

| Check | Result | Reproduce |
|---|---|---|
| Send order on Postgres, PR | Eight sends that commit one after another reach history in send order, over two wake cycles. | `cargo test -p autumn-harvest --test integration signal_tests::committed_sends_are_recorded_in_send_order` |
| Reset with `Buffer`, PR | The new run reads eight buffered signals in the source order. | `cargo test -p autumn-harvest --test integration signal_tests::reset_buffer_keeps_the_order_of_pending_signals` |
| Signal and timer race, PR | A signal before the deadline is recorded first. A signal after it is recorded second. | `cargo test -p autumn-harvest --lib merge_wake_events_signal` |
| Handler dispatch order, PR | Handlers fire in history order, not in registration order. | `cargo test -p autumn-harvest --lib signal_handlers_dispatch_in_history_order` |
| Replay with reordered signals, PR | A history with the signals in the other order replays without divergence. | `cargo test -p autumn-harvest --no-default-features --features testing --test integration replayer_tests::replayer_replays_signal_handler_workflow_with_reordered_signals` |
| Keyed dedupe, PR | Five sends with one key add one signal. | `cargo test -p autumn-harvest --test integration signal_tests::idempotent_signal_with_same_key_lands_exactly_once` |
| Signal-with-start, PR | The signal is in history before the first dispatch. | `cargo test -p autumn-harvest --test integration signal_with_start_tests::signal_with_start_appends_signal_to_history_before_first_dispatch` |
| Signal during a shard move, PR | The signal aborts the cutover and is not lost. | `cargo test -p autumn-harvest --features testing --test integration shard_rebalance_db_tests::a_signal_arriving_mid_migration_aborts_the_cutover_and_is_not_lost` |
| Outbox double delivery (#492), Nightly | History holds one signal row and one `ExternalSignalDelivered`. | `cargo test -p autumn-harvest --features chaos --test integration chaos_tests::chaos_repro_492_outbox_cannot_double_deliver_inline_external_signal -- --nocapture` |

### Known limits

- Two signals sent in one transaction share one `NOW()`. A random row id
  breaks the tie, so their order can differ from the send order. A decision
  that signals one workflow twice does this.
- `NOW()` is the start time of the send transaction, not its commit time.
  Two overlapping sends have no defined order. The ingest only sorts the rows
  that it sees, so history can hold them in start order or in commit order.
- A signal sent during the final decision of a run is lost when the run
  completes or fails (#2079, open). The sender sees success.
- At continue-as-new, a signal that history holds but the body did not read
  stays on the old run.
- No TLA+ model, simulator or history check covers signals. No
  infrastructure-fault test sends a signal.
- The SQLite backend has its own signal limits, for example #2020 and #2028.

## Bugs these tests found

The infrastructure-fault tests found four bugs
([`chaos.md`](testing/chaos.md#infrastructure-faults-issue-1801)):

- #1871: a lost result write after a DB error. Fixed in part. See
  [Exactly-once completion](#exactly-once-completion).
- #1870: a `StartToClose` timeout ignored the retry policy. Fixed.
- #1876: orphan reclaim blocked on a row that a cut-off session locked.
  Fixed.
- #1879: slow heartbeat ticks made a live worker look dead. Fixed.

The `WorkflowTaskClaim` model found #1917 before any test did. It is fixed.

The models and the simulator also pin earlier fence bugs: #1789 and #1806.
A pre-fix config or seed reproduces each one.

The review of this report found one more bug. A reset with the `Buffer`
policy gave each buffered signal a new `received_at`. One INSERT wrote every
row, so all rows got one `NOW()`, and the random row id set their order. This
change keeps the source `received_at`. The reset row in the signal table
pins the fix.

## Limits of this report

- No tool measures test coverage (#1818, open). The report lists the tests
  that exist. It cannot say which code paths no test reaches.
- The chaos suite runs nightly, not on each PR. The `test-db-linux` and
  `test-nodb` jobs run the PR suites, and neither is a required check yet.
- The thread timing of a DB test is not deterministic. A seed fixes the
  random choices of a history test, not the schedule.
- The report covers four guarantees. It does not cover determinism, codec
  rotation or retention.
- Each source page keeps its own limits. This report restates the ones that
  touch the four guarantees:
  [`formal-methods.md`](testing/formal-methods.md#not-modelled-yet) and
  [`simulation.md`](testing/simulation.md#limits).

## Related

- [Chaos and fault-injection harness](testing/chaos.md) (#940, #1801, #1829).
- [Formal methods: TLA+ and Kani](testing/formal-methods.md) (#1819).
- [Deterministic simulation](testing/simulation.md) (#1830).
- [Property tests and fuzzing](testing/property-and-fuzz.md) (#1829).
- [Comparison with other engines](comparison.md).
