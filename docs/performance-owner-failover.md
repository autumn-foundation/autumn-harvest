# Owner failover: frozen owner, kill -9 and scale-out

Issue #2254 tests owner failover against real Postgres. Owner mode is the
stage 1a model of [ADR 0009](adr/0009-decision-owner.md). The suite is
`autumn-harvest/tests/integration/owner_failover_tests.rs`. CI runs it on
the Linux database job.

## Result

All four tests pass. The engine needed four fixes first. The
[RED results](#red-results-and-the-fixes) section gives the failures that
they fix.

| Test | Pass condition | Measured |
|---|---|---|
| `a_frozen_owner_is_fenced_after_a_takeover` | The commit of the woken owner is fenced. No event is lost or duplicated. | Fenced. History exact. Takeover 9.2 s to 10.0 s after `SIGSTOP`. |
| `a_frozen_commit_transaction_does_not_block_the_takeover` | The takeover does not wait for the frozen commit transaction. The woken owner writes nothing. | Takeover 12.0 s to 12.1 s after `SIGSTOP`. History exact. |
| `after_kill_9_the_next_decision_runs_within_ttl_plus_one_second` | The next decision on the partitions of the killed owner runs within TTL + 1 s, 11 s. | 10.2 s to 10.3 s, with a full TTL left on the lease at the kill. |
| `scale_out_from_4_to_8_workers_recovers_within_two_ttls` | Recovery within 2 TTLs, 20 s. No fenced commit after the fleet settles. | Recovery 8.4 s to 9.8 s. 0 fences after the settle. 0 to 2 before it. |

## Beside the stage 1 table

The stage 1 table is in
`reports/Actor ownership prior art for Harvest.md`.
Issue #2248 records its baselines. This page fills its two failover rows.

| Stage 1 measurement | Pass condition | Issue #2254 result |
|---|---|---|
| Kill -9 owner: time to next decision on its partitions | At most lease TTL + 1 s | Pass. 10.2 s to 10.3 s at the default 10 s TTL. |
| Scale-out 4 to 8 workers: throughput dip and recovery | Recovery within 2 lease TTLs; zero fenced-commit errors after settle | Pass. Recovery in 8.4 s to 9.8 s. The slowest second ran at 10% to 17% of the baseline. 0 fences after the settle. |

## Method

- Each test runs on its own database, because owner mode is a switch per
  database. Each worker uses the default lease TTL of 10 s.
- The owner under test is a child process of the test binary. So the test
  can send it `SIGSTOP`, `SIGCONT` and `SIGKILL`. The standby workers run
  in the test process.
- `harvest_events.timestamp` is the start of the writing transaction. It
  dates each decision. A decision event is `ActivityScheduled`,
  `TimerStarted` or `WorkflowCompleted`. A foreign writer writes none of
  them.
- Each test compares each history with the history of the same workflow on
  one worker. A lost event, a duplicated event or a gap in the event ids
  fails the test.
- The host was a 4-CPU container with Postgres 16 on the same host. The
  build was a debug build.

### Frozen owner

The child owns all 512 partitions. A run waits for a signal. In the child,
the decision after the signal blocks once, after the decision pinned its
tenure. The test then stops the child with `SIGSTOP`.

A standby starts. It acquires the partition of the run at the next epoch
when the lease of the child ends. The poison-pill reclaimer re-pends the
claim of the stopped child. The standby then completes the run.

The test wakes the child with `SIGCONT`. The child then tries to commit its
decision.
The fence finds a new holder and a new epoch, so the transaction rolls
back. The child counts the fence. The history is the same as before the
wake. This is the test of golemcloud/golem#3845.

### Frozen commit

The child stops inside its open commit transaction. It holds the owner row
`FOR KEY SHARE`. A test trigger makes the first event insert of the fenced
transaction sleep for 2 s. The test sends `SIGSTOP` while the backend
sleeps.

The fence sets `idle_in_transaction_session_timeout` to the TTL. So 10 s
after the sleep, the database ends the frozen transaction and frees the
lock. The standby then acquires. That is the 12 s takeover: 2 s of sleep
plus 10 s of idle timeout. The child writes nothing after `SIGCONT`.

### Kill -9

The child and a standby own about 256 partitions each. The child has 40 runs of
25 one-second timers, and the standby has 12. With 40 runs, the first
chunk of 32 partitions that the standby takes almost always holds one.
The test waits until the child renews its leases.
Then it sends `SIGKILL` at once. So each lease of the child has a full TTL
left. That is the worst case for the bound.

| Run | Lease left at the kill | Takeover | Next decision |
|---|---|---|---|
| 1 | 9.97 s | 10.00 s | 10.18 s |
| 2 | 9.96 s | 9.99 s | 10.19 s |
| 3 | 9.98 s | 10.01 s | 10.30 s |
| 4 | 10.00 s | 10.03 s | 10.22 s |

The takeover is the first acquisition on the partitions of the child. It
follows the end of the lease by 27 ms to 33 ms. The next decision follows
it by 0.2 s to 0.3 s. Every run then completes with an exact history.

### Scale-out

Four workers own about 128 partitions each. A driver keeps 32 runs of a
three-activity workflow in flight. After 15 s, four more workers start.
The fleet has settled when balance asks no worker to act. All 512
partitions then have an owner, and no worker holds more than 65: its
target of 64, plus one. The rates count decision commits per second.

| Run | Baseline | Rate after | Slowest second | Settled | Recovered | Fences before, after |
|---|---|---|---|---|---|---|
| 1 | 70.2/s | 57.2/s | 17% | 8.9 s | 8.9 s | 0, 0 |
| 2 | 74.5/s | 57.6/s | 15% | 8.4 s | 8.4 s | 1, 0 |
| 3 | 76.3/s | 56.8/s | 17% | 9.8 s | 9.8 s | 2, 0 |
| 4 | 72.9/s | 55.4/s | 10% | 9.8 s | 9.8 s | 1, 0 |
| 5 | 74.9/s | 56.9/s | 13% | 8.6 s | 8.6 s | 1, 0 |
| 6 | 73.8/s | 56.9/s | 16% | 9.6 s | 9.6 s | 1, 0 |

"Recovered" is the later of two times. The first is the settle. The
second is the first second from which five 3 s windows in a row hold
80% of the lower rate. A rate after the scale-out below 50% of the
baseline fails the test.

The rate falls from about 74/s with 4 workers to about 57/s with 8. That
is not an owner-mode cost. The control,
`scale_out_without_owner_mode_measurement`, runs the same scale-out with
owner mode off. Its rate also falls, from about 77/s to between 52/s and 60/s.
Eight workers in one test process share one Postgres server and claim
one queue. The host CPU stayed about 30% idle, so the
CPU is not the limit.

Owner mode costs the dip. The control recovers in 1 s to 2 s. Owner mode
recovers in about 9 s. A released partition takes no claim until its new
owner acquires it. Its first decision on the new owner then replays cold.

## RED results and the fixes

The tests ran first against the engine of issue #2253.

- **Kill -9.** The next decision ran 10.86 s, 11.19 s and 11.16 s after
  the kill. Two of three runs failed. A standby scanned only once a
  second, so its takeover came up to 1 s after the lease end. Its first
  decision then waited for all 256 acquisitions of the scan.
- **Scale-out.** The fleet settled after 14.4 s and 16.0 s. A joiner took
  only 10 to 15 partitions per second under load. A released partition
  became an orphan after one renewal interval, 3.3 s. Owners at their
  target then took it. They went over target and released again.

The fixes:

1. A standby scans again just after the next foreign lease end, when that
   end comes before the next scan. `decision_owner::next_scan_wait` sets
   the sleep. `OwnerTick::next_change` gives the end.
2. An acquisition wakes the idle claim loop of the worker. So the new
   owner claims the waiting tasks at once.
3. The worker acquires at most 32 partitions per scan. It adopts them,
   then scans again at once while free partitions remain.
4. A partition becomes an orphan one TTL after its lease end, not one
   renewal interval after it. `OwnerLeaseTtl::orphan_after` gives the
   wait.

A tick that stops using partitions still waits a full scan interval
before the release. So the decisions in flight in those partitions can
commit first. ADR 0009 decision 9 calls that the drain.

## Limits

- The bound holds at the default TTL of 10 s. The tests do not measure a
  shorter TTL, so Harvest still rejects a TTL below 3 s.
- The test process runs all standbys. A fleet of separate hosts can show
  another dip and another rate.
- The kill test kills one of two owners. A larger fleet splits the
  partitions of the dead owner among more peers. So each peer acquires
  fewer of them.
- The frozen-commit test uses a test trigger to stop inside the
  transaction. The trigger runs only on the test database.

## Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --test integration owner_failover_tests -- \
  --test-threads=1 --nocapture
```

Add `--ignored` with the filter
`owner_failover_tests::scale_out_without_owner_mode_measurement` to run the
control. Without `HARVEST_TEST_DATABASE_URL`, each test starts its own
Postgres container.
