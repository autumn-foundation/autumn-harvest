# Design: issue #2254, owner failover tests

Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the
protocol. Issues #2251 to #2253 built it. This issue proves failover against
real Postgres before an operator turns owner mode on.

**No migration. No new `WorkflowEvent` variant. No replay impact.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- A worker builds its owner loop with `OwnerLeaseConfig::default()`. The
  TTL is 10 s. The loop scans every second (`OWNER_SCAN_INTERVAL`).
- A crashed holder stays a candidate for 3 s. Its lease ends at most one
  TTL after its last renewal. A standby acquires at its next scan after
  that end.
- So the takeover after `kill -9` is the residual lease, plus up to one
  scan interval, plus one acquisition. The residual lease is up to one TTL.
  The worst case is a little more than TTL + 1 s.
- The decision pins its tenure before the workflow code runs. The commit
  fences that pinned tenure. A frozen owner that wakes after a takeover
  fails that fence and writes nothing.
- The fence sets `idle_in_transaction_session_timeout` to the TTL. A
  frozen open transaction ends after one TTL, and its `FOR KEY SHARE`
  lock goes with it.
- A frozen holder keeps its worker heartbeat row until the stale window.
  The owner loop judges liveness with a window of at least 120 s. The
  poison-pill reclaimer uses twice the heartbeat interval. So the
  reclaimer, not the acquisition, re-pends a frozen holder's claim.
- A `SIGSTOP` needs its own process. `infra_faults.rs` already runs a
  worker as a child process of the test binary.
- `harvest_events.timestamp` defaults to `NOW()`, the start of the
  writing transaction. It dates each decision commit.
- No page records the stage 1 table yet. Issue #2248 records the
  baselines. The source table is in
  `reports/Actor ownership prior art for Harvest.md`.

### 0.2 Brainstorm: how can a test prove failover?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Run the owner as a child process of the test binary. `SIGSTOP`, `SIGCONT` and `SIGKILL` then act on a real process. | **Adopted.** The pattern of `infra_faults.rs`. |
| B2 | Freeze the owner inside its workflow code. The decision has pinned its tenure but has not opened its commit. | **Adopted.** The commit after the takeover must fail its epoch fence. |
| B3 | Freeze the owner inside its open commit transaction, with a test trigger that sleeps on the event insert. | **Adopted.** It proves that the idle timeout frees the lock of a frozen commit. |
| B4 | Kill the owner just after a renewal, so the residual lease is a full TTL. | **Adopted.** The bound is then tested at its worst case, not by luck. |
| B5 | Date the next decision with `harvest_events.timestamp`. | **Adopted.** No polling error. |
| B6 | Scale out with workers in one process, each with its own tenures. | **Adopted.** The owner suites already run two workers in one process. |
| B7 | Count fences with a `MetricsRecorder`. The child writes them to a file. | **Adopted.** The parent reads the file. |
| B8 | Make the TTL a worker setting, so the tests run with 3 s. | Rejected. The bound must hold at the default TTL that operators run. |
| B9 | Port Golem's test as a deterministic simulation only. | Rejected. Issue #2250 owns the model. This issue needs real Postgres. |

### 0.3 Reverse brainstorm: how can the tests lie?

| # | How to make the tests lie | Counter-measure |
|---|---------------------------|-----------------|
| H1 | The frozen owner never decides, so no fence is needed. | The test waits for the hold marker. Then the decision holds a pinned tenure. |
| H2 | The fence is never hit, because the decision times out first. | The child raises the workflow-task budget to 60 s. The test asserts a fence count of at least 1. |
| H3 | The standby decides before the takeover, so the fence is moot. | The test reads the owner row. It asserts a new holder and a new epoch while the owner is still stopped. |
| H4 | The new owner never decides the frozen run, so a duplicate cannot show. | The test waits until the new owner completes the run, before `SIGCONT`. |
| H5 | The kill lands just before a renewal, so the residual lease is short. | The test kills just after a renewal moves the lease end. It records the residual lease. |
| H6 | The kill is a clean exit, not a crash. | The test asserts signal 9 on the child status. |
| H7 | Scale-out never moves a partition, so no fence can occur. | The test asserts that each new worker holds partitions after the settle. |
| H8 | "Settled" is defined so that it hides late fences. | Settle is the first time that each worker holds its target, by the owner rows. Fences after that time count. |
| H9 | A lost or duplicated event hides in a long history. | Each history must match the history of the same run shape on one worker, event by event. |
| H10 | A child outlives a failed test. | A guard kills the child on drop. The child exits on its own after 300 s. |
| H11 | The test passes on a slow machine by chance and fails in CI. | Each bound has a margin from the design, not from one run. The page records the measured values. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue has four done-when items:
  the frozen-owner test in CI, the kill -9 bound, the scale-out bound and a
  performance page.
- **Red (gut).** Signals and child processes feel flaky. The child
  pattern already runs in the chaos suite. Each wait has a bound and a
  clear message.
- **Black (risks).** Section 0.3. The kill -9 bound has no margin in the
  worst case. A standby can scan up to one second after the lease ends.
- **Yellow (value).** Golem's bug was an unowned shard for 43 minutes. These
  tests bound the unowned window and prove that no stale commit lands.
- **Green (ideas).** A standby wakes at the next foreign lease end, not
  only every second. The takeover then follows the lease end by
  milliseconds.
- **Blue (process).** Red tests first. Then the scan change until green.
  Then the page, the ADR answers and a review from several angles.

## 1. Change

1. `owner_failover_tests.rs`: two frozen-owner tests, the kill -9 test, the
   scale-out test and its control without owner mode.
2. `decision_owner.rs`: `OwnerTick::next_change`, `next_scan_wait`,
   `OwnerLoop::with_acquire_chunk`, `OwnerTick::more` and
   `OwnerLeaseTtl::orphan_after`. A tick that stops using partitions asks
   for a full scan interval.
3. `worker.rs`: the owner loop sleeps by `next_scan_wait`, acquires in
   chunks of 32 and wakes a claim loop after an acquisition.
4. `docs/performance-owner-failover.md`: the measured results beside the
   stage 1 table.
5. ADR 0009: decisions 8 and 9, and the frozen-owner and pass-condition
   rows. `sticky-routing.md`: the failover bullet.
6. The CI manifest row, a docs guard and a changelog fragment.

## 2. Measurements

### 2.1 RED, on the engine of issue #2253

- Frozen owner and frozen commit passed at once. The fence and the idle
  timeout already worked.
- Kill -9 failed in two of three runs: 10.86 s, 11.19 s and 11.16 s. The
  standby scanned once a second, and its first decision waited for all
  256 acquisitions.
- Scale-out settled after 14.4 s and 16.0 s. Owners at target took the
  partitions of slow joiners as orphans after 3.3 s, and then released
  them again.
- The rate after scale-out was lower than before. The CPU stayed 30% idle.
  The control without owner mode showed the same drop. So the drop comes
  from eight workers in one process, not from owner mode.

### 2.2 GREEN

- Kill -9: 10.20 s to 10.40 s after a kill with a full TTL left.
- Scale-out: recovery in 7.6 s to 8.6 s. No fence after the settle.
- `docs/performance-owner-failover.md` has the tables.

## 3. Out of scope

- A TTL below 3 s. The tests measure the default TTL only.
- Batched acquisition in one transaction. Chunks of 32 meet the bounds and
  keep the lock protocol of ADR 0009.
- The persist and throughput rows of the stage 1 table. Issue #2248 owns
  them.
