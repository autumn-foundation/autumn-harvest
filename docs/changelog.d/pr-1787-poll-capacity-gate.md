## Phase 5.x — Postgres poll path claims only against a free local permit (issue #1787)

The default Postgres poll path claimed a task before the worker had a free
permit for it. The claim stamps `started_at`, and start-to-close and heartbeat
deadlines run from `started_at`. A task queued behind a saturated worker's
semaphore therefore used its timeout budget without running. An idle peer could
not take it. The timeout then failed the task and fed the circuit breaker.

- **Per-kind claim gate.** `poll_once` reads the free workflow and activity
  permits first. With no free permit, it does not claim. With one kind free, it
  claims that kind only. With both free, it uses the unchanged claim statement.
  The opt-in Redis dispatch path already had this gate (issue #1312).
- **Reservation.** The gate takes a `DispatchReservation` before the claim, as
  the Redis path does. The spawned task drops it when it holds its permit. The
  next poll therefore cannot count the same free permit twice.
- **Kind-filtered claim query.** `queue::claim_task_query_for_kind` splices one
  literal `task_type` predicate into the `candidate` CTE. It composes with the
  DR fence. No bind numbers change. `queue::claim_task_of_kind_on_shard` runs
  it; `claim_task_on_shard` keeps its signature and statement.
- **Capacity wake-up.** While a pool is full, the idle wait of each poll loop
  also wakes on a released permit. This applies to the single-shard loop, the
  multi-shard loop and the Redis-degraded `drain_postgres` fallback. A NOTIFY
  alone cannot do it, because the backlog sent its NOTIFY long ago.
- **Slot tuner signal.** The tuner grows a full pool when a task waited at
  least 50 ms for its permit. The gate moves a backlog from the permit to
  `PENDING`. When the gate refused a kind, the next dispatch of that kind now
  adds its queue wait to the signal. A custom `SlotTuner` sees the same change
  in `SlotObservations::max_permit_wait`. Without the queue-wait term,
  `tuner_grows_under_load_and_drains_cleanly` fails. That suite now honors
  `HARVEST_TEST_DATABASE_URL` and runs in CI.
- **Schedule-to-start SLI is unchanged.** The sample still runs from task
  eligibility to handler start. The wait moves from the local permit queue to
  `PENDING`, and the sample still includes it.
- **Out of scope.** Feeding the circuit breaker only from attempts whose handler
  started is issue #1809.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not touched.

Tests: `poll_capacity_gate_tests` (DB) covers AC1, AC2 and AC3, the
kind-filtered statement, and the wake-up latency. AC1, AC2 and the wake-up test
failed before their fix and pass after it. Unit tests pin `poll_admission`,
`PollReservations`, `CapacityPermit` and the kind-filtered query splice.
