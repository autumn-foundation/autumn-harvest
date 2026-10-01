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
  DR fence. No bind numbers change.
- **Capacity wake-up.** A saturated poll loop waits for a released permit, for
  `poll_interval`, or for shutdown. Throughput on a saturated worker does not
  fall to one task per `poll_interval`.
- **Slot tuner signal.** The tuner grows a full pool when a task waited at
  least 50 ms for its permit. The gate moves that wait from the permit to
  `PENDING`. The signal is now the queue wait plus the permit wait, so the
  tuner still sees a backlog. Without this, `tuner_grows_under_load_and_drains_cleanly`
  fails. That suite now honours `HARVEST_TEST_DATABASE_URL` and runs in CI.
- **Schedule-to-start SLI is unchanged.** The sample still runs from task
  eligibility to handler start. The wait moves from the local permit queue to
  `PENDING`, and the sample still includes it.
- **Out of scope.** Feeding the circuit breaker only from attempts whose handler
  started is issue #1809.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not touched.

Tests: `poll_capacity_gate_tests` (DB, AC1 and AC2) failed before the change
and pass after it. Unit tests pin `poll_admission`, `PollReservations`,
`CapacityPermit` and the kind-filtered query splice.
