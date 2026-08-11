## Phase 5.x — Schedule reconciler: stop the "DAG Storm" (issue #1157)

**Implemented.** An upstream deployment reported the scheduler's per-tick registration pass wedging into a permanent write storm: a single unconvergeable `harvest_schedules` row caused the same failing `UPDATE` to be re-issued once per second forever, starved every other DAG's registration, and — because every process runs its own `SchedulerRuntime` — multiplied by the size of the fleet. Four independent defects compose into that outcome; all four are fixed here. **No new `WorkflowEvent` variant, no migration, no schema change, no change to the operator PATCH path's semantics** — this is entirely a reconciler-convergence and write-volume slice.

### The load-bearing invariant

`DagInfo::as_workflow_schedule` (`info.rs`) *always* sets `workflow_name: self.name` **and** `dag_name: Some(self.name)`. A persisted row whose `dag_name` is non-NULL and different from its `workflow_name` is therefore unreachable via any registration path and is provably corrupt. Every repair decision below keys off that invariant rather than guessing which of two rows "wins".

### Defect 1 — resolver blind spot (permanent unique violation)

`find_reusable_dag_workflow_schedule` looked for a `workflow_name = D` holder **only among rows with `dag_name IS NULL`**. A row holding `workflow_name = D` with a non-NULL, non-`D` `dag_name` matched neither shape, so the resolver returned the `dag_name = D` row and `apply_workflow_schedule_update` issued `UPDATE … SET workflow_name = D` onto it while the *other* row still held `D` → a guaranteed, permanent violation of `harvest_schedules_workflow_name_unique`, re-issued every tick.

Fixed by dropping the `dag_name IS NULL` predicate from the holder query and routing the outcome through a **pure, total classifier** rather than an ad-hoc branch:

```rust
enum NameHolder<'a> { Absent, Unowned, OwnedBy(&'a str) }
enum WorkflowNameHolder { Vacant, WorkflowOnly, Squatter, Conflict, SelfInconsistent }
const fn classify_workflow_name_holder(registering_dag, registering_workflow, holder) -> WorkflowNameHolder
```

- `Vacant` → proceed with the `dag_name = D` row (today's happy path, unchanged).
- `WorkflowOnly` (holder has `dag_name IS NULL`) → today's merge-and-delete / adopt path, unchanged.
- `Squatter` (holder's `dag_name` ≠ its own `workflow_name` — i.e. **the holder violates the invariant**) → `release_squatted_workflow_name` sets that row's `workflow_name = NULL` (**not** a delete: the row's id, pause state, counters and `dag_name` identity are preserved) and emits a `tracing::warn!`, then registration proceeds normally. The row remains legal under `harvest_schedules_kind_check` because its `dag_name` is non-NULL by construction.
- `SelfInconsistent` (**our own** registration is decoupled: `ws.dag_name != ws.workflow_name`) → refuse, **including when the name is completely free**. This is the subtle one, and getting it wrong is worse than the original bug. Classifying a vacant name as writable regardless of self-consistency persists a row shaped *exactly* like the corrupt one `Squatter` strips; registering a DAG by that name later would null the victim's `workflow_name`, and since the due list requires it non-NULL that schedule would stop firing **silently and permanently**, with its own registration then failing forever. The repair path would have manufactured its own victim — converting a loud, non-destructive storm into a quiet outage. Refusing up front is precisely what makes `Squatter`'s premise ("unreachable through registration ⇒ corrupt") true.
- `Conflict` (a well-formed peer genuinely owns the name) → **return `HarvestError::Config` and write nothing.** This is the reverse-brainstorming outcome that shaped the design: a naive "release whatever squats the name" would either destroy legitimate data or start a *new* 1 Hz flap-storm as two registrations stole the name from each other every tick. The error names both schedule ids and tells the operator to rename one. This arm is *defensive* — unreachable in practice, since `harvest_schedules.dag_name` is UNIQUE and the resolver excludes our own dag row by id first, so no foreign holder can carry our `dag_name`. Kept so the classifier stays total if that constraint is ever relaxed.

### Defect 1b — same blind spot in INSERT form

`insert_dag_workflow_schedule_if_missing` inserted a row carrying **both** `dag_name` and `workflow_name` under `ON CONFLICT (dag_name) DO NOTHING`, whose arbiter offers no protection against the `workflow_name` unique index — reached whenever there is no `dag_name = D` row at all. The arbiter is now the bare `.on_conflict_do_nothing()` (covering *both* unique indexes), and the follow-up select is `.optional()` with an `.ok_or_else(…)` that returns a named `HarvestError::Config` instead of surfacing an opaque unique violation.

### Defect 2 — one bad DAG starved the rest, and retried at 1 Hz forever

`register_workflow_schedules_for_shard` propagated the first error with `?`, so one unconvergeable DAG prevented **every DAG after it in iteration order** from being registered at all — a single bad row could silently disable a whole shard's scheduling. Both `_for_shard` registration passes now **collect per-schedule errors instead of short-circuiting**: each schedule is attempted independently, a failure emits a single `WARN` naming the schedule (`warn_registration_failure`) and is recorded against a new per-process `ScheduleRegistrationBackoff` keyed `"{shard}:{kind}:{name}"`.

Backoff is capped exponential — `REGISTRATION_BACKOFF_BASE` 2 s, doubling, `REGISTRATION_BACKOFF_CAP` 300 s — so a permanently-unconvergeable schedule costs **≤ 12 attempts/hour instead of 3600**, while every *other* schedule on the shard keeps reconciling at full cadence. A success clears the entry immediately. The public `register_schedules` / `register_workflow_schedules` (the startup contract) deliberately keep their fail-fast `?` semantics; only the tick-path `_for_shard` variants collect.

### Defect 3 — unconditional re-registration at 1 Hz

`tick_once_sharded` calls both registration passes every tick; with `unified-dag-execution` on, every DAG was reconciled twice per second by two code paths — roughly **2 million no-op `UPDATE`s/day on a 12-row table**. Both upserts now short-circuit when the resolved row already matches the desired state:

- `workflow_schedule_row_is_converged(existing, ws, now)` compares every column `apply_workflow_schedule_update` writes. The contract is "*would the write be a no-op*", which is **broader than the changeset**: the two conditional post-update statements count as writes too, so it also `&&`s in `exhaustion_reconciliation_is_noop` (mirroring the #478 bounded-runs block) and checks #360's auto-pause clear. `updated_at` is excluded by definition; `is_paused` is excluded because registration never writes it.
- `dag_schedule_row_is_converged(existing, dag, now)` mirrors `upsert_schedule`'s changeset.
- A NULL `next_run_at` is drift **only when the update's `or_else` fallback would actually fill it**. `Schedule::Manual` and an unscheduled DAG both yield `None` permanently, so a bare `is_none()` guard would mean those schedules could *never* converge — and since `register_schedules_for_shard` iterates the whole catalog, trigger-only DAGs (often most of one) would have kept rewriting every tick, leaving most of the cited write volume in place.

The skip lives in `upsert_workflow_schedule` / `upsert_schedule`, so **both** the tick path and the public/API registration paths benefit. The green-hat design point: making the converged case the *fast path* means a healthy fleet performs N cheap `SELECT`s and **zero** writes, zero transactions and zero lock acquisitions per tick — which is also what makes the Defect-4 advisory lock nearly free.

The obvious failure mode of a convergence check is a **false positive**: a row reported converged but actually drifted can never be repaired. That risk is guarded by `every_written_column_is_compared`, an anti-rot test that mutates each written column in turn and asserts the row is then reported *not* converged — so a future column added to the changeset cannot silently break convergence detection. (Writing that guard immediately caught a real subtlety: the changeset writes `ws.dag_name.or(existing.dag_name)`, so a workflow-only `ws` legitimately *preserves* an existing `dag_name` — genuinely converged, not a missed repair. `dag_name` is therefore excluded from the generic mutator list and covered by two dedicated tests instead.) The classic-DAG probe has its own mirrored guard, `every_dag_written_column_is_compared` — its changeset is smaller but carries the identical hazard, and it previously had no test at all.

The one write that escapes a changeset-shaped comparison entirely is #360's `auto_paused_at` clear, which lives *after* the changeset and does not even bump `updated_at`. A row left `(consecutive_failure_limit = NULL, auto_paused_at = set)` — reachable when those two autocommit statements are torn by a crash on the non-transactional `register_workflow_schedules` path — would otherwise be reported converged forever, leaving the schedule silently auto-paused with no log and no metric. It is now compared, and covered by a mutator in the drift guard.

### Defect 4 — per-process registration, no leader election

Every process with `scheduler_enabled` (the default) spawns a `SchedulerRuntime`, so the registration write volume scaled with the fleet. The two tick-path registration writers now wrap the **actual write** in `pg_try_advisory_xact_lock(hashtext($1)::int8)` keyed `REGISTRATION_LOCK_KEY = "harvest:schedule_registration:v1"`; a peer already reconciling causes this process to skip its duplicate write rather than queue behind it. The **one-argument** form is mandatory — the two-argument keyspace is reserved to `queue_pause` by a source-scanning guard test.

Two deliberate scoping decisions: the lock wraps only writes (the converged fast path takes no transaction and no lock at all), and it is applied **only** on the tick path — an operator API action (`POST /schedules` → `register_workflow_schedules`) is never silently skipped because a peer holds the lock.

The pre-transaction convergence probe is an *optimization*, not the decision: the authoritative check lives inside `upsert_workflow_schedule` / `upsert_schedule`, which re-resolve the row **inside the locked transaction**. So the stale-probe race — two processes both observe drift, one repairs and commits, the other then acquires the freed lock — resolves to a no-op rather than a redundant second write, and the "1× fleet write volume" property is exact rather than approximate.

### Not addressed (and why)

The issue's own "What this report does not explain" section notes the described mechanism predicts ~3600 errors/hour from *one* schedule, whereas the incident showed ~100/hour across 10+ schedule names — implying an unidentified intermittent second cause. That root cause is **not** diagnosed here. What this change guarantees is that neither the identified cause nor an unidentified one can any longer *sustain* a storm: Defect 2's backoff bounds retry volume regardless of the trigger, and Defect 3's convergence check removes the steady-state write floor.

### Test evidence

Strict RED→GREEN, verified per-defect by neutering each fix in isolation against a live Postgres 16 and re-running the suite:

- **Defect 1** (restore the `dag_name IS NULL` filter) → 2 tests fail with the issue's exact error, `duplicate key value violates unique constraint "harvest_schedules_workflow_name_unique"`.
- **Defect 2** (restore `?` propagation) → `one_unconvergeable_schedule_does_not_starve_the_rest` fails with the `Conflict` error escaping the tick.
- **Defect 3** (force the converged probes to `false`) → `a_converged_registration_pass_performs_no_writes` fails.
- **Defect 4** (force the lock acquisition to `true`) → `a_peer_holding_the_registration_lock_makes_the_pass_skip` fails.

New DB suite `autumn-harvest/tests/integration/scheduler_registration_tests.rs` (11 tests, registered in `.github/ci/integration-suites.txt` for the Docker-backed Linux run): `a_squatting_row_no_longer_wedges_the_reconciler`, `a_squatted_name_with_no_dag_row_no_longer_fails_the_insert`, `a_consistent_peer_row_is_a_conflict_not_a_steal` (asserts a typed `HarvestError::Config` naming both schedules, and specifically **not** a leaked `duplicate key` violation), `a_decoupled_registration_is_refused_rather_than_persisted` (the manufactured-victim guard, at the DB layer), `one_unconvergeable_schedule_does_not_starve_the_rest` (drives the real `tick_once`), `a_converged_registration_pass_performs_no_writes` (asserts `updated_at` is unchanged), `a_drifted_row_is_still_repaired` (the anti-false-positive guard), `a_peer_holding_the_registration_lock_makes_the_pass_skip`, `the_tick_path_also_repairs_a_squatting_row` (the squatter repair reached through `tick_once`, not just the direct registration entry point), `releasing_a_squatted_name_preserves_the_rows_other_state` (pins release-not-delete: id, `dag_name`, pause state, counters and cadence all survive), `a_failing_schedule_is_suppressed_on_the_very_next_tick` (proves the backoff is actually *wired into* the tick path, not merely unit-correct).

Pure no-DB unit tests in `scheduler.rs`: `holder_classification_truth_table`, `a_decoupled_registration_is_refused_even_when_the_name_is_free`, `registration_backoff_delay_grows_and_caps`, `backoff_suppresses_retries_until_the_delay_elapses`, `backoff_is_per_schedule_and_clears_on_success`, `backoff_escalates_on_repeated_failures`, `a_lock_skip_preserves_the_accumulated_backoff`, `a_converged_row_needs_no_write`, `every_written_column_is_compared`, `dag_name_drift_is_absorbed_for_a_workflow_only_schedule`, `dag_name_drift_forces_a_write_for_a_dag_backed_schedule`, `updated_at_alone_never_forces_a_write`, `pause_state_is_not_a_convergence_input`, `a_manual_schedule_converges_despite_a_null_next_run_at`, `a_scheduled_row_with_a_null_next_run_at_is_still_repaired`, `a_converged_dag_row_needs_no_write`, `every_dag_written_column_is_compared`, `an_unscheduled_dag_converges_despite_a_null_next_run_at`.

### Review round (findings fixed after the initial green)

Four fixes came out of a multi-angle review pass, each re-verified RED by neutering it in isolation:

1. **The `Vacant` arm manufactured its own victim** (the sharpest finding). Classifying a free name as writable *without* checking self-consistency persists exactly the corrupt row shape `Squatter` later strips — turning a loud, non-destructive storm into a silent permanent outage. Hence the `SelfInconsistent` arm now short-circuits **before** the vacant fast path.
2. **A lock skip wiped the accumulated backoff.** `Ok(())` from a skipped pass was being recorded as success, so a schedule that failed and then hit lock contention reset its penalty to zero and retried at full rate. Registration now returns a three-state `RegistrationOutcome { Settled, Skipped }`; only `Settled` clears the entry, `Skipped` is neither success nor failure.
3. **`auto_paused_at` was a convergence false positive** (see the escape-hatch note above).
4. **Manual and unscheduled schedules could never converge** — a bare `next_run_at.is_none()` drift check permanently mis-reported them, leaving most of the cited write volume in place for trigger-only catalogs.

The registration transactions are also pinned to `read_committed` explicitly rather than inheriting the session default, and `ScheduleRegistrationBackoff` was narrowed out of the crate's public re-export surface (it stays `#[doc(hidden)] pub` purely as a test seam) with its non-durability and per-process scope written into the type's own doc contract: the fleet-wide attempt rate for a stuck schedule is `N/CAP`, not `1/CAP`.
