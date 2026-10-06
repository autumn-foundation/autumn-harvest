# Chaos / fault-injection test harness (issue #940)

A deterministic, seedable fault-injection harness that lets tests inject faults —
a killed worker task, an injected Diesel/connection error, a dropped
`LISTEN`/`NOTIFY` wake, an expired lease/heartbeat — at **named points** in the
production code path, and asserts that the engine's convergence guarantees hold
under adversarial timing.

It is a **test-only** capability behind the `chaos` Cargo feature. `chaos` is off
by default and **never** part of `default`. This is *not* production/runtime
chaos (that is issue #796); the harness exists purely to reproduce and guard the
engine's internal race classes.

```toml
# autumn-harvest/Cargo.toml
[features]
chaos = ["db"]      # implies db; never in `default`
```

## Zero production impact (AC6)

When the `chaos` feature is **off** (every production build), an injection point
compiles to a `const _: ChaosPoint = points::NAME;` item that the compiler
discards — **no branch, no atomic load, no code at all** at the call site. The
hot path is untouched.

When the feature is **on** but the harness is disarmed, `hit`/`hit_fallible`/
`should_drop_notify` are a single `SeqCst` atomic load followed by an early
return — no lock, no `.await` yield.

The harness introduces **no** production semantic change: no new `WorkflowEvent`
variant, no migration, no adjacently-tagged JSON change, and no behaviour when
off.

## Two halves

- **`autumn_harvest::chaos::points`** is *unconditional* (compiled into every
  build). It is a const catalogue of named injection points. A
  `points::ChaosPoint` value can only ever be a catalogue const (its `name`
  field is private), so a **typo is a compile error, never a silent runtime
  no-op** (AC2).
- **The controller** (`arm`, `ChaosPlan`, `ChaosGuard`, `hit`, ...) is
  `#[cfg(feature = "chaos")]` — it exists only in a `chaos` build.

## Injection-point catalogue

| Point const | name | caps | Race class it guards |
|---|---|---|---|
| `QUEUE_PARK_BEFORE_UPDATE` | `queue.park.before_update` | KILL, DELAY | #601 lost-wake (`wake_requested`) — a wake landing between the pre-park check and the park's atomic `UPDATE` |
| `WORKER_PERSIST_BEFORE_COMMIT` | `worker.persist.before_commit` | KILL, DELAY | #367 poison-pill — worker death after claim but before the persist commit |
| `WORKER_AFTER_OUTER_COMMIT` | `worker.persist.after_outer_commit` | KILL, DELAY | discovery — a crash after the persist commit, before deferred-trigger fan-out |
| `OUTBOX_INLINE_AFTER_REQUESTED` | `outbox.inline.after_requested` | KILL, DELAY | #492 — an outbox sweep observing a half-written external-signal/cancel |
| `SCHED_AFTER_CLAIM` | `scheduler.after_claim` | KILL, DELAY | #350 — the claiming replica crashing mid-fire |
| `SCHED_AFTER_START_BEFORE_ADVANCE` | `scheduler.after_start.before_advance` | KILL, DELAY | #350 — double-fire on crash-recovery |
| `POISON_RECLAIM_BEFORE_LOAD` | `poison.reclaim.before_load` | ERROR | AC1(b) — a transient Diesel/connection error at the reclaim scan |
| `NOTIFY_TASK_ENQUEUED` | `notify.task_enqueued` | DROP_NOTIFY | AC1(c) — a dropped `LISTEN`/`NOTIFY` wake; dispatch must converge via the poll loop |
| `DISPATCH_AFTER_CLAIM_BEFORE_ACK` | `dispatch.after_claim.before_ack` | KILL, DELAY | #1312 — a worker death between the by-id claim commit and the reference ack |
| `WORKER_DISPATCH_BEFORE_START` | `worker.dispatch.before_start` | KILL, DELAY | #1813 — a shutdown between the claim and the task start |

Caps declare the primitive classes a point can host:

- **KILL** — the point runs inside a spawned task, so a panic there simulates a
  task crash rather than crashing the test driver.
- **ERROR** — a `?`-returning (`chaos_fallible!`) site; can return an injected
  `ChaosError`.
- **DROP_NOTIFY** — a `LISTEN`/`NOTIFY` send site whose wake can be dropped.
- **DELAY** — tolerates a bounded artificial delay.

A **seeded** plan only ever picks a cap the point declares. A **scripted** `*_at`
builder is validated the **same way, at plan-build time**: `kill_at` /
`kill_at_hit` / `hold_at` / `error_at` / `drop_notify_at` / `delay_at` assert
(via `assert_scripted_cap`) that the point's declared caps allow the action and
**panic next to the offending call** otherwise — e.g. `kill_at(NOTIFY_TASK_ENQUEUED)`
(a `DROP_NOTIFY`-only point) panics at build time rather than blowing up far away
at the entry point. `hold_at` additionally requires an **async** site — one
declaring any of `KILL`/`ERROR`/`DELAY` — because a `Hold` rendezvous parks the
point on `.await`; a synchronous `DROP_NOTIFY`-only point cannot host it (its
`should_drop_notify` is not `async`, so a scripted `Hold` there would hang the
test's `reached().await` forever).

`CHAOS_POINTS_MAX` (currently 16) is a ratchet on the catalogue size — bump it
deliberately, never silently. A source-scan drift test
(`tests/integration/chaos_catalogue_drift.rs`) asserts every point in `ALL` is
actually wired at exactly one call site in `src/`, so a catalogue entry can never
silently lose its wiring.

## How to add an injection point

1. **Add the const** to the catalogue in `src/chaos.rs` (`pub mod points`) with a
   doc comment naming the race window and the correct `caps` bitset. Use a dotted
   name (`subsystem.site.detail`).
2. **Add it to `points::ALL`** (stable order).
3. **Wire exactly one call site** in the production path with the matching macro:
   - `crate::chaos_point!(NAME);` — a plain `.await` point (KILL / DELAY / HOLD).
   - `crate::chaos_fallible!(NAME);` — a `?`-returning point that can inject a
     `ChaosError` (ERROR). Place it where the enclosing `fn` returns a `Result`
     whose error type is `From<ChaosError>`.
   - `if crate::chaos_drop_notify!(NAME) { return Ok(()); }` — a NOTIFY send site
     (DROP_NOTIFY).
4. If it exceeds the ratchet, bump `CHAOS_POINTS_MAX` in the same change.
5. Document it in the catalogue table above.

The macros expand to **nothing** but a const-type check in a non-`chaos` build,
so a wired point costs zero in production. A misspelled `NAME` is a compile
error in both build configurations.

## How to write a reproducer

Build a plan, `arm` it (this holds a process-wide serialization lock for the
guard's lifetime, so chaos runs never bleed across tests), drive the code path,
assert convergence.

```rust
// Scripted (targeted) plan:
let guard = arm(ChaosPlan::scripted().kill_at(WORKER_PERSIST_BEFORE_COMMIT)).await;
// ... drive one workflow decision cycle in a spawned task ...
assert!(outcome.is_err(), "the KILL must crash the cycle; {}", guard.diagnostics());
```

- `kill_at(p)` / `kill_at_hit(p, n)` — panic on the first / `n`th hit.
- `hold_at(p)` — a two-phase rendezvous; retrieve the handle with
  `guard.hold(p)`, `handle.reached().await`, `handle.release()`. The point must
  be an **async** site (declare `KILL`, `ERROR`, or `DELAY`); a `DROP_NOTIFY`-only
  point panics at plan-build time (see the caps section above).
- `error_at(p, ChaosError::Generic)` — inject an error on every hit.
- `drop_notify_at(p)` / `delay_at(p, ms)`.

Each `*_at` builder validates the point's declared caps at plan-build time, so a
caps/site mismatch (`kill_at` on a `DROP_NOTIFY`-only point, `error_at` on a
non-`ERROR` point, …) panics next to the call rather than silently mis-scripting
a directive that would blow up at the entry point.
- `ChaosGuard::hits(p)`, `.actions_fired()` (anti-vacuity: assert `>= 1`),
  `.seed()`, `.diagnostics()`.

A **KILL must be triggered inside a spawned task** — the harness's
`worker::chaos_drive_one_workflow_task` drives one decision cycle on its own
owned (non-pooled) connection inside `tokio::spawn`, so the panic surfaces as a
`JoinError` and the dropped connection rolls back mid-flight work server-side
exactly as a crashed worker process would.

**Await-discipline (cross-test isolation).** A reproducer **awaits the spawned
task's `JoinHandle` before dropping the guard**, so a fired fault never executes
past disarm. Isolation has two layers that together make cross-test bleed
impossible: (1) *stateful* — the entry snapshot + the armed-generation fence
decide which plan a resolved action belongs to *before* any side effect, so a
straddling task can never adopt a newer plan's directive; (2) *temporal* —
await-discipline keeps a fault's side effect within its own test. The generation
fence deliberately is not held across the side effect (a lock spanning a `Hold`
rendezvous / `Delay` sleep would deadlock the guard's `Drop`), because layer (1)
already prevents the only *stateful* bleed and a side effect that ran in the gap
would touch no global chaos state or later-test workload — a temporal overlap
with no correctness consequence.

The four historical race classes are reproduced in
`tests/integration/chaos_tests.rs`; each has an inline **RED procedure** describing
the one edit that makes it fail on the pre-fix engine shape.

## Determinism and replaying a seed (AC3)

Randomised plans (`ChaosPlan::seeded(seed)`) are derived from a `u64` seed with a
hand-rolled `splitmix64` over per-point-independent streams
(`splitmix64(seed ^ fnv1a(point_name))`). The same seed always produces the same
plan. `rand::StdRng` is deliberately **not** used — its output is not guaranteed
stable across crate versions, which would silently invalidate a recorded
reproducer seed.

Every reproducer embeds `guard.diagnostics()` (which contains the seed and the
fired-action trace) in its assert messages, so a CI failure prints exactly what
to replay. A `CHAOS_SEEDS` override is trusted verbatim (any count), so a single
printed seed replays in one command — the AC5 "≥ 5" floor is only imposed on the
*computed default*, never on an operator-chosen replay set:

```bash
# Replay a single failing seed locally:
CHAOS_SEEDS=8 cargo test -p autumn-harvest --features chaos --test integration \
  chaos_seeded_convergence_sweep -- --nocapture

# Point at an already-migrated local Postgres for fast iteration. Scope the
# run to `chaos_tests::` — the same filter CI itself uses — rather than the
# whole `integration` binary: chaos tests share a global `DB_BODY_SERIAL`
# lock over the shared database, but no *other* integration-test module
# joins that lock, so an unscoped run risks a chaos test's scrub()
# (a `TRUNCATE`) racing a concurrent, unrelated module's assertions against
# the same `HARVEST_TEST_DATABASE_URL` database:
HARVEST_TEST_DATABASE_URL=postgres://harvest@127.0.0.1:5432/harvest_chaos \
  CHAOS_SEEDS=8 cargo test -p autumn-harvest --features chaos --test integration \
  chaos_tests::
```

Without `HARVEST_TEST_DATABASE_URL` the suite spins a fresh migrated Postgres 16
container per test (the CI path).

The `infra_faults` tests always start their own containers and need Docker.
To leave them out of a fast local run, add `--skip infra_faults` after the
`--`.

## The convergence sweep (AC5)

`chaos_seeded_convergence_sweep` runs a bounded workload under
`ChaosPlan::seeded(seed)` for each seed in the resolved seed set and asserts the
**convergence invariant** after the harness is disarmed and the recovery loop
(reclaim orphans + re-drive) has run:

- every workflow reaches a terminal state (`COMPLETED`) — terminal-or-parked;
- **no** task is stranded `RUNNING` with a dead worker;
- **no** `ExternalSignalRequested` event lacks an eventual terminal.

**Workload → the disruptive point.** The sweep drives single-cycle `chaos_noop`
workflows, so it exercises the worker decision-cycle *persist* path. Only a KILL
at `worker.persist.before_commit` actually *strands an orphan* the recovery loop
must clean up: it crashes the cycle *before* the commit, while the claim's
`state = 'RUNNING'` is already durable, so it leaves a `RUNNING` row owned by a
never-registered (dead) worker — exactly the #367 recovery path the invariant
checks. A KILL at `worker.persist.after_outer_commit` is post-commit (the
execution is already `COMPLETED`) and a `Delay` merely perturbs timing — both are
*convergence-benign*. The other five catalogue points are each covered precisely
by their own dedicated reproducer above; a parking / external-signal / scheduler
workload can't be folded into this one sweep because a *seeded* plan never
selects `Hold` and delivers no signals, so a parked workflow would never reach
`COMPLETED`.

**Computed, orphan-stranding default seed set.** The default set is **computed**,
not a hardcoded list: it is the first *N* (currently 7, ≥ 5 for AC5) seeds from 1
upward whose seeded plan arms a **KILL** at `worker.persist.before_commit`
(`default_sweep_seeds()` / `seed_strands_an_orphan()`). Requiring a *disruptive*
pre-commit crash — not merely "any activation at a reachable point," which a
convergence-benign `Delay` or a post-commit kill would satisfy — keeps the default
non-vacuous *by construction* (review P2-1), while staying fully deterministic. A
no-DB unit test (`default_sweep_seeds_are_at_least_five_and_strand_an_orphan`)
pins the ≥ 5 / distinct / orphan-stranding properties. (With today's catalogue
and seeded logic the computed set is `[8, 13, 14, 15, 20, 25, 33]`.)

**Anti-vacuity (two layers).** Per seed the sweep asserts (1) at least one honored
fault fired (`guard.actions_fired() >= 1`), and (2) for an orphan-stranding seed —
every default seed, and any override that strands one — that a task really was left
`RUNNING` with a dead worker **before** the recovery loop ran. Layer (2) is the
direct proof: it shows the recovery loop had real work to reclaim (which the final
post-recovery `stranded == 0` assert then exercises), not just that some
possibly-benign directive fired. `ChaosPlan::seeded` is a pure function of the seed,
so both are deterministic — a vacuous seed fails loudly, naming itself for replay,
rather than passing convergence for a healthy, un-faulted run. A hand-picked
`CHAOS_SEEDS` override still must fire a fault (layer 1); it is additionally held to
the orphan proof (layer 2) only when it happens to arm the disruptive KILL, so
single-seed replay of any operator-chosen seed (AC3) is never blocked.

The CI job (`.github/workflows/chaos.yml`) runs the suite on `workflow_dispatch`
and on a nightly `cron`. The cron leaves `CHAOS_SEEDS` empty so the sweep uses
its computed default (≥ 5 seeds per run, AC5); a manual dispatch can supply
explicit seeds to replay a printed failure.

### Nightly watchdog (issue #1790)

Before the fix for issue #1790, `chaos.yml` did not parse, so the nightly never
ran. Two checks now catch a repeat:

- `ci_run_coverage` parses every workflow file. It also checks that `chaos.yml`
  has a cron and runs `chaos_tests::` with the `chaos` feature. An `if` or a
  `continue-on-error: true` on that step or its job fails the check. In the
  step's `run:` text, a flag such as `--no-run` or `--skip` fails it too. The
  step must be one plain `cargo test` command, so `|| true`, `; true` or an
  `echo` of the arguments also fails it.
- `.github/workflows/chaos-watchdog.yml` runs daily at 10:41 UTC. It runs
  `.github/ci/chaos-watchdog.sh`. When no scheduled `chaos.yml` run succeeds in
  a 48-hour window, the script opens an issue with this title:
  `Chaos nightly: no successful scheduled run in 48 h`.
  While the gap continues, the script adds a comment to that issue. After the
  next success, it closes the issue.

The watchdog is a separate file, so a defect in `chaos.yml` cannot also stop the
alert. An API error makes the watchdog run fail. It does not open a false alert.
A final `if: failure()` step then runs `chaos-watchdog.sh self-failed`. It opens
an issue titled `Chaos watchdog: a watchdog run failed`, or comments on the open
one. GitHub tells only the last editor of a cron about a failed scheduled run, so
without this step the failure is silent. The next clean watchdog run closes that
issue. Each step has its own timeout, and the step timeouts sum to less than the
job timeout. A step timeout is a failure, so the report step still runs, also
after a hung checkout. When checkout fails, the report step opens the
issue without the script, or comments on the open one. A concurrency group runs
one watchdog job at a time, so two runs cannot both open an issue.

## Infrastructure faults (issue #1801)

A chaos KILL is a panic in one tokio task. The tests in
`chaos_tests::infra_faults` inject faults below the engine instead. They are a
submodule of `chaos_tests`, so the nightly `chaos_tests::` step runs them, and
the watchdog alerts on a failure. The module compiles on Unix only.

Each test starts its own Postgres 16 container and a toxiproxy container on a
private docker network. Workers connect through the `worker` proxy. The test
reads state through the `admin` proxy, which never gets a toxic. A Postgres
restart therefore does not change any URL. These tests ignore
`HARVEST_TEST_DATABASE_URL`, because a restart or a pause cannot target a
shared database.

Workers use a 500 ms heartbeat, so the lease TTL (the stale threshold) is 1 s.
The latency test is the exception, see the known bugs below.

| Scenario | Test | Fault | Proof the fault landed |
|---|---|---|---|
| Kill mid-commit | `terminate_backend_mid_commit_append` | `pg_terminate_backend` while COMMIT of an `ActivityCompleted` insert waits; the transaction rolls back | the blocked backend exits |
| | `terminate_backend_mid_commit_complete` | the same, for a `WorkflowCompleted` insert | the blocked backend exits |
| | `terminate_backend_mid_commit_claim` | the same, for the first `PENDING` to `RUNNING` claim | the blocked backend exits |
| Lost COMMIT ack | `terminate_backend_after_commit_ack_lost_append` | a downstream blackhole drops the replies; the COMMIT lands; then `pg_terminate_backend` | the backend goes idle, then exits |
| | `terminate_backend_after_commit_ack_lost_complete` | the same, for a `WorkflowCompleted` insert | the backend goes idle, then exits |
| | `terminate_backend_after_commit_ack_lost_claim` | the same, for the first claim | the backend goes idle, then exits |
| Restart | `postgres_crash_restart_mid_workload` | stop Postgres with no grace period, then start it | an activity runs at the crash |
| Pause | `postgres_pause_longer_than_lease_ttl` | `docker pause` for 4 s, two workers | activities run at the pause |
| Latency | `toxiproxy_latency_between_worker_and_db` | 100 ms ± 50 ms in each direction | a probe round trip is slow |
| Partition | `toxiproxy_partition_longer_than_lease_ttl` | blackhole worker A for at least 4 s, until worker B holds every task | B reclaims all three tasks; the stale results of A are rejected |
| SIGKILL | `sigkill_child_worker_mid_activity` | SIGKILL of a worker that runs as a child process | the child dies by signal 9; its task is reclaimed |

**The COMMIT rendezvous.** A test-only `DEFERRABLE INITIALLY DEFERRED`
constraint trigger runs inside COMMIT. It waits on a shared advisory lock that
the test holds. The test finds the waiting backend in `pg_locks`. For a
rollback, it terminates the backend, waits up to 10 s for the exit, and then
releases the lock. For a lost acknowledgement, it first blackholes the
replies, then releases the lock. The COMMIT lands, and the test terminates the
idle backend. Either way the kill lands after COMMIT is sent and before the
worker reads a reply. No production code changes.

**The partition test.** Attempt 1 of each activity waits on worker A. Worker
B reclaims the tasks, and its attempt 2 waits too. After the heal, the test
releases attempt 1, and A writes three stale results. No result exists yet, so
only the claim fence can reject them, and the test asserts that no
`ActivityCompleted` exists. Then the test releases attempt 2, and B finishes
the workflows. With the fence disabled, the test fails.

**The oracle.** Every test checks `assert_converged`, the oracle of the
convergence sweep. It requires every workflow `COMPLETED`, exactly one terminal
event per execution, no stranded `RUNNING` task, and no dangling
`ExternalSignalRequested`. The table key does not stop a second terminal event
at a new event id, so the oracle counts terminal events itself.
`oracle_flags_a_duplicate_terminal_event` forges a duplicate to prove the
check. Each activity workflow must also have exactly one activity terminal
event, an `ActivityCompleted`.

**Known bugs.** The tests found four bugs:

- #1871: the worker did not retry the activity result write after a DB
  error. The result was lost, and only `start_to_close` recovered the task.
  #1788 fixed #1871 for a session that the server ends. The worker now
  writes the result again, up to 10 times, and gets a new connection after
  a lost one.
- #1870: a `StartToClose` timeout ignored the retry policy and failed the
  workflow. #1809 and #1870 fixed it. A timeout with attempts left now starts
  a new attempt.
- #1876: Postgres keeps the open transaction of a partitioned worker until
  TCP keepalive ends the session. That transaction keeps its row locks.
  Orphan reclaim blocked on such a lock, so no orphan was reclaimed. The
  #1876 fix makes reclaim skip a locked row and reclaim the other rows.
- #1879: the heartbeat period is the interval plus the tick latency. When a
  tick takes longer than one interval, a live worker looks dead. False
  reclaims then count crash strikes and quarantine healthy work.

Each test works around a bug only where the bug applies:

- Both append tests require `COMPLETED`, with one attempt of the activity.
  The rolled-back test pins the #1871 fix: the worker writes the result
  again, and the handler does not run again. With the repeat turned off, the
  worker gives the claim back and a second attempt runs. Only the attempt
  check fails. In the ack-lost test, the commit landed, so the repeat must
  change nothing.
- A result write reaches the `StartToClose` timeout when its repeats and
  the claim give-back all fail. A crash restart can cause that. The retry
  policy then starts a new attempt (#1870), so the restart test requires
  `COMPLETED` for every workflow.
- The partition test sets `idle_in_transaction_session_timeout = 5s` on the
  server. This setting is no longer a #1876 workaround. A row that the
  cut-off session locks stays locked until the session ends. The 5 s limit
  ends that session within the test budget, so the setting stays.
- For #1879, the latency test uses a 2 s heartbeat. A decision cycle also
  takes more than 10 s at that latency, so it keeps the default 60 s
  workflow-task budget.

When a fix for a bug merges, remove its workaround.

**Replay.** Run one test locally with Docker:

```bash
cargo test -p autumn-harvest --features chaos --test integration \
  chaos_tests::infra_faults::toxiproxy_partition_longer_than_lease_ttl -- --nocapture
```

## History checks (issue #1829)

Two chaos reproducers and a crash suite record client histories and check
them for linearizability. `tests/integration/history_checker.rs` holds the checker.
It follows Porcupine: a search for one real-time order that a sequential
model accepts, done per key. An operation with no clear outcome after a
crash is an `info` operation, as in Jepsen. It may or may not have taken
effect.

- `chaos_repro_350_crashed_fire_claim_is_refired_exactly_once` and
  `chaos_repro_350_post_start_crash_dedupes_to_exactly_one` record their
  ticks and reads. The history must satisfy the `ExactlyOnceFire` model.
- `tests/integration/history_crash_tests.rs` runs concurrent clients while
  it drops request futures and calls `pg_terminate_backend` at random.
  Idempotent starts must satisfy `StartIdempotency`. Scheduler replicas
  must satisfy `ExactlyOnceFire`. The suite needs only the `db` feature, so
  the `test-db-linux` job runs it on each change. That job is not a required
  check yet. Set `HISTORY_SEED` to replay the random choices of a printed
  seed. The thread timing can still differ.

A crashed operation has no known outcome, so it may take effect at any
later time. When the test knows that nothing a crash started can still run,
it calls `Recorder::bound_open_infos`. A later read then constrains the
crashed operations. Each suite calls it after the server has no session of
the test left. The chaos reproducers call it after the killed task joins.

The convergence sweep does not record a history. It drives workflow tasks,
not starts or schedule fires.

The checker self-tests feed it forged violations: two creators, a read of
two runs, a real-time inversion, a lost fire, a replaced run, and a crashed
fire that takes effect after its bound.

## Out of scope

Production/runtime chaos (#796), Antithesis-style deterministic simulation, DAG
what-if simulation, and *fixing* any new bug the harness surfaces — new bugs
are filed and fixed separately.
