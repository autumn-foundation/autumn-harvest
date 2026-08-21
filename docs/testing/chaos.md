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
`should_drop_notify` are a single `Relaxed` atomic load followed by an early
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

Caps (the primitive classes a *seeded* plan may pick for a point; scripted
`kill_at`/`hold_at` ignore caps and are always honoured):

- **KILL** — the point runs inside a spawned task, so a panic there simulates a
  task crash rather than crashing the test driver.
- **ERROR** — a `?`-returning (`chaos_fallible!`) site; can return an injected
  `ChaosError`.
- **DROP_NOTIFY** — a `LISTEN`/`NOTIFY` send site whose wake can be dropped.
- **DELAY** — tolerates a bounded artificial delay.

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
  `guard.hold(p)`, `handle.reached().await`, `handle.release()`.
- `error_at(p, ChaosError::Generic)` — inject an error on every hit.
- `drop_notify_at(p)` / `delay_at(p, ms)`.
- `ChaosGuard::hits(p)`, `.actions_fired()` (anti-vacuity: assert `>= 1`),
  `.seed()`, `.diagnostics()`.

A **KILL must be triggered inside a spawned task** — the harness's
`worker::chaos_drive_one_workflow_task` drives one decision cycle on its own
owned (non-pooled) connection inside `tokio::spawn`, so the panic surfaces as a
`JoinError` and the dropped connection rolls back mid-flight work server-side
exactly as a crashed worker process would.

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

# Point at an already-migrated local Postgres for fast iteration:
HARVEST_TEST_DATABASE_URL=postgres://harvest@127.0.0.1:5432/harvest_chaos \
  CHAOS_SEEDS=8 cargo test -p autumn-harvest --features chaos --test integration
```

Without `HARVEST_TEST_DATABASE_URL` the suite spins a fresh migrated Postgres 16
container per test (the CI path).

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

## Out of scope

Production/runtime chaos (#796), network-partition / Jepsen / Antithesis-style
testing, DAG what-if simulation, and *fixing* any new bug the harness surfaces —
new bugs are filed and fixed separately.
