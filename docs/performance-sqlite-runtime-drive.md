# `SqliteRuntime` decision-cycle drive: full-history reload dominates cost, no local fix clears the floor

**Outcome: findings only — no production code changed.** This pass built the
first CPU/allocation-count profiling harness for `autumn-harvest-sqlite`
(previously zero benchmark/profiling infrastructure existed for this crate),
profiled a realistic end-to-end workflow drive, and traced the dominant cost
to a specific, source-confirmed mechanism: **every decision cycle reloads and
re-deserializes the entire event history from scratch**, with no warm-cache
or delta-load path — a gap the Postgres backend already closed (issue #235).
The mechanism (reload + reparse + the subsequent by-value clone) accounts
for **70.25% of allocation bytes** and a clear, super-linear (trending toward
quadratic) instruction-count scaling curve on this workload — but only
**~40%** of that (the reload + reparse itself) is addressable by a fix; the
remaining ~30% is a related, architecturally separate clone cost that the
same fix does not eliminate (see "Allocation-site attribution" and
"Recommendation" below for the precise split). No candidate local fix
(avoiding one intermediate `String` allocation; caching a small
`SELECT MAX(seq)` point query) clears this agent's autonomous floor (≥5%
instruction reduction / ≥10% allocation reduction). The fix that would
address the reload/reparse share — an in-process warm cache with
delta-append, mirroring the Postgres backend's shipped `cache.rs` /
`store::load_history_since` (issue #235) — is an architectural change to
`SqliteRuntime`'s internal state and public construction shape, and is
reported here for a human decision rather than implemented unilaterally; it
would shrink the per-cycle constant but not the `O(n²)` complexity class.

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/byte total
(`valgrind --tool=dhat`), reproducible run-to-run to within noise well under
0.01% (see "Reproduce" below for the measured figure) — unlike wall-clock
timing, which on this machine varies by double-digit percentages between
otherwise-identical runs.

## Workload

`benches/runtime_drive_profile.rs` (new, `harness = false` with its own
`main()` — no criterion wall-clock loop, mirroring
`autumn-harvest/benches/replay_profile.rs`'s convention exactly) drives a
`sequential_workflow` workflow through `SqliteRuntime::run_until_blocked`
end-to-end: `open_in_memory` → register → `start_workflow` → repeated
decision cycles → completion. The workflow calls `ctx.execute_activity_raw`
`n` times sequentially, each carrying a realistic ~230-byte JSON payload
(`activity_payload`, an order line item with a nested `items` array —
copied verbatim from `autumn-harvest/benches/replay_profile_support.rs` so
the shape matches the existing Postgres-side replay harness) as both the
scheduled input and the completed output.

This is deliberately **not** a single-cycle replay slice. Unlike
`replay_profile.rs` (which calls `WorkflowReplayer::replay_from_events` once
over a pre-built history), this harness drives the *whole run* — every
decision cycle the SQLite backend's single-writer, poll-driven architecture
actually performs to complete an `n`-activity workflow. That is **`n + 1`
cycles, not `n`** (confirmed both by reading
`SqliteRuntime::drive_one_cycle`/`drive_suspension` and by empirically
instrumenting a cycle counter for `n = 1, 2, 3, 5, 10`, which returned
`2, 3, 4, 6, 11` respectively): cycles `1..n` each do one
`apply_commands` + `drain_ready` pass that schedules **and** synchronously
completes one activity inline (this workload's `ActivitySpec` is a
synchronous `Ok(input)` closure, so scheduling and completion land in the
*same* cycle), but the workflow function does not observe activity `n`'s
completion — and therefore cannot fall off the end of its loop and return —
until it replays *again* on a fresh reload. That extra, `(n + 1)`-th cycle
reloads the full, now-complete history (the largest reload in the entire
run — larger than any of the `n` work-performing cycles, none of which ever
see the full final-size history) purely to discover there is no more work
and persist `WorkflowCompleted`. That is the realistic full-run cost a
production embedder pays, not an isolated function call.

`SQLITE_RUNTIME_PROFILE_N` (default `100`) sets the activity count.
`SQLITE_RUNTIME_PROFILE_REPS` (default `1`) repeats the whole
register+start+drive cycle against a fresh in-memory database.

## Profile

```
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out <runtime_drive_profile binary>
callgrind_annotate --threshold=100 callgrind.out
```

### Instruction-count scaling (the asymptotic argument)

| n (activities) | Ir (whole process) | Ir / activity | Per-doubling ratio |
|---:|---:|---:|---:|
| 20  | 34,059,018    | 1,702,951 | — |
| 40  | 98,928,342    | 2,473,209 | 2.905× |
| 80  | 330,706,853   | 4,133,836 | 3.343× |
| 160 | 1,202,152,343 | 7,513,452 | 3.635× |

A doubling of `n` under pure linear (O(n)) cost would produce a 2.0× ratio at
every step; under pure quadratic (O(n²)) cost, a flat 4.0× ratio. The
measured ratios climb monotonically — 2.905× → 3.343× → 3.635× — trending
toward, but not yet at, 4.0×. The "cost per activity" column makes the same
point directly: it should be *constant* under O(n) total cost, but it grows
4.4× (1,702,951 → 7,513,452) across an 8× increase in `n`. This is
super-linear scaling with a clearly super-linear trend, corroborated across
four independent input sizes as required by this agent's evidence rules for
an asymptotic argument (no wall-clock timing was used to produce any of these
numbers).

### Instruction-count flat profile at n=80 (mechanism attribution)

Grouping `callgrind_annotate --threshold=100`'s flat (self-cost) output by
mechanism, out of 330,706,853 total instructions:

| Mechanism | Ir | % of total |
|---|---:|---:|
| malloc/free (glibc allocator) | 128,905,230 | 38.98% |
| `serde_json` parse/deserialize | 57,070,631 | 17.26% |
| `BTreeMap` (`Value::Object` is `BTreeMap`-backed — no `preserve_order`) | 36,763,184 | 11.12% |
| sqlite3 SQL execution/parsing (`VdbeExec`, `RunParser`, `yy_reduce`) | 21,901,015 | 6.62% |
| `libc` `memcpy`/`memcmp` (generic, driven by the above) | 16,038,170 | 4.85% |
| string formatting (`format!`/`Display`/`fmt::Write` glue) | 4,193,922 | 1.27% |
| `String::clone` | 3,939,441 | 1.19% |
| `Value` `drop_in_place` | 3,915,951 | 1.18% |
| `sequential_workflow`'s own re-execution (name/payload `format!` rebuild) | 2,358,617 | 0.71% |
| `uuid::parser::try_parse` | 1,574,400 | 0.48% |
| `Vec::clone` | 1,192,305 | 0.36% |
| **Sum of the above** | **277,852,866** | **84.02%** |

The target mechanism (history reload/reparse/clone; see Hypothesis below) is
not a minor contributor sitting under this agent's 5% "stop, it's inherent"
threshold — it is the large majority of instructions in this profile, spread
across allocator, JSON-parse, and `BTreeMap` machinery because a JSON
`serde_json::Value` deserialize necessarily drives all three.

### Allocation-count/byte scaling (`dhat`)

```
valgrind --tool=dhat --dhat-out-file=dhat.json <runtime_drive_profile binary>
```

| n | Total bytes | Total blocks | Byte ratio | Block ratio |
|---:|---:|---:|---:|---:|
| 20 | 4,863,196  | 30,033  | — | — |
| 40 | 14,259,658 | 105,648 | 2.932× | 3.518× |
| 80 | 48,293,750 | 397,344 | 3.387× | 3.761× |

Same super-linear signature as the instruction counts, trending toward the
4.0× quadratic ratio, and — because allocation *block count* (not just bytes)
climbs even faster than bytes — this is not merely "each event's payload got
bigger to parse", it is genuinely more individual allocations happening per
activity as `n` grows.

### Allocation-site attribution at n=80 (mechanism breakdown)

Every one of `dhat`'s 397,344 allocated blocks / 48,293,750 bytes was
categorized by walking its full call-stack (`ftbl`/`pps` in the `dhat.json`
output) for a small set of mutually-exclusive frame markers, via the
committed, auditable
`autumn-harvest-sqlite/scripts/classify_dhat_allocations.py` — re-running it
against a freshly captured `dhat.json` (see "Reproduce" below) reproduces
the table exactly; its module docstring specifies the full precedence order
and the rationale for each category boundary.

**Methodology note — DHAT's default stack-capture depth silently
misattributes deep recursive-JSON allocations.** `valgrind --tool=dhat`
records 12 caller frames per allocation site by default
(`--num-callers=12`). `serde_json::Value::deserialize`'s recursive descent
through this workload's nested `items` array is deep enough that, at that
default depth, a large share of `store::load_history`'s own per-event
deserialize allocations have their `store::load_history` ancestor frame
truncated off the recorded stack entirely — indistinguishable from a
`serde_json` deserialize call happening anywhere else in the program. A
first pass at the default depth put 17.91% (8,647,588 bytes) of all
allocation bytes in an "elsewhere / unattributed `serde_json` deserialize"
bucket. Re-running with `--num-callers=30` (deep enough to reach past the
JSON recursion to the real Rust caller in every case observed) reclassifies
the overwhelming majority of that bucket into `load_history`'s own
deserialize cost, collapsing the genuinely-unrelated "elsewhere" total to
**173,732 bytes (0.36%)** — which the categorizer script attributes to two
distinct, genuinely-unrelated call sites
(`queue::claim_next_ready_task_tx`: 171,587 bytes; `store::execution_output`:
2,145 bytes; run the script yourself to see this split, per "Reproduce"
below). The table below, and every other allocation-byte figure in this
document, uses the corrected 30-frame-deep categorization.

| Category | Bytes | % of total | Blocks |
|---|---:|---:|---:|
| `history.clone()` (`BTreeMap`/`Vec` clone before the by-value executor call) | 14,644,790 | 30.32% | 142,662 |
| `store::load_history`'s own `serde_json` deserialize (JSON reparse) | 13,955,181 | 28.90% | 142,560 |
| `sequential_workflow`'s own re-execution (`activity_payload(i)` + name rebuild) | 7,264,465 | 15.04% | 86,880 |
| sqlite3 internals from call sites OTHER than the reload query (task claiming, activity enqueueing, timer bookkeeping — see Methodology note below) | 6,014,584 | 12.45% | 5,691 |
| `store::load_history`'s own `Vec<WorkflowEvent>` growth + query execution | 5,322,770 | 11.02% | 7,167 |
| other/uncategorized (tokio runtime setup/teardown, rusqlite plumbing not already attributed to `load_history`, UUID-to-string formatting, small `store`/`queue`/`worker`/`context` glue functions, one-time process startup — see the script's docstring for why this is a real bucket, not an under-specified rule) | 580,582 | 1.20% | 7,291 |
| `activity_input_clone` — a bounded, per-activity clone of one activity's input `Value` (schedule-time + drain-time), distinct from `history.clone()` (see Methodology note below) | 332,934 | 0.69% | 3,360 |
| `serde_json` deserialize, genuinely elsewhere (2 unrelated call sites, see above) | 173,732 | 0.36% | 1,701 |
| sqlite3 internals actually reachable from the reload query (see Methodology note below) | 4,712 | 0.01% | 32 |

Four categories sum to **70.25%** of all allocation bytes in this workload
(48,293,750 bytes total), but they split into two *architecturally distinct*
costs, not one:

- **The reload + reparse itself** — `load_history`'s own Vec growth + query
  execution (11.02%) + its own `serde_json` deserialize (28.90%) + the
  sliver of sqlite3 C-engine work actually reachable from that query
  (0.01%) = **39.93%**. This is directly caused by, and fully addressable
  by fixing, the single design choice traced below (every decision cycle
  re-reads and re-deserializes the full history from scratch).
- **The `history.clone()`** (30.32%) — a *related but separate* cost. It is
  triggered today by the reload producing a fresh `Vec` that must then be
  cloned for the by-value executor call, but the clone itself would still
  happen against a *cached* history too: `drive_one_cycle` needs the
  original `history` value again after the executor call regardless of
  where that `Vec` came from (see the "Recommendation" section below for why
  this specific 30.32% is **not** eliminated by the proposed cache).

So while today's implementation makes both costs scale together (70.25%
combined), only the 39.93% reload/reparse share is what the recommended
fix below addresses. The fifth category, `sequential_workflow`'s own
re-execution (15.04%), is a **separate, inherent cost of the
deterministic-replay execution model itself** (present in the Postgres
backend too — see "What isn't the finding" below), not part of this
finding's recommendation.

**Methodology note — sqlite3 C-engine work is dominated by call sites OTHER
than the history reload.** An earlier pass at this table folded the whole
12.45% `sqlite3_internals` bucket into the "reload + reparse" aggregate on
the assumption it represented "sqlite3 internals executing the reload
query." Checking whether `store::load_history` is ALSO on the stack for
every sqlite3-C-frame allocation shows that assumption was wrong: only
4,712 bytes (0.01%, the separate row above) of sqlite3 C-engine work is
reachable from the reload query at all. The remaining 6,014,584 bytes
(12.45%) come from other call sites entirely — task claiming
(`queue::claim_next_ready_task_tx`), activity enqueueing
(`queue::enqueue_activity`), and timer bookkeeping — none of which the
recommended cache below would touch. `sqlite3_internals` is therefore its
own, third, cost bucket (no smaller a share of the total than
`load_history_vec_query` itself), orthogonal to the reload/reparse story
this document is about; it is neither part of the addressable 39.93% nor
part of the `history.clone()` 30.32%.

**Methodology note — the `history.clone()` bucket is not purely the
full-history clone.** The same generic `Vec`/`BTreeMap`/`String` clone
markers that identify `drive_one_cycle`'s full-history `.clone()` (see
"Hypothesis" below) also match two smaller, architecturally distinct clone
call sites: `persist_scheduled_activity` clones an activity's input
`serde_json::Value` once, at SCHEDULE time, to build the durable
`ActivityScheduled` event; `worker::drain_ready` clones the same input a
second time, at DRAIN time, immediately before invoking the registered
activity body closure. Both are bounded — happening exactly once per
activity dispatched, not once per decision cycle — so they are not part of
the "N decision cycles reload the whole history" story this document is
about; an earlier draft of the categorizer lumped them into `history.clone()`
regardless. They are discriminated by checking whether `drive_suspension`
(where the schedule-time clone is inlined through) or `drain_ready` (where
the drain-time clone happens directly) is on the clone's call stack — both
are absent from `drive_one_cycle`'s own clone, since `drive_one_cycle` is
fully inlined into `run_until_blocked::{{closure}}`, its sole caller — and
bucketed separately as `activity_input_clone` (332,934 bytes, 0.69%). The
remaining 14,644,790 bytes (30.32%) is the genuine, per-decision-cycle
full-history clone.

## Hypothesis (source-confirmed, not inferred from the number alone)

`SqliteRuntime::drive_one_cycle` (`autumn-harvest-sqlite/src/runtime.rs`)
begins every decision cycle with:

```rust
let history = store::load_history(&self.conn, exec)?;
// ...
let (outcome, pending, _span) = run_workflow_with_state_history_policy_and_caps(
    exec,
    history.clone(),   // consumed by value; `history` is needed again below
    handler, input, /* ... */
).await;
```

`store::load_history` (`autumn-harvest-sqlite/src/store.rs`) runs
`SELECT event_json FROM harvest_events WHERE exec_id = ?1 ORDER BY seq`
(via `prepare_cached`, so the SQL statement itself is cached) and, for
**every** row, materializes an owned `String` (`rusqlite::row::Row::get::<_,
String>`) before parsing it with `serde_json::from_str` into a
`WorkflowEvent`, collecting the whole result into a fresh `Vec`.

For an `n`-activity sequential workflow, cycle `k` (of `n + 1` total cycles,
see "Workload" above) BEGINS with `O(2k - 1)` accumulated events
(`WorkflowStarted` plus an `ActivityScheduled`/`ActivityCompleted` pair for
each of the `k - 1` already-completed activities) for `k = 1..n`, and the
final, `(n + 1)`-th cycle begins with `O(2n + 1)` events — the largest
reload in the run, since it is the only cycle that ever sees all `n`
activities' events at once. `drive_one_cycle` reloads and fully re-parses
**all** of them on every cycle, not just the delta since the previous one —
so total work across the run is `Σ(k=1..n) O(2k - 1) + O(2n + 1) = O(n²) +
O(n) = O(n²)`, exactly the trend the four-point Ir/dhat scaling curves above
show empirically. The extra `+1` cycle is strictly the single largest
individual reload in the run (`2n + 1` events vs. at most `2n - 1` for any
of the `n` work cycles), but its contribution to the *total* — one `O(n)`
term against a `Σ(k=1..n) O(2k - 1) = n²` sum — shrinks as a *fraction* of
total reload volume as `n` grows (2.45% of the 6,561 total events reloaded
at `n = 80`): the quadratic sum of the `n` work cycles, not the one extra
cycle, is what dominates at scale. The `.clone()` on top is required
because `run_workflow_with_state_history_policy_and_caps`'s signature takes
`history: Vec<WorkflowEvent>` by value (confirmed in
`autumn-harvest/src/executor.rs`), while `drive_one_cycle` needs the
pre-call `history` again in its `WorkflowOutcome::Suspended` branch
(`drive_suspension` → `apply_commands`/`activity_race_in_flight`/
`pending_timer_arms`/`history_has_activity_scheduled`, all of which take
`&history` for idempotency/defensive-reapplication checks on the *next*
suspension).

**This is not unique to the SQLite backend in isolation.** The core
executor's by-value `history` parameter forces the identical clone-before-
consuming-call pattern in the Postgres worker too (`autumn-harvest/src/worker.rs`,
`history_events.clone()` at the equivalent call site) — eliminating the clone
itself is not a local, SQLite-only fix; it would require a core-crate
signature change (e.g. `Arc`-wrapping `history`) that ripples across both
backends, which is exactly the "architectural change: public API shape"
category this agent must ask before making.

**What the Postgres backend has that this one doesn't** is the *reload*
mitigation, not the clone-avoidance: issue #235 (`autumn-harvest/src/cache.rs`,
`WorkflowCache`, `store::load_history_since`) lets a same-worker cache hit
load only the *delta* of new events since the last suspension, rather than
the full history, on every cycle. The SQLite backend has no equivalent
mechanism at all — every cycle is a guaranteed full reload, with no
mitigating cache path even in principle today.

Notably, the SQLite backend's single-writer, single-process design removes
one entire failure mode Postgres's cache has to reason about: Postgres's
cache can miss on a genuine cross-worker handoff (sticky routing can route
a follow-up task to a different worker with a cold cache), but
`SqliteRuntime` is one process driving one file — absent a restart (see
below), every decision cycle for a given execution is, by construction,
driven by the *same* runtime instance that drove the previous one, so
**staleness** — a cache entry that is present but out of date relative to a
fresher copy held elsewhere — can never be the cause of a miss here, unlike
Postgres. A restart instead produces a *cold* miss (no cache entry at all,
in a brand-new instance) — a structurally different failure mode, covered
next.

This does **not** imply a 100% hit rate is free, though — the Postgres cache
being mirrored (`cache.rs::WorkflowCache`) is a **bounded, fixed-capacity
LRU**, chosen specifically to cap memory: retaining every in-flight
execution's *full* history unboundedly is a real memory cost, not a free
lunch. If `SqliteRuntime` interleaves more concurrently-blocked executions
than a bounded cache's capacity (e.g. under `run_until_idle` driving several
executions to completion in round-robin), the LRU can evict an execution's
entry before its next decision cycle — a genuine cache miss, purely from
capacity pressure, with no cross-worker staleness involved at all.

A third condition is structurally distinct from capacity tuning entirely:
the cache would live inside the `SqliteRuntime` process instance, and this
backend explicitly supports — and demonstrates, in
`examples/durability.rs:120-153` — dropping that instance mid-flight (a
simulated crash/restart) and later reopening the *same* on-disk file to
resume a workflow parked on a durable signal wait purely by replaying its
committed history. Registrations and any in-process cache are process
state, not durable state, so neither survives a restart; the first decision
cycle after every reopen therefore runs against a fresh, empty cache and
must cold-load the full history regardless of the cache's capacity or
eviction policy — no capacity/eviction choice removes this. So a 100% hit
rate requires *all of*: an unbounded cache (trading memory for hit rate —
plausible for this crate's stated edge/local-first scope, where concurrent
in-flight execution counts are typically small) *or* a capacity sized to
the workload's actual concurrency, *and* no restart occurring between an
execution's decision cycles — the capacity/eviction-policy choice (left
open above) plus this additional, unavoidable per-restart cold-load cost
for restart-heavy or intermittently-resumed workloads, both left for a
maintainer to weigh alongside the rest of this architectural change.

## Why no local fix clears the floor

Two smaller, local-only candidates were identified and quantified precisely
from the same `dhat` data — neither clears this agent's autonomous floor
(≥5% instruction reduction on a benchmark representing ≥5% of workload cost,
or ≥10% allocation reduction):

1. **Avoid the intermediate owned `String` in `load_history`'s row mapping**
   (`row.get::<_, String>(0)` → `row.get_ref(0)?.as_str()?`, parsing
   `serde_json::from_str` directly from the borrowed `&str` returned by
   SQLite rather than an owned copy). Isolated precisely via the `dhat`
   call-stack data: the `rusqlite::row::Row::get <- store::load_history`
   allocation site is **2,068,754 bytes (4.28% of total) / 6,561 blocks
   (1.65% of total)** at n=80 — below both the 5%/10% floors on its own, and
   it does nothing for the dominant instruction cost (parsing a borrowed
   `&str` still touches every byte of the JSON; only the allocation, not the
   scan/lex work, would be avoided).

2. **Cache the small point queries** (`store::next_seq`'s per-append
   `SELECT MAX(seq)`, and several other `execution_input`/`workflow_name_of`/
   `workflow_id_of`-style plain `query_row` calls that don't use
   `prepare_cached`). The sqlite3 SQL-parsing subset of the flat profile
   (`sqlite3RunParser` + `yy_reduce.isra.0`) is 3,909,343 + 3,088,300 =
   6,997,643 Ir at n=80 — 2.12% of total, below the 5% floor. This cost is
   also **linear** in the number of decision cycles, while total cost is
   super-linear — so as a fraction of total cost it can only shrink further
   at larger, more realistic `n`, not grow toward the floor.

Both candidates were left unimplemented; a change that cannot clear the
floor is not opened as a PR per this agent's mandate (a below-floor local
change would need to be reverted, and reverting an isolated no-op diff would
add no value here — the harness itself, which is the useful long-lived
artifact, has no production-code footprint to revert).

## What isn't the finding

The `sequential_workflow` re-execution category (15.04% of allocation bytes
at n=80) is **not** part of this recommendation. It is `activity_payload(i)`
being rebuilt as a fresh `json!()` value every time the workflow function
replays from the top and loops past an already-completed `i` — the
deterministic-replay execution model's documented, load-bearing property
(CLAUDE.md: "the workflow author's surrounding Rust code re-executes on
every replay cycle even for already-completed calls; only the actual
dispatch is skipped"). This is shared by the Postgres backend's identical
execution model and by design — a workflow author's surrounding computation
around an already-matched `ctx.execute_activity_raw` call is expected to
re-run; fixing it would mean changing how workflow functions are invoked
during replay, not a SQLite-backend persistence detail. It is called out
here only so the categorized breakdown adds up to a legible whole, not as
something this finding proposes changing.

## Recommendation (requires a human decision — not implemented here)

Give `SqliteRuntime` an in-process warm cache keyed by `ExecutionId`, holding
`(events: Vec<WorkflowEvent>, next_seq: i64)`, updated in place after each
decision cycle's commit rather than reloaded from scratch, plus a
`store::load_history_since(conn, exec, since_seq)` query mirroring the
Postgres backend's `load_history_since` (issue #235) for the (rare, only
needed for consistency/defensive re-verification) case where a delta reload
is still wanted. This would turn the per-cycle **SQL query + row parse +
`serde_json::from_str` deserialize** cost — `load_history`'s own Vec growth
and query execution (11.02%) + its own serde_json deserialize (28.90%) +
the sliver of sqlite3 C-engine work reachable from that query (0.01%) =
~39.93% of allocation bytes at n=80, and the corresponding instruction
share — from `O(k)` (reloading and reparsing the
full history every cycle) into
`O(1)` amortized (only the new events since the last cycle touch SQL or
`serde_json` at all).

**This substantially shrinks the constant factor but does not change the
`O(n²)` complexity class.** Two costs are `O(k)` per decision cycle
regardless of caching, and neither is addressed by this recommendation:
`drive_one_cycle` still `.clone()`s the *entire* accumulated
`Vec<WorkflowEvent>` before handing it, by value, to the core executor
(30.32% of allocation bytes at n=80 — a cache holding the full history
in-process still has to clone it out for that by-value call); and the
workflow function itself still replays every prior activity call from the
top on every cycle (the 15.04% "what isn't the finding" cost above). Both
are inherent to a from-the-top-replay execution model, independent of where
the history data lives. So `Σ(k=1..n) O(2k - 1) + O(2n + 1)` (see
"Hypothesis" above for the `n + 1`-cycle derivation) — the `O(n²)` total-run
cost this profile demonstrates — remains `O(n²)` after this fix, just with a
meaningfully smaller per-cycle constant (the ~39.93% SQL/parse share removed
from each term). Eliminating the clone would need the core-crate signature change
already ruled out above (shared with Postgres, an architectural ask of its
own); eliminating the from-the-top replay would mean changing how workflow
functions are invoked during replay, which is a property of the
determinism engine itself, not a `SqliteRuntime` persistence detail.

This is explicitly **not** implemented in this pass: it changes
`SqliteRuntime`'s internal state shape (a new cache field/lifetime to
reason about across `open`/`open_in_memory`/every public driving method) and
touches the core hot path (`drive_one_cycle`) this crate's own architecture
docs describe in unusual depth (see the atomic-persistence and
crash-recovery invariants documented at the top of `runtime.rs` and in
`lib.rs`'s module doc comment) — exactly the "architectural changes... data
structure ownership model" category this agent must ask before touching
rather than deciding unilaterally. A maintainer should weigh this against
the crate's stated scope (an embedded, single-writer backend for edge/local-
first deployments, where `n` is typically much smaller than the values swept
here) before deciding whether the complexity is warranted.

## What changed in this PR

Nothing in production code. Only:

- `autumn-harvest-sqlite/benches/runtime_drive_profile.rs` (new) — the
  profiling harness described above. Each of the workload's `n` activity
  calls uses a distinct, per-iteration `format!("activity_{i}")` name (with
  one `register_activity_raw` call per name), not a constant string — this
  deliberately mirrors
  `autumn-harvest/benches/replay_profile_support.rs::sequential_workflow`'s
  identical per-iteration naming exactly, since that is the comparison
  workload this document's "same workload issue #135 budgets for" claim
  rests on. (Caught and fixed during review: an earlier draft used a
  constant activity name, which understated both the `history.clone()` and
  `sequential_workflow`'s-own-re-execution allocation-byte costs, since a
  longer per-iteration name string allocates a larger `String` buffer on
  every replay pass than a fixed short constant would — `String` has no
  small-string optimization in Rust, so every heap allocation is sized to
  content. The fix changed the measured split but not any conclusion: the
  reload/reparse-addressable share and every scaling-ratio signature are
  materially unchanged, while the two non-cacheable shares — the clone and
  the workflow's own re-execution — grew both relatively and in absolute
  bytes, if anything strengthening the "a cache shrinks the constant, not
  the complexity class" argument below.)
- `autumn-harvest-sqlite/Cargo.toml` — registers the new `[[bench]]` target
  (`harness = false`, same convention as every other deterministic profiling
  harness in this repo).
- This document.

`cargo fmt --all -- --check`, `cargo check -p autumn-harvest-sqlite
--all-targets`, and the full existing `autumn-harvest-sqlite` test suite
(120 tests + 1 doctest) all pass unchanged. `cargo clippy -p
autumn-harvest-sqlite --all-targets --all-features -- -D warnings` could not
be run to completion in this sandbox: it fails while compiling the
unmodified `autumn-harvest` dependency on an unrelated, pre-existing
`#[cfg_attr(.., allow(clippy::unused_async_trait_impl))]` in
`autumn-harvest/src/context.rs` that names a lint clippy only recognizes from
1.98 onward, while this sandbox's `stable` toolchain resolves to clippy
0.1.94 — confirmed identical and reproducible with this PR's changes fully
reverted (`git stash`), i.e. unrelated to anything in this document.

## Reproduce

```bash
# Locate the compiled binary (cargo bench builds in release, doesn't run it):
BIN=$(cargo bench -p autumn-harvest-sqlite --bench runtime_drive_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason == "compiler-artifact" and .target.name == "runtime_drive_profile") | .executable')

export SQLITE_RUNTIME_PROFILE_N=80

# Sanity: prints a summary line, runs natively (no valgrind overhead):
"$BIN"

# Instruction count -- valgrind inherits the exported env var, so it must be
# set (exported) in the shell BEFORE the valgrind invocation, not passed as
# an argument to it:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out "$BIN"
callgrind_annotate --threshold=100 callgrind.out | head -40

# Allocation counts/bytes -- `--num-callers=30` is REQUIRED to reproduce the
# allocation-site attribution table above; the default depth (12) truncates
# `serde_json::Value::deserialize`'s recursive descent before it reaches the
# real Rust caller for a large share of allocations (see "Methodology note"
# under "Allocation-site attribution" above):
valgrind --tool=dhat --dhat-out-file=dhat.json --num-callers=30 "$BIN"

# Reproduce the "Allocation-site attribution" table above from that capture:
python3 autumn-harvest-sqlite/scripts/classify_dhat_allocations.py dhat.json
```

`SQLITE_RUNTIME_PROFILE_N` (default `100`) and `SQLITE_RUNTIME_PROFILE_REPS`
(default `1`) are read from the environment; the four-point scaling tables
above were produced by re-running the same binary at `N=20,40,80,160` with
`REPS=1`. Instruction counts are deterministic up to ~0.003% run-to-run noise
(observed: 34,059,018 vs. 34,057,986 Ir across two independent runs at
`N=20`, both against the identical binary) from environment-dependent
allocator/runtime-initialization detail unrelated to the measured workload —
well below anything that would change a conclusion in this document.

## See also

- `docs/performance-replay.md` — the equivalent investigation and shipped
  fix on the Postgres side's `WorkflowReplayer` hot path (issue #135).
- `docs/performance.md` — Ledger's (this repo's DB-query-focused sibling
  agent) `queue::claim_task` scaling and per-predicate attribution work,
  including its own "known limitations" list of unmeasured scenarios.
- Issue #235 — the Postgres backend's shipped `WorkflowCache` +
  `load_history_since` delta-load mechanism, the shape this recommendation
  mirrors.
