# `SqliteRuntime` decision-cycle drive: full-history reload dominates cost, no local fix clears the floor

**Outcome: findings only — no production code changed.** This pass built the
first CPU/allocation-count profiling harness for `autumn-harvest-sqlite`
(previously zero benchmark/profiling infrastructure existed for this crate),
profiled a realistic end-to-end workflow drive, and traced the dominant cost
to a specific, source-confirmed mechanism: **every decision cycle reloads and
re-deserializes the entire event history from scratch**, with no warm-cache
or delta-load path — a gap the Postgres backend already closed (issue #235).
The mechanism (reload + reparse + the subsequent by-value clone) accounts
for **81.6% of allocation bytes** and a clear, super-linear (trending toward
quadratic) instruction-count scaling curve on this workload — but only
**~53%** of that (the reload + reparse itself) is addressable by a fix; the
remaining ~29% is a related, architecturally separate clone cost that the
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
(`valgrind --tool=dhat`), reproducible bit-for-bit on any machine.

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
over a pre-built history), this harness drives the *whole run* — every one of
the `n` decision cycles the SQLite backend's single-writer, poll-driven
architecture actually performs to complete an `n`-activity workflow (confirmed
by reading `SqliteRuntime::drive_one_cycle`/`drive_suspension`: each cycle
does exactly one `apply_commands` + `drain_ready` pass, so an `n`-activity
sequential workflow takes `n` decision cycles, not `2n` or `1`). That is the
realistic full-run cost a production embedder pays, not an isolated function
call.

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
| 20  | 33,852,949    | 1,692,647 | — |
| 40  | 98,276,836    | 2,456,921 | 2.903× |
| 80  | 328,441,045   | 4,105,513 | 3.342× |
| 160 | 1,194,590,711 | 7,466,192 | 3.637× |

A doubling of `n` under pure linear (O(n)) cost would produce a 2.0× ratio at
every step; under pure quadratic (O(n²)) cost, a flat 4.0× ratio. The
measured ratios climb monotonically — 2.903× → 3.342× → 3.637× — trending
toward, but not yet at, 4.0×. The "cost per activity" column makes the same
point directly: it should be *constant* under O(n) total cost, but it grows
4.4× (1,692,647 → 7,466,192) across an 8× increase in `n`. This is
super-linear scaling with a clearly super-linear trend, corroborated across
four independent input sizes as required by this agent's evidence rules for
an asymptotic argument (no wall-clock timing was used to produce any of these
numbers).

### Instruction-count flat profile at n=80 (mechanism attribution)

Grouping `callgrind_annotate --threshold=100`'s flat (self-cost) output by
mechanism, out of 328,441,045 total instructions:

| Mechanism | Ir | % of total |
|---|---:|---:|
| malloc/free (glibc allocator) | 124,810,735 | 38.00% |
| `serde_json` parse/deserialize | 46,225,717 | 14.07% |
| `BTreeMap` (`Value::Object` is `BTreeMap`-backed — no `preserve_order`) | 41,452,330 | 12.62% |
| `libc` `memcpy`/`memcmp` (generic, driven by the above) | 15,894,824 | 4.84% |
| sqlite3 SQL execution/parsing (`VdbeExec`, `RunParser`, `yy_reduce`) | 15,883,425 | 4.84% |
| `String::clone` | 3,939,441 | 1.20% |
| `Value` `drop_in_place` | 3,915,951 | 1.19% |
| `uuid::parser::try_parse` | 1,574,400 | 0.48% |
| **Sum of the above** | **253,696,823** | **77.24%** |

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
| 20 | 4,845,077  | 29,781  | — | — |
| 40 | 14,202,633 | 104,741 | 2.932× | 3.517× |
| 80 | 48,111,881 | 393,931 | 3.387× | 3.761× |

Same super-linear signature as the instruction counts, trending toward the
4.0× quadratic ratio, and — because allocation *block count* (not just bytes)
climbs even faster than bytes — this is not merely "each event's payload got
bigger to parse", it is genuinely more individual allocations happening per
activity as `n` grows.

### Allocation-site attribution at n=80 (mechanism breakdown)

Every one of `dhat`'s 393,931 allocated blocks / 48,111,881 bytes was
categorized by walking its full call-stack (`ftbl`/`pps` in the `dhat.json`
output) for a small set of mutually-exclusive frame markers:

| Category | Bytes | % of total | Blocks |
|---|---:|---:|---:|
| `BTreeMap`/`Vec` clone (`history.clone()`) | 13,817,911 | 28.72% | 139,461 |
| `store::load_history`'s own `Vec<WorkflowEvent>` growth | 10,760,469 | 22.37% | 84,930 |
| `serde_json` deserialize (JSON reparse) | 8,681,029 | 18.04% | 66,686 |
| `sequential_workflow`'s own re-execution (`activity_payload(i)` rebuild) | 7,204,155 | 14.97% | 83,560 |
| sqlite3 internals (SQL execution engine) | 6,006,440 | 12.48% | 5,711 |
| other `autumn_harvest_sqlite` glue (statement-cache bookkeeping etc.) | 1,628,807 | 3.39% | 13,555 |
| other/uncategorized | 13,070 | 0.03% | 28 |

Four categories sum to **81.6%** of all allocation bytes in this workload,
but they split into two *architecturally distinct* costs, not one:

- **The reload + reparse itself** — `load_history`'s Vec growth (22.37%) +
  `serde_json` deserialize (18.04%) + sqlite3 internals executing the reload
  query (12.48%) = **52.89%**. This is directly caused by, and fully
  addressable by fixing, the single design choice traced below (every
  decision cycle re-reads and re-deserializes the full history from
  scratch).
- **The `history.clone()`** (28.72%) — a *related but separate* cost. It is
  triggered today by the reload producing a fresh `Vec` that must then be
  cloned for the by-value executor call, but the clone itself would still
  happen against a *cached* history too: `drive_one_cycle` needs the
  original `history` value again after the executor call regardless of
  where that `Vec` came from (see the "Recommendation" section below for why
  this specific 28.72% is **not** eliminated by the proposed cache).

So while today's implementation makes both costs scale together (81.6%
combined), only the 52.89% reload/reparse share is what the recommended
fix below addresses. The fifth category, `sequential_workflow`'s own
re-execution, is a **separate, inherent cost of the deterministic-replay
execution model itself** (present in the Postgres backend too — see "What
isn't the finding" below), not part of this finding's recommendation.

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

For an `n`-activity sequential workflow, cycle `k` (of `n` total cycles) has
accumulated `O(k)` events (`WorkflowStarted` + `ActivityScheduled`/
`ActivityCompleted` pairs for the `k-1` already-completed activities plus the
one in flight). `drive_one_cycle` reloads and fully re-parses **all** of them
on cycle `k`, not just the delta since cycle `k-1` — so total work across the
run is `Σ(k=1..n) O(k) = O(n²)`, exactly the trend the four-point Ir/dhat
scaling curves above show empirically. The `.clone()` on top is required
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

Notably, the SQLite backend's single-writer, single-process design makes it
an **easier** target for this fix than Postgres, not harder: Postgres's cache
can miss on a genuine cross-worker handoff (sticky routing can route a
follow-up task to a different worker with a cold cache), but `SqliteRuntime`
is one process driving one file — every decision cycle for a given execution
is, by construction, driven by the *same* runtime instance that drove the
previous one. An in-process cache here could have a 100% hit rate for the
life of the runtime, with no cross-worker staleness concern to reason about
at all; it would only need to be cleared on `open()`/reopen (which already
happens implicitly, since a fresh `SqliteRuntime` has no prior cache state).

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
   allocation site is **2,046,829 bytes (4.25% of total) / 6,561 blocks
   (1.67% of total)** at n=80 — below both the 5%/10% floors on its own, and
   it does nothing for the dominant instruction cost (parsing a borrowed
   `&str` still touches every byte of the JSON; only the allocation, not the
   scan/lex work, would be avoided).

2. **Cache the small point queries** (`store::next_seq`'s per-append
   `SELECT MAX(seq)`, and several other `execution_input`/`workflow_name_of`/
   `workflow_id_of`-style plain `query_row` calls that don't use
   `prepare_cached`). The sqlite3 SQL-parsing subset of the flat profile
   (`sqlite3RunParser` + `yy_reduce.isra.0`) is 3,909,343 + 3,088,300 =
   6,997,643 Ir at n=80 — 2.13% of total, below the 5% floor. This cost is
   also **linear** in the number of decision cycles, while total cost is
   super-linear — so as a fraction of total cost it can only shrink further
   at larger, more realistic `n`, not grow toward the floor.

Both candidates were left unimplemented; a change that cannot clear the
floor is not opened as a PR per this agent's mandate (a below-floor local
change would need to be reverted, and reverting an isolated no-op diff would
add no value here — the harness itself, which is the useful long-lived
artifact, has no production-code footprint to revert).

## What isn't the finding

The `sequential_workflow` re-execution category (14.97% of allocation bytes
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
`serde_json::from_str` deserialize** cost — `load_history`'s Vec growth
(22.37%) + serde_json deserialize (18.04%) + sqlite3 internals (12.48%) =
~53% of allocation bytes at n=80, and the corresponding instruction share —
from `O(k)` (reloading and reparsing the full history every cycle) into
`O(1)` amortized (only the new events since the last cycle touch SQL or
`serde_json` at all).

**This substantially shrinks the constant factor but does not change the
`O(n²)` complexity class.** Two costs are `O(k)` per decision cycle
regardless of caching, and neither is addressed by this recommendation:
`drive_one_cycle` still `.clone()`s the *entire* accumulated
`Vec<WorkflowEvent>` before handing it, by value, to the core executor
(28.72% of allocation bytes at n=80 — a cache holding the full history
in-process still has to clone it out for that by-value call); and the
workflow function itself still replays every prior activity call from the
top on every cycle (the 14.97% "what isn't the finding" cost above). Both
are inherent to a from-the-top-replay execution model, independent of where
the history data lives. So `Σ(k=1..n) O(k)` — the `O(n²)` total-run cost
this profile demonstrates — remains `O(n²)` after this fix, just with a
meaningfully smaller per-k constant (the ~53% SQL/parse share removed from
each term). Eliminating the clone would need the core-crate signature change
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
  profiling harness described above.
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

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`SQLITE_RUNTIME_PROFILE_N` (default `100`) and `SQLITE_RUNTIME_PROFILE_REPS`
(default `1`) are read from the environment; the four-point scaling tables
above were produced by re-running the same binary at `N=20,40,80,160` with
`REPS=1`. Instruction counts are deterministic up to ~0.002% run-to-run noise
(observed: 33,852,949 vs. 33,853,542 Ir across two independent runs at
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
