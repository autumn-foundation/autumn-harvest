## Phase 3.53 — Durable per-execution workflow logs (issue #790)

**Opt-in. Additive.** `ctx.log_info` / `log_warn` / `log_error` (issue #379) are
already the blessed, replay-safe way to log from workflow code, but they were
fire-and-forget to the host app's `tracing` subscriber: reading a run's lines
back meant leaving Vantage for Loki/Elastic/OTel and correlating by
`execution_id` by hand — the context switch that dominates MTTR for the first
question an operator asks (*what did this run actually say?*). This slice adds a
**durable sink** for those same lines, readable in one call, with **zero**
external log-aggregation correlation.

**Enabling it is one builder call and the workflow body does not change at all**:

```rust
HarvestPlugin::new()
    // Defaults: 1,000 lines per execution, 4 KiB per message.
    .workflow_log_persistence(WorkflowLogPolicy::default())
```

Absent that call, `ctx.logger()` is byte-for-byte today's behaviour
(tracing-only) and every existing workflow compiles and runs unchanged (AC6).

**No new `WorkflowEvent` variant, no event-schema change, no replay-determinism
impact, no macro-path change.** One additive migration
(`20260719000000_harvest_workflow_logs`).

### Mechanics

`ctx.log_*` pushes a new **bookkeeping** `WorkflowCommand::RecordLog { seq, level,
message }` — the `SetCurrentDetails` (#473/#593) / `PublishProgress` (#791)
pattern: no `result_tx`, never drives a suspension shape, appends **nothing** to
`harvest_events`. The worker writes the cycle's lines to the separate
`harvest_workflow_logs` table at persist time (`worker::collect_log_lines` →
`store::append_workflow_logs`).

**At-most-once, deduplicated (AC2)** is two mechanisms, not one. Emission
inherits `#379`'s replay suppression (a no-op while `ctx.is_replaying()` is
true), but that alone is *not* sufficient: a decision cycle that logs and then
parks can be re-driven at an **unchanged** history position — a spurious wake, or
a rolled-back persist — where `is_replaying()` is still `false`, so the line is
genuinely re-emitted. `seq` is therefore a pure **call ordinal**: the Nth
`ctx.log_*` call made by this run of the workflow body always carries `seq == N`,
whichever cycle emits it live. The ordinal is claimed *before* the replay gate,
on every call including a suppressed one, so a suppressed call still consumes its
slot and later lines never shift down onto another line's id. A re-driven
position therefore re-mints the *same* `seq` and a `UNIQUE (workflow_exec_id,
seq)` index plus `ON CONFLICT DO NOTHING` collapses it to one stored row.
Result: **never more than one stored row per logical emission**, no duplication
and no reordering, however many times a cycle is re-driven.

Deliberately **not** the `encode_progress_seq(epoch, local_index)` encoding its
sibling `publish_progress` (#791) uses. That encoding keys a line to the
loaded-history length at cycle start, which is only stable when a re-drive
happens at an *unchanged* history length. Two ordinary paths break it — a cycle
that appends any event before a later log, and a pause/resume (#383) inserting
replay-transparent events — and each re-mints a *different* `seq` for the same
logical line, defeating the dedup and duplicating it. The randomized
interleaving test drives exactly that shape and fails under the epoch encoding
(18 stored rows for 6 emitted lines) while passing under the ordinal.

Delivery is **best-effort**, not exactly-once: the INSERT rides a SAVEPOINT (see
below) and is warn-and-swallow, so a line can be stored *zero* times if its
cycle's persist is rolled back and the workflow then re-drives down a different
path. Logs are observational (AC7); a missing line is an observability gap,
never a correctness one. Relatedly, `seq` keys *position*, not *content*: if a
deploy changes the message text of the Nth call and an in-flight execution is
re-driven at that position, the originally stored text wins.

`append_workflow_logs` returns the INSERT's **affected-row count**, not the
number of lines it offered (Codex review round 2, P2): a re-driven cycle whose
rows are all collapsed by the conflict clause honestly reports `0` written. The
truncation-marker decision deliberately stays keyed on the *admitted* count —
gating it on the inserted count would stamp a marker on every re-drive and tell
an operator a healthy run had lost lines.

**A log write can never wedge a workflow.** The INSERT runs in a nested
`conn.transaction()` (a SAVEPOINT) inside the persist transaction and is
warn-and-swallow on failure — an observability table erroring must not become a
self-inflicted outage.

**Bounds (AC4).** Per message: `max_message_bytes` (default 4 KiB), truncated on
a UTF-8 character boundary, never rejected. Per execution: `max_lines` (default
1,000), **drop-newest** — the first lines of a run are the ones that explain how
it got where it is. Overflow is **visible, never silent**: a single synthetic
**truncation marker** row is appended at the reserved `seq = i64::MAX` sentinel
(sorts last, idempotent through the same conflict clause, needs no extra
column), surfaced as `"truncation_marker": true` on the wire plus a
response-level `truncated` flag that is probed directly and is therefore
**filter-independent**. `with_max_lines(0)` clamps up to `1` — a policy that
admits nothing would be a silent-loss trap, and disabling persistence is what
*omitting* the builder call already means.

`max_lines` bounds **memory as well as storage** (Codex review round 2, P2): a
decision cycle stops queuing `RecordLog` commands once it holds `max_lines + 1`,
checked *before* the message is cloned, so a workflow logging in a tight loop
without suspending cannot retain hundreds of thousands of capped strings while
advertising a 1,000-line cap. The `+ 1` is load-bearing — the store admits at
most `max_lines` from any batch, so queuing exactly `max_lines` would satisfy
`admit == lines.len()` and the truncation marker would never fire, converting
AC4's *visible* overflow into silent loss. A dropped call still consumes its
ordinal, so the `seq` identity AC2 depends on is undisturbed. The stored outcome
is unchanged; only the point at which the excess is discarded moves.

The marker is **terminal** (Codex review, P2): once it exists the gate stays
shut, even if `max_lines` is later RAISED. `max_lines` is per-worker-process
config, so on a rolling deployment a run can truncate under an old worker's cap
and have its next decision cycle handled by a new worker with a larger one.
Re-deciding admission against the current policy would store a line *after* the
one that was dropped, leaving a hole in the stored prefix and a marker whose
"subsequent lines were dropped" claim is false. Latching keeps the stored rows a
contiguous prefix of the run and keeps the marker honest; the already-dropped
lines are unrecoverable either way, so re-opening the gate buys nothing.
Rejecting a post-marker batch wholesale loses nothing, since every line in such
a batch is either already stored (and would have been collapsed by the conflict
clause) or was deliberately dropped.

**Retention (AC4)** is tied to workflow-history retention with **zero janitor
code**: the `workflow_exec_id` FK carries `ON DELETE CASCADE`, so the retention
janitor's execution delete takes the logs with it in the same statement. There
is no separate log-retention setting to misconfigure and no separate janitor to
fall behind. Targeted PII erasure (issue #495) **deletes** log rows rather than
tombstoning them — a log line is a single free-form author string with no field
structure to preserve, so a tombstone would carry no information a plain absence
does not; `EraseOutcome` reports `logs_deleted`.

### Read surfaces

**HTTP (AC3):** `GET /api/harvest/workflows/{id}/logs`, admin-gated (a log
message is free-form author text that routinely carries business detail, so it
takes the same posture as the sibling per-execution diagnostics like
`/awaitables`, not the plain execution-row one). Params: `limit` (default 200,
clamped `1..=1000`), `cursor` (alias `after`, exclusive keyset on `seq`), `level`
(repeatable or comma-separated; `info`/`warn`/`error`), `since` (RFC 3339
exclusive `occurred_at` bound). An unknown `level=` is a **400**, never a
silently-empty page — a typo must never look like "this run logged nothing".
Unknown id → **404**; the shard read uses `db_conn_for_execution_exact` so a
mis-routed read fails closed with **503** rather than being indistinguishable
from "no logs". Response carries `execution_id`, `lines`, `next_cursor`,
`total_lines`, `truncated`.

Lines are ordered by **`seq`, deliberately not `occurred_at`** — a workflow's
decision cycles can run on different workers whose clocks disagree, so
`occurred_at` is reported for context but is neither the sort key nor monotonic.

**CLI:** `harvest workflow logs <id> [--level …] [--limit …] [--cursor …]
[--since …]`.

**Vantage (AC5):** the execution-detail page gains a **Logs** panel with level
filtering (All / info / warn / error) over the same rows, mirroring the API's
admin gate via `has_harvest_admin_access`.

### Contract: observational only (AC7)

The load-bearing boundary, documented in `docs/workflow-logs.md`. Durable logs
are **not** part of the durable execution contract: not part of the event
history (`harvest_events` untouched); no determinism guarantee (a message may
embed non-deterministic content — it is never replayed, so nothing depends on
it); and **never read back into workflow logic** (there is no `ctx.read_logs()`
and there never will be — a workflow that needs to *act* on something must
record it as state). Concretely: the sink can be enabled or disabled, and rows
can be deleted by retention or erasure, with **zero** effect on whether a
workflow replays or what it computes.

### Test harness

`WorkflowTestEnv::with_log_policy(...)` opts a no-DB test into the sink, and
`TestRunOutcome::recorded_logs()` returns the `RecordedLogLine`s the run would
have persisted — `seq`-ordered and de-duplicated first-write-wins, exactly the
shape the store's unique index produces. It deliberately does not model the
per-execution line cap or its truncation marker (store-layer behaviours covered
by the database-backed tests).

### Tests

Core DB suite `tests/integration/workflow_logs_tests.rs` (store-level coverage
plus three worker-driven end-to-end tests: author lines persisted in order with
no log text in `harvest_events`; the sink-disabled path persisting nothing and
leaving the run unchanged; and a multi-cycle workflow storing each line exactly
once). Plugin HTTP suite `autumn-harvest-plugin/tests/workflow_logs_integration.rs`
(the single-call success metric, level filter + 400 on an unknown level, cursor
pagination exclusivity and the `after` alias, the `since` bound, never-logged →
empty 200, unknown → 404 / malformed → 400, the cap marker and
filter-independent `truncated`, and the admin gate on an execution that
genuinely has lines). Route-classification pins in `contract_regression.rs` and
`security.rs`; CLI mapping + contract-coverage tests. Example
`autumn-harvest/examples/workflow_logs.rs` with three embedded `WorkflowTestEnv`
tests (sink-enabled ordering, AC6 sink-disabled, and zero-event-footprint +
`ReplaySucceeded`). Both new suites are wired into
`.github/ci/integration-suites.txt` for the Docker-backed Linux run.
