## SQLite — the agent cost ledger (issue #2275)

Section 13 of the design note `DESIGN-2262.md` of epic #2262. The SQLite
backend now writes the agent cost ledger of issue #1996. Before
this change, the SQLite agent path called the model with no activity
context, so it wrote no ledger rows.

**Table.** The SQLite schema gains `harvest_llm_ledger`, with the columns
and CHECK bounds of the Postgres table. The key is `(exec_id, event_seq,
call_index)`. `event_seq` is the `seq` of the completion event, as
`event_id` is on Postgres. `recorded_at` is the drive clock at the
completion, in epoch milliseconds, with an index for window reads. The
table is new, so `CREATE TABLE IF NOT EXISTS` adds it to an older file on
the next open. `harvest_events` does not change, and no `WorkflowEvent`
variant is added, so replay does not change.

**Writes.** The runtime writes one row per call in the transaction that
appends `ActivityCompleted` or `LocalActivityCompleted`. A crash before that
commit writes no row, and the re-run writes the rows of its own attempt
once. A failed, timed-out or oversized attempt writes no row, as on
Postgres. Two sources feed it:

- `ActivityContext::record_llm_call` in an `#[activity]` body, on the
  worker path and the local path.
- The new `SqliteRuntime::register_activity_with_ledger(&info, body)`. The
  closure returns `Ok((output, Vec<LlmCall>))`.

**Reads.** `SqliteRuntime::llm_ledger(exec)` and
`SqliteRuntime::llm_ledger_between(from, until)` return `LlmLedgerRow`
values. `SqliteReader` has both reads, and a file from before this change
reads as having no rows. Retention removes the rows with their run.
`autumn_harvest_sqlite` re-exports `LlmCall`.

**Agent adapter.** `sqlite::register` registers `agent_model_turn` with the
ledger variant. One builder, `AgentHarness::ledger_call`, makes the call for
both backends, so one script gives the same rows. A response cache hit
records no row. The §9 limit "The cost ledger needs Postgres" is removed
from `docs/agent-adapter.md`.

**Tests.**

- `autumn-harvest-agent/tests/sqlite_ledger.rs`: one row per model call of
  a three-turn script; the same rows as the Postgres activity for the same
  script; no row for a cache hit or a tool call; a crash inside a model
  call and inside a tool call, each in a child process that aborts, then
  resumes with exactly one row per turn.
- `autumn-harvest-sqlite/tests/integration/llm_ledger.rs`: a ledger
  closure, an `#[activity]` body and a local activity; a failed attempt
  and its retry; a timeout; a restart; an older file; the reader; a window
  read; retention.
- Unit tests in the SQLite `llm_ledger.rs` and `worker.rs`: the CHECK
  bounds match the core limits, the model id charset, the per-attempt cap,
  and a rolled-back or failed finalize writes no row.
