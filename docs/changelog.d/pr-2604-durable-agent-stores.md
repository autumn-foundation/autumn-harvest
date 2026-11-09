## Durable agent stores on SQLite and Postgres (issue #2604)

The agent crate ships durable episode, memory and transcript stores. An
app no longer writes its own store to turn on episodes, consolidation or
transcript deltas. The design is in `docs/design/DESIGN-2604.md`.

- New module `autumn_harvest_agent::stores`: `SqliteEpisodeStore`,
  `SqliteMemoryStore`, `SqliteTranscriptStore` behind `sqlite`, and
  `PostgresEpisodeStore`, `PostgresMemoryStore`,
  `PostgresTranscriptStore` behind `postgres`. The queries are in
  `autumn_harvest::agent_store` and `autumn_harvest_sqlite::SqliteAgentStoreDb`.
- Each episode and memory scope is one document with its applied edit
  keys. One statement writes an edit and its key, so a retried append
  after a crash lands once. A write names the version that it read, so
  two workers on one scope lose no edit. An episode clear keeps a
  tombstone with the generation and no episode text.
- A transcript window keeps its run and its session. A durable store
  keeps the newest 4 windows of a run and no window older than 7 days.
- **Breaking.** `TranscriptStore` is async. `get` and `put` return a boxed
  future, and `put` takes a `WindowOwner`. A new `erase(session)` deletes
  the windows of a session. Its default reports no count.
- `ErrorKind` now derives `Serialize` and `Deserialize`, in `snake_case`.
- Under the patch `agent_store_counts`, `Control::ResetMemory` counts the
  memory entries that it removes and erases the transcript windows of the
  agent. `AgentState` gains `memory_entries_erased` and
  `transcripts_erased`. `harvest agents erase` reports both. New
  activities: `agent_memory_erase` and `agent_transcript_erase`. A reset
  that history recorded before the patch replays with no count.
- **Migration.** `20261109005653_harvest_agent_stores` adds the tables
  `harvest_agent_store_docs` and `harvest_agent_transcripts`. They are new
  and not hot. No existing table changes. No new `WorkflowEvent` variant.
- Evidence:
  - `agent_stores` (SQLite): the store suite,
    `a_crash_after_the_sqlite_append_does_not_duplicate_it` (a child
    process aborts after the append commits) and
    `an_erase_removes_the_rows_of_the_agent_and_counts_them`.
  - `agent_stores_postgres`: the same suite,
    `a_retried_postgres_append_after_a_lost_ack_lands_once` on a real
    worker, and the erase test.
  - `the_report_counts_the_erased_memory_entries_and_windows` in the CLI.
