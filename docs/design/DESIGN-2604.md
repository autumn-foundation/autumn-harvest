# DESIGN-2604: durable agent stores on SQLite and Postgres

Issue #2604. Related: #2454 (episodes), #2264 (transcript deltas), #2273
(memory as state), #2267 (erase).

## 1. Problem

The agent crate ships in-memory stores only. An app that turns on
episodes or transcript deltas must write its own durable store, and must
get the once-only rules right. `harvest agents erase` does not reach a
`TranscriptStore`, so an app must also clear windows itself.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the SQL lives | the agent crate · **the core crate (`agent_store`) and `autumn-harvest-sqlite`, as for #2371 and #2439** |
| Episode and memory shape | one row per episode · **one document per scope that holds the whole `EpisodeLog` or block list** |
| Who holds the rules | SQL · **`EpisodeLog` and `apply_to`, so every store applies the same rules** |
| How two writers agree | an advisory lock · a transaction across crates · **a compare-and-swap on a row version** |
| Where an edit key goes | a key table · **inside the document, written by the same statement** |
| Transcript shape | a chain of deltas · **one row per window, keyed by digest and session** |
| Who owns a window | a new field on the turn input · **the session of the turn, which is the agent id for a durable agent** |
| Transcript bound | a byte bound · **the newest windows of each run, and an age limit** |
| `TranscriptStore` calls | sync · **async, so a Postgres store does not block** |
| Body column | `JSONB` · **`BYTEA`, so a NUL in model text is safe** |

## 3. Reverse brainstorming

"How could the store lose, double or leak data?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| An append commits, the ack is lost, and the retry appends again. | The key is in the document. One statement writes both. |
| Two writers read version 3 and both write. | A write needs the version that it read. The loser reads again. |
| A pass that runs across an erase writes its facts back. | A clear keeps the generation in a tombstone document. |
| An erase leaves transcript text in the store. | Each window keeps its session. The reset deletes the rows of the session and counts them. |
| An old run replays a reset with a new activity. | The new steps run behind the patch `agent_store_counts`. |
| A transcript read error fails the turn. | A read error is a miss. The turn goes in the full form. |
| The tables grow without a bound. | The key list keeps 4 096 keys. A put deletes old windows of its run and expired windows. |
| `Debug` prints personal data. | `Debug` of a store prints no row. |
| A migration blocks a hot table. | The tables are new. The migration sets `lock_timeout`. |

## 4. Six thinking hats

- **White (facts).** `EpisodeLog` serialises and holds every rule. The
  durable agent sets the session of each turn to the agent id. The erase
  report reads its counts from the final state.
- **Red (feeling).** A change to `TranscriptStore` breaks each app that
  implements it. The trait has two methods, so the port is short.
- **Black (risk).** One op rewrites one document of up to about 500 KB.
  A `SQLite` call runs on the polling thread. The stores keep text in
  clear, outside the payload codecs.
- **Yellow (value).** An app gets durable, once-only stores with one
  line. The erase reaches every store that the crate ships.
- **Green (ideas).** One backend trait serves the three stores on both
  backends.
- **Blue (process).** Red tests first, then the SQL layer, then the
  stores, then the erase step, then the docs.

## 5. Decision

- `autumn_harvest::agent_store` holds the Postgres queries and
  `autumn_harvest_sqlite::SqliteAgentStoreDb` the `SQLite` queries. Two
  tables: `harvest_agent_store_docs` and `harvest_agent_transcripts`.
- `autumn_harvest_agent::stores` holds `SqliteEpisodeStore`,
  `SqliteMemoryStore`, `SqliteTranscriptStore` and their `Postgres` forms.
- `TranscriptStore::put` takes a `WindowOwner`. `TranscriptStore::erase`
  deletes the windows of a session. The default reports no count.
- Under the patch `agent_store_counts`, `Control::ResetMemory` counts the
  memory entries that it removes and erases the transcript windows of the
  agent. `harvest agents erase` reports `memory_entries_erased` and
  `transcripts_erased`.

## 6. Acceptance criteria

| Criterion | Proof |
|---|---|
| A retried append after a crash lands once on `SQLite` | `a_crash_after_the_sqlite_append_does_not_duplicate_it` |
| A retried append after a lost ack lands once on Postgres | `a_retried_postgres_append_after_a_lost_ack_lands_once` |
| An erase removes the rows of the agent and counts them | `an_erase_removes_the_rows_of_the_agent_and_counts_them` on each backend, and the CLI report test |
| Each query has a Ledger profile | `cargo xtask audit ledger-profiles` |
| The migration follows the lock rules | `cargo xtask audit migration-lock-safety` |
| `docs/agent-adapter.md` names the stores in §10 and §15 | the guide |
