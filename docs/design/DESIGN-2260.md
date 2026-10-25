# Design: issue #2260, an owner-local SQLite projection for SQL over entity state

Stage 3b of epic #2247. It builds on the keyed-entity state events of stage
3a (#2259) and the owner disk cache of stage 2 (#2258).
[ADR 0012](../adr/0012-owner-disk-cache.md) records the amendment.

**No migration. No new event. Off by default.** The projection needs the
`owner-disk-cache` feature, a cache directory, owner mode and
`WorkerConfig::with_owner_entity_sql`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- Stage 3a records each state change as `SideEffectRecorded` with kind
  `EntityState`. Its value is `{"state", "stats"}`, or an `omitted` marker
  past a payload cap. The Postgres row `harvest_entity_state` points at the
  last snapshot event of each key: `execution_id` and `event_id`.
- Stage 2 keeps one sealed SQLite file per partition. The node key lives in
  memory only. The worker wipes the directory at start, at a tenure end and
  at shutdown. A file stamp holds the epoch and the codec key generation.
  A terminal commit, an evict wake, an erasure wake and a 30 s reconcile
  pass purge a run.
- After each committed suspension, `write_disk_entry` holds the decoded
  history of the run as it was before the decision. The next decision
  loads the new events as a delta.
- On Unix, a deleted file stays readable through an open connection. A
  cache that never checks the path keeps reading the deleted inode.
- `rusqlite` 0.40 has the `hooks` (authorizer, progress handler) and
  `limits` features. Neither adds a crate.
- `sqlite3_stmt_readonly` is true for `ATTACH`. `query_only` alone does not
  stop a file write through `ATTACH` or `VACUUM INTO`.

### 0.2 Brainstorm: where does the projection live, and how does SQL see it?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A new file per partition, next to the stage 2 file. | Rejected. It doubles the wipe, the stamp and the tenure-end paths. |
| B2 | A new table in the stage 2 partition file. | **Adopted.** The wipe, the stamp and the tenure end cover it. An entity index routes the purges and the reconcile pass to it. |
| B3 | Plaintext columns that user SQL reads in place. | Rejected. The issue asks for a memory-only key. Plaintext must not reach the disk. |
| B4 | SQLCipher, or an encrypting VFS. | Rejected. A C dependency, or a large unsafe surface. |
| B5 | One sealed row per run. A query opens it into a fresh in-memory SQLite database, and user SQL runs there. | **Adopted.** The disk holds only ciphertext. The SQL engine is SQLite. |
| B6 | Store a whole SQLite image per entity. | Rejected. A snapshot row is smaller, and the in-memory build is cheap. |
| B7 | Apply each snapshot after the commit, in `write_disk_entry`. | **Adopted.** It is the stage 2 write point. Its cached history ends before the decision, so one indexed read of the committed `harvest_entity_state` row finds the new snapshot. |
| B8 | Rebuild on a miss from the one snapshot event that the Postgres row names. | **Adopted.** Two indexed reads. No full history read. |
| B9 | Rebuild by a replay of the whole run. | Rejected. The snapshot event already holds the state. |
| B10 | Serve a query on any worker. Only an owner with a live tenure writes the file. | **Adopted.** A non-owner answers from history and writes nothing. |
| B11 | Bind user parameters. | **Adopted.** User code then needs no string building. |
| B12 | One entity per query. | **Adopted.** The issue asks for SQL over a single entity. |

### 0.3 Reverse brainstorm: how can the projection cause harm?

| # | How to make it harmful | Counter-measure |
|---|------------------------|-----------------|
| H1 | Keep erased PII on disk. | The stage 2 purge points delete the entity row. The erasure test runs on this table. |
| H2 | Keep retired-key ciphertext. | The row holds decoded state, sealed under the node key. A new codec generation deletes the file. |
| H3 | Write plaintext through user SQL: `ATTACH`, `VACUUM INTO`, a temp spill. | An authorizer allows reads and functions only. The attach limit is 0. Each statement must be read-only. `temp_store` is `MEMORY`. |
| H4 | Change data through user SQL. | `query_only` is on. The authorizer refuses each write. The database is a fresh copy for each query. |
| H5 | Run a query forever, or exhaust memory. | A progress handler stops it at a step budget, a 5 s deadline or a memory budget. A row cap and a byte cap end a large result. A semaphore runs at most four queries. |
| H6 | Serve a stale row after a new snapshot. | A hit needs the run and the event id of the Postgres row. |
| H7 | Serve a deleted file. | Each read compares the path with the open inode. A gone file closes the partition, and the query rebuilds. |
| H8 | Serve a row after a takeover. | The file stamp holds the epoch. A tenure end deletes the file. |
| H9 | A late write lands after a purge. | The write checks the tenure under the partition lock, as stage 2 does. |
| H10 | Put the entity key on disk in plaintext. | The row holds no key. Only the run id is plaintext, as in stage 2. |
| H11 | Turn it on by default. | It needs a feature, a directory, owner mode and its own switch. |
| H12 | A disk error fails a query. | Any disk doubt reads as a miss. The query reads history. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The issue asks for a rebuild test and for
  the stage 2 erasure and key-rotation tests on this file.
- **Red (gut).** User SQL next to PII feels risky. The in-memory database,
  the authorizer and the sealed file answer that. Each one has a test.
- **Black (risks).** Section 0.3. The main risks are a plaintext write
  through SQL (H3) and a stale answer (H6, H7). The issue also asks for a
  customer request before work starts. None is linked.
- **Yellow (value).** A user filters and aggregates entity state with SQL
  and JSON functions. The owner answers from a local file.
- **Green (ideas).** SQL over all entities of a partition. Deferred: the
  issue asks for one entity. A metric for hit and rebuild. Deferred: the
  result names its source.
- **Blue (process).** Red tests first: the runner tests, the module tests,
  then the worker tests on Postgres. Then the code until green. Then the
  docs, the gates and a review from several angles.

## 1. Change

1. `entity_sql.rs`: the read-only runner over one snapshot.
2. `owner_disk_cache.rs`: the `harvest_entity_projection` table, its store,
   load and purge, and the deleted-file check.
3. `worker.rs`: the apply after a committed suspension, and
   `Worker::query_entity_sql`.
4. `WorkerConfig::with_owner_entity_sql`.
5. Docs: the ADR 0012 amendment, `docs/sticky-routing.md`, the module
   guide and a changelog fragment.

## 2. Tests

| Test | Proves |
|------|--------|
| `deleting_the_file_mid_run_rebuilds_from_history` | The issue's red test. The next query rebuilds the file from history, with identical results. |
| `an_erased_entity_leaves_no_local_bytes` | The stage 2 erasure test, on this table. |
| `an_erasure_purges_an_entity_that_a_missed_purge_left` | The stage 2 missed-purge erasure test, on this table. |
| `a_rotated_codec_key_leaves_no_entity_ciphertext_on_disk` | The stage 2 key-rotation test, on this table. |
| `a_foreign_terminal_purges_the_projection` | The stage 2 foreign-terminal test, on this table. |
| `the_reconcile_pass_purges_an_entity_that_is_not_running` | The stage 2 reconcile test, on this table. |
| `a_lost_tenure_deletes_the_projection_file` | The stage 2 lost-tenure test, on this table. |
| `a_shutdown_wipes_the_projection` | The stage 2 shutdown test, on this table. |
| `a_query_off_the_owner_reads_history` | A worker with no tenure writes no file. |
| `the_switch_off_keeps_no_projection` | With the switch off, the owner writes no row. |
| `entity_sql` module tests | Read-only enforcement, parameters, limits, value mapping. |
| `owner_disk_cache` module tests | The sealed row, the purge, the codec generation, the killed owner, the deleted file. |

## 3. Review findings

Three review agents read the change: correctness, security, and tests
with docs. The fixes:

- A purge removed the index entry after it released the partition lock. A
  store in that gap kept its row with no entry, so no purge found it. Each
  purge now clears the index under the partition lock. A run with rows in
  two partitions purges both.
- A query picked the tenure by partition only. A worker of two databases
  could write a row into the file of the other one. The query now uses the
  owner shard of its claim target, and none for several targets.
- A slow query could write a row after the evict hold of its run ended.
  A query now writes only within the hold of its pointer read.
- A query with a stale epoch could delete the file of a newer tenure. A
  load now checks the tenure under the partition lock first.
- A first store could miss an evict wake that came before its index entry.
  A store now checks the hold again after the insert. The stage 2 store
  has the same fix.
- A transient metadata error read as a deleted file. Only a missing file
  is gone now.
- One query could exhaust memory or run for hours. Blobs became hex
  strings, the value cap dropped to 1 MiB, and a result byte cap, a memory
  budget, a 5 s deadline, a cancel on drop and a limit of four queries
  came in.
- The worker tests read the file only after a query wrote it. They now
  wait for the row that the commit applied, with no query. That exposed a
  real defect: the apply read the cached history, which ends before the
  decision, so it never found the new snapshot. The apply now reads the
  committed `harvest_entity_state` row of the run. New tests port the
  stage 2 reconcile, foreign-terminal, lost-tenure and shutdown tests.

## 4. Out of scope

- SQL over more than one entity.
- A write path. The projection is read-only.
- A projection that survives a restart.
