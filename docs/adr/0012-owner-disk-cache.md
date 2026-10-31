# ADR 0012: The owner-local disk cache

## Status

Accepted as an opt-in (issue #2258). Epic #2247, stage 2. The stage 2 gate
(#2257) is still undecided, so the cache is off by default.

## Context

A decision owner keeps its live runs in a memory cache of
`workflow_cache_size` entries, 1,000 by default.
A run past that cap, and every run after a restart or a takeover, replays
cold. A cold decision reads the full history from Postgres. The gate bench
(`docs/rnd/2026-10-22-stage-2-gate.md`) shows that a local SQLite read is
2.2 to 3.3 times faster than one Postgres read.

A disk copy has two hazards that memory does not have:

- **PII erasure** (`erase.rs`) rewrites a finished history in Postgres
  only. A disk copy keeps the erased data. A delete at the end of a run is
  not enough. The owner can crash between the terminal commit and the
  delete. Nothing then revisits the file.
- **Codec rotation** (`codec_rotation.rs`) re-encrypts live histories. A
  disk copy of codec ciphertext blocks the destruction of a retired key. A
  disk copy of plaintext bypasses encryption at rest.

## Decision

Local disk is a cache only. Postgres stays the source of truth.

1. **Format.** One SQLite file per partition, `s{shard}-p{partition}.hcache`.
   Its table `harvest_events(exec_id, seq, event_json)` has the shape of
   `autumn-harvest-sqlite`. The core crate cannot depend on that crate, so
   the cache has its own copy of the schema. There is no `StorageBackend`
   trait, and `SqliteRuntime` is not used.
2. **Decoded events, sealed.** The cache stores events after the payload
   codec decodes them. AES-256-GCM seals each row under a node key. The key
   comes from the OS random source at start and lives in memory only. The
   associated data binds the partition, the run and the sequence. So no
   codec ciphertext reaches the disk, and a crash leaves only ciphertext
   whose key no longer exists.
3. **Wipe.** The worker deletes every cache file at start. A tenure end
   deletes the file of that partition. A tenure end is a fence, a scan
   drop, a release, a new acquisition or a local deadline that passes while
   the database is out of reach. A stopped worker deletes the files that it
   wrote. The cache needs no durable inventory.
4. **Purge.** A terminal commit purges the run. An evict wake purges the
   run (#2253). A foreign state change and an erasure both send one. After
   an evict wake, a store refuses the run for 30 s, so a decision in flight
   cannot write it back. The owner scan runs a reconcile pass at most once
   in 30 s per shard. It purges each cached run that is not `RUNNING`, and
   each file of an older codec key generation. That pass is the backstop
   for a lost wake. `secure_delete` zeroes the deleted rows. The journal
   and temporary tables stay in memory.
5. **Validity stamps.** The file stamp holds the owner epoch and the codec
   key generation. The run stamp holds the run state, the next event id and
   the hash of the last event. A read compares that hash with the stored
   event of the same id in Postgres. Any mismatch purges, and the decision
   loads from Postgres.
6. **Lazy rebuild.** A file fills on the first committed suspension of each
   run after a takeover. A later suspension appends only its new events. A
   run with fewer than `owner_disk_cache_min_events` events, 100 by
   default, never goes to disk. One Postgres read wins for such a run.
7. **One query per hit.** A disk hit reads the anchor event and the delta
   after it in one Postgres query.
8. **One process per directory.** An exclusive OS lock on
   `owner-disk-cache.lock` keeps a second process out, as
   `autumn-harvest-sqlite/src/lock.rs` does. On Unix the open creates the
   directory with the mode `0700`. It refuses an existing directory that
   other users can reach, and it never changes the mode of one.

### Order of a tenure end

A disk write checks the tenure under the partition lock. A tenure end first
drops the tenure, then deletes the file under the same lock. So a write that
passed the check finishes before the delete, and a later write is refused.

## Consequences

- An erased run leaves no local bytes. A killed owner leaves only sealed
  rows, and the next start deletes them.
- A retired codec key leaves no ciphertext on disk, because the disk never
  holds codec ciphertext. A key change makes each file stale. The next read
  or reconcile pass deletes it.
- A damaged file fails its seal. The decision replays cold from Postgres,
  with no non-determinism error.
- A restart starts with an empty cache. The cache helps once each run has
  decided once: a run past the memory cap then reads the local file. From
  about 100 events, that next pass is 1.5 to 1.9 times faster. See
  `docs/rnd/2026-10-23-owner-disk-cache.md`.
- Each committed suspension of an owned run costs one local write.
- A finished run can stay on disk when its purge and its evict wake are
  both lost. The erasure sends a new wake. If that wake is lost too, the
  run stays for about one reconcile interval (30 s) plus one owner scan.
  Its rows stay sealed, and the key never leaves the process.
- The free blocks of a deleted file can hold old ciphertext until the file
  system reuses them. The key dies with the process.
- A core dump or swap can write the key and decoded events to disk. Turn
  off core dumps on an owner host, or encrypt its swap.

## Amendment: the entity SQL projection (issue #2260)

Epic #2247, stage 3b. Some users want SQL over the state of one entity.
Stage 3a (#2259) records each state change as a snapshot event. The
projection keeps the last one of each live entity run in the partition
file, for SQL that user code runs read-only.

1. **One more table.** `harvest_entity_projection(exec_id, sealed)` lives
   in the partition file. So the node key, the wipe, the tenure end and
   the stamp cover it. An entity index in memory routes the purges and the
   reconcile pass to the row. The format version is 2.
2. **Sealed, with no key.** The row holds the event id, the deleted flag
   and the decoded snapshot. The associated data binds the partition and
   the run. The entity key is not in the row.
3. **Applied after the commit.** After each committed suspension of a held
   tenure, the owner reads the run's row of `harvest_entity_state`. The
   trigger wrote it in the commit, and the index on `execution_id` serves
   the read. The cached history of the decision does not hold the new
   events, so the owner cannot read the snapshot from memory. The owner
   seals a newer snapshot into the file. A row never moves back to an
   older event.
4. **Checked on read.** A hit needs the run and the event id of the
   Postgres row `harvest_entity_state`. Otherwise the query reads that
   snapshot event from history. An owner with a live tenure on the owner
   shard of the query then writes the row again. A worker with several
   claim targets cannot tell that shard, so it writes nothing. A file that
   left the directory reads as stale. A store then makes a new file. The
   check needs a local file system with stable inode numbers.
5. **SQL in memory.** A query copies the snapshot into a fresh in-memory
   SQLite database. An authorizer allows reads and functions only. The
   attach limit is 0, and temporary data stays in memory. So user SQL
   cannot change data or write a file. A step budget, a 5 s deadline, a
   memory budget, a row cap and a result byte cap bound each query. At
   most four queries run at a time. A dropped caller stops its query.
6. **Opt-in.** `WorkerConfig::with_owner_entity_sql` turns it on. It needs
   the disk cache directory.

Consequences:

- A deleted file costs one history read on the next query. The answer is
  the same.
- An erased or retired-key run leaves no row, by the rules above.
- A query that rebuilds a row writes it only within the evict hold of its
  pointer read. A later evict wake therefore always finds the row.
- With the switch on, each committed suspension of an owned run costs one
  indexed Postgres read. A new snapshot also costs one local write. A run
  with no snapshot writes nothing.

## Alternatives

- **Store the stored rows.** Rejected. Retired-key ciphertext would stay on
  disk.
- **SQLCipher.** Rejected. It adds a C dependency. Row sealing gives the
  same property for the data that the cache holds.
- **Write to disk only on a memory eviction.** Rejected. Each eviction then
  writes the full history.
- **A cache that survives a restart.** Deferred. It needs a sealed key in a
  KMS and a durable inventory.
