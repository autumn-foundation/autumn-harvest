# ADR 0012: The owner-local disk cache

## Status

Accepted as an opt-in (issue #2258). Epic #2247, stage 2. The stage 2 gate
(#2257) is still undecided, so the cache is off by default.

## Context

A decision owner keeps its live runs in a memory cache of 1,000 entries.
A run past that cap, and every run after a restart or a takeover, replays
cold. A cold decision reads the full history from Postgres. The gate bench
(`docs/rnd/2026-10-22-stage-2-gate.md`) shows that a local SQLite read is
2.2 to 3.3 times faster than one Postgres read.

A disk copy has two hazards that memory does not have:

- **PII erasure** (`erase.rs`) rewrites a finished history in Postgres
  only. A disk copy keeps the erased data. A delete at the end of a run is
  not enough: the owner can crash between the terminal commit and the
  delete, and nothing revisits the file.
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
3. **Wipe.** The worker deletes every cache file at start, at each tenure
   end and at shutdown. A tenure end is a fence, a scan drop, a release or a
   new acquisition. The cache needs no durable inventory.
4. **Purge.** A terminal commit purges the run. An evict wake from a foreign
   writer (#2253) purges the run. Every 30 s the owner purges each cached
   run that is no longer `RUNNING`. That pass is the backstop for a lost
   wake. `secure_delete` zeroes the deleted rows. The journal and temporary
   tables stay in memory.
5. **Validity stamps.** The file stamp holds the owner epoch and the codec
   key generation. The run stamp holds the run state, the next event id and
   the hash of the last event. A read compares that hash with the stored
   event of the same id in Postgres. Any mismatch purges, and the decision
   loads from Postgres.
6. **Lazy rebuild.** A file fills on the first committed suspension of each
   run after a takeover. A later suspension appends only its new events.
7. **One process per directory.** An exclusive OS lock on
   `owner-disk-cache.lock` keeps a second process out, as
   `autumn-harvest-sqlite/src/lock.rs` does.

### Order of a tenure end

A disk write checks the tenure under the partition lock. A tenure end first
drops the tenure, then deletes the file under the same lock. So a write that
passed the check finishes before the delete, and a later write is refused.

## Consequences

- An erased run leaves no local bytes. A killed owner leaves only sealed
  rows, and the next start deletes them.
- A retired codec key leaves no ciphertext on disk, because the disk never
  holds codec ciphertext. A key change still purges each file.
- A damaged file fails its seal. The decision replays cold from Postgres,
  with no non-determinism error.
- A restart starts with an empty cache. The cache helps once each run has
  decided once: a run past the memory cap then reads the local file.
- Each committed suspension of an owned run costs one local write.
- A run that a foreign writer ended can stay on disk for up to 30 s when
  its evict wake is lost. Its rows stay sealed, and the key never leaves
  the process.
- The free blocks of a deleted file can hold old ciphertext until the file
  system reuses them. The key dies with the process.

## Alternatives

- **Store the stored rows.** Rejected. Retired-key ciphertext would stay on
  disk.
- **SQLCipher.** Rejected. It adds a C dependency. Row sealing gives the
  same property for the data that the cache holds.
- **Write to disk only on a memory eviction.** Rejected. Each eviction then
  writes the full history.
- **A cache that survives a restart.** Deferred. It needs a sealed key in a
  KMS and a durable inventory.
