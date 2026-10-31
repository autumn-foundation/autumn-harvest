# `OwnerDiskCache`: store, append, load and the entity row

The owner-local disk cache (issue #2258) keeps the decoded history of each
live run in a sealed SQLite file, one file per partition.
[ADR 0012](adr/0012-owner-disk-cache.md) records the design. A decision
owner writes the cache after each committed suspension. `store_cache_entry`
(`worker.rs:29244`) calls `write_disk_entry` (`worker.rs:29522`), which calls
`DiskTier::write` (`worker.rs:22881`) for a held tenure (`worker.rs:22876`).
`DiskTier::write` (`owner_disk_cache.rs:1784`) returns at once for a history
under `owner_disk_cache_min_events` (`owner_disk_cache.rs:1795`). The default
is 100 events (`builder.rs:3898`). Otherwise it calls `store_marked`
(`owner_disk_cache.rs:1801`). The first store of a run after a takeover writes
every event. Each later store appends the new events only. On a memory miss,
a decision calls `read_disk_entry` (`worker.rs:23105`). That calls
`DiskTier::read` (`worker.rs:22840`), which reads the file only when the
in-memory index holds the run (`owner_disk_cache.rs:1733`). It then calls
`load` (`owner_disk_cache.rs:1741`). With entity SQL on, `apply_entity`
(`worker.rs:22879`) calls `store_entity` after a committed suspension with a
newer snapshot (`owner_disk_cache.rs:1652`). An entity query calls
`load_entity` (`owner_disk_cache.rs:1476`), and `store_entity` on a miss
(`owner_disk_cache.rs:1517`). `open` runs once per worker registry
(`worker.rs:1581`).

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

A fresh store costs about 17,300 instructions per event, and a load about
17,200. Both are linear in the history length. An append decision costs
about 196,000 instructions. Its two new events account for at most 21.75% of
that. The rest is fixed work on every store: two stamp reads, three SQL
parses, two SHA-256 hashes and a commit.

## Workload

[`benches/owner_disk_cache_profile.rs`](../autumn-harvest/benches/owner_disk_cache_profile.rs)
drives the public
[`OwnerDiskCache`](../autumn-harvest/src/owner_disk_cache.rs) API. It writes
a real SQLite file and opens no Postgres connection. A history starts with
`WorkflowStarted`. Pairs of `ActivityScheduled` and `ActivityCompleted`
follow, each with an order payload. The first scheduled and completed events
encode to 246 and 213 bytes.
`OWNER_DISK_CACHE_PROFILE_SHAPE` picks the workload:

| Shape | Measured entry points | Asserted outcome |
|---|---|---|
| `store` (default) | One `store` of N events into a new file. | `Written { appended: N }`. |
| `append` | One `store` of N events, then 100 decisions. Each decision adds two events and calls `store` again. | Each decision appends exactly 2 events. |
| `load` | One `store` of N events, then one `load`. | A hit with N events, and the last event matches. |
| `entity` | One `store_entity` and one `load_entity` of a state with N order items. | The loaded row equals the stored row. |

`DiskTier::write` calls `store_marked` with the history byte mark of the
decision. The harness calls the public `store`, which passes no mark. The
sealed run stamp is then a few bytes shorter. The rest of the path is the
same.

Each entry point runs in its own `#[inline(never)]` wrapper (`measure_*`), so
the inclusive cost of the wrapper leaves the setup out. The run ids, activity
ids and timestamps are fixed. The harness removes
`OWNER_DISK_CACHE_PROFILE_DIR` and creates it again with the mode `0700`.
It refuses a directory that holds any file other than a cache file or the
lock file. A run leaves the lock file behind. So a missing directory is
created with an empty lock file first, and the first run removes the same
files as a later run.

Before each measured step, `settle_heap` asks for one 4 KiB block. That makes
glibc merge the small chunks that the setup freed, so no entry point pays
for them. It is not called between two `append` decisions.

Three sources of nondeterminism remain:

- `Sealer::new` draws the node key and the nonce prefix from `OsRng`. The
  AES-GCM instruction count does not depend on them.
- The index maps use `RandomState`. SipHash costs the same for any key, and
  each map holds one or two entries.
- File I/O is a system call. Callgrind does not count the kernel side.

Every pair of runs below gave the same total, to the instruction. Each pair
ran from one working directory into a directory that already existed. A first
run into a missing directory costs 1,977 `Ir` more, all in the setup. Its
`measure_store` moved by 8 `Ir`. The environment and the paths also move the
heap layout. A working directory three characters longer moved the `store`
total by 3,663 `Ir` (0.01%). So compare runs made with the same command from
the same directory.

This page was re-measured after PR #2355, which moved the toolchain to Rust
1.99.1 and `aes-gcm` to 0.11. The `store` total at N=1000 fell from
28,149,220 to 27,678,900 `Ir` (1.67%). Most of that drop is in `Sealer::seal`,
which fell by 447,421 `Ir`. The seal share of a fresh store fell from 17.53%
to 15.41%, and the AES-GCM share from 15.53% to 13.35%. The unseal share of a
load fell from 17.22% to 14.38%.

## Running

```bash
BIN=$(cargo bench -p autumn-harvest --features owner-disk-cache \
  --bench owner_disk_cache_profile --bench entity_sql_profile --no-run \
  --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="owner_disk_cache_profile") | .executable')
# The harness deletes this directory first. A relative path keeps its length
# the same in any working directory.
export OWNER_DISK_CACHE_PROFILE_DIR=cache-dir
OWNER_DISK_CACHE_PROFILE_SHAPE=store OWNER_DISK_CACHE_PROFILE_N=1000 \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --inclusive=yes --threshold=99 cg.out
OWNER_DISK_CACHE_PROFILE_SHAPE=append OWNER_DISK_CACHE_PROFILE_N=1000 \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='owner_disk_cache_profile::measure_append' \
  --callgrind-out-file=cg.append.out "$BIN"
callgrind_annotate --threshold=90 cg.append.out
OWNER_DISK_CACHE_PROFILE_SHAPE=load OWNER_DISK_CACHE_PROFILE_N=1000 \
  valgrind --tool=dhat --num-callers=64 --dhat-out-file=dhat.json "$BIN"
```

Set `OWNER_DISK_CACHE_PROFILE_SHAPE` to `store`, `append`, `load` or
`entity`, and `OWNER_DISK_CACHE_PROFILE_N` to the size. The defaults are
`store` and 1000. `OWNER_DISK_CACHE_PROFILE_REPS` (default 1) repeats the
workload. The breakdown tables use `--toggle-collect` on one wrapper. That
run collects only the cost inside the wrapper.

## Profile

Callgrind, total `Ir` at N=1000. Two runs each:

| Shape | Run 1 | Run 2 | Entry point | Entry Ir | Share |
|---|---:|---:|---|---:|---:|
| `store` | 27,678,900 | 27,678,900 | `store`, 1,000 new events | 17,289,979 | 62.47% |
| `load` | 48,164,229 | 48,164,229 | `load`, 1,000 events | 17,223,868 | 35.76% |
| `append` | 49,197,954 | 49,197,954 | 100 × `store`, 2 new events each | 19,585,864 | 39.81% |
| `entity` | 44,791,625 | 44,791,625 | `store_entity`, 121,882 bytes | 4,285,320 | 9.57% |
| `entity` | | | `load_entity`, 121,882 bytes | 9,157,946 | 20.45% |

`open` costs 5,705 `Ir` in each of these runs. Callgrind does not count its
system calls. The rest of each total is the harness: it builds the history,
drops it and checks the outcome. The final drop of the cache closes the file
and deletes it. That `wipe` costs 1,115,974 (4.03%) in the `store` run.

### `store`, a fresh write, N=1000

| Entry | Inclusive Ir | Share |
|---|---:|---:|
| `store_marked` | 17,289,959 | 100.00% |
| `write_run` | 16,414,403 | 94.94% |
| SQLite insert, `execute_with_bound_parameters` | 7,662,634 | 44.32% |
| Event encode, `WorkflowEvent::serialize` | 4,410,098 | 25.51% |
| `Sealer::seal` | 2,664,819 | 15.41% |
| AES-GCM `encrypt_inout_detached` | 2,308,510 | 13.35% |
| Index key compare, `vdbeRecordCompareString` | 1,474,030 | 8.53% |
| `ensure_file`, which creates the file | 871,982 | 5.04% |
| `aad` | 145,537 | 0.84% |
| `event_hash`, once | 21,922 | 0.13% |

The total of this toggled run is 17,289,979. The shares are of that total.
The rows are inclusive and overlap.
The top self costs are the JSON string escape (1,883,241, 10.89%),
`sqlite3VdbeExec` (1,385,159, 8.01%) and `memset` (954,493, 5.52%). Next
come `memcpy` (925,011, 5.35%) and `sqlite3VdbeRecordCompareWithSkip`
(781,760, 4.52%). SQLite zeroes each new page, and that is most of the
`memset`. The GHASH multiply no longer shows as one large self cost. Its
work spreads over several `ghash` and `polyval` functions. None of them
costs more than 2.44%.

### `append`, per decision, N=1000

The toggled run collects 19,585,864 for 100 decisions:

| Entry | Ir per decision | Share |
|---|---:|---:|
| `measure_append` | 195,859 | 100.00% |
| `write_run` | 171,945 | 87.79% |
| SQL parse, `sqlite3_prepare_v3` | 47,863 | 24.44% |
| The three statements that `prepare_with_flags` parses | 44,805 | 22.88% |
| `event_hash`, two calls | 44,115 | 22.52% |
| SHA-256 `compress256` | 33,636 | 17.17% |
| `read_run_stamp` | 30,682 | 15.67% |
| `check_file_stamp`, from `ensure_file` | 20,838 | 10.64% |
| `Transaction::commit` | 13,813 | 7.05% |
| `RunStamp` decode | 8,378 | 4.28% |
| `Sealer::seal`, three calls | 7,762 | 3.96% |

The rows are inclusive and overlap. The three parsed statements are the file
stamp read (11,766), the run stamp read (13,196) and the run stamp upsert
(19,843). The event insert uses `prepare_cached`, so it parses once per
file. One call of `event_hash` hashes the new
last event. The other hashes the stored last event to prove the prefix
(`owner_disk_cache.rs:1128`). That second hash costs 22,075 per decision
(11.27%). The top self cost is `sha2` in its software path, 3,360,000
(17.16%).

### `load`, N=1000

| Entry | Inclusive Ir | Share |
|---|---:|---:|
| `load` | 17,223,846 | 100.00% |
| Event decode, `from_slice::<WorkflowEvent>` | 12,219,000 | 70.94% |
| `Value` decode inside it | 8,319,451 | 48.30% |
| UTF-8 checks inside it, `from_utf8` | 1,803,754 | 10.47% |
| `Sealer::open` | 2,477,102 | 14.38% |
| SQLite read, `sqlite3_step` | 1,260,154 | 7.32% |
| `aad` | 145,264 | 0.84% |
| `read_run_stamp` | 30,659 | 0.18% |
| `event_hash`, once | 21,844 | 0.13% |
| `check_file_stamp` | 21,098 | 0.12% |

The toggled run collects 17,223,868. The rows are inclusive and overlap.
The top self costs are `_int_malloc` (1,906,597, 11.07%), `from_utf8`
(1,803,754, 10.47%) and the `serde_json` string scan `skip_to_escape`
(1,296,641, 7.53%).

### `entity`, 121,882 bytes

`store_entity` costs 4,285,308 inside the wrapper. The shares below are of
the toggled total, 4,285,320. `write_entity` is 3,410,216 (79.58%), and
`ensure_file` is 871,365 (20.33%). The snapshot encode costs 2,232,163
(52.09%), and the seal costs 711,262 (16.60%). The JSON string escape
(928,069, 21.66%) and `memcpy` (718,223, 16.76%) lead the self costs.

`load_entity` costs 9,157,923. The decode of the row costs 8,400,858
(91.73%), and `Sealer::open` costs 588,783 (6.43%). `_int_malloc` leads the
self costs at 1,762,100 (19.24%).

## Scaling

Callgrind, total `Ir` of the harness and the inclusive cost of the measured
entry point. The unit cost divides the entry cost by the events, the
decisions or the state bytes.

| Shape | N | Total Ir | Entry Ir | Unit cost |
|---|---:|---:|---:|---:|
| `store` | 100 | 4,125,830 | 2,493,563 | 24,936 per event |
| `store` | 1000 | 27,678,900 | 17,289,979 | 17,290 per event |
| `store` | 2000 | 54,217,816 | 34,102,417 | 17,051 per event |
| `store` | 4000 | 107,261,485 | 67,796,939 | 16,949 per event |
| `load` | 100 | 6,258,469 | 1,805,251 | 18,053 per event |
| `load` | 1000 | 48,164,229 | 17,223,868 | 17,224 per event |
| `load` | 2000 | 95,118,798 | 34,380,124 | 17,190 per event |
| `load` | 4000 | 189,703,684 | 68,378,249 | 17,095 per event |
| `append` | 100 | 25,383,996 | 19,273,689 | 192,737 per decision |
| `append` | 1000 | 49,197,954 | 19,585,864 | 195,859 per decision |
| `append` | 2000 | 75,773,132 | 19,620,832 | 196,208 per decision |
| `append` | 4000 | 128,888,917 | 19,675,170 | 196,752 per decision |

The N=2000 rows are a check size between the others.
The `store` row at N=100 includes the file creation, 871,973 (34.97% of the
entry). Each partition file pays it once per tenure. `write_run` leaves it
out, and costs 16,180, 16,414, 16,613 and 16,730 per event. The `load`
slope is 17,132 per event from N=100 to N=1000, and 17,051 from N=1000 to
N=4000. Both are linear. An append decision does not depend on the history
length. From N=100 to N=4000 it grows by 2.1%.

The `entity` shape, by the size of the encoded state:

| N | State bytes | Total Ir | `store_entity` | Per byte, without the file creation | `load_entity` | Per byte |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 11,922 | 5,930,485 | 1,253,872 | 32.1 | 957,957 | 80.4 |
| 1000 | 121,882 | 44,791,625 | 4,285,320 | 28.0 | 9,157,946 | 75.1 |
| 2000 | 245,662 | 88,745,881 | 7,724,187 | 27.9 | 18,319,348 | 74.6 |
| 4000 | 493,222 | 175,212,980 | 14,411,987 | 27.5 | 36,622,883 | 74.3 |

The entity row is linear in its size.

### The superlinear term

Each event insert seeks the index of `PRIMARY KEY (exec_id, seq)`
(`owner_disk_cache.rs:90`). The key starts with the run id as 36 bytes of
text, so each compare reads that text first. The insert cost per event grows
with the index depth:

| N | Insert per event | Key compare per event | Compare share of `store` |
|---:|---:|---:|---:|
| 100 | 6,474 | 882 | 3.54% |
| 1000 | 7,663 | 1,474 | 8.53% |
| 2000 | 7,913 | 1,642 | 9.63% |
| 4000 | 8,134 | 1,807 | 10.66% |

That is an O(log N) term per event, so a fresh store is O(N log N). The
term is small. From N=100 to N=4000, `write_run` grows by 3.4% per event.
Every run of a partition shares one index, so the depth also grows with the
other runs in the file. That case was not measured.

## Allocations (dhat)

Dhat at N=1000, with `--num-callers=64`. The entry columns sum the
allocation points whose stack holds the wrapper.

| Shape | Total bytes | Blocks | Entry point | Entry bytes | Entry blocks |
|---|---:|---:|---|---:|---:|
| `store` | 4,026,794 | 22,861 | `store` | 2,293,807 | 5,807 |
| `load` | 6,238,100 | 39,876 | `load` | 2,207,175 | 17,012 |
| `append` | 7,457,934 | 31,878 | 100 decisions | 2,807,952 | 5,619 |
| `entity` | 7,627,973 | 55,662 | `store_entity` | 1,068,830 | 567 |
| `entity` | | | `load_entity` | 1,898,501 | 13,023 |

The peaks are 2,096,432 bytes (`store`), 3,714,913 (`load`), 2,618,674
(`append`) and 4,781,971 (`entity`). Each run ends with 548 bytes in 2
blocks. A fresh store allocates about 5.8 blocks per event, and a load about
17. One append decision allocates 28,080 bytes in 56 blocks. `store_entity`
allocates 8.8 times the size of its state. A build before the last harness
change counted 55 blocks per decision. Whether `realloc` grows a buffer in
place depends on the heap layout.

## Findings

### Where the cost goes

- **A fresh store** spends 44.32% in the SQLite insert, 25.51% in the event
  encode and 15.41% in the seal. It is linear in N, with the small index term
  above.
- **A load** spends 70.94% in the event decode and 14.38% in the unseal.
  The SQLite read is 7.32%. It is linear in N.
- **An append decision** is mostly fixed work. The two row inserts cost
  25,844 and the two encodes 8,991. All three seals cost 7,762. That is
  42,597 (21.75%). The rest runs on every store, whatever the batch size.
- **The entity row** spends most of its time in the JSON encode and decode of
  the state. Per byte, an entity load costs about 2.7 times a store.

AES-GCM costs 2,309 `Ir` per event in a fresh store. No soft AES code
runs: AES uses the AES-NI backend (`aes::backends::x86_aes`). GHASH uses
the CLMUL backend (`polyval::backend::intrinsics`). `sha2` takes its
software path (`soft::unroll::compress`), because valgrind does not offer
SHA-NI. On a host with SHA-NI, the hash share of an append decision is
likely lower. That was not measured.

### Ideas, not implemented

| Idea | Location | Upper bound |
|---|---|---|
| Cache the file stamp read, the run stamp read and the run stamp upsert with `prepare_cached`. | `owner_disk_cache.rs:1012`, `:1024`, `:1169` | 22.88% of an append decision (44,805 `Ir`). |
| Keep the hash of the stored last event in memory, so a store need not hash that event again. | `owner_disk_cache.rs:1128` | 11.27% of an append decision (22,075 `Ir`). |
| Key the event rows by the 16 bytes of the run id, not its 36-byte text. | `owner_disk_cache.rs:87` | 8.53% of a fresh store at N=1000, and 10.66% at N=4000. |
| Check the UTF-8 of each plaintext once, then decode with `serde_json::from_str`. | `owner_disk_cache.rs:1103` | 10.47% of a load. |
| Seal the row in one buffer, without the two copies in `Sealer::seal`. | `owner_disk_cache.rs:282` | 16.76% of `store_entity`, the whole `memcpy` self cost. |

Each bound is the measured cost of the code that the idea removes. The
replacement work is not subtracted. A cached statement still resets and binds.
The 16-byte key still compares 16 bytes. The single UTF-8 check is one more
pass.

The second idea changes a validity check. The stored hash proves that the
cached prefix ends with the same event as the new state. Code that skips the
hash must give the same proof another way. The first idea keeps every check
as it is.

None of these ideas changes the order of growth. A store and a load stay
linear in the history. In production, a decision also pays for its Postgres
work, so these shares of the cache are not shares of a decision.

The cost of a disk hit against a Postgres read is in
[the R&D note](rnd/2026-10-23-owner-disk-cache.md). The SQL that reads the
entity row has its own page,
[`performance-entity-sql.md`](performance-entity-sql.md).

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.1, measured 2026-10-27.
