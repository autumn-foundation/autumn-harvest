# `OwnerDiskCache`: store, append, load and the entity row

The owner-local disk cache (issue #2258) keeps the decoded history of each
live run in a sealed SQLite file, one file per partition.
[ADR 0012](adr/0012-owner-disk-cache.md) records the design. A decision
owner writes the cache after each committed suspension. `store_cache_entry`
(`worker.rs:29244`) calls `write_disk_entry` (`worker.rs:29522`), which calls
`DiskTier::write` (`worker.rs:22881`) for a held tenure (`worker.rs:22876`).
`DiskTier::write` (`owner_disk_cache.rs:1783`) returns at once for a history
under `owner_disk_cache_min_events` (`owner_disk_cache.rs:1794`). The default
is 100 events (`builder.rs:3898`). Otherwise it calls `store_marked`
(`owner_disk_cache.rs:1800`). The first store of a run after a takeover writes
every event. Each later store appends the new events only. On a memory miss,
a decision calls `read_disk_entry` (`worker.rs:23105`). That calls
`DiskTier::read` (`worker.rs:22840`), which reads the file only when the
in-memory index holds the run (`owner_disk_cache.rs:1732`). It then calls
`load` (`owner_disk_cache.rs:1740`). With entity SQL on, `apply_entity`
(`worker.rs:22879`) calls `store_entity` after a committed suspension with a
newer snapshot (`owner_disk_cache.rs:1651`). An entity query calls
`load_entity` (`owner_disk_cache.rs:1475`), and `store_entity` on a miss
(`owner_disk_cache.rs:1516`). `open` runs once per worker registry
(`worker.rs:1581`).

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

A fresh store and a load each cost about 17,800 instructions per event, and
both are linear in the history length. An append decision costs about 198,000
instructions. Its two new events account for at most 22.17% of that. The
rest is fixed work on every store: two stamp reads, three SQL parses, two
SHA-256 hashes and a commit.

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
heap layout. A working directory of another length moved the `store` total
by 3,281 `Ir` (0.01%). So compare runs made with the same command from the
same directory.

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
| `store` | 28,149,220 | 28,149,220 | `store`, 1,000 new events | 17,758,737 | 63.09% |
| `load` | 49,228,135 | 49,228,135 | `load`, 1,000 events | 17,817,469 | 36.19% |
| `append` | 49,886,420 | 49,886,420 | 100 × `store`, 2 new events each | 19,803,996 | 39.70% |
| `entity` | 45,502,559 | 45,502,559 | `store_entity`, 121,882 bytes | 4,639,911 | 10.20% |
| `entity` | | | `load_entity`, 121,882 bytes | 9,512,713 | 20.91% |

`open` costs 5,872 `Ir` in each of these runs. Callgrind does not count its
system calls. The rest of each total is the harness: it builds the history,
drops it and checks the outcome. The final drop of the cache closes the file
and deletes it. That `wipe` costs 1,115,974 (3.96%) in the `store` run.

### `store`, a fresh write, N=1000

| Entry | Inclusive Ir | Share |
|---|---:|---:|
| `store_marked` | 17,758,717 | 100.00% |
| `write_run` | 16,882,871 | 95.07% |
| SQLite insert, `execute_with_bound_parameters` | 7,662,634 | 43.15% |
| Event encode, `WorkflowEvent::serialize` | 4,410,091 | 24.83% |
| `Sealer::seal` | 3,112,240 | 17.53% |
| AES-GCM `encrypt_in_place_detached` | 2,757,935 | 15.53% |
| Index key compare, `vdbeRecordCompareString` | 1,474,030 | 8.30% |
| `ensure_file`, which creates the file | 872,272 | 4.91% |
| `aad` | 145,537 | 0.82% |
| `event_hash`, once | 21,922 | 0.12% |

The total of this toggled run is 17,758,737. The shares are of that total.
The rows are inclusive and overlap.
The top self costs are the JSON string escape (1,883,241, 10.60%),
`sqlite3VdbeExec` (1,385,159, 7.80%) and the CLMUL GHASH multiply
(1,141,425, 6.43%). Next come `memset` (954,493, 5.37%) and `memcpy`
(864,929, 4.87%). SQLite zeroes each new page, and that is most of the
`memset`.

### `append`, per decision, N=1000

The toggled run collects 19,803,996 for 100 decisions:

| Entry | Ir per decision | Share |
|---|---:|---:|
| `measure_append` | 198,040 | 100.00% |
| `write_run` | 173,921 | 87.82% |
| SQL parse, `sqlite3_prepare_v3` | 47,863 | 24.17% |
| The three statements that `prepare_with_flags` parses | 44,805 | 22.62% |
| `event_hash`, two calls | 44,115 | 22.28% |
| SHA-256 `compress256` | 33,636 | 16.98% |
| `read_run_stamp` | 31,260 | 15.78% |
| `check_file_stamp`, from `ensure_file` | 21,043 | 10.63% |
| `Transaction::commit` | 13,813 | 6.97% |
| `RunStamp` decode | 8,378 | 4.23% |
| `Sealer::seal`, three calls | 9,071 | 4.58% |

The rows are inclusive and overlap. The three parsed statements are the file
stamp read (11,766), the run stamp read (13,196) and the run stamp upsert
(19,843). The event insert uses `prepare_cached`, so it parses once per
file. One call of `event_hash` hashes the new
last event. The other hashes the stored last event to prove the prefix
(`owner_disk_cache.rs:1127`). That second hash costs 22,075 per decision
(11.15%). The top self cost is `sha2` in its software path, 3,360,000
(16.97%).

### `load`, N=1000

| Entry | Inclusive Ir | Share |
|---|---:|---:|
| `load` | 17,817,447 | 100.00% |
| Event decode, `from_slice::<WorkflowEvent>` | 12,221,832 | 68.59% |
| `Value` decode inside it | 8,319,451 | 46.69% |
| UTF-8 checks inside it, `from_utf8` | 1,803,754 | 10.12% |
| `Sealer::open` | 3,067,808 | 17.22% |
| SQLite read, `sqlite3_step` | 1,260,154 | 7.07% |
| `aad` | 145,264 | 0.82% |
| `read_run_stamp` | 31,251 | 0.18% |
| `event_hash`, once | 21,844 | 0.12% |
| `check_file_stamp` | 21,303 | 0.12% |

The toggled run collects 17,817,469. The rows are inclusive and overlap.
The top self costs are `_int_malloc` (1,906,597, 10.70%), `from_utf8`
(1,803,754, 10.12%) and the `serde_json` string scan `skip_to_escape`
(1,296,641, 7.28%).

### `entity`, 121,882 bytes

`store_entity` costs 4,639,899 inside the wrapper. The shares below are of
the toggled total, 4,639,911. `write_entity` is 3,764,535 (81.13%), and
`ensure_file` is 871,637 (18.79%). The snapshot encode costs 2,232,212
(48.11%), and the seal costs 1,065,702 (22.97%). The JSON string escape
(928,069, 20.00%) and `memcpy` (718,129, 15.48%) lead the self costs.

`load_entity` costs 9,512,690. The decode of the row costs 8,400,858
(88.31%), and `Sealer::open` costs 943,509 (9.92%). `_int_malloc` leads the
self costs at 1,762,100 (18.52%).

## Scaling

Callgrind, total `Ir` of the harness and the inclusive cost of the measured
entry point. The unit cost divides the entry cost by the events, the
decisions or the state bytes.

| Shape | N | Total Ir | Entry Ir | Unit cost |
|---|---:|---:|---:|---:|
| `store` | 100 | 4,174,916 | 2,541,101 | 25,411 per event |
| `store` | 1000 | 28,149,220 | 17,758,737 | 17,759 per event |
| `store` | 2000 | 55,156,160 | 35,039,199 | 17,520 per event |
| `store` | 4000 | 109,223,740 | 69,757,632 | 17,439 per event |
| `load` | 100 | 6,366,849 | 1,864,528 | 18,645 per event |
| `load` | 1000 | 49,228,135 | 17,817,469 | 17,817 per event |
| `load` | 2000 | 97,227,985 | 35,550,981 | 17,775 per event |
| `load` | 4000 | 194,093,972 | 70,806,298 | 17,702 per event |
| `append` | 100 | 25,646,704 | 19,487,297 | 194,873 per decision |
| `append` | 1000 | 49,886,420 | 19,803,996 | 198,040 per decision |
| `append` | 2000 | 76,931,911 | 19,841,253 | 198,413 per decision |
| `append` | 4000 | 131,072,319 | 19,896,303 | 198,963 per decision |

The N=2000 rows are a check size between the others.
The `store` row at N=100 includes the file creation, 872,243 (34.33% of the
entry). Each partition file pays it once per tenure. `write_run` leaves it
out, and costs 16,653, 16,883, 17,082 and 17,220 per event. The `load`
slope is 17,725 per event from N=100 to N=1000, and 17,663 from N=1000 to
N=4000. Both are linear. An append decision does not depend on the history
length. From N=100 to N=4000 it grows by 2.1%.

The `entity` shape, by the size of the encoded state:

| N | State bytes | Total Ir | `store_entity` | Per byte, without the file creation | `load_entity` | Per byte |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 11,922 | 6,002,304 | 1,288,917 | 35.0 | 993,169 | 83.3 |
| 1000 | 121,882 | 45,502,559 | 4,639,911 | 30.9 | 9,512,713 | 78.0 |
| 2000 | 245,662 | 90,176,281 | 8,438,520 | 30.8 | 19,033,839 | 77.5 |
| 4000 | 493,222 | 178,082,254 | 15,845,746 | 30.4 | 38,056,822 | 77.2 |

The entity row is linear in its size.

### The superlinear term

Each event insert seeks the index of `PRIMARY KEY (exec_id, seq)`
(`owner_disk_cache.rs:90`). The key starts with the run id as 36 bytes of
text, so each compare reads that text first. The insert cost per event grows
with the index depth:

| N | Insert per event | Key compare per event | Compare share of `store` |
|---:|---:|---:|---:|
| 100 | 6,474 | 882 | 3.47% |
| 1000 | 7,663 | 1,474 | 8.30% |
| 2000 | 7,913 | 1,642 | 9.37% |
| 4000 | 8,134 | 1,807 | 10.36% |

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

- **A fresh store** spends 43.15% in the SQLite insert, 24.83% in the event
  encode and 17.52% in the seal. It is linear in N, with the small index term
  above.
- **A load** spends 68.57% in the event decode and 17.22% in the unseal.
  The SQLite read is 7.07%. It is linear in N.
- **An append decision** is mostly fixed work. The two row inserts cost
  25,844 and the two encodes 8,991. All three seals cost 9,071. That is
  43,906 (22.17%). The rest runs on every store, whatever the batch size.
- **The entity row** spends most of its time in the JSON encode and decode of
  the state. Per byte, an entity load costs about 2.5 times a store.

AES-GCM costs 2,758 `Ir` per event in a fresh store. No `aes::soft` code
runs, and GHASH uses the CLMUL backend (`polyval::backend::clmul`). `sha2`
takes its software path (`soft::unroll::compress`), because valgrind does not
offer SHA-NI. On a host with SHA-NI, the hash share of an append decision is
likely lower. That was not measured.

### Ideas, not implemented

| Idea | Location | Upper bound |
|---|---|---|
| Cache the file stamp read, the run stamp read and the run stamp upsert with `prepare_cached`. | `owner_disk_cache.rs:1011`, `:1023`, `:1168` | 22.62% of an append decision (44,805 `Ir`). |
| Keep the hash of the stored last event in memory, so a store need not hash that event again. | `owner_disk_cache.rs:1127` | 11.15% of an append decision (22,075 `Ir`). |
| Key the event rows by the 16 bytes of the run id, not its 36-byte text. | `owner_disk_cache.rs:87` | 8.30% of a fresh store at N=1000, and 10.36% at N=4000. |
| Check the UTF-8 of each plaintext once, then decode with `serde_json::from_str`. | `owner_disk_cache.rs:1102` | 10.12% of a load. |
| Seal the row in one buffer, without the two copies in `Sealer::seal`. | `owner_disk_cache.rs:282` | 15.48% of `store_entity`, the whole `memcpy` self cost. |

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

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
