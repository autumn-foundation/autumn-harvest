# Entity snapshot replay: the cold replay of a keyed entity's state snapshots

This page profiles the replay of a keyed entity (issue #2259). The loop of
`Entity::run` records a state snapshot at the start of each run and after
each op (`autumn-harvest/src/entity.rs:531`, `:571`). A warm decision resumes
its resident future (`autumn-harvest/src/worker.rs:27022`) and replays
nothing. A cold decision replays every op since the last checkpoint. That
happens on a cache miss (`worker.rs:23032`), on an owner disk-cache hit
(`worker.rs:23100`) and when the resident future declines
(`worker.rs:27037`). The worker then builds the context with
`build_task_context` (`worker.rs:27042`) and drives it with
`drive_workflow_keep` (`worker.rs:27077`). Each replayed op runs
`record_snapshot` (`entity.rs:680`) and `Published::of` (`entity.rs:572`).

Every number below is a deterministic instruction count (`valgrind --tool=callgrind`) of the commands below. Wall-clock timing is not admissible evidence on this shared-vCPU machine.

The page was re-measured after the merge of PR #2355 (Rust 1.99.1 and
aes-gcm 0.11). The headline total moved from 3,900,383,350 to 3,913,488,409
`Ir`. The same merge brought PR #2357, which changed the signal stash that
the `batched` layout uses.

**Outcome: baseline only. No code change.**

The replay is O(M×S) for M ops with a state of S bytes. A 20 KB cart costs
171.9 `Ir` per snapshot byte at N=800, and 168.1 to 172.7 at each size and
layout. A counter costs about 26,000 to 28,000 `Ir` per op at each size of
the `interleaved` layout. Per op, the replay makes about eight full passes
over the state. It builds a live
snapshot and discards it. It clones the recorded value and decodes the clone.
It serializes the state again for the query view and frees the old view. It
copies the state for the handler and frees the old state. Ideas 1 to 3 below
remove four of these passes and one byte count. Together they bound a
saving of 56.6% of the cart replay.

## Workload

[`benches/entity_snapshot_replay_profile.rs`](../autumn-harvest/benches/entity_snapshot_replay_profile.rs)
runs `Entity::from_input(ctx, input).run(handler)` over N ops.
`ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE` picks the state:

- `counter`: an `i64`. Each op adds 1 to 97. The last snapshot is 84 bytes.
- `cart`: a shopping cart. The first 200 ops each add an item of about 100
  bytes. Each later op sets the quantity of one item. The last snapshot is
  20,712 bytes, with 200 items.

`ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT` picks the history layout:

- `interleaved` (default): one op per decision, as a steady trickle of ops
  makes. Each op signal comes before its snapshot and its checkpoint
  decision.
- `batched`: every op signal arrives before the first decision, as a burst
  makes.

`WorkflowTestEnv` records each history with a frozen clock and writes it to
a file. In the `interleaved` layout each decision replays the whole history
so far, so the recording is O(N²). The cart recording at N=800 took about
five minutes outside valgrind. The measured run reads the file and replays
it once. Two recordings of each history gave the same file, byte for byte.

The replay calls `executor::run_workflow` (`autumn-harvest/src/executor.rs:1029`)
through the bench function `replay`. It reaches the same `build_task_context`
(`executor.rs:2170`) and `drive_workflow` (`executor.rs:2349`) as the cold
decision above. `WorkflowReplayer::replay_from_events` is not used, because
it runs the strict replay path that a worker does not run.
`drive_query_replay` is not used either. It serves the state query, which the
projection of issue #2259 now answers without a replay. All three paths run
the same `record_snapshot`.

The inclusive cost of `replay` leaves out the file read, its decode, the
history check and the history clone. The harness asserts that the replay
suspends at the next op wait with no other command. It also asserts that
the handler ran once per op and that the last recorded state is the expected
one.

```bash
BIN=$(cargo bench -p autumn-harvest --features testing --bench owner_kernels_profile \
  --bench entity_snapshot_replay_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="entity_snapshot_replay_profile") | .executable')
for layout in interleaved batched; do
  for shape in counter cart; do
    for n in 50 200 800; do
      export ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT=$layout ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE=$shape \
        ENTITY_SNAPSHOT_REPLAY_PROFILE_N=$n ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY=$shape-$layout-$n.json
      ENTITY_SNAPSHOT_REPLAY_PROFILE_MODE=record "$BIN"
      valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
        --callgrind-out-file="cg.$shape.$layout.$n" "$BIN"
    done
  done
done
export ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT=interleaved ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE=cart \
  ENTITY_SNAPSHOT_REPLAY_PROFILE_N=800 ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY=cart-interleaved-800.json
callgrind_annotate --inclusive=yes --threshold=99 cg.cart.interleaved.800
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='entity_snapshot_replay_profile::replay' --callgrind-out-file=cg.replay "$BIN"
callgrind_annotate --threshold=99 cg.replay
valgrind --tool=dhat --num-callers=100 --dhat-out-file=dhat.json "$BIN"
```

## Profile

Headline: `cart`, `interleaved`, N=800, run with the commands above. Two
callgrind runs gave a total of 3,913,488,409 `Ir` each. `replay` is
2,495,319,396 `Ir` in both, 63.76% of the total. The setup is the rest: the
file decode costs 721,605,329, the history clone 271,900,379, the history
check 242,866,269 and the drop of the recording 181,228,763.

The rows are inclusive and nest, so they do not sum.

| Function | Calls | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|---:|
| `replay` | 1 | 2,495,319,396 | 100.00% |
| The entity loop (`cart::{closure#0}`, with `run_loop` inlined) | 1 | 2,242,885,110 | 89.88% |
| `record_snapshot` | 801 | 1,245,373,750 | 49.91% |
| `to_value` of the live snapshot, in `record_snapshot` (`entity.rs:686`) | 801 | 410,495,879 | 16.45% |
| `WorkflowContext::record_entity_snapshot` (`autumn-harvest/src/context.rs:5261`) | 801 | 468,876,133 | 18.79% |
| `Value::clone` in `match_entity_snapshot` (`autumn-harvest/src/replay.rs:6374`) | 801 | 289,965,996 | 11.62% |
| The drop of the unused live snapshot, in `record_entity_snapshot` | 801 | 178,527,319 | 7.15% |
| `from_value::<EntitySnapshot<Cart>>` (`entity.rs:697`) | 801 | 308,946,990 | 12.38% |
| The drop of the handler result, which the snapshot replaces | 801 | 56,886,440 | 2.28% |
| `Published::of`, a second `to_value` of the state (`entity.rs:723`) | 801 | 398,817,186 | 15.98% |
| The drop of the old published value (`entity.rs:572`) | 800 | 200,400,016 | 8.03% |
| `checkpoint_due` | 800 | 226,568,315 | 9.08% |
| `loaded_history_bytes`, inside `checkpoint_due` (`entity.rs:848`) | 1 | 224,120,393 | 8.98% |
| The state copy for the handler (`entity.rs:545`) | 800 | 112,859,786 | 4.52% |
| The free of the old state | 280,599 | 45,799,529 | 1.84% |
| The op wait, the op decode, `json_len` and the handler | | 10,494,913 | 0.42% |
| The drop of the context and its history | 1 | 252,218,352 | 10.11% |

`match_entity_snapshot` clones only in its `Matched` arm. The clone runs 801
times, so every recorded snapshot matched.

The split between these rows moves with the heap layout, by up to 40% for a
small row. The `replay` row moved by 0.03%. See
[Nondeterminism](#nondeterminism).

A second run collected only inside `replay` (`--toggle-collect`). It
collected 2,495,319,396 `Ir`, the same as the inclusive row. Its flat self
view:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `_int_malloc` | 506,610,011 | 20.30% |
| `_int_free` | 305,972,427 | 12.26% |
| `malloc_consolidate` | 191,603,049 | 7.68% |
| `malloc` | 147,925,670 | 5.93% |
| `free` | 132,142,677 | 5.30% |
| `format_escaped_str_contents` (the serialization in `loaded_history_bytes`) | 112,610,212 | 4.51% |
| `BTreeMap<String, Value>::insert` | 102,046,767 | 4.09% |
| `BTreeMap` `IntoIter::dying_next` (the drop of an object) | 101,711,855 | 4.08% |
| `unlink_chunk` | 78,717,652 | 3.15% |
| `memcpy` | 71,903,947 | 2.88% |

The `malloc` and `free` rows add the `malloc.c` and `arena.c` entries.
Grouped, the allocator entries (`malloc.c`, `arena.c`, `realloc` and the
`__rdl_` and `__rust_` shims) cost 1,462,249,176 `Ir` (58.60%). The `BTreeMap`
entries cost 404,667,188 `Ir` (16.22%).

### The counter

For the `counter` at N=800, `replay` costs 21,346,398 `Ir` in both runs. The
program total is 46,064,435 in both. The state is one number, so the fixed
costs lead:

| Function | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|
| `checkpoint_due` | 8,458,618 | 39.63% |
| `loaded_history_bytes`, once | 6,102,098 | 28.59% |
| The replay of the checkpoint decision (`side_effect`) | 2,050,118 | 9.60% |
| `record_snapshot` | 6,082,466 | 28.49% |
| `record_entity_snapshot`, the clone and the drop of the live value | 2,476,183 | 11.60% |
| `from_value::<EntitySnapshot<i64>>` | 1,718,946 | 8.05% |
| `to_value` of `EntityStats` in the live snapshot | 1,485,099 | 6.96% |
| The op decode | 1,745,196 | 8.18% |
| The op wait | 1,239,046 | 5.80% |
| `json_len` of the op | 774,714 | 3.63% |
| The drop of the context and its history | 2,673,329 | 12.52% |

## Scaling

Inclusive `Ir` of `replay`. The snapshot bytes are the sum of the serialized
snapshots in the history.

| Shape | Layout | N | Events | Snapshot bytes | `replay` | Per op | Per snapshot byte |
|---|---|---:|---:|---:|---:|---:|---:|
| `counter` | interleaved | 50 | 152 | 4,109 | 1,370,720 | 27,414.4 | 333.6 |
| `counter` | interleaved | 200 | 602 | 16,510 | 5,588,585 | 27,942.9 | 338.5 |
| `counter` | interleaved | 400 | 1,202 | 33,286 | 11,130,132 | 27,825.3 | 334.4 |
| `counter` | interleaved | 800 | 2,402 | 66,886 | 21,346,398 | 26,683.0 | 319.1 |
| `cart` | interleaved | 50 | 152 | 136,863 | 23,010,065 | 460,201.3 | 168.1 |
| `cart` | interleaved | 200 | 602 | 2,092,438 | 359,842,493 | 1,799,212.5 | 172.0 |
| `cart` | interleaved | 400 | 1,202 | 6,234,825 | 1,069,762,573 | 2,674,406.4 | 171.6 |
| `cart` | interleaved | 800 | 2,402 | 14,519,625 | 2,495,319,396 | 3,119,149.2 | 171.9 |
| `counter` | batched | 50 | 152 | 4,109 | 1,542,675 | 30,853.5 | 375.4 |
| `counter` | batched | 200 | 602 | 16,510 | 6,278,008 | 31,390.0 | 380.3 |
| `counter` | batched | 800 | 2,402 | 66,886 | 24,808,934 | 31,011.2 | 370.9 |
| `cart` | batched | 50 | 152 | 136,863 | 23,199,996 | 463,999.9 | 169.5 |
| `cart` | batched | 200 | 602 | 2,092,438 | 361,376,385 | 1,806,881.9 | 172.7 |
| `cart` | batched | 800 | 2,402 | 14,519,625 | 2,489,252,235 | 3,111,565.3 | 171.4 |

The N=400 rows are a check size that the loop above does not run. Record
them with `n` set to 400.

The `counter` costs about the same per op at each N, so its replay is linear
in N. The `cart` grows its state for 200 ops, so its cost per op grows. Its
cost per snapshot byte stays within 168.1 to 172.7. So the replay is linear
in the sum of the snapshot sizes, O(M×S), and no worse.

The `batched` counter costs 12.3% to 16.2% more per op. At N=800 the program
totals differ by 3,462,534 `Ir`. A diff of the self costs puts 2,526,829 of
them in the allocator entries. The signal stash functions, with the map and
the SipHash of its signal names, add 779,808 `Ir`. The `cart` layouts differ
by 0.83% or less. At N=50 and N=200 the `batched` cart costs more, in the
allocator and the signal stash. At N=800 it costs 0.24% less. There its rows
move as in the heap-layout run under [Nondeterminism](#nondeterminism).

## Allocations

Dhat, `cart`, `interleaved`, N=800, with 100 frames per stack: 672,868,551
bytes in 5,287,263 blocks in total. The peak is 215,683,409 bytes in
2,006,948 blocks. `replay` allocates 390,690,771 bytes in 3,270,263 blocks,
4,088 blocks and 488,363 bytes per op:

| Caller inside `replay` | Blocks | Bytes |
|---|---:|---:|
| `Value::clone` in `match_entity_snapshot` | 990,311 | 105,240,756 |
| `to_value` of the live snapshot | 985,505 | 104,703,285 |
| `Published::of` | 983,903 | 104,193,048 |
| The state copy for the handler | 280,798 | 19,335,328 |
| `loaded_history_bytes` | 9,306 | 45,941,248 |
| `from_value::<EntitySnapshot<Cart>>` | 800 | 8,966,400 |
| The rest | 19,640 | 2,310,706 |

The decode allocates almost nothing, because it moves each string out of the
cloned value. The clone, the live snapshot and the query view each allocate
about 1,230 blocks per op. The setup allocates the rest: the file decode
1,005,673 blocks, the history clone 1,001,113 and the history check 10,118.

For the `counter` at N=800, `replay` allocates 4,068,051 bytes in 26,452
blocks, 33 blocks per op.

## Findings

### Where the cost goes

Per replayed op, the state passes through these copies:

1. `record_snapshot` serializes the live state to a `Value`
   (`entity.rs:686`). A replay discards it, and
   `record_entity_snapshot` frees it (`context.rs:5267`). Together: 23.61%.
2. `match_entity_snapshot` clones the recorded value (`replay.rs:6374`):
   11.62%.
3. `from_value` decodes the clone into the state (`entity.rs:697`): 12.38%.
4. `Published::of` serializes the state again for the query view
   (`entity.rs:723`), and the old view is freed (`entity.rs:572`). Together:
   24.01%.
5. The handler gets a copy of the state (`entity.rs:545`), and the old state
   is freed. Together: 6.36%.

`loaded_history_bytes` (`replay.rs:2708`) serializes every loaded event once
per decision to count its bytes. That is 8.98% of the cart replay and 28.59%
of the counter replay. The drop of the context and its history is another
10.11%.

### Superlinear terms

None in the replay. The cost per snapshot byte is flat from N=50 to N=800 in
both layouts.

The recording is O(N²) because each of its decisions is cold.
`WorkflowTestEnv` keeps no resident future, so each decision replays the
whole history. A worker whose decisions all miss the cache pays the same sum
over the life of a run. The resident future and the checkpoint bound
limit that sum in production.

A cold decision replays every op since the last checkpoint. `checkpoint_due`
(`entity.rs:831`) bounds that history at 10,000 events or half of the 50 MiB
byte cap. At 171.9 `Ir` per snapshot byte, a history of 25 MiB of snapshots
would cost about 4.5G `Ir` to replay. That is an estimate, not a
measurement.

### Ideas, not implemented

1. **Skip the live snapshot in a replay.** Pass a closure that builds the live
   value, and call it only at the frontier. The bound is 589,023,198 `Ir`,
   23.61% of the cart replay.
2. **Publish the query view lazily.** The worker calls no query handler
   during a decision. A view built when a query asks for it saves the
   second serialization and its free. The bound is 599,217,202 `Ir`,
   24.01%.
3. **Count the loaded bytes at load.** The store reads each event row, so it
   can keep the byte sizes. The bound is 224,120,393 `Ir`, 8.98% of the
   cart replay and 28.59% of the counter replay.
4. **Decode from the recorded value without a clone.** `EntitySnapshot<S>` can
   deserialize from `&Value`. The bound is the clone, 289,965,996 `Ir`
   (11.62%). The decode would then copy each string, so the real saving is
   lower.

Ideas 1, 2 and 3 together bound a saving of 1,412,360,793 `Ir`, 56.60% of
the cart replay. Each keeps the recorded history and the replay result as
they are. With the other heap layout below, the same sum was 57.1%.

## Not measured

- The live path of a warm decision. It serializes the snapshot, then
  `record_entity_snapshot` serializes it again to a string for the size cap
  and walks it for the depth cap (`context.rs:5284`).
- The history clone of the worker (`worker.rs:27044`). The harness clones
  the history outside `replay`.

## Nondeterminism

The recording uses a frozen clock and fixed ops. The replay uses a fixed
execution id. The two headline runs of each shape matched exactly. The
`WorkflowTestEnv` run id is random, but it is not in the recorded events.

The heap layout depends on the environment and the length of the history
path. A run of the same `cart` file at N=800 through a longer absolute path
gave a total of 3,914,203,932 `Ir`, 0.02% more than above. Almost all of that
difference is in `replay`. It moved by 712,175 `Ir` (0.03%), to
2,496,031,571. The rest of the program moved by less than 3,500 `Ir`.

The rows inside `replay` moved more, because `malloc_consolidate` ran inside
other calls. In that run, `Published::of` cost 433,027,419 `Ir`, 8.6% more.
The state copy cost 135,211,347, 19.8% more. The drop of the handler result
cost 34,186,818, 39.9% less. The `cart` replay at N=50 moved by less than 0.001%. So
compare runs made with the same command, and treat a row split as an
estimate.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.1, measured 2026-10-27.
