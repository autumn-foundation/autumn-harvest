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

**Outcome: baseline only. No code change.**

The replay is O(M×S) for M ops with a state of S bytes. A 20 KB cart costs
170.8 `Ir` per snapshot byte at N=800, and 166.9 to 171.4 at each size and
layout. A counter costs about 26,000 to 28,000 `Ir` per op at each size of
the `interleaved` layout. Per op, the replay makes about eight full passes
over the state. It builds a live
snapshot and discards it. It clones the recorded value and decodes the clone.
It serializes the state again for the query view and frees the old view. It
copies the state for the handler and frees the old state. Ideas 1 to 3 below
remove four of these passes and one byte count. Together they bound a
saving of 56.4% of the cart replay.

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
callgrind runs gave a total of 3,900,383,350 `Ir` each. `replay` is
2,480,001,533 `Ir` in both, 63.58% of the total. The setup is the rest: the
file decode costs 721,574,156, the history clone 271,757,074, the history
check 242,869,653 and the drop of the recording 183,609,858.

The rows are inclusive and nest, so they do not sum.

| Function | Calls | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|---:|
| `replay` | 1 | 2,480,001,533 | 100.00% |
| The entity loop (`cart::{closure#0}`, with `run_loop` inlined) | 1 | 2,228,304,183 | 89.85% |
| `record_snapshot` | 801 | 1,247,747,629 | 50.31% |
| `to_value` of the live snapshot, in `record_snapshot` (`entity.rs:686`) | 801 | 410,498,637 | 16.55% |
| `WorkflowContext::record_entity_snapshot` (`autumn-harvest/src/context.rs:5261`) | 801 | 471,242,741 | 19.00% |
| `Value::clone` in `match_entity_snapshot` (`autumn-harvest/src/replay.rs:6230`) | 801 | 289,965,996 | 11.69% |
| The drop of the unused live snapshot, in `record_entity_snapshot` | 801 | 180,897,809 | 7.29% |
| `from_value::<EntitySnapshot<Cart>>` (`entity.rs:697`) | 801 | 308,950,194 | 12.46% |
| The drop of the handler result, which the snapshot replaces | 801 | 56,886,440 | 2.29% |
| `Published::of`, a second `to_value` of the state (`entity.rs:723`) | 801 | 398,815,968 | 16.08% |
| The drop of the old published value (`entity.rs:572`) | 800 | 202,765,520 | 8.18% |
| `checkpoint_due` | 800 | 207,272,434 | 8.36% |
| `loaded_history_bytes`, inside `checkpoint_due` (`entity.rs:848`) | 1 | 204,837,018 | 8.26% |
| The state copy for the handler (`entity.rs:545`) | 800 | 112,859,786 | 4.55% |
| The free of the old state | 280,599 | 45,799,529 | 1.85% |
| The op wait, the op decode, `json_len` and the handler | | 10,475,718 | 0.42% |
| The drop of the context and its history | 1 | 251,481,679 | 10.14% |

`match_entity_snapshot` clones only in its `Matched` arm. The clone runs 801
times, so every recorded snapshot matched.

The split between these rows moves with the heap layout, by up to 40% for a
small row. The `replay` row moved by 0.03%. See
[Nondeterminism](#nondeterminism).

A second run collected only inside `replay` (`--toggle-collect`). It
collected 2,480,001,533 `Ir`, the same as the inclusive row. Its flat self
view:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `_int_malloc` | 506,610,011 | 20.43% |
| `_int_free` | 305,972,427 | 12.34% |
| `malloc_consolidate` | 191,603,049 | 7.73% |
| `malloc` | 147,925,670 | 5.96% |
| `free` | 132,142,677 | 5.33% |
| `format_escaped_str_contents` (the serialization in `loaded_history_bytes`) | 112,610,212 | 4.54% |
| `BTreeMap<String, Value>::insert` | 102,046,767 | 4.11% |
| `BTreeMap` `IntoIter::dying_next` (the drop of an object) | 101,711,855 | 4.10% |
| `unlink_chunk` | 78,717,652 | 3.17% |
| `memcpy` | 71,903,789 | 2.90% |

The `malloc` and `free` rows add the `malloc.c` and `arena.c` entries.
Grouped, the allocator entries (`malloc.c`, `arena.c`, `realloc` and the
`__rdl_` and `__rust_` shims) cost 1,462,249,176 `Ir` (58.96%). The `BTreeMap`
entries cost 416,014,405 `Ir` (16.77%).

### The counter

For the `counter` at N=800, `replay` costs 21,058,349 `Ir` in both runs. The
program total is 45,764,805 in both. The state is one number, so the fixed
costs lead:

| Function | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|
| `checkpoint_due` | 8,193,050 | 38.91% |
| `loaded_history_bytes`, once | 5,849,085 | 27.78% |
| The replay of the checkpoint decision (`side_effect`) | 2,047,163 | 9.72% |
| `record_snapshot` | 6,088,174 | 28.91% |
| `record_entity_snapshot`, the clone and the drop of the live value | 2,477,077 | 11.76% |
| `from_value::<EntitySnapshot<i64>>` | 1,722,150 | 8.18% |
| `to_value` of `EntityStats` in the live snapshot | 1,485,911 | 7.06% |
| The op decode | 1,743,364 | 8.28% |
| The op wait | 1,228,105 | 5.83% |
| `json_len` of the op | 774,714 | 3.68% |
| The drop of the context and its history | 2,654,159 | 12.60% |

## Scaling

Inclusive `Ir` of `replay`. The snapshot bytes are the sum of the serialized
snapshots in the history.

| Shape | Layout | N | Events | Snapshot bytes | `replay` | Per op | Per snapshot byte |
|---|---|---:|---:|---:|---:|---:|---:|
| `counter` | interleaved | 50 | 152 | 4,109 | 1,352,645 | 27,052.9 | 329.2 |
| `counter` | interleaved | 200 | 602 | 16,510 | 5,513,799 | 27,569.0 | 334.0 |
| `counter` | interleaved | 400 | 1,202 | 33,286 | 10,985,582 | 27,464.0 | 330.0 |
| `counter` | interleaved | 800 | 2,402 | 66,886 | 21,058,349 | 26,322.9 | 314.8 |
| `cart` | interleaved | 50 | 152 | 136,863 | 22,844,095 | 456,881.9 | 166.9 |
| `cart` | interleaved | 200 | 602 | 2,092,438 | 357,956,662 | 1,789,783.3 | 171.1 |
| `cart` | interleaved | 400 | 1,202 | 6,234,825 | 1,065,419,198 | 2,663,548.0 | 170.9 |
| `cart` | interleaved | 800 | 2,402 | 14,519,625 | 2,480,001,533 | 3,100,001.9 | 170.8 |
| `counter` | batched | 50 | 152 | 4,109 | 1,474,330 | 29,486.6 | 358.8 |
| `counter` | batched | 200 | 602 | 16,510 | 5,993,342 | 29,966.7 | 363.0 |
| `counter` | batched | 800 | 2,402 | 66,886 | 23,658,817 | 29,573.5 | 353.7 |
| `cart` | batched | 50 | 152 | 136,863 | 22,989,336 | 459,786.7 | 168.0 |
| `cart` | batched | 200 | 602 | 2,092,438 | 358,733,376 | 1,793,666.9 | 171.4 |
| `cart` | batched | 800 | 2,402 | 14,519,625 | 2,469,869,669 | 3,087,337.1 | 170.1 |

The N=400 rows are a check size that the loop above does not run. Record
them with `n` set to 400.

The `counter` costs about the same per op at each N, so its replay is linear
in N. The `cart` grows its state for 200 ops, so its cost per op grows. Its
cost per snapshot byte stays within 166.9 to 171.4. So the replay is linear
in the sum of the snapshot sizes, O(M×S), and no worse.

The `batched` counter costs 8.7% to 12.3% more per op. At N=800 the program
totals differ by 2,600,572 `Ir`. A diff of the self costs puts 2,520,652 of
them in the allocator entries. The signal stash functions add 116,836 `Ir`.
The `cart` layouts differ by 0.64% or less. That is within the heap-layout
noise of the `cart` shape.

## Allocations

Dhat, `cart`, `interleaved`, N=800, with 100 frames per stack: 672,868,479
bytes in 5,287,263 blocks in total. The peak is 215,683,337 bytes in
2,006,948 blocks. `replay` allocates 390,690,699 bytes in 3,270,263 blocks,
4,088 blocks and 488,363 bytes per op:

| Caller inside `replay` | Blocks | Bytes |
|---|---:|---:|
| `Value::clone` in `match_entity_snapshot` | 990,311 | 105,240,756 |
| `to_value` of the live snapshot | 985,505 | 104,703,285 |
| `Published::of` | 983,903 | 104,193,048 |
| The state copy for the handler | 280,798 | 19,335,328 |
| `loaded_history_bytes` | 9,306 | 45,941,248 |
| `from_value::<EntitySnapshot<Cart>>` | 800 | 8,966,400 |
| The rest | 19,640 | 2,310,634 |

The decode allocates almost nothing, because it moves each string out of the
cloned value. The clone, the live snapshot and the query view each allocate
about 1,230 blocks per op. The setup allocates the rest: the file decode
1,005,673 blocks, the history clone 1,001,113 and the history check 10,118.

For the `counter` at N=800, `replay` allocates 4,067,979 bytes in 26,452
blocks, 33 blocks per op.

## Findings

### Where the cost goes

Per replayed op, the state passes through these copies:

1. `record_snapshot` serializes the live state to a `Value`
   (`entity.rs:686`). A replay discards it, and
   `record_entity_snapshot` frees it (`context.rs:5267`). Together: 23.85%.
2. `match_entity_snapshot` clones the recorded value (`replay.rs:6230`):
   11.69%.
3. `from_value` decodes the clone into the state (`entity.rs:697`): 12.46%.
4. `Published::of` serializes the state again for the query view
   (`entity.rs:723`), and the old view is freed (`entity.rs:572`). Together:
   24.26%.
5. The handler gets a copy of the state (`entity.rs:545`), and the old state
   is freed. Together: 6.40%.

`loaded_history_bytes` (`replay.rs:2701`) serializes every loaded event once
per decision to count its bytes. That is 8.26% of the cart replay and 27.78%
of the counter replay. The drop of the context and its history is another
10.14%.

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
byte cap. At 170.8 `Ir` per snapshot byte, a history of 25 MiB of snapshots
would cost about 4.5G `Ir` to replay. That is an estimate, not a
measurement.

### Ideas, not implemented

1. **Skip the live snapshot in a replay.** Pass a closure that builds the live
   value, and call it only at the frontier. The bound is 591,396,446 `Ir`,
   23.85% of the cart replay.
2. **Publish the query view lazily.** The worker calls no query handler
   during a decision. A view built when a query asks for it saves the
   second serialization and its free. The bound is 601,581,488 `Ir`,
   24.26%.
3. **Count the loaded bytes at load.** The store reads each event row, so it
   can keep the byte sizes. The bound is 204,837,018 `Ir`, 8.26% of the
   cart replay and 27.78% of the counter replay.
4. **Decode from the recorded value without a clone.** `EntitySnapshot<S>` can
   deserialize from `&Value`. The bound is the clone, 289,965,996 `Ir`
   (11.69%). The decode would then copy each string, so the real saving is
   lower.

Ideas 1, 2 and 3 together bound a saving of 1,397,814,952 `Ir`, 56.36% of
the cart replay. Each keeps the recorded history and the replay result as
they are. With the other heap layout below, the same sum was 56.9%.

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
gave a total of 3,884,238,044 `Ir`, 0.41% less than above. Most of that
difference is in the setup. There, the history check grows its buffers with
`realloc`, and one layout copies more of them. `replay` moved by 684,892 `Ir`
(0.03%), to 2,480,686,425.

The rows inside `replay` moved more, because `malloc_consolidate` ran inside
other calls. In that run, `Published::of` cost 433,016,766 `Ir`, 8.6% more.
The state copy cost 135,211,347, 19.8% more. The drop of the handler result
cost 34,186,818, 39.9% less. The `cart` replay at N=50 moved by 0.55%. So
compare runs made with the same command, and treat a row split as an
estimate.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
