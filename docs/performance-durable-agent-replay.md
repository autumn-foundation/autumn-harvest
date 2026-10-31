# Durable `agent` replay: one op, from the entity snapshot to the transcript digest

This page profiles the replay of a durable `agent` run (issue #2263). The
workflow `agent` (`autumn-harvest-agent/src/agent/mod.rs:1206`) runs the
entity loop `Entity::run_with_wakes` (`agent/mod.rs:1247`). Each op runs
`AgentRun::handle` (`agent/mod.rs:1279`), then `work` (`agent/mod.rs:1565`)
and `run_segment_as` (`agent/mod.rs:1743`). That calls the `run_segment` of
`agent_loop` (`autumn-harvest-agent/src/workflow.rs:665`). The entity loop
copies the state for the handler (`autumn-harvest/src/entity.rs:545`). After
each op it records a snapshot (`entity.rs:571`) and publishes the query view
(`entity.rs:572`). `sqlite::register` registers the workflow
(`autumn-harvest-agent/src/sqlite.rs:54`), and so does `workflows()`
(`workflow.rs:2519`). The op signal wakes the run for a decision, and so
does each completed activity. On SQLite, each decision replays the whole
run. `drive_one_cycle_inner` (`autumn-harvest-sqlite/src/runtime.rs:3033`)
loads the history (`runtime.rs:3114`) and replays it through
`run_workflow_with_state_history_policy_and_caps` (`runtime.rs:3178`). A
cold Postgres decision replays every op since the last checkpoint through
`build_task_context` (`autumn-harvest/src/worker.rs:27042`) and
`drive_workflow_keep` (`worker.rs:27077`). The design is in
[DESIGN-2262 §1](design/DESIGN-2262.md#1-the-agent-2263).

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

With a tool call in each op, the replay of 200 ops costs 3,095,039,458
`Ir`, 15.5M per op. The default window keeps 200 messages, and op 50 fills
it. From then on each op costs 17.7M to 18.0M `Ir`, so the replay is linear
in the op count. The transcript digest of issue #2264 is the largest single
cost. It is 34.4% of the replay at 200 ops, and 28% to 55% at each measured
size. Each op checks the last sent window by hashing it again. Once the
window is full, the trim makes that check fail. Each op then hashes the
whole window again for the full form. The entity snapshot work costs 25% to
36% of the replay. The agent loop, the digest included, costs 48% to 66%.
Near op 288 the history reaches half of its 50 MiB byte cap. The entity
checkpoint then continues the run as new, which bounds one replay.

## Workload

[`benches/durable_agent_replay_profile.rs`](../autumn-harvest-agent/benches/durable_agent_replay_profile.rs)
records one durable agent with M user-message ops, then replays it. The
profile is `AgentProfile::new("support")` with a system prompt and the
default window of 200 messages (`DEFAULT_WINDOW`, `agent/mod.rs:74`).
`DURABLE_AGENT_REPLAY_PROFILE_SHAPE` picks the work of one op:

- `tool` (default): a model turn with one tool call, the call, and a model
  turn that answers. Each tool result holds 512 bytes. An op adds four
  messages, so op 50 fills the window.
- `chat`: one model turn that answers. An op adds two messages, so op 100
  fills the window.

`WorkflowTestEnv` records the run with a frozen clock and scripted mocks of
`agent_profile`, `agent_model_turn` and `agent_tool_call`. It sends one op,
then drives the run until the agent waits again. The record mode writes the
history to a file. The harness asserts that the run stays in one run, with
one snapshot per op, no failed op and the expected last answer. The last
snapshot must hold min(4M, 200) messages, or min(2M, 200) for `chat`.
`DURABLE_AGENT_REPLAY_PROFILE_N` is M (default 50). A `tool` recording of
288 ops or more fails with the name of the event that ended the run.

The bench function `decode` decodes the file. The bench function `replay`
runs the history through `run_workflow_with_state_history_policy_and_caps`
with the span metadata that the SQLite runtime builds. The replay must
suspend at the next op wait and schedule no new work. A recorded command
that the replay does not consume fails the outcome (issue #1791), so a
passing replay matched every recorded event.

## Running

A random hash seed moves the heap layout (see
[Nondeterminism](#nondeterminism)). Each measured run below preloads a
`getrandom` that returns fixed bytes, so every run gets the same seed.

```bash
BIN=$(cargo bench -p autumn-harvest-agent --bench agent_loop_replay_profile \
  --bench durable_agent_replay_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="durable_agent_replay_profile") | .executable')
mkdir -p fixrand
cat > fixrand/fixrand.c <<'C_EOF'
/* Fixed bytes for getrandom, so every run gets the same hash seed. */
#include <stddef.h>
#include <sys/types.h>
static unsigned long long state = 0x9e3779b97f4a7c15ULL;
ssize_t getrandom(void *buf, size_t len, unsigned int flags) {
    unsigned char *p = buf;
    (void)flags;
    for (size_t i = 0; i < len; i++) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        p[i] = (unsigned char)(state >> 56);
    }
    return (ssize_t)len;
}
C_EOF
gcc -O2 -shared -fPIC -o fixrand/libfixrand.so fixrand/fixrand.c
FIX=$PWD/fixrand/libfixrand.so
for shape in tool chat; do
  for n in 10 50 100 200 250; do
    [ $shape = chat ] && [ $n = 250 ] && continue
    DURABLE_AGENT_REPLAY_PROFILE_SHAPE=$shape DURABLE_AGENT_REPLAY_PROFILE_N=$n \
      DURABLE_AGENT_REPLAY_PROFILE_RECORD=$shape-$n.json "$BIN"
    DURABLE_AGENT_REPLAY_PROFILE_SHAPE=$shape DURABLE_AGENT_REPLAY_PROFILE_N=$n \
      DURABLE_AGENT_REPLAY_PROFILE_HISTORY=$shape-$n.json LD_PRELOAD=$FIX valgrind \
      --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.$shape.$n "$BIN"
  done
done
export DURABLE_AGENT_REPLAY_PROFILE_SHAPE=tool DURABLE_AGENT_REPLAY_PROFILE_N=200 \
  DURABLE_AGENT_REPLAY_PROFILE_HISTORY=tool-200.json
LD_PRELOAD=$FIX valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=cg.tool.200.b "$BIN"
callgrind_annotate --inclusive=yes --threshold=99.5 cg.tool.200
callgrind_annotate --inclusive=yes --tree=calling --threshold=100 cg.tool.200
LD_PRELOAD=$FIX valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='durable_agent_replay_profile::replay' --callgrind-out-file=cg.replay "$BIN"
callgrind_annotate --threshold=99 cg.replay
LD_PRELOAD=$FIX valgrind --tool=dhat --num-callers=100 --dhat-out-file=dhat.json "$BIN"
for seed in 1 2 3; do
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file=cg.seed.$seed "$BIN"
done
for seed in 1 2 3 4 5; do
  DURABLE_AGENT_REPLAY_PROFILE_N=10 DURABLE_AGENT_REPLAY_PROFILE_HISTORY=tool-10.json \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file=cg.seed10.$seed "$BIN"
done
```

The last eight runs keep the random seed. They show the spread that the
fixed seed removes.

## Profile

Headline: `tool`, M=200, with the fixed seed. Two runs gave a total of
3,684,392,936 `Ir` each. `replay` is 3,095,039,458 `Ir` of it (84.00%).
`decode` is 489,853,205 `Ir` (13.30%). With a random seed, the total moves
by up to 1.2%. The section [Nondeterminism](#nondeterminism) gives the
cause.

The rows are inclusive and nest, so they do not sum.

| Function | Calls | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|---:|
| `replay` | 1 | 3,095,039,458 | 100.00% |
| The entity loop (`Entity::run_loop`) | 1 | 2,937,211,212 | 94.90% |
| **The agent loop:** `AgentRun::work` (`agent/mod.rs:1565`) | 200 | 1,898,337,591 | 61.33% |
| `run_segment` (`workflow.rs:665`) | 200 | 1,787,280,575 | 57.75% |
| `Sent::of`, the digest of the whole window for the full form (`workflow.rs:866`) | 150 | 909,731,656 | 29.39% |
| `Sent::is_prefix_of`, the check of the last window (`workflow.rs:640`) | 199 | 154,920,891 | 5.01% |
| `model_turn`, the replayed race and its input (`workflow.rs:802`) | 400 | 217,009,348 | 7.01% |
| `exceeds_bytes` of each request (`workflow.rs:707`) | 400 | 191,353,457 | 6.18% |
| `round_budget` (`workflow.rs:1032`) | 200 | 155,823,053 | 5.03% |
| `Vec<ChatMessage>::clone` in `run_segment` | 650 | 66,839,085 | 2.16% |
| `without_system` of the transcript (`agent/mod.rs:1825`) | 200 | 41,836,299 | 1.35% |
| The transcript copy into `Progress` (`agent/mod.rs:1784`) | 200 | 30,856,896 | 1.00% |
| **The entity snapshot work:** `record_snapshot` (`entity.rs:680`) | 201 | 522,851,448 | 16.89% |
| `to_value` of the live snapshot (`entity.rs:686`) | 201 | 155,873,175 | 5.04% |
| `from_value` of the recorded snapshot (`entity.rs:697`) | 201 | 154,083,527 | 4.98% |
| `match_entity_snapshot`, the clone of the recorded value (`autumn-harvest/src/replay.rs:6374`) | 201 | 115,547,799 | 3.73% |
| The drop of the unused live snapshot | 201 | 79,270,236 | 2.56% |
| `Published::of`, a second `to_value` of the state (`entity.rs:723`) | 201 | 152,936,659 | 4.94% |
| The drop of the old published view (`entity.rs:572`) | 200 | 80,970,223 | 2.62% |
| The transcript copy in the state clone for the handler (`entity.rs:545`) | 200 | 60,513,350 | 1.96% |
| The drop of the old state after the handler | 200 | 19,298,755 | 0.62% |
| `checkpoint_due`, almost all one `loaded_history_bytes` call (`entity.rs:848`) | 200 | 195,556,553 | 6.32% |
| The drop of the context and its history | 1 | 157,520,249 | 5.09% |

The snapshot rows sum to 836,570,435 `Ir` (27.03%). They are
`record_snapshot`, `Published::of`, the drop of the old view, the state
copy and the drop of the old state. `checkpoint_due` adds 6.32%. It calls
`loaded_history_bytes` once per replay, and that call serializes the whole
history: 194,959,584 `Ir`. The later checks read the cached count.

`transcript::chain` holds all the digest work. It costs 1,083,255,296 `Ir`
(35.00%). SHA-256 takes 661,181,889 of it. The 35,900 calls of `hex` take
297,936,237 (9.63% of the replay). `hex` writes each byte with
`write!(out, "{byte:02x}")` (`autumn-harvest-agent/src/transcript.rs:71`).

A second run collected only inside `replay` (`--toggle-collect`). With the
fixed seed, it collected 3,095,039,458 `Ir`, the same as the inclusive row.
Its flat self view:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `sha2` `compress` | 660,378,600 | 21.34% |
| `_int_malloc` | 380,684,600 | 12.30% |
| `format_escaped_str_contents` | 254,937,322 | 8.24% |
| `_int_free` | 222,205,972 | 7.18% |
| `format_escaped_str` | 141,054,776 | 4.56% |
| `memcpy` | 122,718,095 | 3.96% |
| `malloc` | 108,728,112 | 3.51% |
| `core::fmt::write` | 93,248,847 | 3.01% |
| `malloc_consolidate` | 92,937,416 | 3.00% |
| `free` | 91,068,769 | 2.94% |

Grouped, the allocator entries cost 1,086,449,474 `Ir` (35.10%). They are
the `malloc.c` and `arena.c` entries and the `__rdl_` and `__rust_` shims.
SHA-256 costs 661,181,889 (21.36%). The JSON serialization entries cost
496,210,115 (16.03%). They are the `serde_json::ser` entries and the
`Serialize` entries that write to a `Vec<u8>`. The formatting entries of
`hex` cost 276,165,249 (8.92%).

## Scaling

Callgrind with the fixed seed, inclusive `Ir`, one run per row. "Full
turns" counts the model-turn inputs in the history that carry no prefix.
"Agent" is `AgentRun::work`. "Snapshot" is the sum of the snapshot rows
above.

| Shape | M | Events | File bytes | Snapshot bytes | Messages | Full turns | Total | `decode` | `replay` | Per op | Agent | Snapshot | Digest |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `tool` | 10 | 145 | 106,887 | 61,459 | 40 | 1 | 25,510,949 | 3,691,203 | 20,105,403 | 2,010,540 | 58.9% | 26.9% | 28.2% |
| `tool` | 50 | 705 | 1,514,464 | 1,289,865 | 200 | 1 | 465,265,007 | 45,854,473 | 402,705,061 | 8,054,101 | 60.5% | 30.1% | 37.0% |
| `tool` | 100 | 1,405 | 6,644,164 | 3,787,635 | 200 | 50 | 1,531,606,379 | 192,890,263 | 1,294,448,670 | 12,944,487 | 61.2% | 27.8% | 35.1% |
| `tool` | 200 | 2,805 | 17,034,064 | 8,799,035 | 200 | 150 | 3,684,392,936 | 489,853,205 | 3,095,039,458 | 15,475,197 | 61.3% | 27.0% | 34.4% |
| `tool` | 250 | 3,505 | 22,234,014 | 11,307,185 | 200 | 200 | 4,743,605,983 | 638,351,151 | 3,978,119,335 | 15,912,477 | 61.6% | 27.0% | 34.4% |
| `chat` | 10 | 85 | 37,843 | 20,145 | 20 | 1 | 10,221,996 | 1,734,498 | 7,412,266 | 741,227 | 48.1% | 32.1% | 28.3% |
| `chat` | 50 | 405 | 416,243 | 330,626 | 100 | 1 | 153,226,966 | 17,606,954 | 129,997,175 | 2,599,944 | 53.9% | 35.2% | 41.6% |
| `chat` | 100 | 805 | 1,411,348 | 1,240,778 | 200 | 1 | 559,408,383 | 57,836,805 | 483,756,365 | 4,837,564 | 55.8% | 35.9% | 44.9% |
| `chat` | 200 | 1,605 | 6,278,771 | 3,641,882 | 200 | 100 | 2,307,966,741 | 250,098,375 | 2,009,729,112 | 10,048,646 | 66.5% | 25.2% | 54.6% |

"Digest" is `Sent::of` plus `Sent::is_prefix_of`, as a share of `replay`.
The shares of "Agent" hold the digest.

The cost of one op grows with the transcript until the window is full. For
`tool`, ops 11 to 50 cost 9,564,991 `Ir` each on average. Ops 51 to 100 cost
17,834,872 each, and ops 101 to 200 cost 18,005,908 each. The M=250 row was
recorded for this check: ops 201 to 250 cost 17,661,598 each. So once the
window is full, the replay is linear in M. Before that it is O(M²), and the
window bounds that term. For `chat`, ops 11 to 50 cost 3,064,623 each. Ops
51 to 100 cost 7,075,184 each, and ops 101 to 200 cost 15,259,727 each.

The jump at the fill comes with the full form. In the `tool` shape, each op
from op 52 on sends its first turn in full form. In the `chat` shape, that
starts at op 102. In the `tool` history at M=200, 150 of the 400 model-turn
inputs have no prefix: the first turn of op 1 and 149 whole windows. They
hold 7,428,618 bytes in all.

`decode` costs 28.7 to 34.5 `Ir` per file byte for `tool`, and 39.8 to 45.8
for `chat`.

### One op once the window is full

The difference between the `tool` runs at M=100 and M=200, divided by 100
ops:

| Part | `Ir` per op | Share of the op |
|---|---:|---:|
| `replay` | 18,005,908 | 100.0% |
| `Sent::of` | 6,105,679 | 33.9% |
| `record_snapshot` | 2,966,142 | 16.5% |
| `model_turn` | 1,419,804 | 7.9% |
| `checkpoint_due` | 1,184,829 | 6.6% |
| `exceeds_bytes` | 1,119,718 | 6.2% |
| The drop of the context and its history | 959,712 | 5.3% |
| `round_budget` | 894,055 | 5.0% |
| `Published::of` | 876,747 | 4.9% |
| `decode`, outside `replay` | 2,969,629 | 16.5% |

The `loaded_history_bytes` call in `checkpoint_due` and the drop of the
context each run once per replay. Their rows show how each pass grows with
the history.

`Sent::is_prefix_of` adds no measurable cost per op here. Its total at
M=200 is within 490 `Ir` of its total at M=100. The trimmed window is
shorter than the last sent window, so the length check fails first. For
`chat` it costs 4,424,478 `Ir` per op. There the two lengths are equal, so
the check hashes the whole prefix, fails, and `Sent::of` hashes the window
again. The digest is then 57.7% of each `chat` op.

## Allocations

Dhat, `tool`, M=200, with the fixed seed and 100 frames per stack:
766,340,473 bytes in 3,304,706 blocks in total. The peak is 172,271,877
bytes in 792,351 blocks. `replay` allocates 566,775,376 bytes in 2,505,750
blocks, 2,833,877 bytes and 12,529 blocks per op. `decode` allocates
158,519,348 bytes in 794,310 blocks.

| Caller inside `replay` | Blocks | Bytes |
|---|---:|---:|
| `match_entity_snapshot`, the clone of the recorded value | 414,495 | 78,145,912 |
| `to_value` of the live snapshot | 414,294 | 78,018,880 |
| `Published::of` | 412,686 | 77,755,972 |
| `model_turn` | 375,150 | 69,815,920 |
| `exceeds_bytes` | 3,873 | 46,636,032 |
| `checkpoint_due` | 9,439 | 45,064,630 |
| `round_budget` | 130,561 | 44,050,389 |
| `run_segment`, the rest | 246,901 | 39,537,548 |
| `run_segment_as`, the rest | 233,586 | 35,164,830 |
| `Sent::of` | 82,700 | 20,949,550 |
| The entity loop, mostly the state clone | 121,643 | 17,309,604 |
| `from_value` of the recorded snapshot | 44,477 | 10,204,728 |
| `Sent::is_prefix_of` | 14,075 | 3,577,025 |
| `Sent::after` | 1,850 | 519,000 |
| The rest | 20 | 25,356 |

The three full copies of the state per op allocate 1,241,475 blocks, 49.5%
of the blocks of `replay`.

## Findings

### Where the cost goes

Per op, the replay runs these passes over the transcript or the state:

1. **The digest.** `open_segment` checks that the last sent window is a
   prefix of the new one (`workflow.rs:640`). `is_prefix_of` hashes the
   whole prefix again (`transcript.rs:187`). Once the window is full,
   `trim_window` (`agent/mod.rs:2051`, called at `:1826`) drops the oldest
   exchange. The check then fails, and `send_turn` hashes the whole window
   with `Sent::of` (`workflow.rs:866`). Each step of `chain`
   (`transcript.rs:52`) serializes one message, runs SHA-256 and formats 32
   bytes as hex.
2. **The entity snapshot.** The state is serialized twice, cloned once as a
   recorded value, decoded once and copied once. The freed old values add
   the drops. These are the passes of
   [the entity snapshot replay profile](performance-entity-snapshot-replay.md).
3. **The request measure.** `exceeds_bytes` and `round_budget` serialize
   the whole request in each model turn and each tool round. See
   [the `agent_loop` replay profile](performance-agent-loop-replay.md).
4. **The full form.** After the trim, the first model turn of each op holds
   the whole window. `execute_activity` serializes it into a `Value`. The
   input adds about 50 KB to the history per op, and `decode` and
   `loaded_history_bytes` read those bytes again.

### Superlinear terms

- **Until the window fills.** Each op costs O(W) for a transcript of W
  messages, and W grows with each op. So the replay is O(M²) until op 50
  for `tool` and op 100 for `chat`. `DEFAULT_WINDOW` bounds W.
- **Past the fill.** Each op costs about the same, so the replay is linear
  in M. One op then costs 17.7M to 18.0M `Ir`, and ops 11 to 50 cost 9.6M
  on average. `Sent::of` alone costs 6,105,679 `Ir` per op past the fill.
- **The checkpoint bound.** The history grows by about 104 KB per op past
  the fill. Near op 288 it passes half of the 50 MiB byte cap
  (`over_byte_budget`, `entity.rs:853`). The entity loop then continues the
  run as new. A recording of 290 ops ended at op 288 with
  `WorkflowContinuedAsNew`. So one replay of this shape holds at most about
  288 ops.
- **One replay per decision.** The SQLite runtime replays the whole run at
  each decision. An op makes four decisions in the `tool` shape and two in
  `chat`. So a run of M ops makes O(M) replays of O(M) cost each, O(M²) in
  all. The [`agent_loop` page](performance-agent-loop-replay.md#superlinear-terms)
  measures that sum for `agent_loop`. It is not measured here.

### Ideas, not implemented

1. **Write the hex with a table.** `hex` (`transcript.rs:66`) formats each
   byte through `core::fmt`. The bound is 297,936,237 `Ir`, 9.63% of the
   replay. A table still writes 64 bytes per digest, so the real saving is
   lower. The digest text stays the same, so no recorded value changes.
2. **Check the prefix without hashing it.** The check needs only to know
   that the window still starts with the sent messages. Per-message digests
   kept with `Sent` would let it compare digests, not hash messages. The
   state then grows by one digest per message. The bound is
   `is_prefix_of`: 154,920,891 `Ir` (5.01%) here. It is 37.0% of the `tool`
   replay at M=50 and 32.8% of the `chat` replay at M=200.
3. **Keep the delta form across a trim.** Trim in larger steps, so that the
   prefix stays valid for several ops. Or use a digest of the window that a
   trim does not change. The first changes the window that the model reads.
   The second changes the recorded `PrefixRef` digest, so it needs a patch
   gate. The bound is `Sent::of`: 909,731,656 `Ir` (29.39%). The smaller
   history that the delta form records is a further saving. It is not
   measured.
4. **The entity snapshot ideas.** The
   [ideas of the entity snapshot profile](performance-entity-snapshot-replay.md#ideas-not-implemented)
   apply here. Skip the live snapshot in a replay: 235,143,411 `Ir`
   (7.60%). Publish the view lazily: 233,906,882 `Ir` (7.56%). Count the
   loaded bytes at load: 194,959,584 `Ir` (6.30%).
5. **The request measure ideas.** The ideas of the
   [`agent_loop` page](performance-agent-loop-replay.md#ideas-not-implemented)
   bound `exceeds_bytes` and `round_budget`: 347,176,510 `Ir` (11.22%).

Ideas 2 to 5 do not overlap. Together they bound a saving of 2,075,838,934
`Ir`, 67.07% of the replay. Idea 1 overlaps with ideas 2 and 3. Each idea
keeps the replay result as it is.

## Not measured

- The live path at the frontier of a warm decision.
- The sum of the replays of a whole run on SQLite.
- A profile with memory in the state, a workspace, peers or a heartbeat.
- A window other than the default.

## Nondeterminism

The recording uses a frozen clock and scripted mocks. The incarnation, the
activity ids and the execution id are random at record time. Each op also
records the wall clock through `system_now`. These values have a fixed
length, and the replay reads them from the file.

The replay reads one random input: the hash seed of `std` maps. With a
random seed, the program total is not stable to 0.1%. Three runs of the
headline gave 3,685,403,793, 3,728,316,517 and 3,682,119,026 `Ir`. One of
them lands 1.19% above the fixed-seed total. The other two lie within
0.062% of it. At M=10 five runs spread by 0.34%.

The cause is `upsert_search_attrs` (`autumn-harvest/src/context.rs:5826`).
It collects each patch into a `HashMap` with a random seed per process.
`AgentRun::publish` calls it twice per op. Between two runs with a random
seed, that `insert` is the only Rust function whose self cost changes, by
159 `Ir`. The patch order changes the heap layout. The rest of the
difference is in glibc `malloc_consolidate` and `unlink_chunk`. The largest
inclusive change is in the final drop of the context, 43,903,845 `Ir`. The
drop of the history matcher is 43,681,627 `Ir` of it. Outside libc, the
self costs of the two runs differ by 263 `Ir` in all.

A preloaded `getrandom` that returns fixed bytes fixes the seed. Two runs
with it gave 3,684,392,936 `Ir` each on this build. All tables on this page
use the fixed seed. The `agent_loop` replay upserts no attribute, and its
runs match exactly without the shim. The process environment also moves the
count a little. In three shell environments, the `replay` row spans 2,852
`Ir` (0.0001%), all in `memcpy`.

This page was re-measured after PR #2355 (Rust 1.99.1 and aes-gcm 0.11):
the headline moved from 3,675,316,572 to 3,684,392,936 `Ir`. The largest
moves are in serialization. `exceeds_bytes` rose 9.1%, and
`loaded_history_bytes` fell 5.8%.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.1, measured 2026-10-27.
