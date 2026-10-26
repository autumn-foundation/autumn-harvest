# `agent_loop` replay: the workflow side of the agent loop

This page profiles the replay of a recorded `agent_loop` run (issue #1973).
`agent_loop` (`autumn-harvest-agent/src/workflow.rs:429`) is the workflow of
one agent run. `workflows()` registers it for a `HarvestBuilder`
(`workflow.rs:2519`), and `sqlite::register` registers it on the SQLite
runtime (`autumn-harvest-agent/src/sqlite.rs:52`). Each model turn and each
tool call is an activity, and each completed activity wakes the run for a
new decision. So a run of N tool rounds makes more than 2N decisions. On
SQLite, every decision replays the whole history.
`drive_one_cycle_inner` (`autumn-harvest-sqlite/src/runtime.rs:3033`) loads
it (`runtime.rs:3114`) and runs it through
`run_workflow_with_state_history_policy_and_caps` (`runtime.rs:3178`). On
Postgres, a warm decision resumes its resident future
(`autumn-harvest/src/worker.rs:27022`) and replays nothing. A cold decision
builds its context with `build_task_context` (`worker.rs:27042`) and replays
the whole history with `drive_workflow_keep` (`worker.rs:27077`). Each
replayed turn runs the per-turn code of `run_segment` (`workflow.rs:665`)
again.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

The replay of N tool rounds is quadratic in N. It grows 3.53, 3.75 and 4.00
times per doubling from N=50 to N=400. At N=100 the replay costs 217,126,195
`Ir`. Two whole-request serializations per turn cost 55.5% of it. They
measure the request against the request cap. Two clones and two drops of the
transcript per turn cost another 27.4%. A backend that replays every
decision pays a replay once per decision. The recording of N=100 does that,
and it costs 15,904,592,945 `Ir`.

## Workload

[`benches/agent_loop_replay_profile.rs`](../autumn-harvest-agent/benches/agent_loop_replay_profile.rs)
records one `agent_loop` run of N tool rounds, then replays it. Each round is
one model turn with a short narration and one call of the tool
`search_orders`. Each tool result holds 1,024 bytes of text, and each round
gets other text, so the loop guard sees no repeat. The last turn answers with
no call. The task has a system prompt, `max_steps` of N and the default
request cap of 2 MiB, so no bound stops the run.
`AGENT_LOOP_REPLAY_PROFILE_SHAPE` picks the request form:

- `delta` (default): each model turn records only the new messages, as
  `AgentTask::new` sets it (issue #2264).
- `full`: each model turn records the whole transcript.

`WorkflowTestEnv` records the run with a frozen clock and scripted mocks of
`agent_model_turn` and `agent_tool_call`. The record mode writes the history,
without its terminal event, to a file. The measured run reads the file and
replays it once.

The bench function `decode` decodes the file. The bench function `replay`
runs the history through `run_workflow_with_state_history_policy_and_caps`
with the span metadata that the SQLite runtime builds. That replay is the
last decision of the run, so it completes the run. The harness asserts that
the report has the stop `completed`, N steps, N tool calls and 2N+2
messages. A recorded command that the replay does not consume fails the
outcome (issue #1791), so a passing replay matched every recorded event.

`WorkflowReplayer::replay_from_events` is not used. It runs the strict
replay path that a worker does not run, and that path has its own profile in
[`performance-replay.md`](performance-replay.md).

## Running

```bash
BIN=$(cargo bench -p autumn-harvest-agent --bench agent_loop_replay_profile \
  --bench durable_agent_replay_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="agent_loop_replay_profile") | .executable')
for shape in delta full; do
  for n in 10 25 50 100 200; do
    AGENT_LOOP_REPLAY_PROFILE_SHAPE=$shape AGENT_LOOP_REPLAY_PROFILE_N=$n \
      AGENT_LOOP_REPLAY_PROFILE_RECORD=$shape-$n.json "$BIN"
    AGENT_LOOP_REPLAY_PROFILE_SHAPE=$shape AGENT_LOOP_REPLAY_PROFILE_N=$n \
      AGENT_LOOP_REPLAY_PROFILE_HISTORY=$shape-$n.json valgrind --tool=callgrind \
      --branch-sim=no --cache-sim=no --callgrind-out-file=cg.$shape.$n "$BIN"
  done
done
AGENT_LOOP_REPLAY_PROFILE_N=400 AGENT_LOOP_REPLAY_PROFILE_RECORD=delta-400.json "$BIN"
AGENT_LOOP_REPLAY_PROFILE_N=400 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-400.json \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.delta.400 "$BIN"
AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-100.json \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.headline "$BIN"
callgrind_annotate --inclusive=yes --threshold=99 cg.headline
callgrind_annotate --inclusive=yes --tree=calling --threshold=100 cg.headline
AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-100.json \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --dump-instr=yes \
  --compress-strings=no --compress-pos=no --callgrind-out-file=cg.instr "$BIN"
awk '/^fn=/{fn=$0} /^cfn=/{cfn=$0} /^calls=/{c=$1; next}
  c != "" {if (fn ~ /workflow::run_segment::\{closure#0\}$/ && cfn ~ /Vec<.*ChatMessage>/)
  {s[$1 " " cfn] += $NF; n[$1 " " cfn] += substr(c, 7)} c = ""}
  END {for (k in s) print k, n[k], s[k]}' cg.instr
AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-100.json \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --toggle-collect='agent_loop_replay_profile::replay' --callgrind-out-file=cg.replay "$BIN"
callgrind_annotate --threshold=99 cg.replay
AGENT_LOOP_REPLAY_PROFILE_N=100 AGENT_LOOP_REPLAY_PROFILE_HISTORY=delta-100.json \
  valgrind --tool=dhat --num-callers=100 --dhat-out-file=dhat.json "$BIN"
for n in 25 50 100; do
  AGENT_LOOP_REPLAY_PROFILE_N=$n AGENT_LOOP_REPLAY_PROFILE_RECORD=rec-$n.json \
    valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.record.$n "$BIN"
done
```

## Profile

Headline: `delta`, N=100. Two callgrind runs gave a total of 231,383,510
`Ir` each. `replay` is 217,126,195 `Ir` in both, 93.84% of the total.
`decode` is 9,062,640 `Ir` (3.92%). The rest is the file read, the checks
and the drop of the recording.

The rows are inclusive and nest, so they do not sum.

| Function | Calls | Inclusive `Ir` | Share of `replay` |
|---|---:|---:|---:|
| `replay` | 1 | 217,126,195 | 100.00% |
| `run_segment` (`workflow.rs:665`) | 1 | 212,956,884 | 98.08% |
| `run_round` (`workflow.rs:1091`) | 100 | 101,061,566 | 46.55% |
| `round_budget` (`workflow.rs:1032`) | 100 | 94,790,460 | 43.66% |
| `json_len` of the projected next request (`workflow.rs:1060`) | 100 | 59,959,828 | 27.62% |
| The clone of the transcript (`workflow.rs:1053`) | 100 | 17,726,001 | 8.16% |
| The drop of the projected request | 100 | 13,160,343 | 6.06% |
| `placeholder`, built again in each round (`workflow.rs:1045`) | 100 | 2,950,786 | 1.36% |
| `exceeds_bytes` of the request (`workflow.rs:707`) | 101 | 60,621,145 | 27.92% |
| `Vec<ChatMessage>::clone` in `run_segment`, the transcript for `next_request` (`workflow.rs:699`) | 201 | 17,942,452 | 8.26% |
| `Sent::after`, the digest of the new messages (`workflow.rs:860`) | 100 | 11,746,944 | 5.41% |
| `Vec<ChatMessage>` drops in `run_segment`, the whole window in `send_turn` (`workflow.rs:842`) | 302 | 10,636,578 | 4.90% |
| `model_turn`, the replayed activity (`workflow.rs:802`) | 101 | 4,321,497 | 1.99% |
| `Settle::settle`, the loop guard and the budget (`workflow.rs:1272`) | 100 | 3,711,415 | 1.71% |
| `run_tool`, the replayed activity | 100 | 2,225,258 | 1.02% |
| The drop of the context and its history | 1 | 2,049,014 | 0.94% |
| `to_value` of the report | 1 | 1,436,451 | 0.66% |
| `Progress::report` (`workflow.rs:212`) | 1 | 415,804 | 0.19% |

A run with `--dump-instr=yes` splits the clones and drops of `run_segment`
by call site. Of the clones, 101 copy the whole transcript for
`next_request` and cost 17,938,652 `Ir`. The other 100 clone the empty
`messages` field of `..request.clone()` (`workflow.rs:854`) and cost 3,800.
The delta copies its new messages with `to_vec` (`:853`), which is inlined.
Of the 302 drops, 100 free the whole window at the end of `send_turn` and
cost 10,632,740 `Ir`. The other 202 free empty lists. `send_turn` is
inlined into `run_segment`.

A second run collected only inside `replay` (`--toggle-collect`). It
collected 217,126,229 `Ir`, 34 more than the inclusive row. Its flat self
view:

| Self entry | `Ir` | Share |
|---|---:|---:|
| `format_escaped_str` | 53,576,432 | 24.68% |
| `format_escaped_str_contents` | 49,628,379 | 22.86% |
| `_int_malloc` | 23,308,427 | 10.73% |
| `_int_free` | 11,428,205 | 5.26% |
| `memcpy` | 8,507,504 | 3.92% |
| `sha2` `compress` | 8,062,032 | 3.71% |
| `malloc` | 6,893,541 | 3.17% |
| `malloc_consolidate` | 5,912,310 | 2.72% |
| `free` | 4,586,400 | 2.11% |
| `String::clone` | 3,537,951 | 1.63% |

Grouped, the JSON serialization entries cost 117,537,906 `Ir` (54.13%).
They are the `serde_json::ser` entries and the `Serialize` entries that write
to a `Vec<u8>`. The allocator entries (`malloc.c`, `arena.c` and the
`__rdl_` and `__rust_` shims) cost 61,939,239 `Ir` (28.53%).

## Scaling

Callgrind, inclusive `Ir`. "Transcript" is the serialized size of the
messages of the final report.

| Shape | N | Events | History file bytes | Transcript bytes | Total | `decode` | `replay` | `replay` per round |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `delta` | 10 | 45 | 39,756 | 13,682 | 7,033,690 | 972,453 | 4,970,263 | 497,026 |
| `delta` | 25 | 105 | 96,757 | 33,917 | 23,521,013 | 2,321,318 | 19,269,454 | 770,778 |
| `delta` | 50 | 205 | 191,783 | 67,642 | 69,212,401 | 4,568,090 | 61,444,476 | 1,228,890 |
| `delta` | 100 | 405 | 382,005 | 135,093 | 231,383,510 | 9,062,640 | 217,126,195 | 2,171,262 |
| `delta` | 200 | 805 | 763,105 | 270,193 | 842,336,716 | 18,060,069 | 813,479,495 | 4,067,397 |
| `delta` | 400 | 1,605 | 1,525,456 | 540,393 | 3,305,947,346 | 35,875,708 | 3,249,966,485 | 8,124,916 |
| `full` | 10 | 44 | 101,279 | 13,682 | 8,119,137 | 1,993,712 | 4,936,132 | 493,613 |
| `full` | 25 | 104 | 503,520 | 33,917 | 35,635,189 | 8,995,362 | 24,074,404 | 962,976 |
| `full` | 50 | 204 | 1,848,445 | 67,642 | 125,265,043 | 31,605,991 | 87,888,459 | 1,757,769 |
| `full` | 100 | 404 | 7,067,792 | 135,093 | 469,583,286 | 117,745,914 | 346,547,930 | 3,465,479 |
| `full` | 200 | 804 | 27,634,542 | 270,193 | 1,822,868,828 | 454,210,958 | 1,358,636,450 | 6,793,182 |

The `delta` replay grows 3.53 times from N=50 to N=100, 3.75 times from
N=100 to N=200 and 4.00 times from N=200 to N=400. Its cost per round
doubles with N, so the replay is quadratic. A fit to N=100 and N=200 gives
18,961 × N² + 275,127 × N. It matches N=50 within 0.5% and N=25 within
2.8%. The N=400 run, recorded for this check, costs 3.3% more than the fit.
Its file and its `decode` grow about 2 times per doubling, so the delta form
keeps the history linear.

From N=50 up, the `full` replay grows 3.92 to 3.94 times per doubling.
Each turn records the whole transcript, so its history is quadratic, and
`decode` grows 3.86 times from N=100 to N=200. At N=200 the `full` replay
costs 1.67 times the `delta` replay. `model_turn` takes most of the difference: 457,619,482 `Ir`
against 8,620,941, because the activity input holds the whole transcript.

## Allocations

Dhat, `delta`, N=100, with 100 frames per stack: 67,551,444 bytes in
176,160 blocks in total. The peak is 2,276,897 bytes in 11,565 blocks.
`replay` allocates 64,064,186 bytes in 160,768 blocks, 640,642 bytes and
1,608 blocks per round:

| Caller inside `replay` | Blocks | Bytes |
|---|---:|---:|
| `exceeds_bytes`, the buffer of the whole request | 1,017 | 18,989,440 |
| `json_len` in `round_budget`, the buffer of the projected request | 1,008 | 18,601,472 |
| `run_segment`, mostly the transcript clone for `next_request` | 74,876 | 12,196,202 |
| `round_budget`, the transcript clone and the placeholder answers | 67,350 | 11,345,895 |
| `model_turn` | 6,034 | 955,340 |
| `placeholder` | 4,900 | 691,800 |
| `Sent::after` and `Sent::of` | 805 | 402,348 |
| The rest | 4,778 | 881,689 |

Each serialization grows one `Vec<u8>` to the size of the request. So the
two serializations allocate few blocks and most of the bytes. The two clones
allocate most of the blocks.

## Findings

### Where the cost goes

Per tool round, the replay runs this work over the whole transcript:

1. `exceeds_bytes` serializes the whole request for the next turn
   (`workflow.rs:707`): 27.92%.
2. `round_budget` clones the transcript, adds a placeholder answer and
   serializes the projected next request (`workflow.rs:1053`, `:1060`). Then
   it drops that request. Together: 43.66%.
3. `next_request` gets a clone of the transcript (`workflow.rs:699`). The
   delta form sends only the new messages. `send_turn` hashes them, then
   drops the whole window. Together: 13.16%.

The replay of the recorded activities is small. `model_turn` and `run_tool`
cost 3.01% together. The digest of the new messages costs 5.41%, and the
loop guard 1.71%.

### Superlinear terms

- **The per-turn request measure.** Items 1 to 3 above each cost O(W) for a
  transcript of W bytes, once per round. W grows with each round, so the
  replay is O(N × W), which is O(N²). The quadratic term of the fit is
  18,961 × N² `Ir`. At N=200 the replay costs 30.0 `Ir` per byte of the
  transcript sizes that the rounds measure, summed. The sum is taken as the
  final transcript times (N+1)/2.
- **The full form.** With `transcript_delta` off, each model-turn input
  holds the whole transcript. `execute_activity` then serializes it into a
  `Value` in each turn, and the history holds O(N²) bytes. A run recorded
  before the delta form replays this way.
- **One replay per decision.** The SQLite runtime replays the whole history
  at each decision, and so does a cold Postgres decision. `WorkflowTestEnv`
  does the same, so the recording measures that sum. It cost 433,270,126
  `Ir` at N=25, 2,454,719,805 at N=50 and 15,904,592,945 at N=100. That
  grows 5.67 and 6.48 times per doubling, toward O(N³). At N=100 it is 73.3
  times the one replay. The recording includes the mocks and the work of
  `WorkflowTestEnv`, so it is an upper bound on the replays alone.

### Ideas, not implemented

1. **Count the request bytes without a serialization.** Keep the serialized
   size of each message when it joins the transcript. The request size is
   then the fixed fields plus the sum, with one comma per message. The bound
   is both serializations: 120,580,973 `Ir`, 55.53% of the replay. The count
   must equal `json_len` byte for byte. Otherwise a recorded run near the
   cap replays another stop.
2. **Borrow the transcript.** A request type that borrows the messages needs
   no clone for the measure. With idea 1, `round_budget` needs no request at
   all. Ideas 1 and 2 together bound a saving of `exceeds_bytes`,
   `round_budget`, the clone in `run_segment` and the window drop:
   183,990,635 `Ir`, 84.74% of the replay.
3. **Build the placeholder once.** It is a constant of the build. The bound is
   2,950,786 `Ir` (1.36%). It is inside the bound of ideas 1 and 2.

Each idea keeps the recorded history and the replay result as they are. On
a backend that replays every decision, the saving applies to each decision.

## Not measured

- The first run of each turn at the frontier. It runs the same code once per
  turn, and a warm Postgres decision runs only that.
- The activity side of a model turn and a tool call. The replay reads their
  recorded results.
- A run with delegations, approvals, memory or follow-ups.

## Nondeterminism

The recording uses a frozen clock and scripted mocks. The activity ids and
the execution id are random at record time. They have a fixed length, and
the replay reads them from the file. The two headline runs matched exactly.
The N=100 row of the scaling table comes from them. The scaling loop also
sets `AGENT_LOOP_REPLAY_PROFILE_SHAPE`, and its N=100 run gave 231,384,581
`Ir`, 1,071 more (0.0005%). The environment moves the heap layout, so
compare runs made with the same command. A history recorded again gives
the same counts within 0.001%.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
