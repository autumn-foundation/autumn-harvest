# `AgentHarness::model_turn`: one model turn over a growing window

This page records a baseline profile of the activity side of one agent model
turn (issues #1973 and #2264). Every model turn of every agent run runs this
body once. A delta turn whose prefix misses runs it again in the full form.
On Postgres, the `agent_model_turn` activity calls `model_turn_timed_in` at
`workflow.rs:2353`. On SQLite, the registration calls it at `sqlite.rs:91`. The loop sends each turn through `send_turn`
(`workflow.rs:832`), once per pass at `workflow.rs:710`. `AgentTask::new`
turns the delta form on (`types.rs:220`), so a new run sends a `PrefixRef`
and the added messages from its second turn on. The public
[`model_turn`](../autumn-harvest-agent/src/harness.rs) (`harness.rs:583`)
runs the same body with no live sink.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`). Wall-clock timing is not admissible evidence
on this shared-vCPU machine.

**Outcome: baseline only. No code change.**

The suspicion holds. In the delta form, one turn costs O(W) instructions in
the window size W, so a run of T turns costs O(T²). The prefix check in
`window` (`harness.rs:433`) hashes the whole held window again on every
turn. At T=200 it is 82.9% of all instructions. The module doc of
[`transcript.rs`](../autumn-harvest-agent/src/transcript.rs) says that the
workflow hashes each message once. That holds, but the activity hashes the
whole prefix again on each turn.

## Workload

[`benches/agent_model_turn_profile.rs`](../autumn-harvest-agent/benches/agent_model_turn_profile.rs)
runs one conversation of T model turns through `model_turn`:

- The window starts with a system prompt and a task of 1,024 bytes of text
  each.
- A canned model answers each turn with one `lookup` tool call. It clones
  no part of the request and serializes nothing.
- After each turn the profile adds the assistant message and a tool result
  of 1,024 bytes, as `push_assistant` and `run_round` do. The serialized
  window grows by about 1,219 bytes per turn. At T=200 it is 245,821 bytes
  in 402 messages.
- The harness has one tool, the `AllowAll` policy and the default
  `InMemoryTranscriptStore` with its 64 MiB bound.
- A current-thread tokio runtime with a timer drives the turns.

`AGENT_MODEL_TURN_PROFILE_SHAPE` picks the request form:

- `delta` (default): the form that `send_turn` sends. Turn 0 sends the whole
  window. Each later turn sends a `PrefixRef` of the previous window and the
  two added messages. Every turn sets `remember`.
- `full`: the form of a task without `transcript_delta`. Each turn sends the
  whole window, with no prefix and no `remember`.
- `cache`: as `delta`, with a `ResponseCache` over an in-process store, and
  a model with a model id. Each turn misses the cache, then stores its
  answer.

The profile builds each request outside `model_turn`. It also computes
`Sent::of` and `Sent::after`, as `send_turn` does. That workflow digest is
in the program total, but not in the inclusive cost of `model_turn`. The
profile asserts that every turn asks for one call and gets `Allow`. It also
asserts that every later turn sent a prefix, and that every `cache` turn
stored its answer.

In this build, `model_turn`, `model_turn_in` and `model_turn_timed_in` are
inlined into the caller. The outermost symbol of the body is
`model_turn_spanned::{closure#0}`, so its inclusive cost stands for
`model_turn`. The wrappers read `Utc::now` and open the tracing span.
`Utc::now` costs 38,200 `Ir` over 200 turns. `decide_calls` is inlined into
the body.

## Running

```bash
BIN=$(cargo bench -p autumn-harvest-agent --bench agent_model_turn_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="agent_model_turn_profile") | .executable')
AGENT_MODEL_TURN_PROFILE_N=200 valgrind --tool=callgrind --branch-sim=no \
  --cache-sim=no --callgrind-out-file=cg.out "$BIN"
AGENT_MODEL_TURN_PROFILE_SHAPE=cache AGENT_MODEL_TURN_PROFILE_N=200 valgrind \
  --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.cache.out "$BIN"
callgrind_annotate --inclusive=yes --threshold=100 cg.out
callgrind_annotate --inclusive=yes --tree=caller --threshold=100 cg.out
AGENT_MODEL_TURN_PROFILE_N=200 valgrind --tool=callgrind --branch-sim=no \
  --cache-sim=no --dump-instr=yes --compress-strings=no --compress-pos=no \
  --callgrind-out-file=cg.instr.out "$BIN"
awk '/^fn=/{fn=$0} /^cfn=/{cfn=$0} /^calls=/{c=$1; next}
  c != "" {if (fn ~ /AgentHarness>::window$/ && cfn ~ /transcript::chain$/)
  {s[$1] += $NF; n[$1] += substr(c, 7)} c = ""}
  END {for (k in s) print k, n[k], s[k]}' cg.instr.out
AGENT_MODEL_TURN_PROFILE_N=200 valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
for shape in delta full cache; do
  for t in 25 50 100 200 300; do
    [ $shape = cache ] && [ $t = 300 ] && continue
    AGENT_MODEL_TURN_PROFILE_SHAPE=$shape AGENT_MODEL_TURN_PROFILE_N=$t valgrind \
      --tool=callgrind --branch-sim=no --cache-sim=no --dump-instr=yes \
      --compress-strings=no --compress-pos=no --callgrind-out-file=cg.$shape.$t "$BIN"
  done
done
```

The `awk` step prints the cost of each call site of `chain` in `window`.
Run it on `cg.delta.$t` for the prefix check column of the scaling section.
`--dump-instr=yes` moves a total by about 0.0001% at most. The `delta` and
`cache` rows of the scaling table at T=25 to T=100 come from such runs.

`AGENT_MODEL_TURN_PROFILE_N` is T (default 200).
`AGENT_MODEL_TURN_PROFILE_REPS` (default 1) repeats the run with a fresh
harness.

## Profile

### `delta`, T=200

Two runs gave 2,491,395,707 and 2,491,396,497 `Ir`. The table uses the first.
The split of `chain` comes from a third run with `--dump-instr=yes`.

| Function | Inclusive Ir | Share |
|---|---:|---:|
| Program total | 2,491,395,707 | 100.0% |
| `model_turn` body (`model_turn_spanned`) | 2,426,044,577 | 97.38% |
| `window` | 2,395,855,886 | 96.17% |
| `chain`, called from `window` (399 calls) | 2,086,793,429 | 83.76% |
| of it, the prefix check `chain(None, held)` (199 calls) | 2,066,159,895 | 82.93% |
| of it, the `remember` digest (200 calls) | 20,633,534 | 0.83% |
| `InMemoryTranscriptStore::put` | 260,984,325 | 10.48% |
| of it, the serializer that `json_len` runs | 213,974,598 | 8.59% |
| of it, the message clones of `window.to_vec()` | 45,895,245 | 1.84% |
| `InMemoryTranscriptStore::get`, the clone of the held window | 46,277,228 | 1.86% |
| drop of the `ChatRequest` | 26,233,135 | 1.05% |
| `fits_result_cap` | 1,243,639 | 0.05% |
| `fit_turn` | 883,106 | 0.04% |
| `turn_tools` | 533,433 | 0.02% |
| `cache_key` (no cache, so it returns at once) | 3,400 | 0.00% |
| `chain` from `Sent::after` and `Sent::of`, the workflow digest | 33,415,200 | 1.34% |

The rows are inclusive and overlap, so they do not sum to 100%.

`callgrind_annotate` does not split the 399 `chain` calls of `window` by
call site. The `awk` step of the Running block does. The prefix check is the
call site that runs 199 times and passes no previous digest. The `chain`
rows are the same in every run.

The `remember` digest hashes the same messages as `Sent::after` and
`Sent::of` in the profile. It costs 20,633,534 `Ir`, and those two cost
33,415,200 `Ir`. Their SHA-256 and `hex` costs per call are the same, so
the difference is in the serializer. The profile digest runs right after
the drop of the whole request. Allocator state is the likely cause.

Top self costs:

| Function | Self Ir | Share |
|---|---:|---:|
| `sha2::sha256::soft::unroll::compress` | 1,505,615,720 | 60.43% |
| `serde_json::ser::format_escaped_str` | 371,043,313 | 14.89% |
| `core::fmt::write` | 105,813,949 | 4.25% |
| `Formatter::pad_integral` | 68,989,448 | 2.77% |
| `_int_malloc` | 62,230,140 | 2.50% |
| `__memcpy_avx_unaligned_erms` | 57,768,657 | 2.32% |
| `<u8 as LowerHex>::fmt` | 42,457,864 | 1.70% |
| `<String as fmt::Write>::write_str` | 35,291,264 | 1.42% |
| `_int_free` | 29,081,824 | 1.17% |
| `transcript::hex` | 28,764,705 | 1.15% |
| `Formatter::pad_integral::write_prefix` | 27,430,998 | 1.10% |

This host has no SHA extensions (`sha_ni` is not in `/proc/cpuinfo`). So
`sha2` runs its software rounds, natively and under valgrind. A host with
SHA extensions runs other code for SHA-256, so its counts differ.

The `fmt` rows come from `transcript::hex`. It formats each digest byte with
`write!("{byte:02x}")`. Its inclusive cost is 338,401,873 `Ir` (13.58%) for
40,801 calls, or 8,294 `Ir` for each 32-byte digest. `chain` calls it once
for each message it hashes.

### `cache`, T=200

Two runs gave 4,000,937,767 and 4,000,938,584 `Ir`.

| Function | Inclusive Ir | Share |
|---|---:|---:|
| Program total | 4,000,937,767 | 100.0% |
| `model_turn` body | 3,934,151,883 | 98.33% |
| `window` | 2,390,204,608 | 59.74% |
| `cache_key` | 1,504,852,500 | 37.61% |
| of it, SHA-256 of the key document | 1,289,320,258 | 32.23% |
| of it, the serializer of the messages | 212,192,527 | 5.30% |
| `put` | 259,973,100 | 6.50% |
| `cache_store` (the `ResponseCache::store` future) | 7,967,700 | 0.20% |
| `fits_result_cap` | 1,276,552 | 0.03% |
| `fit_turn` | 994,361 | 0.02% |
| `cache_lookup` (the `ResponseCache::lookup` future) | 132,978 | 0.00% |

`ResponseCache::key` (`response_cache.rs:309`) serializes the tools and the
whole window, then hashes the document. A miss and a store cost little.

### `full`, T=200

Two runs gave 179,136,026 `Ir` each.

| Function | Inclusive Ir | Share |
|---|---:|---:|
| Program total | 179,136,026 | 100.0% |
| `model_turn` body | 115,325,657 | 64.38% |
| `window` | 72,441,504 | 40.44% |
| of it, the clone of the messages | 46,066,379 | 25.72% |
| of it, the drop of the original messages | 26,087,809 | 14.56% |
| drop of the `ChatRequest` | 37,109,353 | 20.72% |
| `fits_result_cap` | 3,396,933 | 1.90% |
| `fit_turn` | 911,367 | 0.51% |
| the request clone of the profile, outside `model_turn` | 58,264,181 | 32.53% |

With no prefix, `window` clones the messages (`harness.rs:425`). Then it
drops the original, which it never reads again.

## Scaling

Callgrind, one run per cell. "Per turn" is the body divided by T. "Ratio" is
the body over the body at half the size.

| Shape | T | Program total | `model_turn` body | Per turn | Ratio |
|---|---:|---:|---:|---:|---:|
| `delta` | 25 | 45,540,005 | 41,101,654 | 1,644,066 | |
| `delta` | 50 | 166,385,897 | 156,904,036 | 3,138,081 | 3.82 |
| `delta` | 100 | 636,358,256 | 613,089,886 | 6,130,899 | 3.91 |
| `delta` | 200 | 2,491,395,707 | 2,426,044,577 | 12,130,223 | 3.96 |
| `full` | 25 | 4,165,372 | 2,257,807 | 90,312 | |
| `full` | 50 | 13,189,630 | 7,930,570 | 158,611 | 3.51 |
| `full` | 100 | 47,674,968 | 30,640,925 | 306,409 | 3.86 |
| `full` | 200 | 179,136,026 | 115,325,657 | 576,628 | 3.76 |
| `cache` | 25 | 72,925,492 | 68,380,844 | 2,735,234 | |
| `cache` | 50 | 267,227,061 | 257,545,664 | 5,150,913 | 3.77 |
| `cache` | 100 | 1,022,136,590 | 998,446,354 | 9,984,464 | 3.88 |
| `cache` | 200 | 4,000,937,767 | 3,934,151,883 | 19,670,759 | 3.94 |

In all three shapes, the cost of one turn grows 1.76 to 1.98 times when T
doubles. So one turn is linear in T, and a run is quadratic in T. At T=200
the `delta` body costs 21.0 times the `full` body.

A check at T=300 holds the same: the `delta` body is 5,434,408,678 `Ir`, and
the `full` body is 256,760,399. They are 2.24 and 2.23 times the bodies at
T=200. A quadratic run gives 2.25. T=300 stays below the store bound, so no
window is evicted.

Inclusive shares of the program total, by T. The prefix check column is the
call site that the `awk` step prints:

| T | `delta`: `window` | `delta`: prefix check | `delta`: `put` | `delta`: `fit_turn` | `cache`: `cache_key` |
|---:|---:|---:|---:|---:|---:|
| 25 | 88.21% | 71.24% | 9.51% | 0.240% | 35.84% |
| 50 | 92.69% | 77.78% | 9.97% | 0.132% | 36.83% |
| 100 | 94.99% | 81.24% | 10.25% | 0.069% | 37.35% |
| 200 | 96.17% | 82.93% | 10.48% | 0.035% | 37.61% |

The prefix check takes a larger share as T grows. The parts that do not grow
with the window shrink: `fits_result_cap` and `fit_turn` together cost 10,543
`Ir` per turn at T=25 and 10,634 at T=200.

The `remember` digest hashes the two added messages of each turn. It costs
102,549 `Ir` per turn at T=25 and 102,842 at T=200, so it is constant. The
profile computes the same digest outside `model_turn` with `Sent`. That
copy costs 112,805 `Ir` per turn at T=25 and 167,076 at T=200. Its SHA-256
and `hex` cost per call stay the same, so the rise is in its serializer. The
same work costs less inside `model_turn`, so the likely cause is the heap
state of the profile.

## Allocations

Dhat, `delta`, T=200:

| Measure | Value |
|---|---|
| Total | 226,629,311 bytes in 457,262 blocks |
| At the peak (t-gmax) | 39,316,106 bytes in 163,859 blocks |
| At the end | 596 bytes in 3 blocks |
| Reads | 240,148,829 bytes |
| Writes | 169,051,347 bytes |

By call site:

- `put`: 107,313,988 bytes in 162,401 blocks. At the peak it holds
  38,509,752 bytes in 160,602 blocks. The store keeps a whole copy of every
  window of the run. That is about 157 times the 245,821 serialized bytes of
  the final window.
- `chain`: 75,740,081 bytes in 122,801 blocks, all short-lived. These are the
  serializer buffers and the hex strings of each message.
- `get`: 40,628,238 bytes in 158,802 blocks, all short-lived.

Dhat, `full`, T=200: 79,109,084 bytes in 330,067 blocks. The peak is
1,171,737 bytes in 4,826 blocks. The clone in `window` takes 38,463,400 bytes
in 160,200 blocks.

## Findings

### Where the cost goes

A `delta` turn makes five passes over the whole window:

1. The prefix check serializes and hashes the held window again
   (`harness.rs:433`, `chain` at `transcript.rs:52`).
2. `put` serializes the whole window only to measure it (`transcript.rs:141`).
3. `put` clones the whole window into the store (`transcript.rs:161`).
4. `get` clones the held window out of the store (`transcript.rs:137`).
5. The drop of the `ChatRequest` frees the window.

With a cache, `ResponseCache::key` serializes and hashes it once more
(`response_cache.rs:342` and `:344`).

The workflow digest hashes only the added messages, so its work per turn
stays constant. The activity throws that saving away when it hashes the
prefix again.

The canned model serializes nothing. A real client serializes the window at
least once per turn. One serialization pass costs about the `json_len` row:
213,974,598 `Ir` over 200 turns. The prefix check costs 9.7 times that.

The parts of the turn that do not depend on the window are small:
`turn_tools`, `decide_calls`, `fits_result_cap` and `fit_turn`.
`fits_result_cap` and `fit_turn` serialize the reply twice, at about 10.6K
`Ir` per turn. That is not worth work.

### Superlinear terms

- `harness.rs:433`: the prefix check is O(W) per turn and O(T²) per run.
  It is the largest term.
- `transcript.rs:141`, `:161` and `:137`: the measure, the store clone and
  the read clone are each O(W) per turn.
- `response_cache.rs:342` to `:344`: the cache key is O(W) per turn.
- `harness.rs:425`: the `full` form clones and drops the window. That is
  O(W) per turn.
- Memory: the store holds every window of a run, so it holds O(T²) bytes
  until the bound evicts. From the measured window sizes, one run of this
  shape fills the 64 MiB default near T=330. That figure is derived, not
  measured.

### Ideas, not implemented

Each bound is the inclusive cost that the idea removes at most, at T=200.

| Idea | Location | Upper bound |
|---|---|---|
| Trust a window that the in-process store put itself, and skip the prefix check | `harness.rs:433` | 2,066,159,895 `Ir`: 82.9% of `delta`, 85.2% of its body |
| Encode hex from a table, not with `write!` | `transcript.rs:66` | 338,401,873 `Ir`: 13.6% of `delta` |
| Pass the window size to `put`, not a second serialization | `transcript.rs:141` | 213,974,598 `Ir`: 8.6% of `delta` |
| Share windows as `Arc<[ChatMessage]>` in the store and the request | `transcript.rs:137`, `:161` | 118,405,608 `Ir`: 4.8% of `delta` |
| Move the messages into the window when there is no prefix | `harness.rs:425` | 72,441,504 `Ir`: 62.8% of the `full` body |
| Key the cache by the window digest, not by the messages | `response_cache.rs:342` | 1,504,852,500 `Ir`: 37.6% of `cache` |
| Keep one window per run in the store | `transcript.rs:140` | the 38,509,752 bytes that `put` holds at the peak, down to one window |

Notes on the ideas:

- The prefix check stops a shared store from putting words into a turn. An
  in-process store holds only windows that the harness hashed itself. The
  `TranscriptStore` trait cannot say which kind of store it is, so the first
  idea needs a new trait method and its own safety argument. Without the
  check, the rest of the `delta` body is 359,884,682 `Ir`. That rest is still
  quadratic, from the other four passes.
- The hex idea overlaps the first idea. `window` makes 40,400 of the 40,801
  `hex` calls, almost all of them in the prefix check. The output string
  stays the same, so recorded digests still match.
- The shared-window idea changes the public `TranscriptStore` and
  `ChatRequest` types.
- The `full` form runs for a task recorded before the delta form, and for a
  retry after `TranscriptPrefixMissing`. That idea changes no public API.
- A new cache key format gives every stored entry one miss.
- [DESIGN-2262](design/DESIGN-2262.md#2-transcript-delta-2264) first planned
  links `{prev, added}`, not whole windows. The build stores whole windows so
  that a lookup walks no chain. The memory cost above is the price of that
  choice.

### Nondeterminism

These inputs vary between runs:

- `model_turn_in` reads `Utc::now` (`harness.rs:621`).
- The body reads `std::time::Instant::now` for the latency (`harness.rs:733`).
- `decide_calls` and each `tokio::time::timeout` read the tokio clock
  (`harness.rs:887`).
- `ResponseCache::store` stamps each entry with `SystemTime::now`
  (`response_cache.rs:519`).
- The transcript store and the cache index are `HashMap`s with a random seed
  (`transcript.rs:104`, `response_cache.rs:114`).

The profile fixes every other input. Two runs at T=200 differ by 790 `Ir`
for `delta` (0.00003%), 817 `Ir` for `cache` (0.00002%) and 0 `Ir` for
`full`.

An edit of the profile moves its code in the binary. The build before the
check of `AGENT_MODEL_TURN_PROFILE_REPS` gave 2,491,389,940 `Ir` for `delta`
at T=200. That is 5,767 `Ir` (0.0002%) below the table above.

Host: 4 logical CPUs, valgrind-3.22.0, Rust 1.99.0, measured 2026-10-26.
