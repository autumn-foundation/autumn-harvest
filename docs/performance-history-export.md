# `history_export::export_history` — a self-referential re-serialization loop

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), both reproducible run-to-run.

## 🎯 Workload

`export_history` is the archival-export path `retention.rs`'s reclamation
sweep calls once per retiring execution, right before deleting its row
(issue #524/#698/#772/#798). It is also re-exported at the crate root as
public API.

The harness is `autumn-harvest/benches/history_export_profile.rs`, newly
committed for this pass. It reuses
`replay_profile_support::build_history` byte-for-byte — the same fixture
issue #135's replay budget uses — to build a realistic 10,001-event history
(5,000 sequential activities, each carrying a ~230-byte JSON payload), then
calls `export_history` under `HistoryPayloadPolicy::Full` (the policy
`retention.rs`'s archival path always uses) `HISTORY_EXPORT_PROFILE_REPS`
times (default 20) against a fresh clone of the same request. The history is
built once, outside the measured loop.

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench history_export_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="history_export_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=99 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Flat profile, pre-fix (`docs/perf-artifacts/history-export-measure-bytes/before-callgrind-flat.txt`):

```
9,658,376,214 (100.0%)  PROGRAM TOTALS

1,728,475,728 (17.90%)  serde_json::ser::format_escaped_str_contents
1,412,337,078 (14.62%)  _int_malloc
1,027,016,320 (10.63%)  serde_json::value::ser::<impl Serialize for Value>::serialize'2
  774,886,870 ( 8.02%)  _int_free
  512,370,057 ( 5.30%)  malloc_consolidate
  471,017,087 ( 4.88%)  malloc
```

`export_history`, `export_events` and `measure_export_bytes` are all fully
inlined into `export_history_decoded` (confirmed against the raw callgrind
call graph — no separate `fn=` record for any of the three), so none shows
up as its own flat-profile line. The dominant costs are JSON
serialization internals and malloc traffic, driven by two separate full
passes over the document: `export_events`'s per-event `serde_json::to_value`
(building the `events: Vec<Value>` field) and `measure_export_bytes`'s
whole-document `serde_json::to_vec`, looped up to 4 times.

To attribute cost to `measure_export_bytes` specifically without a callgraph
tool, a throwaway experiment capped its loop at 1 iteration (never
committed) and re-ran the same harness: instructions dropped from
9,658,376,214 to 7,525,986,076 — a **22.08%** reduction from skipping the
loop's later passes alone. That number is direct measurement, not
reasoning about the loop's shape, and it is far above this gate's 5%
threshold for "worth changing." (That experiment also produced a
slightly-wrong `actual_bytes` — expected, since truncating the loop skips
convergence. It exists only to bound the achievable win before writing the
correct fix below, which the Measurement section verifies is
byte-for-byte identical to baseline.)

## 💡 Hypothesis

`measure_export_bytes` embeds its own future serialized length in
`document.size_limit.actual_bytes`, then re-serializes the *entire*
document up to 4 times, checking on each pass whether the newly-measured
length still matches the field it just wrote — because writing a bigger
number can itself grow the document (more decimal digits), which can in
turn change the number again. That is a real fixed point, but every byte
of the document *other than that one field's digit count* is invariant
across passes. The loop was re-doing full O(document size) work
(`serde_json::to_vec` walks and re-escapes every string, re-visits every
`Value` node) to resolve what is actually an O(1) question: how many
decimal digits does the final byte count have? One real serialization
reveals the constant part of the length; the digit-width fixed point over
that constant converges with cheap arithmetic, no further serialization
needed.

## 🔧 Change

`autumn-harvest/src/history_export.rs`, `measure_export_bytes`:

* Serialize the document exactly once (`serde_json::to_vec`), instead of up
  to 4 times.
* Add `decimal_digit_width(n)` (`1` for `0`, else `n.ilog10() + 1` — the
  same digit count `serde_json` renders `n` as).
* Derive `constant` — the serialized length of everything in the document
  except `actual_bytes`'s own digits — from that one real serialization,
  then solve the same fixed point the original loop solved, iteration for
  iteration (same 4-pass cap, same convergence check, same non-convergence
  fallback), using `constant + decimal_digit_width(actual)` in place of a
  fresh `serde_json::to_vec` call on every subsequent pass.

No behavior change: `actual_bytes`'s digit count is the *only* thing that
can change the document's serialized length across passes (`max_bytes`,
`truncated`, `truncation_behavior` are fixed for the duration of this
function), so the arithmetic recurrence produces the exact same sequence of
candidate values the original re-serialization loop would have, including
its 4-pass cap and fallback if a real input somehow never converges. All 24
existing `history_export::tests::*` unit tests, all 4
`replayer_tests::parent_aware_child_*` integration tests that round-trip
through `export_history`, and the full `autumn-harvest` lib test suite pass
unmodified — no test's expected value needed to change.

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 9,658,376,214 |
| After  | 7,525,987,828 |
| **Reduction** | **2,132,388,386 (22.08%)** |

`format_escaped_str_contents`'s self-cost — the string-escaping work at the
core of every JSON serialization pass — drops from 1,728,475,728 (17.90% of
the pre-fix total) to 576,158,804 (7.66% of the smaller post-fix total), a
66.67% drop in that function's own instruction count, consistent with
removing one full extra pass over the same string-heavy document.

Clears the ≥5%-of-instructions floor by a wide margin: the isolating
experiment above independently measured a 22.08% reduction from touching
this exact code path, and the real fix lands within 1,752 Ir of that
number (7,525,987,828 vs. 7,525,986,076) while producing byte-identical
output.

### Correctness check baked into the harness

`history_export_profile`'s printed `total_exported_bytes` is identical
before and after: `65185800` for both the baseline run and the fixed run
(20 reps × the same 10,001-event history). The throwaway 1-iteration
experiment above, by contrast, printed `65185680` — proof that experiment
was *not* behavior-preserving, and that the real fix (unlike the
experiment) reproduces the original's exact converged byte count.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Change |
|---|---|---|---|
| Total bytes | 1,686,984,980 | 1,351,445,780 | **-335,539,200 (-19.89%)** |
| Total blocks | 10,841,571 | 10,840,931 | -640 (-0.006%) |

Bytes drop sharply (removing a whole extra ~3 MB-per-rep serialization
pass's buffer-growth churn) while block count barely moves — consistent
with `Vec<u8>`'s geometric growth needing only a couple dozen reallocations
per multi-megabyte `to_vec` call, not one allocation per byte. Clears the
≥10%-reduction-in-bytes floor as well, independently of the instruction-count
result above.

### Correctness

* `cargo fmt -p autumn-harvest -- --check` — clean.
* `cargo test -p autumn-harvest --lib` — **3,486 passed, 0 failed, 1
  ignored**, including all 24 `history_export::tests::*`, unchanged in
  expectation.
* `cargo test -p autumn-harvest --test integration --no-default-features
  --features testing -- parent_aware_child` — **4 passed, 0 failed**
  (the `export_history`-round-trip replayer tests).
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — OK, no
  Tier A findings, no Tier B regressions.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench history_export_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="history_export_profile") | .executable')

# Instructions:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=99 cg.out | head -10

# Allocations:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

Full artifacts: `docs/perf-artifacts/history-export-measure-bytes/{before,after}-callgrind-flat.txt`,
`{before,after}-dhat.json`.
