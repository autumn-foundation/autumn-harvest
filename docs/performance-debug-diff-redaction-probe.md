# `is_redacted_export` — probe one key without building the document

`harvest debug diff <LEFT> <RIGHT>` loads two `HistorySnapshot` files. For each
file, `load_snapshot` first calls `debugger::is_redacted_export`. That call
parsed the whole file into a `serde_json::Value` tree, read one top-level key,
and dropped the tree. The CLI then parsed the same text a second time into a
`HistorySnapshot`.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Wall-clock timing is not admissible evidence on this shared-vCPU machine.

## Workload

`benches/debugger_diff_profile.rs` mirrors `run_diff` minus file I/O and
rendering. Two independent recordings of one workflow, each `5_000`
activities (`10_001` events) with the issue #135 payload shape. The diff is
clean, so the walk covers every step. Setup (history build and JSON
serialization) is not part of the pipeline.

## Profile (before)

Inclusive instructions of the pipeline stages, both sides together:

| Stage | Ir | Share of pipeline |
|---|---:|---:|
| `diff_traces` | 533,710,869 | 30% |
| `is_redacted_export` (x2) | 514,087,439 | 29% |
| `ReplayTrace::from_history` (x2) | 398,865,162 | 23% |
| `HistorySnapshot` parse (x2) | 320,020,407 | 18% |

The pipeline is the sum of the four rows, 1.77G Ir. The redaction probe costs
more than the diff. It is a second parse of data the CLI parses again at once.

## Change

`RedactionProbe` deserializes the top-level object with a map visitor. It reads
`payload_policy` and skips every other value with `IgnoredAny`. No `Value` tree
exists, and the `events` array allocates nothing. The probe still checks the
whole document for valid JSON. Behaviour is unchanged for every input:

- a repeated `payload_policy` key keeps the last value, as a `Value` map does;
- a value that is not the string `"redacted"` gives `false`;
- a document that is not an object, or is not valid JSON, gives `false`.

`redaction_probe_matches_a_full_value_parse` compares the probe with a full
`Value` parse on thirteen inputs that cover those cases.

## Measurement

Same machine, same session, same harness. `n = 5000`.

| Counter | Before | After | Delta |
|---|---:|---:|---:|
| `is_redacted_export` (x2), Ir | 514,087,439 | 92,813,673 | -81.9% |
| Pipeline (`load` + `from_history` + `diff_traces`), Ir | 1,766,683,877 | 1,347,626,827 | -23.7% |
| Whole process, Ir | 2,602,256,412 | 2,183,203,958 | -16.1% |
| Whole process, dhat blocks | 3,760,200 | 3,140,154 | -16.5% |
| Whole process, dhat bytes | 446,806,870 | 374,656,936 | -16.1% |

`diff_traces` and `from_history` are unchanged (533.7M and 399.5M Ir).

## Not addressed

`diff_traces` clones every non-boundary `DebugStep` on both sides to renumber
history positions (`boundary_free_steps`). Clone and drop are 492M of its 534M
Ir, 92%. Comparing the steps is 36M. A fix must keep the renumbering for
traces that hold decision boundaries, so it is a larger change than this one.

## Reproduce

```text
cargo bench -p autumn-harvest --no-default-features --features debugger \
  --bench debugger_diff_profile --no-run
BIN=$(ls -t target/release/deps/debugger_diff_profile-* | grep -v '\.d$' | head -1)
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --inclusive=yes --threshold=100 cg.out | grep -E 'load|is_redacted_export|diff_traces|from_history'
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```
