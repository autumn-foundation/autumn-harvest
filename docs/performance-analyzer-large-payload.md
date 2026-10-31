# `LargePayloadRule` — count payload bytes without building the text

`HistoryAnalyzer::analyze` runs each rule over a whole history.
`LargePayloadRule` measured every payload with `value.to_string().len()`.
That call serializes the payload into a new `String` and drops it.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Wall-clock timing is not admissible evidence on this shared-vCPU machine.

## Workload

`benches/analyzer_profile.rs` lints a 6,000-event, activity-heavy history
with all three built-in rules. Payloads range from tens of bytes to a few KiB.

## Profile

Inside `HistoryAnalyzer::analyze`, `LargePayloadRule::analyze` is 99.2% of
instructions. `Value`'s `Display::fmt` alone is 98.5%. The top self costs
are `String::write_str`, the `WriterFormatter` `write_all`, string
escaping and `memcpy`. All of them belong to building the throwaway text.

## Change

`serialized_len` serializes the payload with `serde_json::to_writer` into a
writer that only counts bytes. `Display` and `to_writer` both use the compact
formatter, so the length is identical. A unit test checks the equality on
escapes, Unicode and nested values.

## Measurement

| Counter (per tool) | Before | After |
| --- | --- | --- |
| Instructions in `analyze`, 5 calls (callgrind) | 1,323,279,983 | 297,604,486 |
| Bytes allocated per `analyze` (dhat) | 11,176,347 | 264,499 |
| Blocks allocated per `analyze` (dhat) | 40,324 | 431 |

The warning count is unchanged (1,005 over five calls).

## Reproduce

```text
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench analyzer_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="analyzer_profile") | .executable')
valgrind --tool=callgrind --toggle-collect='*HistoryAnalyzer*analyze*' "$BIN"
ANALYZER_PROFILE_REPS=1 valgrind --tool=dhat "$BIN"
ANALYZER_PROFILE_REPS=5 valgrind --tool=dhat "$BIN"
```

Subtract the `REPS=1` dhat totals from the `REPS=5` totals and divide by 4.
