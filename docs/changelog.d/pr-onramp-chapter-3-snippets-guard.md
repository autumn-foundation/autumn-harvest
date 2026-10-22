## Onramp — chapter 3 snippets guarded in CI

Docs tooling only. `scripts/check-durable-timers-examples.sh` extracts the three flagship timer workflows of chapter 3 and compiles them with `cargo check` in CI's lint job. Baseline: 3 snippets, 0 compile errors, 8 min cold. The guard finds no defect today. It keeps the chapter from drifting from the timer API. No public API change.
