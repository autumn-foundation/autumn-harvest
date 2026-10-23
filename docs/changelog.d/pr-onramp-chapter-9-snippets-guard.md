## Onramp — chapter 9 snippets guarded in CI

Docs tooling only. `scripts/check-worker-routing-examples.sh` extracts the four Rust snippets of chapter 9 (activity `requires`, label map, `with_label`, `with_queue_weights`) and compiles them with `cargo check` in CI's lint job. Baseline: 4 snippets, 0 compile errors, 13.8 min cold. The guard finds no defect today. It keeps the chapter from drifting from the routing API. No public API change.
