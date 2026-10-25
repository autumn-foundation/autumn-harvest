## Onramp — chapter 9 snippets guarded in CI

Docs tooling only. `scripts/check-worker-routing-examples.sh` extracts the four Rust blocks of chapter 9 (`requires`, `with_labels`, `with_label`, `with_queue_weights`) and compiles them with `cargo check` in CI's lint job. Baseline: 4 snippets, 0 compile errors. A deliberate typo in a snippet makes the guard fail. The guard finds no defect today. It keeps the chapter from drifting from the worker API. No public API change.
