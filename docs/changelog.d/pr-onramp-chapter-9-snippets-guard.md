## Onramp — chapter 9 snippets guarded in CI

Docs tooling only. `scripts/check-worker-routing-examples.sh` extracts the four Rust snippets of chapter 9 (activity `requires`, label map, `with_label`, `with_queue_weights`) and compiles them in CI's lint job. It also runs the example and passes the `requires` string to `eligibility::parse_requirements`, because the macro does not parse it. Baseline: 4 snippets, 0 errors, 18.2 min cold. The guard finds no defect today. It keeps the chapter from drifting from the routing API. No public API change.
