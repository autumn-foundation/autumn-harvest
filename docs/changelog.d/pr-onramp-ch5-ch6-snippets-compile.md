## Onramp — chapter 5 and 6 flagship snippets compile

Docs-only. Chapter 5's `spawn_child_workflow_raw` call passed a workflow id the method does not take (E0061). Chapter 6's `charge_card` used `.map_err(|e| e.to_string())?` in a `HarvestResult` function, and `HarvestError` has no `From<String>` (E0277). Both snippets now compile. `scripts/check-child-idempotency-examples.sh` compiles them in CI's lint job (baseline 2 errors, after 0). No public API or `harvest_events` change.
