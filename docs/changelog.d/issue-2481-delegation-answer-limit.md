## The harness limit governs delegation answers (issue #2481)

A delegation answer was cut at a fixed 8 000 characters.
`AgentHarness::tool_output_limit` did not change that cut, and the cut was
silent. Now the configured limit governs every delegation answer.

- `AgentHarness::tool_output_limit` caps each tool result and each
  delegation answer. The default stays 8 000 characters.
- `Peer::answer_limit(chars)` overrides the harness limit for one peer.
- The rule covers an inline child, a fan-out child and a durable peer.
- The delegation result JSON has `"truncated": true` or `false`. A cut sets
  `DelegationRecord::answer_truncated` in the report.
- `agent_discover_peers` records the resolved limit on each `Peer`, so
  replay cuts each answer as the live run did. A lookup recorded before this
  change has no limit. That run keeps the old cut and the old JSON shape.
  No `patched` gate and no new history event are needed.
- No migration.

Design record: `docs/design/DESIGN-2481.md`. Tests: one per delegation path
in `tests/mesh.rs`, `tests/fan_out.rs` and `tests/durable_peers.rs`, a
legacy-lookup test in `workflow.rs`, and a property test of the cut flag in
`harness.rs`.
