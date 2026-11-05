## The harness limit governs delegation answers (issue #2481)

A delegation answer was cut at a fixed 8 000 characters.
`AgentHarness::tool_output_limit` did not change that cut, and the cut was
silent. Now the configured limit governs every delegation answer.

- `AgentHarness::tool_output_limit` caps each tool result and each
  delegation answer. The default stays 8 000 characters.
- `Peer::answer_limit(chars)` overrides the harness limit for one peer.
- The rule covers an inline child, a fan-out child and a durable peer.
- The encoded delegation result is also cut to a sixth of the request cap,
  as a tool result is under its result cap. The size counts each JSON
  escape. So a large limit cannot drop a whole answer and end the run as
  `transcript_full`.
- The delegation result JSON has `"truncated": true` or `false`. A cut sets
  `DelegationRecord::answer_truncated` in the report and logs a warning.
- `Peer` gains a public field. A struct literal needs `..Peer::new(..)`.
- `agent_discover_peers` records the resolved limit on each `Peer`, so
  replay cuts each answer as the live run did. A lookup recorded before this
  change has no limit. That run keeps the old cut and the old JSON shape.
  No `patched` gate and no new history event are needed.
- No migration.

The design is in [`docs/design/DESIGN-2481.md`](../design/DESIGN-2481.md).
Its section 6 maps each rule to a test. The tests cover each delegation
path, the byte share, a nested cut, and a strict replay of a history that
`trunk-dev` recorded before the change. A property test covers the cut
flag.
