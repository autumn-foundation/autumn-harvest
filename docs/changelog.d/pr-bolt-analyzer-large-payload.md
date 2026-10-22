## Phase — LargePayloadRule counts payload bytes without allocating (Bolt)

`LargePayloadRule` now measures a payload with a counting writer instead of
`Value::to_string()`. The byte count and every warning stay identical. On a
6,000-event history, instructions in `HistoryAnalyzer::analyze` fall 77.5%
and allocated bytes fall 97.6%. No `WorkflowEvent` variant or migration
changes. See `docs/performance-analyzer-large-payload.md`.
