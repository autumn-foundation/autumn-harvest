## Fix — autoresearch compiles again after three parallel fixes

PR #2187 added `start_provenance` to `testing::HistorySnapshot`, and the
autoresearch replay helper did not set it. PRs #2201, #2204 and #2207 each
added the missing field on their own. Git merged all three without a
conflict, so the struct literal named the field three times, and `trunk-dev`
did not compile `autoresearch`. The helper now sets it once.

No migration. No `WorkflowEvent` change.

The same PR fixes a second parallel-merge break. PRs #2205 and #2209 both made
`decode_event` move payload trees into place, and both added a
`pub(crate) enum PayloadSlot` to `event.rs` with the same two variants. The
library failed with E0428. One definition remains, and both
`WorkflowEvent::payload_slot` and `payload_slot_mut` return it. The two lift
paths compose: `decode_event_moving_payloads` lifts first, so `event_from_wire`
finds nothing left to lift.
