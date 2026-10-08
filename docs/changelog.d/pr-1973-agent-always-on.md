## Feature — Always-on agents: heartbeats, follow-ups, delivery, memory, loop guard (issue #1973)

**What shipped.** `autumn-harvest-agent` gains the always-on primitives,
modelled on `autumn-plugin-agent` with no dependency on it.
`docs/agent-adapter.md` section 10 shows them end to end.

**Design.**

- **Heartbeat.** The workflow `agent_heartbeat` runs one tick. The activity
  `agent_precheck` can skip it before any model call. A tick is read-only
  unless `HeartbeatTask::allow_actions` is set. `heartbeat::schedule` builds
  the Postgres engine schedule. `sqlite::start_heartbeat` starts a tick on
  SQLite.
- **Follow-up.** The `schedule_followup` tool books a durable timer in the
  same workflow. A new segment then runs the prompt in the same
  conversation. One follow-up per segment, a maximum delay and a chain cap
  bound it. Approval names count steps across segments.
- **Delivery.** The activity `agent_deliver` sends each answer once. A
  silent `HEARTBEAT_OK` answer is not sent. A failed delivery never fails
  the run.
- **Memory.** The activity `agent_memory_snapshot` reads a frozen snapshot
  once per segment. The `memory` tool writes through to the `MemoryStore`.
- **Loop guard.** An FNV-1a fingerprint of name, arguments and result. It
  warns, then ends the run as `loop_detected`. No call runs after the stop.
  It is on by default.
- `ToolRules::read_only` combines with the app policy through `Strictest`.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change. The new `AgentTask` and `AgentReport` fields are `#[serde(default)]`.

**Tests.** `tests/always_on.rs` covers delivery, quiet and noteworthy
heartbeats, read-only ticks, the precheck, a follow-up across a durable
timer, the delay and chain caps, the frozen memory snapshot, and the loop
guard. A restart during the follow-up wait resends no report and repeats no
model call. `tests/engine_activities.rs` covers the new activity handlers.
