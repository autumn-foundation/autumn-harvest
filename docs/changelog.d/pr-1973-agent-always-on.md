## Feature — Always-on agents: heartbeats, follow-ups, delivery, memory, loop guard (issue #1973)

**What shipped.** `autumn-harvest-agent` gains the always-on primitives,
modelled on `autumn-plugin-agent` with no dependency on it.
`docs/agent-adapter.md` section 10 shows them end to end.

**Design.**

- **Heartbeat.** The workflow `agent_heartbeat` runs one tick. The activity
  `agent_precheck` can skip it before any model call. `heartbeat::schedule`
  builds the Postgres engine schedule. `sqlite::start_heartbeat` starts a
  tick on SQLite. The engine keeps one schedule per workflow name.
- **Follow-up.** The `schedule_followup` tool books a durable timer in the
  same workflow. A new segment then runs the prompt in the same
  conversation, framed as the agent's own note. One follow-up per segment, a
  maximum delay and a chain cap bound it.
- **Unattended runs are read-only.** A heartbeat tick and a follow-up
  segment deny `Write`, `External` and unknown tools, and cannot write
  memory. The tool-call activity checks the effect again. Each rule has an
  opt-in.
- **Delivery.** The activity `agent_deliver` sends each segment result. A
  silent `HEARTBEAT_OK` answer is not sent. An early stop sends a notice. A
  failed delivery never fails the run. `Report::key()` lets a delivery dedupe
  a retried send.
- **Memory.** The activity `agent_memory_snapshot` reads a frozen snapshot
  once per segment. The snapshot escapes each entry, and the `memory` tool
  writes through.
- **Loop guard.** An FNV-1a fingerprint of name, arguments and result. It
  warns, then ends the run as `loop_detected`. It is on by default and covers
  the whole run.
- `max_total_tokens` and the report totals cover the whole follow-up chain.
- New hook budget: `AgentHarness::hook_timeout` bounds the memory read, the
  delivery and the precheck.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change. The new payload fields are `#[serde(default)]`. The new activity
input fields are left out at their defaults, so strict replay still matches.

**Tests.** `tests/always_on.rs` has 24 SQLite tests. They cover delivery,
heartbeats, the precheck, follow-ups and their caps, read-only rules, memory
and the loop guard. A restart during the follow-up wait resends no report and
repeats no model call. Unit tests cover escaping, silence rules and serde
compatibility.
