# Time-travel replay debugger

`harvest debug` and `autumn_harvest::debugger` step through a **recorded**
workflow history one event at a time, showing what your workflow code does next
at each point — and diff two runs to find the *exact* first place they part ways.

It is the tool you reach for when a deploy wedges an in-flight execution and you
need to answer **"which line of my change broke this history?"** rather than just
"something diverged".

Everything here is **read-only and local**: no database, no HTTP, no activity is
ever executed. It appends no `WorkflowEvent`, runs no migration, and changes no
engine runtime behaviour.

---

## How it relates to the other replay tools

Harvest has four replay-shaped surfaces. They are complements, not alternatives:

| Tool | Question it answers | When |
|---|---|---|
| [`replay-verify`](replay-verify.md) | "Does my change break these curated **completed** histories?" | CI gate, pre-merge |
| [`replay-drift gate`](replay-drift-gate.md) | "Does my change break the executions **in flight right now**?" | CI gate, pre-deploy |
| `POST /workflows/{id}/replay-diagnosis` (#614) | "Is *this one* production run diverging under the code I have deployed?" | One-shot, server-side, during an incident |
| **`harvest debug` (this page)** | "**Where exactly**, and **why**?" | Interactive, local, after the above says *yes* |

The server-side diagnosis (#614) gives you a verdict and a single divergence
record. The debugger gives you the whole trace leading up to it: every step's
pending commands, open awaitables, version gates, side effects and markers — and
a side-by-side diff against a second build.

---

## The two arms

The shipped `harvest` binary is statically linked and **cannot register your
application's `#[workflow]` handler functions** (the same constraint
`harvest-replay` states). That splits the surface cleanly, and both arms produce
the same `ReplayTrace` type, so a session that starts in the CLI graduates to the
library without reshaping anything:

| Capability | Needs your code? | Where |
|---|---|---|
| Step through a history; inspect open awaitables, version/patch gates, side effects, markers | **no** | `harvest debug replay` |
| Run to a breakpoint (event type / index / activity / signal) | **no** | `harvest debug replay --break-at-*` |
| Diff **two fixture histories** | **no** | `harvest debug diff` |
| Per-step pending `WorkflowCommand`s | yes | `ReplayDebugger::trace_snapshot` |
| Diff **two workflow-code registrations** | yes | `diff_traces` |

---

## Walkthrough: from an exported history to a root cause

The scenario: you shipped a change to `order_flow` and in-flight executions
started failing to replay. You have the exported history.

### 1. Export the history (once)

```bash
harvest history export <EXECUTION_ID> --payload-policy full > order_flow.json
```

> **`--payload-policy full` is load-bearing.** `harvest history export` defaults
> to `redacted`, which rewrites every `input` / `output` / `payload` field in
> place with a digest stub. Replaying a redacted history makes the *first*
> payload-bearing activity report a divergence that is an artefact of the
> export, not of your code — so `ReplayDebugger` **refuses** a redacted export
> (`DebugError::RedactedHistory`) and the CLI warns. Same hazard, same fix as
> the replay-drift gate: see
> [`docs/replay-drift-gate.md`](replay-drift-gate.md).


This is the `HistorySnapshot` JSON round-trip format. Everything below runs
against that file with **zero** database access, so you can debug a production
history on a laptop.

> Payload-codec and offload envelopes display as their envelopes when no codec
> or payload store is configured. The debugger never errors on them.

### 2. Look at the shape of the run

```bash
harvest debug replay order_flow.json
```

```
workflow: order_flow
execution: 0000e3f1-...
events: 9

  idx  event                             open  detail
  ---  --------------------------------  ----  ------
    0  WorkflowStarted                      0
    1  ActivityScheduled                    1  opens activity reserve_stock
    2  ActivityCompleted                    0
    3  ActivityScheduled                    1  opens activity charge_card
    4  ActivityCompleted                    0
    5  TimerStarted                         1  opens timer settle_delay
    6  TimerFired                           0
    7  ActivityScheduled                    1  opens activity ship_order
    8  ActivityCompleted                    0
```

Nine events, three activities, one timer. Now find the interesting one.

### 3. Run to a breakpoint

Break on the activity you suspect:

```bash
harvest debug replay order_flow.json --break-at-activity charge_card
```

```
… the overview table above, then:

breakpoint hit at step 3

step 3/8  event ActivityScheduled  [not_replayed]
  pending commands: <unavailable — no workflow handler registered; use the library API, see docs/replay-debugger.md>

  open awaitables:
    activity         charge_card                  opened at event 3

  resolved payload: {"amount_cents":4999,"currency":"USD"}
```

A breakpoint hit prints the **whole overview first**, so you keep the shape of
the run in view while looking at one step of it. And note the `pending commands`
line: this is the handler-free arm (see [the two arms](#the-two-arms)) — the
step's *history-derived* facts are all there, but "what does the code do next"
needs your workflow code, which is what step 5 moves to.

`--break-at-event-type`, `--break-at-index` and `--break-at-signal` are the other
three forms. They are alternatives, not a conjunction — passing two is rejected
at the argument layer, so a breakpoint never silently loses to another.

### 4. Step interactively

```bash
harvest debug replay order_flow.json --tui
```

| Key | Action |
|---|---|
| `n` / `→` / `↓` | next step |
| `p` / `←` / `↑` | previous step |
| `g` / `Home` | first step |
| `G` / `End` | last step |
| `d` | jump to the next divergence (see the note below) |
| `q` / `Esc` | quit |

> **`d` needs workflow code.** A divergence is a *code-vs-history* fact, and the
> CLI trace is handler-free (see the two-arms table above) — so in
> `harvest debug replay --tui` every step reports "not replayed" and `d` finds
> nothing, and says so in the footer. Divergences come from the library API
> (`ReplayDebugger::register_fn`) or from `harvest debug diff`.


"Backward" is not special-cased: a step is a fresh replay of `events[0..=k]`, and
replay is deterministic, so stepping back is just re-running forward to a smaller
index.

### 5. Diff the two builds — the money step

This is the "old build vs new build" workflow. Register **both** versions of your
workflow function and replay the *same* history against each:

```rust
use autumn_harvest::debugger::{ReplayDebugger, diff_traces};

let history = std::fs::read_to_string("order_flow.json")?;

let old = ReplayDebugger::new()
    .register_fn("order_flow", old_build::order_flow_handler)
    .trace_json(&history)
    .await?;

let new = ReplayDebugger::new()
    .register_fn("order_flow", new_build::order_flow_handler)
    .trace_json(&history)
    .await?;

let diff = diff_traces(&old, &new);
if let Some(div) = &diff.divergence {
    println!("first divergence at step {}", div.step_index);
    println!("  old: {}", side(div.left.as_ref()));
    println!("  new: {}", side(div.right.as_ref()));
// where:
//   fn side(step: Option<&DebugStep>) -> String {
//       step.map(|s| s.commands.iter().map(ToString::to_string)
//                     .collect::<Vec<_>>().join(", "))
//           .unwrap_or_default()
//   }
}
```

```
first divergence at step 2
  old: [ScheduleActivity(charge_card -> payments)]
  new: [ScheduleActivity(fraud_check -> payments)]
```

There it is: at step 2 the old build scheduled `charge_card` and the new build
schedules `fraud_check` — an activity inserted *before* an already-recorded one,
with no `ctx.patched(...)` gate around it.

The diff **stops at the first difference**. One root cause usually produces dozens
of downstream differences; reporting them all would bury the actionable one.

### 6. Fix, and confirm

Gate the new activity so pre-change executions keep their recorded path:

```rust
if ctx.patched("fraud-check-v1") {
    ctx.execute_activity(&fraud_check_info(), input.clone()).await?;
}
ctx.execute_activity(&charge_card_info(), input).await?;
```

Now ask the question that actually matters — *"does the fixed build replay this
recorded history cleanly?"* — which is a **single-trace** property: no step
reports a divergence.

```
replaying the recorded history:
  new build   diverges at steps [3, 4]
  fixed build diverges at steps []  <- clean
```

(Deliberately **not** `diff_traces(old, fixed).is_none()` — see
[Gates at a truncated frontier](#gates-at-a-truncated-frontier) below for why a
correctly-gated build still differs from the ungated one at intermediate steps.)

Then let the existing gates confirm it fleet-wide —
[`replay-verify`](replay-verify.md) for curated completed histories, and the
[replay-drift gate](replay-drift-gate.md) for a live in-flight sample.

---

## Gates at a truncated frontier

A step is a replay of `events[0..=k]`, so an **intermediate** step models an
execution *parked at that truncated frontier*. That is what makes stepping work
at all — but it has one consequence worth knowing.

A `ctx.patched(...)` gate (#687) or `ctx.version(...)` gate reached at a frontier
is, correctly, **newly** patched: it records its marker and returns `true`,
exactly as a live execution reaching that point for the first time would. So a
gated build legitimately differs from an ungated one at intermediate steps —
even when it replays the *full* recorded history perfectly.

This means the two questions need two different checks:

| Question | Check |
|---|---|
| "Where do these two builds first behave differently?" | `diff_traces(a, b).divergence` |
| "Does this build replay this recorded history **cleanly**?" | no step has `divergence.is_some()` |

Use the second one to confirm a fix. `examples/replay_debugger.rs` asserts both,
and the walkthrough in step 6 above uses the second.

---

## Diffing two fixture histories

The CLI's `diff` compares two **recordings** rather than two builds:

```bash
harvest debug diff before.json after.json
```

```
first divergence at step 3
  left  (before.json): 9 steps
  right (after.json): 9 steps

  the recorded histories differ (open_awaitables)

  left  (before.json) event ActivityScheduled [not_replayed]
      open_awaitables: activity charge_card (opened at 3)

  right (after.json) event ActivityScheduled [not_replayed]
      open_awaitables: activity fraud_check (opened at 3)
```

It exits `1` when a divergence is found, mirroring `diff(1)`'s "differences
found", so it drops straight into a CI pipeline. Both sides render the **value**
of the field that differs, not just its name — a handler-free trace has no
commands to show, so naming `open_awaitables` without printing them would leave
the operator no better off.

Because a handler-free trace emits no commands, this arm compares the
*history-derived* facts. It does so in two layers:

1. **Curated projections**, checked first because they produce the legible
   field name you see in the report: event type, signal name, awaitable
   kind/name/opening index, version and patch gates, marker names and details,
   and the values of `Custom` side effects.
2. **`event_facts`**, the completeness backstop: the *whole* recorded event,
   serialized, with only per-run identity normalized away (below). A curated
   list silently rots as the event enum grows, and a missed field means two
   genuinely different histories compare **equal** — a false clean, the worst
   thing a divergence-finding tool can do. `event_facts` is exhaustive by
   construction, so anything layer 1 does not name is still caught:

   ```
   first divergence at step 1
     the recorded histories differ (event_facts)

     left  (before.json) event TimerStarted [not_replayed]
         event_facts: {"data":{"duration_secs":30,"timer_id":"escalate"},"type":"TimerStarted"}

     right (after.json) event TimerStarted [not_replayed]
         event_facts: {"data":{"duration_secs":60,"timer_id":"escalate"},"type":"TimerStarted"}
   ```

**Per-run identity is normalized out.** Two independent recordings of the same
scenario necessarily differ on freshly-minted values — activity and child
execution UUIDs, and the values recorded by `ctx.system_now()` / `ctx.new_uuid()`
/ `ctx.random_*()`. Comparing those verbatim would report a divergence at the
first activity of *every* real pair, burying the signal in noise. So they are
excluded, while everything semantically meaningful is compared.

This cannot hide a code-caused divergence: in the two-registrations arm both
sides replay byte-identical events (so the normalized fields are equal by
construction and the divergence surfaces through `commands`), and in the
two-histories arm a change that actually matters — a renamed or reordered
activity, a different input, a new marker, a changed version gate — moves a field
that *is* compared.

---

## Cost

A step is a full prefix replay, so building a complete trace of an `N`-event
history performs `N` replays and is **O(N²)** in total work. That is fine for the
interactive histories this tool is for (tens to low hundreds of events) and
deliberately not how the CI gates work — they replay each history once.

Cap it explicitly on a pathological history:

```bash
harvest debug replay huge.json --max-steps 200
```

The trace reports `truncated: true` so a capped view is never mistaken for a
complete one.

---

## Reference

### CLI

```
harvest debug replay <HISTORY> [--step N]
                               [--break-at-event-type TYPE]
                               [--break-at-index N]
                               [--break-at-activity NAME]
                               [--break-at-signal NAME]
                               [--max-steps N]
                               [--format text|json]
                               [--tui]

harvest debug diff <LEFT> <RIGHT> [--format text|json]
```

`--format json` emits the full `ReplayTrace` / `TraceDiff` for a `jq` pipeline.
In JSON mode the *whole* trace is always emitted — silently emitting a partial
document would be a trap for a machine consumer.

Exit codes: `0` success, `1` on a missed breakpoint, an out-of-range `--step`,
an unreadable/malformed history, or a diff that found a divergence.

### Library

Enable the `debugger` feature (it implies `testing`):

```toml
[dev-dependencies]
autumn-harvest = { version = "0.5", default-features = false, features = ["debugger"] }
```

| Item | Purpose |
|---|---|
| `ReplayTrace::from_history` | Handler-free O(N) projection — everything but pending commands |
| `ReplayDebugger::trace_json` / `trace_snapshot` | Prefix replay against registered code, with per-step commands |
| `ReplayTrace::find_breakpoint` | Run-to-breakpoint over an already-built trace |
| `diff_traces` | First-divergence detection between two traces |
| `Breakpoint`, `DebugStep`, `TraceDiff`, `DiffKind` | The structured snapshot types |

---

## Out of scope

Per issue #949: Vantage UI embedding (TUI-first), live debugging of a *running*
execution, editing history or "fix-forward from event N" (that is workflow reset,
#148/#366), and breakpoints inside activity code.
