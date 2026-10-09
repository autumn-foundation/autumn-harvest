# Design — Issue #1995: pre-deploy upgrade verdict per in-flight run

Issue #1995 asks for one pre-deploy command. The command gives each
in-flight run a verdict against a candidate build:

- **migrate** — the candidate build can take the run over.
- **review** — a person must look first.
- **pin** — the run must stay on its current build.

A July 2026 paper names three failure modes for code upgrades under
long-lived runs. Each mode has its own check:

| Failure mode | Check | Worst verdict |
|---|---|---|
| Determinism violation | Bounded replay of the recorded prefix under the candidate build | pin |
| Rehydration failure | Replay decodes each recorded payload into the candidate types. Schema check on each signal and update payload, and on each pending signal | pin |
| Behavioral drift | Diff of the command-emitting call graph that `harvest-verify` resolves, for steps the run has not passed | review |

**No migration. No new `WorkflowEvent` variant. No new route. No engine
change.** The check reads history and writes nothing.

---

## 0. Planning record

### 0.1 Brainstorm — where can the check live, and what can it see?

| # | Idea | Verdict |
|---|------|---------|
| B1 | A `harvest upgrade-check` subcommand in the `harvest` CLI. | Rejected. The CLI cannot load user workflow types or user codecs. It cannot replay a user workflow. |
| B2 | A library API in the candidate build, the way `ReplayVerifier` works. The operator runs it from the candidate binary. | **Adopted.** Only the candidate build holds its own types and codec keys. |
| B3 | Export a sample bundle, then replay it in CI (#798). | Rejected as the main path. The issue asks for no export. The bundle path stays for CI. |
| B4 | Read in-flight runs straight from each shard. Decode payloads in memory with the candidate codecs. | **Adopted.** Plaintext never leaves the process. |
| B5 | Diff `harvest-verify` JSON reports of two builds. | Rejected. The report holds verdicts only. It has no call graph and no step data. |
| B6 | Add `--emit-structure` to `harvest-verify`. It writes the resolved call graph of each workflow, with a digest per body. | **Adopted.** The analyzer already resolves the graph. A recorder keeps what it visits. |
| B7 | Diff workflow source text with `syn`. | Rejected. It cannot follow helpers across modules or crates. The issue names the `harvest-verify` graph. |
| B8 | Split the workflow body into segments at each `.await`, and digest each segment. | Rejected. Optimized MIR renumbers locals and blocks across the body. A segment digest changes on unrelated edits. |
| B9 | A step is a helper body that emits commands. A change confined to such a body is "passed" when the run has completed every command the body can emit. | **Adopted.** §3 lists the proof conditions. |
| B10 | Read each step key from the MIR: a string constant at the sink, or the `X_info()` call that builds a typed activity or child workflow. | **Adopted.** An unknown key makes the step unprovable, so the verdict is review. |
| B11 | Validate signal and update payloads against the candidate schemas (#373, #610). | **Adopted.** A buffered signal waits in `harvest_signals`, outside history. No replay reads it, so only a schema can check it. |
| B12 | Wait for activity and side-effect schemas (#1994). | Rejected as a blocker. Replay consumes every completed activity output and side-effect value before the frontier. #1994 adds a second check later. |

### 0.2 Reverse brainstorm — how can this check give a wrong "migrate"?

| # | How to make it lie | Mitigation |
|---|--------------------|------------|
| R1 | Call a strict replay. An in-flight run then reads as a divergence. | Use the canary replay mode. A run that parks at the end of its history passes. |
| R2 | Read a change to the workflow body as "passed" because the run passed part of it. | A change to the root body is always review. Only a helper body can be "passed". |
| R3 | A changed helper runs again later, in a loop or from a second call site. | Each call edge from the root to the helper must be the only call site of its callee and must sit outside a CFG cycle. |
| R4 | A changed helper has not started, but another body completed the same activity name. | Each step key of the helper must be emitted only inside the helper's subtree. |
| R5 | A changed helper is still suspended inside itself, or its code after the last result has not run. | No command of the helper may be open, a parking call without a command is unprovable, and a decision must run after the last result. |
| R6 | A changed helper emits nothing, but its return value feeds a later step. | A helper with no step sites cannot be "passed". The verdict is review. |
| R7 | A dependency body changes, but the analyzer cannot see it. | Any `unknown` boundary in the workflow graph gives review. |
| R8 | A line shift above a body changes its digest. | The digest drops spans and `allocN` numbers. A false change gives review, never migrate. |
| R9 | A buffered signal in `harvest_signals` holds an old payload shape. | Validate it against the candidate schema. With no candidate schema, the verdict is review. |
| R10 | The report leaks plaintext through an error message. Serde errors quote the bad value. | The report holds kinds, names and indexes only. It holds no error text and no payload. A test asserts it. |
| R11 | New workflow code hangs in replay. | A per-run replay timeout gives review. It cannot stop a loop that never yields. |
| R12 | A shard is down, or the run list is cut at the limit. | The report says so, and the exit code is 2. |
| R13 | The candidate build does not register the workflow type. | Verdict pin. |
| R14 | The manifests come from other builds than the ones under test. | The manifest records the rustc version and model version. The operator guide says to build both manifests from the two release commits. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Canary replay exists (`replay_canary_snapshot`). `load_history_inflated` decodes and inflates with a codec registry in memory. `harvest-verify` resolves the graph but does not serialize it. No activity schema exists yet (#1994). `assigned_build_id` pins a run at start. No code re-pins a run. |
| Red | Operators fear a silent "migrate" more than a noisy "review". A wall of "review" still erodes trust. |
| Black | MIR text is not a stable API. Step keys from MIR are best effort. Value-level drift, such as a changed threshold in a static, is invisible to the graph. A cross-crate helper is a boundary. |
| Yellow | Most upgrades touch activities only. Those runs get "migrate" with no human step. The check uses the operator's own keys, so no history leaves the trust boundary. |
| Green | Make every uncertainty an explicit review reason, so an operator sees what blocks "migrate". Keep the manifest format versioned, so a later step-graph emitter (#2010) can replace the body model. |
| Blue | Red phase: verify fixtures and structure tests, core tests per failure mode, codec test, DB test. Green phase: recorder, manifest, diff, verdict engine, DB driver, command. Refactor phase: docs, gates, review. |

### 0.4 Corrections after the review

Three review agents read the first version. They found wrong `migrate`
verdicts, and each fix has a test:

1. The digest hashed the parsed body, which drops `switchInt` case values.
   It now hashes the raw MIR text, `const` items and `allocN` footers.
2. A closure passed to `map` ran many times but counted as one start. Such
   a call now counts as a loop.
3. A helper could hold a result whose next decision had not run, as in a
   `PAUSED` run. Condition 7 now asks for a later decision event.
4. `await_condition` parks with no command. It is now a step that no run
   can prove complete.
5. A repeated step, a callee shared with the root, and an unknown key
   outside the helper each passed. Conditions 3 to 6 now refuse them.
6. A future type in generic arguments made a real call look like a resume.
   Only `into_future`, `poll` and the `Pin` constructors resume now.

---

## 1. Verdict rules

Each run gets a list of findings. The verdict is the worst finding:
pin > review > migrate. A run with no finding gets migrate.

| Finding | Verdict |
|---|---|
| `workflow-not-registered` — the candidate has no handler for the type | pin |
| `nondeterminism` — replay diverges inside the recorded prefix | pin |
| `replay-failed` — replay fails where the recorded run did not | pin |
| `payload-schema-violation` — a recorded or pending payload breaks a candidate schema | pin |
| `history-undecodable` — the candidate codecs cannot decode the history, a pending signal or the context headers | pin |
| `replay-timed-out` | review |
| `payload-unchecked` — a signal, recorded or pending, or an open update has no candidate schema | review |
| `payload-offloaded` — the run holds offloaded payloads, and the check has no offloader | review |
| `structure-unavailable` — a manifest is missing, the two manifests come from different toolchains or models, two workflows share the name, or the run is assigned to another build than the baseline | review |
| `unknown-boundary` — the workflow graph has an `unknown` boundary, or the workflow has declarative update handlers | review |
| `root-changed` — the workflow body itself changed | review |
| `step-not-passed` — a changed helper may still run for this run | review |

## 2. Structure manifest

`cargo harvest-verify --emit-structure FILE` writes this JSON:

```json
{
  "format": "harvest-structure/1",
  "model_version": "2026.10.0",
  "rustc_version": "rustc 1.99.0 (...)",
  "workflows": [{
    "workflow": "my_crate::orders::place_order",
    "name": "place_order",
    "root": "my_crate::orders::place_order::{closure#0}",
    "boundaries": ["external-crate-body: other_crate::f"],
    "bodies": [{
      "id": "my_crate::orders::reserve::{closure#0}",
      "digest": "9f3c...",
      "calls": [{ "callee": "my_crate::orders::helper", "in_loop": false, "resume": false }],
      "steps": [{ "sink": "execute_activity", "kind": "activity", "key": "reserve", "in_loop": false }]
    }]
  }]
}
```

- `name` is the registered name. The `#[workflow]` macro registers the fn
  name.
- `id` is the body path with each span removed.
- `digest` hashes the raw MIR text of the body. It also hashes the items
  nested under the body, the `const` items it reads and the `allocN`
  footers they name. It drops spans and `allocN` numbers.
- `calls` holds one entry per call site. `in_loop` is true when the call
  sits in a cycle of the caller's control flow, or runs a closure that the
  caller passes to another call. `resume` is true when the call handles a
  future that another call built: `into_future`, `poll`, a `Pin`
  constructor or drop glue.
- `steps` holds one entry per sink call site, and one per `await_condition`
  call, which can park with no command. `key` is `null` when the MIR does
  not show it.

## 3. When a changed helper counts as "passed"

A body is **changed** when its digest differs, or when only one manifest
has it. For a run `R` of workflow `W`, a changed body `b` is passed only
when all of these hold, in each manifest that has `b`. The run ran the
baseline code, so a wait that only the baseline `b` holds still counts:

1. `b` is not the root body.
2. `b` starts at most once per run. Each body on the call chain from the
   root to `b` has exactly one starting call site. No such call site is in
   a loop.
3. The subtree of `b` runs only inside a run of `b`. No body in it, except
   `b`, is called from outside it. No call inside it is in a loop.
4. Each step site in the subtree has a known key and a kind that history
   can prove complete. No step site is in a loop.
5. No step site outside the subtree has the same kind with the same key,
   or with an unknown key.
6. For each key, `R` completed it at least as often as the subtree has
   sites for it, and no instance of it is open.
7. A decision ran after the last result that the engine wrote for those
   keys. A later decision event in history proves it.

Conditions 2, 3 and 5 make the keys belong to one run of `b`. Condition 6
proves that `b` started and is not parked on a command. Condition 7 proves
that the code after the last result ran: a decision writes its events at
the history position it loaded, so a later decision event saw the result.
So `b` is finished, and its new code never runs for `R`. The replay of the
prefix already ran the new code of `b` against the recorded history.

Step kinds map to history as follows:

| Kind | Completed | Open |
|---|---|---|
| `activity` | `ActivityScheduled` or `ActivityAwaitingExternal` with `ActivityCompleted` or `ActivityCompletedExternally` | scheduled with neither |
| `local-activity` | `LocalActivityScheduled` with `LocalActivityCompleted` | scheduled without it |
| `timer` | `TimerStarted` with `TimerFired` or `TimerCancelled` | started with neither |
| `child` | `ChildWorkflowStarted` with a completed or failed child, or `ChildWorkflowSpawnedDetached` | started with neither |
| `side-effect` | `SideEffectRecorded` | never |
| `version` / `patch` | `MarkerRecorded` with the name `version:<key>` / `patch:<key>` | never |

No other kind can prove completion, so a helper that reaches one cannot be
passed. Signals, mutexes, `continue_as_new`, random values and
`await_condition` are such kinds.

## 4. Rehydration

- Canary replay deserializes each recorded payload with the candidate
  types: workflow input, activity outputs, child results, side-effect
  values, signals and finished updates. A failure is `replay-failed`.
- The worker writes `SignalReceived` into history at claim time, and the
  canary flags one that the code does not consume. A signal that waits in
  `harvest_signals` is not in history yet, so no replay reads it. The
  canary also excuses an open update.
- A recorded signal can also wait in the workflow's buffer while the run
  waits on another step. Replay then passes it with no decode.
- So the check validates each signal, recorded or pending, and each open
  update against the candidate schema for its name. With no schema, the
  finding is `payload-unchecked`.
- The check also validates each recorded workflow input, `SignalReceived`
  and `UpdateAdmitted` payload, when the candidate publishes a schema.
- An offloaded payload is inflated through the candidate offloader. With
  no offloader, the finding is `payload-offloaded`. Replay and the schema
  checks then do not run, because a claim-check stub is not the payload.

## 5. Trust boundary

The DB driver loads each history and each pending signal with the
candidate codecs and offloader. Both reads share one read-only
`REPEATABLE READ` snapshot, so a signal that a worker ingests between them
is not lost. Plaintext
exists only in process memory. A run verdict holds execution ids, workflow
names, finding kinds, event indexes and code names. It holds no payload
and no error text. The `incomplete` list of a report holds shard database
errors only. The check runs no write statement.

Candidate workflow code runs during the replay. Code that logs its own
input can still print plaintext.

## 6. The command

The library gives `UpgradeCheck` and `upgrade_check::run_command`. The
operator adds one binary to the candidate build. The example
`autumn-harvest/examples/upgrade_check.rs` shows it.

```console
$ my-worker-upgrade-check \
    --database-url-env SHARD0_URL --database-url-env SHARD1_URL \
    --baseline-structure old.structure.json \
    --candidate-structure new.structure.json \
    --format json
```

| Exit code | Meaning |
|---|---|
| 0 | Every run gets migrate. |
| 1 | At least one run gets review or pin. |
| 2 | The check is incomplete: a shard failed, the run list was cut, or an input was bad. |

## 7. TDD plan and acceptance criteria

| AC | Red test first |
|---|---|
| A command outputs migrate / review / pin per run | `run_command_prints_one_verdict_per_run`; `the_db_check_gives_a_verdict_per_in_flight_run` |
| A nondeterministic change gives a non-migrate verdict | `a_nondeterministic_change_is_pinned` |
| A breaking payload type change gives a non-migrate verdict | `a_breaking_activity_output_type_is_pinned`; `a_buffered_signal_that_breaks_the_schema_is_pinned`; `a_recorded_update_that_breaks_the_schema_is_pinned` |
| A structural change in an unreached step gives a non-migrate verdict | `a_change_in_an_unreached_step_needs_review` |
| A change confined to activities gives migrate | `an_activity_body_change_leaves_the_workflow_graph_unchanged` and `end_to_end::an_activity_only_change_migrates` (verify, real manifests); `a_change_confined_to_activities_migrates` (core) |
| A change confined to passed steps gives migrate | `a_change_confined_to_a_passed_step_migrates` (core); `end_to_end::a_passed_step_migrates_and_an_unreached_step_needs_review` (verify, real manifests) |
| Codec-encrypted runs are checked without exporting plaintext | `an_encrypted_history_is_checked_in_memory`; `an_encrypted_pending_signal_is_checked_in_memory`; `the_db_check_decodes_in_memory_and_writes_nothing`; `a_pending_signal_in_the_database_is_decoded_and_checked` |

## 8. Known limits

- Value-level drift outside a changed body is invisible. A changed static
  or a changed config value can alter behavior with no digest change.
- A step key behind a function parameter is unknown, so that helper is
  never passed.
- A change to a helper in another crate is a boundary, so the verdict is
  review.
- MIR text is not a stable API. Build both manifests with the same
  toolchain. The check refuses two manifests from different toolchains.
- A declarative update handler is not in the graph, so a workflow with one
  always needs review.
- The replay timeout cannot stop candidate code that never yields.
- Nothing ties the candidate manifest to the binary under test. Build both
  from the same commit.
- The check reads the in-flight runs once. A run that starts on the
  baseline build after that read is not in the report. The operator closes
  the baseline set first: the build policy points new starts at the
  candidate build. The check only reads, so it cannot enforce this. Compat
  is declared after the check, never before.
- With `--baseline-build`, the scan reads only runs on that build or on no
  build. Compat from the candidate covers no other run.
- The baseline manifest describes one build. A run with an assigned build
  is compared only when `--baseline-build` names that build. A run with no
  assigned build is trusted to have run the baseline.
