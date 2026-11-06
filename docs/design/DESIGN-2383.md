# DESIGN-2383: the refine example, guide section and ADR

Issue #2383. Parent: #2369. Needs #2381. Uses #2382 and #2394.

## Problem

The refine loop has shipped in parts: proposals (#2379), the run (#2380),
the gate (#2381), lessons (#2382) and the policy (#2394). No example runs
it end to end. The guide has a section per part, but no section explains
the loop as one design. No ADR records why the loop is offline, under
ceilings and under operator policy.

## Decision

1. **The example.** `autumn-harvest-agent/examples/agent_refine.rs` runs
   on SQLite with scripted models and no credentials.
   1. Op 1 is a recorded `agent_loop` run of the kind `support`. Its
      prompt lacks the refund rule, so its answer fails the check.
   2. `agent_refine` with k = 1 reads op 1. The scripted proposer adds
      the rule. The probe replays op 1 under the candidate.
   3. The run waits at the gate. The example approves it with
      `sqlite::decide_refine`.
   4. Op 2 is an op of the durable agent `support/customer-2`. It reads
      the active version and passes the check.
   - The check is the built-in `ExactMatch` scorer.
   - The example prints the active version before and after, and the
     result of op 2. It exits non-zero when op 2 fails.
   - `test = true` runs its tests in the crate test step. A test with a
     reject shows that op 2 then fails, so the check is not vacuous.
   - The refine workflow needs the `eval` feature. So the example needs
     `sqlite` and `eval`, and CI runs it with both.
2. **The guide.** `docs/agent-adapter.md` gets section 25, "Refine an
   agent". It explains the loop as one design and links each part
   section. It does not copy the settings table of section 18. It names
   each setting and links the table, so one table stays the source.
3. **The ADR.** `docs/adr/0016-agent-refinement.md`. ADR 0014 holds the
   policy rule and ADR 0015 the xtask rule, so 0016 is the next free
   number. It records the four decisions of the issue and links ADR 0014
   for the policy detail.
4. **The ADR index.** No index existed. `docs/adr/README.md` lists each
   ADR, in file order. A guard checks that each file in `docs/adr/` has a
   row, so the index cannot drift. `docs/design/README.md` links the
   index and ADR 0016.
5. **The comparison.** No docs guard asks for a refinement row in
   `docs/comparison.md`, so the page does not change.
6. **The guard.** `autumn-harvest-agent/tests/refine_docs.rs` checks the
   guide section, the ADR, both indexes, the fragment and the CI step.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| What op 1 is | A durable op. An `agent_loop` run. | An `agent_loop` run. A durable run has no replay probe, so refine refuses it as a source. |
| What op 2 is | A new `agent_loop` run. A durable op. | A durable op. Only a durable op reads the active version. |
| How an op fails | A plain string test. An answer check. The `ExactMatch` scorer. | `ExactMatch`. It is the shipped check, and the guide documents it. |
| How the proposer finds the rule | A model call. A fixed reply. | A fixed reply for the DOM stage. The INV stage keeps its base, so the run shows a `KeptBase` stage. |
| Where the proof of success runs | `main` only. A test only. Both. | Both. `cargo run` proves the AC. The test runs in the crate step and adds the reject control. |
| The version before | An operator version 1. None. | None. The kind starts on its base profile, as a new kind does. The version after is 1. |
| The lesson library | Leave it out. A memory store. | An in-memory store. The run writes one lesson, and the example prints it. |
| The settings table | Copy it. Link it. | Link it. A copy drifts and conflicts. |
| The ADR index | A list in the design README. A new `docs/adr/README.md`. | A new index with a guard. The design README links it. |

## Reverse brainstorm

How can the example lie, or the docs drift? Each failure has a guard.

| Failure | Guard |
|---|---|
| Op 2 passes on the base prompt, so the refine proves nothing. | Op 1 runs on the same base and must fail. The reject test shows op 2 fails without the approval. |
| Op 2 fails and CI stays green. | `main` returns an error, so the process exits 1. The test asserts the pass. |
| The approval goes in before the gate opens and is dropped. | The example waits for `refine_status = waiting_approval` first. |
| The example needs a key or the network. | The models are scripted. The store is in memory. The database is a temp file. |
| CI stops running the example. | The guard reads `ci.yml` for the `cargo run` line. |
| A guide fence stops compiling. | The guide adds no Rust fence. It links the example. |
| The guide copies the settings table, and the copy drifts. | The section links section 18. |
| A new ADR has no index row. | The guard lists `docs/adr/` and checks each row. |
| The section states a live count. | It states rules and bounds only. |
| A comment cites the issue. | No comment names the issue. Comment hygiene runs on each `*.rs` file. |

## Six hats

- **White.** `sqlite::start_refine`, `sqlite::decide_refine`,
  `sqlite::send`, `sqlite::drive` and `sqlite::state` exist. The refine
  run needs `eval` and a multi-thread runtime. `AgentState::last_answer`
  holds the answer of a durable op. ADR numbers 0014 and 0015 are taken.
- **Red.** A reader wants to see the loop work before reading 800 lines
  of guide. The output must show the change in a few lines.
- **Black.** The issue command names `--features sqlite` only. The
  refine workflow needs `eval` too. The example cannot run without it.
  The command in CI and in the docs names both.
- **Yellow.** The example is a tested template for an app: install a
  proposer, a composer and a store, then refine, approve and serve.
- **Green.** Later: a CLI walk-through on a Postgres server with
  `harvest agents profile approve`.
- **Blue.** Plan. Red: the guard test and the example test fail. Green:
  the example, the section, the ADR, the index, the fragment and the CI
  step. Refactor: the gates, then a review.

## Limits

- The models are scripted. The example shows the mechanics of the loop,
  not the quality of a real proposer.
- SQLite has no fork, so the example runs no live re-probe.

## Tests

- `autumn-harvest-agent/examples/agent_refine.rs`: op 1 fails, op 2
  passes after the approval, and op 2 fails after a reject.
- `autumn-harvest-agent/tests/refine_docs.rs`: the docs guards.
