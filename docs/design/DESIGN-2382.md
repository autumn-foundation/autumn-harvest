# DESIGN-2382: the lesson library of an agent kind

Issue #2382. Parent: #2369. Needs #2381. Policy: #2394. Related: #2272,
#2267.

## Problem

Harness² keeps, for each domain, each adopted setup and a lesson that the
evidence supports. The next refinement reads them. The `MemoryStore` is
scoped to one agent (`agent:<id>`), not to a kind. A lesson from untrusted
input can also steer a later editor into a prompt edit. That is an
injection path.

## Decision

`autumn-harvest-agent/src/refine/lessons.rs` holds the library.

1. **The scope.** Each kind has the memory scope `kind:<kind>:lessons`.
   It holds one block, `lessons`. A new scope gets no default block. Each
   entry is one lesson as one line of JSON:

   ```json
   {"version":3,"why":"...","lesson":"...","sources":["<exec id>"],"agents":["support/acct-42"]}
   ```

   `version` is the adopted version. `why` and `lesson` come from the
   select step. `sources` holds the source exec ids. `agents` holds each
   agent of the kind that a source names: its workflow id, the agent of
   its memory scope `agent:<id>`, and each agent of its mandate chain. It
   also holds each agent of the lessons that the run read.
2. **The write.** After the gate, a run that ends `adopted` or
   `auto_adopted` runs the activity `agent_refine_lesson`. It is behind
   the patch `agent_refine_lesson`, so an old history replays.
   - `MemoryStore::ensure_block` adds the empty `lessons` block once.
   - `MemoryStore::apply_labelled` adds the entry with the labels of the
     run. So the write reuses `apply_labelled_op`, and a store that cannot
     keep labels refuses a labelled write, as it does today.
   - The step reads the current policy of the kind. It removes the oldest
     entries until the block holds at most `lessons.max_entries`. So a
     change of the bound applies to the next write.
   - Each edit has a key from the run id, so a retry adds and removes
     nothing twice. `evicted` counts the removals of the attempt that
     answered.
   - A failed or timed-out step is a note in the report,
     `lesson.outcome = failed`. The adoption stands. Any other engine
     error, such as a cancel, fails the run as before.
3. **Taint.** The start step reads the labels of each source with
   `source_labels`. The run labels are the union of the source labels and
   the labels of the snapshot that it read. The lesson carries the run
   labels. The report holds them as `flow_labels`. A start record of an
   older release has no labels, so the run writes no lesson, and the
   report notes why.
4. **The read.** The start step loads the scope. It drops the oldest
   entries until the rendered snapshot fits 32 768 characters, and
   records it. A failed read gives no snapshot and a warning, so a store
   fault never fails the start step. Each
   proposer stage gets it as `ProposeRequest::lessons`.
   `ProposeContext::lessons` shows it to the proposer. `ModelProposer`
   puts it in the user message, under a heading that marks it as data.
5. **The select step.** `OverlaySelector::choose` returns a `Choice` with
   an id, a `why` and a `lesson`. The default calls `select` and gives no
   text. The run keeps the text of the selection that set the chosen
   overlay.
6. **Erase.** `Control::ResetMemory` runs two activities behind the patch
   `agent_lesson_erase`. `agent_lesson_match` records the hash of each
   lesson of the kind whose `agents` holds the agent id. `agent_lesson_erase`
   removes each such lesson, then loads the block again. A lesson that is
   still there fails the step. The count comes from the recorded match, so
   a retry keeps it. The final state holds `lessons_removed`, and
   `harvest agents erase` reports it. A plain memory reset also removes
   these lessons, and a lesson that other agents share goes too.

### The kernel

`formal/verus/src/refine_lessons.rs` proves two pure functions.

- `evictions(len, max)` gives the number of oldest entries to remove. The
  rest is at most `max`. It removes nothing when `len <= max`. It never
  removes the newest entry when `max >= 1`.
- `writes_lesson(status)` is `true` for `adopted` and `auto_adopted`
  only. So a rejected, timed-out or superseded run writes no lesson.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where lessons live | A new store trait. A `MemoryStore` scope. | A `MemoryStore` scope, as the issue asks. It reuses the label rules and the harness slot. |
| The block of a new scope | The `memory` block. The store template. A new trait method. | `ensure_block`. The default template has no `lessons` block, and `memory` holds 2 200 characters. |
| The entry format | Prose. JSON. | One line of JSON. The erase must read `agents` back. |
| Who owns a source run | The exec ids from the CLI. The workflow id. The task. | Each agent that the run names: its workflow id, its memory scope and its mandate chain. The worker knows no exec id list. |
| A lesson that read another lesson | Keep its own agents. Add the agents of the read lessons. | Add them. Its text can hold what it read, so an erase of those agents reaches it. |
| The erase count after a retry | Count each attempt. Record a match first. | Record a match first. A retry then keeps the count. |
| When the bound applies | The recorded policy. The policy at write time. | The policy at write time. The issue asks for a change at runtime. |
| How the proposer reads it | Inside `Evidence::Text`. A new field. | A new field. The labels go with the text. |
| The `why` and `lesson` text | A breaking change of `select`. A provided `choose`. | A provided `choose`. Each app selector still compiles. |
| A failed write | Fail the run. A note. | A note. The adoption already happened. |

## Reverse brainstorm

How can the library leak or lose a taint? Each failure has a guard.

| Failure | Guard |
|---|---|
| A lesson from untrusted input loses its labels. | The write goes through `apply_labelled`. A store with no label support refuses it. |
| A second refine reads a tainted lesson and writes a clean one. | The run labels hold the snapshot labels. The new lesson takes them. |
| An old start record gives no labels, so the lesson is clean. | No labels means no lesson. |
| A rejected or timed-out run writes a lesson. | `writes_lesson` is `true` for an adoption only. Verus proves it. |
| The library grows without a bound. | `evictions` removes the oldest. Verus proves the bound. |
| A retry adds a lesson twice. | Each edit has a key from the run id. |
| A raised bound applies one run late. | The write reads the current policy. |
| An erase misses a lesson. | `agents` holds each agent that a source names, and each agent of the lessons that the run read. The erase loads the block again and fails while a match remains. |
| An erase skips a lesson of a newer release. | The match reads the key `agents` only, so an unknown key does not hide it. |
| A retry of an erase reports too few lessons. | The match step records the count first. |
| An erase removes the lesson of another agent. | It matches the exact agent id. A test keeps the lessons of `support/acct-4` and `support/acct-7`. |
| A lesson closes its block or forges a prompt section. | `lessons::snapshot` escapes each entry. The JSON has no line break. |
| A large library fills each proposer request. | The rendered snapshot keeps the newest entries in 32 768 characters. |
| A slow memory store fails a refine run. | A failed read gives no snapshot and a warning. |
| A cancel during the write ends the run as a success. | Only a failed or timed-out step is a note. Each other error fails the run. |
| A replay of an old history runs a new activity. | Each new activity is behind its own patch. |
| A failed write undoes the adoption. | The write runs after the move. A failure is a note. |

## Six hats

- **White.** `apply_labelled_op` keeps labels per entry. `MemorySnapshot`
  records labels. `source_labels` reads the labels of a recorded run.
  `RefinePolicy::lessons.max_entries` is 1 to 4096, default 64.
  `harness.refine_policy` reads the current policy.
- **Red.** An operator must trust that a lesson never launders a taint.
  An erase must remove the lessons of a person.
- **Black.** The library is per kind, so one agent can steer every agent
  of its kind. The taint rule keeps that visible. A store with no
  `ensure_block` refuses every write. The note names the fix.
- **Yellow.** The next refine starts from what the last ones learned. The
  library is plain memory, so the existing tools read it.
- **Green.** Later: a CLI that lists the library, and a lesson of a
  rejection, as Harness² keeps.
- **Blue.** Spec and proof of the kernel. Red tests on SQLite. Then the
  store method, the activities, the agent erase, the CLI report and the
  docs.

## Limits

- The `why` and `lesson` text needs a selector that gives it. The rule
  pick and a sequential run give none. The lesson still names the version
  and the sources.
- The erase finds a run of an agent only by the names that the run holds:
  its workflow id, its memory scope and its mandate chain. A run that
  names none of them is no run of the agent.
- The erase reads the library of the kind of the agent only.
- A refine run that read a lesson before an erase can write a lesson
  after it. That lesson names the erased agent, so a second erase removes
  it.
- History keeps copies: the selection text in each refine report, the
  `LessonRequest` of the write, and the snapshot in each later
  `ProposeRequest`. The erase does not reach them. Erase those runs with
  `harvest workflow erase-payloads <exec id>`.
- `InMemoryMemoryStore` lives in one process. A fleet of workers needs a
  shared store that implements `ensure_block` and `apply_labelled`.
- `ProposeRequest` refuses an unknown key. Upgrade each activity worker
  before a refine run reads lessons.
- The snapshot of the proposer holds at most 32 768 rendered characters.

## Tests

`autumn-harvest-agent/tests/refine_lessons.rs` covers each criterion on
SQLite. Unit tests and proptests in `refine/lessons.rs` cover the kernel,
the entry format and the erase. `agents.rs` tests cover the CLI report.
