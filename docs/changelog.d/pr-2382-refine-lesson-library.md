## The lesson library of an agent kind (issue #2382)

Each adoption of a refine run now adds one lesson to the library of its
kind. The next refine of the kind reads it. The design is in
`docs/design/DESIGN-2382.md`.

- The memory scope `kind:<kind>:lessons` holds the block `lessons`. Each
  entry is one `refine::Lesson` as one line of JSON. It holds the adopted
  version, the `why` and `lesson` text of the select step, the source
  exec ids and the agents that the sources name.
- A run that ends `adopted` or `auto_adopted` runs the activity
  `agent_refine_lesson`, behind the patch `agent_refine_lesson`. Any other
  end writes no lesson. `RefineReport::lesson` holds the result. A failed
  or timed-out write is a note, and the adoption stands.
- The lesson keeps the flow labels of the run: the labels of each source
  and of the lessons that the run read. `RefineReport::flow_labels` holds
  them. The write goes through `MemoryStore::apply_labelled`, so a store
  that cannot keep labels refuses a labelled write.
- The write reads the current `lessons.max_entries` and removes the
  oldest lessons first. A change of the bound applies to the next write.
- `ProposeRequest::lessons` and `ProposeContext::lessons` carry the
  snapshot of the library to each proposer stage. `ModelProposer` puts it
  in its prompt as data. The snapshot keeps the newest lessons that fit
  32 768 rendered characters. A failed read gives no snapshot.
- `OverlaySelector::choose` returns a `Choice` with an id, a `why` and a
  `lesson`. The default calls `select`.
- `MemoryStore::ensure_block` adds a block to a scope once. The default
  refuses. `InMemoryMemoryStore` and the `FileMemoryStore` of
  `examples/autoresearch` implement it.
- **Behavior change.** `Control::ResetMemory` runs `agent_lesson_match`
  and `agent_lesson_erase`, behind the patch `agent_lesson_erase`. It
  removes each lesson of the kind that names the agent, also one that
  other agents share. `AgentState::lessons_removed` counts them, and
  `harvest agents erase` reports `lessons_removed`.
- **API change.** These exhaustive structs get new fields:
  - `ProposeRequest::lessons` and `RefineStart::lessons`, `lesson_agents`;
  - `SourceBrief::labels`, `agents`;
  - `Selection::why`, `lesson` and `SelectionRecord::why`, `lesson`;
  - `RefineReport::flow_labels`, `lesson`;
  - `StoresOutcome::lessons_removed` in the CLI crate.

  A struct literal needs the new fields. A record of an older release
  decodes with none.
- **Upgrade.** Upgrade each activity worker first. An older worker refuses
  a `ProposeRequest` that holds `lessons`.
- Verus proves that a write keeps the bound and that only an adoption
  writes a lesson (`formal/verus/src/refine_lessons.rs`).
