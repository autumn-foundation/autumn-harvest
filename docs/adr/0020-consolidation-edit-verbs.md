# ADR 0020: A consolidation pass can replace and remove facts

## Status

Accepted. littleguy idea #13 proposed it. The owner accepted it on
2026-11-23.

## Context

- The consolidation pass of issue #2454 turns episodes into entries of the
  facts block. Its reply shape was `{subject, relation, object, episodes}`.
  Each fact became a `MemoryOp::Add`. So a pass could only add.
- A fact goes stale or proves wrong, and no pass can take it out. A
  person must edit the block by hand. littleguy saw this in its own block:
  a memory capacity of 2,200 characters stayed after the setting moved to
  32,000. Two tool gaps stayed after the tools shipped. A sandbox fact was
  wrong from the start.
- Mem0 gives each candidate an ADD, UPDATE, DELETE or NOOP verb. Letta
  rewrites the whole block. Zep marks a fact invalid and keeps it.
- The write path has all three edits already. `MemoryOp` has `Add`,
  `Replace` and `Remove`, and `apply_labelled_op` keeps labels through
  each one.
- The pass did not read the block. A model that cannot see an entry
  cannot name it.
- Labels are a taint. `apply_labelled_op` gives a changed entry its own
  labels and the labels of the writer, so an edit never removes a taint.
  A removed entry takes its labels with it. So a remove from a writer
  with fewer labels launders a taint out of the block.
- `FactWrite.mark` keeps the sources of a refused fact unconsolidated. A
  refused remove with that rule comes back at each pass, with no end.
- `PASS_SYSTEM` asked for facts that "stay true across runs". A value
  such as a capacity, a file list or a tool list stays true for days, then
  changes with no episode to record the change. The request gave each
  episode its time as epoch milliseconds only. A small model cannot turn
  that into a date safely.

## Decision

1. `ProposedFact` gets a verb: `add`, `replace` or `remove`, as
   `FactVerb`. A reply with no verb decodes as `add`. So a recorded reply
   from before this ADR replays the same. A verb is case-blind. `update`
   reads as `replace`, and `delete` as `remove`. Any other word is
   `FactVerb::Unknown`. The pass rejects that one fact, not the reply.
2. `ProposedFact` gets `fact`, the id of the block entry that a replace or
   a remove changes. `subject`, `relation` and `object` decode with a
   default, so a remove needs only `fact` and `subject`.
3. The pass reads the facts block. `ConsolidateRead.facts` holds each entry
   whose labels are a subset of the labels of the pass episodes. The
   request lists them as `<fact id="f1">…</fact>`. So the block adds no
   taint to the pass. An entry with more labels is hidden, and no edit can
   name it.
4. `fact_ops` turns the reply into edits. An id names one entry, so the
   match is unique by construction. The write sends the whole entry text
   as `old`, so `apply_op` still checks the unique match.
   - An add needs every part and a known source, as before.
   - A replace needs the same, and a known id. A replace to the same text
     is skipped.
   - A remove needs a known id and a subject that occurs in that entry,
     case-blind. The subject guards against a wrong id. A remove needs no
     episode, because the block itself can show an entry wrong.
   - A second edit of one entry in one pass is rejected.
   `fact_entries` keeps its signature. It reads no block, so it rejects
   each edit.
5. `PassFact` carries the edit: `verb` and `target`. An add-only fact
   serializes as before, and its edit key keeps the form
   `<pass>:<fact_hash>`. A replace or a remove puts its verb and target
   hash in its key.
6. The write guards each remove. It loads the block first. When the
   target has a label that the pass lacks, the write drops the remove and
   keeps the entry. The step 3 filter makes this rare: it needs a write
   that adds a label to the entry while the pass runs.
7. A dropped edit consumes its sources. The causes are the guard of
   step 6 and a target that is gone. A retry finds the same state, so to
   keep the sources would repeat the drop at each pass. An add that the
   store refuses, and a replace that does not fit, still keep their
   sources, as before.
8. `ConsolidationReport` adds `facts_removed`, `edits_dropped` and
   `edit_drops`. A drop reason names the entry by its `fact_hash`, never by
   its text. `FactsWritten` adds `removed` and `edit_drops`.
9. `PASS_SYSTEM` asks for each verb. It asks the model to remove a fact
   that an episode contradicts or states better, or that the block itself
   shows wrong. It asks the model to write a volatile value as of the date
   of its episode, and not to state it as true now.
10. The request gives each episode a `date="YYYY-MM-DD"` beside `at`. The
    date is UTC and has no time of day. So the model copies a date and
    never computes one. Two passes that copy one date write one text, and
    the `fact_hash` of the fact holds.
11. `ProposedFact` gets no `as_of` field. A model that sends one would see
    it parse and then vanish. The date goes in the fact text.

## Consequences

- A pass can retract a stale fact, correct a wrong one and drop a weak one
  that a later episode states better.
- The guard holds for each pass. A narrow race stays: a writer can add a
  label to the target between the load and the remove. The window is one
  activity long, and the entry text goes with its labels, so no tainted
  text stays behind.
- A pass sends the readable block in each request. A full block of 32,000
  characters adds about 8,000 tokens. Size `max_pass_output_tokens` and
  the budget for it.
- `ProposedFact` and `PassFact` add public fields. A struct literal must
  add them, or use `..ProposedFact::default()`. `ConsolidateRead` and
  `ConsolidateTurn` add `facts`. Each new field decodes with a default,
  so a history from before this ADR replays the same.
- `PASS_SYSTEM` changes. A test model that matches on its text sees the
  new text.
- littleguy reads the write input of a pass for its reflection. It reads
  `entry` only. A remove has an empty `entry`, so littleguy needs a change
  to show removes and `edit_drops`.
