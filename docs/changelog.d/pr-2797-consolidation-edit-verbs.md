## Agents — a consolidation pass can replace and remove facts (ADR 0020)

The pass of issue #2454 could only add facts, so a stale or wrong fact
stayed until a person edited the block. littleguy proposed the change as
its idea #13.

- **Verbs.** `ProposedFact` adds `verb` (`FactVerb`: `add`, `replace`,
  `remove`) and `fact`, the id of a block entry. A reply with no verb is
  an add, so a recorded reply replays the same. An unknown verb rejects
  one fact, not the reply.
- **The block in the request.** The read loads each entry of
  `facts_block` whose labels are a subset of the pass labels, into
  `ConsolidateRead.facts`. The request lists them as `<fact id="f1">`. An
  entry with more labels stays hidden, so the pass takes no new taint.
- **Edits.** `fact_ops` turns the reply into `PassFact` edits with a
  unique target. A remove names the subject of its entry and needs no
  episode. `fact_entries` keeps its signature and rejects each edit.
- **Taint guard.** The write drops a remove of an entry with a label that
  the pass lacks, so a remove cannot launder a taint. A dropped edit
  consumes its episodes, so the next pass does not propose it again.
  `ConsolidationReport` adds `facts_removed`, `edits_dropped` and
  `edit_drops`. A reason names the entry by its `fact_hash`.
- **Dates.** Each episode in the request has `date="YYYY-MM-DD"` (UTC).
  `PASS_SYSTEM` asks for a volatile value as of that date.

No `WorkflowEvent` variant, no migration. Each new field decodes with a
default, and an add-only `PassFact` serializes as before, so an old
history replays the same.

Tests: `consolidate::tests` covers the verb decode, the edit rules, the
request, the date and the write guard. `tests/episodes.rs` runs a pass
that retracts a stale fact, a remove that loses a race to a tainted
writer, and a pass that never sees a tainted entry.
