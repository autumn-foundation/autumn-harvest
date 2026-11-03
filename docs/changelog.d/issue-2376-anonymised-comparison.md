## Anonymised comparison reports (issue #2376)

A model that reads a comparison to pick a candidate is no longer biased by
labels or order. Parent: #2369. It builds on the overlay candidates of
#2375. The design is in `docs/design/DESIGN-2376.md`.

- **`eval::compare_anonymised(history, candidates, seed)`** runs a pool of
  any size on one source. It returns an `AnonymisedReport`: the rendered
  `text`, the structured `pool` and the `key`. The text and the pool hold no
  model name, overlay digest, label or input index.
- **Ids.** Each candidate gets `c-<hex>`, a prefix of
  `SHA-256(seed, source run id, Candidate::digest)`. The pool sorts by that
  hash, so the input order changes no id and no text. The prefix has at
  least 12 digits and grows until no two ids are equal.
- **Key.** Each id maps to a `CandidateRef`: the input index, the candidate
  digest, the overlay digest and the label. `AnonymisedReport` does not
  implement `Serialize`, so one call cannot encode the key with the text.
- **Candidate identity.** `Candidate::label` names a candidate, for example
  with an overlay version id. `Candidate::digest` hashes the overlay digest,
  the label, the routes, the ceilings and the turn cap. Equal digests give
  `EvalError::DuplicateCandidate` before any model call.
- **Kept fields.** `AnonymisedEvaluation` keeps every field of `Evaluation`
  except `overlay_digest`. An exhaustive destructure fails the build when
  `Evaluation` gains a field.
- **Proof.** `formal/verus/src/agent_anonymise.rs` mirrors `shared_digits`
  and `id_width`. It proves that the width is the least safe width from
  the floor, and that two prefixes of that width differ.
- **Tests.** Eight integration tests in `autumn-harvest-agent/tests/eval.rs`
  and nine unit tests in `eval/anonymise.rs`, with a proptest that any input
  order gives each candidate the same id and rank. No new `WorkflowEvent`
  variant. No migration.
