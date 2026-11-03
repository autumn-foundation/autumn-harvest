# DESIGN-2376 — Anonymised comparison reports

Issue #2376. Parent epic #2369. Needs #2375. After Harness²
(`harness2/core/selection.py`).

## 1. Problem

A model reads an `eval::compare` report to pick or compose a candidate.
Labels and order bias that choice. A model name, an overlay version, or
"first" against "second" can sway it. Harness² anonymises the pool before
selection. Harvest has no such step.

This design adds `eval::compare_anonymised`. It runs a pool of candidates on
one source. It gives each candidate an opaque id in a seeded order. It
renders the turn diffs under those ids. The id-to-candidate map stays
outside the text.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Pool size | two candidates, as `compare` · **any number, as a slice** |
| What names a candidate | the overlay digest · the input index · **`Candidate::digest`: the overlay digest, the routes, the admitted models, the ceilings, the turn cap and an operator label** |
| How a version id reaches the candidate | a field on `ProfileOverlay` · **`Candidate::label`, which enters the digest and never the report** |
| How the order is seeded | a seeded shuffle of the input · **a sort by `SHA-256(seed, source run id, candidate digest)`** |
| The id | an ordinal (`c-1`) · a fixed 16-hex prefix · **`c-` and the shortest hex prefix of the sort key, at least 12 digits, that keeps every id unique** |
| Two equal candidates | a counter suffix · **`EvalError::DuplicateCandidate`, before any model call** |
| Where the map goes | inside the report · **a separate `key` field. `AnonymisedReport` is not `Serialize`, so one call cannot encode the text with the map** |
| The run order | input order · **pool order, so shared state cannot tell the input order** |
| The report form | text only · structured only · **both: `AnonymisedPool` (JSON) and its rendered text** |
| What the text shows of a run end | the whole `AgentReport` · **the stop, the counts, the final text and the error** |

## 3. Reverse brainstorming

"How could the report still name a candidate, or still carry an order?"
Each answer gives a rule.

| Failure | Rule |
|---|---|
| The report holds `overlay_digest`. | The anonymised entry drops it. An exhaustive destructure of `Evaluation` fails the build when a new field arrives unclassified. |
| The id derives from the label or the model name alone, so a reader can guess it. | The id hashes the seed and the source run id with the digest. Keep the seed from the reader. |
| The input order leaks through the run order, for example through a shared scripted model. | The harness runs the candidates in pool order. |
| The input order leaks through the text. | The text lists the candidates in pool order only. It shows no index. |
| Two short ids collide. | The id width grows until every pair differs. `formal/verus/src/agent_anonymise.rs` proves the width rule. |
| Two equal candidates get one id, so the map loses one. | Equal digests give equal keys. The harness refuses the pool before any model call. |
| A caller serialises the whole report into a prompt. | `AnonymisedReport` does not derive `Serialize`. Its `Debug` form hides the key. The pool and the key encode apart. |
| A route to a missing model fails the run, and the harness error names the model. | A candidate refuses each route that its harness does not hold, before any model call. |
| A provider error names its model. | Each failure text replaces each label, digest, route and model id of the pool. Only a whole name is replaced. |
| A new call id holds the provider id, and its form names the vendor. | A new `eval_` id holds the turn and the position only. |
| A source with no run id leaves the engine run id in the candidate report, so two runs differ. | The pool clears the run ids of such a report. |
| A model reply names its own model. | Out of scope. The text renders model output as written, because that output is what the reader judges. The guide states this limit, with the token usage and the policy reasons. |
| A refused overlay or a duplicate costs model calls first. | Each overlay and each id is checked before the first run. |

## 4. Six thinking hats

- **White (facts).** `compare` runs two candidates. `Evaluation` holds one
  identity field, `overlay_digest`. The source history holds no execution
  id. The recorded run id is the execution id by default. `ProfileOverlay`
  already has a SHA-256 digest.
- **Red (feeling).** A selection step that leaks a label defeats the epic.
  The guarantee must hold by construction, not by care.
- **Black (risks).** Identity in the text, order in the text, an id
  collision, and a lost map entry. Section 3 answers each one.
- **Yellow (value).** A refine loop can hand the text to a model with no
  label bias. The key turns its choice back into a candidate.
- **Green (alternatives).** Redact known names from the model output. It
  mangles text when a name is short, and it cannot catch a paraphrase. It
  gives false confidence, so model output keeps its text. A failure text is
  harness or provider text, not the subject of the choice. So the pool
  redacts whole names there.
- **Blue (process).** Spec, proof, red tests, then code. The Verus kernel
  comes first. Each acceptance criterion has a test.

## 5. Decision

```rust
pub struct CandidateId(String);                 // "c-<hex>"
impl Candidate {
    pub fn label(self, label: impl Into<String>) -> Self;
    pub fn digest(&self) -> String;              // "sha256:<hex>"
}
pub async fn compare_anonymised(
    history: &[WorkflowEvent],
    candidates: &[Candidate],
    seed: u64,
) -> Result<AnonymisedReport, EvalError>;
pub struct AnonymisedReport {
    pub text: String,                            // pool.render()
    pub pool: AnonymisedPool,                    // no identity
    pub key: BTreeMap<CandidateId, CandidateRef>,
}
```

1. The harness reads the source and applies every overlay. A refusal costs
   no model call.
2. Each candidate gets a sort key: `SHA-256` of a domain tag, the seed, the
   source run id and the candidate digest, each length-prefixed.
3. The pool sorts by key. The id width is the least width of at least 12
   hex digits that is above the shared prefix of every pair. A pair with
   equal keys gives `DuplicateCandidate`.
4. The candidates run in pool order. Each `Evaluation` becomes an
   `AnonymisedEvaluation`, which keeps every field except `overlay_digest`.
5. `AnonymisedPool::render` writes the text. It holds the ids and the
   source end. Per candidate it holds the counts, the end, each turn
   verdict and each turn decision as JSON.
6. `CandidateRef` holds the input index, the candidate digest, the overlay
   digest and the label.

## 6. Acceptance criteria and their tests

All tests are in `autumn-harvest-agent/tests/eval.rs` unless named.

| Criterion | Test |
|---|---|
| The text holds no model name, overlay digest or version id | `the_anonymised_text_names_no_candidate` |
| `[a, b]` and `[b, a]` give the same id for `a` and the same text | `the_input_order_changes_no_id_and_no_text` |
| Two seeds order a pair apart | `two_seeds_order_a_pair_apart`, a unit test in `eval/anonymise.rs` with fixed seeds and a fixed run id. `the_seed_reorders_the_pool` checks a recorded source. Its run id changes on each run, so that test tries seeds until the order changes. |
| The map resolves every id, and no id repeats | `the_key_resolves_every_id_once` |
| The report keeps every non-identity field of `Evaluation` | `the_report_keeps_every_field_that_is_not_an_identity`, and `anonymise` destructures `Evaluation` exhaustively |
| The id width rule | `formal/verus/src/agent_anonymise.rs`, and the unit test `a_shared_prefix_widens_every_id` |
| Duplicates and bad overlays cost no model call | `equal_candidates_are_refused_before_any_model_call`, `a_bad_overlay_in_the_pool_is_refused_before_any_model_call`, `an_unknown_agent_route_is_refused_before_any_model_call` |
| Failure texts name no candidate | `a_failure_text_names_no_candidate`, and the unit test `only_a_whole_name_is_redacted` |
| The run order is the pool order | `the_candidates_run_in_pool_order` |
