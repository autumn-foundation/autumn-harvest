# DESIGN-2450: the inference strategies example, guide section and ADR

Issue #2450. Parent epic #2438. Needs #2443, #2444 and #2445. Related:
ADR 0013 (concurrent agent children).

## 1. Problem

The epic adds five strategies over durable agent runs. Each one has its
own guide section and design record. No page shows them side by side. An
app author cannot tell when best-of-N is worth N times the cost. No ADR
records why Harvest scores with verifiers and not log-probabilities.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where the example lives | an example of the agent crate · **the workspace package `examples/agent-strategies`, so `cargo run -p agent-strategies` runs it** |
| The backend | Postgres · **SQLite, with no service** |
| The model | a real provider · **a scripted model, so no key and no network** |
| What it runs | each strategy on its own task · **one task under each of four strategies: single, best-of-3, vote and a two-tier cascade** |
| What it prints | the answer only · **the selected answer, the draws and the cost of each** |
| The scorer | a judge model · **`ExactMatch` with the expected answer** |
| The guide | a new page · **a new section of `docs/agent-adapter.md`, with one table** |
| The ADR | one ADR per decision · **ADR 0017, with four decisions** |
| The guards | none · **a test of the example cost, a docs guard and a CI step** |

## 3. Reverse brainstorming

"How could the example or the docs mislead a reader?" Each answer gives a
rule.

| Failure | Rule |
|---|---|
| The example drifts from the API and still prints. | CI runs it, and a test checks each selected answer. |
| The output does not name a strategy. | CI checks the output for each of the four names. |
| The printed cost is not the cost of the draws. | A test checks that the best-of-3 cost is the sum of its three draws. |
| The table copies a setting that later changes. | The table names the policy setting and links section 19. |
| The ADR repeats the design records. | The ADR records decisions only, and links the guide and the records. |
| A reader thinks a draw that is not selected can write. | The guide and the ADR say that such a draw runs `ReadOnly` tools by default. |
| A doc lands at the repository root. | Each new page is under `docs/`. `root-files` checks it. |

## 4. Six thinking hats

- **White (facts).** `sweep_compare` and `agent_refine` show the pattern
  of an offline example: a scripted model with a price per token, an
  in-memory policy store and a temporary SQLite file. `SampleReport`
  holds each draw with its cost, and the cost of the run tree.
- **Red (feelings).** A reader wants to see the cost next to the gain.
  A table with one row per strategy gives that at a glance.
- **Black (risks).** A new workspace member adds build time to CI. The
  example has the same dependencies as the agent crate, so the cache
  covers it.
- **Yellow (benefits).** One command shows each strategy, and the ADR
  gives one place for the reasons.
- **Green (ideas).** A later example can delegate a strategy to a peer
  (#2610), or compare strategies with a sweep.
- **Blue (process).** Red test first: the example test asserts each
  selected answer and the best-of-3 cost. Then the example, the guide
  section, the ADR, the indexes, the changelog and CI.

## 5. Decision

- `examples/agent-strategies` is a binary with a library part. `run`
  returns one `Outcome` per strategy, and `main` prints them.
- The task is one question with an expected answer. The cheap model
  answers it right on some draws only. The strong model always answers it
  right, at ten times the price.
- The policy of the kind allows `best_of_n`, `vote` and `cascade`, with
  both models in `cascade_models`.
- Guide section 28, "Inference strategies", holds the table.
- ADR 0017 records the four decisions of the issue.

## 6. Acceptance

| Line of the issue | Evidence |
|---|---|
| `cargo run -p agent-strategies` exits 0 in CI and names four strategies. | The CI step greps each name. |
| A test asserts that the best-of-3 cost is the sum of its three draws. | `the_best_of_three_cost_is_the_sum_of_its_draws` |
| The ADR is in `docs/design/README.md`, and the link and comment checks pass. | `strategies_docs.rs`, `corpus-link-check` and `comment-hygiene` |
| No new document at the repository root. | `cargo xtask audit root-files` |
