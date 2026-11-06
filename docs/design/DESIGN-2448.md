# DESIGN-2448: the `agent_sweep` workflow

Issue #2448. Parent epic #2438. Needs #2441 (scorers) and #2443
(`agent_sample`). Related: #2380 (`agent_refine` can use a sweep as a
probe).

## 1. Problem

Cascades' `DatasetSampler` runs one inference method over each item of a
dataset and puts the samples in one table. It runs in one process. A crash
on item 900 of 1000 loses the sweep.

Harvest's `eval` replays recorded runs. It cannot compare two strategies
on live runs over a fixed set of inputs. That comparison needs a durable
run over the set, with one row per item and the cost of each row.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| What runs one item | a new item loop · `agent_loop` and a score in the sweep · **an `agent_sample` child run** |
| A single draw | best-of-1, which the policy must list · **a new `SampleStrategy::Single`, which needs no strategy in the policy** |
| The expected answer of an item | a scorer per item · **`expected` on `SampleInput` and `ScoreInput`; `ExactMatch` reads it** |
| Items over the activity cap | split the sweep · offload through the engine payload store · **a `SweepItemStore` on the harness, and a digest reference in the input** |
| How the sweep reads a stored list | one read of the whole list · **a recorded page read: at most 1 MiB of items for each read** |
| The item concurrency | free · each item takes the policy cap · **items × draws per item ≤ the policy cap** |
| The budget | no budget · a new budget type · **the token cap of the input, split over each wave with `sibling_cap`, as in `agent_sample`; the LLM budgets apply to each child** |
| The crash unit | one item · **one child run: history records the end of each child** |
| A child that fails in the engine | fail the sweep · **a `failed` row; the sweep goes on** |
| The report size | each `SampleReport` · **one short row per item: the answer and the reason are cut** |

## 3. Reverse brainstorming

"How could a sweep spend more, run more, or report a false table?" Each
answer gives a rule.

| Failure | Rule |
|---|---|
| A crash runs a completed item again. | Each item is a child run. History records its end. Replay reads it. |
| A sweep of 16 items × 16 draws opens 256 model calls. | The policy cap bounds items × draws per item. A wider ask is refused before the first model call. |
| A sweep ignores a spent budget. | A child that stops `budget_exceeded` stops the sweep after its wave. Each later item is `not_run`. |
| The sweep starts an item with no token left. | `sample_step` gives the item wave. With no token left, it is `BudgetExceeded`, and no child starts. |
| An item that the budget refused before any call counts as run. | A `budget_exceeded` child with no spend is `not_run`. |
| The stored list changes between two page reads. | The reference holds the SHA-256 digest and the item count. Each read checks both. |
| A stored list is too large for a read result or a child input. | Each item is at most 64 KiB. A page is at most 1 MiB. A list is at most 1000 items. |
| A large answer or reason makes the report too large. | A row keeps at most 512 bytes of the answer and 256 bytes of a reason. |
| A single draw needs a policy entry that the operator did not give. | `single` needs no strategy. The policy still admits the scorer. |
| A policy field rides in the input. | `SweepInput` and `SweepItem` deny unknown fields. |
| The total cost differs from the ledger. | The total sums the cost of each child report. Each child sums the ledger cost of its turns and its judge calls. |

## 4. Six thinking hats

- **White (facts).** `agent_sample` runs one input with one strategy, and
  its report holds the usage and the cost of its tree. `sample_step` and
  `sibling_cap` are proven kernels. A child result goes into the history of
  its parent. The engine caps an activity input and a result at 2 MiB.
  The engine payload store does not offload a workflow start input.
- **Red (feelings).** An operator fears a sweep that multiplies the spend
  and the concurrency of a kind. A user fears a table that hides a skipped
  item.
- **Black (risks).** A field on `SampleInput` and `ScoreInput` touches
  recorded types. Each new field is optional and serde skips it when empty,
  so old histories decode. A sweep of 1000 items has a long history. The
  sweep does not use continue-as-new.
- **Yellow (benefits).** A crash costs only the items in flight. One report
  compares strategies by score, accept rate and cost per accepted answer.
- **Green (ideas).** `agent_refine` can run a sweep as a probe. A vote or a
  cascade strategy can join `SampleStrategy` later, and the sweep runs it
  with no change.
- **Blue (process).** Red tests first, one for each acceptance line. Then
  the kernel and its proof, the workflow, the store and the docs. Then a
  review from several angles.

## 5. Decision

```rust
pub enum SampleStrategy { Single, BestOfN { n }, Rejection { max_attempts, threshold } }
pub struct SweepItem { input: String, expected: Option<String> }
pub enum SweepItems { Inline(Vec<SweepItem>), Stored(SweepItemsRef) }
pub struct SweepItemsRef { key: String, digest: String, items: u32, bytes: u64 }
pub struct SweepInput {
    kind: String, items: SweepItems, strategy: SampleStrategy,
    scorer: ScorerRef, concurrency: Option<NonZeroU32>,
    max_total_tokens: Option<u32>,
}
pub enum ItemStatus { Completed, BudgetExceeded, Failed, NotRun }
pub struct SweepRow {
    item, status, verdict: Option<Verdict>, accepted, answer: Option<String>,
    draws, usage, cost_usd_micros, error: Option<String>,
}
pub struct SweepTotals {
    items, run, accepted, not_run, failed, mean_score, accept_rate,
    usage, cost_usd_micros, cost_per_accepted_usd_micros,
}
pub enum SweepStop { Completed, BudgetExceeded }
pub struct SweepReport {
    kind, strategy, revision, concurrency, draw_concurrency, stopped,
    rows: Vec<SweepRow>, totals: SweepTotals,
}
```

1. The sweep checks the kind, then reads the profile and the policy once
   each, as `agent_sample` does. It admits the strategy and the scorer of
   each item before the first model call.
2. `sweep_plan` (the kernel) gives the item concurrency `c` and the draws
   per item `d`. `c` is the ask, or the cap of the policy. `d` is
   `cap / c`, at most the draws of the strategy. So `c × d ≤ cap`. An ask
   over the cap is refused with `too_many_concurrent_draws`.
3. Each item runs as one `agent_sample` child with the concurrency `d`. A
   wave holds at most `c` items. `sample_step` gives its width over the
   items. Item `i` of a wave gets the token cap `sibling_cap(left, width,
   i)`.
4. After a wave, the spend of each item counts against the budget. A child
   that stops `budget_exceeded` stops the sweep. With no token left and
   items left, the sweep stops. Each item that did not start is `not_run`.
5. A `budget_exceeded` child with no tokens and no cost is `not_run`: the
   budget refused it before its first model call.
6. A child that fails in the engine gives a `failed` row with the error.
   The sweep goes on. Its spend is not in the report.
7. A stored list is read in pages. A page holds at most 1 MiB of items, and
   at least one item. Each read checks the digest and the item count.
8. `SampleInput::expected` passes the expected answer of an item to
   `ScoreInput::expected`. `ExactMatch` compares with it when it is set.
   `ExactMatch::per_item` has no text of its own and rejects an item with
   no expected answer.
9. The row verdict is the verdict of the selected draw. With no selection,
   it is the best score, or the first reject.
10. `mean_score` is the mean over the rows with a verdict. A reject counts
    as 0. `accept_rate` is the accepted rows over the rows that ran.
    `cost_per_accepted_usd_micros` is the total cost over the accepted
    rows, rounded down.

**Known limits.**

- The sweep does not use continue-as-new, so a sweep of 1000 items has a
  long history. `MAX_SWEEP_ITEMS` bounds it.
- The orphan sweep of a shared payload store can delete a stored list that
  no run references. Give the item store its own payload store.
- A failed child loses its spend in the report. The ledger keeps it.
- The loop checks a token cap between model calls, so an item can pass its
  share by one call, as in `agent_sample`.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| A 5-item sweep with `ExactMatch` gives 5 rows, the right verdicts and the accept rate | `a_five_item_sweep_with_exact_match_gives_five_rows` in `tests/sweep.rs` |
| A crash after item 3 resumes at item 4 | `a_crash_after_item_three_resumes_at_item_four` in `tests/sweep_crash.rs` |
| A budget for 2 items marks items 3 to 5 `not_run` | `a_budget_for_two_items_marks_the_rest_not_run` |
| The total cost equals the ledger | `the_total_cost_equals_the_ledger_of_the_sweep_tree` |
| A list over the activity cap goes by reference | `a_list_over_the_activity_cap_goes_by_reference` |
| An example compares two strategies | `examples/sweep_compare.rs` |
| The kernel | `formal/verus/src/agent_sweep.rs` and the proptest `a_sweep_plan_never_passes_the_cap` |
