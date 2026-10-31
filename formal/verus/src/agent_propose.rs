//! A proposer stage retries a bounded number of times, then inherits
//! (issue #2379).
//!
//! The stage loop in `refine/propose.rs` calls `next_step` after each
//! reply. The copy below is that kernel. The spec `run` drives the loop
//! over any sequence of verdicts. The theorems state three properties. A
//! stage uses 1 to `max` attempts. It falls back exactly when every attempt
//! is invalid. It never asks again after a valid reply. The proptest
//! `a_stage_is_bounded_and_stops_at_the_first_valid_reply` checks the same
//! loop on the engine.

use vstd::prelude::*;

verus! {

/// The check of one reply.
pub enum Verdict {
    Invalid,
    Same,
    Differs,
}

/// What a stage does after one reply.
pub enum Step {
    Retry,
    Propose,
    Keep,
    FallBack,
}

/// `next_step` as a spec.
pub open spec fn step_of(attempt: int, max: int, verdict: Verdict) -> Step {
    match verdict {
        Verdict::Differs => Step::Propose,
        Verdict::Same => Step::Keep,
        Verdict::Invalid => if attempt < max {
            Step::Retry
        } else {
            Step::FallBack
        },
    }
}

// mirror: autumn-harvest-agent/src/refine/propose.rs next_step
pub fn next_step(attempt: u32, max: u32, verdict: Verdict) -> (r: Step)
    ensures
        r == step_of(attempt as int, max as int, verdict),
        r is Retry <==> (verdict is Invalid && attempt < max),
        r is FallBack <==> (verdict is Invalid && attempt >= max),
        r is Propose <==> verdict is Differs,
        r is Keep <==> verdict is Same,
{
    match verdict {
        Verdict::Differs => Step::Propose,
        Verdict::Same => Step::Keep,
        Verdict::Invalid => {
            if attempt < max {
                Step::Retry
            } else {
                Step::FallBack
            }
        }
    }
}

/// The verdict of attempt `i`. Attempt 1 reads `verdicts[0]`. An attempt
/// past the script is invalid, as a proposer with no answer is.
pub open spec fn verdict_at(verdicts: Seq<Verdict>, i: int) -> Verdict {
    if 1 <= i <= verdicts.len() {
        verdicts[i - 1]
    } else {
        Verdict::Invalid
    }
}

/// The stage loop from attempt `attempt`. Returns the last step and the
/// attempts used.
pub open spec fn run(verdicts: Seq<Verdict>, max: int, attempt: int) -> (Step, int)
    decreases max - attempt,
{
    let step = step_of(attempt, max, verdict_at(verdicts, attempt));
    if step is Retry {
        run(verdicts, max, attempt + 1)
    } else {
        (step, attempt)
    }
}

/// The loop from `attempt` ends between `attempt` and `max`. Each attempt
/// before the last one is invalid. The last step matches the last verdict.
proof fn lemma_run(verdicts: Seq<Verdict>, max: int, attempt: int)
    requires
        1 <= attempt <= max,
    ensures
        attempt <= run(verdicts, max, attempt).1 <= max,
        !(run(verdicts, max, attempt).0 is Retry),
        forall|i: int|
            attempt <= i < run(verdicts, max, attempt).1 ==> #[trigger] verdict_at(verdicts, i)
                is Invalid,
        run(verdicts, max, attempt).0 is FallBack ==> run(verdicts, max, attempt).1 == max
            && verdict_at(verdicts, max) is Invalid,
        run(verdicts, max, attempt).0 is Propose <==> verdict_at(
            verdicts,
            run(verdicts, max, attempt).1,
        ) is Differs,
        run(verdicts, max, attempt).0 is Keep <==> verdict_at(
            verdicts,
            run(verdicts, max, attempt).1,
        ) is Same,
    decreases max - attempt,
{
    if step_of(attempt, max, verdict_at(verdicts, attempt)) is Retry {
        lemma_run(verdicts, max, attempt + 1);
    }
}

/// A stage uses from 1 to `max` attempts, and it never ends on a retry.
pub proof fn theorem_a_stage_is_bounded(verdicts: Seq<Verdict>, max: int)
    requires
        1 <= max,
    ensures
        1 <= run(verdicts, max, 1).1 <= max,
        !(run(verdicts, max, 1).0 is Retry),
{
    lemma_run(verdicts, max, 1);
}

/// A stage falls back exactly when each of its `max` attempts is invalid.
/// It then uses all `max` attempts.
pub proof fn theorem_a_stage_falls_back_exactly_when_every_attempt_is_invalid(
    verdicts: Seq<Verdict>,
    max: int,
)
    requires
        1 <= max,
    ensures
        run(verdicts, max, 1).0 is FallBack <==> forall|i: int|
            1 <= i <= max ==> #[trigger] verdict_at(verdicts, i) is Invalid,
        run(verdicts, max, 1).0 is FallBack ==> run(verdicts, max, 1).1 == max,
{
    lemma_run(verdicts, max, 1);
    let n = run(verdicts, max, 1).1;
    if !(run(verdicts, max, 1).0 is FallBack) {
        assert(!(verdict_at(verdicts, n) is Invalid));
    }
}

/// A stage that does not fall back ends at its first valid reply. So the
/// proposer is never asked after a valid reply. A `Same` reply keeps the
/// base, and a `Differs` reply proposes.
pub proof fn theorem_a_stage_stops_at_the_first_valid_reply(verdicts: Seq<Verdict>, max: int)
    requires
        1 <= max,
    ensures
        !(run(verdicts, max, 1).0 is FallBack) ==> !(verdict_at(
            verdicts,
            run(verdicts, max, 1).1,
        ) is Invalid),
        forall|i: int|
            1 <= i < run(verdicts, max, 1).1 ==> #[trigger] verdict_at(verdicts, i) is Invalid,
        run(verdicts, max, 1).0 is Keep <==> verdict_at(verdicts, run(verdicts, max, 1).1) is Same,
        run(verdicts, max, 1).0 is Propose <==> verdict_at(
            verdicts,
            run(verdicts, max, 1).1,
        ) is Differs,
{
    lemma_run(verdicts, max, 1);
}

} // verus!
