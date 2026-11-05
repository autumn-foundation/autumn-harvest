//! A fan-out names why each lone delegation ran alone (issue #2483).
//!
//! `fan_out_plan` gives each call of one model turn a step. A batch runs at
//! the same time. Any other delegation runs alone, and its step names why.
//! The copy below proves the plan for every turn and every bound.
//!
//! A bound under 2 turns the fan-out off, so no step names a reason. With a
//! bound of 2 or more, no delegation that can run falls back in silence. Each
//! reason is true of the turn. A batch holds from 2 to `bound` calls that can
//! join, and each of its members follows its lead.

use vstd::prelude::*;

verus! {

/// Why a delegation ran alone.
pub enum SerialReason {
    /// No other call of the turn could join a batch.
    SingleCall,
    /// Another call could join, but no neighbour of this call could.
    NotConsecutive,
    /// The policy gated the call.
    Gated,
    /// The call delegates to a durable peer.
    DurablePeer,
    /// The batch before the call holds `bound` calls.
    BoundReached,
}

/// What one call is to a fan-out.
pub enum Fit {
    /// An allowed delegation to an inline peer. It can join a batch.
    Batchable,
    /// A delegation that the policy gated.
    Gated,
    /// A delegation to a durable peer.
    Durable,
    /// Any other call. The plan does not touch it.
    Other,
}

/// The plan for one call.
pub enum Step {
    /// The call leads a batch of this many calls.
    Lead(usize),
    /// The call is in the batch of an earlier lead.
    Member,
    /// The delegation runs alone, for this reason.
    Alone(SerialReason),
    /// The plan does not touch the call.
    Skip,
}

/// Can the call join a batch?
pub open spec fn joins(fit: Fit) -> bool {
    fit is Batchable
}

/// The count of calls in `fits[0..n]` that can join a batch.
pub open spec fn count(fits: Seq<Fit>, n: int) -> int
    decreases n,
{
    if n <= 0 {
        0
    } else {
        count(fits, n - 1) + if joins(fits[n - 1]) { 1int } else { 0int }
    }
}

/// Another call of the turn than call `i` can join a batch.
pub open spec fn others(fits: Seq<Fit>, i: int) -> bool {
    exists|j: int| 0 <= j < fits.len() && j != i && #[trigger] joins(fits[j])
}

/// A neighbour of call `i` before it can join a batch.
pub open spec fn left(fits: Seq<Fit>, i: int) -> bool {
    0 < i && joins(fits[i - 1])
}

/// A neighbour of call `i` after it can join a batch.
pub open spec fn right(fits: Seq<Fit>, i: int) -> bool {
    i + 1 < fits.len() && joins(fits[i + 1])
}

/// Each reason is true of the turn.
pub open spec fn reason_ok(fits: Seq<Fit>, i: int, reason: SerialReason) -> bool {
    match reason {
        SerialReason::Gated => fits[i] is Gated,
        SerialReason::DurablePeer => fits[i] is Durable,
        SerialReason::BoundReached => joins(fits[i]) && left(fits, i) && !right(fits, i),
        SerialReason::NotConsecutive => joins(fits[i]) && !left(fits, i) && !right(fits, i)
            && others(fits, i),
        SerialReason::SingleCall => joins(fits[i]) && !others(fits, i),
    }
}

/// Step `i` of `steps` is right for the turn and the bound.
pub open spec fn step_ok(fits: Seq<Fit>, bound: int, steps: Seq<Step>, i: int) -> bool {
    if bound < 2 {
        steps[i] is Skip
    } else {
        match steps[i] {
            Step::Skip => fits[i] is Other,
            Step::Lead(n) => 2 <= n <= bound && i + n <= steps.len() && (forall|j: int|
                i <= j < i + n ==> #[trigger] joins(fits[j])) && (forall|j: int|
                i < j < i + n ==> #[trigger] steps[j] is Member),
            Step::Member => joins(fits[i]) && exists|k: int|
                0 <= k < i && #[trigger] steps[k] is Lead && i < k + steps[k]->Lead_0,
            Step::Alone(reason) => reason_ok(fits, i, reason),
        }
    }
}

/// The count is at least 0 and at most `n`.
proof fn lemma_count_bounds(fits: Seq<Fit>, n: int)
    requires
        0 <= n,
    ensures
        0 <= count(fits, n) <= n,
    decreases n,
{
    if n > 0 {
        lemma_count_bounds(fits, n - 1);
    }
}

/// One call that can join, and no other in `fits[0..n]`, counts as 1.
proof fn lemma_a_lone_call_counts_one(fits: Seq<Fit>, n: int, i: int)
    requires
        0 <= i < n <= fits.len(),
        joins(fits[i]),
        forall|j: int| 0 <= j < n && j != i ==> !#[trigger] joins(fits[j]),
    ensures
        count(fits, n) == 1,
    decreases n,
{
    if n - 1 > i {
        lemma_a_lone_call_counts_one(fits, n - 1, i);
    } else {
        lemma_none_counts_zero(fits, n - 1);
    }
}

/// No call that can join in `fits[0..n]` counts as 0.
proof fn lemma_none_counts_zero(fits: Seq<Fit>, n: int)
    requires
        0 <= n <= fits.len(),
        forall|j: int| 0 <= j < n ==> !#[trigger] joins(fits[j]),
    ensures
        count(fits, n) == 0,
    decreases n,
{
    if n > 0 {
        lemma_none_counts_zero(fits, n - 1);
    }
}

/// Two calls that can join in `fits[0..n]` count as 2 or more.
proof fn lemma_two_calls_count_two(fits: Seq<Fit>, n: int, i: int, j: int)
    requires
        0 <= i < j < n <= fits.len(),
        joins(fits[i]),
        joins(fits[j]),
    ensures
        count(fits, n) >= 2,
    decreases n,
{
    if n - 1 > j {
        lemma_two_calls_count_two(fits, n - 1, i, j);
    } else {
        lemma_one_call_counts_one(fits, n - 1, i);
    }
}

/// One call that can join in `fits[0..n]` counts as 1 or more.
proof fn lemma_one_call_counts_one(fits: Seq<Fit>, n: int, i: int)
    requires
        0 <= i < n <= fits.len(),
        joins(fits[i]),
    ensures
        count(fits, n) >= 1,
    decreases n,
{
    lemma_count_bounds(fits, n - 1);
    if n - 1 > i {
        lemma_one_call_counts_one(fits, n - 1, i);
    }
}

/// The count of the whole turn tells whether another call can join.
proof fn lemma_the_count_names_the_others(fits: Seq<Fit>, i: int)
    requires
        0 <= i < fits.len(),
        joins(fits[i]),
    ensures
        count(fits, fits.len() as int) > 1 <==> others(fits, i),
{
    if others(fits, i) {
        let j = choose|j: int| 0 <= j < fits.len() && j != i && #[trigger] joins(fits[j]);
        if j < i {
            lemma_two_calls_count_two(fits, fits.len() as int, j, i);
        } else {
            lemma_two_calls_count_two(fits, fits.len() as int, i, j);
        }
    } else {
        lemma_a_lone_call_counts_one(fits, fits.len() as int, i);
    }
}

/// Each step of `steps[0..n]` is right.
pub open spec fn prefix_ok(fits: Seq<Fit>, bound: int, steps: Seq<Step>, n: int) -> bool {
    forall|i: int| 0 <= i < n ==> #[trigger] step_ok(fits, bound, steps, i)
}

/// A push keeps each right step of `steps[0..n]` right.
proof fn lemma_push(fits: Seq<Fit>, bound: int, steps: Seq<Step>, step: Step, n: int)
    requires
        0 <= n <= steps.len(),
        prefix_ok(fits, bound, steps, n),
    ensures
        prefix_ok(fits, bound, steps.push(step), n),
{
    let after = steps.push(step);
    assert forall|i: int| 0 <= i < n implies #[trigger] step_ok(fits, bound, after, i) by {
        assert(after[i] == steps[i]);
        assert(step_ok(fits, bound, steps, i));
        if bound >= 2 {
            match steps[i] {
                Step::Lead(len) => {
                    assert forall|j: int| i < j < i + len implies #[trigger] after[j] is Member by {
                        assert(after[j] == steps[j]);
                    }
                },
                Step::Member => {
                    let k = choose|k: int|
                        0 <= k < i && #[trigger] steps[k] is Lead && i < k + steps[k]->Lead_0;
                    assert(after[k] == steps[k]);
                },
                _ => {},
            }
        }
    }
}

/// A push of a member keeps each member of the open batch at `at` right.
/// The batch holds `len` calls.
proof fn lemma_push_member(fits: Seq<Fit>, bound: int, steps: Seq<Step>, at: int, len: usize)
    requires
        2 <= bound,
        len <= bound,
        0 <= at < steps.len() < at + len <= fits.len(),
        steps[at] == Step::Lead(len),
        prefix_ok(fits, bound, steps, at),
        forall|j: int| at <= j < at + len ==> #[trigger] joins(fits[j]),
        forall|j: int| at < j < steps.len() ==> #[trigger] steps[j] is Member,
        forall|j: int| at < j < steps.len() ==> #[trigger] step_ok(fits, bound, steps, j),
    ensures
        prefix_ok(fits, bound, steps.push(Step::Member), at),
        forall|j: int| at < j <= steps.len() ==> #[trigger] steps.push(Step::Member)[j] is Member,
        forall|j: int|
            at < j <= steps.len() ==> #[trigger] step_ok(fits, bound, steps.push(Step::Member), j),
{
    let after = steps.push(Step::Member);
    lemma_push(fits, bound, steps, Step::Member, at);
    assert(after[at] == steps[at]);
    assert forall|j: int| at < j <= steps.len() implies #[trigger] after[j] is Member by {
        if j < steps.len() {
            assert(after[j] == steps[j]);
        }
    }
    assert forall|j: int| at < j <= steps.len() implies #[trigger] step_ok(
        fits,
        bound,
        after,
        j,
    ) by {
        assert(after[at] is Lead && j < at + after[at]->Lead_0);
        assert(after[j] is Member);
        assert(joins(fits[j]));
    }
}

// mirror: autumn-harvest-agent/src/fan_out_plan.rs can_join
pub fn can_join(fit: &Fit) -> (r: bool)
    ensures
        r == joins(*fit),
{
    matches!(fit, Fit::Batchable)
}

// mirror: autumn-harvest-agent/src/fan_out_plan.rs fan_out_plan
pub fn fan_out_plan(fits: &[Fit], bound: usize) -> (steps: Vec<Step>)
    ensures
        steps.len() == fits.len(),
        prefix_ok(fits@, bound as int, steps@, fits.len() as int),
{
    let mut steps: Vec<Step> = Vec::new();
    if bound < 2 {
        let mut i = 0;
        while i < fits.len()
            invariant
                bound < 2,
                i <= fits.len(),
                steps.len() == i,
                prefix_ok(fits@, bound as int, steps@, i as int),
            decreases fits.len() - i,
        {
            proof {
                lemma_push(fits@, bound as int, steps@, Step::Skip, i as int);
            }
            steps.push(Step::Skip);
            i += 1;
        }
        return steps;
    }
    let mut joining: usize = 0;
    let mut i = 0;
    while i < fits.len()
        invariant
            i <= fits.len(),
            joining == count(fits@, i as int),
            steps.len() == 0,
        decreases fits.len() - i,
    {
        proof {
            lemma_count_bounds(fits@, i as int);
        }
        if can_join(&fits[i]) {
            joining += 1;
        }
        i += 1;
    }
    let mut at = 0;
    while at < fits.len()
        invariant
            2 <= bound,
            at <= fits.len(),
            steps.len() == at,
            joining == count(fits@, fits.len() as int),
            prefix_ok(fits@, bound as int, steps@, at as int),
        decreases fits.len() - at,
    {
        match fits[at] {
            Fit::Batchable => {
                let mut end = at + 1;
                while end < fits.len() && end - at < bound && can_join(&fits[end])
                    invariant
                        at < end <= fits.len(),
                        end - at <= bound,
                        forall|j: int| at <= j < end ==> #[trigger] joins(fits@[j]),
                    decreases fits.len() - end,
                {
                    end += 1;
                }
                if end - at >= 2 {
                    proof {
                        let lead = Step::Lead((end - at) as usize);
                        lemma_push(fits@, bound as int, steps@, lead, at as int);
                    }
                    steps.push(Step::Lead(end - at));
                    let mut k = at + 1;
                    while k < end
                        invariant
                            2 <= bound,
                            at < k <= end <= fits.len(),
                            end - at <= bound,
                            steps.len() == k,
                            steps@[at as int] == Step::Lead((end - at) as usize),
                            prefix_ok(fits@, bound as int, steps@, at as int),
                            forall|j: int| at <= j < end ==> #[trigger] joins(fits@[j]),
                            forall|j: int| at < j < k ==> #[trigger] steps@[j] is Member,
                            forall|j: int|
                                at < j < k ==> #[trigger] step_ok(fits@, bound as int, steps@, j),
                        decreases end - k,
                    {
                        proof {
                            let len = (end - at) as usize;
                            lemma_push_member(fits@, bound as int, steps@, at as int, len);
                        }
                        steps.push(Step::Member);
                        k += 1;
                    }
                    proof {
                        assert(step_ok(fits@, bound as int, steps@, at as int));
                    }
                } else {
                    let step = if at > 0 && can_join(&fits[at - 1]) {
                        Step::Alone(SerialReason::BoundReached)
                    } else if joining > 1 {
                        Step::Alone(SerialReason::NotConsecutive)
                    } else {
                        Step::Alone(SerialReason::SingleCall)
                    };
                    proof {
                        lemma_the_count_names_the_others(fits@, at as int);
                        lemma_push(fits@, bound as int, steps@, step, at as int);
                        assert(end == at + 1);
                        assert(!right(fits@, at as int));
                    }
                    steps.push(step);
                    proof {
                        assert(step_ok(fits@, bound as int, steps@, at as int));
                    }
                }
                at = end;
            }
            Fit::Gated => {
                proof {
                    let step = Step::Alone(SerialReason::Gated);
                    lemma_push(fits@, bound as int, steps@, step, at as int);
                }
                steps.push(Step::Alone(SerialReason::Gated));
                at += 1;
            }
            Fit::Durable => {
                proof {
                    let step = Step::Alone(SerialReason::DurablePeer);
                    lemma_push(fits@, bound as int, steps@, step, at as int);
                }
                steps.push(Step::Alone(SerialReason::DurablePeer));
                at += 1;
            }
            Fit::Other => {
                proof {
                    lemma_push(fits@, bound as int, steps@, Step::Skip, at as int);
                }
                steps.push(Step::Skip);
                at += 1;
            }
        }
    }
    steps
}

/// No delegation that can run falls back in silence. With a bound of 2 or
/// more, each call other than `Other` leads a batch, joins one, or names
/// why it ran alone.
pub proof fn theorem_no_fallback_is_silent(fits: Seq<Fit>, bound: int, steps: Seq<Step>, i: int)
    requires
        2 <= bound,
        0 <= i < fits.len(),
        prefix_ok(fits, bound, steps, fits.len() as int),
        !(fits[i] is Other),
    ensures
        !(steps[i] is Skip),
{
    assert(step_ok(fits, bound, steps, i));
}

/// A bound under 2 turns the fan-out off. No step names a reason.
pub proof fn theorem_an_off_fan_out_names_no_reason(
    fits: Seq<Fit>,
    bound: int,
    steps: Seq<Step>,
    i: int,
)
    requires
        bound < 2,
        0 <= i < fits.len(),
        prefix_ok(fits, bound, steps, fits.len() as int),
    ensures
        steps[i] is Skip,
{
    assert(step_ok(fits, bound, steps, i));
}

} // verus!
