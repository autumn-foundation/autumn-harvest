//! The plan and the acknowledgement rule of group commit (issue #2256).
//!
//! The engine accepts an entry to a batch with `accepts`, bounds a batch
//! with `share` and reports a result with `acknowledges`. These copies
//! prove each one for every input. With `share`, a leader takes at most
//! `max_batch` entries, and `width` leaders cover the queue.
//! The spec `plan` models `plan_batch` over any queue. It accepts each entry
//! with the same rule. The theorems show that a batch starts with the
//! leader, holds at most `max_batch` entries, keeps one tenure, and holds
//! one entry per execution. The last theorem shows that one decision's
//! result depends only on its own stage and on the batch outcome. Another
//! stage changes it only through a fence error, which ends the batch.

use vstd::prelude::*;

verus! {

/// The admission rule of `plan_batch`.
pub open spec fn spec_accepts(
    batch_len: int,
    max_batch: int,
    same_tenure: bool,
    execution_in_batch: bool,
) -> bool {
    batch_len < max_batch && same_tenure && !execution_in_batch
}

// mirror: autumn-harvest/src/group_commit.rs accepts
pub fn accepts(
    batch_len: usize,
    max_batch: usize,
    same_tenure: bool,
    execution_in_batch: bool,
) -> (r: bool)
    ensures
        r == spec_accepts(batch_len as int, max_batch as int, same_tenure, execution_in_batch),
{
    batch_len < max_batch && same_tenure && !execution_in_batch
}

// mirror: autumn-harvest/src/group_commit.rs acknowledges
pub fn acknowledges(stage_passed: bool, batch_committed: bool) -> (r: bool)
    ensures
        r == (stage_passed && batch_committed),
        r ==> batch_committed,
        r ==> stage_passed,
{
    stage_passed && batch_committed
}

/// The rounded-up share of `queued` over `width` slots fits in a `usize`
/// and covers the queue.
proof fn lemma_share_bounds(queued: int, width: int)
    requires
        0 <= queued <= usize::MAX,
        1 <= width,
    ensures
        queued % width != 0 ==> queued / width + 1 <= usize::MAX,
        (queued / width + if queued % width == 0 { 0int } else { 1int }) * width >= queued,
{
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(queued, width);
    vstd::arithmetic::div_mod::lemma_mod_pos_bound(queued, width);
    vstd::arithmetic::div_mod::lemma_div_pos_is_pos(queued, width);
    let q = queued / width;
    let m = queued % width;
    let up = if m == 0 {
        0int
    } else {
        1int
    };
    assert((q + up) * width >= queued) by (nonlinear_arith)
        requires
            queued == width * q + m,
            0 <= m < width,
            0 <= up <= 1,
            m != 0 ==> up == 1,
            q >= 0,
    ;
    if m != 0 {
        assert(q * 2 <= queued) by (nonlinear_arith)
            requires
                queued == width * q + m,
                width >= 2,
                q >= 0,
                m >= 0,
        ;
    }
}

// mirror: autumn-harvest/src/group_commit.rs share
pub fn share(queued: usize, width: usize, max_batch: usize) -> (r: usize)
    requires
        max_batch >= 1,
    ensures
        1 <= r <= max_batch,
        r < max_batch ==> r * (if width == 0 { 1 } else { width }) >= queued,
{
    let slots = if width == 0 {
        1
    } else {
        width
    };
    proof {
        lemma_share_bounds(queued as int, slots as int);
    }
    let fair = queued / slots + if queued % slots == 0 {
        0
    } else {
        1
    };
    proof {
        assert(fair * slots >= queued);
        assert(fair == 0 ==> queued == 0) by (nonlinear_arith)
            requires
                fair * slots >= queued,
                queued >= 0,
        ;
        assert(slots == if width == 0 {
            1
        } else {
            width
        });
    }
    if fair == 0 {
        1
    } else if fair < max_batch {
        fair
    } else {
        max_batch
    }
}

/// One queued decision: the epoch of its tenure and its execution.
pub struct Entry {
    pub tenure: int,
    pub execution: int,
}

/// Some entry of `batch` belongs to `execution`.
pub open spec fn has_execution(queue: Seq<Entry>, batch: Seq<int>, execution: int) -> bool {
    exists|k: int| 0 <= k < batch.len() && #[trigger] queue[batch[k]].execution == execution
}

/// The plan from index `i` on, with `batch` taken so far.
pub open spec fn plan_from(
    queue: Seq<Entry>,
    own: int,
    max_batch: int,
    i: int,
    batch: Seq<int>,
) -> Seq<int>
    decreases queue.len() - i,
{
    if i >= queue.len() || i < 0 {
        batch
    } else if i != own && spec_accepts(
        batch.len() as int,
        max_batch,
        queue[i].tenure == queue[own].tenure,
        has_execution(queue, batch, queue[i].execution),
    ) {
        plan_from(queue, own, max_batch, i + 1, batch.push(i))
    } else {
        plan_from(queue, own, max_batch, i + 1, batch)
    }
}

/// The batch of the leader at `own`. It is empty when `own` is out of range.
pub open spec fn plan(queue: Seq<Entry>, own: int, max_batch: int) -> Seq<int> {
    if 0 <= own < queue.len() && max_batch >= 1 {
        plan_from(queue, own, max_batch, 0, seq![own])
    } else {
        seq![]
    }
}

/// The rules that each batch keeps.
pub open spec fn well_formed(queue: Seq<Entry>, own: int, max_batch: int, batch: Seq<int>) -> bool {
    &&& 0 <= own < queue.len()
    &&& 1 <= batch.len() <= max_batch
    &&& batch[0] == own
    &&& forall|k: int| 0 <= k < batch.len() ==> 0 <= #[trigger] batch[k] < queue.len()
    &&& forall|k: int|
        0 <= k < batch.len() ==> #[trigger] queue[batch[k]].tenure == queue[own].tenure
    &&& forall|j: int, k: int|
        0 <= j < k < batch.len() ==> #[trigger] queue[batch[j]].execution
            != #[trigger] queue[batch[k]].execution
}

/// Each step of the plan keeps the rules.
proof fn lemma_plan_from_keeps_the_rules(
    queue: Seq<Entry>,
    own: int,
    max_batch: int,
    i: int,
    batch: Seq<int>,
)
    requires
        0 <= i,
        well_formed(queue, own, max_batch, batch),
    ensures
        well_formed(queue, own, max_batch, plan_from(queue, own, max_batch, i, batch)),
    decreases queue.len() - i,
{
    if i < queue.len() {
        let same = queue[i].tenure == queue[own].tenure;
        let shared = has_execution(queue, batch, queue[i].execution);
        if i != own && spec_accepts(batch.len() as int, max_batch, same, shared) {
            let next = batch.push(i);
            assert forall|k: int| 0 <= k < next.len() implies 0 <= #[trigger] next[k]
                < queue.len() by {
                if k < batch.len() {
                    assert(next[k] == batch[k]);
                }
            }
            assert forall|k: int| 0 <= k < next.len() implies #[trigger] queue[next[k]].tenure
                == queue[own].tenure by {
                if k < batch.len() {
                    assert(next[k] == batch[k]);
                }
            }
            assert forall|j: int, k: int| 0 <= j < k < next.len() implies #[trigger] queue[
                next[j]].execution != #[trigger] queue[next[k]].execution by {
                assert(next[j] == batch[j]);
                if k < batch.len() {
                    assert(next[k] == batch[k]);
                } else {
                    assert(next[k] == i);
                    if queue[batch[j]].execution == queue[i].execution {
                        assert(has_execution(queue, batch, queue[i].execution));
                    }
                }
            }
            lemma_plan_from_keeps_the_rules(queue, own, max_batch, i + 1, next);
        } else {
            lemma_plan_from_keeps_the_rules(queue, own, max_batch, i + 1, batch);
        }
    }
}

/// A batch starts with the leader, holds at most `max_batch` entries of
/// the queue, keeps the leader's tenure, and holds one entry per execution.
pub proof fn theorem_a_plan_keeps_every_rule(queue: Seq<Entry>, own: int, max_batch: int)
    requires
        0 <= own < queue.len(),
        max_batch >= 1,
    ensures
        well_formed(queue, own, max_batch, plan(queue, own, max_batch)),
{
    let start = seq![own];
    assert(start[0] == own);
    lemma_plan_from_keeps_the_rules(queue, own, max_batch, 0, start);
}

/// With no leader, the plan is empty.
pub proof fn theorem_no_leader_plans_nothing(queue: Seq<Entry>, own: int, max_batch: int)
    requires
        own < 0 || own >= queue.len() || max_batch < 1,
    ensures
        plan(queue, own, max_batch).len() == 0,
{
}

/// Whether a batch commits. A fence error from any stage, or a failed
/// `COMMIT`, ends the whole batch.
pub open spec fn commits(fence_errors: Seq<bool>, commit_ok: bool) -> bool {
    commit_ok && forall|k: int| 0 <= k < fence_errors.len() ==> !#[trigger] fence_errors[k]
}

/// The result of decision `i` of a batch, by `acknowledges`. `stages[i]`
/// holds whether its stage passed. `fence_errors[k]` holds whether stage
/// `k` returned a fence error.
pub open spec fn acked(stages: Seq<bool>, fence_errors: Seq<bool>, commit_ok: bool, i: int) -> bool {
    stages[i] && commits(fence_errors, commit_ok)
}

/// A savepoint isolates each decision. An ordinary failure of stage `j`
/// cannot change the result of decision `i`. A fence error of any stage,
/// or a failed `COMMIT`, fails every decision. A failed stage is never
/// acknowledged.
pub proof fn theorem_a_result_depends_on_its_own_stage_and_the_batch(
    stages: Seq<bool>,
    fence_errors: Seq<bool>,
    commit_ok: bool,
    i: int,
    j: int,
    other: bool,
)
    requires
        stages.len() == fence_errors.len(),
        0 <= i < stages.len(),
        0 <= j < stages.len(),
        i != j,
    ensures
        acked(stages.update(j, other), fence_errors, commit_ok, i) == acked(
            stages,
            fence_errors,
            commit_ok,
            i,
        ),
        fence_errors[j] ==> !acked(stages, fence_errors, commit_ok, i),
        !commit_ok ==> !acked(stages, fence_errors, commit_ok, i),
        !stages[i] ==> !acked(stages, fence_errors, commit_ok, i),
{
}

} // verus!
