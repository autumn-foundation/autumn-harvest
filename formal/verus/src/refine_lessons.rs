//! The lesson library of a kind stays in its bound, and only an adoption
//! writes a lesson.
//!
//! The copies below are `evictions` and `writes_lesson` of
//! `refine/lessons.rs` in the agent crate. `evictions` gives the number of
//! oldest entries that a write removes. `writes_lesson` tells whether a
//! refine run that ended with a status writes a lesson.

use vstd::prelude::*;

verus! {

/// How a refine run ended.
pub enum RefineStatus {
    AutoAdopted,
    AwaitingApproval,
    Unchanged,
    TimedOut,
    Chosen,
    BudgetExhausted,
    Adopted,
    Rejected,
    Superseded,
}

/// `evictions` as a spec.
pub open spec fn spec_evictions(len: int, max: int) -> int {
    if len > max {
        len - max
    } else {
        0
    }
}

// mirror: autumn-harvest-agent/src/refine/lessons.rs evictions
pub fn evictions(len: usize, max: usize) -> (r: usize)
    ensures
        r == spec_evictions(len as int, max as int),
        r <= len,
        len - r <= max,
        len <= max ==> r == 0,
        len > max ==> len - r == max,
{
    if len > max {
        len - max
    } else {
        0
    }
}

/// A write keeps the newest entry, so a new lesson never evicts itself.
pub proof fn theorem_the_newest_entry_stays(len: int, max: int)
    requires
        1 <= max,
        1 <= len,
    ensures
        spec_evictions(len, max) < len,
{
}

/// A raised bound removes no entry that a lower bound kept.
pub proof fn theorem_a_raised_bound_evicts_no_more(len: int, low: int, high: int)
    requires
        0 <= low <= high,
        0 <= len,
    ensures
        spec_evictions(len, high) <= spec_evictions(len, low),
{
}

/// `writes_lesson` as a spec.
pub open spec fn spec_writes_lesson(status: RefineStatus) -> bool {
    status is Adopted || status is AutoAdopted
}

// mirror: autumn-harvest-agent/src/refine/lessons.rs writes_lesson
pub fn writes_lesson(status: RefineStatus) -> (r: bool)
    ensures
        r == spec_writes_lesson(status),
        r <==> (status is Adopted || status is AutoAdopted),
{
    match status {
        RefineStatus::AutoAdopted => true,
        RefineStatus::AwaitingApproval => false,
        RefineStatus::Unchanged => false,
        RefineStatus::TimedOut => false,
        RefineStatus::Chosen => false,
        RefineStatus::BudgetExhausted => false,
        RefineStatus::Adopted => true,
        RefineStatus::Rejected => false,
        RefineStatus::Superseded => false,
    }
}

/// A rejected, timed-out or superseded run writes no lesson.
pub proof fn theorem_an_end_with_no_adoption_writes_no_lesson()
    ensures
        !spec_writes_lesson(RefineStatus::Rejected),
        !spec_writes_lesson(RefineStatus::TimedOut),
        !spec_writes_lesson(RefineStatus::Superseded),
        !spec_writes_lesson(RefineStatus::Unchanged),
        !spec_writes_lesson(RefineStatus::BudgetExhausted),
        !spec_writes_lesson(RefineStatus::Chosen),
        !spec_writes_lesson(RefineStatus::AwaitingApproval),
{
}

} // verus!
