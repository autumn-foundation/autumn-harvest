use vstd::prelude::*;

verus! {
    /// Minimal model for the DAG scheduler activation claim.
    pub enum RunState {
        Queued,
        Running,
        Terminal,
    }

    /// Mirrors the SQL claim predicate:
    /// `UPDATE harvest_dag_runs SET state = 'RUNNING' WHERE id IN (...) AND state = 'QUEUED'`.
    pub open spec fn claim_result(before: RunState, selected_by_scheduler: bool) -> RunState {
        if selected_by_scheduler && before == RunState::Queued {
            RunState::Running
        } else {
            before
        }
    }

    pub open spec fn was_claimed(before: RunState, selected_by_scheduler: bool) -> bool {
        selected_by_scheduler && before == RunState::Queued
    }

    proof fn stale_running_row_is_not_claimed(selected_by_scheduler: bool)
        ensures
            claim_result(RunState::Running, selected_by_scheduler) == RunState::Running,
            !was_claimed(RunState::Running, selected_by_scheduler),
    {
    }

    proof fn only_selected_queued_rows_are_claimed(
        before: RunState,
        selected_by_scheduler: bool,
    )
        ensures
            was_claimed(before, selected_by_scheduler)
                ==> selected_by_scheduler && before == RunState::Queued,
            selected_by_scheduler && before == RunState::Queued
                ==> claim_result(before, selected_by_scheduler) == RunState::Running,
            !was_claimed(before, selected_by_scheduler)
                ==> claim_result(before, selected_by_scheduler) == before,
    {
    }
}

fn main() {}
