//! Cross-backend conformance suite (issue #2030).
//!
//! Each test runs one corpus scenario on Postgres and on `SQLite`. It then
//! asserts the same outcome and a replay-equivalent history. A scenario that
//! `SQLite` rejects must fail there loudly, by name. `README.md` has the rules.

mod commands;
mod compare;
mod corpus;
mod coverage;
mod pg;
mod runner;
mod scenario;
mod sqlite;

/// Make one test per scenario, and the list that the coverage guard reads.
///
/// A scenario in `defects` reproduces a known Postgres defect. Its test is
/// `#[ignore]`d with the reason, and the coverage guard does not count it.
macro_rules! corpus {
    (
        scenarios { $($name:ident),* $(,)? }
        defects { $($defect:ident => $reason:literal),* $(,)? }
    ) => {
        /// Every scenario that CI runs, with its constructor name.
        pub fn scenarios() -> Vec<(&'static str, scenario::Scenario)> {
            vec![$((stringify!($name), corpus::$name())),*]
        }

        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                runner::check(corpus::$name()).await;
            }
        )*

        $(
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = $reason]
            async fn $defect() {
                runner::check(corpus::$defect()).await;
            }
        )*
    };
}

corpus! {
    scenarios {
        activity_completes,
        activity_retries_then_succeeds,
        activity_exhausts_retries,
        activity_fails_non_retryable,
        workflow_fails,
        workflow_fails_typed,
        local_activity,
        signal_while_activity_retries,
        signal_wait,
        signal_before_first_cycle,
        signal_drain,
        signal_try_receive,
        signal_handler,
        signal_beats_deadline,
        deadline_beats_signal,
        signal_matching,
        cancellable_timers,
        race_activity_wins,
        side_effects_and_markers,
        bookkeeping,
        mutex,
        update,
        child_workflow,
        detached_child_signal_and_await,
        detached_child_cancel,
        continue_as_new,
        cancel_parked_run,
        terminate_parked_run,
        fan_out_result_writer_needs_a_store,
        rejects_external_activity,
        rejects_cross_type_continue_as_new,
        rejects_worker_session,
        rejects_acquire_beside_timer,
        rejects_local_activity_beside_timer,
        rejects_unawaited_activity_in_last_cycle,
    }
    defects {
        same_cycle_signal_to_new_child =>
            "Postgres can deliver a signal twice to a child spawned in the same cycle",
        same_cycle_cancel_of_new_child =>
            "Postgres can record a cancel twice for a child spawned in the same cycle",
    }
}
