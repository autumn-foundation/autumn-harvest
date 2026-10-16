//! Seeded interleaving simulation of user workflow code (issue #2033).

#![allow(clippy::todo, missing_docs, unused_variables)]

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::WorkflowTestEnv;
use crate::dst::{SeedPlan, SplitMix64};
use crate::event::WorkflowEvent;
use crate::info::WorkflowHandlerFn;
use crate::types::TimerId;

pub const SEED_VAR: &str = "HARVEST_SIM_SEED";
pub const SEEDS_VAR: &str = "HARVEST_SIM_SEEDS";
pub const SEED_BASE_VAR: &str = "HARVEST_SIM_SEED_BASE";
pub const DEFAULT_SEEDS: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SimStatus {
    Completed,
    Failed,
    ContinuedAsNew,
    Cancelled,
    Blocked,
    TaskFailed,
    HarnessError,
}

pub struct SimOutcome;

impl SimOutcome {
    pub fn result(&self) -> &Result<Value, String> {
        todo!()
    }
    pub fn status(&self) -> SimStatus {
        todo!()
    }
    pub fn events(&self) -> &[WorkflowEvent] {
        todo!()
    }
    pub fn tasks(&self) -> usize {
        todo!()
    }
    pub fn task_of_events(&self, positions: &[usize]) -> BTreeSet<usize> {
        todo!()
    }
    pub fn elapsed(&self) -> chrono::Duration {
        todo!()
    }
    pub fn trace(&self) -> &[String] {
        todo!()
    }
    pub fn faults(&self) -> &[String] {
        todo!()
    }
    pub fn crashed_at(&self) -> Option<usize> {
        todo!()
    }
    pub fn same_outcome(&self, other: &Self) -> Result<(), String> {
        todo!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct SweepReport {
    pub seeds: u64,
    pub replayed: u64,
    pub reached: BTreeSet<String>,
    pub statuses: BTreeMap<SimStatus, u64>,
}

#[derive(Debug, Clone)]
pub struct SimFailure {
    pub seed: u64,
    pub reason: String,
    pub repro: String,
    pub trace_tail: String,
}

impl std::fmt::Display for SimFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct FaultPlan;

impl FaultPlan {
    pub fn new() -> Self {
        Self
    }
    pub fn fail(self, name: &str, ordinal: u32, error: &str) -> Self {
        todo!()
    }
    pub fn transient(self, name: &str, ordinal: u32, failures: u32) -> Self {
        todo!()
    }
}

pub struct MockContext;

impl MockContext {
    pub fn rng(&mut self) -> &mut SplitMix64 {
        todo!()
    }
    pub fn now(&self) -> DateTime<Utc> {
        todo!()
    }
}

pub struct Simulation<'env> {
    env: &'env WorkflowTestEnv,
}

impl Simulation<'_> {
    pub fn signal(self, name: &str, payload: Value) -> Self {
        todo!()
    }
    pub fn cancel(self, reason: &str) -> Self {
        todo!()
    }
    pub fn fault_percent(self, percent: u8) -> Self {
        todo!()
    }
    pub fn faults(self, plan: FaultPlan) -> Self {
        todo!()
    }
    pub fn seeds(self, plan: SeedPlan) -> Self {
        todo!()
    }
    pub fn test_name(self, name: &str) -> Self {
        todo!()
    }
    pub fn replay_with(self, handler: WorkflowHandlerFn) -> Self {
        todo!()
    }
    pub async fn run(&self, seed: u64, handler: WorkflowHandlerFn, input: Value) -> SimOutcome {
        todo!()
    }
    pub async fn run_with_crash(
        &self,
        seed: u64,
        task: usize,
        handler: WorkflowHandlerFn,
        input: Value,
    ) -> SimOutcome {
        todo!()
    }
    pub async fn sweep(
        &self,
        handler: WorkflowHandlerFn,
        input: Value,
        check: impl Fn(&SimOutcome),
    ) -> Result<SweepReport, Box<SimFailure>> {
        todo!()
    }
}

impl WorkflowTestEnv {
    pub fn simulation(&self) -> Simulation<'_> {
        Simulation { env: self }
    }
    #[must_use]
    pub fn mock_activity_seeded<F>(self, name: impl Into<String>, mock: F) -> Self
    where
        F: Fn(Value, &mut MockContext) -> Result<Value, String> + Send + Sync + 'static,
    {
        todo!()
    }
}

pub fn wake_order(
    clock: u64,
    timers: &[(TimerId, u64)],
    signals: &[(String, Value, u64)],
) -> Vec<WorkflowEvent> {
    todo!()
}

pub fn check_legal(events: &[WorkflowEvent]) -> Result<(), String> {
    todo!()
}

pub fn repro_command(seed: u64, test_name: Option<&str>) -> String {
    todo!()
}

#[doc(hidden)]
pub fn record_always(cond: bool, label: &str) -> bool {
    todo!()
}

#[doc(hidden)]
pub fn record_sometimes(cond: bool, label: &str) -> bool {
    todo!()
}

#[macro_export]
macro_rules! always {
    ($cond:expr, $label:expr $(,)?) => {
        $crate::testing::sim::record_always($cond, $label)
    };
}

#[macro_export]
macro_rules! sometimes {
    ($cond:expr, $label:expr $(,)?) => {
        $crate::testing::sim::record_sometimes($cond, $label)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ActivityExecId;
    use serde_json::json;

    fn timer(id: &str) -> TimerId {
        TimerId::new(id)
    }

    fn names(events: &[WorkflowEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                WorkflowEvent::TimerFired { timer_id } => format!("fire {}", timer_id.as_str()),
                WorkflowEvent::SignalReceived { signal_name, .. } => format!("signal {signal_name}"),
                other => other.type_name().to_string(),
            })
            .collect()
    }

    #[test]
    fn a_signal_received_before_a_deadline_goes_first() {
        let order = wake_order(
            60,
            &[(timer("t"), 60)],
            &[("go".to_string(), json!(1), 59)],
        );
        assert_eq!(names(&order), ["signal go", "fire t"]);
    }

    #[test]
    fn a_tie_goes_to_the_timer() {
        let order = wake_order(
            60,
            &[(timer("t"), 60)],
            &[("go".to_string(), json!(1), 60)],
        );
        assert_eq!(names(&order), ["fire t", "signal go"]);
    }

    #[test]
    fn due_timers_fire_by_deadline_then_id_and_later_ones_wait() {
        let order = wake_order(
            30,
            &[(timer("b"), 30), (timer("late"), 31), (timer("a"), 30), (timer("z"), 5)],
            &[],
        );
        assert_eq!(names(&order), ["fire z", "fire a", "fire b"]);
    }

    #[test]
    fn signals_keep_their_arrival_order() {
        let order = wake_order(
            0,
            &[],
            &[
                ("first".to_string(), json!(1), 0),
                ("second".to_string(), json!(2), 0),
            ],
        );
        assert_eq!(names(&order), ["signal first", "signal second"]);
    }

    fn started() -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn scheduled(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "a".into(),
            input: Value::Null,
            queue: "q".into(),
        }
    }

    fn completed(id: ActivityExecId) -> WorkflowEvent {
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: Value::Null,
        }
    }

    #[test]
    fn a_result_before_its_schedule_is_illegal() {
        let id = ActivityExecId::new();
        assert!(check_legal(&[started(), scheduled(id), completed(id)]).is_ok());
        let error = check_legal(&[started(), completed(id), scheduled(id)]).unwrap_err();
        assert!(error.contains("before it was scheduled"), "{error}");
    }

    #[test]
    fn a_second_terminal_is_illegal() {
        let id = ActivityExecId::new();
        let error =
            check_legal(&[started(), scheduled(id), completed(id), completed(id)]).unwrap_err();
        assert!(error.contains("second terminal"), "{error}");
    }

    #[test]
    fn timers_must_fire_in_deadline_order() {
        let start = |id: &str, secs| WorkflowEvent::TimerStarted {
            timer_id: timer(id),
            duration_secs: secs,
        };
        let fire = |id: &str| WorkflowEvent::TimerFired { timer_id: timer(id) };
        let legal = [started(), start("a", 10), start("b", 20), fire("a"), fire("b")];
        assert!(check_legal(&legal).is_ok());
        let early = [started(), start("a", 10), start("b", 20), fire("b"), fire("a")];
        let error = check_legal(&early).unwrap_err();
        assert!(error.contains("before its deadline"), "{error}");
        let unstarted = [started(), fire("a")];
        assert!(check_legal(&unstarted).is_err());
    }

    #[test]
    fn nothing_follows_a_cancel() {
        let id = ActivityExecId::new();
        let cancel = WorkflowEvent::WorkflowCancelled {
            reason: "stop".into(),
        };
        assert!(check_legal(&[started(), scheduled(id), cancel.clone()]).is_ok());
        let error = check_legal(&[started(), scheduled(id), cancel, completed(id)]).unwrap_err();
        assert!(error.contains("after the cancel"), "{error}");
    }

    #[test]
    fn the_repro_command_is_one_line_and_names_the_seed_and_test() {
        let command = repro_command(42, Some("tests::race"));
        assert!(command.starts_with("HARVEST_SIM_SEED=42 cargo test"), "{command}");
        assert!(command.contains("tests::race -- --exact --nocapture"), "{command}");
        assert!(!command.contains('\n'), "{command}");
        let bare = repro_command(7, None);
        assert!(bare.starts_with("HARVEST_SIM_SEED=7 cargo test"), "{bare}");
    }

    #[test]
    fn the_macros_do_nothing_outside_a_run() {
        assert!(!crate::always!(false, "outside"));
        assert!(crate::sometimes!(true, "outside"));
    }
}
