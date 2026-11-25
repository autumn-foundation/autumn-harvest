//! Outage blast radius for DAG definitions.
//!
//! A circuit breaker trips per activity, not per task. When a downstream
//! service goes hard-down, every task that calls its activity fails at once.
//! One activity name can back many nodes of a DAG. This module asks what that
//! outage costs the run.
//!
//! For each activity, the analysis fails every task of that activity. It then
//! walks the execution levels and applies each trigger rule, as the worker
//! does. It compares the result with a baseline run in which every task
//! succeeds. A task that succeeds in the baseline but not in the outage is
//! lost. A task that succeeds only in the outage is activated. An activated
//! task is often a fallback that waits on `OneFailed` or `AllFailed`.
//!
//! A signal gate dispatches no activity, so the analysis never fails one.
//! A signal name is not an activity, so it gets no outage entry either.
//!
//! A mapped node follows the engine rules. A mapped upstream that did not
//! succeed leaves no array, so the node fails before it dispatches. With
//! `CollectAll`, an outage of the mapped activity fails each item, not the
//! node. With `FailFast`, the outage fails the node. The analysis assumes that
//! each mapped upstream yields a non-empty array. An empty array dispatches
//! nothing, so the node succeeds even during an outage.
//!
//! A `condition` predicate reads upstream outputs, and a static analysis has
//! none. The analysis therefore assumes that each condition passes. Each such
//! task is listed in [`DagBlastRadius::conditional`]. A result that names one
//! of them holds only when its condition passes.
//!
//! The ranking tells an operator which dependency to guard first. An activity
//! with a large blast radius is a good candidate for a fallback branch or a
//! `Defer` breaker.
//!
//! The module is pure. It sends no SQL and reads no clock.

use crate::dag::{DagDefinition, DagTask};
use crate::policy::{MapFailurePolicy, TaskStatus};

/// The effect of one activity outage on a DAG run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityOutage {
    /// Name of the activity that is down.
    pub activity: String,
    /// Tasks that pass their trigger rule and fail, by topological index.
    pub failed: Vec<usize>,
    /// Tasks that succeed in the baseline but not in the outage.
    pub lost: Vec<usize>,
    /// Tasks that succeed only in the outage.
    pub activated: Vec<usize>,
}

impl ActivityOutage {
    /// Number of tasks that the outage costs the run.
    #[must_use]
    pub const fn blast_radius(&self) -> usize {
        self.lost.len()
    }
}

/// Blast radius of every activity of a DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagBlastRadius {
    /// Status of each task when every task succeeds, by topological index.
    pub baseline: Vec<TaskStatus>,
    /// Tasks with a `condition`, which the analysis assumes to pass.
    pub conditional: Vec<usize>,
    /// One entry per activity, largest blast radius first.
    ///
    /// Ties sort by activity name, so the order is deterministic.
    pub outages: Vec<ActivityOutage>,
}

impl DagBlastRadius {
    /// Analyze an outage of each activity of `dag`.
    #[must_use]
    pub fn analyze(dag: &DagDefinition) -> Self {
        let baseline = propagate(dag, None);
        let mut activities: Vec<&str> = dag
            .tasks()
            .iter()
            .filter(|t| t.signal.is_none())
            .map(|t| t.activity_name.as_str())
            .collect();
        activities.sort_unstable();
        activities.dedup();

        let mut outages: Vec<ActivityOutage> = activities
            .into_iter()
            .map(|activity| {
                let states = propagate(dag, Some(activity));
                let pick = |keep: fn(TaskStatus, TaskStatus) -> bool| -> Vec<usize> {
                    (0..states.len())
                        .filter(|&i| keep(baseline[i], states[i]))
                        .collect()
                };
                ActivityOutage {
                    activity: activity.to_string(),
                    failed: pick(|_, now| now == TaskStatus::Failed),
                    lost: pick(|was, now| {
                        was == TaskStatus::Succeeded && now != TaskStatus::Succeeded
                    }),
                    activated: pick(|was, now| {
                        was != TaskStatus::Succeeded && now == TaskStatus::Succeeded
                    }),
                }
            })
            .collect();
        // The sort is stable, so ties keep the name order from above.
        outages.sort_by_key(|o| std::cmp::Reverse(o.blast_radius()));
        let conditional = dag
            .tasks()
            .iter()
            .enumerate()
            .filter(|(_, t)| t.condition.is_some())
            .map(|(i, _)| i)
            .collect();
        Self {
            baseline,
            conditional,
            outages,
        }
    }

    /// The outage entry of `activity`, if the DAG calls it.
    #[must_use]
    pub fn outage(&self, activity: &str) -> Option<&ActivityOutage> {
        self.outages.iter().find(|o| o.activity == activity)
    }
}

/// Final status of each task when the `down` activity fails.
///
/// A task that runs succeeds unless it calls the `down` activity. A signal
/// gate calls no activity, so it always succeeds. A mapped node fails when
/// its mapped upstream did not succeed. A `CollectAll` mapped node survives
/// an outage of its own activity.
fn propagate(dag: &DagDefinition, down: Option<&str>) -> Vec<TaskStatus> {
    let tasks = dag.tasks();
    let mut states = vec![TaskStatus::Skipped; tasks.len()];
    for &i in dag.execution_levels().iter().flatten() {
        let task = &tasks[i];
        let upstreams = task.upstreams.iter().map(|&up| &states[up]);
        states[i] = if !task.trigger_rule.should_run(upstreams) {
            TaskStatus::Skipped
        } else if fails(task, &states, down) {
            TaskStatus::Failed
        } else {
            TaskStatus::Succeeded
        };
    }
    states
}

/// Whether `task` fails once its trigger rule passes.
fn fails(task: &DagTask, states: &[TaskStatus], down: Option<&str>) -> bool {
    if let Some(up) = task.map_upstream {
        if states[up] != TaskStatus::Succeeded {
            return true;
        }
        if task.map_failure_policy == MapFailurePolicy::CollectAll {
            return false;
        }
    }
    task.signal.is_none() && down == Some(task.activity_name.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::policy::TriggerRule;

    fn extract() {}
    fn transform() {}
    fn load() {}
    fn notify() {}
    fn fallback() {}

    #[test]
    fn a_linear_chain_loses_everything_downstream() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let t = b.activity(transform).upstream(&e);
        let l = b.activity(load).upstream(&t);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert_eq!(report.baseline, vec![TaskStatus::Succeeded; 3]);
        let head = &report.outages[0];
        assert_eq!(head.activity, "extract");
        assert_eq!(head.failed, vec![e.index()]);
        assert_eq!(head.lost, vec![e.index(), t.index(), l.index()]);
        assert_eq!(head.activated, Vec::<usize>::new());
        assert_eq!(report.outage("load").unwrap().blast_radius(), 1);
    }

    #[test]
    fn outages_rank_by_blast_radius() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let t = b.activity(transform).upstream(&e);
        let _ = b.activity(load).upstream(&t);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let order: Vec<_> = report.outages.iter().map(|o| o.activity.as_str()).collect();
        assert_eq!(order, ["extract", "transform", "load"]);
    }

    #[test]
    fn ties_sort_by_activity_name() {
        let mut b = DagBuilder::new();
        let _ = b.activity(transform);
        let _ = b.activity(extract);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let order: Vec<_> = report.outages.iter().map(|o| o.activity.as_str()).collect();
        assert_eq!(order, ["extract", "transform"]);
    }

    #[test]
    fn one_activity_on_many_nodes_fails_them_all() {
        let mut b = DagBuilder::new();
        let n1 = b.activity(notify);
        let e = b.activity(extract).upstream(&n1);
        let n2 = b.activity(notify).upstream(&e);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("notify").unwrap();
        // The second node never runs, because its upstream is skipped.
        assert_eq!(outage.failed, vec![n1.index()]);
        assert_eq!(outage.lost, vec![n1.index(), e.index(), n2.index()]);
    }

    #[test]
    fn a_fallback_branch_is_activated() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let f = b
            .activity(fallback)
            .upstream(&e)
            .trigger_rule(TriggerRule::OneFailed);
        let l = b.activity(load).upstream(&e);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert_eq!(report.baseline[f.index()], TaskStatus::Skipped);
        let outage = report.outage("extract").unwrap();
        assert_eq!(outage.activated, vec![f.index()]);
        assert_eq!(outage.lost, vec![e.index(), l.index()]);
    }

    #[test]
    fn an_all_done_task_survives_the_outage() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let n = b
            .activity(notify)
            .upstream(&e)
            .trigger_rule(TriggerRule::AllDone);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("extract").unwrap();
        assert_eq!(outage.lost, vec![e.index()]);
        assert!(!outage.lost.contains(&n.index()));
    }

    #[test]
    fn one_success_survives_one_failed_branch() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let t = b.activity(transform);
        let l = b
            .activity(load)
            .upstream(&e)
            .upstream(&t)
            .trigger_rule(TriggerRule::OneSuccess);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("extract").unwrap();
        assert_eq!(outage.lost, vec![e.index()]);
        assert!(!outage.lost.contains(&l.index()));
    }

    #[test]
    fn a_manual_node_is_never_lost() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let _ = b
            .activity(load)
            .upstream(&e)
            .trigger_rule(TriggerRule::Manual);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert_eq!(report.outage("load").unwrap().blast_radius(), 0);
        assert_eq!(report.outage("load").unwrap().failed, Vec::<usize>::new());
    }

    #[test]
    fn a_signal_gate_is_not_an_activity() {
        let mut b = DagBuilder::new();
        let g = b.signal_gate("extract");
        let e = b.activity(extract).upstream(&g);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert_eq!(report.outages.len(), 1);
        let outage = report.outage("extract").unwrap();
        assert_eq!(outage.failed, vec![e.index()]);
        assert_eq!(outage.lost, vec![e.index()]);
    }

    #[test]
    fn a_conditional_task_is_listed_as_an_assumption() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let f = b
            .activity(fallback)
            .upstream(&e)
            .trigger_rule(TriggerRule::OneFailed)
            .condition(|_| false);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert_eq!(report.conditional, vec![f.index()]);
        assert_eq!(report.outage("extract").unwrap().activated, vec![f.index()]);
    }

    #[test]
    fn a_collect_all_mapped_node_survives_its_outage() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let m = b
            .map_activity(transform)
            .over(&e)
            .map_failure_policy(MapFailurePolicy::CollectAll);
        let l = b.activity(load).upstream(&m);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("transform").unwrap();
        assert_eq!(outage.failed, Vec::<usize>::new());
        assert_eq!(outage.lost, Vec::<usize>::new());
        assert_eq!(report.baseline[l.index()], TaskStatus::Succeeded);
    }

    #[test]
    fn a_fail_fast_mapped_node_fails_in_its_outage() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let m = b.map_activity(transform).over(&e);
        let l = b.activity(load).upstream(&m);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("transform").unwrap();
        assert_eq!(outage.failed, vec![m.index()]);
        assert_eq!(outage.lost, vec![m.index(), l.index()]);
    }

    #[test]
    fn a_mapped_node_fails_without_its_array() {
        let mut b = DagBuilder::new();
        let e = b.activity(extract);
        let m = b
            .map_activity(transform)
            .over(&e)
            .trigger_rule(TriggerRule::AllDone);
        let l = b.activity(load).upstream(&m);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        let outage = report.outage("extract").unwrap();
        assert_eq!(outage.failed, vec![e.index(), m.index()]);
        assert_eq!(outage.lost, vec![e.index(), m.index(), l.index()]);
    }

    #[test]
    fn an_unknown_activity_has_no_outage() {
        let mut b = DagBuilder::new();
        let _ = b.activity(extract);
        let report = DagBlastRadius::analyze(&b.build().unwrap());

        assert!(report.outage("missing").is_none());
    }
}
