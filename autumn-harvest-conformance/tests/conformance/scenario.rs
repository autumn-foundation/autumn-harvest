//! A scenario: one workflow, its input, and a script of external inputs.

use autumn_harvest::info::{ActivityInfo, UpdateHandlerInfo, WorkflowInfo};
use serde_json::Value;

use crate::commands::CommandKind;

/// One external input, or one wait, in a scenario script.
#[derive(Debug, Clone)]
pub enum Step {
    /// Send these signals, in order, one transaction each.
    ///
    /// Postgres can run a decision cycle between two of them. A scenario must
    /// give the same history for each split.
    Signals(Vec<(&'static str, Value)>),
    /// Wait until the root history holds `count` events of type `event`.
    ///
    /// The event commits with the cycle that wrote it. A later step therefore
    /// reaches the run only after that cycle.
    AwaitEvent { event: &'static str, count: usize },
    /// Run a declarative update and record its result.
    Update { name: &'static str, input: Value },
    /// Request a cancel with this reason.
    Cancel(&'static str),
    /// Terminate with this reason.
    Terminate(&'static str),
}

/// What the suite expects from the `SQLite` run.
#[derive(Debug, Clone, Copy)]
pub enum Expect {
    /// The same outcome and a replay-equivalent history as on Postgres.
    Same,
    /// A loud `UnsupportedFeature` failure that names this feature.
    ///
    /// The suite does not run such a scenario on Postgres. `SQLite` makes no
    /// parity claim for it.
    Rejected { feature: &'static str },
}

/// One entry of the corpus.
#[derive(Clone)]
pub struct Scenario {
    /// A unique name. A failure message starts with it.
    pub name: &'static str,
    /// The root workflow type.
    pub workflow: &'static str,
    /// The root workflow input.
    pub input: Value,
    /// Every workflow type that the run starts, the root included.
    pub workflows: fn() -> Vec<WorkflowInfo>,
    /// Every activity that the run schedules.
    pub activities: fn() -> Vec<ActivityInfo>,
    /// Every declarative update handler.
    pub updates: fn() -> Vec<UpdateHandlerInfo>,
    /// Signals sent after the start and before the first decision cycle.
    pub initial_signals: Vec<(&'static str, Value)>,
    /// The script. The driver runs it in order, then waits for the end.
    pub steps: Vec<Step>,
    /// The commands that this scenario makes the workflow emit.
    pub covers: &'static [CommandKind],
    /// The expected `SQLite` result.
    pub expect: Expect,
}

impl Scenario {
    /// A scenario with no activities, updates, signals or steps.
    pub fn new(
        name: &'static str,
        workflow: &'static str,
        workflows: fn() -> Vec<WorkflowInfo>,
        covers: &'static [CommandKind],
    ) -> Self {
        Self {
            name,
            workflow,
            input: Value::from(0),
            workflows,
            activities: Vec::new,
            updates: Vec::new,
            initial_signals: Vec::new(),
            steps: Vec::new(),
            covers,
            expect: Expect::Same,
        }
    }

    pub fn input(mut self, input: Value) -> Self {
        self.input = input;
        self
    }

    pub fn activities(mut self, activities: fn() -> Vec<ActivityInfo>) -> Self {
        self.activities = activities;
        self
    }

    pub fn updates(mut self, updates: fn() -> Vec<UpdateHandlerInfo>) -> Self {
        self.updates = updates;
        self
    }

    pub fn initial_signal(mut self, name: &'static str, payload: Value) -> Self {
        self.initial_signals.push((name, payload));
        self
    }

    pub fn step(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    pub const fn rejected(mut self, feature: &'static str) -> Self {
        self.expect = Expect::Rejected { feature };
        self
    }
}
