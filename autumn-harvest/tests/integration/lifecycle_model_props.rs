#![cfg(feature = "db")]
#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
//! Stateful model-based property test of the workflow lifecycle (issue #1829).
//!
//! The test follows the `ShardStore` approach (SOSP'21). Proptest generates a
//! random sequence of client operations: start, claim, heartbeat, park,
//! complete, signal, cancel, worker death, worker revival and orphan
//! reclaim. Each operation runs against a real Postgres and against
//! [`Model`], a small executable reference model. After each operation the
//! test asserts three things:
//!
//! 1. The operation returns what the model predicts.
//! 2. The database rows of the case equal the model state.
//! 3. Each run state change is a sanctioned transition in
//!    `autumn_harvest::lifecycle::TRANSITIONS`. The model asserts this on
//!    every step it takes, so the model is an executable copy of that table.
//!
//! The model comes from four documented contracts:
//!
//! - the reuse-policy matrix on `start_or_load_workflow_execution`;
//! - the claim fence `(worker_id, attempt)`, `docs/architecture.md` section 9;
//! - the lost-wake rule of `park_workflow_task`;
//! - the orphan reclaim rules of `poison_pill`.
//!
//! The claim order follows `docs/operations/claim-order.md` (issue #1824).
//! A claim sorts by `scheduled_at`, but a new start sorts 30 seconds later.
//!
//! # Case count
//!
//! The case count comes from `PROPTEST_CASES`, the same knob as the
//! `property` target. CI runs the default of 128 cases on each change. The
//! nightly `proptest-nightly.yml` workflow runs a deep pass.
//!
//! # Isolation
//!
//! The orphan scan is global, so each case starts on empty engine tables.
//! The test therefore owns its database: a container, or a throwaway
//! database on the `HARVEST_TEST_DATABASE_URL` server.

#[path = "../property/prop_config.rs"]
mod prop_config;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

use autumn_harvest::StartWorkflowParams;
use autumn_harvest::error::HarvestError;
use autumn_harvest::lifecycle::{WorkflowState, is_sanctioned};
use autumn_harvest::queue::{CLAIM_ORDER_DUE_SQL, ClaimWrite, NEW_START_HANDICAP_SECS, TaskClaim};
use autumn_harvest::types::{
    ExecutionId, ShardId, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text, Uuid as SqlUuid};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// Workflow ids per case. A small number makes collisions likely.
const SLOTS: usize = 3;
/// Worker clients per case.
const WORKERS: usize = 2;
/// Chunks per case, at most. A chunk is one operation or one short motif.
const MAX_CHUNKS: usize = 16;
/// The crash-strike count that sends an orphan to the dead-letter queue.
const STRIKE_THRESHOLD: i32 = 2;
/// A worker with no heartbeat in this many seconds is dead.
const WORKER_STALE_SECS: i64 = 60;
/// The backdate of a fresh enqueue, `IMMEDIATE_SCHEDULE_SKEW_SECS` in
/// `queue.rs`, in milliseconds.
const SKEW_MS: i64 = 5_000;
/// Due times closer than this are a tie for the claim order.
const TIE_MS: i64 = 250;
/// [`NEW_START_HANDICAP_SECS`] in milliseconds on the case clock (issue #1824).
const NEW_START_HANDICAP_MS: i64 = NEW_START_HANDICAP_SECS as i64 * 1_000;

// ── Operations ──────────────────────────────────────────────────────────────

/// The reuse policies the model covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    AllowDuplicate,
    RejectDuplicate,
    AllowDuplicateFailedOnly,
    TerminateIfRunning,
}

impl Policy {
    const fn engine(self) -> WorkflowIdReusePolicy {
        match self {
            Self::AllowDuplicate => WorkflowIdReusePolicy::AllowDuplicate,
            Self::RejectDuplicate => WorkflowIdReusePolicy::RejectDuplicate,
            Self::AllowDuplicateFailedOnly => WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            Self::TerminateIfRunning => WorkflowIdReusePolicy::TerminateIfRunning,
        }
    }
}

/// One client operation. `pick` counts the runs of a slot from the newest,
/// so an operation can also target an old run that a newer run replaced.
/// `claim` counts the claims a worker holds from the newest, so a worker
/// can also use a stale claim.
#[derive(Debug, Clone, Copy)]
enum Op {
    Start { slot: usize, policy: Policy },
    Claim { worker: usize },
    Heartbeat { worker: usize, claim: usize },
    Park { worker: usize, claim: usize },
    Complete { worker: usize, claim: usize },
    Signal { slot: usize, pick: usize },
    Cancel { slot: usize, pick: usize },
    KillWorker { worker: usize },
    ReviveWorker { worker: usize },
    Reclaim,
}

fn policy() -> impl Strategy<Value = Policy> {
    prop_oneof![
        Just(Policy::AllowDuplicate),
        Just(Policy::RejectDuplicate),
        Just(Policy::AllowDuplicateFailedOnly),
        Just(Policy::TerminateIfRunning),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    let slot = 0..SLOTS;
    let worker = 0..WORKERS;
    prop_oneof![
        3 => (slot.clone(), policy()).prop_map(|(slot, policy)| Op::Start { slot, policy }),
        4 => worker.clone().prop_map(|worker| Op::Claim { worker }),
        1 => (worker.clone(), 0..3usize).prop_map(|(worker, claim)| Op::Heartbeat { worker, claim }),
        2 => (worker.clone(), 0..3usize).prop_map(|(worker, claim)| Op::Park { worker, claim }),
        3 => (worker.clone(), 0..3usize).prop_map(|(worker, claim)| Op::Complete { worker, claim }),
        2 => (slot.clone(), 0..4usize).prop_map(|(slot, pick)| Op::Signal { slot, pick }),
        1 => (slot, 0..4usize).prop_map(|(slot, pick)| Op::Cancel { slot, pick }),
        1 => worker.clone().prop_map(|worker| Op::KillWorker { worker }),
        1 => worker.prop_map(|worker| Op::ReviveWorker { worker }),
        2 => Just(Op::Reclaim),
    ]
}

/// A short sequence that reaches a rare branch. Random operations alone
/// reach these branches too seldom for a coverage check at 128 cases.
fn motif() -> impl Strategy<Value = Vec<Op>> {
    let start = |slot| Op::Start {
        slot,
        policy: Policy::AllowDuplicate,
    };
    prop_oneof![
        // A signal lands while a worker holds the task. The park sees it.
        (0..SLOTS, 0..WORKERS).prop_map(move |(slot, worker)| vec![
            start(slot),
            Op::Claim { worker },
            Op::Signal { slot, pick: 0 },
            Op::Park { worker, claim: 0 },
        ]),
        // A task orphaned twice goes to the dead-letter queue.
        (0..SLOTS, 0..WORKERS).prop_map(move |(slot, worker)| vec![
            start(slot),
            Op::KillWorker { worker },
            Op::Claim { worker },
            Op::Reclaim,
            Op::Claim { worker },
            Op::Reclaim,
            Op::ReviveWorker { worker },
        ]),
        // A held claim heartbeats and completes. The run is then terminal,
        // and the claim is stale.
        (0..SLOTS, 0..WORKERS).prop_map(move |(slot, worker)| vec![
            start(slot),
            Op::Claim { worker },
            Op::Heartbeat { worker, claim: 0 },
            Op::Complete { worker, claim: 0 },
            Op::Signal { slot, pick: 0 },
            Op::Cancel { slot, pick: 0 },
            Op::Heartbeat { worker, claim: 0 },
        ]),
        // A cancel of a cancelled run is a no-op, and the run rejects a
        // signal. A failed-only start replaces it.
        (0..SLOTS).prop_map(move |slot| vec![
            start(slot),
            Op::Cancel { slot, pick: 0 },
            Op::Cancel { slot, pick: 0 },
            Op::Signal { slot, pick: 0 },
            Op::Start {
                slot,
                policy: Policy::AllowDuplicateFailedOnly,
            },
            Op::Signal { slot, pick: 1 },
        ]),
        // A worker re-claims its own orphan. Only `attempt` fences the old
        // claim, because the worker id is the same.
        (0..SLOTS, 0..WORKERS).prop_map(move |(slot, worker)| vec![
            start(slot),
            Op::KillWorker { worker },
            Op::Claim { worker },
            Op::Reclaim,
            Op::ReviveWorker { worker },
            Op::Claim { worker },
            Op::Heartbeat { worker, claim: 1 },
            Op::Complete { worker, claim: 1 },
            Op::Complete { worker, claim: 0 },
        ]),
        // A claim lost to a reclaim is stale for its old owner.
        (0..SLOTS, 0..WORKERS).prop_map(move |(slot, worker)| vec![
            start(slot),
            Op::Claim { worker },
            Op::KillWorker { worker },
            Op::Reclaim,
            Op::ReviveWorker { worker },
            Op::Claim {
                worker: (worker + 1) % WORKERS,
            },
            Op::Heartbeat { worker, claim: 0 },
            Op::Complete { worker, claim: 0 },
        ]),
    ]
}

/// A case: random single operations mixed with motifs.
fn ops() -> impl Strategy<Value = Vec<Op>> {
    let chunk = prop_oneof![4 => op().prop_map(|o| vec![o]), 1 => motif()];
    proptest::collection::vec(chunk, 1..=MAX_CHUNKS)
        .prop_map(|chunks| chunks.into_iter().flatten().collect())
}

/// The observable result of one operation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Res {
    /// The operation did not touch the database. The model skips an
    /// operation that a real client could not issue.
    Skipped,
    Ok,
    /// A start returned a run.
    Started {
        run: usize,
        created: bool,
    },
    /// A claim returned this task, or no task.
    Claimed(Option<usize>),
    Heartbeat(ClaimWrite),
    /// A park returned whether a wake raced it.
    Parked {
        had_wake: bool,
    },
    Reclaimed {
        requeued: usize,
        quarantined: usize,
    },
    AlreadyExists,
    Cancelled,
    AlreadyTerminal,
    ClaimAmbiguous,
}

// ── Reference model ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskState {
    Pending,
    Running,
    Completed,
    Failed,
}

impl TaskState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

/// The workflow task of one run. A run in this test has exactly one task.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Task {
    state: TaskState,
    /// The claiming worker. `None` while a `RUNNING` task is parked.
    worker: Option<usize>,
    attempt: i32,
    strikes: i32,
    wake_requested: bool,
    /// The `scheduled_at` of the task, in milliseconds on the case clock.
    due: i64,
    /// The engine's `new_start` flag. Every start in the model sets it.
    new_start: bool,
}

impl Task {
    /// The claim-order due time, as `queue::CLAIM_ORDER_DUE_SQL` computes it.
    /// A new start that no claim has kept sorts the handicap later. A claim
    /// takes the task with the earliest claim-order due time.
    const fn claim_due(&self) -> i64 {
        if self.new_start && self.attempt == 0 {
            self.due + NEW_START_HANDICAP_MS
        } else {
            self.due
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    slot: usize,
    state: WorkflowState,
    task: Task,
    signals: i64,
    /// The lifecycle event types of the run, in history order.
    events: Vec<&'static str>,
}

/// What a worker client believes it holds. It can be stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Held {
    run: usize,
    attempt: i32,
    strikes: i32,
}

#[derive(Debug, Clone, Default)]
struct Model {
    runs: Vec<Run>,
    alive: [bool; WORKERS],
    /// The claims each worker client holds, oldest first.
    held: [Vec<Held>; WORKERS],
    /// The case clock in milliseconds. The runner sets it before each step.
    now: i64,
    /// Tasks that the orphan reclaim sent to the dead-letter queue.
    dead_letters: i64,
}

impl Model {
    fn new() -> Self {
        Self {
            alive: [true; WORKERS],
            ..Self::default()
        }
    }

    /// The `scheduled_at` of a fresh enqueue or a wake. The engine sets it
    /// `IMMEDIATE_SCHEDULE_SKEW_SECS` in the past to absorb host clock skew.
    const fn backdated(&self) -> i64 {
        self.now - SKEW_MS
    }

    /// Move a run to `to`. The move must be a sanctioned lifecycle transition.
    fn transition(&mut self, run: usize, to: WorkflowState) {
        let from = self.runs[run].state;
        assert!(
            is_sanctioned(from, to),
            "model bug: {from:?} -> {to:?} is not in lifecycle::TRANSITIONS"
        );
        self.runs[run].state = to;
    }

    /// The one run of a slot that holds the workflow id. A sealed run frees it.
    fn live_run(&self, slot: usize) -> Option<usize> {
        self.runs
            .iter()
            .rposition(|r| r.slot == slot && r.state != WorkflowState::ContinuedAsNew)
    }

    fn runs_of(&self, slot: usize) -> Vec<usize> {
        (0..self.runs.len())
            .filter(|&i| self.runs[i].slot == slot)
            .collect()
    }

    fn insert_run(&mut self, slot: usize) -> usize {
        let due = self.backdated();
        self.runs.push(Run {
            slot,
            state: WorkflowState::Running,
            task: Task {
                state: TaskState::Pending,
                worker: None,
                attempt: 0,
                strikes: 0,
                wake_requested: false,
                due,
                new_start: true,
            },
            signals: 0,
            events: vec!["WorkflowStarted"],
        });
        self.runs.len() - 1
    }

    /// Cancel moves the run to `CANCELLED` and fails its open task.
    fn cancel(&mut self, run: usize) {
        self.transition(run, WorkflowState::Cancelled);
        self.runs[run].events.push("WorkflowCancelled");
        self.fail_open_task(run);
    }

    fn fail_open_task(&mut self, run: usize) {
        let task = &mut self.runs[run].task;
        if matches!(task.state, TaskState::Pending | TaskState::Running) {
            task.state = TaskState::Failed;
        }
    }

    /// The reuse matrix on `start_or_load_workflow_execution`.
    fn start(&mut self, slot: usize, policy: Policy) -> Res {
        let Some(prior) = self.live_run(slot) else {
            let run = self.insert_run(slot);
            return Res::Started { run, created: true };
        };
        let state = self.runs[prior].state;
        let existing = Res::Started {
            run: prior,
            created: false,
        };
        let replace = match (policy, state) {
            (Policy::RejectDuplicate, _) => return Res::AlreadyExists,
            (Policy::AllowDuplicate, _)
            | (
                Policy::AllowDuplicateFailedOnly,
                WorkflowState::Running | WorkflowState::Completed,
            ) => return existing,
            (Policy::AllowDuplicateFailedOnly | Policy::TerminateIfRunning, _) => prior,
        };
        if self.runs[replace].state == WorkflowState::Running {
            self.cancel(replace);
        }
        self.transition(replace, WorkflowState::ContinuedAsNew);
        let run = self.insert_run(slot);
        Res::Started { run, created: true }
    }

    fn claim(&mut self, worker: usize) -> Res {
        let best = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, r)| r.task.state == TaskState::Pending)
            .map(|(i, r)| (r.task.claim_due(), i))
            .min();
        let Some((_, run)) = best else {
            return Res::Claimed(None);
        };
        self.apply_claim(worker, run);
        Res::Claimed(Some(run))
    }

    /// A valid claim takes the pending task with the earliest claim-order due
    /// time. Two due times closer than [`TIE_MS`] are a tie, because the model
    /// clock and the database clock read at slightly different moments.
    fn claim_is_valid(&self, run: usize) -> bool {
        let min = self
            .runs
            .iter()
            .filter(|r| r.task.state == TaskState::Pending)
            .map(|r| r.task.claim_due())
            .min();
        let task = &self.runs[run].task;
        task.state == TaskState::Pending && min.is_some_and(|m| task.claim_due() <= m + TIE_MS)
    }

    fn apply_claim(&mut self, worker: usize, run: usize) {
        let task = &mut self.runs[run].task;
        task.state = TaskState::Running;
        task.worker = Some(worker);
        task.attempt += 1;
        task.wake_requested = false;
        self.held[worker].push(Held {
            run,
            attempt: task.attempt,
            strikes: task.strikes,
        });
    }

    /// The claim fence: the task is `RUNNING` and `(worker_id, attempt)`
    /// matches. A completion also checks the crash-strike count.
    fn holds(&self, worker: usize, held: Held, check_strikes: bool) -> bool {
        let task = &self.runs[held.run].task;
        task.state == TaskState::Running
            && task.worker == Some(worker)
            && task.attempt == held.attempt
            && (!check_strikes || task.strikes == held.strikes)
    }

    /// Claim `claim` of a worker, counted from the newest, with its index.
    fn claim_of(&self, worker: usize, claim: usize) -> Option<(usize, Held)> {
        let list = &self.held[worker];
        let index = list.len().checked_sub(1 + claim % list.len().max(1))?;
        Some((index, list[index]))
    }

    fn heartbeat(&self, worker: usize, claim: usize) -> Res {
        match self.claim_of(worker, claim) {
            None => Res::Skipped,
            Some((_, held)) if self.holds(worker, held, false) => {
                Res::Heartbeat(ClaimWrite::Applied)
            }
            Some(_) => Res::Heartbeat(ClaimWrite::LeaseLost),
        }
    }

    /// A worker parks only a task it holds. A wake that raced the park makes
    /// the engine re-wake the task at once.
    fn park(&mut self, worker: usize, claim: usize) -> Res {
        let Some((_, held)) = self.claim_of(worker, claim) else {
            return Res::Skipped;
        };
        // The engine parks only inside the persist transaction, after it
        // locks and checks the claim. A stale claim never reaches a park.
        if !self.holds(worker, held, false) {
            return Res::Skipped;
        }
        let task = &mut self.runs[held.run].task;
        let had_wake = task.wake_requested;
        task.worker = None;
        task.wake_requested = false;
        if had_wake {
            self.wake(held.run);
        }
        Res::Parked { had_wake }
    }

    /// `wake_workflow_task`: a parked task becomes pending. A claimed task
    /// gets the `wake_requested` flag.
    fn wake(&mut self, run: usize) {
        let due = self.backdated();
        let task = &mut self.runs[run].task;
        if task.state != TaskState::Running {
            return;
        }
        if task.worker.is_some() {
            task.wake_requested = true;
        } else {
            task.state = TaskState::Pending;
            task.due = due;
        }
    }

    fn complete(&mut self, worker: usize, claim: usize) -> Res {
        let Some((_, held)) = self.claim_of(worker, claim) else {
            return Res::Skipped;
        };
        if !self.holds(worker, held, true) {
            return Res::ClaimAmbiguous;
        }
        // The worker keeps the claim, so a later use of it is stale.
        self.transition(held.run, WorkflowState::Completed);
        self.runs[held.run].events.push("WorkflowCompleted");
        self.runs[held.run].task.state = TaskState::Completed;
        Res::Ok
    }

    /// Run `pick` of a slot, counted from the newest. `0` is the newest run.
    fn pick(&self, slot: usize, pick: usize) -> Option<usize> {
        let runs = self.runs_of(slot);
        (!runs.is_empty()).then(|| runs[runs.len() - 1 - pick % runs.len()])
    }

    fn signal(&mut self, run: usize) -> Res {
        match self.runs[run].state {
            WorkflowState::Running => {
                self.runs[run].signals += 1;
                self.wake(run);
                Res::Ok
            }
            WorkflowState::Cancelled => Res::Cancelled,
            _ => Res::AlreadyTerminal,
        }
    }

    fn cancel_op(&mut self, run: usize) -> Res {
        match self.runs[run].state {
            WorkflowState::Running => {
                self.cancel(run);
                Res::Ok
            }
            WorkflowState::Cancelled => Res::Ok,
            _ => Res::AlreadyTerminal,
        }
    }

    /// `reclaim_orphaned_tasks`: each claimed task of a dead worker gets a
    /// strike. At the threshold the task goes to the dead-letter queue and
    /// its run fails. Below it the task is pending again.
    fn reclaim(&mut self) -> Res {
        // The requeue stamps `clock_timestamp()` with no backdate. The orphan
        // keeps its `attempt`, so it is a continuation with no handicap. It
        // sorts ahead of a fresh start made up to 25 seconds before the
        // reclaim (issue #1923).
        let due = self.now;
        let (mut requeued, mut quarantined) = (0, 0);
        for run in 0..self.runs.len() {
            let task = &self.runs[run].task;
            let orphan =
                task.state == TaskState::Running && task.worker.is_some_and(|w| !self.alive[w]);
            if !orphan {
                continue;
            }
            let strikes = task.strikes + 1;
            self.runs[run].task.strikes = strikes;
            if strikes >= STRIKE_THRESHOLD {
                quarantined += 1;
                self.dead_letters += 1;
                self.runs[run].task.state = TaskState::Failed;
                if self.runs[run].state == WorkflowState::Running {
                    self.transition(run, WorkflowState::Failed);
                    self.runs[run].events.push("WorkflowFailed");
                }
            } else {
                requeued += 1;
                let task = &mut self.runs[run].task;
                task.state = TaskState::Pending;
                task.worker = None;
                task.due = due;
            }
        }
        Res::Reclaimed {
            requeued,
            quarantined,
        }
    }
}

// ── Database driver ─────────────────────────────────────────────────────────

/// One case's names and the execution ids of its runs.
struct Case {
    name: String,
    queue: String,
    workers: Vec<String>,
    execs: Vec<ExecutionId>,
}

impl Case {
    fn new() -> Self {
        let id = Uuid::new_v4().simple().to_string();
        Self {
            name: format!("lm_{id}"),
            queue: format!("lm-{id}"),
            workers: (0..WORKERS).map(|w| format!("lm-{id}-w{w}")).collect(),
            execs: Vec::new(),
        }
    }

    fn workflow_id(&self, slot: usize) -> String {
        format!("{}-wf{slot}", self.name)
    }

    fn run_of(&self, exec: ExecutionId) -> Option<usize> {
        self.execs.iter().position(|e| *e == exec)
    }
}

fn classify(err: &HarvestError) -> Option<Res> {
    match err {
        HarvestError::AlreadyExists { .. } => Some(Res::AlreadyExists),
        HarvestError::Cancelled(_) => Some(Res::Cancelled),
        HarvestError::Config(msg) if msg.contains("terminal") => Some(Res::AlreadyTerminal),
        HarvestError::TerminalWriteClaimAmbiguous { .. } => Some(Res::ClaimAmbiguous),
        _ => None,
    }
}

fn params<'a>(
    case: &'a Case,
    wf_id: &'a str,
    exec: ExecutionId,
    policy: Policy,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: &case.name,
        workflow_id: wf_id,
        exec_id: exec,
        input: serde_json::json!(null).into(),
        parent_id: None,
        queue_name: &case.queue,
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: policy.engine(),
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
        priority: autumn_harvest::Priority::default(),
        max_workflow_input_bytes: 0,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,
        sla: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: None,
        origin: None,
        completion_callbacks: None,
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
    }
}

/// Run `op` against the database. `model` is the state before the operation.
/// It gives the client view: which claim a worker holds and which run a slot
/// index names.
async fn apply_db(
    conn: &mut AsyncPgConnection,
    case: &mut Case,
    model: &Model,
    op: Op,
) -> Result<Res, String> {
    let fail = |e: HarvestError| format!("{op:?} failed unexpectedly: {e}");
    match op {
        Op::Start { slot, policy } => {
            let exec = ExecutionId::new_for_shard(ShardId::new(0));
            let wf_id = case.workflow_id(slot);
            let out = autumn_harvest::execution::start_or_load_workflow_execution(
                conn,
                params(case, &wf_id, exec, policy),
                None,
            )
            .await;
            match out {
                Ok(started) => {
                    if started.created {
                        case.execs.push(started.exec_id);
                    }
                    let run = case
                        .run_of(started.exec_id)
                        .ok_or_else(|| format!("{op:?} returned an unknown run"))?;
                    Ok(Res::Started {
                        run,
                        created: started.created,
                    })
                }
                Err(e) => classify(&e).ok_or_else(|| fail(e)),
            }
        }
        Op::Claim { worker } => {
            let due = due_tasks(conn, &case.queue).await?;
            let task = autumn_harvest::queue::claim_task(
                conn,
                std::slice::from_ref(&case.queue),
                &case.workers[worker],
                "",
                None,
                &[],
                &[],
            )
            .await
            .map_err(fail)?;
            if let Some(task) = &task {
                let earliest = due.iter().map(|(_, at)| *at).min();
                let claimed = due.iter().find(|(id, _)| *id == task.id).map(|(_, at)| *at);
                if claimed.is_none() || claimed != earliest {
                    return Err(format!(
                        "the claim took a task with claim-order due time {claimed:?}, \
                         but the earliest is {earliest:?}"
                    ));
                }
            }
            let run = match task {
                None => None,
                Some(task) => {
                    let exec = task
                        .workflow_exec_id
                        .ok_or("a claimed workflow task has no execution")?;
                    Some(
                        case.run_of(ExecutionId::from_uuid(exec))
                            .ok_or("a claim returned a task of another case")?,
                    )
                }
            };
            Ok(Res::Claimed(run))
        }
        Op::Heartbeat { worker, claim } => {
            let Some((_, held)) = model.claim_of(worker, claim) else {
                return Ok(Res::Skipped);
            };
            let task = task_id(conn, case.execs[held.run]).await?;
            let claim = TaskClaim::new(task, case.workers[worker].clone(), held.attempt);
            let write =
                autumn_harvest::queue::record_heartbeat(conn, &claim, serde_json::json!({}))
                    .await
                    .map_err(fail)?;
            Ok(Res::Heartbeat(write))
        }
        Op::Park { worker, claim } => {
            let Some((_, held)) = model.claim_of(worker, claim) else {
                return Ok(Res::Skipped);
            };
            if !model.holds(worker, held, false) {
                return Ok(Res::Skipped);
            }
            let task = task_id(conn, case.execs[held.run]).await?;
            let had_wake = autumn_harvest::queue::park_workflow_task(conn, task, None)
                .await
                .map_err(fail)?;
            if had_wake {
                autumn_harvest::queue::wake_workflow_task(conn, case.execs[held.run])
                    .await
                    .map_err(fail)?;
            }
            Ok(Res::Parked { had_wake })
        }
        Op::Complete { worker, claim } => {
            let Some((_, held)) = model.claim_of(worker, claim) else {
                return Ok(Res::Skipped);
            };
            let exec = case.execs[held.run];
            let task = task_id(conn, exec).await?;
            let next_event_id = autumn_harvest::store::load_history(conn, exec)
                .await
                .map_err(fail)?
                .next_event_id;
            let mut cancel_metrics = Vec::new();
            let out = autumn_harvest::worker::persist_workflow_completion(
                conn,
                task,
                exec,
                next_event_id,
                &case.workers[worker],
                held.strikes,
                held.attempt,
                serde_json::json!("done"),
                None,
                None,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
                &mut cancel_metrics,
            )
            .await;
            match out {
                Ok(_) => Ok(Res::Ok),
                Err(e) => classify(&e).ok_or_else(|| fail(e)),
            }
        }
        Op::Signal { slot, pick } => {
            let Some(run) = model.pick(slot, pick) else {
                return Ok(Res::Skipped);
            };
            let out = autumn_harvest::signal::send_signal(
                conn,
                case.execs[run],
                "go",
                serde_json::json!({}),
            )
            .await;
            match out {
                Ok(()) => Ok(Res::Ok),
                Err(e) => classify(&e).ok_or_else(|| fail(e)),
            }
        }
        Op::Cancel { slot, pick } => {
            let Some(run) = model.pick(slot, pick) else {
                return Ok(Res::Skipped);
            };
            let out = autumn_harvest::execution::cancel_workflow_execution(
                conn,
                case.execs[run],
                "model",
                &autumn_harvest::telemetry::NoOpMetrics,
            )
            .await;
            match out {
                Ok(_) => Ok(Res::Ok),
                Err(e) => classify(&e).ok_or_else(|| fail(e)),
            }
        }
        Op::KillWorker { worker } => {
            diesel::sql_query(
                "UPDATE harvest_workers \
                 SET last_heartbeat_at = NOW() - INTERVAL '1 hour' WHERE worker_id = $1",
            )
            .bind::<Text, _>(&case.workers[worker])
            .execute(conn)
            .await
            .map_err(|e| format!("kill worker: {e}"))?;
            Ok(Res::Ok)
        }
        Op::ReviveWorker { worker } => {
            let rows = autumn_harvest::workers::heartbeat_worker(
                conn,
                &case.workers[worker],
                0,
                &serde_json::json!({}),
                0,
                &[],
            )
            .await
            .map_err(fail)?;
            if rows == 1 {
                Ok(Res::Ok)
            } else {
                Err(format!("{op:?} updated {rows} rows"))
            }
        }
        Op::Reclaim => {
            let summary = autumn_harvest::poison_pill::reclaim_orphaned_tasks(
                conn,
                STRIKE_THRESHOLD,
                WORKER_STALE_SECS,
                None,
                &autumn_harvest::telemetry::NoOpMetrics,
                &autumn_harvest::payload_codec::PayloadCodecs::default(),
            )
            .await
            .map_err(fail)?;
            Ok(Res::Reclaimed {
                requeued: summary.requeued,
                quarantined: summary.quarantined,
            })
        }
    }
}

/// Apply `op` to the model. A claim takes the run the database chose when
/// that run is a valid choice, because tasks can share a queue position.
fn apply_model(model: &mut Model, op: Op, db: &Res) -> Res {
    match op {
        Op::Start { slot, policy } => model.start(slot, policy),
        Op::Claim { worker } => match db {
            Res::Claimed(Some(run)) if model.claim_is_valid(*run) => {
                model.apply_claim(worker, *run);
                db.clone()
            }
            _ => model.claim(worker),
        },
        Op::Heartbeat { worker, claim } => model.heartbeat(worker, claim),
        Op::Park { worker, claim } => model.park(worker, claim),
        Op::Complete { worker, claim } => model.complete(worker, claim),
        Op::Signal { slot, pick } => model
            .pick(slot, pick)
            .map_or(Res::Skipped, |run| model.signal(run)),
        Op::Cancel { slot, pick } => model
            .pick(slot, pick)
            .map_or(Res::Skipped, |run| model.cancel_op(run)),
        Op::KillWorker { worker } => {
            model.alive[worker] = false;
            Res::Ok
        }
        Op::ReviveWorker { worker } => {
            model.alive[worker] = true;
            Res::Ok
        }
        Op::Reclaim => model.reclaim(),
    }
}

/// The pending tasks of `queue` that are due, with their claim-order due
/// time. The query uses the engine's own term, so the check cannot drift
/// from the claim (issue #1824).
async fn due_tasks(
    conn: &mut AsyncPgConnection,
    queue: &str,
) -> Result<Vec<(Uuid, chrono::DateTime<chrono::Utc>)>, String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        claim_due: chrono::DateTime<chrono::Utc>,
    }
    diesel::sql_query(format!(
        "SELECT id, {CLAIM_ORDER_DUE_SQL} AS claim_due FROM harvest_task_queue \
         WHERE queue_name = $1 AND state = 'PENDING' AND scheduled_at <= NOW()"
    ))
    .bind::<Text, _>(queue)
    .load::<Row>(conn)
    .await
    .map(|rows| rows.into_iter().map(|r| (r.id, r.claim_due)).collect())
    .map_err(|e| format!("load due tasks: {e}"))
}

async fn task_id(conn: &mut AsyncPgConnection, exec: ExecutionId) -> Result<Uuid, String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
    }
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE workflow_exec_id = $1")
        .bind::<SqlUuid, _>(exec.as_uuid())
        .get_result::<Row>(conn)
        .await
        .map(|r| r.id)
        .map_err(|e| format!("load task id of {exec}: {e}"))
}

/// One run as the database stores it, in the model's terms.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Observed {
    state: String,
    task_state: String,
    /// The claiming worker of an open task. A closed task keeps a stale
    /// `worker_id` for bookkeeping, so the test ignores it there.
    worker: Option<usize>,
    attempt: i32,
    strikes: i32,
    wake_requested: bool,
    signals: i64,
    tasks: i64,
    /// The lifecycle event types, comma-separated, in history order.
    events: String,
}

/// The event types that change the lifecycle of a run.
const LIFECYCLE_EVENTS: &str = "'WorkflowStarted', 'WorkflowCompleted', 'WorkflowFailed', \
     'WorkflowCancelled', 'WorkflowContinuedAsNew', 'WorkflowResetTerminated', \
     'WorkflowExecutionTimedOut'";

/// A task state that can still change.
fn is_open(task_state: &str) -> bool {
    task_state == "PENDING" || task_state == "RUNNING"
}

impl Observed {
    fn of(run: &Run) -> Self {
        Self {
            state: run.state.as_str().to_string(),
            task_state: run.task.state.as_str().to_string(),
            worker: is_open(run.task.state.as_str())
                .then_some(run.task.worker)
                .flatten(),
            attempt: run.task.attempt,
            strikes: run.task.strikes,
            wake_requested: run.task.wake_requested,
            signals: run.signals,
            tasks: 1,
            events: run.events.join(","),
        }
    }
}

/// Read every run of the case in one query, in the order the case created
/// them.
async fn observe(conn: &mut AsyncPgConnection, case: &Case) -> Result<Vec<Observed>, String> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = Nullable<Text>)]
        task_state: Option<String>,
        #[diesel(sql_type = Nullable<Text>)]
        worker_id: Option<String>,
        #[diesel(sql_type = Nullable<Integer>)]
        attempt: Option<i32>,
        #[diesel(sql_type = Nullable<Integer>)]
        crash_strikes: Option<i32>,
        #[diesel(sql_type = Nullable<Bool>)]
        wake_requested: Option<bool>,
        #[diesel(sql_type = BigInt)]
        signals: i64,
        #[diesel(sql_type = BigInt)]
        tasks: i64,
        #[diesel(sql_type = Nullable<Text>)]
        events: Option<String>,
    }
    let sql = format!(
        "SELECT e.id, e.state, t.state AS task_state, t.worker_id, t.attempt, \
                t.crash_strikes, t.wake_requested, \
                (SELECT COUNT(*) FROM harvest_signals s \
                  WHERE s.workflow_exec_id = e.id) AS signals, \
                (SELECT COUNT(*) FROM harvest_task_queue q \
                  WHERE q.workflow_exec_id = e.id) AS tasks, \
                (SELECT string_agg(v.event_type, ',' ORDER BY v.event_id) \
                   FROM harvest_events v \
                  WHERE v.workflow_exec_id = e.id \
                    AND v.event_type IN ({LIFECYCLE_EVENTS})) AS events \
         FROM harvest_workflow_executions e \
         LEFT JOIN harvest_task_queue t ON t.workflow_exec_id = e.id \
         WHERE e.workflow_name = $1"
    );
    let rows: Vec<Row> = diesel::sql_query(sql)
        .bind::<Text, _>(&case.name)
        .load(conn)
        .await
        .map_err(|e| format!("observe: {e}"))?;
    let mut by_id: HashMap<Uuid, Row> = rows.into_iter().map(|r| (r.id, r)).collect();
    let mut observed = Vec::with_capacity(case.execs.len());
    for exec in &case.execs {
        let row = by_id
            .remove(&exec.as_uuid())
            .ok_or_else(|| format!("run {exec} is missing"))?;
        let open = row.task_state.as_deref().is_some_and(is_open);
        observed.push(Observed {
            state: row.state,
            task_state: row.task_state.unwrap_or_default(),
            worker: if open {
                row.worker_id
                    .and_then(|w| case.workers.iter().position(|x| *x == w))
            } else {
                None
            },
            attempt: row.attempt.unwrap_or(-1),
            strikes: row.crash_strikes.unwrap_or(-1),
            wake_requested: row.wake_requested.unwrap_or(false),
            signals: row.signals,
            tasks: row.tasks,
            events: row.events.unwrap_or_default(),
        });
    }
    if !by_id.is_empty() {
        return Err(format!("{} runs exist that no start returned", by_id.len()));
    }
    Ok(observed)
}

/// Check that each observed state change is a sanctioned transition.
fn check_transitions(op: Op, before: &[Observed], after: &[Observed]) -> Result<(), String> {
    let terminate = matches!(
        op,
        Op::Start {
            policy: Policy::TerminateIfRunning,
            ..
        }
    );
    for (i, now) in after.iter().enumerate() {
        let to = WorkflowState::from_db(&now.state)
            .ok_or_else(|| format!("run {i} has an unknown state {}", now.state))?;
        match before.get(i) {
            None if to != WorkflowState::Running => {
                return Err(format!("run {i} was inserted as {to:?}, not RUNNING"));
            }
            Some(old) if old.state != now.state => {
                let from = WorkflowState::from_db(&old.state).unwrap();
                // A start with `TerminateIfRunning` cancels and seals in one
                // call, so one operation can take two sanctioned steps.
                let direct = is_sanctioned(from, to);
                let via_cancel = terminate
                    && is_sanctioned(from, WorkflowState::Cancelled)
                    && is_sanctioned(WorkflowState::Cancelled, to);
                if !direct && !via_cancel {
                    return Err(format!(
                        "run {i} moved {from:?} -> {to:?}, which is not sanctioned"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

// ── Case runner ─────────────────────────────────────────────────────────────

/// The dead-letter count. Each case starts on an empty table.
async fn dead_letter_count(conn: &mut AsyncPgConnection) -> Result<i64, String> {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_dead_letters")
        .get_result::<Count>(conn)
        .await
        .map(|c| c.n)
        .map_err(|e| format!("count dead letters: {e}"))
}

/// Remove the rows of all earlier cases. The orphan scan is global, so a
/// failed case that leaves a claimed task would change the next result.
async fn scrub(conn: &mut AsyncPgConnection) -> Result<(), TestCaseError> {
    conn.batch_execute(
        "TRUNCATE harvest_workflow_executions, harvest_events, harvest_task_queue, \
         harvest_signals, harvest_workers, harvest_dead_letters CASCADE",
    )
    .await
    .map_err(|e| TestCaseError::fail(format!("scrub: {e}")))
}

/// A coverage label for a result. The runner collects the labels of all
/// cases, so a pass that never reaches a branch fails instead.
fn label(model: &Model, op: Op, res: &Res) -> &'static str {
    match (op, res) {
        (Op::Start { slot, .. }, Res::Started { created: true, .. }) => {
            if model.live_run(slot).is_some() {
                "start replaced"
            } else {
                "start created"
            }
        }
        (Op::Start { .. }, Res::Started { created: false, .. }) => "start attached",
        (Op::Start { .. }, Res::AlreadyExists) => "start rejected",
        (Op::Claim { .. }, Res::Claimed(Some(run))) if model.runs[*run].task.strikes > 0 => {
            "claim requeued orphan"
        }
        (Op::Claim { .. }, Res::Claimed(Some(_))) => "claim",
        (Op::Claim { .. }, Res::Claimed(None)) => "claim empty",
        (_, Res::Heartbeat(ClaimWrite::Applied)) => "heartbeat applied",
        (_, Res::Heartbeat(ClaimWrite::LeaseLost)) => "heartbeat lease lost",
        (_, Res::Parked { had_wake: true }) => "park with raced wake",
        (_, Res::Parked { had_wake: false }) => "park",
        (Op::Complete { .. }, Res::Ok) => "complete",
        (Op::Complete { .. }, Res::ClaimAmbiguous) => "complete stale claim",
        (Op::Signal { .. }, Res::Ok) => "signal",
        (Op::Signal { .. }, Res::Cancelled) => "signal cancelled run",
        (Op::Signal { .. }, Res::AlreadyTerminal) => "signal terminal run",
        (Op::Cancel { slot, pick }, Res::Ok) => match model.pick(slot, pick) {
            Some(run) if model.runs[run].state == WorkflowState::Cancelled => "cancel again",
            _ => "cancel",
        },
        (Op::Cancel { .. }, Res::AlreadyTerminal) => "cancel terminal run",
        (
            _,
            Res::Reclaimed {
                quarantined: 1.., ..
            },
        ) => "reclaim quarantine",
        (_, Res::Reclaimed { requeued: 1.., .. }) => "reclaim requeue",
        _ => "other",
    }
}

/// Every label a sound run reaches with the default case count.
const REQUIRED: &[&str] = &[
    "start created",
    "start replaced",
    "start attached",
    "start rejected",
    "claim",
    "claim requeued orphan",
    "claim empty",
    "heartbeat applied",
    "heartbeat lease lost",
    "park with raced wake",
    "park",
    "complete",
    "complete stale claim",
    "signal",
    "signal cancelled run",
    "signal terminal run",
    "cancel",
    "cancel again",
    "cancel terminal run",
    "reclaim quarantine",
    "reclaim requeue",
];

async fn run_case(
    conn: &mut AsyncPgConnection,
    ops: &[Op],
    seen: &mut BTreeSet<&'static str>,
) -> Result<(), TestCaseError> {
    scrub(conn).await?;
    let mut case = Case::new();
    for worker in &case.workers {
        autumn_harvest::workers::register_worker(
            conn,
            worker,
            std::slice::from_ref(&case.queue),
            &[],
            1,
            "model-host",
            None,
            "",
            None,
            &HashMap::<String, String>::new(),
            0,
            &[],
        )
        .await
        .map_err(|e| TestCaseError::fail(format!("register worker: {e}")))?;
    }

    let mut model = Model::new();
    let mut before = Vec::new();
    let clock = std::time::Instant::now();
    for (step, &op) in ops.iter().enumerate() {
        model.now = i64::try_from(clock.elapsed().as_millis()).unwrap_or(i64::MAX);
        let db = apply_db(conn, &mut case, &model, op)
            .await
            .map_err(|e| TestCaseError::fail(format!("step {step}: {e}")))?;
        let tag = label(&model, op, &db);
        let expected = apply_model(&mut model, op, &db);
        prop_assert_eq!(
            &db,
            &expected,
            "step {}: {:?} returned another result",
            step,
            op
        );
        seen.insert(tag);
        let after = observe(conn, &case)
            .await
            .map_err(|e| TestCaseError::fail(format!("step {step}: {e}")))?;
        let wanted: Vec<Observed> = model.runs.iter().map(Observed::of).collect();
        prop_assert_eq!(&after, &wanted, "step {}: rows differ after {:?}", step, op);
        let dead_letters = dead_letter_count(conn)
            .await
            .map_err(|e| TestCaseError::fail(format!("step {step}: {e}")))?;
        prop_assert_eq!(
            dead_letters,
            model.dead_letters,
            "step {}: dead letters differ after {:?}",
            step,
            op
        );
        check_transitions(op, &before, &after)
            .map_err(|e| TestCaseError::fail(format!("step {step}: {e}")))?;
        before = after;
    }

    Ok(())
}

/// A database that this test alone uses. Each case truncates the engine
/// tables, so the test never runs on a shared database. With
/// `HARVEST_TEST_DATABASE_URL` set, it creates a throwaway database on that
/// server and drops it at the end. Otherwise it starts a Postgres 16
/// container.
async fn database() -> (
    String,
    Option<ContainerAsync<Postgres>>,
    Option<crate::throwaway_db::ThrowawayDb>,
) {
    use testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    if let Some(db) = crate::throwaway_db::ThrowawayDb::create("harvest_lifecycle_model").await {
        return (db.url(), None, Some(db));
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container), None)
}

/// Run `body` on a runtime and a connection to [`database`].
fn on_own_database<T>(
    body: impl FnOnce(&tokio::runtime::Runtime, &mut AsyncPgConnection) -> T,
) -> T {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    // The container drops in an async context. Keep the runtime entered so
    // a failing case can unwind past it.
    let _enter = rt.enter();
    let (url, _container, _db) = rt.block_on(database());
    let mut conn = rt
        .block_on(AsyncPgConnection::establish(&url))
        .expect("connect");
    body(&rt, &mut conn)
}

/// The stateful lifecycle property. The database and the model must agree
/// after every operation of every generated sequence.
#[test]
fn lifecycle_matches_the_reference_model() {
    let seen = on_own_database(|rt, conn| {
        let conn = RefCell::new(conn);
        // A database-backed shrink step costs about 150 ms. Cap the shrink so
        // a late failure still prints its sequence before the job times out.
        let mut runner = TestRunner::new(proptest::test_runner::Config {
            max_shrink_time: 20 * 60 * 1000,
            ..prop_config::config()
        });
        let seen = RefCell::new(BTreeSet::new());
        let result = runner.run(&ops(), |ops| {
            rt.block_on(run_case(
                &mut conn.borrow_mut(),
                &ops,
                &mut seen.borrow_mut(),
            ))
        });
        if let Err(e) = result {
            panic!("{e}");
        }
        seen.into_inner()
    });
    let missing: Vec<_> = REQUIRED.iter().filter(|l| !seen.contains(*l)).collect();
    assert!(
        missing.is_empty(),
        "the generated sequences never reached {missing:?}; the pass is too weak"
    );
}

// ── Pinned counterexamples ──────────────────────────────────────────────────

/// A shrunk counterexample that replays against the database.
struct Pinned {
    name: &'static str,
    /// The coverage labels the replay must reach. Without them, a pin can
    /// stop reaching its branch after a change and still pass.
    reaches: &'static [&'static str],
    ops: &'static [Op],
}

/// Shrunk counterexamples from the deep nightly pass. The `test-db-linux`
/// job replays each one against the database. So a model drift shows on a
/// pull request, and not only in the nightly.
const PINNED: &[Pinned] = &[
    // Issue #1923: a worker re-claims its own requeued orphan. The orphan
    // is a continuation, so it sorts ahead of the fresh start of slot 1.
    Pinned {
        name: "#1923 re-claim of an own orphan",
        reaches: &["claim requeued orphan", "complete stale claim"],
        ops: &[
            Op::Start {
                slot: 0,
                policy: Policy::AllowDuplicate,
            },
            Op::Start {
                slot: 1,
                policy: Policy::AllowDuplicate,
            },
            Op::KillWorker { worker: 0 },
            Op::Claim { worker: 0 },
            Op::Reclaim,
            Op::ReviveWorker { worker: 0 },
            Op::Claim { worker: 0 },
            Op::Heartbeat {
                worker: 0,
                claim: 1,
            },
            Op::Complete {
                worker: 0,
                claim: 1,
            },
            Op::Complete {
                worker: 0,
                claim: 0,
            },
        ],
    },
    // Issue #1923: an orphan is requeued, claimed again and quarantined.
    Pinned {
        name: "#1923 orphan to the dead-letter queue",
        reaches: &["claim requeued orphan", "reclaim quarantine"],
        ops: &[
            Op::Start {
                slot: 0,
                policy: Policy::AllowDuplicate,
            },
            Op::Start {
                slot: 1,
                policy: Policy::AllowDuplicate,
            },
            Op::KillWorker { worker: 0 },
            Op::Claim { worker: 0 },
            Op::Reclaim,
            Op::Claim { worker: 0 },
            Op::Reclaim,
            Op::ReviveWorker { worker: 0 },
        ],
    },
    // Issue #1923: another worker claims the requeued orphan. The old
    // claim is then stale.
    Pinned {
        name: "#1923 claim lost to a reclaim",
        reaches: &[
            "claim requeued orphan",
            "heartbeat lease lost",
            "complete stale claim",
        ],
        ops: &[
            Op::Start {
                slot: 2,
                policy: Policy::AllowDuplicate,
            },
            Op::Start {
                slot: 0,
                policy: Policy::AllowDuplicate,
            },
            Op::Claim { worker: 0 },
            Op::KillWorker { worker: 0 },
            Op::Reclaim,
            Op::ReviveWorker { worker: 0 },
            Op::Claim { worker: 1 },
            Op::Heartbeat {
                worker: 0,
                claim: 0,
            },
            Op::Complete {
                worker: 0,
                claim: 0,
            },
        ],
    },
];

/// Each pinned counterexample replays clean against the database and reaches
/// its labels. The test replays all of them and reports every failure.
#[test]
fn pinned_counterexamples_replay() {
    let failures = on_own_database(|rt, conn| {
        PINNED
            .iter()
            .filter_map(|pin| {
                let mut seen = BTreeSet::new();
                if let Err(e) = rt.block_on(run_case(conn, pin.ops, &mut seen)) {
                    return Some(format!("{}: {e}", pin.name));
                }
                let missed: Vec<_> = pin.reaches.iter().filter(|l| !seen.contains(*l)).collect();
                (!missed.is_empty()).then(|| format!("{}: never reached {missed:?}", pin.name))
            })
            .collect::<Vec<_>>()
    });
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ── Model self-tests (no database) ──────────────────────────────────────────

/// After a reclaim and a new claim, the old claim is stale. The fence then
/// rejects its heartbeat and its completion.
#[test]
fn the_fence_decides_a_stale_heartbeat() {
    let mut m = Model::new();
    let _ = m.start(0, Policy::AllowDuplicate);
    assert_eq!(m.claim(0), Res::Claimed(Some(0)));
    assert_eq!(m.heartbeat(0, 0), Res::Heartbeat(ClaimWrite::Applied));
    m.alive[0] = false;
    let _ = m.reclaim();
    assert_eq!(m.claim(1), Res::Claimed(Some(0)));
    assert_eq!(
        m.heartbeat(0, 0),
        Res::Heartbeat(ClaimWrite::LeaseLost),
        "the old claim is stale after a reclaim and a new claim"
    );
    assert_eq!(m.complete(0, 0), Res::ClaimAmbiguous);
    assert_eq!(m.complete(1, 0), Res::Ok);
}

/// A worker that re-claims its own orphan holds two claims with the same
/// worker id. Only `attempt` makes the older one stale.
#[test]
fn attempt_alone_fences_a_reclaimed_claim_of_the_same_worker() {
    let mut m = Model::new();
    let _ = m.start(0, Policy::AllowDuplicate);
    m.alive[0] = false;
    let _ = m.claim(0);
    let _ = m.reclaim();
    m.alive[0] = true;
    let _ = m.claim(0);
    assert_eq!(m.heartbeat(0, 1), Res::Heartbeat(ClaimWrite::LeaseLost));
    assert_eq!(m.heartbeat(0, 0), Res::Heartbeat(ClaimWrite::Applied));
    assert_eq!(m.complete(0, 1), Res::ClaimAmbiguous);
    assert_eq!(m.complete(0, 0), Res::Ok);
}

/// Every reuse policy follows the documented matrix.
#[test]
fn the_reuse_matrix_matches_the_documented_table() {
    let mut m = Model::new();
    assert_eq!(
        m.start(0, Policy::RejectDuplicate),
        Res::Started {
            run: 0,
            created: true
        }
    );
    assert_eq!(
        m.start(0, Policy::AllowDuplicate),
        Res::Started {
            run: 0,
            created: false
        }
    );
    assert_eq!(m.start(0, Policy::RejectDuplicate), Res::AlreadyExists);
    assert_eq!(
        m.start(0, Policy::AllowDuplicateFailedOnly),
        Res::Started {
            run: 0,
            created: false
        }
    );
    assert_eq!(
        m.start(0, Policy::TerminateIfRunning),
        Res::Started {
            run: 1,
            created: true
        }
    );
    assert_eq!(m.runs[0].state, WorkflowState::ContinuedAsNew);
    assert_eq!(m.cancel_op(1), Res::Ok);
    assert_eq!(
        m.start(0, Policy::AllowDuplicateFailedOnly),
        Res::Started {
            run: 2,
            created: true
        }
    );
}

/// A wake that lands while a worker holds the task sets the flag. The park
/// then reports it, and the task is pending again.
#[test]
fn a_wake_during_a_claim_is_not_lost() {
    let mut m = Model::new();
    let _ = m.start(0, Policy::AllowDuplicate);
    let _ = m.claim(0);
    assert_eq!(m.signal(0), Res::Ok);
    assert!(m.runs[0].task.wake_requested);
    assert_eq!(m.park(0, 0), Res::Parked { had_wake: true });
    assert_eq!(m.runs[0].task.state, TaskState::Pending);
}

/// A requeued orphan is a continuation: its claim kept `attempt` above 0. A
/// fresh start is a new start and sorts the handicap later (issue #1824). The
/// orphan is due 5 seconds after the start, so it still goes first.
#[test]
fn a_requeued_orphan_sorts_ahead_of_a_fresh_start() {
    let mut m = Model::new();
    let _ = m.start(0, Policy::AllowDuplicate);
    m.alive[0] = false;
    let _ = m.claim(0);
    let _ = m.reclaim();
    m.now = 100;
    let _ = m.start(1, Policy::AllowDuplicate);
    assert!(
        !m.claim_is_valid(1),
        "the fresh start yields to the continuation"
    );
    assert_eq!(m.claim(1), Res::Claimed(Some(0)));
}

/// Two strikes send an orphan to the dead-letter queue and fail its run.
#[test]
fn repeated_orphans_are_quarantined() {
    let mut m = Model::new();
    let _ = m.start(0, Policy::AllowDuplicate);
    m.alive[0] = false;
    let _ = m.claim(0);
    assert_eq!(
        m.reclaim(),
        Res::Reclaimed {
            requeued: 1,
            quarantined: 0
        }
    );
    let _ = m.claim(0);
    assert_eq!(
        m.reclaim(),
        Res::Reclaimed {
            requeued: 0,
            quarantined: 1
        }
    );
    assert_eq!(m.runs[0].state, WorkflowState::Failed);
    assert_eq!(m.runs[0].task.state, TaskState::Failed);
}

/// Each model step stays inside the lifecycle table. A random walk over the
/// model alone covers far more steps than the database pass can.
#[test]
fn model_walks_stay_inside_the_lifecycle_table() {
    let mut runner = TestRunner::new(prop_config::config());
    runner
        .run(&proptest::collection::vec(ops(), 1..=8), |cases| {
            let mut m = Model::new();
            for op in cases.into_iter().flatten() {
                let _ = apply_model(&mut m, op, &Res::Skipped);
            }
            Ok(())
        })
        .unwrap();
}
