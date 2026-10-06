//! The activity claim store contract and its in-memory oracle.
//!
//! [`Op`] names each store operation that the simulator drives. [`Outcome`]
//! is its result. A backend applies one operation at a time, in the order
//! the simulator chose. The oracle and the Postgres adapter in
//! `tests/integration/dst_differential_tests.rs` must agree on every
//! outcome and every row.

use std::collections::BTreeMap;
use std::fmt;

/// The predicate that guards an owner write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fencing {
    /// The claim epoch of issue #1789: `state`, `worker_id` and `attempt`.
    ClaimEpoch,
    /// The guard before issue #1789: `state = 'RUNNING'` only.
    StateOnly,
}

impl Fencing {
    /// The name that `HARVEST_DST_FENCING` accepts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClaimEpoch => "claim-epoch",
            Self::StateOnly => "state-only",
        }
    }

    /// Parse a name from [`Fencing::as_str`].
    ///
    /// # Errors
    ///
    /// Returns a message that names the accepted values.
    pub fn parse(name: &str) -> Result<Self, String> {
        [Self::ClaimEpoch, Self::StateOnly]
            .into_iter()
            .find(|fencing| fencing.as_str() == name)
            .ok_or_else(|| format!("unknown fencing {name:?}: use claim-epoch or state-only"))
    }
}

/// The `state` column of a task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Ready to claim.
    Pending,
    /// Claimed by a worker.
    Running,
    /// Finished by an owner.
    Completed,
}

impl TaskState {
    /// The column value in `harvest_task_queue.state`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
        }
    }
}

/// One claim of a task row. It is the fencing token of issue #1789.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Claim {
    /// The task index.
    pub task: usize,
    /// The worker that holds the claim.
    pub worker: String,
    /// The row's `attempt` value that the claim wrote.
    pub attempt: i32,
}

/// A row that the orphan scan found.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Orphan {
    /// The task index.
    pub task: usize,
    /// The worker whose liveness row is stale.
    pub worker: String,
    /// The `crash_strikes` value at scan time.
    pub crash_strikes: i32,
}

/// The columns of a task row that the simulator compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The `state` column.
    pub state: TaskState,
    /// The `worker_id` column.
    pub worker: Option<String>,
    /// The `attempt` column.
    pub attempt: i32,
    /// The `crash_strikes` column.
    pub crash_strikes: i32,
    /// The tag in `heartbeat_details`.
    pub heartbeat: Option<u64>,
    /// The tag in `output`.
    pub output: Option<u64>,
}

impl Row {
    /// A new `PENDING` row.
    #[must_use]
    pub const fn pending() -> Self {
        Self {
            state: TaskState::Pending,
            worker: None,
            attempt: 0,
            crash_strikes: 0,
            heartbeat: None,
            output: None,
        }
    }
}

/// One store operation.
///
/// Times are virtual milliseconds since the start of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// A worker refreshes its liveness row (`harvest_workers`).
    Beat {
        /// The worker id.
        worker: String,
        /// The virtual time of the beat.
        at_ms: u64,
    },
    /// A worker claims one task (`queue::claim_task`).
    Claim {
        /// The worker id.
        worker: String,
        /// The task index.
        task: usize,
    },
    /// The start fence (`append_activity_started`).
    Start {
        /// The claim that starts.
        claim: Claim,
    },
    /// A task heartbeat (`queue::record_heartbeat`).
    Heartbeat {
        /// The claim that sends the heartbeat.
        claim: Claim,
        /// The payload tag for `heartbeat_details`.
        tag: u64,
    },
    /// The terminal write (`finalize_activity_completion`).
    Complete {
        /// The claim that finishes.
        claim: Claim,
        /// The payload tag for `output`.
        tag: u64,
    },
    /// The orphan scan (`orphaned_running_tasks_query`).
    Scan {
        /// The virtual time of the scan.
        now_ms: u64,
    },
    /// The orphan requeue (`requeue_orphan_stmt`).
    Requeue {
        /// The row that the scan found.
        orphan: Orphan,
        /// The virtual time of the requeue.
        now_ms: u64,
    },
}

/// The result of an owner write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The write took effect.
    Applied,
    /// The guard did not match, so the write changed nothing.
    LeaseLost,
}

/// The result of one [`Op`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The liveness row is up to date.
    Beat,
    /// The claim, or `None` when the row was not claimable.
    Claimed(Option<Claim>),
    /// The result of `Start`, `Heartbeat` or `Complete`.
    Write(WriteOutcome),
    /// The rows that the scan found, in task order.
    Orphans(Vec<Orphan>),
    /// Whether the requeue moved the row.
    Requeued(bool),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Beat => f.write_str("ok"),
            Self::Claimed(None) => f.write_str("none"),
            Self::Claimed(Some(claim)) => write!(f, "claimed a{}", claim.attempt),
            Self::Write(WriteOutcome::Applied) => f.write_str("applied"),
            Self::Write(WriteOutcome::LeaseLost) => f.write_str("lease-lost"),
            Self::Orphans(orphans) => {
                f.write_str("orphans [")?;
                for (i, orphan) in orphans.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(
                        f,
                        "t{} {} s{}",
                        orphan.task, orphan.worker, orphan.crash_strikes
                    )?;
                }
                f.write_str("]")
            }
            Self::Requeued(true) => f.write_str("requeued"),
            Self::Requeued(false) => f.write_str("kept"),
        }
    }
}

/// A backend for the activity claim protocol.
///
/// The simulator calls [`ClaimStore::apply`] once per step, on one thread.
pub trait ClaimStore {
    /// Apply `op` and return its outcome.
    fn apply(&mut self, op: &Op) -> Outcome;

    /// Every task row, in task order.
    fn rows(&self) -> Vec<Row>;
}

/// The in-memory oracle backend.
///
/// Each operation follows the SQL statement that [`Op`] names. The
/// differential test checks that claim against Postgres.
#[derive(Debug, Clone)]
pub struct OracleStore {
    fencing: Fencing,
    stale_after_ms: u64,
    rows: Vec<Row>,
    beats: BTreeMap<String, u64>,
}

impl OracleStore {
    /// A store with `tasks` pending rows.
    ///
    /// A worker is dead when its last beat is `stale_after_ms` old or older.
    #[must_use]
    pub fn new(tasks: usize, fencing: Fencing, stale_after_ms: u64) -> Self {
        Self {
            fencing,
            stale_after_ms,
            rows: vec![Row::pending(); tasks],
            beats: BTreeMap::new(),
        }
    }

    /// `NOT EXISTS` a liveness row newer than `now_ms - stale_after_ms`.
    fn is_dead(&self, worker: &str, now_ms: u64) -> bool {
        self.beats
            .get(worker)
            .is_none_or(|beat| beat.saturating_add(self.stale_after_ms) <= now_ms)
    }

    /// The `WHERE` clause of an owner write.
    fn accepts(&self, claim: &Claim) -> bool {
        let row = &self.rows[claim.task];
        let running = row.state == TaskState::Running;
        match self.fencing {
            Fencing::ClaimEpoch => {
                running
                    && row.worker.as_deref() == Some(claim.worker.as_str())
                    && row.attempt == claim.attempt
            }
            Fencing::StateOnly => running,
        }
    }

    /// `claim_task`: `PENDING` to `RUNNING`, set `worker_id`, add 1 to
    /// `attempt`.
    fn claim(&mut self, worker: &str, task: usize) -> Outcome {
        let row = &mut self.rows[task];
        if row.state != TaskState::Pending {
            return Outcome::Claimed(None);
        }
        row.state = TaskState::Running;
        row.worker = Some(worker.to_string());
        row.attempt += 1;
        Outcome::Claimed(Some(Claim {
            task,
            worker: worker.to_string(),
            attempt: row.attempt,
        }))
    }

    /// An owner write. `write` changes the row when the guard matches.
    fn owner_write(&mut self, claim: &Claim, write: impl FnOnce(&mut Row)) -> Outcome {
        if !self.accepts(claim) {
            return Outcome::Write(WriteOutcome::LeaseLost);
        }
        write(&mut self.rows[claim.task]);
        Outcome::Write(WriteOutcome::Applied)
    }

    /// `orphaned_running_tasks_query` with the time bound to `now_ms`.
    fn scan(&self, now_ms: u64) -> Outcome {
        let orphans = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.state == TaskState::Running)
            .filter_map(|(task, row)| {
                let worker = row.worker.as_deref()?;
                self.is_dead(worker, now_ms).then(|| Orphan {
                    task,
                    worker: worker.to_string(),
                    crash_strikes: row.crash_strikes,
                })
            })
            .collect();
        Outcome::Orphans(orphans)
    }

    /// `requeue_orphan_stmt`. It keeps `attempt` and `heartbeat_details`.
    fn requeue(&mut self, orphan: &Orphan, now_ms: u64) -> Outcome {
        let dead = self.is_dead(&orphan.worker, now_ms);
        let row = &mut self.rows[orphan.task];
        let matches = row.state == TaskState::Running
            && row.worker.as_deref() == Some(orphan.worker.as_str())
            && row.crash_strikes == orphan.crash_strikes;
        if !(matches && dead) {
            return Outcome::Requeued(false);
        }
        row.state = TaskState::Pending;
        row.worker = None;
        row.crash_strikes = orphan.crash_strikes + 1;
        Outcome::Requeued(true)
    }
}

impl ClaimStore for OracleStore {
    fn apply(&mut self, op: &Op) -> Outcome {
        match op {
            Op::Beat { worker, at_ms } => {
                self.beats.insert(worker.clone(), *at_ms);
                Outcome::Beat
            }
            Op::Claim { worker, task } => self.claim(worker, *task),
            Op::Start { claim } => self.owner_write(claim, |_| {}),
            Op::Heartbeat { claim, tag } => {
                self.owner_write(claim, |row| row.heartbeat = Some(*tag))
            }
            Op::Complete { claim, tag } => self.owner_write(claim, |row| {
                row.state = TaskState::Completed;
                row.output = Some(*tag);
                row.heartbeat = None;
            }),
            Op::Scan { now_ms } => self.scan(*now_ms),
            Op::Requeue { orphan, now_ms } => self.requeue(orphan, *now_ms),
        }
    }

    fn rows(&self) -> Vec<Row> {
        self.rows.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STALE: u64 = 10_000;

    fn claim(store: &mut OracleStore, worker: &str, task: usize) -> Claim {
        match store.apply(&Op::Claim {
            worker: worker.to_string(),
            task,
        }) {
            Outcome::Claimed(Some(claim)) => claim,
            other => panic!("expected a claim, got {other:?}"),
        }
    }

    fn beat(store: &mut OracleStore, worker: &str, at_ms: u64) {
        assert_eq!(
            store.apply(&Op::Beat {
                worker: worker.to_string(),
                at_ms,
            }),
            Outcome::Beat
        );
    }

    fn reclaim(store: &mut OracleStore, now_ms: u64) {
        let Outcome::Orphans(orphans) = store.apply(&Op::Scan { now_ms }) else {
            panic!("scan returns orphans");
        };
        assert_eq!(orphans.len(), 1, "one orphan expected: {orphans:?}");
        let requeued = store.apply(&Op::Requeue {
            orphan: orphans[0].clone(),
            now_ms,
        });
        assert_eq!(requeued, Outcome::Requeued(true));
    }

    fn write(store: &mut OracleStore, op: Op) -> WriteOutcome {
        match store.apply(&op) {
            Outcome::Write(outcome) => outcome,
            other => panic!("expected a write outcome, got {other:?}"),
        }
    }

    /// Worker `w1` claims, stalls, is reclaimed, and claims again.
    fn stale_and_fresh(fencing: Fencing) -> (OracleStore, Claim, Claim) {
        let mut store = OracleStore::new(1, fencing, STALE);
        beat(&mut store, "w1", 0);
        let stale = claim(&mut store, "w1", 0);
        reclaim(&mut store, STALE);
        let fresh = claim(&mut store, "w1", 0);
        (store, stale, fresh)
    }

    #[test]
    fn claim_sets_the_worker_and_adds_one_to_attempt() {
        let mut store = OracleStore::new(2, Fencing::ClaimEpoch, STALE);
        let first = claim(&mut store, "w1", 1);
        assert_eq!(first.attempt, 1);
        assert_eq!(store.rows()[1].state, TaskState::Running);
        assert_eq!(store.rows()[1].worker.as_deref(), Some("w1"));
        assert_eq!(store.rows()[0], Row::pending());
        let again = store.apply(&Op::Claim {
            worker: "w2".to_string(),
            task: 1,
        });
        assert_eq!(
            again,
            Outcome::Claimed(None),
            "a running row is not claimable"
        );
    }

    #[test]
    fn requeue_keeps_attempt_and_adds_a_strike() {
        let (store, stale, fresh) = stale_and_fresh(Fencing::ClaimEpoch);
        assert_eq!(stale.attempt, 1);
        assert_eq!(fresh.attempt, 2);
        assert_eq!(store.rows()[0].crash_strikes, 1);
    }

    #[test]
    fn scan_skips_a_live_worker_and_finds_a_worker_with_no_beat() {
        let mut store = OracleStore::new(2, Fencing::ClaimEpoch, STALE);
        beat(&mut store, "w1", 5_000);
        claim(&mut store, "w1", 0);
        claim(&mut store, "w2", 1);
        let scan = store.apply(&Op::Scan { now_ms: 14_999 });
        assert_eq!(
            scan,
            Outcome::Orphans(vec![Orphan {
                task: 1,
                worker: "w2".to_string(),
                crash_strikes: 0,
            }])
        );
        let scan = store.apply(&Op::Scan { now_ms: 15_000 });
        let Outcome::Orphans(orphans) = scan else {
            panic!("scan returns orphans");
        };
        assert_eq!(orphans.len(), 2, "a beat exactly STALE old is stale");
    }

    #[test]
    fn requeue_rechecks_liveness_and_strikes() {
        let mut store = OracleStore::new(1, Fencing::ClaimEpoch, STALE);
        beat(&mut store, "w1", 0);
        claim(&mut store, "w1", 0);
        let Outcome::Orphans(orphans) = store.apply(&Op::Scan { now_ms: STALE }) else {
            panic!("scan returns orphans");
        };
        beat(&mut store, "w1", STALE);
        let requeue = Op::Requeue {
            orphan: orphans[0].clone(),
            now_ms: STALE,
        };
        assert_eq!(
            store.apply(&requeue),
            Outcome::Requeued(false),
            "a worker that beats between scan and requeue keeps its row"
        );
        let mut changed = orphans[0].clone();
        changed.crash_strikes = 9;
        let requeue = Op::Requeue {
            orphan: changed,
            now_ms: 3 * STALE,
        };
        assert_eq!(store.apply(&requeue), Outcome::Requeued(false));
    }

    #[test]
    fn claim_epoch_rejects_every_stale_owner_write() {
        let (mut store, stale, fresh) = stale_and_fresh(Fencing::ClaimEpoch);
        let ops = [
            Op::Start {
                claim: stale.clone(),
            },
            Op::Heartbeat {
                claim: stale.clone(),
                tag: 1,
            },
            Op::Complete {
                claim: stale,
                tag: 1,
            },
        ];
        for op in ops {
            assert_eq!(write(&mut store, op), WriteOutcome::LeaseLost);
        }
        assert_eq!(store.rows()[0].state, TaskState::Running);
        let done = Op::Complete {
            claim: fresh,
            tag: 2,
        };
        assert_eq!(write(&mut store, done), WriteOutcome::Applied);
        assert_eq!(store.rows()[0].state, TaskState::Completed);
        assert_eq!(store.rows()[0].output, Some(2));
    }

    #[test]
    fn state_only_lets_a_stale_owner_finish_the_task() {
        let (mut store, stale, fresh) = stale_and_fresh(Fencing::StateOnly);
        let done = Op::Complete {
            claim: stale,
            tag: 1,
        };
        assert_eq!(write(&mut store, done), WriteOutcome::Applied);
        assert_eq!(store.rows()[0].output, Some(1), "the stale result wins");
        let late = Op::Complete {
            claim: fresh,
            tag: 2,
        };
        assert_eq!(write(&mut store, late), WriteOutcome::LeaseLost);
    }

    #[test]
    fn heartbeat_sets_details_and_complete_clears_them() {
        let mut store = OracleStore::new(1, Fencing::ClaimEpoch, STALE);
        let held = claim(&mut store, "w1", 0);
        let hb = Op::Heartbeat {
            claim: held.clone(),
            tag: 4,
        };
        assert_eq!(write(&mut store, hb), WriteOutcome::Applied);
        assert_eq!(store.rows()[0].heartbeat, Some(4));
        let done = Op::Complete {
            claim: held,
            tag: 5,
        };
        assert_eq!(write(&mut store, done), WriteOutcome::Applied);
        assert_eq!(store.rows()[0].heartbeat, None);
    }

    #[test]
    fn requeue_keeps_heartbeat_details() {
        let mut store = OracleStore::new(1, Fencing::ClaimEpoch, STALE);
        let held = claim(&mut store, "w1", 0);
        let hb = Op::Heartbeat {
            claim: held,
            tag: 4,
        };
        assert_eq!(write(&mut store, hb), WriteOutcome::Applied);
        reclaim(&mut store, 0);
        assert_eq!(store.rows()[0].heartbeat, Some(4));
        assert_eq!(store.rows()[0].worker, None);
    }

    #[test]
    fn fencing_names_round_trip() {
        for fencing in [Fencing::ClaimEpoch, Fencing::StateOnly] {
            assert_eq!(Fencing::parse(fencing.as_str()), Ok(fencing));
        }
        assert!(Fencing::parse("none").is_err());
    }

    #[test]
    fn outcome_display_is_short() {
        assert_eq!(Outcome::Beat.to_string(), "ok");
        assert_eq!(Outcome::Claimed(None).to_string(), "none");
        assert_eq!(
            Outcome::Claimed(Some(Claim {
                task: 0,
                worker: "w1".to_string(),
                attempt: 3,
            }))
            .to_string(),
            "claimed a3"
        );
        assert_eq!(Outcome::Write(WriteOutcome::Applied).to_string(), "applied");
        assert_eq!(
            Outcome::Write(WriteOutcome::LeaseLost).to_string(),
            "lease-lost"
        );
        assert_eq!(Outcome::Requeued(true).to_string(), "requeued");
        assert_eq!(Outcome::Requeued(false).to_string(), "kept");
        assert_eq!(Outcome::Orphans(Vec::new()).to_string(), "orphans []");
    }
}
