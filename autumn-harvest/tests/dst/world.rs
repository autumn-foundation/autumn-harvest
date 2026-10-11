//! The world driver of issue #2002, against a toy in-memory world.
//!
//! The Postgres world in `tests/integration/dst_world_tests.rs` runs the
//! real worker loop. These tests check the driver itself: the planner, the
//! clock, the invariants, the trace and the replay command.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use autumn_harvest::dst::SeedPlan;
use autumn_harvest::dst::world::{
    self, Effect, ExecFacts, Fact, Plant, Ran, Snapshot, World, WorldAction, WorldConfig,
    WorldInvariant,
};

/// A defect that the toy world can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bug {
    None,
    EarlyTimer,
    DoubleFire,
    EarlyFire,
    WrongOutput,
    Stuck,
    Blocked,
    TwoTerminals,
    TwoTimerFires,
    TwoActivityResults,
    StatusWithoutEvent,
    FireWithoutRun,
}

/// One toy execution: a fixed script of facts.
#[derive(Debug, Clone)]
struct Toy {
    label: String,
    status: &'static str,
    blocked: bool,
    events: Vec<Fact>,
    step: usize,
    timer_at: u64,
    signalled: bool,
    seen_by: Vec<usize>,
    expected: serde_json::Value,
}

/// An in-memory world with chains of five decisions and one schedule.
struct ToyWorld {
    bug: Bug,
    now: u64,
    execs: Vec<Toy>,
    next_slot: u64,
    held: BTreeMap<usize, u64>,
    runs: usize,
    max_runs: usize,
    incarnation: Vec<u32>,
    abandoned: u64,
}

/// Toy executions start once per process, so this counter differs between
/// the two runs of a seed. Only the nondeterministic toy reads it.
static GLOBAL_STARTS: AtomicU64 = AtomicU64::new(0);

impl ToyWorld {
    fn new(config: &WorldConfig, bug: Bug) -> Self {
        let execs = (0..config.workflows)
            .map(|i| Toy {
                label: format!("c{i}"),
                status: "RUNNING",
                blocked: false,
                events: vec![Fact::Started],
                step: 0,
                timer_at: 0,
                signalled: false,
                seen_by: Vec::new(),
                expected: serde_json::json!({ "result": i }),
            })
            .collect();
        Self {
            bug,
            now: 0,
            execs,
            next_slot: 2,
            held: BTreeMap::new(),
            runs: 0,
            max_runs: 2,
            incarnation: vec![1; config.workers],
            abandoned: 0,
        }
    }

    fn runnable(&self, toy: &Toy) -> bool {
        match toy.step {
            0..=1 => true,
            2 => self.bug == Bug::EarlyTimer || self.now >= toy.timer_at,
            3 => toy.signalled,
            4 => self.bug != Bug::Stuck || toy.label != "c0",
            _ => false,
        }
    }

    fn poll(&mut self, w: usize) -> Effect {
        let Some(index) = (0..self.execs.len()).find(|&i| self.runnable(&self.execs[i])) else {
            return Effect::Idle;
        };
        let now = self.now;
        let bug = self.bug;
        let worker = w * 100 + self.incarnation[w] as usize;
        let toy = &mut self.execs[index];
        let decision = if toy.seen_by.contains(&worker) {
            Ran::Warm
        } else if toy.seen_by.is_empty() {
            Ran::Cold
        } else {
            Ran::Declined
        };
        toy.seen_by.push(worker);
        match toy.step {
            0 => toy.events.push(Fact::ActivityScheduled {
                activity: 0,
                name: "add".to_string(),
            }),
            1 => {
                toy.events.push(Fact::ActivityCompleted { activity: 0 });
                if bug == Bug::TwoActivityResults {
                    toy.events.push(Fact::ActivityCompleted { activity: 0 });
                }
                toy.events.push(Fact::TimerStarted {
                    timer: "nap".to_string(),
                    secs: world::TICK_SECS,
                });
                toy.timer_at = now + 1;
            }
            2 => {
                toy.events.push(Fact::TimerFired {
                    timer: "nap".to_string(),
                });
                if bug == Bug::TwoTimerFires {
                    toy.events.push(Fact::TimerFired {
                        timer: "nap".to_string(),
                    });
                }
            }
            3 => toy.events.push(Fact::Signal {
                name: "go".to_string(),
            }),
            _ => {
                let output = if bug == Bug::WrongOutput {
                    serde_json::json!("wrong")
                } else {
                    toy.expected.clone()
                };
                if bug == Bug::Blocked && toy.label == "c1" {
                    toy.blocked = true;
                    return Effect::Polled(decision);
                }
                if bug == Bug::StatusWithoutEvent {
                    toy.status = "COMPLETED";
                    toy.step += 1;
                    return Effect::Polled(decision);
                }
                toy.events.push(Fact::Completed {
                    output: output.to_string(),
                });
                if bug == Bug::TwoTerminals {
                    toy.events.push(Fact::Completed {
                        output: output.to_string(),
                    });
                }
                toy.status = "COMPLETED";
            }
        }
        toy.step += 1;
        Effect::Polled(decision)
    }

    fn fire(&mut self, s: usize) -> Effect {
        let Some(slot) = self.held.remove(&s) else {
            return Effect::Fired(None);
        };
        let fresh = slot == self.next_slot;
        if !(fresh || self.bug == Bug::DoubleFire) || self.runs >= self.max_runs {
            return Effect::Fired(None);
        }
        let slot = if self.bug == Bug::EarlyFire {
            self.now + 1
        } else {
            slot
        };
        if fresh {
            self.next_slot += 2;
        }
        if self.bug == Bug::FireWithoutRun {
            self.runs += 1;
            return Effect::Fired(Some(slot));
        }
        let expected = serde_json::json!({ "result": 100 });
        self.execs.push(Toy {
            label: format!("s{}", self.runs),
            status: "COMPLETED",
            blocked: false,
            events: vec![
                Fact::Started,
                Fact::Completed {
                    output: expected.to_string(),
                },
            ],
            step: 9,
            timer_at: 0,
            signalled: false,
            seen_by: Vec::new(),
            expected,
        });
        self.runs += 1;
        Effect::Fired(Some(slot))
    }
}

impl ToyWorld {
    fn step(&mut self, action: WorldAction, now_tick: u64) -> Effect {
        self.now = now_tick;
        match action {
            WorldAction::Poll(w) => self.poll(w),
            WorldAction::Signal(i) => {
                self.execs[i].signalled = true;
                Effect::Done
            }
            WorldAction::Restart(w) => {
                self.incarnation[w] += 1;
                Effect::Done
            }
            WorldAction::ScheduleScan(s) => {
                if self.now >= self.next_slot && self.runs < self.max_runs {
                    self.held.insert(s, self.next_slot);
                    Effect::Scanned(1)
                } else {
                    Effect::Scanned(0)
                }
            }
            WorldAction::ScheduleFire(s) => self.fire(s),
            WorldAction::Abandon(_) => {
                self.abandoned += 1;
                Effect::Done
            }
            WorldAction::Reclaim => Effect::Reclaimed(std::mem::take(&mut self.abandoned)),
            WorldAction::Sweep => Effect::Swept(0),
            WorldAction::Advance
            | WorldAction::Beat(_)
            | WorldAction::Stall(_)
            | WorldAction::Crash(_) => Effect::Done,
        }
    }

    fn view(&self) -> Snapshot {
        Snapshot {
            executions: self
                .execs
                .iter()
                .map(|toy| ExecFacts {
                    label: toy.label.clone(),
                    status: toy.status.to_string(),
                    blocked: toy.blocked,
                    expected: Some(toy.expected.clone()),
                    events: toy.events.clone(),
                })
                .collect(),
            schedules_done: self.runs >= self.max_runs,
        }
    }
}

impl World for ToyWorld {
    fn apply(&mut self, action: WorldAction, now_tick: u64) -> impl Future<Output = Effect> {
        std::future::ready(self.step(action, now_tick))
    }

    fn snapshot(&mut self) -> impl Future<Output = Snapshot> {
        std::future::ready(self.view())
    }
}

/// A toy world whose first execution depends on a process-wide counter.
struct FlakyWorld(ToyWorld);

impl World for FlakyWorld {
    fn apply(&mut self, action: WorldAction, now_tick: u64) -> impl Future<Output = Effect> {
        std::future::ready(self.0.step(action, now_tick))
    }

    fn snapshot(&mut self) -> impl Future<Output = Snapshot> {
        let mut snapshot = self.0.view();
        let starts = GLOBAL_STARTS.fetch_add(1, Ordering::SeqCst);
        if starts == 0 {
            snapshot.executions[0]
                .events
                .push(Fact::Other("Extra".to_string()));
        }
        std::future::ready(snapshot)
    }
}

fn toy_config(seed: u64) -> WorldConfig {
    WorldConfig {
        workflows: 2,
        fault_steps: 60,
        drain_steps: 300,
        ..WorldConfig::new(seed)
    }
}

async fn run_toy(config: &WorldConfig, bug: Bug) -> world::WorldReport {
    world::run(config, ToyWorld::new(config, bug)).await
}

#[tokio::test]
async fn a_toy_sweep_converges_and_covers_every_action() {
    let mut stats = world::WorldStats::default();
    for seed in 0..16 {
        let config = toy_config(seed);
        let report = run_toy(&config, Bug::None).await;
        assert_eq!(report.violation, None, "{}", report.trace_tail(30));
        assert!(report.converged, "seed {seed}: {}", report.trace_tail(30));
        stats.merge(&report.stats);
    }
    assert!(
        stats.cold > 0 && stats.warm > 0 && stats.declined > 0,
        "{stats:?}"
    );
    assert!(stats.timers_fired > 0 && stats.signals > 0, "{stats:?}");
    assert!(stats.fires > 0 && stats.lost_fires > 0, "{stats:?}");
    assert!(stats.stalls > 0 && stats.crashes > 0, "{stats:?}");
    assert!(stats.abandons > 0 && stats.reclaimed > 0, "{stats:?}");
    assert!(stats.advances > 0 && stats.idle_polls > 0, "{stats:?}");
}

/// FNV-1a over the trace text.
fn trace_hash(report: &world::WorldReport) -> u64 {
    report
        .trace
        .join("\n")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

/// The driver traces of a few toy seeds are fixed.
///
/// A seed that a nightly run reports must replay on a later commit and on
/// every platform. A change to the planner, its weights or its draws
/// changes these values on purpose. Update them in the same change.
#[tokio::test]
async fn golden_world_traces_are_equal_on_every_platform() {
    let golden = [
        (0, 86, 0xf10b_2b92_93ad_b137),
        (1, 86, 0x14b4_c534_1177_e2c2),
        (2, 86, 0xf9b7_d969_c2c4_cc6b),
    ];
    for (seed, lines, hash) in golden {
        let report = run_toy(&toy_config(seed), Bug::None).await;
        assert_eq!(
            (report.trace.len(), trace_hash(&report)),
            (lines, hash),
            "seed {seed}: the trace changed"
        );
    }
}

#[tokio::test]
async fn each_invariant_catches_its_bug() {
    let cases = [
        (Bug::EarlyTimer, WorldInvariant::TimerNotEarly),
        (Bug::DoubleFire, WorldInvariant::ScheduleSlotOnce),
        (Bug::EarlyFire, WorldInvariant::ScheduleNotEarly),
        (Bug::WrongOutput, WorldInvariant::ExpectedOutput),
        (Bug::Stuck, WorldInvariant::Converges),
        (Bug::Blocked, WorldInvariant::Deterministic),
        (Bug::TwoTerminals, WorldInvariant::OneTerminal),
        (Bug::TwoTimerFires, WorldInvariant::TimerFiresOnce),
        (Bug::TwoActivityResults, WorldInvariant::ActivityResultOnce),
        (
            Bug::StatusWithoutEvent,
            WorldInvariant::StatusMatchesHistory,
        ),
        (Bug::FireWithoutRun, WorldInvariant::FireStartsRun),
    ];
    for (bug, expected) in cases {
        let mut found = None;
        for seed in 0..32 {
            let report = run_toy(&toy_config(seed), bug).await;
            if let Some(violation) = report.violation {
                found = Some(violation);
                break;
            }
        }
        let violation = found.unwrap_or_else(|| panic!("{bug:?} found no violation"));
        assert_eq!(violation.invariant, expected, "{bug:?}: {violation}");
    }
}

#[tokio::test]
async fn an_unchecked_invariant_does_not_stop_the_run() {
    let config = toy_config(0).checking(&[WorldInvariant::OneTerminal]);
    let report = run_toy(&config, Bug::WrongOutput).await;
    assert_eq!(report.violation, None, "{}", report.trace_tail(20));
}

#[tokio::test]
async fn equal_seeds_give_equal_traces_and_distinct_seeds_differ() {
    let a = run_toy(&toy_config(5), Bug::None).await;
    let b = run_toy(&toy_config(5), Bug::None).await;
    let c = run_toy(&toy_config(6), Bug::None).await;
    assert_eq!(a, b);
    assert_ne!(a.trace, c.trace);
}

#[tokio::test]
async fn the_trace_names_the_step_the_tick_and_each_new_fact() {
    let report = run_toy(&toy_config(1), Bug::None).await;
    let first = &report.trace[0];
    assert!(first.starts_with("0000 t=000 "), "{first}");
    assert!(
        report
            .trace
            .iter()
            .any(|line| line.contains(&format!("c0 + TimerStarted nap {}s", world::TICK_SECS))),
        "{}",
        report.trace.join("\n")
    );
    assert!(
        report
            .trace
            .iter()
            .any(|line| line.contains("c0 status COMPLETED")),
        "{}",
        report.trace.join("\n")
    );
    let advances = report
        .trace
        .iter()
        .filter(|l| l.contains(" advance "))
        .count();
    assert_eq!(advances as u64, report.stats.advances);
    let last_tick = report.trace.last().map(|line| line[7..10].to_string());
    assert_eq!(last_tick, Some(format!("{:03}", report.stats.advances)));
}

#[tokio::test]
async fn the_drain_phase_injects_no_fault() {
    for seed in 0..8 {
        let config = toy_config(seed);
        let report = run_toy(&config, Bug::None).await;
        let late_faults = report
            .trace
            .iter()
            .filter(|line| line[..4].parse::<usize>().unwrap_or(0) >= config.fault_steps)
            .filter(|line| {
                line.contains(" stall ") || line.contains(" crash ") || line.contains(" abandon ")
            })
            .count();
        assert_eq!(late_faults, 0, "{}", report.trace.join("\n"));
    }
}

#[tokio::test]
async fn run_twice_reports_a_nondeterministic_world() {
    let config = toy_config(2);
    GLOBAL_STARTS.store(0, Ordering::SeqCst);
    let error = world::run_twice(&config, async || {
        FlakyWorld(ToyWorld::new(&config, Bug::None))
    })
    .await
    .expect_err("the traces differ");
    assert_eq!(error.seed, 2);
    assert!(error.to_string().contains("not deterministic"), "{error}");
}

#[tokio::test]
async fn a_sweep_failure_prints_a_replay_command_that_rebuilds_the_config() {
    let plan = SeedPlan {
        first: 0,
        count: 32,
    };
    let failure = world::sweep(
        &plan,
        |seed| toy_config(seed).with_plant(Plant::ForeignState),
        async |config: &WorldConfig| ToyWorld::new(config, Bug::WrongOutput),
    )
    .await
    .expect_err("the toy bug fails a seed");
    let text = failure.to_string();
    let seed = failure.config.seed;
    assert!(
        text.contains(&format!("HARVEST_DST_SEED={seed} ")),
        "{text}"
    );
    assert!(
        text.contains("HARVEST_DST_WORLD_PLANT=foreign-state"),
        "{text}"
    );
    assert!(
        text.contains("HARVEST_DST_WORLD_CHECKS=OneTerminal,"),
        "{text}"
    );
    assert!(
        text.contains("dst_world_tests::replay_one_world_seed"),
        "{text}"
    );
    assert!(text.contains("ExpectedOutput"), "{text}");

    let checks = world::checks_arg(&failure.config.checks);
    let rebuilt =
        world::config_from_vars(seed, Some("foreign-state"), Some(&checks)).expect("valid");
    assert_eq!(rebuilt.plant, Plant::ForeignState);
    assert_eq!(rebuilt.checks, failure.config.checks);
    assert_eq!(rebuilt.seed, seed);
}

#[test]
fn plant_and_invariant_names_round_trip() {
    for plant in [Plant::None, Plant::ForeignState] {
        assert_eq!(Plant::parse(plant.as_str()), Ok(plant));
    }
    assert!(Plant::parse("chaos").is_err());
    for invariant in WorldInvariant::ALL {
        assert_eq!(WorldInvariant::parse(invariant.name()), Ok(invariant));
    }
    assert!(WorldInvariant::parse("Nope").is_err());
    assert_eq!(
        world::config_from_vars(3, None, None),
        Ok(WorldConfig::new(3))
    );
    assert!(world::config_from_vars(3, Some("chaos"), None).is_err());
}

#[test]
fn facts_from_events_number_activities_by_first_appearance() {
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::types::{ActivityExecId, TimerId};

    let first = ActivityExecId::new();
    let second = ActivityExecId::new();
    let scheduled = |id: ActivityExecId| WorkflowEvent::ActivityScheduled {
        activity_id: id,
        name: "add".to_string(),
        input: serde_json::json!({}),
        queue: "q".to_string(),
    };
    let events = vec![
        scheduled(second),
        scheduled(first),
        WorkflowEvent::ActivityCompleted {
            activity_id: first,
            output: serde_json::json!(1),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("nap"),
            duration_secs: 7_200,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("nap"),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "go".to_string(),
            payload: serde_json::json!(null),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "result": 3 }),
        },
    ];
    let facts = Fact::from_events(&events);
    assert_eq!(
        facts,
        vec![
            Fact::ActivityScheduled {
                activity: 0,
                name: "add".to_string()
            },
            Fact::ActivityScheduled {
                activity: 1,
                name: "add".to_string()
            },
            Fact::ActivityCompleted { activity: 1 },
            Fact::TimerStarted {
                timer: "nap".to_string(),
                secs: 7_200
            },
            Fact::TimerFired {
                timer: "nap".to_string()
            },
            Fact::Signal {
                name: "go".to_string()
            },
            Fact::Completed {
                output: r#"{"result":3}"#.to_string()
            },
        ]
    );
    assert_eq!(facts[3].to_string(), "TimerStarted nap 7200s");
}
