//! A seeded simulation of the desk mesh (issue #2276).
//!
//! Each seed runs the mesh on SQLite under a schedule that the seed picks.
//! The nemeses are:
//!
//! - **restart:** the runtime drops and a new one opens the file, as after a
//!   crash between two steps or a change of owner;
//! - **reorder:** the agents run in a seeded order, one step at a time, so a
//!   reply can land before or after other work;
//! - **duplicate:** an op is sent again with the same key;
//! - **clock:** the clock jumps, so heartbeats and approval deadlines fire;
//! - **reckless model:** in a scheduled check, the model tries a write.
//!
//! After the schedule, the mesh runs to rest and every reply is decided.
//! Four invariants must hold on every seed:
//!
//! 1. No lost and no duplicate message. The triage agent applies each op key
//!    once. Each peer runs each message that it was sent exactly once.
//! 2. No widened mandate: each message lets its receiver do less than its
//!    sender.
//! 3. No budget overrun: the spend of each day passes the cap by one model
//!    turn at most, because the check runs before each turn.
//! 4. No write in an unattended tick: only an approved answer files a reply.
//!
//! Each nemesis must fire on some seed, so the sweep cannot pass by never
//! reaching the hard cases. Set `AGENT_MESH_SEED` to run one seed, or
//! `AGENT_MESH_SEEDS` and `AGENT_MESH_SEED_BASE` for a range.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_mesh::desk::Desk;
use agent_mesh::mesh::{self, DeskDelivery, MeshConfig};
use agent_mesh::scripted::{ScriptedDesk, TOKENS_PER_CALL, last_user};
use autumn_harvest::chrono::{DateTime, Duration, Utc};
use autumn_harvest::dst::{SeedPlan, SeedVars, SplitMix64};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest_agent::approval::SIGNAL_TOOL_APPROVAL;
use autumn_harvest_agent::{
    AgentError, AgentId, AgentModel, AgentStatus, Approval, BoxFuture, ChatRequest, ChatResponse,
    ChatRole, ContentPart, PeerEnvelope, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};

const VARS: SeedVars = SeedVars {
    seed: "AGENT_MESH_SEED",
    seeds: "AGENT_MESH_SEEDS",
    base: "AGENT_MESH_SEED_BASE",
};

/// Enough seeds to reach every nemesis, few enough for each CI run.
const DEFAULT_SEEDS: u64 = 24;

/// The steps of one schedule.
const STEPS: u32 = 30;

/// The reckless model. It counts the writes that it tried in a check, and
/// the first turn of each answer op, by ticket.
#[derive(Debug, Default)]
struct Counted {
    inner: ScriptedDesk,
    tick_writes: AtomicUsize,
    answers: Mutex<BTreeMap<String, usize>>,
}

impl AgentModel for Counted {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let response = self.inner.answer(request);
        // The last user message is the op that runs. The transcript of the
        // agent also holds the earlier ops.
        let user = last_user(request);
        let writes = response
            .content
            .iter()
            .any(|part| matches!(part, ContentPart::ToolCall { name, .. } if name == "file_reply"));
        if user.starts_with("This is a scheduled check") && writes {
            self.tick_writes.fetch_add(1, Ordering::SeqCst);
        }
        // A first turn ends with the user message. A later turn ends with a
        // tool result.
        let first_turn = request
            .messages
            .last()
            .is_some_and(|message| message.role == ChatRole::User);
        if let Some(rest) = user.strip_prefix("Answer ticket ")
            && first_turn
        {
            let ticket = rest.split(':').next().unwrap_or_default().to_owned();
            *self.answers.lock().unwrap().entry(ticket).or_default() += 1;
        }
        Box::pin(async move { Ok(response) })
    }
}

/// What one seed saw. The sweep checks reachability over all seeds.
///
/// Each flag is one reachability check, so the flags stay plain bools.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default, Clone, Copy)]
struct Seen {
    restart_in_flight: bool,
    reorder_in_flight: bool,
    duplicate: bool,
    heartbeat_on_jump: bool,
    deadline: bool,
    approved: bool,
    rejected: bool,
    parked: bool,
    tick_write: bool,
}

impl Seen {
    const fn merge(&mut self, other: Self) {
        self.restart_in_flight |= other.restart_in_flight;
        self.reorder_in_flight |= other.reorder_in_flight;
        self.duplicate |= other.duplicate;
        self.heartbeat_on_jump |= other.heartbeat_on_jump;
        self.deadline |= other.deadline;
        self.approved |= other.approved;
        self.rejected |= other.rejected;
        self.parked |= other.parked;
        self.tick_write |= other.tick_write;
    }
}

struct World {
    desk: Desk,
    model: Arc<Counted>,
    config: MeshConfig,
    rt: Option<SqliteRuntime>,
    /// Whether the last drive left an agent in the middle of an exchange.
    /// The state is durable, so a restart keeps it.
    in_exchange: bool,
    /// Whether a reply waits for a decision that nobody sent yet.
    awaiting_approval: bool,
}

impl World {
    fn rt(&mut self) -> &mut SqliteRuntime {
        if self.rt.is_none() {
            let harness = mesh::harness(
                &self.desk,
                self.model.clone(),
                &self.config,
                None,
                Arc::new(DeskDelivery::new(self.desk.clone(), false)),
            );
            self.rt = Some(mesh::open(&self.desk, harness).unwrap());
        }
        self.rt.as_mut().unwrap()
    }
}

/// Every event of `agent`, in every run of its lives.
fn history(rt: &SqliteRuntime, agent: &AgentId) -> Vec<WorkflowEvent> {
    let filter = autumn_harvest_sqlite::ExecutionFilter::new()
        .workflow_name(autumn_harvest_agent::AGENT_WORKFLOW_NAME)
        .workflow_id(agent.as_str());
    let mut out = Vec::new();
    for summary in rt.list_executions(&filter).unwrap().executions {
        if let Some(exec) = summary.execution_id() {
            out.extend(rt.load_history(exec).unwrap());
        }
    }
    out
}

/// The ops of `kind` that `agent` received, as their wire form.
fn received_ops(rt: &SqliteRuntime, agent: &AgentId, kind: &str) -> Vec<serde_json::Value> {
    history(rt, agent)
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::SignalReceived { payload, .. } if payload["op"]["type"] == kind => {
                Some(payload["op"].clone())
            }
            _ => None,
        })
        .collect()
}

/// The peer messages that `agent` received.
fn received(rt: &SqliteRuntime, agent: &AgentId) -> Vec<PeerEnvelope> {
    received_ops(rt, agent, "peer_message")
        .into_iter()
        .map(|op| serde_json::from_value(op).unwrap())
        .collect()
}

/// Whether the mesh still has work: a parked agent or a held op.
fn has_work(rt: &SqliteRuntime) -> impl Future<Output = bool> + Send + 'static {
    let states = mesh::states(rt);
    async move {
        states.await.unwrap().iter().any(|(_, state)| {
            matches!(state.status, AgentStatus::Parked { .. }) || !state.held.is_empty()
        })
    }
}

/// The ticks of the triage agent.
fn ticks(rt: &SqliteRuntime) -> impl Future<Output = u64> + Send + 'static {
    let states = mesh::states(rt);
    async move {
        states
            .await
            .unwrap()
            .into_iter()
            .find(|(agent, _)| *agent == mesh::triage())
            .map_or(0, |(_, state)| state.ticks)
    }
}

/// Run one seed. Panics with the seed when an invariant fails.
#[allow(clippy::too_many_lines)]
async fn run_seed(seed: u64) -> Seen {
    let mut rng = SplitMix64::new(seed);
    let dir = tempfile::tempdir().unwrap();
    let desk = Desk::new(dir.path().join("desk"));
    desk.init().unwrap();
    // A small cap holds six model turns. One answer takes seven, so the
    // budget can cut an answer in the middle.
    let cap = if rng.chance(30) {
        u64::from(TOKENS_PER_CALL) * 6
    } else {
        200_000
    };
    let mut world = World {
        desk: desk.clone(),
        model: Arc::new(Counted {
            inner: ScriptedDesk::reckless(),
            ..Counted::default()
        }),
        config: MeshConfig {
            daily_tokens: Some(cap),
            ..MeshConfig::default()
        },
        rt: None,
        in_exchange: false,
        awaiting_approval: false,
    };
    let mut reached = Seen::default();
    let mut clock: DateTime<Utc> = "2026-10-23T08:00:00Z".parse().unwrap();
    let mut asked: BTreeSet<String> = BTreeSet::new();
    let mut approved = 0_usize;
    world.in_exchange = mesh::start(world.rt(), clock).await.unwrap().in_exchange;

    for _ in 0..STEPS {
        match rng.below(6) {
            0 => {
                // A restart. It counts when an agent waits for a reply or
                // for a person.
                reached.restart_in_flight |= world.in_exchange;
                world.rt = None;
            }
            1 => {
                // Most jumps are short, so a reply still waits for its
                // decision. A long jump passes the approval deadline. It
                // comes more often while a reply waits.
                let long = rng.chance(if world.awaiting_approval { 75 } else { 5 });
                clock += if long {
                    Duration::from_std(world.config.approval_timeout).unwrap() + Duration::hours(1)
                } else {
                    Duration::minutes(i64::try_from(1 + rng.below(40)).unwrap())
                };
                let before = ticks(world.rt()).await;
                let order = shuffled(&mut rng);
                reached.reorder_in_flight |= world.in_exchange && order != mesh::agents();
                let settled = mesh::settle_in(world.rt(), clock, &order).await.unwrap();
                world.in_exchange = settled.in_exchange;
                world.awaiting_approval = settled.approval.is_some();
                reached.heartbeat_on_jump |= ticks(world.rt()).await > before;
            }
            2 => {
                // Ask for an answer. A key repeats on some seeds.
                let ticket = if rng.chance(50) { "T-1" } else { "T-2" };
                let key = format!("answer:{ticket}:{}", rng.below(2));
                reached.duplicate |= !asked.insert(key.clone());
                let ticket = desk.ticket(ticket).unwrap().unwrap();
                mesh::ask_answer(world.rt(), &ticket, &key).unwrap();
            }
            3 => {
                // One agent takes one step. Replies can land out of order.
                let agents = mesh::agents();
                let agent = &agents[usize::try_from(rng.below(3)).unwrap()];
                reached.reorder_in_flight |= world.in_exchange;
                let state = mesh::drive_at(world.rt(), agent, clock).await.unwrap();
                // Only the triage agent waits on a peer or a person.
                if *agent == mesh::triage() {
                    world.in_exchange = state.as_ref().is_some_and(mesh::mid_exchange);
                    world.awaiting_approval = matches!(
                        &state,
                        Some(RunState::WaitingSignal(name)) if name.starts_with(SIGNAL_TOOL_APPROVAL)
                    );
                }
            }
            _ => {
                let order = shuffled(&mut rng);
                reached.reorder_in_flight |= world.in_exchange && order != mesh::agents();
                let settled = mesh::settle_in(world.rt(), clock, &order).await.unwrap();
                world.in_exchange = settled.in_exchange;
                world.awaiting_approval = settled.approval.is_some();
                // On some steps the person is away, and the reply waits.
                if let Some((exec, signal)) = settled.approval.filter(|_| rng.chance(70)) {
                    world.awaiting_approval = false;
                    let decision = if rng.chance(70) {
                        approved += 1;
                        Approval::Approve
                    } else {
                        reached.rejected = true;
                        Approval::Reject {
                            reason: "Not yet.".into(),
                        }
                    };
                    mesh::decide(world.rt(), exec, &signal, &decision).unwrap();
                }
            }
        }
        let parked = mesh::states(world.rt())
            .await
            .unwrap()
            .iter()
            .any(|(_, state)| state.status.name() == "parked");
        reached.parked |= parked;
    }

    // Run to rest. Each reply is approved before the clock moves, so no
    // deadline denies it. A new day opens any spent budget.
    for _ in 0..64 {
        let settled = mesh::settle(world.rt(), clock).await.unwrap();
        if let Some((exec, signal)) = settled.approval {
            approved += 1;
            mesh::decide(world.rt(), exec, &signal, &Approval::Approve).unwrap();
            continue;
        }
        if !has_work(world.rt()).await {
            break;
        }
        clock += Duration::days(1);
    }
    let rt = world.rt();
    assert!(
        !has_work(rt).await,
        "seed {seed}: the mesh did not come to rest"
    );

    // Invariant 1: the triage agent got each op key once and applied it
    // once. A first turn that the budget refuses calls no model and holds
    // the op, so each op has exactly one first model turn.
    let delivered: Vec<String> = received_ops(rt, &mesh::triage(), "user_message")
        .iter()
        .map(|op| op["message_id"].as_str().unwrap_or_default().to_owned())
        .collect();
    let distinct: BTreeSet<&String> = delivered.iter().collect();
    assert_eq!(
        delivered.len(),
        distinct.len(),
        "seed {seed}: an op key arrived twice: {delivered:?}"
    );
    assert_eq!(
        distinct.into_iter().cloned().collect::<BTreeSet<_>>(),
        asked,
        "seed {seed}: the delivered keys are not the asked keys"
    );
    let mut per_ticket: BTreeMap<String, usize> = BTreeMap::new();
    for key in &asked {
        let ticket = key.split(':').nth(1).unwrap().to_owned();
        *per_ticket.entry(ticket).or_default() += 1;
    }
    assert_eq!(
        *world.model.answers.lock().unwrap(),
        per_ticket,
        "seed {seed}: the first answer turns do not match the asked keys"
    );
    let rt = world.rt();
    let triage = sqlite::state(rt, &mesh::triage()).await.unwrap().unwrap();
    for peer in [mesh::researcher(), mesh::critic()] {
        let state = sqlite::state(rt, &peer).await.unwrap().unwrap();
        let sent = triage.outbound_seq.get(peer.as_str()).copied().unwrap_or(0);
        // Invariant 1: each message ran once, none lost, none twice.
        assert_eq!(
            state.segments, sent,
            "seed {seed}: {peer} ran {} messages of {sent} sent",
            state.segments
        );
        // Invariant 2: a message never widens what its receiver may do.
        for envelope in received(rt, &peer) {
            for tool in ["check_inbox", "file_reply"] {
                assert!(
                    !envelope.mandate.allows_tool(tool),
                    "seed {seed}: {peer} got {tool} from {}",
                    envelope.from
                );
            }
        }
    }
    // Invariant 3: the check runs before each turn, so a day passes the cap
    // by one turn at most. Each row carries the drive clock of its turn.
    // Every run of the file is an agent of the desk mesh.
    let mut day: DateTime<Utc> = "2026-10-23T00:00:00Z".parse().unwrap();
    while day <= clock {
        let tokens: u64 = rt
            .llm_ledger_between(day, day + Duration::days(1))
            .unwrap()
            .iter()
            .map(|row| row.input_tokens + row.output_tokens)
            .sum();
        assert!(
            tokens <= cap + u64::from(TOKENS_PER_CALL),
            "seed {seed}: {tokens} tokens on {day}, cap {cap}"
        );
        day += Duration::days(1);
    }
    // Invariant 4: no tick filed a reply. Each run of the tool follows an
    // approval.
    let filed = desk.filed_log();
    assert!(
        filed.len() <= approved,
        "seed {seed}: {} replies filed, {approved} approved",
        filed.len()
    );
    for ticket in desk.tickets().unwrap() {
        assert!(
            !desk
                .reply(&ticket.id)
                .unwrap_or_default()
                .contains("An unattended reply"),
            "seed {seed}: a tick filed a reply to {}",
            ticket.id
        );
    }
    reached.approved = !filed.is_empty();
    reached.deadline = history(rt, &mesh::triage()).iter().any(|event| {
        serde_json::to_string(event)
            .unwrap()
            .contains("no approval arrived")
    });
    reached.tick_write = world.model.tick_writes.load(Ordering::SeqCst) > 0;
    reached
}

fn shuffled(rng: &mut SplitMix64) -> Vec<AgentId> {
    let mut order = mesh::agents().to_vec();
    for i in (1..order.len()).rev() {
        let j = usize::try_from(rng.below(u64::try_from(i + 1).unwrap())).unwrap();
        order.swap(i, j);
    }
    order
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mesh_keeps_its_four_invariants_under_every_nemesis() {
    let plan = SeedPlan::parse_named(
        VARS,
        std::env::var(VARS.seed).ok().as_deref(),
        std::env::var(VARS.seeds).ok().as_deref(),
        std::env::var(VARS.base).ok().as_deref(),
        DEFAULT_SEEDS,
    )
    .unwrap();
    let mut any = Seen::default();
    for seed in plan.first..plan.first + plan.count {
        any.merge(run_seed(seed).await);
    }
    if plan.count >= DEFAULT_SEEDS {
        assert!(any.restart_in_flight, "no seed restarted mid-exchange");
        assert!(any.reorder_in_flight, "no seed reordered mid-exchange");
        assert!(any.duplicate, "no seed sent a duplicate");
        assert!(any.heartbeat_on_jump, "no clock jump fired a heartbeat");
        assert!(any.deadline, "no clock jump passed an approval deadline");
        assert!(any.approved, "no seed filed an approved reply");
        assert!(any.rejected, "no seed rejected a reply");
        assert!(any.parked, "no seed spent its budget");
        assert!(any.tick_write, "no tick tried a write");
    }
}
