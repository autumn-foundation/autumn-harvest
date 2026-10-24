//! The mesh of the desk: three durable agents, their harness, and the
//! drive loop of the embedded SQLite runtime.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::durable_promise::PROMISE_SIGNAL_PREFIX;
use autumn_harvest_agent::delivery::{Delivery, Report};
use autumn_harvest_agent::mesh_control::{BudgetPeriod, MeshBudget, MeshSpend, Spend};
use autumn_harvest_agent::sqlite::{self, LedgerSpend};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentHeartbeat, AgentId, AgentModel, AgentOp, AgentProfile,
    AgentState, Approval, BoxFuture, Control, LiveEvent, LiveSink, Mandate, Peer, Rule, ToolEffect,
    ToolRules,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteResult, SqliteRuntime};

use crate::desk::Desk;

/// The id of the mesh. Every run of the desk carries it.
pub const MESH: &str = "desk";

/// The system prompt of the triage agent. The scripted model finds its
/// role by the first words.
pub const TRIAGE_SYSTEM: &str = "You are the triage agent of a support desk. On a scheduled \
check, read the inbox with check_inbox and report the open tickets. When a person asks you to \
answer a ticket, ask the researcher for the facts, ask the critic to check your draft, then file \
the reply with file_reply. Keep replies short and exact.";

/// The system prompt of the researcher.
pub const RESEARCHER_SYSTEM: &str = "You are the researcher of a support desk. Search the notes \
with search_notes and answer with the facts that you found, and the note that holds each one.";

/// The system prompt of the critic.
pub const CRITIC_SYSTEM: &str = "You are the critic of a support desk. Check a draft reply. \
Say whether it is correct and clear, and name each fix.";

/// The triage agent.
///
/// # Panics
///
/// Never: the id is a valid constant.
#[must_use]
pub fn triage() -> AgentId {
    AgentId::new("triage", "desk").expect("a valid agent id")
}

/// The researcher.
///
/// # Panics
///
/// Never: the id is a valid constant.
#[must_use]
pub fn researcher() -> AgentId {
    AgentId::new("researcher", "kb").expect("a valid agent id")
}

/// The critic.
///
/// # Panics
///
/// Never: the id is a valid constant.
#[must_use]
pub fn critic() -> AgentId {
    AgentId::new("critic", "qa").expect("a valid agent id")
}

/// Every agent of the mesh, in drive order.
#[must_use]
pub fn agents() -> [AgentId; 3] {
    [triage(), researcher(), critic()]
}

/// The settings of a mesh.
#[derive(Debug, Clone)]
pub struct MeshConfig {
    /// The time between two heartbeat ticks of the triage agent.
    pub heartbeat: Duration,
    /// The most tokens of the mesh in one day. `None` sets no budget.
    pub daily_tokens: Option<u64>,
    /// How long a reply waits for a person, on the desk clock. After that,
    /// the triage agent reads the reply as denied and files nothing.
    pub approval_timeout: Duration,
}

/// The default wait of a reply for a person: one day of desk clock.
///
/// The agent default is one hour. Each `tick` moves the desk clock 16
/// minutes, so four ticks would deny a reply that nobody saw yet.
pub const APPROVAL_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(15 * 60),
            daily_tokens: Some(200_000),
            approval_timeout: APPROVAL_TIMEOUT,
        }
    }
}

/// Writes each report to `reports.log` and prints it.
#[derive(Debug, Clone)]
pub struct DeskDelivery {
    desk: Desk,
    print: bool,
}

impl DeskDelivery {
    /// A delivery into `desk`. `print` also prints each report.
    #[must_use]
    pub const fn new(desk: Desk, print: bool) -> Self {
        Self { desk, print }
    }
}

impl Delivery for DeskDelivery {
    fn deliver<'a>(&'a self, report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            if self.print {
                println!("\n[report {:?}] {}", report.source, report.text);
            }
            self.desk
                .log_report(&format!("{:?}: {}", report.source, report.text))
                .map_err(|err| {
                    AgentError::new(
                        autumn_harvest_agent::ErrorKind::Unavailable,
                        format!("the report log failed: {err}"),
                    )
                })
        })
    }
}

/// Prints the live text of each model turn as it comes.
#[derive(Debug, Default)]
pub struct PrintSink;

impl LiveSink for PrintSink {
    fn send(&self, event: &LiveEvent) {
        use std::io::Write as _;
        match event {
            LiveEvent::Start { run_id, step } => print!("\n[{run_id} step {step}] "),
            LiveEvent::Delta { text, .. } => print!("{text}"),
        }
        let _ = std::io::stdout().flush();
    }
}

/// The harness of the mesh: the tools, the policy, the peers and the three
/// profiles.
///
/// The triage agent wakes on a heartbeat and streams its turns. A write
/// tool waits for a person. Each agent carries a mandate, so a peer message
/// can only narrow what the receiver may do. The mesh shares one daily
/// token budget, read from the cost ledger of the SQLite file.
#[must_use]
pub fn harness(
    desk: &Desk,
    model: Arc<dyn AgentModel>,
    config: &MeshConfig,
    live: Option<Arc<dyn LiveSink>>,
    delivery: Arc<dyn Delivery>,
) -> AgentHarness {
    let mut harness = AgentHarness::new(model)
        .tool(desk.check_inbox_tool())
        .tool(desk.search_notes_tool())
        .tool(desk.file_reply_tool())
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        // A peer entry narrows what a message may let its receiver do. The
        // researcher may only search. The critic may use no tool.
        .peer(
            Peer::agent(
                "researcher",
                "Searches the notes of the desk and answers with facts.",
                &researcher(),
            )
            .tools(["search_notes"]),
        )
        .peer(
            Peer::agent(
                "critic",
                "Checks a draft reply and names each fix.",
                &critic(),
            )
            .tools(Vec::<String>::new()),
        )
        .agent(
            AgentProfile::new("triage")
                .system(TRIAGE_SYSTEM)
                .heartbeat(AgentHeartbeat::every(config.heartbeat))
                .approval_timeout(config.approval_timeout)
                .mandate(
                    Mandate::new("triage")
                        .peers(["researcher", "critic"])
                        .tools(["check_inbox", "file_reply", "search_notes"]),
                )
                .mesh(MESH)
                .deliver()
                .stream(),
        )
        .agent(
            AgentProfile::new("researcher")
                .system(RESEARCHER_SYSTEM)
                .mandate(Mandate::new("researcher").tools(["search_notes"]))
                .mesh(MESH),
        )
        .agent(
            AgentProfile::new("critic")
                .system(CRITIC_SYSTEM)
                .mandate(Mandate::new("critic").tools(Vec::<String>::new()))
                .mesh(MESH),
        )
        .mesh_spend(Arc::new(LedgerSpend::open(desk.db())))
        .delivery(delivery);
    if let Some(max) = config.daily_tokens {
        harness = harness.mesh_budget(MeshBudget::daily(MESH).max_tokens(max));
    }
    if let Some(live) = live {
        harness = harness.live_sink(live);
    }
    harness
}

/// Open the SQLite file of `desk` and register `harness`.
///
/// # Errors
///
/// Returns an error when the file cannot be opened.
pub fn open(desk: &Desk, harness: AgentHarness) -> SqliteResult<SqliteRuntime> {
    let mut rt = SqliteRuntime::open(desk.db())?;
    sqlite::register(&mut rt, Arc::new(harness));
    Ok(rt)
}

/// What a drive of the mesh left behind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settled {
    /// The approval that the triage agent waits for, if any: its run and
    /// the signal name.
    pub approval: Option<(ExecutionId, String)>,
    /// Whether an agent waits in the middle of an exchange, as
    /// [`mid_exchange`] tells.
    pub in_exchange: bool,
}

/// Whether a drive that ended in `state` stopped in the middle of an
/// exchange.
///
/// The agent then waits for the reply of a peer or for a person. An idle
/// agent waits for its next op or its next tick, which is not an exchange.
#[must_use]
pub fn mid_exchange(state: &RunState) -> bool {
    match state {
        RunState::WaitingSignal(name) => {
            name.starts_with(PROMISE_SIGNAL_PREFIX)
                || name.starts_with(autumn_harvest_agent::approval::SIGNAL_TOOL_APPROVAL)
        }
        RunState::WaitingWorkflow(_) => true,
        _ => false,
    }
}

/// The run of `agent` and the length of its history: a mark of progress.
fn mark(rt: &SqliteRuntime, agent: &AgentId) -> SqliteResult<Option<(ExecutionId, usize)>> {
    let Some(exec) = sqlite::current_run(rt, agent)? else {
        return Ok(None);
    };
    Ok(Some((exec, rt.load_history(exec)?.len())))
}

/// Drive `agent` at the clock `at`, through each checkpoint.
///
/// # Errors
///
/// Returns an error when the runtime fails.
pub async fn drive_at(
    rt: &mut SqliteRuntime,
    agent: &AgentId,
    at: DateTime<Utc>,
) -> SqliteResult<Option<RunState>> {
    let Some(mut exec) = sqlite::current_run(rt, agent)? else {
        return Ok(None);
    };
    loop {
        match rt.run_until_blocked_as_of(exec, at).await? {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return Ok(Some(other)),
        }
    }
}

/// Drive every agent at the clock `at`, in turns, until no agent moves.
///
/// A peer message moves the receiver, and its reply moves the sender, so
/// one pass is not enough. Each pass drives the agents in `order`.
///
/// # Errors
///
/// Returns an error when the runtime fails.
pub async fn settle_in(
    rt: &mut SqliteRuntime,
    at: DateTime<Utc>,
    order: &[AgentId],
) -> SqliteResult<Settled> {
    let mut settled = Settled::default();
    for _ in 0..64 {
        let mut moved = false;
        settled.in_exchange = false;
        for agent in order {
            let before = mark(rt, agent)?;
            let state = drive_at(rt, agent, at).await?;
            settled.in_exchange |= state.as_ref().is_some_and(mid_exchange);
            if *agent == triage() {
                settled.approval = match (state, sqlite::current_run(rt, agent)?) {
                    (Some(RunState::WaitingSignal(name)), Some(exec))
                        if name
                            .starts_with(autumn_harvest_agent::approval::SIGNAL_TOOL_APPROVAL) =>
                    {
                        Some((exec, name))
                    }
                    _ => None,
                };
            }
            moved |= mark(rt, agent)? != before;
        }
        if !moved {
            break;
        }
    }
    Ok(settled)
}

/// [`settle_in`], in the default order.
///
/// # Errors
///
/// As [`settle_in`].
pub async fn settle(rt: &mut SqliteRuntime, at: DateTime<Utc>) -> SqliteResult<Settled> {
    settle_in(rt, at, &agents()).await
}

/// Start each agent that has no run, so a peer can reach it. A second
/// call sends nothing new: the op key is fixed.
///
/// # Errors
///
/// Returns an error when the runtime fails.
pub async fn start(rt: &mut SqliteRuntime, at: DateTime<Utc>) -> SqliteResult<Settled> {
    for agent in agents() {
        if sqlite::current_run(rt, &agent)?.is_none() {
            let key = format!("start:{agent}");
            sqlite::send(rt, &agent, &AgentOp::Control(Control::Resume), Some(&key))?;
        }
    }
    settle(rt, at).await
}

/// Ask the triage agent to answer `ticket`, once for each `key`.
///
/// # Errors
///
/// Returns an error when the runtime fails.
pub fn ask_answer(
    rt: &mut SqliteRuntime,
    ticket: &crate::desk::Ticket,
    key: &str,
) -> SqliteResult<()> {
    let text = format!(
        "Answer ticket {}: {}\n\n{}",
        ticket.id, ticket.subject, ticket.body
    );
    let op = AgentOp::message_with_id(text, key);
    sqlite::send(rt, &triage(), &op, Some(key))?;
    Ok(())
}

/// Send the decision `approval` to the wait `signal` of the run `exec`.
///
/// # Errors
///
/// Returns an error when the runtime fails.
pub fn decide(
    rt: &mut SqliteRuntime,
    exec: ExecutionId,
    signal: &str,
    approval: &Approval,
) -> SqliteResult<()> {
    sqlite::decide(rt, exec, signal, approval)
}

/// Every agent of the mesh, in a static. A state query then borrows no
/// local id.
static AGENT_IDS: LazyLock<[AgentId; 3]> = LazyLock::new(agents);

/// The committed state of each agent that runs.
///
/// The queries start at once, so the future holds no borrow of the runtime.
/// A runtime is not `Sync`, so a future that held one would not be `Send`.
/// Each query is large, so it waits on the heap.
///
/// # Errors
///
/// Returns an error when a query fails.
pub fn states(
    rt: &SqliteRuntime,
) -> impl Future<Output = SqliteResult<Vec<(AgentId, AgentState)>>> + Send + 'static {
    let queries: Vec<_> = AGENT_IDS
        .iter()
        .map(|agent| (agent.clone(), Box::pin(sqlite::state(rt, agent))))
        .collect();
    async move {
        let mut out = Vec::new();
        for (agent, query) in queries {
            if let Some(state) = query.await? {
                out.push((agent, state));
            }
        }
        Ok(out)
    }
}

/// What the mesh spent since the start of the day of `now`.
///
/// # Errors
///
/// Returns an error when the ledger cannot be read.
pub async fn spent_today(desk: &Desk, now: DateTime<Utc>) -> Result<Spend, AgentError> {
    let (since, _) = BudgetPeriod::Day.window(now);
    LedgerSpend::open(desk.db()).spent(MESH, since).await
}
