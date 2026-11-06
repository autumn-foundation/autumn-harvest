//! The refine loop of an agent kind on SQLite, end to end. It needs no API
//! key and no network, because scripted models stand in.
//!
//! 1. Op 1 is a recorded `agent_loop` run of the kind `support`. Its prompt
//!    lacks the refund rule, so its answer fails the check.
//! 2. `agent_refine` with k = 1 reads op 1. It proposes an overlay that
//!    adds the rule, and probes it against the recorded op.
//! 3. The run waits at the adoption gate. The example approves it.
//! 4. Op 2 goes to a durable agent of the kind. It reads the adopted
//!    version, so its answer passes the check.
//!
//! The example exits non-zero when op 2 fails. The refine workflow needs
//! the `eval` feature.
//!
//! ```sh
//! cargo run -p autumn-harvest-agent --features sqlite,eval --example agent_refine
//! ```
//!
//! The guide is section 25 of `docs/agent-adapter.md`.

use std::error::Error;
use std::sync::Arc;

use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::{
    Charge, ComposeContext, Dimension, OverlayComposer, REFINE_STATUS_ATTR, RefineDecision,
    RefineInput, RefineMode, RefineReport,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentModel, AgentOp, AgentProfile, AgentReport, AgentStop,
    AgentTask, ChatRequest, ChatResponse, ChatRole, ContentPart, ExactMatch, InMemoryMemoryStore,
    InMemoryOverlayStore, OverlayProposer, OverlayVersion, ProfileOverlay, ProfileOverlayStore,
    ProposeContext, ProposerReply, RefineStatus, ScoreContext, ScoreInput, Scorer, StopReason,
    TokenUsage, Verdict, sqlite,
};
use autumn_harvest_sqlite::{ExecutionId, RunState, SqliteRuntime};

/// The agent kind.
const KIND: &str = "support";

/// The system prompt of the operator. It lacks the rule.
const BASE: &str = "You are a support agent. Answer in one sentence.";

/// The rule that the refine loop adds.
const RULE: &str = "A refund over 100 USD needs a manager.";

/// The step bound of the profile. A source run must stay inside it.
const MAX_STEPS: u32 = 4;

/// The task of each op.
const TASK: &str = "Refund 250 USD for order 17.";

/// The answer that follows the rule.
const PASS: &str = "A manager must approve this refund.";

/// The answer that misses the rule.
const MISS: &str = "The refund is done.";

/// The system text of a model request.
fn system_of(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::System)
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

/// A scripted support model. It follows the rule only when its prompt holds
/// the rule.
#[derive(Debug)]
struct SupportModel;

impl AgentModel for SupportModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let answer = if system_of(request).contains(RULE) {
            PASS
        } else {
            MISS
        };
        Box::pin(async move {
            Ok(ChatResponse {
                content: vec![ContentPart::Text(answer.to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(40, 8),
            })
        })
    }
}

/// A scripted proposer. The DOM stage adds the rule. The INV stage keeps
/// its base, so the run records it as `kept_base`.
#[derive(Debug)]
struct RuleProposer;

impl OverlayProposer for RuleProposer {
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(200, 30)));
        let reply = match context.dimension {
            Dimension::Inv => context.current.clone(),
            Dimension::Dom => {
                let from = context
                    .current
                    .system
                    .as_deref()
                    .or(context.profile.system.as_deref())
                    .unwrap_or_default();
                ProfileOverlay {
                    system: Some(format!("{from}\n{RULE}")),
                    ..context.current.clone()
                }
            }
        };
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// A scripted composer. It keeps the last probed candidate.
#[derive(Debug)]
struct LastComposer;

impl OverlayComposer for LastComposer {
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        context.charge(Charge::new(Some("scripted"), TokenUsage::new(150, 20)));
        let reply = context
            .candidates
            .last()
            .map_or_else(|| context.current.clone(), |c| c.overlay.clone());
        Box::pin(async move { Ok(ProposerReply::Overlay(reply)) })
    }
}

/// What one walk through the loop shows.
#[derive(Debug)]
struct Outcome {
    /// The active version before the refine run.
    before: Option<OverlayVersion>,
    /// The active version after the gate.
    after: Option<OverlayVersion>,
    /// The check of op 1.
    op1: Verdict,
    /// The report of the refine run.
    report: RefineReport,
    /// The answer of op 2.
    op2_answer: String,
    /// The check of op 2.
    op2: Verdict,
}

/// The pieces of one walk. The runtime drops before the directory, so it
/// closes the database file before the directory goes.
struct Walk {
    rt: SqliteRuntime,
    harness: AgentHarness,
    store: Arc<InMemoryOverlayStore>,
    _dir: tempfile::TempDir,
}

/// A runtime on a new database file, with the scripted pieces.
fn walk() -> Result<Walk, Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("refine.db");
    let base = AgentHarness::new(Arc::new(SupportModel) as Arc<dyn AgentModel>)
        .agent(AgentProfile::new(KIND).system(BASE).max_steps(MAX_STEPS));
    let store = Arc::new(InMemoryOverlayStore::new(Arc::new(base.clone())));
    let harness = base
        .overlay_store(Arc::clone(&store) as Arc<dyn ProfileOverlayStore>)
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .proposer(Arc::new(RuleProposer))
        .composer(Arc::new(LastComposer))
        .refine_sources(Arc::new(sqlite::Sources::open(&db)));
    let mut rt = SqliteRuntime::open(&db)?;
    sqlite::register(&mut rt, Arc::new(harness.clone()));
    Ok(Walk {
        rt,
        harness,
        store,
        _dir: dir,
    })
}

/// The active version of the kind.
async fn active(store: &InMemoryOverlayStore) -> Result<Option<OverlayVersion>, Box<dyn Error>> {
    Ok(store.active(KIND).await?.map(|s| s.version()))
}

/// Check one op with the built-in `ExactMatch` scorer. A run that did not
/// stop as `completed` fails.
async fn check(
    harness: &AgentHarness,
    answer: &str,
    stop: AgentStop,
) -> Result<Verdict, Box<dyn Error>> {
    let input = ScoreInput::new(TASK, answer, stop);
    let cx = ScoreContext::unmetered(harness);
    Ok(ExactMatch::new(PASS).score(&input, &cx).await?)
}

impl Walk {
    /// Op 1: a recorded `agent_loop` run of the kind, on the base prompt.
    async fn op1(&mut self) -> Result<(ExecutionId, String, AgentStop), Box<dyn Error>> {
        // The run takes the prompt and the bound of the profile.
        let mut task = AgentTask::new(TASK).kind(KIND).max_steps(MAX_STEPS);
        task.system = Some(BASE.to_owned());
        let exec = sqlite::start(&mut self.rt, &task)?;
        match self.rt.run_until_blocked(exec).await? {
            RunState::Completed(value) => {
                let report: AgentReport = serde_json::from_value(value)?;
                Ok((exec, report.text, report.stop))
            }
            other => Err(format!("op 1 did not complete: {other:?}").into()),
        }
    }

    /// Refine the kind from op 1, then send `decision` at the gate.
    async fn refine(
        &mut self,
        source: ExecutionId,
        decision: &RefineDecision,
    ) -> Result<RefineReport, Box<dyn Error>> {
        let input = RefineInput::new(KIND, vec![source], 1, RefineMode::Sequential, 7);
        let exec = sqlite::start_refine(&mut self.rt, &input)?;
        let state = self.rt.run_until_blocked(exec).await?;
        if !matches!(state, RunState::WaitingSignal(_)) {
            return Err(format!("the refine run did not reach the gate: {state:?}").into());
        }
        // The gate drops a decision that arrives before its wait opens.
        let waiting = RefineStatus::AwaitingApproval.as_str();
        if self.status(exec)?.as_deref() != Some(waiting) {
            return Err("the refine run does not wait for approval".into());
        }
        sqlite::decide_refine(&mut self.rt, exec, decision)?;
        match self.rt.run_until_blocked(exec).await? {
            RunState::Completed(value) => Ok(serde_json::from_value(value)?),
            other => Err(format!("the refine run did not end: {other:?}").into()),
        }
    }

    /// The `refine_status` search attribute of `exec`.
    fn status(&self, exec: ExecutionId) -> Result<Option<String>, Box<dyn Error>> {
        let description = self.rt.describe(exec)?;
        Ok(description
            .summary
            .search_attributes
            .value()
            .and_then(|attrs| attrs.get(REFINE_STATUS_ATTR)?.as_str().map(str::to_owned)))
    }

    /// Op 2: an op of a durable agent of the kind. It reads the active
    /// version.
    async fn op2(&mut self) -> Result<(String, AgentStop), Box<dyn Error>> {
        let id = AgentId::new(KIND, "customer-2")?;
        sqlite::send(&mut self.rt, &id, &AgentOp::message(TASK), Some("op-2"))?;
        sqlite::drive(&mut self.rt, &id).await?;
        let state = sqlite::state(&self.rt, &id).await?;
        let state = state.ok_or("op 2 left no agent state")?;
        let answer = state.last_answer.ok_or("op 2 gave no answer")?;
        let stop = state.last_stop.ok_or("op 2 gave no stop")?;
        Ok((answer, stop))
    }
}

/// Walk through the loop. The person sends `decision` at the gate.
async fn run(decision: &RefineDecision) -> Result<Outcome, Box<dyn Error>> {
    let mut walk = walk()?;
    let before = active(&walk.store).await?;
    let (op1_exec, op1_answer, op1_stop) = walk.op1().await?;
    let op1 = check(&walk.harness, &op1_answer, op1_stop).await?;
    let report = walk.refine(op1_exec, decision).await?;
    let after = active(&walk.store).await?;
    let (op2_answer, op2_stop) = walk.op2().await?;
    let op2 = check(&walk.harness, &op2_answer, op2_stop).await?;
    Ok(Outcome {
        before,
        after,
        op1,
        report,
        op2_answer,
        op2,
    })
}

/// A version for a person to read.
fn show(version: Option<OverlayVersion>) -> String {
    version.map_or_else(|| "none (the base profile)".to_owned(), |v| v.to_string())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let outcome = run(&RefineDecision::Approve).await?;
    println!("op 1 check: {:?}", outcome.op1);
    println!("version before: {}", show(outcome.before));
    let report = &outcome.report;
    if let Some(gate) = &report.gate {
        println!("approved version: {}", gate.version);
    }
    let chosen = report.chosen.as_ref().and_then(|o| o.system.as_deref());
    println!("chosen prompt: {:?}", chosen.unwrap_or_default());
    println!("refine status: {:?}", report.status);
    println!("refine ledger: {:?}", report.ledger);
    println!("lesson: {:?}", report.lesson);
    println!("version after: {}", show(outcome.after));
    println!("op 2 answer: {}", outcome.op2_answer);
    println!("op 2 check: {:?}", outcome.op2);
    if !outcome.op1.is_reject() {
        return Err("op 1 passed on the base prompt, so the walk proves nothing".into());
    }
    if report.status != RefineStatus::Adopted || outcome.after == outcome.before {
        return Err("the refine run adopted no new version".into());
    }
    if !chosen.unwrap_or_default().contains(RULE) {
        return Err("the adopted prompt lacks the rule".into());
    }
    if outcome.op2.is_reject() {
        return Err("op 2 failed after the approval".into());
    }
    println!("op 2 succeeded");
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn op_two_passes_after_the_approval() {
        let outcome = run(&RefineDecision::Approve).await.unwrap();
        assert!(outcome.op1.is_reject(), "op 1 fails: {outcome:?}");
        assert_eq!(outcome.before, None);
        let report = &outcome.report;
        assert_eq!(report.status, RefineStatus::Adopted, "{report:?}");
        assert!(outcome.after.is_some(), "a version goes live");
        assert_eq!(outcome.op2_answer, PASS);
        assert!(!outcome.op2.is_reject(), "op 2 passes: {outcome:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_run_keeps_its_budget_and_writes_a_lesson() {
        let outcome = run(&RefineDecision::Approve).await.unwrap();
        let report = outcome.report;
        // Sequential with k = 1: one INV, one DOM and one composition. The
        // INV stage keeps its base, so it adds no probe.
        assert_eq!(report.ledger.sessions, 3, "{:?}", report.ledger);
        assert_eq!(report.ledger.kept_base, 1, "{:?}", report.ledger);
        assert_eq!(report.ledger.probes, 2, "{:?}", report.ledger);
        let chosen = report.chosen.expect("a chosen overlay");
        assert!(chosen.system.unwrap_or_default().contains(RULE));
        assert!(
            matches!(
                report.lesson,
                Some(autumn_harvest_agent::refine::LessonOutcome::Written { .. })
            ),
            "{:?}",
            report.lesson
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn op_two_fails_after_a_reject() {
        let reject = RefineDecision::Reject {
            reason: "not now".to_owned(),
        };
        let outcome = run(&reject).await.unwrap();
        let report = &outcome.report;
        assert_eq!(report.status, RefineStatus::Rejected, "{report:?}");
        assert_eq!(outcome.after, None, "a reject activates nothing");
        assert_eq!(outcome.op2_answer, MISS);
        assert!(outcome.op2.is_reject(), "op 2 fails: {outcome:?}");
    }
}
