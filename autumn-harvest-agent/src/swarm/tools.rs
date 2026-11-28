//! The tools of a swarm member (issue #2814).
//!
//! [`SwarmTools`] gives a member six tools over one [`Swarm`]. The member is
//! the session of the run. A durable agent has its agent id as its session.
//! Each write names the idempotency key of its call, so a call that runs
//! again after a crash writes once.
//!
//! The tools never show a score unless [`SwarmPolicy::reveal_scores`] is
//! set, and no tool runs the judge on demand.
//!
//! [`SwarmPolicy::reveal_scores`]: super::SwarmPolicy::reveal_scores

use std::sync::Arc;

use autumn_harvest::swarm_board::{ItemQuery, ItemRow, MergeRow, PostRow};
use serde_json::{Value, json};

use super::Verdict;
use super::board::{ClaimOutcome, Swarm};
use super::kernel::FenceRefusal;
use crate::error::{AgentError, ErrorKind};
use crate::model::BoxFuture;
use crate::tool::{Tool, ToolContext, ToolEffect};
use crate::workspace::Workspace;

/// The name of the tool that reads the board.
pub const BOARD_TOOL: &str = "swarm_board";

/// The name of the tool that adds a post.
pub const POST_TOOL: &str = "swarm_post";

/// The name of the tool that claims an item.
pub const CLAIM_TOOL: &str = "swarm_claim";

/// The name of the tool that renews a lease.
pub const RENEW_TOOL: &str = "swarm_renew";

/// The name of the tool that releases a claim.
pub const RELEASE_TOOL: &str = "swarm_release";

/// The name of the tool that submits a change to the merge gate.
pub const SUBMIT_TOOL: &str = "swarm_submit";

/// The default number of items and posts that one board read returns.
pub const DEFAULT_BOARD_LIMIT: u32 = 50;

/// The six tools of a member of one swarm.
#[derive(Debug, Clone)]
pub struct SwarmTools {
    swarm: Arc<Swarm>,
}

impl SwarmTools {
    /// The tools of `swarm`.
    #[must_use]
    pub const fn new(swarm: Arc<Swarm>) -> Self {
        Self { swarm }
    }

    /// Every tool, ready for `AgentHarness::tools`.
    #[must_use]
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        [
            Kind::Board,
            Kind::Post,
            Kind::Claim,
            Kind::Renew,
            Kind::Release,
            Kind::Submit,
        ]
        .into_iter()
        .map(|kind| {
            Arc::new(SwarmTool {
                swarm: self.swarm.clone(),
                kind,
            }) as Arc<dyn Tool>
        })
        .collect()
    }
}

/// Which tool a [`SwarmTool`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Board,
    Post,
    Claim,
    Renew,
    Release,
    Submit,
}

/// One tool of a swarm member.
#[derive(Debug)]
struct SwarmTool {
    swarm: Arc<Swarm>,
    kind: Kind,
}

/// A tool error that the model reads.
fn tool_error(message: impl Into<String>) -> AgentError {
    AgentError::new(ErrorKind::Tool, message)
}

/// The member of the call: its session.
fn member(ctx: &ToolContext) -> Result<String, AgentError> {
    ctx.session_id
        .as_ref()
        .map(|session| session.as_str().to_owned())
        .ok_or_else(|| tool_error("the swarm tools need a session; run them in a durable agent"))
}

/// The idempotency key of the call.
fn call_key(ctx: &ToolContext) -> String {
    format!("{}:{}:{}", ctx.run_id.as_str(), ctx.step, ctx.call_id)
}

/// The text field `name` of `input`.
fn text<'a>(input: &'a Value, name: &str) -> Result<&'a str, AgentError> {
    input
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| tool_error(format!("the input needs the text field {name:?}")))
}

/// The optional text field `name` of `input`.
fn opt_text<'a>(input: &'a Value, name: &str) -> Option<&'a str> {
    input.get(name).and_then(Value::as_str)
}

/// The number field `name` of `input`.
fn number(input: &Value, name: &str) -> Result<u64, AgentError> {
    input
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| tool_error(format!("the input needs the number field {name:?}")))
}

/// Run `f` on the workspace of the call.
fn with_workspace<T>(
    ctx: &ToolContext,
    f: impl FnOnce(&mut Workspace) -> T,
) -> Result<T, AgentError> {
    let handle = ctx.workspace.as_ref().ok_or_else(|| {
        tool_error("the swarm tools need a workspace; turn it on with AgentProfile::workspace")
    })?;
    Ok(handle.with(f))
}

/// An item for the board list, with no text.
fn summary(row: &ItemRow, member: &str) -> Value {
    json!({
        "id": row.item,
        "lane": row.lane,
        "title": row.title,
        "status": row.status,
        "holder": row.holder,
        "epoch": row.epoch,
        "yours": row.holder.as_deref() == Some(member) && row.status == "claimed",
    })
}

/// An item with its text.
fn detail(row: &ItemRow, member: &str) -> Value {
    let mut value = summary(row, member);
    value["body"] = json!(row.body);
    value["base_version"] = json!(row.base_version);
    value["lease_until_ms"] = json!(row.lease_until_ms);
    value
}

fn post_view(row: &PostRow) -> Value {
    json!({
        "seq": row.seq,
        "channel": row.channel,
        "author": row.author,
        "text": row.body,
    })
}

fn fence_view(result: Result<ItemRow, FenceRefusal>, done: &str) -> Value {
    match result {
        Ok(row) => json!({
            "verdict": done,
            "item": row.item,
            "epoch": row.epoch,
            "lease_until_ms": row.lease_until_ms,
        }),
        Err(refusal) => json!({"verdict": refusal.as_str()}),
    }
}

impl SwarmTool {
    async fn board(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let swarm = &self.swarm;
        let limit = input
            .get("limit")
            .and_then(Value::as_u64)
            .map_or(DEFAULT_BOARD_LIMIT, |n| {
                u32::try_from(n).unwrap_or(u32::MAX)
            });
        let mine = input.get("mine").and_then(Value::as_bool).unwrap_or(false);
        let query = ItemQuery {
            lane: opt_text(input, "lane"),
            status: opt_text(input, "status").or_else(|| mine.then_some("claimed")),
            holder: mine.then_some(member.as_str()),
            after: opt_text(input, "after_item"),
            limit,
        };
        let items = swarm.items(query).await?;
        let after_post = input.get("after_post").and_then(Value::as_u64).unwrap_or(0);
        let posts = swarm
            .posts(opt_text(input, "channel"), after_post, limit)
            .await?;
        let next_post = posts.last().map_or(after_post, |post| post.seq);
        let main = swarm.main().await?;
        let mut out = json!({
            "swarm": swarm.id().as_str(),
            "you": member,
            "main": main.map(|main| json!({
                "version": main.version,
                "item": main.item,
                "author": main.author,
            })),
            "items": items.iter().map(|row| summary(row, &member)).collect::<Vec<_>>(),
            "posts": posts.iter().map(post_view).collect::<Vec<_>>(),
            "next_post": next_post,
        });
        if let Some(id) = opt_text(input, "item") {
            out["item"] = swarm
                .item(id)
                .await?
                .map_or(Value::Null, |row| detail(&row, &member));
        }
        Ok(out)
    }

    async fn post(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let row = self
            .swarm
            .post(
                &member,
                text(input, "channel")?,
                text(input, "text")?,
                &call_key(ctx),
            )
            .await?;
        Ok(json!({"seq": row.seq}))
    }

    async fn claim(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let item = text(input, "item")?;
        let mut workspace = with_workspace(ctx, |files| files.clone())?;
        let outcome = self.swarm.claim(&member, item, &mut workspace).await?;
        let (verdict, row) = match outcome {
            ClaimOutcome::Claimed(row) => ("claimed", row),
            ClaimOutcome::Kept(row) => ("kept", row),
            ClaimOutcome::Refused { refusal, item } => {
                return Ok(json!({
                    "verdict": refusal.as_str(),
                    "item": summary(&item, &member),
                }));
            }
        };
        with_workspace(ctx, |files| *files = workspace)?;
        Ok(json!({
            "verdict": verdict,
            "item": detail(&row, &member),
            "root": self.swarm.policy().prefix(),
        }))
    }

    async fn renew(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let result = self
            .swarm
            .renew(&member, text(input, "item")?, number(input, "epoch")?)
            .await?;
        Ok(fence_view(result, "renewed"))
    }

    async fn release(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let result = self
            .swarm
            .release(&member, text(input, "item")?, number(input, "epoch")?)
            .await?;
        Ok(fence_view(result, "released"))
    }

    async fn submit(&self, input: &Value, ctx: &ToolContext) -> Result<Value, AgentError> {
        let member = member(ctx)?;
        let item = text(input, "item")?;
        let epoch = number(input, "epoch")?;
        let mut workspace = with_workspace(ctx, |files| files.clone())?;
        let row = self
            .swarm
            .submit(&member, item, epoch, &call_key(ctx), &mut workspace)
            .await?;
        if row.verdict == Verdict::Merged.as_str() {
            with_workspace(ctx, |files| *files = workspace)?;
        }
        Ok(self.verdict_view(&row))
    }

    fn verdict_view(&self, row: &MergeRow) -> Value {
        let mut out = json!({
            "verdict": row.verdict,
            "item": row.item,
            "main_version": row.merged_version.unwrap_or(row.main_version),
        });
        let hint = match Verdict::parse(&row.verdict) {
            Some(Verdict::Merged) => "main now holds your change; your workspace is the new main",
            Some(Verdict::Rejected) => {
                "the change does not beat main; you still hold the item, so improve it and submit again"
            }
            Some(Verdict::Conflict) => {
                "main changed the same paths; claim the item again to check out your base, then redo the change"
            }
            Some(Verdict::Fenced) => "you do not hold this item at this epoch",
            Some(Verdict::JudgeFailed) => "the eval failed; submit again later",
            Some(Verdict::Contended) | None => "main kept moving; submit again",
        };
        out["hint"] = json!(hint);
        if !row.detail.is_empty() && row.verdict != Verdict::JudgeFailed.as_str() {
            out["detail"] = json!(row.detail);
        }
        if self.swarm.policy().reveal_scores {
            out["score"] = json!(row.candidate_score);
            out["main_score"] = json!(row.main_score);
        }
        out
    }
}

impl Tool for SwarmTool {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Board => BOARD_TOOL,
            Kind::Post => POST_TOOL,
            Kind::Claim => CLAIM_TOOL,
            Kind::Renew => RENEW_TOOL,
            Kind::Release => RELEASE_TOOL,
            Kind::Submit => SUBMIT_TOOL,
        }
    }

    fn description(&self) -> &str {
        match self.kind {
            Kind::Board => {
                "Read the swarm board: the work items, the new posts after `after_post`, and the \
                 main version. Pass `item` to read the text of one item. Pass `mine: true` for \
                 the items you hold. Keep `next_post` and pass it as `after_post` next time."
            }
            Kind::Post => {
                "Post `text` on the swarm channel `channel`, for the other members to read. Say \
                 what you work on, what you found and what blocks you."
            }
            Kind::Claim => {
                "Claim the open item `item`. A claim checks out main into your workspace and \
                 returns the epoch of your claim. Pass the epoch to renew, release and submit. \
                 A claim of an item you hold checks out its base again and drops your changes."
            }
            Kind::Renew => {
                "Renew the lease of your claim on `item` at `epoch`. A lapsed lease lets another \
                 member take the item."
            }
            Kind::Release => {
                "Release your claim on `item` at `epoch`, so another member can take it."
            }
            Kind::Submit => {
                "Submit your workspace for `item` at `epoch`. The gate merges your changes onto \
                 main, and merges only a change that beats main on a hidden eval. A rejected \
                 change keeps your claim, so you can improve it and submit again."
            }
        }
    }

    fn input_schema(&self) -> Value {
        let fenced = json!({
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "epoch": {"type": "integer", "minimum": 0},
            },
            "required": ["item", "epoch"],
        });
        match self.kind {
            Kind::Board => json!({
                "type": "object",
                "properties": {
                    "lane": {"type": "string"},
                    "status": {"type": "string", "enum": ["open", "claimed", "merged", "dropped"]},
                    "mine": {"type": "boolean"},
                    "after_item": {"type": "string"},
                    "item": {"type": "string"},
                    "channel": {"type": "string"},
                    "after_post": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1},
                },
            }),
            Kind::Post => json!({
                "type": "object",
                "properties": {
                    "channel": {"type": "string"},
                    "text": {"type": "string"},
                },
                "required": ["channel", "text"],
            }),
            Kind::Claim => json!({
                "type": "object",
                "properties": {"item": {"type": "string"}},
                "required": ["item"],
            }),
            Kind::Renew | Kind::Release | Kind::Submit => fenced,
        }
    }

    fn execute<'a>(
        &'a self,
        input: Value,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async move {
            match self.kind {
                Kind::Board => self.board(&input, ctx).await,
                Kind::Post => self.post(&input, ctx).await,
                Kind::Claim => self.claim(&input, ctx).await,
                Kind::Renew => self.renew(&input, ctx).await,
                Kind::Release => self.release(&input, ctx).await,
                Kind::Submit => self.submit(&input, ctx).await,
            }
        })
    }

    fn effect(&self) -> ToolEffect {
        match self.kind {
            Kind::Board => ToolEffect::ReadOnly,
            Kind::Post | Kind::Claim | Kind::Renew | Kind::Release | Kind::Submit => {
                ToolEffect::Write
            }
        }
    }

    fn uses_workspace(&self) -> bool {
        matches!(self.kind, Kind::Claim | Kind::Submit)
    }
}
