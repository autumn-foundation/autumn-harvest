//! Swarm mode end to end (issue #2814).
//!
//! The first tests call the swarm tools with a hand-made `ToolContext`. The
//! last test drives two durable agents on one `SQLite` runtime. They share a
//! board, claim items, write files, and submit to the merge gate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;
// This target uses the judge and the file helper of the suite only.
#[allow(dead_code)]
mod swarm_store_suite;

use std::path::PathBuf;
use std::sync::Arc;

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest_agent::swarm::{
    BOARD_TOOL, CLAIM_TOOL, InMemorySwarmStore, NewItem, POST_TOOL, RELEASE_TOOL, RENEW_TOOL,
    SUBMIT_TOOL, SqliteSwarmStore, Swarm, SwarmId, SwarmPolicy, SwarmTools,
};
use autumn_harvest_agent::{
    AgentHarness, AgentId, AgentOp, AgentProfile, AgentState, ChatRole, ContentPart, FnTool,
    InMemoryWorkspaceBlobs, RunId, SessionId, Tool, ToolContext, ToolEffect, Workspace,
    WorkspaceHandle, WorkspaceStore, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use common::{ScriptedModel, answer, calls, runtime};
use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};
use swarm_store_suite::{CountJudge, files};

/// A swarm on `store` with `policy`, seeded with two plain files.
async fn swarm(policy: SwarmPolicy) -> Arc<Swarm> {
    let workspaces = Arc::new(WorkspaceStore::new(
        Arc::new(InMemoryWorkspaceBlobs::default()),
        PayloadCodecs::default(),
    ));
    let swarm = Swarm::new(
        SwarmId::new("pybot").unwrap(),
        Arc::new(InMemorySwarmStore::new()),
        workspaces,
        Arc::new(CountJudge::default()),
    )
    .with_policy(policy);
    swarm
        .seed(&files(&[("a.txt", "plain"), ("b.txt", "plain")]))
        .await
        .unwrap();
    for id in ["one", "two"] {
        swarm
            .add_item(
                &NewItem::new(id, "answers", format!("fix {id}"), "make it good"),
                "ops",
            )
            .await
            .unwrap();
    }
    Arc::new(swarm)
}

fn tool(tools: &SwarmTools, name: &str) -> Arc<dyn Tool> {
    tools
        .tools()
        .into_iter()
        .find(|tool| tool.name() == name)
        .unwrap()
}

fn ctx(member: Option<&str>, call: &str, workspace: Option<&WorkspaceHandle>) -> ToolContext {
    ToolContext {
        run_id: RunId::new("run-1"),
        call_id: call.to_owned(),
        session_id: member.map(SessionId::new),
        step: 0,
        workspace: workspace.cloned(),
    }
}

#[tokio::test]
async fn the_tools_have_names_effects_and_schemas() {
    let tools = SwarmTools::new(swarm(SwarmPolicy::default()).await);
    let names: Vec<String> = tools.tools().iter().map(|t| t.name().to_owned()).collect();
    assert_eq!(
        names,
        [
            BOARD_TOOL,
            POST_TOOL,
            CLAIM_TOOL,
            RENEW_TOOL,
            RELEASE_TOOL,
            SUBMIT_TOOL
        ]
    );
    for tool in tools.tools() {
        let expected = if tool.name() == BOARD_TOOL {
            ToolEffect::ReadOnly
        } else {
            ToolEffect::Write
        };
        assert_eq!(tool.effect(), expected, "{}", tool.name());
        assert_eq!(
            tool.uses_workspace(),
            [CLAIM_TOOL, SUBMIT_TOOL].contains(&tool.name()),
            "{}",
            tool.name()
        );
        assert_eq!(tool.input_schema()["type"], "object");
        assert_ne!(tool.description(), "");
    }
}

#[tokio::test]
async fn a_call_needs_a_session_and_a_workspace() {
    let tools = SwarmTools::new(swarm(SwarmPolicy::default()).await);
    let board = tool(&tools, BOARD_TOOL);
    let err = board
        .execute(json!({}), &ctx(None, "c1", None))
        .await
        .unwrap_err();
    assert!(err.message().contains("need a session"), "{err}");
    let claim = tool(&tools, CLAIM_TOOL);
    let err = claim
        .execute(json!({"item": "one"}), &ctx(Some("coder/c1"), "c2", None))
        .await
        .unwrap_err();
    assert!(err.message().contains("need a workspace"), "{err}");
    let err = claim
        .execute(json!({}), &ctx(Some("coder/c1"), "c3", None))
        .await
        .unwrap_err();
    assert!(err.message().contains("\"item\""), "{err}");
}

#[tokio::test]
async fn the_tools_run_a_claim_to_a_merge_and_hide_the_scores() {
    let shared = swarm(SwarmPolicy::default()).await;
    let tools = SwarmTools::new(shared.clone());
    let handle = WorkspaceHandle::new(Workspace::default());
    let me = Some("coder/c1");
    let claimed = tool(&tools, CLAIM_TOOL)
        .execute(json!({"item": "one"}), &ctx(me, "k1", Some(&handle)))
        .await
        .unwrap();
    assert_eq!(claimed["verdict"], "claimed");
    assert_eq!(claimed["item"]["epoch"], 1);
    assert_eq!(claimed["item"]["body"], "make it good");
    assert_eq!(
        handle
            .with(|ws| ws.read_text("a.txt").map(str::to_owned))
            .as_deref(),
        Some("plain")
    );
    let board = tool(&tools, BOARD_TOOL)
        .execute(json!({"mine": true, "item": "one"}), &ctx(me, "k2", None))
        .await
        .unwrap();
    assert_eq!(board["items"].as_array().unwrap().len(), 1);
    assert_eq!(board["items"][0]["yours"], true);
    assert_eq!(board["item"]["body"], "make it good");
    assert_eq!(board["main"]["version"], 1);
    let renewed = tool(&tools, RENEW_TOOL)
        .execute(json!({"item": "one", "epoch": 1}), &ctx(me, "k3", None))
        .await
        .unwrap();
    assert_eq!(renewed["verdict"], "renewed");
    let stale = tool(&tools, RENEW_TOOL)
        .execute(json!({"item": "one", "epoch": 0}), &ctx(me, "k4", None))
        .await
        .unwrap();
    assert_eq!(stale["verdict"], "stale_epoch");
    handle.with(|ws| ws.write("a.txt", "good")).unwrap();
    let submit = tool(&tools, SUBMIT_TOOL);
    let call = ctx(me, "k5", Some(&handle));
    let merged = submit
        .execute(json!({"item": "one", "epoch": 1}), &call)
        .await
        .unwrap();
    assert_eq!(merged["verdict"], "merged");
    assert_eq!(merged["main_version"], 2);
    assert!(
        merged.get("score").is_none(),
        "the scores stay hidden: {merged}"
    );
    let again = submit
        .execute(json!({"item": "one", "epoch": 1}), &call)
        .await
        .unwrap();
    assert_eq!(again, merged, "a call that runs again returns its verdict");
    assert_eq!(shared.main().await.unwrap().unwrap().version, 2);
    assert_eq!(shared.merges(0, 10).await.unwrap().len(), 1);
    let released = tool(&tools, RELEASE_TOOL)
        .execute(json!({"item": "one", "epoch": 1}), &ctx(me, "k6", None))
        .await
        .unwrap();
    assert_eq!(released["verdict"], "not_claimed");
    let posted = tool(&tools, POST_TOOL)
        .execute(
            json!({"channel": "general", "text": "one is in"}),
            &ctx(me, "k7", None),
        )
        .await
        .unwrap();
    assert_eq!(posted["seq"], 1);
}

#[tokio::test]
async fn an_operator_can_reveal_the_scores() {
    let shared = swarm(SwarmPolicy {
        reveal_scores: true,
        ..SwarmPolicy::default()
    })
    .await;
    let tools = SwarmTools::new(shared);
    let handle = WorkspaceHandle::new(Workspace::default());
    let me = Some("coder/c1");
    tool(&tools, CLAIM_TOOL)
        .execute(json!({"item": "one"}), &ctx(me, "k1", Some(&handle)))
        .await
        .unwrap();
    let rejected = tool(&tools, SUBMIT_TOOL)
        .execute(
            json!({"item": "one", "epoch": 1}),
            &ctx(me, "k2", Some(&handle)),
        )
        .await
        .unwrap();
    assert_eq!(rejected["verdict"], "rejected");
    assert_eq!(
        (rejected["score"].clone(), rejected["main_score"].clone()),
        (json!(0), json!(0))
    );
}

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("agent.db");
    (dir, path)
}

/// `write_file`: write `text` at `path` in the workspace.
fn write_tool() -> Arc<dyn Tool> {
    FnTool::with_context(
        "write_file",
        "Write a file in the workspace.",
        json!({"type": "object"}),
        |input, ctx| async move {
            let path = input["path"].as_str().unwrap_or_default().to_owned();
            let text = input["text"].as_str().unwrap_or_default().to_owned();
            let workspace = ctx.workspace.expect("the tool uses the workspace");
            workspace.with(|files| files.write(&path, text))?;
            Ok(json!({"written": path}))
        },
    )
    .effect(ToolEffect::Internal)
    .uses_workspace()
    .shared()
}

async fn drive(rt: &mut SqliteRuntime, agent: &AgentId) {
    let mut at = Timestamp::now();
    for _ in 0..4 {
        let mut exec = sqlite::current_run(rt, agent).unwrap().unwrap();
        while let RunState::ContinuedAsNew(next) =
            rt.run_until_blocked_as_of(exec, at).await.unwrap()
        {
            exec = next;
        }
        at += SignedDuration::from_mins(5);
    }
}

async fn say(rt: &mut SqliteRuntime, agent: &AgentId, text: &str) -> AgentState {
    sqlite::send(rt, agent, &AgentOp::message(text), None).unwrap();
    drive(rt, agent).await;
    sqlite::state(rt, agent)
        .await
        .unwrap()
        .expect("the agent runs")
}

/// The JSON of each tool result in the transcript, in order.
fn results(state: &AgentState) -> Vec<Value> {
    state
        .transcript
        .iter()
        .filter(|m| m.role == ChatRole::Tool)
        .flat_map(|m| &m.content)
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => serde_json::from_str(content).ok(),
            _ => None,
        })
        .collect()
}

fn one(id: &str, name: &str, input: Value) -> common::Reply {
    calls(&[(id, name, input)], 1)
}

/// A swarm on a `SQLite` file in `dir`, seeded with two plain files and two
/// items, and the workspace blobs that its agents share.
async fn sqlite_swarm(dir: &tempfile::TempDir) -> (Arc<Swarm>, Arc<InMemoryWorkspaceBlobs>) {
    let blobs = Arc::new(InMemoryWorkspaceBlobs::default());
    let workspaces = Arc::new(WorkspaceStore::new(blobs.clone(), PayloadCodecs::default()));
    let store = Arc::new(SqliteSwarmStore::open(dir.path().join("swarm.db")).unwrap());
    let policy = SwarmPolicy {
        root: "repo".to_owned(),
        ..SwarmPolicy::default()
    };
    let shared = Arc::new(
        Swarm::new(
            SwarmId::new("pybot").unwrap(),
            store,
            workspaces,
            Arc::new(CountJudge::default()),
        )
        .with_policy(policy),
    );
    shared
        .seed(&files(&[("a.txt", "plain"), ("b.txt", "plain")]))
        .await
        .unwrap();
    for id in ["one", "two"] {
        shared
            .add_item(
                &NewItem::new(id, "answers", format!("fix {id}"), "make it good"),
                "ops",
            )
            .await
            .unwrap();
    }
    (shared, blobs)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_durable_agents_share_a_board_and_the_gate() {
    let (dir, db) = fresh_db();
    let (shared, blobs) = sqlite_swarm(&dir).await;
    let model = ScriptedModel::new(vec![
        one("b1", BOARD_TOOL, json!({})),
        one("c1", CLAIM_TOOL, json!({"item": "one"})),
        one(
            "w1",
            "write_file",
            json!({"path": "repo/a.txt", "text": "good"}),
        ),
        one("s1", SUBMIT_TOOL, json!({"item": "one", "epoch": 1})),
        answer("merged one", 1),
        one("c2", CLAIM_TOOL, json!({"item": "one"})),
        one("c3", CLAIM_TOOL, json!({"item": "two"})),
        one(
            "w2",
            "write_file",
            json!({"path": "repo/b.txt", "text": "meh"}),
        ),
        one("s2", SUBMIT_TOOL, json!({"item": "two", "epoch": 1})),
        one(
            "p1",
            POST_TOOL,
            json!({"channel": "answers", "text": "two needs work"}),
        ),
        answer("two was rejected", 1),
    ]);
    let harness = AgentHarness::new(model)
        .tools(SwarmTools::new(shared.clone()).tools())
        .tool(write_tool())
        .workspace(WorkspaceStore::new(blobs, PayloadCodecs::default()))
        .agent(AgentProfile::new("coder").workspace());
    let mut rt = runtime(&db, harness);
    let ada = AgentId::new("coder", "ada").unwrap();
    let bob = AgentId::new("coder", "bob").unwrap();

    let first = say(&mut rt, &ada, "work on the board").await;
    let seen = results(&first);
    assert_eq!(seen[0]["you"], "coder/ada");
    assert_eq!(seen[0]["items"].as_array().unwrap().len(), 2);
    assert_eq!(seen[1]["verdict"], "claimed");
    assert_eq!(seen[3]["verdict"], "merged", "{seen:?}");
    assert!(seen[3].get("score").is_none());
    let (main, main_files) = shared.main_files().await.unwrap();
    assert_eq!(
        (main.version, main.author.as_deref()),
        (2, Some("coder/ada"))
    );
    assert_eq!(main_files, files(&[("a.txt", "good"), ("b.txt", "plain")]));

    let second = say(&mut rt, &bob, "work on the board").await;
    let seen = results(&second);
    assert_eq!(seen[0]["verdict"], "closed");
    assert_eq!(seen[1]["verdict"], "claimed");
    assert_eq!(
        seen[1]["item"]["base_version"], 2,
        "bob checks out the new main"
    );
    assert_eq!(seen[3]["verdict"], "rejected", "{seen:?}");
    assert!(seen[3]["hint"].as_str().unwrap().contains("still hold"));
    assert_eq!(seen[4]["seq"], 1);

    assert_eq!(shared.main().await.unwrap().unwrap().version, 2);
    let two = shared.item("two").await.unwrap().unwrap();
    assert_eq!(
        (two.status.as_str(), two.holder.as_deref()),
        ("claimed", Some("coder/bob"))
    );
    let log = shared.merges(0, 10).await.unwrap();
    let verdicts: Vec<(&str, &str)> = log
        .iter()
        .map(|row| (row.holder.as_str(), row.verdict.as_str()))
        .collect();
    assert_eq!(
        verdicts,
        [("coder/ada", "merged"), ("coder/bob", "rejected")]
    );
    let posts = shared.posts(Some("answers"), 0, 10).await.unwrap();
    assert_eq!(posts[0].author, "coder/bob");
}
