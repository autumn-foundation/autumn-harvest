# Agent swarm: a shared board and a merge gate

A **swarm** is a set of durable agents that work on one goal as flat peers.
They share a **board** of work items and posts. Each member claims an item,
changes files in its own workspace, and submits. A **merge gate** takes the
change into the shared **main** only when it beats main on an eval that the
members never see.

The module is `swarm` in `autumn-harvest-agent` (issue #2814). It works on
Postgres and on SQLite. The design is in
[`docs/design/DESIGN-2814.md`](design/DESIGN-2814.md).

A swarm is not the [agent mesh](agent-mesh.md). A mesh is a delegation tree:
a parent hands a task down, and authority narrows down the tree. A swarm has
no parent. The members coordinate through the board.

Each Rust example compiles as written, and CI checks this (issue #2482). An
example takes each value from your app as a function parameter.

## 1. Why this shape

Hugging Face ran 1,000 agents on one goal for nine days in November 2026.
The agents were flat peers on a message board, in small teams per lane. A
change merged only when it beat main on a test deck. The final eval stayed
hidden from the agents. The authors found that rule did more than a
hierarchy, a vote or strict teams.

The swarm module makes each part of that rule durable and fenced:

| Part | Type | What it does |
|---|---|---|
| Board | `Swarm::items`, `Swarm::posts` | Lists work items by lane and status. Reads posts after a cursor. |
| Claim | `Swarm::claim` | Takes an item with a lease, at a new epoch. Checks main out into the workspace. |
| Fence | `kernel::fence` | Lets a renew, a release or a submit through only for the holder at the current epoch. |
| Main | `Swarm::seed`, `Swarm::main_files` | One versioned head over a workspace snapshot, with its score. |
| Merge | `three_way` | Merges the files of a member onto the current main, path by path. |
| Gate | `SwarmJudge`, `Bar` | Scores the merged files. Lands the merge only when it passes the bar over main. |
| Log | `Swarm::merges` | Every verdict, append-only. |
| Tools | `SwarmTools` | Six tools for a member. |

## 2. Set up a swarm

A swarm needs four things:

- a `SwarmStore`: `InMemorySwarmStore`, `SqliteSwarmStore` or
  `PostgresSwarmStore`;
- a `WorkspaceStore` for the main snapshots. It can share the blobs of the
  agent workspaces. The swarm keeps its snapshots in the scope
  `swarm:<name>`;
- a `SwarmJudge`: the eval. You write it;
- a `SwarmPolicy`, or the default.

```rust
use std::sync::Arc;

use autumn_harvest_agent::swarm::{
    Files, NewItem, SqliteSwarmStore, Swarm, SwarmError, SwarmId, SwarmJudge, SwarmPolicy,
    SwarmTools,
};
use autumn_harvest_agent::{AgentHarness, AgentProfile, WorkspaceStore};

async fn start_swarm(
    harness: AgentHarness,
    workspaces: Arc<WorkspaceStore>,
    judge: Arc<dyn SwarmJudge>,
    seed: Files,
) -> Result<AgentHarness, SwarmError> {
    let swarm = Swarm::new(
        SwarmId::new("pybot")?,
        Arc::new(SqliteSwarmStore::open("swarm.db")?),
        workspaces,
        judge,
    )
    .with_policy(SwarmPolicy {
        root: "repo".to_owned(),
        ..SwarmPolicy::default()
    });
    swarm.seed(&seed).await?;
    swarm
        .add_item(
            &NewItem::new("greetings", "answers", "Answer greetings", "Reply to hello."),
            "ops",
        )
        .await?;
    let tools = SwarmTools::new(Arc::new(swarm)).tools();
    Ok(harness
        .tools(tools)
        .agent(AgentProfile::new("coder").workspace()))
}
```

Each member is a durable agent with a workspace, such as `coder/ada` and
`coder/bob`. A member is the session of its run. A durable agent has its
agent id as its session, so the tools know who calls them. A run with no
session, or with no workspace, gets a tool error.

`seed` scores the files with the judge, and saves them as main version 1. A
second `seed` keeps the main that is there.

## 3. The judge

```rust
use autumn_harvest_agent::swarm::{Files, Score, SwarmId, SwarmJudge};
use autumn_harvest_agent::{AgentError, BoxFuture};

/// A judge that counts the files with a passing marker.
#[derive(Debug)]
struct MarkerJudge;

impl SwarmJudge for MarkerJudge {
    fn score<'a>(
        &'a self,
        _swarm: &'a SwarmId,
        files: &'a Files,
    ) -> BoxFuture<'a, Result<Score, AgentError>> {
        let passing = files
            .values()
            .filter(|file| file.data.starts_with(b"PASS"))
            .count();
        let units = i64::try_from(passing).unwrap_or(i64::MAX);
        Box::pin(async move { Ok(Score::new(units)) })
    }
}
```

- A higher score is better. `Score::from_f64` maps a ratio, such as a win
  rate, to integer micro units, so the bar is exact on every backend.
- The judge gets only the files under `SwarmPolicy::root`.
- Give the same answer for the same files. The gate compares a candidate with
  the **stored** score of main. It does not score main again.
- The judge runs inside the `swarm_submit` tool call, so the tool timeout
  bounds it. Run a long eval in a service, and call it from the judge.
- A judge failure logs `judge_failed` and merges nothing.

The judge never runs as a tool. A member cannot ask for a score. It sees the
verdict, and the scores only when `SwarmPolicy::reveal_scores` is set. Keep
the eval hidden: a member that sees the eval fits its work to the eval.

## 4. The policy

| Field | Default | Meaning |
|---|---|---|
| `lease` | 30 minutes | How long a claim holds without a renew. |
| `max_claims` | 1 | How many items one member holds at a time. A take that finds the cap broken after its write, by a concurrent take of the same member, gives its item back with `at_cap`. |
| `bar` | `Bar::Improve { margin: 0 }` | A merge must beat main by more than `margin` score units. `Bar::NoRegression` takes a tie. |
| `reveal_scores` | `false` | Whether a verdict shows the scores to the member. |
| `root` | empty | The workspace folder that holds the files of main. Empty is the whole workspace. |
| `max_attempts` | 8 | How many times one submit tries again after another merge moved main. |

## 5. The tools

| Tool | Effect | Input | What it does |
|---|---|---|---|
| `swarm_board` | read only | `lane`, `status`, `mine`, `after_item`, `item`, `channel`, `after_post`, `limit` | Lists items and the posts after `after_post`. Returns `next_post`, the cursor for the next read. `item` adds the text of one item. |
| `swarm_post` | write | `channel`, `text` | Adds a post. |
| `swarm_claim` | write | `item` | Takes the item and checks out main under the root. Returns the epoch. |
| `swarm_renew` | write | `item`, `epoch` | Renews the lease. |
| `swarm_release` | write | `item`, `epoch` | Opens the item again. |
| `swarm_submit` | write | `item`, `epoch` | Sends the workspace to the gate. Returns the verdict and a hint. |

A refusal is a result, not an error, so the model reads it. The verdict
names are `taken`, `closed`, `at_cap` and `epoch_full` for a claim, and
`not_claimed`, `not_holder` and `stale_epoch` for the fence.

A second `swarm_claim` of an item that the member holds keeps its epoch. It
renews the lease and checks out the **base** of the claim again, so it drops
the changes of the member. That makes a claim that runs again after a crash
leave the same files.

## 6. The gate

A submit runs these steps:

1. A submit with the key of an earlier call returns the earlier verdict. The
   key is the idempotency key of the tool call. So a call that runs again
   after a crash merges once.
2. The fence must pass. Otherwise the verdict is `fenced`, and the judge
   does not run.
3. `three_way` merges the files of the member onto the current main, against
   the base of the claim. Each path takes the side that changed it. Two
   different changes of one path are a `conflict`, and the verdict names the
   paths.
4. The judge scores the merged files. A score that does not pass the bar is
   `rejected`.
5. The gate saves the merged files as the next main version. One transaction
   moves main, closes the item and logs `merged`. It compares the main
   version and the item `rev`. When another merge moved main first, the
   gate merges again onto the newer main and scores again. After
   `max_attempts` it logs `contended`.

A merge checks the current main out into the workspace of the member. A
replayed verdict does the same. The current main holds the merge, so a
replay works after `Swarm::prune` deleted the snapshot of the merge. A
`rejected` or `conflict` verdict keeps the claim and the files, so the member
can improve its change and submit again.

## 7. The fence

Each take of an item increments its epoch. A renew, a release and a submit
name the epoch of their claim. The fence passes only for the holder at the
current epoch.

The fence does not check the lease end. A lapsed claim stays valid until
another member takes the item. That take moves the epoch, so the old holder
fails the fence with `not_holder` or `stale_epoch`. This is the rule of the
decision owner fence ([ADR 0009](adr/0009-decision-owner.md)): the commit
compares holder and epoch, never the lease end.

`formal/verus/src/agent_swarm.rs` proves the claim, the fence, the merge pick
and the bar for every input.

## 8. Stores and retention

The stores keep rows and compare versions. They decide nothing, so the three
stores decide alike. `tests/swarm_store_suite` runs one suite on each.

| Table | Holds |
|---|---|
| `harvest_swarm_items` | The items, with the holder, the epoch, the lease and the base. |
| `harvest_swarm_posts` | The posts. Append-only. |
| `harvest_swarm_mains` | The main head of each swarm and its score. |
| `harvest_swarm_merges` | The verdicts. Append-only. |
| `harvest_swarm_counters` | The `seq` counters of posts and verdicts. |

A post and a verdict take their `seq` under the lock of a counter row. So the
posts of one swarm commit in `seq` order, and a reader with a cursor never
skips one. A trigger refuses each `UPDATE` of a post or a verdict.

On Postgres, apply the migration `20261128001506_harvest_swarm_board`. On
SQLite, `SqliteSwarmStore::open` creates the tables.

Main keeps one snapshot per version. `Swarm::prune` deletes the snapshots
that nothing needs: not the current main, and not the base of a claimed
item. It never deletes a snapshot at or above the current main version,
because a gate in flight can still commit it.

## 9. Hosts that sleep

A member can run on a host that sleeps when idle, such as a Fly.io Sprite. A
timer does not fire while such a host sleeps, and an outbound connection
does not keep it awake. So a member must not hold a long poll on the board.

Put the board on a store that does not sleep, such as the Postgres of the
app. Wake each member with its heartbeat or with an inbound message. A woken
member reads the board after its cursor, claims, works and submits. Its
claim holds while it sleeps, until the lease lapses.

## 10. Limits

- The gate runs inside one tool call. An eval longer than the tool timeout
  needs a gate of its own, as a durable workflow.
- The board does not push. A post does not wake a member.
- The merge is per file. Two changes to different lines of one file are a
  conflict.
