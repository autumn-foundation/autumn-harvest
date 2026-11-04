//! The operator refine policy (issue #2394).
//!
//! Each test maps to one acceptance test of #2394. The adoption tests run
//! on the in-memory store. The read tests run a workflow on `SQLite`. The
//! process test starts a second process that runs a workflow, and sets
//! the policy from this one.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// The `#[workflow]` macro reads its input through a binding.
#![allow(clippy::used_underscore_binding)]

use std::collections::BTreeSet;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::refine::policy::OverlayPart;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, ChatRequest, ChatResponse, ErrorKind,
    InMemoryOverlayStore, ProfileOverlay, ProfileOverlayStore, Provenance, RefinePolicy,
    RefineStatus, SqliteOverlayStore, WaitReason, adopt, read_refine_policy, sqlite,
};
use autumn_harvest_sqlite::{RunState, SqliteRuntime};
use serde_json::json;

const KIND: &str = "support";
const CHILD_ENV: &str = "REFINE_POLICY_CHILD_DB";

#[derive(Debug)]
struct NoModel;

impl AgentModel for NoModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "no model")) })
    }
}

fn harness() -> AgentHarness {
    AgentHarness::new(Arc::new(NoModel))
        .model_named("m2", Arc::new(NoModel))
        .agent(AgentProfile::new(KIND).system("Base.").max_steps(20))
}

fn policy(value: serde_json::Value) -> RefinePolicy {
    serde_json::from_value(value).expect("a valid policy")
}

fn noon() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-03T12:00:00Z")
        .expect("time")
        .with_timezone(&Utc)
}

fn system(text: &str) -> ProfileOverlay {
    ProfileOverlay {
        system: Some(text.to_owned()),
        ..ProfileOverlay::default()
    }
}

fn store() -> InMemoryOverlayStore {
    InMemoryOverlayStore::new(Arc::new(harness()))
}

/// No policy stored: every setting reads its default, and a `system`-only
/// overlay waits for approval.
#[tokio::test]
async fn with_no_policy_a_system_overlay_waits() {
    let store = store();
    let state = store.policy(KIND).await.expect("policy");
    assert_eq!(state.policy, RefinePolicy::default());
    let run = ExecutionId::new();
    let adoption = adopt(&store, KIND, &system("New."), run, &state.policy)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AwaitingApproval);
    assert_eq!(
        adoption.wait,
        Some(WaitReason::PartsNeedApproval {
            parts: BTreeSet::from([OverlayPart::System])
        })
    );
    let version = adoption.version.expect("stored to wait");
    let stored = store.get(KIND, version).await.expect("get").expect("v1");
    assert_eq!(stored.provenance(), &Provenance::refine(run));
    assert_eq!(store.active(KIND).await.expect("active"), None);
}

/// Store `policy` as an operator does, then read it back as a run does.
async fn operator_sets(store: &dyn ProfileOverlayStore, value: serde_json::Value) -> RefinePolicy {
    let revision = store.policy(KIND).await.expect("policy").revision;
    store
        .set_policy(KIND, &policy(value), revision, Provenance::operator("ops"))
        .await
        .expect("set");
    store.policy(KIND).await.expect("policy").policy
}

/// `parts = [system]`: a `system`-only overlay is active at once, as
/// `auto:<run>`, with `refine_status = auto_adopted`. An overlay that
/// also changes `tool_guidance` waits.
#[tokio::test]
async fn a_system_policy_adopts_a_system_overlay_at_once() {
    let store = store();
    let allow = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}}),
    )
    .await;
    let run = ExecutionId::new();
    let adoption = adopt(&store, KIND, &system("New."), run, &allow)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AutoAdopted);
    assert_eq!(
        serde_json::to_value(adoption.refine_status).expect("encode"),
        json!("auto_adopted")
    );
    let active = store.active(KIND).await.expect("active").expect("set");
    assert_eq!(Some(active.version()), adoption.version);
    assert!(
        active.provenance().to_string().starts_with("auto:"),
        "{}",
        active.provenance()
    );
    assert_eq!(active.provenance(), &Provenance::auto(run));

    let harness = harness().tool(
        autumn_harvest_agent::FnTool::new(
            "search",
            "Search.",
            json!({"type": "object"}),
            |_| async { Ok(json!("found")) },
        )
        .shared(),
    );
    let store = InMemoryOverlayStore::new(Arc::new(harness));
    let allow = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}}),
    )
    .await;
    let wider = ProfileOverlay {
        tool_guidance: [("search".to_owned(), "Use it first.".to_owned())].into(),
        ..system("Newer.")
    };
    let adoption = adopt(&store, KIND, &wider, ExecutionId::new(), &allow)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AwaitingApproval);
    assert_eq!(
        adoption.wait,
        Some(WaitReason::PartsNeedApproval {
            parts: BTreeSet::from([OverlayPart::ToolGuidance])
        })
    );
    assert_eq!(store.active(KIND).await.expect("active"), None);
}

/// `parts = [system, model]`: an overlay that changes both is active at
/// once.
#[tokio::test]
async fn a_system_and_model_policy_adopts_both() {
    let store = store();
    let allow = operator_sets(
        &store,
        json!({
            "auto_adopt": {"parts": ["system", "model"]},
            "ceilings": {"models": ["default", "m2"]}
        }),
    )
    .await;
    let both = ProfileOverlay {
        model: Some("m2".to_owned()),
        ..system("New.")
    };
    let adoption = adopt(&store, KIND, &both, ExecutionId::new(), &allow)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AutoAdopted);
    let active = store.active(KIND).await.expect("active").expect("set");
    assert_eq!(active.overlay(), &both);
}

/// A subscriber that keeps the text of each `harvest.audit` event.
#[derive(Clone, Default)]
struct Audit(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Audit {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Audit {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `max_per_day = 1`: a second qualifying refine on the same UTC day
/// waits, and the audit event names the cap. With `unlimited`, it does not
/// wait. The store clock is fixed, so the test never spans midnight.
#[tokio::test]
async fn the_daily_cap_makes_a_second_refine_wait() {
    let audit = Audit::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(audit.clone())
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let store = store().with_clock(Arc::new(noon));
    let capped = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": 1}}),
    )
    .await;
    let first = adopt(&store, KIND, &system("One."), ExecutionId::new(), &capped)
        .await
        .expect("adopt");
    assert_eq!(first.refine_status, RefineStatus::AutoAdopted);
    let second = adopt(&store, KIND, &system("Two."), ExecutionId::new(), &capped)
        .await
        .expect("adopt");
    assert_eq!(second.refine_status, RefineStatus::AwaitingApproval);
    assert_eq!(second.wait, Some(WaitReason::DailyCap { cap: 1 }));
    let active = store.active(KIND).await.expect("active").expect("set");
    assert_eq!(Some(active.version()), first.version);
    let text = String::from_utf8(audit.0.lock().unwrap().clone()).expect("utf-8");
    assert!(text.contains("harvest.audit"), "{text}");
    assert!(text.contains("cap=1"), "{text}");
    assert!(text.contains("max_per_day"), "{text}");

    let unlimited = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}}),
    )
    .await;
    let third = adopt(
        &store,
        KIND,
        &system("Three."),
        ExecutionId::new(),
        &unlimited,
    )
    .await
    .expect("adopt");
    assert_eq!(third.refine_status, RefineStatus::AutoAdopted);
}

/// An overlay equal to the active one stores nothing and spends no cap. A
/// retry after a lost reply finds its own version, and reports it adopted.
#[tokio::test]
async fn an_unchanged_candidate_stores_nothing() {
    let store = store();
    let allow = operator_sets(&store, json!({"auto_adopt": {"parts": ["system"]}})).await;
    let run = ExecutionId::new();
    let first = adopt(&store, KIND, &system("One."), run, &allow)
        .await
        .expect("adopt");
    assert_eq!(first.refine_status, RefineStatus::AutoAdopted);
    let retry = adopt(&store, KIND, &system("One."), run, &allow)
        .await
        .expect("adopt");
    assert_eq!(retry, first, "a retry of the same run reports its adoption");
    let same = adopt(&store, KIND, &system("One."), ExecutionId::new(), &allow)
        .await
        .expect("adopt");
    assert_eq!(same.refine_status, RefineStatus::Unchanged);
    assert_eq!(same.version, None);
    assert_eq!(store.list(KIND).await.expect("list").len(), 1);
}

/// A retry after a lost reply never moves the pointer again. An operator
/// rolled the adoption back before the retry, and the rollback holds.
#[tokio::test]
async fn a_retry_never_undoes_a_rollback() {
    let store = store();
    let allow = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}}),
    )
    .await;
    let v1 = store
        .put(KIND, &system("Old."), Provenance::operator("ops"))
        .await
        .expect("v1");
    store
        .activate(KIND, v1, None, Provenance::operator("ops"))
        .await
        .expect("v1");
    let run = ExecutionId::new();
    let first = adopt(&store, KIND, &system("New."), run, &allow)
        .await
        .expect("adopt");
    assert_eq!(first.refine_status, RefineStatus::AutoAdopted);
    store
        .rollback(KIND, Provenance::operator("oncall"))
        .await
        .expect("rollback");
    let retry = adopt(&store, KIND, &system("New."), run, &allow)
        .await
        .expect("retry");
    assert_eq!(retry, first, "the retry reports the first adoption");
    let active = store.active(KIND).await.expect("active").expect("set");
    assert_eq!(active.version(), v1, "the rollback holds");
    assert_eq!(store.list(KIND).await.expect("list").len(), 2);
}

/// A validator that resets the policy of the kind once, from a second
/// connection. It runs after the gate reads the policy and before the
/// adoption commits, so it models a reset that lands in that window.
struct ResetDuringGate {
    db: std::path::PathBuf,
    armed: std::sync::atomic::AtomicBool,
    inner: AgentHarness,
}

impl autumn_harvest_agent::OverlayValidator for ResetDuringGate {
    fn validate(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        policy: &RefinePolicy,
    ) -> Result<(), autumn_harvest_agent::OverlayStoreError> {
        use autumn_harvest::overlay_log::{PolicyExpect, PolicyWrite};
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            let log = autumn_harvest_sqlite::SqliteOverlayLog::open(&self.db).expect("open");
            let write = PolicyWrite {
                body: None,
                expected: PolicyExpect::Any,
                actor_kind: "operator",
                actor: "oncall",
            };
            log.append_policy(kind, &write).expect("reset");
        }
        self.inner.validate(kind, overlay, policy)
    }
}

/// A policy reset that commits after the gate reads the policy still wins.
/// The adoption sees the new revision, runs the gate again, and waits.
#[tokio::test]
async fn a_reset_during_the_gate_still_wins() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("race.db");
    let gate = Arc::new(ResetDuringGate {
        db: db.clone(),
        armed: std::sync::atomic::AtomicBool::new(false),
        inner: harness(),
    });
    let store = SqliteOverlayStore::open(&db, gate.clone()).expect("store");
    let allow = operator_sets(
        &store,
        json!({"auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}}),
    )
    .await;
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let adoption = adopt(&store, KIND, &system("New."), ExecutionId::new(), &allow)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AwaitingApproval);
    assert_eq!(store.active(KIND).await.expect("active"), None);
    assert!(
        !store.policy(KIND).await.expect("policy").custom,
        "the reset holds"
    );
}

/// A run keeps the gate that it read, but a narrowing in the store reaches
/// the adoption at once. The stricter policy decides.
#[tokio::test]
async fn a_narrowed_store_policy_binds_a_running_refine() {
    let store = store();
    let wide = operator_sets(&store, json!({"auto_adopt": {"parts": ["system"]}})).await;
    operator_sets(&store, json!({})).await;
    let adoption = adopt(&store, KIND, &system("New."), ExecutionId::new(), &wide)
        .await
        .expect("adopt");
    assert_eq!(adoption.refine_status, RefineStatus::AwaitingApproval);
    assert_eq!(store.active(KIND).await.expect("active"), None);
}

/// A refine-like run: it reads the policy once, waits for a signal, then
/// returns the policy that it read.
#[workflow]
async fn read_once(ctx: &WorkflowContext, kind: String) -> Result<RefinePolicy, String> {
    let policy = read_refine_policy(ctx, &kind)
        .await
        .map_err(|err| err.to_string())?;
    ctx.wait_for_signal("go")
        .await
        .map_err(|err| err.to_string())?;
    Ok(policy)
}

fn runtime(db: &Path) -> SqliteRuntime {
    let store = SqliteOverlayStore::open(db, Arc::new(harness())).expect("store");
    let mut rt = SqliteRuntime::open(db).expect("runtime");
    sqlite::register(&mut rt, Arc::new(harness().overlay_store(Arc::new(store))));
    rt.register_workflow(&read_once_info());
    rt
}

async fn finish(rt: &mut SqliteRuntime, exec: ExecutionId) -> RefinePolicy {
    rt.send_signal(exec, "go", json!(null)).expect("signal");
    match rt.run_until_blocked(exec).await.expect("run") {
        RunState::Completed(value) => serde_json::from_value(value).expect("a policy"),
        other => panic!("the run did not finish: {other:?}"),
    }
}

/// A run reads the policy once. A change during the run does not reach
/// that run, and a replay gives the same read. The next run sees it.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_reads_the_policy_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("refine.db");
    let mut rt = runtime(&db);
    let exec = rt.start_workflow("read_once", json!(KIND)).expect("start");
    let blocked = rt.run_until_blocked(exec).await.expect("run");
    assert!(matches!(blocked, RunState::WaitingSignal(_)), "{blocked:?}");
    let operator = SqliteOverlayStore::open(&db, Arc::new(harness())).expect("store");
    let wide = policy(json!({"auto_adopt": {"parts": ["system"]}}));
    operator
        .set_policy(KIND, &wide, None, Provenance::operator("ops"))
        .await
        .expect("set");
    assert_eq!(finish(&mut rt, exec).await, RefinePolicy::default());

    let history = rt.load_history(exec).expect("history");
    let reads = history
        .iter()
        .filter(|event| {
            matches!(event, autumn_harvest::event::WorkflowEvent::ActivityScheduled { name, .. }
                if name == "agent_refine_policy")
        })
        .count();
    assert_eq!(reads, 1, "the run reads the policy once");
    let report = WorkflowReplayer::new()
        .register(vec![read_once_info()])
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{report:?}"
    );

    let next = rt.start_workflow("read_once", json!(KIND)).expect("start");
    rt.run_until_blocked(next).await.expect("run");
    assert_eq!(finish(&mut rt, next).await, wide);
}

/// The second process: it opens the database, runs one read, and prints
/// it. It waits for a line on stdin. Then it runs a second read and prints
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn child_process() {
    let Ok(db) = std::env::var(CHILD_ENV) else {
        return;
    };
    let mut rt = runtime(Path::new(&db));
    for _ in 0..2 {
        let exec = rt.start_workflow("read_once", json!(KIND)).expect("start");
        rt.run_until_blocked(exec).await.expect("run");
        let read = finish(&mut rt, exec).await;
        println!("POLICY {}", serde_json::to_string(&read).expect("encode"));
        std::io::stdout().flush().expect("flush");
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).expect("stdin");
    }
}

/// Setting the policy from one process changes the next refine run in a
/// second, running process, with no restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_set_reaches_a_running_second_process() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("refine.db");
    drop(runtime(&db));
    let mut child = Command::new(std::env::current_exe().expect("exe"))
        .args([
            "--exact",
            "child_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, &db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut lines = BufReader::new(child.stdout.take().expect("stdout")).lines();
    let mut next_read = || {
        lines
            .by_ref()
            .map_while(Result::ok)
            .find_map(|line| line.split_once("POLICY ").map(|(_, json)| json.to_owned()))
            .map(|json| serde_json::from_str::<RefinePolicy>(&json).expect("a policy"))
            .expect("the child prints a read")
    };
    assert_eq!(next_read(), RefinePolicy::default());

    let operator = SqliteOverlayStore::open(&db, Arc::new(harness())).expect("store");
    let wide = policy(json!({"probe_effects": ["read_only", "internal"]}));
    operator
        .set_policy(KIND, &wide, None, Provenance::operator("ops"))
        .await
        .expect("set");
    writeln!(stdin, "next").expect("stdin");
    assert_eq!(next_read(), wide, "the running process sees the new policy");
    writeln!(stdin, "done").expect("stdin");
    assert!(child.wait().expect("wait").success());
}
