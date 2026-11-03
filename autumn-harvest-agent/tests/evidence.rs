//! The probe evidence bundle (issue #2377).
//!
//! Each test records a source run on SQLite, evaluates candidates against
//! it, and writes the bundle into an in-memory workspace store.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest_agent::eval::evidence::{
    DEFAULT_PART_BYTES, EvidenceError, EvidenceWorkspace, LABELS_PATH, MASK, MISSION_PATH,
    OVERLAY_PATH, PART_INFIX, SOURCE_STEM, TASK_PATH, read_bundle, render_source, source_labels,
    write_bundle,
};
use autumn_harvest_agent::eval::{Candidate, Evaluation, evaluate};
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentTask, BoxFuture, ContentPart, Evidence, FlowRules, FnTool,
    InMemoryWorkspaceBlobs, ModelTurn, ProfileOverlay, Rule, ToolEffect, WorkspaceBlobs,
    WorkspaceStore, agent_model_turn_info, sqlite,
};
use autumn_harvest_sqlite::RunState;
use common::{Recorder, Reply, ScriptedModel, answer, calls, recorded_tool, report, runtime};
use serde_json::json;

/// Blobs in memory that count each put.
#[derive(Debug, Default)]
struct CountingBlobs {
    inner: InMemoryWorkspaceBlobs,
    puts: AtomicUsize,
}

impl CountingBlobs {
    fn puts(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
    }
}

impl WorkspaceBlobs for CountingBlobs {
    fn put<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put(scope, digest, bytes)
    }

    fn get<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, AgentError>> {
        self.inner.get(scope, digest)
    }

    fn list<'a>(&'a self, scope: &'a str) -> BoxFuture<'a, Result<Vec<String>, AgentError>> {
        self.inner.list(scope)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a str,
        digest: &'a str,
    ) -> BoxFuture<'a, Result<bool, AgentError>> {
        self.inner.delete(scope, digest)
    }
}

fn counting_store() -> (Arc<CountingBlobs>, WorkspaceStore) {
    let blobs = Arc::new(CountingBlobs::default());
    let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
    (blobs, store)
}

/// Run `task` to its end on SQLite with `harness`. Returns the history and
/// the report.
async fn record_with(
    task: &AgentTask,
    harness: AgentHarness,
) -> (Vec<WorkflowEvent>, autumn_harvest_agent::AgentReport) {
    let dir = tempfile::tempdir().unwrap();
    let mut rt = runtime(&dir.path().join("agent.db"), harness);
    let exec = sqlite::start(&mut rt, task).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let history = rt.load_history(exec).unwrap();
    (history, report(state))
}

/// A source run with one `lookup` round and a final answer.
async fn one_lookup() -> Vec<WorkflowEvent> {
    let recorder = Arc::new(Recorder::default());
    let harness = AgentHarness::new(ScriptedModel::new(lookup_then("done"))).tool(recorded_tool(
        "lookup",
        ToolEffect::ReadOnly,
        &recorder,
    ));
    record_with(&AgentTask::new("look it up").system("Be brief."), harness)
        .await
        .0
}

fn lookup_then(text: &str) -> Vec<Reply> {
    vec![
        calls(&[("c1", "lookup", json!({"q": "x"}))], 5),
        answer(text, 5),
    ]
}

const FAST: &str = "fast-model-x";
const SLOW: &str = "slow-model-y";
const VERSION_ID: &str = "ov-version-0042";

/// Two evaluations of `source`: `FAST` repeats the source and names
/// itself, and `SLOW` answers at once.
async fn two_evaluations(source: &[WorkflowEvent]) -> [Evaluation; 2] {
    let fast = ScriptedModel::new(lookup_then(&format!("I am {FAST}. done")));
    let slow = ScriptedModel::new(vec![answer(&format!("{SLOW} says no"), 5)]);
    let recorder = Arc::new(Recorder::default());
    let harness = Arc::new(
        AgentHarness::new(ScriptedModel::new(Vec::new()))
            .model_named(FAST, fast)
            .model_named(SLOW, slow)
            .tool(recorded_tool("lookup", ToolEffect::ReadOnly, &recorder)),
    );
    let a = evaluate(source, &Candidate::shared(Arc::clone(&harness)).model(FAST))
        .await
        .unwrap();
    let b = evaluate(
        source,
        &Candidate::shared(harness)
            .model(SLOW)
            .system_prompt("Answer at once."),
    )
    .await
    .unwrap();
    assert_ne!(a.overlay_digest, b.overlay_digest);
    [a, b]
}

fn current_overlay() -> ProfileOverlay {
    ProfileOverlay {
        guardrails: Some("Never refund twice.".to_owned()),
        ..ProfileOverlay::default()
    }
}

/// Every file of the bundle that `manifest` names, by path, as text.
async fn files_of(
    store: &WorkspaceStore,
    manifest: &autumn_harvest_agent::BundleManifest,
) -> BTreeMap<String, String> {
    let bundle = read_bundle(store, manifest).await.unwrap();
    bundle
        .files
        .files()
        .iter()
        .map(|(path, file)| {
            let text = String::from_utf8(file.data.clone()).expect("each file is UTF-8");
            (path.clone(), text)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_source_splits_into_parts_that_join_to_the_single_render() {
    let mut source = one_lookup().await;
    // About 1 MiB of lines with multibyte text, in the final answer.
    let line = "the answer — ünïcødé line\n";
    let big = line.repeat((1024 * 1024) / line.len() + 1);
    let model = agent_model_turn_info().name;
    let mut model_ids = HashSet::new();
    for event in &mut source {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == model => {
                model_ids.insert(*activity_id);
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } if model_ids.contains(activity_id) => {
                let mut turn: ModelTurn = serde_json::from_value(output.clone()).unwrap();
                if turn.calls().is_empty() {
                    turn.content = vec![ContentPart::Text(big.clone())];
                    *output = serde_json::to_value(&turn).unwrap();
                }
            }
            _ => {}
        }
    }
    let single = render_source(&source).unwrap();
    assert!(single.len() > 1024 * 1024, "{}", single.len());
    assert!(single.contains(&big), "the render cuts nothing");

    let (_, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:long");
    let manifest = write_bundle(&workspace, &source, &[], &current_overlay())
        .await
        .unwrap();

    let files = files_of(&store, &manifest).await;
    assert!(
        !files.contains_key(&format!("{SOURCE_STEM}.md")),
        "a long source has no single file"
    );
    let parts: Vec<(&String, &String)> = files
        .iter()
        .filter(|(path, _)| path.starts_with(&format!("{SOURCE_STEM}{PART_INFIX}")))
        .collect();
    assert!(parts.len() >= 4, "{} parts", parts.len());
    for (path, text) in &parts {
        assert!(text.len() <= DEFAULT_PART_BYTES, "{path}: {}", text.len());
        assert!(!text.is_empty(), "{path} is empty");
    }
    let joined: String = parts.iter().map(|(_, text)| text.as_str()).collect();
    assert_eq!(joined.len(), single.len());
    assert!(joined == single, "the parts join to the single render");
    assert_eq!(
        parts.first().map(|(path, _)| path.as_str()),
        Some("rollouts/source.part-001.md")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_manifest_lists_every_file_and_each_file_exists() {
    let source = one_lookup().await;
    let [a, b] = two_evaluations(&source).await;
    let (_, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:layout").seed(7);
    let overlay = current_overlay();
    let manifest = write_bundle(&workspace, &source, &[a, b], &overlay)
        .await
        .unwrap();

    let files = files_of(&store, &manifest).await;
    let listed: BTreeMap<String, u64> = manifest
        .files
        .iter()
        .map(|file| (file.path.clone(), file.bytes))
        .collect();
    let stored: BTreeMap<String, u64> = files
        .iter()
        .map(|(path, text)| (path.clone(), text.len() as u64))
        .collect();
    assert_eq!(listed, stored, "the manifest lists each stored file");
    assert_eq!(manifest.files.len(), listed.len(), "no path repeats");
    assert_eq!(manifest.head.files as usize, files.len());

    assert_eq!(manifest.candidates.len(), 2);
    let ids: BTreeSet<&str> = manifest.candidates.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids.len(), 2, "no id repeats");
    let indexes: BTreeSet<usize> = manifest.candidates.iter().map(|c| c.index).collect();
    assert_eq!(indexes, BTreeSet::from([0, 1]));
    let mut want = vec![
        TASK_PATH.to_owned(),
        format!("{SOURCE_STEM}.md"),
        OVERLAY_PATH.to_owned(),
        LABELS_PATH.to_owned(),
        MISSION_PATH.to_owned(),
    ];
    for id in &ids {
        assert!(id.starts_with("c-"), "{id}");
        want.push(format!("rollouts/{id}.md"));
        want.push(format!("diffs/{id}.md"));
    }
    let want: BTreeSet<String> = want.into_iter().collect();
    let got: BTreeSet<String> = files.keys().cloned().collect();
    assert_eq!(got, want);

    assert_eq!(files[TASK_PATH], "look it up");
    let decoded: ProfileOverlay = serde_json::from_str(&files[OVERLAY_PATH]).unwrap();
    assert_eq!(decoded, overlay);
    let mission = &files[MISSION_PATH];
    assert_eq!(mission, &manifest.mission());
    for path in files.keys().filter(|path| *path != MISSION_PATH) {
        assert!(mission.contains(path.as_str()), "MISSION.md names {path}");
    }
    assert_eq!(
        Evidence::Bundle(manifest.clone()).render(),
        mission.as_str(),
        "the proposer pointer is MISSION.md"
    );
    assert!(files[&format!("{SOURCE_STEM}.md")].contains("lookup"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erased_source_is_refused_with_erased() {
    let mut source = one_lookup().await;
    for event in &mut source {
        if let WorkflowEvent::ActivityCompleted { output, .. } = event {
            *output = json!({"_harvest_erased": true});
        }
    }
    let (blobs, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store, "evidence:erased");

    let result = write_bundle(&workspace, &source, &[], &current_overlay()).await;

    assert!(matches!(result, Err(EvidenceError::Erased)), "{result:?}");
    assert_eq!(blobs.puts(), 0, "a refused source writes nothing");
    assert!(matches!(render_source(&source), Err(EvidenceError::Erased)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_labelled_source_reads_back_with_its_labels() {
    let inbox = FnTool::new("inbox", "Reads mail.", json!({}), |_| async {
        Ok(json!("mail"))
    })
    .effect(ToolEffect::ReadOnly)
    .label(labels::UNTRUSTED_INPUT)
    .shared();
    let model = ScriptedModel::new(vec![
        calls(&[("c1", "inbox", json!({}))], 5),
        answer("read", 5),
    ]);
    let harness = AgentHarness::new(model)
        .tool(inbox)
        .flow_rules(FlowRules::new().forbid(
            [labels::UNTRUSTED_INPUT, "effect:external"],
            Rule::Deny("no send after untrusted input".into()),
        ));
    let task = AgentTask::new("check mail").flow_labels(["prior-run"]);
    let (source, source_report) = record_with(&task, harness).await;
    assert!(source_report.flow_labels.contains(labels::UNTRUSTED_INPUT));

    let (_, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:labels");
    let manifest = write_bundle(&workspace, &source, &[], &current_overlay())
        .await
        .unwrap();
    let bundle = read_bundle(&store, &manifest).await.unwrap();

    assert!(
        bundle.flow_labels.is_superset(&source_report.flow_labels),
        "{:?} holds {:?}",
        bundle.flow_labels,
        source_report.flow_labels
    );
    assert!(bundle.flow_labels.contains("prior-run"));
    assert!(bundle.flow_labels.contains(labels::UNTRUSTED_INPUT));
    assert_eq!(bundle.flow_labels, manifest.flow_labels);
    assert_eq!(bundle.flow_labels, source_labels(&source).unwrap());
    assert!(bundle.files.get(LABELS_PATH).is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn no_file_names_a_candidate_model_or_overlay_id() {
    let source = one_lookup().await;
    let [a, b] = two_evaluations(&source).await;
    let digests = [a.overlay_digest.clone(), b.overlay_digest.clone()];
    let (_, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:anon")
        .seed(3)
        .hide(FAST)
        .hide(SLOW)
        .hide(VERSION_ID);
    let manifest = write_bundle(
        &workspace,
        &source,
        &[a.clone(), b.clone()],
        &current_overlay(),
    )
    .await
    .unwrap();

    let files = files_of(&store, &manifest).await;
    let needles = [FAST, SLOW, VERSION_ID, &digests[0], &digests[1]];
    for (path, text) in &files {
        for needle in needles {
            assert!(!text.contains(needle), "{path} names {needle}");
        }
    }
    let a_id = &manifest
        .candidates
        .iter()
        .find(|c| c.index == 0)
        .unwrap()
        .id;
    assert!(
        files[&format!("rollouts/{a_id}.md")].contains(MASK),
        "the self-naming text is masked"
    );

    // The input order does not show: [b, a] gives the same files.
    let (_, other_store) = counting_store();
    let swapped = EvidenceWorkspace::new(other_store.clone(), "evidence:anon")
        .seed(3)
        .hide(FAST)
        .hide(SLOW)
        .hide(VERSION_ID);
    let again = write_bundle(&swapped, &source, &[b, a], &current_overlay())
        .await
        .unwrap();
    assert_eq!(files_of(&other_store, &again).await, files);
    let a_again = &again.candidates.iter().find(|c| c.index == 1).unwrap().id;
    assert_eq!(a_again, a_id, "a keeps its id");
    assert_eq!(again.head.digest, manifest.head.digest);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hidden_digest_in_the_current_overlay_is_a_leak() {
    let source = one_lookup().await;
    let [a, b] = two_evaluations(&source).await;
    let overlay = ProfileOverlay {
        system: Some(format!("Copy {}.", a.overlay_digest)),
        ..ProfileOverlay::default()
    };
    let (blobs, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store, "evidence:leak");

    let result = write_bundle(&workspace, &source, &[a, b], &overlay).await;

    match result {
        Err(EvidenceError::Leak { path }) => assert_eq!(path, OVERLAY_PATH),
        other => panic!("a leak is refused: {other:?}"),
    }
    assert_eq!(blobs.puts(), 0, "a refused bundle writes nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bundle_is_one_put() {
    let source = one_lookup().await;
    let [a, b] = two_evaluations(&source).await;
    let (blobs, store) = counting_store();
    let workspace = EvidenceWorkspace::new(store.clone(), "evidence:one-put")
        .generation(4)
        .part_bytes(64);

    let manifest = write_bundle(&workspace, &source, &[a, b], &current_overlay())
        .await
        .unwrap();

    assert_eq!(blobs.puts(), 1, "one put writes the whole bundle");
    assert_eq!(manifest.head.generation, 4);
    assert!(
        manifest.files.len() > 7,
        "a small cap makes many parts, all in the one put"
    );
    assert_eq!(
        store.blobs().list("evidence:one-put").await.unwrap(),
        vec![manifest.head.key()]
    );
}
