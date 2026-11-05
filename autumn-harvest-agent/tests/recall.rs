//! The long-horizon recall eval (issue #2455).
//!
//! A test labelled "AC n" checks acceptance criterion n of issue #2455.
//! `docs/design/DESIGN-2455.md` maps each test to its criterion. The model
//! is `ReaderModel`, which answers only from its request. A score of 0
//! therefore means that the fact was not in the request.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use autumn_harvest_agent::recall::{
    self, EPISODES, MEMORY, NOTES, ReaderModel, RecallFixture, RecallOutcome, RecallReport, WINDOW,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, BoxFuture, ChatRequest, ChatResponse,
    ChatRole, ContentPart, ErrorKind, sqlite,
};
use autumn_harvest_sqlite::SqliteRuntime;
use common::ScriptedModel;

/// The window of every test, in messages.
const WINDOW_SIZE: u32 = 2;

/// The item of AC 1 and AC 2. Its fact is in op 1 and its question in op 3.
const OP1_ITEM: &str = "vault-code";

/// The item whose fact is in the op just before the question.
const CONTROL_ITEM: &str = "parcel-number";

/// The config whose model fails each call.
const BROKEN: &str = "broken";

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("recall.db");
    (dir, path)
}

/// The recall harness on `model`, with one more profile that fails.
fn harness(model: Arc<dyn AgentModel>, window: u32) -> AgentHarness {
    let broken = ScriptedModel::new(Vec::new());
    recall::harness(model, window)
        .model_named(BROKEN, broken)
        .agent(AgentProfile::new(BROKEN).window(window).model(BROKEN))
}

/// Run the sweep of `configs` over `fixture` on the database `db`.
async fn sweep_on(
    db: &Path,
    model: Arc<dyn AgentModel>,
    window: u32,
    fixture: &RecallFixture,
    configs: &[&str],
) -> RecallReport {
    let harness = Arc::new(harness(model, window));
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    sqlite::register(&mut rt, Arc::clone(&harness));
    let configs: Vec<String> = configs.iter().map(|name| (*name).to_owned()).collect();
    recall::sweep(&mut rt, &harness, fixture, &configs).await
}

/// Run the sweep on `ReaderModel` with the test window.
async fn sweep(
    db: &Path,
    model: &Arc<ReaderModel>,
    fixture: &RecallFixture,
    configs: &[&str],
) -> RecallReport {
    sweep_on(db, model.clone(), WINDOW_SIZE, fixture, configs).await
}

/// The built-in fixture.
fn fixture() -> RecallFixture {
    RecallFixture::builtin().expect("the built-in fixture is valid")
}

/// The fixture with the one item `id`.
fn only(id: &str) -> RecallFixture {
    let item = fixture()
        .item(id)
        .cloned()
        .unwrap_or_else(|| panic!("the fixture has the item {id}"));
    RecallFixture { items: vec![item] }
}

/// The answer of a scored row.
fn answer(report: &RecallReport, config: &str, item: &str) -> String {
    match &report.row(config, item).expect("a row").outcome {
        RecallOutcome::Scored { answer, .. } => answer.clone(),
        other => panic!("{config}/{item} has no score: {other:?}"),
    }
}

/// The row of `item` under `config` failed.
fn failed(report: &RecallReport, config: &str, item: &str) -> bool {
    matches!(
        report.row(config, item).expect("a row").outcome,
        RecallOutcome::Failed { .. }
    )
}

/// AC 1: with `window = 2` and no notes, a fact of op 1 scores 0.
#[tokio::test(flavor = "multi_thread")]
async fn with_the_window_only_a_fact_of_op_1_scores_0() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only(OP1_ITEM), &[WINDOW]).await;

    assert_eq!(report.score(WINDOW, OP1_ITEM), Some(0.0));
    assert_eq!(answer(&report, WINDOW, OP1_ITEM), ReaderModel::UNKNOWN);
}

/// AC 2: the same item scores 1 with notes on.
#[tokio::test(flavor = "multi_thread")]
async fn with_notes_on_the_same_item_scores_1() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only(OP1_ITEM), &[NOTES]).await;

    assert_eq!(report.score(NOTES, OP1_ITEM), Some(1.0));
    assert_eq!(answer(&report, NOTES, OP1_ITEM), "7319");
}

/// AC 3: the report lists every item under every config.
#[tokio::test(flavor = "multi_thread")]
async fn the_report_lists_every_item_under_every_config() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let fixture = fixture();
    let configs = [WINDOW, NOTES, MEMORY, EPISODES];
    let report = sweep(&db, &model, &fixture, &configs).await;

    assert_eq!(report.configs, configs);
    assert_eq!(report.rows.len(), configs.len() * fixture.items.len());
    for config in configs {
        for item in &fixture.items {
            assert!(!failed(&report, config, &item.id), "{config}/{}", item.id);
        }
    }
    let table = report.to_markdown();
    for item in &fixture.items {
        assert!(table.contains(&format!("| {} |", item.id)), "{table}");
    }
    for config in configs {
        assert!(table.contains(config), "{table}");
    }
    // Notes and episodes remember more than the window alone. The memory
    // tool alone adds nothing for a model that never calls it.
    let mean = |config| report.mean(config).expect("a mean");
    assert!(mean(NOTES) > mean(WINDOW), "{table}");
    assert!(mean(EPISODES) > mean(MEMORY), "{table}");
    assert!(
        (mean(MEMORY) - mean(WINDOW)).abs() < f64::EPSILON,
        "{table}"
    );
}

/// AC 4: one config's crash does not lose another's scores.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_config_keeps_the_scores_of_the_others() {
    let fixture = fixture();
    let (_dir, db) = fresh_db();
    let clean = sweep(&db, &ReaderModel::new(), &fixture, &[WINDOW, NOTES]).await;
    let (_dir, db) = fresh_db();
    let mixed = sweep(&db, &ReaderModel::new(), &fixture, &[WINDOW, BROKEN, NOTES]).await;

    assert_eq!(mixed.rows.len(), 3 * fixture.items.len());
    for item in &fixture.items {
        assert!(failed(&mixed, BROKEN, &item.id));
        assert_eq!(mixed.score(BROKEN, &item.id), None);
        for config in [WINDOW, NOTES] {
            assert_eq!(mixed.row(config, &item.id), clean.row(config, &item.id));
        }
    }
    assert_eq!(mixed.mean(BROKEN), None);
    assert!(mixed.to_markdown().contains("error"));
}

/// AC 4: a config with no profile fails before any op. The other rows
/// keep their scores.
#[tokio::test(flavor = "multi_thread")]
async fn a_config_with_no_profile_fails_alone() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only(OP1_ITEM), &["ghost", NOTES]).await;

    assert!(failed(&report, "ghost", OP1_ITEM));
    assert_eq!(report.score(NOTES, OP1_ITEM), Some(1.0));
}

/// A model that is rate limited on each question, and reads otherwise.
#[derive(Debug)]
struct LimitedOnQuestions(Arc<ReaderModel>);

impl AgentModel for LimitedOnQuestions {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let asks = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == ChatRole::User)
            .is_some_and(|message| {
                message
                    .content
                    .iter()
                    .any(|part| matches!(part, ContentPart::Text(text) if text.starts_with("What")))
            });
        if asks {
            let err = AgentError::new(ErrorKind::RateLimited, "slow down");
            return Box::pin(std::future::ready(Err(err)));
        }
        self.0.chat(request)
    }
}

/// An op that waits on a retry timer did not end. The older answer in the
/// state is not scored.
#[tokio::test(flavor = "multi_thread")]
async fn an_op_that_waits_for_a_retry_fails_its_item() {
    let (_dir, db) = fresh_db();
    let model = Arc::new(LimitedOnQuestions(ReaderModel::new()));
    let report = sweep_on(&db, model, WINDOW_SIZE, &only(CONTROL_ITEM), &[WINDOW]).await;

    assert!(failed(&report, WINDOW, CONTROL_ITEM), "{report:?}");
}

/// The control item: the fact is one op before the question, so the window
/// keeps it. A 0 on another item is therefore a loss, not a broken harness.
#[tokio::test(flavor = "multi_thread")]
async fn the_control_item_scores_1_with_the_window_only() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only(CONTROL_ITEM), &[WINDOW]).await;

    assert_eq!(report.score(WINDOW, CONTROL_ITEM), Some(1.0));
}

/// The episodes config recalls the fact of op 1 from its consolidated facts.
#[tokio::test(flavor = "multi_thread")]
async fn with_episodes_on_the_same_item_scores_1() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only(OP1_ITEM), &[EPISODES]).await;

    assert_eq!(report.score(EPISODES, OP1_ITEM), Some(1.0));
}

/// A later value replaces an earlier one, and every config that keeps the
/// fact answers the later value.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_stated_value_wins() {
    let (_dir, db) = fresh_db();
    let model = ReaderModel::new();
    let report = sweep(&db, &model, &only("wifi-pin"), &[NOTES, EPISODES]).await;

    assert_eq!(answer(&report, NOTES, "wifi-pin"), "6604");
    assert_eq!(answer(&report, EPISODES, "wifi-pin"), "6604");
}

/// A rerun of the sweep on the same database delivers each op once. It
/// calls no model, and it gives the same report.
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_after_the_sweep_calls_no_model() {
    let (_dir, db) = fresh_db();
    let fixture = fixture();
    let configs = [WINDOW, NOTES, MEMORY, EPISODES];
    let once = ReaderModel::new();
    let first = sweep(&db, &once, &fixture, &configs).await;
    assert!(once.calls() > 0, "the first sweep calls the model");

    let again = ReaderModel::new();
    let second = sweep(&db, &again, &fixture, &configs).await;
    assert_eq!(again.calls(), 0);
    assert_eq!(second, first);
}

/// A model with no id, which reads like `ReaderModel`.
#[derive(Debug)]
struct Unnamed(Arc<ReaderModel>);

impl AgentModel for Unnamed {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.0.chat(request)
    }
}

/// A model with no id has no stable identity. A rerun on the same database
/// gets new agents, so it never reports the rows of another model.
#[tokio::test(flavor = "multi_thread")]
async fn a_model_with_no_id_never_reuses_a_finished_item() {
    let (_dir, db) = fresh_db();
    let fixture = only(OP1_ITEM);
    let first = Arc::new(Unnamed(ReaderModel::new()));
    sweep_on(&db, first, WINDOW_SIZE, &fixture, &[WINDOW]).await;

    let second = ReaderModel::new();
    let unnamed = Arc::new(Unnamed(second.clone()));
    let report = sweep_on(&db, unnamed, WINDOW_SIZE, &fixture, &[WINDOW]).await;
    assert!(second.calls() > 0, "the second model runs its own agents");
    assert_eq!(report.score(WINDOW, OP1_ITEM), Some(0.0));
}

/// A model with a given id, which reads like `ReaderModel`.
#[derive(Debug)]
struct Named(&'static str, Arc<ReaderModel>);

impl AgentModel for Named {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.1.chat(request)
    }

    fn model_id(&self) -> Option<&str> {
        Some(self.0)
    }
}

/// Run `episodes` with its passes on the harness model `pass`, which has the
/// id `id`.
async fn sweep_with_pass(db: &Path, id: &'static str, pass: &Arc<ReaderModel>) -> RecallReport {
    let settings = autumn_harvest_agent::EpisodeSettings::default()
        .every(1)
        .model("pass");
    let profile = recall::profiles(WINDOW_SIZE)
        .into_iter()
        .find(|profile| profile.kind == EPISODES)
        .expect("the episodes profile")
        .episodes(settings);
    let harness = Arc::new(
        harness(ReaderModel::new(), WINDOW_SIZE)
            .model_named("pass", Arc::new(Named(id, pass.clone())))
            .agent(profile),
    );
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    sqlite::register(&mut rt, Arc::clone(&harness));
    let configs = [EPISODES.to_owned()];
    recall::sweep(&mut rt, &harness, &only(OP1_ITEM), &configs).await
}

/// A new pass model gets new agents, so a rerun never reports the facts of
/// the old pass model.
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_with_another_pass_model_runs_again() {
    let (_dir, db) = fresh_db();
    let first = ReaderModel::new();
    let report = sweep_with_pass(&db, "pass-a", &first).await;
    assert!(first.calls() > 0, "the pass runs on the pass model");
    assert_eq!(report.score(EPISODES, OP1_ITEM), Some(1.0));

    let second = ReaderModel::new();
    sweep_with_pass(&db, "pass-b", &second).await;
    assert!(second.calls() > 0, "the new pass model runs its own agents");
}

/// A rerun with another window on the same database gets new agents, so it
/// never reports the rows of the old window.
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_with_another_window_runs_again() {
    let (_dir, db) = fresh_db();
    let fixture = only(OP1_ITEM);
    let narrow = sweep(&db, &ReaderModel::new(), &fixture, &[WINDOW]).await;
    assert_eq!(narrow.score(WINDOW, OP1_ITEM), Some(0.0));

    let wide = ReaderModel::new();
    let report = sweep_on(&db, wide.clone(), 8, &fixture, &[WINDOW]).await;
    assert!(wide.calls() > 0, "the wide window runs its own agents");
    assert_eq!(report.score(WINDOW, OP1_ITEM), Some(1.0));
}

/// A validator that accepts each overlay.
struct AcceptAll;

impl autumn_harvest_agent::OverlayValidator for AcceptAll {
    fn validate(
        &self,
        _kind: &str,
        _overlay: &autumn_harvest_agent::ProfileOverlay,
        _policy: &autumn_harvest_agent::RefinePolicy,
    ) -> Result<(), autumn_harvest_agent::OverlayStoreError> {
        Ok(())
    }
}

/// Put an overlay with the system prompt `system` on `kind`, and activate it.
async fn activate_system(store: &Arc<autumn_harvest_agent::InMemoryOverlayStore>, system: &str) {
    use autumn_harvest_agent::{ProfileOverlay, ProfileOverlayStore, Provenance};

    let overlay = ProfileOverlay {
        system: Some(system.to_owned()),
        ..ProfileOverlay::default()
    };
    let who = || Provenance::operator("ops");
    let active = store.active(WINDOW).await.expect("active");
    let version = store.put(WINDOW, &overlay, who()).await.expect("put");
    let expected = active.map(|stored| stored.version());
    store
        .activate(WINDOW, version, expected, who())
        .await
        .expect("activate");
}

/// Run the window config on `db` with `store` as the overlay store.
async fn sweep_with_overlays(
    db: &Path,
    store: &Arc<autumn_harvest_agent::InMemoryOverlayStore>,
    model: &Arc<ReaderModel>,
) -> RecallReport {
    let harness = Arc::new(harness(model.clone(), WINDOW_SIZE).overlay_store(store.clone()));
    let mut rt = SqliteRuntime::open(db).expect("the database opens");
    sqlite::register(&mut rt, Arc::clone(&harness));
    let configs = [WINDOW.to_owned()];
    recall::sweep(&mut rt, &harness, &only(OP1_ITEM), &configs).await
}

/// Issue #2556: an activation that changes the prompt gets new agents, so a
/// rerun never reports the scores of the old overlay.
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_after_an_activation_runs_again() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(autumn_harvest_agent::InMemoryOverlayStore::new(Arc::new(
        AcceptAll,
    )));
    activate_system(&store, "Answer briefly.").await;
    let first = ReaderModel::new();
    sweep_with_overlays(&db, &store, &first).await;
    assert!(first.calls() > 0, "the first sweep calls the model");

    activate_system(&store, "Answer in full.").await;
    let second = ReaderModel::new();
    sweep_with_overlays(&db, &store, &second).await;
    assert!(second.calls() > 0, "the new overlay runs its own agents");
}

/// Issue #2556: a rerun with the same active overlay reuses the agents.
#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_with_no_activation_calls_no_model() {
    let (_dir, db) = fresh_db();
    let store = Arc::new(autumn_harvest_agent::InMemoryOverlayStore::new(Arc::new(
        AcceptAll,
    )));
    activate_system(&store, "Answer briefly.").await;
    sweep_with_overlays(&db, &store, &ReaderModel::new()).await;

    let again = ReaderModel::new();
    sweep_with_overlays(&db, &store, &again).await;
    assert_eq!(again.calls(), 0);
}
