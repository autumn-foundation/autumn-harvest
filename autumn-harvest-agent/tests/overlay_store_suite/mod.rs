//! One suite for every `ProfileOverlayStore` (issue #2371).
//!
//! `overlay_store.rs` runs it on the in-memory and `SQLite` stores.
//! `overlay_store_postgres.rs` runs it on Postgres. Each case uses a new
//! kind, so the cases can share one database.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use autumn_harvest::ExecutionId;
use autumn_harvest::chrono::{DateTime, TimeDelta, Utc};
use autumn_harvest_agent::agent::overlay::apply;
use autumn_harvest_agent::agent::overlay_store::AutoAdoptOutcome;
use autumn_harvest_agent::refine::policy::{OverlayPart, RefinePolicy};
use autumn_harvest_agent::{
    AgentProfile, OverlayError, OverlayStoreError, OverlayValidator, OverlayVersion,
    ProfileOverlay, ProfileOverlayStore, Provenance, Rollback, StoreClock,
};

/// Makes a store that validates with the given validator.
pub type MakeStore<'a> =
    &'a (dyn Fn(Arc<dyn OverlayValidator>) -> Box<dyn ProfileOverlayStore> + Sync);

/// Makes a store that validates with the given validator and reads the
/// given clock (issue #2394).
pub type MakeClockedStore<'a> =
    &'a (dyn Fn(Arc<dyn OverlayValidator>, StoreClock) -> Box<dyn ProfileOverlayStore> + Sync);

/// A clock that a test moves by hand.
#[derive(Clone)]
pub struct HandClock(Arc<std::sync::Mutex<DateTime<Utc>>>);

impl HandClock {
    /// A clock at `at`.
    pub fn at(at: DateTime<Utc>) -> Self {
        Self(Arc::new(std::sync::Mutex::new(at)))
    }

    /// Move the clock to `at`.
    pub fn set(&self, at: DateTime<Utc>) {
        *self.0.lock().expect("clock") = at;
    }

    /// The clock as a store clock.
    pub fn clock(&self) -> StoreClock {
        let cell = Arc::clone(&self.0);
        Arc::new(move || *cell.lock().expect("clock"))
    }
}

/// A validator for any kind. Its base profile allows 10 steps, or 5 when
/// it is tight, and the model `m1`.
#[derive(Default)]
pub struct Gate {
    tight: AtomicBool,
}

impl Gate {
    fn base(&self, kind: &str) -> AgentProfile {
        let steps = if self.tight.load(Ordering::SeqCst) {
            5
        } else {
            10
        };
        AgentProfile::new(kind)
            .system("Help the user.")
            .max_steps(steps)
            .max_total_tokens(5_000)
            .model("m1")
    }
}

impl OverlayValidator for Gate {
    fn validate(
        &self,
        kind: &str,
        overlay: &ProfileOverlay,
        policy: &RefinePolicy,
    ) -> Result<(), OverlayStoreError> {
        let profile = self.base(kind);
        let registry: BTreeSet<String> = ["m1", "m2"].map(str::to_owned).into();
        apply(
            &profile,
            overlay,
            &policy.ceilings.resolve(&profile),
            &registry,
        )?;
        Ok(())
    }
}

/// A kind that no earlier case or run used.
fn kind(case: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{case}-{nanos}-{}", NEXT.fetch_add(1, Ordering::SeqCst))
}

fn narrow(steps: u32, system: &str) -> ProfileOverlay {
    ProfileOverlay {
        system: Some(system.to_owned()),
        max_steps: Some(steps),
        ..ProfileOverlay::default()
    }
}

fn ops() -> Provenance {
    Provenance::operator("ops@example")
}

const fn version(n: u64) -> OverlayVersion {
    match OverlayVersion::new(n) {
        Some(version) => version,
        None => panic!("a version is at least 1"),
    }
}

/// Run every case on stores from `make` and `clocked`.
pub async fn run_all(make: MakeStore<'_>, clocked: MakeClockedStore<'_>) {
    get_returns_the_bytes_written_for_each_version(make).await;
    put_twice_returns_increasing_versions(make).await;
    activate_of_an_unknown_version_changes_nothing(make).await;
    two_activates_with_one_expected_pointer_have_one_winner(make).await;
    a_widening_put_is_refused_with_the_overlay_error(make).await;
    activate_validates_the_version_again(make).await;
    a_rollback_restores_an_exact_earlier_version(make).await;
    provenance_is_checked_and_kept(make).await;
    an_unknown_kind_reads_empty(make).await;
    concurrent_puts_get_distinct_versions(make).await;
    kinds_do_not_share_versions(make).await;
    the_pointer_compares_exactly(make).await;
    each_move_is_logged_with_its_actor(make).await;
    a_rollback_activates_the_version_before(make).await;
    a_rollback_with_no_earlier_activation_changes_nothing(make).await;
    an_activation_checks_its_actor(make).await;
    a_kind_with_no_policy_reads_every_default(make).await;
    a_set_policy_is_read_back_and_logged(make).await;
    a_stale_policy_revision_changes_nothing(make).await;
    reset_restores_every_default_and_is_logged(make).await;
    only_an_operator_sets_the_policy(make).await;
    a_policy_ceiling_widens_the_validator(make).await;
    an_auto_adoption_records_auto_provenance(clocked).await;
    the_daily_cap_holds_in_the_store(clocked).await;
    an_auto_adoption_checks_the_pointer(make).await;
    a_rollback_undoes_an_auto_adoption(make).await;
    the_store_enforces_both_policies(make).await;
    only_auto_adopt_writes_an_auto_actor(make).await;
}

/// After `put` v2, `get(kind, v1)` returns the bytes written for v1.
pub async fn get_returns_the_bytes_written_for_each_version(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("bytes");
    let first = narrow(9, "First prompt.");
    let second = ProfileOverlay {
        guardrails: Some("Never share keys.".to_owned()),
        max_total_tokens: Some(4_000),
        ..narrow(8, "Second prompt.")
    };
    let v1 = store.put(&kind, &first, ops()).await.expect("put v1");
    let v2 = store.put(&kind, &second, ops()).await.expect("put v2");
    let got = store
        .get(&kind, v1)
        .await
        .expect("get v1")
        .expect("v1 exists");
    assert_eq!(got.bytes(), serde_json::to_vec(&first).expect("encode"));
    assert_eq!(got.overlay(), &first);
    assert_eq!(got.version(), v1);
    let got = store
        .get(&kind, v2)
        .await
        .expect("get v2")
        .expect("v2 exists");
    assert_eq!(got.bytes(), serde_json::to_vec(&second).expect("encode"));
}

/// `put` twice returns `v1 < v2`, and `list` returns both in order.
pub async fn put_twice_returns_increasing_versions(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("order");
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    let v2 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v2");
    assert!(v1 < v2, "{v1} < {v2}");
    assert_eq!(v1, version(1), "a new kind starts at 1");
    assert_eq!(v2, version(2));
    let listed: Vec<_> = store
        .list(&kind)
        .await
        .expect("list")
        .iter()
        .map(autumn_harvest_agent::StoredOverlay::version)
        .collect();
    assert_eq!(listed, [v1, v2]);
}

/// `activate` of a version that does not exist returns `UnknownVersion`,
/// and `active` is unchanged.
pub async fn activate_of_an_unknown_version_changes_nothing(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("unknown");
    let missing = version(99);
    let err = store.activate(&kind, missing, None, ops()).await;
    assert_eq!(
        err,
        Err(OverlayStoreError::UnknownVersion { version: missing })
    );
    assert_eq!(store.active(&kind).await.expect("active"), None);

    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    store
        .activate(&kind, v1, None, ops())
        .await
        .expect("activate v1");
    let err = store.activate(&kind, missing, Some(v1), ops()).await;
    assert_eq!(
        err,
        Err(OverlayStoreError::UnknownVersion { version: missing })
    );
    let active = store.active(&kind).await.expect("active").expect("v1");
    assert_eq!(active.version(), v1);
}

/// Two activates with the same `expected_active`: exactly one succeeds.
/// The other returns `Conflict { actual }`.
///
/// The two calls overlap only on Postgres, where each takes its own pool
/// connection. The in-memory and `SQLite` futures finish in one poll, so
/// `join!` runs them in turn. `autumn-harvest-sqlite` races two connections
/// on one file in its own tests.
pub async fn two_activates_with_one_expected_pointer_have_one_winner(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    for round in 0..8 {
        let kind = kind(&format!("race{round}"));
        let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
        let v2 = store.put(&kind, &narrow(8, "b"), ops()).await.expect("v2");
        let v3 = store.put(&kind, &narrow(7, "c"), ops()).await.expect("v3");
        store
            .activate(&kind, v1, None, ops())
            .await
            .expect("activate v1");
        let (left, right) = tokio::join!(
            store.activate(&kind, v2, Some(v1), ops()),
            store.activate(&kind, v3, Some(v1), ops()),
        );
        let winner = match (&left, &right) {
            (Ok(()), Err(_)) => v2,
            (Err(_), Ok(())) => v3,
            _ => panic!("want exactly one winner, got {left:?} and {right:?}"),
        };
        let loser = if winner == v2 { right } else { left };
        assert_eq!(
            loser,
            Err(OverlayStoreError::Conflict {
                actual: Some(winner)
            })
        );
        let active = store.active(&kind).await.expect("active").expect("set");
        assert_eq!(active.version(), winner);
        // The race logs one move only: from v1 to the winner (issue #2374).
        let last = store
            .last_activation(&kind)
            .await
            .expect("last")
            .expect("move");
        assert_eq!((last.seq(), last.from(), last.to()), (2, Some(v1), winner));
    }
}

/// Every stored version passes `overlay::apply`. A widening put fails with
/// the `OverlayError`, and stores nothing.
pub async fn a_widening_put_is_refused_with_the_overlay_error(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("widen");
    let more_steps = narrow(11, "a");
    assert_eq!(
        store.put(&kind, &more_steps, ops()).await,
        Err(OverlayStoreError::Overlay(OverlayError::AboveCeiling {
            field: "max_steps"
        }))
    );
    let other_model = ProfileOverlay {
        model: Some("m2".to_owned()),
        ..ProfileOverlay::default()
    };
    assert_eq!(
        store.put(&kind, &other_model, ops()).await,
        Err(OverlayStoreError::Overlay(OverlayError::ModelNotAllowed {
            model: "m2".to_owned()
        }))
    );
    assert_eq!(store.list(&kind).await.expect("list"), []);
}

/// A deploy can narrow the base profile. Then an old version widens it, and
/// `activate` refuses it.
pub async fn activate_validates_the_version_again(make: MakeStore<'_>) {
    let gate = Arc::new(Gate::default());
    let store = make(gate.clone());
    let kind = kind("narrowed");
    let v1 = store.put(&kind, &narrow(8, "a"), ops()).await.expect("v1");
    gate.tight.store(true, Ordering::SeqCst);
    assert_eq!(
        store.activate(&kind, v1, None, ops()).await,
        Err(OverlayStoreError::Overlay(OverlayError::AboveCeiling {
            field: "max_steps"
        }))
    );
    assert_eq!(store.active(&kind).await.expect("active"), None);
}

/// A rollback activates an earlier version. The active version then has its
/// exact bytes.
pub async fn a_rollback_restores_an_exact_earlier_version(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("rollback");
    let v1 = store
        .put(&kind, &narrow(9, "One."), ops())
        .await
        .expect("v1");
    let v2 = store
        .put(&kind, &narrow(6, "Two."), ops())
        .await
        .expect("v2");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    store
        .activate(&kind, v2, Some(v1), ops())
        .await
        .expect("v2");
    store
        .activate(&kind, v1, Some(v2), ops())
        .await
        .expect("roll back");
    let active = store.active(&kind).await.expect("active").expect("set");
    let written = store.get(&kind, v1).await.expect("get").expect("v1");
    assert_eq!(active, written);
    assert_eq!(
        active.bytes(),
        serde_json::to_vec(&narrow(9, "One.")).expect("encode")
    );
}

/// A put refuses an empty operator id. Each provenance reads back as it
/// was written.
pub async fn provenance_is_checked_and_kept(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("author");
    for bad in ["", "  ", "line\nbreak"] {
        assert!(matches!(
            store
                .put(&kind, &narrow(9, "a"), Provenance::operator(bad))
                .await,
            Err(OverlayStoreError::InvalidProvenance { .. })
        ));
    }
    let run = ExecutionId::new();
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    let v2 = store
        .put(&kind, &narrow(9, "b"), Provenance::refine(run))
        .await
        .expect("v2");
    let got = store.get(&kind, v1).await.expect("get").expect("v1");
    assert_eq!(got.provenance(), &ops());
    let got = store.get(&kind, v2).await.expect("get").expect("v2");
    assert_eq!(got.provenance(), &Provenance::refine(run));
}

/// A kind with no versions has no active version, no version and an empty
/// list.
pub async fn an_unknown_kind_reads_empty(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("empty");
    assert_eq!(store.active(&kind).await.expect("active"), None);
    assert_eq!(store.get(&kind, version(1)).await.expect("get"), None);
    assert_eq!(store.list(&kind).await.expect("list"), []);
}

/// Puts of one kind in flight together get the versions 1 to N, each once.
pub async fn concurrent_puts_get_distinct_versions(make: MakeStore<'_>) {
    const PUTS: u64 = 8;
    let store = make(Arc::new(Gate::default()));
    let kind = kind("puts");
    let overlays: Vec<_> = (0..PUTS)
        .map(|n| narrow(9, &format!("prompt {n}")))
        .collect();
    let results = futures::future::join_all(
        overlays
            .iter()
            .map(|overlay| store.put(&kind, overlay, ops())),
    )
    .await;
    let mut versions: Vec<u64> = results
        .into_iter()
        .map(|version| version.expect("put").get())
        .collect();
    versions.sort_unstable();
    assert_eq!(versions, (1..=PUTS).collect::<Vec<_>>());
    let listed = store.list(&kind).await.expect("list");
    assert_eq!(listed.len(), overlays.len());
    for stored in listed {
        let written = serde_json::from_slice::<ProfileOverlay>(stored.bytes()).expect("decode");
        assert_eq!(&written, stored.overlay());
    }
}

/// A version of one kind is not a version of another kind.
pub async fn kinds_do_not_share_versions(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let (left, right) = (kind("left"), kind("right"));
    let v1 = store
        .put(&left, &narrow(9, "left"), ops())
        .await
        .expect("v1");
    assert_eq!(store.get(&right, v1).await.expect("get"), None);
    assert_eq!(
        store.activate(&right, v1, None, ops()).await,
        Err(OverlayStoreError::UnknownVersion { version: v1 })
    );
    let r1 = store
        .put(&right, &narrow(8, "right"), ops())
        .await
        .expect("r1");
    assert_eq!(r1, version(1), "each kind counts from 1");
    let got = store.get(&right, r1).await.expect("get").expect("r1");
    assert_eq!(got.overlay(), &narrow(8, "right"));
    assert_eq!(got.kind(), right);
}

/// An expected pointer must equal the pointer. An unset pointer is not a
/// set one. Activating the active version again over itself succeeds.
pub async fn the_pointer_compares_exactly(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("pointer");
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    assert_eq!(
        store.activate(&kind, v1, Some(v1), ops()).await,
        Err(OverlayStoreError::Conflict { actual: None })
    );
    store
        .activate(&kind, v1, None, ops())
        .await
        .expect("activate v1");
    assert_eq!(
        store.activate(&kind, v1, None, ops()).await,
        Err(OverlayStoreError::Conflict { actual: Some(v1) })
    );
    store
        .activate(&kind, v1, Some(v1), ops())
        .await
        .expect("the same version again");
    let far = OverlayVersion::new(u64::MAX).expect("max");
    assert_eq!(
        store.activate(&kind, v1, Some(far), ops()).await,
        Err(OverlayStoreError::Conflict { actual: Some(v1) })
    );
}

/// Each move of the pointer logs its start, its end and its actor (issue
/// #2374). A refused activation and an activation of the active version
/// log nothing.
pub async fn each_move_is_logged_with_its_actor(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("log");
    assert_eq!(store.last_activation(&kind).await.expect("last"), None);
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    let v2 = store.put(&kind, &narrow(8, "b"), ops()).await.expect("v2");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    let first = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("v1");
    assert_eq!(first.seq(), 1);
    assert_eq!(first.from(), None);
    assert_eq!(first.to(), v1);
    assert_eq!(first.actor(), &ops());
    assert_eq!(first.kind(), kind);

    let run = ExecutionId::new();
    store
        .activate(&kind, v2, Some(v1), Provenance::refine(run))
        .await
        .expect("v2");
    let second = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("v2");
    assert_eq!(second.seq(), 2);
    assert_eq!((second.from(), second.to()), (Some(v1), v2));
    assert_eq!(second.actor(), &Provenance::refine(run));
    assert!(second.activated_at() >= first.activated_at());

    store
        .activate(&kind, v2, Some(v2), ops())
        .await
        .expect("the active version again");
    assert!(store.activate(&kind, v1, None, ops()).await.is_err());
    let last = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("v2");
    assert_eq!(last, second, "a stay in place and a refusal log nothing");
}

/// `rollback` after v1 then v2 makes v1 active, and logs the move back. A
/// second rollback restores v2, as `kubectl rollout undo` does.
pub async fn a_rollback_activates_the_version_before(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("undo");
    let v1 = store
        .put(&kind, &narrow(9, "One."), ops())
        .await
        .expect("v1");
    let v2 = store
        .put(&kind, &narrow(6, "Two."), ops())
        .await
        .expect("v2");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    store
        .activate(&kind, v2, Some(v1), ops())
        .await
        .expect("v2");

    let back = store
        .rollback(&kind, Provenance::operator("oncall"))
        .await
        .expect("rollback");
    assert_eq!(back, Rollback { from: v2, to: v1 });
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
    assert_eq!(
        active.bytes(),
        serde_json::to_vec(&narrow(9, "One.")).expect("encode")
    );
    let last = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("move");
    assert_eq!((last.from(), last.to()), (Some(v2), v1));
    assert_eq!(last.actor(), &Provenance::operator("oncall"));

    let again = store.rollback(&kind, ops()).await.expect("rollback again");
    assert_eq!(again, Rollback { from: v1, to: v2 });
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v2);
}

/// `rollback` with no earlier activation returns `NoEarlierActivation` and
/// changes nothing: with no activation, and after one first activation.
pub async fn a_rollback_with_no_earlier_activation_changes_nothing(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("no-undo");
    assert_eq!(
        store.rollback(&kind, ops()).await,
        Err(OverlayStoreError::NoEarlierActivation)
    );
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    assert_eq!(
        store.rollback(&kind, ops()).await,
        Err(OverlayStoreError::NoEarlierActivation)
    );
    assert_eq!(store.active(&kind).await.expect("active"), None);
    assert_eq!(store.last_activation(&kind).await.expect("last"), None);

    store.activate(&kind, v1, None, ops()).await.expect("v1");
    let before = store.last_activation(&kind).await.expect("last");
    assert_eq!(
        store.rollback(&kind, ops()).await,
        Err(OverlayStoreError::NoEarlierActivation)
    );
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
    assert_eq!(store.last_activation(&kind).await.expect("last"), before);
}

/// An activation refuses a bad actor, and the pointer does not move.
pub async fn an_activation_checks_its_actor(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("actor");
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    for bad in ["", " ops", "o\u{200B}ps"] {
        assert!(matches!(
            store
                .activate(&kind, v1, None, Provenance::operator(bad))
                .await,
            Err(OverlayStoreError::InvalidProvenance { .. })
        ));
    }
    assert_eq!(store.active(&kind).await.expect("active"), None);
    assert_eq!(store.last_activation(&kind).await.expect("last"), None);
}

/// A fixed time in one UTC day, so a cap test never spans midnight.
fn noon() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-03T12:00:00Z")
        .expect("time")
        .with_timezone(&Utc)
}

fn policy(json: serde_json::Value) -> RefinePolicy {
    serde_json::from_value(json).expect("a valid policy")
}

/// Issue #2394: with no policy stored, every setting reads its default.
pub async fn a_kind_with_no_policy_reads_every_default(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("no-policy");
    let state = store.policy(&kind).await.expect("policy");
    assert_eq!(state.policy, RefinePolicy::default());
    assert_eq!(state.revision, None);
    assert!(!state.custom);
    assert_eq!(store.policy_log(&kind).await.expect("log"), []);
}

/// A set policy reads back. The log keeps the old and the new value.
pub async fn a_set_policy_is_read_back_and_logged(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("set-policy");
    let wide = policy(serde_json::json!({"auto_adopt": {"parts": ["system"]}}));
    let first = store
        .set_policy(&kind, &wide, None, ops())
        .await
        .expect("set");
    assert_eq!(first.revision(), 1);
    assert_eq!(first.before(), None);
    assert_eq!(first.after(), Some(&wide));
    assert_eq!(first.actor(), &ops());
    let state = store.policy(&kind).await.expect("policy");
    assert_eq!(state.policy, wide);
    assert_eq!(state.revision, Some(1));
    assert!(state.custom);

    let wider = policy(serde_json::json!({
        "auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"}
    }));
    let second = store
        .set_policy(&kind, &wider, Some(1), Provenance::operator("lead"))
        .await
        .expect("set again");
    assert_eq!(second.revision(), 2);
    assert_eq!(second.before(), Some(&wide));
    assert_eq!(second.after(), Some(&wider));
    let log = store.policy_log(&kind).await.expect("log");
    assert_eq!(log, [first, second]);
    let other = store.policy(&self::kind("other")).await.expect("policy");
    assert_eq!(other.policy, RefinePolicy::default(), "kinds do not share");
}

/// A set over a stale revision is a `PolicyConflict`. Nothing changes.
pub async fn a_stale_policy_revision_changes_nothing(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("stale-policy");
    let one = policy(serde_json::json!({"lessons": {"max_entries": 10}}));
    let two = policy(serde_json::json!({"lessons": {"max_entries": 20}}));
    store
        .set_policy(&kind, &one, None, ops())
        .await
        .expect("set");
    for stale in [None, Some(7)] {
        let err = store.set_policy(&kind, &two, stale, ops()).await;
        assert_eq!(
            err,
            Err(OverlayStoreError::PolicyConflict { actual: Some(1) }),
            "{stale:?}"
        );
    }
    let empty = self::kind("stale-empty");
    let err = store.set_policy(&empty, &two, Some(1), ops()).await;
    assert_eq!(err, Err(OverlayStoreError::PolicyConflict { actual: None }));
    assert_eq!(store.policy(&kind).await.expect("policy").policy, one);
    assert_eq!(store.policy_log(&kind).await.expect("log").len(), 1);
}

/// `reset` restores every default, and the log records it.
pub async fn reset_restores_every_default_and_is_logged(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("reset-policy");
    let wide = policy(serde_json::json!({"probe_effects": ["read_only", "internal"]}));
    store
        .set_policy(&kind, &wide, None, ops())
        .await
        .expect("set");
    let reset = store
        .reset_policy(&kind, Provenance::operator("oncall"))
        .await
        .expect("reset");
    assert_eq!(reset.revision(), 2);
    assert_eq!(reset.before(), Some(&wide));
    assert_eq!(reset.after(), None);
    assert_eq!(reset.actor(), &Provenance::operator("oncall"));
    let state = store.policy(&kind).await.expect("policy");
    assert_eq!(state.policy, RefinePolicy::default());
    assert_eq!(state.revision, Some(2));
    assert!(!state.custom);
    let again = store.reset_policy(&kind, ops()).await.expect("reset again");
    assert_eq!((again.revision(), again.before()), (3, None));
}

/// Only an operator writes the policy. A refine or auto run cannot.
pub async fn only_an_operator_sets_the_policy(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("operator-only");
    let wide = policy(serde_json::json!({"auto_adopt": {"parts": ["system"]}}));
    let run = ExecutionId::new();
    for actor in [Provenance::refine(run), Provenance::auto(run)] {
        assert!(
            matches!(
                store.set_policy(&kind, &wide, None, actor.clone()).await,
                Err(OverlayStoreError::InvalidProvenance { .. })
            ),
            "{actor:?} set the policy"
        );
        assert!(
            matches!(
                store.reset_policy(&kind, actor.clone()).await,
                Err(OverlayStoreError::InvalidProvenance { .. })
            ),
            "{actor:?} reset the policy"
        );
    }
    assert!(matches!(
        store
            .set_policy(&kind, &wide, None, Provenance::operator(" "))
            .await,
        Err(OverlayStoreError::InvalidProvenance { .. })
    ));
    assert_eq!(store.policy_log(&kind).await.expect("log"), []);
}

/// `ceilings.max_steps = 40`, over a profile of 10: a put of 30 passes,
/// and a put of 50 fails. The default refuses 30.
pub async fn a_policy_ceiling_widens_the_validator(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("ceiling");
    let steps = |n| ProfileOverlay {
        max_steps: Some(n),
        ..ProfileOverlay::default()
    };
    assert!(matches!(
        store.put(&kind, &steps(30), ops()).await,
        Err(OverlayStoreError::Overlay(
            OverlayError::AboveCeiling { .. }
        ))
    ));
    let wide = policy(serde_json::json!({"ceilings": {"max_steps": 40}}));
    store
        .set_policy(&kind, &wide, None, ops())
        .await
        .expect("set");
    let v1 = store.put(&kind, &steps(30), ops()).await.expect("30 fits");
    assert!(matches!(
        store.put(&kind, &steps(50), ops()).await,
        Err(OverlayStoreError::Overlay(
            OverlayError::AboveCeiling { .. }
        ))
    ));
    store.reset_policy(&kind, ops()).await.expect("reset");
    assert!(
        matches!(
            store.activate(&kind, v1, None, ops()).await,
            Err(OverlayStoreError::Overlay(_))
        ),
        "a lowered ceiling refuses the activation"
    );
}

/// A policy that auto-adopts `system` and `bounds` under `cap`.
fn auto_policy(cap: &serde_json::Value) -> RefinePolicy {
    policy(serde_json::json!({
        "auto_adopt": {"parts": ["system", "bounds"], "max_per_day": cap}
    }))
}

/// Store `policy` for `kind`, as an operator does.
async fn allow(store: &dyn ProfileOverlayStore, kind: &str, policy: &RefinePolicy) {
    let revision = store.policy(kind).await.expect("policy").revision;
    store
        .set_policy(kind, policy, revision, ops())
        .await
        .expect("set");
}

/// An auto adoption puts the version as `auto:<run>` and moves the
/// pointer, with `auto:<run>` as the actor, at the time of the store clock.
pub async fn an_auto_adoption_records_auto_provenance(make: MakeClockedStore<'_>) {
    let clock = HandClock::at(noon());
    let store = make(Arc::new(Gate::default()), clock.clock());
    let kind = kind("auto");
    let unlimited = auto_policy(&serde_json::json!("unlimited"));
    allow(store.as_ref(), &kind, &unlimited).await;
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    let run = ExecutionId::new();
    let outcome = store
        .auto_adopt(&kind, &narrow(9, "b"), run, Some(v1), &unlimited)
        .await
        .expect("auto adopt");
    let AutoAdoptOutcome::Adopted(v2) = outcome else {
        panic!("want Adopted, got {outcome:?}");
    };
    assert_eq!(v2, version(2));
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v2);
    assert_eq!(active.provenance(), &Provenance::auto(run));
    assert!(active.provenance().to_string().starts_with("auto:"));
    let last = store
        .last_activation(&kind)
        .await
        .expect("last")
        .expect("a move");
    assert_eq!((last.from(), last.to()), (Some(v1), v2));
    assert_eq!(last.actor(), &Provenance::auto(run));
    assert_eq!(last.activated_at(), noon());
}

/// With a cap of 1, a second auto adoption in the same UTC day of the store
/// clock reaches the cap. Nothing is stored and the pointer stays. The
/// next day adopts. `unlimited` never reaches a cap.
pub async fn the_daily_cap_holds_in_the_store(make: MakeClockedStore<'_>) {
    let clock = HandClock::at(noon());
    let store = make(Arc::new(Gate::default()), clock.clock());
    let kind = kind("cap");
    let unlimited = auto_policy(&serde_json::json!("unlimited"));
    allow(store.as_ref(), &kind, &unlimited).await;
    let one = auto_policy(&serde_json::json!(1));
    let adopt = |text: &'static str, expected, run_policy: RefinePolicy| {
        let store = &store;
        let kind = &kind;
        async move {
            store
                .auto_adopt(
                    kind,
                    &narrow(9, text),
                    ExecutionId::new(),
                    expected,
                    &run_policy,
                )
                .await
                .expect("auto adopt")
        }
    };
    let AutoAdoptOutcome::Adopted(v1) = adopt("a", None, one.clone()).await else {
        panic!("the first adoption passes");
    };
    clock.set(noon() + TimeDelta::hours(11));
    assert_eq!(
        adopt("b", Some(v1), one.clone()).await,
        AutoAdoptOutcome::CapReached { cap: 1, used: 1 }
    );
    assert_eq!(store.list(&kind).await.expect("list").len(), 1);
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
    store
        .activate(&kind, v1, Some(v1), ops())
        .await
        .expect("an operator move is not capped");

    assert_eq!(
        adopt("c", Some(v1), auto_policy(&serde_json::json!(0))).await,
        AutoAdoptOutcome::CapReached { cap: 0, used: 1 }
    );
    let AutoAdoptOutcome::Adopted(v2) = adopt("d", Some(v1), unlimited.clone()).await else {
        panic!("unlimited never waits");
    };
    allow(store.as_ref(), &kind, &one).await;
    assert_eq!(
        adopt("e", Some(v2), unlimited).await,
        AutoAdoptOutcome::CapReached { cap: 1, used: 2 },
        "the stricter cap of the store counts"
    );
    clock.set(noon() + TimeDelta::days(1));
    let AutoAdoptOutcome::Adopted(v3) = adopt("f", Some(v2), one).await else {
        panic!("a new UTC day has a new cap");
    };
    assert!(v1 < v2 && v2 < v3);
}

/// An auto adoption is a compare-and-swap on the pointer, and it validates
/// the overlay first.
pub async fn an_auto_adoption_checks_the_pointer(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("auto-cas");
    let unlimited = auto_policy(&serde_json::json!("unlimited"));
    allow(store.as_ref(), &kind, &unlimited).await;
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    let run = ExecutionId::new();
    let stale = store
        .auto_adopt(&kind, &narrow(9, "b"), run, None, &unlimited)
        .await;
    assert_eq!(stale, Err(OverlayStoreError::Conflict { actual: Some(v1) }));
    let wide = store
        .auto_adopt(&kind, &narrow(50, "b"), run, Some(v1), &unlimited)
        .await;
    assert!(
        matches!(wide, Err(OverlayStoreError::Overlay(_))),
        "{wide:?}"
    );
    let missing = store
        .auto_adopt(&kind, &narrow(9, "b"), run, Some(version(9)), &unlimited)
        .await;
    assert_eq!(
        missing,
        Err(OverlayStoreError::UnknownVersion {
            version: version(9)
        })
    );
    assert_eq!(store.list(&kind).await.expect("list").len(), 1);
}

/// `rollback` after an auto adoption restores the version before it.
pub async fn a_rollback_undoes_an_auto_adoption(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("auto-rollback");
    let unlimited = auto_policy(&serde_json::json!("unlimited"));
    allow(store.as_ref(), &kind, &unlimited).await;
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    let outcome = store
        .auto_adopt(
            &kind,
            &narrow(9, "b"),
            ExecutionId::new(),
            Some(v1),
            &unlimited,
        )
        .await
        .expect("auto adopt");
    let AutoAdoptOutcome::Adopted(v2) = outcome else {
        panic!("want Adopted, got {outcome:?}");
    };
    let back = store.rollback(&kind, ops()).await.expect("rollback");
    assert_eq!(back, Rollback { from: v2, to: v1 });
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
}

/// The store enforces the gate. A part goes live only when the run policy
/// and the stored policy both allow it. An unchanged overlay stores
/// nothing. With no stored policy, nothing goes live.
pub async fn the_store_enforces_both_policies(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("both");
    let wide = auto_policy(&serde_json::json!("unlimited"));
    let run = ExecutionId::new();
    let refused = store
        .auto_adopt(&kind, &narrow(9, "a"), run, None, &wide)
        .await
        .expect("gate");
    assert_eq!(
        refused,
        AutoAdoptOutcome::NeedsApproval {
            parts: BTreeSet::from([OverlayPart::System, OverlayPart::Bounds])
        },
        "the stored default allows nothing"
    );
    let system_only = policy(serde_json::json!({"auto_adopt": {"parts": ["system"]}}));
    allow(store.as_ref(), &kind, &system_only).await;
    let refused = store
        .auto_adopt(&kind, &narrow(9, "a"), run, None, &wide)
        .await
        .expect("gate");
    assert_eq!(
        refused,
        AutoAdoptOutcome::NeedsApproval {
            parts: BTreeSet::from([OverlayPart::Bounds])
        }
    );
    let prompt_only = ProfileOverlay {
        system: Some("Only the prompt.".to_owned()),
        ..ProfileOverlay::default()
    };
    let AutoAdoptOutcome::Adopted(v1) = store
        .auto_adopt(&kind, &prompt_only, run, None, &wide)
        .await
        .expect("gate")
    else {
        panic!("both policies allow system");
    };
    assert_eq!(
        store
            .auto_adopt(&kind, &prompt_only, run, Some(v1), &wide)
            .await
            .expect("gate"),
        AutoAdoptOutcome::Unchanged
    );
    let narrow_run = RefinePolicy::default();
    let other = ProfileOverlay {
        system: Some("Another prompt.".to_owned()),
        ..ProfileOverlay::default()
    };
    assert_eq!(
        store
            .auto_adopt(&kind, &other, run, Some(v1), &narrow_run)
            .await
            .expect("gate"),
        AutoAdoptOutcome::NeedsApproval {
            parts: BTreeSet::from([OverlayPart::System])
        },
        "the run policy also binds"
    );
    assert_eq!(store.list(&kind).await.expect("list").len(), 1);
}

/// Only `auto_adopt` writes an `auto` actor. A put, an activation and a
/// rollback with one are refused, so each auto move passes the cap.
pub async fn only_auto_adopt_writes_an_auto_actor(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    let kind = kind("auto-actor");
    let auto = Provenance::auto(ExecutionId::new());
    assert!(matches!(
        store.put(&kind, &narrow(9, "a"), auto.clone()).await,
        Err(OverlayStoreError::InvalidProvenance { .. })
    ));
    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    assert!(matches!(
        store.activate(&kind, v1, None, auto.clone()).await,
        Err(OverlayStoreError::InvalidProvenance { .. })
    ));
    let v2 = store.put(&kind, &narrow(8, "b"), ops()).await.expect("v2");
    store.activate(&kind, v1, None, ops()).await.expect("v1");
    store
        .activate(&kind, v2, Some(v1), ops())
        .await
        .expect("v2");
    assert!(matches!(
        store.rollback(&kind, auto).await,
        Err(OverlayStoreError::InvalidProvenance { .. })
    ));
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v2);
}
