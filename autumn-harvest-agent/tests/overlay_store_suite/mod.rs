//! One suite for every `ProfileOverlayStore` (issue #2371).
//!
//! `overlay_store.rs` runs it on the in-memory and `SQLite` stores.
//! `overlay_store_postgres.rs` runs it on Postgres. Each case uses a new
//! kind, so the cases can share one database.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use autumn_harvest::ExecutionId;
use autumn_harvest_agent::agent::overlay::apply;
use autumn_harvest_agent::{
    AgentProfile, Ceilings, OverlayError, OverlayStoreError, OverlayValidator, OverlayVersion,
    ProfileOverlay, ProfileOverlayStore, Provenance,
};

/// Makes a store that validates with the given validator.
pub type MakeStore<'a> =
    &'a (dyn Fn(Arc<dyn OverlayValidator>) -> Box<dyn ProfileOverlayStore> + Sync);

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
    fn validate(&self, kind: &str, overlay: &ProfileOverlay) -> Result<(), OverlayStoreError> {
        let profile = self.base(kind);
        let registry: BTreeSet<String> = ["m1", "m2"].map(str::to_owned).into();
        apply(
            &profile,
            overlay,
            &Ceilings::from_profile(&profile),
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

/// Run every case on stores from `make`.
pub async fn run_all(make: MakeStore<'_>) {
    get_returns_the_bytes_written_for_each_version(make).await;
    put_twice_returns_increasing_versions(make).await;
    activate_of_an_unknown_version_changes_nothing(make).await;
    two_activates_with_one_expected_pointer_have_one_winner(make).await;
    a_widening_put_is_refused_with_the_overlay_error(make).await;
    activate_validates_the_version_again(make).await;
    a_rollback_restores_an_exact_earlier_version(make).await;
    provenance_is_checked_and_kept(make).await;
    an_unknown_kind_reads_empty(make).await;
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
    assert_eq!(got.kind(), kind);
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
    let err = store.activate(&kind, missing, None).await;
    assert_eq!(
        err,
        Err(OverlayStoreError::UnknownVersion { version: missing })
    );
    assert_eq!(store.active(&kind).await.expect("active"), None);

    let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
    store.activate(&kind, v1, None).await.expect("activate v1");
    let err = store.activate(&kind, missing, Some(v1)).await;
    assert_eq!(
        err,
        Err(OverlayStoreError::UnknownVersion { version: missing })
    );
    let active = store.active(&kind).await.expect("active").expect("v1");
    assert_eq!(active.version(), v1);
}

/// Two activates with the same `expected_active`: exactly one succeeds.
/// The other returns `Conflict { actual }`.
pub async fn two_activates_with_one_expected_pointer_have_one_winner(make: MakeStore<'_>) {
    let store = make(Arc::new(Gate::default()));
    for round in 0..8 {
        let kind = kind(&format!("race{round}"));
        let v1 = store.put(&kind, &narrow(9, "a"), ops()).await.expect("v1");
        let v2 = store.put(&kind, &narrow(8, "b"), ops()).await.expect("v2");
        let v3 = store.put(&kind, &narrow(7, "c"), ops()).await.expect("v3");
        store.activate(&kind, v1, None).await.expect("activate v1");
        let (left, right) = tokio::join!(
            store.activate(&kind, v2, Some(v1)),
            store.activate(&kind, v3, Some(v1)),
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
        store.activate(&kind, v1, None).await,
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
    store.activate(&kind, v1, None).await.expect("v1");
    store.activate(&kind, v2, Some(v1)).await.expect("v2");
    store
        .activate(&kind, v1, Some(v2))
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
