//! The recorded profile read of a durable agent (issue #2372).
//!
//! A run reads its profile once, through the activity `agent_profile`. With
//! an overlay store, each op also reads the active overlay, through the
//! activity `agent_profile_overlay`. History records both results, so a
//! replay never reads the store. The design is in
//! `docs/design/DESIGN-2372.md`.

use serde::{Deserialize, Serialize};

use super::AgentProfile;
use super::overlay::{Ceilings, ProfileOverlay, apply};
use super::overlay_store::{OverlayStoreError, OverlayVersion};
use crate::harness::{AgentHarness, truncate};
use crate::mesh::MAX_NAME_LEN;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

/// One recorded profile read.
///
/// The profile fields sit at the top level, so a run result that an older
/// worker recorded still decodes. Such a result has no version and no
/// store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileRead {
    /// The profile. An op read holds the profile with the overlay applied.
    #[serde(flatten)]
    pub profile: AgentProfile,
    /// The overlay version that the profile holds. `None` is the bare
    /// profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_version: Option<OverlayVersion>,
    /// Whether the harness has an overlay store. A run read sets it. Each
    /// op of the run then reads its overlay.
    #[serde(default, skip_serializing_if = "crate::types::is_false")]
    pub overlay_store: bool,
}

impl ProfileRead {
    /// A read of the bare `profile`.
    #[must_use]
    pub const fn bare(profile: AgentProfile) -> Self {
        Self {
            profile,
            overlay_version: None,
            overlay_store: false,
        }
    }
}

/// The input of the op read: the kind, and the profile of the run.
///
/// The read applies the overlay to the run profile. So a field that the
/// overlay does not set keeps its run value, also after a deploy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverlayRead {
    /// The agent kind.
    pub kind: String,
    /// The profile that the run read recorded.
    pub profile: AgentProfile,
}

/// The tighter of two bounds. A `None` bound sets no limit.
const fn tighter(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
        (Some(bound), None) | (None, Some(bound)) => Some(bound),
        (None, None) => None,
    }
}

/// The ceilings that both `a` and `b` allow.
fn meet(a: &Ceilings, b: &Ceilings) -> Ceilings {
    Ceilings {
        max_steps: tighter(a.max_steps, b.max_steps),
        max_total_tokens: tighter(a.max_total_tokens, b.max_total_tokens),
        max_output_tokens: tighter(a.max_output_tokens, b.max_output_tokens),
        models: a.models.intersection(&b.models).cloned().collect(),
        allow_clear_read_only: a.allow_clear_read_only && b.allow_clear_read_only,
    }
}

/// The profile of one op: `base` with no version, or the applied profile
/// of the active version.
///
/// An active version that does not apply is an error that names the
/// version. So an op never runs on `base` while a version is active.
/// `formal/verus/src/agent_profile_read.rs` proves it.
pub(crate) fn op_profile<P, V, E>(
    base: P,
    active: Option<(V, Result<P, E>)>,
) -> Result<(P, Option<V>), (V, E)> {
    match active {
        None => Ok((base, None)),
        Some((version, Ok(applied))) => Ok((applied, Some(version))),
        Some((version, Err(err))) => Err((version, err)),
    }
}

/// `run` with the fields that an overlay can set taken from `read`.
///
/// Every other field keeps its run value. So an op read never changes the
/// mandate, the peers or the heartbeat of a run.
pub(crate) fn overlaid(run: &AgentProfile, read: &AgentProfile) -> AgentProfile {
    // A new overlay field fails to compile here until this function reads it.
    let ProfileOverlay {
        system: _,
        guardrails: _,
        model: _,
        max_steps: _,
        max_total_tokens: _,
        max_output_tokens: _,
        read_only: _,
        tool_guidance: _,
        skills: _,
    } = ProfileOverlay::default();
    AgentProfile {
        system: read.system.clone(),
        model: read.model.clone(),
        max_steps: read.max_steps,
        max_total_tokens: read.max_total_tokens,
        max_output_tokens: read.max_output_tokens,
        read_only: read.read_only,
        tool_guidance: read.tool_guidance.clone(),
        skills: read.skills.clone(),
        ..run.clone()
    }
}

/// A non-retryable failure of an op read. A retry cannot fix it.
fn refused(class: &str, message: String) -> String {
    ActivityFailure::non_retryable(class, message).into_error_payload()
}

impl AgentHarness {
    /// The run read of `kind`: the bare profile, and whether an overlay
    /// store is installed.
    #[must_use]
    pub fn profile_read(&self, kind: &str) -> Option<ProfileRead> {
        self.agent_profile(kind).map(|profile| ProfileRead {
            overlay_store: self.overlays().is_some(),
            ..ProfileRead::bare(profile)
        })
    }

    /// The op read of `request`: the run profile with the active overlay
    /// applied, and its version (issue #2372).
    ///
    /// With no active version, it returns the run profile and no version.
    /// The overlay applies under the ceilings of the run profile and of the
    /// harness profile of the kind, both from [`Ceilings::from_profile`].
    /// The harness is the registry. So a deploy that narrows the profile or
    /// removes a model refuses an old version.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable activity error when the overlay does not
    /// apply. That error names the version. A kind with no harness profile
    /// and a corrupt row also give a non-retryable error. A store failure
    /// gives a retryable error. So does a worker with no store, because the
    /// run read found one. Another worker can then take the retry.
    pub async fn overlaid_profile(&self, request: &OverlayRead) -> Result<ProfileRead, String> {
        let kind = request.kind.as_str();
        let kind_name = truncate(kind, MAX_NAME_LEN);
        let current = self.agent_profile(kind).ok_or_else(|| {
            refused(
                "UnknownAgentKind",
                format!("no agent profile for the kind {kind_name:?}"),
            )
        })?;
        let Some(store) = self.overlays() else {
            return Err(format!(
                "the run of the kind {kind_name:?} reads overlays, and this worker has no overlay store"
            ));
        };
        let active = match store.active(kind).await {
            Ok(active) => active,
            Err(OverlayStoreError::Backend { message }) => {
                return Err(format!("the overlay store failed: {message}"));
            }
            Err(err) => return Err(refused("OverlayReadFailed", err.to_string())),
        };
        let run = &request.profile;
        let ceilings = meet(
            &Ceilings::from_profile(run),
            &Ceilings::from_profile(&current),
        );
        let active = active.map(|stored| {
            let applied = apply(run, stored.overlay(), &ceilings, self);
            (stored.version(), applied)
        });
        match op_profile(run.clone(), active) {
            Ok((profile, overlay_version)) => Ok(ProfileRead {
                profile,
                overlay_version,
                overlay_store: true,
            }),
            Err((version, err)) => Err(refused(
                "OverlayRefused",
                format!("the overlay {version} of the kind {kind_name:?} does not apply: {err}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::collections::BTreeSet;

    use crate::mesh::Mandate;
    use crate::skill::SkillText;
    use proptest::prelude::*;
    use serde_json::json;

    #[test]
    fn an_old_run_result_decodes_as_a_bare_read() {
        let profile = AgentProfile::new("support").system("Help.");
        let old = serde_json::to_value(&profile).unwrap();
        let read: ProfileRead = serde_json::from_value(old).unwrap();
        assert_eq!(read, ProfileRead::bare(profile));
    }

    #[test]
    fn a_bare_read_encodes_as_the_old_profile_bytes() {
        let profile = AgentProfile::new("support").max_steps(3);
        assert_eq!(
            serde_json::to_vec(&ProfileRead::bare(profile.clone())).unwrap(),
            serde_json::to_vec(&profile).unwrap(),
        );
    }

    #[test]
    fn a_read_with_a_version_round_trips() {
        let read = ProfileRead {
            profile: AgentProfile::new("support"),
            overlay_version: OverlayVersion::new(2),
            overlay_store: true,
        };
        let value = serde_json::to_value(&read).unwrap();
        assert_eq!(value["overlay_version"], json!(2));
        assert_eq!(value["overlay_store"], json!(true));
        assert_eq!(serde_json::from_value::<ProfileRead>(value).unwrap(), read);
    }

    #[test]
    fn an_old_worker_reads_a_new_result_as_its_profile() {
        let read = ProfileRead {
            profile: AgentProfile::new("support").system("Help."),
            overlay_version: OverlayVersion::new(1),
            overlay_store: true,
        };
        let value = serde_json::to_value(&read).unwrap();
        let old: AgentProfile = serde_json::from_value(value).unwrap();
        assert_eq!(old, read.profile);
    }

    #[test]
    fn the_projection_takes_only_the_overlay_fields() {
        let run = AgentProfile::new("support")
            .system("run")
            .mandate(Mandate::new("support").peers(["desk"]))
            .tenant("acme")
            .max_ops_per_run(5)
            .window(9);
        let mut read = AgentProfile::new("support")
            .system("read")
            .model("m2")
            .max_steps(3)
            .max_total_tokens(70)
            .read_only()
            .tool_guidance("lookup", "Look it up.")
            .skill("refund", SkillText::new("Pay.", "Check first."))
            .tenant("other")
            .window(1);
        read.max_output_tokens = Some(40);
        let out = overlaid(&run, &read);
        assert_eq!(out.system, read.system);
        assert_eq!(out.model, read.model);
        assert_eq!(out.max_steps, 3);
        assert_eq!(out.max_total_tokens, Some(70));
        assert_eq!(out.max_output_tokens, Some(40));
        assert!(out.read_only);
        assert_eq!(out.tool_guidance, read.tool_guidance);
        assert_eq!(out.skills, read.skills);
        assert_eq!(out.mandate, run.mandate);
        assert_eq!(out.tenant, run.tenant);
        assert_eq!(out.max_ops_per_run, run.max_ops_per_run);
        assert_eq!(out.window, run.window);
    }

    #[test]
    fn the_meet_takes_the_tighter_ceiling() {
        let wide = Ceilings::from_profile(
            &AgentProfile::new("support")
                .max_steps(50)
                .max_total_tokens(900)
                .model("m1"),
        );
        let mut narrow = Ceilings::from_profile(&AgentProfile::new("support").max_steps(20));
        narrow.models.insert("m1".to_owned());
        let out = meet(&wide, &narrow);
        assert_eq!(out.max_steps, Some(20));
        assert_eq!(out.max_total_tokens, Some(900));
        assert_eq!(out.max_output_tokens, None);
        assert_eq!(out.models, BTreeSet::from(["m1".to_owned()]));
        assert!(!out.allow_clear_read_only);
        assert_eq!(meet(&narrow, &wide), out, "the meet is symmetric");
    }

    /// A model that no read calls.
    #[derive(Debug)]
    struct NoModel;

    impl crate::model::AgentModel for NoModel {
        fn chat<'a>(
            &'a self,
            _request: &'a crate::model::ChatRequest,
        ) -> crate::model::BoxFuture<'a, Result<crate::model::ChatResponse, crate::AgentError>>
        {
            Box::pin(async { Err(crate::AgentError::new(crate::ErrorKind::Provider, "unused")) })
        }
    }

    /// A validator that takes every overlay, so a test can store any one.
    struct TakeAll;

    impl crate::OverlayValidator for TakeAll {
        fn validate(
            &self,
            _kind: &str,
            _overlay: &ProfileOverlay,
        ) -> Result<(), OverlayStoreError> {
            Ok(())
        }
    }

    /// A harness with the profile `current`, and a store whose active
    /// version is `overlay`.
    async fn deployed(current: AgentProfile, overlay: &ProfileOverlay) -> AgentHarness {
        let store = crate::InMemoryOverlayStore::new(std::sync::Arc::new(TakeAll));
        let version = crate::ProfileOverlayStore::put(
            &store,
            "support",
            overlay,
            crate::Provenance::operator("ops"),
        )
        .await
        .unwrap();
        crate::ProfileOverlayStore::activate(&store, "support", version, None)
            .await
            .unwrap();
        AgentHarness::new(std::sync::Arc::new(NoModel))
            .agent(current)
            .overlay_store(std::sync::Arc::new(store))
    }

    fn request(profile: AgentProfile) -> OverlayRead {
        OverlayRead {
            kind: "support".to_owned(),
            profile,
        }
    }

    #[tokio::test]
    async fn a_worker_with_no_store_retries_and_never_runs_bare() {
        let profile = AgentProfile::new("support");
        let harness = AgentHarness::new(std::sync::Arc::new(NoModel)).agent(profile.clone());
        let err = harness
            .overlaid_profile(&request(profile))
            .await
            .unwrap_err();
        assert!(err.contains("no overlay store"), "{err}");
        assert!(
            !err.contains("non_retryable"),
            "a retry can reach another worker: {err}"
        );
    }

    #[tokio::test]
    async fn a_wider_deploy_never_widens_the_run_profile() {
        let run = AgentProfile::new("support").max_steps(20).read_only();
        let current = AgentProfile::new("support").max_steps(50);
        let guardrails = ProfileOverlay {
            guardrails: Some("Be brief.".to_owned()),
            ..ProfileOverlay::default()
        };
        let harness = deployed(current, &guardrails).await;
        let read = harness.overlaid_profile(&request(run)).await.unwrap();
        assert_eq!(read.overlay_version, OverlayVersion::new(1));
        assert_eq!(read.profile.max_steps, 20);
        assert!(read.profile.read_only);
        assert!(read.profile.system.unwrap().contains("Be brief."));
    }

    #[tokio::test]
    async fn a_narrower_deploy_refuses_an_old_version_by_name() {
        let run = AgentProfile::new("support").max_steps(50);
        let current = AgentProfile::new("support").max_steps(20);
        let steps = ProfileOverlay {
            max_steps: Some(40),
            ..ProfileOverlay::default()
        };
        let harness = deployed(current, &steps).await;
        let err = harness.overlaid_profile(&request(run)).await.unwrap_err();
        assert!(err.contains("v1") && err.contains("max_steps"), "{err}");
    }

    #[tokio::test]
    async fn no_active_version_reads_the_run_profile() {
        let run = AgentProfile::new("support").system("run");
        let store = crate::InMemoryOverlayStore::new(std::sync::Arc::new(TakeAll));
        let harness = AgentHarness::new(std::sync::Arc::new(NoModel))
            .agent(AgentProfile::new("support").system("current"))
            .overlay_store(std::sync::Arc::new(store));
        let read = harness
            .overlaid_profile(&request(run.clone()))
            .await
            .unwrap();
        assert_eq!(read.overlay_version, None);
        assert_eq!(read.profile, run);
    }

    proptest! {
        /// The engine kernel agrees with the Verus spec on every input.
        #[test]
        fn the_kernel_never_runs_bare_under_a_version(
            base in any::<u8>(),
            active in proptest::option::of((any::<u64>(), any::<Result<u8, u8>>())),
        ) {
            let out = op_profile(base, active);
            match active {
                None => prop_assert_eq!(out, Ok((base, None))),
                Some((version, Ok(applied))) => {
                    prop_assert_eq!(out, Ok((applied, Some(version))));
                }
                Some((version, Err(err))) => prop_assert_eq!(out, Err((version, err))),
            }
        }
    }
}
