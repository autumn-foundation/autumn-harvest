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

    /// The op read of `kind`: the profile with the active overlay applied,
    /// and its version (issue #2372).
    ///
    /// With no store or no active version, it returns the bare profile and
    /// no version. It applies the version under
    /// [`Ceilings::from_profile`], with this harness as the registry.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable activity error that names the version when
    /// the overlay does not apply, and for a kind with no profile or a
    /// corrupt row. Returns a retryable error when the store fails.
    pub async fn overlaid_profile(&self, kind: &str) -> Result<ProfileRead, String> {
        let kind_name = truncate(kind, MAX_NAME_LEN);
        let base = self.agent_profile(kind).ok_or_else(|| {
            refused(
                "UnknownAgentKind",
                format!("no agent profile for the kind {kind_name:?}"),
            )
        })?;
        let Some(store) = self.overlays() else {
            return Ok(ProfileRead::bare(base));
        };
        let active = match store.active(kind).await {
            Ok(active) => active,
            Err(OverlayStoreError::Backend { message }) => {
                return Err(format!("the overlay store failed: {message}"));
            }
            Err(err) => return Err(refused("OverlayReadFailed", err.to_string())),
        };
        let ceilings = Ceilings::from_profile(&base);
        let active = active.map(|stored| {
            let applied = apply(&base, stored.overlay(), &ceilings, self);
            (stored.version(), applied)
        });
        match op_profile(base, active) {
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
