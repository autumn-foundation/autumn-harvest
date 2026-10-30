//! The profile overlay and its ceiling kernel (issue #2370).
//!
//! A model writes a [`ProfileOverlay`]. [`apply`] merges it into an
//! [`AgentProfile`]. [`Ceilings`] bound the result, so an overlay never goes
//! past what the operator allows. The design is in
//! `docs/design/DESIGN-2370.md`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::AgentProfile;
use crate::harness::{AgentHarness, DEFAULT_MODEL, truncate};
use crate::mesh::MAX_NAME_LEN;

/// The heading of the guardrails in the system prompt.
pub const GUARDRAILS_HEADING: &str = "## Guardrails";

/// A change to an [`AgentProfile`] that a model may write.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileOverlay {
    /// The system prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Text after the system prompt, under [`GUARDRAILS_HEADING`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guardrails: Option<String>,
    /// The harness model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The bound on tool rounds in one op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<u32>,
    /// The bound on provider-reported tokens in one op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
    /// The output cap of one model call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Refuse every tool call that writes or acts outside the app.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// The limits of the profile that [`apply`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ceilings {
    /// The highest `max_steps`. `None` is no limit.
    pub max_steps: Option<u32>,
    /// The highest `max_total_tokens`. `None` is no limit.
    pub max_total_tokens: Option<u32>,
    /// The highest `max_output_tokens`. `None` is no limit.
    pub max_output_tokens: Option<u32>,
    /// The models that the profile may name.
    pub models: BTreeSet<String>,
    /// Let an overlay clear `read_only`.
    pub allow_clear_read_only: bool,
}

impl Ceilings {
    /// The default ceilings of `profile`. An overlay may only narrow it.
    #[must_use]
    pub fn from_profile(profile: &AgentProfile) -> Self {
        Self {
            max_steps: Some(profile.max_steps),
            max_total_tokens: profile.max_total_tokens,
            max_output_tokens: profile.max_output_tokens,
            models: BTreeSet::from([profile
                .model
                .clone()
                .unwrap_or_else(|| DEFAULT_MODEL.to_owned())]),
            allow_clear_read_only: false,
        }
    }
}

/// The models that a harness holds.
pub trait ModelRegistry {
    /// Whether the registry holds the model `name`.
    fn has_model(&self, name: &str) -> bool;
}

impl ModelRegistry for AgentHarness {
    fn has_model(&self, name: &str) -> bool {
        Self::has_model(self, name)
    }
}

impl ModelRegistry for BTreeSet<String> {
    fn has_model(&self, name: &str) -> bool {
        self.contains(name)
    }
}

/// Why [`apply`] refuses an overlay.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum OverlayError {
    /// The result bound `field` is above its ceiling.
    #[error("the {field} of the result is above its ceiling")]
    AboveCeiling {
        /// The name of the bound.
        field: &'static str,
    },
    /// The overlay clears `read_only`, and the ceilings do not allow it.
    #[error("the overlay clears read_only, and the ceilings do not allow it")]
    ClearsReadOnly,
    /// The ceilings do not hold the result model.
    #[error("the ceilings do not allow the model {:?}", truncate(.model, MAX_NAME_LEN))]
    ModelNotAllowed {
        /// The model name.
        model: String,
    },
    /// The harness has no model of the name that the overlay gives.
    #[error("the harness has no model named {:?}", truncate(.model, MAX_NAME_LEN))]
    UnknownModel {
        /// The model name.
        model: String,
    },
}

/// The bound after an overlay sets `set` over `current`, or `None` when it
/// passes `ceiling`. A `None` bound is no bound.
///
/// `formal/verus/src/agent_overlay.rs` proves it.
pub(crate) const fn merged_bound(
    current: Option<u32>,
    set: Option<u32>,
    ceiling: Option<u32>,
) -> Option<Option<u32>> {
    let next = match set {
        Some(value) => Some(value),
        None => current,
    };
    match ceiling {
        None => Some(next),
        Some(cap) => match next {
            Some(value) if value <= cap => Some(next),
            _ => None,
        },
    }
}

/// The `read_only` flag after an overlay sets `set` over `current`, or
/// `None` when it clears the flag and `allow_clear` is false.
///
/// `formal/verus/src/agent_overlay.rs` proves it.
pub(crate) const fn merged_read_only(
    current: bool,
    set: Option<bool>,
    allow_clear: bool,
) -> Option<bool> {
    let next = match set {
        Some(value) => value,
        None => current,
    };
    if current && !next && !allow_clear {
        None
    } else {
        Some(next)
    }
}

/// `system` with the guardrails section at its end.
///
/// A prompt that ends with the section stays as it is. So a second apply
/// adds nothing, and the text of the operator never changes.
fn with_guardrails(system: Option<&str>, guardrails: &str) -> String {
    let section = format!("{GUARDRAILS_HEADING}\n\n{guardrails}");
    match system {
        None | Some("") => section,
        Some(system) if system.ends_with(&section) => system.to_owned(),
        Some(system) => format!("{system}\n\n{section}"),
    }
}

/// Merges `overlay` into `profile` under `ceilings`.
///
/// Each bound is the overlay bound, or else the profile bound. The result
/// must be under each ceiling, so a profile bound above an operator ceiling
/// fails too. The model is the overlay model, or else the profile model.
/// The guardrails go after the system prompt. Each other field keeps its
/// profile value. The Verus proof `agent_overlay.rs` models this function.
///
/// # Errors
///
/// Returns [`OverlayError::AboveCeiling`] for a bound above its ceiling,
/// [`OverlayError::ClearsReadOnly`] for a cleared flag that the ceilings
/// do not allow, [`OverlayError::UnknownModel`] for an overlay model that
/// `registry` does not hold, and [`OverlayError::ModelNotAllowed`] for a
/// model outside the ceilings.
pub fn apply<R: ModelRegistry + ?Sized>(
    profile: &AgentProfile,
    overlay: &ProfileOverlay,
    ceilings: &Ceilings,
    registry: &R,
) -> Result<AgentProfile, OverlayError> {
    let above = |field| OverlayError::AboveCeiling { field };
    let Some(Some(max_steps)) = merged_bound(
        Some(profile.max_steps),
        overlay.max_steps,
        ceilings.max_steps,
    ) else {
        return Err(above("max_steps"));
    };
    let max_total_tokens = merged_bound(
        profile.max_total_tokens,
        overlay.max_total_tokens,
        ceilings.max_total_tokens,
    )
    .ok_or_else(|| above("max_total_tokens"))?;
    let max_output_tokens = merged_bound(
        profile.max_output_tokens,
        overlay.max_output_tokens,
        ceilings.max_output_tokens,
    )
    .ok_or_else(|| above("max_output_tokens"))?;
    let read_only = merged_read_only(
        profile.read_only,
        overlay.read_only,
        ceilings.allow_clear_read_only,
    )
    .ok_or(OverlayError::ClearsReadOnly)?;
    if let Some(model) = &overlay.model
        && !registry.has_model(model)
    {
        return Err(OverlayError::UnknownModel {
            model: model.clone(),
        });
    }
    let model = overlay.model.as_ref().or(profile.model.as_ref());
    let name = model.map_or(DEFAULT_MODEL, String::as_str);
    if !ceilings.models.contains(name) {
        return Err(OverlayError::ModelNotAllowed {
            model: name.to_owned(),
        });
    }
    let system = overlay.system.as_ref().or(profile.system.as_ref());
    let system = match &overlay.guardrails {
        Some(guardrails) => Some(with_guardrails(system.map(String::as_str), guardrails)),
        None => system.cloned(),
    };
    Ok(AgentProfile {
        system,
        max_steps,
        max_total_tokens,
        max_output_tokens,
        read_only,
        model: model.cloned(),
        ..profile.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::Mandate;
    use proptest::prelude::*;
    use serde_json::json;

    fn registry(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    /// A profile with every field that an overlay cannot name set.
    fn rich_profile() -> AgentProfile {
        let mut profile = AgentProfile::new("support")
            .system("Help the user.")
            .max_steps(20)
            .max_total_tokens(9_000)
            .model("m1")
            .read_only()
            .deliver()
            .parallel_children(3)
            .stream()
            .workspace();
        profile.max_output_tokens = Some(800);
        profile.mandate = Some(Mandate::new("support/a").read_only());
        profile.tenant = Some("acme".to_owned());
        profile.mesh = Some("ops".to_owned());
        profile.max_request_bytes = Some(4_096);
        profile.window = 50;
        profile.max_ops_per_run = Some(7);
        profile
    }

    #[test]
    fn an_empty_overlay_keeps_the_profile_bytes() {
        let profile = rich_profile();
        let ceilings = Ceilings::from_profile(&profile);
        let out = apply(
            &profile,
            &ProfileOverlay::default(),
            &ceilings,
            &registry(&[]),
        )
        .expect("an empty overlay applies");
        assert_eq!(
            serde_json::to_vec(&out).expect("encode"),
            serde_json::to_vec(&profile).expect("encode"),
        );
    }

    #[test]
    fn default_ceilings_refuse_a_cleared_read_only() {
        let profile = rich_profile();
        let overlay = ProfileOverlay {
            read_only: Some(false),
            ..ProfileOverlay::default()
        };
        let err = apply(
            &profile,
            &overlay,
            &Ceilings::from_profile(&profile),
            &registry(&[]),
        );
        assert_eq!(err, Err(OverlayError::ClearsReadOnly));
    }

    #[test]
    fn default_ceilings_refuse_more_steps() {
        let profile = rich_profile();
        let overlay = ProfileOverlay {
            max_steps: Some(21),
            ..ProfileOverlay::default()
        };
        let err = apply(
            &profile,
            &overlay,
            &Ceilings::from_profile(&profile),
            &registry(&[]),
        );
        assert_eq!(err, Err(OverlayError::AboveCeiling { field: "max_steps" }));
    }

    #[test]
    fn default_ceilings_refuse_more_tokens() {
        let profile = rich_profile();
        let ceilings = Ceilings::from_profile(&profile);
        for (overlay, field) in [
            (
                ProfileOverlay {
                    max_total_tokens: Some(9_001),
                    ..ProfileOverlay::default()
                },
                "max_total_tokens",
            ),
            (
                ProfileOverlay {
                    max_output_tokens: Some(801),
                    ..ProfileOverlay::default()
                },
                "max_output_tokens",
            ),
        ] {
            let err = apply(&profile, &overlay, &ceilings, &registry(&[]));
            assert_eq!(err, Err(OverlayError::AboveCeiling { field }));
        }
    }

    #[test]
    fn an_overlay_narrows_under_default_ceilings() {
        let profile = rich_profile();
        let overlay = ProfileOverlay {
            max_steps: Some(5),
            max_total_tokens: Some(1_000),
            max_output_tokens: Some(100),
            read_only: Some(true),
            system: Some("Be brief.".to_owned()),
            ..ProfileOverlay::default()
        };
        let out = apply(
            &profile,
            &overlay,
            &Ceilings::from_profile(&profile),
            &registry(&[]),
        )
        .expect("a narrower overlay applies");
        assert_eq!(out.max_steps, 5);
        assert_eq!(out.max_total_tokens, Some(1_000));
        assert_eq!(out.max_output_tokens, Some(100));
        assert!(out.read_only);
        assert_eq!(out.system.as_deref(), Some("Be brief."));
    }

    #[test]
    fn an_unbounded_profile_takes_any_token_bound() {
        let profile = AgentProfile::new("open");
        let overlay = ProfileOverlay {
            max_total_tokens: Some(u32::MAX),
            ..ProfileOverlay::default()
        };
        let out = apply(
            &profile,
            &overlay,
            &Ceilings::from_profile(&profile),
            &registry(&[]),
        )
        .expect("no ceiling on tokens");
        assert_eq!(out.max_total_tokens, Some(u32::MAX));
    }

    #[test]
    fn an_operator_ceiling_admits_up_to_its_bound() {
        let profile = AgentProfile::new("support").max_steps(20);
        let ceilings = Ceilings {
            max_steps: Some(40),
            ..Ceilings::from_profile(&profile)
        };
        let up = ProfileOverlay {
            max_steps: Some(30),
            ..ProfileOverlay::default()
        };
        let out = apply(&profile, &up, &ceilings, &registry(&[])).expect("30 is under 40");
        assert_eq!(out.max_steps, 30);
        let past = ProfileOverlay {
            max_steps: Some(50),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            apply(&profile, &past, &ceilings, &registry(&[])),
            Err(OverlayError::AboveCeiling { field: "max_steps" })
        );
    }

    #[test]
    fn a_profile_bound_above_an_operator_ceiling_fails() {
        let profile = AgentProfile::new("support").max_steps(20);
        let ceilings = Ceilings {
            max_steps: Some(10),
            max_total_tokens: Some(500),
            ..Ceilings::from_profile(&profile)
        };
        let silent = ProfileOverlay::default();
        assert_eq!(
            apply(&profile, &silent, &ceilings, &registry(&[])),
            Err(OverlayError::AboveCeiling { field: "max_steps" })
        );
        let steps = ProfileOverlay {
            max_steps: Some(10),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            apply(&profile, &steps, &ceilings, &registry(&[])),
            Err(OverlayError::AboveCeiling {
                field: "max_total_tokens"
            })
        );
        let both = ProfileOverlay {
            max_total_tokens: Some(500),
            ..steps
        };
        let out = apply(&profile, &both, &ceilings, &registry(&[])).expect("both under");
        assert_eq!((out.max_steps, out.max_total_tokens), (10, Some(500)));
    }

    #[test]
    fn an_operator_may_allow_a_cleared_read_only() {
        let profile = rich_profile();
        let ceilings = Ceilings {
            allow_clear_read_only: true,
            ..Ceilings::from_profile(&profile)
        };
        let overlay = ProfileOverlay {
            read_only: Some(false),
            ..ProfileOverlay::default()
        };
        let out = apply(&profile, &overlay, &ceilings, &registry(&[])).expect("allowed");
        assert!(!out.read_only);
        assert_eq!(out.mandate, profile.mandate);
    }

    #[test]
    fn a_model_outside_the_ceilings_is_not_allowed() {
        let profile = rich_profile();
        let overlay = ProfileOverlay {
            model: Some("m2".to_owned()),
            ..ProfileOverlay::default()
        };
        let err = apply(
            &profile,
            &overlay,
            &Ceilings::from_profile(&profile),
            &registry(&["m1", "m2"]),
        );
        assert_eq!(
            err,
            Err(OverlayError::ModelNotAllowed {
                model: "m2".to_owned()
            })
        );
    }

    #[test]
    fn an_unregistered_model_is_unknown() {
        let profile = rich_profile();
        let ceilings = Ceilings {
            models: registry(&["m1", "m9"]),
            ..Ceilings::from_profile(&profile)
        };
        let overlay = ProfileOverlay {
            model: Some("m9".to_owned()),
            ..ProfileOverlay::default()
        };
        let err = apply(&profile, &overlay, &ceilings, &registry(&["m1"]));
        assert_eq!(
            err,
            Err(OverlayError::UnknownModel {
                model: "m9".to_owned()
            })
        );
    }

    #[test]
    fn an_allowed_registered_model_applies() {
        let profile = rich_profile();
        let ceilings = Ceilings {
            models: registry(&["m1", "m2"]),
            ..Ceilings::from_profile(&profile)
        };
        let overlay = ProfileOverlay {
            model: Some("m2".to_owned()),
            ..ProfileOverlay::default()
        };
        let out = apply(&profile, &overlay, &ceilings, &registry(&["m2"])).expect("allowed");
        assert_eq!(out.model.as_deref(), Some("m2"));
    }

    #[test]
    fn no_model_is_the_default_model() {
        let profile = AgentProfile::new("support");
        assert_eq!(
            Ceilings::from_profile(&profile).models,
            registry(&[DEFAULT_MODEL])
        );
        let named = ProfileOverlay {
            model: Some(DEFAULT_MODEL.to_owned()),
            ..ProfileOverlay::default()
        };
        let ceilings = Ceilings::from_profile(&profile);
        let out = apply(&profile, &named, &ceilings, &registry(&[DEFAULT_MODEL]))
            .expect("the default model is the profile model");
        assert_eq!(out.model.as_deref(), Some(DEFAULT_MODEL));
        let other = Ceilings {
            models: registry(&["m1"]),
            ..ceilings
        };
        assert_eq!(
            apply(&profile, &ProfileOverlay::default(), &other, &registry(&[])),
            Err(OverlayError::ModelNotAllowed {
                model: DEFAULT_MODEL.to_owned()
            })
        );
    }

    #[test]
    fn the_guardrails_go_under_the_heading_once() {
        let profile = rich_profile();
        let ceilings = Ceilings::from_profile(&profile);
        let overlay = ProfileOverlay {
            guardrails: Some("Never share keys.".to_owned()),
            ..ProfileOverlay::default()
        };
        let once = apply(&profile, &overlay, &ceilings, &registry(&[])).expect("applies");
        assert_eq!(
            once.system.as_deref(),
            Some("Help the user.\n\n## Guardrails\n\nNever share keys.")
        );
        let twice = apply(&once, &overlay, &ceilings, &registry(&[])).expect("applies");
        assert_eq!(twice, once);
        let bare = AgentProfile::new("bare");
        let out = apply(
            &bare,
            &overlay,
            &Ceilings::from_profile(&bare),
            &registry(&[]),
        )
        .expect("applies");
        assert_eq!(
            out.system.as_deref(),
            Some("## Guardrails\n\nNever share keys.")
        );
        let replaced = ProfileOverlay {
            system: Some("New.".to_owned()),
            ..overlay
        };
        let out = apply(&once, &replaced, &ceilings, &registry(&[])).expect("applies");
        assert_eq!(
            out.system.as_deref(),
            Some("New.\n\n## Guardrails\n\nNever share keys.")
        );
    }

    #[test]
    fn an_overlay_that_names_a_reviewed_field_fails_to_decode() {
        for field in [
            "tools",
            "tool_rules",
            "flow_rules",
            "mandate",
            "tenant",
            "mesh",
            "memory",
            "parallel_children",
            "kind",
        ] {
            let value = json!({ field: [] });
            assert!(
                serde_json::from_value::<ProfileOverlay>(value).is_err(),
                "{field} must not decode"
            );
        }
        assert!(serde_json::from_value::<ProfileOverlay>(json!({"tools": []})).is_err());
    }

    #[test]
    fn an_overlay_round_trips_through_json() {
        assert_eq!(
            serde_json::to_value(ProfileOverlay::default()).expect("encode"),
            json!({})
        );
        let overlay: ProfileOverlay = serde_json::from_value(json!({
            "system": "s",
            "guardrails": "g",
            "model": "m1",
            "max_steps": 3,
            "max_total_tokens": null,
            "max_output_tokens": 9,
            "read_only": true,
        }))
        .expect("decode");
        assert_eq!(overlay.max_total_tokens, None);
        let back: ProfileOverlay =
            serde_json::from_value(serde_json::to_value(&overlay).expect("encode"))
                .expect("decode");
        assert_eq!(back, overlay);
    }

    #[test]
    fn an_error_names_a_long_model_in_short() {
        let model = "m".repeat(10_000);
        let text = OverlayError::UnknownModel { model }.to_string();
        assert!(text.len() < 200, "{}", text.len());
    }

    const MODELS: [&str; 4] = [DEFAULT_MODEL, "m1", "m2", "m3"];

    fn any_model() -> impl Strategy<Value = Option<String>> {
        proptest::option::of(proptest::sample::select(MODELS.to_vec()).prop_map(str::to_owned))
    }

    fn any_text() -> impl Strategy<Value = Option<String>> {
        proptest::option::of("[a-z#\n ]{0,12}")
    }

    fn any_bound() -> impl Strategy<Value = Option<u32>> {
        proptest::option::of(0_u32..64)
    }

    fn any_models() -> impl Strategy<Value = BTreeSet<String>> {
        proptest::sample::subsequence(MODELS.to_vec(), 0..=MODELS.len())
            .prop_map(|names| names.into_iter().map(str::to_owned).collect())
    }

    prop_compose! {
        fn any_profile()(
            system in any_text(),
            model in any_model(),
            max_steps in 0_u32..64,
            max_total_tokens in any_bound(),
            max_output_tokens in any_bound(),
            read_only in any::<bool>(),
            tenant in proptest::option::of("[a-z]{1,4}"),
            window in 0_u32..300,
        ) -> AgentProfile {
            let mut profile = AgentProfile::new("kind").max_steps(max_steps);
            profile.system = system;
            profile.model = model;
            profile.max_total_tokens = max_total_tokens;
            profile.max_output_tokens = max_output_tokens;
            profile.read_only = read_only;
            profile.tenant = tenant;
            profile.window = window;
            profile
        }
    }

    prop_compose! {
        fn any_overlay()(
            system in any_text(),
            guardrails in any_text(),
            model in any_model(),
            max_steps in any_bound(),
            max_total_tokens in any_bound(),
            max_output_tokens in any_bound(),
            read_only in proptest::option::of(any::<bool>()),
        ) -> ProfileOverlay {
            ProfileOverlay {
                system,
                guardrails,
                model,
                max_steps,
                max_total_tokens,
                max_output_tokens,
                read_only,
            }
        }
    }

    prop_compose! {
        fn any_ceilings()(
            max_steps in any_bound(),
            max_total_tokens in any_bound(),
            max_output_tokens in any_bound(),
            models in any_models(),
            allow_clear_read_only in any::<bool>(),
        ) -> Ceilings {
            Ceilings {
                max_steps,
                max_total_tokens,
                max_output_tokens,
                models,
                allow_clear_read_only,
            }
        }
    }

    /// Whether `value` is at most `ceiling`. `None` is no bound.
    fn under(value: Option<u32>, ceiling: Option<u32>) -> bool {
        match (value, ceiling) {
            (_, None) => true,
            (Some(value), Some(ceiling)) => value <= ceiling,
            (None, Some(_)) => false,
        }
    }

    /// `profile` with each field that an overlay can name taken from `base`.
    fn unnamed(profile: &AgentProfile, base: &AgentProfile) -> AgentProfile {
        AgentProfile {
            system: base.system.clone(),
            model: base.model.clone(),
            max_steps: base.max_steps,
            max_total_tokens: base.max_total_tokens,
            max_output_tokens: base.max_output_tokens,
            read_only: base.read_only,
            ..profile.clone()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn apply_is_idempotent(
            profile in any_profile(),
            overlay in any_overlay(),
            ceilings in any_ceilings(),
            names in any_models(),
        ) {
            if let Ok(once) = apply(&profile, &overlay, &ceilings, &names) {
                let twice = apply(&once, &overlay, &ceilings, &names);
                prop_assert_eq!(twice, Ok(once));
            }
        }

        #[test]
        fn a_result_stays_under_the_ceilings(
            profile in any_profile(),
            overlay in any_overlay(),
            ceilings in any_ceilings(),
            names in any_models(),
        ) {
            if let Ok(out) = apply(&profile, &overlay, &ceilings, &names) {
                prop_assert!(under(Some(out.max_steps), ceilings.max_steps));
                prop_assert!(under(out.max_total_tokens, ceilings.max_total_tokens));
                prop_assert!(under(out.max_output_tokens, ceilings.max_output_tokens));
                prop_assert!(!profile.read_only || out.read_only || ceilings.allow_clear_read_only);
                let model = out.model.as_deref().unwrap_or(DEFAULT_MODEL);
                prop_assert!(ceilings.models.contains(model));
                if let Some(model) = &overlay.model {
                    prop_assert!(names.contains(model));
                }
                prop_assert_eq!(unnamed(&out, &profile), profile);
            }
        }

        #[test]
        fn default_ceilings_only_narrow(
            profile in any_profile(),
            overlay in any_overlay(),
            names in any_models(),
        ) {
            let ceilings = Ceilings::from_profile(&profile);
            if let Ok(out) = apply(&profile, &overlay, &ceilings, &names) {
                prop_assert!(out.max_steps <= profile.max_steps);
                prop_assert!(under(out.max_total_tokens, profile.max_total_tokens));
                prop_assert!(under(out.max_output_tokens, profile.max_output_tokens));
                prop_assert!(!profile.read_only || out.read_only);
                prop_assert_eq!(
                    out.model.as_deref().unwrap_or(DEFAULT_MODEL),
                    profile.model.as_deref().unwrap_or(DEFAULT_MODEL)
                );
            }
        }
    }
}
