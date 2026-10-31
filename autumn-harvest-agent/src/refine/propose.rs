//! The proposer contract: INV and DOM proposers, bounded retries, then
//! inherit (issue #2379).
//!
//! An app implements [`OverlayProposer`]. The activity
//! [`agent_refine_propose`] runs one stage. It asks the proposer up to
//! [`ProposalAttempts`] times and checks each reply with
//! [`overlay::apply`] under the ceilings of the run. It returns one
//! [`Stage`]. The retries run inside the activity, so the engine records
//! one result per stage.

use std::borrow::Cow;
use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::agent::AgentProfile;
use crate::agent::overlay::{self, Ceilings, ProfileOverlay};
use crate::bounds::json_len;
use crate::error::{AgentError, ErrorKind};
use crate::harness::{AgentHarness, DEFAULT_MODEL, model_failure, truncate};
use crate::model::BoxFuture;

/// The most attempts of one stage.
///
/// Each attempt has a budget of at most [`DEFAULT_PROPOSER_TIMEOUT`]. So
/// eight attempts fit in the 2-hour `start_to_close` of
/// [`agent_refine_propose`].
pub const MAX_PROPOSAL_ATTEMPTS: u32 = 8;

/// The default and highest time budget of one proposer call: 13 minutes.
pub const DEFAULT_PROPOSER_TIMEOUT: Duration = Duration::from_secs(13 * 60);

/// The default number of attempts of one stage, as in Harness².
pub const DEFAULT_PROPOSAL_ATTEMPTS: u32 = 2;

/// The most characters of a rejection that a stage keeps, before the
/// truncation mark. A small result cap keeps fewer.
pub const MAX_REJECTION_CHARS: usize = 1_000;

/// The bytes that a [`Stage`] adds around its overlay JSON.
const STAGE_ENVELOPE_BYTES: u64 = 128;

/// The error type of a stage with no proposer installed.
const PROPOSER_MISSING: &str = "OverlayProposerMissing";

/// The error type of a stage whose current overlay does not apply.
const CURRENT_REFUSED: &str = "CurrentOverlayRefused";

/// The part of the setup that a proposer edits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dimension {
    /// How the agent works, independent of the subject.
    Inv,
    /// What a practitioner in the domain knows.
    Dom,
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inv => "INV",
            Self::Dom => "DOM",
        })
    }
}

/// The evidence that a proposer reads.
///
/// Issue #2377 adds a bundle of workspace files. Until then, the evidence
/// is plain text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Evidence {
    /// The evidence as one text.
    Text(String),
}

impl Evidence {
    /// Evidence as one text.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(text.into())
    }

    /// The evidence as the text that a prompt holds.
    #[must_use]
    pub fn render(&self) -> Cow<'_, str> {
        match self {
            Self::Text(text) => Cow::Borrowed(text),
        }
    }
}

/// Why a [`ProposalAttempts`] value is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("proposal attempts must be from 1 to {MAX_PROPOSAL_ATTEMPTS}, not {0}")]
pub struct ProposalAttemptsError(pub u32);

/// The number of attempts of one stage: 1 to [`MAX_PROPOSAL_ATTEMPTS`].
///
/// The default is [`DEFAULT_PROPOSAL_ATTEMPTS`].
/// `RefinePolicy::proposal_attempts` (issue #2394) sets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ProposalAttempts(NonZeroU32);

impl ProposalAttempts {
    /// The attempt count `n`.
    ///
    /// # Errors
    ///
    /// Returns [`ProposalAttemptsError`] when `n` is 0 or above
    /// [`MAX_PROPOSAL_ATTEMPTS`].
    pub const fn new(n: u32) -> Result<Self, ProposalAttemptsError> {
        match NonZeroU32::new(n) {
            Some(value) if n <= MAX_PROPOSAL_ATTEMPTS => Ok(Self(value)),
            _ => Err(ProposalAttemptsError(n)),
        }
    }

    /// The attempt count.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for ProposalAttempts {
    fn default() -> Self {
        Self(NonZeroU32::new(DEFAULT_PROPOSAL_ATTEMPTS).unwrap_or(NonZeroU32::MIN))
    }
}

impl TryFrom<u32> for ProposalAttempts {
    type Error = ProposalAttemptsError;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::new(n)
    }
}

impl From<ProposalAttempts> for u32 {
    fn from(attempts: ProposalAttempts) -> Self {
        attempts.get()
    }
}

/// What a proposer reads for one attempt.
///
/// It is `#[non_exhaustive]`, so a new field does not break a proposer.
/// Build one with [`ProposeContext::new`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ProposeContext<'a> {
    /// The part of the setup to edit.
    pub dimension: Dimension,
    /// The evidence of the source runs.
    pub evidence: &'a Evidence,
    /// The base profile of the operator. Each overlay applies to it, so a
    /// proposal that sets `system` replaces its system prompt.
    pub profile: &'a AgentProfile,
    /// The ceilings of the run. A reply above them is invalid.
    pub ceilings: &'a Ceilings,
    /// The overlay that the stage edits.
    pub current: &'a ProfileOverlay,
    /// The attempt number. The first attempt is 1.
    pub attempt: u32,
    /// Why the stage refused the reply of the last attempt.
    pub rejection: Option<&'a str>,
}

impl<'a> ProposeContext<'a> {
    /// The context of attempt 1, with no rejection.
    #[must_use]
    pub const fn new(
        dimension: Dimension,
        evidence: &'a Evidence,
        profile: &'a AgentProfile,
        ceilings: &'a Ceilings,
        current: &'a ProfileOverlay,
    ) -> Self {
        Self {
            dimension,
            evidence,
            profile,
            ceilings,
            current,
            attempt: 1,
            rejection: None,
        }
    }
}

/// The answer of a proposer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposerReply {
    /// Overlay JSON text, as a model writes it. The stage decodes it.
    Json(String),
    /// A decoded overlay.
    Overlay(ProfileOverlay),
}

/// A proposer of overlays.
///
/// An app implements it for its provider, as it does
/// [`AgentModel`](crate::AgentModel). [`ModelProposer`](super::ModelProposer)
/// implements it over a model.
///
/// Return an [`AgentError`] whose kind says what the stage does:
///
/// - `RateLimited`, `Transport` and `Unavailable` fail the activity as
///   retryable. The engine retries the whole stage.
/// - `Config`, `Authentication` and `Budget` fail the activity for good. A
///   new attempt cannot fix them.
/// - Any other kind spends one attempt.
///
/// The proposed `system` and `guardrails` text is model output. Treat it
/// as untrusted until the adoption gate (issue #2381) accepts it.
pub trait OverlayProposer: Send + Sync + fmt::Debug {
    /// Propose an overlay for one attempt.
    fn propose<'a>(
        &'a self,
        context: &'a ProposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>>;
}

/// The input of [`agent_refine_propose`].
///
/// The refine workflow (issue #2380) builds it from recorded reads. The
/// `ceilings` and `attempts` come from the operator policy (issue #2394),
/// never from the input of the refine run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposeRequest {
    /// The part of the setup to edit.
    pub dimension: Dimension,
    /// The evidence of the source runs.
    pub evidence: Evidence,
    /// The base profile of the operator. Each overlay applies to it.
    pub profile: AgentProfile,
    /// The overlay that the stage edits.
    pub current: ProfileOverlay,
    /// The ceilings of the run.
    pub ceilings: Ceilings,
    /// The number of attempts.
    #[serde(default)]
    pub attempts: ProposalAttempts,
}

/// The result of one stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageOutcome {
    /// A valid overlay that differs from `current`.
    Proposed(ProfileOverlay),
    /// A valid overlay that gives the profile of `current`. It is not a
    /// candidate.
    KeptBase,
    /// No attempt gave a valid overlay. The stage inherits `current`.
    FellBack {
        /// The rejection of the last attempt.
        error: String,
    },
}

/// One recorded stage of a proposer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    /// The part of the setup that the stage edited.
    pub dimension: Dimension,
    /// The proposer calls of the recorded run of the stage.
    pub attempts: u32,
    /// The result.
    pub outcome: StageOutcome,
}

impl Stage {
    /// The overlay that the stage gives: the proposal, or else `current`.
    #[must_use]
    pub const fn overlay<'a>(&'a self, current: &'a ProfileOverlay) -> &'a ProfileOverlay {
        match &self.outcome {
            StageOutcome::Proposed(overlay) => overlay,
            StageOutcome::KeptBase | StageOutcome::FellBack { .. } => current,
        }
    }

    /// Whether the stage gives a candidate to probe.
    #[must_use]
    pub const fn is_candidate(&self) -> bool {
        matches!(self.outcome, StageOutcome::Proposed(_))
    }
}

/// The check of one reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The reply is not valid.
    Invalid,
    /// The reply is valid and gives the profile of `current`.
    Same,
    /// The reply is valid and gives another profile.
    Differs,
}

/// What a stage does after one reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// Ask again.
    Retry,
    /// End as `Proposed`.
    Propose,
    /// End as `KeptBase`.
    Keep,
    /// End as `FellBack`.
    FallBack,
}

/// The step after the reply of attempt `attempt` of `max`.
///
/// `formal/verus/src/agent_propose.rs` proves it.
pub(crate) const fn next_step(attempt: u32, max: u32, verdict: Verdict) -> Step {
    match verdict {
        Verdict::Differs => Step::Propose,
        Verdict::Same => Step::Keep,
        Verdict::Invalid => {
            if attempt < max {
                Step::Retry
            } else {
                Step::FallBack
            }
        }
    }
}

/// Runs one stage on `harness`.
///
/// # Errors
///
/// Returns a non-retryable payload when no proposer is installed, when
/// `current` does not apply under the ceilings, or when the proposer fails
/// with a configuration kind. Returns a retryable payload when the proposer
/// fails with a retryable kind.
pub(crate) async fn run_stage(
    harness: &AgentHarness,
    request: ProposeRequest,
) -> Result<Stage, String> {
    let proposer = harness.overlay_proposer().ok_or_else(|| {
        ActivityFailure::non_retryable(
            PROPOSER_MISSING,
            "no OverlayProposer is installed: pass one to AgentHarness::proposer",
        )
        .into_error_payload()
    })?;
    let ProposeRequest {
        dimension,
        evidence,
        profile,
        current,
        ceilings,
        attempts,
    } = request;
    // A fallback hands `current` on, so `current` must apply.
    let base = overlay::apply(&profile, &current, &ceilings, harness).map_err(|err| {
        ActivityFailure::non_retryable(
            CURRENT_REFUSED,
            format!("the current overlay does not apply under the ceilings of the run: {err}"),
        )
        .into_error_payload()
    })?;
    let cap = harness.result_cap();
    let check = Check {
        profile: &profile,
        ceilings: &ceilings,
        registry: harness,
        base: with_named_model(base),
        cap,
    };
    let limit = rejection_chars(cap);
    let budget = harness.proposer_budget();
    let max = attempts.get();
    let mut rejection: Option<String> = None;
    let mut attempt = 1;
    loop {
        let context = ProposeContext {
            attempt,
            rejection: rejection.as_deref(),
            ..ProposeContext::new(dimension, &evidence, &profile, &ceilings, &current)
        };
        let reply = tokio::time::timeout(budget, proposer.propose(&context)).await;
        let checked = match reply {
            Err(_) => Err(format!(
                "the proposer did not answer within {} ms",
                budget.as_millis()
            )),
            Ok(Err(err)) if fails_the_stage(err.kind()) => return Err(model_failure(&err)),
            Ok(Err(err)) => Err(format!("the proposer failed: {err}")),
            Ok(Ok(reply)) => check.reply(reply),
        }
        .map_err(|error| truncate(&error, limit));
        let verdict = match &checked {
            Err(_) => Verdict::Invalid,
            Ok(None) => Verdict::Same,
            Ok(Some(_)) => Verdict::Differs,
        };
        if next_step(attempt, max, verdict) == Step::Retry {
            rejection = checked.err();
            attempt += 1;
            continue;
        }
        // The proof of `next_step` maps `Differs` to `Propose`, `Same` to
        // `Keep` and a last `Invalid` to `FallBack`. This match is that map.
        let outcome = match checked {
            Ok(Some(overlay)) => StageOutcome::Proposed(overlay),
            Ok(None) => StageOutcome::KeptBase,
            Err(error) => StageOutcome::FellBack { error },
        };
        return Ok(Stage {
            dimension,
            attempts: attempt,
            outcome,
        });
    }
}

/// Whether a proposer error of `kind` fails the activity.
///
/// A retryable kind fails it as retryable. A configuration kind fails it
/// for good, because a new attempt sends the same request with the same
/// setup.
const fn fails_the_stage(kind: ErrorKind) -> bool {
    kind.is_retryable()
        || matches!(
            kind,
            ErrorKind::Config | ErrorKind::Authentication | ErrorKind::Budget
        )
}

/// The most characters of a rejection under the result cap `cap`.
///
/// JSON writes one character in at most six bytes (`\u0000`). So a sixth
/// of the cap, less the stage envelope, always fits. A cap of zero is no
/// cap.
fn rejection_chars(cap: u64) -> usize {
    if cap == 0 {
        return MAX_REJECTION_CHARS;
    }
    let room = cap.saturating_sub(STAGE_ENVELOPE_BYTES) / 6;
    usize::try_from(room).map_or(MAX_REJECTION_CHARS, |room| room.min(MAX_REJECTION_CHARS))
}

/// `profile` with its model named. `None` and [`DEFAULT_MODEL`] name the
/// same model, so the two must compare equal.
fn with_named_model(mut profile: AgentProfile) -> AgentProfile {
    if profile.model.is_none() {
        profile.model = Some(DEFAULT_MODEL.to_owned());
    }
    profile
}

/// The inputs that check one reply.
struct Check<'a> {
    profile: &'a AgentProfile,
    ceilings: &'a Ceilings,
    registry: &'a AgentHarness,
    /// The profile under `current`, with its model named.
    base: AgentProfile,
    /// The activity result cap, in bytes. Zero is no cap.
    cap: u64,
}

impl Check<'_> {
    /// Checks one reply. `Ok(None)` means that the reply gives the profile
    /// of `current`. `Ok(Some)` holds a proposal. `Err` holds a rejection.
    fn reply(&self, reply: ProposerReply) -> Result<Option<ProfileOverlay>, String> {
        let overlay = match reply {
            ProposerReply::Json(text) => serde_json::from_str::<ProfileOverlay>(&text)
                .map_err(|err| format!("the reply is not a valid overlay: {err}"))?,
            ProposerReply::Overlay(overlay) => overlay,
        };
        let applied = overlay::apply(self.profile, &overlay, self.ceilings, self.registry)
            .map_err(|err| format!("the overlay is refused: {err}"))?;
        if with_named_model(applied) == self.base {
            return Ok(None);
        }
        let bytes = json_len(&overlay).saturating_add(STAGE_ENVELOPE_BYTES);
        if self.cap > 0 && bytes > self.cap {
            return Err(format!(
                "the overlay is {bytes} bytes, over the result cap of {} bytes",
                self.cap
            ));
        }
        Ok(Some(overlay))
    }
}

/// One proposer stage, as an activity.
///
/// It asks the installed [`OverlayProposer`] up to `request.attempts`
/// times. A retryable proposer failure retries the whole stage with
/// backoff.
///
/// The activity sends no heartbeat. On Postgres, a lost worker therefore
/// holds the stage until its 2-hour `start_to_close` ends. Then the engine
/// runs the stage again from attempt 1.
///
/// # Errors
///
/// Returns an error when no harness or no proposer is installed, when
/// `current` does not apply, or when the proposer fails with a
/// configuration kind. A retryable failure on every engine attempt also
/// fails the activity. The refine workflow (issue #2380) then inherits
/// `current`.
#[activity(
    start_to_close = "2h",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn agent_refine_propose(
    ctx: &ActivityContext,
    request: ProposeRequest,
) -> Result<Stage, String> {
    run_stage(crate::workflow::harness(ctx)?, request).await
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unnecessary_wraps
    )]

    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::agent::overlay::OverlayError;
    use crate::model::{AgentModel, ChatRequest, ChatResponse};
    use proptest::prelude::*;
    use serde_json::json;

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

    type Scripted = Result<ProposerReply, AgentError>;

    /// A proposer that answers from a script and records each context.
    #[derive(Debug, Default)]
    struct ScriptedProposer {
        replies: Mutex<VecDeque<Scripted>>,
        calls: AtomicUsize,
        seen: Mutex<Vec<(Dimension, u32, Option<String>)>>,
        delay: Option<Duration>,
    }

    impl ScriptedProposer {
        fn new(replies: Vec<Scripted>) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(replies.into()),
                ..Self::default()
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl OverlayProposer for ScriptedProposer {
        fn propose<'a>(
            &'a self,
            context: &'a ProposeContext<'a>,
        ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push((
                context.dimension,
                context.attempt,
                context.rejection.map(str::to_owned),
            ));
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(AgentError::new(ErrorKind::Provider, "script ran out")));
            let delay = self.delay;
            Box::pin(async move {
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                reply
            })
        }
    }

    fn profile() -> AgentProfile {
        AgentProfile::new("support")
            .system("Help the user.")
            .max_steps(20)
    }

    fn harness(proposer: Arc<ScriptedProposer>) -> AgentHarness {
        AgentHarness::new(Arc::new(NoModel)).proposer(proposer)
    }

    fn request(attempts: u32, current: ProfileOverlay) -> ProposeRequest {
        let profile = profile();
        ProposeRequest {
            dimension: Dimension::Inv,
            evidence: Evidence::text("run 1 skipped the refund check"),
            ceilings: Ceilings::from_profile(&profile),
            profile,
            current,
            attempts: ProposalAttempts::new(attempts).unwrap(),
        }
    }

    fn current() -> ProfileOverlay {
        ProfileOverlay {
            system: Some("Help the user. Check refunds.".to_owned()),
            ..ProfileOverlay::default()
        }
    }

    fn valid() -> Scripted {
        Ok(ProposerReply::Json(
            r#"{"system": "Help the user. Check refunds first.", "max_steps": 12}"#.to_owned(),
        ))
    }

    fn valid_overlay() -> ProfileOverlay {
        ProfileOverlay {
            system: Some("Help the user. Check refunds first.".to_owned()),
            max_steps: Some(12),
            ..ProfileOverlay::default()
        }
    }

    /// A reply past the default ceilings: more steps than the profile.
    fn past_ceiling() -> Scripted {
        Ok(ProposerReply::Overlay(ProfileOverlay {
            max_steps: Some(50),
            ..ProfileOverlay::default()
        }))
    }

    /// A reply that does not decode: `tools` is not an overlay part.
    fn undecodable() -> Scripted {
        Ok(ProposerReply::Json(r#"{"tools": ["shell"]}"#.to_owned()))
    }

    async fn stage(proposer: &Arc<ScriptedProposer>, request: ProposeRequest) -> Stage {
        run_stage(&harness(Arc::clone(proposer)), request)
            .await
            .expect("the stage ends")
    }

    #[tokio::test]
    async fn invalid_then_valid_gives_proposed_with_two_attempts() {
        let proposer = ScriptedProposer::new(vec![past_ceiling(), valid()]);
        let request = ProposeRequest {
            attempts: ProposalAttempts::default(),
            ..request(2, current())
        };
        let stage = stage(&proposer, request).await;
        assert_eq!(stage.outcome, StageOutcome::Proposed(valid_overlay()));
        assert_eq!(stage.attempts, 2);
        assert_eq!(proposer.calls(), 2);
        assert!(stage.is_candidate());
    }

    #[tokio::test]
    async fn invalid_twice_falls_back_to_current() {
        let proposer = ScriptedProposer::new(vec![undecodable(), past_ceiling(), valid()]);
        let current = current();
        let request = ProposeRequest {
            attempts: ProposalAttempts::default(),
            ..request(1, current.clone())
        };
        let stage = stage(&proposer, request).await;
        assert!(
            matches!(stage.outcome, StageOutcome::FellBack { .. }),
            "{stage:?}"
        );
        assert_eq!(stage.attempts, 2);
        assert_eq!(stage.overlay(&current), &current);
        assert_eq!(proposer.calls(), 2, "the third reply is never asked for");
        assert!(!stage.is_candidate());
    }

    #[tokio::test]
    async fn three_attempts_take_a_third_reply() {
        let proposer = ScriptedProposer::new(vec![undecodable(), past_ceiling(), valid()]);
        let stage = stage(&proposer, request(3, current())).await;
        assert_eq!(stage.outcome, StageOutcome::Proposed(valid_overlay()));
        assert_eq!(stage.attempts, 3);
        assert_eq!(proposer.calls(), 3);
    }

    #[tokio::test]
    async fn one_attempt_falls_back_on_one_invalid_reply() {
        let proposer = ScriptedProposer::new(vec![past_ceiling(), valid()]);
        let current = current();
        let stage = stage(&proposer, request(1, current.clone())).await;
        assert!(matches!(stage.outcome, StageOutcome::FellBack { .. }));
        assert_eq!(stage.attempts, 1);
        assert_eq!(stage.overlay(&current), &current);
        assert_eq!(proposer.calls(), 1);
    }

    #[tokio::test]
    async fn a_valid_reply_equal_to_current_keeps_the_base() {
        let current = current();
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Overlay(current.clone()))]);
        let stage = stage(&proposer, request(2, current.clone())).await;
        assert_eq!(stage.outcome, StageOutcome::KeptBase);
        assert_eq!(stage.attempts, 1);
        assert_eq!(stage.overlay(&current), &current);
        assert!(!stage.is_candidate());
    }

    #[tokio::test]
    async fn a_reply_that_restates_a_profile_value_keeps_the_base() {
        let current = current();
        let restated = ProfileOverlay {
            max_steps: Some(20),
            ..current.clone()
        };
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Overlay(restated))]);
        let stage = stage(&proposer, request(2, current)).await;
        assert_eq!(stage.outcome, StageOutcome::KeptBase);
    }

    #[tokio::test]
    async fn a_reply_past_the_ceilings_names_the_overlay_error() {
        let proposer = ScriptedProposer::new(vec![past_ceiling(), past_ceiling()]);
        let stage = stage(&proposer, request(2, current())).await;
        let StageOutcome::FellBack { error } = &stage.outcome else {
            panic!("{stage:?}");
        };
        let text = OverlayError::AboveCeiling { field: "max_steps" }.to_string();
        assert!(error.contains(&text), "{error:?} must hold {text:?}");
    }

    #[tokio::test]
    async fn an_undecodable_reply_names_the_unknown_field() {
        let proposer = ScriptedProposer::new(vec![undecodable()]);
        let stage = stage(&proposer, request(1, current())).await;
        let StageOutcome::FellBack { error } = &stage.outcome else {
            panic!("{stage:?}");
        };
        assert!(error.contains("tools"), "{error:?}");
    }

    #[tokio::test]
    async fn the_next_attempt_reads_the_last_rejection() {
        let proposer = ScriptedProposer::new(vec![past_ceiling(), valid()]);
        let _ = stage(&proposer, request(2, current())).await;
        let seen = proposer.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (Dimension::Inv, 1, None));
        let (_, attempt, rejection) = &seen[1];
        assert_eq!(*attempt, 2);
        let rejection = rejection
            .as_deref()
            .expect("the second attempt has feedback");
        assert!(rejection.contains("max_steps"), "{rejection:?}");
    }

    #[tokio::test]
    async fn a_retryable_proposer_error_fails_the_activity_as_retryable() {
        let proposer = ScriptedProposer::new(vec![Err(AgentError::new(
            ErrorKind::RateLimited,
            "slow down",
        ))]);
        let err = run_stage(&harness(Arc::clone(&proposer)), request(2, current()))
            .await
            .expect_err("a rate limit fails the activity");
        let failure = autumn_harvest::failure::parse_typed_payload(&err).expect("typed");
        assert!(!failure.non_retryable, "{err}");
        assert_eq!(proposer.calls(), 1);
    }

    #[tokio::test]
    async fn a_non_retryable_proposer_error_spends_one_attempt() {
        let proposer = ScriptedProposer::new(vec![
            Err(AgentError::new(ErrorKind::Provider, "refused")),
            valid(),
        ]);
        let stage = stage(&proposer, request(2, current())).await;
        assert_eq!(stage.outcome, StageOutcome::Proposed(valid_overlay()));
        assert_eq!(stage.attempts, 2);
    }

    #[tokio::test]
    async fn a_slow_attempt_is_a_rejection() {
        let proposer = Arc::new(ScriptedProposer {
            replies: Mutex::new(vec![valid()].into()),
            delay: Some(Duration::from_secs(5)),
            ..ScriptedProposer::default()
        });
        let harness = harness(Arc::clone(&proposer)).proposer_timeout(Duration::from_millis(20));
        let stage = run_stage(&harness, request(1, current())).await.unwrap();
        let StageOutcome::FellBack { error } = &stage.outcome else {
            panic!("{stage:?}");
        };
        assert!(error.contains("did not answer"), "{error:?}");
    }

    #[test]
    fn the_proposer_budget_never_rises_above_the_default() {
        let proposer = ScriptedProposer::new(Vec::new());
        let harness = harness(proposer);
        assert_eq!(harness.proposer_budget(), DEFAULT_PROPOSER_TIMEOUT);
        let raised = harness.clone().proposer_timeout(Duration::from_secs(3_600));
        assert_eq!(raised.proposer_budget(), DEFAULT_PROPOSER_TIMEOUT);
        let lowered = harness.proposer_timeout(Duration::from_secs(5));
        assert_eq!(lowered.proposer_budget(), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn an_overlay_over_the_result_cap_is_a_rejection() {
        let big = ProfileOverlay {
            system: Some("x".repeat(4_096)),
            ..ProfileOverlay::default()
        };
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Overlay(big))]);
        let harness = harness(Arc::clone(&proposer)).max_result_bytes(2_048);
        let stage = run_stage(&harness, request(1, current())).await.unwrap();
        let StageOutcome::FellBack { error } = &stage.outcome else {
            panic!("{stage:?}");
        };
        assert!(error.contains("result cap"), "{error:?}");
    }

    #[tokio::test]
    async fn a_long_rejection_is_cut() {
        let long = format!(r#"{{"{}": 1}}"#, "k".repeat(5_000));
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Json(long))]);
        let stage = stage(&proposer, request(1, current())).await;
        let StageOutcome::FellBack { error } = &stage.outcome else {
            panic!("{stage:?}");
        };
        assert!(
            error.chars().count() <= MAX_REJECTION_CHARS + 16,
            "{}",
            error.len()
        );
    }

    #[tokio::test]
    async fn a_stage_with_no_proposer_fails_for_good() {
        let harness = AgentHarness::new(Arc::new(NoModel));
        let err = run_stage(&harness, request(2, current()))
            .await
            .expect_err("no proposer");
        let failure = autumn_harvest::failure::parse_typed_payload(&err).expect("typed");
        assert!(failure.non_retryable, "{err}");
        assert_eq!(failure.error_type, PROPOSER_MISSING);
    }

    #[tokio::test]
    async fn the_activity_reads_the_proposer_from_worker_state() {
        use std::any::TypeId;
        use std::collections::HashMap;

        use autumn_harvest::context::SharedStateMap;

        let proposer = ScriptedProposer::new(vec![valid()]);
        let mut state: SharedStateMap = HashMap::new();
        state.insert(
            TypeId::of::<AgentHarness>(),
            Box::new(harness(Arc::clone(&proposer))),
        );
        let ctx = ActivityContext::new_test_with_state(Arc::new(state));
        let input = serde_json::to_value(request(2, current())).unwrap();
        let raw = (agent_refine_propose_info().handler)(&ctx, input)
            .await
            .expect("the activity ends");
        let recorded: Stage = serde_json::from_value(raw).unwrap();
        assert_eq!(recorded.outcome, StageOutcome::Proposed(valid_overlay()));
        assert_eq!(agent_refine_propose_info().name, "agent_refine_propose");
    }

    #[tokio::test]
    async fn the_proposer_reads_the_base_profile_and_the_ceilings() {
        #[derive(Debug, Default)]
        struct Peek(Mutex<Option<(Option<String>, Option<u32>)>>);
        impl OverlayProposer for Peek {
            fn propose<'a>(
                &'a self,
                context: &'a ProposeContext<'a>,
            ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
                *self.0.lock().unwrap() =
                    Some((context.profile.system.clone(), context.ceilings.max_steps));
                Box::pin(async { Ok(ProposerReply::Json("{}".to_owned())) })
            }
        }
        let peek = Arc::new(Peek::default());
        let harness = AgentHarness::new(Arc::new(NoModel)).proposer(Arc::clone(&peek) as _);
        let _ = run_stage(&harness, request(1, current())).await.unwrap();
        let seen = peek.0.lock().unwrap().clone();
        assert_eq!(seen, Some((Some("Help the user.".to_owned()), Some(20))));
    }

    #[tokio::test]
    async fn a_fallback_fits_a_small_result_cap() {
        let key = "\u{1}".repeat(1_000);
        let reply = serde_json::to_string(&serde_json::json!({ key: 1 })).unwrap();
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Json(reply))]);
        let harness = harness(Arc::clone(&proposer)).max_result_bytes(2_048);
        let stage = run_stage(&harness, request(1, current())).await.unwrap();
        assert!(
            matches!(stage.outcome, StageOutcome::FellBack { .. }),
            "{stage:?}"
        );
        assert!(json_len(&stage) <= 2_048, "{} bytes", json_len(&stage));
    }

    #[tokio::test]
    async fn a_configuration_error_fails_the_stage_for_good() {
        for kind in [
            ErrorKind::Config,
            ErrorKind::Authentication,
            ErrorKind::Budget,
        ] {
            let proposer =
                ScriptedProposer::new(vec![Err(AgentError::new(kind, "bad key")), valid()]);
            let err = run_stage(&harness(Arc::clone(&proposer)), request(2, current()))
                .await
                .expect_err("a configuration error fails the stage");
            let failure = autumn_harvest::failure::parse_typed_payload(&err).expect("typed");
            assert!(failure.non_retryable, "{kind:?}: {err}");
            assert_eq!(proposer.calls(), 1, "{kind:?}");
        }
    }

    #[tokio::test]
    async fn a_current_overlay_past_the_ceilings_is_refused() {
        let proposer = ScriptedProposer::new(vec![valid()]);
        let current = ProfileOverlay {
            max_steps: Some(50),
            ..ProfileOverlay::default()
        };
        let err = run_stage(&harness(Arc::clone(&proposer)), request(2, current))
            .await
            .expect_err("the current overlay is past the ceilings");
        let failure = autumn_harvest::failure::parse_typed_payload(&err).expect("typed");
        assert!(failure.non_retryable, "{err}");
        assert_eq!(failure.error_type, CURRENT_REFUSED);
        assert_eq!(proposer.calls(), 0);
    }

    #[tokio::test]
    async fn naming_the_default_model_keeps_the_base() {
        let reply = ProfileOverlay {
            model: Some(crate::harness::DEFAULT_MODEL.to_owned()),
            ..current()
        };
        let proposer = ScriptedProposer::new(vec![Ok(ProposerReply::Overlay(reply))]);
        let stage = stage(&proposer, request(2, current())).await;
        assert_eq!(stage.outcome, StageOutcome::KeptBase);
    }

    #[test]
    fn a_zero_proposer_timeout_keeps_the_default() {
        let harness = harness(ScriptedProposer::new(Vec::new())).proposer_timeout(Duration::ZERO);
        assert_eq!(harness.proposer_budget(), DEFAULT_PROPOSER_TIMEOUT);
    }

    #[test]
    fn ceilings_with_a_missing_key_do_not_decode() {
        let ceilings = Ceilings::from_profile(&profile());
        let full = serde_json::to_value(&ceilings).unwrap();
        assert_eq!(
            serde_json::from_value::<Ceilings>(full.clone()).unwrap(),
            ceilings
        );
        for key in [
            "max_steps",
            "max_total_tokens",
            "max_output_tokens",
            "models",
        ] {
            let mut part = full.clone();
            part.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<Ceilings>(part).is_err(), "{key}");
        }
        let mut open = full;
        open["max_steps"] = serde_json::Value::Null;
        let open: Ceilings = serde_json::from_value(open).unwrap();
        assert_eq!(open.max_steps, None, "an explicit null is no limit");
    }

    #[test]
    fn attempts_are_one_to_eight_and_default_to_two() {
        assert_eq!(ProposalAttempts::default().get(), 2);
        assert_eq!(ProposalAttempts::new(0), Err(ProposalAttemptsError(0)));
        assert_eq!(
            ProposalAttempts::new(MAX_PROPOSAL_ATTEMPTS + 1),
            Err(ProposalAttemptsError(MAX_PROPOSAL_ATTEMPTS + 1))
        );
        for n in 1..=MAX_PROPOSAL_ATTEMPTS {
            assert_eq!(ProposalAttempts::new(n).unwrap().get(), n);
        }
        assert!(serde_json::from_value::<ProposalAttempts>(json!(0)).is_err());
        assert!(serde_json::from_value::<ProposalAttempts>(json!(9)).is_err());
        assert_eq!(
            serde_json::from_value::<ProposalAttempts>(json!(3)).unwrap(),
            ProposalAttempts::new(3).unwrap()
        );
    }

    #[test]
    fn a_request_without_attempts_takes_the_default() {
        let mut value = serde_json::to_value(request(3, current())).unwrap();
        value.as_object_mut().unwrap().remove("attempts");
        let decoded: ProposeRequest = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.attempts, ProposalAttempts::default());
    }

    #[test]
    fn a_stage_round_trips_in_a_stable_shape() {
        let stage = Stage {
            dimension: Dimension::Dom,
            attempts: 2,
            outcome: StageOutcome::FellBack {
                error: "no".to_owned(),
            },
        };
        let value = serde_json::to_value(&stage).unwrap();
        assert_eq!(
            value,
            json!({"dimension": "dom", "attempts": 2, "outcome": {"fell_back": {"error": "no"}}})
        );
        assert_eq!(serde_json::from_value::<Stage>(value).unwrap(), stage);
        let kept = Stage {
            outcome: StageOutcome::KeptBase,
            ..stage
        };
        assert_eq!(serde_json::to_value(&kept).unwrap()["outcome"], "kept_base");
        assert_eq!(Dimension::Inv.to_string(), "INV");
        assert_eq!(Dimension::Dom.to_string(), "DOM");
    }

    #[test]
    fn the_kernel_retries_only_an_invalid_reply_below_the_limit() {
        use Step::{FallBack, Keep, Propose, Retry};
        use Verdict::{Differs, Invalid, Same};
        let cases = [
            (1, 2, Invalid, Retry),
            (2, 2, Invalid, FallBack),
            (1, 1, Invalid, FallBack),
            (1, 2, Same, Keep),
            (2, 2, Differs, Propose),
            (3, 2, Invalid, FallBack),
        ];
        for (attempt, max, verdict, want) in cases {
            assert_eq!(next_step(attempt, max, verdict), want, "{attempt}/{max}");
        }
    }

    fn verdict() -> impl Strategy<Value = Verdict> {
        prop_oneof![
            Just(Verdict::Invalid),
            Just(Verdict::Same),
            Just(Verdict::Differs)
        ]
    }

    /// The stage loop over scripted verdicts. Returns the last step and
    /// the attempts used.
    fn drive(verdicts: &[Verdict], max: u32) -> (Step, u32) {
        let mut attempt = 1;
        loop {
            let verdict = verdicts
                .get(attempt as usize - 1)
                .copied()
                .unwrap_or(Verdict::Invalid);
            match next_step(attempt, max, verdict) {
                Step::Retry => attempt += 1,
                step => return (step, attempt),
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn a_stage_is_bounded_and_stops_at_the_first_valid_reply(
            verdicts in proptest::collection::vec(verdict(), 0..12),
            max in 1..=MAX_PROPOSAL_ATTEMPTS,
        ) {
            let (step, attempts) = drive(&verdicts, max);
            prop_assert!((1..=max).contains(&attempts));
            let asked = &verdicts[..verdicts.len().min(attempts as usize)];
            let first_valid = verdicts
                .iter()
                .take(max as usize)
                .position(|v| *v != Verdict::Invalid);
            match first_valid {
                None => {
                    prop_assert_eq!(step, Step::FallBack);
                    prop_assert_eq!(attempts, max);
                }
                Some(at) => {
                    prop_assert_eq!(attempts as usize, at + 1);
                    let want = if verdicts[at] == Verdict::Same { Step::Keep } else { Step::Propose };
                    prop_assert_eq!(step, want);
                    prop_assert!(asked[..at].iter().all(|v| *v == Verdict::Invalid));
                }
            }
        }
    }
}
