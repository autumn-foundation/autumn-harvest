//! Anonymised comparison reports (issue #2376).
//!
//! A model that reads a comparison to pick a candidate is biased by labels
//! and by order. [`compare_anonymised`] removes both. It gives each candidate
//! an opaque [`AnonId`] and lists the candidates in a seeded order. The
//! text holds no model name, no overlay digest and no label. The key that
//! maps each id back to its candidate is a separate field.
//!
//! # The ids
//!
//! [`anon::assign`] gives the ids, as for an evidence bundle (issue #2377).
//! Its key of a candidate is [`Candidate::digest`]. The id is `c-` and a hex
//! prefix of the SHA-256 of the seed, the source run id and that digest.
//! The prefix has at least [`MIN_ANON_HEX`](anon::MIN_ANON_HEX) digits. It
//! grows until no two ids are equal. The pool lists the ids in order, so the
//! input order changes neither the ids nor the text.
//!
//! The source run id is the run id of the first recorded model request. It
//! is the execution id of the source, unless its task names another run id
//! source. A source with no model turn uses the empty string.
//!
//! # Failure texts
//!
//! A failure text comes from the harness or the provider, so it can name a
//! model. Each run end error and each report error replaces every identity
//! of every candidate in the pool with [`REDACTED`]. The source end gets the
//! same treatment, and so does the error of a [`LiveCheck`]. The routes
//! include each route that a run asked for, such as the model of a recorded
//! peer. An identity is a label,
//! a digest, a route or a model id. Only a whole name is replaced. A name
//! ends at a character that is not a letter, a digit, `-` or `_`. So a
//! short route, such as `a`, also replaces that word. The same rule redacts
//! each reject reason of a scorer (issue #2447). Each judge route of a
//! scorer counts as a route, with the model id behind it.
//!
//! # Limits
//!
//! The text renders each model reply as written. A reply can name its own
//! model. Token usage stays in the pool, and the tokenizer can show a model
//! family. A harness setting, such as a policy timeout, can show in a policy
//! reason. Keep the seed from the reader, because the seed and the digests
//! give the ids.
//!
//! Two candidates with the same scorer and harness share one source verdict
//! (issue #2447). Only the first of them in pool order holds its tokens. So
//! the source verdicts and the scorer usage can show which ids share a
//! scorer or a harness.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};

use autumn_harvest::event::WorkflowEvent;
use serde::{Deserialize, Serialize};

use super::anon::{self, AnonId, CandidateKey};
use super::{
    Candidate, Divergence, EvalError, Evaluation, Recording, RunEnd, Side, SourceScores,
    TurnDecision, TurnDiff, Verdict, run_with_routes,
};
use crate::harness::TRUNCATED;
use crate::message::TokenUsage;
use crate::score;

/// The text that replaces an identity in a failure text.
pub const REDACTED: &str = "[redacted]";

/// What an id resolves to. Never give it to the reader of the text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateRef {
    /// The index of the candidate in the input slice.
    pub index: usize,
    /// The [`Candidate::digest`].
    pub digest: String,
    /// The [`Evaluation::overlay_digest`].
    pub overlay_digest: String,
    /// The [`Candidate::label`], if any.
    pub label: Option<String>,
}

/// One [`Evaluation`] with no identity.
///
/// It keeps every field of `Evaluation` except `overlay_digest` and
/// `candidate_cost_usd_micros`. A price names a model, so the cost is an
/// identity. The recorded end is the same for each candidate, so
/// [`AnonymisedPool::recorded`] holds it once.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnonymisedEvaluation {
    /// The opaque id of the candidate.
    pub id: AnonId,
    /// One comparison per model turn, as in [`Evaluation::turns`].
    pub turns: Vec<TurnDiff>,
    /// The index of the first divergent turn.
    pub first_divergence: Option<usize>,
    /// How the candidate run ended.
    pub end: RunEnd,
    /// The rule of [`Evaluation::end_diverged`].
    pub end_diverged: bool,
    /// The candidate tool calls that got a recorded outcome.
    pub replayed_tool_calls: u32,
    /// The candidate tool calls that got the stub outcome.
    pub stubbed_tool_calls: u32,
    /// `true` when the turn cap stopped the candidate run.
    pub turn_cap_reached: bool,
    /// The rule of [`Evaluation::recorded_verdict`], with each reject
    /// reason redacted (issue #2447).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_verdict: Option<score::Verdict>,
    /// The rule of [`Evaluation::candidate_verdict`], with each reject
    /// reason redacted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_verdict: Option<score::Verdict>,
    /// The rule of [`Evaluation::scorer_usage`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorer_usage: Option<TokenUsage>,
    /// The rule of [`Evaluation::candidate_usage`] (issue #2380). The pool
    /// leaves out the cost, because a price names a model.
    #[serde(default, skip_serializing_if = "is_no_usage")]
    pub candidate_usage: TokenUsage,
    /// The live re-probe of the candidate, with each error redacted. `None`
    /// when the caller holds no live record, such as [`compare_anonymised`]
    /// or an input recorded before this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveCheck>,
}

/// The live re-probe of one candidate, with no identity.
///
/// A refine run re-probes a candidate live on a fork when its replay probe
/// stubs a tool call. This type holds no fork id, no token count and no
/// cost. A price names a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "live", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LiveCheck {
    /// The replay probe stubbed no tool call, so no live run was needed.
    NotNeeded,
    /// The backend has no fork, so the live run did not start.
    Skipped,
    /// The fork or its wait failed, so the live run gave no end. This
    /// says nothing about the candidate. The error stays out of the pool,
    /// because a host error can name a run or a fork.
    NoEnd,
    /// The live run completed.
    Completed,
    /// The live run did not complete. It failed, ended in another state, or
    /// did not end in time. This is a live failure.
    Failed {
        /// The error of the run, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

impl LiveCheck {
    /// `true` for a live failure: a live run that did not complete. A
    /// refine run never picks such a candidate.
    #[must_use]
    pub const fn failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }

    /// `self` with each identity of `scrub` replaced in its error.
    fn scrubbed(self, scrub: &Scrub) -> Self {
        match self {
            Self::Failed { error } => Self::Failed {
                error: error.map(|error| redact(&uncut(&error), &scrub.identities)),
            },
            other => other,
        }
    }
}

/// `text` with no part of a name before its cut mark.
///
/// A host cuts a long error before the pool redacts it. A cut can split a
/// model id, and the redaction matches whole names only. So the part of the
/// name before the mark goes too.
fn uncut(text: &str) -> String {
    let Some(kept) = text.strip_suffix(TRUNCATED) else {
        return text.to_owned();
    };
    let is_name_char = |c: char| c.is_alphanumeric() || c == '-' || c == '_';
    format!("{}{TRUNCATED}", kept.trim_end_matches(is_name_char))
}

/// Whether `usage` is empty, so an entry leaves it out.
fn is_no_usage(usage: &TokenUsage) -> bool {
    *usage == TokenUsage::default()
}

/// The candidates of one source, in pool order, with no identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnonymisedPool {
    /// How the recorded run ended.
    pub recorded: RunEnd,
    /// One entry per candidate, in the seeded order.
    pub candidates: Vec<AnonymisedEvaluation>,
}

/// The result of [`compare_anonymised`].
///
/// Give [`text`](Self::text) or [`pool`](Self::pool) to the reader. Keep
/// [`key`](Self::key) apart. The type does not implement `Serialize`, so
/// one call cannot encode the text with the key. Its `Debug` form shows
/// the number of keys only.
#[derive(Clone, PartialEq)]
pub struct AnonymisedReport {
    /// The rendered pool. It equals [`AnonymisedPool::render`].
    pub text: String,
    /// The structured pool.
    pub pool: AnonymisedPool,
    /// Each id and its candidate.
    pub key: BTreeMap<AnonId, CandidateRef>,
}

impl fmt::Debug for AnonymisedReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnonymisedReport")
            .field("text", &self.text)
            .field("pool", &self.pool)
            .field("key", &format_args!("<{} ids, hidden>", self.key.len()))
            .finish()
    }
}

impl AnonymisedReport {
    /// The candidate of `id`, or `None` for an id outside the pool.
    #[must_use]
    pub fn resolve(&self, id: &AnonId) -> Option<&CandidateRef> {
        self.key.get(id)
    }
}

/// Evaluate each of `candidates` against one recorded `history`, then
/// report them under opaque ids in an order seeded by `seed` (issue #2376).
///
/// Each overlay passes [`overlay::apply`](crate::agent::overlay::apply)
/// before the first model call. The pool also checks the ids for a
/// duplicate first. With a [`Candidate::scorer`], the pool then scores the
/// source, in pool order (issue #2447). The candidates then run one after the
/// other, in pool order. An empty slice gives an empty pool.
///
/// # Errors
///
/// The errors of [`evaluate`](super::evaluate). Two candidates with equal
/// digests give [`EvalError::DuplicateCandidate`]. A refused history,
/// overlay or pool costs no model call.
pub async fn compare_anonymised(
    history: &[WorkflowEvent],
    candidates: &[Candidate],
    seed: u64,
) -> Result<AnonymisedReport, EvalError> {
    let recording = Recording::read(history)?;
    let tasks = candidates
        .iter()
        .map(|candidate| candidate.task_under_overlay(&recording.task))
        .collect::<Result<Vec<_>, _>>()?;
    let digests: Vec<String> = candidates.iter().map(Candidate::digest).collect();
    let source = recording.run_id.as_deref().unwrap_or_default();
    let slots = pool_slots(seed, source, &digests)?;

    let mut jobs: Vec<_> = candidates
        .iter()
        .zip(tasks)
        .zip(slots)
        .enumerate()
        .collect();
    jobs.sort_by_key(|(_, (_, (rank, _)))| *rank);
    // Each source score comes before the first model call. Pool order keeps
    // the input order out of the usage of each entry.
    let mut sources = SourceScores::default();
    let mut scored = Vec::with_capacity(jobs.len());
    for (_, ((candidate, _), _)) in &jobs {
        scored.push(sources.of(&recording, candidate).await?);
    }
    let mut runs = Vec::with_capacity(jobs.len());
    let mut asked = BTreeSet::new();
    for ((index, ((candidate, task), (_, id))), source) in jobs.into_iter().zip(scored) {
        let (evaluation, routes) = run_with_routes(&recording, task, candidate, source).await?;
        asked.extend(routes);
        runs.push((index, candidate, id, evaluation));
    }

    // The identities wait for every run, so each route that any run asked
    // for counts.
    let identities = identities(candidates, &asked);
    let scrub = Scrub {
        identities: identities.clone(),
        clear_run_ids: recording.run_id.is_none(),
    };
    // The source run id is the same for each candidate, so it stays.
    let source_scrub = Scrub {
        identities,
        clear_run_ids: false,
    };
    let mut entries = Vec::with_capacity(runs.len());
    let mut key = BTreeMap::new();
    for (index, candidate, id, evaluation) in runs {
        key.insert(
            id.clone(),
            CandidateRef {
                index,
                digest: candidate.digest(),
                overlay_digest: evaluation.overlay_digest.clone(),
                label: candidate.label.clone(),
            },
        );
        entries.push(anonymise(evaluation, id, &scrub));
    }
    let pool = AnonymisedPool {
        recorded: scrub_end(recording.end, &source_scrub),
        candidates: entries,
    };
    Ok(AnonymisedReport {
        text: pool.render(),
        pool,
        key,
    })
}

/// The pool of recorded `entries` under their ids, in id order. No
/// candidate runs again. Each entry holds its live re-probe, if any.
///
/// `identities` are the names to redact from each failure text, such as
/// the overlay digests and the model names of the pool.
pub fn recorded_pool(
    recorded: RunEnd,
    mut entries: Vec<(AnonId, Evaluation, Option<LiveCheck>)>,
    mut identities: Vec<String>,
) -> AnonymisedPool {
    identities.retain(|name| !name.is_empty());
    identities.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    identities.dedup();
    let scrub = Scrub {
        identities,
        clear_run_ids: false,
    };
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    AnonymisedPool {
        recorded: scrub_end(recorded, &scrub),
        candidates: entries
            .into_iter()
            .map(|(id, evaluation, live)| AnonymisedEvaluation {
                live: live.map(|live| live.scrubbed(&scrub)),
                ..anonymise(evaluation, id, &scrub)
            })
            .collect(),
    }
}

/// What [`anonymise`] removes from the values that it keeps.
struct Scrub {
    /// Each identity of the pool, longest first.
    identities: Vec<String>,
    /// The source has no run id. The candidate report then holds the run id
    /// of the test engine, which changes on each run.
    clear_run_ids: bool,
}

/// Every identity of every candidate, longest first, so that a longer name
/// is replaced before a name inside it. `asked` holds each route that any
/// run asked for. Each candidate harness resolves each such route.
fn identities(candidates: &[Candidate], asked: &BTreeSet<Option<String>>) -> Vec<String> {
    let mut names: Vec<String> = candidates
        .iter()
        .flat_map(|candidate| candidate.identities(asked))
        .filter(|name| !name.is_empty())
        .collect();
    names.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    names.dedup();
    names
}

/// `evaluation` under `id`, with every identity field left out, and with
/// the run end scrubbed.
fn anonymise(evaluation: Evaluation, id: AnonId, scrub: &Scrub) -> AnonymisedEvaluation {
    // An exhaustive destructure. A new field of `Evaluation` fails the build
    // here until this function keeps it or names it an identity.
    let Evaluation {
        turns,
        first_divergence,
        // The pool holds the recorded end once.
        recorded: _,
        candidate,
        end_diverged,
        replayed_tool_calls,
        stubbed_tool_calls,
        turn_cap_reached,
        // The identity of the candidate.
        overlay_digest: _,
        recorded_verdict,
        candidate_verdict,
        scorer_usage,
        candidate_usage,
        // A price names a model, so the cost is an identity.
        candidate_cost_usd_micros: _,
        // Each call names its model and its price.
        candidate_calls: _,
    } = evaluation;
    AnonymisedEvaluation {
        id,
        turns,
        first_divergence,
        end: scrub_end(candidate, scrub),
        end_diverged,
        replayed_tool_calls,
        stubbed_tool_calls,
        turn_cap_reached,
        recorded_verdict: recorded_verdict.map(|verdict| scrub_verdict(verdict, scrub)),
        candidate_verdict: candidate_verdict.map(|verdict| scrub_verdict(verdict, scrub)),
        scorer_usage,
        candidate_usage,
        live: None,
    }
}

/// `verdict` with each identity of `scrub` replaced in its reject reason.
/// A judge reason can name a model.
fn scrub_verdict(verdict: score::Verdict, scrub: &Scrub) -> score::Verdict {
    match verdict {
        // Redact the whole reason first, then cut it. A cut then never
        // splits a name, and a longer reason still fits the cap.
        score::Verdict::Reject(reason) => {
            score::Verdict::reject(redact(&reason, &scrub.identities))
        }
        verdict @ score::Verdict::Score(_) => verdict,
    }
}

/// `end` with each identity of `scrub` replaced in its error texts.
fn scrub_end(end: RunEnd, scrub: &Scrub) -> RunEnd {
    match end {
        RunEnd::Completed(mut report) => {
            report.error = report.error.map(|error| redact(&error, &scrub.identities));
            if scrub.clear_run_ids {
                report.run_id = None;
                for delegation in &mut report.delegations {
                    delegation.run_id = None;
                    // A key holds the run id of its sender (issue #2484).
                    delegation.idempotency_key = None;
                }
            }
            RunEnd::Completed(report)
        }
        RunEnd::Failed(error) => RunEnd::Failed(redact(&error, &scrub.identities)),
    }
}

/// `text` with each whole occurrence of each of `names` replaced by
/// [`REDACTED`].
fn redact(text: &str, names: &[String]) -> String {
    names
        .iter()
        .fold(text.to_owned(), |text, name| redact_one(&text, name))
}

/// `text` with each whole occurrence of `name` replaced by [`REDACTED`].
fn redact_one(text: &str, name: &str) -> String {
    let is_name_char = |c: char| c.is_alphanumeric() || c == '-' || c == '_';
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for (at, _) in text.match_indices(name) {
        let before = text[..at].chars().next_back();
        let after = text[at + name.len()..].chars().next();
        if before.is_some_and(is_name_char) || after.is_some_and(is_name_char) {
            continue;
        }
        out.push_str(&text[from..at]);
        out.push_str(REDACTED);
        from = at + name.len();
    }
    out.push_str(&text[from..]);
    out
}

/// The pool rank and the id of each candidate digest, by input index.
///
/// # Errors
///
/// Returns [`EvalError::DuplicateCandidate`] for the first pair of equal
/// digests, in input order. Equal digests would get one id.
fn pool_slots(
    seed: u64,
    source: &str,
    digests: &[String],
) -> Result<Vec<(usize, AnonId)>, EvalError> {
    for (first, a) in digests.iter().enumerate() {
        if let Some(second) = (first + 1..digests.len()).find(|&at| digests[at] == *a) {
            return Err(EvalError::DuplicateCandidate { first, second });
        }
    }
    let keys: Vec<CandidateKey<'_>> = digests
        .iter()
        .map(|digest| CandidateKey {
            digest,
            occurrence: 0,
        })
        .collect();
    let ids = anon::assign(seed, source, &keys);
    let mut order: Vec<usize> = (0..ids.len()).collect();
    order.sort_by(|a, b| ids[*a].cmp(&ids[*b]));
    let mut ranks = vec![0; ids.len()];
    for (rank, index) in order.into_iter().enumerate() {
        ranks[index] = rank;
    }
    Ok(ranks.into_iter().zip(ids).collect())
}

impl AnonymisedPool {
    /// The pool as text for a model to read.
    ///
    /// The text lists the candidates in pool order under their ids. It shows
    /// each turn decision as JSON. It holds no input index and no identity.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::from(
            "# Anonymised comparison\n\n\
             Each candidate below ran one recorded source run again. \
             The ids are opaque, and the order is random.\n\n",
        );
        // A write to a `String` cannot fail.
        let _ = writeln!(out, "Source end: {}", end_text(&self.recorded));
        for entry in &self.candidates {
            let _ = render_entry(&mut out, entry);
        }
        out
    }
}

fn render_entry(out: &mut String, entry: &AnonymisedEvaluation) -> fmt::Result {
    let first = entry
        .first_divergence
        .map_or_else(|| "none".to_owned(), |turn| format!("turn {turn}"));
    writeln!(out, "\n## {}\n", entry.id)?;
    writeln!(out, "- First divergence: {first}")?;
    writeln!(out, "- End diverged: {}", yes_no(entry.end_diverged))?;
    writeln!(
        out,
        "- Turn cap reached: {}",
        yes_no(entry.turn_cap_reached)
    )?;
    writeln!(out, "- Tool calls replayed: {}", entry.replayed_tool_calls)?;
    writeln!(out, "- Tool calls stubbed: {}", entry.stubbed_tool_calls)?;
    writeln!(out, "- End: {}", end_text(&entry.end))?;
    if let Some(live) = &entry.live {
        writeln!(out, "- Live re-probe: {}", live_text(live))?;
    }
    if let Some(verdict) = &entry.recorded_verdict {
        writeln!(out, "- Source verdict: {}", score_text(verdict))?;
    }
    if let Some(verdict) = &entry.candidate_verdict {
        writeln!(out, "- Candidate verdict: {}", score_text(verdict))?;
    }
    for turn in &entry.turns {
        writeln!(out, "- Turn {}: {}", turn.turn, verdict_text(turn.verdict))?;
        writeln!(out, "  - Source: {}", decision_text(turn.recorded.as_ref()))?;
        writeln!(
            out,
            "  - Candidate: {}",
            decision_text(turn.candidate.as_ref())
        )?;
    }
    Ok(())
}

/// One live re-probe in words. The text writes an error as a JSON string, so
/// a newline in it cannot break the layout.
fn live_text(live: &LiveCheck) -> String {
    match live {
        LiveCheck::NotNeeded => "not needed".to_owned(),
        LiveCheck::Skipped => "skipped, no fork".to_owned(),
        LiveCheck::NoEnd => "no end, the fork or its wait failed".to_owned(),
        LiveCheck::Completed => "completed".to_owned(),
        LiveCheck::Failed { error: None } => "failed".to_owned(),
        LiveCheck::Failed { error: Some(error) } => format!("failed, error {}", json(error)),
    }
}

const fn yes_no(flag: bool) -> &'static str {
    if flag { "yes" } else { "no" }
}

fn verdict_text(verdict: Verdict) -> String {
    let divergence = match verdict {
        Verdict::Same => return "same".to_owned(),
        Verdict::Reworded => return "reworded".to_owned(),
        Verdict::Diverged(divergence) => divergence,
    };
    let kind = match divergence {
        Divergence::Shape => "shape",
        Divergence::Calls => "calls",
        Divergence::Policy => "policy",
        Divergence::Stop => "stop",
        Divergence::Missing(Side::Recorded) => "the source has no such turn",
        Divergence::Missing(Side::Candidate) => "the candidate has no such turn",
    };
    format!("diverged ({kind})")
}

/// One scorer verdict in words. A reason is JSON, so a newline in it cannot
/// break the layout.
fn score_text(verdict: &score::Verdict) -> String {
    match verdict {
        score::Verdict::Score(value) => format!("score {value}"),
        score::Verdict::Reject(reason) => format!("reject, reason {}", json(reason)),
    }
}

/// One turn decision as compact JSON, or `none`.
fn decision_text(decision: Option<&TurnDecision>) -> String {
    decision.map_or_else(|| "none".to_owned(), json)
}

/// One run end on one line. Each string is JSON, so a newline in it cannot
/// break the layout.
fn end_text(end: &RunEnd) -> String {
    match end {
        RunEnd::Completed(report) => {
            let mut text = format!(
                "completed, stop {}, {} steps, {} tool calls, text {}",
                json(&report.stop),
                report.steps_used,
                report.tool_calls,
                json(&report.text),
            );
            if let Some(error) = &report.error {
                text.push_str(", error ");
                text.push_str(&json(error));
            }
            text
        }
        RunEnd::Failed(error) => format!("failed, error {}", json(error)),
    }
}

/// `value` as compact JSON. A report value always encodes.
fn json<T: Serialize + ?Sized>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn digests(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    /// The input indices in pool order.
    fn order(slots: &[(usize, AnonId)]) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..slots.len()).collect();
        indices.sort_by_key(|index| slots[*index].0);
        indices
    }

    #[test]
    fn two_seeds_order_a_pair_apart() {
        let pair = digests(&["sha256:aa", "sha256:bb"]);
        // Seed 0 orders `aa` first and seed 1 orders `bb` first.
        let one = pool_slots(0, "exec-1", &pair).unwrap();
        let two = pool_slots(1, "exec-1", &pair).unwrap();
        assert_ne!(order(&one), order(&two), "{one:?} {two:?}");
        assert_ne!(one[0].1, two[0].1, "the seed changes the id");
    }

    #[test]
    fn a_seed_gives_the_same_pool_again() {
        let pool = digests(&["x", "y", "z"]);
        assert_eq!(
            pool_slots(9, "exec-1", &pool).unwrap(),
            pool_slots(9, "exec-1", &pool).unwrap()
        );
        assert_ne!(
            pool_slots(9, "exec-1", &pool).unwrap(),
            pool_slots(9, "exec-2", &pool).unwrap(),
            "the source run id enters the key"
        );
    }

    #[test]
    fn an_id_is_c_and_the_least_prefix() {
        let slots = pool_slots(0, "exec-1", &digests(&["x", "y"])).unwrap();
        for (_, id) in &slots {
            let digits = id.as_str().strip_prefix("c-").unwrap();
            assert_eq!(digits.len(), anon::MIN_ANON_HEX, "{id}");
            assert!(digits.bytes().all(|b| b.is_ascii_hexdigit()), "{id}");
        }
    }

    #[test]
    fn equal_digests_are_refused() {
        let result = pool_slots(0, "exec-1", &digests(&["x", "y", "x"]));
        assert!(
            matches!(
                result,
                Err(EvalError::DuplicateCandidate {
                    first: 0,
                    second: 2
                })
            ),
            "{result:?}"
        );
        assert_eq!(pool_slots(0, "exec-1", &[]).unwrap(), Vec::new());
    }

    #[test]
    fn only_a_whole_name_is_redacted() {
        let names = vec!["gpt-x-mini".to_owned(), "gpt-x".to_owned()];
        assert_eq!(
            redact("no model \"gpt-x\"; gpt-x-mini failed; gpt-x2 ok", &names),
            "no model \"[redacted]\"; [redacted] failed; gpt-x2 ok"
        );
        assert_eq!(
            redact("a plan for a", &["a".to_owned()]),
            "[redacted] plan for [redacted]"
        );
        assert_eq!(
            redact("abc", &[String::new()]),
            "abc",
            "an empty name changes nothing"
        );
        assert_eq!(redact("é-x é", &["é".to_owned()]), "é-x [redacted]");
    }

    #[test]
    fn each_verdict_has_words() {
        assert_eq!(verdict_text(Verdict::Same), "same");
        assert_eq!(verdict_text(Verdict::Reworded), "reworded");
        assert_eq!(
            verdict_text(Verdict::Diverged(Divergence::Missing(Side::Candidate))),
            "diverged (the candidate has no such turn)"
        );
        assert_eq!(
            end_text(&RunEnd::Failed("a\nb".into())),
            "failed, error \"a\\nb\""
        );
    }

    // Issue #2484: a key holds the run id of its sender, so the scrub
    // clears it with the run ids.
    #[test]
    fn a_scrub_that_clears_run_ids_clears_each_key() {
        let mut report: crate::AgentReport = serde_json::from_value(serde_json::json!({
            "stop": "completed",
            "text": "",
            "steps_used": 0,
            "tool_calls": 0,
            "usage": crate::message::TokenUsage::default(),
            "messages": [],
            "run_id": "e1",
            "delegations": [{
                "chain": ["a", "b"],
                "run_id": "agent:b:1",
                "refusal": "reply_timeout",
                "idempotency_key": "e1:1",
            }],
        }))
        .expect("a report");
        let scrub = |clear_run_ids| Scrub {
            identities: Vec::new(),
            clear_run_ids,
        };
        let kept = scrub_end(RunEnd::Completed(report.clone()), &scrub(false));
        let RunEnd::Completed(kept) = kept else {
            panic!("the end stays a report");
        };
        assert_eq!(kept.delegations[0].idempotency_key.as_deref(), Some("e1:1"));
        report = match scrub_end(RunEnd::Completed(report), &scrub(true)) {
            RunEnd::Completed(report) => report,
            RunEnd::Failed(error) => panic!("{error}"),
        };
        assert_eq!(report.delegations[0].run_id, None);
        assert_eq!(report.delegations[0].idempotency_key, None);
    }

    /// A probe that failed with `error`.
    fn failed_probe(error: &str) -> Evaluation {
        serde_json::from_value(serde_json::json!({
            "turns": [],
            "first_divergence": null,
            "recorded": {"end": "failed", "detail": "x"},
            "candidate": {"end": "failed", "detail": error},
            "end_diverged": false,
            "replayed_tool_calls": 0,
            "stubbed_tool_calls": 1,
            "turn_cap_reached": false,
            "overlay_digest": "sha256:d1",
        }))
        .expect("an evaluation")
    }

    fn id(text: &str) -> AnonId {
        serde_json::from_value(serde_json::json!(text)).expect("an id")
    }

    #[test]
    fn a_live_end_is_redacted_and_shown_on_one_line() {
        let pool = recorded_pool(
            RunEnd::Failed("x".into()),
            vec![
                (
                    id("c-000000000002"),
                    failed_probe("e"),
                    Some(LiveCheck::Failed {
                        error: Some("m-1 refused\nthe call".into()),
                    }),
                ),
                (
                    id("c-000000000001"),
                    failed_probe("e"),
                    Some(LiveCheck::NoEnd),
                ),
                (id("c-000000000003"), failed_probe("e"), None),
                (
                    id("c-000000000004"),
                    failed_probe("e"),
                    Some(LiveCheck::Failed {
                        error: Some(format!("a call to m-1 and to m-{TRUNCATED}")),
                    }),
                ),
            ],
            vec!["m-1".to_owned()],
        );
        let lives: Vec<Option<LiveCheck>> = pool
            .candidates
            .iter()
            .map(|entry| entry.live.clone())
            .collect();
        assert_eq!(
            lives,
            [
                Some(LiveCheck::NoEnd),
                Some(LiveCheck::Failed {
                    error: Some("[redacted] refused\nthe call".into()),
                }),
                None,
                Some(LiveCheck::Failed {
                    error: Some(format!("a call to [redacted] and to {TRUNCATED}")),
                }),
            ]
        );
        let text = pool.render();
        assert!(
            text.contains("- Live re-probe: no end, the fork or its wait failed\n"),
            "{text}"
        );
        assert!(
            text.contains("- Live re-probe: failed, error \"[redacted] refused\\nthe call\"\n"),
            "{text}"
        );
        assert!(!text.contains("m-1"), "{text}");
        assert_eq!(
            text.matches("Live re-probe").count(),
            3,
            "an entry with no live end has no line"
        );
    }

    #[test]
    fn each_live_end_has_words() {
        assert_eq!(live_text(&LiveCheck::NotNeeded), "not needed");
        assert_eq!(live_text(&LiveCheck::Skipped), "skipped, no fork");
        assert_eq!(live_text(&LiveCheck::Completed), "completed");
        assert_eq!(uncut("a b"), "a b");
        assert_eq!(
            uncut(&format!("a sha256:ab{TRUNCATED}")),
            format!("a sha256:{TRUNCATED}")
        );
        assert_eq!(live_text(&LiveCheck::Failed { error: None }), "failed");
        assert!(LiveCheck::Failed { error: None }.failed());
        for other in [
            LiveCheck::NotNeeded,
            LiveCheck::Skipped,
            LiveCheck::Completed,
            LiveCheck::NoEnd,
        ] {
            assert!(!other.failed(), "{other:?}");
        }
    }

    #[test]
    fn an_entry_with_no_live_end_decodes() {
        let pool = recorded_pool(
            RunEnd::Failed("x".into()),
            vec![(id("c-000000000001"), failed_probe("e"), None)],
            Vec::new(),
        );
        let value = serde_json::to_value(&pool.candidates[0]).expect("json");
        assert!(value.get("live").is_none(), "{value}");
        let read: AnonymisedEvaluation = serde_json::from_value(value).expect("decode");
        assert_eq!(read.live, None);
        let live = serde_json::to_value(LiveCheck::Failed { error: None }).expect("json");
        assert_eq!(live, serde_json::json!({"live": "failed"}));
    }

    /// A copy of `digests` in an order that `shuffle` picks.
    fn shuffled(digests: &[String], shuffle: u64) -> Vec<String> {
        let mut out = digests.to_vec();
        let mut state = shuffle;
        for i in (1..out.len()).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let j = usize::try_from(state >> 33).unwrap_or(0) % (i + 1);
            out.swap(i, j);
        }
        out
    }

    fn slots_by_digest(seed: u64, digests: &[String]) -> BTreeMap<String, (usize, AnonId)> {
        let slots = pool_slots(seed, "exec-1", digests).unwrap();
        digests.iter().cloned().zip(slots).collect()
    }

    fn distinct_ids(slots: &BTreeMap<String, (usize, AnonId)>) -> usize {
        slots
            .values()
            .map(|slot| &slot.1)
            .collect::<BTreeSet<_>>()
            .len()
    }

    // `harvest-verify` treats `proptest!` as a macro that declares no type
    // (issue #2545). So this body can name types without a verdict change.
    proptest! {
        /// Any order of the input gives each digest the same id and rank.
        #[test]
        fn the_input_order_changes_no_slot(
            names in proptest::collection::btree_set("[a-z]{1,8}", 0..12),
            seed in any::<u64>(),
            shuffle in any::<u64>(),
        ) {
            let forward = names.into_iter().collect::<Vec<_>>();
            let one = slots_by_digest(seed, &forward);
            let two = slots_by_digest(seed, &shuffled(&forward, shuffle));
            prop_assert_eq!(&one, &two);
            prop_assert_eq!(distinct_ids(&one), forward.len(), "no id repeats");
        }
    }
}
