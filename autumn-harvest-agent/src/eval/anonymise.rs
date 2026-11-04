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
//! same treatment. The routes include each route that a run asked for, such
//! as the model of a recorded peer. An identity is a label,
//! a digest, a route or a model id. Only a whole name is replaced. A name
//! ends at a character that is not a letter, a digit, `-` or `_`. So a
//! short route, such as `a`, also replaces that word.
//!
//! # Limits
//!
//! The text renders each model reply as written. A reply can name its own
//! model. Token usage stays in the pool, and the tokenizer can show a model
//! family. A harness setting, such as a policy timeout, can show in a policy
//! reason. Keep the seed from the reader, because the seed and the digests
//! give the ids.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};

use autumn_harvest::event::WorkflowEvent;
use serde::{Deserialize, Serialize};

use super::anon::{self, AnonId, CandidateKey};
use super::{
    Candidate, Divergence, EvalError, Evaluation, Recording, RunEnd, Side, TurnDecision, TurnDiff,
    Verdict, run_with_routes,
};

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
/// It keeps every field of `Evaluation` except `overlay_digest`. The
/// recorded end is the same for each candidate, so
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
/// duplicate first. The candidates then run one after the other, in pool
/// order. An empty slice gives an empty pool.
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
    let mut runs = Vec::with_capacity(jobs.len());
    let mut asked = BTreeSet::new();
    for (index, ((candidate, task), (_, id))) in jobs {
        let (evaluation, routes) = run_with_routes(&recording, task, candidate).await?;
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

/// Append the section of `entry` to `out`.
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

const fn yes_no(flag: bool) -> &'static str {
    if flag { "yes" } else { "no" }
}

/// One verdict in words.
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

    /// The pool slot of each digest, by digest.
    fn slots_by_digest(seed: u64, digests: &[String]) -> BTreeMap<String, (usize, AnonId)> {
        let slots = pool_slots(seed, "exec-1", digests).unwrap();
        digests.iter().cloned().zip(slots).collect()
    }

    /// The number of distinct ids in `slots`.
    fn distinct_ids(slots: &BTreeMap<String, (usize, AnonId)>) -> usize {
        slots
            .values()
            .map(|slot| &slot.1)
            .collect::<BTreeSet<_>>()
            .len()
    }

    // `harvest-verify` reads each bare type name in the tokens of a macro
    // with no local definition as a type that can write through. So the
    // body below names no type. The helpers above hold the types. Issue
    // #2545 tracks the fix in the analyzer.
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
