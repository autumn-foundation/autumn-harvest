//! Anonymised comparison reports (issue #2376).
//!
//! A model that reads a comparison to pick a candidate is biased by labels
//! and by order. [`compare_anonymised`] removes both. It gives each candidate
//! an opaque [`CandidateId`] and lists the candidates in a seeded order. The
//! text holds no model name, no overlay digest and no label. The key that
//! maps each id back to its candidate is a separate field.
//!
//! # The ids
//!
//! The sort key of a candidate is the SHA-256 of the seed, the source run id
//! and [`Candidate::digest`]. The pool sorts by that key. So the input order
//! changes neither the ids nor the text. The id is `c-` and a hex prefix of
//! the key. The prefix has at least [`MIN_ID_DIGITS`] digits. It grows until
//! no two ids are equal. `formal/verus/src/agent_anonymise.rs` proves the
//! width rule.
//!
//! The source run id is the execution id of the source, unless its task
//! names another run id source.
//!
//! # Limits
//!
//! The text renders each model reply as written. A reply can name its own
//! model. Keep the seed from the reader, because the seed and the digests
//! give the ids.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};

use autumn_harvest::event::WorkflowEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::{
    Candidate, Divergence, EvalError, Evaluation, Recording, RunEnd, Side, TurnDecision, TurnDiff,
    Verdict, run,
};
use crate::transcript::hex;

/// The least number of hex digits in an id.
pub const MIN_ID_DIGITS: usize = 12;

/// The number of hex digits in a sort key: the length of a SHA-256 in hex.
const KEY_DIGITS: usize = 64;

/// The domain tag of a sort key. A change of the key layout changes it.
const KEY_DOMAIN: &[u8] = b"autumn-harvest/eval/anonymised/v1\0";

/// The opaque id of one candidate in a pool, such as `c-3f9a1b2c4d5e`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandidateId(String);

impl CandidateId {
    /// The id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CandidateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

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
    pub id: CandidateId,
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
/// one call cannot encode the text with the key.
#[derive(Debug, Clone, PartialEq)]
pub struct AnonymisedReport {
    /// The rendered pool. It equals [`AnonymisedPool::render`].
    pub text: String,
    /// The structured pool.
    pub pool: AnonymisedPool,
    /// Each id and its candidate.
    pub key: BTreeMap<CandidateId, CandidateRef>,
}

impl AnonymisedReport {
    /// The candidate of `id`, or `None` for an id outside the pool.
    #[must_use]
    pub fn resolve(&self, id: &CandidateId) -> Option<&CandidateRef> {
        self.key.get(id)
    }
}

/// Evaluate each of `candidates` against one recorded `history`, then
/// report them under opaque ids in an order seeded by `seed` (issue #2376).
///
/// Each overlay passes [`overlay::apply`](crate::agent::overlay::apply)
/// before the first model call. So do the ids. The candidates then run one
/// after the other, in pool order. An empty slice gives an empty pool.
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
    let mut entries = Vec::with_capacity(jobs.len());
    let mut key = BTreeMap::new();
    for (index, ((candidate, task), (_, id))) in jobs {
        let evaluation = run(&recording, task, candidate).await?;
        key.insert(
            id.clone(),
            CandidateRef {
                index,
                digest: candidate.digest(),
                overlay_digest: evaluation.overlay_digest.clone(),
                label: candidate.label.clone(),
            },
        );
        entries.push(anonymise(evaluation, id));
    }
    let pool = AnonymisedPool {
        recorded: recording.end,
        candidates: entries,
    };
    Ok(AnonymisedReport {
        text: pool.render(),
        pool,
        key,
    })
}

/// `evaluation` under `id`, with every identity field left out.
fn anonymise(evaluation: Evaluation, id: CandidateId) -> AnonymisedEvaluation {
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
        end: candidate,
        end_diverged,
        replayed_tool_calls,
        stubbed_tool_calls,
        turn_cap_reached,
    }
}

/// The pool rank and the id of each candidate digest, by input index.
///
/// # Errors
///
/// Returns [`EvalError::DuplicateCandidate`] for the first pair of equal
/// sort keys, in input order. Only equal digests give equal keys.
fn pool_slots(
    seed: u64,
    source: &str,
    digests: &[String],
) -> Result<Vec<(usize, CandidateId)>, EvalError> {
    let keys: Vec<String> = digests
        .iter()
        .map(|digest| sort_key(seed, source, digest))
        .collect();
    let mut shared = Vec::new();
    for (first, a) in keys.iter().enumerate() {
        for (second, b) in keys.iter().enumerate().skip(first + 1) {
            let count = shared_digits(a.as_bytes(), b.as_bytes());
            if count >= KEY_DIGITS {
                return Err(EvalError::DuplicateCandidate { first, second });
            }
            shared.push(count);
        }
    }
    let width = id_width(&shared, MIN_ID_DIGITS, KEY_DIGITS);
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|a, b| keys[*a].cmp(&keys[*b]));
    let mut ranks = vec![0; keys.len()];
    for (rank, index) in order.into_iter().enumerate() {
        ranks[index] = rank;
    }
    Ok(keys
        .iter()
        .zip(ranks)
        .map(|(key, rank)| {
            let prefix = key.get(..width).unwrap_or(key);
            (rank, CandidateId(format!("c-{prefix}")))
        })
        .collect())
}

/// The sort key of the candidate `digest`, as 64 hex digits.
///
/// Each part has a length prefix, so two part lists never give one input.
fn sort_key(seed: u64, source: &str, digest: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(KEY_DOMAIN);
    hasher.update(seed.to_be_bytes());
    for part in [source, digest] {
        let len = u64::try_from(part.len()).unwrap_or(u64::MAX);
        hasher.update(len.to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hex(&hasher.finalize())
}

/// The number of leading digits that `a` and `b` share.
///
/// `formal/verus/src/agent_anonymise.rs` proves it.
const fn shared_digits(a: &[u8], b: &[u8]) -> usize {
    let mut i = 0;
    while i < a.len() && i < b.len() && a[i] == b[i] {
        i += 1;
    }
    i
}

/// The id width: the least width from `floor` that is above each shared
/// count below `cap`.
///
/// `formal/verus/src/agent_anonymise.rs` proves that no smaller width from
/// the floor is above each count. Two prefixes of this width then differ.
const fn id_width(shared: &[usize], floor: usize, cap: usize) -> usize {
    let mut width = floor;
    let mut i = 0;
    while i < shared.len() {
        if shared[i] >= width && shared[i] < cap {
            width = shared[i] + 1;
        }
        i += 1;
    }
    width
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
    fn order(slots: &[(usize, CandidateId)]) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..slots.len()).collect();
        indices.sort_by_key(|index| slots[*index].0);
        indices
    }

    #[test]
    fn two_seeds_order_a_pair_apart() {
        let pair = digests(&["sha256:aa", "sha256:bb"]);
        let one = pool_slots(1, "exec-1", &pair).unwrap();
        let two = pool_slots(2, "exec-1", &pair).unwrap();
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
            assert_eq!(digits.len(), MIN_ID_DIGITS, "{id}");
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
    fn a_shared_prefix_widens_every_id() {
        assert_eq!(id_width(&[], 12, 64), 12);
        assert_eq!(id_width(&[3, 11], 12, 64), 12);
        assert_eq!(id_width(&[12], 12, 64), 13);
        assert_eq!(id_width(&[3, 20, 13], 12, 64), 21);
        assert_eq!(id_width(&[64], 12, 64), 12, "an equal pair is not fixable");
    }

    #[test]
    fn shared_digits_counts_the_common_prefix() {
        assert_eq!(shared_digits(b"abc", b"abd"), 2);
        assert_eq!(shared_digits(b"ab", b"ab"), 2);
        assert_eq!(shared_digits(b"", b"a"), 0);
        assert_eq!(shared_digits(b"x", b"y"), 0);
    }

    #[test]
    fn a_key_parts_list_gives_one_input_only() {
        assert_ne!(sort_key(0, "ab", "c"), sort_key(0, "a", "bc"));
        assert_eq!(sort_key(0, "a", "b").len(), KEY_DIGITS);
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

    proptest! {
        /// Any order of the input gives each digest the same id and rank.
        #[test]
        fn the_input_order_changes_no_slot(
            names in proptest::collection::btree_set("[a-z]{1,8}", 0..12),
            seed in any::<u64>(),
            shuffle in any::<u64>(),
        ) {
            let forward: Vec<String> = names.into_iter().collect();
            let mut shuffled = forward.clone();
            let mut state = shuffle;
            for i in (1..shuffled.len()).rev() {
                state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let j = usize::try_from(state >> 33).unwrap_or(0) % (i + 1);
                shuffled.swap(i, j);
            }
            let by_digest = |digests: &[String]| -> BTreeMap<String, (usize, CandidateId)> {
                let slots = pool_slots(seed, "exec-1", digests).unwrap();
                digests.iter().cloned().zip(slots).collect()
            };
            let one = by_digest(&forward);
            let two = by_digest(&shuffled);
            prop_assert_eq!(&one, &two);
            let ids: std::collections::BTreeSet<_> = one.values().map(|slot| &slot.1).collect();
            prop_assert_eq!(ids.len(), forward.len(), "no id repeats");
        }
    }
}
