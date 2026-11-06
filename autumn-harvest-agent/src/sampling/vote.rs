//! The vote strategy of `agent_sample`: self-consistency by answer key.
//!
//! An [`AnswerKey`] maps each final answer to a key text. The key with the
//! most votes wins. A tie goes to the key whose first vote has the lowest
//! draw id. A draw whose answer has no key casts no vote.
//!
//! [`Tally`] counts the votes. Its integer kernel is `key_votes`,
//! `pick_key`, `vote_winner`, `runner_up` and [`vote_settled`].
//! `formal/verus/src/agent_vote.rs` proves it. When the leader cannot
//! lose, the run starts no more draws, and the winner is still the winner
//! of the full run. The order in which draws end does not change the
//! result.
//!
//! An app key function implements [`AnswerKeyFn`]. The recorded
//! [`agent_vote_key`] activity calls it, so replay reads each app key.

use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::draw::DrawId;
use crate::harness::AgentHarness;
use crate::mesh::{MAX_ITEM_LEN, valid_name};

/// The most bytes of one key. A longer key casts no vote.
pub const MAX_KEY_BYTES: usize = 1024;

/// How a vote maps a final answer to its key.
///
/// It encodes as `"trimmed"`, `"last_number"`, `{"json_field": "/a"}` or
/// `{"custom": "id"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AnswerKey {
    /// The answer with no white space at each end.
    Trimmed,
    /// The last decimal number of the answer, in canonical form. The form
    /// has no thousands commas, no leading zeros, no trailing fraction
    /// zeros and no `-0`. So `1,200.50` gives `1200.5`.
    LastNumber,
    /// The field at this JSON pointer (RFC 6901) of the answer. The
    /// trimmed answer must be JSON. A string field gives its text. A
    /// number, a bool, an array or an object gives its JSON text. A
    /// `null` field gives no key.
    JsonField(String),
    /// The app key function with this id. See [`AnswerKeyFn`].
    Custom(String),
}

impl AnswerKey {
    /// The field at the JSON pointer `pointer`.
    #[must_use]
    pub fn json_field(pointer: impl Into<String>) -> Self {
        Self::JsonField(pointer.into())
    }

    /// The app key function with the id `id`.
    #[must_use]
    pub fn custom(id: impl Into<String>) -> Self {
        Self::Custom(id.into())
    }

    /// Whether the key is valid. A JSON pointer is empty or starts with
    /// `/`, and has at most 128 bytes and no control character. A custom
    /// id is an agent name.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Trimmed | Self::LastNumber => true,
            Self::JsonField(pointer) => {
                (pointer.is_empty() || pointer.starts_with('/'))
                    && pointer.len() <= MAX_ITEM_LEN
                    && !pointer.chars().any(char::is_control)
            }
            Self::Custom(id) => valid_name(id),
        }
    }

    /// The key of `answer` under a built-in key.
    ///
    /// Returns `None` for no key, for a key over [`MAX_KEY_BYTES`], and for
    /// a [`Custom`](Self::Custom) key, which only the harness can apply.
    #[must_use]
    pub fn key_of(&self, answer: &str) -> Option<String> {
        let key = match self {
            Self::Trimmed => Some(answer.trim().to_owned()),
            Self::LastNumber => last_number(answer),
            Self::JsonField(pointer) => json_field(answer, pointer),
            Self::Custom(_) => None,
        };
        key.and_then(fit_key)
    }
}

/// `key`, or `None` when it is empty or over [`MAX_KEY_BYTES`].
fn fit_key(key: String) -> Option<String> {
    (!key.is_empty() && key.len() <= MAX_KEY_BYTES).then_some(key)
}

/// The last decimal number of `answer`, in canonical form.
///
/// A comma joins two digit groups only when exactly three digits follow
/// it. So `1,000` is one number, and `1,2,3` is three. A `-` right before
/// the digits makes the number negative, unless a letter or a digit comes
/// before the `-`.
fn last_number(answer: &str) -> Option<String> {
    let bytes = answer.as_bytes();
    let digit = |i: usize| bytes.get(i).is_some_and(u8::is_ascii_digit);
    let mut last = None;
    let mut i = 0;
    while i < bytes.len() {
        if !digit(i) {
            i += 1;
            continue;
        }
        let start = i;
        let mut whole = String::new();
        loop {
            while digit(i) {
                whole.push(char::from(bytes[i]));
                i += 1;
            }
            let group = bytes.get(i) == Some(&b',')
                && digit(i + 1)
                && digit(i + 2)
                && digit(i + 3)
                && !digit(i + 4);
            if !group {
                break;
            }
            i += 1;
        }
        let mut fraction = String::new();
        if bytes.get(i) == Some(&b'.') && digit(i + 1) {
            i += 1;
            while digit(i) {
                fraction.push(char::from(bytes[i]));
                i += 1;
            }
        }
        let negative = start > 0
            && bytes[start - 1] == b'-'
            && (start < 2 || !bytes[start - 2].is_ascii_alphanumeric());
        last = Some((negative, whole, fraction));
    }
    let (negative, whole, fraction) = last?;
    let whole = whole.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    let mut text = String::new();
    if negative && !(whole.is_empty() && fraction.is_empty()) {
        text.push('-');
    }
    text.push_str(if whole.is_empty() { "0" } else { whole });
    if !fraction.is_empty() {
        text.push('.');
        text.push_str(fraction);
    }
    Some(text)
}

/// The field at `pointer` of the JSON answer `answer`.
fn json_field(answer: &str, pointer: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(answer.trim()).ok()?;
    match value.pointer(pointer)? {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// An app function that maps a final answer to its key.
///
/// The [`agent_vote_key`] activity calls it, and history records each key.
/// Make it a pure function of the answer. Return `None` when the answer
/// has no key. A key over [`MAX_KEY_BYTES`] casts no vote.
pub trait AnswerKeyFn: Send + Sync + std::fmt::Debug {
    /// The id that [`AnswerKey::Custom`] names. It is an agent name.
    fn id(&self) -> &str;

    /// The key of `answer`, or `None` for no key.
    fn key(&self, answer: &str) -> Option<String>;
}

/// The input of [`agent_vote_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct VoteKeyRequest {
    /// The id of the app key function.
    pub key: String,
    /// The answer to key. `None` only checks that the harness has the
    /// function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,
}

impl VoteKeyRequest {
    /// A request for the key of `answer` under the function `key`.
    #[must_use]
    pub fn new(key: impl Into<String>, answer: Option<String>) -> Self {
        Self {
            key: key.into(),
            answer,
        }
    }
}

/// The result of [`agent_vote_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct VoteKeyReply {
    /// Whether the harness has the function.
    pub known: bool,
    /// The key of the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Apply the app key function of `request` with `harness`.
#[must_use]
pub fn run_vote_key(harness: &AgentHarness, request: &VoteKeyRequest) -> VoteKeyReply {
    let Some(function) = harness.answer_key_of(&request.key) else {
        return VoteKeyReply {
            known: false,
            key: None,
        };
    };
    let key = request
        .answer
        .as_deref()
        .and_then(|answer| function.key(answer))
        .and_then(fit_key);
    VoteKeyReply { known: true, key }
}

/// Key one answer with an app key function, as an activity.
///
/// # Errors
///
/// Returns an error when no harness is installed.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_vote_key(
    ctx: &ActivityContext,
    request: VoteKeyRequest,
) -> Result<VoteKeyReply, String> {
    Ok(run_vote_key(crate::workflow::harness(ctx)?, &request))
}

/// The votes of one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct VoteCount {
    /// The key.
    pub key: String,
    /// The votes.
    pub votes: u32,
    /// The lowest draw id that voted for the key.
    pub first: DrawId,
}

impl VoteCount {
    /// The count of `key`.
    #[must_use]
    pub fn new(key: impl Into<String>, votes: u32, first: DrawId) -> Self {
        Self {
            key: key.into(),
            votes,
            first,
        }
    }
}

/// The votes of one run.
///
/// Each draw casts at most one vote. A second vote of the same draw is
/// ignored. Each key is held as the index of its first appearance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tally {
    keys: Vec<String>,
    votes: Vec<(u32, Option<u32>)>,
}

impl Tally {
    /// A tally with no vote.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            keys: Vec::new(),
            votes: Vec::new(),
        }
    }

    /// Record the vote of `draw`. A `key` of `None` casts no vote.
    pub fn cast(&mut self, draw: DrawId, key: Option<&str>) {
        if self.votes.iter().any(|(seen, _)| *seen == draw.get()) {
            return;
        }
        let index = key.map(|key| {
            let found = self.keys.iter().position(|seen| seen == key);
            let at = found.unwrap_or_else(|| {
                self.keys.push(key.to_owned());
                self.keys.len() - 1
            });
            u32::try_from(at).unwrap_or(u32::MAX)
        });
        self.votes.push((draw.get(), index));
    }

    fn key_count(&self) -> u32 {
        u32::try_from(self.keys.len()).unwrap_or(u32::MAX)
    }

    fn count_of(&self, key: u32, votes: u32, first: u32) -> VoteCount {
        let text = self.keys.get(key as usize).cloned().unwrap_or_default();
        VoteCount::new(text, votes, DrawId::new(first))
    }

    /// The winning key: the most votes, then the lowest first draw.
    /// `None` when no draw cast a vote.
    #[must_use]
    pub fn winner(&self) -> Option<VoteCount> {
        vote_winner(&self.votes, self.key_count())
            .map(|(key, votes, first)| self.count_of(key, votes, first))
    }

    /// Whether the leader keeps the lead after `left` more votes, whatever
    /// they are. Then the run needs no more draws.
    #[must_use]
    pub fn settled(&self, left: u32) -> bool {
        let keys = self.key_count();
        vote_winner(&self.votes, keys).is_some_and(|(key, lead, _first)| {
            vote_settled(lead, runner_up(&self.votes, keys, key), left)
        })
    }

    /// The count of each key that has a vote, the winner first.
    #[must_use]
    pub fn counts(&self) -> Vec<VoteCount> {
        let mut counts: Vec<(u32, u32, u32)> = (0..self.key_count())
            .map(|key| {
                let (votes, first) = key_votes(&self.votes, key);
                (key, votes, first)
            })
            .collect();
        counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.2.cmp(&b.2)));
        counts
            .into_iter()
            .map(|(key, votes, first)| self.count_of(key, votes, first))
            .collect()
    }
}

/// The votes and the first draw of the key `key`.
///
/// The first draw is `u32::MAX` when no draw votes for the key.
/// `formal/verus/src/agent_vote.rs` proves it.
const fn key_votes(votes: &[(u32, Option<u32>)], key: u32) -> (u32, u32) {
    let mut count = 0;
    let mut first = u32::MAX;
    let mut i = 0;
    while i < votes.len() {
        if let Some(k) = votes[i].1 {
            if k == key {
                count += 1;
                if votes[i].0 < first {
                    first = votes[i].0;
                }
            }
        }
        i += 1;
    }
    (count, first)
}

/// The better of `best` and the entry `next`.
///
/// An entry is a key, its votes and its first draw. An entry with no vote
/// never wins. More votes win, then the lower first draw.
/// `formal/verus/src/agent_vote.rs` proves it.
const fn pick_key(best: Option<(u32, u32, u32)>, next: (u32, u32, u32)) -> Option<(u32, u32, u32)> {
    if next.1 == 0 {
        return best;
    }
    match best {
        None => Some(next),
        Some(b) => {
            if next.1 > b.1 || (next.1 == b.1 && next.2 < b.2) {
                Some(next)
            } else {
                best
            }
        }
    }
}

/// The winner among the keys `0..keys`, as a key, its votes and its first
/// draw.
///
/// `formal/verus/src/agent_vote.rs` proves three properties. The winner
/// has the most votes, and the lowest first draw on a tie. An early stop
/// keeps it. The order of `votes` does not change it.
const fn vote_winner(votes: &[(u32, Option<u32>)], keys: u32) -> Option<(u32, u32, u32)> {
    let mut best = None;
    let mut key = 0;
    while key < keys {
        let (count, first) = key_votes(votes, key);
        best = pick_key(best, (key, count, first));
        key += 1;
    }
    best
}

/// The most votes of a key below `keys` other than `winner`. 0 when there
/// is none.
/// `formal/verus/src/agent_vote.rs` proves it.
const fn runner_up(votes: &[(u32, Option<u32>)], keys: u32, winner: u32) -> u32 {
    let mut second = 0;
    let mut key = 0;
    while key < keys {
        if key != winner {
            let (count, _first) = key_votes(votes, key);
            if count > second {
                second = count;
            }
        }
        key += 1;
    }
    second
}

/// Whether a leader with `lead` votes keeps the lead over a runner-up with
/// `second` votes after `left` more votes: `lead > second + left`.
///
/// `formal/verus/src/agent_vote.rs` proves it with no overflow.
#[must_use]
pub const fn vote_settled(lead: u32, second: u32, left: u32) -> bool {
    lead > second && lead - second > left
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::{Just, Strategy, prop, prop_assert, prop_assert_eq, proptest};

    #[test]
    fn last_number_gives_the_canonical_form() {
        let cases = [
            ("The answer is 42.", Some("42")),
            ("about 7", Some("7")),
            ("7.0 apples", Some("7")),
            ("1,200.50 dollars", Some("1200.5")),
            ("from 3 to 0042", Some("42")),
            ("x = -5", Some("-5")),
            ("-0.00", Some("0")),
            ("step-3", Some("3")),
            ("1,2,3", Some("3")),
            ("1,0000", Some("0")),
            ("0.5", Some("0.5")),
            ("no idea", None),
            ("", None),
        ];
        for (answer, want) in cases {
            assert_eq!(
                AnswerKey::LastNumber.key_of(answer).as_deref(),
                want,
                "{answer:?}"
            );
        }
    }

    #[test]
    fn json_field_reads_the_pointer() {
        let key = AnswerKey::json_field("/a/b");
        assert_eq!(key.key_of(r#" {"a": {"b": "x"}} "#).as_deref(), Some("x"));
        assert_eq!(key.key_of(r#"{"a": {"b": 4}}"#).as_deref(), Some("4"));
        assert_eq!(key.key_of(r#"{"a": {"b": true}}"#).as_deref(), Some("true"));
        assert_eq!(key.key_of(r#"{"a": {"b": null}}"#), None);
        assert_eq!(key.key_of(r#"{"a": {}}"#), None);
        assert_eq!(key.key_of("not json"), None);
        assert_eq!(key.key_of(r#"{"a": {"b": ""}}"#), None, "an empty key");
    }

    #[test]
    fn trimmed_drops_the_white_space_and_caps_the_key() {
        assert_eq!(
            AnswerKey::Trimmed.key_of("  a b \n").as_deref(),
            Some("a b")
        );
        assert_eq!(AnswerKey::Trimmed.key_of("   "), None);
        let long = "x".repeat(MAX_KEY_BYTES + 1);
        assert_eq!(AnswerKey::Trimmed.key_of(&long), None);
        let fits = "x".repeat(MAX_KEY_BYTES);
        assert_eq!(AnswerKey::Trimmed.key_of(&fits), Some(fits));
        assert_eq!(AnswerKey::custom("f").key_of("a"), None);
    }

    #[test]
    fn a_key_is_valid_only_in_its_form() {
        assert!(AnswerKey::Trimmed.is_valid());
        assert!(AnswerKey::LastNumber.is_valid());
        assert!(AnswerKey::json_field("").is_valid());
        assert!(AnswerKey::json_field("/answer/0").is_valid());
        assert!(!AnswerKey::json_field("answer").is_valid());
        assert!(!AnswerKey::json_field("/\n").is_valid());
        assert!(!AnswerKey::json_field(format!("/{}", "a".repeat(MAX_ITEM_LEN))).is_valid());
        assert!(AnswerKey::custom("lower").is_valid());
        assert!(!AnswerKey::custom("Lower").is_valid());
        assert!(!AnswerKey::custom("").is_valid());
    }

    #[test]
    fn the_settle_bound_is_strict_and_never_overflows() {
        assert!(vote_settled(3, 0, 2));
        assert!(!vote_settled(3, 0, 3));
        assert!(!vote_settled(2, 2, 0));
        assert!(vote_settled(u32::MAX, 0, u32::MAX - 1));
        assert!(!vote_settled(u32::MAX, 0, u32::MAX));
        assert!(!vote_settled(0, 0, 0));
    }

    #[test]
    fn a_tally_ignores_a_second_vote_of_a_draw() {
        let mut tally = Tally::new();
        tally.cast(DrawId::new(0), Some("a"));
        tally.cast(DrawId::new(0), Some("b"));
        tally.cast(DrawId::new(1), None);
        assert_eq!(tally.counts(), [VoteCount::new("a", 1, DrawId::new(0))]);
        assert!(!tally.settled(1));
        assert!(tally.settled(0));
    }

    #[test]
    fn an_empty_tally_has_no_winner() {
        let tally = Tally::new();
        assert_eq!(tally.winner(), None);
        assert!(tally.counts().is_empty());
        assert!(!tally.settled(0));
    }

    #[test]
    fn a_tie_goes_to_the_lowest_first_draw() {
        let mut tally = Tally::new();
        for (draw, key) in [(4, "c"), (1, "b"), (3, "b"), (0, "a"), (2, "a")] {
            tally.cast(DrawId::new(draw), Some(key));
        }
        assert_eq!(tally.winner(), Some(VoteCount::new("a", 2, DrawId::new(0))));
        assert_eq!(
            tally.counts(),
            [
                VoteCount::new("a", 2, DrawId::new(0)),
                VoteCount::new("b", 2, DrawId::new(1)),
                VoteCount::new("c", 1, DrawId::new(4)),
            ]
        );
    }

    /// The winner of `answers` with the early stop, in waves of `width`.
    /// Returns the winner and the draws that ran.
    fn early(answers: &[Option<u8>], width: usize) -> (Option<VoteCount>, usize) {
        let mut tally = Tally::new();
        let mut started = 0;
        while started < answers.len() {
            let end = (started + width).min(answers.len());
            for (draw, answer) in answers.iter().enumerate().take(end).skip(started) {
                let key = answer.map(|a| a.to_string());
                tally.cast(DrawId::new(u32::try_from(draw).unwrap()), key.as_deref());
            }
            started = end;
            let left = u32::try_from(answers.len() - started).unwrap();
            if tally.settled(left) {
                break;
            }
        }
        (tally.winner(), started)
    }

    fn full(votes: &[(usize, Option<u8>)]) -> Tally {
        let mut tally = Tally::new();
        for (draw, answer) in votes {
            let key = answer.map(|a| a.to_string());
            tally.cast(DrawId::new(u32::try_from(*draw).unwrap()), key.as_deref());
        }
        tally
    }

    proptest! {
        #[test]
        fn an_early_stop_never_changes_the_winner(
            answers in prop::collection::vec(prop::option::of(0_u8..4), 1..24),
            width in 1_usize..6,
        ) {
            let all: Vec<(usize, Option<u8>)> = answers.iter().copied().enumerate().collect();
            // The early run may have fewer votes, but the same key and the
            // same selected draw.
            let pick = |count: Option<VoteCount>| count.map(|c| (c.key, c.first));
            let (winner, ran) = early(&answers, width);
            prop_assert_eq!(pick(winner), pick(full(&all).winner()));
            prop_assert!(ran <= answers.len());
        }

        #[test]
        fn the_order_of_the_votes_does_not_change_the_tally(
            (votes, shuffled) in prop::collection::vec(prop::option::of(0_u8..4), 0..24)
                .prop_flat_map(|answers| {
                    let votes: Vec<(usize, Option<u8>)> =
                        answers.into_iter().enumerate().collect();
                    (Just(votes.clone()), Just(votes).prop_shuffle())
                }),
        ) {
            let a = full(&votes);
            let b = full(&shuffled);
            prop_assert_eq!(a.winner(), b.winner());
            prop_assert_eq!(a.counts(), b.counts());
            for left in 0..4 {
                prop_assert_eq!(a.settled(left), b.settled(left));
            }
        }
    }
}
