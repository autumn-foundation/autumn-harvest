//! The vote strategy of `agent_sample`: self-consistency by answer key.
//!
//! An [`AnswerKey`] maps each final answer to a key text. The key with the
//! most votes wins. A tie goes to the key whose first vote has the lowest
//! draw id. A draw whose answer has no key casts no vote.
//!
//! [`Tally`] counts the votes. Its integer kernel is `key_votes`,
//! `pick_key`, `vote_winner`, `runner_up` and [`vote_settled`].
//! `formal/verus/src/agent_vote.rs` proves the kernel. When the leader
//! cannot lose, the run starts no more draws, and the winner is still the
//! winner of the full run. The order in which draws end does not change
//! the result.
//!
//! The recorded [`agent_vote_key`] activity applies each key, built-in or
//! app. So replay reads each key, and a later change to a key rule never
//! changes a recorded run. An app key function implements
//! [`AnswerKeyFn`].

use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::draw::DrawId;
use crate::harness::AgentHarness;
use crate::mesh::{MAX_ITEM_LEN, valid_name};

/// The most bytes of one key as JSON text, quotes excluded. A longer key
/// casts no vote.
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
    /// zeros and no `-0`. So `1,200.50` gives `1200.5`, and `.5` gives
    /// `0.5`. Only ASCII digits count, and an exponent is a number of its
    /// own: `1e5` gives `5`.
    LastNumber,
    /// The field at this JSON pointer (RFC 6901) of the answer. The
    /// trimmed answer must be JSON. A string field gives its trimmed text.
    /// A number gives its exact decimal form, so `1`, `1.0` and `1e0` all
    /// give `1`, and no digit is lost. A bool, an array or an object gives its JSON text. A `null`
    /// field gives no key.
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
    /// `/`. It has at most 128 bytes and no control character, and each
    /// `~` comes before `0` or `1`. A custom id is an agent name.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Trimmed | Self::LastNumber => true,
            Self::JsonField(pointer) => {
                (pointer.is_empty() || pointer.starts_with('/'))
                    && pointer.len() <= MAX_ITEM_LEN
                    && !pointer.chars().any(char::is_control)
                    && pointer
                        .split('~')
                        .skip(1)
                        .all(|rest| rest.starts_with('0') || rest.starts_with('1'))
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

/// `key`, or `None` when it is empty or over [`MAX_KEY_BYTES`] as JSON
/// text. The JSON form bounds the size of the report.
pub fn fit_key(key: String) -> Option<String> {
    let fits =
        serde_json::to_string(&key).is_ok_and(|text| text.len() <= MAX_KEY_BYTES.saturating_add(2));
    (!key.is_empty() && fits).then_some(key)
}

/// The last decimal number of `answer`, in canonical form.
///
/// A comma joins two digit groups only when exactly three digits follow
/// it. So `1,000` is one number, and `1,2,3` is three. A `.` right before
/// the digits, with no digit before it, makes them a fraction. A `-` right
/// before the number makes it negative, unless a letter or a digit comes
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
        let mut sign_at = start;
        if start > 0 && bytes[start - 1] == b'.' && !digit(start.wrapping_sub(2)) {
            fraction = std::mem::take(&mut whole);
            sign_at = start - 1;
        } else if bytes.get(i) == Some(&b'.') && digit(i + 1) {
            i += 1;
            while digit(i) {
                fraction.push(char::from(bytes[i]));
                i += 1;
            }
        }
        // The `-` is ASCII, so the slice before it ends on a char boundary.
        let negative = sign_at > 0
            && bytes[sign_at - 1] == b'-'
            && !answer[..sign_at - 1]
                .chars()
                .next_back()
                .is_some_and(char::is_alphanumeric);
        last = Some((negative, whole, fraction));
    }
    let (negative, whole, fraction) = last?;
    Some(canonical_decimal(negative, &whole, &fraction))
}

/// The canonical text of a decimal: no leading zeros, no trailing fraction
/// zeros and no `-0`.
fn canonical_decimal(negative: bool, whole: &str, fraction: &str) -> String {
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
    text
}

/// The field at `pointer` of the JSON answer `answer`.
///
/// The walk keeps the raw text of each value. So a number keeps each digit
/// of its source, and never passes through `f64`.
fn json_field(answer: &str, pointer: &str) -> Option<String> {
    use serde_json::value::RawValue;

    let mut raw: &RawValue = serde_json::from_str(answer.trim()).ok()?;
    if !pointer.is_empty() {
        for token in pointer.strip_prefix('/')?.split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            raw = match raw.get().as_bytes().first()? {
                b'{' => {
                    let map: std::collections::BTreeMap<String, &RawValue> =
                        serde_json::from_str(raw.get()).ok()?;
                    *map.get(&token)?
                }
                b'[' => {
                    let index = array_index(&token)?;
                    let list: Vec<&RawValue> = serde_json::from_str(raw.get()).ok()?;
                    *list.get(index)?
                }
                _ => return None,
            };
        }
    }
    match serde_json::from_str(raw.get()).ok()? {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => Some(text.trim().to_owned()),
        serde_json::Value::Number(_) => number_text(raw.get()),
        other => Some(other.to_string()),
    }
}

/// The array index of a pointer token: digits with no leading zero.
fn array_index(token: &str) -> Option<usize> {
    let digits = !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit());
    let lead = token.len() > 1 && token.starts_with('0');
    if digits && !lead {
        token.parse().ok()
    } else {
        None
    }
}

/// The decimal form of the JSON number text `text`, with no exponent.
///
/// It works on the digits, so it keeps each one. A form longer than
/// [`MAX_KEY_BYTES`] gives `None`, so a large exponent builds no text.
fn number_text(text: &str) -> Option<String> {
    let (negative, rest) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(at) => (&rest[..at], rest[at + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some("0".to_owned());
    }
    // The point sits `point` digits after the first significant digit.
    let skipped = i64::try_from(format!("{whole}{fraction}").len() - digits.len()).ok()?;
    let point = i64::try_from(whole.len())
        .ok()?
        .checked_add(exponent)?
        .checked_sub(skipped)?;
    let count = i64::try_from(digits.len()).ok()?;
    let limit = i64::try_from(MAX_KEY_BYTES).ok()?;
    if point > limit || point < -limit {
        return None;
    }
    let (whole, fraction) = if point <= 0 {
        let zeros = "0".repeat(usize::try_from(-point).ok()?);
        (String::new(), format!("{zeros}{digits}"))
    } else if point >= count {
        let zeros = "0".repeat(usize::try_from(point - count).ok()?);
        (format!("{digits}{zeros}"), String::new())
    } else {
        let at = usize::try_from(point).ok()?;
        (digits[..at].to_owned(), digits[at..].to_owned())
    };
    Some(canonical_decimal(negative, &whole, &fraction))
}

/// An app function that maps a final answer to its key.
///
/// The [`agent_vote_key`] activity calls it, and history records each key.
/// Make it a fast, pure function of the answer: the activity runs it
/// inline, with no time budget. Return `None` when the answer has no key.
/// A key over [`MAX_KEY_BYTES`] casts no vote.
///
/// The harness is the gate: a vote can name only a function that the app
/// added with `AgentHarness::answer_key`.
pub trait AnswerKeyFn: Send + Sync + std::fmt::Debug {
    /// The id that [`AnswerKey::Custom`] names. It must be an agent name,
    /// or no vote can name it.
    fn id(&self) -> &str;

    /// The key of `answer`, or `None` for no key.
    fn key(&self, answer: &str) -> Option<String>;
}

/// The input of [`agent_vote_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct VoteKeyRequest {
    /// The answer key to apply.
    pub key: AnswerKey,
    /// The answer to key. `None` only checks that the harness has the
    /// function of a custom key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<String>,
}

impl VoteKeyRequest {
    /// A request for the key of `answer` under `key`.
    #[must_use]
    pub const fn new(key: AnswerKey, answer: Option<String>) -> Self {
        Self { key, answer }
    }
}

/// The result of [`agent_vote_key`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct VoteKeyReply {
    /// Whether the harness has the function. Always true for a built-in
    /// key.
    pub known: bool,
    /// The key of the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Apply the key of `request` with `harness`.
///
/// # Errors
///
/// Returns an error when the request holds an answer and the harness has
/// no function for its custom key. A retry can then reach a worker that
/// has it. A probe with no answer gets `known: false` instead.
pub fn run_vote_key(
    harness: &AgentHarness,
    request: &VoteKeyRequest,
) -> Result<VoteKeyReply, String> {
    let AnswerKey::Custom(id) = &request.key else {
        let key = request
            .answer
            .as_deref()
            .and_then(|answer| request.key.key_of(answer));
        return Ok(VoteKeyReply { known: true, key });
    };
    let Some(function) = harness.answer_key_of(id) else {
        if request.answer.is_some() {
            return Err(format!(
                "this worker has no answer key function {:?}",
                crate::harness::truncate(id, crate::mesh::MAX_NAME_LEN)
            ));
        }
        return Ok(VoteKeyReply {
            known: false,
            key: None,
        });
    };
    let key = request
        .answer
        .as_deref()
        .and_then(|answer| function.key(answer))
        .and_then(fit_key);
    Ok(VoteKeyReply { known: true, key })
}

/// Key one answer, as an activity.
///
/// # Errors
///
/// Returns the errors of [`run_vote_key`], and an error when the worker
/// has no harness.
#[activity(
    start_to_close = "1m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_vote_key(
    ctx: &ActivityContext,
    request: VoteKeyRequest,
) -> Result<VoteKeyReply, String> {
    run_vote_key(crate::workflow::harness(ctx)?, &request)
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
/// Each draw casts at most one vote. The tally ignores a second vote of
/// the same draw. It holds each key as the index of its first appearance.
/// The order of the votes does not change the winner, the counts or the
/// early stop.
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
        let hit = if let Some(k) = votes[i].1 {
            k == key
        } else {
            false
        };
        if hit {
            count += 1;
            if votes[i].0 < first {
                first = votes[i].0;
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
///
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
            ("about .5", Some("0.5")),
            ("-.25", Some("-0.25")),
            ("1e5", Some("5")),
            ("é-5", Some("5")),
            ("e -5", Some("-5")),
            ("1,234,567.890", Some("1234567.89")),
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
    fn a_json_number_key_keeps_each_digit() {
        let key = AnswerKey::json_field("/n");
        let cases = [
            (r#"{"n": 9007199254740993}"#, Some("9007199254740993")),
            (r#"{"n": 9007199254740993.0}"#, Some("9007199254740993")),
            (
                r#"{"n": 0.1000000000000000055511}"#,
                Some("0.1000000000000000055511"),
            ),
            (r#"{"n": 0.1}"#, Some("0.1")),
            (r#"{"n": 1.5e3}"#, Some("1500")),
            (r#"{"n": 2.5E-3}"#, Some("0.0025")),
            (r#"{"n": -12.50e+1}"#, Some("-125")),
            (r#"{"n": -0e5}"#, Some("0")),
            (r#"{"n": 1e999999999}"#, None),
            (r#"{"n": 1e-999999999}"#, None),
        ];
        for (answer, want) in cases {
            assert_eq!(key.key_of(answer).as_deref(), want, "{answer}");
        }
    }

    #[test]
    fn a_json_pointer_walks_escapes_and_arrays() {
        let answer = r#"{"a/b": {"m~n": [10, {"x": "y"}]}}"#;
        let at = |pointer: &str| AnswerKey::json_field(pointer).key_of(answer);
        assert_eq!(at("/a~1b/m~0n/0").as_deref(), Some("10"));
        assert_eq!(at("/a~1b/m~0n/1/x").as_deref(), Some("y"));
        assert_eq!(at("/a~1b/m~0n/2"), None);
        assert_eq!(at("/a~1b/m~0n/01"), None, "an index has no leading zero");
        assert_eq!(at("/a~1b/m~0n/+1"), None);
        assert_eq!(at("/missing"), None);
        assert_eq!(
            AnswerKey::json_field("").key_of(" 42 ").as_deref(),
            Some("42")
        );
        assert_eq!(
            AnswerKey::json_field("/a")
                .key_of(r#"{"a": 1, "a": 2}"#)
                .as_deref(),
            Some("2"),
            "the last of two equal keys wins, as in serde_json"
        );
    }

    #[test]
    fn json_field_reads_the_pointer() {
        let key = AnswerKey::json_field("/a/b");
        assert_eq!(key.key_of(r#" {"a": {"b": "x"}} "#).as_deref(), Some("x"));
        assert_eq!(key.key_of(r#"{"a": {"b": 4}}"#).as_deref(), Some("4"));
        for one in [r#"{"a": {"b": 1.0}}"#, r#"{"a": {"b": 1e0}}"#] {
            assert_eq!(key.key_of(one).as_deref(), Some("1"), "{one}");
        }
        assert_eq!(key.key_of(r#"{"a": {"b": -0.0}}"#).as_deref(), Some("0"));
        assert_eq!(key.key_of(r#"{"a": {"b": 2.5}}"#).as_deref(), Some("2.5"));
        assert_eq!(key.key_of(r#"{"a": {"b": " x "}}"#).as_deref(), Some("x"));
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
        let escaped = "\u{1}".repeat(MAX_KEY_BYTES / 2);
        assert_eq!(
            AnswerKey::Trimmed.key_of(&format!("a{escaped}a")),
            None,
            "the cap counts the JSON escapes"
        );
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
        assert!(AnswerKey::json_field("/a~0b~1c").is_valid());
        assert!(!AnswerKey::json_field("/a~2").is_valid());
        assert!(!AnswerKey::json_field("/a~").is_valid());
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
        assert_eq!(tally.counts(), Vec::new());
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

    /// A model that the key tests never call.
    #[derive(Debug)]
    struct NoModel;

    impl crate::model::AgentModel for NoModel {
        fn chat<'a>(
            &'a self,
            _request: &'a crate::ChatRequest,
        ) -> crate::model::BoxFuture<'a, Result<crate::model::ChatResponse, crate::AgentError>>
        {
            Box::pin(async { Err(crate::AgentError::new(crate::ErrorKind::Provider, "unused")) })
        }
    }

    /// A key function that gives the answer twice.
    #[derive(Debug)]
    struct Twice;

    impl AnswerKeyFn for Twice {
        fn id(&self) -> &'static str {
            "twice"
        }

        fn key(&self, answer: &str) -> Option<String> {
            Some(answer.repeat(2))
        }
    }

    #[test]
    fn the_key_activity_applies_each_kind_of_key() {
        let harness =
            AgentHarness::new(std::sync::Arc::new(NoModel)).answer_key(std::sync::Arc::new(Twice));
        let ask = |key: AnswerKey, answer: Option<&str>| {
            run_vote_key(
                &harness,
                &VoteKeyRequest::new(key, answer.map(str::to_owned)),
            )
        };
        let reply = ask(AnswerKey::LastNumber, Some("it is 4")).unwrap();
        assert_eq!((reply.known, reply.key.as_deref()), (true, Some("4")));
        let reply = ask(AnswerKey::custom("twice"), Some("ab")).unwrap();
        assert_eq!(reply.key.as_deref(), Some("abab"));
        let long = "x".repeat(MAX_KEY_BYTES);
        let reply = ask(AnswerKey::custom("twice"), Some(&long)).unwrap();
        assert_eq!(reply.key, None, "the app key passes the cap");
        let probe = ask(AnswerKey::custom("missing"), None).unwrap();
        assert!(!probe.known, "a probe learns that the function is missing");
        let error = ask(AnswerKey::custom("missing"), Some("a")).unwrap_err();
        assert!(error.contains("missing"), "{error}");
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
