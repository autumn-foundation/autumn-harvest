//! Anonymous candidate ids (issues #2376 and #2377).
//!
//! A model that picks a candidate can be swayed by a model name, an overlay
//! name or the order of the list. So a report or a bundle names each
//! candidate by an anonymous id only.
//!
//! - An id is [`ANON_PREFIX`] and hex digits. The hex is the SHA-256 digest
//!   of the seed, the source run id, the candidate digest and its
//!   occurrence.
//! - All ids of one call use the same number of digits: the fewest, but at
//!   least [`MIN_ANON_HEX`], that keep the ids distinct.
//!   `formal/verus/src/agent_anonymise.rs` proves that width rule.
//! - A list in id order does not show the input order. A new seed gives a
//!   new order.
//!
//! [`leaks`] is the check that a text names no hidden identity.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::response_cache::hex;

/// The prefix of each anonymous id.
pub const ANON_PREFIX: &str = "c-";

/// The fewest hex digits of an anonymous id.
pub const MIN_ANON_HEX: usize = 12;

/// The domain tag of the id digest. A new id scheme takes a new tag.
const DOMAIN: &[u8] = b"autumn-harvest/eval-anon/v1";

/// The anonymous id of one candidate, as in `c-1a2b3c4d5e6f`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AnonId(String);

impl AnonId {
    /// The id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AnonId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The identity of one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateKey<'a> {
    /// The candidate digest, such as an overlay digest.
    pub digest: &'a str,
    /// The place of the candidate among candidates with the same digest.
    /// Give each such candidate its own value, so each gets its own id.
    pub occurrence: u32,
}

/// The anonymous id of each key, in the order of `keys`.
///
/// `source` names the source run. Equal keys get equal ids, so give each
/// candidate a distinct key.
#[must_use]
pub fn assign(seed: u64, source: &str, keys: &[CandidateKey<'_>]) -> Vec<AnonId> {
    let full: Vec<String> = keys.iter().map(|key| digest(seed, source, key)).collect();
    let mut shared = Vec::new();
    for (at, a) in full.iter().enumerate() {
        for b in full.iter().skip(at + 1) {
            shared.push(shared_digits(a.as_bytes(), b.as_bytes()));
        }
    }
    let cap = full.first().map_or(MIN_ANON_HEX, String::len);
    let width = id_width(&shared, MIN_ANON_HEX, cap.max(MIN_ANON_HEX));
    full.iter()
        .map(|hex| AnonId(format!("{ANON_PREFIX}{}", &hex[..width.min(hex.len())])))
        .collect()
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
/// count below `cap`. An equal pair shares `cap` digits, and no width
/// splits it.
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

/// The SHA-256 digest of one key, as hex. Each field has a length prefix,
/// so no two field lists give one byte string.
fn digest(seed: u64, source: &str, key: &CandidateKey<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    hasher.update(seed.to_le_bytes());
    for field in [source.as_bytes(), key.digest.as_bytes()] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hasher.update(key.occurrence.to_le_bytes());
    hex(&hasher.finalize())
}

/// Each needle that `text` holds. An empty needle never counts.
#[must_use]
pub fn leaks<'n>(text: &str, needles: &[&'n str]) -> Vec<&'n str> {
    needles
        .iter()
        .copied()
        .filter(|needle| !needle.is_empty() && text.contains(needle))
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;

    fn key(digest: &str) -> CandidateKey<'_> {
        CandidateKey {
            digest,
            occurrence: 0,
        }
    }

    #[test]
    fn an_id_is_the_prefix_and_at_least_twelve_hex_digits() {
        let ids = assign(1, "run-1", &[key("a"), key("b")]);
        for id in &ids {
            let hex = id.as_str().strip_prefix(ANON_PREFIX).expect("the prefix");
            assert!(hex.len() >= MIN_ANON_HEX, "{id}");
            assert!(hex.bytes().all(|b| b.is_ascii_hexdigit()), "{id}");
        }
    }

    #[test]
    fn the_input_order_does_not_change_an_id() {
        let forward = assign(9, "run-1", &[key("a"), key("b")]);
        let backward = assign(9, "run-1", &[key("b"), key("a")]);
        assert_eq!(forward[0], backward[1]);
        assert_eq!(forward[1], backward[0]);
    }

    #[test]
    fn two_seeds_order_a_pair_in_two_ways() {
        let order = |seed| {
            let ids = assign(seed, "run-1", &[key("a"), key("b")]);
            ids[0] < ids[1]
        };
        assert_ne!(
            order(SEED_A),
            order(SEED_B),
            "the seeds are known to differ"
        );
    }

    #[test]
    fn an_id_depends_on_the_seed_the_source_and_the_digest() {
        let base = assign(1, "run-1", &[key("a")]);
        assert_ne!(base, assign(2, "run-1", &[key("a")]));
        assert_ne!(base, assign(1, "run-2", &[key("a")]));
        assert_ne!(base, assign(1, "run-1", &[key("b")]));
        assert_eq!(base, assign(1, "run-1", &[key("a")]), "it is reproducible");
    }

    #[test]
    fn an_equal_digest_gets_another_id_by_its_occurrence() {
        let twice = [
            key("a"),
            CandidateKey {
                digest: "a",
                occurrence: 1,
            },
        ];
        let ids = assign(1, "run-1", &twice);
        assert_ne!(ids[0], ids[1]);
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
    fn a_leak_names_each_needle_that_the_text_holds() {
        let found = leaks("model fast said hi", &["fast", "slow", ""]);
        assert_eq!(found, vec!["fast"]);
        assert_eq!(leaks("clean", &["fast"]), Vec::<&str>::new());
    }

    /// Two seeds that order the keys `a` and `b` in two ways.
    const SEED_A: u64 = 1;
    const SEED_B: u64 = 2;

    proptest! {
        #[test]
        fn distinct_keys_give_distinct_ids(
            seed in any::<u64>(),
            digests in proptest::collection::btree_set("[a-f0-9]{1,8}", 1..24),
        ) {
            let keys: Vec<CandidateKey<'_>> = digests.iter().map(|d| key(d)).collect();
            let ids = assign(seed, "run", &keys);
            let unique: BTreeSet<&AnonId> = ids.iter().collect();
            prop_assert_eq!(unique.len(), keys.len());
            let width = ids[0].as_str().len();
            prop_assert!(ids.iter().all(|id| id.as_str().len() == width));
        }
    }
}
