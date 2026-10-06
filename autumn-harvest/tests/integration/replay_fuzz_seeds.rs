//! Seed corpus and generator checks for the `fuzz_replay` target (issue #1835).
//!
//! The nightly fuzzer grows the corpus. This suite keeps the committed seeds
//! green on every change, so the #1253 and #1758 reproducers stay pinned. It
//! also runs the `arbitrary` generator over fixed bytes on stable Rust.

use std::collections::BTreeSet;
use std::path::PathBuf;

use autumn_harvest::fuzzing::{ReplayCase, Verdict, check_case};

fn seed_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fuzz/seeds/fuzz_replay")
}

/// Every seed file, sorted by name.
fn seeds() -> Vec<(String, Vec<u8>)> {
    let dir = seed_dir();
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("seed dir entry").path())
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let bytes = std::fs::read(&path).expect("seed file reads");
            (name, bytes)
        })
        .collect();
    out.sort();
    out
}

fn seed(prefix: &str) -> Vec<(String, ReplayCase)> {
    seeds()
        .into_iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .map(|(name, bytes)| {
            let case = ReplayCase::from_fuzz_bytes(&bytes)
                .unwrap_or_else(|| panic!("seed {name} must parse as a JSON case"));
            (name, case)
        })
        .collect()
}

#[test]
fn every_seed_passes_the_oracles() {
    let all = seeds();
    assert!(!all.is_empty(), "{} has no seeds", seed_dir().display());
    for (name, bytes) in all {
        let case = ReplayCase::from_fuzz_bytes(&bytes)
            .unwrap_or_else(|| panic!("seed {name} must parse as a JSON case"));
        // `check_case` panics on a violated oracle. The name gives context.
        eprintln!("seed {name}: {:?}", check_case(&case));
    }
}

/// The reproducers go through the write path, read back unchanged, and
/// replay to the end. Before the fixes, the read failed or changed the value.
#[test]
fn issue_reproducers_round_trip_and_replay_clean() {
    for prefix in ["issue-1253-", "issue-1758-"] {
        let cases = seed(prefix);
        assert!(
            !cases.is_empty(),
            "the seed corpus must hold a {prefix}* reproducer"
        );
        for (name, case) in cases {
            assert!(!case.stored, "{name} must go through the write path");
            assert_eq!(
                check_case(&case),
                Verdict::Replayed("ReplaySucceeded".to_string()),
                "{name} must replay clean"
            );
        }
    }
}

/// The #1253 seeds hold a keyed flat codec envelope look-alike. The #1758
/// seeds hold a complete offload envelope look-alike.
#[test]
fn issue_reproducers_carry_the_look_alike_shapes() {
    use autumn_harvest::payload_codec::CODEC_ENVELOPE_KEY;
    use autumn_harvest::payload_store::OFFLOAD_ENVELOPE_KEY;

    let has = |prefix: &str, needle: &dyn Fn(&serde_json::Value) -> bool| {
        seed(prefix).iter().all(|(_, case)| {
            let json = serde_json::to_value(&case.history).unwrap();
            any_value(&json, needle)
        })
    };
    assert!(has("issue-1253-", &|v| {
        v.get(CODEC_ENVELOPE_KEY) == Some(&serde_json::json!(2))
            && v.get("kid").is_some()
            && v.as_object().is_some_and(|o| o.len() == 4)
    }));
    assert!(has("issue-1758-", &|v| {
        v.get(OFFLOAD_ENVELOPE_KEY) == Some(&serde_json::json!(1))
            && ["store_id", "key", "len", "checksum"]
                .iter()
                .all(|k| v.get(k).is_some())
    }));
}

fn any_value(v: &serde_json::Value, pred: &dyn Fn(&serde_json::Value) -> bool) -> bool {
    pred(v)
        || match v {
            serde_json::Value::Array(items) => items.iter().any(|i| any_value(i, pred)),
            serde_json::Value::Object(map) => map.values().any(|i| any_value(i, pred)),
            _ => false,
        }
}

/// Fixed pseudo-random bytes, so the run is the same on every machine.
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            // SplitMix64.
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)).to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn generated_cases_pass_the_oracles() {
    for seed in 0..300 {
        let data = bytes(seed, 2048);
        if let Some(case) = ReplayCase::from_fuzz_bytes(&data) {
            let _ = check_case(&case);
        }
    }
}

/// The generator reaches every `WorkflowEvent` variant. A variant it cannot
/// build would never be fuzzed.
#[test]
fn generator_reaches_every_event_variant() {
    let source = include_str!("../../src/event.rs").replace("\r\n", "\n");
    let start = source
        .find("pub enum WorkflowEvent")
        .expect("enum declared");
    let body = &source[start..];
    let body = &body[..body.find("\n}\n").expect("enum ends")];
    let declared: BTreeSet<String> = body
        .lines()
        .filter_map(|l| l.strip_prefix("    "))
        .filter(|l| l.starts_with(|c: char| c.is_ascii_uppercase()))
        .filter_map(|l| l.split([' ', ',', '{', '(']).next())
        .map(str::to_string)
        .collect();
    assert!(declared.len() >= 40, "the variant scan found {declared:?}");

    let mut seen = BTreeSet::new();
    for seed in 0..4000 {
        if let Some(case) = ReplayCase::from_fuzz_bytes(&bytes(seed, 512)) {
            for event in &case.history {
                let json = serde_json::to_value(event).unwrap();
                seen.insert(json["type"].as_str().unwrap().to_string());
            }
        }
    }
    let missing: Vec<_> = declared.difference(&seen).collect();
    assert!(missing.is_empty(), "the generator never built {missing:?}");
}
