//! Seed corpus and generator checks for the `fuzz_replay` target (issue #1835).
//!
//! The nightly fuzzer grows the corpus. This suite keeps the committed seeds
//! green on every change, so the #1253 and #1758 reproducers stay pinned. It
//! also runs the `arbitrary` generator over fixed bytes on stable Rust.

use std::collections::BTreeSet;
use std::path::PathBuf;

use autumn_harvest::fuzzing::{Op, ReplayCase, Verdict, check_case};

fn seed_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fuzz/seeds/fuzz_replay")
}

/// Every `*.json` seed file, sorted by name.
fn seeds() -> Vec<(String, Vec<u8>)> {
    let dir = seed_dir();
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("seed dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let bytes = std::fs::read(&path).expect("seed file reads");
            (name, bytes)
        })
        .collect();
    out.sort();
    out
}

/// A seed parsed as a JSON case. A seed that the generator would read as
/// random bytes is a broken seed.
fn parse(name: &str, bytes: &[u8]) -> ReplayCase {
    assert!(
        ReplayCase::is_json(bytes),
        "seed {name} must be a JSON case"
    );
    ReplayCase::from_json(bytes).unwrap_or_else(|| panic!("seed {name} must parse as a JSON case"))
}

fn seed(prefix: &str) -> Vec<(String, ReplayCase)> {
    seeds()
        .into_iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .map(|(name, bytes)| {
            let case = parse(&name, &bytes);
            (name, case)
        })
        .collect()
}

/// A written seed must reach the replayer. A stored seed may stop at the
/// read path, which is the point of a stored seed.
#[test]
fn every_seed_passes_the_oracles() {
    let all = seeds();
    assert!(!all.is_empty(), "{} has no seeds", seed_dir().display());
    for (name, bytes) in all {
        let case = parse(&name, &bytes);
        // `check_case` panics on a violated oracle.
        let verdict = check_case(&case);
        if case.stored {
            assert!(
                matches!(verdict, Verdict::Unreadable(_) | Verdict::Replayed(_)),
                "seed {name}: {verdict:?}"
            );
        } else {
            assert!(
                matches!(verdict, Verdict::Replayed(_)),
                "seed {name} must reach the replayer: {verdict:?}"
            );
        }
    }
}

#[test]
fn json_detection_ignores_a_bom_and_white_space() {
    let json = br#"{"stored": true, "history": []}"#;
    for prefix in [&b""[..], b"\xEF\xBB\xBF", b"\n  "] {
        let bytes = [prefix, &json[..]].concat();
        assert!(ReplayCase::is_json(&bytes));
        let case = ReplayCase::from_json(&bytes).expect("a JSON case");
        assert!(case.stored && case.history.is_empty());
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
        let cases = seed(prefix);
        !cases.is_empty()
            && cases.iter().all(|(_, case)| {
                let json = serde_json::to_value(&case.history).unwrap();
                any_value(&json, needle)
            })
    };
    assert!(has("issue-1253-", &|v| {
        v.get(CODEC_ENVELOPE_KEY) == Some(&serde_json::json!(2))
            && v.get("kid") == Some(&serde_json::json!("2026-q3"))
            && v.get("codec_id").is_some_and(serde_json::Value::is_string)
            && v.get("data").is_some_and(serde_json::Value::is_string)
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

/// Generated cases pass the oracles, and most of them reach the replayer
/// with a history of several events. A generator that builds empty
/// histories would fuzz nothing.
#[test]
fn generated_cases_pass_the_oracles() {
    let (mut cases, mut replayed, mut events) = (0_usize, 0_usize, 0_usize);
    for seed in 0..300 {
        let data = bytes(seed, 2048);
        if let Some(case) = ReplayCase::from_fuzz_bytes(&data) {
            cases += 1;
            events += case.history.len();
            if matches!(check_case(&case), Verdict::Replayed(_)) {
                replayed += 1;
            }
        }
    }
    assert!(cases >= 290, "only {cases} of 300 inputs decoded");
    assert!(
        replayed * 10 >= cases * 8,
        "only {replayed} of {cases} cases replayed"
    );
    assert!(
        events >= cases * 3,
        "a mean history of {events}/{cases} events is too short"
    );
}

/// Every generated program survives the JSON round trip. The replayed
/// workflow reads its program back from a JSON header. A value that JSON
/// cannot hold, such as a NaN `f64`, once crashed the fuzz target.
#[test]
fn generated_programs_round_trip_through_json() {
    let mut programs = 0_usize;
    for seed in 0..300 {
        let Some(case) = ReplayCase::from_fuzz_bytes(&bytes(seed, 2048)) else {
            continue;
        };
        let Some(program) = case.program else {
            continue;
        };
        programs += 1;
        let json = serde_json::to_string(&program).expect("a program serializes");
        let back: Vec<Op> = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("input {seed}: the program does not read back: {e}"));
        assert_eq!(back.len(), program.len(), "input {seed}");
    }
    assert!(programs >= 50, "only {programs} inputs carried a program");
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
