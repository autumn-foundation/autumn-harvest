//! Guard for the nightly fuzz workflow (issue #1835). No DB, no feature gate.
//!
//! `fuzz-nightly.yml` must fuzz every target on a cron. It must restore the
//! corpus before the run and save it after the run, also after a crash. A
//! failed scheduled run must open an issue. The target lists in
//! `fuzz/Cargo.toml`, `fuzz/smoke.sh` and the workflow must agree, and so
//! must the nightly toolchain that the two files pin.

use std::collections::BTreeSet;

use super::ci_run_coverage::{parse_workflow, parse_workflow_text, ungated, workflow_crons};

const FUZZ_CARGO_TOML: &str = include_str!("../../../fuzz/Cargo.toml");
const FUZZ_SMOKE_SH: &str = include_str!("../../../fuzz/smoke.sh");
const NIGHTLY: &str = ".github/workflows/fuzz-nightly.yml";

/// The path that both cache steps must name.
const CORPUS_PATH: &str = "fuzz/corpus/${{ matrix.target }}";

/// The `name` of each `[[bin]]` in `fuzz/Cargo.toml`.
fn cargo_targets(toml: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut in_bin = false;
    for line in toml.lines().map(str::trim) {
        if line.starts_with('[') {
            in_bin = line == "[[bin]]";
            continue;
        }
        if in_bin && let Some(value) = line.strip_prefix("name = ") {
            out.insert(value.trim_matches('"').to_string());
        }
    }
    out
}

/// The entries of the `TARGETS=( ... )` array in `fuzz/smoke.sh`.
fn smoke_targets(script: &str) -> BTreeSet<String> {
    let start = script.find("TARGETS=(").expect("smoke.sh declares TARGETS");
    let body = &script[start + "TARGETS=(".len()..];
    let end = body.find(')').expect("the TARGETS array closes");
    body[..end].split_whitespace().map(str::to_string).collect()
}

/// The default of `TOOLCHAIN="${FUZZ_TOOLCHAIN:-<pin>}"` in `fuzz/smoke.sh`.
fn smoke_toolchain(script: &str) -> &str {
    let start = script
        .find("${FUZZ_TOOLCHAIN:-")
        .expect("smoke.sh defaults FUZZ_TOOLCHAIN");
    let rest = &script[start + "${FUZZ_TOOLCHAIN:-".len()..];
    &rest[..rest.find('}').expect("the default closes")]
}

/// The `matrix.target` list of a job.
fn matrix_targets(job: &serde_yaml::Value) -> BTreeSet<String> {
    job.get("strategy")
        .and_then(|s| s.get("matrix"))
        .and_then(|m| m.get("target"))
        .and_then(serde_yaml::Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(serde_yaml::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn steps(job: &serde_yaml::Value) -> Vec<&serde_yaml::Value> {
    job.get("steps")
        .and_then(serde_yaml::Value::as_sequence)
        .map(|s| s.iter().collect())
        .unwrap_or_default()
}

fn str_at<'a>(node: &'a serde_yaml::Value, path: &[&str]) -> &'a str {
    let mut cur = node;
    for key in path {
        match cur.get(key) {
            Some(next) => cur = next,
            None => return "",
        }
    }
    cur.as_str().unwrap_or("")
}

/// True when a job restores the corpus before it fuzzes and saves it after.
///
/// The restore step must fall back to the newest earlier corpus through
/// `restore-keys`. The save step must use a key unique to the run, because a
/// cache key is immutable. It must run also after a failed step, so a crash
/// does not discard the corpus that the run grew.
fn persists_the_corpus(job: &serde_yaml::Value) -> bool {
    let steps = steps(job);
    let position = |pred: &dyn Fn(&serde_yaml::Value) -> bool| steps.iter().position(|s| pred(s));
    let restore = position(&|s| {
        str_at(s, &["uses"]).starts_with("actions/cache/restore@")
            && str_at(s, &["with", "path"]) == CORPUS_PATH
            && !str_at(s, &["with", "restore-keys"]).is_empty()
    });
    let fuzz = position(&|s| {
        let run = str_at(s, &["run"]);
        run.contains(" fuzz run ") && run.contains("corpus/") && run.contains("seeds/")
    });
    let save = position(&|s| {
        str_at(s, &["uses"]).starts_with("actions/cache/save@")
            && str_at(s, &["with", "path"]) == CORPUS_PATH
            && str_at(s, &["with", "key"]).contains("${{ github.run_id }}")
            && str_at(s, &["if"]).contains("always()")
    });
    matches!((restore, fuzz, save), (Some(r), Some(f), Some(s)) if r < f && f < s)
}

/// True when a job opens an issue for a failed scheduled run of `needs`.
fn alerts_on_scheduled_failure(job: &serde_yaml::Value, needs: &str) -> bool {
    let condition = str_at(job, &["if"]);
    let needs_ok = match job.get("needs") {
        Some(serde_yaml::Value::String(s)) => s == needs,
        Some(serde_yaml::Value::Sequence(s)) => s.iter().any(|n| n.as_str() == Some(needs)),
        _ => false,
    };
    needs_ok
        && condition.contains("failure()")
        && condition.contains("'schedule'")
        && str_at(job, &["permissions", "issues"]) == "write"
        && steps(job)
            .iter()
            .any(|s| str_at(s, &["run"]).contains("gh issue create"))
}

#[test]
fn fuzz_target_lists_agree() {
    let cargo = cargo_targets(FUZZ_CARGO_TOML);
    assert!(
        cargo.contains("fuzz_replay"),
        "fuzz/Cargo.toml must declare the `fuzz_replay` target; found {cargo:?}"
    );
    assert_eq!(
        smoke_targets(FUZZ_SMOKE_SH),
        cargo,
        "fuzz/smoke.sh TARGETS must list every fuzz/Cargo.toml [[bin]]"
    );
    let doc = parse_workflow(NIGHTLY);
    assert_eq!(
        matrix_targets(&doc["jobs"]["fuzz"]),
        cargo,
        "{NIGHTLY} must fuzz every fuzz/Cargo.toml [[bin]]"
    );
}

/// The workflow and `smoke.sh` fuzz with one dated nightly. A newer nightly
/// can fail to compile the crate, so the pin must be a date.
#[test]
fn fuzz_toolchain_pins_agree() {
    let doc = parse_workflow(NIGHTLY);
    let pinned = str_at(&doc, &["env", "FUZZ_TOOLCHAIN"]);
    let dated = pinned
        .strip_prefix("nightly-")
        .is_some_and(|d| d.len() == 10 && d.chars().all(|c| c.is_ascii_digit() || c == '-'));
    assert!(
        dated,
        "{NIGHTLY} must pin `env.FUZZ_TOOLCHAIN: nightly-YYYY-MM-DD`; found {pinned:?}"
    );
    assert_eq!(
        smoke_toolchain(FUZZ_SMOKE_SH),
        pinned,
        "fuzz/smoke.sh must default to the nightly that {NIGHTLY} pins"
    );
    let installs = steps(&doc["jobs"]["fuzz"]).iter().any(|s| {
        str_at(s, &["uses"]).starts_with("dtolnay/rust-toolchain@")
            && str_at(s, &["with", "toolchain"]) == "${{ env.FUZZ_TOOLCHAIN }}"
    });
    assert!(installs, "the `fuzz` job must install `env.FUZZ_TOOLCHAIN`");
}

#[test]
fn fuzz_nightly_runs_on_a_cron_with_a_persisted_corpus() {
    let doc = parse_workflow(NIGHTLY);
    assert!(
        !workflow_crons(&doc).is_empty(),
        "{NIGHTLY} must have an `on.schedule` cron"
    );
    let fuzz = &doc["jobs"]["fuzz"];
    assert!(
        ungated(fuzz),
        "the `fuzz` job must have no `if` and no `continue-on-error`"
    );
    assert!(
        persists_the_corpus(fuzz),
        "the `fuzz` job must restore {CORPUS_PATH}, fuzz with corpus/ and seeds/, \
         then save it under a run-unique key with `if: always()`"
    );
    assert!(
        alerts_on_scheduled_failure(&doc["jobs"]["alert"], "fuzz"),
        "the `alert` job must open an issue when a scheduled `fuzz` run fails"
    );
}

/// Self-test: each missing part of the corpus cycle fails the check.
#[test]
fn corpus_check_rejects_a_broken_cycle() {
    let restore = "      - uses: actions/cache/restore@v4\n        with:\n          path: \
                   fuzz/corpus/${{ matrix.target }}\n          key: k-${{ github.run_id }}\n          \
                   restore-keys: k-\n";
    let fuzz = "      - run: cargo +nightly fuzz run t corpus/t seeds/t\n";
    let save = "      - uses: actions/cache/save@v4\n        if: always()\n        with:\n          \
                path: fuzz/corpus/${{ matrix.target }}\n          key: k-${{ github.run_id }}\n";
    let job = |steps: &str| {
        let text = format!("jobs:\n  fuzz:\n    runs-on: x\n    steps:\n{steps}");
        let doc = parse_workflow_text(&text).expect("synthetic workflow must parse");
        persists_the_corpus(&doc["jobs"]["fuzz"])
    };
    assert!(job(&format!("{restore}{fuzz}{save}")));
    assert!(!job(&format!("{fuzz}{save}")), "no restore");
    assert!(!job(&format!("{restore}{fuzz}")), "no save");
    assert!(!job(&format!("{save}{fuzz}{restore}")), "wrong order");
    let no_fallback = restore.replace("          restore-keys: k-\n", "");
    assert!(
        !job(&format!("{no_fallback}{fuzz}{save}")),
        "no restore-keys"
    );
    let fixed_key = save.replace("k-${{ github.run_id }}", "k-fixed");
    assert!(
        !job(&format!("{restore}{fuzz}{fixed_key}")),
        "an immutable key"
    );
    let skipped_on_crash = save.replace("        if: always()\n", "");
    assert!(
        !job(&format!("{restore}{fuzz}{skipped_on_crash}")),
        "a save that a crash skips"
    );
    let no_seeds = fuzz.replace(" seeds/t", "");
    assert!(
        !job(&format!("{restore}{no_seeds}{save}")),
        "no seed corpus"
    );
}

/// Self-test: the target parsers read the shapes the files use.
#[test]
fn target_parsers_read_the_file_shapes() {
    let toml = "[package]\nname = \"pkg\"\n\n[[bin]]\nname = \"a\"\npath = \"a.rs\"\n\n\
                [profile.release]\ndebug = 1\n\n[[bin]]\nname = \"b\"\n";
    assert_eq!(
        cargo_targets(toml),
        BTreeSet::from(["a".to_string(), "b".to_string()])
    );
    let script = "TARGETS=(\n  a\n  b\n)\nTOOLCHAIN=\"${FUZZ_TOOLCHAIN:-nightly-2026-01-02}\"\n";
    assert_eq!(smoke_toolchain(script), "nightly-2026-01-02");
    assert_eq!(
        smoke_targets(script),
        BTreeSet::from(["a".to_string(), "b".to_string()])
    );
}
