//! Guard for the nightly fuzz workflow (issue #1835). No DB, no feature gate.
//!
//! `fuzz-nightly.yml` must fuzz every target on a cron. It must restore the
//! corpus artifact before the run and upload it after the run, also after a
//! crash. A
//! failed scheduled run must open an issue. The target lists in
//! `fuzz/Cargo.toml`, `fuzz/smoke.sh` and the workflow must agree, and so
//! must the nightly toolchain that the two files pin.

use std::collections::BTreeSet;

use super::ci_run_coverage::{
    parse_workflow, parse_workflow_text, repo_root, ungated, workflow_crons,
};

const FUZZ_CARGO_TOML: &str = include_str!("../../../fuzz/Cargo.toml");
const FUZZ_SMOKE_SH: &str = include_str!("../../../fuzz/smoke.sh");
const NIGHTLY: &str = ".github/workflows/fuzz-nightly.yml";

/// The path that the upload step must name.
const CORPUS_PATH: &str = "fuzz/corpus/${{ matrix.target }}";

/// The artifact name that the upload step must use.
const CORPUS_ARTIFACT: &str = "fuzz-corpus-${{ matrix.target }}";

/// The same artifact name, as the restore script spells it.
const CORPUS_ARTIFACT_SH: &str = "fuzz-corpus-${TARGET}";

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
/// The corpus lives in a run artifact, not in the actions cache. The CI
/// build caches fill the cache, and GitHub evicted a corpus entry within
/// minutes. The restore script downloads the newest artifact of the same
/// name. It must skip artifacts of a fork run, so a fork cannot seed the
/// nightly, and it needs `actions: read`. The upload must run also after a
/// failed step, so a crash does not discard the corpus that the run grew. It
/// must overwrite, so a re-run attempt can upload again. The fuzz step must
/// not hide its own failure.
fn persists_the_corpus(job: &serde_yaml::Value) -> bool {
    let steps = steps(job);
    let restore = steps.iter().position(|s| {
        let run = str_at(s, &["run"]);
        run.contains("gh run download")
            && run.contains(CORPUS_ARTIFACT_SH)
            && run.contains("fuzz/corpus/${TARGET}")
            && run.contains("head_repository_id")
    });
    let save = steps.iter().position(|s| {
        str_at(s, &["uses"]).starts_with("actions/upload-artifact@")
            && str_at(s, &["with", "path"]) == CORPUS_PATH
            && str_at(s, &["with", "name"]) == CORPUS_ARTIFACT
    });
    let fuzz = steps.iter().position(|s| {
        let run = str_at(s, &["run"]);
        ungated(s)
            && run.contains(" fuzz run ")
            && run.contains("corpus/")
            && run.contains("seeds/")
    });
    let (Some(r), Some(sv)) = (restore, save) else {
        return false;
    };
    let upload = steps[sv];
    let overwrites = upload
        .get("with")
        .and_then(|w| w.get("overwrite"))
        .and_then(serde_yaml::Value::as_bool)
        == Some(true);
    str_at(job, &["env", "TARGET"]) == "${{ matrix.target }}"
        && str_at(job, &["permissions", "actions"]) == "read"
        && str_at(upload, &["if"]).contains("always()")
        && overwrites
        && fuzz.is_some_and(|f| r < f && f < sv)
}

/// The draft-skip condition that `ci.yml` uses. A schedule always runs.
const DRAFT_SKIP: &str =
    "github.event_name != 'pull_request' || github.event.pull_request.draft == false";

/// True when a job has no `continue-on-error` and no `if` but [`DRAFT_SKIP`].
fn runs_on_every_schedule(job: &serde_yaml::Value) -> bool {
    let soft = job
        .get("continue-on-error")
        .is_some_and(|v| v.as_bool() != Some(false));
    !soft
        && matches!(
            job.get("if").map(serde_yaml::Value::as_str),
            None | Some(Some(DRAFT_SKIP))
        )
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
        runs_on_every_schedule(fuzz),
        "the `fuzz` job must have no `continue-on-error` and no `if` but the draft-skip"
    );
    assert!(
        persists_the_corpus(fuzz),
        "the `fuzz` job must download the {CORPUS_ARTIFACT} artifact of a run of this \
         repository, fuzz with corpus/ and seeds/, then upload {CORPUS_PATH} with \
         `if: always()` and `overwrite: true`"
    );
    assert!(
        alerts_on_scheduled_failure(&doc["jobs"]["alert"], "fuzz"),
        "the `alert` job must open an issue when a scheduled `fuzz` run fails"
    );
}

/// Self-test: each missing part of the corpus cycle fails the check.
#[test]
fn corpus_check_rejects_a_broken_cycle() {
    let head =
        "    env:\n      TARGET: ${{ matrix.target }}\n    permissions:\n      actions: read\n";
    let restore = "      - run: |\n          id=$(gh api x --jq 'select(.head_repository_id)')\n          \
                   gh run download \"$id\" --name fuzz-corpus-${TARGET} --dir fuzz/corpus/${TARGET}\n";
    let fuzz = "      - run: cargo +nightly fuzz run t corpus/t seeds/t\n";
    let save = "      - uses: actions/upload-artifact@v4\n        if: always()\n        with:\n          \
                name: fuzz-corpus-${{ matrix.target }}\n          path: fuzz/corpus/${{ matrix.target }}\n          \
                overwrite: true\n";
    let job = |head: &str, steps: &str| {
        let text = format!("jobs:\n  fuzz:\n    runs-on: x\n{head}    steps:\n{steps}");
        let doc = parse_workflow_text(&text).expect("synthetic workflow must parse");
        persists_the_corpus(&doc["jobs"]["fuzz"])
    };
    assert!(job(head, &format!("{restore}{fuzz}{save}")));
    assert!(!job(head, &format!("{fuzz}{save}")), "no restore");
    assert!(!job(head, &format!("{restore}{fuzz}")), "no save");
    assert!(!job(head, &format!("{save}{fuzz}{restore}")), "wrong order");
    let no_read = head.replace("    permissions:\n      actions: read\n", "");
    assert!(
        !job(&no_read, &format!("{restore}{fuzz}{save}")),
        "no `actions: read`"
    );
    let trusts_forks = restore.replace("select(.head_repository_id)", "true");
    assert!(
        !job(head, &format!("{trusts_forks}{fuzz}{save}")),
        "a restore that takes a fork's corpus"
    );
    let other_name = save.replace("name: fuzz-corpus-", "name: corpus-");
    assert!(
        !job(head, &format!("{restore}{fuzz}{other_name}")),
        "an upload name that the restore never reads"
    );
    let skipped_on_crash = save.replace("        if: always()\n", "");
    assert!(
        !job(head, &format!("{restore}{fuzz}{skipped_on_crash}")),
        "an upload that a crash skips"
    );
    let no_overwrite = save.replace("          overwrite: true\n", "");
    assert!(
        !job(head, &format!("{restore}{fuzz}{no_overwrite}")),
        "an upload that a re-run attempt cannot repeat"
    );
    let no_seeds = fuzz.replace(" seeds/t", "");
    assert!(
        !job(head, &format!("{restore}{no_seeds}{save}")),
        "no seed corpus"
    );
    let soft_fuzz = format!("{fuzz}        continue-on-error: true\n");
    assert!(
        !job(head, &format!("{restore}{soft_fuzz}{save}")),
        "a fuzz step that hides a crash"
    );
}

/// The `on.pull_request.paths` filter of a workflow.
fn pr_paths(doc: &serde_yaml::Value) -> Vec<&str> {
    doc.get("on")
        .and_then(|on| on.get("pull_request"))
        .and_then(|pr| pr.get("paths"))
        .and_then(serde_yaml::Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(serde_yaml::Value::as_str)
        .collect()
}

/// True when a filter entry matches `file`: the same path, or a `dir/**`
/// entry over it.
fn covers(paths: &[&str], file: &str) -> bool {
    paths.iter().any(|entry| {
        entry.strip_suffix("/**").map_or(*entry == file, |dir| {
            file.strip_prefix(dir)
                .is_some_and(|rest| rest.starts_with('/'))
        })
    })
}

/// Every `.rs` file under `dir`, relative to the repository root.
fn rust_files(dir: &str) -> Vec<String> {
    let root = repo_root();
    let mut out = Vec::new();
    let mut todo = vec![root.join(dir)];
    while let Some(path) = todo.pop() {
        for entry in std::fs::read_dir(&path).expect("the source directory is readable") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                todo.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let rel = path.strip_prefix(&root).expect("under the repository root");
                out.push(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    out
}

/// The fuzz targets link the whole `autumn-harvest` crate. `fuzz_replay`
/// drives the replayer, the codecs and the workflow context. So a change to
/// any of its source files, or to the manifests that set its dependencies,
/// must run the PR campaign.
#[test]
fn a_change_to_the_crate_source_runs_the_pr_campaign() {
    let doc = parse_workflow(NIGHTLY);
    let paths = pr_paths(&doc);
    let files = rust_files("autumn-harvest/src");
    assert!(
        files.iter().any(|f| f == "autumn-harvest/src/replay.rs"),
        "the walk must find the replayer"
    );
    let missed: Vec<&String> = files.iter().filter(|f| !covers(&paths, f)).collect();
    assert!(
        missed.is_empty(),
        "{NIGHTLY} `on.pull_request.paths` must cover {missed:?}"
    );
    // The root manifest and lockfile set the crate's dependencies.
    for manifest in ["Cargo.toml", "Cargo.lock", "autumn-harvest/Cargo.toml"] {
        assert!(
            covers(&paths, manifest),
            "{NIGHTLY} `on.pull_request.paths` must cover {manifest}"
        );
    }
}

/// Self-test: the path check reads both entry shapes, and the old filter
/// misses the replayer.
#[test]
fn path_check_rejects_a_filter_without_the_replayer() {
    let old = [
        "autumn-harvest/src/event.rs",
        "autumn-harvest/src/fuzzing.rs",
    ];
    assert!(covers(&old, "autumn-harvest/src/event.rs"));
    assert!(!covers(&old, "autumn-harvest/src/replay.rs"));
    let new = ["autumn-harvest/src/**"];
    assert!(covers(&new, "autumn-harvest/src/replay.rs"));
    assert!(covers(&new, "autumn-harvest/src/replay/matcher.rs"));
    assert!(!covers(&new, "autumn-harvest/srcx/lib.rs"));
    let text = "on:\n  pull_request:\n    paths:\n      - \"a/**\"\n      - b.rs\n";
    let doc = parse_workflow_text(text).expect("synthetic workflow must parse");
    assert_eq!(pr_paths(&doc), vec!["a/**", "b.rs"]);
}

/// Self-test: only the draft-skip `if` may gate the fuzz job.
#[test]
fn schedule_gate_accepts_only_the_draft_skip() {
    let job = |extra: &str| {
        let text = format!("jobs:\n  fuzz:\n    runs-on: x\n{extra}");
        let doc = parse_workflow_text(&text).expect("synthetic workflow must parse");
        runs_on_every_schedule(&doc["jobs"]["fuzz"])
    };
    assert!(job(""));
    assert!(job(&format!("    if: {DRAFT_SKIP}\n")));
    assert!(!job("    if: github.event_name == 'workflow_dispatch'\n"));
    assert!(!job("    continue-on-error: true\n"));
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
