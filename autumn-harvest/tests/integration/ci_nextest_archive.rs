//! Guard for the live-DB nextest archive (`docs/testing/ci-db-archive.md`).
//!
//! `test-db-archive` builds the test binaries of each `linux` and `linuxpart`
//! manifest row into one archive. Each `test-db-linux` shard runs a slice of
//! it. A row that the archive or the filterset drops stops running in CI, and
//! no test fails. These tests read the manifest and compare it with what the
//! runner executes. They need no DB and no feature.
//!
//! The runner tests start `bash`, so they run on Unix only. The workflow tests
//! parse YAML and run on every OS.

use std::collections::BTreeSet;

use super::ci_run_coverage::{job_needs, parse_workflow, repo_root, steps, str_at};

const MANIFEST: &str = ".github/ci/integration-suites.txt";

/// The artifact that carries the archive from the build job to the shards.
const ARTIFACT: &str = "nextest-db-archive";

/// The step `if` that skips a docs-only change.
const CODE_GATE: &str = "needs.changes.outputs.code == 'true'";

/// One data line of the manifest.
struct Row {
    class: String,
    krate: String,
    target: String,
    features: String,
    filter: String,
}

fn manifest_rows() -> Vec<Row> {
    let text = std::fs::read_to_string(repo_root().join(MANIFEST)).expect("read the manifest");
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let cols: Vec<&str> = l.split_whitespace().collect();
            assert_eq!(cols.len(), 5, "a manifest row has five columns: {l}");
            Row {
                class: cols[0].to_string(),
                krate: cols[1].to_string(),
                target: cols[2].to_string(),
                features: cols[3].to_string(),
                filter: cols[4].to_string(),
            }
        })
        .collect()
}

fn rows_of(class: &str) -> Vec<Row> {
    manifest_rows()
        .into_iter()
        .filter(|r| r.class == class)
        .collect()
}

fn live_db_rows() -> Vec<Row> {
    manifest_rows()
        .into_iter()
        .filter(|r| r.class == "linux" || r.class == "linuxpart")
        .collect()
}

/// The filterset term that selects the tests of one row.
fn expected_term(row: &Row) -> String {
    let binary = format!("binary_id(={}::{})", row.krate, row.target);
    if row.filter == "-" {
        binary
    } else {
        format!("({binary} & test({}))", row.filter)
    }
}

// ── The runner, run with DRY_RUN=1 ──────────────────────────────────────────

#[cfg(unix)]
mod runner {
    use std::process::{Command, Output};

    use super::repo_root;

    const RUNNER: &str = ".github/ci/run-suites.sh";

    /// Runs the runner with `DRY_RUN=1` and the given extra environment.
    ///
    /// The shard variables and `SUITES_MANIFEST` are cleared first, so the
    /// environment of the test process cannot change the result.
    pub fn run(args: &[&str], env: &[(&str, &str)]) -> Output {
        let mut cmd = Command::new("bash");
        cmd.arg(repo_root().join(RUNNER))
            .args(args)
            .current_dir(repo_root())
            .env("DRY_RUN", "1")
            .env_remove("SEMAPHORE_SHARD_COUNT")
            .env_remove("SEMAPHORE_SHARD_INDEX")
            .env_remove("SUITES_MANIFEST");
        for (key, value) in env {
            cmd.env(key, value);
        }
        cmd.output().expect("start bash")
    }

    /// The command that `DRY_RUN=1` prints on its `+ ` line.
    pub fn printed(out: &Output) -> String {
        assert!(
            out.status.success(),
            "the runner failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let lines: Vec<&str> = stdout.lines().filter(|l| l.starts_with("+ ")).collect();
        assert_eq!(lines.len(), 1, "expected one command, got: {stdout}");
        lines[0].trim_start_matches("+ ").to_string()
    }

    /// The values that follow each `flag` in a printed command.
    pub fn values_after<'a>(tokens: &[&'a str], flag: &str) -> Vec<&'a str> {
        tokens
            .windows(2)
            .filter(|w| w[0] == flag)
            .map(|w| w[1])
            .collect()
    }
}

#[cfg(unix)]
#[test]
fn archive_holds_every_live_db_crate_target_and_feature() {
    let command = runner::printed(&runner::run(&["archive", "/tmp/a.tar.zst"], &[]));
    let tokens: Vec<&str> = command.split_whitespace().collect();
    let rows = live_db_rows();

    let crates: BTreeSet<&str> = runner::values_after(&tokens, "-p").into_iter().collect();
    let want: BTreeSet<&str> = rows.iter().map(|r| r.krate.as_str()).collect();
    assert_eq!(crates, want, "the archive must build each live-DB crate");

    let targets: BTreeSet<&str> = runner::values_after(&tokens, "--test")
        .into_iter()
        .collect();
    let want: BTreeSet<&str> = rows.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(targets, want, "the archive must build each live-DB target");

    let features: BTreeSet<&str> = runner::values_after(&tokens, "--features")
        .iter()
        .flat_map(|f| f.split(','))
        .collect();
    let want: BTreeSet<String> = rows
        .iter()
        .filter(|r| r.features != "-")
        .flat_map(|r| r.features.split(',').map(|f| format!("{}/{f}", r.krate)))
        .collect();
    let want: BTreeSet<&str> = want.iter().map(String::as_str).collect();
    assert_eq!(
        features, want,
        "the archive must enable each feature of a live-DB row, qualified by its crate"
    );

    assert!(
        tokens.contains(&"CARGO_PROFILE_TEST_DEBUG=0")
            && tokens.contains(&"CARGO_PROFILE_DEV_DEBUG=0"),
        "the archive must build without debug info, or it does not fit an artifact: {command}"
    );
    assert!(
        command.contains("cargo nextest archive --archive-file /tmp/a.tar.zst"),
        "the archive must go to the named file: {command}"
    );
}

#[cfg(unix)]
#[test]
fn filterset_has_one_term_per_row() {
    for class in ["linux", "linuxpart"] {
        let out = runner::run(&["filterset", class], &[]);
        assert!(out.status.success(), "filterset {class} failed");
        let got = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let rows = rows_of(class);
        assert!(!rows.is_empty(), "the manifest must have `{class}` rows");
        let want = rows
            .iter()
            .map(expected_term)
            .collect::<Vec<_>>()
            .join(" | ");
        assert_eq!(got, want, "each `{class}` row must give one term, in order");
    }
}

#[cfg(unix)]
#[test]
fn nextest_partition_is_the_shard_index_plus_one() {
    for (index, partition) in [("0", "hash:1/21"), ("20", "hash:21/21")] {
        let env = [
            ("SEMAPHORE_SHARD_COUNT", "21"),
            ("SEMAPHORE_SHARD_INDEX", index),
        ];
        let command = runner::printed(&runner::run(&["nextest", "linux"], &env));
        let tokens: Vec<&str> = command.split_whitespace().collect();
        assert_eq!(
            runner::values_after(&tokens, "--partition"),
            vec![partition],
            "shard {index} of 21 must run partition {partition}"
        );
    }
    let command = runner::printed(&runner::run(&["nextest", "linux"], &[]));
    assert!(
        !command.contains("--partition"),
        "with no shard count, one run must take every test: {command}"
    );
}

#[cfg(unix)]
#[test]
fn nextest_runs_serially_from_the_extracted_archive() {
    for class in ["linux", "linuxpart"] {
        let command = runner::printed(&runner::run(&["nextest", class], &[]));
        for part in [
            "cargo nextest run",
            "--binaries-metadata target/nextest/binaries-metadata.json",
            "--cargo-metadata target/nextest/cargo-metadata.json",
            "--test-threads 1",
            "--no-fail-fast",
        ] {
            assert!(
                command.contains(part),
                "`nextest {class}` must pass `{part}`: {command}"
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn only_linuxpart_runs_with_the_partitioned_layout() {
    let part = runner::printed(&runner::run(&["nextest", "linuxpart"], &[]));
    assert!(
        part.starts_with("env HARVEST_TEST_PARTITIONED=1 cargo nextest run"),
        "`nextest linuxpart` must set HARVEST_TEST_PARTITIONED=1: {part}"
    );
    let plain = runner::printed(&runner::run(&["nextest", "linux"], &[]));
    assert!(
        !plain.contains("HARVEST_TEST_PARTITIONED"),
        "`nextest linux` must run the default layout: {plain}"
    );
}

#[cfg(unix)]
#[test]
fn extract_unpacks_into_the_checkout() {
    let command = runner::printed(&runner::run(&["extract", "/tmp/a.tar.zst"], &[]));
    assert_eq!(
        command,
        "cargo nextest list --archive-file /tmp/a.tar.zst --workspace-remap . \
         --extract-to . --extract-overwrite --list-type binaries-only"
    );
}

#[cfg(unix)]
#[test]
fn a_filter_outside_the_safe_charset_is_refused() {
    let dir = tempfile::tempdir().expect("make a temp dir");
    let manifest = dir.path().join("suites.txt");
    std::fs::write(
        &manifest,
        "linux autumn-harvest integration - good_tests\n\
         linux autumn-harvest integration - bad|tests\n",
    )
    .expect("write the manifest");
    let manifest = manifest.to_str().expect("a UTF-8 path");
    for args in [&["filterset", "linux"][..], &["nextest", "linux"][..]] {
        let out = runner::run(args, &[("SUITES_MANIFEST", manifest)]);
        assert!(
            !out.status.success(),
            "{args:?} must refuse a filter that the filterset grammar would read"
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("unsupported manifest filter: bad|tests"),
            "{args:?} must name the refused filter"
        );
    }
}

// ── The workflow wiring ─────────────────────────────────────────────────────

/// The steps of `job` whose `run` mentions `needle`.
fn steps_running<'a>(job: &'a serde_yaml::Value, needle: &str) -> Vec<&'a serde_yaml::Value> {
    steps(job)
        .into_iter()
        .filter(|s| str_at(s, &["run"]).contains(needle))
        .collect()
}

/// The `with.tool` of the nextest install step of `job`.
fn nextest_tool(job: &serde_yaml::Value) -> String {
    let tools: Vec<&str> = steps(job)
        .into_iter()
        .filter(|s| str_at(s, &["uses"]).starts_with("taiki-e/install-action@"))
        .map(|s| str_at(s, &["with", "tool"]))
        .filter(|t| t.starts_with("cargo-nextest@"))
        .collect();
    assert_eq!(tools.len(), 1, "expected one nextest install step");
    tools[0].to_string()
}

/// True when `job` has a step that `uses` `action` with `with.name` equal to
/// the archive artifact.
fn moves_the_artifact(job: &serde_yaml::Value, action: &str) -> bool {
    steps(job).into_iter().any(|s| {
        str_at(s, &["uses"]).starts_with(action) && str_at(s, &["with", "name"]) == ARTIFACT
    })
}

#[test]
fn the_archive_job_builds_and_uploads_the_archive() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let job = &ci["jobs"]["test-db-archive"];
    assert_eq!(
        steps_running(job, "run-suites.sh archive").len(),
        1,
        "`test-db-archive` must build the archive once"
    );
    assert!(
        moves_the_artifact(job, "actions/upload-artifact@"),
        "`test-db-archive` must upload the `{ARTIFACT}` artifact"
    );
}

#[test]
fn every_shard_runs_both_live_db_classes_from_the_archive() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let shard = &ci["jobs"]["test-db-linux"];
    assert!(
        job_needs(shard).contains("test-db-archive"),
        "`test-db-linux` must wait for `test-db-archive`"
    );
    assert!(
        moves_the_artifact(shard, "actions/download-artifact@"),
        "`test-db-linux` must download the `{ARTIFACT}` artifact"
    );
    assert_eq!(steps_running(shard, "run-suites.sh extract").len(), 1);

    // An exact `run` line, because `nextest linux` is a prefix of
    // `nextest linuxpart`. Every shard runs both. A shard gate such as
    // `matrix.shard == 0` would drop the other slices of the partition.
    for class in ["linux", "linuxpart"] {
        let run = format!("bash .github/ci/run-suites.sh nextest {class}");
        let matching: Vec<_> = steps(shard)
            .into_iter()
            .filter(|s| str_at(s, &["run"]).trim() == run)
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "`test-db-linux` must have one `{run}` step"
        );
        assert_eq!(
            str_at(matching[0], &["if"]),
            CODE_GATE,
            "the `{run}` step must run on every shard of a code change"
        );
    }
    assert!(
        steps_running(shard, "run-suites.sh run linux").is_empty(),
        "a shard must not compile its rows again with `run linux`"
    );
}

#[test]
fn both_ends_of_the_archive_use_one_nextest_version() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    assert_eq!(
        nextest_tool(&ci["jobs"]["test-db-archive"]),
        nextest_tool(&ci["jobs"]["test-db-linux"]),
        "the archive format is internal to nextest, so both jobs must pin one version"
    );
}

// ── The image pre-pull on a hash-partitioned shard ──────────────────────────

/// The emulator images that only some suites need.
const EMULATORS: &[&str] = &[
    "softwaremill/elasticmq:",
    "pgsty/minio:",
    "fsouza/fake-gcs-server:",
];

/// A hash partition sends the tests of each emulator suite to many shards.
/// The row plan of `pull-test-images.sh` cannot name those shards, so each
/// shard must pull every emulator.
#[test]
fn every_shard_pulls_every_emulator_image() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let pulls = steps_running(&ci["jobs"]["test-db-linux"], "pull-test-images.sh");
    assert_eq!(pulls.len(), 1, "`test-db-linux` must pre-pull its images");
    assert_eq!(
        str_at(pulls[0], &["env", "PULL_EVERY_SUITE_IMAGE"]),
        "1",
        "the pre-pull step must set PULL_EVERY_SUITE_IMAGE=1"
    );
}

/// Runs `pull-test-images.sh` with a `docker` stub that prints each pull.
#[cfg(unix)]
fn pulled_images(env: &[(&str, &str)]) -> String {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("make a temp dir");
    let stub = dir.path().join("docker");
    std::fs::write(
        &stub,
        "#!/usr/bin/env bash\n[ \"$1\" = pull ] && echo \"PULL $2\"\nexit 0\n",
    )
    .expect("write the docker stub");
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755))
        .expect("make the stub executable");
    let path = format!(
        "{}:{}",
        dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join(".github/ci/pull-test-images.sh"))
        .current_dir(repo_root())
        .env("PATH", path)
        .env_remove("PULL_EVERY_SUITE_IMAGE")
        .env_remove("SEMAPHORE_SHARD_COUNT")
        .env_remove("SEMAPHORE_SHARD_INDEX");
    for (key, value) in env {
        cmd.env(key, value);
    }
    let out = cmd.output().expect("start bash");
    assert!(
        out.status.success(),
        "the pre-pull failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The first shard of 21 whose rows hold no emulator suite under the row plan.
///
/// The coverage nightly keeps the row plan, so such a shard pulls no
/// emulator there.
#[cfg(unix)]
fn shard_without_emulator_rows() -> String {
    (0..21)
        .map(|i| i.to_string())
        .find(|index| {
            let pulled = pulled_images(&[
                ("SEMAPHORE_SHARD_COUNT", "21"),
                ("SEMAPHORE_SHARD_INDEX", index),
            ]);
            EMULATORS.iter().all(|e| !pulled.contains(e))
        })
        .expect("under the row plan, some shard of 21 must pull no emulator image")
}

/// On that same shard, `PULL_EVERY_SUITE_IMAGE=1` still pulls each emulator.
#[cfg(unix)]
#[test]
fn pull_every_suite_image_pulls_each_emulator_on_any_shard() {
    let index = shard_without_emulator_rows();
    let pulled = pulled_images(&[
        ("SEMAPHORE_SHARD_COUNT", "21"),
        ("SEMAPHORE_SHARD_INDEX", &index),
        ("PULL_EVERY_SUITE_IMAGE", "1"),
    ]);
    for image in EMULATORS {
        assert!(
            pulled.contains(&format!("PULL {image}")),
            "shard {index} with PULL_EVERY_SUITE_IMAGE=1 must pull {image}: {pulled}"
        );
    }
}
