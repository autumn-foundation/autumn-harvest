//! Guards for the sandboxed code activity docs.
//!
//! `docs/sandbox-activities.md` is the guide, and
//! `DESIGN-sandbox-activities.md` is the design record. These guards read the
//! sources and check that the docs agree with them:
//!
//! * The guide has one failure row per sandbox error type, and each retry
//!   column matches the doc comment of the constant.
//! * The guide names the patched Firecracker minimums that the posture check
//!   enforces, and the kernel arguments that the config sets.
//! * Each acceptance criterion of the design record has evidence, and each
//!   test that the evidence cites exists.
//! * Each test command of the guide runs in CI.
//! * Each prose sentence of both pages has 25 words or fewer.
//!
//! Pure: no database, no async, no feature gate. Runs on every OS.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::docs_guard_support::{check_stanza, prose_sentences, section, step_stanza};

const GUIDE: &str = "docs/sandbox-activities.md";
const IMAGE_SCRIPT: &str = "scripts/sandbox/build-guest-image.sh";
const PUBLISH_SCRIPT: &str = "scripts/sandbox/publish-image.sh";
const DESIGN: &str = "DESIGN-sandbox-activities.md";
const FILTER: &str = "--test integration sandbox_activities_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// The sources whose test functions the design record can cite.
const TEST_SOURCES: &[&str] = &[
    "autumn-harvest/src/sandbox",
    "autumn-harvest-agent/src/sandbox.rs",
    "autumn-harvest-sandbox-guest/src",
    "autumn-harvest/tests/integration/sandbox_activities_docs.rs",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

/// Each `.rs` file under `relative`, or the file itself.
fn rust_files(relative: &str) -> Vec<PathBuf> {
    let mut pending = vec![repo_root().join(relative)];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            let entries = std::fs::read_dir(&path)
                .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));
            pending.extend(entries.map(|e| e.expect("readable dir entry").path()));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            files.push(path);
        }
    }
    files
}

/// The names of the functions in the test sources.
fn function_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for source in TEST_SOURCES {
        for file in rust_files(source) {
            let text = std::fs::read_to_string(&file).expect("readable source");
            for line in text.lines().map(str::trim_start) {
                let line = line.strip_prefix("async ").unwrap_or(line);
                if let Some(rest) = line.strip_prefix("fn ") {
                    names.insert(
                        rest.chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect(),
                    );
                }
            }
        }
    }
    names
}

/// The backticked names in `text` that look like test functions: snake case
/// with at least three words.
fn cited_tests(text: &str) -> BTreeSet<String> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .filter(|name| {
            name.matches('_').count() >= 2
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
        .map(str::to_owned)
        .collect()
}

/// `(error type, retryable)` for each `ERROR_TYPE_SANDBOX_*` constant, read
/// from the doc comment above it.
fn sandbox_error_types() -> Vec<(String, bool)> {
    let source = read("autumn-harvest/src/failure.rs");
    let lines: Vec<&str> = source.lines().collect();
    let mut types = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let Some(rest) = line.strip_prefix("pub const ERROR_TYPE_SANDBOX_") else {
            continue;
        };
        let value = rest
            .split('"')
            .nth(1)
            .unwrap_or_else(|| panic!("cannot parse `{line}`"))
            .to_owned();
        let doc: String = lines[..index]
            .iter()
            .rev()
            .take_while(|l| l.starts_with("///") || l.starts_with("#["))
            .map(|l| l.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
        let retryable = !doc.contains("non-retryable");
        assert!(
            doc.contains("retryable"),
            "the doc comment of {value} must say whether it is retryable"
        );
        types.push((value, retryable));
    }
    types
}

fn assert_short_sentences(page: &str) {
    let text = read(page);
    let long: Vec<String> = prose_sentences(&text)
        .into_iter()
        .filter(|s| s.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{page} has sentences over {MAX_SENTENCE_WORDS} words:\n  {}",
        long.join("\n  ")
    );
}

#[test]
fn the_guide_has_a_failure_row_per_error_type() {
    let guide = read(GUIDE);
    let failures = section(&guide, "## Failures", "\n## ").expect("the guide has `## Failures`");
    let types = sandbox_error_types();
    assert_eq!(types.len(), 4, "four sandbox error types: {types:?}");
    for (name, retryable) in types {
        let row = failures
            .lines()
            .find(|line| line.starts_with(&format!("| `{name}` |")))
            .unwrap_or_else(|| panic!("{GUIDE} has no failure row for `{name}`"));
        let retry = row.split('|').nth(2).map(str::trim);
        let expected = if retryable { "Yes" } else { "No" };
        assert_eq!(retry, Some(expected), "the retry column of `{name}`");
    }
}

#[test]
fn the_guide_names_the_enforced_firecracker_minimums() {
    let guide = read(GUIDE);
    let posture = read("autumn-harvest/src/sandbox/firecracker/posture.rs");
    for version in ["1.14.4", "1.15.1"] {
        assert!(guide.contains(version), "{GUIDE} must name {version}");
        assert!(
            posture.contains(version),
            "posture.rs must enforce {version}"
        );
    }
    for cve in ["CVE-2026-5747", "CVE-2026-1386"] {
        assert!(guide.contains(cve), "{GUIDE} must name {cve}");
        assert!(posture.contains(cve), "posture.rs must name {cve}");
    }
}

#[test]
fn a_fakeroot_image_gets_root_owners_on_the_host_too() {
    // A jailed launcher trusts only image files that root owns. Fakeroot
    // gives root the files in the image, not the image files themselves.
    let install = "sudo install -o root -g root -m 0644";
    let guide = read(GUIDE);
    let script = read(IMAGE_SCRIPT);
    assert!(guide.contains(install), "{GUIDE} must show `{install}`");
    assert!(
        script.contains(install),
        "build-guest-image.sh must print `{install}` after a non-root build"
    );
}

#[test]
fn the_image_script_pins_its_default_kernel_by_digest() {
    // The kernel runs untrusted code, so the script checks each download.
    let script = read(IMAGE_SCRIPT);
    let digest = script
        .lines()
        .find_map(|line| line.strip_prefix("default_kernel_sha256=\""))
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or_else(|| panic!("{IMAGE_SCRIPT} must set default_kernel_sha256"));
    assert_eq!(digest.len(), 64, "{digest}");
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()), "{digest}");
    assert!(
        !script.contains("if [ -n \"${KERNEL_SHA256:-}\" ]"),
        "{IMAGE_SCRIPT} must check the kernel digest on every path"
    );
}

#[cfg(unix)]
#[test]
fn the_image_script_refuses_a_kernel_url_without_a_digest() {
    let out = std::env::temp_dir().join(format!("hsbx-image-{}", std::process::id()));
    let result = std::process::Command::new("bash")
        .arg(repo_root().join(IMAGE_SCRIPT))
        .arg(&out)
        .env("KERNEL_URL", "file:///nonexistent/vmlinux")
        .env_remove("KERNEL_SHA256")
        .env("AGENT_BIN", "/nonexistent/harvest-guest-agent")
        .output()
        .expect("bash runs");
    let _ = std::fs::remove_dir_all(&out);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(!result.status.success(), "{stderr}");
    assert!(stderr.contains("KERNEL_SHA256"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn a_failed_image_build_keeps_the_old_image() {
    use sha2::Digest as _;
    let scratch = std::env::temp_dir().join(format!("hsbx-rebuild-{}", std::process::id()));
    let out = scratch.join("image");
    std::fs::create_dir_all(&out).expect("out dir");
    std::fs::write(out.join("vmlinux"), b"old kernel").expect("old kernel");
    std::fs::write(out.join("rootfs.ext4"), b"old rootfs").expect("old rootfs");
    let kernel = scratch.join("new-kernel");
    std::fs::write(&kernel, b"new kernel").expect("new kernel");
    let digest = sha2::Sha256::digest(b"new kernel")
        .iter()
        .fold(String::new(), |mut hex, b| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
            hex
        });
    let mut cases = vec![(
        "a kernel digest mismatch",
        "0".repeat(64),
        "python:3.12-alpine".to_owned(),
    )];
    // The kernel is good, and the root file system step fails.
    cases.push((
        "a failed root file system",
        digest,
        "harvest.invalid/!!".to_owned(),
    ));
    for (case, sha, base) in cases {
        let result = std::process::Command::new("bash")
            .arg(repo_root().join(IMAGE_SCRIPT))
            .arg(&out)
            .env("KERNEL_URL", format!("file://{}", kernel.display()))
            .env("KERNEL_SHA256", sha)
            .env("BASE_IMAGE", base)
            .env("AGENT_BIN", "/nonexistent/harvest-guest-agent")
            .output()
            .expect("bash runs");
        assert!(!result.status.success(), "{case}: the build must fail");
        let kept = |name: &str| std::fs::read(out.join(name)).unwrap_or_default();
        assert_eq!(kept("vmlinux"), b"old kernel", "{case}");
        assert_eq!(kept("rootfs.ext4"), b"old rootfs", "{case}");
        let mut left: Vec<_> = std::fs::read_dir(&out)
            .expect("out dir")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        left.sort();
        assert_eq!(
            left,
            ["rootfs.ext4", "vmlinux"],
            "{case}: a staged file is left"
        );
    }
    let _ = std::fs::remove_dir_all(&scratch);
}

#[cfg(target_os = "linux")]
#[test]
fn a_failed_publish_puts_back_the_old_image() {
    const FILES: [&str; 3] = ["rootfs.ext4", "vmlinux", "image.sha256"];
    let scratch = std::env::temp_dir().join(format!("hsbx-publish-{}", std::process::id()));
    let publish = |stage: &Path, out: &Path| {
        std::process::Command::new("bash")
            .arg(repo_root().join(PUBLISH_SCRIPT))
            .arg(stage)
            .arg(out)
            .output()
            .expect("bash runs")
    };
    let setup = |case: &str, staged: &[&str]| {
        let (stage, out) = (
            scratch.join(case).join("stage"),
            scratch.join(case).join("out"),
        );
        std::fs::create_dir_all(&stage).expect("stage dir");
        std::fs::create_dir_all(&out).expect("out dir");
        for file in FILES {
            std::fs::write(out.join(file), format!("old {file}")).expect("old file");
        }
        for file in staged {
            std::fs::write(stage.join(file), format!("new {file}")).expect("new file");
        }
        (stage, out)
    };
    let names = |dir: &Path| {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .expect("dir")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("name")
            })
            .collect();
        names.sort();
        names
    };
    let mut sorted = FILES.map(str::to_owned).to_vec();
    sorted.sort();

    // The last file is missing, so its move fails after two moves.
    let (stage, out) = setup("missing", &FILES[..2]);
    let result = publish(&stage, &out);
    assert!(!result.status.success(), "a missing staged file must fail");
    for file in FILES {
        let kept = std::fs::read_to_string(out.join(file)).unwrap_or_default();
        assert_eq!(kept, format!("old {file}"), "{file} must be the old one");
    }
    assert_eq!(names(&out), sorted, "a backup or staged file is left");

    // A complete stage replaces each file.
    let (stage, out) = setup("complete", &FILES);
    let result = publish(&stage, &out);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    for file in FILES {
        let new = std::fs::read_to_string(out.join(file)).unwrap_or_default();
        assert_eq!(new, format!("new {file}"));
    }
    assert_eq!(names(&out), sorted, "a backup is left");
    let _ = std::fs::remove_dir_all(&scratch);
}

#[cfg(target_os = "linux")]
#[test]
fn image_links_cannot_take_a_build_write_out_of_the_image() {
    use std::os::unix::fs::symlink;
    let root = std::env::temp_dir().join(format!("hsbx-image-path-{}", std::process::id()));
    std::fs::create_dir_all(root.join("usr/sbin")).expect("usr/sbin");
    std::fs::create_dir_all(root.join("etc")).expect("etc");
    let helper = repo_root().join("scripts/sandbox/image-path.sh");
    let resolve = |path: &str| {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(". \"$1\"; image_path \"$2\" \"$3\"")
            .arg("sh")
            .arg(&helper)
            .arg(&root)
            .arg(path)
            .output()
            .expect("sh runs")
    };
    let resolved = |path: &str| {
        let out = resolve(path);
        assert!(out.status.success(), "{path}: {out:?}");
        String::from_utf8(out.stdout).expect("utf-8").trim().to_owned()
    };
    let inside = |rel: &str| root.join(rel).display().to_string();
    // An absolute link starts again at the image root, and `..` stops there.
    symlink("/etc", root.join("abs")).expect("abs link");
    assert_eq!(resolved("/abs/x"), inside("etc/x"));
    symlink("../../../../../../etc", root.join("up")).expect("up link");
    assert_eq!(resolved("/up"), inside("etc"));
    // A merged `/usr` link stays inside the image.
    symlink("usr/sbin", root.join("sbin")).expect("sbin link");
    assert_eq!(resolved("/sbin"), inside("usr/sbin"));
    assert_eq!(resolved("/work"), inside("work"));
    symlink("loop", root.join("loop")).expect("loop link");
    assert!(!resolve("/loop").status.success(), "a link loop must fail");
    // The build writes the agent and its directories through the helper.
    let script = read("scripts/sandbox/build-guest-image.sh");
    assert!(script.contains("image_path \"${rootfs}\" /sbin"), "{script}");
    assert!(!script.contains("install -D"), "install -D follows image links");
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(target_os = "linux")]
#[test]
fn a_publish_waits_for_a_launcher_that_pins() {
    use std::io::BufRead as _;
    const FILES: [&str; 3] = ["rootfs.ext4", "vmlinux", "image.sha256"];
    let scratch = std::env::temp_dir().join(format!("hsbx-publish-lock-{}", std::process::id()));
    let (stage, out) = (scratch.join("stage"), scratch.join("out"));
    std::fs::create_dir_all(&stage).expect("stage dir");
    std::fs::create_dir_all(&out).expect("out dir");
    for file in FILES {
        std::fs::write(out.join(file), format!("old {file}")).expect("old file");
        std::fs::write(stage.join(file), format!("new {file}")).expect("new file");
    }
    // A launcher pins under a shared lock of the image directory.
    let mut pin = std::process::Command::new("flock")
        .arg("--shared")
        .arg(&out)
        .args(["sh", "-c", "echo held; read _"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("flock runs");
    let mut held = String::new();
    std::io::BufReader::new(pin.stdout.take().expect("stdout"))
        .read_line(&mut held)
        .expect("the pin reports its lock");
    assert_eq!(held.trim(), "held");
    let mut publish = std::process::Command::new("bash")
        .arg(repo_root().join(PUBLISH_SCRIPT))
        .arg(&stage)
        .arg(&out)
        .spawn()
        .expect("bash runs");
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(
        publish.try_wait().expect("publish state").is_none(),
        "the publish did not wait for the pin"
    );
    for file in FILES {
        let kept = std::fs::read_to_string(out.join(file)).unwrap_or_default();
        assert_eq!(kept, format!("old {file}"), "{file} moved during a pin");
    }
    drop(pin.stdin.take());
    pin.wait().expect("the pin ends");
    assert!(publish.wait().expect("the publish ends").success());
    for file in FILES {
        let new = std::fs::read_to_string(out.join(file)).unwrap_or_default();
        assert_eq!(new, format!("new {file}"));
    }
    let _ = std::fs::remove_dir_all(&scratch);
}

#[test]
fn the_guide_names_the_kernel_arguments_that_the_config_sets() {
    let guide = read(GUIDE);
    let config = read("autumn-harvest/src/sandbox/firecracker/config.rs");
    for arg in ["reboot=k", "panic=1", "8250.nr_uarts=0"] {
        assert!(guide.contains(arg), "{GUIDE} must name {arg}");
        assert!(
            config.contains(&format!("\"{arg}\"")),
            "config.rs must set {arg}"
        );
    }
}

#[test]
fn each_acceptance_criterion_has_evidence_that_exists() {
    let design = read(DESIGN);
    let criteria = section(&design, "## 1. Acceptance criteria", "\n## ")
        .expect("the design record has `## 1. Acceptance criteria`");
    let evidence = section(&design, "## 3. Evidence", "\n## ")
        .expect("the design record has `## 3. Evidence`");
    let ids: Vec<String> = criteria
        .lines()
        .filter_map(|line| line.strip_prefix("| AC"))
        .map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
        })
        .filter(|number| !number.is_empty())
        .map(|number| format!("AC{number}"))
        .collect();
    assert!(ids.len() >= 20, "the record lists the criteria: {ids:?}");
    let functions = function_names();
    for id in ids {
        let row = evidence
            .lines()
            .find(|line| line.starts_with(&format!("| {id} |")))
            .unwrap_or_else(|| panic!("{DESIGN} has no evidence row for {id}"));
        let cited = cited_tests(row);
        assert!(!cited.is_empty(), "{id} cites no test: {row}");
        let missing: Vec<&String> = cited.difference(&functions).collect();
        assert!(
            missing.is_empty(),
            "{id} cites tests that do not exist: {missing:?}"
        );
    }
}

#[test]
fn each_guide_test_command_runs_in_ci() {
    let guide = read(GUIDE);
    let testing = section(&guide, "## Testing", "\n## ").expect("the guide has `## Testing`");
    let workflow = read(".github/workflows/ci.yml");
    let commands: Vec<&str> = testing
        .lines()
        .filter(|line| line.starts_with("cargo test"))
        .collect();
    assert!(commands.len() >= 3, "the guide lists the test commands");
    for command in commands {
        assert!(
            workflow.contains(command),
            "ci.yml must run the guide command `{command}`"
        );
    }
}

#[test]
fn the_pages_keep_sentences_short() {
    assert_short_sentences(GUIDE);
    assert_short_sentences(DESIGN);
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!("the `lint` job in ci.yml must run these guards with the filter `{FILTER}`")
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(
        cited_tests("see `a_long_test_name`, `Short`, `two_words` and `x.rs`"),
        BTreeSet::from(["a_long_test_name".to_owned()])
    );
    let types = sandbox_error_types();
    assert!(types.contains(&("SandboxDenied".to_owned(), false)));
    assert!(types.contains(&("SandboxUnavailable".to_owned(), true)));
}
