//! `cargo xtask check otel-workflow-semconv`: run the upstream
//! OpenTelemetry policy checks on the workflow model (issue #2015).
//! Also check the draft against the files of the upstream process.
//!
//! This is the port of `scripts/check-otel-workflow-semconv.sh`.
//!
//! The command mirrors the `make check-policies` recipe of the upstream
//! `semantic-conventions` repository. It pins one upstream commit. It reads
//! the Weaver image and the policy package pins from that commit. The
//! container runs with `--network none` and reads local mounts only.
//!
//! The command runs Weaver two times:
//!
//! 1. On the model at its upstream path, `model/workflow`. It must pass, and
//!    Weaver must report no warning for the model.
//! 2. On a negative control, a copy with a namespace collision. It must fail
//!    on the upstream naming policy. This proves that run 1 read the model
//!    and ran the package policies.
//!
//! Before Weaver, the command checks the files of the upstream process:
//!
//! - The upstream issue form for new conventions has the area and the field
//!   that section 9 of the draft names.
//! - The project proposal has each required section of the community
//!   `project-template.md`, at a pinned community commit. It keeps none of
//!   the guidance text of the template. A copy without one section must
//!   fail.
//!
//! `SEMCONV_LATEST=1` replaces the pins with upstream `main`, the latest
//! release and community `main`. A weekly job uses it to find upstream
//! changes before a pin moves.
//!
//! `WEAVER_REGISTRY` names the image registry. The default is
//! `mirror.gcr.io`, a Docker Hub mirror. The digest pins the content.
//!
//! The module keeps its helpers to itself. `otel-semconv.yml` runs when this
//! file changes, so a change to a helper here starts that workflow.

use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as Process, ExitCode, ExitStatus, Stdio};

use regex::bytes::Regex;
use smop::prelude::*;

use crate::util::repo_path;

/// Upstream `main` on 2026-10-15.
///
/// To move the pins, change these values and [`COMMUNITY_COMMIT`], and run
/// this command. Then update sections 9 and 10 of
/// `docs/otel-workflow-semconv/README.md`.
const SEMCONV_COMMIT: &str = "331541c4057c53d133c32efadaa7b656636eda77";
/// The latest upstream release on the day of [`SEMCONV_COMMIT`].
const BASELINE_TAG: &str = "v1.44.0";
const SEMCONV_REPO: &str = "https://github.com/open-telemetry/semantic-conventions.git";
/// Community `main` on 2026-10-18. Move it with the pins above.
const COMMUNITY_COMMIT: &str = "9c30b48354004ef818bcf3661ece2b32ca4dd787";
const COMMUNITY_REPO: &str = "https://github.com/open-telemetry/community.git";
const DEFAULT_REGISTRY: &str = "mirror.gcr.io";

/// The model, as a path from the repository root.
const MODEL: &str = "docs/otel-workflow-semconv/model/workflow";
/// The project proposal, as a path from the repository root.
const PROPOSAL: &str = "docs/otel-workflow-semconv/project-proposal.md";

/// The upstream issue form for new conventions.
const ISSUE_FORM: &str = ".github/ISSUE_TEMPLATE/new-conventions.yaml";
/// The lines of the issue form that section 9 of the draft names.
const FORM_LINES: [&str; 2] = ["- area:new", "label: Propose new conventions"];

/// The upstream file that pins the Weaver image.
const DOCKERFILE: &str = "dependencies.Dockerfile";
/// The upstream file that pins the policy packages.
const MAKEFILE: &str = "Makefile";
/// The Makefile assignment of the policy packages.
const PACKAGES_PREFIX: &str = "WEAVER_PACKAGES_REPO=";

/// The package policies of `make check-policies`.
const PACKAGE_POLICIES: [&str; 4] = [
    "--policy=/home/weaver/packages/policies/check/naming_conventions",
    "--policy=/home/weaver/packages/policies/check/stability",
    "--policy=/home/weaver/packages/policies/check/entity_associations",
    "--policy=/home/weaver/packages/policies/check/backwards-compatibility",
];

/// A finding of Weaver for the model, in a log with no line breaks.
const MODEL_FINDING: &str = r"weaver/source/workflow/|span\.workflow\.|registry\.workflow";
/// The upstream naming policy that the negative control must break.
const COLLISION: &str = "naming_convention_attribute_namespace_collision";

/// The entry that the negative control appends to the registry.
///
/// The attributes list ends the registry, so an appended entry joins it.
/// `workflow.signal.id.kind` makes `workflow.signal.id` a namespace.
const COLLISION_ENTRY: &str = "      - id: workflow.signal.id.kind
        type: string
        stability: development
        brief: A negative control.
        examples: [\"x\"]
";

/// Arguments of `cargo xtask check otel-workflow-semconv`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask check otel-workflow-semconv`.
pub fn run(_args: Args) -> Result<ExitCode> {
    match check() {
        Err(error) => match error.downcast_ref::<Exit>() {
            Some(Exit(code)) => Ok(ExitCode::from(*code)),
            None => Err(error),
        },
        ok => ok,
    }
}

/// An exit with this code. The script ran under `set -e`, so a failed
/// step ended it with the exit code of that step.
#[derive(Debug)]
struct Exit(u8);

impl fmt::Display for Exit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exit {}", self.0)
    }
}

impl std::error::Error for Exit {}

/// Prints `message` to stderr and ends the command with exit code 1.
fn fail<T>(message: impl fmt::Display) -> Result<T> {
    eprintln!("{message}");
    Err(Exit(1).into())
}

/// The exit code of `status`, as a shell reports it.
#[cfg_attr(
    not(unix),
    allow(
        clippy::unnecessary_lazy_evaluations,
        reason = "only the Unix closure reads the status"
    )
)]
fn exit_of(status: ExitStatus) -> Exit {
    let code = status.code().or_else(|| {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            status.signal().map(|signal| 128 + signal)
        }
        #[cfg(not(unix))]
        {
            None
        }
    });
    Exit(code.and_then(|c| u8::try_from(c).ok()).unwrap_or(1))
}

/// Runs one step with inherited stdio. A failed step ends the command.
fn step(command: sh::CommandBuilder) -> Result<()> {
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(exit_of(status).into())
    }
}

/// Runs one step and returns its stdout. Its stderr goes to stderr.
fn capture(command: sh::CommandBuilder) -> Result<String> {
    let output = command.output_raw()?;
    std::io::stderr().write_all(&output.stderr)?;
    if !output.status.success() {
        return Err(exit_of(output.status).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Strips the final newlines, as a shell command substitution does.
fn substitution(text: &str) -> &str {
    text.trim_end_matches('\n')
}

/// The first tab field of each line, as `cut -f1` prints it.
fn first_fields(text: &str) -> String {
    let fields: Vec<&str> = lines(text)
        .into_iter()
        .map(|line| line.split('\t').next().unwrap_or_default())
        .collect();
    fields.join("\n")
}

/// The commit of `main` of `repo`, as `git ls-remote | cut -f1` prints it.
fn main_commit(repo: &str) -> Result<String> {
    let refs = capture(sh::cmd("git").args(["ls-remote", repo, "refs/heads/main"]))?;
    Ok(substitution(&first_fields(&refs)).to_string())
}

/// The newest `v*` tag of `repo` in the order of `sort -V`.
fn latest_tag(repo: &str) -> Result<String> {
    let refs = capture(sh::cmd("git").args(["ls-remote", "--tags", "--refs", repo, "v*"]))?;
    // `sed 's#.*refs/tags/##'` drops all text up to the last `refs/tags/`.
    let mut tags = String::new();
    for line in lines(substitution(&refs)) {
        if !line.is_empty() {
            tags.push_str(line.rfind("refs/tags/").map_or(line, |at| &line[at + 10..]));
            tags.push('\n');
        }
    }
    // GNU `sort -V` has its own order. Run it, so the order is the same.
    let mut sort = Process::new("sort")
        .arg("-V")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("Failed to execute command: sort -V")?;
    sort.stdin
        .take()
        .context("sort has a stdin")?
        .write_all(tags.as_bytes())?;
    let output = sort.wait_with_output()?;
    if !output.status.success() {
        return Err(exit_of(output.status).into());
    }
    let sorted = String::from_utf8_lossy(&output.stdout);
    Ok(sorted.lines().last().unwrap_or_default().to_string())
}

/// A shallow clone of one ref of `repo` into `dir`.
fn clone_at(repo: &str, reference: &str, dir: &Path) -> Result<()> {
    step(sh::cmd("git").args(["init", "-q"]).arg(dir))?;
    step(
        sh::cmd("git")
            .arg("-C")
            .arg(dir)
            .args(["fetch", "-q", "--depth", "1", repo, reference]),
    )?;
    step(sh::cmd("git").arg("-C").arg(dir).args([
        "-c",
        "advice.detachedHead=false",
        "checkout",
        "-q",
        "FETCH_HEAD",
    ]))
}

/// True for a character of the POSIX class `[[:space:]]`.
const fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{b}' | '\u{c}' | '\r')
}

/// The lines of `text` as `grep` reads them.
fn lines(text: &str) -> Vec<&str> {
    text.strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .collect()
}

/// True when the form holds `line` as a whole line, with optional spaces.
///
/// This is `grep -qxE "[[:space:]]*${line}[[:space:]]*"`.
fn form_has(form: &str, line: &str) -> bool {
    lines(form).iter().any(|l| l.trim_matches(is_space) == line)
}

/// The required sections of the template.
///
/// A required section is a heading that is not Optional or Post-Approval.
fn required_sections(template: &str) -> Result<Vec<String>> {
    let heading = regex::Regex::new(r"^#{2,4} ")?;
    let optional = regex::Regex::new(r"\((Optional|Post-Approval)\)$")?;
    if template.is_empty() {
        return Ok(Vec::new());
    }
    Ok(lines(template)
        .into_iter()
        .filter(|l| heading.is_match(l) && !optional.is_match(l))
        .map(str::to_string)
        .collect())
}

/// Drops a final `\r` and the white space at the end of a line.
///
/// This is `sed 's/\r$//; s/[[:space:]]*$//'`.
fn clean_line(line: &str) -> &str {
    line.strip_suffix('\r')
        .unwrap_or(line)
        .trim_end_matches(is_space)
}

/// The guidance text: each template line that is not a heading or blank.
fn guidance(template: &str) -> Vec<String> {
    if template.is_empty() {
        return Vec::new();
    }
    lines(template)
        .into_iter()
        .filter(|l| !l.starts_with('#') && !l.chars().all(is_space))
        .map(|l| clean_line(l).to_string())
        .collect()
}

/// The cleaned lines of a file, as `$(sed ...)` holds them.
///
/// The command substitution drops the final empty lines.
fn clean_lines(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<String> = lines(text)
        .into_iter()
        .map(|l| clean_line(l).to_string())
        .collect();
    while out.last().is_some_and(String::is_empty) {
        out.pop();
    }
    out
}

/// Each required section that `clean` lacks, then each guidance line that
/// `clean` keeps.
fn missing_sections(clean: &[String], required: &[String], guidance: &[String]) -> Vec<String> {
    let lacks = required.iter().filter(|r| !clean.contains(r)).cloned();
    let keeps = clean.iter().filter(|l| guidance.contains(l)).cloned();
    lacks.chain(keeps).collect()
}

/// The Weaver image that the Dockerfile pins, from `awk '$4 == "weaver"'`.
fn weaver_image(dockerfile: &str) -> String {
    let images: Vec<&str> = lines(dockerfile)
        .into_iter()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split([' ', '\t']).filter(|f| !f.is_empty()).collect();
            (fields.get(3) == Some(&"weaver")).then(|| fields.get(1).copied().unwrap_or_default())
        })
        .collect();
    images.join("\n")
}

/// The policy package pin of the Makefile, with all white space removed.
fn packages_pin(makefile: &str) -> String {
    lines(makefile)
        .into_iter()
        .filter_map(|line| line.strip_prefix(PACKAGES_PREFIX))
        .flat_map(str::chars)
        .filter(|c| !is_space(*c))
        .collect()
}

/// Splits a pin `<repo>@<ref>` at its last `@`.
///
/// A pin with no `@` gives itself as both parts, as the shell expansions do.
fn split_pin(pin: &str) -> (&str, &str) {
    pin.rfind('@')
        .map_or((pin, pin), |at| (&pin[..at], &pin[at + 1..]))
}

/// True when `reference` is a full commit SHA.
fn is_commit(reference: &str) -> bool {
    reference.len() == 40
        && reference
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A log with its line breaks, spaces and box rules removed.
///
/// Weaver wraps long lines, so the search joins them first. This is
/// `tr -d '\n │'`, which deletes each byte of the UTF-8 rule.
fn joined(log: &[u8]) -> Vec<u8> {
    log.iter()
        .copied()
        .filter(|b| !matches!(b, b'\n' | b' ' | 0xe2 | 0x94 | 0x82))
        .collect()
}

/// Reads a file that the script read with `grep`, `sed` or `awk`.
///
/// A missing file gives the error of that tool on stderr, and `None`.
fn read_or_report(tool: &str, path: &Path) -> Option<String> {
    match std::fs::read(path) {
        Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        Err(error) => {
            let text = error.to_string();
            let reason = text
                .rfind(" (os error ")
                .map_or(text.as_str(), |at| &text[..at]);
            let path = path.display();
            match tool {
                "sed" => eprintln!("sed: can't read {path}: {reason}"),
                "awk" => eprintln!("awk: cannot open \"{path}\" ({reason})"),
                _ => eprintln!("{tool}: {path}: {reason}"),
            }
            None
        }
    }
}

/// The trees and the image that the Weaver runs use.
struct Weaver {
    work: PathBuf,
    semconv: PathBuf,
    image: String,
    config: bool,
    user: String,
}

impl Weaver {
    /// Puts the model at `dir` at its upstream path.
    fn place(&self, dir: &Path) -> Result<()> {
        let target = self.semconv.join("model/workflow");
        step(sh::cmd("rm").arg("-rf").arg(&target))?;
        step(sh::cmd("cp").arg("-R").arg(dir).arg(&target))
    }

    /// The `docker run` command of the upstream checks on the placed model.
    fn command(&self) -> Result<Process> {
        let cache = tempfile_dir(&self.work, "cache.")?;
        let semconv = self.semconv.display();
        let work = self.work.display();
        let mount = |source: String, target: &str| {
            format!("type=bind,source={source},target=/home/weaver/{target},readonly")
        };
        let mut command = Process::new("docker");
        command
            .args(["run", "--rm", "--network", "none", "-u", &self.user])
            .args(["--env", "USER=weaver", "--env", "HOME=/home/weaver"])
            .arg("-v")
            .arg(format!("{}:/home/weaver/.weaver", cache.display()))
            .args(["--mount", &mount(format!("{semconv}/policies"), "policies")])
            .args(["--mount", &mount(format!("{semconv}/model"), "source")])
            .args([
                "--mount",
                &mount(format!("{work}/baseline/model"), "baseline"),
            ])
            .args(["--mount", &mount(format!("{work}/packages"), "packages")]);
        // Upstream filters some known findings in `.weaver.toml`. Older
        // commits lack it.
        if self.config {
            command.args([
                "--mount",
                &mount(format!("{semconv}/.weaver.toml"), ".weaver.toml"),
            ]);
        }
        command.args([self.image.as_str(), "registry", "check", "--v2"]);
        if self.config {
            command.arg("--config=/home/weaver/.weaver.toml");
        }
        command
            .args(["--registry=/home/weaver/source"])
            .args(["--baseline-registry=/home/weaver/baseline"])
            .args(["--policy=/home/weaver/policies"])
            .args(PACKAGE_POLICIES);
        Ok(command)
    }
}

/// Makes a new directory in `parent` whose name starts with `prefix`.
fn tempfile_dir(parent: &Path, prefix: &str) -> Result<PathBuf> {
    for n in 0..10_000_u32 {
        let dir = parent.join(format!("{prefix}{:06}", std::process::id().wrapping_add(n)));
        match std::fs::create_dir(&dir) {
            Ok(()) => return Ok(dir),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error).with_context(|| format!("creating {}", dir.display())),
        }
    }
    bail!("no free cache directory in {}", parent.display())
}

/// Runs `command` with stdout and stderr on one pipe.
///
/// With `tee`, the output also goes to stdout as it arrives. The result
/// holds the exit status and a copy of the output.
fn run_merged(command: &mut Process, tee: bool) -> Result<(ExitStatus, Vec<u8>)> {
    let (mut reader, writer) = std::io::pipe().context("creating a pipe")?;
    let line = format!("{command:?}");
    let mut child = command
        .stdout(writer.try_clone()?)
        .stderr(writer)
        .spawn()
        .with_context(|| format!("Failed to execute command: {line}"))?;
    // The command keeps a copy of each pipe end. Drop them both, or the
    // read below never sees the end of the output.
    command.stdout(Stdio::null()).stderr(Stdio::null());
    let mut copy = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut out = std::io::stdout();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        if tee {
            out.write_all(&buffer[..n])?;
            out.flush()?;
        }
        copy.extend_from_slice(&buffer[..n]);
    }
    Ok((child.wait()?, copy))
}

/// Writes a log to stderr, as `cat log >&2` does.
fn show(log: &[u8]) -> Result<()> {
    let mut err = std::io::stderr();
    err.write_all(log)?;
    err.flush()?;
    Ok(())
}

/// Runs every check, and returns the exit code of the gate.
#[allow(clippy::too_many_lines)]
fn check() -> Result<ExitCode> {
    let model = repo_path(MODEL);
    let proposal = repo_path(PROPOSAL);
    let registry = std::env::var("WEAVER_REGISTRY")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_REGISTRY.to_string());

    if path::which("docker").is_err() {
        return fail("error: the script needs docker");
    }

    let latest = std::env::var("SEMCONV_LATEST").ok().as_deref() == Some("1");
    let (semconv_commit, baseline_tag, community_commit) = if latest {
        (
            main_commit(SEMCONV_REPO)?,
            latest_tag(SEMCONV_REPO)?,
            main_commit(COMMUNITY_REPO)?,
        )
    } else {
        (
            SEMCONV_COMMIT.to_string(),
            BASELINE_TAG.to_string(),
            COMMUNITY_COMMIT.to_string(),
        )
    };

    let temp = fs::temp_dir()?;
    let work = temp.path().to_path_buf();
    let semconv = work.join("semconv");
    clone_at(SEMCONV_REPO, &semconv_commit, &semconv)?;
    clone_at(
        SEMCONV_REPO,
        &format!("refs/tags/{baseline_tag}"),
        &work.join("baseline"),
    )?;
    clone_at(COMMUNITY_REPO, &community_commit, &work.join("community"))?;

    println!("Community {community_commit}");
    println!("Check: the issue form has the area and the field of section 9.");
    let form = read_or_report("grep", &semconv.join(ISSUE_FORM)).unwrap_or_default();
    for line in FORM_LINES {
        if !form_has(&form, line) {
            return fail(format!(
                "error: the upstream issue form lacks the line '{line}'"
            ));
        }
    }

    println!("Check: the proposal has each required section of the project template.");
    let Ok(proposal_bytes) = std::fs::read(&proposal) else {
        return fail(format!(
            "error: there is no project proposal at {}",
            proposal.display()
        ));
    };
    let template =
        read_or_report("grep", &work.join("community/project-template.md")).unwrap_or_default();
    let required = required_sections(&template)?;
    if required.is_empty() {
        return fail("error: the project template has no required section");
    }
    let guidance = guidance(&template);
    if guidance.is_empty() {
        // The script took the guidance from a pipeline under pipefail. With
        // no guidance line, grep failed and so did the script, with no text.
        return Err(Exit(1).into());
    }

    let proposal_text = String::from_utf8_lossy(&proposal_bytes);
    let clean = clean_lines(&proposal_text);
    let missing = missing_sections(&clean, &required, &guidance);
    if !missing.is_empty() {
        return fail(format!(
            "error: the project proposal lacks or keeps these lines:\n{}",
            missing.join("\n")
        ));
    }
    // A negative control: a copy without the first required section must fail.
    let first = &required[0];
    let broken: Vec<String> = clean.iter().filter(|l| *l != first).cloned().collect();
    if missing_sections(&broken, &required, &guidance).is_empty() {
        return fail(format!(
            "error: the section check passed a proposal without '{first}'"
        ));
    }

    // The script read each pin through a command substitution under `set -e`.
    // A missing file then ended it with the exit code 2 of awk or sed.
    let Some(dockerfile) = read_or_report("awk", &semconv.join(DOCKERFILE)) else {
        return Err(Exit(2).into());
    };
    let image = weaver_image(&dockerfile);
    if !image.contains("@sha256:") {
        return fail(format!(
            "error: upstream names the Weaver image '{image}' with no digest"
        ));
    }
    let image = format!("{registry}/{image}");

    let Some(makefile) = read_or_report("sed", &semconv.join(MAKEFILE)) else {
        return Err(Exit(2).into());
    };
    let packages = packages_pin(&makefile);
    let (packages_repo, packages_ref) = split_pin(&packages);
    if !is_commit(packages_ref) || packages_repo == packages {
        return fail(format!(
            "error: upstream does not pin WEAVER_PACKAGES_REPO to a commit: '{packages}'"
        ));
    }
    clone_at(packages_repo, packages_ref, &work.join("packages"))?;

    let user = format!(
        "{}:{}",
        substitution(&capture(sh::cmd("id").arg("-u"))?),
        substitution(&capture(sh::cmd("id").arg("-g"))?)
    );
    let weaver = Weaver {
        config: semconv.join(".weaver.toml").is_file(),
        work: work.clone(),
        semconv,
        image,
        user,
    };

    println!(
        "Upstream {semconv_commit}, baseline {baseline_tag}, image {}",
        weaver.image
    );
    println!("Run 1: the model must pass with no warning.");
    weaver.place(&model)?;
    let (status, log) = run_merged(&mut weaver.command()?, true)?;
    if !status.success() {
        return Err(exit_of(status).into());
    }
    if Regex::new(MODEL_FINDING)?.is_match(&joined(&log)) {
        return fail("error: Weaver reports a finding for the model");
    }

    println!("Run 2: the negative control must fail on a package policy.");
    let broken = work.join("broken");
    step(sh::cmd("cp").arg("-R").arg(&model).arg(&broken))?;
    let mut registry_file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(broken.join("registry.yaml"))
        .context("opening the registry of the negative control")?;
    registry_file.write_all(COLLISION_ENTRY.as_bytes())?;
    drop(registry_file);
    weaver.place(&broken)?;
    let (status, log) = run_merged(&mut weaver.command()?, false)?;
    if status.success() {
        show(&log)?;
        return fail("error: the negative control passed, so run 1 proves nothing");
    }
    let joined_log = joined(&log);
    if !joined_log
        .windows(COLLISION.len())
        .any(|w| w == COLLISION.as_bytes())
    {
        show(&log)?;
        return fail("error: the negative control failed for another reason");
    }
    println!("The model passes the upstream policy checks.");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE: &str = "# Title\n\n## Background and description\nSay why.\r\n\
                            ### Current challenges\n## Extra (Optional)\n\
                            ## Later (Post-Approval)\n##### Deep\n#Tight\n  \n  Indented text.  \n";

    #[test]
    fn the_pins_are_full_commits_and_a_release() {
        assert!(is_commit(SEMCONV_COMMIT));
        assert!(is_commit(COMMUNITY_COMMIT));
        assert!(BASELINE_TAG.starts_with('v'));
    }

    #[test]
    fn required_sections_skip_optional_ones() {
        assert_eq!(
            required_sections(TEMPLATE).expect("regex"),
            ["## Background and description", "### Current challenges"]
        );
        assert_eq!(required_sections("").expect("regex"), Vec::<String>::new());
    }

    #[test]
    fn guidance_is_the_text_that_is_not_a_heading() {
        assert_eq!(guidance(TEMPLATE), ["Say why.", "  Indented text."]);
    }

    #[test]
    fn missing_sections_name_lacking_and_kept_lines() {
        let required = required_sections(TEMPLATE).expect("regex");
        let guidance = guidance(TEMPLATE);
        let good =
            clean_lines("## Background and description\nOur text.  \n### Current challenges\n\n\n");
        assert_eq!(good.len(), 3);
        assert_eq!(
            missing_sections(&good, &required, &guidance),
            Vec::<String>::new()
        );
        let bad = clean_lines("## Background and description\r\nSay why.\n");
        assert_eq!(
            missing_sections(&bad, &required, &guidance),
            ["### Current challenges", "Say why."]
        );
    }

    #[test]
    fn the_form_lines_may_have_spaces_around_them() {
        let form = "labels:\n  - area:new \nbody:\n  - label: Propose new conventions\n";
        assert!(form_has(form, "- area:new"));
        assert!(!form_has(form, "label: Propose new conventions"));
        assert!(form_has(
            "label: Propose new conventions",
            "label: Propose new conventions"
        ));
    }

    #[test]
    fn the_image_comes_from_the_weaver_stage() {
        let file = "FROM docker.io/otel/weaver:v0.1@sha256:abc AS weaver\nFROM x:1 AS other\n";
        assert_eq!(weaver_image(file), "docker.io/otel/weaver:v0.1@sha256:abc");
        assert_eq!(weaver_image("FROM a AS b\n"), "");
    }

    #[test]
    fn the_package_pin_splits_at_the_last_at_sign() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        let makefile = format!("X=1\nWEAVER_PACKAGES_REPO= https://h/r.git@{sha} \n");
        let pin = packages_pin(&makefile);
        assert_eq!(pin, format!("https://h/r.git@{sha}"));
        assert_eq!(split_pin(&pin), ("https://h/r.git", sha));
        assert_eq!(split_pin("no-at"), ("no-at", "no-at"));
        assert!(!is_commit("main"));
    }

    #[test]
    fn the_joined_log_drops_breaks_spaces_and_rules() {
        let log = "a │ weaver/sour\nce/workflow/x\n".as_bytes();
        let joined = joined(log);
        assert_eq!(joined, b"aweaver/source/workflow/x");
        let finding = Regex::new(MODEL_FINDING).expect("regex");
        assert!(finding.is_match(&joined));
        assert!(!finding.is_match(b"spanXworkflow."));
    }

    #[test]
    fn cut_and_substitution_match_the_shell() {
        assert_eq!(first_fields("abc\trefs/heads/main\n"), "abc");
        assert_eq!(substitution("x\n\n"), "x");
    }

    #[test]
    fn the_repo_proposal_passes_its_own_template_shape() {
        let text = std::fs::read_to_string(repo_path(PROPOSAL)).expect("read the proposal");
        let clean = clean_lines(&text);
        for heading in ["## Background and description", "## Deliverables"] {
            assert!(clean.iter().any(|l| l == heading), "{heading}");
        }
    }
}
