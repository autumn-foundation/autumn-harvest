//! `cargo xtask dev clean-room-getting-started`: the port of
//! `scripts/clean-room-getting-started.sh`.
//!
//! Clean-room run of the getting-started path (Onramp harness).
//!
//! Starts from an empty directory. Pastes the Chapter 1 `Cargo.toml` and
//! `src/main.rs` and the Chapter 2 workflow, activity and registration
//! blocks from the docs. The only edit splices the Chapter 2 blocks into the
//! Chapter 1 file. Then runs `cargo check`. Every step logs its command,
//! exit code and seconds to a TSV, followed by a summary line.
//!
//! Modes:
//!
//! - (default): resolve `autumn-harvest*` from crates.io, as a newcomer does.
//! - `--checkout`: point `autumn-harvest*` at this checkout with path
//!   dependencies. Then check that Cargo resolved them from disk. Use it to
//!   test a docs or API change before release.
//!
//! Not covered: the Postgres steps (`docker compose up`, `curl`). They need
//! a Docker daemon. `cargo xtask dev run-standalone-chapter` covers that
//! half for the standalone chapter.
//!
//! Usage: `cargo xtask dev clean-room-getting-started [--checkout] [--log FILE]`.
//!
//! Exit code: 0 when every step passes, 1 otherwise, 2 for a usage error.
//!
//! For a pristine container, mount the checkout read-only into `rust:latest`.
//! The xtask build needs a writable target directory there:
//!
//! ```sh
//! docker run --rm -v "$PWD":/src:ro -w /src -e CARGO_TARGET_DIR=/tmp/xtask \
//!   rust:latest cargo xtask dev clean-room-getting-started
//! ```
//!
//! The steps run with the default toolchain, as the steps of the old script
//! did. The rustup proxy of `cargo xtask` sets `RUSTUP_TOOLCHAIN` to the pin
//! of this repository. The steps drop that value, because a newcomer has no
//! such pin.

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use regex::{NoExpand, Regex};
use smop::prelude::*;

use crate::util::repo_root;

/// Arguments of `cargo xtask dev clean-room-getting-started`.
///
/// The command parses its own flags, so that the usage errors keep the text
/// and the exit code of the old script.
#[derive(Debug, Args)]
pub struct Args {
    /// `--checkout` and `--log FILE`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// The parsed flags.
#[derive(Debug, PartialEq, Eq)]
struct Options {
    checkout: bool,
    log: Option<String>,
}

/// Parses the flags as the `while` loop of the old script did.
///
/// # Errors
///
/// Returns the usage message for a bad flag.
fn parse(args: &[String]) -> Result<Options, String> {
    let mut options = Options {
        checkout: false,
        log: None,
    };
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--checkout" => options.checkout = true,
            "--log" => match args.next() {
                // An empty path gives the default log, as `${log:-...}` did.
                Some(file) if !file.starts_with("--") => {
                    options.log = Some(file.clone()).filter(|file| !file.is_empty());
                }
                _ => return Err("--log needs a file path".to_owned()),
            },
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(options)
}

/// The files that the docs paste into the clean-room crate.
#[derive(Debug, PartialEq, Eq)]
pub struct Pasted {
    /// The Chapter 1 `Cargo.toml`.
    pub cargo_toml: String,
    /// The Chapter 1 `src/main.rs` with the Chapter 2 blocks spliced in.
    pub main_rs: String,
}

/// Returns each fenced block of `text` as `(language, body)`.
///
/// A block opens with three backticks, a word and a newline. It ends at the
/// next three backticks.
fn blocks(text: &str) -> Result<Vec<(String, String)>> {
    let fence = Regex::new(r"(?s)```(\w+)\n(.*?)```")?;
    Ok(fence
        .captures_iter(text)
        .map(|caps| (caps[1].to_owned(), caps[2].to_owned()))
        .collect())
}

/// Reads a chapter with universal newlines, as Python `open()` does.
fn read_chapter(path: &Path) -> Result<String> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// Pastes the getting-started blocks into the two files of the crate.
///
/// # Errors
///
/// Returns the message of the old extract step when the docs lose a block,
/// or when the registration splice does not match once.
pub fn paste(ch1: &str, ch2: &str) -> Result<Pasted> {
    let mut cargo = None;
    let mut main = None;
    for (_, body) in blocks(ch1)? {
        if body.starts_with("# Cargo.toml") {
            cargo = Some(body.clone());
        }
        if body.starts_with("// src/main.rs") {
            main = Some(body);
        }
    }
    let rust2: Vec<String> = blocks(ch2)?
        .into_iter()
        .filter(|(lang, _)| lang == "rust")
        .map(|(_, body)| body)
        .collect();
    let (Some(cargo), Some(main), [workflow, register, ..]) = (cargo, main, rust2.as_slice())
    else {
        bail!("docs no longer contain the expected Cargo.toml/main.rs/Chapter 2 blocks");
    };
    // Chapter 2 imports `Duration` and the prelude. Chapter 1 already has the prelude.
    let main = main.replace(
        "use autumn_harvest_plugin::HarvestPlugin;",
        "use std::time::Duration;\nuse autumn_harvest_plugin::HarvestPlugin;",
    );
    let splice = Regex::new(r#"(?s)HarvestPlugin::new\(\)\n.*?\.api\("/api/harvest"\),\n"#)?;
    if splice.find_iter(&main).count() != 1 {
        bail!("could not splice the Chapter 2 registration into Chapter 1 main.rs");
    }
    let registration = format!("{},\n", register.trim());
    let main = splice.replace(&main, NoExpand(&registration)).into_owned();
    let workflow = workflow.replace(
        "use std::time::Duration;\n\nuse autumn_harvest::prelude::*;\n",
        "",
    );
    Ok(Pasted {
        cargo_toml: cargo,
        main_rs: format!("{main}\n{workflow}"),
    })
}

/// Points the two direct dependencies at `repo`, and patches the macro crate.
///
/// A `[patch]` entry only adds a resolver candidate. Cargo still picks a
/// newer crates.io release over the checkout. So the command rewrites the
/// two direct dependencies to paths. It also patches the macro crate that
/// the engine pulls in.
pub fn point_at_checkout(cargo_toml: &str, repo: &str) -> Result<String> {
    let mut text = cargo_toml.to_owned();
    for name in ["autumn-harvest", "autumn-harvest-plugin"] {
        let line = Regex::new(&format!(r#"(?m)^{} = "[^"\n]*""#, regex::escape(name)))?;
        let path = format!("{name} = {{ path = \"{repo}/{name}\" }}");
        text = line.replace_all(&text, NoExpand(&path)).into_owned();
    }
    text.push_str("\n[patch.crates-io]\n");
    let _ = writeln!(
        text,
        "autumn-harvest-macros = {{ path = \"{repo}/autumn-harvest-macros\" }}"
    );
    Ok(text)
}

/// Returns the names of the packages of this repository that came from a
/// registry, from the JSON of `cargo metadata`.
pub fn registry_packages(metadata: &str) -> Result<Vec<String>> {
    let meta: serde_json::Value =
        serde_json::from_str(metadata).context("cargo metadata printed no JSON")?;
    let packages = meta["packages"]
        .as_array()
        .context("cargo metadata has no packages")?;
    Ok(packages
        .iter()
        .filter(|p| {
            p["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("autumn-harvest"))
        })
        .filter(|p| !p["source"].is_null())
        .filter_map(|p| p["name"].as_str().map(str::to_owned))
        .collect())
}

/// Returns the channel that `rust-toolchain.toml` of the repository pins.
fn pinned_channel(repo: &Path) -> Option<String> {
    let text = std::fs::read_to_string(repo.join("rust-toolchain.toml")).ok()?;
    let table: toml::Table = text.parse().ok()?;
    table
        .get("toolchain")?
        .get("channel")?
        .as_str()
        .map(str::to_owned)
}

/// Reports whether `value` of `RUSTUP_TOOLCHAIN` names the pinned channel.
///
/// The rustup proxy writes the full name, such as
/// `1.99.1-x86_64-unknown-linux-gnu`, for a pin of `1.99.1`.
fn names_pin(value: &str, channel: &str) -> bool {
    value == channel
        || value
            .strip_prefix(channel)
            .is_some_and(|rest| rest.starts_with('-'))
}

/// Returns a `cargo` command for a step in `krate`.
///
/// The command drops a `RUSTUP_TOOLCHAIN` that names the pin of this
/// repository. The rustup proxy of `cargo xtask` sets it, and the old
/// script never saw it. A value that names another toolchain stays.
fn step_cargo(repo: &Path, krate: &Path) -> Command {
    let mut command = Command::new("cargo");
    command.current_dir(krate);
    let inherited = std::env::var("RUSTUP_TOOLCHAIN").ok();
    if let (Some(value), Some(channel)) = (inherited, pinned_channel(repo))
        && names_pin(&value, &channel)
    {
        command.env_remove("RUSTUP_TOOLCHAIN");
    }
    command
}

/// Runs `cargo xtask dev clean-room-getting-started`.
pub fn run(args: Args) -> Result<ExitCode> {
    let options = match parse(&args.rest) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            return Ok(ExitCode::from(2));
        }
    };
    let repo = repo_root();
    let mode = if options.checkout {
        "checkout"
    } else {
        "crates-io"
    };
    let work = smop::fs::temp_dir()?;
    let log = options
        .log
        .unwrap_or_else(|| work.path().join("steps.tsv").display().to_string());
    if File::create(&log)
        .and_then(|mut file| file.write_all(b"step\texit\tseconds\tcommand\n"))
        .is_err()
    {
        eprintln!("cannot create the log file: {log}");
        return Ok(ExitCode::from(2));
    }
    let mut steps = Steps {
        work: work.path().to_path_buf(),
        log: PathBuf::from(&log),
        count: 0,
        failures: 0,
        seconds: 0,
    };

    let krate = work.path().join("crate");
    std::fs::create_dir_all(krate.join("src")).context("cannot create the crate")?;
    let ch1 = repo.join("docs/getting-started/01-project-skeleton.md");
    let ch2 = repo.join("docs/getting-started/02-first-workflow.md");

    // Extract the pasted files. A missing block is a failed step, not a skip.
    let extract = format!(
        "extract {} {} {}",
        ch1.display(),
        ch2.display(),
        krate.display()
    );
    let ex = steps.record("extract", &extract, |_| {
        let pasted = paste(&read_chapter(&ch1)?, &read_chapter(&ch2)?)?;
        std::fs::write(krate.join("Cargo.toml"), &pasted.cargo_toml)?;
        std::fs::write(krate.join("src/main.rs"), &pasted.main_rs)?;
        Ok(())
    })?;

    if ex && options.checkout {
        let manifest = krate.join("Cargo.toml");
        let text =
            std::fs::read_to_string(&manifest).context("cannot read the pasted Cargo.toml")?;
        let text = point_at_checkout(&text, &repo.display().to_string())?;
        std::fs::write(&manifest, text).context("cannot rewrite the pasted Cargo.toml")?;
    }

    if ex {
        steps.command(
            "cargo-check",
            step_cargo(&repo, &krate).args(["check", "--quiet", "--message-format=short"]),
        )?;
    }

    // Fail if a checkout run compiled any published copy of this repo's crates.
    if ex && options.checkout {
        steps.record(
            "verify-sources",
            "cargo metadata --format-version=1",
            |out| {
                // The output of cargo goes to the step output, as in the old step.
                let output = step_cargo(&repo, &krate)
                    .args(["metadata", "--format-version=1"])
                    .stderr(out.try_clone().context("cannot share the step output")?)
                    .output()
                    .context("cannot run cargo metadata")?;
                ensure!(
                    output.status.success(),
                    "cargo metadata failed: {}",
                    output.status
                );
                let bad = registry_packages(&String::from_utf8_lossy(&output.stdout))?;
                ensure!(
                    bad.is_empty(),
                    "resolved from a registry, not the checkout: {}",
                    bad.join(", ")
                );
                Ok(())
            },
        )?;
    }

    println!(
        "clean-room mode={mode} steps={} failures={} seconds={} log={log}",
        steps.count, steps.failures, steps.seconds
    );
    let text = std::fs::read(&log).with_context(|| format!("cannot read {log}"))?;
    std::io::stdout()
        .write_all(&text)
        .context("cannot print the log")?;
    drop(work);
    Ok(crate::util::gate_exit(steps.failures == 0))
}

/// The step log of one clean-room run.
struct Steps {
    work: PathBuf,
    log: PathBuf,
    count: u32,
    failures: u32,
    seconds: u64,
}

impl Steps {
    /// Runs `command` as the step `name` and records it.
    fn command(&mut self, name: &str, command: &mut Command) -> Result<bool> {
        let line = std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(|word| word.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        let out = self.work.join(format!("{name}.out"));
        let file =
            File::create(&out).with_context(|| format!("cannot create {}", out.display()))?;
        let err = file.try_clone().context("cannot share the step output")?;
        command.stdout(file).stderr(err);
        let start = now();
        let code = match command.status() {
            Ok(status) => shell_code(status),
            Err(err) => {
                let program = command.get_program().to_string_lossy().into_owned();
                let (code, text) = if err.kind() == std::io::ErrorKind::NotFound {
                    (127, format!("{program}: command not found\n"))
                } else {
                    (126, format!("{program}: {err}\n"))
                };
                std::fs::write(&out, text).context("cannot write the step output")?;
                code
            }
        };
        self.finish(name, &line, code, start)
    }

    /// Runs `body` in process as the step `name`. The body gets the step
    /// output. A failure writes its message there and counts as exit code 1.
    fn record(
        &mut self,
        name: &str,
        line: &str,
        body: impl FnOnce(&File) -> Result<()>,
    ) -> Result<bool> {
        let out = self.work.join(format!("{name}.out"));
        let mut file =
            File::create(&out).with_context(|| format!("cannot create {}", out.display()))?;
        let start = now();
        let code = match body(&file) {
            Ok(()) => 0,
            Err(err) => {
                writeln!(file, "{err:#}").context("cannot write the step output")?;
                1
            }
        };
        drop(file);
        self.finish(name, line, code, start)
    }

    /// Appends one TSV line, and reports a failed step on standard error.
    fn finish(&mut self, name: &str, line: &str, code: i32, start: u64) -> Result<bool> {
        let seconds = now().saturating_sub(start);
        self.count += 1;
        self.seconds += seconds;
        let mut log = OpenOptions::new()
            .append(true)
            .open(&self.log)
            .with_context(|| format!("cannot append to {}", self.log.display()))?;
        writeln!(log, "{name}\t{code}\t{seconds}\t{line}")
            .with_context(|| format!("cannot append to {}", self.log.display()))?;
        if code != 0 {
            self.failures += 1;
            eprintln!("--- FAIL: {name} (exit {code})");
            let out = std::fs::read(self.work.join(format!("{name}.out"))).unwrap_or_default();
            eprint!("{}", tail(&String::from_utf8_lossy(&out), 30));
        }
        Ok(code == 0)
    }
}

/// Returns the wall clock in whole seconds, as `date +%s` prints it.
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// Returns the last `count` lines of `text`, as `tail -n` prints them.
fn tail(text: &str, count: usize) -> String {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    lines[lines.len().saturating_sub(count)..].concat()
}

/// Returns the exit code that a shell reports for `status`.
fn shell_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    status.code().unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CH1: &str = "\
# Chapter 1

```toml
# Cargo.toml
[dependencies]
autumn-harvest = \"0.9\"
autumn-harvest-plugin = \"0.9\" # the plugin
autumn-harvest-other = \"0.9\"
```

```rust
// src/main.rs
use autumn_harvest_plugin::HarvestPlugin;

fn main() {
    let plugin = HarvestPlugin::new()
        .with_x()
        .api(\"/api/harvest\"),
    ;
}
```
";

    const CH2: &str = "\
```rust
use std::time::Duration;

use autumn_harvest::prelude::*;
#[workflow]
async fn hello() {}
```

```bash
cargo run
```

```rust
HarvestPlugin::new()
    .workflow::<Hello>()
    .api(\"/api/$1\")
```
";

    fn strings(args: &[&str]) -> Vec<String> {
        args.iter().map(|&arg| arg.to_owned()).collect()
    }

    #[test]
    fn flags_parse_with_the_old_messages() {
        assert_eq!(
            parse(&strings(&["--checkout", "--log", "x.tsv"])).unwrap(),
            Options {
                checkout: true,
                log: Some("x.tsv".to_owned())
            }
        );
        assert_eq!(
            parse(&strings(&["--log"])).unwrap_err(),
            "--log needs a file path"
        );
        assert_eq!(
            parse(&strings(&["--log", "--checkout"])).unwrap_err(),
            "--log needs a file path"
        );
        assert_eq!(
            parse(&strings(&["-x"])).unwrap_err(),
            "unknown argument: -x"
        );
        // An empty path keeps the default log.
        assert_eq!(parse(&strings(&["--log", ""])).unwrap().log, None);
    }

    #[test]
    fn only_the_pinned_toolchain_is_dropped() {
        assert!(names_pin("1.99.1-x86_64-unknown-linux-gnu", "1.99.1"));
        assert!(names_pin("1.99.1", "1.99.1"));
        assert!(!names_pin("1.99.10-x86_64-unknown-linux-gnu", "1.99.1"));
        assert!(!names_pin("nightly", "1.99.1"));
        assert!(pinned_channel(&repo_root()).is_some());
    }

    #[test]
    fn the_chapters_paste_into_one_crate() {
        let pasted = paste(CH1, CH2).unwrap();
        assert!(pasted.cargo_toml.starts_with("# Cargo.toml\n"));
        let main = &pasted.main_rs;
        assert!(
            main.contains("use std::time::Duration;\nuse autumn_harvest_plugin::HarvestPlugin;")
        );
        // The splice keeps a `$1` of the docs as written.
        assert!(main.contains("let plugin = HarvestPlugin::new()\n    .workflow::<Hello>()\n    .api(\"/api/$1\"),\n    ;"));
        assert!(main.ends_with("}\n\n#[workflow]\nasync fn hello() {}\n"));
        assert!(!main.contains("use autumn_harvest::prelude::*;"));
    }

    #[test]
    fn a_missing_block_or_splice_fails_the_extract() {
        let err = paste(CH1, "no blocks").unwrap_err().to_string();
        assert_eq!(
            err,
            "docs no longer contain the expected Cargo.toml/main.rs/Chapter 2 blocks"
        );
        let flat = CH1.replace(".api(\"/api/harvest\"),", ".api(\"/x\"),");
        let err = paste(&flat, CH2).unwrap_err().to_string();
        assert_eq!(
            err,
            "could not splice the Chapter 2 registration into Chapter 1 main.rs"
        );
    }

    #[test]
    fn the_real_chapters_still_paste() {
        let repo = repo_root();
        let ch1 = read_chapter(&repo.join("docs/getting-started/01-project-skeleton.md")).unwrap();
        let ch2 = read_chapter(&repo.join("docs/getting-started/02-first-workflow.md")).unwrap();
        let pasted = paste(&ch1, &ch2).unwrap();
        assert!(pasted.main_rs.contains("use std::time::Duration;"));
    }

    #[test]
    fn a_checkout_run_rewrites_the_direct_dependencies() {
        let toml = "[dependencies]\nautumn-harvest = \"0.9\"\nautumn-harvest-plugin = \"0.9\" # x\nautumn-harvest-other = \"0.9\"\n";
        let text = point_at_checkout(toml, "/repo").unwrap();
        assert_eq!(
            text,
            "[dependencies]\n\
             autumn-harvest = { path = \"/repo/autumn-harvest\" }\n\
             autumn-harvest-plugin = { path = \"/repo/autumn-harvest-plugin\" } # x\n\
             autumn-harvest-other = \"0.9\"\n\
             \n[patch.crates-io]\n\
             autumn-harvest-macros = { path = \"/repo/autumn-harvest-macros\" }\n"
        );
    }

    #[test]
    fn registry_copies_of_the_repo_crates_are_named() {
        let meta = r#"{"packages":[
            {"name":"autumn-harvest","source":null},
            {"name":"autumn-harvest-macros","source":"registry+https://github.com/rust-lang/crates.io-index"},
            {"name":"serde","source":"registry+https://github.com/rust-lang/crates.io-index"}
        ]}"#;
        assert_eq!(registry_packages(meta).unwrap(), ["autumn-harvest-macros"]);
        assert!(registry_packages("not json").is_err());
    }
}
