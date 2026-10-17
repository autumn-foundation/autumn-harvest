//! The trainers: a simulated one for offline runs, and a command for a GPU.
//!
//! [`TrainerSpec::Simulated`] reads the hyperparameter block of `train.py` and
//! scores it on a fixed loss landscape. It needs no GPU, no Python and no
//! data, so CI can run a whole night of research in seconds. The landscape
//! has a known optimum, so a test can check that the loop moves towards it.
//!
//! [`TrainerSpec::Command`] runs a real command, such as `uv run train.py`,
//! in the lab. It writes stdout and stderr to `run.log` and kills the child
//! at its timeout, as upstream kills a run over ten minutes.
//!
//! The command runs code that the agent wrote, so it gets no secrets from
//! the environment. Its process group dies with the run: at the end, at the
//! timeout, and when the runner drops the activity. A runner that dies
//! with no cleanup leaves the group alive. A lock file finds that group,
//! and the next run kills it first. This is not a sandbox: see the README.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

/// The file that each run writes, as upstream redirects it.
pub const RUN_LOG: &str = "run.log";

/// The lines of the log that a run returns, as `tail -n 50 run.log`.
pub const TAIL_LINES: usize = 50;

/// The most bytes of the log that a run returns.
pub const TAIL_BYTES: u64 = 16 * 1024;

/// The lock file of the command trainer, in the state directory. It holds
/// the process group of the run in flight.
pub const TRAINER_LOCK: &str = "trainer.lock";

/// The environment variables that a training run gets.
///
/// The list holds the shell basics, the locale, and the settings of CUDA,
/// `PyTorch`, Python, uv and Hugging Face. An entry that ends in `_` is a
/// prefix, and so is `PYTHON`. The run gets nothing else, so a secret such
/// as a database URL stays out of code that the model wrote.
pub const PASSED_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LANGUAGE",
    "TZ",
    "TERM",
    "TMPDIR",
    "LD_LIBRARY_PATH",
    "VIRTUAL_ENV",
    "SYSTEMROOT",
    "LC_",
    "XDG_",
    "CUDA_",
    "NVIDIA_",
    "NCCL_",
    "TORCH_",
    "PYTORCH_",
    "TRITON_",
    "OMP_",
    "MKL_",
    "PYTHON",
    "UV_",
    "HF_",
];

/// Parts of a variable name that mark a secret. A variable of
/// [`PASSED_ENV`] with such a name stays out too, such as `HF_TOKEN`.
const SECRET_MARKS: &[&str] = &["API_KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"];

/// The memory of one H100, in MB. A simulated run above it is out of memory.
pub const GPU_MEMORY_MB: f64 = 81_920.0;

/// How to train one commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TrainerSpec {
    /// Score `train.py` on the fixed landscape. `seconds` adds wall time,
    /// with progress, so a demo can show a run in flight.
    Simulated {
        /// Wall time of one run. Zero runs at once.
        #[serde(default)]
        seconds: u64,
    },
    /// Run `program` with `args` in the lab.
    Command {
        /// The program, for example `uv`.
        program: String,
        /// Its arguments, for example `["run", "train.py"]`.
        args: Vec<String>,
        /// The run is killed after this many seconds.
        timeout_secs: u64,
        /// More environment variables to pass to the run, by name. The
        /// run gets [`PASSED_ENV`] and these, and nothing else.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pass_env: Vec<String>,
    },
}

impl Default for TrainerSpec {
    fn default() -> Self {
        Self::Simulated { seconds: 0 }
    }
}

impl TrainerSpec {
    /// The upstream command: `uv run train.py`, killed after ten minutes.
    #[must_use]
    pub fn upstream() -> Self {
        Self::Command {
            program: "uv".into(),
            args: vec!["run".into(), "train.py".into()],
            timeout_secs: 600,
            pass_env: Vec::new(),
        }
    }
}

/// What one run left behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainRun {
    /// The exit code. `None` when the run was killed or did not start.
    pub exit_code: Option<i32>,
    /// The run passed its timeout and was killed.
    pub timed_out: bool,
    /// The last [`TAIL_LINES`] lines of `run.log`.
    pub log_tail: String,
}

/// The result of one simulated run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Simulated {
    /// The whole log, as `train.py` would print it.
    pub log: String,
    /// Zero on success, one on a crash.
    pub exit_code: i32,
}

/// One hyperparameter of the landscape.
struct Knob {
    name: &'static str,
    /// The upstream value. A deleted line falls back to it.
    default: f64,
    /// The value with the lowest loss.
    best: f64,
    /// The weight of the squared distance from `best`.
    weight: f64,
    /// Measure the distance in log space, for rates and sizes.
    log: bool,
}

/// The landscape. The upstream defaults sit about 0.016 above the optimum,
/// so a good night of research finds real gains.
const KNOBS: &[Knob] = &[
    Knob {
        name: "MATRIX_LR",
        default: 0.04,
        best: 0.06,
        weight: 0.02,
        log: true,
    },
    Knob {
        name: "EMBEDDING_LR",
        default: 0.6,
        best: 0.6,
        weight: 0.004,
        log: true,
    },
    Knob {
        name: "UNEMBEDDING_LR",
        default: 0.004,
        best: 0.004,
        weight: 0.004,
        log: true,
    },
    Knob {
        name: "SCALAR_LR",
        default: 0.5,
        best: 0.5,
        weight: 0.002,
        log: true,
    },
    Knob {
        name: "WEIGHT_DECAY",
        default: 0.2,
        best: 0.1,
        weight: 0.08,
        log: false,
    },
    Knob {
        name: "WARMUP_RATIO",
        default: 0.0,
        best: 0.0,
        weight: 0.05,
        log: false,
    },
    Knob {
        name: "WARMDOWN_RATIO",
        default: 0.5,
        best: 0.7,
        weight: 0.05,
        log: false,
    },
    Knob {
        name: "FINAL_LR_FRAC",
        default: 0.0,
        best: 0.0,
        weight: 0.02,
        log: false,
    },
    Knob {
        name: "DEPTH",
        default: 8.0,
        best: 10.0,
        weight: 0.0015,
        log: false,
    },
    Knob {
        name: "ASPECT_RATIO",
        default: 64.0,
        best: 64.0,
        weight: 0.01,
        log: true,
    },
    Knob {
        name: "HEAD_DIM",
        default: 128.0,
        best: 128.0,
        weight: 0.005,
        log: true,
    },
    Knob {
        name: "TOTAL_BATCH_SIZE",
        default: 524_288.0,
        best: 262_144.0,
        weight: 0.0058,
        log: true,
    },
];

/// The upstream baseline `val_bpb` at the upstream defaults.
const BASELINE_BPB: f64 = 0.9979;
/// The upstream baseline peak memory, in MB.
const BASELINE_VRAM_MB: f64 = 45_060.2;
/// The upstream baseline step count.
const BASELINE_STEPS: f64 = 953.0;

/// One parsed `NAME = value` line.
enum Value {
    Number(f64),
    Text(String),
    Other,
}

/// Parse the `NAME = value` lines of `train_py`. A line that starts with an
/// upper-case name and `=` must parse, or the run is a syntax error.
fn hyperparameters(train_py: &str) -> Result<BTreeMap<String, Value>, String> {
    let mut values = BTreeMap::new();
    for (index, line) in train_py.lines().enumerate() {
        let Some((name, rest)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        let is_name = !name.is_empty()
            && name.starts_with(|c: char| c.is_ascii_uppercase())
            && name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
        if !is_name || line.starts_with(char::is_whitespace) {
            continue;
        }
        let code = rest.split('#').next().unwrap_or("").trim();
        let value = parse_value(code).ok_or_else(|| {
            format!(
                "  File \"train.py\", line {}\n    {line}\nSyntaxError: invalid syntax",
                index + 1
            )
        })?;
        values.insert(name.to_string(), value);
    }
    Ok(values)
}

/// A number, `a**b`, or a quoted string. A digit separator `_` is allowed.
/// An empty value, or one that starts with `=`, is a syntax error. Any
/// other value, such as `True` or an expression, is [`Value::Other`]: the
/// landscape does not score it, so its knob keeps the default.
fn parse_value(code: &str) -> Option<Value> {
    if code.is_empty() || code.starts_with('=') {
        return None;
    }
    let number = |text: &str| text.trim().replace('_', "").parse::<f64>().ok();
    if let Some((base, exp)) = code.split_once("**")
        && let (Some(base), Some(exp)) = (number(base), number(exp))
    {
        return Some(Value::Number(base.powf(exp)));
    }
    if let Some(value) = number(code) {
        return Some(Value::Number(value));
    }
    let quoted = (code.starts_with('"') && code.ends_with('"'))
        || (code.starts_with('\'') && code.ends_with('\''));
    if quoted && code.len() >= 2 {
        return Some(Value::Text(code[1..code.len() - 1].to_string()));
    }
    Some(Value::Other)
}

/// The 64-bit FNV-1a hash: stable on every platform.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// The loss above the optimum for `values`.
fn penalty(values: &BTreeMap<String, Value>) -> f64 {
    let mut total = 0.0;
    for knob in KNOBS {
        let x = match values.get(knob.name) {
            Some(Value::Number(x)) => *x,
            _ => knob.default,
        };
        let distance = if knob.log {
            (x.max(1e-12) / knob.best).ln()
        } else {
            x - knob.best
        };
        total = (knob.weight * distance).mul_add(distance, total);
    }
    total
        + match values.get("WINDOW_PATTERN") {
            Some(Value::Text(pattern)) if pattern == "SSSL" => 0.0,
            Some(Value::Text(pattern)) if pattern == "L" => 0.0015,
            Some(_) => 0.003,
            None => 0.0,
        }
}

fn number(values: &BTreeMap<String, Value>, name: &str, default: f64) -> f64 {
    match values.get(name) {
        Some(Value::Number(x)) => *x,
        _ => default,
    }
}

/// Score `train_py` on the landscape. The same text gives the same log.
#[must_use]
pub fn simulate(train_py: &str) -> Simulated {
    let crash = |log: String| Simulated { log, exit_code: 1 };
    let values = match hyperparameters(train_py) {
        Ok(values) => values,
        Err(syntax) => return crash(format!("Traceback (most recent call last):\n{syntax}\n")),
    };
    let depth = number(&values, "DEPTH", 8.0);
    let device_batch = number(&values, "DEVICE_BATCH_SIZE", 128.0);
    let aspect = number(&values, "ASPECT_RATIO", 64.0);
    let vram = BASELINE_VRAM_MB * (depth / 8.0).powi(2) * (device_batch / 128.0) * (aspect / 64.0);
    if vram > GPU_MEMORY_MB {
        return crash(format!(
            "Traceback (most recent call last):\n  File \"train.py\", in <module>\n\
             torch.OutOfMemoryError: CUDA out of memory. Tried to allocate {:.0} MiB.\n",
            vram - GPU_MEMORY_MB
        ));
    }
    if number(&values, "MATRIX_LR", 0.04) > 0.3 || number(&values, "EMBEDDING_LR", 0.6) > 3.0 {
        return crash(
            "step 00042 (4.4%) | loss: nan\nTraceback (most recent call last):\n\
             RuntimeError: loss is NaN at step 42\n"
                .into(),
        );
    }
    let defaults = BTreeMap::new();
    let floor = BASELINE_BPB - penalty(&defaults);
    // Run-to-run noise: a fixed function of the text, within 0.0002.
    let jitter = u32::try_from(fnv1a(train_py.as_bytes()) % 4001).unwrap_or(0);
    let noise = f64::from(jitter) / 10_000_000.0 - 0.0002;
    let val_bpb = floor + penalty(&values) + noise;
    let batch = number(&values, "TOTAL_BATCH_SIZE", 524_288.0);
    let steps = (BASELINE_STEPS * 524_288.0 / batch * 8.0 / depth).round();
    let mut log = String::new();
    for tenth in 1..=10_u32 {
        let step = (steps * f64::from(tenth) / 10.0).round();
        let loss = (val_bpb - 0.9).mul_add(f64::from(11 - tenth), 3.0);
        let _ = writeln!(log, "step {step:05} ({}.0%) | loss: {loss:.6}", tenth * 10);
    }
    let _ = write!(
        log,
        "---\nval_bpb:          {val_bpb:.6}\ntraining_seconds: 300.1\n\
         total_seconds:    325.9\npeak_vram_mb:     {vram:.1}\nnum_steps:        {steps}\n\
         depth:            {depth}\n"
    );
    Simulated { log, exit_code: 0 }
}

/// Run one training of the lab at `lab`. `state` is the state directory,
/// which holds the lock file of the command trainer.
///
/// `progress` gets each progress line, for an activity heartbeat.
///
/// # Errors
///
/// Returns an error when the run cannot start or `run.log` cannot be
/// written. A crash or a timeout is a result, not an error.
pub async fn run(
    spec: &TrainerSpec,
    lab: &Path,
    state: &Path,
    mut progress: impl FnMut(&str) + Send,
) -> Result<TrainRun, String> {
    let log_path = lab.join(RUN_LOG);
    match spec {
        TrainerSpec::Simulated { seconds } => {
            let train_py = std::fs::read_to_string(lab.join("train.py"))
                .map_err(|e| format!("cannot read train.py: {e}"))?;
            let run = simulate(&train_py);
            let lines: Vec<&str> = run.log.lines().collect();
            let pause =
                Duration::from_secs(*seconds) / u32::try_from(lines.len().max(1)).unwrap_or(1);
            for line in &lines {
                if line.starts_with("step ") {
                    progress(line);
                }
                if !pause.is_zero() {
                    tokio::time::sleep(pause).await;
                }
            }
            new_log(&log_path)?;
            std::fs::write(&log_path, &run.log)
                .map_err(|e| format!("cannot write run.log: {e}"))?;
            Ok(TrainRun {
                exit_code: Some(run.exit_code),
                timed_out: false,
                log_tail: crate::summary::tail(&run.log, TAIL_LINES),
            })
        }
        TrainerSpec::Command {
            program,
            args,
            timeout_secs,
            pass_env,
        } => {
            let run = Run {
                program,
                args,
                timeout_secs: *timeout_secs,
                pass_env,
                lab,
                state,
                log_path: &log_path,
            };
            run_command(&run, &mut progress).await
        }
    }
}

/// Remove `run.log`, so the run writes a new file. A symlink that code in
/// the lab planted then cannot send the log to another file.
fn new_log(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("cannot remove the old run.log: {e}")),
    }
}

/// `true` when a training run gets the variable `name`: a name of
/// [`PASSED_ENV`] that marks no secret, or a name that `extra` lists.
fn passes(name: &str, extra: &[String]) -> bool {
    if extra.iter().any(|e| e == name) {
        return true;
    }
    let upper = name.to_ascii_uppercase();
    let listed = PASSED_ENV.iter().any(|entry| {
        if entry.ends_with('_') || *entry == "PYTHON" {
            upper.starts_with(entry)
        } else {
            upper == *entry
        }
    });
    listed && !SECRET_MARKS.iter().any(|mark| upper.contains(mark))
}

/// One command run.
struct Run<'a> {
    program: &'a str,
    args: &'a [String],
    timeout_secs: u64,
    pass_env: &'a [String],
    lab: &'a Path,
    state: &'a Path,
    log_path: &'a Path,
}

/// Run the command with its output in `run.log`. Kill it at the timeout.
async fn run_command(
    run: &Run<'_>,
    progress: &mut (impl FnMut(&str) + Send),
) -> Result<TrainRun, String> {
    let Run {
        program,
        args,
        timeout_secs,
        pass_env,
        lab,
        state,
        log_path,
    } = *run;
    let lock = Lock::take(state).await?;
    new_log(log_path)?;
    let log = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(log_path)
        .map_err(|e| format!("cannot create run.log: {e}"))?;
    let errors = log
        .try_clone()
        .map_err(|e| format!("cannot share run.log: {e}"))?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .current_dir(lab)
        .stdin(lock.stdin()?)
        .stdout(log)
        .stderr(errors)
        .kill_on_drop(true);
    command.env_clear();
    for (name, value) in std::env::vars_os() {
        if passes(&name.to_string_lossy(), pass_env) {
            command.env(name, value);
        }
    }
    // A new process group, so a kill reaches the children of the run too.
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| format!("cannot start {program}: {e}"))?;
    drop(command);
    // The group dies when this guard drops, on every path out.
    let group = Group::of(&child);
    lock.hold(child.id())?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    let mut beat = tokio::time::interval(Duration::from_secs(5));
    let (exit_code, timed_out) = loop {
        tokio::select! {
            status = child.wait() => {
                let status = status.map_err(|e| format!("cannot wait for {program}: {e}"))?;
                break (status.code(), false);
            }
            () = tokio::time::sleep_until(deadline) => {
                // Upstream: "If a run exceeds 10 minutes, kill it".
                group.kill();
                let _ = child.kill().await;
                break (None, true);
            }
            _ = beat.tick() => {
                if let Some(line) = last_line(log_path) {
                    progress(&line);
                }
            }
        }
    };
    // A child that the run left behind dies now too.
    drop(group);
    if timed_out {
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(log_path)
            .await
            .map_err(|e| format!("cannot open run.log: {e}"))?;
        let note = format!("\nautoresearch: killed after the {timeout_secs}s time budget\n");
        file.write_all(note.as_bytes())
            .await
            .map_err(|e| format!("cannot write run.log: {e}"))?;
    }
    Ok(TrainRun {
        exit_code,
        timed_out,
        log_tail: tail_of(log_path, TAIL_BYTES),
    })
}

/// The process group of a run. It dies when the guard drops. Off Unix, the
/// guard does nothing, and the kill of the child alone must do.
struct Group(Option<i32>);

impl Group {
    fn of(child: &tokio::process::Child) -> Self {
        Self(child.id().and_then(|id| i32::try_from(id).ok()))
    }

    fn kill(&self) {
        #[cfg(unix)]
        if let Some(pid) = self.0.and_then(rustix::process::Pid::from_raw) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        self.kill();
    }
}

/// The lock file of the command trainer.
///
/// The run gets the locked file as its stdin, and each child inherits it.
/// So the lock stays held while any process of the run lives, even after
/// the runner dies. The next run tries the lock. When the lock is held,
/// the file names the group of a run that outlived its runner, and the next
/// run kills that group first. A free lock proves that no run lives, so a
/// stale group id is never used.
struct Lock(std::fs::File);

impl Lock {
    /// How long a run waits for a killed group to let go of the lock.
    const WAIT: Duration = Duration::from_secs(10);

    async fn take(state: &Path) -> Result<Self, String> {
        let path = state.join(TRAINER_LOCK);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        let lock = Self(file);
        let deadline = tokio::time::Instant::now() + Self::WAIT;
        let mut killed = false;
        while !lock.try_lock()? {
            if !killed {
                lock.kill_holder();
                killed = true;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("a training run from an earlier process still holds the GPU".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(lock)
    }

    #[cfg(unix)]
    fn try_lock(&self) -> Result<bool, String> {
        use rustix::fs::{FlockOperation, flock};
        match flock(&self.0, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(true),
            Err(rustix::io::Errno::WOULDBLOCK) => Ok(false),
            Err(e) => Err(format!("cannot lock the trainer: {e}")),
        }
    }

    #[cfg(not(unix))]
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn try_lock(&self) -> Result<bool, String> {
        Ok(true)
    }

    /// Kill the group that the file names.
    fn kill_holder(&self) {
        let mut text = String::new();
        let _ = (&self.0).read_to_string(&mut text);
        let group = Group(text.trim().parse().ok());
        group.kill();
    }

    /// A copy of the locked file, for the stdin of the run.
    fn stdin(&self) -> Result<Stdio, String> {
        self.0
            .try_clone()
            .map(Stdio::from)
            .map_err(|e| format!("cannot share the trainer lock: {e}"))
    }

    /// Write the group of the run into the file. The run holds the lock
    /// through its stdin, so this handle can close.
    fn hold(self, group: Option<u32>) -> Result<(), String> {
        use std::io::Write as _;
        let mut file = self.0;
        file.set_len(0)
            .and_then(|()| file.seek(SeekFrom::Start(0)).map(drop))
            .and_then(|()| writeln!(file, "{}", group.unwrap_or(0)))
            .map_err(|e| format!("cannot write the trainer lock: {e}"))
    }
}

/// The end of the log at `path`: at most `bytes` bytes and [`TAIL_LINES`]
/// lines, as lossy UTF-8. A carriage return counts as a line end, so a
/// progress line that redraws itself reads as its last state.
fn tail_of(path: &Path, bytes: u64) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    let len = file.metadata().map_or(0, |meta| meta.len());
    let mut buffer = Vec::new();
    if file
        .seek(SeekFrom::Start(len.saturating_sub(bytes)))
        .is_err()
        || file.take(bytes).read_to_end(&mut buffer).is_err()
    {
        return String::new();
    }
    let text = String::from_utf8_lossy(&buffer).replace('\r', "\n");
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    lines[lines.len().saturating_sub(TAIL_LINES)..].join("\n")
}

/// The last non-empty line of `path`, for a heartbeat.
fn last_line(path: &Path) -> Option<String> {
    tail_of(path, 4 * 1024)
        .lines()
        .next_back()
        .map(|line| line.trim().to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use super::*;
    use crate::summary;

    /// The hyperparameter block of upstream `train.py`, lines 432 to 451,
    /// copied from karpathy/autoresearch (MIT).
    const UPSTREAM_BLOCK: &str = r#"# Model architecture
ASPECT_RATIO = 64       # model_dim = depth * ASPECT_RATIO
HEAD_DIM = 128          # target head dimension for attention
WINDOW_PATTERN = "SSSL" # sliding window pattern: L=full, S=half context

# Optimization
TOTAL_BATCH_SIZE = 2**19 # ~524K tokens per optimizer step
EMBEDDING_LR = 0.6      # learning rate for token embeddings (Adam)
UNEMBEDDING_LR = 0.004  # learning rate for lm_head (Adam)
MATRIX_LR = 0.04        # learning rate for matrix parameters (Muon)
SCALAR_LR = 0.5         # learning rate for per-layer scalars (Adam)
WEIGHT_DECAY = 0.2      # cautious weight decay for Muon
ADAM_BETAS = (0.8, 0.95) # Adam beta1, beta2
WARMUP_RATIO = 0.0      # fraction of time budget for LR warmup
WARMDOWN_RATIO = 0.5    # fraction of time budget for LR warmdown
FINAL_LR_FRAC = 0.0     # final LR as fraction of initial

# Model size
DEPTH = 8               # number of transformer layers
DEVICE_BATCH_SIZE = 128  # per-device batch size (reduce if OOM)
"#;

    fn bpb(text: &str) -> Option<f64> {
        summary::parse(&simulate(text).log).map(|s| s.val_bpb)
    }

    #[test]
    fn the_upstream_defaults_score_near_the_upstream_baseline() {
        let run = simulate(UPSTREAM_BLOCK);
        assert_eq!(run.exit_code, 0);
        let summary = summary::parse(&run.log).unwrap();
        assert!(
            (summary.val_bpb - 0.9979).abs() < 0.001,
            "{}",
            summary.val_bpb
        );
        assert!((summary.peak_vram_mb - 45_060.2).abs() < 1.0);
    }

    #[test]
    fn the_same_text_scores_the_same() {
        assert_eq!(simulate(UPSTREAM_BLOCK), simulate(UPSTREAM_BLOCK));
    }

    #[test]
    fn a_step_towards_the_optimum_scores_better() {
        let base = bpb(UPSTREAM_BLOCK).unwrap();
        let better = UPSTREAM_BLOCK.replace("MATRIX_LR = 0.04 ", "MATRIX_LR = 0.05 ");
        assert!(bpb(&better).unwrap() < base - 0.001);
        let worse = UPSTREAM_BLOCK.replace("WARMDOWN_RATIO = 0.5 ", "WARMDOWN_RATIO = 0.1 ");
        assert!(bpb(&worse).unwrap() > base);
    }

    #[test]
    fn a_model_too_large_for_the_gpu_crashes_out_of_memory() {
        let big = UPSTREAM_BLOCK.replace("DEPTH = 8 ", "DEPTH = 12 ");
        let run = simulate(&big);
        assert_eq!(run.exit_code, 1);
        assert!(run.log.contains("CUDA out of memory"));
        assert_eq!(summary::parse(&run.log), None);
        // The fix that upstream suggests: reduce the device batch size.
        let fixed = big.replace("DEVICE_BATCH_SIZE = 128 ", "DEVICE_BATCH_SIZE = 64 ");
        assert!(bpb(&fixed).is_some());
    }

    #[test]
    fn a_learning_rate_far_too_high_diverges() {
        let wild = UPSTREAM_BLOCK.replace("MATRIX_LR = 0.04 ", "MATRIX_LR = 0.4 ");
        let run = simulate(&wild);
        assert_eq!(run.exit_code, 1);
        assert!(run.log.contains("NaN"));
    }

    #[test]
    fn a_line_that_does_not_parse_is_a_syntax_error() {
        let broken = UPSTREAM_BLOCK.replace("DEPTH = 8 ", "DEPTH = = 8 ");
        let run = simulate(&broken);
        assert_eq!(run.exit_code, 1);
        assert!(run.log.contains("SyntaxError"));
    }

    #[test]
    fn valid_python_values_are_not_syntax_errors() {
        // The same setting scores the same, up to the noise of the text.
        let underscores = UPSTREAM_BLOCK.replace("2**19", "524_288");
        let delta = (bpb(&underscores).unwrap() - bpb(UPSTREAM_BLOCK).unwrap()).abs();
        assert!(delta < 0.0005, "{delta}");
        let flag = format!("{UPSTREAM_BLOCK}USE_FLASH = True\n");
        assert_eq!(simulate(&flag).exit_code, 0);
        let expression = UPSTREAM_BLOCK.replace("DEPTH = 8 ", "DEPTH = 4 * 2 ");
        assert_eq!(simulate(&expression).exit_code, 0);
    }

    #[test]
    fn a_deleted_setting_falls_back_to_its_default() {
        let shorter = UPSTREAM_BLOCK.replace(
            "WARMUP_RATIO = 0.0      # fraction of time budget for LR warmup\n",
            "",
        );
        let delta = (bpb(&shorter).unwrap() - bpb(UPSTREAM_BLOCK).unwrap()).abs();
        assert!(delta < 0.0005, "{delta}");
    }

    #[tokio::test]
    async fn the_simulated_trainer_writes_run_log_in_the_lab() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("train.py"), UPSTREAM_BLOCK).unwrap();
        let mut lines = 0;
        let run = run(&TrainerSpec::default(), dir.path(), dir.path(), |_| {
            lines += 1;
        })
        .await
        .unwrap();
        assert_eq!(run.exit_code, Some(0));
        assert!(!run.timed_out);
        assert!(run.log_tail.contains("val_bpb:"));
        let log = std::fs::read_to_string(dir.path().join(RUN_LOG)).unwrap();
        assert!(log.ends_with(&format!("{}\n", run.log_tail)));
        assert!(lines > 0);
    }

    #[tokio::test]
    async fn the_command_trainer_captures_the_summary() {
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "echo step 1; echo '---'; echo 'val_bpb:          0.990000'; \
                 echo 'peak_vram_mb:     1024.0' >&2"
                    .into(),
            ],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        let run = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert_eq!(run.exit_code, Some(0));
        let summary = summary::parse(&run.log_tail).unwrap();
        assert_eq!(summary.val_bpb, 0.99);
        assert_eq!(summary.peak_vram_mb, 1024.0);
    }

    #[tokio::test]
    async fn the_command_trainer_kills_a_run_at_its_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "echo started; sleep 30".into()],
            timeout_secs: 1,
            pass_env: Vec::new(),
        };
        let started = std::time::Instant::now();
        let run = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert!(run.timed_out);
        assert_eq!(run.exit_code, None);
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(run.log_tail.contains("started"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_timeout_kills_the_children_of_the_run_too() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("child-survived");
        let script = format!("(sleep 2; touch '{}') & wait", marker.display());
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            timeout_secs: 1,
            pass_env: Vec::new(),
        };
        let run = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert!(run.timed_out);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!marker.exists(), "the child of the run outlived the kill");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_dropped_run_kills_its_whole_group() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("child-survived");
        let script = format!("(sleep 2; touch '{}') & wait", marker.display());
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            timeout_secs: 60,
            pass_env: Vec::new(),
        };
        // The runner drops the activity, as Ctrl-C does.
        let dropped = tokio::time::timeout(
            Duration::from_millis(500),
            run(&spec, dir.path(), dir.path(), |_| {}),
        )
        .await;
        assert!(dropped.is_err());
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert!(!marker.exists(), "a child of the dropped run lived on");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_next_run_kills_a_run_that_outlived_its_runner() {
        use std::io::Write as _;
        use std::os::unix::process::CommandExt as _;

        let dir = tempfile::tempdir().unwrap();
        // A run from a runner that died: it holds the lock through its
        // stdin, and the file names its group.
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.path().join(TRAINER_LOCK))
            .unwrap();
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::LockExclusive).unwrap();
        let mut orphan = std::process::Command::new("sleep")
            .arg("60")
            .stdin(lock.try_clone().unwrap())
            .process_group(0)
            .spawn()
            .unwrap();
        writeln!(&lock, "{}", orphan.id()).unwrap();
        drop(lock);

        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "echo fresh".into()],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        let fresh = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert_eq!(fresh.exit_code, Some(0));
        let status = orphan.wait().unwrap();
        assert!(!status.success(), "the old run was killed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_planted_run_log_symlink_is_replaced_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("train.py"), UPSTREAM_BLOCK).unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, "keep me").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join(RUN_LOG)).unwrap();
        run(&TrainerSpec::default(), dir.path(), dir.path(), |_| {})
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep me");
        assert!(!dir.path().join(RUN_LOG).is_symlink());
    }

    #[test]
    fn a_run_gets_only_the_listed_environment() {
        let none: [String; 0] = [];
        for name in [
            "PATH",
            "HOME",
            "LC_ALL",
            "CUDA_VISIBLE_DEVICES",
            "UV_CACHE_DIR",
            "PYTHONPATH",
        ] {
            assert!(passes(name, &none), "{name}");
        }
        for name in [
            "ANTHROPIC_API_KEY",
            "DATABASE_URL",
            "REDIS_URL",
            "AWS_ACCESS_KEY_ID",
            "HF_TOKEN",
            "UV_PUBLISH_TOKEN",
        ] {
            assert!(!passes(name, &none), "{name}");
        }
        // A person can pass a variable by name.
        assert!(passes("DATABASE_URL", &["DATABASE_URL".to_string()]));
    }

    #[test]
    fn a_log_tail_is_bounded_in_bytes_and_reads_a_redrawn_line() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join(RUN_LOG);
        let redraw = "\rstep 00042".repeat(100_000);
        std::fs::write(&log, format!("{redraw}\n---\nval_bpb: 0.99\n")).unwrap();
        let tail = tail_of(&log, TAIL_BYTES);
        assert!(tail.len() <= usize::try_from(TAIL_BYTES).unwrap());
        assert!(tail.ends_with("val_bpb: 0.99"));
        assert_eq!(last_line(&log).as_deref(), Some("val_bpb: 0.99"));
        assert!(tail.lines().count() <= TAIL_LINES);
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "autoresearch-no-such-program".into(),
            args: Vec::new(),
            timeout_secs: 5,
            pass_env: Vec::new(),
        };
        assert!(run(&spec, dir.path(), dir.path(), |_| {}).await.is_err());
    }
}
