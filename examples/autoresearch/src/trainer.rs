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
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};

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
/// as a database URL stays out of code that the model wrote. A listed
/// variable whose value is a URL with login details stays out too.
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
    /// The longest one run can take, in seconds.
    #[must_use]
    pub const fn longest_secs(&self) -> u64 {
        match self {
            Self::Simulated { seconds } => *seconds,
            Self::Command { timeout_secs, .. } => *timeout_secs,
        }
    }

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
            let train_py = crate::stores::read_regular(&lab.join("train.py"))
                .map_err(|e| format!("cannot read train.py: {e}"))?
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .ok_or("cannot read train.py: it does not exist")?;
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

/// Hide the memory and the environment of this process from the other
/// processes of the user, such as a training run.
///
/// The run is code that the model wrote, and it runs as the same user. It
/// could read the Claude key from `/proc/<runner>/environ`. On Linux the
/// runner stops being dumpable, so only root can read it. An exec resets
/// the flag, so a run is not hidden.
///
/// # Errors
///
/// Returns an error when the kernel refuses.
pub fn hide_runner() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    rustix::process::set_dumpable_behavior(rustix::process::DumpableBehavior::NotDumpable)
        .map_err(|e| format!("cannot hide the runner from its training runs: {e}"))?;
    Ok(())
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

/// `true` when a training run gets the variable `name` with `value`.
///
/// A run gets a name that `extra` lists. It also gets a name of
/// [`PASSED_ENV`] that marks no secret, with a value that holds no login
/// details.
pub(crate) fn passes(name: &str, value: &str, extra: &[String]) -> bool {
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
    listed && !SECRET_MARKS.iter().any(|mark| upper.contains(mark)) && !has_login(value)
}

/// `true` when `value` holds a URL with login details, such as
/// `https://user:token@host`. A private package index is set this way.
fn has_login(value: &str) -> bool {
    value.split("://").skip(1).any(|rest| {
        let authority = rest
            .split(['/', '?', '#', ' ', '\t', '\n'])
            .next()
            .unwrap_or("");
        authority.contains('@')
    })
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
    // The run can put a symlink at `run.log`. The runner reads and appends
    // through this handle, so the path never sends it to another file.
    let log = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(log_path)
        .map_err(|e| format!("cannot create run.log: {e}"))?;
    let reader = LogReader::new(&log, log_path)?;
    let share = || {
        log.try_clone()
            .map_err(|e| format!("cannot share run.log: {e}"))
    };
    let (output, errors) = (share()?, share()?);
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .current_dir(lab)
        .stdin(lock.stdin()?)
        .stdout(output)
        .stderr(errors)
        .kill_on_drop(true);
    command.env_clear();
    for (name, value) in std::env::vars_os() {
        if passes(&name.to_string_lossy(), &value.to_string_lossy(), pass_env) {
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
    // The group dies when this guard drops, on every path out. The lock
    // drops after it and clears the file.
    let group = Group::of(&child);
    let id = child.id().unwrap_or(0);
    lock.name(id, group_start(id).unwrap_or(0))?;
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
                if let Some(line) = last_line(&reader) {
                    progress(&line);
                }
            }
        }
    };
    // A child that the run left behind dies now too.
    drop(group);
    if timed_out {
        // The group is dead, so nothing else writes at the end of the log.
        let note = format!("\nautoresearch: killed after the {timeout_secs}s time budget\n");
        let mut file = &log;
        file.seek(SeekFrom::End(0))
            .and_then(|_| file.write_all(note.as_bytes()))
            .map_err(|e| format!("cannot write run.log: {e}"))?;
    }
    Ok(TrainRun {
        exit_code,
        timed_out,
        log_tail: tail_of(&reader, TAIL_BYTES),
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
/// run kills that group first. The file also holds the start time of the
/// group leader. The next run kills the group only when that time still
/// matches, so a group that reused the id lives on. Off Linux no start time
/// can be read, so the next run kills nothing. It waits and fails instead.
///
/// A group id in the file can name another group once its run ends. So a
/// run writes zero when it ends, on every path out, and the run that takes
/// a free lock writes zero first. A child that left the group can keep the
/// lock after its run. A runner that dies between the start of its run and
/// the write of the group leaves zero too. In both cases the next run kills
/// nothing. It waits [`Lock::WAIT`] and fails, and the activity retries.
struct Lock(std::fs::File);

/// Kill a run that an earlier runner left behind, and wait until it lets
/// go of the lock. A train call that comes again after its runner stopped
/// does this before it puts the lab back.
///
/// # Errors
///
/// Returns an error when the lock file cannot be opened, or a run holds
/// the lock past the wait.
pub async fn reap(state: &Path) -> Result<(), String> {
    Lock::take(state).await.map(drop)
}

impl Lock {
    /// How long a run waits for a killed group to let go of the lock.
    const WAIT: Duration = Duration::from_secs(10);

    async fn take(state: &Path) -> Result<Self, String> {
        let lock = Self(crate::stores::open_lock(&state.join(TRAINER_LOCK))?);
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
        lock.name(0, 0)?;
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

    /// Kill the group that the file names, when it is still that group.
    ///
    /// The start time of the group leader must match the file, so a group
    /// that reused the id lives on. Off Linux no start time can be read, so
    /// no group is killed.
    fn kill_holder(&self) {
        let mut text = String::new();
        let _ = (&self.0).read_to_string(&mut text);
        let mut fields = text.split_whitespace();
        let Some(id) = fields.next().and_then(|f| f.parse::<u32>().ok()) else {
            return;
        };
        let start = fields.next().and_then(|f| f.parse::<u64>().ok());
        if start.is_none() || group_start(id) != start {
            return;
        }
        Group(i32::try_from(id).ok()).kill();
    }

    /// A copy of the locked file, for the stdin of the run.
    fn stdin(&self) -> Result<Stdio, String> {
        self.0
            .try_clone()
            .map(Stdio::from)
            .map_err(|e| format!("cannot share the trainer lock: {e}"))
    }

    /// Write `group` and the start time of its leader into the file. Zero
    /// names no group.
    fn name(&self, group: u32, start: u64) -> Result<(), String> {
        use std::io::Write as _;
        let mut file = &self.0;
        file.set_len(0)
            .and_then(|()| file.seek(SeekFrom::Start(0)).map(drop))
            .and_then(|()| writeln!(file, "{group} {start}"))
            .map_err(|e| format!("cannot write the trainer lock: {e}"))
    }
}

impl Drop for Lock {
    /// The run has ended, so its group is dead. The file names no group.
    fn drop(&mut self) {
        let _ = self.name(0, 0);
    }
}

/// The start time of process `id`, in clock ticks after boot, when it leads
/// its own process group. `None` off Linux, or when no such group lives.
fn group_start(id: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{id}/stat")).ok()?;
    // Field 2, the name, can hold spaces and parentheses. Count from its end.
    let fields: Vec<&str> = stat
        .get(stat.rfind(')')? + 1..)?
        .split_whitespace()
        .collect();
    // `fields` starts at field 3. The group is field 5, the start time 22.
    let group: u32 = fields.get(2)?.parse().ok()?;
    (group == id).then(|| fields.get(19)?.parse().ok())?
}

/// Reads a log that a run writes, through a handle and not by path.
///
/// On Unix it reads the handle that the run writes to, at an offset, so the
/// offset of the run does not move. Off Unix it is a second handle that is
/// opened before the run starts.
struct LogReader<'a> {
    #[cfg(unix)]
    file: &'a std::fs::File,
    #[cfg(not(unix))]
    file: std::fs::File,
    #[cfg(not(unix))]
    owner: std::marker::PhantomData<&'a std::fs::File>,
}

impl<'a> LogReader<'a> {
    #[cfg(unix)]
    #[allow(clippy::unnecessary_wraps)]
    const fn new(file: &'a std::fs::File, _path: &Path) -> Result<Self, String> {
        Ok(Self { file })
    }

    #[cfg(not(unix))]
    fn new(_file: &'a std::fs::File, path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("cannot read run.log: {e}"))?;
        Ok(Self {
            file,
            owner: std::marker::PhantomData,
        })
    }

    /// The bytes from `start` to the end, at most `limit` of them.
    #[cfg(unix)]
    fn read(&self, start: u64, limit: u64) -> std::io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt as _;
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 8192];
        while (buffer.len() as u64) < limit {
            let at = start + buffer.len() as u64;
            let n = self.file.read_at(&mut chunk, at)?;
            if n == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..n]);
        }
        buffer.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(buffer)
    }

    /// The bytes from `start` to the end, at most `limit` of them.
    #[cfg(not(unix))]
    fn read(&self, start: u64, limit: u64) -> std::io::Result<Vec<u8>> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(start))?;
        let mut buffer = Vec::new();
        file.take(limit).read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn len(&self) -> u64 {
        self.file.metadata().map_or(0, |meta| meta.len())
    }
}

/// The end of a log: at most `bytes` bytes and [`TAIL_LINES`] lines, as
/// lossy UTF-8. A carriage return counts as a line end, so a progress line
/// that redraws itself reads as its last state.
fn tail_of(log: &LogReader<'_>, bytes: u64) -> String {
    let Ok(buffer) = log.read(log.len().saturating_sub(bytes), bytes) else {
        return String::new();
    };
    let text = String::from_utf8_lossy(&buffer).replace('\r', "\n");
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    lines[lines.len().saturating_sub(TAIL_LINES)..].join("\n")
}

/// The last non-empty line of a log, for a heartbeat.
fn last_line(log: &LogReader<'_>) -> Option<String> {
    tail_of(log, 4 * 1024)
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

    /// The kill needs the start time from `/proc`, so it runs on Linux only.
    #[cfg(target_os = "linux")]
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
        let start = group_start(orphan.id()).unwrap_or(0);
        writeln!(&lock, "{} {start}", orphan.id()).unwrap();
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

    /// A run that ended leaves no group id in the lock file. A child that
    /// left the group can keep the lock, and the old id can name another
    /// group by then.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_finished_run_names_no_group() {
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "true".into()],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        let text = std::fs::read_to_string(dir.path().join(TRAINER_LOCK)).unwrap();
        assert_eq!(text.trim(), "0 0");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_free_lock_forgets_the_group_of_an_earlier_run() {
        let dir = tempfile::tempdir().unwrap();
        // The group of a run that ended. Its id can name another group now.
        std::fs::write(dir.path().join(TRAINER_LOCK), "424242\n").unwrap();
        let lock = Lock::take(dir.path()).await.unwrap();
        let text = std::fs::read_to_string(dir.path().join(TRAINER_LOCK)).unwrap();
        assert_eq!(text.trim(), "0 0");
        drop(lock);
    }

    /// A run can plant a link or a pipe at the lock file. The next run must
    /// not write the group to the target of a link, or hang on a pipe.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_planted_link_or_pipe_at_the_lock_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join(TRAINER_LOCK);
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "keep me").unwrap();
        std::os::unix::fs::symlink(&victim, &lock).unwrap();
        assert!(Lock::take(dir.path()).await.is_err());
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "keep me");
        std::fs::remove_file(&lock).unwrap();
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &lock,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            0,
        )
        .unwrap();
        let taken = tokio::time::timeout(Duration::from_secs(5), Lock::take(dir.path())).await;
        assert!(taken.expect("no hang").is_err());
    }

    /// The id in the lock file can name another group by now. The start time
    /// tells them apart.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_stale_lock_spares_a_group_that_reused_its_id() {
        use std::os::unix::process::CommandExt as _;

        let dir = tempfile::tempdir().unwrap();
        let mut other = std::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .spawn()
            .unwrap();
        let id = other.id();
        let start = group_start(id).unwrap();
        let path = dir.path().join(TRAINER_LOCK);
        let open = || {
            Lock(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap(),
            )
        };
        // The file names the id with the start time of an earlier group.
        std::fs::write(&path, format!("{id} {}\n", start + 1)).unwrap();
        open().kill_holder();
        assert!(other.try_wait().unwrap().is_none(), "the other group lives");
        // No start time, as off Linux: the group cannot be checked.
        std::fs::write(&path, format!("{id} 0\n")).unwrap();
        open().kill_holder();
        assert!(
            other.try_wait().unwrap().is_none(),
            "an unchecked group lives"
        );
        std::fs::write(&path, format!("{id} {start}\n")).unwrap();
        open().kill_holder();
        assert!(
            !other.wait().unwrap().success(),
            "the group of the run dies"
        );
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

    /// The run is code that the model wrote. It can put a symlink at
    /// `run.log` while it runs. The runner reads the log that the run wrote,
    /// not the file that the symlink names, such as `/proc/self/environ`.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_runner_reads_the_log_the_run_wrote_not_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "hunter2\n").unwrap();
        let script = format!(
            "echo ---; echo 'val_bpb: 0.99'; rm run.log; ln -s '{}' run.log",
            secret.display()
        );
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        let run = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert!(!run.log_tail.contains("hunter2"), "{run:?}");
        assert!(run.log_tail.contains("val_bpb"), "{run:?}");
    }

    /// A run past its time budget gets a note at the end of its log. The
    /// note goes to the log that the run wrote, not through a symlink.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_timeout_note_does_not_follow_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        std::fs::write(&target, "keep me").unwrap();
        let script = format!(
            "echo started; rm run.log; ln -s '{}' run.log; sleep 30",
            target.display()
        );
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            timeout_secs: 1,
            pass_env: Vec::new(),
        };
        let run = run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        assert!(run.timed_out);
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep me");
        assert!(run.log_tail.contains("started"), "{run:?}");
        assert!(run.log_tail.contains("time budget"), "{run:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_runner_hides_its_memory_and_environment() {
        hide_runner().unwrap();
        assert_eq!(
            rustix::process::dumpable_behavior().unwrap(),
            rustix::process::DumpableBehavior::NotDumpable
        );
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
            assert!(passes(name, "/x", &none), "{name}");
        }
        for name in [
            "ANTHROPIC_API_KEY",
            "DATABASE_URL",
            "REDIS_URL",
            "AWS_ACCESS_KEY_ID",
            "HF_TOKEN",
            "UV_PUBLISH_TOKEN",
        ] {
            assert!(!passes(name, "/x", &none), "{name}");
        }
        // A listed name can hold a secret in its value: login details in a
        // URL, as a private package index has.
        for value in [
            "https://__token__:pypi-AgEI@pypi.corp/simple",
            "https://ghp_abc@github.com/org/repo",
            "http://pypi.corp/simple https://u:p@mirror/simple",
        ] {
            assert!(!passes("UV_INDEX_URL", value, &none), "{value}");
        }
        assert!(passes("UV_INDEX_URL", "https://pypi.corp/simple", &none));
        assert!(passes("HF_ENDPOINT", "https://hf.co/a@b", &none));
        // A person can pass a variable by name, value and all.
        let named = ["DATABASE_URL".to_string(), "UV_INDEX_URL".to_string()];
        assert!(passes("DATABASE_URL", "postgres://u:p@db/x", &named));
        assert!(passes(
            "UV_INDEX_URL",
            "https://u:p@pypi.corp/simple",
            &named
        ));
    }

    /// The run starts with a clear environment. Cargo gives each test
    /// process its `CARGO_` variables, and the run must not see them.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_run_sees_no_variable_off_the_list() {
        assert!(
            std::env::vars().any(|(name, _)| name.starts_with("CARGO_")),
            "cargo sets CARGO_ variables for a test"
        );
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), "env".into()],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        run(&spec, dir.path(), dir.path(), |_| {}).await.unwrap();
        let seen = std::fs::read_to_string(dir.path().join(RUN_LOG)).unwrap();
        assert!(seen.lines().any(|line| line.starts_with("PATH=")), "{seen}");
        assert!(
            !seen.lines().any(|line| line.starts_with("CARGO_")),
            "{seen}"
        );
    }

    #[test]
    fn a_log_tail_is_bounded_in_bytes_and_reads_a_redrawn_line() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join(RUN_LOG);
        let redraw = "\rstep 00042".repeat(100_000);
        std::fs::write(&log, format!("{redraw}\n---\nval_bpb: 0.99\n")).unwrap();
        let file = std::fs::File::open(&log).unwrap();
        let reader = LogReader::new(&file, &log).unwrap();
        let tail = tail_of(&reader, TAIL_BYTES);
        assert!(tail.len() <= usize::try_from(TAIL_BYTES).unwrap());
        assert!(tail.ends_with("val_bpb: 0.99"));
        assert_eq!(last_line(&reader).as_deref(), Some("val_bpb: 0.99"));
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
