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

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

/// The file that each run writes, as upstream redirects it.
pub const RUN_LOG: &str = "run.log";

/// The lines of the log that a run returns, as `tail -n 50 run.log`.
pub const TAIL_LINES: usize = 50;

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

/// A number, `a**b`, a quoted string, or a tuple. Anything else does not parse.
fn parse_value(code: &str) -> Option<Value> {
    if code.is_empty() || code.starts_with('=') {
        return None;
    }
    if let Some((base, exp)) = code.split_once("**") {
        let base: f64 = base.trim().parse().ok()?;
        let exp: i32 = exp.trim().parse().ok()?;
        return Some(Value::Number(base.powi(exp)));
    }
    if let Ok(number) = code.parse::<f64>() {
        return Some(Value::Number(number));
    }
    let quoted = (code.starts_with('"') && code.ends_with('"'))
        || (code.starts_with('\'') && code.ends_with('\''));
    if quoted && code.len() >= 2 {
        return Some(Value::Text(code[1..code.len() - 1].to_string()));
    }
    (code.starts_with('(') && code.ends_with(')')).then_some(Value::Other)
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

/// Run one training of the lab at `lab`.
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
        } => run_command(program, args, *timeout_secs, lab, &log_path, &mut progress).await,
    }
}

/// Run `program` with its output in `log_path`. Kill it at the timeout.
async fn run_command(
    program: &str,
    args: &[String],
    timeout_secs: u64,
    lab: &Path,
    log_path: &Path,
    progress: &mut (impl FnMut(&str) + Send),
) -> Result<TrainRun, String> {
    let log = std::fs::File::create(log_path).map_err(|e| format!("cannot create run.log: {e}"))?;
    let errors = log
        .try_clone()
        .map_err(|e| format!("cannot share run.log: {e}"))?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
        .current_dir(lab)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(errors)
        .kill_on_drop(true);
    // A new process group, so a kill reaches the children of the run too.
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| format!("cannot start {program}: {e}"))?;
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
                kill_group(&child);
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
    let text = std::fs::read_to_string(log_path).unwrap_or_default();
    Ok(TrainRun {
        exit_code,
        timed_out,
        log_tail: crate::summary::tail(&text, TAIL_LINES),
    })
}

/// Kill the process group of `child`. A no-op off Unix, where the kill of
/// the child alone must do.
fn kill_group(child: &tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw)
    {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    #[cfg(not(unix))]
    let _ = child;
}

/// The last non-empty line of `path`, for a heartbeat.
fn last_line(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
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
        let run = run(&TrainerSpec::default(), dir.path(), |_| lines += 1)
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
        };
        let run = run(&spec, dir.path(), |_| {}).await.unwrap();
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
        };
        let started = std::time::Instant::now();
        let run = run(&spec, dir.path(), |_| {}).await.unwrap();
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
        };
        let run = run(&spec, dir.path(), |_| {}).await.unwrap();
        assert!(run.timed_out);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!marker.exists(), "the child of the run outlived the kill");
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let spec = TrainerSpec::Command {
            program: "autoresearch-no-such-program".into(),
            args: Vec::new(),
            timeout_secs: 5,
        };
        assert!(run(&spec, dir.path(), |_| {}).await.is_err());
    }
}
