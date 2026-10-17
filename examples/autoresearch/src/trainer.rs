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

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, PartialEq)]
pub struct Simulated {
    /// The whole log, as `train.py` would print it.
    pub log: String,
    /// Zero on success, one on a crash.
    pub exit_code: i32,
}

/// Score `train_py` on the landscape. The same text gives the same log.
#[must_use]
pub fn simulate(train_py: &str) -> Simulated {
    let _ = train_py;
    unimplemented!("green phase")
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
    progress: impl FnMut(&str) + Send,
) -> Result<TrainRun, String> {
    let _ = (spec, lab, progress, Duration::ZERO);
    unimplemented!("green phase")
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
        assert!((summary.val_bpb - 0.9979).abs() < 0.001, "{}", summary.val_bpb);
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
        let shorter = UPSTREAM_BLOCK.replace("WARMUP_RATIO = 0.0      # fraction of time budget for LR warmup\n", "");
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
