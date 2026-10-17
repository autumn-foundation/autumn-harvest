//! Read the summary block that `train.py` prints at its end.
//!
//! ```text
//! ---
//! val_bpb:          0.997900
//! training_seconds: 300.1
//! peak_vram_mb:     45060.2
//! num_steps:        953
//! ```
//!
//! Upstream reads it with `grep "^val_bpb:\|^peak_vram_mb:" run.log`. No
//! `val_bpb` line means the run crashed.

use serde::{Deserialize, Serialize};

/// The numbers of one finished run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    /// Validation bits per byte. Lower is better.
    pub val_bpb: f64,
    /// Peak GPU memory in MB.
    pub peak_vram_mb: f64,
    /// Training time in seconds, when the log has it.
    pub training_seconds: Option<f64>,
    /// Optimizer steps, when the log has it.
    pub num_steps: Option<u64>,
}

/// Parse the summary of `log`. The last occurrence of each key wins.
///
/// Returns `None` when the log has no finite `val_bpb`, which is a crash. A
/// log with `val_bpb` and no `peak_vram_mb` reports zero memory.
#[must_use]
pub fn parse(log: &str) -> Option<Summary> {
    let last = |key: &str| {
        log.lines()
            .rev()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(':'))
            .and_then(|value| value.trim().parse::<f64>().ok())
    };
    let val_bpb = last("val_bpb").filter(|value| value.is_finite())?;
    Some(Summary {
        val_bpb,
        peak_vram_mb: last("peak_vram_mb").unwrap_or(0.0),
        training_seconds: last("training_seconds"),
        num_steps: log
            .lines()
            .rev()
            .find_map(|line| line.strip_prefix("num_steps:"))
            .and_then(|value| value.trim().parse::<u64>().ok()),
    })
}

/// The last `lines` lines of `text`, as upstream reads a crash with
/// `tail -n 50 run.log`.
#[must_use]
pub fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use super::*;

    const UPSTREAM: &str = "step 00953 (100.0%) | loss: 3.1\n\
        ---\n\
        val_bpb:          0.997900\n\
        training_seconds: 300.1\n\
        total_seconds:    325.9\n\
        peak_vram_mb:     45060.2\n\
        mfu_percent:      39.80\n\
        total_tokens_M:   499.6\n\
        num_steps:        953\n\
        num_params_M:     50.3\n\
        depth:            8\n";

    #[test]
    fn the_upstream_summary_parses() {
        let summary = parse(UPSTREAM).unwrap();
        assert_eq!(summary.val_bpb, 0.9979);
        assert_eq!(summary.peak_vram_mb, 45_060.2);
        assert_eq!(summary.training_seconds, Some(300.1));
        assert_eq!(summary.num_steps, Some(953));
    }

    #[test]
    fn a_traceback_with_no_summary_is_a_crash() {
        let log = "Traceback (most recent call last):\n  File \"train.py\"\n\
                   torch.OutOfMemoryError: CUDA out of memory.\n";
        assert_eq!(parse(log), None);
    }

    #[test]
    fn a_non_finite_val_bpb_is_a_crash() {
        assert_eq!(parse("val_bpb: nan\npeak_vram_mb: 1.0\n"), None);
        assert_eq!(parse("val_bpb: inf\n"), None);
    }

    #[test]
    fn only_a_line_that_starts_with_the_key_counts() {
        // `grep "^val_bpb:"` does not match an indented or quoted key.
        assert_eq!(parse("  val_bpb: 0.5\nprint('val_bpb: 0.4')\n"), None);
    }

    #[test]
    fn the_last_value_wins_and_missing_memory_is_zero() {
        let summary = parse("val_bpb: 1.5\nval_bpb: 1.25\n").unwrap();
        assert_eq!(summary.val_bpb, 1.25);
        assert_eq!(summary.peak_vram_mb, 0.0);
    }

    #[test]
    fn tail_keeps_the_last_lines() {
        assert_eq!(tail("a\nb\nc\n", 2), "b\nc");
        assert_eq!(tail("a\nb", 5), "a\nb");
        assert_eq!(tail("", 3), "");
    }
}
