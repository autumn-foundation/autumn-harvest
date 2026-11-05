//! `cargo xtask audit vantage-degraded-banner-focus`: the port of
//! `docs/audits/vantage-degraded-banner-focus.py`.
//!
//! This is the full-page-load status-message announcement audit for the
//! degraded-state banners of the Vantage dashboard
//! (`autumn-harvest-plugin/src/ui.rs`).
//!
//! The audit is deterministic and reproducible on any checkout. It needs no
//! server, no browser and no network access. It is the `div.degraded-banner`
//! counterpart to `cargo xtask audit vantage-flash-live-region`, which audits
//! `div.flash` for the same mechanism. The two classes render the same kind
//! of content: a status message that the operator must not miss. Both render
//! through the same full-page-load path, so they need the same fix. The flash
//! audit scopes its scan to a `class` that contains "flash". It does not see
//! `degraded-banner` at all.
//!
//! A `div.degraded-banner` is how Vantage tells an operator that the page is
//! incomplete or wrong. Examples are a rejected form submission ("Backfill
//! not started"), and a paused or exhausted schedule that silently has no
//! fire times to show. Another is a cross-shard read that came back partial
//! or unavailable. Its banner says "this history and its summary cover only
//! the shards that answered; counts may be understated". The source comment
//! at the `runs` page call site names this explicitly: "a partial
//! cross-shard answer is always visible, never silently truncated data".
//! Each of these is a full HTTP response to a GET or a redirected POST. None
//! is a client-side update to a page already on screen.
//!
//! Two things must both be true for that message to reach a screen-reader
//! user, and this audit checks both:
//!
//! 1. **A live-region role.** That is `role="status"` or `role="alert"`,
//!    matched only as a real attribute and not as a substring of
//!    `data-role="status"`. An `aria-live` attribute counts only with an
//!    active value, `polite` or `assertive`. The value `aria-live="off"` is
//!    explicitly not live.
//! 2. **Programmatic focus on load.** That is `tabindex="-1"` *and* a bare
//!    `autofocus`. A live-region role alone does not reliably announce
//!    content that is present when the accessibility tree is first built.
//!    ARIA live regions fire on *changes after* registration, not on initial
//!    content (WAI-ARIA Authoring Practices Guide, "Live Region Perceivable
//!    Author Practices"). The fix for a full page load needs no JavaScript:
//!    it moves focus to the message. The `tabindex="-1"` attribute makes a
//!    non-interactive `<div>` programmatically focusable without adding it to
//!    the tab order. The `autofocus` attribute makes the browser focus it
//!    when the page finishes loading. Assistive tech then announces the
//!    content of the focused element. The `role="status"` attribute stays
//!    alongside it for the correct semantic. On its own, it is not
//!    sufficient here. Each `div.flash` in the same file already carries both
//!    halves at every render site. This audit exists because
//!    `div.degraded-banner` did not.
//!
//! Run `cargo xtask audit vantage-degraded-banner-focus`. The command exits 1
//! if any render site lacks either half of the mechanism, and 0 otherwise.
//! So it is safe as a CI gate, or as a standalone report. The scan itself
//! lives in `vantage_flash_live_region.rs`. Its pattern comments apply to
//! the "degraded-banner" token here exactly as they apply to "flash" there.

use std::process::ExitCode;

use smop::prelude::*;

use super::vantage_flash_live_region::{StatusSpec, run_status_audit};

/// Arguments of `cargo xtask audit vantage-degraded-banner-focus`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// The spec of `cargo xtask audit vantage-degraded-banner-focus`.
const DEGRADED_BANNER: StatusSpec = StatusSpec {
    class: "degraded-banner",
    none_found: &[
        "ERROR: found no `div.degraded-banner` render sites at all — no `div`",
        "in ui.rs carries a \"degraded-banner\" class this audit recognizes.",
        "Either the markup was rewritten in a spelling this audit doesn't",
        "recognize (a silent pass would hide that), or degraded banners were",
        "removed outright.",
    ],
};

/// Runs `cargo xtask audit vantage-degraded-banner-focus`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    run_status_audit(&DEGRADED_BANNER)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::audit::vantage_flash_live_region::report;

    #[test]
    fn a_complete_banner_passes() {
        let src = "div.notice.degraded-banner role=\"alert\" tabindex=\"-1\" autofocus {}\n";
        let (lines, passed) = report(src, "ui.rs", &DEGRADED_BANNER);
        assert!(passed, "{lines:?}");
        assert_eq!(lines[1], "1 `div.degraded-banner` render sites checked.\n");
    }

    #[test]
    fn a_banner_without_focus_fails() {
        let src = "\ndiv class=\"degraded-banner\" role=\"status\" {}\n";
        let (lines, passed) = report(src, "ui.rs", &DEGRADED_BANNER);
        assert!(!passed);
        assert_eq!(
            lines[3],
            "  ui.rs:2  no tabindex=\"-1\"+autofocus (won't announce on a full page load)"
        );
    }

    #[test]
    fn a_flash_is_not_a_banner() {
        let (lines, passed) = report("div.flash {}\n", "ui.rs", &DEGRADED_BANNER);
        assert!(!passed);
        assert!(lines[2].starts_with("ERROR: found no `div.degraded-banner`"));
    }
}
