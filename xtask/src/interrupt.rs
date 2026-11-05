//! Turns Ctrl-C, `SIGTERM` and `SIGHUP` into a flag that a command polls.
//!
//! A shell runs its `EXIT` trap when a signal stops the script. Rust runs no
//! `Drop` and no cleanup code when a signal kills the process. So a command
//! that must clean up after an interrupt holds a [`Guard`].
//!
//! While a guard lives, the signals only set a flag. The command checks
//! [`requested`] between steps, stops its work, and runs its cleanup. A child
//! still stops on Ctrl-C, because `exec` resets a caught signal to its
//! default action. The terminal sends Ctrl-C to the whole foreground group,
//! so the child and this process both see it.
//!
//! The handler only stores to an atomic flag. That store is
//! async-signal-safe.

use std::sync::atomic::{AtomicBool, Ordering};

/// The exit code of a command that an interrupt stopped, as bash reports it.
pub const EXIT_CODE: u8 = 130;

/// Set by the signal handler. Never cleared while a guard lives.
static REQUESTED: AtomicBool = AtomicBool::new(false);

/// Reports whether an interrupt arrived while a guard lived.
pub fn requested() -> bool {
    REQUESTED.load(Ordering::SeqCst)
}

/// Defers the signals to the flag until it drops.
///
/// Only one guard may live at a time. A command installs it once, at the
/// start of its `run`.
#[must_use = "the signals stop the process again when the guard drops"]
pub struct Guard {
    #[cfg(unix)]
    saved: [usize; 3],
}

#[cfg(unix)]
mod signals {
    use std::os::raw::c_int;

    /// `SIGHUP`, `SIGINT` and `SIGTERM`.
    pub const DEFERRED: [c_int; 3] = [1, 2, 15];

    unsafe extern "C" {
        /// The C library `signal` call. A handler is a pointer-sized value.
        pub fn signal(signum: c_int, handler: usize) -> usize;
    }

    /// Records the interrupt. It does nothing else, so it is
    /// async-signal-safe.
    pub extern "C" fn on_signal(_signum: c_int) {
        super::REQUESTED.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Installs the handler for each deferred signal and clears the flag.
pub fn install() -> Guard {
    REQUESTED.store(false, Ordering::SeqCst);
    #[cfg(unix)]
    {
        let handler: extern "C" fn(std::os::raw::c_int) = signals::on_signal;
        let mut saved = [0; 3];
        for (slot, signum) in saved.iter_mut().zip(signals::DEFERRED) {
            // SAFETY: the handler only stores to an atomic, which is
            // async-signal-safe. The call changes only the disposition of
            // this process.
            *slot = unsafe { signals::signal(signum, handler as usize) };
        }
        Guard { saved }
    }
    #[cfg(not(unix))]
    {
        Guard {}
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        #[cfg(unix)]
        for (previous, signum) in self.saved.iter().zip(signals::DEFERRED) {
            // SAFETY: the value came from `signal` for the same signal. So it
            // is a disposition that this process had before.
            unsafe {
                signals::signal(signum, *previous);
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    unsafe extern "C" {
        fn raise(signum: std::os::raw::c_int) -> std::os::raw::c_int;
    }

    /// Set in the child process that runs the real signal test.
    const CHILD: &str = "XTASK_INTERRUPT_TEST_CHILD";

    /// The flag is global to the process. So the signal test runs in a child
    /// process, and a set flag never reaches a test on another thread.
    #[test]
    fn a_guard_turns_an_interrupt_into_the_flag() {
        let exe = std::env::current_exe().expect("the test binary has a path");
        let status = std::process::Command::new(exe)
            .args([
                "--exact",
                "interrupt::tests::signal_under_a_guard",
                "--ignored",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .status()
            .expect("the child test runs");
        assert!(status.success(), "the child signal test failed: {status}");
    }

    /// The child half of the test above. It does nothing in a normal run.
    #[test]
    #[ignore = "runs only as the child process of a_guard_turns_an_interrupt_into_the_flag"]
    fn signal_under_a_guard() {
        if std::env::var_os(CHILD).is_none() {
            return;
        }
        let guard = install();
        assert!(!requested(), "a new guard starts with a clear flag");
        // SAFETY: the guard holds the handler for SIGINT, so the signal only
        // sets the flag and the test process keeps running.
        assert_eq!(unsafe { raise(2) }, 0);
        assert!(
            requested(),
            "SIGINT must set the flag while the guard lives"
        );
        // SAFETY: as above, for SIGTERM.
        assert_eq!(unsafe { raise(15) }, 0);
        drop(guard);
        let again = install();
        assert!(!requested(), "a new guard clears the flag");
        drop(again);
    }
}
