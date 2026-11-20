//! Keep the tracing callsite cache stable in the library tests.
//!
//! tracing-core caches an `Interest` for each callsite. A new dispatcher
//! sets a flag when it finds no other live dispatcher. While that flag is
//! on, the first registration of a callsite asks only the dispatcher of its
//! own thread, and it holds no lock. So another test thread can store
//! `never` after a log test made its capture. The span of that test then
//! goes disabled (issue #2762, `docs/design/DESIGN-2762.md`).
//!
//! A constructor makes two keepers before `main`. The second keeper finds
//! the first one live, so the flag goes off before any test runs. One
//! keeper is not enough: it finds no other dispatcher, so the flag stays on.

use std::sync::OnceLock;

/// The keepers. They live for the whole process.
///
/// Each is a `NoSubscriber`. It answers `never` and enables nothing. It has
/// no level hint, so the max level stays at TRACE. A test sees the same
/// spans and events.
static KEEPERS: OnceLock<[tracing::Dispatch; 2]> = OnceLock::new();

/// Make the keepers before `main`.
///
/// Safety argument for `ctor(unsafe)`: the body uses only the allocator and
/// the locks of tracing-core. Its only `unwrap` is on a lock that cannot be
/// poisoned before `main`.
#[ctor::ctor(unsafe)]
fn install() {
    KEEPERS.get_or_init(|| [keeper(), keeper()]);
}

/// One keeper dispatcher.
fn keeper() -> tracing::Dispatch {
    tracing::Dispatch::new(tracing::subscriber::NoSubscriber::new())
}

mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::Duration;

    use tracing::subscriber::Interest;
    use tracing::{Event, Metadata, span};

    /// The name of the thread that registers the probe callsite first.
    const FIRST: &str = "trace-keepers-first";

    /// The longest time that the gate holds a registration.
    ///
    /// With the keepers live, each new dispatcher of the binary waits this
    /// long, so it is short.
    const HOLD: Duration = Duration::from_millis(250);

    /// The span that only this test uses.
    fn probe() -> tracing::Span {
        tracing::info_span!("trace_keepers.probe")
    }

    /// A subscriber that holds the first registration of the probe.
    ///
    /// It holds only on the thread `FIRST`, so a rebuild on another thread
    /// does not wait. It answers `never`, as a thread with no dispatcher
    /// does.
    struct Gate {
        held: AtomicBool,
        entered: Mutex<Sender<()>>,
        release: Mutex<Receiver<()>>,
    }

    impl tracing::Subscriber for Gate {
        fn register_callsite(&self, meta: &'static Metadata<'static>) -> Interest {
            let first = std::thread::current().name() == Some(FIRST);
            if first
                && meta.name() == "trace_keepers.probe"
                && !self.held.swap(true, Ordering::SeqCst)
            {
                let entered = self
                    .entered
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // The test can be gone. Then nobody waits for this signal.
                let _ = entered.send(());
                drop(entered);
                let release = self
                    .release
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // With the keepers, the capture waits for this lock. So the
                // release comes after this timeout.
                let _ = release.recv_timeout(HOLD);
            }
            Interest::never()
        }

        fn enabled(&self, _: &Metadata<'_>) -> bool {
            false
        }

        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

        fn event(&self, _: &Event<'_>) {}

        fn enter(&self, _: &span::Id) {}

        fn exit(&self, _: &span::Id) {}
    }

    // Issue #2762.
    #[test]
    fn the_keepers_are_live_before_any_test() {
        assert!(
            super::KEEPERS.get().is_some(),
            "the constructor did not run"
        );
    }

    // Issue #2762.
    #[test]
    fn a_late_first_registration_cannot_disable_a_live_capture() {
        let (entered_tx, entered_rx) = channel();
        let (release_tx, release_rx) = channel();
        let gate = tracing::Dispatch::new(Gate {
            held: AtomicBool::new(false),
            entered: Mutex::new(entered_tx),
            release: Mutex::new(release_rx),
        });
        let first = std::thread::Builder::new()
            .name(FIRST.to_owned())
            .spawn(move || tracing::dispatcher::with_default(&gate, || drop(probe())))
            .expect("spawn the first thread");
        entered_rx
            .recv_timeout(HOLD * 40)
            .expect("the first thread registers the probe");
        // The capture comes while the first registration is open.
        let capture = tracing::Dispatch::new(tracing_subscriber::registry());
        // The first thread can have timed out already.
        let _ = release_tx.send(());
        first.join().expect("the first thread ends");
        let enabled = tracing::dispatcher::with_default(&capture, || !probe().is_disabled());
        assert!(enabled, "a late first registration disabled the probe span");
    }
}
