//! An untrusted dependency with no MIR in the analyzed set (issue #2010).
use std::future::Future;

/// Reads the clock, so a call to it must stay a boundary.
pub fn wrap<F: Future>(f: F) -> F {
    if std::time::SystemTime::now().elapsed().is_ok() { f } else { f }
}
