//! Companion crate for `trait_path_trust.rs`: compiled to an rlib WITHOUT
//! `--emit=mir`, so its bodies never reach the analyzed set.
//!
//! `Clock` is a dependency's type and the `From<Clock> for String` impl —
//! reached through the blanket `impl<T, U: From<T>> Into<U> for T` — is a
//! dependency's body that reads the wall clock. The only std thing about the
//! call rustc prints for it is the qualifying TRAIT.
pub struct Clock;

impl From<Clock> for String {
    fn from(_: Clock) -> Self {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default()
    }
}
