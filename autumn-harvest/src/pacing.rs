//! Verified-by-construction pacing policy resolution.
//!
//! Overrides are replicated as configuration to every shard, while each shard
//! owns and debits its token buckets independently. They are deliberately not
//! workflow-history events: admission and dispatch consult wall-clock time.

use chrono::{DateTime, Utc};

/// A positive, finite pacing quantity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Positive(f64);

impl Positive {
    /// Constructs a value, rejecting zero, negatives, NaN, and infinity.
    pub fn new(value: f64) -> Result<Self, &'static str> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err("pacing value must be finite and greater than zero")
        }
    }

    /// Returns the represented value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

/// Declared policy, which remains the immutable baseline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Baseline {
    pub refill_per_sec: Positive,
    pub burst: Positive,
}

/// A durable, partial, expiring operator override.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Override {
    pub refill_per_sec: Option<Positive>,
    pub burst: Option<Positive>,
    pub expires_at: DateTime<Utc>,
}

/// Effective policy plus read-model metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Resolution {
    pub refill_per_sec: Positive,
    pub burst: Positive,
    pub override_active: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Resolves at the consumption instant. Equality is expired (`now < expires_at`).
#[must_use]
pub fn resolve(baseline: Baseline, candidate: Option<Override>, now: DateTime<Utc>) -> Resolution {
    match candidate.filter(|value| now < value.expires_at) {
        Some(value) => Resolution {
            refill_per_sec: value.refill_per_sec.unwrap_or(baseline.refill_per_sec),
            burst: value.burst.unwrap_or(baseline.burst),
            override_active: true,
            expires_at: Some(value.expires_at),
        },
        None => Resolution {
            refill_per_sec: baseline.refill_per_sec,
            burst: baseline.burst,
            override_active: false,
            expires_at: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn positive(value: f64) -> Positive {
        Positive::new(value).expect("positive fixture")
    }
    fn baseline() -> Baseline {
        Baseline {
            refill_per_sec: positive(10.0),
            burst: positive(20.0),
        }
    }

    #[test]
    fn partial_override_inherits_and_expiry_restores_baseline() {
        let now = Utc::now();
        let candidate = Override {
            refill_per_sec: Some(positive(2.0)),
            burst: None,
            expires_at: now + Duration::seconds(1),
        };
        let active = resolve(baseline(), Some(candidate), now);
        assert_eq!(
            (
                active.refill_per_sec.get(),
                active.burst.get(),
                active.override_active
            ),
            (2.0, 20.0, true)
        );
        assert_eq!(
            resolve(baseline(), Some(candidate), candidate.expires_at)
                .refill_per_sec
                .get(),
            10.0
        );
        assert!(!resolve(baseline(), Some(candidate), candidate.expires_at).override_active);
    }

    #[test]
    fn impossible_values_are_rejected() {
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Positive::new(value).is_err());
        }
    }
}
