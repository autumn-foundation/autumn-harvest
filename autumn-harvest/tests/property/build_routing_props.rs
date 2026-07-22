//! Property tests for [`autumn_harvest::build_routing`] ramp helpers (Tier A
//! target 3). The `build_routing` module is `#[cfg(feature = "db")]`, so this
//! suite only compiles/runs under a `db` build.
//!
//! Contract under test:
//! - `ramp_bucket` is a **pure, deterministic** function of the `ExecutionId`
//!   (issue #604 AC3): the same id maps to the same bucket on every call;
//! - the bucket is always in `0..100`;
//! - `validate_ramp_percent` accepts exactly `0..=100` and rejects everything
//!   else, never panicking.

use autumn_harvest::build_routing::{ramp_bucket, validate_ramp_percent};
use autumn_harvest::types::ExecutionId;
use proptest::prelude::*;
use uuid::Uuid;

use super::prop_config::config;

fn exec_id() -> impl Strategy<Value = ExecutionId> {
    proptest::array::uniform16(any::<u8>())
        .prop_map(|b| ExecutionId::from_uuid(Uuid::from_bytes(b)))
}

proptest! {
    #![proptest_config(config())]

    /// Deterministic per id (checked across many repeated calls) and always in 0..100.
    #[test]
    fn ramp_bucket_is_deterministic_and_bounded(id in exec_id()) {
        let first = ramp_bucket(id);
        prop_assert!(first < 100, "bucket {first} out of range");
        for _ in 0..25 {
            prop_assert_eq!(ramp_bucket(id), first, "ramp_bucket is not deterministic");
        }
    }

    /// `validate_ramp_percent` is `Ok` iff the percent is in `0..=100`.
    ///
    /// The strategy explicitly seeds the inclusive-bound cases (-1, 0, 100, 101)
    /// alongside `any::<i32>()`, since proptest rarely samples those exact
    /// boundaries on its own — this reliably catches an off-by-one on the
    /// inclusive bound (`0..100` vs `0..=100`).
    #[test]
    fn validate_ramp_percent_accepts_exactly_0_to_100(
        percent in prop_oneof![Just(-1i32), Just(0i32), Just(100i32), Just(101i32), any::<i32>()]
    ) {
        let expected_ok = (0..=100).contains(&percent);
        prop_assert_eq!(validate_ramp_percent(percent).is_ok(), expected_ok);
    }
}
