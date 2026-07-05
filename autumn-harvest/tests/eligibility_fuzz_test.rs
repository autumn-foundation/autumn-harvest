use proptest::prelude::*;
use autumn_harvest::eligibility::parse_requirements;

proptest! {
    #[test]
    fn does_not_crash(s in ".*") {
        let _ = parse_requirements(&s);
    }
}
