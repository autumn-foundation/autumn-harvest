use autumn_harvest::eligibility::parse_requirements;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]
    #[test]
    fn havoc_parse_requirements_cannot_panic(s in "\\PC*") {
        // Havoc: Just throw random Unicode at the parser to verify no slice/unwrap panics.
        let _ = parse_requirements(&s);
    }
}
