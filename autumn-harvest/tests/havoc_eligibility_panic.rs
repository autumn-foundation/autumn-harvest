#[test]
fn hvg_eligibility_panic_on_emoji() {
    let s = "a 💩 in [b]"; // poop emoji (multibyte)
    let _ = autumn_harvest::eligibility::parse_requirements(s);

    let s_exact = "a 💩 = b";
    let _ = autumn_harvest::eligibility::parse_requirements(s_exact);
}
