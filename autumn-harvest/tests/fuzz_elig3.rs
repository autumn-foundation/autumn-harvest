#[cfg(test)]
mod tests {
    use autumn_harvest::eligibility::parse_requirements;

    #[test]
    fn trigger_havoc_parse_requirements() {
        let token = "𐍈 in [a,b]";
        let _ = parse_requirements(token);
    }
}
