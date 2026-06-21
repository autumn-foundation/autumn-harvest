#[cfg(test)]
mod tests {
    use autumn_harvest::eligibility::parse_requirements;

    #[test]
    fn trigger_havoc_parse_exact() {
        let token = "𐍈=b";
        let _ = parse_requirements(token);
    }
}
