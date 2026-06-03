fn next_char(line: &str, pos: usize) -> Option<(char, usize)> {
    let ch = line[pos..].chars().next()?;
    Some((ch, pos + ch.len_utf8()))
}
fn main() {
    println!("{:?}", next_char("a💀b", 2));
}
