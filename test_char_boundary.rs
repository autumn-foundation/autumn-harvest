fn next_char(line: &str, pos: usize) -> Option<(char, usize)> {
    if pos >= line.len() {
        return None;
    }
    // ensure valid byte index
    if !line.is_char_boundary(pos) {
        return None; // or we can panic/log, but returning None skips invalid data safely
    }
    let ch = line[pos..].chars().next()?;
    Some((ch, pos + ch.len_utf8()))
}
fn main() {
    println!("{:?}", next_char("a💀b", 2));
}
