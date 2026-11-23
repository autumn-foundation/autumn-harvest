// Apparatus for assay #19. Never merges. Arm A: strip whitespace outside JSON strings.
use std::io::{Read, Write};
fn strip(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len());
    let (mut in_str, mut esc) = (false, false);
    for &b in src {
        if in_str {
            out.push(b);
            if esc { esc = false; } else if b == b'\\' { esc = true; } else if b == b'"' { in_str = false; }
        } else if b == b'"' {
            in_str = true;
            out.push(b);
        } else if !matches!(b, b' ' | b'\n' | b'\r' | b'\t') {
            out.push(b);
        }
    }
    out.push(b'\n');
    out
}
fn main() {
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf).unwrap();
    std::io::stdout().write_all(&strip(&buf)).unwrap();
}
