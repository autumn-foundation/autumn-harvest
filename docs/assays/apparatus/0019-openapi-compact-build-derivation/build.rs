// Apparatus for assay #19. Never merges. Derives the compact document at build time.
use std::time::Instant;
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
    let t = Instant::now();
    println!("cargo:rerun-if-changed=openapi.pretty.json");
    println!("cargo:rerun-if-changed=build.rs");
    let src = std::fs::read("openapi.pretty.json").expect("openapi.pretty.json");
    let out = std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("openapi.json");
    std::fs::write(&out, strip(&src)).unwrap();
    let ns = t.elapsed().as_nanos();
    if let Ok(log) = std::env::var("ASSAY19_TIMING_LOG") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(log).unwrap();
        writeln!(f, "{ns}").unwrap();
    }
}
