//! `cargo harvest-verify` — CLI front door (also runnable as `cargo-harvest-verify harvest-verify ...`).

fn main() {
    std::process::exit(autumn_harvest_verify::cli_main(std::env::args().collect()));
}
