// Issue #940 AC2: an unknown or misspelled chaos injection point is a
// COMPILE ERROR, never a silent runtime no-op. A `ChaosPoint` value is always
// a catalogue const. Its `name` field is private, so no external code can
// fabricate one. So a name that is not in the catalogue fails to compile.
// The `chaos::points` module is compiled into every build, so this holds
// with or without the `chaos` feature.

fn main() {
    // There is no such catalogue const — this must not resolve.
    let _ = autumn_harvest::chaos::points::QUEUE_PARK_BEFORE_UPDAT;
}
