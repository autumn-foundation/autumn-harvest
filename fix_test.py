import re

with open("autumn-harvest/tests/scheduled_time_tests.rs", "r") as f:
    content = f.read()

# Replace testing imports since it's gated behind the `testing` feature.
# Or wait! The test file might need the `testing` feature enabled to compile!
# We can just add `#![cfg(feature = "testing")]` at the top of the file!

if '#![cfg(feature = "testing")]' not in content:
    content = '#![cfg(feature = "testing")]\n' + content

with open("autumn-harvest/tests/scheduled_time_tests.rs", "w") as f:
    f.write(content)
