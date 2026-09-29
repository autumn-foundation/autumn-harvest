## CI — repair chaos.yml and guard workflow YAML (Semaphore)

`chaos.yml` was invalid YAML: two `run:` scalars held `chaos::` followed by a space, which YAML reads as a mapping value. GitHub ran the file as a zero-job failed run on every push and never fired the nightly cron. The two commands now use folded scalars with the test filter quoted.

`docs/audits/workflow-yaml-parse.py` parses every workflow file in the `lint` job, so an unparsable workflow fails the build instead of failing silently.
