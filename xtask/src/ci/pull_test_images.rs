//! `cargo xtask ci pull-test-images`: pre-pull the Docker test images.
//!
//! It pulls the named images, or the images that the `linux` manifest rows of
//! this shard need. This is the port of `.github/ci/pull-test-images.sh`.
//!
//! `testcontainers` retries nothing on a pull, so a registry blip fails a
//! live-DB test. This command retries each pull. It reads
//! `SEMAPHORE_SHARD_COUNT` and `SEMAPHORE_SHARD_INDEX` through
//! `cargo xtask ci run-suites`, as the suite step does. `ci.yml`
//! (`test-db-linux`, `sqlite-plugin-split`), `coverage-nightly.yml` (`db`)
//! and `dst-nightly.yml` (`differential`, `world`) call it.
//!
//! It pulls each tag that the suites request, not just the default of
//! `testcontainers-modules`. The default alone left each suite that overrides
//! it as exposed as before.
//!
//! - `postgres:11-alpine` is the default that no suite overrides. The own
//!   `TestDb` of `autumn-web` uses it, for example in the webhook suites.
//! - `postgres:16` is the tag that about 220 DB suites of this repository
//!   request with `.with_tag("16")`.
//! - `postgres:16-alpine` is the override of one outlier suite.
//! - `redis:5.0` is the Redis default, and no suite overrides it. The issue
//!   #1312 dispatch suites need it. The jobs force them with
//!   `HARVEST_TEST_REQUIRE_REDIS`.
//! - `redis:7.4-alpine` is the TLS-only server of the issue #1834
//!   `tls_redis` suite. Redis 5 has no TLS.
//! - `softwaremill/elasticmq:1.5.2` is pulled only on the shard that runs the
//!   `connector_sqs_broker` suite. PR #1773 failed that suite with "bytes
//!   remaining on stream", because the image was not pulled first.
//! - The `MinIO` and `fake-gcs-server` emulators are pulled only on the shards
//!   that run their suites (issue #1983).
//!
//! With arguments, the command pulls only those images and skips the shard
//! plan. `dst-nightly.yml` passes `postgres:16` (issue #2280).
//!
//! With `PULL_EVERY_SUITE_IMAGE=1`, the command pulls each emulator on every
//! shard and skips the shard plan. `test-db-linux` sets it. That job runs
//! from the nextest archive and partitions by test hash. So the tests of one
//! suite reach many shards, and the row plan above cannot name them.

use std::process::ExitCode;
use std::time::Duration;

use smop::prelude::*;

use super::run_suites;

/// The images that every shard pulls.
const BASE_IMAGES: &[&str] = &[
    "postgres:11-alpine",
    "postgres:16",
    "postgres:16-alpine",
    "redis:5.0",
    "redis:7.4-alpine",
];

/// The plan text under `PULL_EVERY_SUITE_IMAGE=1`.
///
/// Each `--test` pattern of [`EMULATORS`] then matches.
const EVERY_SUITE_PLAN: &str =
    "--test connector_sqs_broker --test object_store_s3_minio --test object_store_gcs_emulator";

/// Each emulator image, after the `--test` pattern of the suite that needs it.
///
/// Issue #1983 added the object-store emulators. The official `MinIO` images
/// are no longer published, so the suite uses the `pgsty/minio` community
/// build.
const EMULATORS: &[(&str, &str)] = &[
    (
        "--test connector_sqs_broker",
        "softwaremill/elasticmq:1.5.2",
    ),
    (
        "--test object_store_s3_minio",
        "pgsty/minio:RELEASE.2026-08-04T00-00-00Z",
    ),
    (
        "--test object_store_gcs_emulator",
        "fsouza/fake-gcs-server:1.56.1",
    ),
];

/// The mirrors of an official image, in the order that they are tried.
///
/// Docker Hub rate-limits anonymous pulls, and the ECR Public mirror has a
/// data limit. An official image (no slash in its name) comes from the
/// Google mirror first, then from ECR Public. The tag then gives it the name
/// that the suites request.
const MIRRORS: &[&str] = &["mirror.gcr.io/library", "public.ecr.aws/docker/library"];

/// The number of attempts for each image.
const ATTEMPTS: u64 = 5;

/// Arguments of `cargo xtask ci pull-test-images`.
#[derive(Debug, Args)]
pub struct Args {
    /// The images to pull. With none, the shard plan picks them.
    #[arg(allow_hyphen_values = true)]
    pub images: Vec<String>,
}

/// Returns the images to pull for `plan`, the dry-run text of the shard.
///
/// A substring match finds each suite, as `grep -q -- '--test <suite>'` did.
pub fn images_for_plan(plan: &str) -> Vec<String> {
    let mut images: Vec<String> = BASE_IMAGES.iter().map(|s| (*s).to_string()).collect();
    for (pattern, image) in EMULATORS {
        if plan.contains(pattern) {
            images.push((*image).to_string());
        }
    }
    images
}

/// The docker calls and the waits of a pull, so a test can replace them.
pub trait Docker {
    /// Runs `docker` with `args` and returns its exit code.
    ///
    /// A program that cannot start counts as code 127, as in a shell.
    fn docker(&mut self, args: &[&str]) -> i32;
    /// Waits before the next attempt.
    fn sleep(&mut self, seconds: u64);
}

/// The real `docker` on `PATH`.
struct RealDocker;

impl Docker for RealDocker {
    fn docker(&mut self, args: &[&str]) -> i32 {
        match sh::cmd("docker").args(args).status() {
            Ok(status) => status.code().unwrap_or(1),
            Err(error) => {
                eprintln!("pull-test-images: {error:#}");
                127
            }
        }
    }

    fn sleep(&mut self, seconds: u64) {
        std::thread::sleep(Duration::from_secs(seconds));
    }
}

/// Pulls each image, with retries, and returns the exit code.
///
/// A failed `docker tag` stops the run with its own code, as `set -e` did.
pub fn pull_all(docker: &mut dyn Docker, images: &[String]) -> ExitCode {
    'image: for image in images {
        for attempt in 1..=ATTEMPTS {
            if !image.contains('/') {
                for mirror in MIRRORS {
                    let source = format!("{mirror}/{image}");
                    if docker.docker(&["pull", &source]) == 0 {
                        let code = docker.docker(&["tag", &source, image]);
                        if code != 0 {
                            return ExitCode::from(u8::try_from(code).unwrap_or(1));
                        }
                        continue 'image;
                    }
                }
            }
            if docker.docker(&["pull", image]) == 0 {
                continue 'image;
            }
            println!("pull of {image} failed (attempt {attempt}/{ATTEMPTS})");
            docker.sleep(attempt * 5);
        }
        println!("::error::failed to pull {image} after {ATTEMPTS} attempts");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Returns the images to pull, from the arguments or from the shard plan.
///
/// An empty argument is refused with `Err(())`.
fn images_to_pull(args: &[String]) -> Result<std::result::Result<Vec<String>, ()>> {
    if !args.is_empty() {
        if args.iter().any(String::is_empty) {
            return Ok(Err(()));
        }
        // The script split the joined arguments on the default IFS.
        let joined = args.join(" ");
        return Ok(Ok(joined
            .split(run_suites::is_ifs)
            .filter(|image| !image.is_empty())
            .map(str::to_string)
            .collect()));
    }
    // Pull ElasticMQ only on the shard that runs `connector_sqs_broker`. The
    // dry run lists the cargo calls of this shard without running them. So no
    // shard number is hard-coded here.
    let plan = if std::env::var("PULL_EVERY_SUITE_IMAGE").is_ok_and(|v| v == "1") {
        EVERY_SUITE_PLAN.to_string()
    } else {
        run_suites::run_dry_run_text("linux")?
    };
    Ok(Ok(images_for_plan(&plan)))
}

/// Runs `cargo xtask ci pull-test-images`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    if let Ok(images) = images_to_pull(&args.images)? {
        return Ok(pull_all(&mut RealDocker, &images));
    }
    println!("::error::an image argument is empty");
    Ok(ExitCode::FAILURE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A docker that fails each pull of a source in `failing`.
    struct FakeDocker {
        failing: Vec<String>,
        tag_code: i32,
        calls: Vec<String>,
        slept: Vec<u64>,
    }

    impl FakeDocker {
        fn new(failing: &[&str]) -> Self {
            Self {
                failing: failing.iter().map(|s| (*s).to_string()).collect(),
                tag_code: 0,
                calls: Vec::new(),
                slept: Vec::new(),
            }
        }
    }

    impl Docker for FakeDocker {
        fn docker(&mut self, args: &[&str]) -> i32 {
            self.calls.push(args.join(" "));
            if args[0] == "tag" {
                return self.tag_code;
            }
            i32::from(self.failing.iter().any(|f| f == args[1]))
        }

        fn sleep(&mut self, seconds: u64) {
            self.slept.push(seconds);
        }
    }

    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn an_official_image_comes_from_the_first_mirror_and_is_tagged() {
        let mut docker = FakeDocker::new(&[]);
        let code = pull_all(&mut docker, &names(&["postgres:16"]));
        assert_eq!(code, ExitCode::SUCCESS);
        assert_eq!(
            docker.calls,
            vec![
                "pull mirror.gcr.io/library/postgres:16",
                "tag mirror.gcr.io/library/postgres:16 postgres:16",
            ]
        );
    }

    #[test]
    fn a_failed_mirror_falls_back_to_the_next_one_then_to_the_hub() {
        let mut docker = FakeDocker::new(&[
            "mirror.gcr.io/library/redis:5.0",
            "public.ecr.aws/docker/library/redis:5.0",
        ]);
        let code = pull_all(&mut docker, &names(&["redis:5.0"]));
        assert_eq!(code, ExitCode::SUCCESS);
        assert_eq!(
            docker.calls.last().map(String::as_str),
            Some("pull redis:5.0")
        );
        assert_eq!(docker.slept, Vec::<u64>::new());
    }

    #[test]
    fn an_image_with_a_slash_skips_the_mirrors() {
        let mut docker = FakeDocker::new(&[]);
        pull_all(&mut docker, &names(&["softwaremill/elasticmq:1.5.2"]));
        assert_eq!(docker.calls, vec!["pull softwaremill/elasticmq:1.5.2"]);
    }

    #[test]
    fn five_failed_attempts_fail_the_run_with_growing_waits() {
        let mut docker = FakeDocker::new(&["a/b:1"]);
        let code = pull_all(&mut docker, &names(&["a/b:1", "c/d:2"]));
        assert_eq!(code, ExitCode::FAILURE);
        assert_eq!(docker.slept, vec![5, 10, 15, 20, 25]);
        assert!(
            docker.calls.iter().all(|c| c == "pull a/b:1"),
            "the run must stop at the first image that fails"
        );
    }

    #[test]
    fn a_failed_tag_stops_the_run_with_its_code() {
        let mut docker = FakeDocker::new(&[]);
        docker.tag_code = 3;
        let code = pull_all(&mut docker, &names(&["postgres:16", "redis:5.0"]));
        assert_eq!(code, ExitCode::from(3));
        assert_eq!(docker.calls.len(), 2);
    }

    #[test]
    fn the_plan_adds_only_the_emulators_its_suites_need() {
        let base = images_for_plan("+ cargo test -p x --test integration");
        assert_eq!(base, names(BASE_IMAGES));
        let sqs = images_for_plan("+ cargo test -p x --test connector_sqs_broker -- --x");
        assert_eq!(
            sqs.last().map(String::as_str),
            Some("softwaremill/elasticmq:1.5.2")
        );
        let every = images_for_plan(EVERY_SUITE_PLAN);
        assert_eq!(every.len(), BASE_IMAGES.len() + EMULATORS.len());
    }

    #[test]
    fn arguments_replace_the_plan_and_an_empty_one_is_refused() {
        let got = images_to_pull(&names(&["postgres:16", "a b"])).ok();
        assert_eq!(got, Some(Ok(names(&["postgres:16", "a", "b"]))));
        assert_eq!(images_to_pull(&names(&["x", ""])).ok(), Some(Err(())));
        // Only a space, a tab or a newline splits a word, as the default IFS
        // did. A no-break space stays inside the name.
        let got = images_to_pull(&names(&["a\tb\nc", "d\u{a0}e"])).ok();
        assert_eq!(got, Some(Ok(names(&["a", "b", "c", "d\u{a0}e"]))));
    }
}
