//! Configuration management for the Harvest plugin (embedded, split, or external modes).

use std::path::{Path, PathBuf};

use autumn_web::config::{ConfigError, DatabaseConfig, Env, OsEnv};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HarvestMode {
    #[default]
    Embedded,
    Split,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HarvestDatabaseConfig {
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarvestOutboxConfig {
    pub enabled: bool,
    pub batch_size: i64,
    pub poll_interval_ms: u64,
    pub claim_ttl_ms: u64,
    pub base_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
}

/// Tunables for the batch-operations executor (issue #102).
///
/// `concurrency` caps the number of in-flight per-target operations so a
/// 10k-target batch can't monopolise the connection pool. `tick_interval_ms`
/// is how often the background loop wakes up to scan for new open jobs;
/// the same loop drains in-progress jobs synchronously within a tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarvestBatchConfig {
    pub concurrency: u32,
    pub tick_interval_ms: u64,
}

/// Readiness and health endpoint behavior.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HarvestReadinessConfig {
    /// When true, `/health` returns 503 unless writable/candidate shards are ready.
    pub require_shard_readiness: bool,
}

/// Redis dispatch channel settings (issue #1312).
///
/// The channel carries references to claimable `harvest_task_queue` rows.
/// Postgres stays the source of truth. `url` is the switch: `None` leaves
/// every worker on the Postgres claim path, which is the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarvestRedisConfig {
    /// Redis connection URL. `None` disables Redis dispatch.
    pub url: Option<String>,
    /// Prefix for every key the channel owns.
    pub key_prefix: String,
    /// Redis Streams consumer group the workers join.
    pub consumer_group: String,
    /// Time a delivered reference may stay unacked before recovery.
    pub visibility_timeout_ms: u64,
    /// Wait for one blocking read when the channel is idle.
    pub poll_interval_ms: u64,
    /// Interval for the reconcile sweep over due `PENDING` rows.
    pub reconcile_interval_ms: u64,
}

/// What to do when workflow-type reachability finds an orphaned type at
/// startup (issue #700 AC4).
///
/// An *orphaned* type has ≥1 non-terminal execution whose `#[workflow]` handler
/// is no longer registered in this build — those runs would wedge in permanent
/// replay failure. This action defaults to `Warn` (non-breaking): mixed fleets
/// mid-rollout must not crash-loop just because an old handler was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OrphanStartupAction {
    /// Skip the check entirely.
    Off,
    /// Log a warning naming the orphaned types, then continue (default).
    #[default]
    Warn,
    /// Refuse startup (return an error) when orphaned types are present and the
    /// cross-shard report is complete. A partial/unavailable report degrades to
    /// `Warn` so a transient shard outage never crash-loops boot.
    Fail,
}

/// Boot-time startup gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HarvestStartupConfig {
    /// Action to take when orphaned workflow types are detected at startup.
    pub orphaned_workflows: OrphanStartupAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarvestRuntimeConfig {
    pub mode: HarvestMode,
    pub worker_enabled: bool,
    pub scheduler_enabled: bool,
    pub database: HarvestDatabaseConfig,
    pub outbox: HarvestOutboxConfig,
    pub batch: HarvestBatchConfig,
    pub readiness: HarvestReadinessConfig,
    pub startup: HarvestStartupConfig,
    pub redis: HarvestRedisConfig,
}

impl HarvestRuntimeConfig {
    /// Load Harvest runtime configuration from Autumn config files and process environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when config files cannot be read or parsed, environment overrides
    /// are invalid, or the resulting topology configuration is not valid.
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_with_env(&OsEnv)
    }

    /// Load Harvest runtime configuration using an explicit environment provider.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when config files cannot be read or parsed, environment overrides
    /// are invalid, or the resulting topology configuration is not valid.
    pub fn load_with_env(env: &dyn Env) -> Result<Self, ConfigError> {
        let profile = resolve_profile(env);
        let mut config = Self::default();

        if let Some(root) = load_partial_root(&find_config_file_named("autumn.toml", env))? {
            config.apply_partial(root.harvest);
        }

        if let Some(profile) = profile {
            let path = find_config_file_named(&format!("autumn-{profile}.toml"), env);
            if let Some(root) = load_partial_root(&path)? {
                config.apply_partial(root.harvest);
            }
        }

        config.apply_env_overrides(env)?;
        config.validate()?;
        Ok(config)
    }

    fn apply_partial(&mut self, partial: PartialHarvestRuntimeConfig) {
        if let Some(mode) = partial.mode {
            self.mode = mode;
        }
        if let Some(worker_enabled) = partial.worker_enabled {
            self.worker_enabled = worker_enabled;
        }
        if let Some(scheduler_enabled) = partial.scheduler_enabled {
            self.scheduler_enabled = scheduler_enabled;
        }
        if let Some(url) = partial.database.url {
            self.database.url = Some(url);
        }
        if let Some(enabled) = partial.outbox.enabled {
            self.outbox.enabled = enabled;
        }
        if let Some(batch_size) = partial.outbox.batch_size {
            self.outbox.batch_size = batch_size;
        }
        if let Some(poll_interval_ms) = partial.outbox.poll_interval_ms {
            self.outbox.poll_interval_ms = poll_interval_ms;
        }
        if let Some(claim_ttl_ms) = partial.outbox.claim_ttl_ms {
            self.outbox.claim_ttl_ms = claim_ttl_ms;
        }
        if let Some(base_retry_delay_ms) = partial.outbox.base_retry_delay_ms {
            self.outbox.base_retry_delay_ms = base_retry_delay_ms;
        }
        if let Some(max_retry_delay_ms) = partial.outbox.max_retry_delay_ms {
            self.outbox.max_retry_delay_ms = max_retry_delay_ms;
        }
        if let Some(concurrency) = partial.batch.concurrency {
            self.batch.concurrency = concurrency;
        }
        if let Some(concurrency) = partial.batch.batch_concurrency {
            self.batch.concurrency = concurrency;
        }
        if let Some(tick_interval_ms) = partial.batch.tick_interval_ms {
            self.batch.tick_interval_ms = tick_interval_ms;
        }
        if let Some(require_shard_readiness) = partial.readiness.require_shard_readiness {
            self.readiness.require_shard_readiness = require_shard_readiness;
        }
        if let Some(orphaned_workflows) = partial.startup.orphaned_workflows {
            self.startup.orphaned_workflows = orphaned_workflows;
        }
        if let Some(url) = partial.redis.url {
            self.redis.url = Some(url);
        }
        if let Some(key_prefix) = partial.redis.key_prefix {
            self.redis.key_prefix = key_prefix;
        }
        if let Some(consumer_group) = partial.redis.consumer_group {
            self.redis.consumer_group = consumer_group;
        }
        if let Some(visibility_timeout_ms) = partial.redis.visibility_timeout_ms {
            self.redis.visibility_timeout_ms = visibility_timeout_ms;
        }
        if let Some(poll_interval_ms) = partial.redis.poll_interval_ms {
            self.redis.poll_interval_ms = poll_interval_ms;
        }
        if let Some(reconcile_interval_ms) = partial.redis.reconcile_interval_ms {
            self.redis.reconcile_interval_ms = reconcile_interval_ms;
        }
    }

    fn apply_env_overrides(&mut self, env: &dyn Env) -> Result<(), ConfigError> {
        if let Ok(mode) = env.var("AUTUMN_HARVEST__MODE") {
            self.mode = parse_mode(&mode)?;
        }

        if let Ok(worker_enabled) = env.var("AUTUMN_HARVEST__WORKER_ENABLED") {
            self.worker_enabled = parse_bool("AUTUMN_HARVEST__WORKER_ENABLED", &worker_enabled)?;
        }

        if let Ok(scheduler_enabled) = env.var("AUTUMN_HARVEST__SCHEDULER_ENABLED") {
            self.scheduler_enabled =
                parse_bool("AUTUMN_HARVEST__SCHEDULER_ENABLED", &scheduler_enabled)?;
        }

        if let Ok(url) = env.var("AUTUMN_HARVEST_DATABASE__URL") {
            self.database.url = (!url.is_empty()).then_some(url);
        }

        if let Ok(enabled) = env.var("AUTUMN_HARVEST_OUTBOX__ENABLED") {
            self.outbox.enabled = parse_bool("AUTUMN_HARVEST_OUTBOX__ENABLED", &enabled)?;
        }
        if let Ok(batch_size) = env.var("AUTUMN_HARVEST_OUTBOX__BATCH_SIZE") {
            self.outbox.batch_size = parse_i64("AUTUMN_HARVEST_OUTBOX__BATCH_SIZE", &batch_size)?;
        }
        if let Ok(poll_interval_ms) = env.var("AUTUMN_HARVEST_OUTBOX__POLL_INTERVAL_MS") {
            self.outbox.poll_interval_ms =
                parse_u64("AUTUMN_HARVEST_OUTBOX__POLL_INTERVAL_MS", &poll_interval_ms)?;
        }
        if let Ok(claim_ttl_ms) = env.var("AUTUMN_HARVEST_OUTBOX__CLAIM_TTL_MS") {
            self.outbox.claim_ttl_ms =
                parse_u64("AUTUMN_HARVEST_OUTBOX__CLAIM_TTL_MS", &claim_ttl_ms)?;
        }
        if let Ok(base_retry_delay_ms) = env.var("AUTUMN_HARVEST_OUTBOX__BASE_RETRY_DELAY_MS") {
            self.outbox.base_retry_delay_ms = parse_u64(
                "AUTUMN_HARVEST_OUTBOX__BASE_RETRY_DELAY_MS",
                &base_retry_delay_ms,
            )?;
        }
        if let Ok(max_retry_delay_ms) = env.var("AUTUMN_HARVEST_OUTBOX__MAX_RETRY_DELAY_MS") {
            self.outbox.max_retry_delay_ms = parse_u64(
                "AUTUMN_HARVEST_OUTBOX__MAX_RETRY_DELAY_MS",
                &max_retry_delay_ms,
            )?;
        }

        // Issue #102 calls the knob `batch_concurrency`; we accept either
        // spelling so operators can use whichever matches their habits and
        // the AC name resolves verbatim.
        if let Ok(concurrency) = env.var("AUTUMN_HARVEST_BATCH__CONCURRENCY") {
            self.batch.concurrency = parse_u32("AUTUMN_HARVEST_BATCH__CONCURRENCY", &concurrency)?;
        }
        if let Ok(concurrency) = env.var("AUTUMN_HARVEST_BATCH__BATCH_CONCURRENCY") {
            self.batch.concurrency =
                parse_u32("AUTUMN_HARVEST_BATCH__BATCH_CONCURRENCY", &concurrency)?;
        }
        if let Ok(tick_interval_ms) = env.var("AUTUMN_HARVEST_BATCH__TICK_INTERVAL_MS") {
            self.batch.tick_interval_ms =
                parse_u64("AUTUMN_HARVEST_BATCH__TICK_INTERVAL_MS", &tick_interval_ms)?;
        }
        if let Ok(require_shard_readiness) =
            env.var("AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS")
        {
            self.readiness.require_shard_readiness = parse_bool(
                "AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS",
                &require_shard_readiness,
            )?;
        }
        if let Ok(orphaned_workflows) = env.var("AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS") {
            self.startup.orphaned_workflows = parse_orphan_startup_action(
                "AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS",
                &orphaned_workflows,
            )?;
        }

        // Issue #1312. An empty `AUTUMN_HARVEST_REDIS__URL` means "off", the
        // same convention `AUTUMN_HARVEST_DATABASE__URL` uses above.
        if let Ok(url) = env.var("AUTUMN_HARVEST_REDIS__URL") {
            self.redis.url = (!url.is_empty()).then_some(url);
        }
        if let Ok(key_prefix) = env.var("AUTUMN_HARVEST_REDIS__KEY_PREFIX") {
            self.redis.key_prefix = key_prefix;
        }
        if let Ok(consumer_group) = env.var("AUTUMN_HARVEST_REDIS__CONSUMER_GROUP") {
            self.redis.consumer_group = consumer_group;
        }
        if let Ok(visibility_timeout_ms) = env.var("AUTUMN_HARVEST_REDIS__VISIBILITY_TIMEOUT_MS") {
            self.redis.visibility_timeout_ms = parse_u64(
                "AUTUMN_HARVEST_REDIS__VISIBILITY_TIMEOUT_MS",
                &visibility_timeout_ms,
            )?;
        }
        if let Ok(poll_interval_ms) = env.var("AUTUMN_HARVEST_REDIS__POLL_INTERVAL_MS") {
            self.redis.poll_interval_ms =
                parse_u64("AUTUMN_HARVEST_REDIS__POLL_INTERVAL_MS", &poll_interval_ms)?;
        }
        if let Ok(reconcile_interval_ms) = env.var("AUTUMN_HARVEST_REDIS__RECONCILE_INTERVAL_MS") {
            self.redis.reconcile_interval_ms = parse_u64(
                "AUTUMN_HARVEST_REDIS__RECONCILE_INTERVAL_MS",
                &reconcile_interval_ms,
            )?;
        }

        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let database_config = DatabaseConfig {
            url: self.database.url.clone(),
            ..DatabaseConfig::default()
        };
        database_config.validate()?;

        if matches!(self.mode, HarvestMode::Split | HarvestMode::External)
            && self.database.url.is_none()
        {
            return Err(ConfigError::Validation(format!(
                "harvest.database.url is required when harvest.mode is {:?}",
                self.mode
            )));
        }

        if self.outbox.batch_size < 1 {
            return Err(ConfigError::Validation(
                "harvest.outbox.batch_size must be at least 1".to_owned(),
            ));
        }
        if self.outbox.poll_interval_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.outbox.poll_interval_ms must be at least 1".to_owned(),
            ));
        }
        if self.outbox.claim_ttl_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.outbox.claim_ttl_ms must be at least 1".to_owned(),
            ));
        }
        if self.outbox.base_retry_delay_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.outbox.base_retry_delay_ms must be at least 1".to_owned(),
            ));
        }
        if self.outbox.max_retry_delay_ms < self.outbox.base_retry_delay_ms {
            return Err(ConfigError::Validation(
                "harvest.outbox.max_retry_delay_ms must be greater than or equal to harvest.outbox.base_retry_delay_ms".to_owned(),
            ));
        }

        if self.batch.concurrency < 1 {
            return Err(ConfigError::Validation(
                "harvest.batch.concurrency must be at least 1".to_owned(),
            ));
        }
        if self.batch.tick_interval_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.batch.tick_interval_ms must be at least 1".to_owned(),
            ));
        }

        self.validate_redis()?;

        Ok(())
    }

    /// Validate the `[harvest.redis]` section (issue #1312).
    ///
    /// A build without the `redis` cargo feature carries no channel
    /// implementation. A configured URL there is rejected, so an operator
    /// never runs a binary that silently ignores the setting.
    fn validate_redis(&self) -> Result<(), ConfigError> {
        if self.redis.visibility_timeout_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.redis.visibility_timeout_ms must be at least 1".to_owned(),
            ));
        }
        if self.redis.poll_interval_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.redis.poll_interval_ms must be at least 1".to_owned(),
            ));
        }
        if self.redis.reconcile_interval_ms < 1 {
            return Err(ConfigError::Validation(
                "harvest.redis.reconcile_interval_ms must be at least 1".to_owned(),
            ));
        }

        if self.redis.url.is_some() && !cfg!(feature = "redis") {
            return Err(ConfigError::Validation(
                "harvest.redis.url is set but this binary is built without the `redis` cargo \
                 feature of autumn-harvest-plugin; rebuild with `--features redis` or unset \
                 harvest.redis.url"
                    .to_owned(),
            ));
        }

        Ok(())
    }
}

impl Default for HarvestRuntimeConfig {
    fn default() -> Self {
        Self {
            mode: HarvestMode::Embedded,
            worker_enabled: true,
            scheduler_enabled: true,
            database: HarvestDatabaseConfig::default(),
            outbox: HarvestOutboxConfig::default(),
            batch: HarvestBatchConfig::default(),
            readiness: HarvestReadinessConfig::default(),
            startup: HarvestStartupConfig::default(),
            redis: HarvestRedisConfig::default(),
        }
    }
}

impl HarvestRedisConfig {
    /// The configured URL with any userinfo removed.
    ///
    /// A Redis URL can carry a user name and a password. Startup logs and
    /// error messages name the endpoint, so they use this form. Returns
    /// `None` when Redis dispatch is off.
    #[must_use]
    pub fn redacted_url(&self) -> Option<String> {
        self.url.as_deref().map(redact_userinfo)
    }
}

impl Default for HarvestRedisConfig {
    fn default() -> Self {
        Self {
            url: None,
            key_prefix: "harvest".to_owned(),
            consumer_group: "harvest_workers".to_owned(),
            visibility_timeout_ms: 60_000,
            poll_interval_ms: 20,
            reconcile_interval_ms: 1_000,
        }
    }
}

impl Default for HarvestOutboxConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 32,
            poll_interval_ms: 1_000,
            claim_ttl_ms: 30_000,
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 60_000,
        }
    }
}

impl Default for HarvestBatchConfig {
    fn default() -> Self {
        Self {
            // Matches the issue's default: cap fan-out at 32 in-flight ops.
            concurrency: 32,
            tick_interval_ms: 500,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct PartialRoot {
    #[serde(default)]
    harvest: PartialHarvestRuntimeConfig,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestRuntimeConfig {
    mode: Option<HarvestMode>,
    worker_enabled: Option<bool>,
    scheduler_enabled: Option<bool>,
    #[serde(default)]
    database: PartialHarvestDatabaseConfig,
    #[serde(default)]
    outbox: PartialHarvestOutboxConfig,
    #[serde(default)]
    batch: PartialHarvestBatchConfig,
    #[serde(default)]
    readiness: PartialHarvestReadinessConfig,
    #[serde(default)]
    startup: PartialHarvestStartupConfig,
    #[serde(default)]
    redis: PartialHarvestRedisConfig,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestDatabaseConfig {
    url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestOutboxConfig {
    enabled: Option<bool>,
    batch_size: Option<i64>,
    poll_interval_ms: Option<u64>,
    claim_ttl_ms: Option<u64>,
    base_retry_delay_ms: Option<u64>,
    max_retry_delay_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestBatchConfig {
    concurrency: Option<u32>,
    /// Issue #102 spells the field `batch_concurrency`. Either key sets the
    /// same field; if both are present, `batch_concurrency` wins because it
    /// is the issue's canonical name.
    batch_concurrency: Option<u32>,
    tick_interval_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestReadinessConfig {
    require_shard_readiness: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestStartupConfig {
    orphaned_workflows: Option<OrphanStartupAction>,
}

#[derive(Debug, Default, Deserialize)]
struct PartialHarvestRedisConfig {
    url: Option<String>,
    key_prefix: Option<String>,
    consumer_group: Option<String>,
    visibility_timeout_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    reconcile_interval_ms: Option<u64>,
}

fn find_config_file_named(filename: &str, env: &dyn Env) -> PathBuf {
    if let Ok(manifest_dir) = env.var("AUTUMN_MANIFEST_DIR") {
        let candidate = PathBuf::from(manifest_dir).join(filename);
        if candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from(filename)
}

fn load_partial_root(path: &Path) -> Result<Option<PartialRoot>, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(toml::from_str(&contents)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ConfigError::Io(error)),
    }
}

fn resolve_profile(env: &dyn Env) -> Option<String> {
    if let Ok(profile) = env.var("AUTUMN_PROFILE")
        && !profile.is_empty()
    {
        return Some(profile);
    }

    let args: Vec<String> = std::env::args().collect();
    for (i, arg) in args.iter().enumerate() {
        if arg == "--profile"
            && let Some(profile) = args.get(i + 1)
        {
            return Some(profile.clone());
        }
        if let Some(profile) = arg.strip_prefix("--profile=") {
            return Some(profile.to_owned());
        }
    }

    match env.var("AUTUMN_IS_DEBUG").ok().as_deref() {
        Some("1") => Some("dev".to_owned()),
        Some("0") => Some("prod".to_owned()),
        _ => None,
    }
}

fn parse_mode(value: &str) -> Result<HarvestMode, ConfigError> {
    match value {
        "embedded" => Ok(HarvestMode::Embedded),
        "split" => Ok(HarvestMode::Split),
        "external" => Ok(HarvestMode::External),
        _ => Err(ConfigError::Validation(format!(
            "invalid harvest mode {value:?}; expected one of: embedded, split, external"
        ))),
    }
}

/// Parse an [`OrphanStartupAction`] from an environment override.
///
/// Case-insensitive (`off`/`warn`/`fail` in any casing). The error message
/// names the `orphaned_workflows` field so an operator can find it regardless
/// of the (uppercase) env-var key. TOML values are lowercase-only via
/// `#[serde(rename_all = "lowercase")]`, matching the `HarvestMode` precedent.
fn parse_orphan_startup_action(key: &str, value: &str) -> Result<OrphanStartupAction, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "off" => Ok(OrphanStartupAction::Off),
        "warn" => Ok(OrphanStartupAction::Warn),
        "fail" => Ok(OrphanStartupAction::Fail),
        _ => Err(ConfigError::Validation(format!(
            "invalid orphaned_workflows value for {key}: {value:?}; expected one of: off, warn, fail"
        ))),
    }
}

/// Remove the `user:password@` part of a URL authority.
///
/// The scan is bounded to the authority: the first `/`, `?` or `#` after the
/// scheme ends it. An `@` later in the path or the query is left alone.
fn redact_userinfo(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority = &url[authority_start..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    let Some(at) = authority[..authority_end].rfind('@') else {
        return url.to_owned();
    };
    format!("{}{}", &url[..authority_start], &authority[at + 1..])
}

fn parse_bool(key: &str, value: &str) -> Result<bool, ConfigError> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(ConfigError::Validation(format!(
            "invalid boolean for {key}: {value:?}"
        ))),
    }
}

fn parse_i64(key: &str, value: &str) -> Result<i64, ConfigError> {
    value
        .parse::<i64>()
        .map_err(|_| ConfigError::Validation(format!("invalid integer for {key}: {value:?}")))
}

fn parse_u64(key: &str, value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .map_err(|_| ConfigError::Validation(format!("invalid integer for {key}: {value:?}")))
}

fn parse_u32(key: &str, value: &str) -> Result<u32, ConfigError> {
    value
        .parse::<u32>()
        .map_err(|_| ConfigError::Validation(format!("invalid integer for {key}: {value:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};

    use autumn_web::config::MockEnv;

    #[test]
    fn harvest_config_defaults_to_embedded_mode() {
        let env = MockEnv::new();
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.mode, HarvestMode::Embedded);
        assert!(config.worker_enabled);
        assert!(config.scheduler_enabled);
        assert_eq!(config.database.url, None);
    }

    #[test]
    fn harvest_config_split_mode_requires_database_url() {
        let dir = unique_temp_dir("harvest-config-split");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest]
mode = "split"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_PROFILE", "dev");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("split mode must fail without harvest database url");

        assert!(
            error.to_string().contains("harvest.database.url"),
            "expected missing harvest.database.url validation error, got {error}"
        );
    }

    #[test]
    fn harvest_config_external_mode_requires_database_url() {
        let dir = unique_temp_dir("harvest-config-external");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest]
mode = "external"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_PROFILE", "prod");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("external mode must fail without harvest database url");

        assert!(
            error.to_string().contains("harvest.database.url"),
            "expected missing harvest.database.url validation error, got {error}"
        );
    }

    #[test]
    fn harvest_config_allows_external_mode_with_runtime_toggles_disabled() {
        let dir = unique_temp_dir("harvest-config-toggles");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest]
mode = "external"
worker_enabled = false
scheduler_enabled = false

[harvest.database]
url = "postgres://harvest:harvest@localhost:5432/harvest"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_PROFILE", "prod");

        let config =
            HarvestRuntimeConfig::load_with_env(&env).expect("external config should load");

        assert_eq!(config.mode, HarvestMode::External);
        assert!(!config.worker_enabled);
        assert!(!config.scheduler_enabled);
        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://harvest:harvest@localhost:5432/harvest")
        );
    }

    #[test]
    fn harvest_config_env_overrides_toml() {
        let dir = unique_temp_dir("harvest-config-env");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest]
mode = "embedded"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_PROFILE", "dev")
            .with("AUTUMN_HARVEST__MODE", "external")
            .with("AUTUMN_HARVEST__WORKER_ENABLED", "false")
            .with("AUTUMN_HARVEST__SCHEDULER_ENABLED", "false")
            .with(
                "AUTUMN_HARVEST_DATABASE__URL",
                "postgres://harvest:harvest@localhost:5432/env_override",
            );

        let config =
            HarvestRuntimeConfig::load_with_env(&env).expect("env override config should load");

        assert_eq!(config.mode, HarvestMode::External);
        assert!(!config.worker_enabled);
        assert!(!config.scheduler_enabled);
        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://harvest:harvest@localhost:5432/env_override")
        );
    }

    #[test]
    fn harvest_config_outbox_defaults_are_sane() {
        let env = MockEnv::new();
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert!(config.outbox.enabled);
        assert_eq!(config.outbox.batch_size, 32);
        assert_eq!(config.outbox.poll_interval_ms, 1_000);
        assert_eq!(config.outbox.claim_ttl_ms, 30_000);
        assert_eq!(config.outbox.base_retry_delay_ms, 1_000);
        assert_eq!(config.outbox.max_retry_delay_ms, 60_000);
    }

    #[test]
    fn harvest_config_outbox_env_overrides_are_applied() {
        let env = MockEnv::new()
            .with("AUTUMN_HARVEST_OUTBOX__ENABLED", "false")
            .with("AUTUMN_HARVEST_OUTBOX__BATCH_SIZE", "64")
            .with("AUTUMN_HARVEST_OUTBOX__POLL_INTERVAL_MS", "250")
            .with("AUTUMN_HARVEST_OUTBOX__CLAIM_TTL_MS", "120000")
            .with("AUTUMN_HARVEST_OUTBOX__BASE_RETRY_DELAY_MS", "500")
            .with("AUTUMN_HARVEST_OUTBOX__MAX_RETRY_DELAY_MS", "900000");

        let config =
            HarvestRuntimeConfig::load_with_env(&env).expect("env overrides should parse cleanly");

        assert!(!config.outbox.enabled);
        assert_eq!(config.outbox.batch_size, 64);
        assert_eq!(config.outbox.poll_interval_ms, 250);
        assert_eq!(config.outbox.claim_ttl_ms, 120_000);
        assert_eq!(config.outbox.base_retry_delay_ms, 500);
        assert_eq!(config.outbox.max_retry_delay_ms, 900_000);
    }

    #[test]
    fn harvest_config_outbox_rejects_invalid_values() {
        let env = MockEnv::new()
            .with("AUTUMN_HARVEST_OUTBOX__BATCH_SIZE", "0")
            .with("AUTUMN_HARVEST_OUTBOX__POLL_INTERVAL_MS", "0");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("invalid outbox settings must fail validation");

        assert!(
            error.to_string().contains("outbox"),
            "expected outbox validation error, got {error}"
        );
    }

    #[test]
    fn harvest_config_readiness_defaults_do_not_gate_health() {
        let env = MockEnv::new();
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert!(!config.readiness.require_shard_readiness);
    }

    #[test]
    fn harvest_config_readiness_toml_can_gate_health() {
        let dir = unique_temp_dir("harvest-config-readiness-toml");
        write_file(
            &dir.join("autumn.toml"),
            r"
[harvest.readiness]
require_shard_readiness = true
",
        );
        let env = MockEnv::new().with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref());

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert!(config.readiness.require_shard_readiness);
    }

    #[test]
    fn harvest_config_readiness_env_overrides_toml() {
        let dir = unique_temp_dir("harvest-config-readiness-env");
        write_file(
            &dir.join("autumn.toml"),
            r"
[harvest.readiness]
require_shard_readiness = false
",
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_HARVEST_READINESS__REQUIRE_SHARD_READINESS", "true");

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert!(config.readiness.require_shard_readiness);
    }

    #[test]
    fn startup_orphaned_workflows_defaults_to_warn() {
        let env = MockEnv::new();
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");
        assert_eq!(config.startup.orphaned_workflows, OrphanStartupAction::Warn);
    }

    #[test]
    fn startup_orphaned_workflows_from_toml() {
        let dir = unique_temp_dir("harvest-config-startup-toml");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest.startup]
orphaned_workflows = "fail"
"#,
        );
        let env = MockEnv::new().with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref());
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");
        assert_eq!(config.startup.orphaned_workflows, OrphanStartupAction::Fail);
    }

    #[test]
    fn startup_orphaned_workflows_from_env() {
        let dir = unique_temp_dir("harvest-config-startup-env");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest.startup]
orphaned_workflows = "warn"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS", "off");
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");
        assert_eq!(config.startup.orphaned_workflows, OrphanStartupAction::Off);
    }

    #[test]
    fn startup_orphaned_workflows_rejects_unknown_value() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS", "explode");
        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("unknown orphaned_workflows value must fail validation");
        assert!(
            error.to_string().contains("orphaned_workflows"),
            "expected orphaned_workflows validation error, got {error}"
        );
    }

    #[test]
    fn startup_orphaned_workflows_rejects_unknown_value_from_toml() {
        // Sibling of the env-path test above: the TOML path must also reject an
        // unknown value (serde deserialization error), never silently default.
        let dir = unique_temp_dir("harvest-config-startup-toml-reject");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest.startup]
orphaned_workflows = "explode"
"#,
        );
        let env = MockEnv::new().with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref());
        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("unknown orphaned_workflows TOML value must fail to load");
        let message = error.to_string();
        assert!(
            message.contains("explode") || message.contains("orphaned_workflows"),
            "expected the TOML deserialization error to reference the bad value \
             or the field, got {error}"
        );
    }

    #[test]
    fn harvest_config_redis_defaults_leave_dispatch_off() {
        let env = MockEnv::new();
        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.redis.url, None);
        assert_eq!(config.redis.key_prefix, "harvest");
        assert_eq!(config.redis.consumer_group, "harvest_workers");
        assert_eq!(config.redis.visibility_timeout_ms, 60_000);
        assert_eq!(config.redis.poll_interval_ms, 20);
        assert_eq!(config.redis.reconcile_interval_ms, 1_000);
    }

    #[test]
    fn harvest_config_redis_section_parses_from_toml() {
        let dir = unique_temp_dir("harvest-config-redis-toml");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest.redis]
key_prefix = "acme"
consumer_group = "acme_workers"
visibility_timeout_ms = 30000
poll_interval_ms = 5
reconcile_interval_ms = 250
"#,
        );
        let env = MockEnv::new().with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref());

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.redis.url, None);
        assert_eq!(config.redis.key_prefix, "acme");
        assert_eq!(config.redis.consumer_group, "acme_workers");
        assert_eq!(config.redis.visibility_timeout_ms, 30_000);
        assert_eq!(config.redis.poll_interval_ms, 5);
        assert_eq!(config.redis.reconcile_interval_ms, 250);
    }

    #[test]
    fn harvest_config_redis_env_overrides_toml() {
        let dir = unique_temp_dir("harvest-config-redis-env");
        write_file(
            &dir.join("autumn.toml"),
            r#"
[harvest.redis]
key_prefix = "from_toml"
"#,
        );
        let env = MockEnv::new()
            .with("AUTUMN_MANIFEST_DIR", dir.to_string_lossy().as_ref())
            .with("AUTUMN_HARVEST_REDIS__KEY_PREFIX", "from_env")
            .with("AUTUMN_HARVEST_REDIS__CONSUMER_GROUP", "env_workers")
            .with("AUTUMN_HARVEST_REDIS__VISIBILITY_TIMEOUT_MS", "15000")
            .with("AUTUMN_HARVEST_REDIS__POLL_INTERVAL_MS", "40")
            .with("AUTUMN_HARVEST_REDIS__RECONCILE_INTERVAL_MS", "2000");

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.redis.key_prefix, "from_env");
        assert_eq!(config.redis.consumer_group, "env_workers");
        assert_eq!(config.redis.visibility_timeout_ms, 15_000);
        assert_eq!(config.redis.poll_interval_ms, 40);
        assert_eq!(config.redis.reconcile_interval_ms, 2_000);
    }

    #[test]
    fn harvest_config_redis_empty_url_env_leaves_dispatch_off() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__URL", "");

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.redis.url, None);
    }

    #[test]
    fn harvest_config_redis_rejects_a_zero_poll_interval() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__POLL_INTERVAL_MS", "0");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("a zero poll interval must fail validation");

        assert!(
            error.to_string().contains("harvest.redis.poll_interval_ms"),
            "expected a redis poll_interval_ms validation error, got {error}"
        );
    }

    #[test]
    fn harvest_config_redis_rejects_a_zero_reconcile_interval() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__RECONCILE_INTERVAL_MS", "0");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("a zero reconcile interval must fail validation");

        assert!(
            error
                .to_string()
                .contains("harvest.redis.reconcile_interval_ms"),
            "expected a redis reconcile_interval_ms validation error, got {error}"
        );
    }

    #[test]
    fn harvest_config_redis_rejects_a_zero_visibility_timeout() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__VISIBILITY_TIMEOUT_MS", "0");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("a zero visibility timeout must fail validation");

        assert!(
            error
                .to_string()
                .contains("harvest.redis.visibility_timeout_ms"),
            "expected a redis visibility_timeout_ms validation error, got {error}"
        );
    }

    /// A build without the `redis` feature carries no channel implementation.
    /// A configured URL must therefore fail at load, not start a runtime that
    /// silently ignores it.
    #[cfg(not(feature = "redis"))]
    #[test]
    fn harvest_config_redis_url_without_the_feature_is_rejected() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__URL", "redis://127.0.0.1:6379");

        let error = HarvestRuntimeConfig::load_with_env(&env)
            .expect_err("a redis url must fail validation without the redis feature");

        let message = error.to_string();
        assert!(
            message.contains("redis") && message.contains("feature"),
            "expected the error to name the `redis` cargo feature, got {error}"
        );
    }

    /// A build with the `redis` feature accepts a configured URL.
    #[cfg(feature = "redis")]
    #[test]
    fn harvest_config_redis_url_with_the_feature_is_accepted() {
        let env = MockEnv::new().with("AUTUMN_HARVEST_REDIS__URL", "redis://127.0.0.1:6379");

        let config = HarvestRuntimeConfig::load_with_env(&env).expect("harvest config should load");

        assert_eq!(config.redis.url.as_deref(), Some("redis://127.0.0.1:6379"));
    }

    #[test]
    fn redacted_url_is_none_when_dispatch_is_off() {
        assert_eq!(HarvestRedisConfig::default().redacted_url(), None);
    }

    #[test]
    fn redacted_url_keeps_the_host_and_drops_the_credentials() {
        let config = HarvestRedisConfig {
            url: Some("redis://operator:hunter2@cache.internal:6379/2".to_owned()),
            ..HarvestRedisConfig::default()
        };

        let redacted = config.redacted_url().expect("a url is set");

        assert_eq!(redacted, "redis://cache.internal:6379/2");
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("operator"));
    }

    #[test]
    fn redacted_url_leaves_a_credential_free_url_intact() {
        let config = HarvestRedisConfig {
            url: Some("rediss://cache.internal:6380".to_owned()),
            ..HarvestRedisConfig::default()
        };

        assert_eq!(
            config.redacted_url().as_deref(),
            Some("rediss://cache.internal:6380")
        );
    }

    #[test]
    fn redacted_url_ignores_an_at_sign_after_the_authority() {
        // A password in the path or query must not make the host disappear.
        let config = HarvestRedisConfig {
            url: Some("redis://cache.internal:6379/0?token=a@b".to_owned()),
            ..HarvestRedisConfig::default()
        };

        assert_eq!(
            config.redacted_url().as_deref(),
            Some("redis://cache.internal:6379/0?token=a@b")
        );
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "autumn-harvest-plugin-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    fn write_file(path: &Path, contents: &str) {
        fs::write(path, contents)
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
    }
}
