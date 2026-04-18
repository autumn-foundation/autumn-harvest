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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarvestRuntimeConfig {
    pub mode: HarvestMode,
    pub worker_enabled: bool,
    pub scheduler_enabled: bool,
    pub database: HarvestDatabaseConfig,
    pub outbox: HarvestOutboxConfig,
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
    if let Ok(profile) = env.var("AUTUMN_PROFILE") {
        if !profile.is_empty() {
            return Some(profile);
        }
    }

    let args: Vec<String> = std::env::args().collect();
    for (i, arg) in args.iter().enumerate() {
        if arg == "--profile" {
            if let Some(profile) = args.get(i + 1) {
                return Some(profile.clone());
            }
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
