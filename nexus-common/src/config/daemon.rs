use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, path::PathBuf};
use tracing::error;

use crate::{file::CONFIG_FILE_NAME, types::DynError};

use super::{file::ConfigLoader, ApiConfig, StackConfig, WatcherConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub watcher: WatcherConfig,
    pub stack: StackConfig,
}

impl DaemonConfig {
    /// Returns the config file path in this directory
    fn get_config_file_path(expanded_path: PathBuf) -> PathBuf {
        expanded_path.join(CONFIG_FILE_NAME)
    }

    /// Writes the default [DaemonConfig] config file into the specified path
    fn write_default_config_file(config_file_path: PathBuf) -> std::io::Result<()> {
        // Make sure before write the file, the directory path exists
        if let Some(parent) = config_file_path.parent() {
            println!(
                "Validating existence of '{}' and creating it if missing before copying '{CONFIG_FILE_NAME}' file…",
                parent.display()
            );
            std::fs::create_dir_all(parent)?;
        }
        // Create the file
        std::fs::write(config_file_path, super::file::reader::DEFAULT_CONFIG_TOML)?;
        Ok(())
    }

    /// Given a directory path, ensures the directory exists, writes a default
    /// [DaemonConfig] file if absent, then parses and returns the loaded config
    pub async fn read_or_create_config_file(
        expanded_path: PathBuf,
    ) -> Result<DaemonConfig, DynError> {
        let config_file_path = Self::get_config_file_path(expanded_path);

        if !config_file_path.exists() {
            Self::write_default_config_file(config_file_path.clone())?;
        }

        println!("nexusd loading config file {}", config_file_path.display());
        Self::load(&config_file_path).await.inspect_err(|e| {
            error!("Failed to load config file: {e}");
        })
    }
}

#[async_trait]
impl ConfigLoader<DaemonConfig> for DaemonConfig {}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf, str::FromStr};

    use pubky_app_specs::PubkyId;

    use crate::{file::validate_and_expand_path, file::ConfigLoader, DaemonConfig, Level};

    #[tokio_shared_rt::test(shared)]
    async fn test_toml_parsing() {
        let c: DaemonConfig = DaemonConfig::read_or_create_config_file(
            tempfile::TempDir::new().unwrap().path().to_path_buf(),
        )
        .await
        .unwrap();

        assert_eq!(c.api.name, "nexusd.api");
        assert_eq!(c.api.public_addr, SocketAddr::from(([127, 0, 0, 1], 8080)));

        assert_eq!(c.watcher.name, "nexusd.watcher");
        assert!(!c.watcher.testnet);
        assert_eq!(
            c.watcher.homeserver,
            PubkyId::try_from("8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty").unwrap()
        );
        assert_eq!(c.watcher.events_limit, 50);
        assert_eq!(c.watcher.watcher_sleep, 5_000);
        assert_eq!(
            c.watcher.moderation_id,
            PubkyId::try_from("51y9w1skwcryb3iq4sia3x49qwpgstc5feo5tqon65gid7o99khy").unwrap()
        );
        assert_eq!(
            c.watcher.moderated_tags,
            vec![
                "hatespeech",
                "harassement",
                "terrorism",
                "violence",
                "illegal_activities",
                "il_adult_nu_sex_act",
            ]
        );

        assert_eq!(c.stack.log_level, Level::Info);
        assert_eq!(
            c.stack.files_path,
            validate_and_expand_path(PathBuf::from_str("~/.pubky-nexus/static/files").unwrap())
                .unwrap()
        );
        assert_eq!(c.stack.db.redis, "redis://127.0.0.1:6379");
        assert_eq!(c.stack.db.neo4j.uri, "bolt://localhost:7687");
    }

    /// Railway `/data/config.toml` (entrypoint-railway.sh) and the reserve-scrub
    /// `~/.pubky-nexus/migrations/config.toml` layout (docs/railway-deploy.md).
    /// Redis userinfo is the fake fixture `redis://user:pass@host:6379/0`, never a
    /// production URL.
    const FIXTURE_REDIS: &str = "redis://user:pass@host:6379/0";
    const FIXTURE_BOLT: &str = "bolt://neo4j.railway.internal:7687";

    const RAILWAY_DATA_CONFIG_TOML: &str = r#"
[api]
name = "nexusd.api"
public_ip = "0.0.0.0"
public_addr = "0.0.0.0:8080"
pubky_listen_socket = "0.0.0.0:8081"

[watcher]
name = "nexusd.watcher"
testnet = false
testnet_host = "localhost"
homeserver = "ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy"
events_limit = 1000
monitored_homeservers_limit = 50
watcher_sleep = 500
moderation_id = "51y9w1skwcryb3iq4sia3x49qwpgstc5feo5tqon65gid7o99khy"
moderated_tags = []

[stack]
log_level = "info"
files_path = "/data/static/files"

[stack.db]
redis = "redis://user:pass@host:6379/0"

[stack.db.neo4j]
uri = "bolt://neo4j.railway.internal:7687"
password = "fixture-neo4j-password"
"#;

    const RESERVE_SCRUB_MIGRATION_CONFIG_TOML: &str = r#"
name = "nexusd.migration"
backfill_ready = ["ListingAuctionTermsReindex1787256279", "ReviewBackfill1787905961"]
testnet = false
testnet_host = "localhost"

[stack]
log_level = "info"
files_path = "/data/static/files"

[stack.db]
redis = "redis://user:pass@host:6379/0"

[stack.db.neo4j]
uri = "bolt://neo4j.railway.internal:7687"
password = "fixture-neo4j-password"
"#;

    #[derive(Debug, serde::Deserialize)]
    struct ReserveScrubMigrationConfig {
        name: String,
        backfill_ready: Vec<String>,
        testnet: bool,
        testnet_host: String,
        stack: crate::StackConfig,
    }

    fn assert_fixture_connection_urls(db: &crate::db::DatabaseConfig) {
        assert_eq!(db.redis.as_str(), FIXTURE_REDIS);
        assert_eq!(db.neo4j.uri.as_str(), FIXTURE_BOLT);

        #[derive(serde::Serialize)]
        struct UrlFields<'a> {
            redis: &'a crate::db::ConnectionUrl,
            uri: &'a crate::db::ConnectionUrl,
        }

        let fields_toml = toml::to_string(&UrlFields {
            redis: &db.redis,
            uri: &db.neo4j.uri,
        })
        .expect("redis/uri fields serialize to TOML");
        let db_toml = toml::to_string(db).expect("DatabaseConfig serializes to TOML");
        let redis_json = serde_json::to_string(&db.redis).expect("ConnectionUrl JSON");
        let uri_json = serde_json::to_string(&db.neo4j.uri).expect("ConnectionUrl JSON");

        for encoded in [&fields_toml, &db_toml, &redis_json] {
            assert!(
                encoded.contains(FIXTURE_REDIS),
                "serde write-back lost redis userinfo: {encoded}"
            );
            assert!(
                !encoded.contains("[redacted]"),
                "serde write-back redacted redis: {encoded}"
            );
        }
        for encoded in [&fields_toml, &db_toml, &uri_json] {
            assert!(
                encoded.contains(FIXTURE_BOLT),
                "serde write-back lost bolt URI: {encoded}"
            );
        }

        let redis_display = db.redis.to_string();
        let redis_debug = format!("{:?}", db.redis);
        let uri_display = db.neo4j.uri.to_string();
        let uri_debug = format!("{:?}", db.neo4j.uri);
        let db_debug = format!("{db:?}");

        assert_eq!(redis_display, "redis://[redacted]@host:6379/0");
        assert!(redis_debug.contains("[redacted]"));
        assert!(!redis_display.contains("user:pass"));
        assert!(!redis_debug.contains("user:pass"));
        assert!(!db_debug.contains("user:pass"));
        assert_eq!(uri_display, FIXTURE_BOLT);
        assert!(uri_debug.contains(FIXTURE_BOLT));
        assert!(!uri_debug.contains("user:pass"));
    }

    #[test]
    fn production_toml_shapes_round_trip_raw_urls_and_redact_display() {
        let daemon = DaemonConfig::try_from_str(RAILWAY_DATA_CONFIG_TOML)
            .expect("Railway /data/config.toml shape parses");
        assert_eq!(
            daemon.api.public_addr,
            SocketAddr::from(([0, 0, 0, 0], 8080))
        );
        assert_eq!(daemon.stack.files_path, PathBuf::from("/data/static/files"));
        assert_fixture_connection_urls(&daemon.stack.db);
        assert!(!format!("{daemon:?}").contains("user:pass"));
        assert!(!format!("{}", daemon.stack.db.redis).contains("pass"));

        let migration: ReserveScrubMigrationConfig =
            toml::from_str(RESERVE_SCRUB_MIGRATION_CONFIG_TOML)
                .expect("reserve-scrub migrations/config.toml shape parses");
        assert_eq!(migration.name, "nexusd.migration");
        assert_eq!(
            migration.backfill_ready,
            vec![
                "ListingAuctionTermsReindex1787256279",
                "ReviewBackfill1787905961"
            ]
        );
        assert!(!migration.testnet);
        assert_eq!(migration.testnet_host, "localhost");
        assert_eq!(
            migration.stack.files_path,
            PathBuf::from("/data/static/files")
        );
        assert_fixture_connection_urls(&migration.stack.db);
        assert!(!format!("{migration:?}").contains("user:pass"));
        assert!(!format!("{}", migration.stack.db.redis).contains("pass"));
    }
}
