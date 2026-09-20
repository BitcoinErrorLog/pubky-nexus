use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::connection_url::ConnectionUrl;

mod neo4j;
pub use neo4j::Neo4JConfig;

pub const REDIS_URI: &str = "redis://localhost:6379";

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct DatabaseConfig {
    pub redis: ConnectionUrl,
    pub neo4j: Neo4JConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            redis: ConnectionUrl::from(REDIS_URI),
            neo4j: Neo4JConfig::default(),
        }
    }
}
