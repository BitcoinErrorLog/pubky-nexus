/// Name of the watcher config file
pub const WATCHER_CONFIG_FILE_NAME: &str = "watcher-config.toml";
///  Per-homeserver hard timeout (seconds)
// TODO: Set timeout maybe from the config file
pub const PROCESSING_TIMEOUT_SECS: u64 = 3_600;
/// Hard deadline for one events poll (the GET to `/events/`). Kept far below
/// [`PROCESSING_TIMEOUT_SECS`]: a poll against an unreachable homeserver must
/// fail fast so it cannot starve the other homeservers' processors.
pub const POLL_TIMEOUT_SECS: u64 = 30;
