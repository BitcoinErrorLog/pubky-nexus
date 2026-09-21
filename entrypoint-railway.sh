#!/bin/sh
set -e

# Config dir is the Railway volume in production; tests override NEXUS_CONFIG_DIR.
CONFIG_DIR="${NEXUS_CONFIG_DIR:-/data}"

NEO4J_URI="${NEXUS_NEO4J_URI:-bolt://localhost:7687}"
NEO4J_PASS="${NEXUS_NEO4J_PASSWORD:-pubkywebindex}"
REDIS_URL="${NEXUS_REDIS_URL:-redis://127.0.0.1:6379}"
# Default: the official staging homeserver (homeserver.staging.pubky.app)
HOMESERVER="${NEXUS_HOMESERVER:-ufibwbmed6jeq9k4p583go95wofakh9fwpp4k734trq79pd9u1uy}"
API_PORT="${PORT:-8080}"
TESTNET="${NEXUS_TESTNET:-false}"
TESTNET_HOST="${NEXUS_TESTNET_HOST:-localhost}"
# Replay tuning: the watcher fetches EVENTS_LIMIT events per poll and sleeps
# WATCHER_SLEEP ms between polls. History replay from cursor zero is O(total
# events), so keep the batch large and the sleep short.
EVENTS_LIMIT="${NEXUS_EVENTS_LIMIT:-1000}"
WATCHER_SLEEP="${NEXUS_WATCHER_SLEEP:-500}"

# Echoed config must never contain URL userinfo (`://user:pass@`) or secret
# assignment values. The file written for nexusd keeps the real values.
redact_generated_config() {
	awk '
		{
			line = $0
			low = tolower(line)
			if (low ~ /^[[:space:]]*password[[:space:]]*=/) {
				print "password = \"<redacted>\""
				next
			}
			if (low ~ /^[[:space:]]*[a-z0-9_]*(_url|_password|_pass|_secret|_token|_auth)[[:space:]]*=/) {
				sub(/=.*/, "= \"<redacted>\"")
				print
				next
			}
			while (match(line, /:\/\/[^\/]*:[^@]*@/)) {
				line = substr(line, 1, RSTART - 1) "://[redacted]@" substr(line, RSTART + RLENGTH)
			}
			print line
		}
	'
}

mkdir -p "$CONFIG_DIR/static/files"

echo "=== Railway nexusd entrypoint ==="
echo "TESTNET=${TESTNET}"
echo "TESTNET_HOST=${TESTNET_HOST}"
echo "HOMESERVER=${HOMESERVER}"

cat > "$CONFIG_DIR/config.toml" <<EOF
[api]
name = "nexusd.api"
public_ip = "0.0.0.0"
public_addr = "0.0.0.0:${API_PORT}"
pubky_listen_socket = "0.0.0.0:8081"

[watcher]
name = "nexusd.watcher"
testnet = ${TESTNET}
testnet_host = "${TESTNET_HOST}"
homeserver = "${HOMESERVER}"
events_limit = ${EVENTS_LIMIT}
monitored_homeservers_limit = 50
watcher_sleep = ${WATCHER_SLEEP}
moderation_id = "${NEXUS_MODERATION_ID:-51y9w1skwcryb3iq4sia3x49qwpgstc5feo5tqon65gid7o99khy}"
moderated_tags = []

[stack]
log_level = "info"
files_path = "/data/static/files"

[stack.db]
redis = "${REDIS_URL}"

[stack.db.neo4j]
uri = "${NEO4J_URI}"
password = "${NEO4J_PASS}"
EOF

echo "Generated config (credentials redacted):"
redact_generated_config < "$CONFIG_DIR/config.toml"
exec nexusd --config-dir "$CONFIG_DIR"
