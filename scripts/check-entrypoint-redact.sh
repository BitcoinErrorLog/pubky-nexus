#!/bin/sh
# Fail if entrypoint-railway.sh stdout/stderr contains URL userinfo (://user:pass@).
# The on-disk config must still receive the real Redis URL and Neo4j password.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
ENTRYPOINT="$ROOT/entrypoint-railway.sh"
test -f "$ENTRYPOINT"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/data"
printf '%s\n' '#!/bin/sh' 'exit 0' > "$TMP/bin/nexusd"
chmod +x "$TMP/bin/nexusd"

export PATH="$TMP/bin:$PATH"
export NEXUS_CONFIG_DIR="$TMP/data"
export NEXUS_REDIS_URL='redis://ci-user:ci-super-secret@redis.example:6379/0'
export NEXUS_NEO4J_PASSWORD='neo4j-super-secret'
export NEXUS_NEO4J_URI='bolt://neo4j.example:7687'

OUT="$TMP/out.txt"
# Do not print captured output: it is the secret-bearing fixture path on failure.
if ! sh "$ENTRYPOINT" >"$OUT" 2>&1; then
	echo "FAIL: entrypoint exited non-zero" >&2
	exit 1
fi

if grep -Eq '://[^/]*:[^@]*@' "$OUT"; then
	echo "FAIL: entrypoint output matched ://user:pass@ userinfo" >&2
	exit 1
fi

if grep -Fqe 'ci-super-secret' "$OUT" || grep -Fqe 'neo4j-super-secret' "$OUT" || grep -Fqe 'ci-user' "$OUT"; then
	echo "FAIL: entrypoint output contained fixture secret" >&2
	exit 1
fi

CFG="$TMP/data/config.toml"
test -f "$CFG"
if ! grep -Fqe 'redis://ci-user:ci-super-secret@redis.example:6379/0' "$CFG"; then
	echo "FAIL: on-disk config lost the Redis URL" >&2
	exit 1
fi
if ! grep -Fqe 'neo4j-super-secret' "$CFG"; then
	echo "FAIL: on-disk config lost the Neo4j password" >&2
	exit 1
fi

echo "PASS: entrypoint echo has no URL userinfo; on-disk config keeps credentials"
