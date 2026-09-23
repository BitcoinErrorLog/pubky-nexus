#!/usr/bin/env bash
# Pre-push gate. Last line on success: PREPUSH OK <sha> <seconds>
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

start=$(date +%s)
LOCK="/Volumes/t7/vibes-dev/.locks/heavy.lock"

run_heavy() {
  mkdir -p "$(dirname "$LOCK")"
  lockf "$LOCK" "$@"
}

if [ ! -t 0 ]; then
  skip=1
  saw=0
  while read -r _local_ref local_sha _remote_ref remote_sha; do
    [ -n "${local_sha:-}" ] || continue
    saw=1
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
      continue
    fi
    if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
      range="$local_sha"
    else
      range="${remote_sha}..${local_sha}"
    fi
    if git log --format=%s "$range" | grep -qv '\[skip ci\]'; then
      skip=0
    fi
  done
  if [ "$saw" = 1 ] && [ "$skip" = 1 ]; then
    echo "prepush: every commit has [skip ci]; gate not run"
    exit 0
  fi
fi

echo "prepush: cargo fmt"
cargo fmt --check

if ! docker info >/dev/null 2>&1; then
  echo "prepush: docker is required for the scram-sha-256 Postgres" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. docker/.env-sample
set +a

port="${PREPUSH_PG_PORT:-55434}"
name="${PREPUSH_PG_CONTAINER:-prepush-nexus-pg}"
export POSTGRES_PORT="$port"
export TEST_PUBKY_CONNECTION_STRING="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${port}/${POSTGRES_DB}?pubky-test=true"

if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
    -p "127.0.0.1:${port}:5432" \
    postgres:18-alpine >/dev/null
fi

ready=0
for _ in $(seq 1 60); do
  if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "prepush: Postgres did not become ready" >&2
  exit 1
fi

encryption="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
host_all="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT auth_method FROM pg_hba_file_rules WHERE type = 'host' AND address = 'all'" | tr -d '[:space:]')"
if [ "$encryption" != "scram-sha-256" ] || [ "$host_all" != "scram-sha-256" ]; then
  echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} host_all=${host_all})" >&2
  exit 1
fi

if ! nc -z 127.0.0.1 6379 >/dev/null 2>&1; then
  echo "prepush: Redis is not accepting connections on 127.0.0.1:6379" >&2
  exit 1
fi

# db mock runs /test-graph/run-queries.sh via `docker exec neo4j`.
neo_name="${PREPUSH_NEO4J_CONTAINER:-neo4j}"
if ! nc -z 127.0.0.1 7687 >/dev/null 2>&1; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$neo_name"; then
    echo "prepush: Neo4j port 7687 is closed and container name ${neo_name} is already taken" >&2
    exit 1
  fi
  docker run -d --name "$neo_name" \
    -e NEO4J_AUTH="${NEO4J_DB_USERNAME}/${NEO4J_PASSWORD}" \
    -e NEO4J_server_memory_pagecache_size=1G \
    -e NEO4J_server_memory_heap_initial__size=2G \
    -e NEO4J_server_memory_heap_max__size=2G \
    -e NEO4J_dbms_usage__report_enabled=false \
    -e NEO4J_client_allow__telemetry=false \
    -p 127.0.0.1:7474:7474 \
    -p 127.0.0.1:7687:7687 \
    -v "$ROOT/docker/test-graph:/test-graph:ro" \
    neo4j:5.26.20-community >/dev/null
fi

neo_ready=0
for _ in $(seq 1 90); do
  if curl -sf http://127.0.0.1:7474 >/dev/null 2>&1; then
    neo_ready=1
    break
  fi
  sleep 2
done
if [ "$neo_ready" != 1 ]; then
  echo "prepush: Neo4j did not become ready" >&2
  exit 1
fi

echo "prepush: cargo clippy"
run_heavy cargo clippy --workspace --all-targets -- -D warnings

echo "prepush: cargo test"
run_heavy bash -c 'cargo run -p nexusd -- db mock && cargo test --workspace --no-fail-fast'

sha="$(git rev-parse HEAD)"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds}"
