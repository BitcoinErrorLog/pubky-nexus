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

if ! docker info >/dev/null 2>&1; then
  echo "prepush: docker is required for the scram-sha-256 Postgres" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. docker/.env-sample
set +a
export TEST_PUBKY_CONNECTION_STRING

if ! docker ps --format '{{.Names}}' | grep -qx postgres; then
  docker compose --env-file .env-sample -f docker/docker-compose.yml up -d
fi

ready=0
for _ in $(seq 1 60); do
  if docker exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "prepush: Postgres did not become ready" >&2
  exit 1
fi

encryption="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" postgres psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
trust_hosts="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" postgres psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM pg_hba_file_rules WHERE type = 'host' AND auth_method = 'trust'")"
if [ "$encryption" != "scram-sha-256" ] || [ "${trust_hosts:-1}" != "0" ]; then
  echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} trust_host_rules=${trust_hosts})" >&2
  exit 1
fi

redis_ready=0
for _ in $(seq 1 60); do
  if nc -z 127.0.0.1 6379 >/dev/null 2>&1; then
    redis_ready=1
    break
  fi
  sleep 1
done
if [ "$redis_ready" != 1 ]; then
  echo "prepush: Redis did not become ready" >&2
  exit 1
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

echo "prepush: cargo fmt"
cargo fmt --check

echo "prepush: cargo clippy"
run_heavy cargo clippy --workspace --all-targets -- -D warnings

echo "prepush: cargo test"
run_heavy bash -c 'cargo run -p nexusd -- db mock && cargo test --workspace -- --no-fail-fast'

sha="$(git rev-parse HEAD)"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds}"
