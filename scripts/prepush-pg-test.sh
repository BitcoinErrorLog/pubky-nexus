#!/usr/bin/env bash
# Proves scripts/prepush-pg.sh gives each worktree its own Postgres and
# removes the old shared container only when nothing uses it. Needs Docker
# and the postgres:18-alpine and alpine images. Every container it creates
# is named prepush-pg-test-<pid>-*, and all of them are removed on exit.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-pg-test.XXXXXX")"
TAG="prepush-pg-test-$$"
export NEXUS_PG_LEGACY_NAME="${TAG}-legacy"
export POSTGRES_USER=test_user POSTGRES_PASSWORD=test_pass POSTGRES_DB=postgres
# shellcheck source=prepush-pg.sh
source "$DIR/prepush-pg.sh"

pids=""
cleanup() {
  local p c
  for p in $pids; do
    kill -9 "$p" 2>/dev/null || true
  done
  for c in $(docker ps -a --format '{{.Names}}' | grep "^${TAG}" || true); do
    docker rm -f -v "$c" >/dev/null 2>&1 || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pg_ready() {
  local name="$1" i
  for i in $(seq 1 60); do
    if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name" \
      psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "test: the legacy marker matches the old gate's default-name line"
# shellcheck disable=SC2016
old_line='name="${PREPUSH_PG_CONTAINER:-prepush-nexus-pg}"'
printf '%s\n' "$old_line" | grep -qF -- "$(NEXUS_PG_LEGACY_MARKER= bash -c 'source "$1"; printf %s "$NEXUS_PG_LEGACY_MARKER"' _ "$DIR/prepush-pg.sh")" \
  || fail "default marker does not match the old line"
if grep -qF -- 'PREPUSH_PG_CONTAINER:-prepush-nexus-pg}' "$DIR/prepush.sh"; then
  fail "this prepush.sh still carries the old marker"
fi

echo "test: names and ports are per worktree and stable"
a="$(nexus_pg_name /w/one/lane)"
b="$(nexus_pg_name /w/two/lane)"
[ "$a" != "$b" ] || fail "same basename in two paths shares a name: $a"
[ "$a" = "$(nexus_pg_name /w/one/lane)" ] || fail "name is not stable"
[[ "$a" =~ ^prepush-nexus-pg-lane-[0-9]+$ ]] || fail "name shape: $a"
p="$(nexus_pg_port_start /w/one/lane)"
[ "$p" -ge 55500 ] && [ "$p" -lt 55900 ] || fail "port start out of range: $p"

echo "test: a gate recreates only its own container"
root_a="$WORK/lane-a"
root_b="$WORK/lane-b"
name_a="${TAG}-a"
name_b="${TAG}-b"
docker run -d --name "$name_b" --label "${NEXUS_PG_OWNER_LABEL}=${root_b}" alpine sleep 300 >/dev/null
port_a="$(nexus_pg_recreate "$name_a" "$root_a")"
pg_ready "$name_a" || fail "own container did not become ready"
first_id="$(docker inspect -f '{{.Id}}' "$name_a")"
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name_a" \
  psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -qc 'CREATE DATABASE leftover' >/dev/null
port_a2="$(nexus_pg_recreate "$name_a" "$root_a")"
pg_ready "$name_a" || fail "recreated container did not become ready"
[ "$(docker inspect -f '{{.Id}}' "$name_a")" != "$first_id" ] || fail "container was not recreated"
left="$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$name_a" \
  psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM pg_database WHERE datname = 'leftover'")"
[ "$left" = 0 ] || fail "leftover database survived the recreate"
[ "$(docker inspect -f '{{.State.Running}}' "$name_b")" = true ] || fail "another worktree's container was touched"
[ "$port_a" -ge 55500 ] && [ "$port_a2" -ge 55500 ] || fail "ports: $port_a $port_a2"

echo "test: a container with this name that another worktree owns is refused"
docker run -d --name "${TAG}-c" --label "${NEXUS_PG_OWNER_LABEL}=${root_b}" alpine sleep 300 >/dev/null
if nexus_pg_recreate "${TAG}-c" "$root_a" >/dev/null 2>"$WORK/refuse.err"; then
  fail "recreated a container owned by another worktree"
fi
grep -q 'did not create it' "$WORK/refuse.err" || fail "no refusal message"
[ "$(docker inspect -f '{{.State.Running}}' "${TAG}-c")" = true ] || fail "foreign container was removed"

echo "test: a taken port moves to the next free one"
nc_port="$(nexus_pg_port_start "$WORK/lane-d")"
docker run -d --name "${TAG}-blocker" -p "127.0.0.1:${nc_port}:5432" alpine sleep 300 >/dev/null
port_d="$(nexus_pg_recreate "${TAG}-d" "$WORK/lane-d")"
[ "$port_d" != "$nc_port" ] || fail "used the taken port $nc_port"

echo "test: a stopped legacy container is removed"
docker create --name "$NEXUS_PG_LEGACY_NAME" alpine true >/dev/null
nexus_pg_legacy_cleanup | grep -q 'removed' || fail "stopped legacy not reported removed"
nexus_pg_exists "$NEXUS_PG_LEGACY_NAME" && fail "stopped legacy still exists"

echo "test: a legacy container is kept while an old-style gate runs"
docker create --name "$NEXUS_PG_LEGACY_NAME" alpine true >/dev/null
mkdir -p "$WORK/old/scripts"
{
  printf '#!/usr/bin/env bash\n'
  printf '%s\n' "$old_line"
  printf 'sleep 60\n'
} > "$WORK/old/scripts/prepush.sh"
(cd "$WORK/old" && exec bash scripts/prepush.sh) &
old_gate=$!
pids="$pids $old_gate"
sleep 1
nexus_pg_legacy_cleanup | grep -q 'old-style gate' || fail "old gate not detected"
nexus_pg_exists "$NEXUS_PG_LEGACY_NAME" || fail "legacy removed while an old gate ran"
kill -9 "$old_gate" 2>/dev/null || true
wait "$old_gate" 2>/dev/null || true
docker rm -f -v "$NEXUS_PG_LEGACY_NAME" >/dev/null

echo "test: a running legacy container is kept while a client is connected, removed when idle"
docker run -d --name "$NEXUS_PG_LEGACY_NAME" \
  -e POSTGRES_USER="$POSTGRES_USER" -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" "$NEXUS_PG_IMAGE" >/dev/null
pg_ready "$NEXUS_PG_LEGACY_NAME" || fail "legacy Postgres did not become ready"
docker exec -d -e PGPASSWORD="$POSTGRES_PASSWORD" "$NEXUS_PG_LEGACY_NAME" \
  psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT pg_sleep(8)'
sleep 1
nexus_pg_legacy_cleanup | grep -q 'client connection' || fail "open client not detected"
nexus_pg_exists "$NEXUS_PG_LEGACY_NAME" || fail "legacy removed with a client connected"
sleep 9
nexus_pg_legacy_cleanup | grep -q 'removed' || fail "idle legacy not removed"
nexus_pg_exists "$NEXUS_PG_LEGACY_NAME" && fail "idle legacy still exists"

echo "ALL OK"
