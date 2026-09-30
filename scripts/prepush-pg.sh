#!/usr/bin/env bash
# Per-worktree Postgres for scripts/prepush.sh. Source it.
#
# Each worktree gets its own container, prepush-nexus-pg-<dir>-<hash of the
# worktree path>, labelled with that path. A gate removes and recreates only
# its own container (with its data volume), so leftover test databases never
# accumulate and two Nexus gates can run at once. The host port is the first
# free port from a start derived from the same hash, retried if docker
# reports it taken. PREPUSH_PG_CONTAINER and PREPUSH_PG_PORT override both.
#
# The old gate used one shared container, prepush-nexus-pg on 55434, which
# still-unmigrated worktrees keep using. nexus_pg_legacy_cleanup removes it
# only when no such gate is running and it has no client connections.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "source scripts/prepush-pg.sh" >&2
  exit 1
fi

NEXUS_PG_OWNER_LABEL="pubky.prepush.worktree"
NEXUS_PG_IMAGE="${NEXUS_PG_IMAGE:-postgres:18-alpine}"
NEXUS_PG_LEGACY_NAME="${NEXUS_PG_LEGACY_NAME:-prepush-nexus-pg}"
# The default-name line that only the old shared-container gate contains.
if [ -z "${NEXUS_PG_LEGACY_MARKER:-}" ]; then
  NEXUS_PG_LEGACY_MARKER='PREPUSH_PG_CONTAINER:-prepush-nexus-pg}'
fi

nexus_pg_hash() {
  printf '%s' "$1" | cksum | awk '{print $1}'
}

nexus_pg_name() {
  local root="$1" slug
  slug="$(printf '%s' "$(basename "$root")" | tr -c 'A-Za-z0-9_.-' '-')"
  printf 'prepush-nexus-pg-%s-%s\n' "$slug" "$(nexus_pg_hash "$root")"
}

nexus_pg_port_start() {
  printf '%s\n' "$((55500 + $(nexus_pg_hash "$1") % 400))"
}

nexus_pg_exists() {
  docker ps -a --format '{{.Names}}' | grep -qx "$1"
}

# Removes the named container only when this worktree created it.
nexus_pg_remove_own() {
  local name="$1" root="$2" owner
  if ! nexus_pg_exists "$name"; then
    return 0
  fi
  owner="$(docker inspect -f "{{index .Config.Labels \"${NEXUS_PG_OWNER_LABEL}\"}}" "$name")"
  if [ "$owner" != "$root" ]; then
    echo "prepush: container ${name} exists and this worktree did not create it (owner: ${owner:-none})" >&2
    return 1
  fi
  docker rm -f -v "$name" >/dev/null
}

# Starts a fresh container for this worktree and prints its host port.
# Needs POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB.
nexus_pg_recreate() {
  local name="$1" root="$2" fixed_port="${3:-}" port attempt err
  nexus_pg_remove_own "$name" "$root" || return 1
  if [ -n "$fixed_port" ]; then
    port="$fixed_port"
  else
    port="$(nexus_pg_port_start "$root")"
  fi
  for attempt in $(seq 1 40); do
    if [ -z "$fixed_port" ] && nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
      port=$((port + 1))
      continue
    fi
    if err="$(docker run -d --name "$name" \
      --label "${NEXUS_PG_OWNER_LABEL}=${root}" \
      -e POSTGRES_USER="$POSTGRES_USER" \
      -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
      -e POSTGRES_DB="$POSTGRES_DB" \
      -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
      -p "127.0.0.1:${port}:5432" \
      "$NEXUS_PG_IMAGE" 2>&1 >/dev/null)"; then
      printf '%s\n' "$port"
      return 0
    fi
    docker rm -f -v "$name" >/dev/null 2>&1 || true
    if [ -n "$fixed_port" ] || ! printf '%s' "$err" | grep -qiE 'port is already allocated|address already in use'; then
      echo "prepush: docker run ${name} on port ${port} failed: ${err}" >&2
      return 1
    fi
    port=$((port + 1))
  done
  echo "prepush: no free Postgres port for ${name} after 40 tries" >&2
  return 1
}

# 0 when a running prepush.sh is an old gate that uses the shared container.
nexus_pg_legacy_gate_running() {
  local pid cwd
  for pid in $(pgrep -f 'scripts/prepush\.sh' 2>/dev/null || true); do
    [ "$pid" != "$$" ] || continue
    cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -1)"
    [ -n "$cwd" ] || continue
    if [ -f "$cwd/scripts/prepush.sh" ] && grep -qF -- "$NEXUS_PG_LEGACY_MARKER" "$cwd/scripts/prepush.sh"; then
      return 0
    fi
  done
  return 1
}

nexus_pg_legacy_cleanup() {
  local legacy="$NEXUS_PG_LEGACY_NAME" running clients
  nexus_pg_exists "$legacy" || return 0
  if nexus_pg_legacy_gate_running; then
    echo "prepush: kept ${legacy}: an old-style gate that uses it is running"
    return 0
  fi
  running="$(docker inspect -f '{{.State.Running}}' "$legacy" 2>/dev/null || echo unknown)"
  if [ "$running" = true ]; then
    # shellcheck disable=SC2016
    if ! clients="$(docker exec "$legacy" sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM pg_stat_activity WHERE backend_type = '"'"'client backend'"'"' AND pid <> pg_backend_pid()"' 2>/dev/null | tr -d '[:space:]')" \
      || ! [[ "$clients" =~ ^[0-9]+$ ]]; then
      echo "prepush: kept ${legacy}: could not count its client connections"
      return 0
    fi
    if [ "$clients" != 0 ]; then
      echo "prepush: kept ${legacy}: ${clients} client connection(s) open"
      return 0
    fi
  elif [ "$running" != false ]; then
    echo "prepush: kept ${legacy}: state ${running}"
    return 0
  fi
  docker rm -f -v "$legacy" >/dev/null
  echo "prepush: removed the old shared container ${legacy}"
}
