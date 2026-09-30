#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo, docker, nc and curl
# are stand-ins on PATH that log their arguments; nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

mkdir -p "$WORK/bin"
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/cargo"
EOF
cat > "$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/docker"
case "$*" in
  *password_encryption*|*pg_hba_file_rules*) echo scram-sha-256 ;;
esac
exit 0
EOF
for tool in nc curl; do
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >> "$FAKE_LOG/%s"\n' "$tool" > "$WORK/bin/$tool"
done
chmod +x "$WORK/bin/"*

manifest() {
  mkdir -p "$1/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\n' "$2" > "$1/Cargo.toml"
  printf '// lib\n' > "$1/src/lib.rs"
}

git init -q "$WORK/ws"
cd "$WORK/ws"
git config user.email test@example.invalid
git config user.name test
printf '[workspace]\nmembers = ["nexus-webapi", "nexus-common", "nexus-watcher", "nexusd", "examples"]\n' > Cargo.toml
printf '# lock\n' > Cargo.lock
manifest nexus-common nexus-common
manifest nexus-webapi nexus-webapi
manifest nexus-watcher nexus-watcher
manifest nexusd nexusd
manifest examples nexus-examples
mkdir -p docs docker/test-graph scripts
printf 'doc\n' > docs/readme.md
printf 'CREATE (n);\n' > docker/test-graph/mocks.cypher
cat > docker/.env-sample <<'EOF'
POSTGRES_USER=test_user
POSTGRES_PASSWORD=test_pass
POSTGRES_DB=postgres
NEO4J_DB_USERNAME=neo4j
NEO4J_PASSWORD=12345678
EOF
cp "$DIR/prepush.sh" "$DIR/prepush-stamp.sh" "$DIR/prepush-packages.sh" \
  "$DIR/prepush-pg.sh" "$DIR/heavy-lock.sh" scripts/
git add -A
git commit -qm base
BASE="$(git rev-parse HEAD)"

# shellcheck source=prepush-packages.sh
source "$DIR/prepush-packages.sh"
pkgs() {
  prepush_changed_packages "$BASE" Cargo.toml Cargo.lock 'docker/*' | tr '\n' ' ' | sed 's/ $//'
}
reset_ws() {
  git reset -q --hard "$BASE"
  git clean -qfd
}

echo "test: packages: no change selects nothing"
[ -z "$(pkgs)" ] || fail "clean tree: $(pkgs)"

echo "test: packages: each changed package is listed once, sorted"
printf '// x\n' >> nexusd/src/lib.rs
printf '// x\n' >> nexus-common/src/lib.rs
printf '// new\n' > nexus-common/src/new.rs
git add nexusd/src/lib.rs
[ "$(pkgs)" = "nexus-common nexusd" ] || fail "two packages: $(pkgs)"
reset_ws

echo "test: packages: an untracked file alone selects its package"
printf '// new\n' > nexus-watcher/src/new.rs
[ "$(pkgs)" = nexus-watcher ] || fail "untracked: $(pkgs)"
reset_ws

echo "test: packages: the examples directory is package nexus-examples"
printf '// x\n' >> examples/src/lib.rs
[ "$(pkgs)" = nexus-examples ] || fail "examples: $(pkgs)"
reset_ws

echo "test: packages: deletes and both sides of a rename count"
git mv nexusd/src/lib.rs nexus-webapi/src/moved.rs
[ "$(pkgs)" = "nexus-webapi nexusd" ] || fail "rename: $(pkgs)"
reset_ws

echo "test: packages: docs and scripts select nothing"
printf 'more\n' >> docs/readme.md
printf 'echo\n' >> scripts/prepush.sh
[ -z "$(pkgs)" ] || fail "docs and scripts: $(pkgs)"
reset_ws

echo "test: packages: docker/ and the root manifests select --workspace"
for file in docker/test-graph/mocks.cypher docker/.env-sample Cargo.toml Cargo.lock; do
  printf '\n' >> "$file"
  [ "$(pkgs)" = --workspace ] || fail "$file: $(pkgs)"
  reset_ws
done

echo "test: packages: a base git cannot read fails"
if prepush_changed_packages 0000000000000000000000000000000000000001 >/dev/null 2>&1; then
  fail "unknown base returned 0"
fi

# The real gate. Each case commits one change on top of the base, so the
# selection is that change alone and no stamp is reused.
gate() {
  rm -rf "$WORK/log"
  mkdir -p "$WORK/log"
  local st=0
  FAKE_LOG="$WORK/log" PATH="$WORK/bin:$PATH" PREPUSH_BASE="$BASE" \
    PREPUSH_CARGO_TARGET="$WORK/target" PREPUSH_PG_PORT=55999 \
    NEXUS_PG_LEGACY_NAME="prepush-mode-test-legacy-$$" \
    HEAVY_LOCK_DIR="$WORK/locks" HEAVY_DISK_VOLUMES="" \
    bash scripts/prepush.sh </dev/null >"$WORK/out" 2>&1 || st=$?
  printf '%s\n' "$st"
}
cargo_log() { cat "$WORK/log/cargo" 2>/dev/null || true; }
expect_ok() {
  [ "$1" = 0 ] || fail "$2: status $1: $(cat "$WORK/out")"
  tail -1 "$WORK/out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) [0-9]* $3$" \
    || fail "$2: last line: $(tail -1 "$WORK/out")"
}
commit_change() {
  reset_ws
  printf '// %s %s\n' "$RANDOM" "$(date +%s)" >> "$1"
  git commit -qam "change $1"
}

echo "test: gate: fast tests only nexusd for a nexusd change"
commit_change nexusd/src/lib.rs
st="$(gate)"
expect_ok "$st" "nexusd fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexusd --all-targets -- -D warnings
run -p nexusd -- db mock
test -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1" ] \
  || fail "nexusd fast: cargo calls: $(cargo_log)"

echo "test: gate: fast runs only nextest for a watcher change"
commit_change nexus-watcher/src/lib.rs
st="$(gate)"
expect_ok "$st" "watcher fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexus-watcher --all-targets -- -D warnings
run -p nexusd -- db mock
nextest run -p nexus-watcher --no-fail-fast -j 1" ] \
  || fail "watcher fast: cargo calls: $(cargo_log)"

echo "test: gate: fast runs fmt only, and starts nothing, when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
for tool in docker nc curl; do
  [ ! -e "$WORK/log/$tool" ] || fail "docs fast called $tool: $(cat "$WORK/log/$tool")"
done

workspace_calls="fmt --check
clippy --workspace --all-targets -- -D warnings
run -p nexusd -- db mock
test --workspace --exclude nexus-watcher --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1"

echo "test: gate: fast runs the workspace for a docker/ change"
commit_change docker/test-graph/mocks.cypher
st="$(gate)"
expect_ok "$st" "docker fast" fast
[ "$(cargo_log)" = "$workspace_calls" ] || fail "docker fast: cargo calls: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=1 runs the workspace and does not reuse a fast pass"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast before full" fast
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "docs full" full
[ "$(cargo_log)" = "$workspace_calls" ] || fail "docs full: cargo calls: $(cargo_log)"

echo "test: gate: a fast run after a full pass reuses it"
commit_change docs/readme.md
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "full before fast" full
st="$(gate)"
expect_ok "$st" "fast after full" "fast reused:full .*"
[ -z "$(cargo_log)" ] || fail "fast after full ran cargo: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=yes is refused before any step"
commit_change docs/readme.md
st="$(PREPUSH_FULL=yes gate)"
[ "$st" = 1 ] || fail "PREPUSH_FULL=yes: status $st"
grep -q 'PREPUSH_FULL must be 1, 0 or unset' "$WORK/out" || fail "PREPUSH_FULL=yes: $(cat "$WORK/out")"
[ -z "$(cargo_log)" ] || fail "PREPUSH_FULL=yes ran cargo: $(cargo_log)"

echo "ALL OK"
