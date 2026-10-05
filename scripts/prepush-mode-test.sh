#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo, docker, nc and curl
# are stand-ins on PATH that log their arguments; nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
# A caller's CARGO_TARGET_DIR would make the gate seed its private target by
# copying that directory.
unset CARGO_TARGET_DIR

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

REAL_CARGO="$(command -v cargo)"
export REAL_CARGO
mkdir -p "$WORK/bin" "$WORK/broken"
# metadata goes to the real cargo, so package widening reads a real graph.
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = metadata ]; then
  exec "$REAL_CARGO" "$@"
fi
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
printf '#!/usr/bin/env bash\nexit 101\n' > "$WORK/broken/cargo"
chmod +x "$WORK/bin/"* "$WORK/broken/cargo"

# A real workspace with this repository's member graph: webapi and watcher
# depend on common; nexusd and the examples depend on all three.
manifest() {
  local dir="$1" name="$2" dep
  shift 2
  mkdir -p "$dir/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\n' "$name" > "$dir/Cargo.toml"
  for dep in "$@"; do
    printf '%s = { path = "../%s" }\n' "$dep" "$dep" >> "$dir/Cargo.toml"
  done
  printf '// lib\n' > "$dir/src/lib.rs"
  printf '// extra\n' > "$dir/src/extra.rs"
}

git init -q "$WORK/ws"
cd "$WORK/ws"
git config user.email test@example.invalid
git config user.name test
printf '[workspace]\nresolver = "2"\nmembers = ["nexus-webapi", "nexus-common", "nexus-watcher", "nexusd", "examples"]\n' > Cargo.toml
manifest nexus-common nexus-common
manifest nexus-webapi nexus-webapi nexus-common
manifest nexus-watcher nexus-watcher nexus-common
manifest nexusd nexusd nexus-common nexus-webapi nexus-watcher
mkdir -p examples/src
printf '[package]\nname = "nexus-examples"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nnexus-common = { path = "../nexus-common" }\nnexus-webapi = { path = "../nexus-webapi" }\nnexus-watcher = { path = "../nexus-watcher" }\n' > examples/Cargo.toml
printf '// lib\n' > examples/src/lib.rs
"$REAL_CARGO" generate-lockfile --offline --quiet
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
printf '/target\n' > .gitignore
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
all_five="nexus-common nexus-examples nexus-watcher nexus-webapi nexusd"

echo "test: packages: no change selects nothing"
[ -z "$(pkgs)" ] || fail "clean tree: $(pkgs)"

echo "test: packages: a nexus-common change selects every package built on it"
printf '// x\n' >> nexus-common/src/lib.rs
git commit -qam common
[ "$(pkgs)" = "$all_five" ] || fail "common: $(pkgs)"
reset_ws

echo "test: packages: a watcher change adds nexusd and the examples, not webapi"
printf '// x\n' >> nexus-watcher/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexus-watcher nexusd" ] || fail "watcher: $(pkgs)"
reset_ws

echo "test: packages: a webapi change adds nexusd and the examples, not the watcher"
printf '// x\n' >> nexus-webapi/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexus-webapi nexusd" ] || fail "webapi: $(pkgs)"
reset_ws

echo "test: packages: nexusd and examples changes select only themselves"
printf '// x\n' >> nexusd/src/lib.rs
git add nexusd/src/lib.rs
printf '// x\n' >> examples/src/lib.rs
[ "$(pkgs)" = "nexus-examples nexusd" ] || fail "nexusd and examples: $(pkgs)"
reset_ws

echo "test: packages: an untracked file alone selects its package and dependents"
printf '// new\n' > nexus-watcher/src/new.rs
[ "$(pkgs)" = "nexus-examples nexus-watcher nexusd" ] || fail "untracked: $(pkgs)"
reset_ws

echo "test: packages: deletes and both sides of a rename count"
git mv nexusd/src/extra.rs nexus-webapi/src/moved.rs
[ "$(pkgs)" = "nexus-examples nexus-webapi nexusd" ] || fail "rename: $(pkgs)"
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

echo "test: packages: an unreadable package graph selects --workspace"
printf '// x\n' >> nexusd/src/lib.rs
got="$(PATH="$WORK/broken:$PATH" pkgs 2>/dev/null)"
[ "$got" = --workspace ] || fail "failing cargo metadata: $got"
reset_ws

echo "test: packages: widening is transitive and counts dev-dependencies"
(
  git init -q "$WORK/chain"
  cd "$WORK/chain"
  printf '[workspace]\nresolver = "2"\nmembers = ["a", "b", "c", "d", "e"]\n' > Cargo.toml
  for spec in "a:" "b:a" "c:b" "e:"; do
    name="${spec%%:*}"
    dep="${spec#*:}"
    mkdir -p "$name/src"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "$name" > "$name/Cargo.toml"
    [ -z "$dep" ] || printf '\n[dependencies]\n%s = { path = "../%s" }\n' "$dep" "$dep" >> "$name/Cargo.toml"
    printf '// lib\n' > "$name/src/lib.rs"
  done
  mkdir -p d/src
  printf '[package]\nname = "d"\nversion = "0.1.0"\nedition = "2021"\n\n[dev-dependencies]\na = { path = "../a" }\n' > d/Cargo.toml
  printf '// lib\n' > d/src/lib.rs
  "$REAL_CARGO" generate-lockfile --offline --quiet
  got="$(prepush_with_dependents a | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "a b c d" ] || fail "chain from a: $got"
  got="$(prepush_with_dependents c e | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "c e" ] || fail "leaves: $got"
)

echo "test: packages: a base git cannot read fails"
if prepush_changed_packages 0000000000000000000000000000000000000001 >/dev/null 2>&1; then
  fail "unknown base returned 0"
fi

echo "test: packages: this repository's graph widens nexus-common to every package"
got="$(cd "$DIR/.." && prepush_with_dependents nexus-common | tr '\n' ' ' | sed 's/ $//')"
[ "$got" = "$all_five" ] || fail "repository graph: $got"

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
clippy -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1" ] \
  || fail "nexusd fast: cargo calls: $(cargo_log)"

echo "test: gate: fast widens a watcher change to nexusd and the examples"
commit_change nexus-watcher/src/lib.rs
st="$(gate)"
expect_ok "$st" "watcher fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexus-examples -p nexus-watcher -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexus-examples -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1" ] \
  || fail "watcher fast: cargo calls: $(cargo_log)"

echo "test: gate: fast widens a nexus-common change to every package, by name"
commit_change nexus-common/src/lib.rs
st="$(gate)"
expect_ok "$st" "common fast" fast
[ "$(cargo_log)" = "fmt --check
clippy -p nexus-common -p nexus-examples -p nexus-watcher -p nexus-webapi -p nexusd --all-targets -- -D warnings -A clippy::double_must_use
run -p nexusd -- db mock
test -p nexus-common -p nexus-examples -p nexus-webapi -p nexusd --lib --bins --tests --no-fail-fast -- --test-threads=1
nextest run -p nexus-watcher --no-fail-fast -j 1" ] \
  || fail "common fast: cargo calls: $(cargo_log)"

echo "test: gate: fast runs fmt only, and starts nothing, when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
for tool in docker nc curl; do
  [ ! -e "$WORK/log/$tool" ] || fail "docs fast called $tool: $(cat "$WORK/log/$tool")"
done

workspace_calls="fmt --check
clippy --workspace --all-targets -- -D warnings -A clippy::double_must_use
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
