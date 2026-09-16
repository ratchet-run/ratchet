#!/bin/sh
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT HUP INT TERM
cat > "$TEST_DIR/docker" <<'STUB'
#!/bin/sh
printf '%s\n' "$@" > "$LOADTEST_TEST_ARGS"
STUB
chmod +x "$TEST_DIR/docker"
export PATH="$TEST_DIR:$PATH"
export LOADTEST_TEST_ARGS="$TEST_DIR/args"
sh "$ROOT/infra/loadtest/run.sh"
grep -qx 'compose.postgresql.yml' "$LOADTEST_TEST_ARGS"
grep -qx 'ratchet-node=3' "$LOADTEST_TEST_ARGS"
sh "$ROOT/infra/loadtest/run.sh" mysql
grep -qx 'compose.mysql.yml' "$LOADTEST_TEST_ARGS"
sh "$ROOT/infra/loadtest/run.sh" mongodb 5 chaos
grep -qx 'ratchet-node=5' "$LOADTEST_TEST_ARGS"
grep -qx 'chaos' "$LOADTEST_TEST_ARGS"
for args in 'oracle 3' 'sqlserver 3' 'mysql 0' 'mysql invalid' 'mysql 2 unexpected'; do
  rm -f "$LOADTEST_TEST_ARGS"
  # Deliberate splitting of these fixed test argument vectors.
  if sh "$ROOT/infra/loadtest/run.sh" $args > "$TEST_DIR/output" 2>&1; then
    echo "Expected argument rejection: $args" >&2
    exit 1
  fi
  test ! -e "$LOADTEST_TEST_ARGS"
done
printf '%s\n' 'load-test launcher arguments: passed'
