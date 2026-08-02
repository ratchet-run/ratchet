#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-sync-version.XXXXXX")"
trap 'rm -rf "$FIXTURE"' EXIT

mkdir -p "$FIXTURE/scripts"
cp "$ROOT/scripts/sync-version.sh" "$FIXTURE/scripts/sync-version.sh"

while IFS= read -r path; do
  mkdir -p "$FIXTURE/$(dirname "$path")"
  cp "$ROOT/$path" "$FIXTURE/$path"
done < <(
  git -C "$ROOT" ls-files \
    README.md \
    integrations/ratchet-quarkus/README.md \
    website/docs \
    infra/loadtest/Dockerfile \
    .github/ISSUE_TEMPLATE/bug_report.yml
)

assert_contains() {
  local file="$1" expected="$2"
  if ! grep -Fq "$expected" "$FIXTURE/$file"; then
    echo "expected $file to contain: $expected" >&2
    exit 1
  fi
}

assert_count() {
  local file="$1" expected="$2" count="$3"
  if [[ ! -f "$FIXTURE/$file" ]]; then
    echo "missing fixture file: $file" >&2
    exit 1
  fi
  local actual
  actual="$(grep -Fc "$expected" "$FIXTURE/$file" || true)"
  if [[ "$actual" -ne "$count" ]]; then
    echo "expected $file to contain $count occurrence(s) of: $expected; got $actual" >&2
    exit 1
  fi
}

tree_digest() {
  find "$FIXTURE" -type f -exec shasum {} + | LC_ALL=C sort | shasum | awk '{print $1}'
}

initial_public_version="$(
  sed -n '/<artifactId>ratchet-bom<\/artifactId>/{n;s/.*<version>\([^<]*\)<\/version>.*/\1/p;q;}' \
    "$FIXTURE/README.md"
)"
if [[ -z "$initial_public_version" || "$initial_public_version" == *-SNAPSHOT ]]; then
  echo "README public version is missing or unpublished: $initial_public_version" >&2
  exit 1
fi
initial_quarkus_version="$(
  sed -n '/<artifactId>ratchet-quarkus<\/artifactId>/{n;s/.*<version>\([^<]*\)<\/version>.*/\1/p;q;}' \
    "$FIXTURE/website/docs/deployment/quarkus.md"
)"
if [[ -z "$initial_quarkus_version" || "$initial_quarkus_version" == *-SNAPSHOT ]]; then
  echo "Quarkus public version is missing or unpublished: $initial_quarkus_version" >&2
  exit 1
fi

# The current public guides omit these JAR commands because neither store was
# published in 0.1.1. Add fixture-only commands to prove both files participate
# in the first release that contains those artifacts.
printf '\njar xf ratchet-store-oracle-0.1.1.jar ddl/oracle-schema.sql\n' \
  >> "$FIXTURE/website/docs/deployment/oracle.md"
printf '\njar xf ratchet-store-sqlserver-0.1.1.jar ddl/sqlserver-schema.sql\n' \
  >> "$FIXTURE/website/docs/deployment/sqlserver.md"

# A local development bump must not invent an unpublished public coordinate.
env -u RELEASE_VERSION "$FIXTURE/scripts/sync-version.sh" 9.8.7-SNAPSHOT >/dev/null
assert_contains README.md "<version>$initial_public_version</version>"
assert_count website/docs/deployment/quarkus.md "<version>$initial_quarkus_version</version>" 4
assert_count integrations/ratchet-quarkus/README.md "<version>$initial_quarkus_version</version>" 2
assert_contains README.md 'Ratchet is in **9.8.7-SNAPSHOT**.'
assert_contains infra/loadtest/Dockerfile 'ratchet-loadtest-9.8.7-SNAPSHOT.war'

# Cutting a release advances both public and project references.
"$FIXTURE/scripts/sync-version.sh" 9.8.7 >/dev/null
assert_contains README.md '<version>9.8.7</version>'
assert_contains README.md 'Ratchet is in **9.8.7**.'
assert_contains website/docs/deployment/database-setup.md 'ratchet-store-postgresql-9.8.7.jar'
assert_contains website/docs/deployment/oracle.md 'ratchet-store-oracle-9.8.7.jar'
assert_contains website/docs/deployment/sqlserver.md 'ratchet-store-sqlserver-9.8.7.jar'
assert_count website/docs/deployment/quarkus.md '<version>9.8.7</version>' 4
assert_count integrations/ratchet-quarkus/README.md '<version>9.8.7</version>' 2

# The following development bump keeps public snippets on the release while
# advancing source-tree and verified-against references to the next SNAPSHOT.
RELEASE_VERSION=9.8.7 "$FIXTURE/scripts/sync-version.sh" 9.8.8-SNAPSHOT >/dev/null
assert_contains README.md '<version>9.8.7</version>'
assert_contains README.md 'Ratchet is in **9.8.8-SNAPSHOT**.'
assert_contains website/docs/use-cases/durable-llm-workflows.md '<version>9.8.7</version>'
assert_contains website/docs/use-cases/durable-llm-workflows.md '`ratchet-api` `9.8.8-SNAPSHOT`'
assert_contains website/docs/deployment/oracle.md 'ratchet-store-oracle-9.8.7.jar'
assert_contains website/docs/deployment/sqlserver.md 'ratchet-store-sqlserver-9.8.7.jar'
assert_count website/docs/deployment/quarkus.md '<version>9.8.7</version>' 4
assert_count integrations/ratchet-quarkus/README.md '<version>9.8.7</version>' 2
assert_contains infra/loadtest/Dockerfile 'ratchet-loadtest-9.8.8-SNAPSHOT.war'

# Repeating the same transition is idempotent.
before="$(tree_digest)"
RELEASE_VERSION=9.8.7 "$FIXTURE/scripts/sync-version.sh" 9.8.8-SNAPSHOT >/dev/null
after="$(tree_digest)"
if [[ "$before" != "$after" ]]; then
  echo "sync-version changed files on an idempotent rerun" >&2
  exit 1
fi

echo "sync-version transition checks passed"
