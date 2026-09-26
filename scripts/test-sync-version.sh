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
    examples/quarkus/pom.xml \
    integrations/ratchet-quarkus/README.md \
    integrations/ratchet-spring-boot/consumer-tests/pom.xml \
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
initial_quarkus_version_count="$(
  grep -Fc "<version>$initial_quarkus_version</version>" "$FIXTURE/website/docs/deployment/quarkus.md"
)"

# Once the starters are published, the guide keeps its released coordinate
# through development bumps. Before that, it follows the checkout version.
initial_spring_version=""
if ! grep -Fq '<!-- spring-starter-unreleased:start -->' \
    "$FIXTURE/website/docs/deployment/spring-boot.md"; then
  initial_spring_version="$(
    sed -n 's/.*<ratchet\.version>\([^<]*\)<\/ratchet\.version>.*/\1/p' \
      "$FIXTURE/website/docs/deployment/spring-boot.md" | head -n 1
  )"
  if [[ -z "$initial_spring_version" || "$initial_spring_version" == *-SNAPSHOT ]]; then
    echo "Spring public version is missing or unpublished: $initial_spring_version" >&2
    exit 1
  fi
fi

# A local development bump must not invent an unpublished public coordinate.
env -u RELEASE_VERSION "$FIXTURE/scripts/sync-version.sh" 9.8.7-SNAPSHOT >/dev/null
assert_contains README.md "<version>$initial_public_version</version>"
assert_contains examples/quarkus/pom.xml "<ratchet.version>$initial_public_version</ratchet.version>"
assert_count website/docs/deployment/quarkus.md "<version>$initial_quarkus_version</version>" "$initial_quarkus_version_count"
assert_count integrations/ratchet-quarkus/README.md "<version>$initial_quarkus_version</version>" 2
assert_contains README.md 'Ratchet is in **9.8.7-SNAPSHOT**.'
assert_contains infra/loadtest/Dockerfile 'ratchet-loadtest-9.8.7-SNAPSHOT.war'
assert_contains integrations/ratchet-spring-boot/consumer-tests/pom.xml '<ratchet.version>9.8.7-SNAPSHOT</ratchet.version>'
if [[ -n "$initial_spring_version" ]]; then
  assert_contains website/docs/deployment/spring-boot.md "<ratchet.version>$initial_spring_version</ratchet.version>"
else
  assert_contains website/docs/deployment/spring-boot.md '<ratchet.version>9.8.7-SNAPSHOT</ratchet.version>'
fi

# Exercise placeholder expansion even when a previous release already replaced
# every placeholder in the checked-in guide.
perl -0777 -i -pe \
  's{(<artifactId>ratchet-quarkus-parent</artifactId>\s*<version>)[^<]+(</version>)}{$1\${ratchet.version}$2}g' \
  "$FIXTURE/website/docs/deployment/quarkus.md"
assert_contains website/docs/deployment/quarkus.md '<version>${ratchet.version}</version>'

# Keep the unpublished guide: the release workflow restores main before its bump PR.
cp "$FIXTURE/website/docs/deployment/spring-boot.md" "$FIXTURE/spring-guide-before-release.md"

# Cutting a release advances both public and project references.
"$FIXTURE/scripts/sync-version.sh" 9.8.7 >/dev/null
assert_contains README.md '<version>9.8.7</version>'
assert_contains examples/quarkus/pom.xml '<ratchet.version>9.8.7</ratchet.version>'
assert_contains README.md 'Ratchet is in **9.8.7**.'
assert_contains website/docs/deployment/database-setup.md 'ratchet-store-postgresql-9.8.7.jar'
assert_contains website/docs/deployment/oracle.md 'ratchet-store-oracle-9.8.7.jar'
assert_contains website/docs/deployment/sqlserver.md 'ratchet-store-sqlserver-9.8.7.jar'
assert_count website/docs/deployment/quarkus.md '<version>9.8.7</version>' 5
assert_count integrations/ratchet-quarkus/README.md '<version>9.8.7</version>' 2
assert_contains integrations/ratchet-spring-boot/consumer-tests/pom.xml '<ratchet.version>9.8.7</ratchet.version>'
assert_contains website/docs/deployment/spring-boot.md '<ratchet.version>9.8.7</ratchet.version>'

# The following development bump keeps public snippets on the release while
# advancing source-tree and verified-against references to the next SNAPSHOT.
cp "$FIXTURE/spring-guide-before-release.md" "$FIXTURE/website/docs/deployment/spring-boot.md"
RELEASE_VERSION=9.8.7 "$FIXTURE/scripts/sync-version.sh" 9.8.8-SNAPSHOT >/dev/null
assert_contains README.md '<version>9.8.7</version>'
assert_contains examples/quarkus/pom.xml '<ratchet.version>9.8.7</ratchet.version>'
assert_contains README.md 'Ratchet is in **9.8.8-SNAPSHOT**.'
assert_contains website/docs/use-cases/durable-llm-workflows.md '<version>9.8.7</version>'
assert_contains website/docs/use-cases/durable-llm-workflows.md '`ratchet-api` `9.8.8-SNAPSHOT`'
assert_contains website/docs/deployment/oracle.md 'ratchet-store-oracle-9.8.7.jar'
assert_contains website/docs/deployment/sqlserver.md 'ratchet-store-sqlserver-9.8.7.jar'
assert_count website/docs/deployment/quarkus.md '<version>9.8.7</version>' 5
assert_count integrations/ratchet-quarkus/README.md '<version>9.8.7</version>' 2
assert_contains infra/loadtest/Dockerfile 'ratchet-loadtest-9.8.8-SNAPSHOT.war'
assert_contains integrations/ratchet-spring-boot/consumer-tests/pom.xml '<ratchet.version>9.8.8-SNAPSHOT</ratchet.version>'
assert_contains website/docs/deployment/spring-boot.md '<ratchet.version>9.8.7</ratchet.version>'
assert_count website/docs/deployment/spring-boot.md 'starters are unreleased' 0
assert_count website/docs/deployment/spring-boot.md 'staged_repo' 0

# Repeating the same transition is idempotent.
before="$(tree_digest)"
RELEASE_VERSION=9.8.7 "$FIXTURE/scripts/sync-version.sh" 9.8.8-SNAPSHOT >/dev/null
after="$(tree_digest)"
if [[ "$before" != "$after" ]]; then
  echo "sync-version changed files on an idempotent rerun" >&2
  exit 1
fi

# Ordinary development bumps also retain an already released starter guide.
env -u RELEASE_VERSION "$FIXTURE/scripts/sync-version.sh" 9.8.9-SNAPSHOT >/dev/null
assert_contains website/docs/deployment/spring-boot.md '<ratchet.version>9.8.7</ratchet.version>'
assert_count website/docs/deployment/spring-boot.md 'starters are unreleased' 0

echo "sync-version transition checks passed"
