#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-spring-topology-test.XXXXXX")"
trap 'rm -rf "$FIXTURES"' EXIT

fail() {
  echo "publication topology script test: $*" >&2
  exit 1
}

make_fixture() {
  local name="$1"
  local fixture="$FIXTURES/$name"

  mkdir -p \
    "$fixture/scripts" \
    "$fixture/.github/workflows" \
    "$fixture/ratchet-bom" \
    "$fixture/integrations/ratchet-spring-boot"
  cp "$ROOT/scripts/test-spring-boot-publication-topology.sh" "$fixture/scripts/"
  cp "$ROOT/pom.xml" "$fixture/pom.xml"
  cp "$ROOT/ratchet-bom/pom.xml" "$fixture/ratchet-bom/pom.xml"
  cp "$ROOT/.github/workflows/release.yml" "$fixture/.github/workflows/release.yml"
  cp "$ROOT/integrations/ratchet-spring-boot/publication-topology.json" \
    "$fixture/integrations/ratchet-spring-boot/publication-topology.json"

  while IFS= read -r pom; do
    relative="${pom#"$ROOT/"}"
    mkdir -p "$fixture/$(dirname "$relative")"
    cp "$pom" "$fixture/$relative"
  done < <(
    find "$ROOT/integrations/ratchet-spring-boot" \
      -type d -name target -prune -o \
      -type f -name pom.xml -print \
      | LC_ALL=C sort
  )

  printf '%s\n' "$fixture"
}

assert_failure() {
  local fixture="$1"
  local expected="$2"
  local output

  if output="$(bash "$fixture/scripts/test-spring-boot-publication-topology.sh" 2>&1)"; then
    fail "expected validation failure containing: $expected"
  fi
  if ! grep -Fq "$expected" <<<"$output"; then
    echo "$output" >&2
    fail "validation failure did not contain: $expected"
  fi
}

success_fixture="$(make_fixture success)"
bash "$success_fixture/scripts/test-spring-boot-publication-topology.sh" >/dev/null

unmanifested_fixture="$(make_fixture unmanifested)"
mkdir -p "$unmanifested_fixture/integrations/ratchet-spring-boot/unmanifested"
cp \
  "$unmanifested_fixture/integrations/ratchet-spring-boot/integration-tests/postgresql/pom.xml" \
  "$unmanifested_fixture/integrations/ratchet-spring-boot/unmanifested/pom.xml"
assert_failure \
  "$unmanifested_fixture" \
  "module POM is not declared in publication-topology.json: integrations/ratchet-spring-boot/unmanifested"

missing_exclusion_fixture="$(make_fixture missing-exclusion)"
perl -0pi -e \
  's{\s*<excludeArtifact>ratchet-spring-boot-it-compatibility</excludeArtifact>}{}' \
  "$missing_exclusion_fixture/pom.xml"
assert_failure \
  "$missing_exclusion_fixture" \
  "ratchet-spring-boot-it-compatibility must have exactly one Central exclusion; found 0"

deploy_skip_fixture="$(make_fixture deploy-skip)"
perl -0pi -e \
  's{<maven\.deploy\.skip>true</maven\.deploy\.skip>}{<maven.deploy.skip>false</maven.deploy.skip>}' \
  "$deploy_skip_fixture/integrations/ratchet-spring-boot/integration-tests/compatibility/pom.xml"
assert_failure \
  "$deploy_skip_fixture" \
  "integration-tests/compatibility/pom.xml must declare maven.deploy.skip=true"

profile_deploy_skip_fixture="$(make_fixture profile-deploy-skip)"
perl -0pi -e '
  s{
    (<id>boot-3\.5</id>\s*
     <properties>)
  }{$1\n        <maven.deploy.skip>false</maven.deploy.skip>}x
' "$profile_deploy_skip_fixture/integrations/ratchet-spring-boot/integration-tests/compatibility/pom.xml"
assert_failure \
  "$profile_deploy_skip_fixture" \
  "integration-tests/compatibility/pom.xml must not override maven.deploy.skip away from true in a profile"

parent_fixture="$(make_fixture parent)"
perl -0pi -e \
  's{<relativePath>\.\./pom\.xml</relativePath>}{<relativePath>../../wrong-parent.xml</relativePath>}' \
  "$parent_fixture/integrations/ratchet-spring-boot/integration-tests/compatibility/pom.xml"
assert_failure \
  "$parent_fixture" \
  "integration-tests/compatibility/pom.xml parent relativePath is '../../wrong-parent.xml'; expected '../pom.xml'"

managed_version_fixture="$(make_fixture managed-version)"
perl -0pi -e '
  s{
    (<artifactId>ratchet-spring-boot-autoconfigure</artifactId>\s*
     <version>)\$\{project\.version\}(</version>)
  }{$1\${spring-boot35.version}$2}x
' "$managed_version_fixture/integrations/ratchet-spring-boot/pom.xml"
assert_failure \
  "$managed_version_fixture" \
  'Spring parent must manage ratchet-spring-boot-autoconfigure exactly once at ${project.version}; found 0'

release_inventory_fixture="$(make_fixture release-inventory)"
perl -0pi -e \
  's{JAR_PATHS=""}{JAR_PATHS=""\n          JAR_PATHS="$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-starter:ratchet-spring-boot-starter"}' \
  "$release_inventory_fixture/.github/workflows/release.yml"
assert_failure \
  "$release_inventory_fixture" \
  "unexpected Spring artifact in release inventory: ratchet-spring-boot-starter"

separate_release_collection_fixture="$(make_fixture separate-release-collection)"
perl -0pi -e '
  s{
    (\n\s+ls\ -la\ release-assets/)
  }{
    \n          cp "integrations/ratchet-spring-boot/ratchet-spring-boot-starter/target/ratchet-spring-boot-starter.jar" "release-assets/"$1
  }x
' "$separate_release_collection_fixture/.github/workflows/release.yml"
assert_failure \
  "$separate_release_collection_fixture" \
  "unexpected Spring artifact in release inventory: ratchet-spring-boot-starter"

glob_release_collection_fixture="$(make_fixture glob-release-collection)"
perl -0pi -e '
  s{
    (\n\s+ls\ -la\ release-assets/)
  }{
    \n          cp integrations/ratchet-spring-boot/*/target/*.jar "release-assets/"$1
  }x
' "$glob_release_collection_fixture/.github/workflows/release.yml"
assert_failure \
  "$glob_release_collection_fixture" \
  "release workflow references the Spring Boot tree while releaseInventory=false"

release_ready_fixture="$(make_fixture release-ready)"
perl -0pi -e \
  's{"releaseReady": false}{"releaseReady": true}' \
  "$release_ready_fixture/integrations/ratchet-spring-boot/publication-topology.json"
assert_failure \
  "$release_ready_fixture" \
  "publication-topology.json is malformed or enables the PR 1 publication gate"

echo "Spring Boot publication topology script checks passed"
