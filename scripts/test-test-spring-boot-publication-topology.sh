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

  # Direct fixture callers exercise the release-ineligible state, while
  # make_release_ready_fixture applies the atomic flip below. Once the real
  # tree is release-ready, reverse that flip in the copy so both states remain
  # covered instead of inheriting whichever state the checkout happens to use.
  if jq -e '.releaseReady == true' \
      "$fixture/integrations/ratchet-spring-boot/publication-topology.json" \
      >/dev/null; then
    jq '
      .releaseReady = false
      | .coordinates |= map(
          .snapshotEligible = false
          | .centralEligible = false
          | .bomManaged = false
          | .releaseInventory = false
        )
    ' "$fixture/integrations/ratchet-spring-boot/publication-topology.json" \
      > "$fixture/topology.tmp"
    mv \
      "$fixture/topology.tmp" \
      "$fixture/integrations/ratchet-spring-boot/publication-topology.json"

    for path in \
      integrations/ratchet-spring-boot \
      integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure \
      integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa \
      integrations/ratchet-spring-boot/ratchet-spring-boot-starter \
      integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-mongodb \
      integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb \
      integrations/ratchet-spring-boot/ratchet-spring-boot-aot-spring7; do
      perl -0pi -e '
        s{(<properties>)}{$1
    <!-- Temporarily unpublished until the Spring release-candidate gate. -->
    <maven.deploy.skip>true</maven.deploy.skip>}
      ' "$fixture/$path/pom.xml"
    done

    perl -0pi -e '
      s{(\n\s*</excludeArtifacts>)}{
                <excludeArtifact>ratchet-spring-boot-parent</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-autoconfigure</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-autoconfigure-jpa</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-starter</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-autoconfigure-mongodb</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-starter-mongodb</excludeArtifact>
                <excludeArtifact>ratchet-spring-boot-aot-spring7</excludeArtifact>$1
      }x
    ' "$fixture/pom.xml"

    perl -0pi -e '
      s{\s*<!--\s*Spring\ Boot\ integration\ artifacts\.\s*-->}{}x;
      s{
        \s*<dependency>\s*
          <groupId>run\.ratchet</groupId>\s*
          <artifactId>ratchet-spring-boot-
            (?:autoconfigure(?:-jpa|-mongodb)?|starter(?:-mongodb)?|aot-spring7)
          </artifactId>\s*
          <version>\$\{project\.version\}</version>\s*
        </dependency>
      }{}gx
    ' "$fixture/ratchet-bom/pom.xml"

    # The ineligible-state guard rejects any reference to the Spring tree,
    # including the verifier path outside JAR_PATHS. Neutralize only the copied
    # workflow; individual fixtures add real tokens back for their assertions.
    perl -0pi -e \
      's{ratchet-spring-boot}{fixture-spring-boot}g' \
      "$fixture/.github/workflows/release.yml"
  fi

  printf '%s\n' "$fixture"
}

make_release_ready_fixture() {
  local name="$1"
  local fixture
  local artifact
  local path
  local production_artifacts=(
    ratchet-spring-boot-parent
    ratchet-spring-boot-autoconfigure
    ratchet-spring-boot-autoconfigure-jpa
    ratchet-spring-boot-starter
    ratchet-spring-boot-autoconfigure-mongodb
    ratchet-spring-boot-starter-mongodb
    ratchet-spring-boot-aot-spring7
  )
  local production_paths=(
    integrations/ratchet-spring-boot
    integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure
    integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa
    integrations/ratchet-spring-boot/ratchet-spring-boot-starter
    integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-mongodb
    integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb
    integrations/ratchet-spring-boot/ratchet-spring-boot-aot-spring7
  )

  fixture="$(make_fixture "$name")"

  jq '
    def production_jar_artifacts:
      [
        "ratchet-spring-boot-autoconfigure",
        "ratchet-spring-boot-autoconfigure-jpa",
        "ratchet-spring-boot-starter",
        "ratchet-spring-boot-autoconfigure-mongodb",
        "ratchet-spring-boot-starter-mongodb",
        "ratchet-spring-boot-aot-spring7"
      ];
    .releaseReady = true
    | .coordinates |= map(
        . as $coordinate
        | if $coordinate.artifactId == "ratchet-spring-boot-parent" then
            .snapshotEligible = true
            | .centralEligible = true
            | .bomManaged = false
            | .releaseInventory = false
          elif (production_jar_artifacts | index($coordinate.artifactId)) != null then
            .snapshotEligible = true
            | .centralEligible = true
            | .bomManaged = true
            | .releaseInventory = true
          else
            .snapshotEligible = false
            | .centralEligible = false
            | .bomManaged = false
            | .releaseInventory = false
          end
      )
  ' "$fixture/integrations/ratchet-spring-boot/publication-topology.json" \
    > "$fixture/topology.tmp"
  mv \
    "$fixture/topology.tmp" \
    "$fixture/integrations/ratchet-spring-boot/publication-topology.json"

  for path in "${production_paths[@]}"; do
    perl -0pi -e '
      s{
        \n\s*<!--\s*Temporarily\ unpublished\ until\ the\ Spring
        \ release-candidate\ gate\.\s*-->
        \s*<maven\.deploy\.skip>true</maven\.deploy\.skip>
      }{}x
    ' "$fixture/$path/pom.xml"
  done

  for artifact in "${production_artifacts[@]}"; do
    perl -0pi -e \
      "s{\\s*<excludeArtifact>${artifact}</excludeArtifact>}{}" \
      "$fixture/pom.xml"
  done

  perl -0pi -e '
    s{(\n\s*</dependencies>)}{
            <!-- Spring Boot production artifacts. -->
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-autoconfigure</artifactId>
                <version>\${project.version}</version>
            </dependency>
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-autoconfigure-jpa</artifactId>
                <version>\${project.version}</version>
            </dependency>
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-starter</artifactId>
                <version>\${project.version}</version>
            </dependency>
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-autoconfigure-mongodb</artifactId>
                <version>\${project.version}</version>
            </dependency>
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-starter-mongodb</artifactId>
                <version>\${project.version}</version>
            </dependency>
            <dependency>
                <groupId>run.ratchet</groupId>
                <artifactId>ratchet-spring-boot-aot-spring7</artifactId>
                <version>\${project.version}</version>
            </dependency>$1
    }x
  ' "$fixture/ratchet-bom/pom.xml"

  perl -0pi -e '
    s{JAR_PATHS=""}{JAR_PATHS=""
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure:ratchet-spring-boot-autoconfigure"
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa:ratchet-spring-boot-autoconfigure-jpa"
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-starter:ratchet-spring-boot-starter"
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-mongodb:ratchet-spring-boot-autoconfigure-mongodb"
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb:ratchet-spring-boot-starter-mongodb"
          JAR_PATHS="\$JAR_PATHS integrations/ratchet-spring-boot/ratchet-spring-boot-aot-spring7:ratchet-spring-boot-aot-spring7"}
  ' "$fixture/.github/workflows/release.yml"

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

fixture_head() {
  local fixture="$1"
  local tree
  local commit

  git -C "$fixture" init -q
  git -C "$fixture" add .
  tree="$(git -C "$fixture" write-tree)"
  commit="$(
    GIT_AUTHOR_NAME="Ratchet Fixture" \
    GIT_AUTHOR_EMAIL="fixture@ratchet.run" \
    GIT_COMMITTER_NAME="Ratchet Fixture" \
    GIT_COMMITTER_EMAIL="fixture@ratchet.run" \
      git -C "$fixture" commit-tree "$tree" -m "fixture"
  )"
  git -C "$fixture" update-ref refs/heads/main "$commit"
  git -C "$fixture" symbolic-ref HEAD refs/heads/main
  printf '%s\n' "$commit"
}

write_valid_attestation() {
  local fixture="$1"
  local commit="$2"
  local scenario="${3:-postgresql-runtime}"
  local flavor="${scenario%-runtime}"
  local topology="$fixture/integrations/ratchet-spring-boot/publication-topology.json"
  local target="$fixture/integrations/ratchet-spring-boot/integration-tests/target"

  mkdir -p "$target"
  jq -n \
    --arg commit "$commit" \
    --arg scenario "$scenario" \
    --arg flavor "$flavor" \
    --slurpfile topology "$topology" '
      def digest:
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
      {
        schemaVersion: 1,
        algorithm: "SHA-256",
        commit: $commit,
        projectVersion: "fixture",
        scenario: {
          name: $scenario,
          sha256: digest
        },
        scenarios: [$scenario],
        coordinates: [
          $topology[0].coordinates[]
          | select(
              .requiredQualificationScenarios
              | index($scenario) != null
            )
          | {
              coordinate: (
                "run.ratchet:" + .artifactId + ":" + .packaging
              ),
              source: (
                if .packaging == "pom"
                then "installed"
                else "jvm-matrix"
                end
              ),
              sha256: digest,
              scenarios: [$scenario]
            }
        ],
        runtimeDependencies: [
          {
            lane: "boot-3.5",
            flavor: $flavor,
            treeSha256: digest,
            dependencies: [
              {
                coordinate: "org.example:runtime:jar:1.0",
                sha256: digest,
                scopes: ["runtime"]
              }
            ]
          },
          {
            lane: "boot-4.1",
            flavor: $flavor,
            treeSha256: digest,
            dependencies: [
              {
                coordinate: "org.example:runtime:jar:2.0",
                sha256: digest,
                scopes: ["runtime"]
              }
            ]
          }
        ],
        reports: [
          {
            path: "reports/TEST-fixture.xml",
            lane: "boot-3.5",
            afterCommand: "jvm-matrix",
            kind: "surefire",
            reportClass: "run.ratchet.FixtureTest",
            sha256: digest
          }
        ],
        conformance: [
          {
            path: "conformance/boot-3.5/report.md",
            sha256: digest
          },
          {
            path: "conformance/boot-4.1/report.md",
            sha256: digest
          }
        ],
        toolchain: {
          javaVersion: "17.0.0",
          javaSpecificationVersion: 17,
          mavenVersion: "3.9.0",
          consumerJavaRuntime: 17
        }
      }
    ' > "$target/qualification-attestation.json"
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

for eligibility_flag in \
    snapshotEligible centralEligible bomManaged releaseInventory; do
  qualification_eligible_fixture="$(
    make_fixture "qualification-eligible-$eligibility_flag"
  )"
  jq --arg eligibilityFlag "$eligibility_flag" \
    '(.coordinates[0][$eligibilityFlag]) = true' \
    "$qualification_eligible_fixture/integrations/ratchet-spring-boot/publication-topology.json" \
    > "$qualification_eligible_fixture/topology.tmp"
  mv \
    "$qualification_eligible_fixture/topology.tmp" \
    "$qualification_eligible_fixture/integrations/ratchet-spring-boot/publication-topology.json"
  assert_failure \
    "$qualification_eligible_fixture" \
    "qualification-required coordinate must remain release-ineligible: ratchet-spring-boot-parent"
done

release_ready_fixture="$(make_release_ready_fixture release-ready)"
bash "$release_ready_fixture/scripts/test-spring-boot-publication-topology.sh" >/dev/null

release_ready_exclusion_fixture="$(
  make_release_ready_fixture release-ready-missing-exclusion-removal
)"
perl -0pi -e '
  s{(\n\s*</excludeArtifacts>)}{
                <excludeArtifact>ratchet-spring-boot-starter</excludeArtifact>$1
  }x
' "$release_ready_exclusion_fixture/pom.xml"
assert_failure \
  "$release_ready_exclusion_fixture" \
  "unexpected Spring Central exclusion not declared by publication-topology.json: ratchet-spring-boot-starter"

release_ready_bom_fixture="$(
  make_release_ready_fixture release-ready-missing-bom-entry
)"
perl -0pi -e '
  s{
    \s*<dependency>\s*
      <groupId>run\.ratchet</groupId>\s*
      <artifactId>ratchet-spring-boot-starter</artifactId>\s*
      <version>\$\{project\.version\}</version>\s*
    </dependency>
  }{}x
' "$release_ready_bom_fixture/ratchet-bom/pom.xml"
assert_failure \
  "$release_ready_bom_fixture" \
  "ratchet-spring-boot-starter must have exactly one ratchet-bom entry while bomManaged=true; found 0"

release_ready_inventory_fixture="$(
  make_release_ready_fixture release-ready-missing-release-inventory
)"
perl -0pi -e '
  s{
    \n\s*JAR_PATHS="\$JAR_PATHS\ integrations/ratchet-spring-boot/
    ratchet-spring-boot-starter:ratchet-spring-boot-starter"
  }{}x
' "$release_ready_inventory_fixture/.github/workflows/release.yml"
assert_failure \
  "$release_ready_inventory_fixture" \
  "ratchet-spring-boot-starter release inventory presence is false; expected true"

release_ready_deploy_skip_fixture="$(
  make_release_ready_fixture release-ready-deploy-skip
)"
perl -0pi -e '
  s{(<properties>)}{$1\n    <maven.deploy.skip>true</maven.deploy.skip>}
' "$release_ready_deploy_skip_fixture/integrations/ratchet-spring-boot/ratchet-spring-boot-starter/pom.xml"
assert_failure \
  "$release_ready_deploy_skip_fixture" \
  "ratchet-spring-boot-starter/pom.xml still declares maven.deploy.skip=true while snapshotEligible=true"

release_ready_it_eligible_fixture="$(
  make_release_ready_fixture release-ready-integration-test-eligible
)"
jq '
  (.coordinates[]
   | select(.artifactId == "ratchet-spring-boot-it-compatibility")
   | .snapshotEligible) = true
' "$release_ready_it_eligible_fixture/integrations/ratchet-spring-boot/publication-topology.json" \
  > "$release_ready_it_eligible_fixture/topology.tmp"
mv \
  "$release_ready_it_eligible_fixture/topology.tmp" \
  "$release_ready_it_eligible_fixture/integrations/ratchet-spring-boot/publication-topology.json"
assert_failure \
  "$release_ready_it_eligible_fixture" \
  "publication-topology.json is malformed or violates release-readiness invariants"

scenario_scoped_fixture="$(make_fixture scenario-scoped)"
scenario_scoped_commit="$(fixture_head "$scenario_scoped_fixture")"
write_valid_attestation "$scenario_scoped_fixture" "$scenario_scoped_commit"
if ! jq -e '
  ([.coordinates[].requiredQualificationScenarios[]] | unique | length) == 6
' "$scenario_scoped_fixture/integrations/ratchet-spring-boot/publication-topology.json" \
    >/dev/null; then
  fail "scenario-scoped fixture topology does not contain six scenarios"
fi
if ! jq -e '
  .scenario.name == "postgresql-runtime"
  and .scenarios == ["postgresql-runtime"]
  and (
    .coordinates
    | length > 0
      and all(.scenarios == ["postgresql-runtime"])
  )
' "$scenario_scoped_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
    >/dev/null; then
  fail "scenario-scoped fixture attestation is not limited to one scenario"
fi
bash "$scenario_scoped_fixture/scripts/test-spring-boot-publication-topology.sh" >/dev/null

stale_commit_fixture="$(make_fixture stale-commit)"
stale_current_commit="$(fixture_head "$stale_commit_fixture")"
write_valid_attestation "$stale_commit_fixture" "$stale_current_commit"
jq --arg commit "0000000000000000000000000000000000000000" \
  '.commit = $commit' \
  "$stale_commit_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
  > "$stale_commit_fixture/attestation.tmp"
mv \
  "$stale_commit_fixture/attestation.tmp" \
  "$stale_commit_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
assert_failure \
  "$stale_commit_fixture" \
  "qualification attestation commit is stale"

missing_scenario_fixture="$(make_fixture missing-scenario)"
missing_scenario_commit="$(fixture_head "$missing_scenario_fixture")"
write_valid_attestation "$missing_scenario_fixture" "$missing_scenario_commit"
jq '.scenarios = ["another-scenario"]' \
  "$missing_scenario_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
  > "$missing_scenario_fixture/attestation.tmp"
mv \
  "$missing_scenario_fixture/attestation.tmp" \
  "$missing_scenario_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
assert_failure \
  "$missing_scenario_fixture" \
  "qualification attestation is missing required scenario: postgresql-runtime"

unknown_scenario_fixture="$(make_fixture unknown-scenario)"
unknown_scenario_commit="$(fixture_head "$unknown_scenario_fixture")"
write_valid_attestation "$unknown_scenario_fixture" "$unknown_scenario_commit"
jq '
  .scenario.name = "unknown-runtime"
  | .scenarios = ["unknown-runtime"]
  | .coordinates[].scenarios = ["unknown-runtime"]
' \
  "$unknown_scenario_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
  > "$unknown_scenario_fixture/attestation.tmp"
mv \
  "$unknown_scenario_fixture/attestation.tmp" \
  "$unknown_scenario_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
assert_failure \
  "$unknown_scenario_fixture" \
  "qualification attestation scenario is not required by any coordinate: unknown-runtime"

missing_coordinate_fixture="$(make_fixture missing-coordinate)"
missing_coordinate_commit="$(fixture_head "$missing_coordinate_fixture")"
write_valid_attestation "$missing_coordinate_fixture" "$missing_coordinate_commit"
jq 'del(.coordinates[0])' \
  "$missing_coordinate_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
  > "$missing_coordinate_fixture/attestation.tmp"
mv \
  "$missing_coordinate_fixture/attestation.tmp" \
  "$missing_coordinate_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
assert_failure \
  "$missing_coordinate_fixture" \
  "qualification attestation is missing coordinate evidence: run.ratchet:ratchet-spring-boot-parent:pom"

missing_coordinate_coverage_fixture="$(make_fixture missing-coordinate-coverage)"
missing_coordinate_coverage_commit="$(fixture_head "$missing_coordinate_coverage_fixture")"
write_valid_attestation \
  "$missing_coordinate_coverage_fixture" \
  "$missing_coordinate_coverage_commit"
jq '.coordinates[0].scenarios = ["another-scenario"]' \
  "$missing_coordinate_coverage_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json" \
  > "$missing_coordinate_coverage_fixture/attestation.tmp"
mv \
  "$missing_coordinate_coverage_fixture/attestation.tmp" \
  "$missing_coordinate_coverage_fixture/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
assert_failure \
  "$missing_coordinate_coverage_fixture" \
  "qualification attestation coordinate run.ratchet:ratchet-spring-boot-parent:pom does not cover scenario: postgresql-runtime"

echo "Spring Boot publication topology script checks passed"
