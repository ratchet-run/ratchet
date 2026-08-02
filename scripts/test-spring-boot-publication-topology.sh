#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "spring-boot publication topology: $*" >&2
  exit 1
}

for command in git jq perl; do
  command -v "$command" >/dev/null 2>&1 \
    || fail "required command is not available: $command"
done

MAVEN_REPO_ARGUMENT=""
for argument in "$@"; do
  case "$argument" in
    -Dmaven.repo.local=*)
      candidate="${argument#-Dmaven.repo.local=}"
      [[ -n "$candidate" ]] || fail "maven.repo.local must not be empty"
      [[ "$candidate" == /* ]] \
        || fail "maven.repo.local must be an absolute path: $candidate"
      if [[ -n "$MAVEN_REPO_ARGUMENT" && "$MAVEN_REPO_ARGUMENT" != "$candidate" ]]; then
        fail "conflicting maven.repo.local arguments"
      fi
      MAVEN_REPO_ARGUMENT="$candidate"
      ;;
    *)
      fail "unexpected argument: $argument"
      ;;
  esac
done

if [[ -n "${MAVEN_REPO_LOCAL:-}" ]]; then
  [[ "$MAVEN_REPO_LOCAL" == /* ]] \
    || fail "MAVEN_REPO_LOCAL must be an absolute path: $MAVEN_REPO_LOCAL"
  if [[ -n "$MAVEN_REPO_ARGUMENT" && "$MAVEN_REPO_ARGUMENT" != "$MAVEN_REPO_LOCAL" ]]; then
    fail "MAVEN_REPO_LOCAL does not match the maven.repo.local argument"
  fi
  MAVEN_REPO_ARGUMENT="$MAVEN_REPO_LOCAL"
fi

if [[ -n "$MAVEN_REPO_ARGUMENT" \
      && -n "${HOME:-}" \
      && "$MAVEN_REPO_ARGUMENT" == "${HOME}/.m2/repository" ]]; then
  fail "refusing to use the shared Maven repository: $MAVEN_REPO_ARGUMENT"
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPRING_ROOT="$ROOT/integrations/ratchet-spring-boot"
TOPOLOGY="$SPRING_ROOT/publication-topology.json"
ROOT_POM="$ROOT/pom.xml"
BOM_POM="$ROOT/ratchet-bom/pom.xml"
RELEASE_WORKFLOW="$ROOT/.github/workflows/release.yml"
ATTESTATION="$SPRING_ROOT/integration-tests/target/qualification-attestation.json"

for file in "$TOPOLOGY" "$ROOT_POM" "$BOM_POM" "$RELEASE_WORKFLOW"; do
  [[ -f "$file" ]] || fail "required file is missing: ${file#"$ROOT/"}"
done

qualification_eligible="$(
  jq -r '
    first(
      .coordinates[]?
      | select(
          (.requiredQualificationScenarios? | type == "array" and length > 0)
          and (
            .snapshotEligible == true
            or .centralEligible == true
            or .bomManaged == true
            or .releaseInventory == true
          )
        )
      | .artifactId
    ) // empty
  ' "$TOPOLOGY" 2>/dev/null || true
)"
if [[ -n "$qualification_eligible" ]]; then
  fail "qualification-required coordinate must remain release-ineligible: $qualification_eligible"
fi

if ! jq -e '
  type == "object"
  and (.releaseReady | type == "boolean")
  and (.coordinates | type == "array" and length > 0)
  and (
    .coordinates
    | all(
        type == "object"
        and (.path | type == "string"
             and test("^integrations/ratchet-spring-boot(/[^/]+)*$"))
        and (.artifactId | type == "string"
             and test("^ratchet-spring-boot-[a-z0-9-]+$"))
        and (.packaging == "jar" or .packaging == "pom")
        and (.parentArtifactId | type == "string" and length > 0)
        and (.relativePath | type == "string" and length > 0)
        and (.requiredQualificationScenarios
             | type == "array" and all(type == "string" and length > 0))
        and (.snapshotEligible | type == "boolean")
        and (.centralEligible | type == "boolean")
        and (.bomManaged | type == "boolean")
        and (.releaseInventory | type == "boolean")
      )
  )
  and ((.coordinates | map(.path) | unique | length) == (.coordinates | length))
  and ((.coordinates | map(.artifactId) | unique | length) == (.coordinates | length))
  and (.releaseReady == false)
  and (
    .coordinates
    | all(
        (.snapshotEligible | not)
        and (.centralEligible | not)
        and (.bomManaged | not)
        and (.releaseInventory | not)
      )
  )
' "$TOPOLOGY" >/dev/null; then
  fail "publication-topology.json is malformed or enables the PR 1 publication gate"
fi

if [[ -e "$ATTESTATION" && ! -f "$ATTESTATION" ]]; then
  fail "qualification attestation is not a regular file: ${ATTESTATION#"$ROOT/"}"
fi
if [[ -f "$ATTESTATION" ]]; then
  if ! jq -e '
    def sha256:
      type == "string" and test("^[0-9a-f]{64}$");
    def nonempty_strings:
      type == "array"
      and length > 0
      and all(type == "string" and length > 0)
      and ((unique | length) == length);

    type == "object"
    and .schemaVersion == 1
    and .algorithm == "SHA-256"
    and (.projectVersion | type == "string" and length > 0)
    and (.commit
         | type == "string"
           and test("^[0-9a-f]{40}([0-9a-f]{24})?$"))
    and (.scenario
         | type == "object"
           and (.name | type == "string" and length > 0)
           and (.sha256 | sha256))
    and (.scenarios | nonempty_strings)
    and (.coordinates
         | type == "array"
           and length > 0
           and all(
             type == "object"
             and (.coordinate
                  | type == "string"
                    and test("^run\\.ratchet:ratchet-spring-boot-[a-z0-9-]+:(jar|pom)$"))
             and (.source == "installed" or .source == "jvm-matrix")
             and (.sha256 | sha256)
             and (.scenarios | nonempty_strings)
           ))
    and ((.coordinates | map(.coordinate) | unique | length)
         == (.coordinates | length))
    and (.runtimeDependencies
         | type == "array"
           and length > 0
           and all(
             type == "object"
             and (.lane | type == "string" and length > 0)
             and (.flavor | type == "string" and length > 0)
             and (.treeSha256 | sha256)
             and (.dependencies
                  | type == "array"
                    and length > 0
                    and all(
                      type == "object"
                      and (.coordinate | type == "string" and length > 0)
                      and (.sha256 | sha256)
                      and (.scopes
                           | type == "array"
                             and length > 0
                             and all(. == "compile" or . == "runtime")
                             and ((unique | length) == length))
                    ))
           ))
    and ((.runtimeDependencies | map([.lane, .flavor]) | unique | length)
         == (.runtimeDependencies | length))
    and (.reports
         | type == "array"
           and length > 0
           and all(
             type == "object"
             and (.path | type == "string" and length > 0)
             and (.lane | type == "string" and length > 0)
             and (.afterCommand | type == "string" and length > 0)
             and (.kind == "surefire" or .kind == "failsafe")
             and (.reportClass | type == "string" and length > 0)
             and (.sha256 | sha256)
           ))
    and ((.reports | map(.path) | unique | length) == (.reports | length))
    and (.conformance
         | type == "array"
           and length > 0
           and all(
             type == "object"
             and (.path | type == "string" and length > 0)
             and (.sha256 | sha256)
           ))
    and ((.conformance | map(.path) | unique | length)
         == (.conformance | length))
    and (.toolchain
         | type == "object"
           and (.javaVersion | type == "string" and length > 0)
           and (.javaSpecificationVersion
                | type == "number" and . >= 1 and floor == .)
           and (.mavenVersion | type == "string" and length > 0)
           and (.consumerJavaRuntime
                | type == "number" and . >= 1 and floor == .)
           and (.consumerJavaRuntime == .javaSpecificationVersion))
  ' "$ATTESTATION" >/dev/null; then
    fail "qualification attestation is malformed"
  fi

  if ! current_commit="$(git -C "$ROOT" rev-parse --verify HEAD 2>/dev/null)"; then
    fail "cannot determine current Git commit for qualification attestation"
  fi
  attested_commit="$(jq -r '.commit' "$ATTESTATION")"
  if [[ "$attested_commit" != "$current_commit" ]]; then
    fail "qualification attestation commit is stale: $attested_commit; expected $current_commit"
  fi

  attested_scenario="$(jq -r '.scenario.name' "$ATTESTATION")"
  scoped_coordinate_count="$(
    jq --arg scenario "$attested_scenario" '
      [
        .coordinates[]
        | select(
            .requiredQualificationScenarios
            | index($scenario) != null
          )
      ]
      | length
    ' "$TOPOLOGY"
  )"
  if [[ "$scoped_coordinate_count" == "0" ]]; then
    fail "qualification attestation scenario is not required by any coordinate: $attested_scenario"
  fi

  while IFS= read -r required_scenario; do
    if ! jq -e --arg scenario "$required_scenario" \
        '.scenarios | index($scenario) != null' "$ATTESTATION" >/dev/null; then
      fail "qualification attestation is missing required scenario: $required_scenario"
    fi
  done < <(
    jq -r --arg scenario "$attested_scenario" '
      [
        .coordinates[]
        | select(
            .requiredQualificationScenarios
            | index($scenario) != null
          )
        | .requiredQualificationScenarios[]
        | select(. == $scenario)
      ]
      | unique[]
    ' "$TOPOLOGY"
  )

  while IFS=$'\t' read -r artifact packaging; do
    coordinate="run.ratchet:$artifact:$packaging"
    evidence_count="$(
      jq --arg coordinate "$coordinate" \
        '[.coordinates[] | select(.coordinate == $coordinate)] | length' \
        "$ATTESTATION"
    )"
    if [[ "$evidence_count" == "0" ]]; then
      fail "qualification attestation is missing coordinate evidence: $coordinate"
    fi
    if [[ "$evidence_count" != "1" ]]; then
      fail "qualification attestation has duplicate coordinate evidence: $coordinate"
    fi
    if ! jq -e \
        --arg coordinate "$coordinate" \
        --arg scenario "$attested_scenario" '
          any(
            .coordinates[];
            .coordinate == $coordinate
            and (.scenarios | index($scenario) != null)
          )
        ' "$ATTESTATION" >/dev/null; then
      fail "qualification attestation coordinate $coordinate does not cover scenario: $attested_scenario"
    fi
  done < <(
    jq -r --arg scenario "$attested_scenario" '
      .coordinates[]
      | select(
          .requiredQualificationScenarios
          | index($scenario) != null
        )
      | [.artifactId, .packaging]
      | @tsv
    ' "$TOPOLOGY"
  )

  skipped_coordinate_count="$(
    jq --arg scenario "$attested_scenario" '
      [
        .coordinates[]
        | select(
            (.requiredQualificationScenarios | length > 0)
            and (
              .requiredQualificationScenarios
              | index($scenario) == null
            )
          )
      ]
      | length
    ' "$TOPOLOGY"
  )"
  if [[ "$skipped_coordinate_count" != "0" ]]; then
    echo "spring-boot publication topology: qualification attestation for $attested_scenario skips" \
      "$skipped_coordinate_count coordinate(s) scoped to other scenarios"
  fi
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-spring-topology.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

jq -r '.coordinates[].path' "$TOPOLOGY" | LC_ALL=C sort > "$TMP/manifest-paths"
find "$SPRING_ROOT" \
  -type d -name target -prune -o \
  -type f -name pom.xml -print \
  | sed "s#^$ROOT/##" \
  | sed 's#/pom\.xml$##' \
  | LC_ALL=C sort > "$TMP/module-paths"

while IFS= read -r path; do
  [[ -z "$path" ]] \
    || fail "module POM is not declared in publication-topology.json: $path"
done < <(comm -13 "$TMP/manifest-paths" "$TMP/module-paths")

while IFS= read -r path; do
  [[ -z "$path" ]] \
    || fail "publication-topology.json entry has no module POM: $path"
done < <(comm -23 "$TMP/manifest-paths" "$TMP/module-paths")

# POMs in this repository have a deliberately simple Maven shape. This parser
# validates balanced XML tags first, then extracts only the direct structures
# the publication contract owns. It keeps the script self-contained on the
# same jq + Perl toolchain already used by other repository scripts.
parse_xml() {
  local mode="$1"
  local file="$2"

  perl - "$mode" "$file" <<'PERL'
use strict;
use warnings;

my ($mode, $file) = @ARGV;
open my $handle, '<', $file or die "cannot open $file: $!\n";
local $/;
my $xml = <$handle>;
close $handle or die "cannot close $file: $!\n";

sub xml_error {
    my ($message) = @_;
    die "invalid XML in $file: $message\n";
}

my $scan = $xml;
while ($scan =~ /<!--/g) {
    my $start = pos($scan) - 4;
    my $end = index($scan, '-->', pos($scan));
    xml_error('unterminated comment') if $end < 0;
    pos($scan) = $end + 3;
}
while ($scan =~ /<\?/g) {
    my $end = index($scan, '?>', pos($scan));
    xml_error('unterminated processing instruction') if $end < 0;
    pos($scan) = $end + 2;
}
$scan =~ s{<!--.*?-->}{}gs;
$scan =~ s{<\?.*?\?>}{}gs;
$scan =~ s{<!\[CDATA\[.*?\]\]>}{}gs;
$scan =~ s{<!DOCTYPE[^>]*>}{}gs;

my @stack;
my $cursor = 0;
my $root_count = 0;
my $root_name = '';
while ($scan =~ m{<([^<>]*)>}g) {
    my $between = substr($scan, $cursor, $-[0] - $cursor);
    xml_error('stray angle bracket') if $between =~ /[<>]/;
    xml_error('text outside the root element') if !@stack && $between =~ /\S/;
    my $token = $1;
    $cursor = $+[0];
    next if $token =~ /^\s*!/;
    if ($token =~ m{^\s*/\s*([A-Za-z_][A-Za-z0-9_.:-]*)\s*$}) {
        my $closing = $1;
        xml_error("unexpected closing tag $closing") unless @stack;
        my $opening = pop @stack;
        xml_error("closing tag $closing does not match $opening")
            unless $closing eq $opening;
    } elsif ($token =~ m{/\s*$}) {
        xml_error('invalid self-closing tag')
            unless $token =~ /^\s*[A-Za-z_][A-Za-z0-9_.:-]*/;
        if (!@stack) {
            $root_count++;
            ($root_name) = $token =~ /^\s*([A-Za-z_][A-Za-z0-9_.:-]*)/;
        }
    } elsif ($token =~ /^\s*([A-Za-z_][A-Za-z0-9_.:-]*)/) {
        if (!@stack) {
            $root_count++;
            $root_name = $1;
        }
        push @stack, $1;
    } else {
        xml_error('invalid tag');
    }
}
my $trailing = substr($scan, $cursor);
xml_error('stray angle bracket') if $trailing =~ /[<>]/;
xml_error('text outside the root element') if $trailing =~ /\S/;
xml_error('unclosed tag ' . $stack[-1]) if @stack;
xml_error("expected one root element, found $root_count") if $root_count != 1;
xml_error("root element is $root_name, expected project") if $root_name ne 'project';
my $entity_scan = $scan;
$entity_scan =~ s/&(?:amp|lt|gt|apos|quot|#[0-9]+|#x[0-9A-Fa-f]+);//g;
xml_error('invalid entity reference') if $entity_scan =~ /&/;

$xml =~ s{<!--.*?-->}{}gs;
$xml =~ s{<\?.*?\?>}{}gs;
my ($project) = $xml =~ m{<project\b[^>]*>(.*)</project>}s;
xml_error('missing project element') unless defined $project;

sub blocks {
    my ($body, $name) = @_;
    return ($body =~ m{<$name\b[^>]*>(.*?)</$name>}gs);
}

sub value {
    my ($body, $name) = @_;
    my @values = blocks($body, $name);
    return '' unless @values;
    my $result = $values[0];
    $result =~ s/^\s+|\s+$//g;
    xml_error("$name contains nested XML") if $result =~ /[<>]/;
    return $result;
}

sub dependency_lines {
    my ($body) = @_;
    my @management = blocks($body, 'dependencyManagement');
    return () unless @management;
    xml_error('multiple dependencyManagement elements') if @management != 1;
    my @containers = blocks($management[0], 'dependencies');
    xml_error('dependencyManagement has no dependencies element')
        unless @containers;
    xml_error('multiple dependencyManagement dependencies elements')
        if @containers != 1;

    my @lines;
    for my $dependency (blocks($containers[0], 'dependency')) {
        my $group = value($dependency, 'groupId');
        my $artifact = value($dependency, 'artifactId');
        my $version = value($dependency, 'version');
        xml_error('managed dependency has no groupId') unless length $group;
        xml_error('managed dependency has no artifactId') unless length $artifact;
        xml_error("managed dependency $artifact has no version") unless length $version;
        push @lines, join("\t", $group, $artifact, $version);
    }
    return @lines;
}

if ($mode eq 'pom') {
    my @parents = blocks($project, 'parent');
    xml_error('expected exactly one parent element') if @parents != 1;
    my $direct = $project;
    $direct =~ s{<parent\b[^>]*>.*?</parent>}{}s;
    for my $nested (qw(
        modules
        dependencyManagement
        dependencies
        build
        reporting
        profiles
    )) {
        $direct =~ s{<$nested\b[^>]*>.*?</$nested>}{}gs;
    }

    my $artifact = value($direct, 'artifactId');
    my $packaging = value($direct, 'packaging') || 'jar';
    my $parent = value($parents[0], 'artifactId');
    my $relative = value($parents[0], 'relativePath');
    my @properties = blocks($direct, 'properties');
    xml_error('expected exactly one properties element') if @properties != 1;
    my $deploy_skip = value($properties[0], 'maven.deploy.skip') || '__ABSENT__';
    my $profile_deploy_safe = 'true';
    for my $profile (blocks($project, 'profile')) {
        for my $profile_properties (blocks($profile, 'properties')) {
            for my $profile_deploy_skip (
                    blocks($profile_properties, 'maven.deploy.skip')) {
                $profile_deploy_skip =~ s/^\s+|\s+$//g;
                xml_error('profile maven.deploy.skip contains nested XML')
                    if $profile_deploy_skip =~ /[<>]/;
                $profile_deploy_safe = 'false'
                    unless $profile_deploy_skip eq 'true';
            }
        }
    }

    xml_error('project has no artifactId') unless length $artifact;
    xml_error('parent has no artifactId') unless length $parent;
    xml_error('parent has no relativePath') unless length $relative;
    print join(
        "\t",
        $artifact,
        $packaging,
        $parent,
        $relative,
        $deploy_skip,
        $profile_deploy_safe
    ), "\n";
} elsif ($mode eq 'managed') {
    print "$_\n" for dependency_lines($project);
} elsif ($mode eq 'central') {
    my @central_profiles;
    for my $profile (blocks($project, 'profile')) {
        push @central_profiles, $profile if value($profile, 'id') eq 'central';
    }
    xml_error('expected exactly one central profile') if @central_profiles != 1;

    my @central_plugins;
    for my $plugin (blocks($central_profiles[0], 'plugin')) {
        my $group = value($plugin, 'groupId');
        my $artifact = value($plugin, 'artifactId');
        if ($group eq 'org.sonatype.central'
                && $artifact eq 'central-publishing-maven-plugin') {
            push @central_plugins, $plugin;
        }
    }
    xml_error('expected exactly one Central publishing plugin')
        if @central_plugins != 1;
    my @exclude_containers = blocks($central_plugins[0], 'excludeArtifacts');
    xml_error('expected exactly one excludeArtifacts element')
        if @exclude_containers != 1;
    for my $excluded (blocks($exclude_containers[0], 'excludeArtifact')) {
        $excluded =~ s/^\s+|\s+$//g;
        xml_error('empty excludeArtifact element') unless length $excluded;
        xml_error('excludeArtifact contains nested XML') if $excluded =~ /[<>]/;
        print "$excluded\n";
    }
} else {
    die "unknown parser mode: $mode\n";
}
PERL
}

if ! parse_xml central "$ROOT_POM" > "$TMP/central-exclusions"; then
  fail "could not parse Central exclusions from pom.xml"
fi
if ! parse_xml managed "$BOM_POM" > "$TMP/bom-managed"; then
  fail "could not parse dependency management from ratchet-bom/pom.xml"
fi
if ! parse_xml managed "$SPRING_ROOT/pom.xml" > "$TMP/spring-managed"; then
  fail "could not parse dependency management from integrations/ratchet-spring-boot/pom.xml"
fi

if ! awk '
  /^[[:space:]]*JAR_PATHS=""[[:space:]]*$/ {
    if (started) {
      exit 2
    }
    started = 1
  }
  started {
    print
  }
  started && /^[[:space:]]*for entry in \$JAR_PATHS; do[[:space:]]*$/ {
    finished = 1
    exit
  }
  END {
    if (!started || !finished) {
      exit 3
    }
  }
' "$RELEASE_WORKFLOW" > "$TMP/release-inventory"; then
  fail "could not parse the JAR_PATHS release inventory from .github/workflows/release.yml"
fi

count_line() {
  local expected="$1"
  local file="$2"
  local count
  count="$(grep -Fxc "$expected" "$file" || true)"
  printf '%s' "$count"
}

jq -r \
  '.coordinates[] | select(.centralEligible | not) | .artifactId' \
  "$TOPOLOGY" | LC_ALL=C sort > "$TMP/expected-central-exclusions"
awk '/^ratchet-spring-boot-/' "$TMP/central-exclusions" \
  | LC_ALL=C sort > "$TMP/actual-central-exclusions"

if duplicate="$(LC_ALL=C sort "$TMP/central-exclusions" | uniq -d | head -1)" \
    && [[ -n "$duplicate" ]]; then
  fail "duplicate Central exclusion: $duplicate"
fi
while IFS= read -r artifact; do
  [[ -z "$artifact" ]] \
    || fail "unexpected Spring Central exclusion not declared by publication-topology.json: $artifact"
done < <(
  comm -13 "$TMP/expected-central-exclusions" "$TMP/actual-central-exclusions"
)

jq -r \
  '.coordinates[] | select(.bomManaged) | .artifactId' \
  "$TOPOLOGY" | LC_ALL=C sort > "$TMP/expected-spring-bom"
awk -F '\t' \
  '$1 == "run.ratchet" && $2 ~ /^ratchet-spring-boot-/ { print $2 }' \
  "$TMP/bom-managed" | LC_ALL=C sort > "$TMP/actual-spring-bom"
while IFS= read -r artifact; do
  [[ -z "$artifact" ]] \
    || fail "unexpected Spring artifact in ratchet-bom: $artifact"
done < <(comm -13 "$TMP/expected-spring-bom" "$TMP/actual-spring-bom")

jq -r \
  '.coordinates[] | select(.releaseInventory) | .artifactId' \
  "$TOPOLOGY" | LC_ALL=C sort > "$TMP/expected-release-inventory"
perl -ne \
  'while (/(ratchet-spring-boot-[a-z0-9-]+)/g) { print "$1\n" }' \
  "$RELEASE_WORKFLOW" \
  | LC_ALL=C sort -u > "$TMP/actual-release-inventory"
while IFS= read -r artifact; do
  [[ -z "$artifact" ]] \
    || fail "unexpected Spring artifact in release inventory: $artifact"
done < <(
  comm -13 "$TMP/expected-release-inventory" "$TMP/actual-release-inventory"
)
if grep -Fq "ratchet-spring-boot" "$RELEASE_WORKFLOW"; then
  fail "release workflow references the Spring Boot tree while releaseInventory=false"
fi

while IFS=$'\t' read -r path artifact packaging parent relative_path \
    snapshot_eligible central_eligible bom_managed release_inventory; do
  pom="$ROOT/$path/pom.xml"
  if ! record="$(parse_xml pom "$pom")"; then
    fail "could not parse module POM: $path/pom.xml"
  fi
  IFS=$'\t' read -r actual_artifact actual_packaging actual_parent \
    actual_relative_path deploy_skip profile_deploy_safe <<<"$record"

  [[ "$actual_artifact" == "$artifact" ]] \
    || fail "$path/pom.xml artifactId is '$actual_artifact'; expected '$artifact'"
  [[ "$actual_packaging" == "$packaging" ]] \
    || fail "$path/pom.xml packaging is '$actual_packaging'; expected '$packaging'"
  [[ "$actual_parent" == "$parent" ]] \
    || fail "$path/pom.xml parent artifactId is '$actual_parent'; expected '$parent'"
  [[ "$actual_relative_path" == "$relative_path" ]] \
    || fail "$path/pom.xml parent relativePath is '$actual_relative_path'; expected '$relative_path'"
  [[ "$profile_deploy_safe" == "true" ]] \
    || fail "$path/pom.xml must not override maven.deploy.skip away from true in a profile"

  if [[ "$snapshot_eligible" == "false" ]]; then
    [[ "$deploy_skip" == "true" ]] \
      || fail "$path/pom.xml must declare maven.deploy.skip=true while snapshotEligible=false"
  elif [[ "$deploy_skip" == "true" ]]; then
    fail "$path/pom.xml still declares maven.deploy.skip=true while snapshotEligible=true"
  fi

  exclusion_count="$(count_line "$artifact" "$TMP/central-exclusions")"
  if [[ "$central_eligible" == "false" ]]; then
    [[ "$exclusion_count" == "1" ]] \
      || fail "$artifact must have exactly one Central exclusion; found $exclusion_count"
  elif [[ "$exclusion_count" != "0" ]]; then
    fail "$artifact is centralEligible but remains Central-excluded"
  fi

  bom_count="$(
    awk -F '\t' -v artifact="$artifact" \
      '$1 == "run.ratchet" && $2 == artifact { count++ } END { print count + 0 }' \
      "$TMP/bom-managed"
  )"
  if [[ "$bom_managed" == "false" ]]; then
    [[ "$bom_count" == "0" ]] \
      || fail "$artifact is present in ratchet-bom while bomManaged=false"
  elif [[ "$bom_count" != "1" ]]; then
    fail "$artifact must have exactly one ratchet-bom entry while bomManaged=true; found $bom_count"
  fi

  if [[ "$(count_line "$artifact" "$TMP/actual-release-inventory")" == "1" ]]; then
    in_release_inventory="true"
  else
    in_release_inventory="false"
  fi
  if [[ "$release_inventory" != "$in_release_inventory" ]]; then
    fail "$artifact release inventory presence is $in_release_inventory; expected $release_inventory"
  fi
done < <(
  jq -r '
    .coordinates[]
    | [
        .path,
        .artifactId,
        .packaging,
        .parentArtifactId,
        .relativePath,
        (.snapshotEligible | tostring),
        (.centralEligible | tostring),
        (.bomManaged | tostring),
        (.releaseInventory | tostring)
      ]
    | @tsv
  ' "$TOPOLOGY"
)

managed_siblings=(
  ratchet-spring-boot-autoconfigure
  ratchet-spring-boot-autoconfigure-jpa
  ratchet-spring-boot-starter
  ratchet-spring-boot-autoconfigure-mongodb
  ratchet-spring-boot-starter-mongodb
  ratchet-spring-boot-aot-spring7
)

spring_managed_count="$(
  awk -F '\t' \
    '$1 == "run.ratchet" && $2 ~ /^ratchet-spring-boot-/ { count++ }
     END { print count + 0 }' \
    "$TMP/spring-managed"
)"
[[ "$spring_managed_count" == "${#managed_siblings[@]}" ]] \
  || fail "Spring parent must manage exactly ${#managed_siblings[@]} Spring siblings; found $spring_managed_count"

for artifact in "${managed_siblings[@]}"; do
  managed_count="$(
    awk -F '\t' -v artifact="$artifact" \
      '$1 == "run.ratchet" && $2 == artifact && $3 == "${project.version}" { count++ }
       END { print count + 0 }' \
      "$TMP/spring-managed"
  )"
  [[ "$managed_count" == "1" ]] \
    || fail "Spring parent must manage $artifact exactly once at \${project.version}; found $managed_count"
done

echo "Spring Boot publication topology checks passed"
