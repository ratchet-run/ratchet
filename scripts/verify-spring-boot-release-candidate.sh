#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "spring-boot release candidate: $*" >&2
  exit 1
}

log() {
  echo "spring-boot release candidate: $*"
}

for command in git jq mvn python3 gpg; do
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

[[ -n "$MAVEN_REPO_ARGUMENT" ]] || fail "maven.repo.local is required"
repo_property="-Dmaven.repo.local=$MAVEN_REPO_ARGUMENT"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPRING_ROOT="$ROOT/integrations/ratchet-spring-boot"
EVIDENCE_OUTPUT="$SPRING_ROOT/integration-tests/target/release-candidate-evidence"
GPG_HOME=""
STAGING_REPO=""
PROJECT_ROOT=""

cleanup() {
  local directory
  if [[ -n "$GPG_HOME" && -d "$GPG_HOME" ]] \
      && command -v gpgconf >/dev/null 2>&1; then
    GNUPGHOME="$GPG_HOME" gpgconf --homedir "$GPG_HOME" \
      --kill gpg-agent >/dev/null 2>&1 || true
  fi
  for directory in "$PROJECT_ROOT" "$STAGING_REPO" "$GPG_HOME"; do
    if [[ -n "$directory" && -d "$directory" ]]; then
      rm -rf -- "$directory"
    fi
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

GPG_HOME="$(mktemp -d "/tmp/ratchet-spring-boot-rc-gpg.XXXXXX")"
STAGING_REPO="$(mktemp -d "/tmp/ratchet-spring-boot-rc-staging.XXXXXX")"
PROJECT_ROOT="$(mktemp -d "/tmp/ratchet-spring-boot-rc-projects.XXXXXX")"
case "$PROJECT_ROOT/" in
  "$ROOT/"*) fail "fresh-project temporary directory must be outside the reactor: $PROJECT_ROOT" ;;
esac
chmod 700 "$GPG_HOME"
export GNUPGHOME="$GPG_HOME"

project_version="$(
  python3 - "$ROOT/pom.xml" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET

path = pathlib.Path(sys.argv[1])
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
try:
    root = ET.parse(path).getroot()
except (OSError, ET.ParseError) as exc:
    raise SystemExit(f"cannot parse reactor POM {path}: {exc}")
version = root.findtext("m:version", namespaces=namespace)
if not version:
    raise SystemExit(f"reactor POM has no project version: {path}")
print(version)
PY
)" || fail "could not determine the reactor version"
[[ -n "$project_version" ]] || fail "reactor version must not be empty"
log "staged reactor version: $project_version"

log "generating an ephemeral signing key in a temporary GNUPGHOME"
gpg --batch \
  --pinentry-mode loopback \
  --passphrase "" \
  --quick-generate-key \
  "Ratchet Release Candidate Verifier <release-candidate@ratchet.run>" \
  rsa2048 sign 0 >/dev/null 2>&1 \
  || fail "could not generate the ephemeral GPG key"

ephemeral_key="$(
  gpg --batch --with-colons --list-secret-keys \
    "Ratchet Release Candidate Verifier" 2>/dev/null \
    | awk -F: '$1 == "fpr" { print $10; exit }'
)"
[[ "$ephemeral_key" =~ ^[0-9A-Fa-f]{40}$ ]] \
  || fail "could not determine the ephemeral GPG key fingerprint"
gpg_executable="$(command -v gpg)"

log "deploying the complete reactor to temporary file staging (never Central)"
mvn -B -ntp \
  "$repo_property" \
  -DskipTests \
  -Dspotbugs.skip=true \
  -Dmaven.deploy.skip=false \
  "-Dgpg.executable=$gpg_executable" \
  "-Dgpg.homedir=$GPG_HOME" \
  "-Dgpg.keyname=$ephemeral_key" \
  -Dgpg.passphrase= \
  -Dgpg.signer=gpg \
  deploy \
  -P "rc-staging,sign,!central" \
  "-DaltDeploymentRepository=rc-staging::file://$STAGING_REPO" \
  || fail "temporary release-candidate staging deploy failed"

shopt -s nullglob

verify_staged_file() {
  local artifact_file="$1"
  local label="$2"
  local signature="${artifact_file}.asc"
  local checksum

  [[ -s "$artifact_file" ]] || fail "missing or empty staged $label: $artifact_file"
  [[ -s "$signature" ]] || fail "missing or empty signature for staged $label: $signature"
  gpg --batch --verify "$signature" "$artifact_file" >/dev/null 2>&1 \
    || fail "GPG verification failed for staged $label: $artifact_file"

  for checksum in md5 sha1; do
    [[ -s "${artifact_file}.${checksum}" ]] \
      || fail "missing or empty $checksum checksum for staged $label: ${artifact_file}.${checksum}"
    [[ -s "${signature}.${checksum}" ]] \
      || fail "missing or empty $checksum checksum for staged $label signature: ${signature}.${checksum}"
  done
}

verify_staged_module() {
  local artifact="$1"
  local packaging="$2"
  local module_path="$3"
  local artifact_dir="$STAGING_REPO/run/ratchet/$artifact/$project_version"
  local candidates=()
  local all_jars=()
  local main_jars=()
  local file
  local base
  local sbom

  [[ -d "$artifact_dir" ]] \
    || fail "staged coordinate directory is missing for run.ratchet:$artifact:$project_version"

  candidates=( "$artifact_dir/$artifact-"*.pom )
  [[ ${#candidates[@]} -eq 1 ]] \
    || fail "expected exactly one staged POM for $artifact; found ${#candidates[@]}"
  verify_staged_file "${candidates[0]}" "$artifact POM"

  if [[ "$packaging" == "jar" ]]; then
    all_jars=( "$artifact_dir/$artifact-"*.jar )
    for file in "${all_jars[@]}"; do
      base="${file##*/}"
      case "$base" in
        *-sources.jar|*-javadoc.jar)
          ;;
        *)
          main_jars+=( "$file" )
          ;;
      esac
    done
    [[ ${#main_jars[@]} -eq 1 ]] \
      || fail "expected exactly one staged main JAR for $artifact; found ${#main_jars[@]}"
    verify_staged_file "${main_jars[0]}" "$artifact JAR"

    candidates=( "$artifact_dir/$artifact-"*-sources.jar )
    [[ ${#candidates[@]} -eq 1 ]] \
      || fail "expected exactly one staged sources JAR for $artifact; found ${#candidates[@]}"
    verify_staged_file "${candidates[0]}" "$artifact sources JAR"

    candidates=( "$artifact_dir/$artifact-"*-javadoc.jar )
    [[ ${#candidates[@]} -eq 1 ]] \
      || fail "expected exactly one staged javadoc JAR for $artifact; found ${#candidates[@]}"
    verify_staged_file "${candidates[0]}" "$artifact javadoc JAR"
  fi

  for sbom in bom.json bom.xml; do
    [[ -s "$ROOT/$module_path/target/$sbom" ]] \
      || fail "missing or empty $artifact CycloneDX SBOM: $module_path/target/$sbom"
  done
}

log "checking staged artifacts, signatures, checksums, and module SBOMs"
while IFS='|' read -r artifact packaging module_path; do
  verify_staged_module "$artifact" "$packaging" "$module_path"
done <<'MODULES'
ratchet-spring-boot-parent|pom|integrations/ratchet-spring-boot
ratchet-spring-boot-autoconfigure|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure
ratchet-spring-boot-autoconfigure-jpa|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa
ratchet-spring-boot-starter|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-starter
ratchet-spring-boot-autoconfigure-mongodb|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-mongodb
ratchet-spring-boot-starter-mongodb|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb
ratchet-spring-boot-aot-spring7|jar|integrations/ratchet-spring-boot/ratchet-spring-boot-aot-spring7
MODULES

log "checking sync-version idempotency at $project_version"
SYNC_FILES=()
while IFS= read -r file; do
  SYNC_FILES+=( "$file" )
done < <(
  git -C "$ROOT" ls-files \
    README.md \
    integrations/ratchet-quarkus/README.md \
    website/docs \
    infra/loadtest/Dockerfile \
    .github/ISSUE_TEMPLATE/bug_report.yml
)
[[ ${#SYNC_FILES[@]} -gt 0 ]] || fail "could not identify sync-version managed files"

sync_backup="$PROJECT_ROOT/sync-version-backup"
for file in "${SYNC_FILES[@]}"; do
  mkdir -p "$sync_backup/$(dirname "$file")"
  cp -p "$ROOT/$file" "$sync_backup/$file"
done

restore_sync_files() {
  local file
  for file in "${SYNC_FILES[@]}"; do
    cp -p "$sync_backup/$file" "$ROOT/$file"
  done
}

sync_status=0
env -u RELEASE_VERSION bash "$ROOT/scripts/sync-version.sh" "$project_version" \
  || sync_status=$?
if [[ "$sync_status" -ne 0 ]]; then
  restore_sync_files
  fail "scripts/sync-version.sh failed at the current version"
fi
if ! git -C "$ROOT" diff --exit-code -- "${SYNC_FILES[@]}"; then
  restore_sync_files
  fail "scripts/sync-version.sh is not idempotent at $project_version; restored touched files"
fi

log "checking six Spring JAR modules for third-party runtime SNAPSHOT dependencies"
DEPENDENCY_LISTS=()
while IFS='|' read -r artifact module_path; do
  dependency_list="$PROJECT_ROOT/$artifact-runtime-dependencies.txt"
  mvn -B -ntp \
    "$repo_property" \
    -f "$ROOT/$module_path/pom.xml" \
    -DskipTests \
    -Dspotbugs.skip=true \
    -DincludeScope=runtime \
    -DexcludeReactor=false \
    "-DoutputFile=$dependency_list" \
    -DappendOutput=false \
    dependency:list \
    || fail "could not resolve runtime dependencies for $artifact"
  [[ -s "$dependency_list" ]] \
    || fail "runtime dependency list is empty for $artifact"
  DEPENDENCY_LISTS+=( "$dependency_list" )
done <<'MODULES'
ratchet-spring-boot-autoconfigure|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure
ratchet-spring-boot-autoconfigure-jpa|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa
ratchet-spring-boot-starter|integrations/ratchet-spring-boot/ratchet-spring-boot-starter
ratchet-spring-boot-autoconfigure-mongodb|integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-mongodb
ratchet-spring-boot-starter-mongodb|integrations/ratchet-spring-boot/ratchet-spring-boot-starter-mongodb
ratchet-spring-boot-aot-spring7|integrations/ratchet-spring-boot/ratchet-spring-boot-aot-spring7
MODULES

python3 - "$project_version" "${DEPENDENCY_LISTS[@]}" <<'PY' \
  || fail "runtime dependency SNAPSHOT hygiene failed"
import pathlib
import sys

project_version = sys.argv[1]
for raw_path in sys.argv[2:]:
    path = pathlib.Path(raw_path)
    resolved = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip().partition(" -- ")[0].strip()
        optional_suffix = " (optional)"
        if line.endswith(optional_suffix):
            line = line[:-len(optional_suffix)]
        parts = line.split(":")
        if len(parts) < 5 or parts[-1] not in {
            "compile", "provided", "runtime", "system", "test"
        }:
            continue
        group = parts[0]
        artifact = parts[1]
        version = parts[-2]
        resolved += 1
        if version.endswith("-SNAPSHOT") and not (
            group == "run.ratchet" and version == project_version
        ):
            raise SystemExit(
                f"{path.name}: disallowed runtime SNAPSHOT dependency "
                f"{group}:{artifact}:{version}"
            )
    if resolved == 0:
        raise SystemExit(f"{path.name}: no resolved dependencies were parsed")
PY

log "validating and materializing five-flavor conformance evidence"
[[ -n "${RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR:-}" ]] \
  || fail "RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR must be set"
EVIDENCE_SOURCE="$(
  python3 - "$RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR" "$EVIDENCE_OUTPUT" <<'PY'
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
source = source.resolve()
output = output.resolve()
if source == output or source in output.parents or output in source.parents:
    raise SystemExit(
        "RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR must be separate from the "
        f"materialized evidence path: {source}"
    )
print(source)
PY
)" || fail "invalid RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR"
[[ -d "$EVIDENCE_SOURCE" ]] \
  || fail "RATCHET_SPRING_BOOT_RC_EVIDENCE_DIR is not a directory: $EVIDENCE_SOURCE"

EVIDENCE_NAMES=(
  spring-boot-postgresql-conformance
  spring-boot-mongodb-conformance
  spring-boot-mysql-conformance
  spring-boot-oracle-conformance
  spring-boot-sqlserver-conformance
)

mkdir -p "$EVIDENCE_OUTPUT"
for evidence_name in "${EVIDENCE_NAMES[@]}"; do
  source_dir="$EVIDENCE_SOURCE/$evidence_name"
  destination_dir="$EVIDENCE_OUTPUT/$evidence_name"
  [[ -d "$source_dir" ]] \
    || fail "missing conformance evidence directory for $evidence_name: $source_dir"
  [[ -s "$source_dir/qualification-attestation.json" ]] \
    || fail "missing or empty qualification attestation for $evidence_name"
  if [[ -z "$(find "$source_dir" -type f ! -name qualification-attestation.json -size +0c -print -quit)" ]]; then
    fail "conformance report tree is empty for $evidence_name"
  fi
  rm -rf -- "$destination_dir"
  mkdir -p "$destination_dir"
  cp -R "$source_dir"/. "$destination_dir"/
done

current_commit="$(git -C "$ROOT" rev-parse --verify HEAD)" \
  || fail "could not determine the current Git commit"
python3 - "$EVIDENCE_OUTPUT" "$current_commit" <<'PY' \
  || fail "five-flavor conformance evidence validation failed"
import hashlib
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
commit = sys.argv[2]
expected = {
    "spring-boot-postgresql-conformance": "postgresql-runtime",
    "spring-boot-mongodb-conformance": "mongodb-runtime",
    "spring-boot-mysql-conformance": "dialect-mysql",
    "spring-boot-oracle-conformance": "dialect-oracle",
    "spring-boot-sqlserver-conformance": "dialect-sqlserver",
}
expected_flavors = {
    "spring-boot-postgresql-conformance": "postgresql",
    "spring-boot-mongodb-conformance": "mongodb",
    "spring-boot-mysql-conformance": "mysql",
    "spring-boot-oracle-conformance": "oracle",
    "spring-boot-sqlserver-conformance": "sqlserver",
}
sql_names = [
    "spring-boot-postgresql-conformance",
    "spring-boot-mysql-conformance",
    "spring-boot-oracle-conformance",
    "spring-boot-sqlserver-conformance",
]
coordinate_maps = {}


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def find_conformance_file(flavor_root, declared_path, flavor):
    declared = pathlib.PurePosixPath(declared_path)
    candidates = []
    direct = flavor_root.joinpath(*declared.parts)
    if direct.is_file():
        candidates.append(direct)
    target_prefix = pathlib.PurePosixPath(
        "integrations/ratchet-spring-boot/integration-tests/target"
    )
    try:
        relative = declared.relative_to(target_prefix)
    except ValueError:
        relative = None
    if relative is not None:
        stripped = flavor_root.joinpath(*relative.parts)
        if stripped.is_file() and stripped not in candidates:
            candidates.append(stripped)
    suffix = pathlib.PurePosixPath(*declared.parts[-2:])
    # Artifact uploads may retain more leading directories, so accept the
    # declared path's unambiguous final two-component suffix.
    for candidate in flavor_root.rglob(declared.name):
        relative_candidate = pathlib.PurePosixPath(
            candidate.relative_to(flavor_root).as_posix()
        )
        if tuple(relative_candidate.parts[-2:]) == tuple(suffix.parts):
            candidates.append(candidate)
    unique = list(dict.fromkeys(candidates))
    if len(unique) != 1:
        raise SystemExit(
            f"{flavor}: conformance file {declared_path!r} resolved to "
            f"{len(unique)} files"
        )
    return unique[0]


for name, scenario_name in expected.items():
    flavor_root = root / name
    attestation_path = flavor_root / "qualification-attestation.json"
    try:
        with attestation_path.open(encoding="utf-8") as stream:
            attestation = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{name}: cannot parse qualification attestation: {exc}")
    if not isinstance(attestation, dict):
        raise SystemExit(f"{name}: qualification attestation must be an object")
    if attestation.get("schemaVersion") != 1:
        raise SystemExit(f"{name}: schemaVersion must be 1")
    if attestation.get("commit") != commit:
        raise SystemExit(
            f"{name}: attestation commit {attestation.get('commit')!r} "
            f"does not match HEAD {commit}"
        )
    scenario = attestation.get("scenario")
    if not isinstance(scenario, dict) or scenario.get("name") != scenario_name:
        raise SystemExit(
            f"{name}: expected scenario.name {scenario_name!r}"
        )

    coordinates = attestation.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        raise SystemExit(f"{name}: coordinates must be a non-empty array")
    coordinate_map = {}
    for index, entry in enumerate(coordinates):
        if not isinstance(entry, dict):
            raise SystemExit(f"{name}: coordinates[{index}] must be an object")
        coordinate = entry.get("coordinate")
        sha256 = entry.get("sha256")
        if (
            not isinstance(coordinate, str)
            or not coordinate.startswith("run.ratchet:")
            or not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise SystemExit(
                f"{name}: coordinates[{index}] has an invalid coordinate or sha256"
            )
        if coordinate in coordinate_map:
            raise SystemExit(f"{name}: duplicate coordinate {coordinate}")
        coordinate_map[coordinate] = sha256
    coordinate_maps[name] = coordinate_map

    runtime_dependencies = attestation.get("runtimeDependencies")
    if not isinstance(runtime_dependencies, list) or not runtime_dependencies:
        raise SystemExit(f"{name}: runtimeDependencies must be a non-empty array")
    runtime_lanes = set()
    for index, entry in enumerate(runtime_dependencies):
        if not isinstance(entry, dict):
            raise SystemExit(f"{name}: runtimeDependencies[{index}] must be an object")
        lane = entry.get("lane")
        flavor = entry.get("flavor")
        tree_sha256 = entry.get("treeSha256")
        if lane not in {"boot-3.5", "boot-4.1"}:
            raise SystemExit(f"{name}: runtimeDependencies[{index}].lane is invalid")
        if lane in runtime_lanes:
            raise SystemExit(f"{name}: duplicate runtime dependency lane {lane}")
        runtime_lanes.add(lane)
        if flavor != expected_flavors[name]:
            raise SystemExit(
                f"{name}: runtimeDependencies[{index}].flavor must be "
                f"{expected_flavors[name]!r}"
            )
        if not isinstance(tree_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", tree_sha256
        ) is None:
            raise SystemExit(
                f"{name}: runtimeDependencies[{index}].treeSha256 is invalid"
            )
        dependencies = entry.get("dependencies")
        if not isinstance(dependencies, list) or not dependencies:
            raise SystemExit(
                f"{name}: runtimeDependencies[{index}].dependencies "
                "must be a non-empty array"
            )
        seen_dependencies = set()
        for dependency_index, dependency in enumerate(dependencies):
            if not isinstance(dependency, dict):
                raise SystemExit(
                    f"{name}: runtimeDependencies[{index}].dependencies"
                    f"[{dependency_index}] must be an object"
                )
            coordinate = dependency.get("coordinate")
            sha256 = dependency.get("sha256")
            scopes = dependency.get("scopes")
            if not isinstance(coordinate, str) or not coordinate:
                raise SystemExit(
                    f"{name}: runtime dependency coordinate is invalid"
                )
            if coordinate in seen_dependencies:
                raise SystemExit(
                    f"{name}: duplicate runtime dependency {coordinate} in {lane}"
                )
            seen_dependencies.add(coordinate)
            if not isinstance(sha256, str) or re.fullmatch(
                r"[0-9a-f]{64}", sha256
            ) is None:
                raise SystemExit(
                    f"{name}: runtime dependency {coordinate} sha256 is invalid"
                )
            if (
                not isinstance(scopes, list)
                or not scopes
                or not all(isinstance(scope, str) and scope for scope in scopes)
            ):
                raise SystemExit(
                    f"{name}: runtime dependency {coordinate} scopes are invalid"
                )
    if runtime_lanes != {"boot-3.5", "boot-4.1"}:
        raise SystemExit(
            f"{name}: runtimeDependencies must cover boot-3.5 and boot-4.1"
        )

    conformance = attestation.get("conformance")
    if not isinstance(conformance, list) or not conformance:
        raise SystemExit(f"{name}: conformance must be a non-empty array")
    for index, entry in enumerate(conformance):
        if not isinstance(entry, dict):
            raise SystemExit(f"{name}: conformance[{index}] must be an object")
        declared_path = entry.get("path")
        expected_sha256 = entry.get("sha256")
        if not isinstance(declared_path, str) or not declared_path:
            raise SystemExit(f"{name}: conformance[{index}].path is invalid")
        if not isinstance(expected_sha256, str) or re.fullmatch(
            r"[0-9a-f]{64}", expected_sha256
        ) is None:
            raise SystemExit(f"{name}: conformance[{index}].sha256 is invalid")
        report = find_conformance_file(flavor_root, declared_path, name)
        if report.stat().st_size == 0:
            raise SystemExit(f"{name}: conformance report is empty: {report}")
        actual_sha256 = digest(report)
        if actual_sha256 != expected_sha256:
            raise SystemExit(
                f"{name}: conformance report hash mismatch for {declared_path}"
            )

sql_reference_name = sql_names[0]
sql_reference = coordinate_maps[sql_reference_name]
for name in sql_names[1:]:
    if coordinate_maps[name] != sql_reference:
        missing = sorted(set(sql_reference) - set(coordinate_maps[name]))
        extra = sorted(set(coordinate_maps[name]) - set(sql_reference))
        mismatched = sorted(
            coordinate
            for coordinate in set(sql_reference) & set(coordinate_maps[name])
            if sql_reference[coordinate] != coordinate_maps[name][coordinate]
        )
        raise SystemExit(
            f"{name}: production coordinate hashes differ from "
            f"{sql_reference_name}; missing={missing}, extra={extra}, "
            f"hashMismatch={mismatched}"
        )

seen = {}
seen_in = {}
for name, coordinate_map in coordinate_maps.items():
    for coordinate, sha256 in coordinate_map.items():
        if coordinate in seen and seen[coordinate] != sha256:
            raise SystemExit(
                f"{name}: coordinate {coordinate} hash differs from {seen_in[coordinate]}"
            )
        seen[coordinate] = sha256
        seen_in.setdefault(coordinate, name)
PY

consumer_java_home="${RATCHET_MATRIX_JAVA_HOME:-}"
consumer_java="$consumer_java_home/bin/java"
if [[ -n "$consumer_java_home" ]]; then
  [[ "$consumer_java_home" == /* ]] \
    || fail "RATCHET_MATRIX_JAVA_HOME must be an absolute path: $consumer_java_home"
  [[ -f "$consumer_java" ]] \
    || fail "RATCHET_MATRIX_JAVA_HOME does not contain a java executable: $consumer_java_home"
else
  consumer_java="$(command -v java)"
fi
consumer_java_version="$(
  "$consumer_java" -XshowSettings:properties -version 2>&1 \
    | sed -n 's/^[[:space:]]*java\.specification\.version[[:space:]]*=[[:space:]]*//p'
)"
[[ "$consumer_java_version" == "17" ]] \
  || fail "fresh-project consumer builds require Java 17; got $consumer_java_version"

staging_uri="$(
  python3 - "$STAGING_REPO" <<'PY'
import pathlib
import sys
print(pathlib.Path(sys.argv[1]).resolve().as_uri())
PY
)"

write_consumer_pom() {
  local project="$1"
  local artifact="$2"
  local production_dependencies="$3"
  local testcontainers_artifact="$4"
  cat > "$project/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.16</version>
    <relativePath />
  </parent>
  <groupId>com.example</groupId>
  <artifactId>$artifact</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <properties>
    <java.version>17</java.version>
  </properties>
  <repositories>
    <repository>
      <id>ratchet-rc-staging</id>
      <url>$staging_uri</url>
      <releases>
        <enabled>true</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
        <updatePolicy>always</updatePolicy>
      </snapshots>
    </repository>
    <repository>
      <id>central</id>
      <url>https://repo.maven.apache.org/maven2</url>
    </repository>
  </repositories>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>run.ratchet</groupId>
        <artifactId>ratchet-bom</artifactId>
        <version>$project_version</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-bom</artifactId>
        <version>2.0.5</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
$production_dependencies
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>$testcontainers_artifact</artifactId>
      <version>2.0.5</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
EOF
}

write_application() {
  local project="$1"
  mkdir -p "$project/src/main/java/com/example"
  cat > "$project/src/main/java/com/example/RcApplication.java" <<'EOF'
package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RcApplication {
  public static void main(String[] args) {
    SpringApplication.run(RcApplication.class, args);
  }
}
EOF
}

write_postgresql_test() {
  local project="$1"
  local owned_entity="$2"
  mkdir -p "$project/src/test/java/com/example"
  if [[ "$owned_entity" == "true" ]]; then
    cat > "$project/src/main/java/com/example/ConsumerRecord.java" <<'EOF'
package com.example;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class ConsumerRecord {
  @Id
  private Long id;

  protected ConsumerRecord() {}

  public ConsumerRecord(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }
}
EOF
  fi

  cat > "$project/src/test/java/com/example/RcApplicationSmokeTest.java" <<EOF
package com.example;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import run.ratchet.api.JobSchedulerService;

@SpringBootTest(properties = {
    "ratchet.schema.auto-migrate=true",
    "ratchet.class-policy.allowed-packages=com.example",
    "spring.jpa.hibernate.ddl-auto=none",
    "spring.jpa.open-in-view=false"
})
class RcApplicationSmokeTest {
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:16")
          .withDatabaseName("ratchet_spring_boot")
          .withUsername("ratchet")
          .withPassword("ratchet");

  static {
    POSTGRES.start();
  }

  @DynamicPropertySource
  static void databaseProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
  }

  @Autowired
  JobSchedulerService scheduler;
EOF
  if [[ "$owned_entity" == "true" ]]; then
    cat >> "$project/src/test/java/com/example/RcApplicationSmokeTest.java" <<'EOF'

  @Autowired
  EntityManagerFactory entityManagerFactory;

  @Test
  void contextStartsWithApplicationOwnedEntity() {
    assertNotNull(scheduler);
    assertTrue(
        entityManagerFactory.getMetamodel().getEntities().stream()
            .anyMatch(entity -> entity.getJavaType().equals(ConsumerRecord.class)));
  }
}
EOF
  else
    cat >> "$project/src/test/java/com/example/RcApplicationSmokeTest.java" <<'EOF'

  @Test
  void contextStartsWithoutApplicationEntities() {
    assertNotNull(scheduler);
  }
}
EOF
  fi
}

write_mongodb_test() {
  local project="$1"
  mkdir -p "$project/src/test/java/com/example"
  cat > "$project/src/test/java/com/example/RcApplicationSmokeTest.java" <<'EOF'
package com.example;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.mongodb.MongoDBContainer;
import run.ratchet.api.JobSchedulerService;

@SpringBootTest(properties = {
    "ratchet.mongodb.database=ratchet_spring_boot",
    "ratchet.class-policy.allowed-packages=com.example"
})
class RcApplicationSmokeTest {
  private static final MongoDBContainer MONGODB =
      new MongoDBContainer("mongo:7.0")
          .withReplicaSet()
          .waitingFor(
              Wait.forLogMessage("(?i).*waiting for connections.*", 1)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  static {
    MONGODB.start();
  }

  @DynamicPropertySource
  static void databaseProperties(DynamicPropertyRegistry registry) {
    registry.add("ratchet.mongodb.connection-string", MONGODB::getConnectionString);
  }

  @Autowired
  JobSchedulerService scheduler;

  @Test
  void contextStartsAgainstMongoDb() {
    assertNotNull(scheduler);
  }
}
EOF
}

postgres_dependencies='    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-spring-boot-starter</artifactId>
    </dependency>
    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-store-postgresql</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>'
mongodb_dependencies='    <dependency>
      <groupId>run.ratchet</groupId>
      <artifactId>ratchet-spring-boot-starter-mongodb</artifactId>
    </dependency>'

postgres_owned="$PROJECT_ROOT/postgresql-owned-entity"
postgres_empty="$PROJECT_ROOT/postgresql-no-entity"
mongodb_project="$PROJECT_ROOT/mongodb"
for project in "$postgres_owned" "$postgres_empty" "$mongodb_project"; do
  mkdir -p "$project"
  write_application "$project"
done
write_consumer_pom \
  "$postgres_owned" \
  "ratchet-rc-postgresql-owned-entity" \
  "$postgres_dependencies" \
  "testcontainers-postgresql"
write_consumer_pom \
  "$postgres_empty" \
  "ratchet-rc-postgresql-no-entity" \
  "$postgres_dependencies" \
  "testcontainers-postgresql"
write_consumer_pom \
  "$mongodb_project" \
  "ratchet-rc-mongodb" \
  "$mongodb_dependencies" \
  "testcontainers-mongodb"
write_postgresql_test "$postgres_owned" true
write_postgresql_test "$postgres_empty" false
write_mongodb_test "$mongodb_project"

log "building three fresh Boot 3.5.16 consumer projects under Java 17"
[[ -d "$STAGING_REPO/run/ratchet" ]] \
  || fail "temporary staging repository has no run.ratchet artifacts to seed consumers"
for project in "$postgres_owned" "$postgres_empty" "$mongodb_project"; do
  project_name="${project##*/}"
  scoped_repo="$(mktemp -d "$PROJECT_ROOT/${project_name}-m2.XXXXXX")"
  mkdir -p "$scoped_repo/run"
  cp -R "$STAGING_REPO/run/ratchet" "$scoped_repo/run/"
  (
    cd "$project"
    if [[ -n "$consumer_java_home" ]]; then
      export JAVA_HOME="$consumer_java_home"
      export PATH="$JAVA_HOME/bin:$PATH"
    fi
    export MAVEN_REPO_LOCAL="$scoped_repo"
    mvn -q "-Dmaven.repo.local=$scoped_repo" test
  ) || fail "fresh consumer project failed: $project_name"
done

for project in "$postgres_owned" "$postgres_empty" "$mongodb_project"; do
  if grep -R --include='*.java' "run.ratchet.ri" "$project" >/dev/null 2>&1; then
    fail "fresh consumer project imports a Ratchet internal package: ${project##*/}"
  fi
  if [[ -n "$(find "$project" -type f \( -name '*-hints.json' -o -name 'reachability-metadata.json' -o -path '*/META-INF/native-image/*' \) -print -quit)" ]]; then
    fail "fresh consumer project contains forbidden hints or reachability metadata: ${project##*/}"
  fi
done
if [[ -n "$(find "$postgres_empty" -name persistence.xml -print -quit)" ]]; then
  fail "PostgreSQL no-entity consumer contains persistence.xml"
fi

log "checking staged Maven metadata contains only $project_version"
python3 - "$STAGING_REPO" "$project_version" <<'PY' \
  || fail "staging repository metadata hygiene failed"
import pathlib
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1]) / "run" / "ratchet"
expected = sys.argv[2]
metadata_files = sorted(root.rglob("maven-metadata*.xml"))
if not metadata_files:
    raise SystemExit("no run.ratchet maven-metadata files were staged")
found_versions = []
for path in metadata_files:
    try:
        document = ET.parse(path)
    except (OSError, ET.ParseError) as exc:
        raise SystemExit(f"cannot parse {path}: {exc}")
    for element in document.iter():
        if element.tag.rsplit("}", 1)[-1] == "version" and element.text:
            value = element.text.strip()
            found_versions.append((path, value))
            if value != expected:
                raise SystemExit(
                    f"{path}: metadata version {value!r} does not match {expected!r}"
                )
if not found_versions:
    raise SystemExit("staged maven-metadata contains no version entries")
PY

log "running the publication-topology gate as the final RC verdict"
bash "$ROOT/scripts/test-spring-boot-publication-topology.sh" "$repo_property" \
  || fail "publication-topology gate failed"
log "all release-candidate verification stages passed"
