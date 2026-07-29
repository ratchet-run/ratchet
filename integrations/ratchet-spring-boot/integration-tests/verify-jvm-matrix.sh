#!/usr/bin/env bash
#
# verify-jvm-matrix.sh — build the production jars once, execute every JVM
# consumer lane without reactor assistance, and prove the installed jar hashes
# remain identical across the matrix.
#
# Usage:
#   verify-jvm-matrix.sh
#   verify-jvm-matrix.sh -Dmaven.repo.local=/absolute/isolated/repository

set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [-Dmaven.repo.local=/absolute/isolated/repository]" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MATRIX="$ROOT/integrations/ratchet-spring-boot/integration-tests/compatibility-matrix.json"
for command in java mvn python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

ARGUMENT_REPO=""
if [[ $# -eq 1 ]]; then
  case "$1" in
    -Dmaven.repo.local=*)
      ARGUMENT_REPO="${1#-Dmaven.repo.local=}"
      ;;
    *)
      echo "unsupported argument: $1" >&2
      echo "usage: $0 [-Dmaven.repo.local=/absolute/isolated/repository]" >&2
      exit 2
      ;;
  esac
fi

ENVIRONMENT_REPO="${MAVEN_REPO_LOCAL:-}"
if [[ -n "$ARGUMENT_REPO" && -n "$ENVIRONMENT_REPO" ]]; then
  argument_resolved="$(
    python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve())' \
      "$ARGUMENT_REPO"
  )"
  environment_resolved="$(
    python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve())' \
      "$ENVIRONMENT_REPO"
  )"
  if [[ "$argument_resolved" != "$environment_resolved" ]]; then
    echo "Maven repository argument and MAVEN_REPO_LOCAL disagree:" >&2
    echo "  argument: $argument_resolved" >&2
    echo "  environment: $environment_resolved" >&2
    exit 2
  fi
fi

MAVEN_REPO="${ARGUMENT_REPO:-$ENVIRONMENT_REPO}"
OWN_REPO=false
if [[ -z "$MAVEN_REPO" ]]; then
  MAVEN_REPO="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-spring-boot-matrix-m2.XXXXXX")"
  OWN_REPO=true
fi

cleanup() {
  if [[ "$OWN_REPO" == true ]]; then
    rm -rf "$MAVEN_REPO"
  fi
}
trap cleanup EXIT

MAVEN_REPO="$(
  python3 - "$MAVEN_REPO" <<'PY'
import os
import pathlib
import sys

candidate = pathlib.Path(sys.argv[1])
if not candidate.is_absolute():
    raise SystemExit(
        f"Maven repository must be an absolute path: {candidate}"
    )
if not candidate.is_dir():
    raise SystemExit(f"Maven repository does not exist: {candidate}")

resolved = candidate.resolve()
shared = pathlib.Path(os.path.expanduser("~/.m2/repository")).resolve()
if resolved == shared:
    raise SystemExit(
        f"refusing to use the shared Maven repository: {shared}"
    )
print(resolved)
PY
)"

python3 - "$ROOT" "$MATRIX" "$MAVEN_REPO" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1]).resolve()
matrix_path = pathlib.Path(sys.argv[2]).resolve()
maven_repo = pathlib.Path(sys.argv[3]).resolve()
repo_property = f"-Dmaven.repo.local={maven_repo}"
pom_namespace = {"m": "http://maven.apache.org/POM/4.0.0"}


def fail(message):
    raise SystemExit(f"verify-jvm-matrix.sh: {message}")


def load_json(path):
    if not path.is_file():
        fail(f"missing compatibility matrix: {path}")
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse compatibility matrix {path}: {exc}")
    if not isinstance(value, dict):
        fail("compatibility matrix must contain a JSON object")
    return value


def repo_path(raw_path, label):
    if not isinstance(raw_path, str) or not raw_path:
        fail(f"{label} must be a non-empty repository-relative path")
    candidate = (root / raw_path).resolve()
    if candidate == root or root not in candidate.parents:
        fail(f"{label} escapes the repository: {raw_path}")
    return candidate


def require_string(value, label):
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    return value


def require_list(value, label):
    if not isinstance(value, list):
        fail(f"{label} must be a JSON array")
    return value


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def installed_artifact_path(artifact, version):
    group_path = pathlib.Path(*artifact["groupId"].split("."))
    filename = (
        f"{artifact['artifactId']}-{version}.{artifact['packaging']}"
    )
    return (
        maven_repo
        / group_path
        / artifact["artifactId"]
        / version
        / filename
    )


matrix = load_json(matrix_path)
if matrix.get("schemaVersion") != 1:
    fail("compatibility matrix schemaVersion must be 1")

expected_java = matrix.get("javaRuntime")
if not isinstance(expected_java, int) or isinstance(expected_java, bool):
    fail("compatibility matrix javaRuntime must be an integer")
java_result = subprocess.run(
    ["java", "-XshowSettings:properties", "-version"],
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    check=False,
)
java_output = f"{java_result.stdout}\n{java_result.stderr}"
java_match = re.search(
    r"^\s*java\.specification\.version\s*=\s*(\d+)\s*$",
    java_output,
    re.MULTILINE,
)
if java_result.returncode != 0 or java_match is None:
    fail("cannot determine the active Java runtime")
actual_java = int(java_match.group(1))
if actual_java != expected_java:
    fail(
        f"compatibility matrix requires Java {expected_java}, "
        f"but Java {actual_java} is active"
    )

root_pom = root / "pom.xml"
try:
    pom_document = ET.parse(root_pom)
except (OSError, ET.ParseError) as exc:
    fail(f"cannot parse root POM {root_pom}: {exc}")
project_version = pom_document.getroot().findtext("m:version", namespaces=pom_namespace)
if not project_version:
    fail("root POM does not declare a project version")

contract = matrix.get("sameJarHashContract")
if not isinstance(contract, dict):
    fail("compatibility matrix must declare sameJarHashContract")
if contract.get("algorithm") != "SHA-256":
    fail("sameJarHashContract.algorithm must be SHA-256")
if contract.get("productionBuildCount") != 1:
    fail("sameJarHashContract.productionBuildCount must be 1")
if contract.get("identicalAcrossBootLanes") is not True:
    fail("sameJarHashContract.identicalAcrossBootLanes must be true")

artifact_entries = require_list(
    contract.get("artifacts"), "sameJarHashContract.artifacts"
)
if len(artifact_entries) != 6:
    fail(
        "sameJarHashContract.artifacts must contain the six "
        "production jar coordinates"
    )
artifacts = []
coordinates = set()
for index, artifact in enumerate(artifact_entries):
    if not isinstance(artifact, dict):
        fail(f"sameJarHashContract.artifacts[{index}] must be an object")
    group_id = require_string(
        artifact.get("groupId"), f"artifacts[{index}].groupId"
    )
    artifact_id = require_string(
        artifact.get("artifactId"), f"artifacts[{index}].artifactId"
    )
    packaging = require_string(
        artifact.get("packaging"), f"artifacts[{index}].packaging"
    )
    if packaging != "jar":
        fail(f"production hash artifact must be a jar: {artifact_id}")
    coordinate = f"{group_id}:{artifact_id}:{packaging}"
    if coordinate in coordinates:
        fail(f"duplicate production hash coordinate: {coordinate}")
    coordinates.add(coordinate)
    artifacts.append(
        {
            "groupId": group_id,
            "artifactId": artifact_id,
            "packaging": packaging,
            "coordinate": coordinate,
        }
    )

flavor_entries = require_list(matrix.get("flavors"), "flavors")
if not flavor_entries:
    fail("compatibility matrix must declare at least one flavor")
flavors = {}
for index, flavor in enumerate(flavor_entries):
    if not isinstance(flavor, dict):
        fail(f"flavors[{index}] must be an object")
    flavor_id = require_string(flavor.get("id"), f"flavors[{index}].id")
    if flavor_id in flavors:
        fail(f"duplicate compatibility flavor: {flavor_id}")
    consumer_pom = repo_path(
        flavor.get("consumerPom"), f"flavors[{index}].consumerPom"
    )
    if not consumer_pom.is_file():
        fail(f"compatibility flavor consumer POM is missing: {consumer_pom}")
    goals = require_list(flavor.get("goals"), f"flavors[{index}].goals")
    if not goals or not all(
        isinstance(goal, str)
        and re.fullmatch(r"[A-Za-z0-9_.:-]+", goal)
        for goal in goals
    ):
        fail(f"compatibility flavor {flavor_id} has invalid Maven goals")
    if "-am" in goals or "--also-make" in goals:
        fail(
            f"compatibility flavor {flavor_id} must execute without "
            "reactor assistance"
        )
    flavors[flavor_id] = {
        "consumerPom": consumer_pom,
        "goals": goals,
    }

lanes = require_list(matrix.get("bootLanes"), "bootLanes")
if not lanes:
    fail("compatibility matrix must declare at least one Boot lane")
lane_ids = set()
for index, lane in enumerate(lanes):
    if not isinstance(lane, dict):
        fail(f"bootLanes[{index}] must be an object")
    lane_id = require_string(lane.get("id"), f"bootLanes[{index}].id")
    if lane_id in lane_ids:
        fail(f"duplicate Boot lane: {lane_id}")
    lane_ids.add(lane_id)
    require_string(lane.get("version"), f"bootLanes[{index}].version")
    profile = require_string(
        lane.get("mavenProfile"), f"bootLanes[{index}].mavenProfile"
    )
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", profile):
        fail(f"Boot lane has an invalid Maven profile: {profile!r}")
    lane_flavors = require_list(
        lane.get("flavors"), f"bootLanes[{index}].flavors"
    )
    if not lane_flavors:
        fail(f"Boot lane {lane_id} must declare at least one flavor")
    for flavor_id in lane_flavors:
        if flavor_id not in flavors:
            fail(
                f"Boot lane {lane_id} names unknown compatibility "
                f"flavor: {flavor_id}"
            )

artifact_paths = {
    artifact["coordinate"]: installed_artifact_path(artifact, project_version)
    for artifact in artifacts
}
trusted_repo_raw = os.environ.get(
    "RATCHET_SPRING_BOOT_REACTOR_INSTALLED_REPO", ""
)
trusted_install = (
    bool(trusted_repo_raw)
    and pathlib.Path(trusted_repo_raw).is_absolute()
    and pathlib.Path(trusted_repo_raw).resolve() == maven_repo
)
if not trusted_install or not all(
    path.is_file() for path in artifact_paths.values()
):
    if trusted_install:
        build_reason = "Same-run root install is missing production jars"
    else:
        build_reason = "No same-run root install attests this repository"
    print(
        f"{build_reason}; building the complete root reactor once...",
        flush=True,
    )
    try:
        subprocess.run(
            [
                "mvn",
                "-B",
                "-ntp",
                repo_property,
                "-DskipTests",
                "clean",
                "install",
            ],
            cwd=root,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        fail(f"production reactor build failed: {exc}")

missing = [
    f"{coordinate} ({path})"
    for coordinate, path in artifact_paths.items()
    if not path.is_file()
]
if missing:
    fail("installed production jars are missing: " + ", ".join(missing))

baseline_hashes = {
    coordinate: sha256(path)
    for coordinate, path in artifact_paths.items()
}
lane_hashes = {}
for lane in lanes:
    lane_id = lane["id"]
    for flavor_id in lane["flavors"]:
        flavor = flavors[flavor_id]
        invocation = [
            "mvn",
            "-B",
            "-ntp",
            repo_property,
            "-f",
            str(flavor["consumerPom"]),
            f"-P{lane['mavenProfile']}",
            *flavor["goals"],
        ]
        print(
            f"Running {lane_id}/{flavor_id} consumer with Boot "
            f"{lane['version']} against installed jars...",
            flush=True,
        )
        try:
            subprocess.run(invocation, cwd=root, check=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            fail(f"Boot lane {lane_id}/{flavor_id} failed: {exc}")

        current_hashes = {
            coordinate: sha256(path)
            for coordinate, path in artifact_paths.items()
        }
        changed = [
            coordinate
            for coordinate, digest in current_hashes.items()
            if digest != baseline_hashes[coordinate]
        ]
        if changed:
            fail(
                f"Boot lane {lane_id}/{flavor_id} changed installed "
                "production jars: " + ", ".join(changed)
            )
    lane_hashes[lane_id] = current_hashes

hash_record = repo_path(matrix.get("hashRecord"), "hashRecord")
hash_record.parent.mkdir(parents=True, exist_ok=True)
record = {
    "schemaVersion": 1,
    "algorithm": "SHA-256",
    "projectVersion": project_version,
    "javaRuntime": actual_java,
    "bootLanes": [
        {
            "id": lane["id"],
            "version": lane["version"],
            "hashes": lane_hashes[lane["id"]],
        }
        for lane in lanes
    ],
    "productionArtifacts": [
        {
            "coordinate": artifact["coordinate"],
            "sha256": baseline_hashes[artifact["coordinate"]],
        }
        for artifact in artifacts
    ],
}
try:
    with hash_record.open("w", encoding="utf-8") as stream:
        json.dump(record, stream, indent=2, sort_keys=True)
        stream.write("\n")
except OSError as exc:
    fail(f"cannot write hash record {hash_record}: {exc}")

print(f"Recorded JVM matrix hashes: {hash_record}", flush=True)
print("JVM matrix same-jar hash contract passed.", flush=True)
PY
