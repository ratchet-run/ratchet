#!/usr/bin/env bash
#
# verify-jvm-matrix.sh — build the production jars once, execute every JVM
# consumer lane without reactor assistance, and prove the installed jar hashes
# remain identical across the matrix.
#
# The production reactor build runs on whatever JVM is ambient in the calling
# shell (the build JDK — the repo currently builds on Temurin 25). The
# compatibility matrix's declared consumerJavaRuntime is a requirement on the
# CONSUMER lanes only: the JVM that actually executes the installed jars. Set
# RATCHET_MATRIX_JAVA_HOME to an absolute JDK home to run the consumer lane
# invocations under that JVM regardless of the ambient build JDK; if it is
# unset, the consumer lanes fall back to the ambient JVM and it must already
# be the declared consumerJavaRuntime.
#
# Usage:
#   verify-jvm-matrix.sh [flavor]
#   verify-jvm-matrix.sh -Dmaven.repo.local=/absolute/isolated/repository [flavor]
#
# Environment:
#   RATCHET_MATRIX_JAVA_HOME=/absolute/path/to/jdk-17   (optional)

set -euo pipefail

if [[ $# -gt 2 ]]; then
  echo "usage: $0 [-Dmaven.repo.local=/absolute/isolated/repository] [flavor]" >&2
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
SELECTED_FLAVOR=""
for argument in "$@"; do
  case "$argument" in
    -Dmaven.repo.local=*)
      if [[ -n "$ARGUMENT_REPO" ]]; then
        echo "maven.repo.local may be specified only once" >&2
        exit 2
      fi
      ARGUMENT_REPO="${argument#-Dmaven.repo.local=}"
      ;;
    *)
      if [[ -n "$SELECTED_FLAVOR" ]]; then
        echo "only one compatibility flavor may be selected" >&2
        echo "usage: $0 [-Dmaven.repo.local=/absolute/isolated/repository] [flavor]" >&2
        exit 2
      fi
      SELECTED_FLAVOR="$argument"
      ;;
  esac
done

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

python3 - "$ROOT" "$MATRIX" "$MAVEN_REPO" "$SELECTED_FLAVOR" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1]).resolve()
matrix_path = pathlib.Path(sys.argv[2]).resolve()
maven_repo = pathlib.Path(sys.argv[3]).resolve()
selected_flavor = sys.argv[4]
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


def dependency_artifact(node, label):
    group_id = require_string(node.get("groupId"), f"{label}.groupId")
    artifact_id = require_string(
        node.get("artifactId"), f"{label}.artifactId"
    )
    version = require_string(node.get("version"), f"{label}.version")
    artifact_type = require_string(node.get("type"), f"{label}.type")
    classifier = node.get("classifier", "")
    if not isinstance(classifier, str):
        fail(f"{label}.classifier must be a string")

    extension = "jar" if artifact_type == "test-jar" else artifact_type
    coordinate_parts = [group_id, artifact_id, artifact_type]
    if classifier:
        coordinate_parts.append(classifier)
    coordinate_parts.append(version)
    coordinate = ":".join(coordinate_parts)
    classifier_suffix = f"-{classifier}" if classifier else ""
    artifact_path = (
        maven_repo
        / pathlib.Path(*group_id.split("."))
        / artifact_id
        / version
        / f"{artifact_id}-{version}{classifier_suffix}.{extension}"
    )
    return coordinate, artifact_path


def runtime_dependency_evidence(tree_path, lane_id, flavor_id):
    try:
        with tree_path.open(encoding="utf-8") as stream:
            tree = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse runtime dependency tree {tree_path}: {exc}")
    if not isinstance(tree, dict):
        fail(f"runtime dependency tree must contain an object: {tree_path}")

    resolved = {}

    def visit(node, label, project_node=False):
        if not isinstance(node, dict):
            fail(f"{label} must be an object")
        if not project_node:
            scope = require_string(node.get("scope"), f"{label}.scope")
            if scope in {"compile", "runtime"}:
                coordinate, artifact_path = dependency_artifact(node, label)
                if not artifact_path.is_file():
                    fail(
                        f"resolved runtime dependency is missing: "
                        f"{coordinate} ({artifact_path})"
                    )
                digest = sha256(artifact_path)
                prior = resolved.get(coordinate)
                if prior is None:
                    prior = {"sha256": digest, "scopes": set()}
                    resolved[coordinate] = prior
                elif prior["sha256"] != digest:
                    fail(
                        f"resolved runtime dependency changed while reading "
                        f"the tree: {coordinate}"
                    )
                prior["scopes"].add(scope)

        children = node.get("children", [])
        if not isinstance(children, list):
            fail(f"{label}.children must be an array")
        for index, child in enumerate(children):
            visit(child, f"{label}.children[{index}]")

    visit(tree, "runtimeDependencyTree", project_node=True)
    if not resolved:
        fail(
            f"runtime dependency tree contains no resolved compile/runtime "
            f"dependencies: {tree_path}"
        )

    return {
        "lane": lane_id,
        "flavor": flavor_id,
        "treeSha256": sha256(tree_path),
        "dependencies": [
            {
                "coordinate": coordinate,
                "sha256": evidence["sha256"],
                "scopes": sorted(evidence["scopes"]),
            }
            for coordinate, evidence in sorted(resolved.items())
        ],
    }


matrix = load_json(matrix_path)
if matrix.get("schemaVersion") != 1:
    fail("compatibility matrix schemaVersion must be 1")

report_archive = matrix_path.parent / "target" / "jvm-matrix-reports"
if report_archive.exists():
    shutil.rmtree(report_archive)

expected_java = matrix.get("consumerJavaRuntime")
if not isinstance(expected_java, int) or isinstance(expected_java, bool):
    fail("compatibility matrix consumerJavaRuntime must be an integer")

# consumerJavaRuntime constrains the CONSUMER lanes (the JVM that runs the installed
# jars), not the build JDK used for the root reactor install above/below.
consumer_java_home_raw = os.environ.get("RATCHET_MATRIX_JAVA_HOME", "")
consumer_java_home = None
consumer_environment = os.environ.copy()
if consumer_java_home_raw:
    candidate = pathlib.Path(consumer_java_home_raw)
    if not candidate.is_absolute():
        fail(
            "RATCHET_MATRIX_JAVA_HOME must be an absolute path: "
            f"{consumer_java_home_raw}"
        )
    consumer_java_home = candidate.resolve()
    consumer_java_binary = consumer_java_home / "bin" / "java"
    if not consumer_java_binary.is_file():
        fail(
            "RATCHET_MATRIX_JAVA_HOME does not contain a java executable: "
            f"{consumer_java_home}"
        )
    consumer_environment["JAVA_HOME"] = str(consumer_java_home)
    consumer_environment["PATH"] = (
        f"{consumer_java_home / 'bin'}{os.pathsep}{consumer_environment.get('PATH', '')}"
    )
    java_binary = str(consumer_java_binary)
else:
    java_binary = "java"

java_result = subprocess.run(
    [java_binary, "-XshowSettings:properties", "-version"],
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
    fail("cannot determine the consumer lane Java runtime")
actual_java = int(java_match.group(1))
if actual_java != expected_java:
    if consumer_java_home is not None:
        fail(
            f"RATCHET_MATRIX_JAVA_HOME {consumer_java_home} runs Java "
            f"{actual_java}, but the compatibility matrix requires the "
            f"consumer lanes to run Java {expected_java}"
        )
    fail(
        f"compatibility matrix requires the consumer lanes to run Java "
        f"{expected_java}, but the ambient Java is {actual_java}. Set "
        "RATCHET_MATRIX_JAVA_HOME to a Java "
        f"{expected_java} JDK, or make Java {expected_java} the ambient JVM."
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
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", flavor_id):
        fail(f"compatibility flavor has a non-portable id: {flavor_id!r}")
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

if selected_flavor:
    if selected_flavor not in flavors:
        available = ", ".join(sorted(flavors))
        fail(
            f"unknown compatibility flavor {selected_flavor!r}; "
            f"available flavors: {available}"
        )
    selected_flavors = {selected_flavor}
else:
    selected_flavors = set(flavors)

lanes = require_list(matrix.get("bootLanes"), "bootLanes")
if not lanes:
    fail("compatibility matrix must declare at least one Boot lane")
lane_ids = set()
for index, lane in enumerate(lanes):
    if not isinstance(lane, dict):
        fail(f"bootLanes[{index}] must be an object")
    lane_id = require_string(lane.get("id"), f"bootLanes[{index}].id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", lane_id):
        fail(f"Boot lane has a non-portable id: {lane_id!r}")
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
    if not selected_flavors.intersection(lane_flavors):
        requested = ", ".join(sorted(selected_flavors))
        fail(
            f"Boot lane {lane_id} does not declare any selected flavor: "
            f"{requested}"
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
runtime_dependencies = []
for lane in lanes:
    lane_id = lane["id"]
    lane_flavors = [
        flavor_id
        for flavor_id in lane["flavors"]
        if flavor_id in selected_flavors
    ]
    for flavor_id in lane_flavors:
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
            subprocess.run(
                invocation, cwd=root, env=consumer_environment, check=True
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            fail(f"Boot lane {lane_id}/{flavor_id} failed: {exc}")

        consumer_target = flavor["consumerPom"].parent / "target"
        copied_reports = False
        for report_directory in ("surefire-reports", "failsafe-reports"):
            source = consumer_target / report_directory
            if source.is_dir():
                destination = (
                    report_archive
                    / lane_id
                    / flavor_id
                    / report_directory
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(source, destination)
                copied_reports = True
        if not copied_reports:
            fail(
                f"Boot lane {lane_id}/{flavor_id} produced no test reports"
            )

        for conformance_report in sorted(
            consumer_target.glob("tck-*-conformance-report.md")
        ):
            destination = (
                report_archive
                / "conformance"
                / flavor_id
                / lane_id
                / conformance_report.name
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(conformance_report, destination)

        runtime_dependency_tree = (
            consumer_target / "runtime-dependency-tree.json"
        )
        # Runtime-store flavors feed qualification and dependency-policy evidence.
        if (
            flavor_id in {"postgresql", "mongodb", "mysql"}
            and not runtime_dependency_tree.is_file()
        ):
            fail(
                f"Boot lane {lane_id}/{flavor_id} produced no runtime "
                "dependency tree"
            )
        if runtime_dependency_tree.is_file():
            runtime_dependencies.append(
                runtime_dependency_evidence(
                    runtime_dependency_tree, lane_id, flavor_id
                )
            )

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
    "consumerJavaRuntime": actual_java,
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
    "runtimeDependencies": runtime_dependencies,
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
