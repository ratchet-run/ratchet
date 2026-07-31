#!/usr/bin/env bash
#
# verify.sh — run one Spring Boot verification scenario against an isolated
# Maven repository and the exact commands declared in scenario-manifest.json.

set -euo pipefail

if [[ $# -ne 1 || -z "${1:-}" ]]; then
  echo "usage: $0 <scenario>" >&2
  exit 2
fi

SCENARIO="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MANIFEST="$ROOT/integrations/ratchet-spring-boot/integration-tests/scenario-manifest.json"
MATRIX="$ROOT/integrations/ratchet-spring-boot/integration-tests/compatibility-matrix.json"
QUALIFICATION_ATTESTATION="$ROOT/integrations/ratchet-spring-boot/integration-tests/target/qualification-attestation.json"
PRESERVE_QUALIFICATION_ATTESTATION=false
MAVEN_REPO=""

cleanup() {
  if [[ "$PRESERVE_QUALIFICATION_ATTESTATION" != "true" \
        && ( -e "$QUALIFICATION_ATTESTATION" || -L "$QUALIFICATION_ATTESTATION" ) ]]; then
    if [[ -f "$QUALIFICATION_ATTESTATION" || -L "$QUALIFICATION_ATTESTATION" ]]; then
      rm -f -- "$QUALIFICATION_ATTESTATION" \
        || echo "could not remove unverified qualification attestation: $QUALIFICATION_ATTESTATION" >&2
    else
      echo "refusing to preserve non-file qualification evidence: $QUALIFICATION_ATTESTATION" >&2
    fi
  fi
  if [[ -n "$MAVEN_REPO" && -d "$MAVEN_REPO" ]]; then
    rm -rf -- "$MAVEN_REPO"
  fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in git java mvn python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

MAVEN_REPO="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-spring-boot-m2.XXXXXX")"

# Canonicalize both paths before comparing them so a symlink cannot disguise
# the user's shared repository as an apparently isolated location.
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

resolved = candidate.resolve()
shared = pathlib.Path(os.path.expanduser("~/.m2/repository")).resolve()
if resolved == shared:
    raise SystemExit(
        f"refusing to use the shared Maven repository: {shared}"
    )
print(resolved)
PY
)"

echo "Spring Boot verification scenario: $SCENARIO"
echo "Isolated Maven repository: $MAVEN_REPO"

python3 - "$ROOT" "$MANIFEST" "$MATRIX" "$SCENARIO" "$MAVEN_REPO" <<'PY'
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1]).resolve()
manifest_path = pathlib.Path(sys.argv[2]).resolve()
matrix_path = pathlib.Path(sys.argv[3]).resolve()
scenario_name = sys.argv[4]
maven_repo = pathlib.Path(sys.argv[5]).resolve()
repo_property = f"-Dmaven.repo.local={maven_repo}"


def fail(message):
    raise SystemExit(f"verify.sh: {message}")


def load_json(path, label):
    if not path.is_file():
        fail(f"missing {label}: {path}")
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse {label} {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{label} must contain a JSON object: {path}")
    return value


def repo_path(raw_path, label):
    if not isinstance(raw_path, str) or not raw_path:
        fail(f"{label} must be a non-empty repository-relative path")
    candidate = (root / raw_path).resolve()
    if candidate == root or root not in candidate.parents:
        fail(f"{label} escapes the repository: {raw_path}")
    return candidate


def require_list(value, label):
    if not isinstance(value, list):
        fail(f"{label} must be a JSON array")
    return value


def require_string(value, label):
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    return value


def require_sha256(value, label):
    value = require_string(value, label)
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        fail(f"{label} must be a lowercase SHA-256 digest")
    return value


def sha256(path):
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        fail(f"cannot hash {path}: {exc}")
    return digest.hexdigest()


def canonical_json_sha256(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def relative_to_root(path):
    return path.relative_to(root).as_posix()


def atomic_write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.unlink(missing_ok=True)
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        fail(f"cannot atomically write {path}: {exc}")


def xml_local_name(element):
    return element.tag.rsplit("}", 1)[-1]


def report_counts(report_path, expected_class):
    try:
        document = ET.parse(report_path)
    except (OSError, ET.ParseError) as exc:
        fail(f"cannot parse test report {report_path}: {exc}")

    document_root = document.getroot()
    if xml_local_name(document_root) == "testsuite":
        suites = [document_root]
    elif xml_local_name(document_root) == "testsuites":
        suites = [
            child
            for child in document_root
            if xml_local_name(child) == "testsuite"
        ]
    else:
        fail(
            f"test report has an unexpected root element "
            f"{document_root.tag}: {report_path}"
        )

    if not suites:
        fail(f"test report contains no testsuite elements: {report_path}")

    tests = 0
    skipped = 0
    class_found = False
    for suite in suites:
        try:
            tests += int(suite.attrib.get("tests", "0"))
            skipped += int(suite.attrib.get("skipped", "0"))
        except ValueError as exc:
            fail(f"test report contains a non-numeric count: {report_path}: {exc}")
        if suite.attrib.get("name") == expected_class:
            class_found = True
        for case in suite.iter():
            if (
                xml_local_name(case) == "testcase"
                and case.attrib.get("classname") == expected_class
            ):
                class_found = True

    if not class_found:
        fail(
            f"test report does not contain expected class "
            f"{expected_class}: {report_path}"
        )
    return tests, skipped


manifest = load_json(manifest_path, "scenario manifest")
matrix = load_json(matrix_path, "compatibility matrix")
if manifest.get("schemaVersion") != 1:
    fail("scenario manifest schemaVersion must be 1")
if matrix.get("schemaVersion") != 1:
    fail("compatibility matrix schemaVersion must be 1")

scenarios = manifest.get("scenarios")
if not isinstance(scenarios, dict) or not scenarios:
    fail("scenario manifest must contain a non-empty scenarios object")
scenario = scenarios.get(scenario_name)
if not isinstance(scenario, dict):
    available = ", ".join(sorted(scenarios)) or "(none)"
    fail(f"unknown scenario {scenario_name!r}; available scenarios: {available}")

lane_entries = require_list(matrix.get("bootLanes"), "compatibility bootLanes")
lanes = {}
for index, lane in enumerate(lane_entries):
    if not isinstance(lane, dict):
        fail(f"compatibility bootLanes[{index}] must be an object")
    lane_id = require_string(lane.get("id"), f"bootLanes[{index}].id")
    if lane_id in lanes:
        fail(f"duplicate compatibility lane: {lane_id}")
    require_string(lane.get("version"), f"bootLanes[{index}].version")
    require_string(
        lane.get("mavenProfile"), f"bootLanes[{index}].mavenProfile"
    )
    lanes[lane_id] = lane

scenario_lanes = require_list(
    scenario.get("bootLanes"), f"scenario {scenario_name}.bootLanes"
)
if not scenario_lanes:
    fail(f"scenario {scenario_name} must declare at least one Boot lane")
for lane_id in scenario_lanes:
    if lane_id not in lanes:
        fail(
            f"scenario {scenario_name} names unknown compatibility lane: "
            f"{lane_id}"
        )

require_string(scenario.get("database"), f"scenario {scenario_name}.database")
require_list(
    scenario.get("sharedTckContracts"),
    f"scenario {scenario_name}.sharedTckContracts",
)
require_string(
    scenario.get("requiredCiJob"),
    f"scenario {scenario_name}.requiredCiJob",
)
conformance = scenario.get("conformanceArtifact")
if not isinstance(conformance, dict) or not isinstance(
    conformance.get("applicable"), bool
):
    fail(
        f"scenario {scenario_name}.conformanceArtifact must declare "
        "boolean applicable"
    )
conformance_path = None
conformance_files = []
if conformance["applicable"]:
    require_string(
        conformance.get("name"),
        f"scenario {scenario_name}.conformanceArtifact.name",
    )
    conformance_path = repo_path(
        conformance.get("path"),
        f"scenario {scenario_name}.conformanceArtifact.path",
    )
    expected_files = require_list(
        conformance.get("expectedFiles"),
        f"scenario {scenario_name}.conformanceArtifact.expectedFiles",
    )
    if not expected_files:
        fail(
            f"scenario {scenario_name}.conformanceArtifact.expectedFiles "
            "must not be empty"
        )
    seen_conformance_files = set()
    for index, raw_path in enumerate(expected_files):
        label = (
            f"scenario {scenario_name}.conformanceArtifact."
            f"expectedFiles[{index}]"
        )
        relative_path = require_string(raw_path, label)
        candidate = (conformance_path / relative_path).resolve()
        if (
            candidate == conformance_path
            or conformance_path not in candidate.parents
        ):
            fail(f"{label} escapes the conformance artifact path: {raw_path}")
        if candidate in seen_conformance_files:
            fail(
                f"{label} duplicates another expected conformance file: "
                f"{raw_path}"
            )
        seen_conformance_files.add(candidate)
        conformance_files.append(candidate)

qualification = scenario.get("qualification")
attestation_path = None
matrix_hash_record = None
runtime_dependency_flavor = None
topology_guard = None
qualification_coordinates = []
if qualification is not None:
    if not isinstance(qualification, dict):
        fail(f"scenario {scenario_name}.qualification must be an object")
    attestation_path = repo_path(
        qualification.get("attestationPath"),
        f"scenario {scenario_name}.qualification.attestationPath",
    )
    matrix_hash_record = repo_path(
        qualification.get("matrixHashRecord"),
        f"scenario {scenario_name}.qualification.matrixHashRecord",
    )
    runtime_dependency_flavor = require_string(
        qualification.get("runtimeDependencyFlavor"),
        f"scenario {scenario_name}.qualification.runtimeDependencyFlavor",
    )
    topology_guard = repo_path(
        qualification.get("topologyGuard"),
        f"scenario {scenario_name}.qualification.topologyGuard",
    )
    if not topology_guard.is_file():
        fail(f"qualification topology guard is missing: {topology_guard}")
    expected_attestation_path = (
        root
        / "integrations/ratchet-spring-boot/integration-tests/target/"
        "qualification-attestation.json"
    ).resolve()
    expected_matrix_hash_record = (
        root
        / "integrations/ratchet-spring-boot/integration-tests/target/"
        "jvm-matrix-hashes.json"
    ).resolve()
    expected_topology_guard = (
        root / "scripts/test-spring-boot-publication-topology.sh"
    ).resolve()
    if attestation_path != expected_attestation_path:
        fail(
            f"scenario {scenario_name}.qualification.attestationPath must be "
            f"{relative_to_root(expected_attestation_path)}"
        )
    if matrix_hash_record != expected_matrix_hash_record:
        fail(
            f"scenario {scenario_name}.qualification.matrixHashRecord must be "
            f"{relative_to_root(expected_matrix_hash_record)}"
        )
    if topology_guard != expected_topology_guard:
        fail(
            f"scenario {scenario_name}.qualification.topologyGuard must be "
            f"{relative_to_root(expected_topology_guard)}"
        )
    if runtime_dependency_flavor != "postgresql":
        fail(
            f"scenario {scenario_name}.qualification."
            "runtimeDependencyFlavor must be postgresql"
        )

    coordinate_entries = require_list(
        qualification.get("coordinates"),
        f"scenario {scenario_name}.qualification.coordinates",
    )
    if not coordinate_entries:
        fail(
            f"scenario {scenario_name}.qualification.coordinates "
            "must not be empty"
        )
    seen_coordinates = set()
    for index, entry in enumerate(coordinate_entries):
        label = f"scenario {scenario_name}.qualification.coordinates[{index}]"
        if not isinstance(entry, dict):
            fail(f"{label} must be an object")
        coordinate = require_string(entry.get("coordinate"), f"{label}.coordinate")
        if not re.fullmatch(
            r"run\.ratchet:ratchet-spring-boot-[a-z0-9-]+:(?:jar|pom)",
            coordinate,
        ):
            fail(f"{label}.coordinate is invalid: {coordinate!r}")
        if coordinate in seen_coordinates:
            fail(f"duplicate qualification coordinate: {coordinate}")
        seen_coordinates.add(coordinate)

        hash_source = entry.get("hashSource")
        if not isinstance(hash_source, dict):
            fail(f"{label}.hashSource must be an object")
        source_type = hash_source.get("type")
        if source_type not in {"installed", "matrix"}:
            fail(f"{label}.hashSource.type must be installed or matrix")
        qualification_coordinates.append(
            {
                "coordinate": coordinate,
                "sourceType": source_type,
            }
        )

    expected_qualification_sources = {
        "run.ratchet:ratchet-spring-boot-parent:pom": "installed",
        "run.ratchet:ratchet-spring-boot-autoconfigure:jar": "matrix",
        "run.ratchet:ratchet-spring-boot-autoconfigure-jpa:jar": "matrix",
        "run.ratchet:ratchet-spring-boot-starter:jar": "matrix",
    }
    actual_qualification_sources = {
        entry["coordinate"]: entry["sourceType"]
        for entry in qualification_coordinates
    }
    if actual_qualification_sources != expected_qualification_sources:
        fail(
            f"scenario {scenario_name}.qualification.coordinates must "
            "declare the parent installed POM and three matrix jars"
        )

    try:
        attestation_path.unlink(missing_ok=True)
    except OSError as exc:
        fail(f"cannot clear stale qualification attestation {attestation_path}: {exc}")

sentinels = require_list(
    scenario.get("sourceSentinels"),
    f"scenario {scenario_name}.sourceSentinels",
)
for index, sentinel in enumerate(sentinels):
    if not isinstance(sentinel, dict):
        fail(f"sourceSentinels[{index}] must be an object")
    if sentinel.get("kind") != "file-exists":
        fail(
            f"sourceSentinels[{index}] has unsupported kind: "
            f"{sentinel.get('kind')!r}"
        )
    path = repo_path(
        sentinel.get("path"), f"sourceSentinels[{index}].path"
    )
    if not path.is_file():
        fail(f"source sentinel is missing: {path}")

commands = require_list(
    scenario.get("commands"), f"scenario {scenario_name}.commands"
)
if not commands:
    fail(f"scenario {scenario_name} must contain at least one command")

command_by_id = {}
for index, command in enumerate(commands):
    if not isinstance(command, dict):
        fail(f"commands[{index}] must be an object")
    command_id = require_string(command.get("id"), f"commands[{index}].id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", command_id):
        fail(f"command id is not portable: {command_id!r}")
    if command_id in command_by_id:
        fail(f"duplicate command id: {command_id}")
    if not isinstance(command.get("producesTests"), bool):
        fail(f"command {command_id} must declare boolean producesTests")
    command_by_id[command_id] = command

reports = require_list(
    scenario.get("expectedReports"),
    f"scenario {scenario_name}.expectedReports",
)
reports_by_command = {}
report_specs = []
for index, report in enumerate(reports):
    if not isinstance(report, dict):
        fail(f"expectedReports[{index}] must be an object")
    command_id = require_string(
        report.get("afterCommand"),
        f"expectedReports[{index}].afterCommand",
    )
    if command_id not in command_by_id:
        fail(
            f"expected report refers to unknown command: {command_id}"
        )
    if report.get("kind") not in {"surefire", "failsafe"}:
        fail(
            f"expectedReports[{index}].kind must be surefire or failsafe"
        )
    lane_id = require_string(
        report.get("lane"), f"expectedReports[{index}].lane"
    )
    if lane_id not in scenario_lanes:
        fail(
            f"expectedReports[{index}] names lane outside scenario: "
            f"{lane_id}"
        )
    report_path = repo_path(
        report.get("path"), f"expectedReports[{index}].path"
    )
    report_class = require_string(
        report.get("reportClass"),
        f"expectedReports[{index}].reportClass",
    )
    minimum = report.get("minimumExecutedTests")
    if not isinstance(minimum, int) or isinstance(minimum, bool) or minimum < 1:
        fail(
            f"expectedReports[{index}].minimumExecutedTests "
            "must be a positive integer"
        )
    reports_by_command.setdefault(command_id, []).append(
        (report_path, report_class, minimum, lane_id)
    )
    report_specs.append(
        {
            "afterCommand": command_id,
            "kind": report["kind"],
            "path": report_path,
            "reportClass": report_class,
            "lane": lane_id,
        }
    )

for command_id, command in command_by_id.items():
    if command["producesTests"] and command_id not in reports_by_command:
        fail(
            f"test-producing command {command_id} has no expected report"
        )

environment = os.environ.copy()
environment["MAVEN_REPO_LOCAL"] = str(maven_repo)

print("Installing the complete root reactor with tests skipped...", flush=True)
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
        env=environment,
        check=True,
    )
except (OSError, subprocess.CalledProcessError) as exc:
    fail(f"complete root reactor install failed: {exc}")

# Child verification scripts may reuse installed artifacts only when this
# same verify.sh run vouches for the exact isolated repository.
environment["RATCHET_SPRING_BOOT_REACTOR_INSTALLED_REPO"] = str(
    maven_repo
)

for command in commands:
    command_id = command["id"]
    command_type = command.get("type")
    if command_type == "script":
        script = repo_path(command.get("path"), f"command {command_id}.path")
        if not script.is_file():
            fail(f"command script is missing: {script}")
        arguments = require_list(
            command.get("arguments"), f"command {command_id}.arguments"
        )
        if not all(isinstance(argument, str) for argument in arguments):
            fail(f"command {command_id}.arguments must contain only strings")
        interpreter = command.get("interpreter", "bash")
        if interpreter == "bash":
            invocation = [
                "bash",
                str(script),
                repo_property,
                *arguments,
            ]
        elif interpreter == "python3":
            invocation = ["python3", str(script), *arguments]
        else:
            fail(
                f"command {command_id}.interpreter must be bash or python3"
            )
    elif command_type == "maven":
        pom = repo_path(command.get("pom"), f"command {command_id}.pom")
        if not pom.is_file():
            fail(f"command POM is missing: {pom}")
        profiles = require_list(
            command.get("profiles"), f"command {command_id}.profiles"
        )
        goals = require_list(
            command.get("goals"), f"command {command_id}.goals"
        )
        arguments = command.get("arguments", [])
        require_list(arguments, f"command {command_id}.arguments")
        if not profiles or not all(
            isinstance(profile, str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+", profile)
            for profile in profiles
        ):
            fail(f"command {command_id} has invalid Maven profiles")
        if not goals or not all(
            isinstance(goal, str)
            and re.fullmatch(r"[A-Za-z0-9_.:-]+", goal)
            for goal in goals
        ):
            fail(f"command {command_id} has invalid Maven goals")
        if not all(isinstance(argument, str) for argument in arguments):
            fail(f"command {command_id}.arguments must contain only strings")
        forbidden = {"-am", "--also-make"}
        if forbidden.intersection([*goals, *arguments]):
            fail(
                f"command {command_id} must run installed artifacts "
                "without -am/--also-make"
            )
        if any(
            argument.startswith("-Dmaven.repo.local")
            for argument in arguments
        ):
            fail(
                f"command {command_id} must not override the isolated "
                "Maven repository"
            )
        lane_id = require_string(
            command.get("lane"), f"command {command_id}.lane"
        )
        if lane_id not in scenario_lanes:
            fail(
                f"command {command_id} names lane outside scenario: "
                f"{lane_id}"
            )
        required_profile = lanes[lane_id]["mavenProfile"]
        if required_profile not in profiles:
            fail(
                f"command {command_id} for {lane_id} does not activate "
                f"required profile {required_profile}"
            )
        invocation = [
            "mvn",
            "-B",
            "-ntp",
            repo_property,
            "-f",
            str(pom),
            f"-P{','.join(profiles)}",
            *arguments,
            *goals,
        ]
    else:
        fail(
            f"command {command_id} has unsupported type: "
            f"{command_type!r}"
        )

    print(f"Running manifest command: {command_id}", flush=True)
    try:
        subprocess.run(
            invocation,
            cwd=root,
            env=environment,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        fail(f"command {command_id} failed: {exc}")

    for report_path, report_class, minimum, lane_id in reports_by_command.get(
        command_id, []
    ):
        if not report_path.is_file():
            fail(
                f"missing expected test report after {command_id}: "
                f"{report_path}"
            )
        tests, skipped = report_counts(report_path, report_class)
        executed = tests - skipped
        if executed < minimum:
            fail(
                f"test report for {command_id} executed {executed} tests; "
                f"expected at least {minimum}: {report_path}"
            )
        print(
            f"Verified {lane_id} {report_class}: "
            f"{executed} executed ({skipped} skipped)",
            flush=True,
        )

if conformance_path is not None:
    if not conformance_path.is_dir():
        fail(f"missing conformance artifact path: {conformance_path}")
    for conformance_file in conformance_files:
        if not conformance_file.is_file():
            fail(f"missing expected conformance file: {conformance_file}")
        try:
            size = conformance_file.stat().st_size
        except OSError as exc:
            fail(f"cannot inspect conformance file {conformance_file}: {exc}")
        if size == 0:
            fail(f"expected conformance file is empty: {conformance_file}")
        print(f"Verified conformance file: {conformance_file}", flush=True)

if attestation_path is not None:
    matrix_record = load_json(matrix_hash_record, "JVM matrix hash record")
    if matrix_record.get("schemaVersion") != 1:
        fail("JVM matrix hash record schemaVersion must be 1")
    if matrix_record.get("algorithm") != "SHA-256":
        fail("JVM matrix hash record algorithm must be SHA-256")
    project_version = require_string(
        matrix_record.get("projectVersion"),
        "JVM matrix hash record projectVersion",
    )
    consumer_java_runtime = matrix_record.get("consumerJavaRuntime")
    if not isinstance(consumer_java_runtime, int) or isinstance(
        consumer_java_runtime, bool
    ):
        fail("JVM matrix hash record consumerJavaRuntime must be an integer")

    production_hashes = {}
    production_entries = require_list(
        matrix_record.get("productionArtifacts"),
        "JVM matrix hash record productionArtifacts",
    )
    for index, entry in enumerate(production_entries):
        label = f"JVM matrix productionArtifacts[{index}]"
        if not isinstance(entry, dict):
            fail(f"{label} must be an object")
        coordinate = require_string(entry.get("coordinate"), f"{label}.coordinate")
        digest = require_sha256(entry.get("sha256"), f"{label}.sha256")
        if coordinate in production_hashes:
            fail(f"duplicate JVM matrix production artifact: {coordinate}")
        production_hashes[coordinate] = digest

    runtime_entries = require_list(
        matrix_record.get("runtimeDependencies"),
        "JVM matrix hash record runtimeDependencies",
    )
    runtime_evidence = []
    runtime_keys = set()
    for index, entry in enumerate(runtime_entries):
        label = f"JVM matrix runtimeDependencies[{index}]"
        if not isinstance(entry, dict):
            fail(f"{label} must be an object")
        lane_id = require_string(entry.get("lane"), f"{label}.lane")
        flavor_id = require_string(entry.get("flavor"), f"{label}.flavor")
        require_sha256(entry.get("treeSha256"), f"{label}.treeSha256")
        dependencies = require_list(
            entry.get("dependencies"), f"{label}.dependencies"
        )
        if not dependencies:
            fail(f"{label}.dependencies must not be empty")
        seen_dependencies = set()
        for dependency_index, dependency in enumerate(dependencies):
            dependency_label = (
                f"{label}.dependencies[{dependency_index}]"
            )
            if not isinstance(dependency, dict):
                fail(f"{dependency_label} must be an object")
            coordinate = require_string(
                dependency.get("coordinate"),
                f"{dependency_label}.coordinate",
            )
            require_sha256(
                dependency.get("sha256"), f"{dependency_label}.sha256"
            )
            scopes = require_list(
                dependency.get("scopes"), f"{dependency_label}.scopes"
            )
            if not scopes or not all(
                scope in {"compile", "runtime"} for scope in scopes
            ):
                fail(
                    f"{dependency_label}.scopes must contain only "
                    "compile/runtime"
                )
            if coordinate in seen_dependencies:
                fail(f"{dependency_label} duplicates {coordinate}")
            seen_dependencies.add(coordinate)

        key = (lane_id, flavor_id)
        if key in runtime_keys:
            fail(
                f"duplicate JVM matrix runtime dependency evidence: "
                f"{lane_id}/{flavor_id}"
            )
        runtime_keys.add(key)
        if lane_id in scenario_lanes and flavor_id == runtime_dependency_flavor:
            runtime_evidence.append(entry)

    expected_runtime_keys = {
        (lane_id, runtime_dependency_flavor) for lane_id in scenario_lanes
    }
    actual_runtime_keys = {
        (entry["lane"], entry["flavor"]) for entry in runtime_evidence
    }
    if actual_runtime_keys != expected_runtime_keys:
        missing = sorted(expected_runtime_keys - actual_runtime_keys)
        fail(
            "JVM matrix hash record is missing runtime dependency evidence: "
            + ", ".join(f"{lane}/{flavor}" for lane, flavor in missing)
        )

    coordinate_evidence = []
    for entry in qualification_coordinates:
        coordinate = entry["coordinate"]
        source_type = entry["sourceType"]
        if source_type == "matrix":
            digest = production_hashes.get(coordinate)
            if digest is None:
                fail(
                    f"JVM matrix hash record is missing qualification "
                    f"coordinate: {coordinate}"
                )
        elif source_type == "installed":
            group_id, artifact_id, packaging = coordinate.split(":")
            installed_path = (
                maven_repo
                / pathlib.Path(*group_id.split("."))
                / artifact_id
                / project_version
                / f"{artifact_id}-{project_version}.{packaging}"
            )
            if not installed_path.is_file():
                fail(
                    f"installed qualification artifact is missing: "
                    f"{coordinate} ({installed_path})"
                )
            digest = sha256(installed_path)
        else:
            fail(f"unsupported qualification hash source: {source_type}")
        coordinate_evidence.append(
            {
                "coordinate": coordinate,
                "source": (
                    "jvm-matrix" if source_type == "matrix" else source_type
                ),
                "sha256": digest,
                "scenarios": [scenario_name],
            }
        )

    git_result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    commit = git_result.stdout.strip()
    if git_result.returncode != 0 or not re.fullmatch(
        r"[0-9a-f]{40}(?:[0-9a-f]{24})?", commit
    ):
        fail("cannot determine the current Git commit")

    consumer_java_home_raw = os.environ.get("RATCHET_MATRIX_JAVA_HOME", "")
    if consumer_java_home_raw:
        consumer_java_home = pathlib.Path(consumer_java_home_raw)
        if not consumer_java_home.is_absolute():
            fail(
                "RATCHET_MATRIX_JAVA_HOME must be an absolute path: "
                f"{consumer_java_home_raw}"
            )
        consumer_java_home = consumer_java_home.resolve()
        consumer_java_binary = consumer_java_home / "bin" / "java"
        if not consumer_java_binary.is_file():
            fail(
                "RATCHET_MATRIX_JAVA_HOME does not contain a java "
                f"executable: {consumer_java_home}"
            )
        java_binary = str(consumer_java_binary)
    else:
        java_binary = "java"

    java_result = subprocess.run(
        [java_binary, "-XshowSettings:properties", "-version"],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    java_output = f"{java_result.stdout}\n{java_result.stderr}"
    java_match = re.search(
        r"^\s*java\.version\s*=\s*(\S+)\s*$", java_output, re.MULTILINE
    )
    java_spec_match = re.search(
        r"^\s*java\.specification\.version\s*=\s*(\d+)\s*$",
        java_output,
        re.MULTILINE,
    )
    if (
        java_result.returncode != 0
        or java_match is None
        or java_spec_match is None
    ):
        fail(
            "cannot determine consumer java.version for qualification "
            "attestation"
        )
    actual_consumer_java_runtime = int(java_spec_match.group(1))
    if actual_consumer_java_runtime != consumer_java_runtime:
        fail(
            f"qualification consumer Java is "
            f"{actual_consumer_java_runtime}, but the JVM matrix record "
            f"requires Java {consumer_java_runtime}"
        )

    maven_result = subprocess.run(
        ["mvn", "-version"],
        cwd=root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    maven_output = f"{maven_result.stdout}\n{maven_result.stderr}"
    maven_match = re.search(r"Apache Maven\s+(\S+)", maven_output)
    if maven_result.returncode != 0 or maven_match is None:
        fail("cannot determine Maven version for qualification attestation")

    attestation = {
        "schemaVersion": 1,
        "algorithm": "SHA-256",
        "commit": commit,
        "projectVersion": project_version,
        "scenario": {
            "name": scenario_name,
            "sha256": canonical_json_sha256(scenario),
        },
        "scenarios": [scenario_name],
        "coordinates": coordinate_evidence,
        "runtimeDependencies": runtime_evidence,
        "reports": [
            {
                "path": relative_to_root(report["path"]),
                "lane": report["lane"],
                "afterCommand": report["afterCommand"],
                "kind": report["kind"],
                "reportClass": report["reportClass"],
                "sha256": sha256(report["path"]),
            }
            for report in report_specs
        ],
        "conformance": [
            {
                "path": relative_to_root(conformance_file),
                "sha256": sha256(conformance_file),
            }
            for conformance_file in conformance_files
        ],
        "toolchain": {
            "javaVersion": java_match.group(1),
            "javaSpecificationVersion": actual_consumer_java_runtime,
            "mavenVersion": maven_match.group(1),
            "consumerJavaRuntime": consumer_java_runtime,
        },
    }
    atomic_write_json(attestation_path, attestation)
    print(f"Wrote qualification attestation: {attestation_path}", flush=True)

    guard_result = None
    guard_error = None
    try:
        guard_result = subprocess.run(
            ["bash", str(topology_guard), repo_property],
            cwd=root,
            env=environment,
            check=False,
        )
    except OSError as exc:
        guard_error = str(exc)
    if guard_error is not None or guard_result.returncode != 0:
        try:
            attestation_path.unlink(missing_ok=True)
        except OSError as exc:
            fail(
                f"qualification topology guard failed and the invalid "
                f"attestation could not be removed: {exc}"
            )
        if guard_error is not None:
            fail(f"qualification topology guard could not run: {guard_error}")
        fail(
            f"qualification topology guard failed with exit status "
            f"{guard_result.returncode}"
        )

print(f"Spring Boot scenario {scenario_name} passed.", flush=True)
PY

if [[ -f "$QUALIFICATION_ATTESTATION" ]]; then
  PRESERVE_QUALIFICATION_ATTESTATION=true
fi
