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

for command in mvn python3; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

MAVEN_REPO="$(mktemp -d "${TMPDIR:-/tmp}/ratchet-spring-boot-m2.XXXXXX")"
cleanup() {
  rm -rf "$MAVEN_REPO"
}
trap cleanup EXIT

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
if conformance["applicable"]:
    require_string(
        conformance.get("name"),
        f"scenario {scenario_name}.conformanceArtifact.name",
    )
    require_string(
        conformance.get("path"),
        f"scenario {scenario_name}.conformanceArtifact.path",
    )

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
        invocation = [
            "bash",
            str(script),
            repo_property,
            *arguments,
        ]
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

print(f"Spring Boot scenario {scenario_name} passed.", flush=True)
PY
