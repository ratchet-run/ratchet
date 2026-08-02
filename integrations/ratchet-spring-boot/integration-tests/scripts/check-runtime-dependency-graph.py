#!/usr/bin/env python3
"""Enforce a scenario-owned allowlist policy on runtime dependency graphs."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import NoReturn


EXPECTED_LANES = ("boot-3.5", "boot-4.1")
SCENARIO_MANIFEST = Path(
    "integrations/ratchet-spring-boot/integration-tests/scenario-manifest.json"
)
MATRIX_HASH_RECORD = Path(
    "integrations/ratchet-spring-boot/integration-tests/target/jvm-matrix-hashes.json"
)


def fail(message: str) -> NoReturn:
    raise SystemExit(f"check-runtime-dependency-graph.py: {message}")


def repository_root() -> Path:
    script = Path(__file__).resolve()
    for candidate in script.parents:
        if (candidate / SCENARIO_MANIFEST).is_file():
            return candidate
    fail(f"cannot locate repository root from {script}")


def load_object(path: Path, label: str) -> dict[str, object]:
    try:
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse {label} {path}: {exc}")
    if not isinstance(document, dict):
        fail(f"{label} must contain a JSON object")
    return document


def require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    return value


def require_string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        fail(f"{label} must be an array of non-empty strings")
    if len(value) != len(set(value)):
        fail(f"{label} must not contain duplicates")
    return value


def scenario_policy(
    manifest: dict[str, object], flavor: str
) -> tuple[str, list[re.Pattern[str]], set[str]]:
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, dict):
        fail("scenario manifest scenarios must be a JSON object")

    matches: list[tuple[str, dict[str, object]]] = []
    for scenario_name, raw_scenario in scenarios.items():
        if not isinstance(scenario_name, str) or not isinstance(raw_scenario, dict):
            fail("scenario manifest contains a malformed scenario")
        qualification = raw_scenario.get("qualification")
        if (
            isinstance(qualification, dict)
            and qualification.get("runtimeDependencyFlavor") == flavor
        ):
            matches.append((scenario_name, raw_scenario))

    if len(matches) != 1:
        fail(
            f"expected exactly one scenario with runtime dependency flavor "
            f"{flavor!r}; found {len(matches)}"
        )

    scenario_name, scenario = matches[0]
    policy = scenario.get("dependencyPolicy")
    if not isinstance(policy, dict):
        fail(f"scenario {scenario_name} lacks dependencyPolicy")

    denied_patterns = require_string_list(
        policy.get("deniedPatterns"),
        f"scenario {scenario_name}.dependencyPolicy.deniedPatterns",
    )
    allowed_exceptions = set(
        require_string_list(
            policy.get("allowedExceptions"),
            f"scenario {scenario_name}.dependencyPolicy.allowedExceptions",
        )
    )

    compiled_patterns: list[re.Pattern[str]] = []
    for pattern in denied_patterns:
        try:
            compiled_patterns.append(re.compile(pattern))
        except re.error as exc:
            fail(
                f"scenario {scenario_name} has invalid denied pattern "
                f"{pattern!r}: {exc}"
            )
    return scenario_name, compiled_patterns, allowed_exceptions


def group_artifact(coordinate: object, label: str) -> tuple[str, str]:
    value = require_string(coordinate, label)
    parts = value.split(":")
    if len(parts) < 4 or not parts[0] or not parts[1]:
        fail(
            f"{label} must use group:artifact:type[:classifier]:version format; "
            f"found {value!r}"
        )
    return value, f"{parts[0]}:{parts[1]}"


def runtime_entries(
    matrix_record: dict[str, object], flavor: str
) -> dict[str, dict[str, object]]:
    raw_entries = matrix_record.get("runtimeDependencies")
    if not isinstance(raw_entries, list):
        fail("JVM matrix hash record runtimeDependencies must be an array")

    entries: dict[str, dict[str, object]] = {}
    for index, raw_entry in enumerate(raw_entries):
        label = f"JVM matrix runtimeDependencies[{index}]"
        if not isinstance(raw_entry, dict):
            fail(f"{label} must be an object")
        entry_flavor = require_string(raw_entry.get("flavor"), f"{label}.flavor")
        if entry_flavor != flavor:
            continue
        lane = require_string(raw_entry.get("lane"), f"{label}.lane")
        if lane in entries:
            fail(f"duplicate runtime dependency entry for {lane}/{flavor}")
        entries[lane] = raw_entry

    missing = [lane for lane in EXPECTED_LANES if lane not in entries]
    unexpected = sorted(set(entries) - set(EXPECTED_LANES))
    if missing:
        fail(
            f"runtime dependency entries for {flavor} are missing lanes: "
            + ", ".join(missing)
        )
    if unexpected:
        fail(
            f"runtime dependency entries for {flavor} contain unexpected lanes: "
            + ", ".join(unexpected)
        )
    return entries


def validate_dependencies(
    entries: dict[str, dict[str, object]],
    flavor: str,
    denied_patterns: list[re.Pattern[str]],
    allowed_exceptions: set[str],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    offenders: list[str] = []

    for lane in EXPECTED_LANES:
        raw_dependencies = entries[lane].get("dependencies")
        if not isinstance(raw_dependencies, list):
            fail(f"runtime dependency entry for {lane}/{flavor} lacks dependencies")
        counts[lane] = len(raw_dependencies)
        for index, raw_dependency in enumerate(raw_dependencies):
            label = f"runtime dependency {lane}/{flavor}[{index}]"
            if not isinstance(raw_dependency, dict):
                fail(f"{label} must be an object")
            coordinate, artifact = group_artifact(
                raw_dependency.get("coordinate"), f"{label}.coordinate"
            )
            if artifact in allowed_exceptions:
                continue
            matched = next(
                (pattern.pattern for pattern in denied_patterns if pattern.search(artifact)),
                None,
            )
            if matched is not None:
                offenders.append(
                    f"{lane}: {coordinate} (group:artifact {artifact}, "
                    f"matched {matched!r})"
                )

    if offenders:
        fail(
            f"runtime dependency policy rejected {len(offenders)} coordinate(s) "
            f"for {flavor}:\n  "
            + "\n  ".join(offenders)
        )
    return counts


def main(arguments: list[str]) -> None:
    if len(arguments) != 1 or not arguments[0]:
        fail("usage: check-runtime-dependency-graph.py <flavor>")
    flavor = arguments[0]
    root = repository_root()
    manifest = load_object(root / SCENARIO_MANIFEST, "scenario manifest")
    matrix_record = load_object(root / MATRIX_HASH_RECORD, "JVM matrix hash record")
    scenario_name, denied_patterns, allowed_exceptions = scenario_policy(
        manifest, flavor
    )
    entries = runtime_entries(matrix_record, flavor)
    counts = validate_dependencies(
        entries, flavor, denied_patterns, allowed_exceptions
    )
    count_summary = ", ".join(
        f"{lane}={counts[lane]}" for lane in EXPECTED_LANES
    )
    print(
        f"Runtime dependency graph checks passed for {scenario_name} "
        f"({flavor}): {count_summary} coordinates"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
