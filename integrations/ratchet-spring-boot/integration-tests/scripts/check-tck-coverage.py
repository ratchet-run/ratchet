#!/usr/bin/env python3
"""Validate Spring Boot shared-TCK coverage and reject copied TCK sources."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import NoReturn


POSTGRESQL_FLAVOR = "postgresql"
POSTGRESQL_SCENARIO = "postgresql-runtime"
CONTRACT_SOURCE_PATHS = {
    "ratchet-tck-api": Path("testing/ratchet-tck/api/src/main/java"),
    "ratchet-tck-store": Path("testing/ratchet-tck/store/src/main/java"),
    "ratchet-tck-util": Path("testing/ratchet-tck/util/src/main/java"),
}
PACKAGE_PATTERN = re.compile(
    r"^\s*package\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\s*;", re.MULTILINE
)
CONTRACT_PATTERN = re.compile(
    r"^\s*(?:public\s+)?abstract\s+class\s+(Abstract[A-Za-z0-9_]*Contract)\b",
    re.MULTILINE,
)
TCK_PACKAGE_PATTERN = re.compile(
    r"^\s*package\s+run\.ratchet\.tck(?:\s*;|\.)", re.MULTILINE
)
TYPE_DECLARATION_PATTERN = re.compile(
    r"^\s*(?:(?:public|protected|private|abstract|final|sealed|non-sealed|static)\s+)*"
    r"(?:class|interface|enum|record)\s+([A-Za-z_]\w*)\b",
    re.MULTILINE,
)


def fail(message: str) -> NoReturn:
    raise SystemExit(f"check-tck-coverage.py: {message}")


def repository_root() -> Path:
    script = Path(__file__).resolve()
    for candidate in script.parents:
        if all((candidate / source).is_dir() for source in CONTRACT_SOURCE_PATHS.values()):
            return candidate
    fail(f"cannot locate repository root from {script}")


def read_text(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        fail(f"cannot read {label} {path}: {exc}")


def discover_contracts(root: Path) -> dict[str, tuple[str, str]]:
    contracts: dict[str, tuple[str, str]] = {}
    for source_artifact, relative_source in CONTRACT_SOURCE_PATHS.items():
        source_root = root / relative_source
        for source in sorted(source_root.rglob("Abstract*Contract.java")):
            text = read_text(source, "contract source")
            package_matches = PACKAGE_PATTERN.findall(text)
            contract_matches = CONTRACT_PATTERN.findall(text)
            relative = source.relative_to(root)
            if len(package_matches) != 1:
                fail(
                    f"expected one package declaration in {relative}; "
                    f"found {len(package_matches)}"
                )
            if len(contract_matches) != 1:
                fail(
                    f"expected one abstract contract declaration in {relative}; "
                    f"found {len(contract_matches)}"
                )
            class_name = contract_matches[0]
            if class_name != source.stem:
                fail(
                    f"contract filename/declaration mismatch in {relative}: "
                    f"{source.stem} != {class_name}"
                )
            qualified_name = f"{package_matches[0]}.{class_name}"
            key = f"{source_artifact}:{qualified_name}"
            if key in contracts:
                fail(f"duplicate shared contract declaration: {key}")
            contracts[key] = (source_artifact, class_name)
    return contracts


def load_coverage(path: Path) -> dict[str, object]:
    try:
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot parse coverage manifest {path}: {exc}")
    if not isinstance(document, dict):
        fail("coverage manifest must contain a JSON object")
    if document.get("schemaVersion") != 1:
        fail("coverage manifest schemaVersion must be 1")
    return document


def validate_coverage(
    document: dict[str, object], discovered: dict[str, tuple[str, str]]
) -> tuple[int, int]:
    contract_sources = document.get("contractSources")
    if contract_sources != list(CONTRACT_SOURCE_PATHS):
        fail(
            "contractSources must list ratchet-tck-api, ratchet-tck-store, "
            "and ratchet-tck-util in canonical order"
        )

    entries = document.get("contracts")
    if not isinstance(entries, dict):
        fail("coverage manifest contracts must be a JSON object")

    declared_keys = set(entries)
    discovered_keys = set(discovered)
    missing = sorted(discovered_keys - declared_keys)
    stale = sorted(declared_keys - discovered_keys)
    if missing:
        fail("shared contracts missing coverage entries: " + ", ".join(missing))
    if stale:
        fail("coverage entries name nonexistent contracts: " + ", ".join(stale))

    covered = 0
    unsupported = 0
    for key in sorted(discovered):
        source_artifact, _ = discovered[key]
        entry = entries[key]
        if not isinstance(entry, dict):
            fail(f"coverage entry must be an object: {key}")
        if entry.get("sourceArtifact") != source_artifact:
            fail(
                f"coverage entry sourceArtifact does not match its key: {key}"
            )
        flavors = entry.get("flavors")
        if not isinstance(flavors, dict):
            fail(f"coverage entry flavors must be an object: {key}")
        flavor = flavors.get(POSTGRESQL_FLAVOR)
        if not isinstance(flavor, dict):
            fail(f"coverage entry lacks {POSTGRESQL_FLAVOR} flavor: {key}")

        status = flavor.get("status")
        scenarios = flavor.get("scenarios")
        if not isinstance(scenarios, list) or not all(
            isinstance(scenario, str) and scenario for scenario in scenarios
        ):
            fail(f"{POSTGRESQL_FLAVOR} scenarios must be an array of names: {key}")
        if len(scenarios) != len(set(scenarios)):
            fail(f"{POSTGRESQL_FLAVOR} scenarios contain duplicates: {key}")

        expected_status = "covered" if source_artifact == "ratchet-tck-api" else "unsupported"
        if status != expected_status:
            fail(
                f"{POSTGRESQL_FLAVOR} status for {key} must be "
                f"{expected_status!r}; found {status!r}"
            )
        if status == "covered":
            if scenarios != [POSTGRESQL_SCENARIO]:
                fail(
                    f"covered {POSTGRESQL_FLAVOR} contract must name only "
                    f"{POSTGRESQL_SCENARIO}: {key}"
                )
            covered += 1
        else:
            rationale = flavor.get("rationale")
            if not isinstance(rationale, str) or not rationale.strip():
                fail(f"unsupported contract lacks a rationale: {key}")
            if scenarios:
                fail(f"unsupported contract must not name scenarios: {key}")
            unsupported += 1
    return covered, unsupported


def validate_no_tck_duplication(
    root: Path, contract_class_names: set[str]
) -> None:
    spring_root = root / "integrations/ratchet-spring-boot"
    violations: list[str] = []
    for source in sorted(spring_root.rglob("*.java")):
        if "target" in source.parts:
            continue
        text = read_text(source, "Spring source")
        relative = source.relative_to(root)
        if TCK_PACKAGE_PATTERN.search(text):
            violations.append(f"{relative}: declares package run.ratchet.tck")
        for class_name in TYPE_DECLARATION_PATTERN.findall(text):
            if class_name in contract_class_names:
                violations.append(
                    f"{relative}: redeclares shared contract class {class_name}"
                )
    if violations:
        fail("copied TCK source detected:\n  " + "\n  ".join(violations))


def main() -> None:
    root = repository_root()
    coverage_path = (
        root
        / "integrations/ratchet-spring-boot/integration-tests/tck-coverage.json"
    )
    discovered = discover_contracts(root)
    document = load_coverage(coverage_path)
    covered, unsupported = validate_coverage(document, discovered)
    validate_no_tck_duplication(
        root, {class_name for _, class_name in discovered.values()}
    )
    print(
        "Spring Boot TCK coverage checks passed: "
        f"{len(discovered)} contracts "
        f"({covered} covered, {unsupported} unsupported)"
    )


if __name__ == "__main__":
    main()
