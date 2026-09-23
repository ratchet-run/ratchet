#!/usr/bin/env python3
"""Validate the Spring runtime consumer Failsafe report matrix.

The runtime lane intentionally runs both consumer modules from the independent
reactor.  A successful Maven exit can still leave a missing or filtered suite,
so this guard checks the exact source-backed class/test-count contract.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

MANIFEST_PATH = Path(__file__).with_name("runtime-consumer-expected.json")
MANIFEST = json.loads(MANIFEST_PATH.read_text())
SQL_EXPECTED = {str(name): int(count) for name, count in MANIFEST["sql"].items()}
MONGO_EXPECTED = {str(name): int(count) for name, count in MANIFEST["mongodb"].items()}


def simple_name(value: str | None) -> str:
    return (value or "").rsplit(".", 1)[-1].split("$", 1)[0]


def _suite_rows(path: Path) -> list[dict[str, object]]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    rows = []
    for suite in suites:
        rows.append(
            {
                "file": str(path),
                "class": simple_name(suite.get("name")),
                "tests": int(suite.get("tests") or 0),
                "failures": int(suite.get("failures") or 0),
                "errors": int(suite.get("errors") or 0),
                "skipped": int(suite.get("skipped") or 0),
            }
        )
    return rows


def inspect_report_dir(report_dir: Path, expected: dict[str, int]) -> dict[str, object]:
    files = sorted(report_dir.glob("TEST-*.xml")) if report_dir.is_dir() else []
    rows: list[dict[str, object]] = []
    parse_errors: list[dict[str, str]] = []
    for path in files:
        try:
            rows.extend(_suite_rows(path))
        except (ET.ParseError, OSError, ValueError) as exc:
            parse_errors.append({"file": str(path), "error": str(exc)})

    actual: dict[str, dict[str, int]] = {}
    for row in rows:
        name = str(row["class"])
        totals = actual.setdefault(name, {"tests": 0, "failures": 0, "errors": 0, "skipped": 0})
        for key in totals:
            totals[key] += int(row[key])

    expected_names = set(expected)
    actual_names = set(actual)
    missing = sorted(expected_names - actual_names)
    unexpected = sorted(actual_names - expected_names)
    mismatches = []
    for name in sorted(expected_names & actual_names):
        if actual[name]["tests"] != expected[name]:
            mismatches.append({"class": name, "expected_tests": expected[name], "actual_tests": actual[name]["tests"]})
        if any(actual[name][key] for key in ("failures", "errors", "skipped")):
            mismatches.append({"class": name, "quality": {key: actual[name][key] for key in ("failures", "errors", "skipped")}})

    status = "passed" if files and not parse_errors and not missing and not unexpected and not mismatches else "failed"
    return {
        "report_dir": str(report_dir),
        "report_file_count": len(files),
        "suite_count": len(rows),
        "expected": expected,
        "actual": actual,
        "missing_classes": missing,
        "unexpected_classes": unexpected,
        "mismatches": mismatches,
        "parse_errors": parse_errors,
        "status": status,
    }


def validate(sql_report_dir: Path, mongo_report_dir: Path) -> dict[str, object]:
    modules = {
        "sql": inspect_report_dir(sql_report_dir, SQL_EXPECTED),
        "mongodb": inspect_report_dir(mongo_report_dir, MONGO_EXPECTED),
    }
    problems = []
    for module, result in modules.items():
        if result["status"] != "passed":
            problems.append(module)
    return {"status": "passed" if not problems else "failed", "modules": modules, "failed_modules": problems}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-report-dir", type=Path, required=True)
    parser.add_argument("--mongo-report-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    result = validate(args.sql_report_dir, args.mongo_report_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
