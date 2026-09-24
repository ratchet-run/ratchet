#!/usr/bin/env python3
"""Reject incomplete OWASP reports and bind results to staged artifact hashes."""

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from urllib.parse import unquote

from stage_owasp import InventoryError, ROLES, digest, read_json


def check(directory):
    manifest = read_json(directory / "manifest.json")
    if manifest["schemaVersion"] != 1 or set(manifest["roles"]) != set(ROLES):
        raise InventoryError("Invalid OWASP input manifest")
    summary = {}
    for role in ROLES:
        report_file = directory / "reports" / role / "dependency-check-report.json"
        report = read_json(report_file)
        dependencies = report["dependencies"]
        if not dependencies or report["scanInfo"].get("analysisExceptions"):
            raise InventoryError(f"Empty/incomplete OWASP report: {role}")
        data_sources = report["scanInfo"].get("dataSource")
        if not isinstance(data_sources, list) or not data_sources:
            raise InventoryError(f"Missing vulnerability database metadata: {role}")
        checked = []
        for source in data_sources:
            timestamp = dt.datetime.fromisoformat(source["timestamp"].replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                raise InventoryError(f"Missing database timestamp timezone: {role}")
            if source["name"].startswith("NVD ") and source["name"].endswith("Last Checked"):
                checked.append(timestamp)
        age = dt.datetime.now(dt.timezone.utc) - max(checked) if checked else None
        if age is None or age > dt.timedelta(days=7) or age < -dt.timedelta(hours=1):
            raise InventoryError(f"Missing/stale NVD database check timestamp: {role}")
        reported = {item.get("sha256") for item in dependencies}
        identities = {}
        for item in dependencies:
            identities.setdefault(item.get("sha256"), set()).update(
                unquote(package["id"].split("?", 1)[0]) for package in item.get("packages", [])
            )
        records = manifest["roles"][role]
        if not records:
            raise InventoryError(f"Empty input manifest: {role}")
        for record in records:
            for key, hash_key in (("file", "sha256"), ("pom", "pomSha256")):
                staged = directory / "inputs" / role / record[key]
                if digest(staged) != record[hash_key]:
                    raise InventoryError(f"Changed scan input: {role}: {record[key]}")
            if not record["file"].endswith(".pom") and record["sha256"] not in reported:
                raise InventoryError(f"Artifact absent from report: {role}: {record['file']}")
            if not record["file"].endswith(".pom"):
                group, name, _, _, version = record["coordinate"].split(":")
                expected = f"pkg:maven/{group}/{name}@{version}"
                if expected not in identities.get(record["sha256"], set()):
                    raise InventoryError(f"Maven identity absent from report: {role}: {expected}")
        findings = {}
        for dependency in dependencies:
            for vulnerability in dependency.get("vulnerabilities", []):
                findings.setdefault(vulnerability["name"], set()).add(dependency["fileName"])
        summary[role] = {
            "reportSha256": digest(report_file),
            "engineVersion": report["scanInfo"]["engineVersion"],
            "dataSource": data_sources,
            "inputArtifacts": len(records),
            "reportedDependencies": len(dependencies),
            "findings": {name: sorted(files) for name, files in sorted(findings.items())},
        }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        result = check(args.directory)
        for role, row in result.items():
            print(f"{role}: {row['inputArtifacts']} inputs, {len(row['findings'])} advisory IDs")
    except (InventoryError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"OWASP evidence failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
