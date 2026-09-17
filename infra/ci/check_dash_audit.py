#!/usr/bin/env python3
"""Distinguish unavailable Dash audits from completed audits needing review."""

import argparse
import csv
import os
import sys
from pathlib import Path


def classify(report, exit_code):
    if exit_code:
        return (
            2,
            f"Audit unavailable: Dash exited {exit_code}. No clean license result was established.",
        )
    try:
        with report.open() as stream:
            rows = list(csv.reader(stream, skipinitialspace=True))
        if not rows or any(
            len(row) != 4
            or not all(field.strip() for field in row)
            or row[2] not in {"approved", "restricted"}
            for row in rows
        ):
            raise ValueError("empty or malformed summary")
    except (OSError, ValueError, csv.Error) as error:
        return (
            2,
            f"Audit unavailable: {error}. No clean license result was established.",
        )
    restricted = [row[0] for row in rows if row[2] == "restricted"]
    if restricted:
        return (
            1,
            f"Review required: Dash flagged {len(restricted)} of {len(rows)} dependencies. See dependencies.csv.",
        )
    return 0, f"Audit completed: Dash approved all {len(rows)} dependencies."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("exit_code", type=int)
    args = parser.parse_args()
    status, message = classify(args.report, args.exit_code)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    (args.report.parent / "status.txt").write_text(message + "\n")
    print(message)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as stream:
            stream.write(f"## Eclipse Dash license audit\n\n{message}\n")
    return status


if __name__ == "__main__":
    sys.exit(main())
