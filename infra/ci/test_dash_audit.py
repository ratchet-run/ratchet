#!/usr/bin/env python3
"""A service failure must never look like a clean license audit."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_dash_audit import classify


class DashAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.report = self.root / "dependencies.csv"
        self.report.write_text(
            "maven/mavencentral/example/library/1.0, Apache-2.0, approved, clearlydefined\n"
        )

    def test_completed_approved_audit_passes(self):
        status, message = classify(self.report, 0)
        self.assertEqual(0, status)
        self.assertIn("Audit completed", message)

    def test_review_needed_is_a_finding(self):
        with self.report.open("a") as stream:
            stream.write(
                "maven/mavencentral/example/review/1.0, unknown, restricted, none\n"
            )
        status, message = classify(self.report, 0)
        self.assertEqual(1, status)
        self.assertIn("Review required", message)
        self.assertIn("1 of 2", message)

    def test_timeouts_and_process_failures_are_unavailable_even_with_partial_report(
        self,
    ):
        for exit_code in (1, 2, 124, 137):
            with self.subTest(exit_code=exit_code):
                status, message = classify(self.report, exit_code)
                self.assertEqual(2, status)
                self.assertIn("Audit unavailable", message)

    def test_missing_report_is_unavailable(self):
        self.report.unlink()
        self.assertEqual(2, classify(self.report, 0)[0])

    def test_empty_or_malformed_report_is_unavailable(self):
        for contents in (
            "",
            "id, Apache-2.0, approved\n",
            "id, Apache-2.0, unexpected, source\n",
            "id, , approved, source\n",
        ):
            with self.subTest(contents=contents):
                self.report.write_text(contents)
                self.assertEqual(2, classify(self.report, 0)[0])

    def test_command_writes_failure_status_and_actions_summary(self):
        summary = self.root / "summary.md"
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("check_dash_audit.py")),
                str(self.report),
                "124",
            ],
            env={**os.environ, "GITHUB_STEP_SUMMARY": str(summary)},
            capture_output=True,
            text=True,
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("Audit unavailable", summary.read_text())
        self.assertIn("Audit unavailable", (self.root / "status.txt").read_text())


if __name__ == "__main__":
    unittest.main()
