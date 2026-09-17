#!/usr/bin/env python3
"""License-policy controls; run with python3 -m unittest discover -s infra/ci."""

import copy
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from check_dependency_licenses import (
    PolicyError,
    check,
    expression_allowed,
    read_json,
    reactor_coordinates,
)

DIRECTORY = Path(__file__).resolve().parent
APACHE = [
    {"name": "Apache License, Version 2.0", "url": "https://example.org/Apache-2.0"}
]
COORDINATE = "example:library:1.0"


class PolicyTest(unittest.TestCase):
    def setUp(self):
        self.inventory = {
            "dependencies": [
                {"coordinate": COORDINATE, "licenses": copy.deepcopy(APACHE)}
            ]
        }
        self.policy = {
            "schemaVersion": 1,
            "allowedLicenses": ["Apache-2.0", "MIT"],
            "reactorLicenses": copy.deepcopy(APACHE),
            "dependencies": {
                COORDINATE: {
                    "licenses": copy.deepcopy(APACHE),
                    "expression": "Apache-2.0",
                    "evidence": ["https://example.org/library/1.0/LICENSE"],
                }
            },
        }

    def evaluate(self):
        return check(self.inventory, self.policy, set())

    def test_reviewed_dependency_passes(self):
        self.assertEqual(1, self.evaluate())

    def test_new_version_requires_review(self):
        self.inventory["dependencies"][0]["coordinate"] = "example:library:1.1"
        with self.assertRaisesRegex(PolicyError, "Unreviewed"):
            self.evaluate()

    def test_changed_metadata_requires_review(self):
        self.inventory["dependencies"][0]["licenses"][0]["url"] += "-changed"
        with self.assertRaisesRegex(PolicyError, "metadata changed"):
            self.evaluate()

    def test_missing_and_unknown_metadata_fail(self):
        for licenses in (
            [],
            [{"name": "", "url": ""}],
            [{"name": "UNKNOWN", "url": ""}],
            None,
        ):
            with self.subTest(licenses=licenses):
                self.inventory["dependencies"][0]["licenses"] = licenses
                with self.assertRaises(PolicyError):
                    self.evaluate()

    def test_missing_url_requires_matching_review_and_evidence(self):
        self.inventory["dependencies"][0]["licenses"][0]["url"] = ""
        with self.assertRaisesRegex(PolicyError, "metadata changed"):
            self.evaluate()
        self.policy["dependencies"][COORDINATE]["licenses"][0]["url"] = ""
        self.assertEqual(1, self.evaluate())
        self.policy["dependencies"][COORDINATE]["evidence"] = []
        with self.assertRaisesRegex(PolicyError, "Missing review evidence"):
            self.evaluate()

    def test_prohibited_license_fails(self):
        for expression in (
            "GPL-3.0-only",
            "LGPL-2.1-or-later",
            "AGPL-3.0-only",
            "SSPL-1.0",
            "LicenseRef-Unknown",
        ):
            with self.subTest(expression=expression):
                self.policy["dependencies"][COORDINATE]["expression"] = expression
                with self.assertRaisesRegex(PolicyError, "License policy rejects"):
                    self.evaluate()

    def test_apache_does_not_clear_a_conjunctive_gpl_requirement(self):
        self.policy["dependencies"][COORDINATE][
            "expression"
        ] = "Apache-2.0 AND GPL-2.0-only"
        with self.assertRaisesRegex(PolicyError, "License policy rejects"):
            self.evaluate()

    def test_explicit_dual_license_choice_passes(self):
        self.policy["dependencies"][COORDINATE][
            "expression"
        ] = "Apache-2.0 OR GPL-2.0-only"
        self.assertEqual(1, self.evaluate())

    def test_exception_applies_only_to_reviewed_version_and_metadata(self):
        review = self.policy["dependencies"][COORDINATE]
        review["expression"] = "LGPL-2.1-or-later"
        review["exception"] = "Reviewed separate-library integration, with evidence."
        self.assertEqual(1, self.evaluate())
        self.inventory["dependencies"][0]["coordinate"] = "example:library:1.1"
        with self.assertRaisesRegex(PolicyError, "Unreviewed"):
            self.evaluate()
        self.inventory["dependencies"][0]["coordinate"] = COORDINATE
        self.inventory["dependencies"][0]["licenses"][0]["name"] = "GPL-3.0-only"
        with self.assertRaisesRegex(PolicyError, "metadata changed"):
            self.evaluate()

    def test_empty_inventory_and_duplicate_entries_fail(self):
        self.inventory["dependencies"] *= 2
        with self.assertRaisesRegex(PolicyError, "Duplicate dependency"):
            self.evaluate()
        self.inventory["dependencies"] = []
        with self.assertRaisesRegex(PolicyError, "Empty dependency"):
            self.evaluate()

    def test_only_exact_reactor_coordinates_are_exempt(self):
        self.assertEqual(1, check(self.inventory, self.policy, {COORDINATE}))
        self.policy["dependencies"] = {}
        self.inventory["dependencies"][0]["coordinate"] = "example:library:2.0"
        with self.assertRaisesRegex(PolicyError, "Unreviewed"):
            check(self.inventory, self.policy, {COORDINATE})

    def test_reactor_license_change_fails(self):
        self.inventory["dependencies"][0]["licenses"][0]["name"] = "GPL-2.0-only"
        with self.assertRaisesRegex(PolicyError, "Reactor license metadata changed"):
            check(self.inventory, self.policy, {COORDINATE})

    def test_precedence_parentheses_and_with(self):
        allowed = {"Apache-2.0", "MIT", "GPL-2.0-only WITH Classpath-exception-2.0"}
        for expression, expected in [
            ("Apache-2.0 OR MIT AND GPL-3.0-only", True),
            ("(Apache-2.0 OR MIT) AND GPL-3.0-only", False),
            ("GPL-2.0-only", False),
            ("GPL-2.0-only WITH Classpath-exception-2.0", True),
            ("GPL-3.0-only WITH Classpath-exception-2.0", False),
            ("Apache-2.0 WITH Made-up-exception", False),
        ]:
            with self.subTest(expression=expression):
                self.assertEqual(expected, expression_allowed(expression, allowed))

    def test_malformed_alternative_is_not_short_circuited(self):
        for expression in (
            "",
            "Apache-2.0 OR",
            "GPL-3.0-only AND",
            "(MIT",
            "MIT)",
            "MIT OR @",
            "MIT MIT",
            "MIT WITH",
            "(MIT) WITH test",
        ):
            with self.subTest(expression=expression), self.assertRaises(PolicyError):
                expression_allowed(expression, {"MIT", "Apache-2.0"})

    def test_duplicate_json_keys_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text('{"dependencies": {}, "dependencies": {}}')
            with self.assertRaisesRegex(PolicyError, "Duplicate JSON key"):
                read_json(path)

    def test_checked_in_policy_expressions_and_evidence(self):
        policy = read_json(DIRECTORY / "dependency-license-policy.json")
        inventory = {
            "dependencies": [
                dict(coordinate=key, licenses=value["licenses"])
                for key, value in policy["dependencies"].items()
            ]
        }
        self.assertEqual(len(policy["dependencies"]), check(inventory, policy, set()))


class MavenWrapperTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        helper = self.root / "infra/ci"
        helper.mkdir(parents=True)
        for filename in (
            "check-dependency-licenses.sh",
            "check_dependency_licenses.py",
            "dependency-license-policy.json",
        ):
            shutil.copy(DIRECTORY / filename, helper / filename)
        self.script = helper / "check-dependency-licenses.sh"
        (self.root / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>run.ratchet</groupId><artifactId>fixture</artifactId><version>1.0</version></project>'
        )
        policy = read_json(helper / "dependency-license-policy.json")
        coordinate, review = next(iter(policy["dependencies"].items()))
        (self.root / "fixture.json").write_text(
            json.dumps(
                {
                    "dependencies": [
                        {"coordinate": coordinate, "licenses": review["licenses"]}
                    ]
                }
            )
        )
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.report = (
            self.root / "target/generated-sources/license/dependency-licenses.json"
        )
        self.report.parent.mkdir(parents=True)
        shutil.copy(self.root / "fixture.json", self.report)

    def run_wrapper(self, body, *args):
        maven = self.bin / "mvn"
        maven.write_text("#!/usr/bin/env bash\nset -eu\n" + body)
        maven.chmod(0o755)
        env = {**os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"]}
        return subprocess.run(
            ["bash", str(self.script), *args],
            cwd="/",
            env=env,
            capture_output=True,
            text=True,
        )

    def test_fresh_inventory_passes_with_offline_flag(self):
        result = self.run_wrapper(
            'printf "%s\\n" "$@" > arguments\ncp fixture.json target/generated-sources/license/dependency-licenses.json\n',
            "--offline",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        args = (self.root / "arguments").read_text().splitlines()
        for expected in (
            "--offline",
            "-Dlicense.force=true",
            "-Dlicense.failOnMissing=true",
            "-Dlicense.excludedScopes=test,provided,system",
        ):
            self.assertIn(expected, args)

    def test_maven_failure_does_not_reuse_stale_report(self):
        result = self.run_wrapper("exit 1\n")
        self.assertEqual(2, result.returncode)
        self.assertFalse(self.report.exists())

    def test_success_without_report_does_not_reuse_stale_report(self):
        result = self.run_wrapper("exit 0\n")
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self.report.exists())

    def test_unresolved_reactor_fails_even_with_valid_report(self):
        result = self.run_wrapper(
            'cp fixture.json target/generated-sources/license/dependency-licenses.json\necho "dependencies could not be resolved at this point of the build but seem to be part of the reactor"\n'
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("reactor not fully resolved", result.stderr)

    def test_malformed_report_fails(self):
        result = self.run_wrapper(
            "echo invalid > target/generated-sources/license/dependency-licenses.json\n"
        )
        self.assertNotEqual(0, result.returncode)

    def test_release_version_change_does_not_need_external_review(self):
        pom = self.root / "pom.xml"
        for version in ("1.0", "1.1", "1.2-SNAPSHOT"):
            pom.write_text(
                f'<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>run.ratchet</groupId><artifactId>fixture</artifactId><version>{version}</version></project>'
            )
            self.assertEqual(
                {f"run.ratchet:fixture:{version}"}, reactor_coordinates(self.root)
            )


if __name__ == "__main__":
    unittest.main()
