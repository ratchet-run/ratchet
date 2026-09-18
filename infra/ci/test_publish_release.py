import unittest

from publish_release import expected_artifacts, validate_bundle, validate_run


class PublicationChecksTest(unittest.TestCase):
    def setUp(self):
        self.run = {
            "conclusion": "success", "status": "completed", "event": "workflow_dispatch",
            "head_branch": "main", "path": ".github/workflows/release.yml",
            "repository": {"full_name": "ratchet-run/ratchet"},
        }
        self.bundle = {
            "deploymentId": "deployment", "deploymentState": "VALIDATED",
            "purls": ["pkg:maven/run.ratchet/ratchet@0.4.0",
                      "pkg:maven/run.ratchet/ratchet-api@0.4.0?classifier=sources"],
        }
        self.artifacts = {"ratchet", "ratchet-api"}

    def test_accepts_successful_main_release(self):
        validate_run(self.run, "ratchet-run/ratchet")

    def test_rejects_unverified_or_unrelated_source_run(self):
        for key, value in {
            "conclusion": "failure", "status": "in_progress", "event": "pull_request",
            "head_branch": "feature", "path": ".github/workflows/snapshot.yml",
            "repository": {"full_name": "other/repo"},
        }.items():
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_run(self.run | {key: value}, "ratchet-run/ratchet")

    def test_accepts_exact_coordinates_and_resumable_states(self):
        for state in ("VALIDATED", "PUBLISHING", "PUBLISHED"):
            with self.subTest(state=state):
                validate_bundle(self.bundle | {"deploymentState": state}, "deployment", "0.4.0", self.artifacts)

    def test_rejects_different_deployment(self):
        with self.assertRaises(ValueError):
            validate_bundle(self.bundle, "another", "0.4.0", self.artifacts)

    def test_rejects_unready_deployment(self):
        for state in ("PENDING", "VALIDATING", "FAILED"):
            with self.subTest(state=state), self.assertRaises(ValueError):
                validate_bundle(self.bundle | {"deploymentState": state}, "deployment", "0.4.0", self.artifacts)

    def test_rejects_missing_extra_or_wrong_version_coordinates(self):
        for purls in (
            [], ["pkg:maven/run.ratchet/ratchet@0.4.0"],
            self.bundle["purls"] + ["pkg:maven/run.ratchet/extra@0.4.0"],
            ["pkg:maven/run.ratchet/ratchet@0.3.1", "pkg:maven/run.ratchet/ratchet-api@0.4.0"],
            ["pkg:maven/other/ratchet@0.4.0", "pkg:maven/run.ratchet/ratchet-api@0.4.0"],
        ):
            with self.subTest(purls=purls), self.assertRaises(ValueError):
                validate_bundle(self.bundle | {"purls": purls}, "deployment", "0.4.0", self.artifacts)

    def test_bom_deduplicates_classifiers_and_ignores_external_dependencies(self):
        bom = '''<project xmlns="http://maven.apache.org/POM/4.0.0">
          <dependencyManagement><dependencies>
            <dependency><groupId>run.ratchet</groupId><artifactId>ratchet</artifactId></dependency>
            <dependency><groupId>run.ratchet</groupId><artifactId>ratchet</artifactId><classifier>tests</classifier></dependency>
            <dependency><groupId>other</groupId><artifactId>external</artifactId></dependency>
          </dependencies></dependencyManagement></project>'''
        self.assertEqual(expected_artifacts(bom), {
            "ratchet", "ratchet-parent", "ratchet-bom", "ratchet-quarkus-parent", "ratchet-tck",
        })

    def test_rejects_empty_bom(self):
        with self.assertRaises(ValueError):
            expected_artifacts('<project xmlns="http://maven.apache.org/POM/4.0.0"/>')


if __name__ == "__main__":
    unittest.main()
