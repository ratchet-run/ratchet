"""Regression controls for dependency scan coverage and input provenance."""

import json
import datetime as dt
import re
import shutil
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from check_owasp_reports import check
from stage_owasp import InventoryError, NS, artifact, read_json, stage


def node(name, scope="compile", children=None, kind="jar", classifier=""):
    result = dict(groupId="example", artifactId=name, version="1.0", type=kind,
                  classifier=classifier, scope=scope, optional="false")
    if children:
        result["children"] = children
    return result


class OwaspTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "source"
        self.repository = Path(self.temp.name) / "repository"
        self.output = self.root / "target/owasp"
        self.policy = {".": "metadata", "api": "runtime", "deployment": "tooling", "it": "tests"}
        self.write_module(".", node("parent", "", kind="pom"), ["api", "deployment", "it"])
        self.write_module("api", node("api", "", [
            node("framework"), node("host-api", "provided"), node("junit", "test"),
            dict(node("optional-runtime", "runtime"), optional="true"),
        ]))
        self.write_module("deployment", node("deployment", "", [node("docker-java")]))
        self.write_module("it", node("it", "", [node("test-server")]))

    def write_artifacts(self, tree):
        _, binary, pom = artifact(tree)
        for path in (binary, pom):
            full = self.repository / path
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(str(path))
        for child in tree.get("children", []):
            self.write_artifacts(child)

    def write_module(self, path, tree, modules=()):
        directory = self.root / path
        (directory / "target").mkdir(parents=True, exist_ok=True)
        (directory / "pom.xml").write_text(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            f'<groupId>example</groupId><artifactId>{tree["artifactId"]}</artifactId>'
            f'<version>1.0</version><packaging>{tree["type"]}</packaging><modules>'
            + "".join(f"<module>{m}</module>" for m in modules) + "</modules></project>"
        )
        (directory / "target/owasp-tree.json").write_text(json.dumps(tree))
        self.write_artifacts(tree)
        if tree["type"] != "pom":
            _, binary, _ = artifact(tree)
            (directory / "target" / binary.name).write_bytes((self.repository / binary).read_bytes())

    def write_models(self):
        projects = ET.Element("projects")
        for path in self.root.glob("**/pom.xml"):
            if "target" in path.relative_to(self.root).parts:
                continue
            text = path.read_text().replace("${project.build.directory}", str(path.parent / "target"))
            text = text.replace("${baseline}", "0.9")
            project = ET.fromstring(text)
            build = project.find("m:build", NS)
            if build is None:
                build = ET.SubElement(project, f"{{{NS['m']}}}build")
            ET.SubElement(build, f"{{{NS['m']}}}directory").text = str(path.parent / "target")
            ET.SubElement(build, f"{{{NS['m']}}}finalName").text = (
                project.findtext("m:artifactId", namespaces=NS) + "-1.0")
            projects.append(project)
        ET.ElementTree(projects).write(self.root / "target/owasp-effective-pom.xml")

    def stage(self):
        self.write_models()
        return stage(self.root, self.repository, self.policy, self.output)

    def reports(self, manifest):
        for role, rows in manifest["roles"].items():
            output = self.output / "reports" / role
            output.mkdir(parents=True)
            (output / "dependency-check-report.json").write_text(json.dumps({
                "scanInfo": {"engineVersion": "13.0.0", "dataSource": [{
                    "name": "NVD API Last Checked",
                    "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                }]},
                "dependencies": [{"sha256": row["sha256"], "fileName": row["file"],
                                  "packages": [{"id": "pkg:maven/" + row["coordinate"].split(":")[0]
                                                + "/" + row["coordinate"].split(":")[1]
                                                + "@" + row["coordinate"].split(":")[4]}]}
                                 for row in rows],
            }))

    def test_separates_roles_and_keeps_provided_and_test_dependencies(self):
        manifest = self.stage()
        names = {role: {r["coordinate"].split(":")[1] for r in rows}
                 for role, rows in manifest["roles"].items()}
        self.assertEqual(names["runtime"], {"api", "framework", "host-api", "optional-runtime"})
        self.assertEqual(names["tooling"], {"deployment", "docker-java"})
        self.assertEqual(names["tests"], {"it", "junit", "test-server"})
        host = next(r for r in manifest["roles"]["runtime"] if "host-api" in r["coordinate"])
        self.assertEqual(host["origins"], [{"module": "api", "scope": "provided"}])
        self.assertTrue((self.output / "inputs/runtime" / host["pom"]).is_file())

    def test_test_scoped_descendants_stay_in_tests(self):
        self.write_module("deployment", node("deployment", "", [
            node("docker-java"),
            node("testcontainers-mongodb", "test", [
                node("testcontainers", "compile", [node("docker-java-transport-zerodep")]),
            ]),
        ]))
        manifest = self.stage()
        names = {role: {r["coordinate"].split(":")[1] for r in rows}
                 for role, rows in manifest["roles"].items()}
        self.assertEqual(names["tooling"], {"deployment", "docker-java"})
        self.assertEqual(names["tests"], {
            "it", "junit", "test-server", "testcontainers-mongodb", "testcontainers",
            "docker-java-transport-zerodep",
        })
        child = next(r for r in manifest["roles"]["tests"]
                     if r["coordinate"].split(":")[1] == "docker-java-transport-zerodep")
        self.assertEqual(child["origins"], [{"module": "deployment", "scope": "compile"}])

    def test_new_module_requires_classification(self):
        del self.policy["api"]
        with self.assertRaisesRegex(InventoryError, "Module roles differ"):
            self.stage()

    def test_missing_tree_rejected(self):
        (self.root / "api/target/owasp-tree.json").unlink()
        with self.assertRaisesRegex(InventoryError, "Dependency trees differ"):
            self.stage()
        self.assertFalse(self.output.exists())

    def test_wrong_tree_rejected(self):
        path = self.root / "api/target/owasp-tree.json"
        tree = read_json(path)
        tree["version"] = "0.9"
        path.write_text(json.dumps(tree))
        with self.assertRaisesRegex(InventoryError, "another module"):
            self.stage()

    def test_missing_metadata_rejected(self):
        (self.repository / "example/framework/1.0/framework-1.0.pom").unlink()
        with self.assertRaisesRegex(InventoryError, "Missing or external"):
            self.stage()
        self.assertFalse(self.output.exists())

    def test_stale_output_rejected(self):
        self.output.mkdir(parents=True)
        with self.assertRaisesRegex(InventoryError, "stale output"):
            self.stage()

    def test_stale_installed_project_rejected(self):
        (self.repository / "example/api/1.0/api-1.0.jar").write_text("previous build")
        with self.assertRaisesRegex(InventoryError, "Project archive differs from repository"):
            self.stage()
        self.assertFalse(self.output.exists())

    def test_unknown_scope_rejected(self):
        self.write_module("api", node("api", "", [node("extra", "system")]))
        with self.assertRaisesRegex(InventoryError, "Unreviewed Maven scope"):
            self.stage()

    def test_unknown_artifact_type_rejected(self):
        path = self.root / "api/target/owasp-tree.json"
        tree = read_json(path)
        tree["children"][0]["type"] = "zip"
        path.write_text(json.dumps(tree))
        with self.assertRaisesRegex(InventoryError, "Unreviewed Maven artifact type"):
            self.stage()

    def test_pom_cannot_be_classified_as_runtime_archive(self):
        self.policy["."] = "runtime"
        with self.assertRaisesRegex(InventoryError, "POM/module role mismatch"):
            self.stage()

    def test_metadata_dependencies_retained_in_tests(self):
        self.write_module(".", node("parent", "", [node("parent-dependency")], kind="pom"),
                          ["api", "deployment", "it"])
        result = self.stage()
        self.assertTrue(any("parent-dependency" in r["coordinate"] for r in result["roles"]["tests"]))

    def test_classifier_preserved(self):
        self.write_module("api", node("api", "", [node("fixture", "test", kind="test-jar", classifier="tests")]))
        manifest = self.stage()
        fixture = next(r for r in manifest["roles"]["tests"] if "fixture" in r["coordinate"])
        self.assertEqual(fixture["file"], "example/fixture/1.0/fixture-1.0-tests.jar")
        self.assertEqual(fixture["pom"], "example/fixture/1.0/fixture-1.0.pom")

    def copy_fixture(self):
        tree = node("legacy", kind="jar")
        tree["version"] = "0.9"
        self.write_artifacts(tree)
        _, relative, _ = artifact(tree)
        copied = self.root / "it/target/legacy" / relative.name
        copied.parent.mkdir()
        copied.write_bytes((self.repository / relative).read_bytes())
        pom = self.root / "it/pom.xml"
        pom.write_text(pom.read_text().replace("</project>", """
          <properties><baseline>0.9</baseline></properties>
          <build><plugins><plugin><artifactId>maven-dependency-plugin</artifactId>
            <executions><execution><id>legacy-fixture</id><goals><goal>copy</goal></goals>
              <configuration><outputDirectory>${project.build.directory}/legacy</outputDirectory>
                <artifactItems><artifactItem><groupId>example</groupId><artifactId>legacy</artifactId>
                  <version>${baseline}</version></artifactItem></artifactItems>
              </configuration></execution></executions></plugin></plugins></build></project>
        """))
        return copied

    def test_plugin_copied_fixture_outside_dependency_tree_included(self):
        self.copy_fixture()
        manifest = self.stage()
        fixture = next(r for r in manifest["roles"]["tests"] if "legacy" in r["coordinate"])
        self.assertEqual(fixture["coordinate"], "example:legacy:jar::0.9")
        self.assertEqual(fixture["origins"], [{"module": "it", "scope": "plugin-copy:legacy-fixture"}])

    def test_changed_plugin_copy_rejected(self):
        self.copy_fixture().write_text("different archive")
        with self.assertRaisesRegex(InventoryError, "differs from repository"):
            self.stage()

    def test_unreviewed_unpack_execution_rejected(self):
        self.copy_fixture()
        pom = self.root / "it/pom.xml"
        pom.write_text(pom.read_text().replace("<goal>copy</goal>", "<goal>unpack</goal>"))
        with self.assertRaisesRegex(InventoryError, "Unreviewed default dependency-plugin"):
            self.stage()

    def test_other_dependency_fetch_goals_rejected(self):
        self.copy_fixture()
        pom = self.root / "it/pom.xml"
        original = pom.read_text()
        for goal in ("copy-dependencies", "unpack-dependencies", "get"):
            with self.subTest(goal=goal):
                shutil.rmtree(self.output, ignore_errors=True)
                pom.write_text(original.replace("<goal>copy</goal>", f"<goal>{goal}</goal>"))
                with self.assertRaisesRegex(InventoryError, "Unreviewed default dependency-plugin"):
                    self.stage()

    def test_external_plugin_copy_rejected(self):
        self.copy_fixture()
        pom = self.root / "it/pom.xml"
        pom.write_text(pom.read_text().replace("${project.build.directory}/legacy", "/outside"))
        with self.assertRaisesRegex(InventoryError, "External dependency:copy output"):
            self.stage()

    def test_effective_inherited_copy_used_without_raw_module_execution(self):
        self.copy_fixture()
        self.write_models()
        # Simulate Maven merging a parent's execution into this effective model.
        pom = self.root / "it/pom.xml"
        pom.write_text(re.sub(r"<build>.*</build>", "", pom.read_text(), flags=re.DOTALL))
        result = stage(self.root, self.repository, self.policy, self.output)
        self.assertTrue(any("legacy" in row["coordinate"] for row in result["roles"]["tests"]))

    def test_profile_module_in_effective_reactor_requires_role(self):
        self.write_module("profile-only", node("profile-only", ""))
        with self.assertRaisesRegex(InventoryError, "Module roles differ"):
            self.stage()

    def test_unlisted_dependency_tree_rejected(self):
        extra = self.root / "unlisted/target/owasp-tree.json"
        extra.parent.mkdir(parents=True)
        extra.write_text(json.dumps(node("extra", "")))
        with self.assertRaisesRegex(InventoryError, "Dependency trees differ"):
            self.stage()

    def test_complete_reports_accepted(self):
        self.reports(self.stage())
        self.assertEqual(set(check(self.output)), {"runtime", "tooling", "tests"})

    def test_report_missing_artifact_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["dependencies"].pop()
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "absent from report"):
            check(self.output)

    def test_report_missing_maven_identity_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["dependencies"][0]["packages"] = [{"id": "pkg:maven/wrong/product@1.0"}]
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "Maven identity absent from report"):
            check(self.output)

    def test_nested_duplicate_does_not_hide_standalone_identity(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        duplicate = dict(report["dependencies"][0])
        duplicate.pop("packages")
        report["dependencies"].append(duplicate)
        path.write_text(json.dumps(report))
        self.assertEqual(set(check(self.output)), {"runtime", "tooling", "tests"})

    def test_empty_report_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["dependencies"] = []
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "Empty/incomplete"):
            check(self.output)

    def test_missing_vulnerability_database_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        del report["scanInfo"]["dataSource"]
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "Missing vulnerability database"):
            check(self.output)

    def test_stale_vulnerability_database_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["scanInfo"]["dataSource"][0]["timestamp"] = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=8)).isoformat()
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "stale NVD database"):
            check(self.output)

    def test_analyzer_exception_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["scanInfo"]["analysisExceptions"] = [{"message": "analyzer failed"}]
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "Empty/incomplete"):
            check(self.output)

    def test_future_database_timestamp_rejected(self):
        self.reports(self.stage())
        path = self.output / "reports/runtime/dependency-check-report.json"
        report = read_json(path)
        report["scanInfo"]["dataSource"][0]["timestamp"] = (
            dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).isoformat()
        path.write_text(json.dumps(report))
        with self.assertRaisesRegex(InventoryError, "NVD database check timestamp"):
            check(self.output)

    def test_changed_input_rejected(self):
        manifest = self.stage()
        self.reports(manifest)
        for key in ("file", "pom"):
            with self.subTest(key=key):
                path = self.output / "inputs/runtime" / manifest["roles"]["runtime"][0][key]
                original = path.read_bytes()
                path.write_text("changed")
                with self.assertRaisesRegex(InventoryError, "Changed scan input"):
                    check(self.output)
                path.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
