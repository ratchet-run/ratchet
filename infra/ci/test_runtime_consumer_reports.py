import tempfile
import unittest
from pathlib import Path
import xml.etree.ElementTree as ET

from check_runtime_consumer_reports import MONGO_EXPECTED, SQL_EXPECTED, validate


def write_suite(directory: Path, class_name: str, tests: int, *, failures: int = 0, errors: int = 0, skipped: int = 0) -> None:
    root = ET.Element(
        "testsuite",
        name=f"example.runtime.{class_name}",
        tests=str(tests),
        failures=str(failures),
        errors=str(errors),
        skipped=str(skipped),
    )
    for index in range(tests):
        case = ET.SubElement(root, "testcase", classname=f"example.runtime.{class_name}", name=f"case{index}")
        if index < skipped:
            ET.SubElement(case, "skipped")
        elif index < skipped + failures:
            ET.SubElement(case, "failure")
        elif index < skipped + failures + errors:
            ET.SubElement(case, "error")
    ET.ElementTree(root).write(directory / f"TEST-{class_name}.xml", encoding="unicode")


def populate(sql: Path, mongo: Path, *, mutate=None) -> None:
    for name, count in SQL_EXPECTED.items():
        write_suite(sql, name, count)
    for name, count in MONGO_EXPECTED.items():
        write_suite(mongo, name, count)
    if mutate:
        mutate(sql, mongo)


class RuntimeConsumerReportGuardTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.sql = root / "sql" / "target" / "failsafe-reports"
        self.mongo = root / "mongodb" / "target" / "failsafe-reports"
        self.sql.mkdir(parents=True)
        self.mongo.mkdir(parents=True)

    def tearDown(self):
        self.temp.cleanup()

    def test_valid_sql_and_mongodb_matrix_is_exactly_24(self):
        populate(self.sql, self.mongo)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(sum(SQL_EXPECTED.values()), 20)
        self.assertEqual(sum(MONGO_EXPECTED.values()), 4)

    def test_missing_class_fails(self):
        def mutate(sql, mongo):
            (sql / "TEST-WebAndClusterRuntimeIT.xml").unlink()
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertIn("WebAndClusterRuntimeIT", result["modules"]["sql"]["missing_classes"])

    def test_empty_class_fails(self):
        def mutate(sql, mongo):
            write_suite(sql, "DatabaseFailuresRuntimeIT", 0)
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["modules"]["sql"]["mismatches"])

    def test_unexpected_skip_fails(self):
        def mutate(sql, mongo):
            write_suite(mongo, "MongoProcessesRuntimeIT", 2, skipped=1)
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["modules"]["mongodb"]["mismatches"])

    def test_failure_or_error_fails(self):
        def mutate(sql, mongo):
            write_suite(sql, "SecurityOverridesRuntimeIT", 1, failures=1)
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["modules"]["sql"]["mismatches"])

    def test_malformed_report_fails(self):
        def mutate(sql, mongo):
            (mongo / "TEST-MongoProcessesRuntimeIT.xml").write_text("<testsuite>")
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["modules"]["mongodb"]["parse_errors"])

    def test_unexpected_class_fails(self):
        def mutate(sql, mongo):
            write_suite(mongo, "UnexpectedRuntimeIT", 1)
        populate(self.sql, self.mongo, mutate=mutate)
        result = validate(self.sql, self.mongo)
        self.assertEqual(result["status"], "failed")
        self.assertIn("UnexpectedRuntimeIT", result["modules"]["mongodb"]["unexpected_classes"])

    def test_missing_module_fails(self):
        populate(self.sql, self.mongo)
        result = validate(self.sql, self.mongo.parent / "missing")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["modules"]["mongodb"]["report_file_count"], 0)


if __name__ == "__main__":
    unittest.main()
