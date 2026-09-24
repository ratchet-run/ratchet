"""Keep genuine vulnerabilities visible when correcting product identity matches."""

import json
import re
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


class SuppressionTest(unittest.TestCase):
    def test_product_identity_controls(self):
        root = Path(__file__).resolve().parents[2]
        ns = {"s": "https://jeremylong.github.io/DependencyCheck/dependency-suppression.1.4.xsd"}
        rules = ET.parse(root / "owasp-suppressions.xml").getroot()
        cases = json.loads(Path(__file__).with_name("owasp-suppression-controls.json").read_text())["cases"]
        self.assertEqual(len(cases), 21)
        self.assertEqual(len({(p, c) for p, c, _ in cases}), len(cases))
        # This fast CI guard intentionally rejects selector forms it cannot
        # model. Extending the policy then requires controls for that form,
        # rather than silently treating an unknown suppression as harmless.
        for rule in rules:
            self.assertFalse(rule.attrib, "Review conditional/base suppression semantics")
            self.assertEqual(len(rule.findall("s:packageUrl", ns)) + len(rule.findall("s:filePath", ns)),
                             1, "Each suppression must have exactly one reviewed package or file selector")
            self.assertTrue({item.tag.split("}")[-1] for item in rule} <= {
                "notes", "packageUrl", "filePath", "cve", "cpe",
            }, "Unmodeled suppression element")
            path = rule.find("s:filePath", ns)
            if path is not None:
                self.assertIn(path.text, {
                    r".*/protobuf-java-[^/]+\.jar$",
                    r".*/micrometer-registry-prometheus-[^/]+\.jar$",
                }, "New file-path suppression needs its own identity controls")
            for item in rule:
                self.assertTrue(set(item.attrib) <= {"regex", "caseSensitive"})

        def matches(element, value):
            case_sensitive = element.get("caseSensitive", "false") == "true"
            if element.get("regex") == "true":
                flags = 0 if case_sensitive else re.IGNORECASE
                return re.fullmatch(element.text, value, flags) is not None
            return element.text == value if case_sensitive else element.text.lower() == value.lower()

        for package_url, cve, expected in cases:
            with self.subTest(package_url=package_url, cve=cve):
                suppressed = False
                for rule in rules:
                    package = rule.find("s:packageUrl", ns)
                    if package is None:
                        continue
                    if matches(package, package_url):
                        # Conservatively reject any matching CPE-level rule on
                        # these identities: it can remove findings before CVE
                        # suppression runs. The real DC parser is also exercised
                        # during security qualification.
                        if rule.find("s:cpe", ns) is not None or any(
                            matches(item, cve) for item in rule.findall("s:cve", ns)
                        ):
                            suppressed = True
                self.assertEqual(suppressed, expected)


if __name__ == "__main__":
    unittest.main()
