import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from affected_lanes import (
    BOOT, COVERAGE, DATABASES, Graph, JAVA, LANES, SERVERS, STORE_MODULES,
    TESTSUITE, classify, read_pom, reduce_rows, row_model, select,
)

REPO = Path(__file__).resolve().parents[2]


class GraphFixtureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.pom('.', 'root', '<modules><module>aggregator</module><module>dep</module><module>bom</module></modules>')
        self.pom('aggregator', 'different-artifact', '<modules><module>child</module></modules>')
        self.pom('aggregator/child', 'child', '<parent><artifactId>different-artifact</artifactId></parent>')
        self.pom('dep', 'dep')
        self.pom('bom', 'bom')

    def pom(self, directory, artifact, body='', namespace=True):
        path = self.root / directory / 'pom.xml'
        path.parent.mkdir(parents=True, exist_ok=True)
        ns = ' xmlns="http://maven.apache.org/POM/4.0.0"' if namespace else ''
        path.write_text(f'<project{ns}><artifactId>{artifact}</artifactId>{body}</project>')

    def child(self, body):
        self.pom('aggregator/child', 'child', body, namespace=False)
        return Graph(self.root)

    def test_parent_edge_uses_artifact_not_directory(self):
        self.assertEqual(Graph(self.root).closure(['aggregator/child']), {'aggregator/child', 'aggregator'})

    def test_profiles_default_explicit_and_property(self):
        graph = self.child('''<profiles>
          <profile><id>default</id><activation><activeByDefault>true</activeByDefault></activation>
            <dependencies><dependency><artifactId>dep</artifactId></dependency></dependencies></profile>
          <profile><id>explicit</id><dependencies><dependency><artifactId>bom</artifactId></dependency></dependencies></profile>
          <profile><id>property</id><activation><property><name>flag</name></property></activation>
            <dependencies><dependency><artifactId>different-artifact</artifactId></dependency></dependencies></profile>
          <profile><id>inactive</id><dependencies><dependency><artifactId>child</artifactId></dependency></dependencies></profile>
        </profiles>''')
        self.assertEqual(graph.edges('aggregator/child', set()), {'dep', 'aggregator'})
        self.assertEqual(graph.edges('aggregator/child', {'explicit'}), {'bom', 'aggregator'})
        self.assertEqual(graph.edges('aggregator/child', {'elsewhere'}), {'dep', 'aggregator'})

    def test_test_jar_optional_dependency_is_edge(self):
        graph = self.child('''<dependencies><dependency><artifactId>dep</artifactId>
          <scope>test</scope><type>test-jar</type><classifier>tests</classifier><optional>true</optional>
          </dependency></dependencies>''')
        self.assertEqual(graph.edges('aggregator/child', set()), {'dep'})
        self.assertEqual(graph.downstream({'dep'}), {'dep', 'aggregator/child'})

    def test_bom_import_only(self):
        graph = self.child('''<dependencyManagement><dependencies>
          <dependency><artifactId>bom</artifactId><scope>import</scope></dependency>
          <dependency><artifactId>dep</artifactId></dependency>
          </dependencies></dependencyManagement>''')
        self.assertEqual(graph.edges('aggregator/child', set()), {'bom'})

    def test_plugins_dependencies_extensions_and_no_plugin_management(self):
        graph = self.child('''<build><plugins><plugin><artifactId>bom</artifactId><dependencies>
          <dependency><artifactId>dep</artifactId></dependency></dependencies></plugin></plugins>
          <extensions><extension><artifactId>different-artifact</artifactId></extension></extensions>
          <pluginManagement><plugins><plugin><artifactId>child</artifactId></plugin></plugins></pluginManagement>
          </build>''')
        self.assertEqual(graph.edges('aggregator/child', set()), {'bom', 'dep', 'aggregator'})

    def test_profile_build_edges(self):
        graph = self.child('''<profiles><profile><id>build</id><build><plugins><plugin>
          <artifactId>dep</artifactId></plugin></plugins></build></profile></profiles>''')
        self.assertEqual(graph.edges('aggregator/child', set()), set())
        self.assertEqual(graph.edges('aggregator/child', {'build'}), {'dep'})

    def test_profile_modules_discovered(self):
        self.pom('dep', 'dep', '<profiles><profile><id>extra</id><modules><module>extra</module></modules></profile></profiles>')
        self.pom('dep/extra', 'extra')
        self.assertIn('dep/extra', Graph(self.root).poms)

    def test_unresolved_artifact_fails(self):
        self.pom('dep', '${artifact}')
        with self.assertRaisesRegex(ValueError, 'Unresolved reactor artifactId.*dep/pom.xml'):
            Graph(self.root)
        result = subprocess.run([sys.executable, str(REPO / 'infra/ci/affected_lanes.py'),
                                 '--repo-root', str(self.root), '--full'], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Unresolved reactor artifactId', result.stderr)

    def test_classification(self):
        graph = Graph(self.root)
        cases = [
            (['aggregator/child/src/X.java'], {'aggregator/child'}, set(), False, True, False),
            (['README.md', 'x.adoc', 'LICENSE', 'website/x.js'], set(), set(), False, False, True),
            (['.github/workflows/codeql.yml', 'scripts/x', 'infra/x', 'examples/x', 'src/main/javadoc/x',
              '.githooks/x', 'qodana.yaml', 'qodana.sarif.json', 'owasp-suppressions.xml',
              'spotbugs-include.xml', '.gitignore', '.dockerignore', 'NOTICE'], set(), set(), False, True, False),
            (['pom.xml', '.mvn/x', '.github/actions/x', '.github/workflows/ci.yml', 'infra/ci/affected_lanes.py'],
             set(), set(), True, True, False),
            (['unknown.txt'], set(), set(), True, True, False),
            ([], set(), set(), True, False, False),
            (['integrations/ratchet-spring-boot/consumer-tests/sql/pom.xml'], set(),
             {'spring_consumer', 'spring_native', 'spring_runtime'}, False, True, False),
            (['infra/ci/runtime-consumer-expected.json'], set(), {'spring_runtime'}, False, True, False),
        ]
        for paths, changed, triggers, global_change, code, web in cases:
            with self.subTest(paths=paths):
                actual = classify(graph, paths)
                self.assertEqual((actual[0], actual[1], bool(actual[2]), actual[3], actual[4]),
                                 (changed, triggers, global_change, code, web))

    def test_diagonal_and_two_groups(self):
        rows = reduce_rows('integration', LANES['integration'])
        self.assertEqual(rows, [{'server': server, 'database': DATABASES[i % 5]} for i, server in enumerate(SERVERS)])
        consumer = reduce_rows('spring_consumer', LANES['spring_consumer'])
        self.assertEqual(consumer, [dict(java=JAVA[i % 2], boot=BOOT[i % 2], store=store, provider='default')
                                    for i, store in enumerate(DATABASES)] +
                         [dict(java=java, boot=boot, store='postgresql', provider='eclipselink')
                          for java, boot in zip(JAVA, BOOT)])
        for lane, full in LANES.items():
            reduced = reduce_rows(lane, full)
            self.assertTrue(all(row in full for row in reduced))
            for key in full[0]:
                self.assertEqual({r[key] for r in full}, {r[key] for r in reduced})
        for lane in ['quarkus_jvm', 'quarkus_native']:
            self.assertEqual(reduce_rows(lane, LANES[lane]), LANES[lane])


class RealRepoTest(unittest.TestCase):
    def output(self, *paths):
        return select(REPO, list(paths))[0]

    def rows(self, outputs, lane):
        return json.loads(outputs[lane])

    def counts(self, outputs, expected):
        for lane in LANES:
            with self.subTest(lane=lane):
                self.assertEqual(len(self.rows(outputs, lane)), expected.get(lane, 0))
                self.assertEqual(outputs['has_' + lane], str(bool(expected.get(lane, 0))).lower())
                self.assertTrue(all(r in LANES[lane] for r in self.rows(outputs, lane)))

    def reduced_all(self, outputs):
        self.counts(outputs, dict(integration=7, spring_consumer=7, spring_native=5,
                                 spring_runtime=2, quarkus_jvm=5, quarkus_native=2))
        self.assertEqual(outputs['has_showcase'], 'true')

    def test_full(self):
        outputs = select(REPO)[0]
        self.counts(outputs, dict(integration=35, spring_consumer=24, spring_native=10,
                                 spring_runtime=4, quarkus_jvm=5, quarkus_native=2))
        for key in ['code', 'web', 'has_showcase']:
            self.assertEqual(outputs[key], 'true')
        for prefix in ['unit', 'ee11']:
            self.assertEqual(outputs[prefix + '_scope'], 'all')
            self.assertEqual(outputs[prefix + '_modules'], '')
        self.assertEqual(outputs['mode'], 'full')

    def test_core(self):
        outputs = self.output('ratchet/src/main/java/run/ratchet/Foo.java')
        self.reduced_all(outputs)
        self.assertEqual({r['server'] for r in self.rows(outputs, 'integration')}, set(SERVERS))
        self.assertEqual({r['database'] for r in self.rows(outputs, 'integration')}, set(DATABASES))
        self.assertTrue(outputs['unit_scope'] == 'all' or 'ratchet' in outputs['unit_modules'].split(','))

    def test_mysql(self):
        outputs = self.output('stores/ratchet-store-mysql/src/main/java/Foo.java')
        self.counts(outputs, dict(integration=7, quarkus_jvm=1, spring_consumer=2, spring_native=2))
        self.assertEqual(self.rows(outputs, 'integration'), [{'server': server, 'database': 'mysql'} for server in SERVERS])
        self.assertEqual(self.rows(outputs, 'quarkus_jvm'), [{'db': 'mysql', 'profile': 'db-mysql'}])
        self.assertEqual(self.rows(outputs, 'spring_consumer'),
                         [dict(java=java, boot=boot, store='mysql', provider='default') for java, boot in zip(JAVA, BOOT)])
        self.assertEqual(self.rows(outputs, 'spring_native'), [dict(boot=boot, store='mysql') for boot in BOOT])
        self.assertEqual(outputs['has_showcase'], 'false')
        modules = outputs['unit_modules'].split(',')
        self.assertIn(STORE_MODULES['mysql'], modules)
        self.assertIn('integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa', modules)
        self.assertNotIn('ratchet', modules)
        self.assertNotIn(STORE_MODULES['postgresql'], modules)

    def test_mongodb(self):
        outputs = self.output('stores/ratchet-store-mongodb/src/main/java/Foo.java')
        self.assertEqual(self.rows(outputs, 'integration'), [{'server': server, 'database': 'mongodb'} for server in SERVERS])
        self.assertEqual(outputs['has_spring_runtime'], 'true')
        self.assertEqual(self.rows(outputs, 'quarkus_native'), [LANES['quarkus_native'][0]])

    def test_quarkus_mongodb(self):
        outputs = self.output('integrations/ratchet-quarkus/mongodb/src/main/java/Foo.java')
        self.counts(outputs, dict(quarkus_jvm=1, quarkus_native=1))
        self.assertEqual(self.rows(outputs, 'quarkus_jvm'), [LANES['quarkus_jvm'][-1]])
        self.assertEqual(self.rows(outputs, 'quarkus_native'), [LANES['quarkus_native'][0]])
        self.assertEqual(outputs['has_showcase'], 'false')

    def test_consumer_path(self):
        outputs = self.output('integrations/ratchet-spring-boot/consumer-tests/sql/pom.xml')
        self.counts(outputs, dict(spring_consumer=7, spring_native=5, spring_runtime=2))
        self.assertEqual(outputs['unit_scope'], 'none')
        self.assertEqual(outputs['ee11_scope'], 'none')
        self.assertEqual(outputs['has_showcase'], 'false')

    def test_testsuite_and_coverage(self):
        for path in ['testing/ratchet-testsuite/src/test/java/Foo.java', 'testing/ratchet-coverage/pom.xml']:
            with self.subTest(path=path):
                self.counts(self.output(path), dict(integration=7))

    def test_docs_and_no_lane(self):
        for paths, code, web in [(['README.md', 'website/docs/x.md'], 'false', 'true'),
                                 (['.github/workflows/codeql.yml'], 'true', 'false')]:
            outputs = self.output(*paths)
            self.counts(outputs, {})
            self.assertEqual(outputs['has_showcase'], 'false')
            self.assertEqual(outputs['code'], code)
            self.assertEqual(outputs['web'], web)
            for prefix in ['unit', 'ee11']:
                self.assertEqual(outputs[prefix + '_scope'], 'none')
                self.assertEqual(outputs[prefix + '_modules'], '')

    def test_global_and_empty(self):
        for paths in [['.github/workflows/ci.yml'], ['foo.txt'], []]:
            with self.subTest(paths=paths):
                outputs = self.output(*paths)
                self.reduced_all(outputs)
                self.assertEqual(outputs['unit_scope'], 'all')
                self.assertEqual(outputs['ee11_scope'], 'all')

    def test_runtime_guard(self):
        outputs = self.output('infra/ci/check_runtime_consumer_reports.py')
        self.counts(outputs, dict(spring_runtime=2))
        self.assertEqual(outputs['unit_scope'], 'none')
        self.assertEqual(outputs['has_showcase'], 'false')

    def test_store_ignore_invariants(self):
        pom = read_pom(REPO / 'integrations/ratchet-spring-boot/ratchet-spring-boot-autoconfigure-jpa/pom.xml')
        deps = {d.findtext('artifactId'): d for d in pom.findall('dependencies/dependency')}
        for db in ['mysql', 'postgresql', 'oracle', 'sqlserver']:
            self.assertEqual(deps['ratchet-store-' + db].findtext('optional'), 'true',
                             'store-ignore requires SQL autoconfigure store dependencies to be optional')
        pom = read_pom(REPO / TESTSUITE / 'pom.xml')
        mongo = next(d for d in pom.findall('dependencies/dependency') if d.findtext('artifactId') == 'ratchet-store-mongodb')
        self.assertEqual(mongo.findtext('scope'), 'provided',
                         'store-ignore requires the testsuite base Mongo dependency to be provided')
        graph = Graph(REPO)
        required = set(STORE_MODULES.values()) | {COVERAGE}
        for lane, rows in {**LANES, 'showcase': [{}]}.items():
            for row in rows:
                required.update(row_model(lane, row)[0])
        self.assertTrue(required <= set(graph.poms), 'Every lane root, trigger and store must be in the reactor')

    def test_workflow_output_keys(self):
        keys = set(select(REPO)[0])
        refs = set(re.findall(r'needs\.changes\.outputs\.(\w+)', (REPO / '.github/workflows/ci.yml').read_text()))
        self.assertTrue(refs <= keys, refs - keys)
        mapped = set(re.findall(r'steps\.select\.outputs\.(\w+)', (REPO / '.github/workflows/ci.yml').read_text()))
        self.assertEqual(mapped, keys)

    def test_cli_output_append_and_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            changed = root / 'changed.txt'
            changed.write_text('\n stores/ratchet-store-mysql/pom.xml\n\n')
            output = root / 'output'
            summary = root / 'summary'
            output.write_text('previous=value\n')
            env = dict(os.environ, GITHUB_OUTPUT=str(output), GITHUB_STEP_SUMMARY=str(summary))
            result = subprocess.run([sys.executable, str(REPO / 'infra/ci/affected_lanes.py'),
                                     '--changed-files', str(changed)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, '')
            lines = output.read_text().splitlines()
            self.assertEqual(lines[0], 'previous=value')
            self.assertEqual(dict(line.split('=', 1) for line in lines[1:]), self.output('stores/ratchet-store-mysql/pom.xml'))
            self.assertIn('stores/ratchet-store-mysql', summary.read_text())
            self.assertIn('integration: 7 rows; databases/stores: mysql', summary.read_text())


if __name__ == '__main__':
    unittest.main()
