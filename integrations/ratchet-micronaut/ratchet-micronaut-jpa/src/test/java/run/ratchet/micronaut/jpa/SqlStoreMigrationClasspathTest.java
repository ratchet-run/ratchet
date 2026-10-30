/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.micronaut.jpa;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URL;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import run.ratchet.store.migration.SchemaMigrationDialect;
import run.ratchet.store.migration.SchemaMigrator;
import run.ratchet.store.migration.SchemaMigrator.MigrationScript;
import run.ratchet.store.mysql.MysqlSchemaMigrationDialect;
import run.ratchet.store.oracle.OracleSchemaMigrationDialect;
import run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect;
import run.ratchet.store.sqlserver.SqlserverSchemaMigrationDialect;

/** This module's test classpath holds all four SQL stores, as an application with two might. */
class SqlStoreMigrationClasspathTest {
  static Stream<SchemaMigrationDialect> dialects() {
    return Stream.of(
        new MysqlSchemaMigrationDialect(),
        new PostgresqlSchemaMigrationDialect(),
        new OracleSchemaMigrationDialect(),
        new SqlserverSchemaMigrationDialect());
  }

  @ParameterizedTest
  @MethodSource("dialects")
  void discoversOnlyTheDialectsOwnScripts(SchemaMigrationDialect dialect) throws Exception {
    List<MigrationScript> scripts =
        new SchemaMigrator(mock(DataSource.class), dialect).discoverMigrations();
    String store =
        dialect.getClass().getProtectionDomain().getCodeSource().getLocation().toString();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();

    assertFalse(scripts.isEmpty());
    for (MigrationScript script : scripts) {
      URL resource = loader.getResource(script.resourceName());
      assertTrue(
          resource.toString().contains(store),
          script.resourceName() + " resolved to " + resource + ", not " + store);
    }
  }
}
