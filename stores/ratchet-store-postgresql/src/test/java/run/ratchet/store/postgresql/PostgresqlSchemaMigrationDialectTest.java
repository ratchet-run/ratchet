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
package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.migration.SchemaMigrator;

class PostgresqlSchemaMigrationDialectTest {

  private final PostgresqlSchemaMigrationDialect dialect = new PostgresqlSchemaMigrationDialect();
  private Connection connection;
  private Statement statement;

  @BeforeEach
  void setUp() throws Exception {
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  void advertisesPostgresqlIdentityOnSharedConnection() {
    assertEquals("postgresql", dialect.id());
    assertFalse(dialect.usesDedicatedLockConnection());
  }

  @Test
  void acquiresSessionAdvisoryLock() throws Exception {
    dialect.acquireLock(connection);

    verify(statement).execute(startsWith("SELECT pg_advisory_lock("));
  }

  @Test
  void releasesSessionAdvisoryLock() throws Exception {
    dialect.releaseLock(connection);

    verify(statement).execute(startsWith("SELECT pg_advisory_unlock("));
  }

  @Test
  void versionLedgerSqlIsPostgresqlFlavored() {
    assertTrue(dialect.createVersionTableSql().contains("TIMESTAMPTZ"));
    assertTrue(dialect.recordVersionSql().startsWith("INSERT INTO ratchet_schema_version"));
    assertTrue(dialect.recordVersionSql().contains("ON CONFLICT (version) DO UPDATE"));
  }

  @Test
  void preservesReleasedV001Checksum() throws Exception {
    List<SchemaMigrator.MigrationScript> migrations =
        new SchemaMigrator(mock(DataSource.class), dialect).discoverMigrations();
    SchemaMigrator.MigrationScript v001 =
        migrations.stream()
            .filter(script -> script.version().equals("001"))
            .findFirst()
            .orElseThrow();

    assertEquals(
        List.of("001", "002", "003", "004", "005"),
        migrations.stream().map(script -> script.version()).toList());
    assertEquals(
        "de49b983ef0b9110af22f59047a9e91fd1b37efdf7a790241b8297f82c857160", v001.checksum());
  }
}
