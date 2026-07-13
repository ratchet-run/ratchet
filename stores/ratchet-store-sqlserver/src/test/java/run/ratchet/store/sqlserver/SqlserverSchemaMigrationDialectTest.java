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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.migration.SchemaMigrationException;
import run.ratchet.store.migration.SchemaMigrator;

class SqlserverSchemaMigrationDialectTest {

  private final SqlserverSchemaMigrationDialect dialect = new SqlserverSchemaMigrationDialect();
  private Connection connection;
  private Statement statement;

  @BeforeEach
  void setUp() throws Exception {
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
  }

  private void stubAppLockResult(int result) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getInt(1)).thenReturn(result);
  }

  @Test
  void advertisesSqlserverIdentityOnSharedConnection() {
    assertEquals("sqlserver", dialect.id());
    assertFalse(dialect.usesDedicatedLockConnection());
  }

  @Test
  void acquiresApplicationLockViaSpGetAppLock() throws Exception {
    stubAppLockResult(0);

    dialect.acquireLock(connection);

    verify(statement).executeQuery(contains("sp_getapplock"));
  }

  @Test
  void failsWhenAppLockReturnsNegative() throws Exception {
    stubAppLockResult(-1);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> dialect.acquireLock(connection));

    assertTrue(ex.getMessage().contains("Timed out acquiring SQL Server schema migration lock"));
  }

  @Test
  void releasesApplicationLock() throws Exception {
    dialect.releaseLock(connection);

    verify(statement).execute(contains("sp_releaseapplock"));
  }

  @Test
  void versionLedgerSqlIsSqlserverFlavored() {
    assertTrue(dialect.createVersionTableSql().contains("OBJECT_ID"));
    assertTrue(dialect.createVersionTableSql().contains("DATETIME2(6)"));
    assertTrue(dialect.recordVersionSql().startsWith("MERGE ratchet_schema_version"));
    assertTrue(dialect.recordVersionSql().contains("WHEN MATCHED THEN UPDATE"));
  }

  @Test
  void preservesV001ChecksumAndContiguousCrossStoreVersions() throws Exception {
    List<SchemaMigrator.MigrationScript> migrations =
        new SchemaMigrator(mock(DataSource.class), dialect).discoverMigrations();
    SchemaMigrator.MigrationScript v001 = migrations.get(0);

    assertEquals(
        List.of("001", "002", "003", "004", "005"),
        migrations.stream().map(script -> script.version()).toList());
    assertEquals(
        "1dd8f2d437ef11447c7f74ef5601aa90d531475568d01349a12d56d4393fbd87", v001.checksum());
  }
}
