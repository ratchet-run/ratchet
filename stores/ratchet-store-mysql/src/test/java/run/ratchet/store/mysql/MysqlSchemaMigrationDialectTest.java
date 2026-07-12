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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

class MysqlSchemaMigrationDialectTest {

  private static final String GET_LOCK = "SELECT GET_LOCK('ratchet_schema_migration', 30)";
  private static final String RELEASE_LOCK = "SELECT RELEASE_LOCK('ratchet_schema_migration')";

  private final MysqlSchemaMigrationDialect dialect = new MysqlSchemaMigrationDialect();
  private Connection connection;
  private Statement statement;

  @BeforeEach
  void setUp() throws Exception {
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
  }

  private void stubLockQuery(String sql, int result) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(statement.executeQuery(sql)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getInt(1)).thenReturn(result);
  }

  @Test
  void advertisesMysqlIdentityOnSharedConnection() {
    assertEquals("mysql", dialect.id());
    assertFalse(dialect.usesDedicatedLockConnection());
  }

  @Test
  void acquiresAdvisoryLockViaGetLock() throws Exception {
    stubLockQuery(GET_LOCK, 1);

    dialect.acquireLock(connection);

    verify(statement).executeQuery(GET_LOCK);
  }

  @Test
  void failsWhenGetLockDoesNotReturnOne() throws Exception {
    stubLockQuery(GET_LOCK, 0);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> dialect.acquireLock(connection));

    assertTrue(ex.getMessage().contains("Timed out acquiring MySQL schema migration lock"));
  }

  @Test
  void releasesAdvisoryLock() throws Exception {
    stubLockQuery(RELEASE_LOCK, 1);

    dialect.releaseLock(connection);

    verify(statement).executeQuery(RELEASE_LOCK);
  }

  @Test
  void failsWhenReleaseLockDoesNotReturnOne() throws Exception {
    stubLockQuery(RELEASE_LOCK, 0);

    SQLException ex = assertThrows(SQLException.class, () -> dialect.releaseLock(connection));

    assertTrue(ex.getMessage().contains("Failed to release MySQL schema migration lock"));
  }

  @Test
  void versionLedgerSqlIsMysqlFlavored() {
    assertTrue(dialect.createVersionTableSql().contains("ENGINE = InnoDB"));
    assertTrue(dialect.recordVersionSql().startsWith("INSERT INTO ratchet_schema_version"));
    assertTrue(dialect.recordVersionSql().contains("ON DUPLICATE KEY UPDATE"));
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
        "0b339e555cddc589c0844184a04e2eff8f803bc7d1ef18a695b02dacb1224112", v001.checksum());
  }
}
