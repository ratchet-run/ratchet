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
package run.ratchet.store.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.RatchetOptions;

class SchemaMigratorTest {

  private DataSource dataSource;
  private Connection connection;
  private Statement statement;
  private PreparedStatement selectVersion;
  private PreparedStatement insertVersion;
  private ResultSet mysqlLock;
  private ResultSet mysqlRelease;

  private static ResultSet missingVersion() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    return resultSet;
  }

  private static ResultSet existingVersion(String checksum) throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn(checksum);
    return resultSet;
  }

  private static int indexOfContaining(List<String> values, String needle) {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).contains(needle)) {
        return i;
      }
    }
    return -1;
  }

  private SchemaMigrator migrator(String classpathPrefix) {
    return new SchemaMigrator(dataSource, "mysql", classpathPrefix);
  }

  @BeforeEach
  void setUp() throws Exception {
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    selectVersion = mock(PreparedStatement.class);
    insertVersion = mock(PreparedStatement.class);
    mysqlLock = mock(ResultSet.class);
    mysqlRelease = mock(ResultSet.class);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.prepareStatement(startsWith("SELECT checksum"))).thenReturn(selectVersion);
    when(connection.prepareStatement(startsWith("INSERT INTO ratchet_schema_version")))
        .thenReturn(insertVersion);
    when(statement.executeQuery("SELECT GET_LOCK('ratchet_schema_migration', 30)"))
        .thenReturn(mysqlLock);
    when(statement.executeQuery("SELECT RELEASE_LOCK('ratchet_schema_migration')"))
        .thenReturn(mysqlRelease);
    when(mysqlLock.next()).thenReturn(true);
    when(mysqlLock.getInt(1)).thenReturn(1);
    when(mysqlRelease.next()).thenReturn(true);
    when(mysqlRelease.getInt(1)).thenReturn(1);
  }

  @Test
  void appliesClasspathMigrationsInVersionOrder() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    ResultSet secondMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion, secondMissingVersion);

    SchemaMigrator.MigrationResult result = migrator("schema-migrator").migrate();

    assertEquals(2, result.appliedCount());
    assertEquals(0, result.skippedCount());
    assertEquals(List.of("001", "002"), result.applied().stream().map(s -> s.version()).toList());

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(statement, atLeast(3)).execute(sqlCaptor.capture());
    List<String> executedSql = sqlCaptor.getAllValues();
    assertTrue(
        indexOfContaining(executedSql, "CREATE TABLE IF NOT EXISTS ratchet_schema_version") >= 0);
    assertTrue(
        indexOfContaining(executedSql, "CREATE TABLE ratchet_test_order")
            < indexOfContaining(executedSql, "INSERT INTO ratchet_test_order"));
    assertTrue(
        executedSql.stream()
            .anyMatch(sql -> sql.contains("second; still one") && sql.startsWith("INSERT")));

    verify(insertVersion, times(2)).executeUpdate();
    verify(connection, times(2)).commit();
  }

  @Test
  void skipsMigrationsWithMatchingChecksums() throws Exception {
    SchemaMigrator migrator = migrator("schema-migrator");
    List<SchemaMigrator.MigrationScript> scripts = migrator.discoverMigrations();
    ResultSet firstExistingVersion = existingVersion(scripts.get(0).checksum());
    ResultSet secondExistingVersion = existingVersion(scripts.get(1).checksum());
    when(selectVersion.executeQuery()).thenReturn(firstExistingVersion, secondExistingVersion);

    SchemaMigrator.MigrationResult result = migrator.migrate();

    assertEquals(0, result.appliedCount());
    assertEquals(2, result.skippedCount());
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenRecordedChecksumDoesNotMatchClasspathScript() throws Exception {
    ResultSet mismatchedVersion = existingVersion("not-the-current-checksum");
    when(selectVersion.executeQuery()).thenReturn(mismatchedVersion);

    assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenRecordedChecksumIsMissing() throws Exception {
    ResultSet missingChecksum = existingVersion(" ");
    when(selectVersion.executeQuery()).thenReturn(missingChecksum);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("already recorded without a checksum"));
    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
  }

  @Test
  void failsWhenMysqlAdvisoryLockCannotBeAcquired() throws Exception {
    when(mysqlLock.getInt(1)).thenReturn(0);

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("Timed out acquiring MySQL schema migration lock"));
    verify(statement, never()).execute(startsWith("CREATE TABLE IF NOT EXISTS"));
    verify(statement, never()).executeQuery("SELECT RELEASE_LOCK('ratchet_schema_migration')");
  }

  @Test
  void postgresqlDialectUsesAdvisoryLock() throws Exception {
    SchemaMigrator.MigrationResult result =
        new SchemaMigrator(dataSource, "postgresql", "schema-migrator-empty").migrate();

    assertEquals(0, result.appliedCount());
    assertEquals(0, result.skippedCount());
    verify(statement).execute(startsWith("SELECT pg_advisory_lock("));
    verify(statement).execute(startsWith("SELECT pg_advisory_unlock("));
  }

  @Test
  void rollsBackAndRestoresAutoCommitWhenMigrationStatementFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new java.sql.SQLException("boom"));

    java.sql.SQLException ex =
        assertThrows(java.sql.SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("boom", ex.getMessage());
    verify(connection).setAutoCommit(false);
    verify(connection).rollback();
    verify(connection).setAutoCommit(true);
    verify(insertVersion, never()).executeUpdate();
    verify(statement).executeQuery("SELECT RELEASE_LOCK('ratchet_schema_migration')");
  }

  @Test
  void keepsMigrationFailureWhenRollbackAlsoFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new java.sql.SQLException("ddl failed"));
    doThrow(new java.sql.SQLException("rollback failed")).when(connection).rollback();

    java.sql.SQLException ex =
        assertThrows(java.sql.SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("ddl failed", ex.getMessage());
    assertEquals(1, ex.getSuppressed().length);
    assertEquals("rollback failed", ex.getSuppressed()[0].getMessage());
    verify(connection).setAutoCommit(true);
  }

  @Test
  void surfacesMysqlReleaseLockFailureAfterMigrationWorkCompletes() throws Exception {
    when(mysqlRelease.getInt(1)).thenReturn(0);

    java.sql.SQLException ex =
        assertThrows(
            java.sql.SQLException.class, () -> migrator("schema-migrator-empty").migrate());

    assertTrue(ex.getMessage().contains("Failed to release MySQL schema migration lock"));
    verify(statement).executeQuery("SELECT RELEASE_LOCK('ratchet_schema_migration')");
  }

  @Test
  void keepsMigrationFailureWhenMysqlReleaseLockAlsoFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new java.sql.SQLException("migration failed"));
    when(mysqlRelease.getInt(1)).thenReturn(0);

    java.sql.SQLException ex =
        assertThrows(java.sql.SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("migration failed", ex.getMessage());
    assertEquals(1, ex.getSuppressed().length);
    assertTrue(ex.getSuppressed()[0].getMessage().contains("Failed to release MySQL"));
    verify(connection).rollback();
    verify(statement).executeQuery("SELECT RELEASE_LOCK('ratchet_schema_migration')");
  }

  @Test
  void dialectFromMetadataMapsKnownProducts() throws Exception {
    DatabaseMetaData mysqlMeta = mock(DatabaseMetaData.class);
    when(mysqlMeta.getDatabaseProductName()).thenReturn("MySQL");
    Connection mysqlConn = mock(Connection.class);
    when(mysqlConn.getMetaData()).thenReturn(mysqlMeta);
    assertEquals("mysql", SchemaMigrator.dialectFromMetadata(mysqlConn));

    DatabaseMetaData mariaMeta = mock(DatabaseMetaData.class);
    when(mariaMeta.getDatabaseProductName()).thenReturn("MariaDB");
    Connection mariaConn = mock(Connection.class);
    when(mariaConn.getMetaData()).thenReturn(mariaMeta);
    assertEquals("mysql", SchemaMigrator.dialectFromMetadata(mariaConn));

    DatabaseMetaData pgMeta = mock(DatabaseMetaData.class);
    when(pgMeta.getDatabaseProductName()).thenReturn("PostgreSQL");
    Connection pgConn = mock(Connection.class);
    when(pgConn.getMetaData()).thenReturn(pgMeta);
    assertEquals("postgresql", SchemaMigrator.dialectFromMetadata(pgConn));
  }

  @Test
  void dialectFromMetadataRejectsLookalikes() throws Exception {
    DatabaseMetaData crdbMeta = mock(DatabaseMetaData.class);
    when(crdbMeta.getDatabaseProductName()).thenReturn("CockroachDB");
    Connection crdbConn = mock(Connection.class);
    when(crdbConn.getMetaData()).thenReturn(crdbMeta);

    SchemaInitializationException ex =
        assertThrows(
            SchemaInitializationException.class,
            () -> SchemaMigrator.dialectFromMetadata(crdbConn));
    assertTrue(ex.getMessage().contains("CockroachDB"));
    assertTrue(ex.getMessage().contains("Supported"));
  }

  @Test
  void lifecycleHookNamesExceptionWhenMigrationFailureHasNoMessage() throws Exception {
    @SuppressWarnings("unchecked")
    Instance<DataSource> dataSources = mock(Instance.class);
    DataSource failingDataSource = mock(DataSource.class);
    when(dataSources.isUnsatisfied()).thenReturn(false);
    when(dataSources.isAmbiguous()).thenReturn(false);
    when(dataSources.get()).thenReturn(failingDataSource);
    when(failingDataSource.getConnection()).thenThrow(new java.sql.SQLException());

    RatchetOptions options =
        RatchetOptions.builder()
            .schema(
                schema ->
                    schema
                        .autoMigrate(true)
                        .migrationDialect("mysql")
                        .migrationPrefix("schema-migrator-empty"))
            .build();
    SchemaMigrationLifecycleHook hook = new SchemaMigrationLifecycleHook(options, dataSources);

    SchemaInitializationException ex =
        assertThrows(SchemaInitializationException.class, hook::beforeStart);

    assertEquals("Ratchet schema auto-migration failed: SQLException", ex.getMessage());
  }
}
