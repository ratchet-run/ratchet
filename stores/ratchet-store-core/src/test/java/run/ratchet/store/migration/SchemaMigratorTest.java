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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.RatchetOptions;

/**
 * Exercises the dialect-agnostic {@link SchemaMigrator} engine — discovery, checksum validation,
 * the apply loop, and lock orchestration — against a {@link RecordingDialect} stub.
 * Dialect-specific SQL (lock statements, version DDL, upserts) is covered by each store's own
 * dialect test.
 */
class SchemaMigratorTest {

  private DataSource dataSource;
  private Connection connection;
  private Statement statement;
  private PreparedStatement selectVersion;
  private PreparedStatement insertVersion;
  private RecordingDialect dialect;

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
    return new SchemaMigrator(dataSource, dialect, classpathPrefix);
  }

  @BeforeEach
  void setUp() throws Exception {
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    selectVersion = mock(PreparedStatement.class);
    insertVersion = mock(PreparedStatement.class);
    dialect = new RecordingDialect();

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.prepareStatement(startsWith("SELECT checksum"))).thenReturn(selectVersion);
    when(connection.prepareStatement(startsWith("INSERT INTO ratchet_schema_version")))
        .thenReturn(insertVersion);
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
    assertEquals(1, dialect.acquireCount());
    assertEquals(1, dialect.releaseCount());
  }

  @Test
  void singleStatementDirectivePreservesJdbcBlocksWithInternalSemicolons() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);

    migrator("schema-migrator-single-statement").migrate();

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(statement, atLeast(2)).execute(sqlCaptor.capture());
    List<String> executedSql = sqlCaptor.getAllValues();
    List<String> blocks = executedSql.stream().filter(sql -> sql.startsWith("BEGIN")).toList();
    assertEquals(1, blocks.size());
    assertTrue(blocks.get(0).contains("EXECUTE IMMEDIATE 'SELECT 1 FROM dual';"));
    assertTrue(blocks.get(0).endsWith("END;"));
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
  void failsWhenAdvisoryLockCannotBeAcquired() throws Exception {
    dialect.failAcquireWith(
        new SchemaMigrationException("Timed out acquiring schema migration lock"));

    SchemaMigrationException ex =
        assertThrows(SchemaMigrationException.class, () -> migrator("schema-migrator").migrate());

    assertTrue(ex.getMessage().contains("Timed out acquiring schema migration lock"));
    verify(statement, never()).execute(startsWith("CREATE TABLE IF NOT EXISTS"));
    assertEquals(0, dialect.releaseCount());
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
    assertEquals(1, dialect.releaseCount());
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
  void surfacesReleaseLockFailureAfterMigrationWorkCompletes() {
    dialect.failReleaseWith(new java.sql.SQLException("Failed to release schema migration lock"));

    java.sql.SQLException ex =
        assertThrows(
            java.sql.SQLException.class, () -> migrator("schema-migrator-empty").migrate());

    assertTrue(ex.getMessage().contains("Failed to release schema migration lock"));
    assertEquals(1, dialect.releaseCount());
  }

  @Test
  void keepsMigrationFailureWhenReleaseLockAlsoFails() throws Exception {
    ResultSet firstMissingVersion = missingVersion();
    when(selectVersion.executeQuery()).thenReturn(firstMissingVersion);
    when(statement.execute(contains("CREATE TABLE ratchet_test_order")))
        .thenThrow(new java.sql.SQLException("migration failed"));
    dialect.failReleaseWith(new java.sql.SQLException("Failed to release schema migration lock"));

    java.sql.SQLException ex =
        assertThrows(java.sql.SQLException.class, () -> migrator("schema-migrator").migrate());

    assertEquals("migration failed", ex.getMessage());
    assertEquals(1, ex.getSuppressed().length);
    assertTrue(ex.getSuppressed()[0].getMessage().contains("Failed to release"));
    verify(connection).rollback();
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

    SchemaMigrationDialect mysqlDialect = mock(SchemaMigrationDialect.class);
    when(mysqlDialect.id()).thenReturn("mysql");
    @SuppressWarnings("unchecked")
    Instance<SchemaMigrationDialect> dialects = mock(Instance.class);
    when(dialects.isUnsatisfied()).thenReturn(false);
    when(dialects.iterator()).thenReturn(List.of(mysqlDialect).iterator());

    RatchetOptions options =
        RatchetOptions.builder()
            .schema(
                schema ->
                    schema
                        .autoMigrate(true)
                        .migrationDialect("mysql")
                        .migrationPrefix("schema-migrator-empty"))
            .build();
    SchemaMigrationLifecycleHook hook =
        new SchemaMigrationLifecycleHook(options, dataSources, dialects);

    SchemaInitializationException ex =
        assertThrows(SchemaInitializationException.class, hook::beforeStart);

    assertEquals("Ratchet schema auto-migration failed: SQLException", ex.getMessage());
  }

  /**
   * Minimal {@link SchemaMigrationDialect} for engine tests: returns predictable SQL and records
   * (or fails) lock calls so the engine's orchestration can be asserted without a real database.
   */
  private static final class RecordingDialect implements SchemaMigrationDialect {

    private RuntimeException acquireFailure;
    private SQLException releaseFailure;
    private int acquireCount;
    private int releaseCount;

    void failAcquireWith(RuntimeException failure) {
      this.acquireFailure = failure;
    }

    void failReleaseWith(SQLException failure) {
      this.releaseFailure = failure;
    }

    int acquireCount() {
      return acquireCount;
    }

    int releaseCount() {
      return releaseCount;
    }

    @Override
    public String id() {
      return "stub";
    }

    @Override
    public String createVersionTableSql() {
      return "CREATE TABLE IF NOT EXISTS ratchet_schema_version (version VARCHAR(20) NOT NULL)";
    }

    @Override
    public String recordVersionSql() {
      return "INSERT INTO ratchet_schema_version (version, description, checksum) VALUES (?, ?, ?)";
    }

    @Override
    public boolean usesDedicatedLockConnection() {
      return false;
    }

    @Override
    public void acquireLock(Connection connection) {
      acquireCount++;
      if (acquireFailure != null) {
        throw acquireFailure;
      }
    }

    @Override
    public void releaseLock(Connection connection) throws SQLException {
      releaseCount++;
      if (releaseFailure != null) {
        throw releaseFailure;
      }
    }
  }
}
