package run.ratchet.store.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

    SchemaMigrator.MigrationResult result =
        new SchemaMigrator(dataSource, "mysql", "schema-migrator").migrate();

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
    SchemaMigrator migrator = new SchemaMigrator(dataSource, "mysql", "schema-migrator");
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

    assertThrows(
        SchemaMigrationException.class,
        () -> new SchemaMigrator(dataSource, "mysql", "schema-migrator").migrate());

    verify(insertVersion, never()).executeUpdate();
    verify(connection, never()).commit();
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
}
